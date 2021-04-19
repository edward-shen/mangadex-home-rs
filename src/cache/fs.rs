use actix_web::error::PayloadError;
use futures::{Stream, StreamExt};
use log::debug;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::fmt::Display;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::fs::{create_dir_all, remove_file, File};
use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};
use tokio::sync::RwLock;
use tokio::time::Interval;
use tokio_util::codec::{BytesCodec, FramedRead};

use super::{BoxedImageStream, CacheStream, CacheStreamItem};

/// Keeps track of files that are currently being written to.
///
/// Why is this necessary? Consider the following situation:
///
/// Client A requests file `foo.png`. We construct a transparent file stream,
/// and now the file is being streamed into and from.
///
/// Client B requests the same file `foo.png`. A naive implementation would
/// attempt to either read directly the file as it sees the file existing. This
/// is problematic as the file could still be written to. If Client B catches
/// up to Client A's request, then Client B could receive a broken image, as it
/// thinks it's done reading the file.
///
/// We effectively use `WRITING_STATUS` as a status relay to ensure concurrent
/// reads to the file while it's being written to will wait for writing to be
/// completed.
static WRITING_STATUS: Lazy<RwLock<HashMap<PathBuf, Arc<CacheStatus>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Tries to read from the file, returning a byte stream if it exists
pub async fn read_file(path: &Path) -> Option<Result<CacheStream, std::io::Error>> {
    if path.exists() {
        let status = WRITING_STATUS.read().await.get(path).map(Arc::clone);

        if let Some(status) = status {
            Some(
                ConcurrentFsStream::new(path, status)
                    .await
                    .map(CacheStream::Concurrent),
            )
        } else {
            Some(
                File::open(path)
                    .await
                    .map(|f| CacheStream::Completed(FramedRead::new(f, BytesCodec::new()))),
            )
        }
    } else {
        None
    }
}

/// Maps the input byte stream into one that writes to disk instead, returning
/// a stream that reads from disk instead.
pub async fn write_file(
    path: &Path,
    mut byte_stream: BoxedImageStream,
) -> Result<CacheStream, std::io::Error> {
    let done_writing_flag = Arc::new(CacheStatus::new());

    let mut file = {
        let mut write_lock = WRITING_STATUS.write().await;
        let parent = path.parent().unwrap();
        create_dir_all(parent).await?;
        let file = File::create(path).await?; // we need to make sure the file exists and is truncated.
        write_lock.insert(path.to_path_buf(), Arc::clone(&done_writing_flag));
        file
    };

    let write_flag = Arc::clone(&done_writing_flag);
    // need owned variant because async lifetime
    let path_buf = path.to_path_buf();
    tokio::spawn(async move {
        let path_buf = path_buf; // moves path buf into async
        let mut errored = false;
        while let Some(bytes) = byte_stream.next().await {
            if let Ok(bytes) = bytes {
                file.write_all(&bytes).await?
            } else {
                errored = true;
                break;
            }
        }

        if errored {
            // It's ok if the deleting the file fails, since we truncate on
            // create anyways, but it should be best effort
            let _ = remove_file(&path_buf).await;
        } else {
            file.flush().await?;
            file.sync_all().await?; // we need metadata
            debug!("writing to file done");
        }

        let mut write_lock = WRITING_STATUS.write().await;
        // This needs to be written atomically with the write lock, else
        // it's possible we have an inconsistent state
        if errored {
            write_flag.store(WritingStatus::Error);
        } else {
            write_flag.store(WritingStatus::Done);
        }
        write_lock.remove(&path_buf);

        // We don't ever check this, so the return value doesn't matter
        Ok::<_, std::io::Error>(())
    });

    Ok(CacheStream::Concurrent(
        ConcurrentFsStream::new(path, done_writing_flag).await?,
    ))
}

pub struct ConcurrentFsStream {
    file: Pin<Box<File>>,
    sleep: Pin<Box<Interval>>,
    is_file_done_writing: Arc<CacheStatus>,
}

impl ConcurrentFsStream {
    async fn new(path: &Path, is_done: Arc<CacheStatus>) -> Result<Self, std::io::Error> {
        Ok(Self {
            file: Box::pin(File::open(path).await?),
            // 0.5ms
            sleep: Box::pin(tokio::time::interval(Duration::from_micros(250))),
            is_file_done_writing: is_done,
        })
    }
}

/// Represents some upstream error.
#[derive(Debug)]
pub struct UpstreamError;

impl std::error::Error for UpstreamError {}

impl Display for UpstreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "An upstream error occurred")
    }
}

impl Stream for ConcurrentFsStream {
    type Item = CacheStreamItem;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let status = self.is_file_done_writing.load();

        let mut bytes = [0; 1460].to_vec();
        let mut buffer = ReadBuf::new(&mut bytes);
        let polled_result = self.file.as_mut().poll_read(cx, &mut buffer);
        let filled = buffer.filled().len();
        match (status, filled) {
            // Prematurely reached EOF, schedule a poll in the future
            (WritingStatus::NotDone, 0) => {
                let _ = self.sleep.as_mut().poll_tick(cx);
                Poll::Pending
            }
            // We got an error, abort the read.
            (WritingStatus::Error, _) => Poll::Ready(Some(Err(UpstreamError))),
            _ => {
                bytes.truncate(filled);
                polled_result.map(|_| {
                    if bytes.is_empty() {
                        None
                    } else {
                        Some(Ok(bytes.into()))
                    }
                })
            }
        }
    }
}

impl From<UpstreamError> for actix_web::Error {
    #[inline]
    fn from(_: UpstreamError) -> Self {
        PayloadError::Incomplete(None).into()
    }
}

struct CacheStatus(AtomicU8);

impl CacheStatus {
    #[inline]
    const fn new() -> Self {
        Self(AtomicU8::new(WritingStatus::NotDone as u8))
    }

    #[inline]
    fn store(&self, status: WritingStatus) {
        self.0.store(status as u8, Ordering::Release);
    }

    #[inline]
    fn load(&self) -> WritingStatus {
        self.0.load(Ordering::Acquire).into()
    }
}

#[derive(Debug)]
enum WritingStatus {
    NotDone = 0,
    Done,
    Error,
}

impl From<u8> for WritingStatus {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0 => Self::NotDone,
            1 => Self::Done,
            2 => Self::Error,
            _ => unreachable!(),
        }
    }
}
