use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::{Future, Stream, StreamExt};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use reqwest::Error;
use tokio::fs::{remove_file, File, OpenOptions};
use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};
use tokio::time::Sleep;

static WRITING_STATUS: Lazy<RwLock<HashMap<PathBuf, Arc<CacheStatus>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

pub async fn transparent_file_stream(
    path: &Path,
    mut byte_stream: impl Stream<Item = Result<Bytes, Error>> + Unpin + Send + 'static,
) -> Result<impl Stream<Item = Result<Bytes, UpstreamError>>, std::io::Error> {
    if let Some(arc) = WRITING_STATUS.read().get(path) {
        FromFsStream::new(path, Arc::clone(&arc)).await
    } else {
        let done_writing_flag = Arc::new(CacheStatus::new());

        {
            let mut write_lock = WRITING_STATUS.write();
            File::create(path).await?; // we need to make sure the file exists and is truncated.
            write_lock.insert(path.to_path_buf(), Arc::clone(&done_writing_flag));
        }

        let write_flag = Arc::clone(&done_writing_flag);
        // need owned variant because async lifetime
        let mut file = OpenOptions::new().write(true).open(path).await?;
        let path_buf = path.to_path_buf();
        tokio::spawn(async move {
            let path_buf = path_buf; // moves path buf into async
            let mut was_errored = false;
            while let Some(bytes) = byte_stream.next().await {
                match bytes {
                    Ok(bytes) => file.write_all(&bytes).await?,
                    Err(_) => was_errored = true,
                }
            }

            if was_errored {
                // It's ok if the deleting the file fails, since we truncate on
                // create anyways
                let _ = remove_file(&path_buf).await;
            } else {
                file.flush().await?;
                file.sync_all().await?;
            }

            let mut write_lock = WRITING_STATUS.write();
            // This needs to be written atomically with the write lock, else
            // it's possible we have an inconsistent state
            if was_errored {
                write_flag.store(WritingStatus::Error);
            } else {
                write_flag.store(WritingStatus::Done);
            }
            write_lock.remove(&path_buf);

            // We don't ever check this, so the return value doesn't matter
            Ok::<_, std::io::Error>(())
        });

        FromFsStream::new(path, Arc::clone(&done_writing_flag)).await
    }
}

struct FromFsStream {
    file: Pin<Box<File>>,
    sleep: Pin<Box<Sleep>>,
    is_file_done_writing: Arc<CacheStatus>,
}

impl FromFsStream {
    async fn new(path: &Path, is_done: Arc<CacheStatus>) -> Result<Self, std::io::Error> {
        Ok(Self {
            file: Box::pin(File::open(path).await?),
            // 0.5ms
            sleep: Box::pin(tokio::time::sleep(Duration::from_micros(500))),
            is_file_done_writing: is_done,
        })
    }
}

/// Represents some upstream error.
pub struct UpstreamError;

impl Stream for FromFsStream {
    type Item = Result<Bytes, UpstreamError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let status = self.is_file_done_writing.load();

        let mut bytes = BytesMut::with_capacity(1460);
        let mut buffer = ReadBuf::new(&mut bytes);
        let polled_result = self.file.as_mut().poll_read(cx, &mut buffer);

        match (status, buffer.filled().len()) {
            // Prematurely reached EOF, schedule a poll in the future
            (WritingStatus::NotDone, 0) => {
                let _ = self.sleep.as_mut().poll(cx);
                Poll::Pending
            }
            // We got an error, abort the read.
            (WritingStatus::Error, _) => Poll::Ready(Some(Err(UpstreamError))),
            _ => polled_result.map(|_| Some(Ok(bytes.split().into()))),
        }
    }
}

struct CacheStatus(AtomicU8);

impl CacheStatus {
    #[inline]
    fn new() -> Self {
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
