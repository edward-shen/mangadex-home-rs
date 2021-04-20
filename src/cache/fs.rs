use actix_web::error::PayloadError;
use bytes::Buf;
use futures::{Stream, StreamExt};
use log::{debug, error};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::fmt::Display;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::{create_dir_all, remove_file, File};
use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::watch::{channel, Receiver};
use tokio::sync::RwLock;
use tokio_stream::wrappers::WatchStream;
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
static WRITING_STATUS: Lazy<RwLock<HashMap<PathBuf, Receiver<WritingStatus>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Tries to read from the file, returning a byte stream if it exists
pub async fn read_file(path: &Path) -> Option<Result<CacheStream, std::io::Error>> {
    if path.exists() {
        let status = WRITING_STATUS.read().await.get(path).map(Clone::clone);

        if let Some(status) = status {
            Some(
                ConcurrentFsStream::new(path, WatchStream::new(status))
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
    notifier: UnboundedSender<u64>,
) -> Result<CacheStream, std::io::Error> {
    let (tx, rx) = channel(WritingStatus::NotDone);

    let mut file = {
        let mut write_lock = WRITING_STATUS.write().await;
        let parent = path.parent().unwrap();
        create_dir_all(parent).await?;
        let file = File::create(path).await?; // we need to  make sure the file exists and is truncated.
        write_lock.insert(path.to_path_buf(), rx.clone());
        file
    };

    // need owned variant because async lifetime
    let path_buf = path.to_path_buf();
    tokio::spawn(async move {
        let path_buf = path_buf; // moves path buf into async
        let mut errored = false;
        let mut bytes_written: u64 = 0;
        while let Some(bytes) = byte_stream.next().await {
            if let Ok(mut bytes) = bytes {
                loop {
                    match file.write(&bytes).await? {
                        0 => break,
                        n => {
                            bytes.advance(n);
                            // We don't care if we don't have receivers
                            bytes_written += n as u64;
                            let _ = tx.send(WritingStatus::NotDone);
                        }
                    }
                }
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
        //
        // We don't really care if we have no receivers
        if errored {
            let _ = tx.send(WritingStatus::Error);
        } else {
            let _ = tx.send(WritingStatus::Done);
        }
        write_lock.remove(&path_buf);

        // notify
        if let Err(e) = notifier.send(bytes_written) {
            error!(
                "Failed to notify cache of new entry size: {}. Cache no longer can prune FS!",
                e
            );
        }

        // We don't ever check this, so the return value doesn't matter
        Ok::<_, std::io::Error>(())
    });

    Ok(CacheStream::Concurrent(
        ConcurrentFsStream::new(path, WatchStream::new(rx)).await?,
    ))
}

pub struct ConcurrentFsStream {
    file: Pin<Box<File>>,
    receiver: Pin<Box<WatchStream<WritingStatus>>>,
}

impl ConcurrentFsStream {
    async fn new(
        path: &Path,
        receiver: WatchStream<WritingStatus>,
    ) -> Result<Self, std::io::Error> {
        Ok(Self {
            file: Box::pin(File::open(path).await?),
            receiver: Box::pin(receiver),
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
        match self.receiver.as_mut().poll_next_unpin(cx) {
            Poll::Ready(status) => {
                let mut bytes = [0; 1460].to_vec();
                let mut buffer = ReadBuf::new(&mut bytes);
                let polled_result = self.file.as_mut().poll_read(cx, &mut buffer);
                let filled = buffer.filled().len();
                match (status, filled) {
                    (Some(WritingStatus::NotDone), 0) => Poll::Pending,
                    // We got an error, abort the read.
                    (Some(WritingStatus::Error), _) => Poll::Ready(Some(Err(UpstreamError))),
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
            Poll::Pending => Poll::Pending,
        }
    }
}

impl From<UpstreamError> for actix_web::Error {
    #[inline]
    fn from(_: UpstreamError) -> Self {
        PayloadError::Incomplete(None).into()
    }
}

#[derive(Debug, Clone, Copy)]
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
