use actix_web::error::PayloadError;
use bytes::{Buf, Bytes};
use futures::{Stream, StreamExt};
use log::{debug, error};
use once_cell::sync::Lazy;
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt::Display;
use std::io::SeekFrom;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::fs::{create_dir_all, remove_file, File};
use tokio::io::{AsyncRead, AsyncSeekExt, AsyncWriteExt, ReadBuf};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::watch::{channel, Receiver};
use tokio::sync::RwLock;
use tokio_stream::wrappers::WatchStream;
use tokio_util::codec::{BytesCodec, FramedRead};

use super::{BoxedImageStream, CacheStream, CacheStreamItem, ImageMetadata};

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
pub async fn read_file(
    path: &Path,
) -> Option<Result<(CacheStream, ImageMetadata), std::io::Error>> {
    let std_file = std::fs::File::open(path).ok()?;
    let file = File::from_std(std_file.try_clone().ok()?);

    let metadata = {
        let mut de = serde_json::Deserializer::from_reader(std_file);
        ImageMetadata::deserialize(&mut de).ok()?
    };

    // False positive, `file` is used in both cases, which means that it's not
    // possible to move this into a map_or_else without cloning `file`.
    #[allow(clippy::option_if_let_else)]
    let stream = if let Some(status) = WRITING_STATUS.read().await.get(path).map(Clone::clone) {
        CacheStream::Concurrent(ConcurrentFsStream::from_file(
            file,
            WatchStream::new(status),
        ))
    } else {
        CacheStream::Completed(FramedRead::new(file, BytesCodec::new()))
    };

    Some(Ok((stream, metadata)))
}

/// Maps the input byte stream into one that writes to disk instead, returning
/// a stream that reads from disk instead.
pub async fn write_file(
    path: &Path,
    mut byte_stream: BoxedImageStream,
    metadata: ImageMetadata,
    notifier: UnboundedSender<u64>,
) -> Result<CacheStream, std::io::Error> {
    let (tx, rx) = channel(WritingStatus::NotDone);

    let mut file = {
        let mut write_lock = WRITING_STATUS.write().await;
        let parent = path.parent().unwrap();
        create_dir_all(parent).await?;
        let file = File::create(path).await?; // we need to make sure the file exists and is truncated.
        write_lock.insert(path.to_path_buf(), rx.clone());
        file
    };

    let metadata = serde_json::to_string(&metadata).unwrap();
    let metadata_size = metadata.len();
    // need owned variant because async lifetime
    let path_buf = path.to_path_buf();
    tokio::spawn(async move {
        let path_buf = path_buf; // moves path buf into async
        let mut errored = false;
        let mut bytes_written: u64 = 0;
        file.write_all(&metadata.as_bytes()).await?;
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

        {
            let mut write_lock = WRITING_STATUS.write().await;
            // This needs to be written atomically with the write lock, else
            // it's possible we have an inconsistent state
            //
            // We don't really care if we have no receivers
            if errored {
                let _ = tx.send(WritingStatus::Error);
            } else {
                let _ = tx.send(WritingStatus::Done(bytes_written));
            }
            write_lock.remove(&path_buf);
        }

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
        ConcurrentFsStream::new(path, metadata_size, WatchStream::new(rx)).await?,
    ))
}

pub struct ConcurrentFsStream {
    file: Pin<Box<File>>,
    receiver: Pin<Box<WatchStream<WritingStatus>>>,
    bytes_read: u64,
    bytes_total: Option<NonZeroU64>,
}

impl ConcurrentFsStream {
    async fn new(
        path: &Path,
        seek: usize,
        receiver: WatchStream<WritingStatus>,
    ) -> Result<Self, std::io::Error> {
        let mut file = File::open(path).await?;
        file.seek(SeekFrom::Start(seek as u64)).await?;
        Ok(Self::from_file(file, receiver))
    }

    fn from_file(file: File, receiver: WatchStream<WritingStatus>) -> Self {
        Self {
            file: Box::pin(file),
            receiver: Box::pin(receiver),
            bytes_read: 0,
            bytes_total: None,
        }
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
        // First, try to read from the file...

        // TODO: Might be more efficient to have a larger buffer
        let mut bytes = [0; 4 * 1024].to_vec();
        let mut buffer = ReadBuf::new(&mut bytes);
        match self.file.as_mut().poll_read(cx, &mut buffer) {
            Poll::Ready(Ok(_)) => (),
            Poll::Ready(Err(_)) => return Poll::Ready(Some(Err(UpstreamError))),
            Poll::Pending => return Poll::Pending,
        }

        // At this point, we know that we "successfully" read some amount of
        // data. Let's see if there's actual data in there...

        let filled = buffer.filled().len();
        if filled == 0 {
            // Filled is zero, which indicates two situations:
            // 1. We are actually done.
            // 2. We read to the EOF while the writer is still writing to it.

            // To handle the second case, we need to see the status of the
            // writer, and if it's done writing yet.

            if let Poll::Ready(Some(WritingStatus::Done(n))) =
                self.receiver.as_mut().poll_next_unpin(cx)
            {
                self.bytes_total = Some(NonZeroU64::new(n).unwrap())
            }

            // Okay, now we know if we've read enough bytes or not. If the
            // writer hasn't told use that it's done yet, then we know that
            // there must be more bytes to read from.

            if let Some(bytes_total) = self.bytes_total {
                if bytes_total.get() == self.bytes_read {
                    // We matched the number of bytes the writer said it wrote,
                    // so we're finally done
                    return Poll::Ready(None);
                }
            }

            // We haven't read enough bytes, so just return an empty bytes and
            // have the executor request some bytes some time in the future.
            //
            // This case might be solved by io_uring, but for now this is this
            // the best we can do.
            Poll::Ready(Some(Ok(Bytes::new())))
        } else {
            // We have data! Give it to the reader!
            self.bytes_read += filled as u64;
            bytes.truncate(filled);
            Poll::Ready(Some(Ok(bytes.into())))
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
    NotDone,
    Done(u64),
    Error,
}
