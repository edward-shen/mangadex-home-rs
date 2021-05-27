//! This module contains two functions whose sole purpose is to allow a single
//! producer multiple consumer (SPMC) system using the filesystem as an
//! intermediate.
//!
//! Consider the scenario where two clients, A and B, request the same uncached
//! file, one after the other. In a typical caching system, both requests would
//! result in a cache miss, and both requests would then be proxied from
//! upstream. But, we can do better. We know that by the time one request
//! begins, there should be a file on disk for us to read from. Why require
//! subsequent requests to read from upstream, when we can simply fetch one and
//! read from the filesystem that we know will have the exact same data?
//! Instead, we can just read from the filesystem and just inform all readers
//! when the file is done. This is beneficial to both downstream and upstream as
//! upstream no longer needs to process duplicate requests and sequential cache
//! misses are treated as closer as a cache hit.

use std::collections::HashMap;
use std::error::Error;
use std::fmt::Display;
use std::io::SeekFrom;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};

use actix_web::error::PayloadError;
use bytes::{Buf, Bytes, BytesMut};
use futures::{Future, Stream, StreamExt};
use log::{debug, warn};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use sodiumoxide::crypto::secretstream::{
    Header, Pull, Push, Stream as SecretStream, Tag, HEADERBYTES,
};
use tokio::fs::{create_dir_all, remove_file, File};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::sync::mpsc::Sender;
use tokio::sync::watch::{channel, Receiver};
use tokio::sync::RwLock;
use tokio_stream::wrappers::WatchStream;
use tokio_util::codec::{BytesCodec, FramedRead};

use super::{
    BoxedImageStream, CacheKey, CacheStreamItem, ImageMetadata, InnerStream, ENCRYPTION_KEY,
};

#[derive(Serialize, Deserialize)]
pub enum OnDiskMetadata {
    Encrypted(Header, ImageMetadata),
    Plaintext(ImageMetadata),
}

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

/// Attempts to lookup the file on disk, returning a byte stream if it exists.
/// Note that this could return two types of streams, depending on if the file
/// is in progress of being written to.
pub(super) async fn read_file(
    path: &Path,
) -> Option<Result<(InnerStream, Option<Header>, ImageMetadata), std::io::Error>> {
    let file = std::fs::File::open(path).ok()?;
    let file_0 = file.try_clone().unwrap();
    // Try reading decrypted header first...
    let mut deserializer = serde_json::Deserializer::from_reader(file);
    let maybe_metadata = ImageMetadata::deserialize(&mut deserializer);

    let parsed_metadata;
    let mut maybe_header = None;
    let mut reader: Option<Pin<Box<dyn AsyncRead + Send>>> = None;
    if let Ok(metadata) = maybe_metadata {
        // image is decrypted
        if ENCRYPTION_KEY.get().is_some() {
            // invalidate cache since we're running in at-rest encryption and
            // the file wasn't encrypted.
            warn!("Found file, but encrypted header was not found. Assuming corrupted!");
            return None;
        }

        reader = Some(Box::pin(File::from_std(file_0)));
        parsed_metadata = Some(metadata);
    } else {
        let mut file = File::from_std(file_0);
        let file_0 = file.try_clone().await.unwrap();

        // image is decrypted or corrupt

        // If the encryption key was set, use the encrypted disk reader instead;
        // else, just directly read from file.
        if let Some(key) = ENCRYPTION_KEY.get() {
            let mut header_bytes = [0; HEADERBYTES];
            if file.read_exact(&mut header_bytes).await.is_err() {
                warn!("Found file, but encrypted header was not found. Assuming corrupted!");
                return None;
            }

            let header = if let Some(header) = Header::from_slice(&header_bytes) {
                header
            } else {
                warn!("Found file, but encrypted header was invalid. Assuming corrupted!");
                return None;
            };

            let secret_stream = if let Ok(stream) = SecretStream::init_pull(&header, key) {
                stream
            } else {
                warn!("Failed to init secret stream with key and header. Assuming corrupted!");
                return None;
            };

            maybe_header = Some(header);

            reader = Some(Box::pin(EncryptedDiskReader::new(file, secret_stream)));
        }

        let mut deserializer = serde_json::Deserializer::from_reader(file_0.into_std().await);
        parsed_metadata = ImageMetadata::deserialize(&mut deserializer).ok();
    }

    // parsed_metadata is either set or unset here. If it's set then we
    // successfully decoded the data; otherwise the file is garbage.

    if let Some(reader) = reader {
        // False positive lint, `file` is used in both cases, which means that it's
        // not possible to move this into a map_or_else without cloning `file`.
        #[allow(clippy::option_if_let_else)]
        let stream = if let Some(status) = WRITING_STATUS.read().await.get(path).map(Clone::clone) {
            InnerStream::Concurrent(ConcurrentFsStream::from_reader(
                reader,
                WatchStream::new(status),
            ))
        } else {
            InnerStream::Completed(FramedRead::new(reader, BytesCodec::new()))
        };

        parsed_metadata.map(|metadata| Ok((stream, maybe_header, metadata)))
    } else {
        None
    }
}

struct EncryptedDiskReader {
    file: Pin<Box<File>>,
    stream: SecretStream<Pull>,
    buf: Vec<u8>,
}

impl EncryptedDiskReader {
    fn new(file: File, stream: SecretStream<Pull>) -> Self {
        Self {
            file: Box::pin(file),
            stream,
            buf: vec![],
        }
    }
}

impl AsyncRead for EncryptedDiskReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let cursor_start = buf.filled().len();

        let res = self.as_mut().file.as_mut().poll_read(cx, buf);
        if res.is_pending() {
            return Poll::Pending;
        }

        let cursor_new = buf.filled().len();

        // pull_to_vec internally calls vec.clear() and vec.reserve(). Generally
        // speaking we should be reading about the same amount of data each time
        // so we shouldn't experience too much of a slow down w.r.t resizing the
        // buffer...
        let new_self = Pin::into_inner(self);
        new_self
            .stream
            .pull_to_vec(
                &buf.filled()[cursor_start..cursor_new],
                None,
                &mut new_self.buf,
            )
            .unwrap();

        // data is strictly smaller than the encrypted stream, since you need to
        // encode tags as well, so this is always safe.

        // rewrite encrypted data into decrypted data
        let buffer = buf.filled_mut();
        for (old, new) in buffer[cursor_start..].iter_mut().zip(&new_self.buf) {
            *old = *new;
        }
        buf.set_filled(cursor_start + new_self.buf.len());

        res
    }
}

/// Writes the metadata and input stream (in that order) to a file, returning a
/// stream that reads from that file. Accepts a db callback function that is
/// provided the number of bytes written, and an optional on-complete callback
/// that is called with a completed cache entry.
pub(super) async fn write_file<Fut, DbCallback>(
    path: &Path,
    cache_key: CacheKey,
    mut byte_stream: BoxedImageStream,
    metadata: ImageMetadata,
    db_callback: DbCallback,
    on_complete: Option<Sender<(CacheKey, Bytes, ImageMetadata, u64)>>,
) -> Result<(InnerStream, Option<Header>), std::io::Error>
where
    Fut: 'static + Send + Sync + Future<Output = ()>,
    DbCallback: 'static + Send + Sync + FnOnce(u64) -> Fut,
{
    let (tx, rx) = channel(WritingStatus::NotDone);

    let file = {
        let mut write_lock = WRITING_STATUS.write().await;
        let parent = path.parent().expect("The path to have a parent");
        create_dir_all(parent).await?;
        let file = File::create(path).await?; // we need to make sure the file exists and is truncated.
        write_lock.insert(path.to_path_buf(), rx.clone());
        file
    };

    let metadata_string = serde_json::to_string(&metadata).expect("serialization to work");
    let metadata_size = metadata_string.len();

    let (mut writer, maybe_header): (Pin<Box<dyn AsyncWrite + Send>>, _) =
        if let Some((enc, header)) = ENCRYPTION_KEY
            .get()
            .map(|key| SecretStream::init_push(key).expect("Failed to init enc stream"))
        {
            (Box::pin(EncryptedDiskWriter::new(file, enc)), Some(header))
        } else {
            (Box::pin(file), None)
        };

    // need owned variant because async lifetime
    let path_buf = path.to_path_buf();
    tokio::spawn(async move {
        let path_buf = path_buf; // moves path buf into async
        let mut errored = false;
        let mut bytes_written: u64 = 0;
        let mut acc_bytes = BytesMut::new();
        let accumulate = on_complete.is_some();
        writer.write_all(metadata_string.as_bytes()).await?;

        while let Some(bytes) = byte_stream.next().await {
            if let Ok(mut bytes) = bytes {
                if accumulate {
                    acc_bytes.extend(&bytes);
                }

                loop {
                    match writer.write(&bytes).await? {
                        0 => break,
                        n => {
                            bytes.advance(n);
                            bytes_written += n as u64;

                            // We don't care if we don't have receivers
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
            // create anyways, but it should be best effort.
            //
            // We don't care about the result of the call.
            std::mem::drop(remove_file(&path_buf).await);
        } else {
            writer.flush().await?;
            // writer.sync_all().await?; // we need metadata
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

        tokio::spawn(db_callback(bytes_written));

        if let Some(sender) = on_complete {
            tokio::spawn(async move {
                sender
                    .send((cache_key, acc_bytes.freeze(), metadata, bytes_written))
                    .await
            });
        }

        // We don't ever check this, so the return value doesn't matter
        Ok::<_, std::io::Error>(())
    });

    Ok((
        InnerStream::Concurrent(
            ConcurrentFsStream::new(path, metadata_size, WatchStream::new(rx)).await?,
        ),
        maybe_header,
    ))
}

struct EncryptedDiskWriter {
    file: Pin<Box<File>>,
    stream: Option<SecretStream<Push>>,
    encryption_buffer: Vec<u8>,
    write_buffer: Vec<u8>,
}

impl EncryptedDiskWriter {
    fn new(file: File, stream: SecretStream<Push>) -> Self {
        Self {
            file: Box::pin(file),
            stream: Some(stream),
            encryption_buffer: vec![],
            write_buffer: vec![],
        }
    }
}

impl AsyncWrite for EncryptedDiskWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let new_self = Pin::into_inner(self);
        {
            let encryption_buffer = &mut new_self.encryption_buffer;
            if let Some(stream) = new_self.stream.as_mut() {
                stream
                    .push_to_vec(buf, None, Tag::Message, encryption_buffer)
                    .expect("Failed to write encrypted data to buffer");
            }
        }

        new_self.write_buffer.extend(&new_self.encryption_buffer);

        match new_self
            .file
            .as_mut()
            .poll_write(cx, &new_self.write_buffer)
        {
            Poll::Ready(Ok(n)) => {
                new_self.write_buffer.drain(..n);
                Poll::Ready(Ok(n))
            }
            poll => poll,
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        self.file.as_mut().poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        self.as_mut()
            .stream
            .take()
            .map(|stream| stream.finalize(None));
        self.file.as_mut().poll_shutdown(cx)
    }
}

pub struct ConcurrentFsStream {
    /// The File to read from
    reader: Pin<Box<dyn AsyncRead + Send>>,
    /// The channel to get updates from. The writer must send its status, else
    /// this reader will never complete.
    receiver: Pin<Box<WatchStream<WritingStatus>>>,
    /// The number of bytes the reader has read
    bytes_read: u64,
    /// The number of bytes that the writer has reported it has written. If the
    /// writer has not reported yet, this value is None.
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
        Ok(Self::from_reader(Box::pin(file), receiver))
    }

    fn from_reader(
        reader: Pin<Box<dyn AsyncRead + Send>>,
        receiver: WatchStream<WritingStatus>,
    ) -> Self {
        Self {
            reader: Box::pin(reader),
            receiver: Box::pin(receiver),
            bytes_read: 0,
            bytes_total: None,
        }
    }
}

/// Represents some upstream error.
#[derive(Debug)]
pub struct UpstreamError;

impl Error for UpstreamError {}

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
        match self.reader.as_mut().poll_read(cx, &mut buffer) {
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
                self.bytes_total = Some(NonZeroU64::new(n).expect("Stored a 0 byte image?"))
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

#[cfg(test)]
mod storage {
    #[test]
    fn wut() {}
}
