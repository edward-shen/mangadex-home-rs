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

use std::error::Error;
use std::fmt::Display;
use std::io::{Seek, SeekFrom};
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use actix_web::error::PayloadError;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Future;
use serde::{Deserialize, Serialize};
use sodiumoxide::crypto::secretstream::{
    Header, Pull, Push, Stream as SecretStream, Tag, HEADERBYTES,
};
use tokio::fs::{create_dir_all, remove_file, File};
use tokio::io::{
    AsyncBufRead, AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt, BufReader,
    ReadBuf,
};
use tokio::sync::mpsc::Sender;
use tokio_util::codec::{BytesCodec, FramedRead};
use tracing::{debug, instrument, warn};

use super::compat::LegacyImageMetadata;
use super::{CacheKey, ImageMetadata, InnerStream, ENCRYPTION_KEY};

#[derive(Serialize, Deserialize)]
pub enum OnDiskMetadata {
    Encrypted(Header, ImageMetadata),
    Plaintext(ImageMetadata),
}

/// Attempts to lookup the file on disk, returning a byte stream if it exists.
/// Note that this could return two types of streams, depending on if the file
/// is in progress of being written to.
#[inline]
pub(super) async fn read_file_from_path(
    path: &Path,
) -> Option<Result<(InnerStream, Option<Header>, ImageMetadata), std::io::Error>> {
    read_file(std::fs::File::open(path).ok()?).await
}

#[instrument(level = "debug")]
async fn read_file(
    file: std::fs::File,
) -> Option<Result<(InnerStream, Option<Header>, ImageMetadata), std::io::Error>> {
    let mut file_0 = file.try_clone().unwrap();
    let file_1 = file.try_clone().unwrap();

    // Try reading decrypted header first...
    let mut deserializer = serde_json::Deserializer::from_reader(file);
    let mut maybe_metadata = ImageMetadata::deserialize(&mut deserializer);

    // Failed to parse normally, see if we have a legacy file format
    if maybe_metadata.is_err() {
        file_0.seek(SeekFrom::Start(2)).ok()?;
        let mut deserializer = serde_json::Deserializer::from_reader(file_0);
        maybe_metadata =
            LegacyImageMetadata::deserialize(&mut deserializer).map(LegacyImageMetadata::into);
    }

    let parsed_metadata;
    let mut maybe_header = None;
    let mut reader: Option<Pin<Box<dyn MetadataFetch + Send>>> = None;
    if let Ok(metadata) = maybe_metadata {
        // image is decrypted
        if ENCRYPTION_KEY.get().is_some() {
            // invalidate cache since we're running in at-rest encryption and
            // the file wasn't encrypted.
            warn!("Found file but was not encrypted!");
            return None;
        }

        reader = Some(Box::pin(BufReader::new(File::from_std(file_1))));
        parsed_metadata = Some(metadata);
        debug!("Found not encrypted file");
    } else {
        debug!("metadata read failed, trying to see if it's encrypted");
        let mut file = File::from_std(file_1);
        file.seek(SeekFrom::Start(0)).await.ok()?;

        // image is encrypted or corrupt

        // If the encryption key was set, use the encrypted disk reader instead;
        // else, just directly read from file.
        if let Some(key) = ENCRYPTION_KEY.get() {
            let mut header_bytes = [0; HEADERBYTES];
            if let Err(e) = file.read_exact(&mut header_bytes).await {
                warn!("Found file but failed reading header: {}", e);
                return None;
            }

            dbg!(&header_bytes);

            debug!("header bytes: {:x?}", header_bytes);

            let file_header = if let Some(header) = Header::from_slice(&header_bytes) {
                header
            } else {
                warn!("Found file, but encrypted header was invalid. Assuming corrupted!");
                return None;
            };

            let secret_stream = if let Ok(stream) = SecretStream::init_pull(&file_header, key) {
                debug!("Valid header found!");
                stream
            } else {
                warn!("Failed to init secret stream with key and header. Assuming corrupted!");
                return None;
            };

            maybe_header = Some(file_header);
            reader = Some(Box::pin(EncryptedDiskReader::new(file, secret_stream)));
        }

        parsed_metadata = if let Some(reader) = reader.as_mut() {
            debug!("trying to read metadata");
            dbg!(reader.as_mut().metadata().await.ok())
        } else {
            debug!("Failed to read encrypted data");
            None
        };
    }

    // parsed_metadata is either set or unset here. If it's set then we
    // successfully decoded the data; otherwise the file is garbage.

    if let Some(reader) = reader {
        let stream =
            InnerStream::Completed(FramedRead::new(reader as Pin<Box<_>>, BytesCodec::new()));
        parsed_metadata.map(|metadata| Ok((stream, maybe_header, metadata)))
    } else {
        debug!("Reader was invalid, file is corrupt");
        None
    }
}

struct EncryptedDiskReader {
    file: Pin<Box<File>>,
    stream: SecretStream<Pull>,
    // Bytes we read from the secret stream
    read_buffer: Box<[u8; 4096]>,
    decryption_buffer: Vec<u8>,
    // Bytes we write out to the read buf
    write_buffer: Vec<u8>,
}

impl EncryptedDiskReader {
    fn new(file: File, stream: SecretStream<Pull>) -> Self {
        Self {
            file: Box::pin(file),
            stream,
            read_buffer: Box::new([0; 4096]),
            decryption_buffer: Vec::with_capacity(4096),
            write_buffer: Vec::with_capacity(4096),
        }
    }
}

#[async_trait]
pub trait MetadataFetch: AsyncBufRead {
    async fn metadata(mut self: Pin<&mut Self>) -> Result<ImageMetadata, ()>;
}

#[async_trait]
impl<R: AsyncBufRead + Send> MetadataFetch for R {
    #[inline]
    async fn metadata(mut self: Pin<&mut Self>) -> Result<ImageMetadata, ()> {
        MetadataFuture(&mut self).await
    }
}

impl AsyncRead for EncryptedDiskReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // First, try and read from the underlying file.
        let pinned_self = Pin::into_inner(self);
        let mut read_buf = ReadBuf::new(pinned_self.read_buffer.as_mut());
        let read_res = pinned_self.file.as_mut().poll_read(cx, &mut read_buf);

        // If the file
        if read_res.is_pending() {
            return Poll::Pending;
        }

        if pinned_self
            .stream
            .pull_to_vec(read_buf.filled(), None, &mut pinned_self.decryption_buffer)
            .is_err()
        {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to decrypt data",
            )));
        }

        pinned_self
            .write_buffer
            .extend_from_slice(&pinned_self.decryption_buffer);

        // find the amount of bytes we can put into the output buffer.
        let bytes_to_write = buf.remaining().min(pinned_self.write_buffer.len());
        buf.put_slice(
            &pinned_self
                .write_buffer
                .drain(..bytes_to_write)
                .collect::<Vec<_>>(),
        );

        Poll::Ready(Ok(()))
    }
}

impl AsyncBufRead for EncryptedDiskReader {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<&[u8]>> {
        // First, try and read from the underlying file.
        let pinned_self = Pin::into_inner(self);
        let mut read_buf = ReadBuf::new(pinned_self.read_buffer.as_mut());
        let read_res = pinned_self.file.as_mut().poll_read(cx, &mut read_buf);

        // If the file
        if read_res.is_pending() {
            return Poll::Pending;
        }

        dbg!(&read_buf.filled().len());
        if pinned_self
            .stream
            .pull_to_vec(read_buf.filled(), None, &mut pinned_self.decryption_buffer)
            .is_err()
        {
            dbg!(line!());
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to decrypt data",
            )));
        }

        pinned_self
            .write_buffer
            .extend_from_slice(&pinned_self.decryption_buffer);

        Poll::Ready(Ok(&pinned_self.write_buffer))
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        self.as_mut().write_buffer.drain(..amt);
    }
}

struct MetadataFuture<'a, R>(&'a mut Pin<&'a mut R>);

impl<'a, R: AsyncBufRead> Future for MetadataFuture<'a, R> {
    type Output = Result<ImageMetadata, ()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut filled = 0;
        loop {
            let buf = match self.0.as_mut().poll_fill_buf(cx) {
                Poll::Ready(Ok(buffer)) => buffer,
                Poll::Ready(Err(e)) => {
                    dbg!(e);
                    return Poll::Ready(Err(()));
                }
                Poll::Pending => return Poll::Pending,
            };

            if filled == buf.len() {
                dbg!(line!());
                return Poll::Ready(Err(()));
            } else {
                filled = buf.len();
            }

            let mut reader = serde_json::Deserializer::from_slice(buf).into_iter();
            let (res, bytes_consumed) = match reader.next() {
                Some(Ok(metadata)) => (Poll::Ready(Ok(metadata)), reader.byte_offset()),
                Some(Err(e)) if e.is_eof() => {
                    continue;
                }
                Some(Err(_)) | None => {
                    dbg!(line!());
                    return Poll::Ready(Err(()));
                }
            };

            // This needs to be outside the loop because we need to drop the
            // reader ref, since that depends on a mut self.
            self.0.as_mut().consume(bytes_consumed);
            return res;
        }
    }
}

/// Writes the metadata and input stream (in that order) to a file, returning a
/// stream that reads from that file. Accepts a db callback function that is
/// provided the number of bytes written, and an optional on-complete callback
/// that is called with a completed cache entry.
pub(super) async fn write_file<Fut, DbCallback>(
    path: &Path,
    cache_key: CacheKey,
    bytes: Bytes,
    metadata: ImageMetadata,
    db_callback: DbCallback,
    on_complete: Option<Sender<(CacheKey, Bytes, ImageMetadata, u64)>>,
) -> Result<(), std::io::Error>
where
    Fut: 'static + Send + Sync + Future<Output = ()>,
    DbCallback: 'static + Send + Sync + FnOnce(u64) -> Fut,
{
    let mut file = {
        let parent = path.parent().expect("The path to have a parent");
        create_dir_all(parent).await?;
        let file = File::create(path).await?; // we need to make sure the file exists and is truncated.
        file
    };

    let mut writer: Pin<Box<dyn AsyncWrite + Send>> = if let Some((enc, header)) = ENCRYPTION_KEY
        .get()
        .map(|key| SecretStream::init_push(key).expect("Failed to init enc stream"))
    {
        file.write_all(dbg!(header.as_ref())).await?;
        Box::pin(EncryptedDiskWriter::new(file, enc))
    } else {
        Box::pin(file)
    };

    let metadata_string = serde_json::to_string(&metadata).expect("serialization to work");
    let metadata_size = metadata_string.len();

    let mut error = writer.write_all(metadata_string.as_bytes()).await.err();
    if error.is_none() {
        error = writer.write_all(&bytes).await.err();
    }

    if let Some(e) = error {
        // It's ok if the deleting the file fails, since we truncate on
        // create anyways, but it should be best effort.
        //
        // We don't care about the result of the call.
        std::mem::drop(remove_file(path).await);
        return Err(e);
    }

    writer.flush().await?;
    debug!("writing to file done");

    let bytes_written = (metadata_size + bytes.len()) as u64;
    tokio::spawn(db_callback(bytes_written));

    if let Some(sender) = on_complete {
        tokio::spawn(async move {
            sender
                .send((cache_key, bytes, metadata, bytes_written))
                .await
        });
    }

    Ok(())
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

        if let Some(stream) = new_self.stream.as_mut() {
            stream
                .push_to_vec(buf, None, Tag::Message, &mut new_self.encryption_buffer)
                .expect("Failed to write encrypted data to buffer");
        }

        new_self.write_buffer.extend(&new_self.encryption_buffer);

        match new_self
            .file
            .as_mut()
            .poll_write(cx, &new_self.write_buffer)
        {
            Poll::Ready(Ok(n)) => {
                new_self.write_buffer.drain(..n);
                // We buffered all the bytes that were provided to use.
                Poll::Ready(Ok(buf.len()))
            }
            poll => poll,
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        if self.as_ref().write_buffer.is_empty() {
            self.file.as_mut().poll_flush(cx)
        } else {
            let new_self = Pin::into_inner(self);
            let buffer = new_self.write_buffer.as_ref();
            match new_self.file.as_mut().poll_write(cx, buffer) {
                Poll::Ready(res) => {
                    let n = res?;
                    new_self.write_buffer.drain(..n);
                    // We're immediately ready to do some more flushing!
                    cx.waker().wake_by_ref();
                    // Return pending here because we still need to flush the
                    // file
                    Poll::Pending
                }
                Poll::Pending => Poll::Pending,
            }
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let maybe_bytes = self
            .as_mut()
            .stream
            .take()
            .map(|stream| stream.finalize(None));

        // If we've yet to finalize the stream, finalize it and add the bytes to
        // our writer buffer.
        if let Some(Ok(bytes)) = maybe_bytes {
            // We just need to push it into the buffer, we don't really care
            // about the result, since we can check later
            let _ = self.as_mut().poll_write(cx, &bytes);
        }

        // Now wait for us to fully flush out our write buffer
        if !self.write_buffer.is_empty() {
            return self.poll_flush(cx);
        }

        // Write buffer is empty, flush file
        self.file.as_mut().poll_shutdown(cx)
    }
}

/// Represents some upstream error.
#[derive(Debug, PartialEq, Eq)]
pub struct UpstreamError;

impl Error for UpstreamError {}

impl Display for UpstreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "An upstream error occurred")
    }
}

impl From<UpstreamError> for actix_web::Error {
    #[inline]
    fn from(_: UpstreamError) -> Self {
        PayloadError::Incomplete(None).into()
    }
}

#[cfg(test)]
mod read_file {
    use crate::cache::{ImageContentType, ImageMetadata};

    use super::read_file;
    use bytes::Bytes;
    use chrono::DateTime;
    use futures::StreamExt;
    use std::io::{Seek, SeekFrom, Write};
    use tempfile::tempfile;

    #[tokio::test]
    async fn can_read() {
        let mut temp_file = tempfile().unwrap();
        temp_file
            .write_all(
                br#"{"content_type":0,"content_length":708370,"last_modified":"2021-04-13T04:37:41+00:00"}abc"#,
            )
            .unwrap();
        temp_file.seek(SeekFrom::Start(0)).unwrap();

        let (inner_stream, maybe_header, metadata) = read_file(temp_file).await.unwrap().unwrap();

        let foo: Vec<_> = inner_stream.collect().await;
        assert_eq!(foo, vec![Ok(Bytes::from("abc"))]);
        assert!(maybe_header.is_none());
        assert_eq!(
            metadata,
            ImageMetadata {
                content_length: Some(708_370),
                content_type: Some(ImageContentType::Png),
                last_modified: Some(
                    DateTime::parse_from_rfc3339("2021-04-13T04:37:41+00:00").unwrap()
                )
            }
        );
    }
}

#[cfg(test)]
mod read_file_compat {
    use crate::cache::{ImageContentType, ImageMetadata};

    use super::read_file;
    use bytes::Bytes;
    use chrono::DateTime;
    use futures::StreamExt;
    use std::io::{Seek, SeekFrom, Write};
    use tempfile::tempfile;

    #[tokio::test]
    async fn can_read_legacy() {
        let mut temp_file = tempfile().unwrap();
        temp_file
            .write_all(
                b"\x00\x5b{\"content_type\":\"image/jpeg\",\"last_modified\":\"Sat, 10 Apr 2021 10:55:22 GMT\",\"size\":117888}abc",
            )
            .unwrap();
        temp_file.seek(SeekFrom::Start(0)).unwrap();

        let (inner_stream, maybe_header, metadata) = read_file(temp_file).await.unwrap().unwrap();

        let foo: Vec<_> = inner_stream.collect().await;
        assert_eq!(foo, vec![Ok(Bytes::from("abc"))]);
        assert!(maybe_header.is_none());
        assert_eq!(
            metadata,
            ImageMetadata {
                content_length: Some(117_888),
                content_type: Some(ImageContentType::Jpeg),
                last_modified: Some(
                    DateTime::parse_from_rfc2822("Sat, 10 Apr 2021 10:55:22 GMT").unwrap()
                )
            }
        );
    }
}
