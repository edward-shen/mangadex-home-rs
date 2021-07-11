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
use bytes::Bytes;
use futures::Future;
use log::{debug, warn};
use serde::{Deserialize, Serialize};
use sodiumoxide::crypto::secretstream::{
    Header, Pull, Push, Stream as SecretStream, Tag, HEADERBYTES,
};
use tokio::fs::{create_dir_all, remove_file, File};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::sync::mpsc::Sender;
use tokio_util::codec::{BytesCodec, FramedRead};

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
    let mut reader: Option<Pin<Box<dyn AsyncRead + Send>>> = None;
    if let Ok(metadata) = maybe_metadata {
        // image is decrypted
        if ENCRYPTION_KEY.get().is_some() {
            // invalidate cache since we're running in at-rest encryption and
            // the file wasn't encrypted.
            warn!("Found file but was not encrypted!");
            return None;
        }

        reader = Some(Box::pin(File::from_std(file_1)));
        parsed_metadata = Some(metadata);
        debug!("Found not encrypted file");
    } else {
        let mut file = File::from_std(file_1);
        let file_0 = file.try_clone().await.unwrap();

        // image is encrypted or corrupt

        // If the encryption key was set, use the encrypted disk reader instead;
        // else, just directly read from file.
        if let Some(key) = ENCRYPTION_KEY.get() {
            let mut header_bytes = [0; HEADERBYTES];
            if let Err(e) = file.read_exact(&mut header_bytes).await {
                warn!("Found file but failed reading header: {}", e);
                return None;
            }

            let file_header = if let Some(header) = Header::from_slice(&header_bytes) {
                header
            } else {
                warn!("Found file, but encrypted header was invalid. Assuming corrupted!");
                return None;
            };

            let secret_stream = if let Ok(stream) = SecretStream::init_pull(&file_header, key) {
                stream
            } else {
                warn!("Failed to init secret stream with key and header. Assuming corrupted!");
                return None;
            };

            maybe_header = Some(file_header);

            reader = Some(Box::pin(EncryptedDiskReader::new(file, secret_stream)));
        }

        let mut deserializer = serde_json::Deserializer::from_reader(file_0.into_std().await);
        parsed_metadata = ImageMetadata::deserialize(&mut deserializer).ok();

        if parsed_metadata.is_some() {
            debug!("Found encrypted file");
        }
    }

    // parsed_metadata is either set or unset here. If it's set then we
    // successfully decoded the data; otherwise the file is garbage.

    if let Some(reader) = reader {
        let stream = InnerStream::Completed(FramedRead::new(reader, BytesCodec::new()));
        parsed_metadata.map(|metadata| Ok((stream, maybe_header, metadata)))
    } else {
        debug!("Reader was invalid, file is corrupt");
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
    bytes: Bytes,
    metadata: ImageMetadata,
    db_callback: DbCallback,
    on_complete: Option<Sender<(CacheKey, Bytes, ImageMetadata, u64)>>,
) -> Result<(), std::io::Error>
where
    Fut: 'static + Send + Sync + Future<Output = ()>,
    DbCallback: 'static + Send + Sync + FnOnce(u64) -> Fut,
{
    let file = {
        let parent = path.parent().expect("The path to have a parent");
        create_dir_all(parent).await?;
        let file = File::create(path).await?; // we need to make sure the file exists and is truncated.
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

    let mut error = if let Some(header) = maybe_header {
        writer.write_all(header.as_ref()).await.err()
    } else {
        None
    };

    if error.is_none() {
        error = writer.write_all(metadata_string.as_bytes()).await.err();
    }
    if error.is_none() {
        error = error.or(writer.write_all(&bytes).await.err());
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
        self.as_mut()
            .stream
            .take()
            .map(|stream| stream.finalize(None));
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
                content_length: Some(708370),
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
                content_length: Some(117888),
                content_type: Some(ImageContentType::Jpeg),
                last_modified: Some(
                    DateTime::parse_from_rfc2822("Sat, 10 Apr 2021 10:55:22 GMT").unwrap()
                )
            }
        );
    }
}
