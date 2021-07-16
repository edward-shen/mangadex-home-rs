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
use std::io::SeekFrom;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use actix_web::error::PayloadError;
use async_trait::async_trait;
use bytes::Bytes;
use chacha20::cipher::{NewCipher, StreamCipher};
use chacha20::{Key, XChaCha20, XNonce};
use futures::Future;
use serde::Deserialize;
use sodiumoxide::crypto::stream::xchacha20::{gen_nonce, NONCEBYTES};
use tokio::fs::{create_dir_all, remove_file, File};
use tokio::io::{
    AsyncBufRead, AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt, BufReader,
    ReadBuf,
};
use tokio::sync::mpsc::Sender;
use tokio_util::codec::{BytesCodec, FramedRead};
use tracing::{debug, instrument, warn};

use super::compat::LegacyImageMetadata;
use super::{CacheEntry, CacheKey, CacheStream, ImageMetadata, ENCRYPTION_KEY};

/// Attempts to lookup the file on disk, returning a byte stream if it exists.
/// Note that this could return two types of streams, depending on if the file
/// is in progress of being written to.
#[instrument(level = "debug")]
pub(super) async fn read_file(
    file: File,
) -> Option<Result<(CacheStream, Option<XNonce>, ImageMetadata), std::io::Error>> {
    let mut file_0 = file.try_clone().await.ok()?;
    let file_1 = file.try_clone().await.ok()?;

    // Try reading decrypted header first...
    let mut deserializer = serde_json::Deserializer::from_reader(file.into_std().await);
    let mut maybe_metadata = ImageMetadata::deserialize(&mut deserializer);

    // Failed to parse normally, see if we have a legacy file format
    if maybe_metadata.is_err() {
        file_0.seek(SeekFrom::Start(2)).await.ok()?;
        let mut deserializer = serde_json::Deserializer::from_reader(file_0.into_std().await);
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

        reader = Some(Box::pin(BufReader::new(file_1)));
        parsed_metadata = Some(metadata);
        debug!("Found not encrypted file");
    } else {
        debug!("metadata read failed, trying to see if it's encrypted");
        let mut file = file_1;
        file.seek(SeekFrom::Start(0)).await.ok()?;

        // image is encrypted or corrupt

        // If the encryption key was set, use the encrypted disk reader instead;
        // else, just directly read from file.
        if let Some(key) = ENCRYPTION_KEY.get() {
            let mut nonce_bytes = [0; NONCEBYTES];
            if let Err(e) = file.read_exact(&mut nonce_bytes).await {
                warn!("Found file but failed reading header: {}", e);
                return None;
            }

            debug!("header bytes: {:x?}", nonce_bytes);

            maybe_header = Some(*XNonce::from_slice(&nonce_bytes));
            reader = Some(Box::pin(BufReader::new(EncryptedReader::new(
                file,
                XNonce::from_slice(XNonce::from_slice(&nonce_bytes)),
                key,
            ))));
        }

        parsed_metadata = if let Some(reader) = reader.as_mut() {
            if let Ok(metadata) = reader.as_mut().metadata().await {
                debug!("Successfully parsed encrypted metadata");
                Some(metadata)
            } else {
                debug!("Failed to parse encrypted metadata");
                None
            }
        } else {
            debug!("Failed to read encrypted data");
            None
        };
    }

    // parsed_metadata is either set or unset here. If it's set then we
    // successfully decoded the data; otherwise the file is garbage.

    if let Some(reader) = reader {
        let stream =
            CacheStream::Completed(FramedRead::new(reader as Pin<Box<_>>, BytesCodec::new()));
        parsed_metadata.map(|metadata| Ok((stream, maybe_header, metadata)))
    } else {
        debug!("Reader was invalid, file is corrupt");
        None
    }
}

struct EncryptedReader<R> {
    file: Pin<Box<R>>,
    keystream: XChaCha20,
}

impl<R> EncryptedReader<R> {
    fn new(file: R, nonce: &XNonce, key: &Key) -> Self {
        Self {
            file: Box::pin(file),
            keystream: XChaCha20::new(key, nonce),
        }
    }
}

impl<R: AsyncRead> AsyncRead for EncryptedReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let mut pinned = self.as_mut();
        let previously_read = buf.filled().len();
        let res = pinned.file.as_mut().poll_read(cx, buf);
        let bytes_modified = buf.filled().len() - previously_read;
        pinned.keystream.apply_keystream(
            &mut buf.filled_mut()[previously_read..previously_read + bytes_modified],
        );
        res
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
        MetadataFuture(self).await
    }
}

struct MetadataFuture<'a, R>(Pin<&'a mut R>);

impl<'a, R: AsyncBufRead> Future for MetadataFuture<'a, R> {
    type Output = Result<ImageMetadata, ()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut filled = 0;
        let mut pinned = self.0.as_mut();

        loop {
            let buf = match pinned.as_mut().poll_fill_buf(cx) {
                Poll::Ready(Ok(buffer)) => buffer,
                Poll::Ready(Err(_)) => return Poll::Ready(Err(())),
                Poll::Pending => return Poll::Pending,
            };

            if filled == buf.len() {
                return Poll::Ready(Err(()));
            }

            filled = buf.len();

            let mut reader = serde_json::Deserializer::from_slice(buf).into_iter();
            let (res, bytes_consumed) = match reader.next() {
                Some(Ok(metadata)) => (Poll::Ready(Ok(metadata)), reader.byte_offset()),
                Some(Err(e)) if e.is_eof() => continue,
                Some(Err(_)) | None => return Poll::Ready(Err(())),
            };

            assert_ne!(bytes_consumed, 0);

            // This needs to be outside the loop because we need to drop the
            // reader ref, since that depends on a mut self.
            pinned.as_mut().consume(bytes_consumed);
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
    key: CacheKey,
    data: Bytes,
    metadata: ImageMetadata,
    db_callback: DbCallback,
    on_complete: Option<Sender<CacheEntry>>,
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

    let mut writer: Pin<Box<dyn AsyncWrite + Send>> = if let Some(key) = ENCRYPTION_KEY.get() {
        let nonce = gen_nonce();
        file.write_all(nonce.as_ref()).await?;
        Box::pin(EncryptedDiskWriter::new(
            file,
            XNonce::from_slice(nonce.as_ref()),
            key,
        ))
    } else {
        Box::pin(file)
    };

    let metadata_string = serde_json::to_string(&metadata).expect("serialization to work");
    let metadata_size = metadata_string.len();

    let mut error = writer.write_all(metadata_string.as_bytes()).await.err();
    if error.is_none() {
        debug!("decrypted write {:x?}", &data[..40]);
        error = writer.write_all(&data).await.err();
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

    let on_disk_size = (metadata_size + data.len()) as u64;
    tokio::spawn(db_callback(on_disk_size));

    if let Some(sender) = on_complete {
        tokio::spawn(async move {
            sender
                .send(CacheEntry {
                    key,
                    data,
                    metadata,
                    on_disk_size,
                })
                .await
        });
    }

    Ok(())
}

struct EncryptedDiskWriter {
    file: Pin<Box<File>>,
    keystream: XChaCha20,
    buffer: Vec<u8>,
}

impl EncryptedDiskWriter {
    fn new(file: File, nonce: &XNonce, key: &Key) -> Self {
        Self {
            file: Box::pin(file),
            keystream: XChaCha20::new(key, nonce),
            buffer: vec![],
        }
    }
}

impl AsyncWrite for EncryptedDiskWriter {
    #[inline]
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let pinned = Pin::into_inner(self);

        let old_buffer_size = pinned.buffer.len();
        pinned.buffer.extend_from_slice(buf);
        pinned
            .keystream
            .apply_keystream(&mut pinned.buffer[old_buffer_size..]);
        match pinned.file.as_mut().poll_write(cx, &pinned.buffer) {
            Poll::Ready(Ok(n)) => {
                pinned.buffer.drain(..n);
                Poll::Ready(Ok(buf.len()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            // We have written the data to our buffer, even if we haven't
            // couldn't write the file to disk.
            Poll::Pending => Poll::Ready(Ok(buf.len())),
        }
    }

    #[inline]
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        let pinned = Pin::into_inner(self);

        while !pinned.buffer.is_empty() {
            match pinned.file.as_mut().poll_write(cx, &pinned.buffer) {
                Poll::Ready(Ok(n)) => {
                    pinned.buffer.drain(..n);
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }

        pinned.file.as_mut().poll_flush(cx)
    }

    #[inline]
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match self.as_mut().poll_flush(cx) {
            Poll::Ready(Ok(())) => self.as_mut().file.as_mut().poll_shutdown(cx),
            poll => poll,
        }
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
    use tokio::fs::File;

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn can_read() {
        let mut temp_file = tempfile().unwrap();
        temp_file
            .write_all(
                br#"{"content_type":0,"content_length":708370,"last_modified":"2021-04-13T04:37:41+00:00"}abc"#,
            )
            .unwrap();
        temp_file.seek(SeekFrom::Start(0)).unwrap();
        let temp_file = File::from_std(temp_file);

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
    use tokio::fs::File;

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn can_read_legacy() {
        let mut temp_file = tempfile().unwrap();
        temp_file
            .write_all(
                b"\x00\x5b{\"content_type\":\"image/jpeg\",\"last_modified\":\"Sat, 10 Apr 2021 10:55:22 GMT\",\"size\":117888}abc",
            )
            .unwrap();
        temp_file.seek(SeekFrom::Start(0)).unwrap();
        let temp_file = File::from_std(temp_file);

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

#[cfg(test)]
mod metadata_future {
    use std::{collections::VecDeque, io::ErrorKind};

    use super::*;
    use crate::cache::ImageContentType;
    use chrono::DateTime;

    #[derive(Default)]
    struct TestReader {
        fill_buf_events: VecDeque<Poll<std::io::Result<&'static [u8]>>>,
        consume_events: VecDeque<usize>,
        buffer: Vec<u8>,
    }

    impl TestReader {
        fn new() -> Self {
            Self::default()
        }

        fn push_fill_buf_event(&mut self, event: Poll<std::io::Result<&'static [u8]>>) {
            self.fill_buf_events.push_back(event);
        }

        fn push_consume_event(&mut self, event: usize) {
            self.consume_events.push_back(event);
        }
    }

    impl AsyncRead for TestReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            assert!(self.consume_events.is_empty());
            assert!(self
                .fill_buf_events
                .iter()
                .all(|event| matches!(event, Poll::Pending)));
            buf.put_slice(&self.as_mut().buffer.drain(..).collect::<Vec<_>>());
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncBufRead for TestReader {
        fn poll_fill_buf(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<&[u8]>> {
            let pinned = Pin::into_inner(self);
            match pinned.fill_buf_events.pop_front() {
                Some(Poll::Ready(Ok(bytes))) => {
                    pinned.buffer.extend_from_slice(bytes);
                    String::from_utf8_lossy(&pinned.buffer);
                    return Poll::Ready(Ok(pinned.buffer.as_ref()));
                }
                Some(res @ Poll::Ready(_)) => res,
                Some(Poll::Pending) => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                None => panic!("poll_fill_buf was called but no events are left"),
            }
        }

        fn consume(mut self: Pin<&mut Self>, amt: usize) {
            assert_eq!(self.as_mut().consume_events.pop_front(), Some(amt));
            self.as_mut().buffer.drain(..amt);
        }
    }

    // We don't use the tokio executor here because it relies on epoll, which
    // isn't supported by miri
    #[test]
    fn full_data_is_available() -> Result<(), Box<dyn Error>> {
        let content = br#"{"content_type":0,"content_length":708370,"last_modified":"2021-04-13T04:37:41+00:00"}abc"#;
        let mut reader = Box::pin(BufReader::new(&content[..]));
        let metadata = futures::executor::block_on(async {
            MetadataFuture(reader.as_mut())
                .await
                .map_err(|_| "metadata future returned error")
        })?;

        assert_eq!(metadata.content_type, Some(ImageContentType::Png));
        assert_eq!(metadata.content_length, Some(708370));
        assert_eq!(
            metadata.last_modified,
            Some(DateTime::parse_from_rfc3339("2021-04-13T04:37:41+00:00")?)
        );

        let mut buf = vec![];
        futures::executor::block_on(reader.read_to_end(&mut buf))?;

        assert_eq!(&buf, b"abc");
        Ok(())
    }

    #[test]
    fn data_is_immediately_available_in_chunks() -> Result<(), Box<dyn Error>> {
        let mut test_reader = TestReader::new();
        let msg_0 = br#"{"content_type":0,"content_length":708370,"last_"#;
        let msg_1 = br#"modified":"2021-04-13T04:37:41+00:00"}abc"#;
        test_reader.push_fill_buf_event(Poll::Ready(Ok(msg_0)));
        test_reader.push_fill_buf_event(Poll::Ready(Ok(msg_1)));
        test_reader.push_consume_event(86);
        let mut reader = Box::pin(test_reader);
        let metadata = futures::executor::block_on(async {
            MetadataFuture(reader.as_mut())
                .await
                .map_err(|_| "metadata future returned error")
        })?;

        assert_eq!(metadata.content_type, Some(ImageContentType::Png));
        assert_eq!(metadata.content_length, Some(708370));
        assert_eq!(
            metadata.last_modified,
            Some(DateTime::parse_from_rfc3339("2021-04-13T04:37:41+00:00")?)
        );

        let mut buf = vec![];
        futures::executor::block_on(reader.read_to_end(&mut buf))?;

        assert_eq!(&buf, b"abc");
        Ok(())
    }

    #[test]
    fn data_is_available_in_chunks() -> Result<(), Box<dyn Error>> {
        let mut test_reader = TestReader::new();
        let msg_0 = br#"{"content_type":0,"content_length":708370,"last_"#;
        let msg_1 = br#"modified":"2021-04-13T04:37:41+00:00"}abc"#;
        test_reader.push_fill_buf_event(Poll::Pending);
        test_reader.push_fill_buf_event(Poll::Ready(Ok(msg_0)));
        test_reader.push_fill_buf_event(Poll::Pending);
        test_reader.push_fill_buf_event(Poll::Ready(Ok(msg_1)));
        test_reader.push_fill_buf_event(Poll::Pending);
        test_reader.push_consume_event(86);
        let mut reader = Box::pin(test_reader);
        let metadata = futures::executor::block_on(async {
            MetadataFuture(reader.as_mut())
                .await
                .map_err(|_| "metadata future returned error")
        })?;

        assert_eq!(metadata.content_type, Some(ImageContentType::Png));
        assert_eq!(metadata.content_length, Some(708370));
        assert_eq!(
            metadata.last_modified,
            Some(DateTime::parse_from_rfc3339("2021-04-13T04:37:41+00:00")?)
        );

        let mut buf = vec![];
        futures::executor::block_on(reader.read_to_end(&mut buf))?;

        assert_eq!(&buf, b"abc");
        Ok(())
    }

    #[test]
    fn underlying_reader_reports_err() -> Result<(), Box<dyn Error>> {
        let mut test_reader = TestReader::new();
        let msg_0 = br#"{"content_type":0,"content_length":708370,"last_"#;
        test_reader.push_fill_buf_event(Poll::Pending);
        test_reader.push_fill_buf_event(Poll::Ready(Ok(msg_0)));
        test_reader.push_fill_buf_event(Poll::Pending);
        test_reader.push_fill_buf_event(Poll::Ready(Err(std::io::Error::new(
            ErrorKind::Other,
            "sup",
        ))));
        let mut reader = Box::pin(test_reader);
        let metadata = futures::executor::block_on(MetadataFuture(reader.as_mut()));
        assert!(metadata.is_err());
        Ok(())
    }

    #[test]
    fn underlying_reader_reports_early_eof() -> Result<(), Box<dyn Error>> {
        let mut test_reader = TestReader::new();
        test_reader.push_fill_buf_event(Poll::Ready(Ok(&[])));
        let mut reader = Box::pin(test_reader);
        let metadata = futures::executor::block_on(MetadataFuture(reader.as_mut()));
        assert!(metadata.is_err());
        Ok(())
    }

    #[test]
    fn invalid_metadata() -> Result<(), Box<dyn Error>> {
        let mut test_reader = TestReader::new();
        // content type is incorrect, should be a number
        let msg_0 = br#"{"content_type":"foo","content_length":708370,"last_modified":"2021-04-13T04:37:41+00:00"}"#;
        test_reader.push_fill_buf_event(Poll::Ready(Ok(msg_0)));
        let mut reader = Box::pin(test_reader);
        let metadata = futures::executor::block_on(MetadataFuture(reader.as_mut()));
        assert!(metadata.is_err());
        Ok(())
    }
}
