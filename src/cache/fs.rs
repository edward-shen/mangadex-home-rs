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
use std::path::Path;
use tokio_stream::Stream;
use tokio_uring::buf::IoBuf;
use tokio_uring::buf::IoBufMut;
use tokio_uring::buf::Slice;
use tokio_uring::BufResult;

use actix_web::error::PayloadError;
use chacha20::cipher::{NewCipher, StreamCipher};
use chacha20::{Key, XChaCha20, XNonce};
use futures::Future;
use sodiumoxide::crypto::stream::xchacha20::{gen_nonce, NONCEBYTES};
use tokio::fs::{create_dir_all, remove_file};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf};
use tokio::sync::mpsc::Sender;
use tokio_uring::fs::File;
use tokio_util::io::ReaderStream;
use tracing::{debug, instrument, warn};

use super::compat::LegacyImageMetadata;
use super::{CacheEntry, CacheKey, CacheStream, ImageMetadata, ENCRYPTION_KEY};

/// Attempts to lookup the file on disk, returning a byte stream if it exists.
/// Note that this could return two types of streams, depending on if the file
/// is in progress of being written to.
// #[instrument(level = "debug")]
pub(super) async fn read_file(
    file: File,
) -> Option<Result<(Reader, Option<XNonce>, ImageMetadata), std::io::Error>> {
    // Try reading decrypted header first...
    let mut buf = vec![0; 128];
    let res = file.read_at(buf, 0).await;
    let maybe_n = res.0;

    if let Ok(n) = maybe_n {
        let buf = res.1;
        let metadata_len = u16::from_le_bytes(*(buf.as_ptr() as *const [u8; 2]));
        // metadata should be smaller than 128 bytes
        assert!(n <= buf.len());
        let maybe_data = if let Ok(data) = serde_json::from_slice::<ImageMetadata>(&buf[2..n]) {
            Some(data)
        } else {
            serde_json::from_slice(&buf[2..n])
                .map(LegacyImageMetadata::into)
                .ok()
        };

        if let Some(data) = maybe_data {
            if ENCRYPTION_KEY.get().is_some() {
                // invalidate cache since we're running in at-rest encryption and
                // the file wasn't encrypted.
                warn!("Found file but was not encrypted!");
                return None;
            }

            debug!("Found not encrypted file");
            return Some(Ok((Reader::Raw(file), None, data)));
        }
    }

    // image is encrypted or corrupt
    debug!("metadata read failed, trying to see if it's encrypted");

    let key = match ENCRYPTION_KEY.get() {
        Some(key) => key,
        None => {
            warn!("Encryption is disabled!");
            return None;
        }
    };

    let nonce_bytes = match file.read_at(vec![0; NONCEBYTES], 0).await {
        (Ok(n), bytes) => {
            assert_eq!(n, NONCEBYTES);
            bytes
        }
        (Err(e), _) => {
            warn!("Found file but failed reading header: {}", e);
            return None;
        }
    };

    debug!("header bytes: {:x?}", nonce_bytes);

    let maybe_header = *XNonce::from_slice(&nonce_bytes);
    let mut reader = EncryptedReader::new(file, XNonce::from_slice(&nonce_bytes), key);

    let parsed_metadata = {
        if let Ok(metadata) = reader.metadata().await {
            debug!("Successfully parsed encrypted metadata");
            Some(metadata)
        } else {
            debug!("Failed to parse encrypted metadata");
            None
        }
    };

    parsed_metadata.map(|metadata| Ok((Reader::Encrypted(reader), Some(maybe_header), metadata)))
}

enum Reader {
    Encrypted(EncryptedReader),
    Raw(File),
}

impl Stream for Reader {
    type Item = ();
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::option::Option<<Self as futures::Stream>::Item>> {
        todo!()
    }
}

struct EncryptedReader {
    file: File,
    keystream: XChaCha20,
    buffer: Box<[u8; 4096]>,
}

impl EncryptedReader {
    fn new(file: File, nonce: &XNonce, key: &Key) -> Self {
        Self {
            file,
            keystream: XChaCha20::new(key, nonce),
            buffer: Box::new([0; 4096]),
        }
    }

    async fn metadata(&mut self) -> Result<ImageMetadata, Box<dyn std::error::Error>> {
        todo!()
    }
}

/// Writes the metadata and input stream (in that order) to a file, returning a
/// stream that reads from that file. Accepts a db callback function that is
/// provided the number of bytes written, and an optional on-complete callback
/// that is called with a completed cache entry.
pub(super) async fn write_file<Fut, DbCallback>(
    path: &Path,
    key: CacheKey,
    mut data: Vec<u8>,
    metadata: ImageMetadata,
    db_callback: DbCallback,
    on_complete: Option<Sender<CacheEntry>>,
) -> Result<(), std::io::Error>
where
    Fut: 'static + Send + Future<Output = ()>,
    DbCallback: 'static + Send + FnOnce(u64) -> Fut,
{
    let file = {
        let parent = path.parent().expect("The path to have a parent");
        create_dir_all(parent).await?;
        let file = File::create(path).await?; // we need to make sure the file exists and is truncated.
        file
    };

    let mut writer: WriterType = if let Some(key) = ENCRYPTION_KEY.get() {
        let nonce = gen_nonce();
        let mut buf: Slice<_> = nonce.as_ref().to_vec().slice(..);
        let mut index: usize = 0;

        loop {
            match file.write_at(buf, index as u64).await {
                (Ok(0), buf) => break,
                (Ok(n), remaining) => {
                    index = n;
                    buf = remaining.into_inner().slice(index..);
                }
                (e, _) => return e.map(|_| ()),
            }
        }

        WriterType::Encrypted(
            EncryptedDiskWriter::new(file, XNonce::from_slice(nonce.as_ref()), key),
            0,
        )
    } else {
        WriterType::Raw(file, 0)
    };

    let metadata_string = serde_json::to_string(&metadata).expect("serialization to work");
    let metadata_size = metadata_string.len();

    let mut error = writer.write_all(metadata_string.into_bytes()).await.0.err();
    if error.is_none() {
        debug!("decrypted write {:x?}", &data[..40]);
        let res = writer.write_all(data).await;
        error = res.0.err();
        data = res.1;
    }

    if let Some(e) = error {
        // It's ok if the deleting the file fails, since we truncate on
        // create anyways, but it should be best effort.
        //
        // We don't care about the result of the call.
        std::mem::drop(remove_file(path).await);
        return Err(e);
    }

    writer.sync_and_close().await?;
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

enum WriterType {
    Encrypted(EncryptedDiskWriter, u64),
    Raw(File, u64),
}

impl WriterType {
    async fn write_all<T: IoBufMut>(&mut self, data: T) -> BufResult<usize, T> {
        match self {
            Self::Encrypted(writer, pos) => {
                // todo: encrypt
                let mut data = data.slice(..);
                loop {
                    match writer.write_at(data, *pos).await {
                        (Ok(0), buf) => break,
                        (Ok(n), buf) => {
                            *pos = n as u64;
                            data = buf.into_inner().slice(n..);
                        }
                        _ => todo!(),
                    }
                }
            }
            Self::Raw(writer, pos) => {
                let mut data = data.slice(..);
                loop {
                    match writer.write_at(data, *pos).await {
                        (Ok(0), buf) => break,
                        (Ok(n), buf) => {
                            *pos = n as u64;
                            data = buf.into_inner().slice(n..);
                        }
                        _ => todo!(),
                    }
                }
            }
        }

        todo!()
    }

    async fn sync_and_close(self) -> std::io::Result<()> {
        match self {
            Self::Encrypted(writer, _) => {
                writer.sync_all().await?;
                writer.close().await
            }
            Self::Raw(writer, _) => {
                writer.sync_all().await?;
                writer.close().await
            }
        }
    }
}

struct EncryptedDiskWriter {
    file: File,
    keystream: XChaCha20,
}

impl EncryptedDiskWriter {
    fn new(file: File, nonce: &XNonce, key: &Key) -> Self {
        Self {
            file,
            keystream: XChaCha20::new(key, nonce),
        }
    }

    async fn write_at<T: IoBuf>(&self, data: T, pos: u64) -> BufResult<usize, T> {
        self.file.write_at(data, pos).await
    }

    async fn sync_all(&self) -> std::io::Result<()> {
        self.file.sync_all().await
    }

    async fn close(self) -> std::io::Result<()> {
        self.file.close().await
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
    use tokio_uring::fs::File;

    #[tokio::test]
    #[cfg_attr(any(target = "unix", miri), ignore)]
    async fn can_read_legacy() {
        let mut temp_file = tempfile().unwrap();
        temp_file
            .write_all(
                b"\x00\x5b{\"content_type\":\"image/jpeg\",\"last_modified\":\"Sat, 10 Apr 2021 10:55:22 GMT\",\"size\":117888}abc",
            )
            .unwrap();
        temp_file.seek(SeekFrom::Start(0)).unwrap();

        let fd = temp_file.as_raw_fd();
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
