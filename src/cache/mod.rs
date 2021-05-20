use std::fmt::Display;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};

use actix_web::http::HeaderValue;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, FixedOffset};
use fs::ConcurrentFsStream;
use futures::{Stream, StreamExt};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use sodiumoxide::crypto::secretstream::{Header, Key, Pull, Stream as SecretStream};
use thiserror::Error;
use tokio::io::AsyncRead;
use tokio::sync::mpsc::Sender;
use tokio_util::codec::{BytesCodec, FramedRead};

pub use disk::DiskCache;
pub use fs::UpstreamError;
pub use mem::MemoryCache;

pub static ENCRYPTION_KEY: OnceCell<Key> = OnceCell::new();

mod disk;
mod fs;
pub mod mem;

#[derive(PartialEq, Eq, Hash, Clone)]
pub struct CacheKey(pub String, pub String, pub bool);

impl Display for CacheKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.2 {
            write!(f, "saver/{}/{}", self.0, self.1)
        } else {
            write!(f, "data/{}/{}", self.0, self.1)
        }
    }
}

impl From<CacheKey> for PathBuf {
    #[inline]
    fn from(key: CacheKey) -> Self {
        key.to_string().into()
    }
}

impl From<&CacheKey> for PathBuf {
    #[inline]
    fn from(key: &CacheKey) -> Self {
        key.to_string().into()
    }
}

#[derive(Clone)]
pub struct CachedImage(pub Bytes);

#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct ImageMetadata {
    pub content_type: Option<ImageContentType>,
    pub content_length: Option<u32>,
    pub last_modified: Option<DateTime<FixedOffset>>,
}

// Confirmed by Ply to be these types: https://link.eddie.sh/ZXfk0
#[derive(Copy, Clone, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum ImageContentType {
    Png = 0,
    Jpeg,
    Gif,
}

pub struct InvalidContentType;

impl FromStr for ImageContentType {
    type Err = InvalidContentType;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "image/png" => Ok(Self::Png),
            "image/jpeg" => Ok(Self::Jpeg),
            "image/gif" => Ok(Self::Gif),
            _ => Err(InvalidContentType),
        }
    }
}

impl AsRef<str> for ImageContentType {
    #[inline]
    fn as_ref(&self) -> &str {
        match self {
            Self::Png => "image/png",
            Self::Jpeg => "image/jpeg",
            Self::Gif => "image/gif",
        }
    }
}

#[allow(clippy::pub_enum_variant_names)]
#[derive(Debug)]
pub enum ImageRequestError {
    InvalidContentType,
    InvalidContentLength,
    InvalidLastModified,
}

impl ImageMetadata {
    pub fn new(
        content_type: Option<HeaderValue>,
        content_length: Option<HeaderValue>,
        last_modified: Option<HeaderValue>,
    ) -> Result<Self, ImageRequestError> {
        Ok(Self {
            content_type: content_type
                .map(|v| match v.to_str() {
                    Ok(v) => ImageContentType::from_str(v),
                    Err(_) => Err(InvalidContentType),
                })
                .transpose()
                .map_err(|_| ImageRequestError::InvalidContentType)?,
            content_length: content_length
                .map(|header_val| {
                    header_val
                        .to_str()
                        .map_err(|_| ImageRequestError::InvalidContentLength)?
                        .parse()
                        .map_err(|_| ImageRequestError::InvalidContentLength)
                })
                .transpose()?,
            last_modified: last_modified
                .map(|header_val| {
                    DateTime::parse_from_rfc2822(
                        header_val
                            .to_str()
                            .map_err(|_| ImageRequestError::InvalidLastModified)?,
                    )
                    .map_err(|_| ImageRequestError::InvalidLastModified)
                })
                .transpose()?,
        })
    }
}

type BoxedImageStream = Box<dyn Stream<Item = Result<Bytes, CacheError>> + Unpin + Send>;

#[derive(Error, Debug)]
pub enum CacheError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error(transparent)]
    Upstream(#[from] UpstreamError),
    #[error("An error occurred while reading the decryption header")]
    DecryptionFailure,
}

#[async_trait]
pub trait Cache: Send + Sync {
    async fn get(&self, key: &CacheKey)
        -> Option<Result<(CacheStream, ImageMetadata), CacheError>>;

    async fn put(
        &self,
        key: CacheKey,
        image: BoxedImageStream,
        metadata: ImageMetadata,
    ) -> Result<CacheStream, CacheError>;

    /// Increases the size of the cache. This is a double-dispatch method, so
    /// see specific implementations for complete detail. This only accepts a
    /// u32 as all files should be smaller than a u32 and some cache
    /// implementations can only handle up to a u32.
    fn increase_usage(&self, amt: u32);

    /// Decreases the size of the cache. This is a double-dispatch method, so
    /// see specific implementations for complete detail.
    fn decrease_usage(&self, amt: u64);

    /// Reports the on-disk size of the cache.
    fn on_disk_size(&self) -> u64;

    /// Reports the memory size of the cache.
    fn mem_size(&self) -> u64;

    async fn put_with_on_completed_callback(
        &self,
        key: CacheKey,
        image: BoxedImageStream,
        metadata: ImageMetadata,
        on_complete: Sender<(CacheKey, Bytes, ImageMetadata, usize)>,
    ) -> Result<CacheStream, CacheError>;

    /// Double-dispatch method. Used by cache implementations that require a
    /// completed entry to put items into their cache.
    async fn put_internal(&self, key: CacheKey, image: Bytes, metadata: ImageMetadata, size: usize);

    /// Pops an entry from the memory cache, if one exists.
    async fn pop_memory(&self) -> Option<(CacheKey, Bytes, ImageMetadata, usize)>;
}

#[async_trait]
impl<T: Cache> Cache for std::sync::Arc<T> {
    #[inline]
    async fn get(
        &self,
        key: &CacheKey,
    ) -> Option<Result<(CacheStream, ImageMetadata), CacheError>> {
        self.as_ref().get(key).await
    }

    #[inline]
    async fn put(
        &self,
        key: CacheKey,
        image: BoxedImageStream,
        metadata: ImageMetadata,
    ) -> Result<CacheStream, CacheError> {
        self.as_ref().put(key, image, metadata).await
    }

    #[inline]
    fn increase_usage(&self, amt: u32) {
        self.as_ref().increase_usage(amt)
    }

    #[inline]
    fn decrease_usage(&self, amt: u64) {
        self.as_ref().decrease_usage(amt)
    }

    #[inline]
    fn on_disk_size(&self) -> u64 {
        self.as_ref().on_disk_size()
    }

    #[inline]
    fn mem_size(&self) -> u64 {
        self.as_ref().mem_size()
    }

    #[inline]
    async fn put_with_on_completed_callback(
        &self,
        key: CacheKey,
        image: BoxedImageStream,
        metadata: ImageMetadata,
        on_complete: Sender<(CacheKey, Bytes, ImageMetadata, usize)>,
    ) -> Result<CacheStream, CacheError> {
        self.as_ref()
            .put_with_on_completed_callback(key, image, metadata, on_complete)
            .await
    }

    #[inline]
    async fn put_internal(
        &self,
        key: CacheKey,
        image: Bytes,
        metadata: ImageMetadata,
        size: usize,
    ) {
        self.as_ref().put_internal(key, image, metadata, size).await
    }

    #[inline]
    async fn pop_memory(&self) -> Option<(CacheKey, Bytes, ImageMetadata, usize)> {
        self.as_ref().pop_memory().await
    }
}

pub struct CacheStream {
    inner: InnerStream,
    decrypt: Option<SecretStream<Pull>>,
}

impl CacheStream {
    pub(self) fn new(inner: InnerStream, header: Option<Header>) -> Result<Self, ()> {
        Ok(Self {
            inner,
            decrypt: header
                .map(|header| SecretStream::init_pull(&header, ENCRYPTION_KEY.get().unwrap()))
                .transpose()?,
        })
    }
}

impl Stream for CacheStream {
    type Item = CacheStreamItem;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx).map(|data| {
            // False positive (`data`): https://link.eddie.sh/r1fXX
            #[allow(clippy::option_if_let_else)]
            if let Some(keystream) = self.decrypt.as_mut() {
                data.map(|bytes_res| {
                    bytes_res.and_then(|bytes| {
                        keystream
                            .pull(&bytes, None)
                            .map(|(data, _tag)| Bytes::from(data))
                            .map_err(|_| UpstreamError)
                    })
                })
            } else {
                data
            }
        })
    }
}

pub(self) enum InnerStream {
    Concurrent(ConcurrentFsStream),
    Memory(MemStream),
    Completed(FramedRead<Pin<Box<dyn AsyncRead + Send>>, BytesCodec>),
}

impl From<CachedImage> for InnerStream {
    fn from(image: CachedImage) -> Self {
        Self::Memory(MemStream(image.0))
    }
}

type CacheStreamItem = Result<Bytes, UpstreamError>;

impl Stream for InnerStream {
    type Item = CacheStreamItem;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            Self::Concurrent(stream) => stream.poll_next_unpin(cx),
            Self::Memory(stream) => stream.poll_next_unpin(cx),
            Self::Completed(stream) => stream
                .poll_next_unpin(cx)
                .map_ok(BytesMut::freeze)
                .map_err(|_| UpstreamError),
        }
    }
}

pub struct MemStream(pub Bytes);

impl Stream for MemStream {
    type Item = CacheStreamItem;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut new_bytes = Bytes::new();
        std::mem::swap(&mut self.0, &mut new_bytes);
        if new_bytes.is_empty() {
            Poll::Ready(None)
        } else {
            Poll::Ready(Some(Ok(new_bytes)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_size() {
        assert_eq!(std::mem::size_of::<ImageMetadata>(), 32);
    }
}
