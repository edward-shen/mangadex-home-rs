use std::fmt::Display;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};

use actix_web::http::HeaderValue;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use chacha20::Key;
use chrono::{DateTime, FixedOffset};
use futures::{Stream, StreamExt};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use thiserror::Error;
use tokio::sync::mpsc::Sender;
use tokio_util::codec::{BytesCodec, FramedRead};

pub use disk::DiskCache;
pub use fs::UpstreamError;
pub use mem::MemoryCache;

use self::compat::LegacyImageMetadata;
use self::fs::MetadataFetch;

pub static ENCRYPTION_KEY: OnceCell<Key> = OnceCell::new();

mod compat;
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

#[derive(Copy, Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct ImageMetadata {
    pub content_type: Option<ImageContentType>,
    pub content_length: Option<u32>,
    pub last_modified: Option<DateTime<FixedOffset>>,
}

// Confirmed by Ply to be these types: https://link.eddie.sh/ZXfk0
#[derive(Copy, Clone, Serialize_repr, Deserialize_repr, Debug, PartialEq, Eq)]
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

impl From<LegacyImageMetadata> for ImageMetadata {
    fn from(legacy: LegacyImageMetadata) -> Self {
        Self {
            content_type: legacy.content_type.map(|v| v.0),
            content_length: legacy.size,
            last_modified: legacy.last_modified.map(|v| v.0),
        }
    }
}

#[derive(Debug)]
pub enum ImageRequestError {
    ContentType,
    ContentLength,
    LastModified,
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
                .map_err(|_| ImageRequestError::ContentType)?,
            content_length: content_length
                .map(|header_val| {
                    header_val
                        .to_str()
                        .map_err(|_| ImageRequestError::ContentLength)?
                        .parse()
                        .map_err(|_| ImageRequestError::ContentLength)
                })
                .transpose()?,
            last_modified: last_modified
                .map(|header_val| {
                    DateTime::parse_from_rfc2822(
                        header_val
                            .to_str()
                            .map_err(|_| ImageRequestError::LastModified)?,
                    )
                    .map_err(|_| ImageRequestError::LastModified)
                })
                .transpose()?,
        })
    }
}

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
        image: Bytes,
        metadata: ImageMetadata,
    ) -> Result<(), CacheError>;
}

#[async_trait]
impl<T: Cache> Cache for Arc<T> {
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
        image: Bytes,
        metadata: ImageMetadata,
    ) -> Result<(), CacheError> {
        self.as_ref().put(key, image, metadata).await
    }
}

#[async_trait]
pub trait CallbackCache: Cache {
    async fn put_with_on_completed_callback(
        &self,
        key: CacheKey,
        image: Bytes,
        metadata: ImageMetadata,
        on_complete: Sender<CacheEntry>,
    ) -> Result<(), CacheError>;
}

#[async_trait]
impl<T: CallbackCache> CallbackCache for Arc<T> {
    #[inline]
    async fn put_with_on_completed_callback(
        &self,
        key: CacheKey,
        image: Bytes,
        metadata: ImageMetadata,
        on_complete: Sender<CacheEntry>,
    ) -> Result<(), CacheError> {
        self.as_ref()
            .put_with_on_completed_callback(key, image, metadata, on_complete)
            .await
    }
}

pub struct CacheEntry {
    key: CacheKey,
    data: Bytes,
    metadata: ImageMetadata,
    on_disk_size: u64,
}

pub enum CacheStream {
    Memory(MemStream),
    Completed(FramedRead<Pin<Box<dyn MetadataFetch + Send>>, BytesCodec>),
}

impl From<CachedImage> for CacheStream {
    fn from(image: CachedImage) -> Self {
        Self::Memory(MemStream(image.0))
    }
}

type CacheStreamItem = Result<Bytes, UpstreamError>;

impl Stream for CacheStream {
    type Item = CacheStreamItem;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
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
