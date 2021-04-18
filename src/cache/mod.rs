use std::fmt::Display;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};

use actix_web::http::HeaderValue;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, FixedOffset};
use fs::FsStream;
use futures::{Stream, StreamExt};
use thiserror::Error;

pub use fs::UpstreamError;
pub use generational::GenerationalCache;
pub use low_mem::LowMemCache;

mod fs;
mod generational;
mod low_mem;

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

#[derive(Clone)]
pub struct CachedImage(pub Bytes);

#[derive(Copy, Clone)]
pub struct ImageMetadata {
    pub content_type: Option<ImageContentType>,
    // If we can guarantee a non-zero u32 here we can save 4 bytes
    pub content_length: Option<u32>,
    pub last_modified: Option<DateTime<FixedOffset>>,
}

// Confirmed by Ply to be these types: https://link.eddie.sh/ZXfk0
#[derive(Copy, Clone)]
pub enum ImageContentType {
    Png,
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
}

#[async_trait]
pub trait Cache: Send + Sync {
    async fn get(
        &mut self,
        key: &CacheKey,
    ) -> Option<Result<(CacheStream, &ImageMetadata), CacheError>>;
    async fn put(
        &mut self,
        key: CacheKey,
        image: BoxedImageStream,
        metadata: ImageMetadata,
    ) -> Result<(CacheStream, &ImageMetadata), CacheError>;
}

pub enum CacheStream {
    Fs(FsStream),
    Memory(MemStream),
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
            Self::Fs(stream) => stream.poll_next_unpin(cx),
            Self::Memory(stream) => stream.poll_next_unpin(cx),
        }
    }
}

pub struct MemStream(Bytes);

impl Stream for MemStream {
    type Item = CacheStreamItem;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut new_bytes = Bytes::new();
        std::mem::swap(&mut self.0, &mut new_bytes);
        Poll::Ready(Some(Ok(new_bytes)))
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
