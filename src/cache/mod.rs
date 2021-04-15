use std::path::PathBuf;
use std::{fmt::Display, str::FromStr};

use actix_web::http::HeaderValue;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, FixedOffset};
use futures::Stream;

pub use generational::GenerationalCache;
pub use low_mem::LowMemCache;

use self::fs::FromFsStream;

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

pub struct CachedImage(pub Bytes);

#[derive(Copy, Clone)]
pub struct ImageMetadata {
    pub content_type: Option<ImageContentType>,
    pub content_length: Option<usize>,
    pub last_modified: Option<DateTime<FixedOffset>>,
}

// Note to self: If these are wrong blame Triscuit 9
#[derive(Copy, Clone)]
pub enum ImageContentType {
    Png,
    Jpeg,
    Gif,
    Bmp,
    Tif,
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
            "image/bmp" => Ok(Self::Bmp),
            "image/tif" => Ok(Self::Tif),
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
            Self::Bmp => "image/bmp",
            Self::Tif => "image/tif",
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

#[async_trait]
pub trait Cache {
    async fn get(&mut self, _key: &CacheKey) -> Option<&(CachedImage, ImageMetadata)> {
        unimplemented!()
    }

    async fn put(&mut self, _key: CacheKey, _image: CachedImage, _metadata: ImageMetadata) {
        unimplemented!()
    }

    async fn get_stream(
        &mut self,
        _key: &CacheKey,
    ) -> Option<Result<FromFsStream, std::io::Error>> {
        unimplemented!()
    }

    async fn put_stream(
        &mut self,
        _key: CacheKey,
        _image: impl Stream<Item = Result<Bytes, reqwest::Error>> + Unpin + Send + 'static,
    ) {
        unimplemented!()
    }
}
