//! Low memory caching stuff

use std::path::PathBuf;

use async_trait::async_trait;
use lru::LruCache;

use super::{BoxedImageStream, Cache, CacheError, CacheKey, CacheStream, ImageMetadata};

pub struct LowMemCache {
    on_disk: LruCache<CacheKey, ImageMetadata>,
    disk_path: PathBuf,
    disk_max_size: u64,
    disk_cur_size: u64,
}

impl LowMemCache {
    pub fn new(disk_max_size: u64, disk_path: PathBuf) -> Self {
        Self {
            on_disk: LruCache::unbounded(),
            disk_path,
            disk_max_size,
            disk_cur_size: 0,
        }
    }
}

// todo: schedule eviction

#[async_trait]
impl Cache for LowMemCache {
    async fn get(
        &mut self,
        key: &CacheKey,
    ) -> Option<Result<(CacheStream, &ImageMetadata), CacheError>> {
        let metadata = self.on_disk.get(key)?;
        let path = self.disk_path.clone().join(PathBuf::from(key.clone()));
        super::fs::read_file(&path).await.map(|res| {
            res.map(|stream| (CacheStream::Fs(stream), metadata))
                .map_err(Into::into)
        })
    }

    async fn put(
        &mut self,
        key: CacheKey,
        image: BoxedImageStream,
        metadata: ImageMetadata,
    ) -> Result<(CacheStream, &ImageMetadata), CacheError> {
        let path = self.disk_path.clone().join(PathBuf::from(key.clone()));
        self.on_disk.put(key.clone(), metadata);
        super::fs::write_file(&path, image)
            .await
            .map(CacheStream::Fs)
            .map(move |stream| (stream, self.on_disk.get(&key).unwrap()))
            .map_err(Into::into)
    }
}
