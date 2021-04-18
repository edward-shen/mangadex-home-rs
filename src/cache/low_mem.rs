//! Low memory caching stuff

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use lru::LruCache;

use super::{fs::FromFsStream, ByteStream, Cache, CacheKey};

pub struct LowMemCache {
    on_disk: LruCache<CacheKey, ()>,
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

#[async_trait]
impl Cache for LowMemCache {
    async fn get_stream(&mut self, key: &CacheKey) -> Option<Result<FromFsStream, std::io::Error>> {
        if self.on_disk.get(key).is_some() {
            super::fs::read_file(Path::new(&key.to_string())).await
        } else {
            None
        }
    }

    async fn put_stream(
        &mut self,
        key: CacheKey,
        image: ByteStream,
    ) -> Result<FromFsStream, std::io::Error> {
        super::fs::write_file(&PathBuf::from(key), image).await
    }
}
