//! Low memory caching stuff

use std::{path::PathBuf, sync::Arc};

use async_trait::async_trait;
use lru::LruCache;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::RwLock;

use super::{BoxedImageStream, Cache, CacheError, CacheKey, CacheStream, ImageMetadata};

pub struct LowMemCache {
    on_disk: LruCache<CacheKey, ImageMetadata>,
    disk_path: PathBuf,
    disk_max_size: u64,
    disk_cur_size: u64,
    master_sender: UnboundedSender<u64>,
}

impl LowMemCache {
    pub fn new(disk_max_size: u64, disk_path: PathBuf) -> Arc<RwLock<Box<dyn Cache>>> {
        let (tx, mut rx) = unbounded_channel();
        let new_self: Arc<RwLock<Box<dyn Cache>>> = Arc::new(RwLock::new(Box::new(Self {
            on_disk: LruCache::unbounded(),
            disk_path,
            disk_max_size,
            disk_cur_size: 0,
            master_sender: tx,
        })));

        let new_self_0 = Arc::clone(&new_self);
        tokio::spawn(async move {
            loop {
                let new_size = match rx.recv().await {
                    Some(v) => v,
                    None => break,
                };

                new_self_0.write().await.increase_usage(new_size).await;
            }
        });

        new_self.clone()
    }

    async fn prune(&mut self) {
        todo!()
    }
}

#[async_trait]
impl Cache for LowMemCache {
    async fn get(
        &mut self,
        key: &CacheKey,
    ) -> Option<Result<(CacheStream, &ImageMetadata), CacheError>> {
        let metadata = self.on_disk.get(key)?;
        let path = self.disk_path.clone().join(PathBuf::from(key.clone()));
        super::fs::read_file(&path)
            .await
            .map(|res| res.map(|stream| (stream, metadata)).map_err(Into::into))
    }

    async fn put(
        &mut self,
        key: CacheKey,
        image: BoxedImageStream,
        metadata: ImageMetadata,
    ) -> Result<(CacheStream, &ImageMetadata), CacheError> {
        let path = self.disk_path.clone().join(PathBuf::from(key.clone()));
        self.on_disk.put(key.clone(), metadata);
        super::fs::write_file(&path, image, self.master_sender.clone())
            .await
            .map(move |stream| (stream, self.on_disk.get(&key).unwrap()))
            .map_err(Into::into)
    }

    async fn increase_usage(&mut self, amt: u64) {
        self.disk_cur_size += amt;
        if self.disk_cur_size > self.disk_max_size {
            self.prune().await;
        }
    }
}
