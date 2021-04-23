use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::cache::DiskCache;

use super::{BoxedImageStream, Cache, CacheKey, CacheStream, ImageMetadata, MemStream};
use async_trait::async_trait;
use bytes::Bytes;
use futures::FutureExt;
use lru::LruCache;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::Mutex;

/// Memory accelerated disk cache. Uses an LRU in memory to speed up reads.
///
pub struct MemoryLruCache {
    inner: Arc<Box<dyn Cache>>,
    cur_mem_size: AtomicU64,
    mem_cache: Mutex<LruCache<CacheKey, (Bytes, ImageMetadata, usize)>>,
    master_sender: Sender<(CacheKey, Bytes, ImageMetadata, usize)>,
}

impl MemoryLruCache {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(
        disk_max_size: u64,
        disk_path: PathBuf,
        max_mem_size: u64,
    ) -> Arc<Box<dyn Cache>> {
        let (tx, mut rx) = channel(100);
        let new_self = Arc::new(Box::new(Self {
            inner: DiskCache::new(disk_max_size, disk_path).await,
            cur_mem_size: AtomicU64::new(0),
            mem_cache: Mutex::new(LruCache::unbounded()),
            master_sender: tx,
        }) as Box<dyn Cache>);

        let new_self_0 = Arc::clone(&new_self);
        tokio::spawn(async move {
            let new_self = new_self_0;
            let max_mem_size = max_mem_size / 20 * 19;
            while let Some((key, bytes, metadata, size)) = rx.recv().await {
                new_self.increase_usage(size as u32);
                new_self.put_internal(key, bytes, metadata, size).await;
                while new_self.mem_size() >= max_mem_size {
                    if let Some((_, _, _, size)) = new_self.pop_memory().await {
                        new_self.decrease_usage(size as u64);
                    } else {
                        break;
                    }
                }
            }
        });

        new_self
    }
}

#[async_trait]
impl Cache for MemoryLruCache {
    #[inline]
    async fn get(
        &self,
        key: &CacheKey,
    ) -> Option<Result<(CacheStream, ImageMetadata), super::CacheError>> {
        match self.mem_cache.lock().now_or_never() {
            Some(mut mem_cache) => match mem_cache.get(key).map(|(bytes, metadata, _)| {
                Ok((CacheStream::Memory(MemStream(bytes.clone())), *metadata))
            }) {
                Some(v) => Some(v),
                None => self.inner.get(key).await,
            },
            None => self.inner.get(key).await,
        }
    }

    #[inline]
    async fn put(
        &self,
        key: CacheKey,
        image: BoxedImageStream,
        metadata: ImageMetadata,
    ) -> Result<CacheStream, super::CacheError> {
        self.inner
            .put_with_on_completed_callback(key, image, metadata, self.master_sender.clone())
            .await
    }

    #[inline]
    fn increase_usage(&self, amt: u32) {
        self.cur_mem_size
            .fetch_add(u64::from(amt), Ordering::Release);
    }

    #[inline]
    fn decrease_usage(&self, amt: u64) {
        self.cur_mem_size.fetch_sub(amt, Ordering::Release);
    }

    #[inline]
    fn on_disk_size(&self) -> u64 {
        self.inner.on_disk_size()
    }

    #[inline]
    fn mem_size(&self) -> u64 {
        self.cur_mem_size.load(Ordering::Acquire)
    }

    #[inline]
    async fn put_with_on_completed_callback(
        &self,
        key: CacheKey,
        image: BoxedImageStream,
        metadata: ImageMetadata,
        on_complete: Sender<(CacheKey, Bytes, ImageMetadata, usize)>,
    ) -> Result<CacheStream, super::CacheError> {
        self.inner
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
        self.mem_cache
            .lock()
            .await
            .put(key, (image, metadata, size));
    }

    #[inline]
    async fn pop_memory(&self) -> Option<(CacheKey, Bytes, ImageMetadata, usize)> {
        self.mem_cache
            .lock()
            .await
            .pop_lru()
            .map(|(key, (bytes, metadata, size))| (key, bytes, metadata, size))
    }
}
