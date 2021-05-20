use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::{
    BoxedImageStream, Cache, CacheError, CacheKey, CacheStream, CallbackCache, ImageMetadata,
    InnerStream, MemStream,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::FutureExt;
use lfu_cache::LfuCache;
use lru::LruCache;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::Mutex;

type CacheValue = (Bytes, ImageMetadata, u64);

/// Use LRU as the eviction strategy
pub type Lru = LruCache<CacheKey, CacheValue>;
/// Use LFU as the eviction strategy
pub type Lfu = LfuCache<CacheKey, CacheValue>;

/// Adapter trait for memory cache backends
pub trait InternalMemoryCache: Sync + Send {
    fn unbounded() -> Self;
    fn get(&mut self, key: &CacheKey) -> Option<&CacheValue>;
    fn push(&mut self, key: CacheKey, data: CacheValue);
    fn pop(&mut self) -> Option<(CacheKey, CacheValue)>;
}

impl InternalMemoryCache for Lfu {
    #[inline]
    fn unbounded() -> Self {
        Self::unbounded()
    }

    #[inline]
    fn get(&mut self, key: &CacheKey) -> Option<&CacheValue> {
        self.get(key)
    }

    #[inline]
    fn push(&mut self, key: CacheKey, data: CacheValue) {
        self.insert(key, data);
    }

    #[inline]
    fn pop(&mut self) -> Option<(CacheKey, CacheValue)> {
        self.pop_lfu_key_value()
    }
}

impl InternalMemoryCache for Lru {
    #[inline]
    fn unbounded() -> Self {
        Self::unbounded()
    }

    #[inline]
    fn get(&mut self, key: &CacheKey) -> Option<&CacheValue> {
        self.get(key)
    }

    #[inline]
    fn push(&mut self, key: CacheKey, data: CacheValue) {
        self.put(key, data);
    }

    #[inline]
    fn pop(&mut self) -> Option<(CacheKey, CacheValue)> {
        self.pop_lru()
    }
}

/// Memory accelerated disk cache. Uses the internal cache implementation in
/// memory to speed up reads.
pub struct MemoryCache<InternalCacheImpl, InnerCache> {
    inner: InnerCache,
    cur_mem_size: AtomicU64,
    mem_cache: Mutex<InternalCacheImpl>,
    master_sender: Sender<(CacheKey, Bytes, ImageMetadata, u64)>,
}

impl<InternalCacheImpl: 'static + InternalMemoryCache, InnerCache: 'static + Cache>
    MemoryCache<InternalCacheImpl, InnerCache>
{
    pub async fn new(inner: InnerCache, max_mem_size: u64) -> Arc<Self> {
        let (tx, mut rx) = channel(100);
        let new_self = Arc::new(Self {
            inner,
            cur_mem_size: AtomicU64::new(0),
            mem_cache: Mutex::new(InternalCacheImpl::unbounded()),
            master_sender: tx,
        });

        let new_self_0 = Arc::clone(&new_self);
        tokio::spawn(async move {
            let new_self = new_self_0;
            let max_mem_size = max_mem_size / 20 * 19;
            while let Some((key, bytes, metadata, size)) = rx.recv().await {
                // Add to memory cache
                new_self
                    .cur_mem_size
                    .fetch_add(size as u64, Ordering::Release);
                new_self
                    .mem_cache
                    .lock()
                    .await
                    .push(key, (bytes, metadata, size));

                // Pop if too large
                while new_self.cur_mem_size.load(Ordering::Acquire) >= max_mem_size {
                    let popped = new_self
                        .mem_cache
                        .lock()
                        .await
                        .pop()
                        .map(|(key, (bytes, metadata, size))| (key, bytes, metadata, size));
                    if let Some((_, _, _, size)) = popped {
                        new_self
                            .cur_mem_size
                            .fetch_sub(size as u64, Ordering::Release);
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
impl<InternalCacheImpl, InnerCache> Cache for MemoryCache<InternalCacheImpl, InnerCache>
where
    InternalCacheImpl: InternalMemoryCache,
    InnerCache: CallbackCache,
{
    #[inline]
    async fn get(
        &self,
        key: &CacheKey,
    ) -> Option<Result<(CacheStream, ImageMetadata), super::CacheError>> {
        match self.mem_cache.lock().now_or_never() {
            Some(mut mem_cache) => match mem_cache.get(key).map(|(bytes, metadata, _)| {
                Ok((InnerStream::Memory(MemStream(bytes.clone())), *metadata))
            }) {
                Some(v) => Some(v.and_then(|(inner, metadata)| {
                    CacheStream::new(inner, None)
                        .map(|v| (v, metadata))
                        .map_err(|_| CacheError::DecryptionFailure)
                })),
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
}
