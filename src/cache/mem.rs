use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::{Cache, CacheEntry, CacheKey, CacheStream, CallbackCache, ImageMetadata, MemStream};
use async_trait::async_trait;
use bytes::Bytes;
use futures::FutureExt;
use lfu_cache::LfuCache;
use lru::LruCache;
use tokio::sync::mpsc::{channel, Receiver, Sender};
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
pub struct MemoryCache<MemoryCacheImpl, ColdCache> {
    inner: ColdCache,
    cur_mem_size: AtomicU64,
    mem_cache: Mutex<MemoryCacheImpl>,
    master_sender: Sender<CacheEntry>,
}

impl<MemoryCacheImpl, ColdCache> MemoryCache<MemoryCacheImpl, ColdCache>
where
    MemoryCacheImpl: 'static + InternalMemoryCache,
    ColdCache: 'static + Cache,
{
    pub fn new(inner: ColdCache, max_mem_size: crate::units::Bytes) -> Arc<Self> {
        let (tx, rx) = channel(100);
        let new_self = Arc::new(Self {
            inner,
            cur_mem_size: AtomicU64::new(0),
            mem_cache: Mutex::new(MemoryCacheImpl::unbounded()),
            master_sender: tx,
        });

        tokio::spawn(internal_cache_listener(
            Arc::clone(&new_self),
            max_mem_size,
            rx,
        ));

        new_self
    }

    /// Returns an instance of the cache with the receiver for callback events
    /// Really only useful for inspecting the receiver, e.g. for testing
    #[cfg(test)]
    pub fn new_with_receiver(
        inner: ColdCache,
        _: crate::units::Bytes,
    ) -> (Self, Receiver<CacheEntry>) {
        let (tx, rx) = channel(100);
        (
            Self {
                inner,
                cur_mem_size: AtomicU64::new(0),
                mem_cache: Mutex::new(MemoryCacheImpl::unbounded()),
                master_sender: tx,
            },
            rx,
        )
    }
}

async fn internal_cache_listener<MemoryCacheImpl, ColdCache>(
    cache: Arc<MemoryCache<MemoryCacheImpl, ColdCache>>,
    max_mem_size: crate::units::Bytes,
    mut rx: Receiver<CacheEntry>,
) where
    MemoryCacheImpl: InternalMemoryCache,
    ColdCache: Cache,
{
    let max_mem_size = max_mem_size.get() / 20 * 19;
    while let Some(CacheEntry {
        key,
        data,
        metadata,
        on_disk_size,
    }) = rx.recv().await
    {
        // Add to memory cache
        // We can add first because we constrain our memory usage to 95%
        cache
            .cur_mem_size
            .fetch_add(on_disk_size as u64, Ordering::Release);
        cache
            .mem_cache
            .lock()
            .await
            .push(key, (data, metadata, on_disk_size));

        // Pop if too large
        while cache.cur_mem_size.load(Ordering::Acquire) >= max_mem_size as u64 {
            let popped = cache
                .mem_cache
                .lock()
                .await
                .pop()
                .map(|(key, (bytes, metadata, size))| (key, bytes, metadata, size));
            if let Some((_, _, _, size)) = popped {
                cache.cur_mem_size.fetch_sub(size as u64, Ordering::Release);
            } else {
                break;
            }
        }
    }
}

#[async_trait]
impl<MemoryCacheImpl, ColdCache> Cache for MemoryCache<MemoryCacheImpl, ColdCache>
where
    MemoryCacheImpl: InternalMemoryCache,
    ColdCache: CallbackCache,
{
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
        image: Bytes,
        metadata: ImageMetadata,
    ) -> Result<(), super::CacheError> {
        self.inner
            .put_with_on_completed_callback(key, image, metadata, self.master_sender.clone())
            .await
    }
}

#[cfg(test)]
mod test_util {
    use std::collections::HashMap;

    use super::{CacheValue, InternalMemoryCache};
    use crate::cache::{
        Cache, CacheEntry, CacheError, CacheKey, CacheStream, CallbackCache, ImageMetadata,
    };
    use async_trait::async_trait;
    use tokio::sync::mpsc::Sender;

    pub struct TestDiskCache(
        pub HashMap<CacheKey, Result<(CacheStream, ImageMetadata), CacheError>>,
    );

    #[async_trait]
    impl Cache for TestDiskCache {
        async fn get(
            &self,
            key: &CacheKey,
        ) -> Option<Result<(CacheStream, ImageMetadata), CacheError>> {
            // todo: Actually implement nontrivial code
            None
        }

        async fn put(
            &self,
            key: CacheKey,
            image: bytes::Bytes,
            metadata: ImageMetadata,
        ) -> Result<(), CacheError> {
            todo!()
        }
    }

    #[async_trait]
    impl CallbackCache for TestDiskCache {
        async fn put_with_on_completed_callback(
            &self,
            key: CacheKey,
            image: bytes::Bytes,
            metadata: ImageMetadata,
            on_complete: Sender<CacheEntry>,
        ) -> Result<(), CacheError> {
            todo!()
        }
    }

    #[derive(Default)]
    pub struct TestMemoryCache(pub HashMap<CacheKey, CacheValue>);

    impl InternalMemoryCache for TestMemoryCache {
        fn unbounded() -> Self {
            Self::default()
        }

        fn get(&mut self, key: &CacheKey) -> Option<&CacheValue> {
            self.0.get(key)
        }

        fn push(&mut self, key: CacheKey, data: CacheValue) {
            self.0.insert(key, data);
        }

        fn pop(&mut self) -> Option<(CacheKey, CacheValue)> {
            unimplemented!("shouldn't be needed for tests");
        }
    }
}

#[cfg(test)]
mod cache_ops {
    use std::collections::HashMap;
    use std::error::Error;

    use bytes::Bytes;
    use futures::FutureExt;

    use crate::cache::mem::InternalMemoryCache;
    use crate::cache::{Cache, CacheKey, CacheStream, ImageMetadata, MemStream};

    use super::test_util::{TestDiskCache, TestMemoryCache};
    use super::MemoryCache;

    #[tokio::test]
    async fn get_mem_cached() -> Result<(), Box<dyn Error>> {
        let (cache, mut rx) = MemoryCache::<TestMemoryCache, _>::new_with_receiver(
            TestDiskCache(HashMap::new()),
            crate::units::Bytes(10),
        );

        let key = CacheKey("a".to_string(), "b".to_string(), false);
        let metadata = ImageMetadata {
            content_type: None,
            content_length: Some(1),
            last_modified: None,
        };
        let bytes = Bytes::from_static(b"abcd");
        let value = (bytes.clone(), metadata.clone(), 34);

        // Populate the cache, need to drop the lock else it's considered locked
        // when we actually call the cache
        {
            let mem_cache = &mut cache.mem_cache.lock().await;
            mem_cache.push(key.clone(), value.clone());
        }

        let (stream, ret_metadata) = cache.get(&key).await.unwrap()?;
        assert_eq!(metadata, ret_metadata);
        if let CacheStream::Memory(MemStream(ret_stream)) = stream {
            assert_eq!(bytes, ret_stream);
        } else {
            panic!("wrong stream type");
        }

        assert!(rx.recv().now_or_never().is_none());

        Ok(())
    }

    #[test]
    fn get_disk_cached() {}

    #[test]
    fn get_miss() {}
}

#[cfg(test)]
mod db_listener {
    use super::*;
}
