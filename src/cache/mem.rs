use std::borrow::Cow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::{Cache, CacheEntry, CacheKey, CacheStream, CallbackCache, ImageMetadata, MemStream};
use async_trait::async_trait;
use bytes::Bytes;
use futures::FutureExt;
use lfu_cache::LfuCache;
use lru::LruCache;
use redis::{
    Client as RedisClient, Commands, FromRedisValue, RedisError, RedisResult, ToRedisArgs,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Mutex;
use tracing::warn;

#[derive(Clone, Serialize, Deserialize)]
pub struct CacheValue {
    data: Bytes,
    metadata: ImageMetadata,
    on_disk_size: u64,
}

impl CacheValue {
    #[inline]
    fn new(data: Bytes, metadata: ImageMetadata, on_disk_size: u64) -> Self {
        Self {
            data,
            metadata,
            on_disk_size,
        }
    }
}

impl FromRedisValue for CacheValue {
    fn from_redis_value(v: &redis::Value) -> RedisResult<Self> {
        use bincode::ErrorKind;

        if let redis::Value::Data(data) = v {
            bincode::deserialize(data).map_err(|err| match *err {
                ErrorKind::Io(e) => RedisError::from(e),
                ErrorKind::Custom(e) => RedisError::from((
                    redis::ErrorKind::ResponseError,
                    "bincode deserialize failed",
                    e,
                )),
                e => RedisError::from((
                    redis::ErrorKind::ResponseError,
                    "bincode deserialized failed",
                    e.to_string(),
                )),
            })
        } else {
            Err(RedisError::from((
                redis::ErrorKind::TypeError,
                "Got non data type from redis db",
            )))
        }
    }
}

impl ToRedisArgs for CacheValue {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        out.write_arg(&bincode::serialize(self).expect("serialization to work"));
    }
}

/// Use LRU as the eviction strategy
pub type Lru = LruCache<CacheKey, CacheValue>;
/// Use LFU as the eviction strategy
pub type Lfu = LfuCache<CacheKey, CacheValue>;

/// Adapter trait for memory cache backends
pub trait InternalMemoryCacheInitializer: InternalMemoryCache {
    fn new() -> Self;
}

pub trait InternalMemoryCache: Sync + Send {
    fn get(&mut self, key: &CacheKey) -> Option<Cow<CacheValue>>;
    fn push(&mut self, key: CacheKey, data: CacheValue);
    fn pop(&mut self) -> Option<(CacheKey, CacheValue)>;
}

#[cfg(not(tarpaulin_include))]
impl InternalMemoryCacheInitializer for Lfu {
    #[inline]
    fn new() -> Self {
        Self::unbounded()
    }
}

#[cfg(not(tarpaulin_include))]
impl InternalMemoryCache for Lfu {
    #[inline]
    fn get(&mut self, key: &CacheKey) -> Option<Cow<CacheValue>> {
        self.get(key).map(Cow::Borrowed)
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

#[cfg(not(tarpaulin_include))]
impl InternalMemoryCacheInitializer for Lru {
    #[inline]
    fn new() -> Self {
        Self::unbounded()
    }
}

#[cfg(not(tarpaulin_include))]
impl InternalMemoryCache for Lru {
    #[inline]
    fn get(&mut self, key: &CacheKey) -> Option<Cow<CacheValue>> {
        self.get(key).map(Cow::Borrowed)
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

#[cfg(not(tarpaulin_include))]
impl InternalMemoryCache for RedisClient {
    fn get(&mut self, key: &CacheKey) -> Option<Cow<CacheValue>> {
        Commands::get(self, key).ok().map(Cow::Owned)
    }

    fn push(&mut self, key: CacheKey, data: CacheValue) {
        if let Err(e) = Commands::set::<_, _, ()>(self, key, data) {
            warn!("Failed to push to redis: {}", e);
        }
    }

    fn pop(&mut self) -> Option<(CacheKey, CacheValue)> {
        unimplemented!("redis should handle its own memory")
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
    MemoryCacheImpl: 'static + InternalMemoryCacheInitializer,
    ColdCache: 'static + Cache,
{
    pub fn new(inner: ColdCache, max_mem_size: crate::units::Bytes) -> Arc<Self> {
        let (tx, rx) = channel(100);
        let new_self = Arc::new(Self {
            inner,
            cur_mem_size: AtomicU64::new(0),
            mem_cache: Mutex::new(MemoryCacheImpl::new()),
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
                mem_cache: Mutex::new(MemoryCacheImpl::new()),
                master_sender: tx,
            },
            rx,
        )
    }
}

impl<MemoryCacheImpl, ColdCache> MemoryCache<MemoryCacheImpl, ColdCache>
where
    MemoryCacheImpl: 'static + InternalMemoryCache,
    ColdCache: 'static + Cache,
{
    pub fn new_with_cache(inner: ColdCache, init_mem_cache: MemoryCacheImpl) -> Self {
        Self {
            inner,
            cur_mem_size: AtomicU64::new(0),
            mem_cache: Mutex::new(init_mem_cache),
            master_sender: channel(1).0,
        }
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
    let max_mem_size = mem_threshold(&max_mem_size);
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
            .push(key, CacheValue::new(data, metadata, on_disk_size));

        // Pop if too large
        while cache.cur_mem_size.load(Ordering::Acquire) >= max_mem_size as u64 {
            let popped = cache.mem_cache.lock().await.pop().map(
                |(
                    key,
                    CacheValue {
                        data,
                        metadata,
                        on_disk_size,
                    },
                )| (key, data, metadata, on_disk_size),
            );
            if let Some((_, _, _, size)) = popped {
                cache.cur_mem_size.fetch_sub(size as u64, Ordering::Release);
            } else {
                break;
            }
        }
    }
}

const fn mem_threshold(bytes: &crate::units::Bytes) -> usize {
    bytes.get() / 20 * 19
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
            Some(mut mem_cache) => {
                match mem_cache.get(key).map(Cow::into_owned).map(
                    |CacheValue { data, metadata, .. }| {
                        Ok((CacheStream::Memory(MemStream(data)), metadata))
                    },
                ) {
                    Some(v) => Some(v),
                    None => self.inner.get(key).await,
                }
            }
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
    use std::borrow::Cow;
    use std::cell::RefCell;
    use std::collections::{BTreeMap, HashMap};

    use super::{CacheValue, InternalMemoryCache, InternalMemoryCacheInitializer};
    use crate::cache::{
        Cache, CacheEntry, CacheError, CacheKey, CacheStream, CallbackCache, ImageMetadata,
    };
    use async_trait::async_trait;
    use parking_lot::Mutex;
    use tokio::io::BufReader;
    use tokio::sync::mpsc::Sender;
    use tokio_util::io::ReaderStream;

    #[derive(Default)]
    pub struct TestDiskCache(
        pub Mutex<RefCell<HashMap<CacheKey, Result<(CacheStream, ImageMetadata), CacheError>>>>,
    );

    #[async_trait]
    impl Cache for TestDiskCache {
        async fn get(
            &self,
            key: &CacheKey,
        ) -> Option<Result<(CacheStream, ImageMetadata), CacheError>> {
            self.0.lock().get_mut().remove(key)
        }

        async fn put(
            &self,
            key: CacheKey,
            image: bytes::Bytes,
            metadata: ImageMetadata,
        ) -> Result<(), CacheError> {
            let reader = Box::pin(BufReader::new(tokio_util::io::StreamReader::new(
                tokio_stream::once(Ok::<_, std::io::Error>(image)),
            )));
            let stream = CacheStream::Completed(ReaderStream::new(reader));
            self.0.lock().get_mut().insert(key, Ok((stream, metadata)));
            Ok(())
        }
    }

    #[async_trait]
    impl CallbackCache for TestDiskCache {
        async fn put_with_on_completed_callback(
            &self,
            key: CacheKey,
            data: bytes::Bytes,
            metadata: ImageMetadata,
            on_complete: Sender<CacheEntry>,
        ) -> Result<(), CacheError> {
            self.put(key.clone(), data.clone(), metadata.clone())
                .await?;
            let on_disk_size = data.len() as u64;
            let _ = on_complete
                .send(CacheEntry {
                    key,
                    data,
                    metadata,
                    on_disk_size,
                })
                .await;
            Ok(())
        }
    }

    #[derive(Default)]
    pub struct TestMemoryCache(pub BTreeMap<CacheKey, CacheValue>);

    impl InternalMemoryCacheInitializer for TestMemoryCache {
        fn new() -> Self {
            Self::default()
        }
    }

    impl InternalMemoryCache for TestMemoryCache {
        fn get(&mut self, key: &CacheKey) -> Option<Cow<CacheValue>> {
            self.0.get(key).map(Cow::Borrowed)
        }

        fn push(&mut self, key: CacheKey, data: CacheValue) {
            self.0.insert(key, data);
        }

        fn pop(&mut self) -> Option<(CacheKey, CacheValue)> {
            let mut cache = BTreeMap::new();
            std::mem::swap(&mut cache, &mut self.0);
            let mut iter = cache.into_iter();
            let ret = iter.next();
            self.0 = iter.collect();
            ret
        }
    }
}

#[cfg(test)]
mod cache_ops {
    use std::error::Error;

    use bytes::Bytes;
    use futures::{FutureExt, StreamExt};

    use crate::cache::mem::{CacheValue, InternalMemoryCache};
    use crate::cache::{Cache, CacheEntry, CacheKey, CacheStream, ImageMetadata, MemStream};

    use super::test_util::{TestDiskCache, TestMemoryCache};
    use super::MemoryCache;

    #[tokio::test]
    async fn get_mem_cached() -> Result<(), Box<dyn Error>> {
        let (cache, mut rx) = MemoryCache::<TestMemoryCache, _>::new_with_receiver(
            TestDiskCache::default(),
            crate::units::Bytes(10),
        );

        let key = CacheKey("a".to_string(), "b".to_string(), false);
        let metadata = ImageMetadata {
            content_type: None,
            content_length: Some(1),
            last_modified: None,
        };
        let bytes = Bytes::from_static(b"abcd");
        let value = CacheValue::new(bytes.clone(), metadata.clone(), 34);

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

    #[tokio::test]
    async fn get_disk_cached() -> Result<(), Box<dyn Error>> {
        let (mut cache, mut rx) = MemoryCache::<TestMemoryCache, _>::new_with_receiver(
            TestDiskCache::default(),
            crate::units::Bytes(10),
        );

        let key = CacheKey("a".to_string(), "b".to_string(), false);
        let metadata = ImageMetadata {
            content_type: None,
            content_length: Some(1),
            last_modified: None,
        };
        let bytes = Bytes::from_static(b"abcd");

        {
            let cache = &mut cache.inner;
            cache
                .put(key.clone(), bytes.clone(), metadata.clone())
                .await?;
        }

        let (mut stream, ret_metadata) = cache.get(&key).await.unwrap()?;
        assert_eq!(metadata, ret_metadata);
        assert!(matches!(stream, CacheStream::Completed(_)));
        assert_eq!(stream.next().await, Some(Ok(bytes.clone())));

        assert!(rx.recv().now_or_never().is_none());

        Ok(())
    }

    // Identical to the get_disk_cached test but we hold a lock on the mem_cache
    #[tokio::test]
    async fn get_mem_locked() -> Result<(), Box<dyn Error>> {
        let (mut cache, mut rx) = MemoryCache::<TestMemoryCache, _>::new_with_receiver(
            TestDiskCache::default(),
            crate::units::Bytes(10),
        );

        let key = CacheKey("a".to_string(), "b".to_string(), false);
        let metadata = ImageMetadata {
            content_type: None,
            content_length: Some(1),
            last_modified: None,
        };
        let bytes = Bytes::from_static(b"abcd");

        {
            let cache = &mut cache.inner;
            cache
                .put(key.clone(), bytes.clone(), metadata.clone())
                .await?;
        }

        // intentionally not dropped
        let _mem_cache = &mut cache.mem_cache.lock().await;

        let (mut stream, ret_metadata) = cache.get(&key).await.unwrap()?;
        assert_eq!(metadata, ret_metadata);
        assert!(matches!(stream, CacheStream::Completed(_)));
        assert_eq!(stream.next().await, Some(Ok(bytes.clone())));

        assert!(rx.recv().now_or_never().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn get_miss() {
        let (cache, mut rx) = MemoryCache::<TestMemoryCache, _>::new_with_receiver(
            TestDiskCache::default(),
            crate::units::Bytes(10),
        );

        let key = CacheKey("a".to_string(), "b".to_string(), false);
        assert!(cache.get(&key).await.is_none());
        assert!(rx.recv().now_or_never().is_none());
    }

    #[tokio::test]
    async fn put_puts_into_disk_and_hears_from_rx() -> Result<(), Box<dyn Error>> {
        let (cache, mut rx) = MemoryCache::<TestMemoryCache, _>::new_with_receiver(
            TestDiskCache::default(),
            crate::units::Bytes(10),
        );

        let key = CacheKey("a".to_string(), "b".to_string(), false);
        let metadata = ImageMetadata {
            content_type: None,
            content_length: Some(1),
            last_modified: None,
        };
        let bytes = Bytes::from_static(b"abcd");
        let bytes_len = bytes.len() as u64;

        cache
            .put(key.clone(), bytes.clone(), metadata.clone())
            .await?;

        // Because the callback is supposed to let the memory cache insert the
        // entry into its cache, we can check that it properly stored it on the
        // disk layer by checking if we can successfully fetch it.

        let (mut stream, ret_metadata) = cache.get(&key).await.unwrap()?;
        assert_eq!(metadata, ret_metadata);
        assert!(matches!(stream, CacheStream::Completed(_)));
        assert_eq!(stream.next().await, Some(Ok(bytes.clone())));

        // Check that we heard back
        let cache_entry = rx
            .recv()
            .now_or_never()
            .flatten()
            .ok_or("failed to hear back from cache")?;
        assert_eq!(
            cache_entry,
            CacheEntry {
                key,
                data: bytes,
                metadata,
                on_disk_size: bytes_len,
            }
        );
        Ok(())
    }
}

#[cfg(test)]
mod db_listener {
    use std::error::Error;
    use std::iter::FromIterator;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use bytes::Bytes;
    use tokio::task;

    use crate::cache::{Cache, CacheKey, ImageMetadata};

    use super::test_util::{TestDiskCache, TestMemoryCache};
    use super::{internal_cache_listener, MemoryCache};

    #[tokio::test]
    async fn put_into_memory() -> Result<(), Box<dyn Error>> {
        let (cache, rx) = MemoryCache::<TestMemoryCache, _>::new_with_receiver(
            TestDiskCache::default(),
            crate::units::Bytes(0),
        );
        let cache = Arc::new(cache);
        tokio::spawn(internal_cache_listener(
            Arc::clone(&cache),
            crate::units::Bytes(20),
            rx,
        ));

        // put small image into memory
        let key = CacheKey("a".to_string(), "b".to_string(), false);
        let metadata = ImageMetadata {
            content_type: None,
            content_length: Some(1),
            last_modified: None,
        };
        let bytes = Bytes::from_static(b"abcd");
        cache.put(key.clone(), bytes.clone(), metadata).await?;

        // let the listener run first
        for _ in 0..10 {
            task::yield_now().await;
        }

        assert_eq!(
            cache.cur_mem_size.load(Ordering::SeqCst),
            bytes.len() as u64
        );

        // Since we didn't populate the cache, fetching must be from memory, so
        // this should succeed since the cache listener should push the item
        // into cache
        assert!(cache.get(&key).await.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn pops_items() -> Result<(), Box<dyn Error>> {
        let (cache, rx) = MemoryCache::<TestMemoryCache, _>::new_with_receiver(
            TestDiskCache::default(),
            crate::units::Bytes(0),
        );
        let cache = Arc::new(cache);
        tokio::spawn(internal_cache_listener(
            Arc::clone(&cache),
            crate::units::Bytes(20),
            rx,
        ));

        // put small image into memory
        let key_0 = CacheKey("a".to_string(), "b".to_string(), false);
        let key_1 = CacheKey("c".to_string(), "d".to_string(), false);
        let metadata = ImageMetadata {
            content_type: None,
            content_length: Some(1),
            last_modified: None,
        };
        let bytes = Bytes::from_static(b"abcde");

        cache.put(key_0, bytes.clone(), metadata.clone()).await?;
        cache.put(key_1, bytes.clone(), metadata).await?;

        // let the listener run first
        task::yield_now().await;
        for _ in 0..10 {
            task::yield_now().await;
        }

        // Items should be in cache now
        assert_eq!(
            cache.cur_mem_size.load(Ordering::SeqCst),
            (bytes.len() * 2) as u64
        );

        let key_3 = CacheKey("e".to_string(), "f".to_string(), false);
        let metadata = ImageMetadata {
            content_type: None,
            content_length: Some(1),
            last_modified: None,
        };
        let bytes = Bytes::from_iter(b"0".repeat(16).into_iter());
        let bytes_len = bytes.len();
        cache.put(key_3, bytes, metadata).await?;

        // let the listener run first
        task::yield_now().await;
        for _ in 0..10 {
            task::yield_now().await;
        }

        // Items should have been evicted, only 16 bytes should be there now
        assert_eq!(cache.cur_mem_size.load(Ordering::SeqCst), bytes_len as u64);
        Ok(())
    }
}

#[cfg(test)]
mod mem_threshold {
    use crate::units::Bytes;

    use super::mem_threshold;

    #[test]
    fn small_amount_works() {
        assert_eq!(mem_threshold(&Bytes(100)), 95);
    }

    #[test]
    fn large_amount_cannot_overflow() {
        assert_eq!(mem_threshold(&Bytes(usize::MAX)), 17524406870024074020);
    }
}
