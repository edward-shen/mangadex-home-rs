//! Low memory caching stuff

use std::convert::TryFrom;
use std::hint::unreachable_unchecked;
use std::os::unix::prelude::OsStrExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use log::LevelFilter;
use md5::digest::generic_array::GenericArray;
use md5::{Digest, Md5};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, Sqlite, SqlitePool, Transaction};
use tokio::fs::remove_file;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, instrument, warn};

use crate::units::Bytes;

use super::{Cache, CacheError, CacheKey, CacheStream, CallbackCache, ImageMetadata};

#[derive(Debug)]
pub struct DiskCache {
    disk_path: PathBuf,
    disk_cur_size: AtomicU64,
    db_update_channel_sender: Sender<DbMessage>,
}

enum DbMessage {
    Get(Arc<PathBuf>),
    Put(Arc<PathBuf>, u64),
}

impl DiskCache {
    /// Constructs a new low memory cache at the provided path and capacity.
    /// This internally spawns a task that will wait for filesystem
    /// notifications when a file has been written.
    pub async fn new(disk_max_size: Bytes, disk_path: PathBuf) -> Arc<Self> {
        let db_pool = {
            let db_url = format!("sqlite:{}/metadata.sqlite", disk_path.to_string_lossy());
            let mut options = SqliteConnectOptions::from_str(&db_url)
                .unwrap()
                .create_if_missing(true);
            options.log_statements(LevelFilter::Trace);
            SqlitePool::connect_with(options).await.unwrap()
        };

        Self::from_db_pool(db_pool, disk_max_size, disk_path).await
    }

    async fn from_db_pool(pool: SqlitePool, disk_max_size: Bytes, disk_path: PathBuf) -> Arc<Self> {
        let (db_tx, db_rx) = channel(128);
        // Run db init
        sqlx::query_file!("./db_queries/init.sql")
            .execute(&mut pool.acquire().await.unwrap())
            .await
            .unwrap();

        // This is intentional.
        #[allow(clippy::cast_sign_loss)]
        let disk_cur_size = {
            let mut conn = pool.acquire().await.unwrap();
            sqlx::query!("SELECT IFNULL(SUM(size), 0) AS size FROM Images")
                .fetch_one(&mut conn)
                .await
                .map(|record| record.size)
                .unwrap_or_default()
                .unwrap_or_default() as u64
        };

        let new_self = Arc::new(Self {
            disk_path,
            disk_cur_size: AtomicU64::new(disk_cur_size),
            db_update_channel_sender: db_tx,
        });

        tokio::spawn(db_listener(
            Arc::clone(&new_self),
            db_rx,
            pool,
            disk_max_size.get() as u64 / 20 * 19,
        ));

        new_self
    }

    #[cfg(test)]
    fn in_memory() -> (Self, Receiver<DbMessage>) {
        let (db_tx, db_rx) = channel(128);
        (
            Self {
                disk_path: PathBuf::new(),
                disk_cur_size: AtomicU64::new(0),
                db_update_channel_sender: db_tx,
            },
            db_rx,
        )
    }
}

/// Spawn a new task that will listen for updates to the db, pruning if the size
/// becomes too large.
async fn db_listener(
    cache: Arc<DiskCache>,
    db_rx: Receiver<DbMessage>,
    db_pool: SqlitePool,
    max_on_disk_size: u64,
) {
    let mut recv_stream = ReceiverStream::new(db_rx).ready_chunks(128);
    while let Some(messages) = recv_stream.next().await {
        let mut transaction = match db_pool.begin().await {
            Ok(transaction) => transaction,
            Err(e) => {
                error!("Failed to start a transaction to DB, cannot update DB. Disk cache may be losing track of files! {}", e);
                continue;
            }
        };

        for message in messages {
            match message {
                DbMessage::Get(entry) => handle_db_get(&entry, &mut transaction).await,
                DbMessage::Put(entry, size) => {
                    handle_db_put(&entry, size, &cache, &mut transaction).await;
                }
            }
        }

        if let Err(e) = transaction.commit().await {
            error!(
                "Failed to commit transaction to DB. Disk cache may be losing track of files! {}",
                e
            );
        }

        let on_disk_size = (cache.disk_cur_size.load(Ordering::Acquire) + 4095) / 4096 * 4096;
        if on_disk_size >= max_on_disk_size {
            let mut conn = match db_pool.acquire().await {
                Ok(conn) => conn,
                Err(e) => {
                    error!(
                        "Failed to get a DB connection and cannot prune disk cache: {}",
                        e
                    );
                    continue;
                }
            };

            let items = {
                let request =
                    sqlx::query!("select id, size from Images order by accessed asc limit 1000")
                        .fetch_all(&mut conn)
                        .await;
                match request {
                    Ok(items) => items,
                    Err(e) => {
                        error!(
                            "Failed to fetch oldest images and cannot prune disk cache: {}",
                            e
                        );
                        continue;
                    }
                }
            };

            let mut size_freed = 0;
            #[allow(clippy::cast_sign_loss)]
            for item in items {
                debug!("deleting file due to exceeding cache size");
                size_freed += item.size as u64;
                tokio::spawn(async move {
                    let key = item.id;
                    if let Err(e) = remove_file(key.clone()).await {
                        match e.kind() {
                            std::io::ErrorKind::NotFound => {
                                let hash = Md5Hash(*GenericArray::from_slice(key.as_bytes()));
                                let path: PathBuf = hash.into();
                                if let Err(e) = remove_file(&path).await {
                                    warn!(
                                        "Failed to delete file `{}` from cache: {}",
                                        path.to_string_lossy(),
                                        e
                                    );
                                }
                            }
                            _ => {
                                warn!("Failed to delete file `{}` from cache: {}", &key, e);
                            }
                        }
                    }
                });
            }

            cache.disk_cur_size.fetch_sub(size_freed, Ordering::Release);
        }
    }
}

#[instrument(level = "debug", skip(transaction))]
async fn handle_db_get(entry: &Path, transaction: &mut Transaction<'_, Sqlite>) {
    let hash = if let Ok(hash) = Md5Hash::try_from(entry) {
        hash
    } else {
        error!(
            "Failed to derive hash from entry, dropping message: {}",
            entry.to_string_lossy()
        );
        return;
    };

    let hash_str = hash.to_hex_string();
    let key = entry.as_os_str().to_str();
    let now = chrono::Utc::now();
    let query = sqlx::query!(
        "update Images set accessed = ? where id = ? or id = ?",
        now,
        key,
        hash_str
    )
    .execute(transaction)
    .await;
    if let Err(e) = query {
        warn!("Failed to update timestamp in db for {:?}: {}", key, e);
    }
}

#[instrument(level = "debug", skip(transaction, cache))]
async fn handle_db_put(
    entry: &Path,
    size: u64,
    cache: &DiskCache,
    transaction: &mut Transaction<'_, Sqlite>,
) {
    let key = entry.as_os_str().to_str();
    let now = chrono::Utc::now();

    // This is intentional.
    #[allow(clippy::cast_possible_wrap)]
    let casted_size = size as i64;
    let query = sqlx::query_file!("./db_queries/insert_image.sql", key, casted_size, now)
        .execute(transaction)
        .await;

    if let Err(e) = query {
        warn!("Failed to add to db: {}", e);
    }

    cache.disk_cur_size.fetch_add(size, Ordering::Release);
}

/// Represents a Md5 hash that can be converted to and from a path. This is used
/// for compatibility with the official client, where the image id and on-disk
/// path is determined by file path.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Md5Hash(GenericArray<u8, <Md5 as md5::Digest>::OutputSize>);

impl Md5Hash {
    fn to_hex_string(self) -> String {
        format!("{:x}", self.0)
    }
}

impl TryFrom<&Path> for Md5Hash {
    type Error = ();

    fn try_from(path: &Path) -> Result<Self, Self::Error> {
        let mut iter = path.iter();
        let file_name = iter.next_back().ok_or(())?;
        let chapter_hash = iter.next_back().ok_or(())?;
        let is_data_saver = iter.next_back().ok_or(())? == "saver";
        let mut hasher = Md5::new();
        if is_data_saver {
            hasher.update("saver");
        }
        hasher.update(chapter_hash.as_bytes());
        hasher.update(".");
        hasher.update(file_name.as_bytes());
        Ok(Self(hasher.finalize()))
    }
}

impl From<Md5Hash> for PathBuf {
    fn from(hash: Md5Hash) -> Self {
        let hex_value = dbg!(hash.to_hex_string());
        let path = hex_value[0..3]
            .chars()
            .rev()
            .map(|char| Self::from(char.to_string()))
            .reduce(|first, second| first.join(second));

        match path {
            Some(p) => p.join(hex_value),
            None => unsafe { unreachable_unchecked() }, // literally not possible
        }
    }
}

#[async_trait]
impl Cache for DiskCache {
    async fn get(
        &self,
        key: &CacheKey,
    ) -> Option<Result<(CacheStream, ImageMetadata), CacheError>> {
        let channel = self.db_update_channel_sender.clone();

        // TODO: Check legacy path as well

        let path = Arc::new(self.disk_path.clone().join(PathBuf::from(key)));
        let path_0 = Arc::clone(&path);

        tokio::spawn(async move { channel.send(DbMessage::Get(path_0)).await });

        super::fs::read_file_from_path(&path).await.map(|res| {
            res.map(|(stream, _, metadata)| (stream, metadata))
                .map_err(|_| CacheError::DecryptionFailure)
        })
    }

    async fn put(
        &self,
        key: CacheKey,
        image: bytes::Bytes,
        metadata: ImageMetadata,
    ) -> Result<(), CacheError> {
        let channel = self.db_update_channel_sender.clone();

        let path = Arc::new(self.disk_path.clone().join(PathBuf::from(&key)));
        let path_0 = Arc::clone(&path);

        let db_callback = |size: u64| async move {
            std::mem::drop(channel.send(DbMessage::Put(path_0, size)).await);
        };

        super::fs::write_file(&path, key, image, metadata, db_callback, None)
            .await
            .map_err(CacheError::from)
    }
}

#[async_trait]
impl CallbackCache for DiskCache {
    async fn put_with_on_completed_callback(
        &self,
        key: CacheKey,
        image: bytes::Bytes,
        metadata: ImageMetadata,
        on_complete: Sender<(CacheKey, bytes::Bytes, ImageMetadata, u64)>,
    ) -> Result<(), CacheError> {
        let channel = self.db_update_channel_sender.clone();

        let path = Arc::new(self.disk_path.clone().join(PathBuf::from(&key)));
        let path_0 = Arc::clone(&path);

        let db_callback = |size: u64| async move {
            // We don't care about the result of the send
            std::mem::drop(channel.send(DbMessage::Put(path_0, size)).await);
        };

        super::fs::write_file(&path, key, image, metadata, db_callback, Some(on_complete))
            .await
            .map_err(CacheError::from)
    }
}

#[cfg(test)]
mod disk_cache {
    use std::error::Error;
    use std::path::PathBuf;
    use std::sync::atomic::Ordering;

    use chrono::Utc;
    use sqlx::SqlitePool;

    use crate::units::Bytes;

    use super::DiskCache;

    #[tokio::test]
    async fn db_is_initialized() -> Result<(), Box<dyn Error>> {
        let conn = SqlitePool::connect("sqlite::memory:").await?;
        let _cache = DiskCache::from_db_pool(conn.clone(), Bytes(1000), PathBuf::new()).await;
        let res = sqlx::query("select * from Images").execute(&conn).await;
        assert!(res.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn db_initializes_empty() -> Result<(), Box<dyn Error>> {
        let conn = SqlitePool::connect("sqlite::memory:").await?;
        let cache = DiskCache::from_db_pool(conn.clone(), Bytes(1000), PathBuf::new()).await;
        assert_eq!(cache.disk_cur_size.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn db_can_load_from_existing() -> Result<(), Box<dyn Error>> {
        let conn = SqlitePool::connect("sqlite::memory:").await?;
        sqlx::query_file!("./db_queries/init.sql")
            .execute(&conn)
            .await?;

        let now = Utc::now();
        sqlx::query_file!("./db_queries/insert_image.sql", "a", 4, now)
            .execute(&conn)
            .await?;

        let now = Utc::now();
        sqlx::query_file!("./db_queries/insert_image.sql", "b", 15, now)
            .execute(&conn)
            .await?;

        let cache = DiskCache::from_db_pool(conn.clone(), Bytes(1000), PathBuf::new()).await;
        assert_eq!(cache.disk_cur_size.load(Ordering::SeqCst), 19);
        Ok(())
    }
}

#[cfg(test)]
mod db {
    use chrono::{DateTime, Utc};
    use sqlx::{Connection, Row, SqliteConnection};
    use std::error::Error;

    use super::*;

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn get() -> Result<(), Box<dyn Error>> {
        let (cache, _) = DiskCache::in_memory();
        let path = PathBuf::from_str("a/b/c")?;
        let mut conn = SqliteConnection::connect("sqlite::memory:").await?;
        sqlx::query_file!("./db_queries/init.sql")
            .execute(&mut conn)
            .await?;

        // Add an entry
        let mut transaction = conn.begin().await?;
        handle_db_put(&path, 10, &cache, &mut transaction).await;
        transaction.commit().await?;

        let time_fence = Utc::now();

        let mut transaction = conn.begin().await?;
        handle_db_get(&path, &mut transaction).await;
        transaction.commit().await?;

        let mut rows: Vec<_> = sqlx::query("select * from Images")
            .fetch(&mut conn)
            .collect()
            .await;
        assert_eq!(rows.len(), 1);
        let entry = rows.pop().unwrap()?;
        assert!(time_fence < entry.get::<'_, DateTime<Utc>, _>("accessed"));

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn put() -> Result<(), Box<dyn Error>> {
        let (cache, _) = DiskCache::in_memory();
        let path = PathBuf::from_str("a/b/c")?;
        let mut conn = SqliteConnection::connect("sqlite::memory:").await?;
        sqlx::query_file!("./db_queries/init.sql")
            .execute(&mut conn)
            .await?;

        let mut transaction = conn.begin().await?;
        let transaction_time = Utc::now();
        handle_db_put(&path, 10, &cache, &mut transaction).await;
        transaction.commit().await?;

        let mut rows: Vec<_> = sqlx::query("select * from Images")
            .fetch(&mut conn)
            .collect()
            .await;
        assert_eq!(rows.len(), 1);

        let entry = rows.pop().unwrap()?;
        assert_eq!(entry.get::<'_, &str, _>("id"), "a/b/c");
        assert_eq!(entry.get::<'_, i64, _>("size"), 10);

        let accessed: DateTime<Utc> = entry.get("accessed");
        assert!(transaction_time < accessed);
        assert!(accessed < Utc::now());

        assert_eq!(cache.disk_cur_size.load(Ordering::SeqCst), 10);

        Ok(())
    }
}

#[cfg(test)]
mod md5_hash {
    use super::*;

    #[test]
    fn to_cache_path() {
        let hash = Md5Hash(
            *GenericArray::<_, <Md5 as md5::Digest>::OutputSize>::from_slice(&[
                0xab, 0xcd, 0xef, 0xab, 0xcd, 0xef, 0xab, 0xcd, 0xef, 0xab, 0xcd, 0xef, 0xab, 0xcd,
                0xef, 0xab,
            ]),
        );
        assert_eq!(
            PathBuf::from(hash).to_str(),
            Some("c/b/a/abcdefabcdefabcdefabcdefabcdefab")
        )
    }

    #[test]
    fn from_data_path() {
        let mut expected_hasher = Md5::new();
        expected_hasher.update("foo.bar.png");
        assert_eq!(
            Md5Hash::try_from(Path::new("data/foo/bar.png")),
            Ok(Md5Hash(expected_hasher.finalize()))
        );
    }

    #[test]
    fn from_data_saver_path() {
        let mut expected_hasher = Md5::new();
        expected_hasher.update("saverfoo.bar.png");
        assert_eq!(
            Md5Hash::try_from(Path::new("saver/foo/bar.png")),
            Ok(Md5Hash(expected_hasher.finalize()))
        );
    }

    #[test]
    fn can_handle_long_paths() {
        assert_eq!(
            Md5Hash::try_from(Path::new("a/b/c/d/e/f/g/saver/foo/bar.png")),
            Md5Hash::try_from(Path::new("saver/foo/bar.png")),
        );
    }

    #[test]
    fn from_invalid_paths() {
        assert!(Md5Hash::try_from(Path::new("foo/bar.png")).is_err());
        assert!(Md5Hash::try_from(Path::new("bar.png")).is_err());
        assert!(Md5Hash::try_from(Path::new("")).is_err());
    }
}
