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
use sodiumoxide::hex;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, Sqlite, SqlitePool, Transaction};
use tokio::fs::{remove_file, rename, File};
use tokio::join;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info, instrument, warn};

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
        let cache_path = disk_path.to_string_lossy();
        // Migrate old to new path
        if rename(
            format!("{}/metadata.sqlite", cache_path),
            format!("{}/metadata.db", cache_path),
        )
        .await
        .is_ok()
        {
            info!("Found old metadata file, migrating to new location.");
        }

        let db_pool = {
            let db_url = format!("sqlite:{}/metadata.db", cache_path);
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
    // This is in a receiver stream to process up to 128 simultaneous db updates
    // in one transaction
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
                tokio::spawn(remove_file_handler(item.id));
            }

            cache.disk_cur_size.fetch_sub(size_freed, Ordering::Release);
        }
    }
}

/// Returns if a file was successfully deleted.
async fn remove_file_handler(key: String) -> bool {
    let error = if let Err(e) = remove_file(&key).await {
        e
    } else {
        return true;
    };

    if error.kind() != std::io::ErrorKind::NotFound {
        warn!("Failed to delete file `{}` from cache: {}", &key, error);
        return false;
    }

    if let Ok(bytes) = hex::decode(&key) {
        if bytes.len() != 16 {
            warn!("Failed to delete file `{}`; invalid hash size.", &key);
            return false;
        }

        let hash = Md5Hash(*GenericArray::from_slice(&bytes));
        let path: PathBuf = hash.into();
        if let Err(e) = remove_file(&path).await {
            warn!(
                "Failed to delete file `{}` from cache: {}",
                path.to_string_lossy(),
                e
            );
            false
        } else {
            true
        }
    } else {
        warn!("Failed to delete file `{}`; not a md5hash.", &key);
        false
    }
}

#[instrument(level = "debug", skip(transaction))]
async fn handle_db_get(entry: &Path, transaction: &mut Transaction<'_, Sqlite>) {
    let key = entry.as_os_str().to_str();
    let now = chrono::Utc::now();
    let query = sqlx::query!("update Images set accessed = ? where id = ?", now, key)
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
        let hex_value = hash.to_hex_string();
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

        let path = Arc::new(self.disk_path.clone().join(PathBuf::from(key)));
        let path_0 = Arc::clone(&path);

        let legacy_path = Md5Hash::try_from(path_0.as_path())
            .map(PathBuf::from)
            .map(|path| self.disk_path.clone().join(path))
            .map(Arc::new);

        // Get file and path of first existing location path
        let (file, path) = if let Ok(legacy_path) = legacy_path {
            let maybe_files = join!(
                File::open(legacy_path.as_path()),
                File::open(path.as_path()),
            );
            match maybe_files {
                (Ok(f), _) => Some((f, legacy_path)),
                (_, Ok(f)) => Some((f, path)),
                _ => return None,
            }
        } else {
            File::open(path.as_path())
                .await
                .ok()
                .map(|file| (file, path))
        }?;

        tokio::spawn(async move { channel.send(DbMessage::Get(path)).await });

        super::fs::read_file(file).await.map(|res| {
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
mod remove_file_handler {

    use std::error::Error;

    use tempfile::tempdir;
    use tokio::fs::{create_dir_all, remove_dir_all};

    use super::*;

    #[tokio::test]
    async fn should_not_panic_on_invalid_path() {
        assert!(!remove_file_handler("/this/is/a/non-existent/path/".to_string()).await);
    }

    #[tokio::test]
    async fn should_not_panic_on_invalid_hash() {
        assert!(!remove_file_handler("68b329da9893e34099c7d8ad5cb9c940".to_string()).await);
    }

    #[tokio::test]
    async fn should_not_panic_on_malicious_hashes() {
        assert!(!remove_file_handler("68b329da9893e34".to_string()).await);
        assert!(
            !remove_file_handler("68b329da9893e34099c7d8ad5cb9c940aaaaaaaaaaaaaaaaaa".to_string())
                .await
        );
    }

    #[tokio::test]
    async fn should_delete_existing_file() -> Result<(), Box<dyn Error>> {
        let temp_dir = tempdir()?;
        let mut dir_path = temp_dir.path().to_path_buf();
        dir_path.push("abc123.png");

        // create a file, it can be empty
        File::create(&dir_path).await?;

        assert!(remove_file_handler(dir_path.to_string_lossy().into_owned()).await);

        Ok(())
    }

    #[tokio::test]
    async fn should_delete_existing_hash() -> Result<(), Box<dyn Error>> {
        create_dir_all("b/8/6").await?;
        File::create("b/8/6/68b329da9893e34099c7d8ad5cb9c900").await?;

        assert!(remove_file_handler("68b329da9893e34099c7d8ad5cb9c900".to_string()).await);

        remove_dir_all("b").await?;
        Ok(())
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
