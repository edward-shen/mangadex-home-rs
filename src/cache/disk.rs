//! Low memory caching stuff

use std::os::unix::prelude::OsStrExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use log::{debug, error, warn, LevelFilter};
use md5::digest::generic_array::GenericArray;
use md5::{Digest, Md5};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, SqlitePool};
use tokio::fs::remove_file;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;

use crate::units::Bytes;

use super::{Cache, CacheError, CacheKey, CacheStream, CallbackCache, ImageMetadata};

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
        let (db_tx, db_rx) = channel(128);
        let db_pool = {
            let db_url = format!("sqlite:{}/metadata.sqlite", disk_path.to_string_lossy());
            let mut options = SqliteConnectOptions::from_str(&db_url)
                .unwrap()
                .create_if_missing(true);
            options.log_statements(LevelFilter::Trace);
            let db = SqlitePool::connect_with(options).await.unwrap();

            // Run db init
            sqlx::query_file!("./db_queries/init.sql")
                .execute(&mut db.acquire().await.unwrap())
                .await
                .unwrap();

            db
        };

        // This is intentional.
        #[allow(clippy::cast_sign_loss)]
        let disk_cur_size = {
            let mut conn = db_pool.acquire().await.unwrap();
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

        // maybe this is better? in theory this sounds like it should be faster
        // but in practice I haven't seen any perf improvements

        // let ret = Arc::clone(&new_self);
        // std::thread::spawn(move || {
        //     tokio::runtime::Builder::new_current_thread()
        //         .enable_all()
        //         .build()
        //         .unwrap()
        //         .block_on(db_listener(
        //             Arc::clone(&new_self),
        //             db_rx,
        //             db_pool,
        //             disk_max_size.get() as u64 / 20 * 19,
        //         ))
        // });
        // ret

        tokio::spawn(db_listener(
            Arc::clone(&new_self),
            db_rx,
            db_pool,
            disk_max_size.get() as u64 / 20 * 19,
        ));

        new_self
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
        let now = chrono::Utc::now();
        let mut transaction = match db_pool.begin().await {
            Ok(transaction) => transaction,
            Err(e) => {
                error!("Failed to start a transaction to DB, cannot update DB. Disk cache may be losing track of files! {}", e);
                continue;
            }
        };
        for message in messages {
            match message {
                DbMessage::Get(entry) => {
                    let hash = Md5Hash::from(entry.as_path());
                    let hash_str = hash.to_hex_string();
                    let key = entry.as_os_str().to_str();
                    // let legacy_key = key.map();
                    let query = sqlx::query!(
                        "update Images set accessed = ? where id = ? or id = ?",
                        now,
                        key,
                        hash_str
                    )
                    .execute(&mut transaction)
                    .await;
                    if let Err(e) = query {
                        warn!("Failed to update timestamp in db for {:?}: {}", key, e);
                    }
                }
                DbMessage::Put(entry, size) => {
                    let key = entry.as_os_str().to_str();
                    {
                        // This is intentional.
                        #[allow(clippy::cast_possible_wrap)]
                        let size = size as i64;
                        let query = sqlx::query!(
                            "insert into Images (id, size, accessed) values (?, ?, ?) on conflict do nothing",
                            key,
                            size,
                            now,
                        )
                        .execute(&mut transaction)
                        .await;
                        if let Err(e) = query {
                            warn!("Failed to add {:?} to db: {}", key, e);
                        }
                    }

                    cache.disk_cur_size.fetch_add(size, Ordering::Release);
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

/// Represents a Md5 hash that can be converted to and from a path. This is used
/// for compatibility with the official client, where the image id and on-disk
/// path is determined by file path.
#[derive(Clone, Copy)]
struct Md5Hash(GenericArray<u8, <Md5 as md5::Digest>::OutputSize>);

impl Md5Hash {
    fn to_hex_string(self) -> String {
        format!("{:x}", self.0)
    }
}

impl From<&Path> for Md5Hash {
    fn from(path: &Path) -> Self {
        let mut iter = path.iter();
        let file_name = iter.next_back().unwrap();
        let chapter_hash = iter.next_back().unwrap();
        let is_data_saver = iter.next_back().unwrap() == "saver";
        let mut hasher = Md5::new();
        if is_data_saver {
            hasher.update("saver");
        }
        hasher.update(chapter_hash.as_bytes());
        hasher.update(".");
        hasher.update(file_name.as_bytes());
        Self(hasher.finalize())
    }
}

// Lint is overly aggressive here, as Md5Hash guarantees there to be at least 3
// bytes.
#[allow(clippy::fallible_impl_from)]
impl From<Md5Hash> for PathBuf {
    fn from(hash: Md5Hash) -> Self {
        let hex_value = hash.to_hex_string();
        hex_value[0..3]
            .chars()
            .rev()
            .map(|char| Self::from(char.to_string()))
            .reduce(|first, second| first.join(second))
            .unwrap() // literally not possible
            .join(hex_value)
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

        tokio::spawn(async move { channel.send(DbMessage::Get(path_0)).await });

        super::fs::read_file_from_path(&path).await.map(|res| {
            let (inner, maybe_header, metadata) = res?;
            CacheStream::new(inner, maybe_header)
                .map(|stream| (stream, metadata))
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
