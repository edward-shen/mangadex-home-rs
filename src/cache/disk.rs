//! Low memory caching stuff

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use log::{error, warn, LevelFilter};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, SqlitePool};
use tokio::fs::remove_file;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;

use super::{
    BoxedImageStream, Cache, CacheError, CacheKey, CacheStream, CallbackCache, ImageMetadata,
};

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
    pub async fn new(disk_max_size: u64, disk_path: PathBuf) -> Arc<Self> {
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

        tokio::spawn(db_listener(
            Arc::clone(&new_self),
            db_rx,
            db_pool,
            disk_max_size / 20 * 19,
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
                    let key = entry.as_os_str().to_str();
                    let query =
                        sqlx::query!("update Images set accessed = ? where id = ?", now, key)
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
            for item in items {
                // Can't be helped, SQLite doesn't support unsigned integers
                #[allow(clippy::cast_sign_loss)]
                {
                    size_freed += item.size as u64;
                }
                tokio::spawn(remove_file(item.id));
            }

            cache.disk_cur_size.fetch_sub(size_freed, Ordering::Release);
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

        tokio::spawn(async move { channel.send(DbMessage::Get(path_0)).await });

        super::fs::read_file(&path).await.map(|res| {
            let (inner, maybe_header, metadata) = res?;
            CacheStream::new(inner, maybe_header)
                .map(|stream| (stream, metadata))
                .map_err(|_| CacheError::DecryptionFailure)
        })
    }

    async fn put(
        &self,
        key: CacheKey,
        image: BoxedImageStream,
        metadata: ImageMetadata,
    ) -> Result<CacheStream, CacheError> {
        let channel = self.db_update_channel_sender.clone();

        let path = Arc::new(self.disk_path.clone().join(PathBuf::from(&key)));
        let path_0 = Arc::clone(&path);

        let db_callback = |size: u64| async move {
            std::mem::drop(channel.send(DbMessage::Put(path_0, size)).await);
        };

        super::fs::write_file(&path, key, image, metadata, db_callback, None)
            .await
            .map_err(CacheError::from)
            .and_then(|(inner, maybe_header)| {
                CacheStream::new(inner, maybe_header).map_err(|_| CacheError::DecryptionFailure)
            })
    }
}

#[async_trait]
impl CallbackCache for DiskCache {
    async fn put_with_on_completed_callback(
        &self,
        key: CacheKey,
        image: BoxedImageStream,
        metadata: ImageMetadata,
        on_complete: Sender<(CacheKey, Bytes, ImageMetadata, u64)>,
    ) -> Result<CacheStream, CacheError> {
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
            .and_then(|(inner, maybe_header)| {
                CacheStream::new(inner, maybe_header).map_err(|_| CacheError::DecryptionFailure)
            })
    }
}
