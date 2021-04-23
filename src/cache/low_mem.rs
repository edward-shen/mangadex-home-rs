//! Low memory caching stuff

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use log::{warn, LevelFilter};
use sqlx::{sqlite::SqliteConnectOptions, ConnectOptions, SqlitePool};
use tokio::{
    fs::remove_file,
    sync::mpsc::{channel, Sender},
};
use tokio_stream::wrappers::ReceiverStream;

use super::{BoxedImageStream, Cache, CacheError, CacheKey, CacheStream, ImageMetadata};

pub struct LowMemCache {
    disk_path: PathBuf,
    disk_cur_size: AtomicU64,
    db_update_channel_sender: Sender<DbMessage>,
}

enum DbMessage {
    Get(Arc<PathBuf>),
    Put(Arc<PathBuf>, u32),
}

impl LowMemCache {
    /// Constructs a new low memory cache at the provided path and capaci ty.
    /// This internally spawns a task that will wait for filesystem
    /// notifications when a file has been written.
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(disk_max_size: u64, disk_path: PathBuf) -> Arc<Box<dyn Cache>> {
        let (db_tx, db_rx) = channel(128);
        let db_pool = {
            let db_url = format!("sqlite:{}/metadata.sqlite", disk_path.to_str().unwrap());
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

        let new_self: Arc<Box<dyn Cache>> = Arc::new(Box::new(Self {
            disk_path,
            disk_cur_size: AtomicU64::new(0),
            db_update_channel_sender: db_tx,
        }));

        // Spawns a new task that continuously listens for events received by
        // the channel, which informs the low memory cache the total size of the
        // item that was put into the cache.
        let new_self_0 = Arc::clone(&new_self);

        // Spawn a new task that will listen for updates to the db.
        tokio::spawn(async move {
            let db_pool = db_pool;
            let max_on_disk_size = disk_max_size / 20 * 19;
            let mut recv_stream = ReceiverStream::new(db_rx).ready_chunks(128);
            while let Some(messages) = recv_stream.next().await {
                let now = chrono::Utc::now();
                let mut transaction = db_pool.begin().await.unwrap();
                for message in messages {
                    match message {
                        DbMessage::Get(entry) => {
                            let key = entry.as_os_str().to_str();
                            let query = sqlx::query!(
                                "update Images set accessed = ? where id = ?",
                                now,
                                key
                            )
                            .execute(&mut transaction)
                            .await;
                            if let Err(e) = query {
                                warn!("Failed to update timestamp in db for {:?}: {}", key, e);
                            }
                        }
                        DbMessage::Put(entry, size) => {
                            let key = entry.as_os_str().to_str();
                            let query = sqlx::query!(
                                "insert into Images (id, size, accessed) values (?, ?, ?)",
                                key,
                                size,
                                now,
                            )
                            .execute(&mut transaction)
                            .await;
                            if let Err(e) = query {
                                warn!("Failed to add {:?} to db: {}", key, e);
                            }

                            new_self_0.increase_usage(size);
                        }
                    }
                }
                transaction.commit().await.unwrap();

                if new_self_0.on_disk_size() >= max_on_disk_size {
                    let mut conn = db_pool.acquire().await.unwrap();
                    let items = sqlx::query!(
                        "select id, size from Images order by accessed asc limit 1000"
                    )
                    .fetch_all(&mut conn)
                    .await
                    .unwrap();

                    let mut size_freed = 0;
                    for item in items {
                        size_freed += item.size as u64;
                        tokio::spawn(remove_file(item.id));
                    }

                    new_self_0.decrease_usage(size_freed);
                }
            }
        });

        new_self
    }
}

#[async_trait]
impl Cache for LowMemCache {
    async fn get(
        &self,
        key: Arc<CacheKey>,
    ) -> Option<Result<(CacheStream, ImageMetadata), CacheError>> {
        let channel = self.db_update_channel_sender.clone();

        let path = Arc::new(
            self.disk_path
                .clone()
                .join(PathBuf::from(Arc::clone(&key).as_ref())),
        );
        let path_0 = Arc::clone(&path);

        tokio::spawn(async move { channel.send(DbMessage::Get(path_0)).await });

        super::fs::read_file(&path)
            .await
            .map(|res| res.map_err(Into::into))
    }

    async fn put(
        &self,
        key: Arc<CacheKey>,
        image: BoxedImageStream,
        metadata: ImageMetadata,
    ) -> Result<CacheStream, CacheError> {
        let channel = self.db_update_channel_sender.clone();

        let path = Arc::new(self.disk_path.clone().join(PathBuf::from(key.as_ref())));
        let path_0 = Arc::clone(&path);

        let db_callback = |size: u32| async move {
            let _ = channel.send(DbMessage::Put(path_0, size)).await;
        };
        super::fs::write_file(&path, image, metadata, db_callback)
            .await
            .map_err(Into::into)
    }

    #[inline]
    fn increase_usage(&self, amt: u32) {
        self.disk_cur_size.fetch_add(amt as u64, Ordering::Release);
    }

    #[inline]
    fn on_disk_size(&self) -> u64 {
        (self.disk_cur_size.load(Ordering::Acquire) + 4095) / 4096 * 4096
    }

    fn decrease_usage(&self, amt: u64) {
        self.disk_cur_size.fetch_sub(amt, Ordering::Release);
    }
}
