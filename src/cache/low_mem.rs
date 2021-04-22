//! Low memory caching stuff

use std::sync::{atomic::Ordering, Arc};
use std::{path::PathBuf, sync::atomic::AtomicU64};

use async_trait::async_trait;
use futures::StreamExt;
use sqlx::SqlitePool;
use tokio::sync::mpsc::{channel, unbounded_channel, Sender, UnboundedSender};
use tokio_stream::wrappers::ReceiverStream;

use super::{BoxedImageStream, Cache, CacheError, CacheKey, CacheStream, ImageMetadata};

pub struct LowMemCache {
    disk_path: PathBuf,
    disk_max_size: u64,
    disk_cur_size: AtomicU64,
    file_size_channel_sender: UnboundedSender<u64>,
    db_update_channel_sender: Sender<DbMessage>,
}

enum DbMessage {
    Get(CacheKey),
    Put(CacheKey, ImageMetadata),
}

impl LowMemCache {
    /// Constructs a new low memory cache at the provided path and capaci ty.
    /// This internally spawns a task that will wait for filesystem
    /// notifications when a file has been written.
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(disk_max_size: u64, disk_path: PathBuf) -> Arc<Box<dyn Cache>> {
        let (file_size_tx, mut file_size_rx) = unbounded_channel();
        let (db_tx, db_rx) = channel(128);
        let db_pool = {
            let db_url = format!(
                "sqlite:{}/metadata.sqlite?mode=rwc",
                disk_path.to_str().unwrap()
            );
            let db = SqlitePool::connect(&db_url).await.unwrap();

            // Run db init
            sqlx::query_file!("./db_queries/init.sql")
                .execute(&mut db.acquire().await.unwrap())
                .await
                .unwrap();

            db
        };

        let new_self: Arc<Box<dyn Cache>> = Arc::new(Box::new(Self {
            disk_path,
            disk_max_size,
            disk_cur_size: AtomicU64::new(0),
            file_size_channel_sender: file_size_tx,
            db_update_channel_sender: db_tx,
        }));

        // Spawns a new task that continuously listens for events received by
        // the channel, which informs the low memory cache the total size of the
        // item that was put into the cache.
        let new_self_0 = Arc::clone(&new_self);
        tokio::spawn(async move {
            // This will never return None, effectively a loop
            while let Some(new_size) = file_size_rx.recv().await {
                new_self_0.increase_usage(new_size).await;
            }
        });

        // Spawn a new task that will listen for updates to the db.
        tokio::spawn(async move {
            let db_pool = db_pool;
            let mut recv_stream = ReceiverStream::new(db_rx).ready_chunks(128);
            while let Some(messages) = recv_stream.next().await {
                let mut transaction = db_pool.begin().await.unwrap();
                for message in messages {}

                transaction.commit().await.unwrap();
            }
        });

        new_self
    }
}

#[async_trait]
impl Cache for LowMemCache {
    async fn get(
        &self,
        key: &CacheKey,
    ) -> Option<Result<(CacheStream, ImageMetadata), CacheError>> {
        let channel = self.db_update_channel_sender.clone();
        let key_0 = key.clone();

        tokio::spawn(async move { channel.send(DbMessage::Get(key_0)).await });

        let path = self.disk_path.clone().join(PathBuf::from(key.clone()));
        super::fs::read_file(&path)
            .await
            .map(|res| res.map_err(Into::into))
    }

    async fn put(
        &self,
        key: CacheKey,
        image: BoxedImageStream,
        metadata: ImageMetadata,
    ) -> Result<CacheStream, CacheError> {
        let channel = self.db_update_channel_sender.clone();
        let key_0 = key.clone();
        tokio::spawn(async move { channel.send(DbMessage::Put(key_0, metadata)).await });

        let path = self.disk_path.clone().join(PathBuf::from(key));

        super::fs::write_file(
            &path,
            image,
            metadata,
            self.file_size_channel_sender.clone(),
        )
        .await
        .map_err(Into::into)
    }

    async fn increase_usage(&self, amt: u64) {
        self.disk_cur_size.fetch_add(amt, Ordering::Release);
    }
}
