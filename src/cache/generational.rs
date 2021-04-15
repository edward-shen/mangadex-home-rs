use std::path::PathBuf;

use async_trait::async_trait;
use bytes::Bytes;
use log::{debug, warn};
use lru::LruCache;
use tokio::fs::{remove_file, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{Cache, CacheKey, CachedImage, ImageMetadata};

pub struct GenerationalCache {
    in_memory: LruCache<CacheKey, (CachedImage, ImageMetadata)>,
    memory_max_size: usize,
    memory_cur_size: usize,

    on_disk: LruCache<CacheKey, ImageMetadata>,
    disk_path: PathBuf,
    disk_max_size: usize,
    disk_cur_size: usize,
}

impl GenerationalCache {
    pub fn new(memory_max_size: usize, disk_max_size: usize, disk_path: PathBuf) -> Self {
        Self {
            in_memory: LruCache::unbounded(),
            memory_max_size,
            memory_cur_size: 0,

            on_disk: LruCache::unbounded(),
            disk_path,
            disk_max_size,
            disk_cur_size: 0,
        }
    }

    async fn push_into_cold(&mut self, key: CacheKey, image: CachedImage, metadata: ImageMetadata) {
        let new_img_size = image.0.len();
        let mut to_drop = vec![];

        if self.disk_max_size >= new_img_size {
            // Add images to drop from cold cache into a queue
            while new_img_size + self.disk_cur_size > self.disk_max_size {
                match self.on_disk.pop_lru() {
                    Some((key, _)) => {
                        let mut path = self.disk_path.clone();
                        path.push(PathBuf::from(key));
                        let async_file = File::open(&path).await;

                        // Basically, the assumption here is that if the meta
                        // data was deleted, we can't assume that deleting the
                        // value will yield any free space back, so we assume a
                        // size of zero while this is the case. This also means
                        // that we automatically drop broken links as well.
                        if let Ok(file) = async_file {
                            let file_size = file
                                .into_std()
                                .await
                                .metadata()
                                .map(|metadata| {
                                    #[cfg(target_os = "windows")]
                                    {
                                        use std::os::windows::fs::MetadataExt;
                                        metadata.file_size()
                                    }

                                    #[cfg(target_os = "linux")]
                                    {
                                        use std::os::unix::fs::MetadataExt;
                                        metadata.size()
                                    }
                                })
                                .unwrap_or_default();

                            self.disk_cur_size -= file_size as usize;
                        }

                        to_drop.push(path);
                    }
                    None => unreachable!(concat!(
                        "Invariant violated. Cache is empty but we already ",
                        "guaranteed we can remove items from cache to make space."
                    )),
                }
            }
        } else {
            warn!(
                "Got request to push file larger than maximum disk cache. Refusing to insert on disk! {}",
                key
            );
            return;
        }

        // Run all cold caching in parallel, we don't care if the removal fails
        // because it just means it's already been removed.
        //
        // We also don't care when it happens, so just spawn tasks to do them
        // later, whenever is convenient.
        for to_drop in to_drop {
            tokio::spawn(remove_file(to_drop));
        }

        let new_key_path = {
            let mut root = self.disk_path.clone();
            root.push(PathBuf::from(&key));
            root
        };

        match File::open(&new_key_path).await {
            Ok(mut file) => {
                debug!("Starting to write to file: {:?}", &new_key_path);
                match file.write_all(&image.0).await {
                    Ok(_) => {
                        self.on_disk.put(key, metadata);
                        self.disk_cur_size += new_img_size;
                        debug!(
                            "Successfully written data to disk for file {:?}",
                            new_key_path
                        );
                    }
                    Err(e) => {
                        warn!("Failed to write to {:?}: {}", new_key_path, e);
                    }
                }
            }
            Err(e) => {
                warn!("Failed to open {:?}: {}", new_key_path, e);
            }
        }
    }
}

#[async_trait]
impl Cache for GenerationalCache {
    async fn get(&mut self, key: &CacheKey) -> Option<&(CachedImage, ImageMetadata)> {
        if self.in_memory.contains(key) {
            return self.in_memory.get(key);
        }

        if let Some(metadata) = self.on_disk.pop(key) {
            let new_key_path = {
                let mut root = self.disk_path.clone();
                root.push(PathBuf::from(key));
                root
            };

            // extract from disk, if it exists
            let file = File::open(new_key_path.clone()).await;

            let mut buffer = if let Some(size) = metadata.content_length {
                Vec::with_capacity(size)
            } else {
                Vec::new()
            };

            match file {
                Ok(mut file) => {
                    match file.read_to_end(&mut buffer).await {
                        Ok(_) => {
                            // We don't particularly care if we fail to delete from disk since
                            // if it's an error it means it's already been dropped.
                            tokio::spawn(remove_file(new_key_path.clone()));
                        }
                        Err(e) => {
                            warn!("Failed to read from {:?}: {}", new_key_path, e);
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to open {:?}: {}", new_key_path, e);
                    return None;
                }
            }

            buffer.shrink_to_fit();

            self.disk_cur_size -= buffer.len();
            let image = CachedImage(Bytes::from(buffer));

            // Since we just put it in the in-memory cache it should be there
            // when we retrieve it
            self.put(key.clone(), image, metadata).await;
            return self.get(key).await;
        }

        None
    }

    #[inline]
    async fn put(&mut self, key: CacheKey, image: CachedImage, metadata: ImageMetadata) {
        let mut hot_evicted = vec![];
        let new_img_size = image.0.len();

        if self.memory_max_size >= new_img_size {
            // Evict oldest entires to make space for new image.
            while new_img_size + self.memory_cur_size > self.memory_max_size {
                match self.in_memory.pop_lru() {
                    Some((key, (image, metadata))) => {
                        self.memory_cur_size -= image.0.len();
                        hot_evicted.push((key, image, metadata));
                    }
                    None => unreachable!(concat!(
                        "Invariant violated. Cache is empty but we already ",
                        "guaranteed we can remove items from cache to make space."
                    )),
                }
            }

            self.in_memory.put(key, (image, metadata));
            self.memory_cur_size += new_img_size;
        } else {
            // Image was larger than memory capacity, push directly into cold
            // storage.
            self.push_into_cold(key, image, metadata).await;
        };

        // Push evicted hot entires into cold storage.
        for (key, image, metadata) in hot_evicted {
            self.push_into_cold(key, image, metadata).await;
        }
    }
}
