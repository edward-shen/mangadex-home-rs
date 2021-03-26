use std::{fmt::Display, path::PathBuf};

use futures::future::join_all;
use log::warn;
use lru::LruCache;
use serde::{Deserialize, Serialize};
use ssri::Integrity;

#[derive(PartialEq, Eq, Hash, Clone)]
pub struct CacheKey(pub String, pub String, pub bool);

impl Display for CacheKey {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		if self.2 {
			write!(f, "saver/{}/{}", self.0, self.1)
		} else {
			write!(f, "data/{}/{}", self.0, self.1)
		}
	}
}

#[derive(Serialize, Deserialize)]
pub struct CachedImage {
	pub data: Vec<u8>,
	pub content_type: Option<Vec<u8>>,
	pub content_length: Option<Vec<u8>>,
	pub last_modified: Option<Vec<u8>>,
}

impl CachedImage {
	#[inline]
	fn len(&self) -> usize {
		self.data.capacity()
				+ self
				.content_type
				.as_ref()
				.map(Vec::capacity)
				.unwrap_or_default()
				+ self
				.content_length
				.as_ref()
				.map(Vec::capacity)
				.unwrap_or_default()
				+ self
				.last_modified
				.as_ref()
				.map(Vec::capacity)
				.unwrap_or_default()
	}

	#[inline]
	fn shrink_to_fit(&mut self) {
		self.data.shrink_to_fit();
		self.content_length.as_mut().map(Vec::shrink_to_fit);
		self.last_modified.as_mut().map(Vec::shrink_to_fit);
	}
}

pub struct Cache {
	in_memory: LruCache<CacheKey, CachedImage>,
	memory_max_size: usize,
	memory_cur_size: usize,

	on_disk: LruCache<CacheKey, Integrity>,
	disk_path: PathBuf,
	disk_max_size: usize,
	disk_cur_size: usize,
}

impl Cache {
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

	pub async fn get(&mut self, key: &CacheKey) -> Option<&CachedImage> {
		if self.in_memory.contains(key) {
			return self.in_memory.get(key);
		}

		if let Some(disk_key) = self.on_disk.pop(key) {
			// extract from disk, if it exists
			let bytes = cacache::read_hash(&self.disk_path, &disk_key).await.ok()?;
			// We don't particularly care if we fail to delete from disk since
			// if it's an error it means it's already been dropped.
			cacache::remove_hash(&self.disk_path, &disk_key).await.ok();
			self.disk_cur_size -= bytes.len();
			let cached_image: CachedImage = match bincode::deserialize(&bytes) {
				Ok(image) => image,
				Err(e) => {
					warn!("Failed to serialize on-disk data?! {}", e);
					return None;
				}
			};

			// put into in-memory
			self.memory_cur_size += cached_image.len();
			self.put(key.clone(), cached_image).await;
		}

		None
	}

	pub async fn put(&mut self, key: CacheKey, mut image: CachedImage) {
		image.shrink_to_fit();
		let mut hot_evicted = vec![];
		let new_img_size = image.len();

		if self.memory_max_size >= new_img_size {
			// Evict oldest entries to make space for new image.
			while new_img_size + self.memory_cur_size > self.memory_max_size {
				match self.in_memory.pop_lru() {
					Some((key, evicted_image)) => {
						self.memory_cur_size -= evicted_image.len();
						hot_evicted.push((key, evicted_image));
					}
					None => unreachable!(concat!(
					"Invariant violated. Cache is empty but we already ",
					"guaranteed we can remove items from cache to make space."
					)),
				}
			}

			self.in_memory.put(key, image);
			self.memory_cur_size += new_img_size;
		} else {
			// Image was larger than memory capacity, push directly into cold
			// storage.
			self.push_into_cold(key, image).await;
		}

		// Push evicted hot entries into cold storage.
		for (key, image) in hot_evicted {
			self.push_into_cold(key, image).await;
		}
	}

	async fn push_into_cold(&mut self, key: CacheKey, image: CachedImage) {
		let image = bincode::serialize(&image).unwrap(); // This should never fail.
		let new_img_size = image.len();
		let mut to_drop = vec![];

		if self.disk_max_size >= new_img_size {
			// Add images to drop from cold cache into a queue
			while new_img_size + self.disk_cur_size > self.disk_max_size {
				match self.on_disk.pop_lru() {
					Some((key, disk_key)) => {
						// Basically, the assumption here is that if the meta
						// data was deleted, we can't assume that deleting the
						// value will yield any free space back, so we assume a
						// size of zero while this is the case. This also means
						// that we automatically drop broken links as well.
						let on_disk_size = cacache::metadata(&self.disk_path, key.to_string())
								.await
								.unwrap_or_default()
								.map(|metadata| metadata.size)
								.unwrap_or_default();
						self.disk_cur_size -= on_disk_size;

						to_drop.push(disk_key);
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

		let mut futs = vec![];
		for key in &to_drop {
			futs.push(cacache::remove_hash(&self.disk_path, key));
		}
		// Run all cold caching in parallel, we don't care if the removal fails
		// because it just means it's already been removed.
		join_all(futs).await;

		let new_disk_key = match cacache::write(&self.disk_path, key.to_string(), image).await {
			Ok(key) => key,
			Err(e) => {
				warn!(
					"failed to write to disk cache, dropping value instead: {}",
					e
				);
				return;
			}
		};
		self.on_disk.put(key.clone(), new_disk_key);
		self.disk_cur_size += new_img_size;
	}
}
