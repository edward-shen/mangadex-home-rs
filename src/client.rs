use std::{collections::HashMap, sync::Arc, time::Duration};

use actix_web::{
    http::{HeaderMap, HeaderName, HeaderValue},
    web::Data,
};
use bytes::Bytes;
use log::{debug, error, warn};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use reqwest::{
    header::{
        ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS, CACHE_CONTROL, CONTENT_LENGTH,
        CONTENT_TYPE, LAST_MODIFIED, X_CONTENT_TYPE_OPTIONS,
    },
    Client, StatusCode,
};
use tokio::sync::{
    watch::{channel, Receiver},
    Notify,
};

use crate::cache::{Cache, CacheKey, ImageMetadata};

pub static HTTP_CLIENT: Lazy<CachingClient> = Lazy::new(|| CachingClient {
    inner: Client::builder()
        .pool_idle_timeout(Duration::from_secs(180))
        .https_only(true)
        .http2_prior_knowledge()
        .build()
        .expect("Client initialization to work"),
    locks: RwLock::new(HashMap::new()),
});

pub struct CachingClient {
    inner: Client,
    locks: RwLock<HashMap<String, Receiver<FetchResult>>>,
}

#[derive(Clone, Debug)]
pub enum FetchResult {
    ServiceUnavailable,
    InternalServerError,
    Data(StatusCode, HeaderMap, Bytes),
    Processing,
}

impl CachingClient {
    pub async fn fetch_and_cache(
        &'static self,
        url: String,
        key: CacheKey,
        cache: Data<dyn Cache>,
    ) -> FetchResult {
        if let Some(recv) = self.locks.read().get(&url) {
            let mut recv = recv.clone();
            loop {
                if !matches!(*recv.borrow(), FetchResult::Processing) {
                    break;
                }
                if recv.changed().await.is_err() {
                    break;
                }
            }

            return recv.borrow().clone();
        }
        let url_0 = url.clone();

        let notify = Arc::new(Notify::new());
        let notify2 = Arc::clone(&notify);

        tokio::spawn(async move {
            let (tx, rx) = channel(FetchResult::Processing);

            self.locks.write().insert(url.clone(), rx);
            notify.notify_one();
            let resp = self.inner.get(&url).send().await;

            let resp = match resp {
                Ok(mut resp) => {
                    let content_type = resp.headers().get(CONTENT_TYPE);

                    let is_image = content_type
                        .map(|v| String::from_utf8_lossy(v.as_ref()).contains("image/"))
                        .unwrap_or_default();

                    if resp.status() != StatusCode::OK || !is_image {
                        warn!("Got non-OK or non-image response code from upstream, proxying and not caching result.");

                        let mut headers = HeaderMap::new();

                        if let Some(content_type) = content_type {
                            headers.insert(CONTENT_TYPE, content_type.clone());
                        }

                        headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
                        headers.insert(
                            ACCESS_CONTROL_ALLOW_ORIGIN,
                            HeaderValue::from_static("https://mangadex.org"),
                        );
                        headers
                            .insert(ACCESS_CONTROL_EXPOSE_HEADERS, HeaderValue::from_static("*"));
                        headers.insert(
                            CACHE_CONTROL,
                            HeaderValue::from_static("public, max-age=1209600"),
                        );
                        headers.insert(
                            HeaderName::from_static("timing-allow-origin"),
                            HeaderValue::from_static("https://mangadex.org"),
                        );

                        FetchResult::Data(
                            resp.status(),
                            headers,
                            resp.bytes().await.unwrap_or_default(),
                        )
                    } else {
                        let (content_type, length, last_mod) = {
                            let headers = resp.headers_mut();
                            (
                                headers.remove(CONTENT_TYPE),
                                headers.remove(CONTENT_LENGTH),
                                headers.remove(LAST_MODIFIED),
                            )
                        };

                        let body = resp.bytes().await.unwrap();

                        debug!("Inserting into cache");

                        let metadata = ImageMetadata::new(
                            content_type.clone(),
                            length.clone(),
                            last_mod.clone(),
                        )
                        .unwrap();

                        match cache.put(key, body.clone(), metadata).await {
                            Ok(()) => {
                                debug!("Done putting into cache");

                                let mut headers = HeaderMap::new();
                                if let Some(content_type) = content_type {
                                    headers.insert(CONTENT_TYPE, content_type);
                                }

                                if let Some(content_length) = length {
                                    headers.insert(CONTENT_LENGTH, content_length);
                                }

                                if let Some(last_modified) = last_mod {
                                    headers.insert(LAST_MODIFIED, last_modified);
                                }

                                headers.insert(
                                    X_CONTENT_TYPE_OPTIONS,
                                    HeaderValue::from_static("nosniff"),
                                );
                                headers.insert(
                                    ACCESS_CONTROL_ALLOW_ORIGIN,
                                    HeaderValue::from_static("https://mangadex.org"),
                                );
                                headers.insert(
                                    ACCESS_CONTROL_EXPOSE_HEADERS,
                                    HeaderValue::from_static("*"),
                                );
                                headers.insert(
                                    CACHE_CONTROL,
                                    HeaderValue::from_static("public, max-age=1209600"),
                                );
                                headers.insert(
                                    HeaderName::from_static("timing-allow-origin"),
                                    HeaderValue::from_static("https://mangadex.org"),
                                );
                                FetchResult::Data(StatusCode::OK, headers, body)
                            }
                            Err(e) => {
                                warn!("Failed to insert into cache: {}", e);
                                FetchResult::InternalServerError
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to fetch image from server: {}", e);
                    FetchResult::ServiceUnavailable
                }
            };
            // This shouldn't happen
            tx.send(resp).unwrap();
            self.locks.write().remove(&url);
        });

        notify2.notified().await;

        let mut recv = self.locks.read().get(&url_0).unwrap().clone();
        loop {
            if !matches!(*recv.borrow(), FetchResult::Processing) {
                break;
            }
            if recv.changed().await.is_err() {
                break;
            }
        }
        let resp = recv.borrow().clone();
        resp
    }

    #[inline]
    pub const fn inner(&self) -> &Client {
        &self.inner
    }
}
