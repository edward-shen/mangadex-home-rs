use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use actix_web::http::header::{HeaderMap, HeaderName, HeaderValue};
use actix_web::web::Data;
use bytes::Bytes;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use reqwest::header::{
    ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS, CACHE_CONTROL, CONTENT_LENGTH,
    CONTENT_TYPE, LAST_MODIFIED, X_CONTENT_TYPE_OPTIONS,
};
use reqwest::{Client, Proxy, StatusCode};
use tokio::sync::watch::{channel, Receiver};
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

use crate::cache::{Cache, CacheKey, ImageMetadata};
use crate::config::{DISABLE_CERT_VALIDATION, USE_PROXY};

pub static HTTP_CLIENT: Lazy<CachingClient> = Lazy::new(|| {
    let mut inner = Client::builder()
        .pool_idle_timeout(Duration::from_secs(180))
        .https_only(true)
        .http2_prior_knowledge();

    if let Some(socket_addr) = USE_PROXY.get() {
        info!(
            "Using {} as a proxy for upstream requests.",
            socket_addr.as_str()
        );
        inner = inner.proxy(Proxy::all(socket_addr.as_str()).unwrap());
    }

    if DISABLE_CERT_VALIDATION.load(Ordering::Acquire) {
        inner = inner.danger_accept_invalid_certs(true);
    }

    let inner = inner.build().expect("Client initialization to work");
    CachingClient {
        inner,
        locks: RwLock::new(HashMap::new()),
    }
});

#[cfg(not(tarpaulin_include))]
pub static DEFAULT_HEADERS: Lazy<HeaderMap> = Lazy::new(|| {
    let mut headers = HeaderMap::with_capacity(8);
    headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    headers.insert(
        ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("https://mangadex.org"),
    );
    headers.insert(ACCESS_CONTROL_EXPOSE_HEADERS, HeaderValue::from_static("*"));
    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=1209600"),
    );
    headers.insert(
        HeaderName::from_static("timing-allow-origin"),
        HeaderValue::from_static("https://mangadex.org"),
    );
    headers
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
        let maybe_receiver = {
            let lock = self.locks.read();
            lock.get(&url).map(Clone::clone)
        };
        if let Some(mut recv) = maybe_receiver {
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

        let notify = Arc::new(Notify::new());
        tokio::spawn(self.fetch_and_cache_impl(cache, url.clone(), key, Arc::clone(&notify)));
        notify.notified().await;

        let mut recv = self
            .locks
            .read()
            .get(&url)
            .expect("receiver to exist since we just made one")
            .clone();
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

    async fn fetch_and_cache_impl(
        &self,
        cache: Data<dyn Cache>,
        url: String,
        key: CacheKey,
        notify: Arc<Notify>,
    ) {
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

                    let mut headers = DEFAULT_HEADERS.clone();

                    if let Some(content_type) = content_type {
                        headers.insert(CONTENT_TYPE, content_type.clone());
                    }

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

                    let metadata =
                        ImageMetadata::new(content_type.clone(), length.clone(), last_mod.clone())
                            .unwrap();

                    match cache.put(key, body.clone(), metadata).await {
                        Ok(()) => {
                            debug!("Done putting into cache");

                            let mut headers = DEFAULT_HEADERS.clone();
                            if let Some(content_type) = content_type {
                                headers.insert(CONTENT_TYPE, content_type);
                            }

                            if let Some(content_length) = length {
                                headers.insert(CONTENT_LENGTH, content_length);
                            }

                            if let Some(last_modified) = last_mod {
                                headers.insert(LAST_MODIFIED, last_modified);
                            }

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
    }

    #[inline]
    pub const fn inner(&self) -> &Client {
        &self.inner
    }
}
