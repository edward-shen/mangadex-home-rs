use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::client::HTTP_CLIENT;
use crate::config::{ClientSecret, Config, OFFLINE_MODE};
use crate::ping::{Request, Response, CONTROL_CENTER_PING_URL};
use arc_swap::{ArcSwap, ArcSwapOption};
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::{CertifiedKey, RsaSigningKey, SigningKey};
use rustls::Certificate;
use sodiumoxide::crypto::box_::{PrecomputedKey, PRECOMPUTEDKEYBYTES};
use thiserror::Error;
use tracing::{error, info, warn};
use url::Url;

pub struct ServerState {
    pub precomputed_key: PrecomputedKey,
    pub image_server: Url,
    pub url: Url,
    pub url_overridden: bool,
}

pub static PREVIOUSLY_PAUSED: AtomicBool = AtomicBool::new(false);
pub static PREVIOUSLY_COMPROMISED: AtomicBool = AtomicBool::new(false);

pub static TLS_PREVIOUSLY_CREATED: OnceCell<ArcSwap<String>> = OnceCell::new();
static TLS_SIGNING_KEY: OnceCell<ArcSwap<RsaSigningKey>> = OnceCell::new();
static TLS_CERTS: OnceCell<ArcSwap<Vec<Certificate>>> = OnceCell::new();

pub static CERTIFIED_KEY: ArcSwapOption<CertifiedKey> = ArcSwapOption::const_empty();

#[derive(Error, Debug)]
pub enum ServerInitError {
    #[error(transparent)]
    MalformedResponse(reqwest::Error),
    #[error(transparent)]
    Timeout(reqwest::Error),
    #[error(transparent)]
    SendFailure(reqwest::Error),
    #[error("Failed to parse token key")]
    KeyParseError(String),
    #[error("Token key was not provided in initial request")]
    MissingTokenKey,
    #[error("Got error response from control center")]
    ErrorResponse,
}

impl ServerState {
    pub async fn init(secret: &ClientSecret, config: &Config) -> Result<Self, ServerInitError> {
        let resp = HTTP_CLIENT
            .inner()
            .post(CONTROL_CENTER_PING_URL)
            .json(&Request::from((secret, config)))
            .send()
            .await;

        match resp {
            Ok(resp) => match resp.json::<Response>().await {
                Ok(Response::Ok(mut resp)) => {
                    let key = resp
                        .token_key
                        .ok_or(ServerInitError::MissingTokenKey)
                        .and_then(|key| {
                            base64::decode(&key)
                                .ok()
                                .and_then(|k| PrecomputedKey::from_slice(&k))
                                .map_or_else(
                                    || {
                                        error!("Failed to parse token key: got {}", key);
                                        Err(ServerInitError::KeyParseError(key))
                                    },
                                    Ok,
                                )
                        })?;

                    PREVIOUSLY_COMPROMISED.store(resp.compromised, Ordering::Release);
                    if resp.compromised {
                        error!("Got compromised response from control center!");
                    }

                    PREVIOUSLY_PAUSED.store(resp.paused, Ordering::Release);
                    if resp.paused {
                        warn!("Control center has paused this node!");
                    }

                    if let Some(ref override_url) = config.override_upstream {
                        resp.image_server = override_url.clone();
                        warn!("Upstream URL overridden to: {}", resp.image_server);
                    } else {
                    }

                    info!("This client's URL has been set to {}", resp.url);

                    let tls = resp.tls.unwrap();

                    CERTIFIED_KEY.store(Some(Arc::new(CertifiedKey {
                        cert: tls.certs.clone(),
                        key: Arc::clone(&tls.priv_key) as Arc<dyn SigningKey>,
                        ocsp: None,
                        sct_list: None,
                    })));

                    std::mem::drop(
                        TLS_PREVIOUSLY_CREATED.set(ArcSwap::from_pointee(tls.created_at)),
                    );
                    std::mem::drop(TLS_SIGNING_KEY.set(ArcSwap::new(tls.priv_key)));
                    std::mem::drop(TLS_CERTS.set(ArcSwap::from_pointee(tls.certs)));

                    Ok(Self {
                        precomputed_key: key,
                        image_server: resp.image_server,
                        url: resp.url,
                        url_overridden: config.override_upstream.is_some(),
                    })
                }
                Ok(Response::Error(resp)) => {
                    error!(
                        "Got an {} error from upstream: {}",
                        resp.status as u16, resp.error
                    );
                    Err(ServerInitError::ErrorResponse)
                }
                Err(e) => {
                    error!("Got malformed response: {}. Is MangaDex@Home down?", e);
                    Err(ServerInitError::MalformedResponse(e))
                }
            },
            Err(e) => match e {
                e if e.is_timeout() => {
                    error!("Response timed out to control server. Is MangaDex@Home down?");
                    Err(ServerInitError::Timeout(e))
                }
                e => {
                    error!("Failed to send request: {}", e);
                    Err(ServerInitError::SendFailure(e))
                }
            },
        }
    }

    pub fn init_offline() -> Self {
        assert!(OFFLINE_MODE.load(Ordering::Acquire));
        Self {
            precomputed_key: PrecomputedKey::from_slice(&[41; PRECOMPUTEDKEYBYTES])
                .expect("expect offline config to work"),
            image_server: Url::from_file_path("/dev/null").expect("expect offline config to work"),
            url: Url::from_str("http://localhost").expect("expect offline config to work"),
            url_overridden: false,
        }
    }
}

pub struct RwLockServerState(pub RwLock<ServerState>);

pub struct DynamicServerCert;

impl ResolvesServerCert for DynamicServerCert {
    fn resolve(&self, _: ClientHello) -> Option<Arc<CertifiedKey>> {
        // TODO: wait for actix-web to use a new version of rustls so we can
        // remove cloning the certs all the time
        CERTIFIED_KEY.load_full()
    }
}
