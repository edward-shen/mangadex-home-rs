#![warn(clippy::pedantic, clippy::nursery)]
#![allow(clippy::future_not_send)] // We're end users, so this is ok

use std::env::{self, VarError};
use std::time::Duration;
use std::{num::ParseIntError, sync::Arc};

use crate::ping::Tls;
use actix_web::rt::{spawn, time};
use actix_web::web::Data;
use actix_web::{App, HttpServer};
use awc::{error::SendRequestError, Client};
use log::{debug, error, info, warn};
use lru::LruCache;
use parking_lot::RwLock;
use ping::{Request, Response};
use rustls::sign::{CertifiedKey, RSASigningKey};
use rustls::PrivateKey;
use rustls::{Certificate, NoClientAuth, ResolvesServerCert, ServerConfig};
use simple_logger::SimpleLogger;
use sodiumoxide::crypto::box_::PrecomputedKey;
use thiserror::Error;
use url::Url;

mod ping;
mod routes;
mod stop;

const CONTROL_CENTER_PING_URL: &str = "https://api.mangadex.network/ping";

#[macro_export]
macro_rules! client_api_version {
    () => {
        "30"
    };
}

struct ServerState {
    precomputed_key: PrecomputedKey,
    image_server: Url,
    tls_config: Tls,
    disabled_tokens: bool,
    url: String,
    cache: LruCache<(String, String, bool), CachedImage>,
}

struct CachedImage {
    data: Vec<u8>,
    content_type: Option<Vec<u8>>,
    content_length: Option<Vec<u8>>,
    last_modified: Option<Vec<u8>>,
}

impl ServerState {
    async fn init(config: &Config) -> Result<Self, ()> {
        let resp = Client::new()
            .post(CONTROL_CENTER_PING_URL)
            .send_json(&Request::from(config))
            .await;

        match resp {
            Ok(mut resp) => match resp.json::<Response>().await {
                Ok(resp) => {
                    let key = resp
                        .token_key
                        .and_then(|key| {
                            if let Some(key) = PrecomputedKey::from_slice(key.as_bytes()) {
                                Some(key)
                            } else {
                                error!("Failed to parse token key: got {}", key);
                                None
                            }
                        })
                        .unwrap();

                    if resp.compromised {
                        warn!("Got compromised response from control center!");
                    }

                    if resp.paused {
                        debug!("Got paused response from control center.");
                    }

                    info!("This client's URL has been set to {}", resp.url);

                    if resp.disabled_tokens {
                        info!("This client will not validated tokens");
                    }

                    Ok(Self {
                        precomputed_key: key,
                        image_server: resp.image_server,
                        tls_config: resp.tls.unwrap(),
                        disabled_tokens: resp.disabled_tokens,
                        url: resp.url,
                        cache: LruCache::new(1000),
                    })
                }
                Err(e) => {
                    warn!("Got malformed response: {}", e);
                    Err(())
                }
            },
            Err(e) => match e {
                SendRequestError::Timeout => {
                    error!("Response timed out to control server. Is MangaDex down?");
                    Err(())
                }
                e => {
                    warn!("Failed to send request: {}", e);
                    Err(())
                }
            },
        }
    }
}

pub struct RwLockServerState(RwLock<ServerState>);

impl ResolvesServerCert for RwLockServerState {
    fn resolve(&self, _: rustls::ClientHello) -> Option<CertifiedKey> {
        let read_guard = self.0.read();
        Some(CertifiedKey {
            cert: vec![Certificate(read_guard.tls_config.certificate.clone())],
            key: Arc::new(Box::new(
                RSASigningKey::new(&PrivateKey(read_guard.tls_config.private_key.clone())).unwrap(),
            )),
            ocsp: None,
            sct_list: None,
        })
    }
}

#[derive(Error, Debug)]
enum ServerError {
    #[error("There was a failure parsing config")]
    Config(#[from] VarError),
    #[error("Failed to parse an int")]
    ParseInt(#[from] ParseIntError),
}

#[actix_web::main]
async fn main() -> Result<(), std::io::Error> {
    dotenv::dotenv().ok();
    SimpleLogger::new().init().unwrap();

    let config = Config::new().unwrap();
    let port = config.port;

    let server = ServerState::init(&config).await.unwrap();
    let data_0 = Arc::new(RwLockServerState(RwLock::new(server)));
    let data_1 = Arc::clone(&data_0);
    let data_2 = Arc::clone(&data_0);

    spawn(async move {
        let mut interval = time::interval(Duration::from_secs(90));
        let mut data = Arc::clone(&data_0);
        loop {
            interval.tick().await;
            ping::update_server_state(&config, &mut data).await;
        }
    });

    let mut tls_config = ServerConfig::new(NoClientAuth::new());
    tls_config.cert_resolver = data_2;

    HttpServer::new(move || {
        App::new()
            .service(routes::token_data)
            .app_data(Data::from(Arc::clone(&data_1)))
    })
    .shutdown_timeout(60)
    .bind_rustls(format!("0.0.0.0:{}", port), tls_config)?
    .run()
    .await
}

pub struct Config {
    secret: String,
    port: u16,
    disk_quota: usize,
    network_speed: usize,
}

impl Config {
    fn new() -> Result<Self, ServerError> {
        let secret = env::var("CLIENT_SECRET")?;
        let port = env::var("PORT")?.parse::<u16>()?;
        let disk_quota = env::var("MAX_STORAGE_BYTES")?.parse::<usize>()?;
        let network_speed = env::var("MAX_NETWORK_SPEED")?.parse::<usize>()?;

        Ok(Self {
            secret,
            port,
            disk_quota,
            network_speed,
        })
    }
}
