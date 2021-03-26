use std::{
    io::BufReader,
    sync::{atomic::Ordering, Arc},
};

use crate::ping::{Request, Response, Tls, CONTROL_CENTER_PING_URL};
use crate::routes::SEND_SERVER_VERSION;
use crate::{cache::Cache, config::CliArgs};
use log::{error, info, warn};
use parking_lot::RwLock;
use rustls::internal::pemfile::{certs, rsa_private_keys};
use rustls::sign::{CertifiedKey, RSASigningKey};
use rustls::ResolvesServerCert;
use sodiumoxide::crypto::box_::PrecomputedKey;
use url::Url;

pub struct ServerState {
    pub precomputed_key: PrecomputedKey,
    pub image_server: Url,
    pub tls_config: Tls,
    pub force_tokens: bool,
    pub url: String,
    pub cache: Cache,
    pub log_state: LogState,
}

pub struct LogState {
    pub was_paused_before: bool,
}

impl ServerState {
    pub async fn init(secret: &str, config: &CliArgs) -> Result<Self, ()> {
        let resp = reqwest::Client::new()
            .post(CONTROL_CENTER_PING_URL)
            .json(&Request::from((secret, config)))
            .send()
            .await;

        SEND_SERVER_VERSION.store(config.enable_server_string, Ordering::Release);

        match resp {
            Ok(resp) => match resp.json::<Response>().await {
                Ok(resp) => {
                    let key = resp
                        .token_key
                        .and_then(|key| {
                            if let Some(key) = base64::decode(&key)
                                .ok()
                                .and_then(|k| PrecomputedKey::from_slice(&k))
                            {
                                Some(key)
                            } else {
                                error!("Failed to parse token key: got {}", key);
                                None
                            }
                        })
                        .unwrap();

                    if resp.compromised {
                        error!("Got compromised response from control center!");
                    }

                    if resp.paused {
                        warn!("Control center has paused this node!");
                    }

                    info!("This client's URL has been set to {}", resp.url);

                    if resp.force_tokens {
                        info!("This client will validate tokens.");
                    } else {
                        info!("This client will not validate tokens.");
                    }

                    Ok(Self {
                        precomputed_key: key,
                        image_server: resp.image_server,
                        tls_config: resp.tls.unwrap(),
                        force_tokens: resp.force_tokens,
                        url: resp.url,
                        cache: Cache::new(
                            config.memory_quota.get(),
                            config.disk_quota,
                            config.cache_path.clone(),
                        ),
                        log_state: LogState {
                            was_paused_before: resp.paused,
                        },
                    })
                }
                Err(e) => {
                    warn!("Got malformed response: {}", e);
                    Err(())
                }
            },
            Err(e) => match e {
                e if e.is_timeout() => {
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

pub struct RwLockServerState(pub RwLock<ServerState>);

impl ResolvesServerCert for RwLockServerState {
    fn resolve(&self, _: rustls::ClientHello) -> Option<CertifiedKey> {
        let read_guard = self.0.read();
        let priv_key = rsa_private_keys(&mut BufReader::new(
            read_guard.tls_config.private_key.as_bytes(),
        ))
        .ok()?
        .pop()
        .unwrap();

        let certs = certs(&mut BufReader::new(
            read_guard.tls_config.certificate.as_bytes(),
        ))
        .ok()?;

        Some(CertifiedKey {
            cert: certs,
            key: Arc::new(Box::new(RSASigningKey::new(&priv_key).unwrap())),
            ocsp: None,
            sct_list: None,
        })
    }
}
