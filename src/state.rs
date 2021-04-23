use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::config::{CliArgs, SEND_SERVER_VERSION, VALIDATE_TOKENS};
use crate::ping::{Request, Response, Tls, CONTROL_CENTER_PING_URL};
use log::{error, info, warn};
use parking_lot::RwLock;
use rustls::sign::CertifiedKey;
use rustls::ResolvesServerCert;
use sodiumoxide::crypto::box_::PrecomputedKey;
use url::Url;

pub struct ServerState {
    pub precomputed_key: PrecomputedKey,
    pub image_server: Url,
    pub tls_config: Tls,
    pub url: Url,
    pub url_overridden: bool,
}

pub static PREVIOUSLY_PAUSED: AtomicBool = AtomicBool::new(false);
pub static PREVIOUSLY_COMPROMISED: AtomicBool = AtomicBool::new(false);

impl ServerState {
    pub async fn init(secret: &str, config: &CliArgs) -> Result<Self, ()> {
        let resp = reqwest::Client::new()
            .post(CONTROL_CENTER_PING_URL)
            .json(&Request::from((secret, config)))
            .send()
            .await;

        if config.enable_server_string {
            warn!("Client will send Server header in responses. This is not recommended!");
            SEND_SERVER_VERSION.store(true, Ordering::Release);
        }

        match resp {
            Ok(resp) => match resp.json::<Response>().await {
                Ok(mut resp) => {
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

                    PREVIOUSLY_COMPROMISED.store(resp.paused, Ordering::Release);
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

                    if config.disable_token_validation {
                        warn!("Token validation is explicitly disabled!");
                    } else {
                        if resp.force_tokens {
                            info!("This client will validate tokens.");
                        } else {
                            info!("This client will not validate tokens.");
                        }
                        VALIDATE_TOKENS.store(resp.force_tokens, Ordering::Release);
                    }

                    Ok(Self {
                        precomputed_key: key,
                        image_server: resp.image_server,
                        tls_config: resp.tls.unwrap(),
                        url: resp.url,
                        url_overridden: config.override_upstream.is_some(),
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
        Some(CertifiedKey {
            cert: read_guard.tls_config.certs.clone(),
            key: Arc::clone(&read_guard.tls_config.priv_key),
            ocsp: None,
            sct_list: None,
        })
    }
}
