use std::sync::Arc;

use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use sodiumoxide::crypto::box_::PrecomputedKey;
use url::Url;

use crate::state::RwLockServerState;
use crate::{client_api_version, Config};

pub const CONTROL_CENTER_PING_URL: &str = "https://api.mangadex.network/ping";

#[derive(Serialize, Debug)]
pub struct Request<'a> {
    secret: &'a str,
    port: u16,
    disk_space: usize,
    network_speed: usize,
    build_version: usize,
    tls_created_at: Option<String>,
}

impl<'a> Request<'a> {
    fn from_config_and_state(config: &'a Config, state: &'a Arc<RwLockServerState>) -> Self {
        Self {
            secret: &config.secret,
            port: config.port,
            disk_space: config.disk_quota,
            network_speed: config.network_speed,
            build_version: client_api_version!().parse().unwrap(),
            tls_created_at: Some(state.0.read().tls_config.created_at.clone()),
        }
    }
}

#[allow(clippy::fallible_impl_from)]
impl<'a> From<&'a Config> for Request<'a> {
    fn from(config: &'a Config) -> Self {
        Self {
            secret: &config.secret,
            port: config.port,
            disk_space: config.disk_quota,
            network_speed: config.network_speed,
            build_version: client_api_version!().parse().unwrap(),
            tls_created_at: None,
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct Response {
    pub image_server: Url,
    pub latest_build: usize,
    pub url: String,
    pub token_key: Option<String>,
    pub compromised: bool,
    pub paused: bool,
    pub force_tokens: bool,
    pub tls: Option<Tls>,
}

#[derive(Deserialize, Debug)]
pub struct Tls {
    pub created_at: String,
    pub private_key: String,
    pub certificate: String,
}

pub async fn update_server_state(req: &Config, data: &mut Arc<RwLockServerState>) {
    let req = Request::from_config_and_state(req, data);
    let client = reqwest::Client::new();
    let resp = client.post(CONTROL_CENTER_PING_URL).json(&req).send().await;
    match resp {
        Ok(resp) => match resp.json::<Response>().await {
            Ok(resp) => {
                let mut write_guard = data.0.write();

                write_guard.image_server = resp.image_server;

                if let Some(key) = resp.token_key {
                    if let Some(key) = base64::decode(&key)
                        .ok()
                        .and_then(|k| PrecomputedKey::from_slice(&k))
                    {
                        write_guard.precomputed_key = key;
                    } else {
                        error!("Failed to parse token key: got {}", key);
                    }
                }

                write_guard.force_tokens = resp.force_tokens;

                if let Some(tls) = resp.tls {
                    write_guard.tls_config = tls;
                }

                if resp.compromised {
                    warn!("Got compromised response from control center!");
                }

                if resp.paused {
                    debug!("Got paused response from control center.");
                }

                if resp.url != write_guard.url {
                    info!("This client's URL has been updated to {}", resp.url);
                }
            }
            Err(e) => warn!("Got malformed response: {}", e),
        },
        Err(e) => match e {
            e if e.is_timeout() => {
                error!("Response timed out to control server. Is MangaDex down?")
            }
            e => warn!("Failed to send request: {}", e),
        },
    }
}
