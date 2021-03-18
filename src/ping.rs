use std::sync::Arc;

use awc::{error::SendRequestError, Client};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use sodiumoxide::crypto::box_::PrecomputedKey;
use url::Url;

use crate::{client_api_version, Config, RwLockServerState, CONTROL_CENTER_PING_URL};

#[derive(Serialize)]
pub struct PingRequest<'a> {
    secret: &'a str,
    port: u16,
    disk_space: usize,
    network_speed: usize,
    build_version: usize,
    tls_created_at: Option<String>,
}

impl<'a> PingRequest<'a> {
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

impl<'a> From<&'a Config> for PingRequest<'a> {
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

#[derive(Deserialize)]
pub(crate) struct PingResponse {
    pub(crate) image_server: Url,
    pub(crate) latest_build: usize,
    pub(crate) url: String,
    pub(crate) token_key: Option<String>,
    pub(crate) compromised: bool,
    pub(crate) paused: bool,
    pub(crate) disabled_tokens: bool,
    pub(crate) tls: Option<Tls>,
}

#[derive(Deserialize)]
pub(crate) struct Tls {
    pub created_at: String,
    pub private_key: Vec<u8>,
    pub certificate: Vec<u8>,
}

pub(crate) async fn update_server_state(req: &Config, data: &mut Arc<RwLockServerState>) {
    let req = PingRequest::from_config_and_state(req, data);
    let resp = Client::new()
        .post(CONTROL_CENTER_PING_URL)
        .send_json(&req)
        .await;
    match resp {
        Ok(mut resp) => match resp.json::<PingResponse>().await {
            Ok(resp) => {
                let mut write_guard = data.0.write();

                write_guard.image_server = resp.image_server;

                if let Some(key) = resp.token_key {
                    match PrecomputedKey::from_slice(key.as_bytes()) {
                        Some(key) => write_guard.precomputed_key = key,
                        None => error!("Failed to parse token key: got {}", key),
                    }
                }

                write_guard.disabled_tokens = resp.disabled_tokens;

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
            SendRequestError::Timeout => {
                error!("Response timed out to control server. Is MangaDex down?")
            }
            e => warn!("Failed to send request: {}", e),
        },
    }
}
