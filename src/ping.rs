use std::sync::atomic::Ordering;
use std::{io::BufReader, sync::Arc};

use rustls::internal::pemfile::{certs, rsa_private_keys};
use rustls::sign::{RSASigningKey, SigningKey};
use rustls::Certificate;
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_repr::Deserialize_repr;
use sodiumoxide::crypto::box_::PrecomputedKey;
use tracing::{debug, error, info, warn};
use url::Url;

use crate::config::{ClientSecret, Config, UnstableOptions, VALIDATE_TOKENS};
use crate::state::{
    RwLockServerState, PREVIOUSLY_COMPROMISED, PREVIOUSLY_PAUSED, TLS_CERTS,
    TLS_PREVIOUSLY_CREATED, TLS_SIGNING_KEY,
};
use crate::units::{BytesPerSecond, Mebibytes, Port};
use crate::CLIENT_API_VERSION;

pub const CONTROL_CENTER_PING_URL: &str = "https://api.mangadex.network/ping";

#[derive(Serialize)]
pub struct Request<'a> {
    secret: &'a ClientSecret,
    port: Port,
    disk_space: Mebibytes,
    network_speed: BytesPerSecond,
    build_version: usize,
    tls_created_at: Option<String>,
}

impl<'a> Request<'a> {
    fn from_config_and_state(secret: &'a ClientSecret, config: &Config) -> Self {
        Self {
            secret,
            port: config.port,
            disk_space: config.disk_quota,
            network_speed: config.network_speed.into(),
            build_version: CLIENT_API_VERSION,
            tls_created_at: TLS_PREVIOUSLY_CREATED
                .get()
                .map(|v| v.load().as_ref().clone()),
        }
    }
}

impl<'a> From<(&'a ClientSecret, &Config)> for Request<'a> {
    fn from((secret, config): (&'a ClientSecret, &Config)) -> Self {
        Self {
            secret,
            port: config.port,
            disk_space: config.disk_quota,
            network_speed: config.network_speed.into(),
            build_version: CLIENT_API_VERSION,
            tls_created_at: None,
        }
    }
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum Response {
    Ok(OkResponse),
    Error(ErrorResponse),
}

#[derive(Deserialize, Debug)]
pub struct OkResponse {
    pub image_server: Url,
    pub latest_build: usize,
    pub url: Url,
    pub token_key: Option<String>,
    pub compromised: bool,
    pub paused: bool,
    #[serde(default)]
    pub force_tokens: bool,
    pub tls: Option<Tls>,
}

#[derive(Deserialize, Debug)]
pub struct ErrorResponse {
    pub error: String,
    pub status: ErrorCode,
}

#[derive(Deserialize_repr, Debug, Copy, Clone)]
#[repr(u16)]
pub enum ErrorCode {
    MalformedJson = 400,
    InvalidSecret = 401,
    InvalidContentType = 415,
}

pub struct Tls {
    pub created_at: String,
    pub priv_key: Arc<Box<dyn SigningKey>>,
    pub certs: Vec<Certificate>,
}

impl<'de> Deserialize<'de> for Tls {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct TlsVisitor;

        impl<'de> Visitor<'de> for TlsVisitor {
            type Value = Tls;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a tls struct")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut created_at = None;
                let mut priv_key = None;
                let mut certificates = None;

                while let Some((key, value)) = map.next_entry::<&str, String>()? {
                    match key {
                        "created_at" => created_at = Some(value.to_string()),
                        "private_key" => {
                            priv_key = rsa_private_keys(&mut BufReader::new(value.as_bytes()))
                                .ok()
                                .and_then(|mut v| {
                                    v.pop().and_then(|key| RSASigningKey::new(&key).ok())
                                });
                        }
                        "certificate" => {
                            certificates = certs(&mut BufReader::new(value.as_bytes())).ok();
                        }
                        _ => (), // Ignore extra fields
                    }
                }

                match (created_at, priv_key, certificates) {
                    (Some(created_at), Some(priv_key), Some(certificates)) => Ok(Tls {
                        created_at,
                        priv_key: Arc::new(Box::new(priv_key)),
                        certs: certificates,
                    }),
                    _ => Err(serde::de::Error::custom("Could not deserialize tls info")),
                }
            }
        }

        deserializer.deserialize_map(TlsVisitor)
    }
}

impl std::fmt::Debug for Tls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tls")
            .field("created_at", &self.created_at)
            .finish()
    }
}

pub async fn update_server_state(
    secret: &ClientSecret,
    cli: &Config,
    data: &mut Arc<RwLockServerState>,
) {
    let req = Request::from_config_and_state(secret, cli);
    let client = reqwest::Client::new();
    let resp = client.post(CONTROL_CENTER_PING_URL).json(&req).send().await;
    match resp {
        Ok(resp) => match resp.json::<Response>().await {
            Ok(Response::Ok(resp)) => {
                debug!("got write guard for server state");
                let mut write_guard = data.0.write();

                let image_server_changed = write_guard.image_server != resp.image_server;
                if !write_guard.url_overridden && image_server_changed {
                    write_guard.image_server = resp.image_server;
                } else if image_server_changed {
                    warn!("Ignoring new upstream url!");
                }

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

                if !cli
                    .unstable_options
                    .contains(&UnstableOptions::DisableTokenValidation)
                    && VALIDATE_TOKENS.load(Ordering::Acquire) != resp.force_tokens
                {
                    if resp.force_tokens {
                        info!("Client received command to enforce token validity.");
                    } else {
                        info!("Client received command to no longer enforce token validity");
                    }
                    VALIDATE_TOKENS.store(resp.force_tokens, Ordering::Release);
                }

                if let Some(tls) = resp.tls {
                    TLS_PREVIOUSLY_CREATED
                        .get()
                        .unwrap()
                        .swap(Arc::new(tls.created_at));
                    TLS_SIGNING_KEY.get().unwrap().swap(tls.priv_key);
                    TLS_CERTS.get().unwrap().swap(Arc::new(tls.certs));
                }

                let previously_compromised = PREVIOUSLY_COMPROMISED.load(Ordering::Acquire);
                if resp.compromised != previously_compromised {
                    PREVIOUSLY_COMPROMISED.store(resp.compromised, Ordering::Release);
                    if resp.compromised {
                        error!("Got compromised response from control center!");
                    } else if previously_compromised {
                        info!("No longer compromised!");
                    }
                }

                let previously_paused = PREVIOUSLY_PAUSED.load(Ordering::Acquire);
                if resp.paused != previously_paused {
                    PREVIOUSLY_PAUSED.store(resp.paused, Ordering::Release);
                    if resp.paused {
                        warn!("Control center has paused this node.");
                    } else {
                        info!("Control center is no longer pausing this node.");
                    }
                }

                if resp.url != write_guard.url {
                    info!("This client's URL has been updated to {}", resp.url);
                }

                debug!("dropping write guard for server state");
            }
            Ok(Response::Error(resp)) => {
                error!(
                    "Got an {} error from upstream: {}",
                    resp.status as u16, resp.error
                );
            }
            Err(e) => warn!("Got malformed response: {}", e),
        },
        Err(e) => match e {
            e if e.is_timeout() => {
                error!("Response timed out to control server. Is MangaDex down?");
            }
            e => warn!("Failed to send request: {}", e),
        },
    }
}
