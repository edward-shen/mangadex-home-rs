use std::num::{NonZeroU16, NonZeroU64};
use std::sync::atomic::Ordering;
use std::{io::BufReader, sync::Arc};

use log::{debug, error, info, warn};
use rustls::internal::pemfile::{certs, rsa_private_keys};
use rustls::sign::{RSASigningKey, SigningKey};
use rustls::Certificate;
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Serialize};
use sodiumoxide::crypto::box_::PrecomputedKey;
use url::Url;

use crate::config::{CliArgs, VALIDATE_TOKENS};
use crate::state::{
    RwLockServerState, PREVIOUSLY_COMPROMISED, PREVIOUSLY_PAUSED, TLS_CERTS,
    TLS_PREVIOUSLY_CREATED, TLS_SIGNING_KEY,
};
use crate::{client_api_version, config::UnstableOptions};

pub const CONTROL_CENTER_PING_URL: &str = "https://api.mangadex.network/ping";

#[derive(Serialize, Debug)]
pub struct Request<'a> {
    secret: &'a str,
    port: NonZeroU16,
    disk_space: u64,
    network_speed: NonZeroU64,
    build_version: u64,
    tls_created_at: Option<String>,
}

impl<'a> Request<'a> {
    fn from_config_and_state(secret: &'a str, config: &CliArgs) -> Self {
        Self {
            secret,
            port: config.port,
            disk_space: config.disk_quota,
            network_speed: config.network_speed,
            build_version: client_api_version!()
                .parse()
                .expect("to parse the build version"),
            tls_created_at: TLS_PREVIOUSLY_CREATED
                .get()
                .map(|v| v.load().as_ref().to_owned()),
        }
    }
}

#[allow(clippy::fallible_impl_from)]
impl<'a> From<(&'a str, &CliArgs)> for Request<'a> {
    fn from((secret, config): (&'a str, &CliArgs)) -> Self {
        Self {
            secret,
            port: config.port,
            disk_space: config.disk_quota,
            network_speed: config.network_speed,
            build_version: client_api_version!()
                .parse()
                .expect("to parse the build version"),
            tls_created_at: None,
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct Response {
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
                                })
                        }
                        "certificate" => {
                            certificates = certs(&mut BufReader::new(value.as_bytes())).ok()
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

pub async fn update_server_state(secret: &str, cli: &CliArgs, data: &mut Arc<RwLockServerState>) {
    let req = Request::from_config_and_state(secret, cli);
    let client = reqwest::Client::new();
    let resp = client.post(CONTROL_CENTER_PING_URL).json(&req).send().await;
    match resp {
        Ok(resp) => match resp.json::<Response>().await {
            Ok(resp) => {
                debug!("got write guard for server state");
                let mut write_guard = data.0.write();

                if !write_guard.url_overridden && write_guard.image_server != resp.image_server {
                    warn!("Ignoring new upstream url!");
                    write_guard.image_server = resp.image_server;
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
                    } else {
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
