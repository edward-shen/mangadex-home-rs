use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::Ordering;
use std::{io::BufReader, sync::Arc};

use rustls::sign::{CertifiedKey, RsaSigningKey, SigningKey};
use rustls::{Certificate, PrivateKey};
use rustls_pemfile::{certs, rsa_private_keys};
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_repr::Deserialize_repr;
use sodiumoxide::crypto::box_::PrecomputedKey;
use tracing::{debug, error, info, warn};
use url::Url;

use crate::client::HTTP_CLIENT;
use crate::config::{ClientSecret, Config};
use crate::state::{
    RwLockServerState, CERTIFIED_KEY, PREVIOUSLY_COMPROMISED, PREVIOUSLY_PAUSED,
    TLS_PREVIOUSLY_CREATED,
};
use crate::units::{Bytes, BytesPerSecond, Port};
use crate::CLIENT_API_VERSION;

pub const CONTROL_CENTER_PING_URL: &str = "https://api.mangadex.network/ping";

#[derive(Serialize, Debug)]
pub struct Request<'a> {
    secret: &'a ClientSecret,
    port: Port,
    disk_space: Bytes,
    network_speed: BytesPerSecond,
    build_version: usize,
    tls_created_at: Option<String>,
    ip_address: Option<IpAddr>,
}

impl<'a> Request<'a> {
    fn from_config_and_state(secret: &'a ClientSecret, config: &Config) -> Self {
        Self {
            secret,
            port: config
                .external_address
                .and_then(|v| Port::new(v.port()))
                .unwrap_or(config.port),
            disk_space: config.disk_quota.into(),
            network_speed: config.network_speed.into(),
            build_version: CLIENT_API_VERSION,
            tls_created_at: TLS_PREVIOUSLY_CREATED
                .get()
                .map(|v| v.load().as_ref().clone()),
            ip_address: config.external_address.as_ref().map(SocketAddr::ip),
        }
    }
}

impl<'a> From<(&'a ClientSecret, &Config)> for Request<'a> {
    fn from((secret, config): (&'a ClientSecret, &Config)) -> Self {
        Self {
            secret,
            port: config
                .external_address
                .and_then(|v| Port::new(v.port()))
                .unwrap_or(config.port),
            disk_space: config.disk_quota.into(),
            network_speed: config.network_speed.into(),
            build_version: CLIENT_API_VERSION,
            tls_created_at: None,
            ip_address: config.external_address.as_ref().map(SocketAddr::ip),
        }
    }
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum Response {
    Ok(Box<OkResponse>),
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
    pub priv_key: Arc<RsaSigningKey>,
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
                                    v.pop()
                                        .and_then(|key| RsaSigningKey::new(&PrivateKey(key)).ok())
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
                        priv_key: Arc::new(priv_key),
                        certs: certificates.into_iter().map(Certificate).collect(),
                    }),
                    _ => Err(serde::de::Error::custom("Could not deserialize tls info")),
                }
            }
        }

        deserializer.deserialize_map(TlsVisitor)
    }
}

#[cfg(not(tarpaulin_include))]
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
    debug!("Sending ping request: {:?}", req);
    let resp = HTTP_CLIENT
        .inner()
        .post(CONTROL_CENTER_PING_URL)
        .json(&req)
        .send()
        .await;
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

                if let Some(tls) = resp.tls {
                    TLS_PREVIOUSLY_CREATED
                        .get()
                        .unwrap()
                        .swap(Arc::new(tls.created_at));
                    CERTIFIED_KEY.store(Some(Arc::new(CertifiedKey {
                        cert: tls.certs.clone(),
                        key: Arc::clone(&tls.priv_key) as Arc<dyn SigningKey>,
                        ocsp: None,
                        sct_list: None,
                    })));
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
