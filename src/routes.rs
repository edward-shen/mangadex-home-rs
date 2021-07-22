use std::hint::unreachable_unchecked;
use std::sync::atomic::Ordering;

use actix_web::error::ErrorNotFound;
use actix_web::http::header::{CONTENT_LENGTH, CONTENT_TYPE, LAST_MODIFIED};
use actix_web::http::HeaderValue;
use actix_web::web::Path;
use actix_web::HttpResponseBuilder;
use actix_web::{get, web::Data, HttpRequest, HttpResponse, Responder};
use base64::DecodeError;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::Stream;
use prometheus::{Encoder, TextEncoder};
use serde::Deserialize;
use sodiumoxide::crypto::box_::{open_precomputed, Nonce, PrecomputedKey, NONCEBYTES};
use thiserror::Error;
use tracing::{debug, error, info, trace};

use crate::cache::{Cache, CacheKey, ImageMetadata, UpstreamError};
use crate::client::{FetchResult, DEFAULT_HEADERS, HTTP_CLIENT};
use crate::config::{OFFLINE_MODE, VALIDATE_TOKENS};
use crate::metrics::{
    CACHE_HIT_COUNTER, CACHE_MISS_COUNTER, REQUESTS_DATA_COUNTER, REQUESTS_DATA_SAVER_COUNTER,
    REQUESTS_OTHER_COUNTER, REQUESTS_TOTAL_COUNTER,
};
use crate::state::RwLockServerState;

const BASE64_CONFIG: base64::Config = base64::Config::new(base64::CharacterSet::UrlSafe, false);

pub enum ServerResponse {
    TokenValidationError(TokenValidationError),
    HttpResponse(HttpResponse),
}

impl Responder for ServerResponse {
    #[inline]
    fn respond_to(self, req: &HttpRequest) -> HttpResponse {
        match self {
            Self::TokenValidationError(e) => e.respond_to(req),
            Self::HttpResponse(resp) => {
                REQUESTS_TOTAL_COUNTER.inc();
                resp.respond_to(req)
            }
        }
    }
}

#[allow(clippy::unused_async)]
#[get("/")]
async fn index() -> impl Responder {
    HttpResponse::Ok().body(include_str!("index.html"))
}

#[get("/{token}/data/{chapter_hash}/{file_name}")]
async fn token_data(
    state: Data<RwLockServerState>,
    cache: Data<dyn Cache>,
    path: Path<(String, String, String)>,
) -> impl Responder {
    REQUESTS_DATA_COUNTER.inc();
    let (token, chapter_hash, file_name) = path.into_inner();
    if VALIDATE_TOKENS.load(Ordering::Acquire) {
        if let Err(e) = validate_token(&state.0.read().precomputed_key, token, &chapter_hash) {
            return ServerResponse::TokenValidationError(e);
        }
    }
    fetch_image(state, cache, chapter_hash, file_name, false).await
}

#[get("/{token}/data-saver/{chapter_hash}/{file_name}")]
async fn token_data_saver(
    state: Data<RwLockServerState>,
    cache: Data<dyn Cache>,
    path: Path<(String, String, String)>,
) -> impl Responder {
    REQUESTS_DATA_SAVER_COUNTER.inc();
    let (token, chapter_hash, file_name) = path.into_inner();
    if VALIDATE_TOKENS.load(Ordering::Acquire) {
        if let Err(e) = validate_token(&state.0.read().precomputed_key, token, &chapter_hash) {
            return ServerResponse::TokenValidationError(e);
        }
    }

    fetch_image(state, cache, chapter_hash, file_name, true).await
}

#[allow(clippy::future_not_send)]
pub async fn default(state: Data<RwLockServerState>, req: HttpRequest) -> impl Responder {
    REQUESTS_OTHER_COUNTER.inc();
    let path = &format!(
        "{}{}",
        state.0.read().image_server,
        req.path().chars().skip(1).collect::<String>()
    );

    if OFFLINE_MODE.load(Ordering::Acquire) {
        info!("Got unknown path in offline mode, returning 404: {}", path);
        return ServerResponse::HttpResponse(
            ErrorNotFound("Path is not valid in offline mode").into(),
        );
    }

    info!("Got unknown path, just proxying: {}", path);

    let mut resp = match HTTP_CLIENT.inner().get(path).send().await {
        Ok(resp) => resp,
        Err(e) => {
            error!("{}", e);
            return ServerResponse::HttpResponse(HttpResponse::BadGateway().finish());
        }
    };
    let content_type = resp.headers_mut().remove(CONTENT_TYPE);
    let mut resp_builder = HttpResponseBuilder::new(resp.status());
    let mut headers = DEFAULT_HEADERS.clone();
    if let Some(content_type) = content_type {
        headers.insert(CONTENT_TYPE, content_type);
    }
    // push_headers(&mut resp_builder);

    let mut resp = resp_builder.body(resp.bytes().await.unwrap_or_default());
    *resp.headers_mut() = headers;
    ServerResponse::HttpResponse(resp)
}

#[allow(clippy::unused_async)]
#[get("/prometheus")]
pub async fn metrics() -> impl Responder {
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    TextEncoder::new()
        .encode(&metric_families, &mut buffer)
        .unwrap();
    String::from_utf8(buffer).unwrap()
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum TokenValidationError {
    #[error("Failed to decode base64 token.")]
    DecodeError(#[from] DecodeError),
    #[error("Nonce was too short.")]
    IncompleteNonce,
    #[error("Decryption failed")]
    DecryptionFailure,
    #[error("The token format was invalid.")]
    InvalidToken,
    #[error("The token has expired.")]
    TokenExpired,
    #[error("Invalid chapter hash.")]
    InvalidChapterHash,
}

impl Responder for TokenValidationError {
    #[inline]
    fn respond_to(self, _: &HttpRequest) -> HttpResponse {
        let mut resp = HttpResponse::Forbidden().finish();
        *resp.headers_mut() = DEFAULT_HEADERS.clone();
        resp
    }
}

fn validate_token(
    precomputed_key: &PrecomputedKey,
    token: String,
    chapter_hash: &str,
) -> Result<(), TokenValidationError> {
    #[derive(Deserialize)]
    struct Token<'a> {
        expires: DateTime<Utc>,
        hash: &'a str,
    }

    let data = base64::decode_config(token, BASE64_CONFIG)?;
    if data.len() < NONCEBYTES {
        return Err(TokenValidationError::IncompleteNonce);
    }

    let (nonce, encrypted) = data.split_at(NONCEBYTES);

    let nonce = match Nonce::from_slice(nonce) {
        Some(nonce) => nonce,
        // We split at NONCEBYTES, so this should never happen.
        None => unsafe { unreachable_unchecked() },
    };
    let decrypted = open_precomputed(encrypted, &nonce, precomputed_key)
        .map_err(|_| TokenValidationError::DecryptionFailure)?;

    let parsed_token: Token =
        serde_json::from_slice(&decrypted).map_err(|_| TokenValidationError::InvalidToken)?;

    if parsed_token.expires < Utc::now() {
        return Err(TokenValidationError::TokenExpired);
    }

    if parsed_token.hash != chapter_hash {
        return Err(TokenValidationError::InvalidChapterHash);
    }

    debug!("Token validated!");

    Ok(())
}

#[allow(clippy::future_not_send)]
async fn fetch_image(
    state: Data<RwLockServerState>,
    cache: Data<dyn Cache>,
    chapter_hash: String,
    file_name: String,
    is_data_saver: bool,
) -> ServerResponse {
    let key = CacheKey(chapter_hash, file_name, is_data_saver);

    match cache.get(&key).await {
        Some(Ok((image, metadata))) => {
            CACHE_HIT_COUNTER.inc();
            return construct_response(image, &metadata);
        }
        Some(Err(_)) => {
            return ServerResponse::HttpResponse(HttpResponse::BadGateway().finish());
        }
        None => (),
    }

    CACHE_MISS_COUNTER.inc();

    // If in offline mode, return early since there's nothing else we can do
    if OFFLINE_MODE.load(Ordering::Acquire) {
        return ServerResponse::HttpResponse(
            ErrorNotFound("Offline mode enabled and image not in cache").into(),
        );
    }

    let url = if is_data_saver {
        format!(
            "{}/data-saver/{}/{}",
            state.0.read().image_server,
            &key.0,
            &key.1,
        )
    } else {
        format!("{}/data/{}/{}", state.0.read().image_server, &key.0, &key.1)
    };

    match HTTP_CLIENT.fetch_and_cache(url, key, cache).await {
        FetchResult::ServiceUnavailable => {
            ServerResponse::HttpResponse(HttpResponse::ServiceUnavailable().finish())
        }
        FetchResult::InternalServerError => {
            ServerResponse::HttpResponse(HttpResponse::InternalServerError().finish())
        }
        FetchResult::Data(status, headers, data) => {
            let mut resp = HttpResponseBuilder::new(status);
            let mut resp = resp.body(data);
            *resp.headers_mut() = headers;
            ServerResponse::HttpResponse(resp)
        }
        FetchResult::Processing => panic!("Race condition found with fetch result"),
    }
}

#[inline]
pub fn construct_response(
    data: impl Stream<Item = Result<Bytes, UpstreamError>> + Unpin + 'static,
    metadata: &ImageMetadata,
) -> ServerResponse {
    trace!("Constructing response");

    let mut resp = HttpResponse::Ok();

    let mut headers = DEFAULT_HEADERS.clone();
    if let Some(content_type) = metadata.content_type {
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_str(content_type.as_ref()).unwrap(),
        );
    }

    if let Some(content_length) = metadata.content_length {
        headers.insert(CONTENT_LENGTH, HeaderValue::from(content_length));
    }

    if let Some(last_modified) = metadata.last_modified {
        headers.insert(
            LAST_MODIFIED,
            HeaderValue::from_str(&last_modified.to_rfc2822()).unwrap(),
        );
    }

    let mut ret = resp.streaming(data);
    *ret.headers_mut() = headers;

    ServerResponse::HttpResponse(ret)
}

#[cfg(test)]
mod token_validation {
    use super::*;
    use sodiumoxide::crypto::box_::precompute;
    use sodiumoxide::crypto::box_::seal_precomputed;
    use sodiumoxide::crypto::box_::{gen_keypair, gen_nonce, PRECOMPUTEDKEYBYTES};

    #[test]
    fn invalid_base64() {
        let res = validate_token(
            &PrecomputedKey::from_slice(&b"1".repeat(PRECOMPUTEDKEYBYTES))
                .expect("valid test token"),
            "a".to_string(),
            "b",
        );

        assert_eq!(
            res,
            Err(TokenValidationError::DecodeError(
                DecodeError::InvalidLength
            ))
        );
    }

    #[test]
    fn not_long_enough_for_nonce() {
        let res = validate_token(
            &PrecomputedKey::from_slice(&b"1".repeat(PRECOMPUTEDKEYBYTES))
                .expect("valid test token"),
            "aGVsbG8gaW50ZXJuZXR-Cg==".to_string(),
            "b",
        );

        assert_eq!(res, Err(TokenValidationError::IncompleteNonce));
    }

    #[test]
    fn invalid_precomputed_key() {
        let precomputed_1 = {
            let (pk, sk) = gen_keypair();
            precompute(&pk, &sk)
        };
        let precomputed_2 = {
            let (pk, sk) = gen_keypair();
            precompute(&pk, &sk)
        };
        let nonce = gen_nonce();

        // Seal with precomputed_2, open with precomputed_1

        let data = seal_precomputed(b"hello world", &nonce, &precomputed_2);
        let data: Vec<u8> = nonce.as_ref().into_iter().copied().chain(data).collect();
        let data = base64::encode_config(data, BASE64_CONFIG);

        let res = validate_token(&precomputed_1, data, "b");
        assert_eq!(res, Err(TokenValidationError::DecryptionFailure));
    }

    #[test]
    fn invalid_token_data() {
        let precomputed = {
            let (pk, sk) = gen_keypair();
            precompute(&pk, &sk)
        };
        let nonce = gen_nonce();

        let data = seal_precomputed(b"hello world", &nonce, &precomputed);
        let data: Vec<u8> = nonce.as_ref().into_iter().copied().chain(data).collect();
        let data = base64::encode_config(data, BASE64_CONFIG);

        let res = validate_token(&precomputed, data, "b");
        assert_eq!(res, Err(TokenValidationError::InvalidToken));
    }

    #[test]
    fn token_must_have_valid_expiration() {
        let precomputed = {
            let (pk, sk) = gen_keypair();
            precompute(&pk, &sk)
        };
        let nonce = gen_nonce();

        let time = Utc::now() - chrono::Duration::weeks(1);
        let data = seal_precomputed(
            serde_json::json!({
                "expires": time.to_rfc3339(),
                "hash": "b",
            })
            .to_string()
            .as_bytes(),
            &nonce,
            &precomputed,
        );
        let data: Vec<u8> = nonce.as_ref().into_iter().copied().chain(data).collect();
        let data = base64::encode_config(data, BASE64_CONFIG);

        let res = validate_token(&precomputed, data, "b");
        assert_eq!(res, Err(TokenValidationError::TokenExpired));
    }

    #[test]
    fn token_must_have_valid_chapter_hash() {
        let precomputed = {
            let (pk, sk) = gen_keypair();
            precompute(&pk, &sk)
        };
        let nonce = gen_nonce();

        let time = Utc::now() + chrono::Duration::weeks(1);
        let data = seal_precomputed(
            serde_json::json!({
                "expires": time.to_rfc3339(),
                "hash": "b",
            })
            .to_string()
            .as_bytes(),
            &nonce,
            &precomputed,
        );
        let data: Vec<u8> = nonce.as_ref().into_iter().copied().chain(data).collect();
        let data = base64::encode_config(data, BASE64_CONFIG);

        let res = validate_token(&precomputed, data, "");
        assert_eq!(res, Err(TokenValidationError::InvalidChapterHash));
    }

    #[test]
    fn valid_token_returns_ok() {
        let precomputed = {
            let (pk, sk) = gen_keypair();
            precompute(&pk, &sk)
        };
        let nonce = gen_nonce();

        let time = Utc::now() + chrono::Duration::weeks(1);
        let data = seal_precomputed(
            serde_json::json!({
                "expires": time.to_rfc3339(),
                "hash": "b",
            })
            .to_string()
            .as_bytes(),
            &nonce,
            &precomputed,
        );
        let data: Vec<u8> = nonce.as_ref().into_iter().copied().chain(data).collect();
        let data = base64::encode_config(data, BASE64_CONFIG);

        let res = validate_token(&precomputed, data, "b");
        assert!(res.is_ok());
    }
}
