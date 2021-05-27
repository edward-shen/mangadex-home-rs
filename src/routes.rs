use std::sync::atomic::Ordering;

use actix_web::error::ErrorNotFound;
use actix_web::http::header::{
    ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS, CACHE_CONTROL, CONTENT_LENGTH,
    CONTENT_TYPE, LAST_MODIFIED, X_CONTENT_TYPE_OPTIONS,
};
use actix_web::web::Path;
use actix_web::HttpResponseBuilder;
use actix_web::{get, web::Data, HttpRequest, HttpResponse, Responder};
use base64::DecodeError;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{Stream, TryStreamExt};
use log::{debug, error, info, warn};
use once_cell::sync::Lazy;
use prometheus::{Encoder, TextEncoder};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use sodiumoxide::crypto::box_::{open_precomputed, Nonce, PrecomputedKey, NONCEBYTES};
use thiserror::Error;

use crate::cache::{Cache, CacheKey, ImageMetadata, UpstreamError};
use crate::client_api_version;
use crate::config::{OFFLINE_MODE, SEND_SERVER_VERSION, VALIDATE_TOKENS};
use crate::metrics::{
    CACHE_HIT_COUNTER, CACHE_MISS_COUNTER, REQUESTS_DATA_COUNTER, REQUESTS_DATA_SAVER_COUNTER,
    REQUESTS_OTHER_COUNTER, REQUESTS_TOTAL_COUNTER,
};
use crate::state::RwLockServerState;

pub const BASE64_CONFIG: base64::Config = base64::Config::new(base64::CharacterSet::UrlSafe, false);

static HTTP_CLIENT: Lazy<Client> = Lazy::new(Client::new);

const SERVER_ID_STRING: &str = concat!(
    env!("CARGO_CRATE_NAME"),
    " ",
    env!("CARGO_PKG_VERSION"),
    " (",
    client_api_version!(),
    ") - Conforming to spec revision b82043289",
);

enum ServerResponse {
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

#[get("/")]
async fn index() -> impl Responder {
    HttpResponse::Ok().body(include_str!("index.html"))
}

#[allow(clippy::future_not_send)]
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

#[allow(clippy::future_not_send)]
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

    let resp = match HTTP_CLIENT.get(path).send().await {
        Ok(resp) => resp,
        Err(e) => {
            error!("{}", e);
            return ServerResponse::HttpResponse(HttpResponse::BadGateway().finish());
        }
    };
    let content_type = resp.headers().get(CONTENT_TYPE);
    let mut resp_builder = HttpResponseBuilder::new(resp.status());
    if let Some(content_type) = content_type {
        resp_builder.insert_header((CONTENT_TYPE, content_type));
    }
    push_headers(&mut resp_builder);

    ServerResponse::HttpResponse(resp_builder.body(resp.bytes().await.unwrap_or_default()))
}

#[allow(clippy::future_not_send)]
#[get("/metrics")]
pub async fn metrics() -> impl Responder {
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    TextEncoder::new()
        .encode(&metric_families, &mut buffer)
        .unwrap();
    String::from_utf8(buffer).unwrap()
}

#[derive(Error, Debug)]
enum TokenValidationError {
    #[error("Failed to decode base64 token.")]
    DecodeError(#[from] DecodeError),
    #[error("Nonce was too short.")]
    IncompleteNonce,
    #[error("Invalid nonce.")]
    InvalidNonce,
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
        push_headers(&mut HttpResponse::Forbidden()).finish()
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

    let nonce = Nonce::from_slice(&nonce).ok_or(TokenValidationError::InvalidNonce)?;
    let decrypted = open_precomputed(&encrypted, &nonce, precomputed_key)
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

#[inline]
fn push_headers(builder: &mut HttpResponseBuilder) -> &mut HttpResponseBuilder {
    builder
        .insert_header((X_CONTENT_TYPE_OPTIONS, "nosniff"))
        .insert_header((ACCESS_CONTROL_ALLOW_ORIGIN, "https://mangadex.org"))
        .insert_header((ACCESS_CONTROL_EXPOSE_HEADERS, "*"))
        .insert_header((CACHE_CONTROL, "public, max-age=1209600"))
        .insert_header(("Timing-Allow-Origin", "https://mangadex.org"));

    if SEND_SERVER_VERSION.load(Ordering::Acquire) {
        builder.insert_header(("Server", SERVER_ID_STRING));
    }

    builder
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
        _ => (),
    }

    CACHE_MISS_COUNTER.inc();

    // If in offline mode, return early since there's nothing else we can do
    if OFFLINE_MODE.load(Ordering::Acquire) {
        return ServerResponse::HttpResponse(
            ErrorNotFound("Offline mode enabled and image not in cache").into(),
        );
    }

    // It's important to not get a write lock before this request, else we're
    // holding the read lock until the await resolves.

    let resp = if is_data_saver {
        HTTP_CLIENT
            .get(format!(
                "{}/data-saver/{}/{}",
                state.0.read().image_server,
                &key.0,
                &key.1
            ))
            .send()
    } else {
        HTTP_CLIENT
            .get(format!(
                "{}/data/{}/{}",
                state.0.read().image_server,
                &key.0,
                &key.1
            ))
            .send()
    }
    .await;

    match resp {
        Ok(mut resp) => {
            let content_type = resp.headers().get(CONTENT_TYPE);

            let is_image = content_type
                .map(|v| String::from_utf8_lossy(v.as_ref()).contains("image/"))
                .unwrap_or_default();

            if resp.status() != StatusCode::OK || !is_image {
                warn!(
                    "Got non-OK or non-image response code from upstream, proxying and not caching result.",
                );

                let mut resp_builder = HttpResponseBuilder::new(resp.status());
                if let Some(content_type) = content_type {
                    resp_builder.insert_header((CONTENT_TYPE, content_type));
                }

                push_headers(&mut resp_builder);

                return ServerResponse::HttpResponse(
                    resp_builder.body(resp.bytes().await.unwrap_or_default()),
                );
            }

            let (content_type, length, last_mod) = {
                let headers = resp.headers_mut();
                (
                    headers.remove(CONTENT_TYPE),
                    headers.remove(CONTENT_LENGTH),
                    headers.remove(LAST_MODIFIED),
                )
            };

            let body = resp.bytes_stream().map_err(|e| e.into());

            debug!("Inserting into cache");

            let metadata = ImageMetadata::new(content_type, length, last_mod).unwrap();
            let stream = {
                match cache.put(key, Box::new(body), metadata).await {
                    Ok(stream) => stream,
                    Err(e) => {
                        warn!("Failed to insert into cache: {}", e);
                        return ServerResponse::HttpResponse(
                            HttpResponse::InternalServerError().finish(),
                        );
                    }
                }
            };

            debug!("Done putting into cache");

            construct_response(stream, &metadata)
        }
        Err(e) => {
            error!("Failed to fetch image from server: {}", e);
            ServerResponse::HttpResponse(
                push_headers(&mut HttpResponse::ServiceUnavailable()).finish(),
            )
        }
    }
}

fn construct_response(
    data: impl Stream<Item = Result<Bytes, UpstreamError>> + Unpin + 'static,
    metadata: &ImageMetadata,
) -> ServerResponse {
    debug!("Constructing response");

    let mut resp = HttpResponse::Ok();
    if let Some(content_type) = metadata.content_type {
        resp.append_header((CONTENT_TYPE, content_type.as_ref()));
    }

    if let Some(content_length) = metadata.content_length {
        resp.append_header((CONTENT_LENGTH, content_length));
    }

    if let Some(last_modified) = metadata.last_modified {
        resp.append_header((LAST_MODIFIED, last_modified.to_rfc2822()));
    }

    ServerResponse::HttpResponse(push_headers(&mut resp).streaming(data))
}
