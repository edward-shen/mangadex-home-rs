use std::convert::Infallible;
use std::sync::atomic::Ordering;

use actix_web::dev::HttpResponseBuilder;
use actix_web::http::header::{
    ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS, CACHE_CONTROL, CONTENT_LENGTH,
    CONTENT_TYPE, LAST_MODIFIED, X_CONTENT_TYPE_OPTIONS,
};
use actix_web::web::Path;
use actix_web::{get, web::Data, HttpRequest, HttpResponse, Responder};
use base64::DecodeError;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream;
use log::{error, info, warn};
use parking_lot::Mutex;
use serde::Deserialize;
use sodiumoxide::crypto::box_::{open_precomputed, Nonce, PrecomputedKey, NONCEBYTES};
use thiserror::Error;

use crate::cache::{Cache, CacheKey, CachedImage, GenerationalCache, ImageMetadata};
use crate::client_api_version;
use crate::config::{SEND_SERVER_VERSION, VALIDATE_TOKENS};
use crate::state::RwLockServerState;

pub const BASE64_CONFIG: base64::Config = base64::Config::new(base64::CharacterSet::UrlSafe, false);

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
            Self::HttpResponse(resp) => resp.respond_to(req),
        }
    }
}

#[get("/{token}/data/{chapter_hash}/{file_name}")]
async fn token_data(
    state: Data<RwLockServerState>,
    cache: Data<Mutex<GenerationalCache>>,
    path: Path<(String, String, String)>,
) -> impl Responder {
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
    cache: Data<Mutex<GenerationalCache>>,
    path: Path<(String, String, String)>,
) -> impl Responder {
    let (token, chapter_hash, file_name) = path.into_inner();
    if VALIDATE_TOKENS.load(Ordering::Acquire) {
        if let Err(e) = validate_token(&state.0.read().precomputed_key, token, &chapter_hash) {
            return ServerResponse::TokenValidationError(e);
        }
    }
    fetch_image(state, cache, chapter_hash, file_name, true).await
}

pub async fn default(state: Data<RwLockServerState>, req: HttpRequest) -> impl Responder {
    let path = &format!(
        "{}{}",
        state.0.read().image_server,
        req.path().chars().skip(1).collect::<String>()
    );
    info!("Got unknown path, just proxying: {}", path);
    let resp = reqwest::get(path).await.unwrap();
    let content_type = resp.headers().get(CONTENT_TYPE);
    let mut resp_builder = HttpResponseBuilder::new(resp.status());
    if let Some(content_type) = content_type {
        resp_builder.insert_header((CONTENT_TYPE, content_type));
    }
    push_headers(&mut resp_builder);

    ServerResponse::HttpResponse(resp_builder.body(resp.bytes().await.unwrap_or_default()))
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

    let nonce = Nonce::from_slice(&data[..NONCEBYTES]).ok_or(TokenValidationError::InvalidNonce)?;
    let decrypted = open_precomputed(&data[NONCEBYTES..], &nonce, precomputed_key)
        .map_err(|_| TokenValidationError::DecryptionFailure)?;

    let parsed_token: Token =
        serde_json::from_slice(&decrypted).map_err(|_| TokenValidationError::InvalidToken)?;

    if parsed_token.expires < Utc::now() {
        return Err(TokenValidationError::TokenExpired);
    }

    if parsed_token.hash != chapter_hash {
        return Err(TokenValidationError::InvalidChapterHash);
    }

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

async fn fetch_image(
    state: Data<RwLockServerState>,
    cache: Data<Mutex<GenerationalCache>>,
    chapter_hash: String,
    file_name: String,
    is_data_saver: bool,
) -> ServerResponse {
    let key = CacheKey(chapter_hash, file_name, is_data_saver);

    if let Some((image, metadata)) = cache.lock().get(&key).await {
        return construct_response(image, metadata);
    }

    // It's important to not get a write lock before this request, else we're
    // holding the read lock until the await resolves.

    let resp = if is_data_saver {
        reqwest::get(format!(
            "{}/data-saver/{}/{}",
            state.0.read().image_server,
            &key.1,
            &key.2
        ))
    } else {
        reqwest::get(format!(
            "{}/data/{}/{}",
            state.0.read().image_server,
            &key.1,
            &key.2
        ))
    }
    .await;

    match resp {
        Ok(mut resp) => {
            let content_type = resp.headers().get(CONTENT_TYPE);

            let is_image = content_type
                .map(|v| String::from_utf8_lossy(v.as_ref()).contains("image/"))
                .unwrap_or_default();
            if resp.status() != 200 || !is_image {
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
            let body = resp.bytes().await;
            match body {
                Ok(bytes) => {
                    let cached = ImageMetadata::new(content_type, length, last_mod).unwrap();
                    let image = CachedImage(bytes);
                    let resp = construct_response(&image, &cached);
                    cache.lock().put(key, image, cached).await;
                    return resp;
                }
                Err(e) => {
                    warn!("Got payload error from image server: {}", e);
                    ServerResponse::HttpResponse(
                        push_headers(&mut HttpResponse::ServiceUnavailable()).finish(),
                    )
                }
            }
        }
        Err(e) => {
            error!("Failed to fetch image from server: {}", e);
            ServerResponse::HttpResponse(
                push_headers(&mut HttpResponse::ServiceUnavailable()).finish(),
            )
        }
    }
}

fn construct_response(cached: &CachedImage, metadata: &ImageMetadata) -> ServerResponse {
    let data: Vec<Result<Bytes, Infallible>> = cached
        .0
        .to_vec()
        .chunks(1460) // TCP MSS default size
        .map(|v| Ok(Bytes::from(v.to_vec())))
        .collect();
    let mut resp = HttpResponse::Ok();
    if let Some(content_type) = &metadata.content_type {
        resp.append_header((CONTENT_TYPE, content_type.as_ref()));
    }
    if let Some(content_length) = &metadata.content_length {
        resp.append_header((CONTENT_LENGTH, content_length.to_string()));
    }
    if let Some(last_modified) = &metadata.last_modified {
        resp.append_header((LAST_MODIFIED, last_modified.to_rfc2822()));
    }

    ServerResponse::HttpResponse(push_headers(&mut resp).streaming(stream::iter(data)))
}
