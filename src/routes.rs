use std::convert::Infallible;

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
use serde::Deserialize;
use sodiumoxide::crypto::box_::{open_precomputed, Nonce, PrecomputedKey, NONCEBYTES};
use thiserror::Error;

use crate::cache::{CacheKey, CachedImage};
use crate::client_api_version;
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
    path: Path<(String, String, String)>,
) -> impl Responder {
    let (token, chapter_hash, file_name) = path.into_inner();
    if state.0.read().force_tokens {
        if let Err(e) = validate_token(&state.0.read().precomputed_key, token, &chapter_hash) {
            return ServerResponse::TokenValidationError(e);
        }
    }

    fetch_image(state, chapter_hash, file_name, false).await
}

#[get("/{token}/data-saver/{chapter_hash}/{file_name}")]
async fn token_data_saver(
    state: Data<RwLockServerState>,
    path: Path<(String, String, String)>,
) -> impl Responder {
    let (token, chapter_hash, file_name) = path.into_inner();
    if state.0.read().force_tokens {
        if let Err(e) = validate_token(&state.0.read().precomputed_key, token, &chapter_hash) {
            return ServerResponse::TokenValidationError(e);
        }
    }
    fetch_image(state, chapter_hash, file_name, true).await
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
        .insert_header(("Timing-Allow-Origin", "https://mangadex.org"))
        .insert_header(("Server", SERVER_ID_STRING))
}

async fn fetch_image(
    state: Data<RwLockServerState>,
    chapter_hash: String,
    file_name: String,
    is_data_saver: bool,
) -> ServerResponse {
    let key = CacheKey(chapter_hash, file_name, is_data_saver);

    if let Some(cached) = state.0.write().cache.get(&key).await {
        return construct_response(cached);
    }

    let mut state = state.0.write();
    let resp = if is_data_saver {
        reqwest::get(format!(
            "{}/data-saver/{}/{}",
            state.image_server, &key.1, &key.2
        ))
    } else {
        reqwest::get(format!("{}/data/{}/{}", state.image_server, &key.1, &key.2))
    }
    .await;

    match resp {
        Ok(resp) => {
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

            let headers = resp.headers();
            let content_type = headers.get(CONTENT_TYPE).map(AsRef::as_ref).map(Vec::from);
            let content_length = headers
                .get(CONTENT_LENGTH)
                .map(AsRef::as_ref)
                .map(Vec::from);
            let last_modified = headers.get(LAST_MODIFIED).map(AsRef::as_ref).map(Vec::from);
            let body = resp.bytes().await;
            match body {
                Ok(bytes) => {
                    let cached = CachedImage {
                        data: bytes.to_vec(),
                        content_type,
                        content_length,
                        last_modified,
                    };
                    let resp = construct_response(&cached);
                    state.cache.put(key, cached).await;
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

fn construct_response(cached: &CachedImage) -> ServerResponse {
    let data: Vec<Result<Bytes, Infallible>> = cached
        .data
        .to_vec()
        .chunks(1460) // TCP MSS default size
        .map(|v| Ok(Bytes::from(v.to_vec())))
        .collect();
    let mut resp = HttpResponse::Ok();
    if let Some(content_type) = &cached.content_type {
        resp.append_header((CONTENT_TYPE, &**content_type));
    }
    if let Some(content_length) = &cached.content_length {
        resp.append_header((CONTENT_LENGTH, &**content_length));
    }
    if let Some(last_modified) = &cached.last_modified {
        resp.append_header((LAST_MODIFIED, &**last_modified));
    }

    return ServerResponse::HttpResponse(push_headers(&mut resp).streaming(stream::iter(data)));
}
