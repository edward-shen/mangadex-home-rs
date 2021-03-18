use std::convert::Infallible;

use actix_web::{dev::HttpResponseBuilder, get, web::Data, HttpRequest, Responder};
use actix_web::{web::Path, HttpResponse};
use awc::Client;
use base64::DecodeError;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream;
use log::{error, warn};
use serde::Deserialize;
use sodiumoxide::crypto::box_::{open_precomputed, Nonce, PrecomputedKey, NONCEBYTES};
use thiserror::Error;

use crate::{client_api_version, RwLockServerState};

const BASE64_CONFIG: base64::Config = base64::Config::new(base64::CharacterSet::UrlSafe, false);

const SERVER_ID_STRING: &str = concat!(
    env!("CARGO_CRATE_NAME"),
    " ",
    env!("CARGO_PKG_VERSION"),
    " (",
    client_api_version!(),
    ")",
);

enum ServerResponse {
    TokenValidationError(TokenValidationError),
    HttpResponse(HttpResponse),
}

impl Responder for ServerResponse {
    fn respond_to(self, req: &HttpRequest) -> HttpResponse {
        match self {
            ServerResponse::TokenValidationError(e) => e.respond_to(req),
            ServerResponse::HttpResponse(resp) => resp.respond_to(req),
        }
    }
}

#[get("/{token}/data/{chapter_hash}/{file_name}")]
async fn token_data(
    state: Data<RwLockServerState>,
    path: Path<(String, String, String)>,
) -> impl Responder {
    let (token, chapter_hash, file_name) = path.into_inner();
    if !state.0.read().disabled_tokens {
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
    if !state.0.read().disabled_tokens {
        if let Err(e) = validate_token(&state.0.read().precomputed_key, token, &chapter_hash) {
            return ServerResponse::TokenValidationError(e);
        }
    }
    fetch_image(state, chapter_hash, file_name, true).await
}

#[get("/data/{chapter_hash}/{file_name}")]
async fn no_token_data(
    state: Data<RwLockServerState>,
    path: Path<(String, String)>,
) -> impl Responder {
    let (chapter_hash, file_name) = path.into_inner();
    fetch_image(state, chapter_hash, file_name, false).await
}

#[get("/data-saver/{chapter_hash}/{file_name}")]
async fn no_token_data_saver(
    state: Data<RwLockServerState>,
    path: Path<(String, String)>,
) -> impl Responder {
    let (chapter_hash, file_name) = path.into_inner();
    fetch_image(state, chapter_hash, file_name, true).await
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
    fn respond_to(self, _: &HttpRequest) -> HttpResponse {
        push_headers(&mut HttpResponse::Forbidden()).finish()
    }
}

fn validate_token(
    precomputed_key: &PrecomputedKey,
    token: String,
    chapter_hash: &str,
) -> Result<(), TokenValidationError> {
    let data = base64::decode_config(token, BASE64_CONFIG)?;
    if data.len() < NONCEBYTES {
        return Err(TokenValidationError::IncompleteNonce);
    }

    let nonce = Nonce::from_slice(&data[..NONCEBYTES]).ok_or(TokenValidationError::InvalidNonce)?;
    let decrypted = open_precomputed(&data[NONCEBYTES..], &nonce, precomputed_key)
        .map_err(|_| TokenValidationError::DecryptionFailure)?;

    #[derive(Deserialize)]
    struct Token<'a> {
        expires: DateTime<Utc>,
        hash: &'a str,
    }

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

fn push_headers(builder: &mut HttpResponseBuilder) -> &mut HttpResponseBuilder {
    builder
        .insert_header(("X-Content-Type-Options", "nosniff"))
        .insert_header(("Access-Control-Allow-Origin", "https://mangadex.org"))
        .insert_header(("Access-Control-Expose-Headers", "*"))
        .insert_header(("Cache-Control", "public, max-age=1209600"))
        .insert_header(("Timing-Allow-Origin", "https://mangadex.org"))
        .insert_header(("Server", SERVER_ID_STRING))
}

async fn fetch_image(
    state: Data<RwLockServerState>,
    chapter_hash: String,
    file_name: String,
    is_data_saver: bool,
) -> ServerResponse {
    let key = (chapter_hash, file_name, is_data_saver);

    if let Some(cached) = state.0.write().cache.get(&key) {
        let data = cached.to_vec();
        let data: Vec<Result<Bytes, Infallible>> = data
            .chunks(1024)
            .map(|v| Ok(Bytes::from(v.to_vec())))
            .collect();
        return ServerResponse::HttpResponse(HttpResponse::Ok().streaming(stream::iter(data)));
    }

    let mut state = state.0.write();
    let resp = if is_data_saver {
        Client::new().get(format!(
            "{}/data-saver/{}/{}",
            state.image_server, &key.1, &key.2
        ))
    } else {
        Client::new().get(format!("{}/data/{}/{}", state.image_server, &key.1, &key.2))
    }
    .send()
    .await;

    match resp {
        Ok(mut resp) => match resp.body().await {
            Ok(bytes) => {
                state.cache.put(key, bytes.to_vec());
                let bytes: Vec<Result<Bytes, Infallible>> = bytes
                    .chunks(1024)
                    .map(|v| Ok(Bytes::from(v.to_vec())))
                    .collect();
                return ServerResponse::HttpResponse(
                    HttpResponse::Ok().streaming(stream::iter(bytes)),
                );
            }
            Err(e) => {
                warn!("Got payload error from image server: {}", e);
                todo!()
            }
        },
        Err(e) => {
            error!("Failed to fetch image from server: {}", e);
            todo!()
        }
    }
}
