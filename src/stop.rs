use reqwest::StatusCode;
use serde::Serialize;
use tracing::{info, warn};

use crate::client::HTTP_CLIENT;
use crate::config::ClientSecret;

const CONTROL_CENTER_STOP_URL: &str = "https://api.mangadex.network/ping";

#[derive(Serialize)]
struct StopRequest<'a> {
    secret: &'a ClientSecret,
}

pub async fn send_stop(secret: &ClientSecret) {
    match HTTP_CLIENT
        .inner()
        .post(CONTROL_CENTER_STOP_URL)
        .json(&StopRequest { secret })
        .send()
        .await
    {
        Ok(resp) => {
            if resp.status() == StatusCode::OK {
                info!("Successfully sent stop message to control center.");
            } else {
                warn!("Got weird response from server: {:?}", resp.headers());
            }
        }
        Err(e) => warn!("Got error while sending stop message: {}", e),
    }
}
