#![cfg(not(tarpaulin_include))]

use reqwest::StatusCode;
use serde::Serialize;
use tracing::{info, warn};

use crate::client::HTTP_CLIENT;
use crate::config::ClientSecret;

const CONTROL_CENTER_STOP_URL: &str = "https://api.mangadex.network/stop";

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

#[cfg(test)]
mod stop {
    use super::CONTROL_CENTER_STOP_URL;

    #[test]
    fn stop_url_does_not_have_ping_in_url() {
        // This looks like a dumb test, yes, but it ensures that clients don't
        // get marked compromised because apparently just sending a json obj
        // with just the secret is acceptable to the ping endpoint, which messes
        // up non-trivial client configs.
        assert!(!CONTROL_CENTER_STOP_URL.contains("ping"))
    }
}
