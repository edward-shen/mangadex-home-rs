use log::{info, warn};
use serde::Serialize;

const CONTROL_CENTER_STOP_URL: &str = "https://api.mangadex.network/ping";

#[derive(Serialize)]
struct StopRequest<'a> {
    secret: &'a str,
}

pub async fn send_stop(secret: &str) {
    let request = StopRequest { secret };
    let client = reqwest::Client::new();
    match client
        .post(CONTROL_CENTER_STOP_URL)
        .json(&request)
        .send()
        .await
    {
        Ok(resp) => {
            if resp.status() == 200 {
                info!("Successfully sent stop message.");
            } else {
                warn!("Got weird response from server: {:?}", resp.headers());
            }
        }
        Err(e) => warn!("Got error while sending stop message: {}", e),
    }
}
