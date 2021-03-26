#![warn(clippy::pedantic, clippy::nursery)]
// We're end users, so these is ok
#![allow(clippy::future_not_send, clippy::module_name_repetitions)]

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;
use std::{
    env::{self, VarError},
    process,
};
use std::{num::ParseIntError, sync::atomic::Ordering};

use actix_web::rt::{spawn, time, System};
use actix_web::web::{self, Data};
use actix_web::{App, HttpServer};
use clap::Clap;
use config::CliArgs;
use log::{debug, error, warn, LevelFilter};
use parking_lot::RwLock;
use rustls::{NoClientAuth, ServerConfig};
use simple_logger::SimpleLogger;
use state::{RwLockServerState, ServerState};
use stop::send_stop;
use thiserror::Error;

mod cache;
mod config;
mod ping;
mod routes;
mod state;
mod stop;

#[macro_export]
macro_rules! client_api_version {
    () => {
        "30"
    };
}
#[derive(Error, Debug)]
enum ServerError {
    #[error("There was a failure parsing config")]
    Config(#[from] VarError),
    #[error("Failed to parse an int")]
    ParseInt(#[from] ParseIntError),
}

#[actix_web::main]
async fn main() -> Result<(), std::io::Error> {
    // It's ok to fail early here, it would imply we have a invalid config.
    dotenv::dotenv().ok();
    let cli_args = CliArgs::parse();

    SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .init()
        .unwrap();
    let port = cli_args.port;

    let client_secret = if let Ok(v) = env::var("CLIENT_SECRET") {
        v
    } else {
        eprintln!("Client secret not found in ENV. Please set CLIENT_SECRET.");
        process::exit(1);
    };
    let client_secret_1 = client_secret.clone();

    let server = ServerState::init(&client_secret, &cli_args).await.unwrap();

    // Set ctrl+c to send a stop message
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        let client_secret = client_secret.clone();
        System::new().block_on(async move {
            send_stop(&client_secret).await;
        });
        r.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    let data_0 = Arc::new(RwLockServerState(RwLock::new(server)));
    let data_1 = Arc::clone(&data_0);
    let data_2 = Arc::clone(&data_0);

    spawn(async move {
        let mut interval = time::interval(Duration::from_secs(90));
        let mut data = Arc::clone(&data_0);
        loop {
            interval.tick().await;
            debug!("Sending ping!");
            ping::update_server_state(&client_secret_1, &cli_args, &mut data).await;
        }
    });

    let mut tls_config = ServerConfig::new(NoClientAuth::new());
    tls_config.cert_resolver = data_2;

    HttpServer::new(move || {
        App::new()
            .service(routes::token_data)
            .service(routes::token_data_saver)
            .route("{tail:.*}", web::get().to(routes::default))
            .app_data(Data::from(Arc::clone(&data_1)))
    })
    .shutdown_timeout(60)
    .bind_rustls(format!("0.0.0.0:{}", port), tls_config)?
    .run()
    .await?;

    // Waiting for us to finish sending stop message
    while running.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(250));
    }

    Ok(())
}
