#![warn(clippy::pedantic, clippy::nursery)]
// We're end users, so these is ok
#![allow(clippy::future_not_send, clippy::module_name_repetitions)]

use std::env::{self, VarError};
use std::process;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;
use std::{num::ParseIntError, sync::atomic::Ordering};

use actix_web::rt::{spawn, time, System};
use actix_web::web::{self, Data};
use actix_web::{App, HttpServer};
use cache::Cache;
use clap::Clap;
use config::CliArgs;
use log::{debug, error, warn, LevelFilter};
use parking_lot::{Mutex, RwLock};
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
    let port = cli_args.port;
    let memory_max_size = cli_args.memory_quota.get();
    let disk_quota = cli_args.disk_quota;
    let cache_path = cli_args.cache_path.clone();

    SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .init()
        .unwrap();

    let client_secret = if let Ok(v) = env::var("CLIENT_SECRET") {
        v
    } else {
        error!("Client secret not found in ENV. Please set CLIENT_SECRET.");
        process::exit(1);
    };
    let client_secret_1 = client_secret.clone();

    let server = ServerState::init(&client_secret, &cli_args).await.unwrap();
    let data_0 = Arc::new(RwLockServerState(RwLock::new(server)));
    let data_1 = Arc::clone(&data_0);

    // What's nice is that Rustls only supports TLS 1.2 and 1.3.
    let mut tls_config = ServerConfig::new(NoClientAuth::new());
    tls_config.cert_resolver = data_0.clone();

    //
    // At this point, the server is ready to start, and starts the necessary
    // threads.
    //

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

    // Spawn ping task
    spawn(async move {
        let mut interval = time::interval(Duration::from_secs(90));
        let mut data = Arc::clone(&data_0);
        loop {
            interval.tick().await;
            debug!("Sending ping!");
            ping::update_server_state(&client_secret_1, &cli_args, &mut data).await;
        }
    });

    // Start HTTPS server
    HttpServer::new(move || {
        App::new()
            .service(routes::token_data)
            .service(routes::token_data_saver)
            .route("{tail:.*}", web::get().to(routes::default))
            .app_data(Data::from(Arc::clone(&data_1)))
            .app_data(Data::new(Mutex::new(Cache::new(
                memory_max_size,
                disk_quota,
                cache_path.clone(),
            ))))
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
