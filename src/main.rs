#![warn(clippy::pedantic, clippy::nursery)]
// We're end users, so these is ok
#![allow(clippy::module_name_repetitions)]

use std::env::{self, VarError};
use std::error::Error;
use std::fmt::Display;
use std::num::ParseIntError;
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use actix_web::rt::{spawn, time, System};
use actix_web::web::{self, Data};
use actix_web::{App, HttpResponse, HttpServer};
use cache::{Cache, DiskCache};
use config::Config;
use log::{debug, error, info, warn};
use parking_lot::RwLock;
use rustls::{NoClientAuth, ServerConfig};
use simple_logger::SimpleLogger;
use sodiumoxide::crypto::secretstream::gen_key;
use state::{RwLockServerState, ServerState};
use stop::send_stop;
use thiserror::Error;

use crate::cache::mem::{Lfu, Lru};
use crate::cache::{MemoryCache, ENCRYPTION_KEY};
use crate::config::{CacheType, UnstableOptions, OFFLINE_MODE};
use crate::state::DynamicServerCert;

mod cache;
mod config;
mod metrics;
mod ping;
mod routes;
mod state;
mod stop;
mod units;

const CLIENT_API_VERSION: usize = 31;

#[derive(Error, Debug)]
enum ServerError {
    #[error("There was a failure parsing config")]
    Config(#[from] VarError),
    #[error("Failed to parse an int")]
    ParseInt(#[from] ParseIntError),
}

#[actix_web::main]
async fn main() -> Result<(), Box<dyn Error>> {
    sodiumoxide::init().expect("Failed to initialize crypto");
    // It's ok to fail early here, it would imply we have a invalid config.
    dotenv::dotenv().ok();

    //
    // Config loading
    //

    let config = config::load_config()?;
    let memory_quota = config.memory_quota;
    let disk_quota = config.disk_quota;
    let cache_type = config.cache_type;
    let cache_path = config.cache_path.clone();
    let disable_tls = config
        .unstable_options
        .contains(&UnstableOptions::DisableTls);
    let bind_address = config.bind_address;

    //
    // Logging and warnings
    //

    SimpleLogger::new().with_level(config.log_level).init()?;

    if let Err(e) = print_preamble_and_warnings(&config) {
        error!("{}", e);
        return Err(e);
    }

    let client_secret = if let Ok(v) = env::var("CLIENT_SECRET") {
        v
    } else {
        error!("Client secret not found in ENV. Please set CLIENT_SECRET.");
        process::exit(1);
    };
    let client_secret_1 = client_secret.clone();

    if config.ephemeral_disk_encryption {
        info!("Running with at-rest encryption!");
        ENCRYPTION_KEY.set(gen_key()).unwrap();
    }

    metrics::init();

    // HTTP Server init

    let server = if OFFLINE_MODE.load(Ordering::Acquire) {
        ServerState::init_offline()
    } else {
        ServerState::init(&client_secret, &config).await?
    };
    let data_0 = Arc::new(RwLockServerState(RwLock::new(server)));
    let data_1 = Arc::clone(&data_0);

    //
    // At this point, the server is ready to start, and starts the necessary
    // threads.
    //

    // Set ctrl+c to send a stop message
    let running = Arc::new(AtomicBool::new(true));
    let running_1 = running.clone();
    let system = System::current();
    ctrlc::set_handler(move || {
        let system = &system;
        let client_secret = client_secret.clone();
        let running_2 = Arc::clone(&running_1);
        if !OFFLINE_MODE.load(Ordering::Acquire) {
            System::new().block_on(async move {
                if running_2.load(Ordering::SeqCst) {
                    send_stop(&client_secret).await;
                } else {
                    warn!("Got second Ctrl-C, forcefully exiting");
                    system.stop()
                }
            });
        }
        running_1.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    // Spawn ping task
    if !OFFLINE_MODE.load(Ordering::Acquire) {
        spawn(async move {
            let mut interval = time::interval(Duration::from_secs(90));
            let mut data = Arc::clone(&data_0);
            loop {
                interval.tick().await;
                debug!("Sending ping!");
                ping::update_server_state(&client_secret_1, &config, &mut data).await;
            }
        });
    }

    let memory_max_size = memory_quota.into();
    let cache = DiskCache::new(disk_quota.into(), cache_path.clone()).await;
    let cache: Arc<dyn Cache> = match cache_type {
        CacheType::OnDisk => cache,
        CacheType::Lru => MemoryCache::<Lfu, _>::new(cache, memory_max_size).await,
        CacheType::Lfu => MemoryCache::<Lru, _>::new(cache, memory_max_size).await,
    };

    let cache_0 = Arc::clone(&cache);

    // Start HTTPS server
    let server = HttpServer::new(move || {
        App::new()
            .service(routes::index)
            .service(routes::token_data)
            .service(routes::token_data_saver)
            .service(routes::metrics)
            .route(
                "/data/{tail:.*}",
                web::get().to(HttpResponse::UnavailableForLegalReasons),
            )
            .route(
                "/data-saver/{tail:.*}",
                web::get().to(HttpResponse::UnavailableForLegalReasons),
            )
            .route("{tail:.*}", web::get().to(routes::default))
            .app_data(Data::from(Arc::clone(&data_1)))
            .app_data(Data::from(Arc::clone(&cache_0)))
    })
    .shutdown_timeout(60);

    if disable_tls {
        server.bind(bind_address)?.run().await?;
    } else {
        // Rustls only supports TLS 1.2 and 1.3.
        let tls_config = {
            let mut tls_config = ServerConfig::new(NoClientAuth::new());
            tls_config.cert_resolver = Arc::new(DynamicServerCert);
            tls_config
        };

        server.bind_rustls(bind_address, tls_config)?.run().await?;
    }

    // Waiting for us to finish sending stop message
    while running.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(250));
    }

    Ok(())
}

#[derive(Debug)]
enum InvalidCombination {
    MissingUnstableOption(&'static str, UnstableOptions),
}

impl Display for InvalidCombination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InvalidCombination::MissingUnstableOption(opt, arg) => {
                write!(
                    f,
                    "The option '{}' requires the unstable option '-Z {}'",
                    opt, arg
                )
            }
        }
    }
}

impl Error for InvalidCombination {}

fn print_preamble_and_warnings(args: &Config) -> Result<(), Box<dyn Error>> {
    println!(concat!(
        env!("CARGO_PKG_NAME"),
        " ",
        env!("CARGO_PKG_VERSION"),
        " (",
        env!("VERGEN_GIT_SHA_SHORT"),
        ")",
        "  Copyright (C) 2021  ",
        env!("CARGO_PKG_AUTHORS"),
        "\n\n",
        env!("CARGO_PKG_NAME"),
        " is free software: you can redistribute it and/or modify\n\
        it under the terms of the GNU General Public License as published by\n\
        the Free Software Foundation, either version 3 of the License, or\n\
        (at your option) any later version.\n\n",
        env!("CARGO_PKG_NAME"),
        " is distributed in the hope that it will be useful,\n\
        but WITHOUT ANY WARRANTY; without even the implied warranty of\n\
        MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the\n\
        GNU General Public License for more details.\n\n\
        You should have received a copy of the GNU General Public License\n\
        along with ",
        env!("CARGO_PKG_NAME"),
        ". If not, see <https://www.gnu.org/licenses/>.\n"
    ));

    if !args.unstable_options.is_empty() {
        warn!("Unstable options are enabled. These options should not be used in production!");
    }

    if args
        .unstable_options
        .contains(&UnstableOptions::OfflineMode)
    {
        warn!("Running in offline mode. No communication to MangaDex will be made!");
    }

    if args.unstable_options.contains(&UnstableOptions::DisableTls) {
        warn!("Serving insecure traffic! You better be running this for development only.");
    }

    if args.override_upstream.is_some()
        && !args
            .unstable_options
            .contains(&UnstableOptions::OverrideUpstream)
    {
        Err(Box::new(InvalidCombination::MissingUnstableOption(
            "override-upstream",
            UnstableOptions::OverrideUpstream,
        )))
    } else {
        Ok(())
    }
}
