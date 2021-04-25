#![warn(clippy::pedantic, clippy::nursery)]
// We're end users, so these is ok
#![allow(clippy::module_name_repetitions)]

use std::num::ParseIntError;
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{
    env::{self, VarError},
    fmt::Display,
};
use std::{error::Error, hint::unreachable_unchecked};

use actix_web::rt::{spawn, time, System};
use actix_web::web::{self, Data};
use actix_web::{App, HttpServer};
use cache::{Cache, DiskCache};
use clap::Clap;
use config::CliArgs;
use log::{debug, error, warn, LevelFilter};
use parking_lot::RwLock;
use rustls::{NoClientAuth, ServerConfig};
use simple_logger::SimpleLogger;
use state::{RwLockServerState, ServerState};
use stop::send_stop;
use thiserror::Error;

use crate::state::DynamicServerCert;
use crate::{
    cache::{MemoryLfuCache, MemoryLruCache},
    config::UnstableOptions,
};

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
async fn main() -> Result<(), Box<dyn Error>> {
    // It's ok to fail early here, it would imply we have a invalid config.
    dotenv::dotenv().ok();
    let cli_args = CliArgs::parse();

    let port = cli_args.port;
    let memory_max_size = cli_args.memory_quota.get();
    let disk_quota = cli_args.disk_quota;
    let cache_path = cli_args.cache_path.clone();
    let low_mem_mode = cli_args.low_memory;
    let use_lfu = cli_args.unstable_options.contains(&UnstableOptions::UseLfu);

    let log_level = match (cli_args.quiet, cli_args.verbose) {
        (n, _) if n > 2 => LevelFilter::Off,
        (2, _) => LevelFilter::Error,
        (1, _) => LevelFilter::Warn,
        (0, 0) => LevelFilter::Info,
        (_, 1) => LevelFilter::Debug,
        (_, n) if n > 1 => LevelFilter::Trace,
        _ => unsafe { unreachable_unchecked() },
    };

    SimpleLogger::new().with_level(log_level).init()?;

    if let Err(e) = print_preamble_and_warnings(&cli_args) {
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

    let server = ServerState::init(&client_secret, &cli_args).await?;
    let data_0 = Arc::new(RwLockServerState(RwLock::new(server)));
    let data_1 = Arc::clone(&data_0);

    // What's nice is that Rustls only supports TLS 1.2 and 1.3.
    let mut tls_config = ServerConfig::new(NoClientAuth::new());
    tls_config.cert_resolver = Arc::new(DynamicServerCert);

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
        System::new().block_on(async move {
            if running_2.load(Ordering::SeqCst) {
                send_stop(&client_secret).await;
            } else {
                warn!("Got second ctrl-c, forcefully exiting");
                system.stop()
            }
        });
        running_1.store(false, Ordering::SeqCst);
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

    let cache: Arc<Box<dyn Cache>> = if low_mem_mode {
        DiskCache::new(disk_quota, cache_path.clone()).await
    } else {
        if use_lfu {
            MemoryLfuCache::new(disk_quota, cache_path.clone(), memory_max_size).await
        } else {
            MemoryLruCache::new(disk_quota, cache_path.clone(), memory_max_size).await
        }
    };

    let cache_0 = Arc::clone(&cache);

    // Start HTTPS server
    HttpServer::new(move || {
        App::new()
            .service(routes::token_data)
            .service(routes::token_data_saver)
            .route("{tail:.*}", web::get().to(routes::default))
            .app_data(Data::from(Arc::clone(&data_1)))
            .app_data(Data::from(Arc::clone(&cache_0)))
    })
    .shutdown_timeout(60)
    .bind_rustls(format!("0.0.0.0:{}", port), tls_config)?
    // .bind(format!("0.0.0.0:{}", port))?
    .run()
    .await?;

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

fn print_preamble_and_warnings(args: &CliArgs) -> Result<(), Box<dyn Error>> {
    println!(concat!(
        env!("CARGO_PKG_NAME"),
        " ",
        env!("CARGO_PKG_VERSION"),
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
