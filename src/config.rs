use std::fmt::{Display, Formatter};
use std::fs::{File, OpenOptions};
use std::hint::unreachable_unchecked;
use std::io::{ErrorKind, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroU16;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};

use clap::{crate_authors, crate_description, crate_version, Clap};
use log::LevelFilter;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::units::{KilobitsPerSecond, Mebibytes, Port};

// Validate tokens is an atomic because it's faster than locking on rwlock.
pub static VALIDATE_TOKENS: AtomicBool = AtomicBool::new(false);
pub static OFFLINE_MODE: AtomicBool = AtomicBool::new(false);

pub fn load_config() -> Result<Config, serde_yaml::Error> {
    // Load cli args first
    let cli_args: CliArgs = CliArgs::parse();

    // Load yaml file next
    let config_file: Result<YamlArgs, _> = {
        let config_path = cli_args
            .config_path
            .as_deref()
            .unwrap_or_else(|| Path::new("./settings.yaml"));
        match File::open(config_path) {
            Ok(file) => serde_yaml::from_reader(file),
            Err(e) if e.kind() == ErrorKind::NotFound => {
                let mut file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(config_path)
                    .unwrap();

                let default_config = include_str!("../settings.sample.yaml");
                file.write_all(default_config.as_bytes()).unwrap();
                serde_yaml::from_str(default_config)
            }
            e => panic!(
                "Failed to open file at {}: {:?}",
                config_path.to_string_lossy(),
                e
            ),
        }
    };

    // generate config
    let config = Config::from_cli_and_file(cli_args, config_file?);

    // initialize globals
    OFFLINE_MODE.store(
        config
            .unstable_options
            .contains(&UnstableOptions::OfflineMode),
        Ordering::Release,
    );

    Ok(config)
}

/// Represents a fully parsed config, from a variety of sources.
pub struct Config {
    pub cache_type: CacheType,
    pub cache_path: PathBuf,
    pub shutdown_timeout: NonZeroU16,
    pub log_level: LevelFilter,
    pub client_secret: ClientSecret,
    pub port: Port,
    pub bind_address: SocketAddr,
    pub external_address: Option<SocketAddr>,
    pub ephemeral_disk_encryption: bool,
    pub network_speed: KilobitsPerSecond,
    pub disk_quota: Mebibytes,
    pub memory_quota: Mebibytes,
    pub unstable_options: Vec<UnstableOptions>,
    pub override_upstream: Option<Url>,
    pub enable_metrics: bool,
}

impl Config {
    fn from_cli_and_file(cli_args: CliArgs, file_args: YamlArgs) -> Self {
        let file_extended_options = file_args.extended_options.unwrap_or_default();

        let log_level = match (cli_args.quiet, cli_args.verbose) {
            (n, _) if n > 2 => LevelFilter::Off,
            (2, _) => LevelFilter::Error,
            (1, _) => LevelFilter::Warn,
            // Use log level from file if no flags were provided to CLI
            (0, 0) => file_extended_options
                .logging_level
                .unwrap_or(LevelFilter::Info),
            (_, 1) => LevelFilter::Debug,
            (_, n) if n > 1 => LevelFilter::Trace,
            // compiler can't figure it out
            _ => unsafe { unreachable_unchecked() },
        };

        let bind_port = cli_args
            .port
            .unwrap_or(file_args.server_settings.port)
            .get();

        // This needs to be outside because rust isn't smart enough yet to
        // realize a disjointed borrow of a moved value is ok. This will be
        // fixed in Rust 2021.
        let external_port = file_args
            .server_settings
            .external_port
            .map_or(bind_port, Port::get);

        Self {
            cache_type: cli_args
                .cache_type
                .or(file_extended_options.cache_type)
                .unwrap_or_default(),
            cache_path: cli_args
                .cache_path
                .or(file_extended_options.cache_path)
                .unwrap_or_else(|| PathBuf::from_str("./cache").unwrap()),
            shutdown_timeout: file_args
                .server_settings
                .graceful_shutdown_wait_seconds
                .unwrap_or(unsafe { NonZeroU16::new_unchecked(60) }),
            log_level,
            // secret should never be in CLI
            client_secret: file_args.server_settings.secret,
            port: cli_args.port.unwrap_or(file_args.server_settings.port),
            bind_address: SocketAddr::new(
                file_args
                    .server_settings
                    .hostname
                    .unwrap_or_else(|| IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0))),
                bind_port,
            ),
            external_address: file_args
                .server_settings
                .external_ip
                .map(|ip_addr| SocketAddr::new(ip_addr, external_port)),
            ephemeral_disk_encryption: cli_args
                .ephemeral_disk_encryption
                .or(file_extended_options.ephemeral_disk_encryption)
                .unwrap_or_default(),
            network_speed: cli_args
                .network_speed
                .unwrap_or(file_args.server_settings.external_max_kilobits_per_second),
            disk_quota: cli_args
                .disk_quota
                .unwrap_or(file_args.max_cache_size_in_mebibytes),
            memory_quota: cli_args
                .memory_quota
                .or(file_extended_options.memory_quota)
                .unwrap_or_default(),
            enable_metrics: file_extended_options.enable_metrics.unwrap_or_default(),

            // Unstable options (and related) should never be in yaml config
            unstable_options: cli_args.unstable_options,
            override_upstream: cli_args.override_upstream,
        }
    }
}

#[derive(Deserialize)]
struct YamlArgs {
    // Naming is legacy
    max_cache_size_in_mebibytes: Mebibytes,
    server_settings: YamlServerSettings,
    // This implementation custom options
    extended_options: Option<YamlExtendedOptions>,
}

// Naming is legacy
#[derive(Deserialize)]
struct YamlServerSettings {
    secret: ClientSecret,
    #[serde(default)]
    port: Port,
    external_max_kilobits_per_second: KilobitsPerSecond,
    external_port: Option<Port>,
    graceful_shutdown_wait_seconds: Option<NonZeroU16>,
    hostname: Option<IpAddr>,
    external_ip: Option<IpAddr>,
}

// this intentionally does not implement display or debug
#[derive(Deserialize, Serialize)]
pub struct ClientSecret(String);

#[derive(Deserialize, Default)]
struct YamlExtendedOptions {
    memory_quota: Option<Mebibytes>,
    cache_type: Option<CacheType>,
    ephemeral_disk_encryption: Option<bool>,
    enable_metrics: Option<bool>,
    logging_level: Option<LevelFilter>,
    cache_path: Option<PathBuf>,
}

#[derive(Deserialize, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum CacheType {
    OnDisk,
    Lru,
    Lfu,
}

impl FromStr for CacheType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "on_disk" => Ok(Self::OnDisk),
            "lru" => Ok(Self::Lru),
            "lfu" => Ok(Self::Lfu),
            _ => Err(format!("Unknown option: {}", s)),
        }
    }
}

impl Default for CacheType {
    fn default() -> Self {
        Self::OnDisk
    }
}

#[derive(Clap, Clone)]
#[clap(version = crate_version!(), author = crate_authors!(), about = crate_description!())]
struct CliArgs {
    /// The port to listen on.
    #[clap(short, long)]
    pub port: Option<Port>,
    /// How large, in mebibytes, the in-memory cache should be. Note that this
    /// does not include runtime memory usage.
    #[clap(long)]
    pub memory_quota: Option<Mebibytes>,
    /// How large, in mebibytes, the on-disk cache should be. Note that actual
    /// values may be larger for metadata information.
    #[clap(long)]
    pub disk_quota: Option<Mebibytes>,
    /// Sets the location of the disk cache.
    #[clap(long)]
    pub cache_path: Option<PathBuf>,
    /// The network speed to advertise to Mangadex@Home control server.
    #[clap(long)]
    pub network_speed: Option<KilobitsPerSecond>,
    /// Changes verbosity. Default verbosity is INFO, while increasing counts of
    /// verbose flags increases the verbosity to DEBUG and TRACE, respectively.
    #[clap(short, long, parse(from_occurrences), conflicts_with = "quiet")]
    pub verbose: usize,
    /// Changes verbosity. Default verbosity is INFO, while increasing counts of
    /// quiet flags decreases the verbosity to WARN, ERROR, and no logs,
    /// respectively.
    #[clap(short, long, parse(from_occurrences), conflicts_with = "verbose")]
    pub quiet: usize,
    #[clap(short = 'Z', long)]
    pub unstable_options: Vec<UnstableOptions>,
    #[clap(long)]
    pub override_upstream: Option<Url>,
    /// Enables ephemeral disk encryption. Items written to disk are first
    /// encrypted with a key generated at runtime. There are implications to
    /// performance, privacy, and usability with this flag enabled.
    #[clap(short, long)]
    pub ephemeral_disk_encryption: Option<bool>,
    #[clap(short, long)]
    pub config_path: Option<PathBuf>,
    #[clap(short, long)]
    pub cache_type: Option<CacheType>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnstableOptions {
    /// Overrides the upstream URL to fetch images from. Don't use this unless
    /// you know what you're dealing with.
    OverrideUpstream,

    /// Disables token validation. Don't use this unless you know the
    /// ramifications of this command.
    DisableTokenValidation,

    /// Tries to run without communication to MangaDex.
    OfflineMode,

    /// Serves HTTP in plaintext
    DisableTls,
}

impl FromStr for UnstableOptions {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "override-upstream" => Ok(Self::OverrideUpstream),
            "disable-token-validation" => Ok(Self::DisableTokenValidation),
            "offline-mode" => Ok(Self::OfflineMode),
            "disable-tls" => Ok(Self::DisableTls),
            _ => Err(format!("Unknown unstable option '{}'", s)),
        }
    }
}

impl Display for UnstableOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OverrideUpstream => write!(f, "override-upstream"),
            Self::DisableTokenValidation => write!(f, "disable-token-validation"),
            Self::OfflineMode => write!(f, "offline-mode"),
            Self::DisableTls => write!(f, "disable-tls"),
        }
    }
}
