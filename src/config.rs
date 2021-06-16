use std::fmt::{Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::net::IpAddr;
use std::num::{NonZeroU16, NonZeroU64};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;

use clap::{crate_authors, crate_description, crate_version, Clap};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::units::{Kilobits, Mebibytes, Port};

// Validate tokens is an atomic because it's faster than locking on rwlock.
pub static VALIDATE_TOKENS: AtomicBool = AtomicBool::new(false);
// We use an atomic here because it's better for us to not pass the config
// everywhere.
pub static SEND_SERVER_VERSION: AtomicBool = AtomicBool::new(false);

pub static OFFLINE_MODE: AtomicBool = AtomicBool::new(false);

pub fn load_config() -> Config {
    const CONFIG_PATH: &str = "./settings.yaml";
    let config_file: Result<YamlArgs, _> = match File::open(CONFIG_PATH) {
        Ok(file) => serde_yaml::from_reader(file),
        Err(e) if e.kind() == ErrorKind::NotFound => {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(CONFIG_PATH)
                .unwrap();

            file.write_all(include_bytes!("../settings.sample.yaml"))
                .unwrap();

            return load_config();
        }
        _ => panic!(),
    };

    todo!()
}

pub struct Config {}

#[derive(Deserialize)]
struct YamlArgs {
    // Naming is legacy
    max_cache_size_in_mebibytes: Mebibytes,
    server_settings: YamlServerSettings,
    // This implementation custom options
    extended_options: YamlExtendedOptions,
}

// Naming is legacy
#[derive(Deserialize)]
struct YamlServerSettings {
    secret: ClientSecret,
    port: Port,
    external_max_kilobits_per_second: Kilobits,
    external_port: Option<Port>,
    graceful_shutdown_wait_seconds: Option<NonZeroU16>,
    hostname: Option<IpAddr>,
    external_ip: Option<IpAddr>,
}

// this intentionally does not implement display or debug
#[derive(Deserialize, Serialize)]
struct ClientSecret(String);

#[derive(Deserialize)]
struct YamlExtendedOptions {
    memory_quota: Option<NonZeroU64>,
    #[serde(default)]
    send_server_string: bool,
    #[serde(default)]
    cache_type: YamlCacheType,
    #[serde(default)]
    ephemeral_disk_encryption: bool,
    #[serde(default)]
    enable_metrics: bool,
}

#[derive(Deserialize)]
enum YamlCacheType {
    OnDisk,
    Lru,
    Lfu,
}

impl Default for YamlCacheType {
    fn default() -> Self {
        Self::OnDisk
    }
}

#[derive(Clap, Clone)]
#[clap(version = crate_version!(), author = crate_authors!(), about = crate_description!())]
pub struct CliArgs {
    /// The port to listen on.
    #[clap(short, long, default_value = "42069")]
    pub port: Port,
    /// How large, in bytes, the in-memory cache should be. Note that this does
    /// not include runtime memory usage.
    #[clap(long, conflicts_with = "low-memory")]
    pub memory_quota: Option<NonZeroU64>,
    /// How large, in bytes, the on-disk cache should be. Note that actual
    /// values may be larger for metadata information.
    #[clap(long)]
    pub disk_quota: u64,
    /// Sets the location of the disk cache.
    #[clap(long, default_value = "./cache")]
    pub cache_path: PathBuf,
    /// The network speed to advertise to Mangadex@Home control server.
    #[clap(long)]
    pub network_speed: NonZeroU64,
    /// Whether or not to provide the Server HTTP header to clients. This is
    /// useful for debugging, but is generally not recommended for security
    /// reasons.
    #[clap(long, takes_value = false)]
    pub send_server_string: bool,
    /// Changes the caching behavior to avoid buffering images in memory, and
    /// instead use the filesystem as the buffer backing. This is useful for
    /// clients in low (< 1GB) RAM environments.
    #[clap(short, long, conflicts_with("memory-quota"), takes_value = false)]
    pub low_memory: bool,
    /// Changes verbosity. Default verbosity is INFO, while increasing counts of
    /// verbose flags increases the verbosity to DEBUG and TRACE, respectively.
    #[clap(short, long, parse(from_occurrences))]
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
    pub ephemeral_disk_encryption: bool,
    #[clap(short, long)]
    pub config_path: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnstableOptions {
    /// Overrides the upstream URL to fetch images from. Don't use this unless
    /// you know what you're dealing with.
    OverrideUpstream,

    /// Use an LFU implementation for the in-memory cache instead of the default
    /// LRU implementation.
    UseLfu,

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
            "use-lfu" => Ok(Self::UseLfu),
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
            Self::UseLfu => write!(f, "use-lfu"),
            Self::DisableTokenValidation => write!(f, "disable-token-validation"),
            Self::OfflineMode => write!(f, "offline-mode"),
            Self::DisableTls => write!(f, "disable-tls"),
        }
    }
}
