use std::num::{NonZeroU16, NonZeroU64};
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use clap::{crate_authors, crate_description, crate_version, Clap};
use url::Url;

// Validate tokens is an atomic because it's faster than locking on rwlock.
pub static VALIDATE_TOKENS: AtomicBool = AtomicBool::new(false);
// We use an atomic here because it's better for us to not pass the config
// everywhere.
pub static SEND_SERVER_VERSION: AtomicBool = AtomicBool::new(false);

#[derive(Clap, Clone)]
#[clap(version = crate_version!(), author = crate_authors!(), about = crate_description!())]
pub struct CliArgs {
    /// The port to listen on.
    #[clap(short, long, default_value = "42069", env = "PORT")]
    pub port: NonZeroU16,
    /// How large, in bytes, the in-memory cache should be. Note that this does
    /// not include runtime memory usage.
    #[clap(long, env = "MEM_CACHE_QUOTA_BYTES")]
    pub memory_quota: NonZeroU64,
    /// How large, in bytes, the on-disk cache should be. Note that actual
    /// values may be larger for metadata information.
    #[clap(long, env = "DISK_CACHE_QUOTA_BYTES")]
    pub disk_quota: u64,
    /// Sets the location of the disk cache.
    #[clap(long, default_value = "./cache", env = "DISK_CACHE_PATH")]
    pub cache_path: PathBuf,
    /// The network speed to advertise to Mangadex@Home control server.
    #[clap(long, env = "MAX_NETWORK_SPEED")]
    pub network_speed: NonZeroU64,
    /// Whether or not to provide the Server HTTP header to clients. This is
    /// useful for debugging, but is generally not recommended for security
    /// reasons.
    #[clap(long, env = "ENABLE_SERVER_STRING", takes_value = false)]
    pub enable_server_string: bool,
    /// Changes the caching behavior to avoid buffering images in memory, and
    /// instead use the filesystem as the buffer backing. This is useful for
    /// clients in low (< 1GB) RAM environments.
    #[clap(
        short,
        long,
        conflicts_with("memory-quota"),
        conflicts_with("use-lfu"),
        env = "LOW_MEMORY_MODE",
        takes_value = false
    )]
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
    /// Overrides the upstream URL to fetch images from. Don't use this unless
    /// you know what you're dealing with.
    #[clap(long)]
    pub override_upstream: Option<Url>,
    /// Disables token validation. Don't use this unless you know the
    /// ramifications of this command.
    #[clap(long)]
    pub disable_token_validation: bool,
    /// Use an LFU implementation for the in-memory cache instead of the default
    /// LRU implementation.
    #[clap(short = 'F', long)]
    pub use_lfu: bool,
}
