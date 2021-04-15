use std::num::{NonZeroU16, NonZeroU64};
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use clap::Clap;

// Validate tokens is an atomic because it's faster than locking on rwlock.
pub static VALIDATE_TOKENS: AtomicBool = AtomicBool::new(false);
// We use an atomic here because it's better for us to not pass the config
// everywhere.
pub static SEND_SERVER_VERSION: AtomicBool = AtomicBool::new(false);

#[derive(Clap, Clone)]
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
    #[clap(short, long, conflicts_with("memory_quota"))]
    pub low_memory: bool,
}
