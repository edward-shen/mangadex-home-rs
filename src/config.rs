use std::num::{NonZeroU16, NonZeroUsize};
use std::path::PathBuf;

use clap::Clap;

#[derive(Clap)]
pub struct CliArgs {
    /// The port to listen on.
    #[clap(short, long, default_value = "42069", env = "PORT")]
    pub port: NonZeroU16,
    /// How large, in bytes, the in-memory cache should be. Note that this does
    /// not include runtime memory usage.
    #[clap(long, env = "MEM_CACHE_QUOTA_BYTES")]
    pub memory_quota: NonZeroUsize,
    /// How large, in bytes, the on-disk cache should be. Note that actual
    /// values may be larger for metadata information.
    #[clap(long, env = "DISK_CACHE_QUOTA_BYTES")]
    pub disk_quota: usize,
    /// Sets the location of the disk cache.
    #[clap(long, default_value = "./cache", env = "DISK_CACHE_PATH")]
    pub cache_path: PathBuf,
    /// The network speed to advertise to Mangadex@Home control server.
    #[clap(long, env = "MAX_NETWORK_SPEED")]
    pub network_speed: NonZeroUsize,
    /// Whether or not to provide the Server HTTP header to clients. This is
    /// useful for debugging, but is generally not recommended for security
    /// reasons.
    #[clap(long, env = "ENABLE_SERVER_STRING")]
    pub enable_server_string: bool,
}
