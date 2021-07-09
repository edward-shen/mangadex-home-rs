use std::error::Error;
use std::process::Command;

use vergen::{vergen, Config, ShaKind};

fn main() -> Result<(), Box<dyn Error>> {
    // Initialize vergen stuff
    let mut config = Config::default();
    *config.git_mut().sha_kind_mut() = ShaKind::Short;
    vergen(config)?;

    // Initialize SQL stuff
    let project_root = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    Command::new("mkdir")
        .args(["cache", "--parents"])
        .current_dir(&project_root)
        .output()?;

    Command::new("sqlite3")
        .args(["cache/metadata.sqlite", include_str!("db_queries/init.sql")])
        .current_dir(&project_root)
        .output()?;

    Ok(())
}
