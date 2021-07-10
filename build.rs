use std::path::PathBuf;
use std::process::Command;
use std::str::FromStr;
use std::{error::Error, io::Write};

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

    let env_var_regex = "^DATABASE_URL=sqlite:./cache/metadata.sqlite$";
    if !Command::new("grep")
        .args([env_var_regex, ".env"])
        .current_dir(&project_root)
        .output()?
        .status
        .success()
    {
        let mut path = PathBuf::from_str(&project_root)?;
        path.push(".env");

        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)?;
        file.write_all(b"\nDATABASE_URL=sqlite:./cache/metadata.sqlite\n")?;
        file.sync_all()?;
    }

    Ok(())
}
