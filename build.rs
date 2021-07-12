use std::error::Error;

use vergen::{vergen, Config, ShaKind};

fn main() -> Result<(), Box<dyn Error>> {
    // Initialize vergen stuff
    let mut config = Config::default();
    *config.git_mut().sha_kind_mut() = ShaKind::Short;
    vergen(config)?;

    Ok(())
}
