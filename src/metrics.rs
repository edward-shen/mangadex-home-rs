use std::fs::metadata;
use std::hint::unreachable_unchecked;
use std::time::SystemTime;

use chrono::Duration;
use flate2::read::GzDecoder;
use maxminddb::geoip2::Country;
use once_cell::sync::{Lazy, OnceCell};
use prometheus::{register_int_counter, register_int_counter_vec, IntCounter, IntCounterVec};
use tar::Archive;
use thiserror::Error;
use tracing::{debug, field::debug, info, warn};

use crate::client::HTTP_CLIENT;
use crate::config::ClientSecret;

pub static GEOIP_DATABASE: OnceCell<maxminddb::Reader<Vec<u8>>> = OnceCell::new();

static COUNTRY_VISIT_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "country_visits_total",
        "The number of visits from a country",
        &["country"]
    )
    .unwrap()
});

macro_rules! init_counters {
    ($(($counter:ident, $ty:ty, $name:literal, $desc:literal),)*) => {
        $(
            pub static $counter: Lazy<$ty> = Lazy::new(|| {
                register_int_counter!($name, $desc).unwrap()
            });
        )*

        #[allow(clippy::shadow_unrelated)]
        pub fn init() {
            // These need to be called at least once, otherwise the macro never
            // called and thus the metrics don't get logged
            $(let _a = $counter.get();)*

            init_other();
        }
    };
}

init_counters!(
    (
        CACHE_HIT_COUNTER,
        IntCounter,
        "cache_hit_total",
        "The number of cache hits."
    ),
    (
        CACHE_MISS_COUNTER,
        IntCounter,
        "cache_miss_total",
        "The number of cache misses."
    ),
    (
        REQUESTS_TOTAL_COUNTER,
        IntCounter,
        "requests_total",
        "The total number of requests served."
    ),
    (
        REQUESTS_DATA_COUNTER,
        IntCounter,
        "requests_data_total",
        "The number of requests served from the /data endpoint."
    ),
    (
        REQUESTS_DATA_SAVER_COUNTER,
        IntCounter,
        "requests_data_saver_total",
        "The number of requests served from the /data-saver endpoint."
    ),
    (
        REQUESTS_OTHER_COUNTER,
        IntCounter,
        "requests_other_total",
        "The total number of request not served by primary endpoints."
    ),
);

// initialization for any other counters that aren't simple int counters
fn init_other() {
    let _a = COUNTRY_VISIT_COUNTER.local();
}

#[derive(Error, Debug)]
pub enum DbLoadError {
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    MaxMindDb(#[from] maxminddb::MaxMindDBError),
}

pub async fn load_geo_ip_data(license_key: ClientSecret) -> Result<(), DbLoadError> {
    const DB_PATH: &str = "./GeoLite2-Country.mmdb";

    // Check date of db
    let db_date_created = metadata(DB_PATH)
        .ok()
        .and_then(|metadata| {
            if let Ok(time) = metadata.created() {
                Some(time)
            } else {
                debug("fs didn't report birth time, fall back to last modified instead");
                metadata.modified().ok()
            }
        })
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let duration = if let Ok(time) = SystemTime::now().duration_since(db_date_created) {
        Duration::from_std(time).expect("duration to fit")
    } else {
        warn!("Clock may have gone backwards?");
        Duration::max_value()
    };

    // DB expired, fetch a new one
    if duration > Duration::weeks(1) {
        fetch_db(license_key).await?;
    } else {
        info!("Geo IP database isn't old enough, not updating.");
    }

    // Result literally cannot panic here, buuuuuut if it does we'll panic
    GEOIP_DATABASE
        .set(maxminddb::Reader::open_readfile(DB_PATH)?)
        .map_err(|_| ())
        .expect("to set the geo ip db singleton");

    Ok(())
}

async fn fetch_db(license_key: ClientSecret) -> Result<(), DbLoadError> {
    let resp = HTTP_CLIENT
        .inner()
        .get(format!("https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-Country&license_key={}&suffix=tar.gz", license_key.as_str()))
        .send()
        .await?
        .bytes()
        .await?;
    let mut decoder = Archive::new(GzDecoder::new(resp.as_ref()));
    let mut decoded_paths: Vec<_> = decoder
        .entries()?
        .filter_map(Result::ok)
        .filter_map(|mut entry| {
            let path = entry.path().ok()?.to_path_buf();
            let file_name = path.file_name()?;
            if file_name != "GeoLite2-Country.mmdb" {
                return None;
            }
            entry.unpack(file_name).ok()?;
            Some(path)
        })
        .collect();

    assert_eq!(decoded_paths.len(), 1);

    let path = match decoded_paths.pop() {
        Some(path) => path,
        None => unsafe { unreachable_unchecked() },
    };

    debug!("Extracted {}", path.as_path().to_string_lossy());

    Ok(())
}

pub fn record_country_visit(country: Option<Country>) {
    let iso_code = country.map_or("unknown", |country| {
        country
            .country
            .and_then(|c| c.iso_code)
            .unwrap_or("unknown")
    });

    COUNTRY_VISIT_COUNTER
        .get_metric_with_label_values(&[iso_code])
        .unwrap()
        .inc();
}
