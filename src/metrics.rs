use once_cell::sync::Lazy;
use prometheus::{register_int_counter, IntCounter};

pub static CACHE_HIT_COUNTER: Lazy<IntCounter> =
    Lazy::new(|| register_int_counter!("cache.hit", "The number of cache hits").unwrap());

pub static CACHE_MISS_COUNTER: Lazy<IntCounter> =
    Lazy::new(|| register_int_counter!("cache.miss", "The number of cache misses").unwrap());

pub static REQUESTS_TOTAL_COUNTER: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!("requests.total", "The total number of requests served.").unwrap()
});

pub static REQUESTS_DATA_COUNTER: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "requests.data",
        "The number of requests served from the /data endpoint."
    )
    .unwrap()
});

pub static REQUESTS_DATA_SAVER_COUNTER: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "requests.data-saver",
        "The number of requests served from the /data-saver endpoint."
    )
    .unwrap()
});

pub static REQUESTS_OTHER_COUNTER: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "requests.other",
        "The total number of request not served by primary endpoints."
    )
    .unwrap()
});
