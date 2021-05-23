use once_cell::sync::Lazy;
use prometheus::{register_int_counter, IntCounter};

macro_rules! init_counters {
    ($(($counter:ident, $ty:ty, $name:literal, $desc:literal),)*) => {
        $(
            pub static $counter: Lazy<$ty> = Lazy::new(|| {
                register_int_counter!($name, $desc).unwrap()
            });
        )*

        #[allow(clippy::shadow_unrelated)]
        pub fn init() {
            $(let _a = $counter.get();)*
        }
    };
}

init_counters!(
    (
        CACHE_HIT_COUNTER,
        IntCounter,
        "cache_hit",
        "The number of cache hits."
    ),
    (
        CACHE_MISS_COUNTER,
        IntCounter,
        "cache_miss",
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
        "requests_data",
        "The number of requests served from the /data endpoint."
    ),
    (
        REQUESTS_DATA_SAVER_COUNTER,
        IntCounter,
        "requests_data_saver",
        "The number of requests served from the /data-saver endpoint."
    ),
    (
        REQUESTS_OTHER_COUNTER,
        IntCounter,
        "requests_other",
        "The total number of request not served by primary endpoints."
    ),
);
