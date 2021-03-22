A Rust implementation of a Mangadex @ Home client.

## Building

```sh
cargo build --release
```

## Cache implementation

This client implements a multi-tier in-memory and on-disk LRU cache constrained
by quotas. In essence, it acts as an unified LRU, where in-memory items are
evicted and pushed into the on-disk LRU and fetching a item from the on-disk LRU
promotes it to the in-memory LRU.

Note that the capacity of each LRU is dynamic, depending on the maximum byte
capacity that you permit each cache to be. A large item may evict multiple
smaller items to fit within this constraint, for example.

Note that these quotas are closer to a rough estimate, and is not guaranteed to
be strictly below these values, so it's recommended to under set your config
values to make sure you don't exceed the actual quota.

## Running

This version relies on loading configurations from `env`, or from a file called
`.env`. The config options are below:

```
# Your MD@H client secret
CLIENT_SECRET=
# The port to use
PORT=
# The maximum disk cache size, in bytes
DISK_CACHE_QUOTA_BYTES=
# The path where the on-disk cache should be stored
DISK_CACHE_PATH="./cache" # Optional, default is "./cache"
# The maximum memory cache size, in bytes
MEM_CACHE_QUOTA_BYTES=
# The maximum memory speed, in bytes per second.
MAX_NETWORK_SPEED=
```

After these values have been set, simply run the client.