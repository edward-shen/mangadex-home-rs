A Rust implementation of a MangaDex@Home client.

This client contains the following features:

 - Multi-threaded
 - HTTP/2 support
 - No support for TLS 1.1 or 1.0

## Building

Since we use SQLx there are a few things you'll need to do. First, you'll need
to run the init cache script, which initializes the db cache at
`./cache/metadata.sqlite`. Then you'll need to add the location of that to a
`.env` file:

```sh
# In the project root
./init_cache.sh
echo "DATABASE_URL=sqlite:./cache/metadata.sqlite" >> .env
cargo build
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

## Installation

Either build it from source or run `cargo install mangadex-home`.

## Running

Run `mangadex-home`, and make sure the advertised port is open on your firewall.
Do note that some configuration fields are required. See the next section for
details.

## Configuration

Most configuration options can be either provided on the command line, sourced
from a `.env` file, or sourced directly from the environment. Do not that the
client secret is an exception. You must provide the client secret from the
environment or from the `.env` file, as providing client secrets in a shell is a
operation security risk.

The following options are required:

 - Client Secret
 - Memory cache quota
 - Disk cache quota
 - Advertised network speed

The following are optional as a default value will be set for you:

 - Port
 - Disk cache path

 ### Advanced configuration

 This implementation prefers to act more secure by default. As a result, some
 features that the official specification requires are not enabled by default.
 If you don't know why these features are disabled by default, then don't enable
 these, as they may generally weaken the security stance of the client for more
 compatibility.

 - Sending Server version string