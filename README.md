A Rust implementation of a MangaDex@Home client.

This client contains the following features:

 - Multi-threaded
 - HTTP/2 support
 - No support for TLS 1.1 or 1.0

## Building

```sh
cargo build
cargo test
```

You may need to set a client secret, see Configuration for more information.

# Client implementation

This client follows a secure-first approach. As such, your statistics may report
a _ever-so-slightly_ higher-than-average failure rate. Specifically, this client
choses to:
 - Not support TLS 1.1 or 1.0, which would be a primary source of
 incompatibility.
 - Not provide a server identification string in the header of served requests.
 - HTTPS by enabled by default, HTTP is provided (and unsupported).

That being said, this client should be backwards compatibility with the official
client data and config. That means you should be able to replace the binary and
preserve all your settings and cache.

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

## Configuration

Most configuration options can be either provided on the command line or sourced
from a file named `settings.yaml` from the directory you ran the command from,
which will be created on first run.

Note that the client secret (`CLIENT_SECRET`) is the only configuration option
that can only can be provided from the environment, an `.env` file, or the
`settings.yaml` file. In other words, you _cannot_ provide this value from the
command line.

## Special thanks

This project could not have been completed without the assistance of the
following:

#### Development Assistance (Alphabetical Order)

- carbotaniuman#6974
- LFlair#1337
- Plykiya#1738
- Tristan 9#6752
- The Rust Discord community

#### Beta testers

- NigelVH#7162
