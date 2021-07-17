A Rust implementation of a MangaDex@Home client.

This client contains the following features:

 - Easy migration from the official client
 - Fully compliant with MangaDex@Home specifications
 - Multi-threaded, high performance, and low overhead client
 - HTTP/2 support for API users, HTTP/2 only for upstream connections
 - Secure and privacy oriented features:
   - Only supports TLS 1.2 or newer; HTTP is not enabled by default
   - Options for no logging and no metrics
   - Support for on-disk XChaCha20 encryption with ephemeral key generation
 - Supports an internal LFU, LRU, or a redis instance for in-memory caching

## Building

```sh
cargo build
cargo test
```

You may need to set a client secret, see Configuration for more information.

# Migration

Migration from the official client was made to be as painless as possible. There
are caveats though:
  - If you ever want to return to using the official client, you will need to
  clear your cache.
  - As this is an unofficial client implementation, the only support you can
  probably get is from me.

Otherwise, the steps to migration is easy:
  1. Place the binary in the same folder as your `images` folder and
  `settings.yaml`.
  2. Rename `images` to `cache`.

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

---

If using the geo IP logging feature, then this product includes GeoLite2 data
created by MaxMind, available from https://www.maxmind.com.