# nzbdav-rs

Public source for the Rust NZB/WebDAV integration crates used by rustnzb's
optional `webdav` feature.

The workspace builds using public crates.io dependencies. The maintained
source location is `TheDancingDeveloper-org/nzbdav-rs`; no private registry
is required for a normal build.

```sh
cargo check --workspace --locked
cargo test --workspace --locked
```

Published `nzbdav-*` releases must retain this repository metadata so Cargo
users can audit the source used by rustnzb.
