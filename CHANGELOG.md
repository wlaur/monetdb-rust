# Change Log

## monetdb NEXTVERSION - YYYY-MM-DD

New features:

- Add binary result-set metadata and window retrieval through `Xexportbin`,
  including reusable response buffers.

- Add eager and lazy in-memory `COPY BINARY ... ON CLIENT` uploads, buffered
  writes, and configurable upload message sizes.

- Recognize PREPARE metadata results and expose their server-side statement
  ids. Prepared statements can be queued for nonblocking deallocation.

- Add `Connection::metadata()`, `Connection::server_info()`, and runtime
  autocommit control.

- Preserve source table names separately from bare result-column names and
  recognize additional MonetDB types used by binary clients.

- Add TLS 1.3 support with system, custom-CA, SHA-256 certificate-pin, and
  mutual-TLS authentication modes.

- Add connect_timeout setting.

Bug fixes:

- Fix build issue on Windows, Unix domain sockets are not supported there.

- Fix Unix-socket selection, short reply-header panics, malformed result
  residency metadata, and unchecked server-controlled allocations.

- Keep synchronized connections usable after refused uploads, stale delayed
  cleanup commands, and rejected autocommit changes.

- Track transaction replies in connection state and honor the client binary
  level when reporting the negotiated server capability.

Other:

- Add integration tests, by default they try to connect to
  `monetdb:///test-monetdb-rust`.

- Move the crate to Rust 2024 and test current stable toolchains.

Breaking changes:

- `ResultColumn::name()` now returns the bare result-column name. Use
  `ResultColumn::table_name()` for its source table; earlier development
  versions combined the two in `name()`.


## monetdb 0.2.0 - 2024-10-04

First public release.

- This version can be used to connect to MonetDB and execute queries.
  The API is subject to change.

- There are typed getters for boolean and the various integer types.
  Other types, including decimals and temporal types, must be retrieved
  as strings and converted manually.

- Understands the full MonetDB URL syntax, though not all features have been
  implemented.

- There is a demo program and a number of unit tests but this release has
  not seen much testing.

- Has been tested mostly with MonetDB versions Aug2024 (11.51.3) and
  Jun2020 (11.37.13) but older versions are believed to work fine.

- Works with Rust 1.80.0, the exact minimum supported Rust version yet to be
  decided.

- Extremely basic and untested TLS support can optionally be compiled in
  be enabling the `rustls` Cargo feature.
