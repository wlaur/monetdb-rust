Rust client for the [MonetDB](https://www.monetdb.org/) analytics database.

The crate follows semantic versioning. Its synchronous SQL/MAPI API includes
typed result conversion, TLS, binary result windows, and client-side binary
uploads.

Examples
--------

```rust,no_run
use std::error::Error;
use monetdb::Connection;

fn main() -> Result<(), Box<dyn Error>> {
    let url = "monetdb:///demo?user=monetdb&password=monetdb";
    let conn = Connection::connect_url(url)?;
    let mut cursor = conn.cursor();

    cursor.execute("SELECT hostname, clientpid, client, remark FROM sys.sessions")?;
    while cursor.next_row()? {
        // getters return Option< >, None means NULL
        let hostname: Option<&str> = cursor.get_str(0)?;
        let clientpid: Option<u32> = cursor.get_u32(1)?;
        let client: Option<&str> = cursor.get_str(2)?;
        let remark: Option<&str> = cursor.get_str(3)?; // usually NULL
        println!("host={hostname:?} clientpid={clientpid:?} client={client:?} remark={remark:?}",);
    }
    Ok(())
}

// Example output:
// host=Some("totoro") clientpid=Some(1895691) client=Some("libmapi 11.51.4") remark=None
// host=Some("totoro") clientpid=Some(1914127) client=Some("monetdb-rust 0.1.1") remark=None
```

You can also use a [`Parameters`] object to fine tune the connection parameters:

```rust,no_run
# use std::error::Error;
use monetdb::{Parameters, Connection};
# fn main() -> Result<(), Box<dyn Error>> {
let parms = Parameters::basic("demo", "monetdb", "monetdb")? // database / user / password
    .with_autocommit(false)?;
let conn = Connection::new(parms)?;
# Ok(())
# }
```

Current status
--------------

* MonetDB Dec2025 (11.55.0) and later are supported. Protocol compatibility
  with older releases is not maintained.

* Rust 2024 edition; current stable Rust is the supported toolchain.

* The full `monetdb://` connection URL syntax is supported, though not all features have been implemented.

* Most data types can be retrieved in string form using `get_str()`, with
  typed conversions for primitive, decimal, temporal, UUID, and BLOB values.

* The primitive types bool, i8/u8, i16/u16, i32/u32, i64/u64, i128/u128,
  isize/usize, f32/f64 have typed getters, for example `get_i8()`.

* A single call to `Cursor::execute()` can return multiple result sets.

* Optional TLS 1.3 (`monetdbs://`) support includes platform verification,
  custom certificate authorities, SHA-256 certificate pins, and mutual TLS.
  Certificate pins require at least 16 hexadecimal digits (64 bits).

* Binary result windows are available through [`Cursor::fetch_binary()`] and
  [`Cursor::fetch_binary_into()`].

* `COPY BINARY ... ON CLIENT` can upload eager or lazily produced in-memory
  files, with configurable message sizes.

* PREPARE result metadata and statement ids are exposed for clients that
  implement parameter binding.

Not implemented yet but planned:

* A high-level parameter-binding API. PREPARE metadata and statement ids are
  already exposed for clients that implement binding.

* Adaptive paging window sizes

* scanning /tmp for Unix Domain sockets

* Non-SQL, for example language=mal for MonetDB's tracing / profiling API

* Async, seems to be needed for [sqlx]

* Integration with database frameworks such as [sqlx] and [Diesel].
  There does not seem to be a JDBC equivalent for Rust.

[sqlx]: https://crates.io/crates/sqlx

[Diesel]: https://crates.io/crates/diesel


Optional features
-----------------

The `monetdb` crate defines the following optional features:

* **rustls** Enable TLS connections using
  [rustls](https://crates.io/crates/rustls/). URL parameters select system
  verification, a custom CA (`cert=`), a SHA-256 certificate pin
  (`certhash=`), and optional client certificates (`clientkey=` and
  `clientcert=`). To enable it, pass it on the command line like this:
  ```plain
  cargo run --features=rustls --example testconnect -- monetdbs://my.tls.host/demo
  ```
  or enable it in your application's Cargo.toml like this:
  ```plain
  [dependencies]
  monetdb = { version="0.2", features=["rustls"]}
  ```

* **uuid** Enable support for UUID's as defined by the [uuid crate](https://crates.io/crates/uuid).
  Enabled by default.

* **rust_decimal** Enable support for Decimal as defined by the [rust_decimal crate](https://crates.io/crates/rust_decimal).
  Disabled by default.


* **decimal-rs** Enable support for Decimal as defined by the [decimal-rs crate](https://crates.io/crates/decimal-rs).
  Disabled by default.
