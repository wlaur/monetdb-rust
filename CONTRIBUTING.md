# Contributing

Keep protocol changes generic to MonetDB MAPI and preserve the crate's Rust
1.85 minimum. Before opening a pull request, run:

```console
cargo fmt --all -- --check
cargo clippy --tests --all-features -- -D warnings
cargo test --lib --examples --all-features
cargo +1.85.0 test --lib --examples
./checklicense.py --check
```

Live tests use the internal `ci-tests` feature:

```console
CI_SERVER_URL='monetdb://host/database?user=user&password=password' \
  cargo test --test ci --all-features
```

Review protocol changes across these boundaries:

- default, sentinel, and explicit parameter values;
- inline, paged, exhausted, dropped, cancelled, and failed results;
- default-schema, explicit-schema, temporary, and quoted identifiers;
- success, server error, malformed response, timeout, and cleanup failure;
- minimum Rust, current stable, supported operating systems, and both endian
  compile paths;
- public traits from a downstream crate's perspective, including whether all
  types in their signatures are reachable.

Claims in the README or release notes need a required CI job or a regression
test. A server error may leave a connection reusable only after every expected
response has been consumed and the protocol stream is synchronized.
