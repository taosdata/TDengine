# Rust Connector

![Crates.io](https://img.shields.io/crates/v/libtaos) ![Crates.io](https://img.shields.io/crates/d/libtaos)

> Note that the rust connector is under active development and the APIs will changes a lot between versions. But we promise to ensure backward compatibility after version 1.0 .

Thanks [@songtianyi](https://github.com/songtianyi) for [libtdengine](https://github.com/songtianyi/tdengine-rust-bindings) - a rust bindings project for [TDengine]. It's an new design for [TDengine] rust client based on C interface or the REST API. It'll will provide Rust-like APIs and all rust things (like async/stream/iterators and others).

## Dependencies

- [Rust](https://www.rust-lang.org/learn/get-started) of course.

if you use the default features, it'll depend on:

- [TDengine Client](https://www.taosdata.com/cn/getting-started/#%E9%80%9A%E8%BF%87%E5%AE%89%E8%A3%85%E5%8C%85%E5%AE%89%E8%A3%85)  library and headers.
- clang because bindgen will requires the clang AST library.

## Features

In-design features:

- [x] API for both C interface
- [x] REST API support by feature `rest`.
- [x] [r2d2] Pool support by feature `r2d2`
- [ ] Iterators for fields fetching
- [ ] Stream support
- [ ] Subscribe support

## Build and test

```sh
cargo build
cargo test
```

`test` will use default TDengine user and password on localhost (TDengine default).

Set variables if it's not default:

- `TEST_TAOS_IP`
- `TEST_TAOS_PORT`
- `TEST_TAOS_USER`
- `TEST_TAOS_PASS`
- `TEST_TAOS_DB`

## Usage

For default C-based client API, set in Cargo.toml

```toml
[dependencies]
libtaos = "v0.3.8"
```

For r2d2 support:

```toml
[dependencies]
libtaos = { version = "*", features = ["r2d2"] }
```

For REST client:

```toml
[dependencies]
libtaos = { version = "*", features = ["rest"] }
```

There's a [demo app](https://github.com/taosdata/libtaos-rs/blob/main/examples/demo.rs) in examples directory, looks like this:

```rust
// ...
#[tokio::main]
async fn main() -> Result<(), Error> {
    init();
    let taos = taos_connect()?;

    assert_eq!(
        taos.query("drop database if exists demo").await.is_ok(),
        true
    );
    assert_eq!(taos.query("create database demo").await.is_ok(), true);
    assert_eq!(taos.query("use demo").await.is_ok(), true);
    assert_eq!(
        taos.query("create table m1 (ts timestamp, speed int)")
            .await
            .is_ok(),
        true
    );

    for i in 0..10i32 {
        assert_eq!(
            taos.query(format!("insert into m1 values (now+{}s, {})", i, i).as_str())
                .await
                .is_ok(),
            true
        );
    }
    let rows = taos.query("select * from m1").await?;

    println!("{}", rows.column_meta.into_iter().map(|col| col.name).join(","));
    for row in rows.rows {
        println!("{}", row.into_iter().join(","));
    }
    Ok(())
}
```

You can check out the experimental [bailongma-rs](https://github.com/taosdata/bailongma-rs) - a TDengine adapters for prometheus written with Rust - as a more productive code example.

[libtaos-rs]: https://github.com/taosdata/libtaos-rs
[TDengine]: https://github.com/taosdata/TDengine
[bailongma-rs]: https://github.com/taosdata/bailongma-rs
[r2d2]: https://crates.io/crates/r2d2