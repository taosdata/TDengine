---
toc_max_heading_level: 4
sidebar_position: 5
sidebar_label: Rust
title: TDengine Rust Connector
description: Detailed guide for Rust Connector
---


`libtaos` is the official Rust language connector for TDengine. Rust developers can develop applications to access the TDengine instance data.

The source code for `libtaos` is hosted on [GitHub](https://github.com/taosdata/libtaos-rs).

## Installation

### Pre-installation
  
Install the Rust development toolchain.

### Adding libtaos dependencies

```toml
[dependencies]
# use rest feature
libtaos = { version = "*", features = ["rest"]}
```

### Using connection pools

Please enable the `r2d2` feature in `Cargo.toml`.

```toml
[dependencies]
libtaos = { version = "*", features = ["rest", "r2d2"] }
```

## Create a connection

Create `TaosCfg` from TDengine cloud DSN. The DSN should be in form of `<http | https>://<host>[:port]?token=<token>`.

```rust
use libtaos::*;
let cfg = TaosCfg::from_dsn(DSN)?;
```

You can now use this object to create the connection.

```rust
let conn = cfg.connect()? ;
```

The connection object can create more than one.

```rust
let conn = cfg.connect()? ;
let conn2 = cfg.connect()? ;
```

You can use connection pools in applications.

```rust
let pool = r2d2::Pool::builder()
    .max_size(10000) // max connections
    .build(cfg)? ;

// ...
// Use pool to get connection
let conn = pool.get()? ;
```

After that, you can perform the following operations on the database.

```rust
async fn demo() -> Result<(), Error> {
    // get connection ...

    // create database
    conn.exec("create database if not exists demo").await?
    // create table
    conn.exec("create table if not exists demo.tb1 (ts timestamp, v int)").await?
    // insert
    conn.exec("insert into demo.tb1 values(now, 1)").await?
    // query
    let rows = conn.query("select * from demo.tb1").await?
    for row in rows.rows {
        println!("{}", row.into_iter().join(","));
    }
}
```

## API Reference

### Connection pooling

In complex applications, we recommend enabling connection pools. Connection pool for [libtaos] is implemented using [r2d2].

As follows, a connection pool with default parameters can be generated.

```rust
let pool = r2d2::Pool::new(cfg)? ;
```

You can set the same connection pool parameters using the connection pool's constructor.

```rust
    use std::time::Duration;
    let pool = r2d2::Pool::builder()
        .max_size(5000) // max connections
        .max_lifetime(Some(Duration::from_minutes(100))) // lifetime of each connection
        .min_idle(Some(1000)) // minimal idle connections
        .connection_timeout(Duration::from_minutes(2))
        .build(cfg);
```

In the application code, use `pool.get()? ` to get a connection object [Taos].

```rust
let taos = pool.get()? ;
```

The [Taos] structure is the connection manager in [libtaos] and provides two main APIs.

1. `exec`: Execute some non-query SQL statements, such as `CREATE`, `ALTER`, `INSERT`, etc.

    ```rust
    taos.exec().await?
    ```

2. `query`: Execute the query statement and return the [TaosQueryData] object.

    ```rust
    let q = taos.query("select * from log.logs").await?
    ```

    The [TaosQueryData] object stores the query result data and basic information about the returned columns (column name, type, length).

    Column information is stored using [ColumnMeta].

    ```rust
    let cols = &q.column_meta;
    for col in cols {
        println!("name: {}, type: {:?} , bytes: {}", col.name, col.type_, col.bytes);
    }
    ```

    It fetches data line by line.

    ```rust
    for (i, row) in q.rows.iter().enumerate() {
        for (j, cell) in row.iter().enumerate() {
            println!("cell({}, {}) data: {}", i, j, cell);
        }
    }
    ```

Note that Rust asynchronous functions and an asynchronous runtime are required.

[Taos] provides a few Rust methods that encapsulate SQL to reduce the frequency of `format!` code blocks.

- `.describe(table: &str)`: Executes `DESCRIBE` and returns a Rust data structure.
- `.create_database(database: &str)`: Executes the `CREATE DATABASE` statement.


Please move to the Rust documentation hosting page for other related structure API usage instructions: <https://docs.rs/libtaos>.

[libtaos]: https://github.com/taosdata/libtaos-rs
[tdengine]: https://github.com/taosdata/TDengine
[r2d2]: https://crates.io/crates/r2d2
[TaosCfg]: https://docs.rs/libtaos/latest/libtaos/struct.TaosCfg.html
[Taos]: https://docs.rs/libtaos/latest/libtaos/struct.Taos.html
[TaosQueryData]: https://docs.rs/libtaos/latest/libtaos/field/struct.TaosQueryData.html
[Field]: https://docs.rs/libtaos/latest/libtaos/field/enum.Field.html
