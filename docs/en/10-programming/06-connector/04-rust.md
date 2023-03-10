---
toc_max_heading_level: 4
sidebar_position: 5
sidebar_label: Rust
title: TDengine Rust Connector
description: This document describes the TDengine Rust connector.
---
[![Crates.io](https://img.shields.io/crates/v/taos)](https://crates.io/crates/taos) ![Crates.io](https://img.shields.io/crates/d/taos) [![docs.rs](https://img.shields.io/docsrs/taos)](https://docs.rs/taos)

`taos` is the official Rust connector for TDengine. Rust developers can develop applications to access the TDengine instance data.

The source code for the Rust connectors is located on [GitHub](https://github.com/taosdata/taos-connector-rust).

## Version support

Please refer to [version support list](/reference/connector#version-support)

The Rust Connector is still under rapid development and is not guaranteed to be backward compatible before 1.0. We recommend using TDengine version 3.0 or higher to avoid known issues.

## Installation

### Pre-installation
  
Install the Rust development toolchain.

### Adding taos dependencies

Depending on the connection method, add the [taos][taos] dependency in your Rust project as follows:

In `cargo.toml`, add [taos][taos]:

```toml
[dependencies]
# use default feature
taos = "*"
```

## Establishing a connection

[TaosBuilder] creates a connection constructor through the DSN connection description string.
The DSN should be in form of `<http | https>://<host>[:port]?token=<token>`.

```rust
let builder = TaosBuilder::from_dsn(DSN)?;
```

You can now use this object to create the connection.

```rust
let conn = builder.build()?;
```

The connection object can create more than one.

```rust
let conn1 = builder.build()?;
let conn2 = builder.build()?;
```

After that, you can perform the following operations on the database.

```rust
async fn demo(taos: &Taos, db: &str) -> Result<(), Error> {
    // prepare database
    taos.exec_many([
        format!("DROP DATABASE IF EXISTS `{db}`"),
        format!("CREATE DATABASE `{db}`"),
        format!("USE `{db}`"),
    ])
    .await?;

    let inserted = taos.exec_many([
        // create super table
        "CREATE TABLE `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) \
         TAGS (`groupid` INT, `location` BINARY(24))",
        // create child table
        "CREATE TABLE `d0` USING `meters` TAGS(0, 'California.LosAngles')",
        // insert into child table
        "INSERT INTO `d0` values(now - 10s, 10, 116, 0.32)",
        // insert with NULL values
        "INSERT INTO `d0` values(now - 8s, NULL, NULL, NULL)",
        // insert and automatically create table with tags if not exists
        "INSERT INTO `d1` USING `meters` TAGS(1, 'California.SanFrancisco') values(now - 9s, 10.1, 119, 0.33)",
        // insert many records in a single sql
        "INSERT INTO `d1` values (now-8s, 10, 120, 0.33) (now - 6s, 10, 119, 0.34) (now - 4s, 11.2, 118, 0.322)",
    ]).await?;

    assert_eq!(inserted, 6);
    let mut result = taos.query("select * from `meters`").await?;

    for field in result.fields() {
        println!("got field: {}", field.name());
    }

    let values = result.
}
```

## API Reference

### Connection pooling

In complex applications, we recommend enabling connection pools. [taos] implements connection pools based on [r2d2].

As follows, a connection pool with default parameters can be generated.

```rust
let pool = TaosBuilder::from_dsn(dsn)?.pool()?;
```

You can set the same connection pool parameters using the connection pool's constructor.

```rust
let dsn = std::env::var("TDENGINE_CLOUD_DSN")?;;

let opts = PoolBuilder::new()
    .max_size(5000) // max connections
    .max_lifetime(Some(Duration::from_secs(60 * 60))) // lifetime of each connection
    .min_idle(Some(1000)) // minimal idle connections
    .connection_timeout(Duration::from_secs(2));

let pool = TaosBuilder::from_dsn(dsn)?.with_pool_builder(opts)?;
```

In the application code, use `pool.get()? ` to get a connection object [Taos].

```rust
let taos = pool.get()? ;
```
# Connectors

The [Taos][struct.Taos] object provides an API to perform operations on multiple databases.

1. `exec`: Execute some non-query SQL statements, such as `CREATE`, `ALTER`, `INSERT`, etc.

    ```rust
    let affected_rows = taos.exec("INSERT INTO tb1 VALUES(now, NULL)").await?;
    ```

2. `exec_many`: Run multiple SQL statements simultaneously or in order.

    ```rust
    taos.exec_many([
        "CREATE DATABASE test",
        "USE test",
        "CREATE TABLE `tb1` (`ts` TIMESTAMP, `val` INT)",
    ]).await?;
    ```

3. `query`: Run a query statement and return a [ResultSet] object.

    ```rust
    let mut q = taos.query("select * from log.logs").await?;
    ```

    The [ResultSet] object stores query result data and the names, types, and lengths of returned columns

    You can obtain column information by using [.fields()].

    ```rust
    let cols = q.fields();
    for col in cols {
        println!("name: {}, type: {:?} , bytes: {}", col.name(), col.ty(), col.bytes());
    }
    ```

    It fetches data line by line.

    ```rust
    let mut rows = result.rows();
    let mut nrows = 0;
    while let Some(row) = rows.try_next().await? {
        for (col, (name, value)) in row.enumerate() {
            println!(
                "[{}] got value in col {} (named `{:>8}`): {}",
                nrows, col, name, value
            );
        }
        nrows += 1;
    }
    ```

    Or use the [serde](https://serde.rs) deserialization framework.

    ```rust
    #[derive(Debug, Deserialize)]
    struct Record {
        // deserialize timestamp to chrono::DateTime<Local>
        ts: DateTime<Local>,
        // float to f32
        current: Option<f32>,
        // int to i32
        voltage: Option<i32>,
        phase: Option<f32>,
        groupid: i32,
        // binary/varchar to String
        location: String,
    }

    let records: Vec<Record> = taos
        .query("select * from `meters`")
        .await?
        .deserialize()
        .try_collect()
        .await?;
    ```

Note that Rust asynchronous functions and an asynchronous runtime are required.

[Taos][struct.Taos] provides Rust methods for some SQL statements to reduce the number of `format!`s.

- `.describe(table: &str)`: Executes `DESCRIBE` and returns a Rust data structure.
- `.create_database(database: &str)`: Executes the `CREATE DATABASE` statement.
- `.use_database(database: &str)`: Executes the `USE` statement.

In addition, this structure is also the entry point for [Parameter Binding](#Parameter Binding Interface) and [Line Protocol Interface](#Line Protocol Interface). Please refer to the specific API descriptions for usage.

For information about other structure APIs, see the [Rust documentation](https://docs.rs/taos).

[taos]: https://github.com/taosdata/rust-connector-taos
[r2d2]: https://crates.io/crates/r2d2
[TaosBuilder]: https://docs.rs/taos/latest/taos/struct.TaosBuilder.html
[TaosCfg]: https://docs.rs/taos/latest/taos/struct.TaosCfg.html
[struct.Taos]: https://docs.rs/taos/latest/taos/struct.Taos.html
[Stmt]: https://docs.rs/taos/latest/taos/struct.Stmt.html
