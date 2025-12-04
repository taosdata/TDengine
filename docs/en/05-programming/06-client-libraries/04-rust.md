---
toc_max_heading_level: 4
sidebar_position: 5
sidebar_label: Rust
title: TDengine Rust Client Library
description: This document describes the TDengine Rust client library.
---
[![Crates.io](https://img.shields.io/crates/v/taos)](https://crates.io/crates/taos) ![Crates.io](https://img.shields.io/crates/d/taos) [![docs.rs](https://img.shields.io/docsrs/taos)](https://docs.rs/taos)

`taos` is the official Rust client library for TDengine. Rust developers can develop applications to access the TDengine instance data.

The source code for the Rust client library is located on [GitHub](https://github.com/taosdata/taos-connector-rust).

## Version support

Please refer to [version support list and history](https://docs.tdengine.com/reference/connectors/rust/#version-history)

## Connection types

`taos` provides Native Connection and WebSocket connection

More details about establishing a connection, please check[Developer Guide - Connected - Rust](../01-connect/04-rust.md)

## Installation

### Pre-installation

Install the Rust development toolchain.

### Adding taos dependencies

The taos connector uses the WebSocket to access TDengine Cloud instances. You need to add the `taos` dependency to your Rust project and enable the `ws` and `ws-rustls` features.

Add [taos][taos] to your Cargo.toml file and enable features. Both of the following methods are acceptable:

* Enable default features

    ```toml
    [dependencies]
    taos = { version = "*"}
    ```

* Disable default features and enable ws and ws-rustls features

    ```toml
    [dependencies]
    taos = { version = "*", default-features = false, features = ["ws", "ws-rustls"] }

## Expampe

:::note IMPORTANT

Before you run the code below,please create a database named `power` on the[TDengine Cloud - Explorer](https://cloud.taosdata.com/explorer) page.

:::

### Establish a connection

[TaosBuilder] creates a connection builder through a DSN. The DSN is composed of the following format: `wss://<host>?token=<token>`.

```rust
let builder = TaosBuilder::from_dsn(DSN)?;
```

You can now use this object to create a connection:

```rust
let conn = builder.build()?;
```

Multiple connection objects can be created:

```rust
let conn1 = builder.build()?;
let conn2 = builder.build()?;
```

### Insert data

`exec`: Execute some non-query SQL statements, such as `CREATE`, `ALTER`, `INSERT`, etc.

```rust title="exec"
{{#include docs/examples/rust/cloud-example/examples/tutorial.rs:insert}}
```

`exec_many`: Run multiple SQL statements simultaneously or in order.

```rust title="exec_many"
{{#include docs/examples/rust/cloud-example/examples/tutorial.rs:exec_many}}
```

### Query data

`query`: Run a query statement and return a [ResultSet] object.

```rust
{{#include docs/examples/rust/cloud-example/examples/query.rs:query:nrc}}
```

Get column meta from the result

You can obtain column information by using [.fields()].

```rust
{{#include docs/examples/rust/cloud-example/examples/query.rs:meta:nrc}}
```

Get first 5 rows and print each row:

```rust
{{#include docs/examples/rust/cloud-example/examples/query.rs:iter}}
```

Get first 5 rows and print each row and each column:

```rust
{{#include docs/examples/rust/cloud-example/examples/query.rs:iter_column}}
```

Use the [serde](https://serde.rs) deserialization framework.

```rust
{{#include docs/examples/rust/cloud-example/examples/serde.rs}}
```

### 连接池

In complex applications, we recommend enabling connection pools. [taos] implements connection pools based on [r2d2].

As follows, a connection pool with default parameters can be generated.

```rust
let pool = TaosBuilder::from_dsn(dsn)?.pool()?;
```

In the application code, use `pool.get()?` to get a connection object [Taos].

```rust
let taos = pool.get()?;
```

## API Reference

* [Api Reference](https://docs.tdengine.com/reference/connectors/rust/#api-reference)
* [taos](https://docs.rs/taos)

[taos]: https://github.com/taosdata/rust-connector-taos
[r2d2]: https://crates.io/crates/r2d2
[TaosBuilder]: https://docs.rs/taos/latest/taos/struct.TaosBuilder.html
[ResultSet]: https://docs.rs/taos/latest/taos/struct.ResultSet.html
