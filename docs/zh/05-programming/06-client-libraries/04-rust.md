---
toc_max_heading_level: 4
sidebar_position: 5
sidebar_label: Rust
title: TDengine Rust Connector
---

[![Crates.io](https://img.shields.io/crates/v/taos)](https://crates.io/crates/taos) ![Crates.io](https://img.shields.io/crates/d/taos) [![docs.rs](https://img.shields.io/docsrs/taos)](https://docs.rs/taos)

`taos` 是 TDengine 的官方 Rust 语言连接器。Rust 开发人员可以通过它开发存取 TDengine 数据库的应用软件。

该 Rust 连接器的源码托管在 [GitHub](https://github.com/taosdata/taos-connector-rust)。

## 版本支持

请参考[版本历史及支持列表](https://docs.taosdata.com/reference/connector/rust/#版本历史)

## 连接方式

`taos` 提供**原生连接**和**WebSocket 连接**两种方式，我们使用 **WebSocket 连接**方式访问 TDengine Cloud 实例。
它通过 taosAdapter 的 WebSocket 接口连接实例

关于如何建立连接的详细介绍请参考：[开发指南 - 建立连接-Rust](../01-connect/04-rust.md)

## 安装

### 安装前准备

安装 Rust 开发工具链

### 添加 taos 依赖

`taos` 连接器使用 WebSocket 方式连接 TDengine Cloud 实例。需要在 [Rust](https://rust-lang.org) 项目中添加 [taos][taos] 依赖，并启用`ws`和`ws-rustls`特性。

在 `Cargo.toml` 文件中添加 [taos][taos] 并启用特性，以下两种方式都可以：

* 启用默认特性

    ```toml
    [dependencies]
    taos = { version = "*"}
    ```

* 禁用默认特性，并启用 ws 和 ws-rustls 特性

    ```toml
    [dependencies]
    taos = { version = "*", default-features = false, features = ["ws", "ws-rustls"] }
    ```

## 使用示例

:::note IMPORTANT
在执行下面样例代码的之前，您必须先在 [TDengine Cloud - 数据浏览器](https://cloud.taosdata.com/explorer) 页面创建一个名为 power 的数据库
:::

### 建立连接

[TaosBuilder] 通过 DSN 连接描述字符串创建一个连接构造器。DSN 由下面的格式组成`wss://<host>?token=<token>`。

```rust
let builder = TaosBuilder::from_dsn(DSN)?;
```

现在您可以使用该对象创建连接：

```rust
let conn = builder.build()?;
```

连接对象可以创建多个：

```rust
let conn1 = builder.build()?;
let conn2 = builder.build()?;
```

### 插入数据

`exec` 方法执行某个非查询类 SQL 语句，例如 `CREATE`，`ALTER`，`INSERT` 等。

```rust title="exec"
{{#include docs/examples/rust/cloud-example/examples/tutorial.rs:insert}}
```

`exec_many` 方法同时（顺序）执行多个 SQL 语句。

```rust title="exec_many"
{{#include docs/examples/rust/cloud-example/examples/tutorial.rs:exec_many}}
```

### 查询数据

`query` 方法执行查询语句，返回 [ResultSet] 对象。

在这个例子里面，我们使用查询方法来执行 SQL，然后获取到 [ResultSet] 对象。

```rust
{{#include docs/examples/rust/cloud-example/examples/query.rs:query:nrc}}
```

获取列的元数据。

[ResultSet] 对象存储了查询结果数据和返回的列的基本信息（列名，类型，长度，列信息使用 [.fields()] 方法获取：

```rust
{{#include docs/examples/rust/cloud-example/examples/query.rs:meta:nrc}}
```

获取前 5 行数据并输出每一行数据：

```rust
{{#include docs/examples/rust/cloud-example/examples/query.rs:iter}}
```

逐行逐列获取数据

```rust
{{#include docs/examples/rust/cloud-example/examples/query.rs:iter_column}}
```

使用 [serde](https://serde.rs) 序列化框架

```rust
{{#include docs/examples/rust/cloud-example/examples/serde.rs}}
```

### 连接池

在复杂应用中，建议启用连接池。[taos] 的连接池使用 [r2d2] 实现。

如下，可以生成一个默认参数的连接池。

```rust
let pool = TaosBuilder::from_dsn(dsn)?.pool()?;
```

在应用代码中，使用 `pool.get()?` 来获取一个连接对象 `taos`。

```rust
let taos = pool.get()?;
```

## API 参考

* [连接器-Rust-Api 参考](https://docs.taosdata.com/reference/connector/rust/#api-参考)
* [taos](https://docs.rs/taos)

[taos]: https://github.com/taosdata/rust-connector-taos
[r2d2]: https://crates.io/crates/r2d2
[TaosBuilder]: https://docs.rs/taos/latest/taos/struct.TaosBuilder.html
[ResultSet]: https://docs.rs/taos/latest/taos/struct.ResultSet.html
