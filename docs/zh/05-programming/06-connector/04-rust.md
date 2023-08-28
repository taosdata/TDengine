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

请参考[版本支持列表](../#版本支持)

Rust 连接器仍然在快速开发中，1.0 之前无法保证其向后兼容。建议使用 3.0 版本以上的 TDengine，以避免已知问题。

## 安装

### 安装前准备

安装 Rust 开发工具链

### 添加 taos 依赖

根据选择的连接方式，按照如下说明在 [Rust](https://rust-lang.org) 项目中添加 [taos][taos] 依赖：

在 `Cargo.toml` 文件中添加 [taos][taos]：

```toml
[dependencies]
# use default feature
taos = "*"
```

## 建立连接

[TaosBuilder] 通过 DSN 连接描述字符串创建一个连接构造器。DSN 有下面的格式组成`<http | https>://<host>[:port]?token=<token>`。

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

建立连接后，您可以进行相关数据库操作：

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

## API 参考

### 连接池

在复杂应用中，建议启用连接池。[taos] 的连接池使用 [r2d2] 实现。

如下，可以生成一个默认参数的连接池。

```rust
let pool = TaosBuilder::from_dsn(dsn)?.pool()?;
```

同样可以使用连接池的构造器，对连接池参数进行设置：

```rust
let dsn = "taos://localhost:6030";

let opts = PoolBuilder::new()
    .max_size(5000) // max connections
    .max_lifetime(Some(Duration::from_secs(60 * 60))) // lifetime of each connection
    .min_idle(Some(1000)) // minimal idle connections
    .connection_timeout(Duration::from_secs(2));

let pool = TaosBuilder::from_dsn(dsn)?.with_pool_builder(opts)?;
```

在应用代码中，使用 `pool.get()?` 来获取一个连接对象 [Taos]。

```rust
let taos = pool.get()?;
```

### 连接

[Taos][struct.Taos] 对象提供了多个数据库操作的 API：

1. `exec`: 执行某个非查询类 SQL 语句，例如 `CREATE`，`ALTER`，`INSERT` 等。

    ```rust
    let affected_rows = taos.exec("INSERT INTO tb1 VALUES(now, NULL)").await?;
    ```

2. `exec_many`: 同时（顺序）执行多个 SQL 语句。

    ```rust
    taos.exec_many([
        "CREATE DATABASE test",
        "USE test",
        "CREATE TABLE `tb1` (`ts` TIMESTAMP, `val` INT)",
    ]).await?;
    ```

3. `query`：执行查询语句，返回 [ResultSet] 对象。

    ```rust
    let mut q = taos.query("select * from log.logs").await?;
    ```

    [ResultSet] 对象存储了查询结果数据和返回的列的基本信息（列名，类型，长度）：

    列信息使用 [.fields()] 方法获取：

    ```rust
    let cols = q.fields();
    for col in cols {
        println!("name: {}, type: {:?} , bytes: {}", col.name(), col.ty(), col.bytes());
    }
    ```

    逐行获取数据：

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

    或使用 [serde](https://serde.rs) 序列化框架。

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

需要注意的是，需要使用 Rust 异步函数和异步运行时。

[Taos][struct.Taos] 提供部分 SQL 的 Rust 方法化以减少 `format!` 代码块的频率：

- `.describe(table: &str)`: 执行 `DESCRIBE` 并返回一个 Rust 数据结构。
- `.create_database(database: &str)`: 执行 `CREATE DATABASE` 语句。
- `.use_database(database: &str)`: 执行 `USE` 语句。

除此之外，该结构也是 [参数绑定](#参数绑定接口) 和 [行协议接口](#行协议接口) 的入口，使用方法请参考具体的 API 说明。


其他相关结构体 API 使用说明请移步 Rust 文档托管网页：<https://docs.rs/taos>。

[taos]: https://github.com/taosdata/rust-connector-taos
[r2d2]: https://crates.io/crates/r2d2
[TaosBuilder]: https://docs.rs/taos/latest/taos/struct.TaosBuilder.html
[TaosCfg]: https://docs.rs/taos/latest/taos/struct.TaosCfg.html
[struct.Taos]: https://docs.rs/taos/latest/taos/struct.Taos.html
[Stmt]: https://docs.rs/taos/latest/taos/struct.Stmt.html
