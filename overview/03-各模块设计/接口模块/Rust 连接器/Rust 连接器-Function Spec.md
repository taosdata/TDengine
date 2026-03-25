# Rust 连接器-Function Spec

## 1. 修订记录

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/01/05 | 1.0 | 郭振伟 | 编写文档。 |
| 2026/01/19 | 1.1 | 郭振伟 | 更新文档至 TDengine v3.4.0.0 版本。 |

## 2. 背景

在当今物联网和工业互联网快速发展的背景下，大量设备产生的时序数据需要高效的存储和实时分析解决方案。TDengine 作为一款专为物联网、车联网等场景设计的时序数据库，已广泛应用于各行各业。然而，随着 Rust 编程语言在系统编程和高性能应用开发中的流行，Rust 社区对与 TDengine 数据库交互的需求也日益增长。
Rust 以其内存安全、高性能和零运行时开销的特点受到开发者的青睐，特别适用于对性能和可靠性要求极高的场景。为了满足 Rust 开发者与 TDengine 交互的需求，设计并实现一个功能强大的 Rust 连接器显得尤为重要。
Rust 连接器旨在为开发者提供高效、简洁且易用的 API 接口，支持 Rust 语言的现代编程风格和生态系统。通过封装 TDengine 的核心功能，Rust 连接器将支持 SQL 执行、参数绑定、无模式写入和数据订阅等关键特性。

## 3. 定义

1. **DSN（Data Source Name）**：数据源名称，包含连接数据库所需的配置信息，如主机地址、端口、用户名、密码等。通过 DSN，可以简化数据库连接的配置过程。
2. **TMQ（TDengine Message Queue）**：TDengine 消息队列，用于实时订阅数据库中的数据变化。客户端可以通过 TMQ 接收增量数据，适用于需要实时监控和数据流处理的场景。
3. **SML（Schemaless）**：无模式写入是一种写入机制，在写入数据时无需事先定义表结构，系统会根据数据自动生成或调整表结构。
4. **Stmt（Prepared Statement）**：参数绑定，通过使用占位符替代具体的值，可防止 SQL 注入并提升查询性能。

## 4. 行为说明

Rust 连接器的接口分为同步接口和异步接口，一般同步接口是由异步接口实现，方法签名除 async 关键字外基本相同。对于同步接口和异步接口功能一样的接口，本文档只提供同步接口的说明。
对于 WebSocket 连接和原生连接两种方式，除了建立连接的 DSN 不同，其余接口调用没有区别。

### 4.1 连接功能

#### 4.1.1 API

##### 4.1.1.1 DSN

TaosBuilder 通过 DSN 连接描述字符串创建一个连接构造器。DSN 描述字符串基本结构如下：
```plaintext
<driver>[+<protocol>]://[[<username>:<password>@]<host>:<port>][/<database>][?<p1>=<v1>[&<p2>=<v2>]]
|------|------------|---|-----------|-----------|------|------|------------|-----------------------|
|driver|   protocol |   | username  | password  | host | port |  database  |  params               |
```

```plaintext
<driver>[+<protocol>]://[<username>:<password>@][<host1>:<port1>[,...<hostN>:<portN>]][/<database>][?<key1>=<value1>[&...<keyN>=<valueN>]]
|------|-----------|---|----------|-----------|-------------------------------------|------------|--------------------------------------|
|driver|  protocol |   | username | password  |  addresses                          |   database |   params                             |
```

各部分意义如下：
- driver：必须指定驱动名以便连接器选择何种方式创建连接，支持如下驱动名：
  - taos：使用 TDengine 连接器驱动，默认是使用 taos 驱动。
  - tmq：使用 TMQ 订阅数据。
- protocol：显示指定以何种方式建立连接，例如：taos+ws://localhost:6041 指定以 WebSocket 方式建立连接。
  - http/ws：使用 WebSocket 创建连接。
  - https/wss：在 WebSocket 连接方式下显示启用 SSL/TLS 连接。
- username/password：用于创建连接的用户名及密码。
- addresses：指定创建连接的服务器地址，多个地址间用英文逗号分隔。对于 IPv6 地址，必须使用中括号括起来（如 `[::1]` 或 `[2001:db8:1234:5678::1]`），以避免端口号解析冲突。
  - 示例：`ws://host1:6041,host2:6041` 或 `ws://`（等同于 `ws://localhost:6041`）。
- database：指定默认连接的数据库名，可选参数。
- params：其他可选参数。
一个完整的 DSN 描述字符串示例如下：taos+ws://localhost:6041/test，表示使用 WebSocket（ws）方式通过 6041 端口连接服务器 localhost，并指定默认数据库为 test。

##### 4.1.1.2 TaosBuilder

TaosBuilder 结构体主要提供了根据 DSN 构建 Taos 对象的方法，还提供了检查连接，以及获取客户端版本号等功能。
- fn available_params() -> &'static [&'static str]
  - 接口说明：获取 DSN 中可用的参数列表。
  - 返回值：返回静态字符串切片的引用，包含可用的参数名称。
- fn from_dsn<D: IntoDsn>(dsn: D) -> RawResult<Self>
  - 接口说明：使用 DSN 字符串创建连接，不检查连接。
  - 参数说明：
    - dsn：DSN 字符串或可转换为 DSN 的类型。
  - 返回值：成功时返回自身类型的 RawResult，失败时返回错误。
- fn client_version() -> &'static str
  - 接口说明：获取客户端版本。
  - 返回值：返回客户端版本的静态字符串。
- fn server_version(&self) -> RawResult<&str>
  - 接口说明：获取服务端版本。
  - 返回值：成功时返回服务端版本的字符串，失败时返回错误。
- fn ping(&self, conn: &mut Self::Target) -> RawResult<()>
  - 接口说明：检查连接是否仍然存活。
  - 参数说明：
    - conn：目标连接的可变引用。
  - 返回值：成功时返回空的 RawResult，失败时返回错误。
- fn ready(&self) -> bool
  - 接口说明：检查是否准备好连接。
  - 返回值：大多数情况下返回 true，表示地址准备好连接。
- fn build(&self) -> RawResult<Self::Target>
  - 接口说明：从此结构创建新的 Taos 对象。
  - 返回值：成功时返回目标连接类型的 RawResult，失败时返回错误。
- fn is_enterprise_edition(&self) -> RawResult<bool>
  - 接口说明：判断连接的 TDengine 数据库是否为企业版。
  - 返回值：成功时返回连接的 TDengine 数据库是否为企业版，失败时返回错误。
- fn get_edition(&self) -> RawResult<Edition>
  - 接口说明：获取连接的 TDengine 数据库的版本信息。
  - 返回值：成功时返回连接的 TDengine 数据库的版本信息，失败时返回错误。

#### 4.1.2 示例代码

```rust
use taos::{sync::TBuilder, TaosBuilder};

fn main() -> anyhow::Result<()> {
    let available_params = TaosBuilder::available_params();
    println!("Available params: {available_params:?}");

    let dsn = "ws://localhost:6041";
    let builder = TaosBuilder::from_dsn(dsn)?;

    let ready = builder.ready();
    println!("Ready: {ready}");

    let mut taos = builder.build()?;
    builder.ping(&mut taos)?;

    let client_version = TaosBuilder::client_version();
    println!("Client version: {client_version}");

    let server_version = builder.server_version()?;
    println!("Server version: {server_version}");

    let is_enterprise_edition = builder.is_enterprise_edition()?;
    println!("Is enterprise edition: {is_enterprise_edition}");

    let edition = builder.get_edition()?;
    println!("Edition: {edition:?}");

    Ok(())
}
```

### 4.2 执行 SQL

#### 4.2.1 API

执行 SQL 主要使用 Taos 结构体，Taos 结构体提供了多个数据库操作的 API，包括：执行 SQL、无模式写入以及一些常用数据库查询的封装。
- pub fn is_native(&self) -> bool
  - 接口说明：判断连接是否使用本地协议。
  - 返回值：如果使用本地协议，则返回 true，否则返回 false。
- pub fn is_ws(&self) -> bool
  - 接口说明：判断连接是否使用 WebSocket 协议。
  - 返回值：如果使用 WebSocket 协议，则返回 true，否则返回 false。
- fn query<T: AsRef<str>>(&self, sql: T) -> RawResult<Self::ResultSet>
  - 接口说明：执行 SQL 查询。
  - 参数说明：
    - sql：要执行的 SQL 语句。
  - 返回值：成功时返回结果集 ResultSet 的 RawResult，失败时返回错误。
- fn query_with_req_id<T: AsRef<str>>(&self, sql: T, req_id: u64) -> RawResult<Self::ResultSet>
  - 接口说明：带请求 ID 执行 SQL 查询。
  - 参数说明：
    - sql：要执行的 SQL 语句。
    - req_id：请求 ID。
  - 返回值：成功时返回结果集 ResultSet 的 RawResult，失败时返回错误。
- fn exec<T: AsRef<str>>(&self, sql: T) -> RawResult<usize>
  - 接口说明：执行 SQL 语句。
  - 参数说明：
    - sql：要执行的 SQL 语句。
  - 返回值：成功时返回受影响的行数，失败时返回错误。
- fn exec_many<T: AsRef<str>, I: IntoIterator<Item = T>>(&self, input: I) -> RawResult<usize>
  - 接口说明：批量执行 SQL 语句。
  - 参数说明：
    - input：要执行的 SQL 语句集合。
  - 返回值：成功时返回总共受影响的行数，失败时返回错误。
- fn query_one<T: AsRef<str>, O: DeserializeOwned>(&self, sql: T) -> RawResult<Option<O>>
  - 接口说明：执行 SQL 查询并返回单个结果。
  - 参数说明：
    - sql：要执行的 SQL 语句。
  - 返回值：成功时返回可选的结果对象，失败时返回错误。
- fn server_version(&self) -> RawResult<Cow<str>>
  - 接口说明：获取服务器版本。
  - 返回值：成功时返回服务器版本字符串的 RawResult，失败时返回错误。
- fn create_topic(&self, name: impl AsRef<str>, sql: impl AsRef<str>) -> RawResult<()>
  - 接口说明：创建主题。
  - 参数说明：
    - name：主题名称。
    - sql：关联的 SQL 语句。
  - 返回值：成功时返回空的 RawResult，失败时返回错误。
- fn databases(&self) -> RawResult<Vec<ShowDatabase>>
  - 接口说明：获取数据库列表。
  - 返回值：成功时返回数据库列表的 RawResult，失败时返回错误。
- fn topics(&self) -> RawResult<Vec<Topic>>
  - 接口说明：获取主题信息。
  - 返回值：成功时返回主题列表的 RawResult，失败时返回错误。
- fn describe(&self, table: &str) -> RawResult<Describe>
  - 接口说明：描述表结构。
  - 参数说明：
    - table：表名称。
  - 返回值：成功时返回表结构描述的 RawResult，失败时返回错误。
- fn database_exists(&self, name: &str) -> RawResult<bool>
  - 接口说明：检查数据库是否存在。
  - 参数说明：
    - name：数据库名称。
  - 返回值：成功时返回布尔值的 RawResult，指示数据库是否存在，失败时返回错误。
- fn put(&self, data: &SmlData) -> RawResult<()>
  - 接口说明：写入无模式数据，SmlData 结构介绍见下文。
  - 参数说明：
    - data：无模式数据。
  - 返回值：成功时返回空的 RawResult，失败时返回错误。

#### 4.2.2 示例代码

```rust
use serde::Deserialize;
use taos::taos_query::common::{SchemalessPrecision, SchemalessProtocol, SmlDataBuilder};
use taos::taos_query::prelude::sync::*;
use taos::{sync::TBuilder, TaosBuilder};

fn main() -> anyhow::Result<()> {
    let dsn = "ws://localhost:6041";
    let taos = TaosBuilder::from_dsn(dsn)?.build()?;

    let is_native = taos.is_native();
    println!("Is native: {is_native}");

    let is_ws = taos.is_ws();
    println!("Is ws: {is_ws}");

    let db = "test";
    let topic = "topic_test";

    taos.exec_many([
        &format!("drop topic if exists {topic}"),
        &format!("drop database if exists {db}"),
        &format!("create database {db}"),
        &format!("use {db}"),
        "create table t0 (ts timestamp, c1 int)",
        "insert into t0 values (now, 1)",
    ])?;

    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    struct Record {
        ts: i64,
        c1: i32,
    }

    let records: Vec<Record> = taos
        .query("select * from t0")?
        .deserialize()
        .try_collect()?;
    println!("Records: {records:?}");

    let records: Vec<Record> = taos
        .query_with_req_id("select * from t0", 1001)?
        .deserialize()
        .try_collect()?;
    println!("Records: {records:?}");

    let record: Option<Record> = taos.query_one("select * from t0")?;
    println!("Record: {record:?}");

    let server_version = taos.server_version()?;
    println!("Server version: {server_version}");

    let databases = taos.databases()?;
    println!("Databases: {databases:?}");

    let database_exist = taos.database_exists(db)?;
    println!("Database exist: {database_exist}");

    taos.create_topic(topic, format!("select * from {db}.t0"))?;

    let topics = taos.topics()?;
    println!("Topics: {topics:?}");

    let dsn = format!("ws://localhost:6041/{db}");
    let taos = TaosBuilder::from_dsn(dsn)?.build()?;

    let data = [
        "measurement,host=host1 field1=2i,field2=2.0 1577837300000".to_owned(),
        "measurement,host=host1 field1=2i,field2=2.0 1577837400000".to_owned(),
        "measurement,host=host1 field1=2i,field2=2.0 1577837500000".to_owned(),
        "measurement,host=host1 field1=2i,field2=2.0 1577837600000".to_owned(),
    ];

    let sml_data = SmlDataBuilder::default()
        .protocol(SchemalessProtocol::Line)
        .precision(SchemalessPrecision::Millisecond)
        .data(data)
        .ttl(1000)
        .req_id(1002u64)
        .build()?;

    taos.put(&sml_data)?;

    taos.exec(format!("drop topic {topic}"))?;
    taos.exec(format!("drop database {db}"))?;

    Ok(())
}
```

### 4.3 无模式写入

#### 4.3.1 API

SmlData 结构体提供了无模式写入的数据结构，以及获取属性的方法。
- pub struct SmlData
  - 结构体说明：SmlData 结构体用于存储无模式数据及其相关信息。
  - 字段说明：
    - protocol：无模式协议，支持 InfluxDB Line，OpenTSDB Telnet 和 OpenTSDB Json。
    - precision：时间戳精度，支持 Hours，Minutes，Seconds，Millisecond（默认），Microsecond 和 Nanosecond。
    - data：数据列表。
    - ttl：数据存活时间，单位为秒。
    - req_id：请求 ID。
- pub fn protocol(&self) -> SchemalessProtocol
  - 接口说明：获取无模式协议。
  - 返回值：无模式协议类型，支持 InfluxDB Line，OpenTSDB Telnet 和 OpenTSDB Json。
- pub fn precision(&self) -> SchemalessPrecision
  - 接口说明：获取时间戳精度。
  - 返回值：时间戳精度类型，支持 Hours，Minutes，Seconds，Millisecond（默认），Microsecond 和 Nanosecond。
- pub fn data(&self) -> &Vec<String>
  - 接口说明：获取数据列表。
  - 返回值：数据列表的引用。
- pub fn ttl(&self) -> Option<i32>
  - 接口说明：获取数据存活时间。
  - 返回值：数据存活时间（可选），单位为秒。
- pub fn req_id(&self) -> Option<u64>
  - 接口说明：获取请求 ID。
  - 返回值：请求 ID（可选）。

#### 4.3.2 示例代码

```rust
use taos::taos_query::common::{SchemalessPrecision, SchemalessProtocol, SmlDataBuilder};

fn main() -> anyhow::Result<()> {
    let data = [
        "measurement,host=host1 field1=2i,field2=2.0 1577837300000".to_owned(),
        "measurement,host=host1 field1=2i,field2=2.0 1577837400000".to_owned(),
        "measurement,host=host1 field1=2i,field2=2.0 1577837500000".to_owned(),
        "measurement,host=host1 field1=2i,field2=2.0 1577837600000".to_owned(),
    ];

    let sml_data = SmlDataBuilder::default()
        .protocol(SchemalessProtocol::Line)
        .precision(SchemalessPrecision::Millisecond)
        .data(data)
        .ttl(1000)
        .req_id(1001u64)
        .build()?;

    let protocol = sml_data.protocol();
    println!("Protocol: {protocol:?}");

    let precision = sml_data.precision();
    println!("Precision: {precision:?}");

    let data = sml_data.data();
    println!("Data: {data:?}");

    let ttl = sml_data.ttl();
    println!("TTL: {ttl:?}");

    let req_id = sml_data.req_id();
    println!("Req ID: {req_id:?}");

    Ok(())
}
```

### 4.4 结果获取

#### 4.4.1 API

##### 4.4.1.1 ResultSet

ResultSet 结构体提供了结果集的一些方法，可以用来获取结果集的数据和元数据。
- fn affected_rows(&self) -> i32
  - 接口说明：获取受影响的行数。
  - 返回值：受影响的行数，类型为 i32。
- fn precision(&self) -> Precision
  - 接口说明：获取精度信息。
  - 返回值：精度信息，类型为 Precision。
- fn fields(&self) -> &[Field]
  - 接口说明：获取字段信息。
  - 返回值：字段信息数组的引用。
- fn summary(&self) -> (usize, usize)
  - 接口说明：获取摘要信息。
  - 返回值：包含两个 usize 类型的元组，分别表示某些统计信息。
- fn num_of_fields(&self) -> usize
  - 接口说明：获取字段数量。
  - 返回值：字段数量，类型为 usize。
- fn blocks(&mut self) -> IBlockIter<'_, Self>
  - 接口说明：获取原始数据块的迭代器。
  - 返回值：原始数据块的迭代器，类型为 IBlockIter<'_, Self>。
- fn rows(&mut self) -> IRowsIter<'_, Self>
  - 接口说明：获取按行查询的迭代器。
  - 返回值：按行查询的迭代器，类型为 IRowsIter<'_, Self>。
- fn deserialize<T>(&mut self) -> Map<IRowsIter<'_, Self>, fn(_: Result<RowView<'_>, Error>) -> Result<T, Error>>
  - 接口说明：反序列化行数据。
  - 泛型参数：
    - T：目标类型，需实现 DeserializeOwned。
  - 返回值：反序列化结果的映射，类型为 Map<IRowsIter<'_, Self>, fn(_: Result<RowView<'_>, Error>) -> Result<T, Error>>。
- fn to_rows_vec(&mut self) -> Result<Vec<Vec<Value>>, Error>
  - 接口说明：将结果集转换为值的二维向量。
  - 返回值：成功时返回值的二维向量，失败时返回错误，类型为 Result<Vec<Vec<Value>>, Error>。

##### 4.4.1.2 Feild

Feild 结构体提供了字段信息的一些方法。
- pub const fn empty() -> Field
  - 接口说明：创建一个空的 Field 实例。
  - 返回值：返回一个空的 Field 实例。
- pub fn new(name: impl Into<String>, ty: Ty, bytes: u32) -> Field
  - 接口说明：创建一个新的 Field 实例。
  - 参数说明：
    - name：字段名称。
    - ty：字段类型。
    - bytes：字段数据长度。
  - 返回值：返回一个新的 Field 实例。
- pub fn name(&self) -> &str
  - 接口说明：获取字段名称。
  - 返回值：返回字段的名称。
- pub fn escaped_name(&self) -> String
  - 接口说明：获取转义后的字段名称。
  - 返回值：返回转义后的字段名称。
- pub const fn ty(&self) -> Ty
  - 接口说明：获取字段类型。
  - 返回值：返回字段的类型。
- pub const fn bytes(&self) -> u32
  - 接口说明：获取字段的预设长度。
  - 返回值：对于变长数据类型，返回其预设长度；对于其他类型，返回其字节宽度。
- pub fn to_c_field(&self) -> c_field_t
  - 接口说明：将 Field 实例转换为 C 语言结构体。
  - 返回值：返回 C 语言结构体表示的字段。
- pub fn sql_repr(&self) -> String
  - 接口说明：表示字段在 SQL 中的数据类型。
  - 返回值：例如："INT", "VARCHAR(100)" 等 SQL 数据类型表示。

#### 4.4.2 示例代码

##### 4.4.2.1 ResultSet

```rust
use serde::Deserialize;
use taos::taos_query::prelude::sync::*;
use taos::{sync::TBuilder, TaosBuilder};

fn main() -> anyhow::Result<()> {
    let dsn = "ws://localhost:6041";
    let taos = TaosBuilder::from_dsn(dsn)?.build()?;

    let db = "test";

    taos.exec_many([
        &format!("drop database if exists {db}"),
        &format!("create database {db}"),
        &format!("use {db}"),
        "create table t0 (ts timestamp, c1 int)",
        "insert into t0 values (now, 1)",
    ])?;

    let mut rs = taos.query("select * from t0")?;

    let affected_rows = rs.affected_rows();
    println!("Affected rows: {affected_rows}");

    let precision = rs.precision();
    println!("Precision: {precision}");

    let fields_num = rs.num_of_fields();
    println!("Fields number: {fields_num}");

    let fields = rs.fields();
    println!("Fields: {fields:?}");

    let summary = rs.summary();
    println!("Summary: {summary:?}");

    for block in rs.blocks() {
        let block = block?;
        println!("Block: {block:?}");
    }

    let mut rs = taos.query("select * from t0")?;

    for row in rs.rows() {
        let row = row?;
        println!("Row: {row:?}");
    }

    let mut rs = taos.query("select * from t0")?;

    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    struct Record {
        ts: i64,
        c1: i32,
    }

    let records: Vec<Record> = rs.deserialize().try_collect()?;
    println!("Records: {records:?}");

    let mut rs = taos.query("select * from t0")?;
    let rows = rs.to_rows_vec()?;
    println!("Rows: {rows:?}");

    taos.exec(format!("drop database {db}"))?;

    Ok(())
}
```

##### 4.4.2.2 Feild

```rust
use taos::taos_query::prelude::sync::*;

fn main() {
    let field = Field::empty();
    println!("Field: {field:?}");

    let field = Field::new("name", Ty::NChar, 10);
    println!("Field: {field:?}");

    let name = field.name();
    println!("Name: {name}");

    let escaped_name = field.escaped_name();
    println!("Escaped name: {escaped_name}");

    let ty = field.ty();
    println!("Type: {ty:?}");

    let len = field.bytes();
    println!("Length: {len}");

    let c_field = field.to_c_field();
    println!("C field: {c_field:?}");

    let sql_ty = field.sql_repr();
    println!("SQL type: {sql_ty}");
}
```

### 4.5 参数绑定

#### 4.5.1 API

Stmt 结构体提供了参数绑定相关功能，用于实现高效写入。
- fn init(taos: &Q) -> RawResult<Self>
  - 接口说明：初始化参数绑定实例。
  - 参数说明：
    - taos：数据库连接实例。
  - 返回值：成功时返回初始化的实例，失败时返回错误。
- fn init_with_req_id(taos: &Q, req_id: u64) -> RawResult<Self>
  - 接口说明：使用请求 ID 初始化参数绑定实例。
  - 参数说明：
    - taos：数据库连接实例。
    - req_id：请求 ID。
  - 返回值：成功时返回初始化的实例，失败时返回错误。
- fn prepare<S: AsRef<str>>(&mut self, sql: S) -> RawResult<&mut Self>
  - 接口说明：准备要绑定的 SQL 语句。
  - 参数说明：
    - sql：要准备的 SQL 语句。
  - 返回值：成功时返回自身的可变引用，失败时返回错误。
- fn set_tbname<S: AsRef<str>>(&mut self, name: S) -> RawResult<&mut Self>
  - 接口说明：设置表名称。
  - 参数说明：
    - name：表名称。
  - 返回值：成功时返回自身的可变引用，失败时返回错误。
- fn set_tags(&mut self, tags: &[Value]) -> RawResult<&mut Self>
  - 接口说明：设置标签。
  - 参数说明：
    - tags：标签数组。
  - 返回值：成功时返回自身的可变引用，失败时返回错误。
- fn set_tbname_tags<S: AsRef<str>>(&mut self, name: S, tags: &[Value]) -> RawResult<&mut Self>
  - 接口说明：设置表名称和标签。
  - 参数说明：
    - name：表名称。
    - tags：标签数组。
  - 返回值：成功时返回自身的可变引用，失败时返回错误。
- fn bind(&mut self, params: &[ColumnView]) -> RawResult<&mut Self>
  - 接口说明：绑定参数。
  - 参数说明：
    - params：参数数组。
  - 返回值：成功时返回自身的可变引用，失败时返回错误。
- fn add_batch(&mut self) -> RawResult<&mut Self>
  - 接口说明：添加批处理。
  - 返回值：成功时返回自身的可变引用，失败时返回错误。
- fn execute(&mut self) -> RawResult<usize>
  - 接口说明：执行语句。
  - 返回值：成功时返回受影响的行数，失败时返回错误。
- fn affected_rows(&self) -> usize
  - 接口说明：获取受影响的行数。
  - 返回值：受影响的行数。

#### 4.5.2 示例代码

```rust
use taos::taos_query::prelude::sync::*;
use taos::Stmt;
use taos::{sync::TBuilder, TaosBuilder};

fn main() -> anyhow::Result<()> {
    let dsn = "ws://localhost:6041";
    let taos = TaosBuilder::from_dsn(dsn)?.build()?;

    let db = "test";

    taos.exec_many([
        &format!("drop database if exists {db}"),
        &format!("create database {db}"),
        &format!("use {db}"),
        "create table s0 (ts timestamp, c1 int) tags (t1 int)",
    ])?;

    let mut stmt = Stmt::init(&taos)?;

    let params = vec![
        ColumnView::from_millis_timestamp(vec![1726803358466]),
        ColumnView::from_ints(vec![1]),
    ];

    let res = stmt
        .prepare("insert into ? using s0 tags(?) values(?, ?)")?
        .set_tbname_tags("d0", &[Value::Int(1)])?
        .bind(&params)?
        .add_batch()?
        .execute()?;

    println!("Res: {res:?}");

    let affected_rows = stmt.affected_rows();
    println!("Affected rows: {affected_rows}");

    Ok(())
}
```

### 4.6 参数绑定v2

#### 4.6.1 API

Stmt2 结构体提供了参数绑定v2相关功能，用于实现高效写入。
- fn init(taos: &Q) -> RawResult<Self>;
  - 接口说明：初始化参数绑定v2实例。
  - 参数说明：
    - taos：数据库连接实例。
  - 返回值：成功时返回初始化的实例，失败时返回错误。
- fn prepare(&mut self, sql: &str) -> RawResult<&mut Self>;
  - 接口说明：准备要绑定的 SQL 语句。
  - 参数说明：
    - sql：要准备的 SQL 语句。
  - 返回值：成功时返回自身的可变引用，失败时返回错误。
- fn bind(&mut self, params: &[Stmt2BindParam]) -> RawResult<&mut Self>;
  - 接口说明：绑定参数。
  - 参数说明：
    - params：参数数组。
  - 返回值：成功时返回自身的可变引用，失败时返回错误。
- fn exec(&mut self) -> RawResult<usize>;
  - 接口说明：执行语句。
  - 返回值：成功时返回受影响的行数，失败时返回错误。
- fn affected_rows(&self) -> usize;
  - 接口说明：获取受影响的行数。
  - 返回值：受影响的行数。
- fn result_set(&self) -> RawResult<Q::ResultSet>;
  - 接口说明：获取查询结果。
  - 返回值：成功时返回查询结果集，失败时返回错误。

#### 4.6.2 示例代码

```rust
use taos::sync::*;

fn main() -> anyhow::Result<()> {
    let db = "test_stmt2";
    let dsn = "ws://localhost:6041";

    let taos = TaosBuilder::from_dsn(dsn)?.build()?;
    taos.exec_many([
        &format!("drop database if exists {db}"),
        &format!("create database {db} keep 36500"),
        &format!("use {db}"),
        "create table t0 (ts timestamp, c1 bool, c2 int)",
    ])?;

    let mut stmt2 = Stmt2::init(&taos)?;
    stmt2.prepare("insert into t0 values(?, ?, ?)")?;

    let cols = vec![
        ColumnView::from_millis_timestamp(vec![1726803356466]),
        ColumnView::from_bools(vec![true]),
        ColumnView::from_ints(vec![99]),
    ];

    let param = Stmt2BindParam::new(None, None, Some(cols));
    let affected = stmt2.bind(&[param])?.exec()?;
    assert_eq!(affected, 1);
    assert_eq!(stmt2.affected_rows(), 1);

    stmt2.prepare("select * from t0 where c2 > ?")?;

    let cols = vec![ColumnView::from_ints(vec![0])];
    let param = Stmt2BindParam::new(None, None, Some(cols));
    let affected = stmt2.bind(&[param])?.exec()?;
    assert_eq!(affected, 0);

    #[derive(Debug, serde::Deserialize)]
    struct Row {
        ts: i64,
        c1: bool,
        c2: i32,
    }

    let rows: Vec<Row> = stmt2.result_set()?.deserialize().try_collect()?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].ts, 1726803356466);
    assert_eq!(rows[0].c1, true);
    assert_eq!(rows[0].c2, 99);

    Ok(())
}
```

### 4.7 数据订阅

#### 4.7.1 API

数据订阅主要涉及三个结构体，提供连接建立的 TmqBuilder，消费数据和提交偏移量的 Consumer，以及偏移量 Offset。

##### 4.7.1.1 TmqBuilder

同 TaosBuilder 类似，TmqBuilder 提供了创建消费者对象的功能。
- fn available_params() -> &'static [&'static str]
  - 接口说明：获取 DSN 中可用的参数列表。
  - 返回值：返回静态字符串切片的引用，包含可用的参数名称。
- fn from_dsn<D: IntoDsn>(dsn: D) -> RawResult<Self>
  - 接口说明：使用 DSN 字符串创建连接，不检查连接。
  - 参数说明：
    - dsn：DSN 字符串或可转换为 DSN 的类型。
  - 返回值：成功时返回自身类型的 RawResult，失败时返回错误。
- fn client_version() -> &'static str
  - 接口说明：获取客户端版本。
  - 返回值：返回客户端版本的静态字符串。
- fn ping(&self, conn: &mut Self::Target) -> RawResult<()>
  - 接口说明：检查连接是否仍然存活。
  - 参数说明：
    - conn：目标连接的可变引用。
  - 返回值：成功时返回空的 RawResult，失败时返回错误。
- fn ready(&self) -> bool
  - 接口说明：检查是否准备好连接。
  - 返回值：大多数情况下返回 true，表示地址准备好连接。
- fn build(&self) -> RawResult<Self::Target>
  - 接口说明：从此结构创建新的连接。
  - 返回值：成功时返回目标连接类型的 RawResult，失败时返回错误。

##### 4.7.1.2 Consumer

1. Consumer 结构体提供了订阅相关的功能，包括订阅，获取消息，提交偏移量和设置偏移量等。
- fn subscribe<T: Into<String>, I: IntoIterator<Item = T> + Send>(&mut self, topics: I) -> RawResult<()>
  - 接口说明：订阅一系列主题。
  - 参数说明：
    - topics：要订阅的主题列表。
  - 返回值：成功时返回空的 RawResult，失败时返回错误。
- fn recv_timeout(&self, timeout: Timeout) -> RawResult<Option<(Self::Offset, MessageSet<Self::Meta, Self::Data>)>>
  - 接口说明：在指定超时时间内接收消息。
  - 参数说明：
    - timeout：超时时间。
  - 返回值：成功时返回消息，失败时返回错误。
- fn commit(&self, offset: Self::Offset) -> RawResult<()>
  - 接口说明：提交给定的偏移量。
  - 参数说明：
    - offset：要提交的偏移量，见下文 Offset 结构体。
  - 返回值：成功时返回空的 RawResult，失败时返回错误。
- fn commit_offset(&self, topic_name: &str, vgroup_id: VGroupId, offset: i64) -> RawResult<()>
  - 接口说明：为特定主题和分区提交偏移量。
  - 参数说明：
    - topic_name：主题名称。
    - vgroup_id：分区 ID。
    - offset：要提交的偏移量。
  - 返回值：成功时返回空的 RawResult，失败时返回错误。
- fn list_topics(&self) -> RawResult<Vec<String>>
  - 接口说明：列出所有可用主题。
  - 返回值：成功时返回主题列表，失败时返回错误。
- fn assignments(&self) -> Option<Vec<(String, Vec<Assignment>)>>
  - 接口说明：获取当前分配的主题和分区。
  - 返回值：成功时返回分配信息，失败时返回 None。
- fn offset_seek(&mut self, topic: &str, vg_id: VGroupId, offset: i64) -> RawResult<()>
  - 接口说明：为特定主题和分区设置偏移量。
  - 参数说明：
    - topic：主题名称。
    - vg_id：分区 ID。
    - offset：要设置的偏移量。
  - 返回值：成功时返回空的 RawResult，失败时返回错误。
- fn committed(&self, topic: &str, vgroup_id: VGroupId) -> RawResult<i64>
  - 接口说明：获取特定主题和分区的已提交偏移量。
  - 参数说明：
    - topic：主题名称。
    - vgroup_id：分区 ID。
  - 返回值：成功时返回偏移量，失败时返回错误。
- fn position(&self, topic: &str, vgroup_id: VGroupId) -> RawResult<i64>
  - 接口说明：获取特定主题和分区的当前位置。
  - 参数说明：
    - topic：主题名称。
    - vgroup_id：分区 ID。
  - 返回值：成功时返回当前位置，失败时返回错误。

##### 4.7.1.3 Offset

Offset 结构体提供了获取当前消息所属的数据库，主题和分区信息。
- fn database(&self) -> &str
  - 接口说明：获取当前消息的数据库名称。
  - 返回值：数据库名称的引用。
- fn topic(&self) -> &str
  - 接口说明：获取当前消息的主题名称。
  - 返回值：主题名称的引用。
- fn vgroup_id(&self) -> VGroupId
  - 接口说明：获取当前消息的分区 ID。
  - 返回值：分区 ID。

#### 4.7.2 示例代码

##### 4.7.2.1 TmqBuilder

```rust
use taos::sync::TBuilder;
use taos::TmqBuilder;

fn main() -> anyhow::Result<()> {
    let available_params = TmqBuilder::available_params();
    println!("Available params: {available_params:?}");

    let client_version = TmqBuilder::client_version();
    println!("Client version: {client_version}");

    let dsn = "ws://localhost:6041?group.id=1";
    let builder = TmqBuilder::from_dsn(dsn)?;

    let ready = builder.ready();
    println!("Ready: {ready}");

    let mut consumer = builder.build()?;

    builder.ping(&mut consumer)?;

    Ok(())
}
```

##### 4.7.2.2 Consumer

```rust
use taos::sync::{AsConsumer, TBuilder};
use taos::taos_query::prelude::sync::*;
use taos::{TaosBuilder, TmqBuilder};

fn main() -> anyhow::Result<()> {
    let db = "test";
    let topic = "tmq_meters";
    let group_id = 1;

    let dsn = "ws://localhost:6041";
    let taos = TaosBuilder::from_dsn(dsn)?.build()?;

    taos.exec_many([
        &format!("drop topic if exists {topic}"),
        &format!("drop database if exists {db}"),
        &format!("create database {db}"),
        &format!("create topic {topic} as database {db}"),
        &format!("use {db}"),
        "create table t0 (ts timestamp, c1 int)",
        "insert into t0 values (now, 1)",
    ])?;

    let dsn = format!("ws://localhost:6041?group.id={group_id}&auto.offset.reset=earliest");
    let mut consumer = TmqBuilder::from_dsn(dsn)?.build()?;

    let topics = consumer.list_topics()?;
    println!("Topics: {topics:?}");

    let assignments = consumer.assignments();
    println!("Assignments: {assignments:?}");

    consumer.subscribe(["tmq_meters"])?;

    if let Some((offset, msg)) = consumer.recv_timeout(taos::Timeout::Never)? {
        println!("Offset: {offset:?}, msg: {msg:?}");

        let vgroup_id = offset.vgroup_id();

        let committed = consumer.committed(topic, vgroup_id)?;
        println!("Committed: {committed:?}");

        let position = consumer.position(topic, vgroup_id)?;
        println!("Position: {position:?}");

        consumer.commit(offset)?;
    }

    consumer.unsubscribe();

    Ok(())
}
```

##### 4.7.2.3 Offset

```rust
use taos::sync::{AsConsumer, TBuilder};
use taos::taos_query::prelude::sync::*;
use taos::{TaosBuilder, TmqBuilder};

fn main() -> anyhow::Result<()> {
    let db = "test";
    let topic = "topic_test";
    let group_id = 1;

    let dsn = "ws://localhost:6041";
    let taos = TaosBuilder::from_dsn(dsn)?.build()?;

    taos.exec_many([
        &format!("drop topic if exists {topic}"),
        &format!("drop database if exists {db}"),
        &format!("create database {db}"),
        &format!("create topic {topic} as database {db}"),
        &format!("use {db}"),
        "create table t0 (ts timestamp, c1 int)",
        "insert into t0 values (now, 1)",
    ])?;

    let dsn = format!("ws://localhost:6041?group.id={group_id}&auto.offset.reset=earliest");
    let mut consumer = TmqBuilder::from_dsn(dsn)?.build()?;

    consumer.subscribe([topic])?;

    if let Some((offset, _)) = consumer.recv_timeout(taos::Timeout::Never)? {
        let db = offset.database();
        let topic = offset.topic();
        let vgroup_id = offset.vgroup_id();
        println!("Database: {db}, Topic: {topic}, VGroup ID: {vgroup_id}");
    }

    consumer.unsubscribe();

    Ok(())
}
```

## 5. 安全特性

Rust 连接器提供了多层次的安全保障机制，确保数据传输和存储的安全性。

### 5.1 传输层安全

#### 5.1.1 TLS/SSL 连接

Rust 连接器通过 WebSocket Secure (wss://) 协议支持加密传输。

##### 5.1.1.1 DSN 配置

```rust
// 基础 TLS 连接（系统默认 CA）
let taos = TaosBuilder::from_dsn("wss://localhost:6041")?.build().await?;

// 自定义 CA 证书 - verify_ca 模式
let dsn = "wss://localhost:6041?\
    tls_mode=verify_ca&\
    tls_version=tlsv1.2,tlsv1.3&\
    tls_ca=/path/to/ca.crt";
let taos = TaosBuilder::from_dsn(dsn)?.build().await?;

// 完整身份验证 - verify_identity 模式
let dsn = "wss://localhost:6041?\
    tls_mode=verify_identity&\
    tls_version=tlsv1.3&\
    tls_ca=/path/to/ca.crt";
let taos = TaosBuilder::from_dsn(dsn)?.build().await?;

// 使用 PEM 格式证书内容
let ca_cert = include_str!("ca.crt");  // 在编译时嵌入证书
let dsn = format!("wss://localhost:6041?tls_mode=verify_identity&tls_ca={}", ca_cert);
let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
```

##### 5.1.1.2 TLS 参数说明

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| `tls_mode` | String | `verify_ca`: 验证 CA 证书<br>`verify_identity`: 验证身份+主机名 |
| `tls_version` | String | 逗号分隔的版本列表: `tlsv1.2`, `tlsv1.3` |
| `tls_ca` | String | PEM 格式证书内容或证书文件路径 |

##### 5.1.1.3 特性标志

使用 TLS 功能需启用以下 Cargo 特性之一:
```toml
[dependencies]
taos = { version = "0.12", features = ["ws-rustls"] }        # rustls (默认)
taos = { version = "0.12", features = ["ws-native-tls"] }    # OpenSSL
```

##### 5.1.1.4 TLS 验证模式对比

| 特性 | verify_ca | verify_identity |
| --- | --- | --- |
| 验证 CA 证书 | ✓ | ✓ |
| 验证证书链 | ✓ | ✓ |
| 验证证书有效期 | ✓ | ✓ |
| 验证主机名/IP | ✗ | ✓ |
| 验证 SAN | ✗ | ✓ |
| 适用场景 | 内部环境 | 生产环境 |

#### 5.1.2 TLS 错误处理

```rust
use taos::*;

match TaosBuilder::from_dsn("wss://localhost:6041")?.build().await {
    Ok(taos) => { /* 正常处理 */ },
    Err(e) => {
        // TLS 相关错误会包含详细信息
        if e.to_string().contains("certificate") {
            eprintln!("证书验证失败: {}", e);
            // 可能的原因：
            // - 证书过期
            // - 主机名不匹配 (SAN mismatch)
            // - CA 证书不可信
        } else if e.to_string().contains("tls") || e.to_string().contains("ssl") {
            eprintln!("TLS 连接错误: {}", e);
            // 可能的原因：
            // - TLS 版本不匹配
            // - 密码套件不支持
            // - 协议协商失败
        }
        return Err(e.into());
    }
}
```

常见 TLS 错误及解决方案：

| 错误信息 | 原因 | 解决方案 |
| --- | --- | --- |
| `certificate not valid for name` | 主机名与证书 SAN 不匹配 | 使用证书中定义的主机名连接 |
| `UnknownIssuer` | CA 证书不可信 | 检查 `tls_ca` 参数是否指向正确的 CA 证书 |
| `expired` | 证书已过期 | 更新服务器证书 |
| `UnsupportedVersion` | TLS 版本不匹配 | 调整 `tls_version` 参数 |

### 5.2 认证机制

#### 5.2.1 支持的认证方式

```rust
// 1. 基础认证（用户名/密码）
let taos = TaosBuilder::from_dsn("ws://root:taosdata@localhost:6041")?.build().await?;

// 2. Token 认证
let taos = TaosBuilder::from_dsn("ws://localhost:6041?token=YOUR_TOKEN")?.build().await?;

// 3. TOTP 双因素认证
let dsn = "ws://user:password@localhost:6041?totp_code=123456";
let taos = TaosBuilder::from_dsn(dsn)?.build().await?;

// 4. Bearer Token
let dsn = "ws://localhost:6041?bearer_token=YOUR_BEARER_TOKEN";
let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
```

#### 5.2.2 凭证安全最佳实践

##### 5.2.2.1 ⚠️ 禁止硬编码密码

❌ **不安全示例:**
```rust
let dsn = "ws://root:my_secret_password@localhost:6041";  // 禁止！
```

✅ **推荐做法 - 使用环境变量:**
```rust
use std::env;

let username = env::var("TAOS_USER").unwrap_or("root".to_string());
let password = env::var("TAOS_PASSWORD").unwrap_or_else(|_| {
    panic!("TAOS_PASSWORD environment variable not set");
});
let dsn = format!("ws://{username}:{password}@localhost:6041");
let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
```

✅ **推荐做法 - 配置文件 + 权限控制:**
```rust
// config.toml (设置文件权限 chmod 600)
use serde::Deserialize;

#[derive(Deserialize)]
struct Config {
    dsn: String,
}

let config_content = std::fs::read_to_string("config.toml")?;
let config: Config = toml::from_str(&config_content)?;
let taos = TaosBuilder::from_dsn(&config.dsn)?.build().await?;
```

✅ **推荐做法 - 使用密钥管理服务:**
```rust
// 示例：从 AWS Secrets Manager 读取
use aws_sdk_secretsmanager::Client;

async fn get_password_from_secrets_manager() -> Result<String, Box<dyn std::error::Error>> {
    let config = aws_config::load_from_env().await;
    let client = Client::new(&config);
    
    let response = client
        .get_secret_value()
        .secret_id("taos/prod/password")
        .send()
        .await?;
    
    Ok(response.secret_string().unwrap().to_string())
}

let password = get_password_from_secrets_manager().await?;
let dsn = format!("ws://root:{}@localhost:6041", password);
```

### 5.3 超时与会话管理

#### 5.3.1 超时配置

```rust
// 连接超时 (默认 10 秒)
let dsn = "ws://localhost:6041?conn_timeout=30";

// 读取超时 (默认 300 秒)
let dsn = "ws://localhost:6041?read_timeout=600";

// 组合配置
let dsn = "ws://root:pass@localhost:6041?conn_timeout=15&read_timeout=120";
let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
```

#### 5.3.2 连接重试策略

```rust
// 重试次数 (默认 5)
let dsn = "ws://localhost:6041?conn_retries=3";

// 退避时间 (默认 200ms)
let dsn = "ws://localhost:6041?retry_backoff_ms=500";

// 最大退避时间 (默认 2000ms)
let dsn = "ws://localhost:6041?retry_backoff_max_ms=5000";

// 完整示例：指数退避重试
let dsn = "ws://localhost:6041?\
    conn_retries=5&\
    retry_backoff_ms=200&\
    retry_backoff_max_ms=10000";
let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
```

#### 5.3.3 连接池安全配置

```rust
use taos::*;
use std::time::Duration;

// deadpool 连接池 (异步)
let pool = TaosBuilder::from_dsn("ws://localhost:6041")?
    .pool_builder()
    .config(deadpool::managed::PoolConfig {
        max_size: 100,                                    // 最大连接数
        timeouts: deadpool::managed::Timeouts {
            wait: Some(Duration::from_secs(30)),          // 获取连接超时
            create: Some(Duration::from_secs(10)),        // 创建连接超时
            recycle: Some(Duration::from_secs(5)),        // 回收连接超时
        },
        queue_mode: deadpool::managed::QueueMode::Fifo,
    })
    .runtime(deadpool::Runtime::Tokio1)
    .build()?;

// r2d2 连接池 (同步)
use taos::sync::*;

let pool = TaosBuilder::from_dsn("ws://localhost:6041")?
    .pool_builder()
    .max_size(100)                                        // 最大连接数
    .connection_timeout(Duration::from_secs(30))          // 获取连接超时
    .max_lifetime(Some(Duration::from_secs(6 * 3600)))   // 连接最大生命周期 (6 小时)
    .idle_timeout(Some(Duration::from_secs(10 * 60)))    // 空闲超时 (10 分钟)
    .min_idle(Some(2))                                    // 最小空闲连接
    .build(Manager::new(TaosBuilder::from_dsn("ws://localhost:6041")?))?
```

**安全考虑:**
- 定期回收长期连接，防止连接劫持
- 空闲超时避免资源泄露
- 限制最大连接数防止资源耗尽

### 5.4 安全日志记录

#### 5.4.1 启用安全审计日志

```rust
use tracing_subscriber;

// 配置日志级别
tracing_subscriber::fmt()
    .with_max_level(tracing::Level::INFO)  // 生产环境使用 INFO
    .with_target(false)                    // 不显示模块路径
    .with_thread_ids(true)                 // 显示线程 ID
    .with_file(true)                       // 显示文件名
    .with_line_number(true)                // 显示行号
    .init();

// 不要在生产环境使用 DEBUG 级别！
// tracing_subscriber::fmt()
//     .with_max_level(tracing::Level::DEBUG)  // ⚠️ 可能泄露敏感信息
//     .init();
```

#### 5.4.2 认证失败处理

```rust
use taos::*;
use std::time::Duration;

let mut retry_count = 0;
let max_retries = 3;
let mut backoff = Duration::from_secs(1);

loop {
    match TaosBuilder::from_dsn(dsn)?.build().await {
        Ok(taos) => {
            tracing::info!("连接成功建立");
            break Ok(taos);
        },
        Err(e) if e.errno() == Code::UNAUTHORIZED => {
            retry_count += 1;
            // 记录认证失败(不含密码)
            tracing::warn!(
                "认证失败 (attempt {}/{})",
                retry_count, max_retries
            );
            
            if retry_count >= max_retries {
                tracing::error!("达到最大重试次数，放弃连接");
                break Err(e.into());
            }
            
            // 指数退避
            tracing::info!("等待 {:?} 后重试", backoff);
            tokio::time::sleep(backoff).await;
            backoff = backoff * 2;  // 指数退避
        },
        Err(e) => {
            tracing::error!("连接失败: {}", e);
            break Err(e.into());
        }
    }
}
```

#### 5.4.3 日志分级策略

| 级别 | 使用场景 |
| --- | --- |
| ERROR | 认证失败、TLS 错误、连接被拒绝、关键操作失败 |
| WARN | 重试、超时、证书即将过期、配置问题 |
| INFO | 连接建立、连接关闭、版本信息、主要操作成功 |
| DEBUG | 详细的协议交互(不含密码)，仅用于开发/测试环境 |
| TRACE | 极为详细的调试信息，仅用于深度调试 |

### 5.5 常见安全错误处理

#### 5.5.1 安全相关错误码

| 错误码 | 错误描述 | 安全建议 |
| --- | --- | --- |
| 0xE006 | 未授权 | 检查用户名密码;记录失败事件;实施速率限制 |
| 0xE009 | TLS 错误 | 验证证书有效性、主机名匹配、TLS 版本支持 |
| 0xE000 | DSN 错误 | 避免在错误消息中回显完整 DSN(可能包含密码) |
| 0xE003 | 发送消息超时 | 可能是 DoS 攻击,启用速率限制 |
| 0xE008 | WebSocket 连接断开 | 检查网络状况,实现自动重连机制 |

#### 5.5.2 错误处理示例

```rust
use taos::*;

async fn secure_connect(dsn: &str) -> Result<Taos, Box<dyn std::error::Error>> {
    match TaosBuilder::from_dsn(dsn)?.build().await {
        Ok(taos) => Ok(taos),
        Err(e) => {
            let error_code = e.errno();
            let error_msg = e.to_string();
            
            // 根据错误类型进行不同处理
            match error_code {
                Code::UNAUTHORIZED => {
                    // 认证失败 - 记录但不记录密码
                    tracing::error!("Authentication failed");
                    // 不要返回包含密码的 DSN
                    Err("认证失败，请检查凭证".into())
                },
                code if code.to_string().contains("E009") => {
                    // TLS 错误
                    tracing::error!("TLS connection failed: {}", error_msg);
                    Err(format!("TLS 连接错误: {}", error_msg).into())
                },
                _ => {
                    // 其他错误
                    tracing::error!("Connection failed: {}", error_msg);
                    Err(e.into())
                }
            }
        }
    }
}
```

### 5.6 生产环境部署检查清单

在部署到生产环境之前，请确保完成以下安全检查：

#### 5.6.1 连接安全

- [ ] 启用 TLS 加密 (`wss://`)
- [ ] 使用 `verify_identity` 模式验证服务器证书
- [ ] 配置合适的 TLS 版本 (建议 TLSv1.3)
- [ ] 验证 CA 证书有效性和过期时间

#### 5.6.2 凭证管理

- [ ] 从环境变量或密钥管理服务读取密码
- [ ] 配置文件权限设置为 600 (`chmod 600 config.toml`)
- [ ] 代码中无硬编码密码或 Token
- [ ] 定期轮换密码和 Token

#### 5.6.3 超时与重试

- [ ] 配置合理的连接超时 (10-30秒)
- [ ] 配置合理的读取超时 (60-300秒)
- [ ] 启用指数退避重试策略
- [ ] 限制最大重试次数 (3-5次)

#### 5.6.4 连接池

- [ ] 启用连接池并限制最大连接数 (50-200)
- [ ] 设置连接最大生命周期 (6-12小时)
- [ ] 配置空闲连接超时 (5-15分钟)
- [ ] 设置合理的最小空闲连接数

#### 5.6.5 日志与审计

- [ ] 日志级别设置为 INFO 或更高
- [ ] 启用安全事件审计日志
- [ ] 验证日志中无敏感信息泄露
- [ ] 配置日志轮转和备份策略

#### 5.6.6 错误处理

- [ ] 实现完善的错误处理机制
- [ ] 认证失败时实现速率限制
- [ ] 错误消息不包含敏感信息
- [ ] 实现健康检查和自动故障转移

#### 5.6.7 监控与告警

- [ ] 配置连接失败告警
- [ ] 配置认证失败告警
- [ ] 配置 TLS 错误告警
- [ ] 建立安全事件响应流程

## 6. 性能

无。

## 7. 兼容性

| Rust 连接器版本 | 主要变化 | TDengine 版本 |
| --- | --- | --- |
| v0.12.3 | 1. 优化了 WebSocket 查询和插入性能。 1. 支持了 VARBINARY 和 GEOMETRY 类型。 | 3.3.0.0 及更高版本 |
| v0.12.0 | WebSocket 支持压缩。 | 3.2.3.0 及更高版本 |
| v0.11.0 | TMQ 功能优化。 | 3.2.0.0 及更高版本 |
| v0.10.0 | WebSocket endpoint 变更。 | 3.1.0.0 及更高版本 |
| v0.9.2 | STMT：WebSocket 下获取 tag_fields、col_fields。 | 3.0.7.0 及更高版本 |
| v0.8.12 | 消息订阅：获取消费进度及按照指定进度开始消费。 | 3.0.5.0 及更高版本 |
| v0.8.0 | 支持无模式写入。 | 3.0.4.0 及更高版本 |
| v0.7.6 | 支持在请求中使用 req_id。 | 3.0.3.0 及更高版本 |
| v0.6.0 | 基础功能。 | 3.0.0.0 及更高版本 |

## 8. 运维

无。

## 9. 使用场景

支持原生连接与 WebSocket 连接，在这两种连接方式下均具备以下功能：
1. 数据查询
2. 数据写入
3. 参数绑定
4. 数据订阅
5. 无模式写入

## 10. 约束和限制

1. Rust 版本要求：仅支持使用 Rust 1.90 及以上版本开发的应用。
2. 版本一致性：当采用原生连接方式时，需安装并部署 TDengine 客户端，同时确保 taosc 驱动版本与 TDengine 数据库版本严格匹配。
3. 事务支持：当前 Rust 连接器不支持事务功能。

## 11. 常见错误和排查

| 错误码 | 错误描述 | 建议措施 |
| --- | --- | --- |
| 0xE000 | DSN 错误 | 检查 DSN 配置是否正确，确保格式和内容无误。 |
| 0xE001 | WebScoket 错误 | 检查 WebSocket 网络是否正常，验证服务可达性。 |
| 0xE002 | 连接关闭 | 检查连接状态，必要时重新创建连接并执行相关指令。 |
| 0xE003 | 发送消息超时 | 建议适当增大超时时间，确保网络稳定性。 |
| 0xE004 | 接收消息超时 | 排查 `taosadapter` 日志 |
| 0xE005 | I/O 错误 | 检查网络或磁盘状态，确保读写通畅。 |
| 0xE006 | 未授权 | 验证权限配置是否正确，检查授权凭据。 |
| 0xE007 | 编解码错误 | 检查数据格式，排查 `taosadapter` 日志 |
| 0xE008 | WebSocket 连接断开 | 检查网络状况，重新建立连接 |
| 0xE009 | TLS 错误 | 检查 TLS 配置 |
| 0xE100 | 对象为空 | 确保传递的对象已正确初始化并非空值。 |
| 0xE101 | TQM 主题追加错误 | 检查 TQM 主题的配置和状态是否正确。 |

其它错误码请参考：[TDengine 错误码](https://docs.taosdata.com/reference/error-code/#tmq)

## 12. 可观测性

提供 Rust 连接器日志，以便进行问题定位与排查工作 。

## 13. 安装和卸载

1. 安装前准备
  确保已安装 Rust 开发环境，且 Rust 版本不低于 1.90。
1. 安装步骤
  在项目目录下运行以下 Cargo 命令：
  ```bash
  cargo add taos
  ```

  或者，手动在 Cargo.toml 文件的 [dependencies] 部分添加以下内容：
  ```toml
  taos = "0.12.3"
  ```

1. 卸载步骤
  在项目目录下运行以下 Cargo 命令：
  ```bash
  cargo rm taos
  ```

  或者，手动删除 Cargo.toml 文件的 [dependencies] 部分的以下内容：
  ```toml
  taos = "0.12.3"
  ```

## 14. 文档

需要在官方文档中添加/修改章节【TDengine Rust Connector】。

## 15. 参考文档

1. TDengine C/C++ Connector：https://docs.taosdata.com/reference/connector/cpp/
2. TDengine taosAdapter 参考手册：https://docs.taosdata.com/reference/components/taosadapter/
3. deadpool 连接池：https://docs.rs/deadpool/0.12.1/deadpool/
4. r2d2 连接池：https://docs.rs/r2d2/0.8.10/r2d2/
5. Rust 异步运行时：https://tokio.rs/tokio/tutorial

## 16. 附录

无。
