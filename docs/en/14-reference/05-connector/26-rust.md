---
toc_max_heading_level: 4
sidebar_label: Rust
title: Rust Client Library
slug: /tdengine-reference/client-libraries/rust
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import Preparation from "../../assets/resources/_preparation.mdx";
import RequestId from "../../assets/resources/_request_id.mdx";

`taos` is the official Rust language connector for TDengine. Rust developers can use it to develop applications that access the TDengine database.

The source code for this Rust connector is hosted on [GitHub](https://github.com/taosdata/taos-connector-rust).

## Rust Version Compatibility

Supports Rust 1.70 and above.

## Supported Platforms

- The platforms supported by the native connection are consistent with those supported by the TDengine client driver.
- WebSocket connection supports all platforms that can run Rust.

## Version History

| Rust Connector Version | Major Changes                                                                                         | TDengine Version   |
| ---------------------- | ----------------------------------------------------------------------------------------------------- | ------------------ |
| v0.12.3                | 1. Optimized WebSocket query and insert performance. <br/> 2. Supported VARBINARY and GEOMETRY types. | 3.3.0.0 and higher |
| v0.12.0                | WebSocket supports compression.                                                                       | 3.2.3.0 and higher |
| v0.11.0                | TMQ feature optimization.                                                                             | 3.2.0.0 and higher |
| v0.10.0                | WebSocket endpoint change.                                                                            | 3.1.0.0 and higher |
| v0.9.2                 | STMT: WebSocket to get tag_fields, col_fields.                                                        | 3.0.7.0 and higher |
| v0.8.12                | Message subscription: get consumption progress and start consuming at a specified progress.           | 3.0.5.0 and higher |
| v0.8.0                 | Supports schema-less writing.                                                                         | 3.0.4.0 and higher |
| v0.7.6                 | Supports using req_id in requests.                                                                    | 3.0.3.0 and higher |
| v0.6.0                 | Basic functionality.                                                                                  | 3.0.0.0 and higher |

## Exceptions and Error Codes

After an error occurs, you can obtain detailed information about the error:

```rust
match conn.exec(sql) {
    Ok(_) => {
        Ok(())
    }
    Err(e) => {
        eprintln!("ERROR: {:?}", e);
        Err(e)
    }
}
```

For specific error codes, please refer to [Error Codes](../../error-codes/)

## Data Type Mapping

TDengine currently supports timestamp, numeric, character, and boolean types, with corresponding Rust type conversions as follows:

| TDengine DataType | Rust DataType     |
| ----------------- | ----------------- |
| TIMESTAMP         | Timestamp         |
| INT               | i32               |
| BIGINT            | i64               |
| FLOAT             | f32               |
| DOUBLE            | f64               |
| SMALLINT          | i16               |
| TINYINT           | i8                |
| BOOL              | bool              |
| BINARY            | Vec\<u8>          |
| NCHAR             | String            |
| JSON              | serde_json::Value |
| VARBINARY         | Bytes             |
| GEOMETRY          | Bytes             |

**Note**: The JSON type is only supported in tags.

## Summary of Example Programs

Please refer to: [rust example](https://github.com/taosdata/TDengine/tree/main/docs/examples/rust)

## Frequently Asked Questions

Please refer to [FAQ](../../../frequently-asked-questions/)

## API Reference

The Rust connector interfaces are divided into synchronous and asynchronous interfaces, where the synchronous interfaces are generally implemented by the asynchronous ones, and the method signatures are basically the same except for the async keyword. For interfaces where the synchronous and asynchronous functionalities are the same, this document only provides explanations for the synchronous interfaces.  
For WebSocket connections and native connections, other than the different DSNs required to establish the connections, there is no difference in calling other interfaces.

### Connection Features

#### DSN

TaosBuilder creates a connection builder through a DSN connection description string.
The basic structure of the DSN description string is as follows:

```text
<driver>[+<protocol>]://[<username>:<password>@][<host1>:<port1>[,...<hostN>:<portN>]][/<database>][?<key1>=<value1>[&...<keyN>=<valueN>]]
|------|------------|---|----------|-----------|-------------------------------------|------------|--------------------------------------|
|driver|   protocol |   | username | password  |  addresses                          |   database |   params                             |
```

The meanings of each part are as follows:

- **driver**: Must specify the driver name so that the connector can choose how to create the connection, supporting the following driver names:
  - **taos**: Use the TDengine connector driver, default is the taos driver.
  - **tmq**: Use TMQ to subscribe to data.
- **protocol**: Explicitly specify how to establish the connection, for example: `taos+ws://localhost:6041` specifies establishing a connection via WebSocket.
  - **http/ws**: Create a connection using WebSocket.
  - **https/wss**: Explicitly enable SSL/TLS connection under WebSocket connection mode.
- **username/password**: Username and password used to create the connection.
- **addresses**: Specifies the server addresses to create a connection. The following configuration methods are supported:
  - Native connection: only supports a single address. When the address is not specified, the default is `localhost:6030`.
    - Example: `taos://localhost:6030` or `taos://` (equivalent to the former).
  - WebSocket connection: supports multiple addresses, separated by commas. When the address is not specified, the default is `localhost:6041`.
    - Example: `ws://host1:6041,host2:6041` or `ws://` (equivalent to `ws://localhost:6041`).
- **database**: Specifies the default database name to connect to, optional parameter.
- **params**:
  - `token`: Authentication for the TDengine TSDB cloud service.
  - `timezone`: Time zone setting, in the format of an IANA time zone name (e.g., `Asia/Shanghai`). The default is the local time zone.
  - `compression`: Whether to enable data compression. The default is `false`.
  - `conn_retries`: Maximum number of retries upon connection failure. The default is 5.
  - `retry_backoff_ms`: Initial wait time (in milliseconds) upon connection failure. The default is 200. This value increases exponentially with consecutive failures until the maximum wait time is reached.
  - `retry_backoff_max_ms`: Maximum wait time (in milliseconds) upon connection failure. The default is 2000.

A complete DSN description string example is as follows: `taos+ws://localhost:6041/test`, indicating using WebSocket (`ws`) mode to connect to the server `localhost` through port `6041`, and specifying the default database as `test`.

#### TaosBuilder

The TaosBuilder struct primarily provides methods for building Taos objects based on DSN, as well as features for checking connections and obtaining the client version number.

- `fn available_params() -> &'static [&'static str]`

  - **Interface Description**: Retrieves a list of available parameters in the DSN.
  - **Return Value**: Returns a reference to a static slice of strings containing the names of available parameters.

- `fn from_dsn<D: IntoDsn>(dsn: D) -> RawResult<Self>`

  - **Interface Description**: Creates a connection using a DSN string without checking the connection.
  - **Parameter Description**:
    - `dsn`: DSN string or a type that can be converted into a DSN.
  - **Return Value**: On success, returns a `RawResult` of its own type; on failure, returns an error.

- `fn client_version() -> &'static str`

  - **Interface Description**: Gets the client version.
  - **Return Value**: Returns a static string of the client version.

- `fn ping(&self, _: &mut Self::Target) -> RawResult<()>`

  - **Interface Description**: Checks if the connection is still alive.
  - **Parameter Description**:
    - `_`: Mutable reference to the target connection.
  - **Return Value**: On success, returns an empty `RawResult`; on failure, returns an error.

- `fn ready(&self) -> bool`

  - **Interface Description**: Checks if it is ready to connect.
  - **Return Value**: Mostly returns `true`, indicating the address is ready for connection.

- `fn build(&self) -> RawResult<Self::Target>`
  - **Interface Description**: Creates a new Taos object from this structure.
  - **Return Value**: On success, returns a `RawResult` of the target connection type; on failure, returns an error.

### Executing SQL

Executing SQL primarily uses the Taos struct, and obtaining the result set and metadata requires the ResultSet struct and Field struct introduced in the next section.

#### Taos

The Taos struct provides multiple database operation APIs, including: executing SQL, schema-less writing, and some common database query encapsulations (such as creating databases, fetching)

- `pub fn is_native(&self) -> bool`

  - **Interface Description**: Determines if the connection uses a native protocol.
  - **Return Value**: Returns `true` if using a native protocol, otherwise returns `false`.

- `pub fn is_ws(&self) -> bool`

  - **Interface Description**: Determines if the connection uses the WebSocket protocol.
  - **Return Value**: Returns `true` if using the WebSocket protocol, otherwise returns `false`.

- `fn query<T: AsRef<str>>(&self, sql: T) -> RawResult<Self::ResultSet>`

  - **Interface Description**: Executes an SQL query.
  - **Parameter Description**:
    - `sql`: The SQL statement to execute.
  - **Return Value**: On success, returns a `RawResult` of the `ResultSet`; on failure, returns an error.

- `fn query_with_req_id<T: AsRef<str>>(&self, sql: T, req_id: u64) -> RawResult<Self::ResultSet>`

  - **Interface Description**: Executes an SQL query with a request ID.
  - **Parameter Description**:
    - `sql`: The SQL statement to execute.
    - `req_id`: Request ID.
  - **Return Value**: On success, returns a `RawResult` of the `ResultSet`; on failure, returns an error.

- `fn exec<T: AsRef<str>>(&self, sql: T) -> RawResult<usize>`

  - **Interface Description**: Executes an SQL statement.
  - **Parameter Description**:
    - `sql`: The SQL statement to execute.
  - **Return Value**: On success, returns the number of affected rows; on failure, returns an error.

- `fn exec_many<T: AsRef<str>, I: IntoIterator<Item = T>>(&self, input: I) -> RawResult<usize>`

  - **Interface Description**: Executes multiple SQL statements in batch.
  - **Parameter Description**:
    - `input`: Collection of SQL statements to execute.
  - **Return Value**: On success, returns the total number of affected rows; on failure, returns an error.

- `fn query_one<T: AsRef<str>, O: DeserializeOwned>(&self, sql: T) -> RawResult<Option<O>>`

  - **Interface Description**: Executes an SQL query and returns a single result.
  - **Parameter Description**:
    - `sql`: The SQL statement to execute.
  - **Return Value**: On success, returns an optional result object; on failure, returns an error.

- `fn server_version(&self) -> RawResult<Cow<str>>`

  - **Interface Description**: Gets the server version.
  - **Return Value**: On success, returns the server version string as a `RawResult`; on failure, returns an error.

- `fn create_topic(&self, name: impl AsRef<str>, sql: impl AsRef<str>) -> RawResult<()>`

  - **Interface Description**: Creates a topic.
  - **Parameter Description**:
    - `name`: The name of the topic.
    - `sql`: The associated SQL statement.
  - **Return Value**: On success, returns an empty `RawResult`; on failure, returns an error.

- `fn databases(&self) -> RawResult<Vec<ShowDatabase>>`

  - **Interface Description**: Retrieves a list of databases.
  - **Return Value**: On success, returns a list of databases as a `RawResult`; on failure, returns an error.

- `fn topics(&self) -> RawResult<Vec<Topic>>`

  - **Interface Description**: Retrieves topic information.
  - **Return Value**: On success, returns a list of topics as a `RawResult`; on failure, returns an error.

- `fn describe(&self, table: &str) -> RawResult<Describe>`

  - **Interface Description**: Describes the table structure.
  - **Parameter Description**:
    - `table`: The name of the table.
  - **Return Value**: On success, returns a description of the table structure as a `RawResult`; on failure, returns an error.

- `fn database_exists(&self, name: &str) -> RawResult<bool>`

  - **Interface Description**: Checks if a database exists.
  - **Parameter Description**:
    - `name`: The name of the database.
  - **Return Value**: On success, returns a boolean `RawResult` indicating whether the database exists; on failure, returns an error.

- `fn put(&self, data: &SmlData) -> RawResult<()>`
  - **Interface Description**: Writes schema-less data, see below for the SmlData structure description.
  - **Parameter Description**:
    - `data`: Schema-less data.
  - **Return Value**: On success, returns an empty `RawResult`; on failure, returns an error.

### SmlData

The SmlData structure provides a data structure for schema-less writing and methods for accessing properties.

- `pub struct SmlData`

  - **Structure Description**: The `SmlData` structure is used to store schema-less data and related information.
  - **Field Description**:
    - `protocol`: Schema-less protocol, supports InfluxDB `Line`, OpenTSDB `Telnet`, OpenTSDB `Json`.
    - `precision`: Timestamp precision, supports `Hours`, `Minutes`, `Seconds`, `Millisecond` (default), `Microsecond`, `Nanosecond`.
    - `data`: List of data.
    - `ttl`: Data time-to-live, in seconds.
    - `req_id`: Request ID.

- `pub fn protocol(&self) -> SchemalessProtocol`

  - **Interface Description**: Gets the schema-less protocol.
  - **Return Value**: Schema-less protocol type, supports InfluxDB `Line`, OpenTSDB `Telnet`, OpenTSDB `Json`.

- `pub fn precision(&self) -> SchemalessPrecision`

  - **Interface Description**: Gets the timestamp precision.
  - **Return Value**: Timestamp precision type, supports `Hours`, `Minutes`, `Seconds`, `Millisecond` (default), `Microsecond`, `Nanosecond`.

- `pub fn data(&self) -> &Vec<String>`

  - **Interface Description**: Retrieves the list of data.
  - **Return Value**: Reference to the list of data.

- `pub fn ttl(&self) -> Option<i32>`

  - **Interface Description**: Get the data time-to-live.
  - **Return Value**: Time-to-live of the data (optional), in seconds.

- `pub fn req_id(&self) -> Option<u64>`
  - **Interface Description**: Get the request ID.
  - **Return Value**: Request ID (optional).

### Result Retrieval

#### ResultSet

The ResultSet structure provides methods for accessing the data and metadata of the result set.

- `fn affected_rows(&self) -> i32`

  - **Interface Description**: Get the number of affected rows.
  - **Return Value**: Number of affected rows, type `i32`.

- `fn precision(&self) -> Precision`

  - **Interface Description**: Get precision information.
  - **Return Value**: Precision information, type `Precision`.

- `fn fields(&self) -> &[Field]`

  - **Interface Description**: Get field information. See the Field structure description below.
  - **Return Value**: Reference to an array of field information.

- `fn summary(&self) -> (usize, usize)`

  - **Interface Description**: Get summary information.
  - **Return Value**: A tuple containing two `usize` types, representing some statistical information.

- `fn num_of_fields(&self) -> usize`

  - **Interface Description**: Get the number of fields.
  - **Return Value**: Number of fields, type `usize`.

- `fn blocks(&mut self) -> IBlockIter<'_, Self>`

  - **Interface Description**: Get an iterator for the raw data blocks.
  - **Return Value**: Iterator for the raw data blocks, type `IBlockIter<'_, Self>`.

- `fn rows(&mut self) -> IRowsIter<'_, Self>`

  - **Interface Description**: Get an iterator for row-wise querying.
  - **Return Value**: Iterator for row-wise querying, type `IRowsIter<'_, Self>`.

- `fn deserialize<T>(&mut self) -> Map<IRowsIter<'_, Self>, fn(_: Result<RowView<'_>, Error>) -> Result<T, Error>>`

  - **Interface Description**: Deserialize row data.
  - **Generic Parameters**:
    - `T`: Target type, must implement `DeserializeOwned`.
  - **Return Value**: Map of the deserialization results, type `Map<IRowsIter<'_, Self>, fn(_: Result<RowView<'_>, Error>) -> Result<T, Error>>`.

- `fn to_rows_vec(&mut self) -> Result<Vec<Vec<Value>>, Error>`
  - **Interface Description**: Convert the result set into a two-dimensional vector of values.
  - **Return Value**: On success, returns a two-dimensional vector of values, on failure returns an error, type `Result<Vec<Vec<Value>>, Error>`.

#### Field

The Field structure provides methods for accessing field information.

- `pub const fn empty() -> Field`

  - **Interface Description**: Create an empty `Field` instance.
  - **Return Value**: Returns an empty `Field` instance.

- `pub fn new(name: impl Into<String>, ty: Ty, bytes: u32) -> Field`

  - **Interface Description**: Create a new `Field` instance.
  - **Parameter Description**:
    - `name`: Field name.
    - `ty`: Field type.
    - `bytes`: Field data length.
  - **Return Value**: Returns a new `Field` instance.

- `pub fn name(&self) -> &str`

  - **Interface Description**: Get the field name.
  - **Return Value**: Returns the field name.

- `pub fn escaped_name(&self) -> String`

  - **Interface Description**: Get the escaped field name.
  - **Return Value**: Returns the escaped field name.

- `pub const fn ty(&self) -> Ty`

  - **Interface Description**: Get the field type.
  - **Return Value**: Returns the field type.

- `pub const fn bytes(&self) -> u32`

  - **Interface Description**: Get the preset length of the field.
  - **Return Value**: For variable-length data types, returns the preset length; for other types, returns the byte width.

- `pub fn to_c_field(&self) -> c_field_t`

  - **Interface Description**: Converts a `Field` instance into a C language structure.
  - **Return Value**: Returns the field represented by a C language structure.

- `pub fn sql_repr(&self) -> String`
  - **Interface Description**: Represents the data type of the field in SQL.
  - **Return Value**: For example: "INT", "VARCHAR(100)" and other SQL data type representations.

### Parameter Binding

Parameter binding functionality is mainly supported by the Stmt structure.

#### Stmt

The Stmt structure provides functionality related to parameter binding, used for efficient writing.

- `fn init(taos: &Q) -> RawResult<Self>`

  - **Interface Description**: Initialize the parameter binding instance.
  - **Parameter Description**:
    - `taos`: Database connection instance.
  - **Return Value**: On success, returns the initialized instance; on failure, returns an error.

- `fn init_with_req_id(taos: &Q, req_id: u64) -> RawResult<Self>`

  - **Interface Description**: Initialize the parameter binding instance using a request ID.
  - **Parameter Description**:
    - `taos`: Database connection instance.
    - `req_id`: Request ID.
  - **Return Value**: On success, returns the initialized instance; on failure, returns an error.

- `fn prepare<S: AsRef<str>>(&mut self, sql: S) -> RawResult<&mut Self>`

  - **Interface Description**: Prepare the SQL statement to be bound.
  - **Parameter Description**:
    - `sql`: SQL statement to prepare.
  - **Return Value**: On success, returns a mutable reference to itself; on failure, returns an error.

- `fn set_tbname<S: AsRef<str>>(&mut self, name: S) -> RawResult<&mut Self>`

  - **Interface Description**: Set the table name.
  - **Parameter Description**:
    - `name`: Table name.
  - **Return Value**: On success, returns a mutable reference to itself; on failure, returns an error.

- `fn set_tags(&mut self, tags: &[Value]) -> RawResult<&mut Self>`

  - **Interface Description**: Set tags.
  - **Parameter Description**:
    - `tags`: Array of tags.
  - **Return Value**: On success, returns a mutable reference to itself; on failure, returns an error.

- `fn set_tbname_tags<S: AsRef<str>>(&mut self, name: S, tags: &[Value]) -> RawResult<&mut Self>`

  - **Interface Description**: Set the table name and tags.
  - **Parameter Description**:
    - `name`: Table name.
    - `tags`: Array of tags.
  - **Return Value**: On success, returns a mutable reference to itself; on failure, returns an error.

- `fn bind(&mut self, params: &[ColumnView]) -> RawResult<&mut Self>`

  - **Interface Description**: Bind parameters.
  - **Parameter Description**:
    - `params`: Array of parameters.
  - **Return Value**: On success, returns a mutable reference to itself; on failure, returns an error.

- `fn add_batch(&mut self) -> RawResult<&mut Self>`

  - **Interface Description**: Add a batch.
  - **Return Value**: On success, returns a mutable reference to itself; on failure, returns an error.

- `fn execute(&mut self) -> RawResult<usize>`

  - **Interface Description**: Execute the statement.
  - **Return Value**: On success, returns the number of affected rows; on failure, returns an error.

- `fn affected_rows(&self) -> usize`
  - **Interface Description**: Get the number of affected rows.
  - **Return Value**: Number of affected rows.

### Data Subscription

Data subscription mainly involves three structures, providing connection establishment with TmqBuilder, consuming data and committing offsets with Consumer, and the Offset structure.

#### TmqBuilder

Similar to TaosBuilder, TmqBuilder provides the functionality to create consumer objects.

- `fn available_params() -> &'static [&'static str]`

  - **Interface Description**: Get the list of available parameters in the DSN.
  - **Return Value**: Returns a reference to a static slice of strings, containing the names of available parameters.

- `fn from_dsn<D: IntoDsn>(dsn: D) -> RawResult<Self>`

  - **Interface Description**: Create a connection using a DSN string, without checking the connection.
  - **Parameter Description**:
    - `dsn`: DSN string or a type that can be converted into DSN.
  - **Return Value**: On success, returns `RawResult` of its own type, on failure returns an error.

- `fn client_version() -> &'static str`

  - **Interface Description**: Get the client version.
  - **Return Value**: Returns a static string of the client version.

- `fn ping(&self, conn: &mut Self::Target) -> RawResult<()>`

  - **Interface Description**: Check if the connection is still alive.
  - **Parameter Description**:
    - `conn`: Mutable reference to the target connection.
  - **Return Value**: On success, returns an empty `RawResult`, on failure returns an error.

- `fn ready(&self) -> bool`

  - **Interface Description**: Check if it is ready to connect.
  - **Return Value**: Mostly returns `true`, indicating that the address is ready to connect.

- `fn build(&self) -> RawResult<Self::Target>`
  - **Interface Description**: Create a new connection from this structure.
  - **Return Value**: On success, returns `RawResult` of the target connection type, on failure returns an error.

#### Consumer

The Consumer structure provides subscription-related functionalities, including subscribing, fetching messages, committing offsets, setting offsets, etc.

- `fn subscribe<T: Into<String>, I: IntoIterator<Item = T> + Send>(&mut self, topics: I) -> RawResult<()>`

  - **Interface Description**: Subscribe to a series of topics.
  - **Parameter Description**:
    - `topics`: List of topics to subscribe to.
  - **Return Value**: On success, returns an empty `RawResult`, on failure returns an error.

- `fn recv_timeout(&self, timeout: Timeout) -> RawResult<Option<(Self::Offset, MessageSet<Self::Meta, Self::Data>)>>`

  - **Interface Description**: Receive messages within a specified timeout period.
  - **Parameter Description**:
    - `timeout`: Timeout period.
  - **Return Value**: On success, returns messages, on failure returns an error.

- `fn commit(&self, offset: Self::Offset) -> RawResult<()>`

  - **Interface Description**: Commit the given offset.
  - **Parameter Description**:
    - `offset`: The offset to commit, see the Offset structure below.
  - **Return Value**: On success, returns an empty `RawResult`, on failure returns an error.

- `fn commit_offset(&self, topic_name: &str, vgroup_id: VGroupId, offset: i64) -> RawResult<()>`

  - **Interface Description**: Commit offset for a specific topic and partition.
  - **Parameter Description**:
    - `topic_name`: Topic name.
    - `vgroup_id`: Partition ID.
    - `offset`: The offset to commit.
  - **Return Value**: On success, returns an empty `RawResult`, on failure returns an error.

- `fn list_topics(&self) -> RawResult<Vec<String>>`

  - **Interface Description**: List all available topics.
  - **Return Value**: On success, returns a list of topics, on failure returns an error.

- `fn assignments(&self) -> Option<Vec<(String, Vec<Assignment>)>>`

  - **Interface Description**: Get the current assignments of topics and partitions.
  - **Return Value**: On success, returns assignment information, on failure returns `None`.

- `fn offset_seek(&mut self, topic: &str, vg_id: VGroupId, offset: i64) -> RawResult<()>`

  - **Interface Description**: Set the offset for a specific topic and partition.
  - **Parameter Description**:
    - `topic`: Topic name.
    - `vg_id`: Partition ID.
    - `offset`: The offset to set.
  - **Return Value**: On success, returns an empty `RawResult`, on failure returns an error.

- `fn committed(&self, topic: &str, vgroup_id: VGroupId) -> RawResult<i64>`

  - **Interface Description**: Get the committed offset for a specific topic and partition.
  - **Parameter Description**:
    - `topic`: Topic name.
    - `vgroup_id`: Partition ID.
  - **Return Value**: Returns the offset on success, or an error on failure.

- `fn position(&self, topic: &str, vgroup_id: VGroupId) -> RawResult<i64>`
  - **Interface Description**: Get the current position for a specific topic and partition.
  - **Parameter Description**:
    - `topic`: Topic name.
    - `vgroup_id`: Partition ID.
  - **Return Value**: Returns the current position on success, or an error on failure.

#### Offset

The Offset structure provides information about the database, topic, and partition to which the current message belongs.

- `fn database(&self) -> &str`

  - **Interface Description**: Get the database name of the current message.
  - **Return Value**: Reference to the database name.

- `fn topic(&self) -> &str`

  - **Interface Description**: Get the topic name of the current message.
  - **Return Value**: Reference to the topic name.

- `fn vgroup_id(&self) -> VGroupId`
  - **Interface Description**: Get the partition ID of the current message.
  - **Return Value**: Partition ID.

## Appendix

- [Rust connector documentation](https://docs.rs/taos)
- [Rust connector project URL](https://github.com/taosdata/taos-connector-rust)
- [deadpool connection pool](https://crates.io/crates/deadpool)
- [r2d2 connection pool](https://crates.io/crates/r2d2)
