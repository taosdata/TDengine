---
toc_max_heading_level: 4
sidebar_label: Python
title: Python Client Library
slug: /tdengine-reference/client-libraries/python
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import RequestId from "../../assets/resources/_request_id.mdx";

`taospy` is the official TDengine python connector, which provides multiple access interfaces for database writing, querying, subscribing, etc.

The installation command is as follows:

``` bash
# Native connection and REST connection
pip3 install taospy

# WebSocket connection, optional installation
pip3 install taos-ws-py
```

The connector code is open sourced and hosted on Github [Taos Connector Python](https://github.com/taosdata/taos-connector-python).

## Connection Methods

`taospy` provides three connection methods, and we recommend using WebSocket connection.

- **Native Connection**, Python connector loads TDengine client driver (libtaos.so/taos.dll), directly connects to TDengine instance, with high performance and fast speed.
 Functionally, it supports functions such as data writing, querying, data subscription, schemaless interface, and parameter binding interface.
- **REST Connection**, The Python connector connects to the TDengine instance through the HTTP interface provided by the taosAdapter, with minimal dependencies and no need to install the TDengine client driver.
 Functionality does not support features such as schemaless and data subscription.
- **WebSocket Connection**, The Python connector connects to the TDengine instance through the WebSocket interface provided by the taosAdapter, which combines the advantages of the first two types of connections, namely high performance and low dependency.
 In terms of functionality, there are slight differences between the WebSocket connection implementation feature set and native connections.

For a detailed introduction of the connection method, please refer to: [Connection Method](../../../developer-guide/connecting-to-tdengine/)

In addition to encapsulating Native and REST interfaces, `taospy` also provides compliance with [the Python Data Access Specification (PEP 249)](https://peps.python.org/pep-0249/) The programming interface.
This makes it easy to integrate `taospy` with many third-party tools, such as [SQLAlchemy](https://www.sqlalchemy.org/) and [pandas](https://pandas.pydata.org/).

The method of establishing a connection directly with the server using the native interface provided by the client driver is referred to as "Native Connection" in the following text;
The method of establishing a connection with the server using the REST interface or WebSocket interface provided by the taosAdapter is referred to as a "REST Connection" or "WebSocket connection" in the following text.

:::note

- If you need to use SQLAlchemy with taos-ws-py, you must install taospy version 2.8.5 or higher.
- For performance-critical applications, it is recommended to adopt the WebSocket connection method for the following reasons:
  - Due to the limitations of Python's Global Interpreter Lock (GIL), multithreading cannot leverage multi-core advantages and essentially executes serially. With native connections, Python-based data conversion and parsing operations are constrained by the GIL, reducing efficiency. In contrast, WebSocket connections release the GIL during I/O operations (e.g., network requests, file I/O), allowing other threads to acquire the lock and execute. This significantly improves throughput in I/O-intensive scenarios.
  - Native connections require extensive data type conversions between C and Python. The WebSocket approach only requires interface-level data conversion, while data processing and parsing are handled by the WebSocket connector (Rust) and taosAdapter (Go). This effectively bypasses Python's performance bottlenecks.
:::

## Python Version Compatibility

Supports Python 3.0 and above.

## Supported Platforms

-The platforms supported by native connections are consistent with those supported by the TDengine client driver.
-WebSocket/REST connections support all platforms that can run Python.

## Version History

Python Connector historical versions (it is recommended to use the latest version of 'taopsy'):

|Python Connector Version | Major Changes                                                                           | TDengine Version|
| --------- | ----------------------------------------------------------------------------------------------------- | ----------------- |
|2.8.6 | Support for pandas' read_Sql_table, to_Sql, and read_Sql interface calls                                    | - |
|2.8.5 | Support the SQLAlchemy feature of taos-ws-py                                                                | - |
|2.8.4 | Support DBUtils connection pool.                                                                            | - |
|2.8.3 | Support BLOB data type.                                                                                     | - |
|2.8.2 | The connection parameter settings support cross-platform compatibility.                                     | - |
|2.8.1 | Add two functions to set the connect property                                                               | - |
|2.8.0 | Remove Apache Superset Driver                                                                               | - |
|2.7.23 | Supports DECIMAL data type                                                                                 | - |
|2.7.22 | Supports Python 3.12 and above                                                                             | - |
|2.7.21 | Supports native STMT2 writing                                                                              | - |
|2.7.19 | Supports Apache Superset connection to TDengine Cloud data source                                          | - |
|2.7.18 | Supports Apache SuperSet BI Tools                                                                          | - |
|2.7.16 | Adds subscription configuration (session.timeout.ms, Max.roll.interval.ms)                                 | - |
|2.7.15 | Supports VARBINARY and GEOMETRY data types                                                                 | - |
|2.7.14 | Fixes known issues                                                                                         | - |
|2.7.13 | Adds tmq synchronous submission offset interface                                                           | - |
|2.7.12 | 1. Adds support for VARBINARY type (STMT currently does not support VARBINARY) <br/> 2 Improves query performance (thanks to contributor [hadrianl](https://github.com/taosdata/taos-connector-python/pull/209) ) | 3.1.1.2 and higher|
|2.7.9  | Data subscription supports obtaining and resetting consumption progress                                    | 3.0.2.6 and higher|
|2.7.8  | Adds 'executioner_many'                                                                                    | 3.0.0.0 and higher|

WebSocket Connector Historical Versions:

|WebSocket Connector Version | Major Changes                                                                                    | TDengine Version|
| ----------------------- | -------------------------------------------------------------------------------------------------- | ----------------- |
|0.6.3 | Support configuring the response timeout for WebSocket connections (excluding data subscription) | - |
|0.6.2 | Fixes memory leaks | - |
|0.6.1 | 1. Support BLOB data type <br/> 2. Support timezone <br/> 3. Support load balancing | - |
|0.5.3 | Support IPv6 address format | - |
|0.5.2 | Upgrade Rust connector to fix dsn token param issue                                                                     | - |
|0.5.1 | Support WebSocket STMT2 writing and querying                                                                            | - |
|0.4.0 | Support dynamic add tmq attribute                                                                                       | - |
|0.3.9 | Fixes the problem of incomplete data retrieval when customizing the number of rows with the "fetchmany" method          | - |
|0.3.8 | Supports connecting SuperSet to the TDengine cloud service instance                                                     | - |
|0.3.5 | Fixes the issues in the crypto provider                                                                                 | - |
|0.3.4 | Supports VARBINARY and GEOMETRY data types                                                                              | 3.3.0.0 and higher |
|0.3.2 | Optimizes WebSocket SQL query and insertion performance, modifies readme and documentation, fixes known issues          | 3.2.3.0 and higher|
|0.2.9 | Fixes known issues                                                                                                      | - |
|0.2.5 | 1. Data subscription supports obtaining and resetting consumption progress <br/>2. Supports schemaless <br/>3. Supports STMT | - |
|0.2.4 | Adds data subscription addition/unsubscribe methods                                                                     | 3.0.5.0 and higher|

## Exception Handling

The Python connector may generate 4 types of exceptions:

- Exceptions from the Python connector itself
- Exceptions from native connection methods
- WebSocket connection exceptions
- Data subscription exceptions
- For other TDengine module errors, please refer to [Error Codes](../../error-codes/)

| Error Type       | Description                                                | Suggested Actions                                            |
| :--------------- | :--------------------------------------------------------- | :----------------------------------------------------------- |
| InterfaceError   | taosc version too low, does not support the used interface | Please check the TDengine client version                     |
| ConnectionError  | Database connection error                                  | Please check the TDengine server status and connection parameters |
| DatabaseError    | Database error                                             | Please check the TDengine server version and upgrade the Python connector to the latest version |
| OperationalError | Operation error                                            | API usage error, please check your code                      |
| ProgrammingError | Interface call error                                       | Please check if the submitted data is correct                |
| StatementError   | stmt related exception                                     | Please check if the binding parameters match the SQL         |
| ResultError      | Operation data error                                       | Please check if the operation data matches the data type in the database |
| SchemalessError  | schemaless related exception                               | Please check the data format and corresponding protocol type are correct |
| TmqError         | tmq related exception                                      | Please check if the Topic and consumer configuration are correct |

In Python, exceptions are usually handled using try-expect. For more on exception handling, refer to [Python Errors and Exceptions Documentation](https://docs.python.org/3/tutorial/errors.html).  
For other TDengine module errors, please refer to [Error Codes](../../error-codes/)

All database operations in the Python Connector, if an exception occurs, will be thrown directly. The application is responsible for handling exceptions. For example:

```python
{{#include docs/examples/python/handle_exception.py}}
```

## Data Type Mapping

TDengine currently supports timestamp, numeric, character, boolean types, and the corresponding Python type conversions are as follows:

| TDengine DataType | Python DataType |
| :---------------- | :-------------- |
| TIMESTAMP         | datetime        |
| INT               | int             |
| BIGINT            | int             |
| FLOAT             | float           |
| DOUBLE            | int             |
| SMALLINT          | int             |
| TINYINT           | int             |
| BOOL              | bool            |
| BINARY            | str             |
| NCHAR             | str             |
| JSON              | str             |
| GEOMETRY          | bytearray       |
| VARBINARY         | bytearray       |
| DECIMAL           | Decimal         |
| BLOB              | bytearray       |
## Example Programs Summary

| Example Program Link                                         | Example Program Content                        |
| ------------------------------------------------------------ | ---------------------------------------------- |
| [bind_multi.py](https://github.com/taosdata/taos-connector-python/blob/main/examples/bind-multi.py) | Parameter binding, bind multiple rows at once  |
| [bind_row.py](https://github.com/taosdata/taos-connector-python/blob/main/examples/bind-row.py) | Parameter binding, bind one row at a time      |
| [insert_lines.py](https://github.com/taosdata/taos-connector-python/blob/main/examples/insert-lines.py) | InfluxDB line protocol writing                 |
| [json_tag.py](https://github.com/taosdata/taos-connector-python/blob/main/examples/json-tag.py) | Using JSON type tags                           |
| [tmq_consumer.py](https://github.com/taosdata/taos-connector-python/blob/main/examples/tmq_consumer.py) | tmq subscription                               |
| [native_all_type_query.py](https://github.com/taosdata/taos-connector-python/blob/main/examples/native_all_type_query.py) | Example supporting all types                   |
| [native_all_type_stmt.py](https://github.com/taosdata/taos-connector-python/blob/main/examples/native_all_type_stmt.py) | Parameter binding example supporting all types |
| [test_stmt2.py](https://github.com/taosdata/taos-connector-python/blob/main/tests/test_stmt2.py) | Example of STMT2 writing                       |
| Example program source code can be found at:                 |                                                |

1. [More native example programs](https://github.com/taosdata/taos-connector-python/tree/main/examples)
2. [More WebSocket example programs](https://github.com/taosdata/taos-connector-python/tree/main/taos-ws-py/examples)

## About Nanosecond (nanosecond)

Due to the current imperfect support for nanoseconds in Python (see the links below), the current implementation returns an integer when nanosecond precision is used, rather than the datetime type returned for ms and us. Application developers need to handle this themselves, and it is recommended to use pandas' to_datetime(). If Python officially fully supports nanoseconds in the future, the Python connector may modify the relevant interfaces.

## Common Questions

Feel free to [ask questions or report issues](https://github.com/taosdata/taos-connector-python/issues).

## API Reference

### WebSocket Connection

#### URL Specification

```text
[+<protocol>]://[<username>:<password>@][<host1>:<port1>[,...<hostN>:<portN>]][/<database>][?<key1>=<value1>[&...<keyN>=<valueN>]]
|-----------|---|----------|-----------|-------------------------------------|------------|--------------------------------------|
|  protocol |   | username | password  |  addresses                          |   database |   params                             |
```

- **protocol**: Specifies the protocol to use. For example, `ws://localhost:6041` establishes a connection using the WebSocket protocol.
  - **ws**: Establishes a connection using the WebSocket protocol.
  - **wss**: Establishes a connection using the WebSocket protocol with SSL/TLS encryption enabled.
- **username/password**: Username and password for the database.
- **addresses**: Specifies the server addresses to create a connection. Multiple addresses are separated by commas. For IPv6 addresses, square brackets must be used (e.g., `[::1]` or `[2001:db8:1234:5678::1]`) to avoid port number parsing conflicts.
  - Example: `ws://host1:6041,host2:6041` or `ws://` (equivalent to `ws://localhost:6041`).
- **database**: Database name.
- **params**:
  - `token`: Authentication for the TDengine TSDB cloud service.
  - `timezone`: Time zone, IANA format (e.g., `Asia/Shanghai`), defaults to the local time zone.
  - `compression`: Whether to enable data compression, defaults to `false`.
  - `conn_retries`: Maximum number of retries when a connection fails, defaults to 5.
  - `retry_backoff_ms`: Initial wait time (milliseconds) when a connection fails, defaults to 200. This value increases exponentially with consecutive failures until the maximum wait time is reached.
  - `retry_backoff_max_ms`: Maximum wait time (milliseconds) when a connection fails, defaults to 2000.
  - `read_timeout`: WebSocket connection response timeout (seconds), excluding data subscription, defaults to 300 (5 minutes).

#### Establishing Connection

- `fn connect(dsn: Option<&str>, args: Option<&PyDict>) -> PyResult<Connection>`
  - **Interface Description**: Establish a taosAdapter connection.
  - **Parameter Description**:
    - `dsn`: Optional parameter, of type `Option<&str>`. Data Source Name (DSN), used to specify the database connection information, including protocol, username, password, host, port, database name and parameters, etc.
    - `args`: Optional parameter, of type `Option<&PyDict>`. Provided as a Python dictionary, where all values ​​are strings. Can be used to configure the following options:
      - `user`: Username for the database
      - `password`: Password for the database
      - `host`: Host address
      - `port`: Port number
      - `database`: Database name
      - `timezone`: Time zone, IANA format (e.g., `Asia/Shanghai`), defaults to the local time zone.
      - `compression`: Whether to enable data compression, defaults to `false`.
      - `conn_retries`: Maximum number of retries when a connection fails, defaults to 5.
      - `retry_backoff_ms`: Initial wait time (milliseconds) when a connection fails, defaults to 200. This value increases exponentially with consecutive failures until the maximum wait time is reached.
      - `retry_backoff_max_ms`: Maximum wait time (milliseconds) when a connection fails, defaults to 2000.
      - `read_timeout`: WebSocket connection response timeout (seconds), excluding data subscription, defaults to 300 (5 minutes).
  - **Return Value**: Connection object.
  - **Exception**: Throws `ConnectionError` exception on operation failure.
- `fn cursor(&self) -> PyResult<Cursor>`
  - **Interface Description**: Creates a new database cursor object for executing SQL commands and queries.
  - **Return Value**: Database cursor object.
  - **Exception**: Throws `ConnectionError` exception on operation failure.

#### Executing SQL

- `fn execute(&self, sql: &str) -> PyResult<i32>`
  - **Interface Description**: Execute an SQL statement.
  - **Parameter Description**:
    - `sql`: SQL statement to be executed.
  - **Return Value**: Number of rows affected.
  - **Exception**: Throws `QueryError` exception on operation failure.
- `fn execute_with_req_id(&self, sql: &str, req_id: u64) -> PyResult<i32>`
  - **Interface Description**: Execute an SQL statement with a req_id.
  - **Parameter Description**:
    - `sql`: SQL statement to be executed.
    - `reqId`: Used for issue tracking.
  - **Return Value**: Number of rows affected.
  - **Exception**: Throws `QueryError` exception on operation failure.
- `fn query(&self, sql: &str) -> PyResult<TaosResult>`
  - **Interface Description**: Query data.
  - **Parameter Description**:
    - `sql`: SQL statement to be executed.
  - **Return Value**: `TaosResult` data set object.
  - **Exception**: Throws `QueryError` exception on operation failure.
- `fn query_with_req_id(&self, sql: &str, req_id: u64) -> PyResult<TaosResult>`
  - **Interface Description**: Query an SQL statement with a req_id.
  - **Parameter Description**:
    - `sql`: SQL statement to be executed.
    - `reqId`: Used for issue tracking.
  - **Return Value**: `TaosResult` data set object.
  - **Exception**: Throws `QueryError` exception on operation failure.

#### Dataset

TaosResult objects can be accessed by iterating over them to retrieve the queried data.

- `fn fields(&self) -> Vec<TaosField>`
  - **Interface Description**: Get the field information of the queried data, including: name, type, and field length.
  - **Return Value**: `Vec<TaosField>` Array of field information.
- `fn field_count(&self) -> i32`
  - **Interface Description**: Get the number of records queried.
  - **Return Value**: `i32` Number of records queried.

#### Schemaless Insert

- `fn schemaless_insert(&self, lines: Vec<String>, protocol: PySchemalessProtocol, precision: PySchemalessPrecision, ttl: i32, req_id: u64) -> PyResult<()>`
  - **Interface Description**: Schemaless insert.
  - **Parameter Description**:
    - `lines`: Array of data to be written, for specific data format refer to `Schemaless Insert`.
    - `protocol`: Protocol type
      - `PySchemalessProtocol::Line`: InfluxDB Line Protocol.
      - `PySchemalessProtocol::Telnet`: OpenTSDB Text Line Protocol.
      - `PySchemalessProtocol::Json`: JSON Protocol format.
    - `precision`: Time precision
      - `PySchemalessPrecision::Hour`: Hour
      - `PySchemalessPrecision::Minute`: Minute
      - `PySchemalessPrecision::Second` Second
      - `PySchemalessPrecision::Millisecond`: Millisecond
      - `PySchemalessPrecision::Microsecond`: Microsecond
      - `PySchemalessPrecision::Nanosecond`: Nanosecond
    - `ttl`: Table expiration time in days.
    - `reqId`: Used for issue tracking.
  - **Exception**: Throws `DataError` or `OperationalError` on failure.

#### Parameter Binding

Since version 0.5.1, an interface for binding parameters of stmt2 has been provided to achieve efficient writing.

- `fn stmt2_statement(&self) -> PyResult<TaosStmt2>`
  - **Interface Description**: Create a stmt2 object using a connection object.
  - **Return Value**: stmt object.
  - **Exception**: Throws `ConnectionError` on failure.
- `fn prepare(&mut self, sql: &str) -> PyResult<()>`
  - **Interface Description**: Bind a precompiled SQL statement.
  - **Parameter Description**:
    - `sql`: Precompiled SQL statement.
  - **Exception**: Throws `ProgrammingError` on failure.
- `fn bind(&mut self, params: Vec<PyStmt2BindParam>) -> PyResult<()>`
  - **Interface Description**: Bind data.
  - **Parameter Description**:
    - `params`: Data to bind.
  - **Exception**: Throws `ProgrammingError` on failure.
- `fn execute(&mut self) -> PyResult<usize>`
  - **Interface Description**: Execute to write all bound data.
  - **Return Value**: Number of entries written.
  - **Exception**: Throws `QueryError` on failure.
- `fn result_set(&mut self) -> PyResult<TaosResult>`
  - **Interface Description**: Get the bind query result set.
  - **Return Value**: `TaosResult` data set object.
  - **Exception**: Throws `QueryError` on failure.
- `fn affect_rows(&mut self) -> PyResult<usize>`
  - **Interface Description**: Get the number of entries written.
  - **Return Value**: Number of entries written.
- `fn close(&self) -> PyResult<()>`
  - **Interface Description**: Close the stmt object.

The interface for binding parameters of the standard Stmt.

- `fn statement(&self) -> PyResult<TaosStmt>`
  - **Interface Description**: Create a stmt2 object using a connection object.
  - **Return Value**: stmt2 object.
  - **Exception**: Throws `ConnectionError` on failure.
- `fn prepare(&mut self, sql: &str) -> PyResult<()>`
  - **Interface Description**: Bind a precompiled SQL statement.
  - **Parameter Description**:
    - `sql`: Precompiled SQL statement.
  - **Exception**: Throws `ProgrammingError` on failure.
- `fn set_tbname(&mut self, table_name: &str) -> PyResult<()>`
  - **Interface Description**: Set the table name where data will be written.
  - **Parameter Description**:
    - `tableName`: Table name, to specify a database, use `db_name.table_name`.
  - **Exception**: Throws `ProgrammingError` on failure.
- `fn set_tags(&mut self, tags: Vec<PyTagView>) -> PyResult<()>`
  - **Interface Description**: Set table Tags data, used for automatic table creation.
  - **Parameter Description**:
    - `paramsArray`: Tags data.
  - **Exception**: Throws `ProgrammingError` on failure.
- `fn bind_param(&mut self, params: Vec<PyColumnView>) -> PyResult<()>`
  - **Interface Description**: Bind data.
  - **Parameter Description**:
    - `paramsArray`: Data to bind.
  - **Exception**: Throws `ProgrammingError` on failure.
- `fn add_batch(&mut self) -> PyResult<()>`
  - **Interface Description**: Submit bound data.
  - **Exception**: Throws `ProgrammingError` on failure.
- `fn execute(&mut self) -> PyResult<usize>`
  - **Interface Description**: Execute to write all bound data.
  - **Return Value**: Number of entries written.
  - **Exception**: Throws `QueryError` on failure.
- `fn affect_rows(&mut self) -> PyResult<usize>`
  - **Interface Description**: Get the number of entries written.
  - **Return Value**: Number of entries written.
- `fn close(&self) -> PyResult<()>`
  - **Interface Description**: Close the stmt2 object.

#### Data Subscription

- **Supported properties list for creating consumers**:
  - host: Host address.
  - port: Port number.
  - group.id: The group it belongs to.
  - client.id: Client ID.
  - td.connect.user: Database username.
  - td.connect.pass: Database password.
  - td.connect.token: Database connection token.
  - auto.offset.reset: Determines the consumption position as either the latest data (latest) or including old data (earliest).
  - enable.auto.commit: Whether to allow automatic commit.
  - auto.commit.interval.ms: Automatic commit interval.

- `fn Consumer(conf: Option<&PyDict>, dsn: Option<&str>) -> PyResult<Self>`
  - **Interface Description** Consumer constructor.
    - `conf`: Type `Option<&PyDict>` optional, provided in the form of a Python dictionary, see the properties list for specific configurations.
    - `dsn`: Type `Option<&str>` optional, Data Source Name (DSN), used to specify the location and authentication information of the database to connect to.
  - **Return Value**: Consumer object.
  - **Exception**: Throws `ConsumerException` on operation failure.
- `fn subscribe(&mut self, topics: &PyList) -> PyResult<()>`
  - **Interface Description** Subscribe to a set of topics.
  - **Parameter Description**:
    - `topics`: List of topics to subscribe to.
  - **Exception**: Throws `ConsumerException` on operation failure.
- `fn unsubscribe(&mut self)`
  - **Interface Description** Unsubscribe.
  - **Exception**: Throws `ConsumerException` on operation failure.
- `fn poll(&mut self, timeout: Option<f64>) -> PyResult<Option<Message>>`
  - **Interface Description** Poll for messages.
  - **Parameter Description**:
    - `timeoutMs`: Indicates the polling timeout in milliseconds.
  - **Return Value**: `Message` data corresponding to each topic.
  - **Exception**: Throws `ConsumerException` on operation failure.
- `fn commit(&mut self, message: &mut Message) -> PyResult<()>`
  - **Interface Description** Commit the offset of the currently processed message.
  - **Parameter Description**:
    - `message`: Type `Message`, the offset of the currently processed message.
  - **Exception**: Throws `ConsumerException` on operation failure.
- `fn assignment(&mut self) -> PyResult<Option<Vec<TopicAssignment>>>`
  - **Interface Description**: Get the specified partitions or all partitions currently assigned to the consumer.
  - **Return Value**: Return type `Vec<TopicAssignment>`, i.e., all partitions currently assigned to the consumer.
  - **Exception**: Throws `ConsumerException` on operation failure.
- `fn seek(&mut self, topic: &str, vg_id: i32, offset: i64) -> PyResult<()>`
  - **Interface Description**: Set the offset of a given partition to a specified position.
  - **Parameter Description**:
    - `topic`: Subscribed topic.
    - `vg_id`: vgroupid.
    - `offset`: The offset to be set.
  - **Exception**: Throws `ConsumerException` on operation failure.
- `fn committed(&mut self, topic: &str, vg_id: i32) -> PyResult<i64>`
  - **Interface Description**: Get the last committed offset of the vgroupid partition of the subscribed topic.
  - **Parameter Description**:
    - `topic`: Subscribed topic.
    - `vg_id`: vgroupid.
  - **Return Value**: `i64`, the last committed offset of the partition.
  - **Exception**: Throws `ConsumerException` on operation failure.
- `fn position(&mut self, topic: &str, vg_id: i32) -> PyResult<i64>`
  - **Interface Description**: Get the current offset of a given partition.
  - **Parameter Description**:
    - `topic`: Subscribed topic.
    - `vg_id`: vgroupid.
  - **Return Value**: `i64`, the last committed offset of the partition.
  - **Exception**: Throws `ConsumerException` on operation failure.
- `fn close(&mut self)`
  - **Interface Description**: Close the tmq connection.
  - **Exception**: Throws `ConsumerException` on operation failure.

### Native Connection

#### Establishing Connection

- `def connect(*args, **kwargs):`
  - **Interface Description**: Establish a connection to taosAdapter.
  - **Parameter Description**:
    - `kwargs`: Provided in the form of a Python dictionary, can be used to set
      - `user`: Username for the database
      - `password`: Password for the database.
      - `host`: Host address
      - `port`: Port number
      - `database`: Database name
      - `timezone`: Time zone
  - **Return Value**: `TaosConnection` connection object.
  - **Exceptions**: Throws `AttributeError` or `ConnectionError` if operation fails.
- `def cursor(self)`
  - **Interface Description**: Creates a new database cursor object for executing SQL commands and queries.
  - **Return Value**: Database cursor object.

#### Executing SQL

- `def execute(self, operation, req_id: Optional[int] = None)`
  - **Interface Description**: Execute an SQL statement.
  - **Parameter Description**:
    - `operation`: SQL statement to be executed.
    - `reqId`: Used for issue tracking.
  - **Return Value**: Number of rows affected.
  - **Exceptions**: Throws `ProgrammingError` if operation fails.
- `def query(self, sql: str, req_id: Optional[int] = None) -> TaosResult`
  - **Interface Description**: Query data.
  - **Parameter Description**:
    - `sql`: SQL statement to be executed.
    - `reqId`: Used for issue tracking.
  - **Return Value**: `TaosResult` data set object.
  - **Exceptions**: Throws `ProgrammingError` if operation fails.

#### Data Set

TaosResult object can be iterated over to retrieve queried data.

- `def fields(&self)`
  - **Interface Description**: Get field information of the queried data, including: name, type, and field length.
  - **Return Value**: `TaosFields` list of field information.
- `def field_count(&self)`
  - **Interface Description**: Get the number of records queried.
  - **Return Value**: Number of records queried.
- `def fetch_all_into_dict(self)`
  - **Interface Description**: Convert all records into dictionaries.
  - **Return Value**: List of dictionaries.

#### Schemaless Insertion

- `def schemaless_insert(&self, lines: List[str], protocol: SmlProtocol, precision: SmlPrecision, req_id: Optional[int] = None, ttl: Optional[int] = None) -> int:`
  - **Interface Description**: Schemaless insertion.
  - **Parameter Description**:
    - `lines`: Array of data to be inserted, specific data format for schemaless can refer to `Schemaless Insertion`.
    - `protocol`: Protocol type
      - `SmlProtocol.LINE_PROTOCOL`: InfluxDB Line Protocol.
      - `SmlProtocol.TELNET_PROTOCOL`: OpenTSDB text line protocol.
      - `SmlProtocol.JSON_PROTOCOL`: JSON protocol format
    - `precision`: Time precision
      - `SmlPrecision.Hour`: Hour
      - `SmlPrecision.Minute`: Minute
      - `SmlPrecision.Second` Second
      - `SmlPrecision.Millisecond`: Millisecond
      - `SmlPrecision.Microsecond`: Microsecond
      - `SmlPrecision.Nanosecond`: Nanosecond
    - `ttl`: Table expiration time in days.
    - `reqId`: Used for issue tracking.
  - **Return Value**: Number of rows affected.
  - **Exceptions**: Throws `SchemalessError` if operation fails.

#### Parameter Binding

- `def statement2(self, sql=None, option=None)`
  - **Interface Description**: Creating an STMT2 object using a connection object
  - **Parameter Description**:
    - `sql`: The bound SQL statement will call the `prepare` function if it is not empty
    - `option` Pass in `TaoStmt2Option` class instance
  - **Return Value**: STMT2 object
  - **Exception**: Throws `ConnectionError` on failure
- `def prepare(self, sql)`
  - **Interface Description**: Bind a precompiled SQL statement
  - **Parameter Description**:
    - `sql`: Precompiled SQL statement
  - **Exception**: Throws `StatementError` on failure
- `def bind_param(self, tbnames, tags, datas)`
  - **Interface Description**: Binding Data as an Independent Array
  - **Parameter Description**:
    - `tbnames`:Bind table name array, data type is list
    - `tags`: Bind tag column value array, data type is list
    - `datas`: Bind data column value array, data type of list
  - **Exception**: Throws `StatementError` on failure
- `def bind_param_with_tables(self, tables)`
  - **Interface Description**: Bind data in an independent table format. Independent tables are organized by table units, with table name, TAG value, and data column attributes in table object
  - **Parameter Description**:
    - `tables`: `BindTable` Independent table object array
  - **Exception**: Throws `StatementError` on failure
- `def execute(self) -> int:`
  - **Interface Description**: Execute to write all bound data
  - **Return Value**: Affects the number of rows
  - **Exception**: Throws `QueryError` on failure
- `def result(self)`
  - **Interface Description**: Get parameter binding query result set
  - **Return Value**: Returns the TaosResult object
- `def close(self)`
  - **Interface Description**: close the STMT2 object

#### Data Subscription

- **Supported properties list for creating consumers**:
  - td.connect.ip: Host address.
  - td.connect.port: Port number.
  - group.id: The group it belongs to.
  - client.id: Client ID.
  - td.connect.user: Database username.
  - td.connect.pass: Database password.
  - td.connect.token: Database connection token.
  - auto.offset.reset: Determines the consumption position as either the latest data (latest) or including old data (earliest).
  - enable.auto.commit: Whether to allow automatic submission.
  - auto.commit.interval.ms: Automatic submission interval.
- `def Consumer(configs)`
  - **Interface Description** Consumer constructor.
    - `configs`: Provided in the form of a Python dictionary, see the properties list for specific configurations.
  - **Return Value**: Consumer object.
  - **Exception**: Throws `TmqError` exception on failure.
- `def subscribe(self, topics)`
  - **Interface Description** Subscribes to a set of topics.
  - **Parameter Description**:
    - `topics`: List of topics to subscribe to.
  - **Exception**: Throws `TmqError` exception on failure.
- `def unsubscribe(self)`
  - **Interface Description** Unsubscribes.
  - **Exception**: Throws `TmqError` exception on failure.
- `def poll(self, timeout: float = 1.0)`
  - **Interface Description** Polls for messages.
  - **Parameter Description**:
    - `timeout`: Polling timeout in milliseconds.
  - **Return Value**: `Message` data for each topic.
  - **Exception**: Throws `TmqError` exception on failure.
- `def commit(self, message: Message = None, offsets: [TopicPartition] = None)`
  - **Interface Description** Submits the offset of the currently processed message.
  - **Parameter Description**:
    - `message`: Type `Message`, the offset of the currently processed message.
    - `offsets`: Type `[TopicPartition]`, submits the offset of a batch of messages.
  - **Exception**: Throws `TmqError` exception on failure.
- `def assignment(self)`
  - **Interface Description**: Gets the specified partition or all partitions currently assigned to the consumer.
  - **Return Value**: Return type `[TopicPartition]`, all partitions currently assigned to the consumer.
  - **Exception**: Throws `TmqError` exception on failure.
- `def seek(self, partition)`
  - **Interface Description**: Sets the offset of the given partition to a specified position.
  - **Parameter Description**:
    - `partition`: Offset to be set.
      - `topic`: Subscribed topic
      - `partition`: Partition
      - `offset`: Offset
  - **Exception**: Throws `TmqError` exception on failure.
- `def committed(self, partitions)`
  - **Interface Description**: Gets the last committed offset of the subscribed topic's partition.
  - **Parameter Description**:
    - `partition`: Offset to be set.
      - `topic`: Subscribed topic
      - `partition`: Partition
  - **Return Value**: `partition`, the last committed offset of the partition.
  - **Exception**: Throws `TmqError` exception on failure.
- `def position(self, partitions)`
  - **Interface Description**: Gets the current offset of the given partition.
  - **Parameter Description**:
    - `partition`: Offset to be set.
      - `topic`: Subscribed topic
      - `partition`: Partition
  - **Return Value**: `partition`, the last committed offset of the partition.
  - **Exception**: Throws `TmqError` exception on failure.
- `def close(self)`
  - **Interface Description**: Closes the tmq connection.
  - **Exception**: Throws `TmqError` exception on failure.

### REST Connection

- `def connect(**kwargs) -> TaosRestConnection`
  - **Interface Description**: Establish a connection to taosAdapter.
  - **Parameter Description**:
    - `kwargs`: Provided as a Python dictionary, can be used to set
      - `user`: Username for the database
      - `password`: Password for the database.
      - `host`: Host address
      - `port`: Port number
      - `database`: Database name
  - **Return Value**: Connection object.
  - **Exception**: Throws `ConnectError` exception if operation fails.
- `def execute(self, sql: str, req_id: Optional[int] = None) -> Optional[int]`
  - **Interface Description**: Execute an SQL statement.
  - **Parameter Description**:
    - `sql`: SQL statement to be executed.
    - `reqId`: Used for issue tracking.
  - **Return Value**: Number of rows affected.
  - **Exception**: Throws `ConnectError` or `HTTPError` exception if operation fails.
- `def query(self, sql: str, req_id: Optional[int] = None) -> Result`
  - **Interface Description**: Query data.
  - **Parameter Description**:
    - `sql`: SQL statement to be executed.
    - `reqId`: Used for issue tracking.
  - **Return Value**: `Result` dataset object.
  - **Exception**: Throws `ConnectError` or `HTTPError` exception if operation fails.
- `RestClient(self, url: str, token: str = None, database: str = None, user: str = "root", password: str = "taosdata", timeout: int = None, convert_timestamp: bool = True, timezone: Union[str, datetime.tzinfo] = None)`
  - **Interface Description**: Establish a taosAdapter connection client.
  - **Parameter Description**:
    - `url`: URL of the taosAdapter REST service.
    - `user`: Username for the database.
    - `password`: Password for the database.
    - `database`: Database name.
    - `timezone`: Time zone.
    - `timeout`: HTTP request timeout in seconds.
    - `convert_timestamp`: Whether to convert timestamps from STR type to datetime type.
    - `timezone`: Time zone.
  - **Return Value**: Connection object.
  - **Exception**: Throws `ConnectError` exception if operation fails.
- `def sql(self, q: str, req_id: Optional[int] = None) -> dict`
  - **Interface Description**: Execute an SQL statement.
  - **Parameter Description**:
    - `sql`: SQL statement to be executed.
    - `reqId`: Used for issue tracking.
  - **Return Value**: Returns a list of dictionaries.
  - **Exception**: Throws `ConnectError` or `HTTPError` exception if operation fails.
