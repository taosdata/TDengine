---
toc_max_heading_level: 4
sidebar_label: C#
title: C# Client Library
slug: /tdengine-reference/client-libraries/csharp
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import RequestId from "../../assets/resources/_request_id.mdx";

`TDengine.Connector` is the C# language connector provided by TDengine. C# developers can use it to develop C# applications that access data in the TDengine cluster.

## .Net Version Compatibility

- Supports .NET Framework 4.6 and above.
- Supports .NET 5.0 and above.

## Supported Platforms

- Native connection supports the same platforms as the TDengine client driver.
- WebSocket connection supports all platforms that can run the .NET runtime.

## Version History

| Connector Version | Major Changes                                                                                                            | TDengine Version   |
|-------------------|--------------------------------------------------------------------------------------------------------------------------|--------------------|
| 3.1.9             | The external interface of stmt remains unchanged; the internal implementation has been refactored into stmt2.            | -                  |
| 3.1.8             | Support connection-level timezone settings and strictly validate the binding types of statements against database types. | -                  |
| 3.1.7             | Support IPv6 connections and DECIMAL data type.                                                                          | 3.3.6.0 and higher |
| 3.1.6             | Optimize WebSocket connection message handling.                                                                          | -                  |
| 3.1.5             | Fix WebSocket encoding error for Chinese character length.                                                               | -                  |
| 3.1.4             | Improved WebSocket query and insert performance.                                                                         | 3.3.2.0 and higher |
| 3.1.3             | Supported WebSocket auto-reconnect.                                                                                      | -                  |
| 3.1.2             | Fixed schemaless resource release.                                                                                       | -                  |
| 3.1.1             | Supported varbinary and geometry types.                                                                                  | -                  |
| 3.1.0             | WebSocket uses a native C# implementation.                                                                               | 3.2.1.0 and higher |

## Exceptions and Error Codes

`TDengine.Connector` will throw exceptions, and applications need to handle these exceptions. The taosc exception type `TDengineError` includes an error code and error message, which applications can use to handle the error.
For error reporting in other TDengine modules, please refer to [Error Codes](../../error-codes/)

For error code information please refer to [Error Codes](../../error-codes/)

## Data Type Mapping

| TDengine DataType | C# Type  |
|-------------------|----------|
| TIMESTAMP         | DateTime |
| TINYINT           | sbyte    |
| SMALLINT          | short    |
| INT               | int      |
| BIGINT            | long     |
| TINYINT UNSIGNED  | byte     |
| SMALLINT UNSIGNED | ushort   |
| INT UNSIGNED      | uint     |
| BIGINT UNSIGNED   | ulong    |
| FLOAT             | float    |
| DOUBLE            | double   |
| BOOL              | bool     |
| BINARY            | byte[]   |
| NCHAR             | string   |
| JSON              | byte[]   |
| VARBINARY         | byte[]   |
| GEOMETRY          | byte[]   |
| DECIMAL           | decimal  |

**Note**: 

- JSON type is only supported in tags.
- The GEOMETRY type is binary data in little endian byte order, conforming to the WKB standard. For more details, please refer to [Data Types](../../sql-manual/data-types/)
For WKB standard, please refer to [Well-Known Binary (WKB)](https://libgeos.org/specifications/wkb/)
- The DECIMAL type in C# is represented using the `decimal` type, which supports high-precision decimal numbers. 
Since C#'s `decimal` type differs from TDengine's DECIMAL type in precision and range,
the C#'s `decimal` has a maximum precision of 29 digits, while TDengine's DECIMAL type supports up to 38 digits of precision.
The following should be noted when using it:
  - When the value does not exceed the range of C#'s `decimal` type, you can use `GetDecimal` or `GetValue` to retrieve it.
  - When the value exceeds the range of C#'s `decimal` type, the `GetDecimal` and `GetValue` methods will throw an `OverflowException`. 
  In such cases, you can use the `GetString` method to obtain the string representation.

## Summary of Example Programs

For the source code of the example programs, please refer to: [Example Programs](https://github.com/taosdata/taos-connector-dotnet/tree/3.0/examples)

## API Reference

### ADO.NET Driver

The `TDengine.Data.Client` interface implements the ADO.NET driver, supporting connections to the TDengine database for data operations.

#### Parameter Specifications

ConnectionStringBuilder uses a key-value pair method to set connection parameters, where the key is the parameter name and the value is the parameter value, separated by semicolons `;`.

For example:

```csharp
"protocol=WebSocket;host=127.0.0.1;port=6041;useSSL=false"
```

##### Native Connection

For example: `"host=127.0.0.1;port=6030;username=root;password=taosdata;protocol=Native;db=test"`

Supported parameters include:

- `host`: Address of the TDengine instance.
- `port`: Port of the TDengine instance.
- `username`: Username for the connection.
- `password`: Password for the connection.
- `protocol`: Connection protocol, options are Native or WebSocket, default is Native.
- `db`: Database to connect to.
- `timezone`: The timezone used for parsing time types in the query result set. Defaults to the local timezone. For format details, see [Timezone Settings](#timezone-settings).
- `connectionTimezone`: Connection-level timezone setting (supported in version 3.1.8 and above), only available for .NET 6+ and supports IANA timezone format exclusively. Cannot be set simultaneously with `timezone`. For details, see [Timezone Settings](#timezone-settings).

##### WebSocket Connection

For example: `"protocol=WebSocket;host=127.0.0.1;port=6041;useSSL=false;enableCompression=true;autoReconnect=true;reconnectIntervalMs=10;reconnectRetryCount=5"`

Supported parameters include:

- `host`: Address of the TDengine instance.
- `port`: Port of the TDengine instance.
- `username`: Username for the connection.
- `password`: Password for the connection.
- `protocol`: Connection protocol, options are Native or WebSocket, default is Native.
- `db`: Database to connect to.
- `timezone`: The timezone used for parsing time types in the query result set. Defaults to the local timezone. For format details, see [Timezone Settings](#timezone-settings).
- `connectionTimezone`: Connection-level timezone setting (supported in version 3.1.8 and above), only available for .NET 6+ and supports IANA timezone format exclusively. Cannot be set simultaneously with `timezone`. For details, see [Timezone Settings](#timezone-settings).
- `connTimeout`: Connection timeout, default is 1 minute.
- `readTimeout`: Read timeout, default is 5 minutes.
- `writeTimeout`: Send timeout, default is 10 seconds.
- `token`: Token for connecting to TDengine cloud.
- `useSSL`: Whether to use SSL connection, default is false.
- `enableCompression`: Whether to enable WebSocket compression, default is false.
- `autoReconnect`: Whether to automatically reconnect, default is false.
- `reconnectRetryCount`: Number of retries for reconnection, default is 3.
- `reconnectIntervalMs`: Interval for reconnection in milliseconds, default is 2000.

#### Timezone Settings

##### Connection-Level Timezone Settings

Starting from version 3.1.8, the C# driver supports connection-level timezone settings. You can specify the connection-level timezone by setting the `connectionTimezone` parameter.

###### Feature Specifications

- Only supported in .NET 6.0 and above
- Only supports IANA timezone formats, e.g., `America/New_York`
- Not supported for native connections on Windows platforms
- Cannot be used when TDengine server or taosAdapter is deployed on Windows
- Cannot be set simultaneously with the `timezone` parameter

###### Functional Impact

This parameter affects the time type resolution for all queries and write operations executed through this connection, as well as the time type resolution of query result sets.

###### Example

When the TDengine server is in UTC timezone and `connectionTimezone=America/New_York` is set:

Execute the write SQL:

`insert into db.tb values('2025-08-08 14:00:00',1)`

- The written time will be `2025-08-08 14:00:00` in New York timezone
- Querying via taos shell will display `2025-08-08 18:00:00.000` (UTC time)

When executing query SQL `select * from db.tb` with `connectionTimezone=America/New_York`, the time types in the query result set will be parsed as New York timezone.

| Method              | Return Type      | Example Output                                                                                                    | Remarks                                                                                       |
|---------------------|------------------|-------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------|
| `GetValue`          | `DateTime`       | `2025-08-08 14:00:00.000`                                                                                         | Represents New York timezone time                                                             |
| `GetDateTime`       | `DateTime`       | `2025-08-08 14:00:00.000`                                                                                         | Same behavior as `GetValue`                                                                   |
| `GetDateTimeOffset` | `DateTimeOffset` | `2025-08-08 14:00:00.000` (yyyy-MM-dd HH:mm:ss.fff) or `2025-08-08 14:00:00.000-04:00` (yyyy-MM-dd HH:mm:ss.fffK) | New method added in 3.1.8                                                                     |
| `GetInt64`          | `long`           | `1754676000000` (millisecond precision)                                                                           | Time precision matches database definition (3.1.8 supports returning timestamps via GetInt64) |

##### Query Result Set Timezone Parsing Settings

The `timezone` parameter sets the timezone used for parsing time types in query result sets, internally using the `TimeZoneInfo.FindSystemTimeZoneById` method to obtain timezone information:

- On Windows systems, `FindSystemTimeZoneById` attempts to match subkey names under the registry branch `HKEY_LOCAL_MACHINE\Software\Microsoft\Windows NT\CurrentVersion\Time Zones`.
- On Linux and macOS, it uses timezone information provided by the [ICU library](https://unicode-org.github.io/icu/userguide/datetime/timezone/).

For example, for New York timezone:
- Windows uses `Eastern Standard Time`
- Linux and macOS use `America/New_York`

For .NET 6.0 and above, you can uniformly use [IANA](https://www.iana.org/time-zones) timezone formats, e.g., `America/New_York`.

##### Parameter Binding Notes

- Since DateTime doesn't contain offset information, the Kind property of the obtained DateTime is `Unspecified`. Using the `ToUniversalTime()` method will treat it as local timezone to get UTC time, resulting in incorrect UTC time. Therefore, **all DateTime objects with Kind property as `Unspecified` are prohibited for parameter binding**.
- Starting from version 3.1.8, binding supports using DateTimeOffset type to write TDengine `TIMESTAMP` type, converting to timestamp using `UtcTicks` for writing.
- Starting from version 3.1.8, binding supports using long type to write TDengine `TIMESTAMP` type, with time precision needing to match the database.

#### Interface Description

The `ConnectionStringBuilder` class provides functionality for parsing connection configuration strings.

- `public ConnectionStringBuilder(string connectionString)`
  - **Interface Description**: Constructor for ConnectionStringBuilder.
  - **Parameter Description**:
    - `connectionString`: Connection configuration string.

### Connection Features

The C# driver supports creating ADO.NET connections, returning objects that support the ADO.NET standard `DbConnection` interface, and also provides the `ITDengineClient` interface, which extends some schema-less write interfaces.

#### Standard Interfaces

The standard interfaces supported by ADO.NET connections are as follows:

- `public TDengineConnection(string connectionString)`
  - **Interface Description**: Constructor for TDengineConnection.
  - **Parameter Description**:
    - `connectionString`: Connection configuration string.
  - **Exception**: Throws `ArgumentException` if the format is incorrect.

- `public void ChangeDatabase(string databaseName)`
  - **Interface Description**: Switch databases.
  - **Parameter Description**:
    - `databaseName`: Database name.
  - **Exception**: Throws `TDengineError` exception if execution fails.

- `public void Close()`
  - **Interface Description**: Close connection.

- `public void Open()`
  - **Interface Description**: Open connection.
  - **Exception**: Throws `TDengineError` exception if opening fails, be aware of potential network issues with WebSocket connections.

- `public string ServerVersion`
  - **Interface Description**: Returns the server version.
  - **Return Value**: Server version string.
  - **Exception**: Throws `TDengineError` exception if execution fails.

- `public string DataSource`
  - **Interface Description**: Returns the data source.
  - **Return Value**: Configuration of the host used to create the connection.

- `public string Database`
  - **Interface Description**: Returns the connected database.
  - **Return Value**: Configuration of the database used to create the connection.

- `public TDengineCommand(TDengineConnection connection)`
  - **Interface Description**: Constructor for TDengineCommand.
  - **Parameter Description**:
    - `connection`: TDengineConnection object.
  - **Exception**: Throws `TDengineError` exception if execution fails.

- `public void Prepare()`
  - **Interface Description**: Checks the connection and command text, and prepares for command execution.
  - **Exception**: Throws `InvalidOperationException` if `open` has not been executed or `CommandText` is not set.

- `public string CommandText`
  - **Interface Description**: Gets or sets the command text.
  - **Return Value**: Command text.

- `public new virtual TDengineParameterCollection Parameters`
  - **Interface Description**: Gets the parameter collection.
  - **Return Value**: TDengineParameterCollection object.

#### Schemaless Writing

- `public static ITDengineClient Open(ConnectionStringBuilder builder)`
  - **Interface Description**: Open connection.
  - **Parameter Description**:
    - `builder`: Connection configuration.
  - **Return Value**: ITDengineClient interface.
  - **Exception**: Throws `TDengineError` exception if opening fails, be aware of potential network issues with WebSocket connections.

- `void SchemalessInsert(string[] lines, TDengineSchemalessProtocol protocol,TDengineSchemalessPrecision precision, int ttl, long reqId)`
  - **Interface Description**: Schemaless writing.
  - **Parameter Description**:
    - `lines`: Array of data lines.
    - `protocol`: Data protocol, supported protocols: `TSDB_SML_LINE_PROTOCOL = 1` `TSDB_SML_TELNET_PROTOCOL = 2` `TSDB_SML_JSON_PROTOCOL = 3`.
    - `precision`: Time precision, supported configurations: `TSDB_SML_TIMESTAMP_NOT_CONFIGURED = 0` `TSDB_SML_TIMESTAMP_HOURS = 1` `TSDB_SML_TIMESTAMP_MINUTES = 2` `TSDB_SML_TIMESTAMP_SECONDS = 3` `TSDB_SML_TIMESTAMP_MILLI_SECONDS = 4` `TSDB_SML_TIMESTAMP_MICRO_SECONDS = 5` `TSDB_SML_TIMESTAMP_NANO_SECONDS = 6`.
    - `ttl`: Data expiration time, 0 means not configured.
    - `reqId`: Request ID.
  - **Exception**: Throws `TDengineError` exception if execution fails.

### Execute SQL

The C# driver provides a `DbCommand` interface that complies with the ADO.NET standard, supporting the following features:

1. **Execute SQL Statements**: Execute static SQL statements and return the resulting object.
2. **Query Execution**: Can execute queries that return a dataset (`SELECT` statements).
3. **Update Execution**: Can execute SQL statements that affect the number of rows, such as `INSERT`, `UPDATE`, `DELETE`, etc.
4. **Get Results**: Can obtain the result set (`ResultSet` object) returned after query execution and iterate through the returned data.
5. **Get Update Count**: For non-query SQL statements, can obtain the number of rows affected after execution.
6. **Close Resources**: Provides methods to close and release database resources.

Additionally, the C# driver also provides an extended interface for request link tracking.

#### Standard Interface

- `public int ExecuteNonQuery()`
  - **Interface Description**: Executes an SQL statement and returns the number of rows affected.
  - **Return Value**: Number of rows affected.
  - **Exception**: Throws `TDengineError` exception on execution failure.

- `public object ExecuteScalar()`
  - **Interface Description**: Executes a query and returns the first column of the first row in the query result.
  - **Return Value**: The first column of the first row in the query result.
  - **Exception**: Throws `TDengineError` exception on execution failure.

- `public DbDataReader ExecuteReader()`
  - **Interface Description**: Executes a query and returns a data reader for the query results.
  - **Return Value**: Data reader for the query results.
  - **Exception**: Throws `TDengineError` exception on execution failure.

- `public void Dispose();`
  - **Interface Description**: Releases resources.

#### Extended Interface

The extended interface is mainly used for request link tracking.

- `IRows Query(string query, long reqId)`
  - **Interface Description**: Executes a query and returns the query results.
  - **Parameter Description**:
    - `query`: Query statement.
    - `reqId`: Request ID.
  - **Return Value**: Query results.
  - **Exception**: Throws `TDengineError` exception on execution failure.

- `long Exec(string query, long reqId)`
  - **Interface Description**: Executes an SQL statement.
  - **Parameter Description**:
    - `query`: SQL statement.
    - `reqId`: Request ID.
  - **Return Value**: Number of rows affected.
  - **Exception**: Throws `TDengineError` exception on execution failure.

### Result Retrieval

The C# driver provides a `DbDataReader` interface that complies with the ADO.NET standard, offering methods to read metadata and data from the result set.

#### Result Set

The `DbDataReader` interface provides the following methods to retrieve the result set:

- `public bool GetBoolean(int ordinal)`
  - **Interface Description**: Gets the boolean value of a specified column.
  - **Parameter Description**:
    - `ordinal`: Column index.
  - **Return Value**: Boolean value.
  - **Exception**: Throws `InvalidCastException` exception if types do not match.

- `public byte GetByte(int ordinal)`
  - **Interface Description**: Gets the byte value of a specified column.
  - **Parameter Description**:
    - `ordinal`: Column index.
  - **Return Value**: Byte value.
  - **Exception**: Throws `InvalidCastException` exception if types do not match.

- `public long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)`
  - **Interface Description**: Gets the byte value of a specified column.
  - **Parameter Description**:
    - `ordinal`: Column index.
    - `dataOffset`: Data offset.
    - `buffer`: Buffer.
    - `bufferOffset`: Buffer offset.
    - `length`: Length.
  - **Return Value**: Byte value.
  - **Exception**: Throws `InvalidCastException` exception if types do not match.

- `public char GetChar(int ordinal)`
  - **Interface Description**: Gets the character value of the specified column.
  - **Parameter Description**:
    - `ordinal`: Column index.
  - **Return Value**: Character value.
  - **Exception**: Throws `InvalidCastException` if the type does not correspond.

- `public long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)`
  - **Interface Description**: Gets the character value of the specified column.
  - **Parameter Description**:
    - `ordinal`: Column index.
    - `dataOffset`: Data offset.
    - `buffer`: Buffer.
    - `bufferOffset`: Buffer offset.
    - `length`: Length.
  - **Return Value**: Character value.
  - **Exception**: Throws `InvalidCastException` if the type does not correspond.

- `public DateTime GetDateTime(int ordinal)`
  - **Interface Description**: Gets the date and time value of the specified column.
  - **Parameter Description**:
    - `ordinal`: Column index.
  - **Return Value**: Date and time value.
  - **Exception**: Throws `InvalidCastException` if the type does not correspond.

- `public decimal GetDecimal(int ordinal)`
  - **Interface Description**: Gets the decimal value of the specified column.
  - **Parameter Description**:
    - `ordinal`: Column index.
  - **Return Value**: Decimal value.
  - **Exception**: 
    - Throws `InvalidCastException` if the type does not correspond.
    - Throws `OverflowException` if the value exceeds the range of C#'s `decimal` type.

- `public double GetDouble(int ordinal)`
  - **Interface Description**: Gets the double precision floating-point value of the specified column.
  - **Parameter Description**:
    - `ordinal`: Column index.
  - **Return Value**: Double precision floating-point value.
  - **Exception**: Throws `InvalidCastException` if the type does not correspond.

- `public float GetFloat(int ordinal)`
  - **Interface Description**: Gets the single precision floating-point value of the specified column.
  - **Parameter Description**:
    - `ordinal`: Column index.
  - **Return Value**: Single precision floating-point value.
  - **Exception**: Throws `InvalidCastException` if the type does not correspond.

- `public short GetInt16(int ordinal)`
  - **Interface Description**: Gets the 16-bit integer value of the specified column.
  - **Parameter Description**:
    - `ordinal`: Column index.
  - **Return Value**: 16-bit integer value.
  - **Exception**: Throws `InvalidCastException` if the type does not correspond.

- `public int GetInt32(int ordinal)`
  - **Interface Description**: Gets the 32-bit integer value of the specified column.
  - **Parameter Description**:
    - `ordinal`: Column index.
  - **Return Value**: 32-bit integer value.
  - **Exception**: Throws `InvalidCastException` if the type does not correspond.

- `public long GetInt64(int ordinal)`
  - **Interface Description**: Gets the 64-bit integer value of the specified column.
  - **Parameter Description**:
    - `ordinal`: Column index.
  - **Return Value**: 64-bit integer value.
  - **Exception**: Throws `InvalidCastException` if the type does not correspond.

- `public string GetString(int ordinal)`
  - **Interface Description**: Gets the string value of the specified column.
  - **Parameter Description**:
    - `ordinal`: Column index.
  - **Return Value**: String value.
  - **Exception**: Throws `InvalidCastException` if the type does not correspond.

- `public object GetValue(int ordinal)`
  - **Interface Description**: Gets the value of the specified column.
  - **Parameter Description**:
    - `ordinal`: Column index.
  - **Return Value**: Result object.

- `public int GetValues(object[] values)`
  - **Interface Description**: Gets the values of all columns.
  - **Parameter Description**:
    - `values`: Array of values.
  - **Return Value**: Number of values.

- `public bool IsDBNull(int ordinal)`
  - **Interface Description**: Determines if the specified column is NULL.
  - **Parameter Description**:
    - `ordinal`: Column index.
  - **Return Value**: Whether it is NULL.

- `public int RecordsAffected`
  - **Interface Description**: Gets the number of affected rows.
  - **Return Value**: Number of affected rows.

- `public bool HasRows`
  - **Interface Description**: Whether the result has row data.
  - **Return Value**: Whether the result has row data.

- `public bool Read()`
  - **Interface Description**: Reads the next row.
  - **Return Value**: Whether the read was successful.

- `public IEnumerator GetEnumerator()`
  - **Interface Description**: Gets the enumerator.
  - **Return Value**: Enumerator.

- `public void Close()`
  - **Interface Description**: Closes the result set.

#### Result Set Metadata

The `DbDataReader` interface provides the following methods to obtain result set metadata:

- `public DataTable GetSchemaTable()`
  - **Interface Description**: Gets the result set metadata.
  - **Return Value**: Result set metadata.

- `public string GetDataTypeName(int ordinal)`
  - **Interface Description**: Gets the data type name of the specified column.
  - **Parameter Description**:
    - `ordinal`: Column index.
  - **Return Value**: Data type name.

- `public Type GetFieldType(int ordinal)`
  - **Interface Description**: Gets the data type of the specified column.
  - **Parameter Description**:
    - `ordinal`: Column index.
  - **Return Value**: Data type.

- `public string GetName(int ordinal)`
  - **Interface Description**: Gets the name of the specified column.
  - **Parameter Description**:
    - `ordinal`: Column index.
  - **Return Value**: Column name.

- `public int GetFieldSize(int ordinal)`
  - **Interface Description**: Gets the size of the specified column.
  - **Parameter Description**:
    - `ordinal`: Column index.
  - **Return Value**: Column size.

- `public int GetOrdinal(string name)`
  - **Interface Description**: Gets the index of the specified column.
  - **Parameter Description**:
    - `name`: Column name.
  - **Return Value**: Column index.

- `public int FieldCount`
  - **Interface Description**: Gets the number of columns.
  - **Return Value**: Number of columns.

### Parameter Binding

The `TDengineCommand` class supports parameter binding.

#### Standard Interface

The `TDengineCommand` class inherits the `DbCommand` interface, supporting the following features:

- `public string CommandText`
  - **Interface Description**: Gets or sets the command text, supports parameter binding.
  - **Return Value**: Command text.

- `public new virtual TDengineParameterCollection Parameters`
  - **Interface Description**: Gets the parameter collection.
  - **Return Value**: `TDengineParameterCollection` object.

#### Parameter Metadata

The `TDengineParameterCollection` inherits the `DbParameterCollection` interface, supporting the following features:

- `public int Add(object value)`
  - **Interface Description**: Adds a parameter.
  - **Parameter Description**:
    - `value`: Parameter value.
  - **Return Value**: Parameter index.

- `public void Clear()`
  - **Interface Description**: Clears parameters.

- `public bool Contains(object value)`
  - **Interface Description**: Whether it contains a parameter.
  - **Parameter Description**:
    - `value`: Parameter value.
  - **Return Value**: Whether it contains the parameter.

- `public int IndexOf(object value)`
  - **Interface Description**: Get the parameter index.
  - **Parameter Description**:
    - `value`: Parameter value.
  - **Return Value**: Parameter index.

- `public void Insert(int index, object value)`
  - **Interface Description**: Insert a parameter.
  - **Parameter Description**:
    - `index`: Index.
    - `value`: Parameter value.

- `public void Remove(object value)`
  - **Interface Description**: Remove a parameter.
  - **Parameter Description**:
    - `value`: Parameter value.

- `public void RemoveAt(int index)`
  - **Interface Description**: Remove a parameter.
  - **Parameter Description**:
    - `index`: Index.

- `public void RemoveAt(string parameterName)`
  - **Interface Description**: Remove a parameter.
  - **Parameter Description**:
    - `parameterName`: Parameter name.

- `public int Count`
  - **Interface Description**: Get the count of parameters.
  - **Return Value**: Number of parameters.

- `public int IndexOf(string parameterName)`
  - **Interface Description**: Get the parameter index.
  - **Parameter Description**:
    - `parameterName`: Parameter name.
  - **Return Value**: Parameter index.

- `public bool Contains(string value)`
  - **Interface Description**: Check if a parameter is included.
  - **Parameter Description**:
    - `value`: Parameter name.
  - **Return Value**: Whether the parameter is included.

- `public void CopyTo(Array array, int index)`
  - **Interface Description**: Copy parameters.
  - **Parameter Description**:
    - `array`: Target array.
    - `index`: Index.

- `public IEnumerator GetEnumerator()`
  - **Interface Description**: Get an enumerator.
  - **Return Value**: Enumerator.

- `public void AddRange(Array values)`
  - **Interface Description**: Add parameters.
  - **Parameter Description**:
    - `values`: Array of parameters.

`TDengineParameter` inherits from the `DbParameter` interface, supporting the following functionalities:

- `public TDengineParameter(string name, object value)`
  - **Interface Description**: TDengineParameter constructor.
  - **Parameter Description**:
    - `name`: Parameter name, must start with @, such as @0, @1, @2, etc.
    - `value`: Parameter value, must correspond one-to-one with C# column types and TDengine column types.

- `public string ParameterName`
  - **Interface Description**: Get or set the parameter name.
  - **Return Value**: Parameter name.

- `public object Value`
  - **Interface Description**: Get or set the parameter value.
  - **Return Value**: Parameter value.

#### Extended Interface

`ITDengineClient` interface provides an extended parameter binding interface.

- `IStmt StmtInit(long reqId)`
  - **Interface Description**: Initialize a statement object.
  - **Parameter Description**:
    - `reqId`: Request ID.
  - **Return Value**: Object implementing the IStmt interface.
  - **Exception**: Throws `TDengineError` exception on failure.

`IStmt` interface provides an extended parameter binding interface.

- `void Prepare(string query)`
  - **Interface Description**: Prepare a statement.
  - **Parameter Description**:
    - `query`: Query statement.
  - **Exception**: Throws `TDengineError` exception on failure.

- `bool IsInsert()`
  - **Interface Description**: Determines whether it is an insert statement.
  - **Return Value**: Whether it is an insert statement.
  - **Exception**: Throws `TDengineError` exception on execution failure.

- `void SetTableName(string tableName)`
  - **Interface Description**: Sets the table name.
  - **Parameter Description**:
    - `tableName`: Table name.
  - **Exception**: Throws `TDengineError` exception on execution failure.

- `void SetTags(object[] tags)`
  - **Interface Description**: Sets tags.
  - **Parameter Description**:
    - `tags`: Array of tags.
  - **Exception**: Throws `TDengineError` exception on execution failure.

- `TaosFieldE[] GetTagFields()`
  - **Interface Description**: Gets tag attributes.
  - **Return Value**: Array of tag attributes.
  - **Exception**: Throws `TDengineError` exception on execution failure.

- `TaosFieldE[] GetColFields()`
  - **Interface Description**: Gets column attributes.
  - **Return Value**: Array of column attributes.
  - **Exception**: Throws `TDengineError` exception on execution failure.

- `void BindRow(object[] row)`
  - **Interface Description**: Binds a row.
  - **Parameter Description**:
    - `row`: Array of row data.
  - **Exception**: Throws `TDengineError` exception on execution failure.

- `void BindColumn(TaosFieldE[] fields, params Array[] arrays)`
  - **Interface Description**: Binds all columns.
  - **Parameter Description**:
    - `fields`: Array of field attributes.
    - `arrays`: Arrays of multiple column data.
  - **Exception**: Throws `TDengineError` exception on execution failure.

- `void AddBatch()`
  - **Interface Description**: Adds a batch.
  - **Exception**: Throws `TDengineError` exception on execution failure.

- `void Exec()`
  - **Interface Description**: Executes parameter binding.
  - **Exception**: Throws `TDengineError` exception on execution failure.

- `long Affected()`
  - **Interface Description**: Gets the number of affected rows.
  - **Return Value**: Number of affected rows.
  - **Exception**: Throws `TDengineError` exception on execution failure.

- `IRows Result()`
  - **Interface Description**: Gets the result.
  - **Return Value**: Result object.
  - **Exception**: Throws `TDengineError` exception on execution failure.

### Data Subscription

The `ConsumerBuilder` class provides interfaces related to consumer building, the `ConsumeResult` class provides interfaces related to consumption results, and the `TopicPartitionOffset` class provides interfaces related to partition offsets. `ReferenceDeserializer` and `DictionaryDeserializer` provide support for deserialization.

#### Consumer

- `public ConsumerBuilder(IEnumerable<KeyValuePair<string, string>> config)`
  - **Interface Description**: ConsumerBuilder constructor.
  - **Parameter Description**:
    - `config`: Consumption configuration.

Supported properties for creating consumers:

- `useSSL`: Whether to use SSL connection, default is false.
- `token`: Token for connecting to TDengine cloud.
- `ws.message.enableCompression`: Whether to enable WebSocket compression, default is false.
- `ws.autoReconnect`: Whether to automatically reconnect, default is false.
- `ws.reconnect.retry.count`: Number of reconnection attempts, default is 3.
- `ws.reconnect.interval.ms`: Reconnection interval in milliseconds, default is 2000.
- `connectionTimezone`: Connection-level timezone setting (supported in version 3.1.8 and above, currently only for timezone functionality when parsing result sets). Only available for .NET 6+ and supports IANA timezone format exclusively. For details, see [Timezone Settings](#timezone-settings).

For other parameters, please refer to: [Consumer Parameter List](../../../developer-guide/manage-consumers/), note that the default value of auto.offset.reset in message subscription has changed starting from TDengine server version 3.2.0.0.

- `public IConsumer<TValue> Build()`
  - **Interface Description**: Build the consumer.
  - **Return Value**: Consumer object.

`IConsumer` interface provides the following consumer-related APIs:

- `ConsumeResult<TValue> Consume(int millisecondsTimeout)`
  - **Interface Description**: Consume messages.
  - **Parameter Description**:
    - `millisecondsTimeout`: Timeout in milliseconds.
  - **Return Value**: Consumption result.
  - **Exception**: Throws `TDengineError` exception on failure.

- `List<TopicPartition> Assignment { get; }`
  - **Interface Description**: Get assignment information.
  - **Return Value**: Assignment information.
  - **Exception**: Throws `TDengineError` exception on failure.

- `List<string> Subscription()`
  - **Interface Description**: Get subscribed topics.
  - **Return Value**: List of topics.
  - **Exception**: Throws `TDengineError` exception on failure.

- `void Subscribe(IEnumerable<string> topic)`
  - **Interface Description**: Subscribe to a list of topics.
  - **Parameter Description**:
    - `topic`: List of topics.
  - **Exception**: Throws `TDengineError` exception on failure.

- `void Subscribe(string topic)`
  - **Interface Description**: Subscribe to a single topic.
  - **Parameter Description**:
    - `topic`: Topic.
  - **Exception**: Throws `TDengineError` exception on failure.

- `void Unsubscribe()`
  - **Interface Description**: Unsubscribe.
  - **Exception**: Throws `TDengineError` exception on failure.

- `void Commit(ConsumeResult<TValue> consumerResult)`
  - **Interface Description**: Commit consumption result.
  - **Parameter Description**:
    - `consumerResult`: Consumption result.
  - **Exception**: Throws `TDengineError` exception on failure.

- `List<TopicPartitionOffset> Commit()`
  - **Interface Description**: Commit all consumption results.
  - **Return Value**: Partition offsets.
  - **Exception**: Throws `TDengineError` exception on failure.

- `void Commit(IEnumerable<TopicPartitionOffset> offsets)`
  - **Interface Description**: Commit consumption results.
  - **Parameter Description**:
    - `offsets`: Partition offsets.
  - **Exception**: Throws `TDengineError` exception on failure.

- `void Seek(TopicPartitionOffset tpo)`
  - **Interface Description**: Jump to partition offset.
  - **Parameter Description**:
    - `tpo`: Partition offset.
  - **Exception**: Throws `TDengineError` exception on failure.

- `List<TopicPartitionOffset> Committed(TimeSpan timeout)`
  - **Interface Description**: Get partition offsets.
  - **Parameter Description**:
    - `timeout`: Timeout (unused).
  - **Return Value**: Partition offsets.
  - **Exception**: Throws `TDengineError` exception on failure.

- `List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout)`
  - **Interface Description**: Get specified partition offsets.
  - **Parameter Description**:
    - `partitions`: List of partitions.
    - `timeout`: Timeout (unused).
  - **Return Value**: Partition offsets.
  - **Exception**: Throws `TDengineError` exception on failure.

- `Offset Position(TopicPartition partition)`
  - **Interface Description**: Get the consumption position.
  - **Parameter Description**:
    - `partition`: Partition.
  - **Return Value**: Offset.
  - **Exception**: Throws `TDengineError` exception if execution fails.

- `void Close()`
  - **Interface Description**: Close the consumer.

#### Consumption Record

`ConsumeResult` class provides interfaces related to consumption results:

- `public List<TmqMessage<TValue>> Message`
  - **Interface Description**: Get the list of messages.
  - **Return Value**: List of messages.

`TmqMessage` class provides the specific content of messages:

```csharp
    public class TmqMessage<TValue>
    {
        public string TableName { get; set; }
        public TValue Value { get; set; }
    }
```

- `TableName`: Table name
- `Value`: Message content

#### Partition Information

Get `TopicPartitionOffset` from `ConsumeResult`:

```csharp
public TopicPartitionOffset TopicPartitionOffset
```

`TopicPartitionOffset` class provides interfaces for getting partition information:

- `public string Topic { get; }`
  - **Interface Description**: Get the topic.
  - **Return Value**: Topic.

- `public Partition Partition { get; }`
  - **Interface Description**: Get the partition.
  - **Return Value**: Partition.

- `public Offset Offset { get; }`
  - **Interface Description**: Get the offset.
  - **Return Value**: Offset.

- `public TopicPartition TopicPartition`
  - **Interface Description**: Get the topic partition.
  - **Return Value**: Topic partition.

- `public string ToString()`
  - **Interface Description**: Convert to string.
  - **Return Value**: String information.

#### Offset Metadata

`Offset` class provides interfaces related to offset:

- `public long Value`
  - **Interface Description**: Get the offset value.
  - **Return Value**: Offset value.

#### Deserialization

The C# driver provides two deserialization classes: `ReferenceDeserializer` and `DictionaryDeserializer`. Both implement the `IDeserializer` interface.

ReferenceDeserializer is used to deserialize a consumed record into an object, ensuring that the object class's property names correspond to the consumed data's column names and that the types match.

DictionaryDeserializer will deserialize a consumed row of data into a `Dictionary<string, object>` object, where the key is the column name and the value is the object.

Interfaces of ReferenceDeserializer and DictionaryDeserializer are not directly called by users, please refer to the usage examples.

## Appendix

[More example programs](https://github.com/taosdata/taos-connector-dotnet/tree/3.0/examples).
