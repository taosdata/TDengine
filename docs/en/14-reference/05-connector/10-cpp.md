---
toc_max_heading_level: 4
sidebar_label: C/C++
title: C/C++ Client Library
slug: /tdengine-reference/client-libraries/cpp
---

C/C++ developers can use the TDengine client driver (i.e., C/C++ connector) to develop their own applications to connect to the TDengine cluster to complete data storage, query, and other functions. The API of the TDengine client driver is similar to the C API of MySQL. When using the application, it is necessary to include the TDengine header file, which lists the function prototypes of the provided API; the application must also link to the corresponding dynamic library on the platform.

## Connection Method

The TDengine client driver provides the taos dynamic library, which supports two connection methods: WebSocket connection and native connection. The difference between the two connection methods is that WebSocket connection does not require the client and server versions to match completely, while native connection requires version matching; in terms of performance, the WebSocket connection method is close to the native connection. **It is generally recommended to use the WebSocket connection method.**

### Header Files and Dynamic Libraries

Regardless of the connection method used, you need to import the `taos.h` header file and link the `taos` dynamic library:

```c
#include "taos.h"
```

After installing the TDengine client or server, the `taos.h` header file is located at:

- **Linux**: `/usr/local/taos/include`
- **Windows**: `C:\TDengine\include`
- **macOS**: `/usr/local/include`

The dynamic library of the TDengine client driver is located at:

- **Linux**: `/usr/local/taos/driver/libtaos.so`
- **Windows**: `C:\TDengine\driver\taos.dll`
- **macOS**: `/usr/local/lib/libtaos.dylib`

### Connection Method Example

TDengine client driver supports two connection methods, developers can flexibly choose according to needs.

Native connection is the default connection method of TDengine. You can directly call `taos_connect()` to establish a connection:

```c
// Native connection example
TAOS *taos = taos_connect(ip, user, password, database, port);
```

WebSocket connection requires setting the driver type first, and then calling `taos_connect()`:

```c
// WebSocket connection example
taos_options(TSDB_OPTION_DRIVER, "websocket");
TAOS *taos = taos_connect(ip, user, password, database, port);
```

:::warning Important Notes
`taos_options(TSDB_OPTION_DRIVER, arg)` **must be called at the beginning of the program to set the driver type, and can only be called once**. Once set, the configuration is valid for the entire program life cycle and cannot be changed.
:::

## Supported Platforms

TDengine client driver supports multiple platforms. For a list of supported platforms, please refer to: [Supported Platforms List](../#supported-platforms)

## Version Description

### WebSocket Connection

| TDengine Client Version | Major Changes                                                                                                   | TDengine Version   |
| ----------------------- | --------------------------------------------------------------------------------------------------------------- | ------------------ |
| 3.3.6.0                 | Provides comprehensive support for SQL execution, parameter binding, schemaless writing, and data subscription. | 3.3.2.0 and higher |

### Native Connection

The version number of the TDengine client driver strictly corresponds to the version number of the TDengine server. **It is strongly recommended to use the client driver with the same version as the TDengine server.** Although the lower version of the client driver can be compatible with the higher version of the server when the first three segments of the version number are the same (that is, only the fourth segment of the version number is different), this is not recommended. **It is strongly not recommended to use the higher version of the client driver to access the lower version of the server.**

## Error Codes

Please refer to: [Error Codes](../../error-codes/).

## Example Program

This section shows the example code of common access methods of TDengine cluster using client driver.

### WebSocket Connection Example

- Synchronous query example: [Synchronous Query](https://github.com/taosdata/TDengine/tree/main/docs/examples/c-ws-new/query_data_demo.c)

- Asynchronous query example: [Asynchronous Query](https://github.com/taosdata/TDengine/tree/main/docs/examples/c-ws-new/async_demo.c)

- Parameter binding example: [Parameter Binding](https://github.com/taosdata/TDengine/tree/main/docs/examples/c-ws-new/stmt2_insert_demo.c)

- Schemaless write example: [Schemaless Write](https://github.com/taosdata/TDengine/tree/main/docs/examples/c-ws-new/sml_insert_demo.c)

- Subscription and consumption example: [Subscription and Consumption](https://github.com/taosdata/TDengine/tree/main/docs/examples/c-ws-new/tmq_demo.c)

:::info
For more example codes and downloads, please see [GitHub](https://github.com/taosdata/TDengine/tree/main/docs/examples/c-ws-new).
:::

### Native Connection Example

- Synchronous query example: [Synchronous Query](https://github.com/taosdata/TDengine/tree/main/docs/examples/c/demo.c)

- Asynchronous query example: [Asynchronous Query](https://github.com/taosdata/TDengine/tree/main/docs/examples/c/asyncdemo.c)

- Parameter binding example: [Parameter Binding](https://github.com/taosdata/TDengine/tree/main/docs/examples/c/prepare.c)

- Schemaless write example: [Schemaless Write](https://github.com/taosdata/TDengine/tree/main/docs/examples/c/schemaless.c)

- Subscription and consumption example: [Subscription and Consumption](https://github.com/taosdata/TDengine/tree/main/docs/examples/c/tmq.c)

:::info
For more example codes and downloads, please see [GitHub](https://github.com/taosdata/TDengine/tree/main/docs/examples/c).
:::

## API Reference

The following introduces the basic API, synchronous query API, asynchronous query API, parameter binding API, schemaless write API and data subscription API of TDengine client driver respectively.

:::info **Connection method compatibility description**
TDengine client driver supports WebSocket connection and native connection. Most APIs have the same functions in both connection methods, but a few APIs have functional differences:

**Native connection**: All APIs provide full functional support.

**WebSocket connection**: Most APIs are fully functional, and a few APIs only return a success status but do not perform actual operations.

**Usage**:

- **Native connection**: No additional configuration is required, just call the API directly, this is the default connection method.
- **WebSocket connection**: You need to call `taos_options(TSDB_OPTION_DRIVER, "websocket")` to set the driver type first, and then call other APIs.

**WebSocket connection function difference description:**

| API                     | Support Status      | Interface Description             | Usage Restrictions                                                             |
| ----------------------- | ------------------- | --------------------------------- | ------------------------------------------------------------------------------ |
| taos_connect_auth       | Not supported       | MD5 encrypted password connection | Only returns a success status, no actual operation is performed                |
| taos_set_notify_cb      | Not supported       | Set an event callback function    | Only returns a success status, no actual operation is performed                |
| tmq_get_connect         | Not supported       | Get a TMQ connection handle       | Only returns a success status, no actual operation is performed                |
| taos_options_connection | Partially supported | Set client connection options     | Character set settings are not supported, and the UTF-8 character set is fixed |

These APIs are fully functional in native connection mode. If you need to use the above functions, it is recommended to choose native connection mode. Future versions will gradually improve the functional support of WebSocket connection.

**Note**: WebSocket connection requires calling `taos_options(TSDB_OPTION_DRIVER, "websocket")` to set the driver type at the beginning of the program, and it can only be called once. Once set, the configuration is valid for the entire program life cycle and cannot be changed.
:::

### Basic API

The basic API is used to establish database connections and provide a runtime environment for other APIs.

- `int taos_init()`

  - **Interface Description**: Initializes the runtime environment. If this API is not actively called, the driver will automatically call it when `taos_connect()` is invoked, so it is generally not necessary to call it manually.
  - **Return Value**: `0`: Success, non-`0`: Failure, you can call the function taos_errstr(NULL) for more detailed error information.

- `void taos_cleanup()`

  - **Interface Description**: Cleans up the runtime environment, should be called before the application exits.

- `int taos_options(TSDB_OPTION option, const void * arg, ...)`

  - **Interface Description**: Sets client options, currently supports locale (`TSDB_OPTION_LOCALE`), character set (`TSDB_OPTION_CHARSET`), timezone (`TSDB_OPTION_TIMEZONE`), configuration file path (`TSDB_OPTION_CONFIGDIR`), and driver type (`TSDB_OPTION_DRIVER`). Locale, character set, and timezone default to the current settings of the operating system. The driver type can be either the native interface(`native`) or the WebSocket interface(`websocket`), with the default being `websocket`.
  - **Note**: The driver type setting (`TSDB_OPTION_DRIVER`) must be called at the beginning of the program and can only be called once.
  - **Parameter Description**:
    - `option`: [Input] Setting item type.
    - `arg`: [Input] Setting item value.
  - **Return Value**: `0`: Success, `-1`: Failure.

- `int taos_options_connection(TAOS *taos, TSDB_OPTION_CONNECTION option, const void *arg, ...)`

  - **description**:Set each connection option on the client side. Currently, it supports character set setting(`TSDB_OPTION_CONNECTION_CHARSET`), time zone setting(`TSDB_OPTION_CONNECTION_TIMEZONE`), user IP setting(`TSDB_OPTION_CONNECTION_USER_IP`), and user APP setting(`TSDB_OPTION_CONNECTION_USER_APP`), and connector info setting(`TSDB_OPTION_CONNECTION_CONNECTOR_INFO`).
  - **input**:
    - `taos`: returned by taos_connect.
    - `option`: option name.
    - `arg`: option value.
  - **return**:
    - `0`: success.
    - `others`: fail.
  - **notice**:
    - The character set and time zone default to the current settings of the operating system, and Windows does not support connection level time zone settings.
    - When arg is NULL, it means resetting the option.
    - This interface is only valid for the current connection and will not affect other connections.
    - If the same parameter is called multiple times, the latter shall prevail and can be used as a modification method.
    - The option of TSDB_OPTION_CONNECTION_CLEAR is used to reset all connection options.
    - After resetting the time zone and character set, using the operating system settings, the user IP and user app will be reset to empty.
    - The values of the connection options are all string type, and the maximum value of the user app parameter is 23, the maximum value of the connector info parameter is 255, which will be truncated if exceeded; Error reported when other parameters are illegal.
    - If time zone value can not be used to find a time zone file or can not be interpreted as a direct specification, UTC is used, which is the same as the operating system time zone rules. Please refer to the tzset function description for details. You can view the current time zone of the connection by sql:select timezone().
    - Time zones and character sets only work on the client side and do not affect related behaviors on the server side.
    - The time zone file uses the operating system time zone file and can be updated by oneself. If there is an error when setting the time zone, please check if the time zone file or path (mac:/var/db/timezone/zoneinfo, Linux:/var/share/zoneinfo) is correct.

- `char *taos_get_client_info()`

  - **Interface Description**: Gets client version information.
  - **Return Value**: Returns client version information.

- `TAOS *taos_connect(const char *ip, the char *user, the char *pass, the char *db, uint16_t port);`

  - **Interface Description**: Creates a database connection, initializes the connection context.
  - **Parameter Description**:
    - ip: [Input] FQDN of any node in the TDengine cluster.
    - user: [Input] Username.
    - pass: [Input] Password.
    - db: [Input] Database name, if not provided by the user, connection can still be established, and the user can create a new database through this connection. If a database name is provided, it indicates that the database has already been created by the user, and it will be used by default.
    - port: [Input] Port on which the taosd program listens.
  - **Return Value**: Returns the database connection, a null return value indicates failure. The application needs to save the returned parameter for subsequent use.
    :::info
    The same process can connect to multiple TDengine clusters based on different hosts/ports.
    :::

- `TAOS *taos_connect_auth(const char *host, const char *user, const char *auth, const char *db, uint16_t port)`

  - **Interface Description**: Same functionality as taos_connect. Except the pass parameter is replaced by auth, other parameters are the same as taos_connect.
  - **Parameter Description**:
    - ip: [Input] FQDN of any node in the TDengine cluster.
    - user: [Input] Username.
    - auth: [Input] Original password taken as 32-bit lowercase md5.
    - db: [Input] Database name, if not provided by the user, connection can still be established, and the user can create a new database through this connection. If a database name is provided, it indicates that the database has already been created, and it will be used by default.
    - port: [Input] Port listened by the taosd program.
  - **Return Value**: Returns the database connection, a null return value indicates failure. The application needs to save the returned parameter for subsequent use.

- `char *taos_get_server_info(TAOS *taos)`

  - **Interface Description**: Get server version information.
  - **Parameter Description**:
    - taos: [Input] Pointer to the database connection, which is established through the `taos_connect()` function.
  - **Return Value**: Returns the server version information.

- `int taos_select_db(TAOS *taos, const char *db)`

  - **Interface Description**: Sets the current default database to `db`.
  - **Parameter Description**:
    - taos: [Input] Pointer to the database connection, which is established through the `taos_connect()` function.
    - db: [Input] Database name.
  - **Return Value**: `0`: Success, non-`0`: Failure, refer to the error code page for details.

- `int taos_get_current_db(TAOS *taos, char *database, int len, int *required)`

  - **Interface Description**: Get the current database name.
  - **Parameter Description**:
    - taos: [Input] Pointer to the database connection, which is established through the `taos_connect()` function.
    - database: [Output] Stores the current database name.
    - len: [Input] Space size of the database.
    - required: [Output] Stores the space required for the current database name (including the final '\0').
  - **Return Value**: `0`: Success, `-1`: Failure, detailed error information can be obtained by calling the function taos_errstr(NULL).
    - If database == NULL or len \<= 0, returns failure.
    - If len is less than the space required to store the database name (including the final '\0'), returns failure, and the data in the database is truncated and ends with '\0'.
    - If len is greater than or equal to the space required to store the database name (including the final '\0'), returns success, and the database name ends with '\0' in the database.

- `int taos_set_notify_cb(TAOS *taos, __taos_notify_fn_t fp, void *param, int type)`

  - **Interface Description**: Set the event callback function.
  - **Parameter Description**:
    - taos: [Input] Pointer to the database connection, which is established through the `taos_connect()` function.
    - fp: [Input] Event callback function pointer. Function declaration: typedef void (*\_\_taos_notify_fn_t)(void*param, void \*ext, int type); where, param is the user-defined parameter, ext is the extension parameter (dependent on the event type, for TAOS_NOTIFY_PASSVER returns user password version), type is the event type.
    - param: [Input] User-defined parameter.
    - type: [Input] Event type. Range of values: 1) TAOS_NOTIFY_PASSVER: User password change.
  - **Return Value**: `0`: Success, `-1`: Failure, detailed error information can be obtained by calling the function taos_errstr(NULL).

- `void taos_close(TAOS *taos)`
  - **Interface Description**: Close connection.
  - **Parameter Description**:
    - taos: [Input] Pointer to the database connection, which is established through the `taos_connect()` function.

### Synchronous Queries

This section introduces APIs that are all synchronous interfaces. After being called by the application, they will block and wait for a response until a result or error message is received.

- `TAOS_RES* taos_query(TAOS *taos, const char *sql)`

  - **Interface Description**: Executes an SQL statement, which can be a DQL, DML, or DDL statement.
  - **Parameter Description**:
    - taos: [Input] Pointer to the database connection, which is established through the `taos_connect()` function.
    - sql: [Input] The SQL statement to execute.
  - **Return Value**: The execution result cannot be determined by whether the return value is `NULL`. Instead, the `taos_errno()` function must be called to parse the error code in the result set.
    - taos_errno return value: `0`: success, `-1`: failure, for details please call the taos_errstr function to get the error message.

- `int taos_result_precision(TAOS_RES *res)`

  - **Interface Description**: Returns the precision category of the timestamp field in the result set.
  - **Parameter Description**:
    - res: [Input] Result set.
  - **Return Value**: `0`: millisecond, `1`: microsecond, `2`: nanosecond.

- `TAOS_ROW taos_fetch_row(TAOS_RES *res)`

  - **Interface Description**: Fetches data from the query result set row by row.
  - **Parameter Description**:
    - res: [Input] Result set.
  - **Return Value**: Non-`NULL`: success, `NULL`: failure, you can call taos_errstr(NULL) for more detailed error information.

- `int taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows)`

  - **Interface Description**: Batch fetches data from the query result set.
  - **Parameter Description**:
    - res: [Input] Result set.
    - rows: [Output] Used to store rows fetched from the result set.
  - **Return Value**: The return value is the number of rows fetched; if there are no more rows, it returns 0.

- `int taos_num_fields(TAOS_RES *res)` and `int taos_field_count(TAOS_RES *res)`

  - **Interface Description**: These two APIs are equivalent and are used to get the number of columns in the query result set.
  - **Parameter Description**:
    - res: [Input] Result set.
  - **Return Value**: The return value is the number of columns in the result set.

- `int* taos_fetch_lengths(TAOS_RES *res)`

  - **Interface Description**: Gets the length of each field in the result set.
  - **Parameter Description**:
    - res: [Input] Result set.
  - **Return Value**: The return value is an array, the length of which is the number of columns in the result set.

- `int taos_affected_rows(TAOS_RES *res)`

  - **Interface Description**: Gets the number of rows affected by the executed SQL statement.
  - **Parameter Description**:
    - res: [Input] Result set.
  - **Return Value**: The return value indicates the number of affected rows.

- `TAOS_FIELD *taos_fetch_fields(TAOS_RES *res)`

  - **Interface Description**: Gets the attributes of each column's data in the query result set (column name, data type, length), used in conjunction with `taos_num_fields()` to parse the data of a tuple (a row) returned by `taos_fetch_row()`.
  - **Parameter Description**:
    - res: [Input] Result set.
  - **Return Value**: Non-`NULL`: successful, returns a pointer to a TAOS_FIELD structure, each element representing the metadata of a column. `NULL`: failure.

- `TAOS_FIELD_E *taos_fetch_fields_e(TAOS_RES *res)`

  - **Interface Description**: Retrieves the attributes of each column in the query result set (column name, data type, column length). Used in conjunction with `taos_num_fields()`, it can be used to parse the data of a tuple (a row) returned by `taos_fetch_row()`. In addition to the basic information provided by TAOS_FIELD, TAOS_FIELD_E also includes `precision` and `scale` information for the data type.
  - **Parameter Description**:
    - res: [Input] Result set.
  - **Return Value**: Non-`NULL`: Success, returns a pointer to a TAOS_FIELD_E structure, where each element represents the metadata of a column. `NULL`: Failure.

- `int taos_print_row(char *str, TAOS_ROW row, TAOS_FIELD *fields, int num_fields)`

  - **Interface Description**: Formats a row of query results as text according to column types and writes it to the `str` buffer for logging or debugging output.
  - **Parameter Description**:
    - `str`: [Output] A user-provided character buffer that receives the entire line of formatted text. Ensure that the capacity meets the output requirements. If the result exceeds the buffer size, it will be truncated (possibly incomplete output).
    - `fields`: [Input] An array of column metadata, returned by `taos_fetch_fields()`. Used to format each column according to its column type.
    - `num_fields`: [Input] The number of columns, typically the return value of `taos_num_fields()`.
  - **Return Value**: `>=0` indicates the number of characters actually written to `str` (excluding the trailing `'\0'`); `<0` indicates a failure error code.

- `void taos_stop_query(TAOS_RES *res)`

  - **Interface Description**: Stops the execution of the current query.
  - **Parameter Description**:
    - res: [Input] Result set.

- `void taos_free_result(TAOS_RES *res)`

  - **Interface Description**: Frees the query result set and related resources. After completing the query, it is essential to call this API to release resources, otherwise, it may lead to memory leaks in the application. However, be aware that if you call `taos_consume()` or other functions to fetch query results after releasing resources, it will cause the application to crash.
  - **Parameter Description**:
    - res: [Input] Result set.

- `char *taos_errstr(TAOS_RES *res)`

  - **Interface Description**: Gets the reason for the failure of the most recent API call, returning a string indicating the error message.
  - **Parameter Description**:
    - res: [Input] Result set.
  - **Return Value**: A string indicating the error message.

- `int taos_errno(TAOS_RES *res)`
  - **Interface Description**: Retrieves the error code of the last API call failure.
  - **Parameter Description**:
    - res: [Input] Result set.
  - **Return Value**: String indicating the error message.

:::note
From version 2.0, TDengine recommends that each thread in a database application establishes its own connection, or builds a connection pool based on the thread, rather than sharing the connection (TAOS\*) structure across different threads in the application. Operations such as queries and writes based on the TAOS structure are thread-safe, but stateful statements like "USE statement" may interfere with each other across threads. Additionally, the C language connector can dynamically establish new database-oriented connections as needed (this process is invisible to users), and it is recommended to call `taos_close()` to close the connection only when the program is about to exit.
Another point to note is that during the execution of the aforementioned synchronous APIs, APIs like pthread_cancel should not be used to forcibly terminate threads, as this involves synchronization operations of some modules and may cause issues including but not limited to deadlocks.
:::

### Asynchronous Queries

TDengine also offers higher-performance asynchronous APIs for data insertion and query operations. Under the same hardware and software conditions, the asynchronous API processes data insertions 2 to 4 times faster than the synchronous API. Asynchronous APIs use a non-blocking call method, returning immediately before a specific database operation is actually completed. The calling thread can then handle other tasks, thereby enhancing the overall application performance. Asynchronous APIs are particularly advantageous under conditions of severe network latency.

Asynchronous APIs require the application to provide corresponding callback functions, with parameters set as follows: the first two parameters are consistent, the third depends on the specific API. The first parameter, param, is provided by the application during the asynchronous API call for use in the callback to retrieve the context of the operation, depending on the implementation. The second parameter is the result set of the SQL operation; if null, such as in an insert operation, it means no records are returned; if not null, such as in a select operation, it means records are returned.

Asynchronous APIs are relatively demanding for users, who may choose to use them based on specific application scenarios. Below are two important asynchronous APIs:

- `void taos_query_a(TAOS *taos, const char *sql, void (*fp)(void *param, TAOS_RES *, int code), void *param);`

  - **Interface Description**: Asynchronously executes an SQL statement.
  - **Parameter Description**:
    - taos: [Input] Pointer to the database connection, established through the `taos_connect()` function.
    - sql: [Input] SQL statement to be executed.
    - fp: User-defined callback function, where the third parameter `code` indicates whether the operation was successful (`0` for success, negative for failure; call `taos_errstr()` to get the reason for failure). The application should mainly handle the second parameter `TAOS_RES *`, which is the result set returned by the query.
    - param: Parameter provided by the application for the callback.

- `void taos_fetch_rows_a(TAOS_RES *res, void (*fp)(void *param, TAOS_RES *, int numOfRows), void *param);`
  - **Interface Description**: Batch retrieves the result set of an asynchronous query, can only be used in conjunction with `taos_query_a()`.
  - **Parameter Description**:
    - res: Result set returned by the callback of `taos_query_a()`.
    - fp: Callback function. Its parameter `param` is a user-defined parameter structure passed to the callback function; `numOfRows` is the number of rows of data retrieved (not the function of the entire result set). In the callback function, the application can iterate forward through the batch records by calling `taos_fetch_row()`. After reading all the records in a block, the application needs to continue calling `taos_fetch_rows_a()` in the callback function to process the next batch of records until the returned number of rows `numOfRows` is zero (results are completely returned) or the number of rows is negative (query error).

TDengine's asynchronous APIs all use a non-blocking call mode. Applications can open multiple tables simultaneously with multiple threads and can perform queries or insertions on each opened table at the same time. It should be noted that **client applications must ensure that operations on the same table are completely serialized**, meaning that a second insertion or query operation cannot be performed on the same table until the first operation is completed (has not returned).

### Parameter Binding

In addition to directly calling `taos_query()` for queries, TDengine also offers a Prepare API that supports parameter binding, similar in style to MySQL, currently only supporting the use of a question mark `?` to represent the parameter to be bound.

Starting from version 3.3.5.0, TDengine has significantly simplified the usage interface of the old parameter binding. This avoids the resource consumption of SQL syntax parsing when writing data through the parameter binding interface, and through batch binding, significantly improves writing performance in most cases. The typical operation steps are as follows:

1. Call `taos_stmt2_init()` to create a parameter binding object;
2. Call `taos_stmt2_prepare()` to parse INSERT or SELECT statements;
3. Call `taos_stmt2_bind_param()` to bind multiple subtables to a single supertable, with each subtable able to bind multiple rows of data;
4. Call `taos_stmt2_exec()` to execute the prepared batch processing command;
5. Steps 3 to 4 can be repeated to write more data rows without re-parsing SQL;
6. After execution is complete, call `taos_stmt2_close()` to release all resources.

Note: If `taos_stmt2_exec()` executes successfully and there is no need to change the SQL statement, then it is possible to reuse the parsing result of `taos_stmt2_prepare()` and directly proceed to steps 3 to 4 to bind new data. However, if there is an error in execution, it is not recommended to continue working in the current context. Instead, it is advisable to release resources and start over from the `taos_stmt2_init()` step. You can check the specific error reason through `taos_stmt2_error`.

The difference between stmt2 and stmt is:

- stmt2 supports batch binding of data in multiple tables, while stmt only supports binding data in a single table.
- stmt2 supports asynchronous execution, while stmt only supports synchronous execution.
- stmt2 supports efficient write mode and automatic table creation, while stmt does not support it.
- stmt2 supports `insert into stb(...tbname,...)values(?,?,?)` syntax, while stmt does not support it.
- stmt2 supports some labels/columns as fixed values, while stmt requires all columns to be `?`.

stmt upgrade stmt2 changes:

1. Change `taos_stmt_init()` to `taos_stmt2_init()`, add `TAOS_STMT2_OPTION`.
2. Change `taos_stmt_prepare()` to `taos_stmt2_prepare()`.
3. Change `taos_stmt_set_tbname_tags`, `taos_stmt_bind_param()` and `taos_stmt_add_batch` to `taos_stmt2_bind_param()`, change `TAOS_MULTI_BIND` to `TAOS_STMT2_BINDV`.
4. Change `taos_stmt_execute()` to `taos_stmt2_exec()`, add `affected_rows` parameter.
5. Change `taos_stmt_close()` to `taos_stmt2_close()`.

The specific functions related to the interface are as follows (you can also refer to the [stmt2_insert_demo.c](https://github.com/taosdata/TDengine/tree/main/docs/examples/c/stmt2_insert_demo.c) file for how to use the corresponding functions):

- `TAOS_STMT2 *taos_stmt2_init(TAOS *taos, TAOS_STMT2_OPTION *option)`

  - **Interface Description**: Initializes a precompiled SQL statement object.
  - **Parameter Description**:
    - taos: [Input] Pointer to the database connection, which is established through the `taos_connect()` function.
    - option: [Input] Creation configuration. When selecting high-efficiency write mode, `singleStbInsert` and `singleTableBindOnce` need to be set to `true`; when selecting asynchronous execution, the callback function `asyncExecFn` and parameter `userdata` need to be set.
  - **Return Value**: Non-`NULL`: Success, returns a pointer to a TAOS_STMT2 structure representing the precompiled SQL statement object. `NULL`: Failure, please call taos_stmt_errstr() function for error details.

- `int taos_stmt2_prepare(TAOS_STMT2 *stmt, const char *sql, unsigned long length)`

  - **Interface Description**: Parses a precompiled SQL statement and binds the parsing results and parameter information to stmt.
  - **Parameter Description**:
    - stmt: [Input] Pointer to a valid precompiled SQL statement object.
    - sql: [Input] SQL statement to be parsed.
    - length: [Input] Length of the sql parameter. If the length is greater than 0, this parameter will be used as the length of the SQL statement; if it is 0, the length of the SQL statement will be automatically determined.
  - **Return Value**: `0`: Success. Non-`0`: Failure, please refer to the error code page for details.

- `int taos_stmt2_bind_param(TAOS_STMT2 *stmt, TAOS_STMT2_BINDV *bindv, int32_t col_idx)`

  - **Interface Description**: Binds a batch of parameters to a precompiled SQL statement.
  - **Parameter Description**:
    - stmt: [Input] Pointer to a valid precompiled SQL statement object.
    - bindv: [Input] Pointer to a valid TAOS_STMT2_BINDV structure, which contains the table names, tags, and data to be bound. `count` represents the number of tables to be bound, one `tbnames` can correspond to one set of `tags` and multiple sets of `bind_cols`; if it is a `SELECT` statement, only `bind_cols` needs to be bound.
    - col_idx: [Input] Represents the position of the specified column to be bound, `-1` means full column binding.
  - **Return Value**: `0`: Success. Non-`0`: Failure, please refer to the error code page for details.

- `int taos_stmt2_exec(TAOS_STMT2 *stmt, int *affected_rows)`

  - **Interface Description**: Executes the SQL with bound data, can be synchronous or asynchronous, determined by option.
  - **Parameter Description**:
    - stmt: [Input] Pointer to a valid precompiled SQL statement object.
    - affected_rows: [Output] If synchronous execution, represents the number of rows affected by this execution.
  - **Return Value**: `0`: Success. Non-`0`: Failure, please refer to the error code page for details.

- `int taos_stmt2_close(TAOS_STMT2 *stmt)`

  - **Interface Description**: After execution, releases all resources.
  - **Parameter Description**:
    - stmt: [Input] Pointer to a valid precompiled SQL statement object.
  - **Return Value**: `0`: Success. Non-`0`: Failure, please refer to the error code page for details.

- `int taos_stmt2_get_fields(TAOS_STMT2 *stmt, int *count, TAOS_FIELD_ALL **fields)`

  - **Interface Description**: Gets an array of column data attributes (column name, column data type, column length, column schema type) corresponding to the `?` order.
  - **Parameter Description**:
    - stmt: [Input] Pointer to a valid precompiled SQL statement object.
    - count: [Output] Returns the number of `?` in the bound SQL.
    - fields: [Output] Returns the column data attributes corresponding to the `?` order. If it is a `SELECT` statement, this structure returns `NULL`.
  - **Return Value**: `0`: Success. Non-`0`: Failure, please refer to the error code page for details.

- `void taos_stmt2_free_fields(TAOS_STMT2 *stmt, TAOS_FIELD_ALL *fields)`

  - **Interface Description**: Releases the memory of TAOS_FIELD_ALL return value, generally used after taos_stmt2_get_fields.
  - **Parameter Description**:
    - stmt: [Input] Pointer to a valid precompiled SQL statement object.
    - fields: [Input] Pointer to the resource to be released.
  - **Return Value**: None.

- `TAOS_RES *taos_stmt2_result(TAOS_STMT2 *stmt)`

  - **Interface Description**: Gets the result returned after executing SQL.
  - **Parameter Description**:
    - stmt: [Input] Pointer to a valid precompiled SQL statement object.
  - **Return Value**: Returns a pointer to a TAOS_RES structure, which contains the results of the insert operation. The returned TAOS_RES must be managed by the caller to avoid memory leaks.

- `char *taos_stmt2_error(TAOS_STMT2 *stmt)`
  - **Interface Description**: Used to obtain error information when other STMT2 APIs return an error (return error code or null pointer).
  - **Parameter Description**:
    - stmt: [Input] Pointer to a valid precompiled SQL statement object.
  - **Return Value**: Returns a pointer to a string containing error information.

<details>
<summary>Parameter Binding(old)</summary>

Starting from versions 2.1.1.0 and 2.1.2.0, TDengine has significantly improved the parameter binding interface support for data writing (INSERT) scenarios. This avoids the resource consumption of SQL syntax parsing when writing data through the parameter binding interface, thereby significantly improving writing performance in most cases. The typical operation steps are as follows:

1. Call `taos_stmt_init()` to create a parameter binding object;
2. Call `taos_stmt_prepare()` to parse the INSERT statement;
3. If the INSERT statement reserves a table name but no TAGS, then call `taos_stmt_set_tbname()` to set the table name;
4. If the INSERT statement reserves both a table name and TAGS (for example, the INSERT statement adopts the automatic table creation method), then call `taos_stmt_set_tbname_tags()` to set the values of the table name and TAGS;
5. Call `taos_stmt_bind_param_batch()` to set the VALUES in a multi-row manner, or call `taos_stmt_bind_param()` to set the VALUES in a single-row manner;
6. Call `taos_stmt_add_batch()` to add the currently bound parameters to the batch processing;
7. Steps 3 to 6 can be repeated to add more data rows to the batch processing;
8. Call `taos_stmt_execute()` to execute the prepared batch processing command;
9. Once execution is complete, call `taos_stmt_close()` to release all resources.

Note: If `taos_stmt_execute()` is successful and there is no need to change the SQL statement, then it is possible to reuse the parsing result of `taos_stmt_prepare()` and directly proceed to steps 3 to 6 to bind new data. However, if there is an error in execution, it is not recommended to continue working in the current context. Instead, it is advisable to release resources and start over from the `taos_stmt_init()` step.

The specific functions related to the interface are as follows (you can also refer to the [prepare.c](https://github.com/taosdata/TDengine/blob/develop/docs/examples/c/prepare.c) file for how to use the corresponding functions):

- `TAOS_STMT* taos_stmt_init(TAOS *taos)`

  - **Interface Description**: Initializes a precompiled SQL statement object.
  - **Parameter Description**:
    - taos: [Input] Pointer to the database connection, which is established through the `taos_connect()` function.
  - **Return Value**: Non-`NULL`: Success, returns a pointer to a TAOS_STMT structure representing the precompiled SQL statement object. `NULL`: Failure, please call taos_stmt_errstr() function for error details.

- `int taos_stmt_prepare(TAOS_STMT *stmt, const char *sql, unsigned long length)`

  - **Interface Description**: Parses a precompiled SQL statement and binds the parsing results and parameter information to stmt.
  - **Parameter Description**:
    - stmt: [Input] Pointer to a valid precompiled SQL statement object.
    - sql: [Input] SQL statement to be parsed.
    - length: [Input] Length of the sql parameter. If the length is greater than 0, this parameter will be used as the length of the SQL statement; if it is 0, the length of the SQL statement will be automatically determined.
  - **Return Value**: `0`: Success. Non-`0`: Failure, please refer to the error code page for details.

- `int taos_stmt_bind_param(TAOS_STMT *stmt, TAOS_MULTI_BIND *bind)`

  - **Interface Description**: Binds parameters to a precompiled SQL statement. Not as efficient as `taos_stmt_bind_param_batch()`, but can support non-INSERT type SQL statements.
  - **Parameter Description**:
    - stmt: [Input] Pointer to a valid precompiled SQL statement object.
    - bind: [Input] Pointer to a valid TAOS_MULTI_BIND structure, which contains the list of parameters to be bound to the SQL statement. Ensure that the number and order of elements in this array match the parameters in the SQL statement exactly. The usage of TAOS_MULTI_BIND is similar to MYSQL_BIND in MySQL.
  - **Return Value**: `0`: Success. Non-`0`: Failure, please refer to the error code page for details.

- `int taos_stmt_set_tbname(TAOS_STMT* stmt, const char* name)`

  - **Interface Description**: (New in version 2.1.1.0, only supports replacing parameter values in INSERT statements) When the table name in the SQL statement uses a `?` placeholder, this function can be used to bind a specific table name.
  - **Parameter Description**:
    - stmt: [Input] Pointer to a valid precompiled SQL statement object.
    - name: [Input] Pointer to a string constant containing the subtable name.
  - **Return Value**: `0`: Success. Non-`0`: Failure, please refer to the error code page for details.

- `int taos_stmt_set_tbname_tags(TAOS_STMT* stmt, const char* name, TAOS_MULTI_BIND* tags)`

  - **Interface Description**: (Added in version 2.1.2.0, only supports replacing parameter values in INSERT statements) When both the table name and TAGS in the SQL statement use `?` placeholders, this function can be used to bind specific table names and specific TAGS values. The most typical scenario is the INSERT statement that uses the auto-create table feature (the current version does not support specifying specific TAGS columns). The number of columns in the TAGS parameter must match exactly the number of TAGS required by the SQL statement.
  - **Parameter Description**:
    - stmt: [Input] Points to a valid pointer to a precompiled SQL statement object.
    - name: [Input] Points to a string constant containing the subtable name.
    - tags: [Input] Points to a valid pointer to a TAOS_MULTI_BIND structure, which contains the values of the subtable tags.
  - **Return Value**: `0`: Success. Non-`0`: Failure, please refer to the error code page for details.

- `int taos_stmt_bind_param_batch(TAOS_STMT* stmt, TAOS_MULTI_BIND* bind)`

  - **Interface Description**: (Added in version 2.1.1.0, only supports replacing parameter values in INSERT statements) Passes the data to be bound in a multi-column manner, ensuring that the order and number of data columns passed here are completely consistent with the VALUES parameters in the SQL statement.
  - **Parameter Description**:
    - stmt: [Input] Points to a valid pointer to a precompiled SQL statement object.
    - bind: [Input] Points to a valid pointer to a TAOS_MULTI_BIND structure, which contains the list of parameters to be batch bound to the SQL statement.
  - **Return Value**: `0`: Success. Non-`0`: Failure, please refer to the error code page for details.

- `int taos_stmt_add_batch(TAOS_STMT *stmt)`

  - **Interface Description**: Adds the currently bound parameters to the batch processing. After calling this function, you can call `taos_stmt_bind_param()` or `taos_stmt_bind_param_batch()` again to bind new parameters. Note that this function only supports INSERT/IMPORT statements; if it is a SELECT or other SQL statements, it will return an error.
    - stmt: [Input] Points to a valid pointer to a precompiled SQL statement object.
  - **Return Value**: `0`: Success. Non-`0`: Failure, please refer to the error code page for details.

- `int taos_stmt_execute(TAOS_STMT *stmt)`

  - **Interface Description**: Executes the prepared statement. Currently, a statement can only be executed once.
    - stmt: [Input] Points to a valid pointer to a precompiled SQL statement object.
  - **Return Value**: `0`: Success. Non-`0`: Failure, please refer to the error code page for details.

- `int taos_stmt_affected_rows(TAOS_STMT *stmt)`

  - **Interface Description**: Gets the number of rows affected after executing the precompiled SQL statement.
    - stmt: [Input] Points to a valid pointer to a precompiled SQL statement object.
  - **Return Value**: Returns the number of affected rows.

- `int taos_stmt_affected_rows_once(TAOS_STMT *stmt)`

  - **Interface Description**: Gets the number of rows affected by executing a bound statement once.
    - stmt: [Input] Points to a valid pointer to a precompiled SQL statement object.
  - **Return Value**: Returns the number of affected rows.

- `TAOS_RES* taos_stmt_use_result(TAOS_STMT *stmt)`

  - **Interface Description**: Retrieves the result set of the statement. The usage of the result set is consistent with non-parameterized calls, and `taos_free_result()` should be called to release resources after use.
    - stmt: [Input] Points to a valid pointer to a precompiled SQL statement object.
  - **Return Value**: Non-`NULL`: Success, returns a pointer to the query result set. `NULL`: Failure, please call taos_stmt_errstr() function for error details.

- `int taos_stmt_close(TAOS_STMT *stmt)`

  - **Interface Description**: After execution, releases all resources.
    - stmt: [Input] Points to a valid pointer to a precompiled SQL statement object.
  - **Return Value**: `0`: Success. Non-`0`: Failure, please refer to the error code page for details.

- `char * taos_stmt_errstr(TAOS_STMT *stmt)`
  - **Interface Description**: (Added in version 2.1.3.0) Used to obtain error information when other STMT APIs return an error (return error code or null pointer).
    - stmt: [Input] Points to a valid pointer to a precompiled SQL statement object.
  - **Return Value**: Returns a pointer to a string containing error information.

</details>

### Schemaless Insert

In addition to using SQL or parameter binding APIs to insert data, you can also use a Schemaless method for insertion. Schemaless allows you to insert data without having to pre-create the structure of supertables/subtables. The TDengine system will automatically create and maintain the required table structure based on the data content written. For more details on how to use Schemaless, see the [Schemaless Insert](../../../developer-guide/schemaless-ingestion/) section. Here, we introduce the accompanying C/C++ API.

- `TAOS_RES* taos_schemaless_insert(TAOS* taos, const char* lines[], int numLines, int protocol, int precision)`

  - **Interface Description**: Performs a batch insert operation in schemaless mode, writing text data in line protocol to TDengine.
    - taos: [Input] Pointer to the database connection, which is established through the `taos_connect()` function.
    - lines: [Input] Text data. Schemaless text strings that meet the parsing format requirements.
    - numLines: [Input] The number of lines of text data, cannot be 0.
    - protocol: [Input] Line protocol type, used to identify the text data format.
    - precision: [Input] Timestamp precision string in the text data.
  - **Return Value**: Returns a pointer to a TAOS_RES structure, which contains the results of the insert operation. Applications can obtain error information using `taos_errstr()`, or get the error code using `taos_errno()`. In some cases, the returned TAOS_RES may be `NULL`, in which case `taos_errno()` can still be safely called to obtain the error code information.
    The returned TAOS_RES must be managed by the caller to avoid memory leaks.

  **Explanation**

  Protocol type is an enumeration type, including the following three formats:

  - TSDB_SML_LINE_PROTOCOL: InfluxDB Line Protocol
  - TSDB_SML_TELNET_PROTOCOL: OpenTSDB Telnet text line protocol
  - TSDB_SML_JSON_PROTOCOL: OpenTSDB Json protocol format

  The definition of timestamp resolution, defined in the `taos.h` file, is as follows:

  - TSDB_SML_TIMESTAMP_NOT_CONFIGURED = 0,
  - TSDB_SML_TIMESTAMP_HOURS,
  - TSDB_SML_TIMESTAMP_MINUTES,
  - TSDB_SML_TIMESTAMP_SECONDS,
  - TSDB_SML_TIMESTAMP_MILLI_SECONDS,
  - TSDB_SML_TIMESTAMP_MICRO_SECONDS,
  - TSDB_SML_TIMESTAMP_NANO_SECONDS

  Note that the timestamp resolution parameter only takes effect when the protocol type is `SML_LINE_PROTOCOL`.
  For OpenTSDB's text protocols, timestamp parsing follows its official parsing rules — based on the number of characters contained in the timestamp to determine the time precision.

  **Other related schemaless interfaces**

- `TAOS_RES *taos_schemaless_insert_with_reqid(TAOS *taos, char *lines[], int numLines, int protocol, int precision, int64_t reqid)`

  - **Interface Description**: Performs a batch insert operation in schemaless mode, writing text data in line protocol to TDengine. The parameter reqid is passed to track the entire function call chain.
    - taos: [Input] Pointer to the database connection, which is established through `taos_connect()` function.
    - lines: [Input] Text data. Schemaless text strings that meet the parsing format requirements.
    - numLines: [Input] The number of lines of text data, cannot be 0.
    - protocol: [Input] Line protocol type, used to identify the text data format.
    - precision: [Input] Timestamp precision string in the text data.
    - reqid: [Input] Specified request ID, used to track the calling request. The request ID (reqid) can be used to establish a correlation between requests and responses on the client and server sides, which is very useful for tracking and debugging in distributed systems.
  - **Return Value**: Returns a pointer to a TAOS_RES structure, which contains the results of the insert operation. Applications can obtain error information using `taos_errstr()`, or get the error code using `taos_errno()`. In some cases, the returned TAOS_RES may be `NULL`, in which case `taos_errno()` can still be safely called to obtain the error code information.
    The returned TAOS_RES must be managed by the caller to avoid memory leaks.

- `TAOS_RES *taos_schemaless_insert_raw(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision)`

  - **Interface Description**: Executes a schemaless batch insertion operation, writing text data in line protocol format into TDengine. Data is represented by the `lines` pointer and its length `len`, addressing the issue where data containing '\0' gets truncated.
    - taos: [Input] Pointer to the database connection, established through the `taos_connect()` function.
    - lines: [Input] Text data. A schemaless text string that meets parsing format requirements.
    - len: [Input] Total length (in bytes) of the data buffer `lines`.
    - totalRows: [Output] Pointer to an integer, used to return the total number of records successfully inserted.
    - protocol: [Input] Line protocol type, used to identify the text data format.
    - precision: [Input] Precision string for timestamps in the text data.
  - **Return Value**: Returns a pointer to a TAOS_RES structure containing the results of the insertion operation. Errors can be retrieved using `taos_errstr()`, and error codes with `taos_errno()`. In some cases, the returned TAOS_RES may be `NULL`, but `taos_errno()` can still be safely called to obtain error code information.
    The returned TAOS_RES must be freed by the caller to avoid memory leaks.

- `TAOS_RES *taos_schemaless_insert_raw_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision, int64_t reqid)`

  - **Interface Description**: Executes a schemaless batch insertion operation, writing text data in line protocol format into TDengine. Data is represented by the `lines` pointer and its length `len`, addressing the issue where data containing '\0' gets truncated. The `reqid` parameter is passed to track the entire function call chain.
    - taos: [Input] Pointer to the database connection, established through the `taos_connect()` function.
    - lines: [Input] Text data. A schemaless text string that meets parsing format requirements.
    - len: [Input] Total length (in bytes) of the data buffer `lines`.
    - totalRows: [Output] Pointer to an integer, used to return the total number of records successfully inserted.
    - protocol: [Input] Line protocol type, used to identify the text data format.
    - precision: [Input] Precision string for timestamps in the text data.
    - reqid: [Input] Specified request ID, used to track the calling request. The request ID (reqid) can be used to establish a correlation between requests and responses on the client and server sides, which is very useful for tracking and debugging in distributed systems.
  - **Return Value**: Returns a pointer to a TAOS_RES structure containing the results of the insertion operation. Errors can be retrieved using `taos_errstr()`, and error codes with `taos_errno()`. In some cases, the returned TAOS_RES may be `NULL`, but `taos_errno()` can still be safely called to obtain error code information.
    The returned TAOS_RES must be freed by the caller to avoid memory leaks.

- `TAOS_RES *taos_schemaless_insert_ttl(TAOS *taos, char *lines[], int numLines, int protocol, int precision, int32_t ttl)`

  - **Interface Description**: Executes a schemaless batch insertion operation, writing text data in line protocol format into TDengine. The `ttl` parameter is used to control the expiration time of the table's TTL.
    - taos: [Input] Pointer to the database connection, established through the `taos_connect()` function.
    - lines: [Input] Text data. A schemaless text string that meets parsing format requirements.
    - numLines: [Input] Number of lines of text data, cannot be 0.
    - protocol: [Input] Line protocol type, used to identify the text data format.
    - precision: [Input] Precision string for timestamps in the text data.
    - ttl: [Input] Specified Time-To-Live (TTL), in days. Records will be automatically deleted after exceeding this lifespan.
  - **Return Value**: Returns a pointer to a TAOS_RES structure containing the results of the insertion operation. Errors can be retrieved using `taos_errstr()`, and error codes with `taos_errno()`. In some cases, the returned TAOS_RES may be `NULL`, but `taos_errno()` can still be safely called to obtain error code information.
    The returned TAOS_RES must be freed by the caller to avoid memory leaks.

- `TAOS_RES *taos_schemaless_insert_ttl_with_reqid(TAOS *taos, char *lines[], int numLines, int protocol, int precision, int32_t ttl, int64_t reqid)`

  - **Interface Description**: Executes a batch insert operation without a schema, writing line protocol text data into TDengine. The ttl parameter is passed to control the expiration time of the table's ttl. The reqid parameter is passed to track the entire function call chain.
    - taos: [Input] Pointer to the database connection, which is established through the `taos_connect()` function.
    - lines: [Input] Text data. Schemaless text strings that meet parsing format requirements.
    - numLines: [Input] Number of lines of text data, cannot be 0.
    - protocol: [Input] Line protocol type, used to identify the text data format.
    - precision: [Input] Timestamp precision string in the text data.
    - ttl: [Input] Specified Time-To-Live (TTL), in days. Records will be automatically deleted after exceeding this lifespan.
    - reqid: [Input] Specified request ID, used to track the call request. The request ID (reqid) can be used to establish a correlation between requests and responses across client and server sides, which is very useful for tracking and debugging in distributed systems.
  - **Return Value**: Returns a pointer to a TAOS_RES structure, which contains the results of the insert operation. Applications can obtain error information using `taos_errstr()`, or get the error code using `taos_errno()`. In some cases, the returned TAOS_RES may be `NULL`, in which case `taos_errno()` can still be safely called to obtain error code information.
    The returned TAOS_RES must be freed by the caller to avoid memory leaks.

- `TAOS_RES *taos_schemaless_insert_raw_ttl(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision, int32_t ttl)`

  - **Interface Description**: Executes a batch insert operation without a schema, writing line protocol text data into TDengine. The lines pointer and length len are passed to represent the data, to address the issue of data being truncated due to containing '\0'. The ttl parameter is passed to control the expiration time of the table's ttl.
    - taos: [Input] Pointer to the database connection, which is established through the `taos_connect()` function.
    - lines: [Input] Text data. Schemaless text strings that meet parsing format requirements.
    - len: [Input] Total length (in bytes) of the data buffer lines.
    - totalRows: [Output] Points to an integer pointer, used to return the total number of records successfully inserted.
    - protocol: [Input] Line protocol type, used to identify the text data format.
    - precision: [Input] Timestamp precision string in the text data.
    - ttl: [Input] Specified Time-To-Live (TTL), in days. Records will be automatically deleted after exceeding this lifespan.
  - **Return Value**: Returns a pointer to a TAOS_RES structure, which contains the results of the insert operation. Applications can obtain error information using `taos_errstr()`, or get the error code using `taos_errno()`. In some cases, the returned TAOS_RES may be `NULL`, in which case `taos_errno()` can still be safely called to obtain error code information.
    The returned TAOS_RES must be freed by the caller to avoid memory leaks.

- `TAOS_RES *taos_schemaless_insert_raw_ttl_with_reqid(TAOS *taos, char *lines, int len, int32_t *totalRows, int protocol, int precision, int32_t ttl, int64_t reqid)`
  - **Interface Description**: Executes a batch insert operation without a schema, writing line protocol text data into TDengine. The lines pointer and length len are passed to represent the data, to address the issue of data being truncated due to containing '\0'. The ttl parameter is passed to control the expiration time of the table's ttl. The reqid parameter is passed to track the entire function call chain.
    - taos: [Input] Pointer to the database connection, which is established through the `taos_connect()` function.
    - lines: [Input] Text data. Schemaless text strings that meet parsing format requirements.
    - len: [Input] Total length (in bytes) of the data buffer lines.
    - totalRows: [Output] Points to an integer pointer, used to return the total number of records successfully inserted.
    - protocol: [Input] Line protocol type, used to identify the text data format.
    - precision: [Input] Timestamp precision string in the text data.
    - ttl: [Input] Specified Time-To-Live (TTL), in days. Records will be automatically deleted after exceeding this lifespan.
    - reqid: [Input] Specified request ID, used to track the call request. The request ID (reqid) can be used to establish a correlation between requests and responses across client and server sides, which is very useful for tracking and debugging in distributed systems.
  - **Return Value**: Returns a pointer to a TAOS_RES structure, which contains the results of the insert operation. Applications can obtain error information using `taos_errstr()`, or get the error code using `taos_errno()`. In some cases, the returned TAOS_RES may be `NULL`, in which case `taos_errno()` can still be safely called to obtain error code information.
    The returned TAOS_RES must be freed by the caller to avoid memory leaks.

Description:

- The above 7 interfaces are extension interfaces, mainly used for passing ttl and reqid parameters during schemaless writing, and can be used as needed.
- Interfaces with \_raw use the passed parameters lines pointer and length len to represent data, to solve the problem of data containing '\0' being truncated in the original interface. The totalRows pointer returns the number of data rows parsed.
- Interfaces with \_ttl can pass the ttl parameter to control the ttl expiration time of table creation.
- Interfaces with \_reqid can track the entire call chain by passing the reqid parameter.

### Data Subscription

- `const char *tmq_err2str(int32_t code)`

  - **Interface Description**: Used to convert the error code of data subscription into error information.
    - code: [Input] Error code for data subscription.
  - **Return Value**: Returns a pointer to a string containing error information, the return value is not NULL, but the error information may be an empty string.

- `tmq_conf_t *tmq_conf_new()`

  - **Interface Description**: Creates a new TMQ configuration object.
  - **Return Value**: Non `NULL`: Success, returns a pointer to a tmq_conf_t structure, which is used to configure the behavior and features of TMQ. `NULL`: Failure, you can call the function taos_errstr(NULL) for more detailed error information.

- `tmq_conf_res_t tmq_conf_set(tmq_conf_t *conf, const char *key, const char *value)`

  - **Interface Description**: Sets the configuration items in the TMQ configuration object, used to configure consumption parameters.
    - conf: [Input] Pointer to a valid tmq_conf_t structure, representing a TMQ configuration object.
    - key: [Input] Configuration item key name.
    - value: [Input] Configuration item value.
  - **Return Value**: Returns a tmq_conf_res_t enum value, indicating the result of the configuration setting. tmq_conf_res_t defined as follows:

    ```cpp
    typedef enum tmq_conf_res_t {
         TMQ_CONF_UNKNOWN = -2,  // invalid key
         TMQ_CONF_INVALID = -1,  // invalid value
         TMQ_CONF_OK = 0,        // success
       } tmq_conf_res_t;
    ```

- `void tmq_conf_set_auto_commit_cb(tmq_conf_t *conf, tmq_commit_cb *cb, void *param)`

  - **Interface Description**: Sets the auto-commit callback function in the TMQ configuration object.
    - conf: [Input] Pointer to a valid tmq_conf_t structure, representing a TMQ configuration object.
    - cb: [Input] Pointer to a valid tmq_commit_cb callback function, which will be called after the message is consumed to confirm the message handling status.
    - param: [Input] User-defined parameter passed to the callback function.

  The definition of the auto-commit callback function is as follows:

  ```cpp
  typedef void(tmq_commit_cb(tmq_t *tmq, int32_t code, void *param))
  ```

- `void tmq_conf_destroy(tmq_conf_t *conf)`

  - **Interface Description**: Destroys a TMQ configuration object and releases related resources.
    - conf: [Input] Pointer to a valid tmq_conf_t structure, representing a TMQ configuration object.

- `tmq_list_t *tmq_list_new()`

  - **Interface Description**: Used to create a tmq_list_t structure, used to store subscribed topics.
  - **Return Value**: Non `NULL`: Success, returns a pointer to a tmq_list_t structure. `NULL`: Failure, you can call the function taos_errstr(NULL) for more detailed error information.

- `int32_t tmq_list_append(tmq_list_t *list, const char* topic)`

  - **Interface Description**: Used to add a topic to a tmq_list_t structure.
    - list: [Input] Pointer to a valid tmq_list_t structure, representing a TMQ list object.
    - topic: [Input] Topic name.
  - **Return Value**: `0`: Success. Non `0`: Failure, you can call the function `char *tmq_err2str(int32_t code)` for more detailed error information.

- `void tmq_list_destroy(tmq_list_t *list)`

  - **Interface Description**: Used to destroy a tmq_list_t structure, the result of tmq_list_new needs to be destroyed through this interface.
    - list: [Input] Pointer to a valid tmq_list_t structure, representing a TMQ list object.

- `int32_t tmq_list_get_size(const tmq_list_t *list)`

  - **Interface Description**: Used to get the number of topics in the tmq_list_t structure.
    - list: [Input] Points to a valid tmq_list_t structure pointer, representing a TMQ list object.
  - **Return Value**: `>=0`: Success, returns the number of topics in the tmq_list_t structure. `-1`: Failure, indicates the input parameter list is NULL.

- `char **tmq_list_to_c_array(const tmq_list_t *list)`

  - **Interface Description**: Used to convert a tmq_list_t structure into a C array, where each element is a string pointer.
    - list: [Input] Points to a valid tmq_list_t structure pointer, representing a TMQ list object.
  - **Return Value**: Non-`NULL`: Success, returns a C array, each element is a string pointer representing a topic name. `NULL`: Failure, indicates the input parameter list is NULL.

- `tmq_t *tmq_consumer_new(tmq_conf_t *conf, char *errstr, int32_t errstrLen)`

  - **Interface Description**: Used to create a tmq_t structure for consuming data. After consuming the data, tmq_consumer_close must be called to close the consumer.
    - conf: [Input] Points to a valid tmq_conf_t structure pointer, representing a TMQ configuration object.
    - errstr: [Output] Points to a valid character buffer pointer, used to receive error messages that may occur during creation. Memory allocation/release is the responsibility of the caller.
    - errstrLen: [Input] Specifies the size of the errstr buffer (in bytes).
  - **Return Value**: Non-`NULL`: Success, returns a pointer to a tmq_t structure representing a TMQ consumer object. `NULL`: Failure, error information stored in the errstr parameter.

- `int32_t tmq_subscribe(tmq_t *tmq, const tmq_list_t *topic_list)`

  - **Interface Description**: Used to subscribe to a list of topics. After consuming the data, tmq_subscribe must be called to unsubscribe.
    - tmq: [Input] Points to a valid tmq_t structure pointer, representing a TMQ consumer object.
    - topic_list: [Input] Points to a valid tmq_list_t structure pointer, containing one or more topic names.
  - **Return Value**: `0`: Success. Non-`0`: Failure, the function `char *tmq_err2str(int32_t code)` can be called for more detailed error information.

- `int32_t tmq_unsubscribe(tmq_t *tmq)`

  - **Interface Description**: Used to unsubscribe from a list of topics. Must be used in conjunction with tmq_subscribe.
    - tmq: [Input] Points to a valid tmq_t structure pointer, representing a TMQ consumer object.
  - **Return Value**: `0`: Success. Non-`0`: Failure, the function `char *tmq_err2str(int32_t code)` can be called for more detailed error information.

- `int32_t tmq_subscription(tmq_t *tmq, tmq_list_t **topic_list)`

  - **Interface Description**: Used to get the list of subscribed topics.
    - tmq: [Input] Points to a valid tmq_t structure pointer, representing a TMQ consumer object.
    - topic_list: [Output] Points to a pointer of a tmq_list_t structure pointer, used to receive the current list of subscribed topics.
  - **Return Value**: `0`: Success. Non-`0`: Failure, the function `char *tmq_err2str(int32_t code)` can be called for more detailed error information.

- `TAOS_RES *tmq_consumer_poll(tmq_t *tmq, int64_t timeout)`

  - **Interface Description**: Used to poll for consuming data, each consumer can only call this interface in a single thread.
    - tmq: [Input] Points to a valid tmq_t structure pointer, representing a TMQ consumer object.
    - timeout: [Input] Polling timeout in milliseconds, a negative number indicates a default timeout of 1 second.
  - **Return Value**: Non-`NULL`: Success, returns a pointer to a TAOS_RES structure containing the received messages. `NULL`: indicates no data, the error code can be obtained through taos_errno (NULL), please refer to the reference manual for specific error message. TAOS_RES results are consistent with taos_query results, and information in TAOS_RES can be obtained through various query interfaces, such as schema, etc.

- `int32_t tmq_consumer_close(tmq_t *tmq)`

  - **Interface Description**: Used to close a tmq_t structure. Must be used in conjunction with tmq_consumer_new.
    - tmq: [Input] Points to a valid tmq_t structure pointer, which represents a TMQ consumer object.
  - **Return Value**: `0`: Success. Non-`0`: Failure, you can call the function `char *tmq_err2str(int32_t code)` to get more detailed error information.

- `int32_t tmq_get_topic_assignment(tmq_t *tmq, const char *pTopicName, tmq_topic_assignment **assignment, int32_t *numOfAssignment)`

  - **Interface Description**: Returns the information of the vgroup currently assigned to the consumer, including vgId, the maximum and minimum offset of wal, and the current consumed offset.
    - tmq: [Input] Points to a valid tmq_t structure pointer, which represents a TMQ consumer object.
    - pTopicName: [Input] The topic name for which to query the assignment information.
    - assignment: [Output] Points to a pointer to a tmq_topic_assignment structure, used to receive assignment information. The data size is numOfAssignment, and it needs to be released through the tmq_free_assignment interface.
    - numOfAssignment: [Output] Points to an integer pointer, used to receive the number of valid vgroups assigned to the consumer.
  - **Return Value**: `0`: Success. Non-`0`: Failure, you can call the function `char *tmq_err2str(int32_t code)` to get more detailed error information.

- `void tmq_free_assignment(tmq_topic_assignment* pAssignment)`

  - **Interface Description**: Returns the information of the vgroup currently assigned to the consumer, including vgId, the maximum and minimum offset of wal, and the current consumed offset.
    - pAssignment: [Input] Points to a valid tmq_topic_assignment structure array pointer, which contains the vgroup assignment information.

- `int64_t tmq_committed(tmq_t *tmq, const char *pTopicName, int32_t vgId)`

  - **Interface Description**: Gets the committed offset for a specific topic and vgroup of the TMQ consumer object.
    - tmq: [Input] Points to a valid tmq_t structure pointer, which represents a TMQ consumer object.
    - pTopicName: [Input] The topic name for which to query the committed offset.
    - vgId: [Input] The ID of the vgroup.
  - **Return Value**: `>=0`: Success, returns an int64_t value representing the committed offset. `<0`: Failure, the return value is the error code, you can call the function `char *tmq_err2str(int32_t code)` to get more detailed error information.

- `int32_t tmq_commit_sync(tmq_t *tmq, const TAOS_RES *msg)`

  - **Interface Description**: Synchronously commits the message offset processed by the TMQ consumer object.
    - tmq: [Input] Points to a valid tmq_t structure pointer, which represents a TMQ consumer object.
    - msg: [Input] Points to a valid TAOS_RES structure pointer, which contains the processed message. If NULL, commits the current progress of all vgroups consumed by the current consumer.
  - **Return Value**: `0`: Success, the offset has been successfully committed. Non-`0`: Failure, you can call the function `char *tmq_err2str(int32_t code)` to get more detailed error information.

- `void tmq_commit_async(tmq_t *tmq, const TAOS_RES *msg, tmq_commit_cb *cb, void *param)`

  - **Interface Description**: Asynchronously commits the message offset processed by the TMQ consumer object.
    - tmq: [Input] Points to a valid tmq_t structure pointer, which represents a TMQ consumer object.
    - msg: [Input] Points to a valid TAOS_RES structure pointer, which contains the processed message. If NULL, commits the current progress of all vgroups consumed by the current consumer.
    - cb: [Input] A pointer to a callback function, which will be called upon completion of the commit.
    - param: [Input] A user-defined parameter, which will be passed to cb in the callback function.

- `int32_t tmq_commit_offset_sync(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset)`

  - **Interface Description**: Synchronously commits the offset for a specific topic and vgroup of a TMQ consumer object.
    - tmq: [Input] Points to a valid tmq_t structure pointer, representing a TMQ consumer object.
    - pTopicName: [Input] The name of the topic for which the offset is to be committed.
    - vgId: [Input] The ID of the virtual group vgroup.
    - offset: [Input] The offset to be committed.
  - **Return Value**: `0`: Success, the offset has been successfully committed. Non-`0`: Failure, you can call the function `char *tmq_err2str(int32_t code)` to get more detailed error information.

- `void tmq_commit_offset_async(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset, tmq_commit_cb *cb, void *param)`

  - **Interface Description**: Asynchronously commits the offset for a specific topic and vgroup of a TMQ consumer object.
    - tmq: [Input] Points to a valid tmq_t structure pointer, representing a TMQ consumer object.
    - pTopicName: [Input] The name of the topic for which the offset is to be committed.
    - vgId: [Input] The ID of the virtual group vgroup.
    - offset: [Input] The offset to be committed.
    - cb: [Input] A pointer to a callback function that will be called upon completion of the commit.
    - param: [Input] A user-defined parameter that will be passed to the callback function cb.

  **Description**

  - There are two types of commit interfaces, each type has synchronous and asynchronous interfaces:
  - First type: Commit based on message, submitting the progress in the message, if the message is NULL, submit the current progress of all vgroups consumed by the current consumer: tmq_commit_sync/tmq_commit_async
  - Second type: Commit based on the offset of a specific topic and a specific vgroup: tmq_commit_offset_sync/tmq_commit_offset_async

- `int64_t tmq_position(tmq_t *tmq, const char *pTopicName, int32_t vgId)`

  - **Interface Description**: Gets the current consumption position, i.e., the position of the next data that has been consumed.
    - tmq: [Input] Points to a valid tmq_t structure pointer, representing a TMQ consumer object.
    - pTopicName: [Input] The name of the topic for which the current position is being queried.
    - vgId: [Input] The ID of the virtual group vgroup.
  - **Return Value**: `>=0`: Success, returns an int64_t type value representing the offset of the current position. `<0`: Failure, the return value is the error code, you can call the function `char *tmq_err2str(int32_t code)` to get more detailed error information.

- `int32_t tmq_offset_seek(tmq_t *tmq, const char *pTopicName, int32_t vgId, int64_t offset)`

  - **Interface Description**: Sets the offset of a TMQ consumer object in a specific topic and vgroup to a specified position.
    - tmq: [Input] Points to a valid tmq_t structure pointer, representing a TMQ consumer object.
    - pTopicName: [Input] The name of the topic for which the current position is being queried.
    - vgId: [Input] The ID of the virtual group vgroup.
    - offset: [Input] The ID of the virtual group vgroup.
  - **Return Value**: `0`: Success, non-`0`: Failure, you can call the function `char *tmq_err2str(int32_t code)` to get more detailed error information.

- `int64_t tmq_get_vgroup_offset(TAOS_RES* res)`

  - **Interface Description**: Extracts the current consumption data position offset of the virtual group (vgroup) from the message results obtained by the TMQ consumer.
    - res: [Input] Points to a valid TAOS_RES structure pointer, containing messages polled from the TMQ consumer.
  - **Return Value**: `>=0`: Success, returns an int64_t type value representing the offset of the current consumption position. `<0`: Failure, the return value is the error code, you can call the function `char *tmq_err2str(int32_t code)` to get more detailed error information.

- `int32_t tmq_get_vgroup_id(TAOS_RES *res)`

  - **Interface Description**: Extracts the ID of the virtual group (vgroup) from the message results obtained by the TMQ consumer.
    - res: [Input] Points to a valid TAOS_RES structure pointer, which contains messages polled from the TMQ consumer.
  - **Return Value**: `>=0`: Success, returns an int32_t type value representing the ID of the virtual group (vgroup). `<0`: Failure, the return value is the error code, you can call the function `char *tmq_err2str(int32_t code)` to get more detailed error information.

- `TAOS *tmq_get_connect(tmq_t *tmq)`

  - **Interface Description**: Retrieves the connection handle to the TDengine database from the TMQ consumer object.
    - tmq: [Input] Points to a valid tmq_t structure pointer, which represents a TMQ consumer object.
  - **Return Value**: Non-`NULL`: Success, returns a TAOS \* type pointer pointing to the connection handle with the TDengine database. `NULL`: Failure, illegal input parameters.

- `const char *tmq_get_table_name(TAOS_RES *res)`

  - **Interface Description**: Retrieves the table name from the message results obtained by the TMQ consumer.
    - res: [Input] Points to a valid TAOS_RES structure pointer, which contains messages polled from the TMQ consumer.
  - **Return Value**: Non-`NULL`: Success, returns a const char \* type pointer pointing to the table name string. `NULL`: Failure, illegal input parameters.

- `tmq_res_t tmq_get_res_type(TAOS_RES *res)`

  - **Interface Description**: Retrieves the message type from the message results obtained by the TMQ consumer.
    - res: [Input] Points to a valid TAOS_RES structure pointer, which contains messages polled from the TMQ consumer.
  - **Return Value**: Returns a tmq_res_t type enumeration value representing the message type.
    - tmq_res_t represents the type of data consumed, defined as follows:

  ```cpp
  typedef enum tmq_res_t {
    TMQ_RES_INVALID = -1,   // Invalid
    TMQ_RES_DATA = 1,       // Data type
    TMQ_RES_TABLE_META = 2, // Metadata type
    TMQ_RES_METADATA = 3    // Both metadata and data types, i.e., automatic table creation
  } tmq_res_t;
  ```

- `const char *tmq_get_topic_name(TAOS_RES *res)`

  - **Interface Description**: Retrieves the topic name from the message results obtained by the TMQ consumer.
    - res: [Input] Points to a valid TAOS_RES structure pointer, which contains messages polled from the TMQ consumer.
  - **Return Value**: Non-`NULL`: Success, returns a const char \* type pointer pointing to the topic name string. `NULL`: Failure, illegal input parameters.

- `const char *tmq_get_db_name(TAOS_RES *res)`
  - **Interface Description**: Retrieves the database name from the message results obtained by the TMQ consumer.
    - res: [Input] Points to a valid TAOS_RES structure pointer, which contains messages polled from the TMQ consumer.
  - **Return Value**: Non-`NULL`: Success, returns a const char \* type pointer pointing to the database name string. `NULL`: Failure, illegal input parameters.
