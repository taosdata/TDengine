---
toc_max_heading_level: 4
sidebar_label: Node.js
title: Node.js Client Library
slug: /tdengine-reference/client-libraries/node
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import RequestId from "../../assets/resources/_request_id.mdx";

`@tdengine/websocket` is the official Node.js language connector for TDengine. Node.js developers can use it to develop applications that access the TDengine database.

The source code for the Node.js connector is hosted on [GitHub](https://github.com/taosdata/taos-connector-node/tree/main).

## Node.js Version Compatibility

Supports Node.js 14 and above.

## Supported Platforms

Support all platforms that can run Node.js.

## Version History

| Node.js Connector Version | Major Changes                                                            | TDengine Version            |
| ------------------------- | ------------------------------------------------------------------------ | --------------------------- |
| 3.2.0                     | Optimize STMT parameter binding to improve write efficiency. | - | 
| 3.1.9                     | Fix timezone handling in WebSocket connections. | - | 
| 3.1.8                     | Fix when the connection pool returns unavailable connections during network anomalies. | - | 
| 3.1.7                     | Fix cloud service TMQ connection parameter issue. | - |
| 3.1.6                     | 1. Check if the connector supports database version.  <br/> 2. The connector supports adding new subscription parameters. | - |  
| 3.1.5                     | Password supports special characters. |  - |
| 3.1.4                     | Modified the readme.| -                           |
| 3.1.3                     | Upgraded the es5-ext version to address vulnerabilities in the lower version. | -                      |
| 3.1.2                     | Optimized data protocol and parsing, significantly improved performance. | -                           |
| 3.1.1                     | Optimized data transmission performance.                                 | 3.3.2.0 and higher versions |
| 3.1.0                     | New release, supports WebSocket connection.                              | 3.2.0.0 and higher versions |

## Exception Handling

For error code information please refer to [Error Codes](../../error-codes/)

## Data Type Mapping

The table below shows the mapping between TDengine DataType and Node.js DataType

| TDengine DataType | Node.js DataType |
| ----------------- | ---------------- |
| TIMESTAMP         | bigint           |
| TINYINT           | number           |
| SMALLINT          | number           |
| INT               | number           |
| BIGINT            | bigint           |
| TINYINT UNSIGNED  | number           |
| SMALLINT UNSIGNED | number           |
| INT UNSIGNED      | number           |
| BIGINT UNSIGNED   | bigint           |
| FLOAT             | number           |
| DOUBLE            | number           |
| BOOL              | boolean          |
| BINARY            | string           |
| NCHAR             | string           |
| JSON              | string           |
| VARBINARY         | ArrayBuffer      |
| GEOMETRY          | ArrayBuffer      |

**Note**: JSON type is only supported in tags.

## More Example Programs

| Example Program                                                                                                        | Description of Example Program                                    |
| ---------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| [sql_example](https://github.com/taosdata/TDengine/tree/main/docs/examples/node/websocketexample/sql_example.js)       | Basic usage such as establishing connections, executing SQL, etc. |
| [stmt_example](https://github.com/taosdata/TDengine/tree/main/docs/examples/node/websocketexample/stmt_example.js)     | Example of binding parameters for insertion.                      |
| [line_example](https://github.com/taosdata/TDengine/tree/main/docs/examples/node/websocketexample/line_example.js)     | Line protocol writing example.                                    |
| [tmq_example](https://github.com/taosdata/TDengine/tree/main/docs/examples/node/websocketexample/tmq_example.js)       | Example of using subscriptions.                                   |
| [all_type_query](https://github.com/taosdata/TDengine/tree/main/docs/examples/node/websocketexample/all_type_query.js) | Example supporting all types.                                     |
| [all_type_stmt](https://github.com/taosdata/TDengine/tree/main/docs/examples/node/websocketexample/all_type_stmt.js)   | Example of parameter binding supporting all types.                |

## Usage Restrictions

- The Node.js connector (`@tdengine/websocket`) supports Node.js version 14 and above. Versions below 14 may have package compatibility issues.
- Currently, only WebSocket connections are supported, and taosAdapter needs to be started in advance.
- After using the connector, you need to call taos.connectorDestroy(); to release the connector resources.
- To set the time zone for time strings in SQL statements, you need to configure the time zone settings of taosc on the machine where taosadapter is located.
- When parsing result sets, JavaScript does not support the int64 type, so timezone conversion cannot be directly performed. If users have such requirements, third-party libraries can be introduced to provide support.

## Common Issues

1. "Unable to establish connection" or "Unable to resolve FQDN"

   **Reason**: Generally, it is because the FQDN is not configured correctly.

## API Reference

Node.js connector (`@tdengine/websocket`), which connects to a TDengine instance through the WebSocket interface provided by taosAdapter.

### URL Specification

```text
[+<protocol>]://[[<username>:<password>@]<host>:<port>][/<database>][?<p1>=<v1>[&<p2>=<v2>]]
|------------|---|-----------|-----------|------|------|------------|-----------------------|
|   protocol |   | username  | password  | host | port |  database  |  params               |
```

- **protocol**: Use the websocket protocol to establish a connection. For example, `ws://localhost:6041`
- **username/password**: Username and password for the database.
- **host/port**: The host_name parameter supports valid domain names or IP addresses. The `@tdengine/websocket` supports both IPv4 and IPv6 formats. For IPv6 addresses, square brackets must be used (e.g., [::1] or [2001:db8:1234:5678::1]) to avoid port number parsing conflicts.
- **database**: Database name.
- **params**: Other parameters. For example, token.

- Complete DSN example:

```js
  // IPV4:
  ws://root:taosdata@localhost:6041
    
  // IPV6:
  ws://root:taosdata@[::1]:6041
```

### WSConfig

In addition to obtaining a connection through a specified URL, you can also use WSConfig to specify parameters when establishing a connection.

```js
const taos = require("@tdengine/websocket");

async function createConnect() {
    try {
        let url = 'ws://127.0.0.1:6041'
        let conf = new taos.WSConfig(url)
        conf.setUser('root')
        conf.setPwd('taosdata')
        conf.setDb('db')
        conf.setTimeOut(500)
        let wsSql = await taos.sqlConnect(conf)
    } catch (e) {
        console.error(e);
    }
}
```

The configurations in WSConfig are as follows:

- setUrl(url string) Set the taosAdapter connection address url, see the URL specification above.
- setUser(user: string) Set the database username.
- setPwd(pws:string) Set the database password.
- setDb(db: string) Set the database name.
- setTimeOut(ms : number) Set the connection timeout in milliseconds.
- setToken(token: string) Set the taosAdapter authentication token.

### Connection Features

- `static async open(wsConfig:WSConfig):Promise<WsSql>`
  - **Interface Description**: Establish a taosAdapter connection.
  - **Parameter Description**:
    - `wsConfig`: Connection configuration, see the WSConfig section above.
  - **Return Value**: Connection object.
  - **Exception**: Connection failure throws `TDWebSocketClientError` exception.
- `destroyed()`
  - **Interface Description**: Release and destroy resources.
  - **Exception**: Connection failure throws `TDWebSocketClientError` exception.

### Get taosc version number

- `async version(): Promise<string>`
  - **Interface Description**: Get the taosc client version.
  - **Return Value**: taosc client version number.
  - **Exception**: Connection failure throws `TDWebSocketClientError` exception.

### Execute SQL

- `async exec(sql: string, reqId?: number): Promise<TaosResult>`
  - **Interface Description**: Execute an SQL statement.
  - **Parameter Description**:
    - `sql`: The SQL statement to be executed.
    - `reqId`: Request ID, optional, used for issue tracking.
  - **Return Value**: Execution result

  ```js
  TaosResult {
      affectRows: number,   // Number of rows affected
      timing: number,       // Execution duration
      totalTime: number,    // Total response time
  }    
  ```

  - **Exception**: Throws `TDWebSocketClientError` exception if connection fails.
- `async query(sql: string, reqId?:number): Promise<WSRows>`
  - **Interface Description**: Query data.
  - **Parameter Description**:
    - `sql`: The SQL query statement to be executed.
    - `reqId`: Request ID, optional, used for issue tracking.
  - **Return Value**: WSRows data set object.
  - **Exception**: Throws `TDWebSocketClientError` exception if connection fails.

### Data Set

- `getMeta():Array<TDengineMeta> | null`
  - **Interface Description**: Get the number, type, and length of columns in the query result.
  - **Return Value**: Array of TDengineMeta data objects.

    ```js
    export interface TDengineMeta {
        name: string,
        type: string,
        length: number,
    }
    ```

- `async next(): Promise<boolean>`
  - **Interface Description**: Moves the cursor forward one row from the current position. Used to traverse the query result set.
  - **Return Value**: Returns true if the new current row is valid; returns false if there are no more rows in the result set.
  - **Exception**: Throws `TDWebSocketClientError` exception if connection fails.
- `getData(): Array<any>`
  - **Interface Description**: Returns one row of queried data.
  - **Return Value**: Returns one row of queried data; this interface needs to be used together with the next interface.
- `async close():Promise<void>`
  - **Interface Description**: After data reading is complete, release the result set.
  - **Exception**: Throws `TDWebSocketClientError` exception if connection fails.

### Schemaless Writing

- `async schemalessInsert(lines: Array<string>, protocol: SchemalessProto, precision: Precision, ttl: number, reqId?: number): Promise<void>`
  - **Interface Description**: Schemaless writing.
  - **Parameter Description**:
    - `lines`: Array of data to be written, the specific data format for schemaless can refer to `Schemaless Writing`.
    - `protocol`: Protocol type
      - `SchemalessProto.InfluxDBLineProtocol`: InfluxDB Line Protocol.
      - `SchemalessProto.OpenTSDBTelnetLineProtocol`: OpenTSDB Text Line Protocol.
      - `SchemalessProto.OpenTSDBJsonFormatProtocol`: JSON Protocol format.
    - `precision`: Time precision
      - `Precision.HOURS`: Hours
      - `Precision.MINUTES`: Minutes
      - `Precision.SECONDS`: Seconds
      - `Precision.MILLI_SECONDS`: Milliseconds
      - `Precision.MICRO_SECONDS`: Microseconds
      - `Precision.NANO_SECONDS`: Nanoseconds
    - `ttl`: Table expiration time, in days.
    - `reqId`: Optional, used for issue tracking.
  - **Exception**: Throws `TaosResultError` exception if connection fails.

### Parameter Binding

- `async stmtInit(reqId?:number): Promise<WsStmt>`
  - **Interface Description** Create a stmt object using a WsSql object.
  - **Parameter Description**:
    - `reqId`: Request id is optional, used for issue tracking.
  - **Return Value**: stmt object.
  - **Exception**: Throws `TDWebSocketClientError` if connection fails.
- `async prepare(sql: string): Promise<void>`
  - **Interface Description** Bind a precompiled SQL statement.
  - **Parameter Description**:
    - `sql`: Precompiled SQL statement.
  - **Exception**: Throws `TDWebSocketClientError` if connection fails.
- `async setTableName(tableName: string): Promise<void>`
  - **Interface Description** Set the table name for data to be written to.
  - **Parameter Description**:
    - `tableName`: Table name, to specify a database, use: `db_name.table_name`.
  - **Exception**: Throws `TDWebSocketClientError` if connection fails.
  Set binding data through StmtBindParams object.
- `setBoolean(params :any[])`
  - **Interface Description** Set boolean values.
  - **Parameter Description**:
    - `params`: List of boolean types.
  - **Exception**: Throws `TDWebSocketClientError` if connection fails.
- The following interfaces are similar to setBoolean except for the type of value to be set:
  - `setTinyInt(params :any[])`
  - `setUTinyInt(params :any[])`
  - `setSmallInt(params :any[])`
  - `setUSmallInt(params :any[])`
  - `setInt(params :any[])`
  - `setUInt(params :any[])`
  - `setBigint(params :any[])`
  - `setUBigint(params :any[])`
  - `setFloat(params :any[])`
  - `setDouble(params :any[])`
  - `setVarchar(params :any[])`
  - `setBinary(params :any[])`
  - `setNchar(params :any[])`
  - `setJson(params :any[])`
  - `setVarBinary(params :any[])`
  - `setGeometry(params :any[])`
  - `setTimestamp(params :any[])`
- `async setTags(paramsArray:StmtBindParams): Promise<void>`
  - **Interface Description** Set table Tags data for automatic table creation.
  - **Parameter Description**:
    - `paramsArray`: Tags data.
  - **Exception**: Throws `TDWebSocketClientError` if connection fails.
- `async bind(paramsArray:StmtBindParams): Promise<void>`
  - **Interface Description** Bind data.
  - **Parameter Description**:
    - `paramsArray`: Binding data.
  - **Exception**: Throws `TDWebSocketClientError` if connection fails.
- `async batch(): Promise<void>`
  - **Interface Description** Submit bound data.
  - **Exception**: Throws `TDWebSocketClientError` if connection fails.
- `async exec(): Promise<void>`
  - **Interface Description** Execute all bound data to be written.
  - **Exception**: Throws `TDWebSocketClientError` if connection fails.
- `getLastAffected()`
  - **Interface Description** Get the number of writes.
  - **Return Value**: Number of writes.
- `async close(): Promise<void>`
  - **Interface Description** Close the stmt object.
  - **Exception**: Throws `TDWebSocketClientError` if connection fails.

### Data Subscription

- **List of Supported Properties for Creating Consumers**:
  - taos.TMQConstants.CONNECT_USER: Username.
  - taos.TMQConstants.CONNECT_PASS: Password.
  - taos.TMQConstants.GROUP_ID: Group ID.
  - taos.TMQConstants.CLIENT_ID: Client ID.
  - taos.TMQConstants.WS_URL: URL address of taosAdapter.
  - taos.TMQConstants.AUTO_OFFSET_RESET: Determines whether to consume the latest data (latest) or include old data (earliest).
  - taos.TMQConstants.ENABLE_AUTO_COMMIT: Whether to enable auto commit.
  - taos.TMQConstants.AUTO_COMMIT_INTERVAL_MS: Auto commit interval.
  - taos.TMQConstants.CONNECT_MESSAGE_TIMEOUT: Data transfer timeout parameter, in ms, default is 10000 ms.
- `static async newConsumer(wsConfig:Map<string, any>):Promise<WsConsumer>`
  - **Interface Description** Consumer constructor.
  - **Parameter Description**:
    - `wsConfig`: Consumer property configuration.
  - **Return Value**: WsConsumer object.
  - **Exception**: Throws `TDWebSocketClientError` error if an exception occurs during execution.
- `async subscribe(topics: Array<string>, reqId?:number): Promise<void>`
  - **Interface Description** Subscribe to a set of topics.
  - **Parameter Description**:
    - `topics`: List of topics to subscribe.
    - `reqId`: Request ID, optional, used for issue tracking.
  - **Exception**: Throws `TDWebSocketClientError` exception on failure.
- `async unsubscribe(reqId?:number): Promise<void>`
  - **Interface Description** Unsubscribe.
  - **Parameter Description**:
    - `reqId`: Request ID, optional, used for issue tracking.
  - **Exception**: Throws `TDWebSocketClientError` exception on failure.
- `async poll(timeoutMs: number, reqId?:number):Promise<Map<string, TaosResult>>`
  - **Interface Description** Poll messages.
  - **Parameter Description**:
    - `timeoutMs`: Polling timeout in milliseconds.
    - `reqId`: Request ID, optional, used for issue tracking.
  - **Return Value**: `Map<string, TaosResult>` Data corresponding to each topic.
  - **Exception**: Throws `TDWebSocketClientError` exception on failure.
- `async subscription(reqId?:number):Promise<Array<string>>`
  - **Interface Description** Get all currently subscribed topics.
  - **Parameter Description**:
    - `reqId`: Request ID, optional, used for issue tracking.
  - **Return Value**: `Array<string>` List of topics.
  - **Exception**: Throws `TDWebSocketClientError` exception on failure.
- `async commit(reqId?:number):Promise<Array<TopicPartition>>`
  - **Interface Description** Commit the offset of the messages currently being processed.
  - **Parameter Description**:
    - `reqId`: Request ID, optional, used for issue tracking.
  - **Return Value**: `Array<TopicPartition>` Consumption progress for each topic.
  - **Exception**: Throws `TDWebSocketClientError` exception on failure.
- `async committed(partitions:Array<TopicPartition>, reqId?:number):Promise<Array<TopicPartition>>`
  - **Interface Description**: Get the last committed offsets for a set of partitions.
  - **Parameter Description**:
    - `partitions`: An `Array<TopicPartition>` parameter, representing the set of partitions to query.
    - `reqId`: Request ID, optional, used for issue tracking.
  - **Return Value**: `Array<TopicPartition>`, the last committed offsets for a set of partitions.
  - **Exception**: Throws `TDWebSocketClientError` exception if an error occurs while retrieving committed offsets.
- `async seek(partition:TopicPartition, reqId?:number):Promise<void>`
  - **Interface Description**: Set the offset for a given partition to a specified position.
  - **Parameter Description**:
    - `partition`: A `TopicPartition` parameter, representing the partition and the offset to set.
    - `reqId`: Request ID, optional, used for issue tracking.
  - **Exception**: Throws `TDWebSocketClientError` exception if an error occurs while setting the offset.
- `async positions(partitions:Array<TopicPartition>, reqId?:number):Promise<Array<TopicPartition>>`
  - **Interface Description**: Get the current offsets for a given set of partitions.
  - **Parameter Description**:
    - `partitions`: An `Array<TopicPartition>` parameter, representing the partitions to query.
    - `reqId`: Request ID, optional, used for issue tracking.
  - **Return Value**: `Array<TopicPartition>`, the current offsets for a set of partitions.
  - **Exception**: Throws `TDWebSocketClientError` exception if an error occurs while retrieving offsets.
- `async seekToBeginning(partitions:Array<TopicPartition>):Promise<void>`
  - **Interface Description**: Set the offset for a set of partitions to the earliest offset.
  - **Parameter Description**:
    - `partitions`: An `Array<TopicPartition>` parameter, representing the set of partitions to operate on.
  - **Exception**: Throws `TDWebSocketClientError` exception if an error occurs while setting the offset.
- `async seekToEnd(partitions:Array<TopicPartition>):Promise<void>`
  - **Interface Description**: Set the offset for a set of partitions to the latest offset.
  - **Parameter Description**:
    - `partitions`: An `Array<TopicPartition>` parameter, representing the set of partitions to operate on.
  - **Exception**: Throws `TDWebSocketClientError` exception if an error occurs while setting the offset.
- `async assignment(topics?:string[]):Promise<Array<TopicPartition>>`
  - **Interface Description**: Get the currently assigned partitions for the consumer, either specified or all.
  - **Parameter Description**:
    - `topics`: Partitions to retrieve (optional), if not specified, retrieves all partitions.
  - **Return Value**: `Array<TopicPartition>`, the currently assigned partitions for the consumer.
  - **Exception**: Throws `TDWebSocketClientError` exception if an error occurs while retrieving assigned partitions.
- `async close():Promise<void>`
  - **Interface Description**: Close the TMQ connection.
  - **Exception**: Throws `TDWebSocketClientError` exception on failure.
