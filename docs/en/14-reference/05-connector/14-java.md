---
toc_max_heading_level: 4
sidebar_label: Java
title: Java Client Library
slug: /tdengine-reference/client-libraries/java
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import RequestId from "../../assets/resources/_request_id.mdx";

`taos-jdbcdriver` is the official Java connector for TDengine, allowing Java developers to develop applications that access the TDengine database. `taos-jdbcdriver` implements the interfaces of the JDBC driver standard.

:::info
The JDBC driver implementation for TDengine strives to be consistent with relational database drivers. However, due to differences in use cases and technical features between TDengine and relational object databases, there are some differences between `taos-jdbcdriver` and traditional JDBC drivers. Please note the following when using it:

- TDengine currently does not support deletion operations for individual data records.
- Transaction operations are not currently supported.

:::

## JDBC and JRE Version Compatibility

- JDBC: Supports JDBC 4.2 and above.
- JRE: Supports JRE 8 and above.

## Supported Platforms

- Native connection supports the same platforms as the TDengine client driver.
- WebSocket/REST connection supports all platforms that can run Java.

## Version History

| taos-jdbcdriver Version | Major Changes                                                                                                                                                                                                                                                                                                                                                                                               | TDengine Version   |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------ |
| 3.7.8                   | Fixed the bug that the `getTables` method requires an identifier quote string. | -                  |
| 3.7.7                   | 1. Fixed the issue of loading configuration files on the Windows platform. <br/> 2. Fixed the problem of mutual interference between WebSocket connection Statement timeout settings | -                  |
| 3.7.6                   | Optimized WebSocket connection load balancing and the `setObject` method for parameter binding. | -                  |
| 3.7.5                   | 1. WebSocket connections support load balancing. <br/> 2. Automatic reconnection supports parameter binding. <br/> 3. Optimized serialization performance for efficient writing. <br/> 4. Improved the performance of WebSocket connection isValid.   |-|                                                                                                                                                                                                                                                                                                                                  | -                  |
| 3.7.3                   | Optimized WebSocket/Native query implementations.    |-|                                                                                                                                                                                                                                                                                                                                  | -                  |
| 3.7.2                   | Fixed the supportsBatchUpdates issue that caused poor performance in Spring JdbcTemplate parameter binding.    |-|                                                                                                                                                                                                                                                                                                                                  | -                  |
| 3.7.1                   | 1. Replace Java-WebSocket library with Netty to enhance small query performance. <br/>  2. Add IPv6 protocol compatibility. <br/>  3. Implement BLOB (Binary Large Object) data type support. <br/>  4. Enable TDengine version compatibility checks. <br/>  5. Support `varcharAsString` in connection property. <br/>  6. Optimize memory utilization in WebSocket query operations. <br/>  7. Fix timezone handling in WebSocket connections. <br/> |-|                                                                                                                                                                                                                                                                                                                                  | -                  |
| 3.6.3                   | Fixed data type conversion bug in database or super table subscription.                                                                                                                                                                                                                                                                                                                                     | -                  |
| 3.6.2                   | 1. Supports data subscription for databases and super tables (subscription meta not supported). <br/> 1. Resolved the bug in cloud service subscription. <br/> 1. Improved the implement of setQueryTimeout with param 0.                                                                                                                                                                                   | -                  |
| 3.6.1                   | Fixed the performance issue of small queries in WebSocket connection.                                                                                                                                                                                                                                                                                                                                       | -                  |
| 3.6.0                   | Support efficient writing and decimal data types in WebSocket connections.                                                                                                                                                                                                                                                                                                                                  | 3.3.6.0 and higher |
| 3.5.3                   | Support unsigned data types in WebSocket connections.                                                                                                                                                                                                                                                                                                                                                       | -                  |
| 3.5.2                   | Fixed WebSocket result set free bug.                                                                                                                                                                                                                                                                                                                                                                        | -                  |
| 3.5.1                   | Fixed the getObject issue in data subscription.                                                                                                                                                                                                                                                                                                                                                             | -                  |
| 3.5.0                   | 1. Optimized the performance of WebSocket connection parameter binding, supporting parameter binding queries using binary data. <br/> 1. Optimized the performance of small queries in WebSocket connection. <br/> 1. Added support for setting time zone and app info on WebSocket connection.                                                                                                             | 3.3.5.0 and higher |
| 3.4.0                   | 1. Replaced fastjson library with jackson. <br/> 1. WebSocket uses a separate protocol identifier. <br/> 1. Optimized background thread usage to avoid user misuse leading to timeouts.                                                                                                                                                                                                                     | -                  |
| 3.3.4                   | Fixed getInt error when data type is float.                                                                                                                                                                                                                                                                                                                                                                 | -                  |
| 3.3.3                   | Fixed memory leak caused by closing WebSocket statement.                                                                                                                                                                                                                                                                                                                                                    | -                  |
| 3.3.2                   | 1. Optimized parameter binding performance under WebSocket connection. <br/> 1. Improved support for mybatis.                                                                                                                                                                                                                                                                                               | -                  |
| 3.3.0                   | 1. Optimized data transmission performance under WebSocket connection. <br/> 1. Supports skipping SSL verification, off by default.                                                                                                                                                                                                                                                                         | 3.3.2.0 and higher |
| 3.2.11                  | Fixed a bug in closing result set in Native connection.                                                                                                                                                                                                                                                                                                                                                     | -                  |
| 3.2.10                  | 1. REST/WebSocket connections support data compression during transmission. <br/> 1. WebSocket automatic reconnection mechanism, off by default. <br/> 1. Connection class provides methods for schemaless writing. <br/> 1. Optimized data fetching performance for native connections. <br/> 1. Fixed some known issues.  <br/> 1. Metadata retrieval functions can return a list of supported functions. | -                  |
| 3.2.9                   | Fixed bug in closing WebSocket prepareStatement.                                                                                                                                                                                                                                                                                                                                                            | -                  |
| 3.2.8                   | 1. Optimized auto-commit. <br/> 1. Fixed manual commit bug in WebSocket. <br/> 1. Optimized WebSocket prepareStatement using a single connection. <br/> 1. Metadata supports views.                                                                                                                                                                                                                         | -                  |
| 3.2.7                   | 1. Supports VARBINARY and GEOMETRY types. <br/> 1. Added timezone setting support for native connections. <br/> 1. Added WebSocket automatic reconnection feature.                                                                                                                                                                                                                                          | 3.2.0.0 and higher |
| 3.2.5                   | Data subscription adds committed() and assignment() methods.                                                                                                                                                                                                                                                                                                                                                | 3.1.0.3 and higher |
| 3.2.4                   | Data subscription adds enable.auto.commit parameter under WebSocket connection, as well as unsubscribe() method.                                                                                                                                                                                                                                                                                            | -                  |
| 3.2.3                   | Fixed ResultSet data parsing failure in some cases.                                                                                                                                                                                                                                                                                                                                                         | -                  |
| 3.2.2                   | New feature: Data subscription supports seek function.                                                                                                                                                                                                                                                                                                                                                      | 3.0.5.0 and higher |
| 3.2.1                   | 1. WebSocket connection supports schemaless and prepareStatement writing. <br/> 1. Consumer poll returns result set as ConsumerRecord, which can be accessed through value() method.                                                                                                                                                                                                                        | 3.0.3.0 and higher |
| 3.2.0                   | Connection issues, not recommended for use.                                                                                                                                                                                                                                                                                                                                                                 | -                  |
| 3.1.0                   | WebSocket connection supports subscription function.                                                                                                                                                                                                                                                                                                                                                        | -                  |
| 3.0.1 - 3.0.4           | Fixed data parsing errors in result sets under some conditions. 3.0.1 compiled in JDK 11 environment, other versions recommended for JDK 8.                                                                                                                                                                                                                                                                 | -                  |
| 3.0.0                   | Supports TDengine 3.0                                                                                                                                                                                                                                                                                                                                                                                       | 3.0.0.0 and higher |
| 2.0.42                  | Fixed wasNull interface return value in WebSocket connection.                                                                                                                                                                                                                                                                                                                                               | -                  |
| 2.0.41                  | Fixed username and password encoding method in REST connection.                                                                                                                                                                                                                                                                                                                                             | -                  |
| 2.0.39 - 2.0.40         | Added REST connection/request timeout settings.                                                                                                                                                                                                                                                                                                                                                             | -                  |
| 2.0.38                  | JDBC REST connection adds batch fetching function.                                                                                                                                                                                                                                                                                                                                                          | -                  |
| 2.0.37                  | Added support for json tag.                                                                                                                                                                                                                                                                                                                                                                                 | -                  |
| 2.0.36                  | Added support for schemaless writing.                                                                                                                                                                                                                                                                                                                                                                       | -                  |

## Exceptions and Error Codes

After an error occurs, the error information and error code can be obtained through SQLException:

```java
{{#include docs/examples/java/src/main/java/com/taos/example/JdbcBasicDemo.java:jdbc_exception}}
```

For error code information please refer to [Error Codes](../../error-codes/)

## Data Type Mapping

TDengine currently supports timestamp, numeric, character, boolean types, and the corresponding Java type conversions are as follows:

| TDengine DataType | JDBCType             | Remark                                                                                                     |
| ----------------- | -------------------- | ---------------------------------------------------------------------------------------------------------- |
| TIMESTAMP         | java.sql.Timestamp   |                                                                                                            |
| BOOL              | java.lang.Boolean    |                                                                                                            |
| TINYINT           | java.lang.Byte       |                                                                                                            |
| TINYINT UNSIGNED  | java.lang.Short      | only supported in WebSocket connections                                                                    |
| SMALLINT          | java.lang.Short      |                                                                                                            |
| SMALLINT UNSIGNED | java.lang.Integer    | only supported in WebSocket connections                                                                    |
| INT               | java.lang.Integer    |                                                                                                            |
| INT UNSIGNED      | java.lang.Long       | only supported in WebSocket connections                                                                    |
| BIGINT            | java.lang.Long       |                                                                                                            |
| BIGINT UNSIGNED   | java.math.BigInteger | only supported in WebSocket connections                                                                    |
| FLOAT             | java.lang.Float      |                                                                                                            |
| DOUBLE            | java.lang.Double     |                                                                                                            |
| VARCHAR/BINARY    | byte[]               | Setting the `varcharAsString` parameter to `true` on a WebSocket connection can map it to the String type. |
| NCHAR             | java.lang.String     |                                                                                                            |
| JSON              | java.lang.String     | only supported in tags                                                                                     |
| VARBINARY         | byte[]               |                                                                                                            |
| GEOMETRY          | byte[]               |                                                                                                            |
| BLOB              | byte[]               | only supported in columns                                                                                  |
| DECIMAL           | java.math.BigDecimal | only supported in WebSocket connections                                                                    |

**Note**: Due to historical reasons, the BINARY type in TDengine is not truly binary data and is no longer recommended. Please use VARBINARY type instead.  
GEOMETRY type is binary data in little endian byte order, complying with the WKB standard. For more details, please refer to [Data Types](../../sql-manual/data-types/)  
For the WKB standard, please refer to [Well-Known Binary (WKB)](https://libgeos.org/specifications/wkb/)  
For the Java connector, you can use the jts library to conveniently create GEOMETRY type objects, serialize them, and write to TDengine. Here is an example [Geometry Example](https://github.com/taosdata/TDengine/blob/main/docs/examples/java/src/main/java/com/taos/example/GeometryDemo.java)  

## Example Programs Summary

The source code for the example programs is located in `TDengine/docs/examples/JDBC`:

- JDBCDemo: JDBC example source program.
- connectionPools: Using taos-jdbcdriver in connection pools like HikariCP, Druid, dbcp, c3p0.
- SpringJdbcTemplate: Using taos-jdbcdriver in Spring JdbcTemplate.
- mybatisplus-demo: Using taos-jdbcdriver in Springboot + Mybatis.
- springbootdemo: Using taos-jdbcdriver in Springboot.
- consumer-demo: Consumer example consuming TDengine data, with controllable consumption speed through parameters.

Please refer to: [JDBC example](https://github.com/taosdata/TDengine/tree/main/docs/examples/JDBC)

## Frequently Asked Questions

1. Why is there no performance improvement when using Statement's `addBatch()` and `executeBatch()` for "batch writing/updating"?

**Reason**: In TDengine's JDBC implementation, SQL statements submitted through the `addBatch` method are executed in the order they were added, which does not reduce the number of interactions with the server and does not lead to performance improvements.

**Solutions**: 1. Concatenate multiple values in one insert statement; 2. Use multi-threading for concurrent insertion; 3. Use parameter binding for writing

1. java.lang.UnsatisfiedLinkError: no taos in java.library.path

**Reason**: The program cannot find the required local function library taos.

**Solution**: On Windows, you can copy C:\TDengine\driver\taos.dll to the C:\Windows\System32\ directory. On Linux, create the following symlink `ln -s /usr/local/taos/driver/libtaos.so.x.x.x.x /usr/lib/libtaos.so`. On macOS, create a symlink `ln -s /usr/local/lib/libtaos.dylib`.

1. java.lang.UnsatisfiedLinkError: taos.dll Can't load AMD 64 bit on a IA 32-bit platform

**Reason**: TDengine currently only supports 64-bit JDK.

**Solution**: Reinstall 64-bit JDK.

1. java.lang.NoSuchMethodError: setByteArray

**Reason**: taos-jdbcdriver version 3.* only supports TDengine version 3.0 and above.

**Solution**: Use taos-jdbcdriver version 2.*to connect to TDengine version 2.*.

1. java.lang.NoSuchMethodError: java.nio.ByteBuffer.position(I)Ljava/nio/ByteBuffer; ... taos-jdbcdriver-3.0.1.jar

**Reason**: taos-jdbcdriver version 3.0.1 requires JDK 11+ environment.

**Solution**: Switch to taos-jdbcdriver version 3.0.2+.

For other issues, please refer to [FAQ](../../../frequently-asked-questions/)

## API Reference

### JDBC Driver

taos-jdbcdriver implements the JDBC standard Driver interface, providing 3 implementation classes.

- Use `com.taosdata.jdbc.ws.WebSocketDriver` for WebSocket connections.
- Use `com.taosdata.jdbc.TSDBDriver` for native connections.
- Use `com.taosdata.jdbc.rs.RestfulDriver` for REST connections.

#### URL Specification

The JDBC URL format for TDengine is:
`jdbc:[TAOS|TAOS-WS|TAOS-RS]://[host1:port1,host2:port2,...,hostN:portN]/[database_name]?[user={user}|&password={password}|&charset={charset}|&cfgdir={config_dir}|&locale={locale}|&timezone={timezone}|&batchfetch={batchfetch}]`

- The host parameter supports valid domain names or IP addresses. The taos-jdbcdriver supports both IPv4 and IPv6 formats. For IPv6 addresses, square brackets must be used (e.g., `[::1]` or `[2001:db8:1234:5678::1]`) to avoid port number parsing conflicts.  
- **Only the WebSocket connection method supports multiple endpoint addresses**, which should be separated by commas when used. These multiple endpoint addresses will be randomly used during connection to achieve load balancing.
- All properties in **Properties** are supported in the JDBC URL. For details, please refer to the **Properties** section below.   

**Native Connection**  
`jdbc:TAOS://taosdemo.com:6030/power?user=root&password=taosdata`, using the TSDBDriver for native JDBC connection, establishes a connection to the host taosdemo.com, port 6030 (TDengine's default port), and database name power. This URL specifies the username (user) as root and the password (password) as taosdata.

**Note**: For native JDBC connections, taos-jdbcdriver depends on the client driver (libtaos.so on Linux; taos.dll on Windows; libtaos.dylib on macOS).

The supported common configuration parameters for native connection URLs are as follows:

- user: TDengine username, default 'root'.
- password: User login password, default 'taosdata'.
- cfgdir: Client configuration file directory path, default `/etc/taos` on Linux OS, `C:/TDengine/cfg` on Windows OS.
- charset: Character set used by the client, default is the system character set.
- locale: Client language environment, default is the system current locale.
- timezone: Client timezone, default is the system current timezone.
- batchErrorIgnore: true: Continue executing subsequent SQL statements if one fails during the execution of Statement's executeBatch. False: Do not execute any statements after a failed SQL. Default is false.

Using TDengine Client Driver Configuration File to Establish Connection:

When connecting to a TDengine cluster using a native JDBC connection, you can use the TDengine client driver configuration file, specifying parameters such as firstEp and secondEp in the configuration file. In this case, do not specify `host` and `port` in the JDBC URL.
Configuration such as `jdbc:TAOS://:/power?user=root&password=taosdata`.
In the TDengine client driver configuration file, specify firstEp and secondEp, and JDBC will use the client's configuration file to establish a connection. If the firstEp node in the cluster fails, JDBC will attempt to connect to the cluster using secondEp.
In TDengine, as long as one of the nodes in firstEp and secondEp is valid, a connection to the cluster can be established normally.

> **Note**: The configuration file here refers to the configuration file on the machine where the application calling the JDBC Connector is located, with the default value on Linux OS being /etc/taos/taos.cfg, and on Windows OS being C://TDengine/cfg/taos.cfg.

**WebSocket Connection**  
Using JDBC WebSocket connection does not depend on the client driver. Here's an example: `jdbc:TAOS-WS://taosdemo.com:6041,taosdemo2.com:6041/power?user=root&password=taosdata&varcharAsString=true`. Compared to native JDBC connections, you only need to:

1. Specify driverClass as "com.taosdata.jdbc.ws.WebSocketDriver";
2. Start jdbcUrl with "jdbc:TAOS-WS://";
3. Use 6041 as the connection port.
4. It supports configuring multiple endpoints, which are randomly selected during connection to achieve load balancing.

For WebSocket connections, the common configuration parameters in the URL are as follows:

- user: Login username for TDengine, default value 'root'.
- password: User login password, default value 'taosdata'.
- batchErrorIgnore: true: Continues executing the following SQL if one SQL fails during the execution of Statement's executeBatch. false: Does not execute any statements after a failed SQL. Default value: false.
- httpConnectTimeout: Connection timeout in ms, default value 60000.
- messageWaitTimeout: Message timeout in ms, default value 60000.
- useSSL: Whether SSL is used in the connection.
- timezone: Client timezone, default is the system current timezone. Recommended not to set, using the system time zone provides better performance.
- varcharAsString: Maps VARCHAR/BINARY types to String. Effective only when using WebSocket connections. Default value is false.
- conmode: BI mode takes effect only when the WebSocket connection is established. The default value is 0, and it can be set to 1. Setting it to 1 means enabling BI mode, where metadata information does not count sub-tables. This is mainly used in scenarios when integration with BI tools.

**Note**: Some configuration items (such as: locale, charset) do not take effect in WebSocket connections. WebSocket connections only support the UTF-8 character set.

**REST Connection**  

:::warning
It is no longer recommended to use this connection. Please use a WebSocket connection instead.
:::

Using JDBC REST connection does not depend on the client driver. Compared to native JDBC connections, you only need to:

1. Specify driverClass as "com.taosdata.jdbc.rs.RestfulDriver";
2. Start jdbcUrl with "jdbc:TAOS-RS://";
3. Use 6041 as the connection port.

For REST connections, the common configuration parameters in the URL are as follows:

- user: Login username for TDengine, default value 'root'.
- password: User login password, default value 'taosdata'.
- batchErrorIgnore: true: Continues executing the following SQL if one SQL fails during the execution of Statement's executeBatch. false: Does not execute any statements after a failed SQL. Default value: false.
- httpConnectTimeout: Connection timeout in ms, default value 60000.
- httpSocketTimeout: Socket timeout in ms, default value 60000.
- useSSL: Whether SSL is used in the connection.
- httpPoolSize: REST concurrent request size, default 20.

**Note**: Some configuration items (such as: locale, charset and timezone) do not take effect in REST connections.

:::note

- Unlike native connection methods, the REST interface is stateless. When using JDBC REST connections, you need to specify the database name of the table or supertable in SQL.
- If dbname is specified in the URL, then JDBC REST connection will default to using /rest/sql/dbname as the restful request URL, and there is no need to specify dbname in SQL. For example: if the URL is jdbc:TAOS-RS://127.0.0.1:6041/power, then you can execute sql: `INSERT INTO d1001 USING meters TAGS(2,'California.SanFrancisco') VALUES (NOW, 10.30000, 219, 0.31000)`;  

:::

#### Properties

In addition to obtaining a connection through a specified URL, you can also use Properties to specify parameters when establishing a connection.   
All configuration parameters in Properties can also be specified in the JDBC URL. The parameter names in square brackets can be used in the JDBC URL (e.g., TSDBDriver.PROPERTY_KEY_USER[`user`] can be set in the JDBC URL as `user=root` to specify the username).

> **Note**: The client parameter set in the application is at the process level, meaning if you need to update the client's parameters, you must restart the application. This is because the client parameter is a global parameter and only takes effect the first time it is set in the application.

The configuration parameters in properties are as follows:

- TSDBDriver.PROPERTY_KEY_USER [`user`]: Login username for TDengine, default value 'root'.
- TSDBDriver.PROPERTY_KEY_PASSWORD [`password`]: User login password, default value 'taosdata'.
- TSDBDriver.PROPERTY_KEY_BATCH_LOAD [`batchfetch`]: true: Fetch result sets in batches during query execution; false: Fetch result sets row by row. The default value is false. For historical reasons, when using a REST connection, setting this parameter to true will switch to a WebSocket connection.
- TSDBDriver.PROPERTY_KEY_BATCH_ERROR_IGNORE [`batchErrorIgnore`]: true: Continue executing subsequent SQLs when one SQL fails during the execution of Statement's executeBatch; false: Do not execute any statements after a failed SQL. The default value is false.
- TSDBDriver.PROPERTY_KEY_CONFIG_DIR [`cfgdir`]: Effective only when using native JDBC connections. Client configuration file directory path, default value on Linux OS is `/etc/taos`, on Windows OS is `C:/TDengine/cfg`.
- TSDBDriver.PROPERTY_KEY_CHARSET [`charset`]: Character set used by the client, default value is the system character set.
- TSDBDriver.PROPERTY_KEY_LOCALE [`locale`]: Effective only when using native JDBC connections. Client locale, default value is the current system locale.
- TSDBDriver.PROPERTY_KEY_TIME_ZONE [`timezone`]:
  - Native connections: Client time zone, default value is the current system time zone. Effective globally. Due to historical reasons, we only support part of the POSIX standard, such as UTC-8 (representing Shanghai, China), GMT-8, Asia/Shanghai.
  - WebSocket connections: Client time zone, default value is the current system time zone. Effective on the connection. Only IANA time zones are supported, such as Asia/Shanghai. It is recommended not to set this parameter, as using the system time zone provides better performance.
- TSDBDriver.HTTP_CONNECT_TIMEOUT [`httpConnectTimeout`]: Connection timeout, in ms, default value is 60000. Effective only in REST connections.
- TSDBDriver.HTTP_SOCKET_TIMEOUT [`httpSocketTimeout`]: Socket timeout, in ms, default value is 60000. Effective only in REST connections and when batchfetch is set to false.
- TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT [`messageWaitTimeout`]: Message timeout, in ms, default value is 60000. Effective only under WebSocket connections.
- TSDBDriver.PROPERTY_KEY_USE_SSL [`useSSL`]: Whether to use SSL in the connection. Effective only in WebSocket/REST connections.
- TSDBDriver.HTTP_POOL_SIZE [`httpPoolSize`]: REST concurrent request size, default 20.
- TSDBDriver.PROPERTY_KEY_ENABLE_COMPRESSION [`enableCompression`]: Whether to enable compression during transmission. Effective only when using REST/WebSocket connections. true: enabled, false: not enabled. Default is false.
- TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT [`enableAutoReconnect`]: Whether to enable auto-reconnect. Effective only when using WebSocket connections. true: enabled, false: not enabled. Default is false.
  > **Note**: Enabling auto-reconnect is not effective for fetching result sets. Auto-reconnect is only effective for connections established through parameters specifying the database, and ineffective for later `use db` statements to switch databases.

- TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS [`reconnectIntervalMs`]: Auto-reconnect retry interval, in milliseconds, default value 2000. Effective only when PROPERTY_KEY_ENABLE_AUTO_RECONNECT is true.
- TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT [`reconnectRetryCount`]: Auto-reconnect retry count, default value 3, effective only when PROPERTY_KEY_ENABLE_AUTO_RECONNECT is true.
- TSDBDriver.PROPERTY_KEY_DISABLE_SSL_CERT_VALIDATION [`disableSSLCertValidation`]: Disable SSL certificate validation. Effective only when using WebSocket connections. true: enabled, false: not enabled. Default is false.

- TSDBDriver.PROPERTY_KEY_CONNECT_MODE [`conmode`]: BI mode takes effect only when the WebSocket connection is established. The default value is 0, and it can be set to 1. Setting it to 1 means enabling BI mode, where metadata information does not count sub-tables. This is mainly used in scenarios when integration with BI tools.
- TSDBDriver.PROPERTY_KEY_VARCHAR_AS_STRING [`varcharAsString`]: Maps VARCHAR/BINARY types to String. Effective only when using WebSocket connections. Default value is false.
- TSDBDriver.PROPERTY_KEY_APP_NAME [`app_name`]: App name, can be used for display in the `show connections` query result. Effective only when using WebSocket connections. Default value is java.
- TSDBDriver.PROPERTY_KEY_APP_IP [`app_ip`]: App IP, can be used for display in the `show connections` query result. Effective only when using WebSocket connections. Default value is empty.
- TSDBDriver.PROPERTY_KEY_WS_KEEP_ALIVE_SECONDS [`wsKeepAlive`]: The validity period of the WebSocket connection, in seconds. During this period, calling `isValid` will directly return the previous result. The default value is 300.

- TSDBDriver.PROPERTY_KEY_ASYNC_WRITE [`asyncWrite`]: Efficient Writing mode. Currently, only the `stmt` method is supported. Effective only when using WebSocket connections. Default value is empty, meaning Efficient Writing mode is not enabled.
- TSDBDriver.PROPERTY_KEY_BACKEND_WRITE_THREAD_NUM [`backendWriteThreadNum`]: In Efficient Writing mode, this refers to the number of background write threads. Effective only when using WebSocket connections. Default value is 10.
- TSDBDriver.PROPERTY_KEY_BATCH_SIZE_BY_ROW [`batchSizeByRow`]: In Efficient Writing mode, this is the batch size for writing data, measured in rows. Effective only when using WebSocket connections. Default value is 1000.
- TSDBDriver.PROPERTY_KEY_CACHE_SIZE_BY_ROW [`cacheSizeByRow`]: In Efficient Writing mode, this is the cache size, measured in rows. Effective only when using WebSocket connections. Default value is 10000.
- TSDBDriver.PROPERTY_KEY_COPY_DATA [`copyData`]: In Efficient Writing mode, this determines whether to copy the binary data passed by the application through the `addBatch` method. Effective only when using WebSocket connections. Default value is false.
- TSDBDriver.PROPERTY_KEY_STRICT_CHECK [`strictCheck`]: In Efficient Writing mode, this determines whether to validate the length of table names and variable-length data types. Effective only when using WebSocket connections. Default value is false.
- TSDBDriver.PROPERTY_KEY_RETRY_TIMES [`retryTimes`]: In Efficient Writing mode, this is the number of retry attempts for failed write operations. Effective only when using WebSocket connections. Default value is 3.

- TSDBDriver.PROPERTY_KEY_PBS_MODE [`pbsMode`]: Parameter binding serialization mode, currently an experimental feature, only supports `line` mode, which can improve performance when each subtable has only one piece of data in a batch of bound data. Effective only when using WebSocket connections, and not supported in Efficient Writing mode. Default value is empty.

Priority of Configuration Parameters:

When obtaining connections through the three methods mentioned earlier, if configuration parameters are duplicated in the URL, Properties, and client configuration file, the `priority from high to low` is as follows:

1. JDBC URL parameters, as mentioned, can be specified in the parameters of the JDBC URL.
2. Properties connProps
3. When using a native connection, the configuration file taos.cfg of the TDengine client driver

For example: if the password is specified as taosdata in the URL and as taosdemo in the Properties, then JDBC will use the password from the URL to establish the connection.

#### Interface Description

- `Connection connect(String url, java.util.Properties info) throws SQLException`
  - **Interface Description**: Connects to the TDengine database.
  - **Parameter Description**:
    - `url`: Connection address URL, see above for URL specifications.
    - `info`: Connection properties, see above for Properties section.
  - **Return Value**: Connection object.
  - **Exception**: Throws `SQLException` if the connection fails.

- `boolean acceptsURL(String url) throws SQLException`
  - **Interface Description**: Determines whether the driver supports the URL.
  - **Parameter Description**:
    - `url`: Connection address URL.
  - **Return Value**: `true`: supported, `false`: not supported.
  - **Exception**: Throws `SQLException` if the URL is illegal.

- `DriverPropertyInfo[] getPropertyInfo(String url, java.util.Properties info) throws SQLException`
  - **Interface Description**: Retrieves detailed information about all properties that might be needed when attempting to connect to the database. This property information is returned in an array of DriverPropertyInfo objects. Each DriverPropertyInfo object contains detailed information about a database connection property, such as property name, property value, description, etc.
  - **Parameter Description**:
    - `url`: A String parameter representing the database URL.
    - `info`: A java.util.Properties parameter containing a list of properties that might be provided by the user when attempting to connect.
  - **Return Value**: The return type is `DriverPropertyInfo[]`, an array of `DriverPropertyInfo` objects. Each `DriverPropertyInfo` object contains detailed information about a specific database connection property.
  - **Exception**: Throws `SQLException` if a database access error or other error occurs during the retrieval of property information.

- `int getMajorVersion()`
  - **Interface Description**: Gets the major version number of the JDBC driver.

- `int getMinorVersion()`
  - **Interface Description**: Gets the minor version number of the JDBC driver.

### Database Metadata

`DatabaseMetaData` is part of the JDBC API, providing detailed information about the database's metadata, where metadata means data about data. Through the `DatabaseMetaData` interface, you can query detailed information about the database server, such as the database product name, version, installed features, list of tables, views, stored procedures, etc. This is very useful for understanding and adapting to the characteristics of different databases.

- `String getURL() throws SQLException`
  - **Interface Description**: Gets the URL used for connecting to the database.
  - **Return Value**: URL for connecting to the database.
  - **Exception**: Throws `SQLException` if the retrieval fails.

- `String getUserName() throws SQLException`
  - **Interface Description**: Gets the username used for connecting to the database.
  - **Return Value**: Username for connecting to the database.
  - **Exception**: Throws `SQLException` if the retrieval fails.

- `String getDriverName() throws SQLException`
  - **Interface Description**: Gets the name of the JDBC driver.
  - **Return Value**: Driver name string.
  - **Exception**: Throws `SQLException` if the retrieval fails.

- `String getDriverVersion() throws SQLException`
  - **Interface Description**: Get the JDBC driver version.
  - **Return Value**: Driver version string.
  - **Exception**: Throws `SQLException` if the retrieval fails.

- `int getDriverMajorVersion()`
  - **Interface Description**: Get the major version number of the JDBC driver.

- `int getDriverMinorVersion()`
  - **Interface Description**: Get the minor version number of the JDBC driver.

- `String getDatabaseProductName() throws SQLException`
  - **Interface Description**: Get the name of the database product.

- `String getDatabaseProductVersion() throws SQLException`
  - **Interface Description**: Get the version number of the database product.

- `String getIdentifierQuoteString() throws SQLException`
  - **Interface Description**: Get the string used to quote SQL identifiers.

- `String getSQLKeywords() throws SQLException`
  - **Interface Description**: Get a list of SQL keywords specific to the database.

- `String getNumericFunctions() throws SQLException`
  - **Interface Description**: Get a list of numeric function names supported by the database.

- `String getStringFunctions() throws SQLException`
  - **Interface Description**: Get a list of string function names supported by the database.

- `String getSystemFunctions() throws SQLException`
  - **Interface Description**: Get a list of system function names supported by the database.

- `String getTimeDateFunctions() throws SQLException`
  - **Interface Description**: Get a list of time and date function names supported by the database.

- `String getCatalogTerm() throws SQLException`
  - **Interface Description**: Get the term for catalogs in the database.

- `String getCatalogSeparator() throws SQLException`
  - **Interface Description**: Get the separator used between catalog and table names.

- `int getDefaultTransactionIsolation() throws SQLException`
  - **Interface Description**: Get the default transaction isolation level of the database.

- `boolean supportsTransactionIsolationLevel(int level) throws SQLException`
  - **Interface Description**: Determine if the database supports the given transaction isolation level.
  - **Parameter Description**:
    - `level`: Transaction isolation level.
  - **Return Value**: `true`: supported, `false`: not supported.
  - **Exception**: Throws `SQLException` if the operation fails.

- `ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException`
  - **Interface Description**: Get information about tables in the database that match the specified pattern.
  - **Parameter Description**:
    - `catalog`: Catalog name; `null` means no catalog specified.
    - `schemaPattern`: Pattern for the schema name; `null` means no schema specified.
    - `tableNamePattern`: Pattern for the table name.
    - `types`: List of table types, returns tables of specified types.
  - **Return Value**: `ResultSet` containing table information.
  - **Exception**: Throws `SQLException` if the operation fails.

- `ResultSet getCatalogs() throws SQLException`
  - **Interface Description**: Get information about all catalogs in the database.
  - **Return Value**: `ResultSet` containing catalog information.
  - **Exception**: Throws `SQLException` if the operation fails.

- `ResultSet getTableTypes() throws SQLException`
  - **Interface Description**: Get the types of tables supported by the database.
  - **Return Value**: `ResultSet` containing table types.
  - **Exception**: Throws `SQLException` if the operation fails.

- `ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException`
  - **Interface Description**: Retrieves column information matching specified patterns within a specified table.
  - **Parameter Description**:
    - `catalog`: Catalog name; `null` means no catalog specified.
    - `schemaPattern`: Pattern for the schema name; `null` means no schema specified.
    - `tableNamePattern`: Pattern for the table name.
    - `columnNamePattern`: Pattern for the column name.
  - **Return Value**: `ResultSet` containing column information.
  - **Exception**: Throws `SQLException` if operation fails.

- `ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException`
  - **Interface Description**: Retrieves primary key information for a specified table.
  - **Parameter Description**:
    - `catalog`: Catalog name; `null` means no catalog specified.
    - `schema`: Schema name; `null` means no schema specified.
    - `table`: Table name.
  - **Return Value**: `ResultSet` containing primary key information.
  - **Exception**: Throws `SQLException` if operation fails.

- `Connection getConnection() throws SQLException`
  - **Interface Description**: Obtains a database connection.
  - **Return Value**: Database connection `Connection` object.
  - **Exception**: Throws `SQLException` if connection retrieval fails.

- `ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException`
  - **Interface Description**: Retrieves parent table information for a specified table.
  - **Parameter Description**:
    - `catalog`: Catalog name; `null` means no catalog specified.
    - `schemaPattern`: Pattern for the schema name; `null` means no schema specified.
    - `tableNamePattern`: Pattern for the table name.
  - **Return Value**: `ResultSet` containing parent table information.
  - **Exception**: Throws `SQLException` if operation fails.

- `boolean supportsResultSetHoldability(int holdability) throws SQLException`
  - **Interface Description**: Determines if the database supports the specified `ResultSet` holdability.
  - **Parameter Description**:
    - `holdability`: Holdability of the `ResultSet`.
  - **Return Value**: `true` if supported, `false` if not.
  - **Exception**: Throws `SQLException` if operation fails.

- `int getSQLStateType() throws SQLException`
  - **Interface Description**: Retrieves the SQLSTATE type used by the database.
  - **Return Value**: SQLSTATE type code.
  - **Exception**: Throws `SQLException` if operation fails.

List of interface methods that return `true` for supported features, others not explicitly mentioned return `false`.

| Interface Method                               | Description                                                                                     |
| ---------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| `boolean nullsAreSortedAtStart()`              | Determines if `NULL` values are sorted at the start                                             |
| `boolean storesLowerCaseIdentifiers()`         | Determines if the database stores identifiers in lowercase                                      |
| `boolean supportsAlterTableWithAddColumn()`    | Determines if the database supports adding columns with `ALTER TABLE`                           |
| `boolean supportsAlterTableWithDropColumn()`   | Determines if the database supports dropping columns with `ALTER TABLE`                         |
| `boolean supportsColumnAliasing()`             | Determines if the database supports column aliasing                                             |
| `boolean supportsGroupBy()`                    | Determines if the database supports `GROUP BY` statements                                       |
| `boolean isCatalogAtStart()`                   | Determines if the catalog name appears at the start of the fully qualified name in the database |
| `boolean supportsCatalogsInDataManipulation()` | Determines if the database supports catalog names in data manipulation statements               |

### Connection Features

The JDBC driver supports creating connections, returning objects that support the JDBC standard `Connection` interface, and also provides the `AbstractConnection` interface, which expands some schema-less write interfaces.

#### Standard Interfaces

- `Statement createStatement() throws SQLException`
  - **Interface Description**: Creates a `Statement` object to execute SQL statements. For a detailed description of the `Statement` interface, see the section below on executing SQL.
  - **Return Value**: The created `Statement` object.
  - **Exception**: Throws `SQLException` if the operation fails.
- `PreparedStatement prepareStatement(String sql) throws SQLException`
  - **Interface Description**: Creates a `PreparedStatement` object to execute the given SQL statement. For a detailed description of the `PreparedStatement` interface, see the section below on executing SQL.
  - **Parameter Description**:
    - `sql`: The precompiled SQL statement.
  - **Return Value**: The created `PreparedStatement` object.
  - **Exception**: Throws `SQLException` if the operation fails.
- `String nativeSQL(String sql) throws SQLException`
  - **Interface Description**: Converts an SQL statement into database-specific SQL syntax.
  - **Parameter Description**:
    - `sql`: The SQL statement to convert.
  - **Return Value**: The converted SQL statement.
  - **Exception**: Throws `SQLException` if the operation fails.
- `void close() throws SQLException`
  - **Interface Description**: Closes the database connection.
  - **Exception**: Throws `SQLException` if the operation fails.
- `boolean isClosed() throws SQLException`
  - **Interface Description**: Checks whether the database connection is closed.
  - **Return Value**: `true`: closed, `false`: not closed.
  - **Exception**: Throws `SQLException` if the operation fails.
- `DatabaseMetaData getMetaData() throws SQLException`
  - **Interface Description**: Retrieves the metadata of the database.
  - **Return Value**: The database metadata.
  - **Exception**: Throws `SQLException` if the operation fails.
- `void setCatalog(String catalog) throws SQLException`
  - **Interface Description**: Sets the default database for the current connection.
  - **Parameter Description**:
    - `catalog`: The name of the database to set.
  - **Exception**: Throws `SQLException` if the operation fails.
- `String getCatalog() throws SQLException`
  - **Interface Description**: Retrieves the default database for the current connection.
  - **Return Value**: The name of the current connection's catalog.
  - **Exception**: Throws `SQLException` if the operation fails.
- `Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException`
  - **Interface Description**: Creates a `Statement` object, specifying the `ResultSet` type and concurrency mode.
  - **Parameter Description**:
    - `resultSetType`: The `ResultSet` type.
    - `resultSetConcurrency`: The concurrency mode.
  - **Return Value**: The created `Statement` object.
  - **Exception**: Throws `SQLException` if the operation fails.
- `Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException`
  - **Interface Description**: Creates a `Statement` object, specifying the `ResultSet` type, concurrency mode, and holdability.
  - **Parameter Description**:
    - `resultSetType`: The `ResultSet` type.
    - `resultSetConcurrency`: The concurrency mode.
    - `resultSetHoldability`: The holdability.
  - **Return Value**: The created `Statement` object.
  - **Exception**: Throws `SQLException` if the operation fails.
- `PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException`
  - **Interface Description**: Creates a `PreparedStatement` object, specifying the SQL, `ResultSet` type, and concurrency mode.
  - **Parameter Description**:
    - `sql`: The precompiled SQL statement.
    - `resultSetType`: The `ResultSet` type.
    - `resultSetConcurrency`: The concurrency mode.
  - **Return Value**: The created `PreparedStatement` object.
  - **Exception**: Throws `SQLException` if the operation fails.
- `PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException`
  - **Interface Description**: Creates a `PreparedStatement` object, specifying the SQL, `ResultSet` type, concurrency mode, and holdability.
  - **Parameter Description**:
    - `sql`: The precompiled SQL statement.
    - `resultSetType`: The `ResultSet` type.
    - `resultSetConcurrency`: The concurrency mode.
    - `resultSetHoldability`: The holdability.
  - **Return Value**: The created `PreparedStatement` object.
  - **Exception**: Throws `SQLException` if the operation fails.
- `PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException`
  - **Interface Description**: Creates a `PreparedStatement` object, specifying the SQL statement and the flag for auto-generated keys.
  - **Parameter Description**:
    - `sql`: The precompiled SQL statement.
    - `autoGeneratedKeys`: The flag indicating whether auto-generated keys should be produced.
  - **Return Value**: The created `PreparedStatement` object.
  - **Exception**: Throws `SQLException` if the operation fails.
- `void setHoldability(int holdability) throws SQLException`
  - **Interface Description**: Sets the default holdability for `ResultSet` objects.
  - **Parameter Description**:
    - `holdability`: The holdability of the `ResultSet`.
  - **Exception**: Throws `SQLException` if the operation fails.
- `int getHoldability() throws SQLException`
  - **Interface Description**: Retrieves the default holdability for `ResultSet` objects.
  - **Return Value**: The holdability of the `ResultSet`.
  - **Exception**: Throws `SQLException` if the operation fails.
- `boolean isValid(int timeout) throws SQLException`
  - **Interface Description**: Checks if the database connection is valid.
  - **Parameter Description**:
    - `timeout`: The timeout in seconds for the validity check.
  - **Return Value**: `true`: connection is valid, `false`: connection is invalid.
  - **Exception**: Throws `SQLException` if the operation fails.
- `void setClientInfo(String name, String value) throws SQLClientInfoException`
  - **Interface Description**: Sets a client information property.
  - **Parameter Description**:
    - `name`: The property name.
    - `value`: The property value.
  - **Exception**: Throws `SQLClientInfoException` if setting fails.
- `void setClientInfo(Properties properties) throws SQLClientInfoException`
  - **Interface Description**: Sets a set of client information properties.
  - **Parameter Description**:
    - `properties`: The properties collection.
  - **Exception**: Throws `SQLClientInfoException` if setting fails.
- `String getClientInfo(String name) throws SQLException`
  - **Interface Description**: Retrieves the value of a specified client information property.
  - **Parameter Description**:
    - `name`: The property name.
  - **Return Value**: The property value.
  - **Exception**: Throws `SQLException` if the operation fails.
- `Properties getClientInfo() throws SQLException`
  - **Interface Description**: Retrieves all client information properties.
  - **Return Value**: A `Properties` object containing all client information properties.
  - **Exception**: Throws `SQLException` if the operation fails.

#### Schemaless Write

Note: The abstract type interfaces below will be implemented by specific implementation classes, so the connection object obtained after establishing a connection can be directly called.

- `abstract void write(String[] lines, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, Integer ttl, Long reqId) throws SQLException`
  - **Interface Description**: Writes multiple lines of data with the specified protocol type, timestamp type, TTL (Time To Live), and request ID.
  - **Parameter Description**:
    - `lines`: Array of data lines to be written.
    - `protocolType`: Protocol type: supports InfluxDB `LINE`, OpenTSDB `TELNET`, OpenTSDB `JSON`.
    - `timestampType`: Timestamp type, supports HOURS, MINUTES, SECONDS, MILLI_SECONDS, MICRO_SECONDS, and NANO_SECONDS.
    - `ttl`: Data's time to live, in days.
    - `reqId`: Request ID.
  - **Exception**: Throws SQLException on operation failure.
- `void write(String[] lines, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType) throws SQLException`
  - **Interface Description**: Writes multiple lines of data with the specified protocol type and timestamp type.
  - **Parameter Description**:
    - `lines`: Array of data lines to be written.
    - `protocolType`: Protocol type: supports InfluxDB `LINE`, OpenTSDB `TELNET`, OpenTSDB `JSON`.
    - `timestampType`: Timestamp type, supports HOURS, MINUTES, SECONDS, MILLI_SECONDS, MICRO_SECONDS, and NANO_SECONDS.
  - **Exception**: Throws SQLException on operation failure.
- `void write(String line, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType) throws SQLException`
  - **Interface Description**: Writes a single line of data with the specified protocol type and timestamp type.
  - **Parameter Description**:
    - `line`: Data line to be written.
    - `protocolType`: Protocol type: supports InfluxDB `LINE`, OpenTSDB `TELNET`, OpenTSDB `JSON`.
    - `timestampType`: Timestamp type, supports HOURS, MINUTES, SECONDS, MILLI_SECONDS, MICRO_SECONDS, and NANO_SECONDS.
  - **Exception**: Throws SQLException on operation failure.
- `void write(List<String> lines, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType) throws SQLException`
  - **Interface Description**: Writes multiple lines of data (using a list) with the specified protocol type and timestamp type.
  - **Parameter Description**:
    - `lines`: List of data lines to be written.
    - `protocolType`: Protocol type: supports InfluxDB `LINE`, OpenTSDB `TELNET`, OpenTSDB `JSON`.
    - `timestampType`: Timestamp type, supports HOURS, MINUTES, SECONDS, MILLI_SECONDS, MICRO_SECONDS, and NANO_SECONDS.
  - **Exception**: Throws SQLException on operation failure.
- `int writeRaw(String line, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType) throws SQLException`
  - **Interface Description**: Writes multiple lines of raw data separated by carriage returns with the specified protocol type and timestamp type, and returns the operation result.
  - **Parameter Description**:
    - `line`: Raw data to be written.
    - `protocolType`: Protocol type: supports InfluxDB `LINE`, OpenTSDB `TELNET`, OpenTSDB `JSON`.
    - `timestampType`: Timestamp type, supports HOURS, MINUTES, SECONDS, MILLI_SECONDS, MICRO_SECONDS, and NANO_SECONDS.
  - **Return Value**: Operation result.
  - **Exception**: Throws SQLException on operation failure.
- `abstract int writeRaw(String line, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, Integer ttl, Long reqId) throws SQLException`
  - **Interface Description**: Writes multiple lines of raw data separated by carriage returns with the specified protocol type, timestamp type, TTL (Time To Live), and request ID, and returns the operation result.
  - **Parameter Description**:
    - `line`: Raw data to be written.
    - `protocolType`: Protocol type: supports InfluxDB `LINE`, OpenTSDB `TELNET`, OpenTSDB `JSON`.
    - `timestampType`: Timestamp type, supports HOURS, MINUTES, SECONDS, MILLI_SECONDS, MICRO_SECONDS, and NANO_SECONDS.
    - `ttl`: Data's time to live, in days.
    - `reqId`: Request ID.
  - **Return Value**: Operation result.
  - **Exception**: Throws SQLException on operation failure.

### Execute SQL

The JDBC driver provides a JDBC-compliant `Statement` interface that supports the following features:

1. **Execute SQL Statements**: The `Statement` interface is primarily used to execute static SQL statements and return the results they generate.
1. **Query Execution**: Can execute queries that return a dataset (`SELECT` statements).
1. **Update Execution**: Can execute SQL statements that affect the number of rows, such as `INSERT`, `UPDATE`, `DELETE`, etc.
1. **Batch Execution**: Supports batch execution of multiple SQL statements to improve application efficiency.
1. **Get Results**: Can obtain the result set (`ResultSet` object) returned after query execution and traverse the returned data.
1. **Get Update Count**: For non-query SQL statements, the number of rows affected after execution can be obtained.
1. **Close Resources**: Provides a method to close the `Statement` object to release database resources.

Additionally, the JDBC driver also provides an extended interface for request link tracking.

#### Standard Interfaces

- `ResultSet executeQuery(String sql) throws SQLException`
  - **Interface Description**: Executes the given SQL statement, which returns a single `ResultSet` object.
  - **Parameter Description**:
    - `sql`: The SQL query statement to execute.
  - **Return Value**: Query result set `ResultSet`.
  - **Exception**: If a database access error or other errors occur during the query, a `SQLException` will be thrown.

- `int executeUpdate(String sql) throws SQLException`
  - **Interface Description**: Executes the given SQL statement, which may be an `INSERT` or `DELETE` statement, or a SQL statement that does not return any content.
  - **Parameter Description**:
    - `sql`: The SQL update statement to execute.
  - **Return Value**: Number of rows affected.
  - **Exception**: If a database access error or other errors occur during the update, a `SQLException` will be thrown.

- `void close() throws SQLException`
  - **Interface Description**: Immediately releases the database and JDBC resources of this `Statement` object.
  - **Exception**: If a database access error or other errors occur during closure, a `SQLException` will be thrown.

- `int getMaxFieldSize() throws SQLException`
  - **Interface Description**: Gets the maximum number of bytes for character and binary column values that can be read in a `ResultSet` object.
  - **Return Value**: Maximum column size.
  - **Exception**: If a database access error or other errors occur during the retrieval, a `SQLException` will be thrown.

- `int getQueryTimeout() throws SQLException`
  - **Interface Description**: Gets the current query timeout of this `Statement` object.
  - **Return Value**: Query timeout in seconds.
  - **Exception**: If a database access error or other errors occur during the retrieval, a `SQLException` will be thrown.

- `void setQueryTimeout(int seconds) throws SQLException`
  - **Interface Description**: Sets the current query timeout of this `Statement` object.
  - **Parameter Description**:
    - `seconds`: Query timeout in seconds.
  - **Exception**: If a database access error or other errors occur during the setting, a `SQLException` will be thrown.

- `boolean execute(String sql) throws SQLException`
  - **Interface Description**: Executes the given SQL statement, which may return multiple results.
  - **Parameter Description**:
    - `sql`: The SQL statement to execute.
  - **Return Value**: `true` indicates that the result is a `ResultSet` object; `false` indicates that the result is an update count or there is no result.
  - **Exception**: If a database access error or other errors occur during execution, a `SQLException` will be thrown.

- `ResultSet getResultSet() throws SQLException`
  - **Interface Description**: Gets the `ResultSet` object generated by the current `Statement` object.
  - **Return Value**: The result set generated by the current `Statement` object.
  - **Exception**: If a database access error or other errors occur during retrieval, a `SQLException` will be thrown.

- `int getUpdateCount() throws SQLException`
  - **Interface Description**: Gets the update count for the current `Statement` object.
  - **Return Value**: The number of rows affected; returns -1 if the current result is a `ResultSet` object or there are no results.
  - **Exception**: Throws a `SQLException` if a database access error or other error occurs during retrieval.

- `boolean getMoreResults() throws SQLException`
  - **Interface Description**: Moves to the next result of the current `Statement` object to check if it is a `ResultSet` object.
  - **Return Value**: `true` if the next result is a `ResultSet` object; `false` if the next result is an update count or there are no more results.
  - **Exception**: Throws a `SQLException` if a database access error or other error occurs during the move.

- `int getFetchDirection() throws SQLException`
  - **Interface Description**: Gets the direction in which the `Statement` object fetches rows from the database.
  - **Return Value**: The direction of fetching rows; TDengine only supports the `FETCH_FORWARD` direction.
  - **Exception**: Throws a `SQLException` if a database access error or other error occurs during retrieval.

- `void setFetchSize(int rows) throws SQLException`
  - **Interface Description**: Advises the JDBC driver how many rows to fetch from the database at a time.
  - **Parameter Description**:
    - `rows`: The number of rows to fetch at a time.
  - **Exception**: Throws a `SQLException` if a database access error or other error occurs during setting.

- `int getFetchSize() throws SQLException`
  - **Interface Description**: Gets the default fetch size for the `Statement` object.
  - **Return Value**: The default fetch size.
  - **Exception**: Throws a `SQLException` if a database access error or other error occurs during retrieval.

- `int getResultSetConcurrency() throws SQLException`
  - **Interface Description**: Gets the concurrency mode of the `ResultSet` object.
  - **Return Value**: The concurrency mode of the `ResultSet` object.
  - **Exception**: Throws a `SQLException` if a database access error or other error occurs during retrieval.

- `int getResultSetType() throws SQLException`
  - **Interface Description**: Gets the type of the `ResultSet` object.
  - **Return Value**: The type of the `ResultSet` object.
  - **Exception**: Throws a `SQLException` if a database access error or other error occurs during retrieval.

- `void addBatch(String sql) throws SQLException`
  - **Interface Description**: Adds the given SQL statement to the current `Statement` object's batch.
  - **Parameter Description**:
    - `sql`: The SQL statement to be added to the batch.
  - **Exception**: Throws a `SQLException` if a database access error or other error occurs during addition.

- `void clearBatch() throws SQLException`
  - **Interface Description**: Clears the batch of the current `Statement` object.
  - **Exception**: Throws a `SQLException` if a database access error or other error occurs during clearing.

- `int[] executeBatch() throws SQLException`
  - **Interface Description**: Executes all SQL statements in the batch.
  - **Return Value**: The number of rows affected by each SQL statement in the batch.
  - **Exception**: Throws a `SQLException` if a database access error or other error occurs during execution.

- `Connection getConnection() throws SQLException`
  - **Interface Description**: Gets the `Connection` object that produced this `Statement` object.
  - **Return Value**: The database connection that produced this `Statement` object.
  - **Exception**: Throws a `SQLException` if a database access error or other error occurs during retrieval.

- `int getResultSetHoldability() throws SQLException`
  - **Interface Description**: Gets the holdability of the `ResultSet` object.
  - **Return Value**: The holdability of the `ResultSet` object.
  - **Exception**: If a database access error or other error occurs, a `SQLException` will be thrown.

- `boolean isClosed() throws SQLException`
  - **Interface Description**: Checks whether this `Statement` object is closed.
  - **Return Value**: `true` if this `Statement` object is closed; `false` if it is not.
  - **Exception**: If a database access error or other error occurs, a `SQLException` will be thrown.

#### Extended Interfaces

Extended interfaces are mainly used for request link tracing.

- `ResultSet executeQuery(String sql, Long reqId) throws SQLException`
  - **Interface Description**: Executes the given SQL query statement.
  - **Parameter Description**:
    - `sql`: The SQL query statement to execute.
    - `reqId`: Request id.
  - **Return Value**: A `ResultSet` object containing the query results.
  - **Exception**: If the query execution fails, a `SQLException` will be thrown.

- `int executeUpdate(String sql, Long reqId) throws SQLException`
  - **Interface Description**: Executes the given SQL update statement.
  - **Parameter Description**:
    - `sql`: The SQL update statement to execute.
    - `reqId`: Request id.
  - **Return Value**: The number of rows affected.
  - **Exception**: If the update execution fails, a `SQLException` will be thrown.

- `boolean execute(String sql, Long reqId) throws SQLException`
  - **Interface Description**: Executes the given SQL statement, which may be an INSERT, UPDATE, DELETE, or DDL statement.
  - **Parameter Description**:
    - `sql`: The SQL statement to execute.
    - `reqId`: Request id.
  - **Return Value**: `true` if the first result is a `ResultSet` object; `false` if it is an update count or there are no results.
  - **Exception**: If the statement execution fails, a `SQLException` will be thrown.

### Result Retrieval

The JDBC driver supports the standard ResultSet interface and the corresponding ResultSetMetaData interface, providing methods for reading metadata and data from the result set.

#### Result Set

The JDBC driver supports the standard ResultSet interface, providing methods for reading metadata and data from the result set.

- `ResultSetMetaData getMetaData() throws SQLException`
  - **Interface Description**: Gets the number, types, and properties of this ResultSet object's columns.
  - **Return Value**: A ResultSetMetaData object for this ResultSet object's data.
  - **Exception**: If a database access error occurs, a SQLException will be thrown.
- `boolean next() throws SQLException`
  - **Interface Description**: Moves the cursor forward one row from its current position. Used for traversing the query result set.
  - **Return Value**: `true` if the new current row is valid; `false` if there are no more rows in the result set.
  - **Exception**: If a database access error occurs, a SQLException will be thrown.
- `void close() throws SQLException`
  - **Interface Description**: Immediately releases this ResultSet object's database and JDBC resources instead of waiting for this object to automatically close when it is closed.
  - **Exception**: If a database access error occurs, a SQLException will be thrown.
- `boolean wasNull() throws SQLException`
  - **Interface Description**: Reports whether the last column read had a NULL value.
  - **Return Value**: `true` if the last column value read was NULL; otherwise, `false`.
  - **Exception**: If a database access error occurs, a SQLException will be thrown.
- `String getString(int columnIndex) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column as a Java String.
  - **Parameter Description**:
    - `columnIndex`: The column number, with the first column being 1, the second being 2, and so on.
  - **Return Value**: The value of the specified column; if the value is NULL, returns null.
  - **Exception**: If a database access error occurs, a SQLException will be thrown.
- `boolean getBoolean(int columnIndex) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column as a Java boolean.
  - **Parameter Description**:
    - `columnIndex`: The column number.
  - **Return Value**: `true` if the specified column's value is true; `false` if the value is false or NULL.
  - **Exception**: If a database access error occurs, a SQLException will be thrown.
- `byte getByte(int columnIndex) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column as a Java byte.
  - **Parameter Description**:
    - `columnIndex`: The column number.
  - **Return Value**: The value of the specified column; if the value is NULL, returns 0.
  - **Exception**: If a database access error occurs, a SQLException will be thrown.
- `short getShort(int columnIndex) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column as a Java short.
  - **Parameter Description**:
    - `columnIndex`: The column number.
  - **Return Value**: The value of the specified column; if the value is NULL, returns 0.
  - **Exception**: If a database access error occurs, a SQLException will be thrown.
- `int getInt(int columnIndex) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column as a Java int.
  - **Parameter Description**:
    - `columnIndex`: The column number.
  - **Return Value**: The value of the specified column; if the value is NULL, returns 0.
  - **Exception**: If a database access error occurs, a SQLException will be thrown.
- `long getLong(int columnIndex) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column as a Java long.
  - **Parameter Description**:
    - `columnIndex`: The column number.
  - **Return Value**: The value of the specified column; if the value is NULL, returns 0L.
  - **Exception**: If a database access error occurs, a SQLException will be thrown.
- `float getFloat(int columnIndex) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column as a Java float.
  - **Parameter Description**:
    - `columnIndex`: The column number.
  - **Return Value**: The value of the specified column; if the value is NULL, returns 0.0f.
  - **Exception**: If a database access error occurs, a SQLException will be thrown.
- `double getDouble(int columnIndex) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column as a Java double.
  - **Parameter Description**:
    - `columnIndex`: The column number.
  - **Return Value**: The value of the specified column; if the value is NULL, returns 0.0d.
  - **Exception**: If a database access error occurs, a SQLException will be thrown.
- `byte[] getBytes(int columnIndex) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column as a byte array.
  - **Parameter Description**:
    - `columnIndex`: The column number.
  - **Return Value**: The value of the specified column as a byte array; if the value is NULL, returns null.
  - **Exception**: If a database access error occurs, a `SQLException` will be thrown.
- `Date getDate(int columnIndex) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column as a `java.sql.Date` object.
  - **Parameter Description**:
    - `columnIndex`: The column number.
  - **Return Value**: The date value of the specified column; if the value is NULL, returns null.
  - **Exception**: If a database access error occurs, a `SQLException` will be thrown.
- `Time getTime(int columnIndex) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column as a `java.sql.Time` object.
  - **Parameter Description**:
    - `columnIndex`: The column number.
  - **Return Value**: The time value of the specified column; if the value is NULL, returns null.
  - **Exception**: If a database access error occurs, a `SQLException` will be thrown.
- `Timestamp getTimestamp(int columnIndex) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column as a `java.sql.Timestamp`.
  - **Parameter Description**:
    - `columnIndex`: The column number.
  - **Return Value**: The timestamp value of the specified column; if the value is NULL, returns null.
  - **Exception**: If a database access error occurs, a `SQLException` will be thrown.
- `String getNString(int columnIndex) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column as a Java String. This method is used to read NCHAR, NVARCHAR, and LONGNVARCHAR types of columns to support international character sets.
  - **Parameter Description**:
    - `columnIndex`: The column number (starting from 1).
  - **Return Value**: The value of the specified column; if the value is NULL, returns null.
  - **Exception**: If a database access error occurs, a `SQLException` will be thrown.
- `Object getObject(int columnIndex) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column as a Java Object.
  - **Parameter Description**:
    - `columnIndex`: The column number.
  - **Return Value**: The value of the specified column; if the value is NULL, returns null.
  - **Exception**: If a database access error occurs, a `SQLException` will be thrown.
- `String getString(String columnLabel) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column name as a Java String.
  - **Parameter Description**:
    - `columnLabel`: The column label name.
  - **Return Value**: The value of the specified column name; if the value is NULL, returns null.
  - **Exception**: If a database access error occurs, a `SQLException` will be thrown.
- `boolean getBoolean(String columnLabel) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column name as a Java boolean.
  - **Parameter Description**:
    - `columnLabel`: The column label name.
  - **Return Value**: `true` if the specified column name's value is true; if the value is false or NULL, returns false.
  - **Exception**: If a database access error occurs, a `SQLException` will be thrown.
- `byte getByte(String columnLabel) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column name as a Java byte.
  - **Parameter Description**:
    - `columnLabel`: The column label name.
  - **Return Value**: The value of the specified column name; if the value is NULL, returns 0.
  - **Exception**: If a database access error occurs, a `SQLException` will be thrown.
- `short getShort(String columnLabel) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column name as a Java short.
  - **Parameter Description**:
    - `columnLabel`: The column label name.
  - **Return Value**: The value of the specified column name; if the value is NULL, returns 0.
  - **Exception**: If a database access error occurs, a `SQLException` will be thrown.
- `int getInt(String columnLabel) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column name as a Java int.
  - **Parameter Description**:
    - `columnLabel`: The column label name.
  - **Return Value**: The value of the specified column name; if the value is NULL, returns 0.
  - **Exception**: If a database access error occurs, a `SQLException` will be thrown.
- `long getLong(String columnLabel) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column name as a Java long.
  - **Parameter Description**:
    - `columnLabel`: The column label name.
  - **Return Value**: The value of the specified column name; if the value is NULL, returns 0L.
  - **Exception**: If a database access error occurs, a `SQLException` will be thrown.
- `float getFloat(String columnLabel) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column name as a Java float.
  - **Parameter Description**:
    - `columnLabel`: The column label name.
  - **Return Value**: The value of the specified column name; if the value is NULL, returns 0.0f.
  - **Exception**: If a database access error occurs, a `SQLException` will be thrown.
- `double getDouble(String columnLabel) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column name as a Java double.
  - **Parameter Description**:
    - `columnLabel`: The column label name.
  - **Return Value**: The value of the specified column name; if the value is NULL, returns 0.0d.
  - **Exception**: If a database access error occurs, a `SQLException` will be thrown.

- `byte[] getBytes(String columnLabel) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column name as a byte array.
  - **Parameter Description**:
    - `columnLabel`: The label of the column.
  - **Return Value**: Returns the value of the specified column name as a byte array; if the value is NULL, returns null.
  - **Exception**: If a database access error occurs, a `SQLException` will be thrown.

- `Date getDate(String columnLabel) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column name as a `java.sql.Date` object.
  - **Parameter Description**:
    - `columnLabel`: The label of the column.
  - **Return Value**: Returns the date value of the specified column name; if the value is NULL, returns null.
  - **Exception**: If a database access error occurs, a `SQLException` will be thrown.

- `Time getTime(String columnLabel) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column name as a `java.sql.Time` object.
  - **Parameter Description**:
    - `columnLabel`: The label of the column.
  - **Return Value**: Returns the time value of the specified column name; if the value is NULL, returns null.
  - **Exception**: If a database access error occurs, a `SQLException` will be thrown.

- `Timestamp getTimestamp(String columnLabel) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column name as a `java.sql.Timestamp`.
  - **Parameter Description**:
    - `columnLabel`: The label of the column.
  - **Return Value**: Returns the timestamp value of the specified column name; if the value is NULL, returns null.
  - **Exception**: If a database access error occurs, a `SQLException` will be thrown.

- `String getNString(String columnLabel) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column name as a Java String. This method is used to read NCHAR, NVARCHAR, and LONGNVARCHAR type columns to support international character sets.
  - **Parameter Description**:
    - `columnLabel`: The label of the column whose value is to be retrieved.
  - **Return Value**: The value of the specified column name; if the value is NULL, returns null.
  - **Exception**: If a database access error occurs, a `SQLException` will be thrown.

- `Object getObject(String columnLabel) throws SQLException`
  - **Interface Description**: Retrieves the value of the specified column name as a Java Object.
  - **Parameter Description**:
    - `columnLabel`: The label of the column.
  - **Return Value**: Returns the value of the specified column name; if the value is NULL, returns null.
  - **Exception**: If a database access error occurs, a `SQLException` will be thrown.

- `int findColumn(String columnLabel) throws SQLException`
  - **Interface Description**: Retrieves the column number for a given column name.
  - **Parameter Description**:
    - `columnLabel`: The label of the column.
  - **Return Value**: The column number for the given column name.
  - **Exception**: If the column name does not exist or a database access error occurs, a `SQLException` will be thrown.

- `boolean isBeforeFirst() throws SQLException`
  - **Interface Description**: Checks if the cursor is before the first row.
  - **Return Value**: Returns true if the cursor is before the first row; otherwise, returns false.
  - **Exception**: If a database access error occurs, a `SQLException` will be thrown.

- `boolean isAfterLast() throws SQLException`
  - **Interface Description**: Checks if the cursor is after the last row.
  - **Return Value**: Returns true if the cursor is after the last row; otherwise, returns false.
  - **Exception**: If a database access error occurs, a `SQLException` will be thrown.

- `boolean isFirst() throws SQLException`
  - **Interface Description**: Checks if the cursor is on the first row.
  - **Return Value**: Returns true if the cursor is on the first row; otherwise, returns false.
  - **Exception**: If a database access error occurs, a `SQLException` will be thrown.

- `boolean isLast() throws SQLException`
  - **Interface Description**: Determines whether the cursor is on the last row.
  - **Return Value**: Returns true if the cursor is on the last row; otherwise, returns false.
  - **Exception**: Throws `SQLException` if a database access error occurs.

- `int getRow() throws SQLException`
  - **Interface Description**: Gets the row number of the current cursor position.
  - **Return Value**: The row number where the cursor is currently positioned; returns 0 if the cursor is outside the result set.
  - **Exception**: Throws `SQLException` if a database access error occurs.

- `void setFetchSize(int rows) throws SQLException`
  - **Interface Description**: Sets the number of rows for the database to return in the result set. This method is used to guide the database driver on the number of rows to fetch from the server each time, to reduce communication frequency or limit memory usage.
  - **Parameter Description**:
    - `rows`: The specified number of rows to fetch. If set to 0, the driver's default value is used.
  - **Exception**: Throws `SQLException` if the result set is closed or if the rows parameter is less than 0.

- `int getFetchSize() throws SQLException`
  - **Interface Description**: Gets the fetch size of the current result set.
  - **Return Value**: The fetch size of the current result set.
  - **Exception**: Throws `SQLException` if the result set is closed.

- `int getType() throws SQLException`
  - **Interface Description**: Gets the type of `ResultSet`.
  - **Return Value**: The type of `ResultSet`. Always returns `ResultSet.TYPE_FORWARD_ONLY`, indicating that the result set cursor can only move forward.
  - **Exception**: Throws `SQLException` if the result set is closed.

- `int getConcurrency() throws SQLException`
  - **Interface Description**: Gets the concurrency mode of `ResultSet`.
  - **Return Value**: The concurrency mode of `ResultSet`. Always returns `ResultSet.CONCUR_READ_ONLY`, indicating that the result set cannot be updated.
  - **Exception**: Throws `SQLException` if the result set is closed.

- `<T> T getObject(String columnLabel, Class<T> type) throws SQLException`
  - **Interface Description**: Retrieves the value of a specified column using the column label and the Class object for the return type. This allows users to convert column values directly into the desired type in a more flexible manner.
  - **Parameter Description**:
    - `columnLabel`: The label name of the column whose value is to be retrieved.
    - `type`: The Class object of the expected return type in Java.
  - **Return Value**: The value of the specified column, returned in the specified type; returns null if the value is NULL.
  - **Exception**: Throws `SQLException` if a database access error occurs or if the specified type conversion is not supported.

#### Result Set Metadata

`ResultSetMetaData` provides interfaces to obtain result set metadata. A `ResultSetMetaData` type object is obtained through the `getMetaData` interface of a `ResultSet` type object.

- `int getColumnCount() throws SQLException`
  - **Interface Description**: Gets the total number of columns in the result set.
  - **Return Value**: The number of columns in the result set.
- `boolean isSearchable(int column) throws SQLException`
  - **Interface Description**: Determines whether a specified column can be used in a WHERE clause.
  - **Parameter Description**:
    - `column`: The column number (starting from 1).
  - **Return Value**: Returns true if the specified column can be used for searching; otherwise, returns false.
- `int isNullable(int column) throws SQLException`
  - **Interface Description**: Determines whether the value of a specified column can be null.
  - **Parameter Description**:
    - `column`: The column number (starting from 1).
  - **Return Value**: The nullability status of the column, returning `ResultSetMetaData.columnNoNulls`, `ResultSetMetaData.columnNullable`, or `ResultSetMetaData.columnNullableUnknown`.
- `boolean isSigned(int column) throws SQLException`
  - **Interface Description**: Determines whether the value of a specified column is a signed number.
  - **Parameter Description**:
    - `column`: The column number (starting from 1).
  - **Return Value**: Returns true if the column value is signed; otherwise, returns false.
- `int getColumnDisplaySize(int column) throws SQLException`
  - **Interface Description**: Gets the maximum standard width of a specified column, in characters.
  - **Parameter Description**:
    - `column`: The column number (starting from 1).
  - **Return Value**: The maximum width of the column.
  - **Exception**: Throws `SQLException` if the column index is out of range.
- `String getColumnLabel(int column) throws SQLException`
  - **Interface Description**: Gets the suggested title of a specified column, used for printing and display purposes.
  - **Parameter Description**:
    - `column`: The column number (starting from 1).
  - **Return Value**: The suggested title of the column.
  - **Exception**: Throws `SQLException` if the column index is out of range.
- `String getColumnName(int column) throws SQLException`
  - **Interface Description**: Gets the name of a specified column.
  - **Parameter Description**:
    - `column`: The column number (starting from 1).
  - **Return Value**: The name of the column.
  - **Exception**: Throws `SQLException` if the column index is out of range.
- `int getPrecision(int column) throws SQLException`
  - **Interface Description**: Gets the maximum precision of a specified column.
  - **Parameter Description**:
    - `column`: The column number (starting from 1).
  - **Return Value**: The maximum precision of the column.
  - **Exception**: Throws `SQLException` if the column index is out of range.
- `int getScale(int column) throws SQLException`
  - **Interface Description**: Gets the number of decimal places to the right of the decimal point for a specified column.
  - **Parameter Description**:
    - `column`: The column number (starting from 1).
  - **Return Value**: The number of decimal places for the column.
  - **Exception**: Throws `SQLException` if the column index is out of range.
- `String getTableName(int column) throws SQLException`
  - **Interface Description**: Gets the table name where a specified column is located.
  - **Parameter Description**:
    - `column`: The column number (starting from 1).
  - **Return Value**: The table name where the column is located.
- `String getCatalogName(int column) throws SQLException`
  - **Interface Description**: Gets the database name where a specified column is located.
  - **Parameter Description**:
    - `column`: The column number (starting from 1).
  - **Return Value**: The database name where the column is located.
- `int getColumnType(int column) throws SQLException`
  - **Interface Description**: Gets the SQL type of a specified column.
  - **Parameter Description**:
    - `column`: The column number (starting from 1).
  - **Return Value**: The SQL type, from `java.sql.Types`.
- `String getColumnTypeName(int column) throws SQLException`
  - **Interface Description**: Gets the database-specific type name of a specified column.
  - **Parameter Description**:
    - `column`: The column number (starting from 1).
  - **Return Value**: The database-specific type name.
- `boolean isReadOnly(int column) throws SQLException`
  - **Interface Description**: Determines whether a specified column is read-only.
  - **Parameter Description**:
    - `column`: The column number (starting from 1).
  - **Return Value**: Returns true if the column is read-only; otherwise, returns false.
- `String getColumnClassName(int column) throws SQLException`
  - **Interface Description**: Gets the Java class name of a specified column.
  - **Parameter Description**:
    - `column`: The column number (starting from 1).
  - **Return Value**: The Java class name corresponding to the column value.

### Parameter Binding

PreparedStatement allows the use of precompiled SQL statements, which can enhance performance and provide the capability of parameterized queries, thereby increasing security.  
The JDBC driver provides two classes that implement the PreparedStatement interface:  

1. TSDBPreparedStatement corresponding to native connections
1. TSWSPreparedStatement corresponding to WebSocket connections  

Since the JDBC standard does not have a high-performance data binding interface, both TSDBPreparedStatement and TSWSPreparedStatement have added some methods to extend the parameter binding capabilities.  
> **Note**: Since PreparedStatement inherits the Statement interface, the repeated interfaces are not described again here, please refer to the corresponding descriptions in the Statement interface.  

#### Standard Interface

- `void setNull(int parameterIndex, int sqlType) throws SQLException`
  - **Interface Description**: Sets the SQL type of the specified parameter to NULL.
  - **Parameter Description**:
    - `parameterIndex`: An int type parameter, indicating the parameter index position in the precompiled statement.
    - `sqlType`: An int type parameter, indicating the SQL type to be set to NULL.
  - **Exception**: If a database access error occurs, a SQLException will be thrown.
- `void setBoolean(int parameterIndex, boolean x) throws SQLException`
  - **Interface Description**: Sets the value of the specified parameter to a Java boolean.
  - **Parameter Description**:
    - `parameterIndex`: An int type parameter, indicating the parameter index position in the precompiled statement.
    - `x`: A boolean type parameter, indicating the value to be set.
  - **Exception**: If a database access error occurs, a SQLException will be thrown.
- The following interfaces are similar to setBoolean, except for the type of value to be set, and are not described again:
  - `void setByte(int parameterIndex, byte x) throws SQLException`
  - `void setShort(int parameterIndex, short x) throws SQLException`
  - `void setInt(int parameterIndex, int x) throws SQLException`
  - `void setLong(int parameterIndex, long x) throws SQLException`
  - `void setFloat(int parameterIndex, float x) throws SQLException`
  - `void setDouble(int parameterIndex, double x) throws SQLException`
  - `void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException`
  - `void setString(int parameterIndex, String x) throws SQLException`
  - `void setBytes(int parameterIndex, byte[] x) throws SQLException`
  - `void setDate(int parameterIndex, Date x) throws SQLException`
  - `void setTime(int parameterIndex, Time x) throws SQLException`
  - `void setTimestamp(int parameterIndex, Timestamp x) throws SQLException`
- `void clearParameters() throws SQLException`
  - **Interface Description**: Clears all currently set parameter values.
  - **Exception**: If the precompiled statement is closed, a SQLException will be thrown.
- `void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException`
  - **Interface Description**: Sets the value of the specified parameter using the given object, with the object's type specified by targetSqlType.
  - **Parameter Description**:
    - `parameterIndex`: An int type parameter, indicating the parameter index position in the precompiled statement.
    - `x`: An Object type parameter, indicating the value to be set.
    - `targetSqlType`: An int type parameter, indicating the SQL type of the x parameter.
  - **Exception**: If the precompiled statement is closed, a SQLException will be thrown.
- `void setObject(int parameterIndex, Object x) throws SQLException`
  - **Interface Description**: Sets the value of the specified parameter using the given object, with the object's type determined by the object itself.
  - **Parameter Description**:
    - `parameterIndex`: An int type parameter, indicating the parameter index position in the precompiled statement.
    - `x`: An Object type parameter, indicating the value to be set.
  - **Exception**: If the precompiled statement is closed or the parameter index is out of range, a SQLException will be thrown.
- `ResultSetMetaData getMetaData() throws SQLException`
  - **Interface Description**: Obtains the metadata related to the ResultSet object generated by this PreparedStatement object.
  - **Return Value**: If this PreparedStatement object has not yet performed any operations that generate a ResultSet object, it returns null; otherwise, it returns the metadata of this ResultSet object.
  - **Exception**: If a database access error occurs, a SQLException will be thrown.
- `ParameterMetaData getParameterMetaData() throws SQLException`
  - **Interface Description**: Obtains the type and property information of each parameter in this PreparedStatement object. See the Parameter Metadata section below for details.
  - **Return Value**: The parameter metadata of this PreparedStatement object.
  - **Exception**: If the precompiled statement is closed, a SQLException will be thrown.

#### Parameter Metadata

ParameterMetaData provides a parameter metadata interface:

- `int getParameterCount() throws SQLException`
  - **Interface Description**: Gets the number of parameters in the prepared statement.
  - **Return Value**: The number of parameters, type `int`.
  - **Exception**: If an error occurs during the process of getting the number of parameters, a `SQLException` will be thrown.
- `boolean isSigned(int param) throws SQLException`
  - **Interface Description**: Determines whether the specified parameter is a signed number.
  - **Parameter Description**:
    - `param`: The index of the parameter, type `int`.
  - **Return Value**: Returns `true` if the parameter is signed; otherwise, returns `false`.
  - **Exception**: If an error occurs during the determination process, a `SQLException` will be thrown.
- `int getPrecision(int param) throws SQLException`
  - **Interface Description**: Gets the precision of the specified parameter.
  - **Parameter Description**:
    - `param`: The index of the parameter, type `int`.
  - **Return Value**: The precision of the parameter, type `int`.
  - **Exception**: If an error occurs during the process of getting the precision, a `SQLException` will be thrown.
- `int getScale(int param) throws SQLException`
  - **Interface Description**: Gets the scale (number of decimal places) of the specified parameter.
  - **Parameter Description**:
    - `param`: The index of the parameter, type `int`.
  - **Return Value**: The scale of the parameter, type `int`.
  - **Exception**: If an error occurs during the process of getting the scale, a `SQLException` will be thrown.
- `int getParameterType(int param) throws SQLException`
  - **Interface Description**: Gets the SQL type of the specified parameter.
  - **Parameter Description**:
    - `param`: The index of the parameter, type `int`.
  - **Return Value**: The SQL type of the parameter, type `int`.
  - **Exception**: If an error occurs during the process of getting the SQL type, a `SQLException` will be thrown.
- `String getParameterTypeName(int param) throws SQLException`
  - **Interface Description**: Gets the SQL type name of the specified parameter.
  - **Parameter Description**:
    - `param`: The index of the parameter, type `int`.
  - **Return Value**: The SQL type name of the parameter, type `String`.
  - **Exception**: If an error occurs during the process of getting the SQL type name, a `SQLException` will be thrown.
- `String getParameterClassName(int param) throws SQLException`
  - **Interface Description**: Gets the Java class name of the specified parameter.
  - **Parameter Description**:
    - `param`: The index of the parameter, type `int`.
  - **Return Value**: The Java class name of the parameter, type `String`.
  - **Exception**: If an error occurs during the process of getting the Java class name, a `SQLException` will be thrown.
- `int getParameterMode(int param) throws SQLException`
  - **Interface Description**: Gets the mode of the specified parameter (e.g., IN, OUT, INOUT).
  - **Parameter Description**:
    - `param`: The index of the parameter, type `int`.
  - **Return Value**: The mode of the parameter, type `int`.
  - **Exception**: If an error occurs during the process of getting the parameter mode, a `SQLException` will be thrown.

#### Extended Interface  

- `void setTableName(String name) throws SQLException`
  - **Interface Description**: Sets the table name for the current operation.
  - **Parameter Description**:
    - `name`: A String type parameter representing the table name to bind.
- `void setTagNull(int index, int type)`
  - **Interface Description**: Sets a null value for the specified index tag.
  - **Parameter Description**:
    - `index`: The index position of the tag.
    - `type`: The data type of the tag.
- `void setTagBoolean(int index, boolean value)`
  - **Interface Description**: Sets a boolean value for the specified index tag.
  - **Parameter Description**:
    - `index`: The index position of the tag.
    - `value`: The boolean value to set.
- The following interfaces are similar to setTagBoolean, with different value types to set, and are not described further:
  - `void setTagInt(int index, int value)`
  - `void setTagByte(int index, byte value)`
  - `void setTagShort(int index, short value)`
  - `void setTagLong(int index, long value)`
  - `void setTagTimestamp(int index, long value)`
  - `void setTagFloat(int index, float value)`
  - `void setTagDouble(int index, double value)`
  - `void setTagString(int index, String value)`
  - `void setTagNString(int index, String value)`
  - `void setTagJson(int index, String value)`
  - `void setTagVarbinary(int index, byte[] value)`
  - `void setTagGeometry(int index, byte[] value)`

- `void setInt(int columnIndex, ArrayList<Integer> list) throws SQLException`
  - **Interface Description**: Sets batch integer values for the specified column index.
  - **Parameter Description**:
    - `columnIndex`: The index position of the column.
    - `list`: A list containing integer values.
  - **Exception**:
    - If an error occurs during the operation, a SQLException will be thrown.
- The following interfaces are the same as setInt except for the type of value to be set:
  - `void setFloat(int columnIndex, ArrayList<Float> list) throws SQLException`
  - `void setTimestamp(int columnIndex, ArrayList<Long> list) throws SQLException`
  - `void setLong(int columnIndex, ArrayList<Long> list) throws SQLException`
  - `void setDouble(int columnIndex, ArrayList<Double> list) throws SQLException`
  - `void setBoolean(int columnIndex, ArrayList<Boolean> list) throws SQLException`
  - `void setByte(int columnIndex, ArrayList<Byte> list) throws SQLException`
  - `void setShort(int columnIndex, ArrayList<Short> list) throws SQLException`
- `void setString(int columnIndex, ArrayList<String> list, int size) throws SQLException`
  - **Interface Description**: Sets a list of string values for the specified column index.
  - **Parameter Description**:
    - `columnIndex`: The index position of the column.
    - `list`: A list containing string values.
    - `size`: The maximum length of all strings, generally the limit value of the table creation statement.
  - **Exception**:
    - If an error occurs during the operation, a SQLException will be thrown.
- The following interfaces are the same as setString except for the type of value to be set:
  - `void setVarbinary(int columnIndex, ArrayList<byte[]> list, int size) throws SQLException`
  - `void setGeometry(int columnIndex, ArrayList<byte[]> list, int size) throws SQLException`
  - `void setNString(int columnIndex, ArrayList<String> list, int size) throws SQLException`
- `void columnDataAddBatch() throws SQLException`
  - **Interface Description**: Adds the data set by array-style interfaces such as `setInt(int columnIndex, ArrayList<Integer> list)` to the current PrepareStatement object's batch.
  - **Exception**:
    - If an error occurs during the operation, a SQLException will be thrown.
- `void columnDataExecuteBatch() throws SQLException`
  - **Interface Description**: Executes the batch operations of the current PrepareStatement object.
  - **Exception**: If an error occurs during the operation, a SQLException will be thrown.

### Data Subscription

JDBC standards do not support data subscription, therefore all interfaces in this chapter are extension interfaces. The TaosConsumer class provides consumer-related interfaces, ConsumerRecord provides consumption record-related interfaces, TopicPartition and OffsetAndMetadata provide partition information and offset metadata interfaces. Finally, ReferenceDeserializer and MapDeserializer provide support for deserialization.

#### Consumer

- `TaosConsumer(Properties properties) throws SQLException`
  - **Interface Description**: Consumer constructor
  - **Parameter Description**:
    - `properties`: A set of properties, see below for detailed supported properties.
  - **Return Value**: Consumer object
  - **Exception**: If creation fails, a SQLException will be thrown.

Consumer support property list:

- td.connect.type: Connection method. jni: indicates using a dynamic library connection, ws/WebSocket: indicates using WebSocket for data communication. The default is jni.
- bootstrap.servers: 
  1. If using a WebSocket connection, it is the `ip:port` where taosAdapter is located. Multiple endpoints are supported, separated by commas, such as `ip1:port1,ip2:port2`. If the connection is disconnected during the poll process, it can switch to other nodes (requires setting the auto-reconnect parameter `TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT` to `true`).  
  2. If using a Native connection, it is the `ip:port` where the TDengine TSDB server is located. Native connections do not support multiple endpoints.
- enable.auto.commit: Whether to allow automatic commit.
- group.id: The group where the consumer belongs.
- value.deserializer: Result set deserialization method, you can inherit `com.taosdata.jdbc.tmq.ReferenceDeserializer`, specify the result set bean, and implement deserialization. You can also inherit `com.taosdata.jdbc.tmq.Deserializer` and customize the deserialization method based on the SQL resultSet. When subscribing to a database, you need to set `value.deserializer` to `com.taosdata.jdbc.tmq.MapEnhanceDeserializer` when creating a consumer, and then create a consumer of type `TaosConsumer<TMQEnhMap>`. This way, each row of data can be deserialized into a table name and a `Map`.
- httpConnectTimeout: Connection creation timeout parameter, in ms, default is 5000 ms. Only valid under WebSocket connection.
- messageWaitTimeout: Data transfer timeout parameter, in ms, default is 10000 ms. Only valid under WebSocket connection.
- httpPoolSize: Maximum number of parallel requests under the same connection. Only valid under WebSocket connection.
- TSDBDriver.PROPERTY_KEY_ENABLE_COMPRESSION: Whether to enable compression during transmission. Only effective when using WebSocket connection. true: enabled, false: disabled. Default is false.
- TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT: Whether to enable automatic reconnection. Only effective when using WebSocket connection. true: enabled, false: disabled. Default is false.
- TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS: Automatic reconnection retry interval, in milliseconds, default value 2000. Only effective when PROPERTY_KEY_ENABLE_AUTO_RECONNECT is true.
- TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT: Automatic reconnection retry count, default value 3, only effective when PROPERTY_KEY_ENABLE_AUTO_RECONNECT is true.

For other parameters, please refer to: [Consumer parameter list](../../../developer-guide/manage-consumers/), note that the default value of auto.offset.reset in message subscription has changed starting from TDengine server version 3.2.0.0.

- `void subscribe(Collection<String> topics) throws SQLException`
  - **Interface Description**: Subscribe to a set of topics.
  - **Parameter Description**:
    - `topics`: A `Collection<String>` type parameter, representing the list of topics to subscribe to.
  - **Exception**: If an error occurs during the subscription process, a SQLException will be thrown.
- `void unsubscribe() throws SQLException`
  - **Interface Description**: Unsubscribe from all topics.
  - **Exception**: If an error occurs during the unsubscribe process, a SQLException will be thrown.
- `Set<String> subscription() throws SQLException`
  - **Interface Description**: Get all currently subscribed topics.
  - **Return Value**: The return type is `Set<String>`, which is the collection of all currently subscribed topics.
  - **Exception**: If an error occurs during the process of getting subscription information, a SQLException will be thrown.
- `ConsumerRecords<V> poll(Duration timeout) throws SQLException`
  - **Interface Description**: Poll for messages.
  - **Parameter Description**:
    - `timeout`: A `Duration` type parameter, representing the polling timeout.
  - **Return Value**: The return type is `ConsumerRecords<V>`, which are the polled message records.
  - **Exception**: If an error occurs during the polling process, a SQLException will be thrown.
- `void commitAsync() throws SQLException`
  - **Interface Description**: Asynchronously commit the offsets of the messages currently being processed.
  - **Exception**: If an error occurs during the commit process, a SQLException will be thrown.
- `void commitSync() throws SQLException`
  - **Interface Description**: Synchronously commit the offsets of the messages currently being processed.
  - **Exception**: If an error occurs during the commit process, a SQLException will be thrown.
- `void close() throws SQLException`
  - **Interface Description**: Close the consumer and release resources.
  - **Exception**: If an error occurs during the closing process, a SQLException will be thrown.
- `void seek(TopicPartition partition, long offset) throws SQLException`
  - **Interface Description**: Set the offset of a given partition to a specified position.
  - **Parameter Description**:
    - `partition`: A `TopicPartition` type parameter, representing the partition to operate on.
    - `offset`: A `long` type parameter, representing the offset to set.
  - **Exception**: If an error occurs during the offset setting process, a SQLException will be thrown.
- `long position(TopicPartition tp) throws SQLException`
  - **Interface Description**: Get the current offset of a given partition.
  - **Parameter Description**:
    - `tp`: A `TopicPartition` type parameter, representing the partition to query.
  - **Return Value**: The return type is `long`, which is the current offset of the given partition.
  - **Exception**: If an error occurs during the process of getting the offset, a SQLException will be thrown.
- `Map<TopicPartition, Long> beginningOffsets(String topic) throws SQLException`
  - **Interface Description**: Get the earliest offsets for each partition of a specified topic.
  - **Parameter Description**:
    - `topic`: A `String` type parameter, representing the topic to query.
  - **Return Value**: The return type is `Map<TopicPartition, Long>`, which are the earliest offsets for each partition of the specified topic.
  - **Exception**: If an error occurs during the process of getting the earliest offsets, a SQLException will be thrown.
- `Map<TopicPartition, Long> endOffsets(String topic) throws SQLException`
  - **Interface Description**: Get the latest offsets for each partition of a specified topic.
  - **Parameter Description**:
    - `topic`: A `String` type parameter, representing the topic to query.
  - **Return Value**: The return type is `Map<TopicPartition, Long>`, which are the latest offsets for each partition of the specified topic.
  - **Exception**: If an error occurs during the process of getting the latest offsets, a SQLException will be thrown.
- `void seekToBeginning(Collection<TopicPartition> partitions) throws SQLException`
  - **Interface Description**: Set the offsets of a set of partitions to the earliest offsets.
  - **Parameter Description**:
    - `partitions`: A `Collection<TopicPartition>` type parameter, representing the set of partitions to operate on.
  - **Exception**: If an error occurs during the offset setting process, a SQLException will be thrown.
- `void seekToEnd(Collection<TopicPartition> partitions) throws SQLException`
  - **Interface Description**: Set the offsets of a set of partitions to the latest offsets.
  - **Parameter Description**:
    - `partitions`: A `Collection<TopicPartition>` type parameter, representing the set of partitions to operate on.
  - **Exception**: If an error occurs during the offset setting process, a SQLException will be thrown.
- `Set<TopicPartition> assignment() throws SQLException`
  - **Interface Description**: Get all partitions currently assigned to the consumer.
  - **Return Value**: The return type is `Set<TopicPartition>`, which are all partitions currently assigned to the consumer.
  - **Exception**: If an error occurs during the process of getting the assigned partitions, a SQLException will be thrown.
- `OffsetAndMetadata committed(TopicPartition partition) throws SQLException`
  - **Interface Description**: Get the last committed offset of a specified partition.
  - **Parameter Description**:
    - `partition`: A `TopicPartition` type parameter, representing the partition to query.
  - **Return Value**: The return type is `OffsetAndMetadata`, which is the last committed offset of the specified partition.
  - **Exception**: If an error occurs during the process of getting the committed offset, a SQLException will be thrown.
- `Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) throws SQLException`
  - **Interface Description**: Get the last committed offsets of a set of partitions.
  - **Parameter Description**:
    - `partitions`: A `Set<TopicPartition>` type parameter, representing the set of partitions to query.
  - **Return Value**: The return type is `Map<TopicPartition, OffsetAndMetadata>`, which are the last committed offsets of the set of partitions.
  - **Exception**: If an error occurs during the process of getting the committed offsets, a SQLException will be thrown.

#### Consumption Records

The ConsumerRecords class provides consumption record information, allowing iteration over ConsumerRecord objects.

ConsumerRecord provides the following interfaces:

- `String getTopic()`
  - **Interface Description**: Get the message topic.
  - **Return Value**: The return type is `String`, which is the message topic.
- `String getDbName()`
  - **Interface Description**: Get the database name.
  - **Return Value**: The return type is `String`, which is the database name.
- `int getVGroupId()`
  - **Interface Description**: Get the virtual group ID.
  - **Return Value**: The return type is `int`, which is the virtual group ID.
- `V value()`
  - **Interface Description**: Get the value of the message.
  - **Return Value**: The return type is `V`, which is the message value.
- `long getOffset()`
  - **Interface Description**: Get the message offset.
  - **Return Value**: The return type is `long`, which is the message offset.

#### Partition Information

The TopicPartition class provides partition information, including the message topic and virtual group ID.

- `TopicPartition(String topic, int vGroupId)`
  - **Interface Description**: Construct a new TopicPartition instance to represent a specific topic and virtual group ID.
  - **Parameter Description**:
    - `topic`: A `String` type parameter representing the message topic.
    - `vGroupId`: An `int` type parameter representing the virtual group ID.
- `String getTopic()`
  - **Interface Description**: Get the topic of this TopicPartition instance.
  - **Return Value**: The return type is `String`, which is the topic of this TopicPartition instance.
- `int getVGroupId()`
  - **Interface Description**: Get the virtual group ID of this TopicPartition instance.
  - **Return Value**: The return type is `int`, which is the virtual group ID of this TopicPartition instance.

#### Offset Metadata

The OffsetAndMetadata class provides offset metadata information.

- `long offset()`
  - **Interface Description**: Get the offset in this OffsetAndMetadata instance.
  - **Return Value**: The return type is `long`, which is the offset in this OffsetAndMetadata instance.
- `String metadata()`
  - **Interface Description**: Get the metadata in this OffsetAndMetadata instance.
  - **Return Value**: The return type is `String`, which is the metadata in this OffsetAndMetadata instance.

#### Deserialization

The JDBC driver provides two deserialization classes: ReferenceDeserializer and MapDeserializer. Both implement the Deserializer interface.
ReferenceDeserializer is used to deserialize a consumed record into an object, ensuring that the object class's property names correspond to the consumed data's column names and that the types match.
MapDeserializer will deserialize a consumed row of data into a `Map<String, Object>` object, with keys as column names and values as Java objects.
Interfaces of ReferenceDeserializer and MapDeserializer are not directly called by users, please refer to usage examples.

## Appendix

JDBC Javadoc: [JDBC Reference doc](https://docs.oracle.com/javase/8/docs/api/java/sql/package-summary.html)
