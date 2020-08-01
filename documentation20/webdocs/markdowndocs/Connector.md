# TDengine connectors

TDengine provides many connectors for development, including C/C++, JAVA, Python, RESTful, Go, Node.JS, etc.

NOTE: All APIs which require a SQL string as parameter, including but not limit to `taos_query`, `taos_query_a`, `taos_subscribe` in the C/C++ Connector and their counterparts in other connectors, can ONLY process one SQL statement at a time. If more than one SQL statements are provided, their behaviors are undefined.

## C/C++ API

C/C++ APIs are similar to the MySQL APIs. Applications should include TDengine head file _taos.h_ to use C/C++ APIs by adding the following line in code:
```C
#include <taos.h>
```
Make sure TDengine library _libtaos.so_ is installed and use _-ltaos_ option to link the library when compiling. In most cases, if the return value of an API is integer, it return _0_ for success and other values as an error code for failure; if the return value is pointer, then _NULL_ is used for failure.


### Fundamental API

Fundamentatal APIs prepare runtime environment for other APIs, for example, create a database connection.

- `void taos_init()`

  Initialize the runtime environment for TDengine client. The API is not necessary since it is called int _taos_connect_ by default.


- `void taos_cleanup()`

  Cleanup runtime environment, client should call this API before exit.


- `int taos_options(TSDB_OPTION option, const void * arg, ...)`

   Set client options. The parameter _option_ supports values of _TSDB_OPTION_CONFIGDIR_ (configuration directory), _TSDB_OPTION_SHELL_ACTIVITY_TIMER_, _TSDB_OPTION_LOCALE_ (client locale) and _TSDB_OPTION_TIMEZONE_ (client timezone).


- `char* taos_get_client_info()`

  Retrieve version information of client.


- `TAOS *taos_connect(const char *ip, const char *user, const char *pass, const char *db, int port)`

  Open a connection to a TDengine server. The parameters are:
  
  * ip: IP address of the server
  * user: username
  * pass: password
  * db: database to use, **NULL** for no database to use after connection. Otherwise, the database should exist before connection or a connection error is reported.
  * port: port number to connect

  The handle returned by this API should be kept for future use.


- `char *taos_get_server_info(TAOS *taos)`

  Retrieve version information of server.


- `int taos_select_db(TAOS *taos, const char *db)`

  Set default database to `db`.


- `void taos_close(TAOS *taos)`

  Close a connection to a TDengine server by the handle returned by _taos_connect_`


### C/C++ sync API

Sync APIs are those APIs waiting for responses from the server after sending a request. TDengine has the following sync APIs:

- `TAOS_RES* taos_query(TAOS *taos, const char *sql)`

  The API used to run a SQL command. The command can be DQL, DML or DDL. The parameter _taos_ is the handle returned by _taos_connect_. Return value _NULL_ means failure.


- `int taos_result_precision(TAOS_RES *res)`

  Get the timestamp precision of the result set, return value _0_ means milli-second, _1_ mean micro-second and _2_ means nano-second.


- `TAOS_ROW taos_fetch_row(TAOS_RES *res)`

  Fetch a row of return results through _res_.


- `int taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows)`

  Fetch multiple rows from the result set, return value is row count.


- `int taos_num_fields(TAOS_RES *res)` and `int taos_field_count(TAOS_RES* res)`

  These two APIs are identical, both return the number of fields in the return result.


- `int* taos_fetch_lengths(TAOS_RES *res)`

  Get the field lengths of the result set, return value is an array whose length is the field count.


- `int taos_affected_rows(TAOS_RES *res)`

  Get affected row count of the executed statement.


- `TAOS_FIELD *taos_fetch_fields(TAOS_RES *res)`

  Fetch the description of each field. The description includes the property of data type, field name, and bytes. The API should be used with _taos_num_fields_ to fetch a row of data. The structure of `TAOS_FIELD` is:

  ```c
  typedef struct taosField {
    char     name[65];  // field name
    uint8_t  type;      // data type
    int16_t  bytes;     // length of the field in bytes
  } TAOS_FIELD;
  ```


- `void taos_stop_query(TAOS_RES *res)`

  Stop the execution of a query.


- `void taos_free_result(TAOS_RES *res)`

  Free the resources used by a result set. Make sure to call this API after fetching results or memory leak would happen.


- `char *taos_errstr(TAOS_RES *res)`

  Return the reason of the last API call failure. The return value is a string.


- `int *taos_errno(TAOS_RES *res)`

  Return the error code of the last API call failure. The return value is an integer.


**Note**: The connection to a TDengine server is not multi-thread safe. So a connection can only be used by one thread.


### C/C++ async API

In addition to sync APIs, TDengine also provides async APIs, which are more efficient. Async APIs are returned right away without waiting for a response from the server, allowing the application to continute with other tasks without blocking. So async APIs are more efficient, especially useful when in a poor network.

All async APIs require callback functions. The callback functions have the format:
```C
void fp(void *param, TAOS_RES * res, TYPE param3)
```
The first two parameters of the callback function are the same for all async APIs. The third parameter is different for different APIs. Generally, the first parameter is the handle provided to the API for action. The second parameter is a result handle.

- `void taos_query_a(TAOS *taos, const char *sql, void (*fp)(void *param, TAOS_RES *, int code), void *param);`

  The async version of _taos_query_.
  
  * taos: the handle returned by _taos_connect_.
  * sql: the SQL command to run.
  * fp: user defined callback function. The third parameter of the callback function _code_ is _0_ (for success) or a negative number (for failure, call taos_errstr to get the error as a string).  Applications mainly handle the second parameter, the returned result set.
  * param: user provided parameter which is required by the callback function. 


- `void taos_fetch_rows_a(TAOS_RES *res, void (*fp)(void *param, TAOS_RES *, int numOfRows), void *param);`

  The async API to fetch a batch of rows, which should only be used with a _taos_query_a_ call.
  
  * res: result handle returned by _taos_query_a_.
  * fp: the callback function. _param_ is a user-defined structure to pass to _fp_. The parameter _numOfRows_ is the number of result rows in the current fetch cycle. In the callback function, applications should call _taos_fetch_row_ to get records from the result handle. After getting a batch of results, applications should continue to call _taos_fetch_rows_a_ API to handle the next batch, until the _numOfRows_ is _0_ (for no more data to fetch) or _-1_ (for failure).


- `void taos_fetch_row_a(TAOS_RES *res, void (*fp)(void *param, TAOS_RES *, TAOS_ROW row), void *param);`

  The async API to fetch a result row.

  * res: result handle.
  * fp: the callback function. _param_ is a user-defined structure to pass to _fp_. The third parameter of the callback function is a single result row, which is different from that of _taos_fetch_rows_a_ API. With this API, it is not necessary to call _taos_fetch_row_ to retrieve each result row, which is handier than _taos_fetch_rows_a_ but less efficient.


Applications may apply operations on multiple tables. However, **it is important to make sure the operations on the same table are serialized**. That means after sending an insert request in a table to the server, no operations on the table are allowed before a response is received.


### C/C++ parameter binding API

TDengine also provides parameter binding APIs, like MySQL, only question mark `?` can be used to represent a parameter in these APIs.

- `TAOS_STMT* taos_stmt_init(TAOS *taos)`

  Create a TAOS_STMT to represent the prepared statement for other APIs.

- `int taos_stmt_prepare(TAOS_STMT *stmt, const char *sql, unsigned long length)`

  Parse SQL statement _sql_ and bind result to _stmt_ , if _length_ larger than 0, its value is used to determine the length of _sql_, the API auto detects the actual length of _sql_ otherwise.

- `int taos_stmt_bind_param(TAOS_STMT *stmt, TAOS_BIND *bind)`

  Bind values to parameters. _bind_ points to an array, the element count and sequence of the array must be identical as the parameters of the SQL statement. The usage of _TAOS_BIND_ is same as _MYSQL_BIND_ in MySQL, its definition is as below:

  ```c
  typedef struct TAOS_BIND {
    int            buffer_type;
    void *         buffer;
    unsigned long  buffer_length;  // not used in TDengine
    unsigned long *length;
    int *          is_null;
    int            is_unsigned;    // not used in TDengine
    int *          error;          // not used in TDengine
  } TAOS_BIND;
  ```

- `int taos_stmt_add_batch(TAOS_STMT *stmt)`

  Add bound parameters to batch, client can call `taos_stmt_bind_param` again after calling this API. Note this API only support _insert_ / _import_ statements, it returns an error in other cases.

- `int taos_stmt_execute(TAOS_STMT *stmt)`

  Execute the prepared statement. This API can only be called once for a statement at present.

- `TAOS_RES* taos_stmt_use_result(TAOS_STMT *stmt)`

  Acquire the result set of an executed statement. The usage of the result is same as `taos_use_result`, `taos_free_result` must be called after one you are done with the result set to release resources.

- `int taos_stmt_close(TAOS_STMT *stmt)`

  Close the statement, release all resources.


### C/C++ continuous query interface

TDengine provides APIs for continuous query driven by time, which run queries periodically in the background. There are only two APIs:


- `TAOS_STREAM *taos_open_stream(TAOS *taos, const char *sqlstr, void (*fp)(void *param, TAOS_RES * res, TAOS_ROW row), int64_t stime, void *param, void (*callback)(void *));`

  The API is used to create a continuous query.
  * _taos_: the connection handle returned by _taos_connect_.
  * _sqlstr_: the SQL string to run. Only query commands are allowed.
  * _fp_: the callback function to run after a query. TDengine passes query result `row`, query state `res` and user provided parameter `param` to this function. In this callback, `taos_num_fields` and `taos_fetch_fields` could be used to fetch field information.
  * _param_: a parameter passed to _fp_
  * _stime_: the time of the stream starts in the form of epoch milliseconds. If _0_ is given, the start time is set as the current time.
  * _callback_: a callback function to run when the continuous query stops automatically.

  The API is expected to return a handle for success. Otherwise, a NULL pointer is returned.


- `void taos_close_stream (TAOS_STREAM *tstr)`

  Close the continuous query by the handle returned by _taos_open_stream_. Make sure to call this API when the continuous query is not needed anymore.
  

### C/C++ subscription API

For the time being, TDengine supports subscription on one or multiple tables. It is implemented through periodic pulling from a TDengine server. 

* `TAOS_SUB *taos_subscribe(TAOS* taos, int restart, const char* topic, const char *sql, TAOS_SUBSCRIBE_CALLBACK fp, void *param, int interval)`

  The API is used to start a subscription session, it returns the subscription object on success and `NULL` in case of failure, the parameters are:
  * **taos**: The database connnection, which must be established already.
  * **restart**: `Zero` to continue a subscription if it already exits, other value to start from the beginning.
  * **topic**: The unique identifier of a subscription.
  * **sql**: A sql statement for data query, it can only be a `select` statement, can only query for raw data, and can only query data in ascending order of the timestamp field.
  * **fp**: A callback function to receive query result, only used in asynchronization mode and should be `NULL` in synchronization mode, please refer below for its prototype.
  * **param**: User provided additional parameter for the callback function.
  * **interval**: Pulling interval in millisecond. Under asynchronization mode, API will call the callback function `fp` in this interval, system performance will be impacted if this interval is too short. Under synchronization mode, if the duration between two call to `taos_consume` is less than this interval, the second call blocks until the duration exceed this interval.

* `typedef void (*TAOS_SUBSCRIBE_CALLBACK)(TAOS_SUB* tsub, TAOS_RES *res, void* param, int code)`

  Prototype of the callback function, the parameters are:
  * tsub: The subscription object.
  * res: The query result.
  * param: User provided additional parameter when calling `taos_subscribe`.
  * code: Error code in case of failures.

* `TAOS_RES *taos_consume(TAOS_SUB *tsub)`

  The API used to get the new data from a TDengine server. It should be put in an loop. The parameter `tsub` is the handle returned by `taos_subscribe`. This API should only be called in synchronization mode. If the duration between two call to `taos_consume` is less than pulling interval, the second call blocks until the duration exceed the interval. The API returns the new rows if new data arrives, or empty rowset otherwise, and if there's an error, it returns `NULL`.
  
* `void taos_unsubscribe(TAOS_SUB *tsub, int keepProgress)`

  Stop a subscription session by the handle returned by `taos_subscribe`. If `keepProgress` is **not** zero, the subscription progress information is kept and can be reused in later call to `taos_subscribe`, the information is removed otherwise.


##  Java Connector

TDengine 为了方便 Java 应用使用，提供了遵循 JDBC 标准(3.0)API 规范的 `taos-jdbcdriver` 实现。目前可以通过 [Sonatype Repository][1] 搜索并下载。

由于 TDengine 是使用 c 语言开发的，使用 taos-jdbcdriver 驱动包时需要依赖系统对应的本地函数库。

* libtaos.so 
    在 linux 系统中成功安装 TDengine 后，依赖的本地函数库 libtaos.so 文件会被自动拷贝至 /usr/lib/libtaos.so，该目录包含在 Linux 自动扫描路径上，无需单独指定。
    
* taos.dll
    在 windows 系统中安装完客户端之后，驱动包依赖的 taos.dll 文件会自动拷贝到系统默认搜索路径 C:/Windows/System32 下，同样无需要单独指定。
    
> 注意：在 windows 环境开发时需要安装 TDengine 对应的 [windows 客户端][14]，Linux 服务器安装完 TDengine 之后默认已安装 client，也可以单独安装 [Linux 客户端][15] 连接远程 TDengine Server。

TDengine 的 JDBC 驱动实现尽可能的与关系型数据库驱动保持一致，但时序空间数据库与关系对象型数据库服务的对象和技术特征的差异导致 taos-jdbcdriver 并未完全实现 JDBC 标准规范。在使用时需要注意以下几点：

* TDengine 不提供针对单条数据记录的删除和修改的操作，驱动中也没有支持相关方法。
* 由于不支持删除和修改，所以也不支持事务操作。
* 目前不支持表间的 union 操作。
* 目前不支持嵌套查询(nested query)，对每个 Connection 的实例，至多只能有一个打开的 ResultSet 实例；如果在 ResultSet还没关闭的情况下执行了新的查询，TSDBJDBCDriver 则会自动关闭上一个 ResultSet。


## TAOS-JDBCDriver 版本以及支持的 TDengine 版本和 JDK 版本

| taos-jdbcdriver 版本 | TDengine 版本 | JDK 版本 |
| --- | --- | --- |
| 1.0.3 | 1.6.1.x 及以上 | 1.8.x |
| 1.0.2 | 1.6.1.x 及以上 | 1.8.x |
| 1.0.1 | 1.6.1.x 及以上 | 1.8.x |
| 2.0.0 | 2.0.0.x 及以上 | 1.8.x |

## TDengine DataType 和 Java DataType

TDengine 目前支持时间戳、数字、字符、布尔类型，与 Java 对应类型转换如下：

| TDengine DataType | Java DataType |
| --- | --- |
| TIMESTAMP | java.sql.Timestamp |
| INT | java.lang.Integer |
| BIGINT | java.lang.Long |
| FLOAT | java.lang.Float |
| DOUBLE | java.lang.Double |
| SMALLINT, TINYINT |java.lang.Short  |
| BOOL | java.lang.Boolean |
| BINARY, NCHAR | java.lang.String |

## 如何获取 TAOS-JDBCDriver

### maven 仓库

目前 taos-jdbcdriver 已经发布到 [Sonatype Repository][1] 仓库，且各大仓库都已同步。
* [sonatype][8]
* [mvnrepository][9]
* [maven.aliyun][10]

maven 项目中使用如下 pom.xml 配置即可：

```xml
<dependencies>
    <dependency>
        <groupId>com.taosdata.jdbc</groupId>
        <artifactId>taos-jdbcdriver</artifactId>
        <version>2.0.0</version>
    </dependency>
</dependencies>
```

### 源码编译打包

下载 [TDengine][3] 源码之后，进入 taos-jdbcdriver 源码目录 `src/connector/jdbc` 执行 `mvn clean package` 即可生成相应 jar 包。


## 使用说明

### 获取连接

如下所示配置即可获取 TDengine Connection：
```java
Class.forName("com.taosdata.jdbc.TSDBDriver");
String jdbcUrl = "jdbc:TAOS://127.0.0.1:6030/log?user=root&password=taosdata";
Connection conn = DriverManager.getConnection(jdbcUrl);
```
> 端口 6030 为默认连接端口，JDBC URL 中的 log 为系统本身的监控数据库。

TDengine 的 JDBC URL 规范格式为：
`jdbc:TSDB://{host_ip}:{port}/[database_name]?[user={user}|&password={password}|&charset={charset}|&cfgdir={config_dir}|&locale={locale}|&timezone={timezone}]`

其中，`{}` 中的内容必须，`[]` 中为可选。配置参数说明如下：

* user：登录 TDengine 用户名，默认值 root。
* password：用户登录密码，默认值 taosdata。
* charset：客户端使用的字符集，默认值为系统字符集。
* cfgdir：客户端配置文件目录路径，Linux OS 上默认值 /etc/taos ，Windows OS 上默认值 C:/TDengine/cfg。
* locale：客户端语言环境，默认值系统当前 locale。
* timezone：客户端使用的时区，默认值为系统当前时区。

以上参数可以在 3 处配置，`优先级由高到低`分别如下：
1. JDBC URL 参数
    如上所述，可以在 JDBC URL 的参数中指定。
2. java.sql.DriverManager.getConnection(String jdbcUrl, Properties connProps)
```java
public Connection getConn() throws Exception{
  Class.forName("com.taosdata.jdbc.TSDBDriver");
  String jdbcUrl = "jdbc:TAOS://127.0.0.1:0/log?user=root&password=taosdata";
  Properties connProps = new Properties();
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_USER, "root");
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, "taosdata");
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_CONFIG_DIR, "/etc/taos");
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
  Connection conn = DriverManager.getConnection(jdbcUrl, connProps);
  return conn;
}
```

3. 客户端配置文件 taos.cfg

    linux 系统默认配置文件为 /var/lib/taos/taos.cfg，windows 系统默认配置文件路径为 C:\TDengine\cfg\taos.cfg。
```properties
# client default username
# defaultUser           root

# client default password
# defaultPass           taosdata

# default system charset
# charset               UTF-8

# system locale
# locale                en_US.UTF-8
```
> 更多详细配置请参考[客户端配置][13]

### 创建数据库和表

```java
Statement stmt = conn.createStatement();

// create database
stmt.executeUpdate("create database if not exists db");

// use database
stmt.executeUpdate("use db");

// create table
stmt.executeUpdate("create table if not exists tb (ts timestamp, temperature int, humidity float)");
```
> 注意：如果不使用 `use db` 指定数据库，则后续对表的操作都需要增加数据库名称作为前缀，如 db.tb。

### 插入数据

```java
// insert data
int affectedRows = stmt.executeUpdate("insert into tb values(now, 23, 10.3) (now + 1s, 20, 9.3)");

System.out.println("insert " + affectedRows + " rows.");
```
> now 为系统内部函数，默认为服务器当前时间。
> `now + 1s` 代表服务器当前时间往后加 1 秒，数字后面代表时间单位：a(毫秒), s(秒), m(分), h(小时), d(天)，w(周), n(月), y(年)。

### 查询数据

```java
// query data
ResultSet resultSet = stmt.executeQuery("select * from tb");

Timestamp ts = null;
int temperature = 0;
float humidity = 0;
while(resultSet.next()){

    ts = resultSet.getTimestamp(1);
    temperature = resultSet.getInt(2);
    humidity = resultSet.getFloat("humidity");

    System.out.printf("%s, %d, %s\n", ts, temperature, humidity);
}
```
> 查询和操作关系型数据库一致，使用下标获取返回字段内容时从 1 开始，建议使用字段名称获取。


### 关闭资源

```java
resultSet.close();
stmt.close();
conn.close();
```
> `注意务必要将 connection 进行关闭`，否则会出现连接泄露。
## 与连接池使用

**HikariCP**

* 引入相应 HikariCP maven 依赖：
```xml
<dependency>
    <groupId>com.zaxxer</groupId>
    <artifactId>HikariCP</artifactId>
    <version>3.4.1</version>
</dependency>
```

* 使用示例如下：
```java
 public static void main(String[] args) throws SQLException {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl("jdbc:TAOS://127.0.0.1:6030/log");
    config.setUsername("root");
    config.setPassword("taosdata");

    config.setMinimumIdle(3);           //minimum number of idle connection
    config.setMaximumPoolSize(10);      //maximum number of connection in the pool
    config.setConnectionTimeout(10000); //maximum wait milliseconds for get connection from pool
    config.setIdleTimeout(60000);       // max idle time for recycle idle connection 
    config.setConnectionTestQuery("describe log.dn"); //validation query
    config.setValidationTimeout(3000);   //validation query timeout

    HikariDataSource ds = new HikariDataSource(config); //create datasource
    
    Connection  connection = ds.getConnection(); // get connection
    Statement statement = connection.createStatement(); // get statement
    
    //query or insert 
    // ...
    
    connection.close(); // put back to conneciton pool
}
```
> 通过 HikariDataSource.getConnection() 获取连接后，使用完成后需要调用 close() 方法，实际上它并不会关闭连接，只是放回连接池中。
> 更多 HikariCP 使用问题请查看[官方说明][5]

## Python Connector

### Install TDengine Python client

Users can find python client packages in our source code directory _src/connector/python_. There are two directories corresponding two python versions. Please choose the correct package to install. Users can use _pip_ command to install:

```cmd
pip install src/connector/python/python2/
```

or

```
pip install src/connector/python/python3/
```

If _pip_ command is not installed on the system, users can choose to install pip or just copy the _taos_ directory in the python client directory to the application directory to use.

### Python client interfaces

To use TDengine Python client, import TDengine module at first:

```python
import taos 
```

Users can get module information from Python help interface or refer to our [python code example](). We list the main classes and methods below:

- _TDengineConnection_ class

  Run `help(taos.TDengineConnection)` in python terminal for details.

- _TDengineCursor_ class

  Run `help(taos.TDengineCursor)` in python terminal for details.

- connect method

  Open a connection. Run `help(taos.connect)` in python terminal for details.

## RESTful Connector

TDengine also provides RESTful API to satisfy developing on different platforms. Unlike other databases, TDengine RESTful API applies operations to the database through the SQL command in the body of HTTP POST request. What users are required to provide is just a URL.


For the time being, TDengine RESTful API uses a _\<TOKEN\>_ generated from username and password for identification. Safer identification methods will be provided in the future.


### HTTP URL encoding 

To use TDengine RESTful API, the URL should have the following encoding format:
```
http://<ip>:<PORT>/rest/sql
```
- _ip_: IP address of any node in a TDengine cluster
- _PORT_: TDengine HTTP service port. It is 6020 by default.

For example, the URL encoding _http://192.168.0.1:6020/rest/sql_ used to send HTTP request to a TDengine server with IP address as 192.168.0.1.

It is required to add a token in an HTTP request header for identification.

```
Authorization: Basic <TOKEN>
```

The HTTP request body contains the SQL command to run. If the SQL command contains a table name, it should also provide the database name it belongs to in the form of `<db_name>.<tb_name>`. Otherwise, an error code is returned.

For example, use _curl_ command to send a HTTP request:

```
curl -H 'Authorization: Basic <TOKEN>' -d '<SQL>' <ip>:<PORT>/rest/sql
```

or use

```
curl -u username:password -d '<SQL>' <ip>:<PORT>/rest/sql
```

where `TOKEN` is the encryted string of `{username}:{password}` using the Base64 algorithm, e.g. `root:taosdata` will be encoded as `cm9vdDp0YW9zZGF0YQ==`

### HTTP response

The HTTP resonse is in JSON format as below:

```
{
    "status": "succ",
    "head": ["column1","column2", …],
    "data": [
        ["2017-12-12 23:44:25.730", 1],
        ["2017-12-12 22:44:25.728", 4]
    ],
    "rows": 2
} 
```
Specifically,
- _status_: the result of the operation, success or failure
- _head_: description of returned result columns
- _data_: the returned data array. If no data is returned, only an _affected_rows_ field is listed
- _rows_: the number of rows returned

### Example

- Use _curl_ command to query all the data in table _t1_ of database _demo_:

  `curl -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' -d 'select * from demo.t1' 192.168.0.1:6020/rest/sql`

The return value is like:

```
{
    "status": "succ",
    "head": ["column1","column2","column3"],
    "data": [
        ["2017-12-12 23:44:25.730", 1, 2.3],
        ["2017-12-12 22:44:25.728", 4, 5.6]
    ],
    "rows": 2
}
```

- Use HTTP to create a database：

  `curl -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' -d 'create database demo' 192.168.0.1:6020/rest/sql`

    The return value should be:

```
{
    "status": "succ",
    "head": ["affected_rows"],
    "data": [[1]],
    "rows": 1,
}
```

## Go Connector

TDengine provides a GO client package `taosSql`. `taosSql` implements a kind of  interface of GO `database/sql/driver`. User can access TDengine by importing the package in their program with the following instructions, detailed usage please refer to `https://github.com/taosdata/driver-go/blob/develop/taosSql/driver_test.go`

```Go
import (
    "database/sql"
    _ github.com/taosdata/driver-go/taoSql“
)
```
### API

* `sql.Open(DRIVER_NAME string, dataSourceName string) *DB`

   Open DB, generally DRIVER_NAME will be used as a constant with default value `taosSql`, dataSourceName is a combined String with format `user:password@/tcp(host:port)/dbname`. If user wants to access TDengine with multiple goroutine concurrently, the better way is to create an sql.Open object in each goroutine to access TDengine.

  **Note**:	When calling this api, only a few initial work are done, instead the validity check happened during executing `Query` or `Exec`, at this time the connection will be created, and system will check if `user、password、host、port` is valid. Additionaly the most of features are implemented in the taosSql dependency lib `libtaos`, from this view, sql.Open is lightweight.

*  `func (db *DB) Exec(query string, args ...interface{}) (Result, error)`

  Execute non-Query related SQLs, the execution result is stored with type of Result.


*  `func (db *DB) Query(query string, args ...interface{}) (*Rows, error)`

  Execute Query related SQLs, the execution result is *Raw, the detailed usage can refer GO interface `database/sql/driver`

## Node.js Connector

TDengine also provides a node.js connector package that is installable through [npm](https://www.npmjs.com/). The package is also in our source code at *src/connector/nodejs/*. The following instructions are also available [here](https://github.com/taosdata/tdengine/tree/master/src/connector/nodejs)

To get started, just type in the following to install the connector through [npm](https://www.npmjs.com/).

```cmd
npm install td-connector
```

It is highly suggested you use npm. If you don't have it installed, you can also just copy the nodejs folder from *src/connector/nodejs/* into your node project folder.

To interact with TDengine, we make use of the [node-gyp](https://github.com/nodejs/node-gyp) library. To install, you will need to install the following depending on platform (the following instructions are quoted from node-gyp)

### On Unix

- `python` (`v2.7` recommended, `v3.x.x` is **not** supported)
- `make`
- A proper C/C++ compiler toolchain, like [GCC](https://gcc.gnu.org)

### On macOS

- `python` (`v2.7` recommended, `v3.x.x` is **not** supported) (already installed on macOS)

- Xcode

  - You also need to install the 

    ```
    Command Line Tools
    ```

     via Xcode. You can find this under the menu 

    ```
    Xcode -> Preferences -> Locations
    ```

     (or by running 

    ```
    xcode-select --install
    ```

     in your Terminal) 

    - This step will install `gcc` and the related toolchain containing `make`

### On Windows

#### Option 1

Install all the required tools and configurations using Microsoft's [windows-build-tools](https://github.com/felixrieseberg/windows-build-tools) using `npm install --global --production windows-build-tools` from an elevated PowerShell or CMD.exe (run as Administrator).

#### Option 2

Install tools and configuration manually:

- Install Visual C++ Build Environment: [Visual Studio Build Tools](https://visualstudio.microsoft.com/thank-you-downloading-visual-studio/?sku=BuildTools) (using "Visual C++ build tools" workload) or [Visual Studio 2017 Community](https://visualstudio.microsoft.com/pl/thank-you-downloading-visual-studio/?sku=Community) (using the "Desktop development with C++" workload)
- Install [Python 2.7](https://www.python.org/downloads/) (`v3.x.x` is not supported), and run `npm config set python python2.7` (or see below for further instructions on specifying the proper Python version and path.)
- Launch cmd, `npm config set msvs_version 2017`

If the above steps didn't work for you, please visit [Microsoft's Node.js Guidelines for Windows](https://github.com/Microsoft/nodejs-guidelines/blob/master/windows-environment.md#compiling-native-addon-modules) for additional tips.

To target native ARM64 Node.js on Windows 10 on ARM, add the  components "Visual C++ compilers and libraries for ARM64" and "Visual  C++ ATL for ARM64".

### Usage

The following is a short summary of the basic usage of the connector, the  full api and documentation can be found [here](http://docs.taosdata.com/node)

#### Connection

To use the connector, first require the library ```td-connector```. Running the function ```taos.connect``` with the connection options passed in as an object will return a TDengine connection object. The required connection option is ```host```, other options if not set, will be the default values as shown below.

A cursor also needs to be initialized in order to interact with TDengine from Node.js.

```javascript
const taos = require('td-connector');
var conn = taos.connect({host:"127.0.0.1", user:"root", password:"taosdata", config:"/etc/taos",port:0})
var cursor = conn.cursor(); // Initializing a new cursor
```

To close a connection, run

```javascript
conn.close();
```

#### Queries

We can now start executing simple queries through the ```cursor.query``` function, which returns a TaosQuery object.

```javascript
var query = cursor.query('show databases;')
```

We can get the results of the queries through the ```query.execute()``` function, which returns a promise that resolves with a TaosResult object, which contains the raw data and additional functionalities such as pretty printing the results.

```javascript
var promise = query.execute();
promise.then(function(result) {
  result.pretty(); //logs the results to the console as if you were in the taos shell
});
```

You can also query by binding parameters to a query by filling in the question marks in a string as so. The query will automatically parse what was binded and convert it to the proper format for use with TDengine

```javascript
var query = cursor.query('select * from meterinfo.meters where ts <= ? and areaid = ?;').bind(new Date(), 5);
query.execute().then(function(result) {
  result.pretty();
})
```

The TaosQuery object can also be immediately executed upon creation by passing true as the second argument, returning a promise instead of a TaosQuery.

```javascript
var promise = cursor.query('select * from meterinfo.meters where v1 = 30;', true)
promise.then(function(result) {
  result.pretty();
})
```
#### Async functionality

Async queries can be performed using the same functions such as `cursor.execute`, `cursor.query`, but now with `_a` appended to them.

Say you want to execute an two async query on two seperate tables, using `cursor.query_a`, you can do that and get a TaosQuery object, which upon executing with the `execute_a` function, returns a promise that resolves with a TaosResult object.

```javascript
var promise1 = cursor.query_a('select count(*), avg(v1), avg(v2) from meter1;').execute_a()
var promise2 = cursor.query_a('select count(*), avg(v1), avg(v2) from meter2;').execute_a();
promise1.then(function(result) {
  result.pretty();
})
promise2.then(function(result) {
  result.pretty();
})
```


### Example

An example of using the NodeJS connector to create a table with weather data and create and execute queries can be found [here](https://github.com/taosdata/TDengine/tree/master/tests/examples/nodejs/node-example.js) (The preferred method for using the connector)

An example of using the NodeJS connector to achieve the same things but without all the object wrappers that wrap around the data returned to achieve higher functionality can be found [here](https://github.com/taosdata/TDengine/tree/master/tests/examples/nodejs/node-example-raw.js)

