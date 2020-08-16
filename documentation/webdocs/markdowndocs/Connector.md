# TDengine connectors

TDengine provides many connectors for development, including C/C++, JAVA, Python, RESTful, Go, Node.JS, etc.

NOTE: All APIs which require a SQL string as parameter, including but not limit to `taos_query`, `taos_query_a`, `taos_subscribe` in the C/C++ Connector and their counterparts in other connectors, can ONLY process one SQL statement at a time. If more than one SQL statements are provided, their behaviors are undefined.

## C/C++ API

C/C++ APIs are similar to the MySQL APIs. Applications should include TDengine head file _taos.h_ to use C/C++ APIs by adding the following line in code:
```C
#include <taos.h>
```
Make sure TDengine library _libtaos.so_ is installed and use _-ltaos_ option to link the library when compiling. In most cases, if the return value of an API is integer, it return _0_ for success and other values as an error code for failure; if the return value is pointer, then _NULL_ is used for failure.


### C/C++ sync API

Sync APIs are those APIs waiting for responses from the server after sending a request. TDengine has the following sync APIs:


- `TAOS *taos_connect(char *ip, char *user, char *pass, char *db, int port)`

  Open a connection to a TDengine server. The parameters are _ip_ (IP address of the server), _user_ (username to login), _pass_ (password to login), _db_ (database to use after connection) and _port_ (port number to connect). The parameter _db_ can be NULL for no database to use after connection. Otherwise, the database should exist before connection or a connection error is reported. The handle returned by this API should be kept for future use.

- `void taos_close(TAOS *taos)`

  Close a connection to a TDengine server by the handle returned by _taos_connect_`


- `int taos_query(TAOS *taos, char *sqlstr)`

  The API used to run a SQL command. The command can be DQL or DML. The parameter _taos_ is the handle returned by _taos_connect_. Return value _-1_ means failure.


- `TAOS_RES *taos_use_result(TAOS *taos)`

  Use the result after running _taos_query_. The handle returned should be kept for future fetch.


- `TAOS_ROW taos_fetch_row(TAOS_RES *res)`

  Fetch a row of return results through _res_, the handle returned by _taos_use_result_.


- `int taos_num_fields(TAOS_RES *res)`

  Get the number of fields in the return result.


- `TAOS_FIELD *taos_fetch_fields(TAOS_RES *res)`

  Fetch the description of each field. The description includes the property of data type, field name, and bytes. The API should be used with _taos_num_fields_ to fetch a row of data.


- `void taos_free_result(TAOS_RES *res)`

  Free the resources used by a result set. Make sure to call this API after fetching results or memory leak would happen.


- `void taos_init()`

  Initialize the environment variable used by TDengine client. The API is not necessary since it is called int _taos_connect_ by default.


- `char *taos_errstr(TAOS *taos)`

  Return the reason of the last API call failure. The return value is a string.


- `int *taos_errno(TAOS *taos)`

  Return the error code of the last API call failure. The return value is an integer.


-  `int taos_options(TSDB_OPTION option, const void * arg, ...)`

   Set client options. The parameter _option_ supports values of _TSDB_OPTION_CONFIGDIR_ (configuration directory), _TSDB_OPTION_SHELL_ACTIVITY_TIMER_, _TSDB_OPTION_LOCALE_ (client locale) and _TSDB_OPTION_TIMEZONE_ (client timezone).

The 12 APIs are the most important APIs frequently used. Users can check _taos.h_ file for more API information.

**Note**: The connection to a TDengine server is not multi-thread safe. So a connection can only be used by one thread.

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


### C/C++ async API

In addition to sync APIs, TDengine also provides async APIs, which are more efficient. Async APIs are returned right away without waiting for a response from the server, allowing the application to continute with other tasks without blocking. So async APIs are more efficient, especially useful when in a poor network.

All async APIs require callback functions. The callback functions have the format:
```C
void fp(void *param, TAOS_RES * res, TYPE param3)
```
The first two parameters of the callback function are the same for all async APIs. The third parameter is different for different APIs. Generally, the first parameter is the handle provided to the API for action. The second parameter is a result handle.

- `void taos_query_a(TAOS *taos, char *sqlstr, void (*fp)(void *param, TAOS_RES *, int code), void *param);`

  The async query interface. _taos_ is the handle returned by _taos_connect_ interface. _sqlstr_ is the SQL command to run. _fp_ is the callback function. _param_ is the parameter required by the callback function. The third parameter of the callback function _code_ is _0_ (for success) or a negative number (for failure, call taos_errstr to get the error as a string).  Applications mainly handle with the second parameter, the returned result set.


- `void taos_fetch_rows_a(TAOS_RES *res, void (*fp)(void *param, TAOS_RES *, int numOfRows), void *param);`

  The async API to fetch a batch of rows, which should only be used with a _taos_query_a_ call. The parameter _res_ is the result handle returned by _taos_query_a_. _fp_ is the callback function. _param_ is a user-defined structure to pass to _fp_. The parameter _numOfRows_ is the number of result rows in the current fetch cycle. In the callback function, applications should call _taos_fetch_row_ to get records from the result handle. After getting a batch of results, applications should continue to call _taos_fetch_rows_a_ API to handle the next batch, until the _numOfRows_ is _0_ (for no more data to fetch) or _-1_ (for failure).


- `void taos_fetch_row_a(TAOS_RES *res, void (*fp)(void *param, TAOS_RES *, TAOS_ROW row), void *param);`

  The async API to fetch a result row. _res_ is the result handle. _fp_ is the callback function. _param_ is a user-defined structure to pass to _fp_. The third parameter of the callback function is a single result row, which is different from that of _taos_fetch_rows_a_ API. With this API, it is not necessary to call _taos_fetch_row_ to retrieve each result row, which is handier than _taos_fetch_rows_a_ but less efficient.


Applications may apply operations on multiple tables. However, **it is important to make sure the operations on the same table are serialized**. That means after sending an insert request in a table to the server, no operations on the table are allowed before a response is received.

### C/C++ continuous query interface

TDengine provides APIs for continuous query driven by time, which run queries periodically in the background. There are only two APIs:


- `TAOS_STREAM *taos_open_stream(TAOS *taos, char *sqlstr, void (*fp)(void *param, TAOS_RES *, TAOS_ROW row), int64_t stime, void *param, void (*callback)(void *));`

  The API is used to create a continuous query.
  * _taos_: the connection handle returned by _taos_connect_.
  * _sqlstr_: the SQL string to run. Only query commands are allowed.
  * _fp_: the callback function to run after a query
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

To Java delevopers, TDengine provides `taos-jdbcdriver` according to the JDBC(3.0) API. Users can find and download it through [Sonatype Repository][1].

Since the native language of TDengine is C, the necessary TDengine library should be checked before using the taos-jdbcdriver:

* libtaos.so (Linux)
    After TDengine is installed successfully, the library `libtaos.so` will be automatically copied to the `/usr/lib/`, which is the system's default search path. 
    
* taos.dll (Windows)
    After TDengine client is installed, the library `taos.dll` will be automatically copied to the `C:/Windows/System32`, which is the system's default search path. 
    
> Note: Please make sure that [TDengine Windows client][14] has been installed if developing on Windows. Now although TDengine client would be defaultly installed together with TDengine server, it can also be installed [alone][15].

Since TDengine is time-series database, there are still some differences compared with traditional databases in using TDengine JDBC driver: 
* TDengine doesn't allow to delete/modify a single record, and thus JDBC driver also has no such method. 
* No support for transaction
* No support for union between tables
* No support for nested query，`There is at most one open ResultSet for each Connection. Thus, TSDB JDBC Driver will close current ResultSet if it is not closed and a new query begins`.

## Version list of TAOS-JDBCDriver and required TDengine and JDK 

| taos-jdbcdriver | TDengine  | JDK  | 
| --- | --- | --- | 
| 1.0.3 | 1.6.1.x or higher | 1.8.x |
| 1.0.2 | 1.6.1.x or higher | 1.8.x |  
| 1.0.1 | 1.6.1.x or higher | 1.8.x |  

## DataType in TDengine and Java

The datatypes in TDengine include timestamp, number, string and boolean, which are converted as follows in Java:

| TDengine | Java | 
| --- | --- | 
| TIMESTAMP | java.sql.Timestamp | 
| INT | java.lang.Integer | 
| BIGINT | java.lang.Long |  
| FLOAT | java.lang.Float |  
| DOUBLE | java.lang.Double | 
| SMALLINT, TINYINT |java.lang.Short  |
| BOOL | java.lang.Boolean | 
| BINARY, NCHAR | java.lang.String | 

## How to get TAOS-JDBC Driver

### maven repository

taos-jdbcdriver has been published to [Sonatype Repository][1]:
* [sonatype][8]
* [mvnrepository][9]
* [maven.aliyun][10]

Using the following pom.xml for maven projects

```xml
<dependencies>
    <dependency>
        <groupId>com.taosdata.jdbc</groupId>
        <artifactId>taos-jdbcdriver</artifactId>
        <version>1.0.3</version>
    </dependency>
</dependencies>
```

### JAR file from the source code

After downloading the [TDengine][3] source code, execute `mvn clean package` in the directory `src/connector/jdbc` and then the corresponding jar file is generated.

## Usage 

### get the connection

```java
Class.forName("com.taosdata.jdbc.TSDBDriver");
String jdbcUrl = "jdbc:TAOS://127.0.0.1:6030/log?user=root&password=taosdata";
Connection conn = DriverManager.getConnection(jdbcUrl);
```
> `6030` is the default port and `log` is the default database for system monitor.

A normal JDBC URL looks as follows:
`jdbc:TAOS://{host_ip}:{port}/[database_name]?[user={user}|&password={password}|&charset={charset}|&cfgdir={config_dir}|&locale={locale}|&timezone={timezone}]`

values in `{}` are necessary while values in `[]` are optional。Each option in the above URL denotes:

* user：user name for login, defaultly root。
* password：password for login，defaultly taosdata。
* charset：charset for client，defaultly system charset
* cfgdir：log directory for client, defaultly _/etc/taos/_ on Linux and _C:/TDengine/cfg_ on Windows。
* locale：language for client，defaultly system locale。
* timezone：timezone for client，defaultly system timezone。

The options above can be configures (`ordered by priority`):
1. JDBC URL 

    As explained above.
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

3. Configuration file (taos.cfg)

    Default configuration file is _/var/lib/taos/taos.cfg_ On Linux and _C:\TDengine\cfg\taos.cfg_ on Windows
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
> More options can refer to [client configuration][13]

### Create databases and tables

```java
Statement stmt = conn.createStatement();

// create database
stmt.executeUpdate("create database if not exists db");

// use database
stmt.executeUpdate("use db");

// create table
stmt.executeUpdate("create table if not exists tb (ts timestamp, temperature int, humidity float)");
```
> Note: if no step like `use db`, the name of database must be added as prefix like _db.tb_ when operating on tables 

### Insert data

```java
// insert data
int affectedRows = stmt.executeUpdate("insert into tb values(now, 23, 10.3) (now + 1s, 20, 9.3)");

System.out.println("insert " + affectedRows + " rows.");
```
> _now_ is the server time.
> _now+1s_ is 1 second later than current server time. The time unit includes: _a_(millisecond), _s_(second), _m_(minute), _h_(hour), _d_(day), _w_(week), _n_(month), _y_(year).

### Query database

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
> query is consistent with relational database. The subscript start with 1 when retrieving return results. It is recommended to use the column name to retrieve results.

### Close all

```java
resultSet.close();
stmt.close();
conn.close();
```
> `please make sure the connection is closed to avoid the error like connection leakage`

## Using connection pool

**HikariCP**

* dependence in pom.xml：
```xml
<dependency>
    <groupId>com.zaxxer</groupId>
    <artifactId>HikariCP</artifactId>
    <version>3.4.1</version>
</dependency>
```

* Examples：
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
> The close() method will not close the connection from HikariDataSource.getConnection(). Instead, the connection is put back to the connection pool. 
> More instructions can refer to [User Guide][5]

**Druid**

* dependency in pom.xml：

```xml
<dependency>
    <groupId>com.alibaba</groupId>
    <artifactId>druid</artifactId>
    <version>1.1.20</version>
</dependency>
```

* Examples：
```java
public static void main(String[] args) throws Exception {
    Properties properties = new Properties();
    properties.put("driverClassName","com.taosdata.jdbc.TSDBDriver");
    properties.put("url","jdbc:TAOS://127.0.0.1:6030/log");
    properties.put("username","root");
    properties.put("password","taosdata");

    properties.put("maxActive","10"); //maximum number of connection in the pool
    properties.put("initialSize","3");//initial number of connection
    properties.put("maxWait","10000");//maximum wait milliseconds for get connection from pool
    properties.put("minIdle","3");//minimum number of connection in the pool

    properties.put("timeBetweenEvictionRunsMillis","3000");// the interval milliseconds to test connection

    properties.put("minEvictableIdleTimeMillis","60000");//the minimum milliseconds to keep idle
    properties.put("maxEvictableIdleTimeMillis","90000");//the maximum milliseconds to keep idle

    properties.put("validationQuery","describe log.dn"); //validation query
    properties.put("testWhileIdle","true"); // test connection while idle
    properties.put("testOnBorrow","false"); // don't need while testWhileIdle is true
    properties.put("testOnReturn","false"); // don't need while testWhileIdle is true
    
    //create druid datasource
    DataSource ds = DruidDataSourceFactory.createDataSource(properties);
    Connection  connection = ds.getConnection(); // get connection
    Statement statement = connection.createStatement(); // get statement

    //query or insert 
    // ...

    connection.close(); // put back to conneciton pool
}
```
> More instructions can refer to [User Guide][6]

**Notice**
* TDengine `v1.6.4.1` provides a function `select server_status()` to check heartbeat. It is highly recommended to use this function for `Validation Query`.

As follows，`1` will be returned if `select server_status()` is successfully executed。
```shell
taos> select server_status();
server_status()|
================
1              |
Query OK, 1 row(s) in set (0.000141s)
```

## Integrated with framework

* Please refer to [SpringJdbcTemplate][11] if using taos-jdbcdriver in Spring JdbcTemplate
* Please refer to [springbootdemo][12] if using taos-jdbcdriver in Spring JdbcTemplate

## FAQ

* java.lang.UnsatisfiedLinkError: no taos in java.library.path
  
  **Cause**：The application program cannot find Library function _taos_
  
  **Answer**：Copy `C:\TDengine\driver\taos.dll` to `C:\Windows\System32\` on Windows and make a soft link through ` ln -s /usr/local/taos/driver/libtaos.so.x.x.x.x /usr/lib/libtaos.so` on Linux.
  
* java.lang.UnsatisfiedLinkError: taos.dll Can't load AMD 64 bit on a IA 32-bit platform
  
  **Cause**：Currently TDengine only support 64bit JDK
  
  **Answer**：re-install 64bit JDK.

* For other questions, please refer to [Issues][7]


## Python Connector

### Pre-requirement
* TDengine installed, TDengine-client installed if on Windows [(Windows TDengine client installation)](https://www.taosdata.com/cn/documentation/connector/#Windows客户端及程序接口)
* python 2.7 or >= 3.4
* pip installed

### Installation
#### Linux

Users can find python client packages in our source code directory _src/connector/python_. There are two directories corresponding to two python versions. Please choose the correct package to install. Users can use _pip_ command to install:

```cmd
pip install src/connector/python/linux/python3/
```

or

```
pip install src/connector/python/linux/python2/
```
#### Windows
Assumed the Windows TDengine client has been installed , copy the file "C:\TDengine\driver\taos.dll" to the folder "C:\windows\system32", and then enter the _cmd_ Windows command interface
```
cd C:\TDengine\connector\python\windows
pip install python3\
```
or
```
cd C:\TDengine\connector\python\windows
pip install python2\
```
*If _pip_ command is not installed on the system, users can choose to install pip or just copy the _taos_ directory in the python client directory to the application directory to use.

### Usage
#### Examples
* import TDengine module

```python
import taos 
```
* get the connection
```python
conn = taos.connect(host="127.0.0.1", user="root", password="taosdata", config="/etc/taos")
c1 = conn.cursor()
```
*<em>host</em> is the IP of TDengine server, and <em>config</em> is the directory where exists the TDengine client configure file
* insert records into the database
```python
import datetime
 
# create a database
c1.execute('create database db')
c1.execute('use db')
# create a table
c1.execute('create table tb (ts timestamp, temperature int, humidity float)')
# insert a record
start_time = datetime.datetime(2019, 11, 1)
affected_rows = c1.execute('insert into tb values (\'%s\', 0, 0.0)' %start_time)
# insert multiple records in a batch
time_interval = datetime.timedelta(seconds=60)
sqlcmd = ['insert into tb values']
for irow in range(1,11):
  start_time += time_interval
  sqlcmd.append('(\'%s\', %d, %f)' %(start_time, irow, irow*1.2))
affected_rows = c1.execute(' '.join(sqlcmd))
```
* query the database
```python
c1.execute('select * from tb')
# fetch all returned results
data = c1.fetchall()
# data is a list of returned rows with each row being a tuple
numOfRows = c1.rowcount
numOfCols = len(c1.description)
for irow in range(numOfRows):
  print("Row%d: ts=%s, temperature=%d, humidity=%f" %(irow, data[irow][0], data[irow][1],data[irow][2]))
  
# use the cursor as an iterator to retrieve all returned results
c1.execute('select * from tb')
for data in c1:
  print("ts=%s, temperature=%d, humidity=%f" %(data[0], data[1],data[2])
```

* create a subscription
```python
# Create a subscription with topic 'test' and consumption interval 1000ms.
# The first argument is True means to restart the subscription;
# if the subscription with topic 'test' has already been created, then pass
# False to this argument means to continue the existing subscription.
sub = conn.subscribe(True, "test", "select * from meters;", 1000)
```

* consume a subscription
```python
data = sub.consume()
for d in data:
    print(d)
```

* close the subscription
```python
sub.close()
```

* close the connection
```python
c1.close()
conn.close()
```
#### Help information

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

TDengine also provides a Go client package named _taosSql_ for users to access TDengine with Go. The package is in _/usr/local/taos/connector/go/src/taosSql_ by default if you installed TDengine. Users can copy the directory _/usr/local/taos/connector/go/src/taosSql_ to the _src_ directory of your project and import the package in the source code for use.

```Go
import (
    "database/sql"
    _ "taosSql"
)
```

The _taosSql_ package is in _cgo_ form, which calls TDengine C/C++ sync interfaces. So a connection is allowed to be used by one thread at the same time. Users can open multiple connections for multi-thread operations.

Please refer the the demo code in the package for more information.

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

Async queries can be performed using the same functions such as `cursor.execute`, `TaosQuery.execute`, but now with `_a` appended to them.

Say you want to execute an two async query on two seperate tables, using `cursor.query`, you can do that and get a TaosQuery object, which upon executing with the `execute_a` function, returns a promise that resolves with a TaosResult object.

```javascript
var promise1 = cursor.query('select count(*), avg(v1), avg(v2) from meter1;').execute_a()
var promise2 = cursor.query('select count(*), avg(v1), avg(v2) from meter2;').execute_a();
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

[1]: https://search.maven.org/artifact/com.taosdata.jdbc/taos-jdbcdriver
[2]: https://mvnrepository.com/artifact/com.taosdata.jdbc/taos-jdbcdriver
[3]: https://github.com/taosdata/TDengine
[4]: https://www.taosdata.com/blog/2019/12/03/jdbcdriver%e6%89%be%e4%b8%8d%e5%88%b0%e5%8a%a8%e6%80%81%e9%93%be%e6%8e%a5%e5%ba%93/
[5]: https://github.com/brettwooldridge/HikariCP
[6]: https://github.com/alibaba/druid
[7]: https://github.com/taosdata/TDengine/issues
[8]: https://search.maven.org/artifact/com.taosdata.jdbc/taos-jdbcdriver
[9]: https://mvnrepository.com/artifact/com.taosdata.jdbc/taos-jdbcdriver
[10]: https://maven.aliyun.com/mvn/search
[11]:  https://github.com/taosdata/TDengine/tree/develop/tests/examples/JDBC/SpringJdbcTemplate
[12]: https://github.com/taosdata/TDengine/tree/develop/tests/examples/JDBC/springbootdemo
[13]: https://www.taosdata.com/cn/documentation/administrator/#%E5%AE%A2%E6%88%B7%E7%AB%AF%E9%85%8D%E7%BD%AE
[14]: https://www.taosdata.com/cn/documentation/connector/#Windows%E5%AE%A2%E6%88%B7%E7%AB%AF%E5%8F%8A%E7%A8%8B%E5%BA%8F%E6%8E%A5%E5%8F%A3
[15]: https://www.taosdata.com/cn/getting-started/#%E5%BF%AB%E9%80%9F%E4%B8%8A%E6%89%8B
