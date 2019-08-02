# TDengine connectors

TDengine provides many connectors for development, including C/C++, JAVA, Python, RESTful, Go, Node.JS, etc.

## C/C++ API

C/C++ APIs are similar to the MySQL APIs. Applications should include TDengine head file _taos.h_ to use C/C++ APIs by adding the following line in code:
```C
#include <taos.h>
```
Make sure TDengine library _libtaos.so_ is installed and use _-ltaos_ option to link the library when compiling. The return values of all APIs are _-1_ or _NULL_ for failure.


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

For the time being, TDengine supports subscription on one table. It is implemented through periodic pulling from a TDengine server. 

- `TAOS_SUB *taos_subscribe(char *host, char *user, char *pass, char      *db, char *table, int64_t time, int mseconds)`
  The API is used to start a subscription session by given a handle. The parameters required are _host_ (IP address of a TDenginer server), _user_ (username), _pass_ (password), _db_ (database to use), _table_ (table name to subscribe), _time_ (start time to subscribe, 0 for now), _mseconds_ (pulling period). If failed to open a subscription session, a _NULL_ pointer is returned.


- `TAOS_ROW taos_consume(TAOS_SUB *tsub)`
  The API used to get the new data from a TDengine server. It should be put in an infinite loop. The parameter _tsub_ is the handle returned by _taos_subscribe_. If new data are updated, the API will return a row of the result. Otherwise, the API is blocked until new data arrives. If _NULL_ pointer is returned, it means an error occurs.


- `void taos_unsubscribe(TAOS_SUB *tsub)`
  Stop a subscription session by the handle returned by _taos_subscribe_.


- `int taos_num_subfields(TAOS_SUB *tsub)`
  The API used to get the number of fields in a row.


- `TAOS_FIELD *taos_fetch_subfields(TAOS_SUB *tsub)`
  The API used to get the description of each column.

##  Java Connector

### JDBC Interface

TDengine provides a JDBC driver `taos-jdbcdriver-x.x.x.jar` for Enterprise Java developers. TDengine's JDBC Driver is implemented as a subset of the standard JDBC 3.0 Specification and supports the most common Java development frameworks. The driver is currently not published to the online dependency repositories such as Maven Center Repository, and users should manually add the `.jar` file to their local dependency repository.

Please note the JDBC driver itself relies on a native library written in C. On a Linux OS, the driver relies on a `libtaos.so` native library, where .so stands for "Shared Object". After the successful installation of TDengine on Linux, `libtaos.so` should be automatically copied to `/usr/local/lib/taos` and added to the system's default search path. On a Windows OS, the driver relies on a `taos.dll` native library, where .dll stands for "Dynamic Link Library". After the successful installation of the TDengine client on Windows, the `taos-jdbcdriver.jar` file can be found in `C:/TDengine/driver/JDBC`; the `taos.dll` file can be found in `C:/TDengine/driver/C` and should have been automatically copied to the system's searching path `C:/Windows/System32`. 

Developers can refer to the Oracle's official JDBC API documentation for detailed usage on classes and methods. However, there are some differences of connection configurations and supported methods in the driver implementation between TDengine and traditional relational databases. 

For database connections, TDengine's JDBC driver has the following configurable parameters in the JDBC URL. The standard format of a TDengine JDBC URL is:

`jdbc:TSDB://{host_ip}:{port}/{database_name}?[user={user}|&password={password}|&charset={charset}|&cfgdir={config_dir}|&locale={locale}|&timezone={timezone}]`

where `{}` marks the required parameters and `[]` marks the optional. The usage of each parameter is pretty straightforward:

* user - login user name for TDengine; by default, it's `root`
* password - login password; by default, it's `taosdata`
* charset - the client-side charset; by default, it's the operation system's charset
* cfgdir - the directory of TDengine client configuration file; by default it's `/etc/taos` on Linux and `C:\TDengine/cfg` on Windows
* locale - the language environment of TDengine client; by default, it's the operation system's locale
* timezone - the timezone of the TDengine client; by default, it's the operation system's timezone 

All parameters can be configured at the time when creating a connection using the java.sql.DriverManager class, for example:

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import com.taosdata.jdbc.TSDBDriver;

public Connection getConn() throws Exception{
    Class.forName("com.taosdata.jdbc.TSDBDriver");
  String jdbcUrl = "jdbc:TAOS://127.0.0.1:0/db?user=root&password=taosdata";
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

Except `cfgdir`, all the parameters listed above can also be configured in the configuration file. The properties specified when calling DriverManager.getConnection() has the highest priority among all configuration methods. The JDBC URL has the second-highest priority, and the configuration file has the lowest priority. The explicitly configured parameters in a method with higher priorities always overwrite that same parameter configured in methods with lower priorities. For example, if `charset` is explicitly configured as "UTF-8" in the JDBC URL and "GKB" in the `taos.cfg` file, then "UTF-8" will be used.

Although the JDBC driver is implemented following the JDBC standard as much as possible, there are major differences between TDengine and traditional databases in terms of data models that lead to the differences in the driver implementation. Here is a list of head-ups for developers who have plenty of experience on traditional databases but little on TDengine:

*  TDengine does NOT support updating or deleting a specific record, which leads to some unsupported methods in the JDBC driver
* TDengine currently does not support `join` or `union` operations, and thus, is lack of support for associated methods in the JDBC driver
* TDengine supports batch insertions which are controlled at the level of SQL statement writing instead of API calls
* TDengine doesn't support nested queries and neither does the JDBC driver. Thus for each established connection to TDengine, there should be only one open result set associated with it

All the error codes and error messages can be found in `TSDBError.java` . For a more detailed coding example, please refer to the demo project `JDBCDemo` in TDengine's code examples. 

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

