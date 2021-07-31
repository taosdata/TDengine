# Connectors

TDengine provides many connectors for development, including C/C++, JAVA, Python, RESTful, Go, Node.JS, etc.

![image-connector](page://images/connector.png)

At present, TDengine connectors support a wide range of platforms, including hardware platforms such as X64/X86/ARM64/ARM32/MIPS/Alpha, and development environments such as Linux/Win64/Win32. The comparison matrix is as follows:

| **CPU**     | **X64 64bit** | **X64 64bit** | **X64 64bit** | **X86 32bit** | **ARM64** | **ARM32** | **MIPS Godson** | **Alpha Sunway** | **X64 TimecomTech** |
| ----------- | ------------- | ------------- | ------------- | ------------- | --------- | --------- | --------------- | ----------------- | ------------------- |
| **OS**      | **Linux**     | **Win64**     | **Win32**     | **Win32**     | **Linux** | **Linux** | **Linux**       | **Linux**         | **Linux**           |
| **C/C++**   | ●             | ●             | ●             | ○             | ●         | ●         | ○               | ○                 | ○                   |
| **JDBC**    | ●             | ●             | ●             | ○             | ●         | ●         | ○               | ○                 | ○                   |
| **Python**  | ●             | ●             | ●             | ○             | ●         | ●         | ○               | --                | ○                   |
| **Go**      | ●             | ●             | ●             | ○             | ●         | ●         | ○               | --                | --                  |
| **NodeJs**  | ●             | ●             | ○             | ○             | ●         | ●         | ○               | --                | --                  |
| **C#**      | ○             | ●             | ●             | ○             | ○         | ○         | ○               | --                | --                  |
| **RESTful** | ●             | ●             | ●             | ●             | ●         | ●         | ○               | ○                 | ○                   |

Note: ● stands for that has been verified by official tests; ○ stands for that has been verified by unofficial tests. 

Note:

- To access the TDengine database through connectors (except RESTful) in the system without TDengine server software, it is necessary to install the corresponding version of the client installation package to make the application driver (the file name is [libtaos.so](http://libtaos.so/) in Linux system and taos.dll in Windows system) installed in the system, otherwise, the error that the corresponding library file cannot be found will occur.
- All APIs that execute SQL statements, such as `tao_query`, `taos_query_a`, `taos_subscribe` in C/C++ Connector, and APIs corresponding to them in other languages, can only execute one SQL statement at a time. If the actual parameters contain multiple statements, their behavior is undefined.
- Users upgrading to TDengine 2.0. 8.0 must update the JDBC connection. TDengine must upgrade taos-jdbcdriver to 2.0.12 and above.
- No matter which programming language connector is selected, TDengine version 2.0 and above recommends that each thread of database application establish an independent connection or establish a connection pool based on threads to avoid mutual interference between threads of "USE statement" state variables in the connection (but query and write operations of the connection are thread-safe).

## <a class="anchor" id="driver"></a> Steps of Connector Driver Installation

The server should already have the TDengine server package installed. The connector driver installation steps are as follows:

**Linux**

**1. Download from TAOS Data website(https://www.taosdata.com/cn/all-downloads/)**

* X64 hardware environment: TDengine-client-2.x.x.x-Linux-x64.tar.gz
* ARM64 hardware environment: TDengine-client-2.x.x.x-Linux-aarch64.tar.gz
* ARM32 hardware environment: TDengine-client-2.x.x.x-Linux-aarch32.tar.gz

**2. Unzip the package**

Place the package in any directory that current user can read/write, and then execute following command:

`tar -xzvf TDengine-client-xxxxxxxxx.tar.gz`

Where xxxxxx needs to be replaced with you actual version as a string.

**3. Execute installation script**

After extracting the package, you will see the following files (directories) in the extracting directory:

*install_client. sh*: Installation script for application driver

*taos.tar.gz*: Application driver installation package 

*driver*: TDengine application driver 

*connector*: Connectors for various programming languages (go/grafanaplugin/nodejs/python/JDBC) 

*Examples*: Sample programs for various programming languages (C/C #/go/JDBC/MATLAB/python/R)

Run install_client.sh to install.

**4. Configure taos.cfg**

Edit the taos.cfg file (default path/etc/taos/taos.cfg) and change firstEP to End Point of the TDengine server, for example: [h1.taos.com](http://h1.taos.com/):6030.

**Tip: If no TDengine service deployed in this machine, but only the application driver is installed, only firstEP needs to be configured in taos.cfg, and FQDN does not.**

**Windows x64/x86**

**1. Download from TAOS Data website(https://www.taosdata.com/cn/all-downloads/)**

* X64 hardware environment: TDengine-client-2.X.X.X-Windows-x64.exe
* X86 hardware environment: TDengine-client-2.X.X.X-Windows-x86.exe

**2. Execute installation, select default values as prompted to complete**

**3. Installation path**

Default installation path is: C:\TDengine, with following files(directories):

*taos.exe*: taos shell command line program

*cfg*: configuration file directory 

*driver*: application driver dynamic link library 

*examples*: sample program bash/C/C #/go/JDBC/Python/Node.js 

*include*: header file 

*log*: log file 

*unins000. exe*: uninstall program

**4. Configure taos.cfg**

Edit the taos.cfg file (default path/etc/taos/taos.cfg) and change firstEP to End Point of the TDengine server, for example: [h1.taos.com](http://h1.taos.com/):6030.

**Note:**

**1. If you use FQDN to connect to the server, you must confirm that the DNS of the local network environment has been configured, or add FQDN addressing records in the hosts file. For example, edit C:\ Windows\ system32\ drivers\ etc\ hosts, and add the following record: 192.168. 1.99 [h1.taos.com](http://h1.taos.com/)**

**2. Uninstall: Run unins000. exe to uninstall the TDengine application driver.**

**Installation verification**

After the above installation and configuration completed, and confirm that the TDengine service has started running normally, the taos client can be logged in at this time.

**Linux environment:**

If you execute taos directly under Linux shell, you should be able to connect to tdengine service normally and jump to taos shell interface. For Example:

```mysql
$ taos     
Welcome to the TDengine shell from Linux, Client  Version:2.0.5.0  
Copyright (c) 2017 by TAOS Data, Inc. All rights  reserved.     
taos> show databases;           
name       |   created_time    |   ntables  |  vgroups   | replica | quorum | days |    keep1,keep2,keep(D)   | cache(MB)|   blocks  |  minrows   |  maxrows  | wallevel |  fsync    | comp | precision |    status  |  
=========================================================================================================================================================================================================================  
test       | 2020-10-14  10:35:48.617 |     10 |      1 |    1 |   1 |     2 | 3650,3650,3650        |     16|      6 |     100 |    4096 |    1 |    3000 |  2 | ms      | ready    |   
log        | 2020-10-12  09:08:21.651 |      4 |      1 |    1 |   1 |   10 | 30,30,30               |      1|      3 |     100 |    4096 |    1 |    3000 |  2 | us    | ready    |  
Query OK, 2 row(s) in set (0.001198s)     
taos>  
```

**Windows (x64/x86) environment:**

Under cmd, enter the c:\ tdengine directory and directly execute taos.exe, and you should be able to connect to tdengine service normally and jump to taos shell interface. For example:

```mysql
  C:\TDengine>taos     
  Welcome to the TDengine  shell from Linux, Client Version:2.0.5.0  
  Copyright (c) 2017 by  TAOS Data, Inc. All rights reserved.     
  taos> show  databases;         
  name       |   created_time    |   ntables  |  vgroups   | replica | quorum | days |    keep1,keep2,keep(D)   | cache(MB)   |  blocks  |   minrows  |  maxrows   | wallevel |  fsync  | comp | precision |  status    |  
  ===================================================================================================================================================================================================================================================================   
  test       | 2020-10-14  10:35:48.617 |     10 |      1 |    1 |   1 |     2 | 3650,3650,3650        |     16 |      6 |     100 |    4096 |    1 |    3000 |  2 | ms    | ready    |   
  log        | 2020-10-12  09:08:21.651 |      4 |      1 |    1 |   1 |    10 | 30,30,30              |      1 |      3 |     100 |    4096 |    1 |    3000 |  2 | us    | ready    |  
  Query OK, 2 row(s) in  set (0.045000s)     
  taos>  
```

## <a class="anchor" id="c-cpp"></a> C/C++ Connector

**Systems supported by C/C++ connectors as follows:**

| **CPU Type**         | **x64****（****64bit****）** |         |         | **ARM64** | **ARM32**          |
| -------------------- | ---------------------------- | ------- | ------- | --------- | ------------------ |
| **OS Type**          | Linux                        | Win64   | Win32   | Linux     | Linux              |
| **Supported or Not** | Yes                          | **Yes** | **Yes** | **Yes**   | **In development** |

The C/C++ API is similar to MySQL's C API. When application use it, it needs to include the TDengine header file taos.h (after installed, it is located in/usr/local/taos/include):

```C
#include <taos.h>
```

Note:

- The TDengine dynamic library needs to be linked at compiling. The library in Linux is [libtaos.so](http://libtaos.so/), which installed at/usr/local/taos/driver. By Windows, it is taos.dll and installed at C:\ TDengine.
- Unless otherwise specified, when the return value of API is an integer, 0 represents success, others are error codes representing the cause of failure, and when the return value is a pointer, NULL represents failure.

More sample codes for using C/C++ connectors, please visit https://github.com/taosdata/TDengine/tree/develop/tests/examples/c.

### Basic API

The basic API is used to create database connections and provide a runtime environment for the execution of other APIs.

- `void taos_init()`

Initialize the running environment. If the application does not actively call the API, the API will be automatically called when the application call taos_connect, so the application generally does not need to call the API manually.

- `void taos_cleanup()`

Clean up the running environment and call this API before the application exits.

- `int taos_options(TSDB_OPTION option, const void * arg, ...)`

Set client options, currently only time zone setting (_TSDB_OPTIONTIMEZONE) and encoding setting (_TSDB_OPTIONLOCALE) are supported. The time zone and encoding default to the current operating system settings.

- `char *taos_get_client_info()`

Get version information of the client.

- `TAOS *taos_connect(const char *host, const char *user, const char *pass, const char *db, int port)`

Create a database connection and initialize the connection context. The parameters that need to be provided by user include:

* host: FQDN used by TDengine to manage the master node
* user: User name
* pass: Password
* db: Database name. If user does not provide it, it can be connected normally, means user can create a new database through this connection. If user provides a database name, means the user has created the database and the database is used by default
* port: Port number

A null return value indicates a failure. The application needs to save the returned parameters for subsequent API calls.

- `char *taos_get_server_info(TAOS *taos)`

Get version information of the server-side.

- `int taos_select_db(TAOS *taos, const char *db)`

Set the current default database to db.

- `void taos_close(TAOS *taos)`

Close the connection, where `taos` is the pointer returned by `taos_connect` function.

### Synchronous query API

Traditional database operation APIs all make synchronous operations. After the application calls an API, it remains blocked until the server returns the result. TDengine supports the following APIs:

- `TAOS_RES* taos_query(TAOS *taos, const char *sql)`

This API is used to execute SQL statements, which can be DQL, DML or DDL statements. Where `taos` parameter is a pointer obtained through `taos_connect`. You can't judge whether the execution result fails by whether the return value is NULL, but to use `taos_errno` function to parse the error code in the result set.

- `int taos_result_precision(TAOS_RES *res)`

The precision of the timestamp field in the returned result set, `0` for milliseconds, `1` for microseconds, and `2` for nanoseconds.

- `TAOS_ROW taos_fetch_row(TAOS_RES *res)`

Get the data in the query result set by rows.

- `int taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows)`

The data in the query result set is obtained in batch, and the return value is the number of rows of the obtained data.

- `int taos_num_fields(TAOS_RES *res)` 和 `int taos_field_count(TAOS_RES *res)`

The two APIs are equivalent, and are used to get the number of columns in the query result set.

- `int* taos_fetch_lengths(TAOS_RES *res)`

Get the length of each field in the result set. The return value is an array whose length is the number of columns in the result set.

- `int taos_affected_rows(TAOS_RES *res)`

Get the number of rows affected by the executed SQL statement.

- `TAOS_FIELD *taos_fetch_fields(TAOS_RES *res)`

Get the attributes (data type, name, number of bytes) of each column of data in the query result set, which can be used in conjunction with `taos_num_files` to parse the data of a tuple (one row) returned by `taos_fetch_row`. The structure of `TAOS_FIELD` is as follows:

```c
typedef struct taosField {
  char     name[65];  // Column name
  uint8_t  type;      // Data type
  int16_t  bytes;     // Number of bytes
} TAOS_FIELD;
```

- `void taos_stop_query(TAOS_RES *res)`

Stop the execution of a query.

- `void taos_free_result(TAOS_RES *res)`

Release the query result set and related resources. After the query is completed, be sure to call the API to release resources, otherwise it may lead to application memory leakage. However, it should also be noted that after releasing resources, if you call functions such as `taos_consume` to obtain query results, it will lead the application to Crash.

- `char *taos_errstr(TAOS_RES *res)`

Get the reason why the last API call failed, and the return value is a string.

- `char *taos_errno(TAOS_RES *res)`

Get the reason why the last API call failed, and the return value is the error code.

**Note:** TDengine 2.0 and above recommends that each thread of a database application establish an independent connection or establish a connection pool based on threads. It is not recommended to pass the connection (TAOS\*) structure to different threads for sharing in applications. Query and write operations based on TAOS structure have multithread safety, but state variables such as "USE statement" may interfere with each other among threads. In addition, C connector can dynamically establish new database-oriented connections according to requirements (this process is not visible to users), and it is recommended to call `taos_close` to close the connection only when the program finally exits.

### Asynchronous query API

In addition to synchronous API, TDengine also provides higher performance asynchronous call API to handle data insertion and query operations. Under the same software and hardware environment, asynchronous API processes data insertion 2 ~ 4 times faster than synchronous API. Asynchronous API adopts a non-blocking call mode and returns immediately before the system really completes a given database operation. The calling thread can handle other work, thus improving the performance of the whole application. Asynchronous API has outstanding advantages in the case of poor network delay.

Asynchronous APIs all need applications to provide corresponding callback function. The callback function parameters are set as follows: the first two parameters are consistent, and the third parameter depends on different APIs. The first parameter param is provided to the system when the application calls the asynchronous API. When used for callback, the application can retrieve the context of the specific operation, depending on the specific implementation. The second parameter is the result set of SQL operation. If it is empty, such as insert operation, it means that there is no record returned. If it is not empty, such as select operation, it means that there is record returned.

Asynchronous APIs have relatively high requirements for users, who can selectively use them according to specific application scenarios. Here are three important asynchronous APIs:

- `void taos_query_a(TAOS *taos, const char *sql, void (*fp)(void *param, TAOS_RES *, int code), void *param);`
   Execute SQL statement asynchronously.

   * taos: The database connection returned by calling `taos_connect`
   * sql: The SQL statement needed to execute
   * fp: User-defined callback function, whose third parameter `code` is used to indicate whether the operation is successful, `0` for success, and negative number for failure (call `taos_errstr` to get the reason for failure). When defining the callback function, it mainly handles the second parameter `TAOS_RES *`, which is the result set returned by the query
  * param：the parameter for the callback

- `void taos_fetch_rows_a(TAOS_RES *res, void (*fp)(void *param, TAOS_RES *, int numOfRows), void *param);`
  Get the result set of asynchronous queries in batch, which can only be used with `taos_query_a`. Within:

  * res: The result set returned when backcall `taos_query_a`
  * fp: Callback function. Its parameter `param` is a user-definable parameter construct passed to the callback function; `numOfRows` is the number of rows of data obtained (not a function of the entire query result set). In the callback function, applications can get each row of the batch records by calling `taos_fetch_rows` forward iteration. After reading all the records in a block, the application needs to continue calling `taos_fetch_rows_a` in the callback function to obtain the next batch of records for processing until the number of records returned (`numOfRows`) is zero (the result is returned) or the number of records is negative (the query fails).

The asynchronous APIs of TDengine all use non-blocking calling mode. Applications can use multithreading to open multiple tables at the same time, and can query or insert to each open table at the same time. It should be pointed out that the **application client must ensure that the operation on the same table is completely serialized**, that is, when the insertion or query operation on the same table is not completed (when no result returned), the second insertion or query operation cannot be performed.



<a class="anchor" id="stmt"></a>

### Parameter binding API

In addition to calling `taos_query` directly for queries, TDengine also provides a Prepare API that supports parameter binding. Like MySQL, these APIs currently only support using question mark `?` to represent the parameters to be bound, as follows:

- `TAOS_STMT* taos_stmt_init(TAOS *taos)`

Create a `TAOS_STMT` object for calling later.

- `int taos_stmt_prepare(TAOS_STMT *stmt, const char *sql, unsigned long length)`

Parse a SQL statement and bind the parsing result and parameter information to STMT. If the parameter length is greater than 0, this parameter will be used as the length of the SQL statement. If it is equal to 0, the length of the SQL statement will be automatically judged.

- `int taos_stmt_bind_param(TAOS_STMT *stmt, TAOS_BIND *bind)`

For parameter binding, bind points to an array, and it is necessary to ensure that the number and order of elements in this array are exactly the same as the parameters in sql statement. TAOS_BIND is used in the same way as MYSQL_BIND in MySQL and is defined as follows:

```c
typedef struct TAOS_BIND {
  int            buffer_type;
  void *         buffer;
  unsigned long  buffer_length;  // Not in use
  unsigned long *length;
  int *          is_null;
  int            is_unsigned;    // Not in use
  int *          error;          // Not in use
} TAOS_BIND;
```

Add the current bound parameters to the batch. After calling this function, you can call `taos_stmt_bind_param` again to bind the new parameters. It should be noted that this function only supports insert/import statements, and if it is other SQL statements such as select, it will return errors.

- `int taos_stmt_execute(TAOS_STMT *stmt)`

Execute the prepared statement. At the moment, a statement can only be executed once.

- `TAOS_RES* taos_stmt_use_result(TAOS_STMT *stmt)`

Gets the result set of the statement. The result set is used in the same way as when calling nonparameterized. After using it, `taos_free_result` should be called to release resources.

- `int taos_stmt_close(TAOS_STMT *stmt)`

Execution completed, release all resources.

### Continuous query interface

TDengine provides time-driven real-time stream computing APIs. You can perform various real-time aggregation calculation operations on tables (data streams) of one or more databases at regular intervals. The operation is simple, only APIs for opening and closing streams. The details are as follows:

- `TAOS_STREAM *taos_open_stream(TAOS *taos, const char *sql, void (*fp)(void *param, TAOS_RES *, TAOS_ROW row), int64_t stime, void *param, void (*callback)(void *))`

This API is used to create data streams where:

  * taos: Database connection established
  * sql: SQL query statement (query statement only)
  * fp: user-defined callback function pointer. After each stream computing is completed, TDengine passes the query result (TAOS_ROW), query status (TAOS_RES), and user-defined parameters (PARAM) to the callback function. In the callback function, the user can use taos_num_fields to obtain the number of columns in the result set, and taos_fetch_fields to obtain the type of data in each column of the result set.
  * stime: The time when stream computing starts. If it is 0, it means starting from now. If it is not zero, it means starting from the specified time (the number of milliseconds from 1970/1/1 UTC time).
  * param: It is a parameter provided by the application for callback. During callback, the parameter is provided to the application
  * callback: The second callback function is called when the continuous query stop automatically.

The return value is NULL, indicating creation failed; the return value is not NULL, indicating creation successful.

- `void taos_close_stream (TAOS_STREAM *tstr)`

Close the data flow, where the parameter provided is the return value of `taos_open_stream`. When the user stops stream computing, be sure to close the data flow.

### Data subscription interface

The subscription API currently supports subscribing to one or more tables and continuously obtaining the latest data written to the tables through regular polling.

- `TAOS_SUB *taos_subscribe(TAOS* taos, int restart, const char* topic, const char *sql, TAOS_SUBSCRIBE_CALLBACK fp, void *param, int interval)`

This function is for starting the subscription service, returning the subscription object in case of success, and NULL in case of failure. Its parameters are:

  * taos: Database connection established
  * Restart: If the subscription already exists, do you want to start over or continue with the previous subscription
  * Topic: Subject (that is, name) of the subscription. This parameter is the unique identification of the subscription
  * sql: The query statement subscribed. This statement can only be a select statement. It should only query the original data, and can only query the data in positive time sequence
  * fp: The callback function when the query result is received (the function prototype will be introduced later). It is only used when calling asynchronously, and this parameter should be passed to NULL when calling synchronously
  * param: The additional parameter when calling the callback function, which is passed to the callback function as it is by the system API without any processing
  * interval: Polling period in milliseconds. During asynchronous call, the callback function will be called periodically according to this parameter; In order to avoid affecting system performance, it is not recommended to set this parameter too small; When calling synchronously, if the interval between two calls to taos_consume is less than this period, the API will block until the interval exceeds this period.

- `typedef void (*TAOS_SUBSCRIBE_CALLBACK)(TAOS_SUB* tsub, TAOS_RES *res, void* param, int code)`

In asynchronous mode, the prototype of the callback function has the following parameters:

  * tsub: Subscription object
  * res: Query the result set. Note that there may be no records in the result set
  * param: Additional parameters supplied by the client when  `taos_subscribe` is called
  * code: Error code

- `TAOS_RES *taos_consume(TAOS_SUB *tsub)`

In synchronous mode, this function is used to get the results of subscription. The user application places it in a loop. If the interval between two calls to `taos_consume` is less than the polling cycle of the subscription, the API will block until the interval exceeds this cycle. If a new record arrives in the database, the API will return the latest record, otherwise it will return an empty result set with no records. If the return value is NULL, it indicates a system error. In asynchronous mode, user program should not call this API.

- `void taos_unsubscribe(TAOS_SUB *tsub, int keepProgress)`

Unsubscribe. If the parameter `keepProgress` is not 0, the API will keep the progress information of subscription, and the subsequent call to `taos_subscribe` can continue based on this progress; otherwise, the progress information will be deleted and the data can only be read again.

## <a class="anchor" id="python"></a>Python Connector

See [video tutorials](https://www.taosdata.com/blog/2020/11/11/1963.html) for the use of Python connectors.

### Installation preparation

- For application driver installation, please refer to [steps of connector driver installation](https://www.taosdata.com/en/documentation/connector#driver)
- python 2.7 or >= 3.4 installed
- pip or pip3 installed

### Python client installation

#### Linux

Users can find the connector package for python2 and python3 in the source code src/connector/python (or tar.gz/connector/python) folder. Users can install it through `pip` command:

`pip install src/connector/python/linux/python2/`

or

 `pip3 install src/connector/python/linux/python3/`

#### Windows

With Windows TDengine client installed, copy the file "C:\TDengine\driver\taos.dll" to the "C:\ windows\ system32" directory and enter the Windows <em>cmd</em> command line interface:

```cmd
cd C:\TDengine\connector\python
python -m pip install .
```

- If there is no `pip` command on the machine, the user can copy the taos folder under src/connector/python to the application directory for use. For Windows client, after installing the TDengine Windows client, copy C:\ TDengine\driver\taos.dll to the C:\ windows\ system32 directory.

### How to use

#### Code sample

- Import the TDengine client module

```python
import taos
```

- Get the connection and cursor object

```python
conn = taos.connect(host="127.0.0.1", user="root", password="taosdata", config="/etc/taos")
c1 = conn.cursor()
```

- *host* covers all IPs of TDengine server-side, and *config* is the directory where the client configuration files is located
- Write data

```python
import datetime

# Create a database
c1.execute('create database db')
c1.execute('use db')
# Create a table
c1.execute('create table tb (ts timestamp, temperature int, humidity float)')
# Insert data
start_time = datetime.datetime(2019, 11, 1)
affected_rows = c1.execute('insert into tb values (\'%s\', 0, 0.0)' %start_time)
# Insert data in batch
time_interval = datetime.timedelta(seconds=60)
sqlcmd = ['insert into tb values']
for irow in range(1,11):
  start_time += time_interval
  sqlcmd.append('(\'%s\', %d, %f)' %(start_time, irow, irow*1.2))
affected_rows = c1.execute(' '.join(sqlcmd))
```

- Query data

```python
c1.execute('select * from tb')
# pull query result
data = c1.fetchall()
# The result is a list, with each row as an element
numOfRows = c1.rowcount
numOfCols = len(c1.description)
for irow in range(numOfRows):
  print("Row%d: ts=%s, temperature=%d, humidity=%f" %(irow, data[irow][0], data[irow][1],data[irow][2]))

# Use cursor loop directly to pull query result
c1.execute('select * from tb')
for data in c1:
  print("ts=%s, temperature=%d, humidity=%f" %(data[0], data[1],data[2]))
```

- Create subscription

```python
# Create a subscription with the topic ‘test’ and a consumption cycle of 1000 milliseconds
# If the first parameter is True, it means restarting the subscription. If it is False and a subscription with the topic 'test 'has been created before, it means continuing to consume the data of this subscription instead of restarting to consume all the data
sub = conn.subscribe(True, "test", "select * from tb;", 1000)
```

- Consume subscription data

```python
data = sub.consume()
for d in data:
    print(d)
```

- Unsubscription

```python
sub.close()
```

- Close connection

```python
c1.close()
conn.close()
```

#### Using nanosecond in Python connector

So far Python still does not completely support nanosecond type. Please refer to the link 1 and 2. The implementation of the python connector is to return an integer number for nanosecond value rather than datatime type as what ms and us do. The developer needs to handle it themselves. We recommend using pandas to_datetime() function. If Python officially support nanosecond in the future, TAOS Data might be possible to change the interface accordingly, which mean the application need change too.

1. https://stackoverflow.com/questions/10611328/parsing-datetime-strings-containing-nanoseconds
2. https://www.python.org/dev/peps/pep-0564/

#### Helper

Users can directly view the usage information of the module through Python's helper, or refer to the sample program in tests/examples/Python. The following are some common classes and methods:

- *TDengineConnection* class

Refer to help (taos.TDEngineConnection) in python. This class corresponds to a connection between the client and TDengine. In the scenario of client multithreading, it is recommended that each thread apply for an independent connection instance, but not recommended that multiple threads share a connection.

- *TDengineCursor* class

Refer to help (taos.TDengineCursor) in python. This class corresponds to the write and query operations performed by the client. In the scenario of client multithreading, this cursor instance must be kept exclusive to threads and cannot be used by threads, otherwise errors will occur in the returned results.

- *connect* method

Used to generate an instance of taos.TDengineConnection.

### Python client code sample

In tests/examples/python, we provide a sample Python program read_example. py to guide you to design your own write and query program. After installing the corresponding client, introduce the taos class through `import taos`. The steps are as follows:

- Get the `TDengineConnection` object through `taos.connect`, which can be applied for only one by a program and shared among multiple threads.

- Get a new cursor object through the `.cursor ()` method of the `TDengineConnection` object, which must be guaranteed to be exclusive to each thread.

- Execute SQL statements for writing or querying through the `execute()` method of the cursor object.

- If a write statement is executed, `execute` returns the number of rows successfully written affected rows.

- If the query statement is executed, the result set needs to be pulled through the fetchall method after the execution is successful. 

  You can refer to the sample code for specific methods.

## <a class="anchor" id="restful"></a> RESTful Connector

To support the development of various types of platforms, TDengine provides an API that conforms to REST design standards, that is, RESTful API. In order to minimize the learning cost, different from other designs of database RESTful APIs, TDengine directly requests SQL statements contained in BODY through HTTP POST to operate the database, and only needs a URL. See the [video tutorial](https://www.taosdata.com/blog/2020/11/11/1965.html) for the use of RESTful connectors.

###  HTTP request format

```
http://<ip>:<PORT>/rest/sql
```

Parameter description:

- IP: Any host in the cluster
- PORT: httpPort configuration item in the configuration file, defaulting to 6041

For example: [http://192.168.0.1](http://192.168.0.1/): 6041/rest/sql is a URL that points to an IP address of 192.168. 0.1.

The header of HTTP request needs to carry identity authentication information. TDengine supports Basic authentication and custom authentication. Subsequent versions will provide standard and secure digital signature mechanism for identity authentication.

- Custom identity authentication information is as follows (We will introduce <token> later)

```
Authorization: Taosd <TOKEN>
```

- Basic identity authentication information is as follows

```
Authorization: Basic <TOKEN>
```

The BODY of HTTP request is a complete SQL statement. The data table in the SQL statement should provide a database prefix, such as \<db-name>.\<tb-name>. If the table name does not have a database prefix, the system returns an error. Because the HTTP module is just a simple forwarding, there is no current DB concept.

Use curl to initiate an HTTP Request through custom authentication. The syntax is as follows:

```bash
curl -H 'Authorization: Basic <TOKEN>' -d '<SQL>' <ip>:<PORT>/rest/sql
```

or

```bash
curl -u username:password -d '<SQL>' <ip>:<PORT>/rest/sql
```

Where `TOKEN` is the string of `{username}:{password}` encoded by Base64, for example, `root:taosdata` will be encoded as `cm9vdDp0YW9zZGF0YQ==`.

### HTTP return format

The return value is in JSON format, as follows:

```json
{
    "status": "succ",
    "head": ["ts","current", …],
    "column_meta": [["ts",9,8],["current",6,4], …],
    "data": [
        ["2018-10-03 14:38:05.000", 10.3, …],
        ["2018-10-03 14:38:15.000", 12.6, …]
    ],
    "rows": 2
} 
```

Description:

- status: Informs whether the operation results are successful or failed.
- head: The definition of the table, with only one column "affected_rows" if no result set is returned. (Starting from version 2.0. 17, it is recommended not to rely on the head return value to judge the data column type, but to use column_meta. In future versions, head may be removed from the return value.)
- column_meta: Starting with version 2.0. 17, this item is added to the return value to indicate the data type of each column in the data. Each column will be described by three values: column name, column type and type length. For example, ["current", 6, 4] means that the column name is "current"; the column type is 6, that is, float type; the type length is 4, which corresponds to a float represented by 4 bytes. If the column type is binary or nchar, the type length indicates the maximum content length that the column can save, rather than the specific data length in this return value. When the column type is nchar, its type length indicates the number of Unicode characters that can be saved, not bytes.
- data: The specific returned data, rendered line by line, if no result set is returned, then only [[affected_rows]]. The order of the data columns for each row in data is exactly the same as the order of the data columns described in column_meta.
- rows: Indicates the total number of rows of data.

Column types in column_meta:

* 1：BOOL
* 2：TINYINT
* 3：SMALLINT
* 4：INT
* 5：BIGINT
* 6：FLOAT
* 7：DOUBLE
* 8：BINARY
* 9：TIMESTAMP
* 10：NCHAR

### Custom authorization code

The HTTP request requires the authorization code `<TOKEN>` for identification. Authorization codes are usually provided by administrators. Authorization codes can be obtained simply by sending `HTTP GET` requests as follows:

```bash
curl http://<ip>:6041/rest/login/<username>/<password>
```

Where `ip` is the IP address of the TDengine database, `username` is the database user name, `password` is the database password, and the return value is in `JSON` format. The meanings of each field are as follows:

- status: flag bit for request result
- code: code of return value
- desc: Authorization code

Sample to get authorization code:

```bash
curl http://192.168.0.1:6041/rest/login/root/taosdata
```

Return value:

```json
{
  "status": "succ",
  "code": 0,
  "desc": "/KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04"
}
```

### Use case

- Lookup all records of table d1001 in demo database:

```bash
curl -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' -d 'select * from demo.d1001' 192.168.0.1:6041/rest/sql
```

Return value:

```json
{
    "status": "succ",
    "head": ["ts","current","voltage","phase"],
    "column_meta": [["ts",9,8],["current",6,4],["voltage",4,4],["phase",6,4]],
    "data": [
        ["2018-10-03 14:38:05.000",10.3,219,0.31],
        ["2018-10-03 14:38:15.000",12.6,218,0.33]
    ],
    "rows": 2
}
```

- Create a database demo:

```bash
curl -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' -d 'create database demo' 192.168.0.1:6041/rest/sql
```

Return value:

```json
{
    "status": "succ",
    "head": ["affected_rows"],
    "column_meta": [["affected_rows",4,4]],
    "data": [[1]],
    "rows": 1
}
```

### Other cases

### Result set in Unix timestamp

When the HTTP request URL is sqlt, the timestamp of the returned result set will be expressed in Unix timestamp format, for example:

```bash
curl -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' -d 'select * from demo.d1001' 192.168.0.1:6041/rest/sqlt
```

Return value:

```json
{
    "status": "succ",
    "head": ["ts","current","voltage","phase"],
    "column_meta": [["ts",9,8],["current",6,4],["voltage",4,4],["phase",6,4]],
    "data": [
        [1538548685000,10.3,219,0.31],
        [1538548695000,12.6,218,0.33]
    ],
    "rows": 2
}
```

#### Result set in UTC time string

When the HTTP request URL is `sqlutc`, the timestamp of the returned result set will be represented by a UTC time string, for example:

```bash
  curl -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' -d 'select * from demo.t1' 192.168.0.1:6041/rest/sqlutc
```

Return value:

```json
{
    "status": "succ",
    "head": ["ts","current","voltage","phase"],
    "column_meta": [["ts",9,8],["current",6,4],["voltage",4,4],["phase",6,4]],
    "data": [
        ["2018-10-03T14:38:05.000+0800",10.3,219,0.31],
        ["2018-10-03T14:38:15.000+0800",12.6,218,0.33]
    ],
    "rows": 2
}
```

### Important configuration options

Only some configuration parameters related to RESTful interface are listed below. Please refer to the instructions in the configuration file for other system parameters. Note: After the configuration is modified, the taosd service needs to be restarted before it can take effect.

- httpPort: The port number that provides RESTful services externally, which is bound to 6041 by default
- httpMaxThreads: The number of threads started, the default is 2 (starting with version 2.0. 17, the default value is changed to half of the CPU cores and rounded down)
- restfulRowLimit: The maximum number of result sets returned (in JSON format), default 10240
- httpEnableCompress: Compression is not supported by default. Currently, TDengine only supports gzip compression format
- httpdebugflag: Logging switch, 131: error and alarm information only, 135: debugging information, 143: very detailed debugging information, default 131



## <a class="anchor" id="csharp"></a> CSharp Connector

The C # connector supports: Linux 64/Windows x64/Windows x86.

### Installation preparation

- For application driver installation, please refer to the[ steps of installing connector driver](https://www.taosdata.com/en/documentation/connector#driver).
- . NET interface file TDengineDrivercs.cs and reference sample TDengineTest.cs are both located in the Windows client install_directory/examples/C# directory.
- On Windows, C # applications can use the native C interface of TDengine to perform all database operations, and future versions will provide the ORM (Dapper) framework driver.

### Installation verification

Run install_directory/examples/C#/C#Checker/C#Checker.exe

```cmd
cd {install_directory}/examples/C#/C#Checker
csc /optimize *.cs
C#Checker.exe -h <fqdn>
```

### How to use C# connector

On Windows system, .NET applications can use the .NET interface of TDengine to perform all database operations. The steps to use it are as follows:

1. Add the. NET interface file TDengineDrivercs.cs to the .NET project where the application is located.
2. Users can refer to TDengineTest.cs to define database connection parameters and how to perform data insert, query and other operations;

This. NET interface requires the taos.dll file, so before executing the application, copy the taos.dll file in the Windows client install_directory/driver directory to the folder where the. NET project finally generated the .exe executable file. After running the exe file, you can access the TDengine database and do operations such as insert and query.

**Note:**

1. TDengine V2.0. 3.0 supports both 32-bit and 64-bit Windows systems, so when. NET project generates a .exe file, please select the corresponding "X86" or "x64" for the "Platform" under "Solution"/"Project".
2. This. NET interface has been verified in Visual Studio 2015/2017, and other VS versions have yet to be verified.

### Third-party Driver

Maikebing.Data.Taos is an ADO.Net provider for TDengine that supports Linux, Windows. This development package is provided by enthusiastic contributor 麦壳饼@@maikebing. For more details:

```
// Download
https://github.com/maikebing/Maikebing.EntityFrameworkCore.Taos   
// How to use    
https://www.taosdata.com/blog/2020/11/02/1901.html                    
```

## <a class="anchor" id="go"></a> Go Connector

### Installation preparation

- For application driver installation, please refer to the [steps of installing connector driver](https://www.taosdata.com/en/documentation/connector#driver).

The TDengine provides the GO driver taosSql. taosSql implements the GO language's built-in interface database/sql/driver. Users can access TDengine in the application by simply importing the package as follows, see https://github.com/taosdata/driver-go/blob/develop/taosSql/driver_test.go for details.

Sample code for using the Go connector can be found in https://github.com/taosdata/TDengine/tree/develop/tests/examples/go and the [video tutorial](https://www.taosdata.com/blog/2020/11/11/1951.html).

```Go
import (
    "database/sql"
    _ "github.com/taosdata/driver-go/taosSql"
)
```

**It is recommended to use Go version 1.13 or above and turn on module support:**

```bash
go env -w GO111MODULE=on  
go env -w GOPROXY=https://goproxy.io,direct  
```

### Common APIs

- `sql.Open(DRIVER_NAME string, dataSourceName string) *DB`

This API is used to open DB and return an object of type \* DB. Generally, DRIVER_NAME is set to the string `taosSql`, and dataSourceName is set to the string `user:password@/tcp(host:port)/dbname`. If the customer wants to access TDengine with multiple goroutines concurrently, it is necessary to create a `sql.Open` object in each goroutine and use it to access TDengine.

**Note**: When the API is successfully created, there is no permission check. Only when Query or Exec is actually executed can the connection be truly created and whether the user/password/host/port is legal can be checked at the same time. In addition, because most of the implementation of the whole driver sinks into libtaos, which taosSql depends on. Therefore, sql.Open itself is particularly lightweight.

- `func (db *DB) Exec(query string, args ...interface{}) (Result, error)`

`sql.Open` built-in method to execute non-query related SQL

- `func (db *DB) Query(query string, args ...interface{}) (*Rows, error)`

`sql.Open` built-in method used to execute query statements

- `func (db *DB) Prepare(query string) (*Stmt, error)`

`sql.Open` built-in method used to create a prepared statement for later queries or executions.

- `func (s *Stmt) Exec(args ...interface{}) (Result, error)`

`sql.Open` built-in method to execute a prepared statement with the given arguments and returns a Result summarizing the effect of the statement.

- `func (s *Stmt) Query(args ...interface{}) (*Rows, error)`

`sql.Open` built-in method to query executes a prepared query statement with the given arguments and returns the query results as a \*Rows.

- `func (s *Stmt) Close() error`

`sql.Open` built-in method to closes the statement.

## <a class="anchor" id="nodejs"></a> Node.js Connector

The Node.js connector supports the following systems:

| **CPU Type**         | x64(64bit）                  |         |         |   aarch64   |   aarch32   |
| -------------------- | ---------------------------- | ------- | ------- | ----------- | ----------- |
| **OS Type**          | Linux                        | Win64   | Win32   | Linux       | Linux       |
| **Supported or Not** | **Yes**                      | **Yes** | **Yes** | **Yes**     | **Yes**     |

See the [video tutorial](https://www.taosdata.com/blog/2020/11/11/1957.html) for use of the Node.js connector.

### Installation preparation

- For application driver installation, please refer to the [steps of installing connector driver](https://www.taosdata.com/en/documentation/connector#driver).

### Install Node.js connector

Users can install it through [npm](https://www.npmjs.com/) or through the source code src/connector/nodejs/. The specific installation steps are as follows:

First, install the node.js connector through [npm](https://www.npmjs.com/).

```bash
npm install td2.0-connector
```

We recommend that use npm to install the node.js connector. If you do not have npm installed, you can copy src/connector/nodejs/ to your nodejs project directory.

We use [node-gyp](https://github.com/nodejs/node-gyp) to interact with the TDengine server. Before installing the node.js connector, you also need to install the following software:

### Linux

- python (recommended v2.7, not currently supported in v3.x.x)
- node 2.0. 6 supports v12. x and v10. x, 2.0. 5 and earlier supports v10. x, and other versions may have package compatibility issues.
- make
- [GCC](https://gcc.gnu.org/) and other C compilers

### Windows

#### Solution 1

Use Microsoft [windows-build-tools](https://github.com/felixrieseberg/windows-build-tools) to install all necessary tools by executing npm install --global --production windows-build-tools in cmd command line interface.

#### Solution 2

Manually install the following tools:

- Install Visual Studio related tools: [Visual Studio Build Tools](https://visualstudio.microsoft.com/thank-you-downloading-visual-studio/?sku=BuildTools) or [Visual Studio 2017 Community](https://visualstudio.microsoft.com/pl/thank-you-downloading-visual-studio/?sku=Community)
- Install [Python](https://www.python.org/downloads/) 2.7 (not supported in v3.x.x) and execute npm config set python python2.7
- Open `cmd`, `npm config set msvs_version 2017`

If the  steps above cannot be performed successfully, you can refer to Microsoft's Node.js User Manual [Microsoft's Node.js Guidelines for Windows](https://github.com/Microsoft/nodejs-guidelines/blob/master/windows-environment.md#compiling-native-addon-modules).

If you use ARM64 Node.js on Windows 10 ARM, you also need to add "Visual C++ compilers and libraries for ARM64" and "Visual C++ ATL for ARM64".

#### Sample

The sample program source code is located in install_directory/examples/nodejs, and there are:

Node-example.js node.js sample source code Node-example-raw. js

### Installation verification

After installing the TDengine client, the nodejsChecker.js program can verify whether the current environment supports access to TDengine via nodejs.

Steps:

1. Create a new installation verification directory, for example: ~/tdengine-test, copy the nodejsChecker.js source program on github. Download address: （https://github.com/taosdata/TDengine/tree/develop/tests/examples/nodejs/nodejsChecker.js）.

2. Execute the following command:

   ```bash
   npm init -y
   npm install td2.0-connector
   node nodejsChecker.js host=localhost
   ```

3. After performing the above steps, the nodejs connection Tdengine instance will be outputted on the command line, and the short-answer of insertion and query will be executed.

### How to use Node.js

The following are some basic uses of node.js connector. Please refer to [TDengine Node.js connector](http://docs.taosdata.com/node) for details.

### Create connection

When using the node.js connector, you must execute <em>require</em> `td2.0-connector`, and then use the `taos.connect` function. The parameter that `taos.connect` function must provide is `host`, and other parameters will use the following default values if they are not provided. Finally, the `cursor` needs to be initialized to communicate with the TDengine server-side.

```javascript
const taos = require('td2.0-connector');
var conn = taos.connect({host:"127.0.0.1", user:"root", password:"taosdata", config:"/etc/taos",port:0})
var cursor = conn.cursor(); // Initializing a new cursor
```

To close the connect:

```javascript
conn.close();
```

#### To execute SQL and insert data

For DDL statements (such as `create database`, `create table`, `use`, and so on), you can use the `execute` method of `cursor`. The code is as follows:

```js
cursor.execute('create database if not exists test;')
```

The above code creates a database named test. For DDL statements, there is generally no return value, and the execute return value of `cursor` is 0.

For Insert statements, the code is as follows:

```js
var affectRows = cursor.execute('insert into test.weather values(now, 22.3, 34);')
```

The return value of the execute method is the number of rows affected by the statement. If the sql above inserts a piece of data into the weather table of the test database, the return value affectRows is 1.

TDengine does not currently support update and delete statements.

#### Query

You can query the database through  `cursor.query` function.

```javascript
var query = cursor.query('show databases;')
```

The results of the query can be obtained and printed through `query.execute()` function:

```javascript
var promise = query.execute();
promise.then(function(result) {
  result.pretty(); 
});
```

You can also use the `bind` method of `query` to format query statements. For example: `query` automatically fills the `?` with the value provided in the query statement .

```javascript
var query = cursor.query('select * from meterinfo.meters where ts <= ? and areaid = ?;').bind(new Date(), 5);
query.execute().then(function(result) {
  result.pretty();
})
```

If you provide the second parameter in the `query` statement and set it to `true`, you can also get the query results immediately. As follows:

```javascript
var promise = cursor.query('select * from meterinfo.meters where v1 = 30;', true)
promise.then(function(result) {
  result.pretty();
})
```

#### Asynchronous function

The operation of asynchronous query database is similar to the above, only by adding `_a` after `cursor.execute`, `TaosQuery.execute` and other functions.

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

[node-example.js](https://github.com/taosdata/TDengine/tree/master/tests/examples/nodejs/node-example.js) provides a code example that uses the NodeJS connector to create a table, insert weather data, and query the inserted data.

[node-example-raw.js](https://github.com/taosdata/TDengine/tree/master/tests/examples/nodejs/node-example-raw.js) is also a code example that uses the NodeJS connector to create a table, insert weather data, and query the inserted data, but unlike the above, this example only uses cursor.
