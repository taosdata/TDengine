# Java connector

## Introduction

The taos-jdbcdriver is implemented in two forms: JDBC-JNI and JDBC-RESTful (supported from taos-jdbcdriver-2.0.18). JDBC-JNI is implemented by calling the local methods of libtaos.so (or taos.dll) on the client, while JDBC-RESTful encapsulates the RESTful interface implementation internally.

![tdengine-connector](../../images/tdengine-jdbc-connector.png)

The figure above shows the three ways Java applications can access the TDengine:

* JDBC-JNI: The Java application uses JDBC-JNI's API on physical node1 (pnode1) and directly calls the client API (libtaos.so or taos.dll) to send write or query requests to the taosd instance on physical node2 (pnode2).
* RESTful: The Java application sends the SQL to the RESTful connector on physical node2 (pnode2), which then calls the client API (libtaos.so).
* JDBC-RESTful: The Java application uses the JDBC-restful API to encapsulate SQL into a RESTful request and send it to the RESTful connector of physical node 2.

In terms of implementation, the JDBC driver of TDengine is as consistent as possible with the behavior of the relational database driver. However, due to the differences between TDengine and relational database in the object and technical characteristics of services, there are some differences between taos-jdbcdriver and traditional relational database JDBC driver. The following points should be watched:

* deleting a record is not supported in TDengine.
* transaction is not supported in TDengine.

### Difference between JDBC-JNI and JDBC-restful

<table>
<tr align="center"><th>Difference</th><th>JDBC-JNI</th><th>JDBC-RESTful</th></tr>
<tr align="center">
  <td>Supported OS</td>
  <td>linux、windows</td>
  <td>all platform</td>
</tr>
<tr align="center">
  <td>Whether to install the Client</td>
  <td>need</td>
  <td>do not need</td>
</tr>
<tr align="center">
  <td>Whether to upgrade the client after the server is upgraded</td>
  <td>need</td>
  <td>do not need</td>
</tr>
<tr align="center">
  <td>Write performance</td>
  <td colspan="2">JDBC-RESTful is 50% to 90% of JDBC-JNI</td>
</tr>
<tr align="center">
  <td>Read performance</td>
  <td colspan="2">JDBC-RESTful is no different from JDBC-JNI</td>
</tr>
</table>

**Note**: RESTful interfaces are stateless. Therefore, when using JDBC-restful, you should specify the database name in SQL before all table names and super table names, for example:

```sql
INSERT INTO test.t1 USING test.weather (ts, temperature) TAGS('beijing') VALUES(now, 24.6);
```

## JDBC driver version and supported TDengine and JDK versions

| taos-jdbcdriver | TDengine           | JDK   |
| --------------- | ------------------ | ----- |
| 2.0.33 - 2.0.34 | 2.0.3.0 and above  | 1.8.x |
| 2.0.31 - 2.0.32 | 2.1.3.0 and above  | 1.8.x |
| 2.0.22 - 2.0.30 | 2.0.18.0 - 2.1.2.x | 1.8.x |
| 2.0.12 - 2.0.21 | 2.0.8.0 - 2.0.17.x | 1.8.x |
| 2.0.4 - 2.0.11  | 2.0.0.0 - 2.0.7.x  | 1.8.x |
| 1.0.3           | 1.6.1.x and above  | 1.8.x |
| 1.0.2           | 1.6.1.x and above  | 1.8.x |
| 1.0.1           | 1.6.1.x and above  | 1.8.x |

## DataType in TDengine and Java connector

The TDengine supports the following data types and Java data types:

| TDengine DataType | JDBCType (driver version < 2.0.24) | JDBCType (driver version >= 2.0.24) |
| ----------------- | ---------------------------------- | ----------------------------------- |
| TIMESTAMP         | java.lang.Long                     | java.sql.Timestamp                  |
| INT               | java.lang.Integer                  | java.lang.Integer                   |
| BIGINT            | java.lang.Long                     | java.lang.Long                      |
| FLOAT             | java.lang.Float                    | java.lang.Float                     |
| DOUBLE            | java.lang.Double                   | java.lang.Double                    |
| SMALLINT          | java.lang.Short                    | java.lang.Short                     |
| TINYINT           | java.lang.Byte                     | java.lang.Byte                      |
| BOOL              | java.lang.Boolean                  | java.lang.Boolean                   |
| BINARY            | java.lang.String                   | byte array                          |
| NCHAR             | java.lang.String                   | java.lang.String                    |

## Install Java connector

### Runtime Requirements

To run TDengine's Java connector, the following requirements shall be met:

1. A Linux or Windows System

2. Java Runtime Environment 1.8 or later

3. TDengine client (required for JDBC-JNI, not required for JDBC-restful)

**Note**:

* After the TDengine client is successfully installed on Linux, the libtaos.so file is automatically copied to /usr/lib/libtaos.so, which is included in the Linux automatic scan path and does not need to be specified separately.
* After the TDengine client is installed on Windows, the taos.dll file that the driver package depends on is automatically copied to the default search path C:/Windows/System32. You do not need to specify it separately.

### Obtain JDBC driver by maven

To Java delevopers, TDengine provides `taos-jdbcdriver` according to the JDBC(3.0) API. Users can find and download it through [Sonatype Repository](https://search.maven.org/artifact/com.taosdata.jdbc/taos-jdbcdriver). Add the following dependencies in pom.xml for your maven projects.

```xml
<dependencies>
    <dependency>
        <groupId>com.taosdata.jdbc</groupId>
        <artifactId>taos-jdbcdriver</artifactId>
        <version>2.0.34</version>
    </dependency>
</dependencies>
```

### Obtain JDBC driver by compiling source code

You can download the TDengine source code and compile the latest version of the JDBC Connector.

 ```shell
 git clone https://github.com/taosdata/TDengine.git
 cd TDengine/src/connector/jdbc
 mvn clean package -Dmaven.test.skip=true
 ```

a taos-jdbcdriver-2.0.xx-dist.jar will be released in the target directory.

## Usage of java connector

### Establishing a Connection

#### Establishing a connection with URL

Establish the connection by specifying the URL, as shown below:

```java
String jdbcUrl = "jdbc:TAOS-RS://taosdemo.com:6041/test?user=root&password=taosdata";
Connection conn = DriverManager.getConnection(jdbcUrl);
```

In the example above, the JDBC-RESTful driver is used to establish a connection to the hostname of 'taosdemo.com', port of 6041, and database name of 'test'. This URL specifies the user name as 'root' and the password as 'taosdata'.

The JDBC-RESTful does not depend on the local function library. Compared with JDBC-JNI, only the following is required:

* DriverClass designated as "com.taosdata.jdbc.rs.RestfulDriver"
* JdbcUrl starts with "JDBC:TAOS-RS://"
* Use port 6041 as the connection port

For better write and query performance, Java applications can use the JDBC-JNI driver, as shown below:

```java
String jdbcUrl = "jdbc:TAOS://taosdemo.com:6030/test?user=root&password=taosdata";
Connection conn = DriverManager.getConnection(jdbcUrl);
```

In the example above, The JDBC-JNI driver is used to establish a connection to the hostname of 'taosdemo.com', port 6030 (TDengine's default port), and database name of 'test'. This URL specifies the user name as 'root' and the password as 'taosdata'.

<!-- You can also see the JDBC-JNI video tutorial: [JDBC connector of TDengine](https://www.taosdata.com/blog/2020/11/11/1955.html) -->

The format of JDBC URL is: 

```url
jdbc:[TAOS|TAOS-RS]://[host_name]:[port]/[database_name]?[user={user}|&password={password}|&charset={charset}|&cfgdir={config_dir}|&locale={locale}|&timezone={timezone}]
```

The configuration parameters in the URL are as follows:

* user: user name for logging in to the TDengine. The default value is 'root'.
* password: the user login password. The default value is 'taosdata'.
* cfgdir: directory of the client configuration file. It is valid only for JDBC-JNI. The default value is `/etc/taos` on Linux and `C:/TDengine/cfg` on Windows.
* charset: character set used by the client. The default value is the system character set.
* locale: client locale. The default value is the current system locale.
* timezone: timezone used by the client. The default value is the current timezone of the system.
* batchfetch: only valid for JDBC-JNI. True if batch ResultSet fetching is enabled; false if row-by-row ResultSet fetching is enabled. Default value is flase.
* timestampFormat: only valid for JDBC-RESTful. 'TIMESTAMP' if you want to get a long value in a ResultSet; 'UTC' if you want to get a string in UTC date-time format in a ResultSet; 'STRING' if you want to get a local date-time format string in ResultSet. Default value is 'STRING'.
* batchErrorIgnore: true if you want to continue executing the rest of the SQL when error happens during execute the executeBatch method in Statement; false, false if the remaining SQL statements are not executed. Default value is false.

#### Establishing a connection with URL and Properties

In addition to establish the connection with the specified URL, you can also use Properties to specify the parameters to set up the connection, as shown below:

```java
public Connection getConn() throws Exception{
  String jdbcUrl = "jdbc:TAOS://taosdemo.com:6030/test?user=root&password=taosdata";
  // String jdbcUrl = "jdbc:TAOS-RS://taosdemo.com:6041/test?user=root&password=taosdata";
  Properties connProps = new Properties();
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
  Connection conn = DriverManager.getConnection(jdbcUrl, connProps);
  return conn;
}
```

In the example above, JDBC-JNI is used to establish a connection to hostname of 'taosdemo.com', port at 6030, and database name of 'test'. The annotation is the method when using JDBC-RESTful. The connection specifies the user name as 'root' and the password as 'taosdata' in the URL, and the character set to use, locale, time zone, and so on in connProps.

The configuration parameters in properties are as follows:

* TSDBDriver.PROPERTY_KEY_USER: user name for logging in to the TDengine. The default value is 'root'.
* TSDBDriver.PROPERTY_KEY_PASSWORD: the user login password. The default value is 'taosdata'.
* TSDBDriver.PROPERTY_KEY_CONFIG_DIR: directory of the client configuration file. It is valid only for JDBC-JNI. The default value is `/etc/taos` on Linux and `C:/TDengine/cfg on Windows`.
* TSDBDriver.PROPERTY_KEY_CHARSET: character set used by the client. The default value is the system character set.
* TSDBDriver.PROPERTY_KEY_LOCALE: client locale. The default value is the current system locale.
* TSDBDriver.PROPERTY_KEY_TIME_ZONE: timezone used by the client. The default value is the current timezone of the system.
* TSDBDriver.PROPERTY_KEY_BATCH_LOAD: only valid for JDBC-JNI. True if batch ResultSet fetching is enabled; false if row-by-row ResultSet fetching is enabled. Default value is flase.
* TSDBDriver.PROPERTY_KEY_TIMESTAMP_FORMAT: only valid for JDBC-RESTful. 'TIMESTAMP' if you want to get a long value in a ResultSet; 'UTC' if you want to get a string in UTC date-time format in a ResultSet; 'STRING' if you want to get a local date-time format string in ResultSet. Default value is 'STRING'.
* TSDBDriver.PROPERTY_KEY_BATCH_ERROR_IGNORE: true if you want to continue executing the rest of the SQL when error happens during execute the executeBatch method in Statement; false, false if the remaining SQL statements are not executed. Default value is false.

#### Establishing a connection with configuration file

When JDBC-JNI is used to connect to the TDengine cluster, you can specify firstEp and secondEp parameters of the cluster in the client configuration file. As follows:

1. The hostname and port are not specified in Java applications

```java
public Connection getConn() throws Exception{
  String jdbcUrl = "jdbc:TAOS://:/test?user=root&password=taosdata";
  Properties connProps = new Properties();
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
  Connection conn = DriverManager.getConnection(jdbcUrl, connProps);
  return conn;
}
```

2. Specify firstEp and secondEp in the configuration file

```txt
# first fully qualified domain name (FQDN) for TDengine system
firstEp               cluster_node1:6030
# second fully qualified domain name (FQDN) for TDengine system, for cluster only
secondEp              cluster_node2:6030
```

In the above example, JDBC driver uses the client configuration file to establish a connection to the hostname of 'cluster_node1', port 6030, and database name of 'test'. When the firstEp node in the cluster fails, JDBC will try to connect to the cluster using secondEp. In the TDengine, as long as one node in firstEp and secondEp is valid, the connection to the cluster can be established.

**Note**: In this case, the configuration file belongs to TDengine client which is running inside a Java application. default file path of Linux OS is '/etc/taos/taos.cfg', and default file path of Windows OS is 'C://TDengine/cfg/taos.cfg'.

#### Priority of the parameters

If the parameters in the URL, Properties, and client configuration file are repeated set, the priorities of the parameters in descending order are as follows:

1. URL parameters
2. Properties
3. Client configuration file in taos.cfg

For example, if you specify password as 'taosdata' in the URL and password as 'taosdemo' in the Properties, JDBC will establish a connection using the password in the URL.

For details, see Client Configuration:[client configuration](https://www.taosdata.com/en/documentation/administrator#client)

### Create database and table

```java
Statement stmt = conn.createStatement();
// create database
stmt.executeUpdate("create database if not exists db");
// use database
stmt.executeUpdate("use db");
// create table
stmt.executeUpdate("create table if not exists tb (ts timestamp, temperature int, humidity float)");
```

### Insert

```java
// insert data
int affectedRows = stmt.executeUpdate("insert into tb values(now, 23, 10.3) (now + 1s, 20, 9.3)");
System.out.println("insert " + affectedRows + " rows.");
```

**Note**: 'now' is an internal system function. The default value is the current time of the computer where the client resides. 'now + 1s' indicates that the current time on the client is added by one second. The following time units are a(millisecond), s (second), m(minute), h(hour), d(day), w(week), n(month), and y(year).

### Query

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

**Note**: The query is consistent with the operation of the relational database, and the index in ResultSet starts from 1.

### Handle exceptions

```java
try (Statement statement = connection.createStatement()) {
    // executeQuery
    ResultSet resultSet = statement.executeQuery(sql);
    // print result
    printResult(resultSet);
} catch (SQLException e) {
    System.out.println("ERROR Message: " + e.getMessage());
    System.out.println("ERROR Code: " + e.getErrorCode());
    e.printStackTrace();
}
```

The Java connector may report three types of error codes: JDBC Driver (error codes ranging from 0x2301 to 0x2350), JNI method (error codes ranging from 0x2351 to 0x2400), and TDengine Error. For details about the error code, see:

- https://github.com/taosdata/TDengine/blob/develop/src/connector/jdbc/src/main/java/com/taosdata/jdbc/TSDBErrorNumbers.java
- https://github.com/taosdata/TDengine/blob/develop/src/inc/taoserror.h

### Write data through parameter binding

Since version 2.1.2.0, TDengine's JDBC-JNI implementation has significantly improved parameter binding support for data write (INSERT) scenarios. Data can be written in the following way, avoiding SQL parsing and significantly improving the write performance.(**Note**: parameter binding is not supported in JDBC-RESTful)

```java
Statement stmt = conn.createStatement();
Random r = new Random();

// In the INSERT statement, the VALUES clause allows you to specify a specific column; If automatic table creation is adopted, the TAGS clause needs to set the parameter values of all TAGS columns
TSDBPreparedStatement s = (TSDBPreparedStatement) conn.prepareStatement("insert into ? using weather_test tags (?, ?) (ts, c1, c2) values(?, ?, ?)");

s.setTableName("w1");

// set tags
s.setTagInt(0, r.nextInt(10));
s.setTagString(1, "Beijing");
int numOfRows = 10;

// set values
ArrayList<Long> ts = new ArrayList<>();
for (int i = 0; i < numOfRows; i++){
    ts.add(System.currentTimeMillis() + i);
}
s.setTimestamp(0, ts);
ArrayList<Integer> s1 = new ArrayList<>();
for (int i = 0; i < numOfRows; i++){
    s1.add(r.nextInt(100));
}
s.setInt(1, s1);
ArrayList<String> s2 = new ArrayList<>();
for (int i = 0; i < numOfRows; i++){
    s2.add("test" + r.nextInt(100));
}
s.setString(2, s2, 10);

// The cache is not cleared after AddBatch. Do not bind new data again before ExecuteBatch
s.columnDataAddBatch();
s.columnDataExecuteBatch();
// Clear the cache, after which you can bind new data(including table names, tags, values):
s.columnDataClearBatch();
s.columnDataCloseBatch();
```

The methods used to set tags are:

```java
public void setTagNull(int index, int type)
public void setTagBoolean(int index, boolean value)
public void setTagInt(int index, int value)
public void setTagByte(int index, byte value)
public void setTagShort(int index, short value)
public void setTagLong(int index, long value)
public void setTagTimestamp(int index, long value)
public void setTagFloat(int index, float value)
public void setTagDouble(int index, double value)
public void setTagString(int index, String value)
public void setTagNString(int index, String value)
```

The methods used to set columns are:

```java
public void setInt(int columnIndex, ArrayList<Integer> list) throws SQLException
public void setFloat(int columnIndex, ArrayList<Float> list) throws SQLException
public void setTimestamp(int columnIndex, ArrayList<Long> list) throws SQLException
public void setLong(int columnIndex, ArrayList<Long> list) throws SQLException
public void setDouble(int columnIndex, ArrayList<Double> list) throws SQLException
public void setBoolean(int columnIndex, ArrayList<Boolean> list) throws SQLException
public void setByte(int columnIndex, ArrayList<Byte> list) throws SQLException
public void setShort(int columnIndex, ArrayList<Short> list) throws SQLException
public void setString(int columnIndex, ArrayList<String> list, int size) throws SQLException
public void setNString(int columnIndex, ArrayList<String> list, int size) throws SQLException
```

**Note**: Both setString and setNString require the user to declare the column width of the corresponding column in the table definition in the size parameter.

### Data Subscription

#### Subscribe

```java
TSDBSubscribe sub = ((TSDBConnection)conn).subscribe("topic", "select * from meters", false);
```

parameters:

* topic: the unique topic name of the subscription. 
* sql: a select statement.
* restart: true if restart the subscription already exists; false if continue the previous subscription.

In the example above, a subscription named 'topic' is created which use the SQL statement 'select * from meters'. If the subscription already exists, it will continue with the previous query progress, rather than consuming all the data from scratch.

#### Consume

```java
int total = 0;
while(true) {
    TSDBResultSet rs = sub.consume();
    int count = 0;
    while(rs.next()) {
        count++;
    }
    total += count;
    System.out.printf("%d rows consumed, total %d\n", count, total);
    Thread.sleep(1000);
}
```

The consume method returns a result set containing all the new data so far since the last consume. Make sure to call consume as often as you need (like Thread.sleep(1000) in the example), otherwise you will put unnecessary stress on the server.

#### Close

```java
sub.close(true);
// release resources
resultSet.close();
stmt.close();
conn.close();
```

The close method closes a subscription. If the parameter is true, the subscription progress information is reserved, and a subscription with the same name can be created later to continue consuming data. If false, the subscription progress is not retained.

**Note**: the connection must be closed; otherwise, a connection leak may occur.

## Connection Pool

### HikariCP example

```java
public static void main(String[] args) throws SQLException {
    HikariConfig config = new HikariConfig();
    // jdbc properties
    config.setJdbcUrl("jdbc:TAOS://127.0.0.1:6030/log");
    config.setUsername("root");
    config.setPassword("taosdata");
    // connection pool configurations
    config.setMinimumIdle(10);           //minimum number of idle connection
    config.setMaximumPoolSize(10);      //maximum number of connection in the pool
    config.setConnectionTimeout(30000); //maximum wait milliseconds for get connection from pool
    config.setMaxLifetime(0);       // maximum lifetime for each connection
    config.setIdleTimeout(0);       // max idle time for recycle idle connection
    config.setConnectionTestQuery("select server_status()"); //validation query
    HikariDataSource ds = new HikariDataSource(config); //create datasource
    Connection  connection = ds.getConnection(); // get connection
    Statement statement = connection.createStatement(); // get statement
    //query or insert
    // ...
    connection.close(); // put back to connection pool
}
```

### Druid example

```java
public static void main(String[] args) throws Exception {
    DruidDataSource dataSource = new DruidDataSource();
    // jdbc properties
    dataSource.setDriverClassName("com.taosdata.jdbc.TSDBDriver");
    dataSource.setUrl(url);
    dataSource.setUsername("root");
    dataSource.setPassword("taosdata");
    // pool configurations
    dataSource.setInitialSize(10);
    dataSource.setMinIdle(10);
    dataSource.setMaxActive(10);
    dataSource.setMaxWait(30000);
    dataSource.setValidationQuery("select server_status()");
    Connection  connection = dataSource.getConnection(); // get connection
    Statement statement = connection.createStatement(); // get statement
    //query or insert 
    // ...
    connection.close(); // put back to connection pool
}
```

**Note**：

As of TDengine V1.6.4.1, the function select server_status() is supported specifically for heartbeat detection, so it is recommended to use select server_status() for Validation queries when using connection pools.

Select server_status() returns 1 on success, as shown below.

```sql
taos> select server_status();
server_status()|
================
1              |
Query OK, 1 row(s) in set (0.000141s)
```

## Integrated with framework

- Please refer to [SpringJdbcTemplate](https://github.com/taosdata/TDengine/tree/develop/tests/examples/JDBC/SpringJdbcTemplate) if using taos-jdbcdriver in Spring JdbcTemplate.
- Please refer to [springbootdemo](https://github.com/taosdata/TDengine/tree/develop/tests/examples/JDBC/springbootdemo) if using taos-jdbcdriver in Spring JdbcTemplate.

## Example Codes

you see sample code here: [JDBC example](https://github.com/taosdata/TDengine/tree/develop/tests/examples/JDBC)

## FAQ

- java.lang.UnsatisfiedLinkError: no taos in java.library.path

  **Cause**：The application program cannot find Library function *taos*

  **Answer**：Copy `C:\TDengine\driver\taos.dll` to `C:\Windows\System32\` on Windows and make a soft link through `ln -s /usr/local/taos/driver/libtaos.so.x.x.x.x /usr/lib/libtaos.so` on Linux.

- java.lang.UnsatisfiedLinkError: taos.dll Can't load AMD 64 bit on a IA 32-bit platform

  **Cause**：Currently TDengine only support 64bit JDK

  **Answer**：re-install 64bit JDK.

- For other questions, please refer to [Issues](https://github.com/taosdata/TDengine/issues)

