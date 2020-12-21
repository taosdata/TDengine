#  Java Connector

Java连接器支持的系统有：
| **CPU类型**  | x64（64bit） |          |          | ARM64  | ARM32  |
| ------------ | ------------ | -------- | -------- | -------- | -------- |
| **OS类型**   | Linux        | Win64    | Win32    | Linux    | Linux    |
| **支持与否** | **支持**     | **支持** | **支持** | **支持** | **支持** |

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
| 2.0.12 及以上 | 2.0.8.0 及以上 | 1.8.x |
| 2.0.4 - 2.0.11 | 2.0.0.0 - 2.0.7.x | 1.8.x |
| 1.0.3 | 1.6.1.x 及以上 | 1.8.x |
| 1.0.2 | 1.6.1.x 及以上 | 1.8.x |
| 1.0.1 | 1.6.1.x 及以上 | 1.8.x |

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
<dependency>
  <groupId>com.taosdata.jdbc</groupId>
  <artifactId>taos-jdbcdriver</artifactId>
  <version>2.0.4</version>
</dependency>
```

### 源码编译打包

下载 [TDengine][3] 源码之后，进入 taos-jdbcdriver 源码目录 `src/connector/jdbc` 执行 `mvn clean package` 即可生成相应 jar 包。

## 使用说明

### 获取连接

#### 通过JdbcUrl获取连接

通过指定的jdbcUrl获取连接，如下所示：
```java
Class.forName("com.taosdata.jdbc.TSDBDriver");
String jdbcUrl = "jdbc:TAOS://taosdemo.com:6030/test?user=root&password=taosdata";
Connection conn = DriverManager.getConnection(jdbcUrl);
```
以上示例，建立了到hostname为taosdemo.com，端口为6030（TDengine的默认端口），数据库名为test的连接。这个url中指定用户名（user）为root，密码（password）为taosdata。

TDengine 的 JDBC URL 规范格式为：
`jdbc:TAOS://[host_name]:[port]/[database_name]?[user={user}|&password={password}|&charset={charset}|&cfgdir={config_dir}|&locale={locale}|&timezone={timezone}]`
url中的配置参数如下：
* user：登录 TDengine 用户名，默认值 root。
* password：用户登录密码，默认值 taosdata。
* cfgdir：客户端配置文件目录路径，Linux OS 上默认值 /etc/taos ，Windows OS 上默认值 C:/TDengine/cfg。
* charset：客户端使用的字符集，默认值为系统字符集。
* locale：客户端语言环境，默认值系统当前 locale。
* timezone：客户端使用的时区，默认值为系统当前时区。

#### 使用JdbcUrl和Properties获取连接

除了通过指定的jdbcUrl获取连接，还可以使用Properties指定建立连接时的参数，如下所示：
```java
public Connection getConn() throws Exception{
  Class.forName("com.taosdata.jdbc.TSDBDriver");
  String jdbcUrl = "jdbc:TAOS://taosdemo.com:6030/test?user=root&password=taosdata";
  Properties connProps = new Properties();
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
  Connection conn = DriverManager.getConnection(jdbcUrl, connProps);
  return conn;
}
```
以上示例，建立一个到hostname为taosdemo.com，端口为6030，数据库名为test的连接。这个连接在url中指定了用户名(user)为root，密码（password）为taosdata，并在connProps中指定了使用的字符集、语言环境、时区等信息。

properties中的配置参数如下：
* TSDBDriver.PROPERTY_KEY_USER：登录 TDengine 用户名，默认值 root。
* TSDBDriver.PROPERTY_KEY_PASSWORD：用户登录密码，默认值 taosdata。
* TSDBDriver.PROPERTY_KEY_CONFIG_DIR：客户端配置文件目录路径，Linux OS 上默认值 /etc/taos ，Windows OS 上默认值 C:/TDengine/cfg。
* TSDBDriver.PROPERTY_KEY_CHARSET：客户端使用的字符集，默认值为系统字符集。
* TSDBDriver.PROPERTY_KEY_LOCALE：客户端语言环境，默认值系统当前 locale。
* TSDBDriver.PROPERTY_KEY_TIME_ZONE：客户端使用的时区，默认值为系统当前时区。

#### 使用客户端配置文件建立连接
当使用JDBC连接TDengine集群时，可以使用客户端配置文件，在客户端配置文件中指定集群的firstEp、secondEp参数。
如下所示：
1. 在java中不指定hostname和port
```java
public Connection getConn() throws Exception{
  Class.forName("com.taosdata.jdbc.TSDBDriver");
  String jdbcUrl = "jdbc:TAOS://:/test?user=root&password=taosdata";
  Properties connProps = new Properties();
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
  Connection conn = DriverManager.getConnection(jdbcUrl, connProps);
  return conn;
}
```
2. 在配置文件中指定firstEp和secondEp
```
# first fully qualified domain name (FQDN) for TDengine system
firstEp               cluster_node1:6030

# second fully qualified domain name (FQDN) for TDengine system, for cluster only
secondEp              cluster_node2:6030

# default system charset
# charset               UTF-8

# system locale
# locale                en_US.UTF-8
```

以上示例，jdbc会使用客户端的配置文件，建立到hostname为cluster_node1，端口为6030，数据库名为test的连接。当集群中firstEp节点失效时，JDBC会尝试使用secondEp连接集群。
TDengine中，只要保证firstEp和secondEp中一个节点有效，就可以正常建立到集群的连接。

> 注意：这里的配置文件指的是调用JDBC Connector的应用程序所在机器上的配置文件，Linux OS 上默认值 /etc/taos/taos.cfg ，Windows OS 上默认值 C://TDengine/cfg/taos.cfg。

#### 配置参数的优先级
通过以上3种方式获取连接，如果配置参数在url、Properties、客户端配置文件中有重复，则参数的`优先级由高到低`分别如下：
1. JDBC URL 参数，如上所述，可以在 JDBC URL 的参数中指定。
2. Properties connProps
3. 客户端配置文件 taos.cfg
例如：在url中指定了password为taosdata，在Properties中指定了password为taosdemo，那么，JDBC会使用url中的password建立连接。

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

### 订阅

#### 创建

```java
TSDBSubscribe sub = ((TSDBConnection)conn).subscribe("topic", "select * from meters", false);
```

`subscribe` 方法的三个参数含义如下：

* topic：订阅的主题（即名称），此参数是订阅的唯一标识
* sql：订阅的查询语句，此语句只能是 `select` 语句，只应查询原始数据，只能按时间正序查询数据
* restart：如果订阅已经存在，是重新开始，还是继续之前的订阅

如上面的例子将使用 SQL 语句 `select * from meters` 创建一个名为 `topic` 的订阅，如果这个订阅已经存在，将继续之前的查询进度，而不是从头开始消费所有的数据。

#### 消费数据

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

`consume` 方法返回一个结果集，其中包含从上次 `consume` 到目前为止的所有新数据。请务必按需选择合理的调用 `consume` 的频率（如例子中的`Thread.sleep(1000)`），否则会给服务端造成不必要的压力。

#### 关闭订阅

```java
sub.close(true);
```

`close` 方法关闭一个订阅。如果其参数为 `true` 表示保留订阅进度信息，后续可以创建同名订阅继续消费数据；如为 `false` 则不保留订阅进度。

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

**Druid**

* 引入相应 Druid maven 依赖：

```xml
<dependency>
    <groupId>com.alibaba</groupId>
    <artifactId>druid</artifactId>
    <version>1.1.20</version>
</dependency>
```

* 使用示例如下：
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
> 更多 druid 使用问题请查看[官方说明][6]

**注意事项**
* TDengine `v1.6.4.1` 版本开始提供了一个专门用于心跳检测的函数 `select server_status()`，所以在使用连接池时推荐使用 `select server_status()` 进行 Validation Query。

如下所示，`select server_status()` 执行成功会返回 `1`。
```shell
taos> select server_status();
server_status()|
================
1              |
Query OK, 1 row(s) in set (0.000141s)
```

## 与框架使用

* Spring JdbcTemplate 中使用 taos-jdbcdriver，可参考 [SpringJdbcTemplate][11]
* Springboot + Mybatis 中使用，可参考 [springbootdemo][12]

## 常见问题

* java.lang.UnsatisfiedLinkError: no taos in java.library.path

  **原因**：程序没有找到依赖的本地函数库 taos。

  **解决方法**：windows 下可以将 C:\TDengine\driver\taos.dll 拷贝到 C:\Windows\System32\ 目录下，linux 下将建立如下软链 ` ln -s /usr/local/taos/driver/libtaos.so.x.x.x.x /usr/lib/libtaos.so` 即可。

* java.lang.UnsatisfiedLinkError: taos.dll Can't load AMD 64 bit on a IA 32-bit platform

  **原因**：目前 TDengine 只支持 64 位 JDK。

  **解决方法**：重新安装 64 位 JDK。

* 其它问题请参考 [Issues][7]

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
[11]: https://github.com/taosdata/TDengine/tree/develop/tests/examples/JDBC/SpringJdbcTemplate
[12]: https://github.com/taosdata/TDengine/tree/develop/tests/examples/JDBC/springbootdemo
[13]: https://www.taosdata.com/cn/documentation20/administrator/#%E5%AE%A2%E6%88%B7%E7%AB%AF%E9%85%8D%E7%BD%AE
[14]: https://www.taosdata.com/cn/all-downloads/#TDengine-Windows-Client
[15]: https://www.taosdata.com/cn/getting-started/#%E5%BF%AB%E9%80%9F%E4%B8%8A%E6%89%8B
