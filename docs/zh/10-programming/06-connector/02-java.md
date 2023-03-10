---
toc_max_heading_level: 4
sidebar_label: Java
title: TDengine Java Connector
description: TDengine Java 连接器基于标准 JDBC API 实现, 并提供原生连接与 REST连接两种连接器。
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

`taos-jdbcdriver` 是 TDengine 的官方 Java 语言连接器，Java 开发人员可以通过它开发存取 TDengine 数据库的应用软件。`taos-jdbcdriver` 实现了 JDBC driver 标准的接口，通过 REST 接口连接 TDengine 实例。

## TDengine DataType 和 Java DataType

TDengine 目前支持时间戳、数字、字符、布尔类型，与 Java 对应类型转换如下：

| TDengine DataType | JDBCType  |
| ----------------- | ---------------------------------- |
| TIMESTAMP         | java.sql.Timestamp                 |
| INT               | java.lang.Integer                  |
| BIGINT            | java.lang.Long                     |
| FLOAT             | java.lang.Float                    |
| DOUBLE            | java.lang.Double                   |
| SMALLINT          | java.lang.Short                    |
| TINYINT           | java.lang.Byte                     |
| BOOL              | java.lang.Boolean                  |
| BINARY            | byte array                         |
| NCHAR             | java.lang.String                   |
| JSON              | java.lang.String                   |

**注意**：JSON 类型仅在 tag 中支持。

## 安装步骤

### 安装前准备

使用 Java Connector 连接数据库前，需要具备以下条件：

- 已安装 Java 1.8 或以上版本运行时环境和 Maven 3.6 或以上版本
- 已安装 TDengine 客户端驱动（使用原生连接必须安装，使用 REST 连接无需安装），具体步骤请参考[安装客户端驱动](../#安装客户端驱动)

### 安装连接器

<Tabs defaultValue="maven">
<TabItem value="maven" label="使用 Maven 安装">

目前 taos-jdbcdriver 已经发布到 [Sonatype Repository](https://search.maven.org/artifact/com.taosdata.jdbc/taos-jdbcdriver) 仓库，且各大仓库都已同步。

- [sonatype](https://search.maven.org/artifact/com.taosdata.jdbc/taos-jdbcdriver)
- [mvnrepository](https://mvnrepository.com/artifact/com.taosdata.jdbc/taos-jdbcdriver)
- [maven.aliyun](https://maven.aliyun.com/mvn/search)

Maven 项目中，在 pom.xml 中添加以下依赖：

```xml-dtd
<dependency>
  <groupId>com.taosdata.jdbc</groupId>
  <artifactId>taos-jdbcdriver</artifactId>
  <version>3.0.0</version>
</dependency>
```

</TabItem>
<TabItem value="source" label="使用源码编译安装">

可以通过下载 TDengine 的源码，自己编译最新版本的 Java connector

```shell
git clone https://github.com/taosdata/taos-connector-jdbc.git
cd taos-connector-jdbc
mvn clean install -Dmaven.test.skip=true
```

编译后，在 target 目录下会产生 taos-jdbcdriver-3.0.*-dist.jar 的 jar 包，并自动将编译的 jar 文件放在本地的 Maven 仓库中。

</TabItem>
</Tabs>

## 建立连接

TDengine 的 JDBC URL 规范格式为：
`jdbc:TAOS-RS://[host_name]:[port]/[database_name]?batchfetch={true|false}&useSSL={true|false}&token={token}&httpPoolSize={httpPoolSize}&httpKeepAlive={true|false}]&httpConnectTimeout={httpTimeout}&httpSocketTimeout={socketTimeout}`

```java
Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
String jdbcUrl = "jdbc:TAOS-RS://taosdemo.com:6041/test?user=root&password=taosdata";
Connection conn = DriverManager.getConnection(jdbcUrl);
```

:::note

- REST 接口是无状态的。在使用 JDBC REST 连接时，需要在 SQL 中指定表、超级表的数据库名称。例如：

```sql
INSERT INTO test.t1 USING test.weather (ts, temperature) TAGS('California.SanFrancisco') VALUES(now, 24.6);
```

- 如果在 URL 中指定了 dbname，那么 JDBC REST 连接会默认使用 `/rest/sql/dbname` 作为 restful 请求的 URL，在 SQL 中不需要指定 dbname。例如：URL 为 jdbc:TAOS-RS://127.0.0.1:6041/test，那么，可以执行 SQL：

  ```sql
  insert into t1 using weather(ts, temperature) tags('California.SanFrancisco') values(now, 24.6);
  ```
  
:::

### 指定 URL 和 Properties 获取连接

除了通过指定的 URL 获取连接，还可以使用 Properties 指定建立连接时的参数。

**注意**：

- 应用中设置的 client parameter 为进程级别的，即如果要更新 client 的参数，需要重启应用。这是因为 client parameter 是全局参数，仅在应用程序的第一次设置生效。
- 以下示例代码基于 taos-jdbcdriver-3.0.0。

```java
public Connection getRestConn() throws Exception{
  Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
  String jdbcUrl = System.getenv("TDENGINE_JDBC_URL");
  Properties connProps = new Properties();
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
  Connection conn = DriverManager.getConnection(jdbcUrl, connProps);
  return conn;
}
```

### 配置参数的优先级

通过前面三种方式获取连接，如果配置参数在 url、Properties、客户端配置文件中有重复，则参数的`优先级由高到低`分别如下：

1. JDBC URL 参数，如上所述，可以在 JDBC URL 的参数中指定。
2. Properties connProps

## 使用示例

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

### 插入数据

```java
// insert data
int affectedRows = stmt.executeUpdate("insert into tb values(now, 23, 10.3) (now + 1s, 20, 9.3)");

System.out.println("insert " + affectedRows + " rows.");
```

> `now` 为系统内部函数，默认为客户端所在计算机当前时间。
> `now + 1s` 代表客户端当前时间往后加 1 秒，数字后面代表时间单位：a(毫秒)，s(秒)，m(分)，h(小时)，d(天)，w(周)，n(月)，y(年)。

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

### 处理异常

在报错后，通过 SQLException 可以获取到错误的信息和错误码：

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

JDBC 连接器可能报错的错误码包括 3 种：JDBC driver 本身的报错（错误码在 0x2301 到 0x2350 之间），原生连接方法的报错（错误码在 0x2351 到 0x2400 之间），TDengine 其他功能模块的报错。

具体的错误码请参考：

- [TDengine Java Connector](https://github.com/taosdata/taos-connector-jdbc/blob/main/src/main/java/com/taosdata/jdbc/TSDBErrorNumbers.java)

### 关闭资源

```java
resultSet.close();
stmt.close();
conn.close();
```

:::note
 请确保关闭连接，否则会造成连接池泄露。
:::

### 与连接池使用

#### HikariCP

使用示例如下：

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
    config.setMaxLifetime(0);       // maximum life time for each connection
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

> 通过 HikariDataSource.getConnection() 获取连接后，使用完成后需要调用 close() 方法，实际上它并不会关闭连接，只是放回连接池中。
> 更多 HikariCP 使用问题请查看[官方说明](https://github.com/brettwooldridge/HikariCP)。

#### Druid

使用示例如下：

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

> 更多 druid 使用问题请查看[官方说明](https://github.com/alibaba/druid)。

### 更多示例程序

示例程序源码位于 `TDengine/examples/JDBC` 下:

- JDBCDemo：JDBC 示例源程序。
- JDBCConnectorChecker：JDBC 安装校验源程序及 jar 包。
- connectionPools：HikariCP, Druid, dbcp, c3p0 等连接池中使用 taos-jdbcdriver。
- SpringJdbcTemplate：Spring JdbcTemplate 中使用 taos-jdbcdriver。
- mybatisplus-demo：Springboot + Mybatis 中使用 taos-jdbcdriver。

请参考：[JDBC example](https://github.com/taosdata/TDengine/tree/3.0/examples/JDBC)

## 最近更新记录

| taos-jdbcdriver 版本 |            主要变化            |
| :------------------: | :----------------------------: |
|       3.0.3        |  修复 REST 连接在 jdk17+ 版本时间戳解析错误问题    |
|       3.0.1 - 3.0.2        |  修复一些情况下结果集数据解析错误的问题。3.0.1 在 JDK 11 环境编译，JDK 8 环境下建议使用 3.0.2 版本    |
|       3.0.0        |   支持 TDengine 3.0    |
|       2.0.42        |   修在 WebSocket 连接中 wasNull 接口返回值  |
|       2.0.41        |   修正 REST 连接中用户名和密码转码方式    |
|   2.0.39 - 2.0.40    |  增加 REST 连接/请求 超时设置  |
|        2.0.38        | JDBC REST 连接增加批量拉取功能 |
|        2.0.37        |      增加对 json tag 支持      |
|        2.0.36        |   增加对 schemaless 写入支持   |

## 常见问题

1. 使用 Statement 的 `addBatch()` 和 `executeBatch()` 来执行“批量写入/更新”，为什么没有带来性能上的提升？

**原因**：TDengine 的 JDBC 实现中，通过 `addBatch` 方法提交的 SQL 语句，会按照添加的顺序，依次执行，这种方式没有减少与服务端的交互次数，不会带来性能上的提升。

**解决方法**：1. 在一条 insert 语句中拼接多个 values 值；2. 使用多线程的方式并发插入；3. 使用参数绑定的写入方式

2. java.lang.UnsatisfiedLinkError: no taos in java.library.path

**原因**：程序没有找到依赖的本地函数库 taos。

**解决方法**：Windows 下可以将 C:\TDengine\driver\taos.dll 拷贝到 C:\Windows\System32\ 目录下，Linux 下将建立如下软链 `ln -s /usr/local/taos/driver/libtaos.so.x.x.x.x /usr/lib/libtaos.so` 即可，macOS 下需要建立软链 `ln -s /usr/local/lib/libtaos.dylib`。

3. java.lang.UnsatisfiedLinkError: taos.dll Can't load AMD 64 bit on a IA 32-bit platform

**原因**：目前 TDengine 只支持 64 位 JDK。

**解决方法**：重新安装 64 位 JDK。

4. java.lang.NoSuchMethodError: setByteArray

**原因**：taos-jdbcdriver 3.* 版本仅支持 TDengine 3.0 及以上版本。

**解决方法**： 使用 taos-jdbcdriver 2.* 版本连接 TDengine 2.* 版本。

5. java.lang.NoSuchMethodError: java.nio.ByteBuffer.position(I)Ljava/nio/ByteBuffer; ... taos-jdbcdriver-3.0.1.jar

**原因**：taos-jdbcdriver 3.0.1 版本需要在 JDK 11+ 环境使用。

**解决方法**： 更换 taos-jdbcdriver 3.0.2+ 版本。

其它问题请参考 [FAQ](../../../train-faq/faq)

## API 参考

[taos-jdbcdriver doc](https://docs.taosdata.com/api/taos-jdbcdriver)
