
## TAOS-JDBCDriver 概述

TDengine 为了方便 Java 应用使用，提供了遵循 JDBC 标准(3.0)API 规范的 `taos-jdbcdriver` 实现。目前可以通过 [Sonatype Repository][1] 搜索并下载。

由于 TDengine 是使用 c 语言开发的，使用 taos-jdbcdriver 驱动包时需要依赖系统对应的本地函数库。

* libtaos.so 
    在 linux 系统中成功安装 TDengine 后，依赖的本地函数库 libtaos.so 文件会被自动拷贝至 /usr/lib/libtaos.so，该目录包含在 Linux 自动扫描路径上，无需单独指定。
    
* taos.dll
    在 windows 系统中安装完客户端之后，驱动包依赖的 taos.dll 文件会自动拷贝到系统默认搜索路径 C:/Windows/System32 下，同样无需要单独指定。
    
> 注意：在 windows 环境开发时需要安装 TDengine 对应的 windows 版本客户端，由于目前没有提供 Linux 环境单独的客户端，需要安装 TDengine 才能使用。

TDengine 的 JDBC 驱动实现尽可能的与关系型数据库驱动保持一致，但时序空间数据库与关系对象型数据库服务的对象和技术特征的差异导致 taos-jdbcdriver 并未完全实现 JDBC 标准规范。在使用时需要注意以下几点：

* TDengine 不提供针对单条数据记录的删除和修改的操作，驱动中也没有支持相关方法。
* 由于不支持删除和修改，所以也不支持事务操作。
* 目前不支持表间的 union 操作。
* 目前不支持嵌套查询(nested query)，对每个 Connection 的实例，至多只能有一个打开的 ResultSet 实例；如果在 ResultSet还没关闭的情况下执行了新的查询，TSDBJDBCDriver 则会自动关闭上一个 ResultSet。


## TAOS-JDBCDriver 版本以及支持的 TDengine 版本和 JDK 版本

| taos-jdbcdriver 版本 | TDengine 版本 | JDK 版本 | 
| --- | --- | --- | 
| 1.0.3 | 1.6.4.x，1.6.3.x，1.6.2.x，1.6.1.x | 1.8.x |
| 1.0.2 | 1.6.4.x，1.6.3.x，1.6.2.x，1.6.1.x | 1.8.x |  
| 1.0.1 | 1.6.4.x，1.6.3.x，1.6.2.x，1.6.1.x | 1.8.x |  

## TDengine DataType 和 Java DataType

TDengine 目前支持时间戳、数字、字符、布尔类型，与 Java 对应类型转换如下：

| TDengine DataType | Java DataType | 
| --- | --- | 
| TIMESTAMP | java.sql.Timestamp | 
| INT | java.lang.Integer, | 
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
        <version>1.0.3</version>
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
`jdbc:TSDB://{host_ip}:{port}/{database_name}?[user={user}|&password={password}|&charset={charset}|&cfgdir={config_dir}|&locale={locale}|&timezone={timezone}]`

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
    // 
    
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
[11]:  https://github.com/taosdata/TDengine/tree/feature/ylxie/tests/examples/JDBC/SpringJdbcTemplate
[12]: https://github.com/taosdata/TDengine/tree/feature/ylxie/tests/examples/JDBC/springbootdemo
 