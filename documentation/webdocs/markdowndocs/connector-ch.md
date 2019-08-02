# 连接器

TDengine提供了丰富的应用程序开发接口，其中包括C/C++、JAVA、Python、RESTful、Go等，便于用户快速开发应用。

## C/C++ Connector

C/C++的API类似于MySQL的C API。应用程序使用时，需要包含TDengine头文件 _taos.h_（安装后，位于_/usr/local/taos/include_）：

```C
#include <taos.h>
```

在编译时需要链接TDengine动态库_libtaos.so_（安装后，位于/usr/local/taos/driver，gcc编译时，请加上 -ltaos）。 所有API都以返回_-1_或_NULL_均表示失败。

### C/C++同步API

传统的数据库操作API，都属于同步操作。应用调用API后，一直处于阻塞状态，直到服务器返回结果。TDengine支持如下API：

- `TAOS *taos_connect(char *ip, char *user, char *pass, char *db, int port)`

  创建数据库连接，初始化连接上下文。其中需要用户提供的参数包含：TDengine管理主节点的IP地址、用户名、密码、数据库名字和端口号。如果用户没有提供数据库名字，也可以正常连接，用户可以通过该连接创建新的数据库，如果用户提供了数据库名字，则说明该数据库用户已经创建好，缺省使用该数据库。返回值为空表示失败。应用程序需要保存返回的参数，以便后续API调用。


- `void taos_close(TAOS *taos)`

  关闭连接, 其中taos是taos_connect函数返回的指针。


- `int taos_query(TAOS *taos, char *sqlstr)`

  该API用来执行SQL语句，可以是DQL语句也可以是DML语句，或者DDL语句。其中的taos参数是通过taos_connect()获得的指针。返回值-1表示失败。


- `TAOS_RES *taos_use_result(TAOS *taos)`

  选择相应的查询结果集。


- `TAOS_ROW taos_fetch_row(TAOS_RES *res)`

  按行获取查询结果集中的数据。


- `int taos_num_fields(TAOS_RES *res)`

  获取查询结果集中的列数。


- `TAOS_FIELD *taos_fetch_fields(TAOS_RES *res)`

  获取查询结果集每列数据的属性（数据类型、名字、字节数），与taos_num_fileds配合使用，可用来解析taos_fetch_row返回的一个元组(一行)的数据。


- `void taos_free_result(TAOS_RES *res)`

  释放查询结果集以及相关的资源。查询完成后，务必调用该API释放资源，否则可能导致应用内存泄露。


- `void taos_init()`

  初始化环境变量。如果应用没有主动调用该API，那么应用在调用taos_connect时将自动调用。因此一般情况下应用程序无需手动调用该API。 


- `char *taos_errstr(TAOS *taos)`

  获取最近一次API调用失败的原因,返回值为字符串。


- `char *taos_errno(TAOS *taos)`

  获取最近一次API调用失败的原因，返回值为错误代码。


-  `int taos_options(TSDB_OPTION option, const void * arg, ...)`

   设置客户端选项，目前只支持时区设置（_TSDB_OPTION_TIMEZONE_）和编码设置（_TSDB_OPTION_LOCALE_）。时区和编码默认为操作系统当前设置。 

上述12个API是C/C++接口中最重要的API，剩余的辅助API请参看_taos.h_文件。

**注意**：对于单个数据库连接，在同一时刻只能有一个线程使用该链接调用API，否则会有未定义的行为出现并可能导致客户端crash。客户端应用可以通过建立多个连接进行多线程的数据写入或查询处理。

### C/C++异步API

同步API之外，TDengine还提供性能更高的异步调用API处理数据插入、查询操作。在软硬件环境相同的情况下，异步API处理数据插入的速度比同步API快2~4倍。异步API采用非阻塞式的调用方式，在系统真正完成某个具体数据库操作前，立即返回。调用的线程可以去处理其他工作，从而可以提升整个应用的性能。异步API在网络延迟严重的情况下，优点尤为突出。

异步API都需要应用提供相应的回调函数，回调函数参数设置如下：前两个参数都是一致的，第三个参数依不同的API而定。第一个参数param是应用调用异步API时提供给系统的，用于回调时，应用能够找回具体操作的上下文，依具体实现而定。第二个参数是SQL操作的结果集，如果为空，比如insert操作，表示没有记录返回，如果不为空，比如select操作，表示有记录返回。

异步API对于使用者的要求相对较高，用户可根据具体应用场景选择性使用。下面是三个重要的异步API： 

- `void taos_query_a(TAOS *taos, char *sqlstr, void (*fp)(void *param, TAOS_RES *, int code), void *param);`

  异步执行SQL语句。taos是调用taos_connect返回的数据库连接结构体。sqlstr是需要执行的SQL语句。fp是用户定义的回调函数。param是应用提供一个用于回调的参数。回调函数fp的第三个参数code用于指示操作是否成功，0表示成功，负数表示失败(调用taos_errstr获取失败原因)。应用在定义回调函数的时候，主要处理第二个参数TAOS_RES *，该参数是查询返回的结果集。 


- `void taos_fetch_rows_a(TAOS_RES *res, void (*fp)(void *param, TAOS_RES *, int numOfRows), void *param);`

  批量获取异步查询的结果集，只能与taos_query_a配合使用。其中_res_是_taos_query_a回调时返回的结果集结构体指针，fp为回调函数。回调函数中的param是用户可定义的传递给回调函数的参数结构体。numOfRows表明有fetch数据返回的行数（numOfRows并不是本次查询满足查询条件的全部元组数量）。在回调函数中，应用可以通过调用taos_fetch_row前向迭代获取批量记录中每一行记录。读完一块内的所有记录后，应用需要在回调函数中继续调用taos_fetch_rows_a获取下一批记录进行处理，直到返回的记录数（numOfRows）为零（结果返回完成）或记录数为负值（查询出错）。


- `void taos_fetch_row_a(TAOS_RES *res, void (*fp)(void *param, TAOS_RES *, TAOS_ROW row), void *param);`

  异步获取一条记录。其中res是taos_query_a回调时返回的结果集结构体指针。fp为回调函数。param是应用提供的一个用于回调的参数。回调时，第三个参数TAOS_ROW指向一行记录。不同于taos_fetch_rows_a，应用无需调用同步API taos_fetch_row来获取一个元组，更加简单。数据提取性能不及批量获取的API。

TDengine的异步API均采用非阻塞调用模式。应用程序可以用多线程同时打开多张表，并可以同时对每张打开的表进行查询或者插入操作。需要指出的是，**客户端应用必须确保对同一张表的操作完全串行化**，即对同一个表的插入或查询操作未完成时（未返回时），不能够执行第二个插入或查询操作。

### C/C++ 连续查询接口

TDengine提供时间驱动的实时流式计算API。可以每隔一指定的时间段，对一张或多张数据库的表(数据流)进行各种实时聚合计算操作。操作简单，仅有打开、关闭流的API。具体如下： 

- `TAOS_STREAM *taos_open_stream(TAOS *taos, char *sqlstr, void (*fp)(void *param, TAOS_RES *, TAOS_ROW row), int64_t stime, void *param)`

  该API用来创建数据流，其中taos是调用taos_connect返回的结构体指针；sqlstr是SQL查询语句（仅能使用查询语句）；fp是用户定义的回调函数指针，每次流式计算完成后，均回调该函数，用户可在该函数内定义其内部业务逻辑；param是应用提供的用于回调的一个参数，回调时，提供给应用；stime是流式计算开始的时间，如果是0，表示从现在开始，如果不为零，表示从指定的时间开始计算（UTC时间从1970/1/1算起的毫秒数）。返回值为NULL，表示创建成功，返回值不为空，表示成功。TDengine将查询的结果（TAOS_ROW）、查询状态（TAOS_RES）、用户定义参数（PARAM）传递给回调函数，在回调函数内，用户可以使用taos_num_fields获取结果集列数，taos_fetch_fields获取结果集每列数据的类型。


- `void taos_close_stream (TAOS_STREAM *tstr)`

  关闭数据流，其中提供的参数是taos_open_stream的返回值。用户停止流式计算的时候，务必关闭该数据流。
  

### C/C++ 数据订阅接口

订阅API目前支持订阅一张表，并通过定期轮询的方式不断获取写入表中的最新数据。 

- `TAOS_SUB *taos_subscribe(char *host, char *user, char *pass, char *db, char *table, int64_t time, int mseconds)`

  该API用来启动订阅，需要提供的参数包含：TDengine管理主节点的IP地址、用户名、密码、数据库、数据库表的名字；time是开始订阅消息的时间，是从1970年1月1日起计算的毫秒数，为长整型, 如果设为0，表示从当前时间开始订阅；mseconds为查询数据库更新的时间间隔，单位为毫秒，建议设为1000毫秒。返回值为一指向TDengine_SUB结构的指针，如果返回为空，表示失败。

- `TAOS_ROW taos_consume(TAOS_SUB *tsub)`

  该API用来获取最新消息，应用程序一般会将其置于一个无限循环语句中。其中参数tsub是taos_subscribe的返回值。如果数据库有新的记录，该API将返回，返回参数是一行记录。如果没有新的记录，该API将阻塞。如果返回值为空，说明系统出错，需要检查系统是否还在正常运行。

- `void taos_unsubscribe(TAOS_SUB *tsub)`

  该API用于取消订阅，参数tsub是taos_subscribe的返回值。应用程序退出时，需要调用该API，否则有资源泄露。

- `int taos_num_subfields(TAOS_SUB *tsub)`

  该API用来获取返回的一排数据中数据的列数

- `TAOS_FIELD *taos_fetch_subfields(TAOS_SUB *tsub)`

  该API用来获取每列数据的属性（数据类型、名字、字节数），与taos_num_subfields配合使用，可用来解析返回的一排数据。

##  Java Connector

### JDBC接口

如果用户使用Java开发企业级应用，可选用TDengine提供的JDBC Driver来调用服务。TDengine提供的JDBC Driver是标准JDBC规范的子集，遵循JDBC 标准(3.0)API规范，支持现有的各种Java开发框架。目前TDengine的JDBC driver并未发布到在线依赖仓库比如maven的中心仓库。因此用户开发时，需要手动把驱动包`taos-jdbcdriver-x.x.x-dist.jar`安装到开发环境的依赖仓库中。

TDengine 的驱动程序包的在不同操作系统上依赖不同的本地函数库（均由C语言编写）。Linux系统上，依赖一个名为`libtaos.so` 的本地库，.so即"Shared Object"缩写。成功安装TDengine后，`libtaos.so` 文件会被自动拷贝至`/usr/local/lib/taos`目录下，该目录也包含在Linux上自动扫描路径上。Windows系统上，JDBC驱动程序依赖于一个名为`taos.dll` 的本地库，.dll是动态链接库"Dynamic Link Library"的缩写。Windows上成功安装客户端后，JDBC驱动程序包默认位于`C:/TDengine/driver/JDBC/`目录下;其依赖的动态链接库`taos.dll`文件位于`C:/TDengine/driver/C`目录下，`taos.dll` 会被自动拷贝至系统默认搜索路径`C:/Windows/System32`下。

TDengine的JDBC Driver遵循标准JDBC规范，开发人员可以参考Oracle官方的JDBC相关文档来找到具体的接口和方法的定义与用法。TDengine的JDBC驱动在连接配置和支持的方法上与传统数据库驱动稍有不同。 

TDengine的JDBC URL规范格式为：

`jdbc:TSDB://{host_ip}:{port}/{database_name}?[user={user}|&password={password}|&charset={charset}|&cfgdir={config_dir}|&locale={locale}|&timezone={timezone}]`

其中，`{}`中的内容必须，`[]`中为可选。配置参数说明如下：

- user：登陆TDengine所用用户名；默认值root
- password：用户登陆密码；默认值taosdata
- charset：客户端使用的字符集；默认值为系统字符集
- cfgdir：客户端配置文件目录路径；Linux OS上默认值`/etc/taos` ，Windows OS上默认值 `C:/TDengine/cfg`
- locale：客户端语言环境；默认值系统当前locale
- timezone：客户端使用的时区；默认值为系统当前时区

以上所有参数均可在调用java.sql.DriverManager类创建连接时指定，示例如下：

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

这些配置参数中除了cfgdir外，均可在客户端配置文件taos.cfg中进行配置。调用java.sql.DriverManager时声明的配置参数优先级最高，JDBC URL的优先级次之，配置文件的优先级最低。例如charset同时在配置文件taos.cfg中配置，也在JDBC URL中配置，则使用JDBC URL中的配置值。

此外，尽管TDengine的JDBC驱动实现尽可能的与关系型数据库驱动保持一致，但时序空间数据库与关系对象型数据库服务的对象和技术特征的差异导致TDengine的Java API并不能与标准完全相同。对于有大量关系型数据库开发经验而初次接触TDengine的开发者来说，有以下一些值的注意的地方：

* TDengine不提供针对单条数据记录的删除和修改的操作，驱动中也没有支持相关方法
* 目前TDengine不支持表间的join或union操作，因此也缺乏对该部分API的支持
* TDengine支持批量写入，但是支持停留在SQL语句级别，而不是API级别，也就是说用户需要通过写特殊的SQL语句来实现批量
* 目前TDengine不支持嵌套查询(nested query)，对每个Connection的实例，至多只能有一个打开的ResultSet实例；如果在ResultSet还没关闭的情况下执行了新的查询，TSDBJDBCDriver则会自动关闭上一个ResultSet

对于TDengine操作的报错信息，用户可使用JDBCDriver包里提供的枚举类TSDBError.java来获取error message和error code的列表。对于更多的具体操作的相关代码，请参考TDengine提供的使用示范项目`JDBCDemo`。

## Python Connector

### Python客户端安装

用户可以在源代码的src/connector/python文件夹下找到python2和python3的安装包。用户可以通过pip命令安装： 

​		`pip install src/connector/python/python2/`

或

​		`pip install src/connector/python/python3/`

如果机器上没有pip命令，用户可将src/connector/python/python3或src/connector/python/python2下的taos文件夹拷贝到应用程序的目录使用。

### Python客户端接口

在使用TDengine的python接口时，需导入TDengine客户端模块：

```
import taos 
```

用户可通过python的帮助信息直接查看模块的使用信息，或者参考code/examples/python中的示例程序。以下为部分常用类和方法：

- _TDengineConnection_类

  参考python中help(taos.TDengineConnection)。

- _TDengineCursor_类

  参考python中help(taos.TDengineCursor)。

- _connect_方法

  用于生成taos.TDengineConnection的实例。

## RESTful Connector

为支持各种不同类型平台的开发，TDengine提供符合REST设计标准的API，即RESTful API。为最大程度降低学习成本，不同于其他数据库RESTful API的设计方法，TDengine直接通过HTTP POST 请求BODY中包含的SQL语句来操作数据库，仅需要一个URL。 

### HTTP请求格式 

​	`http://<ip>:<PORT>/rest/sql`

​    参数说明：

​    IP: 集群中的任一台主机

​    PORT: 配置文件中httpPort配置项，缺省为6020 

如：http://192.168.0.1:6020/rest/sql 是指向IP地址为192.168.0.1的URL. 

HTTP请求的Header里需带有身份认证信息，TDengine单机版仅支持Basic认证机制。

HTTP请求的BODY里就是一个完整的SQL语句，SQL语句中的数据表应提供数据库前缀，例如\<db-name>.\<tb-name>。如果表名不带数据库前缀，系统会返回错误。因为HTTP模块只是一个简单的转发，没有当前DB的概念。 

使用curl来发起一个HTTP Request, 语法如下：

```
curl -H 'Authorization: Basic <TOKEN>' -d '<SQL>' <ip>:<PORT>/rest/sql
```

或者

```
curl -u username:password -d '<SQL>' <ip>:<PORT>/rest/sql
```

其中，`TOKEN`为`{username}:{password}`经过Base64编码之后的字符串，例如`root:taosdata`编码后为`cm9vdDp0YW9zZGF0YQ==`

### HTTP返回格式

返回值为JSON格式，如下：
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

说明：

- 第一行”status”告知操作结果是成功还是失败;
- 第二行”head”是表的定义，如果不返回结果集，仅有一列“affected_rows”;
- 第三行是具体返回的数据，一排一排的呈现。如果不返回结果集，仅[[affected_rows]]
- 第四行”rows”表明总共多少行数据

### 使用示例

- 在demo库里查询表t1的所有记录, curl如下： 

  `curl -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' -d 'select * from demo.t1' 192.168.0.1:6020/rest/sql`

  返回值：

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

- 创建库demo：

  `curl -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' -d 'create database demo' 192.168.0.1:6020/rest/sql`

  返回值：
```
{
    "status": "succ",
    "head": ["affected_rows"],
    "data": [[1]],
    "rows": 1,
}
```

## Go Connector

TDengine提供了GO驱动程序“taosSql”包。taosSql驱动包是基于GO的“database/sql/driver”接口的实现。用户可在安装后的/usr/local/taos/connector/go目录获得GO的客户端驱动程序。用户需将驱动包/usr/local/taos/connector/go/src/taosSql目录拷贝到应用程序工程的src目录下。然后在应用程序中导入驱动包，就可以使用“database/sql”中定义的接口访问TDengine：

```Go
import (
    "database/sql"
    _ "taosSql"
)
```

taosSql驱动包内采用cgo模式，调用了TDengine的C/C++同步接口，与TDengine进行交互，因此，在数据库操作执行完成之前，客户端应用将处于阻塞状态。单个数据库连接，在同一时刻只能有一个线程调用API。客户应用可以建立多个连接，进行多线程的数据写入或查询处理。

更多使用的细节，请参考下载目录中的示例源码。
