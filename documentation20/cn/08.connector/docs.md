# 连接器

TDengine提供了丰富的应用程序开发接口，其中包括C/C++、Java、Python、Go、Node.js、C# 、RESTful 等，便于用户快速开发应用。

![image-connecotr](page://images/connector.png)

目前TDengine的连接器可支持的平台广泛，包括：X64/X86/ARM64/ARM32/MIPS/Alpha等硬件平台，以及Linux/Win64/Win32等开发环境。对照矩阵如下：

| **CPU**     | **X64   64bit** | **X64   64bit** | **X64   64bit** | **X86   32bit** | **ARM64** | **ARM32** | **MIPS   龙芯** | **Alpha   申威** | **X64   海光** |
| ----------- | --------------- | --------------- | --------------- | --------------- | --------- | --------- | --------------- | ---------------- | -------------- |
| **OS**      | **Linux**       | **Win64**       | **Win32**       | **Win32**       | **Linux** | **Linux** | **Linux**       | **Linux**        | **Linux**      |
| **C/C++**   | ●               | ●               | ●               | ○               | ●         | ●         | ○               | ○                | ○              |
| **JDBC**    | ●               | ●               | ●               | ○               | ●         | ●         | ○               | ○                | ○              |
| **Python**  | ●               | ●               | ●               | ○               | ●         | ●         | ○               | --               | ○              |
| **Go**      | ●               | ●               | ●               | ○               | ●         | ●         | ○               | --               | --             |
| **NodeJs**  | ●               | ●               | ○               | ○               | ●         | ●         | ○               | --               | --             |
| **C#**      | ○               | ●               | ●               | ○               | ○         | ○         | ○               | --               | --             |
| **RESTful** | ●               | ●               | ●               | ●               | ●         | ●         | ○               | ○                | ○              |

其中 ● 表示经过官方测试验证， ○ 表示非官方测试验证。

注意：

* 在没有安装TDengine服务端软件的系统中使用连接器（除RESTful外）访问 TDengine 数据库，需要安装相应版本的客户端安装包来使应用驱动（Linux系统中文件名为libtaos.so，Windows系统中为taos.dll）被安装在系统中，否则会产生无法找到相应库文件的错误。
* 所有执行 SQL 语句的 API，例如 C/C++ Connector 中的 `tao_query`、`taos_query_a`、`taos_subscribe` 等，以及其它语言中与它们对应的API，每次都只能执行一条 SQL 语句，如果实际参数中包含了多条语句，它们的行为是未定义的。
* 升级到TDengine到2.0.8.0版本的用户，必须更新JDBC连接TDengine必须升级taos-jdbcdriver到2.0.12及以上。
* 无论选用何种编程语言的连接器，2.0 及以上版本的 TDengine 推荐数据库应用的每个线程都建立一个独立的连接，或基于线程建立连接池，以避免连接内的“USE statement”状态量在线程之间相互干扰（但连接的查询和写入操作都是线程安全的）。

## <a class="anchor" id="driver"></a>安装连接器驱动步骤

服务器应该已经安装TDengine服务端安装包。连接器驱动安装步骤如下：

**Linux**

**1.   从涛思官网（https://www.taosdata.com/cn/all-downloads/）下载**

* X64硬件环境：TDengine-client-2.x.x.x-Linux-x64.tar.gz

* ARM64硬件环境：TDengine-client-2.x.x.x-Linux-aarch64.tar.gz

* ARM32硬件环境：TDengine-client-2.x.x.x-Linux-aarch32.tar.gz

**2.   解压缩软件包**

将软件包放置在当前用户可读写的任意目录下，然后执行下面的命令：

`tar -xzvf TDengine-client-xxxxxxxxx.tar.gz`

其中xxxxxxx需要替换为实际版本的字符串。

**3.   执行安装脚本**

解压软件包之后，会在解压目录下看到以下文件(目录)：

​    *install_client.sh*：安装脚本，用于应用驱动程序
​    *taos.tar.gz*：应用驱动安装包
​    *driver*：TDengine应用驱动driver
​    *connector*: 各种编程语言连接器（go/grafanaplugin/nodejs/python/JDBC）
​    *examples*: 各种编程语言的示例程序(c/C#/go/JDBC/matlab/python/R)

运行install_client.sh进行安装

**4.   配置taos.cfg**

编辑taos.cfg文件(默认路径/etc/taos/taos.cfg)，将firstEP修改为TDengine服务器的End Point，例如：h1.taos.com:6030

**提示： 如本机没有部署TDengine服务，仅安装了应用驱动，则taos.cfg中仅需配置firstEP，无需配置FQDN。**

**Windows x64/x86**

**1.   从涛思官网（https://www.taosdata.com/cn/all-downloads/）下载 ：**

* X64硬件环境：TDengine-client-2.X.X.X-Windows-x64.exe

* X86硬件环境：TDengine-client-2.X.X.X-Windows-x86.exe

**2. 执行安装程序，按提示选择默认值，完成安装**

**3. 安装路径**

默认安装路径为：C:\TDengine，其中包括以下文件(目录)：

​    *taos.exe*：taos shell命令行程序

​    *cfg* : 配置文件目录
​    *driver*: 应用驱动动态链接库
​    *examples*: 示例程序 bash/C/C#/go/JDBC/Python/Node.js
​    *include*: 头文件
​    *log* : 日志文件
​    *unins000.exe*: 卸载程序

**4. 配置taos.cfg**

编辑taos.cfg文件(默认路径C:\TDengine\cfg\taos.cfg)，将firstEP修改为TDengine服务器的End Point，例如：h1.taos.com:6030

**提示：** 

**1. 如利用FQDN连接服务器，必须确认本机网络环境DNS已配置好，或在hosts文件中添加FQDN寻址记录，如编辑C:\Windows\system32\drivers\etc\hosts，添加如下的记录：** **192.168.1.99  h1.taos.com**

**2．卸载：运行unins000.exe可卸载TDengine应用驱动。**

**安装验证**

以上安装和配置完成后，并确认TDengine服务已经正常启动运行，此时可以执行taos客户端进行登录。

**Linux环境：**

在linux shell下直接执行 taos，应该就能正常链接到tdegine服务，进入到taos shell界面，示例如下：

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

**Windows（x64/x86）环境：**

在cmd下进入到c:\TDengine目录下直接执行 taos.exe，应该就能正常链接到tdegine服务，进入到taos shell界面，示例如下：

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

## <a class="anchor" id="c-cpp"></a>C/C++ Connector

**C/C++连接器支持的系统有**： 

| **CPU类型**  | x64（64bit） |          |          | ARM64    | ARM32      |
| ------------ | ------------ | -------- | -------- | -------- | ---------- |
| **OS类型**   | Linux        | Win64    | Win32    | Linux    | Linux      |
| **支持与否** | **支持**     | **支持** | **支持** | **支持** | **开发中** |

C/C++的API类似于MySQL的C API。应用程序使用时，需要包含TDengine头文件 _taos.h_（安装后，位于 _/usr/local/taos/include_）：

```C
#include <taos.h>
```

注意：

* 在编译时需要链接TDengine动态库。Linux 为 *libtaos.so* ，安装后，位于 _/usr/local/taos/driver_。Windows为 taos.dll，安装后位于  *C:\TDengine*。
* 如未特别说明，当API的返回值是整数时，_0_ 代表成功，其它是代表失败原因的错误码，当返回值是指针时， _NULL_ 表示失败。

使用C/C++连接器的示例代码请参见 https://github.com/taosdata/TDengine/tree/develop/tests/examples/c 。

### 基础API

基础API用于完成创建数据库连接等工作，为其它API的执行提供运行时环境。

- `void taos_init()`

  初始化运行环境。如果应用没有主动调用该API，那么应用在调用`taos_connect`时将自动调用，故应用程序一般无需手动调用该API。 

- `void taos_cleanup()`

  清理运行环境，应用退出前应调用此API。

- `int taos_options(TSDB_OPTION option, const void * arg, ...)`

   设置客户端选项，目前只支持时区设置（_TSDB_OPTION_TIMEZONE_）和编码设置（_TSDB_OPTION_LOCALE_）。时区和编码默认为操作系统当前设置。 

- `char *taos_get_client_info()`

  获取客户端版本信息。

- `TAOS *taos_connect(const char *host, const char *user, const char *pass, const char *db, int port)`

  创建数据库连接，初始化连接上下文。其中需要用户提供的参数包含：

    - host：TDengine管理主节点的FQDN
    - user：用户名
    - pass：密码
    - db：数据库名字，如果用户没有提供，也可以正常连接，用户可以通过该连接创建新的数据库，如果用户提供了数据库名字，则说明该数据库用户已经创建好，缺省使用该数据库
    - port：端口号

  返回值为空表示失败。应用程序需要保存返回的参数，以便后续API调用。

- `char *taos_get_server_info(TAOS *taos)`

  获取服务端版本信息。

- `int taos_select_db(TAOS *taos, const char *db)`

  将当前的缺省数据库设置为`db`。

- `void taos_close(TAOS *taos)`

  关闭连接, 其中`taos`是`taos_connect`函数返回的指针。

### 同步查询API

传统的数据库操作API，都属于同步操作。应用调用API后，一直处于阻塞状态，直到服务器返回结果。TDengine支持如下API：

- `TAOS_RES* taos_query(TAOS *taos, const char *sql)`

  该API用来执行SQL语句，可以是DQL、DML或DDL语句。 其中的`taos`参数是通过`taos_connect`获得的指针。不能通过返回值是否是 NULL 来判断执行结果是否失败，而是需要用`taos_errno`函数解析结果集中的错误代码来进行判断。

- `int taos_result_precision(TAOS_RES *res)`

  返回结果集时间戳字段的精度，`0` 代表毫秒，`1` 代表微秒，`2` 代表纳秒。

- `TAOS_ROW taos_fetch_row(TAOS_RES *res)`

  按行获取查询结果集中的数据。

- `int taos_fetch_block(TAOS_RES *res, TAOS_ROW *rows)`

  批量获取查询结果集中的数据，返回值为获取到的数据的行数。

- `int taos_num_fields(TAOS_RES *res)` 和 `int taos_field_count(TAOS_RES *res)`

  这两个API等价，用于获取查询结果集中的列数。

- `int* taos_fetch_lengths(TAOS_RES *res)`

  获取结果集中每个字段的长度。 返回值是一个数组，其长度为结果集的列数。

- `int taos_affected_rows(TAOS_RES *res)`

  获取被所执行的 SQL 语句影响的行数。

- `TAOS_FIELD *taos_fetch_fields(TAOS_RES *res)`

  获取查询结果集每列数据的属性（数据类型、名字、字节数），与taos_num_fileds配合使用，可用来解析`taos_fetch_row`返回的一个元组(一行)的数据。 `TAOS_FIELD` 的结构如下：

```c
typedef struct taosField {
  char     name[65];  // 列名
  uint8_t  type;      // 数据类型
  int16_t  bytes;     // 字节数
} TAOS_FIELD;
```

- `void taos_stop_query(TAOS_RES *res)`

  停止一个查询的执行。

- `void taos_free_result(TAOS_RES *res)`

  释放查询结果集以及相关的资源。查询完成后，务必调用该API释放资源，否则可能导致应用内存泄露。但也需注意，释放资源后，如果再调用`taos_consume`等获取查询结果的函数，将导致应用Crash。

- `char *taos_errstr(TAOS_RES *res)`

  获取最近一次API调用失败的原因,返回值为字符串。

- `char *taos_errno(TAOS_RES *res)`

  获取最近一次API调用失败的原因，返回值为错误代码。

**注意**：2.0及以上版本 TDengine 推荐数据库应用的每个线程都建立一个独立的连接，或基于线程建立连接池。而不推荐在应用中将该连接 (TAOS\*) 结构体传递到不同的线程共享使用。基于 TAOS 结构体发出的查询、写入等操作具有多线程安全性，但 “USE statement” 等状态量有可能在线程之间相互干扰。此外，C 语言的连接器可以按照需求动态建立面向数据库的新连接（该过程对用户不可见），同时建议只有在程序最后退出的时候才调用 taos_close 关闭连接。

### 异步查询API

同步API之外，TDengine还提供性能更高的异步调用API处理数据插入、查询操作。在软硬件环境相同的情况下，异步API处理数据插入的速度比同步API快2～4倍。异步API采用非阻塞式的调用方式，在系统真正完成某个具体数据库操作前，立即返回。调用的线程可以去处理其他工作，从而可以提升整个应用的性能。异步API在网络延迟严重的情况下，优点尤为突出。

异步API都需要应用提供相应的回调函数，回调函数参数设置如下：前两个参数都是一致的，第三个参数依不同的API而定。第一个参数param是应用调用异步API时提供给系统的，用于回调时，应用能够找回具体操作的上下文，依具体实现而定。第二个参数是SQL操作的结果集，如果为空，比如insert操作，表示没有记录返回，如果不为空，比如select操作，表示有记录返回。

异步API对于使用者的要求相对较高，用户可根据具体应用场景选择性使用。下面是三个重要的异步API： 

- `void taos_query_a(TAOS *taos, const char *sql, void (*fp)(void *param, TAOS_RES *, int code), void *param);`

  异步执行SQL语句。
  
    * taos：调用taos_connect返回的数据库连接
    * sql：需要执行的SQL语句
    * fp：用户定义的回调函数，其第三个参数`code`用于指示操作是否成功，`0`表示成功，负数表示失败(调用`taos_errstr`获取失败原因)。应用在定义回调函数的时候，主要处理第二个参数`TAOS_RES *`，该参数是查询返回的结果集
    * param：应用提供一个用于回调的参数

- `void taos_fetch_rows_a(TAOS_RES *res, void (*fp)(void *param, TAOS_RES *, int numOfRows), void *param);`

  批量获取异步查询的结果集，只能与`taos_query_a`配合使用。其中：
  
    * res：`taos_query_a`回调时返回的结果集
    * fp：回调函数。其参数`param`是用户可定义的传递给回调函数的参数结构体；`numOfRows`是获取到的数据的行数（不是整个查询结果集的函数）。 在回调函数中，应用可以通过调用`taos_fetch_row`前向迭代获取批量记录中每一行记录。读完一块内的所有记录后，应用需要在回调函数中继续调用`taos_fetch_rows_a`获取下一批记录进行处理，直到返回的记录数（numOfRows）为零（结果返回完成）或记录数为负值（查询出错）。

TDengine的异步API均采用非阻塞调用模式。应用程序可以用多线程同时打开多张表，并可以同时对每张打开的表进行查询或者插入操作。需要指出的是，**客户端应用必须确保对同一张表的操作完全串行化**，即对同一个表的插入或查询操作未完成时（未返回时），不能够执行第二个插入或查询操作。

### 参数绑定API

除了直接调用 `taos_query` 进行查询，TDengine也提供了支持参数绑定的Prepare API，与 MySQL 一样，这些API目前也仅支持用问号`?`来代表待绑定的参数，具体如下：

- `TAOS_STMT* taos_stmt_init(TAOS *taos)`

  创建一个 TAOS_STMT 对象用于后续调用。

- `int taos_stmt_prepare(TAOS_STMT *stmt, const char *sql, unsigned long length)`

  解析一条sql语句，将解析结果和参数信息绑定到stmt上，如果参数length大于0，将使用此参数作为sql语句的长度，如等于0，将自动判断sql语句的长度。

- `int taos_stmt_bind_param(TAOS_STMT *stmt, TAOS_BIND *bind)`

  进行参数绑定，bind指向一个数组，需保证此数组的元素数量和顺序与sql语句中的参数完全一致。TAOS_BIND 的使用方法与 MySQL中的 MYSQL_BIND 一致，具体定义如下：

```c
typedef struct TAOS_BIND {
  int            buffer_type;
  void *         buffer;
  unsigned long  buffer_length;  // 未实际使用
  unsigned long *length;
  int *          is_null;
  int            is_unsigned;    // 未实际使用
  int *          error;          // 未实际使用
} TAOS_BIND;
```

- `int taos_stmt_add_batch(TAOS_STMT *stmt)`

  将当前绑定的参数加入批处理中，调用此函数后，可以再次调用`taos_stmt_bind_param`绑定新的参数。需要注意，此函数仅支持 insert/import 语句，如果是select等其他SQL语句，将返回错误。

- `int taos_stmt_execute(TAOS_STMT *stmt)`

  执行准备好的语句。目前，一条语句只能执行一次。

- `TAOS_RES* taos_stmt_use_result(TAOS_STMT *stmt)`

  获取语句的结果集。结果集的使用方式与非参数化调用时一致，使用完成后，应对此结果集调用 `taos_free_result`以释放资源。

- `int taos_stmt_close(TAOS_STMT *stmt)`

  执行完毕，释放所有资源。

### 连续查询接口

TDengine提供时间驱动的实时流式计算API。可以每隔一指定的时间段，对一张或多张数据库的表(数据流)进行各种实时聚合计算操作。操作简单，仅有打开、关闭流的API。具体如下： 

- `TAOS_STREAM *taos_open_stream(TAOS *taos, const char *sql, void (*fp)(void *param, TAOS_RES *, TAOS_ROW row), int64_t stime, void *param, void (*callback)(void *))`

  该API用来创建数据流，其中：
    * taos：已经建立好的数据库连接
    * sql：SQL查询语句（仅能使用查询语句）
    * fp：用户定义的回调函数指针，每次流式计算完成后，TDengine将查询的结果（TAOS_ROW）、查询状态（TAOS_RES）、用户定义参数（PARAM）传递给回调函数，在回调函数内，用户可以使用taos_num_fields获取结果集列数，taos_fetch_fields获取结果集每列数据的类型。
    * stime：是流式计算开始的时间，如果是0，表示从现在开始，如果不为零，表示从指定的时间开始计算（UTC时间从1970/1/1算起的毫秒数）
    * param：是应用提供的用于回调的一个参数，回调时，提供给应用
    * callback: 第二个回调函数，会在连续查询自动停止时被调用。

  返回值为NULL，表示创建成功，返回值不为空，表示成功。

- `void taos_close_stream (TAOS_STREAM *tstr)`

  关闭数据流，其中提供的参数是taos_open_stream的返回值。用户停止流式计算的时候，务必关闭该数据流。

### 数据订阅接口

订阅API目前支持订阅一张或多张表，并通过定期轮询的方式不断获取写入表中的最新数据。 

* `TAOS_SUB *taos_subscribe(TAOS* taos, int restart, const char* topic, const char *sql, TAOS_SUBSCRIBE_CALLBACK fp, void *param, int interval)`

  该函数负责启动订阅服务，成功时返回订阅对象，失败时返回 `NULL`，其参数为：
    * taos：已经建立好的数据库连接
    * restart：如果订阅已经存在，是重新开始，还是继续之前的订阅
    * topic：订阅的主题（即名称），此参数是订阅的唯一标识
    * sql：订阅的查询语句，此语句只能是 `select` 语句，只应查询原始数据，只能按时间正序查询数据
    * fp：收到查询结果时的回调函数（稍后介绍函数原型），只在异步调用时使用，同步调用时此参数应该传 `NULL`
    * param：调用回调函数时的附加参数，系统API将其原样传递到回调函数，不进行任何处理
    * interval：轮询周期，单位为毫秒。异步调用时，将根据此参数周期性的调用回调函数，为避免对系统性能造成影响，不建议将此参数设置的过小；同步调用时，如两次调用`taos_consume`的间隔小于此周期，API将会阻塞，直到时间间隔超过此周期。

* `typedef void (*TAOS_SUBSCRIBE_CALLBACK)(TAOS_SUB* tsub, TAOS_RES *res, void* param, int code)`

  异步模式下，回调函数的原型，其参数为：
    * tsub：订阅对象
    * res：查询结果集，注意结果集中可能没有记录
    * param：调用 `taos_subscribe`时客户程序提供的附加参数
    * code：错误码

* `TAOS_RES *taos_consume(TAOS_SUB *tsub)`

  同步模式下，该函数用来获取订阅的结果。 用户应用程序将其置于一个循环之中。 如两次调用`taos_consume`的间隔小于订阅的轮询周期，API将会阻塞，直到时间间隔超过此周期。 如果数据库有新记录到达，该API将返回该最新的记录，否则返回一个没有记录的空结果集。 如果返回值为 `NULL`，说明系统出错。 异步模式下，用户程序不应调用此API。

* `void taos_unsubscribe(TAOS_SUB *tsub, int keepProgress)`

  取消订阅。 如参数 `keepProgress` 不为0，API会保留订阅的进度信息，后续调用 `taos_subscribe` 时可以基于此进度继续；否则将删除进度信息，后续只能重新开始读取数据。

## <a class="anchor" id="python"></a>Python Connector

Python连接器的使用参见[视频教程](https://www.taosdata.com/blog/2020/11/11/1963.html)

### 安装准备
* 应用驱动安装请参考[安装连接器驱动步骤](https://www.taosdata.com/cn/documentation/connector#driver)。
* 已安装python 2.7 or >= 3.4
* 已安装pip 或 pip3

### Python客户端安装

#### Linux

用户可以在源代码的src/connector/python（或者tar.gz的/connector/python）文件夹下找到python2和python3的connector安装包。用户可以通过pip命令安装： 

​		`pip install src/connector/python/linux/python2/`

或

​		`pip3 install src/connector/python/linux/python3/`

#### Windows
在已安装Windows TDengine 客户端的情况下， 将文件"C:\TDengine\driver\taos.dll" 拷贝到 "C:\windows\system32" 目录下, 然后进入Windwos <em>cmd</em> 命令行界面
```cmd
cd C:\TDengine\connector\python\windows
python -m pip install python2\
```
或
```cmd
cd C:\TDengine\connector\python\windows
python -m pip install python3\
```

* 如果机器上没有pip命令，用户可将src/connector/python/python3或src/connector/python/python2下的taos文件夹拷贝到应用程序的目录使用。
对于windows 客户端，安装TDengine windows 客户端后，将C:\TDengine\driver\taos.dll拷贝到C:\windows\system32目录下即可。

### 使用

#### 代码示例

* 导入TDengine客户端模块

```python
import taos
```
* 获取连接并获取游标对象

```python
conn = taos.connect(host="127.0.0.1", user="root", password="taosdata", config="/etc/taos")
c1 = conn.cursor()
```
* <em>host</em> 是TDengine 服务端所有IP, <em>config</em> 为客户端配置文件所在目录

* 写入数据

```python
import datetime

# 创建数据库
c1.execute('create database db')
c1.execute('use db')
# 建表
c1.execute('create table tb (ts timestamp, temperature int, humidity float)')
# 插入数据
start_time = datetime.datetime(2019, 11, 1)
affected_rows = c1.execute('insert into tb values (\'%s\', 0, 0.0)' %start_time)
# 批量插入数据
time_interval = datetime.timedelta(seconds=60)
sqlcmd = ['insert into tb values']
for irow in range(1,11):
  start_time += time_interval
  sqlcmd.append('(\'%s\', %d, %f)' %(start_time, irow, irow*1.2))
affected_rows = c1.execute(' '.join(sqlcmd))
```

* 查询数据

```python
c1.execute('select * from tb')
# 拉取查询结果
data = c1.fetchall()
# 返回的结果是一个列表，每一行构成列表的一个元素
numOfRows = c1.rowcount
numOfCols = len(c1.description)
for irow in range(numOfRows):
  print("Row%d: ts=%s, temperature=%d, humidity=%f" %(irow, data[irow][0], data[irow][1],data[irow][2]))

# 直接使用cursor 循环拉取查询结果
c1.execute('select * from tb')
for data in c1:
  print("ts=%s, temperature=%d, humidity=%f" %(data[0], data[1],data[2]))
```

* 创建订阅

```python
# 创建一个主题为 'test' 消费周期为1000毫秒的订阅
# 第一个参数为 True 表示重新开始订阅，如为 False 且之前创建过主题为 'test' 的订阅，则表示继续消费此订阅的数据，而不是重新开始消费所有数据
sub = conn.subscribe(True, "test", "select * from tb;", 1000)
```

* 消费订阅的数据

```python
data = sub.consume()
for d in data:
    print(d)
```

* 取消订阅

```python
sub.close()
```

* 关闭连接

```python
c1.close()
conn.close()
```

#### 帮助信息

用户可通过python的帮助信息直接查看模块的使用信息，或者参考tests/examples/python中的示例程序。以下为部分常用类和方法：

- _TDengineConnection_ 类

  参考python中help(taos.TDengineConnection)。
  这个类对应客户端和TDengine建立的一个连接。在客户端多线程的场景下，推荐每个线程申请一个独立的连接实例，而不建议多线程共享一个连接。

- _TDengineCursor_ 类

  参考python中help(taos.TDengineCursor)。
  这个类对应客户端进行的写入、查询操作。在客户端多线程的场景下，这个游标实例必须保持线程独享，不能夸线程共享使用，否则会导致返回结果出现错误。

- _connect_ 方法

  用于生成taos.TDengineConnection的实例。

### Python客户端使用示例代码

在tests/examples/python中，我们提供了一个示例Python程序read_example.py，可以参考这个程序来设计用户自己的写入、查询程序。在安装了对应的客户端后，通过import taos引入taos类。主要步骤如下

- 通过taos.connect获取TDengineConnection对象，这个对象可以一个程序只申请一个，在多线程中共享。
- 通过TDengineConnection对象的 .cursor()方法获取一个新的游标对象，这个游标对象必须保证每个线程独享。
- 通过游标对象的execute()方法，执行写入或查询的SQL语句
- 如果执行的是写入语句，execute返回的是成功写入的行数信息affected rows
- 如果执行的是查询语句，则execute执行成功后，需要通过fetchall方法去拉取结果集。
具体方法可以参考示例代码。

## <a class="anchor" id="restful"></a>RESTful Connector

为支持各种不同类型平台的开发，TDengine提供符合REST设计标准的API，即RESTful API。为最大程度降低学习成本，不同于其他数据库RESTful API的设计方法，TDengine直接通过HTTP POST 请求BODY中包含的SQL语句来操作数据库，仅需要一个URL。RESTful连接器的使用参见[视频教程](https://www.taosdata.com/blog/2020/11/11/1965.html)。

### HTTP请求格式 

```
http://<ip>:<PORT>/rest/sql
```

参数说明：

- IP: 集群中的任一台主机
- PORT: 配置文件中httpPort配置项，缺省为6041

例如：http://192.168.0.1:6041/rest/sql 是指向IP地址为192.168.0.1的URL. 

HTTP请求的Header里需带有身份认证信息，TDengine支持Basic认证与自定义认证两种机制，后续版本将提供标准安全的数字签名机制来做身份验证。

- 自定义身份认证信息如下所示（<token>稍后介绍）

```
Authorization: Taosd <TOKEN>
```

- Basic身份认证信息如下所示

```
Authorization: Basic <TOKEN>
```

HTTP请求的BODY里就是一个完整的SQL语句，SQL语句中的数据表应提供数据库前缀，例如\<db-name>.\<tb-name>。如果表名不带数据库前缀，系统会返回错误。因为HTTP模块只是一个简单的转发，没有当前DB的概念。 

使用curl通过自定义身份认证方式来发起一个HTTP Request，语法如下：

```bash
curl -H 'Authorization: Basic <TOKEN>' -d '<SQL>' <ip>:<PORT>/rest/sql
```

或者

```bash
curl -u username:password -d '<SQL>' <ip>:<PORT>/rest/sql
```

其中，`TOKEN`为`{username}:{password}`经过Base64编码之后的字符串，例如`root:taosdata`编码后为`cm9vdDp0YW9zZGF0YQ==`

### HTTP返回格式

返回值为JSON格式，如下:

```json
{
    "status": "succ",
    "head": ["Time Stamp","current", …],
    "data": [
        ["2018-10-03 14:38:05.000", 10.3, …],
        ["2018-10-03 14:38:15.000", 12.6, …]
    ],
    "rows": 2
} 
```

说明：

- status: 告知操作结果是成功还是失败
- head: 表的定义，如果不返回结果集，仅有一列“affected_rows”
- data: 具体返回的数据，一排一排的呈现，如果不返回结果集，仅[[affected_rows]]
- rows: 表明总共多少行数据

### 自定义授权码

HTTP请求中需要带有授权码`<TOKEN>`，用于身份识别。授权码通常由管理员提供，可简单的通过发送`HTTP GET`请求来获取授权码，操作如下：

```bash
curl http://<ip>:6041/rest/login/<username>/<password>
```

其中，`ip`是TDengine数据库的IP地址，`username`为数据库用户名，`password`为数据库密码，返回值为`JSON`格式，各字段含义如下：

- status：请求结果的标志位

- code：返回值代码

- desc: 授权码

获取授权码示例：

```bash
curl http://192.168.0.1:6041/rest/login/root/taosdata
```

返回值：

```json
{
  "status": "succ",
  "code": 0,
  "desc": "/KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04"
}
```

### 使用示例

- 在demo库里查询表d1001的所有记录： 

```bash
curl -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' -d 'select * from demo.d1001' 192.168.0.1:6041/rest/sql
```
返回值：

```json
{
    "status": "succ",
    "head": ["Time Stamp","current","voltage","phase"],
    "data": [
        ["2018-10-03 14:38:05.000",10.3,219,0.31],
        ["2018-10-03 14:38:15.000",12.6,218,0.33]
    ],
    "rows": 2
}
```

- 创建库demo：

```bash
curl -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' -d 'create database demo' 192.168.0.1:6041/rest/sql
```

返回值：
```json
{
    "status": "succ",
    "head": ["affected_rows"],
    "data": [[1]],
    "rows": 1,
}
```

### 其他用法

#### 结果集采用Unix时间戳

HTTP请求URL采用`sqlt`时，返回结果集的时间戳将采用Unix时间戳格式表示，例如

```bash
curl -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' -d 'select * from demo.d1001' 192.168.0.1:6041/rest/sqlt
```

返回值：

```json
{
    "status": "succ",
    "head": ["column1","column2","column3"],
    "data": [
        [1538548685000,10.3,219,0.31],
        [1538548695000,12.6,218,0.33]
    ],
    "rows": 2
}
```

#### 结果集采用UTC时间字符串

HTTP请求URL采用`sqlutc`时，返回结果集的时间戳将采用UTC时间字符串表示，例如
```bash
  curl -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' -d 'select * from demo.t1' 192.168.0.1:6041/rest/sqlutc
```

返回值：

```json
{
    "status": "succ",
    "head": ["column1","column2","column3"],
    "data": [
        ["2018-10-03T14:38:05.000+0800",10.3,219,0.31],
        ["2018-10-03T14:38:15.000+0800",12.6,218,0.33]
    ],
    "rows": 2
}
```

### 重要配置项

下面仅列出一些与RESTful接口有关的配置参数，其他系统参数请看配置文件里的说明。注意：配置修改后，需要重启taosd服务才能生效

- httpPort: 对外提供RESTful服务的端口号，默认绑定到6041
- httpMaxThreads: 启动的线程数量，默认为2（2.0.17版本开始，默认值改为CPU核数的一半向下取整）
- restfulRowLimit: 返回结果集（JSON格式）的最大条数，默认值为10240
- httpEnableCompress: 是否支持压缩，默认不支持，目前TDengine仅支持gzip压缩格式
- httpDebugFlag: 日志开关，131：仅错误和报警信息，135：调试信息，143：非常详细的调试信息，默认131

## <a class="anchor" id="csharp"></a>CSharp Connector

C#连接器支持的系统有：Linux 64/Windows x64/Windows x86

### 安装准备

* 应用驱动安装请参考[安装连接器驱动步骤](https://www.taosdata.com/cn/documentation/connector#driver)。
* .NET接口文件﻿TDengineDrivercs.cs和参考程序示例TDengineTest.cs均位于Windows客户端install_directory/examples/C#目录下。
* 在Windows系统上，C#应用程序可以使用TDengine的原生C接口来执行所有数据库操作，后续版本将提供ORM（dapper）框架驱动。

### 安装验证

运行install_directory/examples/C#/C#Checker/C#Checker.exe

```cmd
cd {install_directory}/examples/C#/C#Checker
csc /optimize *.cs
C#Checker.exe -h <fqdn>
```

### C#连接器的使用

在Windows系统上，.NET应用程序可以使用TDengine的.NET接口来执行所有数据库的操作。使用.NET接口的步骤如下所示：

1. 将.NET接口文件﻿TDengineDrivercs.cs加入到应用程序所在.NET项目中。
2. 用户可以参考﻿TDengineTest.cs来定义数据库连接参数，以及如何执行数据插入、查询等操作；

此.NET接口需要用到taos.dll文件，所以在执行应用程序前，拷贝Windows客户端install_directory/driver目录中的taos.dll文件到.NET项目最后生成.exe可执行文件所在文件夹。之后运行exe文件，即可访问TDengine数据库并做插入、查询等操作。

**注意：**

1. TDengine V2.0.3.0之后同时支持32位和64位Windows系统，所以.NET项目在生成.exe文件时，“解决方案”/“项目”的“平台”请选择对应的“X86” 或“x64”。
2. 此.NET接口目前已经在Visual Studio 2015/2017中验证过，其它VS版本尚待验证。

### 第三方驱动

Maikebing.Data.Taos是一个TDengine的ADO.Net提供器，支持linux，windows。该开发包由热心贡献者`麦壳饼@@maikebing`提供，具体请参考

```
//接口下载
https://github.com/maikebing/Maikebing.EntityFrameworkCore.Taos   
//用法说明    
https://www.taosdata.com/blog/2020/11/02/1901.html                    
```

## <a class="anchor" id="go"></a>Go Connector

### 安装准备

* 应用驱动安装请参考[安装连接器驱动步骤](https://www.taosdata.com/cn/documentation/connector#driver)。

TDengine提供了GO驱动程序`taosSql`。 `taosSql`实现了GO语言的内置接口`database/sql/driver`。用户只需按如下方式引入包就可以在应用程序中访问TDengine, 详见`https://github.com/taosdata/driver-go/blob/develop/taosSql/driver_test.go`。

使用 Go 连接器的示例代码请参考 https://github.com/taosdata/TDengine/tree/develop/tests/examples/go 以及[视频教程](https://www.taosdata.com/blog/2020/11/11/1951.html)。

```Go
import (
    "database/sql"
    _ "github.com/taosdata/driver-go/taosSql"
)
```
**建议使用Go版本1.13或以上，并开启模块支持：**

```bash
go env -w GO111MODULE=on  
go env -w GOPROXY=https://goproxy.io,direct  
```

### 常用API

- `sql.Open(DRIVER_NAME string, dataSourceName string) *DB` 

  该API用来打开DB，返回一个类型为\*DB的对象，一般情况下，DRIVER_NAME设置为字符串`taosSql`, dataSourceName设置为字符串`user:password@/tcp(host:port)/dbname`，如果客户想要用多个goroutine并发访问TDengine, 那么需要在各个goroutine中分别创建一个sql.Open对象并用之访问TDengine

  **注意**： 该API成功创建的时候，并没有做权限等检查，只有在真正执行Query或者Exec的时候才能真正的去创建连接，并同时检查user/password/host/port是不是合法。 另外，由于整个驱动程序大部分实现都下沉到taosSql所依赖的libtaos中。所以，sql.Open本身特别轻量。 

-  `func (db *DB) Exec(query string, args ...interface{}) (Result, error)`

  sql.Open内置的方法，用来执行非查询相关SQL

-  `func (db *DB) Query(query string, args ...interface{}) (*Rows, error)`

  sql.Open内置的方法，用来执行查询语句

- `func (db *DB) Prepare(query string) (*Stmt, error)`

  sql.Open内置的方法，Prepare creates a prepared statement for later queries or executions.

- `func (s *Stmt) Exec(args ...interface{}) (Result, error)`

  sql.Open内置的方法，executes a prepared statement with the given arguments and returns a Result summarizing the effect of the statement.

- `func (s *Stmt) Query(args ...interface{}) (*Rows, error)`

  sql.Open内置的方法，Query executes a prepared query statement with the given arguments and returns the query results as a \*Rows.

- `func (s *Stmt) Close() error`

  sql.Open内置的方法，Close closes the statement.	

## <a class="anchor" id="nodejs"></a>Node.js Connector

Node.js连接器支持的系统有：

| **CPU类型**  | x64（64bit） |          |          | aarch64  | aarch32  |
| ------------ | ------------ | -------- | -------- | -------- | -------- |
| **OS类型**   | Linux        | Win64    | Win32    | Linux    | Linux    |
| **支持与否** | **支持**     | **支持** | **支持** | **支持** | **支持** |

Node.js连接器的使用参见[视频教程](https://www.taosdata.com/blog/2020/11/11/1957.html)

### 安装准备

* 应用驱动安装请参考[安装连接器驱动步骤](https://www.taosdata.com/cn/documentation/connector#driver)。

### 安装Node.js连接器

用户可以通过[npm](https://www.npmjs.com/)来进行安装，也可以通过源代码*src/connector/nodejs/* 来进行安装。具体安装步骤如下：

首先，通过[npm](https://www.npmjs.com/)安装node.js 连接器.

```bash
npm install td2.0-connector
```
我们建议用户使用npm 安装node.js连接器。如果您没有安装npm, 可以将*src/connector/nodejs/*拷贝到您的nodejs 项目目录下

我们使用[node-gyp](https://github.com/nodejs/node-gyp)和TDengine服务端进行交互。安装node.js 连接器之前，还需安装以下软件：

### Linux

- `python` (建议`v2.7` , `v3.x.x` 目前还不支持)
- `node`  2.0.6支持v12.x和v10.x，2.0.5及更早版本支持v10.x版本，其他版本可能存在包兼容性的问题。
- `make`
- c语言编译器比如[GCC](https://gcc.gnu.org)

### Windows

#### 安装方法1

使用微软的[windows-build-tools](https://github.com/felixrieseberg/windows-build-tools)在`cmd` 命令行界面执行`npm install --global --production windows-build-tools` 即可安装所有的必备工具

#### 安装方法2

手动安装以下工具:

- 安装Visual Studio相关：[Visual Studio Build 工具](https://visualstudio.microsoft.com/thank-you-downloading-visual-studio/?sku=BuildTools) 或者 [Visual Studio 2017 Community](https://visualstudio.microsoft.com/pl/thank-you-downloading-visual-studio/?sku=Community)
- 安装 [Python](https://www.python.org/downloads/) 2.7(`v3.x.x` 暂不支持) 并执行 `npm config set python python2.7` 
- 进入`cmd`命令行界面, `npm config set msvs_version 2017`

如果以上步骤不能成功执行, 可以参考微软的node.js用户手册[Microsoft's Node.js Guidelines for Windows](https://github.com/Microsoft/nodejs-guidelines/blob/master/windows-environment.md#compiling-native-addon-modules)

如果在Windows 10 ARM 上使用ARM64 Node.js, 还需添加 "Visual C++ compilers and libraries for ARM64" 和 "Visual  C++ ATL for ARM64".

### 示例程序

示例程序源码位于install_directory/examples/nodejs，有：

Node-example.js       		node.js示例源程序
Node-example-raw.js

### 安装验证

在安装好TDengine客户端后，使用nodejsChecker.js程序能够验证当前环境是否支持nodejs方式访问Tdengine。

验证方法：

1. 新建安装验证目录，例如：`~/tdengine-test`，拷贝github上nodejsChecker.js源程序。下载地址：（https://github.com/taosdata/TDengine/tree/develop/tests/examples/nodejs/nodejsChecker.js）。

2. 在命令中执行以下命令：

```bash
npm init -y
npm install td2.0-connector
node nodejsChecker.js host=localhost
```

3. 执行以上步骤后，在命令行会输出nodejs连接Tdengine实例，并执行简答插入和查询的结果。

### Node.js连接器的使用

以下是node.js 连接器的一些基本使用方法，详细的使用方法可参考[TDengine Node.js connector](http://docs.taosdata.com/node)

#### 建立连接

使用node.js连接器时，必须先<em>require</em> `td2.0-connector`，然后使用 `taos.connect` 函数。`taos.connect` 函数必须提供的参数是`host`，其它参数在没有提供的情况下会使用如下的默认值。最后需要初始化`cursor` 来和TDengine服务端通信 

```javascript
const taos = require('td2.0-connector');
var conn = taos.connect({host:"127.0.0.1", user:"root", password:"taosdata", config:"/etc/taos",port:0})
var cursor = conn.cursor(); // Initializing a new cursor
```

关闭连接可执行

```javascript
conn.close();
```

#### 执行SQL和插入数据

对于DDL语句（例如create database、create table、use等），可以使用cursor的execute方法。代码如下：

```js
cursor.execute('create database if not exists test;')
```

以上代码创建了一个名称为test的数据库。对于DDL语句，一般没有返回值，cursor的execute返回值为0。

对于Insert语句，代码如下：

```js
var affectRows = cursor.execute('insert into test.weather values(now, 22.3, 34);')
```

execute方法的返回值为该语句影响的行数，上面的sql向test库的weather表中，插入了一条数据，则返回值affectRows为1。

TDengine目前还不支持update和delete语句。

#### 查询

可通过 `cursor.query` 函数来查询数据库。

```javascript
var query = cursor.query('show databases;')
```

查询的结果可以通过 `query.execute()` 函数获取并打印出来

```javascript
var promise = query.execute();
promise.then(function(result) {
  result.pretty(); 
});
```
格式化查询语句还可以使用`query`的`bind`方法。如下面的示例：`query`会自动将提供的数值填入查询语句的`?`里。

```javascript
var query = cursor.query('select * from meterinfo.meters where ts <= ? and areaid = ?;').bind(new Date(), 5);
query.execute().then(function(result) {
  result.pretty();
})
```
如果在`query`语句里提供第二个参数并设为`true`也可以立即获取查询结果。如下：

```javascript
var promise = cursor.query('select * from meterinfo.meters where v1 = 30;', true)
promise.then(function(result) {
  result.pretty();
})
```
#### 异步函数

异步查询数据库的操作和上面类似，只需要在`cursor.execute`, `TaosQuery.execute`等函数后面加上`_a`。
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

### 示例

[node-example.js](https://github.com/taosdata/TDengine/tree/master/tests/examples/nodejs/node-example.js)提供了一个使用NodeJS 连接器建表，插入天气数据并查询插入的数据的代码示例

[node-example-raw.js](https://github.com/taosdata/TDengine/tree/master/tests/examples/nodejs/node-example-raw.js)同样是一个使用NodeJS 连接器建表，插入天气数据并查询插入的数据的代码示例，但和上面不同的是，该示例只使用`cursor`.
