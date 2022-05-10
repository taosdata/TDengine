# 连接器

TDengine 提供了丰富的应用程序开发接口，其中包括 C/C++、Java、Python、Go、Node.js、C# 、RESTful 等，便于用户快速开发应用。

![image-connector](../images/connector.png)

目前 TDengine 的连接器可支持的平台广泛，包括：X64/X86/ARM64/ARM32/MIPS/Alpha 等硬件平台，以及 Linux/Win64/Win32 等开发环境。对照矩阵如下：

| **CPU**     | **X64   64bit** | **X64   64bit** | **X64   64bit** | **X86   32bit** | **ARM64** | **ARM32** | **MIPS   龙芯** | **Alpha   申威** | **X64   海光** |
| ----------- | --------------- | --------------- | --------------- | --------------- | --------- | --------- | --------------- | ---------------- | -------------- |
| **OS**      | **Linux**       | **Win64**       | **Win32**       | **Win32**       | **Linux** | **Linux** | **Linux**       | **Linux**        | **Linux**      |
| **C/C++**   | ●               | ●               | ●               | ○               | ●         | ●         | ○               | ○                | ○              |
| **JDBC**    | ●               | ●               | ●               | ○               | ●         | ●         | ○               | ○                | ○              |
| **Python**  | ●               | ●               | ●               | ○               | ●         | ●         | ○               | --               | ○              |
| **Go**      | ●               | ●               | ●               | ○               | ●         | ●         | ○               | --               | --             |
| **Node.js**  | ●               | ●               | ○               | ○               | ●         | ●         | ○               | --               | --             |
| **C#**      | ●               | ●               | ○               | ○               | ○         | ○         | ○               | --               | --             |
| **RESTful** | ●               | ●               | ●               | ●               | ●         | ●         | ○               | ○                | ○              |

其中 ● 表示官方测试验证通过，○ 表示非官方测试验证通过，-- 表示未经验证。

注意：

* 在没有安装 TDengine 服务端软件的系统中使用连接器（除 RESTful 外）访问 TDengine 数据库，需要安装相应版本的客户端安装包来使应用驱动（Linux 系统中文件名为 libtaos.so，Windows 系统中为 taos.dll）被安装在系统中，否则会产生无法找到相应库文件的错误。
* 所有执行 SQL 语句的 API，例如 C/C++ Connector 中的 `taos_query`、`taos_query_a`、`taos_subscribe` 等，以及其它语言中与它们对应的 API，每次都只能执行一条 SQL 语句，如果实际参数中包含了多条语句，它们的行为是未定义的。
* 升级 TDengine 到 2.0.8.0 版本的用户，必须更新 JDBC。连接 TDengine 必须升级 taos-jdbcdriver 到 2.0.12 及以上。详细的版本依赖关系请参见 [taos-jdbcdriver 文档](https://www.taosdata.com/cn/documentation/connector/java#version)。
* 无论选用何种编程语言的连接器，2.0 及以上版本的 TDengine 推荐数据库应用的每个线程都建立一个独立的连接，或基于线程建立连接池，以避免连接内的“USE statement”状态量在线程之间相互干扰（但连接的查询和写入操作都是线程安全的）。

## <a class="anchor" id="driver"></a>安装连接器驱动步骤

服务器应该已经安装 TDengine 服务端安装包。连接器驱动安装步骤如下：

**Linux**

**1.   从[涛思官网](https://www.taosdata.com/cn/all-downloads/)下载<!-- REPLACE_OPEN_TO_ENTERPRISE__YOU_CAN_GET_CONNECTOR_DRIVER_FROM_TAOS -->：**

* X64 硬件环境：TDengine-client-2.x.x.x-Linux-x64.tar.gz

* ARM64 硬件环境：TDengine-client-2.x.x.x-Linux-aarch64.tar.gz

* ARM32 硬件环境：TDengine-client-2.x.x.x-Linux-aarch32.tar.gz

**2.   解压缩软件包**

将软件包放置在当前用户可读写的任意目录下，然后执行下面的命令：

`tar -xzvf TDengine-client-xxxxxxxxx.tar.gz`

其中xxxxxxxxx需要替换为实际版本的字符串。

**3.   执行安装脚本**

解压软件包之后，会在解压目录下看到以下文件(目录)：

​    *install_client.sh*：安装脚本，用于应用驱动程序
​    *taos.tar.gz*：应用驱动安装包
​    *driver*：TDengine应用驱动driver
​    *connector*: 各种编程语言连接器（Go/Node.js/Python/JDBC）
​    *examples*: 各种编程语言的示例程序（c/C#/Go/JDBC/MATLAB/Python/R）

运行install_client.sh进行安装。

**4.   配置taos.cfg**

编辑 taos.cfg 文件（默认路径/etc/taos/taos.cfg），将 firstEP 修改为 TDengine 服务器的 End Point，例如：h1.taos.com:6030

**提示：**

1. **如本机没有部署 TDengine 服务，仅安装了应用驱动，则 taos.cfg 中仅需配置 firstEP，无需配置 FQDN。**
2. **为防止与服务器端连接时出现 “unable to resolve FQDN” 错误，建议确认客户端的 hosts 文件已经配置正确的 FQDN 值。**

**Windows x64/x86**

**1.   从[涛思官网](https://www.taosdata.com/cn/all-downloads/)下载<!-- REPLACE_OPEN_TO_ENTERPRISE__YOU_CAN_GET_CONNECTOR_DRIVER_WINDOWS_FROM_TAOS -->：**

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

编辑 taos.cfg 文件(默认路径C:\TDengine\cfg\taos.cfg)，将 firstEP 修改为 TDengine 服务器的 End Point，例如：h1.taos.com:6030

**提示：** 

1. **如利用 FQDN 连接服务器，必须确认本机网络环境DNS已配置好，或在 hosts 文件中添加 FQDN 寻址记录，如编辑C:\Windows\system32\drivers\etc\hosts，添加如下的记录：`192.168.1.99  h1.taos.com` **
2. **卸载：运行unins000.exe可卸载TDengine应用驱动。**

### 安装验证

以上安装和配置完成后，并确认 TDengine 服务已经正常启动运行，此时可以执行 taos 客户端进行登录。

**Linux环境：**

在 Linux shell 下直接执行 taos，应该就能正常连接到 TDengine 服务，进入到 taos shell 界面，示例如下：

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

在 cmd 下进入到 C:\TDengine 目录下直接执行 taos.exe，应该就能正常链接到 TDengine 服务，进入到 taos shell 界面，示例如下：

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
| **支持与否** | **支持**     | **支持** | **支持** | **支持** | **支持** |

C/C++ 的 API 类似于 MySQL 的 C API。应用程序使用时，需要包含 TDengine 头文件 *taos.h*，里面列出了提供的 API 的函数原型。安装后，taos.h 位于：

- Linux：`/usr/local/taos/include`
- Windows：`C:\TDengine\include`

```C
#include <taos.h>
```

注意：

* 在编译时需要链接 TDengine 动态库。Linux 为 *libtaos.so* ，安装后，位于 _/usr/local/taos/driver_。Windows 为 taos.dll，安装后位于  *C:\TDengine*。
* 如未特别说明，当API的返回值是整数时，_0_ 代表成功，其它是代表失败原因的错误码，当返回值是指针时， _NULL_ 表示失败。
* 在 taoserror.h 中有所有的错误码，以及对应的原因描述。`tstrerror(errno)` 可以获取错误码对应的错误信息。

### 示例程序

使用 C/C++ 连接器的示例代码请参见 `https://github.com/taosdata/TDengine/tree/develop/examples/c`。

示例程序源码也可以在安装目录下的 `examples/c` 路径下找到：

**apitest.c、asyncdemo.c、demo.c、prepare.c、stream.c、subscribe.c**

该目录下有 makefile，在 Linux 环境下，直接执行 `make` 就可以编译得到执行文件。

在一台机器上启动 TDengine 服务，执行这些示例程序，按照提示输入 TDengine 服务的 FQDN，就可以正常运行，并打印出信息。 

**提示：**在 ARM 环境下编译时，请将 makefile 中的 `-msse4.2` 打开，这个选项只有在 x64/x86 硬件平台上才能支持。

### 基础API

基础 API 用于完成创建数据库连接等工作，为其它 API 的执行提供运行时环境。

- `void taos_init()`

  初始化运行环境。如果应用没有主动调用该 API，那么应用在调用 `taos_connect()` 时将自动调用，故应用程序一般无需手动调用该 API。 

- `void taos_cleanup()`

  清理运行环境，应用退出前应调用此 API。

- `int taos_options(TSDB_OPTION option, const void * arg, ...)`

   设置客户端选项，目前支持区域设置（`TSDB_OPTION_LOCALE`）、字符集设置（`TSDB_OPTION_CHARSET`）、时区设置（`TSDB_OPTION_TIMEZONE`）、配置文件路径设置（`TSDB_OPTION_CONFIGDIR`）。区域设置、字符集、时区默认为操作系统当前设置。 

   返回值为 `0` 表示成功，`-1` 表示失败。

- `char *taos_get_client_info()`

  获取客户端版本信息。

- `TAOS *taos_connect(const char *host, const char *user, const char *pass, const char *db, int port)`

  创建数据库连接，初始化连接上下文。其中需要用户提供的参数包含：

    - host：TDengine 管理主节点的 FQDN
    - user：用户名
    - pass：密码
    - db：数据库名字，如果用户没有提供，也可以正常连接，用户可以通过该连接创建新的数据库，如果用户提供了数据库名字，则说明该数据库用户已经创建好，缺省使用该数据库
    - port：TDengine 管理主节点的端口号

  返回值为空表示失败。应用程序需要保存返回的参数，以便后续 API 调用。

  **提示:** 同一进程可以根据不同的 host/port 连接多个 taosd 集群

- `char *taos_get_server_info(TAOS *taos)`

  获取服务端版本信息。

- `int taos_select_db(TAOS *taos, const char *db)`

  将当前的缺省数据库设置为`db`。

  返回值为错误码。

- `void taos_close(TAOS *taos)`

  关闭连接，其中`taos`是`taos_connect`函数返回的指针。

### 同步查询API

传统的数据库操作 API，都属于同步操作。应用调用 API 后，一直处于阻塞状态，直到服务器返回结果。TDengine 支持如下 API：

- `TAOS_RES* taos_query(TAOS *taos, const char *sql)`

  该API用来执行 SQL 语句，可以是 DQL、DML 或 DDL 语句。 其中的 `taos` 参数是通过 `taos_connect` 获得的指针。不能通过返回值是否是 NULL 来判断执行结果是否失败，而是需要用 `taos_errno` 函数解析结果集中的错误代码来进行判断。

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

  获取查询结果集每列数据的属性（列的名称、列的数据类型、列的长度），与 taos_num_fileds 配合使用，可用来解析 `taos_fetch_row` 返回的一个元组(一行)的数据。 `TAOS_FIELD` 的结构如下：

```c
typedef struct taosField {
  char     name[65];  // 列名
  uint8_t  type;      // 数据类型
  int16_t  bytes;     // 长度，单位是字节
} TAOS_FIELD;
```

- `void taos_stop_query(TAOS_RES *res)`

  停止一个查询的执行。

- `void taos_free_result(TAOS_RES *res)`

  释放查询结果集以及相关的资源。查询完成后，务必调用该 API 释放资源，否则可能导致应用内存泄露。但也需注意，释放资源后，如果再调用 `taos_consume` 等获取查询结果的函数，将导致应用 Crash。

- `char *taos_errstr(TAOS_RES *res)`

  获取最近一次API调用失败的原因,返回值为字符串。

- `int taos_errno(TAOS_RES *res)`

  获取最近一次API调用失败的原因，返回值为错误代码。

**注意**：2.0及以上版本 TDengine 推荐数据库应用的每个线程都建立一个独立的连接，或基于线程建立连接池。而不推荐在应用中将该连接 (TAOS\*) 结构体传递到不同的线程共享使用。基于 TAOS 结构体发出的查询、写入等操作具有多线程安全性，但 “USE statement” 等状态量有可能在线程之间相互干扰。此外，C 语言的连接器可以按照需求动态建立面向数据库的新连接（该过程对用户不可见），同时建议只有在程序最后退出的时候才调用 taos_close 关闭连接。

### 异步查询API

同步 API 之外，TDengine 还提供性能更高的异步调用API处理数据插入、查询操作。在软硬件环境相同的情况下，异步 API 处理数据插入的速度比同步API快2～4倍。异步 API 采用非阻塞式的调用方式，在系统真正完成某个具体数据库操作前，立即返回。调用的线程可以去处理其他工作，从而可以提升整个应用的性能。异步 API 在网络延迟严重的情况下，优点尤为突出。

异步 API 都需要应用提供相应的回调函数，回调函数参数设置如下：前两个参数都是一致的，第三个参数依不同的 API 而定。第一个参数 `param` 是应用调用异步API时提供给系统的，用于回调时，应用能够找回具体操作的上下文，依具体实现而定。第二个参数是 `sql` 操作的结果集，如果为空，比如 insert 操作，表示没有记录返回，如果不为空，比如 select 操作，表示有记录返回。

异步 API 对于使用者的要求相对较高，用户可根据具体应用场景选择性使用。下面是两个重要的异步 API： 

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

TDengine 的异步 API 均采用非阻塞调用模式。应用程序可以用多线程同时打开多张表，并可以同时对每张打开的表进行查询或者插入操作。需要指出的是，**客户端应用必须确保对同一张表的操作完全串行化**，即对同一个表的插入或查询操作未完成时（未返回时），不能够执行第二个插入或查询操作。

<a class="anchor" id="stmt"></a>
### 参数绑定 API

除了直接调用 `taos_query` 进行查询，TDengine 也提供了支持参数绑定的 Prepare API，与 MySQL 一样，这些 API 目前也仅支持用问号 `?` 来代表待绑定的参数。文档中有时也会把此功能称为“原生接口写入”。

从 2.1.1.0 和 2.1.2.0 版本开始，TDengine 大幅改进了参数绑定接口对数据写入（INSERT）场景的支持。这样在通过参数绑定接口写入数据时，就避免了 SQL 语法解析的资源消耗，从而在绝大多数情况下显著提升写入性能。此时的典型操作步骤如下：

1. 调用 `taos_stmt_init` 创建参数绑定对象；
2. 调用 `taos_stmt_prepare` 解析 INSERT 语句；
3. 如果 INSERT 语句中预留了表名但没有预留 TAGS，那么调用 `taos_stmt_set_tbname` 来设置表名；
4. 如果 INSERT 语句中既预留了表名又预留了 TAGS（例如 INSERT 语句采取的是自动建表的方式），那么调用 `taos_stmt_set_tbname_tags` 来设置表名和 TAGS 的值；
5. 调用 `taos_stmt_bind_param_batch` 以多列的方式设置 VALUES 的值，或者调用 `taos_stmt_bind_param` 以单行的方式设置 VALUES 的值；
6. 调用 `taos_stmt_add_batch` 把当前绑定的参数加入批处理；
7. 可以重复第 3～6 步，为批处理加入更多的数据行；
8. 调用 `taos_stmt_execute` 执行已经准备好的批处理指令；
9. 执行完毕，调用 `taos_stmt_close` 释放所有资源。

说明：如果 `taos_stmt_execute` 执行成功，假如不需要改变 SQL 语句的话，那么是可以复用 `taos_stmt_prepare` 的解析结果，直接进行第 3～6 步绑定新数据的。但如果执行出错，那么并不建议继续在当前的环境上下文下继续工作，而是建议释放资源，然后从 `taos_stmt_init` 步骤重新开始。

除 C/C++ 语言外，TDengine 的 Java 语言 JNI Connector 也提供参数绑定接口支持，具体请另外参见：[参数绑定接口的 Java 用法](https://www.taosdata.com/cn/documentation/connector/java#stmt-java)。

接口相关的具体函数如下（也可以参考 [prepare.c](https://github.com/taosdata/TDengine/blob/develop/examples/c/prepare.c) 文件中使用对应函数的方式）：

- `TAOS_STMT* taos_stmt_init(TAOS *taos)`

  创建一个 TAOS_STMT 对象用于后续调用。

- `int taos_stmt_prepare(TAOS_STMT *stmt, const char *sql, unsigned long length)`

  解析一条 SQL 语句，将解析结果和参数信息绑定到 stmt 上，如果参数 length 大于 0，将使用此参数作为 SQL 语句的长度，如等于 0，将自动判断 SQL 语句的长度。

- `int taos_stmt_bind_param(TAOS_STMT *stmt, TAOS_BIND *bind)`

  不如 `taos_stmt_bind_param_batch` 效率高，但可以支持非 INSERT 类型的 SQL 语句。  
  进行参数绑定，bind 指向一个数组（代表所要绑定的一行数据），需保证此数组中的元素数量和顺序与 SQL 语句中的参数完全一致。TAOS_BIND 的使用方法与 MySQL 中的 MYSQL_BIND 一致，具体定义如下：

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

- `int taos_stmt_set_tbname(TAOS_STMT* stmt, const char* name)`

  （2.1.1.0 版本新增，仅支持用于替换 INSERT 语句中的参数值）  
  当 SQL 语句中的表名使用了 `?` 占位时，可以使用此函数绑定一个具体的表名。

- `int taos_stmt_set_tbname_tags(TAOS_STMT* stmt, const char* name, TAOS_BIND* tags)`

  （2.1.2.0 版本新增，仅支持用于替换 INSERT 语句中的参数值）  
  当 SQL 语句中的表名和 TAGS 都使用了 `?` 占位时，可以使用此函数绑定具体的表名和具体的 TAGS 取值。最典型的使用场景是使用了自动建表功能的 INSERT 语句（目前版本不支持指定具体的 TAGS 列）。tags 参数中的列数量需要与 SQL 语句中要求的 TAGS 数量完全一致。

- `int taos_stmt_bind_param_batch(TAOS_STMT* stmt, TAOS_MULTI_BIND* bind)`

  （2.1.1.0 版本新增，仅支持用于替换 INSERT 语句中的参数值）  
  以多列的方式传递待绑定的数据，需要保证这里传递的数据列的顺序、列的数量与 SQL 语句中的 VALUES 参数完全一致。TAOS_MULTI_BIND 的具体定义如下：

```c
typedef struct TAOS_MULTI_BIND {
  int          buffer_type;
  void *       buffer;
  uintptr_t    buffer_length;
  int32_t *    length;
  char *       is_null;
  int          num;             // 列的个数，即 buffer 中的参数个数
} TAOS_MULTI_BIND;
```

- `int taos_stmt_add_batch(TAOS_STMT *stmt)`

  将当前绑定的参数加入批处理中，调用此函数后，可以再次调用 `taos_stmt_bind_param` 或 `taos_stmt_bind_param_batch` 绑定新的参数。需要注意，此函数仅支持 INSERT/IMPORT 语句，如果是 SELECT 等其他 SQL 语句，将返回错误。

- `int taos_stmt_execute(TAOS_STMT *stmt)`

  执行准备好的语句。目前，一条语句只能执行一次。

- `TAOS_RES* taos_stmt_use_result(TAOS_STMT *stmt)`

  获取语句的结果集。结果集的使用方式与非参数化调用时一致，使用完成后，应对此结果集调用 `taos_free_result` 以释放资源。

- `int taos_stmt_close(TAOS_STMT *stmt)`

  执行完毕，释放所有资源。

- `char * taos_stmt_errstr(TAOS_STMT *stmt)`

  （2.1.3.0 版本新增）  
  用于在其他 stmt API 返回错误（返回错误码或空指针）时获取错误信息。

<a class="anchor" id="schemaless"></a>
### Schemaless 方式写入接口

除了使用 SQL 方式或者使用参数绑定 API 写入数据外，还可以使用 Schemaless 的方式完成写入。Schemaless 可以免于预先创建超级表/数据子表的数据结构，而是可以直接写入数据，TDengine 系统会根据写入的数据内容自动创建和维护所需要的表结构。Schemaless 的使用方式详见 [Schemaless 写入](https://www.taosdata.com/cn/documentation/insert#schemaless) 章节，这里介绍与之配套使用的 C/C++ API。  

- `TAOS_RES* taos_schemaless_insert(TAOS* taos, const char* lines[], int numLines, int protocol, int precision)`
  
  **功能说明**  
    该接口将行协议的文本数据写入到 TDengine 中。
    
  **参数说明**  
    taos:  数据库连接，通过 taos_connect 函数建立的数据库连接。  
    lines：文本数据。满足解析格式要求的无模式文本字符串。  
    numLines:文本数据的行数，不能为 0 。  
    protocol: 行协议类型，用于标识文本数据格式。  
    precision：文本数据中的时间戳精度字符串。  
    
  **返回值**  
    TAOS_RES 结构体，应用可以通过使用 taos_errstr 获得错误信息，也可以使用 taos_errno 获得错误码。  
    在某些情况下，返回的 TAOS_RES 为 NULL，此时仍然可以调用 taos_errno 来安全地获得错误码信息。  
    返回的 TAOS_RES 需要调用方来负责释放，否则会出现内存泄漏。  
      
  **说明**  
    协议类型是枚举类型，包含以下三种格式：  
    TSDB_SML_LINE_PROTOCOL：InfluxDB 行协议（Line Protocol)  
    TSDB_SML_TELNET_PROTOCOL: OpenTSDB 文本行协议  
    TSDB_SML_JSON_PROTOCOL: OpenTSDB JSON 协议格式  
    
    时间戳分辨率的定义，定义在 taos.h 文件中，具体内容如下：  
    TSDB_SML_TIMESTAMP_NOT_CONFIGURED = 0,  
    TSDB_SML_TIMESTAMP_HOURS,  
    TSDB_SML_TIMESTAMP_MINUTES,  
    TSDB_SML_TIMESTAMP_SECONDS,  
    TSDB_SML_TIMESTAMP_MILLI_SECONDS,  
    TSDB_SML_TIMESTAMP_MICRO_SECONDS,  
    TSDB_SML_TIMESTAMP_NANO_SECONDS  
    
    需要注意的是，时间戳分辨率参数只在协议类型为 SML_LINE_PROTOCOL 的时候生效。  
    对于 OpenTSDB 的文本协议，时间戳的解析遵循其官方解析规则 — 按照时间戳包含的字符的数量来确认时间精度。
    
  **支持版本**  
    该功能接口从 2.3.0.0 版本开始支持。

```c
#include <stdlib.h>
#include <stdio.h>
#include <taos.h>
 
int main() {
  const char* host = "127.0.0.1";
  const char* user = "root";
  const char* passwd = "taosdata";
    
  // connect to server
  TAOS* taos = taos_connect(host, user, passwd, "test", 0);
   
  // prepare the line string
  char* lines1[] = {
      "stg,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
      "stg,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833641000000"
  };
 
  // schema-less insert
  TAOS_RES* res = taos_schemaless_insert(taos, lines1, 2, TSDB_SML_LINE_PROTOCOL, TSDB_SML_TIMESTAMP_NANO_SECONDS);
  if (taos_errno(res) != 0) {
    printf("failed to insert schema-less data, reason: %s\n", taos_errstr(res));
  }
 
  taos_free_result(res);
 
  // close the connection
  taos_close(taos);
  return (code);
}
```

### 数据订阅接口

订阅 API 目前支持订阅一张或多张表，并通过定期轮询的方式不断获取写入表中的最新数据。 

* `TAOS_SUB *taos_subscribe(TAOS* taos, int restart, const char* topic, const char *sql, TAOS_SUBSCRIBE_CALLBACK fp, void *param, int interval)`

  该函数负责启动订阅服务，成功时返回订阅对象，失败时返回 `NULL`，其参数为：
    * taos：已经建立好的数据库连接
    * restart：如果订阅已经存在，是重新开始，还是继续之前的订阅
    * topic：订阅的主题（即名称），此参数是订阅的唯一标识
    * sql：订阅的查询语句，此语句只能是 `select` 语句，只应查询原始数据，只能按时间正序查询数据
    * fp：收到查询结果时的回调函数（稍后介绍函数原型），只在异步调用时使用，同步调用时此参数应该传 `NULL`
    * param：调用回调函数时的附加参数，系统API将其原样传递到回调函数，不进行任何处理
    * interval：轮询周期，单位为毫秒。异步调用时，将根据此参数周期性的调用回调函数，为避免对系统性能造成影响，不建议将此参数设置的过小；同步调用时，如两次调用 `taos_consume` 的间隔小于此周期，API将会阻塞，直到时间间隔超过此周期。

* `typedef void (*TAOS_SUBSCRIBE_CALLBACK)(TAOS_SUB* tsub, TAOS_RES *res, void* param, int code)`

  异步模式下，回调函数的原型，其参数为：
    * tsub：订阅对象
    * res：查询结果集，注意结果集中可能没有记录
    * param：调用 `taos_subscribe` 时客户程序提供的附加参数
    * code：错误码
  
  **注意**：在这个回调函数里不可以做耗时过长的处理，尤其是对于返回的结果集中数据较多的情况，否则有可能导致客户端阻塞等异常状态。如果必须进行复杂计算，则建议在另外的线程中进行处理。

* `TAOS_RES *taos_consume(TAOS_SUB *tsub)`

  同步模式下，该函数用来获取订阅的结果。 用户应用程序将其置于一个循环之中。 如两次调用 `taos_consume()` 的间隔小于订阅的轮询周期，API将会阻塞，直到时间间隔超过此周期。 如果数据库有新记录到达，该API将返回该最新的记录，否则返回一个没有记录的空结果集。 如果返回值为 `NULL`，说明系统出错。 异步模式下，用户程序不应调用此 API。

  **注意**：在调用 `taos_consume()` 之后，用户应用应确保尽快调用 `taos_fetch_row()` 或 `taos_fetch_block()` 来处理订阅结果，否则服务端会持续缓存查询结果数据等待客户端读取，极端情况下会导致服务端内存消耗殆尽，影响服务稳定性。

* `void taos_unsubscribe(TAOS_SUB *tsub, int keepProgress)`

  取消订阅。 如参数 `keepProgress` 不为0，API会保留订阅的进度信息，后续调用 `taos_subscribe()` 时可以基于此进度继续；否则将删除进度信息，后续只能重新开始读取数据。

<!-- REPLACE_OPEN_TO_ENTERPRISE__JAVA_CONNECTOR_DOC -->

## <a class="anchor" id="python"></a>Python Connector

Python 连接器的使用参见[视频教程](https://www.taosdata.com/blog/2020/11/11/1963.html)

* **安装**：参见下面具体步骤
* **示例程序**：位于 install_directory/examples/python

### 安装

Python 连接器支持的系统有：Linux 64/Windows x64

安装前准备：

- 已安装好 TDengine 应用驱动，请参考[安装连接器驱动步骤](https://www.taosdata.com/cn/documentation/connector#driver)
- 已安装 Python 2.7 or >= 3.4
- 已安装 pip

### Python连接器安装

Python 连接器可以通过 `pip` 从 PyPI 下载安装。注意 TDengine Python 连接器的包名为 `taospy` 而不是 `taos`（这是一个与 TDengine 无关的另一个程序）。但为保持向后兼容性，仍然使用 `import taos` 导入。

```bash
pip install taospy
```

如果不使用系统默认的 `python` 和 `pip`，则需要指定 `pip` 的版本或路径：

```bash
pip2 install taospy
pip3 install taospy
```

Python 命令行依赖 taos 动态库 `libtaos.so` 或 `taos.dll`, 对于 Windows 客户端，安装TDengine windows 客户端后，如果不能正常 `import taos`，可以将 `C:\TDengine\driver\taos.dll` 拷贝到 `C:\windows\system32` 目录后重新尝试。

对于无法联网用户，可以将TDengine客户端中的 `connector/python` 路径（Linux 下其安装路径为 `/usr/local/taos/connector/python/`，Windows 下默认安装路径为 `C:\TDengine\connector\python`）添加到 `PYTHONPATH` 环境变量中使用。

### 示例程序

示例程序源码位于 `<install_directory>/examples/python`，有：

* **read_example.py**       Python示例源程序

用户可以参考 `read_example.py` 这个程序来设计用户自己的写入、查询程序。

在安装了对应的应用驱动后，通过 `import taos` 引入 taos 类。主要步骤如下：

- 通过 taos.connect 获取 TaosConnection对象，这个对象可以一个程序只申请一个，在多线程中共享。

- 通过 TaosConnection 对象的 `.cursor()` 方法获取一个新的游标对象，这个游标对象必须保证每个线程独享。

- 通过游标对象的 execute()方法，执行写入或查询的 SQL 语句。

- 如果执行的是写入语句，execute 返回的是成功写入的行数信息 affected rows。

- 如果执行的是查询语句，则 execute 执行成功后，需要通过 fetchall 方法去拉取结果集。 具体方法可以参考示例代码。

### 安装验证

运行如下指令：

```bash
cd {install_directory}/examples/python/PYTHONConnectorChecker/`
python3 PythonChecker.py -host <fqdn>
```

验证通过将打印出成功信息。

### Python 连接器的使用

#### PEP-249 兼容 API

您可以像其他数据库一样，使用类似 [PEP-249](https://www.python.org/dev/peps/pep-0249/) 数据库 API 规范风格的 API：

```python
import taos

conn = taos.connect()
cursor = conn.cursor()

cursor.execute("show databases")
results = cursor.fetchall()
for row in results:
    print(row)
```

##### 代码示例

1. 导入 TDengine 客户端模块

    ```python
    import taos
    ```

2. 获取连接并获取游标对象

    ```python
    conn = taos.connect(host="127.0.0.1", user="root", password="taosdata", config="/etc/taos")
    c1 = conn.cursor()
    ```

    *host* 是 TDengine 服务端所在IP, *config* 为客户端配置文件所在目录。

3. 写入数据

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

4. 查询数据

    ```python
    c1.execute('select * from tb')
    # 拉取查询结果
    data = c1.fetchall()
    # 返回的结果是一个列表，每一行构成列表的一个元素
    numOfRows = c1.rowcount
    numOfCols = len(c1.description)
    for irow in range(numOfRows):
        print("Row%d: ts=%s, temperature=%d, humidity=%f" %(irow, data[irow][0], data[irow][1], data[irow][2]))

    # 直接使用cursor 循环拉取查询结果
    c1.execute('select * from tb')
    for data in c1:
        print("ts=%s, temperature=%d, humidity=%f" %(data[0], data[1], data[2]))
    ```

#### Query API

从 v2.1.0 版本开始, 我们提供另外一种方法：`connection.query` 来操作数据库。

```python
import taos

conn = taos.connect()
conn.execute("create database if not exists pytest")

result = conn.query("show databases")
num_of_fields = result.field_count
for field in result.fields:
    print(field)
for row in result:
    print(row)
conn.execute("drop database pytest")
```

`query` 方法会返回一个 `TaosResult` 对象，并提供了以下属性或方法:

属性:

- `fields`: `TaosFields` 集合类，提供返回数据的列信息。
- `field_count`: 返回数据的列数.
- `affected_rows`: 插入数据的行数.
- `row_count`: 查询数据结果数.
- `precision`: 当前数据库的时间精度.

方法:

- `fetch_all()`: 类似于 `cursor.fetchall()` 返回同样的集合数据
- `fetch_all_into_dict()`: v2.1.1 新添加的API，将上面的数据转换成字典类型返回
- `blocks_iter()` `rows_iter()`: 根据底层API提供的两种不同迭代器。
- `fetch_rows_a`: 异步API
- `errno`: 错误码
- `errstr`: 错误信息
- `close`: 关闭结果对象，一般不需要直接调用

#### 订阅 API

1. 创建一个同步订阅队列：

    ```python
    # 创建一个主题为 'test' 消费周期为1000毫秒的订阅
    #   第一个参数为 True 表示重新开始订阅，如为 False 且之前创建过主题为 'test' 的订阅，
    #   则表示继续消费此订阅的数据，而不是重新开始消费所有数据
    sub = conn.subscribe(True, "test", "select * from tb;", 1000)
    ```

2. 消费订阅的数据

    ```python
    data = sub.consume()
    for d in data:
        print(d)
    ```

3. 取消订阅

    ```python
    sub.close()
    ```

4. 关闭连接

    ```python
    conn.close()
    ```

#### JSON 类型

从 `taospy` `v2.2.0` 开始，Python 连接器开始支持 JSON 数据类型的标签（TDengine版本要求 Beta 版 2.3.5+， 稳定版 2.4.0+）。

创建一个使用JSON类型标签的超级表及其子表：

```python
# encoding:UTF-8
import taos

conn = taos.connect()
conn.execute("create database if not exists py_test_json_type")
conn.execute("use py_test_json_type")

conn.execute("create stable s1 (ts timestamp, v1 int) tags (info json)")
conn.execute("create table s1_1 using s1 tags ('{\"k1\": \"v1\"}')")
```

查询子表标签及表名：

```python
tags = conn.query("select info, tbname from s1").fetch_all_into_dict()
tags
```

`tags` 内容为：

```python
[{'info': '{"k1":"v1"}', 'tbname': 's1_1'}]
```

获取 JSON 中某值：

```python
k1 = conn.query("select info->'k1' as k1 from s1").fetch_all_into_dict()
"""
>>> k1
[{'k1': '"v1"'}]
"""
```

更多 JSON 类型的操作方式请参考 [JSON 类型使用说明](https://www.taosdata.com/cn/documentation/taos-sql)。

#### 关于纳秒 (nanosecond) 在 Python 连接器中的说明

由于目前 Python 对 nanosecond 支持的不完善(参见链接 1. 2. )，目前的实现方式是在 nanosecond 精度时返回整数，而不是 ms 和 us 返回的 datetime 类型，应用开发者需要自行处理，建议使用 pandas 的 to_datetime()。未来如果 Python 正式完整支持了纳秒，涛思数据可能会修改相关接口。

1. `https://stackoverflow.com/questions/10611328/parsing-datetime-strings-containing-nanoseconds`
2. `https://www.python.org/dev/peps/pep-0564/`

#### 帮助信息

用户可通过 Python 的帮助信息直接查看模块的使用信息，或者参考 `examples/python` 目录中的示例程序。以下为部分常用类和方法：

- _TaosConnection_ 类

  参考 Python 中 help(taos.TaosConnection)。
  这个类对应客户端和 TDengine 建立的一个连接。在客户端多线程的场景下，推荐每个线程申请一个独立的连接实例，而不建议多线程共享一个连接。

- _TaosCursor_ 类

  参考 Python 中 help(taos.TaosCursor)。
  这个类对应客户端进行的写入、查询操作。在客户端多线程的场景下，这个游标实例必须保持线程独享，不能跨线程共享使用，否则会导致返回结果出现错误。

- _connect_ 方法

  用于生成 taos.TaosConnection 的实例。

## <a class="anchor" id="restful"></a>RESTful Connector

为支持各种不同类型平台的开发，TDengine 提供符合 REST 设计标准的 API，即 RESTful API。为最大程度降低学习成本，不同于其他数据库 RESTful API 的设计方法，TDengine 直接通过 HTTP POST 请求 BODY 中包含的 SQL 语句来操作数据库，仅需要一个 URL。RESTful 连接器的使用参见[视频教程](https://www.taosdata.com/blog/2020/11/11/1965.html)。

注意：与原生连接器的一个区别是，RESTful 接口是无状态的，因此 `USE db_name` 指令没有效果，所有对表名、超级表名的引用都需要指定数据库名前缀。（从 2.2.0.0 版本开始，支持在 RESTful url 中指定 db_name，这时如果 SQL 语句中没有指定数据库名前缀的话，会使用 url 中指定的这个 db_name。从 2.4.0.0 版本开始，RESTful 默认有 taosAdapter 提供，要求必须在 url 中指定 db_name。）

### 安装

RESTful 接口不依赖于任何 TDengine 的库，因此客户端不需要安装任何 TDengine 的库，只要客户端的开发语言支持 HTTP 协议即可。

### 验证

在已经安装 TDengine 服务器端的情况下，可以按照如下方式进行验证。

下面以 Ubuntu 环境中使用 curl 工具（确认已经安装）来验证 RESTful 接口的正常。

下面示例是列出所有的数据库，请把 h1.taosdata.com 和 6041（缺省值）替换为实际运行的 TDengine 服务 fqdn 和端口号：

```html
curl -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' -d 'show databases;' h1.taosdata.com:6041/rest/sql
```

返回值结果如下表示验证通过：

```json
{
  "status": "succ",
  "head": ["name","created_time","ntables","vgroups","replica","quorum","days","keep1,keep2,keep(D)","cache(MB)","blocks","minrows","maxrows","wallevel","fsync","comp","precision","status"],
  "data": [
    ["log","2020-09-02 17:23:00.039",4,1,1,1,10,"30,30,30",1,3,100,4096,1,3000,2,"us","ready"],
    ],
  "rows": 1
}
```

### RESTful 连接器的使用

#### HTTP 请求格式 

```
http://<fqdn>:<port>/rest/sql/[db_name]
```

参数说明：

- fqnd: 集群中的任一台主机 FQDN 或 IP 地址
- port: 配置文件中 httpPort 配置项，缺省为 6041
- db_name: 可选参数，指定本次所执行的 SQL 语句的默认数据库库名。（从 2.2.0.0 版本开始支持）

例如：`http://h1.taos.com:6041/rest/sql/test` 是指向地址为 `h1.taos.com:6041` 的 url，并将默认使用的数据库库名设置为 test。

HTTP 请求的 Header 里需带有身份认证信息，TDengine 支持 Basic 认证与自定义认证两种机制，后续版本将提供标准安全的数字签名机制来做身份验证。

- 自定义身份认证信息如下所示（<token>稍后介绍）

```
Authorization: Taosd <TOKEN>
```

- Basic身份认证信息如下所示

```
Authorization: Basic <TOKEN>
```

HTTP 请求的 BODY 里就是一个完整的 SQL 语句，SQL 语句中的数据表应提供数据库前缀，例如 \<db_name>.\<tb_name>。如果表名不带数据库前缀，又没有在 url 中指定数据库名的话，系统会返回错误。因为 HTTP 模块只是一个简单的转发，没有当前 DB 的概念。 

使用 curl 通过自定义身份认证方式来发起一个 HTTP Request，语法如下：

```bash
curl -H 'Authorization: Basic <TOKEN>' -d '<SQL>' <ip>:<PORT>/rest/sql/[db_name]
```

或者

```bash
curl -u username:password -d '<SQL>' <ip>:<PORT>/rest/sql/[db_name]
```

其中，`TOKEN` 为 `{username}:{password}` 经过 Base64 编码之后的字符串，例如 `root:taosdata` 编码后为 `cm9vdDp0YW9zZGF0YQ==`

### HTTP 返回格式

返回值为 JSON 格式，如下:

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

说明：

- status: 告知操作结果是成功还是失败。
- head: 表的定义，如果不返回结果集，则仅有一列 “affected_rows”。（从 2.0.17.0 版本开始，建议不要依赖 head 返回值来判断数据列类型，而推荐使用 column_meta。在未来版本中，有可能会从返回值中去掉 head 这一项。）
- column_meta: 从 2.0.17.0 版本开始，返回值中增加这一项来说明 data 里每一列的数据类型。具体每个列会用三个值来说明，分别为：列名、列类型、类型长度。例如`["current",6,4]`表示列名为“current”；列类型为 6，也即 float 类型；类型长度为 4，也即对应 4 个字节表示的 float。如果列类型为 binary 或 nchar，则类型长度表示该列最多可以保存的内容长度，而不是本次返回值中的具体数据长度。当列类型是 nchar 的时候，其类型长度表示可以保存的 unicode 字符数量，而不是 bytes。
- data: 具体返回的数据，一行一行的呈现，如果不返回结果集，那么就仅有 [[affected_rows]]。data 中每一行的数据列顺序，与 column_meta 中描述数据列的顺序完全一致。
- rows: 表明总共多少行数据。

<a class="anchor" id="column_meta"></a>column_meta 中的列类型说明：

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

### 自定义授权码

HTTP 请求中需要带有授权码 `<TOKEN>`，用于身份识别。授权码通常由管理员提供，可简单的通过发送 `HTTP GET` 请求来获取授权码，操作如下：

```bash
curl http://<fqnd>:<port>/rest/login/<username>/<password>
```

其中，`fqdn` 是 TDengine 数据库的 fqdn 或 ip 地址，port 是 TDengine 服务的端口号，`username` 为数据库用户名，`password` 为数据库密码，返回值为 `JSON` 格式，各字段含义如下：

- status：请求结果的标志位

- code：返回值代码

- desc：授权码

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

- 在 demo 库里查询表 d1001 的所有记录： 

```bash
curl -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' -d 'select * from demo.d1001' 192.168.0.1:6041/rest/sql
```

返回值：

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

- 创建库 demo：

```bash
curl -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' -d 'create database demo' 192.168.0.1:6041/rest/sql
```

返回值：

```json
{
    "status": "succ",
    "head": ["affected_rows"],
    "column_meta": [["affected_rows",4,4]],
    "data": [[1]],
    "rows": 1
}
```

### 其他用法

#### 结果集采用 Unix 时间戳

HTTP 请求 URL 采用 `sqlt` 时，返回结果集的时间戳将采用 Unix 时间戳格式表示，例如

```bash
curl -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' -d 'select * from demo.d1001' 192.168.0.1:6041/rest/sqlt
```

返回值：

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

#### 结果集采用 UTC 时间字符串

HTTP 请求 URL 采用 `sqlutc` 时，返回结果集的时间戳将采用 UTC 时间字符串表示，例如

```bash
  curl -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' -d 'select * from demo.t1' 192.168.0.1:6041/rest/sqlutc
```

返回值：

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

### 重要配置项

下面仅列出一些与 RESTful 接口有关的配置参数，其他系统参数请看配置文件里的说明。（注意：配置修改后，需要重启 taosd 服务才能生效）

- 对外提供 RESTful 服务的端口号，默认绑定到 6041（实际取值是 serverPort + 11，因此可以通过修改 serverPort 参数的设置来修改）。
- httpMaxThreads: 启动的线程数量，默认为 2（2.0.17.0 版本开始，默认值改为 CPU 核数的一半向下取整）。
- restfulRowLimit: 返回结果集（JSON 格式）的最大条数，默认值为 10240。
- httpEnableCompress: 是否支持压缩，默认不支持，目前 TDengine 仅支持 gzip 压缩格式。
- httpDebugFlag: 日志开关，默认 131。131：仅错误和报警信息，135：调试信息，143：非常详细的调试信息，默认 131。
- httpDbNameMandatory: 是否必须在 RESTful url 中指定默认的数据库名。默认为 0，即关闭此检查。如果设置为 1，那么每个 RESTful url 中都必须设置一个默认数据库名，否则无论此时执行的 SQL 语句是否需要指定数据库，都会返回一个执行错误，拒绝执行此 SQL 语句。

## <a class="anchor" id="csharp"></a>CSharp Connector

* C# 连接器支持的系统有：Linux 64/Windows x64/Windows x86

* C# 连接器现在也支持从[Nuget下载引用](https://www.nuget.org/packages/TDengine.Connector/)

* 在 Windows 系统上，C# 应用程序可以使用 TDengine 的原生 C 接口来执行所有数据库操作，后续版本将提供 ORM（Dapper）框架驱动。

### 安装准备

* 应用驱动安装请参考[安装连接器驱动步骤](https://www.taosdata.com/cn/documentation/connector#driver)。
* 接口文件 TDengineDrivercs.cs 和参考程序示例 TDengineTest.cs 均位于 Windows 客户端 install_directory/examples/C#目录下。
* 安装[.NET SDK](https://dotnet.microsoft.com/download)

### 示例程序

示例程序源码位于

* {client_install_directory}/examples/C#
* [C# example source code on GitHub](https://github.com/taosdata/TDengine/tree/develop/examples/C%2523)

**注意:** TDengineTest.cs       C# 示例源程序，包含了数据库连接参数，以及如何执行数据插入、查询等操作。

### 安装验证

需要先安装 .NET SDK

```cmd
cd {client_install_directory}/examples/C#/C#Checker
//运行测试
dotnet run -- -h <FQDN>. // 此步骤会先build，然后再运行。
```

### C#连接器的使用

在 Windows 系统上，C# 应用程序可以使用 TDengine 的 C# 连接器接口来执行所有数据库的操作。使用的具体步骤如下所示：

* 创建一个 C# project（需要 .NET SDK）. 

``` cmd
mkdir test
cd test 
dotnet new console
```

* 通过 Nuget 引用 TDengineDriver 包

``` cmd
dotnet add package TDengine.Connector
```

* 在项目中需要用到 TDengineConnector 的地方引用 TDengineDriver namespace。

```C# 
using TDengineDriver;
```

* 用户可以参考[TDengineTest.cs](https://github.com/taosdata/TDengine/tree/develop/examples/C%2523/TDengineTest)来定义数据库连接参数，以及如何执行数据插入、查询等操作。

**注意：**

* TDengine V2.0.3.0 之后同时支持 32 位和 64 位 Windows 系统，所以 C# 项目在生成 .exe 文件时，“解决方案”/“项目”的“平台”请选择对应的 x86 或 x64。
* 此接口目前已经在 Visual Studio 2015/2017 中验证过，其它 Visual Studio 版本尚待验证。
* 此连接器需要用到 taos.dll 文件，所以在未安装客户端时需要在执行应用程序前，拷贝 Windows{client_install_directory}/driver 目录中的 taos.dll 文件到项目最后生成 .exe 可执行文件所在的文件夹。之后运行 exe 文件，即可访问 TDengine 数据库并做插入、查询等操作。

### 第三方驱动

Maikebing.Data.Taos 是一个 TDengine 的 ADO.NET 提供器，支持 Linux，Windows。该开发包由热心贡献者`麦壳饼@@maikebing`提供，具体请参考

```
//接口下载
https://github.com/maikebing/Maikebing.EntityFrameworkCore.Taos   
//用法说明    
https://www.taosdata.com/blog/2020/11/02/1901.html                    
```

## <a class="anchor" id="go"></a>Go Connector

### 安装准备

Go 连接器支持的系统有：

| **CPU类型** | x64（64bit） |   |   | aarch64 | aarch32 |
| --------------- | ------------ | -------- | -------- | -------- | ---------- |
| **OS类型**  | Linux        | Win64    | Win32    | Linux    | Linux      |
| **支持与否**    | **支持**     | **支持** | **支持** | **支持** | **开发中** |

安装前准备：

- 已安装好 TDengine 应用驱动，参考[安装连接器驱动步骤](https://www.taosdata.com/cn/documentation/connector#driver)。

### 示例程序

使用 Go 连接器的示例代码请参考 `https://github.com/taosdata/TDengine/tree/develop/examples/go` 以及[视频教程](https://www.taosdata.com/blog/2020/11/11/1951.html)。

示例程序源码也位于安装目录下的 examples/go/taosdemo.go 文件中。

**提示：建议 Go 版本是 1.13 及以上，并开启模块支持：**

```sh
go env -w GO111MODULE=on
go env -w GOPROXY=https://goproxy.io,direct
```

在 taosdemo.go 所在目录下进行编译和执行：

```sh
go mod init taosdemo
go get github.com/taosdata/driver-go/taosSql
# use win branch in Windows platform.
#go get github.com/taosdata/driver-go/taosSql@win
go build
./taosdemo -h fqdn -p serverPort
```

### Go 连接器的使用

TDengine 提供了GO驱动程序包`taosSql`。`taosSql` 实现了 GO 语言的内置接口 `database/sql/driver`。用户只需按如下方式引入包就可以在应用程序中访问 TDengine。

```go
import (
  "database/sql"
  _ "github.com/taosdata/driver-go/v2/taosSql"
)
```

**提示**：下划线与双引号之间必须有一个空格。

`taosSql` 的 v2 版本进行了重构，分离出内置数据库操作接口 `database/sql/driver` 到目录 `taosSql`；订阅、 stmt 等其他功能放到目录 `af`。

### 常用 API

- `sql.Open(DRIVER_NAME string, dataSourceName string) *DB` 

  该 API 用来打开 DB，返回一个类型为\*DB的对象，一般情况下，DRIVER_NAME设置为字符串 `taosSql`，dataSourceName 设置为字符串 `user:password@/tcp(host:port)/dbname`，如果客户想要用多个 goroutine 并发访问 TDengine, 那么需要在各个 goroutine 中分别创建一个 sql.Open 对象并用之访问 TDengine。

  **注意**： 该 API 成功创建的时候，并没有做权限等检查，只有在真正执行 Query 或者 Exec 的时候才能真正的去创建连接，并同时检查 user/password/host/port 是不是合法。另外，由于整个驱动程序大部分实现都下沉到 taosSql 所依赖的 libtaos 动态库中。所以，sql.Open 本身特别轻量。

-  `func (db *DB) Exec(query string, args ...interface{}) (Result, error)`

  sql.Open 内置的方法，用来执行非查询相关 SQL

-  `func (db *DB) Query(query string, args ...interface{}) (*Rows, error)`

  sql.Open 内置的方法，用来执行查询语句

- `func (db *DB) Prepare(query string) (*Stmt, error)`

  sql.Open 内置的方法，Prepare creates a prepared statement for later queries or executions.

- `func (s *Stmt) Exec(args ...interface{}) (Result, error)`

  sql.Open 内置的方法，executes a prepared statement with the given arguments and returns a Result summarizing the effect of the statement.

- `func (s *Stmt) Query(args ...interface{}) (*Rows, error)`

  sql.Open 内置的方法，Query executes a prepared query statement with the given arguments and returns the query results as a \*Rows.

- `func (s *Stmt) Close() error`

  sql.Open 内置的方法，Close closes the statement.	

### 其他代码示例

[Consume Messages from Kafka](https://github.com/taosdata/go-demo-kafka) 是一个通过 Go 语言实现消费 Kafka 队列写入 TDengine 的示例程序，也可以作为通过 Go 连接 TDengine 的写法参考。

### Go RESTful的使用

#### 引入

```go restful
import (
  "database/sql"
  _ "github.com/taosdata/driver-go/v2/taosRestful"
)
```

`go.mod ` 的文件 require 块使用 github.com/taosdata/driver-go/v2 develop 之后执行 `go mod tidy `
  
`sql.Open `的driverName 为 `taosRestful`

#### DSN

格式为：

数据库用户名:数据库密码@连接方式(域名或ip:端口)/[数据库][?参数]

样例：

`root:taosdata@http(localhost:6041)/test?readBufferSize=52428800`
  
参数：

`disableCompression` 是否接受压缩数据，默认为 true 不接受压缩数据，如果传输数据使用 gzip 压缩设置为 false。

`readBufferSize` 读取数据的缓存区大小默认为 4K(4096)，当查询结果数据量多时可以适当调大该值。

#### 使用限制

由于 RESTful 接口无状态所以 `use db` 语法不会生效，需要将 db 名称放到 SQL 语句中，如：`create table if not exists tb1 (ts timestamp, a int)`改为`create table if not exists test.tb1 (ts timestamp, a int)`否则将报错`[0x217] Database not specified or available`。

也可以将 db 名称放到 DSN 中，将 `root:taosdata@http(localhost:6041)/` 改为 `root:taosdata@http(localhost:6041)/test`，此方法在 TDengine 2.4.0.5 版本的 taosAdapter 开始支持。当指定的 db 不存在时执行 `create database` 语句不会报错，而执行针对该 db 的其他查询或写入操作会报错。完整示例如下：

```go restful demo
package main
 
import (
    "database/sql"
    "fmt"
    "time"
 
    _ "github.com/taosdata/driver-go/v2/taosRestful"
)
 
func main() {
    var taosDSN = "root:taosdata@http(localhost:6041)/test"
    taos, err := sql.Open("taosRestful", taosDSN)
    if err != nil {
        fmt.Println("failed to connect TDengine, err:", err)
        return
    }
    defer taos.Close()
    taos.Exec("create database if not exists test")
    taos.Exec("create table if not exists tb1 (ts timestamp, a int)")
    _, err = taos.Exec("insert into tb1 values(now, 0)(now+1s,1)(now+2s,2)(now+3s,3)")
    if err != nil {
        fmt.Println("failed to insert, err:", err)
        return
    }
    rows, err := taos.Query("select * from tb1")
    if err != nil {
        fmt.Println("failed to select from table, err:", err)
        return
    }
 
    defer rows.Close()
    for rows.Next() {
        var r struct {
            ts time.Time
            a  int
        }
        err := rows.Scan(&r.ts, &r.a)
        if err != nil {
            fmt.Println("scan error:\n", err)
            return
        }
        fmt.Println(r.ts, r.a)
    }
}
```

#### 常见问题

- 无法找到包`github.com/taosdata/driver-go/v2/taosRestful`
  
  将 `go.mod` 中 require 块对`github.com/taosdata/driver-go/v2`的引用改为`github.com/taosdata/driver-go/v2 develop`，之后执行 `go mod tidy`。

- stmt 相关接口崩溃

  RESTful 不支持 stmt 相关接口，建议使用`db.Exec`和`db.Query`。
  
- 使用 `use db` 语句后执行其他语句报错 `[0x217] Database not specified or available`
  
  在 RESTful 接口中 SQL 语句的执行无上下文关联，使用 `use db` 语句不会生效，解决办法见上方使用限制章节。
  
- 使用 taosSql 不报错使用 taosRestful 报错 `[0x217] Database not specified or available`
  
   因为 RESTful 接口无状态，使用 `use db` 语句不会生效，解决办法见上方使用限制章节。
  
- 升级 `github.com/taosdata/driver-go/v2/taosRestful`
  
  将 `go.mod` 文件中对 `github.com/taosdata/driver-go/v2` 的引用改为 `github.com/taosdata/driver-go/v2 develop`，之后执行 `go mod tidy`。
  
- readBufferSize 参数调大后无明显效果
  
  readBufferSize 调大后会减少获取结果时 syscall 的调用。如果查询结果的数据量不大，修改该参数不会带来明显提升，如果该参数修改过大，瓶颈会在解析 JSON 数据。如果需要优化查询速度，需要根据实际情况调整该值来达到查询效果最优。
  
- disableCompression 参数设置为 false 时查询效率降低
  
  当 disableCompression 参数设置为 false 时查询结果会 gzip 压缩后传输，拿到数据后要先进行 gzip 解压。

## <a class="anchor" id="nodejs"></a> Node.js Connector

Node.js 连接器支持的系统有：

|**CPU类型**  | x64（64bit） |          |          | aarch64  | aarch32  |
| ------------ | ------------ | -------- | -------- | -------- | -------- |
| **OS类型**   | Linux        | Win64    | Win32    | Linux    | Linux    |
| **支持与否** | **支持**     | **支持** | **支持** | **支持** | **支持** |

Node.js 连接器的使用参见[视频教程](https://www.taosdata.com/blog/2020/11/11/1957.html)。

### 安装准备

* 应用驱动安装请参考[安装连接器驱动步骤](https://www.taosdata.com/cn/documentation/connector#driver)。

### 安装Node.js连接器

用户可以通过 [npm](https://www.npmjs.com/) 来进行安装，也可以通过源代码 *src/connector/nodejs/* 来进行安装。具体安装步骤如下：

首先，通过 [npm](https://www.npmjs.com/) 安装 Node.js 连接器。

```bash
npm install td2.0-connector
```

我们建议用户使用 npm 安装 Node.js 连接器。如果您没有安装 npm，可以将 *src/connector/nodejs/* 拷贝到您的 Node.js 项目目录下。

我们使用 [node-gyp](https://github.com/nodejs/node-gyp) 和 TDengine 服务端进行交互。安装 Node.js 连接器之前，还需要根据具体操作系统来安装下文提到的一些依赖工具。

### Linux

- `python` (建议`v2.7` , `v3.x.x` 目前还不支持)
- `node`  2.0.6支持v12.x和v10.x，2.0.5及更早版本支持v10.x版本，其他版本可能存在包兼容性的问题。
- `make`
- C 语言编译器比如 [GCC](https://gcc.gnu.org)

### Windows

#### 安装方法1

使用微软的 [windows-build-tools](https://github.com/felixrieseberg/windows-build-tools) 在 `cmd` 命令行界面执行 `npm install --global --production windows-build-tools` 即可安装所有的必备工具。

#### 安装方法2

手动安装以下工具：

- 安装 Visual Studio 相关：[Visual Studio Build 工具](https://visualstudio.microsoft.com/thank-you-downloading-visual-studio/?sku=BuildTools) 或者 [Visual Studio 2017 Community](https://visualstudio.microsoft.com/pl/thank-you-downloading-visual-studio/?sku=Community)
- 安装 [Python](https://www.python.org/downloads/) 2.7(`v3.x.x` 暂不支持) 并执行 `npm config set python python2.7`
- 进入`cmd`命令行界面，`npm config set msvs_version 2017`

如果以上步骤不能成功执行，可以参考微软的 Node.js 用户手册 [Microsoft's Node.js Guidelines for Windows](https://github.com/Microsoft/nodejs-guidelines/blob/master/windows-environment.md#compiling-native-addon-modules)。

如果在 Windows 10 ARM 上使用 ARM64 Node.js，还需添加 "Visual C++ compilers and libraries for ARM64" 和 "Visual C++ ATL for ARM64"。

### 示例程序

示例程序源码位于 install_directory/examples/nodejs，有：

Node-example.js       		Node.js示例源程序
Node-example-raw.js

### 安装验证

在安装好 TDengine 客户端后，使用 nodejsChecker.js 程序能够验证当前环境是否支持 Node.js 方式访问 TDengine。

验证方法：

1. 新建安装验证目录，例如：`~/tdengine-test`，拷贝 GitHub 上 nodejsChecker.js 源程序。下载地址：`https://github.com/taosdata/TDengine/tree/develop/examples/nodejs/nodejsChecker.js`。

2. 在命令行中执行以下命令：

```bash
npm init -y
npm install td2.0-connector
node nodejsChecker.js host=localhost
```

3. 执行以上步骤后，在命令行会输出 Node.js 连接 TDengine 实例，并执行简答插入和查询的结果。

### Node.js连接器的使用

以下是 Node.js 连接器的一些基本使用方法，详细的使用方法可参考[TDengine Node.js connector](https://github.com/taosdata/TDengine/tree/develop/src/connector/nodejs)。

#### 建立连接

使用 Node.js 连接器时，必须先 `require td2.0-connector`，然后使用 `taos.connect` 函数建立到服务端的连接。例如如下代码：

```javascript
const taos = require('td2.0-connector');
var conn = taos.connect({host:"taosdemo.com", user:"root", password:"taosdata", config:"/etc/taos",port:6030})
var cursor = conn.cursor(); // Initializing a new cursor
```

建立了一个到 hostname 为 taosdemo.com，端口为 6030（TDengine的默认端口号）的连接。连接指定了用户名（root）和密码（taosdata）。taos.connect 函数必须提供的参数是`host`，其它参数在没有提供的情况下会使用如下的默认值。taos.connect 返回了 `cursor` 对象，使用 cursor 来执行 SQL 语句。

#### 执行 SQL 和插入数据

对于 DDL 语句（例如create database、create table、use等），可以使用 cursor 的 execute 方法。代码如下：

```js
cursor.execute('create database if not exists test;')
```

以上代码创建了一个名称为 `test` 的数据库。对于 DDL 语句，一般没有返回值，cursor 的 execute 返回值为 0。

对于 Insert 语句，代码如下：

```js
var affectRows = cursor.execute('insert into test.weather values(now, 22.3, 34);')
```

execute 方法的返回值为该语句影响的行数，上面的 SQL 向 test 库的 weather 表中，插入了一条数据，则返回值 affectRows 为 1。

TDengine 目前还不支持 delete 语句。但从 2.0.8.0 版本开始，可以通过 `CREATE DATABASE` 时指定的 UPDATE 参数来启用对数据行的 update。

#### 查询

可通过 `cursor.query` 函数来查询数据库。

```javascript
var query = cursor.query('show databases;')
```

查询的结果可以通过 `query.execute()` 函数获取并打印出来。

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

#### 关闭连接

在完成插入、查询等操作后，要关闭连接。代码如下：

```js
conn.close();
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

[node-example.js](https://github.com/taosdata/TDengine/blob/master/examples/nodejs/node-example.js) 提供了一个使用 Node.js 连接器建表，插入天气数据并查询插入的数据的代码示例。

[node-example-raw.js](https://github.com/taosdata/TDengine/blob/master/examples/nodejs/node-example-raw.js) 同样是一个使用 Node.js 连接器建表，插入天气数据并查询插入的数据的代码示例，但和上面不同的是，该示例只使用 `cursor`。
