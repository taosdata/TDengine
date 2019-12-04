#系统管理

## 文件目录结构

安装TDengine后，默认会在操作系统中生成下列目录或文件：

| 目录/文件               | 说明                                              |
| ---------------------- | :------------------------------------------------|
| /etc/taos/taos.cfg     | TDengine默认[配置文件]                            |
| /usr/local/taos/driver | TDengine动态链接库目录                            |
| /var/lib/taos          | TDengine默认数据文件目录,可通过[配置文件]修改位置.    |
| /var/log/taos          | TDengine默认日志文件目录,可通过[配置文件]修改位置     |
| /usr/local/taos/bin    | TDengine可执行文件目录                            |

### 可执行文件

TDengine的所有可执行文件默认存放在 _/usr/local/taos/bin_ 目录下。其中包括：

- _taosd_：TDengine服务端可执行文件
- _taos_： TDengine Shell可执行文件
- _taosdump_：数据导出工具
- *rmtaos*： 一个卸载TDengine的脚本, 请谨慎执行

您可以通过修改系统配置文件taos.cfg来配置不同的数据目录和日志目录

## 服务端配置

TDengine系统后台服务由taosd提供，可以在配置文件taos.cfg里修改配置参数，以满足不同场景的需求。配置文件的缺省位置在/etc/taos目录，可以通过taosd命令行执行参数-c指定配置文件目录。比如taosd -c /home/user来指定配置文件位于/home/user这个目录。

下面仅仅列出一些重要的配置参数，更多的参数请看配置文件里的说明。各个参数的详细介绍及作用请看前述章节。**注意：配置修改后，需要重启*taosd*服务才能生效。**

- internalIp: 对外提供服务的IP地址，默认取第一个IP地址
- mgmtShellPort：管理节点与客户端通信使用的TCP/UDP端口号（默认值是6030）。此端口号在内向后连续的5个端口都会被UDP通信占用，即UDP占用[6030-6034]，同时TCP通信也会使用端口[6030]。
- vnodeShellPort：数据节点与客户端通信使用的TCP/UDP端口号（默认值是6035）。此端口号在内向后连续的5个端口都会被UDP通信占用，即UDP占用[6035-6039]，同时TCP通信也会使用端口[6035]
- httpPort：数据节点对外提供RESTful服务使用TCP，端口号[6020]
- dataDir: 数据文件目录，缺省是/var/lib/taos
- maxUsers：用户的最大数量
- maxDbs：数据库的最大数量
- maxTables：数据表的最大数量
- enableMonitor: 系统监测标志位，0：关闭，1：打开
- logDir: 日志文件目录，缺省是/var/log/taos
- numOfLogLines：日志文件的最大行数
- debugFlag: 系统debug日志开关，131：仅错误和报警信息，135：所有

不同应用场景的数据往往具有不同的数据特征，比如保留天数、副本数、采集频次、记录大小、采集点的数量、压缩等都可完全不同。为获得在存储上的最高效率，TDengine提供如下存储相关的系统配置参数：

- days：一个数据文件覆盖的时间长度，单位为天
- keep：数据库中数据保留的天数
- rows: 文件块中记录条数
- comp: 文件压缩标志位，0：关闭，1:一阶段压缩，2:两阶段压缩
- ctime：数据从写入内存到写入硬盘的最长时间间隔，单位为秒
- clog：数据提交日志(WAL)的标志位，0为关闭，1为打开
- tables：每个vnode允许创建表的最大数目
- cache: 内存块的大小（字节数）
- tblocks: 每张表最大的内存块数
- ablocks: 每张表平均的内存块数
- precision：时间戳为微秒的标志位，ms表示毫秒，us表示微秒

对于一个应用场景，可能有多种数据特征的数据并存，最佳的设计是将具有相同数据特征的表放在一个库里，这样一个应用有多个库，而每个库可以配置不同的存储参数，从而保证系统有最优的性能。TDengine容许应用在创建库时指定上述存储参数，如果指定，该参数就将覆盖对应的系统配置参数。举例，有下述SQL： 

```
 create database demo days 10 cache 16000 ablocks 4
```

该SQL创建了一个库demo, 每个数据文件保留10天数据，内存块为16000字节，每个表平均占用4个内存块，而其他参数与系统配置完全一致。

## 客户端配置 

TDengine系统的前台交互客户端应用程序为taos（Windows平台上为taos.exe），可以使用taos.cfg来配置启动和运行配置项。可以使用 `taos -?` 来获取全部的可选项。
启动的时候如果不指定taos加载配置文件路径，默认读取`/etc/taos/`路径下的`taos.cfg`文件来设置启动选项，如果在默认路径下找不到配置文件，则使用默认配置的设置来启动程序，并且会在启动的时候打印一行告警信息。指定配置文件来启动`taos`的命令如下：
```
taos -c /home/cfg/
```
**注意：启动设置的是配置文件所在目录，而不是配置文件本身**

如果`/home/cfg/`目录下没有配置文件，程序会继续启动并打印如下告警信息：
```
Welcome to the TDengine shell from linux, client version:1.6.4.0
option file:/home/cfg/taos.cfg not found, all options are set to system default
```
更多taos的使用方法请见[Shell命令行程序](#_TDengine_Shell命令行程序)。本节主要讲解taos客户端应用在配置文件taos.cfg文件中使用到的参数。

客户端配置参数说明

**masterIP**
客户端默认发起请求的服务器的IP地址

**locale**
- 默认值：系统中动态获取，如果自动获取失败，需要用户在配置文件设置或通过API设置
- 是否必须设置：否

TDengine为存储中文、日文、韩文等非ASCII编码的宽字符，提供一种专门的字段类型`nchar`。写入`nchar`字段的数据将统一采用`UCS4-LE`格式进行编码并发送到服务器。需要注意的是，**编码正确性**是客户端来保证。因此，如果用户想要正常使用`nchar`字段来存储诸如中文、日文、韩文等非ASCII字符，需要正确设置客户端的编码格式。

客户端的输入的字符均采用操作系统当前默认的编码格式，在Linux系统上多为`UTF-8`，部分中文系统编码则可能是`GB18030`或`GBK`等。在docker环境中默认的编码是`POSIX`。在中文版Windows系统中，编码则是`CP936`。客户端需要确保正确设置自己所使用的字符集，即客户端运行的操作系统当前编码字符集，才能保证`nchar`中的数据正确转换为`UCS4-LE`编码格式。

在 Linux 中 locale 的命名规则为：
`<语言>_<地区>.<字符集编码>`
如：`zh_CN.UTF-8`，zh代表中文，CN代表大陆地区，UTF-8表示字符集。字符集编码为客户端正确解析本地字符串提供编码转换的说明。Linux系统与Mac OSX系统可以通过设置locale来确定系统的字符编码，由于Windows使用的locale中不是POSIX标准的locale格式，因此在Windows下需要采用另一个配置参数`charset`来指定字符编码。在Linux系统中也可以使用charset来指定字符编码。

**charset**
- 默认值：系统中动态获取，如果自动获取失败，需要用户在配置文件设置或通过API设置
- 是否必须设置：否

如果配置文件中不设置`charset`，在Linux系统中，taos在启动时候，自动读取系统当前的locale信息，并从locale信息中解析提取charset编码格式。如果自动读取locale信息失败，则尝试读取charset配置，如果读取charset配置也失败，**则中断启动过程**。

在Linux系统中，locale信息包含了字符编码信息，因此正确设置了Linux系统locale以后可以不用再单独设置charset。例如：
```
locale zh_CN.UTF-8
```
在Windows系统中，无法从locale获取系统当前编码。如果无法从配置文件中读取字符串编码信息，`taos`默认设置为字符编码为`CP936`。其等效在配置文件中添加如下配置：
```
charset CP936
```
如果需要调整字符编码，请查阅当前操作系统使用的编码，并在配置文件中正确设置。

在Linux系统中，如果用户同时设置了locale和字符集编码charset，并且locale和charset的不一致，后设置的值将覆盖前面设置的值。
```
locale zh_CN.UTF-8
charset GBK
```
则`charset`的有效值是`GBK`。
```
charset GBK
locale zh_CN.UTF-8
```
`charset`的有效值是`UTF-8`。

**sockettype**
- 默认值：UDP
- 是否必须设置：否

客户端连接服务端的套接字的方式，可以使用`UDP`和`TCP`两种配置。
在客户端和服务端之间的通讯需要经过恶劣的网络环境下（如公共网络、互联网）、客户端与数据库服务端连接不稳定（由于MTU的问题导致UDP丢包）的情况下，可以将连接的套接字类型调整为`TCP`

>注意：客户端套接字的类型需要和服务端的套接字类型相同，否则无法连接数据库。

**compressMsgSize**
- 默认值：-1
- 是否必须设置：否

客户端与服务器之间进行消息通讯过程中，对通讯的消息进行压缩的阈值，默认值为-1（不压缩）。如果要压缩消息，建议设置为64330字节，即大于64330字节的消息体才进行压缩。在配置文件中增加如下配置项即可：
```
compressMsgSize 64330
```
如果配置项设置为0，`compressMsgSize 0`表示对所有的消息均进行压缩。

**timezone**
- 默认值：从系统中动态获取当前的时区设置
- 是否必须设置：否

客户端运行系统所在的时区。为应对多时区的数据写入和查询问题，TDengine采用Unix时间戳([Unix Timestamp](https://en.wikipedia.org/wiki/Unix_time))来记录和存储时间戳。Unix时间戳的特点决定了任一时刻不论在任何时区，产生的时间戳均一致。需要注意的是，Unix时间戳是在客户端完成转换和记录。为了确保客户端其他形式的时间（）转换为正确的Unix时间戳，需要设置正确的时区。

在Linux系统中，客户端会自动读取系统设置的时区信息。用户也可以采用多种方式在配置文件设置时区。例如：
```
timezone UTC-8
timezone GMT-8
timezone Asia/Shanghai
```
均是合法的设置东八区时区的格式。


时区的设置对于查询和写入SQL语句中非Unix时间戳的内容（时间戳字符串、关键词`now`的解析）产生影响。例如：
```
SELECT count(*) FROM table_name WHERE TS<'2019-04-11 12:01:08';
```
在东八区转换的SQL语句等效于
```
SELECT count(*) FROM table_name WHERE TS<1554955268000;
```
在UTC时区，SQL语句等效于
```
SELECT count(*) FROM table_name WHERE TS<1554984068000;
```
为了避免使用字符串时间格式带来的不确定性，也可以直接使用Unix时间戳。此外，还可以在SQL语句中使用带有时区的时间戳字符串，例如：RFC3339格式的时间戳字符串，`2013-04-12T15:52:01.123+08:00`或者ISO-8601格式时间戳字符串`2013-04-12T15:52:01.123+0800`。上述两个字符串转化为Unix时间戳不受系统所在时区的影响。

**defaultUser**
默认登录用户，默认值root

**defaultPass**
默认登录密码，默认值taosdata

TCP/UDP端口，以及日志的配置参数，与server的配置参数完全一样。

启动taos时，你也可以从命令行指定IP地址、端口号，用户名和密码，否则就从taos.cfg读取。


## 用户管理

系统管理员可以在CLI界面里添加、删除用户，也可以修改密码。CLI里SQL语法如下：

```
CREATE USER user_name PASS ‘password’
```

创建用户，并制定用户名和密码，密码需要用单引号引起来

```
DROP USER user_name
```

删除用户，限root用户使用

```
ALTER USER user_name PASS ‘password’  
```

修改用户密码, 为避免被转换为小写，密码需要用单引号引用

```
SHOW USERS
```

显示所有用户

## 数据导入

TDengine提供两种方便的数据导入功能，一种按脚本文件导入，一种按数据文件导入。

**按脚本文件导入**

TDengine的shell支持source filename命令，用于批量运行文件中的SQL语句。用户可将建库、建表、写数据等SQL命令写在同一个文件中，每条命令单独一行，在shell中运行source命令，即可按顺序批量运行文件中的SQL语句。以‘#’开头的SQL语句被认为是注释，shell将自动忽略。

**按数据文件导入**

TDengine也支持在shell对已存在的表从CSV文件中进行数据导入。每个CSV文件只属于一张表且CSV文件中的数据格式需与要导入表的结构相同。其语法如下

```mysql
insert into tb1 file a.csv b.csv tb2 c.csv …
import into tb1 file a.csv b.csv tb2 c.csv …
```

## 数据导出

为方便数据导出，TDengine提供了两种导出方式，分别是按表导出和用taosdump导出。

**按表导出CSV文件**

如果用户需要导出一个表或一个STable中的数据，可在shell中运行

```
select * from <tb_name> >> a.csv
```

这样，表tb中的数据就会按照CSV格式导出到文件a.csv中。

**用taosdump导出数据**

TDengine提供了方便的数据库导出工具taosdump。用户可以根据需要选择导出所有数据库、一个数据库或者数据库中的一张表,所有数据或一时间段的数据，甚至仅仅表的定义。其用法如下：

- 导出数据库中的一张或多张表：taosdump [OPTION…] dbname tbname …
- 导出一个或多个数据库：      taosdump [OPTION…] --databases dbname…
- 导出所有数据库（不含监控数据库）：taosdump [OPTION…] --all-databases

用户可通过运行taosdump --help获得更详细的用法说明

## 系统连接、任务查询管理

系统管理员可以从CLI查询系统的连接、正在进行的查询、流式计算，并且可以关闭连接、停止正在进行的查询和流式计算。CLI里SQL语法如下：

```
SHOW CONNECTIONS
```

显示数据库的连接，其中一列显示ip:port, 为连接的IP地址和端口号。

```
KILL CONNECTION <connection-id>
```

强制关闭数据库连接，其中的connection-id是SHOW CONNECTIONS中显示的 ip:port字串，如“192.168.0.1:42198”，拷贝粘贴即可。

```
SHOW QUERIES
```

显示数据查询，其中一列显示ip:port:id, 为发起该query应用的IP地址，端口号，以及系统分配的ID。

```
KILL QUERY <query-id>
```

强制关闭数据查询，其中query-id是SHOW QUERIES中显示的ip:port:id字串，如“192.168.0.1:42198:11”，拷贝粘贴即可。

```
SHOW STREAMS
```

显示流式计算，其中一列显示ip:port:id, 为启动该stream的IP地址、端口和系统分配的ID。

```
KILL STREAM <stream-id>
```

强制关闭流式计算，其中的中stream-id是SHOW STREAMS中显示的ip:port:id字串，如“192.168.0.1:42198:18”，拷贝粘贴即可。

## 系统监控

TDengine启动后，会自动创建一个监测数据库SYS，并自动将服务器的CPU、内存、硬盘空间、带宽、请求数、磁盘读写速度、慢查询等信息定时写入该数据库。TDengine还将重要的系统操作（比如登录、创建、删除数据库等）日志以及各种错误报警信息记录下来存放在SYS库里。系统管理员可以从CLI直接查看这个数据库，也可以在WEB通过图形化界面查看这些监测信息。

这些监测信息的采集缺省是打开的，但可以修改配置文件里的选项`monitor`将其关闭或打开。
