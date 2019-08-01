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

TDengine系统的前台交互客户端应用程序为taos，它与taosd共享同一个配置文件taos.cfg。运行taos时，使用参数-c指定配置文件目录，如taos -c /home/cfg，表示使用/home/cfg/目录下的taos.cfg配置文件中的参数，缺省目录是/etc/taos。更多taos的使用方法请见[Shell命令行程序](#_TDengine_Shell命令行程序)。本节主要讲解taos客户端应用在配置文件taos.cfg文件中使用到的参数。

客户端配置参数列表及解释

- masterIP：客户端默认发起请求的服务器的IP地址
- charset：指明客户端所使用的字符集，默认值为UTF-8。TDengine存储nchar类型数据时使用的是unicode存储，因此客户端需要告知服务自己所使用的字符集，也即客户端所在系统的字符集。
- locale：设置系统语言环境。Linux上客户端与服务端共享
- defaultUser：默认登录用户，默认值root
- defaultPass：默认登录密码，默认值taosdata

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

这些监测信息的采集缺省是打开的，但可以修改配置文件里的选项enableMonitor将其关闭或打开。