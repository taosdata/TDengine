# 立即开始

## 快速上手

TDengine目前只支持在Linux系统上安装和运行。用户可根据需求选择通过[源码](#通过源码安装)或者[安装包](#通过安装包安装)来安装。

### 通过源码安装

请参考我们的[TDengine github主页](https://github.com/taosdata/TDengine)下载源码并安装.

### 通过安装包安装

我们提供三种安装包，请选择你所需要的。TDengine的安装非常简单，从下载到安装成功仅仅只要几秒钟。
<ul id='packageList'>
<li><a id='tdengine-rpm' style='color:var(--b2)'>TDengine RPM package (1.5M)</a></li>
<li><a id='tdengine-deb' style='color:var(--b2)'>TDengine DEB package (1.7M)</a></li>
<li><a id='tdengine-tar' style='color:var(--b2)'>TDengine Tarball (3.0M)</a></li>
</ul>
目前，TDengine只支持在使用[`systemd`](https://en.wikipedia.org/wiki/Systemd)做进程服务管理的linux系统上安装。其他linux系统的支持正在开发中。用`which`命令来检测系统中是否存在`systemd`:

```cmd
which systemd
```

如果系统中不存在`systemd`命令，请考虑[通过源码安装](#通过源码安装)TDengine。

## 启动并体验TDengine

安装成功后，用户可使用`systemctl`命令来启动TDengine的服务进程。

```cmd
systemctl start taosd
```

检查服务是否正常工作。
```cmd
systemctl status taosd
```

如果TDengine服务正常工作，那么您可以通过TDengine的命令行程序`taos`来访问并体验TDengine。

**注：_systemctl_ 命令需要 _root_ 权限来运行，如果您非 _root_ 用户，请在命令前添加 _sudo_**

## TDengine命令行程序

执行TDengine命令行程序，您只要在Linux终端执行`taos`即可

```cmd
taos
```

如果TDengine终端链接服务成功，将会打印出欢迎消息和版本信息。如果失败，则会打印错误消息出来（请参考[FAQ](https://www.taosdata.com/cn/faq/)来解决终端链接服务端失败的问题）。TDengine终端的提示符号如下：

```cmd
taos>
```

在TDengine终端中，用户可以通过SQL命令来创建/删除数据库、表等，并进行插入查询操作。在终端中运行的SQL语句需要以分号结束来运行。示例：

```mysql
create database db;
use db;
create table t (ts timestamp, cdata int);
insert into t values ('2019-07-15 00:00:00', 10);
insert into t values ('2019-07-15 01:00:00', 20);
select * from t;
          ts          |   speed   |
===================================
 19-07-15 00:00:00.000|         10|
 19-07-15 01:00:00.000|         20|
Query OK, 2 row(s) in set (0.001700s)
```

除执行SQL语句外，系统管理员还可以从TDengine终端检查系统运行状态，添加删除用户账号等。

### 命令行参数

您可通过配置命令行参数来改变TDengine终端的行为。以下为常用的几个命令行参数：

- -c, --config-dir: 指定配置文件目录，默认为_/etc/taos_
- -h, --host: 指定服务的IP地址，默认为本地服务
- -s, --commands: 在不进入终端的情况下运行TDengine命令
- -u, -- user:  链接TDengine服务器的用户名，缺省为root
- -p, --password: 链接TDengine服务器的密码，缺省为taosdata
- -?, --help: 打印出所有命令行参数

示例：

```cmd
taos -h 192.168.0.1 -s "use db; show tables;"
```

### 运行SQL命令脚本

TDengine终端可以通过`source`命令来运行SQL命令脚本.

```
taos> source <filename>;
```
我们在目录“/tests/examples/bash/”下面提供了一个示例脚本“demo.sql",您可以直接将"filename"替换为我们的示例脚本快速尝试一下。 

### Shell小技巧

- 可以使用上下光标键查看已经历史输入的命令
- 修改用户密码。在shell中使用alter user命令
- ctrl+c 中止正在进行中的查询
- 执行`RESET QUERY CACHE`清空本地缓存的表的schema

## TDengine 极速体验
启动TDengine的服务，在Linux终端执行taosdemo 

```
> ./taosdemo
```

该命令将在数据库test下面自动创建一张超级表meters，该超级表下有1万张表，表名为"t0" 到"t9999"，每张表有10万条记录，每条记录有 （f1, f2， f3）三个字段，时间戳从"2017-07-14 10:40:00 000" 到"2017-07-14 10:41:39 999"，每张表带有标签areaid和loc, areaid被设置为1到10, loc被设置为"beijing"或者“shanghai"。

执行这条命令大概需要10分钟，最后共插入10亿条记录。

在TDengine客户端输入查询命令，体验查询速度。

-   查询超级表下记录总条数：

```
taos>select count(*) from test.meters;
```

- 查询10亿条记录的平均值、最大值、最小值等：

```
taos>select avg(f1), max(f2), min(f3) from test.meters;
```

-   查询areaid=10的记录总条数：

```
taos>select count(*) from test.meters where areaid=10;
```

- 查询loc="beijing"的所有记录的平均值、最大值、最小值等：

```
taos>select avg(f1), max(f2), min(f3) from test.meters where loc="beijing";
```

- 对表t10按10s进行平均值、最大值和最小值聚合统计：

```
taos>select avg(f1), max(f2), min(f3) from test.t10 interval(10s);
```

Note: taosdemo命令本身带有很多选项，配置表的数目、记录条数等等，请执行 `taosdemo --help`详细列出。您可以设置不同参数进行体验。

## 主要功能

TDengine的核心功能是时序数据库。除此之外，为减少研发的复杂度、系统维护的难度，TDengine还提供缓存、消息队列、订阅、流式计算等功能。更详细的功能如下：

- 使用类SQL语言用插入或查询数据
- 支持C/C++, Java(JDBC), Python, Go, RESTful, and Node.JS 开发接口
- 可通过Python/R/Matlab or TDengine shell做Ad Hoc查询分析
- 通过定时连续查询支持基于滑动窗口的流式计算
- 使用超级表来更灵活高效的聚合多个时间线的数据
- 时间轴上聚合一个或多个时间线的数据
- 支持数据订阅，一旦有新数据，就立即通知应用
- 支持缓存，每个时间线或设备的最新数据都从内存里快速获取
- 历史数据与实时数据处理完全透明，不用区别对待
- 支持链接Telegraf, Grafana等第三方工具
- 成套的配置和工具，让你更好的管理TDengine 

对于企业版，TDengine还提供如下高级功能：

- 线性水平扩展能力，以提供更高的处理速度和数据容量
- 高可靠，无单点故障，提供运营商级别的服务
- 多个副本自动同步，而且可以跨机房
- 多级存储，让历史数据处理的成本更低
- 用户友好的管理后台和工具，让管理更轻松简单 

TDengine是专为物联网、车联网、工业互联网、运维监测等场景优化设计的时序数据处理引擎。与其他方案相比，它的插入查询速度都快10倍以上。单核一秒钟就能插入100万数据点，读出1000万数据点。由于采用列式存储和优化的压缩算法，存储空间不及普通数据库的1/10.

## 深入了解TDengine

请继续阅读[文档](../documentation)来深入了解TDengine。