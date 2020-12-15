# 立即开始

## 快捷安装

TDengine软件分为服务器、客户端和报警模块三部分，目前2.0版服务器仅能在Linux系统上安装和运行，后续会支持Windows、mac OS等系统。

**应用驱动**

如果应用在Windows和Linux上运行，可使用C/C++/C#/JAVA/Python/Go/Node.js接口连接服务器。如果应用在Mac上运行，目前可以使用RESTful接口连接服务器。

**CPU**

CPU支持X64/ARM64/MIPS64/Alpha64，后续会支持ARM32、RISC-V等CPU架构。用户可根据需求选择通过[源码](https://www.taosdata.com/cn/getting-started/#通过源码安装)或者[安装包](https://www.taosdata.com/cn/getting-started/#通过安装包安装)来安装。

**服务器**

目前TDengine服务器可以运行在以下平台上：

|                | **CentOS**  **6/7/8** | **Ubuntu**  **16/18/20** | **Other Linux** | **统信****UOS** | **银河****/****中标麒麟** | **凝思**  **V60/V80** |
| -------------- | --------------------- | ------------------------ | --------------- | --------------- | ------------------------- | --------------------- |
| X64            | ●                     | ●                        |                 | ○               | ●                         | ●                     |
| 树莓派ARM32    |                       | ●                        | ●               |                 |                           |                       |
| 龙芯MIPS64     |                       |                          | ●               |                 |                           |                       |
| 鲲鹏  ARM64    |                       | ○                        | ○               |                 | ●                         |                       |
| 申威  Alpha64  |                       |                          | ○               | ●               |                           |                       |
| 飞腾ARM64      |                       | ○优麒麟                  |                 |                 |                           |                       |
| 海光X64        | ●                     | ●                        | ●               | ○               | ●                         | ●                     |
| 瑞芯微ARM64/32 |                       |                          | ○               |                 |                           |                       |
| 全志ARM64/32   |                       |                          | ○               |                 |                           |                       |
| 炬力ARM64/32   |                       |                          | ○               |                 |                           |                       |
| TI ARM32       |                       |                          | ○               |                 |                           |                       |

 其中 ● 表示经过官方测试验证， ○ 表示非官方测试验证。



### 通过源码安装

请参考我们的[TDengine github主页](https://github.com/taosdata/TDengine)下载源码并安装.

### 通过Docker容器运行

请参考[TDengine官方Docker镜像的发布、下载和使用](https://www.taosdata.com/blog/2020/05/13/1509.html)

### 通过安装包安装

服务器部分，我们提供三种安装包，您可以根据需要选择。TDengine的安装非常简单，从下载到安装成功仅仅只要几秒钟。

- TDengine-server-2.0.9.0-Linux-x64.rpm (4.2M)
- TDengine-server-2.0.9.0-Linux-x64.deb (2.7M)
- TDengine-server-2.0.9.0-Linux-x64.tar.gz (4.5M)


客户端部分，Linux安装包如下：

- TDengine-client-2.0.9.0-Linux-x64.tar.gz(3.0M)
- TDengine-client-2.0.9.0-Windows-x64.exe(2.8M)
- TDengine-client-2.0.9.0-Windows-x86.exe(2.8M)

报警模块的Linux安装包如下（请参考[报警模块的使用方法](https://github.com/taosdata/TDengine/blob/master/alert/README_cn.md)）：

- TDengine-alert-2.0.9.0-Linux-x64.tar.gz (8.1M)

目前，TDengine 支持在使用[`systemd`](https://en.wikipedia.org/wiki/Systemd)做进程服务管理的linux系统上安装，用`which systemctl`命令来检测系统中是否存在`systemd`包:

```cmd
which systemctl
```

具体的安装过程，请参见<a href="https://www.taosdata.com/blog/2019/08/09/566.html">TDengine多种安装包的安装和卸载</a>。

## 轻松启动

安装成功后，用户可使用`systemctl`命令来启动TDengine的服务进程。

```cmd
systemctl start taosd
```

检查服务是否正常工作。
```cmd
systemctl status taosd
```

如果TDengine服务正常工作，那么您可以通过TDengine的命令行程序`taos`来访问并体验TDengine。  

**注意：**  

- systemctl命令需要 _root_ 权限来运行，如果您非 _root_ 用户，请在命令前添加 sudo
- 为更好的获得产品反馈，改善产品，TDengine会采集基本的使用信息，但您可以修改系统配置文件taos.cfg里的配置参数telemetryReporting, 将其设为0，就可将其关闭。

如果系统中不支持`systemd`，也可以用手动运行 /usr/local/taos/bin/taosd 方式启动 TDengine 服务。

## TDengine命令行程序

执行TDengine命令行程序，您只要在Linux终端执行`taos`即可。

```cmd
taos
```

如果TDengine终端连接服务成功，将会打印出欢迎消息和版本信息。如果失败，则会打印错误消息出来（请参考[FAQ](https://www.taosdata.com/cn/faq/)来解决终端连接服务端失败的问题）。TDengine终端的提示符号如下：

```cmd
taos>
```

在TDengine终端中，用户可以通过SQL命令来创建/删除数据库、表等，并进行插入查询操作。在终端中运行的SQL语句需要以分号结束来运行。示例：

```mysql
create database demo;
use demo;
create table t (ts timestamp, speed int);
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
- -u, -- user:  连接TDengine服务器的用户名，缺省为root
- -p, --password: 连接TDengine服务器的密码，缺省为taosdata
- -?, --help: 打印出所有命令行参数

示例：

```cmd
taos -h 192.168.0.1 -s "use db; show tables;"
```

### 运行SQL命令脚本

TDengine终端可以通过`source`命令来运行SQL命令脚本.

```mysql
taos> source <filename>;
```

### Shell小技巧

- 可以使用上下光标键查看已经历史输入的命令
- 修改用户密码。在shell中使用alter user命令
- ctrl+c 中止正在进行中的查询
- 执行`RESET QUERY CACHE`清空本地缓存的表的schema

## TDengine 极速体验

启动TDengine的服务，在Linux终端执行taosdemo

```
> taosdemo
```

该命令将在数据库test下面自动创建一张超级表meters，该超级表下有1万张表，表名为"t0" 到"t9999"，每张表有10万条记录，每条记录有 （f1, f2， f3）三个字段，时间戳从"2017-07-14 10:40:00 000" 到"2017-07-14 10:41:39 999"，每张表带有标签areaid和loc, areaid被设置为1到10, loc被设置为"beijing"或者“shanghai"。

执行这条命令大概需要10分钟，最后共插入10亿条记录。

在TDengine客户端输入查询命令，体验查询速度。

- 查询超级表下记录总条数：

```
taos>select count(*) from test.meters;
```

- 查询10亿条记录的平均值、最大值、最小值等：

```
taos>select avg(f1), max(f2), min(f3) from test.meters;
```

- 查询loc="beijing"的记录总条数：

```
taos>select count(*) from test.meters where loc="beijing";
```

- 查询areaid=10的所有记录的平均值、最大值、最小值等：

```
taos>select avg(f1), max(f2), min(f3) from test.meters where areaid=10;
```

- 对表t10按10s进行平均值、最大值和最小值聚合统计：

```
taos>select avg(f1), max(f2), min(f3) from test.t10 interval(10s);
```

**Note:** taosdemo命令本身带有很多选项，配置表的数目、记录条数等等，请执行 `taosdemo --help`详细列出。您可以设置不同参数进行体验。

