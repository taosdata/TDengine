# 立即开始

## <a class="anchor" id="install"></a>快捷安装

TDengine 软件分为服务器、客户端和报警模块三部分，目前 2.0 版服务器仅能在 Linux 系统上安装和运行，后续会支持 Windows、Mac OS 等系统。客户端可以在 Windows 或 Linux 上安装和运行。任何 OS 的应用也可以选择 RESTful 接口连接服务器 taosd。CPU 支持 X64/ARM64/MIPS64/Alpha64，后续会支持 ARM32、RISC-V 等 CPU 架构。用户可根据需求选择通过 [源码](https://www.taosdata.com/cn/getting-started/#通过源码安装) 或者 [安装包](https://www.taosdata.com/cn/getting-started/#通过安装包安装) 来安装。

### <a class="anchor" id="source-install"></a>通过源码安装

请参考我们的 [TDengine github 主页](https://github.com/taosdata/TDengine) 下载源码并安装.

### 通过 Docker 容器运行

暂时不建议生产环境采用 Docker 来部署 TDengine 的客户端或服务端，但在开发环境下或初次尝试时，使用 Docker 方式部署是十分方便的。特别是，利用 Docker，可以方便地在 Mac OS X 和 Windows 环境下尝试 TDengine。

详细操作方法请参照 [通过 Docker 快速体验 TDengine](https://www.taosdata.com/cn/documentation/getting-started/docker)。

### <a class="anchor" id="package-install"></a>通过安装包安装

TDengine 的安装非常简单，从下载到安装成功仅仅只要几秒钟。服务端安装包包含客户端和连接器，我们提供三种安装包，您可以根据需要选择：

安装包下载在 [这里](https://www.taosdata.com/cn/getting-started/#通过安装包安装)。

具体的安装过程，请参见 [TDengine 多种安装包的安装和卸载](https://www.taosdata.com/blog/2019/08/09/566.html) 以及 [视频教程](https://www.taosdata.com/blog/2020/11/11/1941.html)。

## <a class="anchor" id="start"></a>轻松启动

安装成功后，用户可使用 `systemctl` 命令来启动 TDengine 的服务进程。

```bash
$ systemctl start taosd
```

检查服务是否正常工作。
```bash
$ systemctl status taosd
```

如果 TDengine 服务正常工作，那么您可以通过 TDengine 的命令行程序 `taos` 来访问并体验 TDengine。  

**注意：**  

- systemctl 命令需要 _root_ 权限来运行，如果您非 _root_ 用户，请在命令前添加 sudo 。
- 为更好的获得产品反馈，改善产品，TDengine 会采集基本的使用信息，但您可以修改系统配置文件 taos.cfg 里的配置参数 telemetryReporting, 将其设为 0，就可将其关闭。
- TDengine 采用 FQDN (一般就是 hostname )作为节点的 ID，为保证正常运行，需要给运行 taosd 的服务器配置好 hostname，在客户端应用运行的机器配置好 DNS 服务或 hosts 文件，保证 FQDN 能够解析。
- `systemctl stop taosd` 指令在执行后并不会马上停止 TDengine 服务，而是会等待系统中必要的落盘工作正常完成。在数据量很大的情况下，这可能会消耗较长时间。

* TDengine 支持在使用 [`systemd`](https://en.wikipedia.org/wiki/Systemd) 做进程服务管理的 linux 系统上安装，用 `which systemctl` 命令来检测系统中是否存在 `systemd` 包:

  ```bash
  $ which systemctl
  ```

  如果系统中不支持 systemd，也可以用手动运行 /usr/local/taos/bin/taosd 方式启动 TDengine 服务。

  
## <a class="anchor" id="console"></a>TDengine 命令行程序

执行 TDengine 命令行程序，您只要在 Linux 终端执行 `taos` 即可。

```bash
$ taos
```

如果 TDengine 终端连接服务成功，将会打印出欢迎消息和版本信息。如果失败，则会打印错误消息出来（请参考 [FAQ](https://www.taosdata.com/cn/documentation/faq/) 来解决终端连接服务端失败的问题）。TDengine 终端的提示符号如下：

```cmd
taos>
```

在 TDengine 终端中，用户可以通过 SQL 命令来创建/删除数据库、表等，并进行插入查询操作。在终端中运行的 SQL 语句需要以分号结束来运行。示例：

```mysql
create database demo;
use demo;
create table t (ts timestamp, speed int);
insert into t values ('2019-07-15 00:00:00', 10);
insert into t values ('2019-07-15 01:00:00', 20);
select * from t;
           ts            |    speed    |
========================================
 2019-07-15 00:00:00.000 |          10 |
 2019-07-15 01:00:00.000 |          20 |
Query OK, 2 row(s) in set (0.003128s)
```

除执行 SQL 语句外，系统管理员还可以从 TDengine 终端检查系统运行状态，添加删除用户账号等。

### 命令行参数

您可通过配置命令行参数来改变 TDengine 终端的行为。以下为常用的几个命令行参数：

- -c, --config-dir: 指定配置文件目录，默认为 _/etc/taos_
- -h, --host: 指定服务的 FQDN 地址（也可以使用 IP），默认为连接本地服务
- -s, --commands: 在不进入终端的情况下运行 TDengine 命令
- -u, --user: 连接 TDengine 服务器的用户名，缺省为 root
- -p, --password: 连接TDengine服务器的密码，缺省为 taosdata
- -?, --help: 打印出所有命令行参数

示例：

```bash
$ taos -h 192.168.0.1 -s "use db; show tables;"
```

### 运行 SQL 命令脚本

TDengine 终端可以通过 `source` 命令来运行 SQL 命令脚本.

```mysql
taos> source <filename>;
```

### Shell 小技巧

- 可以使用上下光标键查看历史输入的指令
- 修改用户密码，在 shell 中使用 alter user 指令
- ctrl+c 中止正在进行中的查询
- 执行 `RESET QUERY CACHE` 清空本地缓存的表 schema


## <a class="anchor" id="demo"></a>TDengine 极速体验

启动 TDengine 的服务，在 Linux 终端执行 taosdemo

```bash
$ taosdemo
```

该命令将在数据库 test 下面自动创建一张超级表 meters，该超级表下有 1 万张表，表名为 "d0" 到 "d9999"，每张表有 1 万条记录，每条记录有 (ts, current, voltage, phase) 四个字段，时间戳从 "2017-07-14 10:40:00 000" 到 "2017-07-14 10:40:09 999"，每张表带有标签 location 和 groupdId，groupdId 被设置为 1 到 10， location 被设置为 "beijing" 或者 "shanghai"。

执行这条命令大概需要几分钟，最后共插入 1 亿条记录。

在 TDengine 客户端输入查询命令，体验查询速度。

- 查询超级表下记录总条数：

```mysql
taos> select count(*) from test.meters;
```

- 查询 1 亿条记录的平均值、最大值、最小值等：

```mysql
taos> select avg(current), max(voltage), min(phase) from test.meters;
```

- 查询 location="beijing" 的记录总条数：

```mysql
taos> select count(*) from test.meters where location="beijing";
```

- 查询 groupdId=10 的所有记录的平均值、最大值、最小值等：

```mysql
taos> select avg(current), max(voltage), min(phase) from test.meters where groupdId=10;
```

- 对表 d10 按 10s 进行平均值、最大值和最小值聚合统计：

```mysql
taos> select avg(current), max(voltage), min(phase) from test.d10 interval(10s);
```

**Note:** taosdemo 命令本身带有很多选项，配置表的数目、记录条数等等，请执行 `taosdemo --help` 详细列出。您可以设置不同参数进行体验。


## 客户端和报警模块

如果客户端和服务端运行在不同的电脑上，可以单独安装客户端。Linux 和 Windows 安装包可以在 [这里](https://www.taosdata.com/cn/getting-started/#客户端) 下载。

报警模块的 Linux 和 Windows 安装包请在 [所有下载链接](https://www.taosdata.com/cn/all-downloads/) 页面搜索“TDengine Alert Linux”章节或“TDengine Alert Windows”章节进行下载。使用方法请参考 [报警模块的使用方法](https://github.com/taosdata/TDengine/blob/master/alert/README_cn.md)。

  
## <a class="anchor" id="platforms"></a>支持平台列表

### TDengine 服务器支持的平台列表

|                | **CentOS 6/7/8** | **Ubuntu 16/18/20** | **Other Linux** | **统信 UOS** | **银河/中标麒麟** | **凝思 V60/V80** | **华为 EulerOS** |
| -------------- | --------------------- | ------------------------ | --------------- | --------------- | ------------------------- | --------------------- | --------------------- |
| X64            | ●                     | ●                        |                 | ○               | ●                         | ●                     | ●                     |
| 龙芯 MIPS64     |                       |                          | ●               |                 |                           |                       |                       |
| 鲲鹏 ARM64      |                       | ○                        | ○               |                 | ●                         |                       |                       |
| 申威 Alpha64    |                       |                          | ○               | ●               |                           |                       |                       |
| 飞腾 ARM64      |                       | ○ 优麒麟                  |                 |                 |                           |                       |                       |
| 海光 X64        | ●                     | ●                        | ●               | ○               | ●                         | ●                     |                       |
| 瑞芯微 ARM64    |                       |                          | ○               |                 |                           |                       |                       |
| 全志 ARM64      |                       |                          | ○               |                 |                           |                       |                       |
| 炬力 ARM64      |                       |                          | ○               |                 |                           |                       |                       |
| 华为云 ARM64    |                       |                          |                 |                 |                           |                       | ●                     |

注： ● 表示经过官方测试验证， ○ 表示非官方测试验证。



### TDengine 客户端和连接器支持的平台列表

目前 TDengine 的连接器可支持的平台广泛，目前包括：X64/X86/ARM64/ARM32/MIPS/Alpha 等硬件平台，以及 Linux/Win64/Win32 等开发环境。

对照矩阵如下：

| **CPU**     | **X64 64bit** |           |           | **X86 32bit** | **ARM64** | **ARM32** | **MIPS 龙芯** | **Alpha 申威** | **X64 海光** |
| ----------- | --------------- | --------- | --------- | --------------- | --------- | --------- | ------------------- | -------------------- | ------------------ |
| **OS**      | **Linux**       | **Win64** | **Win32** | **Win32**       | **Linux** | **Linux** | **Linux**           | **Linux**            | **Linux**          |
| **C/C++**   | ●               | ●         | ●         | ○               | ●         | ●         | ●                   | ●                    | ●                  |
| **JDBC**    | ●               | ●         | ●         | ○               | ●         | ●         | ●                   | ●                    | ●                  |
| **Python**  | ●               | ●         | ●         | ○               | ●         | ●         | ●                   | --                   | ●                  |
| **Go**      | ●               | ●         | ●         | ○               | ●         | ●         | ○                   | --                   | --                 |
| **NodeJs**  | ●               | ●         | ○         | ○               | ●         | ●         | ○                   | --                   | --                 |
| **C#**      | ●               | ●         | ○         | ○               | ○         | ○         | ○                   | --                   | --                 |
| **RESTful** | ●               | ●         | ●         | ●               | ●         | ●         | ●                   | ●                    | ●                  |

注： ● 表示经过官方测试验证， ○ 表示非官方测试验证。

请跳转到 [连接器](https://www.taosdata.com/cn/documentation/connector) 查看更详细的信息。

