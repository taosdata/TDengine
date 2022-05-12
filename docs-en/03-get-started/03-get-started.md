---
title: Get Started
description: "从 Docker，安装包或使用 apt-get 快速安装 TDengine, 通过命令行程序TAOS CLI和工具 taosdemo 快速体验 TDengine 功能"
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import PkgInstall from "./\_pkg_install.mdx";
import AptGetInstall from "./\_apt_get_install.mdx";

## 安装

TDengine 完整的软件包包括服务端(taosd)、用于与第三方系统对接并提供RESTful接口的taosAdapter、应用驱动(taosc)、命令行程序 (CLI，taos) 和一些工具软件，目前 2.X 版服务端taosd、taosAdapter 仅在 Linux 系统上安装和运行，后续将支持 Windows、macOS 等系统。应用驱动 taosc 与 TDengine CLI 可以在 Windows 或 Linux 上安装和运行。TDengine 除 RESTful接口外，还提供一些列编程语言的连接器。2.4 之前的版本中，无 taosAdapter，RESTfule 接口均由 taosd 内置的 http 服务提供。

TDengine 支持 X64/ARM64/MIPS64/Alpha64 硬件平台，后续将支持 ARM32、RISC-V 等 CPU 架构。

<Tabs defaultValue="apt-get">
<TabItem value="docker" label="Docker">
如果已经安装了 docker， 只需执行下面的命令。

```shell
docker run -d -p 6030-6049:6030-6049 -p 6030-6049:6030-6049/udp tdengine/tdengine
```

确定该容器已经启动并且在正常运行

```shell
docker ps
```

进入该容器并执行 bash

```shell
docker exec -it <containrid> bash
```

然后就可以执行相关的 Linux 命令操作和访问 TDengine

详细操作方法请参照 [通过 Docker 快速体验 TDengine](/train-faq/docker)。

:::info
从 2.4.0.10 开始，除 taosd 以外，Docker 镜像还包含：taos、taosAdapter、taosdump、taosBenchmark、TDinsight 安装脚本和示例代码。启动 Docker 容器时，将同时启动 taosAdapter 和 taosd，实现对 RESTful 的支持。

:::

</TabItem>
<TabItem value="apt-get" label="apt-get">
<AptGetInstall />
</TabItem>
<TabItem value="pkg" label="安装包">
<PkgInstall />
</TabItem>
<TabItem value="src" label="源码">

如果您希望对 TDengine 贡献代码或对内部实现感兴趣，请参考我们的 [TDengine GitHub 主页](https://github.com/taosdata/TDengine) 下载源码构建和安装.

下载其他组件、最新 Beta 版及之前版本的安装包，请点击[这里](https://www.taosdata.com/cn/all-downloads/)。

</TabItem>
</Tabs>

## 启动

安装后，请使用 `systemctl` 命令来启动 TDengine 的服务进程。

```bash
systemctl start taosd
```

检查服务是否正常工作：

```bash
systemctl status taosd
```

如果 TDengine 服务正常工作，那么您可以通过 TDengine 的命令行程序 `taos` 来访问并体验 TDengine。

:::info

- systemctl 命令需要 _root_ 权限来运行，如果您非 _root_ 用户，请在命令前添加 sudo 。
- 为更好的获得产品反馈，改善产品，TDengine 会采集基本的使用信息，但您可以修改系统配置文件 taos.cfg 里的配置参数 telemetryReporting，将其设为 0，就可将其关闭。
- TDengine 采用 FQDN（一般就是 hostname）作为节点的 ID，为保证正常运行，需要给运行 taosd 的服务器配置好 FQDN，在 TDengine CLI 或应用运行的机器配置好 DNS 服务或 hosts 文件，保证 FQDN 能够解析。
- `systemctl stop taosd` 指令在执行后并不会马上停止 TDengine 服务，而是会等待系统中必要的落盘工作正常完成。在数据量很大的情况下，这可能会消耗较长时间。

TDengine 支持在使用 [`systemd`](https://en.wikipedia.org/wiki/Systemd) 做进程服务管理的 Linux 系统上安装，用 `which systemctl` 命令来检测系统中是否存在 `systemd` 包:

```bash
which systemctl
```

如果系统中不支持 `systemd`，也可以用手动运行 `/usr/local/taos/bin/taosd` 方式启动 TDengine 服务。

:::note

## TDengine 命令行 (CLI)

为便于检查 TDengine 的状态，执行各种即席(Ad Hoc)查询，TDengine 提供一命令行应用程序(以下简称为 TDengine CLI) taos。要进入 TDengine 命令行，您只要在安装有 TDengine 的 Linux 终端执行 `taos` 即可。

```bash
taos
```

如果连接服务成功，将会打印出欢迎消息和版本信息。如果失败，则会打印错误消息出来（请参考 [FAQ](/train-faq/faq) 来解决终端连接服务端失败的问题）。 TDengine CLI 的提示符号如下：

```cmd
taos>
```

在 TDengine CLI中，用户可以通过 SQL 命令来创建/删除数据库、表等，并进行插入查询操作。在终端中运行的 SQL 语句需要以分号结束来运行。示例：

```sql
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

除执行 SQL 语句外，系统管理员还可以从 TDengine CLI 进行检查系统运行状态、添加删除用户账号等操作。TAOS CLI 连同应用驱动也可以独立安装在 Linux 或 windows 机器上运行，更多细节请参考 [这里](../reference/taos-shell/)

## 使用 taosBenchmark 体验写入速度

启动 TDengine 的服务，在 Linux 终端执行 `taosBenchmark` （曾命名为 `taosdemo`）：

```bash
taosBenchmark
```

该命令将在数据库 test 下面自动创建一张超级表 meters，该超级表下有 1 万张表，表名为 "d0" 到 "d9999"，每张表有 1 万条记录，每条记录有 (ts, current, voltage, phase) 四个字段，时间戳从 "2017-07-14 10:40:00 000" 到 "2017-07-14 10:40:09 999"，每张表带有标签 location 和 groupId，groupId 被设置为 1 到 10， location 被设置为 "beijing" 或者 "shanghai"。

这条命令很快完成 1 亿条记录的插入。具体时间取决于硬件性能，即使在一台普通的 PC 服务器往往也仅需十几秒。

taosBenchmark 命令本身带有很多选项，配置表的数目、记录条数等等，您可以设置不同参数进行体验，请执行 `taosBenchmark --help` 详细列出。taosBenchmark 详细使用方法请参照 [如何使用 taosBenchmark 对 TDengine 进行性能测试](https://www.taosdata.com/2021/10/09/3111.html)。

## 使用 TDengine CLI 体验查询速度

使用上述 taosBenchmark 插入数据后，可以在 TDengine CLI 输入查询命令，体验查询速度。

查询超级表下记录总条数：

```sql
taos> select count(*) from test.meters;
```

查询 1 亿条记录的平均值、最大值、最小值等：

```sql
taos> select avg(current), max(voltage), min(phase) from test.meters;
```

查询 location="beijing" 的记录总条数：

```sql
taos> select count(*) from test.meters where location="beijing";
```

查询 groupId=10 的所有记录的平均值、最大值、最小值等：

```sql
taos> select avg(current), max(voltage), min(phase) from test.meters where groupId=10;
```

对表 d10 按 10s 进行平均值、最大值和最小值聚合统计：

```sql
taos> select avg(current), max(voltage), min(phase) from test.d10 interval(10s);
```
