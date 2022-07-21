---
sidebar_labe: 开始使用
title: 快速体验 TDengine
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import PkgInstall from "./\_pkg_install.mdx";
import AptGetInstall from "./\_apt_get_install.mdx";

## 启动

安装后，请使用 `systemctl` 命令来启动 TDengine 的服务进程。

```bash
systemctl start taosd
```

检查服务是否正常工作：

```bash
systemctl status taosd
```

如果服务进程处于活动状态，则 status 指令会显示如下的相关信息：

```
Active: active (running)
```

如果后台服务进程处于停止状态，则 status 指令会显示如下的相关信息：

```
Active: inactive (dead)
```

如果 TDengine 服务正常工作，那么您可以通过 TDengine 的命令行程序 `taos` 来访问并体验 TDengine。

systemctl 命令汇总：

- 启动服务进程：`systemctl start taosd`

- 停止服务进程：`systemctl stop taosd`

- 重启服务进程：`systemctl restart taosd`

- 查看服务状态：`systemctl status taosd`

:::info

- systemctl 命令需要 _root_ 权限来运行，如果您非 _root_ 用户，请在命令前添加 sudo 。
- `systemctl stop taosd` 指令在执行后并不会马上停止 TDengine 服务，而是会等待系统中必要的落盘工作正常完成。在数据量很大的情况下，这可能会消耗较长时间。
- 如果系统中不支持 `systemd`，也可以用手动运行 `/usr/local/taos/bin/taosd` 方式启动 TDengine 服务。

:::

## TDengine 命令行 (CLI)

为便于检查 TDengine 的状态，执行数据库 (Database) 的各种即席(Ad Hoc)查询，TDengine 提供一命令行应用程序(以下简称为 TDengine CLI) taos。要进入 TDengine 命令行，您只要在安装有 TDengine 的 Linux 终端执行 `taos` 即可。

```bash
taos
```

如果连接服务成功，将会打印出欢迎消息和版本信息。如果失败，则会打印错误消息出来（请参考 [FAQ](/train-faq/faq) 来解决终端连接服务端失败的问题）。 TDengine CLI 的提示符号如下：

```cmd
taos>
```

在 TDengine CLI 中，用户可以通过 SQL 命令来创建/删除数据库、表等，并进行数据库(database)插入查询操作。在终端中运行的 SQL 语句需要以分号结束来运行。示例：

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

除执行 SQL 语句外，系统管理员还可以从 TDengine CLI 进行检查系统运行状态、添加删除用户账号等操作。TDengine CLI 连同应用驱动也可以独立安装在 Linux 或 Windows 机器上运行，更多细节请参考 [这里](../reference/taos-shell/)

## 使用 taosBenchmark 体验写入速度

启动 TDengine 的服务，在 Linux 终端执行 `taosBenchmark` （曾命名为 `taosdemo`）：

```bash
taosBenchmark
```

该命令将在数据库 test 下面自动创建一张超级表 meters，该超级表下有 1 万张表，表名为 "d0" 到 "d9999"，每张表有 1 万条记录，每条记录有 (ts, current, voltage, phase) 四个字段，时间戳从 "2017-07-14 10:40:00 000" 到 "2017-07-14 10:40:09 999"，每张表带有标签 location 和 groupId，groupId 被设置为 1 到 10， location 被设置为 "California.SanFrancisco" 或者 "California.LosAngeles"。

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

查询 location="California.SanFrancisco" 的记录总条数：

```sql
taos> select count(*) from test.meters where location="California.SanFrancisco";
```

查询 groupId=10 的所有记录的平均值、最大值、最小值等：

```sql
taos> select avg(current), max(voltage), min(phase) from test.meters where groupId=10;
```

对表 d10 按 10s 进行平均值、最大值和最小值聚合统计：

```sql
taos> select avg(current), max(voltage), min(phase) from test.d10 interval(10s);
```
