---
sidebar_label: 用安装包快速体验
title: 使用安装包快速体验 TDengine
description: 使用安装包快速体验 TDengine
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import PkgListV3 from "/components/PkgListV3";

TDengine 完整的软件包包括服务端（taosd）、应用驱动（taosc）、用于与第三方系统对接并提供 RESTful 接口的 taosAdapter、命令行程序（CLI，taos）和一些工具软件。目前 TDinsight 仅在 Linux 系统上安装和运行，后续将支持 Windows、macOS 等系统。TDengine 除了提供多种语言的连接器之外，还通过 [taosAdapter](../../reference/components/taosadapter/) 提供 [RESTful 接口](../../reference/connector/rest-api/)。

为方便使用，标准的服务端安装包包含了 taosd、taosAdapter、taosc、taos、taosdump、taosBenchmark、TDinsight 安装脚本和示例代码.

在 Linux 系统上，TDengine 提供了 tar.gz 格式安装包进行安装。

## 运行环境要求
在linux系统中，运行环境最低要求如下:

linux 内核版本 - 3.10.0-1160.83.1.el7.x86_64;

glibc 版本    -  2.17;

如果通过clone源码进行编译安装，还需要满足:

cmake版本 - 3.26.4或以上;

gcc 版本  - 9.3.1或以上;


## 安装

**注意**

从TDengine 3.0.6.0 开始，不再提供单独的 taosTools 安装包，原 taosTools 安装包中包含的工具都在 TDengine-server 安装包中，如果需要请直接下载 TDengine -server 安装包。

<Tabs>

<TabItem label="tar.gz 安装" value="tarinst">

1. 从列表中下载获得 tar.gz 安装包；
   <PkgListV3 type={0}/>
2. 进入到安装包所在目录，使用 `tar` 解压安装包；
3. 进入到安装包所在目录，先解压文件后，进入子目录，执行其中的 install.sh 安装脚本。

> 请将 `<version>` 替换为下载的安装包版本

```bash
tar -zxvf TDengine-server-<version>-Linux-x64.tar.gz
```

解压文件后，进入相应子目录，执行其中的 `install.sh` 安装脚本：

```bash
sudo ./install.sh
```

:::info
install.sh 安装脚本在执行过程中，会通过命令行交互界面询问一些配置信息。如果希望采取无交互安装方式，那么可以运行 `./install.sh -e no`。运行 `./install.sh -h` 指令可以查看所有参数的详细说明信息。
:::

</TabItem>

</Tabs>
:::note
当安装第一个节点时，出现 `Enter FQDN:` 提示的时候，不需要输入任何内容。只有当安装第二个或以后更多的节点时，才需要输入已有集群中任何一个可用节点的 FQDN，支持该新节点加入集群。当然也可以不输入，而是在新节点启动前，配置到新节点的配置文件中。

:::

## 启动

<Tabs>
<TabItem label="Linux 系统" value="linux">

安装后，请使用 `systemctl` 命令来启动 TDengine 的服务进程。

```bash
systemctl start taosd
systemctl start taosadapter
systemctl start taoskeeper
systemctl start taos-explorer
```

你也可以直接运行 start-all.sh 脚本来启动上面的所有服务
```bash
start-all.sh 
```

可以使用 systemctl 来单独管理上面的每一个服务

```bash
systemctl start taosd
systemctl stop taosd
systemctl restart taosd
systemctl status taosd
```

:::info

- `systemctl` 命令需要 _root_ 权限来运行，如果您非 _root_ 用户，请在命令前添加 `sudo`。
- `systemctl stop taosd` 指令在执行后并不会马上停止 TDengine 服务，而是会等待系统中必要的落盘工作正常完成。在数据量很大的情况下，这可能会消耗较长时间。
- 如果系统中不支持 `systemd`，也可以用手动运行 `/usr/local/taos/bin/taosd` 方式启动 TDengine 服务。

:::

</TabItem>
</Tabs>

## TDengine 命令行（CLI）

为便于检查 TDengine 的状态，执行数据库（Database）的各种即席（Ad Hoc）查询，TDengine 提供一命令行应用程序（以下简称为 TDengine CLI）taos。要进入 TDengine 命令行，您只要在终端执行 `taos` (Linux/Mac) 或 `taos.exe` (Windows) 即可。 TDengine CLI 的提示符号如下：

```cmd
taos>
```

在 TDengine CLI 中，用户可以通过 SQL 命令来创建/删除数据库、表等，并进行数据库（Database）插入查询操作。在终端中运行的 SQL 语句需要以分号（;）结束来运行。示例：

```sql
CREATE DATABASE demo;
USE demo;
CREATE TABLE t (ts TIMESTAMP, speed INT);
INSERT INTO t VALUES ('2019-07-15 00:00:00', 10);
INSERT INTO t VALUES ('2019-07-15 01:00:00', 20);
SELECT * FROM t;

           ts            |    speed    |
========================================
 2019-07-15 00:00:00.000 |          10 |
 2019-07-15 01:00:00.000 |          20 |

Query OK, 2 row(s) in set (0.003128s)
```

除执行 SQL 语句外，系统管理员还可以从 TDengine CLI 进行检查系统运行状态、添加删除用户账号等操作。TDengine CLI 连同应用驱动也可以独立安装在机器上运行，更多细节请参考 [TDengine 命令行](../../reference/tools/taos-cli/)。

## 快速体验

### 体验写入

taosBenchmark 是一个专为测试 TDengine 性能而设计的工具，它能够全面评估TDengine 在写入、查询和订阅等方面的功能表现。该工具能够模拟大量设备产生的数据，并允许用户灵活控制数据库、超级表、标签列的数量和类型、数据列的数量和类型、子表数量、每张子表的数据量、写入数据的时间间隔、工作线程数量以及是否写入乱序数据等策略。

启动 TDengine 的服务，在终端中执行如下命令

```shell
taosBenchmark -y
```

系统将自动在数据库 test 下创建一张名为 meters的超级表。这张超级表将包含 10,000 张子表，表名从 d0 到 d9999，每张表包含 10,000条记录。每条记录包含 ts（时间戳）、current（电流）、voltage（电压）和 phase（相位）4个字段。时间戳范围从 “2017-07-14 10:40:00 000” 到 “2017-07-14 10:40:09 999”。每张表还带有 location 和 groupId 两个标签，其中，groupId 设置为 1 到 10，而 location 则设置为 California.Campbell、California.Cupertino 等城市信息。

执行该命令后，系统将迅速完成 1 亿条记录的写入过程。实际所需时间取决于硬件性能，但即便在普通 PC 服务器上，这个过程通常也只需要十几秒。

taosBenchmark 提供了丰富的选项，允许用户自定义测试参数，如表的数目、记录条数等。要查看详细的参数列表，请在终端中输入如下命令
```shell
taosBenchmark --help
```

有关taosBenchmark 的详细使用方法，请参考[taosBenchmark 参考手册](../../reference/tools/taosbenchmark)

### 体验查询

使用上述 taosBenchmark 插入数据后，可以在 TDengine CLI（taos）输入查询命令，体验查询速度。

1. 查询超级表 meters 下的记录总条数
```shell
SELECT COUNT(*) FROM test.meters;
```

2. 查询 1 亿条记录的平均值、最大值、最小值
```shell
SELECT AVG(current), MAX(voltage), MIN(phase) FROM test.meters;
```

3. 查询 location = "California.SanFrancisco" 的记录总条数
```shell
SELECT COUNT(*) FROM test.meters WHERE location = "California.SanFrancisco";
```

4. 查询 groupId = 10 的所有记录的平均值、最大值、最小值
```shell
SELECT AVG(current), MAX(voltage), MIN(phase) FROM test.meters WHERE groupId = 10;
```

5. 对表 d1001 按每 10 秒进行平均值、最大值和最小值聚合统计
```shell
SELECT _wstart, AVG(current), MAX(voltage), MIN(phase) FROM test.d1001 INTERVAL(10s);
```

在上面的查询中，使用系统提供的伪列 _wstart 来给出每个窗口的开始时间。
