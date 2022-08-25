---
sidebar_label: 安装包
title: 使用安装包立即开始
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import PkgListV3 from "/components/PkgListV3";

您可以[用 Docker 立即体验](../../get-started/docker/) TDengine。如果您希望对 TDengine 贡献代码或对内部实现感兴趣，请参考我们的 [TDengine GitHub 主页](https://github.com/taosdata/TDengine) 下载源码构建和安装.

TDengine 完整的软件包包括服务端（taosd）、用于与第三方系统对接并提供 RESTful 接口的 taosAdapter、应用驱动（taosc）、命令行程序 (CLI，taos) 和一些工具软件。目前 taosAdapter 仅在 Linux 系统上安装和运行，后续将支持 Windows、macOS 等系统。TDengine 除了提供多种语言的连接器之外，还通过 [taosAdapter](../../reference/taosadapter/) 提供 [RESTful 接口](../../connector/rest-api/)。

为方便使用，标准的服务端安装包包含了 taosd、taosAdapter、taosc、taos、taosdump、taosBenchmark、TDinsight 安装脚本和示例代码；如果您只需要用到服务端程序和客户端连接的 C/C++ 语言支持，也可以仅下载 lite 版本的安装包。

在 Linux 系统上，TDengine 开源版本提供 deb 和 rpm 格式安装包，用户可以根据自己的运行环境选择合适的安装包。其中 deb 支持 Debian/Ubuntu 及衍生系统，rpm 支持 CentOS/RHEL/SUSE 及衍生系统。同时我们也为企业用户提供 tar.gz 格式安装包，也支持通过 `apt-get` 工具从线上进行安装。需要注意的是，rpm 和 deb 包不含 taosdump 和 TDinsight 安装脚本，这些工具需要通过安装 taosTool 包获得。TDengine 也提供 Windows x64 平台的安装包。

## 安装

<Tabs>
<TabItem label="Deb 安装" value="debinst">

1. 从列表中下载获得 deb 安装包；
<PkgListV3 type={6}/>
2. 进入到安装包所在目录，执行如下的安装命令：

```bash
# 替换为下载的安装包版本
sudo dpkg -i TDengine-server-<version>-Linux-x64.deb
```

</TabItem>

<TabItem label="RPM 安装" value="rpminst">

1. 从列表中下载获得 rpm 安装包；
<PkgListV3 type={5}/>
2. 进入到安装包所在目录，执行如下的安装命令：

```bash
# 替换为下载的安装包版本
sudo rpm -ivh TDengine-server-<version>-Linux-x64.rpm
```

</TabItem>

<TabItem label="tar.gz 安装" value="tarinst">

1. 从列表中下载获得 tar.gz 安装包；
<PkgListV3 type={0}/>
2. 进入到安装包所在目录，先解压文件后，进入子目录，执行其中的 install.sh 安装脚本：

```bash
# 替换为下载的安装包版本
tar -zxvf TDengine-server-<version>-Linux-x64.tar.gz
```

解压后进入相应路径，执行

```bash
sudo ./install.sh
```

:::info
install.sh 安装脚本在执行过程中，会通过命令行交互界面询问一些配置信息。如果希望采取无交互安装方式，那么可以用 -e no 参数来执行 install.sh 脚本。运行 `./install.sh -h` 指令可以查看所有参数的详细说明信息。
:::

</TabItem>

<TabItem value="apt-get" label="apt-get">
可以使用 apt-get 工具从官方仓库安装。

**安装包仓库**

```bash
wget -qO - http://repos.taosdata.com/tdengine.key | sudo apt-key add -
echo "deb [arch=amd64] http://repos.taosdata.com/tdengine-stable stable main" | sudo tee /etc/apt/sources.list.d/tdengine-stable.list
```

如果安装 Beta 版需要安装包仓库

```bash
wget -qO - http://repos.taosdata.com/tdengine.key | sudo apt-key add -
echo "deb [arch=amd64] http://repos.taosdata.com/tdengine-beta beta main" | sudo tee /etc/apt/sources.list.d/tdengine-beta.list
```

**使用 apt-get 命令安装**

```bash
sudo apt-get update
apt-cache policy tdengine
sudo apt-get install tdengine
```

:::tip
apt-get 方式只适用于 Debian 或 Ubuntu 系统
::::
</TabItem>
<TabItem label="Windows 安装" value="windows">           

注意：目前 TDengine 在 Windows 平台上只支持 Windows server 2016/2019 和 Windows 10/11 系统版本。

1. 从列表中下载获得 exe 安装程序；
<PkgListV3 type={3}/>
2. 运行可执行程序来安装 TDengine。

</TabItem>
</Tabs>

:::info
下载其他组件、最新 Beta 版及之前版本的安装包，请点击[发布历史页面](../../releases/tdengine) 
:::

:::note
当安装第一个节点时，出现 Enter FQDN：提示的时候，不需要输入任何内容。只有当安装第二个或以后更多的节点时，才需要输入已有集群中任何一个可用节点的 FQDN，支持该新节点加入集群。当然也可以不输入，而是在新节点启动前，配置到新节点的配置文件中。

:::

## 启动

<Tabs>
<TabItem label="Linux 系统" value="linux">

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

</TabItem>

<TabItem label="Windows 系统" value="windows">

安装后，在 C:\TDengine 目录下，运行 taosd.exe 来启动 TDengine 服务进程。

</TabItem>
</Tabs>

## TDengine 命令行 (CLI)

为便于检查 TDengine 的状态，执行数据库 (Database) 的各种即席(Ad Hoc)查询，TDengine 提供一命令行应用程序(以下简称为 TDengine CLI) taos。要进入 TDengine 命令行，您只要在安装有 TDengine 的 Linux 终端执行 `taos` 即可，也可以在安装有 TDengine 的 Windows 终端的 C:\TDengine 目录下，运行 taos.exe 来启动 TDengine 命令行。

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

除执行 SQL 语句外，系统管理员还可以从 TDengine CLI 进行检查系统运行状态、添加删除用户账号等操作。TDengine CLI 连同应用驱动也可以独立安装在 Linux 或 Windows 机器上运行，更多细节请参考 [这里](../../reference/taos-shell/)

## 使用 taosBenchmark 体验写入速度

启动 TDengine 的服务，在 Linux 或 windows 终端执行 `taosBenchmark` （曾命名为 `taosdemo`）：

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
