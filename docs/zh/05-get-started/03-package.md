---
sidebar_label: 安装包
title: 使用安装包立即开始
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

:::info
如果您希望对 TDengine 贡献代码或对内部实现感兴趣，请参考我们的 [TDengine GitHub 主页](https://github.com/taosdata/TDengine) 下载源码构建和安装.

:::

TDengine 开源版本提供 deb 和 rpm 格式安装包，用户可以根据自己的运行环境选择合适的安装包。其中 deb 支持 Debian/Ubuntu 及衍生系统，rpm 支持 CentOS/RHEL/SUSE 及衍生系统。同时我们也为企业用户提供 tar.gz 格式安装包。也支持通过 `apt-get` 工具从线上进行安装。

## 安装

<Tabs>
<TabItem value="apt-get" label="apt-get">
可以使用 apt-get 工具从官方仓库安装。

**安装包仓库**

```
wget -qO - http://repos.taosdata.com/tdengine.key | sudo apt-key add -
echo "deb [arch=amd64] http://repos.taosdata.com/tdengine-stable stable main" | sudo tee /etc/apt/sources.list.d/tdengine-stable.list
```

如果安装 Beta 版需要安装包仓库

```
echo "deb [arch=amd64] http://repos.taosdata.com/tdengine-beta beta main" | sudo tee /etc/apt/sources.list.d/tdengine-beta.list
```

**使用 apt-get 命令安装**

```
sudo apt-get update
apt-cache policy tdengine
sudo apt-get install tdengine
```

:::tip
apt-get 方式只适用于 Debian 或 Ubuntu 系统
::::
</TabItem>
<TabItem label="Deb 安装" value="debinst">

1、从官网下载获得 deb 安装包，例如 TDengine-server-3.0.0.10002-Linux-x64.deb；
2、进入到 TDengine-server-3.0.0.10002-Linux-x64.deb 安装包所在目录，执行如下的安装命令：

```
$ sudo dpkg -i TDengine-server-3.0.0.10002-Linux-x64.deb
Selecting previously unselected package tdengine.
(Reading database ... 119653 files and directories currently installed.)
Preparing to unpack TDengine-server-3.0.0.10002-Linux-x64.deb ...
Unpacking tdengine (3.0.0.10002) ...
Setting up tdengine (3.0.0.10002) ...
Start to install TDengine...

System hostname is: v3cluster-0002

Enter FQDN:port (like h1.taosdata.com:6030) of an existing TDengine cluster node to join
OR leave it blank to build one:

Enter your email address for priority support or enter empty to skip: 
Created symlink /etc/systemd/system/multi-user.target.wants/taosd.service → /etc/systemd/system/taosd.service.

To configure TDengine : edit /etc/taos/taos.cfg
To start TDengine     : sudo systemctl start taosd
To access TDengine    : taos -h v3cluster-0002 to login into TDengine server


TDengine is installed successfully!

```

</TabItem>

<TabItem label="RPM 安装" value="rpminst">

1、从官网下载获得 rpm 安装包，例如 TDengine-server-3.0.0.10002-Linux-x64.rpm；
2、进入到 TDengine-server-3.0.0.10002-Linux-x64.rpm 安装包所在目录，执行如下的安装命令：

```
$ sudo rpm -ivh TDengine-server-3.0.0.10002-Linux-x64.rpm
Preparing...                          ################################# [100%]
Stop taosd service success!
Updating / installing...
   1:tdengine-3.0.0.10002-3           ################################# [100%]
Start to install TDengine...

System hostname is: chenhaoran01

Enter FQDN:port (like h1.taosdata.com:6030) of an existing TDengine cluster node to join
OR leave it blank to build one:

Enter your email address for priority support or enter empty to skip: 
Created symlink from /etc/systemd/system/multi-user.target.wants/taosd.service to /etc/systemd/system/taosd.service.

To configure TDengine : edit /etc/taos/taos.cfg
To start TDengine     : sudo systemctl start taosd
To access TDengine    : taos -h chenhaoran01 to login into TDengine server


TDengine is installed successfully!

```

</TabItem>

<TabItem label="tar.gz 安装" value="tarinst">

1、从官网下载获得 tar.gz 安装包，例如 TDengine-server-3.0.0.10002-Linux-x64.tar.gz；
2、进入到 TDengine-server-3.0.0.10002-Linux-x64.tar.gz 安装包所在目录，先解压文件后，进入子目录，执行其中的 install.sh 安装脚本：

```
$ tar -zxvf TDengine-server-3.0.0.10002-Linux-x64.tar.gz
TDengine-server-3.0.0.10002/
TDengine-server-3.0.0.10002/driver/
TDengine-server-3.0.0.10002/driver/libtaos.so.3.0.0.10002
TDengine-server-3.0.0.10002/driver/vercomp.txt
TDengine-server-3.0.0.10002/release_note
TDengine-server-3.0.0.10002/taos.tar.gz
TDengine-server-3.0.0.10002/install.sh
...

$ ll
total 56832
drwxr-xr-x 3 root root     4096 Aug  8 10:29 ./
drwxrwxrwx 6 root root     4096 Aug  5 16:45 ../
drwxr-xr-x 4 root root     4096 Aug  4 18:03 TDengine-server-3.0.0.10002/
-rwxr-xr-x 1 root root 58183066 Aug  8 10:28 TDengine-server-3.0.0.10002-Linux-x64.tar.gz*

$ cd TDengine-server-3.0.0.10002/

 $ ll
total 51612
drwxr-xr-x  4 root root     4096 Aug  4 18:03 ./
drwxr-xr-x  3 root root     4096 Aug  8 10:29 ../
drwxr-xr-x  2 root root     4096 Aug  4 18:03 driver/
drwxr-xr-x 11 root root     4096 Aug  4 18:03 examples/
-rwxr-xr-x  1 root root    30980 Aug  4 18:03 install.sh*
-rw-r--r--  1 root root     6724 Aug  4 18:03 release_note
-rw-r--r--  1 root root 52793079 Aug  4 18:03 taos.tar.gz

$ sudo ./install.sh

Start to install TDengine...
Created symlink /etc/systemd/system/multi-user.target.wants/taosd.service → /etc/systemd/system/taosd.service.

System hostname is: v3cluster-0002

Enter FQDN:port (like h1.taosdata.com:6030) of an existing TDengine cluster node to join
OR leave it blank to build one:

Enter your email address for priority support or enter empty to skip: 

To configure TDengine : edit /etc/taos/taos.cfg
To configure taosadapter (if has) : edit /etc/taos/taosadapter.toml
To start TDengine     : sudo systemctl start taosd
To access TDengine    : taos -h v3cluster-0002 to login into TDengine server

TDengine is installed successfully!
```

:::info
install.sh 安装脚本在执行过程中，会通过命令行交互界面询问一些配置信息。如果希望采取无交互安装方式，那么可以用 -e no 参数来执行 install.sh 脚本。运行 `./install.sh -h` 指令可以查看所有参数的详细说明信息。

:::

</TabItem>
</Tabs>

:::note
当安装第一个节点时，出现 Enter FQDN：提示的时候，不需要输入任何内容。只有当安装第二个或以后更多的节点时，才需要输入已有集群中任何一个可用节点的 FQDN，支持该新节点加入集群。当然也可以不输入，而是在新节点启动前，配置到新节点的配置文件中。

:::

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

除执行 SQL 语句外，系统管理员还可以从 TDengine CLI 进行检查系统运行状态、添加删除用户账号等操作。TDengine CLI 连同应用驱动也可以独立安装在 Linux 或 Windows 机器上运行，更多细节请参考 [这里](../../reference/taos-shell/)

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
