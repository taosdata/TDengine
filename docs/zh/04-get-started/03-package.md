---
sidebar_label: 用安装包快速体验
title: 使用安装包快速体验 TDengine
description: 使用安装包快速体验 TDengine
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import PkgListV3 from "/components/PkgListV3";

您可以[用 Docker 立即体验](../../get-started/docker/) TDengine。如果您希望对 TDengine 贡献代码或对内部实现感兴趣，请参考我们的 [TDengine GitHub 主页](https://github.com/taosdata/TDengine) 下载源码构建和安装.

TDengine 完整的软件包包括服务端（taosd）、应用驱动（taosc）、用于与第三方系统对接并提供 RESTful 接口的 taosAdapter、命令行程序（CLI，taos）和一些工具软件。目前 TDinsight 仅在 Linux 系统上安装和运行，后续将支持 Windows、macOS 等系统。TDengine 除了提供多种语言的连接器之外，还通过 [taosAdapter](../../reference/components/taosadapter/) 提供 [RESTful 接口](../../reference/connector/rest-api/)。

为方便使用，标准的服务端安装包包含了 taosd、taosAdapter、taosc、taos、taosdump、taosBenchmark、TDinsight 安装脚本和示例代码；如果您只需要用到服务端程序和客户端连接的 C/C++ 语言支持，也可以仅下载 Lite 版本的安装包。

在 Linux 系统上，TDengine 社区版提供 Deb 和 RPM 格式安装包，其中 Deb 支持 Debian/Ubuntu 及其衍生系统，RPM 支持 CentOS/RHEL/SUSE 及其衍生系统，用户可以根据自己的运行环境自行选择。同时我们也提供了 tar.gz 格式安装包，以及 `apt-get` 工具从线上进行安装。

此外，TDengine 也提供 macOS x64/m1 平台的 pkg 安装包。

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
<TabItem label="Deb 安装" value="debinst">

1. 从列表中下载获得 Deb 安装包；
   <PkgListV3 type={6}/>
2. 进入到安装包所在目录，执行如下的安装命令：

> 请将 `<version>` 替换为下载的安装包版本

```bash
sudo dpkg -i TDengine-server-<version>-Linux-x64.deb
```

</TabItem>

<TabItem label="RPM 安装" value="rpminst">

1. 从列表中下载获得 RPM 安装包；
   <PkgListV3 type={5}/>
2. 进入到安装包所在目录，执行如下的安装命令：

> 请将 `<version>` 替换为下载的安装包版本

```bash
sudo rpm -ivh TDengine-server-<version>-Linux-x64.rpm
```

</TabItem>

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

<TabItem value="apt-get" label="apt-get">

可以使用 `apt-get` 工具从官方仓库安装。

**配置包仓库**

```bash
wget -qO - http://repos.taosdata.com/tdengine.key | sudo apt-key add -
echo "deb [arch=amd64] http://repos.taosdata.com/tdengine-stable stable main" | sudo tee /etc/apt/sources.list.d/tdengine-stable.list
```

如果安装 Beta 版需要安装包仓库：

```bash
wget -qO - http://repos.taosdata.com/tdengine.key | sudo apt-key add -
echo "deb [arch=amd64] http://repos.taosdata.com/tdengine-beta beta main" | sudo tee /etc/apt/sources.list.d/tdengine-beta.list
```

**使用 `apt-get` 命令安装**

```bash
sudo apt-get update
apt-cache policy tdengine
sudo apt-get install tdengine
```

:::tip
apt-get 方式只适用于 Debian 或 Ubuntu 系统。
:::

</TabItem>
<TabItem label="Windows 安装" value="windows">

**注意**
- 目前 TDengine 在 Windows 平台上只支持 Windows Server 2016/2019 和 Windows 10/11。
- 从 TDengine 3.1.0.0 开始，只提供 Windows 客户端安装包。如果需要 Windows 服务端安装包，请联系 TDengine 销售团队升级为企业版。
- Windows 上需要安装 VC 运行时库，可在此下载安装 [VC运行时库](https://learn.microsoft.com/zh-cn/cpp/windows/latest-supported-vc-redist?view=msvc-170), 如果已经安装此运行库可忽略。

按照以下步骤安装：

1. 从列表中下载获得 exe 安装程序；
   <PkgListV3 type={3}/>
2. 运行可执行程序来安装 TDengine。
Note: 从 3.0.1.7 开始，只提供 TDengine 客户端的 Windows 客户端的下载。想要使用TDengine 服务端的 Windows 版本，请联系销售升级为企业版本。

</TabItem>
<TabItem label="macOS 安装" value="macos">

1. 从列表中下载获得 pkg 安装程序；
   <PkgListV3 type={7}/>
2. 运行可执行程序来安装 TDengine。如果安装被阻止，可以右键或者按 Ctrl 点击安装包，选择 `打开`。

</TabItem>
</Tabs>

:::info
下载其他组件、最新 Beta 版及之前版本的安装包，请点击[发布历史页面](https://docs.taosdata.com/releases/tdengine/)。
:::

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

<TabItem label="Windows 系统" value="windows">

安装后，可以在拥有管理员权限的 cmd 窗口执行 `sc start taosd` 或在 `C:\TDengine` 目录下，运行 `taosd.exe` 来启动 TDengine 服务进程。如需使用 http/REST 服务，请执行 `sc start taosadapter` 或运行 `taosadapter.exe` 来启动 taosAdapter 服务进程。

</TabItem>

<TabItem label="macOS 系统" value="macos">

安装后，在应用程序目录下，双击 TDengine 图标来启动程序，也可以运行 `sudo launchctl start ` 来启动 TDengine 服务进程。


```bash
sudo launchctl start com.tdengine.taosd
sudo launchctl start com.tdengine.taosadapter
sudo launchctl start com.tdengine.taoskeeper
sudo launchctl start com.tdengine.taos-explorer
```

你也可以直接运行 start-all.sh 脚本来启动上面的所有服务
```bash
start-all.sh
```

可以使用  `launchctl` 命令管理上面提到的每个 TDengine 服务，以下示例使用 `taosd` ：

```bash
sudo launchctl start com.tdengine.taosd
sudo launchctl stop com.tdengine.taosd
sudo launchctl list | grep taosd
sudo launchctl print system/com.tdengine.taosd
```

:::info

- `launchctl` 命令管理`com.tdengine.taosd`需要管理员权限，务必在前面加 `sudo` 来增强安全性。
- `sudo launchctl list | grep taosd` 指令返回的第一列是 `taosd` 程序的 PID，若为 `-` 则说明 TDengine 服务未运行。
- 故障排查：
- 如果服务异常请查看系统日志 `launchd.log` 或者 `/var/log/taos` 目录下 `taosdlog` 日志获取更多信息。

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