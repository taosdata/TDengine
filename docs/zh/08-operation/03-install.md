---
sidebar_label: 安装部署
title: 安装部署
toc_max_heading_level: 4
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import PkgListV37 from "/components/PkgListV37";

TDengine TSDB 完整的软件包包括服务端（taosd）、应用驱动（taosc）、用于与第三方系统对接并提供 RESTful 接口的 taosAdapter、命令行程序（TDengine CLI）和一些工具软件。目前 TDinsight 仅在 Linux 系统上安装和运行，后续将支持 Windows、macOS 等系统。TDengine 除了提供多种语言的连接器之外，还通过 [taosAdapter](../../reference/components/taosadapter/) 提供 [RESTful 接口](../../reference/connector/rest-api/)。

为方便使用，标准的服务端安装包包含了 taosd、taosAdapter、taosc、taos、taosdump、taosBenchmark、TDinsight 安装脚本和示例代码；如果您只需要用到服务端程序和客户端连接的 C/C++ 语言支持，也可以仅下载 Lite 版本的安装包。

在 Linux 系统上，TDengine 社区版提供 Deb 和 RPM 格式安装包，其中 Deb 支持 Debian/Ubuntu 及其衍生系统，RPM 支持 CentOS/RHEL/SUSE 及其衍生系统，用户可以根据自己的运行环境自行选择。同时我们也提供了 tar.gz 格式安装包，以及 `apt-get` 工具从线上进行安装。

此外，TDengine 也提供 macOS x64/m1 平台的 pkg 安装包。

## 环境要求

在 Linux 系统中，运行环境最低要求如下：

1. Linux 内核版本：3.10.0-1160.83.1.el7.x86_64 或以上
2. GLIBC 版本：2.17 及以上 (x64 架构)，2.27 及以上 (ARM 架构 )

如果通过源码编译安装，还需要满足：

1. cmake 版本：3.26.4 或以上
2. gcc 版本：9.3.1 或以上

## 安装

:::note

1. 从 TDengine TSDB 3.0.6.0 开始，不再提供单独的 taosTools 安装包，原 taosTools 安装包中包含的工具都在 TDengine TSDB 服务端安装包中，如果需要请直接下载 TDengine TSDB 服务端安装包。
2. 当安装第一个节点时，出现 `Enter FQDN:` 提示的时候，不需要输入任何内容。只有当安装第二个或以后更多的节点时，才需要输入已有集群中任何一个可用节点的 FQDN，支持该新节点加入集群。当然也可以不输入，而是在新节点启动前，配置到新节点的配置文件中。

:::

<Tabs>

<TabItem label="tar.gz 安装" value="tarinst">
1. 从列表中下载获得 tar.gz 安装包：
    <PkgListV37 productName="TDengine TSDB-Enterprise" version="3.3.8.4" platform="Linux-Generic" pkgType="Server"/>
    <PkgListV37 productName="TDengine TSDB-OSS" version="3.3.8.4" platform="Linux-Generic" pkgType="Server"/>
2. 进入到安装包所在目录，使用 `tar` 解压安装包；
    ```bash
    tar -zxvf tdengine-tsdb-enterprise-3.3.8.4-linux-x64.tar.gz
    ```
3. 进入到安装包所在目录，先解压文件后，进入子目录，执行其中的 install.sh 安装脚本。
    ```bash
    sudo ./install.sh
    ```

:::info
install.sh 安装脚本在执行过程中，会通过命令行交互界面询问一些配置信息。如果希望采取无交互安装方式，那么可以运行 `./install.sh -e no`。运行 `./install.sh -h` 指令可以查看所有参数的详细说明信息。
:::
</TabItem>

<TabItem label="deb 安装" value="debinst">
1. 从列表中下载获得 deb 安装包：
    <PkgListV37 productName="TDengine TSDB-OSS" version="3.3.8.4" platform="Linux-Ubuntu" arch="x64" pkgType="Server"/>
2. 进入到安装包所在目录，执行如下的安装命令：
    ```bash
    sudo dpkg -i tdengine-tsdb-oss-3.3.8.4-linux-x64.deb
    ```
</TabItem>

<TabItem label="rpm 安装" value="rpminst">
1. 从列表中下载获得 rpm 安装包：
    <PkgListV37 productName="TDengine TSDB-OSS" version="3.3.8.4" platform="Linux-Red Hat" arch="x64" pkgType="Server"/>
2. 进入到安装包所在目录，执行如下的安装命令：
    ```bash
    sudo rpm -ivh tdengine-tsdb-oss-3.3.8.4-linux-x64.rpm
    ```
</TabItem>

<TabItem value="apt-get 安装" label="apt-get">
可以使用 `apt-get` 工具从官方仓库安装。

##### 配置包仓库

```bash
wget -qO - http://repos.taosdata.com/tdengine.key | sudo apt-key add -
echo "deb [arch=amd64] http://repos.taosdata.com/tdengine-stable stable main" | sudo tee /etc/apt/sources.list.d/tdengine-stable.list
```

如果安装 Beta 版需要安装包仓库：

```bash
wget -qO - http://repos.taosdata.com/tdengine.key | sudo apt-key add -
echo "deb [arch=amd64] http://repos.taosdata.com/tdengine-beta beta main" | sudo tee /etc/apt/sources.list.d/tdengine-beta.list
```

##### 使用 `apt-get` 命令安装

```bash
sudo apt-get update
apt-cache policy tdengine-tsdb
sudo apt-get install tdengine-tsdb
```

:::tip
apt-get 方式只适用于 Debian 或 Ubuntu 系统。
:::
</TabItem>

<TabItem label="Windows 安装" value="windows">
1. 从列表中下载获得最新 Windows 安装程序：
    <PkgListV37 productName="TDengine TSDB-Enterprise" version="3.3.8.4" platform="Windows" pkgType="Server"/>
2. 运行可执行程序来安装 TDengine。

:::note

- 从 3.0.1.7 版本开始，只提供 TDengine TSDB 客户端的 Windows 客户端的下载。想要使用 TDengine TSDB 服务端的 Windows 版本，请联系 TDengine TSDB 销售团队升级为企业版。
- 目前 TDengine TSDB 在 Windows 平台上只支持 Windows Server 2016/2019 和 Windows 10/11。
- 从 TDengine TSDB 3.1.0.0 开始，只提供 Windows 客户端安装包。如果需要 Windows 服务端安装包，请联系 TDengine TSDB 销售团队升级为企业版。
- Windows 上需要安装 VC 运行时库，可在此下载安装 [VC 运行时库](https://learn.microsoft.com/zh-cn/cpp/windows/latest-supported-vc-redist?view=msvc-170)，如果已经安装此运行库可忽略。

:::
</TabItem>

<TabItem label="macOS 安装" value="macos">
1. 从列表中下载获得最新 macOS 安装包：
    <PkgListV37 productName="TDengine TSDB-OSS" version="3.3.8.4" platform="macOS" pkgType="Server"/>
2. 运行可执行程序来安装 TDengine。如果安装被阻止，可以右键或者按 Ctrl 点击安装包，选择 `打开`。
</TabItem>

</Tabs>

## 启动

<Tabs>
<TabItem label="Linux 系统" value="linux">

安装后，请使用 `systemctl` 命令来启动 TDengine TSDB 的服务进程。

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
- `systemctl stop taosd` 指令在执行后并不会马上停止 TDengine TSDB 服务，而是会等待系统中必要的落盘工作正常完成。在数据量很大的情况下，这可能会消耗较长时间。
- 如果系统中不支持 `systemd`，也可以用手动运行 `/usr/local/taos/bin/taosd` 方式启动 TDengine TSDB 服务。

:::

</TabItem>

<TabItem label="Windows 系统" value="windows">

安装后，可以在拥有管理员权限的 cmd 窗口执行 `sc start taosd` 或在 `C:\TDengine TSDB` 目录下，运行 `taosd.exe` 来启动 TDengine TSDB 服务进程。如需使用 http/REST 服务，请执行 `sc start taosadapter` 或运行 `taosadapter.exe` 来启动 taosAdapter 服务进程。

</TabItem>

<TabItem label="macOS 系统" value="macos">

安装后，在应用程序目录下，双击 TDengine TSDB 图标来启动程序，也可以运行 `sudo launchctl start` 来启动 TDengine TSDB 服务进程。

```bash
sudo launchctl start com.tdengine.taosd
sudo launchctl start com.tdengine.taosadapter
sudo launchctl start com.tdengine.taoskeeper
sudo launchctl start com.tdengine.taos-explorer
```

你也可以直接运行 `start-all.sh` 脚本来启动上面的所有服务

```bash
start-all.sh
```

可以使用 `launchctl` 命令管理上面提到的每个 TDengine TSDB 服务，以下示例使用 `taosd`：

```bash
sudo launchctl start com.tdengine.taosd
sudo launchctl stop com.tdengine.taosd
sudo launchctl list | grep taosd
sudo launchctl print system/com.tdengine.taosd
```

:::info

- `launchctl` 命令管理 `com.tdengine.taosd` 需要管理员权限，务必在前面加 `sudo` 来增强安全性。
- `sudo launchctl list | grep taosd` 指令返回的第一列是 `taosd` 程序的 PID，若为 `-` 则说明 TDengine TSDB 服务未运行。
- 故障排查：
- 如果服务异常请查看系统日志 `launchd.log` 或者 `/var/log/taos` 目录下 `taosdlog` 日志获取更多信息。

:::

</TabItem>
</Tabs>

历史版本可以前往 TDengine 产品下载中心的 [TDengine TSDB-Enterprise](https://www.taosdata.com/download-center?product=TDengine+TSDB-Enterprise&platform=Linux-Generic&type=Server) 页面。

## 目录结构

以 Linux 为例，安装 TDengine TSDB 后，默认会在操作系统中生成下列目录或文件：

| 目录/文件                 | 说明                                                                 |
| ------------------------- | -------------------------------------------------------------------- |
| /usr/local/taos/bin       | TDengine TSDB 可执行文件目录。其中的执行文件都会软链接到/usr/bin 目录下。 |
| /usr/local/taos/driver    | TDengine TSDB 动态链接库目录。会软链接到/usr/lib 目录下。                 |
| /usr/local/taos/examples  | TDengine TSDB 各种语言应用示例目录。                                      |
| /usr/local/taos/include   | TDengine TSDB 对外提供的 C 语言接口的头文件。                             |
| /etc/taos/taos.cfg        | TDengine TSDB 默认`配置文件`                                             |
| /var/lib/taos             | TDengine TSDB 默认数据文件目录。可通过`配置文件`修改位置。                |
| /var/log/taos             | TDengine TSDB 默认日志文件目录。可通过`配置文件`修改位置。                |

## 可执行程序

TDengine TSDB 的所有可执行文件默认存放在 `/usr/local/taos/bin` 目录下。其中包括：

- `taosd`：TDengine 服务端可执行文件
- `taos`：TDengine Shell 可执行文件
- `taosdump`：数据导入导出工具
- `taosBenchmark`: TDengine TSDB 测试工具
- `remove.sh`: 卸载 TDengine TSDB 的脚本，请谨慎执行，链接到`/usr/bin` 目录下的`rmtaos`命令。会删除 TDengine TSDB 的安装目录`/usr/local/taos`，但会保留`/etc/taos`、`/var/lib/taos`、`/var/log/taos`
- `taosadapter`: 提供 RESTful 服务和接受其他多种软件写入请求的服务端可执行文件
- `TDinsight.sh`: 用于下载 TDinsight 并安装的脚本
- `set_core.sh`: 用于方便调试设置系统生成 core dump 文件的脚本
- `taosd-dump-cfg.gdb`: 用于方便调试 taosd 的 gdb 执行脚本。

TDengine 支持 IPv4 和 IPv6 两种通信方式，其中 IPv6 内容参见 (../08-operation/13-network.md)
