---
sidebar_label: 安装部署
title: 安装部署
toc_max_heading_level: 4
---

import InstallationTabs from "../_installation_tabs.mdx";
import StartTabs from "../_start_tabs.mdx";

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

<InstallationTabs />

## 启动

<StartTabs />

## 使用 Systemd drop-in 文件自定义 taosd 启动参数

如果您希望调整 Linux 下 TDengine TSDB 服务的启动重试窗口等参数，我们推荐使用 Systemd drop-in 机制来定制服务启动参数，以避免在软件升级时被覆盖。
下面以将 taosd 服务的启动重试统计窗口改为 60 秒为例，说明如何使用 drop-in 文件。

1. 创建 drop-in 目录
drop-in 目录的路径为`/etc/systemd/system/<unit-name>.service.d/`（对于 taosd 服务，即为`/etc/systemd/system/taosd.service.d/`），用于存放 `.conf` 格式的覆盖配置文件。

```bash
sudo mkdir -p /etc/systemd/system/taosd.service.d
```

2. 编写 override 文件（必须带段标题，文件名任意，以 .conf 结尾）

```bash
sudo tee /etc/systemd/system/taosd.service.d/60-retry.conf >/dev/null <<'EOF'
[Service]
# 将启动重试统计窗口改为 60 秒
StartLimitInterval=60s
EOF
```

3. 重载并重启服务

```bash
sudo systemctl daemon-reload
sudo systemctl restart taosd
```

4. 说明
override 文件仅需写入要改/新增的字段，其余沿用主服务文件。可同时存在多个 .conf 文件，按字典序加载。

## 获取历史版本

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

TDengine 支持 IPv4 和 IPv6 两种通信方式，其中 IPv6 内容参见[网络配置](../network)
