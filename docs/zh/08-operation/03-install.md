---
sidebar_label: 安装部署
title: 安装部署
toc_max_heading_level: 4
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

TDengine TSDB 完整的软件包包括服务端（taosd）、应用驱动（taosc）、用于与第三方系统对接并提供 RESTful 接口的 taosAdapter、命令行程序（TDengine CLI）和一些工具软件。目前 TDinsight 仅在 Linux 系统上安装和运行，后续将支持 Windows、macOS 等系统。TDengine 除了提供多种语言的连接器之外，还通过 [taosAdapter](../../reference/components/taosadapter/) 提供 [RESTful 接口](../../reference/connector/rest-api/)。

为方便使用，标准的服务端安装包包含了 taosd、taosAdapter、taosc、taos、taosdump、taosBenchmark、TDinsight 安装脚本和示例代码；如果您只需要用到服务端程序和客户端连接的 C/C++ 语言支持，也可以仅下载 Lite 版本的安装包。

在 Linux 系统上，TDengine 社区版提供 Deb 和 RPM 格式安装包，其中 Deb 支持 Debian/Ubuntu 及其衍生系统，RPM 支持 CentOS/RHEL/SUSE 及其衍生系统，用户可以根据自己的运行环境自行选择。同时我们也提供了 tar.gz 格式安装包，以及 `apt-get` 工具从线上进行安装。

此外，TDengine 也提供 macOS x64/m1 平台的 pkg 安装包。

## 环境要求

在 Linux 系统中，运行环境最低要求如下：
1. Linux 内核版本：3.10.0-1160.83.1.el7.x86_64 或以上
2. glibc 版本：2.17 或以上

如果通过源码编译安装，还需要满足：
1. cmake 版本：3.26.4 或以上
2. gcc 版本：9.3.1 或以上

## 安装
详细安装参考[快速体验](../../get-started/package)。

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
