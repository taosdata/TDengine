---
sidebar_label: icinga2
title: icinga2 写入
---

import icinga2 from "../14-reference/_icinga2.mdx"

icinga2 是一款开源主机、网络监控软件，最初由 Nagios 网络监控应用发展而来。目前，icinga2 遵从 GNU GPL v2 许可协议发行。安装 icinga2 请参考[官方文档](https://icinga.com/docs/icinga-2/latest/doc/02-installation/)

TDengine 新版本（2.4.0.0+）包含一个 taosAdapter 独立程序，负责接收包括 icinga2 的多种应用的数据写入。

启动 taosAdapter 的命令为 `systemctl start taosadapter`，可以使用 `systemctl status taosadapter` 检查 taosAdapter 的运行状态。

<icinga2 />

taosAdapter 相关配置参数请参考 `taosadapter --help` 命令输出以及相关文档。
