---
sidebar_label: icinga2
title: icinga2 写入
---

import Icinga2 from "../14-reference/_icinga2.mdx"

icinga2 是一款开源主机、网络监控软件，最初由 Nagios 网络监控应用发展而来。目前，icinga2 遵从 GNU GPL v2 许可协议发行。

将 icinga2 采集的数据存在到 TDengine 中可以充分利用 TDengine 对时序数据的高效存储查询性能和集群处理能力。

安装 icinga2 请参考[官方文档](https://icinga.com/docs/icinga-2/latest/doc/02-installation/)

## 依赖配置

TDengine（2.4.0.0+）包含一个 taosAdapter 独立程序，可以接收包括 icinga2 在内的多种应用的数据写入，只需要将 Telegraf 的配置修改指向 taosAdapter 对应的服务器和端口即可。taosAdapter 可以和 TDengine 部署在同一个系统中，也可以分离部署，taosAdapter 的详细使用方法请参考相关文档。

启动 taosAdapter：

```
systemctl start taosadapter
```

检查 taosAdapter 的运行状态：

```
systemctl status taosadapter
```

<Icinga2 />

taosAdapter 相关配置参数请参考 `taosadapter --help` 命令输出以及相关文档。
