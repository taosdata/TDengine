---
sidebar_label: collectd
title: collectd 写入
---

import CollectD from "../14-reference/_collectd.mdx"

collectd 是一款插件式架构的开源监控软件，它可以收集各种来源的指标，如操作系统，应用程序，日志文件和外部设备，并存储此信息或通过网络提供。将 collectd 采集的数据存在到 TDengine 中可以充分利用 TDengine 对时序数据的高效存储和查询能力。安装 collectd 请参考[官方文档](https://collectd.org/download.shtml)。

## 依赖配置

TDengine 新版本（2.4.0.0+）包含一个 taosAdapter 独立程序，可以接收包括 collectd 的多种应用的数据写入。

启动 taosAdapter：

```
systemctl start taosadapter
```

检查 taosAdapter 的运行状态：

```
systemctl status taosadapter
```

<CollectD />

taosAdapter 相关配置参数请参考 `taosadapter --help` 命令输出以及相关文档。
