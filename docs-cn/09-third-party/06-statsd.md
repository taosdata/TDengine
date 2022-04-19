---
sidebar_label: StatsD
title: StatsD 直接写入
---

import StatsD from "../14-reference/_statsd.mdx"

StatsD 是汇总和总结应用指标的一个简单的守护进程，近些年来发展迅速，已经变成了一个用于收集应用性能指标的统一的协议。安装 StatsD 请参考[官方文档](https://github.com/statsd/statsd)。

## 依赖配置

TDengine 新版本（2.4.0.0+）包含一个 taosAdapter 独立程序，可以接收包括 Telegraf 在内的多种应用的数据写入。

启动 taosAdapter：

```
systemctl start taosadapter
```

检查 taosAdapter 的运行状态：

```
systemctl status taosadapter
```

<StatsD />

taosAdapter 相关配置参数请参考 `taosadapter --help` 命令输出以及相关文档。

