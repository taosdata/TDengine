---
sidebar_label: StatsD
title: StatsD 直接写入
---

import StatsD from "../14-reference/_statsd.mdx"

StatsD 是汇总和总结应用指标的一个简单的守护进程，近些年来发展迅速，已经变成了一个用于收集应用性能指标的统一的协议。将 StatsD 采集的指标数据存在到 TDengine 中可以充分利用 TDengine 对时序数据的高效存储和查询能力。安装 StatsD 请参考[官方文档](https://github.com/statsd/statsd)。

## 依赖配置

TDengine（2.4.0.0+）包含一个 taosAdapter 独立程序，可以接收包括 StatsD 在内的多种应用的数据写入，只需要将 StatsD 的配置修改指向 taosAdapter 对应的服务器地址和端口接口。taosAdapter 可以和 TDengine 部署在同一个系统中，也可以分离部署，taosAdapter 的详细使用方法请参考相关文档。

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

