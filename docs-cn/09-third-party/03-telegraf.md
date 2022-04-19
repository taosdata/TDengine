---
sidebar_label: Telegraf
title: Telegraf 写入
---

import Telegraf from "../14-reference/_telegraf.mdx"

Telegraf 是一款十分流行的指标采集开源软件。在数据采集和平台监控系统中，Telegraf 可以采集多种组件的运行信息，而不需要自己手写脚本定时采集，降低数据获取的难度。将 Telegraf 的数据存在到 TDengine 中可以充分利用 TDengine 对时序数据的高效存储和查询能力。

安装 Telegraf 请参考[官方文档](https://docs.influxdata.com/telegraf/v1.22/install/)。

## 依赖配置

TDengine（2.4.0.0+）包含一个 taosAdapter 独立程序，可以接收包括 Telegraf 在内的多种应用的数据写入，只需要将 Telegraf 的配置修改指向 taosAdapter 对应的 url 及其他配置项即可。taosAdapter 可以和 TDengine 部署在同一个系统中，也可以分离部署，taosAdapter 的详细使用方法请参考相关文档。

启动 taosAdapter：

```
systemctl start taosadapter
```

检查 taosAdapter 的运行状态：

```
systemctl status taosadapter
```

<Telegraf />

taosAdapter 相关配置参数请参考 `taosadapter --help` 命令输出以及相关文档。
