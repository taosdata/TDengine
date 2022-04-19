---
sidebar_label: Telegraf
title: Telegraf 写入
---

import Telegraf from "../14-reference/_telegraf.mdx"

Telegraf 是一款十分流行的指标采集开源软件，将 Telegraf 的数据存在到 TDengine 中可以充分利用 TDengine 对时序数据的高效存储和查询能力。安装 Telegraf 请参考[官方文档](https://portal.influxdata.com/downloads/)。

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

<Telegraf />

taosAdapter 相关配置参数请参考 `taosadapter --help` 命令输出以及相关文档。
