---
sidebar_label: TCollector
title: TCollector 写入
---

import tcollector from "../14-reference/_tcollector.mdx"

TCollector 是 openTSDB 的一部分，它用来采集客户端日志发送给数据库。安装 TCollector 请参考[官方文档](http://opentsdb.net/docs/build/html/user_guide/utilities/tcollector.html#installation-of-tcollector)

TDengine 新版本（2.4.0.0+）包含一个 taosAdapter 独立程序，负责接收包括 TCollector 的多种应用的数据写入。

启动 taosAdapter 的命令为 `systemctl start taosadapter`，可以使用 `systemctl status taosadapter` 检查 taosAdapter 的运行状态。

<tcollector />

taosAdapter 相关配置参数请参考 `taosadapter --help` 命令输出以及相关文档。
