---
sidebar_label: TCollector
title: TCollector 写入
---

import Tcollector from "../14-reference/_tcollector.mdx"

TCollector 是 openTSDB 的一部分，它用来采集客户端日志发送给数据库。将 TCollector 采集的日志数据存在到 TDengine 中可以充分利用 TDengine 对时序数据的高效存储和查询能力。安装 TCollector 请参考[官方文档](http://opentsdb.net/docs/build/html/user_guide/utilities/tcollector.html#installation-of-tcollector)

## 依赖配置

TDengine（2.4.0.0+）包含一个 taosAdapter 独立程序，可以接收包括 TCollector 在内的多种应用的数据写入。只需要将 TCollector 的配置修改指向 taosAdapter 对应的地址和端口即可。taosAdapter 可以和 TDengine 部署在同一个系统中，也可以分离部署，taosAdapter 的详细使用方法请参考相关文档。

启动 taosAdapter：

```
systemctl start taosadapter
```

检查 taosAdapter 的运行状态：

```
systemctl status taosadapter
```

<Tcollector />

taosAdapter 相关配置参数请参考 `taosadapter --help` 命令输出以及相关文档。
