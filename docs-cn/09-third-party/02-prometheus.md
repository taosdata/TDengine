---
sidebar_label: Prometheus
title: Prometheus 远端读取/远端写入
---

import Prometheus from "../14-reference/_prometheus.mdx"

Prometheus 是一款流行的开源监控告警系统。Prometheus 于2016年加入了 Cloud Native Computing Foundation （云原生云计算基金会，简称 CNCF），成为继 Kubernetes 之后的第二个托管项目，该项目拥有非常活跃的开发人员和用户社区。Prometheus 的数据可以通过 remote_write 接口存储到 TDengine 中，也可以通过 remote_read 接口来查询存储在 TDengine 中的 Prometheus 数据，充分利用 TDengine 对时序数据的高效存储和查询能力。

## 依赖配置

TDengine（2.4.0.0+）包含一个 taosAdapter 独立程序，可以接收包括 Prometheus 在内的多种应用的数据写入。只需要将 Prometheus 的 remote_read 和 remote_write url 指向 taosAdapter 对应的 url 同时设置 Basic 验证即可使用。taosAdapter 可以和 TDengine 部署在同一个系统中，也可以分离部署，taosAdapter 的详细使用方法请参考相关文档。

启动 taosAdapter：

```
systemctl start taosadapter
```

检查 taosAdapter 的运行状态：

```
systemctl status taosadapter
```

<Prometheus />

taosAdapter 相关配置参数请参考 `taosadapter --help` 命令输出以及相关文档。
