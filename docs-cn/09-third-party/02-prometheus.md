---
sidebar_label: Prometheus
title: Prometheus 远端读取/远端写入
---

Prometheus 是一款流行的开源监控告警系统。Prometheus 的 remote_read 和 remote_write 的接口提供监控数据的远端读写的能力。

TDengine 新版本（2.4.0.0+）包含一个 taosAdapter 独立程序，负责接收包括 Prometheus 在内的多种应用的数据写入。只需要将 remote_read 和 remote_write url 指向 taosAdapter 对应的 url 同时设置 Basic 验证即可使用。

启动 taosAdapter 的命令为 `systemctl start taosadapter`。可以使用 `systemctl status taosadapter` 检查 taosAdapter 的运行状态。

- remote_read url : `http://host_to_taosAdapter:port(default 6041)/prometheus/v1/remote_read/:db`
- remote_write url : `http://host_to_taosAdapter:port(default 6041)/prometheus/v1/remote_write/:db`

Basic 验证：

- username： TDengine 连接用户名
- password： TDengine 连接密码

示例 prometheus.yml 如下：

```yaml
remote_write:
  - url: "http://localhost:6041/prometheus/v1/remote_write/prometheus_data"
    basic_auth:
      username: root
      password: taosdata

remote_read:
  - url: "http://localhost:6041/prometheus/v1/remote_read/prometheus_data"
    basic_auth:
      username: root
      password: taosdata
    remote_timeout: 10s
    read_recent: true
```

即可在 TDengine 中查询 metrics 数据库中 Prometheus 写入的数据。

taosAdapter 相关配置参数请参考 `taosadapter --help` 命令输出以及相关文档。
