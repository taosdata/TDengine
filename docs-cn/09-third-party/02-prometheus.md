---
sidebar_label: Prometheus
title: Prometheus 写入
---

remote_read 和 remote_write 是 Prometheus 数据读写分离的集群方案。
只需要将 remote_read 和 remote_write url 指向 taosAdapter 对应的 url 同时设置 Basic 验证即可使用。

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
