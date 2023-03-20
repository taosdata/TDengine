---
sidebar_label: Prometheus
title: Prometheus 远程读取
description: Prometheus 从 TDengine Cloud 远程读取   
---

Prometheus 是一款流行的开源监控告警系统。Prometheus 于2016年加入了 Cloud Native Computing Foundation （云原生云计算基金会，简称 CNCF），成为继 Kubernetes 之后的第二个托管项目，该项目拥有非常活跃的开发人员和用户社区。

Prometheus 提供了 `remote_write` 和 `remote_read` 接口来利用其它数据库产品作为它的存储引擎。为了让 Prometheus 生态圈的用户能够利用 TDengine 的高效写入和查询，TDengine 也提供了对这两个接口的支持。

通过适当的配置， Prometheus 的数据可以通过 `remote_write` 接口存储到 TDengine 中，也可以通过 `remote_read` 接口来查询存储在 TDengine 中的数据，充分利用 TDengine 对时序数据的高效存储查询性能和集群处理能力。

## 安装 Prometheus

请参考 [安装 Prometheus](https://docs.taosdata.com/cloud/data-in/prometheus#install-prometheus).

## 配置 Prometheus

请参考 [配置 Prometheus](https://docs.taosdata.com/cloud/prometheus/#configure-prometheus).

## 开始 Prometheus

请参考 [开始 Prometheus](https://docs.taosdata.com/cloud/data-in/prometheus/#start-prometheus).

## 验证远程读取

通过 Prometheus 服务器可以获取 TDengine Cloud 上面的多个指标。请在浏览器打开 <http://localhost:9090/graph> 并点击 “Graph” 标签页。

输入下面的表达式来画出 Prometheus 每秒在创建数据块数量的趋势图：

```
rate(prometheus_tsdb_head_chunks_created_total[1m])
```

![TDengine prometheus remote_read](prometheus_read.webp)

