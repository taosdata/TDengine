---
sidebar_label: Prometheus
title: Prometheus 
---

import Prometheus from "../14-reference/_prometheus.mdx"
import DeployTaosAdapter from "./_deploytaosadapter.mdx"

Prometheus 是一款流行的开源监控告警系统。Prometheus 于2016年加入了 Cloud Native Computing Foundation （云原生云计算基金会，简称 CNCF），成为继 Kubernetes 之后的第二个托管项目，该项目拥有非常活跃的开发人员和用户社区。

Prometheus 提供了 `remote_write` 和 `remote_read` 接口来利用其它数据库产品作为它的存储引擎。为了让 Prometheus 生态圈的用户能够利用 TDengine 的高效写入和查询，TDengine 也提供了对这两个接口的支持。

通过适当的配置， Prometheus 的数据可以通过 `remote_write` 接口存储到 TDengine 中，也可以通过 `remote_read` 接口来查询存储在 TDengine 中的数据，充分利用 TDengine 对时序数据的高效存储查询性能和集群处理能力。

安装 Prometheus 请参考[官方文档](https://prometheus.io/docs/prometheus/latest/installation/)。

## 前置条件

要将 Prometheus 数据写入 TDengine, 需要几方面的准备工作。
- TDengine 集群部署并正在运行
- taosAdapter 安装并正在运行, 具体细节请参考 [taosAdapter 的使用手册](/reference/taosadapter)
- Prometheus 已经安装

## 配置 Prometheus
<Prometheus />


## 验证方法

重启 Prometheus 后可参考以下示例验证从 Prometheus 向 TDengine 写入数据并能够正确读出。

```
这里需要给出 Prometheus 端 写入和查询 TDengine的示例
```