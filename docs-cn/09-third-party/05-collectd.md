---
sidebar_label: collectd
title: collectd 写入
---

import CollectD from "../14-reference/_collectd.mdx"
import DeployTaosAdapter from "./_deploytaosadapter.mdx"

collectd 是一款插件式架构的开源监控软件，它可以收集各种来源的指标，如操作系统，应用程序，日志文件和外部设备，并存储此信息或通过网络提供。

将 collectd 采集的数据存在到 TDengine 中可以充分利用 TDengine 对时序数据的高效存储查询性能和集群处理能力。TDengine（2.4.0.0+）包含一个 taosAdapter 独立程序，可以接收包括 collectd 在内的多种应用的数据写入。只需要将 collectd 的配置修改指向 taosAdapter 对应的服务器和端口。

安装 collectd 请参考[官方文档](https://collectd.org/download.shtml)。

<DeployTaosAdapter />
<CollectD />

