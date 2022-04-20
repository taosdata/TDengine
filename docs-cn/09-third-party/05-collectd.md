---
sidebar_label: collectd
title: collectd 写入
---

import CollectD from "../14-reference/_collectd.mdx"

collectd 是一款插件式架构的开源监控软件，它可以收集各种来源的指标，如操作系统，应用程序，日志文件和外部设备，并存储此信息或通过网络提供。

只需要将 collectd 的配置指向 taosAdapter 对应的服务器和端口即可将 collectd 采集的数据写入到 TDengine，可以充分利用 TDengine 对时序数据的高效存储查询性能和集群处理能力。

安装 collectd 请参考[官方文档](https://collectd.org/download.shtml)。

## 前置条件

要将 collectd 数据写入 TDengine, 需要几方面的准备工作。
- TDengine 集群已经部署并正在运行
- taosAdapter 已经安装并正在运行, 具体细节请参考 [taosAdapter 的使用手册](/reference/taosadapter)
- collectd 已经安装

<CollectD />

## 验证方法

重启 collectd 后可参考以下示例验证从 collectd 向 TDengine 写入数据并能够正确读出。

