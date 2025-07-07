---
sidebar_label: Ontop
title: 与 Ontop 集成
toc_max_heading_level: 5
---

[Quix](https://quix.io/) 是由 ​Quix​ 公司 (创立于2021年，总部伦敦) 开发，核心团队源自大型实时分布式系统平台 ​Improbable​ 背景。它是一个使用 Python 语言开发的流数据处理平台，专为简化构建和运行实时应用与分析而设计。

它通过易于使用的工具（Pipeline、Topics、Workers），开发者用 ​Python/SQL​ 即可快速构建实时数据流水线，处理海量流数据，连接各类数据源与存储，​无需管理底层设施，大幅缩短开发周期。


## 前置条件

Quix 通过 [Restful](../../../reference/connector/rest-api/) 连接 TDengine 数据源，需准备以下环境：

- TDengine 3.1.0.0 及以上版本集群已部署并正常运行（企业及社区版均可）。
- taosAdapter 能够正常运行，详细参考 [taosAdapter 参考手册](../../../reference/components/taosadapter)。
- Quix ，请 [下载源码](https://github.com/ontop/ontop) 并参照 README 构建。

## 配置步骤


## 验证方法


## 使用示例

### 场景介绍

某小区有 1000 台智能电表设备，需要每秒采集一次电压、电流等数据，并对数据进行清洗、过滤及转化后存储到 TDengine 中。
实现上设备数据采集使用 Kafka 进行，数据清洗、过滤使用 Quix，使用 RESTFUL 写入到 TDengine 中。

### 数据采集

