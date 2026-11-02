---
sidebar_label: 数据接入与发布
title: 数据接入与发布
description: TDengine 零代码数据写入、数据发布与边云协同概述
---

TDengine 企业版通过 taosExplorer 与 taosX，提供零代码的数据接入与发布能力：把工业协议、消息队列、关系库等外部数据写入 TDengine，再按需把订阅到的数据分发到 MQTT、Kafka 等下游系统，并支持边云协同的分级汇聚。

除 OPC UA、OPC DA、MQTT 等由 taosX 直接连接的协议外，还可通过与 [Neuron](../13-ecosystem-integrations/01-collection/11-neuron.md)、[Kepware](../13-ecosystem-integrations/01-collection/12-kepware.md) 集成接入更多现场协议。Neuron 的南向驱动按协议族计为 70 多种，按 PLC 子系列等细分驱动计为 100 多种；Kepware 提供 150 多种工业协议驱动。购买企业版后，这两款软件的许可证可由涛思数据代为采购，配置见 [接入更多工业协议](./01-no-code-ingestion/index.md#经工业连接平台接入更多工业协议)。

本章包含四部分：

- [零代码数据写入](./01-no-code-ingestion/index.md)：在浏览器中配置数据源、解析与映射，将第三方数据持续写入 TDengine。
- [数据发布](./02-no-code-delivery/index.md)：将 TMQ 订阅数据或只读查询结果发布到 MQTT、Kafka、Parquet 等目标；另含与 Apache Flink 的连接器集成说明。
- [边云协同](./03-dataflow-management/index.md)：边缘侧与中心侧之间基于数据订阅的选择性同步与汇聚。
- [迁移指南](./04-migration-guide/index.md)：从其他时序数据库迁移到 TDengine 的评估、实施和验收流程。
