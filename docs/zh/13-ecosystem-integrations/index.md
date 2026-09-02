---
sidebar_label: 第三方工具
title: 第三方工具
description: TDengine 与数据采集、可视化、BI、数据库管理及物联网平台等第三方工具的集成概述
---

本章介绍如何将 TDengine 与常见第三方工具集成：数据采集与消息队列、可视化、BI / 分析、数据库管理工具，以及物联网平台。多数场景只需完成连接或插件配置，即可读写或展示 TDengine 中的数据。

零代码接入工业协议、消息队列或关系库，或将订阅数据发布到 MQTT、Kafka 等下游，请参阅 [数据接入与发布](../08-data-ingest-and-delivery/index.md)；编程接入与连接器 API 见 [开发指南](../10-developer-guide/index.md)；组件运维（如 taosAdapter）见 [产品组件](../12-operations-and-tooling/03-components/index.md)。

本章包含：

- [数据采集](./01-collection/index.md)：Prometheus、Telegraf、collectd、StatsD、Icinga2、Kafka、Flink、EMQX / HiveMQ 等。
- [可视化](./02-visual/index.md)：Grafana、Perspective 等。
- [数据分析](./03-bi/index.md)：Power BI、Tableau、Superset、Spark、Pandas 等。
- [数据库管理](./04-tool/index.md)：DBeaver、qStudio、JetBrains 等。
- [物联网平台](./05-iot/index.md)：Node-RED、Ignition 等。
