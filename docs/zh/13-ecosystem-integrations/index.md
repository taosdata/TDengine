---
sidebar_label: 第三方工具
title: 第三方工具
description: TDengine 与数据采集、可视化、BI、数据库管理及物联网平台等第三方工具的集成概述
---

TDengine 支持标准 SQL、常用数据库连接器（如 JDBC）、ORM，以及 InfluxDB Line Protocol、OpenTSDB JSON、OpenTSDB Telnet 等时序写入协议，便于与现有工具链集成。对多数已支持的第三方工具，只需完成连接与插件配置，即可读写或展示 TDengine 中的数据。

若目标是零代码接入工业协议、消息队列或关系库，或把订阅数据发布到 MQTT、Kafka 等下游，请优先参阅 [数据接入与发布](../08-data-ingest-and-delivery/index.md)。编程接入与连接器 API 详见 [开发指南](../10-developer-guide/index.md)；组件运维（如 taosAdapter）详见 [taosAdapter](../12-operations-and-tooling/03-components/03-taosadapter.md)。

本章按集成场景组织：

- [数据采集](./01-collection/index.md)：Prometheus、Telegraf、collectd、StatsD、Kafka Connect、Flink 连接器等。
- [可视化](./02-visual/index.md)：Grafana、Perspective 等。
- [数据分析](./03-bi/index.md)：Power BI、Tableau、Superset、Spark 等 BI / 分析工具。
- [数据库管理](./04-tool/index.md)：DBeaver、qStudio、JetBrains 等。
- [物联网平台](./05-iot/index.md)：Node-RED、Ignition 等。
