---
sidebar_label: 数据采集
title: 与数据采集类工具的集成
description: 将 Prometheus、Telegraf、Kafka、Flink 等采集与流处理工具接入 TDengine
toc_max_heading_level: 4
---

本节说明如何通过连接器或插件，把监控采集组件、消息队列与流处理框架接入 TDengine（含写入，以及 Kafka Connect 等双向同步场景）。

企业版零代码 Kafka / MQTT 等接入见 [零代码数据写入](../../08-data-ingest-and-delivery/01-no-code-ingestion/index.md)；Flink 企业版 Source / CDC 与 Sink 见 [Flink](../../08-data-ingest-and-delivery/02-no-code-delivery/02-Flink.md)。

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
