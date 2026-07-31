---
sidebar_label: Ecosystem Integrations
title: Ecosystem Integrations
description: Overview of integrating TDengine with data collection, visualization, BI, database management, and IoT tools
---

import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

TDengine supports standard SQL commands, common database connector standards (such as JDBC), ORMs, and other popular time-series database write protocols (such as InfluxDB Line Protocol, OpenTSDB JSON, OpenTSDB Telnet, etc.), making it very easy to use TDengine with third-party tools.

For supported third-party tools, no code is needed; you only need to do simple configuration to seamlessly integrate TDengine with these tools.

For zero-code ingestion from industrial protocols, message queues, or relational databases, or to publish subscription data to downstream systems such as MQTT and Kafka, see [Data Ingestion and Delivery](../08-data-ingest-and-delivery/index.md). For programming interfaces and connector APIs, see the [Developer's Guide](../10-developer-guide/index.md). For component operations, see [taosAdapter](../12-operations-and-tooling/03-components/03-taosadapter.md).

This chapter is organized by integration scenario:

- [Data Collection](./01-collection/index.md): Prometheus, Telegraf, collectd, StatsD, Kafka Connect, Flink connectors, and more.
- [Visualization](./02-visual/index.md): Grafana, Perspective, and more.
- [Analytics](./03-bi/index.md): Power BI, Tableau, Superset, Spark, and other BI and analytics tools.
- [Database Management](./04-tool/index.md): DBeaver, qStudio, JetBrains, and more.
- [IoT Platforms](./05-iot/index.md): Node-RED, Ignition, and more.

<DocCardList items={useCurrentSidebarCategory().items}/>
