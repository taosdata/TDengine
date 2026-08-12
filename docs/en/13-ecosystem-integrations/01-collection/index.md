---
sidebar_label: Data Collection
title: Integrate with Data Collection Tools
description: Integrate Prometheus, Telegraf, Kafka, Flink, and other collection and stream-processing tools with TDengine
---

import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

This section explains how to integrate monitoring collectors, message queues, and stream-processing frameworks with TDengine through connectors or plugins (including writes and bidirectional sync scenarios such as Kafka Connect).

For zero-code Kafka, MQTT, and other ingestion options in TDengine Enterprise, see [Zero-Code Data Ingestion](../../08-data-ingest-and-delivery/01-no-code-ingestion/index.md). For the Enterprise Flink Source / CDC and Sink connectors, see [Flink](../../08-data-ingest-and-delivery/02-no-code-delivery/02-Flink.md).

<DocCardList items={useCurrentSidebarCategory().items}/>
