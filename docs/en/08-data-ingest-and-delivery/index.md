---
sidebar_label: Data Ingest and Delivery
title: Data Ingest and Delivery
description: Overview of data ingestion, delivery, edge-cloud synchronization, and migration in TDengine
---

TDengine provides data ingestion, delivery, edge-cloud synchronization, and migration capabilities. TDengine TSDB-Enterprise provides zero-code ingestion and delivery through taosExplorer and taosX. TDengine Cloud, TDengine TSDB-OSS, and TDengine TSDB-Enterprise can all use migration guides to migrate data and applications.

Beyond the protocols that taosX connects to directly, such as OPC UA, OPC DA, and MQTT, TDengine TSDB-Enterprise can also integrate with [Neuron](../13-ecosystem-integrations/01-collection/11-neuron.md) and [Kepware](../13-ecosystem-integrations/01-collection/12-kepware.md) to ingest more field protocols. Neuron southbound drivers cover more than 70 protocols by protocol family and more than 100 when PLC sub-series and similar drivers are counted separately. Kepware provides more than 150 industrial protocol drivers. Coverage depends on the software version and license. After you purchase TDengine TSDB-Enterprise, Taos Data can procure the licenses for both products. See [More Industrial Protocols](./01-no-code-ingestion/index.md#more-industrial-protocols).

This chapter includes four parts:

- [No-Code Data Ingestion](./01-no-code-ingestion/index.md): Configure data sources, parsing, and mappings in the browser to continuously ingest third-party data into TDengine.
- [No-Code Data Delivery](./02-no-code-delivery/index.md): Publish TMQ subscription data or read-only query results to MQTT, Kafka, Parquet, and more.
- [Dataflow Management](./03-dataflow-management/index.md): Perform selective synchronization and aggregation between edge and central deployments using data subscription.
- [Migration Guides](./04-migration-guide/index.md): Assess, implement, and validate migrations from other time-series databases to TDengine.
