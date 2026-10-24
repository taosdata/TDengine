---
sidebar_label: Data Ingest and Delivery
title: Data Ingest and Delivery
description: Overview of data ingestion, delivery, edge-cloud synchronization, and migration in TDengine
---

TDengine provides data ingestion, delivery, edge-cloud synchronization, and migration capabilities. TDengine TSDB-Enterprise provides zero-code ingestion and delivery through taosExplorer and taosX. TDengine Cloud, TDengine TSDB-OSS, and TDengine TSDB-Enterprise can all use migration guides to migrate data and applications.

This chapter includes five parts:

- [No-Code Data Ingestion](./01-no-code-ingestion/index.md): Configure data sources, parsing, and mappings in the browser to continuously ingest third-party data into TDengine.
- [No-Code Data Delivery](./02-no-code-delivery/index.md): Publish TMQ subscription data or read-only query results to MQTT, Kafka, Parquet, and more.
- [Dataflow Management](./03-dataflow-management/index.md): Perform selective synchronization and aggregation between edge and central deployments using data subscription.
- [Edge Gateways](./05-edge-gateway/index.md): Write data already collected by edge gateways such as Neuron into TDengine with one click, without creating an ingestion task.
- [Migration Guides](./04-migration-guide/index.md): Assess, implement, and validate migrations from other time-series databases to TDengine.
