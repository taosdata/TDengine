---
sidebar_label: Data Ingest and Delivery
title: Data Ingest and Delivery
description: Overview of zero-code ingestion, data publishing, and edge-cloud synchronization in TDengine
---

TDengine TSDB Enterprise provides zero-code data ingest and delivery capabilities through taosExplorer and taosX. You can ingest data from industrial protocols, message queues, and relational databases into TDengine, then publish subscribed data to downstream systems such as MQTT and Kafka, with support for hierarchical edge-cloud aggregation.

This chapter includes three parts:

- [No-Code Data Ingestion](./01-no-code-ingestion/index.md): Configure data sources, parsing, and mappings in the browser to continuously ingest third-party data into TDengine.
- [No-Code Data Delivery](./02-no-code-delivery/index.md): Publish TMQ subscription data or read-only query results to MQTT, Kafka, Parquet, and more.
- [Dataflow Management](./03-dataflow-management/index.md): Perform selective synchronization and aggregation between edge and central deployments using data subscription.
