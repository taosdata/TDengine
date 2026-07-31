---
sidebar_label: Introduction
title: Introduction to TDengine TSDB
description: Introduction to TDengine TSDB
toc_max_heading_level: 4
---

TDengine TSDB is a high-performance, cluster-open-source, cloud-native time-series database (TSDB). It is designed and optimized for IoT platforms, industrial internet, power, IT operations, and similar scenarios, with strong elastic scalability. Built-in caching, stream processing, and data subscription reduce system design complexity as well as development and operations cost. As a high-performance, distributed IoT and industrial big-data platform, it securely and efficiently consolidates, stores, analyzes, and distributes terabytes to petabytes of data produced daily by massive devices and collectors, supporting real-time monitoring, alerting, and business insight.

In July 2019, Taos Data open-sourced the single-node edition of TDengine; the cluster and cloud-native editions followed in August 2020 and August 2022. After open-sourcing, TDengine drew global developer attention and repeatedly topped GitHub trending charts. For the latest updates, see [tdengine.com](https://tdengine.com/).

## TDengine TSDB Offerings

TDengine includes open-source TDengine TSDB-OSS, commercial TDengine TSDB-Enterprise, and the managed service TDengine Cloud.

- [TDengine TSDB-OSS](https://tdengine.com/oss/) is an open-source, cloud-native time-series database. Its source code is licensed under the AGPL and publicly available on GitHub. TDengine TSDB-OSS serves as the code base for our paid offerings and provides the same core functionality. Unlike some open-core products, TDengine TSDB-OSS is a full-featured solution that includes the necessary components for production use, including clustering.
- [TDengine TSDB-Enterprise](https://tdengine.com/enterprise/) is a high-performance, scalable time-series database designed for Industry 4.0 and the Industrial IoT. Built on the open-source TDengine TSDB-OSS, it delivers an enterprise-grade feature set tailored to the needs of traditional industries. It can be deployed at the edge, on premises, or on public/private clouds.
- [TDengine Cloud](https://cloud.tdengine.com) delivers all features of TDengine TSDB-Enterprise as a fully managed service that can run on Amazon Web Services (AWS), Microsoft Azure, and Google Cloud Platform (GCP). It is especially suitable for small and mid-sized deployments.

## Core Capabilities

TDengine does not depend on third-party software and is not a thin wrapper around existing open-source databases or stream engines. It provides end-to-end time-series consolidation, storage, analysis, and distribution.

- **Data consolidation**: Ingest from MQTT, OPC UA, OPC DA, Kafka, CSV, and traditional historians such as PI System and Wonderware; clean, transform, and load data so that quality is suitable for centralized monitoring and analysis.
- **Data storage**: Efficient columnar storage, two-level compression, and type-aware algorithms deliver much higher compression than general-purpose databases. Time-based partitioning, per-device sharding, and compute–storage separation provide strong horizontal scalability.
- **Data analysis**: Standard SQL plus time-series extensions (such as time-weighted average), nested queries, UDFs, and real-time stream processing. Through JDBC and ODBC, it integrates with Grafana, Power BI, and other visualization, BI, and AI/ML tools.
- **Data distribution**: Data subscription can push a database, a supertable, a set of tables, a single table, or filtered and aggregated results to third-party applications in real time, with fine-grained control and security options such as permissions and encryption.

## What Makes TDengine TSDB Different

Because it fully uses time-series characteristics and the innovative “one table per data collection point” and “supertable” models, TDengine differs from typical time-series databases in the following ways:

1. **High performance at any scale:** A purpose-built storage engine improves write and query speed and compression. Relative to general-purpose databases, read, write, and compression are typically an order of magnitude better; TSBS benchmarks also show a clear lead over TimescaleDB and InfluxDB.
2. **Efficient data storage:** Multiple compression algorithms can reduce datasets to about one-tenth of raw size. Tiered storage and S3 place data of different ages on appropriate media to lower storage cost.
3. **Strong horizontal scalability:** Designed for scale-out from day one, including cloud-native elasticity since 3.0. It can sustain good performance at billion-timeline / hundred-node scale and mitigate high-cardinality challenges.
4. **Zero-code data consolidation:** Bring industrial sources such as PI System, MQTT, and OPC together with cleaning and transformation. With light configuration, ETL for industrial sources can run without application code.
5. **Full-stack time-series platform:** Built-in caching, stream processing, and data subscription reduce the need to stitch third-party products just to process time-series data.
6. **Open ecosystem:** Core code is open source; standard SQL and JDBC/ODBC plus multi-language connectors integrate with visualization and AI/BI tools. Industrial interfaces and data subscription reduce vendor lock-in.

## What TDengine TSDB Delivers

With its “one table per device” design, unique supertable concept, and optimized storage engine, TDengine TSDB provides the following functionality at the core of an industrial data architecture:

1. [Data Ingestion](../04-quick-start/04-write-data.md): Write with standard SQL or schemaless modes (InfluxDB Line Protocol, OpenTSDB Telnet, OpenTSDB JSON). Integrates with collectors such as Telegraf and Prometheus.
2. [Data Querying](../04-quick-start/05-query-and-aggregate.md): Standard SQL plus time-series extensions (downsampling, windowing, cumulative sum, time-weighted average) and UDFs in C or Python.
3. [Read Caching](../05-tdengine-sql/04-data-query/05-cache-query.md): Time-driven FIFO cache keeps the newest data in memory so real-time status can be read without Redis or similar tools.
4. [Stream Processing](../06-stream-processing/index.md): Built-in stream engine for continuous queries and event-driven processing, with millisecond-level results under high ingest.
5. [Data Subscription](../07-data-subscription/index.md): Define topics in SQL (query, supertable, or database) and consume with a Kafka-like API—no separate message queue required.
6. [Visualization](../13-ecosystem-integrations/02-visual/index.md) and [BI](../13-ecosystem-integrations/03-bi/index.md): REST API, JDBC, and ODBC integrate with Grafana, Power BI, Seeq, and more.
7. [Clustering](../12-operations-and-tooling/02-operations/03-deployment/index.md): Scale out with more nodes, multi-replica HA, Kubernetes, and operational tooling.
8. Data Migration: Script and data-file import/export plus [taosdump](../12-operations-and-tooling/04-tools/03-taosdump.md).
9. [Client Libraries](../10-developer-guide/08-connectors-reference/index.md): Java, Python, C/C++, and more, with sample code you can adapt quickly.
10. O&M Tools: [taos CLI](../12-operations-and-tooling/04-tools/01-taos-cli.md), [taosBenchmark](../12-operations-and-tooling/04-tools/04-taosbenchmark.md), and [TDengine Explorer](../12-operations-and-tooling/03-components/04-explorer.md).
11. [Data Security](https://tdengine.com/security/): Enterprise user/permission controls, IP whitelisting, audit logs, and encryption in transit and at rest.
12. [Zero-Code Data Connectors](../08-data-ingest-and-delivery/01-no-code-ingestion/index.md): Enterprise connectors for MQTT, OPC, AVEVA PI System, Wonderware, Oracle, SQL Server, InfluxDB, OpenTSDB, and more.

## How TDengine TSDB Benefits You

1. **Industry-leading performance:** Faster ingest and queries with lower storage use mean fewer CPU and disk resources and lower bills.
2. **Easy to use:** Standard SQL, third-party integrations, and multi-language client libraries with samples reduce learning cost.
3. **Simplified, fully integrated solution:** Stream processing, caching, and subscription are built in, so you do not need extra products only to process time-series data.

## TDengine TSDB Ecosystem

![TDengine TSDB Ecosystem](../assets/product-intro-01.png)

As shown in the figure, TDengine TSDB acts as the central source of truth in an industrial data ecosystem, ingesting from many sources and sharing data with business applications and stakeholders.

## Typical Application Scenarios

As infrastructure software, TDengine applies wherever machines, devices, or sensors produce data—including IoT, industrial internet, connected vehicles, IT operations, energy, and finance. It is a specialized time-series database and processing tool; because it leans on time-series characteristics, it is not intended for general workloads such as web crawling, social apps, e-commerce, ERP, or CRM. Use the tables below to assess fit.

### Data Source Characteristics

| Data source characteristics | Not suitable | Possibly suitable | Highly suitable | Notes |
| --- | --- | --- | --- | --- |
| Very large overall data volume | | | √ | Strong horizontal scalability and high-compression storage. |
| Occasional or sustained high ingest | | | √ | High write throughput on the same hardware; performance evaluation tools included. |
| Very large number of data sources | | | √ | Optimized for write and query at tens of millions of sources and beyond. |

### System Architecture Requirements

| Architecture requirements | Not suitable | Possibly suitable | Highly suitable | Notes |
| --- | --- | --- | --- | --- |
| Simple, reliable architecture | | | √ | Built-in messaging, cache, stream processing, and monitoring reduce third-party glue. |
| Fault tolerance and high availability | | | √ | Clustering provides HA and disaster recovery. |
| Standards compliance | | | √ | Primary features exposed via standard SQL. |

### System Function Requirements

| Function requirements | Not suitable | Possibly suitable | Highly suitable | Notes |
| --- | --- | --- | --- | --- |
| Full set of industry-specific algorithms built in | | √ | | Common algorithms are included; specialized logic still belongs in the application. |
| Heavy cross-table relational processing | | √ | | Better suited to a relational database, or to TDengine used together with one. |

### System Performance Requirements

| Performance requirements | Not suitable | Possibly suitable | Highly suitable | Notes |
| --- | --- | --- | --- | --- |
| Large overall capacity | | | √ | Scale out with a cluster of servers. |
| High-speed processing | | | √ | IoT-oriented storage and processing usually yield several times the throughput of peers. |
| Fast small-granularity processing | | | √ | Competitive with relational and NoSQL systems on fine-grained work. |

### System Maintenance Requirements

| Maintenance requirements | Not suitable | Possibly suitable | Highly suitable | Notes |
| --- | --- | --- | --- | --- |
| Reliable day-to-day operation | | | √ | Stable architecture and straightforward ops reduce human error. |
| Controllable learning cost for ops | | | √ | Same as above. |
| Large existing talent pool in the market | √ | | | Relatively new category; learning cost is low and vendor training is available. |

Industry examples include:

- [Renewable energy](https://tdengine.com/renewable-energy/)
- [Manufacturing](https://tdengine.com/manufacturing/)
- [Connected cars](https://tdengine.com/connected-cars/)

And application patterns such as:

- [Predictive maintenance](https://tdengine.com/predictive-maintenance/)
- [Vibration analysis](https://tdengine.com/high-frequency-data/)
- [Condition monitoring](https://tdengine.com/condition-monitoring)
