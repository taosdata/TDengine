---
sidebar_label: Introduction
title: Introduction to TDengine TSDB
description: Introduction to TDengine TSDB
toc_max_heading_level: 4
---

TDengine® TSDB is a high-performance, cluster-open-source, cloud-native time-series database (TSDB). It is designed and optimized for IoT platforms, industrial internet, power, IT operations, and similar scenarios, with strong elastic scalability. Built-in caching, stream processing, and data subscription reduce system design complexity as well as development and operations cost. As a high-performance, distributed IoT and industrial big-data platform, it securely and efficiently consolidates, stores, analyzes, and distributes terabytes to petabytes of data produced daily by massive devices and collectors, supporting real-time monitoring, alerting, and business insight.

In July 2019, Taos Data open-sourced the single-node edition of TDengine; the cluster and cloud-native editions followed in August 2020 and August 2022. After open-sourcing, TDengine drew global developer attention and repeatedly topped GitHub trending charts. For the latest updates, see [tdengine.com](https://tdengine.com/).

## TDengine Offerings

The TDengine industrial data platform includes a time-series database component and an industrial data management component:

- **[TDengine TSDB-OSS](https://tdengine.com/oss/)** is an open-source, cloud-native time-series database. Its source code is licensed under the AGPL and publicly available on GitHub. TDengine TSDB-OSS serves as the code base for our paid offerings and provides the same core functionality. Unlike some open-core products, TDengine TSDB-OSS is a full-featured solution that includes the necessary components for production use, including clustering.
- **[TDengine TSDB-Enterprise](https://tdengine.com/enterprise/)** is a high-performance, scalable time-series database designed for Industry 4.0 and the Industrial IoT. Built on the open-source TDengine TSDB-OSS, it delivers an enterprise-grade feature set tailored to the needs of traditional industries. It can be deployed at the edge, on premises, or on public/private clouds.
- **[TDengine Cloud](https://cloud.tdengine.com)** delivers all features of TDengine TSDB-Enterprise as a fully managed service that can run on Amazon Web Services (AWS), Microsoft Azure, and Google Cloud Platform (GCP). It is especially suitable for small and mid-sized deployments.
- **[TDengine IDMP](https://tdengine.com/idmp/)** is an AI-native industrial data management platform. Combined with TSDB, it delivers visualization, event management, root-cause analysis, and AI insights. See also the [documentation entry](../19-tdengine-idmp/index.md).

## Core Capabilities

TDengine does not depend on third-party software and is not a thin wrapper around existing open-source databases or stream engines. It provides end-to-end time-series consolidation, storage, analysis, and distribution.

- **Data consolidation**: Ingest from MQTT, OPC UA, OPC DA, Kafka, CSV, and traditional historians such as PI System and Wonderware; clean, transform, and load data so that quality is suitable for centralized monitoring and analysis.
- **Data storage**: Efficient columnar storage, two-level compression, and type-aware algorithms deliver much higher compression than general-purpose databases. Time-based partitioning, per-device sharding, and compute–storage separation provide strong horizontal scalability.
- **Data analysis**: Standard SQL plus time-series extensions (such as time-weighted average), nested queries, UDFs, and real-time stream processing. Through JDBC and ODBC, it integrates with Grafana, Power BI, and other visualization, BI, and AI/ML tools.
- **Data distribution**: Data subscription can push a database, a supertable, a set of tables, a single table, or filtered and aggregated results to third-party applications in real time, with fine-grained control and security options such as permissions and encryption.

## What Makes TDengine TSDB Different

Because it fully uses time-series characteristics and the innovative “one table per data collection point”, “super table”, and “virtual table” models, TDengine differs from typical time-series databases in the following ways:

1. **High performance at any scale:** A purpose-built storage engine improves write and query speed and compression. Relative to general-purpose databases, read, write, and compression are typically an order of magnitude better; TSBS benchmarks also show a clear lead over TimescaleDB and InfluxDB. With a distributed, scalable architecture that grows with your business, it can sustain the low latency that real-time visualization and reporting demand.
2. **Efficient data storage:** Multiple compression algorithms can reduce datasets to about one-tenth of raw size. Tiered storage and S3 place data of different ages on appropriate media to lower storage cost, so you can retain more history and still get business insight without excessive spend.
3. **Strong horizontal scalability:** Designed for scale-out from day one, including cloud-native elasticity since 3.0. It can sustain good performance at billion-timeline / hundred-node scale and mitigate high-cardinality challenges.
4. **Zero-code data consolidation:** Built-in connectors for industrial sources such as MQTT, Kafka, OPC, and PI System deliver zero-code ingestion and ETL in a centralized platform that can act as a single source of truth. With light configuration, industrial ETL can run without application code.
5. **Full-stack time-series platform:** With out-of-the-box caching, stream processing, and data subscription, TDengine is more than a store—it includes key components for industrial data processing in one product, accessible through familiar SQL, reducing the need to stitch third-party products together.
6. **Open ecosystem:** Core code is open source; standard SQL and JDBC/ODBC plus multi-language connectors integrate with visualization and AI/BI tools. Industrial interfaces and data subscription reduce vendor lock-in.

## What TDengine TSDB Delivers

With its “one table per device” design, unique supertable and virtual table concepts, and highly optimized storage engine, TDengine TSDB is purpose-built for ingesting, querying, and storing massive time-series datasets. In its role at the core of an industrial data architecture, it provides the following functionality:

1. [Data Ingestion](../04-quick-start/04-write-data.md): Write data with standard SQL or in schemaless mode over the InfluxDB Line Protocol, OpenTSDB Telnet Protocol, and OpenTSDB JSON Protocol. TDengine also integrates with collectors such as Telegraf and Prometheus.
2. [Data Querying](../04-quick-start/05-query-and-aggregate.md): In addition to standard SQL, TDengine includes time-series extensions such as downsampling and windowing, plus functions such as cumulative sum and time-weighted average. It also supports user-defined functions (UDFs) in C or Python.
3. [Read Caching](../05-tdengine-sql/04-data-query/05-cache-query.md): A time-driven first-in, first-out (FIFO) cache keeps the most recent data in memory, so you can quickly read the real-time status of any metric without Redis or similar tools—simplifying architecture and reducing operational cost.
4. [Stream Processing](../06-stream-processing/index.md): The built-in stream engine processes data as it is written, supporting continuous queries and event-driven stream processing. This lightweight solution can return results in milliseconds even under high-throughput ingest.
5. [Data Subscription](../07-data-subscription/index.md): Data subscription is built in, so you do not need a separate message queue. Define topics in SQL—subscribing to a query, a supertable, or a database—and consume them with a Kafka-like API.
6. [Visualization](../13-ecosystem-integrations/02-visual/index.md) and [BI](../13-ecosystem-integrations/03-bi/index.md): Through its REST API and standard JDBC and ODBC interfaces, TDengine integrates with platforms such as Grafana, Power BI, and Seeq.
7. [Clustering](../12-operations-and-tooling/02-operations/03-deployment/index.md): Add nodes to scale capacity; multi-replica technology provides high availability, with Kubernetes support and operational tools for managing robust clusters.
8. Data Migration: Convenient import and export options include script-file and data-file workflows plus [taosdump](../12-operations-and-tooling/04-tools/03-taosdump.md).
9. [Client Libraries](../10-developer-guide/08-connectors-reference/index.md): Client libraries for Java, Python, C/C++, and more let you build applications in your preferred language, with sample code you can adapt quickly.
10. O&M Tools: Use the interactive [taos CLI](../12-operations-and-tooling/04-tools/01-taos-cli.md) to manage clusters, check status, and run ad hoc queries; [taosBenchmark](../12-operations-and-tooling/04-tools/04-taosbenchmark.md) to generate sample data and measure performance; and [TDengine Explorer](../12-operations-and-tooling/03-components/04-explorer.md) to simplify day-to-day operations.
11. [Data Security](https://tdengine.com/security/): TDengine TSDB-Enterprise provides fine-grained user and permission controls, IP whitelisting, audit logs, and encryption in transit and at rest that is transparent to applications with minimal performance impact.
12. [Zero-Code Data Connectors](../08-data-ingest-and-delivery/01-no-code-ingestion/index.md): Enterprise connectors cover industrial protocols such as MQTT and OPC, historians such as AVEVA PI System and Wonderware, relational databases such as Oracle and SQL Server, and other TSDBs such as InfluxDB and OpenTSDB—so you can synchronize or migrate data in the GUI without writing code.

## How TDengine TSDB Benefits You

With its high performance, standard SQL support, and built-in components, TDengine can reduce the total cost of data operations:

1. **Industry-leading performance:** Faster ingest and queries with lower storage use mean fewer CPU and disk resources and lower bills. Because data is written faster, stored more efficiently, and queried more quickly, the same workload typically needs less hardware.
2. **Easy to use with low learning cost:** Standard SQL, third-party integrations, and multi-language client libraries with sample code make TDengine easier to adopt than many specialized TSDB stacks and reduce the need for specialized training.
3. **Simplified, fully integrated solution:** Stream processing, caching, and data subscription are built in at no extra product cost, so you do not need to deploy third-party systems only to process time-series data. The components are purpose-built for time-series workloads and kept simple to operate.

## TDengine TSDB Ecosystem

With its open ecosystem, TDengine lets you build the data stack that fits your business. Standard SQL, zero-code connectors for industrial protocols and data solutions, and integration with visualization, analytics, and BI applications make it straightforward to fit TDengine into existing infrastructure.

![TDengine TSDB Ecosystem](../assets/product-intro-01.png)

As shown in the figure, TDengine TSDB acts as the central source of truth in an industrial data ecosystem, ingesting data from a variety of sources and sharing that data with business applications and stakeholders.

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
