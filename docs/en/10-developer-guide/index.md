---
sidebar_label: Developer's Guide
title: Developer's Guide
description: Multilingual connection, SQL, parameter binding, schemaless writes, high-throughput writes, UDF, and data subscription programming guide for TDengine
---

If you plan to use TDengine as a time-series data platform for application development, you typically need to complete the following:

1. **Choose a connection method**. Regardless of the programming language, you can access TDengine through the REST API. Most languages also provide dedicated connectors for connecting, writing, and querying from your application.
2. **Design the data model**. Based on the application scenario and data characteristics, decide whether to create one or more databases; distinguish static tags from collected metrics; create the correct supertables; then create child tables as needed.
3. **Choose a write method**. TDengine supports standard SQL writes and parameter-binding writes. It also supports schemaless writes, so you can write data using line protocols directly and reduce the cost of creating tables by hand.
4. **Write query SQL**. Based on business requirements, write the queries you need for statistics, filtering, and analysis.
5. **Perform real-time statistical analysis**. If you need lightweight real-time statistics on time-series data (including monitoring dashboards), prefer TDengine [stream processing](../07-stream-processing/index.md) instead of deploying complex streaming systems such as Spark or Flink.
6. **Consume newly written data**. If modules in your application need to consume written data and be notified when new data arrives, prefer TDengine [data subscription](../06-data-subscription/index.md) instead of deploying Kafka or other message queue software.
7. **Obtain the latest status**. In many scenarios (such as vehicle management), applications need the latest status of each data collection point. Prefer TDengine Cache instead of deploying separate caching software such as Redis.
8. **Extend compute capabilities**. If built-in functions do not meet your needs, use user-defined functions (UDF) to extend compute logic.

This chapter is organized along the development path above. For easier understanding, TDengine provides example code for each feature and supported programming language at [Example Code](https://github.com/taosdata/TDengine/tree/main/docs/examples). Example correctness is covered by CI; the scripts are at [Example Code CI](https://github.com/taosdata/TDengine/tree/main/test/cases/83-DocTest).

This chapter includes:

- [Connecting to TDengine](./01-connect/index.md): Install drivers and connectors; establish WebSocket or native connections.
- [Running SQL Statements](./02-execute-sql.md): Create databases and tables; write and query data.
- [Parameter Binding](./03-stmt.md): High-performance writes with STMT / STMT2.
- [Schemaless Ingestion](./04-schemaless.md): Write with InfluxDB / OpenTSDB and other line protocols.
- [Ingesting Data Efficiently](./05-high-throughput.md): Connector high-throughput features and performance tips.
- [User-Defined Functions (UDF)](./06-udf.md): C / Python user-defined functions.
- [Managing Consumers](./07-subscription-api.md): TMQ consumer APIs and language examples.
- [Client Libraries](./08-connectors-reference/index.md): Language connectors and REST API details.
- [Error Codes](./09-error-codes.md): Client and server error code reference.

For SQL syntax details, see [TDengine SQL](../05-tdengine-sql/index.md). To integrate TDengine with Grafana and other third-party systems, see [Third-Party Tools](../13-ecosystem-integrations/index.md).

If you encounter problems during development, use [Report Issue](https://github.com/taosdata/TDengine/issues/new/choose) at the bottom of each page to submit a GitHub Issue.
