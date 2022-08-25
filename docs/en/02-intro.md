---
sidebar_label: Introduction
title: Introduction to TDengine Cloud Service
---

TDengine Cloud, is the fast, elastic, serverless and cost effective time-series data processing service based on the popular open source time-series database, TDengine. With TDengine Cloud you get the highly optimized and purpose-built for IoT time-series platform, for which TDengine is known. 

This section introduces the major features, competitive advantages, typical use-cases and benchmarks to help you get a high level overview of TDengine.

## Major Features

The major features are listed below:

1. While TDengine supports [using SQL to insert](/develop/insert-data/sql-writing), it also supports [Schemaless writing](/reference/schemaless/) just like NoSQL databases. TDengine also supports standard protocols like [InfluxDB LINE](/develop/insert-data/influxdb-line)，[OpenTSDB Telnet](/develop/insert-data/opentsdb-telnet), [OpenTSDB JSON ](/develop/insert-data/opentsdb-json) among others.
2. TDengine supports seamless integration with third-party data collection agents like [Telegraf](/third-party/telegraf)，[Prometheus](/third-party/prometheus)，[StatsD](/third-party/statsd)，[collectd](/third-party/collectd)，[icinga2](/third-party/icinga2), [TCollector](/third-party/tcollector), [EMQX](/third-party/emq-broker), [HiveMQ](/third-party/hive-mq-broker). These agents can write data into TDengine with simple configuration and without a single line of code. 
3. Support for [all kinds of queries](/develop/query-data), including aggregation, nested query, downsampling, interpolation and others.
4. Support for [user defined functions](/develop/udf).
5. Support for [caching](/develop/cache). TDengine always saves the last data point in cache, so Redis is not needed in some scenarios.
6. Support for [stream processing](../taos-sql).
7. Support for [data subscription](../taos-sql) with the capability to specify filter conditions.
8. High availability is supported by replication including multi-cloud replication. 
9. Provides an interactive [command-line interface](/reference/taos-shell) for management, maintenance and ad-hoc queries.
10. Provides many ways to [get data in](../data-in) and [get data out](../data-out) data.
11. Provides a Dashboard to monitor your running instances of TDengine.
12. Provides [connectors](../connector/) for [Java](../connector/java), [Python](../connector/python), [Go](../connector/go), [Rust](../connector/rust), and [Node.js](../connector/node).
13. Provides a [REST API](/reference/rest-api/).
14. Supports seamless integration with [Grafana](../visual/grafana) for visualization.
15. Supports seamless integration with Google Data Studio.

For more details on features, please read through the entire documentation. 

## Competitive Advantages

By making full use of [characteristics of time series data](https://tdengine.com/tsdb/characteristics-of-time-series-data/), TDengine Cloud differentiates itself from other time series platforms, with the following advantages.

- **[High-Performance](https://tdengine.com/tdengine/high-performance-time-series-database/)**: TDengine Cloud is a fast, elastic, serverless purpose built platform for IoT time-series data. It is the only time-series platform to solve the high cardinality issue to support billions of data collection points while outperforming other time-series platforms for data ingestion, querying and data compression.

- **[Simplified Solution](https://tdengine.com/tdengine/simplified-time-series-data-solution/)**: Through built-in caching, stream processing and data subscription features, TDengine provides a simplified solution for time-series data processing. It reduces system design complexity and operation costs significantly.

- **[Cloud Native](https://tdengine.com/tdengine/cloud-native-time-series-database/)**: Through native distributed design, sharding and partitioning, separation of compute and storage, RAFT, support for kubernetes deployment and full observability, TDengine is a cloud native Time-Series Database and can be deployed on public, private or hybrid clouds. It is Enterprise ready with backup, multi-cloud replication, VPC peering and IP whitelisting.

- **[Ease of Use](https://tdengine.com/tdengine/easy-time-series-data-platform/)**: For administrators, TDengine Cloud provides worry-free operations with a fully managed cloud native solution. For developers, it provides a simple interface, simplified solution and seamless integration with third party tools. For data users, it provides SQL support with powerful time series extensions built for data analytics.

- **[Easy Data Analytics](https://tdengine.com/tdengine/time-series-data-analytics-made-easy/)**: Through super tables, storage and compute separation, data partitioning by time interval, pre-computation and other means, TDengine makes it easy to explore, format, and get access to data in a highly efficient way.
