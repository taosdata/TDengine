---
sidebar_label: Introduction
title: Introduction to TDengine Cloud Service
description: This document introduces the major features, competitive advantages, and typical use cases of TDengine Cloud.
---

TDengine Cloud, is the fast, elastic, and cost effective time-series data processing service based on the popular open source time-series database, TDengine. With TDengine Cloud you get the highly optimized and purpose-built for IoT time-series platform, for which TDengine is known.

This section introduces the major features, competitive advantages and typical use-cases to help you get a high level overview of TDengine cloud service.

## Major Features

The major features are listed below:

1. [Data In](/advanced-features/data-connectors/)
1. [Data Collection Agents](/third-party-tools/data-collection/).
2. Data Explorer: browse through databases and even run SQL queries once you login.
3. Provides [client libraries](/tdengine-reference/client-libraries/) for Java, Python, Go, Rust, Node.js and other programming languages, including a REST API.
4. [Streams](/advanced-features/stream-processing/): Not only is the continuous query is supported, but TDengine also supports event driven stream processing, so Flink or Spark is not needed for time-series data processing.
5. [Topics](/advanced/subscription/):  Application can subscribe a table or a set of tables. API is the same as Kafka, but you can specify filter conditions and you can share the topic with other users and user groups in TDengien Cloud.
6. [Tools](/tdengine-reference/tools/)
   - Provides an interactive [Command-line Interface (CLI)](/tdengine-reference/tools/tdengine-cli/) for management and ad-hoc queries.
   - Provides a tool [taosBenchmark](/tdengine-reference/tools/taosbenchmark/) for testing the performance of TDengine.
   - Supports exporting data via tool [taosDump](/tdengine-reference/tools/taosdump/).
   - Supports [Grafana](/third-party-tools/visualization/grafana/)
   - Supports [Looker Studio](/third-party-tools/analytics/looker-studio/)
   - Supports writing data to [Prometheus](/third-party-tools/data-collection/prometheus/).
   - Supports [DBeaver](/third-party-tools/management/dbeaver/).
7. [Management](../mgmt)
   - Manage the instance [Users & User Groups](../mgmt/user-mgmt)
   - Support [replication](../mgmt/replication) a database to another region or cloud.
   - Support [backup](../mgmt/backup) the database from the instance.
   - Supports [IP whitelist](../mgmt/ip-whites) for security.
   - Support [operation logs](../mgmt/ops-logs).
8. [Users](../user-mgmt)
   - Manage the [users](../user-mgmt/users) of the current organization.
   - Manage the [user groups](../user-mgmt/usergroups) of the current organization.
   - Manage the [roles](../user-mgmt/roles) of the current organization.
9. [Organizations](../orgs)：management the organization of the current user.
10. [Instances](../instances/)
    - Manage the [Private link](../instances/private-link)
11. [DB Mart](../dbmarts): the published public databases in TDengine Cloud.

For more details on features, please read through the entire documentation.

## Competitive Advantages

By making full use of [characteristics of time series data](https://tdengine.com/tsdb/characteristics-of-time-series-data/) and its cloud native design, TDengine Cloud differentiates itself from other time series data cloud services, with the following advantages.

- **Worry Free**: TDengine Cloud is a fast, elastic, purpose built cloud platform for time-series data. It provides worry-free operations with a fully managed cloud service. You pay as you go.

- **[Simplified Solution](https://tdengine.com/tdengine/simplified-time-series-data-solution/)**: Through built-in caching, stream processing and data subscription features, TDengine provides a simplified solution for time-series data processing. It reduces system design complexity and operation costs significantly.

- **[High-Performance](https://tdengine.com/tdengine/high-performance-time-series-database/)**: It is the only time-series platform to solve the high cardinality issue to support billions of data collection points while outperforming other time-series platforms for data ingestion, querying and data compression.

- **[Ease of Use](https://tdengine.com/tdengine/easy-time-series-data-platform/)**: For administrators, TDengine Cloud provides worry-free operations with a fully managed cloud native solution. For developers, it provides a simple interface, simplified solution and seamless integration with third party tools. For data users, it provides SQL support with powerful time series extensions built for data analytics.

- **[Easy Data Analytics](https://tdengine.com/tdengine/time-series-data-analytics-made-easy/)**: Through super tables, storage and compute separation, data partitioning by time interval, pre-computation and other means, TDengine makes it easy to explore, format, and get access to data in a highly efficient way.

- **Enterprise Ready**: It supports backup, multi-cloud/multi-region database replication, VPC peering and IP whitelisting.

With TDengine cloud, the **total cost of ownership of your time-series data platform can be greatly reduced**.

1. With its built-in caching, stream processing and data subscription, system complexity and operation cost are highly reduced.
2. With SQL support, it can be seamlessly integrated with many third party tools, and learning costs/migration costs are reduced significantly.
3. With the elastic, fully managed service, the operation and maintenance costs are reduced significantly.

## Technical Ecosystem

This is how TDengine would be situated, in a typical time-series data processing platform:

<figure>

![TDengine Database Technical Ecosystem ](eco_system.webp)

<center><figcaption>Figure 1. TDengine Technical Ecosystem</figcaption></center>
</figure>

On the left-hand side, there are data collection agents like OPC-UA, MQTT, Telegraf and Kafka. On the right-hand side, visualization/BI tools, HMI, Python/R, and IoT Apps can be connected. TDengine itself provides an interactive command-line interface and a web interface for management and maintenance.

## Typical Use Cases

As a high-performance and cloud native time-series database, TDengine's typical use case include but are not limited to IoT, Industrial Internet, Connected Vehicles, IT operation and maintenance, energy, financial markets and other fields. TDengine is a purpose-built database optimized for the characteristics of time series data. As such, it cannot be used to process data from web crawlers, social media, e-commerce, ERP, CRM and so on. More generally TDengine is not a suitable storage engine for non-time-series data.
