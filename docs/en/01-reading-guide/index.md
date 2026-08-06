---
sidebar_label: Reading Guide
title: TDengine TSDB Reading Guide
description: TDengine TSDB user manual
slug: /
---

TDengine® TSDB (hereinafter TDengine) is an [open-source](https://tdengine.com/oss/), high-performance, [cloud-native](https://tdengine.com/) [time-series database](https://tdengine.com/) ([Time-Series Database](https://tdengine.com/time-series-database/), TSDB). It is purpose-built for IoT, connected vehicles, industrial internet, finance, IT operations, and similar scenarios. With built-in caching, stream processing, and data subscription, it greatly reduces system design complexity as well as development and operations cost—a streamlined platform for time-series data. This documentation is the user manual. It covers basic concepts, installation, usage, features, development interfaces, operations and maintenance, and kernel design, and is intended for architects, developers, and system administrators. If you are not yet familiar with the basics, value, and business meaning of time-series data, see [Core Concepts](../03-core-concepts/index.md). You can also get a quick overview from ["What Is a Time-Series Database?"](https://tdengine.com/what-is-a-time-series-database/) and [other articles](https://tdengine.com/time-series-database/) on our official website.

TDengine fully leverages the characteristics of time-series data. It introduced the “one table per data collection point”, “super table”, and “virtual table” models and designed an innovative storage engine that significantly improves write, query, and storage efficiency. To use TDengine correctly, regardless of your role, carefully read [Basic Concepts](../04-quick-start/02-basic-concepts.md).

Depending on your role and interests, you can read the following chapters as needed:

- If you are a developer, carefully read the [Developer's Guide](../10-developer-guide/index.md). It covers connecting to the database, data modeling, writing, querying, stream processing, caching, data subscription, user-defined functions, and more, with sample code in multiple languages. In most cases, you can copy the samples and adapt them slightly for your application. For the REST API and language connectors, see [Connectors Reference](../10-developer-guide/08-connectors-reference/index.md).
- If you are a system administrator and need to understand installation, upgrades, fault tolerance and disaster recovery, cluster deployment and maintenance, data import and export, configuration parameters, health monitoring, and performance tuning, carefully read [Operations and Maintenance](../12-operations-and-tooling/02-operations/index.md). In the era of big data, vertical scaling alone cannot meet continuously growing business demand. Systems generally need horizontal scalability, and clustering has become essential for big data and database systems; the TDengine team not only built clustering but also open-sourced this core capability.
- If you are interested in database kernel design or the open-source implementation, carefully read [Inside TDengine](../15-internals/index.md). It covers distributed architecture, the storage engine, the query engine, data subscription, and the stream processing engine. We recommend reading the source code on GitHub alongside the documentation, and we welcome contributions to the open-source community.
- If you would like to install TDengine and experience its features for yourself, see [Get Started](../04-quick-start/index.md).

TDengine uses SQL as its query language to lower learning and migration cost, and extends SQL for time-series scenarios such as interpolation, downsampling, and time-weighted averages. The [TDengine SQL](../05-tdengine-sql/index.md) chapter details the SQL syntax and lists supported commands and functions.

If you need visualization, event management, root-cause analysis, and AI insights in industrial scenarios, learn more about the platform’s other component [TDengine IDMP](../19-tdengine-idmp/index.md), or visit the [product page for a free trial](https://tdengine.com/idmp/).

TDengine TSDB, including this documentation, is an open-source project, and we welcome contributions from the community. If you find any errors or unclear descriptions, click **Edit this page** at the bottom of the page to submit your corrections. To view the source code, visit our [GitHub repository](https://github.com/taosdata/tdengine).

Together, we make a difference!
