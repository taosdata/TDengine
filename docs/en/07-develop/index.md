---
title: Developer Guide
description: This document describes how to use the various components of TDengine from a developer's perspective.
---

Before creating an application to process time-series data with TDengine, consider the following:

1. Choose the method to connect to TDengine. TDengine offers a REST API that can be used with any programming language. It also has client libraries for a variety of languages.
2. Design the data model based on your own use cases. Consider the main [concepts](../concept/) of TDengine, including "one table per data collection point" and the supertable. Learn about static labels, collected metrics, and subtables. Depending on the characteristics of your data and your requirements, you decide to create one or more databases and design a supertable schema that fit your data.
3. Decide how you will insert data. TDengine supports writing using standard SQL, but also supports schemaless writing, so that data can be written directly without creating tables manually.
4. Based on business requirements, find out what SQL query statements need to be written. You may be able to repurpose any existing SQL.
5. If you want to run real-time analysis based on time series data, including various dashboards, use the TDengine stream processing component instead of deploying complex systems such as Spark or Flink.
6. If your application has modules that need to consume inserted data, and they need to be notified when new data is inserted, it is recommended that you use the data subscription function provided by TDengine without the need to deploy Kafka.
7. In many use cases (such as fleet management), the application needs to obtain the latest status of each data collection point. It is recommended that you use the cache function of TDengine instead of deploying Redis separately.
8. If you find that the SQL functions of TDengine cannot meet your requirements, then you can use user-defined functions to solve the problem.

This section is organized in the order described above. For ease of understanding, TDengine provides sample code for each supported programming language for each function. If you want to learn more about the use of SQL, please read the [SQL manual](../taos-sql/). For a more in-depth understanding of the use of each client library, please read the [Client Library Reference Guide](../client-libraries/). If you also want to integrate TDengine with third-party systems, such as Grafana, please refer to the [third-party tools](../third-party/).

If you encounter any problems during the development process, please click ["Submit an issue"](https://github.com/taosdata/TDengine/issues/new/choose) at the bottom of each page and submit it on GitHub right away.

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
