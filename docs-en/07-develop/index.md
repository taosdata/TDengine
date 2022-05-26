---
title: Developer Guide
---

To develop an application to process time-series data using TDengine, we recommend taking the following steps:

1. Choose the method to connect to TDengine. No matter what programming language you use, you can always use the REST interface to access TDengine, but you can also use connectors unique to each programming language.
2. Design the data model based on your own use cases. Learn the [concepts](/concept/) of TDengine including "one table for one data collection point" and the "super table" (STable) concept; learn about static labels, collected metrics, and subtables. Depending on the characteristics of your data and your requirements, you may decide to create one or more databases, and you should design the STable schema to fit your data.
3. Decide how you will insert data. TDengine supports writing using standard SQL, but also supports schemaless writing, so that data can be written directly without creating tables manually.
4. Based on business requirements, find out what SQL query statements need to be written. You may be able to repurpose any existing SQL.
5. If you want to run real-time analysis based on time series data, including various dashboards, it is recommended that you use the TDengine continuous query feature instead of deploying complex streaming processing systems such as Spark or Flink.
6. If your application has modules that need to consume inserted data, and they need to be notified when new data is inserted, it is recommended that you use the data subscription function provided by TDengine without the need to deploy Kafka.
7. In many use cases (such as fleet management), the application needs to obtain the latest status of each data collection point. It is recommended that you use the cache function of TDengine instead of deploying Redis separately.
8. If you find that the SQL functions of TDengine cannot meet your requirements, then you can use user-defined functions to solve the problem.

This section is organized in the order described above. For ease of understanding, TDengine provides sample code for each supported programming language for each function. If you want to learn more about the use of SQL, please read the [SQL manual](/taos-sql/). For a more in-depth understanding of the use of each connector, please read the [Connector Reference Guide](/reference/connector/). If you also want to integrate TDengine with third-party systems, such as Grafana, please refer to the [third-party tools](/third-party/).

If you encounter any problems during the development process, please click ["Submit an issue"](https://github.com/taosdata/TDengine/issues/new/choose) at the bottom of each page and submit it on GitHub right away.

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
