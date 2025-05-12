---
title: Developer's Guide
slug: /developer-guide
---

To develop an application, if you plan to use TDengine as a tool for time-series data processing, there are several things to do:

1. Determine the connection method to TDengine. No matter what programming language you use, you can always use the REST interface, but you can also use connectors unique to each programming language for convenient connections.
2. Based on your application scenario, determine the data model. Depending on the characteristics of the data, decide whether to create one or multiple databases; distinguish between static tags and collected metrics, establish the correct supertables, and create subtables.
3. Decide on the method of inserting data. TDengine supports data insertion using standard SQL, but also supports Schemaless mode insertion, which allows data to be written directly without manually creating tables.
4. Based on business requirements, determine which SQL queries need to be written.
5. If you want to perform lightweight real-time statistical analysis based on time-series data, including various monitoring dashboards, it is recommended to use the streaming computing capabilities of TDengine 3.0, instead of deploying complex streaming computing systems like Spark or Flink.
6. If your application has modules that need to consume inserted data and you want to be notified when new data is inserted, it is recommended to use the data subscription feature provided by TDengine, without the need to deploy Kafka or other messaging queue software.
7. In many scenarios (such as vehicle management), applications need to obtain the latest status of each data collection point, so it is recommended to use TDengine's Cache feature, instead of deploying separate caching software like Redis.
8. If you find that TDengine's functions do not meet your requirements, you can use User Defined Functions (UDF) to solve the problem.

This section is organized in the order mentioned above. For ease of understanding, TDengine provides example code for each feature and each supported programming language, located at [Example Code](https://github.com/taosdata/TDengine/tree/main/docs/examples). All example codes are guaranteed to be correct by CI, scripts located at [Example Code CI](https://github.com/taosdata/TDengine/tree/main/tests/docs-examples-test).
If you want to learn more about using SQL, check out the [SQL Manual](../tdengine-reference/sql-manual/). If you want to learn more about using various connectors, read the [Connector Reference Guide](../tdengine-reference/client-libraries/). If you also want to integrate TDengine with third-party systems, such as Grafana, please refer to [Third-Party Tools](../third-party-tools/).

If you encounter any problems during the development process, please click ["Report Issue"](https://github.com/taosdata/TDengine/issues/new/choose) at the bottom of each page to submit an Issue directly on GitHub.

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
