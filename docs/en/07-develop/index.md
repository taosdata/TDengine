---
title: Developer's Guide
description: A guide to help developers quickly get started.
slug: /developer-guide
---

When developing an application and planning to use TDengine as a tool for time-series data processing, here are several steps to follow:

1. **Determine the Connection Method to TDengine**: Regardless of the programming language you use, you can always connect via the REST interface. However, you can also use the dedicated connectors available for each programming language for a more convenient connection.

2. **Define the Data Model Based on Your Application Scenario**: Depending on the characteristics of your data, decide whether to create one or multiple databases; differentiate between static tags and collected data, establish the correct supertable, and create subtables.

3. **Decide on the Data Insertion Method**: TDengine supports standard SQL for data writing, but it also supports Schemaless mode, allowing you to write data directly without manually creating tables.

4. **Determine the SQL Queries Needed**: Based on business requirements, identify which SQL query statements you need to write.

5. **For Lightweight Real-time Statistical Analysis**: If you plan to perform lightweight real-time statistical analysis on time-series data, including various monitoring dashboards, it is recommended to utilize TDengine 3.0’s stream computing capabilities without deploying complex stream processing systems like Spark or Flink.

6. **For Applications Needing Data Consumption Notifications**: If your application has modules that need to consume inserted data and require notifications for new data inserts, it is recommended to use the data subscription feature provided by TDengine, rather than deploying Kafka or other message queue software.

7. **Utilize TDengine’s Cache Feature**: In many scenarios (e.g., vehicle management), if your application needs to retrieve the latest status of each data collection point, it is advisable to use TDengine’s Cache feature instead of deploying separate caching software like Redis.

8. **Use User-Defined Functions (UDFs) if Necessary**: If you find that TDengine's functions do not meet your requirements, you can create user-defined functions (UDFs) to solve your problems.

This section is organized according to the above sequence. For better understanding, TDengine provides example code for each feature and supported programming language, located in the [Example Code](https://github.com/taosdata/TDengine/tree/main/docs/examples). All example codes are verified for correctness through CI, with the scripts located at [Example Code CI](https://github.com/taosdata/TDengine/tree/main/tests/docs-examples-test).

If you wish to delve deeper into SQL usage, refer to the [SQL Manual](../tdengine-reference/sql-manual/). For more information on the use of each connector, please read the [Connector Reference Guide](../tdengine-reference/client-libraries/). If you want to integrate TDengine with third-party systems, such as Grafana, please refer to [Third-party Tools](../third-party-tools/).

If you encounter any issues during development, please click on the "Feedback Issues" link at the bottom of each page to submit an issue directly on GitHub: [Feedback Issues](https://github.com/taosdata/TDengine/issues/new/choose).

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
