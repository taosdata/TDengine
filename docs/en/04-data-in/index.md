---
sidebar_label: Data In
title: Data In for TDengine Cloud Instance
description: This document describes the methods by which you can get data in to TDengine Cloud.
---

TDengine Cloud provides three types of data writing methods to write external data into TDengine Cloud instances.

The first one is the data source method, which allows users to import a large number of remote data sources into TDengine Cloud instances through a few easy-to-use steps. Currently, the supported data types include TDengine subscription, InfluxDB, MQTT, PI, OPC-UA and OPC-DA, where TDengine subscription includes TDengine on local host or inside the cloud vendor's servers, as well as other instances of TDengine Cloud.

The second is to write data to TDengine Cloud instances through third-party tools or third-party protocols supported by schema-less REST interfaces. Currently supported third-party proxies include Telegraf and Prometheus, and third-party protocols include InfluxDB, OpenTSDB JSON, and OpenTSDB Telnet. Telnet protocol.

<!-- The third type is to import to a sub-table of a database in a TDengine Cloud instance by means of a CSV file. Note that only one CSV file can be uploaded at a time. -->

In addition to the three ways of writing data to TDengine Cloud instance described in this chapter, users can also use TDengine SQL to write data to TDengine Cloud directly, or programmatically use the [client libraries](../programming/client-libraries/) provided by TDengine to write data to TDengine Cloud instance. TDengine also provides a stress testing tool [taosBenchmark](../tools/taosbenchmark) to write data to instances of TDengine Cloud.

:::note IMPORTANT
Due to permission restrictions, you must first create a database in the cloud service's data browser before you can write to it. This restriction is the first thing all write methods must do.
:::

The main functions are shown below:

1. [Data Sources](./ds/): Writes external data to the TDengine Cloud instance by creating a data source.
2. [Data Collection Agents](./dca/): Patternless through third-party proxy tools and REST interfaces.

<!-- 3. [CSV Files](./csv/): 创建、更新或删除用户或者用户组。您还可以创建/编辑/删除自定义角色。 -->

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
