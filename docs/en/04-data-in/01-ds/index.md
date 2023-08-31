---
sidebar_label: Data Sources
title: Data Sources
description: Data Sources management
---

TDengine Cloud provides a simple and fast one-stop data source management to facilitate users to import data from a lot of data sources into the database of TDengine Cloud instances. Currently supported data sources include TDengine, Legacy TDengine, InfluexDB, MQTT, PI system, OPC system and CSV. Users can migrate data from various data sources to TDengine Cloud instances by simply performing the creation steps in the data in page.

This part of the data writing content is divided into two parts, the management of the data source itself and the management of the connection agent. Except for "TDengine Subscription" and "Old TDengine", all other data sources need to create the corresponding connection proxy first before creating the data source itself.

## Data Sources

Users can add a new data source by clicking **Add Data Source** in the upper right corner. When the creation is complete, users can manage the created data sources in the **Operation** column of the data source list. For each created data source, you can modify, start, pause, refresh and delete the data source.

## Connection Agents

Users can add a new connection agent by clicking the **Create New Agent** button at the top right corner of the connection agent list. Follow the step-by-step instructions to create a new proxy. Once created, users can edit the connection agent in the action area of the connection agent, such as modifying its name, refreshing the generated agent token, and deleting the connection agent.

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
