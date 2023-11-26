---
sidebar_label: Legacy TDengine
title: Legacy TDengine Data Source
description: This document describes how to extract data from a legacy TDengine instance to a TDengine Cloud instance.
---

Legacy TDengine type of data source is to extract data from the edge side TDengine Cloud to the currently selected TDengine Cloud instance through query.

## Prerequisites

- The TDengine server version on the edge side must be version 2.6.
- The server has port 6030 open and allows any IP access.
- The user needs to create the corresponding topic inside the TDengine of the data source, which can be one or more databases, or super or normal tables.

## Procedure

1. In TDengine Cloud, open **Data In** page and on the **Data Sources** tab, click **Add Data Source** to open the page. In the **Name** field, input the name and select the **TDengine Subscription** type.
2. In the **Target DB** field, select a database in the current TDengine Cloud instance as the target database.
3. in the **Host** field, configure the IP or domain name of the remote edge side or the other TDengine Cloud instance URL.
4. Configure the port number for the connection in the **Port** field, the default value is empty.
5. in the **Database** field, configure the name of the database in the edge side TDengine.
6. in the **Protocol** part, configure the connection protocol, the default is Native Connection, can be configured as WS, WSS.
7. In the **Migrate Options** part, configure the migration mode in the **Mode** field, you can choose to migrate historical data (history), real-time data (real-time) or both; in the **Schema** field, configure whether or not to migrate the table structure, you can choose to always migrate (always), not to migrate (none) or to migrate only new (only). you can choose to always migrate (always), not migrate (none), or only migrate new (only).
8. In the **STables** part, you can configure one or more super tables; in the **Tables** field, you can configure sub-tables and normal tables individually, and support table names in the form of tb1 or sub-table names in the form of stable.table. If not configured, all the sub-table data under the configured super-table will be migrated.
9. Configure the start time of the migrated data in the **Start** field, then configure the end time of the migrated data in the **End** field, and finally configure the basic unit of the queried data in the **Unit** field, and the query for a long time range will be cut into multiple queries based on this. You can set 0s, 5s, 1m, etc., and support units ms (milliseconds), s (seconds), m (minutes), h (hours), d (days), M (months), y (years). 10.
10. The configuration in the **Realtime Settings** field is valid only when the migration **Mode** is set to real time synchronization (realtime) mode. In the **Retrospection** field, you can set the retrospection time, which can be set to 5s, 10m, and so on. In the **Interval** field, you can set the actual time of the interval, you can set 5s, 10m, and so on. You can set the time to wait for the query after the disordered data is put into the database in the **Excursion** field, and you can set 500ms, 10s, etc. These three columns can support the unit ms (milliseconds), s (seconds), m (minutes), h (hours), d (days), M (months).
11. In the **Authentication** column, you can configure the user name and password for accessing the TDengine server on the remote edge side, the default value of the user name is root and the default value of the password is taosdata.
12. After completing the above information, click the **Add** button to directly start the data synchronization from the old version of TDengine to the TDengine Cloud instance.
