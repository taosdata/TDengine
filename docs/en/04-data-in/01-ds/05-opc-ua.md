---
sidebar_label: OPC UA
title: OPC UA Data Source
description: This document describes how to establish a connection with your OPC UA server and extract data from OPC into a TDengine Cloud instance.
---

OPC UA data source is the process of writing data from the OPC UA server to the currently selected TDengine Cloud instance via a connection agent.

:::note IMPORTANT

There is an additional charge for extracting OPC UA . The charge depends on your TDengine pricing plan. For more information, [contact our team](https://tdengine.com/contact/) or your account representative.

:::

## Prerequisites

- Create an empty database to store your OPC UA data. For more information, see [Database](../../../programming/model/#create-database).
- Ensure that the connection agent is running on a machine located on the same network as your OPC Server. For more information, see [Install Connection Agent](../install-agent/).

## Procedure

1. In TDengine Cloud, open **Data In** page. On the **Data Sources** tab, click **Add Data Source** button to open the new data source page. In the **Name** input, fill in the name of the data source and select the type of **OPC-UA**, and in the **Agent** selection, select the agent you have already created, or if you have not created a agent, click the **Create New Agent** button to create it.
2. In the **Target DB**, select the database of the current TDengine Cloud instance as the target database.
3. In the **Service endpoint** field, you need to configure the address of the OPC UA server in the format of 127.0.0.1:6666/OPCUA/ServerPath.
4. In the **Authentication** field, select the access method. You can select anonymous access, user name and password access, and certificate access. When using certificate access, you need to configure the certificate file information, private key file information, OPC-UA security protocol and OPC-UA security policy.
5. You can click the **Connectivity Check** button to check whether the communication between the Cloud instance and the OPC UA server is available.
6. In the **Data Sets** field, configure the point information. Click the **Select File** button to select the regular expression to filter the points, and up to 10 points can be filtered out at a time.
7. In the **Collect** part, configure the collection interval (unit: second), the number of points, and the collection mode. The Collect mode can be selected as observe (polling mode) and subscribe (subscription mode), and the default value is subscribe.
8. In the **Advanced Options** part, configure the parallelism, single collection report batch (default value is 1000), report timeout (unit: second, default value is 1), and select logging level. In the **Original Data Save** part, configure whether to enable save. If it is saved, please set the save path and the specific number of days to save.
9. After completing the above information, click the **Add** button to directly initiate data synchronization from the OPC UA server to the TDengine Cloud instance.

## CSV Configurations

The following table describes the name, format, required and detailed description of each column in the OPC UA CSV configuration file. Customers can create the corresponding CSV configuration file according to their actual situation.

| Name                      | Format            | Required? | Description                                                                                                                                                                                                                                           |
| :------------------------ | :---------------- | :-------: | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Point ID                  | point_id          |    Yes    | The OPC-UA node id                                                                                                                                                                                                                                    |
| Data Type                 | type              |    Yes    | The data type of current node, it should of types int, double or other TDengine data types.                                                                                                                                                           |
| STable Name               | stable            |    No     | By default "opc\_&lcub;type&rcub;" is used as a template, for better context, users are encouraged to define their own STable naming.                                                                                                                 |
| Table Name                | tbname            |    Yes    | The table name (child table name under STable)                                                                                                                                                                                                        |
| Enabled                   | enabled           |    No     | To update the node or not. `true` by default.                                                                                                                                                                                                         |
| Timestamp Column Name     | ts_col            |    No     | Use OPC message time as primary timestamp value. Default is the OPC message built-in timestamp.                                                                                                                                                        |
| Received Timestamp Column | received_time_col |    No     | Use taosx collecting time as primary timestamp value. If both ts_col and received_time_col are defined, received_time_col will be used as the timestamp index.                                                                                        |
| Value Column Name         | value_col         |    No     | The value column name for each node(under a STable).                                                                                                                                                                                                  |
| Quality Column Name       | quality_col       |    No     | The quality column name for each node like value.                                                                                                                                                                                                     |
| Additional Tag            | tag::type::name   |    No     | The tag field definition, "tag" is the identity prefix, "type" is TDengine data type, usually use string with VARCHAR, "name" is the tag name in the STable. A full example string is: tag::varchar(64)::note . You can add as many tags as you need. |

:::note IMPORTANT

1. It is recommended to use different STable names for different data types
2. It is recommended to use individual table names for each node.
3. It is recommended to use the received timestamp as primary timestamp value.

:::
