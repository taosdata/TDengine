---
sidebar_label: InfluxDB
title: InfluxDB Data Source
description: This document describes how to extract data from InfluxDB into a TDengine Cloud instance.
---

InfluxDB data source is the process of writing data from the InfluxDB server to the currently selected TDengine Cloud instance via a connection agent.

## Prerequisites

- Create an empty database to store your InfluxDB data. For more information, see [Database](../../../programming/model/#create-database).
- Ensure that the connection agent is running on a machine located on the same network as your OPC Server. For more information, see [Install Connection Agent](../install-agent/).

## Procedure

1. In TDengine Cloud, open **Data In** page. On the **Data Sources** tab, click **Add Data Source** button to open the new data source page. In the **Name** input, fill in the name of the data source and select the type of **InfluxDB**, and in the **Agent** selection, select the agent you have already created, or if you have not created a agent, click the **Create New Agent** button to create it.
2. In the **Target DB**, select the database of the current TDengine Cloud instance as the target database.
3. In the **Protocol** column, configure the connection protocol, which can be configured as WS or WSS.
4. In the **IP address** input field, enter the address of the InfluxDB server, either an IP address or a domain name, this field is required.
5. In the **Port** input field, enter the InfluxDB server port. By default, InfluxDB listens on port 8086 for HTTP requests and port 8088 for HTTPS requests, this field is required.
6. In the **Authentication** section, you can click **1.x version** and **2.x version** to switch between different versions of the InfluxDB server version of the data source, and after switching the version, you can fill in the **Version** input box with the specific version. If it is version 2.x of InfluxDB, please enter the organization ID to be synchronized in the **Organization ID** input field, this field is required; and then enter a token with at least read access to the specified bucket under this organization ID in the **Token** input field, this field is required; if it is version 1.x of InfluxDB, please fill in the corresponding user name and password.
7. You can click the **Connectivity Check** button to check whether the communication between the Cloud instance and the InfluxDB server is available.
8. In the **task** card, you can configure the following information:

   - In the **Bucket** input field, enter a bucket that needs to be synchronized. Currently, only one bucket can be synchronized to the TDengine database, this field is required;
   - In the **Measurements** selection, you can select another Measurement to be synchronized, if not, the Measurements contained in the Bucket will be synchronized;
   - In the **Data Begin Time** input field, you can select a start time to synchronize the data by clicking on it, the start time is in UTC time, this field is required;
   - In the **Data End Time** input field, when no end time is specified, the latest data will be synchronized continuously; when an end time is specified, the data will be synchronized only up to this end time; the end time will be in UTC time; this item is an optional field;
   - In the **Time range per read in minutes** input field, configure the maximum time range when the connector reads data from the source InfluxDB database at a time, this is a very important parameter that needs to be determined by the user in combination with the server performance and data storage density. If the range is too small, the execution speed of the synchronization task will be very slow; if the range is too large, the InfluxDB database system may fail due to excessive memory usage.
   - In the **Delay in seconds** input field, to eliminate the impact of out-of-order data, TDengine always waits for the specified duration here before reading data.

9. In the **Advanced Options** card, you can configure the following information:

   - **Log Level** Configure the log level of the connector, support error, warn, info, debug, trace 5 levels, the default value is info.
   - **Maximum read concurrency** Configure the maximum concurrency when the connector reads data from the source InfluxDB database, the default value is 50.
   - **Maximum write concurrency** Configure the maximum concurrency when the connector writes data to the target TDengine database, the default value is 50.
   - **Batch Size** Configure the maximum batch size when the connector writes data to the target TDengine database, the default value is 5000.

10. After completing the above information, click the **Add** button to start the data synchronization from InfluxDB to TDengine Cloud instance directly.
