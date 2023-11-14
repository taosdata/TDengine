---
sidebar_label: InfluxDB
title: InfluxDB Data Source
description: This document describes how to extract data from InfluxDB into TDengine Cloud instance.
---
InfluxDB data source is the process of writing data from the InfluxDB server to the currently selected TDengine Cloud instance via a connection agent.

## Prerequisites

- Create an empty database to store your InfluxDB data. For more information, see [Database](../../../programming/model/#create-database).
- Ensure that the connection agent is running on a machine located on the same network as your OPC Server. For more information, see [Install Connection Agent](../install-agent/).

## Procedure

1. In TDengine Cloud, open **Data In** page. On the **Data Sources** tab, click **Add Data Source** button to open the new data source page. In the **Name** input, fill in the name of the data source and select the type of **InfluxDB**, and in the **Agent** selection, select the agent you have already created, or if you have not created a agent, click the **Create New Agent** button to create it.
2. In the **Target DB**, select the database of the current TDengine Cloud instance as the target database.
3. In the **IP Address** input field, enter the address of the InfluxDB server, either an IP address or a domain name, this field is required.
4. In the **Port** input field, enter the InfluxDB server port. By default, InfluxDB listens on port 8086 for HTTP requests and port 8088 for HTTPS requests.
5. In the **Bucket** input field, enter a bucket that needs to be synchronized. Currently, only one bucket can be synchronized to the TDengine database.
6. In the **Measurements** selection, you can select another Measurement to be synchronized, if not, the Measurements contained in the Bucket will be synchronized.
7. In the **Data Begin Time** input field, you can select a start time to synchronize the data by clicking on it, the start time is in UTC time, this field is required; 8. under the **End Time** item, you can select another Measurement to be synchronized.
8. In the **Data End Time** input field, when no end time is specified, the latest data will be synchronized continuously; when an end time is specified, the data will be synchronized only up to this end time; the end time will be in UTC time; this item is an optional field.
9. In the **Authentication** section, you can click **Version 1.x** and **Version 2.x** to switch between different versions of the InfluxDB server version of the data source, and after switching the version, you can fill in the **Version** input box with the specific version.
10. For 1.x version of InfluxDB, please enter the organization ID to be synchronized in the **Organization ID** input field, this field is required; and then enter a token with at least read access to the specified bucket under this organization ID in the **Token** input field, this field is required; for 1.x version of InfluxDB, please fill in the **Organization ID** input field, this field is required; and for 1.x version of InfluxDB, please fill in the **Token** input field, this field is required. InfluxDB version 1.x, please fill in the corresponding user name and password.
11. After completing the above information, click the **Add** button to start the data synchronization from InfluxDB to TDengine Cloud instance directly.
