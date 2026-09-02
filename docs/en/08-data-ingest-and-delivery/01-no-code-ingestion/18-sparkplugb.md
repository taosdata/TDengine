---
title: SparkplugB
sidebar_label: SparkplugB
---

This section describes how to create a data ingestion task in taosExplorer that reads SparkplugB data into the current TDengine cluster.

## Overview

SparkplugB is an open messaging specification designed for Industrial Internet of Things (IIoT) applications and built on MQTT.

TDengine can subscribe to an MQTT broker through the SparkplugB connector and write the data into TDengine in real time.

## Create a Task

### Add a Data Source

On the **Data In** page, click **Add Data Source**.

### Configure Basic Information

Enter a task name, such as `test_spb`, and select **SparkplugB** as the type.

Optionally select an agent, or click **Create New Agent**. Select the target database, or click **Create Database**.

### Configure the Connection and Authentication

In **Brokers**, enter the MQTT broker address, such as `localhost:1883`. Separate multiple brokers with commas.

Select the MQTT protocol version. The default is `5.0`.

Enter the client identifier used to connect to each broker and configure the keep-alive interval. If the broker receives no message from the client during this interval, it closes the connection.

Enter the MQTT username and password if required.

For **TLS Verification**, select one of the following modes:

1. **Disabled**: Do not verify TLS certificates. The connector first attempts a TCP connection and, if that fails, attempts TLS without certificate verification.
2. **One-way authentication**: Use TLS and verify the server certificate. Upload the CA certificate.
3. **Mutual authentication**: Use mutual TLS. Upload the CA certificate, client certificate, and client private key.

Click **Check Connectivity**.

### Configure the Subscription

In **Group ID**, enter the SparkplugB group ID, which typically represents an organization, factory, or production line.

In **Node/Device List**, enter comma-separated nodes and devices. Specify a node by its ID and a device as `{node-id}/{device-id}`.

In **Message Types**, enter comma-separated SparkplugB message types. Supported values are `NBIRTH`, `NDEATH`, `NDATA`, `NCMD`, `DBIRTH`, `DDEATH`, `DDATA`, `DCMD`, and `STATE`. Node message types match nodes in **Node/Device List**, while device message types match devices.

When **Send REBIRTH Command** is enabled, taosX sends the `Node Control/Rebirth` NCMD command to retrieve all node and device metric metadata, including the mapping between metric names and aliases. You can leave this disabled when publishers do not use metric aliases.

### Configure Payload Transformation

#### Parse the Payload

You can obtain sample data by retrieving it from the server, uploading a file, or entering the message body manually. SparkplugB messages use Protocol Buffers, so data retrieved from the server is decoded to JSON. The JSON parser can process fields such as SparkplugB metadata and properties.

Click the magnifying-glass icon to preview the parsed result.

#### Extract or Split Fields

For example, to convert the value of `datatype_str` to a TDengine data type, select the mapping extractor, enter the following JSON in **Rule**, and enter `td_datatype` in **Name**:

```json
{
  "Int8": "TINYINT",
  "UInt8": "TINYINT UNSIGNED",
  "Int16": "SMALLINT",
  "UInt16": "SMALLINT UNSIGNED",
  "Int32": "INT",
  "UInt32": "INT UNSIGNED",
  "Int64": "BIGINT",
  "UInt64": "BIGINT UNSIGNED",
  "Float": "FLOAT",
  "DOUBLE": "DOUBLE",
  "Boolean": "BOOL",
  "String": "VARCHAR(128)",
  "DateTime": "TIMESTAMP"
}
```

For example, this converts `Int8` in `datatype_str` to `TINYINT` in the new `td_datatype` column.

You can add or delete extraction rules and preview their results.

#### Filter Data

Enter a filter expression. For example, `datatype_str != "Int8"` writes only rows whose `datatype_str` value is not `Int8`.

You can delete the rule or preview its result.

#### Map Tables

Select a target supertable or click **Create Supertable**.

If the supertable must be generated dynamically from each message, select **Create Template**. The supertable name, column names, and column types can contain template variables. When data arrives, taosX evaluates the variables, creates a missing supertable, and adds missing columns to an existing supertable.

Configure the target subtable name, such as `t_{id}`, and map source fields to columns and tags. Mapping rules support default values.

Click **Preview** to inspect the mapping result.

### Configure Advanced Options

import AdvancedOptionsMqtt from './resources/_02-advanced-options-mqtt.mdx'

<AdvancedOptionsMqtt />

### Configure Exception Handling

import ExceptionHandling from './resources/_03-exception-handling-strategy.mdx'

<ExceptionHandling />

### Complete the Task

Click **Submit**. The task status is displayed on the **Data Source List** page.
