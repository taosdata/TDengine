---
sidebar_label: Visual Management
title: Visual Management
toc_max_heading_level: 4
---

taosExplorer is the visual management component for TDengine. It lets administrators inspect cluster status, browse data, configure ingestion, manage streams and subscriptions, and administer users and permissions without relying exclusively on SQL.

## Log In

After installing and starting TDengine, open `http://<IP>:6060/login`, replacing `<IP>` with the address of the taosExplorer host. The navigation menu groups the available functions by task.

For deployment and configuration details, see the [taosExplorer Reference](../03-components/04-explorer.md).

## Monitoring Dashboard

Install the TDengine data source plugin in Grafana, add a TDengine data source, and import the **TDengine for 3.x** dashboard to monitor the cluster and configure alerts. For details, see [Monitoring](./05-monitor.md).

## Programming

The **Programming** page provides runnable write and query examples for Java, Go, Python, Node.js, C#, Rust, R, and other supported languages.

## Data Ingestion

Create ingestion tasks to import data from sources such as AVEVA PI System, OPC-UA/DA, MQTT, Kafka, InfluxDB, OpenTSDB, another TDengine cluster, CSV, and AVEVA Historian. Tasks can include ETL settings and can be started, stopped, edited, deleted, and inspected from the task list.

For details, see [Data Ingestion and Distribution](../../08-data-ingest-and-delivery/index.md).

## Data Browser

The **Data Browser** supports the following operations:

- Create and delete databases, supertables, subtables, and regular tables.
- Browse schemas and table data.
- Execute one or more SQL statements in the SQL editor.
- Save SQL statements as personal favorites and share selected favorites with other users.

Superusers can manage databases from this page. You can create database and table objects either with the visual forms or by executing SQL.

## Stream Processing

Open **Stream Processing** from the navigation menu to create and manage streams. You can use the visual wizard or provide a custom SQL statement. The wizard does not currently support grouping; use custom SQL when grouping or other advanced syntax is required.

For stream concepts and SQL syntax, see [Stream Processing](../../06-stream-processing/index.md).

## Data Subscription

Open **Data Subscription** to create topics with the wizard or custom SQL. The page also lets administrators:

- Share a topic with selected users.
- Inspect consumers and subscriptions.
- Generate connector examples for consuming a topic.
- Choose whether metadata events are included when the topic is used by taosExplorer synchronization tasks.

Do not enable metadata synchronization when an application consumes the topic through a regular connector and expects data records only. For subscription concepts, see [Data Subscription](../../07-data-subscription/index.md).

## Tools

The **Tools** page links to usage guidance for the `taos` shell, taosBenchmark, taosdump, Grafana, Seeq, and supported BI integrations.

## System Management

The **System Management** menu is visible to the `root` user and provides:

- User lifecycle, password, privilege, and whitelist management.
- Import of users and privileges from another TDengine cluster through taosAdapter.
- Slow SQL details and aggregate statistics.
- Backup, restore, and remote synchronization configuration.
- Cluster, license, and proxy information.
