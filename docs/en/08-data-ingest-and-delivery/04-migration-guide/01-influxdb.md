---
title: Migrate from InfluxDB to TDengine
sidebar_label: InfluxDB
description: Migrate InfluxDB 1.x data, write paths, and query applications to TDengine
toc_max_heading_level: 4
---

This guide describes how to migrate InfluxDB 1.x data and applications to TDengine. It covers TDengine Cloud, TDengine TSDB-OSS, and TDengine TSDB-Enterprise.

Migration is more than a one-time data import. To prevent gaps in historical data, data written during migration, and application queries, assess the source, design the target schema, protect incremental data, backfill history, validate data, and then cut over reads and writes.

> The InfluxDB Line Protocol mapping in this guide applies to schemaless ingestion. For full protocol syntax, type inference, and limits, see [Schemaless Ingestion](../../10-developer-guide/04-schemaless.md).

## Migration Workflow and Target Product Forms

Use the following low-risk workflow:

1. Inventory the source data model, write paths, and key queries, and define migration boundary `T0`.
2. Create the target database and validate the data model with representative data.
3. From `T0`, enable application dual writes or continuous synchronization to protect new data.
4. Backfill historical data before `T0`.
5. Validate data by time window, stop source writes, and complete the final catch-up.
6. Gradually switch read requests to TDengine SQL, then retire the source path after an observation period.

The SQL, data model, and validation principles are shared across target product forms, but historical migration and real-time ingestion differ.

| Target product form | Historical migration | Real-time ingestion | Suitable when |
| --- | --- | --- | --- |
| TDengine Cloud | Use a Cloud InfluxDB data source and connection agent; migrate a bounded range or synchronize continuously | Use the Cloud InfluxDB Line Protocol endpoint and a Cloud Token | You want a managed service and the agent can reach the source InfluxDB instance |
| TDengine TSDB-OSS | Export Line Protocol from the source, then ingest it through a self-managed `taosAdapter` | Use the InfluxDB v1 endpoint of a self-managed `taosAdapter` | You use the open-source edition and can operate the target cluster and export path |
| TDengine TSDB-Enterprise | Create an InfluxDB data source task in `taosExplorer` using `taosX`; continuous synchronization is supported | Use the InfluxDB v1 endpoint of a self-managed `taosAdapter` | You need visual task management, resumable progress, or private deployment |

## Pre-migration Assessment and Target Preparation

### Inventory the Source

Record the following information as a migration acceptance baseline:

| Area | What to confirm |
| --- | --- |
| Data scope | Databases, retention policies, earliest and latest timestamps, historical volume, daily increment, and cutoff `T0` |
| Data model | Measurements, tags, fields, field types, null ratios, and tag cardinality |
| Time semantics | Write precision, timezone handling, out-of-order range, and maximum late-data delay |
| Write paths | Applications, collectors, batch size, retry behavior, authentication, and whether dual writes are possible |
| Read paths | Key InfluxQL queries, reports, alerts, APIs, and the acceptable cutover window |
| Source capabilities | Whether Line Protocol can be exported directly; otherwise, whether the service provider has a backup or migration procedure |

For each measurement, select a small time window as a pilot. Include multiple tags, numeric and string fields, null values, and nanosecond timestamps.

### Create the Target Database

InfluxDB data can use second, millisecond, microsecond, or nanosecond precision. Use a nanosecond-precision target database to avoid precision loss:

```sql
CREATE DATABASE migration_db PRECISION 'ns';
```

Before production migration, also:

- Create separate least-privilege credentials for migration jobs and applications.
- Verify network connectivity from migration hosts, connection agents, or `taosX-Agent` to both source and target.
- Define `T0`, the historical cutoff, and the final cutover window.
- Allocate separate storage for exports, failed batches, validation results, and migration logs.
- Complete one full pilot migration and query regression in a non-production database.

## Data Model and Write Semantics

When you ingest InfluxDB Line Protocol, TDengine maps concepts as follows:

| InfluxDB concept | TDengine concept |
| --- | --- |
| measurement | Supertable name |
| tag key/value | Tag column; tag values are converted to `NCHAR` |
| field key/value | Regular column; types are inferred from suffixes or quotation marks |
| timestamp | Primary-key timestamp, named `_ts` by default |
| measurement and tag set | Subtable; the default name is an MD5 hash of sorted tags |

For example:

```text
cpu,host=server01,region=cn-beijing usage=42.5,load=2i,status="ok" 1704067200000000000
```

creates or uses the `cpu` supertable, writes `host` and `region` as tags, writes `usage`, `load`, and `status` as regular columns, and stores the trailing nanosecond timestamp in `_ts`.

Confirm the following semantics before migration:

- Conflicting types for the same field cause write failures; normalize or split those fields first.
- Missing fields or tags are written as `NULL`; schemaless ingestion can add columns but does not remove existing ones.
- Automatically generated subtable names are not readable. Validate naming settings during a pilot if applications need readable names.
- A row cannot exceed 64 KB, and all tag values in a row cannot exceed 16 KB. Split or transform oversized data at the source.
- Supertable and subtable names are case-sensitive. Use consistent names in migration jobs, applications, and SQL.
- Do not manually create a same-named supertable with a different schema before the first schemaless write.

## Protect Incremental Data

An export or backup only covers data that existed when it began. It does not automatically include data written during migration. Protect incremental data before historical backfill begins.

Use one of these approaches:

- **Application dual writes**: From `T0`, write to both source InfluxDB and TDengine. Keep durable retries or a persistent queue for unconfirmed data.
- **Continuous synchronization**: An InfluxDB data source task in TDengine Cloud or TDengine TSDB-Enterprise can continuously read new data when no end time is specified. Monitor task delay and errors.
- **Collector dual output**: A collector that supports multiple outputs can write to both systems. Monitor failures independently for both outputs.

Record `T0` regardless of the method. Historical migration processes data before `T0`; the incremental path processes data at and after `T0`.

## Migrate Historical Data

For every path, first migrate one measurement for a small time window. Verify the model, record counts, and time boundaries before expanding the scope.

### TDengine Cloud

Create an InfluxDB data source in TDengine Cloud and use a connection agent that can reach the source InfluxDB instance.

1. Create a nanosecond-precision target database.
2. Configure the connection agent and a read-only InfluxDB 1.x account.
3. Run the connectivity check.
4. Select measurements and a start time. Set an end time for historical migration, or omit it for continuous synchronization.
5. Tune the time range per read based on source load and data density.
6. Set a delay that covers expected out-of-order or late data, then monitor delay and errors.

For configuration details, see [InfluxDB Data Source](https://docs.taosdata.com/cloud/data-in/ds/influx/).

### TDengine TSDB-Enterprise

Create an InfluxDB data source task in `taosExplorer`; `taosX` reads the source and writes to TDengine.

1. Select a nanosecond-precision target database.
2. Configure the source InfluxDB 1.x address, user, and password, then run the connectivity check.
3. Select measurements, a start time, and optionally an end time.
4. Set the time range per read and delay according to source performance.
5. Monitor progress, delay, and errors. Tasks resume from saved progress after a pause, restart, or automatic recovery.

For form fields and parameters, see [InfluxDB](../01-no-code-ingestion/09-influxdb.md). When the target cannot directly reach the source, deploy `taosX-Agent` as a connection agent.

### TDengine TSDB-OSS

`taosAdapter` accepts writes but does not read InfluxDB. Export InfluxDB Line Protocol from the source, then ingest it into TDengine in batches.

If the source has a local InfluxDB data directory, use an export tool that matches its version. Validate the exported output with a small time window first.

For a managed source without a local data directory, use the provider's export procedure. For example, the [Alibaba Cloud TSDB for InfluxDB migration procedure](https://help.aliyun.com/zh/document_detail/2972630.html) backs up and restores to a self-managed InfluxDB relay, then uses `influx_inspect export -lponly` to export Line Protocol. Provider-specific prerequisites, ports, and resource requirements can change; follow the provider's current documentation.

When importing:

1. Split exports by measurement and time window, and include the time range in file names.
2. Import a small batch first, then verify table structure, time precision, and field types with SQL.
3. Record measurement, time range, row count, and validation result for successful batches.
4. Keep failed source files, fix their data, and retry only failed batches.
5. Keep the incremental-data protection path running throughout historical backfill.

For ingestion parameters and limits, see [taosAdapter](../../12-operations-and-tooling/03-components/03-taosadapter.md) and [Schemaless Ingestion](../../10-developer-guide/04-schemaless.md).

## Switch Real-time Writes

### TDengine Cloud

The TDengine Cloud InfluxDB Line Protocol endpoint has this format:

```text
POST <TDENGINE_CLOUD_URL>/influxdb/v1/write?db=<TDENGINE_DATABASE>&token=<TDENGINE_CLOUD_TOKEN>&precision=ns
```

Supply the Cloud Token through a secret manager, environment variables, or runtime injection. Do not place it in source code, script repositories, or logs. For endpoint and Token retrieval, see [InfluxDB Line Protocol](https://docs.taosdata.com/cloud/data-in/dca/schemaless-influxdb/).

For example:

```shell
curl --request POST \
  "$TDENGINE_CLOUD_URL/influxdb/v1/write?db=$TDENGINE_DATABASE&token=$TDENGINE_CLOUD_TOKEN&precision=ns" \
  --data-binary 'cpu,host=server01,region=cn-beijing usage=42.5,load=2i,status="ok" 1704067200000000'
```

### TDengine TSDB-OSS and TDengine TSDB-Enterprise

The InfluxDB v1 write endpoint of a self-managed `taosAdapter` is:

```text
POST http://<TAOS_ADAPTER_HOST>:6041/influxdb/v1/write?db=<TDENGINE_DATABASE>&precision=ns
```

It supports HTTP Basic Auth and the `u` and `p` URL parameters. TDengine TSDB-Enterprise also supports `Authorization: Bearer <token>`, where the token is created with `CREATE TOKEN`. This is a TDengine Bearer Token, not an InfluxDB token.

```shell
curl --request POST \
  "http://<TAOS_ADAPTER_HOST>:6041/influxdb/v1/write?db=<TDENGINE_DATABASE>&precision=ns" \
  --user "<TDENGINE_USER>:<TDENGINE_PASSWORD>" \
  --data-binary 'cpu,host=server01,region=cn-beijing usage=42.5,load=2i,status="ok" 1704067200000000'
```

First validate the data in a non-production database: it should create the expected supertable, tags, and regular columns. Then enable dual writes and monitor write failure rate, end-to-end latency, and type conflicts. Gradually increase TDengine write traffic after dual writes stabilize, and retain the old path through the observation period.

## Validate, Cut Over, and Roll Back

Validate by measurement and time window after historical migration, continuous synchronization, or dual writes. A whole-database row-count comparison alone cannot prove that time boundaries and critical fields are correct.

| Check | Recommended method |
| --- | --- |
| Time boundaries | Compare minimum and maximum timestamps for each window |
| Data volume | Compare counts by measurement, tag range, or business dimension |
| Field values | Sample field values, tag values, and nulls at selected timestamps |
| Aggregations | Compare representative `COUNT`, `MIN`, `MAX`, and `AVG` results |
| Write quality | Compare failure rate, retries, and end-to-end latency during dual writes |
| Query behavior | Regress key reports, alerts, and APIs, including window, fill, and latest-value semantics |

Use left-closed, right-open intervals such as `[_start, _end)` for adjacent validation windows.

During the cutover window:

1. Stop source writes, or prevent new writes from entering the source.
2. Wait for dual-write queues or continuous synchronization to finish the last incremental window.
3. Validate the final window.
4. Gradually switch reads to TDengine SQL.
5. Observe business results, alerts, and key metrics.
6. Retire the source read and write paths after the observation period has no unresolved differences.

If type conflicts, time-precision errors, excessive delay, or inconsistent query results occur, stop expanding the rollout and switch reads and writes back to the source. Replay unconfirmed data from durable queues, write logs, or failed batches, then validate again.

## Convert InfluxQL Queries

Read requests must be converted from InfluxQL to TDengine SQL. Common mappings are:

| InfluxQL intent | TDengine SQL direction |
| --- | --- |
| Time range | `WHERE _ts >= ... AND _ts < ...` |
| Tag condition | `WHERE tag_name = 'value'` |
| `MEAN`, `MAX`, `COUNT` | `AVG`, `MAX`, `COUNT` |
| `GROUP BY time(1m)` | `INTERVAL(1m)` |
| Group by tag | `PARTITION BY tag_name` or `GROUP BY tag_name`, according to query semantics |
| `LAST(field)` | `LAST(field)` or `LAST_ROW(*)` |
| `fill(null/previous/linear)` | `FILL(NULL/PREV/LINEAR)` |

### Time Range, Tag Filter, and Window Aggregation

InfluxQL:

```sql
SELECT MEAN("usage")
FROM "cpu"
WHERE time >= '2024-01-01T00:00:00Z'
  AND time < '2024-01-01T00:02:00Z'
  AND "host" = 'server01'
GROUP BY time(1m), "host"
```

TDengine SQL:

```sql
SELECT _wstart AS window_start, host, AVG(usage) AS avg_usage
FROM migration_db.cpu
WHERE _ts >= '2024-01-01 00:00:00.000000000'
  AND _ts < '2024-01-01 00:02:00.000000000'
  AND host = 'server01'
PARTITION BY host
INTERVAL(1m);
```

### Aggregate by Tag or Subtable

To calculate each `region` independently:

```sql
SELECT region, MAX(usage) AS max_usage, COUNT(usage) AS sample_count
FROM migration_db.cpu
WHERE _ts >= '2024-01-01 00:00:00.000000000'
  AND _ts < '2024-01-02 00:00:00.000000000'
PARTITION BY region;
```

To calculate a one-hour average for every subtable:

```sql
SELECT tbname, _wstart AS window_start, AVG(usage) AS avg_usage
FROM migration_db.cpu
WHERE _ts >= '2024-01-01 00:00:00.000000000'
  AND _ts < '2024-01-01 01:00:00.000000000'
PARTITION BY tbname
INTERVAL(1h);
```

### Latest Values, Recent Rows, and Fill

Use `LAST_ROW(*)` to return all fields from one latest record:

```sql
SELECT LAST_ROW(*)
FROM migration_db.cpu
WHERE host = 'server01';
```

Use `LAST(expr)` or `LAST(*)` when you need the last non-null value of a field.

To return the most recent ten raw rows:

```sql
SELECT _ts, usage, load
FROM migration_db.cpu
WHERE host = 'server01'
ORDER BY _ts DESC
LIMIT 10;
```

For continuous report windows, use `FILL`. For example, this fills missing one-minute windows with the preceding value:

```sql
SELECT _wstart AS window_start, AVG(usage) AS avg_usage
FROM migration_db.cpu
WHERE _ts >= '2024-01-01 00:00:00.000000000'
  AND _ts < '2024-01-01 01:00:00.000000000'
  AND host = 'server01'
INTERVAL(1m)
FILL(PREV);
```

Validate `FILL(NULL)`, `FILL(PREV)`, and `FILL(LINEAR)` against actual business requirements because they change the meaning of missing windows and downstream calculations. See [Data Queries](../../05-tdengine-sql/04-data-query/01-query.md) and [Functions](../../05-tdengine-sql/04-data-query/03-function.md) for full query semantics.

## Post-migration Capabilities

After basic data validation and query regression are complete, gradually introduce stream processing, data subscription, and virtual tables. Do not deploy these capabilities at the same time as the base migration cutover.

### Stream Processing

[Stream Processing](../../07-stream-processing/index.md) can continuously generate minute- or hour-level aggregates and provide results for alerting services:

```sql
CREATE STREAM cpu_usage_1m_stream
  INTERVAL(1m) SLIDING(1m)
  FROM migration_db.cpu
  INTO cpu_usage_1m
  AS
    SELECT _twstart AS ts,
           AVG(usage) AS avg_usage
    FROM %%trows;
```

### Data Subscription

[Data Subscription](../../06-data-subscription/index.md) distributes new data or query results to downstream consumers:

```sql
CREATE TOPIC cpu_realtime_topic AS
SELECT tbname, _ts, usage, status
FROM migration_db.cpu;
```

Query topics do not support aggregation or time windows. To subscribe to aggregates, first write them to a table with stream processing, then subscribe to that result table.

### Virtual Tables

[Virtual Tables](../../05-tdengine-sql/02-ddl/04-virtualtable.md) combine columns from multiple physical tables by timestamp and provide a unified read-only entry point:

```sql
CREATE VTABLE device_overview (
    ts TIMESTAMP,
    cpu_usage DOUBLE FROM migration_db.cpu_server01.usage,
    temperature FLOAT FROM migration_db.env_server01.temperature
);
```

Virtual tables do not store data. Columns with matching timestamps are returned as one row; timestamps that exist in only one source are retained and missing columns are `NULL`. Replace source tables in the example with actual physical tables or subtables created by the migration.
