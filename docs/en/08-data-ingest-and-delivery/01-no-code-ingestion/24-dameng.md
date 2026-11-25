---
title: Dameng
sidebar_label: Dameng
---

This section describes how to create a data ingestion task in taosExplorer and load data from a Dameng database into the current TDengine cluster.

## Overview

Dameng (DM) is a domestic relational database widely used in power, energy, and government systems. It often stores SCADA snapshot data, equipment inventories, and business data. TDengine can read data from Dameng and write it to TDengine, including full historical migration, real-time incremental synchronization, and scheduled synchronization.

For synchronization modes, field mapping, type conversion, transaction consistency, and data verification, see [Relational Database Integration](./23-rdb-integration.md).

## Prerequisites

| Item | Requirement |
| --- | --- |
| Dameng version | DM8 |
| Runtime | taosX or taosX-Agent runs on Linux (X64, ARM64, or Hygon C86) or Windows |
| Client library | Install the Dameng DPI client library (`libdmdpi.so` or `dmdpi.dll`, included in the Dameng client package) on the host that runs taosX or taosX-Agent. Add its directory to the dynamic library search path (`LD_LIBRARY_PATH` on Linux, `PATH` on Windows), then restart taosX or taosX-Agent. |
| Network | taosX or taosX-Agent can reach the Dameng service port (default `5236`). For the network requirements of a proxy, see [Data Ingestion Prerequisites](./00-prerequisites.md). |
| Account privileges | The Dameng account used for synchronization has `SELECT` on the source tables. **Check Connectivity** and **Retrieve from Server** also need access to data dictionary views such as `ALL_TABLES` and `ALL_TAB_COLUMNS`. |

Create a dedicated read-only account in Dameng, for example:

```sql
CREATE USER TDSYNC IDENTIFIED BY "Sync_Passw0rd";

GRANT SELECT ON SCADA.METER_READINGS TO TDSYNC;
```

## Create a Task

### Add a Data Source

On the data ingestion page, click **+ Add Data Source** to open the new data source page.

### Configure Basic Information

Enter a task name in **Name**, for example *`test_dameng_01`*.

Select *`Dameng`* in the **Type** list. The fields on the page change to match.

**Agent** is optional. If taosX cannot reach Dameng directly, deploy taosX-Agent on a host that can, and select that agent in the list. You can also click **+ Create New Agent**. For the network requirements between the agent and taosX, including which ports to open and how to check them, see [Data Ingestion Prerequisites](./00-prerequisites.md).

**Target Database** is required. You can click **+ Create Database** to create one. The target time precision should be at least the precision of the source data. See [Time and Time Zone](./23-rdb-integration.md#time-and-time-zone).

### Configure Connection Information

Enter the source Dameng connection in **Connection Configuration**:

| Field | Required | Description |
| --- | --- | --- |
| **Host** | Yes | IP address or host name of the Dameng server |
| **Port** | Yes | Dameng service port. The default is `5236`. |
| **Schema** | No | Default schema. When set, table names in the SQL template can omit the schema prefix. When omitted, the schema with the same name as the login user is used. |
| **Character Set** | No | Connection character set: `UTF-8` or `GB18030`. The default is `UTF-8`. Match the Dameng instance, or non-ASCII text can be garbled. |
| **SSL Mode** | No | `DISABLED` (no encryption) or `REQUIRED` (SSL). After SSL is enabled on the Dameng server, choose `REQUIRED` and upload the Dameng server CA certificate. |

### Configure Authentication

**User** is the Dameng user. The user must be able to read the tables to synchronize.

**Password** is the login password of that user.

Click **Check Connectivity** to confirm that the settings can connect to Dameng and read data. If the connection fails, the page shows the Dameng error code and message. See [FAQ](#faq) for common causes.

### Configure the SQL Query

**Subtable Fields** is a `select distinct` statement that returns the distinct combinations used to split subtables. It usually corresponds to tag mappings in the transform:

> Use this together with the **SQL Template**. Otherwise it does not produce the expected result. For example:
>
> 1. Enter `select distinct col_name1, col_name2 from table` in subtable fields. The source columns `col_name1` and `col_name2` split subtables of the target supertable.
> 2. Add the placeholders in the **SQL Template**, for example `select * from table where ts >= ${start} and ts < ${end} and ${col_name1} and ${col_name2}`. At run time `${col_name1}` and `${col_name2}` expand to equality predicates such as `col_name1='deviceA'` and `col_name2=1`, not to bare column names.
> 3. Map `col_name1` and `col_name2` as tags in the transform.

**SQL Template** is the query template. It must contain a time-range condition, and the start time and end time must appear as a pair. The time range is a source column that represents time, plus the placeholders below.

> Placeholders select the time format:
>
> 1. `${start}` and `${end}`: generate a Dameng `TO_TIMESTAMP('2024-03-14 08:00:00.000', 'YYYY-MM-DD HH24:MI:SS.FF3')` expression from the task time settings. Use them for `TIMESTAMP` and `DATETIME` columns.
> 2. `${start_no_tz}` and `${end_no_tz}`: generate a time string without a time zone, such as `'2024-03-14 08:00:00'`.
> 3. `${start_date}` and `${end_date}`: date only, such as `DATE '2024-03-14'`. Use them for `DATE` columns, and only when the query interval is one day.
>
> Add an `ORDER BY` on the time column, for example `ORDER BY ts ASC`, so migrated data is not written out of order.

For example, read meter readings incrementally by ingestion time:

```sql
SELECT METER_ID, LOCATION, READING_TS, INGEST_TS, VOLTAGE, CURRENT_A
FROM SCADA.METER_READINGS
WHERE INGEST_TS >= ${start} AND INGEST_TS < ${end}
ORDER BY INGEST_TS, ID
```

### Configure Collection

**Start Time** is the start time of the migration. This field is required.

**End Time** is the end time of the migration and can be left blank. When it is set, the task finishes after that time is read. When it is blank, the task keeps synchronizing new data and does not stop automatically.

**Query Interval** is the time span of one query. The default is 1 day. The task splits the time range into windows of this length so that one query does not return too much data.

**Delay Duration** avoids missing rows whose transactions commit late. In continuous synchronization, a window is queried only after the window end plus the delay. In scheduled synchronization, the safe watermark is the current time minus the delay. Set the delay longer than the longest write transaction in Dameng.

**Schedule** is **Continuous Synchronization** or **Scheduled Window Synchronization**. For scheduled window synchronization, set the first scheduled time and a fixed interval in minutes, or set a five-field minute-level Cron expression, and set **Maximum Windows per Run**. Scheduling uses the current time zone of the taosExplorer page.

**Time Zone** interprets Dameng time types that have no time zone (`DATETIME` and `TIMESTAMP`). The default is the time zone of the host where taosX runs.

These parameters select the synchronization mode:

| Mode | Start time | End time | Schedule |
| --- | --- | --- | --- |
| Full migration | Start of the historical data | Migration cutoff | Continuous synchronization |
| Incremental synchronization | Time when synchronization starts | Blank | Continuous synchronization |
| Scheduled synchronization | Time when synchronization starts | Optional | Scheduled window synchronization |

For the behavior of each mode, see [Relational Database Integration · Synchronization Modes](./23-rdb-integration.md#synchronization-modes).

### Configure Data Mapping

Fill in the mapping parameters in **Data Mapping**.

Click **Retrieve from Server** to load sample data from Dameng.

In **Extract or Split from Column**, enter fields to extract or split from a column. For example, to split `vValue` into `vValue_0` and `vValue_1`, select the split extractor, set the separator to `,`, and set the number to 2.

In **Filter**, enter a filter such as `Value > 0`. Only rows where Value is greater than 0 are written to TDengine.

In **Mapping**, select the TDengine supertable and the columns to map. If the supertable does not exist, it can be generated from the sample data.

Click **Preview** to view the mapped result.

Dameng types correspond to TDengine types as follows. For the full rules, see [Data Type Conversion](./23-rdb-integration.md#data-type-conversion):

| Dameng type | TDengine type | Notes |
| --- | --- | --- |
| `BIT`, `BOOLEAN` | `BOOL` | |
| `TINYINT`, `BYTE` | `TINYINT` | |
| `SMALLINT` | `SMALLINT` | |
| `INT`, `INTEGER` | `INT` | |
| `BIGINT` | `BIGINT` | |
| `NUMBER(p,s)`, `DECIMAL(p,s)`, `NUMERIC(p,s)` | `DECIMAL(p,s)` | Converted to `DOUBLE` when `p` is greater than 38 |
| `NUMBER` (precision not specified) | `DOUBLE` | |
| `REAL` | `FLOAT` | |
| `FLOAT`, `DOUBLE`, `DOUBLE PRECISION` | `DOUBLE` | |
| `CHAR`, `VARCHAR`, `VARCHAR2` | `NCHAR(n)` | Use `VARCHAR` when the value is ASCII only |
| `TEXT`, `CLOB`, `LONGVARCHAR` | `NCHAR` / `VARCHAR` | Values longer than 65,517 bytes follow the **column length overflow** policy |
| `BINARY`, `VARBINARY` | `VARBINARY` | |
| `BLOB`, `IMAGE`, `LONGVARBINARY` | `BLOB` | One value is at most 16 MB |
| `DATE`, `DATETIME`, `TIMESTAMP` | `TIMESTAMP` | Interpreted with the task time zone |
| `TIMESTAMP WITH TIME ZONE`, `DATETIME WITH TIME ZONE` | `TIMESTAMP` | Converted by the time zone in the value |
| `TIME` | `VARCHAR(18)` | Written as `HH:MI:SS[.ffffff]` |

### Configure Advanced Options

import AdvancedOptions from './resources/_02-advanced_options.mdx'

<AdvancedOptions/>

In addition to the common options, the Dameng data source provides:

**Data Verification** checks each window against the data written to TDengine. Choose **Off**, **Row Count**, or **Row Count and Digest**. The default is Off. To require the synchronized data to match the source, choose **Row Count and Digest**.

**Verification Failure Policy** is **Redo Window** (default) or **Stop on Error**.

**Verification Retries** is the maximum number of retries for one window when the failure policy is **Redo Window**. The default is 3.

For the rules and how to view results, see [Relational Database Integration · Data Verification](./23-rdb-integration.md#data-verification).

### Configure Exception Handling

import ExceptionHandling from './resources/_03-exception-handling-strategy.mdx'

<ExceptionHandling />

### Completion

Click **Submit** to create the Dameng to TDengine synchronization task, and return to the **Data Source List** to view its status. When data verification is enabled, the **Data Verification** tab on the task details page shows the result of each window.

## Example: Migrate SCADA Snapshot Data

A power dispatch system often stores snapshot data in Dameng every 1 or 5 minutes. The table is:

```sql
CREATE TABLE SCADA.SECTION_DATA (
  POINT_ID   VARCHAR(32),
  POINT_NAME VARCHAR(128),
  STATION    VARCHAR(64),
  SAMPLE_TIME DATETIME(3),
  VAL NUMBER(12, 4),
  QUALITY    INT,
  PRIMARY KEY(POINT_ID, SAMPLE_TIME)
);
```

The goal is to migrate the 2024 historical data into TDengine and then keep synchronizing new data.

1. Create a millisecond-precision database and supertable in TDengine:

    ```sql
    CREATE DATABASE scada
      PRECISION 'ms';

    CREATE STABLE scada.section_data (
      ts      TIMESTAMP,
      val     DECIMAL(12, 4),
      quality INT
    ) TAGS (
      point_id   VARCHAR(32),
      point_name NCHAR(128),
      station    NCHAR(64)
    );
    ```

2. Create a Dameng data source task with this SQL template:

    ```sql
    SELECT POINT_ID, POINT_NAME, STATION, SAMPLE_TIME, VAL, QUALITY
    FROM SCADA.SECTION_DATA
    WHERE SAMPLE_TIME >= ${start} AND SAMPLE_TIME < ${end}
    ORDER BY SAMPLE_TIME, POINT_ID
    ```

3. Set **Start Time** to `2024-01-01 00:00:00`, leave **End Time** blank, set **Query Interval** to `1h`, set **Delay Duration** to `60s`, and set **Schedule** to **Continuous Synchronization**. The task first migrates historical data in one-hour windows, then switches to incremental synchronization after it catches up with the current time.
4. In **Data Mapping**, set the subtable name to `p_{POINT_ID}`. Map `POINT_ID`, `POINT_NAME`, and `STATION` to tags, `SAMPLE_TIME` to `ts`, and `VAL` and `QUALITY` to columns of the same name.
5. In **Advanced Options**, set **Data Verification** to **Row Count and Digest**.
6. Submit the task. After the migration, query the snapshot data in TDengine:

    ```sql
    SELECT
      ts,
      val,
      quality
    FROM scada.section_data
    WHERE point_id = 'P0001'
      AND ts >= '2024-06-01'
      AND ts < '2024-06-02';
    ```

`(POINT_ID, SAMPLE_TIME)` is unique in the source table. After mapping, each point has one subtable and `ts` is unique in that subtable, which satisfies [Idempotent Writes](./23-rdb-integration.md#idempotent-writes).

## FAQ {#faq}

**The connection reports that `libdmdpi.so` or `dmdpi.dll` cannot be found**

The Dameng DPI client library is not installed on the host that runs taosX or taosX-Agent, or its directory is not on the dynamic library search path. After installing the Dameng client, add the library directory to `LD_LIBRARY_PATH` (Linux) or `PATH` (Windows), then restart taosX or taosX-Agent.

**The connection times out or the network is unreachable**

Check that the Dameng service is running, that the port is the one actually listening (default `5236`), and that the firewall allows it. See [Data Ingestion Prerequisites](./00-prerequisites.md) for how to check the port.

**Non-ASCII text is garbled**

The **Character Set** in the connection does not match the Dameng instance. Run `SELECT SF_GET_UNICODE_FLAG();` in Dameng. A result of `1` means UTF-8, and `0` means GB18030. Change the task character set and restart the task.

**The synchronized row count is smaller than the source table**

There are three common causes: the target database time precision is coarser than the source, so distinct times are truncated to one timestamp; one point has several rows at the same time and no composite primary key is configured; the delay duration is shorter than the source write transactions, so rows are missed. Enable **Row Count and Digest** to locate the window. See [Relational Database Integration · Transaction Consistency](./23-rdb-integration.md#transaction-consistency).
