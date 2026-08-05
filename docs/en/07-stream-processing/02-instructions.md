---
sidebar_label: Operations and Limits
title: Operations and Limits
description: High availability, permissions, recomputation, and limitations for stream processing
---

## Other Features and Notes

### High Availability

Stream processing in TDengine is architected with a separation of compute and storage, which requires that at least one snode be deployed in the system. Except for data reads, all stream processing functions run exclusively on snodes.

- snode: The node responsible for executing stream processing tasks. A cluster can have one or more snodes (at least one is required). Each dnode can host at most one snode, and each snode has multiple execution threads.
- An snode can be deployed on the same dnode as other node types (vnode/mnode). However, for better resource isolation, it is strongly recommended to deploy snodes on dedicated dnodes. This ensures resource separation so that stream processing does not interfere significantly with writes, queries, or other operations.
- To ensure high availability for stream processing, it is recommended to deploy multiple snodes across different physical nodes in the cluster:
  - Stream tasks are load-balanced across multiple snodes.
  - Each pair of snodes acts as replicas, storing stream state and progress information.
  - If only a single snode is deployed in the cluster, the system cannot guarantee high availability.

### Deploy an Snode

Before creating a stream processing task, an snode must be deployed. The syntax is as follows:

```sql
CREATE SNODE ON DNODE dnode_id;
```

### View Snodes

You can view information about snodes with the following command:

```sql
SHOW SNODES;
```

For more detailed information, use:

```sql
SELECT * FROM information_schema.`ins_snodes`;
```

### Delete an Snode

When you delete an snode, both the snode and its replica must be online to synchronize stream state information. If either the snode or its replica is offline, the deletion will fail.

```sql
DROP SNODE ON DNODE dnode_id;
```

### Permission Control

Permission control for stream processing is tied only to database-level permissions. Since each stream may be associated with multiple databases, the requirements are as follows:

| Associated Database                  | Count     | Auth Action                                       | Required Permission |
| ------------------------------------ | --------- | ------------------------------------------------- | ------------------- |
| Database where the stream is defined | 1         | Create, delete, stop, start, manual recomputation | Write               |
| Database of the trigger table        | 1         | Create                                            | Read                |
| Database of the output table         | 1         | Create                                            | Write               |
| Databases of the computation sources | 1 or more | Create                                            | Read                |

### Recomputation

Most TDengine TSDB window types are associated with primary key columns. For example, event windows rely on data ordered by primary key to determine when to open and close a window. When using window-based triggers, it is important that trigger table data be written in an orderly fashion, as this ensures the highest efficiency in stream processing. If out-of-order data is written, it may affect the correctness of results for windows that have already been triggered. Similarly, updates and deletions can also compromise result correctness.

TDengine supports the use of WATERMARK to mitigate issues caused by out-of-order data, updates, and deletions. A WATERMARK is a user-defined duration based on event time that represents the system’s progress in stream processing, reflecting the user’s tolerance for out-of-order data. The current watermark is defined as `latest processed event time – WATERMARK interval`. Only data with event times earlier than the current watermark are eligible for trigger evaluation. Likewise, only windows or other trigger conditions whose time boundaries are earlier than the current watermark will be triggered. Note: WATERMARK does not apply to PERIOD (scheduled) triggers. In PERIOD mode, no recalculation is performed.

For out-of-order, update, or delete scenarios that exceed the WATERMARK, recalculation is used to ensure the correctness of results. Recalculation means re-triggering and re-executing computations for the data range affected by out-of-order, updated, or deleted records. The results already written to the output table are not deleted; instead, new results are written again. To make this approach effective, users must ensure that their computation statements and source tables are independent of processing time—that is, the same trigger should produce valid results even if executed multiple times.

Recalculation can be either automatic or manual. If automatic recalculation is not needed, it can be disabled via configuration options.

#### Manual Recalculation

Manual recalculation must be explicitly initiated by the user and can be started with an SQL command when needed.

```sql
RECALCULATE STREAM [db_name.]stream_name FROM start_time [TO end_time];
```

Notes:

- You can specify a time range (based on event time) for which the stream should be recalculated. If no end time (end_time) is specified, the recalculation range extends from the given start time (start_time) up to the stream’s current processing progress at the moment the manual recalculation is initiated.
- Manual recalculation is not supported for scheduled triggers (PERIOD) but is supported for all other trigger types.
- For count window triggers, both the start time and end time must be specified. Recalculation applies only to intervals that the stream has already processed. If the specified range includes intervals that the stream has not yet begun processing, those portions are automatically ignored. During recalculation, trigger windows are re-partitioned within the specified interval. This may cause misalignment between the new windows and those computed previously. As a result, users may need to manually delete the existing results for that interval from the output table to avoid duplicate results. Similarly, if no end time is specified, the recalculation request will be ignored. For scenarios that require recalculation from a certain start time with no defined end, the recommended approach is to drop the stream, recreate it, and specify FILL_HISTORY_FIRST.

### Atypical Data Ingestion Scenarios

#### Out-of-Order Data

Out-of-order data refers to records written to the trigger table in a non-sequential order. While the computation itself does not depend on whether the source table is ordered, users must ensure—based on business requirements—that the source table’s data is fully written before a trigger occurs. The impact of out-of-order data and how it is handled vary depending on the trigger type.

| Trigger Type                                                 | Impact and Handling                                          |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| Periodic trigger<br/>Sliding trigger<br/>Count window trigger whose sliding step is not 1 | Ignored; no processing performed.                            |
| Count window trigger whose sliding step is 1, such as `COUNT_WINDOW(1)` or `COUNT_WINDOW(n, 1)` | Default: Handled through recalculation.<br/>Optional: Ignored with `STREAM_OPTIONS(IGNORE_DISORDER)`. |
| Other window triggers                                        | Default: Handled through recalculation.<br/>Optional: Ignored; no processing performed. |

#### Data Updates

Data updates refer to multiple writes of records with the same timestamp, where other column values may or may not change. Update operations affect only the trigger table and the triggering behavior—they do not directly affect the computation process itself. The impact of data updates and how they are handled vary depending on the trigger type.

| Trigger Type                                                 | Impact and Handling                                          |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| Periodic trigger<br/>Sliding trigger<br/>Count window trigger whose sliding step is not 1 | Ignored; no processing performed.                            |
| Count window trigger whose sliding step is 1, such as `COUNT_WINDOW(1)` or `COUNT_WINDOW(n, 1)` | Default: Treated as out-of-order data and handled through recalculation.<br/>Optional: Ignored with `STREAM_OPTIONS(IGNORE_DISORDER)`. |
| Other window triggers                                        | Treated as out-of-order data and handled through recalculation. |

#### Data Deletions

Data deletions affect only the trigger table and the triggering behavior—they do not directly impact the computation process itself. The impact of data deletions and how they are handled vary depending on the trigger type.

| Trigger Type                                                 | Impact and Handling                                          |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| Periodic trigger<br/>Sliding trigger<br/>Count window trigger whose sliding step is not 1 | Ignored; no processing performed.                            |
| Count window trigger whose sliding step is 1, such as `COUNT_WINDOW(1)` or `COUNT_WINDOW(n, 1)` | Default: Ignored; no processing performed.<br/>Optional: Treated as out-of-order data and handled through recalculation with `STREAM_OPTIONS(DELETE_RECALC)`. |
| Other window triggers                                        | Default: Ignored; no processing performed.<br/>Optional: Treated as out-of-order data and handled through recalculation. |

#### Expired Data

The expired_time setting defines a data expiration interval. For each group generated by a stream trigger, the system determines whether new data is expired by comparing the event time of the latest data against the expiration threshold. The threshold is calculated as:`latest event time – expired_time`. All data earlier than this threshold is treated as expired.

- Expired data applies only to real-time data in the trigger table. Historical data and data from other tables do not have the concept of expiration. Expiration is evaluated at the time of stream triggering, and whether data is expired depends on when it is written. Data written in event-time order will never be expired; only out-of-order data may be considered expired.
- Expired data does not automatically trigger new computation or recalculation. This means that under all trigger types, expired data is ignored (not computed or recalculated). If no time ranges need to be excluded from computation or recalculation, you do not need to specify expired_time. If expired data is defined but you still want to compute or recalculate over part of it, you can use manual recalculation.
- Expired data affects only whether automatic triggers occur. It does not affect the computation range itself. Therefore, if a trigger’s computation range includes expired data in the trigger table, that data will still be used in the calculation.

### Database and Table Operations

After a stream is created, users may perform operations on the databases and tables associated with the stream. The effects of these operations on the stream and how the stream handles them are summarized as follows:

| Operation                                                    | Operation Impact and Stream Handling                         |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| User creates a new child table under a trigger supertable (non-virtual) and writes data | The new child table is automatically included in the current stream processing, either joining an existing group or creating a new one. |
| User creates a new child table under a virtual trigger supertable and writes data | Ignored; no additional handling.                             |
| User deletes a child table of the trigger supertable         | Default: Ignored.<br/>Optional: Certain trigger types can be configured to automatically recalculate, or to delete the corresponding result table (only applies to streams grouped by child table).<br/>For `ROLLUP BY` streams, output subtables that have already been generated are retained. |
| User deletes the trigger table                               | Ignored; no additional handling.                             |
| User adds a column to the trigger table                      | Ignored; no additional handling.                             |
| User deletes a column from the trigger table                 | Ignored; no additional handling.                             |
| User modifies the tag value of a child table under the trigger supertable | If the tag column is used by the stream as a grouping key, the operation is not allowed and results in an error.<br/>Otherwise, ignored. |
| User modifies the schema of the trigger table columns        | Ignored; no additional handling. (An error will be raised when a schema mismatch is detected at read time.) |
| User modifies or deletes a source table                      | Ignored; no additional handling.                             |
| User modifies or deletes an output table                     | Ignored; no additional handling. (If a schema mismatch is detected at write time, an error is raised. If the table does not exist, it will be recreated.) |
| User splits a vnode                                          | Not allowed if the database containing the vnode is a source database or trigger table database.<br/>Not allowed if virtual tables are used for triggers or computations.<br/>The user may force execution after confirming no impact with SPLIT VGROUP N FORCE. |
| User deletes a database                                      | Not allowed if the deleted database is a source database of a stream, or a trigger table database that is not the same as the stream’s own database.<br/>Not allowed if the stream involves triggers or computations on virtual tables from non-target databases.<br/>The user may force execution after confirming no impact with `DROP DATABASE name FORCE`. |

Apart from the operations explicitly restricted or specially handled in the table above, all other operations—as well as those marked as ignored; no additional handling—are unrestricted. However, if such operations may affect stream computation, it is the user’s responsibility to decide how to proceed: either ignore the impact or perform a manual recalculation to restore correctness.

### Configuration Parameters

Stream processing-related configuration parameters are listed below. For full details, see [taosd](../12-operations-and-tooling/03-components/01-taosd.md).

- numOfMnodeStreamMgmtThreads: Number of stream management threads on mnodes.
- numOfStreamMgmtThreads: Number of stream management threads on vnodes/snodes.
- numOfVnodeStreamReaderThreads: Number of stream reader threads on vnodes.
- numOfStreamTriggerThreads: Number of stream trigger threads.
- numOfStreamRunnerThreads: Number of stream execution threads
- streamBufferSize: Maximum buffer size available for stream processing, used only for caching results of %%trows (unit: MB).
- streamNotifyMessageSize: Controls the size of event notification messages.
- streamNotifyFrameSize: Controls the underlying frame size used when sending event notification messages.

### Rules and Limitations

The following rules and limitations apply to stream processing:

- Before creating a stream, the cluster must have at least one snode deployed, and there must be an available (running) snode at the time of creation.
- Each stream belongs to a specific database. Therefore, the database must already exist before creating a stream, and streams within the same database cannot share the same name.
- The trigger table and the source table for a stream may be the same or different, and they can belong to different databases.
- The output table of a stream can be in a different database from the stream, trigger table, or source table, but it cannot be the same as the trigger table or the source table.
- Output tables (whether supertables or regular tables) are created automatically when the stream is created. If you want to write to an existing table, its schema must match exactly.
- Output child tables for each group do not need to be created in advance; they are created automatically when results are written during computation.
- The computation results of each trigger group are written to the same child table. If no trigger group is specified, all results are written to a single regular table.
- If different groups are configured to generate child tables with the same name, their results will be written into the same child table. Users must confirm this is the intended behavior; otherwise, ensure each group generates a uniquely named child table.
- In addition to specifying child table names, users can also define the tag columns of the output supertable and the tag values for each child table.
- Stream processing supports nesting, meaning a new stream can be created based on the output table of an existing stream.
- For count window triggers whose sliding step is not 1, out-of-order data, updates, and deletions are ignored. When the sliding step is 1 (for example, `COUNT_WINDOW(1)` or `COUNT_WINDOW(n, 1)`), out-of-order data and updates trigger recalculation by default and can be ignored with `IGNORE_DISORDER`; deletions are ignored by default and can trigger recalculation with `DELETE_RECALC`. In non-`FILL_HISTORY_FIRST` mode, historical and real-time windows may not align.
- For supertable window triggers, only interval and session windows support grouping by tag, by rollup tag, by child table, or no grouping. Other window types only support grouping by child table.
- `ROLLUP BY` is mutually exclusive with `PARTITION BY` and `DELETE_OUTPUT_TABLE`.
- Pseudo-columns qstart, qend, and qduration are not supported in queries.

#### Temporary Restrictions

- Grouping by regular data columns is not yet supported.
- The Geometry data type is not yet supported.
- The ON_FAILURE_PAUSE option in NOTIFY_OPTIONS is not yet supported.

### Compatibility Notes

Compared with `v3.3.6.0`, stream processing has been completely redesigned. Before upgrading from the old version, the following steps must be performed, after which streams should be recreated under the new stream processing version:

- Delete all existing stream processing tasks.
- Delete all TSMA.
- Delete all snodes.
- Remove snode-related directories:
  - The snode directory under the dataDir configuration path (default: /var/lib/taos/snode).
  - The directory specified by the former checkpointBackupDir configuration option (default: /var/lib/taos/backup/checkpoint/).
- Delete all result tables.

Note: If the above steps are not performed, taosd will fail to start.
