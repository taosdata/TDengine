---
toc_max_heading_level: 4
title: Data Replication
---

## Introduction

This article describes how to use taosX commands to replicate data between TDengine clusters. You can replicate data from TDengine 2.6 or 3.0 clusters to TDengine 3.0 clusters. For more information about taosX, see [taosX](../../reference/taosx). You can also perform data replication through taosExplorer. For more information, see [taosExplorer](../explorer/). For information about installing TDengine, see [Installation](../../get-started/).


## Replicating TDengine 3.0 to TDengine 3.0

This section describes how to replicate data from one TDengine 3.0 cluster to another TDengine 3.0 cluster.

### Command-Line Parameters

| Parameter  | Description                                                             | Default                     |
|-----------|------------------------------------------------------------------|----------------------------|
| group.id  | Specify the data subscription group.                                                 | If you do not specify a group, the group is automatically generated based on the hash value. |
| client.id | Specify the client ID for data subscription.                                               | taosx                      |
| timeout   | Specify a timeout for the connection. You can enter `never` to prevent taosX from timing out. | 500 ms                      |
| offset    | Specify an offset from which data subscription begins. Enter the offset in the format `<vgroup_id>:<offset>`. Separate multiple offsets with commas (,).  | If you do not specify an offset, data subscription begins at 0.  |
| token     | Specify the token for the target TDengine cluster. The token is used for authentication.                              | None                                     |

### Example

```shell
taosx run \
  -f 'tmq://root:taosdata@localhost:6030/db1?group.id=taosx1&client.id=taosx&timeout=never&offset=2:10' \
  -t 'taos://root:taosdata@another.com:6030/db2'
```



### Replicating TDengine 2.6 to TDengine 3.0

The following commands migrate data from a TDengine 2.6 cluster to a TDengine 3.0 cluster.

### Command-Line Parameters

| Parameter           | Description                                                                                                                                                                                                                                      | Default                                 |
|--------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------|
| libraryPath        | Specify the path to the taos library in option mode.                                                                                                                                                                                                         | None                                     |
| configDir          | Specify the path to the `taos.cfg` file.                                                                                                                                                                                                                | None                                     |
| mode               | Specify the replication mode for the data source. Enter `history` for historical data. Enter `realtime` for real-time data. Enter `all` for historical and real-time data.                                                                                                                                                            | history                                |
| restro             | Specify a period of data to replicate from the data source before beginning real-time data replication.  For example, setting `restro` to `10m` will replicate real-time data starting 10 minutes before the present.                                                                                                                   | None                                     |
| interval           | Specify the interval at which data is replicated from the data source. For example, setting `mode` to `realtime` and `interval` to `5s` will query new data every 5 seconds.                                                                                                                                                                       | None                                     |
| excursion          | Specify a time period for which out-of-order data can be accepted from the data source.                                                                                                                                                                                                        | 500ms                                  |
| stables            | Specify supertables to replicate from the data source. Separate multiple supertables with commas (,).                                                                                                                                                                         | None                                     |
| tables             | Specify subtables to replicate from the data source. Enter subtables in the format {stable}.{table} or {table}. Separate multiple subtables with commas (,). You can use `@<filepath>` to input a configuration file containing the desired subtables, for example `tables=@./tables.txt`. When using a configuration file, enter each subtable name on a separate line. Empty lines are discarded. | None                                     |
| select-from-stable | Specify columns to replicate from supertables on the data source. Enter a SQL statement in the format `SELECT <columns> FROM stable_name WHERE tbname IN <tb_names>`. Specify tables in the `<stable>.<table>` format, for example `meters.d0` indicates the `d0` subtable within the `meters` supertable.                                                      | All columns are replicated by default. |
| assert             | Specify whether to confirm that a database exists on the target TDengine instance. If the database does not exist, it is created. Use the `taos:///db1?assert` format.                                                                                                                                                    | Databases are not created by default.                     |
| force-stmt         | Specify whether to force STMT mode for data ingestion into the target TDengine instance. TDengine 3.0 and later always use STMT mode.                                                                                                                                                                        | Raw block mode is used by default.              |
| batch-size         | Specify a maximum batch size for STMT data ingestion into the target TDengine instance.                                                                                                                                                                                      |                                        |
| interval           | Specify an interval for data ingestion into the target TDengine instance.                                                                                                                                                                                                      | None                                     |
| max-sql-length     | Specify a maximum length for SQL statements that create tables on the target TDengine instance. Enter a value in bytes.                                                                                                                                                                                      | 800_000 bytes                    |
| failes-to          | Specify a file to contain information about failures to write to tables on the target TDengine instance. When this parameter is specified, replication tasks continue in the event of a write failure.                                                                                                                                   | By default, replication stops on write failure.                 |
| timeout-per-table  | Specify a timeout for replicating standard tables and subtables to the target TDengine instance.                                                                                                                                                                                              | None                                     |
| update-tags        | Specify whether to check whether tag values are consistent. Inconsistent values are updated. If the target database does not exist, it will be created.                                                                                                                                                 | None                                     |

### Example

1. Replicate data over a native connection:

```shell
taosx run \
  -f 'taos://td1:6030/db1?libraryPath=./libtaos.so.2.6.0.30&mode=all' \
  -t 'taos://td2:6030/db2?libraryPath=./libtaos.so.3.0.1.8&assert \
  -v
```

2. Replicate data from `stable1` and `stable2` over a WebSocket connection:

```shell
taosx run \
  -f 'taos+ws://<username>:<password>@td1:6041/db1?stables=stable1,stable2' \
  -t 'taos+wss://td2:6041/db2?assert&token=<token> \
  -v
```