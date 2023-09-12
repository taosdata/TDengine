---
toc_max_heading_level: 4
title: Data Backup and Restoration
---

This section describes how to use the taosX command line to back up data from a TDengine cluster to a local file and how to restore data from a backed up local file to a TDengine cluster. For command line arguments to taosX, see [taosX](../../reference/taosx). You can also use taos-explorer's visual interface for data backup and recovery, please refer to [Visual Management](../explorer). For service installation and deployment, please refer to [Installation and Deployment](../../get-started).

## Back up TDengine data to local machine

### Examples
```shell
taosx run -f 'tmq://root:taosdata@td1:6030/db1' -t 'local:/path_directory/'

```
The result of the above example execution and the parameter description:

Backup all data from database db1 in cluster td1 to the /path_directory path on the taosx device.

The object support of the data source (DSN with -f parameter) is configured to database level (dbname), super table level (dbname.stablename), and sub table/tablename level (dbname.tablename), which corresponds to the level of the backed up data, i.e., database, super table, and sub table/tablename level.


## Restore TDengine data from file

### Examples
```shell
taosx run -f 'local:/path_directory/' -t 'taos://root:taosdata@td2:6030/db1?assert'
```

The result of the above example execution:

Restore the data files that have been backed up under the /path_directory path of the device where taosx is located to the database db1 of cluster td2, and if db1 does not exist, it will be built automatically.

The object in the target source (DSN with -t parameter) is supported to be configured as database (dbname), super table (dbname.stablename), sub/common table (dbname.tablename), which corresponds to the level of the backed up data, database level, super table level, sub/common table level, provided that the backed up datafiles are also of the corresponding Database level, super table level, sub table/ordinary table level data.


## Troubleshooting common errors

(1) If a native connection is used, the task fails to start and reports the following error:

```text
Error: tmq to td task exec error

Caused by:
    [0x000B] Unable to establish connection
```
The reason is that the port link with the data source is abnormal, you need to check whether the data source FQDN is connected and whether port 6030 can be accessed normally.

(2) If you use a WebSocket connection, the task fails to start and reports the following error:

```text
Error: tmq to td task exec error

Caused by:
    0: WebSocket internal error: IO error: failed to lookup address information: Temporary failure in name resolution
    1: IO error: failed to lookup address information: Temporary failure in name resolution
    2: failed to lookup address information: Temporary failure in name resolution
```

There are several types of errors that can be encountered when connecting using a WebSocket. The error message can be viewed after "Caused by", the following are a few possible errors:

- "Temporary failure in name resolution": DNS resolution error, check if the IP or FQDN is accessible.
- "IO error: Connection refused (os error 111)": Port access failed, check if the port is configured correctly or is enabled and accessible.
- "IO error: received corrupt message": Message parsing failed, probably because SSL was enabled using wss, but the source port does not support it.
- "HTTP error: *": Possible connection to wrong taosAdapter port or LSB/Nginx/Proxy configuration error.
- "WebSocket protocol error: Handshake not finished": WebSocket connection error, usually due to an incorrectly configured port.

(3) If the task fails to start and reports the following error:

```text
Error: tmq to td task exec error

Caused by:
    [0x038C] WAL retention period is zero
```

is unable to subscribe due to a misconfiguration of the source database WAL.

Solution:
Modify WAL Configuration:

```sql
alter database test wal_retention_period 3600;
```