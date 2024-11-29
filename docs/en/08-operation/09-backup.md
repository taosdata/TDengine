---
title: Back Up and Restore Data
slug: /operations-and-maintenance/back-up-and-restore-data
---

To prevent data loss and erroneous deletions, TDengine provides comprehensive data backup, recovery, fault tolerance, and real-time remote data synchronization functions to ensure the security of data storage. This section briefly describes the backup and recovery capabilities.

## Data Backup and Recovery Using taosdump

`taosdump` is an open-source tool that supports backing up data from a running TDengine cluster and restoring the backed-up data to the same or another running TDengine cluster. `taosdump` can back up the database as a logical data unit or back up data records within a specified time period in the database. When using `taosdump`, you can specify the directory path for data backup. If no directory path is specified, `taosdump` will default to backing up data to the current directory.

The following is an example of using `taosdump` to perform data backup.

```shell
taosdump -h localhost -P 6030 -D dbname -o /file/path
```

After executing the above command, `taosdump` will connect to the TDengine cluster at localhost:6030, query all data in the database `dbname`, and back up the data to `/file/path`.

When using `taosdump`, if the specified storage path already contains data files, `taosdump` will prompt the user and exit immediately to avoid data being overwritten. This means the same storage path can only be used for one backup. If you see a related prompt, please proceed with caution to avoid data loss due to erroneous operations.

To restore data files from the specified local file path to a running TDengine cluster, you can execute the `taosdump` command by specifying the command line parameters and the path of the data files. Below is an example of executing data recovery with `taosdump`.

```shell
taosdump -i /file/path -h localhost -P 6030
```

After executing the above command, `taosdump` will connect to the TDengine cluster at `localhost:6030` and restore the data files from `/file/path` to the TDengine cluster.

## Data Backup and Recovery Based on TDengine Enterprise

TDengine Enterprise provides an efficient incremental backup feature, with the following process.

Step 1: Access the `taosExplorer` service through a browser. The access address is usually the IP address of the TDengine cluster at port 6060, such as `http://localhost:6060`.

Step 2: In the `taosExplorer` service page, go to the “System Management - Backup” page to add a new data backup task. Fill in the database name to be backed up and the backup storage file path in the task configuration information. After creating the task, you can start the data backup. In the data backup configuration page, you can configure three parameters:

- Backup Cycle: This is a required item, which configures the time interval for each data backup execution. You can select daily, every 7 days, or every 30 days from the dropdown menu, and the backup task will start at 0:00 on the corresponding backup cycle;
- Database: This is a required item, which configures the name of the database to be backed up (the `wal_retention_period` parameter of the database must be greater than 0);
- Directory: This is a required item, which configures the path where data will be backed up to in the environment where `taosX` is running, such as `/root/data_backup`;

Step 3: After the data backup task is completed, find the created data backup task in the list of created tasks on the same page, and simply execute one-click recovery to restore the data into TDengine.

Compared to `taosdump`, if multiple backup operations are performed on the same data in the specified storage path, TDengine Enterprise not only has high backup efficiency but also employs incremental processing, so each backup task will complete quickly. Since `taosdump` is always a full backup, TDengine Enterprise can significantly reduce system overhead and is more convenient in scenarios with large data volumes.

## Common Error Troubleshooting

1. If the task fails to start and reports the following error:

```text
Error: tmq to td task exec error

Caused by:
    [0x000B] Unable to establish connection
```

The cause is an abnormal link to the port of the data source. You need to check whether the FQDN of the data source is reachable and whether port 6030 is accessible.

2. If the task fails to start when using a WebSocket connection and reports the following error:

```text
Error: tmq to td task exec error

Caused by:
    0: WebSocket internal error: IO error: failed to lookup address information: Temporary failure in name resolution
    1: IO error: failed to lookup address information: Temporary failure in name resolution
    2: failed to lookup address information: Temporary failure in name resolution
```

When using a WebSocket connection, various types of errors may occur. The error information can be viewed after “Caused by”. Here are some possible errors:

- "Temporary failure in name resolution": DNS resolution error; check whether the IP or FQDN is accessible.
- "IO error: Connection refused (os error 111)": Port access failure; check whether the port is correctly configured or whether it is open and accessible.
- "IO error: received corrupt message": Message parsing failure, which may be due to enabling SSL with wss, but the source port does not support it.
- "HTTP error: *": Possibly connected to the wrong `taosAdapter` port or misconfigured LSB/Nginx/Proxy.
- "WebSocket protocol error: Handshake not finished": WebSocket connection error, usually due to an incorrect configured port.

3. If the task fails to start and reports the following error:

```text
Error: tmq to td task exec error

Caused by:
    [0x038C] WAL retention period is zero
```

This is due to an error in the WAL configuration of the source database, which cannot be subscribed to.

Resolution:
Modify the WAL configuration of the data:

```sql
alter database test wal_retention_period 3600;
```
