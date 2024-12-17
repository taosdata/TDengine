---
title: Data Backup and Restoration
slug: /operations-and-maintenance/back-up-and-restore-data
---

To prevent data loss and accidental deletions, TDengine provides comprehensive features such as data backup, restoration, fault tolerance, and real-time synchronization of remote data to ensure the security of data storage. This section briefly explains the backup and restoration functions.

## Data Backup and Restoration Using taosdump

taosdump is an open-source tool that supports backing up data from a running TDengine cluster and restoring the backed-up data to the same or another running TDengine cluster. taosdump can back up the database as a logical data unit or back up data records within a specified time period in the database. When using taosdump, you can specify the directory path for data backup. If no directory path is specified, taosdump will default to backing up the data in the current directory.

Below is an example of using taosdump to perform data backup.

```shell
taosdump -h localhost -P 6030 -D dbname -o /file/path
```

After executing the above command, taosdump will connect to the TDengine cluster at localhost:6030, query all data in the database dbname, and back up the data to /file/path.

When using taosdump, if the specified storage path already contains data files, taosdump will prompt the user and exit immediately to avoid data overwriting. This means the same storage path can only be used for one backup. If you see related prompts, please operate carefully to avoid accidental data loss.

To restore data files from a specified local file path to a running TDengine cluster, you can execute the taosdump command by specifying command-line parameters and the data file path. Below is an example code for taosdump performing data restoration.

```shell
taosdump -i /file/path -h localhost -P 6030
```

After executing the above command, taosdump will connect to the TDengine cluster at localhost:6030 and restore the data files from /file/path to the TDengine cluster.

## Data Backup and Restoration Based on TDengine Enterprise

TDengine Enterprise provides an efficient incremental backup feature, with the following process.

Step 1, access the taosExplorer service through a browser, usually at the port 6060 of the IP address where the TDengine cluster is located, such as `http://localhost:6060`.

Step 2, in the "System Management - Backup" page of the taosExplorer service, add a new data backup task, fill in the database name and backup storage file path in the task configuration information, and start the data backup after completing the task creation. Three parameters can be configured on the data backup configuration page:

- Backup cycle: Required, configure the time interval for each data backup execution, which can be selected from a dropdown menu to execute once every day, every 7 days, or every 30 days. After configuration, a data backup task will be initiated at 0:00 of the corresponding backup cycle;
- Database: Required, configure the name of the database to be backed up (the database's wal_retention_period parameter must be greater than 0);
- Directory: Required, configure the path in the running environment of taosX where the data will be backed up, such as `/root/data_backup`;

Step 3, after the data backup task is completed, find the created data backup task in the list of created tasks on the same page, and directly perform one-click restoration to restore the data to TDengine.

Compared to taosdump, if the same data is backed up multiple times in the specified storage path, since TDengine Enterprise not only has high backup efficiency but also implements incremental processing, each backup task will be completed quickly. As taosdump always performs full backups, TDengine Enterprise can significantly reduce system overhead in scenarios with large data volumes and is more convenient.

**Common Error Troubleshooting**

1. If the task fails to start and reports the following error:

```text
Error: tmq to td task exec error

Caused by:
    [0x000B] Unable to establish connection
```

The cause is an abnormal connection to the data source port, check whether the data source FQDN is connected and whether port 6030 is accessible.

2. If using a WebSocket connection, the task fails to start and reports the following error:

```text
Error: tmq to td task exec error

Caused by:
    0: WebSocket internal error: IO error: failed to lookup address information: Temporary failure in name resolution
    1: IO error: failed to lookup address information: Temporary failure in name resolution
    2: failed to lookup address information: Temporary failure in name resolution
```

When using a WebSocket connection, you may encounter various types of errors, which can be seen after "Caused by". Here are some possible errors:

- "Temporary failure in name resolution": DNS resolution error, check if the IP or FQDN can be accessed normally.
- "IO error: Connection refused (os error 111)": Port access failure, check if the port is configured correctly or if it is open and accessible.
- "IO error: received corrupt message": Message parsing failed, possibly because SSL was enabled using wss, but the source port does not support it.
- "HTTP error: *": Possibly connected to the wrong taosAdapter port or incorrect LSB/Nginx/Proxy configuration.
- "WebSocket protocol error: Handshake not finished": WebSocket connection error, usually because the configured port is incorrect.

3. If the task fails to start and reports the following error:

```text
Error: tmq to td task exec error

Caused by:
    [0x038C] WAL retention period is zero
```

This is due to incorrect WAL configuration in the source database, preventing subscription.

Solution:
Modify the data WAL configuration:

```sql
alter database test wal_retention_period 3600;
```
