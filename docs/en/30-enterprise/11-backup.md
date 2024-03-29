---
toc_max_heading_level: 4
title: Data Backup and Restoration
---

## Backup and Restore Using the Visual Interface

This section explains how to perform data backup and restore using the visual interface. For specific details, please refer to [Visual Management](../explorer). For service installation and deployment, refer to [Installation and Deployment](../../get-started).

### Backup and Restore

You can back up data from the currently connected TDengine cluster to one or more local files, and later use these files for data restoration. This section will outline the specific steps for data backup and restore.

#### Backup Data to Local Files

1. Navigate to the system management page, click on **Backup** to enter the data backup page, and then click **Create New Backup** in the upper right corner.
2. In the data backup configuration page, you can configure three parameters:
   - Backup Cycle: Required, configure the time interval for each data backup. You can choose to perform data backup every day, every 7 days, or every 30 days from the drop-down menu. After configuration, a data backup task will start at 0:00 of the corresponding backup cycle.
   - Database: Required, configure the name of the database to be backed up (the wal_retention_period parameter of the database must be greater than 0).
   - Directory: Required, configure the path to back up the data to the location specified in the taosX runtime environment, such as /root/data_backup.
3. Click **Confirm** to create a data backup task.

![backup](./backup-00-new.png)

#### Restore from Local Files

1. After creating the data backup task, click **Data Restore** on the right side of the corresponding data backup task in the page to restore the data that has been backed up to the specified path to the current TDengine.

## Troubleshooting

1. When using a native connection, the job fails with the following error:

```text
Error: tmq to td task exec error

Caused by:
    [0x000B] Unable to establish connection
```
This error occurs when the source and target TDengine clusters cannot connect to each other. Ensure that their FQDNs have connectivity and that port 6030 is open.

2. When using a WebSocket connection, the job fails with the following error:

```text
Error: tmq to td task exec error

Caused by:
    0: WebSocket internal error: IO error: failed to lookup address information: Temporary failure in name resolution
    1: IO error: failed to lookup address information: Temporary failure in name resolution
    2: failed to lookup address information: Temporary failure in name resolution
```

You can check the **Caused by** section to diagnose WebSocket connection errors. Several potential errors are listed as follows:

- **Temporary failure in name resolution**: Ensure that the IP address or FQDN of the target TDengine cluster are accessible.
- **IO error: Connection refused (os error 111)**: Ensure that the required port on the target TDengine cluster is open.
- **IO error: received corrupt message**: Ensure that the specified data source supports SSL.
- **HTTP error: \* **: Ensure that your LSB, nginx, and proxy server configurations are correct and that you are connecting to the correct taosAdapter port.
- **WebSocket protocol error: Handshake not finished**: Ensure that the correct port is configured for the connection.

3. A job fails to start and the following error is displayed:

```text
Error: tmq to td task exec error

Caused by:
    [0x038C] WAL retention period is zero
```

The WAL configuration on the data source is incorrect, causing data subscription to fail.

Solution:
Change the WAL retention period on the affected database:

```sql
alter database test wal_retention_period 3600;
```
