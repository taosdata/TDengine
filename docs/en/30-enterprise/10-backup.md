---
toc_max_heading_level: 4
title: Data Backup and Restoration
---

This article describes how to back up your TDengine data to a local disk and restore it from disk. For more information about taosX, see [taosX](../../reference/taosx/). You can back up and restore data through taosExplorer. For more information, see [taosExplorer](../explorer/). For information about installing TDengine, see [Installation](../../get-started/).

## Back Up TDengine Data

### Example
```shell
taosx run -f 'tmq://root:taosdata@td1:6030/db1' -t 'local:/path_directory/'

```
This command is described as follows:

All data in the `db1` database on the `td1` cluster is backed up to the `/path_directory` directory on the machine running taosX.

You can back up databases, supertables, standard tables, and subtables by specifying the desired object in the DSN.


## Restore TDengine Data

### Example
```shell
taosx run -f 'local:/path_directory/' -t 'taos://root:taosdata@td2:6030/db1?assert'
```

This command is described as follows:

All backed up data in the `/path_directory` directory is restored to the `db1` database on the `tb2` cluster. If `db1` does not exist, it is created.

You can restore databases, supertables, standard tables, and subtables by specifying the desired object in the DSN.


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