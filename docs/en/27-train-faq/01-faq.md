---
title: Frequently Asked Questions
description: This document describes the frequently asked questions about TDengine.
---

## Submit an Issue

If your issue could not be resolved by reviewing this documentation, you can submit your issue on GitHub and receive support from the TDengine Team. When you submit an issue, attach the following directories from your TDengine deployment:

1. The directory containing TDengine logs (`/var/log/taos` by default)
2. The directory containing TDengine configuration files (`/etc/taos` by default)

In your GitHub issue, provide the version of TDengine and the operating system and environment for your deployment, the operations that you performed when the issue occurred, and the time of occurrence and affected tables.

To obtain more debugging information, open `taos.cfg` and set the `debugFlag` parameter to `135`. Then restart TDengine Server and reproduce the issue. The debug-level logs generated help the TDengine Team to resolve your issue. If it is not possible to restart TDengine Server, you can run the following command in the TDengine CLI to set the debug flag:

```
  alter dnode <dnode_id> 'debugFlag' '135';
```

You can run the `SHOW DNODES` command to determine the dnode ID.

When debugging information is no longer needed, set `debugFlag` to 131.

## Frequently Asked Questions

### 1. What are the best practices for upgrading a previous version of TDengine to version 3.0?

TDengine 3.0 is not compatible with the configuration and data files from previous versions. Before upgrading, perform the following steps:

1. Run `sudo rm -rf /etc/taos/taos.cfg` to delete your configuration file.
2. Run `sudo rm -rf /var/log/taos/` to delete your log files.
3. Run `sudo rm -rf /var/lib/taos/` to delete your data files.
4. Install TDengine 3.0.
5. For assistance in migrating data to TDengine 3.0, contact [TDengine Support](https://tdengine.com/support/).

### 2. How can I resolve the "Unable to establish connection" error?

This error indicates that the client could not connect to the server. Perform the following troubleshooting steps:

1. Check the network.

   - For machines deployed in the cloud, verify that your security group can access ports 6030 and 6031 (TCP and UDP).
   - For virtual machines deployed locally, verify that the hosts where the client and server are running are accessible to each other. Do not use localhost as the hostname.
   - For machines deployed on a corporate network, verify that your NAT configuration allows the server to respond to the client.

2. Verify that the client and server are running the same version of TDengine.

3. On the server, run `systemctl status taosd` to verify that taosd is running normally. If taosd is stopped, run `systemctl start taosd`.

4. Verify that the client is configured with the correct FQDN for the server.

5. If the server cannot be reached with the `ping` command, verify that network and DNS or hosts file settings are correct. For a TDengine cluster, the client must be able to ping the FQDN of every node in the cluster.

6. Verify that your firewall settings allow all hosts in the cluster to communicate on ports 6030 and 6041 (TCP and UDP). You can run `ufw status` (Ubuntu) or `firewall-cmd --list-port` (CentOS) to check the configuration.

7. If you are using the Python, Java, Go, Rust, C#, or Node.js client library on Linux to connect to the server, verify that `libtaos.so` is in the `/usr/local/taos/driver` directory and `/usr/local/taos/driver` is in the `LD_LIBRARY_PATH` environment variable.

8. If you are using macOS, verify that `libtaos.dylib` is in the `/usr/local/lib` directory and `/usr/local/lib` is in the `DYLD_LIBRARY_PATH` environment variable..

9. If you are using Windows, verify that `C:\TDengine\driver\taos.dll` is in the `PATH` environment variable. If possible, move `taos.dll` to the `C:\Windows\System32` directory.

10. On Linux/macOS, you can use the `nc` tool to check whether a port is accessible:
   - To check whether a UDP port is open, run `nc -vuz {hostIP} {port}`.
   - To check whether a TCP port on the server side is open, run `nc -l {port}`.
   - To check whether a TCP port on client side is open, run `nc {hostIP} {port}`.

  On Windows systems, you can run `Test-NetConnection -ComputerName {fqdn} -Port {port}` in PowerShell to check whether a port on the server side is accessible.

11. You can also use the TDengine CLI to diagnose network issues. For more information, see [Problem Diagnostics](https://docs.tdengine.com/operation/diagnose/).

### 3. How can I resolve the "Unable to resolve FQDN" error?

Clients and dnodes must be able to resolve the FQDN of each required node. You can confirm your configuration as follows:

1. Verify that the FQDN is configured properly on the server.
2. If your network has a DNS server, verify that it is operational.
3. If your network does not have a DNS server, verify that the FQDNs in the `hosts` file are correct.
4. On the client, use the `ping` command to test your connection to the server. If you cannot ping an FQDN, TDengine cannot reach it.
5. If TDengine has been previously installed and the `hostname` was modified, open `dnode.json` in the `data` folder and verify that the endpoint configuration is correct. The default location of the dnode file is `/var/lib/taos/dnode`. Ensure that you clean up previous installations before reinstalling TDengine.
6. Confirm whether FQDNs are preconfigured in `/etc/hosts` and `/etc/hostname`.

### 4. What is the most effective way to write data to TDengine?

Writing data in batches provides higher efficiency in most situations. You can insert one or more data records into one or more tables in a single SQL statement.

### 5. Why are table names not fully displayed?

The number of columns in the TDengine CLI terminal display is limited. This can cause table names to be cut off, and if you use an incomplete name in a statement, the "Table does not exist" error will occur. You can increase the display size with the `maxBinaryDisplayWidth` parameter or the SQL statement `set max_binary_display_width`. You can also append `\G` to your SQL statement to bypass this limitation.

### 6. How can I migrate data?

In TDengine, the `hostname` uniquely identifies a machine. When you move data files to a new machine, you must configure the new machine to have the same `host name` as the original machine.

:::note

The data structure of previous versions of TDengine is not compatible with version 3.0. To migrate from TDengine 1.x or 2.x to 3.0, you must export data from your older deployment and import it back into TDengine 3.0.

:::

### 7. How can I temporary change the log level from the TDengine Client?

To change the log level for debugging purposes, you can use the following command:

```sql
ALTER LOCAL local_option
 
local_option: {
    'resetLog'
  | 'rpcDebugFlag' 'value'
  | 'tmrDebugFlag' 'value'
  | 'cDebugFlag' 'value'
  | 'uDebugFlag' 'value'
  | 'debugFlag' 'value'
}
```

Use `resetlog` to remove all logs generated on the local client. Use the other parameters to specify a log level for a specific component.

For each parameter, you can set the value to `131` (error and warning), `135` (error, warning, and debug), or `143` (error, warning, debug, and trace).

### 8. Why do TDengine components written in Go fail to compile?

TDengine includes taosAdapter, an independent component written in Go. This component provides the REST API as well as data access for other products such as Prometheus and Telegraf.
When using the develop branch, you must run `git submodule update --init --recursive` to download the taosAdapter repository and then compile it.

TDengine Go components require Go version 1.14 or later.

### 9. How can I query the storage space being used by my data?

The TDengine data files are stored in `/var/lib/taos` by default. Log files are stored in `/var/log/taos`.

To see how much space your data files occupy, run `du -sh /var/lib/taos/vnode --exclude='wal'`. This excludes the write-ahead log (WAL) because its size is relatively fixed while writes are occurring, and it is written to disk and cleared when you shut down TDengine.

If you want to see how much space is occupied by a single database, first determine which vgroup is storing the database by running `show vgroups`. Then check `/var/lib/taos/vnode` for the files associated with the vgroup ID.

### 10. How is timezone information processed for timestamps?

TDengine uses the timezone of the client for timestamps. The server timezone does not affect timestamps. The client converts Unix timestamps in SQL statements to UTC before sending them to the server. When you query data on the server, it provides timestamps in UTC to the client, which converts them to its local time.

Timestamps are processed as follows:

1. The client uses its system timezone unless it has been configured otherwise.
2. A timezone configured in `taos.cfg` takes precedence over the system timezone.
3. A timezone explicitly specified when establishing a connection to TDengine through a client library takes precedence over `taos.cfg` and the system timezone. For example, the Java client library allows you to specify a timezone in the JDBC URL.
4. If you use an RFC 3339 timestamp (2013-04-12T15:52:01.123+08:00), or an ISO 8601 timestamp (2013-04-12T15:52:01.123+0800), the timezone specified in the timestamp is used instead of the timestamps configured using any other method. 

### 11. Which network ports are required by TDengine?

See [serverPort](https://docs.tdengine.com/reference/config/#serverport) in Configuration Parameters.

Note that ports are specified using 6030 as the default first port. If you change this port, all other ports change as well.

### 12. Why do applications such as Grafana fail to connect to TDengine over the REST API?

In TDengine, the REST API is provided by taosAdapter. Ensure that taosAdapter is running before you connect an application to TDengine over the REST API. You can run `systemctl start taosadapter` to start the service.

Note that the log path for taosAdapter must be configured separately. The default path is `/var/log/taos`. You can choose one of eight log levels. The default is `info`. You can set the log level to `panic` to disable log output. You can modify the taosAdapter configuration file to change these settings. The default location is `/etc/taos/taosadapter.toml`.

For more information, see [taosAdapter](https://docs.tdengine.com/reference/taosadapter/).

### 13. How can I resolve out-of-memory (OOM) errors?

OOM errors are thrown by the operating system when its memory, including swap, becomes insufficient and it needs to terminate processes to remain operational. Most OOM errors in TDengine occur for one of the following reasons: free memory is less than the value of `vm.min_free_kbytes` or free memory is less than the size of the request. If TDengine occupies reserved memory, an OOM error can occur even when free memory is sufficient.

TDengine preallocates memory to each vnode. The number of vnodes per database is determined by the `vgroups` parameter, and the amount of memory per vnode is determined by the `buffer` parameter. To prevent OOM errors from occurring, ensure that you prepare sufficient memory on your hosts to support the number of vnodes that your deployment requires. Configure an appropriately sized swap space. If you continue to receive OOM errors, your SQL statements may be querying too much data for your system. TDengine Enterprise Edition includes optimized memory management that increases stability for enterprise customers.
