---
sidebar_label: FAQ
title: Frequently Asked Questions
---

## Submit an Issue

If the tips in FAQ don't help much, please submit an issue on [GitHub](https://github.com/taosdata/TDengine) to describe your problem description, including TDengine version, hardware and OS information, the steps to reproduce the problem, etc. It would be very helpful if you package the contents in `/var/log/taos` and `/etc/taos` and upload. These two are the default directories used by TDengine, if they have been changed in your configuration, please use according to the actual configuration. It's recommended to firstly set `debugFlag` to 135 in `taos.cfg`, restart `taosd`, then reproduce the problem and collect logs. If you don't want to restart, an alternative way of setting `debugFlag` is executing `alter dnode <dnode_id> debugFlag 135` command in TDengine CLI `taos`. During normal running, however, please make sure `debugFlag` is set to 131.

## Frequently Asked Questions

### 1. How to upgrade to TDengine 2.0 from older version?

version 2.x is not compatible with version 1.x regarding configuration file and data file, please do following before upgrading:

1. Delete configuration  files:  `sudo rm -rf /etc/taos/taos.cfg`
2. Delete log files:  `sudo rm -rf /var/log/taos/`
3. Delete data files if the data doesn't need to be kept: `sudo rm -rf /var/lib/taos/`
4. Install latests 2.x version
5. If the data needs to be kept and migrated to newer version, please contact professional service of TDengine for assistance

### 2. How to handle "Unable to establish connection"？

When the client is unable to connect to the server, you can try following ways to find out why.

1. Check the network

 - Check if the hosts where the client and server are running can be accessible to each other, for example by `ping` command.
 - Check if the TCP/UDP on port 6030-6042 are open for access if firewall is enabled. It's better to firstly disable firewall for diagnostics.
 - Check if the FQDN and serverPort are configured correctly in `taos.cfg` used by the server side
 - Check if the `firstEp` is set properly in the `taos.cfg` used by the client side

2. Make sure the client version and server version are same.

3. On server side, check the running status of `taosd` by executing `systemctl status taosd` . If your server is started using another way instead of `systemctl`, use the proper method to check whether the server process is running normally.

4. If using connector of Python, Java, Go, Rust, C#, node.JS on Linux to connect toe the server, please make sure `libtaos.so` is in directory `/usr/local/taos/driver` and `/usr/local/taos/driver` is in system lib search environment variable `LD_LIBRARY_PATH`.

5. If using connector on Windows, please make sure `C:\TDengine\driver\taos.dll` is in your system lib search path, it's suggested to put `taos.dll` under `C:\Windows\System32`.

6. Some advanced network diagnostics tools

 - On Linux system tool `nc` can be used to check whether the TCP/UDP can be accessible on a specified port
   Check whether a UDP port is open: `nc -vuz {hostIP} {port} `
   Check whether a TCP port on server side is open: `nc -l {port}`
   Check whether a TCP port on client side is open: `nc {hostIP} {port}`

 - On Windows system `Net-TestConnection -ComputerName {fqdn} -Port {port}` on PowerShell can be used to check whether the port on serer side is open for access.

7.  TDengine CLI `taos` can also be used to check network, please refer to [TDengine CLI](/reference/taos-shell).

### 3. How to handle "Unexpected generic error in RPC" or "Unable to resolve FQDN" ?

This error is caused because the FQDN can't be resolved. Please try following ways:

1. Check whether the FQDN is configured properly on the server side
2. If DSN server is configured in the network, please check whether it works; otherwise, check `/etc/hosts` to see whether the FQDN is configured with correct IP
3. If the network configuration on the server side is OK, try to ping the server from the client side.
4. If TDengine has been used before with an old hostname then the hostname has been changed, please check `/var/lib/taos/taos/dnode/dnodeEps.json`. Before setting up a new TDengine cluster, it's better to cleanup the directories configured.

### 4. "Invalid SQL" is returned even though the Syntax is correct

"Invalid SQL" is returned when the length of SQL statement exceeds maximum allowed length or the syntax is not correct.

### 5. Whether validation queries are supported?

It's suggested to use a builtin database named as `log` to monitor.

<a class="anchor" id="update"></a>

### 6. Can I delete a record?

From version 2.6.0.0 Enterprise version, deleting data can be supported.

### 7. How to create a table of over 1024 columns?

From version 2.1.7.0, at most 4096 columns can be defined for a table.

### 8. How to improve the efficiency of inserting data?

Inserting data in batch is a good practice. Single SQL statement can insert data for one or multiple tables in batch.

### 9. JDBC Error： the excuted SQL is not a DML or a DDL？

Please upgrade to latest JDBC driver, for details please refer to [Java Connector](/reference/connector/java)

### 10. Failed to connect with error "invalid timestamp"

The most common reason is that the time setting is not aligned on the client side and the server side. On Linux system, please use `ntpdate` command. On Windows system, please enable automatic sync in system time setting.

### 11. Table name is not shown in full

There is a display width setting in TDengine CLI `taos`. It can be controlled by configuration parameter `maxBinaryDisplayWidth`, or can be set using SQL command `set max_binary_display_width`. A more convenient way is to append `\G` in a SQL command to bypass this limitation.

### 12. How to change log level temporarily?

Below SQL command can be used to adjust log level temporarily

```sql
ALTER LOCAL flag_name flag_value;
```
 - flag_name can be: debugFlag，cDebugFlag，tmrDebugFlag，uDebugFlag，rpcDebugFlag
 - flag_value can be: 131 (INFO/WARNING/ERROR), 135 (plus DEBUG), 143 (plus TRACE)

<a class="anchor" id="timezone"></a>

### 13. Hhat to do if go compilation fails?

From version 2.3.0.0, a new component named `taosAdapter` is introduced. Its' developed in Go. If you want to compile from source code and meet go compilation problems, try to do below steps to resolve Go environment problems.

```sh
go env -w GO111MODULE=on
go env -w GOPROXY=https://goproxy.cn,direct
```
