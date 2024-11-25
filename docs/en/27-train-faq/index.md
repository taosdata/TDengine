---
title: Frequently Asked Questions
description: A compilation of solutions to common problems
slug: /frequently-asked-questions
---

## Issue Feedback

If the information in the FAQ does not help you and you need technical support and assistance from the TDengine technical team, please package the contents of the following two directories:

1. /var/log/taos (if you haven't modified the default path)
2. /etc/taos (if you haven't specified other configuration file paths)

Include a necessary description of the problem, including the version information of TDengine you are using, platform environment information, the operations that led to the issue, the symptoms of the problem, and the approximate time, and submit an issue on [GitHub](https://github.com/taosdata/TDengine).

To ensure there is enough debug information, if the problem can be reproduced, please modify the /etc/taos/taos.cfg file, adding a line "debugFlag 135" (without the quotes at all) at the end, then restart taosd, reproduce the problem, and then submit it. You can also temporarily set the log level of taosd with the following SQL statement.

```sql
alter dnode <dnode_id> 'debugFlag' '135';
```

Where dnode_id can be obtained from the output of the show dnodes; command.

However, when the system is running normally, please make sure to set debugFlag to 131; otherwise, it will generate a large amount of log information and reduce system efficiency.

## Common Issues List

### 1. What should I pay attention to when upgrading from versions prior to TDengine 3.0 to 3.0 and above?

Version 3.0 is a complete rewrite based on previous versions, and configuration files and data files are not compatible. Be sure to perform the following operations before upgrading:

1. Delete the configuration file by executing `sudo rm -rf /etc/taos/taos.cfg`
2. Delete log files by executing `sudo rm -rf /var/log/taos/`
3. Ensure that the data is no longer needed before deleting the data files by executing `sudo rm -rf /var/lib/taos/`
4. Install the latest stable version of TDengine 3.0
5. If you need to migrate data or if the data files are damaged, please contact the official technical support team of Taos Data for assistance.

### 2. What should I do if the JDBC Driver cannot find the dynamic link library on Windows?

Please refer to the [technical blog](https://www.taosdata.com/blog/2019/12/03/950.html) written for this issue.

### 3. How can I generate core files when TDengine crashes?

Please refer to the [technical blog](https://www.taosdata.com/blog/2019/12/06/974.html) written for this issue.

### 4. What should I do if I encounter the error "Unable to establish connection"?

When the client encounters a connection failure, please check the following steps:

1. Check the network environment
   - Cloud server: Check whether the security group of the cloud server opens access to TCP/UDP ports 6030/6041.
   - Local virtual machine: Check whether the network can ping, and try to avoid using `localhost` as the hostname.
   - Company server: If it is a NAT network environment, please ensure that the server can return messages to the client.
2. Ensure that the client and server version numbers are completely consistent; the open-source community version and the enterprise version cannot be mixed.
3. On the server, execute `systemctl status taosd` to check the `taosd` running status. If it is not running, start `taosd`.
4. Confirm that the correct server FQDN (Fully Qualified Domain Name - can be obtained by executing the Linux/macOS command hostname -f on the server) is specified when connecting the client; the FQDN configuration reference: [A Clear Explanation of TDengine's FQDN](https://www.taosdata.com/blog/2020/09/11/1824.html).
5. Ping the server FQDN; if there is no response, please check your network, DNS settings, or the system hosts file on the client machine. If deploying a TDengine cluster, the client needs to be able to ping all cluster nodes' FQDN.
6. Check firewall settings (use ufw status for Ubuntu, firewall-cmd --list-port for CentOS) to ensure that all hosts in the cluster can communicate via TCP/UDP protocol on ports 6030/6041.
7. For JDBC connections on Linux, macOS (similar for ODBC, Python, Go interfaces), ensure *libtaos.so* is in the directory `/usr/local/taos/driver`, and that `/usr/local/taos/driver` is in the system library search path `LD_LIBRARY_PATH`.
8. For JDBC connections on macOS, ensure `libtaos.dylib` is in the directory `/usr/local/lib`, and that `/usr/local/lib` is in the system library search path `LD_LIBRARY_PATH`.
9. For JDBC connections on Windows (ODBC, Python, Go, etc.), ensure `C:\TDengine\driver\taos.dll` is in your system library search directory (it is recommended to place `taos.dll` in the directory `C:\Windows\System32`).
10. If the connection fault cannot be ruled out
    - On Linux/macOS, use the command-line tool nc to determine whether TCP and UDP connections to the specified port are smooth. Check whether the UDP port connection works: `nc -vuz {hostIP} {port}`. Check whether the server-side TCP port connection works: `nc -l {port}`. Check whether the client-side TCP port connection works: `nc {hostIP} {port}`.
    - On Windows, use the PowerShell command Test-NetConnection -ComputerName \{fqdn} -Port \{port} to check if the service port is accessible.
11. You can also use the network connectivity detection feature built into the taos program to verify whether the specified port connection between the server and client is smooth: [Operations and Maintenance](../operations-and-maintenance/).

### 5. What should I do if I encounter the error "Unable to resolve FQDN"?

This error occurs because the client or data node cannot resolve the FQDN (Fully Qualified Domain Name). For TAOS Shell or client applications, please check the following:

1. Check whether the FQDN of the server you are connecting to is correct. The FQDN configuration reference: [A Clear Explanation of TDengine's FQDN](https://www.taosdata.com/blog/2020/09/11/1824.html).
2. If the network configuration has a DNS server, check whether it is functioning properly.
3. If the network does not have a DNS server, check the hosts file on the client machine to see whether the FQDN is configured and whether it has the correct IP address.
4. If the network configuration is OK, from the client machine, you should be able to ping the FQDN of the connection; otherwise, the client cannot connect to the server.
5. If the server has previously used TDengine and the hostname has been changed, it is advisable to check whether the dnode.json in the data directory conforms to the currently configured EP, with the default path being /var/lib/taos/dnode. Under normal circumstances, it is recommended to replace the data directory with a new one or back it up and delete the previous data directory to avoid this issue.
6. Check whether /etc/hosts and /etc/hostname are the pre-configured FQDN.

### 6. What is the most effective method for writing data?

Batch insertion. Each write statement can insert multiple records into a single table at the same time, or it can insert multiple records into multiple tables simultaneously.

### 7. How to solve the problem of Chinese characters being parsed as garbled text when inserting nchar type data on Windows?

If there are Chinese characters in the nchar data inserted on Windows, please first ensure that the system's regional settings are set to China (this can be set in the Control Panel). The `taos` client in cmd should then work correctly; if developing a Java application in an IDE like Eclipse or IntelliJ, please ensure that the file encoding in the IDE is set to GBK (the default encoding for Java). Then, initialize the client configuration when generating the Connection, as shown below:

```JAVA
Class.forName("com.taosdata.jdbc.TSDBDriver");
Properties properties = new Properties();
properties.setProperty(TSDBDriver.LOCALE_KEY, "UTF-8");
Connection = DriverManager.getConnection(url, properties);
```

### 8. The client on Windows cannot display Chinese characters properly?

Windows systems generally store Chinese characters in GBK/GB18030, while the default character set of TDengine is UTF-8. When using the TDengine client on Windows, the client driver converts characters to UTF-8 before sending them to the server for storage. Therefore, in application development, it's important to configure the correct current Chinese character set when calling interfaces.

If the TDengine client command-line tool `taos` cannot input or display Chinese characters correctly in a Windows 10 environment, you can configure the client `taos.cfg` as follows:

```text
locale C 
charset UTF-8
```

### 9. The table name is displayed incompletely

Due to the limited width of the TDengine CLI in the terminal, longer table names may not display fully. If operations are performed based on the truncated table name, a "Table does not exist" error may occur. To resolve this, you can modify the `maxBinaryDisplayWidth` setting in the `taos.cfg` file, or directly input the command `set max_binary_display_width 100`. Alternatively, you can use the `\G` parameter at the end of the command to adjust the display format.

### 10. How to perform data migration?

TDengine uniquely identifies a machine based on its hostname. When moving data files from machine A to machine B for version 3.0, you need to reconfigure the hostname of machine B to that of machine A.

Note: The storage structure of version 3.x is incompatible with the earlier 1.x and 2.x versions, requiring the use of migration tools or custom-developed applications to export and import data.

### 11. How to temporarily adjust the log level in the command-line program `taos`?

For debugging convenience, the command-line program `taos` has added commands related to logging:

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

The meaning is to clear all log files generated by the client on this machine (resetLog) or modify the log level of a specific module (only effective for the current command-line program; if the `taos` command-line program is restarted, it needs to be set again):

- The value can be: 131 (outputs error and warning logs), 135 (outputs error, warning, and debug logs), 143 (outputs error, warning, debug, and trace logs).

### 12. How to solve compilation failures when writing components in Go?

TDengine version 3.0 includes a standalone component `taosAdapter` developed in Go, which needs to be run separately, providing RESTful access and supporting data integration with various other software (Prometheus, Telegraf, collectd, StatsD, etc.).
To compile the latest code in the develop branch, you need to first run `git submodule update --init --recursive` to download the `taosAdapter` repository code before compiling.

Go version 1.14 or above is required. If you encounter Go compilation errors, it is often due to issues accessing Go modules from within China. You can solve this by setting the Go environment variables:

```sh
go env -w GO111MODULE=on
go env -w GOPROXY=https://goproxy.cn,direct
```

### 13. How to query the size of storage space occupied by data?

By default, TDengine's data files are stored in `/var/lib/taos`, and log files are stored in `/var/log/taos`.

To view the specific size occupied by all data files, you can execute the shell command: `du -sh /var/lib/taos/vnode --exclude='wal'`. Here, the WAL directory is excluded because, during continuous writing, its size is nearly fixed, and it will be cleared every time TDengine is normally shut down to let the data persist.

To check the size occupied by a single database, specify the database you want to check in the `taos` command-line program and execute `show vgroups;`. Then, you can check the folder size in `/var/lib/taos/vnode` corresponding to the obtained VGroup id.

### 14. How to ensure high availability of the client connection string?

Please refer to the technical blog written for this issue: [Technical Blog](https://www.taosdata.com/blog/2021/04/16/2287.html).

### 15. How is the timezone information of timestamps handled?

In TDengine, the timezone of timestamps is always handled by the client, independent of the server. Specifically, the client converts timestamps in SQL statements to UTC timezone (i.e., Unix timestamp) before sending them to the server for writing and querying; when reading data, the server also provides raw data in UTC, and the client converts the timestamps to the local timezone required by the local system for display.

When handling timestamp strings, the client follows this logic:

1. By default, the client uses the timezone setting of the operating system where it is located unless specified otherwise.
2. If the timezone parameter is set in `taos.cfg`, the client will follow the setting in this configuration file.
3. If a specific timezone is explicitly specified when establishing a database connection in the C/C++/Java/Python and various programming language Connector Drivers, that timezone setting will prevail. For example, there is a timezone parameter in the JDBC URL of the Java Connector.
4. When writing SQL statements, you can also directly use Unix timestamps (e.g., `1554984068000`) or timestamps in string format with time zones, namely in RFC 3339 format (e.g., `2013-04-12T15:52:01.123+08:00`) or ISO-8601 format (e.g., `2013-04-12T15:52:01.123+0800`). In this case, the value of these timestamps will not be affected by other timezone settings.

### 16. What network ports does TDengine 3.0 use?

Please refer to the document for the network ports used: [Operations and Maintenance](../operations-and-maintenance/).

It is important to note that the port numbers listed in the document are based on the default port 6030. If the settings in the configuration file are modified, the listed ports will also change accordingly, and the administrator can refer to the above information to adjust the firewall settings.

### 17. Why is there no response from the RESTful interface, Grafana cannot add TDengine as a data source, and TDengineGUI cannot connect successfully even when port 6041 is selected?

This phenomenon may be caused by `taosAdapter` not being started correctly. You need to execute the command: `systemctl start taosadapter` to start the `taosAdapter` service.

It should be noted that the log path of `taosAdapter` needs to be configured separately; the default path is `/var/log/taos`. The log level (`logLevel`) has 8 levels, with the default level being `info`. Configuring it to `panic` can turn off log output. Please pay attention to the size of the `/` directory in the operating system, and you can modify the configuration via command-line parameters, environment variables, or configuration files. The default configuration file is `/etc/taos/taosadapter.toml`.

For detailed information about the `taosAdapter` component, please refer to the document: [taosAdapter](../tdengine-reference/components/taosadapter/).

### 18. What should I do if I encounter OOM?

OOM is a protective mechanism of the operating system. When the operating system's memory (including SWAP) is insufficient, it kills some processes to ensure stable operation. Typically, memory shortages are caused by one of the following two reasons: either the remaining memory is less than `vm.min_free_kbytes`, or the memory requested by the program exceeds the remaining memory. Another situation is when memory is sufficient, but the program occupies special memory addresses, which can also trigger OOM.

TDengine pre-allocates memory for each VNode. The number of VNodes for each database is influenced by the `vgroups` parameter during database creation, and the amount of memory occupied by each VNode is affected by the `buffer` parameter. To prevent OOM, it is necessary to plan memory reasonably at the beginning of the project and set SWAP appropriately. In addition, querying excessive amounts of data may also lead to memory surges, depending on the specific query statement. The TDengine Enterprise Edition has optimized memory management and uses a new memory allocator. Users with higher stability requirements may consider choosing the Enterprise Edition.

### 19. What should I do if I encounter "Too many open files" on macOS?

If the `taosd` log file reports "Too many open files," it is because `taosd` has exceeded the system's open file limit.
The solution is as follows:

1. Create a file at /Library/LaunchDaemons/limit.maxfiles.plist and write the following content (the following example sets limit and maxfiles to 100,000; you can modify it as needed):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
"http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
<key>Label</key>
  <string>limit.maxfiles</string>
<key>ProgramArguments</key>
<array>
  <string>launchctl</string>
  <string>limit</string>
  <string>maxfiles</string>
  <string>100000</string>
  <string>100000</string>
</array>
<key>RunAtLoad</key>
  <true/>
<key>ServiceIPC</key>
  <false/>
</dict>
</plist>
```

2. Modify the file permissions

```shell
sudo chown root:wheel /Library/LaunchDaemons/limit.maxfiles.plist
sudo chmod 644 /Library/LaunchDaemons/limit.maxfiles.plist
```

3. Load the plist file (or restart the system for it to take effect. `launchd` will automatically load the plist in this directory upon startup)

```shell
sudo launchctl load -w /Library/LaunchDaemons/limit.maxfiles.plist
```

4. Confirm the updated limit

```shell
launchctl limit maxfiles
```

### 20. What to do when you see "Out of dnodes" when creating a database or "Vnodes exhausted" when creating a table?

This prompt indicates that the number of VNodes required to create the database is insufficient, and the required VNodes cannot exceed the upper limit of VNodes in the dnode. By default, there are twice the number of VNodes in a dnode as there are CPU cores, which can be controlled by the `supportVnodes` parameter in the configuration file.
You can normally increase the `supportVnodes` parameter in the `taos.cfg` file.

### 21. Why can I query data for a specified time period using `taos-CLI` on the server but not on the client machine?

This situation is caused by the client and server having inconsistent timezone settings, which can be resolved by adjusting the client and server to use the same timezone.

### 22. The table name is confirmed to exist, but I receive an error saying the table does not exist when writing or querying. What is the reason?

All names in TDengine, including database names and table names, are case-sensitive. If these names are not enclosed in backticks (\`) when used in a program or `taos-CLI`, even if you input them in uppercase, the engine will convert them to lowercase. If you add backticks around the names, the engine will not convert them to lowercase and will retain the original casing.

### 23. In `taos-CLI`, if the field content does not display fully, what should I do?

You can use the `\G` parameter to display vertically, such as `show databases\G;` (to facilitate input, you can press TAB after the backslash to auto-complete the subsequent content).

### 24. Why is data written using the `taosBenchmark` testing tool queried quickly, but the data I write is very slow to query?

If there is a serious out-of-order writing issue when writing data to TDengine, it can severely affect query performance, so it is necessary to resolve the out-of-order issue before writing. If the business is consuming data from Kafka for writing, please design the consumers reasonably, ensuring that data from one subtable is consumed and written by a single consumer to avoid design-induced disorder.

### 25. I want to calculate the time difference between two written records. How can I do that?

You can use the `DIFF` function to view the difference between the time column or numerical column of the two previous records, which is very convenient. For detailed descriptions, refer to the SQL manual -> Functions -> DIFF.

### 26. What does the error "DND ERROR Version not compatible, cliver: 3000700, server version: 3020300" mean?

This indicates that the client and server versions are incompatible. Here, the client version is 3.0.7.0 and the server version is 3.2.3.0. Currently, the compatibility strategy requires that the first three numbers match for the client and server to be compatible.

### 27. After changing the root password for the database, I encountered the error "failed to connect to server, reason: Authentication failure" when starting `taos`

By default, the `taos` service will attempt to connect to `taosd` using the system's default username (root) and password. After changing the root password, you need to specify the username and password when using `taos`, for example: `taos -h xxx.xxx.xxx.xxx -u root -p`, and then enter the new password to connect.

### 28. After changing the root password for the database, the Grafana monitoring plugin TDinsight does not display any data

The data displayed by the TDinsight plugin is collected and stored in the TD log library through the `taosKeeper` and `taosAdapter` services. After changing the root password, you need to synchronize the corresponding password information in the `taosKeeper` and `taosAdapter` configuration files, then restart the `taosKeeper` and `taosAdapter` services (Note: if it is a cluster, you need to restart the corresponding service on each node).

### 29. What should I do if I encounter the error "some vnode/qnode/mnode(s) out of service"?

The client has not configured the FQDN resolution for all server nodes. For example, if there are 3 nodes on the server, and the client has only configured the FQDN resolution for 1 node. For FQDN configuration reference, see: [An article that clarifies TDengine's FQDN](https://www.taosdata.com/blog/2020/09/11/1824.html).
