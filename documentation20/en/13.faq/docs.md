# FAQ

Tutorials & FAQ

## 0.How to report an issue?

If the contents in FAQ cannot help you and you need the technical support and assistance of TDengine team, please package the contents in the following two directories:

1./var/log/taos (if default path has not been modified)

2./etc/taos

Provide the necessary description of the problem, including the version information of TDengine used, the platform environment information, the execution operation of the problem, the characterization of the problem and the approximate time, and submit the Issue on [GitHub](https://github.com/taosdata/TDengine).

To ensure that there is enough debug information, if the problem can be repeated, please modify the/etc/taos/taos.cfg file, add a line of "debugFlag 135" at the end (without quotation marks themselves), then restart taosd, repeat the problem, and then submit. You can also temporarily set the log level of taosd through the following SQL statement.

```
    alter dnode <dnode_id> debugFlag 135;
```

However, when the system is running normally, please set debugFlag to 131, otherwise a large amount of log information will be generated and the system efficiency will be reduced.

## 1.What should I pay attention to when upgrading TDengine from older versions to 2.0 and above? ☆☆☆

Version 2.0 is a complete refactoring of the previous version, and the configuration and data files are incompatible. Be sure to do the following before upgrading:

1. Delete the configuration file, execute sudo rm `-rf /etc/taos/taos.cfg`
2. Delete the log file, execute `sudo rm -rf /var/log/taos/`
3. By ensuring that the data is no longer needed, delete the data file and execute `sudo rm -rf /var/lib/taos/`
4. Install the latest stable version of TDengine
5. If you need to migrate data or the data file is corrupted, please contact the official technical support team of TAOS Data to assist

## 2. When encoutered with the error " Unable to establish connection " in Windows, what can I do?

See the [technical blog](https://www.taosdata.com/blog/2019/12/03/jdbcdriver%E6%89%BE%E4%B8%8D%E5%88%B0%E5%8A%A8%E6%80%81%E9%93%BE%E6%8E%A5%E5%BA%93/) for this issue.

## 3. Why I get “more dnodes are needed” when create a table?

See the [technical blog](https://www.taosdata.com/blog/2019/12/03/%E5%88%9B%E5%BB%BA%E6%95%B0%E6%8D%AE%E8%A1%A8%E6%97%B6%E6%8F%90%E7%A4%BAmore-dnodes-are-needed/) for this issue.

## 4. How do I generate a core file when TDengine crashes?

See the [technical blog](https://www.taosdata.com/blog/2019/12/06/tdengine-crash%E6%97%B6%E7%94%9F%E6%88%90core%E6%96%87%E4%BB%B6%E7%9A%84%E6%96%B9%E6%B3%95/) for this issue.

## 5. What should I do if I encounter an error "Unable to establish connection"?

When the client encountered a connection failure, please follow the following steps to check:

1. Check your network environment

2. - Cloud server: Check whether the security group of the cloud server opens access to TCP/UDP ports 6030-6042
   - Local virtual machine: Check whether the network can be pinged, and try to avoid using localhost as hostname
   - Corporate server: If you are in a NAT network environment, be sure to check whether the server can return messages to the client

2. Make sure that the client and server version numbers are exactly the same, and the open source Community Edition and Enterprise Edition cannot be mixed.
3. On the server, execute systemctl status taosd to check the running status of *taosd*. If not running, start *taosd*.
4. Verify that the correct server FQDN (Fully Qualified Domain Name, which is available by executing the Linux command hostname-f on the server) is specified when the client connects. FQDN configuration reference: "[All about FQDN of TDengine](https://www.taosdata.com/blog/2020/09/11/1824.html)".
5. Ping the server FQDN. If there is no response, please check your network, DNS settings, or the system hosts file of the computer where the client is located.
6. Check the firewall settings (Ubuntu uses ufw status, CentOS uses firewall-cmd-list-port) to confirm that TCP/UDP ports 6030-6042 are open.
7. For JDBC (ODBC, Python, Go and other interfaces are similar) connections on Linux, make sure that libtaos.so is in the directory /usr/local/taos/driver, and /usr/local/taos/driver is in the system library function search path LD_LIBRARY_PATH.
8. For JDBC, ODBC, Python, Go, etc. connections on Windows, make sure that C:\ TDengine\ driver\ taos.dll is in your system library function search directory (it is recommended that taos.dll be placed in the directory C:\ Windows\ System32)
9. If the connection issue still exist

1. - On Linux system, please use the command line tool nc to determine whether the TCP and UDP connections on the specified ports are unobstructed. Check whether the UDP port connection works: nc -vuz {hostIP} {port} Check whether the server-side TCP port connection works: nc -l {port}Check whether the client-side TCP port connection works: nc {hostIP} {port}
   - Windows systems use the PowerShell command Net-TestConnection-ComputerName {fqdn} Port {port} to detect whether the service-segment port is accessed

10. You can also use the built-in network connectivity detection function of taos program to verify whether the specified port connection between the server and the client is unobstructed (including TCP and UDP): [TDengine's Built-in Network Detection Tool Use Guide](https://www.taosdata.com/blog/2020/09/08/1816.html).



## 6.What to do if I encounter an error "Unexpected generic error in RPC" or "TDengine error: Unable to resolve FQDN"?

This error occurs because the client or data node cannot parse the FQDN (Fully Qualified Domain Name). For TAOS shell or client applications, check the following:

1. Please verify whether the FQDN of the connected server is correct. FQDN configuration reference: "[All about FQDN of TDengine](https://www.taosdata.com/blog/2020/09/11/1824.html)".
2. If the network is configured with a DNS server, check that it is working properly.
3. If the network does not have a DNS server configured, check the hosts file of the machine where the client is located to see if the FQDN is configured and has the correct IP address.
4. If the network configuration is OK, from the machine where the client is located, you need to be able to ping the connected FQDN, otherwise the client cannot connect to the server

## 7.Although the syntax is corrected, why do I still get the “Invalid SQL" error?

If you confirm that the syntax is correct, for versions older than 2.0, please check whether the SQL statement length exceeds 64K. If it does, this error will also be returned.

## 8. Are “validation queries” supported?

The TDengine does not yet have a dedicated set of validation queries. However, it is recommended to use the database "log" monitored by the system.

## 9. Can I delete or update a record?

TDengine does not support the deletion function at present, and may support it in the future according to user requirements.

Starting from 2.0. 8.0, TDengine supports the function of updating written data. Using the update function requires using UPDATE 1 parameter when creating the database, and then you can use INSERT INTO command to update the same timestamp data that has been written. UPDATE parameter does not support ALTER DATABASE command modification. Without a database created using UPDATE 1 parameter, writing data with the same timestamp will not modify the previous data with no error reported.

It should also be noted that when UPDATE is set to 0, the data with the same timestamp sent later will be discarded directly, but no error will be reported, and will still be included in affected rows (so the return information of INSERT instruction cannot be used for timestamp duplicate checking). The main reason for this design is that TDengine regards the written data as a stream. Regardless of whether the timestamp conflicts or not, TDengine believes that the original device that generates the data actually generates such data. The UPDATE parameter only controls how such stream data should be processed when persistence-when UPDATE is 0, it means that the data written first overwrites the data written later; When UPDATE is 1, it means that the data written later overwrites the data written first. How to choose this coverage relationship depends on whether the data generated first or later is expected in the subsequent use and statistics compile.

## 10. How to create a table with more than 1024 columns?

Using version 2.0 and above, 1024 columns are supported by default; for older versions, TDengine allowed the creation of a table with a maximum of 250 columns. However, if the limit is exceeded, it is recommended to logically split this wide table into several small ones according to the data characteristics.

## 11. What is the most effective way to write data?

Insert in batches. Each write statement can insert multiple records into one or multiple tables at the same time.

## 12. What is the most effective way to write data? How to solve the problem that Chinese characters in nchar inserted under Windows systems are parsed into messy code?

If there are Chinese characters in nchar data under Windows, please first confirm that the region of the system is set to China (which can be set in the Control Panel), then the taos client in cmd should already support it normally; If you are developing Java applications in an IDE, such as Eclipse and Intellij, please confirm that the file code in the IDE is GBK (this is the default coding type of Java), and then initialize the configuration of the client when generating the Connection. The specific statement is as follows:

```JAVA
Class.forName("com.taosdata.jdbc.TSDBDriver");
Properties properties = new Properties();
properties.setProperty(TSDBDriver.LOCALE_KEY, "UTF-8");
Connection = DriverManager.getConnection(url, properties);
```

## 13. JDBC error: the excluded SQL is not a DML or a DDL?

Please update to the latest JDBC driver.

```xml
<dependency>
  <groupId>com.taosdata.jdbc</groupId>
  <artifactId>taos-jdbcdriver</artifactId>
  <version>2.0.27</version>
</dependency>
```

## 14. taos connect failed, reason: invalid timestamp.

The common reason is that the server time and client time are not calibrated, which can be calibrated by synchronizing with the time server (use ntpdate command under Linux, and select automatic synchronization in the Windows time setting).

## 15. Incomplete display of table name

Due to the limited display width of taos shell in the terminal, it is possible that a relatively long table name is not displayed completely. If relevant operations are carried out according to the displayed incomplete table name, a Table does not exist error will occur. The workaround can be by modifying the setting option maxBinaryDisplayWidth in the taos.cfg file, or directly entering the command `set max_binary_display_width 100`. Or, use the \\G parameter at the end of the command to adjust how the results are displayed.

## 16. How to migrate data?

TDengine uniquely identifies a machine according to hostname. When moving data files from machine A to machine B, pay attention to the following three points:

- For versions 2.0. 0.0 to 2.0. 6. x, reconfigure machine B's hostname to machine A's.
- For 2.0. 7.0 and later versions, go to/var/lib/taos/dnode, repair the FQDN corresponding to dnodeId of dnodeEps.json, and restart. Make sure this file is identical for all machines.
- The storage structures of versions 1. x and 2. x are incompatible, and it is necessary to use migration tools or your own application to export and import data.

## 17. How to temporarily adjust the log level in command line program taos?

For the convenience of debugging, since version 2.0. 16, command line program taos gets two new instructions related to logging:

```mysql
ALTER LOCAL flag_name flag_value;
```

This means that under the current command line program, modify the loglevel of a specific module (only valid for the current command line program, if taos is restarted, it needs to be reset):

- The values of flag_name can be: debugFlag, cDebugFlag, tmrDebugFlag, uDebugFlag, rpcDebugFlag
- Flag_value values can be: 131 (output error and alarm logs), 135 (output error, alarm, and debug logs), 143 (output error, alarm, debug, and trace logs)

```mysql
ALTER LOCAL RESETLOG;
```

This means wiping up all client-generated log files on the machine.

