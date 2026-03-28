---
title: Frequently Asked Questions
slug: /frequently-asked-questions
---

## Issue Feedback

If the information in the FAQ does not help you, and you need technical support and assistance from the TDengine technical team, please package the contents of the following two directories:

1. `/var/log/taos` (if the default path has not been modified)
1. `/etc/taos` (if no other configuration file path has been specified)

Attach the necessary problem description, including the version information of TDengine used, platform environment information, the operations performed when the problem occurred, the manifestation of the problem, and the approximate time, and submit an issue on [GitHub](https://github.com/taosdata/TDengine).

To ensure there is enough debug information, if the problem can be reproduced, please modify the /etc/taos/taos.cfg file, add a line at the end "debugFlag 135" (without the quotes themselves), then restart taosd, reproduce the problem, and then submit it. You can also temporarily set the log level of taosd using the following SQL statement.

```sql
alter dnode <dnode_id> 'debugFlag' '135';
```

Get the dnode_id from the output of the show dnodes; command.

However, when the system is running normally, be sure to set the debugFlag to 131, otherwise, it will generate a large amount of log information and reduce system efficiency.

## List of Common Questions

- [1. Installation & Deployment](#installation-and-deployment)
- [2. Connection](#connection)
- [3. Data Writing](#data-writing)
- [4. Data Query](#data-query)
- [5. Data Subscription (TMQ)](#data-subscription)
- [6. Operations & Monitoring](#operations-and-monitoring)
- [7. Upgrade & Migration](#upgrade-and-migration)
- [8. Client & Tools](#client-and-tools)

## 1. Installation & Deployment {#installation-and-deployment}

### 1.1 How to solve the MSVCP1400.DLL error when running TDengine on Windows?

1. Reinstall Microsoft Visual C++ Redistributable: As msvcp140.dll is part of Microsoft Visual C++ Redistributable, reinstalling this package usually resolves most issues. You can download the corresponding version from the official Microsoft website for installation
2. Manually download and replace the msvcp140.dll file online: You can download the msvcp140.dll file from a reliable source and copy it to the corresponding directory in the system. Ensure that the downloaded files match your system architecture (32-bit or 64-bit) and ensure the security of the source

### 1.2 How to resolve compilation failures of components written in Go?

Version 3.0 of TDengine includes a standalone component developed in Go called `taosAdapter`, which needs to be run separately to provide RESTful access and support data access from various other software (Prometheus, Telegraf, collectd, StatsD, etc.). To compile using the latest develop branch code, first run `git submodule update --init --recursive` to download the `taosAdapter` repository code before compiling.

The Go language version requirement is 1.14 or higher. If there are Go compilation errors, often due to issues accessing Go mod in China, they can be resolved by setting Go environment variables:

```shell
go env -w GO111MODULE=on
go env -w GOPROXY=https://goproxy.cn,direct
```

### 1.3 What should I do if pulling the TDengine image from Docker Hub fails? {#docker-hub-failure}

If you cannot access the Docker Hub official registry (hub.docker.com), try the following:

- Check that your network connection is working.
- Download the image file from the [TDengine Download Center](https://www.taosdata.com/download-center), then load it with `docker load`. Refer to the usage instructions on the download page.
- Try an alternative mirror registry, such as [CNIX Internal Container Registry Mirror](https://m.ixdev.cn/). This mirror is unaffiliated with Taos Data — if it is unavailable, look for another mirror and follow its usage instructions.

### 1.4 How can I obtain all JDBC driver dependency JARs for an air-gapped (offline) environment?

Problem Description:
In a private deployment with no access to Maven Central, all JDBC driver dependency JARs need to be uploaded to an internal repository.

Problem Solution:
After downloading the TDengine JDBC driver source, run the following command to export all compile-scope dependencies to the `./lib` directory:

```bash
mvn dependency:copy-dependencies -DoutputDirectory=./lib -DincludeScope=compile
```

Then upload the contents of `./lib` to your internal Maven repository.

## 2. Connection {#connection}

### 2.1 What should I do if I encounter the error "Unable to establish connection"?

If the client encounters a connection failure, please follow the steps below to check:

1. Check the network environment

   - Cloud server: Check if the security group of the cloud server has opened access permissions for TCP/UDP ports 6030/6041
   - Local virtual machine: Check if the network can ping through, try to avoid using `localhost` as the hostname
   - Company server: If it is a NAT network environment, be sure to check if the server can return messages to the client

1. Ensure that the client and server version numbers are exactly the same, open source TSDB-OSS edition and TSDB-Enterprise edition cannot be mixed

1. On the server, execute `systemctl status taosd` to check the *taosd* running status. If it is not running, start *taosd*

1. Confirm that the correct server FQDN (Fully Qualified Domain Name —— can be obtained by executing the Linux/macOS command hostname -f on the server) was specified when the client connected.

1. Ping the server FQDN, if there is no response, please check your network, DNS settings, or the system hosts file of the client's computer. If a TDengine cluster is deployed, the client needs to be able to ping all cluster node FQDNs.

1. Check the firewall settings (use ufw status on Ubuntu, use firewall-cmd --list-port on CentOS), ensure that all hosts in the cluster can communicate on ports 6030/6041 for TCP/UDP protocols.

1. For Linux JDBC (ODBC, Python, Go, and similar interfaces) connections, ensure that `libtaos.so` is in the directory `/usr/local/taos/driver`, and that `/usr/local/taos/driver` is in the system library search path `LD_LIBRARY_PATH`

1. For JDBC connections on macOS (similar for ODBC, Python, Go, etc.), ensure that `libtaos.dylib` is in the directory `/usr/local/lib`, and that `/usr/local/lib` is included in the system library search path `LD_LIBRARY_PATH`.

1. For JDBC, ODBC, Python, Go, etc., connections on Windows, ensure that `C:\TDengine\driver\taos.dll` is in your system library search directory (it is recommended to place `taos.dll` in the directory `C:\Windows\System32`).

1. If you still cannot eliminate connection issues:

   - On Linux/macOS, use the command line tool nc to separately check if the TCP and UDP connections on the specified port are clear:
     - Check if the UDP port connection is working: `nc -vuz {hostIP} {port}`
     - Check if the server-side TCP port connection is working: `nc -l {port}`
     - Check if the client-side TCP port connection is working: `nc {hostIP} {port}`

   - On Windows, use the PowerShell command `Test-NetConnection -ComputerName {fqdn} -Port {port}` to check if the server-side port is accessible.

1. You can also use the network connectivity test feature embedded in the taos program to verify whether the specified port connection between the server and client is clear: [Operation Guide](../operations-and-maintenance/).

### 2.2 What to do if you encounter the error "Unable to resolve FQDN"?

This error occurs because the client or data node cannot resolve the FQDN (Fully Qualified Domain Name). For the TDengine CLI or client applications, please check the following:

1. Check if the FQDN of the server you are connecting to is correct.
1. If there is a DNS server in the network configuration, check if it is working properly
1. If there is no DNS server configured in the network, check the hosts file on the client machine to see if the FQDN is configured and has the correct IP address
1. If the network configuration is OK, you need to be able to ping the FQDN from the client machine, otherwise the client cannot connect to the server
1. If the server has previously used TDengine and changed the hostname, it is recommended to check if the dnode.json in the data directory matches the currently configured EP, typically located at /var/lib/taos/dnode. Normally, it is advisable to change to a new data directory or backup and delete the previous data directory to avoid this issue.
1. Check /etc/hosts and /etc/hostname for the pre-configured FQDN

### 2.3 Why is there no response from the RESTful interface, Grafana cannot add TDengine as a data source, or TDengineGUI cannot connect even using port 6041?

This phenomenon may be caused by taosAdapter not being started correctly. You need to execute: `systemctl start taosadapter` to start the taosAdapter service.

It should be noted that the log path for taosAdapter needs to be configured separately, the default path is /var/log/taos; there are 8 levels of logLevel, the default level is info, setting it to panic can disable log output. Be aware of the space size of the operating system's / directory, which can be modified through command line parameters, environment variables, or configuration files. The default configuration file is /etc/taos/taosadapter.toml.

For a detailed introduction to the taosAdapter component, please see the document: [taosAdapter](../tdengine-reference/components/taosadapter/)

### 2.4 How to ensure high availability of client connection strings?

Please refer to the [technical blog](https://www.taosdata.com/blog/2021/04/16/2287.html) written for this issue.

### 2.5 Encountering error "DND ERROR Version not compatible, cliver: 3000700 swr wer: 3020300"

This indicates that the client and server versions are incompatible. Here, the cliver version is 3.0.7.0, and the server version is 3.2.3.0. The current compatibility strategy is that the first three digits must match for the client and server to be compatible.

### 2.6 After changing the root password of the database, starting taos encounters the error "failed to connect to server, reason: Authentication failure"

By default, starting the taos service will use the system's default username (root) and password to attempt to connect to taosd. After changing the root password, starting a taos connection will require specifying the username and password, for example: `taos -h xxx.xxx.xxx.xxx -u root -p`, then enter the new password to connection. After changing the password, you also need to modify the password in the configuration file of the taosKeeper component (located at /etc/taos/taoskeeper.toml by default) and restart the service.

Starting from version 3.3.6.6, a new environment variable `TAOS_ROOT_PASSWORD` is introduced for TDengine TSDB Docker image, to set the custom password. When starting a container with the `docker run` command, you can add the `-e TAOS_ROOT_PASSWORD=<password>` parameter to use the custom password to start the TDengine TSDB service, without the need to manually modify the password in the configuration files.

For versions 3.3.6.6 to 3.3.8.4 in Docker environments, if you changed the password in an older version, you need to touch an empty file named `.docker-entrypoint-root-password-changed` in the data directory (default is `/var/lib/taos`), then restart the container.

For version 3.3.8.8 and above in Docker environments, you can upgrade directly.

### 2.7 Encountering error "some vnode/qnode/mnode(s) out of service", what to do?

The client has not configured the FQDN resolution for all server nodes. For example, if there are 3 nodes on the server, the client has only configured the FQDN resolution for 1 node.

### 2.8 What should I do if I encounter "Sync leader is unreachable" when connecting to the cluster for the first time?

Reporting this error indicates that the first connection to the cluster was successful, but the IP address accessed for the first time was not the leader of mnode. An error occurred when the client attempted to establish a connection with the leader. The client searches for the leader node through EP, which specifies the fqdn and port number. There are two common reasons for this error:

- The ports of other dnodes in the cluster are not open
- The client's hosts file is not configured correctly

Therefore, first, check whether all ports on the server and cluster (default 6030 for native connections and 6041 for HTTP connections) are open; Next, check if the client's hosts file has configured the fqdn and IP information for all dnodes in the cluster.
If the issue still cannot be resolved, it is necessary to contact Taos technical personnel for support.

### 2.9 What should I do if loading "libtaosnative.so" or "libtaosws.so" fails with "No such file or directory" or "failed to load libtaosws.so"?

Problem Description:
When using TDengine TSDB client applications (taos-CLI, taosBenchmark, taosdump, etc.) or client connectors (such as Java, Python, Go, etc.), you may encounter errors when loading the dynamic link libraries "libtaosnative.so" or "libtaosws.so".
For example: `failed to load libtaosws.so since No such file or directory [0x80FF0002]`

Problem Cause:
This occurs because the client cannot find the required dynamic link library files, possibly due to incorrect installation or improper configuration of the system library path.

Problem Solution:

- **Check files**: Verify that the symbolic link files `libtaosnative.so` or `libtaosws.so` and their corresponding actual files exist in the system shared library directory and are complete. If the symbolic links or actual files are missing, reinstall them as they are included in both the TDengine TSDB client and server installation packages.
- **Check environment variables**: Ensure that the system shared library loading directory environment variable `LD_LIBRARY_PATH` includes the directory where `libtaosnative.so` or `libtaosws.so` files are located. If not included, add it with `export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:<new_path>`.
- **Check permissions**: Ensure that the current user has read and execute permissions for both the `libtaosnative.so` or `libtaosws.so` symbolic links and their actual files.
- **Check file corruption**: You can verify the integrity of the library files using the command `readelf -h library_file`.
- **Check file dependencies**: You can view the dependencies of the library files using the command `ldd library_file` to ensure that all dependencies are correctly installed and accessible.

### 2.10 What should I do if JDBC Driver cannot find the dynamic link library on Windows platform?

Please refer to the [technical blog](https://www.taosdata.com/blog/2019/12/03/950.html) written for this issue.

### 2.11 What should I do if JDBC native connection throws "UnsatisfiedLinkError: no taos in java.library.path"?

Problem Description:
When using a JDBC native connection, you encounter an error similar to `java.lang.UnsatisfiedLinkError: no taos in java.library.path`.

Problem Cause:
The client cannot find the libtaos dynamic library, typically because the TDengine client (taosc) is not installed, or the library directory is not included in `java.library.path`.

Problem Solution:

1. Verify that the TDengine client (taosc) is installed.
2. Check that `java.library.path` includes the directory containing libtaos (typically `/usr/local/taos/driver` on Linux/macOS, or `C:\TDengine\driver` on Windows). If not, add it via the JVM startup argument `-Djava.library.path=<path>`, or add the directory to `LD_LIBRARY_PATH` (Linux/macOS) / `PATH` (Windows).
3. macOS users on older versions should upgrade to the latest TDengine client.

### 2.12 What should I do if JDBC native connection reports "Operation not permitted"?

Problem Description:
When using a JDBC native connection, you encounter an `Operation not permitted` error.

Problem Cause:
The current user does not have write permission to the TDengine log directory, causing client initialization to fail.

Problem Solution:
Check the permissions of the TDengine log directory (default `/var/log/taos`). Ensure the user running the Java application has write access, or configure a writable directory using the `logDir` setting.

### 2.13 What should I do if JDBC WebSocket connection times out with error "can't create connection with server within: 60000 milliseconds" (error code 0x231d)?

Problem Description:
The call stack looks like:

```plaintext
java.sql.SQLException: ERROR (0x231d): can't create connection with server within: 60000 milliseconds
        at com.taosdata.jdbc.ws.Transport.checkConnection(Transport.java:393)
```

Problem Cause:
Network unreachable, port not open, incorrect Nginx proxy configuration, or JAR dependency conflict.

Problem Solution:

1. Verify network connectivity and that the taosAdapter port (default 6041) is accessible:

   ```bash
   ping <adapterIp>
   telnet <host> 6041
   ```

2. If the taos client is installed, test the Adapter connection directly:

   ```bash
   taos -Z 1 -h <host> -P 6041
   ```

3. If the taos client is not installed, verify the Adapter HTTP port with curl:

   ```bash
   curl -L -H "Authorization: Basic cm9vdDp0YW9zZGF0YQ==" \
     -d "show databases;" \
     <host>:6041/rest/sql
   ```

4. If Nginx is used as a reverse proxy, verify WebSocket support is configured correctly (see question 2.14).
5. If all of the above are normal, run `mvn dependency:tree` to check for conflicting JSON libraries (e.g., jackson or fastjson version conflicts).

### 2.14 What should I do if JDBC through Nginx reports "WebSocket handshake error, code: 400 Bad Request"?

Problem Description:
When connecting to taosAdapter through an Nginx reverse proxy, JDBC reports `WebSocket handshake error, code: 400 Bad Request`.

Problem Cause:
Nginx is not configured to pass the HTTP upgrade headers required for WebSocket connections.

Problem Solution:
Add WebSocket support to the Nginx configuration:

```nginx
location /ws {
    proxy_pass http://<taosadapter>:6041;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "Upgrade";
    proxy_set_header Host $host;
}
```

### 2.15 What should I do if JDBC connections drop frequently when using Nginx as a proxy?

Problem Description:
JDBC WebSocket connections routed through Nginx disconnect after some time.

Problem Cause:
Nginx's `proxy_send_timeout` and `proxy_read_timeout` values are too low, causing long-lived connections to be closed prematurely.

Problem Solution:
Increase the timeout values in the Nginx configuration:

```nginx
proxy_send_timeout 3600s;
proxy_read_timeout 3600s;
```

### 2.16 How should I configure a JDBC connection pool (HikariCP example)?

The recommended configuration is shown below. There is no need to configure `validationQuery` — JDBC driver 3.7.5 and later caches the `isValid` call:

```java
config.setMinimumIdle(10);           // minimum number of idle connections
config.setMaximumPoolSize(10);       // maximum pool size
config.setConnectionTimeout(30000);  // max wait time to get a connection (ms)
config.setMaxLifetime(0);            // max connection lifetime, 0 = unlimited
config.setIdleTimeout(0);            // idle connection timeout, 0 = unlimited
```

Use `show connections;` to verify that the actual connection count matches your pool configuration.

## 3. Data Writing {#data-writing}

### 3.1 What is the most effective method for data insertion?

Batch insertion. Each insert statement can insert multiple records into one table at the same time, or multiple records into multiple tables simultaneously.

### 3.2 How to solve the issue of Chinese characters in nchar type data being parsed as garbled text on Windows systems?

When inserting nchar type data containing Chinese characters on Windows, first ensure that the system's regional settings are set to China (this can be set in the Control Panel). At this point, the `taos` client in cmd should already be working properly; if developing a Java application in an IDE, such as Eclipse or IntelliJ, ensure that the file encoding in the IDE is set to GBK (which is the default encoding type for Java), then initialize the client configuration when creating the Connection, as follows:

```java
Class.forName("com.taosdata.jdbc.TSDBDriver");
Properties properties = new Properties();
properties.setProperty(TSDBDriver.LOCALE_KEY, "UTF-8");
Connection = DriverManager.getConnection(url, properties);
```

### 3.3 Why is querying very fast when using the taosBenchmark testing tool to write data, but very slow when I write data?

When TDengine writes data, severe disorder in the write sequence can significantly affect query performance. Therefore, it is necessary to resolve the disorder before writing. If the business writes from Kafka consumption, please design the consumer reasonably, try to have one consumer consume and write the data of one subtable to avoid disorder caused by design.

### 3.4 Database upgrade from version 2.6 to 3.3: when data migration is carried out and data is being written in the business at the same time, will there be serious out-of-order issues?

In this situation, out-of-order issues generally won't occur. First, let's explain what out-of-order means in TDengine. In TDengine, out-of-order refers to the situation where, starting from a timestamp of 0, time windows are cut according to the Duration parameter set in the database (the default is 10 days). The out-of-order phenomenon occurs when the data written in each time window is not written in chronological order. As long as the data written in the same window is in order, even if the writing between windows is not sequential, there will be no out-of-order situation.

Then, looking at the above scenario, when the backfill of old data and the writing of new data are carried out simultaneously, there is generally a large time gap between the old and new data, and they won't fall within the same window. As long as both the old and new data are written in order, there will be no out-of-order phenomenon.

### 3.5 What should I do if JDBC write reports "Invalid message" (error code 0x115)?

Problem Description:
When writing with auto-create table (using JDBC), you encounter `(0x115): Invalid message`.

Problem Cause:
The subtable being written already exists under a different supertable, causing a conflict.

Problem Solution:
Verify that the supertable specified in the write statement matches the supertable the subtable actually belongs to.

### 3.6 What should I do if a Java write application becomes unresponsive with near-zero QPS?

Problem Description:
A Java application writing to TDengine freezes after running for some time, with QPS dropping to near zero.

Problem Cause:
This is typically caused by JVM GC pressure, not a memory leak in the JDBC driver.

Problem Solution:

1. Run `top -Hp <pid>` to check if GC threads are consuming excessive CPU.
2. Run `jstat -gcutil <pid> 3000 100` to check whether Young GC has stopped and Full GC is dominating.
3. If GC is confirmed as the cause, profile the application to find and reduce excessive object allocation.

### 3.7 JDBC write performance is low — how do I diagnose and improve it?

Common checkpoints:

1. **Physical resources**: Ensure the server uses SSD storage and a 10 Gbps network. Eliminate low-speed networks (e.g., Wi-Fi) when benchmarking locally.
2. **Server-side pressure**: Monitor taosd CPU, memory, network, and disk I/O. Very low CPU on the server usually means requests are not reaching it efficiently.
3. **VGROUP count**: The default VGROUP count when creating a database is 2. Increase it if write concurrency is high.
4. **Stmt object reuse**: Frequently creating new `PreparedStatement` (Stmt) objects significantly hurts performance. Create Stmt objects at application startup and reuse them throughout. Refer to the [high-throughput write example](https://docs.taosdata.com/develop/high/). Use `show queries;` to verify long-running parameter-binding statements are present, confirming Stmt reuse.
5. **ORM frameworks**: When using MyBatis or similar frameworks, check whether writes have degraded to one row per commit. Refer to the [MyBatis write example](https://github.com/taosdata/TDengine/blob/main/docs/examples/JDBC/mybatisplus-demo/src/test/java/com/taosdata/example/mybatisplusdemo/mapper/MetersMapperTest.java).
6. **Skip TAG when subtable exists**: If the subtable already exists, omit the TAG columns to reduce overhead:

   ```sql
   INSERT INTO meters (tbname, ts, current, voltage, phase) VALUES(?, ?, ?, ?, ?)
   ```

## 4. Data Query {#data-query}

### 4.1 How is time zone information handled for timestamps?

In TDengine, the time zone of timestamps is always handled by the client, independent of the server. Specifically, the client converts timestamps in SQL statements to the UTC time zone (i.e., Unix Timestamp) before sending them to the server for writing and querying; when reading data, the server also provides the original data in the UTC time zone, and the client then converts the timestamps to the local time zone required by the local system for display.

The client handles timestamp strings with the following logic:

1. By default, without special settings, the client uses the time zone settings of the operating system it is running on.
1. If the `timezone` parameter is set in taos.cfg, the client will follow the settings in this configuration file.
1. If the timezone is explicitly specified when establishing a database connection in Connector Drivers for various programming languages such as C/C++/Java/Python, that specified time zone setting will be used. For example, the Java Connector's JDBC URL includes a timezone parameter.
1. When writing SQL statements, you can also directly use Unix timestamps (e.g., `1554984068000`) or timestamps with time zone strings, either in RFC 3339 format (e.g., `2013-04-12T15:52:01.123+08:00`) or ISO-8601 format (e.g., `2013-04-12T15:52:01.123+0800`). In these cases, the values of these timestamps are not affected by other time zone settings.

### 4.2 Why can data from a specified time period be queried using TDengine CLI on the server, but not on the client machine?

This issue is due to the client and server having different time zone settings. Adjusting the client's time zone to match the server's will resolve the issue.

### 4.3 The table name is confirmed to exist, but returns "table name does not exist" when writing or querying, why?

In TDengine, all names, including database names and table names, are case-sensitive. If these names are not enclosed in backticks (\`) in the program or TDengine CLI, even if you input them in uppercase, the engine will convert them to lowercase for use. If the names are enclosed in backticks, the engine will not convert them to lowercase and will use them as is.

### 4.4 How can I calculate the time difference between two consecutive write records using the DIFF function?

Use the DIFF function, which allows you to view the difference between two consecutive records in a time or numeric column, very conveniently. See SQL Manual -> Functions -> DIFF for details.

### 4.5 Which is faster: querying data from a super table with TAG filter, or directly from a child table?

Directly querying from child table is fast. The query from super table with TAG filter is designed to meet the convenience of querying. It can filter data from multiple child tables at the same time. If the goal is to pursue performance and the child table has been clearly queried, directly querying from the sub table can achieve higher performance

## 5. Data Subscription (TMQ) {#data-subscription}

### 5.1 What should I do if TMQ subscription reports "Unknown error: 65534" (error code 0xfffe)?

Problem Description:
When using TMQ, you encounter `subscribe topic error, code: (0xfffe), message: Unknown error: 65534`.

Problem Cause:
The `Properties` passed when creating the consumer contains keys that TDengine does not support.

Problem Solution:
Review the `Properties` passed to `TaosConsumer` and remove any unsupported keys. Only use configuration items explicitly documented by TDengine.

### 5.2 How can I get the subtable name from each record when subscribing to a supertable via JDBC?

When subscribing to a database or supertable, set `value.deserializer` to `com.taosdata.jdbc.tmq.MapEnhanceDeserializer` when creating the consumer, and use `TaosConsumer<TMQEnhMap>` as the consumer type. Each record will then be deserialized into a `Map` that includes the subtable name alongside the field values.

## 6. Operations & Monitoring {#operations-and-monitoring}

### 6.1 What network ports are used by TDengine 3.0?

For the network ports used, please refer to the document: [Operation Guide](../operations-and-maintenance/)

Note that the listed port numbers are based on the default port 6030. If the settings in the configuration file are modified, the listed ports will change accordingly. Administrators can refer to the above information to adjust firewall settings.

### 6.2 What to do if OOM occurs?

OOM is a protection mechanism of the operating system. When the memory (including SWAP) of the operating system is insufficient, it will kill some processes to ensure the stable operation of the operating system. Usually, insufficient memory is mainly caused by two reasons: one is that the remaining memory is less than vm.min_free_kbytes; the other is that the memory requested by the program is greater than the remaining memory. Another situation is that there is sufficient memory, but the program occupies a special memory address, which can also trigger OOM.

TDengine pre-allocates memory for each VNode, the number of VNodes per Database is affected by the vgroups parameter set during database creation, and the memory size occupied by each VNode is affected by the buffer parameter. To prevent OOM, it is necessary to plan memory reasonably at the beginning of the project and set SWAP appropriately. In addition, querying excessive data can also cause a surge in memory, depending on the specific query statement. TDengine Enterprise Edition has optimized memory management with a new memory allocator, which is recommended for users with higher stability requirements.

### 6.3 What to do if encountering "Too many open files" on macOS?

The error "Too many open files" in taosd log files is due to taosd opening more files than the system's limit.
Here are the solutions:

1. Create a file /Library/LaunchDaemons/limit.maxfiles.plist, write the following content (the example changes limit and maxfiles to 100,000, modify as needed):

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

1. Modify file permissions

   ```shell
   sudo chown root:wheel /Library/LaunchDaemons/limit.maxfiles.plist
   sudo chmod 644 /Library/LaunchDaemons/limit.maxfiles.plist
   ```

1. Load the plist file (or it will take effect after restarting the system. launchd will automatically load the plist in this directory at startup)

   ```shell
   sudo launchctl load -w /Library/LaunchDaemons/limit.maxfiles.plist
   ```

1. Confirm the changed limit

   ```shell
   launchctl limit maxfiles
   ```

### 6.4 Why am I prompted with "Out of dnodes" when creating a database or "Vnodes exhausted" when creating a table?

This prompt indicates that the number of vnodes required for creating the db is not enough, exceeding the upper limit of vnodes in the dnode. By default, a dnode contains twice the number of CPU cores worth of vnodes, which can also be controlled by the supportVnodes parameter in the configuration file.
Normally, increase the supportVnodes parameter in taos.cfg.

### 6.5 How to check the storage space used by data?

By default, TDengine's data files are stored in `/var/lib/taos`, and log files are stored in `/var/log/taos`.

To view the specific size occupied by all data files, execute the Shell command: `du -sh /var/lib/taos/vnode --exclude='wal'`. This excludes the WAL directory because its size is almost constant under continuous writing, and the WAL directory is cleared each time TDengine is normally shut down to let data settle.

To view the size occupied by a single database, specify the database in the command line program `taos` and execute `show vgroups;`. Then, check the size of the folders contained in `/var/lib/taos/vnode` using the obtained VGroup id.

### 6.6 How to view data compression ratio indicators?

Currently, TDengine only provides compression ratios based on tables, not databases or the entire system. To view the compression ratios, execute the `SHOW TABLE DISTRIBUTED table_name;` command in the client TDengine CLI. The table_name can be a super table, regular table, or subtable. For details, see [SHOW TABLE DISTRIBUTED](https://docs.tdengine.com/tdengine-reference/sql-manual/show-commands/#show-table-distributed).

### 6.7 What should I do if restarting taosd via systemd fails with "start-limit-hit"?

Problem Description:
In TDengine TSDB 3.3.5.1 and later, the `StartLimitInterval` parameter in `taosd.service` was changed from 60 seconds to 900 seconds. If taosd is restarted 3 times within 900 seconds, subsequent `systemctl restart taosd` calls will fail. Running `systemctl status taosd.service` shows: `Failed with result 'start-limit-hit'`.

Problem Cause:
Before 3.3.5.1, `StartLimitInterval` was 60 seconds. If 3 restarts could not complete within 60 seconds (for example, because taosd takes a long time to recover from WAL), the counter would reset in the next 60-second window, causing taosd to restart repeatedly. The interval was increased to 900 seconds to prevent this infinite-restart loop. As a result, hitting `start-limit-hit` during frequent rapid restarts is more likely.

Problem Solution:

1. **Via systemd**: First reset the failure counter with `systemctl reset-failed taosd.service`, then restart with `systemctl restart taosd.service`. For a permanent adjustment, manually edit `/etc/systemd/system/taosd.service` to lower `StartLimitInterval` or raise `StartLimitBurst` (note: reinstalling taosd will reset this file), then run `systemctl daemon-reload` before restarting.
2. **Direct invocation**: Run taosd directly (not via systemd) to bypass `StartLimitInterval` and `StartLimitBurst` entirely.

### 6.8 I modified the configuration file, but the parameters didn't take effect. Why?

#### Problem Description

In TDengine TSDB 3.4.0.0 and later, some users may encounter this: they change a parameter in `taos.cfg`, but after restarting, the change does not take effect, and no errors appear in the logs.

#### Problem Reason

In TDengine TSDB 3.4.0.0 and later, to improve security and prevent configuration file tampering, configuration can no longer be changed by editing the configuration file. Use the `ALTER` command and change parameter values via SQL instead.

### 6.9 How to generate a core file when TDengine TSDB crashes?

Please refer to the [technical blog](https://www.taosdata.com/blog/2019/12/06/974.html) written for this issue.

### 6.10 How to temporarily adjust log levels in the command line program `taos`

For debugging convenience, the command line program `taos` has added instructions related to log recording:

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

This means that in the current command line program, you can clear all log files generated by local clients (`resetLog`), or modify the log recording level of a specific module (only effective for the current command line program; if the `taos` command line program is restarted, the settings need to be reapplied):

- The value can be: 131 (output error and warning logs), 135 (output error, warning, and debug logs), 143 (output error, warning, debug, and trace logs).

### 6.11 After changing the root password of the database, the Grafana monitoring plugin TDinsight shows no data

The data displayed in the TDinsight plugin is collected and stored in TD's log database through the taosKeeper and taosAdapter services. After changing the root password, it is necessary to update the corresponding password information in the configuration files of taosKeeper and taosAdapter, and then restart the taosKeeper and taosAdapter services (Note: if it is a cluster, restart the corresponding services on each node).

### 6.12 Why does the open-source version of TDengine's main process establish a connection with the public network?

This connection only reports the most basic information that does not involve any user data, used by the official to understand the global distribution of the product, thereby optimizing the product and enhancing user experience. The specific collection items include: cluster name, operating system version, CPU information, etc.
This feature is an optional configuration item, which is enabled by default in the open-source version. The specific parameter is telemetryReporting, as explained in the [official documentation](../tdengine-reference/components/taosd/).
You can disable this parameter at any time by modifying telemetryReporting to 0 in taos.cfg, then restarting the database service.
Code located at: [https://github.com/taosdata/TDengine/blob/62e609c558deb764a37d1a01ba84bc35115a85a4/source/dnode/mnode/impl/src/mndTelem.c](https://github.com/taosdata/TDengine/blob/62e609c558deb764a37d1a01ba84bc35115a85a4/source/dnode/mnode/impl/src/mndTelem.c).
Additionally, for the highly secure enterprise version, TDengine Enterprise, this parameter will not be operational.

### 6.13 Why is the original database lost and the cluster ID changed when the data directory dataDir of the database remains unchanged on the same server?

Background: When the TDengine server process (taosd) starts, if there are no valid data file subdirectories (such as mnode, dnode, and vnode) under the data directory (dataDir, which is specified in the configuration file taos.cfg), these directories will be created automatically. When a new mnode directory is created, a new cluster ID will be allocated to generate a new cluster.

Cause analysis: The data directory dataDir of taosd can point to multiple different mount points. If these mount points are not configured for automatic mounting in the fstab file, after the server restarts, dataDir will only exist as a normal directory of the local disk, and it will not point to the mounted disk as expected. At this point, if the taosd service is started, it will create a new directory under dataDir to generate a new cluster.

Impact of the problem: After the server is restarted, the original database is lost (note: it is not really lost, but the original data disk is not attached and cannot be seen for the time being) and the cluster ID changes, resulting in the inability to access the original database. For enterprise users, if they have been authorized for the cluster ID, they will also find that the machine code of the cluster server has not changed, but the original authorization has expired. If the problem is not monitored or found and handled in time, the user will not notice that the original database has been lost, resulting in losses and increased operation and maintenance costs.

Problem solving: You should configure the automatic mount of the dataDir directory in the fstab file to ensure that the dataDir always points to the expected mount point and directory. At this point, restarting the server will retrieve the original database and cluster. In the subsequent version, we will develop a function to enable taosd to exit in the startup phase when it detects that the dataDir changes before and after startup, and provide corresponding error prompts.

## 7. Upgrade & Migration {#upgrade-and-migration}

### 7.1 What should I pay attention to when upgrading from versions before TDengine 3.0 to version 3.0 and above?

Version 3.0 is a complete reconstruction based on previous versions, and the configuration files and data files are not compatible. Be sure to perform the following operations before upgrading:

1. Delete the configuration file, execute `sudo rm -rf /etc/taos/taos.cfg`
1. Delete the log files, execute `sudo rm -rf /var/log/taos/`
1. Under the premise that the data is no longer needed, delete the data files, execute `sudo rm -rf /var/lib/taos/`
1. Install the latest stable version of TDengine 3.0
1. If data migration is needed or data files are damaged, please contact the official technical support team of Taos Data for assistance

### 7.2 How to migrate data?

TDengine uniquely identifies a machine by its hostname. For version 3.0, when moving data files from Machine A to Machine B, it is necessary to reconfigure the hostname of Machine B to that of Machine A.

Note: The storage structures of versions 3.x and earlier versions 1.x, 2.x are not compatible. It is necessary to use migration tools or develop applications to export and import data.

## 8. Client & Tools {#client-and-tools}

### 8.1 How to display Chinese characters correctly on Windows client systems?

In Windows systems, Chinese characters are generally stored using GBK/GB18030 encoding, while the default character set for TDengine is UTF-8. When using the TDengine client on Windows, the client driver will convert characters to UTF-8 encoding before sending them to the server for storage. Therefore, during application development, it is essential to correctly configure the current Chinese character set.

When running the TDengine client command line tool taos on Windows 10, if you cannot properly input or display Chinese characters, you can configure the client taos.cfg as follows:

```text
locale C
charset UTF-8
```

### 8.2 Table name not displaying fully in the TDengine CLI

Due to the limited display width in the TDengine CLI terminal, longer table names may not be displayed fully. If operations are performed using these incomplete table names, a "Table does not exist" error may occur. This can be resolved by modifying the `maxBinaryDisplayWidth` setting in the taos.cfg file, or by directly entering the command `set max_binary_display_width 100`. Alternatively, use the `\G` parameter at the end of the command to adjust the display format of the results.

### 8.3 How to fully display field content in TDengine CLI queries?

You can use the \G parameter for vertical display, such as `show databases\G\;` (for ease of input, press TAB after "\" to automatically complete the content).

### 8.4 What should I do if Chinese or string data appears garbled in DBeaver?

Problem Description:
String data queried from TDengine via DBeaver displays as garbled text.

Problem Cause:
For historical reasons, the JDBC driver treats `varchar` columns as `binary`, causing encoding issues.

Problem Solution:
Upgrade to a recent JDBC driver version and add the appropriate parameter to the DBeaver JDBC connection URL. Refer to the [official documentation](https://docs.taosdata.com/third-party/tool/dbeaver/) for details.
