---
sidebar_label: FAQ and Feedback
title: FAQ and Feedback
description: Common issues and how to get support
---

## Issue Feedback

If the FAQ does not resolve your issue and you need help from the TDengine technical team, package the contents of the following two directories:

1. `/var/log/taos` (if the default path has not been modified)
2. `/etc/taos` (if no other configuration file path has been specified)

Attach a clear problem description, including the TDengine version, platform environment, steps to reproduce, observed symptoms, and approximate time, and submit an issue on [GitHub](https://github.com/taosdata/TDengine).

To collect enough debug information, if the problem is reproducible, add a line `debugFlag 135` at the end of `/etc/taos/taos.cfg`, restart `taosd`, reproduce the issue, and then submit it. You can also temporarily set the `taosd` log level with the following SQL:

```sql
ALTER DNODE <dnode_id> 'debugFlag' '135';
```

You can obtain `dnode_id` from the output of `SHOW DNODES`.

When the system is running normally, set `debugFlag` to `131`; otherwise a large volume of logs will be generated and system efficiency will drop.

<!-- markdownlint-disable MD051 -->
## FAQ List

- [1. Installation & Deployment](#installation-deployment)
- [2. Connection](#connection)
- [3. Data Writing](#data-writing)
- [4. Data Query](#data-query)
- [5. Data Subscription](#data-subscription)
- [6. Operations & Monitoring](#operations-monitoring)
- [7. Upgrade & Migration](#upgrade-migration)
- [8. Client & Tools](#client-tools)
<!-- markdownlint-enable MD051 -->

## 1. Installation & Deployment {#installation-deployment}

### 1.1 How do I resolve a missing `msvcp140.dll` when running TDengine on Windows?

1. Reinstall Microsoft Visual C++ Redistributable: `msvcp140.dll` is part of this runtime. Reinstalling usually resolves the issue. Download the matching version from the Microsoft website.
2. Manually download and replace `msvcp140.dll`: Download from a trusted source and copy it to the appropriate system directory. Confirm that the file matches the system architecture (32-bit or 64-bit) and verify the source is safe.

### 1.2 How do I resolve compilation failures of components written in Go?

Starting with TDengine `v3.0`, the standalone Go component `taosAdapter` must be run separately. It provides RESTful access and supports data ingestion from Prometheus, Telegraf, collectd, StatsD, and other software.
When compiling from the latest `develop` branch, first run `git submodule update --init --recursive` to fetch the `taosAdapter` repository, then compile.

Go 1.14 or later is required. If Go compilation fails, a common cause in China is restricted access to Go modules. Set the Go environment variables as follows:

```sh
go env -w GO111MODULE=on
go env -w GOPROXY=https://goproxy.cn,direct
```

### 1.3 What should I do if pulling the TDengine image from Docker Hub fails? {#docker-hub-failure}

If you cannot access the official Docker Hub registry (hub.docker.com), try the following:

- Check that your network connection is working.
- Download the image file from the [TDengine Download Center](https://www.taosdata.com/download-center), then load it with `docker load`. See the usage instructions on the download page.
- Try an alternative mirror registry, such as [CNIX Internal Container Registry Mirror](https://m.ixdev.cn/). This mirror is unaffiliated with Taos Data. If it is unavailable, try another mirror on the network and follow its usage instructions.

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

If the client encounters a connection failure, check the following:

1. Check the network environment

- Cloud server: Check whether the cloud server security group allows TCP/UDP access on ports 6030/6041
- Local virtual machine: Check whether the network can be pinged; avoid using `localhost` as the hostname when possible
- Company server: In a NAT network, ensure the server can return messages to the client

2. Ensure the client and server version numbers match exactly. TDengine TSDB-OSS and TDengine TSDB-Enterprise must not be mixed.

3. On the server, run `systemctl status taosd` to check the `taosd` status. If it is not running, start `taosd`.

4. Confirm that the client specifies the correct server FQDN (Fully Qualified Domain Name; on the server, run the Linux/macOS command `hostname -f`). For FQDN configuration, see [Understanding TDengine FQDN](https://www.taosdata.com/blog/2020/09/11/1824.html).

5. Ping the server FQDN. If there is no response, check your network, DNS settings, or the hosts file on the client machine. In a TDengine cluster, the client must be able to ping the FQDN of every cluster node.

6. Check firewall settings (Ubuntu: `ufw status`; CentOS: `firewall-cmd --list-port`) and ensure all hosts in the cluster can communicate over TCP/UDP on ports 6030/6041.

7. For JDBC connections on Linux (similar for ODBC, Python, Go, and related interfaces), ensure `libtaos.so` is in `/usr/local/taos/driver` and that directory is in the system library search path `LD_LIBRARY_PATH`.

8. For JDBC connections on macOS (similar for ODBC, Python, Go, and related interfaces), ensure `libtaos.dylib` is in `/usr/local/lib` and that directory is in the system library search path `LD_LIBRARY_PATH`.

9. For JDBC, ODBC, Python, Go, and similar connections on Windows, ensure `C:\TDengine\driver\taos.dll` is in a system library search directory (recommended: place `taos.dll` in `C:\Windows\System32`).

10. If the connection issue still cannot be resolved

- On Linux/macOS, use the `nc` command-line tool to check TCP and UDP connectivity on the specified port:
  Check UDP: `nc -vuz {hostIP} {port}`
  Check server-side TCP: `nc -l {port}`
  Check client-side TCP: `nc {hostIP} {port}`

- On Windows, use the PowerShell command `Test-NetConnection -ComputerName \{fqdn} -Port \{port}` to check whether the server port is reachable

11. You can also use the network connectivity check built into the `taos` shell to verify whether the specified port between server and client is reachable. See [Operations and Maintenance](../12-operations-and-tooling/02-operations/index.md).

### 2.2 What should I do if I encounter the error "Unable to resolve FQDN"?

This error occurs because the client or data node cannot resolve the FQDN (Fully Qualified Domain Name). For the `taos` shell or client applications, check the following:

1. Check whether the FQDN of the server you are connecting to is correct. For FQDN configuration, see [Understanding TDengine FQDN](https://www.taosdata.com/blog/2020/09/11/1824.html).
2. If the network uses a DNS server, check whether it is working properly.
3. If no DNS server is configured, check the hosts file on the client machine for the FQDN and the correct IP address.
4. If the network configuration is correct, you must be able to ping the FQDN from the client machine; otherwise the client cannot connect to the server.
5. If the server previously ran TDengine and the hostname was changed, check whether `dnode.json` under the data directory matches the currently configured EP. The default path is `/var/lib/taos/dnode`. Normally, switch to a new data directory, or back up and delete the old data directory to avoid this issue.
6. Check whether `/etc/hosts` and `/etc/hostname` use the preconfigured FQDN.

### 2.3 Why is there no response from the RESTful interface, Grafana cannot add TDengine as a data source, or TDengine GUI cannot connect even when port 6041 is selected?

This may happen because `taosAdapter` was not started correctly. Run `systemctl start taosadapter` to start the service.

Note: The `taosAdapter` log path (`path`) must be configured separately; the default is `/var/log/taos`. There are 8 `logLevel` levels; the default is `info`, and `panic` disables log output. Watch disk space on the OS root directory `/`. You can change settings via command-line parameters, environment variables, or the configuration file. The default configuration file is `/etc/taos/taosadapter.toml`.

For details on `taosAdapter`, see [taosAdapter Reference](../12-operations-and-tooling/03-components/03-taosadapter.md).

### 2.4 How can a client connection string ensure high availability?

See the [technical blog](https://www.taosdata.com/blog/2021/04/16/2287.html) written for this topic.

### 2.5 What does the error "DND ERROR Version not compatible, client: 3000700, server: 3020300" mean?

This means the client and server versions are incompatible. Here the client version is `v3.0.7.0` and the server version is `v3.2.3.0`. The current compatibility policy requires the first three version components to match.

### 2.6 After changing the database root password, starting taos fails with "failed to connect to server, reason: Authentication failure"

By default, starting the `taos` shell tries to connect to `taosd` with the default username (`root`) and password. After changing the `root` password, you must explicitly specify the username and password when connecting, for example `taos -h xxx.xxx.xxx.xxx -u root -p`, then enter the new password. After changing the password, also update the TDengine access password in the `taosKeeper` configuration file (default `/etc/taos/taoskeeper.toml`) and restart the service.

For container deployments, see [Custom Passwords, Upgrades, and Health Checks](../12-operations-and-tooling/02-operations/03-deployment/02-docker.md#custom-passwords-upgrades-and-health-checks) in the Docker deployment chapter.

`TAOS_ROOT_PASSWORD` is supported from `v3.3.6.6`; `v3.3.8.8` and later support `TAOS_ROOT_PASSWORD_FILE` and can be upgraded directly; `v3.4.1.0` and later support `taos-check startup` and `taos-check service`.

### 2.7 What should I do if I encounter "some vnode/qnode/mnode(s) out of service"?

The client has not configured FQDN resolution for all server nodes. For example, if the server has 3 nodes, the client may have configured FQDN resolution for only 1 node. For FQDN configuration, see [Understanding TDengine FQDN](https://www.taosdata.com/blog/2020/09/11/1824.html).

### 2.8 What should I do if I encounter "Sync leader is unreachable" when connecting to the cluster for the first time?

This error means the first connection to the cluster succeeded, but the first IP accessed was not the mnode leader, and an error occurred when the client tried to connect to the leader. The client finds the leader through EP (the specified FQDN and port). Two common causes are:

- Ports on other nodes in the cluster are not open
- The client `hosts` file is not configured correctly

First check the server: whether all cluster ports are open (default `6030` for native connections, `6041` for HTTP). Then check whether the client `hosts` file includes the FQDN and IP of every cluster node.
If the issue still cannot be resolved, contact Taos Data technical support.

### 2.9 What should I do if loading a dynamic library fails with "No such file or directory" or "failed to load libtaosws.so"?

Problem Description:
When using TDengine client applications (`taos` shell, taosBenchmark, taosdump, and similar tools) or client connectors (such as Java, Python, Go, and others), you may fail to load the dynamic libraries `libtaosnative.so` or `libtaosws.so`.
For example: `failed to load libtaosws.so since No such file or directory [0x80FF0002]`

Problem Cause:
The client cannot find the required dynamic library files, often because they were not installed correctly or the system library path is not configured correctly.

Problem Solution:

- **Check files**: Verify that the `libtaosnative.so` or `libtaosws.so` symlink and the corresponding real files exist and are complete under the system shared-library directory. If the symlink or real files are missing, reinstall them; they are included in both the TDengine client and server packages.
- **Check environment variables**: Ensure the shared-library search path `LD_LIBRARY_PATH` includes the directory that contains `libtaosnative.so` or `libtaosws.so`. If not, add it with `export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:<new_path>`.
- **Check permissions**: Ensure the current user has read and execute permissions on the `libtaosnative.so` or `libtaosws.so` symlink and the real files.
- **Check file corruption**: Use `readelf -h <library_file>` to verify the library file is intact.
- **Check file dependencies**: Use `ldd <library_file>` to inspect dependencies and ensure all of them are installed and accessible.

### 2.10 What should I do if JDBCDriver cannot find the dynamic-link library on Windows?

See the [technical blog](https://www.taosdata.com/blog/2019/12/03/950.html) written for this topic.

### 2.11 What should I do if JDBC native connection throws "no taos in java.library.path" or "UnsatisfiedLinkError"?

Problem Description:
When using a JDBC native connection, you encounter an error similar to `java.lang.UnsatisfiedLinkError: no taos in java.library.path`.

Problem Cause:
The client cannot find the libtaos dynamic library, typically because the taosc client is not installed, or `java.library.path` does not include the library directory.

Problem Solution:

1. Verify that the TDengine client (taosc) is installed.
2. Check that `java.library.path` includes the directory containing libtaos (typically `/usr/local/taos/driver` on Linux/macOS, or `C:\TDengine\driver` on Windows). If not, specify it with the JVM startup argument `-Djava.library.path=<path>`, or add the directory to `LD_LIBRARY_PATH` (Linux/macOS) / `PATH` (Windows).
3. macOS users on older versions should upgrade to a newer TDengine client.

### 2.12 What should I do if JDBC native connection reports "Operation not permitted"?

Problem Description:
When using a JDBC native connection, you encounter an `Operation not permitted` error.

Problem Cause:
The current user does not have permission to write log files, so client initialization fails.

Problem Solution:
Check the permissions of the TDengine log directory (default `/var/log/taos`). Ensure the user running the Java application has write access, or configure a writable directory with the `logDir` setting.

### 2.13 What should I do if JDBC WebSocket connection times out with "can't create connection with server within: 60000 milliseconds" (error code 0x231d)?

Problem Description:
The call stack looks like:

```plaintext
java.sql.SQLException: ERROR (0x231d): can't create connection with server within: 60000 milliseconds
        at com.taosdata.jdbc.ws.Transport.checkConnection(Transport.java:393)
```

Problem Cause:
Network unreachable, port not open, incorrect Nginx proxy configuration, or JAR dependency conflict can all cause this.

Problem Solution:

1. Verify network connectivity and that the port on the host running taosAdapter (default 6041) is accessible:

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
5. If all of the above are normal, run `mvn dependency:tree` to check for conflicting JSON libraries (for example jackson or fastjson version conflicts).

### 2.14 What should I do if JDBC through Nginx reports "WebSocket handshake error, code: 400 Bad Request"?

Problem Description:
When connecting to taosAdapter through an Nginx reverse proxy, JDBC reports `WebSocket handshake error, code: 400 Bad Request`.

Problem Cause:
Nginx is not configured with the HTTP headers required for WebSocket upgrade.

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
Nginx `proxy_send_timeout` and `proxy_read_timeout` are too low, so long-lived connections are closed early.

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

### 3.1 What is the most effective method for writing data?

Batch insertion. Each write statement can insert multiple records into one table at the same time, or multiple records into multiple tables simultaneously.

### 3.2 How do I fix garbled Chinese characters in NCHAR data inserted on Windows?

When writing Chinese text into the `NCHAR` type on Windows, first confirm that the system region is set to China (in Control Panel). The `taos` shell in Command Prompt usually works in that case. If you develop Java applications in an IDE (such as Eclipse or IntelliJ), confirm that the IDE file encoding is GBK (the Java default encoding) and initialize the client configuration when creating the Connection, for example:

```java
Class.forName("com.taosdata.jdbc.TSDBDriver");
Properties properties = new Properties();
properties.setProperty(TSDBDriver.LOCALE_KEY, "UTF-8");
Connection = DriverManager.getConnection(url, properties);
```

### 3.3 Why is querying very fast when using the taosBenchmark testing tool to write data, but very slow when I write data?

Severe out-of-order writes in TDengine can significantly hurt query performance, so eliminate out-of-order data before writing. If the business consumes from Kafka and writes, design consumers carefully so that data for one subtable is written by the same consumer whenever possible, and avoid designs that introduce disorder.

### 3.4 When upgrading a database from `v2.6` to `v3.3`, if migration runs while the business continues writing, will severe out-of-order issues occur?

Usually not. In TDengine, out-of-order means: starting from timestamp 0, time windows are divided by the database `DURATION` parameter (default 10 days), and timestamps within the same window are not written in order. As long as writes within the same window are ordered, interleaved writes across windows are not considered out of order.

In the scenario above, backfilled old data and new data are usually far apart in time and do not fall in the same window. As long as old and new data are each written in order, out-of-order writes will not occur.

### 3.5 What should I do if JDBC write reports "Invalid message" (error code 0x115)?

Problem Description:
When writing with auto-create table using JDBC, you encounter `(0x115): Invalid message`.

Problem Cause:
The subtable being written already exists under a different supertable, causing a conflict.

Problem Solution:
Verify that the supertable specified in the write statement matches the supertable the subtable actually belongs to, and do not write to the wrong supertable.

### 3.6 What should I do if a Java write application becomes unresponsive with near-zero QPS?

Problem Description:
A Java application writing to TDengine freezes after running for some time, with almost no write QPS.

Problem Cause:
This is typically caused by JVM GC issues, not a memory leak in the JDBC driver.

Problem Solution:

1. Run `top -Hp <pid>` to check whether many GC threads are consuming CPU.
2. Run `jstat -gcutil <pid> 3000 100` to observe whether Young GC has stopped and Full GC is consuming all time.
3. If GC is confirmed as the cause, investigate memory allocation in the application code and reduce object creation frequency.

### 3.7 JDBC write performance is low — how do I diagnose and improve it?

Common checkpoints:

1. **Physical resources**: Ensure the server uses SSD storage and a 10 Gbps network. When benchmarking locally, exclude interference from low-speed networks such as Wi-Fi.
2. **Server-side pressure**: Monitor taosd CPU, memory, network, and disk usage. Very low CPU usually means requests are not reaching the server effectively.
3. **VGROUP count**: The default VGROUP count when creating a database is 2. Increase it if write concurrency is high.
4. **Stmt object reuse**: Frequently creating new `PreparedStatement` (Stmt) objects significantly hurts performance. Create Stmt objects at application startup and reuse them. See [Ingesting Data Efficiently](../10-developer-guide/05-high-throughput.md). Use `SHOW QUERIES;` to verify long-running parameter-binding statements are present, confirming Stmt reuse.
5. **ORM frameworks**: When using MyBatis or similar frameworks, check whether writes have degraded to one row per commit. Refer to the [MyBatis write example](https://github.com/taosdata/TDengine/blob/main/docs/examples/JDBC/mybatisplus-demo/src/test/java/com/taosdata/example/mybatisplusdemo/mapper/MetersMapperTest.java).
6. **Skip TAG when subtable exists**: If the subtable already exists, omit TAG columns with SQL in the following form to improve performance:

   ```sql
   INSERT INTO meters (tbname, ts, current, voltage, phase) VALUES(?, ?, ?, ?, ?)
   ```

## 4. Data Query {#data-query}

### 4.1 How is time zone information handled for timestamps?

In TDengine, timestamp time zones are always handled by the client and are independent of the server. Specifically, the client converts timestamps in SQL statements to the UTC time zone (Unix timestamps) before sending them to the server for writing and querying. When reading data, the server also provides raw data in UTC, and the client then converts timestamps to the local system time zone for display.

The client handles timestamp strings with the following logic:

1. By default, without special settings, the client uses the time zone of the operating system it is running on.
2. If the `timezone` parameter is set in `taos.cfg`, the client follows that configuration.
3. If `timezone` is explicitly specified when establishing a connection in connectors such as C/C++/Java/Python, that time zone is used. For example, the Java connector JDBC URL has a `timezone` parameter.
4. When writing SQL statements, you can also use Unix timestamps directly (for example `1554984068000`) or timestamp strings with a time zone, in RFC 3339 format (for example `2013-04-12T15:52:01.123+08:00`) or ISO-8601 format (for example `2013-04-12T15:52:01.123+0800`). In these cases, the timestamp values are not affected by other time zone settings.

### 4.2 Why can data from a specified time period be queried using the `taos` shell on the server, but not on the client machine?

This happens because the client and server time zones differ. Align the client time zone with the server to resolve it.

### 4.3 The table name is confirmed to exist, but writing or querying returns that the table does not exist. Why?

All names in TDengine (including database names and table names) are case-sensitive. If names are not enclosed in backticks in a program or the `taos` shell, even uppercase input is converted to lowercase by the engine. If backticks are used, the names are kept as entered.

### 4.4 How can I calculate the time difference between two consecutive write records?

Use the `DIFF` function to view the difference between two consecutive records in a time or numeric column. See [DIFF](../05-tdengine-sql/04-data-query/03-function.md#diff).

### 4.5 Which is faster: querying child-table data from a supertable with TAG filters, or querying the child table directly?

Querying the child table directly is faster. TAG-filtered queries on a supertable are convenient for filtering data from multiple child tables at once. If the target child table is known and performance matters more, querying the child table directly is usually faster.

## 5. Data Subscription (TMQ) {#data-subscription}

### 5.1 What should I do if TMQ subscription reports "Unknown error: 65534" (error code 0xfffe)?

Problem Description:
When using TMQ, you encounter `subscribe topic error, code: (0xfffe), message: Unknown error: 65534`.

Problem Cause:
The `Properties` passed when creating the consumer contains custom properties that TDengine does not support.

Problem Solution:
Review the `Properties` passed to `TaosConsumer` and remove unsupported keys. Keep only configuration items explicitly documented by TDengine.

### 5.2 How can I get the subtable name from each record when subscribing to a supertable via JDBC?

When subscribing to a database or supertable, set `value.deserializer` to `com.taosdata.jdbc.tmq.MapEnhanceDeserializer` when creating the consumer, and use `TaosConsumer<TMQEnhMap>` as the consumer type. Each record will then be deserialized into a `Map` that includes the subtable name and field values.

## 6. Operations & Monitoring {#operations-monitoring}

### 6.1 What network ports are used by TDengine 3.0?

For the ports used, see [System Requirements · Network Port Requirements](../12-operations-and-tooling/02-operations/01-planning.md#network-port-requirements).

Note: The listed ports assume the default port `6030`. If port-related settings in the configuration file are changed, the actual ports change accordingly. Administrators can adjust firewall rules based on this.

### 6.2 What should I do if OOM occurs?

OOM is an operating-system protection mechanism. When OS memory (including SWAP) is insufficient, it kills some processes to keep the OS stable. Insufficient memory usually has two main causes: remaining memory is less than `vm.min_free_kbytes`, or the program requests more memory than remains. Another case is that memory is sufficient but the program occupies a special memory address, which can also trigger OOM.

TDengine pre-allocates memory for each vnode. The number of vnodes per database is affected by the `vgroups` parameter set at database creation, and the memory size of each vnode is affected by parameters such as `buffer`. To prevent OOM, plan memory reasonably at the start of a project and configure SWAP. Querying too much data can also cause memory spikes, depending on the query statement. TDengine TSDB-Enterprise improves memory management and uses a new memory allocator; users with higher stability requirements may consider the enterprise edition.

### 6.3 What should I do if I encounter "Too many open files" on macOS?

The "Too many open files" error in taosd logs occurs because taosd opened more files than the system limit.
Solutions:

1. Create `/Library/LaunchDaemons/limit.maxfiles.plist` and write the following content (this example sets limit and maxfiles to 100,000; adjust as needed):

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

2. Modify file permissions

```bash
sudo chown root:wheel /Library/LaunchDaemons/limit.maxfiles.plist
sudo chmod 644 /Library/LaunchDaemons/limit.maxfiles.plist
```

3. Load the plist file (or reboot for it to take effect; launchd loads plists in this directory at startup)

```bash
sudo launchctl load -w /Library/LaunchDaemons/limit.maxfiles.plist
```

4. Confirm the changed limit

```bash
launchctl limit maxfiles
```

### 6.4 Why am I prompted with "Out of dnodes" when creating a database or "Vnodes exhausted" when creating a table?

This prompt means there are not enough vnodes to create the database; the required vnode count cannot exceed the dnode vnode limit. By default, each dnode supports twice as many vnodes as CPU cores. You can also control this with the `supportVnodes` configuration parameter. Usually, increasing `supportVnodes` in `taos.cfg` is enough.

### 6.5 How do I check the storage space used by data?

By default, TDengine data files are under `/var/lib/taos` and log files under `/var/log/taos`.

To view space used by all data files, run: `du -sh /var/lib/taos/vnode --exclude='wal'`. The WAL directory is excluded because its size is usually stable under continuous writing, and it is cleared after a normal TDengine shutdown once data is flushed to disk.

To view space used by a single database, select the database in the `taos` shell and run `SHOW VGROUPS;`, then check the corresponding directory size under `/var/lib/taos/vnode` using the returned vgroup id.

### 6.6 How do I view database compression ratio and disk usage metrics?

Versions before `v3.3.5.0` provide compression ratio only at the table level, not database-wide totals. In the client `taos` shell, run `SHOW TABLE DISTRIBUTED table_name;`, where `table_name` can be a supertable, regular table, or subtable. See [`SHOW TABLE DISTRIBUTED`](../05-tdengine-sql/09-system-info/03-show.md#show-table-distributed).

`v3.3.5.0` and later also provide database-wide compression ratio and disk usage statistics. Run `SHOW db_name.DISK_INFO;` for overall compression ratio and disk usage, or `SELECT * FROM INFORMATION_SCHEMA.INS_DISK_USAGE WHERE db_name='db_name';` for per-module disk usage, where `db_name` is the database name. See [View DB Disk Usage](../05-tdengine-sql/02-ddl/01-database.md#view-db-disk-usage).

### 6.7 How does WAL affect storage space and the observed compression ratio?

The Write-Ahead Log (WAL) is TDengine's core mechanism for ensuring data durability. Every write request is recorded in the WAL in **raw, uncompressed format** before being flushed to data files. Therefore, early in writing—before WAL files are cleaned up by policy—the WAL may be larger than the compressed data files, so observed total storage may **temporarily exceed the compressed data size**. This is expected. As writing continues, historical WAL files are cleaned up automatically, and total storage stabilizes near the actual compressed data size.

### 6.8 What should I do if restarting taosd via systemd fails with "start-limit-hit" after too many restarts in a short time?

Problem Description:
In `v3.3.5.1` and later, the systemd configuration for `taosd.service` changed `StartLimitInterval` from 60 seconds to 900 seconds. If `taosd` is restarted 3 times within 900 seconds, later starts via systemd fail, and `systemctl status taosd.service` may show: `Failed with result 'start-limit-hit'`.

Problem Cause:
Before `v3.3.5.1`, `StartLimitInterval` was 60 seconds. If 3 restarts could not complete within 60 seconds (for example, slow startup while recovering a large amount of data from WAL), the next window would reset the counter and could cause continuous restarts. To avoid infinite restarts, the parameter was changed to 900 seconds, so frequent systemd starts in a short period are more likely to hit `start-limit-hit`.

Problem Solution:

1. Restart via systemd: First run `systemctl reset-failed taosd.service` to reset the failure counter, then run `systemctl restart taosd.service`. For a lasting change, edit `/etc/systemd/system/taosd.service` to lower `StartLimitInterval` or raise `StartLimitBurst` (reinstalling `taosd` resets this file, so you must edit it again), then run `systemctl daemon-reload` and restart.
2. You can also start the service by running `taosd` directly, without systemd; in that case `StartLimitInterval` / `StartLimitBurst` do not apply.

### 6.9 I confirmed that I changed parameters in the configuration file, but they did not take effect. Why?

Problem Description:
In `v3.4.0.0` and later, some users may find that after changing parameters in `taos.cfg`, the changes do not take effect after restart, and logs show no obvious errors.

Problem Cause:
Starting with `v3.4.0.0`, to improve security and prevent malicious tampering of the configuration file, TDengine no longer allows runtime configuration parameters to be changed by editing the configuration file. Use `ALTER` statements to change configuration parameters via SQL.

### 6.10 How do I make TDengine generate a core file after a crash?

See the [technical blog](https://www.taosdata.com/blog/2019/12/06/974.html) written for this topic.

### 6.11 How do I temporarily adjust log levels in the `taos` shell?

For debugging, the `taos` shell provides statements related to logging:

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

This means: in the current `taos` shell session, clear the local client log file (`resetLog`), or change the log level of a specific module (effective only for the current session; after restarting the `taos` shell you must set it again):

- The value can be: 131 (error and warning logs), 135 (error, warning, and debug logs), 143 (error, warning, debug, and trace logs).

### 6.12 After changing the database root password, the Grafana monitoring plugin TDinsight shows no data

Data shown by the TDinsight plugin is collected by `taosKeeper` and `taosAdapter` and written to the TDengine `log` database. After changing the `root` password, update the password in both configuration files and restart `taosKeeper` and `taosAdapter` (in a cluster, restart the corresponding services on each node).

### 6.13 Why does the open-source TDengine main process establish a connection to the public network?

This connection only reports basic information that does not involve user data (such as cluster name, OS version, and CPU information) so the official team can understand global product distribution and improve the product and experience.

This feature is optional. It is enabled by default in the open-source edition. The parameter is `telemetryReporting`; see [taosd · Monitoring Related](../12-operations-and-tooling/03-components/01-taosd.md#monitoring-related).

You can disable it at any time by setting `telemetryReporting` to `0` in `taos.cfg`, then restarting the database service.

Related code: [mndTelem.c](https://github.com/taosdata/TDengine/blob/62e609c558deb764a37d1a01ba84bc35115a85a4/source/dnode/mnode/impl/src/mndTelem.c).

In addition, TDengine TSDB-Enterprise, which has stricter security requirements, does not enable this parameter.

### 6.14 On the same server, with the database dataDir unchanged, why is the original database lost and the cluster ID changed?

Background: When the TDengine server process (`taosd`) starts, if the data directory (`dataDir`, specified in `taos.cfg`) has no valid data-file subdirectories (such as `mnode`, `dnode`, and `vnode`), it creates them automatically. Creating a new `mnode` directory allocates a new cluster ID and forms a new cluster.

Cause analysis: `dataDir` can point to multiple mount points. If those mount points are not configured for automatic mounting in `fstab`, after a server reboot `dataDir` may be only a local ordinary directory and not point to the expected disk. Starting `taosd` then creates new directories under that path and forms a new cluster.

Impact: After reboot, the original database appears lost (often because the data disk is not mounted and is temporarily invisible), and the cluster ID changes, so the original database cannot be accessed. For enterprise users authorized by cluster ID, the machine code may be unchanged while authorization becomes invalid. Without monitoring or timely handling, data can appear missing and operations cost can rise.

Solution: Configure automatic mounting of `dataDir` in `fstab` so it always points to the expected mount point and directory, then reboot the server to recover the original database and cluster. Later versions plan to exit during startup with a clear error if `dataDir` changes before and after startup.

## 7. Upgrade & Migration {#upgrade-migration}

### 7.1 What should I pay attention to when upgrading from versions before TDengine `v3.0` to `v3.0` and later?

`v3.0` is a full refactor relative to earlier versions. Configuration files and data files are incompatible. Before upgrading, be sure to:

1. Delete the configuration file: `sudo rm -rf /etc/taos/taos.cfg`
2. Delete log files: `sudo rm -rf /var/log/taos/`
3. If the data is no longer needed, delete data files: `sudo rm -rf /var/lib/taos/`
4. Install the latest stable `v3.0` TDengine
5. If you need to migrate data or data files are damaged, contact Taos Data official technical support

### 7.2 How do I migrate data?

TDengine uniquely identifies a machine by hostname. For `v3.0`, when moving data files from machine A to machine B, configure machine B's hostname to match machine A.

Note: The storage structures of `v3.x` and earlier `v1.x` / `v2.x` are incompatible. Use migration tools or develop an application to export and import data.

## 8. Client & Tools {#client-tools}

### 8.1 Why can't the Windows client display Chinese characters correctly?

Windows systems commonly store Chinese in GBK/GB18030, while TDengine's default character set is UTF-8. When using the TDengine client on Windows, the driver converts characters to UTF-8 before sending them to the server. When developing applications, configure the current Chinese character set correctly at the API call site.

If the `taos` shell on Windows 10 cannot enter or display Chinese correctly, configure the client `taos.cfg` as follows:

```bash
locale C
charset UTF-8
```

### 8.2 Table name not displaying fully

Because the `taos` shell display width in the terminal is limited, long table names may not display fully. Operating on a truncated table name may return `Table does not exist`. Fix this by changing `maxBinaryDisplayWidth` in `taos.cfg`, running `set max_binary_display_width 100`, or appending `\G` to adjust the display format.

### 8.3 How do I fully display field content when querying in the `taos` shell?

Use `\G` for vertical display, for example `SHOW DATABASES\G;` (press Tab after `\` for autocomplete).

### 8.4 What should I do if Chinese or string data appears garbled in DBeaver when connecting to TDengine?

Problem Description:
String data queried from TDengine via DBeaver displays as garbled text.

Problem Cause:
For historical reasons, the JDBC driver treats the `varchar` type as `binary`, causing encoding recognition issues.

Problem Solution:
Upgrade to a newer JDBC driver and add the appropriate parameter to the DBeaver JDBC connection URL. For detailed configuration, see [DBeaver](../13-ecosystem-integrations/04-tool/01-dbeaver.md).
