---
title: taosd Reference
sidebar_label: taosd
slug: /tdengine-reference/components/taosd
---

taosd is the core service of the TDengine database engine, with its configuration file defaulting to `/etc/taos/taos.cfg`, though it can also be specified at a different path. This section provides a detailed introduction to the command line parameters and configuration parameters within the taosd configuration file.

## Command Line Parameters

The command line parameters for taosd are as follows:

- -a `<json file>`: Specifies a JSON file that contains various configuration parameters for the service at startup, formatted like `{"fqdn":"td1"}`. For details on configuration parameters, please refer to the next section.
- -c `<directory>`: Specifies the directory where the configuration file is located.
- -s: Prints SDB information.
- -C: Prints configuration information.
- -e: Specifies an environment variable, formatted like `-e 'TAOS_FQDN=td1'`.
- -k: Gets the machine code.
- -dm: Enables memory scheduling.
- -V: Prints version information.

## Configuration Parameters

:::note

After modifying configuration file parameters, it is necessary to restart the *taosd* service or the client application for changes to take effect.

:::

### Connection Related

| Parameter Name         | support version | Parameter Description                                        |
| :--------------------- |:---------------| :----------------------------------------------------------- |
| firstEp                |                 | The endpoint of the first dnode in the cluster to connect to when taosd starts; default value: localhost:6030 |
| secondEp               |                 | If firstEp cannot connect, attempt to connect to the second dnode's endpoint in the cluster; default value: none |
| fqdn                   |                 | The service address that taosd listens on after startup; default value: the first hostname configured on the server |
| compressMsgSize        |                 | Whether to compress RPC messages; -1: no messages are compressed; 0: all messages are compressed; N (N>0): only messages larger than N bytes are compressed; default value: -1 |
| shellActivityTimer     |                 | The duration in seconds for the client to send heartbeats to the mnode; range: 1-120; default value: 3 |
| numOfRpcSessions       |                 | The maximum number of RPC connections supported; range: 100-100000; default value: 30000 |
| numOfRpcThreads        |                 | The number of threads for RPC data transmission; range: 1-50, default value: half of the CPU cores |
| numOfTaskQueueThreads  |                 | The number of threads for the client to process RPC messages, range: 4-16, default value: half of the CPU cores  |
| rpcQueueMemoryAllowed  |                 | The maximum amount of memory allowed for RPC messages received on a dnode; unit: bytes; range: 104857600-INT64_MAX; default value: 1/10 of server memory |
| resolveFQDNRetryTime   | Removed in 3.x   | The number of retries when FQDN resolution fails |
| timeToGetAvailableConn | Removed in 3.3.4.x | The maximum waiting time to obtain an available connection; range: 10-50000000; unit: milliseconds; default value: 500000 |
| maxShellConns          | Removed in 3.x   | The maximum number of connections allowed to be created |
| maxRetryWaitTime       |                 | The maximum timeout for reconnection; default value: 10s |
| shareConnLimit         | Added in 3.3.4.0 | The number of requests that a connection can share; range: 1-512; default value: 10 |
| readTimeout            | Added in 3.3.4.0 | The minimum timeout for a single request; range: 64-604800; unit: seconds; default value: 900 |

### Monitoring Related

| Parameter Name     | Parameter Description                                        |
| :----------------- | :----------------------------------------------------------- |
| monitor            | Whether to collect monitoring data and report it; 0: off; 1: on; default value: 0 |
| monitorFqdn        | The FQDN of the server where the taosKeeper service is located; default value: none |
| monitorPort        | The port number the taosKeeper service listens on; default value: 6043 |
| monitorInternal    | The time interval for recording system parameters (CPU/memory) in the monitoring database; unit: seconds; range: 1-200000; default value: 30 |
| telemetryReporting | Whether to upload telemetry; 0: do not upload; 1: upload; default value: 1 |
| crashReporting     | Whether to upload crash information; 0: do not upload; 1: upload; default value: 1 |

### Query Related

| Parameter Name         | Parameter Description                                        |
| :--------------------- | :----------------------------------------------------------- |
| queryPolicy            | Query policy; 1: only use vnode, do not use qnode; 2: sub-tasks without scanning operators execute on qnode, those with scanning operators execute on vnode; 3: vnode only runs scanning operators, other operators run on qnode; 4: use client aggregation mode; default value: 1 |
| maxNumOfDistinctRes    | The maximum number of distinct results allowed to be returned; default value: 100,000; maximum allowed value: 100 million |
| countAlwaysReturnValue | Whether the count/hyperloglog function returns a value when the input data is empty or NULL; 0: returns empty row; 1: returns; when set to 1, if the query contains an INTERVAL clause or uses TSMA, and the corresponding group or window has no data or is NULL, that group or window will not return query results. Note that this parameter should have consistent values on both client and server. |

### Regional Related

| Parameter Name |                    Parameter Description                     |
| :------------- | :----------------------------------------------------------: |
| timezone       | The timezone; default value: the timezone configured on the current server |
| locale         | System locale information and encoding format; default value: dynamically retrieved from the system; if auto-retrieval fails, the user needs to set it in the configuration file or through the API |
| charset        | Character set encoding; default value: automatically retrieved from the system |

:::info

1. To address the issue of writing and querying data across multiple time zones, TDengine uses Unix timestamps (Unix Timestamp) to record and store timestamps. The characteristic of Unix timestamps ensures that at any given moment, regardless of time zone, the generated timestamps are consistent. Note that Unix timestamps are converted and recorded on the client side. To ensure other forms of time conversions on the client are correctly converted to Unix timestamps, the correct timezone needs to be set.

   In Linux/macOS, the client will automatically read the timezone information set by the system. Users can also set the timezone in the configuration file in various ways. For example:

   ```text
   timezone UTC-8
   timezone GMT-8
   timezone Asia/Shanghai
   ```

   All of these are valid formats for setting the timezone to UTC+8. However, note that this format (timezone Asia/Shanghai) is not supported under Windows; it must be written as timezone UTC-8.

   The timezone setting affects the contents of SQL statements for queries and writes that are not in Unix timestamp format (e.g., timestamp strings, the interpretation of the keyword now). For example:

   ```sql
   SELECT count(*) FROM table_name WHERE TS<'2019-04-11 12:01:08';
   ```

   In UTC+8, the SQL statement is equivalent to:

   ```sql
   SELECT count(*) FROM table_name WHERE TS<1554955268000;
   ```

   In UTC, the SQL statement is equivalent to:

   ```sql
   SELECT count(*) FROM table_name WHERE TS<1554984068000;
   ```

   To avoid uncertainty when using string time formats, Unix timestamps can be used directly. Additionally, timestamp strings with time zones can also be used in SQL statements, such as RFC3339 format timestamp strings (e.g., 2013-04-12T15:52:01.123+08:00) or ISO-8601 format timestamp strings (e.g., 2013-04-12T15:52:01.123+0800). The conversion of these two strings to Unix timestamps is not affected by the system's time zone.

2. TDengine provides a special field type `nchar` for storing wide characters such as Chinese, Japanese, and Korean that are not ASCII encoded. Data written to `nchar` fields will be encoded in UCS4-LE format and sent to the server. It is important to note that the correctness of the encoding is guaranteed by the client. Therefore, if users want to use `nchar` fields to store non-ASCII characters like Chinese, Japanese, or Korean, the encoding format of the client must be correctly set.

   Characters input by the client use the current default encoding format of the operating system, which is usually UTF-8 on Linux/macOS systems. Some Chinese systems may use GB18030 or GBK, etc. In Docker environments, the default encoding is POSIX. In Chinese versions of Windows, the encoding is CP936. Clients must ensure that the character set they are using is correctly set to guarantee that data in `nchar` is accurately converted to UCS4-LE format.

   In Linux/macOS, the naming convention for locales is: `<language>_<region>.<charset encoding>`, for example: zh_CN.UTF-8, where "zh" stands for Chinese, "CN" represents mainland China, and "UTF-8" denotes the character set. The character set encoding provides guidance for the client to correctly parse local strings. In Linux/macOS, the locale can be set to determine the system's character encoding; because Windows uses a locale format that is not POSIX compliant, it requires another configuration parameter `charset` to specify the character encoding. The `charset` parameter can also be used in Linux/macOS to specify the character encoding.

3. If the configuration file does not specify `charset`, in Linux/macOS, taos will automatically read the system's current locale information at startup and extract the charset encoding format from it. If the automatic reading of locale information fails, it will attempt to read the charset configuration. If reading the charset configuration also fails, the startup process will be interrupted.

   In Linux/macOS, the locale information includes character encoding information, so if the locale of Linux/macOS is set correctly, there is no need to set `charset` separately. For example:

   ```text
   locale zh_CN.UTF-8
   ```

   In Windows, the system cannot retrieve the current encoding from the locale. If the encoding information cannot be retrieved from the configuration file, taos defaults to CP936 as the character encoding. This is equivalent to adding the following configuration to the configuration file:

   ```text
   charset CP936
   ```

   If you need to adjust the character encoding, please refer to the current operating system's encoding and set it correctly in the configuration file.

   In Linux/macOS, if the user sets both `locale` and `charset` and they are inconsistent, the latter will override the former.

   ```text
   locale zh_CN.UTF-8
   charset GBK
   ```

   The effective value of `charset` is GBK.

   ```text
   charset GBK
   locale zh_CN.UTF-8
   ```

   The effective value of `charset` is UTF-8.

:::

### Storage Related

| Parameter Name   |                    Parameter Description                     |
| :--------------- | :----------------------------------------------------------: |
| dataDir          | Directory for data files; all data files will be written to this directory; default value: /var/lib/taos |
| tempDir          | Specifies the directory for temporary files generated during system operation; default value: /tmp |
| minimalTmpDirGB  | Minimum space required to be preserved in the tempDir specified; unit: GB; default value: 1 |
| minimalDataDirGB | Minimum space required to be preserved in the dataDir specified; unit: GB; default value: 2 |

### Cluster Related

| Parameter Name | Parameter Description                                        |
| :------------- | :----------------------------------------------------------- |
| supportVnodes  | Maximum number of vnodes supported by dnode; range: 0-4096; default value: 2 times the number of CPU cores + 5 |

### Memory Related

| Parameter Name             | Parameter Description                                        |
| :------------------------- | :----------------------------------------------------------- |
| rpcQueueMemoryAllowed      | Maximum amount of memory allowed for rpc messages on a dnode; unit: bytes; range: 10485760-INT64_MAX; default value: 1/10 of server memory |
| syncLogBufferMemoryAllowed | Maximum amount of memory allowed for sync log buffer messages on a dnode; unit: bytes; range: 10485760-INT64_MAX; default value: 1/10 of server memory; effective from versions 3.1.3.2/3.3.2.13 |

### Performance Tuning

| Parameter Name     | Parameter Description                                        |
| :----------------- | :----------------------------------------------------------- |
| numOfCommitThreads | Maximum number of write threads; range: 0-1024; default value: 4 |

### Log Related

| Parameter Name   | Parameter Description                                        |
| :--------------- | :----------------------------------------------------------- |
| logDir           | Directory for log files; runtime logs will be written to this directory; default value: /var/log/taos |
| minimalLogDirGB  | When the available disk space of the log folder is less than this value, logging will stop; unit: GB; default value: 1 |
| numOfLogLines    | Maximum number of lines allowed in a single log file; default value: 10,000,000 |
| asyncLog         | Log writing mode; 0: synchronous, 1: asynchronous; default value: 1 |
| logKeepDays      | Maximum retention time for log files; unit: days; default value: 0, meaning unlimited retention; log files will not be renamed, and no new log file rolling will occur, but the contents of the log file may continue to roll depending on the size settings; when set to a value greater than 0, if the log file size reaches the set limit, it will be renamed to taosdlog.xxx, where xxx is the timestamp of the last modification of the log file, and a new log file will be rolled |
| slowLogThreshold | Slow query threshold; if it exceeds or equals the threshold, it is considered a slow query; unit: seconds; default value: 3 |
| slowLogScope     | Determines which types of slow queries to log; optional values: ALL, QUERY, INSERT, OTHERS, NONE; default value: ALL |
| debugFlag        | Run log switch; 131 (outputs error and warning logs), 135 (outputs error, warning, and debug logs), 143 (outputs error, warning, debug, and trace logs); default value: 131 or 135 (depending on different modules) |
| tmrDebugFlag     | Timer module log switch; same value range as above           |
| uDebugFlag       | Shared function module log switch; same value range as above |
| rpcDebugFlag     | RPC module log switch; same value range as above             |
| jniDebugFlag     | JNI module log switch; same value range as above             |
| qDebugFlag       | Query module log switch; same value range as above           |
| dDebugFlag       | Dnode module log switch; same value range as above; default value 135 |
| vDebugFlag       | Vnode module log switch; same value range as above           |
| mDebugFlag       | Mnode module log switch; same value range as above           |
| wDebugFlag       | WAL module log switch; same value range as above             |
| sDebugFlag       | Sync module log switch; same value range as above            |
| tsdbDebugFlag    | TSDB module log switch; same value range as above            |
| tqDebugFlag      | TQ module log switch; same value range as above              |
| fsDebugFlag      | FS module log switch; same value range as above              |
| udfDebugFlag     | UDF module log switch; same value range as above             |
| smaDebugFlag     | SMA module log switch; same value range as above             |
| idxDebugFlag     | Index module log switch; same value range as above           |
| tdbDebugFlag     | TDB module log switch; same value range as above             |

### Compression Parameters

| Parameter Name  | Parameter Description                                        |
| :-------------: | :----------------------------------------------------------- |
| compressMsgSize | Whether to compress RPC messages; -1: no messages are compressed; 0: all messages are compressed; N (N>0): only messages larger than N bytes are compressed; default value: -1 |
|   fPrecision    | Sets the compression precision for float type floating-point numbers; range: 0.1 ~ 0.00000001; default value: 0.00000001; float numbers smaller than this value will have their tail truncated |
|   dPrecision    | Sets the compression precision for double type floating-point numbers; range: 0.1 ~ 0.0000000000000001; default value: 0.0000000000000001; double numbers smaller than this value will have their tail truncated |
|   lossyColumn   | Enables TSZ lossy compression for float and/or double types; range: float, double, none; default value: none; indicates that lossy compression is turned off. **Note: This parameter is no longer used in versions 3.3.0.0 and higher.** |
|    ifAdtFse     | When TSZ lossy compression is enabled, use the FSE algorithm instead of the HUFFMAN algorithm. The FSE algorithm compresses faster but decompresses slightly slower. It can be selected if compression speed is a priority; 0: off, 1: on; default value: 0 |

**Supplementary Notes**

1. Effective from versions 3.2.0.0 to 3.3.0.0 (exclusive), enabling this parameter cannot revert back to the version prior to the upgrade.
2. The TSZ compression algorithm uses data prediction techniques for compression, making it more suitable for data that varies in a regular pattern.
3. TSZ compression may take longer; if your server's CPU is largely idle and storage space is limited, it is suitable to opt for this.
4. Example: Enable lossy compression for both float and double types.

   ```shell
   lossyColumns     float|double
   ```

5. Configuration changes require a service restart to take effect. If you see the following content in the taosd log upon restarting, it indicates that the configuration has taken effect:

   ```sql
   02/22 10:49:27.607990 00002933 UTL  lossyColumns     float|double
   ```

### Other Parameters

| Parameter Name   | Parameter Description                                        |
| :--------------- | :----------------------------------------------------------- |
| enableCoreFile   | Whether to generate a core file upon crash; 0: do not generate; 1: generate; default value: 1; Depending on the startup method, the directory for generated core files is as follows: 1. When started with systemctl start taosd: the core will be generated in the root directory; 2. When started manually, it will be in the directory where taosd is executed. |
| udf              | Whether to start the UDF service; 0: do not start; 1: start; default value: 0 |
| ttlChangeOnWrite | Whether the ttl expiration time changes along with table modifications; 0: do not change; 1: change; default value: 0 |
| tmqMaxTopicNum   | Maximum number of topics that can be established for subscription; range: 1-10000; default value: 20 |
| maxTsmaNum       | Number of TSMA that can be created in the cluster; range: 0-3; default value: 3 |

## taosd Monitoring Metrics

taosd reports monitoring metrics to taosKeeper, which writes these metrics to the monitoring database, defaulting to `log`. This section provides a detailed introduction to these monitoring metrics.

### taosd_cluster_basic Table

The `taosd_cluster_basic` table records basic cluster information.

| field             | type      | is_tag | comment                            |
| :---------------- | :-------- | :----- | :--------------------------------- |
| ts                | TIMESTAMP |        | timestamp                          |
| first_ep          | VARCHAR   |        | Cluster first ep                   |
| first_ep_dnode_id | INT       |        | Dnode ID of the cluster's first ep |
| cluster_version   | VARCHAR   |        | TDengine version, e.g., 3.0.4.0    |
| cluster_id        | VARCHAR   | TAG    | cluster id                         |

### taosd_cluster_info Table

The `taosd_cluster_info` table records cluster information.

| field                   | type      | is_tag | comment                                                      |
| :---------------------- | :-------- | :----- | :----------------------------------------------------------- |
| \_ts                     | TIMESTAMP |        | timestamp                                                    |
| cluster_uptime          | DOUBLE    |        | Current uptime of the master node; unit: seconds             |
| dbs_total               | DOUBLE    |        | Total number of databases                                    |
| tbs_total               | DOUBLE    |        | Current total number of tables in the cluster                |
| stbs_total              | DOUBLE    |        | Current total number of stable tables                        |
| dnodes_total            | DOUBLE    |        | Total number of dnodes in the cluster                        |
| dnodes_alive            | DOUBLE    |        | Total number of alive dnodes in the cluster                  |
| mnodes_total            | DOUBLE    |        | Total number of mnodes in the cluster                        |
| mnodes_alive            | DOUBLE    |        | Total number of alive mnodes in the cluster                  |
| vgroups_total           | DOUBLE    |        | Total number of vgroups in the cluster                       |
| vgroups_alive           | DOUBLE    |        | Total number of alive vgroups in the cluster                 |
| vnodes_total            | DOUBLE    |        | Total number of vnodes in the cluster                        |
| vnodes_alive            | DOUBLE    |        | Total number of alive vnodes in the cluster                  |
| connections_total       | DOUBLE    |        | Total number of connections in the cluster                   |
| topics_total            | DOUBLE    |        | Total number of topics in the cluster                        |
| streams_total           | DOUBLE    |        | Total number of streams in the cluster                       |
| grants_expire_time      | DOUBLE    |        | Expiration time for authentication, valid for enterprise version, max value for community version |
| grants_timeseries_used  | DOUBLE    |        | Used measuring points                                        |
| grants_timeseries_total | DOUBLE    |        | Total measuring points, max value for open source version    |
| cluster_id              | VARCHAR   | TAG    | cluster id                                                   |

### taosd_vgroups_info Table

The `taosd_vgroups_info` table records information about virtual node groups.

| field         | type      | is_tag | comment                                             |
| :------------ | :-------- | :----- | :-------------------------------------------------- |
| \_ts           | TIMESTAMP |        | timestamp                                           |
| tables_num    | DOUBLE    |        | Number of tables in the vgroup                      |
| status        | DOUBLE    |        | vgroup status, value range: unsynced = 0, ready = 1 |
| vgroup_id     | VARCHAR   | TAG    | vgroup id                                           |
| database_name | VARCHAR   | TAG    | Database name the vgroup belongs to                 |
| cluster_id    | VARCHAR   | TAG    | cluster id                                          |

### taosd_dnodes_info Table

The `taosd_dnodes_info` table records dnode information.

| field           | type      | is_tag | comment                                                      |
| :-------------- | :-------- | :----- | :----------------------------------------------------------- |
| \_ts             | TIMESTAMP |        | timestamp                                                    |
| uptime          | DOUBLE    |        | dnode uptime; unit: seconds                                  |
| cpu_engine      | DOUBLE    |        | taosd CPU usage, read from `/proc/<taosd_pid>/stat`          |
| cpu_system      | DOUBLE    |        | Server CPU usage, read from `/proc/stat`                     |
| cpu_cores       | DOUBLE    |        | Number of CPU cores on the server                            |
| mem_engine      | DOUBLE    |        | taosd memory usage, read from `/proc/<taosd_pid>/status`     |
| mem_free        | DOUBLE    |        | Available memory on the server; unit: KB                     |
| mem_total       | DOUBLE    |        | Total memory on the server; unit: KB                         |
| disk_used       | DOUBLE    |        | Disk usage of the data dir mount; unit: bytes                |
| disk_total      | DOUBLE    |        | Total disk capacity of the data dir mount; unit: bytes       |
| system_net_in   | DOUBLE    |        | Network throughput, read from `/proc/net/dev`, received bytes; unit: byte/s |
| system_net_out  | DOUBLE    |        | Network throughput, read from `/proc/net/dev`, transmitted bytes; unit: byte/s |
| io_read         | DOUBLE    |        | I/O throughput, calculated from rchar read from `/proc/<taosd_pid>/io` and previous values; unit: byte/s |
| io_write        | DOUBLE    |        | I/O throughput, calculated from wchar read from `/proc/<taosd_pid>/io` and previous values; unit: byte/s |
| io_read_disk    | DOUBLE    |        | Disk I/O throughput, read from read_bytes from `/proc/<taosd_pid>/io`; unit: byte/s |
| io_write_disk   | DOUBLE    |        | Disk I/O throughput, read from write_bytes from `/proc/<taosd_pid>/io`; unit: byte/s |
| vnodes_num      | DOUBLE    |        | Number of vnodes on the dnode                                |
| masters         | DOUBLE    |        | Number of master nodes on the dnode                          |
| has_mnode       | DOUBLE    |        | Whether the dnode contains an mnode, value range: contains=1, does not contain=0 |
| has_qnode       | DOUBLE    |        | Whether the dnode contains a qnode, value range: contains=1, does not contain=0 |
| has_snode       | DOUBLE    |        | Whether the dnode contains an snode, value range: contains=1, does not contain=0 |
| has_bnode       | DOUBLE    |        | Whether the dnode contains a bnode, value range: contains=1, does not contain=0 |
| error_log_count | DOUBLE    |        | Total number of errors                                       |
| info_log_count  | DOUBLE    |        | Total number of info messages                                |
| debug_log_count | DOUBLE    |        | Total number of debug messages                               |
| trace_log_count | DOUBLE    |        | Total number of trace messages                               |
| dnode_id        | VARCHAR   | TAG    | dnode id                                                     |
| dnode_ep        | VARCHAR   | TAG    | dnode endpoint                                               |
| cluster_id      | VARCHAR   | TAG    | cluster id                                                   |

### taosd_dnodes_status Table

The `taosd_dnodes_status` table records the status information of dnodes.

| field      | type      | is_tag | comment                                       |
| :--------- | :-------- | :----- | :-------------------------------------------- |
| \_ts        | TIMESTAMP |        | timestamp                                     |
| status     | DOUBLE    |        | dnode status; value range: ready=1, offline=0 |
| dnode_id   | VARCHAR   | TAG    | dnode id                                      |
| dnode_ep   | VARCHAR   | TAG    | dnode endpoint                                |
| cluster_id | VARCHAR   | TAG    | cluster id                                    |

### taosd_dnodes_log_dir Table

The `taosd_dnodes_log_dir` table records log directory information.

| field      | type      | is_tag | comment                                           |
| :--------- | :-------- | :----- | :------------------------------------------------ |
| \_ts        | TIMESTAMP |        | timestamp                                         |
| avail      | DOUBLE    |        | Available space in the log directory; unit: bytes |
| used       | DOUBLE    |        | Used space in the log directory; unit: bytes      |
| total      | DOUBLE    |        | Total space in the log directory; unit: bytes     |
| name       | VARCHAR   | TAG    | Log directory name, usually `/var/log/taos/`      |
| dnode_id   | VARCHAR   | TAG    | dnode id                                          |
| dnode_ep   | VARCHAR   | TAG    | dnode endpoint                                    |
| cluster_id | VARCHAR   | TAG    | cluster id                                        |

### taosd_dnodes_data_dir Table

The `taosd_dnodes_data_dir` table records data directory information.

| field | type      | is_tag | comment                                            |
| :---- | :-------- | :----- | :------------------------------------------------- |
| \_ts   | TIMESTAMP |        | timestamp                                          |
| avail | DOUBLE    |        | Available space in the data directory; unit: bytes |
| used  | DOUBLE    |        | Used space in the data directory; unit: bytes      |
| total | DOUBLE    |        | Total space in the data directory; unit: bytes     |
| level | VARCHAR   | TAG    | Multi-level storage level: 0, 1, 2                 |
| name  | VARCHAR   | TAG    | Data directory, usually `/var/lib/taos`            |

### taosd_mnodes_info Table

The `taosd_mnodes_info` table records mnode role information.

| field      | type      | is_tag | comment                                                      |
| :--------- | :-------- | :----- | :----------------------------------------------------------- |
| \_ts        | TIMESTAMP |        | timestamp                                                    |
| role       | DOUBLE    |        | mnode role; value range: offline = 0, follower = 100, candidate = 101, leader = 102, error = 103, learner = 104 |
| mnode_id   | VARCHAR   | TAG    | master node id                                               |
| mnode_ep   | VARCHAR   | TAG    | master node endpoint                                         |
| cluster_id | VARCHAR   | TAG    | cluster id                                                   |

### taosd_vnodes_role Table

The `taosd_vnodes_role` table records virtual node role information.

| field         | type      | is_tag | comment                                                      |
| :------------ | :-------- | :----- | :----------------------------------------------------------- |
| \_ts           | TIMESTAMP |        | timestamp                                                    |
| vnode_role    | DOUBLE    |        | vnode role; value range: offline = 0, follower = 100, candidate = 101, leader = 102, error = 103, learner = 104 |
| vgroup_id     | VARCHAR   | TAG    | dnode id                                                     |
| dnode_id      | VARCHAR   | TAG    | dnode id                                                     |
| database_name | VARCHAR   | TAG    | Database name the vgroup belongs to                          |

### taosd_sql_req Table

The `taosd_sql_req` table records server-side SQL request information.

| field      | type      | is_tag | comment                                            |
| :--------- | :-------- | :----- | :------------------------------------------------- |
| \_ts        | TIMESTAMP |        | timestamp                                          |
| count      | DOUBLE    |        | Number of SQL requests                             |
| result     | VARCHAR   | TAG    | SQL execution result; value range: Success, Failed |
| username   | VARCHAR   | TAG    | User name executing the SQL                        |
| sql_type   | VARCHAR   | TAG    | SQL type; value range: inserted_rows               |
| dnode_id   | VARCHAR   | TAG    | dnode id                                           |
| dnode_ep   | VARCHAR   | TAG    | dnode endpoint                                     |
| vgroup_id  | VARCHAR   | TAG    | vgroup id                                          |
| cluster_id | VARCHAR   | TAG    | cluster id                                         |

### taos_sql_req Table

The `taos_sql_req` table records client-side SQL request information.

| field      | type      | is_tag | comment                                            |
| :--------- | :-------- | :----- | :------------------------------------------------- |
| \_ts        | TIMESTAMP |        | timestamp                                          |
| count      | DOUBLE    |        | Number of SQL requests                             |
| result     | VARCHAR   | TAG    | SQL execution result; value range: Success, Failed |
| username   | VARCHAR   | TAG    | User name executing the SQL                        |
| sql_type   | VARCHAR   | TAG    | SQL type; value range: select, insert, delete      |
| cluster_id | VARCHAR   | TAG    | cluster id                                         |

### taos_slow_sql Table

The `taos_slow_sql` table records client-side slow query information.

| field      | type      | is_tag | comment                                                      |
| :--------- | :-------- | :----- | :----------------------------------------------------------- |
| \_ts        | TIMESTAMP |        | timestamp                                                    |
| count      | DOUBLE    |        | Number of SQL requests                                       |
| result     | VARCHAR   | TAG    | SQL execution result; value range: Success, Failed           |
| username   | VARCHAR   | TAG    | User name executing the SQL                                  |
| duration   | VARCHAR   | TAG    | SQL execution duration; value range: 3-10s, 10-100s, 100-1000s, 1000s- |
| cluster_id | VARCHAR   | TAG    | cluster id                                                   |

## Log Related

TDengine uses log files to record the system's operational status, helping users monitor the system's operation and troubleshoot issues. This section mainly introduces the system logs related to taosc and taosd.

TDengine's log files mainly include ordinary logs and slow logs.

1. Ordinary Log Behavior Explanation
    1. Multiple client processes can run on the same machine, so client log naming follows the format taoslogX.Y, where X is the sequence number (which can be empty or a digit from 0 to 9) and Y is the suffix (0 or 1).
    2. Only one server process can run on the same machine, so server log naming follows the format taosdlog.Y, where Y is the suffix (0 or 1).
       The rules for determining sequence numbers and suffixes are as follows (assuming the log path is /var/log/taos/):
        1. Determine the sequence number: Use 10 sequence numbers as log naming, from /var/log/taos/taoslog0.Y to /var/log/taos/taoslog9.Y, checking each sequence number to see if it is in use. The first unused sequence number will be used for the log file of that process. If all 10 sequence numbers are in use, the sequence number will be empty, i.e., /var/log/taos/taoslog.Y, and all processes will write to the same file.
        2. Determine the suffix: 0 or 1. For example, if the determined sequence number is 3, the alternative log file names will be /var/log/taos/taoslog3.0 and /var/log/taos/taoslog3.1. If both files do not exist, use suffix 0; if one exists and one does not, use the existing suffix. If both exist, use the one with the most recent modification time.
    3. If the log file exceeds the configured number of lines (numOfLogLines), it will switch suffixes and continue logging. For example, when /var/log/taos/taoslog3.0 is filled, it will switch to /var/log/taos/taoslog3.1 to continue logging. /var/log/taos/taoslog3.0 will be renamed with a timestamp suffix and compressed (asynchronous thread operation).
    4. The parameter logKeepDays controls how many days the log files are retained. For instance, if configured to 1, logs older than one day will be checked and deleted when new logs are compressed. This is not based on natural days.

In addition to recording ordinary logs, SQL statements that exceed the configured execution time will be recorded in the slow logs. Slow log files are primarily used to analyze system performance and troubleshoot performance issues.

2. Slow Log Behavior Explanation
    1. Slow logs will be recorded in the local slow log file and, simultaneously, sent to taosKeeper for structured storage (the monitor switch must be enabled).
    2. Slow log file storage rules are:
        1. A slow log file is generated for each day; if there are no slow logs on that day, there will be no file for that day.
        2. The file name is taosSlowLog.yyyy-mm-dd (e.g., taosSlowLog.2024-08-02), and the log storage path is specified by the logDir configuration.
        3. Logs from multiple clients will be stored in the same taosSlowLog.yyyy-mm-dd file in the specified log path.
        4. Slow log files are not automatically deleted and are not compressed.
        5. They share the same three parameters as ordinary log files: logDir, minimalLogDirGB, and asyncLog. The other two parameters, numOfLogLines and logKeepDays, do not apply to slow logs.
