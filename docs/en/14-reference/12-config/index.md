---
title: Configuration Parameters
description: This document describes the configuration parameters for the TDengine server and client.
---

## Configuration File on Server Side

On the server side, the actual service of TDengine is provided by an executable `taosd` whose parameters can be configured in file `taos.cfg` to meet the requirements of different use cases. The default location of `taos.cfg` is `/etc/taos` on Linux system, it's located under `C:\TDengine` on Windows system. The location of configuration file can be specified by using `-c` parameter on the CLI of `taosd`. For example, on Linux system the configuration file can be put under `/home/user` and used like below

```
taosd -c /home/user
```

Parameter `-C` can be used on the CLI of `taosd` to show its configuration, like below:

```
taosd -C
```

## Configuration File on Client Side

TDengine CLI `taos` is the tool for users to interact with TDengine. It can share same configuration file as `taosd` or use a separate configuration file. When launching `taos`, parameter `-c` can be used to specify the location where its configuration file is. For example:

```
taos -c /home/cfg
```

means `/home/cfg/taos.cfg` will be used. If `-c` is not used, the default location of the configuration file is `/etc/taos`. For more details please use `taos --help` to get.

Parameter `-C` can be used on the CLI of `taos` to show its configuration, like below:

```bash
taos -C
```

## Configuration Parameters

:::note
The parameters described in this document by the effect that they have on the system.

:::

:::note
`taosd` needs to be restarted for the parameters changed in the configuration file to take effect.

:::

## Connection Parameters

### firstEp

| Attribute  | Description                                                                                          |
| ---------- | ---------------------------------------------------------------------------------------------------- |
| Applicable | Server and Client                                                                                    |
| Meaning    | The end point of the first dnode in the cluster to be connected to when `taosd` or `taos` is started |
| Default    | localhost:6030                                                                                       |

### secondEp

| Attribute  | Description                                                                                                            |
| ---------- | ---------------------------------------------------------------------------------------------------------------------- |
| Applicable | Server and Client                                                                                                      |
| Meaning    | The end point of the second dnode to be connected to if the firstEp is not available when `taosd` or `taos` is started |
| Default    | None                                                                                                                   |

### fqdn

| Attribute     | Description                                                              |
| ------------- | ------------------------------------------------------------------------ |
| Applicable    | Server Only                                                              |
| Meaning       | The FQDN of the host where `taosd` will be started. It can be IP address |
| Default Value | The first hostname configured for the host                               |
| Note          | It should be within 96 bytes                                             |  |

### serverPort

| Attribute     | Description                                           |
| ------------- | ----------------------------------------------------- |
| Applicable    | Server Only                                           |
| Meaning       | The port for external access after `taosd` is started |
| Default Value | 6030                                                  |

:::note
Ensure that your firewall rules do not block TCP port 6042  on any host in the cluster. Below table describes the ports used by TDengine in details.
:::

| Protocol | Default Port | Description                                                                                               | How to configure                                                                               |
| :------- | :----------- | :-------------------------------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------- |
| TCP      | 6030         | Communication between client and server. In a multi-node cluster, communication between nodes. serverPort |
| TCP      | 6041         | REST connection between client and server                                                                 | Prior to 2.4.0.0: serverPort+11; After 2.4.0.0 refer to [taosAdapter](../taosadapter/) |
| TCP      | 6043         | Service Port of taosKeeper                                                                                | The parameter of taosKeeper                                                                    |
| TCP      | 6044         | Data access port for StatsD                                                                               | Configurable through taosAdapter parameters.                                                   |
| UDP      | 6045         | Data access for statsd                                                                                    | Configurable through taosAdapter parameters.                                                   |
| TCP      | 6060         | Port of Monitoring Service in Enterprise version                                                          |                                                                                                |

### maxShellConns

| Attribute     | Description                                          |
| ------------- | ---------------------------------------------------- |
| Applicable    | Server Only                                          |
| Meaning       | The maximum number of connections a dnode can accept |
| Value Range   | 10-50000000                                          |
| Default Value | 5000                                                 |

### numOfRpcSessions

| Attribute     | Description                                |
| ------------- | ------------------------------------------ |
| Applicable    | Client/Server                              |
| Meaning       | The maximum number of connection to create |
| Value Range   | 100-100000                                 |
| Default Value | 10000                                      |

### timeToGetAvailableConn

| Attribute     | Description                                    |
| ------------- | ---------------------------------------------- |
| Applicable    | Client/Server                                  |
| Meaning       | The maximum waiting time to get available conn |
| Value Range   | 10-50000000(ms)                                |
| Default Value | 500000                                         |

## Monitoring Parameters

:::note
Please note the `taoskeeper` needs to be installed and running to create the `log` database and receiving metrics sent by `taosd` as the full monitoring solution.

:::

### monitor

| Attribute   | Description                                                                                                                                                                                                                                                                                                           |
| ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Applicable  | Server only                                                                                                                                                                                                                                                                                                           |
| Meaning     | The switch for monitoring inside server. The main object of monitoring is to collect information about load on physical nodes, including CPU usage, memory usage, disk usage, and network bandwidth. Monitoring information is sent over HTTP to the taosKeeper service specified by `monitorFqdn` and `monitorProt`. |
| Value Range | 0: monitoring disabled, 1: monitoring enabled                                                                                                                                                                                                                                                                         |
| Default     | 0                                                                                                                                                                                                                                                                                                                     |

### monitorFqdn

| Attribute  | Description                           |
| ---------- | ------------------------------------- |
| Applicable | Server Only                           |
| Meaning    | FQDN of taosKeeper monitoring service |
| Default    | None                                  |

### monitorPort

| Attribute     | Description                           |
| ------------- | ------------------------------------- |
| Applicable    | Server Only                           |
| Meaning       | Port of taosKeeper monitoring service |
| Default Value | 6043                                  |

### monitorInterval

| Attribute     | Description                                |
| ------------- | ------------------------------------------ |
| Applicable    | Server Only                                |
| Meaning       | The interval of collecting system workload |
| Unit          | second                                     |
| Value Range   | 1-200000                                   |
| Default Value | 30                                         |

### telemetryReporting

| Attribute     | Description                                                                  |
| ------------- | ---------------------------------------------------------------------------- |
| Applicable    | Server and Client                                                            |
| Meaning       | Switch for allowing TDengine to collect and report service usage information |
| Value Range   | 0: Not allowed; 1: Allowed                                                   |
| Default Value | 1                                                                            |
### crashReporting

| Attribute     | Description                                                                  |
| ------------- | ---------------------------------------------------------------------------- |
| Applicable    | Server and Client                                                            |
| Meaning       | Switch for allowing TDengine to collect and report crash related information |
| Value Range   | 0,1   0: Not allowed; 1: allowed                                             |
| Default Value | 1                                                                            |


## Query Parameters

### queryPolicy

| Attribute   | Description                                                                                                                                                                                                           |
| ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Applicable  | Client only                                                                                                                                                                                                           |
| Meaning     | Execution policy for query statements                                                                                                                                                                                 |
| Unit        | None                                                                                                                                                                                                                  |
| Default     | 1                                                                                                                                                                                                                     |
| Value Range | 1: Run queries on vnodes and not on qnodes; 2: Run subtasks without scan operators on qnodes and subtasks with scan operators on vnodes; 3: Only run scan operators on vnodes, and run all other operators on qnodes. |

### querySmaOptimize

| Attribute     | Description                                                                                                                                                         |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Applicable    | Client only                                                                                                                                                         |
| Meaning       | SMA index optimization policy                                                                                                                                       |
| Unit          | None                                                                                                                                                                |
| Default Value | 0                                                                                                                                                                   |
| Notes         | 0: Disable SMA indexing and perform all queries on non-indexed data; 1: Enable SMA indexing and perform queries from suitable statements on precomputation results. |

### countAlwaysReturnValue

| Attribute  | Description                                                                                                                                                                                                                     |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Applicable | Server only                                                                                                                                                                                                                     |
| Meaning    | count()/hyperloglog() return value or not if the input data is empty or NULL                                                                                                                                                    |
| Vlue Range | 0: Return empty line, 1: Return 0                                                                                                                                                                                               |
| Default    | 1                                                                                                                                                                                                                               |
| Notes      | When this parameter is setting to 1, for queries containing GROUP BY, PARTITION BY and INTERVAL clause, and input data in certain groups or windows is empty or NULL, the corresponding groups or windows have no return values |

### maxNumOfDistinctRes

| Attribute     | Description                                  |
| ------------- | -------------------------------------------- |
| Applicable    | Server Only                                  |
| Meaning       | The maximum number of distinct rows returned |
| Value Range   | [100,000 - 100,000,000]                      |
| Default Value | 100,000                                      |

### keepColumnName

| Attribute     | Description                                                                                                     |
| ------------- | --------------------------------------------------------------------------------------------------------------- |
| Applicable    | Client only                                                                                                     |
| Meaning       | When the Last, First, and LastRow functions are queried and no alias is specified, the alias is automatically set to the column name (excluding the function name). Therefore, if the order by clause refers to the column name, it will automatically refer to the function corresponding to the column. |
| Value Range   | 1 means automatically setting the alias to the column name (excluding the function name), 0 means not automatically setting the alias.                                   |
| Default Value | 0          |
| Notes | When multiple of the above functions act on the same column at the same time and no alias is specified, if the order by clause refers to the column name, column selection ambiguous will occur because the aliases of multiple columns are the same. |

## Locale Parameters

### timezone

| Attribute     | Description                     |
| ------------- | ------------------------------- |
| Applicable    | Server and Client               |
| Meaning       | TimeZone                        |
| Default Value | TimeZone configured in the host |

:::info
To handle the data insertion and data query from multiple timezones, Unix Timestamp is used and stored in TDengine. The timestamp generated from any timezones at same time is same in Unix timestamp. Note that Unix timestamps are converted and recorded on the client side. To make sure the time on client side can be converted to Unix timestamp correctly, the timezone must be set properly.

On Linux/macOS, TDengine clients automatically obtain timezone from the host. Alternatively, the timezone can be configured explicitly in configuration file `taos.cfg` like below. For example:

```
timezone UTC-8
timezone GMT-8
timezone Asia/Shanghai
```

The above examples are all proper configuration for the timezone of UTC+8. On Windows system, however, `timezone Asia/Shanghai` is not supported, it must be set as `timezone UTC-8`.

The setting for timezone impacts strings that are not in Unix timestamp format and keywords or functions related to date/time. For example:

```sql
SELECT count(*) FROM table_name WHERE TS<'2019-04-11 12:01:08';
```

If the timezone is UTC+8, the above SQL statement is equal to:

```sql
SELECT count(*) FROM table_name WHERE TS<1554955268000;
```

If the timezone is UTC, it's equal to

```sql
SELECT count(*) FROM table_name WHERE TS<1554984068000;
```

To avoid the problems of using time strings, Unix timestamp can be used directly. Furthermore, time strings with timezone can be used in SQL statements. For example "2013-04-12T15:52:01.123+08:00" in RFC3339 format or "2013-04-12T15:52:01.123+0800" in ISO-8601 format are not influenced by timezone setting when converted to Unix timestamp.

:::

### locale

| Attribute     | Description               |
| ------------- | ------------------------- |
| Applicable    | Server and Client         |
| Meaning       | Location code             |
| Default Value | Locale configured in host |

:::info
A specific type "nchar" is provided in TDengine to store non-ASCII characters such as Chinese, Japanese, and Korean. The characters to be stored in nchar type are firstly encoded in UCS4-LE before sending to server side. Note that the correct encoding is determined by the user. To store non-ASCII characters correctly, the encoding format of the client side needs to be set properly.

The characters input on the client side are encoded using the default system encoding, which is UTF-8 on Linux/macOS, or GB18030 or GBK on some systems in Chinese, POSIX in docker, CP936 on Windows in Chinese. The encoding of the operating system in use must be set correctly so that the characters in nchar type can be converted to UCS4-LE.

The locale definition standard on Linux/macOS is: &lt;Language&gt;\_&lt;Region&gt;.&lt;charset&gt;, for example, in "zh_CN.UTF-8", "zh" means Chinese, "CN" means China mainland, "UTF-8" means charset. The charset indicates how to display the characters. On Linux/macOS, the charset can be set by locale in the system. On Windows system another configuration parameter `charset` must be used to configure charset because the locale used on Windows is not POSIX standard. Of course, `charset` can also be used on Linux/macOS to specify the charset.

:::

### charset

| Attribute     | Description               |
| ------------- | ------------------------- |
| Applicable    | Server and Client         |
| Meaning       | Character                 |
| Default Value | charset set in the system |

:::info
On Linux/macOS, if `charset` is not set in `taos.cfg`, when `taos` is started, the charset is obtained from system locale. If obtaining charset from system locale fails, `taos` would fail to start.

So on Linux/macOS, if system locale is set properly, it's not necessary to set `charset` in `taos.cfg`. For example:

```
locale zh_CN.UTF-8
```

On Windows system, it's not possible to obtain charset from system locale. If it's not set in configuration file `taos.cfg`, it would be default to CP936, same as set as below in `taos.cfg`. For example

```
charset CP936
```

Refer to the documentation for your operating system before changing the charset.

On a Linux/macOS, if the charset contained in `locale` is not consistent with that set by `charset`, the later setting in the configuration file takes precedence.

```
locale zh_CN.UTF-8
charset GBK
```

The charset that takes effect is GBK.

```
charset GBK
locale zh_CN.UTF-8
```

The charset that takes effect is UTF-8.

:::

## Storage Parameters

### dataDir

| Attribute     | Description                                                                                                                                                                                                 |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Applicable    | Server Only                                                                                                                                                                                                 |
| Meaning       | All data files are stored in this directory                                                                                                                                                                 |
| Default Value | /var/lib/taos                                                                                                                                                                                               |
| Note          | The [Tiered Storage](https://docs.tdengine.com/tdinternal/arch/#tiered-storage) function needs to be used in conjunction with the [KEEP](https://docs.tdengine.com/taos-sql/database/#parameters) parameter |

### tempDir

| Attribute  | Description                                                                        |
| ---------- | ---------------------------------------------------------------------------------- |
| Applicable | Server only                                                                        |
| Meaning    | The directory where to put all the temporary files generated during system running |
| Default    | /tmp                                                                               |

### minimalTmpDirGB

| Attribute     | Description                                                                                     |
| ------------- | ----------------------------------------------------------------------------------------------- |
| Applicable    | Server and Client                                                                               |
| Meaning       | When the available disk space in tmpDir is below this threshold, writing to tmpDir is suspended |
| Unit          | GB                                                                                              |
| Default Value | 1.0                                                                                             |

### minimalDataDirGB

| Attribute     | Description                                                                                       |
| ------------- | ------------------------------------------------------------------------------------------------- |
| Applicable    | Server Only                                                                                       |
| Meaning       | When the available disk space in dataDir is below this threshold, writing to dataDir is suspended |
| Unit          | GB                                                                                                |
| Default Value | 2.0                                                                                               |

### metaCacheMaxSize

| Attribute     | Description                                                                                       |
| ------------- | ------------------------------------------------------------------------------------------------- |
| Applicable    | Client Only                                                                                       |
| Meaning       | Maximum meta cache size in single client process                                                  |
| Unit          | MB                                                                                                |
| Default Value | -1 (No limitation)                                                                                |


## Cluster Parameters

### supportVnodes

| Attribute     | Description                        |
| ------------- | ---------------------------------- |
| Applicable    | Server Only                        |
| Meaning       | Maximum number of vnodes per dnode |
| Value Range   | 0-4096                             |
| Default Value | 2x the CPU cores                   |

## Performance Tuning

### numOfCommitThreads

| Attribute     | Description                         |
| ------------- | ----------------------------------- |
| Applicable    | Server Only                         |
| Meaning       | Maximum number of threads to commit |
| Value Range   | 0-1024                              |
| Default Value |                                     |

## Log Parameters

### logDir

| Attribute     | Description                         |
| ------------- | ----------------------------------- |
| Applicable    | Server and Client                   |
| Meaning       | The directory for writing log files |
| Default Value | /var/log/taos                       |

### minimalLogDirGB

| Attribute     | Description                                                                                        |
| ------------- | -------------------------------------------------------------------------------------------------- |
| Applicable    | Server and Client                                                                                  |
| Meaning       | When the available disk space in logDir is below this threshold, writing to log files is suspended |
| Unit          | GB                                                                                                 |
| Default Value | 1.0                                                                                                |

### numOfLogLines

| Attribute     | Description                                |
| ------------- | ------------------------------------------ |
| Applicable    | Server and Client                          |
| Meaning       | Maximum number of lines in single log file |
| Default Value | 10000000                                   |

### asyncLog

| Attribute     | Description                  |
| ------------- | ---------------------------- |
| Applicable    | Server and Client            |
| Meaning       | The mode of writing log file |
| Value Range   | 0: sync way; 1: async way    |
| Default Value | 1                            |

### logKeepDays

| Attribute     | Description                                                                                                                                 |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| Applicable    | Server and Client                                                                                                                           |
| Meaning       | The number of days for log files to be kept                                                                                                 |
| Unit          | day                                                                                                                                         |
| Default Value | 0                                                                                                                                           |
| Note          | When it's bigger than 0, the log file would be renamed to "taosdlog.xxx" in which "xxx" is the timestamp when the file is changed last time |

### slowLogThreshold

| Attribute     | Description                                                                                              |
| ------------- | -------------------------------------------------------------------------------------------------------- |
| Applicable    | Client only                                                                                              |
| Meaning       | When an operation execution time exceeds this threshold, the operation will be logged in slow log file   |
| Unit          | second                                                                                                   |
| Default Value | 3                                                                                                        |
| Note          | All slow operations will be logged in file "taosSlowLog" in the log directory                            |

### slowLogScope

| Attribute       | Description                                                             |
| --------------- | ----------------------------------------------------------------------- |
| Applicable      | Client only                                                             |
| Meaning         | Slow log type to be logged                                              |
| Optional Values | ALL, QUERY, INSERT, OTHERS, NONE                                        |
| Default Value   | ALL                                                                     |
| Note            | All slow operations will be logged by default, one option could be set  |

### debugFlag

| Attribute     | Description                                               |
| ------------- | --------------------------------------------------------- |
| Applicable    | Server and Client                                         |
| Meaning       | Log level                                                 |
| Value Range   | 131: INFO/WARNING/ERROR; 135: plus DEBUG; 143: plus TRACE |
| Default Value | 131 or 135, depending on the module                       |

### tmrDebugFlag

| Attribute     | Description               |
| ------------- | ------------------------- |
| Applicable    | Server and Client         |
| Meaning       | Log level of timer module |
| Value Range   | same as debugFlag         |
| Default Value |                           |

### uDebugFlag

| Attribute     | Description                |
| ------------- | -------------------------- |
| Applicable    | Server and Client          |
| Meaning       | Log level of common module |
| Value Range   | same as debugFlag          |
| Default Value |                            |

### rpcDebugFlag

| Attribute     | Description             |
| ------------- | ----------------------- |
| Applicable    | Server and Client       |
| Meaning       | Log level of rpc module |
| Value Range   | same as debugFlag       |
| Default Value |                         |

### jniDebugFlag

| Attribute     | Description             |
| ------------- | ----------------------- |
| Applicable    | Client Only             |
| Meaning       | Log level of jni module |
| Value Range   | same as debugFlag       |
| Default Value |                         |

### qDebugFlag

| Attribute     | Description               |
| ------------- | ------------------------- |
| Applicable    | Server and Client         |
| Meaning       | Log level of query module |
| Value Range   | same as debugFlag         |
| Default Value |                           |

### cDebugFlag

| Attribute     | Description         |
| ------------- | ------------------- |
| Applicable    | Client Only         |
| Meaning       | Log level of Client |
| Value Range   | same as debugFlag   |
| Default Value |                     |

### dDebugFlag

| Attribute     | Description        |
| ------------- | ------------------ |
| Applicable    | Server Only        |
| Meaning       | Log level of dnode |
| Value Range   | same as debugFlag  |
| Default Value | 135                |

### vDebugFlag

| Attribute     | Description        |
| ------------- | ------------------ |
| Applicable    | Server Only        |
| Meaning       | Log level of vnode |
| Value Range   | same as debugFlag  |
| Default Value |                    |

### mDebugFlag

| Attribute     | Description               |
| ------------- | ------------------------- |
| Applicable    | Server Only               |
| Meaning       | Log level of mnode module |
| Value Range   | same as debugFlag         |
| Default Value | 135                       |

### wDebugFlag

| Attribute     | Description             |
| ------------- | ----------------------- |
| Applicable    | Server Only             |
| Meaning       | Log level of WAL module |
| Value Range   | same as debugFlag       |
| Default Value | 135                     |

### sDebugFlag

| Attribute     | Description              |
| ------------- | ------------------------ |
| Applicable    | Server and Client        |
| Meaning       | Log level of sync module |
| Value Range   | same as debugFlag        |
| Default Value | 135                      |

### tsdbDebugFlag

| Attribute     | Description              |
| ------------- | ------------------------ |
| Applicable    | Server Only              |
| Meaning       | Log level of TSDB module |
| Value Range   | same as debugFlag        |
| Default Value |                          |

### tqDebugFlag

| Attribute     | Description            |
| ------------- | ---------------------- |
| Applicable    | Server only            |
| Meaning       | Log level of TQ module |
| Value Range   | same as debugFlag      |
| Default Value |                        |

### fsDebugFlag

| Attribute     | Description            |
| ------------- | ---------------------- |
| Applicable    | Server only            |
| Meaning       | Log level of FS module |
| Value Range   | same as debugFlag      |
| Default Value |                        |

### udfDebugFlag

| Attribute     | Description             |
| ------------- | ----------------------- |
| Applicable    | Server Only             |
| Meaning       | Log level of UDF module |
| Value Range   | same as debugFlag       |
| Default Value |                         |

### smaDebugFlag

| Attribute     | Description             |
| ------------- | ----------------------- |
| Applicable    | Server Only             |
| Meaning       | Log level of SMA module |
| Value Range   | same as debugFlag       |
| Default Value |                         |

### idxDebugFlag

| Attribute     | Description               |
| ------------- | ------------------------- |
| Applicable    | Server Only               |
| Meaning       | Log level of index module |
| Value Range   | same as debugFlag         |
| Default Value |                           |

### tdbDebugFlag

| Attribute     | Description             |
| ------------- | ----------------------- |
| Applicable    | Server Only             |
| Meaning       | Log level of TDB module |
| Value Range   | same as debugFlag       |
| Default Value |                         |

## Schemaless Parameters

### smlChildTableName

| Attribute     | Description                                |
| ------------- | ------------------------------------------ |
| Applicable    | Client only                                |
| Meaning       | Custom subtable name for schemaless writes |
| Type          | String                                     |
| Default Value | None                                       |

### smlAutoChildTableNameDelimiter

| Attribute     | Description                                |
| ------------- | ------------------------------------------ |
| Applicable    | Client only                                |
| Meaning       | Delimiter between tags as table name|
| Type          | String                                     |
| Default Value | None                                       |

### smlTagName

| Attribute     | Description                                                   |
| ------------- | ------------------------------------------------------------- |
| Applicable    | Client only                                                   |
| Meaning       | Default tag for schemaless writes without tag value specified |
| Type          | String                                                        |
| Default Value | _tag_null                                                     |

### smlDataFormat

| Attribute   | Description                                                                         |
| ----------- | ----------------------------------------------------------------------------------- |
| Applicable  | Client only                                                                         |
| Meaning     | Whether schemaless columns are consistently ordered, depat, discarded since 3.0.3.0 |
| Value Range | 0: not consistent; 1: consistent.                                                   |
| Default     | 0                                                                                   |

### smlTsDefaultName

| Attribute     | Description                                                     |
| --------      | --------------------------------------------------------        |
| Applicable    | Client only                                                     |
| Meaning       | The name of the time column for schemaless automatic table creation is set through this configuration |
| Type          | String                                                          |
| Default Value | _ts                                                             |

## Compress Parameters

### compressMsgSize

| Attribute   | Description                                                                                                        |
| ----------- | ------------------------------------------------------------------------------------------------------------------ |
| Applicable  | Both Client and Server side                                                                                        |
| Meaning     | Whether RPC message is compressed                                                                                  |
| Value Range | -1: none message is compressed; 0: all messages are compressed; N (N>0): messages exceeding N bytes are compressed |
| Default     | -1                                                                                                                 |


## Other Parameters

### enableCoreFile

| Attribute     | Description                                                                                                                                                                                                        |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Applicable    | Server and Client                                                                                                                                                                                                  |
| Meaning       | Whether to generate core file when server crashes                                                                                                                                                                  |
| Value Range   | 0: false, 1: true                                                                                                                                                                                                  |
| Default Value | 1                                                                                                                                                                                                                  |
| Note          | The core file is generated under root directory `systemctl start taosd`/`launchctl start com.tdengine.taosd` is used to start, or under the working directory if `taosd` is started directly on Linux/macOS Shell. |

### enableScience

| Attribute     | Description                                                   |
| ------------- | ------------------------------------------------------------- |
| Applicable    | Only taos-CLI client                                          |
| Meaning       | Whether to show float and double with the scientific notation |
| Value Range   | 0: false, 1: true                                             |
| Default Value | 0                                                             |


### udf

| Attribute     | Description                        |
| ------------- | ---------------------------------- |
| Applicable    | Server Only                        |
| Meaning       | Whether the UDF service is enabled |
| Value Range   | 0: disable UDF; 1: enabled UDF     |
| Default Value | 1                                  |

### ttlChangeOnWrite

| Attribute     | Description                                                                   |
| ------------- | ----------------------------------------------------------------------------- |
| Applicable    | Server Only                                                                   |
| Meaning       | Whether the ttl expiration time changes with the table modification operation |
| Value Range   | 0: not change; 1: change by modification                                      |
| Default Value | 0                                                                             |

### tmqMaxTopicNum

| Attribute     | Description               |
| -------- | ------------------ |
| Applicable | Server Only       |
| Meaning     | The max num of topics  |
| Value Range | 1-10000|
| Default Value   | 20                  |

## 3.0 Parameters

| #   |     **Parameter**      | **Applicable to 2.x ** | **Applicable to  3.0 **      | Current behavior in 3.0 |
| --- | :--------------------: | ---------------------- | ---------------------------- | ----------------------- |
| 1   |        firstEp         | Yes                    | Yes                          |                         |
| 2   |        secondEp        | Yes                    | Yes                          |                         |
| 3   |          fqdn          | Yes                    | Yes                          |                         |
| 4   |       serverPort       | Yes                    | Yes                          |                         |
| 5   |     maxShellConns      | Yes                    | Yes                          |                         |
| 6   |        monitor         | Yes                    | Yes                          |                         |
| 7   |      monitorFqdn       | No                     | Yes                          |                         |
| 8   |      monitorPort       | No                     | Yes                          |                         |
| 9   |    monitorInterval     | Yes                    | Yes                          |                         |
| 10  |      queryPolicy       | No                     | Yes                          |                         |
| 11  |    querySmaOptimize    | No                     | Yes                          |                         |
| 12  |  maxNumOfDistinctRes   | Yes                    | Yes                          |                         |
| 15  | countAlwaysReturnValue | Yes                    | Yes                          |                         |
| 16  |        dataDir         | Yes                    | Yes                          |                         |
| 17  |    minimalDataDirGB    | Yes                    | Yes                          |                         |
| 18  |     supportVnodes      | No                     | Yes                          |                         |
| 19  |        tempDir         | Yes                    | Yes                          |                         |
| 20  |    minimalTmpDirGB     | Yes                    | Yes                          |                         |
| 21  |   smlChildTableName    | Yes                    | Yes                          |                         |
| 22  |       smlTagName       | Yes                    | Yes                          |                         |
| 23  |     smlDataFormat      | No                     | Yes(discarded since 3.0.3.0) |                         |
| 24  |     statusInterval     | Yes                    | Yes                          |                         |
| 25  |         logDir         | Yes                    | Yes                          |                         |
| 26  |    minimalLogDirGB     | Yes                    | Yes                          |                         |
| 27  |     numOfLogLines      | Yes                    | Yes                          |                         |
| 28  |        asyncLog        | Yes                    | Yes                          |                         |
| 29  |      logKeepDays       | Yes                    | Yes                          |                         |
| 30  |       debugFlag        | Yes                    | Yes                          |                         |
| 31  |      tmrDebugFlag      | Yes                    | Yes                          |                         |
| 32  |       uDebugFlag       | Yes                    | Yes                          |                         |
| 33  |      rpcDebugFlag      | Yes                    | Yes                          |                         |
| 34  |      jniDebugFlag      | Yes                    | Yes                          |                         |
| 35  |       qDebugFlag       | Yes                    | Yes                          |                         |
| 36  |       cDebugFlag       | Yes                    | Yes                          |                         |
| 37  |       dDebugFlag       | Yes                    | Yes                          |                         |
| 38  |       vDebugFlag       | Yes                    | Yes                          |                         |
| 39  |       mDebugFlag       | Yes                    | Yes                          |                         |
| 40  |       wDebugFlag       | Yes                    | Yes                          |                         |
| 41  |       sDebugFlag       | Yes                    | Yes                          |                         |
| 42  |     tsdbDebugFlag      | Yes                    | Yes                          |                         |
| 43  |      tqDebugFlag       | No                     | Yes                          |                         |
| 44  |      fsDebugFlag       | Yes                    | Yes                          |                         |
| 45  |      udfDebugFlag      | No                     | Yes                          |                         |
| 46  |      smaDebugFlag      | No                     | Yes                          |                         |
| 47  |      idxDebugFlag      | No                     | Yes                          |                         |
| 48  |      tdbDebugFlag      | No                     | Yes                          |                         |
| 49  |     metaDebugFlag      | No                     | Yes                          |                         |
| 50  |        timezone        | Yes                    | Yes                          |                         |
| 51  |         locale         | Yes                    | Yes                          |                         |
| 52  |        charset         | Yes                    | Yes                          |                         |
| 53  |          udf           | Yes                    | Yes                          |                         |
| 54  |     enableCoreFile     | Yes                    | Yes                          |                         |
| 55  |    ttlChangeOnWrite    | No                     | Yes                          |                         |
| 56  |     keepTimeOffset     | Yes                    | Yes(discarded since 3.2.0.0) |                         |
