---
title: Configuration Parameters
description: "Configuration parameters for client and server in TDengine"
---

## Configuration File on Server Side

On the server side, the actual service of TDengine is provided by an executable `taosd` whose parameters can be configured in file `taos.cfg` to meet the requirements of different use cases. The default location of `taos.cfg` is `/etc/taos`, but can be changed by using `-c` parameter on the CLI of `taosd`. For example, the configuration file can be put under `/home/user` and used like below

```
taosd -c /home/user
```

Parameter `-C` can be used on the CLI of `taosd` to show its configuration, like below:

```
taosd -C
```

## Configuration File on Client Side

TDengine CLI `taos` is the tool for users to interact with TDengine. It can share same configuration file as `taosd` or use a separate configuration file. When launching `taos`, parameter `-c` can be used to specify the location where its configuration file is. For example `taos -c /home/cfg` means `/home/cfg/taos.cfg` will be used. If `-c` is not used, the default location of the configuration file is `/etc/taos`. For more details please use `taos --help` to get.

```bash
taos -C
```

```bash
taos --dump-config
```

# Configuration Parameters

:::note
The parameters described in this document by the effect that they have on the system.

:::

:::note
`taosd` needs to be restarted for the parameters changed in the configuration file to take effect.

:::

## Connection Parameters

### firstEp

| Attribute     | Description                            |
| -------- | -------------------------------------------------------------- |
| Applicable | Server and Client                                           |
| Meaning       | The end point of the first dnode in the cluster to be connected to when `taosd` or `taos` is started |
| Default   | localhost:6030                                                 |

### secondEp

| Attribute     | Description                                                                                  |
| -------- | ------------------------------------------------------------------------------------- |
| Applicable | Server and Client                                           |
| Meaning       | The end point of the second dnode to be connected to if the firstEp is not available when `taosd` or `taos` is started |
| Default   | None                                                                                    |

### fqdn

| Attribute     | Description                                                              |
| ------------- | ------------------------------------------------------------------------ |
| Applicable    | Server Only                                                              |
| Meaning       | The FQDN of the host where `taosd` will be started. It can be IP address |
| Default Value | The first hostname configured for the host                             |
| Note          | It should be within 96 bytes                                             |                        |

### serverPort

| Attribute     | Description                                                              |
| -------- | ----------------------------------------------------------------------------------------------------------------------- |
| Applicable    | Server Only                                                              |
| Meaning       | The port for external access after `taosd` is started                                                                           |
| Default Value | 6030                                                                                                                            |

:::note
- Ensure that your firewall rules do not block TCP port 6042  on any host in the cluster. Below table describes the ports used by TDengine in details.
:::
| Protocol | Default Port | Description                                      | How to configure                                                                               |
| :------- | :----------- | :----------------------------------------------- | :--------------------------------------------------------------------------------------------- |
| TCP | 6030 | Communication between client and server. In a multi-node cluster, communication between nodes. serverPort |
| TCP      | 6041         | REST connection between client and server        | Prior to 2.4.0.0: serverPort+11; After 2.4.0.0 refer to [taosAdapter](/reference/taosadapter/) |
| TCP      | 6043         | Service Port of TaosKeeper                       | The parameter of TaosKeeper |
| TCP      | 6044         | Data access port for StatsD                      | Configurable through taosAdapter parameters.
| UDP      | 6045         | Data access for statsd                           | Configurable through taosAdapter parameters.
| TCP      | 6060         | Port of Monitoring Service in Enterprise version | |

### maxShellConns

| Attribute     | Description                                          |
| ------------- | ---------------------------------------------------- |
| Applicable    | Server Only                                          |
| Meaning       | The maximum number of connections a dnode can accept |
| Value Range   | 10-50000000                                          |
| Default Value | 5000                                                 |

## Monitoring Parameters

### monitor

| Attribute     | Description                                                                                                                                                                                               |
| -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Applicable | Server only                                           |
| Meaning       | The switch for monitoring inside server. The main object of monitoring is to collect information about load on physical nodes, including CPU usage, memory usage, disk usage, and network bandwidth. Monitoring information is sent over HTTP to the taosKeeper service specified by `monitorFqdn` and `monitorProt`.
| Value Range   | 0: monitoring disabled, 1: monitoring enabled                                                                                                                                                                |
| Default   | 1                                                                                                                                                                                                  |

### monitorFqdn

| Attribute     | Description                                                    |
| -------- | -------------------------- |
| Applicable    | Server Only                                                    |
| Meaning     | FQDN of taosKeeper monitoring service |
| Default   | None                                                                                    |

### monitorPort

| Attribute     | Description                            |
| -------- | --------------------------- |
| Applicable    | Server Only                                                    |
| Meaning     | Port of taosKeeper monitoring service |
| Default Value | 6043                                                               |

### monitorInterval

| Attribute     | Description                                                                                                                                                                         |
| -------- | -------------------------------------------- |
| Applicable    | Server Only                                                                                                                                                                         |
| Meaning       | The interval of collecting system workload |
| Unit          | second                                     |
| Value Range | 1-200000                                     |
| Default Value | 30                                                                                                                                                                                   |

### telemetryReporting

| Attribute     | Description                                                                                                                                                                         |
| -------- | ---------------------------------------- |
| Applicable    | Server Only                                                                                                                                                                         |
| Meaning       | Switch for allowing TDengine to collect and report service usage information |
| Value Range   | 0: Not allowed; 1: Allowed                                                   |
| Default Value | 1                                                                                                                                                                                   |

## Query Parameters

### queryPolicy

| Attribute     | Description                          |
| -------- | ----------------------------- |
| Applicable | Client only                                           |
| Meaning     | Execution policy for query statements            |
| Unit     | None                            |
| Default   | 1                             |
| Value Range | 1: Run queries on vnodes and not on qnodes 

2: Run subtasks without scan operators on qnodes and subtasks with scan operators on vnodes.

3: Only run scan operators on vnodes; run all other operators on qnodes. |

### querySmaOptimize

| Attribute     | Description                            |
| -------- | -------------------- |
| Applicable | Client only                                           |
| Meaning  | SMA index optimization policy |
| Unit     | None                            |
| Default Value | 0                                                 |
| Notes |

0: Disable SMA indexing and perform all queries on non-indexed data.

1: Enable SMA indexing and perform queries from suitable statements on precomputation results.|

### countAlwaysReturnValue 

| Attribute     | Description                             |
| -------- | -------------------------------- |
| Applicable | Server only                     |
| Meaning   | count()/hyperloglog() return value or not if the result data is NULL |
| Vlue Range | 0：Return empty line，1：Return 0       |
| Default   | 1                            |

### maxNumOfDistinctRes

| Attribute     | Description                                                                                                                                                                         |
| -------- | -------------------------------- |
| Applicable    | Server Only                                                                                                                                                                         |
| Meaning       | The maximum number of distinct rows returned |
| Value Range   | [100,000 - 100,000,000]                      |
| Default Value | 100,000                                      |

### keepColumnName

| Attribute     | Description                             |
| -------- | -------------------------------- |
| Applicable | Client only                     |
| Meaning     | When the Last, First, LastRow function is queried, whether the returned column name contains the function name. |
| Value Range | 0 means including the function name, 1 means not including the function name.     |
| Default Value   | 0                            |

## Locale Parameters

### timezone

| Attribute     | Description                                                                                                                                                                         |
| -------- | ------------------------------ |
| Applicable    | Server and Client                                                                                                      |
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

The locale definition standard on Linux/macOS is: <Language\>\_<Region\>.<charset\>, for example, in "zh_CN.UTF-8", "zh" means Chinese, "CN" means China mainland, "UTF-8" means charset. The charset indicates how to display the characters. On Linux/macOS, the charset can be set by locale in the system. On Windows system another configuration parameter `charset` must be used to configure charset because the locale used on Windows is not POSIX standard. Of course, `charset` can also be used on Linux/macOS to specify the charset.

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

| Attribute     | Description               |
| -------- | ------------------------------------------ |
| Applicable    | Server Only                                 |
| Meaning       | All data files are stored in this directory |
| Default Value | /var/lib/taos                               |

### tempDir

| Attribute    | Description                                     |
| -------- | ------------------------------------------ |
| Applicable | Server only                               |
| Meaning     | The directory where to put all the temporary files generated during system running |
| Default   | /tmp                           |

### minimalTmpDirGB

| Attribute     | Description                            |
| -------- | ------------------------------------------------ |
| Applicable    | Server and Client         |
| Meaning       | When the available disk space in tmpDir is below this threshold, writing to tmpDir is suspended |
| Unit          | GB                            |
| Default Value | 1.0                                                                   |

### minimalDataDirGB

| Attribute     | Description                            |
| -------- | ------------------------------------------------ |
| Applicable    | Server Only                                                    |
| Meaning       | When the available disk space in dataDir is below this threshold, writing to dataDir is suspended |
| Unit          | GB                            |
| Default Value | 2.0                                                                   |

## Cluster Parameters

### supportVnodes

| Attribute     | Description                            |
| -------- | --------------------------- |
| Applicable    | Server Only                                                    |
| Meaning     | Maximum number of vnodes per dnode |
| Value Range | 0-4096                                     |
| Default Value | 2x the CPU cores                                                    |


## Continuous Query Parameters                                 

### minSlidingTime

| Attribute     | Description                                              |
| ------------- | -------------------------------------------------------- |
| Applicable    | Server Only                                              |
| Meaning       | Minimum sliding time of time window                      |
| Unit          | millisecond or microsecond , depending on time precision |
| Value Range   | 10-1000000                                               |
| Default Value | 10                                                       |

### minIntervalTime

| Attribute     | Description                 |
| ------------- | --------------------------- |
| Applicable    | Server Only                 |
| Meaning       | Minimum size of time window |
| Unit          | millisecond                 |
| Value Range   | 1-1000000                   |
| Default Value | 10                          |

:::info
To prevent system resource from being exhausted by multiple concurrent streams, a random delay is applied on each stream automatically. `maxFirstStreamCompDelay` is the maximum delay time before a continuous query is started the first time. `streamCompDelayRatio` is the ratio for calculating delay time, with the size of the time window as base. `maxStreamCompDelay` is the maximum delay time. The actual delay time is a random time not bigger than `maxStreamCompDelay`. If a continuous query fails, `retryStreamComDelay` is the delay time before retrying it, also not bigger than `maxStreamCompDelay`.

:::

## Log Parameters

### logDir

| Attribute     | Description                            |
| -------- | -------------------------------------------------- |
| Applicable    | Server and Client         |
| Meaning       | The directory for writing log files |
| Default Value | /var/log/taos                       |

### minimalLogDirGB

| Attribute     | Description                                                                                                                                                                         |
| -------- | -------------------------------------------- |
| Applicable    | Server and Client         |
| Meaning       | When the available disk space in logDir is below this threshold, writing to log files is suspended |
| Unit          | GB                            |
| Default Value | 1.0                              |

### numOfLogLines

| Attribute     | Description                            |
| -------- | ---------------------------- |
| Applicable    | Server and Client         |
| Meaning       | Maximum number of lines in single log file |
| Default Value | 10000000                            |

### asyncLog

| Attribute     | Description                            |
| -------- | -------------------- |
| Applicable    | Server and Client         |
| Meaning       | The mode of writing log file |
| Value Range   | 0: sync way; 1: async way    |
| Default Value | 1                                                 |

### logKeepDays

| Attribute     | Description                            |
| -------- | ----------------------------------------------------------------------------------- |
| Applicable    | Server and Client         |
| Meaning       | The number of days for log files to be kept                                                                                                 |
| Unit          | day                                                   |
| Default Value | 0                                                               |
| Note          | When it's bigger than 0, the log file would be renamed to "taosdlog.xxx" in which "xxx" is the timestamp when the file is changed last time |

### debugFlag

| Attribute     | Description                            |
| -------- | ------------------------------------------------------------------------------------------------- |
| Applicable    | Server and Client         |
| Meaning       | Log level                                                 |
| Value Range   | 131: INFO/WARNING/ERROR; 135: plus DEBUG; 143: plus TRACE |
| Default Value | 131 or 135, depending on the module                       |

### tmrDebugFlag

| Attribute     | Description                            |
| -------- | -------------------- |
| Applicable    | Server and Client         |
| Meaning       | Log level of timer module |
| Value Range   | same as debugFlag  |
| Default Value | |

### uDebugFlag

| Attribute     | Description                            |
| -------- | ---------------------- |
| Applicable    | Server and Client         |
| Meaning       | Log level of common module |
| Value Range   | same as debugFlag  |
| Default Value | |

### rpcDebugFlag

| Attribute     | Description                            |
| -------- | -------------------- |
| Applicable    | Server and Client         |
| Meaning       | Log level of rpc module |
| Value Range   | same as debugFlag  |
| Default Value | |

### jniDebugFlag

| Attribute     | Description                            |
| -------- | ------------------ |
| Applicable    | Client Only         |
| Meaning       | Log level of jni module |
| Value Range   | same as debugFlag  |
| Default Value | |

### qDebugFlag

| Attribute     | Description                            |
| -------- | -------------------- |
| Applicable    | Server and Client         |
| Meaning     | Log level of query module |
| Value Range   | same as debugFlag  |
| Default Value | |

### cDebugFlag

| Attribute     | Description                            |
| -------- | --------------------- |
| Applicable    | Client Only         |
| Meaning       | Log level of Client |
| Value Range   | same as debugFlag  |
| Default Value | |

### dDebugFlag

| Attribute     | Description                            |
| -------- | -------------------- |
| Applicable    | Server Only                                                    |
| Meaning       | Log level of dnode |
| Value Range   | same as debugFlag  |
| Default Value | 135                              |

### vDebugFlag

| Attribute     | Description                            |
| -------- | -------------------- |
| Applicable    | Server Only                                                    |
| Meaning       | Log level of vnode |
| Value Range   | same as debugFlag  |
| Default Value | |

### mDebugFlag

| Attribute     | Description                            |
| -------- | -------------------- |
| Applicable    | Server Only                                                    |
| Meaning     | Log level of mnode module |
| Value Range   | same as debugFlag  |
| Default Value | 135                              |

### wDebugFlag

| Attribute     | Description                            |
| -------- | ------------------ |
| Applicable    | Server Only                                                    |
| Meaning     | Log level of WAL module |
| Value Range   | same as debugFlag  |
| Default Value | 135                          |

### sDebugFlag

| Attribute     | Description                            |
| -------- | -------------------- |
| Applicable    | Server and Client         |
| Meaning       | Log level of sync module |
| Value Range   | same as debugFlag  |
| Default Value | 135                              |

### tsdbDebugFlag

| Attribute     | Description                            |
| -------- | ------------------- |
| Applicable    | Server Only                                                    |
| Meaning     | Log level of TSDB module |
| Value Range   | same as debugFlag  |
| Default Value | |

### tqDebugFlag

| Attribute     | Description                          |
| -------- | ----------------- |
| Applicable | Server only                                           |
| Meaning     | Log level of TQ module |
| Value Range   | same as debugFlag  |
| Default Value | |

### fsDebugFlag

| Attribute     | Description                          |
| -------- | ----------------- |
| Applicable | Server only                                           |
| Meaning     | Log level of FS module |
| Value Range   | same as debugFlag  |
| Default Value | |

### udfDebugFlag

| Attribute     | Description                            |
| -------- | ------------------ |
| Applicable    | Server Only                                                    |
| Meaning       | Log level of UDF module |
| Value Range   | same as debugFlag  |
| Default Value | |

### smaDebugFlag

| Attribute     | Description                            |
| -------- | ------------------ |
| Applicable    | Server Only                                                    |
| Meaning     | Log level of SMA module |
| Value Range   | same as debugFlag  |
| Default Value | |

### idxDebugFlag

| Attribute     | Description                            |
| -------- | -------------------- |
| Applicable    | Server Only                                                    |
| Meaning     | Log level of index module |
| Value Range   | same as debugFlag  |
| Default Value | |

### tdbDebugFlag

| Attribute     | Description                            |
| -------- | ------------------ |
| Applicable    | Server Only                                                    |
| Meaning     | Log level of TDB module |
| Value Range   | same as debugFlag  |
| Default Value | |

## Schemaless Parameters

### smlChildTableName

| Attribute     | Description                      |
| -------- | ------------------------- |
| Applicable | Client only                                           |
| Meaning     | Custom subtable name for schemaless writes |
| Type     | String                    |
| Default Value   | None                        |

### smlTagName

| Attribute     | Description                            |
| -------- | ------------------------------------ |
| Applicable | Client only                                           |
| Meaning     | Default tag for schemaless writes without tag value specified |
| Type     | String                    |
| Default Value   | _tag_null                                 |

### smlDataFormat

| Attribute     | Description                          |
| -------- | ----------------------------- |
| Applicable | Client only                                           |
| Meaning     | Whether schemaless columns are consistently ordered |
| Value Range     | 0: not consistent; 1: consistent.            |
| Default   | 1                             |

## Other Parameters

### enableCoreFile

| Attribute     | Description                                                                                         |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| Applicable    | Server and Client                                                                                                                                                       |
| Meaning       | Whether to generate core file when server crashes                                                                                                                       |
| Value Range   | 0: false, 1: true                                                                                                                                                       |
| Default Value | 1                                                                                                                                                                       |
| Note          | The core file is generated under root directory `systemctl start taosd`/`launchctl start com.tdengine.taosd` is used to start, or under the working directory if `taosd` is started directly on Linux/macOS Shell. |

### udf

| Attribute     | Description                            |
| -------- | ------------------ |
| Applicable    | Server Only                                                    |
| Meaning     | Whether the UDF service is enabled  |
| Value Range   | 0: disable UDF; 1: enabled UDF |
| Default Value | 1                              |


## 3.0 Parameters

| #   |        **参数**         | **适用于 2.X ** | **适用于 3.0 ** | 3.0 版本的当前行为                                |
| --- | :---------------------: | --------------- | --------------- | ------------------------------------------------- |
| 1   |         firstEp         | 是              | 是              |                                                   |
| 2   |        secondEp         | 是              | 是              |                                                   |
| 3   |          fqdn           | 是              | 是              |                                                   |
| 4   |       serverPort        | 是              | 是              |                                                   |
| 5   |      maxShellConns      | 是              | 是              |                                                   |
| 6   |         monitor         | 是              | 是              |                                                   |
| 7   |       monitorFqdn       | 否              | 是              |                                                   |
| 8   |       monitorPort       | 否              | 是              |                                                   |
| 9   |     monitorInterval     | 是              | 是              |                                                   |
| 10  |       queryPolicy       | 否              | 是              |                                                   |
| 11  |    querySmaOptimize     | 否              | 是              |                                                   |
| 12  |   maxNumOfDistinctRes   | 是              | 是              |                                                   |
| 13  |     minSlidingTime      | 是              | 是              |                                                   |
| 14  |     minIntervalTime     | 是              | 是              |                                                   |
| 15  | countAlwaysReturnValue  | 是              | 是              |                                                   |
| 16  |         dataDir         | 是              | 是              |                                                   |
| 17  |    minimalDataDirGB     | 是              | 是              |                                                   |
| 18  |      supportVnodes      | 否              | 是              |                                                   |
| 19  |         tempDir         | 是              | 是              |                                                   |
| 20  |     minimalTmpDirGB     | 是              | 是              |                                                   |
| 21  |    smlChildTableName    | 是              | 是              |                                                   |
| 22  |       smlTagName        | 是              | 是              |                                                   |
| 23  |      smlDataFormat      | 否              | 是              |                                                   |
| 24  |     statusInterval      | 是              | 是              |                                                   |
| 25  |         logDir          | 是              | 是              |                                                   |
| 26  |     minimalLogDirGB     | 是              | 是              |                                                   |
| 27  |      numOfLogLines      | 是              | 是              |                                                   |
| 28  |        asyncLog         | 是              | 是              |                                                   |
| 29  |       logKeepDays       | 是              | 是              |                                                   |
| 30  |        debugFlag        | 是              | 是              |                                                   |
| 31  |      tmrDebugFlag       | 是              | 是              |                                                   |
| 32  |       uDebugFlag        | 是              | 是              |                                                   |
| 33  |      rpcDebugFlag       | 是              | 是              |                                                   |
| 34  |      jniDebugFlag       | 是              | 是              |                                                   |
| 35  |       qDebugFlag        | 是              | 是              |                                                   |
| 36  |       cDebugFlag        | 是              | 是              |                                                   |
| 37  |       dDebugFlag        | 是              | 是              |                                                   |
| 38  |       vDebugFlag        | 是              | 是              |                                                   |
| 39  |       mDebugFlag        | 是              | 是              |                                                   |
| 40  |       wDebugFlag        | 是              | 是              |                                                   |
| 41  |       sDebugFlag        | 是              | 是              |                                                   |
| 42  |      tsdbDebugFlag      | 是              | 是              |                                                   |
| 43  |       tqDebugFlag       | 否              | 是              |                                                   |
| 44  |       fsDebugFlag       | 是              | 是              |                                                   |
| 45  |      udfDebugFlag       | 否              | 是              |                                                   |
| 46  |      smaDebugFlag       | 否              | 是              |                                                   |
| 47  |      idxDebugFlag       | 否              | 是              |                                                   |
| 48  |      tdbDebugFlag       | 否              | 是              |                                                   |
| 49  |      metaDebugFlag      | 否              | 是              |                                                   |
| 50  |        timezone         | 是              | 是              |                                                   |
| 51  |         locale          | 是              | 是              |                                                   |
| 52  |         charset         | 是              | 是              |                                                   |
| 53  |           udf           | 是              | 是              |                                                   |
| 54  |     enableCoreFile      | 是              | 是              |                                                   |
