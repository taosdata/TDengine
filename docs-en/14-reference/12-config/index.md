---
sidebar_label: Configuration
title: Configuration Parameters
description: "Configuration parameters for client and server in TDengine"
---

In this chapter, all the configuration parameters on both server and client side are described thoroughly.

## Configuration File on Server Side

On the server side, the actual service of TDengine is provided by an executable `taosd` whose parameters can be configured in file `taos.cfg` to meet the requirements of different use cases. The default location of `taos.cfg` is `/etc/taos`, but can be changed by using `-c` parameter on the CLI of `taosd`. For example, the configuration file can be put under `/home/user` and used like below

```bash
taosd -c /home/user
```

Parameter `-C` can be used on the CLI of `taosd` to show its configuration, like below:

```
taosd -C
```

## Configuration File on Client Side

TDengine CLI `taos` is the tool for users to interact with TDengine. It can share same configuration file as `taosd` or use a separate configuration file. When launching `taos`, parameter `-c` can be used to specify the location where its configuration file is. For example `taos -c /home/cfg` means `/home/cfg/taos.cfg` will be used. If `-c` is not used, the default location of the configuration file is `/etc/taos`. For more details please use `taos --help` to get.

From version 2.0.10.0 below commands can be used to show the configuration parameters of the client side.

```bash
taos -C
```

```bash
taos --dump-config
```

# Configuration Parameters

:::note
`taosd` needs to be restarted for the parameters changed in the configuration file to take effect.

:::

## Connection Parameters

### firstEp

| Attribute     | Description                                                                                          |
| ------------- | ---------------------------------------------------------------------------------------------------- |
| Applicable    | Server and Client                                                                                    |
| Meaning       | The end point of the first dnode in the cluster to be connected to when `taosd` or `taos` is started |
| Default Value | localhost:6030                                                                                       |

### secondEp

| Attribute     | Description                                                                                                            |
| ------------- | ---------------------------------------------------------------------------------------------------------------------- |
| Applicable    | Server and Client                                                                                                      |
| Meaning       | The end point of the second dnode to be connected to if the firstEp is not available when `taosd` or `taos` is started |
| Default Value | None                                                                                                                   |

### fqdn

| Attribute     | Description                                                              |
| ------------- | ------------------------------------------------------------------------ |
| Applicable    | Server Only                                                              |
| Meaning       | The FQDN of the host where `taosd` will be started. It can be IP address |
| Default Value | The first hostname configured for the hos                                |
| Note          | It should be within 96 bytes                                             |

### serverPort

| Attribute     | Description                                                                                                                     |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| Applicable    | Server Only                                                                                                                     |
| Meaning       | The port for external access after `taosd` is started                                                                           |
| Default Value | 6030                                                                                                                            |
| Note          | REST service is provided by `taosd` before 2.4.0.0 but by `taosAdapter` after 2.4.0.0, the default port of REST service is 6041 |

:::note
TDengine uses continuous 13 ports, both TCP and TCP, from the port specified by `serverPort`. These ports need to be kept as open if firewall is enabled. Below table describes the ports used by TDengine in details.

:::

| Protocol | Default Port | Description                                      | How to configure                                                                               |
| :------- | :----------- | :----------------------------------------------- | :--------------------------------------------------------------------------------------------- |
| TCP      | 6030         | Communication between client and server          | serverPort                                                                                     |
| TCP      | 6035         | Communication among server nodes in cluster      | serverPort+5                                                                                   |
| TCP      | 6040         | Data syncup among server nodes in cluster        | serverPort+10                                                                                  |
| TCP      | 6041         | REST connection between client and server        | Prior to 2.4.0.0: serverPort+11; After 2.4.0.0 refer to [taosAdapter](/reference/taosadapter/) |
| TCP      | 6042         | Service Port of Arbitrator                       | The parameter of Arbitrator                                                                    |
| TCP      | 6043         | Service Port of TaosKeeper                       | The parameter of TaosKeeper                                                                    |
| TCP      | 6044         | Data access port for StatsD                      | refer to [taosAdapter](/reference/taosadapter/)                                                 |
| UDP      | 6045         | Data access for statsd                           | refer to [taosAdapter](/reference/taosadapter/)                                                 |
| TCP      | 6060         | Port of Monitoring Service in Enterprise version |                                                                                                |
| UDP      | 6030-6034    | Communication between client and server          | serverPort                                                                                     |
| UDP      | 6035-6039    | Communication among server nodes in cluster      | serverPort                                                                                     |

### maxShellConns

| Attribute     | Description                                          |
| ------------- | ---------------------------------------------------- |
| Applicable    | Server Only                                          |
| Meaning       | The maximum number of connections a dnode can accept |
| Value Range   | 10-50000000                                          |
| Default Value | 5000                                                 |

### maxConnections

| Attribute     | Description                                                                   |
| ------------- | ----------------------------------------------------------------------------- |
| Applicable    | Server Only                                                                   |
| Meaning       | The maximum number of connections allowed by a database                       |
| Value Range   | 1-100000                                                                      |
| Default Value | 5000                                                                          |
| Note          | The maximum number of worker threads on the client side is maxConnections/100 |

### rpcForceTcp

| Attribute     | Description                                                         |
| ------------- | ------------------------------------------------------------------- |
| Applicable    | Server and Client                                                   |
| Meaning       | TCP is used by force                                                |
| Value Range   | 0: disabled 1: enabled                                              |
| Default Value | 0                                                                   |
| Note          | It's suggested to configure to enable if network is not good enough |

## Monitoring Parameters

### monitor

| Attribute     | Description                                                                                                                                                                         |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Applicable    | Server Only                                                                                                                                                                         |
| Meaning       | The switch for monitoring inside server. The workload of the hosts, including CPU, memory, disk, network, TTP requests, are collected and stored in a system builtin database `LOG` |
| Value Range   | 0: monitoring disabled, 1: monitoring enabled                                                                                                                                       |
| Default Value | 0                                                                                                                                                                                   |

### monitorInterval

| Attribute     | Description                                |
| ------------- | ------------------------------------------ |
| Applicable    | Server Only                                |
| Meaning       | The interval of collecting system workload |
| Unit          | second                                     |
| Value Range   | 1-600                                      |
| Default Value | 30                                         |

### telemetryReporting

| Attribute     | Description                                                                  |
| ------------- | ---------------------------------------------------------------------------- |
| Applicable    | Server Only                                                                  |
| Meaning       | Switch for allowing TDengine to collect and report service usage information |
| Value Range   | 0: Not allowed; 1: Allowed                                                   |
| Default Value | 1                                                                            |

## Query Parameters

### queryBufferSize

| Attribute     | Description                                                                              |
| ------------- | ---------------------------------------------------------------------------------------- |
| Applicable    | Server Only                                                                              |
| Meaning       | The total memory size reserved for all queries                                           |
| Unit          | MB                                                                                       |
| Default Value | None                                                                                     |
| Note          | It can be estimated by "maximum number of concurrent queries" _ "number of tables" _ 170 |

### ratioOfQueryCores

| Attribute     | Description                                                                                           |
| ------------- | ----------------------------------------------------------------------------------------------------- |
| Applicable    | Server Only                                                                                           |
| Meaning       | Maximum number of query threads                                                                       |
| Default Value | 1                                                                                                     |
| Note          | value range: float number between [0, 2] 0: only 1 query thread; >0: the times of the number of cores |

### maxNumOfDistinctRes

| Attribute     | Description                                  |
| ------------- | -------------------------------------------- |
| Applicable    | Server Only                                  |
| Meaning       | The maximum number of distinct rows returned |
| Value Range   | [100,000 - 100, 000, 000]                    |
| Default Value | 100, 000                                     |
| Note          | After version 2.3.0.0                        |

## Locale Parameters

### timezone

| Attribute     | Description                     |
| ------------- | ------------------------------- |
| Applicable    | Server and Client               |
| Meaning       | TimeZone                        |
| Default Value | TimeZone configured in the host |

:::info
To handle the data insertion and data query from multiple timezones, Unix Timestamp is used and stored TDengine. The timestamp generated from any timezones at same time is same in Unix timestamp. To make sure the time on client side can be converted to Unix timestamp correctly, the timezone must be set properly.

On Linux system, TDengine clients automatically obtain timezone from the host. Alternatively, the timezone can be configured explicitly in configuration file `taos.cfg` like below.

```
timezone UTC-7
timezone GMT-8
timezone Asia/Shanghai
```

The above examples are all proper configuration for the timezone of UTC+8. On Windows system, however, `timezone Asia/Shanghai` is not supported, it must be set as `timezone UTC-8`.

The setting for timezone impacts the strings not in Unix timestamp, keywords or functions related to date/time, for example

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

To avoid the problems of using time strings, Unix timestamp can be used directly. Furthermore, time strings with timezone can be used in SQL statement, for example "2013-04-12T15:52:01.123+08:00" in RFC3339 format or "2013-04-12T15:52:01.123+0800" in ISO-8601 format, they are not influenced by timezone setting when converted to Unix timestamp.

:::

### locale

| Attribute     | Description               |
| ------------- | ------------------------- |
| Applicable    | Server and Client         |
| Meaning       | Location code             |
| Default Value | Locale configured in host |

:::info
A specific type "nchar" is provided in TDengine to store non-ASCII characters such as Chinese, Japanese, Korean. The characters to be stored in nchar type are firstly encoded in UCS4-LE before sending to server side. To store non-ASCII characters correctly, the encoding format of the client side needs to be set properly.

The characters input on the client side are encoded using the default system encoding, which is UTF-8 on Linux, or GB18030 or GBK on some systems in Chinese, POSIX in docker, CP936 on Windows in Chinese. The encoding of the operating system in use must be set correctly so that the characters in nchar type can be converted to UCS4-LE.

The locale definition standard on Linux is: <Language\>\_<Region\>.<charset\>, for example, in "zh_CN.UTF-8", "zh" means Chinese, "CN" means China mainland, "UTF-8" means charset. On Linux andMac OSX, the charset can be set by locale in the system. On Windows system another configuration parameter `charset` must be used to configure charset because the locale used on Windows is not POSIX standard. Of course, `charset` can also be used on Linux to specify the charset.

:::

### charset

| Attribute     | Description               |
| ------------- | ------------------------- |
| Applicable    | Server and Client         |
| Meaning       | Character                 |
| Default Value | charset set in the system |

:::info
On Linux, if `charset` is not set in `taos.cfg`, when `taos` is started, the charset is obtained from system locale. If obtaining charset from system locale fails, `taos` would fail to start. So on Linux system, if system locale is set properly, it's not necessary to set `charset` in `taos.cfg`. For example:

```
locale zh_CN.UTF-8
```

Besides, on Linux system, if the charset contained in `locale` is not consistent with that set by `charset`, the one who comes later in the configuration file is used.

```title="Effective charset is GBK"
locale zh_CN.UTF-8
charset GBK
```

```title="Effective charset is UTF-8"
charset GBK
locale zh_CN.UTF-8
```

On Windows system, it's not possible to obtain charset from system locale. If it's not set in configuration file `taos.cfg`, it would be default to CP936, same as set as below in `taos.cfg`. For example

```
charset CP936
```

:::

## Storage Parameters

### dataDir

| Attribute     | Description                                 |
| ------------- | ------------------------------------------- |
| Applicable    | Server Only                                 |
| Meaning       | All data files are stored in this directory |
| Default Value | /var/lib/taos                               |

### cache

| Attribute     | Description                   |
| ------------- | ----------------------------- |
| Applicable    | Server Only                   |
| Meaning       | The size of each memory block |
| Unit          | MB                            |
| Default Value | 16                            |

### blocks

| Attribute     | Description                                                    |
| ------------- | -------------------------------------------------------------- |
| Applicable    | Server Only                                                    |
| Meaning       | The number of memory blocks of size `cache` used by each vnode |
| Default Value | 6                                                              |

### days

| Attribute     | Description                                           |
| ------------- | ----------------------------------------------------- |
| Applicable    | Server Only                                           |
| Meaning       | The time range of the data stored in single data file |
| Unit          | day                                                   |
| Default Value | 10                                                    |

### keep

| Attribute     | Description                            |
| ------------- | -------------------------------------- |
| Applicable    | Server Only                            |
| Meaning       | The number of days for data to be kept |
| Unit          | day                                    |
| Default Value | 3650                                   |

### minRows

| Attribute     | Description                                |
| ------------- | ------------------------------------------ |
| Applicable    | Server Only                                |
| Meaning       | minimum number of rows in single data file |
| Default Value | 100                                        |

### maxRows

| Attribute     | Description                                |
| ------------- | ------------------------------------------ |
| Applicable    | Server Only                                |
| Meaning       | maximum number of rows in single data file |
| Default Value | 4096                                       |

### walLevel

| Attribute     | Description                                                                        |
| ------------- | ---------------------------------------------------------------------------------- |
| Applicable    | Server Only                                                                        |
| Meaning       | WAL level                                                                          |
| Value Range   | 0: wal disabled <br/> 1: wal enabled without fsync <br/> 2: wal enabled with fsync |
| Default Value | 1                                                                                  |

### fsync

| Attribute     | Description                                                                                                           |
| ------------- | --------------------------------------------------------------------------------------------------------------------- |
| Applicable    | Server Only                                                                                                           |
| Meaning       | The waiting time for invoking fsync when walLevel is 2                                                                |
| Unit          | millisecond                                                                                                           |
| Value Range   | 0: no waiting time, fsync is performed immediately once WAL is written; <br/> maximum value is 180000, i.e. 3 minutes |
| Default Value | 3000                                                                                                                  |

### update

| Attribute     | Description                                                                                            |
| ------------- | ------------------------------------------------------------------------------------------------------ |
| Applicable    | Server Only                                                                                            |
| Meaning       | If it's allowed to update existing data                                                                |
| Value Range   | 0: not allowed <br/> 1: a row can only be updated as a whole <br/> 2: a part of columns can be updated |
| Default Value | 0                                                                                                      |
| Note          | Not available from version 2.0.8.0                                                                     |

### cacheLast

| Attribute     | Description                                                                                                                                                          |
| ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Applicable    | Server Only                                                                                                                                                          |
| Meaning       | Whether to cache the latest rows of each sub table in memory                                                                                                         |
| Value Range   | 0: not cached <br/> 1: the last row of each sub table is cached <br/> 2: the last non-null value of each column is cached <br/> 3: identical to both 1 and 2 are set |
| Default Value | 0                                                                                                                                                                    |

### minimalTmpDirGB

| Attribute     | Description                                                                                     |
| ------------- | ----------------------------------------------------------------------------------------------- |
| Applicable    | Server and Client                                                                               |
| Meaning       | When the available disk space in tmpDir is below this threshold, writing to tmpDir is suspended |
| Unit          | GB                                                                                              |
| Default Value | 1.0                                                                                             |

### minimalDataDirGB

| Attribute     | Description                                                                                      |
| ------------- | ------------------------------------------------------------------------------------------------ |
| Applicable    | Server Only                                                                                      |
| Meaning       | hen the available disk space in dataDir is below this threshold, writing to dataDir is suspended |
| Unit          | GB                                                                                               |
| Default Value | 2.0                                                                                              |

### vnodeBak

| Attribute     | Description                                                                 |
| ------------- | --------------------------------------------------------------------------- |
| Applicable    | Server Only                                                                 |
| Meaning       | Whether to backup the corresponding vnode directory when a vnode is deleted |
| Value Range   | 0: not backed up, 1: backup                                                 |
| Default Value | 1                                                                           |

## Cluster Parameters

### numOfMnodes

| Attribute     | Description                    |
| ------------- | ------------------------------ |
| Applicable    | Server Only                    |
| Meaning       | The number of management nodes |
| Default Value | 3                              |

### replica

| Attribute     | Description                |
| ------------- | -------------------------- |
| Applicable    | Server Only                |
| Meaning       | The number of replications |
| Value Range   | 1-3                        |
| Default Value | 1                          |

### quorum

| Attribute     | Description                                                                                |
| ------------- | ------------------------------------------------------------------------------------------ |
| Applicable    | Server Only                                                                                |
| Meaning       | The number of required confirmations for data replication in case of multiple replications |
| Value Range   | 1,2                                                                                        |
| Default Value | 1                                                                                          |

### role

| Attribute     | Description                                                     |
| ------------- | --------------------------------------------------------------- |
| Applicable    | Server Only                                                     |
| Meaning       | The role of the dnode                                           |
| Value Range   | 0: both mnode and vnode <br/> 1: mnode only <br/> 2: dnode only |
| Default Value | 0                                                               |

### balance

| Attribute     | Description              |
| ------------- | ------------------------ |
| Applicable    | Server Only              |
| Meaning       | Automatic load balancing |
| Value Range   | 0: disabled, 1: enabled  |
| Default Value | 1                        |

### balanceInterval

| Attribute     | Description                                     |
| ------------- | ----------------------------------------------- |
| Applicable    | Server Only                                     |
| Meaning       | The interval for checking load balance by mnode |
| Unit          | second                                          |
| Value Range   | 1-30000                                         |
| Default Value | 300                                             |

### arbitrator

| Attribute     | Description                                        |
| ------------- | -------------------------------------------------- |
| Applicable    | Server Only                                        |
| Meaning       | End point of arbitrator, format is same as firstEp |
| Default Value | None                                               |

## Time Parameters

### precision

| Attribute     | Description                                       |
| ------------- | ------------------------------------------------- |
| Applicable    | Server only                                       |
| Meaning       | Time precision used for each database             |
| Value Range   | ms: millisecond; us: microsecond ; ns: nanosecond |
| Default Value | ms                                                |

### rpcTimer

| Attribute     | Description        |
| ------------- | ------------------ |
| Applicable    | Server and Client  |
| Meaning       | rpc retry interval |
| Unit          | milliseconds       |
| Value Range   | 100-3000           |
| Default Value | 300                |

### rpcMaxTime

| Attribute     | Description                        |
| ------------- | ---------------------------------- |
| Applicable    | Server and Client                  |
| Meaning       | maximum wait time for rpc response |
| Unit          | second                             |
| Value Range   | 100-7200                           |
| Default Value | 600                                |

### statusInterval

| Attribute     | Description                                     |
| ------------- | ----------------------------------------------- |
| Applicable    | Server Only                                     |
| Meaning       | the interval of dnode reporting status to mnode |
| Unit          | second                                          |
| Value Range   | 1-10                                            |
| Default Value | 1                                               |

### shellActivityTimer

| Attribute     | Description                                            |
| ------------- | ------------------------------------------------------ |
| Applicable    | Server and Client                                      |
| Meaning       | The interval for taos shell to send heartbeat to mnode |
| Unit          | second                                                 |
| Value Range   | 1-120                                                  |
| Default Value | 3                                                      |

### tableMetaKeepTimer

| Attribute     | Description                                                                                        |
| ------------- | -------------------------------------------------------------------------------------------------- |
| Applicable    | Server Only                                                                                        |
| Meaning       | The expiration time for metadata in cache, once it's reached the client would refresh the metadata |
| Unit          | second                                                                                             |
| Value Range   | 1-8640000                                                                                          |
| Default Value | 7200                                                                                               |

### maxTmrCtrl

| Attribute     | Description              |
| ------------- | ------------------------ |
| Applicable    | Server and Client        |
| Meaning       | Maximum number of timers |
| Unit          | None                     |
| Value Range   | 8-2048                   |
| Default Value | 512                      |

### offlineThreshold

| Attribute     | Description                                                                                                                   |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| Applicable    | Server Only                                                                                                                   |
| Meaning       | The expiration time for dnode online status, once it's reached before receiving status from a node, the dnode becomes offline |
| Unit          | second                                                                                                                        |
| Value Range   | 5-7200000                                                                                                                     |
| Default Value | 86400\*10 (i.e. 10 days)                                                                                                      |

## Performance Optimization Parameters

### numOfThreadsPerCore

| Attribute     | Description                                 |
| ------------- | ------------------------------------------- |
| Applicable    | Server and Client                           |
| Meaning       | The number of consumer threads per CPU core |
| Default Value | 1.0                                         |

### ratioOfQueryThreads

| Attribute     | Description                                                                                   |
| ------------- | --------------------------------------------------------------------------------------------- |
| Applicable    | Server Only                                                                                   |
| Meaning       | Maximum number of query threads                                                               |
| Value Range   | 0: Only one query thread <br/> 1: Same as number of CPU cores <br/> 2: two times of CPU cores |
| Default Value | 1                                                                                             |
| Note          | This value can be a float number, 0.5 means half of the CPU cores                             |

### maxVgroupsPerDb

| Attribute     | Description                          |
| ------------- | ------------------------------------ |
| Applicable    | Server Only                          |
| Meaning       | Maximum number of vnodes for each DB |
| Value Range   | 0-8192                               |
| Default Value |                                      |

### maxTablesPerVnode

| Attribute     | Description                            |
| ------------- | -------------------------------------- |
| Applicable    | Server Only                            |
| Meaning       | Maximum number of tables in each vnode |
| Default Value | 1000000                                |

### minTablesPerVnode

| Attribute     | Description                            |
| ------------- | -------------------------------------- |
| Applicable    | Server Only                            |
| Meaning       | Minimum number of tables in each vnode |
| Default Value | 1000                                   |

### tableIncStepPerVnode

| Attribute     | Description                                                                                 |
| ------------- | ------------------------------------------------------------------------------------------- |
| Applicable    | Server Only                                                                                 |
| Meaning       | When minTablesPerVnode is reached, the number of tables are allocated for a vnode each time |
| Default Value | 1000                                                                                        |

### maxNumOfOrderedRes

| Attribute     | Description                                 |
| ------------- | ------------------------------------------- |
| Applicable    | Server and Client                           |
| Meaning       | Maximum number of rows ordered for a STable |
| Default Value | 100,000                                     |

### mnodeEqualVnodeNum

| Attribute     | Description                                                                                     |
| ------------- | ----------------------------------------------------------------------------------------------- |
| Applicable    | Server Only                                                                                     |
| Meaning       | The number of vnodes whose system resources consumption are considered as equal to single mnode |
| Default Value | 4                                                                                               |

### numOfCommitThreads

| Attribute     | Description                               |
| ------------- | ----------------------------------------- |
| Applicable    | Server Only                               |
| Meaning       | Maximum of threads for committing to disk |
| Default Value |                                           |

## Compression Parameters

### comp

| Attribute     | Description                                                         |
| ------------- | ------------------------------------------------------------------- |
| Applicable    | Server Only                                                         |
| Meaning       | Whether data is compressed                                          |
| Value Range   | 0: uncompressed, 1: One phase compression, 2: Two phase compression |
| Default Value | 2                                                                   |

### tsdbMetaCompactRatio

| Attribute     | Description                                                                                 |
| ------------- | ------------------------------------------------------------------------------------------- |
| Meaning       | The threshold for percentage of redundant in meta file to trigger compression for meta file |
| Value Range   | 0: no compression forever, [1-100]: The threshold percentage                                |
| Default Value | 0                                                                                           |

### compressMsgSize

| Attribute     | Description                                                                      |
| ------------- | -------------------------------------------------------------------------------- |
| Applicable    | Server Only                                                                      |
| Meaning       | The threshold for message size to compress the message..                         |
| Unit          | bytes                                                                            |
| Value Range   | 0: already compress; >0: compress when message exceeds it; -1: always uncompress |
| Default Value | -1                                                                               |

### compressColData

| Attribute     | Description                                                                                                         |
| ------------- | ------------------------------------------------------------------------------------------------------------------- |
| Applicable    | Server Only                                                                                                         |
| Meaning       | The threshold for size of column data to trigger compression for the query result                                   |
| Unit          | bytes                                                                                                               |
| Value Range   | 0: always compress; >0: only compress when the size of any column data exceeds the threshold; -1: always uncompress |
| Default Value | -1                                                                                                                  |
| Note          | available from version 2.3.0.0                                                                                      |

### lossyColumns

| Attribute     | Description                                                                                                                                 |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| Applicable    | Server Only                                                                                                                                 |
| Meaning       | The floating number types for lossy compression                                                                                             |
| Value Range   | "": lossy compression is disabled <br/> float: only for float <br/>double: only for double <br/> float \| double: for both float and double |
| Default Value | "" , i.e. disabled                                                                                                                          |

### fPrecision

| Attribute     | Description                                                 |
| ------------- | ----------------------------------------------------------- |
| Applicable    | Server Only                                                 |
| Meaning       | Compression precision for float type                        |
| Value Range   | 0.1 ~ 0.00000001                                            |
| Default Value | 0.00000001                                                  |
| Note          | The fractional part lower than this value will be discarded |

### dPrecision

| Attribute     | Description                                                 |
| ------------- | ----------------------------------------------------------- |
| Applicable    | Server Only                                                 |
| Meaning       | Compression precision for double type                       |
| Value Range   | 0.1 ~ 0.0000000000000001                                    |
| Default Value | 0.0000000000000001                                          |
| Note          | The fractional part lower than this value will be discarded |

## Continuous Query Parameters

### stream

| Attribute     | Description                        |
| ------------- | ---------------------------------- |
| Applicable    | Server Only                        |
| Meaning       | Whether to enable continuous query |
| Value Range   | 0: disabled <br/> 1: enabled       |
| Default Value | 1                                  |

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

### maxStreamCompDelay

| Attribute     | Description                                      |
| ------------- | ------------------------------------------------ |
| Applicable    | Server Only                                      |
| Meaning       | Maximum delay before starting a continuous query |
| Unit          | millisecond                                      |
| Value Range   | 10-1000000000                                    |
| Default Value | 20000                                            |

### maxFirstStreamCompDelay

| Attribute     | Description                                                          |
| ------------- | -------------------------------------------------------------------- |
| Applicable    | Server Only                                                          |
| Meaning       | Maximum delay time before starting a continuous query the first time |
| Unit          | millisecond                                                          |
| Value Range   | 10-1000000000                                                        |
| Default Value | 10000                                                                |

### retryStreamCompDelay

| Attribute     | Description                                   |
| ------------- | --------------------------------------------- |
| Applicable    | Server Only                                   |
| Meaning       | Delay time before retrying a continuous query |
| Unit          | millisecond                                   |
| Value Range   | 10-1000000000                                 |
| Default Value | 10                                            |

### streamCompDelayRatio

| Attribute     | Description                                                              |
| ------------- | ------------------------------------------------------------------------ |
| Applicable    | Server Only                                                              |
| Meaning       | The delay ratio, with time window size as the base, for continuous query |
| Value Range   | 0.1-0.9                                                                  |
| Default Value | 0.1                                                                      |

:::info
To prevent system resource from being exhausted by multiple concurrent streams, a random delay is applied on each stream automatically. `maxFirstStreamCompDelay` is the maximum delay time before a continuous query is started the first time. `streamCompDelayRatio` is the ratio for calculating delay time, with the size of the time window as base. `maxStreamCompDelay` is the maximum delay time. The actual delay time is a random time not bigger than `maxStreamCompDelay`. If a continuous query fails, `retryStreamComDelay` is the delay time before retrying it, also not bigger than `maxStreamCompDelay`.

:::

## HTTP Parameters

:::note
HTTP server had been provided by `taosd` prior to version 2.4.0.0, now is provided by `taosAdapter` after version 2.4.0.0.
The parameters described in this section are only application in versions prior to 2.4.0.0. If you are using any version from 2.4.0.0, please refer to [taosAdapter]](/reference/taosadapter/).

:::

### http

| Attribute     | Description                    |
| ------------- | ------------------------------ |
| Applicable    | Server Only                    |
| Meaning       | Whether to enable http service |
| Value Range   | 0: disabled, 1: enabled        |
| Default Value | 1                              |

### httpEnableRecordSql

| Attribute     | Description                                                               |
| ------------- | ------------------------------------------------------------------------- |
| Applicable    | Server Only                                                               |
| Meaning       | Whether to record the SQL invocation through REST interface               |
| Default Value | 0: false; 1: true                                                         |
| Note          | The resulting files, i.e. httpnote.0/httpnote.1, are located under logDir |

### httpMaxThreads

| Attribute     | Description                                  |
| ------------- | -------------------------------------------- |
| Applicable    | Server Only                                  |
| Meaning       | The number of threads for RESTFul interface. |
| Default Value | 2                                            |

### restfulRowLimit

| Attribute     | Description                                                  |
| ------------- | ------------------------------------------------------------ |
| Applicable    | Server Only                                                  |
| Meaning       | Maximum number of rows returned each time by REST interface. |
| Default Value | 10240                                                        |
| Note          | Maximum value is 10,000,000                                  |

### httpDBNameMandatory

| Attribute     | Description                              |
| ------------- | ---------------------------------------- |
| Applicable    | Server Only                              |
| Meaning       | Whether database name is required in URL |
| Value Range   | 0:not required, 1: required              |
| Default Value | 0                                        |
| Note          | From version 2.3.0.0                     |

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
| Default Value | 10,000,000                                 |

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

### debugFlag

| Attribute     | Description                                               |
| ------------- | --------------------------------------------------------- |
| Applicable    | Server and Client                                         |
| Meaning       | Log level                                                 |
| Value Range   | 131: INFO/WARNING/ERROR; 135: plus DEBUG; 143: plus TRACE |
| Default Value | 131 or 135, depending on the module                       |

### mDebugFlag

| Attribute     | Description        |
| ------------- | ------------------ |
| Applicable    | Server Only        |
| Meaning       | Log level of mnode |
| Value Range   | same as debugFlag  |
| Default Value | 135                |

### dDebugFlag

| Attribute     | Description        |
| ------------- | ------------------ |
| Applicable    | Server and Client  |
| Meaning       | Log level of dnode |
| Value Range   | same as debugFlag  |
| Default Value | 135                |

### sDebugFlag

| Attribute     | Description              |
| ------------- | ------------------------ |
| Applicable    | Server and Client        |
| Meaning       | Log level of sync module |
| Value Range   | same as debugFlag        |
| Default Value | 135                      |

### wDebugFlag

| Attribute     | Description             |
| ------------- | ----------------------- |
| Applicable    | Server and Client       |
| Meaning       | Log level of WAL module |
| Value Range   | same as debugFlag       |
| Default Value | 135                     |

### sdbDebugFlag

| Attribute     | Description            |
| ------------- | ---------------------- |
| Applicable    | Server and Client      |
| Meaning       | logLevel of sdb module |
| Value Range   | same as debugFlag      |
| Default Value | 135                    |

### rpcDebugFlag

| Attribute     | Description             |
| ------------- | ----------------------- |
| Applicable    | Server and Client       |
| Meaning       | Log level of rpc module |
| Value Range   | Same as debugFlag       |
| Default Value |                         |

### tmrDebugFlag

| Attribute     | Description               |
| ------------- | ------------------------- |
| Applicable    | Server and Client         |
| Meaning       | Log level of timer module |
| Value Range   | Same as debugFlag         |
| Default Value |                           |

### cDebugFlag

| Attribute     | Description         |
| ------------- | ------------------- |
| Applicable    | Client Only         |
| Meaning       | Log level of Client |
| Value Range   | Same as debugFlag   |
| Default Value |                     |

### jniDebugFlag

| Attribute     | Description             |
| ------------- | ----------------------- |
| Applicable    | Client Only             |
| Meaning       | Log level of jni module |
| Value Range   | Same as debugFlag       |
| Default Value |                         |

### odbcDebugFlag

| Attribute     | Description              |
| ------------- | ------------------------ |
| Applicable    | Client Only              |
| Meaning       | Log level of odbc module |
| Value Range   | Same as debugFlag        |
| Default Value |                          |

### uDebugFlag

| Attribute     | Description                |
| ------------- | -------------------------- |
| Applicable    | Server and Client          |
| Meaning       | Log level of common module |
| Value Range   | Same as debugFlag          |
| Default Value |                            |

### httpDebugFlag

| Attribute     | Description                                 |
| ------------- | ------------------------------------------- |
| Applicable    | Server Only                                 |
| Meaning       | Log level of http module (prior to 2.4.0.0) |
| Value Range   | Same as debugFlag                           |
| Default Value |                                             |

### mqttDebugFlag

| Attribute     | Description              |
| ------------- | ------------------------ |
| Applicable    | Server Only              |
| Meaning       | Log level of mqtt module |
| Value Range   | Same as debugFlag        |
| Default Value |                          |

### monitorDebugFlag

| Attribute     | Description                    |
| ------------- | ------------------------------ |
| Applicable    | Server Only                    |
| Meaning       | Log level of monitoring module |
| Value Range   | Same as debugFlag              |
| Default Value |                                |

### qDebugFlag

| Attribute     | Description               |
| ------------- | ------------------------- |
| Applicable    | Server and Client         |
| Meaning       | Log level of query module |
| Value Range   | Same as debugFlag         |
| Default Value |                           |

### vDebugFlag

| Attribute     | Description        |
| ------------- | ------------------ |
| Applicable    | Server and Client  |
| Meaning       | Log level of vnode |
| Value Range   | Same as debugFlag  |
| Default Value |                    |

### tsdbDebugFlag

| Attribute     | Description              |
| ------------- | ------------------------ |
| Applicable    | Server Only              |
| Meaning       | Log level of TSDB module |
| Value Range   | Same as debugFlag        |
| Default Value |                          |

### cqDebugFlag

| Attribute     | Description                          |
| ------------- | ------------------------------------ |
| Applicable    | Server and Client                    |
| Meaning       | Log level of continuous query module |
| Value Range   | Same as debugFlag                    |
| Default Value |                                      |

## Client Only

### maxSQLLength

| Attribute     | Description                            |
| ------------- | -------------------------------------- |
| Applicable    | Client Only                            |
| Meaning       | Maximum length of single SQL statement |
| Unit          | bytes                                  |
| Value Range   | 65480-1048576                          |
| Default Value | 1048576                                |

### tscEnableRecordSql

| Attribute     | Description                                                                                                                                       |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| Meaning       | Whether to record SQL statements in file                                                                                                          |
| Value Range   | 0: false, 1: true                                                                                                                                 |
| Default Value | 0                                                                                                                                                 |
| Note          | The generated files are named as "tscnote-xxxx.0/tscnote-xxx.1" in which "xxxx" is the pid of the client, and located at same place as client log |

### maxBinaryDisplayWidth

| Attribute     | Description                                                                                         |
| ------------- | --------------------------------------------------------------------------------------------------- |
| Meaning       | Maximum display width of binary and nchar in taos shell. Anything beyond this limit would be hidden |
| Value Range   | 5 -                                                                                                 |
| Default Value | 30                                                                                                  |

:::info
If the length of value exceeds `maxBinaryDisplayWidth`, then the actual display width is max(column name, maxBinaryDisplayLength); otherwise the actual display width is max(length of column name, length of column value). This parameter can also be changed dynamically using `set max_binary_display_width <nn\>` in TDengine CLI `taos`.

:::

### maxWildCardsLength

| Attribute     | Description                                           |
| ------------- | ----------------------------------------------------- |
| Meaning       | The maximum length for wildcard string used with LIKE |
| Unit          | bytes                                                 |
| Value Range   | 0-16384                                               |
| Default Value | 100                                                   |
| Note          | From version 2.1.6.1                                  |

### clientMerge

| Attribute     | Description                                         |
| ------------- | --------------------------------------------------- |
| Meaning       | Whether to filter out duplicate data on client side |
| Value Range   | 0: false; 1: true                                   |
| Default Value | 0                                                   |
| Note          | From version 2.3.0.0                                |

### maxRegexStringLen

| Attribute     | Description                          |
| ------------- | ------------------------------------ |
| Meaning       | Maximum length of regular expression |
| Value Range   | [128, 16384]                         |
| Default Value | 128                                  |
| Note          | From version 2.3.0.0                 |

## Other Parameters

### enableCoreFile

| Attribute     | Description                                                                                                                                                             |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Applicable    | Server and Client                                                                                                                                                       |
| Meaning       | Whether to generate core file when server crashes                                                                                                                       |
| Value Range   | 0: false, 1: true                                                                                                                                                       |
| Default Value | 1                                                                                                                                                                       |
| Note          | The core file is generated under root directory `systemctl start taosd` is used to start, or under the working directory if `taosd` is started directly on Linux Shell. |
