---
title: taosd Reference
sidebar_label: taosd
slug: /tdengine-reference/components/taosd
---

taosd is the core service of the TDengine database engine, and its configuration file is by default located at `/etc/taos/taos.cfg`, but you can also specify a configuration file in a different path. This section provides a detailed introduction to the command-line parameters of taosd and the configuration parameters in the configuration file.

## Command Line Parameters

The command line parameters for taosd are as follows:

- -a `<json file>`: Specifies a JSON file containing various configuration parameters for service startup, formatted like `{"fqdn":"td1"}`. For details on configuration parameters, please refer to the next section.
- -c `<directory>`: Specifies the directory where the configuration file is located.
- -s: Prints SDB information.
- -C: Prints configuration information.
- -e: Specifies environment variables, formatted like `-e 'TAOS_FQDN=td1'`.
- -k: Retrieves the machine code.
- -dm: Enables memory scheduling.
- -V: Prints version information.

## Configuration Parameters

Configuration parameters are divided into two categories:

|Parameter Type      |  Description                     |  Scope    | Modification Method              | View Parameter Command            |
|:------------|:-------------------------|:-----------|:---------------------|:---------------------|
| Global Configuration Parameters  | Parameters shared by all nodes in the cluster     |  Entire Cluster   | 1. Modify via SQL.   | show variables; |
| Local Configuration Parameters  | Parameters configured individually for each cluster node  |  Single Node    | 1. Modify via SQL; 2. Modify via taos.cfg configuration file. | show dnode `<dnode_id>` variables;|

Additional Notes:

1. Method to modify global configuration parameters via SQL: `alter all dnodes 'parameter_name' 'parameter_value';`, Whether the modifications take effect immediately, please refer to the "Dynamic Modification" description for each parameter.
2. Method to modify local configuration parameters via SQL: `alter dnode <dnode_id> 'parameter_name' 'parameter_value';`, Whether the modifications take effect immediately, please refer to the "Dynamic Modification" description for each parameter.
3. To modify local configuration parameters via taos.cfg configuration file, set the `forceReadConfig` parameter to 1 and restart for changes to take effect.
4. For dynamic modification methods of configuration parameters, please refer to [Node Management](../../sql-manual/manage-nodes/).
5. Some parameters exist in both the client (taosc) and server (taosd), with different scopes and meanings in different contexts. For details, please refer to [TDengine Configuration Parameter Scope Comparison](../../components/configuration-scope/).

### Connection Related

| Parameter Name         | Supported Version       | Dynamic Modification                                         | Description                                                  |
| ---------------------- | ----------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| firstEp                |                         | Not supported                                                | Endpoint of the first dnode in the cluster that taosd actively connects to at startup, default value localhost:6030 |
| secondEp               |                         | Not supported                                                | Endpoint of the second dnode in the cluster that taosd tries to connect to if the firstEp is unreachable, no default value |
| fqdn                   |                         | Not supported                                                | The service address that taosd listens on, default is the first hostname configured on the server |
| serverPort             |                         | Not supported                                                | The port that taosd listens on, default value 6030           |
| compressMsgSize        |                         | Supported, effective after restart                           | Whether to compress RPC messages; -1: do not compress any messages; 0: compress all messages; N (N>0): only compress messages larger than N bytes; default value -1 |
| shellActivityTimer     |                         | Supported, effective immediately                             | Duration in seconds for the client to send heartbeat to mnode, range 1-120, default value 3 |
| numOfRpcSessions       |                         | Supported, effective after restart                           | Maximum number of connections supported by RPC, range 100-100000, default value 30000 |
| numOfRpcThreads        |                         | Supported, effective after restart                           | Number of threads for receiving and sending RPC data, range 1-50, default value is half of the CPU cores |
| numOfTaskQueueThreads  |                         | Supported, effective after restart                           | Number of threads for client to process RPC messages, range 4-16, default value is half of the CPU cores |
| rpcQueueMemoryAllowed  |                         | Supported, effective immediately                             | Maximum memory allowed for received RPC messages in dnode, in bytes, range 104857600-INT64_MAX, default value is 1/10 of server memory |
| resolveFQDNRetryTime   | Cancelled after 3.x     | Not supported                                                | Number of retries when FQDN resolution fails                 |
| timeToGetAvailableConn | Cancelled after 3.3.4.x | Maximum waiting time to get an available connection, range 10-50000000, in milliseconds, default value 500000 |                                                              |
| maxShellConns          | Cancelled after 3.x     | Supported, effective after restart                           | Maximum number of connections allowed                        |
| maxRetryWaitTime       |                         | Supported, effective after restart                           | Maximum timeout for reconnection,calculated from the time of retry,range is 3000-86400000,in milliseconds, default value 20000 |
| shareConnLimit         | Added in 3.3.4.0        | Supported, effective after restart                           | Number of requests a connection can share, range 1-512, default value 10 |
| readTimeout            | Added in 3.3.4.0        | Supported, effective after restart                           | Minimum timeout for a single request, range 64-604800, in seconds, default value 900 |

### Monitoring Related

| Parameter Name     | Supported Version | Dynamic Modification               | Description                                                  |
| ------------------ | ----------------- | ---------------------------------- | ------------------------------------------------------------ |
| monitor            |                   | Supported, effective immediately   | Whether to collect and report monitoring data, 0: off; 1: on; default value 0 |
| monitorFqdn        |                   | Supported, effective after restart | The FQDN of the server where the taosKeeper service is located, default value none |
| monitorPort        |                   | Supported, effective after restart | The port number listened to by the taosKeeper service, default value 6043 |
| monitorInterval    |                   | Supported, effective immediately   | The time interval for recording system parameters (CPU/memory) in the monitoring database, in seconds, range 1-200000, default value 30 |
| monitorMaxLogs     |                   | Supported, effective immediately   | Number of cached logs pending report                         |
| monitorComp        |                   | Supported, effective after restart | Whether to use compression when reporting monitoring logs    |
| monitorLogProtocol |                   | Supported, effective immediately   | Whether to print monitoring logs                             |
| monitorForceV2     |                   | Supported, effective immediately   | Whether to use V2 protocol for reporting                     |
| telemetryReporting |                   | Supported, effective immediately   | Whether to upload telemetry, 0: do not upload, 1: upload, default value 1 |
| telemetryServer    |                   | Not supported                      | Telemetry server address                                     |
| telemetryPort      |                   | Not supported                      | Telemetry server port number                                 |
| telemetryInterval  |                   | Supported, effective immediately   | Telemetry upload interval, in seconds, default 86400         |
| crashReporting     |                   | Supported, effective immediately   | Whether to upload crash information; 0: do not upload, 1: upload; default value 1 |

### Query Related

| Parameter Name           | Supported Version | Dynamic Modification               | Description                                                  |
| ------------------------ | ----------------- | ---------------------------------- | ------------------------------------------------------------ |
| countAlwaysReturnValue   |                   | Supported, effective immediately   | Whether count/hyperloglog functions return a value when input data is empty or NULL; 0: return empty row, 1: return; default value 1; When this parameter is set to 1, if the query contains an INTERVAL clause or the query uses TSMA, and the corresponding group or window has empty or NULL data, the corresponding group or window will not return a query result; Note that this parameter should be consistent between client and server |
| tagFilterCache           |                   | Not supported                      | Whether to cache tag filter results                          |
| queryBufferSize          |                   | Supported, effective after restart | Not effective yet                                            |
| queryRspPolicy           |                   | Supported, effective immediately   | Query response strategy                                      |
| queryUseMemoryPool       |                   | Not supported                      | Whether query will use memory pool to manage memory, default value: 1 (on); 0: off, 1: on |
| minReservedMemorySize    |                   | Supported, effective immediately   | The minimum reserved system available memory size, all memory except reserved can be used for queries, unit: MB, default reserved size is 20% of system physical memory, value range 1024-1000000000 |
| singleQueryMaxMemorySize |                   | Not supported                      | The memory limit that a single query can use on a single node (dnode), exceeding this limit will return an error, unit: MB, default value: 0 (no limit), value range 0-1000000000 |
| filterScalarMode         |                   | Not supported                      | Force scalar filter mode, 0: off; 1: on, default value 0     |
| queryRsmaTolerance       |                   | Not supported                      | Internal parameter, tolerance time for determining which level of rsma data to query, in milliseconds |
| pqSortMemThreshold       |                   | Not supported                      | Internal parameter, memory threshold for sorting             |

### Region Related

| Parameter Name | Supported Version | Dynamic Modification | Description                                                  |
| -------------- | ----------------- | -------------------- | ------------------------------------------------------------ |
| timezone       |                   | since 3.1.0.0        | Time zone; defaults to dynamically obtaining the current time zone setting from the system |
| locale         |                   | since 3.1.0.0        | System locale information and encoding format, defaults to obtaining from the system |
| charset        |                   | since 3.1.0.0        | Character set encoding, defaults to obtaining from the system |

:::info

#### Explanation of Regional Related Parameters

1. To address the issue of data writing and querying across multiple time zones, TDengine uses Unix Timestamps to record and store timestamps. The nature of Unix Timestamps ensures that the timestamps generated are consistent at any given moment across any time zone. It is important to note that the conversion to Unix Timestamps is done on the client side. To ensure that other forms of time on the client are correctly converted to Unix Timestamps, it is necessary to set the correct time zone.

On Linux/macOS, the client automatically reads the time zone information set by the system. Users can also set the time zone in the configuration file in various ways. For example:

```text
timezone UTC-8
timezone GMT-8
timezone Asia/Shanghai
```

All are valid settings for the GMT+8 time zone. However, note that on Windows, the format `timezone UTC-8` is not supported, and must be written as `timezone Asia/Shanghai`.

The setting of the time zone affects the querying and writing of SQL statements involving non-Unix timestamp content (timestamp strings, interpretation of the keyword now). For example:

```sql
SELECT count(*) FROM table_name WHERE TS<'2019-04-11 12:01:08';
```

In GMT+8, the SQL statement is equivalent to:

```sql
SELECT count(*) FROM table_name WHERE TS<1554955268000;
```

In the UTC time zone, the SQL statement is equivalent to:

```sql
SELECT count(*) FROM table_name WHERE TS<1554984068000;
```

To avoid the uncertainties brought by using string time formats, you can also directly use Unix Timestamps. Additionally, you can use timestamp strings with time zones in SQL statements, such as RFC3339 formatted timestamp strings, 2013-04-12T15:52:01.123+08:00 or ISO-8601 formatted timestamp strings 2013-04-12T15:52:01.123+0800. The conversion of these two strings to Unix Timestamps is not affected by the system's local time zone.

1. TDengine provides a special field type, nchar, for storing wide characters in non-ASCII encodings such as Chinese, Japanese, and Korean. Data written to the nchar field is uniformly encoded in UCS4-LE format and sent to the server. It is important to note that the correctness of the encoding is ensured by the client. Therefore, if users want to properly use the nchar field to store non-ASCII characters such as Chinese, Japanese, and Korean, they need to correctly set the client's encoding format.

The characters input by the client use the current default encoding format of the operating system, which is often UTF-8 on Linux/macOS systems, but may be GB18030 or GBK on some Chinese systems. The default encoding in a Docker environment is POSIX. In Chinese version Windows systems, the encoding is CP936. The client needs to ensure the correct setting of the character set they are using, i.e., the current encoding character set of the operating system on which the client is running, to ensure that the data in nchar is correctly converted to UCS4-LE encoding format.

In Linux/macOS, the naming rule for locale is: \<language>_\<region>.\<character set encoding> such as: zh_CN.UTF-8, where zh represents Chinese, CN represents mainland China, and UTF-8 represents the character set. The character set encoding provides instructions for the client to correctly parse local strings. Linux/macOS can set the system's character encoding by setting the locale, but since Windows uses a locale format that is not POSIX standard, another configuration parameter charset is used to specify the character encoding in Windows. Charset can also be used in Linux/macOS to specify the character encoding.

1. If charset is not set in the configuration file, in Linux/macOS, taos automatically reads the current locale information of the system at startup, and extracts the charset encoding format from the locale information. If it fails to automatically read the locale information, it attempts to read the charset configuration, and if reading the charset configuration also fails, it interrupts the startup process.

In Linux/macOS, the locale information includes character encoding information, so after correctly setting the locale in Linux/macOS, there is no need to set charset separately. For example:

```text
locale zh_CN.UTF-8
```

In Windows systems, it is not possible to obtain the current encoding from the locale. If it is not possible to read the string encoding information from the configuration file, taos defaults to setting the character encoding to CP936. This is equivalent to adding the following configuration in the configuration file:

```text
charset CP936
```

If you need to adjust the character encoding, please consult the encoding used by the current operating system and set it correctly in the configuration file.

In Linux/macOS, if the user sets both locale and charset encoding, and if the locale and charset are inconsistent, the latter setting will override the earlier one.

```text
locale zh_CN.UTF-8
charset GBK
```

Then the effective value of charset is GBK.

```text
charset GBK
locale zh_CN.UTF-8
```

The effective value of charset is UTF-8.

:::

### Storage Related

| Parameter Name           | Supported Version | Dynamic Modification               | Description                                                  |
| ------------------------ | ----------------- | ---------------------------------- | ------------------------------------------------------------ |
| dataDir                  |                   | Not supported                      | Directory for data files, all data files are written to this directory, default value /var/lib/taos |
| tempDir                  |                   | Not supported                      | Specifies the directory for generating temporary files during system operation, default value /tmp |
| minimalDataDirGB         |                   | Not supported                      | Minimum space to be reserved in the time-series data storage directory specified by dataDir, in GB, default value 2 |
| minimalTmpDirGB          |                   | Not supported                      | Minimum space to be reserved in the temporary file directory specified by tempDir, in GB, default value 1 |
| minDiskFreeSize          | After 3.1.1.0     | Supported, effective immediately   | When the available space on a disk is less than or equal to this threshold, the disk will no longer be selected for generating new data files, unit is bytes, range 52428800-2199023255552, default value 52428800; Enterprise parameter |
| ssAutoMigrateIntervalSec | After 3.3.7.0     | Supported, effective immediately   | Trigger cycle for automatic upload of local data files to shared storage, in seconds. Minimum: 600; Maximum: 100000. Default value 3600; Enterprise parameter |
| ssEnabled                | After 3.3.7.0     | Supported, effective after restart | Whether to enable shared storage, default value is 0, which means disabled, 1 means only enable manual shared storage migration, 2 means enable auto shared storage migration |
| ssAccessString           | After 3.3.7.0     | Supported, effective after restart | A string which contains various options for accessing the shared storage, the format is `<device-type>:<option-name>=<option-value>;<option-name>=<option-value>;...`, the possible options vary from shared storage providers, please refer related document for details |
| ssPageCacheSize          | After 3.3.7.0     | Supported, effective after restart | Number of shared storage page cache pages, range 4-1048576, unit is pages, default value 4096; Enterprise parameter |
| ssUploadDelaySec         | After 3.3.7.0     | Supported, effective immediately   | How long a data file remains unchanged before being uploaded to S3, range 1-2592000 (30 days), in seconds, default value 60; Enterprise parameter |
| cacheLazyLoadThreshold   |                   | Supported, effective immediately   | Internal parameter, cache loading strategy                   |

### Cluster Related

| Parameter Name             | Supported Version | Dynamic Modification               | Description                                                                                                                                                                                                                                                                    |
| -------------------------- | ----------------- | ---------------------------------- |--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| supportVnodes              |                   | Supported, effective immediately   | Maximum number of vnodes supported by a dnode, range 0-4096, default value is twice the number of CPU cores + 5                                                                                                                                                                |
| numOfCommitThreads         |                   | Supported, effective after restart | Maximum number of commit threads, range 1-1024, default value 4                                                                                                                                                                                                                |
| numOfCompactThreads        |                   | Supported, effective after restart | Maximum number of commit threads, range 1-16, default value 2                                                                                                                                                                                                                  |
| numOfMnodeReadThreads      |                   | Supported, effective after restart | Number of Read threads for mnode, range 0-1024, default value is one quarter of the CPU cores (not exceeding 4)                                                                                                                                                                |
| numOfVnodeQueryThreads     |                   | Supported, effective after restart | Number of Query threads for vnode, range 0-1024, default value is twice the number of CPU cores (not exceeding 16)                                                                                                                                                             |
| numOfVnodeFetchThreads     |                   | Supported, effective after restart | Number of Fetch threads for vnode, range 0-1024, default value is one quarter of the CPU cores (not exceeding 4)                                                                                                                                                               |
| numOfVnodeRsmaThreads      |                   | Supported, effective after restart | Number of Rsma threads for vnode, range 0-1024, default value is one quarter of the CPU cores (not exceeding 4)                                                                                                                                                                |
| numOfQnodeQueryThreads     |                   | Supported, effective after restart | Number of Query threads for qnode, range 0-1024, default value is twice the number of CPU cores (not exceeding 16)                                                                                                                                                             |
| ttlUnit                    |                   | Not supported                      | Unit for ttl parameter, range 1-31572500, in seconds, default value 86400                                                                                                                                                                                                      |
| ttlPushInterval            |                   | Supported, effective immediately   | Frequency of ttl timeout checks, range 1-100000, in seconds, default value 10                                                                                                                                                                                                  |
| ttlChangeOnWrite           |                   | Supported, effective immediately   | Whether ttl expiration time changes with table modification; 0: no change, 1: change; default value 0                                                                                                                                                                          |
| ttlBatchDropNum            |                   | Supported, effective immediately   | Number of subtables deleted in a batch for ttl, minimum value 0, default value 10000                                                                                                                                                                                           |
| retentionSpeedLimitMB      |                   | Supported, effective immediately   | Speed limit for data migration across different levels of disks, range 0-1024, in MB, default value 0, which means no limit                                                                                                                                                    |
| maxTsmaNum                 |                   | Supported, effective immediately   | Maximum number of TSMAs that can be created in the cluster; range 0-10; default value 10                                                                                                                                                                                       |
| tmqMaxTopicNum             |                   | Supported, effective immediately   | Maximum number of topics that can be established for subscription; range 1-10000; default value 20                                                                                                                                                                             |
| tmqRowSize                 |                   | Supported, effective immediately   | Maximum number of records in a subscription data block, range 1-1000000, default value 4096                                                                                                                                                                                    |
| audit                      |                   | Supported, effective immediately   | Audit feature switch; Enterprise parameter                                                                                                                                                                                                                                     |
| auditInterval              |                   | Supported, effective immediately   | Time interval for reporting audit data; Enterprise parameter                                                                                                                                                                                                                   |
| auditCreateTable           |                   | Supported, effective immediately   | Whether to enable audit feature for creating subtables; Enterprise parameter                                                                                                                                                                                                   |
| encryptAlgorithm           |                   | Not supported                      | Data encryption algorithm; Enterprise parameter                                                                                                                                                                                                                                |
| encryptScope               |                   | Not supported                      | Encryption scope; Enterprise parameter                                                                                                                                                                                                                                         |
| encryptPassAlgorithm       |v3.3.7.0           |Supported, effective immediately    | Switch for saving user password as encrypted string                                                                                                                                                                                                                            |
| enableWhiteList            |                   | Supported, effective immediately   | Switch for whitelist feature; Enterprise parameter                                                                                                                                                                                                                             |
| syncLogBufferMemoryAllowed |                   | Supported, effective immediately   | Maximum memory allowed for sync log cache messages for a dnode, in bytes, range 104857600-INT64_MAX, default value is 1/10 of server memory, effective from versions 3.1.3.2/3.3.2.13                                                                                          |
| syncApplyQueueSize         |                   | supported, effective immediately   | Size of apply queue for sync log, range 32-2048, default is 512                                                                                                                                                                                                                |
| statusIntervalMs           |                   | supported, effective immediately   | Internal parameter, for debugging synchronization module                                                                                                                                                                                                                       |
| statusSRTimeoutMs          |                   | supported, effective immediately   | Internal parameter, for debugging synchronization module                                                                                                                                                                                                                       |
| statusTimeoutMs            |                   | supported, effective immediately   | Internal parameter, for debugging synchronization module                                                                                                                                                                                                                       |
| syncElectInterval          |                   | Not supported                      | Internal parameter, for debugging synchronization module                                                                                                                                                                                                                       |
| syncHeartbeatInterval      |                   | Not supported                      | Internal parameter, for debugging synchronization module                                                                                                                                                                                                                       |
| syncVnodeElectIntervalMs   |                   | Supported, effective immediately   | Internal parameter, for debugging synchronization module                                                                                                                                                                                                                       |
| syncVnodeHeartbeatIntervalMs|                  | Supported, effective immediately   | Internal parameter, for debugging synchronization module                                                                                                                                                                                                                       |
| syncMnodeElectIntervalMs   |                   | Supported, effective immediately   | Internal parameter, for debugging synchronization module                                                                                                                                                                                                                       |
| syncMnodeHeartbeatIntervalMs|                  | Supported, effective immediately   | Internal parameter, for debugging synchronization module                                                                                                                                                                                                                       |
| syncHeartbeatTimeout       |                   | Not supported                      | Internal parameter, for debugging synchronization module                                                                                                                                                                                                                       |
| syncSnapReplMaxWaitN       |                   | Supported, effective immediately   | Internal parameter, for debugging synchronization module                                                                                                                                                                                                                       |
| arbHeartBeatIntervalSec    |                   | Supported, effective immediately   | Internal parameter, for debugging synchronization module                                                                                                                                                                                                                       |
| arbCheckSyncIntervalSec    |                   | Supported, effective immediately   | Internal parameter, for debugging synchronization module                                                                                                                                                                                                                       |
| arbSetAssignedTimeoutSec   |                   | Supported, effective immediately   | Internal parameter, for debugging synchronization module                                                                                                                                                                                                                       |
| arbHeartBeatIntervalMs     |                   | Supported, effective immediately   | Internal parameter, for debugging synchronization module                                                                                                                                                                                                                       |
| arbCheckSyncIntervalMs     |                   | Supported, effective immediately   | Internal parameter, for debugging synchronization module                                                                                                                                                                                                                       |
| arbSetAssignedTimeoutMs    |                   | Supported, effective immediately   | Internal parameter, for debugging synchronization module                                                                                                                                                                                                                       |
| syncTimeout                |                   | Supported, effective immediately   | Internal parameter, for debugging synchronization module                                                                                                                                                                                                                       |
| mndSdbWriteDelta           |                   | Supported, effective immediately   | Internal parameter, for debugging mnode module                                                                                                                                                                                                                                 |
| mndLogRetention            |                   | Supported, effective immediately   | Internal parameter, for debugging mnode module                                                                                                                                                                                                                                 |
| skipGrant                  |                   | Not supported                      | Internal parameter, for authorization checks                                                                                                                                                                                                                                   |
| trimVDbIntervalSec         |                   | Supported, effective immediately   | Internal parameter, for deleting expired data                                                                                                                                                                                                                                  |
| ttlFlushThreshold          |                   | Supported, effective immediately   | Internal parameter, frequency of ttl timer                                                                                                                                                                                                                                     |
| compactPullupInterval      |                   | Supported, effective immediately   | Internal parameter, frequency of data reorganization timer                                                                                                                                                                                                                     |
| walFsyncDataSizeLimit      |                   | Supported, effective immediately   | Internal parameter, threshold for WAL to perform FSYNC                                                                                                                                                                                                                         |
| walForceRepair             |                   | Not supported                      | Internal parameter, repair WAL file forcibly, range 0-1; default value 0, 0 means not repair, 1 means repair                                                                                                                                                                   |
| transPullupInterval        |                   | Supported, effective immediately   | Internal parameter, retry interval for mnode to execute transactions                                                                                                                                                                                                           |
| mqRebalanceInterval        |                   | Supported, effective immediately   | Internal parameter, interval for consumer rebalancing                                                                                                                                                                                                                          |
| uptimeInterval             |                   | Supported, effective immediately   | Internal parameter, for recording system uptime                                                                                                                                                                                                                                |
| timeseriesThreshold        |                   | Supported, effective immediately   | Internal parameter, for usage statistics                                                                                                                                                                                                                                       |
| udf                        |                   | Supported, effective after restart | Whether to start UDF service; 0: do not start, 1: start; default value 0                                                                                                                                                                                                       |
| udfdResFuncs               |                   | Supported, effective after restart | Internal parameter, for setting UDF result sets                                                                                                                                                                                                                                |
| udfdLdLibPath              |                   | Supported, effective after restart | Internal parameter, indicates the library path for loading UDF                                                                                                                                                                                                                 |
| enableStrongPassword       | After 3.3.6.0     | Supported, effective after restart | The password include at least three types of characters from the following: uppercase letters, lowercase letters, numbers, and special characters, special characters include `! @ # $ % ^ & * ( ) - _ + = [ ] { } : ; > < ? \| ~ , .`; 0: disable, 1: enable; default value 1 |
|enableIpv6                  | 3.3.7.0           |not Supported                       | force nodes to communicate directly via IPv6 only, default value is 0, notes: 1. `firstep`, `sencodep`, and `FQDN` must all resolve to IPv6 addresses. 2. Mixed IPv4/IPv6 deployment is not supported                                                                          |
|statusInterval              | 3.3.0.0           | Supported, effective immediately   | Controls the interval time for dnode to send status reports to mnode                                                                                                                                                                                                           |

### Stream Computing Parameters

| Parameter Name          | Supported Version | Dynamic Modification               | Description                                                  |
| ----------------------- | ----------------- | ---------------------------------- | ------------------------------------------------------------ |
| disableStream           |                   | Supported, effective immediately   | Switch to enable or disable stream computing                 |
| streamBufferSize        |                   | Supported, effective immediately   | Controls the size of the window state cache in memory, default value is 128MB |
| streamAggCnt            |                   | Not supported                      | Internal parameter, number of concurrent aggregation computations |
| checkpointInterval      |                   | Supported, effective after restart | Internal parameter, checkpoint synchronization interval      |
| concurrentCheckpoint    |                   | Supported, effective immediately   | Internal parameter, whether to check checkpoints concurrently |
| maxStreamBackendCache   |                   | Supported, effective immediately   | Internal parameter, maximum cache used by stream computing   |
| streamSinkDataRate      |                   | Supported, effective after restart | Internal parameter, used to control the write speed of stream computing results |
| streamNotifyMessageSize | After 3.3.6.0     | Not supported                      | Internal parameter, controls the message size for event notifications, default value is 8192 |
| streamNotifyFrameSize   | After 3.3.6.0     | Not supported                      | Internal parameter, controls the underlying frame size when sending event notification messages, default value is 256 |
| adapterFqdn             | After 3.3.6.0     | Not supported                      | Internal parameter, The address of the taosadapter services, default value is localhost |
| adapterPort             | After 3.3.6.0     | Not supported                      | Internal parameter, The port of the taosadapter services, default value is 6041 |
| adapterToken            | After 3.3.6.0     | Not supported                      | Internal parameter, The string obtained by Base64-encoding `{username}:{password}`, default value is `cm9vdDp0YW9zZGF0YQ==` |

### Log Related

| Parameter Name   | Supported Version | Dynamic Modification             | Description                                                  |
| ---------------- | ----------------- | -------------------------------- | ------------------------------------------------------------ |
| logDir           |                   | Not supported                    | Log file directory, operational logs will be written to this directory, default value /var/log/taos |
| minimalLogDirGB  |                   | Not supported                    | Stops writing logs when the available space on the disk where the log folder is located is less than this value, unit GB, default value 1 |
| numOfLogLines    |                   | Supported, effective immediately | Maximum number of lines allowed in a single log file, default value 10,000,000 |
| asyncLog         |                   | Supported, effective immediately | Log writing mode, 0: synchronous, 1: asynchronous, default value 1 |
| logKeepDays      |                   | Supported, effective immediately | Maximum retention time for log files, unit: days, default value 0, which means unlimited retention, log files will not be renamed, nor will new log files be rolled out, but the content of the log files may continue to roll depending on the log file size setting; when set to a value greater than 0, when the log file size reaches the set limit, it will be renamed to taosdlog.yyy, where yyy is the timestamp of the last modification of the log file, and a new log file will be rolled out, and log files whose creation time exceeds logKeepDays will be removed; Considering the usage habits of users of TDengine 2.0, starting from TDengine 3.3.6.6, when the value is set to less than 0, except that log files whose creation time exceeds -logKeepDays will be removed, other behaviors are the same as those when the value is greater than 0(For TDengine versions between 3.0.0.0 and 3.3.6.5, it is not recommended to set the value to less than 0) |
| slowLogThreshold | 3.3.3.0 onwards   | Supported, effective immediately | Slow query threshold, queries taking longer than or equal to this threshold are considered slow, unit seconds, default value 3 |
| slowLogMaxLen    | 3.3.3.0 onwards   | Supported, effective immediately | Maximum length of slow query logs, range 1-16384, default value 4096 |
| slowLogScope     | 3.3.3.0 onwards   | Supported, effective immediately | Type of slow query records, range ALL/QUERY/INSERT/OTHERS/NONE, default value QUERY |
| slowLogExceptDb  | 3.3.3.0 onwards   | Supported, effective immediately | Specifies the database that does not report slow queries, only supports configuring one database |
| debugFlag        |                   | Supported, effective immediately | Log switch for running logs, 131 (outputs error and warning logs), 135 (outputs error, warning, and debug logs), 143 (outputs error, warning, debug, and trace logs); default value 131 or 135 (depending on the module) |
| tmrDebugFlag     |                   | Supported, effective immediately | Log switch for the timer module, range as above              |
| uDebugFlag       |                   | Supported, effective immediately | Log switch for the utility module, range as above            |
| rpcDebugFlag     |                   | Supported, effective immediately | Log switch for the rpc module, range as above                |
| qDebugFlag       |                   | Supported, effective immediately | Log switch for the query module, range as above              |
| dDebugFlag       |                   | Supported, effective immediately | Log switch for the dnode module, range as above              |
| vDebugFlag       |                   | Supported, effective immediately | Log switch for the vnode module, range as above              |
| mDebugFlag       |                   | Supported, effective immediately | Log switch for the mnode module, range as above              |
| azDebugFlag      | 3.3.4.3 onwards   | Supported, effective immediately | Log switch for the S3 module, range as above                 |
| sDebugFlag       |                   | Supported, effective immediately | Log switch for the sync module, range as above               |
| tsdbDebugFlag    |                   | Supported, effective immediately | Log switch for the tsdb module, range as above               |
| tqDebugFlag      |                   | Supported, effective immediately | Log switch for the tq module, range as above                 |
| fsDebugFlag      |                   | Supported, effective immediately | Log switch for the fs module, range as above                 |
| udfDebugFlag     |                   | Supported, effective immediately | Log switch for the udf module, range as above                |
| smaDebugFlag     |                   | Supported, effective immediately | Log switch for the sma module, range as above                |
| idxDebugFlag     |                   | Supported, effective immediately | Log switch for the index module, range as above              |
| tdbDebugFlag     |                   | Supported, effective immediately | Log switch for the tdb module, range as above                |
| metaDebugFlag    |                   | Supported, effective immediately | Log switch for the meta module, range as above               |
| stDebugFlag      |                   | Supported, effective immediately | Log switch for the stream module, range as above             |
| sndDebugFlag     |                   | Supported, effective immediately | Log switch for the snode module, range as above              |

### Debugging Related

| Parameter Name       | Supported Version | Dynamic Modification             | Description                                                  |
| -------------------- | ----------------- | -------------------------------- | ------------------------------------------------------------ |
| enableCoreFile       |                   | Supported, effective immediately | Whether to generate a core file when crashing, 0: do not generate, 1: generate; default value is 1 |
| configDir            |                   | Not supported                    | Directory where the configuration files are located          |
| forceReadConfig      |                   | Not supported                    |                                                              |
| scriptDir            |                   | Not supported                    | Directory for internal test tool scripts                     |
| assert               |                   | Not supported                    | Assertion control switch, default value is 0                 |
| randErrorChance      |                   | Supported, effective immediately | Internal parameter, used for random failure testing          |
| randErrorDivisor     |                   | Supported, effective immediately | Internal parameter, used for random failure testing          |
| randErrorScope       |                   | Supported, effective immediately | Internal parameter, used for random failure testing          |
| safetyCheckLevel     |                   | Supported, effective immediately | Internal parameter, used for random failure testing          |
| experimental         |                   | Supported, effective immediately | Internal parameter, used for some experimental features      |
| simdEnable           | After 3.3.4.3     | Not supported                    | Internal parameter, used for testing SIMD acceleration       |
| AVX512Enable         | After 3.3.4.3     | Not supported                    | Internal parameter, used for testing AVX512 acceleration     |
| rsyncPort            |                   | Not supported                    | Internal parameter, used for debugging stream computing      |
| snodeAddress         |                   | Supported, effective immediately | Internal parameter, used for debugging stream computing      |
| checkpointBackupDir  |                   | Supported, effective immediately | Internal parameter, used for restoring snode data            |
| enableAuditDelete    |                   | Not supported                    | Internal parameter, used for testing audit functions         |
| slowLogThresholdTest |                   | Not supported                    | Internal parameter, used for testing slow logs               |
| bypassFlag           | After 3.3.4.5     | Supported, effective immediately | Internal parameter, used for  short-circuit testing          |

### Compression Parameters

| Parameter Name | Supported Version | Dynamic Modification               | Description                                                  |
| -------------- | ----------------- | ---------------------------------- | ------------------------------------------------------------ |
| fPrecision     |                   | Supported, effective immediately   | Sets the compression precision for float type floating numbers, range 0.1 ~ 0.00000001, default value 0.00000001, floating numbers smaller than this value will have their mantissa truncated |
| dPrecision     |                   | Supported, effective immediately   | Sets the compression precision for double type floating numbers, range 0.1 ~ 0.0000000000000001, default value 0.0000000000000001, floating numbers smaller than this value will have their mantissa truncated |
| lossyColumn    | Before 3.3.0.0    | Not supported                      | Enables TSZ lossy compression for float and/or double types; range float/double/none; default value none, indicating lossless compression is off |
| ifAdtFse       |                   | Supported, effective after restart | When TSZ lossy compression is enabled, use the FSE algorithm instead of the HUFFMAN algorithm, FSE algorithm is faster in compression but slightly slower in decompression, choose this for faster compression speed; 0: off, 1: on; default value is 0 |
| maxRange       |                   | Supported, effective after restart | Internal parameter, used for setting lossy compression       |
| curRange       |                   | Supported, effective after restart | Internal parameter, used for setting lossy compression       |
| compressor     |                   | Supported, effective after restart | Internal parameter, used for setting lossy compression       |

## taosd Monitoring Metrics

taosd reports monitoring metrics to taosKeeper, which are written into the monitoring database by taosKeeper, default is `log` database, which can be modified in the taoskeeper configuration file. Below is a detailed introduction to these monitoring metrics.

### taosd_cluster_basic Table

`taosd_cluster_basic` table records basic cluster information.

| field                | type      | is\_tag | comment                         |
| :------------------- | :-------- | :------ | :------------------------------ |
| ts                   | TIMESTAMP |         | timestamp                       |
| first\_ep            | VARCHAR   |         | cluster first ep                |
| first\_ep\_dnode\_id | INT       |         | dnode id of cluster first ep    |
| cluster_version      | VARCHAR   |         | tdengine version. e.g.: 3.0.4.0 |
| cluster\_id          | VARCHAR   | tag     | cluster id                      |

### taosd\_cluster\_info table

`taosd_cluster_info` table records cluster information.

| field                    | type      | is\_tag | comment                                                      |
| :----------------------- | :-------- | :------ | :----------------------------------------------------------- |
| \_ts                     | TIMESTAMP |         | timestamp                                                    |
| cluster_uptime           | DOUBLE    |         | uptime of the current master node. Unit: seconds             |
| dbs\_total               | DOUBLE    |         | total number of databases                                    |
| tbs\_total               | DOUBLE    |         | total number of tables in the current cluster                |
| stbs\_total              | DOUBLE    |         | total number of stables in the current cluster               |
| dnodes\_total            | DOUBLE    |         | total number of dnodes in the current cluster                |
| dnodes\_alive            | DOUBLE    |         | total number of alive dnodes in the current cluster          |
| mnodes\_total            | DOUBLE    |         | total number of mnodes in the current cluster                |
| mnodes\_alive            | DOUBLE    |         | total number of alive mnodes in the current cluster          |
| vgroups\_total           | DOUBLE    |         | total number of vgroups in the current cluster               |
| vgroups\_alive           | DOUBLE    |         | total number of alive vgroups in the current cluster         |
| vnodes\_total            | DOUBLE    |         | total number of vnodes in the current cluster                |
| vnodes\_alive            | DOUBLE    |         | total number of alive vnodes in the current cluster          |
| connections\_total       | DOUBLE    |         | total number of connections in the current cluster           |
| topics\_total            | DOUBLE    |         | total number of topics in the current cluster                |
| streams\_total           | DOUBLE    |         | total number of streams in the current cluster               |
| grants_expire\_time      | DOUBLE    |         | authentication expiration time, valid in TSDB-Enterprise, maximum DOUBLE value in TSDB-OSS |
| grants_timeseries\_used  | DOUBLE    |         | number of used timeseries                                    |
| grants_timeseries\_total | DOUBLE    |         | total number of timeseries, maximum DOUBLE value in open source version |
| cluster\_id              | VARCHAR   | tag     | cluster id                                                   |

### taosd\_vgroups\_info Table

`taosd_vgroups_info` table records virtual node group information.

| field          | type      | is\_tag | comment                                       |
| :------------- | :-------- | :------ | :-------------------------------------------- |
| \_ts           | TIMESTAMP |         | timestamp                                     |
| tables\_num    | DOUBLE    |         | Number of tables in vgroup                    |
| status         | DOUBLE    |         | vgroup status, range: unsynced = 0, ready = 1 |
| vgroup\_id     | VARCHAR   | tag     | vgroup id                                     |
| database\_name | VARCHAR   | tag     | Name of the database the vgroup belongs to    |
| cluster\_id    | VARCHAR   | tag     | cluster id                                    |

### taosd\_dnodes\_info Table

`taosd_dnodes_info` records dnode information.

| field             | type      | is\_tag | comment                                                      |
| :---------------- | :-------- | :------ | :----------------------------------------------------------- |
| \_ts              | TIMESTAMP |         | timestamp                                                    |
| uptime            | DOUBLE    |         | dnode uptime, unit: seconds                                  |
| cpu\_engine       | DOUBLE    |         | taosd CPU usage, read from `/proc/<taosd_pid>/stat`          |
| cpu\_system       | DOUBLE    |         | Server CPU usage, read from `/proc/stat`                     |
| cpu\_cores        | DOUBLE    |         | Number of server CPU cores                                   |
| mem\_engine       | DOUBLE    |         | taosd memory usage, read from `/proc/<taosd_pid>/status`     |
| mem\_free         | DOUBLE    |         | Server free memory, unit: KB                                 |
| mem\_total        | DOUBLE    |         | Total server memory, unit: KB                                |
| disk\_used        | DOUBLE    |         | Disk usage of data dir mount, unit: bytes                    |
| disk\_total       | DOUBLE    |         | Total disk capacity of data dir mount, unit: bytes           |
| system\_net\_in   | DOUBLE    |         | Network throughput, received bytes read from `/proc/net/dev`. Unit: byte/s |
| system\_net\_out  | DOUBLE    |         | Network throughput, transmit bytes read from `/proc/net/dev`. Unit: byte/s |
| io\_read          | DOUBLE    |         | IO throughput, speed calculated from `rchar` read from `/proc/<taosd_pid>/io` since last value. Unit: byte/s |
| io\_write         | DOUBLE    |         | IO throughput, speed calculated from `wchar` read from `/proc/<taosd_pid>/io` since last value. Unit: byte/s |
| io\_read\_disk    | DOUBLE    |         | Disk IO throughput, read_bytes read from `/proc/<taosd_pid>/io`. Unit: byte/s |
| io\_write\_disk   | DOUBLE    |         | Disk IO throughput, write_bytes read from `/proc/<taosd_pid>/io`. Unit: byte/s |
| vnodes\_num       | DOUBLE    |         | Number of vnodes on dnode                                    |
| masters           | DOUBLE    |         | Number of master nodes on dnode                              |
| has\_mnode        | DOUBLE    |         | Whether dnode contains mnode, range: contains=1, does not contain=0 |
| has\_qnode        | DOUBLE    |         | Whether dnode contains qnode, range: contains=1, does not contain=0 |
| has\_snode        | DOUBLE    |         | Whether dnode contains snode, range: contains=1, does not contain=0 |
| has\_bnode        | DOUBLE    |         | Whether dnode contains bnode, range: contains=1, does not contain=0 |
| error\_log\_count | DOUBLE    |         | Total number of error logs                                   |
| info\_log\_count  | DOUBLE    |         | Total number of info logs                                    |
| debug\_log\_count | DOUBLE    |         | Total number of debug logs                                   |
| trace\_log\_count | DOUBLE    |         | Total number of trace logs                                   |
| dnode\_id         | VARCHAR   | tag     | dnode id                                                     |
| dnode\_ep         | VARCHAR   | tag     | dnode endpoint                                               |
| cluster\_id       | VARCHAR   | tag     | cluster id                                                   |

### taosd\_dnodes\_status table

The `taosd_dnodes_status` table records dnode status information.

| field       | type      | is\_tag | comment                                      |
| :---------- | :-------- | :------ | :------------------------------------------- |
| \_ts        | TIMESTAMP |         | timestamp                                    |
| status      | DOUBLE    |         | dnode status, value range ready=1, offline=0 |
| dnode\_id   | VARCHAR   | tag     | dnode id                                     |
| dnode\_ep   | VARCHAR   | tag     | dnode endpoint                               |
| cluster\_id | VARCHAR   | tag     | cluster id                                   |

### taosd\_dnodes\_log\_dir table

The `taosd_dnodes_log_dir` table records log directory information.

| field       | type      | is\_tag | comment                                      |
| :---------- | :-------- | :------ | :------------------------------------------- |
| \_ts        | TIMESTAMP |         | timestamp                                    |
| avail       | DOUBLE    |         | available space in log directory. Unit: byte |
| used        | DOUBLE    |         | used space in log directory. Unit: byte      |
| total       | DOUBLE    |         | space in log directory. Unit: byte           |
| name        | VARCHAR   | tag     | log directory name, usually `/var/log/taos/` |
| dnode\_id   | VARCHAR   | tag     | dnode id                                     |
| dnode\_ep   | VARCHAR   | tag     | dnode endpoint                               |
| cluster\_id | VARCHAR   | tag     | cluster id                                   |

### taosd\_dnodes\_data\_dir table

The `taosd_dnodes_data_dir` table records data directory information.

| field       | type      | is\_tag | comment                                       |
| :---------- | :-------- | :------ | :-------------------------------------------- |
| \_ts        | TIMESTAMP |         | timestamp                                     |
| avail       | DOUBLE    |         | available space in data directory. Unit: byte |
| used        | DOUBLE    |         | used space in data directory. Unit: byte      |
| total       | DOUBLE    |         | space in data directory. Unit: byte           |
| level       | VARCHAR   | tag     | multi-level storage levels 0, 1, 2            |
| name        | VARCHAR   | tag     | data directory, usually `/var/lib/taos`       |
| dnode\_id   | VARCHAR   | tag     | dnode id                                      |
| dnode\_ep   | VARCHAR   | tag     | dnode endpoint                                |
| cluster\_id | VARCHAR   | tag     | cluster id                                    |

### taosd\_mnodes\_info table

The `taosd_mnodes_info` table records mnode role information.

| field       | type      | is\_tag | comment                                                      |
| :---------- | :-------- | :------ | :----------------------------------------------------------- |
| \_ts        | TIMESTAMP |         | timestamp                                                    |
| role        | DOUBLE    |         | mnode role, value range offline = 0, follower = 100, candidate = 101, leader = 102, error = 103, learner = 104 |
| mnode\_id   | VARCHAR   | tag     | master node id                                               |
| mnode\_ep   | VARCHAR   | tag     | master node endpoint                                         |
| cluster\_id | VARCHAR   | tag     | cluster id                                                   |

### taosd\_vnodes\_role table

The `taosd_vnodes_role` table records virtual node role information.

| field          | type      | is\_tag | comment                                                      |
| :------------- | :-------- | :------ | :----------------------------------------------------------- |
| \_ts           | TIMESTAMP |         | timestamp                                                    |
| vnode\_role    | DOUBLE    |         | vnode role, value range offline = 0, follower = 100, candidate = 101, leader = 102, error = 103, learner = 104 |
| vgroup\_id     | VARCHAR   | tag     | dnode id                                                     |
| dnode\_id      | VARCHAR   | tag     | dnode id                                                     |
| database\_name | VARCHAR   | tag     | vgroup's belonging database name                             |
| cluster\_id    | VARCHAR   | tag     | cluster id                                                   |

### taosd_sql_req Table

`taosd_sql_req` records server-side SQL request information.

| field      | type      | is_tag | comment                                             |
| :--------- | :-------- | :----- | :-------------------------------------------------- |
| _ts        | TIMESTAMP |        | timestamp                                           |
| count      | DOUBLE    |        | number of SQL queries                               |
| result     | VARCHAR   | tag    | SQL execution result, values range: Success, Failed |
| username   | VARCHAR   | tag    | user name executing the SQL                         |
| sql_type   | VARCHAR   | tag    | SQL type, value range: inserted_rows                |
| dnode_id   | VARCHAR   | tag    | dnode id                                            |
| dnode_ep   | VARCHAR   | tag    | dnode endpoint                                      |
| vgroup_id  | VARCHAR   | tag    | dnode id                                            |
| cluster_id | VARCHAR   | tag    | cluster id                                          |

### taos_sql_req Table

`taos_sql_req` records client-side SQL request information.

| field      | type      | is_tag | comment                                             |
| :--------- | :-------- | :----- | :-------------------------------------------------- |
| _ts        | TIMESTAMP |        | timestamp                                           |
| count      | DOUBLE    |        | number of SQL queries                               |
| result     | VARCHAR   | tag    | SQL execution result, values range: Success, Failed |
| username   | VARCHAR   | tag    | user name executing the SQL                         |
| sql_type   | VARCHAR   | tag    | SQL type, value range: select, insert, delete       |
| cluster_id | VARCHAR   | tag    | cluster id                                          |

### taos_slow_sql Table

`taos_slow_sql` records client-side slow query information.

| field      | type      | is_tag | comment                                                      |
| :--------- | :-------- | :----- | :----------------------------------------------------------- |
| _ts        | TIMESTAMP |        | timestamp                                                    |
| count      | DOUBLE    |        | number of SQL queries                                        |
| result     | VARCHAR   | tag    | SQL execution result, values range: Success, Failed          |
| username   | VARCHAR   | tag    | user name executing the SQL                                  |
| duration   | VARCHAR   | tag    | SQL execution duration, value range: 3-10s, 10-100s, 100-1000s, 1000s- |
| cluster_id | VARCHAR   | tag    | cluster id                                                   |

### taos\_slow\_sql\_detail Table

`taos_slow_sql_detail` records slow query detail information.The rule of the table name is `{user}_{db}_{ip}_clusterId_{cluster_id}`

| field         | type      | is\_tag | comment                                        |
| :------------ | :-------- | :------ | :--------------------------------------------- |
| start\_ts     | TIMESTAMP |         | sql start exec time in client, ms, primary key |
| request\_id   | UINT64_T  |         | sql request id, random hash                    |
| query\_time   | INT32_T   |         | sql exec time, ms                              |
| code          | INT32_T   |         | sql return code, 0 success                     |
| error\_info   | VARCHAR   |         | error info if sql exec failed                  |
| type          | INT8_T    |         | sql type (1-query, 2-insert, 4-others)        |
| rows\_num     | INT64_T   |         | sql result rows num                            |
| sql           | VARCHAR   |         | sql sting                                      |
| process\_name | VARCHAR   |         | process name                                   |
| process\_id   | VARCHAR   |         | process id                                     |
| db            | VARCHAR   | TAG     | which db the sql belong to                     |
| user          | VARCHAR   | TAG     | the user that exec this sql                    |
| ip            | VARCHAR   | TAG     | the client ip that exec this sql               |
| cluster\_id   | VARCHAR   | TAG     | cluster id                                     |
