---
title: "TDengine Configuration Parameter Scope Comparison"
sidebar_label: "Configuration Parameter Scope"
slug: /tdengine-reference/components/configuration-scope
---

This document compares the configuration parameters of taosd (server-side) and taosc (client-side) in TDengine TSDB, clearly identifying the scope of each parameter.

## Configuration Parameter Scope Comparison Table

| Parameter Name | Scope | Description |
|---------------|-------|-------------|
| **Connection Related** | | |
| firstEp | both | Endpoint of the first dnode in the cluster that taosd actively connects to at startup |
| secondEp | both | Endpoint of the second dnode in the cluster that taosd tries to connect to if the firstEp is unreachable |
| fqdn | taosd | The service address that taosd listens on |
| serverPort | both | The port that taosd listens on |
| compressMsgSize | both | Whether to compress RPC messages |
| shellActivityTimer | both | Duration in seconds for the client to send heartbeat to mnode |
| numOfRpcSessions | both | Maximum number of connections supported by RPC |
| numOfRpcThreads | both | Number of threads for receiving and sending RPC data |
| numOfTaskQueueThreads | both | Number of threads for processing RPC messages |
| rpcQueueMemoryAllowed | taosd | Maximum memory allowed for received RPC messages in dnode |
| resolveFQDNRetryTime | taosd | Number of retries when FQDN resolution fails |
| timeToGetAvailableConn | taosc | Maximum waiting time to get an available connection |
| maxShellConns | taosd | Maximum number of connections allowed |
| maxRetryWaitTime | both | Maximum timeout for reconnection, calculated from the time of retry |
| shareConnLimit | both | Number of requests a connection can share |
| readTimeout | both | Minimum timeout for a single request |
| useAdapter | taosc | Whether to use taosadapter, affects CSV file import |
| **Monitoring Related** | | |
| monitor | taosd | Whether to collect and report monitoring data |
| monitorFqdn | taosd | The FQDN of the server where the taosKeeper service is located |
| monitorPort | taosd | The port number listened to by the taosKeeper service |
| monitorInterval | taosd | The time interval for recording system parameters (CPU/memory) in the monitoring database |
| monitorMaxLogs | taosd | Number of cached logs pending report |
| monitorComp | taosd | Whether to use compression when reporting monitoring logs |
| monitorLogProtocol | taosd | Whether to print monitoring logs |
| monitorForceV2 | taosd | Whether to use V2 protocol for reporting |
| telemetryReporting | taosd | Whether to upload telemetry |
| telemetryServer | taosd | Telemetry server address |
| telemetryPort | taosd | Telemetry server port number |
| telemetryInterval | taosd | Telemetry upload interval |
| crashReporting | both | Whether to upload crash information |
| enableMetrics | taosd | Whether to open write diagnostic tools, collect and upload write metrics |
| metricsInterval | taosd | Interval for write diagnostic tools to upload write metrics |
| metricsLevel | taosd | Level of write metrics uploaded by write diagnostic tools |
| **Query Related** | | |
| countAlwaysReturnValue | both | Whether count/hyperloglog functions return a value when input data is empty or NULL |
| tagFilterCache | taosd | Whether to cache tag filter results |
| stableTagFilterCache | taosd | Whether to cache tag equal condition filter results.  It will not become invalid due to adding or deleting child tables or updating the tag values or modifying super table tags. |
| queryBufferSize | taosd | Query available cache size |
| queryRspPolicy | taosd | Query response strategy |
| queryUseMemoryPool | taosd | Whether query will use memory pool to manage memory |
| minReservedMemorySize | taosd | The minimum reserved system available memory size |
| singleQueryMaxMemorySize | taosd | The memory limit that a single query can use on a single node (dnode) |
| filterScalarMode | taosd | Force scalar filter mode |
| queryNoFetchTimeoutSec | taosd | Timeout when application does not FETCH data for a long time during query |
| queryPlannerTrace | both | Whether the query plan outputs detailed logs |
| queryNodeChunkSize | both | Chunk size of the query plan |
| queryUseNodeAllocator | both | Allocation method of the query plan |
| queryMaxConcurrentTables | both | Allocation method of the query plan |
| queryRsmaTolerance | taosd | Allocation method of the query plan |
| enableQueryHb | both | Whether to send query heartbeat messages |
| pqSortMemThreshold | taosd | Memory threshold for sorting |
| keepColumnName | taosc | Automatically sets the alias to the column name when querying with Last, First, LastRow functions |
| multiResultFunctionStarReturnTags | taosc | Whether last(*)/last_row(*)/first(*) returns tag columns when querying a supertable |
| metaCacheMaxSize | taosc | Specifies the maximum size of metadata cache for a single client |
| maxTsmaCalcDelay | taosc | The allowable delay for tsma calculation by the client during query |
| tsmaDataDeleteMark | taosc | The retention time for intermediate results of historical data calculated by TSMA |
| queryPolicy | taosc | Execution strategy for query statements |
| queryTableNotExistAsEmpty | taosc | Whether to return an empty result set when the queried table does not exist |
| querySmaOptimize | taosc | Optimization strategy for sma index |
| queryMaxConcurrentTables | taosc | Concurrency number of the query plan |
| minSlidingTime | taosc | Minimum allowable value for sliding |
| minIntervalTime | taosc | Minimum allowable value for interval |
| compareAsStrInGreatest | taosc | Comparison type conversion rules for greatest and least functions |
| showFullCreateTableColumn | taosc | Whether show create table returns column compression information |
| rpcRecvLogThreshold| taosd| The threshold for warning logs in the RPC module |
| **Region Related** | | |
| timezone | both | Time zone |
| locale | both | System locale information and encoding format |
| charset | both | Character set encoding |
| **Storage Related** | | |
| dataDir | taosd | Directory for data files, all data files are written to this directory |
| diskIDCheckEnabled | taosd | Whether to check if the disk id of dataDir has changed when restarting dnode |
| tempDir | both | Specifies the directory for generating temporary files during system operation |
| minimalDataDirGB | taosd | Minimum space to be reserved in the time-series data storage directory specified by dataDir |
| minimalTmpDirGB | both | Minimum space to be reserved in the temporary file directory specified by tempDir |
| minDiskFreeSize | taosd | When the available space on a disk is less than or equal to this threshold, the disk will no longer be selected |
| ssAutoMigrateIntervalSec | taosd | Trigger cycle for automatic upload of local data files to shared storage |
| ssEnabled | taosd | Whether to enable shared storage |
| ssAccessString | taosd | A string which contains various options for accessing the shared storage |
| ssPageCacheSize | taosd | Number of shared storage page cache pages |
| ssUploadDelaySec | taosd | How long a data file remains unchanged before being uploaded to shared storage |
| cacheLazyLoadThreshold | taosd | Cache loading strategy |
| **Cluster Related** | | |
| supportVnodes | taosd | Maximum number of vnodes supported by a dnode |
| numOfCommitThreads | taosd | Maximum number of commit threads |
| numOfCompactThreads | taosd | Maximum number of compact threads |
| numOfMnodeReadThreads | taosd | Number of Read threads for mnode |
| numOfVnodeQueryThreads | taosd | Number of Query threads for vnode |
| numOfVnodeFetchThreads | taosd | Number of Fetch threads for vnode |
| numOfVnodeRsmaThreads | taosd | Number of Rsma threads for vnode |
| numOfQnodeQueryThreads | taosd | Number of Query threads for qnode |
| ttlUnit | taosd | Unit for ttl parameter |
| ttlPushInterval | taosd | Frequency of ttl timeout checks |
| ttlChangeOnWrite | taosd | Whether ttl expiration time changes with table modification |
| ttlBatchDropNum | taosd | Number of subtables deleted in a batch for ttl |
| retentionSpeedLimitMB | taosd | Speed limit for data migration across different levels of disks |
| maxTsmaNum | taosd | Maximum number of TSMAs that can be created in the cluster |
| tmqMaxTopicNum | taosd | Maximum number of topics that can be established for subscription |
| tmqRowSize | taosd | Maximum number of records in a subscription data block |
| audit | taosd | Audit feature switch |
| auditInterval | taosd | Time interval for reporting audit data |
| auditCreateTable | taosd | Whether to enable audit feature for creating subtables |
| encryptAlgorithm | taosd | Data encryption algorithm |
| encryptScope | taosd | Encryption scope |
| encryptPassAlgorithm | taosd | Switch for saving user password as encrypted string |
| enableWhiteList | taosd | Switch for whitelist feature |
| syncLogBufferMemoryAllowed | taosd | Maximum memory allowed for sync log cache messages for a dnode |
| syncApplyQueueSize | taosd | Size of apply queue for sync log |
| syncElectInterval | taosd | Internal parameter, for debugging synchronization module |
| syncHeartbeatInterval | taosd | Internal parameter, for debugging synchronization module |
| syncHeartbeatTimeout | taosd | Internal parameter, for debugging synchronization module |
| syncSnapReplMaxWaitN | taosd | Internal parameter, for debugging synchronization module |
| arbHeartBeatIntervalSec | taosd | Internal parameter, for debugging synchronization module |
| arbCheckSyncIntervalSec | taosd | Internal parameter, for debugging synchronization module |
| arbSetAssignedTimeoutSec | taosd | Internal parameter, for debugging synchronization module |
| mndLogRetention | taosd | Internal parameter, for debugging mnode module |
| skipGrant | taosd | Internal parameter, for authorization checks |
| trimVDbIntervalSec | taosd | Internal parameter, for deleting expired data |
| ttlFlushThreshold | taosd | Internal parameter, frequency of ttl timer |
| compactPullupInterval | taosd | Internal parameter, frequency of data reorganization timer |
| walFsyncDataSizeLimit | taosd | Internal parameter, threshold for WAL to perform FSYNC |
| walForceRepair | taosd | Internal parameter, repair WAL file forcibly |
| transPullupInterval | taosd | Internal parameter, retry interval for mnode to execute transactions |
| forceKillTrans | taosd | Internal parameter, for debugging mnode transaction module |
| mqRebalanceInterval | taosd | Internal parameter, interval for consumer rebalancing |
| uptimeInterval | taosd | Internal parameter, for recording system uptime |
| timeseriesThreshold | taosd | Internal parameter, for usage statistics |
| udf | taosd | Whether to start UDF service |
| udfdResFuncs | taosd | Internal parameter, for setting UDF result sets |
| udfdLdLibPath | taosd | Internal parameter, indicates the library path for loading UDF |
| enableStrongPassword | taosd | Password requirements for strong password validation |
| **Stream Computing Parameters** | | |
| numOfMnodeStreamMgmtThreads | taosd | Number of mnode stream computing management threads |
| numOfStreamMgmtThreads | taosd | Number of snode stream computing management threads |
| numOfVnodeStreamReaderThreads | taosd | Number of vnode stream computing read threads |
| numOfStreamTriggerThreads | taosd | Number of stream computing trigger threads |
| numOfStreamRunnerThreads | taosd | Number of stream computing execution threads |
| streamBufferSize | taosd | Maximum cache size that stream computing can use |
| streamNotifyMessageSize | taosd | Controls the message size for event notifications |
| streamNotifyFrameSize | taosd | Controls the underlying frame size when sending event notification messages |
| **Log Related** | | |
| logDir | both | Log file directory, operational logs will be written to this directory |
| minimalLogDirGB | both | Stops writing logs when the available space on the disk where the log folder is located is less than this value |
| numOfLogLines | both | Maximum number of lines allowed in a single log file |
| asyncLog | both | Log writing mode |
| logKeepDays | both | Maximum retention time for log files |
| slowLogThreshold | taosd | Slow query threshold, queries taking longer than or equal to this threshold are considered slow |
| slowLogMaxLen | taosd | Maximum length of slow query logs |
| slowLogScope | taosd | Type of slow query records |
| slowLogExceptDb | taosd | Specifies the database that does not report slow queries |
| debugFlag | both | Log switch for running logs |
| tmrDebugFlag | both | Log switch for the timer module |
| uDebugFlag | both | Log switch for the utility module |
| rpcDebugFlag | both | Log switch for the rpc module |
| qDebugFlag | both | Log switch for the query module |
| dDebugFlag | taosd | Log switch for the dnode module |
| vDebugFlag | taosd | Log switch for the vnode module |
| mDebugFlag | taosd | Log switch for the mnode module |
| azDebugFlag | taosd | Log switch for the S3 module |
| sDebugFlag | taosd | Log switch for the sync module |
| tsdbDebugFlag | taosd | Log switch for the tsdb module |
| tqDebugFlag | taosd | Log switch for the tq module |
| fsDebugFlag | taosd | Log switch for the fs module |
| udfDebugFlag | taosd | Log switch for the udf module |
| smaDebugFlag | taosd | Log switch for the sma module |
| idxDebugFlag | taosd | Log switch for the index module |
| tdbDebugFlag | taosd | Log switch for the tdb module |
| metaDebugFlag | taosd | Log switch for the meta module |
| stDebugFlag | taosd | Log switch for the stream module |
| sndDebugFlag | taosd | Log switch for the snode module |
| jniDebugFlag | taosc | Log switch for the jni module |
| cDebugFlag | taosc | Log switch for the client module |
| simDebugFlag | taosc | Internal parameter, log switch for the test tool |
| tqClientDebugFlag | taosc | Log switch for the client module |
| **Debugging Related** | | |
| enableCoreFile | both | Whether to generate a core file when crashing |
| configDir | both | Directory where the configuration files are located |
| forceReadConfig | taosd | Whether to force reading configuration from file |
| scriptDir | both | Directory for internal test tool scripts |
| assert | both | Assertion control switch |
| randErrorChance | both | Internal parameter, used for random failure testing |
| randErrorDivisor | both | Internal parameter, used for random failure testing |
| randErrorScope | both | Internal parameter, used for random failure testing |
| safetyCheckLevel | both | Internal parameter, used for random failure testing |
| experimental | taosd | Internal parameter, used for some experimental features |
| simdEnable | both | Internal parameter, used for testing SIMD acceleration |
| AVX512Enable | both | Internal parameter, used for testing AVX512 acceleration |
| rsyncPort | taosd | Internal parameter, used for debugging stream computing |
| snodeAddress | taosd | Internal parameter, used for debugging stream computing |
| checkpointBackupDir | taosd | Internal parameter, used for restoring snode data |
| enableAuditDelete | taosd | Internal parameter, used for testing audit functions |
| slowLogThresholdTest | taosd | Internal parameter, used for testing slow logs |
| bypassFlag | both | Internal parameter, used for short-circuit testing |
| **Compression Parameters** | | |
| fPrecision | taosd | Sets the compression precision for float type floating numbers |
| dPrecision | taosd | Sets the compression precision for double type floating numbers |
| lossyColumn | taosd | Enables TSZ lossy compression for float and/or double types |
| ifAdtFse | taosd | When TSZ lossy compression is enabled, use the FSE algorithm instead of the HUFFMAN algorithm |
| enableIpv6 | taosd | Force nodes to communicate directly via IPv6 only |
| maxRange | taosd | Internal parameter, used for setting lossy compression |
| curRange | taosd | Internal parameter, used for setting lossy compression |
| compressor | taosd | Internal parameter, used for setting lossy compression |
| **Writing Related** | | |
| smlChildTableName | taosc | Key for custom child table name in schemaless |
| smlAutoChildTableNameDelimiter | taosc | Delimiter between schemaless tags, concatenated as the child table name |
| smlTagName | taosc | Default tag name when schemaless tag is empty |
| smlTsDefaultName | taosc | Configuration for setting the time column name in schemaless auto table creation |
| smlDot2Underline | taosc | Converts dots in supertable names to underscores in schemaless |
| maxInsertBatchRows | taosc | Maximum number of rows per batch insert |
| **SHELL Related** | | |
| enableScience | taosc | Whether to enable scientific notation for displaying floating numbers |
| **WebSocket Related** | | |
| serverPort | taosc | The port that taosAdapter listens on |
| timezone | taosc | Time zone |
| logDir | taosc | Log file directory, operational logs will be written to this directory |
| logKeepDays | taosc | Maximum retention period for log files in days |
| rotationCount | taosc | Number of log file rotations before deletion |
| rotationSize | taosc | Maximum size of a single log file |
| compression | taosc | Enable WebSocket message compression |
| adapterList | taosc | List of taosAdapter addresses for load balancing and failover |
| connRetries | taosc | Maximum number of retries upon connection failure |
| retryBackoffMs | taosc | Initial wait time in milliseconds after connection failure |
| retryBackoffMaxMs | taosc | Maximum wait time in milliseconds when connection fails |

## Description

- **taosd**: Configuration parameters that only take effect on the server side
- **taosc**: Configuration parameters that only take effect on the client side  
- **both**: Configuration parameters that take effect on both server and client sides