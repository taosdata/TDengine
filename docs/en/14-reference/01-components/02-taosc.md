---
title: taosc Reference
sidebar_label: taosc
slug: /tdengine-reference/components/taosc
---

The TDengine client driver provides all the APIs needed for application programming and plays an important role in the distributed computing across the entire cluster. In addition to the API and its specific parameters, the behavior of the client driver can also be globally controlled through a configuration file. This section lists the configuration parameters that can be used by the TDengine client. Some parameters take effect for Native connections, while others apply to WebSocket connections. Please note the distinction when using them.

## Native Connection Configuration Parameters

The following configuration parameters only take effect for Native connections.  

### Connection Related

|Parameter Name|Supported Version|Dynamic Modification|Description|
|----------------------|----------|--------------------|-------------|
|firstEp               |                  |Supported, effective immediately  |At startup, the endpoint of the first dnode in the cluster to actively connect to, default value: hostname:6030, if the server's hostname cannot be obtained, it is assigned to localhost|
|secondEp              |                  |Supported, effective immediately  |At startup, if the firstEp cannot be connected, try to connect to the endpoint of the second dnode in the cluster, no default value|
|compressMsgSize       |                  |Supported, effective immediately  |Whether to compress RPC messages; -1: no messages are compressed; 0: all messages are compressed; N (N>0): only messages larger than N bytes are compressed; default value -1|
|shellActivityTimer    |                  |Not supported                     |The duration in seconds for the client to send heartbeats to mnode, range 1-120, default value 3|
|numOfRpcSessions      |                  |Supported, effective immediately  |Maximum number of connections supported by RPC, range 100-100000, default value 30000|
|numOfRpcThreads       |                  |Not supported                     |Number of threads for RPC to send and receive data, range 1-50, default value is half of the CPU cores|
|numOfTaskQueueThreads |                  |Not supported                     |Number of threads for the client to handle RPC messages, range 4-16, default value is half of the CPU cores|
|timeToGetAvailableConn| Cancelled after 3.3.4.*   |Not supported                     |The longest waiting time to get an available connection, range 10-50000000, in milliseconds, default value 500000|
|useAdapter            |          |Supported, effective immediately  |Internal parameter, whether to use taosadapter, affects CSV file import|
|shareConnLimit        |Added in 3.3.4.0|Not supported                     |Internal parameter, the number of queries a link can share, range 1-256, default value 10|
|readTimeout           |Added in 3.3.4.0|Not supported                     |Internal parameter, minimum timeout, range 64-604800, in seconds, default value 900|
| maxRetryWaitTime     | v3.3.4.0                        | Supported, effective after restart                           | Maximum timeout for reconnection,calculated from the time of retry,range is 3000-86400000,in milliseconds, default value 20000 |

### Query Related

|Parameter Name|Supported Version|Dynamic Modification|Description|
|----------------------|----------|--------------------|-------------|
|countAlwaysReturnValue           |         |Supported, effective immediately  |Whether the count/hyperloglog function returns a value when the input data is empty or NULL; 0: returns an empty row, 1: returns; default value 1; when this parameter is set to 1, if the query contains an INTERVAL clause or the query uses TSMA, and the corresponding group or window has empty or NULL data, the corresponding group or window will not return a query result; note that this parameter should be consistent between client and server|
|keepColumnName                   |         |Supported, effective immediately  |Automatically sets the alias to the column name (excluding the function name) when querying with Last, First, LastRow functions without specifying an alias, thus the order by clause will automatically refer to the column corresponding to the function; 1: automatically sets the alias to the column name (excluding the function name), 0: does not automatically set an alias; default value: 0|
|multiResultFunctionStarReturnTags|After 3.3.3.0|Supported, effective immediately  |When querying a supertable, whether last(\*)/last_row(\*)/first(\*) returns tag columns; when querying basic tables, subtables, it is not affected by this parameter; 0: does not return tag columns, 1: returns tag columns; default value: 0; when this parameter is set to 0, last(\*)/last_row(\*)/first(\*) only returns the ordinary columns of the supertable; when set to 1, it returns both the ordinary columns and tag columns of the supertable|
|metaCacheMaxSize                 |         |Supported, effective immediately  |Specifies the maximum size of metadata cache for a single client, in MB; default value -1, meaning unlimited|
|maxTsmaCalcDelay                 |         |Supported, effective immediately  |The allowable delay for tsma calculation by the client during query, range 600s - 86400s, i.e., 10 minutes - 1 day; default value: 600 seconds|
|tsmaDataDeleteMark               |         |Supported, effective immediately  |The retention time for intermediate results of historical data calculated by TSMA, in milliseconds; range >= 3600000, i.e., at least 1h; default value: 86400000, i.e., 1d |
|queryPolicy                      |         |Supported, effective immediately  |Execution strategy for query statements, 1: only use vnode, do not use qnode; 2: subtasks without scan operators are executed on qnode, subtasks with scan operators are executed on vnode; 3: vnode only runs scan operators, all other operators are executed on qnode; default value: 1|
|queryTableNotExistAsEmpty        |         |Supported, effective immediately  |Whether to return an empty result set when the queried table does not exist; false: returns an error; true: returns an empty result set; default value false|
|querySmaOptimize                 |         |Supported, effective immediately  |Optimization strategy for sma index, 0: do not use sma index, always query from original data; 1: use sma index, directly query from pre-calculated results for eligible statements; default value: 0|
|queryPlannerTrace                |         |Supported, effective immediately  |Internal parameter, whether the query plan outputs detailed logs|
|queryNodeChunkSize               |         |Supported, effective immediately  |Internal parameter, chunk size of the query plan|
|queryUseNodeAllocator            |         |Supported, effective immediately  |Internal parameter, allocation method of the query plan|
|queryMaxConcurrentTables         |         |Not supported                     |Internal parameter, concurrency number of the query plan|
|enableQueryHb                    |         |Supported, effective immediately  |Internal parameter, whether to send query heartbeat messages|
|minSlidingTime                   |         |Supported, effective immediately  |Internal parameter, minimum allowable value for sliding|
|minIntervalTime                  |         |Supported, effective immediately  |Internal parameter, minimum allowable value for interval|
|compareAsStrInGreatest           | v3.3.6.0 |Supported, effective immediately  |When the greatest and least functions have both numeric and string types as parameters, the comparison type conversion rules are as follows: Integer; 1: uniformly converted to string comparison, 0: uniformly converted to numeric type comparison.|
|showFullCreateTableColumn        | Added in 3.3.7.1 | Supported                          | Whether show column compress info while execute `show create table tablname`, range 0/1, default: 0.

### Writing Related

|Parameter Name|Supported Version|Dynamic Modification|Description|
|----------------------|----------|--------------------|-------------|
| smlChildTableName               |                   |Supported, effective immediately  | Key for custom child table name in schemaless, no default value |
| smlAutoChildTableNameDelimiter  |                   |Supported, effective immediately  | Delimiter between schemaless tags, concatenated as the child table name, no default value |
| smlTagName                      |                   |Supported, effective immediately  | Default tag name when schemaless tag is empty, default value "_tag_null" |
| smlTsDefaultName                |                   |Supported, effective immediately  | Configuration for setting the time column name in schemaless auto table creation, default value "_ts" |
| smlDot2Underline                |                   |Supported, effective immediately  | Converts dots in supertable names to underscores in schemaless |
| maxInsertBatchRows              |                   |Supported, effective immediately  | Internal parameter, maximum number of rows per batch insert |

### Region Related

|Parameter Name|Supported Version|Dynamic Modification|Description|
|----------------------|----------|--------------------|-------------|
| timezone       |                   |Supported, effective immediately  | Time zone; defaults to dynamically obtaining the current system time zone setting |
| locale         |                   |Supported, effective immediately  | System locale and encoding format, defaults to system settings |
| charset        |                   |Supported, effective immediately  | Character set encoding, defaults to system settings |

### Storage Related

|Parameter Name|Supported Version|Dynamic Modification|Description|
|----------------------|----------|--------------------|-------------|
| tempDir         |                   |Supported, effective immediately  | Specifies the directory for generating temporary files during operation, default on Linux platform is /tmp |
| minimalTmpDirGB |                   |Supported, effective immediately  | Minimum space required to be reserved in the directory specified by tempDir, in GB, default value: 1 |

### Log Related

|Parameter Name|Supported Version|Dynamic Modification|Description|
|----------------------|----------|--------------------|-------------|
| logDir           |                   |Not supported                     | Log file directory, operational logs will be written to this directory, default value: /var/log/taos |
| minimalLogDirGB  |                   |Supported, effective immediately  | Stops writing logs when the disk space available in the log directory is less than this value, in GB, default value: 1 |
| numOfLogLines    |                   |Supported, effective immediately  | Maximum number of lines allowed in a single log file, default value: 10,000,000 |
| asyncLog         |                   |Supported, effective immediately  | Log writing mode, 0: synchronous, 1: asynchronous, default value: 1 |
| logKeepDays      |                   |Supported, effective immediately  | Maximum retention time for log files, in days, default value: 0, meaning unlimited retention. Log files will not be renamed, nor will new log files be rolled out, but the content of the log files may continue to roll depending on the log file size setting; when set to a value greater than 0, the log file will be renamed to taoslogx.yyy, where yyy is the timestamp of the last modification of the log file, and a new log file will be rolled out,and log files whose creation time exceeds logKeepDays will be removed; Considering the usage habits of users of TDengine 2.0, starting from TDengine 3.3.6.6, when the value is set to less than 0, except that log files whose creation time exceeds -logKeepDays will be removed, other behaviors are the same as those when the value is greater than 0(For TDengine versions between 3.0.0.0 and 3.3.6.5, it is not recommended to set the value to less than 0) |
| debugFlag        |                   |Supported, effective immediately  | Log switch for running logs, 131 (output error and warning logs), 135 (output error, warning, and debug logs), 143 (output error, warning, debug, and trace logs); default value 131 or 135 (depending on the module) |
| tmrDebugFlag     |                   |Supported, effective immediately  | Log switch for the timer module, value range as above |
| uDebugFlag       |                   |Supported, effective immediately  | Log switch for the utility module, value range as above |
| rpcDebugFlag     |                   |Supported, effective immediately  | Log switch for the rpc module, value range as above |
| jniDebugFlag     |                   |Supported, effective immediately  | Log switch for the jni module, value range as above |
| qDebugFlag       |                   |Supported, effective immediately  | Log switch for the query module, value range as above |
| cDebugFlag       |                   |Supported, effective immediately  | Log switch for the client module, value range as above |
| simDebugFlag     |                   |Supported, effective immediately  | Internal parameter, log switch for the test tool, value range as above |
| tqClientDebugFlag| After 3.3.4.3     |Supported, effective immediately  | Log switch for the client module, value range as above |

### Debugging Related

|Parameter Name|Supported Version|Dynamic Modification|Description|
|----------------------|----------|--------------------|-------------|
| crashReporting   |                   |Supported, effective immediately  | Whether to upload crash to telemetry, 0: do not upload, 1: upload; default value: 1 |
| enableCoreFile   |                   |Supported, effective immediately  | Whether to generate a core file when crashing, 0: do not generate, 1: generate; default value: 1 |
| assert           |                   |Not supported                     | Assertion control switch, default value: 0 |
| configDir        |                   |Not supported                     | Directory for configuration files |
| scriptDir        |                   |Not supported                     | Internal parameter, directory for test cases |
| randErrorChance  | After 3.3.3.0     |Not supported                     | Internal parameter, used for random failure testing |
| randErrorDivisor | After 3.3.3.0     |Not supported                     | Internal parameter, used for random failure testing |
| randErrorScope   | After 3.3.3.0     |Not supported                     | Internal parameter, used for random failure testing |
| safetyCheckLevel | After 3.3.3.0     |Not supported                     | Internal parameter, used for random failure testing |
| simdEnable       | After 3.3.4.3     |Not supported                     | Internal parameter, used for testing SIMD acceleration |
| AVX512Enable     | After 3.3.4.3     |Not supported                     | Internal parameter, used for testing AVX512 acceleration |
| bypassFlag       |After 3.3.4.5      |Supported, effective immediately  | Internal parameter, used for  short-circuit testing|

### SHELL Related

|Parameter Name|Supported Version|Dynamic Modification|Description|
|----------------------|----------|--------------------|-------------|
|enableScience    |          |Not supported                     |Whether to enable scientific notation for displaying floating numbers; 0: do not enable, 1: enable; default value: 1|

## WebSocket Connection Configuration Parameters

The following configuration parameters only take effect for WebSocket connections.

|Parameter Name|Supported Version|Dynamic Modification|Description|
|----------------------|----------|--------------------|-------------|
| serverPort | `≥ v3.3.6.0` | Not supported | The port that taosAdapter listens on, default value: 6041 |
| timezone | `≥ v3.3.6.0` | Not supported | Time zone; defaults to dynamically obtaining the current system time zone setting |
| logDir | `≥ v3.3.6.0` | Not supported | Log file directory, operational logs will be written to this directory, default value: /var/log/taos |
| debugFlag | `≥ v3.3.6.0` | Not supported | Log switch for running logs, 131 (output error and warning logs), 135 (output error, warning, and debug logs), 143 (output error, warning, debug, and trace logs); default value: 131 |
| compression | `≥ v3.3.6.0` | Not supported | Enable WebSocket message compression. 0: disabled (default), 1: enabled |
| adapterList | `≥ v3.3.6.15` and `< v3.3.7.0`, or `≥ v3.3.7.4` | Not supported | List of taosAdapter addresses for load balancing and failover. Multiple addresses are comma-separated, format: `host1:port1,host2:port2,...` |
| logKeepDays | `≥ v3.3.6.28` and `< v3.3.7.0`, or `≥ v3.3.7.4` | Not supported | Maximum retention period for log files in days. When set to 0, no log files are deleted. When greater than 0, log files exceeding the size limit are renamed to taoslog.ts.gz (where ts is the last modification timestamp) and new log files are created. Log files older than the specified days are deleted; default value: 30 |
| rotationSize | `≥ v3.3.6.28` and `< v3.3.7.0`, or `≥ v3.3.7.4` | Not supported | Maximum size of a single log file (supports KB/MB/GB units), default value: 1GB |
| connRetries | `≥ v3.3.6.28` and `< v3.3.7.0`, or `≥ v3.3.7.4` | Not supported | Maximum number of retries upon connection failure, default value: 5 |
| retryBackoffMs | `≥ v3.3.6.28` and `< v3.3.7.0`, or `≥ v3.3.7.4` | Not supported | Initial wait time in milliseconds after connection failure. This value increases exponentially with consecutive failures until reaching the maximum wait time, default value: 200 |
| retryBackoffMaxMs | `≥ v3.3.6.28` and `< v3.3.7.0`, or `≥ v3.3.7.4` | Not supported | Maximum wait time in milliseconds when connection fails, default value: 2000 |
| wsTlsMode | `≥ v3.3.6.32` and `< v3.3.7.0` | Not supported | WebSocket TLS encryption mode. 0: TLS disabled by default; client auto-upgrades if server requires TLS (default); 1: TLS enabled |

## API

Please refer to [Connector](../../client-libraries/)
