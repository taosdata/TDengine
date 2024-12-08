---
title: taosc Reference
sidebar_label: taosc
slug: /tdengine-reference/components/taosc
---

The TDengine client driver provides all the APIs needed for application programming and plays an important role in the distributed computing across the entire cluster. In addition to the API and its specific parameters, the behavior of the client driver can also be globally controlled through a configuration file. This section lists the configuration parameters that can be used by the TDengine client.

## Configuration Parameters

### Connection Related

|Parameter Name|Supported Version|Description|
|----------------------|----------|-------------|
|firstEp               |                  |At startup, the endpoint of the first dnode in the cluster to actively connect to, default value: hostname:6030, if the server's hostname cannot be obtained, it is assigned to localhost|
|secondEp              |                  |At startup, if the firstEp cannot be connected, try to connect to the endpoint of the second dnode in the cluster, no default value|
|compressMsgSize       |                  |Whether to compress RPC messages; -1: no messages are compressed; 0: all messages are compressed; N (N>0): only messages larger than N bytes are compressed; default value -1|
|shellActivityTimer    |                  |The duration in seconds for the client to send heartbeats to mnode, range 1-120, default value 3|
|numOfRpcSessions      |                  |Maximum number of connections supported by RPC, range 100-100000, default value 30000|
|numOfRpcThreads       |                  |Number of threads for RPC to send and receive data, range 1-50, default value is half of the CPU cores|
|numOfTaskQueueThreads |                  |Number of threads for the client to handle RPC messages, range 4-16, default value is half of the CPU cores|
|timeToGetAvailableConn| Cancelled after 3.3.4.*   |The longest waiting time to get an available connection, range 10-50000000, in milliseconds, default value 500000|
|useAdapter            |          |Internal parameter, whether to use taosadapter, affects CSV file import|
|shareConnLimit        |Added in 3.3.4.0|Internal parameter, the number of queries a link can share, range 1-256, default value 10|
|readTimeout           |Added in 3.3.4.0|Internal parameter, minimum timeout, range 64-604800, in seconds, default value 900|

### Query Related

|Parameter Name|Supported Version|Description|
|---------------------------------|---------|-|
|countAlwaysReturnValue           |         |Whether the count/hyperloglog function returns a value when the input data is empty or NULL; 0: returns an empty row, 1: returns; default value 1; when this parameter is set to 1, if the query contains an INTERVAL clause or the query uses TSMA, and the corresponding group or window has empty or NULL data, the corresponding group or window will not return a query result; note that this parameter should be consistent between client and server|
|keepColumnName                   |         |Automatically sets the alias to the column name (excluding the function name) when querying with Last, First, LastRow functions without specifying an alias, thus the order by clause will automatically refer to the column corresponding to the function; 1: automatically sets the alias to the column name (excluding the function name), 0: does not automatically set an alias; default value: 0|
|multiResultFunctionStarReturnTags|After 3.3.3.0|When querying a supertable, whether last(\*)/last_row(\*)/first(\*) returns tag columns; when querying basic tables, subtables, it is not affected by this parameter; 0: does not return tag columns, 1: returns tag columns; default value: 0; when this parameter is set to 0, last(\*)/last_row(\*)/first(\*) only returns the ordinary columns of the supertable; when set to 1, it returns both the ordinary columns and tag columns of the supertable|
|metaCacheMaxSize                 |         |Specifies the maximum size of metadata cache for a single client, in MB; default value -1, meaning unlimited|
|maxTsmaCalcDelay                 |         |The allowable delay for tsma calculation by the client during query, range 600s - 86400s, i.e., 10 minutes - 1 day; default value: 600 seconds|
|tsmaDataDeleteMark               |         |The retention time for intermediate results of historical data calculated by TSMA, in milliseconds; range >= 3600000, i.e., at least 1h; default value: 86400000, i.e., 1d |
|queryPolicy                      |         |Execution strategy for query statements, 1: only use vnode, do not use qnode; 2: subtasks without scan operators are executed on qnode, subtasks with scan operators are executed on vnode; 3: vnode only runs scan operators, all other operators are executed on qnode; default value: 1|
|queryTableNotExistAsEmpty        |         |Whether to return an empty result set when the queried table does not exist; false: returns an error; true: returns an empty result set; default value false|
|querySmaOptimize                 |         |Optimization strategy for sma index, 0: do not use sma index, always query from original data; 1: use sma index, directly query from pre-calculated results for eligible statements; default value: 0|
|queryPlannerTrace                |         |Internal parameter, whether the query plan outputs detailed logs|
|queryNodeChunkSize               |         |Internal parameter, chunk size of the query plan|
|queryUseNodeAllocator            |         |Internal parameter, allocation method of the query plan|
|queryMaxConcurrentTables         |         |Internal parameter, concurrency number of the query plan|
|enableQueryHb                    |         |Internal parameter, whether to send query heartbeat messages|
|minSlidingTime                   |         |Internal parameter, minimum allowable value for sliding|
|minIntervalTime                  |         |Internal parameter, minimum allowable value for interval|

### Writing Related

| Parameter Name                  | Supported Version | Description |
|---------------------------------|-------------------|-------------|
| smlChildTableName               |                   | Key for custom child table name in schemaless, no default value |
| smlAutoChildTableNameDelimiter  |                   | Delimiter between schemaless tags, concatenated as the child table name, no default value |
| smlTagName                      |                   | Default tag name when schemaless tag is empty, default value "_tag_null" |
| smlTsDefaultName                |                   | Configuration for setting the time column name in schemaless auto table creation, default value "_ts" |
| smlDot2Underline                |                   | Converts dots in supertable names to underscores in schemaless |
| maxInsertBatchRows              |                   | Internal parameter, maximum number of rows per batch insert |

### Region Related

| Parameter Name | Supported Version | Description |
|----------------|-------------------|-------------|
| timezone       |                   | Time zone; defaults to dynamically obtaining the current system time zone setting |
| locale         |                   | System locale and encoding format, defaults to system settings |
| charset        |                   | Character set encoding, defaults to system settings |

### Storage Related

| Parameter Name  | Supported Version | Description |
|-----------------|-------------------|-------------|
| tempDir         |                   | Specifies the directory for generating temporary files during operation, default on Linux platform is /tmp |
| minimalTmpDirGB |                   | Minimum space required to be reserved in the directory specified by tempDir, in GB, default value: 1 |

### Log Related

| Parameter Name   | Supported Version | Description |
|------------------|-------------------|-------------|
| logDir           |                   | Log file directory, operational logs will be written to this directory, default value: /var/log/taos |
| minimalLogDirGB  |                   | Stops writing logs when the disk space available in the log directory is less than this value, in GB, default value: 1 |
| numOfLogLines    |                   | Maximum number of lines allowed in a single log file, default value: 10,000,000 |
| asyncLog         |                   | Log writing mode, 0: synchronous, 1: asynchronous, default value: 1 |
| logKeepDays      |                   | Maximum retention time for log files, in days, default value: 0, meaning unlimited retention. Log files will not be renamed, nor will new log files be rolled out, but the content of the log files may continue to roll depending on the log file size setting; when set to a value greater than 0, the log file will be renamed to taoslogx.yyy, where yyy is the timestamp of the last modification of the log file, and a new log file will be rolled out |
| debugFlag        |                   | Log switch for running logs, 131 (output error and warning logs), 135 (output error, warning, and debug logs), 143 (output error, warning, debug, and trace logs); default value 131 or 135 (depending on the module) |
| tmrDebugFlag     |                   | Log switch for the timer module, value range as above |
| uDebugFlag       |                   | Log switch for the utility module, value range as above |
| rpcDebugFlag     |                   | Log switch for the rpc module, value range as above |
| jniDebugFlag     |                   | Log switch for the jni module, value range as above |
| qDebugFlag       |                   | Log switch for the query module, value range as above |
| cDebugFlag       |                   | Log switch for the client module, value range as above |
| simDebugFlag     |                   | Internal parameter, log switch for the test tool, value range as above |
| tqClientDebugFlag| After 3.3.4.3     | Log switch for the client module, value range as above |

### Debugging Related

| Parameter Name   | Supported Version | Description |
|------------------|-------------------|-------------|
| crashReporting   |                   | Whether to upload crash to telemetry, 0: do not upload, 1: upload; default value: 1 |
| enableCoreFile   |                   | Whether to generate a core file when crashing, 0: do not generate, 1: generate; default value: 1 |
| assert           |                   | Assertion control switch, default value: 0 |
| configDir        |                   | Directory for configuration files |
| scriptDir        |                   | Internal parameter, directory for test cases |
| randErrorChance  | After 3.3.3.0     | Internal parameter, used for random failure testing |
| randErrorDivisor | After 3.3.3.0     | Internal parameter, used for random failure testing |
| randErrorScope   | After 3.3.3.0     | Internal parameter, used for random failure testing |
| safetyCheckLevel | After 3.3.3.0     | Internal parameter, used for random failure testing |
| simdEnable       | After 3.3.4.3     | Internal parameter, used for testing SIMD acceleration |
| AVX512Enable     | After 3.3.4.3     | Internal parameter, used for testing AVX512 acceleration |

### SHELL Related

|Parameter Name|Supported Version|Description|
|-----------------|----------|-|
|enableScience    |          |Whether to enable scientific notation for displaying floating numbers; 0: do not enable, 1: enable; default value: 1|

## API

Please refer to [Connector](../../client-libraries/)
