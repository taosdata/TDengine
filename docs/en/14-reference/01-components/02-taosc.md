---
title: taosc Reference
sidebar_label: taosc
slug: /tdengine-reference/components/taosc
---

The TDengine client driver provides all the APIs needed for application programming and plays an important role in distributed computing across the entire cluster. The behavior of the client driver can be globally controlled not only by API and its specific parameters but also through configuration files. This section lists the configuration parameters available for the TDengine client.

## Configuration Parameters

|          Parameter Name           |                      Parameter Meaning                       |
| :-------------------------------: | :----------------------------------------------------------: |
|              firstEp              | The endpoint of the first dnode in the cluster to connect to when taos starts; default value: localhost:6030 |
|             secondEp              | If firstEp fails to connect, attempt to connect to the endpoint of the second dnode in the cluster; no default value |
|         numOfRpcSessions          | The maximum number of connections a client can create; range: 10-50000000 (in milliseconds); default value: 500000 |
|        telemetryReporting         | Whether to upload telemetry; 0: do not upload, 1: upload; default value: 1 |
|          crashReporting           | Whether to upload crash information; 0: do not upload, 1: upload; default value: 1 |
|            queryPolicy            | Execution policy for query statements; 1: use only vnode, do not use qnode; 2: non-scanning sub-tasks executed on qnode, scanning sub-tasks executed on vnode; 3: vnode only runs scanning operators, all other operators run on qnode; default value: 1 |
|         querySmaOptimize          | Optimization strategy for sma index; 0: do not use sma index, always query from raw data; 1: use sma index for qualifying statements, directly query from pre-calculated results; default value: 0 |
|          keepColumnName           | For Last, First, LastRow function queries without specified aliases, automatically set the alias to the column name (excluding the function name), so if the order by clause references the column name, it will automatically reference the corresponding function; 1: automatically set alias to column name (excluding function name), 0: do not automatically set alias; default value: 0 |
|      countAlwaysReturnValue       | Whether to return a value for count/hyperloglog functions when the input data is empty or NULL; 0: return empty row, 1: return; default value: 1; when set to 1, if the query contains an INTERVAL clause or uses TSMA, and the corresponding group or window data is empty or NULL, that group or window will not return query results. Note that this parameter should be consistent between the client and server. |
| multiResultFunctionStarReturnTags | When querying supertables, whether last(*), last_row(*), first(*) should return tag columns; this parameter does not affect queries on basic tables or subtables; 0: do not return tag columns, 1: return tag columns; default value: 0; when set to 0, last(*), last_row(*), first(*) will only return ordinary columns of the supertable; when set to 1, it will return both ordinary columns and tag columns of the supertable |
|         maxTsmaCalcDelay          | The allowable tsma calculation delay for the client during queries; if the tsma calculation delay exceeds this value, that TSMA will not be used; range: 600s - 86400s (i.e., 10 minutes to 1 hour); default value: 600 seconds |
|        tsmaDataDeleteMark         | The retention time for historical intermediate results of TSMA calculations; unit: milliseconds; range: >= 3600000 (i.e., at least 1 hour); default value: 86400000 (i.e., 1 day) |
|             timezone              | Time zone; defaults to dynamically obtaining the current time zone setting from the system |
|              locale               | System locale information and encoding format; defaults to dynamically obtaining from the system |
|              charset              | Character set encoding; defaults to dynamically obtaining from the system |
|         metaCacheMaxSize          | Maximum size of metadata cache for a single client; unit: MB; default value: -1 (unlimited) |
|              logDir               | Log file directory; client runtime logs will be written to this directory; default value: /var/log/taos |
|          minimalLogDirGB          | Stops logging when the available space in the disk where the log directory is located is less than this value; default value: 1 |
|           numOfLogLines           | Maximum number of lines allowed in a single log file; default value: 10,000,000 |
|             asyncLog              | Whether to asynchronously write logs; 0: synchronous; 1: asynchronous; default value: 1 |
|            logKeepDays            | Maximum retention time for log files; default value: 0 (indefinite retention); when greater than 0, log files will be renamed as taosdlog.xxx, where xxx is the timestamp of the last modification of the log file |
|         smlChildTableName         | Key for custom subtable names in schemaless mode; no default value |
|  smlAutoChildTableNameDelimiter   | Delimiter between tags in schemaless mode, combined to form the subtable name; no default value |
|            smlTagName             | Default tag name when schemaless tags are empty; default value: "_tag_null" |
|         smlTsDefaultName          | The name of the time column in automatically created schemaless tables is set through this configuration; default value: "_ts" |
|          enableCoreFile           | Whether to generate a core file in case of a crash; 0: do not generate, 1: generate; default value: 1 |
|           enableScience           | Whether to enable scientific notation for floating-point numbers; 0: disable, 1: enable; default value: 1 |
|          compressMsgSize          | Whether to compress RPC messages; -1: do not compress any messages; 0: compress all messages; N (N>0): compress only messages larger than N bytes; default value: -1 |
|     queryTableNotExistAsEmpty     | Whether to return an empty result set when the queried table does not exist; false: return an error; true: return an empty result set; default value: false |
| numOfRpcThreads        | The number of threads for RPC data transmission; range: 1-50, default value: half of the CPU cores |
| numOfTaskQueueThreads  | The number of threads for the client to process RPC messages, range: 4-16, default value: half of the CPU cores  |
| shareConnLimit         | The number of requests that a connection can share; range: 1-512; default value: 10 |
| readTimeout            | The minimum timeout for a single request; range: 64-604800; unit: seconds; default value: 900 |

## API

Please refer to [Client Libraries](../../client-libraries/)
