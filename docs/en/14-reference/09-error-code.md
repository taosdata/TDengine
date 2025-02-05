---
sidebar_label: Error Codes
title: Error Code Reference
slug: /tdengine-reference/error-codes
---

This document details the server error codes that may be encountered when using the TDengine client and the corresponding actions to be taken. All language connectors will also return these error codes to the caller when using the native connection method.

## rpc

| Error Code | Error Description                            | Possible Error Scenarios or Reasons                          | Recommended User Actions                                     |
| ---------- | -------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 0x8000000B | Unable to establish connection               | 1. Network is unreachable 2. Multiple retries, still unable to perform request | 1. Check network 2. Analyze logs, specific reasons are more complex |
| 0x80000013 | Client and server's time is not synchronized | 1. Client and server are not in the same time zone 2. Client and server are in the same time zone, but their times are not synchronized, differing by more than 900 seconds | 1. Adjust to the same time zone 2. Calibrate client and server time |
| 0x80000015 | Unable to resolve FQDN                       | Invalid fqdn set                                             | Check fqdn settings                                          |
| 0x80000017 | Port already in use                          | The port is already occupied by some service, and the newly started service still tries to bind to that port | 1. Change the server port of the new service 2. Kill the service that previously occupied the port |
| 0x80000018 | Conn is broken                               | Due to network jitter or request time being too long (over 900 seconds), the system actively disconnects | 1. Set the system's maximum timeout duration 2. Check request duration |
| 0x80000019 | Conn read timeout                            | 1. The request processing time is too long 2. The server is overwhelmed 3. The server is deadlocked | 1. Explicitly configure the readTimeout parameter 2. Analyze the stack on taos | 
| 0x80000020 | some vnode/qnode/mnode(s) out of service     | After multiple retries, still unable to connect to the cluster, possibly all nodes have crashed, or the surviving nodes are not Leader nodes | 1. Check the status of taosd, analyze the reasons for taosd crash 2. Analyze why the surviving taosd cannot elect a Leader |
| 0x80000021 | some vnode/qnode/mnode(s) conn is broken     | After multiple retries, still unable to connect to the cluster, possibly due to network issues, request time too long, server deadlock, etc. | 1. Check network 2. Request execution time                   |
| 0x80000022 | rpc open too many session                    | 1. High concurrency causing the number of occupied connections to reach the limit 2. Server BUG, causing connections not to be released | 1. Adjust configuration parameter numOfRpcSessions 2. Adjust configuration parameter timeToGetAvailableConn 3. Analyze reasons for server not releasing connections |
| 0x80000023  | RPC network error                           | 1. Network issues, possibly intermittent 2. Server crash     | 1. Check the network 2. Check if the server has restarted                 |
| 0x80000024  | RPC network bus                             | 1. When pulling data between clusters, no available connections are obtained, or the number of connections has reached the limit | 1. Check if the concurrency is too high 2. Check if there are any anomalies in the cluster nodes, such as deadlocks |
| 0x80000025  | HTTP-report already quit                    | 1. Issues with HTTP reporting                                | Internal issue, can be ignored                                            |
| 0x80000026  | RPC module already quit                     | 1. The client instance has already exited, but still uses the instance for queries | Check the business code to see if there is a mistake in usage             |
| 0x80000027  | RPC async module already quit               | 1. Engine error, can be ignored, this error code will not be returned to the user side | If returned to the user side, the engine side needs to investigate the issue |
| 0x80000028  | RPC async in process                        | 1. Engine error, can be ignored, this error code will not be returned to the user side | If returned to the user side, the engine side needs to investigate the issue |
| 0x80000029  | RPC no state                                | 1. Engine error, can be ignored, this error code will not be returned to the user side | If returned to the user side, the engine side needs to investigate the issue |
| 0x8000002A  | RPC state already dropped                   | 1. Engine error, can be ignored, this error code will not be returned to the user side | If returned to the user side, the engine side needs to investigate the issue |
| 0x8000002B  | RPC msg exceed limit                        | 1. Single RPC message exceeds the limit, this error code will not be returned to the user side | If returned to the user side, the engine side needs to investigate the issue |


## common  

| Error Code | Error Description                 | Possible Error Scenarios or Reasons                          | Recommended User Actions                                     |
| ---------- | --------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 0x80000100 | Operation not supported           | Operation not supported, disallowed scenarios                | Check if the operation is incorrect, confirm if the feature is supported |
| 0x80000102 | Out of Memory                     | Client or server memory allocation failure scenarios         | Check if client, server memory is sufficient                 |
| 0x80000104 | Data file corrupted               | 1. Storage data file damaged 2. udf file cannot be created   | 1. Contact Taos customer support 2. Confirm server has read/write/create file permissions for the temporary directory |
| 0x80000106 | too many Ref Objs                 | No available ref resources                                   | Preserve the scene and logs, report issue on github          |
| 0x80000107 | Ref ID is removed                 | The referenced ref resource has been released                | Preserve the scene and logs, report issue on github          |
| 0x80000108 | Invalid Ref ID                    | Invalid ref ID                                               | Preserve the scene and logs, report issue on github          |
| 0x8000010A | Ref is not there                  | ref information does not exist                               | Preserve the scene and logs, report issue on github          |
| 0x80000110 | Unexpected generic error          | System internal error                                        | Preserve the scene and logs, report issue on github          |
| 0x80000111 | Action in progress                | Operation in progress                                        | 1. Wait for the operation to complete 2. Cancel the operation if necessary 3. If it exceeds a reasonable time and still not completed, preserve the scene and logs, or contact customer support |
| 0x80000112 | Out of range                      | Configuration parameter exceeds allowed value range          | Change the parameter                                         |
| 0x80000115 | Invalid message                   | Incorrect message                                            | 1. Check for version inconsistency between nodes 2. Preserve the scene and logs, report issue on github |
| 0x80000116 | Invalid message len               | Incorrect message length                                     | 1. Check for version inconsistency between nodes 2. Preserve the scene and logs, report issue on github |
| 0x80000117 | Invalid pointer                   | Invalid pointer                                              | Preserve the scene and logs, report issue on github          |
| 0x80000118 | Invalid parameters                | Invalid parameters                                           | Preserve the scene and logs, report issue on github          |
| 0x80000119 | Invalid config option             | Invalid configuration                                        | Preserve the scene and logs, report issue on github          |
| 0x8000011A | Invalid option                    | Invalid option                                               | Preserve the scene and logs, report issue on github          |
| 0x8000011B | Invalid json format               | JSON format error                                            | Preserve the scene and logs, report issue on github          |
| 0x8000011C | Invalid version number            | Invalid version format                                       | Preserve the scene and logs, report issue on github          |
| 0x8000011D | Invalid version string            | Invalid version format                                       | Preserve the scene and logs, report issue on github          |
| 0x8000011E | Version not compatible            | Version incompatibility between nodes                        | Check versions of all nodes (including server and client), ensure node versions are consistent or compatible |
| 0x8000011F | Checksum error                    | File checksum verification failed                            | Preserve the scene and logs, report issue on github          |
| 0x80000120 | Failed to compress msg            | Compression failed                                           | Preserve the scene and logs, report issue on github          |
| 0x80000121 | Message not processed             | Message not correctly processed                              | Preserve the scene and logs, report issue on github          |
| 0x80000122 | Config not found                  | Configuration item not found                                 | Preserve the scene and logs, report issue on github          |
| 0x80000123 | Repeat initialization             | Repeated initialization                                      | Preserve the scene and logs, report issue on github          |
| 0x80000124 | Cannot add duplicate keys to hash | Adding duplicate key data to hash table                      | Preserve the scene and logs, report issue on github          |
| 0x80000125 | Retry needed                      | Application needs to retry                                   | Application should retry according to API usage specifications |
| 0x80000126 | Out of memory in rpc queue        | rpc message queue memory usage reached limit                 | 1. Check and confirm if system load is too high 2. (If necessary) Increase rpc message queue memory limit through configuration rpcQueueMemoryAllowed 3. If the problem persists, preserve the scene and logs, report issue on github |
| 0x80000127 | Invalid timestamp format          | Incorrect timestamp format                                   | Check and confirm the input timestamp format is correct      |
| 0x80000128 | Msg decode error                  | Message decode error                                         | Preserve the scene and logs, report issue on github          |
| 0x8000012A | Not found                         | Internal cache information not found                         | Preserve the scene and logs, report issue on github          |
| 0x8000012B | Out of disk space                 | Insufficient disk space                                      | 1. Check and ensure data directory, temporary file folder directory have sufficient disk space 2. Regularly check and maintain the above directories to ensure enough space |
| 0x80000130 | Database is starting up           | Database is starting up, unable to provide service           | Check database status, wait for the system to finish starting up or retry |
| 0x80000131 | Database is closing down          | Database is closing down or has closed, unable to provide service | Check database status, ensure the system is working in normal state |
| 0x80000132 | Invalid data format               | Incorrect data format                                        | 1. Preserve the scene and logs, report issue on github 2. Contact Taos customer support |
| 0x80000133 | Invalid operation                 | Invalid or unsupported operation                             | 1. Modify to confirm the current operation is legal and supported, check parameter validity 2. If the problem persists, preserve the scene and logs, report issue on github |
| 0x80000134 | Invalid value                     | Invalid value                                                | Preserve the scene and logs, report issue on github          |
| 0x80000135 | Invalid fqdn                      | Invalid FQDN                                                 | Check if the configured or input FQDN value is correct       |
| 0x8000013C | Invalid disk id                   | Invalid disk id                                              | Check users whether the mounted disk is invalid or use the parameter diskIDCheckEnabled to skip the disk check. |


## tsc

| Error Code | Error Description           | Possible Error Scenarios or Reasons             | Recommended Actions for Users                                |
| ---------- | --------------------------- | ----------------------------------------------- | ------------------------------------------------------------ |
| 0x80000207 | Invalid user name           | Invalid database username                       | Check if the database username is correct                    |
| 0x80000208 | Invalid password            | Invalid database password                       | Check if the database password is correct                    |
| 0x80000209 | Database name too long      | Invalid database name                           | Check if the database name is correct                        |
| 0x8000020A | Table name too long         | Invalid table name                              | Check if the table name is correct                           |
| 0x8000020F | Query terminated            | Query was terminated                            | Check if the query was terminated by a user                  |
| 0x80000213 | Disconnected from server    | Connection was interrupted                      | Check if the connection was interrupted by someone or if the client is exiting |
| 0x80000216 | Syntax error in SQL         | SQL syntax error                                | Check and correct the SQL statement                          |
| 0x80000219 | SQL statement too long      | SQL length exceeds limit                        | Check and correct the SQL statement                          |
| 0x8000021A | File is empty               | File content is empty                           | Check the content of the input file                          |
| 0x8000021F | Invalid column length       | Incorrect column length                         | Preserve the scene and logs, report issue on GitHub          |
| 0x80000222 | Invalid JSON data type      | Incorrect JSON data type                        | Check the JSON content input                                 |
| 0x80000224 | Value out of range          | Data size exceeds type range                    | Check the data value input                                   |
| 0x80000229 | Invalid tsc input           | API input error                                 | Check the parameters passed when calling the API from the application |
| 0x8000022A | Stmt API usage error        | Incorrect usage of STMT API                     | Check the order of STMT API calls, applicable scenarios, and error handling |
| 0x8000022B | Stmt table name not set     | STMT table name not set correctly               | Check if the table name setting interface was called         |
| 0x8000022D | Query killed                | Query was terminated                            | Check if the query was terminated by a user                  |
| 0x8000022E | No available execution node | No available query execution node               | Check the current query policy configuration, ensure available Qnode if needed |
| 0x8000022F | Table is not a supertable   | Table name in the statement is not a supertable | Check if the table name used in the statement is a supertable |
| 0x80000230 | Stmt cache error            | STMT internal cache error                       | Preserve the scene and logs, report issue on GitHub          |
| 0x80000231 | Tsc internal error          | TSC internal error                              | Preserve the scene and logs, report issue on GitHub          |

## mnode

| Error Code | Description                                                  | Possible Error Scenarios or Reasons                          | Suggested Actions for Users                                  |
| ---------- | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 0x80000303 | Insufficient privilege for operation                         | No permission                                                | Grant permissions                                            |
| 0x8000030B | Data expired                                                 | Internal error                                               | Report issue                                                 |
| 0x8000030C | Invalid query id                                             | Internal error                                               | Report issue                                                 |
| 0x8000030E | Invalid connection id                                        | Internal error                                               | Report issue                                                 |
| 0x80000315 | User is disabled                                             | User is unavailable                                          | Grant permissions                                            |
| 0x80000320 | Object already there                                         | Internal error                                               | Report issue                                                 |
| 0x80000322 | Invalid table type                                           | Internal error                                               | Report issue                                                 |
| 0x80000323 | Object not there                                             | Internal error                                               | Report issue                                                 |
| 0x80000326 | Invalid action type                                          | Internal error                                               | Report issue                                                 |
| 0x80000328 | Invalid raw data version                                     | Internal error                                               | Report issue                                                 |
| 0x80000329 | Invalid raw data len                                         | Internal error                                               | Report issue                                                 |
| 0x8000032A | Invalid raw data content                                     | Internal error                                               | Report issue                                                 |
| 0x8000032C | Object is creating                                           | Internal error                                               | Report issue                                                 |
| 0x8000032D | Object is dropping                                           | Internal error                                               | Report issue                                                 |
| 0x80000330 | Dnode already exists                                         | Internal error                                               | Report issue                                                 |
| 0x80000331 | Dnode does not exist                                         | Internal error                                               | Report issue                                                 |
| 0x80000332 | Vgroup does not exist                                        | Internal error                                               | Report issue                                                 |
| 0x80000333 | Cannot drop mnode which is leader                            | Operating node is leader                                     | Confirm if the operation is correct                          |
| 0x80000334 | Out of dnodes                                                | Insufficient dnode nodes                                     | Increase dnode nodes                                         |
| 0x80000335 | Cluster cfg inconsistent                                     | Configuration inconsistency                                  | Check if the configuration of dnode and mnode nodes is consistent. Check method: 1. Output in logs when nodes start 2. Use show variables |
| 0x8000033B | Cluster id not match                                         | Node configuration data inconsistency                        | Check the clusterid in each node's data/dnode/dnodes.json file |
| 0x80000340 | Account already exists                                       | (Enterprise only) Internal error                             | Report issue                                                 |
| 0x80000342 | Invalid account options                                      | (Enterprise only) Operation not supported                    | Confirm if the operation is correct                          |
| 0x80000344 | Invalid account                                              | Account does not exist                                       | Confirm if the account is correct                            |
| 0x80000350 | User already exists                                          | Create user, duplicate creation                              | Confirm if the operation is correct                          |
| 0x80000351 | Invalid user                                                 | User does not exist                                          | Confirm if the operation is correct                          |
| 0x80000352 | Invalid user format                                          | Incorrect format                                             | Confirm if the operation is correct                          |
| 0x80000353 | Invalid password format                                      | The password must be between 8 and 16 characters long and include at least three types of characters from the following: uppercase letters, lowercase letters, numbers, and special characters.  | Confirm the format of the password string |
| 0x80000354 | Can not get user from conn                                   | Internal error                                               | Report issue                                                 |
| 0x80000355 | Too many users                                               | (Enterprise only) Exceeding user limit                       | Adjust configuration                                         |
| 0x80000357 | Authentication failure                                       | Incorrect password                                           | Confirm if the operation is correct                          |
| 0x80000358 | User not available                                           | User does not exist                                          | Confirm if the operation is correct                          |
| 0x80000360 | STable already exists                                        | Internal error                                               | Report issue                                                 |
| 0x80000361 | STable not exist                                             | Internal error                                               | Report issue                                                 |
| 0x80000364 | Too many tags                                                | Too many tags                                                | Cannot be modified, code-level restriction                   |
| 0x80000365 | Too many columns                                             | Too many columns                                             | Cannot be modified, code-level restriction                   |
| 0x80000369 | Tag already exists                                           | Tag already exists                                           | Confirm if the operation is correct                          |
| 0x8000036A | Tag does not exist                                           | Tag does not exist                                           | Confirm if the operation is correct                          |
| 0x8000036B | Column already exists                                        | Column already exists                                        | Confirm if the operation is correct                          |
| 0x8000036C | Column does not exist                                        | Column does not exist                                        | Confirm if the operation is correct                          |
| 0x8000036E | Invalid stable options                                       | Internal error                                               | Report issue                                                 |
| 0x8000036F | Invalid row bytes                                            | Internal error                                               | Report issue                                                 |
| 0x80000370 | Invalid func name                                            | Incorrect name length                                        | Confirm if the operation is correct                          |
| 0x80000372 | Invalid func code                                            | Incorrect code length                                        | Confirm if the operation is correct                          |
| 0x80000373 | Func already exists                                          | Func already exists                                          | Confirm if the operation is correct                          |
| 0x80000374 | Func not exists                                              | Func does not exist                                          | Confirm if the operation is correct                          |
| 0x80000375 | Invalid func bufSize                                         | Incorrect bufSize length, or exceeds limit                   | Confirm if the operation is correct                          |
| 0x80000378 | Invalid func comment                                         | Incorrect length, or exceeds limit                           | Confirm if the operation is correct                          |
| 0x80000379 | Invalid func retrieve msg                                    | Incorrect length, or exceeds limit                           | Confirm if the operation is correct                          |
| 0x80000380 | Database not specified or available                          | Database not specified                                       | Use `use database;`                                          |
| 0x80000381 | Database already exists                                      | Database already exists                                      | Confirm if the operation is correct                          |
| 0x80000382 | Invalid database options                                     | Internal error                                               | Report issue                                                 |
| 0x80000383 | Invalid database name                                        | Incorrect length                                             | Confirm if the operation is correct                          |
| 0x80000385 | Too many databases for account                               | Exceeding limit                                              | Adjust configuration                                         |
| 0x80000386 | Database in dropping status                                  | Database is being deleted                                    | Retry, if it remains in this state for a long time, report issue |
| 0x80000388 | Database not exist                                           | Does not exist                                               | Confirm if the operation is correct                          |
| 0x80000389 | Invalid database account                                     | Internal error                                               | Report issue                                                 |
| 0x8000038A | Database options not changed                                 | No change in operation                                       | Confirm if the operation is correct                          |
| 0x8000038B | Index not exist                                              | Does not exist                                               | Confirm if the operation is correct                          |
| 0x80000396 | Database in creating status                                  | Database is being created                                    | Retry                                                        |
| 0x8000039A | Invalid system table name                                    | Internal error                                               | Report issue                                                 |
| 0x800003A0 | Mnode already exists                                         | Already exists                                               | Confirm if the operation is correct                          |
| 0x800003A1 | Mnode not there                                              | Already exists                                               | Confirm if the operation is correct                          |
| 0x800003A2 | Qnode already exists                                         | Already exists                                               | Confirm if the operation is correct                          |
| 0x800003A3 | Qnode not there                                              | Does not exist                                               | Confirm if the operation is correct                          |
| 0x800003A4 | Snode already exists                                         | Already exists                                               | Confirm if the operation is correct                          |
| 0x800003A5 | Snode not there                                              | Does not exist                                               | Confirm if the operation is correct                          |
| 0x800003A8 | The replica of mnode cannot less than 1                      | Less than 1 mnode                                            | Operation not allowed                                        |
| 0x800003A9 | The replica of mnode cannot exceed 3                         | More than 1 mnode                                            | Operation not allowed                                        |
| 0x800003B1 | No enough memory in dnode                                    | Insufficient memory                                          | Adjust configuration                                         |
| 0x800003B3 | Invalid dnode end point                                      | Incorrect ep configuration                                   | Confirm if the operation is correct                          |
| 0x800003B6 | Offline dnode exists                                         | Dnode offline                                                | Check node status                                            |
| 0x800003B7 | Invalid vgroup replica                                       | Internal error                                               | Report issue                                                 |
| 0x800003B8 | Dnode in creating status                                     | Being created                                                | Retry                                                        |
| 0x800003B9 | Dnode in dropping status                                     | Being deleted                                                | Retry                                                        |
| 0x800003C2 | Invalid stable alter options                                 | Internal error                                               | Report issue                                                 |
| 0x800003C3 | STable option unchanged                                      | No change in operation                                       | Confirm if the operation is correct                          |
| 0x800003C4 | Field used by topic                                          | Being used                                                   | Confirm if the operation is correct                          |
| 0x800003C5 | Database is single stable mode                               | Internal error                                               | Report issue                                                 |
| 0x800003C6 | Invalid schema version while alter stb                       | Internal error                                               | Report issue                                                 |
| 0x800003C7 | Invalid stable uid while alter stb                           | Internal error                                               | Report issue                                                 |
| 0x800003C8 | Field used by tsma                                           | Being used                                                   | Confirm if the operation is correct                          |
| 0x800003D1 | Transaction not exists                                       | Does not exist                                               | Confirm if the operation is correct                          |
| 0x800003D2 | Invalid stage to kill                                        | Transaction is at a stage that cannot be killed (e.g., during commit) | Wait for the transaction to end, if it does not end for a long time, report issue |
| 0x800003D3 | Conflict transaction not completed                           | Transaction conflict, cannot perform this operation          | Use the show transactions command to view the conflicting transaction, wait for the conflicting transaction to end, if it does not end for a long time, report issue |
| 0x800003D4 | Transaction commitlog is null                                | Internal error                                               | Report issue                                                 |
| 0x800003D5 | Unable to establish connection While execute transaction and will continue in the background | Network error                                                | Check if the network is normal                               |
| 0x800003D6 | Last Transaction not finished                                | Internal error                                               | Report issue                                                 |
| 0x800003D7 | Sync timeout While execute transaction and will continue in the background | Internal error                                               | Report issue                                                 |
| 0x800003DF | Unknown transaction error                                    | Internal error                                               | Report issue                                                 |
| 0x800003E0 | Topic already exists                                         | Already exists                                               | Confirm if the operation is correct                          |
| 0x800003E1 | Topic not exist                                              | Does not exist                                               | Confirm if the operation is correct                          |
| 0x800003E3 | Invalid topic                                                | Internal error                                               | Report issue                                                 |
| 0x800003E4 | Topic with invalid query                                     | Internal error                                               | Report issue                                                 |
| 0x800003E5 | Topic with invalid option                                    | Internal error                                               | Report issue                                                 |
| 0x800003E6 | Consumer not exist                                           | Does not exist                                               | Confirm if the operation is correct                          |
| 0x800003E7 | Topic unchanged                                              | No change                                                    | Confirm if the operation is correct                          |
| 0x800003E8 | Subcribe not exist                                           | Does not exist                                               | Confirm if the operation is correct                          |
| 0x800003E9 | Offset not exist                                             | Does not exist                                               | Confirm if the operation is correct                          |
| 0x800003EA | Consumer not ready                                           | Internal error                                               | Report issue                                                 |
| 0x800003EB | Topic subscribed cannot be dropped                           | Being used                                                   | Confirm if the operation is correct                          |
| 0x800003EC | Consumer group being used by some consumer                   | Being used                                                   | Confirm if the operation is correct                          |
| 0x800003ED | Topic must be dropped first                                  | Being used                                                   | Confirm if the operation is correct                          |
| 0x800003EE | Invalid subscribe option                                     | Internal error                                               | Confirm if the operation is correct                          |
| 0x800003EF | Topic being rebalanced                                       | In operation                                                 | Retry                                                        |
| 0x800003F0 | Stream already exists                                        | Already exists                                               | Confirm if the operation is correct                          |
| 0x800003F1 | Stream not exist                                             | Does not exist                                               | Confirm if the operation is correct                          |
| 0x800003F2 | Invalid stream option                                        | Internal error                                               | Report issue                                                 |
| 0x800003F3 | Stream must be dropped first                                 | Being used                                                   | Confirm if the operation is correct                          |
| 0x800003F5 | Stream temporarily does not support source db having replica > 1 | Exceeding limit                                              | Operation not allowed                                        |
| 0x800003F6 | Too many streams                                             | Exceeding limit                                              | Cannot be modified, code-level restriction                   |
| 0x800003F7 | Cannot write the same stable as other stream                 | Internal error                                               | Report issue                                                 |
| 0x80000480 | index already exists                                         | Already exists                                               | Confirm if the operation is correct                          |
| 0x80000481 | index not exist                                              | Does not exist                                               | Confirm if the operation is correct                          |
| 0x80000482 | Invalid sma index option                                     | Internal error                                               | Report issue                                                 |
| 0x80000483 | index already exists                                         | Already exists                                               | Confirm if the operation is correct                          |
| 0x80000484 | index not exist                                              | Does not exist                                               | Confirm if the operation is correct                          |

## dnode

| Error Code | Description            | Possible Error Scenarios or Reasons | Recommended Actions |
| ---------- | ---------------------- | ----------------------------------- | ------------------- |
| 0x80000408 | Dnode is offline       | Offline                             | Check node status   |
| 0x80000409 | Mnode already deployed | Already deployed                    | Confirm if correct  |
| 0x8000040A | Mnode not found        | Internal error                      | Report issue        |
| 0x8000040B | Mnode not deployed     | Internal error                      | Report issue        |
| 0x8000040C | Qnode already deployed | Already deployed                    | Confirm if correct  |
| 0x8000040D | Qnode not found        | Internal error                      | Report issue        |
| 0x8000040E | Qnode not deployed     | Internal error                      | Report issue        |
| 0x8000040F | Snode already deployed | Already deployed                    | Confirm if correct  |
| 0x80000410 | Snode not found        | Internal error                      | Report issue        |
| 0x80000411 | Snode not deployed     | Already deployed                    | Confirm if correct  |

## vnode

| Error Code | Description                                        | Possible Error Scenarios or Reasons             | Recommended Actions |
| ---------- | -------------------------------------------------- | ----------------------------------------------- | ------------------- |
| 0x80000503 | Invalid vgroup ID                                  | Old client did not update cache, internal error | Report issue        |
| 0x80000512 | No writing privilege                               | No write permission                             | Seek authorization  |
| 0x80000520 | Vnode does not exist                               | Internal error                                  | Report issue        |
| 0x80000521 | Vnode already exists                               | Internal error                                  | Report issue        |
| 0x80000522 | Hash value of table is not in the vnode hash range | Table does not belong to vnode                  | Report issue        |
| 0x80000524 | Invalid table operation                            | Illegal table operation                         | Report issue        |
| 0x80000525 | Column already exists                              | Column already exists when modifying table      | Report issue        |
| 0x80000526 | Column does not exist                              | Column does not exist when modifying table      | Report issue        |
| 0x80000527 | Column is subscribed                               | Column is subscribed, cannot operate            | Report issue        |
| 0x80000529 | Vnode is stopped                                   | Vnode is closed                                 | Report issue        |
| 0x80000530 | Duplicate write request                            | Duplicate write request, internal error         | Report issue        |
| 0x80000531 | Vnode query is busy                                | Query is busy                                   | Report issue        |
| 0x80000540 | Vnode already exist but Dbid not match             | Internal error                                  | Report issue        |

## tsdb

| Error Code | Error Description                        | Possible Error Scenarios or Reasons                          | Recommended Actions for Users                        |
| ---------- | ---------------------------------------- | ------------------------------------------------------------ | ---------------------------------------------------- |
| 0x80000600 | Invalid table ID to write                | Writing to a non-existent table                              | Restart the client                                   |
| 0x80000602 | Invalid table schema version             | Table schema version is outdated, internal error             | No action needed, automatic internal update          |
| 0x80000603 | Table already exists                     | Table already exists                                         | Report the issue                                     |
| 0x80000604 | Invalid configuration                    | Internal error                                               | Report the issue                                     |
| 0x80000605 | Init failed                              | Startup failure                                              | Report the issue                                     |
| 0x8000060B | Timestamp is out of range                | Writing time range is out of bounds                          | Report the issue, check application write time logic |
| 0x8000060C | Submit message is messed up              | Message error, possibly due to client-server incompatibility | Report the issue                                     |
| 0x80000618 | Table does not exist                     | Table already exists                                         | Report the issue                                     |
| 0x80000619 | Supertable already exists                | Supertable already exists                                    | Report the issue                                     |
| 0x8000061A | Supertable does not exist                | Supertable does not exist                                    | Report the issue                                     |
| 0x8000061B | Invalid table schema version             | Same as TSDB_CODE_TDB_IVD_TB_SCHEMA_VERSION                  | Report the issue                                     |
| 0x8000061D | Table already exists in other supertable | Table exists, but belongs to another supertable              | Check write application logic                        |

## query

| Error Code | Error Description                    | Possible Error Scenarios or Reasons                          | Recommended Actions for Users                                |
| ---------- | ------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 0x80000700 | Invalid query handle                 | Current query handle does not exist                          | Preserve the scene and logs, report issue on GitHub          |
| 0x80000709 | Multiple retrieval of this query     | Current subquery is already in progress                      | Preserve the scene and logs, report issue on GitHub          |
| 0x8000070A | Too many groups/time window in query | Number of groups or windows in query results exceeds the limit | Adjust the query statement to ensure the number of groups and windows does not exceed the limit |
| 0x8000070D | System error                         | Error returned by underlying system API                      | Preserve the scene and logs, report issue on GitHub          |
| 0x80000720 | Scheduler not exist                  | Client information for the current subquery does not exist   | Preserve the scene and logs, report issue on GitHub          |
| 0x80000721 | Task not exist                       | Subquery does not exist                                      | Preserve the scene and logs, report issue on GitHub          |
| 0x80000722 | Task already exist                   | Subquery already exists                                      | Preserve the scene and logs, report issue on GitHub          |
| 0x80000729 | Task message error                   | Query message error                                          | Preserve the scene and logs, report issue on GitHub          |
| 0x8000072B | Task status error                    | Subquery status error                                        | Preserve the scene and logs, report issue on GitHub          |
| 0x8000072F | Job not exist                        | Query JOB no longer exists                                   | Preserve the scene and logs, report issue on GitHub          |
| 0x80000739 | Query memory upper limit is reached  | Single query memory upper limit is reached                   | Modify memory upper limit size or optimize SQL               |
| 0x8000073A | Query memory exhausted               | Query memory in dnode is exhausted                           | Limit concurrent queries or add more physical memory         |
| 0x8000073B | Timeout for long time no fetch       | Query without fetch for a long time                          | Correct application to fetch data asap                       |

## grant

| Error Code | Description                         | Possible Error Scenarios or Reasons                    | Recommended Actions for Users                                |
| ---------- | ----------------------------------- | ------------------------------------------------------ | ------------------------------------------------------------ |
| 0x80000800 | License expired                     | License period expired                                 | Check license information, contact delivery for updated license code |
| 0x80000801 | DNode creation limited by license   | Number of Dnodes exceeds license limit                 | Check license information, contact delivery for updated license code |
| 0x80000802 | Account creation limited by license | Number of accounts exceeds license limit               | Check license information, contact delivery for updated license code |
| 0x80000803 | Time-Series limited by license      | Number of time-series exceeds license limit            | Check license information, contact delivery for updated license code |
| 0x80000804 | DB creation limited by license      | Number of databases exceeds license limit              | Check license information, contact delivery for updated license code |
| 0x80000805 | User creation limited by license    | Number of users exceeds license limit                  | Check license information, contact delivery for updated license code |
| 0x80000806 | Conn creation limited by license    | Number of connections exceeds license limit            | Not limited yet, contact delivery for inspection             |
| 0x80000807 | Stream creation limited by license  | Number of streams exceeds license limit                | Not limited yet, contact delivery for inspection             |
| 0x80000808 | Write speed limited by license      | Write speed exceeds license limit                      | Not limited yet, contact delivery for inspection             |
| 0x80000809 | Storage capacity limited by license | Storage capacity exceeds license limit                 | Check license information, contact delivery for updated license code |
| 0x8000080A | Query time limited by license       | Number of queries exceeds license limit                | Not limited yet, contact delivery for inspection             |
| 0x8000080B | CPU cores limited by license        | Number of CPU cores exceeds license limit              | Not limited yet, contact delivery for inspection             |
| 0x8000080C | STable creation limited by license  | Number of supertables exceeds license limit            | Check license information, contact delivery for updated license code |
| 0x8000080D | Table creation limited by license   | Number of subtables/basic tables exceeds license limit | Check license information, contact delivery for updated license code |

## sync

| Error Code | Description                  | Possible Error Scenarios or Reasons                          | Recommended Actions for Users                                |
| ---------- | ---------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 0x80000903 | Sync timeout                 | Scenario 1: Leader switch occurred, old leader node started negotiation but not yet agreed, leading to timeout. Scenario 2: Follower node response timeout, causing negotiation timeout. | Check cluster status, e.g., `show vgroups`. Check server logs and network conditions between server nodes. |
| 0x8000090C | Sync leader is unreachable   | Scenario 1: During leader election. Scenario 2: Client request routed to follower node, and redirection failed. Scenario 3: Client or server network configuration error. | Check cluster status, network configuration, application access status, etc. Check server logs and network conditions between server nodes. |
| 0x8000090F | Sync new config error        | Member configuration change error                            | Internal error, user cannot intervene                        |
| 0x80000911 | Sync not ready to propose    | Scenario 1: Recovery not completed                           | Check cluster status, e.g., `show vgroups`. Check server logs and network conditions between server nodes. |
| 0x80000914 | Sync leader is restoring     | Scenario 1: Leader switch occurred, leader is replaying logs | Check cluster status, e.g., `show vgroups`. Check server logs and observe recovery progress. |
| 0x80000915 | Sync invalid snapshot msg    | Incorrect snapshot replication message                       | Server internal error                                        |
| 0x80000916 | Sync buffer is full          | Scenario 1: High concurrency of client requests, exceeding server's processing capacity, or due to severe lack of network and CPU resources, or network connection issues. | Check cluster status, system resource usage (e.g., disk IO, CPU, network), and network connections between nodes. |
| 0x80000917 | Sync write stall             | Scenario 1: State machine execution blocked, e.g., due to system busyness, severe lack of disk IO resources, or write failures. | Check cluster status, system resource usage (e.g., disk IO and CPU), and whether there were write failures. |
| 0x80000918 | Sync negotiation win is full | Scenario 1: High concurrency of client requests, exceeding server's processing capacity, or due to severe lack of network and CPU resources, or network connection issues. | Check cluster status, system resource usage (e.g., disk IO, CPU, network), and network connections between nodes. |
| 0x800009FF | Sync internal error          | Other internal errors                                        | Check cluster status, e.g., `show vgroups`                   |

## tq

| Error Code | Description               | Possible Scenarios or Reasons                                | Recommended Actions for Users          |
| ---------- | ------------------------- | ------------------------------------------------------------ | -------------------------------------- |
| 0x80000A0C | TQ table schema not found | The table does not exist when consuming data                 | Internal error, not passed to users    |
| 0x80000A0D | TQ no committed offset    | Consuming with offset reset = none, and no previous offset on server | Set offset reset to earliest or latest |

## wal

| Error Code | Description           | Possible Scenarios or Reasons                   | Recommended Actions for Users |
| ---------- | --------------------- | ----------------------------------------------- | ----------------------------- |
| 0x80001001 | WAL file is corrupted | WAL file damaged                                | Internal server error         |
| 0x80001003 | WAL invalid version   | Requested log version exceeds current log range | Internal server error         |
| 0x80001005 | WAL log not exist     | Requested log record does not exist             | Internal server error         |
| 0x80001006 | WAL checksum mismatch | Scenario: WAL file damaged                      | Internal server error         |
| 0x80001007 | WAL log incomplete    | Log file has been lost or damaged               | Internal server error         |

## tfs

| Error Code | Description                      | Possible Scenarios or Reasons                        | Recommended Actions for Users                                |
| ---------- | -------------------------------- | ---------------------------------------------------- | ------------------------------------------------------------ |
| 0x80002201 | TFS invalid configuration        | Incorrect multi-tier storage configuration           | Check if the configuration is correct                        |
| 0x80002202 | TFS too many disks on one level  | Incorrect multi-tier storage configuration           | Check if the number of disks on one level exceeds the maximum limit |
| 0x80002203 | TFS duplicate primary mount disk | Incorrect multi-tier storage configuration           | Check if the configuration is correct                        |
| 0x80002204 | TFS no primary mount disk        | Incorrect multi-tier storage configuration           | Check if the configuration is correct                        |
| 0x80002205 | TFS no disk mount on tire        | Incorrect multi-tier storage configuration           | Check if the configuration is correct                        |
| 0x80002208 | No disk available on a tier.     | TFS internal error, often occurs when disks are full | Add more disks to expand capacity                            |

## catalog

| Error Code | Description                      | Possible Error Scenarios or Reasons   | Suggested Actions for Users                                  |
| ---------- | -------------------------------- | ------------------------------------- | ------------------------------------------------------------ |
| 0x80002400 | catalog internal error           | Internal error in catalog             | Preserve the scene and logs, report issue on GitHub          |
| 0x80002401 | catalog invalid input parameters | Incorrect input parameters in catalog | Preserve the scene and logs, report issue on GitHub          |
| 0x80002402 | catalog is not ready             | Catalog not fully initialized         | Preserve the scene and logs, report issue on GitHub          |
| 0x80002403 | catalog system error             | System error in catalog               | Preserve the scene and logs, report issue on GitHub          |
| 0x80002404 | Database is dropped              | Database cache deleted                | Preserve the scene and logs, report issue on GitHub          |
| 0x80002405 | catalog is out of service        | Catalog module has exited             | Preserve the scene and logs, report issue on GitHub          |
| 0x80002550 | Invalid msg order                | Incorrect message order               | Preserve the scene and logs, report issue on GitHub          |
| 0x80002501 | Job status error                 | Incorrect job status                  | Preserve the scene and logs, report issue on GitHub          |
| 0x80002502 | scheduler internal error         | Internal error in scheduler           | Preserve the scene and logs, report issue on GitHub          |
| 0x80002504 | Task timeout                     | Subtask timeout                       | Preserve the scene and logs, report issue on GitHub          |
| 0x80002505 | Job is dropping                  | Task being or already canceled        | Check if there was a manual or application interruption of the current task |

## parser

| Error Code | Description                                                                                            | Possible Error Scenarios or Reasons                                        | Suggested Actions for Users                                 |
| ---------- |--------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------| ----------------------------------------------------------- |
| 0x80002600 | syntax error near                                                                                      | SQL syntax error                                                           | Check and correct the SQL statement                         |
| 0x80002601 | Incomplete SQL statement                                                                               | Incomplete SQL statement                                                   | Check and correct the SQL statement                         |
| 0x80002602 | Invalid column name                                                                                    | Illegal or non-existent column name                                        | Check and correct the SQL statement                         |
| 0x80002603 | Table does not exist                                                                                   | Table does not exist                                                       | Check and confirm the existence of the table in the SQL statement |
| 0x80002604 | Column ambiguously defined                                                                             | Column (alias) redefined                                                   | Check and correct the SQL statement                         |
| 0x80002605 | Invalid value type                                                                                     | Illegal constant value                                                     | Check and correct the SQL statement                         |
| 0x80002608 | There mustn't be aggregation                                                                           | Aggregation function used in illegal clause                                | Check and correct the SQL statement                         |
| 0x80002609 | ORDER BY item must be the number of a SELECT-list expression                                           | Illegal position specified in Order by                                     | Check and correct the SQL statement                         |
| 0x8000260A | Not a GROUP BY expression                                                                              | Illegal group by statement                                                 | Check and correct the SQL statement                         |
| 0x8000260B | Not SELECTed expression                                                                                | Illegal expression                                                         | Check and correct the SQL statement                         |
| 0x8000260C | Not a single-group group function                                                                      | Illegal use of column and function                                         | Check and correct the SQL statement                         |
| 0x8000260D | Tags number not matched                                                                                | Mismatched number of tag columns                                           | Check and correct the SQL statement                         |
| 0x8000260E | Invalid tag name                                                                                       | Invalid or non-existent tag name                                           | Check and correct the SQL statement                         |
| 0x80002610 | Value is too long                                                                                      | Value length exceeds limit                                                 | Check and correct the SQL statement or API parameters       |
| 0x80002611 | Password too short or empty                                                                            | Password is empty or less than 8 chars                                     | Use a valid password                                        |
| 0x80002612 | Port should be an integer that is less than 65535 and greater than 0                                   | Illegal port number                                                        | Check and correct the port number                           |
| 0x80002613 | Endpoint should be in the format of 'fqdn:port'                                                        | Incorrect address format                                                   | Check and correct the address information                   |
| 0x80002614 | This statement is no longer supported                                                                  | Feature has been deprecated                                                | Refer to the feature documentation                          |
| 0x80002615 | Interval too small                                                                                     | Interval value exceeds the allowed minimum                                 | Change the INTERVAL value                                   |
| 0x80002616 | Database not specified                                                                                 | Database not specified                                                     | Specify the database for the current operation              |
| 0x80002617 | Invalid identifier name                                                                                | Illegal or invalid length ID                                               | Check the names of related libraries, tables, columns, TAGs, etc. in the statement |
| 0x80002618 | Corresponding supertable not in this db                                                                | Supertable does not exist                                                  | Check if the corresponding supertable exists in the database |
| 0x80002619 | Invalid database option                                                                                | Illegal database option value                                              | Check and correct the database option values                |
| 0x8000261A | Invalid table option                                                                                   | Illegal table option value                                                 | Check and correct the table option values                   |
| 0x80002624 | GROUP BY and WINDOW-clause can't be used together                                                      | Group by and window cannot be used together                                | Check and correct the SQL statement                         |
| 0x80002627 | Aggregate functions do not support nesting                                                             | Functions do not support nested use                                        | Check and correct the SQL statement                         |
| 0x80002628 | Only support STATE_WINDOW on integer/bool/varchar column                                               | Unsupported STATE_WINDOW data type                                         | Check and correct the SQL statement                         |
| 0x80002629 | Not support STATE_WINDOW on tag column                                                                 | STATE_WINDOW not supported on tag column                                   | Check and correct the SQL statement                         |
| 0x8000262A | STATE_WINDOW not support for supertable query                                                          | STATE_WINDOW not supported for supertable                                  | Check and correct the SQL statement                         |
| 0x8000262B | SESSION gap should be fixed time window, and greater than 0                                            | Illegal SESSION window value                                               | Check and correct the SQL statement                         |
| 0x8000262C | Only support SESSION on primary timestamp column                                                       | Illegal SESSION window column                                              | Check and correct the SQL statement                         |
| 0x8000262D | Interval offset cannot be negative                                                                     | Illegal INTERVAL offset value                                              | Check and correct the SQL statement                         |
| 0x8000262E | Cannot use 'year' as offset when interval is 'month'                                                   | Illegal INTERVAL offset unit                                               | Check and correct the SQL statement                         |
| 0x8000262F | Interval offset should be shorter than interval                                                        | Illegal INTERVAL offset value                                              | Check and correct the SQL statement                         |
| 0x80002630 | Does not support sliding when interval is natural month/year                                           | Illegal sliding unit                                                       | Check and correct the SQL statement                         |
| 0x80002631 | sliding value no larger than the interval value                                                        | Illegal sliding value                                                      | Check and correct the SQL statement                         |
| 0x80002632 | sliding value can not less than 1%% of interval value                                                  | Illegal sliding value                                                      | Check and correct the SQL statement                         |
| 0x80002633 | Only one tag if there is a json tag                                                                    | Only single JSON tag column supported                                      | Check and correct the SQL statement                         |
| 0x80002634 | Query block has incorrect number of result columns                                                     | Mismatched number of columns                                               | Check and correct the SQL statement                         |
| 0x80002635 | Incorrect TIMESTAMP value                                                                              | Illegal primary timestamp column value                                     | Check and correct the SQL statement                         |
| 0x80002637 | soffset/offset can not be less than 0                                                                  | Illegal soffset/offset value                                               | Check and correct the SQL statement                         |
| 0x80002638 | slimit/soffset only available for PARTITION/GROUP BY query                                             | slimit/soffset only supported for PARTITION BY/GROUP BY statements         | Check and correct the SQL statement                         |
| 0x80002639 | Invalid topic query                                                                                    | Unsupported TOPIC query                                                    |                                                             |
| 0x8000263A | Cannot drop supertable in batch                                                                        | Batch deletion of supertables not supported                                | Check and correct the SQL statement                         |
| 0x8000263B | Start(end) time of query range required or time range too large                                        | Window count exceeds limit                                                 | Check and correct the SQL statement                         |
| 0x8000263C | Duplicated column names                                                                                | Duplicate column names                                                     | Check and correct the SQL statement                         |
| 0x8000263D | Tags length exceeds max length                                                                         | tag value length exceeds maximum supported range                           | Check and correct the SQL statement                         |
| 0x8000263E | Row length exceeds max length                                                                          | Row length check and correct SQL statement                                 | Check and correct the SQL statement                         |
| 0x8000263F | Illegal number of columns                                                                              | Incorrect number of columns                                                | Check and correct the SQL statement                         |
| 0x80002640 | Too many columns                                                                                       | Number of columns exceeds limit                                            | Check and correct the SQL statement                         |
| 0x80002641 | First column must be timestamp                                                                         | The first column must be the primary timestamp column                      | Check and correct the SQL statement                         |
| 0x80002642 | Invalid binary/nchar column/tag length                                                                 | Incorrect length for binary/nchar                                          | Check and correct the SQL statement                         |
| 0x80002643 | Invalid number of tag columns                                                                          | Incorrect number of tag columns                                            | Check and correct the SQL statement                         |
| 0x80002644 | Permission denied                                                                                      | Permission error                                                           | Check and confirm user permissions                          |
| 0x80002645 | Invalid stream query                                                                                   | Illegal stream statement                                                   | Check and correct the SQL statement                         |
| 0x80002646 | Invalid _c0 or_rowts expression                                                                        | Illegal use of _c0 or_rowts                                                | Check and correct the SQL statement                         |
| 0x80002647 | Invalid timeline function                                                                              | Function depends on non-existent primary timestamp                         | Check and correct the SQL statement                         |
| 0x80002648 | Invalid password                                                                                       | Password does not meet standards                                           | Check and change the password                               |
| 0x80002649 | Invalid alter table statement                                                                          | Illegal modify table statement                                             | Check and correct the SQL statement                         |
| 0x8000264A | Primary timestamp column cannot be dropped                                                             | Primary timestamp column cannot be deleted                                 | Check and correct the SQL statement                         |
| 0x8000264B | Only binary/nchar column length could be modified, and the length can only be increased, not decreased | Illegal column modification                                                | Check and correct the SQL statement                         |
| 0x8000264C | Invalid tbname pseudocolumn                                                                            | Illegal use of tbname column                                               | Check and correct the SQL statement                         |
| 0x8000264D | Invalid function name                                                                                  | Illegal function name                                                      | Check and correct the function name                         |
| 0x8000264E | Comment too long                                                                                       | Comment length exceeds limit                                               | Check and correct the SQL statement                         |
| 0x8000264F | Function(s) only allowed in SELECT list, cannot mixed with non scalar functions or columns             | Illegal mixing of functions                                                | Check and correct the SQL statement                         |
| 0x80002650 | Window query not supported, since no valid timestamp column included in the result of subquery         | Window query depends on non-existent primary timestamp column              | Check and correct the SQL statement                         |
| 0x80002651 | No columns can be dropped                                                                              | Essential columns cannot be deleted                                        | Check and correct the SQL statement                         |
| 0x80002652 | Only tag can be json type                                                                              | Normal columns do not support JSON type                                    | Check and correct the SQL statement                         |
| 0x80002655 | The DELETE statement must have a definite time window range                                            | Illegal WHERE condition in DELETE statement                                | Check and correct the SQL statement                         |
| 0x80002656 | The REDISTRIBUTE VGROUP statement only support 1 to 3 dnodes                                           | Illegal number of DNODEs specified in REDISTRIBUTE VGROUP                  | Check and correct the SQL statement                         |
| 0x80002657 | Fill now allowed                                                                                       | Function does not allow FILL feature                                       | Check and correct the SQL statement                         |
| 0x80002658 | Invalid windows pc                                                                                     | Illegal use of window pseudocolumn                                         | Check and correct the SQL statement                         |
| 0x80002659 | Window not allowed                                                                                     | Function cannot be used in window                                          | Check and correct the SQL statement                         |
| 0x8000265A | Stream not allowed                                                                                     | Function cannot be used in stream computation                              | Check and correct the SQL statement                         |
| 0x8000265B | Group by not allowd                                                                                    | Function cannot be used in grouping                                        | Check and correct the SQL statement                         |
| 0x8000265D | Invalid interp clause                                                                                  | Illegal INTERP or related statement                                        | Check and correct the SQL statement                         |
| 0x8000265E | Not valid function ion window                                                                          | Illegal window statement                                                   | Check and correct the SQL statement                         |
| 0x8000265F | Only support single table                                                                              | Function only supported in single table queries                            | Check and correct the SQL statement                         |
| 0x80002660 | Invalid sma index                                                                                      | Illegal creation of SMA statement                                          | Check and correct the SQL statement                         |
| 0x80002661 | Invalid SELECTed expression                                                                            | Invalid query statement                                                    | Check and correct the SQL statement                         |
| 0x80002662 | Fail to get table info                                                                                 | Failed to retrieve table metadata information                              | Preserve the scene and logs, report issue on GitHub         |
| 0x80002663 | Not unique table/alias                                                                                 | Table name (alias) conflict                                                | Check and correct the SQL statement                         |
| 0x80002664 | Join requires valid time-series input                                                                  | Unsupported JOIN query without primary timestamp column output in subquery | Check and correct the SQL statement                         |
| 0x80002665 | The _TAGS pseudocolumn can only be used for subtable and supertable queries                            | Illegal tag column query                                                   | Check and correct the SQL statement                         |
| 0x80002666 | Subquery does not output primary timestamp column                                                      | Check and correct the SQL statement                                        |                                                             |
| 0x80002667 | Invalid usage of expr: %s                                                                              | Illegal expression                                                         | Check and correct the SQL statement                         |
| 0x80002687 | Invalid virtual table's ref column                                                                     | Create/Update Virtual table using incorrect data source column             | Check and correct the SQL statement           |
| 0x80002688 | Invalid table type                                                                                     | Incorrect Table type                                                       | Check and correct the SQL statement           |
| 0x80002689 | Invalid ref column type                                                                                | Virtual table's column type and data source column's type are different    | Check and correct the SQL statement           |
| 0x8000268A | Create child table using virtual super table                                                           | Create non-virtual child table using virtual super table                   | Check and correct the SQL statement           |
| 0x800026FF | Parser internal error                                                                                  | Internal error in parser                                                   | Preserve the scene and logs, report issue on GitHub         |
| 0x80002700 | Planner internal error                                                                                 | Internal error in planner                                                  | Preserve the scene and logs, report issue on GitHub         |
| 0x80002701 | Expect ts equal                                                                                        | JOIN condition validation failed                                           | Preserve the scene and logs, report issue on GitHub         |
| 0x80002702 | Cross join not support                                                                                 | CROSS JOIN not supported                                                   | Check and correct the SQL statement                         |

## function

| Error Code | Error Description                            | Possible Error Scenarios or Reasons                          | Suggested User Actions                                       |
| ---------- | -------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 0x80002800 | Function internal error                      | Errors caused by unreasonable function parameter inputs, with specific error descriptions returned with the error code. For example, when specifying the algorithm for the APERCENTILE function, only the string "default" can be used. Using other inputs like "t-digest" will cause this error. Or, if the second parameter of the TO_ISO8601 function specifies a timezone, and the string does not conform to timezone format standards. | Adjust the function inputs according to the specific error description. |
| 0x80002801 | Invalid function parameter number            | Incorrect number of input parameters for the function. The function requires n parameters, but the number of parameters provided by the user is not n. For example, COUNT(col1, col2). | Adjust the number of input parameters to the correct number. |
| 0x80002802 | Invalid function parameter type              | Incorrect type of input parameters for the function. The function requires numeric type parameters, but the parameters provided by the user are strings. For example, SUM("abc"). | Adjust the function parameter inputs to the correct type.    |
| 0x80002803 | Invalid function parameter value             | Incorrect value of input parameters for the function. The range of input parameters is incorrect. For example, the SAMPLE function's second parameter specifies a sampling number range of [1, 1000], and it will report an error if it is not within this range. | Adjust the function parameter inputs to the correct value.   |
| 0x80002804 | Not a built-in function                      | The function is not a built-in function. Errors will occur if the built-in function is not in the hash table. Users should rarely encounter this problem, otherwise, it indicates an error during the initialization of the internal built-in function hash or corruption. | Customers should not encounter this; if they do, it indicates a bug in the program, consult the developers. |
| 0x80002805 | Duplicate timestamps not allowed in function | Duplicate timestamps in the function's input primary key column. When querying supertables with certain time-order dependent functions, all subtable data will be sorted by timestamp and merged into one timeline for calculation, which may result in duplicate timestamps, causing errors in some calculations. Functions involved include: CSUM, DERIVATIVE, DIFF, IRATE, MAVG, STATECOUNT, STATEDURATION, TWA | Ensure there are no duplicate timestamp data in subtables when querying supertables using these time-order dependent functions. |

## udf

| Error Code | Description                        | Possible Scenarios or Reasons                                | Recommended Actions                                          |
| ---------- | ---------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 0x80002901 | udf is stopping                    | udf call received when dnode exits                           | Stop executing udf queries                                   |
| 0x80002902 | udf pipe read error                | Error occurred when taosd reads from udfd pipe               | udfd unexpectedly exits, 1) C udf crash 2) udfd crash        |
| 0x80002903 | udf pipe connect error             | Error establishing pipe connection to udfd in taosd          | 1) Corresponding udfd not started in taosd. Restart taosd    |
| 0x80002904 | udf pipe not exist                 | Connection error occurs between two phases of udf setup, call, and teardown, causing the connection to disappear, subsequent phases continue | udfd unexpectedly exits, 1) C udf crash 2) udfd crash        |
| 0x80002905 | udf load failure                   | Error loading udf in udfd                                    | 1) udf does not exist in mnode 2) Error in udf loading. Check logs |
| 0x80002906 | udf invalid function input         | udf input check                                              | udf function does not accept input, such as wrong column type |
| 0x80002907 | udf invalid bufsize                | Intermediate result in udf aggregation function exceeds specified bufsize | Increase bufsize, or reduce intermediate result size         |
| 0x80002908 | udf invalid output type            | udf output type differs from the type specified when creating udf | Modify udf, or the type when creating udf, to match the result |
| 0x80002909 | udf program language not supported | udf programming language not supported                       | Use supported languages, currently supports C, Python        |
| 0x8000290A | udf function execution failure     | udf function execution error, e.g., returning incorrect number of rows | Refer to specific error logs                                 |

## sml

| Error Code | Description                      | Possible Scenarios or Reasons                                | Recommended Actions                                          |
| ---------- | -------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 0x80003000 | Invalid line protocol type       | Illegal protocol passed to schemaless interface              | Check if the protocol passed is one of the three defined in taos.h TSDB_SML_PROTOCOL_TYPE |
| 0x80003001 | Invalid timestamp precision type | Illegal timestamp precision passed to schemaless interface   | Check if the protocol passed is one of the seven defined in taos.h TSDB_SML_TIMESTAMP_TYPE |
| 0x80003002 | Invalid data format              | Illegal data format passed to schemaless interface           | Refer to client-side error log hints                         |
| 0x80003004 | Not the same type as before      | Inconsistent column types within the same batch of schemaless data | Ensure the data type of the same column in each row is consistent |
| 0x80003005 | Internal error                   | General internal logic error in schemaless, typically should not occur | Refer to client-side error log hints                         |

## sma

| Error Code | Description                   | Possible Error Scenarios or Reasons                          | Recommended Actions for Users                      |
| ---------- | ----------------------------- | ------------------------------------------------------------ | -------------------------------------------------- |
| 0x80003100 | Tsma init failed              | TSMA environment initialization failed                       | Check error logs, contact development for handling |
| 0x80003101 | Tsma already exists           | TSMA created repeatedly                                      | Avoid repeated creation                            |
| 0x80003102 | Invalid tsma env              | TSMA runtime environment is abnormal                         | Check error logs, contact development for handling |
| 0x80003103 | Invalid tsma state            | The vgroup of the stream computing result is inconsistent with the vgroup that created the TSMA index | Check error logs, contact development for handling |
| 0x80003104 | Invalid tsma pointer          | Processing the results issued by stream computing, the message body is a null pointer. | Check error logs, contact development for handling |
| 0x80003105 | Invalid tsma parameters       | Processing the results issued by stream computing, the result count is 0. | Check error logs, contact development for handling |
| 0x80003113 | Tsma optimization cannot be applied with INTERVAL AUTO offset. | Tsma optimization cannot be enabled with INTERVAL AUTO OFFSET under the current query conditions. | Use SKIP_TSMA Hint or specify a manual INTERVAL OFFSET. |
| 0x80003150 | Invalid rsma env              | Rsma execution environment is abnormal.                      | Check error logs, contact development for handling |
| 0x80003151 | Invalid rsma state            | Rsma execution state is abnormal.                            | Check error logs, contact development for handling |
| 0x80003152 | Rsma qtaskinfo creation error | Creating stream computing environment failed.                | Check error logs, contact development for handling |
| 0x80003153 | Rsma invalid schema           | Metadata information error during startup recovery           | Check error logs, contact development for handling |
| 0x80003154 | Rsma stream state open        | Failed to open stream operator state storage                 | Check error logs, contact development for handling |
| 0x80003155 | Rsma stream state commit      | Failed to commit stream operator state storage               | Check error logs, contact development for handling |
| 0x80003156 | Rsma fs ref error             | Operator file reference count error                          | Check error logs, contact development for handling |
| 0x80003157 | Rsma fs sync error            | Operator file synchronization failed                         | Check error logs, contact development for handling |
| 0x80003158 | Rsma fs update error          | Operator file update failed                                  | Check error logs, contact development for handling |

## index

| Error Code | Description         | Possible Error Scenarios or Reasons                          | Recommended Actions for Users                      |
| ---------- | ------------------- | ------------------------------------------------------------ | -------------------------------------------------- |
| 0x80003200 | INDEX being rebuilt | 1. Writing too fast, causing the index merge thread to lag 2. Index file is damaged, being rebuilt | Check error logs, contact development for handling |
| 0x80003201 | Index file damaged  | File damaged                                                 | Check error logs, contact development for handling |

## tmq

| Error Code | Description           | Possible Error Scenarios or Reasons                          | Recommended Actions for Users                |
| ---------- | --------------------- | ------------------------------------------------------------ | -------------------------------------------- |
| 0x80004000 | Invalid message       | The subscribed data is illegal, generally does not occur     | Check the client-side error logs for details |
| 0x80004001 | Consumer mismatch     | The vnode requested for subscription and the reassigned vnode are inconsistent, usually occurs when new consumers join the same consumer group | Internal error, not exposed to users         |
| 0x80004002 | Consumer closed       | The consumer no longer exists                                | Check if it has already been closed          |
| 0x80004017 | Invalid status, please subscribe topic first | tmq status invalidate                 | Without calling subscribe, directly poll data     |
| 0x80004100 | Stream task not exist | The stream computing task does not exist                     | Check the server-side error logs             |


## virtual table

| Error Code  | Description                                             | Possible Error Scenarios or Reasons                                                                                                                                  | Recommended Actions for Users                         |
|-------------|---------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------|
| 0x80006200  | Virtual table scan internal error                       | virtual table scan operator internal error, generally does not occur                                                                                                 | Check error logs, contact development for handling    |
| 0x80006201  | Virtual table scan invalid downstream operator type     | The incorrect execution plan generated causes the downstream operator type of the virtual table scan operator to be incorrect.                                       | Check error logs, contact development for handling    |
| 0x80006202  | Virtual table prim timestamp column should not has ref  | The timestamp primary key column of a virtual table should not have a data source. If it does, this error will occur during subsequent queries on the virtual table. | Check error logs, contact development for handling    |
| 0x80006203  | Create virtual child table must use virtual super table | Create virtual child table using non-virtual super table                                                                                                             | create virtual child table using virtual super table  |
