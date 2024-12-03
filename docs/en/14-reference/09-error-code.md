---
sidebar_label: Error Codes
title: Error Code Reference
description: Detailed explanation of TDengine server error codes and actions to take.
slug: /tdengine-reference/error-codes
---

This document provides a detailed list of server error codes that may occur when using the TDengine client and the corresponding actions to take. These error codes are also returned to connectors when using native connection methods.

## rpc

| Error Code | Error Description                                                   | Possible Causes                                                             | Suggested Actions                                                           |
| ---------- | ------------------------------------------------------------------- | --------------------------------------------------------------------------- | ----------------------------------------------------------------------------|
| 0x8000000B | Unable to send/receive requests properly                            | 1. Network issues 2. Multiple retries failed to execute the request         | 1. Check the network 2. Analyze logs for complex causes                     |
| 0x80000013 | Time difference between client and server exceeds 900s              | 1. Client and server are in different time zones 2. Time is not in sync     | 1. Set the same time zone 2. Sync client and server times                   |
| 0x80000015 | Unable to resolve FQDN                                              | Invalid FQDN setting                                                        | Check the FQDN setting                                                      |
| 0x80000017 | Port is in use                                                      | Port P is occupied, but the service still tries to bind to it               | 1. Change the service port 2. Kill the process using the port               |
| 0x80000018 | Network jitter or request timeout over 900s                         | 1. Network instability 2. Request took too long                             | 1. Increase the system’s timeout limit 2. Check request execution time      |
| 0x80000019 | Conn read timeout  | 1. The request processing time is too long  2. The server is overwhelmed  3. The server is deadlocked | 1. Explicitly configure the readTimeout parameter 2. Analyze the stack on taos | 
| 0x80000020 | Unable to connect to dnodes after multiple retries                  | 1. All nodes are down 2. No available master nodes                          | 1. Check taosd status, analyze causes, or ensure a master node is active    |
| 0x80000021 | All dnode connections failed after retries                          | 1. Network issues 2. Server deadlock caused disconnection                   | 1. Check the network 2. Check request execution time                        |
| 0x80000022 | Connection limit reached                                            | 1. High concurrency, connection limit exceeded 2. Server bug not releasing | 1. Increase tsNumOfRpcSessions 2. Analyze logs for unreleased connections   |
| 0x80000023  | RPC network error                            | 1. Network issues, possibly intermittent 2. Server crash                       | 1. Check the network 2. Check if the server has restarted                 |
| 0x80000024  | RPC network bus                              | 1. When pulling data between clusters, no available connections are obtained, or the number of connections has reached the limit | 1. Check if the concurrency is too high 2. Check if there are any anomalies in the cluster nodes, such as deadlocks |
| 0x80000025  | HTTP-report already quit                     | 1. Issues with HTTP reporting                                                  | Internal issue, can be ignored                                            |
| 0x80000026  | RPC module already quit                      | 1. The client instance has already exited, but still uses the instance for queries | Check the business code to see if there is a mistake in usage             |
| 0x80000027  | RPC async module already quit                | 1. Engine error, can be ignored, this error code will not be returned to the user side | If returned to the user side, the engine side needs to investigate the issue |
| 0x80000028  | RPC async in process                         | 1. Engine error, can be ignored, this error code will not be returned to the user side | If returned to the user side, the engine side needs to investigate the issue |
| 0x80000029  | RPC no state                                 | 1. Engine error, can be ignored, this error code will not be returned to the user side | If returned to the user side, the engine side needs to investigate the issue |
| 0x8000002A  | RPC state already dropped                    | 1. Engine error, can be ignored, this error code will not be returned to the user side | If returned to the user side, the engine side needs to investigate the issue |
| 0x8000002B  | RPC msg exceed limit                         | 1. Single RPC message exceeds the limit, this error code will not be returned to the user side | If returned to the user side, the engine side needs to investigate the issue |

## common

| Error Code | Error Description               | Possible Causes                                      | Suggested Actions                                                                     |
| ---------- | -------------------------------- | --------------------------------------------------- | ------------------------------------------------------------------------------------ |
| 0x80000100 | Operation not supported          | Operation not allowed in the current scenario       | Verify if the operation is valid                                                     |
| 0x80000102 | Out of memory                    | Memory allocation failed                            | Ensure sufficient memory on the client and server                                    |
| 0x80000104 | Data file corrupted              | 1. Storage data file damaged 2. Unable to create UDF | 1. Contact Taos support 2. Check if the server has write permissions in temp folders |
| 0x80000106 | Too many Ref Objs                | No available ref resources                          | Capture logs and report issue on GitHub                                              |
| 0x80000107 | Ref ID is removed                | Referenced resource has been released               | Capture logs and report issue on GitHub                                              |
| 0x80000108 | Invalid Ref ID                   | Invalid ref ID                                      | Capture logs and report issue on GitHub                                              |
| 0x8000010A | Ref is not present               | Ref information not found                           | Capture logs and report issue on GitHub                                              |
| 0x80000111 | Action in progress               | Operation is ongoing                                | 1. Wait for completion 2. Cancel if needed 3. Report issue if it takes too long      |
| 0x80000112 | Out of range                     | Configuration parameter out of range                | Adjust the parameter                                                                 |
| 0x80000115 | Invalid message                  | Message error                                       | 1. Verify node version consistency 2. Capture logs and report issue on GitHub        |
| 0x80000116 | Invalid message length           | Message length error                                | 1. Verify node version consistency 2. Capture logs and report issue on GitHub        |
| 0x80000117 | Invalid pointer                  | Invalid pointer                                     | Capture logs and report issue on GitHub                                              |
| 0x80000118 | Invalid parameters               | Invalid parameters                                  | Capture logs and report issue on GitHub                                              |
| 0x80000119 | Invalid configuration option     | Invalid configuration                               | Capture logs and report issue on GitHub                                              |
| 0x8000011A | Invalid option                   | Invalid option                                      | Capture logs and report issue on GitHub                                              |
| 0x8000011B | Invalid JSON format              | Incorrect JSON format                               | Capture logs and report issue on GitHub                                              |
| 0x8000011C | Invalid version number           | Incorrect version format                            | Capture logs and report issue on GitHub                                              |
| 0x8000011D | Invalid version string           | Incorrect version string                            | Capture logs and report issue on GitHub                                              |
| 0x8000011E | Version not compatible           | Version mismatch between nodes                     | Ensure consistent version across nodes                                               |
| 0x8000011F | Checksum error                   | File checksum failed                                | Capture logs and report issue on GitHub                                              |
| 0x80000120 | Failed to compress message       | Compression failed                                  | Capture logs and report issue on GitHub                                              |
| 0x80000121 | Message not processed            | Message was not processed                           | Capture logs and report issue on GitHub                                              |
| 0x80000122 | Configuration not found          | Configuration item missing                          | Capture logs and report issue on GitHub                                              |
| 0x80000123 | Repeat initialization            | Duplicate initialization                            | Capture logs and report issue on GitHub                                              |
| 0x80000124 | Cannot add duplicate keys to hash| Duplicate keys in hash                              | Capture logs and report issue on GitHub                                              |
| 0x80000125 | Retry needed                     | Retry required                                      | Follow API guidelines for retries                                                    |
| 0x80000126 | Out of memory in RPC queue       | RPC queue memory limit reached                      | 1. Check system load 2. Increase rpcQueueMemoryAllowed if needed                     |
| 0x80000127 | Invalid timestamp format         | Incorrect timestamp format                          | Verify the timestamp format                                                          |
| 0x80000128 | Message decode error             | Message decoding failed                             | Capture logs and report issue on GitHub                                              |
| 0x8000012A | Not found                        | Internal cache information not found                | Capture logs and report issue on GitHub                                              |
| 0x8000012B | Out of disk space                | Insufficient disk space                             | 1. Ensure sufficient space in data and temp folders 2. Perform regular maintenance   |
| 0x80000130 | Database is starting up          | Database is starting, service unavailable           | Wait for the database to complete startup                                            |
| 0x80000131 | Database is closing down         | Database is shutting down                           | Ensure the database is in a proper state                                             |
| 0x80000132 | Invalid data format              | Incorrect data format                               | 1. Capture logs and report issue on GitHub 2. Contact Taos support                   |
| 0x80000133 | Invalid operation                | Operation not supported                            | Verify if the operation is valid                                                     |
| 0x80000134 | Invalid value                    | Invalid value                                       | Capture logs and report issue on GitHub                                              |
| 0x80000135 | Invalid FQDN                     | Incorrect FQDN configuration                        | Verify FQDN settings                                                                 |

## tsc

| Error Code | Error Description        | Possible Causes                     | Suggested Actions                                                    |
| ---------- | ------------------------ | ----------------------------------- | ---------------------------------------------------------------------|
| 0x80000207 | Invalid user name        | Invalid database username          | Check if the database username is correct                            |
| 0x80000208 | Invalid password         | Invalid database password          | Verify the database password                                         |
| 0x80000209 | Database name too long   | Database name is invalid           | Verify the correctness of the database name                          |
| 0x8000020A | Table name too long      | Table name is invalid              | Verify the correctness of the table name                              |
| 0x8000020F | Query terminated         | Query was terminated               | Check if a user manually stopped the query                           |
| 0x80000213 | Disconnected from server | Connection was interrupted         | Verify if the connection was closed manually or the client exited    |
| 0x80000216 | Syntax error in SQL      | SQL syntax error                   | Check and correct the SQL statement                                  |
| 0x80000219 | SQL statement too long   | SQL exceeds length limit           | Shorten the SQL statement if needed                                  |
| 0x8000021A | File is empty            | Input file is empty                | Verify the content of the input file                                 |
| 0x8000021F | Invalid column length    | Column length error                | Capture logs and report the issue on GitHub                          |
| 0x80000222 | Invalid JSON data type   | Incorrect JSON data type           | Verify the JSON input                                                |
| 0x80000224 | Value out of range       | Value exceeds the data type range  | Verify the input values                                              |
| 0x80000229 | Invalid tsc input        | API input error                    | Verify the parameters passed to the API                              |
| 0x8000022A | Stmt API usage error     | Incorrect usage of STMT API        | Check the API call sequence, scenarios, and error handling           |
| 0x8000022B | Stmt table name not set  | Table name not properly set        | Verify if the table name was set using the correct API               |
| 0x8000022D | Query killed             | Query was terminated               | Verify if a user interrupted the query                               |
| 0x8000022E | No available execution node | No available query execution nodes | Verify query policy settings and ensure Qnode nodes are available  |
| 0x8000022F | Table is not a super table | Table in query is not a supertable | Verify if the table is a supertable                                  |
| 0x80000230 | Stmt cache error         | STMT internal cache error          | Capture logs and report the issue on GitHub                          |
| 0x80000231 | TSC internal error       | Internal error in TSC              | Capture logs and report the issue on GitHub                          |

## mnode

| Error Code | Error Description                              | Possible Causes                               | Suggested Actions                                                                 |
| ---------- | ---------------------------------------------- | --------------------------------------------- | -------------------------------------------------------------------------------- |
| 0x80000303 | Insufficient privilege for operation           | No permission                                | Grant the necessary privileges                                                   |
| 0x8000030B | Data expired                                   | Internal error                               | Report the issue on GitHub                                                       |
| 0x8000030C | Invalid query ID                               | Internal error                               | Report the issue on GitHub                                                       |
| 0x8000030E | Invalid connection ID                          | Internal error                               | Report the issue on GitHub                                                       |
| 0x80000315 | User is disabled                               | User unavailable                             | Grant the necessary privileges                                                   |
| 0x80000320 | Object already exists                          | Internal error                               | Report the issue on GitHub                                                       |
| 0x80000322 | Invalid table type                             | Internal error                               | Report the issue on GitHub                                                       |
| 0x80000323 | Object does not exist                          | Internal error                               | Report the issue on GitHub                                                       |
| 0x80000326 | Invalid action type                            | Internal error                               | Report the issue on GitHub                                                       |
| 0x80000328 | Invalid raw data version                       | Internal error                               | Report the issue on GitHub                                                       |
| 0x80000329 | Invalid raw data length                        | Internal error                               | Report the issue on GitHub                                                       |
| 0x8000032A | Invalid raw data content                       | Internal error                               | Report the issue on GitHub                                                       |
| 0x8000032C | Object is being created                        | Internal error                               | Report the issue on GitHub                                                       |
| 0x8000032D | Object is being dropped                        | Internal error                               | Report the issue on GitHub                                                       |
| 0x80000330 | Dnode already exists                           | Internal error                               | Report the issue on GitHub                                                       |
| 0x80000331 | Dnode does not exist                           | Internal error                               | Report the issue on GitHub                                                       |
| 0x80000332 | Vgroup does not exist                          | Internal error                               | Report the issue on GitHub                                                       |
| 0x80000333 | Cannot drop mnode which is a leader            | Node is a leader                             | Confirm if the operation is correct                                              |
| 0x80000334 | Out of dnodes                                  | Insufficient dnode nodes                     | Add more dnode nodes                                                             |
| 0x80000335 | Cluster configuration inconsistent             | Configuration mismatch                       | Check dnode and mnode configuration for consistency                              |
| 0x8000033B | Cluster ID mismatch                            | Inconsistent cluster configuration           | Check the clusterid in each node’s `data/dnode/dnodes.json`                      |
| 0x80000340 | Account already exists                         | (Enterprise only) Internal error             | Report the issue on GitHub                                                       |
| 0x80000342 | Invalid account options                        | (Enterprise only) Unsupported operation      | Confirm if the operation is correct                                              |
| 0x80000344 | Invalid account                                | Account does not exist                       | Verify the account                                                               |
| 0x80000350 | User already exists                            | Duplicate user creation                      | Confirm if the operation is correct                                              |
| 0x80000351 | Invalid user                                   | User does not exist                          | Verify the user                                                                  |
| 0x80000352 | Invalid user format                            | Incorrect format                             | Verify the format                                                                |
| 0x80000353 | Invalid password format                        | Incorrect format                             | Verify the password format                                                       |
| 0x80000354 | Cannot retrieve user from connection           | Internal error                               | Report the issue on GitHub                                                       |
| 0x80000355 | Too many users                                 | (Enterprise only) User limit exceeded        | Adjust the configuration                                                         |
| 0x80000357 | Authentication failure                         | Incorrect password                           | Confirm if the operation is correct                                              |
| 0x80000358 | User not available                             | User does not exist                          | Verify the user                                                                  |
| 0x80000360 | STable already exists                          | Internal error                               | Report the issue on GitHub                                                       |
| 0x80000361 | STable does not exist                          | Internal error                               | Report the issue on GitHub                                                       |
| 0x80000364 | Too many tags                                  | Too many tags                                | Cannot modify – code-level limitation                                            |
| 0x80000365 | Too many columns                               | Too many columns                             | Cannot modify – code-level limitation                                            |
| 0x80000369 | Tag already exists                             | Tag already exists                           | Confirm if the operation is correct                                              |
| 0x8000036A | Tag does not exist                             | Tag does not exist                           | Confirm if the operation is correct                                              |
| 0x8000036B | Column already exists                          | Column already exists                        | Confirm if the operation is correct                                              |
| 0x8000036C | Column does not exist                          | Column does not exist                        | Confirm if the operation is correct                                              |
| 0x8000036E | Invalid stable options                         | Internal error                               | Report the issue on GitHub                                                       |
| 0x8000036F | Invalid row bytes                              | Internal error                               | Report the issue on GitHub                                                       |
| 0x80000370 | Invalid function name                          | Incorrect name length                        | Confirm if the operation is correct                                              |
| 0x80000372 | Invalid function code                          | Incorrect code length                        | Confirm if the operation is correct                                              |
| 0x80000373 | Function already exists                        | Function already exists                      | Confirm if the operation is correct                                              |
| 0x80000374 | Function does not exist                        | Function does not exist                      | Confirm if the operation is correct                                              |
| 0x80000375 | Invalid function buffer size                   | Buffer size incorrect or exceeded limit      | Confirm if the operation is correct                                              |
| 0x80000378 | Invalid function comment                       | Incorrect or exceeded length                 | Confirm if the operation is correct                                              |
| 0x80000379 | Invalid function retrieve message              | Incorrect or exceeded length                 | Confirm if the operation is correct                                              |
| 0x80000380 | Database not specified or available            | Database not specified                       | Use the `use database;` command                                                  |
| 0x80000381 | Database already exists                        | Database already exists                      | Confirm if the operation is correct                                              |
| 0x80000382 | Invalid database options                       | Internal error                               | Report the issue on GitHub                                                       |
| 0x80000383 | Invalid database name                          | Incorrect length                             | Confirm if the operation is correct                                              |
| 0x80000385 | Too many databases for the account             | Exceeded limit                               | Adjust the configuration                                                         |
| 0x80000386 | Database in dropping status                    | Database is being dropped                    | Retry, or report the issue if the state persists                                 |
| 0x80000388 | Database does not exist                        | Database not found                           | Confirm if the operation is correct                                              |
| 0x80000389 | Invalid database account                       | Internal error                               | Report the issue on GitHub                                                       |
| 0x8000038A | Database options not changed                   | No changes were made                         | Confirm if the operation is correct                                              |
| 0x8000038B | Index does not exist                           | Index not found                              | Confirm if the operation is correct                                              |
| 0x80000396 | Database in creating status                    | Database is being created                    | Retry                                                                           |
| 0x8000039A | Invalid system table name                      | Internal error                               | Report the issue on GitHub                                                       |
| 0x800003A0 | Mnode already exists                           | Already exists                               | Confirm if the operation is correct                                              |
| 0x800003A1 | Mnode not present                              | Does not exist                               | Confirm if the operation is correct                                              |
| 0x800003A2 | Qnode already exists                           | Already exists                               | Confirm if the operation is correct                                              |
| 0x800003A3 | Qnode not present                              | Does not exist                               | Confirm if the operation is correct                                              |
| 0x800003A4 | Snode already exists                           | Already exists                               | Confirm if the operation is correct                                              |
| 0x800003A5 | Snode not present                              | Does not exist                               | Confirm if the operation is correct                                              |
| 0x800003A8 | The replica of mnode cannot be less than 1     | Less than 1 replica                          | Operation not allowed                                                           |
| 0x800003A9 | The replica of mnode cannot exceed 3           | More than 3 replicas                         | Operation not allowed                                                           |
| 0x800003B1 | Not enough memory in dnode                     | Insufficient memory                          | Adjust the configuration                                                         |
| 0x800003B3 | Invalid dnode endpoint                         | Incorrect endpoint configuration             | Confirm if the operation is correct                                              |
| 0x800003B6 | Offline dnode exists                           | Dnode is offline                             | Check the node status                                                           |
| 0x800003B7 | Invalid vgroup replica                         | Internal error                               | Report the issue on GitHub                                                       |
| 0x800003B8 | Dnode in creating status                       | Dnode is being created                       | Retry                                                                           |
| 0x800003B9 | Dnode in dropping status                       | Dnode is being dropped                       | Retry                                                                           |
| 0x800003C2 | Invalid stable alter options                   | Internal error                               | Report the issue on GitHub                                                       |
| 0x800003C3 | STable option unchanged                        | No changes were made                         | Confirm if the operation is correct                                              |
| 0x800003C4 | Field used by topic                            | In use                                       | Confirm if the operation is correct                                              |
| 0x800003C5 | Database is single stable mode                 | Internal error                               | Report the issue on GitHub                                                       |
| 0x800003C6 | Invalid schema version while altering stb      | Internal error                               | Report the issue on GitHub                                                       |
| 0x800003C7 | Invalid stable UID while altering stb          | Internal error                               | Report the issue on GitHub                                                       |
| 0x800003C8 | Field used by TSMA                              | In use                                       | Confirm if the operation is correct                                              |
| 0x800003D1 | Transaction does not exist                     | Transaction not found                        | Confirm if the operation is correct                                              |
| 0x800003D2 | Invalid stage to kill                          | Cannot kill transaction at this stage        | Wait for the transaction to finish, or report if it persists                     |
| 0x800003D3 | Conflict transaction not completed             | Conflicting transaction                      | Use `show transactions` to identify the conflict, or report the issue            |
| 0x800003D4 | Transaction commit log is null                 | Internal error                               | Report the issue on GitHub                                                       |
| 0x800003D5 | Unable to establish connection                 | Network error                                | Verify the network                                                              |
| 0x800003D6 | Last transaction not finished                  | Internal error                               | Report the issue on GitHub                                                       |
| 0x800003D7 | Sync timeout during transaction                | Internal error                               | Report the issue on GitHub                                                       |
| 0x800003DF | Unknown transaction error                      | Internal error                               | Report the issue on GitHub                                                       |
| 0x800003E0 | Topic already exists                           | Already exists                               | Confirm if the operation is correct                                              |
| 0x800003E1 | Topic does not exist                           | Does not exist                               | Confirm if the operation is correct                                              |
| 0x800003E3 | Invalid topic                                  | Internal error                               | Report the issue on GitHub                                                       |
| 0x800003E4 | Topic with invalid query                       | Internal error                               | Report the issue on GitHub                                                       |
| 0x800003E5 | Topic with invalid option                      | Internal error                               | Report the issue on GitHub                                                       |
| 0x800003E6 | Consumer does not exist                        | Does not exist                               | Confirm if the operation is correct                                              |
| 0x800003E7 | Topic unchanged                                | No changes were made                         | Confirm if the operation is correct                                              |
| 0x800003E8 | Subscription does not exist                    | Does not exist                               | Confirm if the operation is correct                                              |
| 0x800003E9 | Offset does not exist                          | Does not exist                               | Confirm if the operation is correct                                              |
| 0x800003EA | Consumer not ready                             | Internal error                               | Report the issue on GitHub                                                       |
| 0x800003EB | Topic subscribed cannot be dropped             | In use                                       | Confirm if the operation is correct                                              |
| 0x800003EC | Consumer group used by some consumer           | In use                                       | Confirm if the operation is correct                                              |
| 0x800003ED | Topic must be dropped first                    | In use                                       | Confirm if the operation is correct                                              |
| 0x800003EE | Invalid subscription option                    | Internal error                               | Confirm if the operation is correct                                              |
| 0x800003EF | Topic being rebalanced                         | In progress                                  | Retry                                                                           |
| 0x800003F0 | Stream already exists                          | Already exists                               | Confirm if the operation is correct                                              |
| 0x800003F1 | Stream does not exist                          | Does not exist                               | Confirm if the operation is correct                                              |
| 0x800003F2 | Invalid stream option                          | Internal error                               | Report the issue on GitHub                                                       |
| 0x800003F3 | Stream must be dropped first                   | In use                                       | Confirm if the operation is correct                                              |
| 0x800003F5 | Stream temporarily does not support replica >1 | Exceeds limit                                | Operation not allowed                                                           |
| 0x800003F6 | Too many streams                               | Exceeds limit                                | Cannot modify – code-level limitation                                            |
| 0x800003F7 | Cannot write the same stable as other stream   | Internal error                               | Report the issue on GitHub                                                       |
| 0x80000480 | Index already exists                           | Already exists                               | Confirm if the operation is correct                                              |
| 0x80000481 | Index does not exist                           | Does not exist                               | Confirm if the operation is correct                                              |
| 0x80000482 | Invalid SMA index option                       | Internal error                               | Report the issue on GitHub                                                       |
| 0x80000483 | Index already exists                           | Already exists                               | Confirm if the operation is correct                                              |
| 0x80000484 | Index does not exist                           | Does not exist                               | Confirm if the operation is correct                                              |

## dnode

| Error Code | Error Description               | Possible Causes                       | Suggested Actions                  |
| ---------- | ------------------------------- | ------------------------------------- | ---------------------------------- |
| 0x80000408 | Dnode is offline                | Node is offline                      | Check the node status              |
| 0x80000409 | Mnode already deployed          | Already deployed                     | Confirm if the operation is correct |
| 0x8000040A | Mnode not found                 | Internal error                       | Report the issue on GitHub         |
| 0x8000040B | Mnode not deployed              | Internal error                       | Report the issue on GitHub         |
| 0x8000040C | Qnode already deployed          | Already deployed                     | Confirm if the operation is correct |
| 0x8000040D | Qnode not found                 | Internal error                       | Report the issue on GitHub         |
| 0x8000040E | Qnode not deployed              | Internal error                       | Report the issue on GitHub         |
| 0x8000040F | Snode already deployed          | Already deployed                     | Confirm if the operation is correct |
| 0x80000410 | Snode not found                 | Internal error                       | Report the issue on GitHub         |
| 0x80000411 | Snode not deployed              | Already deployed                     | Confirm if the operation is correct |

## vnode

| Error Code | Error Description                                | Possible Causes                           | Suggested Actions                  |
| ---------- | ------------------------------------------------ | ----------------------------------------- | ---------------------------------- |
| 0x80000503 | Invalid vgroup ID                                | Outdated client cache, internal error    | Report the issue on GitHub         |
| 0x80000512 | No writing privilege                             | No write permission                      | Request the necessary authorization |
| 0x80000520 | Vnode does not exist                             | Internal error                           | Report the issue on GitHub         |
| 0x80000521 | Vnode already exists                             | Internal error                           | Report the issue on GitHub         |
| 0x80000522 | Hash value of table is not in the vnode hash range | Table does not belong to the vnode       | Report the issue on GitHub         |
| 0x80000524 | Invalid table operation                          | Illegal table operation                  | Report the issue on GitHub         |
| 0x80000525 | Column already exists                            | Column already exists when modifying the table | Report the issue on GitHub   |
| 0x80000526 | Column does not exist                            | Column does not exist when modifying the table | Report the issue on GitHub   |
| 0x80000527 | Column is subscribed                             | Column is subscribed, operation denied   | Report the issue on GitHub         |
| 0x80000529 | Vnode is stopped                                 | Vnode has been stopped                   | Report the issue on GitHub         |
| 0x80000530 | Duplicate write request                          | Duplicate write request, internal error | Report the issue on GitHub         |
| 0x80000531 | Vnode query is busy                              | Query is busy                            | Report the issue on GitHub         |

## tsdb

| Error Code | Error Description                       | Possible Causes                              | Suggested Actions                  |
| ---------- | --------------------------------------- | -------------------------------------------- | ---------------------------------- |
| 0x80000600 | Invalid table ID to write               | Target table does not exist                  | Restart the client                 |
| 0x80000602 | Invalid table schema version            | Outdated schema version, internal error     | No action needed, automatic update |
| 0x80000603 | Table already exists                    | Table already exists                        | Report the issue on GitHub         |
| 0x80000604 | Invalid configuration                   | Internal error                              | Report the issue on GitHub         |
| 0x80000605 | Initialization failed                   | Startup failed                              | Report the issue on GitHub         |
| 0x8000060B | Timestamp is out of range               | Timestamp exceeds allowable range           | Verify and adjust the application’s timestamp logic |
| 0x8000060C | Submit message is corrupted             | Message error, possibly due to client-server incompatibility | Report the issue on GitHub |
| 0x80000618 | Table does not exist                    | Table already exists                        | Report the issue on GitHub         |
| 0x80000619 | Super table already exists              | Supertable already exists                 | Report the issue on GitHub         |
| 0x8000061A | Super table does not exist              | Supertable not found                      | Report the issue on GitHub         |
| 0x8000061B | Invalid table schema version            | Same as TSDB_CODE_TDB_IVD_TB_SCHEMA_VERSION | Report the issue on GitHub         |
| 0x8000061D | Table already exists in another super table | Table belongs to a different supertable   | Verify the write logic             |

## query

| Error Code | Error Description                     | Possible Causes                              | Suggested Actions                  |
| ---------- | ------------------------------------- | -------------------------------------------- | ---------------------------------- |
| 0x80000700 | Invalid query handle                  | Query handle not found                       | Preserve logs and report the issue on GitHub |
| 0x80000709 | Multiple retrieval of this query      | Subquery is already in progress              | Preserve logs and report the issue on GitHub |
| 0x8000070A | Too many groups/time windows in query | Exceeds the limit for groups or windows      | Adjust the query to reduce the number of groups or windows |
| 0x8000070D | System error                          | System API returned an error                 | Preserve logs and report the issue on GitHub |
| 0x80000720 | Scheduler not found                   | Corresponding client information not found   | Preserve logs and report the issue on GitHub |
| 0x80000721 | Task not found                        | Subquery not found                           | Preserve logs and report the issue on GitHub |
| 0x80000722 | Task already exists                   | Subquery already exists                      | Preserve logs and report the issue on GitHub |
| 0x80000729 | Task message error                    | Query message error                          | Preserve logs and report the issue on GitHub |
| 0x8000072B | Task status error                     | Subquery status error                        | Preserve logs and report the issue on GitHub |
| 0x8000072F | Job not found                         | Query job not found                          | Preserve logs and report the issue on GitHub |

## grant

| Error Code | Error Description                     | Possible Causes                        | Suggested Actions                  |
| ---------- | ------------------------------------- | -------------------------------------- | ---------------------------------- |
| 0x80000800 | License expired                       | License has expired                   | Verify license information and contact support to update the license |
| 0x80000801 | DNode creation limited by license     | Exceeded the licensed limit for dnodes | Verify license information and contact support to update the license |
| 0x80000802 | Account creation limited by license   | Exceeded the licensed limit for accounts | Verify license information and contact support to update the license |
| 0x80000803 | Time series limited by license        | Exceeded the licensed limit for data points | Verify license information and contact support to update the license |
| 0x80000804 | Database creation limited by license  | Exceeded the licensed limit for databases | Verify license information and contact support to update the license |
| 0x80000805 | User creation limited by license      | Exceeded the licensed limit for users  | Verify license information and contact support to update the license |
| 0x80000806 | Connection creation limited by license | Exceeded the licensed limit for connections | No limit currently enforced; contact support for further inspection |
| 0x80000807 | Stream creation limited by license    | Exceeded the licensed limit for streams | No limit currently enforced; contact support for further inspection |
| 0x80000808 | Write speed limited by license        | Exceeded the licensed write speed      | No limit currently enforced; contact support for further inspection |
| 0x80000809 | Storage capacity limited by license   | Exceeded the licensed storage capacity | Verify license information and contact support to update the license |
| 0x8000080A | Query time limited by license         | Exceeded the licensed query limit      | No limit currently enforced; contact support for further inspection |
| 0x8000080B | CPU cores limited by license          | Exceeded the licensed limit for CPU cores | No limit currently enforced; contact support for further inspection |
| 0x8000080C | STable creation limited by license    | Exceeded the licensed limit for supertables | Verify license information and contact support to update the license |
| 0x8000080D | Table creation limited by license     | Exceeded the licensed limit for tables | Verify license information and contact support to update the license |

## sync

| Error Code | Error Description               | Possible Causes                                                                                            | Suggested Actions                                                    |
| ---------- | ------------------------------- | ----------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| 0x80000903 | Sync timeout                    | Scenario 1: Leader switch occurred; the request on the old leader timed out during negotiation. Scenario 2: Timeout due to slow response from the follower. | Check cluster status with `show vgroups`; review logs and network conditions between server nodes. |
| 0x8000090C | Sync leader is unreachable      | Scenario 1: During leader election. Scenario 2: Client request routed to a follower, and redirection failed. Scenario 3: Network configuration error on client or server. | Check cluster status, network configuration, and application access status. Review server logs and network conditions between nodes. |
| 0x8000090F | Sync new config error           | Configuration error during membership change.                                                              | Internal error; user intervention not possible.                      |
| 0x80000911 | Sync not ready to propose       | Scenario 1: Recovery not complete.                                                                          | Check cluster status with `show vgroups`; review logs and network conditions between nodes. |
| 0x80000914 | Sync leader is restoring        | Scenario 1: Leader switch occurred; log replay in progress on the new leader.                                | Check cluster status with `show vgroups`; monitor recovery progress in server logs. |
| 0x80000915 | Sync invalid snapshot msg       | Snapshot replication message error.                                                                         | Internal server error.                                                |
| 0x80000916 | Sync buffer is full             | Scenario: High concurrency or network/CPU bottlenecks caused buffer overflow.                               | Check system resources (e.g., disk I/O, CPU, and network); monitor network connectivity between nodes. |
| 0x80000917 | Sync write stall                | Scenario: Execution stalled due to high load, insufficient disk I/O, or write failure.                      | Check system resources (e.g., disk I/O, CPU) and investigate disk write failures. |
| 0x80000918 | Sync negotiation win is full    | Scenario: High concurrency caused buffer overflow.                                                          | Monitor system resources and network conditions between nodes.       |
| 0x800009FF | Sync internal error             | Other internal errors.                                                                                      | Check cluster status with `show vgroups`.                             |

## tq

| Error Code | Error Description               | Possible Causes                                                                 | Suggested Actions                    |
| ---------- | -------------------------------- | ------------------------------------------------------------------------------ | ------------------------------------ |
| 0x80000A0C | TQ table schema not found        | Table not found during data consumption.                                       | Internal error; not exposed to users. |
| 0x80000A0D | TQ no committed offset           | Offset reset set to `none`, and no previous offset exists on the server side. | Set offset reset to `earliest` or `latest`. |

## wal

| Error Code | Error Description               | Possible Causes                        | Suggested Actions |
| ---------- | -------------------------------- | -------------------------------------- | ------------------ |
| 0x80001001 | WAL file is corrupted           | WAL file corrupted.                    | Internal server error. |
| 0x80001003 | WAL invalid version             | Log version out of range.              | Internal server error. |
| 0x80001005 | WAL log not exist               | Requested log entry not found.         | Internal server error. |
| 0x80001006 | WAL checksum mismatch           | WAL file corruption occurred.          | Internal server error. |
| 0x80001007 | WAL log incomplete              | Log file missing or corrupted.         | Internal server error. |

## tfs

| Error Code | Error Description               | Possible Causes                                 | Suggested Actions                    |
| ---------- | -------------------------------- | ----------------------------------------------- | ------------------------------------ |
| 0x80002201 | TFS invalid configuration        | Multi-tier storage configuration error.        | Verify the configuration.           |
| 0x80002202 | TFS too many disks on one level  | Too many disks configured at one tier.         | Check if the number of disks exceeds the limit. |
| 0x80002203 | TFS duplicate primary mount disk | Duplicate primary disk configuration.          | Verify the configuration.           |
| 0x80002204 | TFS no primary mount disk        | Primary disk not configured.                   | Verify the configuration.           |
| 0x80002205 | TFS no disk mount on tier        | No disk configured for the specified tier.     | Verify the configuration.           |
| 0x80002208 | No disk available on a tier.     | No available disk, usually due to full storage. | Add more disks to increase capacity. |

## catalog

| Error Code | Error Description               | Possible Causes                         | Suggested Actions                     |
| ---------- | -------------------------------- | --------------------------------------- | ------------------------------------- |
| 0x80002400 | catalog internal error           | Internal catalog error.                | Preserve logs and report the issue on GitHub. |
| 0x80002401 | catalog invalid input parameters | Invalid input parameters.              | Preserve logs and report the issue on GitHub. |
| 0x80002402 | catalog is not ready             | Catalog not fully initialized.         | Preserve logs and report the issue on GitHub. |
| 0x80002403 | catalog system error             | System error in the catalog module.    | Preserve logs and report the issue on GitHub. |
| 0x80002404 | Database is dropped              | Database cache deleted.                | Preserve logs and report the issue on GitHub. |
| 0x80002405 | catalog is out of service        | Catalog module stopped.                | Preserve logs and report the issue on GitHub. |
| 0x80002550 | Invalid msg order                | Message sequence error.                | Preserve logs and report the issue on GitHub. |
| 0x80002501 | Job status error                 | Job status error.                      | Preserve logs and report the issue on GitHub. |
| 0x80002502 | scheduler internal error         | Internal scheduler error.              | Preserve logs and report the issue on GitHub. |
| 0x80002504 | Task timeout                     | Sub-task timeout.                      | Preserve logs and report the issue on GitHub. |
| 0x80002505 | Job is dropping                  | Job is being canceled.                 | Check if the task was manually or programmatically interrupted. |

## parser

| Error Code | Error Description                                            | Possible Causes                                              | Suggested Actions                                       |
| ---------- | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------- |
| 0x80002600 | syntax error near                                            | SQL syntax error                                             | Check and correct the SQL statement.                    |
| 0x80002601 | Incomplete SQL statement                                     | Incomplete SQL statement                                     | Check and complete the SQL statement.                   |
| 0x80002602 | Invalid column name                                          | Illegal or non-existent column name                          | Check and correct the SQL statement.                    |
| 0x80002603 | Table does not exist                                         | Table not found                                              | Verify the existence of the table.                      |
| 0x80002604 | Column ambiguously defined                                   | Duplicate column (or alias) definitions                      | Check and correct the SQL statement.                    |
| 0x80002605 | Invalid value type                                           | Illegal constant value                                       | Check and correct the SQL statement.                    |
| 0x80002608 | There mustn't be aggregation                                 | Aggregation function in an illegal clause                    | Check and correct the SQL statement.                    |
| 0x80002609 | ORDER BY item must be the number of a SELECT-list expression | Invalid `ORDER BY` position                                  | Check and correct the SQL statement.                    |
| 0x8000260A | Not a GROUP BY expression                                    | Illegal `GROUP BY` clause                                    | Check and correct the SQL statement.                    |
| 0x8000260B | Not SELECTed expression                                      | Invalid expression                                           | Check and correct the SQL statement.                    |
| 0x8000260C | Not a single-group group function                            | Illegal combination of columns and functions                 | Check and correct the SQL statement.                    |
| 0x8000260D | Tags number not matched                                      | Mismatch in tag columns                                      | Check and correct the SQL statement.                    |
| 0x8000260E | Invalid tag name                                             | Invalid or non-existent tag name                             | Check and correct the SQL statement.                    |
| 0x80002610 | Value is too long                                            | Value exceeds the allowed limit                              | Check and correct SQL or API parameters.                |
| 0x80002611 | Password can not be empty                                    | Empty password                                               | Use a valid password.                                   |
| 0x80002612 | Port should be an integer that is less than 65535 and greater than 0 | Invalid port number                                          | Check and correct the port number.                      |
| 0x80002613 | Endpoint should be in the format of 'fqdn:port'              | Invalid endpoint format                                      | Check and correct the address.                          |
| 0x80002614 | This statement is no longer supported                        | Deprecated feature                                           | Refer to the feature documentation.                     |
| 0x80002615 | Interval too small                                           | Interval value below the minimum allowed                     | Increase the interval value.                            |
| 0x80002616 | Database not specified                                       | No database specified                                        | Specify the target database.                            |
| 0x80002617 | Invalid identifier name                                      | Illegal or invalid-length identifier name                    | Check the names of databases, tables, columns, or tags. |
| 0x80002618 | Corresponding super table not in this db                     | supertable not found in the database                         | Verify the existence of the supertable.                 |
| 0x80002619 | Invalid database option                                      | Illegal database option value                                | Check and correct the database option value.            |
| 0x8000261A | Invalid table option                                         | Illegal table option value                                   | Check and correct the table option value.               |
| 0x80002624 | GROUP BY and WINDOW-clause can't be used together            | `GROUP BY` and `WINDOW` cannot be used together              | Check and correct the SQL statement.                    |
| 0x80002627 | Aggregate functions do not support nesting                   | Nested aggregate functions are not supported                 | Check and correct the SQL statement.                    |
| 0x80002628 | Only support STATE_WINDOW on integer/bool/varchar column     | Unsupported `STATE_WINDOW` data type                         | Check and correct the SQL statement.                    |
| 0x80002629 | Not support STATE_WINDOW on tag column                       | `STATE_WINDOW` not supported on tag columns                  | Check and correct the SQL statement.                    |
| 0x8000262A | STATE_WINDOW not support for super table query               | `STATE_WINDOW` not supported for supertables                 | Check and correct the SQL statement.                    |
| 0x8000262B | SESSION gap should be fixed time window, and greater than 0  | Invalid session window value                                 | Check and correct the SQL statement.                    |
| 0x8000262C | Only support SESSION on primary timestamp column             | Invalid session window column                                | Check and correct the SQL statement.                    |
| 0x8000262D | Interval offset cannot be negative                           | Illegal interval offset value                                | Check and correct the SQL statement.                    |
| 0x8000262E | Cannot use 'year' as offset when interval is 'month'         | Illegal interval offset unit                                 | Check and correct the SQL statement.                    |
| 0x8000262F | Interval offset should be shorter than interval              | Invalid interval offset value                                | Check and correct the SQL statement.                    |
| 0x80002630 | Does not support sliding when interval is natural month/year | Invalid sliding unit                                         | Check and correct the SQL statement.                    |
| 0x80002631 | sliding value no larger than the interval value              | Invalid sliding value                                        | Check and correct the SQL statement.                    |
| 0x80002632 | sliding value can not less than 1%% of interval value        | Invalid sliding value                                        | Check and correct the SQL statement.                    |
| 0x80002633 | Only one tag if there is a json tag                          | Only one JSON tag allowed                                    | Check and correct the SQL statement.                    |
| 0x80002634 | Query block has incorrect number of result columns           | Mismatched number of columns in result                       | Check and correct the SQL statement.                    |
| 0x80002635 | Incorrect TIMESTAMP value                                    | Invalid timestamp value                                      | Check and correct the SQL statement.                    |
| 0x80002637 | soffset/offset can not be less than 0                        | Invalid `soffset`/`offset` value                             | Check and correct the SQL statement.                    |
| 0x80002638 | slimit/soffset only available for PARTITION/GROUP BY query   | `slimit`/`soffset` only allowed in `PARTITION BY`/`GROUP BY` | Check and correct the SQL statement.                    |
| 0x80002639 | Invalid topic query                                          | Unsupported topic query                                      | Check and correct the SQL statement.                    |
| 0x8000263A | Cannot drop super table in batch                             | Batch deletion of supertables not supported                  | Check and correct the SQL statement.                    |
| 0x8000263B | Start(end) time of query range required or time range too large | Query time range exceeds limit                               | Check and correct the SQL statement.                    |
| 0x8000263C | Duplicated column names                                      | Duplicate column names                                       | Check and correct the SQL statement.                    |
| 0x8000263D | Tags length exceeds max length                               | Tag length exceeds the maximum allowed                       | Check and correct the SQL statement.                    |
| 0x8000263E | Row length exceeds max length                                | Row length exceeds the limit                                 | Check and correct the SQL statement.                    |
| 0x8000263F | Illegal number of columns                                    | Incorrect number of columns                                  | Check and correct the SQL statement.                    |
| 0x80002640 | Too many columns                                             | Number of columns exceeds the limit                          | Check and correct the SQL statement.                    |
| 0x80002641 | First column must be timestamp                               | The first column must be a timestamp                         | Check and correct the SQL statement.                    |
| 0x80002642 | Invalid binary/nchar column/tag length                       | Invalid binary/nchar length                                  | Check and correct the SQL statement.                    |
| 0x80002643 | Invalid number of tag columns                                | Incorrect number of tag columns                              | Check and correct the SQL statement.                    |
| 0x80002644 | Permission denied                                            | Permission error                                             | Verify user permissions.                                |
| 0x80002645 | Invalid stream query                                         | Invalid stream query                                         | Check and correct the SQL statement.                    |
| 0x80002646 | Invalid `_c0` or `_rowts` expression                         | Invalid use of `_c0` or `_rowts`                             | Check and correct the SQL statement.                    |
| 0x80002647 | Invalid timeline function                                    | Missing primary timestamp column                             | Check and correct the SQL statement.                    |
| 0x80002648 | Invalid password                                             | Password does not meet the criteria                          | Check and modify the password.                          |
| 0x80002649 | Invalid alter table statement                                | Invalid `ALTER TABLE` statement                              | Check and correct the SQL statement.                    |
| 0x8000264A | Primary timestamp column cannot be dropped                   | Timestamp column cannot be deleted                           | Check and correct the SQL statement.                    |
| 0x8000264B | Only binary/nchar column length could be modified, and the length can only be increased, not decreased | Illegal column modification                                  | Check and correct the SQL statement.                    |
| 0x8000264C | Invalid tbname pseudo column                                 | Invalid use of `tbname` column                               | Check and correct the SQL statement.                    |
| 0x8000264D | Invalid function name                                        | Invalid function name                                        | Check and correct the function name.                    |
| 0x8000264E | Comment too long                                             | Comment length exceeds the limit                             | Check and correct the SQL statement.                    |
| 0x8000264F | Function(s) only allowed in SELECT list, cannot mixed with non scalar functions or columns | Invalid mixing of functions                                  | Check and correct the SQL statement.                    |
| 0x80002650 | Window query not supported, since no valid timestamp column included in the result of subquery | Missing primary timestamp column in result                   | Check and correct the SQL statement.                    |
| 0x80002651 | No columns can be dropped                                    | Essential columns cannot be deleted                          | Check and correct the SQL statement.                    |
| 0x80002652 | Only tag can be json type                                    | Only tags can be of JSON type                                | Check and correct the SQL statement.                    |
| 0x80002655 | The DELETE statement must have a definite time window range  | Invalid `WHERE` condition in `DELETE`                        | Check and correct the SQL statement.                    |
| 0x80002656 | The REDISTRIBUTE VGROUP statement only support 1 to 3 dnodes | Invalid number of `DNODE` in `REDISTRIBUTE VGROUP`           | Check and correct the SQL statement.                    |
| 0x80002657 | Fill now allowed                                             | `FILL` function not allowed                                  | Check and correct the SQL statement.                    |
| 0x80002658 | Invalid windows pc                                           | Invalid use of window pseudo-columns                         | Check and correct the SQL statement.                    |
| 0x80002659 | Window not allowed                                           | Function cannot be used within a window                      | Check and correct the SQL statement.                    |
| 0x8000265A | Stream not allowed                                           | Function cannot be used within a stream                      | Check and correct the SQL statement.                    |
| 0x8000265B | Group by not allowed                                         | Function cannot be used with `GROUP BY`                      | Check and correct the SQL statement.                    |
| 0x8000265D | Invalid interp clause                                        | Invalid `INTERP` clause                                      | Check and correct the SQL statement.                    |
| 0x8000265E | Not valid function in window                                 | Invalid window function usage                                | Check and correct the SQL statement.                    |
| 0x8000265F | Only support single table                                    | Function supports only single-table queries                  | Check and correct the SQL statement.                    |
| 0x80002660 | Invalid sma index                                            | Invalid SMA index                                            | Check and correct the SQL statement.                    |
| 0x80002661 | Invalid SELECTed expression                                  | Invalid query expression                                     | Check and correct the SQL statement.                    |
| 0x80002662 | Fail to get table info                                       | Failed to retrieve table metadata                            | Preserve logs and report the issue on GitHub.           |
| 0x80002663 | Not unique table/alias                                       | Conflicting table names or aliases                           | Check and correct the SQL statement.                    |
| 0x80002664 | Join requires valid time series input                        | JOIN queries require primary timestamp columns               | Check and correct the SQL statement.                    |
| 0x80002665 | The \_TAGS pseudo column can only be used for subtable and supertable queries | Invalid use of `_TAGS` pseudo column                         | Check and correct the SQL statement.                    |
| 0x80002666 | Subquery output does not contain a primary timestamp column  | Ensure that subqueries contain timestamp columns.            |                                                         |
| 0x80002667 | Invalid usage of expr: %s                                    | Invalid expression usage                                     | Check and correct the SQL statement.                    |
| 0x800026FF | Parser internal error                                        | Internal parser error                                        | Preserve logs and report the issue on GitHub.           |
| 0x80002700 | Planner internal error                                       | Internal planner error                                       | Preserve logs and report the issue on GitHub.           |
| 0x80002701 | Expect ts equal                                              | JOIN condition validation failed                             | Preserve logs and report the issue on GitHub.           |
| 0x80002702 | Cross join not supported                                     | `CROSS JOIN` is not supported                                | Check and correct the SQL statement.                    |

## function

| Error Code | Error Description                          | Possible Error Scenarios or Causes                                                                                                                                                       | Suggested Actions                                                   |
| ---------- | ------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| 0x80002800 | Function internal error                    | An error caused by invalid function parameters. For example, the APERCENTILE function only accepts "default" or "t-digest" as the third parameter. If other inputs are used, this error occurs. Similarly, the TO_ISO8601 function might fail if the timezone string is invalid. | Adjust function input based on the specific error message.           |
| 0x80002801 | Invalid function parameter number          | The number of input parameters is incorrect. For example, the function requires `n` parameters, but the user provides a different number, like COUNT(col1, col2).                          | Provide the correct number of parameters.                           |
| 0x80002802 | Invalid function parameter type            | The function received a parameter of the wrong type. For example, the SUM function expects a numerical value, but a string like SUM("abc") was provided.                                  | Provide the correct parameter types.                                |
| 0x80002803 | Invalid function parameter value           | The parameter value is out of range. For example, the SAMPLE function only accepts a range of [1, 1000] for its second parameter.                                                         | Adjust the parameter values to be within the valid range.            |
| 0x80002804 | Not a built-in function                    | The function is not recognized as a built-in function. This might happen if the internal function hash table initialization fails or is corrupted.                                         | This is likely a bug. Contact the development team.                 |
| 0x80002805 | Duplicate timestamps not allowed in function | The function received duplicate timestamps in the primary key column. In certain time-dependent functions like CSUM, DERIVATIVE, or MAVG, merged data from multiple sub-tables with identical timestamps may lead to meaningless calculations and errors. | Ensure that no duplicate timestamps exist in sub-table data.         |

## udf

| Error Code | Error Description               | Possible Error Scenarios or Causes                                                       | Suggested Actions                                  |
| ---------- | -------------------------------- | ---------------------------------------------------------------------------------------- | ------------------------------------------------- |
| 0x80002901 | udf is stopping                  | A udf call was received while a dnode is exiting.                                         | Stop executing the udf query.                      |
| 0x80002902 | udf pipe read error              | An error occurred while taosd was reading the udfd pipe.                                  | This may indicate a crash in C udf or udfd.        |
| 0x80002903 | udf pipe connect error           | An error occurred while establishing a pipe connection between taosd and udfd.            | Restart taosd if the corresponding udfd is not running. |
| 0x80002904 | udf pipe not exist               | A connection issue occurred between the creation, call, or teardown stages of the udf.    | Investigate crashes in C udf or udfd.              |
| 0x80002905 | udf load failure                 | An error occurred while loading a udf in udfd.                                            | Check if the udf exists in mnode and review the logs. |
| 0x80002906 | udf invalid function input       | The input to the udf was invalid.                                                         | Ensure the input data types match the udf requirements. |
| 0x80002907 | udf invalid bufsize              | The intermediate result in a udf aggregation function exceeded the buffer size specified during creation. | Increase the buffer size or reduce intermediate result size. |
| 0x80002908 | udf invalid output type          | The output type of the udf does not match the type specified during creation.              | Modify the udf or ensure matching output types.    |
| 0x80002909 | udf program language not supported | The programming language used for the udf is not supported.                                | Use a supported language, such as C or Python.     |
| 0x8000290A | udf function execution failure   | The udf function execution failed, for example, by returning an incorrect number of rows.  | Review the error logs for more details.            |

## sml

| Error Code | Error Description               | Possible Error Scenarios or Causes                                | Suggested Actions                                               |
| ---------- | ------------------------------- | ----------------------------------------------------------------- | --------------------------------------------------------------- |
| 0x80003000 | Invalid line protocol type      | The protocol provided to the schemaless interface is invalid.      | Check if the protocol matches one of the TSDB_SML_PROTOCOL_TYPE values in taos.h. |
| 0x80003001 | Invalid timestamp precision type | The timestamp precision provided to the schemaless interface is invalid. | Ensure the timestamp precision matches one of the TSDB_SML_TIMESTAMP_TYPE values in taos.h. |
| 0x80003002 | Invalid data format             | The data format provided to the schemaless interface is invalid.   | Check the client error logs for details.                        |
| 0x80003004 | Not the same type as before     | The column types in multiple rows are inconsistent.               | Ensure the column types are consistent across all rows.          |
| 0x80003005 | Internal error                  | An internal logic error occurred within the schemaless interface. | Review the client error logs for more details.                   |

## sma

| Error Code | Error Description                | Possible Error Scenarios or Causes                                  | Suggested Actions                      |
| ---------- | -------------------------------- | ------------------------------------------------------------------- | -------------------------------------- |
| 0x80003100 | Tsma init failed                 | TSMA environment initialization failed.                             | Check the error logs and contact the development team. |
| 0x80003101 | Tsma already exists              | TSMA was created multiple times.                                    | Avoid creating the same TSMA more than once. |
| 0x80003102 | Invalid tsma environment         | The TSMA runtime environment is abnormal.                           | Check the error logs and contact the development team. |
| 0x80003103 | Invalid tsma state               | The vgroup of the stream computation result does not match the TSMA index vgroup. | Check the error logs and contact the development team. |
| 0x80003104 | Invalid tsma pointer             | A null pointer was encountered when processing stream computation results. | Check the error logs and contact the development team. |
| 0x80003105 | Invalid tsma parameters          | The number of results from the stream computation is zero.          | Check the error logs and contact the development team. |
| 0x80003150 | Invalid rsma environment         | The Rsma execution environment is abnormal.                         | Check the error logs and contact the development team. |
| 0x80003151 | Invalid rsma state               | The Rsma execution state is abnormal.                               | Check the error logs and contact the development team. |
| 0x80003152 | Rsma qtaskinfo creation error    | An error occurred while creating the stream computation environment. | Check the error logs and contact the development team. |
| 0x80003153 | Rsma invalid schema              | Metadata information was incorrect during startup recovery.         | Check the error logs and contact the development team. |
| 0x80003154 | Rsma stream state open failed    | Failed to open the stream operator state storage.                   | Check the error logs and contact the development team. |
| 0x80003155 | Rsma stream state commit failed  | Failed to commit the stream operator state storage.                 | Check the error logs and contact the development team. |
| 0x80003156 | Rsma fs ref error                | File reference count error for the operator.                        | Check the error logs and contact the development team. |
| 0x80003157 | Rsma fs sync error               | Failed to synchronize the operator files.                           | Check the error logs and contact the development team. |
| 0x80003158 | Rsma fs update error             | Failed to update the operator files.                                | Check the error logs and contact the development team. |

## index

| Error Code | Error Description              | Possible Error Scenarios or Causes                                   | Suggested Actions                      |
| ---------- | ------------------------------ | -------------------------------------------------------------------- | -------------------------------------- |
| 0x80003200 | Index rebuilding in progress   | 1. Write speed is too fast, causing the merge thread to fall behind. 2. The index file is corrupted and is being rebuilt. | Check the error logs and contact the development team. |
| 0x80003201 | Index file corrupted           | The index file is damaged.                                           | Check the error logs and contact the development team. |

## tmq

| Error Code | Error Description               | Possible Error Scenarios or Causes                                    | Suggested Actions                     |
| ---------- | ------------------------------- | --------------------------------------------------------------------- | ------------------------------------- |
| 0x80004000 | Invalid message                 | The subscribed data is invalid, which usually should not occur.        | Review the client error logs for more details. |
| 0x80004001 | Consumer mismatch               | The subscribed vnode does not match the reassigned vnode, often occurring when a new consumer joins the same consumer group. | This is an internal error not exposed to the user. |
| 0x80004002 | Consumer closed                 | The consumer no longer exists.                                        | Check if the consumer was closed intentionally. |
| 0x80004100 | Stream task not exist           | The stream computation task does not exist.                           | Review the server error logs for more details. |
