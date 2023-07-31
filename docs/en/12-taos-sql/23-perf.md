---
title: Performance_Schema Database
sidebar_label: Statistics
description: This document describes how to use the PERFORMANCE_SCHEMA database in TDengine.
---

TDengine includes a built-in database named `PERFORMANCE_SCHEMA` to provide access to database performance statistics. This document introduces the tables of PERFORMANCE_SCHEMA and their structure.

## PERF_APP

Provides information about clients (such as applications) that connect to the cluster. Similar to SHOW APPS.

| #   |    **Column**    | **Data Type** | **Description**                  |
| --- | :----------: | ------------ | ------------------------------- |
| 1   |    app_id    | UBIGINT      | Client ID                       |
| 2   |      ip      | BINARY(16)   | Client IP address                      |
| 3   |     pid      | INT          | Client process                   |
| 4   |     name     | BINARY(24)   | Client name                      |
| 5   |  start_time  | TIMESTAMP    | Time when client was started                  |
| 6   |  insert_req  | UBIGINT      | Insert requests                |
| 7   |  insert_row  | UBIGINT      | Rows inserted                 |
| 8   | insert_time  | UBIGINT      | Time spent processing insert requests in microseconds |
| 9   | insert_bytes | UBIGINT      | Size of data inserted in byted           |
| 10  | fetch_bytes  | UBIGINT      | Size of query results in bytes                  |
| 11  |  query_time  | UBIGINT      | Time spend processing query requests                |
| 12  |  slow_query  | UBIGINT      | Number of slow queries (greater than or equal to 3 seconds)  |
| 13  |  total_req   | UBIGINT      | Total requests                        |
| 14  | current_req  | UBIGINT      | Requests currently being processed          |
| 15  | last_access  | TIMESTAMP    | Last update time                    |

## PERF_CONNECTIONS

Provides information about connections to the database. Similar to SHOW CONNECTIONS.

| #   |    **Column**    | **Data Type** | **Description**                  |
| --- | :---------: | ------------ | -------------------------------------------------- |
| 1   |   conn_id   | INT          | Connection ID                                            |
| 2   |    user     | BINARY(24)   | User name                                             |
| 3   |     app     | BINARY(24)   | Client name                                         |
| 4   |     pid     | UINT         | Client process ID on client device that initiated the connection |
| 5   |  end_point  | BINARY(128)  | Client endpoint                                         |
| 6   | login_time  | TIMESTAMP    | Login time                                           |
| 7   | last_access | TIMESTAMP    | Last update time                                       |

## PERF_QUERIES

Provides information about SQL queries currently running. Similar to SHOW QUERIES.

| #   |    **Column**    | **Data Type** | **Description**                  |
| --- | :----------: | ------------ | ---------------------------- |
| 1   |   kill_id    | UBIGINT      | ID used to stop the query            |
| 2   |   query_id   | INT          | Query ID                      |
| 3   |   conn_id    | UINT         | Connection ID                      |
| 4   |     app      | BINARY(24)   | Client name                     |
| 5   |     pid      | INT          | Client process ID on client device |
| 6   |     user     | BINARY(24)   | User name                       |
| 7   |  end_point   | BINARY(16)   | Client endpoint                   |
| 8   | create_time  | TIMESTAMP    | Creation time                     |
| 9   |  exec_usec   | BIGINT       | Elapsed time                   |
| 10  | stable_query | BOOL         | Whether the query is on a supertable             |
| 11  |   sub_num    | INT          | Number of subqueries                   |
| 12  |  sub_status  | BINARY(1000) | Subquery status                   |
| 13  |     sql      | BINARY(1024) | SQL statement                     |

## PERF_CONSUMERS

| #   |    **Column**    | **Data Type** | **Description**                  |
| --- | :------------: | ------------ | ----------------------------------------------------------- |
| 1   |  consumer_id   | BIGINT       | Consumer ID                                             |
| 2   | consumer_group | BINARY(192)  | Consumer group                                                    |
| 3   |   client_id    | BINARY(192)  | Client ID (user-defined) |
| 4   |     status     | BINARY(20)   | Consumer status. All possible status include: ready(consumer is in normal state), lost(the connection between consumer and mnode is broken), rebalance(the redistribution of vgroups that belongs to current consumer is now in progress), unknown(consumer is in invalid state)
| 5   |     topics     | BINARY(204)  | Subscribed topic. Returns one row for each topic.              |
| 6   |    up_time     | TIMESTAMP    | Time of first connection to TDengine Server                                     |
| 7   | subscribe_time | TIMESTAMP    | Time of first subscription                                        |
| 8   | rebalance_time | TIMESTAMP    | Time of first rebalance triggering                                 |

## PERF_TRANS

| #   |    **Column**    | **Data Type** | **Description**                  |
| --- | :--------------: | ------------ | -------------------------------------------------------------- |
| 1   |        id        | INT          | ID of the transaction currently running                                           |
| 2   |   create_time    | TIMESTAMP    | Creation time                                                 |
| 3   |      stage       | BINARY(12)   | Transaction stage (redoAction, undoAction, or commit) |
| 4   |       db1        | BINARY(64)   | First database having a conflict with the transaction                               |
| 5   |       db2        | BINARY(64)   | Second database having a conflict with the transaction                               |
| 6   |   failed_times   | INT          | Times the transaction has failed                                           |
| 7   |  last_exec_time  | TIMESTAMP    | Previous time the transaction was run                                             |
| 8   | last_action_info | BINARY(511)  | Reason for failure on previous run                                     |

## PERF_SMAS

| #   |    **Column**    | **Data Type** | **Description**                  |
| --- | :---------: | ------------ | ------------------------------------------- |
| 1   |  sma_name   | BINARY(192)  | Time-range-wise SMA name |
| 2   | create_time | TIMESTAMP    | Creation time                                |
| 3   | stable_name | BINARY(192)  | Supertable name                        |
| 4   |  vgroup_id  | INT          | Dedicated vgroup name                      |
