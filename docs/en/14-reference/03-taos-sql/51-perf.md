---
title: Performance Data
slug: /tdengine-reference/sql-manual/performance-data
---

Starting from TDengine 3.0, a built-in database `performance_schema` is provided, which stores performance-related statistical data. This section details the tables and table structures within it.

## PERF_APP

Provides information about the applications (clients) accessing the cluster. You can also use SHOW APPS to query this information.

| #   |   **Column Name**   | **Data Type** | **Description**                        |
| --- | :-----------------: | ------------ | -------------------------------------- |
| 1   |    app_id    | UBIGINT      | Client ID                       |
| 2   |      ip      | BINARY(16)   | Client address                      |
| 3   |     pid      | INT          | Client process number                   |
| 4   |     name     | BINARY(24)   | Client name                      |
| 5   |  start_time  | TIMESTAMP    | Client start time                  |
| 6   |  insert_req  | UBIGINT      | Number of insert requests                 |
| 7   |  insert_row  | UBIGINT      | Number of rows inserted                 |
| 8   | insert_time  | UBIGINT      | Processing time for insert requests, in microseconds |
| 9   | insert_bytes | UBIGINT      | Byte size of insert request messages           |
| 10  | fetch_bytes  | UBIGINT      | Byte size of query results                  |
| 11  |  query_time  | UBIGINT      | Query request processing time                |
| 12  |  slow_query  | UBIGINT      | Number of slow queries (processing time >= 3 seconds)  |
| 13  |  total_req   | UBIGINT      | Total number of requests                        |
| 14  | current_req  | UBIGINT      | Number of requests currently being processed          |
| 15  | last_access  | TIMESTAMP    | Last update time                    |

## PERF_CONNECTIONS

Information related to the database connections. You can also use SHOW CONNECTIONS to query this information.

| #   |  **Column Name**   | **Data Type** | **Description**                                           |
| --- | :---------------: | ------------ | -------------------------------------------------------- |
| 1   |   conn_id   | INT          | Connection ID                                            |
| 2   |    user     | BINARY(24)   | Username                                             |
| 3   |     app     | BINARY(24)   | Client name                                         |
| 4   |     pid     | UINT         | Process number of the client initiating this connection on its server or host |
| 5   |  end_point  | BINARY(128)  | Client address                                         |
| 6   | login_time  | TIMESTAMP    | Login time                                           |
| 7   | last_access | TIMESTAMP    | Last update time                                       |

## PERF_QUERIES

Provides information about the SQL statements that are currently being executed. You can also use SHOW QUERIES to query this information.

| #   |   **Column Name**   | **Data Type** | **Description**                     |
| --- | :-----------------: | ------------ | ----------------------------------- |
| 1   |   kill_id    | UBIGINT      | ID used to stop the query            |
| 2   |   query_id   | INT          | Query ID                      |
| 3   |   conn_id    | UINT         | Connection ID                      |
| 4   |     app      | BINARY(24)   | App name                     |
| 5   |     pid      | INT          | Process number of the app on its host |
| 6   |     user     | BINARY(24)   | Username                       |
| 7   |  end_point   | BINARY(16)   | Client address                   |
| 8   | create_time  | TIMESTAMP    | Creation time                     |
| 9   |  exec_usec   | BIGINT       | Execution time                   |
| 10  | stable_query | BOOL         | Whether it is a supertable query             |
| 11  |   sub_num    | INT          | Number of subqueries                   |
| 12  |  sub_status  | BINARY(1000) | Subquery status                   |
| 13  |     sql      | BINARY(1024) | SQL statement                     |

## PERF_CONSUMERS

| #   |    **Column Name**    | **Data Type** | **Description**                                                    |
| --- | :-------------------: | ------------ | --------------------------------------------------------------- |
| 1   |  consumer_id   | BIGINT       | Unique ID of the consumer                                             |
| 2   | consumer_group | BINARY(192)  | Consumer group                                                    |
| 3   |   client_id    | BINARY(192)  | User-defined string, displayed by specifying client_id when creating a consumer |
| 4   |     status     | BINARY(20)   | Current status of the consumer. Consumer statuses include: ready (normally available), lost (connection lost), rebalancing (consumer's vgroup is being allocated), unknown (unknown status)|
| 5   |     topics     | BINARY(204)  | Subscribed topics. If multiple topics are subscribed, they are displayed as multiple lines              |
| 6   |    up_time     | TIMESTAMP    | Time when first connected to taosd                                     |
| 7   | subscribe_time | TIMESTAMP    | Time when the last subscription was initiated                                        |
| 8   | rebalance_time | TIMESTAMP    | Time when the last rebalance was triggered                                 |

## PERF_TRANS

| #   |     **Column Name** | **Data Type** | **Description**                                                       |
| --- | :-----------------: | ------------ | -------------------------------------------------------------------- |
| 1   |        id           | INT          | Identifier of the ongoing transaction                                |
| 2   |   create_time       | TIMESTAMP    | Creation time of the transaction                                     |
| 3   |      stage          | BINARY(12)   | Current stage of the transaction, usually one of redoAction, undoAction, commit |
| 4   |       db1           | BINARY(64)   | Name of the first database that has a conflict with this transaction |
| 5   |       db2           | BINARY(64)   | Name of the second database that has a conflict with this transaction|
| 6   |   failed_times      | INT          | Total number of times the transaction has failed                     |
| 7   |  last_exec_time     | TIMESTAMP    | Last execution time of the transaction                               |
| 8   | last_action_info    | BINARY(511)  | Detailed information about the last failed execution of the transaction |

## PERF_SMAS

| #   |  **Column Name** | **Data Type** | **Description**                                  |
| --- | :--------------: | ------------ | ----------------------------------------------- |
| 1   |  sma_name        | BINARY(192)  | Name of the time-range-wise sma (precomputed time dimension) |
| 2   | create_time      | TIMESTAMP    | Creation time of the sma                        |
| 3   | stable_name      | BINARY(192)  | Name of the supertable to which the sma belongs |
| 4   |  vgroup_id       | INT          | Name of the vgroup dedicated to the sma          |
