---
sidebar_label: Performance Data
title: Performance Data
description: PERFORMANCE_SCHEMA performance-related statistical views
---

Starting from `v3.0.0.0`, TDengine provides the built-in database `PERFORMANCE_SCHEMA` for performance-related statistics. The sections below describe each table and its columns (aligned with the current system-table definitions).

Richer metadata and status information is also available via `INFORMATION_SCHEMA` (see [Metadata](./01-meta.md)) and the corresponding [SHOW Commands](./03-show.md).

## PERF_APPS

Provides write/query statistics, slow-query counts, and last-access time for applications (clients) connected to the cluster. You can also query with [`SHOW APPS`](./03-show.md#show-apps).

| #   | **Column** | **Data Type** | **Description** |
| --- | -------------- | ----------- | --- |
| 1   | `app_id`       | UBIGINT     | Client ID |
| 2   | `ip`           | VARCHAR(16) | Client address |
| 3   | `pid`          | INT         | Client process ID |
| 4   | `name`         | VARCHAR(24) | Client name |
| 5   | `start_time`   | TIMESTAMP   | Client start time |
| 6   | `insert_req`   | UBIGINT     | Number of `INSERT` requests |
| 7   | `insert_row`   | UBIGINT     | Number of rows inserted |
| 8   | `insert_time`  | UBIGINT     | `INSERT` request processing time, in microseconds |
| 9   | `insert_bytes` | UBIGINT     | `INSERT` request message size in bytes |
| 10  | `fetch_bytes`  | UBIGINT     | Query result size in bytes |
| 11  | `query_time`   | UBIGINT     | Query request processing time |
| 12  | `slow_query`   | UBIGINT     | Number of slow queries (processing time ≥ 3 seconds) |
| 13  | `total_req`    | UBIGINT     | Total number of requests |
| 14  | `current_req`  | UBIGINT     | Number of requests currently being processed |
| 15  | `last_access`  | TIMESTAMP   | Last update time |

## PERF_CONNECTIONS

Provides user, client, login time, connection type, and token information for current database connections. You can also query with [`SHOW CONNECTIONS`](./03-show.md#show-connections).

| #   | **Column** | **Data Type** | **Description** |
| --- | ---------------- | ----------- | --- |
| 1   | `conn_id`        | UINT        | Connection ID |
| 2   | `user`           | BINARY(24)  | Username. Keyword column; escape with backticks when querying (e.g. `` `user` ``) |
| 3   | `app`            | BINARY(24)  | Client name |
| 4   | `pid`            | UINT        | Client process ID that initiated the connection |
| 5   | `end_point`      | BINARY(134) | Client address |
| 6   | `login_time`     | TIMESTAMP   | Login time |
| 7   | `last_access`    | TIMESTAMP   | Last update time |
| 8   | `user_app`       | BINARY(24)  | User-side application name |
| 9   | `user_ip`        | VARCHAR(22) | User-side IP |
| 10  | `native_version` | BINARY(32)  | Native client version |
| 11  | `connector_info` | BINARY(256) | Connector information |
| 12  | `type`           | BINARY(16)  | Connection type |
| 13  | `token`          | BINARY(32)  | Token name (if logged in with a token) |

## PERF_CONSUMERS

Provides consumer group, status, subscribed topics, parameters, and last poll time for data-subscription consumers. You can also query with [`SHOW CONSUMERS`](./03-show.md#show-consumers).

| #   | **Column** | **Data Type** | **Description** |
| --- | ---------------- | ----------- | --- |
| 1   | `consumer_id`    | BINARY(32)  | Unique consumer ID |
| 2   | `consumer_group` | BINARY(193) | Consumer group |
| 3   | `client_id`      | BINARY(256) | Client identifier specified when creating the consumer |
| 4   | `user`           | BINARY(24)  | Username |
| 5   | `fqdn`           | BINARY(128) | FQDN of the machine running the consumer |
| 6   | `status`         | BINARY(20)  | Current status: `ready` (available), `lost` (connection lost), `rebalancing` (vgroup assignment in progress), `unknown` |
| 7   | `topics`         | BINARY(205) | Subscribed topics; multiple topics are shown as multiple rows |
| 8   | `end_point`      | VARCHAR(22) | Endpoint |
| 9   | `up_time`        | TIMESTAMP   | Time of first connection to `taosd` |
| 10  | `subscribe_time` | TIMESTAMP   | Time of the most recent subscribe request |
| 11  | `rebalance_time` | TIMESTAMP   | Time of the most recent rebalance |
| 12  | `parameters`     | BINARY(192) | Subscription parameters |
| 13  | `poll_time`      | TIMESTAMP   | Time of the most recent poll |

## PERF_INSTANCES

Provides registration information for instances connected to the cluster, including type, description, and registration/expiration times. You can also query with [`SHOW INSTANCES`](./03-show.md#show-instances).

| #   | **Column** | **Data Type** | **Description** |
| --- | ---------------- | ------------ | --- |
| 1   | `id`             | VARCHAR(257) | ID |
| 2   | `type`           | VARCHAR(66)  | Type |
| 3   | `desc`           | VARCHAR(514) | Instance description |
| 4   | `first_reg_time` | TIMESTAMP    | First registration time |
| 5   | `last_reg_time`  | TIMESTAMP    | Most recent registration time |
| 6   | `expire`         | INT          | Expiration time (seconds) |

## PERF_QUERIES

Provides identifiers, elapsed time, phase status, and SQL text for queries currently running. You can also query with [`SHOW QUERIES`](./03-show.md#show-queries).

| #   | **Column** | **Data Type** | **Description** |
| --- | ------------------ | ------------- | --- |
| 1   | `kill_id`          | VARCHAR(26)   | ID used with `KILL QUERY` |
| 2   | `query_id`         | UBIGINT       | Query ID |
| 3   | `conn_id`          | UINT          | Connection ID |
| 4   | `app`              | VARCHAR(24)   | Application name |
| 5   | `pid`              | INT           | Process ID of the application on its host |
| 6   | `user`             | VARCHAR(24)   | Username |
| 7   | `end_point`        | VARCHAR(22)   | Client address |
| 8   | `create_time`      | TIMESTAMP     | Creation time |
| 9   | `exec_usec`        | BIGINT        | Elapsed execution time (microseconds) |
| 10  | `stable_query`     | BOOL          | Whether this is a supertable query |
| 11  | `sub_query`        | BOOL          | Whether this is a subquery |
| 12  | `sub_num`          | INT           | Number of subqueries |
| 13  | `sub_status`       | VARCHAR(1000) | Subquery status (including subquery ID, status, and status start time) |
| 14  | `sql`              | VARCHAR(2048) | SQL statement. Keyword column; escape with backticks when querying |
| 15  | `user_app`         | VARCHAR(24)   | User-side application name |
| 16  | `user_ip`          | VARCHAR(22)   | User-side IP |
| 17  | `phase_state`      | VARCHAR(64)   | Current query phase / status |
| 18  | `phase_start_time` | TIMESTAMP     | Start time of the current phase |

## PERF_TRANS

Provides stage, operation target, failure count, and recent execution details for metadata transactions currently running. You can also query with [`SHOW TRANSACTIONS`](./03-show.md#show-transactions).

| #   | **Column** | **Data Type** | **Description** |
| --- | ------------------ | ------------ | --- |
| 1   | `id`               | BIGINT       | Transaction ID |
| 2   | `create_time`      | TIMESTAMP    | Creation time |
| 3   | `stage`            | VARCHAR(12)  | Current stage (e.g. `redoAction`, `undoAction`, `commit`) |
| 4   | `oper`             | VARCHAR(22)  | Operator |
| 5   | `db`               | VARCHAR(64)  | Related database |
| 6   | `stable`           | VARCHAR(192) | Related supertable |
| 7   | `killable`         | VARCHAR(10)  | Whether the transaction can be killed |
| 8   | `failed_times`     | INT          | Total number of execution failures |
| 9   | `last_exec_time`   | TIMESTAMP    | Last execution time |
| 10  | `last_action_info` | VARCHAR(511) | Details of the last failed execution |
| 11  | `type`             | VARCHAR(10)  | Transaction type |
