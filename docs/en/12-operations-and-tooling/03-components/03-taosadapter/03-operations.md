---
title: Operations and Monitoring
sidebar_label: Operations and Monitoring
toc_max_heading_level: 4
---

## Recording SQL to CSV Files

taosAdapter supports recording SQL requests to CSV files. Users can enable this feature through the configuration parameter `log.enableSqlToCsvLogging` or dynamically enable/disable it via HTTP requests.

### Configuration Parameters

1. New configuration item `log.enableSqlToCsvLogging` (boolean, default: false) determines whether SQL logging is enabled.
When set to true, SQL records will be saved to CSV files.
The recording start time is the service startup time, and the end time is `2300-01-01 00:00:00`.

2. File naming follows the same rules as logs: `taosadapterSql_{instanceId}_{yyyyMMdd}.csv[.index]`
   - `instanceId`: taosAdapter instance ID, configurable via the instanceId parameter.
   - `yyyyMMdd`: Date in year-month-day format.
   - `index`: If multiple files exist, a numeric suffix will be appended to the filename.

3. Existing log parameters are used for space retention, file splitting, and storage path:
   - `log.path`: Storage path
   - `log.keepDays`: Retention period in days
   - `log.rotationCount`: Maximum number of retained files
   - `log.rotationSize`: Maximum size per file
   - `log.compress`: Whether compression is enabled
   - `log.reservedDiskSize`: Reserved disk space size

### Dynamic Enablement

Send an HTTP POST request to the `/record_sql` endpoint to dynamically enable recording. Authentication is the same as for `/rest/sql`. Example:

```bash
curl --location --request POST 'http://127.0.0.1:6041/record_sql' \
-u root:taosdata \
--data '{"start_time":"2026-01-13 17:00:00","end_time":"2026-01-13 18:00:00","location":"Asia/Shanghai"}'
```

Supported parameters:

- `start_time`: [Optional] Start time for recording, formatted as `yyyy-MM-dd HH:mm:ss`. Defaults to the current time if not specified.
- `end_time`: [Optional] End time for recording, formatted as `yyyy-MM-dd HH:mm:ss`. Defaults to 2300-01-01 00:00:00 if not specified.
- `location`: [Optional] Timezone for parsing start and end times, using IANA format (e.g., `Asia/Shanghai`). Defaults to the server's timezone.

If all parameters use default values, the data field can be omitted. Example:

```bash
curl --location --request POST 'http://127.0.0.1:6041/record_sql' \
-u root:taosdata
```

Successful response: HTTP code 200 with the following structure:

```json
{"code":0,"desc":""}
```

Failed response: Non-200 HTTP code with the following JSON structure (non-zero code and error description in desc):

```json
{"code":65535,"desc":"unmarshal json error"}
```

### Dynamic Disablement

Send an HTTP DELETE request to the `/record_sql` endpoint to disable recording. Authentication is the same as for `/rest/sql`. Example:

```bash
curl --location --request DELETE 'http://127.0.0.1:6041/record_sql' \
-u root:taosdata
```

Successful response: HTTP code 200.

1. If a task exists, the response is:

   ```json
   {
           "code": 0,
           "message": "",
           "start_time": "2025-07-23 17:00:00",
           "end_time": "2025-07-23 18:00:00"
   }
   ```

   - `start_time`: Configured start time of the canceled task (timezone: server's timezone).
   - `end_time`: Configured end time of the canceled task (timezone: server's timezone).

1. If no task exists, the response is:

   ```json
   {
           "code": 0,
           "message": ""
   }
   ```

### Query Status

Send an HTTP GET request to the `/record_sql` endpoint to query the task status. Authentication is the same as for `/rest/sql`. Example:

```bash
curl --location 'http://127.0.0.1:6041/record_sql' \
-u root:taosdata
```

Successful response: HTTP code 200 with the following structure:

```json
{
        "code": 0,
        "desc": "",
        "exists": true,
        "running": true,
        "start_time": "2025-07-16 17:00:00",
        "end_time": "2025-07-16 18:00:00",
        "current_concurrent": 100
}
```

- `code`: Error code (0 for success).
- `desc`: Error message (empty string for success).
- `exists`: Whether the task exists.
- `running`: Whether the task is active.
- `start_time`: Start time (timezone: server's timezone).
- `end_time`: End time (timezone: server's timezone).
- `current_concurrent`: Current SQL recording concurrency.

### Recording Format

Records are written before `taos_free_result` is executed or when the task ends (reaching the end time or being manually stopped).
Records are stored in CSV format without headers. Each line includes the following fields:

1. `TS`: Log timestamp (format: yyyy-MM-dd HH:mm:ss.SSSSSS, timezone: server's timezone).
1. `SQL`: Executed SQL statement. Line breaks in SQL are preserved per CSV standards. Special characters (`\n`, `\r`, `"`) are wrapped in double quotes.
   SQL containing special characters cannot be directly copied for use. Example:

   Original SQL:

    ```sql
      select * from t1
      where c1 = "ab"
    ```

   CSV record:

   ```csv
   "select * from t1
   where c1 = ""ab"""
   ```

1. `IP`: Client IP.
1. `User`: Username executing the SQL.
1. `ConnType`: Connection type (HTTP, WS).
1. `QID`: Request ID (saved as hexadecimal).
1. `ReceiveTime`: Time when the SQL was received (format: `yyyy-MM-dd HH:mm:ss.SSSSSS`, timezone: server's timezone).
1. `FreeTime`: Time when the SQL was released (format: `yyyy-MM-dd HH:mm:ss.SSSSSS`, timezone: server's timezone).
1. `QueryDuration(us)`: Time consumed from taos_query_a to callback completion (microseconds).
1. `FetchDuration(us)`: Cumulative time consumed by multiple taos_fetch_raw_block_a executions until callback completion (microseconds).
1. `GetConnDuration(us)`: Time consumed to obtain a connection from the HTTP connection pool (microseconds).
1. `TotalDuration(us)`: Total SQL request completion time (microseconds). For completed SQL: FreeTime - ReceiveTime. For incomplete SQL when the task ends: CurrentTime - ReceiveTime.
1. `SourcePort`: Client port. (Added in version 3.3.6.26 and above / 3.3.8.0 and above).
1. `AppName`: Client application name. (Added in version 3.3.6.26 and above / 3.3.8.0 and above).

Example:

```csv
2025-07-23 17:10:08.724775,show databases,127.0.0.1,root,http,0x2000000000000008,2025-07-23 17:10:08.707741,2025-07-23 17:10:08.724775,14191,965,1706,17034,53600,jdbc_test_app
```

## Recording STMT to CSV Files

taosAdapter supports recording STMT requests to CSV files starting from version v3.4.0.1. Users can enable this feature through the configuration parameter `log.enableStmtToCsvLogging` or dynamically enable/disable it via HTTP requests.

:::warning
Enabling this feature leads to severe performance degradation.
:::

### Configuration Parameters

1. New configuration item `log.enableStmtToCsvLogging` (boolean, default: false) determines whether STMT logging is enabled.
   When set to true, STMT records will be saved to CSV files.
   The recording start time is the service startup time, and the end time is `2300-01-01 00:00:00`.

2. File naming follows the same rules as logs: `taosadapterStmt_{instanceId}_{yyyyMMdd}.csv[.index]`
   - `instanceId`: taosAdapter instance ID, configurable via the instanceId parameter.
   - `yyyyMMdd`: Date in year-month-day format.
   - `index`: If multiple files exist, a numeric suffix will be appended to the filename.

3. Existing log parameters are used for space retention, file splitting, and storage path:
   - `log.path`: Storage path
   - `log.keepDays`: Retention period in days
   - `log.rotationCount`: Maximum number of retained files
   - `log.rotationSize`: Maximum size per file
   - `log.compress`: Whether compression is enabled
   - `log.reservedDiskSize`: Reserved disk space size

### Dynamic Enablement

Send an HTTP POST request to the `/record_stmt` endpoint to dynamically enable recording. Authentication is the same as for `/rest/sql`. Example:

```bash
curl --location --request POST 'http://127.0.0.1:6041/record_stmt' \
-u root:taosdata \
--data '{"start_time":"2026-01-13 17:00:00","end_time":"2026-01-13 18:00:00","location":"Asia/Shanghai"}'
```

Supported parameters:

- `start_time`: [Optional] Start time for recording, formatted as `yyyy-MM-dd HH:mm:ss`. Defaults to the current time if not specified.
- `end_time`: [Optional] End time for recording, formatted as `yyyy-MM-dd HH:mm:ss`. Defaults to the current time plus one hour if not specified.
- `location`: [Optional] Timezone for parsing start and end times, using IANA format (e.g., `Asia/Shanghai`). Defaults to the server's timezone.

If all parameters use default values, the data field can be omitted. Example:

```bash
curl --location --request POST 'http://127.0.0.1:6041/record_stmt' \
-u root:taosdata
```

Successful response: HTTP code 200 with the following structure:

```json
{"code":0,"desc":""}
```

Failed response: Non-200 HTTP code with the following JSON structure (non-zero code and error description in desc):

```json
{"code":65535,"desc":"unmarshal json error"}
```

### Dynamic Disablement

Send an HTTP DELETE request to the `/record_stmt` endpoint to disable recording. Authentication is the same as for `/rest/sql`. Example:

```bash
curl --location --request DELETE 'http://127.0.0.1:6041/record_stmt' \
-u root:taosdata
```

Successful response: HTTP code 200.

1. If a task exists, the response is:

   ```json
   {
           "code": 0,
           "message": "",
           "start_time": "2026-01-13 17:00:00",
           "end_time": "2026-01-13 18:00:00"
   }
   ```

- `start_time`: Configured start time of the canceled task (timezone: server's timezone).
- `end_time`: Configured end time of the canceled task (timezone: server's timezone).

1. If no task exists, the response is:

   ```json
   {
           "code": 0,
           "message": ""
   }
   ```

### Query Status

Send an HTTP GET request to the `/record_stmt` endpoint to query the task status. Authentication is the same as for `/rest/sql`. Example:

```bash
curl --location 'http://127.0.0.1:6041/record_stmt' \
-u root:taosdata
```

Successful response: HTTP code 200 with the following structure:

```json
{
        "code": 0,
        "desc": "",
        "exists": true,
        "running": true,
        "start_time": "2026-01-13 17:00:00",
        "end_time": "2026-01-13 18:00:00",
        "current_concurrent": 100
}
```

- `code`: Error code (0 for success).
- `desc`: Error message (empty string for success).
- `exists`: Whether the task exists.
- `running`: Whether the task is active.
- `start_time`: Start time (timezone: server's timezone).
- `end_time`: End time (timezone: server's timezone).
- `current_concurrent`: Current STMT recording concurrency.

### Recording Format

Records are written after `prepare`, `bind`, and `exec` operations, or when the task ends (reaching the end time or being manually stopped).
Records are stored in CSV format without headers. Each line includes the following fields:

1. `TS`: Log timestamp (format: yyyy-MM-dd HH:mm:ss.SSSSSS, timezone: server's timezone).
2. `IP`: Client IP.
3. `SourcePort`: Client port.
4. `AppName`: Client application name.
5. `User`: Username of the current connection.
6. `ConnType`: Connection type (ws).
7. `QID`: Request ID, saved as hexadecimal.
8. `StartTime`: Start processing time, formatted as yyyy-MM-dd HH:mm:ss.SSSSSS, timezone is the server timezone where taosAdapter is located.
9. `STMT2`: stmt2 memory address, saved as hexadecimal.
10. `Action`: Operation (prepare, bind, exec).
11. `Code`: Operation result, 0 represents success, other values represent error codes.
12. `Duration(us)`: Execution time in microseconds, -1 if not completed.
13. Data: Request or result data
    1. prepare: Prepared SQL statement.
    2. exec: Affected row count.
    3. bind: Bound data, parsed successfully as JSON data, parsing failure will be raw binary data.

#### Bind Data JSON Format

The Data field content for a bind operation is in JSON format and contains the following fields:

| Field Name  | Type                       | Description                                              |
|:------------|:---------------------------|:---------------------------------------------------------|
| count       | int                        | Number of tables with bound parameters                   |
| table_names | [count]string              | List of table names with bound parameters                |
| tags        | [count][tag_count]col_info | List of tag column information for bound parameters      |
| cols        | [count][col_count]col_info | List of ordinary column information for bound parameters |

Where the col_info structure contains the following fields:

| Field Name | Type  | Description                                                  |
|:-----------|:------|:-------------------------------------------------------------|
| type       | int   | Column type                                                  |
| data       | []any | One-dimensional array, each element represents a row of data |

The parsing rules for each type of data in the data field are as follows:

1. varbinary, blob, geometry are parsed as hexadecimal strings
2. binary, nchar, json, decimal are parsed as utf-8 strings
3. Numeric types are converted to numbers
4. Datetime types are converted to numbers
5. Bool types are converted to json bool (true/false)
6. null data is converted to json null (null)

Sample example:

```json
{
        "count": 1,
        "table_names": ["test1"],
        "tags": [
                [{
                        "type": 9,
                        "data": [1726803356466]
                }, {
                        "type": 1,
                        "data": [true]
                }, {
                        "type": 2,
                        "data": [1]
                }, {
                        "type": 3,
                        "data": [2]
                }, {
                        "type": 4,
                        "data": [3]
                }, {
                        "type": 5,
                        "data": [4]
                }, {
                        "type": 6,
                        "data": [5.5]
                }, {
                        "type": 7,
                        "data": [6.6]
                }, {
                        "type": 11,
                        "data": [7]
                }, {
                        "type": 12,
                        "data": [8]
                }, {
                        "type": 13,
                        "data": [9]
                }, {
                        "type": 14,
                        "data": [10]
                }, {
                        "type": 8,
                        "data": ["binary"]
                }, {
                        "type": 10,
                        "data": ["nchar"]
                }, {
                        "type": 20,
                        "data": ["010100000000000000000059400000000000005940"]
                }, {
                        "type": 16,
                        "data": ["76617262696e617279"]
                }, {
                        "type": 17,
                        "data": ["12345.6789"]
                }, {
                        "type": 21,
                        "data": ["98765.4321"]
                }, {
                        "type": 18,
                        "data": ["7468697320697320626c6f622064617461"]
                }]
        ],
        "cols": [
                [{
                        "type": 9,
                        "data": [1726803356466, 1726803357466, 1726803358466]
                }, {
                        "type": 1,
                        "data": [true, null, false]
                }, {
                        "type": 2,
                        "data": [11, null, 12]
                }, {
                        "type": 3,
                        "data": [11, null, 12]
                }, {
                        "type": 4,
                        "data": [11, null, 12]
                }, {
                        "type": 5,
                        "data": [11, null, 12]
                }, {
                        "type": 6,
                        "data": [11.2, null, 12.2]
                }, {
                        "type": 7,
                        "data": [11.2, null, 12.2]
                }, {
                        "type": 11,
                        "data": [11, null, 12]
                }, {
                        "type": 12,
                        "data": [11, null, 12]
                }, {
                        "type": 13,
                        "data": [11, null, 12]
                }, {
                        "type": 14,
                        "data": [11, null, 12]
                }, {
                        "type": 8,
                        "data": ["binary1", null, "binary2"]
                }, {
                        "type": 10,
                        "data": ["nchar1", null, "nchar2"]
                }, {
                        "type": 20,
                        "data": ["010100000000000000000059400000000000005940", null, "010100000000000000000059400000000000005940"]
                }, {
                        "type": 16,
                        "data": ["76617262696e61727931", null, "76617262696e61727932"]
                }, {
                        "type": 17,
                        "data": ["12345.6789", null, "22345.6789"]
                }, {
                        "type": 21,
                        "data": ["98765.4321", null, "88765.4321"]
                }, {
                        "type": 18,
                        "data": ["7468697320697320626c6f622064617461", null, "7468697320697320616e6f7468657220626c6f622064617461"]
                }]
        ]
}
```

## Monitoring Metrics

Currently, taosAdapter only collects monitoring indicators for RESTful/WebSocket related requests. There are no monitoring indicators for other interfaces.

taosAdapter reports monitoring indicators to taosKeeper, which will be written to the monitoring database by taosKeeper. The default is the `log` database, which can be modified in the taoskeeper configuration file. The following is a detailed introduction to these monitoring indicators.

The `adapter_requests` table records taosAdapter monitoring data:

<details>
<summary>Details</summary>

| field            | type             | is_tag | comment                                   |
|:-----------------|:-----------------|:-------|:------------------------------------------|
| ts               | TIMESTAMP        |        | data collection timestamp                 |
| total            | INT UNSIGNED     |        | total number of requests                  |
| query            | INT UNSIGNED     |        | number of query requests                  |
| write            | INT UNSIGNED     |        | number of write requests                  |
| other            | INT UNSIGNED     |        | number of other requests                  |
| in_process       | INT UNSIGNED     |        | number of requests in process             |
| success          | INT UNSIGNED     |        | number of successful requests             |
| fail             | INT UNSIGNED     |        | number of failed requests                 |
| query_success    | INT UNSIGNED     |        | number of successful query requests       |
| query_fail       | INT UNSIGNED     |        | number of failed query requests           |
| write_success    | INT UNSIGNED     |        | number of successful write requests       |
| write_fail       | INT UNSIGNED     |        | number of failed write requests           |
| other_success    | INT UNSIGNED     |        | number of successful other requests       |
| other_fail       | INT UNSIGNED     |        | number of failed other requests           |
| query_in_process | INT UNSIGNED     |        | number of query requests in process       |
| write_in_process | INT UNSIGNED     |        | number of write requests in process       |
| endpoint         | VARCHAR          | TAG    | request endpoint                          |
| req_type         | TINYINT UNSIGNED | TAG    | request type: 0 for REST, 1 for WebSocket |

</details>

The `adapter_status` table records the status data of taosAdapter:

<details>
<summary>Details</summary>

| field                     | type      | is\_tag | comment                                                                            |
|:--------------------------|:----------|:--------|:-----------------------------------------------------------------------------------|
| _ts                       | TIMESTAMP |         | data collection timestamp                                                          |
| go_heap_sys               | DOUBLE    |         | heap memory allocated by Go runtime (bytes)                                        |
| go_heap_inuse             | DOUBLE    |         | heap memory in use by Go runtime (bytes)                                           |
| go_stack_sys              | DOUBLE    |         | stack memory allocated by Go runtime (bytes)                                       |
| go_stack_inuse            | DOUBLE    |         | stack memory in use by Go runtime (bytes)                                          |
| rss                       | DOUBLE    |         | actual physical memory occupied by the process (bytes)                             |
| ws_query_conn             | DOUBLE    |         | current WebSocket connections for `/rest/ws` endpoint                              |
| ws_stmt_conn              | DOUBLE    |         | current WebSocket connections for `/rest/stmt` endpoint                            |
| ws_sml_conn               | DOUBLE    |         | current WebSocket connections for `/rest/schemaless` endpoint                      |
| ws_ws_conn                | DOUBLE    |         | current WebSocket connections for `/ws` endpoint                                   |
| ws_tmq_conn               | DOUBLE    |         | current WebSocket connections for `/rest/tmq` endpoint                             |
| async_c_limit             | DOUBLE    |         | total concurrency limit for the C asynchronous interface                           |
| async_c_inflight          | DOUBLE    |         | current concurrency for the C asynchronous interface                               |
| sync_c_limit              | DOUBLE    |         | total concurrency limit for the C synchronous interface                            |
| sync_c_inflight           | DOUBLE    |         | current concurrency for the C synchronous interface                                |
| ws_query_conn_inc         | DOUBLE    |         | New connections on `/rest/ws` interface (Available since v3.3.6.10)                |
| ws_query_conn_dec         | DOUBLE    |         | Closed connections on `/rest/ws` interface (Available since v3.3.6.10)             |
| ws_stmt_conn_inc          | DOUBLE    |         | New connections on `/rest/stmt` interface (Available since v3.3.6.10)              |
| ws_stmt_conn_dec          | DOUBLE    |         | Closed connections on `/rest/stmt` interface (Available since v3.3.6.10)           |
| ws_sml_conn_inc           | DOUBLE    |         | New connections on `/rest/schemaless` interface (Available since v3.3.6.10)        |
| ws_sml_conn_dec           | DOUBLE    |         | Closed connections on `/rest/schemaless` interface (Available since v3.3.6.10)     |
| ws_ws_conn_inc            | DOUBLE    |         | New connections on `/ws` interface (Available since v3.3.6.10)                     |
| ws_ws_conn_dec            | DOUBLE    |         | Closed connections on `/ws` interface (Available since v3.3.6.10)                  |
| ws_tmq_conn_inc           | DOUBLE    |         | New connections on `/rest/tmq` interface (Available since v3.3.6.10)               |
| ws_tmq_conn_dec           | DOUBLE    |         | Closed connections on `/rest/tmq` interface (Available since v3.3.6.10)            |
| ws_query_sql_result_count | DOUBLE    |         | Current SQL query results held by `/rest/ws` interface (Available since v3.3.6.10) |
| ws_stmt_stmt_count        | DOUBLE    |         | Current stmt objects held by `/rest/stmt` interface (Available since v3.3.6.10)    |
| ws_ws_sql_result_count    | DOUBLE    |         | Current SQL query results held by `/ws` interface (Available since v3.3.6.10)      |
| ws_ws_stmt_count          | DOUBLE    |         | Current stmt objects held by `/ws` interface (Available since v3.3.6.10)           |
| ws_ws_stmt2_count         | DOUBLE    |         | Current stmt2 objects held by `/ws` interface (Available since v3.3.6.10)          |
| cpu_percent               | DOUBLE    |         | CPU usage percentage of taosAdapter (Available since v3.3.6.24/v3.3.7.7)           |
| endpoint                  | NCHAR     | TAG     | request endpoint                                                                   |

</details>

The `adapter_conn_pool` table records the connection pool monitoring data of taosAdapter:

<details>
<summary>Details</summary>

| field            | type      | is\_tag | comment                                                     |
|:-----------------|:----------|:--------|:------------------------------------------------------------|
| _ts              | TIMESTAMP |         | data collection timestamp                                   |
| conn_pool_total  | DOUBLE    |         | maximum connection limit for the connection pool            |
| conn_pool_in_use | DOUBLE    |         | current number of connections in use in the connection pool |
| endpoint         | NCHAR     | TAG     | request endpoint                                            |
| user             | NCHAR     | TAG     | username to which the connection pool belongs               |

</details>

Starting from version **3.3.6.10**, the `adapter_c_interface` table has been added to record taosAdapter C interface call metrics:

<details>
<summary>Details</summary>

| field                                               | type      | is\_tag | comment                                                                      |
|:----------------------------------------------------|:----------|:--------|:-----------------------------------------------------------------------------|
| _ts                                                 | TIMESTAMP |         | Data collection timestamp                                                    |
| taos_connect_total                                  | DOUBLE    |         | Count of total connection attempts                                           |
| taos_connect_success                                | DOUBLE    |         | Count of successful connections                                              |
| taos_connect_fail                                   | DOUBLE    |         | Count of failed connections                                                  |
| taos_close_total                                    | DOUBLE    |         | Count of total close attempts                                                |
| taos_close_success                                  | DOUBLE    |         | Count of successful closes                                                   |
| taos_schemaless_insert_total                        | DOUBLE    |         | Count of schemaless insert operations                                        |
| taos_schemaless_insert_success                      | DOUBLE    |         | Count of successful schemaless inserts                                       |
| taos_schemaless_insert_fail                         | DOUBLE    |         | Count of failed schemaless inserts                                           |
| taos_schemaless_free_result_total                   | DOUBLE    |         | Count of schemaless result set releases                                      |
| taos_schemaless_free_result_success                 | DOUBLE    |         | Count of successful schemaless result set releases                           |
| taos_query_total                                    | DOUBLE    |         | Count of synchronous SQL executions                                          |
| taos_query_success                                  | DOUBLE    |         | Count of successful synchronous SQL executions                               |
| taos_query_fail                                     | DOUBLE    |         | Count of failed synchronous SQL executions                                   |
| taos_query_free_result_total                        | DOUBLE    |         | Count of synchronous SQL result set releases                                 |
| taos_query_free_result_success                      | DOUBLE    |         | Count of successful synchronous SQL result set releases                      |
| taos_query_a_with_reqid_total                       | DOUBLE    |         | Count of async SQL with request ID                                           |
| taos_query_a_with_reqid_success                     | DOUBLE    |         | Count of successful async SQL with request ID                                |
| taos_query_a_with_reqid_callback_total              | DOUBLE    |         | Count of async SQL callbacks with request ID                                 |
| taos_query_a_with_reqid_callback_success            | DOUBLE    |         | Count of successful async SQL callbacks with request ID                      |
| taos_query_a_with_reqid_callback_fail               | DOUBLE    |         | Count of failed async SQL callbacks with request ID                          |
| taos_query_a_free_result_total                      | DOUBLE    |         | Count of async SQL result set releases                                       |
| taos_query_a_free_result_success                    | DOUBLE    |         | Count of successful async SQL result set releases                            |
| tmq_consumer_poll_result_total                      | DOUBLE    |         | Count of consumer polls with data                                            |
| tmq_free_result_total                               | DOUBLE    |         | Count of TMQ data releases                                                   |
| tmq_free_result_success                             | DOUBLE    |         | Count of successful TMQ data releases                                        |
| taos_stmt2_init_total                               | DOUBLE    |         | Count of stmt2 initializations                                               |
| taos_stmt2_init_success                             | DOUBLE    |         | Count of successful stmt2 initializations                                    |
| taos_stmt2_init_fail                                | DOUBLE    |         | Count of failed stmt2 initializations                                        |
| taos_stmt2_close_total                              | DOUBLE    |         | Count of stmt2 closes                                                        |
| taos_stmt2_close_success                            | DOUBLE    |         | Count of successful stmt2 closes                                             |
| taos_stmt2_close_fail                               | DOUBLE    |         | Count of failed stmt2 closes                                                 |
| taos_stmt2_get_fields_total                         | DOUBLE    |         | Count of stmt2 field fetches                                                 |
| taos_stmt2_get_fields_success                       | DOUBLE    |         | Count of successful stmt2 field fetches                                      |
| taos_stmt2_get_fields_fail                          | DOUBLE    |         | Count of failed stmt2 field fetches                                          |
| taos_stmt2_free_fields_total                        | DOUBLE    |         | Count of stmt2 field releases                                                |
| taos_stmt2_free_fields_success                      | DOUBLE    |         | Count of successful stmt2 field releases                                     |
| taos_stmt_init_with_reqid_total                     | DOUBLE    |         | Count of stmt initializations with request ID                                |
| taos_stmt_init_with_reqid_success                   | DOUBLE    |         | Count of successful stmt initializations with request ID                     |
| taos_stmt_init_with_reqid_fail                      | DOUBLE    |         | Count of failed stmt initializations with request ID                         |
| taos_stmt_close_total                               | DOUBLE    |         | Count of stmt closes                                                         |
| taos_stmt_close_success                             | DOUBLE    |         | Count of successful stmt closes                                              |
| taos_stmt_close_fail                                | DOUBLE    |         | Count of failed stmt closes                                                  |
| taos_stmt_get_tag_fields_total                      | DOUBLE    |         | Count of stmt tag field fetches                                              |
| taos_stmt_get_tag_fields_success                    | DOUBLE    |         | Count of successful stmt tag field fetches                                   |
| taos_stmt_get_tag_fields_fail                       | DOUBLE    |         | Count of failed stmt tag field fetches                                       |
| taos_stmt_get_col_fields_total                      | DOUBLE    |         | Count of stmt column field fetches                                           |
| taos_stmt_get_col_fields_success                    | DOUBLE    |         | Count of successful stmt column field fetches                                |
| taos_stmt_get_col_fields_fail                       | DOUBLE    |         | Count of failed stmt column field fetches                                    |
| taos_stmt_reclaim_fields_total                      | DOUBLE    |         | Count of stmt field releases                                                 |
| taos_stmt_reclaim_fields_success                    | DOUBLE    |         | Count of successful stmt field releases                                      |
| tmq_get_json_meta_total                             | DOUBLE    |         | Count of TMQ JSON metadata fetches                                           |
| tmq_get_json_meta_success                           | DOUBLE    |         | Count of successful TMQ JSON metadata fetches                                |
| tmq_free_json_meta_total                            | DOUBLE    |         | Count of TMQ JSON metadata releases                                          |
| tmq_free_json_meta_success                          | DOUBLE    |         | Count of successful TMQ JSON metadata releases                               |
| taos_fetch_whitelist_a_total                        | DOUBLE    |         | Count of async whitelist fetches                                             |
| taos_fetch_whitelist_a_success                      | DOUBLE    |         | Count of successful async whitelist fetches                                  |
| taos_fetch_whitelist_a_callback_total               | DOUBLE    |         | Count of async whitelist callbacks                                           |
| taos_fetch_whitelist_a_callback_success             | DOUBLE    |         | Count of successful async whitelist callbacks                                |
| taos_fetch_whitelist_a_callback_fail                | DOUBLE    |         | Count of failed async whitelist callbacks                                    |
| taos_fetch_rows_a_total                             | DOUBLE    |         | Count of async row fetches                                                   |
| taos_fetch_rows_a_success                           | DOUBLE    |         | Count of successful async row fetches                                        |
| taos_fetch_rows_a_callback_total                    | DOUBLE    |         | Count of async row callbacks                                                 |
| taos_fetch_rows_a_callback_success                  | DOUBLE    |         | Count of successful async row callbacks                                      |
| taos_fetch_rows_a_callback_fail                     | DOUBLE    |         | Count of failed async row callbacks                                          |
| taos_fetch_raw_block_a_total                        | DOUBLE    |         | Count of async raw block fetches                                             |
| taos_fetch_raw_block_a_success                      | DOUBLE    |         | Count of successful async raw block fetches                                  |
| taos_fetch_raw_block_a_callback_total               | DOUBLE    |         | Count of async raw block callbacks                                           |
| taos_fetch_raw_block_a_callback_success             | DOUBLE    |         | Count of successful async raw block callbacks                                |
| taos_fetch_raw_block_a_callback_fail                | DOUBLE    |         | Count of failed async raw block callbacks                                    |
| tmq_get_raw_total                                   | DOUBLE    |         | Count of raw data fetches                                                    |
| tmq_get_raw_success                                 | DOUBLE    |         | Count of successful raw data fetches                                         |
| tmq_get_raw_fail                                    | DOUBLE    |         | Count of failed raw data fetches                                             |
| tmq_free_raw_total                                  | DOUBLE    |         | Count of raw data releases                                                   |
| tmq_free_raw_success                                | DOUBLE    |         | Count of successful raw data releases                                        |
| tmq_consumer_new_total                              | DOUBLE    |         | Count of new consumer creations                                              |
| tmq_consumer_new_success                            | DOUBLE    |         | Count of successful new consumer creations                                   |
| tmq_consumer_new_fail                               | DOUBLE    |         | Count of failed new consumer creations                                       |
| tmq_consumer_close_total                            | DOUBLE    |         | Count of consumer closes                                                     |
| tmq_consumer_close_success                          | DOUBLE    |         | Count of successful consumer closes                                          |
| tmq_consumer_close_fail                             | DOUBLE    |         | Count of failed consumer closes                                              |
| tmq_subscribe_total                                 | DOUBLE    |         | Count of topic subscriptions                                                 |
| tmq_subscribe_success                               | DOUBLE    |         | Count of successful topic subscriptions                                      |
| tmq_subscribe_fail                                  | DOUBLE    |         | Count of failed topic subscriptions                                          |
| tmq_unsubscribe_total                               | DOUBLE    |         | Count of unsubscriptions                                                     |
| tmq_unsubscribe_success                             | DOUBLE    |         | Count of successful unsubscriptions                                          |
| tmq_unsubscribe_fail                                | DOUBLE    |         | Count of failed unsubscriptions                                              |
| tmq_list_new_total                                  | DOUBLE    |         | Count of new topic list creations                                            |
| tmq_list_new_success                                | DOUBLE    |         | Count of successful new topic list creations                                 |
| tmq_list_new_fail                                   | DOUBLE    |         | Count of failed new topic list creations                                     |
| tmq_list_destroy_total                              | DOUBLE    |         | Count of topic list destructions                                             |
| tmq_list_destroy_success                            | DOUBLE    |         | Count of successful topic list destructions                                  |
| tmq_conf_new_total                                  | DOUBLE    |         | Count of TMQ new config creations                                            |
| tmq_conf_new_success                                | DOUBLE    |         | Count of successful TMQ new config creations                                 |
| tmq_conf_new_fail                                   | DOUBLE    |         | Count of failed TMQ new config creations                                     |
| tmq_conf_destroy_total                              | DOUBLE    |         | Count of TMQ config destructions                                             |
| tmq_conf_destroy_success                            | DOUBLE    |         | Count of successful TMQ config destructions                                  |
| taos_stmt2_prepare_total                            | DOUBLE    |         | Count of stmt2 prepares                                                      |
| taos_stmt2_prepare_success                          | DOUBLE    |         | Count of successful stmt2 prepares                                           |
| taos_stmt2_prepare_fail                             | DOUBLE    |         | Count of failed stmt2 prepares                                               |
| taos_stmt2_is_insert_total                          | DOUBLE    |         | Count of insert checks                                                       |
| taos_stmt2_is_insert_success                        | DOUBLE    |         | Count of successful insert checks                                            |
| taos_stmt2_is_insert_fail                           | DOUBLE    |         | Count of failed insert checks                                                |
| taos_stmt2_bind_param_total                         | DOUBLE    |         | Count of stmt2 parameter bindings                                            |
| taos_stmt2_bind_param_success                       | DOUBLE    |         | Count of successful stmt2 parameter bindings                                 |
| taos_stmt2_bind_param_fail                          | DOUBLE    |         | Count of failed stmt2 parameter bindings                                     |
| taos_stmt2_exec_total                               | DOUBLE    |         | Count of stmt2 executions                                                    |
| taos_stmt2_exec_success                             | DOUBLE    |         | Count of successful stmt2 executions                                         |
| taos_stmt2_exec_fail                                | DOUBLE    |         | Count of failed stmt2 executions                                             |
| taos_stmt2_error_total                              | DOUBLE    |         | Count of stmt2 error checks                                                  |
| taos_stmt2_error_success                            | DOUBLE    |         | Count of successful stmt2 error checks                                       |
| taos_fetch_row_total                                | DOUBLE    |         | Count of sync row fetches                                                    |
| taos_fetch_row_success                              | DOUBLE    |         | Count of successful sync row fetches                                         |
| taos_is_update_query_total                          | DOUBLE    |         | Count of update statement checks                                             |
| taos_is_update_query_success                        | DOUBLE    |         | Count of successful update statement checks                                  |
| taos_affected_rows_total                            | DOUBLE    |         | Count of SQL affected rows fetches                                           |
| taos_affected_rows_success                          | DOUBLE    |         | Count of successful SQL affected rows fetches                                |
| taos_num_fields_total                               | DOUBLE    |         | Count of field count fetches                                                 |
| taos_num_fields_success                             | DOUBLE    |         | Count of successful field count fetches                                      |
| taos_fetch_fields_e_total                           | DOUBLE    |         | Count of extended field info fetches                                         |
| taos_fetch_fields_e_success                         | DOUBLE    |         | Count of successful extended field info fetches                              |
| taos_fetch_fields_e_fail                            | DOUBLE    |         | Count of failed extended field info fetches                                  |
| taos_result_precision_total                         | DOUBLE    |         | Count of precision fetches                                                   |
| taos_result_precision_success                       | DOUBLE    |         | Count of successful precision fetches                                        |
| taos_get_raw_block_total                            | DOUBLE    |         | Count of raw block fetches                                                   |
| taos_get_raw_block_success                          | DOUBLE    |         | Count of successful raw block fetches                                        |
| taos_fetch_raw_block_total                          | DOUBLE    |         | Count of raw block pulls                                                     |
| taos_fetch_raw_block_success                        | DOUBLE    |         | Count of successful raw block pulls                                          |
| taos_fetch_raw_block_fail                           | DOUBLE    |         | Count of failed raw block pulls                                              |
| taos_fetch_lengths_total                            | DOUBLE    |         | Count of field length fetches                                                |
| taos_fetch_lengths_success                          | DOUBLE    |         | Count of successful field length fetches                                     |
| taos_write_raw_block_with_reqid_total               | DOUBLE    |         | Count of request ID raw block writes                                         |
| taos_write_raw_block_with_reqid_success             | DOUBLE    |         | Count of successful request ID raw block writes                              |
| taos_write_raw_block_with_reqid_fail                | DOUBLE    |         | Count of failed request ID raw block writes                                  |
| taos_write_raw_block_with_fields_with_reqid_total   | DOUBLE    |         | Count of request ID field raw block writes                                   |
| taos_write_raw_block_with_fields_with_reqid_success | DOUBLE    |         | Count of successful request ID field raw block writes                        |
| taos_write_raw_block_with_fields_with_reqid_fail    | DOUBLE    |         | Count of failed request ID field raw block writes                            |
| tmq_write_raw_total                                 | DOUBLE    |         | Count of TMQ raw data writes                                                 |
| tmq_write_raw_success                               | DOUBLE    |         | Count of successful TMQ raw data writes                                      |
| tmq_write_raw_fail                                  | DOUBLE    |         | Count of failed TMQ raw data writes                                          |
| taos_stmt_prepare_total                             | DOUBLE    |         | Count of stmt prepares                                                       |
| taos_stmt_prepare_success                           | DOUBLE    |         | Count of successful stmt prepares                                            |
| taos_stmt_prepare_fail                              | DOUBLE    |         | Count of failed stmt prepares                                                |
| taos_stmt_is_insert_total                           | DOUBLE    |         | Count of stmt insert checks                                                  |
| taos_stmt_is_insert_success                         | DOUBLE    |         | Count of successful stmt insert checks                                       |
| taos_stmt_is_insert_fail                            | DOUBLE    |         | Count of failed stmt insert checks                                           |
| taos_stmt_set_tbname_total                          | DOUBLE    |         | Count of stmt table name sets                                                |
| taos_stmt_set_tbname_success                        | DOUBLE    |         | Count of successful stmt table name sets                                     |
| taos_stmt_set_tbname_fail                           | DOUBLE    |         | Count of failed stmt table name sets                                         |
| taos_stmt_set_tags_total                            | DOUBLE    |         | Count of stmt tag sets                                                       |
| taos_stmt_set_tags_success                          | DOUBLE    |         | Count of successful stmt tag sets                                            |
| taos_stmt_set_tags_fail                             | DOUBLE    |         | Count of failed stmt tag sets                                                |
| taos_stmt_bind_param_batch_total                    | DOUBLE    |         | Count of stmt batch parameter bindings                                       |
| taos_stmt_bind_param_batch_success                  | DOUBLE    |         | Count of successful stmt batch parameter bindings                            |
| taos_stmt_bind_param_batch_fail                     | DOUBLE    |         | Count of failed stmt batch parameter bindings                                |
| taos_stmt_add_batch_total                           | DOUBLE    |         | Count of stmt batch additions                                                |
| taos_stmt_add_batch_success                         | DOUBLE    |         | Count of successful stmt batch additions                                     |
| taos_stmt_add_batch_fail                            | DOUBLE    |         | Count of failed stmt batch additions                                         |
| taos_stmt_execute_total                             | DOUBLE    |         | Count of stmt executions                                                     |
| taos_stmt_execute_success                           | DOUBLE    |         | Count of successful stmt executions                                          |
| taos_stmt_execute_fail                              | DOUBLE    |         | Count of failed stmt executions                                              |
| taos_stmt_num_params_total                          | DOUBLE    |         | Count of stmt parameter count fetches                                        |
| taos_stmt_num_params_success                        | DOUBLE    |         | Count of successful stmt parameter count fetches                             |
| taos_stmt_num_params_fail                           | DOUBLE    |         | Count of failed stmt parameter count fetches                                 |
| taos_stmt_get_param_total                           | DOUBLE    |         | Count of stmt parameter fetches                                              |
| taos_stmt_get_param_success                         | DOUBLE    |         | Count of successful stmt parameter fetches                                   |
| taos_stmt_get_param_fail                            | DOUBLE    |         | Count of failed stmt parameter fetches                                       |
| taos_stmt_errstr_total                              | DOUBLE    |         | Count of stmt error info fetches                                             |
| taos_stmt_errstr_success                            | DOUBLE    |         | Count of successful stmt error info fetches                                  |
| taos_stmt_affected_rows_once_total                  | DOUBLE    |         | Count of stmt affected rows fetches                                          |
| taos_stmt_affected_rows_once_success                | DOUBLE    |         | Count of successful stmt affected rows fetches                               |
| taos_stmt_use_result_total                          | DOUBLE    |         | Count of stmt result set uses                                                |
| taos_stmt_use_result_success                        | DOUBLE    |         | Count of successful stmt result set uses                                     |
| taos_stmt_use_result_fail                           | DOUBLE    |         | Count of failed stmt result set uses                                         |
| taos_select_db_total                                | DOUBLE    |         | Count of database selections                                                 |
| taos_select_db_success                              | DOUBLE    |         | Count of successful database selections                                      |
| taos_select_db_fail                                 | DOUBLE    |         | Count of failed database selections                                          |
| taos_get_tables_vgId_total                          | DOUBLE    |         | Count of table vgroup ID fetches                                             |
| taos_get_tables_vgId_success                        | DOUBLE    |         | Count of successful table vgroup ID fetches                                  |
| taos_get_tables_vgId_fail                           | DOUBLE    |         | Count of failed table vgroup ID fetches                                      |
| taos_options_connection_total                       | DOUBLE    |         | Count of connection option sets                                              |
| taos_options_connection_success                     | DOUBLE    |         | Count of successful connection option sets                                   |
| taos_options_connection_fail                        | DOUBLE    |         | Count of failed connection option sets                                       |
| taos_validate_sql_total                             | DOUBLE    |         | Count of SQL validations                                                     |
| taos_validate_sql_success                           | DOUBLE    |         | Count of successful SQL validations                                          |
| taos_validate_sql_fail                              | DOUBLE    |         | Count of failed SQL validations                                              |
| taos_check_server_status_total                      | DOUBLE    |         | Count of server status checks                                                |
| taos_check_server_status_success                    | DOUBLE    |         | Count of successful server status checks                                     |
| taos_get_current_db_total                           | DOUBLE    |         | Count of current database fetches                                            |
| taos_get_current_db_success                         | DOUBLE    |         | Count of successful current database fetches                                 |
| taos_get_current_db_fail                            | DOUBLE    |         | Count of failed current database fetches                                     |
| taos_get_server_info_total                          | DOUBLE    |         | Count of server info fetches                                                 |
| taos_get_server_info_success                        | DOUBLE    |         | Count of successful server info fetches                                      |
| taos_options_total                                  | DOUBLE    |         | Count of option sets                                                         |
| taos_options_success                                | DOUBLE    |         | Count of successful option sets                                              |
| taos_options_fail                                   | DOUBLE    |         | Count of failed option sets                                                  |
| taos_set_conn_mode_total                            | DOUBLE    |         | Count of connection mode sets                                                |
| taos_set_conn_mode_success                          | DOUBLE    |         | Count of successful connection mode sets                                     |
| taos_set_conn_mode_fail                             | DOUBLE    |         | Count of failed connection mode sets                                         |
| taos_reset_current_db_total                         | DOUBLE    |         | Count of current database resets                                             |
| taos_reset_current_db_success                       | DOUBLE    |         | Count of successful current database resets                                  |
| taos_set_notify_cb_total                            | DOUBLE    |         | Count of notification callback sets                                          |
| taos_set_notify_cb_success                          | DOUBLE    |         | Count of successful notification callback sets                               |
| taos_set_notify_cb_fail                             | DOUBLE    |         | Count of failed notification callback sets                                   |
| taos_errno_total                                    | DOUBLE    |         | Count of error code fetches                                                  |
| taos_errno_success                                  | DOUBLE    |         | Count of successful error code fetches                                       |
| taos_errstr_total                                   | DOUBLE    |         | Count of error message fetches                                               |
| taos_errstr_success                                 | DOUBLE    |         | Count of successful error message fetches                                    |
| tmq_consumer_poll_total                             | DOUBLE    |         | Count of TMQ consumer polls                                                  |
| tmq_consumer_poll_success                           | DOUBLE    |         | Count of successful TMQ consumer polls                                       |
| tmq_consumer_poll_fail                              | DOUBLE    |         | Count of failed TMQ consumer polls                                           |
| tmq_subscription_total                              | DOUBLE    |         | Count of TMQ subscription info fetches                                       |
| tmq_subscription_success                            | DOUBLE    |         | Count of successful TMQ subscription info fetches                            |
| tmq_subscription_fail                               | DOUBLE    |         | Count of failed TMQ subscription info fetches                                |
| tmq_list_append_total                               | DOUBLE    |         | Count of TMQ list appends                                                    |
| tmq_list_append_success                             | DOUBLE    |         | Count of successful TMQ list appends                                         |
| tmq_list_append_fail                                | DOUBLE    |         | Count of failed TMQ list appends                                             |
| tmq_list_get_size_total                             | DOUBLE    |         | Count of TMQ list size fetches                                               |
| tmq_list_get_size_success                           | DOUBLE    |         | Count of successful TMQ list size fetches                                    |
| tmq_err2str_total                                   | DOUBLE    |         | Count of TMQ error code to string conversions                                |
| tmq_err2str_success                                 | DOUBLE    |         | Count of successful TMQ error code to string conversions                     |
| tmq_conf_set_total                                  | DOUBLE    |         | Count of TMQ config sets                                                     |
| tmq_conf_set_success                                | DOUBLE    |         | Count of successful TMQ config sets                                          |
| tmq_conf_set_fail                                   | DOUBLE    |         | Count of failed TMQ config sets                                              |
| tmq_get_res_type_total                              | DOUBLE    |         | Count of TMQ resource type fetches                                           |
| tmq_get_res_type_success                            | DOUBLE    |         | Count of successful TMQ resource type fetches                                |
| tmq_get_topic_name_total                            | DOUBLE    |         | Count of TMQ topic name fetches                                              |
| tmq_get_topic_name_success                          | DOUBLE    |         | Count of successful TMQ topic name fetches                                   |
| tmq_get_vgroup_id_total                             | DOUBLE    |         | Count of TMQ vgroup ID fetches                                               |
| tmq_get_vgroup_id_success                           | DOUBLE    |         | Count of successful TMQ vgroup ID fetches                                    |
| tmq_get_vgroup_offset_total                         | DOUBLE    |         | Count of TMQ vgroup offset fetches                                           |
| tmq_get_vgroup_offset_success                       | DOUBLE    |         | Count of successful TMQ vgroup offset fetches                                |
| tmq_get_db_name_total                               | DOUBLE    |         | Count of TMQ database name fetches                                           |
| tmq_get_db_name_success                             | DOUBLE    |         | Count of successful TMQ database name fetches                                |
| tmq_get_table_name_total                            | DOUBLE    |         | Count of TMQ table name fetches                                              |
| tmq_get_table_name_success                          | DOUBLE    |         | Count of successful TMQ table name fetches                                   |
| tmq_get_connect_total                               | DOUBLE    |         | Count of TMQ connection fetches                                              |
| tmq_get_connect_success                             | DOUBLE    |         | Count of successful TMQ connection fetches                                   |
| tmq_commit_sync_total                               | DOUBLE    |         | Count of TMQ sync commits                                                    |
| tmq_commit_sync_success                             | DOUBLE    |         | Count of successful TMQ sync commits                                         |
| tmq_commit_sync_fail                                | DOUBLE    |         | Count of failed TMQ sync commits                                             |
| tmq_fetch_raw_block_total                           | DOUBLE    |         | Count of TMQ raw block fetches                                               |
| tmq_fetch_raw_block_success                         | DOUBLE    |         | Count of successful TMQ raw block fetches                                    |
| tmq_fetch_raw_block_fail                            | DOUBLE    |         | Count of failed TMQ raw block fetches                                        |
| tmq_get_topic_assignment_total                      | DOUBLE    |         | Count of TMQ topic assignment fetches                                        |
| tmq_get_topic_assignment_success                    | DOUBLE    |         | Count of successful TMQ topic assignment fetches                             |
| tmq_get_topic_assignment_fail                       | DOUBLE    |         | Count of failed TMQ topic assignment fetches                                 |
| tmq_offset_seek_total                               | DOUBLE    |         | Count of TMQ offset seeks                                                    |
| tmq_offset_seek_success                             | DOUBLE    |         | Count of successful TMQ offset seeks                                         |
| tmq_offset_seek_fail                                | DOUBLE    |         | Count of failed TMQ offset seeks                                             |
| tmq_committed_total                                 | DOUBLE    |         | Count of TMQ committed offset fetches                                        |
| tmq_committed_success                               | DOUBLE    |         | Count of successful TMQ committed offset fetches                             |
| tmq_commit_offset_sync_fail                         | DOUBLE    |         | Count of failed TMQ sync offset commits                                      |
| tmq_position_total                                  | DOUBLE    |         | Count of TMQ current position fetches                                        |
| tmq_position_success                                | DOUBLE    |         | Count of successful TMQ current position fetches                             |
| tmq_commit_offset_sync_total                        | DOUBLE    |         | Count of TMQ sync offset commits                                             |
| tmq_commit_offset_sync_success                      | DOUBLE    |         | Count of successful TMQ sync offset commits                                  |
| taos_connect_totp_total                             | DOUBLE    |         | Count of total TOTP authentication attempts (Available since v3.4.0.0)       |
| taos_connect_totp_success                           | DOUBLE    |         | Count of successful TOTP authentication attempts (Available since v3.4.0.0)  |
| taos_connect_totp_fail                              | DOUBLE    |         | Count of failed TOTP authentication attempts (Available since v3.4.0.0)      |
| taos_connect_token_total                            | DOUBLE    |         | Count of total token authentication attempts (Available since v3.4.0.0)      |
| taos_connect_token_success                          | DOUBLE    |         | Count of successful token authentication attempts (Available since v3.4.0.0) |
| taos_connect_token_fail                             | DOUBLE    |         | Count of failed token authentication attempts (Available since v3.4.0.0)     |
| taos_get_connection_info_total                      | DOUBLE    |         | Count of get_connection_info calls (Available since v3.4.0.0)                |
| taos_get_connection_info_success                    | DOUBLE    |         | Count of successful get_connection_info calls (Available since v3.4.0.0)     |
| taos_get_connection_info_fail                       | DOUBLE    |         | Count of failed get_connection_info calls (Available since v3.4.0.0)         |
| endpoint                                            | NCHAR     | TAG     | Request endpoint                                                             |

</details>

Starting from version **3.3.6.29**/**3.3.8.3**, the `adapter_request_limit` table has been added to record taosAdapter query request throttling metrics:

Note: The `adapter_request_limit` table is created on demand. taosKeeper creates this table in the monitoring database and writes data to it only when the query request concurrency limit feature is enabled (`request.queryLimitEnable = true`, which is `false` by default) and taosAdapter has processed at least one limited query request since startup. Otherwise, this table does not exist.

<details>
<summary>Details</summary>

| field                 | type      | is\_tag | comment                                                                                                                            |
|:----------------------|:----------|:--------|:-----------------------------------------------------------------------------------------------------------------------------------|
| _ts                   | TIMESTAMP |         | Data collection timestamp                                                                                                          |
| query_limit           | DOUBLE    |         | Maximum concurrency of query requests allowed to execute simultaneously                                                            |
| query_max_wait        | DOUBLE    |         | Maximum number of queries allowed to wait in the queue for execution                                                               |
| query_inflight        | DOUBLE    |         | Current number of queries being executed that are subject to concurrency limits                                                    |
| query_wait_count      | DOUBLE    |         | Current number of queries waiting in the queue for execution                                                                       |
| query_count           | DOUBLE    |         | Total number of query requests subject to concurrency limits received in this collection cycle                                     |
| query_wait_fail_count | DOUBLE    |         | Number of query requests that failed due to waiting timeout or exceeding the maximum waiting queue length in this collection cycle |
| endpoint              | NCHAR     | TAG     | Request endpoint                                                                                                                   |
| user                  | NCHAR     | TAG     | Authenticated username that initiated the query request                                                                            |

</details>

Starting from version **3.4.0.0**, the `adapter_input_json` table has been added to record taosAdapter JSON write input metrics:

<details>
<summary>Details</summary>

| field         | type      | is\_tag | comment                                |
|:--------------|:----------|:--------|:---------------------------------------|
| _ts           | TIMESTAMP |         | Data collection timestamp              |
| total_rows    | DOUBLE    |         | Total number of rows in JSON write     |
| success_rows  | DOUBLE    |         | Number of successfully written rows    |
| fail_rows     | DOUBLE    |         | Number of failed rows in JSON write    |
| inflight_rows | DOUBLE    |         | Number of rows currently being written |
| affected_rows | DOUBLE    |         | Number of rows affected by JSON write  |
| url_endpoint  | NCHAR     | TAG     | URL endpoint of the JSON write request |
| endpoint      | NCHAR     | TAG     | Request taosAdapter endpoint           |

</details>
