---
title: 运维与监控
sidebar_label: 运维与监控
toc_max_heading_level: 4
---

## 记录 SQL 到 CSV 文件

taosAdapter 支持将 SQL 请求记录到 CSV 文件中。用户可以通过配置参数 `log.enableSqlToCsvLogging` 来启用此功能，或使用 HTTP 请求动态开启和关闭。

### 配置参数

1. 新增配置项 `log.enableSqlToCsvLogging` 布尔值，默认为 false，表示是否开启 sql 记录到 csv 文件。设置为 true 将开启 sql 记录到 csv 文件任务，开始记录时间为启动时间，结束时间为 `2300-01-01 00:00:00`。
2. 文件命名与日志相同规则：`taosadapterSql_{instanceId}_{yyyyMMdd}.csv[.index]`
   - `instanceId`：taosAdapter 实例 ID，可通过 `instanceId` 参数设置。
   - `yyyyMMdd`：日期，格式为年月日。
   - `index`：如果存在多个文件，则会在文件名后添加数字后缀。
3. 保留空间、文件切割、保存路径等使用 log 已存在参数：
   - `log.path`：保存路径。
   - `log.keepDays` ：保留天数。
   - `log.rotationCount`：最多保留份数。
   - `log.rotationSize`：单个文件最大大小。
   - `log.compress`：是否启用压缩。
   - `log.reservedDiskSize`：保留硬盘空间大小。

### 动态开启

通过发送 HTTP POST 请求到 `/record_sql` 接口来动态开启记录，使用与 `/rest/sql` 相同的鉴权方式，样例如下：

```bash
curl --location --request POST 'http://127.0.0.1:6041/record_sql' \
-u root:taosdata \
--data '{"start_time":"2025-07-15 17:00:00","end_time":"2025-07-15 18:00:00","location":"Asia/Shanghai"}'
```

支持的参数项如下：

- start_time：[可选参数] 开始采集的时间，格式为 `yyyy-MM-dd HH:mm:ss`，如果不设置则使用当前时间。
- end_time：[可选参数] 结束采集的时间，格式为 `yyyy-MM-dd HH:mm:ss`，如果不设置则使用 `2300-01-01 00:00:00`。
- location：[可选参数] 解析采集开始和结束时间使用的时区信息，如果不设置则使用 taosAdapter 所在服务器时区。时区使用 IANA 格式，例如：`Asia/Shanghai`。

如果所有参数都使用默认值则可以不传 data，样例如下：

```bash
curl --location --request POST 'http://127.0.0.1:6041/record_sql' \
-u root:taosdata
```

成功返回 HTTP code 200，返回结构如下

```json
{"code":0,"desc":""}
```

失败返回 HTTP code 非 200，返回 json 结构如下，code 非 0，desc 描述错误内容

```json
{"code":65535,"desc":"unmarshal json error"}
```

### 动态关闭

通过发送 HTTP DELETE 请求到 `/record_sql` 接口来关闭，使用与 `/rest/sql` 相同的鉴权方式，样例如下：

```bash
curl --location --request DELETE 'http://127.0.0.1:6041/record_sql' \
-u root:taosdata
```

成功返回 HTTP code 200

1. 任务存在时返回如下

```json
{
        "code": 0,
        "message": "",
        "start_time": "2025-07-23 17:00:00",
        "end_time": "2025-07-23 18:00:00"
}
```

- start_time 为取消任务配置的启动时间，时区为 taosAdapter 所在服务器时区。
- end_time 为取消任务配置的结束时间，时区为 taosAdapter 所在服务器时区。

2. 任务不存在时返回如下

```json
{
        "code": 0,
        "message": ""
}
```

### 查询状态

通过发送 HTTP GET 请求到 `/record_sql` 接口来查询任务，使用与 `/rest/sql` 相同的鉴权方式，样例如下：

```bash
curl --location 'http://127.0.0.1:6041/record_sql' \
-u root:taosdata
```

成功返回 HTTP code 200，返回样例如下

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

- code：错误码，0 为成功。
- desc：错误信息，成功为空字符串。
- exists：任务是否存在。
- running：任务是否在运行期。
- start_time：开始时间，时区为 taosAdapter 所在服务器时区。
- end_time：结束时间，时区为 taosAdapter 所在服务器时区。
- current_concurrent：当前 SQL 记录并发度。

### 记录格式

在 `taos_free_result` 执行之前和任务结束（到达结束时间或主动关闭）时写入记录。
记录以 CSV 格式存储，无表头，每行记录包含以下字段：

1. TS：打印日志时间，格式为 `yyyy-MM-dd HH:mm:ss.SSSSSS`，时区为 taosAdapter 所在服务器时区。
2. SQL：执行的 SQL，按照 CSV 标准不处理 SQL 中的换行符，当存在特殊字符（\n、\r、"）时使用双引号包裹，包含特殊字符时无法直接复制 SQL 使用，例如：

  原始 sql 为：

  ```sql
   select * from t1
   where c1 = "ab"
  ```

   csv 文件中的记录为：

   ```csv
   "select * from t1
   where c1 = ""ab"""
   ```

3. IP：客户端 IP。
4. User：执行此 SQL 的用户名。
5. ConnType：连接类型（HTTP、WS）。
6. QID：请求 ID，保存为 16 进制。
7. ReceiveTime：接收到 SQL 的时间，格式为 `yyyy-MM-dd HH:mm:ss.SSSSSS`，时区为 taosAdapter 所在服务器时区。
8. FreeTime：SQL 释放的时间，格式为 `yyyy-MM-dd HH:mm:ss.SSSSSS`，时区为 taosAdapter 所在服务器时区。
9. QueryDuration(us)：执行 `taos_query_a` 到回调完成时间消耗，单位微秒。
10. FetchDuration(us)：多次执行 `taos_fetch_raw_block_a` 到回调完成的时间消耗累加值，单位微秒。
11. GetConnDuration(us)：HTTP 请求从连接池获取连接的时间消耗，单位微秒。
12. TotalDuration(us)：SQL 请求完成总时间，单位微秒，当 SQL 正常完成时为（SQL 释放的时间 - 接收到 SQL 的时间），当任务结束未完成时为（当前时间 - 接收到 SQL 的时间）。
13. SourcePort：客户端端口。（3.3.6.26 及以上 / 3.3.8.0 及以上版本添加）
14. AppName：客户应用 AppName。（3.3.6.26 及以上 / 3.3.8.0 及以上版本添加）

样例如下：

```csv
2025-07-23 17:10:08.724775,show databases,127.0.0.1,root,http,0x2000000000000008,2025-07-23 17:10:08.707741,2025-07-23 17:10:08.724775,14191,965,1706,17034,53600,jdbc_test_app
```

## 记录 STMT 到 CSV 文件

taosAdapter 从 `v3.4.0.1` 版本开始支持将 STMT 请求记录到 CSV 文件中。用户可以通过配置参数 `log.enableStmtToCsvLogging` 来启用此功能，或使用 HTTP 请求动态开启和关闭。

:::warning
启用此功能会导致性能严重下降。
:::

### 配置参数

1. 新增配置项 `log.enableStmtToCsvLogging` 布尔值，默认为 false，表示是否开启 STMT 记录到 csv 文件。设置为 true 将开启 STMT 记录到 csv 文件任务，开始记录时间为启动时间，结束时间为 `2300-01-01 00:00:00`。
2. 文件命名与日志相同规则：`taosadapterStmt_{instanceId}_{yyyyMMdd}.csv[.index]`
   - `instanceId`：taosAdapter 实例 ID，可通过 `instanceId` 参数设置。
   - `yyyyMMdd`：日期，格式为年月日。
   - `index`：如果存在多个文件，则会在文件名后添加数字后缀。
3. 保留空间、文件切割、保存路径等使用 log 已存在参数：
   - `log.path`：保存路径。
   - `log.keepDays` ：保留天数。
   - `log.rotationCount`：最多保留份数。
   - `log.rotationSize`：单个文件最大大小。
   - `log.compress`：是否启用压缩。
   - `log.reservedDiskSize`：保留硬盘空间大小。

### 动态开启

通过发送 HTTP POST 请求到 `/record_stmt` 接口来动态开启记录，使用与 `/rest/sql` 相同的鉴权方式，样例如下：

```bash
curl --location --request POST 'http://127.0.0.1:6041/record_stmt' \
-u root:taosdata \
--data '{"start_time":"2026-01-13 17:00:00","end_time":"2026-01-13 18:00:00","location":"Asia/Shanghai"}'
```

支持的参数项如下：

- start_time：[可选参数] 开始采集的时间，格式为 `yyyy-MM-dd HH:mm:ss`，如果不设置则使用当前时间。
- end_time：[可选参数] 结束采集的时间，格式为 `yyyy-MM-dd HH:mm:ss`，如果不设置则使用为当前时间加一小时。
- location：[可选参数] 解析采集开始和结束时间使用的时区信息，如果不设置则使用 taosAdapter 所在服务器时区。时区使用 IANA 格式，例如：`Asia/Shanghai`。

如果所有参数都使用默认值则可以不传 data，样例如下：

```bash
curl --location --request POST 'http://127.0.0.1:6041/record_stmt' \
-u root:taosdata
```

成功返回 HTTP code 200，返回结构如下

```json
{"code":0,"desc":""}
```

失败返回 HTTP code 非 200，返回 json 结构如下，code 非 0，desc 描述错误内容

```json
{"code":65535,"desc":"unmarshal json error"}
```

### 动态关闭

通过发送 HTTP DELETE 请求到 `/record_stmt` 接口来关闭，使用与 `/rest/sql` 相同的鉴权方式，样例如下：

```bash
curl --location --request DELETE 'http://127.0.0.1:6041/record_stmt' \
-u root:taosdata
```

成功返回 HTTP code 200

1. 任务存在时返回如下

```json
{
        "code": 0,
        "message": "",
        "start_time": "2026-01-13 17:00:00",
        "end_time": "2026-01-13 18:00:00"
}
```

- start_time 为取消任务配置的启动时间，时区为 taosAdapter 所在服务器时区。
- end_time 为取消任务配置的结束时间，时区为 taosAdapter 所在服务器时区。

2. 任务不存在时返回如下

```json
{
        "code": 0,
        "message": ""
}
```

### 查询状态

通过发送 HTTP GET 请求到 `/record_stmt` 接口来查询任务，使用与 `/rest/sql` 相同的鉴权方式，样例如下：

```bash
curl --location 'http://127.0.0.1:6041/record_stmt' \
-u root:taosdata
```

成功返回 HTTP code 200，返回样例如下

```json
{
        "code": 0,
        "desc": "",
        "exists": true,
        "running": true,
        "start_time": "2026-01-13 17:00:00",
        "end_time": "2026-01-13 18:00:00",
        "current_concurrent": 1
}
```

- code：错误码，0 为成功。
- desc：错误信息，成功为空字符串。
- exists：任务是否存在。
- running：任务是否在运行期。
- start_time：开始时间，时区为 taosAdapter 所在服务器时区。
- end_time：结束时间，时区为 taosAdapter 所在服务器时区。
- current_concurrent：当前 STMT 记录并发度。

### 记录格式

在执行 prepare、bind、exec 之后和任务结束（到达结束时间或主动关闭）时写入记录。
记录以 CSV 格式存储，无表头，每行记录包含以下字段：

1. TS：打印日志时间，格式为 `yyyy-MM-dd HH:mm:ss.SSSSSS`，时区为 taosAdapter 所在服务器时区。
2. IP：客户端 IP。
3. SourcePort：客户端端口。
4. AppName：客户应用名称。
5. User：当前连接的用户名。
6. ConnType：连接类型（ws）。
7. QID：请求 ID，保存为 16 进制。
8. StartTime: 开始处理时间，格式为 `yyyy-MM-dd HH:mm:ss.SSSSSS`，时区为 taosAdapter 所在服务器时区。
9. STMT2：stmt2 内存地址，保存为 16 进制。
10. Action：操作（prepare、bind、exec）。
11. Code：操作结果 0 代表成功，其他代表错误码。
12. Duration(us)：执行时间，未执行完为 -1。
13. Data：请求或结果数据
    1. prepare 为准备的 sql
    2. exec 为影响行数
    3. bind 为绑定数据，解析成功为 JSON 数据，解析失败将为原始二进制数据

#### bind 数据 JSON 格式

bind 操作的 Data 字段内容为 JSON 格式，包含以下字段：

| 字段名         | 类型                         | 说明           |
|:------------|:---------------------------|:-------------|
| count       | int                        | 绑定参数的表数量     |
| table_names | [count]string              | 绑定参数的表名称列表   |
| tags        | [count][tag_count]col_info | 绑定参数的标签列信息列表 |
| cols        | [count][col_count]col_info | 绑定参数的普通列信息列表 |

其中 col_info 结构包含以下字段：

| 字段名  | 类型    | 说明              |
|:-----|:------|:----------------|
| type | int   | 列类型             |
| data | []any | 一维数组，每个元素代表一行数据 |

data 字段中每种数据的解析规则如下：

1. varbinary、blob、geometry 解析为十六进制字符串
2. binary、nchar、json、decimal 解析为 utf-8 字符串
3. 数字类型转为数字
4. Datetime 类型转为数字
5. Bool 类型转为 json bool（true/false）
6. null 数据转为 json null（null）

样例如下：

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

## taosAdapter 监控指标

taosAdapter 目前仅采集 RESTful/WebSocket 相关请求的监控指标，其他接口暂无监控指标。

taosAdapter 将监控指标上报给 taosKeeper，这些监控指标会被 taosKeeper 写入监控数据库，默认是 `log` 库，可以在 taosKeeper 配置文件中修改。以下是这些监控指标的详细介绍。

`adapter_requests` 表记录 taosAdapter 监控数据：

<details>
<summary>详细信息</summary>

| field              | type             | is\_tag | comment                     |
|:-------------------|:-----------------|:--------|:----------------------------|
| ts                 | TIMESTAMP        |         | 数据采集时间戳                     |
| total              | INT UNSIGNED     |         | 总请求数                        |
| query              | INT UNSIGNED     |         | 查询请求数                       |
| write              | INT UNSIGNED     |         | 写入请求数                       |
| other              | INT UNSIGNED     |         | 其他请求数                       |
| in\_process        | INT UNSIGNED     |         | 正在处理请求数                     |
| success            | INT UNSIGNED     |         | 成功请求数                       |
| fail               | INT UNSIGNED     |         | 失败请求数                       |
| query\_success     | INT UNSIGNED     |         | 查询成功请求数                     |
| query\_fail        | INT UNSIGNED     |         | 查询失败请求数                     |
| write\_success     | INT UNSIGNED     |         | 写入成功请求数                     |
| write\_fail        | INT UNSIGNED     |         | 写入失败请求数                     |
| other\_success     | INT UNSIGNED     |         | 其他成功请求数                     |
| other\_fail        | INT UNSIGNED     |         | 其他失败请求数                     |
| query\_in\_process | INT UNSIGNED     |         | 正在处理查询请求数                   |
| write\_in\_process | INT UNSIGNED     |         | 正在处理写入请求数                   |
| endpoint           | VARCHAR          | TAG     | 请求端点                        |
| req\_type          | TINYINT UNSIGNED | TAG     | 请求类型：0 为 REST，1 为 WebSocket |

</details>

`adapter_status` 表记录 taosAdapter 状态数据：

<details>
<summary>详细信息</summary>

| field                     | type      | is\_tag | comment                                             |
|:--------------------------|:----------|:--------|:----------------------------------------------------|
| _ts                       | TIMESTAMP |         | 数据采集时间戳                                             |
| go_heap_sys               | DOUBLE    |         | Go 运行时系统分配的堆内存大小（字节）                                |
| go_heap_inuse             | DOUBLE    |         | Go 运行时正在使用的堆内存大小（字节）                                |
| go_stack_sys              | DOUBLE    |         | Go 运行时系统分配的栈内存大小（字节）                                |
| go_stack_inuse            | DOUBLE    |         | Go 运行时正在使用的栈内存大小（字节）                                |
| rss                       | DOUBLE    |         | 进程实际占用的物理内存大小（字节）                                   |
| ws_query_conn             | DOUBLE    |         | `/rest/ws` 接口当前 WebSocket 连接数                       |
| ws_stmt_conn              | DOUBLE    |         | `/rest/stmt` 接口当前 WebSocket 连接数                     |
| ws_sml_conn               | DOUBLE    |         | `/rest/schemaless` 接口当前 WebSocket 连接数               |
| ws_ws_conn                | DOUBLE    |         | `/ws` 接口当前 WebSocket 连接数                            |
| ws_tmq_conn               | DOUBLE    |         | `/rest/tmq` 接口当前 WebSocket 连接数                      |
| async_c_limit             | DOUBLE    |         | C 同步接口并发限制总数                                        |
| async_c_inflight          | DOUBLE    |         | C 同步接口当前并发数                                         |
| sync_c_limit              | DOUBLE    |         | C 异步接口并发限制总数                                        |
| sync_c_inflight           | DOUBLE    |         | C 异步接口当前并发数                                         |
| ws_query_conn_inc         | DOUBLE    |         | /rest/ws 接口新增连接（3.3.6.10 及以上）                       |
| ws_query_conn_dec         | DOUBLE    |         | /rest/ws 接口减少连接（3.3.6.10 及以上）                       |
| ws_stmt_conn_inc          | DOUBLE    |         | /rest/stmt 接口新增连接（3.3.6.10 及以上）                     |
| ws_stmt_conn_dec          | DOUBLE    |         | /rest/stmt 接口减少连接（3.3.6.10 及以上）                     |
| ws_sml_conn_inc           | DOUBLE    |         | /rest/schemaless 接口新增连接（3.3.6.10 及以上）               |
| ws_sml_conn_dec           | DOUBLE    |         | /rest/schemaless 接口减少连接（3.3.6.10 及以上）               |
| ws_ws_conn_inc            | DOUBLE    |         | /ws 接口新增连接（3.3.6.10 及以上）                            |
| ws_ws_conn_dec            | DOUBLE    |         | /ws 接口减少连接（3.3.6.10 及以上）                            |
| ws_tmq_conn_inc           | DOUBLE    |         | /rest/tmq 接口新增连接（3.3.6.10 及以上）                      |
| ws_tmq_conn_dec           | DOUBLE    |         | /rest/tmq 接口减少连接（3.3.6.10 及以上）                      |
| ws_query_sql_result_count | DOUBLE    |         | /rest/ws 接口当前持有 SQL 查询结果数量（3.3.6.10 及以上）            |
| ws_stmt_stmt_count        | DOUBLE    |         | /rest/stmt 接口当前持有 stmt 数量（3.3.6.10 及以上）             |
| ws_ws_sql_result_count    | DOUBLE    |         | /ws 接口当前持有 SQL 查询结果数量（3.3.6.10 及以上）                 |
| ws_ws_stmt_count          | DOUBLE    |         | /ws 接口当前持有 stmt 数量（3.3.6.10 及以上）                    |
| ws_ws_stmt2_count         | DOUBLE    |         | /ws 接口当前持有 stmt2 数量（3.3.6.10 及以上）                   |
| cpu_percent               | DOUBLE    |         | taosAdapter 的 CPU 占用百分比（v3.3.6.24 及以上 /v3.3.7.7 及以上） |
| endpoint                  | NCHAR     | TAG     | 请求端点                                                |

</details>

`adapter_conn_pool` 表记录 taosAdapter 连接池监控数据：

<details>
<summary>详细信息</summary>

| field            | type      | is\_tag | comment       |
|:-----------------|:----------|:--------|:--------------|
| _ts              | TIMESTAMP |         | 数据采集时间戳       |
| conn_pool_total  | DOUBLE    |         | 连接池的最大连接数限制   |
| conn_pool_in_use | DOUBLE    |         | 连接池当前正在使用的连接数 |
| endpoint         | NCHAR     | TAG     | 请求端点          |
| user             | NCHAR     | TAG     | 连接池所属的用户名     |

</details>

从 **3.3.6.10** 版本开始新增 `adapter_c_interface` 表记录 taosAdapter 调用 C 接口次数数据：

<details>
<summary>详细信息</summary>

| field                                               | type      | is\_tag | comment                   |
|:----------------------------------------------------|:----------|:--------|:--------------------------|
| _ts                                                 | TIMESTAMP |         | 数据采集时间戳                   |
| taos_connect_total                                  | DOUBLE    |         | 尝试建立连接的总次数                |
| taos_connect_success                                | DOUBLE    |         | 成功建立连接的次数                 |
| taos_connect_fail                                   | DOUBLE    |         | 建立连接失败的次数                 |
| taos_close_total                                    | DOUBLE    |         | 尝试关闭连接的总次数                |
| taos_close_success                                  | DOUBLE    |         | 成功关闭连接的次数                 |
| taos_schemaless_insert_total                        | DOUBLE    |         | schemaless 插入操作的总次数       |
| taos_schemaless_insert_success                      | DOUBLE    |         | schemaless 插入成功的次数        |
| taos_schemaless_insert_fail                         | DOUBLE    |         | schemaless 插入失败的次数        |
| taos_schemaless_free_result_total                   | DOUBLE    |         | schemaless 释放结果集的总次数      |
| taos_schemaless_free_result_success                 | DOUBLE    |         | schemaless 成功释放结果集的次数     |
| taos_query_total                                    | DOUBLE    |         | 执行同步 SQL 的总次数             |
| taos_query_success                                  | DOUBLE    |         | 执行同步 SQL 成功的次数            |
| taos_query_fail                                     | DOUBLE    |         | 执行同步 SQL 失败的次数            |
| taos_query_free_result_total                        | DOUBLE    |         | 释放同步 SQL 结果集的总次数          |
| taos_query_free_result_success                      | DOUBLE    |         | 成功释放同步 SQL 结果集的次数         |
| taos_query_a_with_reqid_total                       | DOUBLE    |         | 带请求 ID 的异步 SQL 总次数        |
| taos_query_a_with_reqid_success                     | DOUBLE    |         | 带请求 ID 的异步 SQL 成功次数       |
| taos_query_a_with_reqid_callback_total              | DOUBLE    |         | 带请求 ID 的异步 SQL 回调总次数      |
| taos_query_a_with_reqid_callback_success            | DOUBLE    |         | 带请求 ID 的异步 SQL 回调成功次数     |
| taos_query_a_with_reqid_callback_fail               | DOUBLE    |         | 带请求 ID 的异步 SQL 回调失败次数     |
| taos_query_a_free_result_total                      | DOUBLE    |         | 异步 SQL 释放结果集的总次数          |
| taos_query_a_free_result_success                    | DOUBLE    |         | 异步 SQL 成功释放结果集的次数         |
| tmq_consumer_poll_result_total                      | DOUBLE    |         | 消费者 poll 有数据的总次数          |
| tmq_free_result_total                               | DOUBLE    |         | 释放 TMQ 数据的总次数             |
| tmq_free_result_success                             | DOUBLE    |         | 成功释放 TMQ 数据的次数            |
| taos_stmt2_init_total                               | DOUBLE    |         | stmt2 初始化的总次数             |
| taos_stmt2_init_success                             | DOUBLE    |         | stmt2 初始化成功的次数            |
| taos_stmt2_init_fail                                | DOUBLE    |         | stmt2 初始化失败的次数            |
| taos_stmt2_close_total                              | DOUBLE    |         | stmt2 关闭的总次数              |
| taos_stmt2_close_success                            | DOUBLE    |         | stmt2 关闭成功的次数             |
| taos_stmt2_close_fail                               | DOUBLE    |         | stmt2 关闭失败的次数             |
| taos_stmt2_get_fields_total                         | DOUBLE    |         | stmt2 获取字段的总次数            |
| taos_stmt2_get_fields_success                       | DOUBLE    |         | stmt2 成功获取字段的次数           |
| taos_stmt2_get_fields_fail                          | DOUBLE    |         | stmt2 获取字段失败的次数           |
| taos_stmt2_free_fields_total                        | DOUBLE    |         | stmt2 释放字段的总次数            |
| taos_stmt2_free_fields_success                      | DOUBLE    |         | stmt2 成功释放字段的次数           |
| taos_stmt_init_with_reqid_total                     | DOUBLE    |         | 带请求 ID 的 stmt 初始化总次数      |
| taos_stmt_init_with_reqid_success                   | DOUBLE    |         | 带请求 ID 的 stmt 初始化成功次数     |
| taos_stmt_init_with_reqid_fail                      | DOUBLE    |         | 带请求 ID 的 stmt 初始化失败次数     |
| taos_stmt_close_total                               | DOUBLE    |         | stmt 关闭的总次数               |
| taos_stmt_close_success                             | DOUBLE    |         | stmt 关闭成功的次数              |
| taos_stmt_close_fail                                | DOUBLE    |         | stmt 关闭失败的次数              |
| taos_stmt_get_tag_fields_total                      | DOUBLE    |         | stmt 获取 tag 字段的总次数        |
| taos_stmt_get_tag_fields_success                    | DOUBLE    |         | stmt 成功获取 tag 字段的次数       |
| taos_stmt_get_tag_fields_fail                       | DOUBLE    |         | stmt 获取 tag 字段失败的次数       |
| taos_stmt_get_col_fields_total                      | DOUBLE    |         | stmt 获取列字段的总次数            |
| taos_stmt_get_col_fields_success                    | DOUBLE    |         | stmt 成功获取列字段的次数           |
| taos_stmt_get_col_fields_fail                       | DOUBLE    |         | stmt 获取列字段失败的次数           |
| taos_stmt_reclaim_fields_total                      | DOUBLE    |         | stmt 释放字段的总次数             |
| taos_stmt_reclaim_fields_success                    | DOUBLE    |         | stmt 成功释放字段的次数            |
| tmq_get_json_meta_total                             | DOUBLE    |         | tmq 获取 JSON 元数据的总次数       |
| tmq_get_json_meta_success                           | DOUBLE    |         | tmq 成功获取 JSON 元数据的次数      |
| tmq_free_json_meta_total                            | DOUBLE    |         | tmq 释放 JSON 元数据的总次数       |
| tmq_free_json_meta_success                          | DOUBLE    |         | tmq 成功释放 JSON 元数据的次数      |
| taos_fetch_whitelist_a_total                        | DOUBLE    |         | 异步获取白名单的总次数               |
| taos_fetch_whitelist_a_success                      | DOUBLE    |         | 异步成功获取白名单的次数              |
| taos_fetch_whitelist_a_callback_total               | DOUBLE    |         | 异步获取白名单回调总次数              |
| taos_fetch_whitelist_a_callback_success             | DOUBLE    |         | 异步成功获取白名单回调次数             |
| taos_fetch_whitelist_a_callback_fail                | DOUBLE    |         | 异步获取白名单回调失败次数             |
| taos_fetch_rows_a_total                             | DOUBLE    |         | 异步获取行的总次数                 |
| taos_fetch_rows_a_success                           | DOUBLE    |         | 异步成功获取行的次数                |
| taos_fetch_rows_a_callback_total                    | DOUBLE    |         | 异步获取行回调总次数                |
| taos_fetch_rows_a_callback_success                  | DOUBLE    |         | 异步成功获取行回调次数               |
| taos_fetch_rows_a_callback_fail                     | DOUBLE    |         | 异步获取行回调失败次数               |
| taos_fetch_raw_block_a_total                        | DOUBLE    |         | 异步获取原始块的总次数               |
| taos_fetch_raw_block_a_success                      | DOUBLE    |         | 异步成功获取原始块的次数              |
| taos_fetch_raw_block_a_callback_total               | DOUBLE    |         | 异步获取原始块回调总次数              |
| taos_fetch_raw_block_a_callback_success             | DOUBLE    |         | 异步成功获取原始块回调次数             |
| taos_fetch_raw_block_a_callback_fail                | DOUBLE    |         | 异步获取原始块回调失败次数             |
| tmq_get_raw_total                                   | DOUBLE    |         | 获取原始数据的总次数                |
| tmq_get_raw_success                                 | DOUBLE    |         | 成功获取原始数据的次数               |
| tmq_get_raw_fail                                    | DOUBLE    |         | 获取原始数据失败的次数               |
| tmq_free_raw_total                                  | DOUBLE    |         | 释放原始数据的总次数                |
| tmq_free_raw_success                                | DOUBLE    |         | 成功释放原始数据的次数               |
| tmq_consumer_new_total                              | DOUBLE    |         | 创建新消费者的总次数                |
| tmq_consumer_new_success                            | DOUBLE    |         | 成功创建新消费者的次数               |
| tmq_consumer_new_fail                               | DOUBLE    |         | 创建新消费者失败的次数               |
| tmq_consumer_close_total                            | DOUBLE    |         | 关闭消费者的总次数                 |
| tmq_consumer_close_success                          | DOUBLE    |         | 成功关闭消费者的次数                |
| tmq_consumer_close_fail                             | DOUBLE    |         | 关闭消费者失败的次数                |
| tmq_subscribe_total                                 | DOUBLE    |         | 订阅主题的总次数                  |
| tmq_subscribe_success                               | DOUBLE    |         | 成功订阅主题的次数                 |
| tmq_subscribe_fail                                  | DOUBLE    |         | 订阅主题失败的次数                 |
| tmq_unsubscribe_total                               | DOUBLE    |         | 取消订阅的总次数                  |
| tmq_unsubscribe_success                             | DOUBLE    |         | 成功取消订阅的次数                 |
| tmq_unsubscribe_fail                                | DOUBLE    |         | 取消订阅失败的次数                 |
| tmq_list_new_total                                  | DOUBLE    |         | 创建新主题列表的总次数               |
| tmq_list_new_success                                | DOUBLE    |         | 成功创建新主题列表的次数              |
| tmq_list_new_fail                                   | DOUBLE    |         | 创建新主题列表失败的次数              |
| tmq_list_destroy_total                              | DOUBLE    |         | 销毁主题列表的总次数                |
| tmq_list_destroy_success                            | DOUBLE    |         | 成功销毁主题列表的次数               |
| tmq_conf_new_total                                  | DOUBLE    |         | tmq 创建新配置的总次数             |
| tmq_conf_new_success                                | DOUBLE    |         | tmq 成功创建新配置的次数            |
| tmq_conf_new_fail                                   | DOUBLE    |         | tmq 创建新配置失败的次数            |
| tmq_conf_destroy_total                              | DOUBLE    |         | tmq 销毁配置的总次数              |
| tmq_conf_destroy_success                            | DOUBLE    |         | tmq 成功销毁配置的次数             |
| taos_stmt2_prepare_total                            | DOUBLE    |         | stmt2 准备的总次数              |
| taos_stmt2_prepare_success                          | DOUBLE    |         | stmt2 准备成功的次数             |
| taos_stmt2_prepare_fail                             | DOUBLE    |         | stmt2 准备失败的次数             |
| taos_stmt2_is_insert_total                          | DOUBLE    |         | 检查是否为插入的总次数               |
| taos_stmt2_is_insert_success                        | DOUBLE    |         | 成功检查是否为插入的次数              |
| taos_stmt2_is_insert_fail                           | DOUBLE    |         | 检查是否为插入失败的次数              |
| taos_stmt2_bind_param_total                         | DOUBLE    |         | stmt2 绑定参数的总次数            |
| taos_stmt2_bind_param_success                       | DOUBLE    |         | stmt2 成功绑定参数的次数           |
| taos_stmt2_bind_param_fail                          | DOUBLE    |         | stmt2 绑定参数失败的次数           |
| taos_stmt2_exec_total                               | DOUBLE    |         | stmt2 执行的总次数              |
| taos_stmt2_exec_success                             | DOUBLE    |         | stmt2 执行成功的次数             |
| taos_stmt2_exec_fail                                | DOUBLE    |         | stmt2 执行失败的次数             |
| taos_stmt2_error_total                              | DOUBLE    |         | stmt2 错误检查的总次数            |
| taos_stmt2_error_success                            | DOUBLE    |         | stmt2 成功检查错误的次数           |
| taos_fetch_row_total                                | DOUBLE    |         | 同步获取行的总次数                 |
| taos_fetch_row_success                              | DOUBLE    |         | 成功同步获取行的次数                |
| taos_is_update_query_total                          | DOUBLE    |         | 检查是否为更新语句的总次数             |
| taos_is_update_query_success                        | DOUBLE    |         | 成功检查是否为更新语句的次数            |
| taos_affected_rows_total                            | DOUBLE    |         | SQL 获取影响行数的总次数            |
| taos_affected_rows_success                          | DOUBLE    |         | SQL 成功获取影响行数的次数           |
| taos_num_fields_total                               | DOUBLE    |         | 获取字段数量的总次数                |
| taos_num_fields_success                             | DOUBLE    |         | 成功获取字段数量的次数               |
| taos_fetch_fields_e_total                           | DOUBLE    |         | 获取字段信息的扩展总次数              |
| taos_fetch_fields_e_success                         | DOUBLE    |         | 成功获取字段信息的扩展次数             |
| taos_fetch_fields_e_fail                            | DOUBLE    |         | 获取字段信息的扩展失败次数             |
| taos_result_precision_total                         | DOUBLE    |         | 获取结果精度的总次数                |
| taos_result_precision_success                       | DOUBLE    |         | 成功获取结果精度的次数               |
| taos_get_raw_block_total                            | DOUBLE    |         | 获取原始块的总次数                 |
| taos_get_raw_block_success                          | DOUBLE    |         | 成功获取原始块的次数                |
| taos_fetch_raw_block_total                          | DOUBLE    |         | 拉取原始块的总次数                 |
| taos_fetch_raw_block_success                        | DOUBLE    |         | 成功拉取原始块的次数                |
| taos_fetch_raw_block_fail                           | DOUBLE    |         | 拉取原始块失败的次数                |
| taos_fetch_lengths_total                            | DOUBLE    |         | 获取字段长度的总次数                |
| taos_fetch_lengths_success                          | DOUBLE    |         | 成功获取字段长度的次数               |
| taos_write_raw_block_with_reqid_total               | DOUBLE    |         | 带请求 ID 写入原始块的总次数          |
| taos_write_raw_block_with_reqid_success             | DOUBLE    |         | 带请求 ID 成功写入原始块的次数         |
| taos_write_raw_block_with_reqid_fail                | DOUBLE    |         | 带请求 ID 写入原始块失败的次数         |
| taos_write_raw_block_with_fields_with_reqid_total   | DOUBLE    |         | 带请求 ID 和字段写入原始块的总次数       |
| taos_write_raw_block_with_fields_with_reqid_success | DOUBLE    |         | 带请求 ID 和字段成功写入原始块的次数      |
| taos_write_raw_block_with_fields_with_reqid_fail    | DOUBLE    |         | 带请求 ID 和字段写入原始块失败的次数      |
| tmq_write_raw_total                                 | DOUBLE    |         | 写入原始数据的 TMQ 总次数           |
| tmq_write_raw_success                               | DOUBLE    |         | 成功写入原始数据的 TMQ 次数          |
| tmq_write_raw_fail                                  | DOUBLE    |         | 写入原始数据的 TMQ 失败次数          |
| taos_stmt_prepare_total                             | DOUBLE    |         | stmt 准备的总次数               |
| taos_stmt_prepare_success                           | DOUBLE    |         | stmt 准备成功的次数              |
| taos_stmt_prepare_fail                              | DOUBLE    |         | stmt 准备失败的次数              |
| taos_stmt_is_insert_total                           | DOUBLE    |         | 检查 stmt 是否为插入的总次数         |
| taos_stmt_is_insert_success                         | DOUBLE    |         | 成功检查 stmt 是否为插入的次数        |
| taos_stmt_is_insert_fail                            | DOUBLE    |         | 检查 stmt 是否为插入失败的次数        |
| taos_stmt_set_tbname_total                          | DOUBLE    |         | stmt 设置表名的总次数             |
| taos_stmt_set_tbname_success                        | DOUBLE    |         | stmt 成功设置表名的次数            |
| taos_stmt_set_tbname_fail                           | DOUBLE    |         | stmt 设置表名失败的次数            |
| taos_stmt_set_tags_total                            | DOUBLE    |         | stmt 设置 tag 的总次数          |
| taos_stmt_set_tags_success                          | DOUBLE    |         | stmt 成功设置 tag 的次数         |
| taos_stmt_set_tags_fail                             | DOUBLE    |         | stmt 设置 tag 失败的次数         |
| taos_stmt_bind_param_batch_total                    | DOUBLE    |         | stmt 批量绑定参数的总次数           |
| taos_stmt_bind_param_batch_success                  | DOUBLE    |         | stmt 成功批量绑定参数的次数          |
| taos_stmt_bind_param_batch_fail                     | DOUBLE    |         | stmt 批量绑定参数失败的次数          |
| taos_stmt_add_batch_total                           | DOUBLE    |         | stmt 添加批处理的总次数            |
| taos_stmt_add_batch_success                         | DOUBLE    |         | stmt 成功添加批处理的次数           |
| taos_stmt_add_batch_fail                            | DOUBLE    |         | stmt 添加批处理失败的次数           |
| taos_stmt_execute_total                             | DOUBLE    |         | stmt 执行的总次数               |
| taos_stmt_execute_success                           | DOUBLE    |         | stmt 执行成功的次数              |
| taos_stmt_execute_fail                              | DOUBLE    |         | stmt 执行失败的次数              |
| taos_stmt_num_params_total                          | DOUBLE    |         | stmt 获取参数数量的总次数           |
| taos_stmt_num_params_success                        | DOUBLE    |         | stmt 成功获取参数数量的次数          |
| taos_stmt_num_params_fail                           | DOUBLE    |         | stmt 获取参数数量失败的次数          |
| taos_stmt_get_param_total                           | DOUBLE    |         | stmt 获取参数的总次数             |
| taos_stmt_get_param_success                         | DOUBLE    |         | stmt 成功获取参数的次数            |
| taos_stmt_get_param_fail                            | DOUBLE    |         | stmt 获取参数失败的次数            |
| taos_stmt_errstr_total                              | DOUBLE    |         | stmt 获取 stmt 错误信息的总次数     |
| taos_stmt_errstr_success                            | DOUBLE    |         | stmt 成功获取 stmt 错误信息的次数    |
| taos_stmt_affected_rows_once_total                  | DOUBLE    |         | stmt 获取单次影响行数的总次数         |
| taos_stmt_affected_rows_once_success                | DOUBLE    |         | stmt 成功获取单次影响行数的次数        |
| taos_stmt_use_result_total                          | DOUBLE    |         | stmt 使用结果集的总次数            |
| taos_stmt_use_result_success                        | DOUBLE    |         | stmt 成功使用结果集的次数           |
| taos_stmt_use_result_fail                           | DOUBLE    |         | stmt 使用结果集失败的次数           |
| taos_select_db_total                                | DOUBLE    |         | 选择数据库的总次数                 |
| taos_select_db_success                              | DOUBLE    |         | 成功选择数据库的次数                |
| taos_select_db_fail                                 | DOUBLE    |         | 选择数据库失败的次数                |
| taos_get_tables_vgId_total                          | DOUBLE    |         | 获取表 vgroup ID 的总次数        |
| taos_get_tables_vgId_success                        | DOUBLE    |         | 成功获取表 vgroup ID 的次数       |
| taos_get_tables_vgId_fail                           | DOUBLE    |         | 获取表 vgroup ID 失败的次数       |
| taos_options_connection_total                       | DOUBLE    |         | 设置连接选项的总次数                |
| taos_options_connection_success                     | DOUBLE    |         | 成功设置连接选项的次数               |
| taos_options_connection_fail                        | DOUBLE    |         | 设置连接选项失败的次数               |
| taos_validate_sql_total                             | DOUBLE    |         | 验证 SQL 的总次数               |
| taos_validate_sql_success                           | DOUBLE    |         | 成功验证 SQL 的次数              |
| taos_validate_sql_fail                              | DOUBLE    |         | 验证 SQL 失败的次数              |
| taos_check_server_status_total                      | DOUBLE    |         | 检查服务器状态的总次数               |
| taos_check_server_status_success                    | DOUBLE    |         | 成功检查服务器状态的次数              |
| taos_get_current_db_total                           | DOUBLE    |         | 获取当前数据库的总次数               |
| taos_get_current_db_success                         | DOUBLE    |         | 成功获取当前数据库的次数              |
| taos_get_current_db_fail                            | DOUBLE    |         | 获取当前数据库失败的次数              |
| taos_get_server_info_total                          | DOUBLE    |         | 获取服务器信息的总次数               |
| taos_get_server_info_success                        | DOUBLE    |         | 成功获取服务器信息的次数              |
| taos_options_total                                  | DOUBLE    |         | 设置选项的总次数                  |
| taos_options_success                                | DOUBLE    |         | 成功设置选项的次数                 |
| taos_options_fail                                   | DOUBLE    |         | 设置选项失败的次数                 |
| taos_set_conn_mode_total                            | DOUBLE    |         | 设置连接模式的总次数                |
| taos_set_conn_mode_success                          | DOUBLE    |         | 成功设置连接模式的次数               |
| taos_set_conn_mode_fail                             | DOUBLE    |         | 设置连接模式失败的次数               |
| taos_reset_current_db_total                         | DOUBLE    |         | 重置当前数据库的总次数               |
| taos_reset_current_db_success                       | DOUBLE    |         | 成功重置当前数据库的次数              |
| taos_set_notify_cb_total                            | DOUBLE    |         | 设置通知回调的总次数                |
| taos_set_notify_cb_success                          | DOUBLE    |         | 成功设置通知回调的次数               |
| taos_set_notify_cb_fail                             | DOUBLE    |         | 设置通知回调失败的次数               |
| taos_errno_total                                    | DOUBLE    |         | 获取错误码的总次数                 |
| taos_errno_success                                  | DOUBLE    |         | 成功获取错误码的次数                |
| taos_errstr_total                                   | DOUBLE    |         | 获取错误信息的总次数                |
| taos_errstr_success                                 | DOUBLE    |         | 成功获取错误信息的次数               |
| tmq_consumer_poll_total                             | DOUBLE    |         | tmq 消费者 poll 的总次数         |
| tmq_consumer_poll_success                           | DOUBLE    |         | tmq 消费者 poll 成功的次数        |
| tmq_consumer_poll_fail                              | DOUBLE    |         | tmq 消费者 poll 失败的次数        |
| tmq_subscription_total                              | DOUBLE    |         | tmq 获取订阅信息的总次数            |
| tmq_subscription_success                            | DOUBLE    |         | tmq 成功获取订阅信息的次数           |
| tmq_subscription_fail                               | DOUBLE    |         | tmq 获取订阅信息失败的次数           |
| tmq_list_append_total                               | DOUBLE    |         | tmq 列表追加的总次数              |
| tmq_list_append_success                             | DOUBLE    |         | tmq 成功列表追加的次数             |
| tmq_list_append_fail                                | DOUBLE    |         | tmq 列表追加失败的次数             |
| tmq_list_get_size_total                             | DOUBLE    |         | tmq 获取列表大小的总次数            |
| tmq_list_get_size_success                           | DOUBLE    |         | tmq 成功获取列表大小的次数           |
| tmq_err2str_total                                   | DOUBLE    |         | tmq 错误码转字符串的总次数           |
| tmq_err2str_success                                 | DOUBLE    |         | tmq 成功将错误码转为字符串的次数        |
| tmq_conf_set_total                                  | DOUBLE    |         | tmq 设置配置的总次数              |
| tmq_conf_set_success                                | DOUBLE    |         | tmq 成功设置配置的次数             |
| tmq_conf_set_fail                                   | DOUBLE    |         | tmq 设置配置失败的次数             |
| tmq_get_res_type_total                              | DOUBLE    |         | tmq 获取资源类型的总次数            |
| tmq_get_res_type_success                            | DOUBLE    |         | tmq 成功获取资源类型的次数           |
| tmq_get_topic_name_total                            | DOUBLE    |         | tmq 获取主题名称的总次数            |
| tmq_get_topic_name_success                          | DOUBLE    |         | tmq 成功获取主题名称的次数           |
| tmq_get_vgroup_id_total                             | DOUBLE    |         | tmq 获取 vgroup ID 的总次数     |
| tmq_get_vgroup_id_success                           | DOUBLE    |         | tmq 成功获取 vgroup ID 的次数    |
| tmq_get_vgroup_offset_total                         | DOUBLE    |         | tmq 获取 vgroup 偏移量的总次数     |
| tmq_get_vgroup_offset_success                       | DOUBLE    |         | tmq 成功获取 vgroup 偏移量的次数    |
| tmq_get_db_name_total                               | DOUBLE    |         | tmq 获取数据库名称的总次数           |
| tmq_get_db_name_success                             | DOUBLE    |         | tmq 成功获取数据库名称的次数          |
| tmq_get_table_name_total                            | DOUBLE    |         | tmq 获取表名称的总次数             |
| tmq_get_table_name_success                          | DOUBLE    |         | tmq 成功获取表名称的次数            |
| tmq_get_connect_total                               | DOUBLE    |         | tmq 获取连接的总次数              |
| tmq_get_connect_success                             | DOUBLE    |         | tmq 成功获取连接的次数             |
| tmq_commit_sync_total                               | DOUBLE    |         | tmq 同步提交的总次数              |
| tmq_commit_sync_success                             | DOUBLE    |         | tmq 同步提交成功的次数             |
| tmq_commit_sync_fail                                | DOUBLE    |         | tmq 同步提交失败的次数             |
| tmq_fetch_raw_block_total                           | DOUBLE    |         | tmq 获取原始块的总次数             |
| tmq_fetch_raw_block_success                         | DOUBLE    |         | tmq 成功获取原始块的次数            |
| tmq_fetch_raw_block_fail                            | DOUBLE    |         | tmq 获取原始块失败的次数            |
| tmq_get_topic_assignment_total                      | DOUBLE    |         | tmq 获取主题分配的总次数            |
| tmq_get_topic_assignment_success                    | DOUBLE    |         | tmq 成功获取主题分配的次数           |
| tmq_get_topic_assignment_fail                       | DOUBLE    |         | tmq 获取主题分配失败的次数           |
| tmq_offset_seek_total                               | DOUBLE    |         | tmq 偏移量定位的总次数             |
| tmq_offset_seek_success                             | DOUBLE    |         | tmq 成功偏移量定位的次数            |
| tmq_offset_seek_fail                                | DOUBLE    |         | tmq 偏移量定位失败的次数            |
| tmq_committed_total                                 | DOUBLE    |         | tmq 获取已提交偏移量的总次数          |
| tmq_committed_success                               | DOUBLE    |         | tmq 成功获取已提交偏移量的次数         |
| tmq_commit_offset_sync_fail                         | DOUBLE    |         | tmq 同步提交偏移量失败的次数          |
| tmq_position_total                                  | DOUBLE    |         | tmq 获取当前位置的总次数            |
| tmq_position_success                                | DOUBLE    |         | tmq 成功获取当前位置的次数           |
| tmq_commit_offset_sync_total                        | DOUBLE    |         | tmq 同步提交偏移量的总次数           |
| tmq_commit_offset_sync_success                      | DOUBLE    |         | tmq 同步提交偏移量成功的次数          |
| taos_connect_totp_total                             | DOUBLE    |         | totp 登录次数（3.4.0.0 及以上）    |
| taos_connect_totp_success                           | DOUBLE    |         | totp 登录成功次数（3.4.0.0 及以上）  |
| taos_connect_totp_fail                              | DOUBLE    |         | totp 登录失败次数（3.4.0.0 及以上）  |
| taos_connect_token_total                            | DOUBLE    |         | token 登录次数（3.4.0.0 及以上）   |
| taos_connect_token_success                          | DOUBLE    |         | token 登录成功次数（3.4.0.0 及以上） |
| taos_connect_token_fail                             | DOUBLE    |         | token 登录失败次数（3.4.0.0 及以上） |
| taos_get_connection_info_total                      | DOUBLE    |         | 获取连接信息次数（3.4.0.0 及以上）     |
| taos_get_connection_info_success                    | DOUBLE    |         | 获取连接信息成功次数（3.4.0.0 及以上）   |
| taos_get_connection_info_fail                       | DOUBLE    |         | 获取连接信息失败次数（3.4.0.0 及以上）   |
| endpoint                                            | NCHAR     | TAG     | 请求端点                      |

</details>

从 **3.3.6.29**/**3.3.8.3** 版本开始新增 `adapter_request_limit` 表记录 taosAdapter 查询请求限流数据：

注意：`adapter_request_limit` 表为按需创建，仅在开启查询请求并发限制功能（`request.queryLimitEnable = true`，默认为 `false`）且 taosAdapter 启动后处理过至少一个受限查询请求后，taosKeeper 才会在监控数据库中创建该表并写入数据；未满足上述条件时该表不存在。

<details>
<summary>详细信息</summary>

| field                 | type      | is\_tag | comment                          |
|:----------------------|:----------|:--------|:---------------------------------|
| _ts                   | TIMESTAMP |         | 数据采集时间戳                          |
| query_limit           | DOUBLE    |         | 允许同时执行的查询请求的最大并发数                |
| query_max_wait        | DOUBLE    |         | 查询队列中允许等待执行的最大查询数量               |
| query_inflight        | DOUBLE    |         | 当前正在执行的、受并发限制的查询数量               |
| query_wait_count      | DOUBLE    |         | 当前在队列中等待执行的查询数量                  |
| query_count           | DOUBLE    |         | 本采集周期内收到的、受并发限制的查询请求总数           |
| query_wait_fail_count | DOUBLE    |         | 本采集周期内因等待超时或超过最大等待队列长度而失败的查询请求数量 |
| endpoint              | NCHAR     | TAG     | 请求端点                             |
| user                  | NCHAR     | TAG     | 发起查询请求的认证用户名                     |

</details>

从 **3.4.0.0** 版本开始新增 `adapter_input_json` 表记录 taosAdapter 的 JSON 数据写入监控信息：

<details>
<summary>详细信息</summary>

| field         | type      | is\_tag | comment            |
|:--------------|:----------|:--------|:-------------------|
| _ts           | TIMESTAMP |         | 数据采集时间戳            |
| total_rows    | DOUBLE    |         | JSON 写入请求行数        |
| success_rows  | DOUBLE    |         | 成功写入请求行数           |
| fail_rows     | DOUBLE    |         | 失败写入请求行数           |
| inflight_rows | DOUBLE    |         | 当前正在执行的行数          |
| affected_rows | DOUBLE    |         | 数据库影响行数            |
| url_endpoint  | NCHAR     | TAG     | JSON 写入端点          |
| endpoint      | NCHAR     | TAG     | 请求的 taosAdapter 端点 |

</details>
