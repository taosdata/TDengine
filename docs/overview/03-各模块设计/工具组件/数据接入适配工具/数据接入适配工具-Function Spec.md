# 数据接入适配工具-Function Spec

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-01-03 | 2025-01-03 | 1.0 | 谭雪峰 | 安可送测第一版 |
| 2025-11-28 | 2025-11-28 | 1.1 | 霍琳贺 | 1. 添加监控检查、配置管理 1. 添加安全功能、审计与日志、性能部分详细说明 1. 添加运维详细说明 |

## 2. 背景

taosAdapter 是一个 TDengine 的配套工具，是 TDengine 集群和应用程序之间的桥梁和适配器。它提供了一种易于使用和高效的方式来直接从数据收集代理软件（如 Telegraf、StatsD、collectd 等）摄取数据。它还提供了 InfluxDB/OpenTSDB 兼容的数据摄取接口，允许 InfluxDB/OpenTSDB 应用程序无缝移植到 TDengine。

## 3. 定义

1. **RESTful 接口： **RESTful 接口基于 HTTP 协议，并利用其常见方法（GET、POST、PUT、DELETE 等）来执行操作。
2. **fqdn： **全限定域名是互联网域名系统（DNS）中的一种完整的、唯一的域名表示形式，用于明确标识互联网上的特定主机或服务器。
3. **RFC3339： **RFC 3339 是一种时间和日期格式的标准，旨在规范化地表示时间和日期，以便在计算机系统之间传递和处理。
4. **stmt： **文中指 Prepare Statement 预处理语句。
5. **Schemaless**：无模式写入，允许在不预先定义表结构的情况下动态写入数据，支持 InfluxDB Line Protocol、OpenTSDB JSON 等格式。
6. **TMQ**：TDengine Message Queue，TDengine 的消息队列功能，支持数据订阅和流式处理。
7. **CGO**：Go 语言调用 C 语言代码的机制，taosAdapter 通过 CGO 调用 TDengine 的 C 客户端库（libtaosnative）。
8. **Raw Block**：TDengine 内部数据块的二进制格式，用于高效的数据传输和批量操作。
9. **VGroup**：Virtual Group，TDengine 中的虚拟节点组，用于数据分片和负载均衡。
10. **连接池**：预先创建并维护一组到 TDengine 的连接，避免频繁创建和销毁连接的开销。

## 4. 行为说明

### 4.1 RESTful 接口

可以使用任何支持 http 协议的客户端通过访问 RESTful 接口地址 `http://<fqdn>:6041/rest/sql` 来写入数据到 TDengine 或从 TDengine 中查询数据

#### 4.1.1 请求格式

```go
http://<fqdn>:<port>/rest/sql/[db_name][?tz=timezone[&req_id=req_id][&row_with_meta=true]]
```

参数说明：
- fqdn: 集群中的任一台主机 FQDN 或 IP 地址。
- port: 配置文件中 httpPort 配置项，缺省为 6041。
- db_name: 可选参数，指定本次所执行的 SQL 语句的默认数据库库名。
- tz: 可选参数，指定返回时间的时区，遵照 IANA Time Zone 规则，如 `America/New_York`。
- req_id: 可选参数，指定请求 id，可以用于 tracing。
例如：`http://h1.taos.com:6041/rest/sql/test` 是指向地址为 `h1.taos.com:6041` 的 URL，并将默认使用的数据库库名设置为 `test`。
HTTP 请求的 Header 里需带有身份认证信息，支持 Basic 认证与自定义认证两种机制。
- 自定义身份认证信息如下所示：
```plaintext
Authorization: Taosd <TOKEN>
```

- Basic 身份认证信息如下所示：
```plaintext
Authorization: Basic <TOKEN>
```

HTTP 请求的 BODY 里就是一个完整的 SQL 语句，SQL 语句中的数据表应提供数据库前缀，例如 db_name.tb_name。如果表名不带数据库前缀，又没有在 URL 中指定数据库名的话，系统会返回错误。因为 HTTP 无状态，没有当前 DB 的概念。
使用 `curl` 通过自定义身份认证方式来发起一个 HTTP Request，语法如下：
```bash
curl -L -H "Authorization: Basic <TOKEN>" -d "<SQL>" <ip>:<PORT>/rest/sql/[db_name][?tz=timezone[&req_id=req_id]]
```

或者，
```bash
curl -L -u username:password -d "<SQL>" <ip>:<PORT>/rest/sql/[db_name][?tz=timezone[&req_id=req_id]]
```

其中，`TOKEN` 为 `{username}:{password}` 经过 Base64 编码之后的字符串，例如 `root:taosdata` 编码后为 `cm9vdDp0YW9zZGF0YQ==`。

#### 4.1.2 自定义授权码

HTTP 请求中需要带有授权码 `<TOKEN>`，用于身份识别。可通过发送 `HTTP GET` 请求来获取授权码，操作如下：
```bash
curl http://<fqnd>:<port>/rest/login/<username>/<password>
```

其中，`fqdn` 是 TDengine 数据库的 FQDN 或 IP 地址，`port` 是 TDengine 服务的端口号，`username` 为数据库用户名，`password` 为数据库密码，返回值为 JSON 格式，各字段含义如下：
- code：返回值代码（0 表示成功）。
- desc：授权码（失败时表示错误内容）。
获取授权码示例：
```bash
curl http://192.168.0.1:6041/rest/login/root/taosdata
```

返回值：
```json
{
  "code": 0,
  "desc": "/KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04"
}
```

#### 4.1.3 返回格式

##### 4.1.3.1 HTTP 响应码

默认情况下，taosAdapter 对大多数 C 接口调用出错时也会返回 200 响应码，但是 HTTP body 中包含错误信息。提供配置参数 `httpCodeServerError` 用来设置当 C 接口返回错误时是否返回非 200 的 HTTP 响应码。无论是否设置此参数，响应 body 里都有详细的错误码和错误信息，具体请参考错误 。
当 httpCodeServerError 为 false 时：

| 分类说明 | HTTP 响应码 |
| --- | --- |
| C 接口调用成功 | 200 |
| C 接口调用出错，且不是鉴权错误 | 200 |
| HTTP 请求 URL 参数错误 | 400 |
| C 接口调用鉴权错误 | 401 |
| 接口不存在 | 404 |
| 系统资源不足 | 503 |

当 httpCodeServerError 为 true 时：

| 分类说明 | HTTP 响应码 |
| --- | --- |
| C 接口调用成功 | 200 |
| HTTP 请求 URL 参数错误和 C 接口调用参数解析错误 | 400 |
| C 接口调用鉴权错误 | 401 |
| 接口不存在 | 404 |
| C 接口调用网络不可用错误 | 502 |
| 系统资源不足 | 503 |
| 其他 C 接口调用错误 | 500 |

C 接口参数解析相关错误码：
- TSDB_CODE_TSC_SQL_SYNTAX_ERROR (0x0216)
- TSDB_CODE_TSC_LINE_SYNTAX_ERROR (0x021B)
- TSDB_CODE_PAR_SYNTAX_ERROR (0x2600)
- TSDB_CODE_TDB_TIMESTAMP_OUT_OF_RANGE (0x060B)
- TSDB_CODE_TSC_VALUE_OUT_OF_RANGE (0x0224)
- TSDB_CODE_PAR_INVALID_FILL_TIME_RANGE (0x263B)
C 接口鉴权相关错误码：
- TSDB_CODE_MND_USER_ALREADY_EXIST (0x0350)
- TSDB_CODE_MND_USER_NOT_EXIST (0x0351)
- TSDB_CODE_MND_INVALID_USER_FORMAT (0x0352)
- TSDB_CODE_MND_INVALID_PASS_FORMAT (0x0353)
- TSDB_CODE_MND_NO_USER_FROM_CONN (0x0354)
- TSDB_CODE_MND_TOO_MANY_USERS (0x0355)
- TSDB_CODE_MND_INVALID_ALTER_OPER (0x0356)
- TSDB_CODE_MND_AUTH_FAILURE (0x0357)
C 接口网络不可用相关错误码：
- TSDB_CODE_RPC_NETWORK_UNAVAIL (0x000B)

##### 4.1.3.2 HTTP body 结构

###### 4.1.3.2.1 **正确执行插入**

样例：
```json
{
  "code": 0,
  "column_meta": [["affected_rows", "INT", 4]],
  "data": [[0]],
  "rows": 1
}
```

说明：
- code：（`int`）0 代表成功。
- column_meta：（`[1][3]any`）只返回 `[["affected_rows", "INT", 4]]`。
- rows：（`int`）只返回 `1`。
- data：（`[][]any`）返回受影响行数。

###### 4.1.3.2.2 **正确执行查询**

样例：
```json
{
  "code": 0,
  "column_meta": [
    ["ts", "TIMESTAMP", 8],
    ["count", "BIGINT", 8],
    ["endpoint", "VARCHAR", 45],
    ["status_code", "INT", 4],
    ["client_ip", "VARCHAR", 40],
    ["request_method", "VARCHAR", 15],
    ["request_uri", "VARCHAR", 128]
  ],
  "data": [
    [
      "2022-06-29T05:50:55.401Z",
      2,
      "LAPTOP-NNKFTLTG:6041",
      200,
      "172.23.208.1",
      "POST",
      "/rest/sql"
    ],
    [
      "2022-06-29T05:52:16.603Z",
      1,
      "LAPTOP-NNKFTLTG:6041",
      200,
      "172.23.208.1",
      "POST",
      "/rest/sql"
    ],
    [
      "2022-06-29T06:28:14.118Z",
      1,
      "LAPTOP-NNKFTLTG:6041",
      200,
      "172.23.208.1",
      "POST",
      "/rest/sql"
    ],
    [
      "2022-06-29T05:52:16.603Z",
      2,
      "LAPTOP-NNKFTLTG:6041",
      401,
      "172.23.208.1",
      "POST",
      "/rest/sql"
    ]
  ],
  "rows": 4
}
```

说明：
- code：（`int`）0 代表成功。
- column_meta：（`[][3]any`） 列信息，每个列会用三个值来说明，分别为：列名（string）、列类型（string）、类型长度（int）。
- rows：（`int`）数据返回行数。
- data：（`[][]any`）具体数据内容（时间格式仅支持 RFC3339，结果集为 0 时区，指定 tz 参数时返回为对应时区）。
列类型使用如下字符串：
- "NULL"
- "BOOL"
- "TINYINT"
- "SMALLINT"
- "INT"
- "BIGINT"
- "FLOAT"
- "DOUBLE"
- "VARCHAR"
- "TIMESTAMP"
- "NCHAR"
- "TINYINT UNSIGNED"
- "SMALLINT UNSIGNED"
- "INT UNSIGNED"
- "BIGINT UNSIGNED"
- "JSON"
- "VARBINARY"
- "GEOMETRY"
`VARBINARY` 和 `GEOMETRY` 类型返回数据为 Hex 字符串，样例：
准备数据
```bash
create database demo
use demo
create table t(ts timestamp,c1 varbinary(20),c2 geometry(100))
insert into t values(now,'\x7f8290','point(100 100)')
```

执行查询
```bash
curl --location 'http://<fqdn>:<port>/rest/sql' \
--header 'Content-Type: text/plain' \
--header 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' \
--data 'select * from demo.t'
```

返回结果
```json
{
    "code": 0,
    "column_meta": [
        [
            "ts",
            "TIMESTAMP",
            8
        ],
        [
            "c1",
            "VARBINARY",
            20
        ],
        [
            "c2",
            "GEOMETRY",
            100
        ]
    ],
    "data": [
        [
            "2023-11-01T06:28:15.210Z",
            "7f8290",
            "010100000000000000000059400000000000005940"
        ]
    ],
    "rows": 1
}
```

- `010100000000000000000059400000000000005940` 为 `point(100 100)` 的 [Well-Known Binary (WKB)](https://libgeos.org/specifications/wkb/) 格式

###### 4.1.3.2.3 **错误**

样例：
```json
{
  "code": 9728,
  "desc": "syntax error near \"1\""
}
```

说明：
- code：（`int`）错误码。
- desc：（`string`）错误描述。

### 4.2 WebSocket 接口

#### 4.2.1 查询写入接口

通过 WebSocket 提供 SQL 写入查询、STMT写入查询、schemaless 写入等功能 
Url 地址：
```go
ws://<fqdn>:<port>/ws
```

请求和响应格式分为 JSON 与二进制
JSON 请求格式总如下
```go
type Request struct {
    Action string          `json:"action"`
    Args   json.RawMessage `json:"args"`
}
```

- action 表示请求的行为，支持的行为见下表
- args 表示对应请求的请求体
JSON 协议 action 表

| action | 描述 |
| --- | --- |
| version | 获取客户端版本 |
| conn | 连接 |
| query | 执行 SQL |
| fetch | 获取查询结果 |
| fetch_block | 获取查询结果数据块 |
| free_result | 释放查询结果 |
| insert | schemaless 协议写入 |
| init | stmt 初始化 |
| prepare | stmt 准备语句 |
| set_table_name | stmt 设置表名 |
| set_tags | stmt 设置标签 |
| bind | stmt 绑定 |
| add_batch | stmt 添加批量 |
| exec | stmt 执行 |
| get_tag_fields | stmt 获取需要绑定的标签信息 |
| get_col_fields | stmt 获取需要绑定的列信息 |
| use_result | 获取 stmt 查询结果 |
| stmt_num_params | stmt 需要绑定的参数个数 |
| stmt_get_param | stmt 获取指定绑定列的信息 |
| close | 关闭 stmt |
| num_fields | 获取查询结果的列数目 |
| get_current_db | 获取当前的 db |
| get_server_info | 获取服务端信息 |
| stmt2_init | stmt2 初始化 |
| stmt2_prepare | stmt2 准备语句 |
| stmt2_exec | stmt2 执行 |
| stmt2_result | stmt2 获取结果 |
| stmt2_close | stmt2 关闭 |
| options_connection | 设置连接属性 |
| check_server_status | 检查服务状态 |

二进制请求协议如下

| 偏移 | 描述 | 类型 |
| --- | --- | --- |
| 0 | 请求 id | uint64 |
| 8 | 请求的资源id（查询结果id 或者 stmt id） | uint64 |
| 16 | action（见下表） | uint64 |
| 24 | 请求内容 | []byte |

二进制协议 action 表

| action | 描述 |
| --- | --- |
| 1 | stmt 设置 tag |
| 2 | stmt 绑定 |
| 3 | tmq 消息写入 |
| 4 | 查询结果 raw block 写入 |
| 5 | 查询结果 raw block 带列信息写入 |
| 6 | 二进制 SQL 执行 |
| 7 | 获取查询结果 raw block |
| 9 | stmt2 绑定数据 |
| 10 | 验证 SQL 是否合法 |

##### 4.2.1.1 获取客户端版本

获取 TDengine 客户端版本，请求格式 JSON, action `version`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| version | string | 客户端版本 |

##### 4.2.1.2 连接

创建与 TDengine 连接，请求格式 JSON，action `conn`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| user | string | 连接 TDengine 使用的用户名 |
| password | string | 连接 TDengine 使用的密码 |
| db | string | 连接 TDengine 的数据库（可为空） |
| mode | int | 连接模式（不传不设置） |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求id |
| timing | int64 | 执行时间（纳秒） |
| version | string | 客户端版本 （3.3.6.12 版本以上存在） |

##### 4.2.1.3 执行 sql

执行 SQL ，请求格式 JSON，action `query`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求id |
| sql | string | 要执行的 sql |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| id | uint64 | 查询结果 id |
| is_update | bool | 是否是更新操作 |
| affected_rows | int | 影响行数 |
| fields_count | int | 字段数量 |
| fields_names | []string | 字段名称 |
| fields_types | []uint8 | 字段类型 |
| fields_lengths | []int64 | 字段长度 |
| precision | int | 查询结果时间精度 |
| fields_precisions | []int64 | 字段精度（decimal 类型使用） |
| fields_scales | []int64 | 小数位数（decimal 类型使用） |

##### 4.2.1.4 获取查询结果

获取查询结果 ，用来确定是否有新的数据块，请求格式 JSON，action `fetch`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| id | uint64 | 查询结果 id |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| id | uint64 | 查询结果 id |
| completed | bool | 查询完成 |
| lengths | []int | 各字段长度（已弃用） |
| rows | int | 结果数据块行数 |

##### 4.2.1.5 获取查询结果数据块

获取查询结果数据块 ，请求格式 JSON，action `fetch_block`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| id | uint64 | 查询结果 id |

响应：
格式二进制，响应内容如下

| 偏移 | 类型 | 说明 |
| --- | --- | --- |
| 0 | uint64 | 执行时间（纳秒） |
| 8 | uint64 | 查询结果 id |
| 16 | []byte | raw block 结果块 |

raw block 格式为 C 接口 `taos_get_raw_block` 返回，格式如下
```go
// +------------------+--------------+--------------+------------------+-----------------+-------------------+--------------------------------------------+------------------------------------+-------------+-----------+-------------+-----------+
// |  version         | total length | total rows    |  total columns  |   flag  seg     |  group id         | col1_schema(type+bytes) | col2_schema(type+bytes) | col3_schema(type+bytes)... | column#1 length, column#2 length...| col1 bitmap or col1 offset | col1 data | col2 bitmap or col2 offset  | col2 data | ....
// |  sizeof(int32_t) |sizeof(int32) | sizeof(int32) |  sizeof(int32)  |  sizeof(int32)  |  sizeof(uint64_t) |           (sizeof(int8_t)+sizeof(int32_t))*numOfCols                           | sizeof(int32_t) * numOfCols        | 
// +------------------+--------------+--------------+------------------+-----------------+-------------------+------+------------------------------------+-------------+-----------+-------------+-----------+
```

具体描述如下：
- 第一个字段：版本号，固定大小，可忽略，占用4个字节
- 第二个字段：raw block 数据的总长度，占用4个字节
- 第三个字段：总行数，占用4个字节
- 第四个字段：总列数，占用4个字节
- 第五个字段：flag，固定大小，可忽略，占用4个字节
- 第六个字段：group id，block分组的id，可忽略，占用8个字节 
- 第七个字段：所有列的 schema，每个列包含类型（1个字节）+所需大小（4个字节）
- 第八个字段：每列数据长度
- 第九个字段：
  - 每列数据内容，具体分变长的string类型和固定长度的类型。
  - 变长的类型，通过前面每行的offset来标记位置，offset=-1，表示该行为NULL，变长数据前两字节为长度，后面为真实数据。
  - 固定长度的类型，通过bitmap来标记，bit位为1表示该行为NULL，根据固定长度获取真实数据（比如int32类型占4个字节固定长度）。

##### 4.2.1.6 释放查询结果

释放查询结果 ，请求格式 JSON，action `free_result`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| id | uint64 | 查询结果 id |

无响应

##### 4.2.1.7 schemaless 写入

Schemaless 写入，请求格式 JSON, action `insert`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| protocol | int | 协议类型（见协议列表） |
| precision | string | 时间精度（见时间精度列表） |
| ttl | int | 表过期时间 |
| data | string | Schemaless 数据 |

协议列表

| 协议 | 值 |
| --- | --- |
| influxdb | 1 |
| openTSDB 行数据 | 2 |
| openTSDB JSON | 3 |

时间精度列表

| 精度 | 值 | C 枚举 |
| --- | --- | --- |
| 纳秒 | ns | 6 |
| 微秒 | u 或 μ | 5 |
| 毫秒 | ms | 4 |
| 秒 | s | 3 |
| 分钟 | m | 2 |
| 小时 | h | 1 |
| 不设置 | 空字符串 | 0 |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |

##### 4.2.1.8 stmt 初始化

预编译语句初始化，请求格式 JSON, action `init`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| stmt_id | uint64 | stmt id |

##### 4.2.1.9 stmt 准备语句

预编译语句准备 sql 语句，请求格式 JSON, action `prepare`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| stmt_id | uint64 | stmt id |
| sql | string | 准备的 sql 语句 |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| stmt_id | uint64 | stmt id |
| is_insert | bool | 是否是写入语句 |

##### 4.2.1.10 stmt 设置表名

预编译语句设置表名，请求格式 JSON, action `set_table_name`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| stmt_id | uint64 | stmt id |
| name | string | 表名 |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| stmt_id | uint64 | stmt id |

##### 4.2.1.11 stmt 设置标签

预编译语句设置标签，请求格式 JSON, action  `set_tags`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| stmt_id | uint64 | stmt id |
| tags | JSON ARRAY | 按行组织的绑定 tag |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| stmt_id | uint64 | stmt id |

##### 4.2.1.12 stmt 绑定

预编译语句绑定数据，请求格式 JSON, action  `bind`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| stmt_id | uint64 | stmt id |
| bind | JSON ARRAY | 按列组织的绑定的多行绑定数据 |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| stmt_id | uint64 | stmt id |

##### 4.2.1.13 stmt 添加批量

预编译语句添加批量，请求格式 JSON, action  `add_batch`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| stmt_id | uint64 | stmt id |
| bind | JSON ARRAY | 按列组织的绑定的多行绑定数据 |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| stmt_id | uint64 | stmt id |

##### 4.2.1.14 stmt 执行

执行预编译语句，请求格式 JSON, action  `exec`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| stmt_id | uint64 | stmt id |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| stmt_id | uint64 | stmt id |
| affected | int | 影响行数 |

##### 4.2.1.15 stmt 获取需要绑定的标签信息

预编译语句获取需要绑定的标签信息，请求格式 JSON, action  `get_tag_fields`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| stmt_id | uint64 | stmt id |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| stmt_id | uint64 | stmt id |
| fields | []field | 字段信息列表，field 结构见下表 |

字段信息

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| name | string | 名称 |
| field_type | int8 | 类型 |
| precision | uint8 | 精度 |
| scale | uint8 | 有效位数(暂未使用) |
| bytes | int32 | 大小 |

##### 4.2.1.16 stmt 获取需要绑定的列信息

预编译语句获取需要绑定的列信息，请求格式 JSON, action  `get_col_fields`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| stmt_id | uint64 | stmt id |

响应：
与获取标签相同，格式 JSON，响应如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| stmt_id | uint64 | stmt id |
| fields | []field | 字段信息列表，field 结构见下表 |

字段信息

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| name | string | 名称 |
| field_type | int8 | 类型 |
| precision | uint8 | 精度 |
| scale | uint8 | 有效位数(暂未使用) |
| bytes | int32 | 大小 |

##### 4.2.1.17 获取 stmt 查询结果

预编译语句为查询时，执行之后获取查询结果 id，请求格式 JSON, action  `use_result`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| stmt_id | uint64 | stmt id |

响应：
返回内容与 4.2.1.3 执行 sql  类似，格式 JSON，响应如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| stmt_id | uint64 | stmt id |
| result_id | uint64 | 查询结果 id |
| fields_count | int | 字段数量 |
| fields_names | []string | 字段名称 |
| fields_types | []uint8 | 字段类型 |
| fields_lengths | []int64 | 字段长度 |
| precision | int | 时间精度 |
| fields_precisions | []int64 | 字段精度（decimal 类型使用） |
| fields_scales | []int64 | 小数位数（decimal 类型使用） |

获取到 result_id 之后与 sql 查询结果一样方式获取数据

##### 4.2.1.18 stmt 需要绑定的参数个数

预编译语句需要绑定的参数个数，请求格式 JSON, action  `stmt_num_params`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| stmt_id | uint64 | stmt id |

响应：
格式 JSON，响应如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| stmt_id | uint64 | stmt id |
| num_params | int | 需要绑定的参数个数 |

##### 4.2.1.19 stmt 获取指定绑定列的信息

预编译语句获取指定绑定列的信息，请求格式 JSON, action  `stmt_get_param`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| stmt_id | uint64 | stmt id |
| index | int | 需要绑定字段序号 |

响应：
格式 JSON，响应如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| stmt_id | uint64 | stmt id |
| index | int | 请求的序号 |
| data_type | int | 类型 |
| length | int | 长度 |

##### 4.2.1.20 关闭 stmt

关闭预编译语句，请求格式 JSON, action  `close`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| stmt_id | uint64 | stmt id |

成功无响应

##### 4.2.1.21 获取查询结果字段数

获取查询结果字段数，请求格式 JSON, action  `num_fields`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| result_id | uint64 | 查询结果 id |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| num_fields | int | 字段数量 |

##### 4.2.1.22 获取当前的 db

获取当前的 db，请求格式 JSON, action  `get_current_db`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| db | string | 当前 DB |

##### 4.2.1.23 stmt2 初始化

预编译语句初始化，请求格式 JSON, action `stmt2_init`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| single_stb_insert | bool | 单个超级表数据绑定 |
| single_table_bind_once | bool | 一个表只绑定一次 |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| stmt_id | uint64 | stmt2 id |

##### 4.2.1.24 stmt2 准备语句

预编译语句准备 sql 语句，请求格式 JSON, action `stmt2_prepare`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| stmt_id | uint64 | stmt2 id |
| sql | string | 准备的 sql 语句 |
| get_fields | bool | 是否获取语句的绑定信息 |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| stmt_id | uint64 | stmt2 id |
| is_insert | bool | 是否是写入语句 |
| fields | []Stmt2AllField | 绑定字段信息（Stmt2AllField 见下表） |
| fields_count | int | 绑定字段数量 |


| 参数 | 类型 | 说明 |
| --- | --- | --- |
| name | string | 名称 |
| field_type | int8 | 类型 |
| precision | uint8 | 精度 |
| scale | uint8 | 小数位 |
| bytes | int32 | 大小 |
| bind_type | int8 | 绑定类型 |

##### 4.2.1.25 stmt2 执行

执行预编译语句，请求格式 JSON, action  `stmt2_exec`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| stmt_id | uint64 | stmt2 id |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| stmt_id | uint64 | stmt2 id |
| affected | int | 影响行数 |

##### 4.2.1.26 获取 stmt2 查询结果

预编译语句为查询时，执行之后获取查询结果 id，请求格式 JSON, action  `stmt2_result`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| stmt_id | uint64 | stmt2 id |

响应：
返回内容与 4.2.1.3 执行 sql  类似，格式 JSON，响应如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| stmt_id | uint64 | stmt2 id |
| result_id | uint64 | 查询结果 id |
| fields_count | int | 字段数量 |
| fields_names | []string | 字段名称 |
| fields_types | []uint8 | 字段类型 |
| fields_lengths | []int64 | 字段长度 |
| precision | int | 时间精度 |
| fields_precisions | []int64 | 字段精度（decimal 类型使用） |
| fields_scales | []int64 | 小数位数（decimal 类型使用） |

获取到 result_id 之后与 sql 查询结果一样方式获取数据

##### 4.2.1.27 关闭 stmt2

关闭预编译语句，请求格式 JSON, action  `stmt2_close`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| stmt_id | uint64 | stmt2 id |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| stmt_id | uint64 | stmt2 id |

##### 4.2.1.28 获取服务端信息

获取当前的 db，请求格式 JSON, action  `get_server_info`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| info | string | 服务端信息 |

##### 4.2.1.29 设置连接属性

设置连接属性，请求格式 JSON, action  `options_connection`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| options | []option | 属性列表（见下表） |


| 参数 | 类型 | 说明 |
| --- | --- | --- |
| option | int | 属性 id |
| value | *string | 属性值，传 null 为清空 |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |

##### 4.2.1.30 检查服务状态

检查服务状态，请求格式 JSON, action  `check_server_status`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| fqdn | *string | 地址 |
| port | int32 | 端口 |


| 参数 | 类型 | 说明 |
| --- | --- | --- |
| option | int | 属性 id |
| value | *string | 属性值，传 null 为清空 |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| status | int32 | 状态 |
| details | string | 描述 |

##### 4.2.1.31 二进制协议 stmt 设置标签

使用 raw block 格式绑定标签
请求

| 偏移 | 描述 | 类型 |
| --- | --- | --- |
| 0 | 请求 id | uint64 |
| 8 | stmt_id | uint64 |
| 16 | action 固定值 1 | uint64 |
| 24 | rawblock （格式见 4.2.1.5 获取查询结果数据块， version 为 1） | []byte |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 固定值 set_tags |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| stmt_id | uint64 | stmt id |

##### 4.2.1.32 二进制协议 stmt 绑定

使用 raw block 格式绑定
请求

| 偏移 | 描述 | 类型 |
| --- | --- | --- |
| 0 | 请求 id | uint64 |
| 8 | stmt_id | uint64 |
| 16 | action 固定值 2 | uint64 |
| 24 | rawblock （格式见 4.2.1.5 获取查询结果数据块， version 为 1） | []byte |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 固定值 bind |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| stmt_id | uint64 | stmt id |

##### 4.2.1.33 二进制协议 tmq 消息写入

写入 tmq 订阅到的原始数据
请求

| 偏移 | 描述 | 类型 |
| --- | --- | --- |
| 0 | 请求 id | uint64 |
| 8 | 固定值 0 | uint64 |
| 16 | action 固定值 3 | uint64 |
| 24 | 原始数据长度 | uint32 |
| 28 | 消息类型 | uint16 |
| 30 | tmq 原始数据 | []byte |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 固定值 write_raw |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |

##### 4.2.1.34 二进制协议查询结果 raw block 写入

写入查询到的 raw block 结果到指定表
请求

| 偏移 | 描述 | 类型 |
| --- | --- | --- |
| 0 | 请求 id | uint64 |
| 8 | 固定值 0 | uint64 |
| 16 | action 固定值 4 | uint64 |
| 24 | Block 包含的行数 | int32 |
| 28 | table_length 要写入的表名长度 | uint16 |
| 30 | 根据 table_length 获取表名 | []byte |
| 30 + table_length | rawblock | []byte |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 固定值 write_raw_block |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |

##### 4.2.1.35 二进制协议查询结果 raw block 带列信息写入

写入查询到的 raw block 结果并携带列信息到指定表
请求

| 偏移 | 描述 | 类型 |
| --- | --- | --- |
| 0 | 请求 id | uint64 |
| 8 | 固定值 0 | uint64 |
| 16 | action 固定值 5 | uint64 |
| 24 | Block 包含的行数 | int32 |
| 28 | table_length 要写入的表名长度 | uint16 |
| 30 | 根据 table_length 获取表名 | []byte |
| 30 + table_length | rawblock | []byte |
| 30 + table_length + rawblock_length | field 信息格式如下 typedef struct taosField { char name[65]; int8_t type; int32_t bytes; } TAOS_FIELD; 内存分布为 name 65 byte type 1 byte padding 2 byte（对齐） bytes 4 byte （rawblock_length 从 rawblock 获取） | []byte |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 固定值 write_raw_block_with_fields |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |

##### 4.2.1.36 二进制协议执行 SQL

二进制协议执行 SQL
请求

| 偏移 | 描述 | 类型 |
| --- | --- | --- |
| 0 | 请求 id | uint64 |
| 8 | 固定值 0 | uint64 |
| 16 | action 固定值 6 | uint64 |
| 24 | version 目前支持 1 | uint16 |
| 26 | SQL 长度 | uint32 |
| 30 | SQL 内容 | []byte |

响应：返回内容与 4.2.1.3 执行 sql 格式相同 action 变为 `binary_query`

##### 4.2.1.37 二进制协议获取查询结果 raw block

获取查询结果 raw block
请求

| 偏移 | 描述 | 类型 |
| --- | --- | --- |
| 0 | 请求 id | uint64 |
| 8 | 查询返回的结果 id | uint64 |
| 16 | action 固定值 7 | uint64 |
| 24 | version 目前支持 1 | uint16 |

响应为二进制

| 序号 | 名称 | 类型 | 字节数 | 备注 |
| --- | --- | --- | --- | --- |
| 1 | Time | uint64 | 8 | 新格式固定为0xffffffff,用来做标志位和兼容 |
| 2 | Action | uint64 | 8 | 固定值 7 |
| 3 | Version | uint16 | 2 | 版本 1 |
| 4 | Time | uint64 | 8 | 执行时间，单位 ns |
| 5 | ReqID | uint64 | 8 | 请求 id |
| 6 | Code | uint32 | 4 | 错误码 |
| 7 | MessageLen | uint32 | 4 | 当 Code = 0 时 MessageLen = 0 |
| 8 | Message | string | MessageLen | 错误内容 |
| 9 | ResultID | uint64 | 8 | 查询结果 id |
| 10 | Finished | uint8 | 1 | 0 代表有block，1代表无block,任何情况都返回 |
| 11 | BlockLen | uint32 | 4 | Block 长度 |
| 12 | Block | byte[] | BlockLen | Block 内容 |

##### 4.2.1.38 二进制协议 stmt2 绑定数据

stmt2 绑定数据
请求
固定 header

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| stmt_id | uint64 | stmt实例id |
| action | uint64 | 固定值 9 |
| version | uint16 | 协议版本 1 |
| col_idx | int32 | 列号，全绑定传 -1 |

数据内容

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| TotalLength | uint32 | 数据内容的全部长度,包括 TotalLength 字段长度 |
| TableCount | int32 | 多少个数据（对应 TAOS_STMT2_BINDV 中 count） |
| TagCount | int32 | 需要绑定多少个标签（sql 中 tag 有多少个问号） |
| ColCount | int32 | 需要绑定多少个列（sql 中列有多少个问号） |
| TableNamesOffset | uint32 | 表名的偏移量（基于数据内容开始）,如果没有则为 0 |
| TagsOffset | uint32 | tag 的偏移量，如果没有 tag 则为 0 |
| ColsOffset | uint32 | 列的偏移量，如果没有列则为 0 |
| TableNameLength | [tableCount]uint16 | 每个表名的长度包含'\0' 长度，最大长度 db+tbname+\0 = 64 + 192 + 1 = 257，如果 TableNamesOffset 为 0 则没有此项 |
| TableNameBuffer | []byte | 表名的二进制数据包含'\0'，如果 TableNamesOffset 为 0 则没有此项 |
| TagsDataLength | [tableCount]uint32 | 标签数据的长度, Count 个元素, 每个元素为一张表 tag 数据的长度，如果某个表不绑定 tag 则对应的 length 为 0 ，如果 TagsOffset 为 0 则没有此项 |
| TagsBuffer | []byte | 标签数据，格式见下表，如果 TagsOffset 为 0 则没有此项 |
| ColDataLength | [tableCount]uint32 | 列数据的长度, Count 个元素, 每个元素为一张表列数据的长度，如果某个表不绑定列则对应的 length 为 0 ，如果 ColsOffset 为 0 则没有此项 |
| ColBuffer | []byte | 列数据，格式见下表，如果 ColsOffset 为 0 则没有此项 |

列和标签数据格式

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| TotalLength | uint32 | 当前 数据的全部长度,包括 TotalLength 字段长度 |
| Type | int32 | 数据类型（对应TAOS_STMT2_BIND 中 buffer_type） |
| Num | int32 | 多少行数据（对应TAOS_STMT2_BIND 中 num） |
| IsNull | [Num]byte | 每个 tag 是否为 null, Num 个元素（对应TAOS_STMT2_BIND 中 is_null） |
| haveLength | byte | 是否有长度，0 为没有，1 为有，当数据类型为变长时必须有长度（binary, nchar, json, varbinary, varchar） |
| Length | [Num]int32 | 每个数据长度, Num 个元素，当 hasLength 为 0 时，无该字段（对应TAOS_STMT2_BIND 中 length） |
| BufferLength | uint32 | Buffer 的长度，如果数据全为 null 则 bufferLength可以为 0 并且 Buffer 是空的 |
| Buffer | [BufferLength]byte | 绑定数据（对应TAOS_STMT2_BIND 中 buffer）如果数据不是变长类型则需要留出空数据的位置（与 Native 规则一致） |

响应为 json

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码 |
| message | string | 错误信息 |
| action | string | stmt2_bind |
| req_id | uint64 | 请求ID |
| timing | int64 | 执行时间 |
| stmt_id | uint64 | stmt2 实例 ID |

##### 4.2.1.39 二进制协议验证 SQL 是否合法

验证 SQL 是否合法
请求

| 偏移 | 描述 | 类型 |
| --- | --- | --- |
| 0 | 请求 id | uint64 |
| 8 | 固定值 0 | uint64 |
| 16 | action 固定值 10 | uint64 |
| 24 | version 目前支持 1 | uint16 |
| 26 | SQL 长度 | uint32 |
| 30 | SQL 内容 | []byte |

响应为 json

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码 |
| message | string | 错误信息 |
| action | string | stmt2_bind |
| req_id | uint64 | 请求ID |
| timing | int64 | 执行时间 |
| result_code | int64 | 结果码 |

#### 4.2.2 TMQ 订阅接口

通过 WebSocket 提供 TMQ 订阅接口
Url 地址：
```go
ws://<fqdn>:<port>/rest/tmq
```

请求和响应格式分为 JSON 与二进制
JSON 请求格式总如下
```go
type Request struct {
    Action string          `json:"action"`
    Args   json.RawMessage `json:"args"`
}
```

- action 表示请求的行为，支持的行为见下表
- args 表示对应请求的请求体
JSON 协议 action 表

| action | 描述 |
| --- | --- |
| version | 获取客户端版本 |
| subscribe | 订阅 |
| poll | 拉取消息 |
| fetch | 获取数据结果 |
| fetch_block | 获取结果数据块 |
| fetch_raw | 获取消息原始数据 |
| fetch_json_meta | 获取json元数据 |
| commit | 提交消息 |
| unsubscribe | 取消订阅 |
| assignment | 获取分配信息 |
| seek | 设置偏移量 |
| commit_offset | 提交偏移量 |
| committed | 获取已提交偏移量 |
| position | 获取当前位置 |
| list_topics | 获取订阅的主题 |
| fetch_raw_data | 获取消息原始数据新格式 |

目前无二进制请求

##### 4.2.2.1 获取客户端版本

获取 TDengine 客户端版本，请求格式 JSON, action `version`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id （3.3.6.12 及以上） |

响应：
格式 JSON，响应内容如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id （3.3.6.12 及以上） |
| version | string | 客户端版本 |

##### 4.2.2.2 订阅

创建订阅，请求格式 JSON, action  `subscribe`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| user | string | 连接 TDengine 用户名 |
| password | string | 连接 TDengine 密码 |
| db | string | 连接 TDengine 数据库 |
| group_id | string | 订阅组 id |
| client_id | string | 客户端 id |
| offset_reset | string | 消费组订阅的初始位置 |
| topics | []string | 订阅主题 |
| auto_commit | string | 是否启用消费位点自动提交 |
| auto_commit_interval_ms | string | 消费记录自动提交消费位点时间间隔，单位为毫秒 |
| snapshot_enable | string | 是否从 tsdb 订阅数据 |
| with_table_name | string | 是否允许从消息中解析表名 |

响应：
格式 JSON，响应如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| version | string | 客户端版本 （3.3.6.12 版本以上存在） |

##### 4.2.2.3 拉取消息

拉取消息，请求格式 JSON, action  `poll`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| blocking_time | int64 | 等待消息时间（毫秒） |

响应：
格式 JSON，响应如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| have_message | bool | 是否有消息 |
| topic | string | 消息的主题 |
| database | string | 消息所属数据库 |
| vgroup_id | int32 | 消息来源 vgroup |
| message_type | int32 | 消息类型 1 表示数据，2表示元数据，3表示数据和元数据 |
| message_id | uint64 | 消息 id |
| offset | int64 | 消息的偏移量 |

##### 4.2.2.4 获取数据结果

获取消息内的数据，请求格式 JSON, action  `fetch`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| message_id | uint64 | 消息 id |

响应：
格式 JSON，响应如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| message_id | uint64 | 消息 id |
| completed | bool | 数据是否全部获取完成 |
| table_name | string | 数据所属表名 |
| rows | int | 数据块包含的行数 |
| fields_count | int | 字段数量 |
| fields_names | []string | 字段名称 |
| fields_types | []uint8 | 字段类型 |
| fields_lengths | []int64 | 字段长度 |
| precision | int | 结果时间类型精度 |

##### 4.2.2.5 获取结果数据块

获取消息内的数据，请求格式 JSON, action  `fetch_block`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id，需要与 `fetch` 请求相同 |
| message_id | uint64 | 消息 id |

响应：
失败返回 JSON

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| message_id | uint64 | 消息 id |

成功返回二进制

| 偏移 | 描述 | 类型 |
| --- | --- | --- |
| 0 | 固定值 0 | uint64 |
| 8 | 请求 id | uint64 |
| 16 | 消息 id | uint64 |
| 24 | rawblock （格式见 4.2.1.5 获取查询结果数据块， version 为 1） | []byte |

##### 4.2.2.6 获取消息原始数据

获取消息原始数据，请求格式 JSON, action  `fetch_raw`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id，需要与 `fetch` 请求相同 |
| message_id | uint64 | 消息 id |

响应：
失败返回 JSON

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| message_id | uint64 | 消息 id |

成功返回二进制

| 偏移 | 描述 | 类型 |
| --- | --- | --- |
| 0 | 执行时间（纳秒） | uint64 |
| 8 | 请求 id | uint64 |
| 16 | 消息 id | uint64 |
| 24 | action 固定值 3 | uint64 |
| 32 | Block 长度 | uint32 |
| 36 | 消息类型（内部类型） | uint16 |
| 38 | tmq raw block | []byte |

##### 4.2.2.7 获取 json 格式元数据

获取消息内元数据，请求格式 JSON, action  `fetch_json_meta`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| message_id | uint64 | 消息 id |

响应：
格式 JSON，响应如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| message_id | uint64 | 消息 id |
| data | JSON | JSON 格式元数据 |

##### 4.2.2.8 提交消息

提交全部已消费消息，请求格式 JSON, action  `commit`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| message_id | uint64 | 消息 id（未使用，保留兼容性） |

响应：
格式 JSON，响应如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| message_id | uint64 | 消息 id |

##### 4.2.2.9 取消订阅

取消订阅，请求格式 JSON, action  `unsubscribe`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |

响应：
格式 JSON，响应如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |

##### 4.2.2.10 获取分配信息

获取指定主题在当前消费者的分配信息，请求格式 JSON, action  assignment
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| topic | string | 主题 |

响应：
格式 JSON，响应如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| assignment | []assignment_info | 分配信息（字段见下表） |

assignment_info 定义如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| vgroup_id | int32 | 分配的 vgroup id |
| offset | int32 | 偏移量 |
| begin | int32 | 起始值 |
| end | int32 | 结束值 |

##### 4.2.2.11 设置偏移量

设置当前消费者指定主题和分区的偏移量，请求格式 JSON, action  `seek`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| topic | string | 主题 |
| vgroup_id | int32 | vgroup id |
| offset | int64 | 偏移量 |

响应：
格式 JSON，响应如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |

##### 4.2.2.12 提交偏移量

提交当前消费者指定主题和分区的偏移量，请求格式 JSON, action  `commit_offset`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| topic | string | 主题 |
| vgroup_id | int32 | vgroup id |
| offset | int64 | 偏移量 |

响应：
格式 JSON，响应如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| topic | string | 主题 |
| vgroup_id | int32 | vgroup id |
| offset | int64 | 偏移量 |

##### 4.2.2.13 获取已提交的偏移量

获取当前消费者指定主题和分区已提交的偏移量，请求格式 JSON, action  `committed`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| topic_vgroup_ids | []topic_vgroups | 分区信息结构见下表 |
| vgroup_id | int32 | vgroup id |
| offset | int64 | 偏移量 |

topic_vgroups 定义如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| topic | string | 主题 |
| vgroup_id | int32 | vgroup id |

响应：
格式 JSON，响应如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| committed | []int64 | 各分区已提交位置 |

##### 4.2.2.14 获取当前消费位置

获取当前消费者指定主题和分区消费位置，请求格式 JSON, action  `position`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| topic_vgroup_ids | []topic_vgroups | 分区信息结构见下表 |

topic_vgroups 定义如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| topic | string | 主题 |
| vgroup_id | int32 | vgroup id |

响应：
格式 JSON，响应如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| position | []int64 | 各分区消费位置 |

##### 4.2.2.15 获取订阅的主题

获取当前消费者已经订阅的主题，请求格式 JSON, action  `list_topics`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |

响应：
格式 JSON，响应如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| code | int | 错误码(0 表示无错误) |
| message | string | 错误信息 |
| action | string | 请求的 action |
| req_id | uint64 | 请求 id |
| timing | int64 | 执行时间（纳秒） |
| topics | []string | 已订阅的主题 |

##### 4.2.2.16 获取消息原始数据新格式

获取消息原始数据新格式，请求格式 JSON, action  `fetch_raw_data`
请求体（args）如下

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| req_id | uint64 | 请求 id |
| message_id | uint64 | 消息 id |

响应：
二进制格式

| 序号 | 名称 | 类型 | 字节数 | 备注 |
| --- | --- | --- | --- | --- |
| 1 | Time | uint64 | 8 | 新格式固定为0xffffffff,用来做标志位和兼容 |
| 2 | Action | uint64 | 8 | Fetch Raw 响应值为 8 |
| 3 | Version | uint16 | 2 | 1 |
| 4 | Time | uint64 | 8 | 执行时间，单位 ns |
| 5 | ReqID | uint64 | 8 | 请求 id |
| 6 | Code | uint32 | 4 | 错误码 |
| 7 | MessageLen | uint32 | 4 | 当 Code = 0 时 MessageLen = 0 |
| 8 | Message | string | MessageLen | 错误内容 |
| 9 | MessageID | uint64 | 8 | 消息 id |
| 10 | MetaType | uint16 | 2 | 元数据类型 |
| 11 | RawBlockLength | uint32 | 4 | raw block 长度 |
| 12 | TMQRawBlock | byte[] | RawBlockLength | raw block 内容 |

### 4.3 数据收集软件接入

#### 4.3.1 InfluxDB 协议

兼容 InfluxDB v1 写接口，可以使用任何支持 http 协议的客户端访问 Restful 接口地址 `http://<fqdn>:6041/influxdb/v1/write` 来写入 InfluxDB 兼容格式的数据到 TDengine。
支持 InfluxDB 参数如下：
- `db` 指定 TDengine 使用的数据库名，需要提前创建
- `precision` TDengine 使用的时间精度
- `u` TDengine 用户名
- `p` TDengine 密码
- `ttl` 自动创建的子表生命周期，以子表的第一条数据的 TTL 参数为准，不可更新。
目前不支持 InfluxDB 的 token 验证方式，仅支持 Basic 验证和查询参数验证。
示例： 
```bash {wrap}
curl --request POST http://127.0.0.1:6041/influxdb/v1/write?db=test --user "root:taosdata" --data-binary "measurement,host=host1 field1=2i,field2=2.0 1577836800000000000"
```

#### 4.3.2 OpenTSDB

##### 4.3.2.1 HTTP 写入

可以使用任何支持 http 协议的客户端访问 Restful 接口地址 `http://<fqdn>:6041/<APIEndPoint>` 来写入 OpenTSDB 兼容格式的数据到 TDengine，支持 Basic 验证。EndPoint 如下：
- JSON 格式写入 `/opentsdb/v1/put/json/<db>`
示例：
```bash {wrap}
curl --location 'http://127.0.0.1:6041/opentsdb/v1/put/json/sml' \
--user "root:taosdata" \
--data '{
    "metric": "sys.cpu.nice",
    "timestamp": 1725525769,
    "value": 123,
    "tags": {
       "host": "web01",
       "dc": "lga"
    }
}'
```

- telnet 格式写入 `/opentsdb/v1/put/telnet/<db>`
示例：
```bash {wrap}
curl --location 'http://127.0.0.1:6041/opentsdb/v1/put/telnet/sml' --user "root:taosdata" --data 'put metric 1725525769 123 host=web01 interface=eth0'
```

##### 4.3.2.2 TCP 写入

在 taosAdapter 配置文件（默认位置为 /etc/taos/taosadapter.toml）中使能配置项
```plaintext
[opentsdb_telnet]
enable = true
dbs = ["opentsdb_telnet", "collectd", "icinga2", "tcollector"]
ports = [6046, 6047, 6048, 6049]
user = "root"
password = "taosdata"
...
```

- `dbs` 与 `ports` 表示端口与写入的 db 的对应，db 需提前创建
- `user` 表示连接 TDengine 的用户名
- `password` 表示连接 TDengine 的密码
`collectd` 、`icinga2` 、`tcollector` 等支持 openTSDB tcp 写入的采集软件可通过修改对应软件的配置进行接入

#### 4.3.3 Statsd

在 taosAdapter 配置文件（默认位置 /etc/taos/taosadapter.toml）中使能配置项
```plaintext
...
[statsd]
enable = true
port = 6044
db = "statsd"
user = "root"
password = "taosdata"
worker = 10
gatherInterval = "5s"
protocol = "udp"
maxTCPConnections = 250
tcpKeepAlive = false
allowPendingMessages = 50000
deleteCounters = true
deleteGauges = true
deleteSets = true
deleteTimings = true
...
```

其中 taosAdapter 默认写入的数据库名称为 `statsd`，也可以修改 taosAdapter 配置文件 db 项来指定不同的名称。user 和 password 填写实际 TDengine 配置的值。

#### 4.3.4 Prometheus

配置 Prometheus 是通过编辑 Prometheus 配置文件 prometheus.yml （默认位置 /etc/prometheus/prometheus.yml）完成的。

配置第三方数据库地址
将其中的 remote_read url 和 remote_write url 指向运行 taosAdapter 服务的服务器域名或 IP 地址，REST 服务端口（taosAdapter 默认使用 6041），以及希望写入 TDengine 的数据库名称，并确保相应的 URL 形式如下：
- remote_read url : `http://<taosAdapter's host>:<REST service port>/prometheus/v1/remote_read/<database name>`
- remote_write url : `http://<taosAdapter's host>:<REST service port>/prometheus/v1/remote_write/<database name>`
示例如下
```bash {wrap}
remote_write:
  - url: "http://localhost:6041/prometheus/v1/remote_write/prometheus_data"
    basic_auth:
      username: root
      password: taosdata

remote_read:
  - url: "http://localhost:6041/prometheus/v1/remote_read/prometheus_data"
    basic_auth:
      username: root
      password: taosdata
    remote_timeout: 10s
    read_recent: true
```

#### 4.3.5 node_exporter

Prometheus 使用的由 *NIX 内核暴露的硬件和操作系统指标的输出器
启用 taosAdapter 的配置 node_exporter.enable
```bash {wrap}
[node_exporter]
enable = false
db = "node_exporter"
user = "root"
password = "taosdata"
urls = ["http://localhost:9100"]
responseTimeout = "5s"
httpUsername = ""
httpPassword = ""
httpBearerTokenString = ""
caCertFile = ""
certFile = ""
keyFile = ""
insecureSkipVerify = true
gatherDuration = "5s"
```

- 设置 urls 为 node_exporter采集地址可配多个
- db 为写入的数据库需提前创建
- user 和 password 为 TDengine 连接使用用户名和密码
- responseTimeout 表示采集超时时间
- httpUsername 、 httpPassword、httpBearerTokenString、caCertFile、certFile、keyFile 为采集地址的认证信息
- insecureSkipVerify 表示跳过 https 验证
- gatherDuration 表示采集间隔

#### 4.3.6 collectd

collectd 除了 OpenTSDB 插件写入外自身协议也可以直接写入 taosAdapter
启用 taosAdapter 的配置 collectd.enable
```go {wrap}
[collectd]
// 启用
enable = false
// udp 端口
port = 6045
// 写入的数据库
db = "collectd"
// 连接 TDengine 的用户名
user = "root"
// 连接 TDengine 的密码
password = "taosdata"
// 消费写入的协程数
worker = 10
```

#### 4.3.7 JSON 数据写入

##### 4.3.7.1 配置

由于此配置复杂，无法支持命令行和环境变量配置，仅支持配置文件进行配置
1. 新增配置 input_json

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| enable | bool | 是否启用（默认为 true） |
| rules | Rule[] | 解析规则数组 |

1. 规则配置

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| endpoint | string | url端点 http://localhost:6041/input_json/v1/{endpoint} |
| db | string | 默认数据库名 |
| dbKey | string | 数据库名的 key，不能与 db 同时设置 |
| superTable | string | 默认超级表名 |
| superTableKey | string | 超级表名的 key，不能与 superTable 同时设置 |
| subTable | string | 默认子表名 |
| subTableKey | string | 子表名的key，不能与 subTable 同时设置 |
| timeKey | string | 时间路径，如果不设置则取收到数据时间 |
| timeFormat | string | 时间格式，当 timeKey 设置时有效，见 [taosAdapter 支持 HTTP JSON 写入 FS](https://taosdata.feishu.cn/wiki/Eb5CwW9QwiqUXjkmDMQcTDzInHh) |
| timezone | string | 解析时间所用时区设置，当 timeKey 设置时有效，IANA 时区格式，**默认值 taosAdapter 所在机器时区** |
| timeFieldName | string | 时间对应数据库列名 |
| fields | []Field | 写入字段配置（包含标签和列但不包括时间列） |
| transformation | string | jsonata 的转换规则 |

1. 字段配置

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| key | string | JSON 的 key，同时对应数据库的字段名 |
| optional | bool | 如果设置为 true，key 不存在时不会报错，生成 SQL 时将不会拼接该列 |

1. timestampFormat 预设的配置
预设以下配置，如果不满足要求可以按照 strftime 解析方式进行扩展 https://pkg.go.dev/github.com/ncruces/go-strftime@v1.0.0

| 配置 | 说明/格式 |
| --- | --- |
| unix | 秒级时间戳 |
| unix_ms | 毫秒级时间戳 |
| unix_us | 微秒级时间戳 |
| unix_ns | 纳秒级时间戳 |
| ansic | Mon Jan _2 15:04:05 2006 |
| rubydate | Mon Jan 02 15:04:05 -0700 2006 |
| rfc822z | 02 Jan 06 15:04 -0700 |
| rfc1123z | Mon, 02 Jan 2006 15:04:05 -0700 |
| rfc3339 | 2006-01-02T15:04:05Z07:00 |
| rfc3339nano | 2006-01-02T15:04:05.999999999Z07:00 支持秒级到纳秒级时间 |
| stamp | Jan _2 15:04:05 |
| stampmilli | Jan _2 15:04:05.000 |
| datetime | 2006-01-02 15:04:05.999999999 支持秒级到纳秒级时间 |

如果时间格式不包含时区信息则一定要配置时区配置 timeTimezone 来正确解析时间

##### 4.3.7.2 JSON 格式

jsonata 文档：https://docs.jsonata.org/overview.html 
仅支持 jsonata 1.5.4 版本，可在 https://try.jsonata.org/ 尝试解析，解析时使用 1.5.4 版本 
![](./images/img_SDKGbSCbvoavMgxvPbZcDG86njb.png)

经过 jsonata 转换后需要变成打平的一维数组，每个元素为一行数据，例如
```json
[
    {"db":"power","super_table_name":"meters","sub_table_name":"d_1001","location":"New York","id":1001,"time":"2025-10-23 15:30:11", "current": 15.5, "voltage": 220.0, "phase": 1},
    {"db":"power","super_table_name":"meters","sub_table_name":"d_1002","location":"Los Angeles","id":1002,"time":"2025-10-23 15:31:12", "current": 12.3, "voltage": 230.0, "phase": 2},
    {"db":"power","super_table_name":"meters","sub_table_name":"d_1003","location":"Chicago","id":1003,"time":"2025-10-23 15:32:13", "current": 14.8, "voltage": 225.0, "phase": 3}
]
```

##### 4.3.7.3 数据类型

列与 tag 数据解析后根据 JSON 类型拼接成 SQL 进行写入。时间使用 jsonata 转换非常复杂将使用 go time 解析模块进行解析，解析后将转换为 rfc3339nano 格式进行写入

##### 4.3.7.4 SQL 拼接规则

1. 对于相同库和超级表且转换后的 json 中所需的 key 都存在的数据将合成一个写入语句。
2. 当 json 中有所需的 key 不存在但设置了 optional 为 true 的数据将变成一条单独的写入语句，key 不存在的列将不指定
3. 生成的 sql 将拼接成接近 1M 的语句进行写入
4. 字符串数据将添加单引号进行包裹，并进行转义，规则如下
   - 忽略 `\0` 字符
   - `'`单引号将转义成两个字符 `''`
   - `\t` 字符将转义成三个字符 `\\t`
   - `\r` 字符将转义成三个字符 `\\r`
   - `\n` 字符将转义成三个字符 `\\n`
   - `\` 字符将转义成两个字符 `\\`
5. 写入 sql 使用自动建表语句，列名使用反引号包裹，如`insert into `power`.`meters` (`tbname`, `location`, `id`, `ts`, `current`, `voltage`, `phase`) values ('d_1001', 'New York', 1001, '2025-10-23T15:30:11+08:00', 15.5, 220.0, 1)"`

##### 4.3.7.5 空运行

由于功能复杂，提供 dry_run 以供调试使用。在请求参数中添加 dry_run=true 将返回处理后的 JSON 以及生成的 SQL，不会进行数据写入。
比如：
```json
curl -uroot:taosdata localhost:6041/input_json/v1/meters?dry_run=true -d '[{"db":"power","super_table_name":"meters","sub_table_name":"d_1001","location":"New York","id":1001,"time":"2025-10-23 15:30:11", "current": 15.5, "voltage": 220.0, "phase": 1}]'
```

响应：
```json
{
        "code": 0,
        "desc": "",
        "json": "[{\"db\":\"power\",\"super_table_name\":\"meters\",\"sub_table_name\":\"d_1001\",\"location\":\"New York\",\"id\":1001,\"time\":\"2025-10-23 15:30:11\", \"current\": 15.5, \"voltage\": 220.0, \"phase\": 1}]",
        "sql": ["insert into `power`.`meters` (`tbname`, `location`, `id`, `ts`, `current`, `voltage`, `phase`) values ('d_1001', 'New York', 1001, '2025-10-23T15:30:11+08:00', 15.5, 220.0, 1)"]
}
```

#### 4.3.8 OpenMetrics

OpenMetrics 是一种云原生监控标准，taosAdapter 通过拉取方式获取 OpenMetrics 数据。配置应用的指标接口、权限信息和采集间隔，taosAdapter 按照配置进行数据拉取，获取到的数据转换为无模式格式写入TDengine。同时支持 OpenMetrics 1.0.0 和 Prometheus 0.0.4，判断 response header 进行区分。

##### 4.3.8.1 配置项

TOML 配置总览：
```toml {wrap}
[openmetrics]

## 5. 启用 OpenMetrics 采集

enable = false

## 6. 写入的数据库列表，需与 URLs 一一对应

dbs = ["openmetrics_data"]

## 7. OpenMetrics 端点 URL 列表

urls = ["http://localhost:9090/metrics"]

## 8. 响应超时时间列表，需与 URLs 一一对应

responseTimeout = ["5s"]

## 9. HTTP Basic 认证用户名列表（可选）

httpUsername = [""]

## 10. HTTP Basic 认证密码列表（可选）

httpPassword = [""]

## 11. HTTP Bearer Token 列表（可选）

httpBearerTokenString = [""]

## 12. CA 证书文件路径列表（可选，用于 HTTPS）

caCertFile = [""]

## 13. 客户端证书文件路径列表（可选）

certFile = [""]

## 14. 客户端密钥文件路径列表（可选）

keyFile = [""]

## 15. 是否跳过 TLS 证书验证

insecureSkipVerify = true

## 16. 采集间隔时间列表（秒），需与 URLs 一一对应

gatherDuration = [5]

## 17. 是否忽略时间戳

ignoreTimestamp = false

## 18. TDengine 连接用户名

user = "root"

## 19. TDengine 连接密码

password = "taosdata"

## 20. TTL（表过期时间）列表，可选

ttl = [0]
```

配置详解：
1. open_metrics.enable
  - 类型：bool
  - 默认值：false
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_ENABLE
  - 说明：是否启用 OpenMetrics 采集
1. open_metrics.user
  - 类型：string
  - 默认值：root
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_USER
  - 说明：连接到 TDengine 的用户名
1. open_metrics.password
  - 类型：string
  - 默认值：taosdata
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_PASSWORD
  - 说明：连接到 TDengine 的密码
1. open_metrics.urls
  - 类型：string 数组
  - 默认值：["http://localhost:9100"]
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_URLS
  - 说明：采集地址，如果没有指定路由将默认添加`/metrics`
1. open_metrics.dbs
  - 类型：string 数组
  - 默认值：["open_metrics"]
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_DBS
  - 说明：写入 TDengine 的数据库，数量与采集地址数量相同，与采集地址一一对应。
1. open_metrics.responseTimeoutSeconds
  - 类型：int 数组
  - 默认值：[5]
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_RESPONSE_TIMEOUT_SECONDS
  - 说明：采集超时秒数，必须与采集地址数量相同，与采集地址一一对应。
1. open_metrics.httpUsernames
  - 类型：string 数组
  - 默认值：空
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_HTTP_USERNAMES
  - 说明：采集使用的 Basic 验证用户名，如果有值，需满足与采集地址数量相同。
1. open_metrics.httpPasswords
  - 类型：string 数组
  - 默认值：空
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_HTTP_PASSWORDS
  - 说明：采集使用的 Basic 验证密码，如果有值，需满足与采集地址数量相同。
1. open_metrics.httpBearerTokenStrings
  - 类型：string 数组
  - 默认值：空
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_HTTP_BEARER_TOKEN_STRINGS
  - 说明：采集使用的 Bearer 验证，如果有值，需满足与采集地址数量相同。
1. open_metrics.caCertFiles
  - 类型：string 数组
  - 默认值：空
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_CA_CERT_FILES
  - 说明：采集使用的根证书路径，如果有值，需满足与采集地址数量相同。
1. open_metrics.certFiles
  - 类型：string 数组
  - 默认值：空
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_CERT_FILES
  - 说明：采集使用的客户端证书路径，如果有值，需满足与采集地址数量相同。
1. open_metrics.keyFiles
  - 类型：string 数组
  - 默认值：空
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_KEY_FILES
  - 说明：采集使用的客户端证书密钥路径，如果有值，需满足与采集地址数量相同。
1. open_metrics.insecureSkipVerify
  - 类型：bool
  - 默认值：true
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_INSECURE_SKIP_VERIFY
  - 说明：采集是否跳过证书验证。
1. open_metrics.gatherDurationSeconds
  - 类型：int 数组
  - 默认值：[5]
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_GATHER_DURATION_SECONDS
  - 说明：采集间隔秒数，必须与采集地址数量相同，与采集地址一一对应。
1. open_metrics.ignoreTimestamp
  - 类型：bool
  - 默认值：false
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_IGNORE_TIMESTAMP
  - 说明：是否忽略采集到的时间戳，如果忽略将使用采集时刻的时间戳。
1. open_metrics.ttl
  - 类型：int 数组
  - 默认值：空
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_TTL
  - 说明：数据表超时时间（0 代表不超时），如果有值，需满足与采集地址数量相同。

##### 20.0.0.1 特性

- 多端点支持：可配置多个端点，每个端点独立的采集参数
- 认证支持：HTTP Basic 与 Bearer Token
- 安全连接：TLS/SSL（自定义 CA、双向证书）
- 协议兼容：OpenMetrics 1.0.0 与 Prometheus 0.0.4
示例：同时采集 Prometheus 与 Node Exporter 指标
```toml
[openmetrics]
enable = true
dbs = ["prometheus_metrics", "node_metrics"]
urls = [
  "http://prometheus-server:9090/metrics",
  "http://node-exporter:9100/metrics"
]
responseTimeout = ["5s", "3s"]
gatherDuration = [15, 10]
user = "root"
password = "taosdata"
```

### 20.1 获取 table 的 VGroup ID

可以 POST 请求 http 接口 `http://<fqdn>:<port>/rest/sql/<db>/vgid` 获取 table 的 VGroup ID，body 是多个表名 JSON 数组。

样例：获取数据库为 power，表名为 d_bind_1 和 d_bind_2 的 VGroup ID
```bash {wrap}
curl --location 'http://127.0.0.1:6041/rest/sql/power/vgid' \
--user 'root:taosdata' \
--data '["d_bind_1","d_bind_2"]
```

响应
```json {wrap}
{"code":0,"vgIDs":[153,152]}
```

### 20.2 健康检查与监控接口

#### 20.2.1 健康检查

用于探测 taosAdapter 当前可用性，支持按功能维度检查。
- 路径：`GET /-/ping`
- 可选参数：`action`
  - 不传或为空：检查全部功能
  - `query`：仅检查查询功能
- 返回：200（健康）/ 503（暂停或不可用）
示例：
```plaintext
curl http://127.0.0.1:6041/-/ping
curl "http://127.0.0.1:6041/-/ping?action=query"
```

典型用法：
- 负载均衡器后端健康检查
- Kubernetes liveness/readiness 探针

#### 20.2.2 Prometheus 指标（/metrics）

导出 taosAdapter 运行指标，供 Prometheus 抓取。
- 路径：`GET /metrics`
- 内容：Prometheus 文本格式（text/plain）
- 指标大类：
  - 请求统计：REST/WS 的总量、成功/失败、进行中等
  - 性能分布：处理时延、块大小等
  - 资源使用：连接池、Go runtime
Prometheus 抓取配置：
```yaml
scrape_configs:
  - job_name: taosadapter
    static_configs:
      - targets: ["localhost:6041"]
    metrics_path: /metrics
    scrape_interval: 15s
```

### 20.3 配置管理与优先级

taosAdapter 支持多种配置方式，优先级如下（从高到低）：
1. 命令行参数
2. 环境变量（前缀 `TAOS_ADAPTER_`）
3. 配置文件（Linux: `/etc/taos/taosadapter.toml`，Windows: `C:\\TDengine\\cfg\\taosadapter.toml`）
4. 内置默认值

#### 20.3.1 常用命令行参数

- `-P, --port`：HTTP 端口，默认 6041
- `--debug`：是否启用调试模式（开启 pprof/pprof）
- `--httpCodeServerError`：服务端错误时返回非 200 HTTP 状态码
- `--logLevel`：日志级别（trace|debug|info|warning|error）
- `--taosConfigDir`：TDengine 客户端配置目录
- `--restfulRowLimit`：REST 查询默认行数上限（-1 不限）
- `--smlAutoCreateDB`：Schemaless 写入时自动建库
- `--instanceId`：实例 ID（0–255）
- `--maxSyncConcurrentLimit` / `--maxAsyncConcurrentLimit`：C 同/异步方法并发上限（0 表示按 CPU 核数）
- `-V, --version`：打印版本信息并退出
- `--help`：打印帮助并退出

#### 20.3.2 环境变量（部分）

- `TAOS_ADAPTER_PORT`
- `TAOS_ADAPTER_DEBUG`
- `TAOS_ADAPTER_HTTP_CODE_SERVER_ERROR`
- `TAOS_ADAPTER_LOG_LEVEL`
- `TAOS_ADAPTER_TAOS_CONFIG_FILE`
- `TAOS_ADAPTER_RESTFUL_ROW_LIMIT`
- `TAOS_ADAPTER_SML_AUTO_CREATE_DB`
- `TAOS_ADAPTER_INSTANCE_ID`
- `TAOS_ADAPTER_MAX_SYNC_CONCURRENT_LIMIT`
- `TAOS_ADAPTER_MAX_ASYNC_CONCURRENT_LIMIT`

#### 20.3.3 配置文件关键段

- `[request]` 查询限流：
  - `queryLimitEnable`、`excludeQueryLimitSql` / `excludeQueryLimitSqlRegex`
  - `[request.default]` 与 `[request.users.<user>]`：`queryLimit`、`queryWaitTimeout`、`queryMaxWait`
- `[pool]` 连接池：`maxConnect`、`maxIdle`、`maxWait`、`waitTimeout`
- `[monitor]` 监控与暂停阈值：`pauseQueryMemoryThreshold`、`pauseAllMemoryThreshold`
- `[uploadKeeper]` 上报 taosKeeper：`url`、`interval`、`retryTimes`
- `[log]` 日志：`level`、`rotation*`、`enableSqlToCsvLogging`
- `[ssl]`（企业版）：`enable`、`certFile`、`keyFile`
- 插件开关：`[opentsdb]`、`[influxdb]`、`[statsd]`、`[node_exporter]`、`[openmetrics]` 等
配置示例可参考仓库 `example/config/taosadapter.toml`。

## 21. 安全特性

### 21.1 认证机制

#### 21.1.1 RESTful 认证

- **Basic 认证**：`Authorization: Basic <base64(user:pass)>`
- **自定义 Token**：`Authorization: Taosd <TOKEN>` （通过 `/rest/login` 获取）
- URL 参数：`curl -u user:pass ...`

#### 21.1.2 WebSocket 认证

- 创建连接时通过 `conn` action 传递用户名和密码进行验证。
- 服务端验证失败则关闭连接。

#### 21.1.3 插件认证

部分插件需要指定用户名、密码或数据库名：
- **OpenTSDB Telnet / StatsD**：需要在配置中指定 `user` / `password`，默认关闭；

### 21.2 TLS/SSL 支持

服务端支持 TLS（企业版）：
```toml
[ssl]
enable = true
certFile = "/path/to/server.crt"
keyFile = "/path/to/server.key"
```

启用后 HTTP 服务升级为 HTTPS，WebSocket 升级为 WSS。

### 21.3 访问控制

#### 21.3.1 白名单（Whitelist）

企业版功能，通过 TDengine 服务端配置限制客户端 IP/用户。
taosAdapter 在 `GetConnection()` 时会自动检查白名单，如未通过则返回错误。

#### 21.3.2 CORS 配置

允许使用 CORS 配置设置允许访问的来源或方法等：
```toml
[cors]
allowAllOrigins = true  # 允许所有来源

## 22. 或指定具体来源

allowOrigins = ["https://example.com"]
allowMethods = ["GET", "POST", "PUT", "DELETE"]
allowHeaders = ["*"]
```

#### 22.0.1 查询限流

基于用户的查询限制：
```toml
[request]
queryLimitEnable = true
excludeQueryLimitSql = ["select 1"]
[request.users.analyst]
queryLimit = 100           # 每次查询最多返回 100 行
queryWaitTimeout = 60      # 等待超时 60秒
queryMaxWait = 5           # 最多 5 个查询在队列中
```

#### 22.0.2 并发控制

1. **HTTP 并发**：使用 Gin Web 框架默认的多 goroutine 处理；
2. **C 方法并发**：为了限制 cgo 线程的并发访问数量，通过以下参数对 C 方法调用进行限制：
  - Sync: `SyncSemaphore`，限制 `maxSyncConcurrentLimit`
  - Async: `AsyncSemaphore`，限制 `maxAsyncConcurrentLimit`
1. **连接池**：使用`maxConnect` 限制同时活动的连接数；
2. **内存流控**：
  - `pauseQueryMemoryThreshold`: 达到阈值会暂停查询；
  - `pauseAllMemoryThreshold`: 达到阈值暂停所有操作；

## 23. 审计日志

### 23.1 SQL 记录

可以通过 API 或配置开启或关闭 SQL 记录。
```toml
[log]
enableSqlToCsvLogging = true  # 记录 HTTP/WS SQL 到 CSV
```

记录字段包括：时间、用户、来源 IP、SQL 语句、执行时间、错误信息等。

### 23.2 请求跟踪

每个请求使用独立的 req_id 参数标记，该 req_id 参数贯穿请求全生命周期，日志中以 reqID 字段进行记录，可用于分布式追踪。

### 23.3 敏感信息保护

1. 密码不记录到日志
2. Token 在日志中脱敏处理

## 24. 性能

### 24.1 写入性能

1. REST 接口：taosBenchmark rest 模式写入，并发 1000，典型电表表结构，100w 子表，每子表写入 100 条，QPS 达到 40w/s 以上。
2. WebSocket 接口：批量写入性能优于 REST，二进制协议比 JSON 协议性能更优。
3. Schemaless 写入：InfluxDB Line Protocol 单实例可达 30w+ points/s。

### 24.2 查询性能

1. REST 查询：使用 JMeter JDBC 方式发送 rest 请求，select last_row(*) from test.${tbname} 语句，tbname 随机，并发 1000，循环 50 次，QPS 达到 1000/s 以上。
2. WebSocket 查询：长连接模式下性能优于 REST，避免 HTTP 握手开销。

### 24.3 性能调优建议

#### 24.3.1 连接池配置

```toml {wrap}
[pool]
maxConnect = 0  # 0 表示使用 CPU 核数 * 2，建议根据负载调整
maxIdle = 0     # 应与 maxConnect 相同
maxWait = 0     # 0 表示无限制，生产环境建议设置合理值如 10000
waitTimeout = 60  # 等待连接超时（秒）
```

推荐配置（基于负载）：
1. 轻负载（<100 QPS）：maxConnect = 10
2. 中等负载（100-1000 QPS）：maxConnect = 50
3. 高负载（>1000 QPS）：maxConnect = 100+，结合水平扩展

#### 24.3.2 并发限制

```toml {wrap}
maxSyncConcurrentLimit = 0   # 建议设置为 CPU 核数的 1-2 倍
maxAsyncConcurrentLimit = 0  # 建议设置为 CPU 核数的 2-4 倍
```

#### 24.3.3 批量操作优化

1. 使用 WebSocket Stmt 代替多次 REST 调用
2. 启用 二进制协议减少序列化开销
3. 合理使用 Raw Block 写入（批量场景）

#### 24.3.4 网络优化

1. 启用 GZIP 压缩（默认启用）降低带宽占用
2. 部署 taosAdapter 与 TDengine 同机房或低延迟网络
3. 使用 HTTP/2 或 WebSocket 长连接

#### 24.3.5 内存管理

```toml {wrap}
[monitor]
pauseQueryMemoryThreshold = 70  # 内存达到 70% 暂停查询
pauseAllMemoryThreshold = 80    # 内存达到 80% 暂停所有操作
```

#### 24.3.6 性能瓶颈分析

##### 24.3.6.1 常见瓶颈

1. 连接池耗尽：表现为请求超时，解决方法是增加 maxConnect
2. C 方法并发限制：表现为吞吐量无法提升，调整 maxSyncConcurrentLimit / maxAsyncConcurrentLimit
3. TDengine 服务端瓶颈：检查 show dnodes、show vnodes 状态
4. 网络带宽不足：启用压缩，优化数据结构

##### 24.3.6.2 性能监控指标

通过 /metrics 端点监控：
1. taosadapter_pool_connections：连接池使用情况
2. rest_in_process / ws_in_process：当前处理中的请求数
3. rest_write_success / rest_query_success：成功率
4. go_memstats_alloc_bytes：内存使用

## 25. 兼容性

1. taosAdapter 版本因为使用 cgo 调用 TDengine C 接口，因此部署上需要与 TDengine 版本对应。
2. 协议兼容性：taosAdapter 的 WebSocket 接口在设计上考虑了兼容性，会尽量保证协议的向后兼容，确保即使在进行版本更新或功能扩展时，原有的接口和协议仍然能够正常工作。

## 26. 运维

### 26.1 水平扩展

taosAdapter 是无状态的，结合 TDengine 特有的胖客户端设计，可以提供 taosadapter 服务的水平扩展能力，上层通过使用负载均衡器进行流量分发，提升集群的总体性能和可靠性。
Nginx 配置示例：
```nginx
upstream taosadapter {
    least_conn;  # 最少连接数算法
    server 192.168.1.10:6041 max_fails=3 fail_timeout=30s;
    server 192.168.1.11:6041 max_fails=3 fail_timeout=30s;
    server 192.168.1.12:6041 max_fails=3 fail_timeout=30s;
    keepalive 32;  # 保持连接
}
server {
    listen 80;
    server_name taosadapter.example.com;
    # REST 接口
    location /rest/ {
        proxy_pass http://taosadapter;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_connect_timeout 10s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;
    }
    # WebSocket 接口（需要会话保持）
    location /ws {
        proxy_pass http://taosadapter;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_connect_timeout 60s;
        proxy_send_timeout 3600s;
        proxy_read_timeout 3600s;
        # WebSocket 会话保持
        ip_hash;
    }
    # 健康检查
    location /-/ping {
        proxy_pass http://taosadapter;
        access_log off;
    }
}
```

### 26.2 资源监控

建议监控 CPU、内存、磁盘和网络的使用情况，确保 taosAdapter 的资源分配能够满足流量波动。工具如 Prometheus 和 Grafana 可用于资源和性能监控，实时反馈系统负载情况。

### 26.3 日志管理

**日志位置**：
- 主日志：`/var/log/taos/taosadapter.log`
- SQL 日志（启用后）：`/var/log/taos/taosadapter_sql.csv`
**日志轮转配置**：
```toml
[log]
path = "/var/log/taos"
level = "info"
rotationCount = 30      # 保留 30 个历史文件
keepDays = 30          # 保留 30 天
rotationSize = "1GB"   # 单文件 1GB 轮转
compress = true        # 压缩历史日志
reservedDiskSize = "1GB"  # 预留磁盘空间
```

**日志分析常用命令**：
```plaintext

## 27. 查看错误日志

grep "ERROR" /var/log/taos/taosadapter.log | tail -100

## 28. 分析慢查询（SQL 日志）

awk -F',' '$6 > 1000 {print $0}' /var/log/taos/taosadapter_sql.csv | tail -20

## 29. 统计请求量

grep "request" /var/log/taos/taosadapter.log | wc -l

## 30. 实时查看日志

tail -f /var/log/taos/taosadapter.log
```

### 30.1 备份与恢复

#### 30.1.1 配置文件备份

```plaintext

## 31. 备份配置

cp /etc/taos/taosadapter.toml /backup/taosadapter.toml.$(date +%Y%m%d)

## 32. 恢复配置

cp /backup/taosadapter.toml.20250124 /etc/taos/taosadapter.toml
systemctl restart taosadapter
```

#### 32.0.1 数据备份

taosAdapter 本身无状态，数据存储在 TDengine 中。备份策略参考 TDengine 官方文档。

### 32.1 故障处理

#### 32.1.1 服务管理

```plaintext

## 33. 启动服务

systemctl start taosadapter

## 34. 停止服务

systemctl stop taosadapter

## 35. 重启服务

systemctl restart taosadapter

## 36. 查看状态

systemctl status taosadapter

## 37. 查看日志

journalctl -u taosadapter -f

## 38. 优雅重启（等待现有连接完成）

kill -SIGTERM $(pidof taosadapter)
```

#### 38.0.1 故障自愈

**Systemd 自动重启配置**：
```toml
[Unit]
Description=taosAdapter service
After=network.target
[Service]
Type=simple
ExecStart=/usr/bin/taosadapter
Restart=always
RestartSec=10s
StartLimitInterval=0
[Install]
WantedBy=multi-user.target
```

### 38.1 升级策略

#### 38.1.1 滚动升级

1. 从负载均衡器移除第一个实例
2. 停止该实例：`systemctl stop taosadapter`
3. 更新二进制文件：`cp taosadapter /usr/bin/`
4. 启动实例：`systemctl start taosadapter`
5. 验证健康：`curl ``http://localhost:6041/-/ping`
6. 加回负载均衡器
7. 重复步骤 1-6 处理其他实例

#### 38.1.2 版本兼容性检查

```plaintext

## 39. 查看当前版本

taosadapter -V

## 40. 检查 TDengine 版本

taos --version

## 41. 确保 taosAdapter 与 TDengine 版本匹配

```

### 41.1 容量管理

#### 41.1.1 磁盘空间监控

```plaintext

## 42. 检查日志目录空间

df -h /var/log/taos

## 43. 清理过期日志（超过 30 天）

find /var/log/taos -name "taosadapter.log.*" -mtime +30 -delete
```

#### 43.0.1 连接数监控

```plaintext

## 44. 查看当前连接数

netstat -an | grep :6041 | wc -l

## 45. 查看 WebSocket 连接

netstat -an | grep :6041 | grep ESTABLISHED
```

## 46. 使用场景

1. taosAdapter 作为一个桥接工具，允许 Telegraf、StatsD 或 collectd 采集的数据直接发送到 TDengine，而无需对现有工具进行大规模配置和修改。用户可以继续使用熟悉的工具来收集数据，但数据会被直接导入到 TDengine 中进行存储和分析
2. taosAdapter 作为数据接入的中间层，能够帮助高效地接入大规模的数据源，并通过 TDengine 提供的分布式处理能力进行扩展。对于需要高吞吐量、高并发的数据采集和存储系统，taosAdapter 可以帮助简化集成过程。

## 47. 约束和限制

1. 版本匹配问题：不同版本的 TDengine 可能会引入新的功能或修复，taosAdapter 需要确保与使用的 TDengine 版本兼容。
2. 连接池限制：当 taosAdapter 需要处理大量并发请求时，连接池的大小和并发限制可能成为瓶颈。对于高并发写入，taosAdapter 可能需要调整连接池的配置，确保连接数足够，避免因连接池耗尽导致的数据写入失败。
3. 处理能力：虽然 taosAdapter 设计上尽量减少性能瓶颈，但如果遇到高并发、大数据量的场景，taosAdapter 可能会成为瓶颈。特别是当数据流入速度远高于 taosAdapter 处理能力时，可能会出现处理延迟。在极端负载情况下，taosAdapter 可能需要进行水平扩展，以避免性能瓶颈。

## 48. 常见错误和排查

### 48.1 WebSocket 连接失败

问题描述：客户端无法成功连接到 taosAdapter 的 WebSocket 服务。
可能原因：
1. 端口问题：WebSocket 服务监听的端口不可用或防火墙阻塞。
2. 网络问题：客户端与服务器之间的网络连接存在问题（如网络延迟、丢包或断开）。
3. WebSocket 协议不匹配：客户端和服务器端的 WebSocket 协议版本不匹配，导致握手失败。
4. 负载均衡配置错误：如果部署了多个 taosAdapter 实例，负载均衡器配置不当可能导致连接到不可用的实例。
排查步骤：
1. 检查端口和防火墙：确保 WebSocket 服务监听的端口是开放的，防火墙未阻止端口访问。使用 telnet <ip> <port> 或 nc 命令测试端口连通性。
2. 检查 WebSocket 协议：使用 postman 等工具验证能否连接到 WebSocket 接口以排除客户端协议问题
3. 检查网络连接：使用 ping 或 traceroute 来测试网络连接是否稳定，排除网络层问题。
4. 负载均衡配置检查：确保负载均衡器正确配置，能够将流量分配到健康的 taosAdapter 实例。检查负载均衡器的健康检查是否有效。

### 48.2 数据写入延迟过高

问题描述：通过 taosAdapter 向 TDengine 写入数据时，出现明显的延迟。
可能原因：
1. 高负载：taosAdapter 或 TDengine 本身的负载过高，导致写入操作排队。
2. 网络带宽不足：数据传输过程中的带宽限制，导致数据写入延迟。
3. 客户端请求过于频繁：客户端发起请求过于频繁，taosAdapter 无法及时处理所有请求。
4. I/O 瓶颈：taosAdapter 与 TDengine 之间的 I/O 操作可能成为瓶颈，特别是当数据量较大时。
排查步骤：
1. 检查系统资源：使用 top 或 htop 命令监控 taosAdapter 和 TDengine 的 CPU、内存、磁盘 I/O 和网络带宽使用情况，确保资源充足。
2. 分析网络带宽：使用 iperf 等工具检测客户端和 taosAdapter 之间的网络带宽，确保数据传输不会受到瓶颈限制。
3. 调整并发设置：根据系统负载，调整 taosAdapter 的并发请求数和数据库写入策略（如批量写入）。
4. 优化写入策略：如果数据量较大，可以尝试通过增加批量写入的大小来减少网络传输和数据库写入的延迟。
5. 调整连接池配置：如果通过 HTTP 请求写入可能受到连接池限制导致请求阻塞在获取连接上，可通过调整连接池配置进行缓解

### 48.3 认证失败错误

**问题描述**：HTTP 401 Unauthorized 或 WebSocket 连接拒绝。
**可能原因**：
- 用户名或密码错误
- Token 过期或格式错误
- TDengine 用户不存在
- 白名单限制（企业版）
**排查步骤**：
1. 验证凭证：`curl ``http://localhost:6041/rest/login/<user>/<pass>`
2. 检查 TDengine 用户：`taos` 命令行登录后 `SHOW USERS;`
3. 查看 taosAdapter 日志中的认证错误（错误码 0x0357）
4. 验证 Basic 认证 Token：`echo -n "root:taosdata" | base64`
5. 检查白名单配置（如果启用）

### 48.4 错误码 0x000B（网络不可用）

**问题描述**：RPC_NETWORK_UNAVAIL 错误。
**可能原因**：
- TDengine 服务端未启动
- 网络不可达（FQDN 解析错误、端口被阻止）
- 防火墙或安全组配置
**排查步骤**：
1. 检查 TDengine 进程：`systemctl status taosd`
2. 测试网络连通性：`ping <taosd_host>` 和 `telnet <taosd_host> 6030`
3. 验证 FQDN 解析：`nslookup <fqdn>` / `dig <fqdn>`
4. 检查防火墙：`iptables -L` / `firewall-cmd --list-all`
5. 查看 taos.cfg 中 `serverPort` / `firstEp` 配置

### 48.5 连接池耗尽

**问题描述**：503 Service Unavailable 或请求超时。
**可能原因**：
- 并发请求过多超过连接池容量
- 连接没有正确释放
- `maxConnect` / `waitTimeout` 配置不合理
**排查步骤**：
1. 检查 Prometheus 指标：`taosadapter_pool_connections`
2. 增加连接池大小：修改 `[pool].maxConnect`
3. 调整超时：`[pool].waitTimeout` 与 `responseTimeout`
4. 排查长连接：检查是否有未关闭的 WebSocket 连接

## 49. 可观测性

taosAdapter 会将 SQL 执行的相关指标上报给 taosKeeper，以下是这些监控指标的详细介绍。

| 字段 | 描述 |
| --- | --- |
| rest_total | RESTful SQL 请求总数量 |
| rest_query | RESTful 查询 SQL 请求数量 |
| rest_write | RESTful 写入 SQL 请求数量 |
| rest_other | RESTful 其他 SQL 请求数量（除了 insert 和 select 之外） |
| rest_success | RESTful SQL 请求成功总数量 |
| rest_query_success | RESTful 查询 SQL 请求成功数量 |
| rest_write_success | RESTful 写入 SQL 请求成功数量 |
| rest_other_success | RESTful 其他 SQL 请求成功数量 |
| rest_fail | RESTful SQL 请求失败总数量 |
| rest_query_fail | RESTful 查询 SQL 请求失败数量 |
| rest_write_fail | RESTful 写入 SQL 请求失败数量 |
| rest_other_fail | RESTful 其他 SQL 请求失败数量 |
| rest_in_process | RESTful 执行中 SQL 总数量 |
| rest_query_in_process | RESTful 执行中查询 SQL 数量 |
| rest_write_in_process | RESTful 执行中写入 SQL 数量 |
| rest_other_in_process | RESTful 执行中其他 SQL 数量 |
| ws_total | WebSocket SQL 请求数量 |
| ws_query | WebSocket 查询 SQL 请求数量 |
| ws_write | WebSocket 写入 SQL 请求数量 |
| ws_other | WebSocket 其他 SQL 请求数量 |
| ws_success | WebSocket SQL 请求成功总数量 |
| ws_query_success | WebSocket 查询 SQL 请求成功数量 |
| ws_write_success | WebSocket 写入 SQL 请求成功数量 |
| ws_other_success | WebSocket 其他 SQL 请求成功数量 |
| ws_fail | WebSocket SQL 请求失败总数量 |
| ws_query_fail | WebSocket 查询 SQL 请求失败数量 |
| ws_write_fail | WebSocket 写入 SQL 请求失败数量 |
| ws_other_fail | WebSocket 其他 SQL 请求失败数量 |
| ws_in_process | WebSocket 执行中 SQL 总数量 |
| ws_query_in_process | WebSocket 执行中查询 SQL 数量 |
| ws_write_in_process | WebSocket 执行中写入 SQL 数量 |
| ws_other_in_process | WebSocket 执行中其他 SQL 数量 |

## 50. 安装和卸载

该组件随 TDengine 产品安装包一同发布，随 TDengine 安装和卸载。

## 51. 文档

需要在官方文档中添加章节【taosAdapter 参考手册】。

## 52. 参考文档

1. [C/C++ 连接器-Function Spec](https://taosdata.feishu.cn/wiki/Hk2Swj9bdipmZCkK0NEcZCKankd)
2. **InfluxDB V1 写接口：** https://docs.influxdata.com/influxdb/v2.0/reference/api/influxdb-1x/write/
3. **OpenTSDB：**
  - [http://opentsdb.net/docs/build/html/api_http/put.html](http://opentsdb.net/docs/build/html/api_http/put.html)
  - [http://opentsdb.net/docs/build/html/api_telnet/put.html](http://opentsdb.net/docs/build/html/api_telnet/put.html)
1. **Prometheus remote_read 和 remote_write：** https://prometheus.io/blog/2019/10/10/remote-read-meets-streaming/
2. **node_exporter：** https://github.com/prometheus/node_exporter
3. **collectd：** https://www.collectd.org/
4. **StatsD：** https://github.com/statsd/statsd
5. **icinga2 OpenTSDB writer：** https://icinga.com/docs/icinga-2/latest/doc/14-features/#opentsdb-writer
6. **TCollector：** http://opentsdb.net/docs/build/html/user_guide/utilities/tcollector.html

## 53. 附录

无。
