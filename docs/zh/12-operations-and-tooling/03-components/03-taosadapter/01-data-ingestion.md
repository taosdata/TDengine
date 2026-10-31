---
title: 数据写入接口
sidebar_label: 数据写入接口
toc_max_heading_level: 4
---

import Prometheus from "../resources/_prometheus.mdx"
import CollectD from "../resources/_collectd.mdx"
import StatsD from "../resources/_statsd.mdx"
import Icinga2 from "../resources/_icinga2.mdx"
import TCollector from "../resources/_tcollector.mdx"

## 数据写入接口

### WebSocket 接口

各语言连接器通过 taosAdapter 的 WebSocket 接口，能够实现 SQL 执行、无模式写入、参数绑定和数据订阅功能。参考 [开发指南](../../../10-developer-guide/01-connect/index.md#websocket-连接)。

### InfluxDB v1 接口

taosAdapter 兼容 InfluxDB v1 的写入和 ping 接口。

#### InfluxDB v1 数据写入

你可以使用任何支持 HTTP 协议的客户端访问 Restful 接口地址 `http://<fqdn>:6041/influxdb/v1/write` 来写入 InfluxDB 兼容格式的数据到 TDengine。

支持 InfluxDB 参数如下：

- `db` 指定 TDengine 使用的数据库名
- `precision` TDengine 使用的时间精度
- `u` TDengine 用户名
- `p` TDengine 密码
- `ttl` 自动创建的子表生命周期，以子表的第一条数据的 TTL 参数为准，不可更新。更多信息请参考 [创建表文档](../../../05-tdengine-sql/02-ddl/02-table.md#创建表)的 TTL 参数。
- `table_name_key` 自定义子表名使用的标签名，如果设置了该参数，则子表名将使用该标签对应的值。

注意：目前不支持 InfluxDB 的 token 验证方式，仅支持 Basic 验证和查询参数验证。
示例：

```shell
curl --request POST http://127.0.0.1:6041/influxdb/v1/write?db=test --user "root:taosdata" --data-binary "measurement,host=host1 field1=2i,field2=2.0 1577836800000000000"
```

#### InfluxDB v1 ping 接口

taosAdapter 兼容 InfluxDB v1 的 ping 接口，可用于健康检查与连通性探测。你可以使用任何支持 HTTP 协议的客户端访问 `http://<fqdn>:6041/influxdb/v1/ping`。

该接口支持 GET 和 HEAD 方法，无需认证，行为与 InfluxDB 1.x 的 `/ping` 保持一致：

- 响应头携带 `X-InfluxDB-Version`，值为 taosAdapter 的版本号。
- 默认返回 `204 No Content`，无响应体。
- 当查询参数 `verbose` 的值非空且不为 `false` 或 `0`（例如 `verbose=true`）时，返回 `200 OK`，响应体为 JSON 格式的版本信息，例如 `{"version":"3.4.0.0"}`。

示例：

```shell
curl -i http://127.0.0.1:6041/influxdb/v1/ping
curl http://127.0.0.1:6041/influxdb/v1/ping?verbose=true
```

### OpenTSDB JSON 和 telnet 格式写入

你可以使用任何支持 HTTP 协议的客户端访问 Restful 接口地址 `http://<fqdn>:6041/<APIEndPoint>` 来写入 OpenTSDB 兼容格式的数据到 TDengine。EndPoint 如下：

```text
/opentsdb/v1/put/json/<db>
/opentsdb/v1/put/telnet/<db>
```

### collectd 数据写入

<CollectD />

### StatsD 数据写入

<StatsD />

### icinga2 OpenTSDB writer 数据写入

<Icinga2 />

### TCollector 数据写入

<TCollector />

### OpenMetrics 采集写入

OpenMetrics 是一种由 CNCF（云原生计算基金会）支持的开放标准，专注于规范指标数据的采集和传输，是云原生生态中监控和可观测性系统的核心规范之一。

从 **3.3.7.0** 版本开始，taosAdapter 支持 OpenMetrics v1.0.0 数据采集与写入，同时兼容 Prometheus 0.0.4 协议，确保与 Prometheus 生态的无缝集成。

启用 OpenMetrics 数据采集写入需要以下步骤：

- 启用 taosAdapter 的配置 `open_metrics.enable`
- 设置 OpenMetrics 的相关配置
- 重新启动 taosAdapter

### Prometheus remote_read 和 remote_write

<Prometheus />

### node_exporter 采集写入

从 **3.3.7.0** 版本开始，可以使用 OpenMetrics 插件替代 node_exporter 进行数据采集和写入。

Prometheus 使用的由 \*NIX 内核暴露的硬件和操作系统指标的输出器。

- 启用 taosAdapter 的配置 node_exporter.enable
- 设置 node_exporter 的相关配置
- 重新启动 taosAdapter

### JSON 数据写入

taosAdapter 从 **3.4.0.0** 版本开始支持通过 RESTful 接口写入 JSON 格式的数据到 TDengine。你可以使用任何支持 HTTP 协议的客户端通过 POST RESTful 接口地址 `http://<fqdn>:6041/input_json/v1/{endpoint}` 来写入 JSON 格式的数据到 TDengine。

JSON 格式要求为一个包含多行数据的数组，每行数据为一个 JSON 对象。每个 JSON 对象对应一条数据记录，数据提取可通过配置文件进行定义，如果输入 JSON 格式与要求不符，可以通过 [JSONata](https://jsonata.org/) 表达式进行转换（支持 JSONata 1.5.4 版本）。

一个配置样例如下（默认配置文件路径：`/etc/taos/taosadapter.toml`）：

```toml
[input_json]
enable = true
[[input_json.rules]]
endpoint = "rule1"
dbKey = "db"
superTableKey = "stb"
subTableKey = "table"
timeKey = "time"
timeFormat = "datetime"
timeZone = "UTC"
transformation = '''
$sort(
    (
        $ts := time;
        $each($, function($value, $key) {
            $key = "time" ? [] : (
                $each($value, function($groupValue, $groupKey) {
                    $each($groupValue, function($deviceValue, $deviceKey) {
                        {
                            "db": "test_input_json",
                            "time": $ts,
                            "location": $key,
                            "groupid": $number($split($groupKey, "_")[1]),
                            "stb": "meters",
                            "table": $deviceKey,
                            "current": $deviceValue.current,
                            "voltage": $deviceValue.voltage,
                            "phase": $deviceValue.phase
                        }
                    })[]
                })[]
            )
        })
    ).[*][*],
    function($l, $r) {
        $l.table > $r.table
    }
)
'''
fields = [
    {key = "current", optional = false},
    {key = "voltage", optional = false},
    {key = "phase", optional = false},
    {key = "location", optional = false},
    {key = "groupid", optional = false},
]
```

修改配置文件后需要重启 taosAdapter 服务以使配置生效。

完整配置参数说明：

- `input_json.enable`：启用或禁用 JSON 数据写入功能（默认值：`false`）。
- `input_json.rules`：定义 JSON 数据写入规则的数组，可以配置多个规则。
  - `endpoint`：指定 RESTful 接口的端点名称，只允许大小写字母与数字以及`_` 和 `-`。
  - `db`：指定写入数据的目标数据库名称，禁止包含反引号 `` ` ``。
  - `dbKey`：指定 JSON 对象中用于表示数据库名称的键名，与 `db` 不可同时配置。
  - `superTable`：指定写入数据的目标超级表名称，禁止包含反引号 `` ` ``。
  - `superTableKey`：指定 JSON 对象中用于表示超级表名称的键名，与 `superTable` 不可同时配置。
  - `subTable`：指定写入数据的目标子表名称。
  - `subTableKey`：指定 JSON 对象中用于表示子表名称的键名，与 `subTable` 不可同时配置。
  - `timeKey`：指定 JSON 对象中用于表示时间戳的键名，如果不设置则默认 `ts`。
  - `timeFormat`：指定时间解析的格式，当 timeKey 设置时有效，具体支持格式见[时间解析格式说明](#时间解析格式说明)。
  - `timezone`：指定时间戳的时区，当 timeKey 设置时有效，IANA 时区格式，默认值 taosAdapter 所在机器时区。
  - `transformation`：使用 JSONata 表达式对输入的 JSON 数据进行转换，以符合 TDengine 的数据写入要求，具体语法见 [JSONata 文档](https://jsonata.org/)。
  - `fields`：定义要写入的字段列表，每个字段包含以下属性：
    - `key`：指定 JSON 对象中用于表示字段值的键名，需要与数据库字段名相同，禁止包含反引号 `` ` ``。
    - `optional`：指定该字段是否为可选字段，默认值为 `false`，表示该字段为必填字段，如果 `key` 不存在时会报错。如果设置为 `true`，则表示该字段为可选字段，如果 `key` 不存在则不会报错，生成 SQL 时将不会包含该列。

数据写入前需要确保目标数据库和超级表已经创建完成，假设已经创建了如下数据库和超级表：

```sql
create database test_input_json;
create table test_input_json.meters (ts timestamp, current float, voltage int, phase float) tags (location nchar(64), `groupid` int);
```

请求示例：

```shell
```shell
curl -L 'http://localhost:6041/input_json/v1/rule1' \
-u root:taosdata \
-d '{"time":"2025-11-04 09:24:13.123","Los Angeles":{"group_1":{"d_001":{"current":10.5,"voltage":220,"phase":30},"d_002":{"current":15.2,"voltage":230,"phase":45},"d_003":{"current":8.7,"voltage":210,"phase":60}},"group_2":{"d_004":{"current":12.3,"voltage":225,"phase":15},"d_005":{"current":9.8,"voltage":215,"phase":75}}},"New York":{"group_1":{"d_006":{"current":11.0,"voltage":240,"phase":20},"d_007":{"current":14.5,"voltage":235,"phase":50}},"group_2":{"d_008":{"current":13.2,"voltage":245,"phase":10},"d_009":{"current":7.9,"voltage":220,"phase":80}}}}'
```

返回结果示例：

```json
{
  "code": 0,
  "desc": "",
  "affected": 9
}
```

- `code`：表示请求的状态码，`0` 表示成功，非 `0` 表示失败。
- `desc`：表示请求的描述信息，如果 `code` 非 `0`，则会包含错误信息。
- `affected`：表示成功写入的记录数。

检查写入结果：

```bash
taos> select tbname,* from test_input_json.meters order by tbname asc;
             tbname             |           ts            |       current        |   voltage   |        phase         |            location            |   groupid   |
======================================================================================================================================================================
 d_001                          | 2025-11-04 17:24:13.123 |                 10.5 |         220 |                   30 | Los Angeles                    |           1 |
 d_002                          | 2025-11-04 17:24:13.123 |                 15.2 |         230 |                   45 | Los Angeles                    |           1 |
 d_003                          | 2025-11-04 17:24:13.123 |                  8.7 |         210 |                   60 | Los Angeles                    |           1 |
 d_004                          | 2025-11-04 17:24:13.123 |                 12.3 |         225 |                   15 | Los Angeles                    |           2 |
 d_005                          | 2025-11-04 17:24:13.123 |                  9.8 |         215 |                   75 | Los Angeles                    |           2 |
 d_006                          | 2025-11-04 17:24:13.123 |                   11 |         240 |                   20 | New York                       |           1 |
 d_007                          | 2025-11-04 17:24:13.123 |                 14.5 |         235 |                   50 | New York                       |           1 |
 d_008                          | 2025-11-04 17:24:13.123 |                 13.2 |         245 |                   10 | New York                       |           2 |
 d_009                          | 2025-11-04 17:24:13.123 |                  7.9 |         220 |                   80 | New York                       |           2 |
```

TDengine 中的数据已经成功写入，TDengine 配置为 UTC+8 时区，因此时间显示为 `2025-11-04 17:24:13.123`。

#### 时间解析格式说明

时间格式预设以下几种类型：

- `unix`：表示时间戳为秒级别的整数或浮点数。
- `unix_ms`：表示时间戳为毫秒级别的整数或浮点数。
- `unix_us`：表示时间戳为微秒级别的整数或浮点数。
- `unix_ns`：表示时间戳为纳秒级别的整数或浮点数。
- `ansic`：表示时间格式为 `Mon Jan _2 15:04:05 2006`。
- `rubydate`：表示时间格式为 `Mon Jan 02 15:04:05 -0700 2006`。
- `rfc822z`：表示时间格式为 `02 Jan 06 15:04 -0700`。
- `rfc1123z`：表示时间格式为 `Mon, 02 Jan 2006 15:04:05 -0700`。
- `rfc3339`：表示时间格式为 `2006-01-02T15:04:05Z07:00`。
- `rfc3339nano`：表示时间格式为 `2006-01-02T15:04:05.999999999Z07:00`。
- `stamp`：表示时间格式为 `Jan _2 15:04:05`。
- `stampmilli`：表示时间格式为 `Jan _2 15:04:05.000`。
- `datetime`：表示时间格式为 `2006-01-02 15:04:05.999999999`。

如果不满足要求可以按照 [strftime 解析方式](https://pkg.go.dev/github.com/ncruces/go-strftime@v1.0.0)进行扩展。

#### transformation 示例说明

对于复杂的 JSON 数据格式，可以通过配置 `transformation` 使用 JSONata 表达式对输入的 JSON 数据进行转换，以符合 TDengine 的数据写入要求。可以通过 [JSONata 在线编辑器](https://try.jsonata.org/) 来调试和验证 JSONata 表达式。

假设输入的 JSON 数据如下：

```json
{
    "time": "2025-11-04 09:24:13.123",
    "Los Angeles": {
        "group_1": {
            "d_001": {
                "current": 10.5,
                "voltage": 220,
                "phase": 30
            },
            "d_002": {
                "current": 15.2,
                "voltage": 230,
                "phase": 45
            },
            "d_003": {
                "current": 8.7,
                "voltage": 210,
                "phase": 60
            }
        },
        "group_2": {
            "d_004": {
                "current": 12.3,
                "voltage": 225,
                "phase": 15
            },
            "d_005": {
                "current": 9.8,
                "voltage": 215,
                "phase": 75
            }
        }
    },
    "New York": {
        "group_1": {
            "d_006": {
                "current": 11.0,
                "voltage": 240,
                "phase": 20
            },
            "d_007": {
                "current": 14.5,
                "voltage": 235,
                "phase": 50
            }
        },
        "group_2": {
            "d_008": {
                "current": 13.2,
                "voltage": 245,
                "phase": 10
            },
            "d_009": {
                "current": 7.9,
                "voltage": 220,
                "phase": 80
            }
        }
    }
}
```

转换表达式使用样例中的配置，转换后的数据如下：

```json
[
  {
    "db": "test_input_json",
    "time": "2025-11-04 09:24:13.123",
    "location": "Los Angeles",
    "groupid": 1,
    "stb": "meters",
    "table": "d_001",
    "current": 10.5,
    "voltage": 220,
    "phase": 30
  },
  {
    "db": "test_input_json",
    "time": "2025-11-04 09:24:13.123",
    "location": "Los Angeles",
    "groupid": 1,
    "stb": "meters",
    "table": "d_002",
    "current": 15.2,
    "voltage": 230,
    "phase": 45
  },
  {
    "db": "test_input_json",
    "time": "2025-11-04 09:24:13.123",
    "location": "Los Angeles",
    "groupid": 1,
    "stb": "meters",
    "table": "d_003",
    "current": 8.7,
    "voltage": 210,
    "phase": 60
  },
  {
    "db": "test_input_json",
    "time": "2025-11-04 09:24:13.123",
    "location": "Los Angeles",
    "groupid": 2,
    "stb": "meters",
    "table": "d_004",
    "current": 12.3,
    "voltage": 225,
    "phase": 15
  },
  {
    "db": "test_input_json",
    "time": "2025-11-04 09:24:13.123",
    "location": "Los Angeles",
    "groupid": 2,
    "stb": "meters",
    "table": "d_005",
    "current": 9.8,
    "voltage": 215,
    "phase": 75
  },
  {
    "db": "test_input_json",
    "time": "2025-11-04 09:24:13.123",
    "location": "New York",
    "groupid": 1,
    "stb": "meters",
    "table": "d_006",
    "current": 11,
    "voltage": 240,
    "phase": 20
  },
  {
    "db": "test_input_json",
    "time": "2025-11-04 09:24:13.123",
    "location": "New York",
    "groupid": 1,
    "stb": "meters",
    "table": "d_007",
    "current": 14.5,
    "voltage": 235,
    "phase": 50
  },
  {
    "db": "test_input_json",
    "time": "2025-11-04 09:24:13.123",
    "location": "New York",
    "groupid": 2,
    "stb": "meters",
    "table": "d_008",
    "current": 13.2,
    "voltage": 245,
    "phase": 10
  },
  {
    "db": "test_input_json",
    "time": "2025-11-04 09:24:13.123",
    "location": "New York",
    "groupid": 2,
    "stb": "meters",
    "table": "d_009",
    "current": 7.9,
    "voltage": 220,
    "phase": 80
  }
]
```

需要注意的是，转换表达式中的 `$each` 函数用于遍历 JSON Object 的键值对，虽然文档中说明 `$each` 函数的返回值是一个数组，但如果只有一个键值对时，返回值会是一个单独的对象而不是数组。因此在使用 `$each` 函数时，需要在外层使用 `[]` 将结果强制转换为数组，以确保后续处理的一致性。
具体可以参考 [JSONata 文档](https://docs.jsonata.org/predicate#singleton-array-and-value-equivalence)。

#### SQL 转换示例

以参考配置样例为例，输入 [transformation 示例说明](#transformation-示例说明) 中的 JSON 数据，生成的 SQL 如下：

```sql
insert into `test_input_json`.`meters`(`tbname`,`ts`,`current`,`voltage`,`phase`,`location`,`groupid`)values
('d_001','2025-11-04T09:24:13.123Z',10.5,220,30,'Los Angeles',1)
('d_002','2025-11-04T09:24:13.123Z',15.2,230,45,'Los Angeles',1)
('d_003','2025-11-04T09:24:13.123Z',8.7,210,60,'Los Angeles',1)
('d_004','2025-11-04T09:24:13.123Z',12.3,225,15,'Los Angeles',2)
('d_005','2025-11-04T09:24:13.123Z',9.8,215,75,'Los Angeles',2)
('d_006','2025-11-04T09:24:13.123Z',11,240,20,'New York',1)
('d_007','2025-11-04T09:24:13.123Z',14.5,235,50,'New York',1)
('d_008','2025-11-04T09:24:13.123Z',13.2,245,10,'New York',2)
('d_009','2025-11-04T09:24:13.123Z',7.9,220,80,'New York',2)
```

生成 SQL 过程说明：

1. 写入 SQL 中的时间戳会根据配置的 `timeFormat` 和 `timezone` 进行解析和转换，最终以 RFC3339nano 格式拼接到 SQL 语句中。
2. 数据会按照 `db`、 `superTable` 、 `subTable` 、 获取到的 `fields`（`optional` 有可能设置为 `true`，因此获取的数据并不一定包含全部的 `fields`）进行分组，分组后按照时间升序排序后生成 SQL 语句。
3. 生成的 SQL 语句会尽量拼接成接近 1M 的 SQL 进行批量写入，以提高写入性能，如果数据量过大则会拆分成多条 SQL 语句进行写入。

#### dry run 模式

为了方便调试和验证 JSON 配置规则的正确性，taosAdapter 提供了 dry run 模式，可以通过在写入请求中添加查询参数 `dry_run=true` 来启用该模式。在 dry-run 模式下，taosAdapter 不会将数据写入 TDengine，而是返回转换后的 JSON 和生成的 SQL 语句供用户查看和验证。

示例请求：

```shell
curl -L 'http://localhost:6041/input_json/v1/rule1?dry_run=true' \
-u root:taosdata \
-d '{"time":"2025-11-04 09:24:13.123","Los Angeles":{"group_1":{"d_001":{"current":10.5,"voltage":220,"phase":30},"d_002":{"current":15.2,"voltage":230,"phase":45},"d_003":{"current":8.7,"voltage":210,"phase":60}},"group_2":{"d_004":{"current":12.3,"voltage":225,"phase":15},"d_005":{"current":9.8,"voltage":215,"phase":75}}},"New York":{"group_1":{"d_006":{"current":11.0,"voltage":240,"phase":20},"d_007":{"current":14.5,"voltage":235,"phase":50}},"group_2":{"d_008":{"current":13.2,"voltage":245,"phase":10},"d_009":{"current":7.9,"voltage":220,"phase":80}}}}'
```

返回结果示例：

```json
{
  "code": 0,
  "desc": "",
  "json": "[{\"current\":10.5,\"db\":\"test_input_json\",\"groupid\":1,\"location\":\"Los Angeles\",\"phase\":30,\"stb\":\"meters\",\"table\":\"d_001\",\"time\":\"2025-11-04 09:24:13.123\",\"voltage\":220},{\"current\":15.2,\"db\":\"test_input_json\",\"groupid\":1,\"location\":\"Los Angeles\",\"phase\":45,\"stb\":\"meters\",\"table\":\"d_002\",\"time\":\"2025-11-04 09:24:13.123\",\"voltage\":230},{\"current\":8.7,\"db\":\"test_input_json\",\"groupid\":1,\"location\":\"Los Angeles\",\"phase\":60,\"stb\":\"meters\",\"table\":\"d_003\",\"time\":\"2025-11-04 09:24:13.123\",\"voltage\":210},{\"current\":12.3,\"db\":\"test_input_json\",\"groupid\":2,\"location\":\"Los Angeles\",\"phase\":15,\"stb\":\"meters\",\"table\":\"d_004\",\"time\":\"2025-11-04 09:24:13.123\",\"voltage\":225},{\"current\":9.8,\"db\":\"test_input_json\",\"groupid\":2,\"location\":\"Los Angeles\",\"phase\":75,\"stb\":\"meters\",\"table\":\"d_005\",\"time\":\"2025-11-04 09:24:13.123\",\"voltage\":215},{\"current\":11,\"db\":\"test_input_json\",\"groupid\":1,\"location\":\"New York\",\"phase\":20,\"stb\":\"meters\",\"table\":\"d_006\",\"time\":\"2025-11-04 09:24:13.123\",\"voltage\":240},{\"current\":14.5,\"db\":\"test_input_json\",\"groupid\":1,\"location\":\"New York\",\"phase\":50,\"stb\":\"meters\",\"table\":\"d_007\",\"time\":\"2025-11-04 09:24:13.123\",\"voltage\":235},{\"current\":13.2,\"db\":\"test_input_json\",\"groupid\":2,\"location\":\"New York\",\"phase\":10,\"stb\":\"meters\",\"table\":\"d_008\",\"time\":\"2025-11-04 09:24:13.123\",\"voltage\":245},{\"current\":7.9,\"db\":\"test_input_json\",\"groupid\":2,\"location\":\"New York\",\"phase\":80,\"stb\":\"meters\",\"table\":\"d_009\",\"time\":\"2025-11-04 09:24:13.123\",\"voltage\":220}]",
  "sql": [
    "insert into `test_input_json`.`meters`(`tbname`,`ts`,`current`,`voltage`,`phase`,`location`,`groupid`)values('d_001','2025-11-04T09:24:13.123Z',10.5,220,30,'Los Angeles',1)('d_002','2025-11-04T09:24:13.123Z',15.2,230,45,'Los Angeles',1)('d_003','2025-11-04T09:24:13.123Z',8.7,210,60,'Los Angeles',1)('d_004','2025-11-04T09:24:13.123Z',12.3,225,15,'Los Angeles',2)('d_005','2025-11-04T09:24:13.123Z',9.8,215,75,'Los Angeles',2)('d_006','2025-11-04T09:24:13.123Z',11,240,20,'New York',1)('d_007','2025-11-04T09:24:13.123Z',14.5,235,50,'New York',1)('d_008','2025-11-04T09:24:13.123Z',13.2,245,10,'New York',2)('d_009','2025-11-04T09:24:13.123Z',7.9,220,80,'New York',2)"
  ]
}
```

- `code`：表示请求的状态码，`0` 表示成功，非 `0` 表示失败。
- `desc`：表示请求的描述信息，如果 `code` 非 `0`，则会包含错误信息。
- `json`：表示转换后的 JSON 数据。
- `sql`：表示生成的 SQL 语句数组。

### RESTful 接口

你可以使用任何支持 HTTP 协议的客户端通过访问 RESTful 接口地址 `http://<fqdn>:6041/rest/sql` 来写入数据到 TDengine 或从 TDengine 中查询数据。细节请参考 [REST API 文档](../../../10-developer-guide/08-connectors-reference/10-rest-api.mdx)。
