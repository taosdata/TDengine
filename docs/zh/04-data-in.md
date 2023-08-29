---
toc_max_heading_level: 4
title: 数据接入
---

本节讲述如何从各种数据源接入数据到 TDengine。

## OPC-UA

### 配置参数

| 参数名称 | 类型    | 描述                                   |  
|-----------------|--------|-----------------------------------------------------------------------------|
| interval | int    | 采集间隔（单位：秒），默认为1秒                                   |
| concurrent | int    | 采集器并发数，默认为1                                   |
| batch_size | int    | 采集器上报的批次点位数，默认为100                                   |
| batch_timeout | int    | 采集器上报的超时时间（单位：秒），默认为20秒                                   |
| connect_timeout | int    | 连接的超时时间（单位：秒），默认为10秒                                  |
| request_timeout | int    | 请求的超时时间（单位：秒），默认为10秒                                              |
| security_policy | string | OPC-UA连接安全策略（可配置为None/Basic128Rsa15/Basic256/Basic256Sha256）                                  |
| security_mode   | string | OPC-UA连接模式（可配置为None/Sign/SignAndEncrypt）                                                    |
| certificate     | string | cert.pem的路径。当安全模式或策略不是”无”时生效        |
| private_key     | string | key.pem的路径。 当安全模式或策略不是”无”时生效 |
| csv_config_file | string | 包含 OPC UA 的点位配置和表配置。与配置 csv_config_file 配置互斥，csv_config_file 优先生效|
| ua.nodes | string | OPC-UA 测点的 NodeID。和 opc_table_config 配置结合使用，两者需要同时配置。与配置 csv_config_file 配置互斥，csv_config_file 优先生效。配置格式为 <nodeid\>::<code\>，code 用于建子表。|
| opc_table_config | string | OPCUA 单列模式表配置。需要与 ua.nodes 配合使用。|
| debug | bool | 启用 OPC 连接器的 debug 日志。默认为 false。|
| enable | bool | 原始数据存储。默认为 false|
| path | string | 原始数据存储路径。enable 为 true 时必须配置。|
| keep | int | 原始数据保存天数。enable 为 true 时必须配置。|

补充：
1. opc_table_config 说明：

```json
{
    "stable_prefix": "meters", // 超级表前缀
    "column_configs":
    [
        {
            "column_name": "received_time", // 存储接收时间
            "column_type": "timestamp",
            "column_alias": "ts", // 接收时间建表列用列名为 ts
            "is_primary_key": true // 接收时间时间戳作为主键
        },
        {
            "column_name": "original_time",
            "column_type": "timestamp",
            "column_alias": "ts_2",
            "is_primary_key": false
        },
        {
            "column_name": "value", // 数据列
            "column_alias": "valueaa", // 数据列别名
            "is_primary_key": false
        },
        {
            "column_name": "quality", // 质量位列
            "column_type": "int",
            "column_alias": "quality11", // 质量位列别名
            "is_primary_key": false
        }
    ]
}
```

### 示例

1. 使用 ua.nodes 和 opc_table_config 的配置示例：
采集 nodeid 为 ns=2;i=2 和 ns=2;i=3 的点位，将其写入到集群 tdengine 的 opc 库中超级表前缀为 meters，如果 ns=2;i=2 的点位类型为 float 则会创建 meters_float 的超级表，超级表使用 opc 接收的数据作为时间戳索引列，并且保留原始时间戳列，原始时间戳列名为 ts_2,数据列存储为 valueaa，同时存储质量数据到 quality11 列。

```shell
taosx run \
    -f "opcua://uauser:uapass@localhost:4840?ua.nodes=ns=2;i=2::DSF1312,ns=2;i=3::DSF1313&opc_table_config={\"stable_prefix\": \"meters\", \"column_configs\": [{\"column_name\": \"received_time\", \"column_type\": \"timestamp\", \"column_alias\": \"ts\", \"is_primary_key\": true }, {\"column_name\": \"original_time\", \"column_type\": \"timestamp\", \"column_alias\": \"ts_2\", \"is_primary_key\": false }, {\"column_name\": \"value\", \"column_alias\": \"valueaa\", \"is_primary_key\": false }, {\"column_name\": \"quality\", \"column_type\": \"int\", \"column_alias\": \"quality11\", \"is_primary_key\": false } ] }" \
    -t "taos://tdengine:6030/opc"
 


```

2. 使用 CSV 配置文件

```shell
taosx run -f "opcua://<server-info>?csv_config_file=@<file_path>" -t "taos+ws://tdengine:6041/opc"
```

### CSV 配置文件模板


## OPC-DA

### 配置参数

| 参数名称 | 类型    | 描述                                   |
|-----------------|--------|-----------------------------------------------------------------------------|
| interval | int    | 采集间隔（单位：秒），默认为1秒                                   |
| concurrent | int    | 采集器并发数，默认为1                                   |
| batch_size | int    | 采集器上报的批次点位数，默认为100                                   |
| batch_timeout | int    | 采集器上报的超时时间（单位：秒），默认为20秒                                   |
| connect_timeout | int    | 连接的超时时间（单位：秒），默认为10秒                                  |
| request_timeout | int    | 请求的超时时间（单位：秒），默认为10秒                                              |
| csv_config_file | string | 包含 OPC UA 的点位配置和表配置。与 ua.nodes 两者之间需要配置一个。CSV 的配置模版参考：OPC 需求汇总及完成现状 |
| da.tags | string | OPC-UA 测点的 NodeID。和 opc_table_config 配置结合使用，两者需要同时配置。与配置 csv_config_file 配置互斥，csv_config_file 优先生效。|
| opc_table_config | string | OPCUA 单列模式表配置。需要与 da.tags 配合使用|
| debug | bool | 启用 OPC 连接器的 debug 日志。默认为 false。|
| enable | bool | 原始数据存储。默认为 false|
| path | string | 原始数据存储路径。enable 为 true 时必须配置。|
| keep | int | 原始数据保存天数。enable 为 true 时必须配置。|

### 应用示例

```shell
taosx run \
    -f "opc+da://Matrikon.OPC.Simulation.1?nodes=localhost&da.tags=Random.Real8::tb3::c1::int"
    -t "taos://tdengine:6030/opc"
```

以上示例的执行结果：

采集 Matrikon.OPC.Simulation.1 服务器上 OPC DA 中 da.tags 为 Random.Real8的数据，数据类型为int，对应在 TDengine 中以表名为 tb3 ，列名为c1，列类型为 int 型 schema 来创建表（如果对应表已存在，则直接采集数据并写入）。

### 常见错误排查

(1) 如果使用原生连接，任务启动失败并打印如下错误：
```text
Error: tmq to td task exec error

Caused by:
    0: Error occurred while creating a new object: [0x000B] Unable to establish connection
```
解决方式：

检查目标端 TDengine 的 FQDN 是否联通及端口 6030 是否可正常访问。

(2) 如果使用 WebSocket 连接任务启动失败并打印如下错误：：

```text
Error: tmq to td task exec error

Caused by:
    0: WebSocket internal error: IO error: failed to lookup address information: Temporary failure in name resolution
    1: IO error: failed to lookup address information: Temporary failure in name resolution
    2: failed to lookup address information: Temporary failure in name resolution
```

使用 WebSocket 连接时可能遇到多种错误类型，错误信息可以在 ”Caused by“ 后查看，以下是几种可能的错误：

- "Temporary failure in name resolution": DNS 解析错误，检查目标端 TDengine的 IP 或 FQDN 是否能够正常访问。
- "IO error: Connection refused (os error 111)": 端口访问失败，检查目标端口是否配置正确或是否已开启和可访问（通常为6041端口）。
- "HTTP error: *": 可能连接到错误的 taosAdapter 端口或 LSB/Nginx/Proxy 配置错误。
- "WebSocket protocol error: Handshake not finished": WebSocket 连接错误，通常是因为配置的端口不正确。

## PI 

### PI DSN 配置

PI DSN 的完整配置如下：

```shell
pi://[<username>:<password>@]PIServerName/AFDatabaseName?[TemplateForPIPoint][&TemplateForAFElement][&PointList][&<PISystemName=pisys>][&<MaxWaitLen>][&UpdateInterval]
```

在 taosX CLI 运行时支持的参数如下，其中 TemplateForPIPoint、TemplateForAFElement、PointList 三个参数至少配置一项：
- PISystemName：选填，连接配置 PI 系统服务名，默认值与 PIServerName 一致
- MaxWaitLen：选填，数据最大缓冲条数，默认值为 1000 ,有效取值范围为 [1,10000]
- UpdateInterval：选填，PI System 取数据频率，默认值为 10000(毫秒：ms),有效取值范围为 [10,600000]
- TemplateForPIPoint：选填，使用 PI Point 模式将模板按照 element 的每个 Arrtribution 作为子表导入到 TDengine 
- TemplateForAFElement：选填，使用 AF Point 模式将模板按照 element 的 Attribution 集合作为一个子表导入到 TDengine 
- PointList：选填，使用 PointList 模式将指定csv文件中描述的点位信息在 PI 数据库中的数据导入到 TDengine


### 应用示例

将位于服务器 WIN-2OA23UM12TN 中的 PI 数据库 Met1，模板 template1、template2配置为 TemplateForPIPoint模式，模板 template3、template4 配置为 TemplateForAFElement 模式，服务器 /home/ 路径下的点位文件 points.csv 配置为 PointList 模式，连接配置 PI 系统服务名为 PI，数据最大缓冲条数为1000，PI System 取数据频率为10000ms，将该库中的数据同步到 服务器 tdengine 的 pi 库中。完整的示例如下：

```shell
taosx run \
    -f "pi://WIN-2OA23UM12TN/Met1?TemplateForPIPoint=template1,template2&TemplateForAFElement=template3,template4" \
    -t "taos://tdengine:6030/pi"
```


### 常见错误排查

(1) 如果使用原生连接，任务启动失败并打印如下错误：
```text
Error: tmq to td task exec error

Caused by:
    0: Error occurred while creating a new object: [0x000B] Unable to establish connection
```
解决方式：

检查目标端 TDengine 的 FQDN 是否联通及端口 6030 是否可正常访问。

(2) 如果使用 WebSocket 连接任务启动失败并打印如下错误：：

```text
Error: tmq to td task exec error

Caused by:
    0: WebSocket internal error: IO error: failed to lookup address information: Temporary failure in name resolution
    1: IO error: failed to lookup address information: Temporary failure in name resolution
    2: failed to lookup address information: Temporary failure in name resolution
```

使用 WebSocket 连接时可能遇到多种错误类型，错误信息可以在 ”Caused by“ 后查看，以下是几种可能的错误：

- "Temporary failure in name resolution": DNS 解析错误，检查目标端 TDengine的 IP 或 FQDN 是否能够正常访问。
- "IO error: Connection refused (os error 111)": 端口访问失败，检查目标端口是否配置正确或是否已开启和可访问（通常为6041端口）。
- "HTTP error: *": 可能连接到错误的 taosAdapter 端口或 LSB/Nginx/Proxy 配置错误。
- "WebSocket protocol error: Handshake not finished": WebSocket 连接错误，通常是因为配置的端口不正确。


## InfluxDB

### 命令行参数

将数据从 InfluxDB 同步至 TDengine 的命令，如下所示：

```bash
taosx run --from "<InfluxDB-DSN>" --to "<TDengine-DSN>"
```

其中，InfluxDB DSN 符合 DSN 的通用规则，这里仅对其特有的参数进行说明：
- version: 必填，InfluxDB 的版本，主要用于区分 1.x 与 2.x 两个版本，二者使用不同的认证参数；
- version = 1.x
  - username: 必填，InfluxDB 用户，该用户至少在该组织中拥有读取权限；
  - password: 必填，InfluxDB 用户的登陆密码；
- version = 2.x
  - orgId: 必填，InfluxDB 中的 Orgnization ID；
  - token: 必填，InfluxDB 中生成的 API token, 这个 token 至少要拥有以上 Bucket 的 Read 权限；
- bucket: 必填，InfluxDB 中的 Bucket 名称，一次只能同步一个 Bucket；
- measurements: 非必填，可以指定需要同步的多个 Measurements（英文逗号分割），未指定则同步全部；
- beginTime: 必填，格式为：YYYY-MM-DD'T'HH:MM:SS'Z', 时区采用 UTC 时区，例如：2023-06-01T00:00:00+0800, 即北京时间2023-06-01 00:00:00（东八区时间）；
- endTime: 非必填，可以不指定该字段或值为空，格式与beginTime相同；如果未指定，提交任务后，将持续进行数据同步；
- readWindow: 非必填，可以不指定该字段或值为空，可选项为D、H、M（天、时、分）；如果未指定，则默认按 M 拆分读取窗口。

### 示例

将位于 192.168.1.10 的 InfluxDB 中, Bucket 名称为 test_bucket, 从UTC时间2023年06月01日00时00分00秒开始的数据，通过运行在 192.168.1.20 上的 taoskeeper, 同步至 TDengine 的 test_db 数据库中，完整的命令如下所示：
```bash
# version = 1.x
taosx run \
  --from "influxdb+http://192.168.1.10:8086/?version=1.7&username=test&password=123456&bucket=test_bucket&measurements=&beginTime=2023-06-01T00:00:00+0800&readWindow=M" \
  --to "taos+http://192.168.1.20:6041/test_db" \
  -vv

# version = 2.x
taosx run \
  --from "influxdb+http://192.168.1.10:8086/?version=2.7&orgId=3233855dc7e37d8d&token=OZ2sB6Ie6qcKcYAmcHnL-i3STfLVg_IRPQjPIzjsAQ4aUxCWzYhDesNape1tp8IsX9AH0ld41C-clTgo08CGYA==&bucket=test_bucket&measurements=&beginTime=2023-06-01T00:00:00+0800&readWindow=M" \
  --to "taos+http://192.168.1.20:6041/test_db" \
  -vv
```

在这个命令中，未指定endTime, 所以任务会长期运行，持续同步最新的数据。


## OpenTSDB

### 命令行参数

将数据从 OpenTSDB 同步至 TDengine 的命令，如下所示：

```bash
taosx run --from "<OpenTSDB-DSN>" --to "<TDengine-DSN>"
```

其中，OpenTSDB DSN 符合 DSN 的通用规则，这里仅对其特有的参数进行说明：
- metrics: 非必填，可以指定需要同步的多个 Metrics（英文逗号分割），未指定则同步全部；
- beginTime: 必填，格式为：YYYY-MM-DD'T'HH:MM:SS'Z', 时区采用 UTC 时区，例如：2023-06-01T00:00:00+0800, 即北京时间2023-06-01 00:00:00（东八区时间）；
- endTime: 非必填，可以不指定该字段或值为空，格式与beginTime相同；如果未指定，提交任务后，将持续进行数据同步；
- readWindow: 非必填，可以不指定该字段或值为空，可选项为D、H、M（天、时、分）；如果未指定，则默认按分钟拆分读取窗口。

### 示例

将位于 192.168.1.10 的 OpenTSDB 中, Metric 名称为 test_metric1 与 test_metric2 的两个数据源, 从UTC时间2023年06月01日00时00分00秒开始的数据，通过运行在 192.168.1.20 上的 taoskeeper, 同步至 TDengine 的 test_db 数据库中，完整的命令如下所示：

```bash
taosx run \
  --from "opentsdb+http://192.168.1.10:4242/?metrics=test_metric1,test_metric2&beginTime=2023-06-01T00:00:00+0800&readWindow=M" \
  --to "taos+http://192.168.1.20:6041/test_db" \
  -vv
```

在这个命令中，未指定endTime, 所以任务会长期运行，持续同步最新的数据。


## MQTT

目前，MQTT 连接器仅支持从 MQTT 服务端消费 JSON 格式的消息，并将其同步至 TDengine. 命令如下所示：

```bash
taosx run --from "<MQTT-DSN>" --to "<TDengine-DSN>" --parser "@<parser-config-file-path>"
```

其中：
- `--from` 用于指定 MQTT 数据源的 DSN
- `--to` 用于指定 TDengine 的 DSN
- `--parser` 用于指定一个 JSON 格式的配置文件，该文件决定了如何解析 JSON 格式的 MQTT 消息，以及写入 TDengine 时的超级表名、子表名、字段名称和类型，以及标签名称和类型等。

### MQTT DSN 配置

MQTT DSN 符合 DSN 的通用规则，这里仅对其特有的参数进行说明：
- topics: 必填，用于配置监听的 MQTT 主题名称和连接器支持的最大 QoS, 采用 `<topic>::<max-Qos>` 的形式；支持配置多个主题，使用逗号分隔；配置主题时，还可以使用 MQTT 协议的支持的通配符#和+;
- version: 非必填，用于配置 MQTT 协议的版本，支持的版本包括：3.1/3.1.1/5.0, 默认值为3.1;
- clean_session: 非必填，用于配置连接器作为 MQTT 客户端连接至 MQTT 服务端时，服务端是否保存该会话信息，其默认值为 true, 即不保存会话信息；
- client_id: 必填，用于配置连接器作为 MQTT 客户端连接至 MQTT 服务端时的客户端 id;
- keep_alive: 非必填，用于配置连接器作为 MQTT 客户端，向 MQTT 服务端发出 PINGREG 消息后的等待时间，如果连接器在该时间内，未收到来自 MQTT 服务端的 PINGREQ, 连接器则主动断开连接；该配置的单位为秒，默认值为 60;
- ca: 非必填，用于指定连接器与 MQTT 服务端建立 SSL/TLS 连接时，使用的 CA 证书，其值为在证书文件的绝对路径前添加@, 例如：@/home/admin/certs/ca.crt;
- cert: 非必填，用于指定连接器与 MQTT 服务端建立 SSL/TLS 连接时，使用的客户端证书，其值为在证书文件的绝对路径前添加@, 例如：@/home/admin/certs/client.crt;
- cert_key: 非必填，用于指定连接器与 MQTT 服务端建立 SSL/TLS 连接时，使用的客户端私钥，其值为在私钥文件的绝对路径前添加@, 例如：@/home/admin/certs/client.key;
- log_level: 非必填，用于配置连接器的日志级别，连接器支持 error/warn/info/debug/trace 5种日志级别，默认值为 info.

一个完整的 MQTT DSN 示例如下：
```bash
mqtt://<username>:<password>@<mqtt-broker-ip>:8883?topics=testtopic/1::2&version=3.1&clean_session=true&log_level=info&client_id=taosdata_1234&keep_alive=60&ca=@/home/admin/certs/ca.crt&cert=@/home/admin/certs/client.crt&cert_key=@/home/admin/certs/client.key
```

### MQTT 连接器的解释器配置

连接器的解释器配置文件，即`--parser`配置项的参数，它的值为一个 JSON 文件，其配置可分为`parse`和`model`两部分，模板如下所示：

```json
{
  "parse": {
    "payload": {
      "json": [
        {
          "name": "ts",
          "alias": "ts",
          "cast": "TIMESTAMP"
        },
        ...
      ]
    }
  },
  "model": {
    "using": "<stable-name>",
    "name": "<subtable-prefix>{alias}",
    "columns": [ ... ],
    "tags": [ ... ]
  }
}
```

各字段的说明如下：
- parse 部分目前仅支持 json 一种 payload, json 字段的值是一个由 JSON Object 构成的 JSON Array:
  - 每个 JSON Ojbect 包括 name, alias, cast 三个字段；
  - name 字段用于指定如何从 MQTT 消息中提取字段，如果 MQTT 消息是一个简单的 JSON Object, 这里可以直接设置其字段名；如果 MQTT 消息是一个复杂的 JSON Object, 这里可以使用 JSON Path 提取字段，例如：`$.data.city`;
  - alias 字段用于命名 MQTT 消息中的字段同步至 TDengine 后使用的名称；
  - cast 字段用于指定 MQTT 消息中的字段同步至 TDengine 后使用的类型。
- model 部分用于设置 TDengine 超级表、子表、列和标签等信息：
  - using 字段用于指定超级表名称；
  - name 字段用于指定子表名称，它的值可以分为前缀和变量两部分，变量为 parse 部分设置的 alias 的值，需要使用{}, 例如：d{id}；
  - columns 字段用于设置 MQTT 消息中的哪些字段作为 TDengine 超级表中的列，取值为 parse 部分设置的 alias 的值；需要注意的是，这里的顺序会决定 TDengine 超级表中列的顺序，因此第一列必须为 TIMESTAMP 类型；
  - tags 字段用于设置 MQTT 消息中的哪些字段作为 TDengine 超级表中的标签，取值为 parse 部分设置的 alias 的值。

### 举例说明

在 192.168.1.10 的 1883 端口运行着一个 MQTT broker, 用户名、口令分别为admin, 123456; 现欲将其中的消息，通过运行在 192.168.1.20 的 taosadapter 同步至 TDengine 的 test 数据库中。MQTT 消息格式为：

```json
{
  "id": 1,
  "current": 10.77,
  "voltage": 222,
  "phase": 0.77,
  "groupid": 7,
  "location": "California.SanDiego"
}
```

MQTT 消息同步至 TDengine 时, 如果采用 meters 作为超级表名，前缀“d”拼接id字段的值作为子表名，ts, id, current, voltage, phase作为超级表的列，groupid, location作为超级表的标签，其解释器的配置如下：
```json
{
  "parse": {
    "payload": {
      "json": [
        {
          "name": "ts",
          "alias": "ts",
          "cast": "TIMESTAMP"
        },
        {
          "name": "id",
          "alias": "id",
          "cast": "INT"
        },
        {
          "name": "voltage",
          "alias": "voltage",
          "cast": "INT"
        },
        {
          "name": "phase",
          "alias": "phase",
          "cast": "FLOAT"
        },
        {
          "name": "current",
          "alias": "current",
          "cast": "FLOAT"
        },
        {
          "name": "groupid",
          "alias": "groupid",
          "cast": "INT"
        },
        {
          "name": "location",
          "alias": "location",
          "cast": "VARCHAR(20)"
        }
      ]
    }
  },
  "model": {
    "name": "d{id}",
    "using": "meters",
    "columns": [
      "ts",
      "id",
      "current",
      "voltage",
      "phase"
    ],
    "tags": [
      "groupid",
      "location"
    ]
  }
}
```

如果以上parser配置位于`/home/admin/parser.json`中，那么完整的命令如下所示：

```bash
taosx run \
  -f "mqtt://admin:123456@192.168.1.10:1883?topics=testtopic/1::2&version=3.1&clean_session=true&log_level=info&client_id=1234&keep_alive=60" \
  -t "taos+ws://192.168.1.20:6041/test"
  --parser "@/home/admin/parser.json"
  --verbose
```

## Kafka

### 命令行参数

taosx 支持从 Kafka 消费数据，写入 TDengine。命令如下所示：
```sehll
taosx run -f "<Kafka-DSN>" -t "<TDengine-DSN>"
```
或
```shell
taosx run -f "<Kafka-DSN>" -t "<TDengine-DSN>" --parser "@<parser-config-file-path>"
```
其中：
- -f或--from： Kafka 的 DSN
- -t或--to ：TDengine 的 DSN
- --parser ：一个 JSON 格式的配置文件，或JSON格式的字符串。
  
### Kafka DSN 配置

| 参数 | 说明 | 必填? | 缺省值 | 适用于 | 示例 | 
|-----|---------------|----------|---------|---------|----------|
| group| 消费者的group。允许组为空字符串，在这种情况下，生成的消费者将是无组的 | 否 | "" | 源端 | |
| topics | 指定要使用的主题。指定主题的所有可用分区都将被使用，除非在指定 topic_partitions 时被覆盖。| 该参数或topic_partitions必须至少指定一个，以便将主题分配给消费者。| None | 源端 |  topics=tp1,tp2 | 
| topic_partitions | 显式指定要使用的主题分区。只使用已标识主题的指定分区。 | 该参数或topics必须至少指定一个，以便将主题分配给消费者。 | None | 源端 | topic_partitions=tp1:0..2,tp2:1 |
| fallback_offset | topic偏移量时可能的值：- Earliest：接收最早的可用偏移量; - Latest：接收最近的偏移量; - ByTime(i64):用于请求在某一特定时间(ms)之前的所有消息;Unix时间戳(毫秒) | 否 | Earliest | 源端 | fallback_offset=Earliest | 
| offset_storage | 定义在获取或提交组偏移量时，要使用的可用存储：- Zookeeper：基于Zookeeper的存储(从kafka 0.8.1开始可用)；- Kafka：基于Kafka的存储(从Kafka 0.8.2开始可用)。这是组存储其偏移量的首选方法。  | 否 | Kafka | 源端  | offset_storage=Kafka |
| timeout | 从kafka订阅数据时，如果超时后没有获取到有效数据，退出 | 否 | 500 | 源端  | timeout=never | 
| use_ssl | 是否使用SSL认证 | 否 |  | 源端  | |
| cert | SSL证书的文件路径 | 否 | | | 源端  | |
| cert_key | SSL证书key的文件路径 | 否 | | 源端  ||


### 示例一

从192.168.1.92服务器的Kafka实例中消费数据，同步到192.168.1.92上的TDengine，不使用parser。

1. kafka

```shell
#!/bin/bash
KAFKA_HOME=/root/zyyang/kafka_2.13-3.1.0
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --topic tp1 --delete
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --topic tp2 --delete
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --topic tp1 --partitions 5 --replication-factor 1 --create
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --topic tp2 --partitions 1 --replication-factor 1 --create
$KAFKA_HOME/bin/kafka-console-producer.sh --bootstrap-server 127.0.0.1:9092 --topic tp1 << EOF
{"id": 1, "message": "hello"}
{"id": 2, "message": "hello"}
{"id": 3, "message": "hello"}
{"id": 4, "message": "hello"}
{"id": 5, "message": "hello"}
EOF
$KAFKA_HOME/bin/kafka-console-producer.sh --bootstrap-server 127.0.0.1:9092 --topic tp2 << EOF
{"id": 1, "message": "aaa"}
{"id": 2, "message": "aaa"}
{"id": 3, "message": "aaa"}
{"id": 4, "message": "aaa"}
{"id": 5, "message": "aaa"}
EOF
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --topic tp1 --describe
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --topic tp2 --describe
```

2. TDengine

```shell
drop database if exists kafka_to_taos;
create database if not exists kafka_to_taos precision 'ms';
use kafka_to_taos;
```

3. taosx

```shell
taosx run -f "kafka://192.168.1.92:9092/?topics=tp1,tp2&timeout=5000" -t "taos://192.168.1.92:6030/kafka_to_taos" --parser "{\"parse\":{\"ts\":{\"as\":\"timestamp(ms)\"},\"topic\":{\"as\":\"varchar\",\"alias\":\"t\"},\"partition\":{\"as\":\"int\",\"alias\":\"p\"},\"offset\":{\"as\":\"bigint\",\"alias\":\"o\"},\"key\":{\"as\":\"binary\",\"alias\":\"k\"},\"value\":{\"as\":\"binary\",\"alias\":\"v\"}},\"model\":[{\"name\":\"t_{t}\",\"using\":\"kafka_data\",\"tags\":[\"t\",\"p\"],\"columns\":[\"ts\",\"o\",\"k\",\"v\"]}]}"
```

### 示例2

从192.168.1.92服务器的Kafka实例中消费数据，同步到192.168.1.92上的TDengine，使用parser解析value中的JSON数据。

1. kafka，同“示例1”
2. TDengine，同“示例1”
3. Taosx
   
```shell
taosx run -f "kafka://192.168.1.92:9092/?topics=tp1,tp2&timeout=5000" -t "taos://192.168.0.201:6030/kafka_to_taos" --parser "{\"parse\":{\"ts\":{\"as\":\"timestamp(ms)\"},\"topic\":{\"as\":\"varchar\",\"alias\":\"t\"},\"partition\":{\"as\":\"int\",\"alias\":\"p\"},\"offset\":{\"as\":\"bigint\",\"alias\":\"o\"},\"value\":{\"json\":[\"id::int\",\"message::binary\"]}},\"model\":[{\"name\":\"t_{t}\",\"using\":\"kafka_data\",\"tags\":[\"t\",\"p\"],\"columns\":[\"ts\",\"o\",\"id\",\"message\"]}]}"
```
