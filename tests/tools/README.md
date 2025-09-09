# taosx-tools

## MQTT publish tool

```
$ cargo run --bin mqtt_pub -- -h

Usage: mqtt_pub [OPTIONS] --schema <SCHEMA> --topic <TOPIC>

Options:
  -f, --schema <SCHEMA>
      --host <BROKER_HOST>       [default: localhost]
      --port <BROKER_PORT>       [default: 1883]
  -u, --username <USERNAME>
  -p, --password <PASSWORD>
  -k, --keep_alive <KEEP_ALIVE>  [default: 5s]
  -c, --client_id <CLIENT_ID>    [default: mqtt_pub_tool_1WgBwKACZL]
  -t, --topic <TOPIC>
  -q, --qos <QOS>                [default: 0]
  -l, --perallel <PERALLEL>      [default: 12]
      --interval <INTERVAL>      [default: 100ms]
      --stdin
      --compress <COMPRESS>      payload compression, support: gzip, lz4, snappy, zstd
      --encoding <ENCODING>      payload encoding, support GBK, GB18030, BIG5
  -h, --help                     Print help
```

## Kafka publish tool

```
$ cargo run --bin kafka_pub -- -h

Usage: kafka_pub [OPTIONS] --schema <SCHEMA> --topic <TOPIC>

Options:
  -f, --schema <SCHEMA>
  -s, --servers <SERVERS>    [default: localhost:9092]
  -t, --topic <TOPIC>
  -l, --perallel <PARALLEL>  [default: 12]
      --interval <INTERVAL>  [default: 100ms]
      --stdin
      --compress <COMPRESS>  payload compression, support: gzip, lz4, snappy, zstd
      --encoding <ENCODING>  payload encoding, support GBK, GB18030, BIG5
  -h, --help                 Print help
```

## schema file syntax

1. object type

```toml
type = "object"

# 时间戳字段
[properties.ts]
type = "timestamp"
# 开始时间戳
start_time = 2027-10-01T00:00:00.888888888
# 时间戳递增的单位，ns/ms/s 代表每条数据递增 1ns/1ms/1s
precision = "ns"

# 数值类型字段，字段名 device
[properties.device]
type = "number"
# 数值范围
range = { min = 1, max = 10 }

#数值类型字段，字段名 a1
[properties.a1]
type = "number"
# 数值为固定值
fixed = 6

# 字符串类型字段，字段名 a2
[properties.a2]
type = "string"
# 字符串生成长度范围
length = { range = { min = 1, max = 10 } }

# 字符串类型字段，字段名 a3
[properties.a3]
type = "string"
# 字符串生成固定长度为 6
length = { fixed = 6 }
# 字符串生成时使用的字符集，默认为字母数字
charset = "abcde"

# 字符串类型字段，字段名 a4
[properties.a4]
type = "string"
# 字符串生成固定长度为 6
fixed = "abcde"

# 浮点数类型字段，字段名 a5
[properties.a5]
type = "float"
# 固定值 6.4
fixed = 6.4

# 浮点数类型字段，字段名 a6
[properties.a6]
type = "float"
# 浮点数生成范围
range = { min = 0.2, max = 0.5 }

# Option 类型字段，字段名 a7
# option 类型表示随机生成此字段或不生成此字段
[properties.a7]
type = "option"

# a7 字段 option 包含的值类型
[properties.a7.value]
type = "string"
# 字符串生成固定长度为 6
fixed = "abcde"

# 布尔类型字段，字段名 a8，随机生成 true/false
[properties.a8]
type = "bool"

# 布尔类型字段，字段名 a8
[properties.a9]
type = "bool"
# 只生成固定值 false
fixed = false
```

2. Array type

```toml
type = "array"

# 数组元素类型
[elements]
type = "string"
fixed = "abcde"
```

# Performance Testing Report

perf_report generates static HTML pages for taosX performance testing.

Input directory must contain:
- perf_cases.toml
- All CSV files declared in perf_cases.toml
## Run
1. From workspace root:

```
$ cargo run -p taosx-tools --bin perf-report -- -i tests/tools/sample -o tests/tools/dist/perf_report
```

Or run inside the sub crate:

```
$ cd tests/tools
$ cargo run --bin perf-report -- -i ./sample -o ./dist/perf_report
```

1. Open the index.html in the output directory with a browser.
