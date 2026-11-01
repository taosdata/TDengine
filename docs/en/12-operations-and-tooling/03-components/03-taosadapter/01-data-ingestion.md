---
title: Data Ingestion Interfaces
sidebar_label: Data Ingestion Interfaces
toc_max_heading_level: 4
---

import Prometheus from "../resources/_prometheus.mdx"
import CollectD from "../resources/_collectd.mdx"
import StatsD from "../resources/_statsd.mdx"
import Icinga2 from "../resources/_icinga2.mdx"
import TCollector from "../resources/_tcollector.mdx"

## Data Ingestion Interfaces

### WebSocket Interface

Through the WebSocket interface of taosAdapter, connectors in various languages can achieve SQL execution, schemaless writing, parameter binding, and data subscription functionalities. Refer to the [Development Guide](../../../10-developer-guide/01-connect/index.md#websocket-connection) for more details.

### InfluxDB v1 interface

taosAdapter is compatible with the InfluxDB v1 write and ping APIs.

#### InfluxDB v1 write interface

You can use any client that supports the HTTP protocol to write data in InfluxDB compatible format to TDengine by accessing the Restful interface URL `http://<fqdn>:6041/influxdb/v1/write`.

Supported InfluxDB parameters are as follows:

- `db` specifies the database name used by TDengine
- `precision` the time precision used by TDengine
- `u` TDengine username
- `p` TDengine password
- `ttl` the lifespan of automatically created subtables, determined by the TTL parameter of the first data entry in the subtable, which cannot be updated. For more information, please refer to the TTL parameter in the [table creation document](../../../05-tdengine-sql/02-ddl/02-table.md).
- `table_name_key` the custom tag key for subtable names. If set, the subtable name will use the value of this tag key

Note: Currently, InfluxDB's token authentication method is not supported, only Basic authentication and query parameter verification are supported.
Example: `curl --request POST http://127.0.0.1:6041/influxdb/v1/write?db=test --user "root:taosdata" --data-binary "measurement,host=host1 field1=2i,field2=2.0 1577836800000000000"`

#### InfluxDB v1 ping interface

taosAdapter is compatible with the InfluxDB v1 ping API, which can be used for health checks and connectivity probing. You can use any client that supports the HTTP protocol to access `http://<fqdn>:6041/influxdb/v1/ping`.

This interface supports the GET and HEAD methods and does not require authentication. Its behavior is consistent with the `/ping` API of InfluxDB 1.x:

- The response carries the `X-InfluxDB-Version` header, whose value is the taosAdapter version.
- By default, it returns `204 No Content` with an empty response body.
- When the value of the `verbose` query parameter is not empty and is neither `false` nor `0` (for example, `verbose=true`), it returns `200 OK` with a JSON body containing the version information, for example `{"version":"3.4.0.0"}`.

Example:

```shell
curl -i http://127.0.0.1:6041/influxdb/v1/ping
curl http://127.0.0.1:6041/influxdb/v1/ping?verbose=true
```

### OpenTSDB JSON and telnet format writing

You can use any client that supports the HTTP protocol to write data in OpenTSDB compatible format to TDengine by accessing the Restful interface URL `http://<fqdn>:6041/<APIEndPoint>`. EndPoint as follows:

```text
/opentsdb/v1/put/json/<db>
/opentsdb/v1/put/telnet/<db>
```

### collectd data writing

<CollectD />

### StatsD data writing

<StatsD />

### icinga2 OpenTSDB writer data writing

<Icinga2 />

### TCollector data writing

<TCollector />

### OpenMetrics data collection and writing

OpenMetrics is an open standard supported by CNCF (Cloud Native Computing Foundation) that focuses on standardizing the collection and transmission of metric data.
It serves as one of the core specifications for monitoring and observability systems in the cloud-native ecosystem.

Starting from version **3.3.7.0**, taosAdapter supports OpenMetrics v1.0.0 data collection and writing,
while maintaining compatibility with Prometheus 0.0.4 protocol to ensure seamless integration with the Prometheus ecosystem.

To enable OpenMetrics data collection and writing, follow these steps:

1. Enable taosAdapter configuration by setting `open_metrics.enable`
2. Configure OpenMetrics related settings
3. Restart taosAdapter service

### Supports Prometheus remote_read and remote_write

<Prometheus />

### node_exporter data collection and writing

Starting with version **3.3.7.0**, you can use the OpenMetrics plugin as a replacement for node_exporter to perform data collection and writing.

An exporter used by Prometheus that exposes hardware and operating system metrics from \*NIX kernels

- Enable configuration of taosAdapter node_exporter.enable
- Set the relevant configuration for node_exporter
- Restart taosAdapter

### JSON data writing

taosAdapter has supported writing JSON-formatted data to TDengine TSDB through the RESTful interface since version **3.4.0.0**. You can use any HTTP-compatible client to send JSON-formatted data to TDengine TSDB via the POST RESTful endpoint at `http://<fqdn>:6041/input_json/v1/{endpoint}`.

The required JSON format is an array containing multiple rows of data, with each row being a JSON object. Each JSON object corresponds to a single data record. Data extraction can be defined through configuration files. If the input JSON format does not meet the requirements, it can be transformed using [JSONata](https://jsonata.org/) expressions(supports JSONata version 1.5.4).

A sample configuration is as follows (default configuration file path: `/etc/taos/taosadapter.toml`):

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
timezone = "UTC"
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

After modifying the configuration file, you need to restart the taosAdapter service for the changes to take effect.

Complete configuration parameter description:

- `input_json.enable`: Enable or disable the JSON data writing function (default value: `false`).
- `input_json.rules`: An array defining JSON data writing rules, allowing multiple rules to be configured.
  - `endpoint`: Specifies the endpoint name for the RESTful interface, allowing only uppercase and lowercase letters, numbers, as well as `_` and `-`.
  - `db`: Specifies the target database name for writing data, prohibiting the inclusion of backticks `` ` ``.
  - `dbKey`: Specifies the key name in the JSON object used to represent the database name. Cannot be configured simultaneously with `db`.
  - `superTable`: Specifies the target supertable name for writing data, prohibiting the inclusion of backticks `` ` ``.
  - `superTableKey`: Specifies the key name in the JSON object used to represent the supertable name. Cannot be configured simultaneously with `superTable`.
  - `subTable`: Specifies the target subtable name for writing data.
  - `subTableKey`: Specifies the key name in the JSON object used to represent the subtable name. Cannot be configured simultaneously with `subTable`.
  - `timeKey`: Specifies the key name in the JSON object used to represent the timestamp. Defaults to `ts` if not set.
  - `timeFormat`: Specifies the format for time parsing. Effective when timeKey is set. See [Time Parsing Format Description](#time-parsing-format-description) for supported formats.
  - `timezone`: Specifies the timezone for the timestamp. Effective when timeKey is set. Uses IANA timezone format, defaulting to the timezone of the machine where taosAdapter is located.
  - `transformation`: Uses JSONata expressions to transform the input JSON data to meet TDengine TSDB's data writing requirements. For specific syntax, refer to the [JSONata documentation](https://jsonata.org/).
  - `fields`: Defines the list of fields to be written, with each field containing the following attributes:
    - `key`: Specifies the key name in the JSON object used to represent the field value. Must match the database field name and cannot contain backticks `` ` ``.
    - `optional`: Specifies whether the field is optional. The default value is `false`, indicating the field is mandatory. An error will occur if the `key` does not exist. If set to `true`, the field is optional, and no error will be generated if the `key` is missing; the column will be excluded from the generated SQL.

Before writing data, ensure that the target database and supertable have been created. Assume the following database and supertable have been created:

```sql
create database test_input_json;
create table test_input_json.meters (ts timestamp, current float, voltage int, phase float) tags (location nchar(64), `groupid` int);
```

Request example:

```shell
```shell
curl -L 'http://localhost:6041/input_json/v1/rule1' \
-u root:taosdata \
-d '{"time":"2025-11-04 09:24:13.123","Los Angeles":{"group_1":{"d_001":{"current":10.5,"voltage":220,"phase":30},"d_002":{"current":15.2,"voltage":230,"phase":45},"d_003":{"current":8.7,"voltage":210,"phase":60}},"group_2":{"d_004":{"current":12.3,"voltage":225,"phase":15},"d_005":{"current":9.8,"voltage":215,"phase":75}}},"New York":{"group_1":{"d_006":{"current":11.0,"voltage":240,"phase":20},"d_007":{"current":14.5,"voltage":235,"phase":50}},"group_2":{"d_008":{"current":13.2,"voltage":245,"phase":10},"d_009":{"current":7.9,"voltage":220,"phase":80}}}}'
```

Response example:

```json
{
  "code": 0,
  "desc": "",
  "affected": 9
}
```

- `code`: Indicates the status code of the request. `0` indicates success, while non-`0` indicates failure.
- `desc`: Provides a description of the request. If `code` is non-`0`, it includes error information.
- `affected`: Indicates the number of records successfully written.

Check the write result:

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

The data has been successfully written to TDengine TSDB. Since TDengine is configured with the UTC+8 timezone, the time is displayed as `2025-11-04 17:24:13.123`.

#### Time Parsing Format Description

The following time format presets are available:

- `unix`: Timestamp as integer or floating-point number in seconds
- `unix_ms`: Timestamp as integer or floating-point number in milliseconds
- `unix_us`: Timestamp as integer or floating-point number in microseconds
- `unix_ns`: Timestamp as integer or floating-point number in nanoseconds
- `ansic`: Time format as `Mon Jan _2 15:04:05 2006`
- `rubydate`: Time format as `Mon Jan 02 15:04:05 -0700 2006`
- `rfc822z`: Time format as `02 Jan 06 15:04 -0700`
- `rfc1123z`: Time format as `Mon, 02 Jan 2006 15:04:05 -0700`
- `rfc3339`: Time format as `2006-01-02T15:04:05Z07:00`
- `rfc3339nano`: Time format as `2006-01-02T15:04:05.999999999Z07:00`
- `stamp`: Time format as `Jan _2 15:04:05`
- `stampmilli`: Time format as `Jan _2 15:04:05.000`
- `datetime`: Time format as `2006-01-02 15:04:05.999999999`

If these presets do not meet your requirements, you can extend the format using the [strftime parsing method](https://pkg.go.dev/github.com/ncruces/go-strftime@v1.0.0).

#### Transformation Example Description

For complex JSON data formats, you can use the `transformation` configuration with JSONata expressions to transform the input JSON data to meet TDengine TSDB's data writing requirements. You can use the [JSONata online editor](https://try.jsonata.org/) to debug and validate JSONata expressions.

Assume the input JSON data is as follows:

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

Using the configuration from the example transformation expression, the converted data is as follows:

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

It should be noted that the `$each` function in the transformation expression is used to iterate over the key-value pairs of a JSON Object. Although the documentation states that the return value of the `$each` function is an array, when there is only one key-value pair, the return value will be a single object instead of an array. Therefore, when using the `$each` function, it is necessary to wrap the result with `[]` to forcibly convert it into an array, ensuring consistency in subsequent processing.

For details, please refer to the [JSONata documentation](https://docs.jsonata.org/predicate#singleton-array-and-value-equivalence).

#### SQL Conversion Example

Taking the reference configuration example, inputting the JSON data from the [Transformation Example Description](#transformation-example-description), the generated SQL is as follows:

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

SQL generation description:

1. The timestamp in the generated SQL will be parsed and converted according to the configured `timeFormat` and `timezone`, and ultimately formatted in RFC3339nano format when concatenated into the SQL statement.
2. Data will be grouped based on `db`, `superTable`, `subTable`, and the obtained `fields` (note that `optional` may be set to `true`, so the obtained data may not include all `fields`). After grouping, the data will be sorted in ascending time order before generating the SQL statement.
3. The generated SQL statements will be concatenated to approach approximately 1MB in size for batch writing to improve write performance. If the data volume is too large, it will be split into multiple SQL statements for writing.

#### Dry-run Mode

To facilitate debugging and validating the correctness of JSON configuration rules, taosAdapter provides a dry run mode. This mode can be enabled by adding the query parameter `dry_run=true` to the write request. In dry-run mode, taosAdapter does not write data to TDengine TSDB but instead returns the converted JSON and generated SQL statements for user review and validation.

Request example:

```shell
curl -L 'http://localhost:6041/input_json/v1/rule1?dry_run=true' \
-u root:taosdata \
-d '{"time":"2025-11-04 09:24:13.123","Los Angeles":{"group_1":{"d_001":{"current":10.5,"voltage":220,"phase":30},"d_002":{"current":15.2,"voltage":230,"phase":45},"d_003":{"current":8.7,"voltage":210,"phase":60}},"group_2":{"d_004":{"current":12.3,"voltage":225,"phase":15},"d_005":{"current":9.8,"voltage":215,"phase":75}}},"New York":{"group_1":{"d_006":{"current":11.0,"voltage":240,"phase":20},"d_007":{"current":14.5,"voltage":235,"phase":50}},"group_2":{"d_008":{"current":13.2,"voltage":245,"phase":10},"d_009":{"current":7.9,"voltage":220,"phase":80}}}}'
```

Response example:

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

- `code`: Indicates the status code of the request. `0` indicates success, while non-`0` indicates failure.
- `desc`: Provides a description of the request. If `code` is non-`0`, it includes error information.
- `json`: Represents the converted JSON data.
- `sql`: Represents the array of generated SQL statements.

### RESTful API

You can use any client that supports the HTTP protocol to write data to TDengine or query data from TDengine by accessing the RESTful interface URL `http://<fqdn>:6041/rest/sql`. For details, please refer to the [REST API documentation](../../../10-developer-guide/08-connectors-reference/10-rest-api.mdx).
