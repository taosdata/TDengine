---
toc_max_heading_level: 4
title: Data Ingestion
---

This article describes how to use taosX to ingest data into TDengine. For more information about taosX, see [taosX](../../reference/taosx/). You can also use taosExplorer to set up data ingestion. For more information, see [taosExplorer](../explorer/). For more information about installing taosX, see [Installation](../../get-started/).

## OPC-UA

### Parameters

| Name | Type    | Description                                   |  
|-----------------|--------|-----------------------------------------------------------------------------|
| interval | int    | Data collection interval, in seconds. Default 1                                   |
| concurrent | int    | Data collection concurrency. Default 1                                   |
| batch_size | int    | Batch size for uploading collected data. Default 100                                   |
| batch_timeout | int    | Timeout for uploading collected data, in seconds. Default 20                                   |
| connect_timeout | int    | Timeout for connections, in seconds. Default 10                                  |
| request_timeout | int    | Timeout for requests, in seconds. Default 10                                              |
| security_policy | string | OPC-UA security policy. Enter None, Basic128Rsa15, Basic256, or Basic256Sha256.                                  |
| security_mode   | string | OPC-UA security mode. Enter None, Sign, or SignAndEncrypt.                                                    |
| certificate     | string | Path to the `cert.pem` file. This option takes effect when `security_policy` and `security_mode` are not `None`.        |
| private_key     | string | Path to the `key.pem` file. This option takes effect when `security_policy` and `security_mode` are not `None`.
| csv_config_file | string | File containing OPC-UA data point and table configurations. Mutually exclusive with `csv_config_file`. If both options are included, only `csv_config_file` takes effect.|
| ua.nodes | string | Identifiers of OPC-UA nodes. This parameter must be used together with the `opc_table_config` parameter. Mutually exclusive with `csv_config_file`. If both options are included, only `csv_config_file` takes effect. Enter identifiers in the format <nodeid\>::<code\> where code is used to create subtables. |
| opc_table_config | string | Configuration for OPC-UA single-column mode. This parameter must be used together with the `ua.nodes` parameter. |
| debug | bool | Enables debug logs on the OPC connector. Default false |
| enable | bool | Enables raw data storage. Default false|
| path | string | Path at which raw data is stored. When `enable` is true, this parameter must be specified. |
| keep | int | Time to retain raw data. When `enable` is true, this parameter must be specified. |

Notes:
1. `opc_table_config` is configured as follows:

```json
{
    "stable_prefix": "meters", // Supertable prefix
    "column_configs":
    [
        {
            "column_name": "received_time", // Storage received time
            "column_type": "timestamp",
            "column_alias": "ts", // The column created for received time is named ts.
            "is_primary_key": true // The received time is the primary key.
        },
        {
            "column_name": "original_time",
            "column_type": "timestamp",
            "column_alias": "ts_2",
            "is_primary_key": false
        },
        {
            "column_name": "value", // Data column
            "column_alias": "valueaa", // Alias of data column
            "is_primary_key": false
        },
        {
            "column_name": "quality", // Quality column
            "column_type": "int",
            "column_alias": "quality11", // Quality column alias
            "is_primary_key": false
        }
    ]
}
```

### Example

1. Configuration with `ua.nodes` and `opc_table_config`:
The identifiers of the OPC nodes are ns=2;1=2 and ns=2;i=3. Data from these nodes is ingested into a TDengine supertable with the prefix meters. If the ns=2;i=2 node contains floating-point data, the meters_float supertable is created in TDengine. The received time in OPC is used as the timestamp and primary key in TDengine. The original timestamp column is retained as the ts_2 column. The data column is stored as valueaa and quality data is stored in the quality11 column.

```shell
taosx run \
    -f "opcua://uauser:uapass@localhost:4840?ua.nodes=ns=2;i=2::DSF1312,ns=2;i=3::DSF1313&opc_table_config={\"stable_prefix\": \"meters\", \"column_configs\": [{\"column_name\": \"received_time\", \"column_type\": \"timestamp\", \"column_alias\": \"ts\", \"is_primary_key\": true }, {\"column_name\": \"original_time\", \"column_type\": \"timestamp\", \"column_alias\": \"ts_2\", \"is_primary_key\": false }, {\"column_name\": \"value\", \"column_alias\": \"valueaa\", \"is_primary_key\": false }, {\"column_name\": \"quality\", \"column_type\": \"int\", \"column_alias\": \"quality11\", \"is_primary_key\": false } ] }" \
    -t "taos://tdengine:6030/opc"
 


```

2. CSV configuration:

```shell
taosx run -f "opcua://<server-info>?csv_config_file=@<file_path>" -t "taos+ws://tdengine:6041/opc"
```

### CSV configuration file template


## OPC-DA

### Parameters

| Name | Type    | Description                                   |
|-----------------|--------|-----------------------------------------------------------------------------|
| interval | int    | Data collection interval, in seconds. Default 1s                                   |
| concurrent | int    | Data collection concurrency, default 1                                   |
| batch_size | int    | Batch size for uploading collected data. Default 100                                   |
| batch_timeout | int    | Timeout for uploading collected data, in seconds. Default 20                                   |
| connect_timeout | int    | Timeout for connections, in seconds. Default 10                                  |
| request_timeout | int    | Timeout for requests, in seconds. Default 10                                              |
| csv_config_file | string | File containing OPC-UA data point and table configurations. Either `csv_config_file` or `ua.nodes` must be specified. For the configuration template, see OPC Requirements and Completion.
| da.tags | string | Identifiers of OPC-UA nodes. This parameter must be used together with the `opc_table_config` parameter. Mutually exclusive with `csv_config_file`. If both options are included, only `csv_config_file` takes effect. |
| opc_table_config | string | Configuration for OPC-UA single-column mode. This parameter must be used together with `da.tags`.
| debug | bool | Enables debug logs on the OPC connector. Default false |
| enable | bool | Enables raw data storage. Default false|
| path | string | Path at which raw data is stored. When `enable` is true, this parameter must be specified. |
| keep | int | Time to retain raw data. When `enable` is true, this parameter must be specified. |

### Example

```shell
taosx run \
    -f "opc+da://Matrikon.OPC.Simulation.1?nodes=localhost&da.tags=Random.Real8::tb3::c1::int"
    -t "taos://tdengine:6030/opc"
```

The results of this example operation are as follows:

The da.tags on the Matrikon.OPC.Simulation OPC-DA server are stored as Random.Real8 data with the `int` type. In TDengine, the corresponding table is named tb3. The column is named c1 and its type is `int`. The table is created automatically if it does not already exist.

### Troubleshooting

1. When using a native connection, jobs fail to start and the following error is displayed:
```text
Error: tmq to td task exec error

Caused by:
    0: Error occurred while creating a new object: [0x000B] Unable to establish connection
```
Solution:

Ensure that the FQDN of the destination TDengine cluster is accessible and that port 6030 on that FQDN is open.

2. When using a WebSocket connection, jobs fail to start and the following error is displayed:

```text
Error: tmq to td task exec error

Caused by:
    0: WebSocket internal error: IO error: failed to lookup address information: Temporary failure in name resolution
    1: IO error: failed to lookup address information: Temporary failure in name resolution
    2: failed to lookup address information: Temporary failure in name resolution
```

You can check the **Caused by** section to diagnose WebSocket connection errors. Several potential errors are listed as follows:

- **Temporary failure in name resolution**: Ensure that the IP address or FQDN of the target TDengine cluster are accessible.
- **IO error: Connection refused (os error 111)**: Ensure that the required port on the target TDengine cluster is open. Typically, port 6041 is used for this connection.
- **HTTP error: \* **: Ensure that your LSB, nginx, and proxy server configurations are correct and that you are connecting to the correct taosAdapter port.
- **WebSocket protocol error: Handshake not finished**: Ensure that the correct port is configured for the connection.

## PI System 

### PI System DSN

The DSN for PI System is configured as follows:

```shell
pi://[<username>:<password>@]PIServerName/AFDatabaseName?[TemplateForPIPoint][&TemplateForAFElement][&PointList][&<PISystemName=pisys>][&<MaxWaitLen>][&UpdateInterval]
```

The supported command-line parameters are described as follows. You must include at least one of the TemplateForPIPoint, TemplateForAFElement, and PointList parameters when configuring your PI System data source.
- PISystemName: (Optional) Specify the name of the PI System server. If you do not specify a value for PISystemName, the value of PIServerName is used.
- MaxWaitLen: (Optional) Specify the maximum buffering time for PI System data. Enter a value between 1 and 10000. The default value is 1000.
- UpdateInterval: (Optional) Specify the interval in milliseconds at which PI System data is obtained. Enter a value between 10 and 600000. The default value is 10000.
- TemplateForPIPoint: (Optional) Ingest PI System data in PI Point mode. This mode creates a subtable in TDengine for each attribution of an element in PI System. 
- TemplateForAFElement: (Optional) Ingest PI System data in AF Element mode. This mode creates a subtable for the set of attributions of an element in PI System.  
- PointList: (Optional) Ingest PI System data in Point List mode. This mode ingests data from the PI Points specified in a CSV file.


### Example

A PI System database Met1 is located on the server WIN-20A23UM12TN. Its template1 and template2 are ingested into TDengine in PI Point mode, template3 and template4 in AF Element mode, and `points.csv` in the `/home/` directory of the server is ingested in Point List mode. The PI System name is PI, the maximum buffering time is 1000, and the interval for obtaining data is 10,000 milliseconds. The data is written to the `pi` database in TDengine. The following command performs this configuration:

```shell
taosx run \
    -f "pi://WIN-2OA23UM12TN/Met1?TemplateForPIPoint=template1,template2&TemplateForAFElement=template3,template4" \
    -t "taos://tdengine:6030/pi"
```


### Troubleshooting

1. When using a native connection, jobs fail to start and the following error is displayed:
```text
Error: tmq to td task exec error

Caused by:
    0: Error occurred while creating a new object: [0x000B] Unable to establish connection
```
Solution:

Ensure that the FQDN of the destination TDengine cluster is accessible and that port 6030 on that FQDN is open.

2. When using a WebSocket connection, jobs fail to start and the following error is displayed:

```text
Error: tmq to td task exec error

Caused by:
    0: WebSocket internal error: IO error: failed to lookup address information: Temporary failure in name resolution
    1: IO error: failed to lookup address information: Temporary failure in name resolution
    2: failed to lookup address information: Temporary failure in name resolution
```

You can check the **Caused by** section to diagnose WebSocket connection errors. Several potential errors are listed as follows:

- **Temporary failure in name resolution**: Ensure that the IP address or FQDN of the target TDengine cluster are accessible.
- **IO error: Connection refused (os error 111)**: Ensure that the required port on the target TDengine cluster is open. Typically, port 6041 is used for this connection.
- **HTTP error: \* **: Ensure that your LSB, nginx, and proxy server configurations are correct and that you are connecting to the correct taosAdapter port.
- **WebSocket protocol error: Handshake not finished**: Ensure that the correct port is configured for the connection.


## InfluxDB

### Command-Line Parameters

The following command ingests data from InfluxDB into TDengine:

```bash
taosx run --from "<InfluxDB-DSN>" --to "<TDengine-DSN>"
```

The InfluxDB DSN complies with standard DSN conventions. Additional parameters are described as follows:
- version: (Mandatory) Specify whether the InfluxDB data source is running InfluxDB 1.x or 2.x.
- version = 1.x
  - username: (Mandatory) Specify an InfluxDB user with at least read permissions in the organization whose data you want to replicate.
  - password: (Mandatory) Specify the password of the InfluxDB user.
- version = 2.x
  - orgId: (Mandatory) Specify the InfluxDB organization ID.
  - token: (Mandatory) Specify the API token generated in InfluxDB. The token must have at least read permissions for the bucket whose data you want to replicate.
- bucket: (Mandatory) Specify the bucket whose data you want to replicate. You can replicate only one bucket per command.
- measurements: (Optional) Specify one or more measurements to replicate, separated by commas (,). If you do not specify a value, all measurements are replicated.
- beginTime: (Mandatory) Specify the starting timestamp for data replication in the YYYY-MM-DD'T'HH:MM:SS'Z' format. For example, 2023-06-01T00:00:00-0700 indicates midnight on June 1, 2023 in Pacific Daylight Time.
- endTime: (Optional) Specify the ending timestamp for data replication in the same format as `beginTime`. If you do not specify a value, data replication is performed continuously.
- readWindow: (Optional) Specify the read window. You can enter D (day), H (hour) or M (minute). The default value is M.
- tolerance: (Optional) Specify the tolerance time (unit is millisecond), only the integer value from 1 to 300000 is supported, if not specified, the default value is 10000

### Example

An InfluxDB server located at 192.168.1.10 has a bucket named `test_bucket`. The following configuration ingests data from `test_bucket` starting at midnight UTC on June 1, 2023 into a TDengine database named `test_db` through a taosKeeper instance located at 192.168.1.20.
```bash
# version = 1.x
taosx run \
  --from "influxdb+http://192.168.1.10:8086/?version=1.7&username=test&password=123456&bucket=test_bucket&measurements=&beginTime=2023-06-01T00:00:00+0800&readWindow=M&tolerance=10000" \
  --to "taos+http://192.168.1.20:6041/test_db" \
  -vv

# version = 2.x
taosx run \
  --from "influxdb+http://192.168.1.10:8086/?version=2.7&orgId=3233855dc7e37d8d&token=OZ2sB6Ie6qcKcYAmcHnL-i3STfLVg_IRPQjPIzjsAQ4aUxCWzYhDesNape1tp8IsX9AH0ld41C-clTgo08CGYA==&bucket=test_bucket&measurements=&beginTime=2023-06-01T00:00:00+0800&readWindow=M&tolerance=10000" \
  --to "taos+http://192.168.1.20:6041/test_db" \
  -vv
```

Because the `endTime` parameter is not specified, this job continues replicating data indefinitely.


## OpenTSDB

### Command-Line Parameters

The following command ingests OpenTSDB data into TDengine:

```bash
taosx run --from "<OpenTSDB-DSN>" --to "<TDengine-DSN>"
```

The OpenTSDB DSN complies with standard DSN conventions. Additional parameters are described as follows:
- metrics: (Optional) Specify the metrics that you want to ingest from OpenTSDB, separated by commas (,). If you do not specify this parameter, all metrics are ingested.
- beginTime: (Mandatory) Specify the starting timestamp for data replication in the YYYY-MM-DD'T'HH:MM:SS'Z' format. For example, 2023-06-01T00:00:00-0700 indicates midnight on June 1, 2023 in Pacific Daylight Time.
- endTime: (Optional) Specify the ending timestamp for data replication in the same format as `beginTime`. If you do not specify a value, data replication is performed continuously.
- readWindow: (Optional) Specify the read window. You can enter D (day), H (hour) or M (minute). The default value is M.
- tolerance: (Optional) Specify the tolerance time (unit is millisecond), only the integer value from 1 to 300000 is supported, if not specified, the default value is 10000.

### Example

An OpenTSDB server located at 192.168.1.10 has metrics named `test_metric1` and `test_metric2`. The following configuration ingests data from these metrics starting at midnight UTC on June 1, 2023 into a TDengine database named `test_db` through a taosKeeper instance located at 192.168.1.20.

```bash
taosx run \
  --from "opentsdb+http://192.168.1.10:4242/?metrics=test_metric1,test_metric2&beginTime=2023-06-01T00:00:00+0800&readWindow=M&tolerance=10000" \
  --to "taos+http://192.168.1.20:6041/test_db" \
  -vv
```

Because the `endTime` parameter is not specified, this job continues replicating data indefinitely.


## MQTT

You can consume data in JSON format from an MQTT server and store it in TDengine. The command is as follows:

```bash
taosx run --from "<MQTT-DSN>" --to "<TDengine-DSN>" --parser "@<parser-config-file-path>"
```

where:
- `--from`: Specify the DSN of the MQTT data source.
- `--to`: Specify the DSN of the TDengine cluster.
- `--parser`: Specify a JSON file that describes how to parse MQTT data with corresponding supertable names, subtable names, data column names and types, and tag column names and types in TDengine.

### MQTT DSN Configuration

The MQTT DSN complies with standard DSN conventions. Additional parameters are described as follows:
- topics: (Mandatory) Specify one or more MQTT topic names and maximum QoS values to ingest into TDengine. Use the `<topic>::<max-QoS>` format and separate multiple values with commas (,). You can also use the number sign (#) and plus sign (+) as defined in the MQTT protocol.
- version: (Optional) Specify the version of MQTT. Enter 3.1, 3.11, or 5.0. The default value is 3.1.
- clean_session: (Optional) Specify whether to retain session information from previous connections to the MQTT server. Enter true to delete session information or false to retain session information. The default value is true.
- client_id: (Mandatory) Specify the client ID for the connection between the MQTT client and server.
- keep_alive: (Optional) Specify how long the MQTT client waits for a response to PINGREG data that it sends to the MQTT server. If the MQTT server does not respond with a PINGREQ in the specified time, the connection is terminated. Enter a value in seconds. The default value is 60.
- ca: (Optional) Specify a CA certificate to use for SSL/TLS connections to the MQTT server. Enter an at sign (@) followed by the path to the certificate file, for example `@/home/admin/certs/ca.crt`.
- cert: (Optional) Specify a client certificate to use for SSL/TLS connections to the MQTT server. Enter an at sign (@) followed by the path to the certificate file, for example `@/home/admin/certs/client.crt`.
- cert_key: (Optional) Specify the client key to use for SSL/TLS connections to the MQTT server. Enter an at sign (@) followed by the path to the key file, for example `@/home/admin/certs/client.key`.
- log_level: (Optional) Specify the log level for the MQTT connection. Enter `error`, `warn`, `info`, `debug`, or `trace`. The default value is `info`.

An example is shown as follows:
```bash
mqtt://<username>:<password>@<mqtt-broker-ip>:8883?topics=testtopic/1::2&version=3.1&clean_session=true&log_level=info&client_id=taosdata_1234&keep_alive=60&ca=@/home/admin/certs/ca.crt&cert=@/home/admin/certs/client.crt&cert_key=@/home/admin/certs/client.key
```

### MQTT Parser

The format of the MQTT data parser is shown as follows. Note that it is separated into `parse` and `model` sections.

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

The parser configuration is described as follows:
- The `parse` section contains a JSON array consisting of JSON objects.
  - Each JSON object includes the `name`, `alias`, and `cast` fields.
  - The `name` field specifies how to obtain data from MQTT messages. If the MQTT message contains a simple JSON object, you can specify the object name. However, if the message contains a more complex JSON object, you must specify the JSON path, for example `$.data.city`.
  - The `alias` field specifies the name that TDengine uses for the MQTT data.
  - The `cast` field specifies the data type that TDengine uses for the MQTT data.
- The `model section describes how supertables, subtables, data columns, and tag columns are created in TDengine for the MQTT data.
  - The `using` field specifies a supertable for the MQTT data.
  - The `name` field specifies a subtable for the MQTT data. This field consists of a prefix followed by the value of the `alias` field specified in the `parse` section. Enter the prefix followed by the alias in brackets ({}), for example `d{id}`.
  - The `columns` field specifies the fields in the MQTT data that you want to parse into data columns in TDengine. Enter the value of the `alias` field specified in the `parse` section. Note that the first column must use the `TIMESTAMP` data type.
  - The `tags` field specifies the fields in the MQTT data that you want to parse into tag columns in TDengine. Enter the value of the `alias` field specified in the `parse` section.

### Example

An MQTT broker is located at 192.168.1.10 on port 1883. The user name for this broker is `admin` and the password is `123456`. The following configuration ingests data from this broker into the `test` database on a TDengine cluster whose taosAdapter instance is located at 192.168.1.20. The MQTT data format is as follows:

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

The following parser configuration specifies `meters` as the supertable; `d` as the subtable prefix; `ts`, `id`, `current`, `voltage`, and `phase` as the data columns; and `groupid` and `location` as the tag columns:
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

The following command ingests MQTT data into TDengine using the preceding parser configuration located at `/home/admin/parser.json`:

```bash
taosx run \
  -f "mqtt://admin:123456@192.168.1.10:1883?topics=testtopic/1::2&version=3.1&clean_session=true&log_level=info&client_id=1234&keep_alive=60" \
  -t "taos+ws://192.168.1.20:6041/test"
  --parser "@/home/admin/parser.json"
  --verbose
```

## Kafka

### Command-Line Parameters

You can consume data from Kafka and ingest it into TDengine. The command is as follows:
```shell
taosx run -f "<Kafka-DSN>" -t "<TDengine-DSN>"
```
or:
```shell
taosx run -f "<Kafka-DSN>" -t "<TDengine-DSN>" --parser "@<parser-config-file-path>"
```
where:
- `-f` or `--from`: Specify the Kafka DSN.
- `-t` or `-to`: Specify the TDengine DSN.
- `--parser`: Specify the parser configuration as a file or as a JSON string.
  
### Kafka DSN Configuration

| Parameter | Description | Mandatory | Default | Location | Example | 
|-----|---------------|----------|---------|---------|----------|
| group| Specify a consumer group. If you do not specify a consumer group, the consumer is not assigned to any group. | No | "" | Source | |
| topics | Specify topics to consume. All availability zones for the specified topics will be used unless otherwise configured with the `topic_partitions` parameter. | Either `topic` or `topic_partitions` must be specified. | None | Source |  topics=tp1,tp2 | 
| topic_partitions | Specify which topic partitions to ingest. Only specified partitions of identified topics are used. | Either `topic` or `topic_partitions` must be specified. | None | Source | topic_partitions=tp1:0..2,tp2:1 |
| fallback_offset | Specify a topic offset mode. Enter `Earliest`, `Latest`, or `ByTime(i64)`. - `Earliest` indicates the earliest available offset. - `Latest` indicates the latest available offset. - `ByTime(i64)` indicates all information before a specified time; enter a Unix time in millisecond precision. | No | Earliest | Source | fallback_offset=Earliest | 
| offset_storage | Specify a storage method for obtaining or submitting a group offset. Enter `Zookeeper` or `Kafka`. - `Zookeeper` uses ZooKeeper and is supported in Kafka 0.8.1 and later. - `Kafka` uses Kafka and is supported in Kafka 0.8.2 and later. These groups store offsets.  | No | Kafka | Source  | offset_storage=Kafka |
| timeout | Specify a timeout for obtaining valid data from a subscribed Kafka topic. | No | 500 | Source  | timeout=never | 
| use_ssl | Specify whether to use SSL. | No |  | Source  | |
| cert | Specify the path to your SSL certificate file. | No | | | Source  | |
| cert_key | Specify the path to your SSL certificate key file. | No | | Source  ||


### Example 1

A Kafka instance is located at 192.168.1.92. This configuration ingests data from the Kafka instance into a TDengine cluster located at 192.168.1.92 without using a parser.

1. Kafka configuration

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

2. TDengine configuration

```shell
drop database if exists kafka_to_taos;
create database if not exists kafka_to_taos precision 'ms';
use kafka_to_taos;
```

3. taosX configuration

```shell
taosx run -f "kafka://192.168.1.92:9092/?topics=tp1,tp2&timeout=5000" -t "taos://192.168.1.92:6030/kafka_to_taos" --parser "{\"parse\":{\"ts\":{\"as\":\"timestamp(ms)\"},\"topic\":{\"as\":\"varchar\",\"alias\":\"t\"},\"partition\":{\"as\":\"int\",\"alias\":\"p\"},\"offset\":{\"as\":\"bigint\",\"alias\":\"o\"},\"key\":{\"as\":\"binary\",\"alias\":\"k\"},\"value\":{\"as\":\"binary\",\"alias\":\"v\"}},\"model\":[{\"name\":\"t_{t}\",\"using\":\"kafka_data\",\"tags\":[\"t\",\"p\"],\"columns\":[\"ts\",\"o\",\"k\",\"v\"]}]}"
```

### Example 2

A Kafka instance is located at 192.168.1.92. This configuration ingests data from the Kafka instance into a TDengine cluster located at 192.168.1.92 and parses JSON data.

1. For the Kafka configuration, see Example 1.
2. For the TDengine configuration, see Example 1.
3. taosX configuration
   
```shell
taosx run -f "kafka://192.168.1.92:9092/?topics=tp1,tp2&timeout=5000" -t "taos://192.168.0.201:6030/kafka_to_taos" --parser "{\"parse\":{\"ts\":{\"as\":\"timestamp(ms)\"},\"topic\":{\"as\":\"varchar\",\"alias\":\"t\"},\"partition\":{\"as\":\"int\",\"alias\":\"p\"},\"offset\":{\"as\":\"bigint\",\"alias\":\"o\"},\"value\":{\"json\":[\"id::int\",\"message::binary\"]}},\"model\":[{\"name\":\"t_{t}\",\"using\":\"kafka_data\",\"tags\":[\"t\",\"p\"],\"columns\":[\"ts\",\"o\",\"id\",\"message\"]}]}"
```
