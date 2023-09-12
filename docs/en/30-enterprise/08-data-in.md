---
toc_max_heading_level: 4
title: Data Ingestion
---

This section describes how to access data from various data sources to TDengine using taosX's command line mode. For command line arguments to taosX, see [taosX](../../reference/taosx). You can also use taos-explorer's visual interface for data ingestion, please refer to [Visual Management](../explorer). For service installation and deployment, please refer to [Installation and Deployment](../../get-started).

## OPC-UA

### Configuration Parameters

| Name | Type    | Description                                   |  
|-----------------|--------|-----------------------------------------------------------------------------|
| interval | int    | Collection interval (seconds), default 1s                                   |
| concurrent | int    | Collection concurrency, default 1                                   |
| batch_size | int    | Collection data points per batch, default 100                                   |
| batch_timeout | int    | Timeout for collectors (seconds), default 20s                                   |
| connect_timeout | int    | Timeout for connections (seconds), default 10s                                  |
| request_timeout | int    | Timeout for requests (seconds), default 10s                                              |
| security_policy | string | OPC-UA connection security policy (None/Basic128Rsa15/Basic256/Basic256Sha256)                                  |
| security_mode   | string | OPC-UA connection mode (None/Sign/SignAndEncrypt)                                                    |
| certificate     | string | cert.pem path Takes effect when connection mode and policy are not none        |
| private_key     | string | key.pem path Takes effect when connection mode and policy are not none |
| csv_config_file | string | Contains OPC UA data point and table configuration. Mutually exclusive with configure csv_config_file configuration, csv_config_file takes precedence|
| ua.nodes | string | OPC-UA node NodeID. Used in conjunction with opc_table_config configuration, both need to be configured at the same time. Mutually exclusive with configure csv_config_file configuration, csv_config_file takes precedence. Configuration format is <nodeid\>::<code\>, code is used to build sub-tables. |
| opc_table_config | string | OPCUA single-column table configuration. Must be used with ua.nodes. |
| debug | bool | Whether to enable OPC connector debug logs. The default value is false. |
| enable | bool | Whether to store raw data. The default value is false|
| path | string | Raw data storage path. Must be configured when enable is set to true. |
| keep | int | Days to store raw data. Must be configured when enable is set to true. |

Notes:
1. opc_table_config:

```json
{
    "stable_prefix": "meters", // Supertable prefix
    "column_configs":
    [
        {
            "column_name": "received_time", // Storage received time
            "column_type": "timestamp",
            "column_alias": "ts", // Receive the time to build the table column with the column name ts
            "is_primary_key": true // Receive time timestamp as primary key
        },
        {
            "column_name": "original_time",
            "column_type": "timestamp",
            "column_alias": "ts_2",
            "is_primary_key": false
        },
        {
            "column_name": "value", // Data column
            "column_alias": "valueaa", // Data column alias
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

### Examples

1. Example configuration using ua.nodes and opc_table_config:
Capture the points with nodeid ns=2;i=2 and ns=2;i=3, write them to the opc library of cluster tdengine with supertable prefixed with meters, and create a supertable of meters_float if the points with ns=2;i=2 are of type float. The supertable uses the data received by opc as the timestamp index column and The super table uses the data received by opc as the timestamp index column and keeps the original timestamp column, the original timestamp column is named ts_2, the data column is stored as valueaa, and the quality data is stored in the quality11 column.

```shell
taosx run \
    -f "opcua://uauser:uapass@localhost:4840?ua.nodes=ns=2;i=2::DSF1312,ns=2;i=3::DSF1313&opc_table_config={\"stable_prefix\": \"meters\", \"column_configs\": [{\"column_name\": \"received_time\", \"column_type\": \"timestamp\", \"column_alias\": \"ts\", \"is_primary_key\": true }, {\"column_name\": \"original_time\", \"column_type\": \"timestamp\", \"column_alias\": \"ts_2\", \"is_primary_key\": false }, {\"column_name\": \"value\", \"column_alias\": \"valueaa\", \"is_primary_key\": false }, {\"column_name\": \"quality\", \"column_type\": \"int\", \"column_alias\": \"quality11\", \"is_primary_key\": false } ] }" \
    -t "taos://tdengine:6030/opc"
 


```

2. Using CSV configuration file

```shell
taosx run -f "opcua://<server-info>?csv_config_file=@<file_path>" -t "taos+ws://tdengine:6041/opc"
```

### CSV configuration file template


## OPC-DA

### Configuration Parameters

| Name | Type    | Description                                   |
|-----------------|--------|-----------------------------------------------------------------------------|
| interval | int    | Collection interval (seconds), default 1s                                   |
| concurrent | int    | Collection concurrency, default 1                                   |
| batch_size | int    | Collection data points per batch, default 100                                   |
| batch_timeout | int    | Timeout for collectors (seconds), default 20s                                   |
| connect_timeout | int    | Timeout for connections (seconds), default 10s                                  |
| request_timeout | int    | Timeout for requests (seconds), default 10s                                              |
| csv_config_file | string | Contains OPC UA data point and table configuration. Either csv_config_file or ua.nodes must be used. CSV Configuration Template Reference: OPC Requirements Summary and Status of Completion |
| da.tags | string | OPC-UA node NodeID. Used in conjunction with opc_table_config configuration, both need to be configured at the same time. Mutually exclusive with configure csv_config_file configuration, csv_config_file takes precedence. |
| opc_table_config | string | OPCUA single-column table configuration. Must be used with da.tags|
| debug | bool | Whether to enable OPC connector debug logs. The default value is false. |
| enable | bool | Whether to store raw data. The default value is false|
| path | string | Raw data storage path. Must be configured when enable is set to true. |
| keep | int | Days to store raw data. Must be configured when enable is set to true. |

### Usage examples

```shell
taosx run \
    -f "opc+da://Matrikon.OPC.Simulation.1?nodes=localhost&da.tags=Random.Real8::tb3::c1::int"
    -t "taos://tdengine:6030/opc"
```

The result of the above example execution:

Capture data from the OPC DA on the Matrikon.OPC.Simulation.1 server with da.tags of Random.Real8 and a data type of int, which corresponds to the creation of a table in TDengine with the table name tb3, the column name c1, and a schema of type int (if the corresponding table already exists, the data is captured directly and written to the table). (if the corresponding table already exists, the data will be collected and written directly).

### Troubleshooting common errors

(1) If a native connection is used, the task fails to start and reports the following error:
```text
Error: tmq to td task exec error

Caused by:
    0: Error occurred while creating a new object: [0x000B] Unable to establish connection
```
Solution:

Check whether the FQDN of the target TDengine is connected and whether port 6030 can be accessed normally.

(2) If you use a WebSocket connection the task fails to start and reports the following error:

```text
Error: tmq to td task exec error

Caused by:
    0: WebSocket internal error: IO error: failed to lookup address information: Temporary failure in name resolution
    1: IO error: failed to lookup address information: Temporary failure in name resolution
    2: failed to lookup address information: Temporary failure in name resolution
```

There are several types of errors that can be encountered when connecting using a WebSocket. The error message can be viewed after "Caused by", the following are a few possible errors:

- "Temporary failure in name resolution": DNS resolution error, check if the IP or FQDN of the target TDengine cluster is accessible.
- "IO error: Connection refused (os error 111)": Port access failed, check if the target port (typically 6041) is configured correctly or is enabled and accessible.
- "HTTP error: *": Possible connection to wrong taosAdapter port or LSB/Nginx/Proxy configuration error.
- "WebSocket protocol error: Handshake not finished": WebSocket connection error, usually due to an incorrectly configured port.

## PI 

### PI DSN Configuration

The PI DSN configuration is as follows:

```shell
pi://[<username>:<password>@]PIServerName/AFDatabaseName?[TemplateForPIPoint][&TemplateForAFElement][&PointList][&<PISystemName=pisys>][&<MaxWaitLen>][&UpdateInterval]
```

The following parameters are supported in the taosX CLI runtime, with at least one of the TemplateForPIPoint, TemplateForAFElement, and PointList parameters configured:
- PISystemName: optional, connection configuration PI system service name, the default value is the same as PIServerName.
- MaxWaitLen: optional, the maximum number of data buffer bars, the default value is 1000, the valid range is [1,10000].
- UpdateInterval: optional, the frequency of data retrieval by PI System, the default value is 10000 (milliseconds: ms), the valid range is [10,600000].
- TemplateForPIPoint: optional, use PI Point mode to import templates into TDengine according to each Arrtribution of an element as a sub-table 
- TemplateForAFElement: optional, use AF Point mode to import the template into TDengine as a sub-table according to the element's Attribution collection 
- PointList: optional, use PointList mode to import the point information described in the specified csv file in the PI database to TDengine.


### Usage examples

Configure the PI database Met1, template template1, template2 as TemplateForPIPoint mode, template template3, template4 as TemplateForAFElement mode, and the points file points.csv under the path of server WIN-2OA23UM12TN as PointList mode, connect and configure the PI System service name as PI, the maximum buffer bar of data as 1000, and the data fetching frequency of PI System as 10000ms. points.csv under the path of home/ is configured as PointList mode, the connection is configured with the PI System service name PI, the maximum number of data buffer entries is 1000, and the frequency of data fetching by the PI System is 10000ms, and the data in the library is synchronized to the pi library of the server tdengine. The complete example is as follows.

```shell
taosx run \
    -f "pi://WIN-2OA23UM12TN/Met1?TemplateForPIPoint=template1,template2&TemplateForAFElement=template3,template4" \
    -t "taos://tdengine:6030/pi"
```


### Troubleshooting common errors

(1) If a native connection is used, the task fails to start and reports the following error:
```text
Error: tmq to td task exec error

Caused by:
    0: Error occurred while creating a new object: [0x000B] Unable to establish connection
```
Solution:

Check whether the FQDN of the target TDengine is connected and whether port 6030 can be accessed normally.

(2) If you use a WebSocket connection the task fails to start and reports the following error:

```text
Error: tmq to td task exec error

Caused by:
    0: WebSocket internal error: IO error: failed to lookup address information: Temporary failure in name resolution
    1: IO error: failed to lookup address information: Temporary failure in name resolution
    2: failed to lookup address information: Temporary failure in name resolution
```

There are several types of errors that can be encountered when connecting using a WebSocket. The error message can be viewed after "Caused by", the following are a few possible errors:

- "Temporary failure in name resolution": DNS resolution error, check if the IP or FQDN of the target TDengine cluster is accessible.
- "IO error: Connection refused (os error 111)": Port access failed, check if the target port (typically 6041) is configured correctly or is enabled and accessible.
- "HTTP error: *": Possible connection to wrong taosAdapter port or LSB/Nginx/Proxy configuration error.
- "WebSocket protocol error: Handshake not finished": WebSocket connection error, usually due to an incorrectly configured port.


## InfluxDB

### Command Line Parameters

The command to synchronize data from InfluxDB to TDengine is shown below:

```bash
taosx run --from "<InfluxDB-DSN>" --to "<TDengine-DSN>"
```

The InfluxDB DSN conforms to the general rules for DSNs, and only the parameters specific to it are described here:
- version: Required, the version of InfluxDB, mainly used to distinguish between 1.x and 2.x versions, which use different authentication parameters;
- version = 1.x
  - username: Required, InfluxDB user that has read access at least in this organization;
  - password: Required, the login password for the InfluxDB user;
- version = 2.x
  - orgId: Required, the Organization ID in InfluxDB;
  - token: Mandatory, the API token generated in InfluxDB, this token must have at least the Read permission of the above Bucket;
- bucket: Required, the name of the Bucket in InfluxDB, only one Bucket can be synchronized at a time;
- measurements: Non-required, you can specify multiple Measurements to be synchronized (English comma-separated), unspecified synchronizes all;
- beginTime: Required, format: YYYY-MM-DD'T'HH:MM:SS'Z', time zone adopts UTC time zone, e.g.: 2023-06-01T00:00:00+0800, i.e. Beijing time 2023-06-01 00:00:00 (East 8 time zone);
- endTime: Non-required, the field can be left unspecified or the value can be empty, in the same format as beginTime; if unspecified, the data synchronization will continue after the task is submitted;
- readWindow: Non-required, the field can be left unspecified or the value is empty, the options are D, H, M (days, hours, minutes); if not specified, the default is to split the read window by M.

### Examples

Synchronize the data from InfluxDB located at 192.168.1.10, Bucket name test_bucket, starting at 00:00:00 UTC on June 01, 2023, to TDengine's test_db via taoskeeper running on 192.168.1.20 database of TDengine, the complete command is shown below:
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

In this command, endTime is not specified, so the task will run for a long time, continuously synchronizing the latest data.


## OpenTSDB

### Command Line Parameters

The command to synchronize data from OpenTSDB to TDengine is shown below:

```bash
taosx run --from "<OpenTSDB-DSN>" --to "<TDengine-DSN>"
```

The OpenTSDB DSN conforms to the general rules for DSNs, and only the parameters specific to it are described here:
- metrics: Non-required, you can specify multiple Metrics to be synchronized (English comma-separated), unspecified synchronizes all;
- beginTime: Required, format: YYYY-MM-DD'T'HH:MM:SS'Z', time zone adopts UTC time zone, e.g.: 2023-06-01T00:00:00+0800, i.e. Beijing time 2023-06-01 00:00:00 (East 8 time zone);
- endTime: Non-required, the field can be left unspecified or the value can be empty, in the same format as beginTime; if unspecified, the data synchronization will continue after the task is submitted;
- readWindow: Non-required, the field can be left unspecified or the value is empty, the options are D, H, M (days, hours, minutes); if not specified, the default is to split the read window by minutes.

### Examples

Synchronize the data from two data sources with metric names test_metric1 and test_metric2 in OpenTSDB located at 192.168.1.10 from 00:00:00 UTC on June 01, 2023 to TDengine's test_db database using taoskeeper running on 192.168.1.20 with the following commands The complete command to synchronize to the test_db database of TDengine is shown below:

```bash
taosx run \
  --from "opentsdb+http://192.168.1.10:4242/?metrics=test_metric1,test_metric2&beginTime=2023-06-01T00:00:00+0800&readWindow=M" \
  --to "taos+http://192.168.1.20:6041/test_db" \
  -vv
```

In this command, endTime is not specified, so the task will run for a long time, continuously synchronizing the latest data.


## MQTT

Currently, the MQTT Connector only supports consuming JSON-formatted messages from the MQTT server and synchronizing them to the TDengine. The commands are shown below:

```bash
taosx run --from "<MQTT-DSN>" --to "<TDengine-DSN>" --parser "@<parser-config-file-path>"
```

The parameters are:
- `--from` specifies the DSN of the MQTT data source
- `--to` specifies the DSN of the TDengine cluster
- `--parser` specifies a JSON-formatted configuration file that determines how to parse JSON-formatted MQTT messages, as well as the super table names, sub-table names, field names and types, and label names and types when writing to TDengine.

### MQTT DSN Configuration

MQTT DSN conforms to the general rules for DSNs, and only the parameters specific to it are described here:
- topics: Mandatory, used to configure the name of the MQTT topic to listen on and the maximum QoS supported by the connector, in the form of `<topic>::<max-Qos>`; multiple topics can be configured, separated by commas; when configuring a topic, you can also use the wildcard characters # and + supported by the MQTT protocol.
- version: Non-required, used to configure the version of MQTT protocol, supported versions include: 3.1/3.1.1/5.0, the default value is 3.1;
- clean_session: Non-required, used to configure the connector as an MQTT client to connect to the MQTT server, the server whether to save the session information, the default value is true, that is, does not save the session information;
- client_id: Mandatory, used to configure the client id of the connector when it connects to the MQTT server as an MQTT client.
- keep_alive: Non-required, used to configure the waiting time after the connector, as an MQTT client, sends a PINGREG message to the MQTT server, if the connector does not receive a PINGREQ message from the MQTT server within this time, the connector will actively disconnect; the unit of this configuration is seconds, and the default value is 60.
- ca: Non-required, used to specify the CA certificate to be used when the connector establishes an SSL/TLS connection with the MQTT server, and its value is @ in front of the absolute path of the certificate file, for example: @/home/admin/certs/ca.crt.
- cert: Non-required, used to specify the client certificate to be used when the connector establishes an SSL/TLS connection with the MQTT server, whose value is @ in front of the absolute path of the certificate file, for example: @/home/admin/certs/client.crt.
- cert_key: Non-required, used to specify the client's private key to be used when the connector establishes an SSL/TLS connection with the MQTT server, whose value is @ in front of the absolute path of the private key file, for example: @/home/admin/certs/client.key.
- log_level: Non-required, used to configure the logging level of the connector, the connector supports error/warn/info/debug/trace 5 logging levels, the default value is info.

A sample MQTT description string is as follows:
```bash
mqtt://<username>:<password>@<mqtt-broker-ip>:8883?topics=testtopic/1::2&version=3.1&clean_session=true&log_level=info&client_id=taosdata_1234&keep_alive=60&ca=@/home/admin/certs/ca.crt&cert=@/home/admin/certs/client.crt&cert_key=@/home/admin/certs/client.key
```

### Interpreter configuration for MQTT connectors

The connector's interpreter configuration file, the parameter to the `--parser` configuration item, which takes the value of a JSON file, can be configured in two parts, `parse` and `model`, as shown in the template below:

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

The fields are described below:
- The parse section currently supports only one type of payload, json, where the value of the json field is a JSON Array consisting of a JSON Object.
  - Each JSON Object consists of three fields: name, alias, cast;
  - The name field specifies how to extract the field from the MQTT message. If the MQTT message is a simple JSON Object, you can set the field name here; if the MQTT message is a complex JSON Object, you can use a JSON Path to extract the field, e.g. `$.data.city`
  - The alias field is used to name the name that will be used after the fields in the MQTT message are synchronized to the TDengine;
  - The cast field is used to specify the type of field used in the MQTT message after it is synchronized to the TDengine.
- The model section is used to set up information about the TDengine super table, sub-tables, columns and labels:
  - The using field is used to specify the super table name;
  - The name field is used to specify the name of the sub-table, and its value can be divided into two parts: the prefix and the variable, the variable is the value of the alias set in the parse part, and you need to use {}, for example: d{id};
  - The columns field is used to set which fields in the MQTT message are to be used as columns in the TDengine super table, and takes the value of the alias set in the parse section; note that the order here determines the order of the columns in the TDengine super table, and therefore the first column must be of type TIMESTAMP;
  - The tags field is used to set which fields in the MQTT message are used as tags in the TDengine super table, and takes the value of the alias set in the parse section.

### Example

There is an MQTT broker running on port 1883 of 192.168.1.10, with usernames and passwords admin, 123456; we want to synchronize the messages from it to TDengine's test database via taosadapter running on 192.168.1.20. The MQTT message format is:

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

When MQTT messages are synchronized to TDengine, if you use meters as the name of the super table, prefix "d" to concatenate the value of id field as the name of the sub-table, ts, id, current, voltage, phase as the columns of the super table, and groupid, location as the labels of the super table, the interpreter is configured as follows:
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

If the above parser configuration is located in `/home/admin/parser.json`, then the full command is shown below:

```bash
taosx run \
  -f "mqtt://admin:123456@192.168.1.10:1883?topics=testtopic/1::2&version=3.1&clean_session=true&log_level=info&client_id=1234&keep_alive=60" \
  -t "taos+ws://192.168.1.20:6041/test"
  --parser "@/home/admin/parser.json"
  --verbose
```

## Kafka

### Command Line Parameters

taosx supports consuming data from Kafka and writing to TDengine. The configuration file is described as follows:
```shell
taosx run -f "<Kafka-DSN>" -t "<TDengine-DSN>"
```
or
```shell
taosx run -f "<Kafka-DSN>" -t "<TDengine-DSN>" --parser "@<parser-config-file-path>"
```
The parameters are:
- -f or --from: Kafka DSN
- -t or --to: TDengine DSN
- --parser: JSON configuration file or string
  
### Kafka DSN configuration

| Parameter | Description | Mandatory | Default | Used On | Example | 
|-----|---------------|----------|---------|---------|----------|
| group| Consumer group. Can be an empty string; in this case the consumer generated has no group | No | "" | Source | |
| topics | Specify the topic to consume. All available partitions for the specified topic will be used unless overridden when topic_partitions is specified. | This parameter or topic_partitions must specify at least one in order for topics to be assigned to consumers. | None | Source |  topics=tp1,tp2 | 
| topic_partitions | Explicitly specify the topic partition to be used. Use only the specified partition with the topic identified. | This parameter or topics must specify at least one in order for the topic to be assigned to the consumer. | None | Source | topic_partitions=tp1:0..2,tp2:1 |
| fallback_offset | Possible values at topic offset: - Earliest: receive the earliest available offset; - Latest: receive the most recent offset; - ByTime(i64): used to request all messages up to a specific time (ms); Unix timestamps (milliseconds) | No | Earliest | Source | fallback_offset=Earliest | 
| offset_storage | Defines the available storage to use when fetching or committing group offsets: - Zookeeper: Zookeeper-based storage (available since kafka 0.8.1); - Kafka: Kafka-based storage (available since Kafka 0.8.2). This is the preferred method for groups to store their offsets.  | No | Kafka | Source  | offset_storage=Kafka |
| timeout | When subscribing to data from kafka, if no valid data is fetched after the timeout, exit | No | 500 | Source  | timeout=never | 
| use_ssl | Whether to use SSL | No |  | Source  | |
| cert | SSL certificate path | No | | | Source  | |
| cert_key | SSL certificate key path | No | | Source  ||


### Example 1

Consume data from a Kafka instance on the 192.168.1.92 server and synchronize it to the TDengine on 192.168.1.92 without using a parser.

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

### Example 2

Consume data from the Kafka instance on the 192.168.1.92 server, synchronize it to the TDengine on 192.168.1.92, and use parser to parse the JSON data in the value.

1. kafka, same as example 1
2. TDengine, same as example 1
3. Taosx
   
```shell
taosx run -f "kafka://192.168.1.92:9092/?topics=tp1,tp2&timeout=5000" -t "taos://192.168.0.201:6030/kafka_to_taos" --parser "{\"parse\":{\"ts\":{\"as\":\"timestamp(ms)\"},\"topic\":{\"as\":\"varchar\",\"alias\":\"t\"},\"partition\":{\"as\":\"int\",\"alias\":\"p\"},\"offset\":{\"as\":\"bigint\",\"alias\":\"o\"},\"value\":{\"json\":[\"id::int\",\"message::binary\"]}},\"model\":[{\"name\":\"t_{t}\",\"using\":\"kafka_data\",\"tags\":[\"t\",\"p\"],\"columns\":[\"ts\",\"o\",\"id\",\"message\"]}]}"
```
