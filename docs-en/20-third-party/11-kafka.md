---
sidebar_label: Kafka
title: TDengine Kafka Connector Tutorial
---

TDengine Kafka Connector contains two plugins: TDengine Source Connector and TDengine Sink Connector. Users only need to provide a simple configuration file to synchronize the data of the specified topic in Kafka (batch or real-time) to TDengine or synchronize the data (batch or real-time) of the specified database in TDengine to Kafka.

## What is Kafka Connect?

Kafka Connect is a component of Apache Kafka that enables other systems, such as databases, cloud services, file systems, etc., to connect to Kafka easily. Data can flow from other software to Kafka via Kafka Connect and Kafka to other systems via Kafka Connect. Plugins that read data from other software are called Source Connectors, and plugins that write data to other software are called Sink Connectors. Neither Source Connector nor Sink Connector will directly connect to Kafka Broker, and Source Connector transfers data to Kafka Connect. Sink Connector receives data from Kafka Connect.

![](kafka/Kafka_Connect.webp)

TDengine Source Connector is used to read data from TDengine in real-time and send it to Kafka Connect. Users can use The TDengine Sink Connector to receive data from Kafka Connect and write it to TDengine.

![](kafka/streaming-integration-with-kafka-connect.webp)

## What is Confluent?

Confluent adds many extensions to Kafka. include:

1. Schema Registry
2. REST Proxy
3. Non-Java Clients
4. Many packaged Kafka Connect plugins
5. GUI for managing and monitoring Kafka - Confluent Control Center

Some of these extensions are available in the community version of Confluent. Some are only available in the enterprise version.
![](kafka/confluentPlatform.webp)

Confluent Enterprise Edition provides the `confluent` command-line tool to manage various components.

## Prerequisites

1. Linux operating system
2. Java 8 and Maven installed
3. Git is installed
4. TDengine is installed and started. If not, please refer to [Installation and Uninstallation](/operation/pkg-install)

## Install Confluent

Confluent provides two installation methods: Docker and binary packages. This article only introduces binary package installation.

Execute in any directory:

````
curl -O http://packages.confluent.io/archive/7.1/confluent-7.1.1.tar.gz
tar xzf confluent-7.1.1.tar.gz -C /opt/test
````

Then you need to add the `$CONFLUENT_HOME/bin` directory to the PATH.

```title=".profile"
export CONFLUENT_HOME=/opt/confluent-7.1.1
PATH=$CONFLUENT_HOME/bin
export PATH
```

Users can append the above script to the current user's profile file (~/.profile or ~/.bash_profile)

After the installation is complete, you can enter `confluent version` for simple verification:

```
# confluent version
confluent - Confluent CLI

Version:     v2.6.1
Git Ref:     6d920590
Build Date:  2022-02-18T06:14:21Z
Go Version:  go1.17.6 (linux/amd64)
Development: false
```

## Install TDengine Connector plugin

### Install from source code

```
git clone https://github.com:taosdata/kafka-connect-tdengine.git
cd kafka-connect-tdengine
mvn clean package
unzip -d $CONFLUENT_HOME/share/confluent-hub-components/ target/components/packages/taosdata-kafka-connect-tdengine-0.1.0.zip
```

The above script first clones the project source code and then compiles and packages it with Maven. After the package is complete, the zip package of the plugin is generated in the `target/components/packages/` directory. Unzip this zip package to the path where the plugin is installed. The path to install the plugin is in the configuration file `$CONFLUENT_HOME/etc/kafka/connect-standalone.properties`. The default path is `$CONFLUENT_HOME/share/confluent-hub-components/`.

### Install with confluent-hub

[Confluent Hub](https://www.confluent.io/hub) provides a service to download Kafka Connect plugins. After TDengine Kafka Connector is published to Confluent Hub, it can be installed using the command tool `confluent-hub`.
**TDengine Kafka Connector is currently not officially released and cannot be installed in this way**.

## Start Confluent

```
confluent local services start
```

:::note
Be sure to install the plugin before starting Confluent. Otherwise, there will be a class not found error. The log of Kafka Connect (default path: /tmp/confluent.xxxx/connect/logs/connect.log) will output the successfully installed plugin, which users can use to determine whether the plugin is installed successfully.
:::

:::tip
If a component fails to start, try clearing the data and restarting. The data directory will be printed to the console at startup, e.g.:

```title="Console output log" {1}
Using CONFLUENT_CURRENT: /tmp/confluent.106668
Starting ZooKeeper
ZooKeeper is [UP]
Starting Kafka
Kafka is [UP]
Starting Schema Registry
Schema Registry is [UP]
Starting Kafka REST
Kafka REST is [UP]
Starting Connect
Connect is [UP]
Starting ksqlDB Server
ksqlDB Server is [UP]
Starting Control Center
Control Center is [UP]
```

To clear data, execute `rm -rf /tmp/confluent.106668`.
:::

## The use of TDengine Sink Connector

The role of the TDengine Sink Connector is to synchronize the data of the specified topic to TDengine. Users do not need to create databases and super tables in advance. The name of the target database can be specified manually (see the configuration parameter connection.database), or it can be generated according to specific rules (see the configuration parameter connection.database.prefix).

TDengine Sink Connector internally uses TDengine [modeless write interface](/reference/connector/cpp#modeless write-api) to write data to TDengine, currently supports data in three formats: [InfluxDB line protocol format](/develop /insert-data/influxdb-line), [OpenTSDB Telnet protocol format](/develop/insert-data/opentsdb-telnet), and [OpenTSDB JSON protocol format](/develop/insert-data/opentsdb-json).

The following example synchronizes the data of the topic meters to the target database power. The data format is the InfluxDB Line protocol format.

### Add configuration file

```
mkdir ~/test
cd ~/test
vi sink-demo.properties
```

sink-demo.properties' content is following:

```ini title="sink-demo.properties"
name=tdengine-sink-demo
connector.class=com.taosdata.kafka.connect.sink.TDengineSinkConnector
tasks.max=1
topics=meters
connection.url=jdbc:TAOS://127.0.0.1:6030
connection.user=root
connection.password=taosdata
connection.database=power
db.schemaless=line
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
```

Key configuration instructions:

1. `topics=meters` and `connection.database=power` means to subscribe to the data of the topic meters and write to the database power.
2. `db.schemaless=line` means the data in the InfluxDB Line protocol format.

### Create Connector instance

````
confluent local services connect connector load TDengineSinkConnector --config ./sink-demo.properties
````

If the above command is executed successfully, the output is as follows:

```json
{
  "name": "TDengineSinkConnector",
  "config": {
    "connection.database": "power",
    "connection.password": "taosdata",
    "connection.url": "jdbc:TAOS://127.0.0.1:6030",
    "connection.user": "root",
    "connector.class": "com.taosdata.kafka.connect.sink.TDengineSinkConnector",
    "db.schemaless": "line",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "tasks.max": "1",
    "topics": "meters",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    "name": "TDengineSinkConnector"
  },
  "tasks": [],
  "type": "sink"
}
```

### Write test data

Prepare text file as test data, its content is following:

```txt title="test-data.txt"
meters,location=California.LoSangeles,groupid=2 current=11.8,voltage=221,phase=0.28 1648432611249000000
meters,location=California.LoSangeles,groupid=2 current=13.4,voltage=223,phase=0.29 1648432611250000000
meters,location=California.LoSangeles,groupid=3 current=10.8,voltage=223,phase=0.29 1648432611249000000
meters,location=California.LoSangeles,groupid=3 current=11.3,voltage=221,phase=0.35 1648432611250000000
```

Use kafka-console-producer to write test data to the topic `meters`.

```
cat test-data.txt | kafka-console-producer --broker-list localhost:9092 --topic meters
```

:::note
TDengine Sink Connector will automatically create the database if the target database does not exist. The time precision used to create the database automatically is nanoseconds, which requires that the timestamp precision of the written data is also nanoseconds. An exception will be thrown if the timestamp precision of the written data is not nanoseconds.
:::

### Verify that the sync was successful

Use the TDengine CLI to verify that the sync was successful.

```
taos> use power;
Database changed.

taos> select * from meters;
              ts               |          current          |          voltage          |           phase           | groupid |            location            |
===============================================================================================================================================================
 2022-03-28 09:56:51.249000000 |              11.800000000 |             221.000000000 |               0.280000000 | 2       | California.LoSangeles                |
 2022-03-28 09:56:51.250000000 |              13.400000000 |             223.000000000 |               0.290000000 | 2       | California.LoSangeles                |
 2022-03-28 09:56:51.249000000 |              10.800000000 |             223.000000000 |               0.290000000 | 3       | California.LoSangeles                |
 2022-03-28 09:56:51.250000000 |              11.300000000 |             221.000000000 |               0.350000000 | 3       | California.LoSangeles                |
Query OK, 4 row(s) in set (0.004208s)
```

If you see the above data, the synchronization is successful. If not, check the logs of Kafka Connect. For detailed description of configuration parameters, see [Configuration Reference](#Configuration Reference).

## The use of TDengine Source Connector

The role of the TDengine Source Connector is to push all the data of a specific TDengine database after a particular time to Kafka. The implementation principle of TDengine Source Connector is to first pull historical data in batches and then synchronize incremental data with the strategy of the regular query. At the same time, the changes in the table will be monitored, and the newly added table can be automatically synchronized. If Kafka Connect is restarted, synchronization will resume where it left off.

TDengine Source Connector will convert the data in TDengine data table into [InfluxDB Line protocol format](/develop/insert-data/influxdb-line/) or [OpenTSDB JSON protocol format](/develop/insert-data/opentsdb-json ) and then write to Kafka.

The following sample program synchronizes the data in the database test to the topic tdengine-source-test.

### Add configuration file

```
vi source-demo.properties
```

Input following content:

```ini title="source-demo.properties"
name=TDengineSourceConnector
connector.class=com.taosdata.kafka.connect.source.TDengineSourceConnector
tasks.max=1
connection.url=jdbc:TAOS://127.0.0.1:6030
connection.username=root
connection.password=taosdata
connection.database=test
connection.attempts=3
connection.backoff.ms=5000
topic.prefix=tdengine-source-
poll.interval.ms=1000
fetch.max.rows=100
out.format=line
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
```

### Prepare test data

Prepare SQL script file to generate test data

```sql title="prepare-source-data.sql"
DROP DATABASE IF EXISTS test;
CREATE DATABASE test;
USE test;
CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT);
INSERT INTO d1001 USING meters TAGS(California.SanFrancisco, 2) VALUES('2018-10-03 14:38:05.000',10.30000,219,0.31000) d1001 USING meters TAGS(California.SanFrancisco, 2) VALUES('2018-10-03 14:38:15.000',12.60000,218,0.33000) d1001 USING meters TAGS(California.SanFrancisco, 2) VALUES('2018-10-03 14:38:16.800',12.30000,221,0.31000) d1002 USING meters TAGS(California.SanFrancisco, 3) VALUES('2018-10-03 14:38:16.650',10.30000,218,0.25000) d1003 USING meters TAGS(California.LoSangeles, 2) VALUES('2018-10-03 14:38:05.500',11.80000,221,0.28000) d1003 USING meters TAGS(California.LoSangeles, 2) VALUES('2018-10-03 14:38:16.600',13.40000,223,0.29000) d1004 USING meters TAGS(California.LoSangeles, 3) VALUES('2018-10-03 14:38:05.000',10.80000,223,0.29000) d1004 USING meters TAGS(California.LoSangeles, 3) VALUES('2018-10-03 14:38:06.500',11.50000,221,0.35000);
```

Use TDengine CLI to execute SQL script

```
taos -f prepare-source-data.sql
```

### Create Connector instance

````
confluent local services connect connector load TDengineSourceConnector --config source-demo.properties
````

### View topic data

Use the kafka-console-consumer command-line tool to monitor data in the topic tdengine-source-test. In the beginning, all historical data will be output. After inserting two new data into TDengine, kafka-console-consumer immediately outputs the two new data.

````
kafka-console-consumer --bootstrap-server localhost:9092 --from-beginning --topic tdengine-source-test
````

output:

````
......
meters,location="California.SanFrancisco",groupid=2i32 current=10.3f32,voltage=219i32,phase=0.31f32 1538548685000000000
meters,location="California.SanFrancisco",groupid=2i32 current=12.6f32,voltage=218i32,phase=0.33f32 1538548695000000000
......
````

All historical data is displayed. Switch to the TDengine CLI and insert two new pieces of data:

````
USE test;
INSERT INTO d1001 VALUES (now, 13.3, 229, 0.38);
INSERT INTO d1002 VALUES (now, 16.3, 233, 0.22);
````

Switch back to kafka-console-consumer, and the command line window has printed out the two pieces of data just inserted.

### unload plugin

After testing, use the unload command to stop the loaded connector.

View currently active connectors:

````
confluent local services connect connector status
````

You should now have two active connectors if you followed the previous steps. Use the following command to unload:

````
confluent local services connect connector unload TDengineSourceConnector
confluent local services connect connector unload TDengineSourceConnector
````

## Configuration reference

### General configuration

The following configuration items apply to TDengine Sink Connector and TDengine Source Connector.

1. `name`: The name of the connector.
2. `connector.class`: The full class name of the connector, for example: com.taosdata.kafka.connect.sink.TDengineSinkConnector.
3. `tasks.max`: The maximum number of tasks, the default is 1.
4. `topics`: A list of topics to be synchronized, separated by commas, such as `topic1,topic2`.
5. `connection.url`: TDengine JDBC connection string, such as `jdbc:TAOS://127.0.0.1:6030`.
6. `connection.user`: TDengine username, default root.
7. `connection.password`: TDengine user password, default taosdata.
8. `connection.attempts` : The maximum number of connection attempts. Default 3.
9. `connection.backoff.ms`: The retry interval for connection creation failure, the unit is ms. Default is 5000.

### TDengine Sink Connector specific configuration

1. `connection.database`: The name of the target database. If the specified database does not exist, it will be created automatically. The time precision used for automatic library building is nanoseconds. The default value is null. When it is NULL, refer to the description of the `connection.database.prefix` parameter for the naming rules of the target database
2. `connection.database.prefix`: When `connection.database` is null, the prefix of the target database. Can contain placeholder '${topic}'. For example, kafka_${topic}, for topic 'orders' will be written to database 'kafka_orders'. Default null. When null, the name of the target database is the same as the name of the topic.
3. `batch.size`: Write the number of records in each batch in batches. When the data received by the sink connector at one time is larger than this value, it will be written in some batches.
4. `max.retries`: The maximum number of retries when an error occurs. Defaults to 1.
5. `retry.backoff.ms`: The time interval for retry when sending an error. The unit is milliseconds. The default is 3000.
6. `db.schemaless`: Data format, could be one of `line`, `json`, and `telnet`. Represent InfluxDB line protocol format, OpenTSDB JSON format, and OpenTSDB Telnet line protocol format.

### TDengine Source Connector specific configuration

1. `connection.database`: source database name, no default value.
2. `topic.prefix`: topic name prefix after data is imported into kafka. Use `topic.prefix` + `connection.database` name as the full topic name. Defaults to the empty string "".
3. `timestamp.initial`: Data synchronization start time. The format is 'yyyy-MM-dd HH:mm:ss'. Default "1970-01-01 00:00:00".
4. `poll.interval.ms`: Pull data interval, the unit is ms. Default is 1000.
5. `fetch.max.rows`: The maximum number of rows retrieved when retrieving the database. Default is 100.
6. `out.format`: The data format. The value could be line or json. The line represents the InfluxDB Line protocol format, and json represents the OpenTSDB JSON format. Default is `line`.

## feedback

https://github.com/taosdata/kafka-connect-tdengine/issues

## Reference

1. https://www.confluent.io/what-is-apache-kafka
2. https://developer.confluent.io/learn-kafka/kafka-connect/intro
3. https://docs.confluent.io/platform/current/platform.html
