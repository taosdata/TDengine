# Efficient Data Writing

TDengine supports multiple ways to write data, including SQL, Prometheus, Telegraf, collectd, StatsD, EMQX MQTT Broker, HiveMQ Broker, CSV file, etc. Kafka, OPC and other interfaces will be provided in the future. Data can be inserted in one single record or in batches, data from one or multiple data collection points can be inserted at the same time. TDengine supports multi-thread insertion, out-of-order data insertion, and also historical data insertion.

## <a class="anchor" id="sql"></a> Data Writing via SQL

Applications insert data by executing SQL insert statements through C/C++, Java, Go, C#, Python, Node.js connectors, and users can manually enter SQL insert statements to insert data through TAOS Shell. For example, the following insert writes a record to table d1001:

```mysql
INSERT INTO d1001 VALUES (1538548685000, 10.3, 219, 0.31);
```

TDengine supports writing multiple records in a single statement. For example, the following command writes two records to table d1001:

```mysql
INSERT INTO d1001 VALUES (1538548684000, 10.2, 220, 0.23) (1538548696650, 10.3, 218, 0.25);
```

TDengine also supports writing data to multiple tables in a single statement. For example, the following command writes two records to d1001 and one record to d1002:

```mysql
INSERT INTO d1001 VALUES (1538548685000, 10.3, 219, 0.31) (1538548695000, 12.6, 218, 0.33) d1002 VALUES (1538548696800, 12.3, 221, 0.31);
```

For the SQL INSERT Grammar, please refer to  [Taos SQL insert](https://www.taosdata.com/en/documentation/taos-sql#insert)。

**Tips:**

- To improve writing efficiency, batch writing is required. The more records written in a batch, the higher the insertion efficiency. However, a record size cannot exceed 48K (it's 16K prior to 2.1.7.0, and the total length of an SQL statement cannot exceed 64K (it can be configured by parameter maxSQLLength, and the maximum can be configured to 1M).
- TDengine supports multi-thread parallel writing. To further improve writing speed, a client needs to open more than 20 threads to write parallelly. However, after the number of threads reaches a certain threshold, it cannot be increased or even become decreased, because too much thread switching brings extra overhead.
- For the same table, if the timestamp of a newly inserted record already exists, the new record will be discarded as default (database option update = 0), that is, the timestamp must be unique in a table. If an application automatically generates records, it is very likely that the generated timestamps will be the same, so the number of records successfully inserted will be smaller than the number of records the application try to insert. If you use UPDATE 1 option when creating a database, inserting a new record with the same timestamp will overwrite the original record.
- The timestamp of written data must be greater than the current time minus the time of configuration parameter keep. If keep is configured for 3650 days, data older than 3650 days cannot be written. The timestamp for writing data cannot be greater than the current time plus configuration parameter days. If days is configured to 2, data 2 days later than the current time cannot be written.

## <a class="anchor" id="schemaless"></a> Data Writing via Schemaless

**Introduction**
<br/> In many IoT applications, data collection is often used in intelligent control, business analysis and device monitoring etc. As fast application upgrade and iteration, or hardware adjustment, data collection metrics can change rapidly over time. To provide solutions to such use cases, from version 2.2.0.0, TDengine supports writing data via Schemaless. When using Schemaless, action of pre-creating table before inserting data is no longer needed anymore. Tables, data columns and tags can be created automatically. Schemaless can also add additional data columns to tables if necessary, to make sure data can be properly stored into TDengine.

<br/> TDengine's all official connectors provide Schemaless API now. Please see [Schemaless data writing API](https://www.taosdata.com/en/documentation/connector#schemaless) for detailed data writing format.
<br/> Super table and corresponding child tables created via Schemaless are identical to the ones created via SQL, so inserting data into these tables via SQL is also supported. Note that child table names are generated via Schemaless are following special rules through tags mapping. Therefore, child table names are usually not meaningful in terms of readability.

**Schemaless writing protocols**
<br/>TDengine Schemaless writing protocol is compatible with InfluxDB's Line Protocol, OpenTSDB's telnet and JSON format protocols. Users need to specify which protocol to use as parameter when writing data using Schemaless API.

For InfluxDB, OpenTSDB data writing protocol format, users can refer to corresponding official documentation for details. Following will give examples of introducing protocol extension from TDengine based on InfluxDB's Line Protocol, allowing users to use Schemaless with more precision.

Schemaless use one line of string literals to represent one data record. (Users can also pass multiple lines to the Schemaless API for batch insertion), the format is as follows:

```json
measurement,tag_set field_set timestamp
```

* measurement is used as the table name. Comma delimiter is used to separate measurement and tag_set.
* tag_set represent tag data in key-value pairs. The format is: `<tag_key>=<tag_value>,<tag_key>=<tag_value>`. Comma delimiter is used to separate multiple tag key-value pairs. Space delimiter is used to separate tag_set and field_set.
* field_set represent column data in key-value pairs. The format is similar to tag_set: `<field_key>=<field_value>,<field_key>=<field_value>`. Comma delimiter is used to separate multiple tag key-value pairs. Space delimiter is used to separate field_set and timestamp.
* Timestamp is the primary key of one data row.

All tag values in tag_set are automatically converted and stored as NCHAR data type in TDengine and no need to be surrounded by double quote("）
<br/> In Schemaless Line Protocol, data format in field_set need to be self-descriptive in order to convert data to corresponding TDengine data types. For example:

* Field value surrounded by double quote indicates data is BINARY(32) data types. For example, `"abc"`.
* Field value surrounded by double quote and L letter prefix indicates data is NCHAR(32) data type. For example `L"报错信息"`.
* Space, equal sign(=), comma(,), double quote(") need to use backslash(\) to escape.
* Numerical values will be converted to corresponding data types according to the suffix:

| **ID** | **Suffix** | **Data Type** | **Size(Bytes)** |
| ------ | ---------- | ------------- | ------ |
|    1   | NA / f64   |  DOUBLE       |   8    |
|    2   | f32        |  FLOAT        |   4    |
|    3   | i8         |  TINYINT      |   1    |
|    4   | i16        |  SMALLINT     |   2    |
|    5   | i32        |  INT          |   4    |
|    6   | i64 / i    |  BIGINT       |   8    |

* t, T, true, True, TRUE, f, F, false, False represents BOOLEAN types。

### Schemaless processing logic

Following rules are followed by Schemaless protocol parsing:

<br/>1. For child table name generation, firstly create following string by concatenating measurement and tag key/values strings together.

```json
"measurement,tag_key1=tag_value1,tag_key2=tag_value2"
```

tag_key1, tag_key2 are not following the original order of user input, but sorted according to tag names.
After MD5 value "md5_val" calculated using the above string, prefix "t_" is prepended to "md5_val" to form the child table name.
<br/>2. If super table does not exist, a new super table will be created.
<br/>3. If child table does not exist, a new child table will be created with its name generated in 1 and 2.
<br/>4. If columns/tags do not exist, new columns/tags will be created. (Columns/tags can only be added, existing columns/tags cannot be deleted)
<br/>5. If columns/tags are not specified in a line, values of such columns/tags will be set to NULL.
<br/>6. For BINARY/NCHAR type columns, if value length exceeds max length of the column, max length will be automatically extended to ensure data integrity.
<br/>7. If child table is already created and tag value is different than previous stored value，old value will be overwritten by new value.
<br/>8. If any error occurs during processing, error code will be returned.

**Note**
<br/>Schemaless will follow TDengine data structure limitations. For example, each table row cannot exceed 48KB (it's 16K prior to 2.1.7.0. For detailed TDengine limitations please refer to `https://www.taosdata.com/en/documentation/taos-sql#limitation`.

**Timestamp precisions**
<br/>Following protocols are supported in Schemaless:

| **ID** |         **Value**          |         **Description**         |
| ---- | ---------------------------- | ------------------------------- |
| 1    | SML_LINE_PROTOCOL            |    InfluxDB Line Protocol       |
| 2    | SML_TELNET_PROTOCOL          |  OpenTSDB telnet Protocol       |
| 3    | SML_JSON_PROTOCOL            |  OpenTSDB JSON format Protocol  |

<br/>When SML_LINE_PROTOCOL used，users need to indicate timestamp precision through API。Available timestamp precisions are：<br/>

| **ID** |       **Precision Definition **       |   **Meaning**  |
| ------ | ------------------------------------- | -------------- |
| 1      | TSDB_SML_TIMESTAMP_NOT_CONFIGURED     |   undefined    |
| 2      | TSDB_SML_TIMESTAMP_HOURS              |   hour         |
| 3      | TSDB_SML_TIMESTAMP_MINUTES            |   minute       |
| 4      | TSDB_SML_TIMESTAMP_SECONDS            |   second       |
| 5      | TSDB_SML_TIMESTAMP_MILLI_SECONDS      |   millisecond  |
| 6      | TSDB_SML_TIMESTAMP_MICRO_SECONDS      |   microsecond  |
| 7      | TSDB_SML_TIMESTAMP_NANO_SECONDS       |   nanosecond   |

When SML_TELNET_PROTOCOL or SML_JSON_PROTOCOL used，timestamp precision is determined by how many digits used in timestamp（following OpenTSDB convention），precision from user input will be ignored。

**Schemaless data mapping rules**
<br/>This section describes how Schemaless data are mapped to TDengine's structured data. Measurement is mapped to super table name. Keys in tag_set/field_set are mapped to tag/column names. For example:

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4f64 1626006833639000000
```

Above line is mapped to a super table with name "st" with 3 NCHAR type tags ("t1", "t2", "t3") and 5 columns: ts（timestamp），c1 (bigint），c3(binary)，c2 (bool),  c4 (bigint). This is identical to create a super table with the following SQL clause:

```json
create stable st (_ts timestamp, c1 bigint, c2 bool, c3 binary(6), c4 bigint) tags(t1 nchar(1), t2 nchar(1), t3 nchar(2))
```

**Schemaless data alternation rules**
<br/>This section describes several data alternation scenarios:

When column with one line has certain type, and following lines attempt to change the data type of this column, an error will be reported by the API:

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4    1626006833639000000
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4i   1626006833640000000
```

For first line of data, c4 column type is declared as DOUBLE with no suffix. However, the second line declared the column type to be BIGINT with suffix "i". Schemaless parsing error will be occurred.

When column is declared as BINARY type, but follow-up line insertion requires longer BINARY length of this column, max length of this column will be extended:

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c5="pass"     1626006833639000000
st,t1=3,t2=4,t3=t3 c1=3i64,c5="passit"   1626006833640000000
```

In first line c5 column store string "pass" with 4 characters as BINARY(4), but in second line c5 requires 2 more characters for storing binary string "passit", c5 column max length will be extend from BINARY(4) to BINARY(6) to accommodate more characters.

```json
st,t1=3,t2=4,t3=t3 c1=3i64               1626006833639000000
st,t1=3,t2=4,t3=t3 c1=3i64,c6="passit"   1626006833640000000
```

In above example second line has one more column c6 with value "passit" compared to the first line. A new column c6 will be added with type BINARY(6).

**Data integrity**
<br/>TDengine ensure data writing through Schemaless is idempotent, which means users can call the API multiple times for writing data with errors. However. atomicity is not guaranteed. When writing multiple lines of data as a batch, data might be partially inserted due to errors.

**Error code**
<br/>If users do not write data following corresponding protocol syntax, application will get TSDB_CODE_TSC_LINE_SYNTAX_ERROR error code, which indicates error is happened in input text. Other generic error codes returned by TDengine can also be obtained through taos_errstr API to get detailed error messages.

<br/> Beside TDengine C/C++ Schemaless API, you can use the API of other official connectors as well, including Java/Go/Python/C#/Node.js/Rust. From TDengine v2.4 and later versions, users can also use taosAdaptor to writing data via Schemaless through RESTful interface.

## <a class="anchor" id="prometheus"></a> Data Writing via Prometheus via taosAdapter

Remote_read and remote_write are cluster schemes for Prometheus data read-write separation.
Just use the REMOTE_READ and REMOTE_WRITE URL to point to the URL corresponding to Taosadapter to use Basic authentication.

* Remote_read url: `http://host_to_taosadapter:port (default 6041) /prometheus/v1/remote_read/:db`
* Remote_write url: `http://host_to_taosadapter:port (default 6041) /Prometheus/v1/remote_write/:db`

Basic verification:

* Username: TDengine connection username
* Password: TDengine connection password

Example Prometheus.yml is as follows:

```yaml
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

## <a class="anchor" id="telegraf"></a> Data Writing via Telegraf and taosAdapter

Please refer to [Official document](https://portal.influxdata.com/downloads/) for Telegraf installation.

TDengine version 2.3.0.0+ includes a stand-alone application taosAdapter in charge of receive data insertion from Telegraf.

Configuration:
Please add following words in /etc/telegraf/telegraf.conf. Fill 'database name' with the database name you want to store in the TDengine for Telegraf data. Please fill the values in TDengine server/cluster host, username and password fields.

```
[[outputs.http]]
  url = "http://<TDengine server/cluster host>:6041/influxdb/v1/write?db=<database name>"
  method = "POST"
  timeout = "5s"
  username = "<TDengine's username>"
  password = "<TDengine's password>"
  data_format = "influx"
  influx_max_line_bytes = 250
```

Then restart telegraf:

```
sudo systemctl start telegraf
```

Now you can query the metrics data of Telegraf from TDengine.

Please find taosAdapter configuration and usage from `taosadapter --help` output.

## <a class="anchor" id="collectd"></a> Data Writing via collectd and taosAdapter

Please refer to [official document](https://collectd.org/download.shtml) for collectd installation.

TDengine version 2.3.0.0+ includes a stand-alone application taosAdapter in charge of receive data insertion from collectd.

Configuration:
Please add following words in /etc/collectd/collectd.conf. Please fill the value 'host' and 'port' with what the TDengine and taosAdapter using.

```
LoadPlugin network
<Plugin network>
  Server "<TDengine cluster/server host>" "<port for collectd>"
</Plugin>
```

Then restart collectd

```
sudo systemctl start collectd
```

Please find taosAdapter configuration and usage from `taosadapter --help` output.

## <a class="anchor" id="statsd"></a> Data Writing via StatsD and taosAdapter

Please refer to [official document](https://github.com/statsd/statsd) for StatsD installation.

TDengine version 2.3.0.0+ includes a stand-alone application taosAdapter in charge of receive data insertion from StatsD.

Please add following words in the config.js file. Please fill the value to 'host' and 'port' with what the TDengine and taosAdapter using.

```
add "./backends/repeater" to backends section.
add { host:'<TDengine server/cluster host>', port: <port for StatsD>} to repeater section.
```

Example file:

```
{
port: 8125
, backends: ["./backends/repeater"]
, repeater: [{ host: '127.0.0.1', port: 6044}]
}
```

## <a class="anchor" id="cinga2"></a> Data Writing via icinga2 and taosAdapter

Use icinga2 to collect check result metrics and performance data

* Follow the doc to enable opentsdb-writer `https://icinga.com/docs/icinga-2/latest/doc/14-features/#opentsdb-writer`
* Enable taosAdapter configuration opentsdb_telnet.enable
* Modify the configuration file /etc/icinga2/features-enabled/opentsdb.conf

```
object OpenTsdbWriter "opentsdb" {
  host = "host to taosAdapter"
  port = 6048
}
```

Please find taosAdapter configuration and usage from `taosadapter --help` output.

## <a class="anchor" id="tcollector"></a> Data Writing via TCollector and taosAdapter

TCollector is a client-side process that gathers data from local collectors and pushes the data to OpenTSDB. You run it on all your hosts, and it does the work of sending each host’s data to the TSD (OpenTSDB backend process).

* Enable taosAdapter configuration opentsdb_telnet.enable
* Modify the TCollector configuration file, modify the OpenTSDB host to the host where taosAdapter is deployed, and modify the port to 6049

Please find taosAdapter configuration and usage from `taosadapter --help` output.

## <a class="anchor" id="emq"></a> Data Writing via EMQX Broker

[EMQX](https://github.com/emqx/emqx) is an open source MQTT Broker software, with no need of coding, only to use "rules" in EMQX Dashboard for simple configuration, and MQTT data can be directly written into TDengine. EMQX supports storing data to the TDengine by sending it to a Web service, and also provides a native TDengine driver on Enterprise Edition for direct data store. Please refer to [EMQX official documents](https://docs.emqx.io/broker/latest/cn/rule/rule-example.html#%E4%BF%9D%E5%AD%98%E6%95%B0%E6%8D%AE%E5%88%B0-tdengine) for more details.

## <a class="anchor" id="hivemq"></a> Data Writing via HiveMQ Broker

[HiveMQ](https://www.hivemq.com/) is an MQTT agent that provides Free Personal and Enterprise Edition versions. It is mainly used for enterprises, emerging machine-to-machine(M2M) communication and internal transmission to meet scalability, easy management and security features. HiveMQ provides an open source plug-in development kit. You can store data to TDengine via HiveMQ extension-TDengine. Refer to the [HiveMQ extension-TDengine documentation](https://github.com/huskar-t/hivemq-tdengine-extension/blob/b62a26ecc164a310104df57691691b237e091c89/README.md) for more details.
