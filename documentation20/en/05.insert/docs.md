# Efficient Data Writing

TDengine supports multiple ways to write data, including SQL, Prometheus, Telegraf, collectd, StatsD, EMQ MQTT Broker, HiveMQ Broker, CSV file, etc. Kafka, OPC and other interfaces will be provided in the future. Data can be inserted in one single record or in batches, data from one or multiple data collection points can be inserted at the same time. TDengine supports multi-thread insertion, out-of-order data insertion, and also historical data insertion.

## <a class="anchor" id="sql"></a> Data Writing via SQL 

Applications insert data by executing SQL insert statements through C/C++, Java, Go, C#, Python, Node.js Connectors, and users can manually enter SQL insert statements to insert data through TAOS Shell. For example, the following insert writes a record to table d1001:

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

- To improve writing efficiency, batch writing is required. The more records written in a batch, the higher the insertion efficiency. However, a record size cannot exceed 16K, and the total length of an SQL statement cannot exceed 64K (it can be configured by parameter maxSQLLength, and the maximum can be configured to 1M).
- TDengine supports multi-thread parallel writing. To further improve writing speed, a client needs to open more than 20 threads to write parallelly. However, after the number of threads reaches a certain threshold, it cannot be increased or even become decreased, because too much thread switching brings extra overhead.
- For the same table, if the timestamp of a newly inserted record already exists, the new record will be discarded as default (database option update = 0), that is, the timestamp must be unique in a table. If an application automatically generates records, it is very likely that the generated timestamps will be the same, so the number of records successfully inserted will be smaller than the number of records the application try to insert. If you use UPDATE 1 option when creating a database, inserting a new record with the same timestamp will overwrite the original record.
- The timestamp of written data must be greater than the current time minus the time of configuration parameter keep. If keep is configured for 3650 days, data older than 3650 days cannot be written. The timestamp for writing data cannot be greater than the current time plus configuration parameter days. If days is configured to 2, data 2 days later than the current time cannot be written.

## <a class="anchor" id="prometheus"></a> Data Writing via Prometheus

As a graduate project of Cloud Native Computing Foundation, [Prometheus](https://www.prometheus.io/) is widely used in the field of performance monitoring and K8S performance monitoring. TDengine provides a simple tool [Bailongma](https://github.com/taosdata/Bailongma), which only needs to be simply configured in Prometheus without any code, and can directly write the data collected by Prometheus into TDengine, then automatically create databases and related table entries in TDengine according to rules. Blog post [Use Docker Container to Quickly Build a Devops Monitoring Demo](https://www.taosdata.com/blog/2020/02/03/1189.html), which is an example of using bailongma to write Prometheus and Telegraf data into TDengine.

### Compile blm_prometheus From Source

Users need to download the source code of [Bailongma](https://github.com/taosdata/Bailongma) from github, then compile and generate an executable file using Golang language compiler. Before you start compiling, you need to prepare:

- A server running Linux OS
- Golang version 1.10 and higher installed
- Since the client dynamic link library of TDengine is used, it is necessary to install the same version of TDengine as the server-side. For example, if the server version is TDengine 2.0. 0, ensure install the same version on the linux server where bailongma is located (can be on the same server as TDengine, or on a different server)

Bailongma project has a folder, blm_prometheus, which holds the prometheus writing API. The compiling process is as follows:

```bash
cd blm_prometheus

go build
```

If everything goes well, an executable of blm_prometheus will be generated in the corresponding directory.

### Install Prometheus

Download and install as the instruction of Prometheus official website. [Download Address](https://prometheus.io/download/)

### Configure Prometheus

Read the Prometheus [configuration document](https://prometheus.io/docs/prometheus/latest/configuration/configuration/) and add following configurations in the section of Prometheus configuration file

- url: The URL provided by bailongma API service, refer to the blm_prometheus startup example section below

After Prometheus launched, you can check whether data is written successfully through query taos client.

### Launch blm_prometheus

blm_prometheus has following options that you can configure when you launch blm_prometheus.

```sh
--tdengine-name

If TDengine is installed on a server with a domain name, you can also access the TDengine by configuring the domain name of it. In K8S environment, it can be configured as the service name that TDengine runs

--batch-size

blm_prometheus assembles the received prometheus data into a TDengine writing request. This parameter controls the number of data pieces carried in a writing request sent to TDengine at a time.

--dbname

Set a name for the database created in TDengine, blm_prometheus will automatically create a database named dbname in TDengine, and the default value is prometheus.

--dbuser

Set the user name to access TDengine, the default value is'root '

--dbpassword

Set the password to access TDengine, the default value is'taosdata '

--port

The port number blm_prometheus used to serve prometheus.
```



### Example

Launch an API service for blm_prometheus with the following command:

```bash
./blm_prometheus -port 8088
```

Assuming that the IP address of the server where blm_prometheus located is "10.1.2. 3", the URL shall be added to the configuration file of Prometheus as:

```yaml
remote_write:
  - url: "http://10.1.2.3:8088/receive"
```

### Query written data of prometheus

The format of generated data by Prometheus is as follows:

```json
{
  Timestamp: 1576466279341,
  Value: 37.000000, 
  apiserver_request_latencies_bucket {
  component="apiserver", 
   instance="192.168.99.116:8443", 
   job="kubernetes-apiservers", 
   le="125000", 
   resource="persistentvolumes", s
   cope="cluster",
   verb="LIST", 
   version=“v1" 
  }
}
```

Where apiserver_request_latencies_bucket is the name of the time-series data collected by prometheus, and the tag of the time-series data is in the following {}. blm_prometheus automatically creates a STable in TDengine with the name of the time series data, and converts the tag in {} into the tag value of TDengine, with Timestamp as the timestamp and value as the value of the time-series data. Therefore, in the client of TDengine, you can check whether this data was successfully written through the following instruction.

```mysql
use prometheus;

select * from apiserver_request_latencies_bucket;
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

## <a class="anchor" id="collectd"></a> collectd 直接写入(通过 taosAdapter)
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

## <a class="anchor" id="statsd"></a> StatsD 直接写入(通过 taosAdapter)
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

Please find taosAdapter configuration and usage from `taosadapter --help` output.


## <a class="anchor" id="taosadapter2-telegraf"></a> Insert data via Bailongma 2.0 and Telegraf

**Notice:**
TDengine 2.3.0.0+ provides taosAdapter to support Telegraf data writing. Bailongma v2 will be abandoned and no more maintained.


## <a class="anchor" id="emq"></a> Data Writing via EMQ Broker

[EMQ](https://github.com/emqx/emqx) is an open source MQTT Broker software, with no need of coding, only to use "rules" in EMQ Dashboard for simple configuration, and MQTT data can be directly written into TDengine. EMQ X supports storing data to the TDengine by sending it to a Web service, and also provides a native TDengine driver on Enterprise Edition for direct data store. Please refer to [EMQ official documents](https://docs.emqx.io/broker/latest/cn/rule/rule-example.html#%E4%BF%9D%E5%AD%98%E6%95%B0%E6%8D%AE%E5%88%B0-tdengine) for more details.

## <a class="anchor" id="hivemq"></a> Data Writing via HiveMQ Broker

[HiveMQ](https://www.hivemq.com/) is an MQTT agent that provides Free Personal and Enterprise Edition versions. It is mainly used for enterprises, emerging machine-to-machine(M2M) communication and internal transmission to meet scalability, easy management and security features. HiveMQ provides an open source plug-in development kit. You can store data to TDengine via HiveMQ extension-TDengine. Refer to the [HiveMQ extension-TDengine documentation](https://github.com/huskar-t/hivemq-tdengine-extension/blob/b62a26ecc164a310104df57691691b237e091c89/README.md) for more details.
