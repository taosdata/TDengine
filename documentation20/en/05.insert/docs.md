# Efficient Data Writing

TDengine supports multiple interfaces to write data, including SQL, Prometheus, Telegraf, EMQ MQTT Broker, HiveMQ Broker, CSV file, etc. Kafka, OPC and other interfaces will be provided in the future. Data can be inserted in a single piece or in batches, data from one or multiple data collection points can be inserted at the same time. TDengine supports multi-thread insertion, nonsequential data insertion, and also historical data insertion.

## <a class="anchor" id="sql"></a> SQL Writing

Applications insert data by executing SQL insert statements through C/C++, JDBC, GO, or Python Connector, and users can manually enter SQL insert statements to insert data through TAOS Shell. For example, the following insert writes a record to table d1001:

```mysql
INSERT INTO d1001 VALUES (1538548685000, 10.3, 219, 0.31);
```

TDengine supports writing multiple records at a time. For example, the following command writes two records to table d1001:

```mysql
INSERT INTO d1001 VALUES (1538548684000, 10.2, 220, 0.23) (1538548696650, 10.3, 218, 0.25);
```

TDengine also supports writing data to multiple tables at a time. For example, the following command writes two records to d1001 and one record to d1002:

```mysql
INSERT INTO d1001 VALUES (1538548685000, 10.3, 219, 0.31) (1538548695000, 12.6, 218, 0.33) d1002 VALUES (1538548696800, 12.3, 221, 0.31);
```

For the SQL INSERT Grammar, please refer to  [Taos SQL insert](https://www.taosdata.com/en/documentation/taos-sql#insert)。

**Tips:**

- To improve writing efficiency, batch writing is required. The more records written in a batch, the higher the insertion efficiency. However, a record cannot exceed 16K, and the total length of an SQL statement cannot exceed 64K (it can be configured by parameter maxSQLLength, and the maximum can be configured to 1M).
- TDengine supports multi-thread parallel writing. To further improve writing speed, a client needs to open more than 20 threads to write parallelly. However, after the number of threads reaches a certain threshold, it cannot be increased or even become decreased, because too much frequent thread switching brings extra overhead.
- For a same table, if the timestamp of a newly inserted record already exists, (no database was created using UPDATE 1) the new record will be discarded as default, that is, the timestamp must be unique in a table. If an application automatically generates records, it is very likely that the generated timestamps will be the same, so the number of records successfully inserted will be smaller than the number of records the application try to insert. If you use UPDATE 1 option when creating a database, inserting a new record with the same timestamp will overwrite the original record.
- The timestamp of written data must be greater than the current time minus the time of configuration parameter keep. If keep is configured for 3650 days, data older than 3650 days cannot be written. The timestamp for writing data cannot be greater than the current time plus configuration parameter days. If days is configured to 2, data 2 days later than the current time cannot be written.

## <a class="anchor" id="prometheus"></a> Direct Writing of Prometheus

As a graduate project of Cloud Native Computing Foundation, [Prometheus](https://www.prometheus.io/) is widely used in the field of performance monitoring and K8S performance monitoring. TDengine provides a simple tool [Bailongma](https://github.com/taosdata/Bailongma), which only needs to be simply configured in Prometheus without any code, and can directly write the data collected by Prometheus into TDengine, then automatically create databases and related table entries in TDengine according to rules. Blog post [Use Docker Container to Quickly Build a Devops Monitoring Demo](https://www.taosdata.com/blog/2020/02/03/1189.html), which is an example of using bailongma to write Prometheus and Telegraf data into TDengine.

### Compile blm_prometheus From Source

Users need to download the source code of [Bailongma](https://github.com/taosdata/Bailongma) from github, then compile and generate an executable file using Golang language compiler. Before you start compiling, you need to complete following prepares:

- A server running Linux OS
- Golang version 1.10 and higher installed
- An appropriated TDengine version. Because the client dynamic link library of TDengine is used, it is necessary to install the same version of TDengine as the server-side; for example, if the server version is TDengine 2.0. 0, ensure install the same version on the linux server where bailongma is located (can be on the same server as TDengine, or on a different server)

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

remote_write:

\- url: "http://10.1.2.3:8088/receive"



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

Where apiserver_request_latencies_bucket is the name of the time-series data collected by prometheus, and the tag of the time-series data is in the following {}. blm_prometheus automatically creates a STable in TDengine with the name of the time series data, and converts the tag in {} into the tag value of TDengine, with Timestamp as the timestamp and value as the value of the time-series data. Therefore, in the client of TDEngine, you can check whether this data was successfully written through the following instruction.

```mysql
use prometheus;

select * from apiserver_request_latencies_bucket;
```



## <a class="anchor" id="telegraf"></a> Direct Writing of Telegraf

[Telegraf](https://www.influxdata.com/time-series-platform/telegraf/) is a popular open source tool for IT operation data collection. TDengine provides a simple tool [Bailongma](https://github.com/taosdata/Bailongma), which only needs to be simply configured in Telegraf without any code, and can directly write the data collected by Telegraf into TDengine, then automatically create databases and related table entries in TDengine according to rules. Blog post [Use Docker Container to Quickly Build a Devops Monitoring Demo](https://www.taosdata.com/blog/2020/02/03/1189.html), which is an example of using bailongma to write Prometheus and Telegraf data into TDengine.

### Compile blm_telegraf From Source Code

Users need to download the source code of [Bailongma](https://github.com/taosdata/Bailongma) from github, then compile and generate an executable file using Golang language compiler. Before you start compiling, you need to complete following prepares:

- A server running Linux OS
- Golang version 1.10 and higher installed
- An appropriated TDengine version. Because the client dynamic link library of TDengine is used, it is necessary to install the same version of TDengine as the server-side; for example, if the server version is TDengine 2.0. 0, ensure install the same version on the linux server where bailongma is located (can be on the same server as TDengine, or on a different server)

Bailongma project has a folder, blm_telegraf, which holds the Telegraf writing API. The compiling process is as follows:

```bash
cd blm_telegraf

go build
```

If everything goes well, an executable of blm_telegraf will be generated in the corresponding directory.

### Install Telegraf

At the moment, TDengine supports Telegraf version 1.7. 4 and above. Users can download the installation package on Telegraf's website according to your current operating system. The download address is as follows: https://portal.influxdata.com/downloads

### Configure Telegraf

Modify the TDengine-related configurations in the Telegraf configuration file /etc/telegraf/telegraf.conf.

In the output plugins section, add the [[outputs.http]] configuration:

- url: The URL provided by bailongma API service, please refer to the example section below
- data_format: "json"
- json_timestamp_units: "1ms"

In agent section:

- hostname: The machine name that distinguishes different collection devices, and it is necessary to ensure its uniqueness
- metric_batch_size: 100, which is the max number of records per batch wriiten by Telegraf allowed. Increasing the number can reduce the request sending frequency of Telegraf.

For information on how to use Telegraf to collect data and more about using Telegraf, please refer to the official [document](https://docs.influxdata.com/telegraf/v1.11/) of Telegraf.

### Launch blm_telegraf

blm_telegraf has following options, which can be set to tune configurations of blm_telegraf when launching.

```sh
--host

The ip address of TDengine server, default is null

--batch-size

blm_prometheus assembles the received telegraf data into a TDengine writing request. This parameter controls the number of data pieces carried in a writing request sent to TDengine at a time.

--dbname

Set a name for the database created in TDengine, blm_telegraf will automatically create a database named dbname in TDengine, and the default value is prometheus.

--dbuser

Set the user name to access TDengine, the default value is 'root '

--dbpassword

Set the password to access TDengine, the default value is'taosdata '

--port

The port number blm_telegraf used to serve Telegraf.
```



### Example

Launch an API service for blm_telegraf with the following command

```bash
./blm_telegraf -host 127.0.0.1 -port 8089
```

Assuming that the IP address of the server where blm_telegraf located is "10.1.2. 3", the URL shall be added to the configuration file of telegraf as:

```yaml
url = "http://10.1.2.3:8089/telegraf"
```

### Query written data of telegraf

The format of generated data by telegraf is as follows:

```json
{
  "fields": {
    "usage_guest": 0, 
    "usage_guest_nice": 0,
    "usage_idle": 89.7897897897898, 
    "usage_iowait": 0,
    "usage_irq": 0,
    "usage_nice": 0,
    "usage_softirq": 0,
    "usage_steal": 0,
    "usage_system": 5.405405405405405, 
    "usage_user": 4.804804804804805
  },
  
  "name": "cpu", 
  "tags": {
    "cpu": "cpu2",
    "host": "bogon" 
  },
  "timestamp": 1576464360 
}
```

Where the name field is the name of the time-series data collected by telegraf, and the tag field is the tag of the time-series data. blm_telegraf automatically creates a STable in TDengine with the name of the time series data, and converts the tag field into the tag value of TDengine, with Timestamp as the timestamp and fields values as the value of the time-series data. Therefore, in the client of TDEngine, you can check whether this data was successfully written through the following instruction.

```mysql
use telegraf;

select * from cpu;
```

MQTT is a popular data transmission protocol in the IoT. TDengine can easily access the data received by MQTT Broker and write it to TDengine.

## <a class="anchor" id="emq"></a> Direct Writing of EMQ Broker

[EMQ](https://github.com/emqx/emqx) is an open source MQTT Broker software, with no need of coding, only to use "rules" in EMQ Dashboard for simple configuration, and MQTT data can be directly written into TDengine. EMQ X supports storing data to the TDengine by sending it to a Web service, and also provides a native TDengine driver on Enterprise Edition for direct data store. Please refer to [EMQ official documents](https://docs.emqx.io/broker/latest/cn/rule/rule-example.html#%E4%BF%9D%E5%AD%98%E6%95%B0%E6%8D%AE%E5%88%B0-tdengine) for more details.



## <a class="anchor" id="hivemq"></a> Direct Writing of HiveMQ Broker

[HiveMQ](https://www.hivemq.com/) is an MQTT agent that provides Free Personal and Enterprise Edition versions. It is mainly used for enterprises, emerging machine-to-machine(M2M) communication and internal transmission to meet scalability, easy management and security features. HiveMQ provides an open source plug-in development kit. You can store data to TDengine via HiveMQ extension-TDengine. Refer to the [HiveMQ extension-TDengine documentation](https://github.com/huskar-t/hivemq-tdengine-extension/blob/b62a26ecc164a310104df57691691b237e091c89/README.md) for more details.
