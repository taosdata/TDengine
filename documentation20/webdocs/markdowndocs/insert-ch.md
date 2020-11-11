# 高效写入数据

TDengine支持多种接口写入数据，包括SQL, Prometheus, Telegraf, EMQ MQTT Broker, HiveMQ Broker, CSV文件等，后续还将提供Kafka, OPC等接口。数据可以单条插入，也可以批量插入，可以插入一个数据采集点的数据，也可以同时插入多个数据采集点的数据。支持多线程插入，支持时间乱序数据插入，也支持历史数据插入。

## SQL写入

应用通过C/C++, JDBC, GO, 或Python Connector 执行SQL insert语句来插入数据，用户还可以通过TAOS Shell，手动输入SQL insert语句插入数据。比如下面这条insert 就将一条记录写入到表d1001中：
```mysql
INSERT INTO d1001 VALUES (1538548685000, 10.3, 219, 0.31);
```
TDengine支持一次写入多条记录，比如下面这条命令就将两条记录写入到表d1001中：
```mysql
INSERT INTO d1001 VALUES (1538548684000, 10.2, 220, 0.23) (1538548696650, 10.3, 218, 0.25);
```

TDengine也支持一次向多个表写入数据，比如下面这条命令就向d1001写入两条记录，向d1002写入一条记录：
```mysql
INSERT INTO d1001 VALUES (1538548685000, 10.3, 219, 0.31) (1538548695000, 12.6, 218, 0.33) d1002 VALUES (1538548696800, 12.3, 221, 0.31);
```

详细的SQL INSERT语法规则请见<a href="https://www.taosdata.com/cn/documentation20/taos-sql/">TAOS SQL </a>

**Tips:** 

- 要提高写入效率，需要批量写入。一批写入的记录条数越多，插入效率就越高。但一条记录不能超过16K，一条SQL语句总长度不能超过64K（可通过参数maxSQLLength配置，最大可配置为1M）。
- TDengine支持多线程同时写入，要进一步提高写入速度，一个客户端需要打开20个以上的线程同时写。但线程数达到一定数量后，无法再提高，甚至还会下降，因为线程切频繁切换，带来额外开销。
- 对同一张表，如果新插入记录的时间戳已经存在，新记录将被直接抛弃，也就是说，在一张表里，时间戳必须是唯一的。如果应用自动生成记录，很有可能生成的时间戳是一样的，这样，成功插入的记录条数会小于应用插入的记录条数。
- 写入的数据的时间戳必须大于当前时间减去配置参数keep的时间。如果keep配置为3650天，那么无法写入比3650天还老的数据。写入数据的时间戳也不能大于当前时间加配置参数days。如果days配置为2，那么无法写入比当前时间还晚2天的数据。

## Prometheus直接写入
[Prometheus](https://www.prometheus.io/)作为Cloud Native Computing Fundation毕业的项目，在性能监控以及K8S性能监控领域有着非常广泛的应用。TDengine提供一个小工具[Bailongma](https://github.com/taosdata/Bailongma)，只需在Prometheus做简单配置，无需任何代码，就可将Prometheus采集的数据直接写入TDengine，并按规则在TDengine自动创建库和相关表项。博文[用Docker容器快速搭建一个Devops监控Demo](https://www.taosdata.com/blog/2020/02/03/1189.html)即是采用bailongma将Prometheus和Telegraf的数据写入TDengine中的示例，可以参考。

### 从源代码编译blm_prometheus
用户需要从github下载[Bailongma](https://github.com/taosdata/Bailongma)的源码，使用Golang语言编译器编译生成可执行文件。在开始编译前，需要准备好以下条件：
- Linux操作系统的服务器
- 安装好Golang, 1.10版本以上
- 对应的TDengine版本。因为用到了TDengine的客户端动态链接库，因此需要安装好和服务端相同版本的TDengine程序；比如服务端版本是TDengine 2.0.0, 则在bailongma所在的linux服务器（可以与TDengine在同一台服务器，或者不同服务器）

Bailongma项目中有一个文件夹blm_prometheus，存放了prometheus的写入API程序。编译过程如下：
```
cd blm_prometheus
go build
```

一切正常的情况下，就会在对应的目录下生成一个blm_prometheus的可执行程序。

### 安装Prometheus
通过Prometheus的官网下载安装。[下载地址](https://prometheus.io/download/)

### 配置Prometheus
参考Prometheus的[配置文档](https://prometheus.io/docs/prometheus/latest/configuration/configuration/),在Prometheus的配置文件中的<remote_write>部分，增加以下配置

- url: bailongma API服务提供的URL, 参考下面的blm_prometheus启动示例章节

启动Prometheus后，可以通过taos客户端查询确认数据是否成功写入。

### 启动blm_prometheus程序
blm_prometheus程序有以下选项，在启动blm_prometheus程序时可以通过设定这些选项来设定blm_prometheus的配置。
```sh
--tdengine-name
如果TDengine安装在一台具备域名的服务器上，也可以通过配置TDengine的域名来访问TDengine。在K8S环境下，可以配置成TDengine所运行的service name

--batch-size 
blm_prometheus会将收到的prometheus的数据拼装成TDengine的写入请求，这个参数控制一次发给TDengine的写入请求中携带的数据条数。

--dbname
设置在TDengine中创建的数据库名称，blm_prometheus会自动在TDengine中创建一个以dbname为名称的数据库，缺省值是prometheus。

--dbuser
设置访问TDengine的用户名，缺省值是'root'

--dbpassword
设置访问TDengine的密码，缺省值是'taosdata'

--port
blm_prometheus对prometheus提供服务的端口号。
```

### 启动示例

通过以下命令启动一个blm_prometheus的API服务
```
./blm_prometheus -port 8088
```
假设blm_prometheus所在服务器的IP地址为"10.1.2.3"，则在prometheus的配置文件中<remote_write>部分增加url为
```yaml
remote_write:
  - url: "http://10.1.2.3:8088/receive"
```

### 查询prometheus写入数据
prometheus产生的数据格式如下：
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
其中，apiserver_request_latencies_bucket为prometheus采集的时序数据的名称，后面{}中的为该时序数据的标签。blm_prometheus会以时序数据的名称在TDengine中自动创建一个超级表，并将{}中的标签转换成TDengine的tag值，Timestamp作为时间戳，value作为该时序数据的值。因此在TDengine的客户端中，可以通过以下指令查到这个数据是否成功写入。
```
use prometheus;
select * from apiserver_request_latencies_bucket;
```

## Telegraf直接写入
[Telegraf](https://www.influxdata.com/time-series-platform/telegraf/)是一流行的IT运维数据采集开源工具，TDengine提供一个小工具[Bailongma](https://github.com/taosdata/Bailongma)，只需在Telegraf做简单配置，无需任何代码，就可将Telegraf采集的数据直接写入TDengine，并按规则在TDengine自动创建库和相关表项。博文[用Docker容器快速搭建一个Devops监控Demo](https://www.taosdata.com/blog/2020/02/03/1189.html)即是采用bailongma将Prometheus和Telegraf的数据写入TDengine中的示例，可以参考。

### 从源代码编译blm_telegraf
用户需要从github下载[Bailongma](https://github.com/taosdata/Bailongma)的源码，使用Golang语言编译器编译生成可执行文件。在开始编译前，需要准备好以下条件：

- Linux操作系统的服务器
- 安装好Golang, 1.10版本以上
- 对应的TDengine版本。因为用到了TDengine的客户端动态链接库，因此需要安装好和服务端相同版本的TDengine程序；比如服务端版本是TDengine 2.0.0, 则在bailongma所在的linux服务器（可以与TDengine在同一台服务器，或者不同服务器）

Bailongma项目中有一个文件夹blm_telegraf，存放了Telegraf的写入API程序。编译过程如下：

```
cd blm_telegraf
go build
```

一切正常的情况下，就会在对应的目录下生成一个blm_telegraf的可执行程序。

### 安装Telegraf
目前TDengine支持Telegraf 1.7.4以上的版本。用户可以根据当前的操作系统，到Telegraf官网下载安装包，并执行安装。下载地址如下：<a href='https://portal.influxdata.com/downloads'>https://portal.influxdata.com/downloads</a>

### 配置Telegraf
修改Telegraf配置文件/etc/telegraf/telegraf.conf中与TDengine有关的配置项。 

在output plugins部分，增加[[outputs.http]]配置项： 

- url： bailongma API服务提供的URL, 参考下面的启动示例章节
- data_format: "json"
- json_timestamp_units:      "1ms"

在agent部分：

- hostname: 区分不同采集设备的机器名称，需确保其唯一性
- metric_batch_size: 100，允许Telegraf每批次写入记录最大数量，增大其数量可以降低Telegraf的请求发送频率。

关于如何使用Telegraf采集数据以及更多有关使用Telegraf的信息，请参考Telegraf官方的[文档](https://docs.influxdata.com/telegraf/v1.11/)。

### 启动blm_telegraf程序
blm_telegraf程序有以下选项，在启动blm_telegraf程序时可以通过设定这些选项来设定blm_telegraf的配置。

```sh
--host 
TDengine服务端的IP地址，缺省值为空

--batch-size 
blm_telegraf会将收到的telegraf的数据拼装成TDengine的写入请求，这个参数控制一次发给TDengine的写入请求中携带的数据条数。

--dbname
设置在TDengine中创建的数据库名称，blm_telegraf会自动在TDengine中创建一个以dbname为名称的数据库，缺省值是prometheus。

--dbuser
设置访问TDengine的用户名，缺省值是'root'

--dbpassword
设置访问TDengine的密码，缺省值是'taosdata'

--port
blm_telegraf对telegraf提供服务的端口号。
```

### 启动示例
通过以下命令启动一个blm_telegraf的API服务
```
./blm_telegraf -host 127.0.0.1 -port 8089
```

假设blm_telegraf所在服务器的IP地址为"10.1.2.3"，则在telegraf的配置文件中, 在output plugins部分，增加[[outputs.http]]配置项： 

```yaml
url = "http://10.1.2.3:8089/telegraf"
```

### 查询telegraf写入数据
telegraf产生的数据格式如下：
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

其中，name字段为telegraf采集的时序数据的名称，tags字段为该时序数据的标签。blm_telegraf会以时序数据的名称在TDengine中自动创建一个超级表，并将tags字段中的标签转换成TDengine的tag值，Timestamp作为时间戳，fields字段中的值作为该时序数据的值。因此在TDengine的客户端中，可以通过以下指令查到这个数据是否成功写入。

```
use telegraf;
select * from cpu;
```

## MQTT Broker直接写入

MQTT是一流行的物联网数据传输协议，TDengine 可以很方便的接入 MQTT Broker 接受的数据并写入到 TDengine。

### EMQ Broker

[EMQ](https://github.com/emqx/emqx)是一开源的MQTT Broker软件，无需任何代码，只需要在EMQ Dashboard里使用“规则”做简单配置，即可将MQTT的数据直接写入TDengine。EMQ X 支持通过 发送到 Web 服务 的方式保存数据到 TDengine，也在企业版上提供原生的 TDEngine 驱动实现直接保存。详细使用方法请参考 [EMQ 官方文档](https://docs.emqx.io/broker/latest/cn/rule/rule-example.html#%E4%BF%9D%E5%AD%98%E6%95%B0%E6%8D%AE%E5%88%B0-tdengine)。

### HiveMQ Broker

[HiveMQ](https://www.hivemq.com/) 是一个提供免费个人版和企业版的 MQTT 代理，主要用于企业和新兴的机器到机器M2M通讯和内部传输，满足可伸缩性、易管理和安全特性。HiveMQ 提供了开源的插件开发包。可以通过 HiveMQ extension - TDengine 保存数据到 TDengine。详细使用方法请参考 [HiveMQ extension - TDengine 说明文档](https://github.com/huskar-t/hivemq-tdengine-extension/blob/b62a26ecc164a310104df57691691b237e091c89/README.md)。

