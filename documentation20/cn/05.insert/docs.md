# 高效写入数据

TDengine支持多种接口写入数据，包括SQL，Prometheus，Telegraf，collectd，StatsD，EMQ MQTT Broker，HiveMQ Broker，CSV文件等，后续还将提供Kafka，OPC等接口。数据可以单条插入，也可以批量插入，可以插入一个数据采集点的数据，也可以同时插入多个数据采集点的数据。支持多线程插入，支持时间乱序数据插入，也支持历史数据插入。

## <a class="anchor" id="sql"></a>SQL 写入

应用通过C/C++, Java, Go, C#, Python, Node.js 连接器执行SQL insert语句来插入数据，用户还可以通过TAOS Shell，手动输入SQL insert语句插入数据。比如下面这条insert 就将一条记录写入到表d1001中：
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

详细的SQL INSERT语法规则请见 [TAOS SQL 的数据写入](https://www.taosdata.com/cn/documentation/taos-sql#insert) 章节。

**Tips:** 

- 要提高写入效率，需要批量写入。一批写入的记录条数越多，插入效率就越高。但一条记录不能超过16K，一条SQL语句总长度不能超过64K（可通过参数maxSQLLength配置，最大可配置为1M）。
- TDengine支持多线程同时写入，要进一步提高写入速度，一个客户端需要打开20个以上的线程同时写。但线程数达到一定数量后，无法再提高，甚至还会下降，因为线程频繁切换，带来额外开销。
- 对同一张表，如果新插入记录的时间戳已经存在，默认情形下（UPDATE=0）新记录将被直接抛弃，也就是说，在一张表里，时间戳必须是唯一的。如果应用自动生成记录，很有可能生成的时间戳是一样的，这样，成功插入的记录条数会小于应用插入的记录条数。如果在创建数据库时使用了 UPDATE 1 选项，插入相同时间戳的新记录将覆盖原有记录。
- 写入的数据的时间戳必须大于当前时间减去配置参数keep的时间。如果keep配置为3650天，那么无法写入比3650天还早的数据。写入数据的时间戳也不能大于当前时间加配置参数days。如果days为2，那么无法写入比当前时间还晚2天的数据。

## <a class="anchor" id="schemaless"></a>无模式（Schemaless）写入
**前言**
<br/>在物联网应用中，常会采集比较多的数据项，用于实现智能控制、业务分析、设备监控等。由于应用逻辑的版本升级，或者设备自身的硬件调整等原因，数据采集项就有可能比较频繁地出现变动。为了在这种情况下方便地完成数据记录工作，TDengine 从 2.2.0.0 版本开始，提供调用 Schemaless 写入方式，可以免于预先创建超级表/子表的步骤，随着数据写入写入接口能够自动创建与数据对应的存储结构。并且在必要时，Schemaless 将自动增加必要的数据列，保证用户写入的数据可以被正确存储。
<br/>目前，TDengine 的 C/C++ Connector 提供支持 Schemaless 的操作接口，详情请参见 [Schemaless 方式写入接口](https://www.taosdata.com/cn/documentation/connector#schemaless)章节。这里对 Schemaless 的数据表达格式进行了描述。
<br/>无模式写入方式建立的超级表及其对应的子表与通过 SQL 直接建立的超级表和子表完全没有区别，您也可以通过 SQL 语句直接向其中写入数据。需要注意的是，通过无模式写入方式建立的表，其表名是基于标签值按照固定的映射规则生成，所以无法明确地进行表意，缺乏可读性。

**无模式写入行协议**
<br/>TDengine 的无模式写入的行协议兼容 InfluxDB 的 行协议（Line Protocol）、OpenTSDB 的 telnet 行协议、OpenTSDB 的 Json 格式协议。但是使用这三种协议的时候，需要在 API 中指定输入内容使用解析协议的标准。

对于InfluxDB、OpenTSDB的标准写入协议请参考各自的文档。下面首先以 InfluxDB 的行协议为基础，介绍 TDengine 扩展的协议内容，允许用户采用更加精细的方式控制（超级表）模式。

Schemaless 采用一个字符串来表达一个数据行（可以向写入 API 中一次传入多行字符串来实现多个数据行的批量写入），其格式约定如下：
```json
measurement,tag_set field_set timestamp
```

其中，
* measurement 将作为数据表名。它与 tag_set 之间使用一个英文逗号来分隔。
* tag_set 将作为标签数据，其格式形如 `<tag_key>=<tag_value>,<tag_key>=<tag_value>`，也即可以使用英文逗号来分隔多个标签数据。它与 field_set 之间使用一个半角空格来分隔。
* field_set 将作为普通列数据，其格式形如 `<field_key>=<field_value>,<field_key>=<field_value>`，同样是使用英文逗号来分隔多个普通列的数据。它与 timestamp 之间使用一个半角空格来分隔。
* timestamp 即本行数据对应的主键时间戳。

tag_set 中的所有的数据自动转化为 nchar 数据类型，并不需要使用双引号（")。
<br/>在无模式写入数据行协议中，field_set 中的每个数据项都需要对自身的数据类型进行描述。具体来说：
* 如果两边有英文双引号，表示 BIANRY(32) 类型。例如 `"abc"`。
* 如果两边有英文双引号而且带有 L 前缀，表示 NCHAR(32) 类型。例如 `L"报错信息"`。
* 对空格、等号（=）、逗号（,）、双引号（"），前面需要使用反斜杠（\）进行转义。（都指的是英文半角符号）
* 数值类型将通过后缀来区分数据类型：

| **序号** | **后缀** | **映射类型** | **大小(字节)** | 
| -- | -------  | ---------| ------ |
| 1  | 无或f64  |  double  |  8     |
| 2  | f32     |  float    |  4    |
| 3  | i8      |  TinyInt  |  1    |
| 4  | i16     |  SmallInt |  2    |
| 5  | i32     |  Int      |  4    |
| 6  | i64或i  |  Bigint   |  8    |
* t, T, true, True, TRUE, f, F, false, False 将直接作为 BOOL 型来处理。
<br/>例如如下数据行表示：向名为 st 的超级表下的 t1 标签为 "3"（NCHAR）、t2 标签为 "4"（NCHAR）、t3 标签为 "t3"（NCHAR）的数据子表，写入 c1 列为 3（BIGINT）、c2 列为 false（BOOL）、c3 列为 "passit"（BINARY）、c4 列为 4（DOUBLE）、主键时间戳为 1626006833639000000 的一行数据。
```json
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4f64 1626006833639000000
```
需要注意的是，如果描述数据类型后缀时使用了错误的大小写，或者为数据指定的数据类型有误，均可能引发报错提示而导致数据写入失败。

### 无模式写入的主要处理逻辑

无模式写入按照如下原则来处理行数据：
1. 当 tag_set 中有 ID 字段时，该字段的值将作为子表的表名。
2. 没有 ID 字段时，将使用如下规则来生成子表名：
首先将measurement 的名称和标签的 key 和 value 组合成为如下的字符串
```json
"measurement,tag_key1=tag_value1,tag_key2=tag_value2"
```
需要注意的是，这里的tag_key1, tag_key2并不是用户输入的标签的原始顺序，而是使用了标签名称按照字符串升序排列后的结果。所以，tag_key1 并不是在行协议中输入的第一个标签。
排列完成以后计算该字符串的 MD5 散列值 "md5_val"。然后将计算的结果与字符串组合生成表名：“t_md5_val”。其中的 “t_” 是固定的前缀，每个通过该映射关系自动生成的表都具有该前缀。
<br/>3. 如果解析行协议获得的超级表不存在，则会创建这个超级表。
<br/>4. 如果解析行协议获得子表不存在，则 Schemaless 会按照步骤 1 或 2 确定的子表名来创建子表。
<br/>5. 如果数据行中指定的标签列或普通列不存在，则在超级表中增加对应的标签列或普通列（只增不减）。
<br/>6. 如果超级表中存在一些标签列或普通列未在一个数据行中被指定取值，那么这些列的值在这一行中会被置为 NULL。
<br/>7. 对 BINARY 或 NCHAR 列，如果数据行中所提供值的长度超出了列类型的限制，自动增加该列允许存储的字符长度上限（只增不减），以保证数据的完整保存。
<br/>8. 如果指定的数据子表已经存在，而且本次指定的标签列取值跟已保存的值不一样，那么最新的数据行中的值会覆盖旧的标签列取值。
<br/>9. 整个处理过程中遇到的错误会中断写入过程，并返回错误代码。

**备注：**
<br/>无模式所有的处理逻辑，仍会遵循 TDengine 对数据结构的底层限制，例如每行数据的总长度不能超过 16k 字节。这方面的具体限制约束请参见 [TAOS SQL 边界限制](https://www.taosdata.com/cn/documentation/taos-sql#limitation) 章节。

**时间分辨率识别**
<br/>无模式写入过程中支持三个指定的模式，具体如下

| **序号** | **值**        | **说明** |
| ---- | ------------------- | ------------ |
| 1    | SML_LINE_PROTOCOL           |    InfluxDB行协议（Line Protocol)   |
| 2    | SML_TELNET_PROTOCOL              |    OpenTSDB文本行协议   |
| 3    | SML_JSON_PROTOCOL              |    Json协议格式   |

<br/>在 SML_LINE_PROTOCOL 解析模式下，需要用户指定输入的时间戳的时间分辨率。可用的时间分辨率如下表所示：<br/>

| **序号** | **时间分辨率定义**        | **含义** |
| ---- | ----------------------------- | --------- |
| 1    | TSDB_SML_TIMESTAMP_NOT_CONFIGURED     | 未定义（无效） |
| 2    | TSDB_SML_TIMESTAMP_HOURS              |   小时        |
| 3    | TSDB_SML_TIMESTAMP_MINUTES            |   分钟        |
| 4    | TSDB_SML_TIMESTAMP_SECONDS            |   秒          |
| 5    | TSDB_SML_TIMESTAMP_MILLI_SECONDS      |   毫秒        |
| 6    | TSDB_SML_TIMESTAMP_MICRO_SECONDS      |   微秒        |
| 7    | TSDB_SML_TIMESTAMP_NANO_SECONDS       |   纳秒        |

在 SML_TELNET_PROTOCOL 和 SML_JSON_PROTOCOL 模式下，根据时间戳的长度来确定时间精度（与 OpenTSDB 标准操作方式相同），此时会忽略用户指定的时间分辨率。

**数据模式变更处理**
<br/>本节将说明不同行数据写入情况下，对于数据模式的影响。

在使用行协议写入一个明确的标识的字段类型的时候，后续更改该字段的类型定义，会出现明确的数据模式错误，即会触发写入 API 报告错误。如下所示，
```json
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4    1626006833639000000
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4i   1626006833640000000
```
第一行的数据类型映射将 c4 列定义为 Double， 但是第二行的数据又通过数值后缀方式声明该列为 BigInt， 由此会触发无模式写入的解析错误。

如果列前面的行协议将数据列声明为了 binary， 后续的要求长度更长的binary长度，此时会触发超级表模式的变更。
```json
st,t1=3,t2=4,t3=t3 c1=3i64,c5="pass"     1626006833639000000
st,t1=3,t2=4,t3=t3 c1=3i64,c5="passit"   1626006833640000000
```
第一行中行协议解析会声明 c5 列是一个 binary(4)的字段，第二次行数据写入会提取列 c5 仍然是 binary 列，但是其宽度为 6，此时需要将binary的宽度增加到能够容纳 新字符串的宽度。
```json
st,t1=3,t2=4,t3=t3 c1=3i64               1626006833639000000
st,t1=3,t2=4,t3=t3 c1=3i64,c6="passit"   1626006833640000000
```
第二行数据相对于第一行来说增加了一个列 c6，类型为binary(6)。那么此时会自动增加一个列 c6， 类型为  binary(6)。

**写入完整性**
<br/>TDengine 提供数据写入的幂等性保证，即您可以反复调用 API 进行出错数据的写入操作。但是不提供多行数据写入的原子性保证。即在多行数据一批次写入过程中，会出现部分数据写入成功，部分数据写入失败的情况。

**错误码**
<br/>如果是无模式写入过程中的数据本身错误，应用会得到 TSDB_CODE_TSC_LINE_SYNTAX_ERROR 错误信息，该错误信息表明错误发生在写入文本中。其他的错误码与原系统一致，可以通过 taos_errstr 获取具体的错误原因。

**后续升级计划**
<br/>当前版本只提供了 C 版本的 API，后续将提供 其他高级语言的 API，例如 Java/Go/Python/C# 等。此外，在TDengine v2.3及后续版本中，您还可以通过 BLM v3 采用 REST 的方式直接写入无模式数据。


## <a class="anchor" id="prometheus"></a>Prometheus 直接写入

[Prometheus](https://www.prometheus.io/)作为Cloud Native Computing Fundation毕业的项目，在性能监控以及K8S性能监控领域有着非常广泛的应用。TDengine提供一个小工具[Bailongma](https://github.com/taosdata/Bailongma)，只需对Prometheus做简单配置，无需任何代码，就可将Prometheus采集的数据直接写入TDengine，并按规则在TDengine自动创建库和相关表项。博文[用Docker容器快速搭建一个Devops监控Demo](https://www.taosdata.com/blog/2020/02/03/1189.html)即是采用Bailongma将Prometheus和Telegraf的数据写入TDengine中的示例，可以参考。

### 从源代码编译 taosadapter_prometheus

用户需要从github下载[Bailongma](https://github.com/taosdata/Bailongma)的源码，使用Golang语言编译器编译生成可执行文件。在开始编译前，需要准备好以下条件：
- Linux操作系统的服务器
- 安装好Golang，1.14版本以上
- 对应的TDengine版本。因为用到了TDengine的客户端动态链接库，因此需要安装好和服务端相同版本的TDengine程序；比如服务端版本是TDengine 2.0.0, 则在Bailongma所在的Linux服务器（可以与TDengine在同一台服务器，或者不同服务器）

Bailongma项目中有一个文件夹taosadapter_prometheus，存放了prometheus的写入API程序。编译过程如下：
```bash
cd taosadapter_prometheus
go build
```

一切正常的情况下，就会在对应的目录下生成一个taosadapter_prometheus的可执行程序。

### 安装 Prometheus

通过Prometheus的官网下载安装。具体请见：[下载地址](https://prometheus.io/download/)。

### 配置 Prometheus

参考Prometheus的[配置文档](https://prometheus.io/docs/prometheus/latest/configuration/configuration/)，在Prometheus的配置文件中的<remote_write>部分，增加以下配置：

```
  - url: "bailongma API服务提供的URL"（参考下面的taosadapter_prometheus启动示例章节）
```

启动Prometheus后，可以通过taos客户端查询确认数据是否成功写入。

### 启动 taosadapter_prometheus 程序

taosadapter_prometheus程序有以下选项，在启动taosadapter_prometheus程序时可以通过设定这些选项来设定taosadapter_prometheus的配置。
```bash
--tdengine-name
如果TDengine安装在一台具备域名的服务器上，也可以通过配置TDengine的域名来访问TDengine。在K8S环境下，可以配置成TDengine所运行的service name。

--batch-size
taosadapter_prometheus会将收到的prometheus的数据拼装成TDengine的写入请求，这个参数控制一次发给TDengine的写入请求中携带的数据条数。

--dbname
设置在TDengine中创建的数据库名称，taosadapter_prometheus会自动在TDengine中创建一个以dbname为名称的数据库，缺省值是prometheus。

--dbuser
设置访问TDengine的用户名，缺省值是'root'。

--dbpassword
设置访问TDengine的密码，缺省值是'taosdata'。

--port
taosadapter_prometheus对prometheus提供服务的端口号。
```

### 启动示例

通过以下命令启动一个taosadapter_prometheus的API服务
```bash
./taosadapter_prometheus -port 8088
```
假设taosadapter_prometheus所在服务器的IP地址为"10.1.2.3"，则在prometheus的配置文件中<remote_write>部分增加url为
```yaml
remote_write:
  - url: "http://10.1.2.3:8088/receive"
```

### 查询 prometheus 写入数据

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
    resource="persistentvolumes", 
    scope="cluster",
    verb="LIST", 
    version="v1" 
  }
}
```
其中，apiserver_request_latencies_bucket为prometheus采集的时序数据的名称，后面{}中的为该时序数据的标签。taosadapter_prometheus会以时序数据的名称在TDengine中自动创建一个超级表，并将{}中的标签转换成TDengine的tag值，Timestamp作为时间戳，value作为该时序数据的值。因此在TDengine的客户端中，可以通过以下指令查到这个数据是否成功写入。
```mysql
use prometheus;
select * from apiserver_request_latencies_bucket;
```

## <a class="anchor" id="telegraf"></a> Telegraf 直接写入(通过 taosadapter)
安装 Telegraf 请参考[官方文档](https://portal.influxdata.com/downloads/)。

TDengine 新版本（2.3.0.0+）包含一个 taosadapter 独立程序，负责接收包括 Telegraf 的多种应用的数据写入。

配置方法，在 /etc/telegraf/telegraf.conf 增加如下文字，其中 database name 请填写希望在 TDengine 保存 Telegraf 数据的数据库名，TDengine server/cluster host、username和 password 填写 TDengine 实际值：
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

然后重启 telegraf：
```
sudo systemctl start telegraf
```
即可在 TDengine 中查询 metrics 数据库中 Telegraf 写入的数据。

taosadapter 相关配置参数请参考 taosadapter --help 命令输出以及相关文档。

## <a class="anchor" id="collectd"></a> collectd 直接写入(通过 taosadapter)
安装 collectd，请参考[官方文档](https://collectd.org/download.shtml)。

TDengine 新版本（2.3.0.0+）包含一个 taosadapter 独立程序，负责接收包括 collectd 的多种应用的数据写入。

在 /etc/collectd/collectd.conf 文件中增加如下内容，其中 host 和 port 请填写 TDengine 和 taosadapter 配置的实际值：
```
LoadPlugin network
<Plugin network>
  Server "<TDengine cluster/server host>" "<port for collectd>"
</Plugin>
```
重启 collectd
```
sudo systemctl start collectd
```
taosadapter 相关配置参数请参考 taosadapter --help 命令输出以及相关文档。

## <a class="anchor" id="statsd"></a> StatsD 直接写入(通过 taosadapter)
安装 StatsD
请参考[官方文档](https://github.com/statsd/statsd)。

TDengine 新版本（2.3.0.0+）包含一个 taosadapter 独立程序，负责接收包括 StatsD 的多种应用的数据写入。

在 config.js 文件中增加如下内容后启动 StatsD，其中 host 和 port 请填写 TDengine 和 taosadapter 配置的实际值：
```
backends 部分添加 "./backends/repeater"
repeater 部分添加 { host:'<TDengine server/cluster host>', port: <port for StatsD>}
```

示例配置文件：
```
{
port: 8125
, backends: ["./backends/repeater"]
, repeater: [{ host: '127.0.0.1', port: 6044}]
}
```

taosadapter 相关配置参数请参考 taosadapter --help 命令输出以及相关文档。


## <a class="anchor" id="taosadapter2-telegraf"></a> 使用 Bailongma 2.0 接入 Telegraf 数据写入

*注意：TDengine 新版本（2.3.0.0+）提供新版本 Bailongma ，命名为 taosadapter ，提供更简便的 Telegraf 数据写入以及其他更强大的功能，Bailongma v2 即之前版本将逐步不再维护。

[Telegraf](https://www.influxdata.com/time-series-platform/telegraf/)是一流行的IT运维数据采集开源工具，TDengine提供一个小工具[Bailongma](https://github.com/taosdata/Bailongma)，只需在Telegraf做简单配置，无需任何代码，就可将Telegraf采集的数据直接写入TDengine，并按规则在TDengine自动创建库和相关表项。博文[用Docker容器快速搭建一个Devops监控Demo](https://www.taosdata.com/blog/2020/02/03/1189.html)即是采用bailongma将Prometheus和Telegraf的数据写入TDengine中的示例，可以参考。

### 从源代码编译 taosadapter_telegraf

用户需要从github下载[Bailongma](https://github.com/taosdata/Bailongma)的源码，使用Golang语言编译器编译生成可执行文件。在开始编译前，需要准备好以下条件：

- Linux操作系统的服务器
- 安装好Golang，1.10版本以上
- 对应的TDengine版本。因为用到了TDengine的客户端动态链接库，因此需要安装好和服务端相同版本的TDengine程序；比如服务端版本是TDengine 2.0.0, 则在Bailongma所在的Linux服务器（可以与TDengine在同一台服务器，或者不同服务器）

Bailongma项目中有一个文件夹taosadapter_telegraf，存放了Telegraf的写入API程序。编译过程如下：

```bash
cd taosadapter_telegraf
go build
```

一切正常的情况下，就会在对应的目录下生成一个taosadapter_telegraf的可执行程序。

### 安装 Telegraf

目前TDengine支持Telegraf 1.7.4以上的版本。用户可以根据当前的操作系统，到Telegraf官网下载安装包，并执行安装。下载地址如下：https://portal.influxdata.com/downloads 。

### 配置 Telegraf

修改Telegraf配置文件/etc/telegraf/telegraf.conf中与TDengine有关的配置项。 

在output plugins部分，增加[[outputs.http]]配置项：

- url：Bailongma API服务提供的URL，参考下面的启动示例章节
- data_format："json"
- json_timestamp_units："1ms"

在agent部分：

- hostname: 区分不同采集设备的机器名称，需确保其唯一性。
- metric_batch_size: 100，允许Telegraf每批次写入记录最大数量，增大其数量可以降低Telegraf的请求发送频率。

关于如何使用Telegraf采集数据以及更多有关使用Telegraf的信息，请参考Telegraf官方的[文档](https://docs.influxdata.com/telegraf/v1.11/)。

### 启动 taosadapter_telegraf 程序

taosadapter_telegraf程序有以下选项，在启动taosadapter_telegraf程序时可以通过设定这些选项来设定taosadapter_telegraf的配置。

```bash
--host
TDengine服务端的IP地址，缺省值为空。

--batch-size
taosadapter_telegraf会将收到的telegraf的数据拼装成TDengine的写入请求，这个参数控制一次发给TDengine的写入请求中携带的数据条数。

--dbname
设置在TDengine中创建的数据库名称，taosadapter_telegraf会自动在TDengine中创建一个以dbname为名称的数据库，缺省值是prometheus。

--dbuser
设置访问TDengine的用户名，缺省值是'root'。

--dbpassword
设置访问TDengine的密码，缺省值是'taosdata'。

--port
taosadapter_telegraf对telegraf提供服务的端口号。
```

### 启动示例

通过以下命令启动一个taosadapter_telegraf的API服务：
```bash
./taosadapter_telegraf -host 127.0.0.1 -port 8089
```

假设taosadapter_telegraf所在服务器的IP地址为"10.1.2.3"，则在telegraf的配置文件中, 在output plugins部分，增加[[outputs.http]]配置项：

```yaml
url = "http://10.1.2.3:8089/telegraf"
```

### 查询 telegraf 写入数据

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

其中，name字段为telegraf采集的时序数据的名称，tags字段为该时序数据的标签。taosadapter_telegraf会以时序数据的名称在TDengine中自动创建一个超级表，并将tags字段中的标签转换成TDengine的tag值，timestamp作为时间戳，fields字段中的值作为该时序数据的值。因此在TDengine的客户端中，可以通过以下指令查到这个数据是否成功写入。

```mysql
use telegraf;
select * from cpu;
```

## <a class="anchor" id="emq"></a>EMQ Broker 直接写入

MQTT是流行的物联网数据传输协议，[EMQ](https://github.com/emqx/emqx)是一开源的MQTT Broker软件，无需任何代码，只需要在EMQ Dashboard里使用“规则”做简单配置，即可将MQTT的数据直接写入TDengine。EMQ X 支持通过 发送到 Web 服务的方式保存数据到 TDEngine，也在企业版上提供原生的 TDEngine 驱动实现直接保存。详细使用方法请参考 [EMQ 官方文档](https://docs.emqx.io/broker/latest/cn/rule/rule-example.html#%E4%BF%9D%E5%AD%98%E6%95%B0%E6%8D%AE%E5%88%B0-tdengine)。

## <a class="anchor" id="hivemq"></a>HiveMQ Broker 直接写入

[HiveMQ](https://www.hivemq.com/) 是一个提供免费个人版和企业版的 MQTT 代理，主要用于企业和新兴的机器到机器M2M通讯和内部传输，满足可伸缩性、易管理和安全特性。HiveMQ 提供了开源的插件开发包。可以通过 HiveMQ extension - TDengine 保存数据到 TDengine。详细使用方法请参考 [HiveMQ extension - TDengine 说明文档](https://github.com/huskar-t/hivemq-tdengine-extension/blob/b62a26ecc164a310104df57691691b237e091c89/README.md)。
