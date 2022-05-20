# 高效写入数据

TDengine 支持多种接口写入数据，包括 SQL，Prometheus，Telegraf，collectd，StatsD，EMQX MQTT Broker，HiveMQ Broker，CSV 文件等，后续还将提供 Kafka，OPC 等接口。数据可以单条插入，也可以批量插入，可以插入一个数据采集点的数据，也可以同时插入多个数据采集点的数据。支持多线程插入，支持时间乱序数据插入，也支持历史数据插入。

## <a class="anchor" id="sql"></a>SQL 写入

应用通过 C/C++, Java, Go, C#, Python, Node.js 连接器执行 SQL insert 语句来插入数据，用户还可以通过 TAOS Shell，手动输入 SQL insert 语句插入数据。比如下面这条 insert 就将一条记录写入到表 d1001 中：

```mysql
INSERT INTO d1001 VALUES (1538548685000, 10.3, 219, 0.31);
```

TDengine 支持一次写入多条记录，比如下面这条命令就将两条记录写入到表 d1001 中：

```mysql
INSERT INTO d1001 VALUES (1538548684000, 10.2, 220, 0.23) (1538548696650, 10.3, 218, 0.25);
```

TDengine 也支持一次向多个表写入数据，比如下面这条命令就向 d1001 写入两条记录，向 d1002 写入一条记录：

```mysql
INSERT INTO d1001 VALUES (1538548685000, 10.3, 219, 0.31) (1538548695000, 12.6, 218, 0.33) d1002 VALUES (1538548696800, 12.3, 221, 0.31);
```

详细的 SQL INSERT 语法规则请见 [TAOS SQL 的数据写入](https://www.taosdata.com/cn/documentation/taos-sql#insert) 章节。

**Tips:**

- 要提高写入效率，需要批量写入。一批写入的记录条数越多，插入效率就越高。但一条记录不能超过 48K（2.1.7.0 之前的版本为 16K），一条 SQL 语句总长度不能超过 1M 。
- TDengine 支持多线程同时写入，要进一步提高写入速度，一个客户端需要打开 20 个以上的线程同时写。但线程数达到一定数量后，无法再提高，甚至还会下降，因为线程频繁切换，带来额外开销。
- 对同一张表，如果新插入记录的时间戳已经存在，默认情形下（UPDATE=0）新记录将被直接抛弃，也就是说，在一张表里，时间戳必须是唯一的。如果应用自动生成记录，很有可能生成的时间戳是一样的，这样，成功插入的记录条数会小于应用插入的记录条数。如果在创建数据库时使用了 UPDATE 1 选项，插入相同时间戳的新记录将覆盖原有记录。
- 写入的数据的时间戳必须大于当前时间减去配置参数 keep 的时间。如果 keep 配置为3650天，那么无法写入比 3650 天还早的数据。写入数据的时间戳也不能大于当前时间加配置参数 days。如果 days 为 2，那么无法写入比当前时间还晚2天的数据。

## <a class="anchor" id="schemaless"></a>无模式（Schemaless）写入

**前言**
<br/>在物联网应用中，常会采集比较多的数据项，用于实现智能控制、业务分析、设备监控等。由于应用逻辑的版本升级，或者设备自身的硬件调整等原因，数据采集项就有可能比较频繁地出现变动。为了在这种情况下方便地完成数据记录工作，TDengine 从 2.2.0.0 版本开始，提供调用 Schemaless 写入方式，可以免于预先创建超级表/子表的步骤，随着数据写入接口能够自动创建与数据对应的存储结构。并且在必要时，Schemaless 将自动增加必要的数据列，保证用户写入的数据可以被正确存储。
<br/>目前，TDengine 的所有官方支持的连接器支持 Schemaless 的操作接口，详情请参见 [Schemaless 方式写入接口](https://www.taosdata.com/cn/documentation/connector#schemaless)章节。这里对 Schemaless 的数据表达格式进行了描述。
<br/>无模式写入方式建立的超级表及其对应的子表与通过 SQL 直接建立的超级表和子表完全没有区别，您也可以通过 SQL 语句直接向其中写入数据。需要注意的是，通过无模式写入方式建立的表，其表名是基于标签值按照固定的映射规则生成，所以无法明确地进行表意，缺乏可读性。

**无模式写入行协议**
<br/>TDengine 的无模式写入的行协议兼容 InfluxDB 的 行协议（Line Protocol）、OpenTSDB 的 telnet 行协议、OpenTSDB 的 JSON 格式协议。但是使用这三种协议的时候，需要在 API 中指定输入内容使用解析协议的标准。

对于 InfluxDB、OpenTSDB 的标准写入协议请参考各自的文档。下面首先以 InfluxDB 的行协议为基础，介绍 TDengine 扩展的协议内容，允许用户采用更加精细的方式控制（超级表）模式。

Schemaless 采用一个字符串来表达一个数据行（可以向写入 API 中一次传入多行字符串来实现多个数据行的批量写入），其格式约定如下：

```json
measurement,tag_set field_set timestamp
```

其中:

* measurement 将作为数据表名。它与 tag_set 之间使用一个英文逗号来分隔。
* tag_set 将作为标签数据，其格式形如 `<tag_key>=<tag_value>,<tag_key>=<tag_value>`，也即可以使用英文逗号来分隔多个标签数据。它与 field_set 之间使用一个半角空格来分隔。
* field_set 将作为普通列数据，其格式形如 `<field_key>=<field_value>,<field_key>=<field_value>`，同样是使用英文逗号来分隔多个普通列的数据。它与 timestamp 之间使用一个半角空格来分隔。
* timestamp 即本行数据对应的主键时间戳。

tag_set 中的所有的数据自动转化为 nchar 数据类型，并不需要使用双引号（")。
<br/>在无模式写入数据行协议中，field_set 中的每个数据项都需要对自身的数据类型进行描述。具体来说：

* 如果两边有英文双引号，表示 BINARY(32) 类型。例如 `"abc"`。
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
<br/>1. 将使用如下规则来生成子表名：首先将 measurement 的名称和标签的 key 和 value 组合成为如下的字符串

```json
"measurement,tag_key1=tag_value1,tag_key2=tag_value2"
```

需要注意的是，这里的 tag_key1, tag_key2 并不是用户输入的标签的原始顺序，而是使用了标签名称按照字符串升序排列后的结果。所以，tag_key1 并不是在行协议中输入的第一个标签。
排列完成以后计算该字符串的 MD5 散列值 "md5_val"。然后将计算的结果与字符串组合生成表名：“t_md5_val”。其中的 “t_” 是固定的前缀，每个通过该映射关系自动生成的表都具有该前缀。
<br/>2. 如果解析行协议获得的超级表不存在，则会创建这个超级表。
<br/>3. 如果解析行协议获得子表不存在，则 Schemaless 会按照步骤 1 或 2 确定的子表名来创建子表。
<br/>4. 如果数据行中指定的标签列或普通列不存在，则在超级表中增加对应的标签列或普通列（只增不减）。
<br/>5. 如果超级表中存在一些标签列或普通列未在一个数据行中被指定取值，那么这些列的值在这一行中会被置为 NULL。
<br/>6. 对 BINARY 或 NCHAR 列，如果数据行中所提供值的长度超出了列类型的限制，自动增加该列允许存储的字符长度上限（只增不减），以保证数据的完整保存。
<br/>7. 如果指定的数据子表已经存在，而且本次指定的标签列取值跟已保存的值不一样，那么最新的数据行中的值会覆盖旧的标签列取值。
<br/>8. 整个处理过程中遇到的错误会中断写入过程，并返回错误代码。

**备注：**
<br/>无模式所有的处理逻辑，仍会遵循 TDengine 对数据结构的底层限制，例如每行数据的总长度不能超过 48K 字节（2.1.7.0 之前的版本为 16K）。这方面的具体限制约束请参见 [TAOS SQL 边界限制](https://www.taosdata.com/cn/documentation/taos-sql#limitation) 章节。

**时间分辨率识别**
<br/>无模式写入过程中支持三个指定的模式，具体如下

| **序号** | **值**        | **说明** |
| ---- | ------------------- | ------------ |
| 1    | SML_LINE_PROTOCOL           |    InfluxDB行协议（Line Protocol)   |
| 2    | SML_TELNET_PROTOCOL              |    OpenTSDB 文本行协议   |
| 3    | SML_JSON_PROTOCOL              |    JSON 协议格式   |

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

**数据模式映射规则**
<br/>本节将说明行协议的数据如何映射成为具有模式的数据。每个行协议中数据  measurement 映射为 超级表名称。tag_set 中的 标签名称为 数据模式中的标签名，field_set 中的名称为列名称。以如下数据为例，说明映射规则：

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4f64 1626006833639000000
```

该行数据映射生成一个超级表： st， 其包含了 3 个类型为 nchar 的标签，分别是：t1, t2, t3。五个数据列，分别是ts（timestamp），c1 (bigint），c3(binary)，c2 (bool),  c4 (bigint）。映射成为如下 SQL 语句：

```json
create stable st (_ts timestamp, c1 bigint, c2 bool, c3 binary(6), c4 bigint) tags(t1 nchar(1), t2 nchar(1), t3 nchar(2))
```

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

第二行数据相对于第一行来说增加了一个列 c6，类型为 binary(6)。那么此时会自动增加一个列 c6， 类型为 binary(6)。

**写入完整性**
<br/>TDengine 提供数据写入的幂等性保证，即您可以反复调用 API 进行出错数据的写入操作。但是不提供多行数据写入的原子性保证。即在多行数据一批次写入过程中，会出现部分数据写入成功，部分数据写入失败的情况。

**错误码**
<br/>如果是无模式写入过程中的数据本身错误，应用会得到 TSDB_CODE_TSC_LINE_SYNTAX_ERROR 错误信息，该错误信息表明错误发生在写入文本中。其他的错误码与原系统一致，可以通过 `taos_errstr()` 获取具体的错误原因。

<br/>除使用 C 版本的 API，也可以使用官网连接器，包括 Java/Go/Python/C#/Node.js/Rust 等。此外，在 TDengine v2.4 及后续版本中，您还可以通过 taosAdapter 采用 RESTful 的方式直接写入无模式数据。

## <a class="anchor" id="prometheus"></a>Prometheus 直接写入（通过 taosAdapter）

remote_read 和 remote_write 是 Prometheus 数据读写分离的集群方案。
只需要将 remote_read 和 remote_write url 指向 taosAdapter 对应的 url 同时设置 Basic 验证即可使用。

* remote_read url :  `http://host_to_taosAdapter:port(default 6041)/prometheus/v1/remote_read/:db`
* remote_write url :  `http://host_to_taosAdapter:port(default 6041)/prometheus/v1/remote_write/:db`

Basic验证：

* username： TDengine 连接用户名
* password： TDengine 连接密码

示例 prometheus.yml  如下：

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

## <a class="anchor" id="telegraf"></a> Telegraf 直接写入(通过 taosAdapter)

安装 Telegraf 请参考[官方文档](https://portal.influxdata.com/downloads/)。

TDengine 新版本（2.3.0.0+）包含一个 taosAdapter 独立程序，负责接收包括 Telegraf 的多种应用的数据写入。

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

taosAdapter 相关配置参数请参考 taosadapter --help 命令输出以及相关文档。

## <a class="anchor" id="collectd"></a> collectd 直接写入(通过 taosAdapter)

安装 collectd，请参考[官方文档](https://collectd.org/download.shtml)。

TDengine 新版本（2.3.0.0+）包含一个 taosAdapter 独立程序，负责接收包括 collectd 的多种应用的数据写入。

在 /etc/collectd/collectd.conf 文件中增加如下内容，其中 host 和 port 请填写 TDengine 和 taosAdapter 配置的实际值：

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

taosAdapter 相关配置参数请参考 taosadapter --help 命令输出以及相关文档。

## <a class="anchor" id="statsd"></a> StatsD 直接写入(通过 taosAdapter)

安装 StatsD
请参考[官方文档](https://github.com/statsd/statsd)。

TDengine 新版本（2.3.0.0+）包含一个 taosAdapter 独立程序，负责接收包括 StatsD 的多种应用的数据写入。

在 config.js 文件中增加如下内容后启动 StatsD，其中 host 和 port 请填写 TDengine 和 taosAdapter 配置的实际值：

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

taosAdapter 相关配置参数请参考 taosadapter --help 命令输出以及相关文档。

icinga2 可以收集监控和性能数据并写入 OpenTSDB，taosAdapter 可以支持接收 icinga2 的数据并写入到 TDengine 中。

## <a class="anchor" id="icinga2"></a> icinga2 直接写入(通过 taosAdapter)

* 参考链接 `https://icinga.com/docs/icinga-2/latest/doc/14-features/#opentsdb-writer` 使能 opentsdb-writer
* 使能 taosAdapter 配置项 opentsdb_telnet.enable
* 修改配置文件 /etc/icinga2/features-enabled/opentsdb.conf

```
object OpenTsdbWriter "opentsdb" {
  host = "host to taosAdapter"
  port = 6048
}
```

taosAdapter 相关配置参数请参考 taosadapter --help 命令输出以及相关文档。

## <a class="anchor" id="tcollector"></a> TCollector 直接写入(通过 taosAdapter)

TCollector 是一个在客户侧收集本地收集器并发送数据到 OpenTSDB 的进程，taosAdapter 可以支持接收 TCollector 的数据并写入到 TDengine 中。

使能 taosAdapter 配置项 opentsdb_telnet.enable
修改 TCollector 配置文件，修改 OpenTSDB 宿主机地址为 taosAdapter 被部署的地址，并修改端口号为 taosAdapter 使用的端口（默认6049）。

taosAdapter 相关配置参数请参考 taosadapter --help 命令输出以及相关文档。

## <a class="anchor" id="emq"></a>EMQX Broker 直接写入

MQTT 是流行的物联网数据传输协议，[EMQX](https://github.com/emqx/emqx) 是一开源的 MQTT Broker 软件，无需任何代码，只需要在 EMQX Dashboard 里使用“规则”做简单配置，即可将 MQTT 的数据直接写入 TDengine。EMQX 支持通过 发送到 Web 服务的方式保存数据到 TDengine，也在企业版上提供原生的 TDengine 驱动实现直接保存。详细使用方法请参考 [EMQX 官方文档](https://docs.emqx.com/zh/enterprise/v4.4/rule/backend_tdengine.html#%E4%BF%9D%E5%AD%98%E6%95%B0%E6%8D%AE%E5%88%B0-tdengine)。

## <a class="anchor" id="hivemq"></a>HiveMQ Broker 直接写入

[HiveMQ](https://www.hivemq.com/) 是一个提供免费个人版和企业版的 MQTT 代理，主要用于企业和新兴的机器到机器M2M通讯和内部传输，满足可伸缩性、易管理和安全特性。HiveMQ 提供了开源的插件开发包。可以通过 HiveMQ extension - TDengine 保存数据到 TDengine。详细使用方法请参考 [HiveMQ extension - TDengine 说明文档](https://github.com/huskar-t/hivemq-tdengine-extension/blob/b62a26ecc164a310104df57691691b237e091c89/README.md)。
