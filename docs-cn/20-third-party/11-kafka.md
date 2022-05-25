---
sidebar_label: Kafka
title: TDengine Kafka Connector 使用教程
---

TDengine Kafka Connector 包含两个插件: TDengine Source Connector 和 TDengine Sink Connector。用户只需提供简单的配置文件，就可以将 Kafka 中指定 topic 的数据（批量或实时）同步到 TDengine， 或将 TDengine 中指定数据库的数据（批量或实时）同步到 Kafka。

## 什么是 Kafka Connect？

Kafka Connect 是 Apache Kafka 的一个组件，用于使其它系统，比如数据库、云服务、文件系统等能方便地连接到 Kafka。数据既可以通过 Kafka Connect 从其它系统流向 Kafka, 也可以通过 Kafka Connect 从 Kafka 流向其它系统。从其它系统读数据的插件称为 Source Connector, 写数据到其它系统的插件称为 Sink Connector。Source Connector 和 Sink Connector 都不会直接连接 Kafka Broker，Source Connector 把数据转交给 Kafka Connect。Sink Connector 从 Kafka Connect 接收数据。

![](kafka/Kafka_Connect.webp)

TDengine Source Connector 用于把数据实时地从 TDengine 读出来发送给 Kafka Connect。TDengine Sink Connector 用于 从 Kafka Connect 接收数据并写入 TDengine。

![](kafka/streaming-integration-with-kafka-connect.webp)

## 什么是 Confluent？

Confluent 在 Kafka 的基础上增加很多扩展功能。包括：

1. Schema Registry
2. REST 代理
3. 非 Java 客户端
4. 很多打包好的 Kafka Connect 插件
5. 管理和监控 Kafka 的 GUI —— Confluent 控制中心

这些扩展功能有的包含在社区版本的 Confluent 中，有的只有企业版能用。
![](kafka/confluentPlatform.webp)

Confluent 企业版提供了 `confluent` 命令行工具管理各个组件。

## 前置条件

运行本教程中示例的前提条件。

1. Linux 操作系统
2. 已安装 Java 8 和 Maven
3. 已安装 Git
4. 已安装并启动 TDengine。如果还没有可参考[安装和卸载](/operation/pkg-install)

## 安装 Confluent

Confluent 提供了 Docker 和二进制包两种安装方式。本文仅介绍二进制包方式安装。

在任意目录下执行：

```
curl -O http://packages.confluent.io/archive/7.1/confluent-7.1.1.tar.gz
tar xzf confluent-7.1.1.tar.gz -C /opt/test
```

然后需要把 `$CONFLUENT_HOME/bin` 目录加入 PATH。

```title=".profile"
export CONFLUENT_HOME=/opt/confluent-7.1.1
PATH=$CONFLUENT_HOME/bin
export PATH
```

以上脚本可以追加到当前用户的 profile 文件（~/.profile 或 ~/.bash_profile）

安装完成之后，可以输入`confluent version`做简单验证：

```
# confluent version
confluent - Confluent CLI

Version:     v2.6.1
Git Ref:     6d920590
Build Date:  2022-02-18T06:14:21Z
Go Version:  go1.17.6 (linux/amd64)
Development: false
```

## 安装 TDengine Connector 插件

### 从源码安装

```
git clone https://github.com:taosdata/kafka-connect-tdengine.git
cd kafka-connect-tdengine
mvn clean package
unzip -d $CONFLUENT_HOME/share/confluent-hub-components/ target/components/packages/taosdata-kafka-connect-tdengine-0.1.0.zip
```

以上脚本先 clone 项目源码，然后用 Maven 编译打包。打包完成后在 `target/components/packages/` 目录生成了插件的 zip 包。把这个 zip 包解压到安装插件的路径即可。安装插件的路径在配置文件 `$CONFLUENT_HOME/etc/kafka/connect-standalone.properties` 中。默认的路径为 `$CONFLUENT_HOME/share/confluent-hub-components/`。

### 用 confluent-hub 安装

[Confluent Hub](https://www.confluent.io/hub) 提供下载 Kafka Connect 插件的服务。在 TDengine Kafka Connector 发布到 Confluent Hub 后可以使用命令工具 `confluent-hub` 安装。
**TDengine Kafka Connector 目前没有正式发布，不能用这种方式安装**。

## 启动 Confluent

```
confluent local services start
```

:::note
一定要先安装插件再启动 Confluent, 否则会出现找不到类的错误。Kafka Connect 的日志（默认路径: /tmp/confluent.xxxx/connect/logs/connect.log）中会输出成功安装的插件，据此可判断插件是否安装成功。
:::

:::tip
若某组件启动失败，可尝试清空数据，重新启动。数据目录在启动时将被打印到控制台，比如 ：

```title="控制台输出日志" {1}
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

清空数据可执行 `rm -rf /tmp/confluent.106668`。
:::

## TDengine Sink Connector 的使用

TDengine Sink Connector 的作用是同步指定 topic 的数据到 TDengine。用户无需提前创建数据库和超级表。可手动指定目标数据库的名字（见配置参数 connection.database）， 也可按一定规则生成(见配置参数 connection.database.prefix)。

TDengine Sink Connector 内部使用 TDengine [无模式写入接口](/reference/connector/cpp#无模式写入-api)写数据到 TDengine，目前支持三种格式的数据：[InfluxDB 行协议格式](/develop/insert-data/influxdb-line)、 [OpenTSDB Telnet 协议格式](/develop/insert-data/opentsdb-telnet) 和 [OpenTSDB JSON 协议格式](/develop/insert-data/opentsdb-json)。

下面的示例将主题 meters 的数据，同步到目标数据库 power。数据格式为 InfluxDB Line 协议格式。

### 添加配置文件

```
mkdir ~/test
cd ~/test
vi sink-demo.properties
```

sink-demo.properties 内容如下：

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

关键配置说明：

1. `topics=meters` 和 `connection.database=power`, 表示订阅主题 meters 的数据，并写入数据库 power。
2. `db.schemaless=line`, 表示使用 InfluxDB Line 协议格式的数据。

### 创建 Connector 实例

```
confluent local services connect connector load TDengineSinkConnector --config ./sink-demo.properties
```

若以上命令执行成功，则有如下输出：

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

### 写入测试数据

准备测试数据的文本文件，内容如下：

```txt title="test-data.txt"
meters,location=California.LosAngeles,groupid=2 current=11.8,voltage=221,phase=0.28 1648432611249000000
meters,location=California.LosAngeles,groupid=2 current=13.4,voltage=223,phase=0.29 1648432611250000000
meters,location=California.LosAngeles,groupid=3 current=10.8,voltage=223,phase=0.29 1648432611249000000
meters,location=California.LosAngeles,groupid=3 current=11.3,voltage=221,phase=0.35 1648432611250000000
```

使用 kafka-console-producer 向主题 meters 添加测试数据。

```
cat test-data.txt | kafka-console-producer --broker-list localhost:9092 --topic meters
```

:::note
如果目标数据库 power 不存在，那么 TDengine Sink Connector 会自动创建数据库。自动创建数据库使用的时间精度为纳秒，这就要求写入数据的时间戳精度也是纳秒。如果写入数据的时间戳精度不是纳秒，将会抛异常。
:::

### 验证同步是否成功

使用 TDengine CLI 验证同步是否成功。

```
taos> use power;
Database changed.

taos> select * from meters;
              ts               |          current          |          voltage          |           phase           | groupid |            location            |
===============================================================================================================================================================
 2022-03-28 09:56:51.249000000 |              11.800000000 |             221.000000000 |               0.280000000 | 2       | California.LosAngeles                |
 2022-03-28 09:56:51.250000000 |              13.400000000 |             223.000000000 |               0.290000000 | 2       | California.LosAngeles                |
 2022-03-28 09:56:51.249000000 |              10.800000000 |             223.000000000 |               0.290000000 | 3       | California.LosAngeles                |
 2022-03-28 09:56:51.250000000 |              11.300000000 |             221.000000000 |               0.350000000 | 3       | California.LosAngeles                |
Query OK, 4 row(s) in set (0.004208s)
```

若看到了以上数据，则说明同步成功。若没有，请检查 Kafka Connect 的日志。配置参数的详细说明见[配置参考](#配置参考)。

## TDengine Source Connector 的使用

TDengine Source Connector 的作用是将 TDengine 某个数据库某一时刻之后的数据全部推送到 Kafka。TDengine Source Connector 的实现原理是，先分批拉取历史数据，再用定时查询的策略同步增量数据。同时会监控表的变化，可以自动同步新增的表。如果重启 Kafka Connect, 会从上次中断的位置继续同步。

TDengine Source Connector 会将 TDengine 数据表中的数据转换成 [InfluxDB Line 协议格式](/develop/insert-data/influxdb-line/) 或 [OpenTSDB JSON 协议格式](/develop/insert-data/opentsdb-json)， 然后写入 Kafka。

下面的示例程序同步数据库 test 中的数据到主题 tdengine-source-test。

### 添加配置文件

```
vi source-demo.properties
```

输入以下内容：

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

### 准备测试数据

准备生成测试数据的 SQL 文件。

```sql title="prepare-source-data.sql"
DROP DATABASE IF EXISTS test;
CREATE DATABASE test;
USE test;
CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT);
INSERT INTO d1001 USING meters TAGS(California.SanFrancisco, 2) VALUES('2018-10-03 14:38:05.000',10.30000,219,0.31000) d1001 USING meters TAGS(California.SanFrancisco, 2) VALUES('2018-10-03 14:38:15.000',12.60000,218,0.33000) d1001 USING meters TAGS(California.SanFrancisco, 2) VALUES('2018-10-03 14:38:16.800',12.30000,221,0.31000) d1002 USING meters TAGS(California.SanFrancisco, 3) VALUES('2018-10-03 14:38:16.650',10.30000,218,0.25000) d1003 USING meters TAGS(California.LosAngeles, 2) VALUES('2018-10-03 14:38:05.500',11.80000,221,0.28000) d1003 USING meters TAGS(California.LosAngeles, 2) VALUES('2018-10-03 14:38:16.600',13.40000,223,0.29000) d1004 USING meters TAGS(California.LosAngeles, 3) VALUES('2018-10-03 14:38:05.000',10.80000,223,0.29000) d1004 USING meters TAGS(California.LosAngeles, 3) VALUES('2018-10-03 14:38:06.500',11.50000,221,0.35000);
```

使用 TDengine CLI, 执行 SQL 文件。

```
taos -f prepare-source-data.sql
```

### 创建 Connector 实例

```
confluent local services connect connector load TDengineSourceConnector --config source-demo.properties
```

### 查看 topic 数据

使用 kafka-console-consumer 命令行工具监控主题 tdengine-source-test 中的数据。一开始会输出所有历史数据， 往 TDengine 插入两条新的数据之后，kafka-console-consumer 也立即输出了新增的两条数据。

```
kafka-console-consumer --bootstrap-server localhost:9092 --from-beginning --topic tdengine-source-test
```

输出：

```
......
meters,location="California.SanFrancisco",groupid=2i32 current=10.3f32,voltage=219i32,phase=0.31f32 1538548685000000000
meters,location="California.SanFrancisco",groupid=2i32 current=12.6f32,voltage=218i32,phase=0.33f32 1538548695000000000
......
```

此时会显示所有历史数据。切换到 TDengine CLI， 插入两条新的数据：

```
USE test;
INSERT INTO d1001 VALUES (now, 13.3, 229, 0.38);
INSERT INTO d1002 VALUES (now, 16.3, 233, 0.22);
```

再切换回 kafka-console-consumer， 此时命令行窗口已经打印出刚插入的 2 条数据。

### unload 插件

测试完毕之后，用 unload 命令停止已加载的 connector。

查看当前活跃的 connector：

```
confluent local services connect connector status
```

如果按照前述操作，此时应有两个活跃的 connector。使用下面的命令 unload：

```
confluent local services connect connector unload TDengineSourceConnector
confluent local services connect connector unload TDengineSourceConnector
```

## 配置参考

### 通用配置

以下配置项对 TDengine Sink Connector 和 TDengine Source Connector 均适用。

1. `name`: connector 名称。
2. `connector.class`: connector 的完整类名， 如: com.taosdata.kafka.connect.sink.TDengineSinkConnector。
3. `tasks.max`: 最大任务数, 默认 1。
4. `topics`: 需要同步的 topic 列表， 多个用逗号分隔, 如 `topic1,topic2`。
5. `connection.url`: TDengine JDBC 连接字符串， 如 `jdbc:TAOS://127.0.0.1:6030`。
6. `connection.user`： TDengine 用户名， 默认 root。
7. `connection.password` ：TDengine 用户密码， 默认 taosdata。
8. `connection.attempts` ：最大尝试连接次数。默认 3。
9. `connection.backoff.ms` ： 创建连接失败重试时间隔时间，单位为 ms。 默认 5000。

### TDengine Sink Connector 特有的配置

1. `connection.database`： 目标数据库名。如果指定的数据库不存在会则自动创建。自动建库使用的时间精度为纳秒。默认值为 null。为 null 时目标数据库命名规则参考 `connection.database.prefix` 参数的说明
2. `connection.database.prefix`： 当 connection.database 为 null 时, 目标数据库的前缀。可以包含占位符 '${topic}'。 比如 kafka_${topic}, 对于主题 'orders' 将写入数据库 'kafka_orders'。 默认 null。当为 null 时，目标数据库的名字和主题的名字是一致的。
3. `batch.size`: 分批写入每批记录数。当 Sink Connector 一次接收到的数据大于这个值时将分批写入。
4. `max.retries`: 发生错误时的最大重试次数。默认为 1。
5. `retry.backoff.ms`: 发送错误时重试的时间间隔。单位毫秒，默认 3000。
6. `db.schemaless`: 数据格式，必须指定为： line、json、telnet 中的一个。分别代表 InfluxDB 行协议格式、 OpenTSDB JSON 格式、 OpenTSDB Telnet 行协议格式。

### TDengine Source Connector 特有的配置

1. `connection.database`: 源数据库名称，无缺省值。
2. `topic.prefix`： 数据导入 kafka 后 topic 名称前缀。 使用 `topic.prefix` + `connection.database` 名称作为完整 topic 名。默认为空字符串 ""。
3. `timestamp.initial`: 数据同步起始时间。格式为'yyyy-MM-dd HH:mm:ss'。默认 "1970-01-01 00:00:00"。
4. `poll.interval.ms`: 拉取数据间隔，单位为 ms。默认 1000。
5. `fetch.max.rows` : 检索数据库时最大检索条数。 默认为 100。
6. `out.format`: 数据格式。取值 line 或 json。line 表示 InfluxDB Line 协议格式， json 表示 OpenTSDB JSON 格式。默认 line。

## 问题反馈

https://github.com/taosdata/kafka-connect-tdengine/issues

## 参考

1. https://www.confluent.io/what-is-apache-kafka
2. https://developer.confluent.io/learn-kafka/kafka-connect/intro
3. https://docs.confluent.io/platform/current/platform.html
