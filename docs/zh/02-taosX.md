---
sidebar_label: taosX
title: 数据接入、同步、备份、导出和迁移
description:  "为了能够方便地将各种数据源中的数据导入 TDengine 3.0，TDengine 3.0 企业版提供了一个全新的工具 taosX 用于帮助用户快速将其它数据源中的数据传输到 TDengine 中"
---

## 简介

为了能够方便地将各种数据源中的数据导入 TDengine 3.0，TDengine 3.0 企业版提供了一个全新的工具 taosX 用于帮助用户快速将其它数据源中的数据传输到 TDengine 中。 taosX 定义了自己的集成框架，方便扩展新的数据源。目前支持的数据源有 TDengine 自身（即从一个 TDengine 集群到另一个 TDengine 集群），Pi, OPC UA。除了数据接入外，taosX 还支持数据备份、数据同步、数据迁移以及数据导出功能。

**使用限制**：taosX 只能用于企业版数据库服务端。

## 安装与配置

有两种安装 taosX 的方式：

1. 使用 TDengine 安装包，在安装了 TDengine 企业版之后，您的系统中就已经拥有了 taosX，请使用 Linux 系统命令 which 来确认它存在于系统中。TDengine 企业版中自带的 taosX 可以进行从 TDengine 到 TDengine 的数据复制和同步，可以进行备份数据到本地文件和从本地文件恢复。
2. 使用独立的 taosX 安装包，其中除了 taosX 之外，还包含 Pi 连接器（限 Windows）， OPC 连接器， InfluxDB 连接器， MQTT 连接器，以及必要的 Agent 组件，taosX + Agent + 某个连接器可以用于将相应数据源的数据同步到 TDengine。

### Linux 安装

下载需要的 taosX 安装包，下文以安装包"taosX-0.5.1-Linux-x64.tar.gz"为例展示如何安装：

``` bash
# 解压文件
tar -zxf taosX-0.5.1-Linux-x64.tar.gz
cd taosX-0.5.1-Linux-x64

# 安装
sudo ./install.sh

# 验证
taosx -V 
# taosx 0.5.1-b9827b00-dirty (built linux-x86_64 2023-05-31 09:11:13 +08:00)

taosx-agent -V 
# taosx-agent 0.1.0-33c1e5e4 (built linux-x86_64 2023-05-26 14:24:13 +08:00)

# 卸载
sudo rmtaox

```

#### FAQ: 
1. 安装后都会有哪些文件被复制到了哪个安装目录？
    * 复制 bin/taosx 、bin/taosx-agent 到 /usr/local/taosX/bin
    * 复制 plugins/influxdb、plugins/mqtt、plugins/opc 等到 /usr/local/taosX/plugins
    * 复制 scripts/taosx.service、script/taosx-agent.service 到 /usr/local/taosX/script
    * 复制 install.sh、rmtaosX.sh 到 /usr/local/taosX 
    * 复制 config/agent.example.toml 到 /usr/local/taosX/config 和 /etc/taosX

2. taosX -V 提示 "Command not found" 应该如何解决？
    * 检验问题1，保证所有的文件都被复制到对应的目录
    * 创建软连接
    ``` bash
    ln -s /usr/local/taosX/bin/taosx /usr/bin/taosx
    ln -s /usr/local/taosX/bin/taosx-agent /usr/bin/taosx-agent
    ln -s /usr/local/taosX/rmtaosX.sh /usr/bin/rmtaosx
    ```

### Windows 安装

@xinsheng，请在此补充详细的 Linux 安装过程


## 运行模式

taosX 是进行数据同步与复制的核心组件，以下运行模式指 taosX 的运行模式，其它组件的运行模式在 taosX 的不同运行模式下与之适配。

### 命令行模式

可以直接在命令行上添加必要的参数直接启动 taosX 即为命令行模式运行。当命令行参数所指定的任务完成后 taosX 会自动停止。taosX 在运行中如果出现错误也会自动停止。也可以在任意时刻使用 ctrl+c 停止 taosX 的运行。本节介绍如何使用 taosX 的各种使用场景下的命令行。

#### 从 TDengine 到 TDengine 的数据同步

1. 3.0 -> 3.0
2. 2.4(2.6) -> 3.0

@chenyang，请在此补充详细的命令行参数，及示例（含 Linux 和 Windows），按如下结构 (下同)

1. 参数列表及其含义
2. Linux/Windows 示例
3. 常见错误排查

#### 从 TDengine 备份数据文件到本地

@chenyang

#### 从本地数据文件恢复到 TDengine

@chenyang

#### 从 OPC-UA 同步数据到 TDengine

@chenyang

#### 从 OPC-DA 同步数据到 TDengine (Windows)

@zhengqin

#### 从 Pi 同步数据到 TDengine (Windows)

@chenyang

### 从 InfluxDB 同步数据到 TDengine

将数据从 InfluxDB 同步至 TDengine 的命令，如下所示：

```bash
taosx run --from "<InfluxDB-DSN>" --to "<TDengine-DSN>"
```

其中，InfluxDB DSN 符合 DSN 的通用规则，这里仅对其特有的参数进行说明：
- orgId: 必填，InfluxDB 中的 Orgnization ID;
- bucket: 必填，InfluxDB 中的 Bucket 名称，一次只能同步一个 Bucket;
- token: 必填，InfluxDB 中生成的 API token, 这个 token 至少要拥有以上 Bucket 的 Read 权限；
- beginTime: 必填，格式为：YYYY-MM-DD HH:MM:SS, 时区采用 UTC 时区，例如：2023-06-01 00:00:00, 即北京时间2023-06-01 08:00:00;
- endTime: 非必填，可以不指定该字段或值为空，格式与beginTime相同；如果未指定，提交任务后，将持续进行数据同步。

#### 举例说明

将位于 192.168.1.10 的 InfluxDB 中, Bucket 名称为 test_bucket, 从UTC时间2023年06月01日00时00分00秒开始的数据，通过运行在 192.168.1.20 上的 taoskeeper, 同步至 TDengine 的 test_db 数据库中，完整的命令如下所示：

```bash
taosx run \
  --from "influxdb://192.168.1.10:8086/?token=OZ2sB6Ie6qcKcYAmcHnL-i3STfLVg_IRPQjPIzjsAQ4aUxCWzYhDesNape1tp8IsX9AH0ld41C-clTgo08CGYA==&orgId=3233855dc7e37d8d&bucket=test_bucket&beginTime=2023-06-01 00:00:00" \
  --to "taos+http://192.168.1.20:6041/test_db" \
  -vv
```

在这个命令中，未指定endTime, 所以任务会长期运行，持续同步最新的数据。

### 从 MQTT 同步数据到 TDengine

目前，MQTT 连接器仅支持从 MQTT 服务端消费 JSON 格式的消息，并将其同步至 TDengine. 命令如下所示：

```bash
taosx run --from "<MQTT-DSN>" --to "<TDengine-DSN>" --parser "@<parser-config-file-path>"
```

其中：
- `--from` 用于指定 MQTT 数据源的 DSN
- `--to` 用于指定 TDengine 的 DSN
- `--parser` 用于指定一个 JSON 格式的配置文件，该文件决定了如何解析 JSON 格式的 MQTT 消息，以及写入 TDengine 时的超级表名、子表名、字段名称和类型，以及标签名称和类型等。

#### MQTT DSN 配置

MQTT DSN 符合 DSN 的通用规则，这里仅对其特有的参数进行说明：
- topics: 必填，用于配置监听的 MQTT 主题名称和连接器支持的最大 QoS, 采用 `<topic>::<max-Qos>` 的形式；支持配置多个主题，使用逗号分隔；配置主题时，还可以使用 MQTT 协议的支持的通配符#和+;
- version: 非必填，用于配置 MQTT 协议的版本，支持的版本包括：3.1/3.1.1/5.0, 默认值为3.1;
- clean_session: 非必填，用于配置连接器作为 MQTT 客户端连接至 MQTT 服务端时，服务端是否保存该会话信息，其默认值为 true, 即不保存会话信息；
- client_id: 必填，用于配置连接器作为 MQTT 客户端连接至 MQTT 服务端时的客户端 id;
- keep_alive: 非必填，用于配置连接器作为 MQTT 客户端，向 MQTT 服务端发出 PINGREG 消息后的等待时间，如果连接器在该时间内，未收到来自 MQTT 服务端的 PINGREQ, 连接器则主动断开连接；该配置的单位为秒，默认值为 60;
- ca: 非必填，用于指定连接器与 MQTT 服务端建立 SSL/TLS 连接时，使用的 CA 证书，其值为在证书文件的绝对路径前添加@, 例如：@/home/admin/certs/ca.crt;
- cert: 非必填，用于指定连接器与 MQTT 服务端建立 SSL/TLS 连接时，使用的客户端证书，其值为在证书文件的绝对路径前添加@, 例如：@/home/admin/certs/client.crt;
- cert_key: 非必填，用于指定连接器与 MQTT 服务端建立 SSL/TLS 连接时，使用的客户端私钥，其值为在私钥文件的绝对路径前添加@, 例如：@/home/admin/certs/client.key;
- log_level: 非必填，用于配置连接器的日志级别，连接器支持 error/warn/info/debug/trace 5种日志级别，默认值为 info.

一个完整的 MQTT DSN 示例如下：
```bash
mqtt://<username>:<password>@<mqtt-broker-ip>:8883?topics=testtopic/1::2&version=3.1&clean_session=true&log_level=info&client_id=taosdata_1234&keep_alive=60&ca=@/home/admin/certs/ca.crt&cert=@/home/admin/certs/client.crt&cert_key=@/home/admin/certs/client.key
```

#### MQTT 连接器的解释器配置

连接器的解释器配置文件，即`--parser`配置项的参数，它的值为一个 JSON 文件，其配置可分为`parse`和`model`两部分，模板如下所示：

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

各字段的说明如下：
- parse 部分目前仅支持 json 一种 payload, json 字段的值是一个由 JSON Object 构成的 JSON Array:
  - 每个 JSON Ojbect 包括 name, alias, cast 三个字段；
  - name 字段用于指定如何从 MQTT 消息中提取字段，如果 MQTT 消息是一个简单的 JSON Object, 这里可以直接设置其字段名；如果 MQTT 消息是一个复杂的 JSON Object, 这里可以使用 JSON Path 提取字段，例如：`$.data.city`;
  - alias 字段用于命名 MQTT 消息中的字段同步至 TDengine 后使用的名称；
  - cast 字段用于指定 MQTT 消息中的字段同步至 TDengine 后使用的类型。
- model 部分用于设置 TDengine 超级表、子表、列和标签等信息：
  - using 字段用于指定超级表名称；
  - name 字段用于指定子表名称，它的值可以分为前缀和变量两部分，变量为 parse 部分设置的 alias 的值，需要使用{}, 例如：d{id}；
  - columns 字段用于设置 MQTT 消息中的哪些字段作为 TDengine 超级表中的列，取值为 parse 部分设置的 alias 的值；需要注意的是，这里的顺序会决定 TDengine 超级表中列的顺序，因此第一列必须为 TIMESTAMP 类型；
  - tags 字段用于设置 MQTT 消息中的哪些字段作为 TDengine 超级表中的标签，取值为 parse 部分设置的 alias 的值。

#### 举例说明

在 192.168.1.10 的 1883 端口运行着一个 MQTT broker, 用户名、口令分别为admin, 123456; 现欲将其中的消息，通过运行在 192.168.1.20 的 taosadapter 同步至 TDengine 的 test 数据库中。MQTT 消息格式为：

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

MQTT 消息同步至 TDengine 时, 如果采用 meters 作为超级表名，前缀“d”拼接id字段的值作为子表名，ts, id, current, voltage, phase作为超级表的列，groupid, location作为超级表的标签，其解释器的配置如下：
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

如果以上parser配置位于`/home/admin/parser.json`中，那么完整的命令如下所示：

```bash
taosx run \
  -f "mqtt://admin:123456@192.168.1.10:1883?topics=testtopic/1::2&version=3.1&clean_session=true&log_level=info&client_id=1234&keep_alive=60" \
  -t "taos+ws://192.168.1.20:6041/test"
  --parser "@/home/admin/parser.json"
  --verbose
```

### 服务模式

在服务模式下， taosX，Agent 以及 taosExplorer 均已服务态运行，各种操作通过 taosExplorer 的图形界面进行。

### 部署 taosX

#### 配置

taosX 仅支持通过命令行参数进行配置。服务模式下，taosX 支持的命令行参数可以通过以下方式查看：

```
taosx serve --help
```

建议通过 Systemd 的方式，启动 taosX 的服务模式，其 Systemd 的配置文件位于：`/etc/systemd/system/taosx.service`. 如需修改 taosX 的启动参数，可以编辑该文件中的以下行：

```
ExecStart=/usr/bin/taosx serve -v
```

修改后，需执行以下命令重启 taosX 服务，使配置生效：

```
systemctl daemon-reload
systemctl restart taosx
```

#### 启动

以 Systemd 的方式启动 taosX 的命令如下：

```
systemctl start taosx
```

#### 问题排查

1. 如何修改 taosX 的日志级别？

taosX 的日志级别是通过命令行参数指定的，默认的日志级别为 Info, 具体参数如下：
- INFO: `taosx serve -v`
- DEBUG: `taosx serve -vv`
- TRACE: `taosx serve -vvv`

Systemd 方式启动时，如何修改命令行参数，请参考“配置”章节。

2. 如何查看 taosX 的日志？

以 Systemd 方式启动时，可通过 journalctl 命令查看日志。以滚动方式，实时查看最新日志的命令如下：

```
journalctl -u taosx -f
```

### 部署 Agent 

#### 配置

Agent 默认的配置文件位于`/etc/taos/agent.toml`, 包含以下配置项：
- endpoint: 必填，taosX 的 GRPC endpoint
- token: 必填，在 taosExplorer 上创建 agent 时，产生的token
- debug_level: 非必填，默认为 info, 还支持 debug, trace 等级别

如下所示：

```
endpoint = "grpc://<taosx-ip>:6055"
token = "<token>"
```

#### 启动

Agent 可以通过 Systemd 命令启动：

```
systemctl start taosx-agent
```

#### 问题排查

可以通过 journalctl 查看 Agent 的日志

```
journalctl -u taosx-agent -f
```


### 部署 taosExplorer

请参考 taosExplorer

### 数据同步功能

请参考 taosExplorer





1.  服务模式

使用 systemctl 命令来启动、停止和检测 taosX 服务。

systemctl start taosx

systemctl stop taosx

systemctl restart taosx

systemctl status taosx

## DSN（Data Source Name)

Taosx 使用 DSN 来表示一个数据源（来源或目的源），典型的 DSN 如下：

```bash
# url-like
<driver>[+<protocol>]://[[<username>:<password>@]<host>:<port>][/<object>][?<p1>=<v1>[&<p2>=<v2>]]
|------|------------|---|-----------|-----------|------|------|----------|-----------------------|
|driver|   protocol |   | username  | password  | host | port |  object  |  params               |

# or path-like
<driver>[+protocol]:<path>[?<p1>=<v1>]
|------|-----------|------|----------|
|driver|  protocol | path | params   |

[] 中的数据都为可选参数。
// url 示例
tmq+ws://root:taosdata@localhost:6030/db1?timeout=never
驱动（driver）分别有 taos，tmq，local，csv，parquet 几个选项。
taos：使用连接 TDengine 的数据源
tmq：启用数据订阅从 TDengine 中获取数据
local：数据备份或恢复
csv：读取或写入 csv 文件
parquet：读取或写入 parquet 文件

localhost:6030 表示数据源的地址和端口，db1 表示具体的数据库，root 和 taosdata 表示该数据源的用户名和密码，问号后则是这个 dsn 的参数。不同的驱动（driver）拥有不同的参数。
+ws 表示使用 rest 获取数据，不使用 +ws 则表示使用原生连接获取数据，此时需要 taosx 所在的服务器安装 taosc。

// path 示例
csv:./meters.csv
csv 表示输出为 csv 文件，./meters.csv 表示文件的地址信息
```

## 数据接入

数据接入是从各种非 TDengine 的数据源将数据接入 TDengine。目前支持的数据源有 PI 和 OPC UA。

### PI

1.  PI 数据源的 DSN 定义如下：

```
pi://<PIServerName>/<AFDatabaseName>?[PISystemName=<PISystemName>&][MaxWaitLen=<MaxWaitLen>&][UpdateInterval=<UpdateInterval>&][TemplateForPIPoint=<TemplateForPIPoint>&][...]
```

2.  命令行示例如下：

```shell
taosx run \
    -f "pi://WIN-2OA23UM12TN/Met1?TemplateForPIPoint=template1,template2&TemplateForAFElement=template3,template4" \
    -t "taos://tdengine:6030/pi"
```

**以上示例的PI参数表示**
- PIServerName：PI 连接配置主机名 ，此示例中为 WIN-2OA23UM12TN
- AFDatabaseName：指定连接的 PI 数据库，此示例中为 Met1
- TemplateForPIPoint：使用 PI Point 模式将模板 template1 ，template2 ，按照 element 的每个Arrtribution作为子表导入到 TDengine 服务器 tdengine 的 pi 库中
- TemplateForAFElement：使用 AF Point 模式将模板template3 ，template4 ，按照 element 的 Attribution 集合作为一个子表导入到 TDengine 服务器 tdengine的 pi 库中

**在 taosX CLI 运行时支持的参数如下**

-   PISystemName：连接配置 PI 系统服务名，默认值与 PIServerName 一致
-   MaxWaitLen：数据最大缓冲条数，默认值为1000
-   UpdateInterval：PI System 取数据频率，默认值为10000（毫秒：ms）
   
**具体使用步骤**
- 命令行模式：在能够连接 PI 数据源的 Windows 服务器上以命令行模式启动 taosX，根据上面的 DSN 和命令行参数来启动数据传输
- Explorer 模式：在能够连接 Pi 数据源的 Windows 服务器上以服务模式 taosX，并将 taosX 的地址配置在 taosExplorer 的配置文件中，启动 taosExplroer服务，通过控制台添加 Pi 数据源和创建传输任务

### OPC

OPC 数据接入目前只支持 taosX 和 OPC UA/DA 连接器都运行在 Windows 平台。要想获取和使用，请联系 TDengine 商务团队。

1.  OPC 数据源的 DSN 定义如下

```
opc+<protocol={ua|da}>://[<user>:<password>@]host:port?ua.nodes=<id::value,...>[&da.tags=..][<param>=<value>]
```

2.  DSN 示例如下

```
opc+ua://uauser:uapass@localhost:4840?ua.nodes=ns=2;i=2::meters::current::double
opc+da://Matrikon.OPC.Simulation.1?nodes=localhost&da.tags=Random.Real8::tb3::c1::int
```

**以上示例的 OPC 参数表示：**

-   protocol: 使用 OPCUA 连接，endpoint 为 opc.tcp://localhost:5880
-   auth_method: 当存在用户名和密码时使用 UserName 方式验证，此示例中用户名和密码为 uauser uapass。
-   ua.nodes: 将 OPCUA 的 ns=2;i=2 点数据以 double 类型传输到 taosx。
   
**在 taosX CLI 运行时支持的参数如下**
-   ua.nodes: OPCUA 的采集点列表，可以使用 @file.csv 的形式输入一个文件，格式为按行分隔的采集点列表，file.csv 应当包含 headers 行： id,table,field,type 共计四列，用于指定将某个点 id 采集并写入到数据表 table 的 field 列，列的类型为 type，是 TDengine SQL 的数据类型文本（如 int,double 等）。也可以用 {id}::{table}::{field}::{value} 的格式，以 , 逗号分隔直接写在参数里，如 ns=2,i=2::meters::current::double。以点位采集的数据，以这种方式显示声明其初始化 transfomer：{ point: [{ id: "ns=2;i=2", table: "meters", field: "current", value: "double" }]} 。
-   da.tags: 类似于 ua.nodes, 在 OPCDA 时指定数据采集点。
-   interval: 采集间隔
-   concurrent：采集器并发数
-   batch_size：采集器上报的批次点位数
-   batch_timeout: 采集器上报的超时时间
   
**在 OPCUA 下，可设置如下参数：**

| connect_timeout | int    | timeout for connect to endpoint in second                                   |
|-----------------|--------|-----------------------------------------------------------------------------|
| request_timeout | int    | timeout for a request in second                                             |
| security_policy | string | None/Basic128Rsa15/Basic256/Basic256Sha256                                  |
| security_mode   | string | None/Sign/SignAndEncrypt                                                    |
| certificate     | string | Path to cert.pem. Required when security mode or policy isn't "None"        |
| private_key     | string | Path to private key.pem. Required when security mode or policy isn't "None" |

   
**在 OPCDA 下，可设置**
-   da.nodes: OPCDA 的节点列表，使用 , 逗号分隔，如 host1,host2。
   
**OPC DA 的使用方法**
- 在能够连接 OPC DA 数据源的 Windows 服务器上以命令行模式启动 taosX，根据上面的 DSN 和命令行参数来启动数据传输
- 在能够连接 OPC DA数据源的 Windows 服务器上以服务模式启动 taosX，并将 taosX 的地址配置在 taosExplorer 的配置文件中，启动 taosExplroer服务，通过控制台添加 OPC DA数据源和创建传输任务

**OPC UA 的使用方法**
- 命令行模式：在能够连接 OPC UA 数据源的 Windows 或 Linux 服务器上以命令行模式启动 taosX，根据上面的 DSN 和命令行参数来启动数据传输
- Explorer 模式：在能够连接 OPC UA数据源的 Windows 或 Linux 服务器上以服务模式启动 taosX，并将 taosX 的地址配置在 taosExplorer 的配置文件中，启动 taosExplroer服务，通过控制台添加 OPC UA数据源和创建传输任务

## 数据同步

数据同步是在两个相同版本 （都是 3.0.x.y）的 TDengine 集群之间将源集群中的存量及增量数据同步到目标集群中。

driver 为 tmq 参数说明：

| 参数名称  | 说明                                                             | 默认值                     |
|-----------|------------------------------------------------------------------|----------------------------|
| group.id  | 订阅使用的分组ID                                                 | 若为空则使用 hash 生成一个 |
| client.id | 订阅使用的客户端ID                                               | taosx                      |
| timeout   | 监听数据的超时时间，当设置为 never 表示 taosx 不会停止持续监听。 | 500ms                      |


**工具模式**

```shell
taosx run -f 'tmq://root:taosdata@localhost:6030/db1?timeout=never' -t 'taos://root:taosdata@another.com:6030/db2'
```

**服务模式**

```shell
curl --location 'localhost:6050/tasks' \
--header 'Content-Type: application/json' \
--data '{
    "from": "tmq+ws://root:taosdata@localhost:6041/db1?timeout=never",
    "to": "taos+ws://root:taosdata@another.com:6041/db2"
}'
```

## 数据备份和恢复

数据备份是从当前所连接的 TDengine 集群中定时或按需触发数据备份，并能够从备份中恢复。

**工具模式**

```shell
备份：taosx run -f 'tmq://this/db1' -t 'local:/path/to/backup/directory'
恢复：taosx run -f 'local:/path/to/backups/of/one' -t 'taos://root:taosdata@another.com:6030/db1'
```

**服务模式**

```shell
创建备份任务：
curl --location 'localhost:6050/tasks' \
--header 'Content-Type: application/json' \
--data '{
    "from": "tmq://this/db1",
    "to": "local:/path/to/backup/directory"
}'

创建定时备份任务：参数 trigger 使用的是 7 位的 cron 表达式。例如：0 0 1 1 * * * 表示每天凌晨1点执行一次
curl --location 'localhost:6050/tasks' \
--header 'Content-Type: application/json' \
--data '{
    "from": "tmq://this/db1",
    "to": "local:/path/to/backup/directory",
    "trigger": "0 0 1 1 * * *"
}'

创建恢复任务：
curl --location 'localhost:6050/tasks' \
--header 'Content-Type: application/json' \
--data '{
    "from": "local:/path/to/backups/of/one",
    "to": "taos://root:taosdata@another.com:6030/db1"
}'
```


## 数据导出和导入

数据导出是将当前所连接的 TDengine 集群中将数据导出为多种不同的格式。目前支持的格式有 partquet，CSV。

**工具模式**

```shell
导出为 CSV：taosx run -f 'taos://root:taosdata@localhost:6030/test?query=select * from meters' -t 'csv:./meters.csv'
导出为 Parquet：taosx run -f 'taos://root:taosdata@localhost:6030/test?query=select * from meters' -t 'parquet:./meters.parquet'
```

**服务模式**

```shell
创建导出为 CSV 文件的任务：
curl --location 'localhost:6050/tasks' \
--header 'Content-Type: application/json' \
--data '{
    "from": "taos://root:taosdata@localhost:6030/test?query=select * from meters",
    "to": "csv:./meters.csv"
}'

创建导出为 Parquet 文件的任务：
curl --location 'localhost:6050/tasks' \
--header 'Content-Type: application/json' \
--data '{
    "from": "taos://root:taosdata@localhost:6030/test?query=select * from meters",
    "to": "parquet:./meters.parquet"
}'
```


## 数据迁移

数据迁移是将 2.x 版本 TDengine 集群中的数据迁移到 3.0 版本 TDengine 集群。

参数说明：

| 参数名称           | 说明                                                                                                                                                                                                                                      | 默认值                                 |
|--------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------|
| libraryPath        | 在 option 模式下指定 taos 库路径                                                                                                                                                                                                          | 无                                     |
| configDir          | 指定 taos.cfg 配置文件路径                                                                                                                                                                                                                | 无                                     |
| mode               | 数据源参数。 history 表示历史数据。 realtime 表示实时同步。 all 表示以上两种。                                                                                                                                                            | history                                |
| restro             | 数据源参数。 在同步实时数据前回溯指定时间长度的数据进行同步。 restro=10m 表示回溯最近 10 分钟的数据以后，启动实时同步。                                                                                                                   | 无                                     |
| interval           | 数据源参数。 轮询间隔 ，mode=realtime&interval=5s 指定轮询间隔为 5s                                                                                                                                                                       | 无                                     |
| excursion          | 数据源参数。 允许一段时间的乱序数据                                                                                                                                                                                                       | 500ms                                  |
| stables            | 数据源参数。 仅同步指定超级表的数据，多个超级表名用英文逗号 ,分隔                                                                                                                                                                         | 无                                     |
| tables             | 数据源参数。 仅同步指定子表的数据，表名格式为 {stable}.{table} 或 {table}，多个表名用英文逗号 , 分隔，支持 @filepath 的方式输入一个文件，每行视为一个表名，如 tables=@./tables.txt 表示从 ./tables.txt 中按行读取每个表名，空行将被忽略。 | 无                                     |
| select-from-stable | 数据源参数。 从超级表获取 select {columns} from stable where tbname in ({tbnames}) ，这种情况 tables 使用 {stable}.{table} 数据格式，如 meters.d0 表示 meters 超级表下面的 d0 子表。                                                      | 默认使用 select \* from table 获取数据 |
| assert             | 目标源参数。 taos:///db1?assert 将检测数据库是否存在，如不存在，将自动创建目标数据库。                                                                                                                                                    | 默认不自动创建库。                     |
| force-stmt         | 目标源参数。 当 TDengine 版本大于 3.0 时，仍然使用 STMT 方式写入。                                                                                                                                                                        | 默认为 raw block 写入方式              |
| batch-size         | 目标源参数。 设置 STMT 写入模式下的最大批次插入条数。                                                                                                                                                                                     |                                        |
| interval           | 目标源参数。 每批次写入后的休眠时间。                                                                                                                                                                                                     | 无                                     |
| max-sql-length     | 目标源参数。 用于建表的 SQL 最大长度，单位为 bytes。                                                                                                                                                                                      | 默认 800_000 字节。                    |
| failes-to          | 目标源参数。 添加此参数，值为文件路径，将写入错误的表及其错误原因写入该文件，正常执行其他表的同步任务。                                                                                                                                   | 默认写入错误立即退出。                 |
| timeout-per-table  | 目标源参数。 为子表或普通表同步任务添加超时。                                                                                                                                                                                             | 无                                     |
| update-tags        | 目标源参数。 检查子表存在与否，不存在时正常建表，存在时检查标签值是否一致，不一致则更新。                                                                                                                                                 | 无                                     |


**工具模式**

```shell
taosx run -f 'taos://td2:6030/db1?libraryPath=./libtaos.so.2.6.0.30&mode=all' -t 'taos://td3:6030/db2?libraryPath=./libtaos.so.3.0.1.8' -v
```

**服务模式**

```shell
curl --location 'localhost:6050/tasks' \
--header 'Content-Type: application/json' \
--data '{
    "from": "taos://td2:6030/db1?libraryPath=./libtaos.so.2.6.0.30&mode=all",
    "to": "taos://td3:6030/db2?libraryPath=./libtaos.so.3.0.1.8"
}'
```
