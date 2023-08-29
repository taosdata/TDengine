---
title: 数据接入、同步和备份
---

## 简介

为了能够方便地将各种数据源中的数据导入 TDengine 3.0，TDengine 3.0 企业版提供了一个全新的工具 taosX 用于帮助用户快速将其它数据源中的数据传输到 TDengine 中。 taosX 定义了自己的集成框架，方便扩展新的数据源。目前支持的数据源有 TDengine 自身（即从一个 TDengine 集群到另一个 TDengine 集群），Pi, OPC UA。除了数据接入外，taosX 还支持数据备份、数据同步、数据迁移以及数据导出功能。

**欲体验 taosX 的各种数据接入能力，请联系 TDengine 市场或销售团队。**

## 使用前提

使用 taosX 需要已经部署好 TDengine 中的 taosd 和 taosAdapter，具体细节请参考 [系统部署](../../deployment/deploy)

**使用限制**：taosX 只能用于企业版数据库服务端。

## 安装与配置

安装 taosX 需要使用独立的 taosX 安装包，其中除了 taosX 之外，还包含 Pi 连接器（限 Windows）， OPC 连接器， InfluxDB 连接器， MQTT 连接器，以及必要的 Agent 组件，taosX + Agent + 某个连接器可以用于将相应数据源的数据同步到 TDengine。taosX 安装包中还包含了 taos-explorer 这个可视化管理组件

### Linux 安装

下载需要的 taosX 安装包，下文以安装包 `taosx-1.0.0-linux-x64.tar.gz` 为例展示如何安装：

``` bash
# 在任意目录下解压文件
tar -zxf taosx-1.0.0-linux-x64.tar.gz
cd taosx-1.0.0-linux-x64

# 安装
sudo ./install.sh

# 验证
taosx -V 
# taosx 1.0.0-494d280c (built linux-x86_64 2023-06-21 11:06:00 +08:00)
taosx-agent -V 
# taosx-agent 1.0.0-494d280c (built linux-x86_64 2023-06-21 11:06:01 +08:00)

# 卸载
cd /usr/local/taosx
sudo ./uninstall.sh
```

**常见问题:**

1. 安装后系统中增加了哪些文件？
    * /usr/bin: taosx, taosx-agent, taos-explorer
    * /usr/local/taosx/plugins: influxdb, mqtt, opc
    * /etc/systemd/system:taosx.service, taosx-agent.service, taos-explorer.service
    * /usr/local/taosx: uninstall.sh 
    * /etc/taox: agent.toml, explorer.toml

2. taosx -V 提示 "Command not found" 应该如何解决？
    * 检验问题1，保证所有的文件都被复制到对应的目录
    ``` bash
    ls /usr/bin | grep taosx
    ```

### Windows 安装

- 下载需要的 taosX 安装包，例如 taosx-1.0.0-Windows-x64-installer.exe，执行安装
- 可使用 uninstall_taosx.exe 进行卸载
- 命令行执行 ```sc start/stop taosx``` 启动/停止 taosx 服务
- 命令行执行 ```sc start/stop taosx-agent``` 启动/停止 taosx-agent 服务
- 命令行执行 ```sc start/stop taos-explorer``` 启动/停止 taosx-agent 服务
- windows 默认安装在```C:\Program Files\taosX```,目录结构如下：
~~~
├── bin
│   ├── taosx.exe
│   ├── taosx-srv.exe
│   ├── taosx-srv.xml
│   ├── taosx-agent.exe
│   ├── taosx-agent-srv.exe
│   ├── taosx-agent-srv.xml
│   ├── taos-explorer.exe
│   ├── taos-explorer-srv.exe
│   └── taos-explorer-srv.xml
├── plugins
│   ├── influxdb
│   │   └── taosx-inflxdb.jar
│   ├── mqtt
│   │   └── taosx-mqtt.exe
│   ├── opc
│   |    └── taosx-opc.exe
│   ├── pi
│   |   └── taosx-pi.exe
│   |   └── taosx-pi-backfill.exe
│   |   └── ...
└── config
│   ├── agent.toml
│   ├── explorer.toml
├── uninstall_taosx.exe
├── uninstall_taosx.dat
~~~

**运行模式**

taosX 是进行数据同步与复制的核心组件，以下运行模式指 taosX 的运行模式，其它组件的运行模式在 taosX 的不同运行模式下与之适配。

## 命令行模式

可以直接在命令行上添加必要的参数直接启动 taosX 即为命令行模式运行。当命令行参数所指定的任务完成后 taosX 会自动停止。taosX 在运行中如果出现错误也会自动停止。也可以在任意时刻使用 ctrl+c 停止 taosX 的运行。本节介绍如何使用 taosX 的各种使用场景下的命令行。

### 命令行参数说明

**注意：部分参数暂无法通过 explorer设置【见：其他参数说明】，之后会逐步开放） **

命令行执行示例：

```shell
taosx -f <from-DSN> -t <to-DSN> <其他参数>
```

以下参数说明及示例中若无特殊说明 `<content>` 的格式均为占位符，使用时需要使用实际参数进行替换。

### DSN (Data Source Name)

taosX 命令行模式使用 DSN 来表示一个数据源（来源或目的源），典型的 DSN 如下：

```bash
# url-like
<driver>[+<protocol>]://[[<username>:<password>@]<host>:<port>][/<object>][?<p1>=<v1>[&<p2>=<v2>]]
|------|------------|---|-----------|-----------|------|------|----------|-----------------------|
|driver|   protocol |   | username  | password  | host | port |  object  |  params               |

// url 示例
tmq+ws://root:taosdata@localhost:6030/db1?timeout=never
```
[] 中的数据都为可选参数。

1. 不同的驱动 (driver) 拥有不同的参数。driver 包含如下选项:

- taos：使用查询接口从 TDengine 获取数据
- tmq：启用数据订阅从 TDengine 获取数据
- local：数据备份或恢复
- pi: 启用 pi-connector从 pi 数据库中获取数据
- opc：启用 opc-connector 从 opc-server 中获取数据
- mqtt: 启用 mqtt-connector 获取 mqtt-broker 中的数据
- kafka: 启用 Kafka 连接器从 Kafka Topics 中订阅消息写入
- influxdb:  启用 influxdb 连接器从 InfluxDB 获取数据
- csv：从 CSV 文件解析数据

2. +protocol 包含如下选项：
- +ws: 当 driver 取值为 taos 或 tmq 时使用，表示使用 rest 获取数据。不使用 +ws 则表示使用原生连接获取数据，此时需要 taosx 所在的服务器安装 taosc。
- +ua: 当 driver 取值为 opc 时使用，表示采集的数据的 opc-server 为 opc-ua
- +da: 当 driver 取值为 opc 时使用，表示采集的数据的 opc-server 为 opc-da

3. host:port 表示数据源的地址和端口。
4. object 表示具体的数据源，可以是TDengine的数据库、超级表、表，也可以是本地备份文件的路径，也可以是对应数据源服务器中的数据库。
5. username 和 password 表示该数据源的用户名和密码。
6. params 代表了 dsn 的参数。

### 其它参数说明

1. parser 通过 --parser 或 -p 设置，设置 transform 的 parser 生效。可以通过 Explorer 在如 CSV，MQTT，KAFKA 数据源的任务配置进行设置。

  配置示例：

  ```shell
  --parser "{\"parse\":{\"ts\":{\"as\":\"timestamp(ms)\"},\"topic\":{\"as\":\"varchar\",\"alias\":\"t\"},\"partition\":{\"as\":\"int\",\"alias\":\"p\"},\"offset\":{\"as\":\"bigint\",\"alias\":\"o\"},\"key\":{\"as\":\"binary\",\"alias\":\"k\"},\"value\":{\"as\":\"binary\",\"alias\":\"v\"}},\"model\":[{\"name\":\"t_{t}\",\"using\":\"kafka_data\",\"tags\":[\"t\",\"p\"],\"columns\":[\"ts\",\"o\",\"k\",\"v\"]}]}"

  ```

2. transform 通过 --transform 或 -T 设置，配置数据同步（仅支持 2.6 到 3.0 以及 3.0 之间同步）过程中对于表名及表字段的一些操作。暂无法通过 Explorer 进行设置。配置说明如下：
   
  ```shell
  1.AddTag，为表添加 TAG。设置示例：-T add-tag:<tag1>=<value1>。
  2.表重命名：
      2.1 重命名表限定
          2.1.1 RenameTable：对所有符合条件的表进行重命名。
          2.1.2 RenameChildTable：对所有符合条件的子表进行重命名。
          2.1.3 RenameSuperTable：对所有符合条件的超级表进行重命名。
      2.2 重命名方式
          2.2.1 Prefix：添加前缀。
          2.2.2 Suffix：添加后缀。
          2.2.3 Template：模板方式。
          2.2.4 ReplaceWithRegex：正则替换。taosx 1.1.0 新增。
  重命名配置方式：
      <表限定>:<重命名方式>:<重命名值>
  使用示例：
      1.为所有表添加前缀 <prefix>
      --transform rename-table:prefix:<prefix>
      2.为符合条件的表替换前缀：prefix1 替换为 prefix2，以下示例中的 <> 为正则表达式的不再是占位符。
      -T rename-child-table:replace_with_regex:^prefix1(?<old>)::prefix2_$old

      示例说明：^prefix1(?<old>) 为正则表达式，该表达式会匹配表名中包含以 prefix1 开始的表名并将后缀部分记录为 old，prefix2$old 则会使用 prefix2 与 old 进行替换。注意：两部分使用关键字符 :: 进行分隔，所以需要保证正则表达式中不能包含该字符。
      若有更复杂的替换需求请参考：https://docs.rs/regex/latest/regex/#example-replacement-with-named-capture-groups 或咨询 taosx 开发人员。
  ```

3. jobs 指定任务并发数，仅支持 tmq 任务。暂无法通过 Explorer 进行设置。通过 --jobs `<number>` 或 -j `<number>` 进行设置。
4. -v 用于指定 taosx 的日志级别，-v 表示启用 info 级别日志，-vv 对应 debug，-vvv 对应 trace。


### 从 TDengine 到 TDengine 的数据同步

#### TDengine 3.0 -> TDengine 3.0

在两个相同版本 （都是 3.0.x.y）的 TDengine 集群之间将源集群中的存量及增量数据同步到目标集群中。

命令行模式下支持的参数如下：

| 参数名称  | 说明                                                             | 默认值                     |
|-----------|------------------------------------------------------------------|----------------------------|
| group.id  | 订阅使用的分组ID                                                 | 若为空则使用 hash 生成一个 |
| client.id | 订阅使用的客户端ID                                               | taosx                      |
| timeout   | 监听数据的超时时间，当设置为 never 表示 taosx 不会停止持续监听。 | 500ms                      |
| offset    | 从指定的 offset 开始订阅，格式为 `<vgroup_id>:<offset>`，若有多个 vgroup 则用半角逗号隔开 | 若为空则从 0 开始订阅  |
| token     | 目标源参数。 认证使用参数。                              | 无                                     |

示例：
```shell
taosx run \
  -f 'tmq://root:taosdata@localhost:6030/db1?group.id=taosx1&client.id=taosx&timeout=never&offset=2:10' \
  -t 'taos://root:taosdata@another.com:6030/db2'
```



#### TDengine 2.6 -> TDengine 3.0

将 2.6 版本 TDengine 集群中的数据迁移到 3.0 版本 TDengine 集群。

#### 命令行参数

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

#### 示例

1.使用原生连接同步数据

```shell
taosx run \
  -f 'taos://td1:6030/db1?libraryPath=./libtaos.so.2.6.0.30&mode=all' \
  -t 'taos://td2:6030/db2?libraryPath=./libtaos.so.3.0.1.8&assert \
  -v
```

2.使用 WebSocket 同步数据超级表 stable1 和 stable2 的数据

```shell
taosx run \
  -f 'taos+ws://<username>:<password>@td1:6041/db1?stables=stable1,stable2' \
  -t 'taos+wss://td2:6041/db2?assert&token=<token> \
  -v
```

### 从 TDengine 备份数据文件到本地

示例：
```shell
taosx run -f 'tmq://root:taosdata@td1:6030/db1' -t 'local:/path_directory/'

```
以上示例执行的结果及参数说明：

将集群 td1 中的数据库 db1 的所有数据，备份到 taosx 所在设备的 /path_directory 路径下。

数据源(-f 参数的 DSN)的 object 支持配置为 数据库级(dbname)、超级表级(dbname.stablename)、子表/普通表级(dbname.tablename)，对应备份数据的级别数据库级、超级表级、子表/普通表级


### 从本地数据文件恢复到 TDengine

#### 示例
```shell
taosx run -f 'local:/path_directory/' -t 'taos://root:taosdata@td2:6030/db1?assert'
```

以上示例执行的结果：

将 taosx 所在设备 /path_directory 路径下已备份的数据文件，恢复到集群 td2 的数据库 db1 中，如果 db1 不存在，则自动建库。

目标源(-t 参数的 DSN)中的 object 支持配置为数据库(dbname)、超级表(dbname.stablename)、子表/普通表(dbname.tablename)，对应备份数据的级别数据库级、超级表级、子表/普通表级，前提是备份的数据文件也是对应的数据库级、超级表级、子表/普通表级数据。


#### 常见错误排查

(1) 如果使用原生连接，任务启动失败并报以下错误：

```text
Error: tmq to td task exec error

Caused by:
    [0x000B] Unable to establish connection
```
产生原因是与数据源的端口链接异常，需检查数据源 FQDN 是否联通及端口 6030 是否可正常访问。

(2) 如果使用 WebSocket 连接，任务启动失败并报以下错误：

```text
Error: tmq to td task exec error

Caused by:
    0: WebSocket internal error: IO error: failed to lookup address information: Temporary failure in name resolution
    1: IO error: failed to lookup address information: Temporary failure in name resolution
    2: failed to lookup address information: Temporary failure in name resolution
```

使用 WebSocket 连接时可能遇到多种错误类型，错误信息可以在 ”Caused by“ 后查看，以下是几种可能的错误：

- "Temporary failure in name resolution": DNS 解析错误，检查 IP 或 FQDN 是否能够正常访问。
- "IO error: Connection refused (os error 111)": 端口访问失败，检查端口是否配置正确或是否已开启和可访问。
- "IO error: received corrupt message": 消息解析失败，可能是使用了 wss 方式启用了 SSL，但源端口不支持。
- "HTTP error: *": 可能连接到错误的 taosAdapter 端口或 LSB/Nginx/Proxy 配置错误。
- "WebSocket protocol error: Handshake not finished": WebSocket 连接错误，通常是因为配置的端口不正确。

(3) 如果任务启动失败并报以下错误：

```text
Error: tmq to td task exec error

Caused by:
    [0x038C] WAL retention period is zero
```

是由于源端数据库 WAL 配置错误，无法订阅。

解决方式：
修改数据 WAL 配置：

```sql
alter database test wal_retention_period 3600;
```

### 从 OPC-UA 同步数据到 TDengine

#### 配置参数

| 参数名称 | 类型    | 描述                                   |  
|-----------------|--------|-----------------------------------------------------------------------------|
| interval | int    | 采集间隔（单位：秒），默认为1秒                                   |
| concurrent | int    | 采集器并发数，默认为1                                   |
| batch_size | int    | 采集器上报的批次点位数，默认为100                                   |
| batch_timeout | int    | 采集器上报的超时时间（单位：秒），默认为20秒                                   |
| connect_timeout | int    | 连接的超时时间（单位：秒），默认为10秒                                  |
| request_timeout | int    | 请求的超时时间（单位：秒），默认为10秒                                              |
| security_policy | string | OPC-UA连接安全策略（可配置为None/Basic128Rsa15/Basic256/Basic256Sha256）                                  |
| security_mode   | string | OPC-UA连接模式（可配置为None/Sign/SignAndEncrypt）                                                    |
| certificate     | string | cert.pem的路径。当安全模式或策略不是”无”时生效        |
| private_key     | string | key.pem的路径。 当安全模式或策略不是”无”时生效 |
| csv_config_file | string | 包含 OPC UA 的点位配置和表配置。与配置 csv_config_file 配置互斥，csv_config_file 优先生效|
| ua.nodes | string | OPC-UA 测点的 NodeID。和 opc_table_config 配置结合使用，两者需要同时配置。与配置 csv_config_file 配置互斥，csv_config_file 优先生效。配置格式为 <nodeid\>::<code\>，code 用于建子表。|
| opc_table_config | string | OPCUA 单列模式表配置。需要与 ua.nodes 配合使用。|
| debug | bool | 启用 OPC 连接器的 debug 日志。默认为 false。|
| enable | bool | 原始数据存储。默认为 false|
| path | string | 原始数据存储路径。enable 为 true 时必须配置。|
| keep | int | 原始数据保存天数。enable 为 true 时必须配置。|

补充：
1. opc_table_config 说明：

```json
{
    "stable_prefix": "meters", // 超级表前缀
    "column_configs":
    [
        {
            "column_name": "received_time", // 存储接收时间
            "column_type": "timestamp",
            "column_alias": "ts", // 接收时间建表列用列名为 ts
            "is_primary_key": true // 接收时间时间戳作为主键
        },
        {
            "column_name": "original_time",
            "column_type": "timestamp",
            "column_alias": "ts_2",
            "is_primary_key": false
        },
        {
            "column_name": "value", // 数据列
            "column_alias": "valueaa", // 数据列别名
            "is_primary_key": false
        },
        {
            "column_name": "quality", // 质量位列
            "column_type": "int",
            "column_alias": "quality11", // 质量位列别名
            "is_primary_key": false
        }
    ]
}
```

#### 示例

1. 使用 ua.nodes 和 opc_table_config 的配置示例：
采集 nodeid 为 ns=2;i=2 和 ns=2;i=3 的点位，将其写入到集群 tdengine 的 opc 库中超级表前缀为 meters，如果 ns=2;i=2 的点位类型为 float 则会创建 meters_float 的超级表，超级表使用 opc 接收的数据作为时间戳索引列，并且保留原始时间戳列，原始时间戳列名为 ts_2,数据列存储为 valueaa，同时存储质量数据到 quality11 列。

```shell
taosx run \
    -f "opcua://uauser:uapass@localhost:4840?ua.nodes=ns=2;i=2::DSF1312,ns=2;i=3::DSF1313&opc_table_config={\"stable_prefix\": \"meters\", \"column_configs\": [{\"column_name\": \"received_time\", \"column_type\": \"timestamp\", \"column_alias\": \"ts\", \"is_primary_key\": true }, {\"column_name\": \"original_time\", \"column_type\": \"timestamp\", \"column_alias\": \"ts_2\", \"is_primary_key\": false }, {\"column_name\": \"value\", \"column_alias\": \"valueaa\", \"is_primary_key\": false }, {\"column_name\": \"quality\", \"column_type\": \"int\", \"column_alias\": \"quality11\", \"is_primary_key\": false } ] }" \
    -t "taos://tdengine:6030/opc"
 


```

2. 使用 CSV 配置文件

```shell
taosx run -f "opcua://<server-info>?csv_config_file=@<file_path>" -t "taos+ws://tdengine:6041/opc"
```

#### CSV 配置文件模板


### 从 OPC-DA 同步数据到 TDengine (Windows)

#### 配置参数

| 参数名称 | 类型    | 描述                                   |
|-----------------|--------|-----------------------------------------------------------------------------|
| interval | int    | 采集间隔（单位：秒），默认为1秒                                   |
| concurrent | int    | 采集器并发数，默认为1                                   |
| batch_size | int    | 采集器上报的批次点位数，默认为100                                   |
| batch_timeout | int    | 采集器上报的超时时间（单位：秒），默认为20秒                                   |
| connect_timeout | int    | 连接的超时时间（单位：秒），默认为10秒                                  |
| request_timeout | int    | 请求的超时时间（单位：秒），默认为10秒                                              |
| csv_config_file | string | 包含 OPC UA 的点位配置和表配置。与 ua.nodes 两者之间需要配置一个。CSV 的配置模版参考：OPC 需求汇总及完成现状 |
| da.tags | string | OPC-UA 测点的 NodeID。和 opc_table_config 配置结合使用，两者需要同时配置。与配置 csv_config_file 配置互斥，csv_config_file 优先生效。|
| opc_table_config | string | OPCUA 单列模式表配置。需要与 da.tags 配合使用|
| debug | bool | 启用 OPC 连接器的 debug 日志。默认为 false。|
| enable | bool | 原始数据存储。默认为 false|
| path | string | 原始数据存储路径。enable 为 true 时必须配置。|
| keep | int | 原始数据保存天数。enable 为 true 时必须配置。|

#### 应用示例

```shell
taosx run \
    -f "opc+da://Matrikon.OPC.Simulation.1?nodes=localhost&da.tags=Random.Real8::tb3::c1::int"
    -t "taos://tdengine:6030/opc"
```

以上示例的执行结果：

采集 Matrikon.OPC.Simulation.1 服务器上 OPC DA 中 da.tags 为 Random.Real8的数据，数据类型为int，对应在 TDengine 中以表名为 tb3 ，列名为c1，列类型为 int 型 schema 来创建表（如果对应表已存在，则直接采集数据并写入）。

#### 常见错误排查

(1) 如果使用原生连接，任务启动失败并打印如下错误：
```text
Error: tmq to td task exec error

Caused by:
    0: Error occurred while creating a new object: [0x000B] Unable to establish connection
```
解决方式：

检查目标端 TDengine 的 FQDN 是否联通及端口 6030 是否可正常访问。

(2) 如果使用 WebSocket 连接任务启动失败并打印如下错误：：

```text
Error: tmq to td task exec error

Caused by:
    0: WebSocket internal error: IO error: failed to lookup address information: Temporary failure in name resolution
    1: IO error: failed to lookup address information: Temporary failure in name resolution
    2: failed to lookup address information: Temporary failure in name resolution
```

使用 WebSocket 连接时可能遇到多种错误类型，错误信息可以在 ”Caused by“ 后查看，以下是几种可能的错误：

- "Temporary failure in name resolution": DNS 解析错误，检查目标端 TDengine的 IP 或 FQDN 是否能够正常访问。
- "IO error: Connection refused (os error 111)": 端口访问失败，检查目标端口是否配置正确或是否已开启和可访问（通常为6041端口）。
- "HTTP error: *": 可能连接到错误的 taosAdapter 端口或 LSB/Nginx/Proxy 配置错误。
- "WebSocket protocol error: Handshake not finished": WebSocket 连接错误，通常是因为配置的端口不正确。

### 从 PI 同步数据到 TDengine (Windows)

#### PI DSN 配置

PI DSN 的完整配置如下：

```shell
pi://[<username>:<password>@]PIServerName/AFDatabaseName?[TemplateForPIPoint][&TemplateForAFElement][&PointList][&<PISystemName=pisys>][&<MaxWaitLen>][&UpdateInterval]
```

在 taosX CLI 运行时支持的参数如下，其中 TemplateForPIPoint、TemplateForAFElement、PointList 三个参数至少配置一项：
- PISystemName：选填，连接配置 PI 系统服务名，默认值与 PIServerName 一致
- MaxWaitLen：选填，数据最大缓冲条数，默认值为 1000 ,有效取值范围为 [1,10000]
- UpdateInterval：选填，PI System 取数据频率，默认值为 10000(毫秒：ms),有效取值范围为 [10,600000]
- TemplateForPIPoint：选填，使用 PI Point 模式将模板按照 element 的每个 Arrtribution 作为子表导入到 TDengine 
- TemplateForAFElement：选填，使用 AF Point 模式将模板按照 element 的 Attribution 集合作为一个子表导入到 TDengine 
- PointList：选填，使用 PointList 模式将指定csv文件中描述的点位信息在 PI 数据库中的数据导入到 TDengine


#### 应用示例

将位于服务器 WIN-2OA23UM12TN 中的 PI 数据库 Met1，模板 template1、template2配置为 TemplateForPIPoint模式，模板 template3、template4 配置为 TemplateForAFElement 模式，服务器 /home/ 路径下的点位文件 points.csv 配置为 PointList 模式，连接配置 PI 系统服务名为 PI，数据最大缓冲条数为1000，PI System 取数据频率为10000ms，将该库中的数据同步到 服务器 tdengine 的 pi 库中。完整的示例如下：

```shell
taosx run \
    -f "pi://WIN-2OA23UM12TN/Met1?TemplateForPIPoint=template1,template2&TemplateForAFElement=template3,template4" \
    -t "taos://tdengine:6030/pi"
```


#### 常见错误排查

(1) 如果使用原生连接，任务启动失败并打印如下错误：
```text
Error: tmq to td task exec error

Caused by:
    0: Error occurred while creating a new object: [0x000B] Unable to establish connection
```
解决方式：

检查目标端 TDengine 的 FQDN 是否联通及端口 6030 是否可正常访问。

(2) 如果使用 WebSocket 连接任务启动失败并打印如下错误：：

```text
Error: tmq to td task exec error

Caused by:
    0: WebSocket internal error: IO error: failed to lookup address information: Temporary failure in name resolution
    1: IO error: failed to lookup address information: Temporary failure in name resolution
    2: failed to lookup address information: Temporary failure in name resolution
```

使用 WebSocket 连接时可能遇到多种错误类型，错误信息可以在 ”Caused by“ 后查看，以下是几种可能的错误：

- "Temporary failure in name resolution": DNS 解析错误，检查目标端 TDengine的 IP 或 FQDN 是否能够正常访问。
- "IO error: Connection refused (os error 111)": 端口访问失败，检查目标端口是否配置正确或是否已开启和可访问（通常为6041端口）。
- "HTTP error: *": 可能连接到错误的 taosAdapter 端口或 LSB/Nginx/Proxy 配置错误。
- "WebSocket protocol error: Handshake not finished": WebSocket 连接错误，通常是因为配置的端口不正确。


### 从 InfluxDB 同步数据到 TDengine

#### 命令行参数

将数据从 InfluxDB 同步至 TDengine 的命令，如下所示：

```bash
taosx run --from "<InfluxDB-DSN>" --to "<TDengine-DSN>"
```

其中，InfluxDB DSN 符合 DSN 的通用规则，这里仅对其特有的参数进行说明：
- version: 必填，InfluxDB 的版本，主要用于区分 1.x 与 2.x 两个版本，二者使用不同的认证参数；
- version = 1.x
  - username: 必填，InfluxDB 用户，该用户至少在该组织中拥有读取权限；
  - password: 必填，InfluxDB 用户的登陆密码；
- version = 2.x
  - orgId: 必填，InfluxDB 中的 Orgnization ID；
  - token: 必填，InfluxDB 中生成的 API token, 这个 token 至少要拥有以上 Bucket 的 Read 权限；
- bucket: 必填，InfluxDB 中的 Bucket 名称，一次只能同步一个 Bucket；
- measurements: 非必填，可以指定需要同步的多个 Measurements（英文逗号分割），未指定则同步全部；
- beginTime: 必填，格式为：YYYY-MM-DD'T'HH:MM:SS'Z', 时区采用 UTC 时区，例如：2023-06-01T00:00:00+0800, 即北京时间2023-06-01 00:00:00（东八区时间）；
- endTime: 非必填，可以不指定该字段或值为空，格式与beginTime相同；如果未指定，提交任务后，将持续进行数据同步；
- readWindow: 非必填，可以不指定该字段或值为空，可选项为D、H、M（天、时、分）；如果未指定，则默认按 M 拆分读取窗口。

#### 示例

将位于 192.168.1.10 的 InfluxDB 中, Bucket 名称为 test_bucket, 从UTC时间2023年06月01日00时00分00秒开始的数据，通过运行在 192.168.1.20 上的 taoskeeper, 同步至 TDengine 的 test_db 数据库中，完整的命令如下所示：
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

在这个命令中，未指定endTime, 所以任务会长期运行，持续同步最新的数据。


### 从 OpenTSDB 同步数据到 TDengine

#### 命令行参数

将数据从 OpenTSDB 同步至 TDengine 的命令，如下所示：

```bash
taosx run --from "<OpenTSDB-DSN>" --to "<TDengine-DSN>"
```

其中，OpenTSDB DSN 符合 DSN 的通用规则，这里仅对其特有的参数进行说明：
- metrics: 非必填，可以指定需要同步的多个 Metrics（英文逗号分割），未指定则同步全部；
- beginTime: 必填，格式为：YYYY-MM-DD'T'HH:MM:SS'Z', 时区采用 UTC 时区，例如：2023-06-01T00:00:00+0800, 即北京时间2023-06-01 00:00:00（东八区时间）；
- endTime: 非必填，可以不指定该字段或值为空，格式与beginTime相同；如果未指定，提交任务后，将持续进行数据同步；
- readWindow: 非必填，可以不指定该字段或值为空，可选项为D、H、M（天、时、分）；如果未指定，则默认按分钟拆分读取窗口。

#### 示例

将位于 192.168.1.10 的 OpenTSDB 中, Metric 名称为 test_metric1 与 test_metric2 的两个数据源, 从UTC时间2023年06月01日00时00分00秒开始的数据，通过运行在 192.168.1.20 上的 taoskeeper, 同步至 TDengine 的 test_db 数据库中，完整的命令如下所示：

```bash
taosx run \
  --from "opentsdb+http://192.168.1.10:4242/?metrics=test_metric1,test_metric2&beginTime=2023-06-01T00:00:00+0800&readWindow=M" \
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

### 从 Kafka 同步数据到 TDengine

#### 命令行参数

taosx 支持从 Kafka 消费数据，写入 TDengine。命令如下所示：
```sehll
taosx run -f "<Kafka-DSN>" -t "<TDengine-DSN>"
```
或
```shell
taosx run -f "<Kafka-DSN>" -t "<TDengine-DSN>" --parser "@<parser-config-file-path>"
```
其中：
- -f或--from： Kafka 的 DSN
- -t或--to ：TDengine 的 DSN
- --parser ：一个 JSON 格式的配置文件，或JSON格式的字符串。
  
#### Kafka DSN 配置的配置

| 参数 | 说明 | 必填? | 缺省值 | 适用于 | 示例 | 
|-----|---------------|----------|---------|---------|----------|
| group| 消费者的group。允许组为空字符串，在这种情况下，生成的消费者将是无组的 | 否 | "" | 源端 | |
| topics | 指定要使用的主题。指定主题的所有可用分区都将被使用，除非在指定 topic_partitions 时被覆盖。| 该参数或topic_partitions必须至少指定一个，以便将主题分配给消费者。| None | 源端 |  topics=tp1,tp2 | 
| topic_partitions | 显式指定要使用的主题分区。只使用已标识主题的指定分区。 | 该参数或topics必须至少指定一个，以便将主题分配给消费者。 | None | 源端 | topic_partitions=tp1:0..2,tp2:1 |
| fallback_offset | topic偏移量时可能的值：- Earliest：接收最早的可用偏移量; - Latest：接收最近的偏移量; - ByTime(i64):用于请求在某一特定时间(ms)之前的所有消息;Unix时间戳(毫秒) | 否 | Earliest | 源端 | fallback_offset=Earliest | 
| offset_storage | 定义在获取或提交组偏移量时，要使用的可用存储：- Zookeeper：基于Zookeeper的存储(从kafka 0.8.1开始可用)；- Kafka：基于Kafka的存储(从Kafka 0.8.2开始可用)。这是组存储其偏移量的首选方法。  | 否 | Kafka | 源端  | offset_storage=Kafka |
| timeout | 从kafka订阅数据时，如果超时后没有获取到有效数据，退出 | 否 | 500 | 源端  | timeout=never | 
| use_ssl | 是否使用SSL认证 | 否 |  | 源端  | |
| cert | SSL证书的文件路径 | 否 | | | 源端  | |
| cert_key | SSL证书key的文件路径 | 否 | | 源端  ||


#### 示例一

从192.168.1.92服务器的Kafka实例中消费数据，同步到192.168.1.92上的TDengine，不使用parser。

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

#### 示例2

从192.168.1.92服务器的Kafka实例中消费数据，同步到192.168.1.92上的TDengine，使用parser解析value中的JSON数据。

1. kafka，同“示例1”
2. TDengine，同“示例1”
3. Taosx
   
```shell
taosx run -f "kafka://192.168.1.92:9092/?topics=tp1,tp2&timeout=5000" -t "taos://192.168.0.201:6030/kafka_to_taos" --parser "{\"parse\":{\"ts\":{\"as\":\"timestamp(ms)\"},\"topic\":{\"as\":\"varchar\",\"alias\":\"t\"},\"partition\":{\"as\":\"int\",\"alias\":\"p\"},\"offset\":{\"as\":\"bigint\",\"alias\":\"o\"},\"value\":{\"json\":[\"id::int\",\"message::binary\"]}},\"model\":[{\"name\":\"t_{t}\",\"using\":\"kafka_data\",\"tags\":[\"t\",\"p\"],\"columns\":[\"ts\",\"o\",\"id\",\"message\"]}]}"
```

## 服务模式

在服务模式下， 一共需要三个组件协同完成数据迁移。 taosX，Agent 以及 taosExplorer 均已服务态运行，各种操作通过 taosExplorer 的图形界面进行。taos-Explorer 组件除了数据迁移之外，还提供了使用 TDengine 的图形化界面。

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

Linux 系统上以 Systemd 的方式启动 taosX 的命令如下：

```shell
systemctl start taosx
```

Windows 系统上，请在 "Services" 系统管理工具中找到 "taosX" 服务，然后点击 "启动这个服务"。

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

```TOML
endpoint = "grpc://<taosx-ip>:6055"
token = "<token>"
log_level = "debug"
```

日志保存时间设置
日志保存的天数可以通过环境变量进行设置 TAOSX_LOGS_KEEP_DAYS， 默认为 30 天。

```shell
export TAOSX_LOGS_KEEP_DAYS=7
```

#### 启动

Linux 系统上 Agent 可以通过 Systemd 命令启动：

```
systemctl start taosx-agent
```

Windows 系统上通过系统管理工具 "Services" 找到 taosx-agent 服务，然后启动它。

#### 问题排查

可以通过 journalctl 查看 Agent 的日志

```
journalctl -u taosx-agent -f
```

### 部署 taosExplorer


### 数据同步功能

请参考 taosExplorer