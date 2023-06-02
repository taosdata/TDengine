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

下载需要的 taosX 安装包，下文以安装包 `taosX-0.5.1-Linux-x64.tar.gz` 为例展示如何安装：

``` bash
# 在任意目录下解压文件
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

**常见问题:**

1. 安装后系统中增加了哪些文件？
    * /usr/local/taosX/bin: taosx, taosx-agent
    * /usr/local/taosX/plugins: taosx-influxdb, taosx-mqtt, taosx-opc, taosx-pi, taosx-pi-backfill
    * /usr/local/taosX/scripts:taosx.service, taosx-agent.service
    * /usr/local/taosX: install.sh, rmtaosX.sh 
    * /usr/local/taosX/config: config/agent.example.toml
    * /etc/taosX: config/agent.example.toml

2. taosX -V 提示 "Command not found" 应该如何解决？
    * 检验问题1，保证所有的文件都被复制到对应的目录
    * 如下创建软链接，或者确保 /usr/local/taosX/bin 被添加到系统环境变量 PATH 中
    ``` bash
    ln -s /usr/local/taosX/bin/taosx /usr/bin/taosx
    ln -s /usr/local/taosX/bin/taosx-agent /usr/bin/taosx-agent
    ln -s /usr/local/taosX/rmtaosX.sh /usr/bin/rmtaosx
    ```

### Windows 安装

- 下载需要的 taosX 安装包，例如 taosx-{version}-windows-installer.exe，执行安装
- 可使用 uninstall_taosx.exe 进行卸载
- 命令行执行 ```sc start/stop taosx``` 启动/停止 taosx 服务
- 命令行执行 ```sc start/stop taosx-ageent``` 启动/停止 taosx-agent 服务
- windows 默认安装在```C:\Program Files\taosX```,目录结构如下：
~~~
├── bin
│   ├── taosx.exe
│   ├── taosx-agent.exe
├── plugins
│   ├── influxdb
│   │   └── taosx-inflxdb.jar
│   ├── mqtt
│   │   └── taosx-mqtt.exe
│   └── opc
│       └── taosx-opc.exe
│   ├── influxdb
│   │   └── taosx-inflxdb.exe
│   └── pi
│       └── taosx-pi.exe
│       └── taosx-pi-backfill.exe
│       └── ...
└── config
│   ├── agent.example.toml
├── uninstall_taosx.exe
├── uninstall_taosx.dat
~~~

## 运行模式

taosX 是进行数据同步与复制的核心组件，以下运行模式指 taosX 的运行模式，其它组件的运行模式在 taosX 的不同运行模式下与之适配。

### 命令行模式

可以直接在命令行上添加必要的参数直接启动 taosX 即为命令行模式运行。当命令行参数所指定的任务完成后 taosX 会自动停止。taosX 在运行中如果出现错误也会自动停止。也可以在任意时刻使用 ctrl+c 停止 taosX 的运行。本节介绍如何使用 taosX 的各种使用场景下的命令行。

#### 从 TDengine 到 TDengine 的数据同步

##### TDengine 3.0 -> TDengine 3.0

在两个相同版本 （都是 3.0.x.y）的 TDengine 集群之间将源集群中的存量及增量数据同步到目标集群中。

参数说明：

| 参数名称  | 说明                                                             | 默认值                     |
|-----------|------------------------------------------------------------------|----------------------------|
| group.id  | 订阅使用的分组ID                                                 | 若为空则使用 hash 生成一个 |
| client.id | 订阅使用的客户端ID                                               | taosx                      |
| timeout   | 监听数据的超时时间，当设置为 never 表示 taosx 不会停止持续监听。 | 500ms                      |

示例：
```shell
taosx run -f 'tmq://root:taosdata@localhost:6030/db1?group.id=taosx1&client.id=taosx&timeout=never' -t 'taos://root:taosdata@another.com:6030/db2'
```
以上示例中的参数表示：


##### TDengine 2.4(2.6) -> TDengine 3.0

将 2.4（2.6） 版本 TDengine 集群中的数据迁移到 3.0 版本 TDengine 集群。



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

在 taosX CLI 运行时支持的参数如下：
- PISystemName：连接配置 PI 系统服务名，默认值与 PIServerName 一致
- MaxWaitLen：数据最大缓冲条数，默认值为1000
- UpdateInterval：PI System 取数据频率，默认值为10000（毫秒：ms）

应用示例：

```shell
taosx run \
    -f "pi://WIN-2OA23UM12TN/Met1?TemplateForPIPoint=template1,template2&TemplateForAFElement=template3,template4" \
    -t "taos://tdengine:6030/pi"
```

以上示例的PI参数：
- PIServerName：PI 连接配置主机名 ，此示例中为 WIN-2OA23UM12TN
- AFDatabaseName：指定连接的 PI 数据库，此示例中为 Met1
- TemplateForPIPoint：使用 PI Point 模式将模板 template1 ，template2 ，按照 element 的每个 Arrtribution 作为子表导入到 TDengine 服务器 tdengine 的 pi 库中
- TemplateForAFElement：使用 AF Point 模式将模板template3 ，template4 ，按照 element 的 Attribution 集合作为一个子表导入到 TDengine 服务器 tdengine的 pi 库中


### 从 InfluxDB 同步数据到 TDengine

@zhengqin

### 从 MQTT 同步数据到 TDengine

@xuwang

### 服务模式

在服务模式下， taosX，Agent 以及 taosExplorer 均已服务态运行，各种操作通过 taosExplorer 的图形界面进行。

### 部署 taosX

@xuwang，此处添加如何配置和启动 taosX 服务，以及如何查看 taosX 日志排查常见错误，并举例常见错误

#### 配置

#### 启动

#### 问题排查

### 部署 Agent 

@xuwang，此处添加如何配置和启动 Agent 服务，以及如何查看 Agent 日志排查常见错误，并举例常见错误

#### 配置

#### 启动

#### 问题排查

### 部署 taosExplorer

请参考  taosExplorer


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
| offset    | 从指定的 offset 开始订阅，格式为 `<vgroup_id>:<offset>`，若有多个 vgroup 则用半角逗号隔开 | 若为空则从 0 开始订阅  |



**工具模式**

```shell
taosx run -f 'tmq://root:taosdata@localhost:6030/db1?timeout=never' -t 'taos://root:taosdata@another.com:6030/db2'
```

从指定 offset 开始订阅：

```shell
taosx run -f 'tmq://root:taosdata@localhost:6030/db1?offset=2:17,3:20' -t 'taos://root:taosdata@another.com:6030/db2'
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

从指定 offset 开始订阅：

```shell
curl --location 'localhost:6050/tasks' \
--header 'Content-Type: application/json' \
--data '{
    "from": "tmq+ws://root:taosdata@localhost:6041/db1?offset=2:17,3:20",
    "to": "taos+ws://root:taosdata@another.com:6041/db2"
}'
```

返回消费进度

```shell
curl -X 'GET' \
  'http://localhost:6050/tasks/{id}/offsets' \
  -H 'accept: text/plain'
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
