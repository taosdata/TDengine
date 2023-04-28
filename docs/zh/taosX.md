---
title: 数据接入、同步、备份、导出和迁移
---

## 简介

为了能够方便地将各种数据源中的数据导入 TDengine 3.0，TDengine 3.0 企业版提供了一个全新的工具 taosX 用于帮助用户快速将其它数据源中的数据传输到 TDengine中。 taosX 定义了自己的集成框架，方便扩展新的数据源。目前支持的数据源有 TDengine 自身（即从一个 TDengine 集群到另一个 TDengine 集群），Pi, OPC UA。除了数据接入外，taosX 还支持数据备份、数据同步、数据迁移以及数据导出功能。

**使用限制**：taosX 只能用于企业版数据库服务端。

**安装与配置**

taosX 没有独立的安装包，在安装了 TDengine 企业版之后，您的系统中就已经拥有了 taosX，请使用 Linux 系统命令 which 来确认它存在于系统中。如果您希望用于将 Pi 或 OPC UA/DA 中的数据迁移到 TDengine 中，可以使用 Windwos 版本的独立安装包，包含 taosX + Pi 连接器，或者包含 taosX + OPC 连接器。

## 运行模式

1.  工具模式

在命令行上添加必要的参数直接启动 taosX 即为工具模式运行。当命令行参数所指定的任务完成后 taosX 会自动停止。taosX 在运行中如果出现错误也会自动停止。也可以在任意时刻使用 ctrl+c 停止 taosX 的运行。


2.  服务模式

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
- TemplateForPIPoint：使用 PI Point 模式将模板 template1 ，template2 下的所有Element导出到 TDengine 服务器 tdengine的 pi 库中
- TemplateForAFElement：使用 AF Point 模式将模板template3 ，template4 下的所有Element导出到 TDengine 服务器 tdengine的 pi 库中

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

## 可视化管理

为了易于企业版用户更容易使用和管理数据库，TDengine 3.0 企业版提供了一个全新的可视化组件 taosExplorer。用户能够在其中方便地管理数据库管理系统中中各元素（数据库、超级表、子表）的生命周期，执行查询，监控系统状态，管理用户和授权，完成数据备份和恢复，与其它集群之间进行数据同步，导出数据，管理 topic 和 stream 。

### 配置和启动

1.  taosExplorer 没有独立的安装包，请使用 TDegnine 企业版安装包进行安装。
2.  在启动 taosExplorer 之前，请先确认 TDengine 集群已经正确设置并运行（即 taosd 服务），taosAdapter 也已经正确设置和运行并与 TDengine 集群保持连接状态。如果想要使用数据备份和恢复或者数据同步功能，请确保 taosX 服务也已经正确设置和运行。
3.  在启动 taosExplorer 之前，请确保配置文件中的内容正确]

```TOML
listen = "0.0.0.0:6060"
log_level = "info"
x_api = "http://localhost:6050"
```

说明：

-   listen - taosExplorer 对外提供服务的地址
-   log_level - 日志级别，可选值为 "debug", "info", "warn", "error", "fatal"
-   x_api - taosX 的服务地址

然后启动 taosExplorer，可以直接在命令行执行 taos-explorer 或者使用下面的 systemctl 脚本用 systemctl 来启动 taosExplorer 服务

```TOML
[Unit]
Description=Explorer for TDengine
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/taos-explorer
Restart=always

[Install]
WantedBy=multi-user.target
```

### 登录

打开浏览器，输入正确的用户名和密码（注：这里的用户名和密码是所连接的 TDengine 中的用户名和密码）

![登录界面](./login.png)

点击登录按钮进入主界面，主界面功能展示如下：

![主界面示例](./ui.png)

### 仪表盘

taosExplorer 内置了一个简单的仪表盘展示以下集群信息，点击左侧功能列表中的 "Dashboard" 可以启用此功能。

-   Uptime: 集群自上次重启后迄今的运行时间
-   Create Time: 集群被创建的时间
-   Edition: TDengine 版本，正式企业版 或 试用企业版
-   Expire Time: 企业版的授权到期时间

![仪表盘](./dashboard.png)

### Explorer

点击功能列表的 "Explorer"入口，在 Explorer 中可以创建和删除数据库、创建和删除超级表和子表，执行SQL语句，查看SQL语句的执行结果。此外，超级管理员还有对数据库的管理权限，其他用户不提供该功能。

具体权限有：

1.查看（提供数据库/超级表/普通表的基本信息）

2.编辑 (编辑数据库/超级表/普通表的信息)

3.数据库管理权限 （仅限超级管理员，该操作可以给指定用户配置数据库管理权限）

4.删除 （删除数据库/超级表/普通表）

5.追加 （选择对应的数据库/超级表/普通表名称直接追加到右侧sql输入区域，避免了手工输入）

例：图为超级管理员权限，展示所有可操作内容

![数据浏览和管理](./data.png)

每个数据库下只有一个STables和Tables文件夹，分别用来添加超级表和普通表

![管理超级表和普通表](./stable.png)

### 用户管理

点击功能列表中的 "Admin" 入口，可以创建用户、对用户进行访问授权、以及删除用户。还能够对当前所管理的集群中的数据进行备份和恢复。也可以配置一个远程 TDengine 的地址进行数据同步。同时也提供了集群信息和license信息以供查看

![用户管理](./admin1.png)

![添加用户](./admin2.png)

### 数据接入

点击功能列表中的 "Data In"，可以配置不同类型的数据源，包括 TDengine, PI, OPC UA，将它们的数据接入到当前正在被管理的 TDengine 集群中。目前PI和OPC暂不支持用户自定义选择测点。

1.可以对已有的数据源进行删除和编辑操作，以及状态修改的操作

![数据接入](./in.png)

2.目前提供四种可添加的数据源类型，如图：

![添加数据源](./add-source.png)

3.添加PI数据类型如图：有红色星号的为必输项，其他的根据需求填写，点击Submit即可创建一条新的PI

![添加Pi数据源](./pi.png)

4.新增opcua如下。红色星号为必输项，填写对应数据点击Submit即可创建一条线的opcua

![添加 OPCA UA 数据源](./opcua.png)

5.创建opcda如下：

![添加 OPC DA 数据源](./opcda.png)

### 管理 topic

包括主题，消费者，共享主题和示例代码

1.主题：该区域可以进行Topic的创建和管理

![创建 Topic](./create-topic.png)

2.消费者：展示消费者信息

![消费者](./consumer.png)

3.共享主题：可进行用户的添加

![共享主题](./share-topic.png)

4.示例代码：以文档形式展现

![示例代码](./sample.png)

### 管理 stream

点击功能列表中的 "Stream" 入口，可以创建和管理 stream。

可通过wizard和sql两种方式进行stream的创建。wizard目前只支持聚合函数，且不支持分组

![流计算](./stream.png)

![创建流计算](./create-stream.png)

### 可视化

展示grafana和google data studio文档

### 编程

展示不同连接器语言的文档

### 工具

展示工具文档

### 数据输出

展示taosdump文档
