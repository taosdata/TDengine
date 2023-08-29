---
toc_max_heading_level: 4
title: taosX
---

## 简介

为了能够方便地将各种数据源中的数据导入 TDengine 3.0，TDengine 3.0 企业版提供了一个全新的工具 taosX 用于帮助用户快速将其它数据源中的数据传输到 TDengine 中。 taosX 定义了自己的集成框架，方便扩展新的数据源。目前支持的数据源有 TDengine 自身（即从一个 TDengine 集群到另一个 TDengine 集群），Pi, OPC UA。除了数据接入外，taosX 还支持数据备份、数据同步、数据迁移以及数据导出功能。

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

## 服务模式

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

#### 准备工作

1.  taosExplorer 没有独立的安装包，请使用 taosX 安装包进行安装。
2.  在启动 taosExplorer 之前，请先确认 TDengine 集群已经正确设置并运行（即 taosd 服务），taosAdapter 也已经正确设置和运行并与 TDengine 集群保持连接状态。如果想要使用数据备份和恢复或者数据同步功能，请确保 taosX 服务和 Agent 服务也已经正确设置和运行。

#### 配置

在启动 taosExplorer 之前，请确保配置文件中的内容正确。

```TOML
listen = "0.0.0.0:6060"
log_level = "info"
cluster = "http://localhost:6041"
x_api = "http://localhost:6050"
```

说明：

-   listen - taosExplorer 对外提供服务的地址
-   log_level - 日志级别，可选值为 "debug", "info", "warn", "error", "fatal"
-   cluster - TDengine集群的 taosadapter 地址 
-   x_api - taosX 的服务地址

#### 启动

然后启动 taosExplorer，可以直接在命令行执行 taos-explorer 或者使用下面的 systemctl 脚本用 systemctl 来启动 taosExplorer 服务

```shell
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

#### 问题排查

1. 当通过浏览器打开taosExplorer站点遇到“无法访问此网站”的错误信息时，请通过命令行登录taosExplorer所在机器，并使用命令systemctl status taos-explorer.service检查服务的状态，如果返回的状态是inactive，请使用命令systemctl start taos-explorer.service启动服务。
2. 如果需要获取taosExplorer的详细日志，可通过命令journalctl -u taos-explorer
