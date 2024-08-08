---
title: taosX 参考手册
sidebar_label: taosX
---

taosX 是 TDengine Enterprise 中的一个核心组件，提供零代码数据接入的能力，taosX 支持两种运行模式：服务模式和命令行模式。本节讲述如何以这两种方式使用 taosX。要想使用 taosX 需要先安装 TDengine Enterprise 安装包。

## 命令行模式

### 命令行格式

taosX 的命令行参数格式如下

```shell
taosx -f <from-DSN> -t <to-DSN> <其它参数>
```

taosX 的命令行参数分为三个主要部分：
- `-f` 指定数据源，即 Source DSN
- `-t` 指定写入目标，即Sink DSN
- 其它参数

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

### 其它参数

1. --jobs `<number>` 指定任务并发数，仅支持 tmq 任务
2. -v 用于指定 taosx 的日志级别，-v 表示启用 info 级别日志，-vv 对应 debug，-vvv 对应 trace 

### 使用举例

#### 用户及权限信息导入导出

从集群 A 导出用户名、密码、权限和白名单信息到集群 B：

```shell
taosx privileges -f "taos://root:taosdata@hostA:6030" \
  -t "taos+ws://root:password@hostB:6041"
```

从集群 A 导出用户名、密码、权限和白名单信息到 JSON 文件：

```shell
taosx privileges -f "taos+ws://root:taosdata@localhost:6041" \
  -o ./user-pass-privileges-backup.json
```

从导出的 JSON 文件中恢复到本机：

```shell
taosx privileges -i ./user-pass-privileges-backup.json -t "taos:///"
```

可用参数列表：

| 参数 | 说明 |
| ---- | ---- |
| -u | 包含用户基本信息（密码、是否启用等） |
| -p | 包含权限信息 |
| -w | 包含白名单信息 |

当 `-u`/`-p` 参数应用时，将仅包含指定的信息，不带参数时，表示所有信息（用户名、密码、权限和白名单）。

`-w` 参数不能单独使用，当其与 `-u` 一起使用时才有效（单独使用 `-u` 将不包含白名单）。

#### 从旧版本迁移数据

1. 同步历史数据

同步整个库：

```shell
taosx run -f 'taos://root:taosdata@localhost:6030/db1' -t 'taos:///db2' -v
```

同步指定超级表:

```shell
taosx run \
  -f 'taos://root:taosdata@localhost:6030/db1?stables=meters' \
  -t 'taos:///db2' -v
```

同步子表或普通表，支持 `{stable}.{table}` 指定超级表的子表，或直接指定表名 `{table}`

```shell
taosx run \
  -f 'taos://root:taosdata@localhost:6030/db1?tables=meters.d0,d1,table1' \
  -t 'taos:///db2' -v
```

2. 同步指定时间区间数据 (使用 RFC3339 时间格式，注意带时区 ):

```shell
taosx run -f 'taos:///db1?start=2022-10-10T00:00:00Z' -t 'taos:///db2' -v
```

3. 持续同步，`restro` 指定同步最近 5m 中的数据，并同步新的数据，示例中每 1s 检查一次，`excursion` 表示允许 500ms 的延时或乱序数据

```shell
taosx run \
  -f 'taos:///db1?mode=realtime&restro=5m&interval=1s&excursion=500ms' \
  -t 'taos:///db2' -v
```

4. 同步历史数据 + 实时数据：

```shell
taosx run -f 'taos:///db1?mode=all' -t 'taos:///db2' -v
```

5. 通过 --transform 或 -T 配置数据同步（仅支持 2.6 到 3.0 以及 3.0 之间同步）过程中对于表名及表字段的一些操作。暂无法通过 Explorer 进行设置。配置说明如下：
  
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

      3. 使用 CSV 映射文件进行表重命名：以下示例使用 map.csv 文件进行表重命名

      `-T rename-child-table:map:@./map.csv`
      
      CSV 文件 `./map.csv` 格式如下：

      name1,newname1
      name2,newname2
  ```

需要注意的是：当迁移两端版本不一致，且使用原生连接时，需要在 DSN 中指定 `libraryPath`，如： `taos:///db1?libraryPath=./libtaos.so`

#### 导入 CSV 文件数据

基本用法如下：

```shell
taosx run -f csv:./meters/meters.csv.gz \
  --parser '@./meters/meters.json' \
  -t taos:///csv1 -qq
```

以电表数据为例，CSV 文件如下：

```csv
tbname,ts,current,voltage,phase,groupid,location
d4,2017-07-14T10:40:00+08:00,-2.598076,16,-0.866025,7,California.LosAngles
d4,2017-07-14T10:40:00.001+08:00,-2.623859,6,-0.87462,7,California.LosAngles
d4,2017-07-14T10:40:00.002+08:00,-2.648843,2,-0.862948,7,California.LosAngles
d4,2017-07-14T10:40:00.003+08:00,-2.673019,16,-0.891006,7,California.LosAngles
d4,2017-07-14T10:40:00.004+08:00,-2.696382,10,-0.898794,7,California.LosAngles
d4,2017-07-14T10:40:00.005+08:00,-2.718924,6,-0.886308,7,California.LosAngles
d4,2017-07-14T10:40:00.006+08:00,-2.740636,10,-0.893545,7,California.LosAngles
```

`--parser` 用于设置入库参数，示例如下：

```json
{
  "parse": {
    "ts": { "as": "TIMESTAMP(ms)" },
    "current": { "as": "FLOAT" },
    "voltage": { "as": "INT" },
    "phase": { "as": "FLOAT" },
    "groupid": { "as": "INT" },
    "location": { "as": "VARCHAR(24)" }
  },
  "model": {
    "name": "${tbname}",
    "using": "meters",
    "tags": ["groupid", "location"],
    "columns": ["ts", "current", "voltage", "phase"]
  }
}
```

它将从 `./meters/meters.csv.gz`（一个gzip压缩的CSV文件）导入数据到超级表 `meters`，每一行都插入到指定的表名 - `${tbname}` 使用CSV内容中的 `tbname` 列作为表名（即在 JSON 解析器中的 `.model.name`）。

## 服务模式

本节讲述如何以服务模式部署 `taosX`。以服务模式运行的 taosX，其各项功能需要通过 taosExplorer 上的图形界面来使用。 

### 配置

`taosX` 支持通过配置文件进行配置。在 Linux 上，默认配置文件路径是 `/etc/taos/taosx.toml`，在 Windows 上，默认配置文件路径是 `C:\\TDengine\\cfg\\taosx.toml`，包含以下配置项：

- `plugins_home`：外部数据源连接器所在目录。
- `data_dir`：数据文件存放目录。
- `logs_home`：日志文件存放目录，`taosX` 日志文件的前缀为 `taosx.log`，外部数据源有自己的日志文件名前缀。
- `log_level`：日志等级，可选级别包括 `error`、`warn`、`info`、`debug`、`trace`，默认值为 `info`。
- `log_keep_days`：日志的最大存储天数，`taosX` 日志将按天划分为不同的文件。
- `jobs`：每个运行时的最大线程数。在服务模式下，线程总数为 `jobs*2`，默认线程数为`当前服务器内核*2`。
- `serve.listen`：是 `taosX` REST API 监听地址，默认值为 `0.0.0.0:6050`。
- `serve.database_url`：`taosX` 数据库的地址，格式为 `sqlite:<path>`。
- `monitor.fqdn`：`taosKeeper` 服务的 FQDN，没有默认值，置空则关闭监控功能。
- `monitor.port`：`taosKeeper` 服务的端口，默认`6043`。
- `monitor.interval`：向 `taosKeeper` 发送指标的频率，默认为每 10 秒一次，只有 1 到 10 之间的值才有效。

如下所示：

```toml
# plugins home
#plugins_home = "/usr/local/taos/plugins" # on linux/macOS
#plugins_home = "C:\\TDengine\\plugins" # on windows

# data dir
#data_dir = "/var/lib/taos/taosx" # on linux/macOS
#data_dir = "C:\\TDengine\\data\\taosx" # on windows

# logs home
#logs_home = "/var/log/taos" # on linux/macOS
#logs_home = "C:\\TDengine\\log" # on windows

# log level: off/error/warn/info/debug/trace
#log_level = "info"

# log keep days
#log_keep_days = 30

# number of jobs, default to 0, will use `jobs` number of works for TMQ
#jobs = 0

[serve]
# listen to ip:port address
#listen = "0.0.0.0:6050"

# database url
#database_url = "sqlite:taosx.db"

[monitor]
# FQDN of taosKeeper service, no default value
#fqdn = "localhost"
# port of taosKeeper service, default 6043
#port = 6043
# how often to send metrics to taosKeeper, default every 10 seconds. Only value from 1 to 10 is valid.
#interval = 10
```

### 启动

Linux 系统上 `taosX` 可以通过 Systemd 命令启动：

```shell
systemctl start taosx
```

Windows 系统上通过系统管理工具 "Services" 找到 `taosX` 服务，然后启动它，或者在命令行工具（cmd.exe 或 PowerShell）中执行以下命令启动：

```shell
sc.exe start taosx
```

### 问题排查

1. 修改 `taosX` 日志级别

`taosX` 的默认日志级别为 `info`，要指定不同的级别，请修改配置文件，或使用以下命令行参数：
- `error`：`taosx serve -qq`
- `debug`：`taosx serve -q`
- `info`：`taosx serve -v`
- `debug`：`taosx serve -vv`
- `trace`：`taosx serve -vvv`

要在 `taosX` 作为服务运行时指定命令行参数，请参阅配置。

2. 查看 `taosX` 日志

您可以查看日志文件或使用 `journalctl` 命令来查看 `taosX` 的日志。

Linux 下 `journalctl` 查看日志的命令如下：

```bash
journalctl -u taosx [-f]
```