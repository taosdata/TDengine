---
toc_max_heading_level: 4
title: taosX
sidebar_label: taosX
---

## 简介

taosX 是用于数据接入，同步，备份和恢复的零代码平台。本节详细介绍 taosX 的命令行。

## 命令行参数说明

**注意：部分参数暂无法通过 explorer设置【见：其他参数说明】，之后会逐步开放） **

命令行执行示例：

```shell
taosx -f <from-DSN> -t <to-DSN> <其他参数>
```

以下参数说明及示例中若无特殊说明 `<content>` 的格式均为占位符，使用时需要使用实际参数进行替换。

## DSN (Data Source Name)

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

## 其它参数说明

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

