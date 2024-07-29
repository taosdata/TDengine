---
toc_max_heading_level: 4
title: "授权码管理"
sidebar_label: "授权码管理"
---

## 概述

- TDengine Enterprise 的授权，是通过对集群中的服务器设置授权码 (activeCode) 的方式完成的。
- 3.2.3.0 版本对授权机制进行了较大程度的重构。因此，本文以 3.2.3.0 版本为界，分别进行说明。

## 3.2.3.0 版本以前

### 授权码与授权项

- 授权码中，包含各个授权项的值。授权项中，常用的包括过期时间和测点数，也包括存储空间、数据库实例数、用户数、dnode 实例数、cpu 核数等。
- 授权码是以集群为单位生效的。如果集群中包含多个有效的授权码，则按较大值优先的原则对各个授权项取并集。


### 授权码获取

- 授权码由 TDengine  公司发放，需要客户提供：机器码 (machine code) 或集群 ID (cluster id)。
- 机器码的获取方式：在目标服务器上运行 `taosd -k`。集群中的每台服务器都对应一个机器码。

```shell
$ taosd -k
machine code: KGQ8Y+haR3iz4lHnX9gHngYl
```

- 集群 ID 的获取方式：在 taos 客户端执行 `show cluster\G;`,  其中，id 字段为集群 ID。集群中的所有服务器共享一个集群 ID。

```shell
taos> show cluster\G;
*************************** 1.row ***************************
         id: 3743835620574542136
       name: 324cbefb-e667-462f-9ebd-d9b0fc753bb3
     uptime: 0
create_time: 2023-08-31 10:37:36.767
    version: trial
expire_time: NULL
Query OK, 1 row(s) in set (0.001976s)
```

### 授权码激活

1. 支持通过 `taos.cfg` 配置文件和 SQL 命令 2 种激活方式，并支持通过 `show dnodes` 命令查看。
2. SQL 命令的优先级更高，如果同时采用了 2 种激活方式，`taos.cfg` 中的授权码会被忽略。
3. 推荐使用 SQL 命令激活，支持通过 `taos.cfg` 激活是为了兼容老版本。
4. 通过 SQL 命令激活的授权码会保存到集群中，因此，支持通过 `show dnodes` 命令查看。
5. 3.1.0.0 版本之前，通过 `taos.cfg` 激活的授权码，无法通过 `show dnodes` 命令查看。3.1.0.0 版本起，如果授权码是在 `taos.cfg` 中激活的， taosd 会自动读取 `taos.cfg` 中的授权码并保存到集群中并支持通过 `show dnodes` 命令查看。


#### 配置文件

- 使用配置文件激活授权码：在 `taos.cfg` 中以 activeCode 开头添加一行，空格后部分为授权码。添加后，重启 mnode leader 所在的 dnode 立即生效，或者不重启 1 分钟内生效。

```shell
activeCode z9sdqG8w67fqBXlHnxWAQezQc/mabvN9N2maa6ksK6JJWl7OxrPZ2ElaXs7Gs9nYSVpezsaz2di72ZL6EAo0mcYiPlK2dDdmAt3P46xKs4Q=
```
#### SQL 命令

- 使用 SQL 命令 `alter dnode` 激活授权码：在 taos 客户端执行，支持针对单个 dnode  或所有 dnode 设置。设置授权码一般在 5 秒内生效，如果集群节点较多，生效时间会长一些。
- 下述命令中，dnode 后边的 1 为 dnode id，可以通过在 taos 客户端执行 `show dnodes` 获取。

```shell
taos> alter dnode 1 'activeCode' 'tP+2soIXpPwxqdKIK2Vz80laXs7Gs9nYN2maa6ksK6JJWl7OxrPZ2ElaXs7Gs9nYSVpezsaz2djdUysgjtzivcYiPlK2dDdmABPzBHc7VCc=';
Query OK, 0 row(s) affected (0.003637s) 
```

- 针对集群中所有 dnode 设置相同的授权码。

```shell
taos> alter all dnodes 'activeCode' 'Wn8j+6KVVRnGIj5StnQ3Zs2XgtVr+h+Vue1VyrZhTL/HeS3wtya3rcYiPlK2dDdmYddxLWSWeUDGIj5StnQ3ZsYiPlK2dDdmrRsroQil5AE=';
```

### 授权码删除

- 以下两种方法均有效

```shell
taos> alter dnode 5 'activeCode' '';
taos> alter dnode 5 'activeCode';
taos> alter all dnodes 'activeCode' '';
taos> alter all dnodes 'activeCode';
```
### 授权码查看

- 通过在 taos 客户端执行 `show dnodes\G;` 查看。

```shell
taos> show dnodes\G;
*************************** 1.row ***************************
            id: 1
      endpoint: u3-31:6030
        vnodes: 0
support_vnodes: 17
        status: ready
   create_time: 2023-08-31 10:37:36.765
   reboot_time: 2023-08-31 10:37:36.754
          note: 
   active_code: Wn8j+6KVVRnGIj5StnQ3Zs2XgtVr+h+Vue1VyrZhTL/HeS3wtya3rcYiPlK2dDdmYddxLWSWeUDGIj5StnQ3ZsYiPlK2dDdmrRsroQil5AE=
 c_active_code: 
Query OK, 1 row(s) in set (0.003062s)
```

### 授权信息查看

- 通过在 taos 客户端执行 `show grants\G;` 查看。
- opc_da 与 mqtt 之间的授权项适用于 taosX 数据源导入。以 opc_da 为例，type 为数据源类型，number 为数据源连接数上限， speed 为数据源速度上限，expire 为 1970-01-01 与数据源过期时间之间的天数。

```shell
taos> show grants\G;
*************************** 1.row ***************************
    version: trial
expire_time: 2023-10-31 09:37:36
    expired: false
    storage: unlimited
 timeseries: unlimited
  databases: unlimited
      users: unlimited
   accounts: unlimited
     dnodes: unlimited
connections: unlimited
    streams: unlimited
  cpu_cores: unlimited
      speed: unlimited
  querytime: unlimited
     opc_da: {"type":"OPC_DA","number":1,"speed":-1,"expire":"19615"}
     opc_ua: {"type":"OPC_UA","number":1,"speed":-1,"expire":"19615"}
         pi: {"type":"Pi","number":1,"speed":-1,"expire":"19615"}
      kafka: {"type":"Kafka","number":1,"speed":-1,"expire":"19615"}
   influxdb: {"type":"InfluxDB","number":1,"speed":-1,"expire":"19615"}
       mqtt: {"type":"MQTT","number":1,"speed":-1,"expire":"19615"}
Query OK, 1 row(s) in set (0.003706s)
```

## 3.2.3.0 版本及以后

### 授权码与授权项

- 授权码中包含 1 个或多个授权项。授权项分为`基础授权项`和`可选功能授权项`。`基础授权项`包括：过期时间、测点数、dnode 数，CPU 核数；`可选功能授权项`包括：维保服务、数据订阅、流计算、视图、审计日志、多级存储、数据备份与恢复、CSV 导入和各个数据源导入等。
- 支持按功能授权，支持单次完整授权和多次叠加授权。集群首次授权时，授权码中必须包含`基础授权项`。


### 授权码获取

- 授权码由 TDengine 公司发放，需要客户提供：集群 ID (cluster id) 和机器码。
- 通过在 taos 客户端执行 `show cluster machines\G;` 获取集群 ID 和机器码：

```shell
taos> show cluster machines\G;
*************************** 1.row ***************************
     id: 6418372034255504533
     dnode_num：5
     machine: AylNik5f3er9a2a9dz08vwW6
Query OK, 1 row(s) in set (0.003007s)
```

### 授权码激活

- 通过在 taos 客户端执行 `alter cluster 'activeCode' '${activeCode}';` 激活授权码，一般在 5 秒内生效。

```shell
taos> alter cluster 'activeCode' 'kv36cnF9GF8Hofj4vUK5XNyDXwbLrKr8dCqLcpsU18HABQ8bxCFgXBxgpcXuqn2znf9gBksqh9c2'; 
Query OK, 0 row(s) affected (0.004947s)
```

### 授权码回收

- 支持通过在 taos 客户端执行 `alter cluster 'activeCode' 'revoked';` 手动回收授权码。
- 执行该命令后，无论完整授权还是叠加授权，授权码均在 7 天后过期。

```shell
taos> alter cluster 'activeCode' 'revoked';
Query OK, 0 row(s) affected (0.003184s)
```

### 授权日志查看

- 通过在 taos 客户端执行 `show grants logs\G;` 查看。
- 以下输出项中，多条记录以分号分隔，每一条记录内部以逗号分隔。
- state 为授权状态变化日志，包括`变化时间/变化原因/初始状态/当前状态`。
- active 为授权码激活日志，包括`激活时间/授权码摘要`。
- machine 为机器码信息，包括`初始上线时间/所属 dnodeId/机器码/机器码类型`。

```shell
taos> show grants logs\G;
*************************** 1.row ***************************
  state: 2024-03-11 10:12:27,init,ungranted,ungranted;2024-03-11 10:26:55,alter,ungranted,granted;2024-03-11 10:30:40,alter,granted,revoked;2024-03-11 10:32:34,alter,revoked,granted;2024-03-11 10:34:24,alter,granted,revoked
 active: 2024-03-11 10:26:55,2Wvuk96zR3YdesRwebfDwXDrFmiTwP;2024-03-11 10:32:34,kv36cnF9GF8Hofj4vUK5XNyDXwbLrK
machine: 2024-03-11 10:32:34,1,AylNik5f3er9a2a9dz08vwW6,3
Query OK, 1 row(s) in set (0.003374s)
```

### 授权信息查看

- 通过在 taos 客户端执行 `show grants\G;` 查看`基础授权信息`。

```shell
taos> show grants\G;
*************************** 1.row ***************************
     version: official
 expire_time: 2024-07-01 08:00:00
service_time: 2024-03-11 10:12:25
     expired: false
       state: granted
  timeseries: 0/999
      dnodes: 1/10
   cpu_cores: 12/12
Query OK, 1 row(s) in set (0.003374s)
```

- 通过在 taos 客户端执行 `show grants full;` 查看`可选功能授权信息`。
- stream 与 view 之间的授权项，支持设置过期时间（expire）和数量（limits。以 0/8 为例，0 表示授权项当前数量，8 表示授权项数量上限）。
- audit 与 backup_restore 之间的授权项，仅支持设置过期时间，不支持设置数量。
- opc_da 与 td3.0 之间的授权项适用于 taosX 数据源导入。以 opc_da 的 limits 为例，number 为数据源连接数上限， speed 为数据源速度上限，expire 为 1970-01-01 00:00:00 与数据源过期时间（expireTime）之间的秒数。

```shell
taos> show grants full;
   grant_name    |    display_name    |        expire        |                                       limits                                        |
====================================================================================================================================================
 stream          | stream             | 2024-03-21 10:12:25  | 0/8                                                                                 |
 subscription    | subscription       | 2024-03-21 10:12:25  | 0/8                                                                                 |
 view            | view               | 2024-03-21 10:12:25  | 0/8                                                                                 |
 audit           | audit              | 2024-03-21 10:12:25  |                                                                                     |
 csv             | csv                | 2024-03-21 10:12:25  |                                                                                     |
 storage         | multi_tier_storage | 2024-03-21 10:12:25  |                                                                                     |
 backup_restore  | backup_restore     | 2024-03-21 10:12:25  |                                                                                     |
 opc_da          | OPC_DA             | 2024-03-21 10:12:25  | {"number":1, "speed":-1, "expire":"1710987145", "expireTime":"2024-03-21 10:12:25"} |
 opc_ua          | OPC_UA             | 2024-03-21 10:12:25  | {"number":1, "speed":-1, "expire":"1710987145", "expireTime":"2024-03-21 10:12:25"} |
 pi              | Pi                 | 2024-03-21 10:12:25  | {"number":1, "speed":-1, "expire":"1710987145", "expireTime":"2024-03-21 10:12:25"} |
 kafka           | Kafka              | 2024-03-21 10:12:25  | {"number":1, "speed":-1, "expire":"1710987145", "expireTime":"2024-03-21 10:12:25"} |
 influxdb        | InfluxDB           | 2024-03-21 10:12:25  | {"number":1, "speed":-1, "expire":"1710987145", "expireTime":"2024-03-21 10:12:25"} |
 mqtt            | MQTT               | 2024-03-21 10:12:25  | {"number":1, "speed":-1, "expire":"1710987145", "expireTime":"2024-03-21 10:12:25"} |
 avevahistorian  | avevaHistorian     | 2024-03-21 10:12:25  | {"number":1, "speed":-1, "expire":"1710987145", "expireTime":"2024-03-21 10:12:25"} |
 opentsdb        | OpenTSDB           | 2024-03-21 10:12:25  | {"number":1, "speed":-1, "expire":"1710987145", "expireTime":"2024-03-21 10:12:25"} |
 td2.6           | TDengine2.6        | 2024-03-21 10:12:25  | {"number":1, "speed":-1, "expire":"1710987145", "expireTime":"2024-03-21 10:12:25"} |
 td3.0           | TDengine3.0        | 2024-03-21 10:12:25  | {"number":1, "speed":-1, "expire":"1710987145", "expireTime":"2024-03-21 10:12:25"} |
Query OK, 17 row(s) in set (0.003513s)
```
