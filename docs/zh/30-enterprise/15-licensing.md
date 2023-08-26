---
toc_max_heading_level: 4
title: "授权码管理"
sidebar_label: "授权码管理"
---

## 概述

TDengine Pro 的授权，是通过对集群中的服务器设置授权码 (activeCode) 的方式完成的。


## 授权码与授权项

- 授权码中，包含各个授权项的值。授权项中，常用的包括过期时间和测点数，也包括存储空间，数据库实例数，用户数，dnode 实例数，cpu 核数等。
- 授权码是以集群为单位生效的。如果集群中包含多个有效的授权码，则按较大值优先的原则对各个授权项取并集。


## 授权码生成与发放

- 授权码由 TDengine 交付团队负责生成，支持 2 种生成方式：基于机器码 (machine code) 或集群 Id (cluster id)。
- 机器码的获取方式：在目标服务器上运行 taosd -k。集群中的每台服务器都对应一个机器码。

```shell
$ taosd -k
machine code: KGQ8Y+haR3iz4lHnX9gHngYl
```

- 集群 Id 的获取方式：在 taos 客户端执行 show cluster\G;,  其中，id 字段为 集群 Id。集群中的所有服务器共享一个 集群 Id。

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

## 授权码设置与查看

1. 支持通过 taos.cfg 配置文件和 SQL 命令 2 种设置方式，并支持通过 show dnodes 命令查看。
2. SQL 命令的优先级更高，如果 2 种方式都设置，taos.cfg 中的授权码会被忽略。
3. 推荐使用 SQL 命令的设置方式，支持 taos.cfg 设置是为了兼容老版本。
4. 通过 SQL 命令设置的授权码会保存到集群中，因此，支持通过 show dnodes 命令查看。
5. 3.1.0.0 版本之前，taos.cfg 中设置的授权码，无法通过 show dnodes 命令查看。3.1.0.0 版本起，如果授权码是在 taos.cfg 中设置的， taosd 会自动读取 taos.cfg 中的授权码并保存到集群中并支持通过 show dnodes 命令查看。
6. 通过 taos.cfg 配置文件设置：在 taos.cfg 中以 activeCode 开头添加一行，空格后部分为授权码。添加后，重启 mnode leader 所在的 dnode 立即生效，或者不重启 5 分钟内生效。


### 配置文件

使用配置文件激活授权码的方式如下，在 `taos.cfg` 配置文件中。

```shell
activeCode z9sdqG8w67fqBXlHnxWAQezQc/mabvN9N2maa6ksK6JJWl7OxrPZ2ElaXs7Gs9nYSVpezsaz2di72ZL6EAo0mcYiPlK2dDdmAt3P46xKs4Q=
```
### SQL 命令

使用 SQL 命令 `alter dnode`` 激活授权码：在 taos 客户端执行，支持针对单个 dnode  或所有 dnode 设置。设置授权码一般在 5 秒内生效，如果集群节点较多，生效时间会长一些。

下述命令中，dnode 后边的 1 为 dnode id，可以通过在 taos 客户端执行 show dnodes 获取。

```shell
taos> alter dnode 1 'activeCode' 'tP+2soIXpPwxqdKIK2Vz80laXs7Gs9nYN2maa6ksK6JJWl7OxrPZ2ElaXs7Gs9nYSVpezsaz2djdUysgjtzivcYiPlK2dDdmABPzBHc7VCc=';
Query OK, 0 row(s) affected (0.003637s) 
```

针对集群中所有 dnode 设置相同的授权码。

```shell
taos> alter all dnodes 'activeCode' 'Wn8j+6KVVRnGIj5StnQ3Zs2XgtVr+h+Vue1VyrZhTL/HeS3wtya3rcYiPlK2dDdmYddxLWSWeUDGIj5StnQ3ZsYiPlK2dDdmrRsroQil5AE=';
```

### 删除授权码

以下两种方法均有效

```shell
taos> alter dnode 5 'activeCode' '';
taos> alter dnode 5 'activeCode';
taos> alter all dnodes 'activeCode';
```
### 查看授权码

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

### 查看集群授权状态

授权状态是以集群为单位的，通过在 taos 客户端执行 show grants\G; 查看。

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
