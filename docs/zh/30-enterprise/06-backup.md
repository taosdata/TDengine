---
toc_max_heading_level: 4
title: 数据备份和恢复
---

本节讲述如何使用 taosX 的命令行将 TDengine 集群中的数据备份到本地文件以及如何从一个备份出的本地文件恢复数据到 TDengine 集群中。您也可以使用 taos-explorer 的可视化界面进行数据备份和恢复，具体请参考[可视化管理](../explorer)。

## 从 TDengine 备份数据文件到本地

### 参数列表

详细的参数列表请参考 [参数列表](../taosX)

### 示例：
```shell
taosx run -f 'tmq://root:taosdata@td1:6030/db1' -t 'local:/path_directory/'

```
以上示例执行的结果及参数说明：

将集群 td1 中的数据库 db1 的所有数据，备份到 taosx 所在设备的 /path_directory 路径下。

数据源(-f 参数的 DSN)的 object 支持配置为 数据库级(dbname)、超级表级(dbname.stablename)、子表/普通表级(dbname.tablename)，对应备份数据的级别数据库级、超级表级、子表/普通表级


## 从本地数据文件恢复到 TDengine

### 参数列表

详细的参数列表请参考 [参数列表](../taosX)


### 示例
```shell
taosx run -f 'local:/path_directory/' -t 'taos://root:taosdata@td2:6030/db1?assert'
```

以上示例执行的结果：

将 taosx 所在设备 /path_directory 路径下已备份的数据文件，恢复到集群 td2 的数据库 db1 中，如果 db1 不存在，则自动建库。

目标源(-t 参数的 DSN)中的 object 支持配置为数据库(dbname)、超级表(dbname.stablename)、子表/普通表(dbname.tablename)，对应备份数据的级别数据库级、超级表级、子表/普通表级，前提是备份的数据文件也是对应的数据库级、超级表级、子表/普通表级数据。


## 常见错误排查

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