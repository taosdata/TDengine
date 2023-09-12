---
toc_max_heading_level: 4
title: 数据同步
---

## Introduction

本节讲述如何使用 taosX 的命令行在 TDengine 集群之间同步数据。数据同步功能的目标端必须是 TDengine 3.0，源端可以是 TDengine 3.0 或 2.6 。对于 taosX 的命令行参数解析，请参考 [taosX](../../reference/taosx)。您也可以使用 taos-explorer 的可视化界面进行数据同步，具体请参考[可视化管理](../explorer)。服务安装与部署请参考 [安装与部署](../../get-started)。


## TDengine 3.0 -> TDengine 3.0

在两个相同版本 （都是 3.0.x.y）的 TDengine 集群之间将源集群中的存量及增量数据同步到目标集群中。

### 命令行模式下支持的参数如下：

| 参数名称  | 说明                                                             | 默认值                     |
|-----------|------------------------------------------------------------------|----------------------------|
| group.id  | 订阅使用的分组ID                                                 | 若为空则使用 hash 生成一个 |
| client.id | 订阅使用的客户端ID                                               | taosx                      |
| timeout   | 监听数据的超时时间，当设置为 never 表示 taosx 不会停止持续监听。 | 500ms                      |
| offset    | 从指定的 offset 开始订阅，格式为 `<vgroup_id>:<offset>`，若有多个 vgroup 则用半角逗号隔开 | 若为空则从 0 开始订阅  |
| token     | 目标源参数。 认证使用参数。                              | 无                                     |

### Examples

```shell
taosx run \
  -f 'tmq://root:taosdata@localhost:6030/db1?group.id=taosx1&client.id=taosx&timeout=never&offset=2:10' \
  -t 'taos://root:taosdata@another.com:6030/db2'
```



## TDengine 2.6 -> TDengine 3.0

将 2.6 版本 TDengine 集群中的数据迁移到 3.0 版本 TDengine 集群。

### 命令行参数

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

### Examples

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