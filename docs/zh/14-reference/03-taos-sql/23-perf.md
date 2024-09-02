---
sidebar_label: 统计数据
title: 统计数据
description: Performance_Schema 数据库中存储了系统中的各种统计信息
---

TDengine 3.0 版本开始提供一个内置数据库 `performance_schema`，其中存储了与性能有关的统计数据。本节详细介绍其中的表和表结构。

## PERF_APP

提供接入集群的应用（客户端）的相关信息。也可以使用 SHOW APPS 来查询这些信息。

| #   |   **列名**   | **数据类型** | **说明**                        |
| --- | :----------: | ------------ | ------------------------------- |
| 1   |    app_id    | UBIGINT      | 客户端 ID                       |
| 2   |      ip      | BINARY(16)   | 客户端地址                      |
| 3   |     pid      | INT          | 客户端进程 号                   |
| 4   |     name     | BINARY(24)   | 客户端名称                      |
| 5   |  start_time  | TIMESTAMP    | 客户端启动时间                  |
| 6   |  insert_req  | UBIGINT      | insert 请求次数                 |
| 7   |  insert_row  | UBIGINT      | insert 插入行数                 |
| 8   | insert_time  | UBIGINT      | insert 请求的处理时间，单位微秒 |
| 9   | insert_bytes | UBIGINT      | insert 请求消息字节数           |
| 10  | fetch_bytes  | UBIGINT      | 查询结果字节数                  |
| 11  |  query_time  | UBIGINT      | 查询请求处理时间                |
| 12  |  slow_query  | UBIGINT      | 慢查询（处理时间 >= 3 秒）个数  |
| 13  |  total_req   | UBIGINT      | 总请求数                        |
| 14  | current_req  | UBIGINT      | 当前正在处理的请求个数          |
| 15  | last_access  | TIMESTAMP    | 最后更新时间                    |

## PERF_CONNECTIONS

数据库的连接的相关信息。也可以使用 SHOW CONNECTIONS 来查询这些信息。

| #   |  **列名**   | **数据类型** | **说明**                                           |
| --- | :---------: | ------------ | -------------------------------------------------- |
| 1   |   conn_id   | INT          | 连接 ID                                            |
| 2   |    user     | BINARY(24)   | 用户名                                             |
| 3   |     app     | BINARY(24)   | 客户端名称                                         |
| 4   |     pid     | UINT         | 发起此连接的客户端在自己所在服务器或主机上的进程号 |
| 5   |  end_point  | BINARY(128)  | 客户端地址                                         |
| 6   | login_time  | TIMESTAMP    | 登录时间                                           |
| 7   | last_access | TIMESTAMP    | 最后更新时间                                       |

## PERF_QUERIES

提供当前正在执行的 SQL 语句的信息。也可以使用 SHOW QUERIES 来查询这些信息。

| #   |   **列名**   | **数据类型** | **说明**                     |
| --- | :----------: | ------------ | ---------------------------- |
| 1   |   kill_id    | UBIGINT      | 用来停止查询的 ID            |
| 2   |   query_id   | INT          | 查询 ID                      |
| 3   |   conn_id    | UINT         | 连接 ID                      |
| 4   |     app      | BINARY(24)   | app 名称                     |
| 5   |     pid      | INT          | app 在自己所在主机上的进程号 |
| 6   |     user     | BINARY(24)   | 用户名                       |
| 7   |  end_point   | BINARY(16)   | 客户端地址                   |
| 8   | create_time  | TIMESTAMP    | 创建时间                     |
| 9   |  exec_usec   | BIGINT       | 已执行时间                   |
| 10  | stable_query | BOOL         | 是否是超级表查询             |
| 11  |   sub_num    | INT          | 子查询数量                   |
| 12  |  sub_status  | BINARY(1000) | 子查询状态                   |
| 13  |     sql      | BINARY(1024) | SQL 语句                     |

## PERF_CONSUMERS

| #   |    **列名**    | **数据类型** | **说明**                                                    |
| --- | :------------: | ------------ | ----------------------------------------------------------- |
| 1   |  consumer_id   | BIGINT       | 消费者的唯一 ID                                             |
| 2   | consumer_group | BINARY(192)  | 消费者组                                                    |
| 3   |   client_id    | BINARY(192)  | 用户自定义字符串，通过创建 consumer 时指定 client_id 来展示 |
| 4   |     status     | BINARY(20)   | 消费者当前状态。消费者状态包括：ready(正常可用)、 lost(连接已丢失)、 rebalancing(消费者所属 vgroup 正在分配中)、unknown(未知状态)|
| 5   |     topics     | BINARY(204)  | 被订阅的 topic。若订阅多个 topic，则展示为多行              |
| 6   |    up_time     | TIMESTAMP    | 第一次连接 taosd 的时间                                     |
| 7   | subscribe_time | TIMESTAMP    | 上一次发起订阅的时间                                        |
| 8   | rebalance_time | TIMESTAMP    | 上一次触发 rebalance 的时间                                 |

## PERF_TRANS

| #   |     **列名**     | **数据类型** | **说明**                                                       |
| --- | :--------------: | ------------ | -------------------------------------------------------------- |
| 1   |        id        | INT          | 正在进行的事务的编号                                           |
| 2   |   create_time    | TIMESTAMP    | 事务的创建时间                                                 |
| 3   |      stage       | BINARY(12)   | 事务的当前阶段，通常为 redoAction、undoAction、commit 三个阶段 |
| 4   |       db1        | BINARY(64)   | 与此事务存在冲突的数据库一的名称                               |
| 5   |       db2        | BINARY(64)   | 与此事务存在冲突的数据库二的名称                               |
| 6   |   failed_times   | INT          | 事务执行失败的总次数                                           |
| 7   |  last_exec_time  | TIMESTAMP    | 事务上次执行的时间                                             |
| 8   | last_action_info | BINARY(511)  | 事务上次执行失败的明细信息                                     |

## PERF_SMAS

| #   |  **列名**   | **数据类型** | **说明**                                    |
| --- | :---------: | ------------ | ------------------------------------------- |
| 1   |  sma_name   | BINARY(192)  | 时间维度的预计算 (time-range-wise sma) 名称 |
| 2   | create_time | TIMESTAMP    | sma 创建时间                                |
| 3   | stable_name | BINARY(192)  | sma 所属的超级表名称                        |
| 4   |  vgroup_id  | INT          | sma 专属的 vgroup 名称                      |
