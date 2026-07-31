---
sidebar_label: 性能数据视图
title: 性能数据视图
description: PERFORMANCE_SCHEMA 中与性能相关的统计视图说明
---

自 `v3.0.0.0` 起，TDengine 提供内置数据库 `PERFORMANCE_SCHEMA`，用于存放与性能相关的统计数据。下文说明其中各表及列结构（与当前版本系统表定义一致）。

更完整的元数据与状态信息也可通过 `INFORMATION_SCHEMA`（见 [元数据视图](./01-meta.md)）与对应 [SHOW 命令](./03-show.md) 查看。

## PERF_APPS

提供接入集群的应用（客户端）的写入/查询统计、慢查询与最近访问时间等信息。也可以使用 [`SHOW APPS`](./03-show.md#show-apps) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | -------------- | ----------- | --- |
| 1   | `app_id`       | UBIGINT     | 客户端 ID |
| 2   | `ip`           | VARCHAR(16) | 客户端地址 |
| 3   | `pid`          | INT         | 客户端进程号 |
| 4   | `name`         | VARCHAR(24) | 客户端名称 |
| 5   | `start_time`   | TIMESTAMP   | 客户端启动时间 |
| 6   | `insert_req`   | UBIGINT     | `INSERT` 请求次数 |
| 7   | `insert_row`   | UBIGINT     | `INSERT` 插入行数 |
| 8   | `insert_time`  | UBIGINT     | `INSERT` 请求处理时间，单位微秒 |
| 9   | `insert_bytes` | UBIGINT     | `INSERT` 请求消息字节数 |
| 10  | `fetch_bytes`  | UBIGINT     | 查询结果字节数 |
| 11  | `query_time`   | UBIGINT     | 查询请求处理时间 |
| 12  | `slow_query`   | UBIGINT     | 慢查询个数（处理时间 ≥ 3 秒） |
| 13  | `total_req`    | UBIGINT     | 总请求数 |
| 14  | `current_req`  | UBIGINT     | 当前正在处理的请求个数 |
| 15  | `last_access`  | TIMESTAMP   | 最后更新时间 |

## PERF_CONNECTIONS

提供当前数据库连接的用户、客户端、登录时间、连接类型与令牌等信息。也可以使用 [`SHOW CONNECTIONS`](./03-show.md#show-connections) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ---------------- | ----------- | --- |
| 1   | `conn_id`        | UINT        | 连接 ID |
| 2   | `user`           | BINARY(24)  | 用户名。关键字列，查询时需用反引号转义（如 `` `user` ``） |
| 3   | `app`            | BINARY(24)  | 客户端名称 |
| 4   | `pid`            | UINT        | 发起该连接的客户端进程号 |
| 5   | `end_point`      | BINARY(134) | 客户端地址 |
| 6   | `login_time`     | TIMESTAMP   | 登录时间 |
| 7   | `last_access`    | TIMESTAMP   | 最后更新时间 |
| 8   | `user_app`       | BINARY(24)  | 用户侧应用名 |
| 9   | `user_ip`        | VARCHAR(22) | 用户侧 IP |
| 10  | `native_version` | BINARY(32)  | 原生客户端版本 |
| 11  | `connector_info` | BINARY(256)      | 连接器信息 |
| 12  | `type`           | BINARY(16)  | 连接类型 |
| 13  | `token`          | BINARY(32)      | 令牌名称（若使用令牌登录） |

## PERF_CONSUMERS

提供数据订阅消费者的组、状态、已订主题、参数与最近 poll 时间等信息。也可以使用 [`SHOW CONSUMERS`](./03-show.md#show-consumers) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ---------------- | ----------- | --- |
| 1   | `consumer_id`    | BINARY(32)  | 消费者唯一 ID |
| 2   | `consumer_group` | BINARY(193) | 消费者组 |
| 3   | `client_id`      | BINARY(256) | 创建 consumer 时指定的客户端标识 |
| 4   | `user`           | BINARY(24)  | 用户名 |
| 5   | `fqdn`           | BINARY(128) | 消费者所在机器 FQDN |
| 6   | `status`         | BINARY(20)  | 当前状态：`ready`（可用）、`lost`（连接丢失）、`rebalancing`（所属 `vgroup` 分配中）、`unknown`（未知） |
| 7   | `topics`         | BINARY(205) | 已订阅主题；订阅多个主题时展示为多行 |
| 8   | `end_point`      | VARCHAR(22) | end_point |
| 9   | `up_time`        | TIMESTAMP   | 首次连接 `taosd` 的时间 |
| 10  | `subscribe_time` | TIMESTAMP   | 最近一次发起订阅的时间 |
| 11  | `rebalance_time` | TIMESTAMP   | 最近一次触发 rebalance 的时间 |
| 12  | `parameters`     | BINARY(192) | 订阅参数 |
| 13  | `poll_time`      | TIMESTAMP   | 最近一次 poll 时间 |

## PERF_INSTANCES

提供接入集群的实例注册信息，包括类型、描述与注册/过期时间等。也可以使用 [`SHOW INSTANCES`](./03-show.md#show-instances) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ---------------- | ------------ | --- |
| 1   | `id`             | VARCHAR(257) | ID |
| 2   | `type`           | VARCHAR(66)  | 类型 |
| 3   | `desc`           | VARCHAR(514) | 实例描述 |
| 4   | `first_reg_time` | TIMESTAMP    | 首次注册时间 |
| 5   | `last_reg_time`  | TIMESTAMP    | 最近注册时间 |
| 6   | `expire`         | INT          | 过期时间（秒） |

## PERF_QUERIES

提供当前正在执行的查询的标识、耗时、阶段状态与 SQL 文本等信息。也可以使用 [`SHOW QUERIES`](./03-show.md#show-queries) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------------ | ------------- | --- |
| 1   | `kill_id`          | VARCHAR(26)   | 用于 `KILL QUERY` 的 ID |
| 2   | `query_id`         | UBIGINT       | 查询 ID |
| 3   | `conn_id`          | UINT          | 连接 ID |
| 4   | `app`              | VARCHAR(24)   | 应用名称 |
| 5   | `pid`              | INT           | 应用所在主机上的进程号 |
| 6   | `user`             | VARCHAR(24)   | 用户名 |
| 7   | `end_point`        | VARCHAR(22)   | 客户端地址 |
| 8   | `create_time`      | TIMESTAMP     | 创建时间 |
| 9   | `exec_usec`        | BIGINT        | 已执行时间（微秒） |
| 10  | `stable_query`     | BOOL          | 是否为超级表查询 |
| 11  | `sub_query`        | BOOL          | 是否为子查询 |
| 12  | `sub_num`          | INT           | 子查询数量 |
| 13  | `sub_status`       | VARCHAR(1000) | 子查询状态（含子查询 ID、状态及该状态开始时间） |
| 14  | `sql`              | VARCHAR(2048) | SQL 语句。关键字列，查询时需用反引号转义 |
| 15  | `user_app`         | VARCHAR(24)   | 用户侧应用名 |
| 16  | `user_ip`          | VARCHAR(22)   | 用户侧 IP |
| 17  | `phase_state`      | VARCHAR(64)   | 查询当前阶段 / 状态 |
| 18  | `phase_start_time` | TIMESTAMP     | 当前阶段开始时间 |

## PERF_TRANS

提供当前正在执行的元数据事务的阶段、操作对象、失败次数与最近执行信息等。也可以使用 [`SHOW TRANSACTIONS`](./03-show.md#show-transactions) 查询。

| #   | **列名** | **数据类型** | **说明** |
| --- | ------------------ | ------------ | --- |
| 1   | `id`               | BIGINT       | 事务编号 |
| 2   | `create_time`      | TIMESTAMP    | 创建时间 |
| 3   | `stage`            | VARCHAR(12)  | 当前阶段（如 `redoAction`、`undoAction`、`commit`） |
| 4   | `oper`             | VARCHAR(22)  | 操作者 |
| 5   | `db`               | VARCHAR(64)  | 相关数据库 |
| 6   | `stable`           | VARCHAR(192) | 相关超级表 |
| 7   | `killable`         | VARCHAR(10)  | 是否可终止 |
| 8   | `failed_times`     | INT          | 执行失败总次数 |
| 9   | `last_exec_time`   | TIMESTAMP    | 上次执行时间 |
| 10  | `last_action_info` | VARCHAR(511) | 上次执行失败明细 |
| 11  | `type`             | VARCHAR(10)  | 事务类型 |
