---
sidebar_label: Parquet
title: Parquet
toc_max_heading_level: 4
---

Parquet Data Out 可以将一条只读 TDengine SQL 查询的结果导出为 taosX server 节点上的单个本地 Parquet 文件。输出路径由 taosX server 解释，不是浏览器本机路径。

> **注意：本功能仅适用于 TDengine 企业版。**

## 创建 Parquet 数据发布任务

在 Explorer 中进入“数据发布”页面，选择 Parquet 作为目标类型，并配置：

- `TDengine DSN`：TDengine 连接地址，例如 `taos+ws://root:taosdata@localhost:6041/db`。
- `SQL Query`：单条只读 `SELECT` 查询，支持 `WITH ... SELECT` 查询。
- `Output File`：taosX server 节点上的输出文件路径，必须以 `.parquet` 结尾。
- `Overwrite Existing File`：新的临时文件成功关闭后，是否允许替换已有最终文件。
- `Compression`：`uncompressed`、`zstd`、`snappy`、`gzip`、`brotli` 或 `lz4_raw`。
- `Compression Level`：仅 `zstd`、`gzip`、`brotli` 支持，可留空使用默认等级。
- `Row Group Size`：单个 Parquet row group 的最大行数，默认 131072。Parquet writer 只在 row group 满或文件 close 时落盘，调小该值可让 `.part` 文件更频繁地增长，但会降低压缩率并增加文件元数据开销。取值范围 1024 ~ 10000000。

## 输出路径

输出文件会创建在 taosX server 节点上，不是浏览器本机路径。

- 相对路径写入 `$DATA_DIR/tasks/<task_id>/<job_id>/`。
- 绝对路径按 taosX server 节点上的绝对路径使用。
- writer 会先在同目录创建 `<final_name>.part`，成功关闭 Parquet writer 并读取 metadata 后，再将临时文件重命名为最终路径。

Parquet Data Out 第一版不支持 agent 或 via 执行。

## DSN 示例

```text
FROM 'taos+ws://root:taosdata@localhost:6041/db?query=select%20*%20from%20meters'
TO 'parquet:/tmp/meters.parquet?overwrite=false&compression=zstd&row_group_size=131072'
```

## 限制

- 只允许单条只读 `SELECT` 查询。
- 不允许 `SHOW`、`DESCRIBE`、`DESC`，也不允许修改数据、表结构、会话或权限的语句。
- 不支持目录输出和多文件切分。
- 导出失败后会从头开始。
- 不支持向已有 Parquet 文件追加写入。
- 第一版任务页面不提供 Parquet 文件下载动作。
- 任务配置页不预估行数、文件大小或剩余时间。

## 性能和可观测性

大结果集导出可能长时间占用 taosX server 的磁盘、CPU 和网络资源。后端会流式读取 TDengine result block，并按 Arrow record batch 写入 Parquet writer。

任务 metrics 包含 Parquet 输出行数、batch 数、block 数、字节数、当前文件大小、耗时、查询耗时、写入耗时、关闭耗时和失败 batch 数。活动日志会记录查询开始、首个结果 block、进度、writer 关闭、完成、取消和失败事件。
