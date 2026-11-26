---
title: OPC DA CSV 配置文件参考
sidebar_label: CSV 参考
---

本文介绍 OPC DA 数据接入任务中，通过 CSV 文件配置点位的完整规则。CSV 文件用于批量配置点位到 TDengine 超级表、子表的映射关系。创建任务时上传 CSV 的方法见 [OPC-DA](./index.md#上传-csv-配置文件)。

## 文件编码

用户上传的 CSV 文件的编码格式必须为以下格式中的一种：

1. UTF-8 with BOM
2. UTF-8（即：UTF-8 without BOM）

## Header 配置规则

Header 是 CSV 文件的第一行，规则如下：

(1) CSV 的 Header 中可以配置以下列：

| 序号 | 列名 | 描述 | 是否必填 | 默认行为 |
|----|-------------------------|-------------------------------------------------------------------------------------------------------------------| --------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1 | tag_name | 数据点位在 OPC DA 服务器上的 id | 是 | 无 |
| 2 | stable | 数据点位在 TDengine 中对应的超级表 | 是 | 无 |
| 3 | tbname | 数据点位在 TDengine 中对应的子表 | 是 | 无 |
| 4 | enable | 是否采集该点位的数据 | 否 | 使用统一的默认值`1`作为 enable 的值 |
| 5 | value_col | 数据点位采集值在 TDengine 中对应的列名 | 否 | 使用统一的默认值`val` 作为 value_col 的值 |
| 6 | value_transform | 数据点位采集值在 taosX 中执行的变换函数 | 否 | 统一不进行采集值的 transform |
| 7 | type | 数据点位采集值的数据类型 | 否 | 统一使用采集值的原始类型作为 TDengine 中的数据类型 |
| 8 | quality_col | 数据点位采集值质量在 TDengine 中对应的列名 | 否 | 统一不在 TDengine 添加 quality 列 |
| 9 | ts_col | 数据点位的原始时间戳在 TDengine 中对应的时间戳列 | 否 | ts_col，request_ts，received_ts 这 3 列，当有 2 列以上存在时，以最左侧的列作为 TDengine 中的主键 |
| 10 | request_ts_col | 请求该点位采集值时的时间戳在 TDengine 中对应的时间戳列 | 否 | ts_col，request_ts，received_ts 这 3 列，当有 2 列以上存在时，以最左侧的列作为 TDengine 中的主键 |
| 11 | received_ts_col | 接收到该点位采集值时的时间戳在 TDengine 中对应的时间戳列 | 否 | ts_col，request_ts，received_ts 这 3 列，当有 2 列以上存在时，以最左侧的列作为 TDengine 中的主键 |
| 12 | ts_transform | 数据点位时间戳在 taosX 中执行的变换函数 | 否 | 统一不进行数据点位原始时间戳的 transform |
| 13 | request_ts_transform | 数据点位请求时间戳在 taosX 中执行的变换函数 | 否 | 统一不进行数据点位请求时间戳的 transform |
| 14 | received_ts_transform | 数据点位接收时间戳在 taosX 中执行的变换函数 | 否 | 统一不进行数据点位接收时间戳的 transform |
| 15 | tag::VARCHAR(200)::name | 数据点位在 TDengine 中对应的 Tag 列。其中 `tag` 为保留关键字，表示该列为一个 tag 列；`VARCHAR(200)` 表示该 tag 的类型，也可以是其它合法的类型；`name` 是该 tag 的实际名称。 | 否 | 配置 1 个以上的 tag 列，则使用配置的 tag 列；没有配置任何 tag 列，且 stable 在 TDengine 中存在，使用 TDengine 中的 stable 的 tag；没有配置任何 tag 列，且 stable 在 TDengine 中不存在，则默认自动添加以下 2 个 tag 列：`tag::VARCHAR(256)::point_id`、`tag::VARCHAR(256)::point_name` |

(2) CSV Header 中，不能有重复的列；

(3) CSV Header 中，类似 `tag::VARCHAR(200)::name` 这样的列可以配置多个，对应 TDengine 中的多个 Tag，但 Tag 的名称不能重复。

(4) CSV Header 中，列的顺序不影响 CSV 文件校验规则；

(5) CSV Header 中，可以配置不在上表中的列，例如：序号，这些列会被自动忽略。

## Row 配置规则

CSV 文件中的每个 Row 配置一个 OPC 数据点位。Row 的规则如下：

(1) 与 Header 中的列有如下对应关系

| 序号 | Header 中的列 | 值的类型 | 值的范围 | 是否必填 | 默认值 |
|----|-------------------------| --------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------|------|--------------|
| 1 | tag_name | String | 类似`root.parent.temperature`这样的字符串，要满足 OPC DA 的 ID 规范 | 是 | |
| 2 | enable | int | 0：不采集该点位，且在 OPC DataIn 任务开始前，删除 TDengine 中点位对应的子表；1：采集该点位，在 OPC DataIn 任务开始前，不删除子表。 | 否 | 1 |
| 3 | stable | String | 符合 TDengine 超级表命名规范的任何字符串；如果存在特殊字符`.`，使用下划线替换；如果存在`{type}`，则：CSV 文件的 type 不为空，使用 type 的值进行替换；CSV 文件的 type 为空，使用采集值的原始类型进行替换 | 是 | |
| 4 | tbname | String | 符合 TDengine 子表命名规范的任何字符串；如果存在特殊字符`.`，使用下划线替换；对于 OPC UA：如果存在`{ns}`，使用 point_id 中的 ns 替换；如果存在`{id}`，使用 point_id 中的 id 替换；对于 OPC DA：如果存在`{tag_name}`，使用 tag_name 替换 | 是 | |
| 5 | value_col | String | 符合 TDengine 命名规范的列名 | 否 | val |
| 6 | value_transform | String | 符合 Rhai 引擎的计算表达式，例如：`(val + 10) / 1000 * 2.0`，`log(val) + 10` 等 | 否 | None |
| 7 | type | String | 支持类型包括：`b`/`bool`、`i8`/`tinyint`、`i16`/`smallint`、`i32`/`int`、`i64`/`bigint`、`u8`/`tinyint unsigned`、`u16`/`smallint unsigned`、`u32`/`int unsigned`、`u64`/`bigint unsigned`、`f32`/`float`、`f64`/`double`、`timestamp`/`timestamp(ms)`/`timestamp(us)`/`timestamp(ns)`、`json` | 否 | 数据点位采集值的原始类型 |
| 8 | quality_col | String | 符合 TDengine 命名规范的列名 | 否 | None |
| 9 | ts_col | String | 符合 TDengine 命名规范的列名 | 否 | ts |
| 10 | request_ts_col | String | 符合 TDengine 命名规范的列名 | 否 | qts |
| 11 | received_ts_col | String | 符合 TDengine 命名规范的列名 | 否 | rts |
| 12 | ts_transform | String | 支持 +、-、*、/、% 操作符，例如：`ts / 1000* 1000`，将一个 ms 单位的时间戳的最后 3 位置为 0；`ts + 8 *3600* 1000`，将一个 ms 精度的时间戳，增加 8 小时；`ts - 8 *3600* 1000`，将一个 ms 精度的时间戳，减去 8 小时 | 否 | None |
| 13 | request_ts_transform | String | 支持 +、-、*、/、% 操作符，例如：`ts / 1000* 1000`，将一个 ms 单位的时间戳的最后 3 位置为 0；`qts + 8 *3600* 1000`，将一个 ms 精度的时间戳，增加 8 小时；`qts - 8 *3600* 1000`，将一个 ms 精度的时间戳，减去 8 小时 | 否 | None |
| 14 | received_ts_transform | String | 支持 +、-、*、/、% 操作符，例如：`ts / 1000* 1000`，将一个 ms 单位的时间戳的最后 3 位置为 0；`rts + 8 *3600* 1000`，将一个 ms 精度的时间戳，增加 8 小时；`rts - 8 *3600* 1000`，将一个 ms 精度的时间戳，减去 8 小时 | 否 | None |
| 15 | tag::VARCHAR(200)::name | String | tag 里的值，当 tag 的类型是 VARCHAR 时，可以是中文 | 否 | NULL |

(2) tag_name 在整个 DataIn 任务中是唯一的，即：在一个 OPC DataIn 任务中，一个数据点位只能被写入到 TDengine 的一张子表。如果需要将一个数据点位写入多张子表，需要建多个 OPC DataIn 任务；

(3) 当 tag_name 不同，但 tbname 相同时，value_col 必须不同。这种配置能够将不同数据类型的多个点位的数据写入同一张子表中不同的列。这种方式对应“OPC 数据入 TDengine 宽表”的使用场景。

## 其他规则

(1) 如果 Header 和 Row 的列数不一致，校验失败，提示用户不满足要求的行号；

(2) Header 在首行，且不能为空；

(3) Row 为 1 行以上；

## 相关文档

- [OPC-DA](./index.md)：创建 OPC DA 数据接入任务的完整步骤
- [OPC UA CSV 映射参考](../05-opcua/02-csv-reference.md)：UA 点位 CSV 的列定义（含 `{ns}`/`{id}` 等占位符）
