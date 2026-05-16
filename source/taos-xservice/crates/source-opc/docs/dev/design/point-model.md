# PointModelConfig — 点位数据到 TDengine 的映射模型

> 源码位置：`taosx-core/src/plugins/sink/point/model.rs`

## 概述

`PointModelConfig` 是 OPC 类数据源（OPC UA、OPC DA、KingHistorian、Pspace）与 TDengine 之间的**核心映射模型**。它定义了数据源中的"点位"如何映射为 TDengine 中的超级表（STable）、子表（Child Table）、列（Column）和标签（Tag）。

在 Point 模式下，每个点位（Point）是一个时间序列变量，其数据结构统一为：**时间戳 + 值 + 质量码**。这里的"点位"不仅限于 OPC 协议——OPC UA、OPC DA、KingHistorian、Pspace 等数据源的点位数据都通过 Point Message 统一处理。`PointModelConfig` 解决的核心问题是：**如何将成千上万个这样的点位，高效地组织到 TDengine 的超级表/子表体系中**。

## 模型结构

```rust
pub struct PointModelConfig {
    pub source_type: SourceType,
    pub update_mode: Option<UpdateMode>,
    pub generate_rule: Option<GeneratePointMappingBy>,
    pub point_config_map: LinkedHashMap<String, PointConfig>,   // key: point_id
    pub table_config_map: LinkedHashMap<String, TableConfig>,   // key: point_id
    pub node_config_map: Option<LinkedHashMap<String, ObjectNodeConfig>>,
}
```

### 各字段职责

| 字段               | 说明                                                                         |
| ------------------ | ---------------------------------------------------------------------------- |
| `source_type`      | 数据源类型：`OPCUA` / `OPCDA` / `KingHistorian` / `Pspace`                   |
| `update_mode`      | 动态点位更新模式：`None`（不更新）/ `Append`（仅新增）/ `Update`（全量覆盖） |
| `generate_rule`    | 映射规则的生成方式：`Rule`（用户配置的表达式规则）或 `Csv`（CSV 文件定义）   |
| `point_config_map` | 每个点位 → TDengine 子表/超级表的映射，key 是 point_id                       |
| `table_config_map` | 每个点位 → TDengine 列和标签结构的映射，key 是 point_id                      |
| `node_config_map`  | OPC Object Node（非变量节点）的配置，用于创建 `opc_object` 超级表            |

## 映射关系总览

```
OPC 数据源                          TDengine
─────────────                       ────────

点位数据类型                        超级表 (STable)
  例: Float, Int32, String    ──→     opc_float, opc_int32, opc_varchar
                                      kinghist_float, pspace_int32 ...

单个点位                            子表 (Child Table)
  例: ns=3;s=温度传感器.Tag1  ──→     t_3_温度传感器_Tag1

点位的采集值                        列 (Column)
  时间戳                      ──→     ts (TIMESTAMP)        [主键]
  值                          ──→     val ({type})           [动态类型]
  质量码                      ──→     quality (INT)

点位的元信息                        标签 (Tag)
  point_id                    ──→     point_id (VARCHAR)
  point_name                  ──→     point_name (VARCHAR)
  自定义标签                  ──→     用户定义 (VARCHAR/NCHAR/...)
```

## 两种映射模式

### 模式一：选择数据点位（ByCommand）— 表达式规则

用户在 Explorer UI 中配置表达式规则，由 `PointMappingRule` 驱动映射。

#### PointMappingRule 结构

```rust
pub struct PointMappingRule {
    pub source_type: SourceType,
    pub stable_expression: String,       // 超级表名表达式，如 "opc_{type}"
    pub tbname_expression: String,       // 子表名表达式，如 "t_{ns}_{id}"
    pub value_col: String,               // 值列名，默认 "val"
    pub value_transform: Option<String>, // 值转换表达式（Rhai 语法）
    pub quality_col: String,             // 质量码列名，默认 "quality"
    pub primary_key: String,             // 主键列：original_ts / request_ts / received_ts
    pub primary_key_alias: String,       // 主键列在 TDengine 中的名称，默认 "ts"
    pub custom_tags: Option<Vec<CustomTag>>,  // 自定义标签列表
}
```

#### DSN 参数对应

| DSN 参数                  | PointMappingRule 字段 | 说明                                              |
| ------------------------- | --------------------- | ------------------------------------------------- |
| `super_table_expression`  | `stable_expression`   | 超级表名表达式                                    |
| `child_table_expression`  | `tbname_expression`   | 子表名表达式                                      |
| `value_col`               | `value_col`           | 值列名                                            |
| `value_transform`         | `value_transform`     | 值转换表达式                                      |
| `quality_col`             | `quality_col`         | 质量码列名                                        |
| `table_primary_key`       | `primary_key`         | 主键列选择                                        |
| `table_primary_key_alias` | `primary_key_alias`   | 主键列别名                                        |
| `custom_tags`             | `custom_tags`         | 自定义标签，格式: `{type}::{name}::{pattern};...` |

### 模式二：上传 CSV 配置文件（ByCsv）

用户上传 CSV 文件，逐行定义每个点位的映射。CSV 列如下：

| CSV 列名                 | 必填 | 说明                                                                   |
| ------------------------ | ---- | ---------------------------------------------------------------------- |
| `point_id` / `tag_name`  | 是   | 点位唯一标识                                                           |
| `enabled`                | 否   | 是否启用：1（启用）/ 0（禁用），默认 1                                 |
| `stable`                 | 否   | 超级表名，支持 `{type}` 占位符                                         |
| `tbname`                 | 是   | 子表名，支持模板占位符                                                 |
| `type`                   | 否   | 值列数据类型（如 `float`、`int`、`varchar(64)`）。为空时运行时动态推断 |
| `value_col`              | 否   | 值列名及别名                                                           |
| `quality_col`            | 否   | 质量码列名及别名                                                       |
| `ts_col`                 | 否   | 原始时间戳列名及别名                                                   |
| `request_ts_col`         | 否   | 请求时间戳列名及别名                                                   |
| `received_ts_col`        | 否   | 接收时间戳列名及别名                                                   |
| `value_transform`        | 否   | 值转换表达式                                                           |
| `ts_transform`           | 否   | 时间戳转换表达式                                                       |
| `{tag_name}::{tag_type}` | 否   | 自定义标签列（列名即 tag 定义）                                        |

## 超级表命名规则

### 表达式模板

超级表名由 `stable_expression` 参数决定，其中 `{type}` 占位符在运行时被替换为点位的数据类型名。

**替换规则**（`generate_stable_from_pattern`）：

| 点位数据类型 | `{type}` 替换为 | 超级表名示例（表达式 `opc_{type}`） |
| ------------ | --------------- | ----------------------------------- |
| `BOOL`       | `bool`          | `opc_bool`                          |
| `INT`        | `int`           | `opc_int`                           |
| `FLOAT`      | `float`         | `opc_float`                         |
| `DOUBLE`     | `double`        | `opc_double`                        |
| `TIMESTAMP`  | `timestamp`     | `opc_timestamp`                     |
| `VARCHAR(n)` | `varchar`       | `opc_varchar`                       |
| `NCHAR(n)`   | `nchar`         | `opc_nchar`                         |

**各数据源的默认前缀**：

| 数据源        | 默认 `stable_expression` | 超级表名示例   |
| ------------- | ------------------------ | -------------- |
| OPC UA        | `opc_{type}`             | `opc_float`    |
| OPC DA        | `opc_{type}`             | `opc_double`   |
| KingHistorian | `kinghist_{type}`        | `kinghist_int` |
| Pspace        | `pspace_{type}`          | `pspace_float` |

### 超级表结构

所有 Variable Node 的超级表具有相同的结构模式：

```sql
CREATE TABLE IF NOT EXISTS `{stable}` (
    `{primary_key_alias}` TIMESTAMP,    -- 主键，默认 ts
    `{value_col}` {type},               -- 值列，类型动态确定
    `{quality_col}` INT                 -- 质量码
) TAGS (
    `{custom_tag_1}` {tag_type_1},      -- 用户自定义标签
    `{custom_tag_2}` {tag_type_2},
    ...
)
```

当 `type` 未知（动态推断模式）时，超级表在 `handle_point_message_init` 阶段**不会**被预创建，而是在 `consume_point_record` 写入时根据实际数据类型自动创建。

## 子表命名规则

### 表达式模板

子表名由 `tbname_expression`（选择数据点位模式）或 CSV 的 `tbname` 列决定。

**`generate_tbname_from_pattern` 函数支持的占位符**：

#### OPC UA（`source_type = OPCUA`）

OPC UA 的 point_id 格式为 `ns={namespace};{type}={identifier}`，例如 `ns=3;s=温度.Tag1`。

| 占位符    | 说明                         | 示例 point_id: `ns=6;s=Device/Temp` |
| --------- | ---------------------------- | ----------------------------------- |
| `{ns}`    | 命名空间编号                 | `6`                                 |
| `{id}`    | 标识符值（去掉 `s=` 等前缀） | `Device/Temp`                       |
| `{id#/_}` | id 中 `/` → `_`，去首尾 `_`  | `Device_Temp`                       |
| `{id#-_}` | id 中 `-` → `_`，去首尾 `_`  | 原样（无 `-`）                      |

**常见子表名模板**：

| 模板             | 生成示例（`ns=3;s=Block.Tag1`）        |
| ---------------- | -------------------------------------- |
| `t_{ns}_{id}`    | `t_3_Block_Tag1`（`.` 最终替换为 `_`） |
| `t_{ns}_{id#/_}` | `t_3_Block_Tag1`                       |

#### OPC DA / KingHistorian（`source_type = OPCDA / KingHistorian`）

OPC DA 的 point_id 格式通常为 `Device.Group.TagName`。

| 占位符                     | 说明                    | 示例 point_id: `Device.Group.TagName` |
| -------------------------- | ----------------------- | ------------------------------------- |
| `{tag_name}` / `{TagName}` | 最后一个 `.` 之后的部分 | `TagName`                             |
| `{/tag_name}`              | 最后一个 `/` 之后的部分 | 完整 point_id（无 `/`）               |
| `{id}`                     | 完整 point_id           | `Device.Group.TagName`                |
| `{_id}` / `{id#/_}`        | point*id 中 `/` → `*`   | `Device.Group.TagName`                |

**常见子表名模板**：`t_{tag_name}` → `t_TagName`

#### Pspace

| 占位符       | 说明          |
| ------------ | ------------- |
| `{point_id}` | 完整 point_id |

**常见子表名模板**：`t_{point_id}`

#### 通用后处理

所有生成的子表名最终会将 `.` 和 `` ` `` 替换为 `_`，以满足 TDengine 表名规范。

### 子表创建 SQL

子表使用超级表创建，支持批量创建（上限 1 MiB）：

```sql
CREATE TABLE
    IF NOT EXISTS `t_3_Tag1` USING `opc_float` (`tag1`, `tag2`) TAGS('value1', 'value2')
    IF NOT EXISTS `t_3_Tag2` USING `opc_float` (`tag1`, `tag2`) TAGS('value3', 'value4')
    ...
```

## Tag 值生成规则

### 内置 Tag

在"选择数据点位"模式下，`consume_point_record` 写入时会自动为每条插入附加 `point_id` 和 `point_name` 两个内置 Tag。

### 自定义 Tag（CustomTag）

自定义 Tag 的详细设计（DSN 配置格式、占位符参考、OPC Node 属性占位符等）请参阅 [`custom-tags.md`](./custom-tags.md)。

## Column 映射详解

### ColumnConfig 结构

```rust
pub struct ColumnConfig {
    pub name: String,            // 语义名：original_ts / received_ts / value / quality / request_ts
    pub r#type: Option<Ty>,      // TDengine 列类型
    pub alias: Option<String>,   // TDengine 中的实际列名
    pub transform: Option<String>, // 转换表达式（Rhai 语法）
    pub is_primary_key: bool,    // 是否为主键列
}
```

### 语义列与 TDengine 列的对应

| 语义名        | IPC 数据列 | 默认 TDengine 列名 | TDengine 类型 | 说明                                 |
| ------------- | ---------- | ------------------ | ------------- | ------------------------------------ |
| `original_ts` | `ts`       | `ts`               | TIMESTAMP     | OPC Server 的采集时间戳              |
| `request_ts`  | `request`  | 用户定义           | TIMESTAMP     | 查询点位值的发起时间                 |
| `received_ts` | `received` | 用户定义           | TIMESTAMP     | 查询点位值的接收时间                 |
| `value`       | `value`    | `val`              | 动态          | 点位的采集值，类型由点位数据类型决定 |
| `quality`     | `status`   | `quality`          | INT           | OPC 数据质量码                       |

**主键列选择**：用户可通过 `table_primary_key` 参数选择 `original_ts`、`request_ts` 或 `received_ts` 之一作为主键。默认为 `original_ts`。

**列名自定义**：`alias` 字段允许用户重命名 TDengine 中的列名。例如将 `value` 的别名设为 `temperature`。

**值转换**：`transform` 字段支持 Rhai 表达式，对原始值进行运算转换后再写入。例如 `value * 0.01` 或 `if value > 100 { 100 } else { value }`。

## Object Node 映射

OPC 协议中除了 Variable Node（数据点位），还有 Object Node（如设备、文件夹等结构节点）。这些节点没有采集值，但携带元数据。

### 固定超级表

```sql
CREATE STABLE IF NOT EXISTS opc_object(
    ts TIMESTAMP,
    _null INT
) TAGS(
    name VARCHAR(1024),
    `BrowseName` VARCHAR(1024),
    `DisplayName` VARCHAR(1024),
    `Description` VARCHAR(1024),
    `Path` VARCHAR(1024)
)
```

### 子表命名

| 数据源                 | 子表名模板       | 示例                |
| ---------------------- | ---------------- | ------------------- |
| OPC UA                 | `t_{ns}_{id#/_}` | `t_3_Device_Folder` |
| OPC DA / KingHistorian | `t_{tagname}`    | `t_Folder1`         |
| Pspace                 | `t_{point_id}`   | `t_object_001`      |

### ObjectNodeConfig 结构

```rust
pub struct ObjectNodeConfig {
    pub id: String,                    // Node ID
    pub name: Option<String>,          // 经过规则替换的名称
    pub browse_name: Option<String>,   // Node BrowseName
    pub display_name: Option<String>,  // Node DisplayName
    pub description: Option<String>,   // Node Description
    pub path: Option<String>,          // Node Path
}
```

## 映射生成流程

### 选择数据点位模式

```
DSN 参数
  │
  ├── super_table_expression = "opc_{type}"
  ├── child_table_expression = "t_{ns}_{id#/_}"
  ├── custom_tags = "varchar(256)::device::{..id.}"
  │
  ▼
PointMappingRule::from_dsn()
  │
  ├── 1. 执行 taosx-opc points 获取点位列表 [DataSet]
  │
  ├── 2. rule.generate(datasets)
  │       ├── 对每个点位调用 gen_point_config() → PointConfig
  │       │     ├── 解析模板生成 tbname (code)
  │       │     ├── 解析模板生成 stable
  │       │     └── 解析模板生成 tag_values
  │       └── 对每个点位调用 gen_table_config() → TableConfig
  │             ├── 生成 value ColumnConfig
  │             ├── 生成 quality ColumnConfig
  │             ├── 生成 primary_key ColumnConfig
  │             └── 生成 TagConfig 列表
  │
  ├── 3. rule.generate_node_config_map(datasets) → Object Node 配置
  │
  └── 4. 组装 PointModelConfig
```

### CSV 配置文件模式

```
CSV 文件
  │
  ├── point_id, enabled, stable, tbname, type, value_col, quality_col, ...
  ├── ns=3;s=Tag1, 1, opc_{type}, t_3_Tag1, float, val, quality, ...
  ├── ns=3;s=Tag2, 1, opc_{type}, t_3_Tag2, int, val, quality, ...
  │
  ▼
CsvParser::parse()
  │
  ├── 解析 CSV header → CsvHeader（识别列类型、tag 列等）
  │
  ├── 逐行解析：
  │     ├── PointConfig::from_csv() → PointConfig
  │     │     ├── code = 解析 tbname 列（支持模板替换）
  │     │     ├── stable = 解析 stable 列
  │     │     ├── value_type = 解析 type 列
  │     │     └── tag_values = 解析所有 tag 列的值
  │     └── TableConfig::from_csv() → TableConfig
  │           ├── column_configs = 解析 value/quality/ts 等列
  │           └── tag_configs = 解析 header 中的 tag 定义列
  │
  └── 组装 PointModelConfig
```

## 具体映射示例

### 示例一：OPC UA 选择数据点位

**配置**：

- `super_table_expression = "opc_{type}"`
- `child_table_expression = "t_{ns}_{id#/_}"`
- `custom_tags = "varchar(256)::device::{..id.};varchar(1024)::path::{id}"`

**点位列表**：

| point_id               | 数据类型 |
| ---------------------- | -------- |
| `ns=3;s=PLC/温度/Tag1` | Float    |
| `ns=3;s=PLC/温度/Tag2` | Float    |
| `ns=3;s=PLC/压力/Tag3` | Double   |

**生成的 TDengine 表结构**：

```sql
-- 超级表（Float 类型）
CREATE TABLE IF NOT EXISTS `opc_float` (
    `ts` TIMESTAMP,
    `val` FLOAT,
    `quality` INT
) TAGS (
    `device` VARCHAR(256),
    `path` VARCHAR(1024)
);

-- 超级表（Double 类型）
CREATE TABLE IF NOT EXISTS `opc_double` (
    `ts` TIMESTAMP,
    `val` DOUBLE,
    `quality` INT
) TAGS (
    `device` VARCHAR(256),
    `path` VARCHAR(1024)
);

-- 子表
CREATE TABLE
IF NOT EXISTS `t_3_PLC_温度_Tag1` USING `opc_float` (`device`, `path`) TAGS('温度', 'PLC/温度/Tag1')
IF NOT EXISTS `t_3_PLC_温度_Tag2` USING `opc_float` (`device`, `path`) TAGS('温度', 'PLC/温度/Tag2')
IF NOT EXISTS `t_3_PLC_压力_Tag3` USING `opc_double` (`device`, `path`) TAGS('压力', 'PLC/压力/Tag3');
```

### 示例二：OPC DA CSV 配置

**CSV 文件**：

```csv
tag_name,enabled,stable,tbname,type,value_col,quality_col,location::varchar(256)
Device.Temp.Tag1,1,opc_{type},t_Tag1,float,val,quality,Factory-A
Device.Temp.Tag2,0,opc_{type},t_Tag2,float,val,quality,Factory-A
Device.Press.Tag3,1,opc_{type},t_Tag3,double,val,quality,Factory-B
```

**生成结果**：

- `Tag2` 的 `enabled=0`，在 `handle_point_message_init` 阶段会被 `DROP TABLE IF EXISTS t_Tag2`
- `Tag1` 和 `Tag3` 正常创建超级表和子表
- `location` 是自定义 Tag，类型为 `VARCHAR(256)`

## 动态点位与静态点位

### 静态点位（预定义）

在 `PointModelConfig` 构建时，`point_config_map` 和 `table_config_map` 中已完全确定映射关系：

- 超级表名已解析（如 `opc_float`）
- 子表名已确定（如 `t_3_Tag1`）
- 列类型已知

这些点位在 `handle_point_message_init` 阶段被预创建。

### 动态点位

以下情况下，表达式模板无法在构建时完全解析：

- `stable` 仍包含 `{type}` 占位符（value 的类型在运行时才知道）
- `tbname` 仍包含 `{id}` 等占位符

这些点位**不会**在 `handle_point_message_init` 中预建表，而是在 `consume_point_record` 写入时，通过错误码 `0x2603`（表不存在）触发自动建表流程。

### 动态点位更新（PointsUpdater）

当 `update_mode` 为 `Append` 或 `Update` 时，`PointsUpdater` 后台任务会定期扫描数据源的最新点位列表，与当前配置对比后更新 `collect.toml`，使 taosx-opc 子进程采集新增的点位。

## 与其他文档的关系

| 文档                                                             | 关联                                                                      |
| ---------------------------------------------------------------- | ------------------------------------------------------------------------- |
| [`opc_to_taos.md`](./opc_to_taos.md)                             | 在步骤二中调用 `OPCConfig::from_dsn_collect_mode` 构建 `PointModelConfig` |
| [`handle-point-message-init.md`](./handle-point-message-init.md) | 使用 `PointModelConfig` 进行预建表                                        |
| [`consume_point_record.md`](./consume_point_record.md)           | 使用 `PointModelConfig` 进行数据写入和异常补建表                          |
| [`custom-tags.md`](./custom-tags.md)                             | 自定义 Tag 的详细设计                                                     |
