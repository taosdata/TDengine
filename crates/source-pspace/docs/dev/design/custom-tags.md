# pSpace 数据源的自定义标签

## 概述

pSpace 数据源支持为每个数据点位配置自定义 Tag（标签），用于在 TDengine 超级表中为子表附加元数据信息。自定义标签通过 **模板表达式（Pattern）** 定义 Tag 值，支持引用点位的属性（如名称、路径、描述等）进行动态替换，从而实现灵活的标签配置。

自定义标签有两种配置途径：

1. **"选择数据点位"模式（Select）**：通过 DSN 参数 `custom_tags` 配置，使用 Tag 表达式统一定义所有点位的标签规则。
2. **"CSV 点位配置文件"模式（CSV）**：在 CSV 文件中通过 `tag::<DataType>::<TagName>` 格式的列标题定义标签，每行直接为对应点位指定 Tag 值。

如果用户在 Select 模式下未指定 `custom_tags` 参数，系统会使用默认的三个自定义标签：`Name`、`LongName` 和 `Description`。

## 定义

- **自定义标签（Custom Tags）**：pSpace 数据源支持对每个数据点位配置自定义的 Tag，即用户可以自行定义 Tag 的名称、数据类型和 Tag 值。自定义标签可通过两种方式配置：一是在 CSV 点位配置文件中，通过 `tag::<DataType>::<TagName>` 列定义；二是在"选择数据点位"模式下，通过 DSN 参数 `custom_tags` 的 Tag 表达式定义。
- **属性值替换**：在配置自定义标签的 Tag 值时，使用 `{Attr}` 或 `{Attr#XY}` 表达式引用数据点位的属性值。其中 `{Attr}` 直接引用属性原始值，`{Attr#XY}` 在引用的同时将属性值中的字符 X 替换为字符 Y，并修剪结果首尾的 Y 字符。
- **Pattern（表达式）**：自定义标签值的模板字符串，可包含静态文本和占位符（如 `{Attr}`、`{Attr#XY}`）。

## pSpaceNode 的属性

pSpace 的数据模型由两种核心结构组成：**节点（PspaceNode）**和**数据点位（PspacePoint）**。

### PspaceNode（节点）

节点对应 pSpace 中 `type = PS_NODE` 的 Tag，用于表示数据的层级结构（树形目录）。

| 属性        | 类型     | 说明                     |
| ----------- | -------- | ------------------------ |
| `id`        | `u64`    | 节点唯一标识             |
| `name`      | `String` | 节点名称，如 `"北京"`    |
| `long_name` | `String` | 节点完整路径，如 `\北京` |
| `is_leaf`   | `bool`   | 是否为叶子节点           |

### PspacePoint（数据点位）

数据点位是 pSpace 中实际的数据采集点，携带实时或历史数据。

| 属性        | 类型             | 说明                                           | 可用占位符                  |
| ----------- | ---------------- | ---------------------------------------------- | --------------------------- |
| `id`        | `u64`            | 点位唯一标识                                   | -                           |
| `name`      | `String`         | 点位名称，如 `"温度"`                          | `{Name}`                    |
| `type`      | `String`         | pSpace 内部类型，如 `PS_ANALOG`、`PS_STRING`   | `{type}` (用于超级表表达式) |
| `long_name` | `String`         | 完整路径名称，如 `\北京\温度`                  | `{LongName}`                |
| `desc`      | `Option<String>` | 可选的描述信息                                 | `{Description}`             |
| `data_type` | `Option<String>` | pSpace 的数据类型枚举名，如 `psDataType_Float` | -                           |

**pSpace 数据类型映射**：

`data_type` 字段会通过 `to_ipc_data_type()` 函数映射为系统内部的 `IpcDataType`，具体映射关系如下：

| pSpace 数据类型      | IpcDataType              |
| -------------------- | ------------------------ |
| `psDataType_Empty`   | `Null`                   |
| `psDataType_Bool`    | `Bool`                   |
| `psDataType_Int8`    | `Int8`                   |
| `psDataType_UInt8`   | `UInt8`                  |
| `psDataType_Int16`   | `Int16`                  |
| `psDataType_UInt16`  | `UInt16`                 |
| `psDataType_Int32`   | `Int32`                  |
| `psDataType_UInt32`  | `UInt32`                 |
| `psDataType_Int64`   | `Int64`                  |
| `psDataType_UInt64`  | `UInt64`                 |
| `psDataType_Float`   | `Float32`                |
| `psDataType_Double`  | `Float64`                |
| `psDataType_Time`    | `Timestamp(Millisecond)` |
| `psDataType_String`  | `VarChar(1024)`          |
| `psDataType_WString` | `NChar(1024)`            |
| `psDataType_Blob`    | `Blob`                   |

## pSpace ID 的占位符替换

pSpace 数据源在生成 TDengine 表名时，使用占位符替换机制将模板表达式中的占位符替换为实际值。

### 超级表名（Super Table）

超级表名通过 `super_table_expression` 参数配置，默认值为 `pspace_{type}`。

- `{type}`：根据点位的数据类型（`IpcDataType`）自动替换为对应的类型名。例如：
  - `psDataType_Float` → `pspace_float32`
  - `psDataType_Int32` → `pspace_int32`
  - `psDataType_String` → `pspace_varchar`
  - `psDataType_WString` → `pspace_nchar`

### 子表名（Child Table）

子表名通过 `child_table_expression` 参数配置，默认值为 `t_{point_id}`。

- `{point_id}`：替换为点位的实际 ID 值。例如：
  - 点位 ID 为 `150017`，则子表名为 `t_150017`。

> **注意**：生成的表名中，`.` 和 `` ` `` 字符会被自动替换为 `_`。

## pSpace 自定义标签

### 自定义标签的配置方式

#### 方式一：DSN 参数 `custom_tags`（Select 模式）

在"选择数据点位"模式下，通过 DSN 的 `custom_tags` 参数定义自定义标签。可以配置多个自定义标签，以 `;` 分隔。每个标签的格式为：

```
<DataType>::<TagName>::<Pattern>
```

- `<DataType>`：Tag 的数据类型，如 `VARCHAR(1024)`、`INT` 等。
- `<TagName>`：Tag 的名称。
- `<Pattern>`：Tag 值的模板表达式，可包含占位符。

**示例 DSN**：

```
pspace://admin:admin888@127.0.0.1:5678?custom_tags=VARCHAR(1024)::Name::{Name};VARCHAR(1024)::LongName::{LongName};VARCHAR(1024)::Description::{Description}
```

上述示例定义了三个自定义标签：

- `Name`（VARCHAR(1024) 类型），值为点位的 `name` 属性。
- `LongName`（VARCHAR(1024) 类型），值为点位的 `long_name` 属性。
- `Description`（VARCHAR(1024) 类型），值为点位的 `desc` 属性。

**默认标签**：

如果 DSN 中未指定 `custom_tags` 参数，系统会自动使用以下三个默认标签：

| Tag 名称      | 数据类型        | Pattern         |
| ------------- | --------------- | --------------- |
| `Name`        | `VARCHAR(1024)` | `{Name}`        |
| `LongName`    | `VARCHAR(1024)` | `{LongName}`    |
| `Description` | `VARCHAR(1024)` | `{Description}` |

#### 方式二：CSV 配置文件（CSV 模式）

在"CSV 点位配置文件"模式下，通过 CSV 文件的列标题定义自定义标签。标签列的标题格式为：

```
tag::<DataType>::<TagName>
```

- `<DataType>`：Tag 的数据类型，如 `VARCHAR(1024)`、`INT` 等。
- `<TagName>`：Tag 的名称。

CSV 文件的每一行对应一个点位，标签列的单元格值即为该点位的 Tag 值。

**CSV 文件必需列**：

| 列名                         | 是否必需 | 说明                            |
| ---------------------------- | -------- | ------------------------------- |
| `point_id`                   | 必需     | 点位 ID                         |
| `stable`                     | 必需     | 超级表名表达式                  |
| `tbname`                     | 必需     | 子表名表达式                    |
| `type`                       | 可选     | 数据类型（如 `FLOAT`、`INT32`） |
| `value_col`                  | 可选     | 值列别名，默认 `val`            |
| `value_transform`            | 可选     | 值列变换表达式                  |
| `quality_col`                | 可选     | 质量列别名，默认 `quality`      |
| `ts_col`                     | 可选     | 原始时间戳列别名                |
| `ts_transform`               | 可选     | 原始时间戳变换表达式            |
| `request_ts_col`             | 可选     | 请求时间戳列别名                |
| `request_ts_transform`       | 可选     | 请求时间戳变换表达式            |
| `received_ts_col`            | 可选     | 接收时间戳列别名                |
| `received_ts_transform`      | 可选     | 接收时间戳变换表达式            |
| `tag::<DataType>::<TagName>` | 可选     | 自定义标签列（可定义多个）      |

**时间戳主键优先级**：当配置了多个时间戳列时，主键的优先级为 `ts_col` > `request_ts_col` > `received_ts_col`。如果均未配置，则自动添加默认的 `original_ts` 列作为主键，别名为 `ts`。

**CSV 文件示例**：

```csv
point_id,stable,tbname,tag::VARCHAR(1024)::name,tag::VARCHAR(1024)::LongName,tag::VARCHAR(1024)::Description
150017,pspace_{type},t_{point_id},气温,\北京\气温,温度传感器
150019,pspace_{type},t_{point_id},气温,\北京\朝阳\气温,
```

在此示例中：

- 定义了 3 个自定义标签：`name`、`LongName`、`Description`。
- 点位 `150017` 的 `name` 为 `"气温"`，`LongName` 为 `"\北京\气温"`，`Description` 为 `"温度传感器"`。
- 点位 `150019` 的 `Description` 为空（空值会被忽略，不写入对应 Tag）。

**完整 CSV 示例（含所有可选列）**：

```csv
No.,point_id,stable,tbname,value_col,value_transform,type,quality_col,ts_col,ts_transform,request_ts_col,request_ts_transform,received_ts_col,received_ts_transform,tag::VARCHAR(1024)::name,tag::VARCHAR(1024)::LongName,tag::VARCHAR(1024)::Description
1,150017,pspace_{type},t_{point_id},val,,,quality,ts,,qts,,rts,,气温,\\北京\\气温,
2,150019,pspace_{type},t_{point_id},val,,,quality,ts,,qts,,rts,,气温,\\北京\\朝阳\\气温,
```

### 属性值替换

在 Select 模式下，自定义标签的 Pattern 中可以使用占位符引用 pSpace 数据点位的属性值。系统会在生成点位映射配置时，将占位符替换为对应点位的实际属性值。

#### 支持的占位符

**简单占位符 `{Attr}`**：直接引用属性的原始值。

| 占位符          | 引用的属性              | 说明                                 |
| --------------- | ----------------------- | ------------------------------------ |
| `{Name}`        | `PspacePoint.name`      | 点位名称                             |
| `{LongName}`    | `PspacePoint.long_name` | 点位完整路径                         |
| `{Description}` | `PspacePoint.desc`      | 点位描述（如果为空则替换为空字符串） |

**转换占位符 `{Attr#XY}`**：引用属性值的同时，将值中所有的字符 X 替换为字符 Y，并修剪结果首尾的 Y 字符。

| 占位符              | 说明                                                     |
| ------------------- | -------------------------------------------------------- |
| `{Name#XY}`         | 将 `name` 中的 X 替换为 Y，trim 首尾 Y                  |
| `{LongName#XY}`     | 将 `long_name` 中的 X 替换为 Y，trim 首尾 Y             |
| `{Description#XY}`  | 将 `desc` 中的 X 替换为 Y，trim 首尾 Y                  |

其中 X 和 Y 为任意单个 ASCII 字符。例如 `{LongName#\_}` 表示将 `long_name` 中的 `\` 替换为 `_`，并修剪首尾的 `_`。

#### 替换流程

1. **生成初始 tag_values**：系统根据 `CustomTag` 列表中的每个标签，调用 `generate_tag_value_from_pattern()` 用 `point_id` 对 Pattern 进行初次替换（pSpace 源类型下此步骤为直接透传原始 Pattern）。
2. **属性值替换（`{Attr#XY}` 优先）**：调用 `extra_custom_tags()` 方法，按以下顺序处理：
   1. 先扫描并替换所有 `{Attr#XY}` 转换占位符（将 X 替换为 Y，trim 首尾 Y）。
   2. 再替换剩余的 `{Name}`、`{LongName}`、`{Description}` 简单占位符。

> **替换顺序很重要**：`{Attr#XY}` 必须在 `{Attr}` 之前处理，否则 `{Name#XY}` 中的 `{Name` 部分会被简单替换错误匹配。

#### 替换示例

假设点位信息如下：

| 属性        | 值           |
| ----------- | ------------ |
| `id`        | `150017`     |
| `name`      | `温度`       |
| `long_name` | `\北京\温度` |
| `desc`      | `温度传感器` |

则各 Pattern 的替换结果为：

| Pattern                | 替换结果             |
| ---------------------- | -------------------- |
| `{Name}`               | `温度`               |
| `{LongName}`           | `\北京\温度`         |
| `{Description}`        | `温度传感器`         |
| `prefix_{Name}_suffix` | `prefix_温度_suffix` |
| `{LongName}/{Name}`    | `\北京\温度/温度`    |
| `{LongName#\_}`        | `北京_温度`          |
| `{LongName#\.}`        | `北京.温度`          |
| `name={Name},path={LongName#\_}` | `name=温度,path=北京_温度` |

`{LongName#\_}` 的替换过程：`\北京\温度` → 将 `\` 替换为 `_` → `_北京_温度` → trim 首尾 `_` → `北京_温度`。

> **注意**：如果点位的 `desc` 字段为 `None`，`{Description}` 会被替换为空字符串 `""`。

#### 混合使用

Pattern 支持将静态文本、简单占位符 `{Attr}` 和转换占位符 `{Attr#XY}` 混合使用。例如：

```
custom_tags=VARCHAR(1024)::FullInfo::Name={Name},Path={LongName#\_}
```

对于上述点位，该 Tag 的值将被替换为：`Name=温度,Path=北京_温度`。
