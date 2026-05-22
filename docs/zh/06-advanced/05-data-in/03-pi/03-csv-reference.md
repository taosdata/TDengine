---
title: "PI 模型配置文件参考"
sidebar_label: "模型配置参考"
---

本页详细说明 PI 数据接入任务所使用的模型配置文件（CSV 格式），涵盖多列模型和单列模型两种格式的完整定义。

## 1. 概述

模型配置文件是一个 CSV 格式的文本文件，定义了从 PI 系统到 TDengine 的数据映射规则，包括：

- PI 数据源（PI Point 或 AF 元素模板）与 TDengine 超级表的对应关系
- PI 属性与 TDengine 列（Metric 列、TAG 列）的映射
- 数据过滤条件
- 数据转换表达式

在 Explorer 中创建 PI 任务时，点击 **下载默认配置** 按钮可以自动生成默认的模型配置文件。你可以下载后编辑，再上传覆盖默认配置。

## 2. 多列模型配置文件

多列模型将一个 PI AF 元素映射为 TDengine 的一张子表。一个元素模板（Template）下的所有元素共享同一个超级表结构。

### 2.1 文件结构

多列模型配置文件由一个或多个**超级表定义块**组成，每个块之间用空行分隔。每个超级表定义块包含：

| 行 | 格式 | 说明 |
| --- | --- | --- |
| 超级表名 | `SuperTable,<名称>` | TDengine 超级表名称 |
| 子表名规则 | `SubTable,<模板>` | 支持占位符，如 `${element_name}_${element_id}` |
| 元素模板 | `Template,<模板名>` | 对应 PI AF 中的元素模板名称 |
| 过滤条件 | `Filter,<条件>` | 可选，用于过滤元素 |
| 列定义 | `<列名>,KEY\|COLUMN\|TAG,<数据类型>,<映射规则>` | 定义每一列的映射 |

### 2.2 列定义说明

| 列类型 | 关键字 | 说明 |
| --- | --- | --- |
| 时间戳列 | `KEY` | 必须有且仅有一个，数据类型为 `TIMESTAMP` |
| 数据列 | `COLUMN` | 对应 PI Point 属性的值 |
| 标签列 | `TAG` | 对应元素的静态属性或元数据 |

### 2.3 完整示例

以下配置文件定义了两个超级表：`metertemplate`（来自 MeterTemplate 模板）和 `farm`（来自 Farm 模板）。

```csv
SuperTable,metertemplate
SubTable,${element_name}_${element_id}
Template,MeterTemplate
Filter,
ts,KEY,TIMESTAMP,$ts
voltage,COLUMN,DOUBLE,$voltage
voltage_status,COLUMN,INT,$voltage_status
current,COLUMN,DOUBLE,$current
current_status,COLUMN,INT,$current_status
element_id,TAG,VARCHAR(100),$element_id
element_name,TAG,VARCHAR(100),$element_name
path,TAG,VARCHAR(100),$path
categories,TAG,VARCHAR(100),$categories

SuperTable,farm
SubTable,${element_name}_${element_id}
Template,Farm
Filter,
ts,KEY,TIMESTAMP,$ts
wind_speed,COLUMN,FLOAT,$wind_speed
wind_speed_status,COLUMN,INT,$wind_speed_status
power_production,COLUMN,FLOAT,$power_production
power_production_status,COLUMN,INT,$power_production_status
lost_power,COLUMN,FLOAT,$lost_power
lost_power_status,COLUMN,INT,$lost_power_status
farm_lifetime_production__weekly_,COLUMN,FLOAT,$farm_lifetime_production__weekly_
farm_lifetime_production__weekly__status,COLUMN,INT,$farm_lifetime_production__weekly__status
farm_lifetime_production__hourly_,COLUMN,FLOAT,$farm_lifetime_production__hourly_
farm_lifetime_production__hourly__status,COLUMN,INT,$farm_lifetime_production__hourly__status
element_id,TAG,VARCHAR(100),$element_id
element_name,TAG,VARCHAR(100),$element_name
path,TAG,VARCHAR(100),$path
categories,TAG,VARCHAR(100),$categories
```

### 2.4 逐行解读

以 `metertemplate` 超级表为例：

- `SuperTable,metertemplate`：定义超级表名为 `metertemplate`
- `SubTable,${element_name}_${element_id}`：子表名由元素名和元素 ID 拼接，如 `Meter001_12345`
- `Template,MeterTemplate`：数据来自 PI AF 中名为 `MeterTemplate` 的元素模板
- `Filter,`：不做过滤，同步所有该模板下的元素
- `ts,KEY,TIMESTAMP,$ts`：时间戳列，取 PI 数据的时间戳
- `voltage,COLUMN,DOUBLE,$voltage`：数据列，取元素的 `voltage` 属性值
- `voltage_status,COLUMN,INT,$voltage_status`：数据列，取 `voltage` 的质量状态码
- `element_id,TAG,VARCHAR(100),$element_id`：标签列，取元素的唯一 ID
- `path,TAG,VARCHAR(100),$path`：标签列，取元素在 AF 层级中的路径

### 2.5 默认映射规则

对于使用 AF Server 的多列模型任务：

- taosX 默认将 **PI Point 属性** 映射为 TDengine **COLUMN**（Metric 列）
- taosX 默认将 **其他属性**（静态属性）映射为 TDengine **TAG** 列

## 3. 单列模型配置文件

单列模型将一个 PI Point 映射为 TDengine 的一张子表。

### 3.1 文件结构

单列模型配置文件分为两个部分：

**第一部分：超级表定义**（与多列模型类似）

定义若干个超级表的列结构。默认生成的配置会按 **UOM（工程单位）+ 数据类型** 将点位自动分组到不同的超级表。

#### 第二部分：点位映射

格式为 `<Point名称>,POINT,<超级表名>`，定义每个 PI Point 属于哪个超级表。

### 3.2 完整示例

```csv
SuperTable,volt_float32
SubTable,${point_name}
Filter,
ts,KEY,TIMESTAMP,$ts
value,COLUMN,FLOAT,$value
status,COLUMN,INT,$status
path,TAG,VARCHAR(200),$path
point_name,TAG,VARCHAR(100),$point_name
ptclassname,TAG,VARCHAR(100),$ptclassname
sourcetag,TAG,VARCHAR(100),$sourcetag
tag,TAG,VARCHAR(100),$tag
descriptor,TAG,VARCHAR(100),$descriptor
exdesc,TAG,VARCHAR(100),$exdesc
engunits,TAG,VARCHAR(100),$engunits
pointsource,TAG,VARCHAR(100),$pointsource
step,TAG,VARCHAR(100),$step
future,TAG,VARCHAR(100),$future
element_paths,TAG,VARCHAR(512),`$element_paths.replace("\\", ".")`

SuperTable,milliampere_float32
SubTable,${point_name}
Filter,
ts,KEY,TIMESTAMP,$ts
value,COLUMN,FLOAT,$value
status,COLUMN,INT,$status
path,TAG,VARCHAR(200),$path
point_name,TAG,VARCHAR(100),$point_name
ptclassname,TAG,VARCHAR(100),$ptclassname
sourcetag,TAG,VARCHAR(100),$sourcetag
tag,TAG,VARCHAR(100),$tag
descriptor,TAG,VARCHAR(100),$descriptor
exdesc,TAG,VARCHAR(100),$exdesc
engunits,TAG,VARCHAR(100),$engunits
pointsource,TAG,VARCHAR(100),$pointsource
step,TAG,VARCHAR(100),$step
future,TAG,VARCHAR(100),$future
element_paths,TAG,VARCHAR(512),`$element_paths.replace("\\", ".")`

Meter_1000004_Voltage,POINT,volt_float32
Meter_1000004_Current,POINT,milliampere_float32
Meter_1000001_Voltage,POINT,volt_float32
Meter_1000001_Current,POINT,milliampere_float32
Meter_1000474_Voltage,POINT,volt_float32
Meter_1000474_Current,POINT,milliampere_float32
```

### 3.3 逐行解读

**超级表定义部分**：

- `SuperTable,volt_float32`：超级表名为 `volt_float32`（电压类 float32 点位）
- `SubTable,${point_name}`：子表名直接使用点位名
- `value,COLUMN,FLOAT,$value`：数据列，存储点位的值
- `status,COLUMN,INT,$status`：数据列，存储质量状态码
- `point_name,TAG,VARCHAR(100),$point_name`：标签列，存储点位名称
- `engunits,TAG,VARCHAR(100),$engunits`：标签列，存储工程单位
- `` element_paths,TAG,VARCHAR(512),`$element_paths.replace("\\", ".")` ``：标签列，使用内联表达式将路径分隔符从 `\` 替换为 `.`

**点位映射部分**：

- `Meter_1000004_Voltage,POINT,volt_float32`：点位 `Meter_1000004_Voltage` 归属于超级表 `volt_float32`
- `Meter_1000004_Current,POINT,milliampere_float32`：点位 `Meter_1000004_Current` 归属于超级表 `milliampere_float32`

## 4. 映射规则与表达式

### 4.1 常用占位符

以下占位符可在列定义的映射规则中使用：

| 占位符 | 说明 | 适用范围 |
| --- | --- | --- |
| `$ts` | 数据时间戳 | 单列/多列 |
| `$value` | 点位值 | 单列模型 |
| `$status` | 质量状态码 | 单列/多列 |
| `$point_name` | PI Point 名称 | 单列模型 |
| `$element_name` | AF 元素名称 | 多列模型 |
| `$element_id` | AF 元素唯一 ID | 多列模型 |
| `$path` | 元素/点位路径 | 单列/多列 |
| `$categories` | AF 元素分类 | 多列模型 |
| `$element_paths` | 点位关联的元素路径 | 单列模型 |
| `$<属性名>` | PI Point 属性或 AF 元素属性 | 按实际属性名引用 |

### 4.2 内联表达式

对于需要数据转换的场景，可以使用反引号包裹内联表达式：

```csv
element_paths,TAG,VARCHAR(512),`$element_paths.replace("\\", ".")`
```

更多关于映射规则和数据转换表达式的语法，请参阅[零代码第三方数据接入](../)"数据提取、过滤和转换"部分。

### 4.3 子表名占位符

`SubTable` 行的模板支持以下占位符：

| 占位符 | 说明 |
| --- | --- |
| `${point_name}` | PI Point 名称（单列模型） |
| `${element_name}` | AF 元素名称（多列模型） |
| `${element_id}` | AF 元素唯一 ID（多列模型） |

占位符可以组合使用，如 `${element_name}_${element_id}`。

## 5. 常见模式与最佳实践

### 5.1 按 UOM 分超级表

默认生成的单列模型配置会按 **UOM（工程单位）+ 数据类型** 自动分组。例如，所有单位为 "Volt" 且数据类型为 Float32 的点位会归入同一个超级表 `volt_float32`。

这是推荐的默认做法，可以保证同一超级表中所有子表的列结构完全一致。

### 5.2 过滤特定点位或模板

在 Explorer 中填写过滤条件后再点击 **下载默认配置**，可以只生成匹配过滤条件的点位/模板的配置。

也可以在下载后手动编辑 CSV：

- **多列模型**：修改 `Filter` 行
- **单列模型**：在点位映射部分删除不需要的点位行

### 5.3 自定义超级表名

如果默认按 UOM 分组的命名不满足需求，可以直接修改 `SuperTable` 行的值，并调整点位映射部分的超级表引用。
