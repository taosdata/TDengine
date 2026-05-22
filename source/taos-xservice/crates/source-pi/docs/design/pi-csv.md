# PI Data In CSV 配置文件规范

## 概述

PI Data In 使用一种**不规则的多功能 CSV 文件**作为数据模型配置。该文件承担两个核心功能：

1. **超级表（SuperTable）结构定义** — 描述 TDengine 超级表的 schema。
2. **点位/元素映射** — 描述 PI 中的 Point 或 Element 到超级表的归属关系。

这两部分**无表头**，写在同一个 CSV 文件中，超级表定义在上，映射列表在下，用空行分隔。

> 所有关键字**不区分大小写**。

---

## 文件整体结构

```
┌─────────────────────────────────────────────┐
│  超级表定义 1 (SuperTable block)              │
│    - SuperTable 行                          │
│    - SubTable 行                            │
│    - [Template 行]  （可选，仅多列模型）       │
│    - Filter 行                              │
│    - Schema 行 × N                          │
├─────────────────────────────────────────────┤
│  空行                                       │
├─────────────────────────────────────────────┤
│  超级表定义 2 (SuperTable block)             │
│    ...                                      │
├─────────────────────────────────────────────┤
│  空行                                       │
├─────────────────────────────────────────────┤
│  点位/元素映射列表                            │
│    point_name,POINT,super_table_name        │
│    ...                                      │
└─────────────────────────────────────────────┘
```

---

## 一、超级表定义块

每个超级表定义以 `SuperTable` 关键字行开始，包含以下几种行：

### 1.1 `SuperTable` 行（必选）

标记一个超级表定义的开始，同时定义超级表名。

```
SuperTable,<超级表名>
```

示例：

```
SuperTable,ts_float32
SuperTable,ts_string
```

**命名规则**（自动生成时）：

- 单列模型：如果 PI 点有 UOM，则 `{uom}_{type}`（如 `kilowatt_single`）；否则 `ts_{type}`（如 `ts_float32`、`ts_string`）。
- 多列模型：将 Template 名做小写 + 非字母数字字符替换为 `_`。

### 1.2 `SubTable` 行（必选）

定义子表名的命名模式，使用 `$` 引用源数据属性。

```
SubTable,<子表名模式>
```

示例：

```
SubTable,${point_name}                        # 单列模型：以 point_name 作为子表名
SubTable,${element_name}_${element_id}        # 多列模型：以 element_name + element_id 作为子表名
```

`$` 引用可嵌入任意前缀/后缀文本，例如 `prefix_${point_name}_suffix`。

### 1.3 `Template` 行（可选，仅多列模型）

标识该超级表定义来源于 PI 系统中哪个 Template。纯信息性字段，不影响解析逻辑。

```
Template,<模板名>
```

示例：

```
Template,Template_Beijing
```

### 1.4 `Filter` 行（必选，值可为空）

定义数据入库前的过滤表达式。若无过滤需求，值留空。

```
Filter,<过滤表达式>
Filter,
```

表达式中可用 `$` 引用字段，例如：`$value > 0 && $status == 0`。

### 1.5 Schema 行（1 至 N 行）

定义超级表的列结构，固定 **4 列**：

```
<列名>,<列类型>,<数据类型>,<映射表达式>
```

| 列         | 说明              | 可选值                                                                     |
| ---------- | ----------------- | -------------------------------------------------------------------------- |
| 列名       | TDengine 中的列名 | 自定义字符串                                                               |
| 列类型     | 列的角色          | `KEY`（主键/时间戳）、`COLUMN`（普通列）、`TAG`（标签列）                  |
| 数据类型   | TDengine 数据类型 | `TIMESTAMP`、`FLOAT`、`DOUBLE`、`INT`、`NCHAR(n)`、`VARCHAR(n)` 等         |
| 映射表达式 | 数据来源映射      | `$字段名` 引用源数据字段，也支持表达式如 `` `$field.replace("\\", ".")` `` |

**示例（单列模型）**：

```
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
```

**示例（多列模型）**：

```
ts,KEY,TIMESTAMP,$ts
current,COLUMN,DOUBLE,$current
current_status,COLUMN,INT,$current_status
voltage,COLUMN,DOUBLE,$voltage
voltage_status,COLUMN,INT,$voltage_status
element_id,TAG,VARCHAR(100),$element_id
element_name,TAG,VARCHAR(100),$element_name
path,TAG,VARCHAR(100),$path
categories,TAG,VARCHAR(100),$categories
```

**映射表达式说明**：

- `$字段名`：直接引用源数据中的同名属性。
- `` `表达式` ``：用反引号包裹的 JavaScript 风格表达式。反引号内逗号不会被当做 CSV 分隔符。
- 常量值：直接写固定值，如 `0`。

---

## 二、点位/元素映射列表

超级表定义全部结束后，接下来是映射列表，描述每个 PI 点或元素归属于哪个超级表。

### 2.1 单列模型 — POINT 行

固定 **3 列**：

```
<point_name>,POINT,<super_table_name>
```

示例：

```
Meter_1000001_Voltage,POINT,ts_float32
Meter_1000001_Current,POINT,ts_float32
HT-102.Alarm Count Major.91ba8c4e-910d-5558-3198-43181f1b426e,POINT,ts_int32
sitec_dig_00_swyd_bkr_1ey_cb_c_status_0eexl0606,POINT,ts_string
```

### 2.2 多列模型 — ELEMENT 行

固定 **4 列**：

```
<element_name>,ELEMENT,<super_table_name>,<element_id>
```

示例：

```
Element_Beijing1,ELEMENT,template_beijing,d552ba74-cf9a-11ee-bf12-00505695feda
```

---

## 三、完整示例

### 单列模型配置文件

```csv
SuperTable,ts_float32
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

SuperTable,ts_float64
SubTable,${point_name}
Filter,
ts,KEY,TIMESTAMP,$ts
value,COLUMN,DOUBLE,$value
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


Meter_1000001_Voltage,POINT,ts_float32
Meter_1000001_Current,POINT,ts_float32
Meter_1000002_Voltage,POINT,ts_float32
Meter_1000002_Current,POINT,ts_float32
```

---

## 四、解析规则总结

1. **空行和注释**：文件开头的空行和 `#` 开头的行会被跳过。
2. **超级表块分割**：遇到 `SuperTable` 关键字时，前一个超级表块结束，新块开始。
3. **点位/元素识别**：包含 `,POINT,`（不区分大小写）的行归入点位列表；包含 `,ELEMENT,` 的行归入元素列表。
4. **CSV 分隔符**：以逗号分隔，但反引号 `` ` `` 内的逗号不作为分隔符。
5. **`$` 引用**：`$字段名` 或 `${字段名}` 引用源数据属性。在 Filter 表达式解析时，`$` 会被自动移除。

---

## 五、单列模型内置属性

以下是单列模型中可用的 `$` 引用属性：

| 属性名           | 说明                                 |
| ---------------- | ------------------------------------ |
| `$ts`            | 时间戳                               |
| `$value`         | 点位值                               |
| `$status`        | 数据质量状态码                       |
| `$path`          | 点位完整路径                         |
| `$point_name`    | 点位名称                             |
| `$ptclassname`   | 点位类名                             |
| `$sourcetag`     | 源标签                               |
| `$tag`           | 标签                                 |
| `$descriptor`    | 描述符                               |
| `$exdesc`        | 扩展描述                             |
| `$engunits`      | 工程单位                             |
| `$pointsource`   | 点位来源                             |
| `$step`          | 阶跃标志                             |
| `$future`        | 未来数据标志                         |
| `$element_paths` | 关联的 AF 元素路径（仅 AF 单列模式） |

## 六、多列模型内置属性

| 属性名             | 说明                          |
| ------------------ | ----------------------------- |
| `$ts`              | 时间戳                        |
| `$element_id`      | 元素唯一 ID                   |
| `$element_name`    | 元素名称                      |
| `$path`            | 元素路径                      |
| `$categories`      | 元素分类                      |
| `${属性名}`        | Template 中定义的各动态属性值 |
| `${属性名}_status` | 各动态属性的状态码            |
