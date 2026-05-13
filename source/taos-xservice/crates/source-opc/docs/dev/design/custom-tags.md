# OPC 数据源自定义标签设计文档

## 概述

OPC 数据源（OPC UA / OPC DA）支持在点位映射配置中使用自定义标签（Custom Tags），用于将 OPC 节点的属性信息作为 TDengine 子表的标签（Tag）写入。自定义标签的值支持静态文本和动态占位符两种方式。

## OPC 节点属性

OpcNode 包含下列属性：

| 属性          | 说明                       | 示例                           | 占位符替换 |
| ------------- | -------------------------- | ------------------------------ | ---------- |
| `BrowseName`  | 节点的浏览名称             | `Temperature_Sensor_01`        | 支持       |
| `DisplayName` | 节点的显示名称             | `温度传感器01`                 | 支持       |
| `Description` | 节点的描述信息             | `一号车间温度传感器`           | 支持       |
| `Path`        | 节点在地址空间中的完整路径 | `Objects/Plant/Area1/Sensor01` | 支持       |
| `NodeClass`   | 节点类型                   | `Object` 或 `Variable`         | 不支持     |
| `ParentId`    | 父节点 ID                  | `ns=2;s=Area1`                 | 不支持     |

> 仅 `BrowseName`、`DisplayName`、`Description`、`Path` 四个属性支持作为自定义标签的占位符使用。`NodeClass` 和 `ParentId` 虽然是 OPC 节点的属性，但当前不支持在自定义标签 pattern 中引用。

## 节点属性占位符

在自定义标签的 pattern 表达式中，可使用以下占位符引用 OPC 节点属性：

| 占位符          | 替换值                        | 为空时         |
| --------------- | ----------------------------- | -------------- |
| `{BrowseName}`  | OpcNode 的 BrowseName 属性值  | 替换为空字符串 |
| `{DisplayName}` | OpcNode 的 DisplayName 属性值 | 替换为空字符串 |
| `{Description}` | OpcNode 的 Description 属性值 | 替换为空字符串 |
| `{Path}`        | OpcNode 的 Path 属性值        | 替换为空字符串 |

### 示例

CSV 配置中 OPC UA 的默认标签定义：

```
tag::VARCHAR(1024)::name        → {id#/.}
tag::VARCHAR(1024)::BrowseName  → {BrowseName}
tag::VARCHAR(1024)::DisplayName → {DisplayName}
tag::VARCHAR(1024)::Description → {Description}
tag::VARCHAR(1024)::Path        → {Path}
```

## 属性值字符替换 `{Attr#XY}`

除了直接引用节点属性值外，还支持 `{Attr#XY}` 语法对属性值进行字符替换：

- `X`：源字符（单个字符）
- `Y`：目标字符（单个字符）
- 替换后自动修剪首尾的目标字符

支持的属性：`BrowseName`、`DisplayName`、`Description`、`Path`。

### 示例

| 占位符             | 属性值                  | 替换结果              |
| ------------------ | ----------------------- | --------------------- |
| `{DisplayName#_.}` | `zs_p1_unit1_float`     | `zs.p1.unit1.float`   |
| `{BrowseName#-.}`  | `zs-p1-unit1`           | `zs.p1.unit1`         |
| `{Path#/_}`        | `/Objects/Plant/Area1/` | `Objects_Plant_Area1` |
| `{DisplayName#./}` | `.Device.Type.Tag.`     | `Device/Type/Tag`     |

可与静态文本组合使用：

- `prefix_{DisplayName#_.}_suffix` + `a_b_c` → `prefix_a.b.c_suffix`
- `{BrowseName#-.}({Description})` + BrowseName=`a-b`, Description=`desc` → `a.b(desc)`

> 注意：`{Attr#XY}` 占位符的优先级高于普通 `{Attr}` 占位符，在替换时会先处理 `{Attr#XY}`，再处理 `{Attr}`。

## 点位 ID 占位符

除节点属性占位符外，自定义标签的 pattern 还支持基于点位 ID 的占位符，用于从点位 ID 中提取或变换出标签值。

### OPC UA 的 ID 占位符

适用于 `ns=<namespace>;s=<identifier>` 格式的点位 ID。

以 `ns=6;s=Device/Type/TagName` 为例：

| 占位符     | 说明                           | 示例结果                              |
| ---------- | ------------------------------ | ------------------------------------- |
| `{ns}`     | 命名空间                       | `6`                                   |
| `{id}`     | 标识符（去掉 `s=` 等前缀）     | `Device/Type/TagName`                 |
| `{id.}`    | id 去掉最后一个 `.` 及其后缀   | `Device/Type` (若 id=`A.B.C` → `A.B`) |
| `{id/}`    | id 去掉最后一个 `/` 及其后缀   | `Device/Type`                         |
| `{id_}`    | id 去掉最后一个 `_` 及其后缀   | (类似逻辑)                            |
| `{id..}`   | id 去掉最后两个 `.` 段         | `A.B.C.D` → `A.B`                     |
| `{..id.}`  | id 按 `.` 分割后的倒数第二段   | `A.B.C` → `B`                         |
| `{id#/.}`  | `/` → `.`，并修剪首尾 `.`      | `Device.Type.TagName`                 |
| `{id#-.}`  | `-` → `.`，并修剪首尾 `.`      | `Device.Type.TagName`                 |
| `{id#/_}`  | `/` → `_`，并修剪首尾 `_`      | `Device_Type_TagName`                 |
| `{id#-_}`  | `-` → `_`，并修剪首尾 `_`      | `Device_Type_TagName`                 |
| `{id/#/.}` | 先执行 `{id/}`，再将 `/` → `.` | `Device/Type/TagName` → `Device.Type` |
| `{id_#_.}` | 先执行 `{id_}`，再将 `_` → `.` | `Device_Type_TagName` → `Device.Type` |

### OPC DA / KingHistorian 的 ID 占位符

适用于 `Device.DeviceType.TagName` 或 `/ASSETS/AB/EDCGQ.MP706AT.PV` 格式的点位 ID。

以 `Device.DeviceType.TagName` 为例：

| 占位符                     | 说明                      | 示例结果                    |
| -------------------------- | ------------------------- | --------------------------- |
| `{TagName}` / `{tag_name}` | 最后一个 `.` 之后的部分   | `TagName`                   |
| `{/tag_name}`              | 最后一个 `/` 之后的部分   | `EDCGQ.MP706AT.PV`          |
| `{id}`                     | 完整的点位 ID             | `Device.DeviceType.TagName` |
| `{_id}`                    | 将 `/` 替换为 `_`         | `Device_DeviceType.TagName` |
| `{id#/.}`                  | `/` → `.`，并修剪首尾 `.` | `Device.Type.TagName`       |
| `{id#-.}`                  | `-` → `.`，并修剪首尾 `.` | `Device.Type.TagName`       |
| `{id#/_}`                  | `/` → `_`，并修剪首尾 `_` | `Device_Type_TagName`       |
| `{id#-_}`                  | `-` → `_`，并修剪首尾 `_` | `Device_Type_TagName`       |

## 自定义标签配置

### DSN 参数方式

通过 DSN 的 `custom_tags` 参数配置，格式为：

```
custom_tags=<DataType>::<TagName>::<Pattern>[;<DataType>::<TagName>::<Pattern>]...
```

- 多个标签以 `;` 分隔
- 每个标签由 `::` 分隔的三部分组成：数据类型、标签名、值表达式

示例：

```
custom_tags=VARCHAR(100)::location::{id#/.};VARCHAR(1024)::browse_name::{BrowseName};INT::version::1
```

上述配置定义了三个自定义标签：

- `location`：VARCHAR(100) 类型，值为点位 ID 经 `/` → `.` 转换后的结果
- `browse_name`：VARCHAR(1024) 类型，值为节点的 BrowseName
- `version`：INT 类型，固定值 `1`

### CSV 配置方式

在 CSV 点位配置文件的表头中，标签列的格式为：

```
tag::<DataType>::<TagName>
```

对应行中填写标签值的 pattern 表达式。

示例表头与数据行：

```csv
No.,node_id,enabled,stable,tbname,...,tag::VARCHAR(1024)::name,tag::VARCHAR(1024)::BrowseName
1,ns=2;s=PLC.DEV.SITE,true,opc_{type},t_{ns}_{id#/_},...,{id#/.},{BrowseName}
```

## 占位符组合使用

占位符可以与静态文本自由组合。例如：

- `prefix_{id#/.}_suffix` → `prefix_Device.Type.TagName_suffix`
- `{BrowseName}({Description})` → `温度传感器(一号车间温度传感器)`
- `ns{ns}_{TagName}` → `ns6_TagName`

## 相关代码

| 模块               | 文件                                                                             | 说明                                         |
| ------------------ | -------------------------------------------------------------------------------- | -------------------------------------------- |
| OPC 节点属性定义   | `taosx-core/src/plugins/runners/opc/points.rs`                                   | `OpcNode` 结构体及属性常量                   |
| 节点属性占位符替换 | `taosx-core/src/plugins/sink/point/model.rs` (`extra_custom_tags`)               | `{BrowseName}` 等占位符替换逻辑              |
| 点位 ID 占位符替换 | `taosx-core/src/plugins/sink/point/model.rs` (`generate_tag_value_from_pattern`) | `{ns}`, `{id}`, `{TagName}` 等占位符替换逻辑 |
| 子表名占位符替换   | `taosx-core/src/plugins/sink/point/model.rs` (`generate_tbname_from_pattern`)    | 子表名表达式中的占位符替换                   |
| 超级表名占位符替换 | `taosx-core/src/plugins/sink/point/model.rs` (`generate_stable_from_pattern`)    | `{type}` 占位符替换                          |
| 自定义标签解析     | `taosx-core/src/plugins/sink/point/model.rs` (`CustomTag::try_from_dsn`)         | DSN 中 `custom_tags` 参数解析                |
| CSV 默认配置       | `crates/source-opc/src/lib.rs` (`UA_HEADER` / `UA_ROW`)                          | OPC UA CSV 模板的默认标签定义                |
