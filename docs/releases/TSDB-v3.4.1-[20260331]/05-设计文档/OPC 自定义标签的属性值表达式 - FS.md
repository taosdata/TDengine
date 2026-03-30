# OPC 自定义标签的属性值表达式 - FS

## 1. 背景

OPC 数据源的自定义标签（Custom Tags）已支持 `{BrowseName}`、`{DisplayName}`、`{Description}`、`{Path}` 等占位符，可将 OPC 节点属性值直接写入 TDengine 子表标签。但在实际工业场景中，属性值的分隔符格式往往不符合用户的目标命名规范，需要进行字符替换。
本功能通过 `{Attr#XY}` 语法，在不修改 OPC 服务器数据的前提下，允许用户在 pattern 表达式中对属性值进行单字符替换和首尾修剪。
任务：

## 2. 变更历史

| **日期** | **版本** | **负责人** | **主要修改内容** |
| --- | --- | --- | --- |
| 2026/3/17 | 0.1 | @杨志宇 | 初稿 |

## 3. 定义

- **自定义标签（Custom Tags）**：OPC UA / OPC DA / KingHistorian / pSpace 等数据源支持对每个数据点位配置自定义的 Tag，即用户可以自行定义 Tag 的名称、数据类型和 Tag 值。自定义标签可通过两种方式配置：一是在 CSV 点位配置文件中，通过 `tag::<DataType>::<TagName>` 列定义；二是在"选择数据点位"模式下，通过 DSN 参数 `custom_tags` 的 Tag 表达式定义。
- **属性值替换**：每种数据源的数据点位拥有各自特有的属性值。例如 OPC 的属性包括 DisplayName、Description、BrowseName、Path 等，KingHistorian 的属性包括 tag_name、description、group_name 等。属性值替换是指在配置自定义标签的 Tag 值时，使用 `{Attr}` 或 `{Attr#XY}` 表达式引用数据点位的属性值。其中 `{Attr}` 直接引用属性原始值，`{Attr#XY}` 在引用的同时将属性值中的字符 X 替换为字符 Y，并修剪结果首尾的 Y 字符。
- **Pattern（表达式）**：自定义标签值的模板字符串，可包含静态文本和占位符（如 `{Attr}`、`{Attr#XY}`）。

## 4. 行为说明

### 4.1 **语法规则**

`{Attr#XY}` 中：
1. `Attr`：属性名，支持 `BrowseName`、`DisplayName`、`Description`、`Path`
2. `#`：固定分隔符
3. `X`：源字符（单个字符）
4. `Y`：目标字符（单个字符）
5. `}`：闭合括号，必须紧跟在 Y 之后
合法示例：`{DisplayName#_.}`、`{BrowseName#-.}`、`{Path#/_}`
非法示例（不会被识别为替换占位符，保留原文）：
- `{DisplayName#_..}`（Y 超过一个字符）
- `{DisplayName#}`（缺少 XY）
- `{NodeClass#_.}`（不支持的属性）

### 4.2 **替换流程**

对每个 PointConfig 的 tag_values 中的每个值，按以下顺序执行替换：
1. `**{Attr#XY}**`**替换**（优先）：扫描 pattern 中所有 `{Attr#XY}` 占位符，将属性值中的字符 X 全部替换为 Y，然后修剪结果首尾的 Y 字符，最后将占位符替换为处理后的结果。依次处理 BrowseName → DisplayName → Description → Path。
2. `**{Attr}**`**普通替换**：将剩余的 `{BrowseName}`、`{DisplayName}`、`{Description}`、`{Path}` 占位符替换为对应属性的原始值。

### 4.3 **替换示例**

#### 4.3.1 **基本替换**

| Pattern | 属性名 | 属性值 | 结果 |
| --- | --- | --- | --- |
| `{DisplayName#_.}` | DisplayName | `zs_p1_unit1_float` | `zs.p1.unit1.float` |
| `{BrowseName#-.}` | BrowseName | `zs-p1-unit1` | `zs.p1.unit1` |
| `{Path#/_}` | Path | `/Objects/Plant/Area1/` | `Objects_Plant_Area1` |
| `{DisplayName#./}` | DisplayName | `.Device.Type.Tag.` | `Device/Type/Tag` |

#### 4.3.2 **首尾修剪行为**

替换完成后，结果字符串首尾的目标字符 Y 会被自动去除：

| Pattern | 属性值 | 替换后（修剪前） | 最终结果（修剪后） |
| --- | --- | --- | --- |
| `{Path#/_}` | `/Objects/Area/` | `_Objects_Area_` | `Objects_Area` |

#### 4.3.3 **组合使用**

| Pattern | 属性值 | 结果 |
| --- | --- | --- |
| `prefix_{DisplayName#_.}_suffix` | DisplayName=`a_b_c` | `prefix_a.b.c_suffix` |
| `{BrowseName#-.}({Description})` | BrowseName=`a-b`, Description=`desc` | `a.b(desc)` |
| `{DisplayName}` | DisplayName=`zs_p1` | `zs_p1`（普通替换，不转换） |

#### 4.3.4 **空值处理**

当属性值为 None 或空字符串时，`{Attr#XY}` 替换为空字符串：

| Pattern | 属性值 | 结果 |
| --- | --- | --- |
| `{DisplayName#_.}` | DisplayName=空 | （空） |
| `tag_{BrowseName}` | BrowseName=None | `tag_` |

### 4.4 **配置方式**

#### 4.4.1 **DSN 参数**

```plaintext
custom_tags=VARCHAR(1024)::location::{DisplayName#_.};VARCHAR(200)::browse::{BrowseName#-.}
```

#### 4.4.2 **CSV 文件**

```plaintext
...,tag::VARCHAR(1024)::location,tag::VARCHAR(200)::browse
...,{DisplayName#_.},{BrowseName#-.}
```

## 5. 性能

本功能为纯内存字符串操作，不引入 I/O 或网络调用。对现有点位映射流程的性能影响可忽略。

## 6. 安全

无安全影响。

## 7. 兼容性

- 完全向后兼容，不影响现有 `{Attr}` 普通占位符
- 不影响 `{id#XY}` 等点位 ID 占位符
- 不影响子表名 / 超级表名的占位符替换

## 8. 运维

无

## 9. 使用场景

### 9.1 **DisplayName 下划线转点号**

用户的 OPC 服务器中 DisplayName 使用下划线分隔层级（如 `zs_p1_unit1_float`），但 TDengine 标签中希望使用点号分隔（`zs.p1.unit1.float`）。
配置：`custom_tags=VARCHAR(1024)::display::{DisplayName#_.}`

### 9.2 **Path 斜杠转下划线**

OPC 节点路径为 `/Objects/Plant/Area1/Sensor01`，用户希望将路径作为标签写入，但需去除斜杠并用下划线替代。
配置：`custom_tags=VARCHAR(1024)::path::{Path#/_}`

### 9.3 **混合使用转换和原始值**

用户希望同时记录转换后的 DisplayName 和原始的 Description。
配置：`custom_tags=VARCHAR(1024)::dn::{DisplayName#_.};VARCHAR(1024)::desc::{Description}`

## 10. 约束和限制

### 10.1 约束

- X 和 Y 必须为单个 ASCII 字符
- 仅支持 BrowseName、DisplayName、Description、Path 四个属性

### 10.2 限制

- 不支持多字符替换（如将 `::` 替换为 `.`）
- 不支持正则表达式
- 不支持链式替换（如先替换 `_` 为 `.` 再替换 `-` 为 `/`），但可通过多个标签分别配置实现

## 11. 常见错误和排查

| 现象 | 可能原因 | 排查方法 |
| --- | --- | --- |
| `{Attr#XY}` 未被替换 | 属性名拼写错误或使用了不支持的属性 | 检查属性名是否为 BrowseName/DisplayName/Description/Path |
| 替换结果为空 | 属性值本身为空 | 检查 OPC 节点是否包含该属性 |
| 首尾字符被意外去除 | 修剪行为导致 | 这是预期行为，替换后会修剪首尾的目标字符 Y |

## 12. 可观测性

无。本功能不影响 UI 组件的行为，仅在自定义标签配置的 description 中增加了语法说明。

## 13. 安装和卸载

无

## 14. 文档

需要修改企业版文档

## 15. 参考文档

无

## 16. 附录

无
