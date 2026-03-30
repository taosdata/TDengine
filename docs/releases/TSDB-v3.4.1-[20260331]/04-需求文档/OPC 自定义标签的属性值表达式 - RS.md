# OPC 自定义标签的属性值表达式 - RS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026/3/17 | 2026-03-17 | 1.0 | @杨志宇 | 初始版本 |

## 2. 引言

### 2.1 术语与缩写名词

- **OPC UA / OPC DA**：工业自动化领域的数据通信协议
- **自定义标签（Custom Tags）**：OPC UA / OPC DA / KingHistorian / pSpace 等数据源支持对每个数据点位配置自定义的 Tag，即用户可以自行定义 Tag 的名称、数据类型和 Tag 值。自定义标签可通过两种方式配置：一是在 CSV 点位配置文件中，通过 `tag::<DataType>::<TagName>` 列定义；二是在"选择数据点位"模式下，通过 DSN 参数 `custom_tags` 的 Tag 表达式定义。
- **属性值替换**：每种数据源的数据点位拥有各自特有的属性值。例如 OPC 的属性包括 DisplayName、Description、BrowseName、Path 等，KingHistorian 的属性包括 tag_name、description、group_name 等。属性值替换是指在配置自定义标签的 Tag 值时，使用 `{Attr}` 或 `{Attr#XY}` 表达式引用数据点位的属性值。其中 `{Attr}` 直接引用属性原始值，`{Attr#XY}` 在引用的同时将属性值中的字符 X 替换为字符 Y，并修剪结果首尾的 Y 字符。
- **Pattern（表达式）**：自定义标签值的模板字符串，可包含静态文本和占位符（如 `{Attr}`、`{Attr#XY}`）
- **OpcNode**：OPC 节点的属性集合，包含 BrowseName、DisplayName、Description、Path 等

### 2.2 相关文档资料

- 

### 2.3 优先级要求

- 优先级：高

### 2.4 版本要求

- 建议在下一可发布版本交付。
- 开源/企业版范围待产品侧确认。

## 3. 需求目标

在工业场景中，OPC 节点的属性值（如 DisplayName、BrowseName、Path）通常使用特定分隔符（如 `_`、`-`、`/`）来组织层级结构。例如：
- DisplayName: `zs_p1_unit1_float`
- BrowseName: `zs-p1-unit1`
- Path: `/Objects/Plant/Area1/`
当用户将这些属性值作为 TDengine 子表标签写入时，往往需要将分隔符替换为其他字符以满足命名规范或可读性要求。例如将 `zs_p1_unit1_float` 转换为 `zs.p1.unit1.float`。
本需求提供 `{Attr#XY}` 语法，允许用户在自定义标签的 pattern 表达式中对属性值进行单字符替换，无需修改 OPC 服务器端的数据。

## 4. 功能需求

| 序号 | 功能类别 | 功能名称 | 功能描述 |
| --- | --- | --- | --- |
| 1 | 占位符语法 | `{Attr#XY}` 替换语法 | 在 pattern 中使用 `{Attr#XY}` 将属性 Attr 的值中所有字符 X 替换为字符 Y，并修剪首尾的 Y 字符 |
| 2 | 占位符语法 | 支持的属性范围 | 支持 BrowseName、DisplayName、Description、Path 四个属性 |
| 3 | 占位符语法 | 单字符替换 | X 和 Y 均为单个字符，支持任意可打印 ASCII 字符 |
| 4 | 占位符语法 | 首尾修剪 | 替换完成后自动去除结果字符串首尾的目标字符 Y |
| 5 | 占位符组合 | 与静态文本组合 | `{Attr#XY}` 可与静态文本自由组合，如 `prefix_{DisplayName#_.}_suffix` |
| 6 | 占位符组合 | 与普通占位符组合 | 同一 pattern 中可同时使用 `{Attr#XY}` 和 `{Attr}`，如 `{BrowseName#-.}({Description})` |
| 7 | 优先级 | 替换顺序 | `{Attr#XY}` 优先于 `{Attr}` 处理，避免普通占位符先替换导致 `#XY` 语法失效 |
| 8 | 空值处理 | 属性为空时的行为 | 当属性值为空（None 或空字符串）时，`{Attr#XY}` 替换为空字符串 |
| 9 | 配置方式 | DSN 参数配置 | 通过 `custom_tags` DSN 参数配置，格式 `<DataType>::<TagName>::<Pattern>` |
| 10 | 配置方式 | CSV 文件配置 | 在 CSV 点位配置文件的 `tag::<DataType>::<TagName>` 列中使用 pattern 表达式 |
| 11 | 前端展示 | UI 说明文本 | 在 OPC UA、OPC DA、KingHistorian 的前端配置界面中展示 `{Attr#XY}` 语法说明 |

## 5. 性能需求

- `{Attr#XY}` 替换为纯字符串操作，单次替换耗时应在微秒级别
- 不引入额外的网络请求或 I/O 操作
- 对现有点位映射流程的性能影响可忽略

## 6. 安全需求

无。本功能仅涉及字符串替换，不涉及用户输入的执行、网络通信或敏感数据处理。

## 7. 其他需求

### 7.1 兼容性需求

- 向后兼容：不影响现有的 `{Attr}` 普通占位符语法
- 不影响 `{id#XY}` 等点位 ID 占位符的行为
- 不影响子表名、超级表名的占位符替换逻辑

### 7.2 接口需求

无新增接口。复用现有的 DSN 参数和 CSV 配置方式。

### 7.3 运维需求

无。

### 7.4 易用性需求

- 前端配置界面的 description 中需包含 `{Attr#XY}` 语法说明和示例
- 设计文档中需包含完整的语法说明、示例表格和组合使用示例

### 7.5 测试需求（不含测试例）

- 单元测试覆盖 `replace_attr_with_transform()` 核心函数
- 单元测试覆盖 `extra_custom_tags()` 端到端流程
- 测试场景包括：常见替换、空值处理、混合占位符、多标签同时替换
