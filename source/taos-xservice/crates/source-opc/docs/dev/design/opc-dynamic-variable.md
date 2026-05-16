# OPC UA Variable 节点分类与 Dynamic Variable 映射规则

> 源码位置：
> - 分类逻辑（Go）：`plugins/opc/client/opcua/classify.go`
> - 节点数据结构（Go）：`plugins/opc/common/point.go`
> - Rust 侧数据结构：`taosx-core/src/plugins/runners/opc/points.rs`
> - 映射生成：`taosx-core/src/plugins/sink/point/model.rs` — `PointMappingGenerator::generate`
>
> 相关 PR：https://github.com/taosdata/taosx/pull/3871

## 背景与问题

### 旧版行为

在 PR #3871 之前，`taosx-opc`（Go 侧）在 BFS 遍历 OPC UA 地址空间时，将**所有 `NodeClass=Variable` 的节点**都视为动态数据点（Dynamic Variable），对每个节点：

1. 下发到 `collect` 订阅，定期采集时序值
2. 在 TDengine 中创建独立的子表

### 问题

OPC UA 规范中，`Variable` 节点有两种语义：

| 类型 | 说明 | 示例 |
|------|------|------|
| **Dynamic Variable** | 承载过程值（process value），值随时间变化 | 温度、压力、流量传感器读数 |
| **Property** | 父 Variable 的**静态元数据**，值基本不变 | `EURange`（量程范围）、`EngineeringUnits`（工程单位）、`ValuePrecision`（精度） |

旧版把 Property 也当成 Dynamic Variable 处理，导致：
- **无效子表膨胀**：每个 Property（如 `EURange`、`EngineeringUnits`）都单独建一张子表，子表数量成倍膨胀
- **无用订阅**：Property 的值几乎不变，订阅它们浪费 OPC Server 资源和网络带宽
- **元数据与数据分离**：用户无法在同一张表中看到传感器的值和它的元数据（量程、单位等）

### 新版行为

PR #3871 引入**节点分类机制**，在 BFS 阶段区分 Dynamic Variable 和 Property：

- **Dynamic Variable** → 建独立子表，订阅时序数据（与旧版行为一致）
- **Property** → **不建子表、不订阅**，读取一次当前值后合并为父 Dynamic Variable 子表的 **Tag**

## 节点分类规则

### 分类函数

```go
// classify.go
func Classify(parentNodeClass ua.NodeClass, refType *ua.NodeID, typeDef *ua.NodeID) (ClassifyResult, string)
```

分类仅针对 `NodeClass=Variable` 的节点（Object 节点由主循环单独处理，走 `opc_object` 超级表通道）。

### 四级分类规则

规则按优先级从高到低依次判定，首个命中即返回：

| 优先级 | 规则 | 条件 | 结果 | 说明 |
|--------|------|------|------|------|
| 1 | rule2-HasProperty | 父节点是 Variable **且** Reference 类型是 `HasProperty`（i=46） | **Property** | OPC UA 规范：HasProperty 引用的子节点是父的元数据 |
| 2 | rule3-PropertyType | TypeDefinition 是 `PropertyType`（i=68） | **Property** | 节点自身的类型定义表明它是 Property |
| 3 | rule3-ItemType | TypeDefinition 属于 ItemType 白名单 | **DynamicVariable** | 标准数据访问类型，承载过程值 |
| 4 | rule4-fallback | 以上都不命中 | **DynamicVariable** | 兜底，记录 WARN 日志 |

### ItemType 白名单

以下 TypeDefinition（均在命名空间 0 下）被识别为 Dynamic Variable：

| TypeDefinition | 说明 |
|---------------|------|
| `BaseDataVariableType` | 所有数据变量的基类型 |
| `DataItemType` | 数据项 |
| `AnalogItemType` | 模拟量（连续值） |
| `AnalogUnitRangeType` | 带单位和范围的模拟量 |
| `AnalogUnitType` | 带单位的模拟量 |
| `DiscreteItemType` | 离散量基类 |
| `TwoStateDiscreteType` | 两态离散量（开/关） |
| `MultiStateDiscreteType` | 多态离散量 |
| `MultiStateValueDiscreteType` | 带数值的多态离散量 |
| `ArrayItemType` | 数组型 |
| `YArrayItemType` / `XYArrayItemType` | Y 数组 / XY 数组 |
| `ImageItemType` / `CubeItemType` / `NDimensionArrayItemType` | 多维数组 |

> 自定义命名空间下的 TypeDefinition 不在白名单中，走兜底规则（rule4-fallback），视为 Dynamic Variable。

## 数据流变化

### BFS 遍历阶段（Go 侧）

```
BFS 遍历 OPC UA 地址空间
  │
  ├── NodeClass = Object → 走 opc_object 通道（不变）
  │
  └── NodeClass = Variable
        │
        ├── Classify() → Property
        │     ├── 标记 IsProperty = true
        │     ├── 加入 propertyReadTask 队列
        │     └── 不加入 result（不下发订阅）
        │
        └── Classify() → DynamicVariable
              ├── 标记 IsProperty = false（或不填）
              ├── 加入 result（下发到 collect 订阅）
              └── Properties map 由 readAndAttachProperties 回填
```

### Property 值收集

BFS 每批处理结束后，调用 `readAndAttachProperties`：

1. 批量 `Read` 所有 Property 节点的当前 Value
2. 将值序列化为字符串（由 `serializePropertyValue` 处理）
3. 回填到父 Dynamic Variable 的 `Properties` map 中

**序列化策略**：

| 值类型 | 序列化方式 | 示例 |
|--------|-----------|------|
| `nil` | 空字符串 | `""` |
| `bool` / 数值 / `string` | `fmt.Sprintf("%v")` | `"42"`, `"true"`, `"hello"` |
| `time.Time` | RFC3339Nano | `"2025-06-01T12:30:45Z"` |
| `LocalizedText` | 取 `.Text` 字段 | `"degree Celsius"` |
| `QualifiedName` | 取 `.Name` 字段 | `"EURange"` |
| struct / slice / map | JSON 序列化 | `'{"Low":0,"High":100}'` |

### Point 数据结构变化

```go
// common/point.go
type Point struct {
    // ... 原有字段 ...

    // 新增字段
    IsProperty bool              `json:"is_property,omitempty"` // 是否为 Property 节点
    Properties map[string]string `json:"properties,omitempty"`  // 父 Variable 的 Property 名→值
}
```

## Rust 侧映射生成

### OpcNode 新增字段

```rust
// points.rs
pub struct OpcNode {
    // ... 原有字段 ...
    pub is_property: Option<bool>,
    pub properties: Option<HashMap<String, String>>,
}
```

通过 `DataSet` 的 `OptionSet` 进行 Go↔Rust 序列化传输：
- `IsProperty` → `OptionSet { name: "IsProperty", display: "true"/"false" }`
- `Properties` → `OptionSet { name: "Properties", display: "{JSON字符串}" }`

### generate() 两遍扫描

`PointMappingRule::generate()` 改为**两遍扫描**：

#### 第一遍：过滤 + 收集 Tag Union

```
遍历所有 DataSet
  │
  ├── NodeClass ≠ Variable → 跳过
  │
  ├── is_property = true → 跳过（Property 不建子表）
  │
  └── 动态 Variable
        ├── 收集该节点 properties 的所有 key → tag_union (BTreeSet)
        └── 检查 Property 名与 custom_tags 是否重名 → 重名则 bail 报错
```

**Tag Union**：所有动态 Variable 的 Property 名的**并集**。使用 `BTreeSet` 保证顺序稳定。

例如：
- Variable A 有 Properties: `{EURange, EngineeringUnits}`
- Variable B 有 Properties: `{EURange, ValuePrecision}`
- Tag Union = `{EURange, EngineeringUnits, ValuePrecision}`

#### 第二遍：生成 PointConfig + TableConfig

```
遍历所有动态 Variable
  │
  ├── gen_point_config() → 子表名、超级表名、custom_tag 值
  │
  ├── 补齐 tag_values：
  │     对 tag_union 中每个 Tag 名：
  │       - 该节点 properties 中有值 → 截断到 1024 字节后写入
  │       - 该节点 properties 中无值 → 写入空字符串 ""
  │
  ├── gen_table_config() → 列结构 + Tag 结构
  │     └── 追加 opc_extra_tag_configs：tag_union 中每个名字 → VARCHAR(1024) Tag
  │
  └── 写入 point_map / table_map
```

### 设计决策

| 编号 | 决策 | 说明 |
|------|------|------|
| #1 | Tag 名 = OPC Property 的 BrowseName | 直接使用原始名称；与 `custom_tags` 重名时报错（bail），不做隐式重命名 |
| #2 | 全部 VARCHAR(1024) | Property 值已在 Go 侧序列化为字符串，复杂结构体为 JSON 字符串。统一用 `VARCHAR(1024)` 足够且简单 |
| #6 | Tag schema 一次性 union 定型 | 在 `generate()` 阶段收集所有动态 Variable 的 Property 名并集，所有子表使用相同的 Tag 列表；缺值的 Variable 对应 Tag 填空串 |

### 值截断

Property 值写入 `VARCHAR(1024)` Tag 前，由 `truncate_utf8_bytes` 在 UTF-8 字符边界处预防性截断到 1024 字节，避免 TDengine 端硬截断导致乱码。

## TDengine 表结构示例

### 旧版（PR 前）

假设 OPC UA Server 有一个传感器 `ns=2;s=Sensor01`（Float），附带两个 Property：`EURange` 和 `EngineeringUnits`。

```
旧版：3 张子表
──────────────────────────────────────────────

超级表 opc_float (ts, val FLOAT, quality INT)
  └── 子表 t_2_Sensor01                        ← 传感器动态值 ✓

超级表 opc_varchar (ts, val VARCHAR, quality INT)
  ├── 子表 t_2_Sensor01_EURange                ← EURange 静态值 ✗ 无意义
  └── 子表 t_2_Sensor01_EngineeringUnits       ← 单位名 静态值 ✗ 无意义
```

### 新版（PR 后）

```
新版：1 张子表，Property 合并为 Tag
──────────────────────────────────────────────

超级表 opc_float (
    ts TIMESTAMP,
    val FLOAT,
    quality INT
) TAGS (
    ...,                              ← custom_tags（用户配置的）
    EURange VARCHAR(1024),            ← 来自 OPC Property
    EngineeringUnits VARCHAR(1024)    ← 来自 OPC Property
)
  └── 子表 t_2_Sensor01
        TAGS(..., '{"Low":0,"High":100}', 'degree Celsius')
```

**效果**：
- 子表数量从 3 → 1（减少 67%）
- 传感器的元数据（量程、单位）与时序数据在同一张表中，查询时无需 JOIN
- 不再订阅 Property 节点，减少 OPC Server 负载

### 多 Variable Tag Union 示例

```
Variable A: Properties = {EURange: "0~100", EngineeringUnits: "°C"}
Variable B: Properties = {EURange: "0~500", ValuePrecision: "0.1"}

Tag Union = {EURange, EngineeringUnits, ValuePrecision}

超级表 opc_float (...) TAGS (
    EURange VARCHAR(1024),
    EngineeringUnits VARCHAR(1024),
    ValuePrecision VARCHAR(1024)
)

子表 t_A: TAGS('0~100', '°C',  '')      ← ValuePrecision 缺值填空串
子表 t_B: TAGS('0~500', '',    '0.1')   ← EngineeringUnits 缺值填空串
```

## 兼容性

### 新旧版本升级兼容

新版代码（PR #3871 后）对 OPC UA 点位的映射规则发生了**本质变化**：旧版每个 Variable 节点对应一个子表，新版仅 Dynamic Variable 对应子表，Property 节点的值合并为 Tag。这意味着新旧版本生成的超级表结构（Tag 列数量和语义）**不兼容**。

**升级策略**：

| 场景 | 行为 |
|------|------|
| 旧任务 + 旧 database | **正常运行**。旧任务的 `PointModelConfig` 从已保存的任务配置（JSON）加载，不会重新走 OPC 插件的 classify 流程，超级表结构不变，数据照常写入 |
| 新任务 + 新 database | **推荐做法**。升级 taosx 后，创建新的 OPC UA 任务，指向一个新的 database，使用新的分类规则和表结构 |
| 新任务 + 旧 database | **不推荐**。旧超级表只有 5 个标签列（如 name, BrowseName, DisplayName, Description, Path），新版子表 SQL 引用了额外的 Property 标签列（如 EURange, EngineeringUnits），标签列数不匹配会导致运行时建表失败 |

**升级建议**：

1. 升级 taosx 后，**旧任务无需任何修改**即可继续运行
2. 如需使用新的 Dynamic Variable 分类功能，创建新的 OPC UA 任务，指定新的目标 database
3. 旧 database 中的历史数据保留，可继续查询
4. 旧任务和新任务可以并行运行，互不影响

### 向后兼容（代码层面）

- 老版本 Go（无 `is_property` / `properties` 字段）的 JSON 输出，Rust 侧 `serde(default)` 保证反序列化不报错，`is_property` / `properties` 均为 `None`
- 当所有 Variable 的 `properties` 都为空时（老版本行为），`tag_union` 为空集，`generate()` 不会追加任何额外 Tag，行为与旧版完全一致

### OPC DA / KingHistorian / Pspace

此次分类机制**仅影响 OPC UA**（分类逻辑在 `plugins/opc/client/opcua/classify.go`）。OPC DA、KingHistorian、Pspace 的数据源端无 Property 概念或处理方式不同，映射行为保持不变。

## 与其他文档的关系

| 文档 | 关联 |
|------|------|
| [`point-model.md`](./point-model.md) | PointModelConfig 的完整结构，包含 Tag 生成规则 |
| [`handle-point-message-init.md`](./handle-point-message-init.md) | 使用 generate() 的结果预建超级表和子表 |
| [`consume_point_record.md`](./consume_point_record.md) | 运行时写入数据，涉及自动建表和 ALTER TABLE |
| [`custom-tags.md`](./custom-tags.md) | 用户自定义 Tag（custom_tags），与 OPC Property 衍生 Tag 共存但不可重名 |
| [`opc_to_taos.md`](./opc_to_taos.md) | 顶层任务入口，调用链中触发 generate() |
