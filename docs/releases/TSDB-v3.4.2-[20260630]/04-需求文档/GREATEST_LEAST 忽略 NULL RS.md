# GREATEST/LEAST 支持忽略 NULL RS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-24 | 2026-04-24 | 1.0 | Simon Guan | 初始版本 |

## 2. 引言

### 2.1 术语与缩写名词

| 术语 | 定义 |
| --- | --- |
| GREATEST | 返回输入参数中最大值的标量函数 |
| LEAST | 返回输入参数中最小值的标量函数 |
| NULL | 数据库中表示"无值"或"未知"的特殊标记 |

### 2.2 相关文档资料

| 文档 | 说明 |
| --- | --- |
| [greatest_least 函数 FS](../../TSDB-v3.3.6-[20250331]/03-设计文档/Engine/greatest_least%20函数%20FS.md) | v3.3.6 版本 GREATEST/LEAST 函数设计文档 |
| [greatest_least 函数测试 TS](../../TSDB-v3.3.6-[20250331]/04-测试文档/Engine/greatest_least%20函数测试%20TS.md) | v3.3.6 版本 GREATEST/LEAST 函数测试文档 |
| [MySQL GREATEST/LEAST 文档](https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#function_greatest) | MySQL 官方行为参考 |

### 2.3 优先级要求

中优先级。时序数据场景中列值 NULL（传感器掉线、数据缺失）非常常见，该功能可显著提升用户体验。期望在 v3.4.2 版本交付。

### 2.4 版本要求

- 开源版本与企业版均支持
- 目标发布版本：TSDB v3.4.2

## 3. 需求目标

TDengine v3.3.6 实现了 `GREATEST`/`LEAST` 函数，当前行为与 MySQL 一致：**任意参数为 NULL 则结果为 NULL**。

在时序数据场景中，数据列出现 NULL 值（传感器掉线、采集延迟、数据缺失等）是常态。用户在使用 `GREATEST`/`LEAST` 对多列求最大/最小值时，期望能跳过 NULL 值，仅在有效的非 NULL 值中进行比较。

**目标**：新增客户端配置项 `ignoreNullInGreatest`，允许用户控制 `GREATEST`/`LEAST` 函数在遇到 NULL 参数时的行为——默认保持与 MySQL 一致（遇 NULL 返回 NULL），可配置为忽略 NULL 参数。

## 4. 功能需求

| 序号 | **功能类别** | **功能名称** | 功能描述 |
| --- | --- | --- | --- |
| 1 | 配置项 | `ignoreNullInGreatest` 配置项 | 新增客户端配置项，控制 GREATEST/LEAST 函数是否忽略 NULL 参数 |
| 2 | 函数行为 | 忽略 NULL 模式 | 当配置值为 1 时，GREATEST/LEAST 跳过 NULL 参数，仅对非 NULL 值比较 |
| 3 | 函数行为 | 全 NULL 返回 NULL | 忽略 NULL 模式下，若所有参数均为 NULL，返回 NULL |
| 4 | 兼容性 | 默认行为不变 | 配置项默认值为 0，保持与 MySQL 一致的现有行为 |

### 4.1 配置项详细说明

| 属性 | 值 |
| --- | --- |
| 名称 | `ignoreNullInGreatest` |
| 作用范围 | 客户端配置（与 `compareAsStrInGreatest` 一致） |
| 数据类型 | INT |
| 取值范围 | `0` 或 `1` |
| 默认值 | `0` |

- **`0`（默认）**：与 MySQL 对齐，任意参数为 NULL 则返回 NULL
- **`1`**：忽略 NULL 参数，仅在非 NULL 值中进行比较；若所有参数均为 NULL，返回 NULL

### 4.2 行为示例

假设表 `t1` 中某行数据：`col1 = 3, col2 = NULL, col3 = 7`

#### ignoreNullInGreatest = 0（默认，MySQL 兼容）

```sql
SELECT GREATEST(col1, col2, col3) FROM t1;  -- 结果: NULL
SELECT LEAST(col1, col2, col3) FROM t1;     -- 结果: NULL
```

#### ignoreNullInGreatest = 1（忽略 NULL）

```sql
SELECT GREATEST(col1, col2, col3) FROM t1;  -- 结果: 7
SELECT LEAST(col1, col2, col3) FROM t1;     -- 结果: 3
```

#### 所有参数为 NULL（无论配置如何）

```sql
SELECT GREATEST(NULL, NULL) FROM t1;         -- 结果: NULL
```

#### 常量与列混合

```sql
-- ignoreNullInGreatest = 1
SELECT GREATEST(col1, NULL, 10) FROM t1;     -- 结果: 10 (忽略常量 NULL，比较 3 和 10)
SELECT LEAST(NULL, col3, 5) FROM t1;         -- 结果: 5 (忽略 NULL，比较 7 和 5)
```

### 4.3 与现有配置项的交互

`ignoreNullInGreatest` 与 `compareAsStrInGreatest` **正交、互不影响**：

| 配置项 | 控制内容 |
| --- | --- |
| `compareAsStrInGreatest` | 数值类型与字符串类型混合时的比较策略 |
| `ignoreNullInGreatest` | 是否跳过 NULL 参数 |

两者可同时设置，各自独立生效。忽略 NULL 仅影响 NULL 参数的处理，不改变非 NULL 值之间的比较规则。

### 4.4 比较规则（不变）

非 NULL 值之间的比较规则保持不变：

- 所有参数都是字符串类型，按照字符串类型比较
- 所有参数都是数值类型，按照数值类型比较
- 混合类型根据 `compareAsStrInGreatest` 配置项决定
- 不同类型比较时，选择范围更大的类型

## 5. 性能需求

- 新增 NULL 跳过逻辑不应引入可观测的性能退化
- 当 `ignoreNullInGreatest = 0`（默认）时，性能与现有实现一致，无额外开销

## 6. 安全需求

无特殊安全需求。配置项遵循现有客户端配置的权限管理机制。

## 7. 其他需求

### 7.1 兼容性需求

- **向后兼容**：默认值 `0` 确保升级后现有行为不变，不影响已有业务
- **无协议变更**：纯客户端行为，不涉及客户端-服务端通信协议变更

### 7.2 接口需求

- 配置项支持通过 `taos.cfg` 文件设置
- 配置项支持通过连接参数设置

### 7.3 运维需求

无特殊运维需求。

### 7.4 易用性需求

- 配置项命名与现有 `compareAsStrInGreatest` 风格一致，降低学习成本
- 官方文档中 GREATEST/LEAST 函数说明需同步更新

### 7.5 测试需求

| 序号 | 测试场景 | 说明 |
| --- | --- | --- |
| 1 | 默认行为（配置为 0） | 验证与现有行为一致，任意 NULL 返回 NULL |
| 2 | 忽略 NULL（配置为 1） | 部分参数为 NULL，结果为非 NULL 值中的最大/最小 |
| 3 | 全 NULL 参数 | 无论配置如何，返回 NULL |
| 4 | 常量 NULL 与列 NULL 混合 | 验证常量 NULL 和列 NULL 均被正确忽略 |
| 5 | 两个配置项交互 | 同时设置 `ignoreNullInGreatest` 和 `compareAsStrInGreatest`，验证正交性 |
| 6 | 多种数据类型 | 数值类型、字符串类型分别验证忽略 NULL 的正确性 |
| 7 | 边界情况 | 只有一个非 NULL 参数时，返回该参数值 |
| 8 | 性能回归 | 默认配置下无性能退化 |
