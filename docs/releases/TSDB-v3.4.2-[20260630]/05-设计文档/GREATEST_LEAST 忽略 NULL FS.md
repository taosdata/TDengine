# GREATEST / LEAST 函数忽略 NULL FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-23 | - | 0.1 | Stephen | 初稿 |
| 2026-05-07| - | 1.0 | Simon Guan | 发布 |

## 2. 背景

MySQL、PostgreSQL、Oracle 等主流数据库均提供 `GREATEST` / `LEAST` 函数，用于在表达式列表中按行求最大/最小值，常用于多列比较、阈值裁剪、跨字段排名等场景。TSDB 自 v3.3.6.0 起已支持这两个函数，本 FS 对其行为进行规范化描述，作为后续测试与文档的输入。

时序数据场景中列值为 NULL（传感器掉线、采集延迟、数据缺失）非常常见，遇 NULL 即返回 NULL 的 MySQL 兼容行为不利于多列求极值的实际使用。v3.4.2 起新增客户端配置项 `ignoreNullInGreatest`，允许用户选择在比较中跳过 NULL 参数（参见《GREATEST_LEAST 忽略 NULL RS》）。

## 3. 定义

1. **GREATEST(expr1, expr2[, ...])**：标量函数，返回参数列表中的最大值。
2. **LEAST(expr1, expr2[, ...])**：标量函数，返回参数列表中的最小值。
3. **比较类型**：根据所有参数类型推导出的统一比较类型，由 `compareAsStrInGreatest` 配置项影响混合类型时的行为。
4. **NULL 传播**：只要某行任一入参为 `NULL`，该行结果即为 `NULL`，与 MySQL 行为一致（默认行为；启用 `ignoreNullInGreatest` 后可跳过 NULL，见 §4.5）。

## 4. 行为说明

### 4.1 核心语义

> **GREATEST/LEAST 的含义是：对每一行，将所有参数转换到统一比较类型后，返回最大/最小值；任一参数为 NULL 时该行结果为 NULL（默认行为；可通过 `ignoreNullInGreatest` 配置项改变此行为，见 §4.5）。**

### 4.2 适用范围

| 查询类型 | 是否支持 | 说明 |
| --- | --- | --- |
| 普通 `SELECT` 投影 | 是 | 标量上下文 |
| `WHERE` 子句 | 是 | 与普通 SELECT 相同 |
| `GROUP BY` / `ORDER BY` | 是 | 作为表达式使用 |
| 嵌套子查询 | 是 | 与普通 SELECT 相同 |
| 流式查询 | 是 | 标量函数按行求值 |
| 聚合上下文 | 否 | 该函数不是聚合，不与 GROUP BY 同列直接组合 |

### 4.3 参数规则

| 参数项 | 取值 | 说明 |
| --- | --- | --- |
| 参数个数 | ≥ 2 | 少于 2 个在翻译阶段报错 |
| 参数类型 | 数值类型（含 BOOL、TIMESTAMP、整型、浮点、DECIMAL）、字符串类型（VARCHAR、NCHAR） | 不支持 BLOB、JSON 等不可比较类型 |
| 参数节点 | 常量、列引用、任意标量表达式 | 可混合 |

### 4.4 返回类型推导

返回类型由所有参数类型共同决定，受配置项 `compareAsStrInGreatest`（默认 `1`）控制：

| 输入类型组合 | `compareAsStrInGreatest` | 返回类型 |
| --- | --- | --- |
| 任一参数为 `NULL` 字面量类型 | 任意 | `NULL` 类型 ¹ |
| 全部同类型 | 任意 | 该类型 |
| 全部为数值 | 任意 | 通过 `vectorGetConvertType` 提升为最宽数值类型 |
| 全部为字符串 | 任意 | 字符串类型，长度为最长参数的 bytes |
| 数值 + 字符串混合 | `1`（默认） | `VARCHAR`，长度 = `MAX(字符串长度, 25)` |
| 数值 + 字符串混合 | `0` | 字符串被转为数值，返回提升后的数值类型 |

实现位于 `translateGreatestleast()`。

> ¹ 当 `ignoreNullInGreatest=1` 时，NULL 字面量参数被剔除，返回类型按剩余非 NULL 参数推导；若全部参数均为 NULL 字面量，仍返回 `NULL` 类型。

### 4.5 NULL 处理

NULL 处理行为受新增配置项 `ignoreNullInGreatest`（默认 `0`）控制：

**`ignoreNullInGreatest = 0`（默认，MySQL 兼容）：**

- 翻译阶段：若任一参数类型为 `TSDB_DATA_TYPE_NULL`（即 NULL 字面量），整列置为 NULL。
- 执行阶段：对每一行，若任一入参的该行值为 `NULL`，结果置为 `NULL`，不参与后续比较（`vectorCompareAndSelect` 中遇 NULL 即 `break`，对应 `resultColIndex[i] = -1`）。

**`ignoreNullInGreatest = 1`（忽略 NULL）：**

- 翻译阶段：参数中含 `TSDB_DATA_TYPE_NULL` 字面量时，**不再**整列置 NULL，仅将其从有效参数集合中剔除；返回类型按剩余参数推导。若只含 NULL 字面量参数，则整列置 NULL。
- 执行阶段：对每一行，跳过 `NULL` 入参，仅在非 NULL 值之间比较：
  - 若至少存在一个非 NULL 入参，结果为非 NULL 值中的最大/最小值。
  - 若所有入参在该行均为 `NULL`，结果为 `NULL`。
- 该模式与 `compareAsStrInGreatest` **正交、互不影响**：忽略 NULL 仅影响 NULL 参数的处理，非 NULL 值之间仍按 4.4 的类型推导规则比较。

### 4.6 配置项

| 名称 | 取值 | 默认 | 作用域 | 动态 | 说明 |
| --- | --- | --- | --- | --- | --- |
| `compareAsStrInGreatest` | `0` / `1` | `1` | 客户端 | 是 | 控制数值与字符串混合时的比较类型 |
| `ignoreNullInGreatest` | `0` / `1` | `0` | 客户端 | 是 | 控制 GREATEST/LEAST 是否跳过 NULL 入参；`0`=遇 NULL 返回 NULL（MySQL 兼容），`1`=忽略 NULL，仅在非 NULL 值中比较 |

两项配置正交：`compareAsStrInGreatest` 决定非 NULL 值之间的比较类型，`ignoreNullInGreatest` 决定是否跳过 NULL。两者可同时设置，各自独立生效。

修改示例：

```sql
ALTER LOCAL 'compareAsStrInGreatest' '0';
ALTER LOCAL 'ignoreNullInGreatest' '1';
```

### 4.7 边界场景

| 场景 | `ignoreNullInGreatest` | 预期行为 |
| --- | --- | --- |
| `GREATEST(1)` / `LEAST(1)` | 任意 | 翻译阶段报参数个数错误 |
| `GREATEST(1, NULL, 5)` | `0`（默认） | 返回 `NULL` |
| `GREATEST(1, NULL, 5)` | `1` | 返回 `5`（跳过 NULL） |
| `LEAST(NULL, 7, 5)` | `1` | 返回 `5`（跳过 NULL） |
| `GREATEST(NULL, NULL)` | 任意 | 返回 `NULL` |
| 行级 NULL：`GREATEST(col1, col2)`，某行 `col1=NULL, col2=3` | `0`（默认） | 该行返回 `NULL` |
| 行级 NULL：同上 | `1` | 该行返回 `3` |
| 行级全 NULL：所有入参在该行均为 NULL | `1` | 该行返回 `NULL` |
| 仅一个非 NULL 参数：`GREATEST(NULL, NULL, 5)` | `1` | 返回 `5`（单一非 NULL 值即为最大/最小值） |
| 全字符串：`GREATEST('apple','banana','cherry')` | 任意 | 返回 `'cherry'`（字典序） |
| 全数值：`GREATEST(1, 2.5, 3)` | 任意 | 返回 `3`（提升为 DOUBLE） |
| 混合默认：`GREATEST(2, '10')` | 任意 | 返回 `'2'`（按字符串比较，`'2' > '10'`） |
| 混合 `compareAsStrInGreatest=0`：`GREATEST(2, '10')` | 任意 | 返回 `10`（按数值比较） |
| 两项同时开启：`GREATEST(2, '10', NULL)`，`compareAsStrInGreatest=0` 且 `ignoreNullInGreatest=1` | — | 返回 `10`（跳过 NULL，按数值比较） |
| 列与标量混合行数：`GREATEST(col, 0)` | 任意 | 标量按行广播 |
| 行数不一致且都不为 1 | 任意 | 内部错误 `TSDB_CODE_TSC_INTERNAL_ERROR` |
| 不可比较类型（BLOB / JSON / GEOMETRY） | 任意 | 翻译阶段报类型错误 |

## 5. 性能

- 计算复杂度：每行 `O(N)`，N 为参数个数；不引入额外网络往返。
- 类型转换：仅当存在异构类型时调用 `vectorConvertSingleCol`，单参数级别一次转换。
- 与下推：标量函数，可下推到 vnode；不阻塞算子流水线。
- `ignoreNullInGreatest = 0`（默认）路径与现有实现一致，无任何额外开销；`= 1` 路径仅在 NULL 检测分支增加跳过逻辑，开销可忽略。

## 6. 安全

- 不读写元数据，无权限新增。
- 字符串比较按字节序，不引入用户区域设置敏感性。
- 不向外暴露内部错误码细节。

## 7. 兼容性

- 自 TDengine v3.3.6.0 起支持基础 GREATEST / LEAST，社区版与企业版语义一致。
- `ignoreNullInGreatest` 默认值 `0` 保证升级到 v3.4.2 后行为与历史版本完全一致，对现有业务零影响。
- `ignoreNullInGreatest = 0` 时，与 MySQL `GREATEST` / `LEAST` 在数值与 NULL 行为上完全兼容；字符串比较按字典序对齐 MySQL 默认行为。
- `compareAsStrInGreatest = 0` 时，混合类型语义对齐 PostgreSQL 数值优先策略。
- 纯客户端配置变更，无客户端-服务端协议变更，无数据格式变更。

## 8. 运维

- 无新增系统表、无新增运维命令。
- 配置项 `compareAsStrInGreatest` 已存在于配置项表。
- 配置项 `ignoreNullInGreatest` 与 `compareAsStrInGreatest` 风格一致，按客户端配置注册（`CFG_SCOPE_CLIENT, CFG_DYN_CLIENT`），可通过 `taos.cfg` 或连接参数设置，变更无需重启。
