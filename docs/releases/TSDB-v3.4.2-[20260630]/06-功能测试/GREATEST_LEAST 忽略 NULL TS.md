# GREATEST / LEAST 函数忽略 NULL TS

# 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-23 | - | 0.1 | Stephen | 初稿 Test Spec，覆盖功能测试设计 |
| 2026-05-07| - | 1.0 | Simon Guan | 发布 |

# 测试目标

- 覆盖 FS 定义的 GREATEST / LEAST 全部行为：基本语义、类型推导、NULL 传播、配置项切换、列与标量广播。
- 覆盖各种类型组合：纯数值、纯字符串、数值+字符串混合，配合 `compareAsStrInGreatest` 两种取值。
- 覆盖 v3.4.2 新增 `ignoreNullInGreatest` 配置项的全部行为：默认兼容、忽略 NULL 模式、全 NULL、与 `compareAsStrInGreatest` 正交性。
- 覆盖不支持范围：参数个数不足、不可比较类型、行数不匹配。
- 为后续自动化回归提供稳定、可扩展的全覆盖基线。

# 参考文档

- Func-GreatestLeast-FS.md
- 《GREATEST_LEAST 忽略 NULL RS》（`04-需求文档/`）
- 在线文档：`community/docs/{en,zh}/14-reference/03-taos-sql/22-function.md`（GREATEST / LEAST 章节）

# 测试结论

- 全部 47 条用例已自动化并通过 ASan 模式下的 `community/test/ci/pytest.sh` 验证：
  - 基础 34 条（GTL-G/L/N/T/CFG/COL/ERR + 边界 GTL-BND）位于 `community/test/cases/11-Functions/01-Scalar/test_fun_sca_greatest_least.py`。
  - `ignoreNullInGreatest` 新增 12 条（GTL-IGN-001..012）位于 `community/test/cases/11-Functions/01-Scalar/test_fun_sca_greatest_least_ignorenull.py`。
  - 性能回归 1 条（GTL-PERF-001）位于 `community/test/cases/11-Functions/01-Scalar/test_fun_sca_greatest_least_perf.py`。
- 三个测试文件均通过（0 ASan 错误，0 内存泄漏）。
- 功能测试设计用例：47 条（基础 31 条 + 边界 3 条 + `ignoreNullInGreatest` 新增 12 条 + 性能回归 1 条）。
- 覆盖目标：
  - 功能覆盖：FS §4（行为说明）、§4.6（配置项，含 `ignoreNullInGreatest`）全覆盖。
  - 类型覆盖：FS §4.4（返回类型推导表）每行至少一条用例。
  - 不支持范围覆盖：参数个数 < 2、不可比较类型、行数不匹配。
  - RS §7.5 测试需求矩阵 8 项全覆盖（含第 8 条性能回归）。

# 测试环境

- OS: Linux x86_64（Ubuntu 22.04+）
- TDengine: 企业版 v3.4.2.0+
- 关键配置：
  - `compareAsStrInGreatest`（客户端，默认 1）
  - `ignoreNullInGreatest`（客户端，默认 0，v3.4.2 新增）
- 测试框架：`new_test_framework`，测试文件 `community/test/cases/11-Functions/01-Scalar/test_fun_sca_greatest_least_ignorenull.py`

# 功能测试

## 1 基本功能 — GREATEST

### 测试要点

- 数值参数返回最大值。
- 字符串参数按字典序返回最大值。
- 至少 2 个参数。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| GTL-G-001 | 数值最大值 | `SELECT GREATEST(3,12,34,8,25)` 返回 34。 | 已通过 |
| GTL-G-002 | 两个参数 | `SELECT GREATEST(1, 2)` 返回 2。 | 已通过 |
| GTL-G-003 | 浮点与整型混合 | `SELECT GREATEST(1, 2.5, 3)` 返回 3，类型为 DOUBLE。 | 已通过 |
| GTL-G-004 | 负数 | `SELECT GREATEST(-5, -1, -10)` 返回 -1。 | 已通过 |
| GTL-G-005 | 字符串字典序 | `SELECT GREATEST('apple','banana','cherry')` 返回 `'cherry'`。 | 已通过 |
| GTL-G-006 | 字符串多元素 | `SELECT GREATEST('cherry','apple','banana')` 返回 `'cherry'`。 | 已通过 |

## 2 基本功能 — LEAST

### 测试要点

- 数值参数返回最小值。
- 字符串参数按字典序返回最小值。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| GTL-L-001 | 数值最小值 | `SELECT LEAST(3,12,34,8,25)` 返回 3。 | 已通过 |
| GTL-L-002 | 浮点最小值 | `SELECT LEAST(1.5, 2, 0.5)` 返回 0.5。 | 已通过 |
| GTL-L-003 | 字符串字典序 | `SELECT LEAST('banana','apple','cherry')` 返回 `'apple'`。 | 已通过 |
| GTL-L-004 | 全负数 | `SELECT LEAST(-1, -2, -3)` 返回 -3。 | 已通过 |

## 3 NULL 处理

### 测试要点

- 任一入参为 `NULL` 字面量 → 整列 `NULL`。
- 行级 `NULL` 传播：`NULL` 行返回 `NULL`，非 `NULL` 行正常计算。
- 全 `NULL` 参数返回 `NULL`。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| GTL-N-001 | 含 NULL 字面量 | `SELECT GREATEST(1, NULL, 5)` 返回 `NULL`。 | 已通过 |
| GTL-N-002 | LEAST 含 NULL | `SELECT LEAST(1, NULL, 5)` 返回 `NULL`。 | 已通过 |
| GTL-N-003 | 全 NULL | `SELECT GREATEST(NULL, NULL)` 返回 `NULL`。 | 已通过 |
| GTL-N-004 | 列含 NULL 行 | 表 `t1(v1, v2)`，部分行 `v1` 为 `NULL`；`SELECT GREATEST(v1, v2) FROM t1` 中 `v1` 为 `NULL` 的行结果为 `NULL`，其它行为两值最大。 | 已通过 |

## 4 类型推导

### 测试要点

- 全数值 → 提升到最宽数值类型。
- 全字符串 → 字符串类型，长度为最长参数。
- 数值与字符串混合行为受 `compareAsStrInGreatest` 控制。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| GTL-T-001 | INT/BIGINT 提升 | `SELECT GREATEST(CAST(1 AS INT), CAST(2 AS BIGINT))` 返回类型为 BIGINT。 | 已通过 |
| GTL-T-002 | INT/DOUBLE 提升 | `SELECT GREATEST(1, 2.0)` 返回类型为 DOUBLE。 | 已通过 |
| GTL-T-003 | VARCHAR 长度 | `SELECT GREATEST('a', 'abcdef')` 返回类型 VARCHAR，长度 ≥ 6。 | 已通过 |
| GTL-T-004 | VARCHAR / NCHAR 混合 | `SELECT GREATEST(CAST('a' AS NCHAR(10)), 'b')` 类型推导成功，结果为 `'b'`。 | 已通过 |

## 5 配置项 `compareAsStrInGreatest`

### 测试要点

- 默认 `1`（按字符串比较）：`GREATEST(2, '10')` 返回 `2`（`'2' > '10'`）。
- 切换到 `0`（按数值比较）：`GREATEST(2, '10')` 返回 `10`。
- 配置项可动态修改，立即生效，无需重启。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| GTL-CFG-001 | 默认按字符串比较 | 默认配置下 `SELECT GREATEST(2, '10')` 返回 `'2'`。 | 已通过 |
| GTL-CFG-002 | 默认 LEAST | 默认配置下 `SELECT LEAST(2, '10')` 返回 `'10'`。 | 已通过 |
| GTL-CFG-003 | 切换为数值比较 | `ALTER LOCAL 'compareAsStrInGreatest' '0'` 后 `SELECT GREATEST(2, '10')` 返回 `10`。 | 已通过 |
| GTL-CFG-004 | 切换后返回类型 | `compareAsStrInGreatest=0` 时 `SELECT GREATEST(CAST(1 AS DOUBLE), '2.5')` 返回类型为 DOUBLE，值为 2.5（INT + VARCHAR 走 `vectorGetConvertType` 推导为 BIGINT，会截断 `'2.5'`；要触发 DOUBLE 提升需至少一个 DOUBLE 输入）。 | 已通过 |
| GTL-CFG-005 | 切换回默认 | `ALTER LOCAL 'compareAsStrInGreatest' '1'` 后行为恢复。 | 已通过 |

## 6 列与表查询

### 测试要点

- 列引用支持。
- 列与标量混合按行广播。
- 多列 GREATEST/LEAST 在 WHERE 中使用。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| GTL-COL-001 | 多列 GREATEST | 表 `m(temperature, humidity, pressure)`，`SELECT GREATEST(temperature, humidity, pressure) FROM m` 每行返回三列最大值。 | 已通过 |
| GTL-COL-002 | 列+标量广播 | `SELECT GREATEST(v, 0) FROM t1`，`v` 为列；负值行返回 0，正值行返回 `v`。 | 已通过 |
| GTL-COL-003 | WHERE 使用 | `SELECT * FROM metrics WHERE GREATEST(cpu_usr, cpu_sys, cpu_io) > 80` 仅返回任一 CPU 字段超过 80 的行。 | 已通过 |
| GTL-COL-004 | 与 LEAST 同时投影 | `SELECT GREATEST(a,b) AS hi, LEAST(a,b) AS lo FROM t1` 返回两列，`hi >= lo` 恒成立（无 NULL 行）。 | 已通过 |

## 7 不支持范围

### 测试要点

- 参数个数 < 2 报错。
- 不可比较类型（BLOB / JSON / GEOMETRY）报类型错误。
- 行数不匹配且都不为 1 报内部错误。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| GTL-ERR-001 | 仅一个参数 | `SELECT GREATEST(1)` 报参数个数错误。 | 已通过 |
| GTL-ERR-002 | 无参数 | `SELECT GREATEST()` 报参数个数错误。 | 已通过 |
| GTL-ERR-003 | LEAST 仅一个参数 | `SELECT LEAST(1)` 报参数个数错误。 | 已通过 |
| GTL-ERR-004 | 不可比较类型 | `SELECT GREATEST(json_col, 1) FROM t_json` 报类型错误。 | 已通过 |

## 7.5 边界与规模用例（PR Round-2 补充）

### 测试要点

- 空表 + 全 NULL 列：触发 `greatestLeastImpl` 中 `numOfRows==0` 快速返回路径，验证零行输出且无内存分配 / 无崩溃。
- 多列入参：50 个常量参数验证 `vectorCompareAndSelect` 的逐列扫描在宽变参下的正确性与解析。
- 常量与多行列混合：验证 `vectorCompareAndSelect` 中 `numOfRows==1?0:i` 广播行索引在常量与多行列同时存在时的正确性。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| GTL-BND-001 | 空表 + 全 NULL 列 | 空表 `t_empty(a int, b int)` 上：`SELECT GREATEST(a, b)`、`SELECT LEAST(a, NULL, b)`（默认 `ignoreNullInGreatest=0`）以及 `SELECT GREATEST(a, b, NULL)`（`ignoreNullInGreatest=1`）均返回 0 行；守护 `numOfRows==0` 快速返回路径不分配内存且不崩溃。 | 已通过 |
| GTL-BND-002 | 多列入参（50 列） | `SELECT GREATEST(1,2,...,50)` 返回 50；`SELECT LEAST(1,2,...,50)` 返回 1；附加 NULL 字面量时默认配置返回 NULL，`ignoreNullInGreatest=1` 仍返回 50；验证宽变参扫描与解析。 | 已通过 |
| GTL-BND-003 | 常量与多行列混合广播 | 多行表 `t1(v,a,b)` 上：`SELECT GREATEST(10, v, a) FROM t1` 每行都返回 10；`SELECT LEAST(10, v, a) FROM t1` 逐行返回 -3、4、0；`SELECT GREATEST(100, v, a, b) FROM t1` 每行都返回 100；守护常量参数 `numOfRows=1` 与列参数交错时的广播行索引读取。 | 已通过 |

## 8 配置项 `ignoreNullInGreatest`（v3.4.2 新增）

### 测试要点

- 默认值 `0`：行为与历史版本完全一致，任一 NULL 入参 → 结果 NULL。
- 切换到 `1`：跳过 NULL 入参，仅在非 NULL 值中比较。
- 全 NULL（无论配置如何）始终返回 NULL。
- 常量 NULL 和列 NULL 均能被正确忽略。
- 与 `compareAsStrInGreatest` 正交、互不影响。
- 配置项动态生效，无需重启。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| GTL-IGN-001 | 默认值兼容 | 默认 `ignoreNullInGreatest=0`，`SELECT GREATEST(1, NULL, 5)` 返回 `NULL`；`SELECT LEAST(1, NULL, 5)` 返回 `NULL`。 | 已通过 |
| GTL-IGN-002 | 忽略常量 NULL — GREATEST | `ALTER LOCAL 'ignoreNullInGreatest' '1'` 后，`SELECT GREATEST(1, NULL, 5)` 返回 `5`。 | 已通过 |
| GTL-IGN-003 | 忽略常量 NULL — LEAST | `ignoreNullInGreatest=1` 时，`SELECT LEAST(1, NULL, 5)` 返回 `1`；`SELECT LEAST(NULL, 7, 5)` 返回 `5`。 | 已通过 |
| GTL-IGN-004 | 忽略列 NULL | 表 `t1`，某行 `col1=3, col2=NULL, col3=7`；`ignoreNullInGreatest=1` 时，`SELECT GREATEST(col1, col2, col3) FROM t1` 该行返回 `7`，`SELECT LEAST(col1, col2, col3) FROM t1` 该行返回 `3`。 | 已通过 |
| GTL-IGN-005 | 列 NULL + 常量混合 | `ignoreNullInGreatest=1` 时，`SELECT GREATEST(col1, NULL, 10) FROM t1`（`col1=3`）返回 `10`；`SELECT LEAST(NULL, col3, 5) FROM t1`（`col3=7`）返回 `5`。 | 已通过 |
| GTL-IGN-006 | 全 NULL — 配置无关 | 不论 `ignoreNullInGreatest` 取 `0` 或 `1`，`SELECT GREATEST(NULL, NULL)` 与 `SELECT LEAST(NULL, NULL)` 均返回 `NULL`；某行所有列均为 NULL 时，该行结果为 `NULL`。 | 已通过 |
| GTL-IGN-007 | 与 `compareAsStrInGreatest` 正交 | 同时启用 `ignoreNullInGreatest=1` 验证两条独立路径：(1) 与 `compareAsStrInGreatest=0` 组合，`SELECT GREATEST(2, '10', NULL)` 返回数值 `10`（VARCHAR `'10'` 被转换为数值后按数值比较，且跳过 NULL）；(2) 与 `compareAsStrInGreatest=1` 组合，同一查询返回 `'2'`（按字符串比较且跳过 NULL）。 | 已通过 |
| GTL-IGN-008 | 字符串类型忽略 NULL | `ignoreNullInGreatest=1` 时，`SELECT GREATEST('apple', NULL, 'cherry')` 返回 `'cherry'`；`SELECT LEAST('banana', NULL, 'cherry')` 返回 `'banana'`。 | 已通过 |
| GTL-IGN-009 | 边界：仅一个非 NULL 参数 | `ignoreNullInGreatest=1` 时，`SELECT GREATEST(NULL, NULL, 5)` 返回 `5`；`SELECT LEAST(NULL, 7, NULL)` 返回 `7`。覆盖 RS §7.5 第 7 条。 | 已通过 |
| GTL-IGN-010 | 单列剩余 + 行级 NULL | `ignoreNullInGreatest=1` 时，仅有一列在翻译阶段存活（如 `GREATEST(NULL, col1)`），且该列在某行取值为 NULL；该行结果应仍为 `NULL`，col1 非 NULL 行返回该值。覆盖 `effectiveNum=1` + 行级 NULL 的运行时路径。 | 已通过 |
| GTL-IGN-011 | 类型化常量 NULL 广播安全 | `CAST(NULL AS INT)` 是 `numOfRows=1` 的运行时常量参数（非 `TSDB_DATA_TYPE_NULL` 字面量，翻译阶段不会被剔除）。多行表 `tbcast(v)` 上：(1) 默认 `ignoreNullInGreatest=0` 时 `SELECT GREATEST(CAST(NULL AS INT), v) FROM tbcast` 每行返回 `NULL`；(2) `ignoreNullInGreatest=1` 时同查询返回各行 `v` 值。守护 `vectorCompareAndSelect` 中常量参数空值位图广播读取（避免越界读）。 | 已通过 |
| GTL-IGN-012 | NULL 字面量 + 多行列 默认配置 | 默认 `ignoreNullInGreatest=0` 时，含 NULL 字面量的查询输出类型在翻译阶段被定为 `TSDB_DATA_TYPE_NULL`。多行表 `tnlit(v)` 上 `SELECT GREATEST(v, NULL) FROM tnlit` 与 `SELECT LEAST(v, NULL) FROM tnlit` 必须将输出列**所有 3 行**都置为 `NULL`，而非仅第 0 行。守护 `greatestLeastImpl` 中 `IsNullType` 短路不应跳过驱动列行数计算。 | 已通过 |

## 9 性能回归（v3.4.2 新增）

### 测试要点

- 默认配置（`ignoreNullInGreatest=0`）下新增 NULL 跳过逻辑无可观测性能退化（RS §5、§7.5 第 8 条）。
- 启用 `ignoreNullInGreatest=1` 引入的额外分支开销在合理范围内。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| GTL-PERF-001 | 默认配置无性能退化 | 在 100K 行表上对四个 INT 列执行 `SELECT GREATEST(c1,c2,c3,c4) FROM t`：(1) 默认 `ignoreNullInGreatest=0` 下端到端耗时不超过 30s 上限（CI 容忍阈值，主要捕获回归性退化而非精确基线）；(2) `ignoreNullInGreatest=1` 下相同查询耗时不超过默认模式的 3 倍。覆盖 RS §7.5 第 8 条。 | 已通过 |
