# SLEEP 函数 TS

# 1 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-14 | 2026-04-22 | 1.0 | Stephen | 初稿 Test Spec，覆盖功能测试设计 |


# 2 测试目标

- 覆盖 FS 定义的 SLEEP 函数全部行为：基本睡眠、返回值、NULL/负数/零参数、多行表查询、非推下执行、超时中断机制。
- 验证 DS 描述的实现正确性：`FUNC_MGT_VOLATILE_FUNC` 防常量折叠、`FUNC_MGT_NO_PUSHDOWN_FUNC` 不下推到 vnode、每行执行一次语义。
- 覆盖所有不支持范围：非数值参数。
- 为后续自动化回归提供稳定、可扩展的全覆盖基线。


# 3 参考文档

- Sleep 函数 FS.md
- Sleep 函数 DS.md


# 4 测试结论

- 当前文档为测试规格与执行计划，测试结果列先标记为"待执行"。
- 功能测试设计用例：23 条。
- 覆盖目标：
  - 功能覆盖：FS §4（行为说明）、§5（性能）、§6（安全）、§8（运维）全覆盖。
  - 设计覆盖：DS §2.1（各层改动）全量设计点覆盖。
  - 不支持范围覆盖：非数值参数全覆盖。


# 5 测试环境

- OS: Linux x86_64（Ubuntu 22.04+）
- TDengine: 企业版 v3.4.2.0+
- 关键配置：
  - `readTimeout`（客户端读取超时，默认 900s）
- 测试框架：`new_test_framework`，测试文件 `test/cases/11-Functions/06-System/test_fun_sys_sleep.py`


# 6 功能测试

## 6.1 基本功能

### 6.1.1 测试要点

- 正数 duration 返回 0 且实际耗时与参数匹配。
- 零值立即返回 0。
- 返回值类型为 INT。

### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| SLP-BASIC-001 | SLEEP(0.2) 返回值与耗时 | `SELECT SLEEP(0.2)` 返回 1 行，值为 0，耗时 0.15s–1.0s。 | 待执行 |
| SLP-BASIC-002 | SLEEP(0) 立即返回 | `SELECT SLEEP(0)` 返回 1 行，值为 0，耗时 < 0.5s。 | 待执行 |
| SLP-BASIC-003 | SLEEP(0.1) 返回值与耗时 | `SELECT SLEEP(0.1)` 返回 1 行，值为 0，耗时 0.05s–0.8s。 | 待执行 |
| SLP-BASIC-004 | SLEEP(1) 整数参数 | `SELECT SLEEP(1)` 返回 1 行，值为 0，耗时 0.9s–2.0s。 | 待执行 |
| SLP-BASIC-005 | SLEEP 用于表达式 | `SELECT SLEEP(0) + 1` 返回 1；`SELECT SLEEP(0) = 0` 返回 1（true）。 | 待执行 |
| SLP-BASIC-006 | SLEEP(-1) 在表达式中 | `SELECT SLEEP(-1) + 1` 返回 1（负数返回 0）。 | 待执行 |

## 6.2 NULL 与负数参数

### 6.2.1 测试要点

- NULL 参数立即返回 0，不等待。
- 负数参数（整数/小数）立即返回 0，不等待。

### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| SLP-NULL-001 | SLEEP(NULL) 立即返回 0 | `SELECT SLEEP(NULL)` 返回 1 行，值为 0，耗时 < 0.5s。 | 待执行 |
| SLP-NULL-002 | SLEEP(-1) 立即返回 0 | `SELECT SLEEP(-1)` 返回 1 行，值为 0，耗时 < 0.5s。 | 待执行 |
| SLP-NULL-003 | SLEEP(-0.5) 立即返回 0 | `SELECT SLEEP(-0.5)` 返回 1 行，值为 0，耗时 < 0.5s。 | 待执行 |

## 6.3 表查询与多行场景

### 6.3.1 测试要点

- 表查询每行均返回 0，SLEEP 按行求值。
- 多行时总耗时约等于各行 duration 之和（MySQL 兼容：每行一次）。
- NULL 列值对应行返回 0，跳过睡眠，不计入总耗时。
- 空表查询返回 0 行，无报错。

### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| SLP-TBL-001 | 多行表查询返回值与行数 | `SELECT SLEEP(0.05), v FROM t1`（3 行），返回 3 行，值均为 0，无报错。 | 待执行 |
| SLP-TBL-002 | 多行表查询耗时 | 3 行各 v=0.1；总耗时 0.25s–1.5s（约 0.3s）。 | 待执行 |
| SLP-TBL-003 | 含 NULL 列多行查询 | v=[0.1, NULL, 0.2]；3 行值均为 0；NULL 行不睡眠，总耗时 0.25s–1.5s（约 0.3s）。 | 待执行 |
| SLP-TBL-004 | 空表查询 | `SELECT SLEEP(v) FROM t1`（空表），返回 0 行，无报错。 | 待执行 |

## 6.4 不支持范围

### 6.4.1 测试要点

- SLEEP() 无参数、非数值参数、参数过多均报错（类型/语法错误）。

### 6.4.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| SLP-ERR-001 | 无参数 | `SELECT SLEEP()` 报错。 | 待执行 |
| SLP-ERR-002 | 非数值字符串参数 | `SELECT SLEEP('abc')` 报类型不匹配错误。 | 待执行 |
| SLP-ERR-003 | 参数过多 | `SELECT SLEEP(1, 2)` 报错。 | 待执行 |

## 6.5 无表查询（防 LOCAL 短路）

### 6.5.1 测试要点

- `SELECT SLEEP(N)` 无表查询必须走正常执行路径，不走 `QUERY_EXEC_MODE_LOCAL` 短路。
- `FUNC_MGT_VOLATILE_FUNC` 标志保证 `hasVolatileFunc=true` 时阻止 LOCAL 模式。

### 6.5.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| SLP-LOCAL-001 | 无表查询正常执行 | `SELECT SLEEP(0.2)` 无 FROM 子句，正常返回 0，耗时 0.15s–1.0s（确认未被短路折叠）。 | 待执行 |
| SLP-LOCAL-002 | 多列无表查询串行 | `SELECT SLEEP(0.1), SLEEP(0.1)` 耗时 0.15s–1.0s（约 0.2s，串行求值，非并行）。 | 待执行 |

## 6.6 不下推到 vnode（NO_PUSHDOWN）

### 6.6.1 测试要点

- `FUNC_MGT_NO_PUSHDOWN_FUNC` 确保 SLEEP 在协调层串行执行。
- 多 vgroup 场景：总耗时应等于各行 duration 之和，而非并行缩短。

### 6.6.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| SLP-NOPD-001 | 多 vgroup 串行执行 | 2 个 vgroup，4 行各 v=0.2；`SELECT SLEEP(v) FROM st`，耗时 0.6s–3.0s（约 0.8s），确认非并行下推。 | 待执行 |

## 6.7 WHERE 子句中使用

### 6.7.1 测试要点

- SLEEP 在 WHERE 子句中可按行求值。
- SLEEP(0)=0 为假值，过滤所有行。

### 6.7.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| SLP-WHERE-001 | WHERE SLEEP(0) 过滤全行 | `SELECT v FROM t1 WHERE SLEEP(0)`，返回 0 行。 | 待执行 |
| SLP-WHERE-002 | WHERE SLEEP(v) 耗时验证 | 3 行各 v=0.05；`WHERE SLEEP(v)` 耗时 0.1s–1.0s（约 0.15s），返回 0 行（SLEEP 返回 0 为假值）。 | 待执行 |

## 6.8 超时机制交互

### 6.8.1 测试要点

- 客户端 `readTimeout` 先于 SLEEP 到期时，查询以超时错误终止。
- `queryNoFetchTimeoutSec` 从最后一次响应起计时，对 `SELECT SLEEP(N)` 首包前阻塞场景不适用，不用于本节测试验证。
- SLEEP 可作为验证 `readTimeout` 客户端超时配置正确性的工具。

### 6.8.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| SLP-TIMEOUT-001 | websocket timeout < duration | 通过 websocket DSN 参数 `timeout=2000ms`，执行 `SELECT SLEEP(30)`；约 2s 内以超时错误终止。若 websocket 连接器不支持 `timeout` DSN 参数，记录提示日志并跳过（测试不失败）。 | 待执行 |

## 6.9 可观测性（SHOW QUERIES）

### 6.9.1 测试要点

- 执行中的 SLEEP 查询应在 `performance_schema.perf_queries` 可见，`sql` 列含原始 SQL。
- `exec_usec` 列非零，反映 SLEEP 已执行时长。

### 6.9.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| SLP-SLOW-001 | perf_queries 显示执行中 SLEEP | 后台线程执行 `SELECT SLEEP(5)`；主线程轮询 `SELECT kill_id, exec_usec, sql FROM performance_schema.perf_queries`，5s 内可见该查询，`sql` 列含 `sleep(5)`，`exec_usec` 非零。 | 待执行 |
