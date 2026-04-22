# SLEEP 函数 DS

## 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-13 | - | 0.1 | Stephen | 初稿 |
| 2026-04-14 | - | 0.2 | Stephen | 更正表查询为每行执行一次语义，移除相关错误设计描述 |

## 1. 引言

### 1.1 目的

本文档给出 TSDB `SLEEP` 函数的详细实现设计，目标是：

- 支持 FS 文档中描述的所有功能；
- 定义 `SLEEP` 函数在各层的改动清单、分类标志；
- 给出测试覆盖规划与风险回滚策略。

### 1.2 范围

本文覆盖以下内容：

- 各层改动清单（Parser 层、Function 层、Planner 层）。
- 函数分类标志设计。
- 测试覆盖规划。
- 风险分析与回滚策略。

### 1.3 受众

- TSDB 内核研发。
- QA。

### 1.4 关联文档

- 功能规格：[Func-Sleep-FS.md](Func-Sleep-FS.md)

## 2. 实现设计

### 2.1 各层改动清单

**Function 层（`source/libs/function/`）：**

1. 在 `functionMgtInt.h` 新增两个分类标志位：
   - `FUNC_MGT_VOLATILE_FUNC`（bit 35）：标记函数不可常量折叠（如 SLEEP）。
   - `FUNC_MGT_NO_PUSHDOWN_FUNC`（bit 36）：标记函数必须在协调层执行，不下推到 vnode。
2. 在 `functionMgt.h` 新增枚举值 `FUNCTION_TYPE_SLEEP`，并声明两个查询接口：
   - `fmIsVolatileFunc(int32_t funcId)`
   - `fmIsNoPushdownFunc(int32_t funcId)`
3. 在 `functionMgt.c` 实现上述两个接口。
4. 在 `builtins.c` 注册 `sleep` 函数定义：
   - 分类：`FUNC_MGT_SCALAR_FUNC | FUNC_MGT_VOLATILE_FUNC | FUNC_MGT_NO_PUSHDOWN_FUNC`
   - 参数：1 个，支持数值类型及 NULL；NULL 和负数均立即返回 `0`，不等待
   - 返回类型：INT（由 `translateSleep()` 在翻译阶段设置）
   - 执行函数：`sleepFunction()`（在 `source/libs/scalar/src/sclfunc.c` 中实现）

**Parser 层（`source/libs/parser/`）：**

1. 在 `parTranslater.c` 的 `translateNormalFunction()` 中，函数翻译成功后检测 `fmIsVolatileFunc`，若为真则置 `pCxt->hasVolatileFunc = true`。
2. 在 `setQuery()` 中，`hasVolatileFunc` 为真时阻止将查询设置为 `QUERY_EXEC_MODE_LOCAL`，确保含 `SLEEP` 的无表查询（如 `SELECT SLEEP(1)`）也走正常执行路径而非本地短路执行。

**Planner 层（`source/libs/planner/`）：**

1. 在 `planPhysiCreater.c` 的 `doRewritePrecalcExprs()` 中，对标量函数做预计算重写时，额外排除 `fmIsNoPushdownFunc` 和 `fmIsVolatileFunc` 的函数，防止 SLEEP 被下推或在计划阶段被折叠。

**Executor 层（`source/libs/executor/`）：**

1. `projectApplyFunctions()` 和 `projectApplyFunctionsWithSelect()` 新增 `SExecTaskInfo* pTaskInfo` 参数，在调用前将 `pTaskInfo` 和 `isTaskKilled` 回调写入 threadlocal `gTaskScalarExtra`，以便 `sleepFunction()` 在执行时感知查询取消状态。
2. `filterSetExecContext()` 新增接口，将 `pTaskInfo` 和 `isTaskKilled` 注入过滤器上下文，确保 WHERE 子句中的 SLEEP 也能被正确中断。

**Scalar 层（`include/libs/scalar/scalar.h`）：**

1. 新增函数指针类型 `sclIsTaskKilled`（`typedef bool (*sclIsTaskKilled)(void*)`）。
2. `SScalarExtraInfo` 新增两个字段：
   - `void* pTaskInfo`：任务句柄，传给 `isTaskKilled` 回调。
   - `sclIsTaskKilled isTaskKilled`：由 executor 注入的取消检测回调。

## 3. 测试覆盖规划

| 测试类别 | 内容 |
| --- | --- |
| 正向 - 基本 | `SLEEP(0)`、`SLEEP(1)`、`SLEEP(0.5)` 返回值和实际耗时验证 |
| 正向 - NULL | `SLEEP(NULL)` 立即返回 `0`，不等待 |
| 正向 - 负数 | `SLEEP(-1)` 立即返回 `0`，实际不等待 |
| 正向 - 超时交互 | `readTimeout < duration`，超时后查询以超时错误终止 |
| 正向 - 无表查询 | `SELECT SLEEP(1)` 正常执行，不走 LOCAL 短路路径 |
| 正向 - 表查询多行 | `SELECT SLEEP(0.1) FROM t`（3 行），验证每行各睡眠一次（MySQL 兼容），总耗时约 0.3s（0.1s × 3），NULL 行跳过睡眠 |
| 负向 - 非数值参数 | `SLEEP('abc')` 类型报错 |

## 4. 风险与回滚策略

### 4.1 主要风险

1. **DoS 风险**：无内置并发 SLEEP 数量限制，大量并发 SLEEP 查询可耗尽线程池。缓解依赖现有连接数限制和客户端 `readTimeout` 配置；`queryNoFetchTimeoutSec` 对首包前阻塞的 SLEEP 查询不适用。
2. **误用风险**：用户在无表查询中误配置超长 duration，导致意外阻塞。

### 4.2 风险控制

1. 文档明确说明表查询中每行各睡眠一次（MySQL 兼容）；NULL 和负数参数均立即返回 `0`。
2. `FUNC_MGT_NO_PUSHDOWN_FUNC` 确保 SLEEP 仅在协调层执行，不扩散到 vnode，限制影响范围。

### 4.3 回滚策略

1. SLEEP 为新增函数，回滚只需从函数注册表中移除 `sleep` 条目，不影响任何已有查询。
2. 新增的 `FUNC_MGT_VOLATILE_FUNC` 和 `FUNC_MGT_NO_PUSHDOWN_FUNC` 分类位对其他函数无影响。
3. 无数据格式变更，无需数据迁移。
