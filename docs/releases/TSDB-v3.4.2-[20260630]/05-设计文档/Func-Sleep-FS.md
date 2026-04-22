# SLEEP 函数 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-13 | - | 0.1 | Stephen | 初稿 |

## 2. 背景

MySQL 提供 `SLEEP(duration)` 函数，允许在 SQL 查询中引入可控的等待时间，广泛用于测试查询超时机制、模拟慢查询、限流调试等场景。TSDB 目前缺少等价能力，导致以下问题无法在 SQL 层面直接验证：

1. 查询超时配置的正确性验证依赖外部工具，不能直接在 SQL 层测试。
2. 连接管理和慢查询日志的触发无法通过 SQL 内部手段模拟。
3. 功能测试中需要模拟延迟场景时，只能借助应用层 sleep，无法将延迟注入到 SQL 执行路径中。

为对齐 MySQL 生态使用习惯、增强测试能力并为后续 SQL 级别的流量控制预留入口，需要在 TSDB 中引入 `SLEEP` 函数。

## 3. 定义

1. **SLEEP(duration)**：标量函数，使当前查询线程挂起 `duration` 秒后继续执行。
2. **正常完成**：`duration` 秒到期后自然唤醒，函数返回 `0`。
3. **被中断**：被连接断开或超时机制终止时，查询以错误结束。
4. **duration**：实数，单位为秒，支持小数（精度至毫秒）；负数和 NULL 视为 0 处理，立即返回 `0`。

## 4. 行为说明

### 4.1 核心语义

> **SLEEP(duration) 的含义是：使当前 SQL 语句的执行线程暂停指定秒数，暂停结束后继续执行并返回 0；被超时机制终止时，查询以超时错误结束。负数和 NULL 视为 0，立即返回 0。**

### 4.2 适用范围

| 查询类型 | 是否支持 | 原因 |
| --- | --- | --- |
| 普通 `SELECT` | 是 | 标量上下文，语义明确 |
| 流式查询 / 连续查询 | 未限制 | 语法层不做拦截，但语义上不建议使用 |
| 嵌套子查询 | 是 | 与普通 SELECT 相同 |
| `SHOW` / 系统语句 | 否 | 系统命令不引入 SLEEP |

### 4.3 参数规则

| 参数值 | 行为 |
| --- | --- |
| `0` 或 `0.0` | 立即返回 `0`，无等待 |
| 正实数（如 `1.5`） | 等待对应秒数后返回 `0` |
| 负数 | 跳过睡眠，立即返回 `0` |
| `NULL` | 跳过睡眠，立即返回 `0` |
| 非数值类型 | 语义层报错：类型不匹配 |

### 4.4 返回值语义

| 场景 | 返回值 |
| --- | --- |
| 等待正常结束 | `0`（INT 类型） |
| 参数为负数 | `0`（立即返回，不等待） |
| 等待期间触发超时 | 查询以超时错误终止，通常不返回正常结果行 |
| 参数为 `NULL` | `0`（立即返回，不等待） |

### 4.5 与超时机制的交互

- SLEEP 期间，客户端 `readTimeout` 计时器持续计时；若到期，查询以超时错误终止，SLEEP 不会完成。
- 服务端 `queryNoFetchTimeoutSec` 从**最后一次响应**起计时，不适用于 `SELECT SLEEP(N)` 这类首包前阻塞场景（`lastAckTs <= 0` 时跳过超时清理）。
- `readTimeout` 场景下，SLEEP 可作为验证客户端超时配置正确性的工具。

### 4.6 并发与线程安全

- SLEEP 在执行线程上以 OS 级 sleep 实现，不占用 CPU。
- 挂起期间线程仍占用连接资源（线程池槽位），需防止大量并发 SLEEP 耗尽线程池。

### 4.7 边界场景

| 场景 | 预期行为 |
| --- | --- |
| `SLEEP(0)` | 立即返回 `0` |
| `SLEEP(NULL)` | 立即返回 `0`，不等待 |
| `SLEEP(-1)` | 立即返回 `0`，不等待 |
| `SLEEP(10)` 且 `readTimeout=5s` | 5s 后客户端超时，查询以超时错误终止 |
| 流式查询中使用 SLEEP | 语法层不拦截，但语义上不建议使用 |
| `SELECT SLEEP(1), SLEEP(1)` | 总等待 2s（串行求值） |
| `SELECT SLEEP(1) FROM t`（多行表） | SLEEP 对每行各求值一次（MySQL 兼容），总等待时间等于各行 duration 之和 |

## 5. 性能

1. SLEEP 以 OS 级 sleep 实现，CPU 开销接近零，但持续占用连接线程槽位。

## 6. 安全

1. **DoS 风险**：恶意用户可通过大量 `SLEEP(N)` 查询耗尽线程池。缓解措施依赖现有连接数限制和客户端 `readTimeout` 配置；`queryNoFetchTimeoutSec` 对首包前阻塞的 SLEEP 查询不适用。
2. **权限控制**：SLEEP 函数仅允许具有 `SELECT` 权限的用户调用，无需单独授权。

## 7. 兼容性

1. **新增行为**：SLEEP 为全新函数，不影响任何已有查询行为。
2. **升级兼容**：无历史数据格式变更，无需升级迁移。
3. **降级兼容**：旧版本不识别 SLEEP 函数，降级后相关查询报解析错误，符合预期。

## 8. 运维

1. SLEEP 函数无独立配置项；客户端超时控制依赖 `readTimeout` 配置。`queryNoFetchTimeoutSec` 从最后一次响应起计时，对首包前阻塞的 SLEEP 查询不适用。
2. 可通过 `SHOW QUERIES` 查看执行中的查询，`exec_usec` 列显示执行时长，`sql` 列显示原始 SQL。

## 9. 使用场景

```sql
-- 基本用法：等待 2 秒
SELECT SLEEP(2);
-- 返回：0

-- 验证读取超时配置（假设 readTimeout=3s）
SELECT SLEEP(10);
-- 预期：3 秒后超时报错，不等满 10 秒

-- 小数秒精度
SELECT SLEEP(0.5);
-- 等待 500ms 后返回 0

-- NULL 参数立即返回 0
SELECT SLEEP(NULL);
-- 返回：0
```

## 10. 约束和限制

1. 在纯标量查询（`SELECT SLEEP(N)`）中每条 SQL 执行一次；在表查询中，对每行各求值一次（MySQL 兼容），总等待时间等于各行 duration 之和。
2. 流式查询/连续查询语法层不拦截，但不建议使用。
3. 参数必须为数值或 NULL；非数值类型报错；负数和 NULL 立即返回 `0`，不等待。
4. SLEEP 仅在协调层执行，不下推到 vnode。

## 11. 常见错误和排查

1. **查询超时终止**：SLEEP 时长超过客户端读取超时（`readTimeout`，默认 900s），查询以超时错误终止，属正常行为。`queryNoFetchTimeoutSec` 从最后一次响应起计时，对 `SELECT SLEEP(N)` 首包前阻塞场景不适用。

## 12. 可观测性

1. `SHOW QUERIES` 显示执行中的查询，exec_usec 列显示执行时长，sql 列显示原始 SQL。

## 13. 安装和卸载

无。SLEEP 作为内置标量函数随版本发布，无需单独安装或卸载。

## 14. 文档

1. 需要在用户文档"标量函数"章节新增 `SLEEP` 条目。

## 15. 参考文档

1. MySQL 8.0 SLEEP 函数文档：https://dev.mysql.com/doc/refman/8.0/en/miscellaneous-functions.html#function_sleep
2. MariaDB SLEEP 函数文档：https://mariadb.com/kb/en/sleep/
3. `community/source/libs/parser/inc/sql.y`
4. `community/source/libs/parser/src/parTranslater.c`
5. `community/source/libs/scalar/src/sclfunc.c`

## 16. 附录

实现细节、测试覆盖规划及风险回滚策略见设计文档：[Func-Sleep-DS.md](Func-Sleep-DS.md)。
