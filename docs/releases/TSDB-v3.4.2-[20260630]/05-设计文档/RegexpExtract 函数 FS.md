# REGEXP_EXTRACT 函数 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-16 | 2026-4-28 | 1.0 | Stephen | 初稿 |


## 2. 背景

Hive、Spark SQL、Trino 等主流 SQL 引擎均提供 `REGEXP_EXTRACT` 函数，用于从字符串中提取正则表达式匹配的子串或指定捕获组的内容，广泛应用于日志解析、数据清洗、结构化字段提取等场景。TSDB 目前缺少该能力，导致以下问题：

1. 日志字段中的子串提取依赖客户端后处理，无法在 SQL 层完成。
2. 基于正则匹配的派生列（如提取 IP 地址、错误码、指标名称）需要额外的应用层处理。
3. 与 Hive / Spark 生态的 SQL 迁移成本较高。

为对齐主流 SQL 生态、增强字符串处理能力，需要在 TSDB 中引入 `REGEXP_EXTRACT` 函数。


## 3. 定义

1. **REGEXP_EXTRACT(str, pattern[, group_idx])**：标量函数，对字符串 `str` 应用正则表达式 `pattern`，返回第一次匹配中第 `group_idx` 个捕获组的内容；`group_idx` 缺省时默认为 `1`（即第一个捕获组）。
2. **匹配成功**：在 `str` 中找到 `pattern` 的匹配，返回对应子串或捕获组内容。
3. **未匹配**：未在 `str` 中找到匹配，函数返回 `NULL`。
4. **group_idx**：0 到 512 之间的整数或 `NULL`，`0` 表示完整匹配子串，`1` 表示第一个捕获组，以此类推；超出实际捕获组数时返回 `NULL`；`NULL` 为合法值，向全部输出行传播 `NULL`；超过 512 在翻译阶段报参数错误。


## 4. 行为说明

### 4.1 核心语义

> **REGEXP_EXTRACT(str, pattern, group_idx) 的含义是：在字符串 str 中搜索 pattern 的第一个匹配，返回该匹配中第 group_idx 个捕获组的内容；若 str、pattern 或 group_idx 为 NULL，或无匹配则返回 NULL；group_idx 默认为 1（第一个捕获组）。**

### 4.2 适用范围

| 查询类型 | 是否支持 | 原因 |
| --- | --- | --- |
| 普通 `SELECT` | 是 | 标量上下文，语义明确 |
| `WHERE` 子句 | 是 | 与普通 SELECT 相同 |
| 嵌套子查询 | 是 | 与普通 SELECT 相同 |
| `GROUP BY` / `ORDER BY` | 是 | 作为表达式使用 |
| 流式查询 / 连续查询 | 是 | 标量函数，按行求值 |
| `SHOW` / 系统语句 | 否 | 系统命令不引入字符串函数 |

### 4.3 参数规则

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| `str` | VARCHAR / NCHAR | 待搜索字符串；`NULL` 时函数返回 `NULL` |
| `pattern` | VARCHAR / NCHAR 常量 | 正则表达式，须为编译期常量；`NULL` 时函数返回 `NULL` |
| `group_idx` | INTEGER 常量 | 0 到 512 之间的整数；`0` = 完整匹配，`≥1` = 对应捕获组；缺省默认 `1`；`NULL` 时函数返回 `NULL` |

**参数约束：**

- `pattern` 须为字符串字面量或常量表达式，不支持列引用（正则在运行时线程缓存中编译，不逐行重新编译）。
- `group_idx` 须为 0 到 512 之间的整数常量；负数或超过 512 在翻译阶段报参数错误。
- `group_idx` 超出 `pattern` 实际捕获组数时，返回 `NULL`，不报错。

### 4.4 返回值语义

| 场景 | 返回值 |
| --- | --- |
| 匹配成功，`group_idx = 0` | 完整匹配子串 |
| 匹配成功，`group_idx = N` | 第 N 个捕获组的内容 |
| 匹配成功，但第 N 个捕获组未参与本次匹配 | `NULL` |
| `str` 为 `NULL` | `NULL` |
| `pattern` 为 `NULL` | `NULL` |
| `group_idx` 为 `NULL` | `NULL` |
| 无匹配 | `NULL` |
| `group_idx` 超出捕获组总数 | `NULL` |
| 捕获组匹配到空串 | `''`（空字符串，非 `NULL`） |

**返回类型：** 与 `str` 保持一致（`str` 为 VARCHAR 则返回 VARCHAR，为 NCHAR 则返回 NCHAR）。

### 4.5 与现有正则功能的关系

TSDB 现有 `MATCH` / `NMATCH` 运算符用于布尔过滤，`REGEXP_EXTRACT` 提供子串提取能力，两者正交：

- `MATCH`：判断字段是否匹配某模式，返回布尔值。
- `REGEXP_EXTRACT`：从字段中提取匹配内容，返回字符串。

### 4.6 正则表达式规范

- 采用 **POSIX 扩展正则表达式 (ERE)** 语法，与 TSDB 现有 `MATCH` / `NMATCH` 保持一致。
- 支持捕获组 `(...)`、字符类 `[...]`、量词 `*`、`+`、`?`、`{m,n}`、锚点 `^`、`$`、交替 `|`。
- 不支持 Perl 兼容扩展（lookahead、lookbehind、命名捕获组、`\d` / `\w` 等简写类）。
- 默认区分大小写；如需大小写不敏感匹配，由调用方通过 `LOWER()` / `UPPER()` 处理。
- 仅返回**第一个**匹配；多次匹配结果不聚合。

### 4.7 边界场景

| 场景 | 预期行为 |
| --- | --- |
| `REGEXP_EXTRACT(NULL, 'a+')` | `NULL` |
| `REGEXP_EXTRACT('abc', NULL)` | `NULL`（pattern 为 NULL，向输出传播 NULL） |
| `REGEXP_EXTRACT('abc', 'x+')` | `NULL`（无匹配） |
| `REGEXP_EXTRACT('abc', '(b)')` | `'b'`（默认 `group_idx=1`） |
| `REGEXP_EXTRACT('abc', 'b', 0)` | `'b'`（`group_idx=0` 返回完整匹配） |
| `REGEXP_EXTRACT('abc', 'b')` | `NULL`（无捕获组，默认 `group_idx=1` 超出范围） |
| `REGEXP_EXTRACT('abc', '(b)(c)', 1)` | `'b'` |
| `REGEXP_EXTRACT('abc', '(b)(c)', 2)` | `'c'` |
| `REGEXP_EXTRACT('abc', '(b)(c)', 3)` | `NULL`（超出捕获组数） |
| `REGEXP_EXTRACT('abc', '(b?)(c?)', 1)` 对 `'a'` 处的匹配 | `''`（捕获组匹配空串） |
| `REGEXP_EXTRACT('', '(a*)')` | `''`（空串匹配空串位置，捕获组内容为空串） |
| `REGEXP_EXTRACT('abc', '(a)(b', 1)` | 翻译阶段报错（无效正则） |
| `REGEXP_EXTRACT('abc', '(b)', NULL)` | `NULL`（group_idx 为 NULL，向输出传播 NULL） |
| `REGEXP_EXTRACT('abc', 'a+', -1)` | 翻译阶段报参数错误 |
| `REGEXP_EXTRACT('abc', '(b)', 513)` | 翻译阶段报参数错误（超过最大值 512） |


## 5. 性能

1. `pattern` 为常量，正则表达式在运行时由线程本地缓存（`threadGetRegComp`）在首次使用时编译并复用，每个唯一 pattern 仅编译一次，不逐行重新编译；翻译阶段另行校验 pattern 语法合法性，不保留编译结果供运行时使用。
2. 回溯型 ERE 引擎在含嵌套量词的 pattern 下可能产生较高 CPU 开销；建议在 pattern 中加锚点以限制回溯范围。


## 6. 安全

1. **正则拒绝服务 (ReDoS)**：恶意构造的正则（如 `(a+)+`）可触发指数级回溯，导致查询线程长时间占用 CPU。缓解措施依赖连接超时（`readTimeout`）和查询级超时配置；当前版本不内置 pattern 复杂度检测或匹配超时。
2. **权限**：与其他标量函数相同，仅需 `SELECT` 权限，无需单独授权。


## 7. 兼容性

1. **新增行为**：`REGEXP_EXTRACT` 为全新函数，不影响任何已有查询行为。
2. **跨引擎语义差异**：

   | 引擎 | `group_idx` | 默认值 | 无匹配时 | 超出组数时 | 正则语法 |
   | --- | --- | --- | --- | --- | --- |
   | Hive | 必填 | — | `NULL` | 报错 | Java regex |
   | Spark / Databricks | 可选 | `1` | `''` | `''` | Java regex |
   | Trino / Presto | 可选 | `0` | `NULL` | `NULL` | RE2 |
   | **本实现** | 可选 | `1` | `NULL` | `NULL` | POSIX ERE |
3. **升级兼容**：无历史数据格式变更，无需升级迁移。
4. **降级兼容**：旧版本不识别 `REGEXP_EXTRACT`，降级后相关查询报解析错误，符合预期。


## 8. 运维

1. `REGEXP_EXTRACT` 无独立配置项。
2. ReDoS 风险的缓解依赖 `readTimeout` 及查询级超时配置；当前版本不内置 pattern 复杂度检测。
3. 可通过 `SHOW QUERIES` 查看执行中的查询，`exec_usec` 列显示执行时长，`sql` 列显示原始 SQL。


## 9. 使用场景

```sql
-- 从日志字段中提取 IPv4 地址（捕获组，默认 group_idx=1）
SELECT REGEXP_EXTRACT(log, '([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)') AS ip FROM logs;

-- 提取 URL 中的协议名（第一个捕获组）
SELECT REGEXP_EXTRACT(url, '(https?)://', 1) AS scheme FROM requests;

-- 提取错误消息中的错误类型（第二个捕获组）
SELECT REGEXP_EXTRACT(msg, 'error\[([0-9]+)\]:([A-Z_]+)', 2) AS err_type FROM events;

-- 返回完整匹配子串（group_idx=0，无需捕获组）
SELECT REGEXP_EXTRACT(raw, '[0-9]+\.[0-9]+', 0) AS val FROM sensor_logs;

-- 结合 WHERE 使用：只返回能提取到值的行
SELECT ts, REGEXP_EXTRACT(raw, '([0-9]+\.[0-9]+)') AS val
FROM sensor_logs
WHERE REGEXP_EXTRACT(raw, '([0-9]+\.[0-9]+)') IS NOT NULL;

-- 嵌套子查询中使用
SELECT scheme, COUNT(*) FROM (
  SELECT REGEXP_EXTRACT(url, '(https?)://', 1) AS scheme FROM requests
) t WHERE scheme IS NOT NULL GROUP BY scheme;
```


## 10. 约束和限制

1. `pattern` 须为编译期常量，不支持列引用。
2. `group_idx` 须为 0 到 512 之间的整数常量，不支持列引用；`NULL` 为合法值，向全部输出行传播 `NULL`。
3. 仅返回**第一个**匹配；如需提取全部匹配，需配合其他机制（当前版本不支持）。
4. 正则语法为 POSIX ERE，不支持 lookahead / lookbehind、命名捕获组等 PCRE 扩展。
5. 返回类型与 `str` 一致：VARCHAR → VARCHAR，NCHAR → NCHAR；不做隐式类型转换。


## 11. 常见错误和排查

| 错误场景 | 原因 | 处理建议 |
| --- | --- | --- |
| 参数数量错误 | 少于 2 个或多于 3 个参数 | 补齐参数 |
| 类型错误：`str` 非字符串 | `str` 为非字符串类型 | 改用 VARCHAR / NCHAR 列或转换 |
| 类型错误：`pattern` 非字符串常量 | `pattern` 使用了列引用或非字符串类型 | 改为字符串字面量 |
| 类型错误：`group_idx` 非整数常量 | `group_idx` 使用了列引用或非整数类型 | 改为 0 到 512 之间的整数常量 |
| 参数错误：`group_idx` 为负数 | 负数不合法 | 改为 0 到 512 之间的整数 |
| 参数错误：`group_idx` 超过最大值 | `group_idx` 大于 512 | 改为不超过 512 的整数 |
| 无效正则表达式 | `pattern` 不符合 POSIX ERE 语法（如括号不配对） | 修正正则语法 |
| 返回 `NULL` 但预期有值 | 无匹配、`group_idx` 超出范围、`str` 为 NULL、pattern 无捕获组而使用默认 `group_idx=1` | 检查 pattern 是否含捕获组；如需完整匹配请显式传 `group_idx=0` |


## 12. 可观测性

1. `SHOW QUERIES` 显示执行中的查询，`sql` 列显示原始 SQL，`exec_usec` 列显示执行时长。
2. 因 ReDoS 触发的高 CPU 查询可通过 `performance_schema.perf_queries` 的 `exec_usec` 列识别。


## 13. 安装和卸载

无。`REGEXP_EXTRACT` 作为内置标量函数随版本发布，无需单独安装或卸载。


## 14. 文档

1. 需要在用户文档"标量函数"章节新增 `REGEXP_EXTRACT` 条目。


## 15. 参考文档

1. Hive LanguageManual UDF — REGEXP_EXTRACT
2. Spark SQL Functions — regexp_extract
3. Trino Functions and Operators — regexp_extract
4. POSIX 正则表达式规范：IEEE Std 1003.1-2017
5. `community/source/libs/parser/inc/sql.y`
6. `community/source/libs/scalar/src/sclfunc.c`


## 16. 附录

实现细节、测试覆盖规划及风险回滚策略见设计文档：Func-RegexpExtract-DS.md。
