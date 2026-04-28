# REGEXP_EXTRACT 函数 TS

# 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-16 | - | 0.1 | Stephen | 初稿 Test Spec，覆盖功能测试设计 |


# 测试目标

- 覆盖 FS 定义的 REGEXP_EXTRACT 函数全部行为：默认 `group_idx=1`、`group_idx=0` 完整匹配、多捕获组、NULL/无匹配/超出范围返回、空串语义、按行求值、WHERE/子查询上下文、NCHAR 类型。
- 验证参数约束：`pattern` 须为常量、`group_idx` 须为 0 到 512 之间的整数常量或 `NULL`、参数数量范围 2–3。
- 覆盖所有不支持范围：参数数量错误、非字符串 `str`、`pattern` 列引用、负数或超过 512 的 `group_idx`、无效正则表达式。
- 为后续自动化回归提供稳定、可扩展的全覆盖基线。


# 参考文档

- Func-RegexpExtract-FS.md


# 测试结论

- 当前文档为测试规格与执行记录，全部用例已执行通过（`test_fun_sca_regexp_extract.py`）。
- 功能测试设计用例：40 条；已全部执行通过。
- 覆盖目标：
  - 功能覆盖：FS §4（行为说明）、§4.3（参数规则）、§4.4（返回值语义）、§4.7（边界场景）全覆盖。
  - 不支持范围覆盖：参数数量错误、非法参数类型、`group_idx` 超出合法范围、无效正则表达式全覆盖。


# 测试环境

- OS: Linux x86_64（Ubuntu 22.04+）
- TDengine: 企业版 v3.4.2.0+
- 测试框架：`new_test_framework`，测试文件 `test/cases/11-Functions/01-Scalar/test_fun_sca_regexp_extract.py`


# 功能测试

## 1 基本功能（默认 group_idx=1）

### 测试要点

- 2 参数形式：`REGEXP_EXTRACT(str, pattern)` 默认返回第一个捕获组内容。
- 存在多个捕获组时，仍只返回第一个。
- `pattern` 无捕获组时，默认 `group_idx=1` 超出范围，返回 `NULL`（区别于 `group_idx=0`）。
- 返回类型与 `str` 类型一致（VARCHAR → VARCHAR）；SQL 层无法通过 `checkData` 断言返回列类型，功能验证由 §6 表查询覆盖，类型正确性为架构确认项。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| RXE-BASIC-001 | 2-arg 单捕获组返回 group 1 | `SELECT REGEXP_EXTRACT('abc', '(b)')` 返回 `'b'`。 | 已通过 |
| RXE-BASIC-002 | 2-arg 多捕获组返回 group 1 | `SELECT REGEXP_EXTRACT('abc', '(b)(c)')` 返回 `'b'`（group 1，非 group 2）。 | 已通过 |
| RXE-BASIC-003 | 2-arg 无捕获组返回 NULL | `SELECT REGEXP_EXTRACT('abc', 'b')` 返回 `NULL`（无捕获组，默认 `group_idx=1` 超出范围）。 | 已通过 |
| RXE-BASIC-004 | VARCHAR 输入提取值正确 | 建表列 `vc VARCHAR(128)`；`SELECT REGEXP_EXTRACT(vc, '([0-9]+)')` 提取到的值正确（功能由 §6 RXE-TBL-001/002 覆盖；类型正确性为架构确认项）。 | 已通过 |

## 2 group_idx=0 完整匹配

### 测试要点

- `group_idx=0` 返回完整匹配子串，无需捕获组。
- `group_idx=0` 配合捕获组 pattern 时，仍返回完整匹配（非 group 1）。
- 无匹配时 `group_idx=0` 也返回 `NULL`。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| RXE-GRP0-001 | group_idx=0 无捕获组返回完整匹配 | `SELECT REGEXP_EXTRACT('abc', 'b', 0)` 返回 `'b'`。 | 已通过 |
| RXE-GRP0-002 | group_idx=0 有捕获组返回完整匹配 | `SELECT REGEXP_EXTRACT('abc', '(b)c', 0)` 返回 `'bc'`（完整匹配，非 group 1 的 `'b'`）。 | 已通过 |
| RXE-GRP0-003 | group_idx=0 无匹配返回 NULL | `SELECT REGEXP_EXTRACT('abc', 'x+', 0)` 返回 `NULL`。 | 已通过 |

## 3 多捕获组索引

### 测试要点

- `group_idx=1` 返回第一个捕获组；`group_idx=2` 返回第二个捕获组。
- `group_idx` 超出实际捕获组数时返回 `NULL`，不报错。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| RXE-GRP-001 | group_idx=1 取第一组 | `SELECT REGEXP_EXTRACT('abc', '(b)(c)', 1)` 返回 `'b'`。 | 已通过 |
| RXE-GRP-002 | group_idx=2 取第二组 | `SELECT REGEXP_EXTRACT('abc', '(b)(c)', 2)` 返回 `'c'`。 | 已通过 |
| RXE-GRP-003 | group_idx 超出组数返回 NULL | `SELECT REGEXP_EXTRACT('abc', '(b)(c)', 3)` 返回 `NULL`，不报错。 | 已通过 |

## 4 NULL 与无匹配

### 测试要点

- `str` 为 `NULL` 时函数返回 `NULL`。
- `pattern` 为 `NULL` 时函数返回 `NULL`。
- `pattern` 在 `str` 中无匹配时返回 `NULL`。
- 仅返回第一个匹配；`str` 中存在多个匹配时，只取最左第一个匹配的结果。
- `group_idx` 为 `NULL` 时函数返回 `NULL`（NULL 值向输出传播）。
- 非参与捕获组（alternation 中未参与匹配的分支）返回 `NULL`（而非空串）。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| RXE-NULL-001 | str 为 NULL 返回 NULL | `SELECT REGEXP_EXTRACT(NULL, '(a+)')` 返回 `NULL`。 | 已通过 |
| RXE-NULL-002 | 无匹配返回 NULL | `SELECT REGEXP_EXTRACT('abc', '(x+)')` 返回 `NULL`。 | 已通过 |
| RXE-NULL-003 | 多次匹配只取第一个 | `SELECT REGEXP_EXTRACT('a1b2', '([0-9])')` 返回 `'1'`（最左第一个匹配）。 | 已通过 |
| RXE-NULL-004 | group_idx=0 但 str 为 NULL | `SELECT REGEXP_EXTRACT(NULL, 'a+', 0)` 返回 `NULL`。 | 已通过 |
| RXE-NULL-005 | group_idx 为 NULL 返回 NULL | `SELECT REGEXP_EXTRACT('abc', '(b)', NULL)` 返回 `NULL`（NULL group_idx 向输出传播 NULL）。 | 已通过 |
| RXE-NULL-006 | 非参与捕获组返回 NULL | `SELECT REGEXP_EXTRACT('b', '(a)\|(b)', 1)` 返回 `NULL`（group 1 未参与本次匹配）。 | 已通过 |
| RXE-NULL-007 | 参与捕获组正常返回值 | `SELECT REGEXP_EXTRACT('b', '(a)\|(b)', 2)` 返回 `'b'`（group 2 参与本次匹配）。 | 已通过 |
| RXE-NULL-008 | pattern 为 NULL 返回 NULL | `SELECT REGEXP_EXTRACT('abc', NULL)` 返回 `NULL`。 | 已通过 |

## 5 空串场景

### 测试要点

- 捕获组匹配到空串时，返回 `''`（空字符串），而非 `NULL`。
- 空输入串 `''` 与能匹配零长度位置的 pattern 配合，捕获组内容为 `''`。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| RXE-EMPTY-001 | 捕获组匹配空串返回空字符串 | `SELECT REGEXP_EXTRACT('ac', '(b?)')` 返回 `''`（`b?` 在起始位置匹配空串，非 `NULL`）。 | 已通过 |
| RXE-EMPTY-002 | 空输入串与零长度匹配 | `SELECT REGEXP_EXTRACT('', '(a*)')` 返回 `''`（空位置匹配，捕获组内容为空串）。 | 已通过 |

## 6 表查询与多行

### 测试要点

- 标量函数按行求值，每行独立提取。
- 列值为 `NULL` 的行，函数返回 `NULL`，其他行正常提取。
- 空表查询返回 0 行，无报错。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| RXE-TBL-001 | 多行表查询按行求值 | 4 行 ct1：`vc = ['code=42,type=DISK_FULL', 'code=7,type=LOW_MEM', 'code=0,type=OK', NULL]`；`SELECT REGEXP_EXTRACT(vc, 'code=([0-9]+)')` 返回 `['42', '7', '0', NULL]`。 | 已通过 |
| RXE-TBL-002 | 含 NULL 列的多行查询 | 4 行 ct1，第 4 行 `vc = NULL`；`REGEXP_EXTRACT(vc, 'type=([A-Z_]+)')` 返回 `['DISK_FULL', 'LOW_MEM', 'OK', NULL]`，NULL 行结果为 NULL。 | 已通过 |
| RXE-TBL-003 | 空表查询无报错 | `SELECT REGEXP_EXTRACT(vc, '([0-9]+)') FROM empty_t`（空表），返回 0 行，无报错。 | 已通过 |

## 7 WHERE 子句

### 测试要点

- `REGEXP_EXTRACT` 可在 `WHERE` 子句中使用，语义与 `SELECT` 列表相同。
- `IS NOT NULL` 过滤可筛选出成功提取的行。
- 与比较运算符结合可做等值过滤。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| RXE-WHERE-001 | IS NOT NULL 过滤匹配行 | `SELECT vc FROM ct1 WHERE REGEXP_EXTRACT(vc, 'code=([4-9][0-9]+)') IS NOT NULL`，仅返回 code ≥ 40 的行（结果：`code=42,type=DISK_FULL`）。 | 已通过 |
| RXE-WHERE-002 | 等值过滤提取结果 | `SELECT vc FROM ct2 WHERE REGEXP_EXTRACT(vc, '(https?)://') = 'https'`，仅返回协议为 https 的行（结果：`https://example.com`）。 | 已通过 |

## 8 NCHAR 类型

### 测试要点

- `str` 为 NCHAR 类型时，提取到的值与同数据的 VARCHAR 列一致（值等价验证；SQL 层无法通过 `checkData` 断言返回列类型，类型正确性为架构确认项）。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| RXE-NCHAR-001 | NCHAR 输入提取值与 VARCHAR 等价 | 建表列 `nc NCHAR(64)`；`SELECT REGEXP_EXTRACT(nc, 'code=([0-9]+)') FROM ct1` 返回 `['42', '7', '0', NULL]`，与同数据的 VARCHAR 列结果一致。 | 已通过 |

## 9 嵌套子查询与 GROUP BY

### 测试要点

- `REGEXP_EXTRACT` 可在嵌套子查询中使用，结果列可作为外层 `GROUP BY` 依据。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| RXE-SUB-001 | 子查询结果用于 GROUP BY | `SELECT scheme, COUNT(*) FROM (SELECT REGEXP_EXTRACT(vc, '(https?)://') AS scheme FROM ct2) t WHERE scheme IS NOT NULL GROUP BY scheme ORDER BY scheme`，结果 2 行：http→1、https→1。 | 已通过 |

## 10 不支持范围（错误）

### 测试要点

- 参数数量少于 2 或多于 3 时报错。
- `str` 为非字符串类型时报类型错误。
- `pattern` 为列引用（非常量）时报错（pattern 须为编译期常量）。
- `group_idx` 为负数时翻译阶段报参数错误。
- `group_idx` 超过最大值（512）时翻译阶段报参数错误。
- `pattern` 不符合 POSIX ERE 语法（如括号不配对）时翻译阶段报错。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| RXE-ERR-001 | 参数过少（1 个）| `SELECT REGEXP_EXTRACT('abc')` 报错。 | 已通过 |
| RXE-ERR-002 | 参数过多（4 个）| `SELECT REGEXP_EXTRACT('abc', '(b)', 1, 0)` 报错。 | 已通过 |
| RXE-ERR-003 | str 为非字符串类型 | `SELECT REGEXP_EXTRACT(iv, '([0-9]+)') FROM ct1`（`iv` 为 INT 列）报类型不匹配错误。 | 已通过 |
| RXE-ERR-004 | pattern 为列引用 | `SELECT REGEXP_EXTRACT(vc, vc) FROM ct1`（`vc` 作为 pattern 列引用）报错（pattern 不支持列引用）。 | 已通过 |
| RXE-ERR-005 | group_idx 为负数 | `SELECT REGEXP_EXTRACT('abc', '(b)', -1)` 翻译阶段报参数错误。 | 已通过 |
| RXE-ERR-006 | 无效正则表达式 | `SELECT REGEXP_EXTRACT('abc', '(b', 1)` 翻译阶段报无效正则错误（括号不配对）。 | 已通过 |
| RXE-ERR-007 | group_idx 超过最大值 | `SELECT REGEXP_EXTRACT('abc', '(b)', 513)` 翻译阶段报参数错误（超过最大值 512）。 | 已通过 |

## 11 正则表达式特性（POSIX ERE）

### 测试要点

- 字符类 `[...]`、量词 `+`/`{m,n}`、锚点 `^`/`$`、交替 `|` 均可正常使用。
- 默认区分大小写；`LOWER()` 可实现不敏感匹配。
- 非参与捕获组行为已在 §4 RXE-NULL-006/007 覆盖，此处不重复。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| RXE-RE-001 | 字符类提取数字串 | `SELECT REGEXP_EXTRACT('v=3.14', '([0-9]+\.[0-9]+)')` 返回 `'3.14'`。 | 已通过 |
| RXE-RE-002 | 锚点 ^ 限制匹配位置 | `SELECT REGEXP_EXTRACT('abc', '^(a)')` 返回 `'a'`；`SELECT REGEXP_EXTRACT('xabc', '^(a)')` 返回 `NULL`。 | 已通过 |
| RXE-RE-003 | 大小写敏感（默认）| `SELECT REGEXP_EXTRACT('ABC', '(abc)')` 返回 `NULL`；`SELECT REGEXP_EXTRACT(LOWER('ABC'), '(abc)')` 返回 `'abc'`。 | 已通过 |

## 12 文档示例验证

### 测试要点

- 逐条执行用户手册（EN/ZH）中 REGEXP_EXTRACT 示例查询，验证其结果与文档描述完全一致。

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| RXE-DOC-001 | 日期串提取年份（group 1）| `SELECT REGEXP_EXTRACT('2026-04-22', '([0-9]{4})-([0-9]{2})-([0-9]{2})', 1)` 返回 `'2026'`。 | 已通过 |
| RXE-DOC-002 | 日期串提取完整匹配（group 0）| `SELECT REGEXP_EXTRACT('2026-04-22', '([0-9]{4})-([0-9]{2})-([0-9]{2})', 0)` 返回 `'2026-04-22'`。 | 已通过 |
| RXE-DOC-003 | 无匹配返回 NULL | `SELECT REGEXP_EXTRACT('no-digits-here', '[0-9]+', 1)` 返回 `NULL`。 | 已通过 |