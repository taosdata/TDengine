# TEXT 外部行集数据源查询支持 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-10 | - | 0.1 | 任新胜 | 将 TEXT 作为数据源的查询支持 |
| 2026-04-22 | - | 0.2 | 任新胜 | 修正 TIMESTAMP 必选要求，补充数据量限制，去除实现细节 |
| 2026-04-23 | 2026-5-16 | 1.0 | 任新胜 | 第一列不再强制 TIMESTAMP，补充无时间戳列场景规则，与 FILE 约束对齐 |

## 2. 背景

新的产品诉求是支持在 SQL 中直接给出结构化行值并构造查询结果，即 `TEXT ... VALUES ...` 是返回"有结构的多行多列结果集"的表源能力。

从用户视角看，TEXT 的目标是把 SQL 中内嵌的结构化数据包装成可查询的结果集，并像普通表一样参与 SQL。

## 3. 定义

TEXT 数据源：以 SQL 中内嵌行值为原始输入构造的外部行集数据源，可在查询语句的表源位置使用。

## 4. 行为说明

### 4.1 功能目标

1. 支持 TEXT 直接作为 `SELECT ... FROM ...` 的表源，无需创建持久化表。
2. 支持 TEXT 作为 `JOIN` 任意一侧的数据源。
3. 支持 TEXT 作为 `UNION` / `UNION ALL` 任意一侧的数据源。

### 4.2 语法定义

TEXT 对外表现为 `FROM` 子句中的一种内联行集表源。

对外语法定义如下：

```text
from_clause: {
    table_reference [, table_reference] ...
  | table_reference join_clause [, join_clause] ...
}

table_reference:
  table_expr [alias]

table_expr:
  table_name
  | view_name
  | (subquery)
  | TEXT (column_list) VALUES row_value [row_value ...]

column_list:
  col_name type_name [, col_name type_name] ...

row_value:
  (field_value [, field_value] ...)
```

语法示例如下：

```sql
-- 独立查询
SELECT *
FROM TEXT (ts TIMESTAMP, current FLOAT, status INT)
VALUES ('2026-04-01 00:00:00', 10.2, 0)
       ('2026-04-01 00:01:00', 11.5, 1);

-- 派生表（子查询包裹）
SELECT *
FROM (
  SELECT ts, current
  FROM TEXT (ts TIMESTAMP, current FLOAT)
  VALUES ('2026-04-01 00:00:00', 10.2)
         ('2026-04-01 00:01:00', 11.5)
) x;

-- JOIN 真实表（alias 紧跟 VALUES 列表末尾）
SELECT m.ts, m.groupid, t.label
FROM meters m
JOIN TEXT (ts TIMESTAMP, label NCHAR(8))
VALUES ('2026-04-01 00:00:00', 'normal')
   ('2026-04-01 00:01:00', 'alarm') t
ON m.ts = t.ts;

-- 作为 EXTERNAL_WINDOW 的窗口定义来源
SELECT count(*)
FROM meters
EXTERNAL_WINDOW((
  SELECT ts, endtime
  FROM TEXT (ts TIMESTAMP, endtime TIMESTAMP)
  VALUES ('2026-04-01 00:00:00', '2026-04-01 00:05:00')
         ('2026-04-01 01:00:00', '2026-04-01 01:05:00')
) w);
```

### 4.3 TEXT 规则说明

TEXT 写法如下：

```sql
SELECT *
FROM TEXT (ts TIMESTAMP, current FLOAT, status INT)
VALUES ('2026-04-01 00:00:00', 10.2, 0)
       ('2026-04-01 00:01:00', 11.5, 1);
```

TEXT 行输入规则如下：

1. TEXT 作为表源，写法遵循以下语法（与第 4.2 节语法定义一致）：
   ```text
   TEXT (column_list) VALUES row_value [row_value ...]
   ```
2. 如果第一列为 `TIMESTAMP` 类型，该列自动成为主时间戳列；第一列也可以为非 `TIMESTAMP` 类型（无主时间戳列时的能力限制见第 4.7 节）。
3. 每一行使用一组 `()` 表示。
4. 列定义中的列顺序与每行字段值顺序一一对应。
5. 每行字段数必须与列定义数一致。
6. 每个字段值必须可转换到对应列类型。
7. TEXT 至少提供一组行值，不支持空输入。

### 4.4 schema 声明与列名解析规则

TEXT 必须在语义分析阶段确定最终 schema，列名、列类型不由系统隐式猜测。

schema 定义示例：

```sql
TEXT (ts TIMESTAMP, current FLOAT, status INT)
VALUES ('2026-04-01 00:00:00', 10.2, 0)
       ('2026-04-01 00:01:00', 11.5, 1)
```

约定如下：

1. 列名由 `TEXT (column_list)` 中的列定义显式确定，不能重复。
2. 列类型由 `TEXT (column_list)` 中的列定义显式确定。
3. 不支持自动类型推断。
4. 类型转换失败直接报错。
5. `SELECT projectlist`、过滤、排序、分组、窗口定义对列名的引用都基于最终 schema 解析。
6. 列名须遵循 TDengine 标识符命名规则，包括：不能使用 SQL 保留字（如 `tag`、`interval`、`timestamp` 等）、不能以数字开头、长度不超过系统标识符最大限制。

以下情况不支持：

1. 省略列定义（schema 必须显式给出）。
2. 使用 `*` 或表达式作为列定义（仅支持 `name TYPE[(len)]` 形式）。
3. 为列指定 `DEFAULT`、`NOT NULL`、`PRIMARY KEY` 等约束修饰符。
4. schema 中不支持以下数据类型：`JSON`、`GEOMETRY`、`BLOB`。

TEXT 的类型转换规则如下：

1. 所有字段值都按列定义执行类型校验和转换。
2. `timestamp` 列按时间戳规则解析，支持现有时间字面量与时间字符串格式；格式非法时报错。
3. 整数、浮点、布尔等数值列按目标类型执行转换与范围检查；转换失败或溢出时报错。
4. `binary`、`varchar`、`nchar` 等字符串列按目标列长度和字符集约束处理；超长时报错。

### 4.5 TEXT 支持范围

直接支持：

1. 独立查询中的 `SELECT ... FROM TEXT (...) VALUES ...`。
2. `SELECT` 中作为 `JOIN` 任意一侧的数据源。
3. `SELECT` 中作为 `UNION` / `UNION ALL` 任意一侧的数据源。
4. `INSERT INTO ... SELECT ...` 中，`SELECT` 的 `FROM` 子句直接使用 TEXT。

间接支持（通过子查询包裹 TEXT）：

凡当前支持子查询表源的位置，均可在子查询内部使用 TEXT，TEXT 的结果集以子查询结果的形式参与上层查询。典型场景包括：

1. 派生表 / 嵌套子查询：`SELECT ... FROM (SELECT ... FROM TEXT (...) VALUES ...) alias`。
2. `EXTERNAL_WINDOW` 的窗口定义子查询：`EXTERNAL_WINDOW((SELECT ts, endtime FROM TEXT (...) VALUES ...) alias)`。
3. 标量子查询、`EXISTS` 子查询等已支持子查询语义的位置。

**注意**：TEXT 通过子查询进入上层入口时，仍须满足该入口的原有约束。例如 `EXTERNAL_WINDOW` 要求窗口定义子查询的前两列均为 `TIMESTAMP`；`INSERT INTO` 要求输出列与目标表 schema 匹配。

### 4.7 无 timestamp 列时的能力限制

TEXT 不强制要求第一列为 `TIMESTAMP` 类型。如果 TEXT 的列定义中不包含 `TIMESTAMP` 列，则仍可用于以下场景：

1. 普通投影查询。
2. `WHERE` 过滤。
3. `GROUP BY`。
4. `PARTITION BY`。
5. `ORDER BY` 非时间列。

如果 TEXT 的列定义中不包含 `TIMESTAMP` 列，则不支持以下依赖时间戳列语义的场景：

1. `JOIN`（JOIN 要求主时间戳等值条件）。
2. `INTERVAL` / `SLIDING` / `FILL`。
3. `SESSION`。
4. `EVENT_WINDOW`。
5. `INTERP` 对应的 `RANGE` / `EVERY` / `FILL` 组合能力。
6. 作为 `EXTERNAL_WINDOW` 的窗口定义子查询结果。
7. `INSERT INTO ... SELECT ...`（需要主时间戳列）。

如果查询语句使用了上述依赖时间戳列的能力，而 TEXT 列定义中不存在可用的 `TIMESTAMP` 列，应直接报错。

## 5. 数据量限制

TEXT 的输入规模受以下三层限制约束：

| 限制维度 | 上限 |
| --- | --- |
| 最大行数 | 10,000 行 |
| 最大单元格数（行数 × 列数） | 1,000,000 |
| 序列化后数据块大小 | 8 MB |

三层限制取最先触达的一层为准，超出任一限制直接报错。

TEXT 主要面向轻量级外部查询、临时分析、窗口定义、测试与调试场景，不以替代批量导入工具为目标。对大规模数据场景，应使用正式导入工具，而不是 TEXT 查询。

## 6. 安全

1. TEXT 通过上述数据量限制防止单条 SQL 携带超大文本造成系统压力。
2. 审计或日志中应能区分 TEXT 数据源使用行为。
3. 大小、行数限制应在 SQL 解析阶段完成校验，在超大输入放大为复杂执行计划之前返回错误。

## 7. 兼容性

### 7.1 向后兼容

1. 现有普通查询语法不变。

### 7.2 兼容性风险

无。

### 7.3 兼容性目标

1. TEXT 在表源语义、错误语义和使用方式上与现有查询对象保持一致。
2. 错误码和错误文案能区分"语法错误""schema 错误""数据解析错误""执行错误"。

## 8. 运维

运维侧需要关注：

1. TEXT 内容格式不合法、类型转换失败等问题是否有清晰报错。
2. TEXT 超出数据量限制时的报错是否能引导用户选择正式导入工具。

## 9. 使用场景

1. 独立查询。
2. `SELECT` 的 `FROM` 子句直接使用 TEXT。
3. 通过派生表、匿名子查询、`EXTERNAL_WINDOW` 子查询等既有子查询入口使用 TEXT。

## 10. 约束和限制

1. 列定义必须显式给出，不支持自动类型推断。
2. 列名不能使用 SQL 保留字。
3. TEXT 的多行输入必须采用 `VALUES` 风格表达，每组 `()` 表示一行。
4. TEXT 至少提供一组行值，不支持空输入。
5. 列名必须唯一。
6. 查询 `SELECT` 列表中引用的列必须在 schema 中存在。
7. TEXT 间接进入既有子查询入口时，必须满足该入口原有输入和输出约束。
8. 如果列定义中不包含 `TIMESTAMP` 列，则不能使用依赖时间戳列语义的查询能力（见第 4.7 节）。

## 11. 常见错误和排查

1. TEXT 为空输入。
   - 应明确报错 TEXT 至少需要一组行值。
2. schema 与数据列数不一致。
   - 应明确提示列数不匹配。
3. 列名使用了 SQL 保留字。
   - 应报语法错误，使用非保留字替代（如将 `tag` 改为 `label`）。
4. 列名重复。
   - 应明确指出重复列名。
5. `SELECT` 或其他表达式引用了不存在的列名。
   - 应明确指出不存在的列名。
6. 类型转换失败。
   - 应报告目标列名、目标类型、原始文本值。
7. TEXT 列定义中无 `TIMESTAMP` 列，却使用了 JOIN、时间窗口、插值或 EXTERNAL_WINDOW 等依赖时间戳列的能力。
   - 应明确报错当前查询能力依赖 `TIMESTAMP` 列。
8. 主时间戳列（第一列为 TIMESTAMP 类型）出现 NULL 值。
   - 应明确报错主时间戳列不允许为 NULL。
9. TEXT 输入超过行数、单元格数或序列化大小限制。
   - 应明确报错超出 TEXT 输入限制。

## 12. 可观测性

1. EXPLAIN 或等价可视信息能体现当前数据源为 TEXT。
2. 错误码至少区分：参数错误、schema 错误、数据解析错误、执行错误。
3. 用户文档中需要明确 TEXT 的适用场景和限制，避免被误解为高吞吐导入接口。

## 13. 安装和卸载

无安装和卸载要求。

## 14. 文档

需要同步准备以下文档：

1. SQL 参考文档：补充 `TEXT (...) VALUES ...` 作为查询表源的语法。
2. 查询基础文档：补充典型示例、适用场景和使用限制。
3. `EXTERNAL_WINDOW` 文档：补充 `EXTERNAL_WINDOW((SELECT ... FROM TEXT (...) VALUES ...) alias)` 的使用示例。
4. 数据导入相关文档：明确 `INSERT INTO ... SELECT ... FROM TEXT (...) VALUES ...` 的标准用法。

## 15. 参考文档

1. `community/docs/zh/05-basic/03-query.md`
2. `community/docs/zh/14-reference/03-taos-sql/20-select.md`
3. `community/source/libs/parser/inc/sql.y`
4. `community/source/libs/parser/src/parTranslater.c`

## 16. 附录

### 16.1 验收要求

需要覆盖至少以下功能验收面：

1. `SELECT * FROM TEXT (...) VALUES ...`。
2. `SELECT * FROM (SELECT ... FROM TEXT (...) VALUES ...)`。
3. 真实表与 TEXT 的 JOIN（LEFT/RIGHT/INNER）。
4. `EXTERNAL_WINDOW((SELECT ... FROM TEXT (...) VALUES ...) alias)`。
5. `INSERT INTO ... SELECT ... FROM TEXT (...) VALUES ...`。
6. 重复列名、`SELECT projectlist` 引用不存在列、列数不匹配、类型转换失败等错误场景。
7. TEXT 单行输入、多行输入、非法空输入、每行字段数不一致等输入场景。
8. 无 timestamp 列时的能力限制：普通查询可用，JOIN、时间窗口、插值等不可用，应正确报错。
9. 超出行数、单元格数、序列化大小限制的报错场景。
10. 列名使用 SQL 保留字的报错场景。
