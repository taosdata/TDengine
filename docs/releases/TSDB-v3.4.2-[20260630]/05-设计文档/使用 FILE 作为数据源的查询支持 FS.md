# FILE 外部行集数据源查询支持 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-09 | - | 0.1 | 任新胜 | 初版，将 FILE 作为数据源的查询支持 |
| 2026-04-10 | - | 0.2 | 任新胜 | 将 FILE/TEXT 拆分为两个独立 FS |
| 2026-04-22 | - | 0.3 | 任新胜 | 补充数据量限制，明确路径语义，去除实现细节 |
| 2026-04-23 | 2026-5-16 | 1.0 | 任新胜 | 修正无 timestamp 列时 JOIN 不支持，与 TEXT 约束对齐 |

## 2. 背景

当前 `FILE 'tmp/csvfile.csv'` 仅作为 `INSERT INTO ... FILE ...` 的专用输入语法存在，不能独立成为查询数据源，也不能出现在其他需要子查询或表源的位置。

新的产品诉求是让 FILE 成为真正的查询表源，使其能够像普通表或子查询一样参与 `SELECT`、`JOIN`、`INSERT INTO ... SELECT ...` 等场景。

## 3. 定义

FILE 数据源：以 CSV 文件内容为原始输入构造的外部行集数据源，可在所有需要查询表源的位置使用。

## 4. 行为说明

### 4.1 功能目标

1. 支持 FILE 直接作为 `SELECT ... FROM ...` 的表源，无需创建持久化表。
2. 支持 FILE 作为 `JOIN` 任意一侧的数据源。
3. 支持 FILE 作为 `UNION` / `UNION ALL` 任意一侧的数据源。

### 4.2 语法定义

FILE 定义为查询对象中的表源类型，语法层级与普通表相同，共用同一套查询对象规则。

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
  | FILE(file_path, column_list [, option [, option] ...])

file_path:
  string_literal        -- 客户端本地路径，支持绝对路径和相对路径

column_list:
  string_literal        -- 格式：col_name type_name [, col_name type_name] ...

option:
  header = true | false -- 首行是否为列名行，默认 false
  delimiter = char      -- 字段分隔符，默认 ','
```

语法示例如下：

```sql
-- 基本查询（无 header 行，按列顺序映射）
SELECT ts, current, voltage
FROM FILE('/tmp/meter_data.csv', 'ts TIMESTAMP, current FLOAT, voltage DOUBLE') f;

-- 读取有 header 行的 CSV（按列名映射）
SELECT ts, current, voltage
FROM FILE('/tmp/meter_data.csv', 'ts TIMESTAMP, current FLOAT, voltage DOUBLE', header=true) f;

-- CSV 有多列，只读取其中部分列（column_list 列数少于文件实际列数）
SELECT ts, current
FROM FILE('/tmp/meter_data.csv', 'ts TIMESTAMP, current FLOAT', header=true) f;

-- 自定义分隔符
SELECT ts, current
FROM FILE('/tmp/meter_data.tsv', 'ts TIMESTAMP, current FLOAT', header=true, delimiter='\t') f;

-- 使用别名
SELECT f.ts, f.current
FROM FILE('/tmp/meter_data.csv', 'ts TIMESTAMP, current FLOAT') AS f;

-- 派生表（子查询包裹）
SELECT *
FROM (
  SELECT ts, current
  FROM FILE('/tmp/meter_data.csv', 'ts TIMESTAMP, current FLOAT')
) sub;

-- JOIN 真实表
SELECT m.groupid, f.ts, f.current
FROM meters m
JOIN FILE('/tmp/meter_data.csv', 'ts TIMESTAMP, current FLOAT') f
ON m.ts = f.ts;
```

### 4.3 FILE 选项说明

`FILE(...)` 支持如下选项：

1. `header = true | false`
   - 是否存在首行列名。
   - 默认值为 `false`。
   - `header=true` 时，文件首行为列名行；`column_list` 中的列名必须与首行列名一致，用于按名映射，`column_list` 列数可以少于文件实际列数（即只读取指定列）。
   - `header=false` 时，文件首行为数据行；`column_list` 按文件列顺序同时定义列名和列类型。

2. `delimiter = char`
   - 字段分隔符，默认值为 `,`。
   - 示例：`delimiter='\t'`（制表符）、`delimiter=';'`（分号）。

### 4.4 schema 声明与列名解析规则

FILE 必须在语义分析阶段确定最终 schema，列名、列类型不由系统隐式猜测。

schema 声明示例：

```sql
FILE('/tmp/meter_data.csv', 'ts TIMESTAMP, current FLOAT, voltage DOUBLE')
```

`column_list` 支持绝大多数 TDengine 基础数据类型，以下类型**不支持**：

| 类型名 | 原因 |
| --- | --- |
| `JSON` | 仅用于标签，不支持作为列类型 |
| `GEOMETRY` | 不支持 WKT/WKB 文本转换 |
| `BLOB` | 不支持 |

约定如下：

1. 列名由 `column_list` 显式确定，不能重复。
2. 列类型由 `column_list` 确定，CSV 文件中的字段先按文本读取，再按对应列类型做转换。
3. `column_list` 列数可以少于实际文件列数，未声明的文件列读取时跳过，不进入结果集。
4. `column_list` 列数多于实际文件列数时直接报错。
5. `header=true` 时，`column_list` 中的列名必须来自文件首行；`header=false` 时，`column_list` 按顺序定义列名。
6. 类型转换失败直接报错。
7. `SELECT projectlist`、过滤、排序、分组、窗口定义对列名的引用都基于最终 schema 解析。

FILE 的类型转换规则如下：

1. `timestamp` 列按时间戳规则解析，支持现有时间字面量与时间字符串格式；格式非法时报错。
2. 整数、浮点、布尔等数值列按目标类型执行转换与范围检查；转换失败或溢出时报错。
3. `binary`、`varchar`、`nchar` 等字符串列按目标列长度和字符集约束处理；超长时报错。

### 4.5 FILE 路径说明

1. 对于原生链接来说：FILE 路径由**发起查询的应用进程**在本地读取，文件内容在客户端侧解析后随查询计划发送至服务端，服务端不直接访问文件路径。这一行为与连接方式（Native、REST、WebSocket）无关，均由应用进程负责读取。
但是对于非原生链接，taosc 执行是在 taosadapter 完成，所以加载 file 会找不到文件报错。这里是否要禁止是现时还需进一步设计
2. 支持绝对路径和相对路径。
3. 相对路径相对于**发起查询的进程的当前工作目录**。
4. 如果进程无法访问目标路径，直接报错，不做静默降级。

### 4.6 FILE 支持范围

直接支持：

1. 独立查询中的 `SELECT ... FROM FILE(...)`。
2. 普通 `SELECT` 中作为 JOIN 一侧的数据源。
3. `INSERT INTO ... SELECT ...` 中，`SELECT` 的 `FROM` 子句直接使用 FILE。
4. `UNION` / `UNION ALL` 中作为任意一侧的数据源。

间接支持（通过 FILE 外套子查询）：

1. 所有当前已经支持子查询表源的位置，都可以在其子查询内部使用 FILE。
2. 例如 `EXTERNAL_WINDOW((SELECT ... FROM FILE(...)) alias)`。
3. FILE 间接进入上层子查询入口时，仍需满足该入口原有约束。

### 4.7 无 timestamp 列时的能力限制

FILE 不强制要求第一列为 `TIMESTAMP` 类型。

如果 FILE 的 `column_list` 中不包含 `timestamp` 列，则仍可用于以下场景：

1. 普通投影查询。
2. `WHERE` 过滤。
3. `GROUP BY`。
4. `PARTITION BY`。
5. `ORDER BY` 非时间列。

如果 FILE 的 `column_list` 中不包含 `timestamp` 列，则不支持以下依赖时间戳列语义的场景：

1. `JOIN`（JOIN 要求主时间戳等值条件）。
2. `INTERVAL` / `SLIDING` / `FILL`。
2. `SESSION`。
3. `EVENT_WINDOW`。
4. `INTERP` 对应的 `RANGE` / `EVERY` / `FILL` 组合能力。
5. 作为 `EXTERNAL_WINDOW` 的窗口定义子查询结果。
6. `INSERT INTO ... SELECT ...`（需要主时间戳列）。

如果查询语句使用了上述依赖时间戳列的能力，而 FILE `column_list` 中不存在可用的 `timestamp` 列，应直接报错。

## 5. 数据量限制

FILE 的输入规模受以下三层限制约束，与 TEXT 保持一致：

| 限制维度 | 上限 |
| --- | --- |
| 最大行数 | 10,000 行 |
| 最大单元格数（行数 × 列数） | 1,000,000 |
| 序列化后数据块大小 | 8 MB |

三层限制取最先触达的一层为准，超出任一限制直接报错。

FILE 主要面向轻量级外部查询、临时分析、窗口定义、测试与调试场景，不以替代批量导入工具为目标。对大规模数据场景，应使用正式导入工具，而不是 FILE 查询。

## 6. 安全

1. FILE 必须限制可访问路径范围，防止任意读取宿主机敏感文件。
2. FILE 的权限模型应与现有文件导入能力保持一致。
3. 审计或日志中应能区分 FILE 数据源访问行为。
4. 应明确区分客户端本地路径不可访问与服务端执行错误这两类问题。

## 7. 兼容性

### 7.1 向后兼容

1. 现有普通查询语法不变。
2. 现有 `INSERT INTO ... FILE ...` 语义不变。
3. 现有 EXTERNAL_WINDOW 语义不变，只是允许其子查询来源扩展为 FILE。

### 7.2 兼容性风险

1. 文档中必须明确"路径按客户端解释"，避免用户误认为文件在服务端读取。

### 7.3 兼容性目标

1. FILE 在表源语义、错误语义和使用方式上与现有查询对象保持一致。
2. 错误码和错误文案能区分"语法错误""schema 错误""文件访问错误""数据解析错误""执行错误"。

## 8. 运维

运维侧需要关注：

1. FILE 路径不存在、权限不足、编码不合法、分隔符错误等问题是否有清晰报错。
2. FILE 超出数据量限制时的报错是否能引导用户选择正式导入工具。

## 9. 使用场景

1. 独立查询。
2. 所有需要子查询的地方，包括 `INSERT INTO tbname` 之后的子查询。
3. 限制：如果 FILE 的 `column_list` 中不包含 `timestamp` 列，则仅支持不依赖时间戳列语义的查询场景（JOIN 不可用）。

## 10. 约束和限制

1. 当前仅支持 CSV 文本格式。
2. `column_list` 为必选项，不支持自动类型推断。
3. `column_list` 支持的类型见第 4.4 节。
4. `column_list` 列名必须唯一。
5. `column_list` 列数可以少于实际文件列数，但不能多于实际文件列数。
6. `header` 仅支持 `true` 和 `false` 两个取值，默认 `false`。
7. `header=true` 时，`column_list` 中的列名必须与文件首行列名一致。
8. 选项以逗号分隔的 `key=value` 形式在 `column_list` 之后传入，无需 `OPTIONS(...)` 包裹。
9. 路径按客户端本地路径解释；客户端无法访问目标路径时直接报错。
10. 查询 `SELECT` 列表中引用的列必须在最终 schema 中存在。
11. 如果 `column_list` 中不存在可用 `timestamp` 列，则不能使用依赖时间戳列语义的查询能力（包括 JOIN）。
12. CSV 单行最大列数为 4096，超出时报错。

## 11. 常见错误和排查

1. 文件路径不存在或权限不足。
   - 报错应区分 `file not found`、`permission denied`、`path not allowed`。
2. `column_list` 未提供。
   - 应明确报错 `column_list` 为必选项。
3. `column_list` 中出现了不支持的类型名。
   - 应明确报错不支持的类型名。
4. `column_list` 中出现了不支持的选项名。
   - 应明确报错未知的选项名。
5. `column_list` 列数多于实际文件列数。
   - 应明确提示声明列数超过实际文件列数。
6. `header=true` 时，`column_list` 中列名在文件首行不存在。
   - 应明确指出不存在于 header 中的列名。
7. 列名重复。
   - 应明确指出重复列名。
8. `SELECT` 或其他表达式引用了不存在的列名。
   - 应明确指出不存在的列名。
9. 类型转换失败。
   - 应报告目标列名、目标类型、原始文本值。
10. FILE 不包含 `timestamp` 列却使用了 JOIN、时间窗口、插值或 EXTERNAL_WINDOW 相关能力。
    - 应明确报错当前查询能力依赖 `timestamp` 列。
11. 超出行数、单元格数或序列化大小限制。
    - 应明确报错超出 FILE 输入限制。

## 12. 可观测性

1. EXPLAIN 或等价可视信息能体现当前数据源为 FILE。
2. 错误码至少区分：参数错误、schema 错误、文件访问错误、数据解析错误、执行错误。
3. 用户文档中需要明确 FILE 的适用场景和限制，避免被误解为高吞吐导入接口。

## 13. 安装和卸载

无安装和卸载要求。

## 14. 文档

需要同步准备以下文档：

1. SQL 参考文档：补充 `FILE(...)` 作为查询表源的语法。
2. 查询基础文档：补充典型示例、适用场景和使用限制。
3. EXTERNAL_WINDOW 文档：补充 FILE 作为窗口定义子查询来源的示例。
4. 数据导入相关文档：明确 `INSERT INTO ... SELECT ... FROM FILE(...)` 的标准用法。
5. 用户手册：明确 FILE 按客户端本地路径解释，由客户端加载；路径相对于客户端进程的当前工作目录。

## 15. 参考文档

1. `community/docs/zh/05-basic/03-query.md`
2. `community/docs/zh/14-reference/03-taos-sql/20-select.md`
3. `community/source/libs/parser/inc/sql.y`
4. `community/source/libs/parser/src/parTranslater.c`
5. `community/source/client/src/clientImpl.c`

## 16. 附录

### 16.1 验收要求

需要覆盖至少以下功能验收面：

1. `SELECT * FROM FILE(...)`。
2. `SELECT * FROM (SELECT ... FROM FILE(...))`。
3. 真实表与 FILE 的 JOIN（LEFT/RIGHT/INNER）。
4. FILE 作为 EXTERNAL_WINDOW 窗口定义来源。
5. `INSERT INTO ... SELECT ... FROM FILE(...)`。
6. `header=true`、`header=false`、`column_list` 列数少于实际文件列数（部分列读取）。
7. 重复列名、缺失列名、`SELECT projectlist` 引用不存在列、列数不匹配、类型转换失败等错误场景。
8. `column_list` 中不含 `timestamp` 列时：普通查询可用，JOIN 及时间窗口/插值/EXTERNAL_WINDOW 不可用，应正确报错。
9. FILE 选项 `header`、`delimiter` 的生效场景。
10. FILE 中整数、浮点、字符串、时间戳等类型转换场景。
11. 超出行数、单元格数、序列化大小限制的报错场景。
12. 绝对路径和相对路径均可正确访问。
