---
sidebar_label: 数据类型与精度
title: 数据类型与精度
description: TDengine 支持的数据类型、时间单位、时间戳、字面量和常量说明
---

本文介绍 TDengine SQL 中可使用的数据类型、时间单位、时间戳规则以及常量写法。

## 数据类型总览

在 TDengine 中，普通表、子表和超级表可以使用以下数据类型。部分类型只能用于特定位置，具体限制见后续章节。

| 类型                | 存储空间       | 说明 |
| ---                 | ---            | --- |
| `TIMESTAMP`         | 8 字节         | 时间戳。默认精度为毫秒，可在创建数据库时设置为微秒或纳秒。 |
| `BOOL`              | 1 字节         | 布尔类型。 |
| `TINYINT`           | 1 字节         | 有符号单字节整数，范围 `[-128, 127]`。 |
| `TINYINT UNSIGNED`  | 1 字节         | 无符号单字节整数，范围 `[0, 255]`。 |
| `SMALLINT`          | 2 字节         | 有符号短整数，范围 `[-32768, 32767]`。 |
| `SMALLINT UNSIGNED` | 2 字节         | 无符号短整数，范围 `[0, 65535]`。 |
| `INT`               | 4 字节         | 有符号整数，范围 `[-2^31, 2^31-1]`。 |
| `INT UNSIGNED`      | 4 字节         | 无符号整数，范围 `[0, 2^32-1]`。 |
| `BIGINT`            | 8 字节         | 有符号长整数，范围 `[-2^63, 2^63-1]`。 |
| `BIGINT UNSIGNED`   | 8 字节         | 无符号长整数，范围 `[0, 2^64-1]`。 |
| `FLOAT`             | 4 字节         | 单精度浮点数，有效位数约 6-7 位，范围约 `[-3.4E38, 3.4E38]`。 |
| `DOUBLE`            | 8 字节         | 双精度浮点数，有效位数约 15-16 位，范围约 `[-1.7E308, 1.7E308]`。 |
| `BINARY`            | 自定义         | 单字节字符串，建议只用于 ASCII 可见字符。 |
| `VARCHAR`           | 自定义         | `BINARY` 类型的别名。 |
| `NCHAR`             | 自定义         | 多字节字符串，适合中文等多字节字符。 |
| `VARBINARY`         | 自定义         | 可变长二进制数据。 |
| `GEOMETRY`          | 自定义         | 几何类型，支持 2D 的 `POINT`、`LINESTRING` 和 `POLYGON`。 |
| `DECIMAL`           | 8 或 16 字节   | 高精度数值类型，取值范围由 `precision` 和 `scale` 决定。 |
| `BLOB`              | 最大 4 MB      | 大对象二进制数据。 |
| `JSON`              | 自定义         | JSON 标签类型，只能用于标签列。 |

## 通用限制

- 表的每行长度不能超过 48 KB（自 `v3.0.5.0` 起为 64 KB）。每个 `BINARY`、`NCHAR`、`GEOMETRY`、`VARBINARY` 类型的列还会额外占用 2 个字节的存储位置。
- `BINARY`、`VARBINARY` 和 `GEOMETRY` 类型的数据列最大长度为 65,517 字节，标签列最大长度为 16,382 字节。
- `BLOB` 类型单列值最大长度为 4,194,304 字节。
- `JSON` 类型只能用于标签列。如果使用 JSON 标签，则标签列只能有一个。
- `DECIMAL` 类型仅支持普通列，暂不支持标签列。

数据库、表、列和标签等命名限制，参见 [命名与边界](./11-appendix/02-limit.md)。

## 时间单位

TDengine SQL 中凡需要指定时间长度的场合（时间运算、INTERVAL、EVERY、SLIDING 等），均使用单字符后缀表示单位。支持的时间单位从小到大如下：

| 单位字符 | 含义     | 说明 |
| :---:    | ---      | --- |
| `b`      | 纳秒     | 最小精度单位，仅在数据库精度为纳秒时有实际意义。 |
| `u`      | 微秒     | 仅在数据库精度为微秒或纳秒时有实际意义。 |
| `a`      | 毫秒     | 数据库默认精度。 |
| `s`      | 秒       | |
| `m`      | 分钟     | |
| `h`      | 小时     | |
| `d`      | 天       | |
| `w`      | 周       | 固定为 7 天。 |
| `n`      | 自然月   | 日历单位，仅可用于 `INTERVAL` 窗口，不可用于时间运算、`EVERY`、`SURROUND` 等场合。 |
| `q`      | 自然季度 | 日历单位，等价于 3 个自然月，仅可用于 `INTERVAL` 窗口，不可用于时间运算、`EVERY`、`SURROUND` 等场合。 |
| `y`      | 自然年   | 日历单位，仅可用于 `INTERVAL` 窗口，不可用于时间运算、`EVERY`、`SURROUND` 等场合。 |

时间单位大小写均可（如 `1S` 与 `1s` 等价）。
关于时区和自然时间单位的完整语义，参见 [时区与自然时间单位](./10-time/01-timezone.md)。

## 时间戳

时间戳是 TDengine 中时序数据的主键。创建表、写入数据和查询历史数据时，通常都需要指定时间戳。

- 时间字符串格式为 `YYYY-MM-DD HH:mm:ss.MS`，默认时间分辨率为毫秒，例如 `2017-08-12 18:25:58.128`。
- `NOW` 表示客户端当前时间。写入数据时，如果时间戳为 `NOW`，则使用提交该记录的客户端当前时间。
- 时间戳也可以写成长整数，表示从 UTC 时间 `1970-01-01 00:00:00` 开始经过的时间。长整数的单位由数据库时间精度决定：毫秒精度表示毫秒数，微秒精度表示微秒数，纳秒精度表示纳秒数。
- 时间表达式支持加减运算，例如 `NOW - 2h` 表示当前时间向前推 2 小时。时间单位参见 [时间单位](#时间单位)。

TDengine 默认时间戳精度为毫秒。创建数据库时，可以通过 `PRECISION` 参数设置为微秒或纳秒。

```sql
CREATE DATABASE db_name PRECISION 'ns';
```

`PRECISION` 参数的完整说明参见 [创建数据库](./02-ddl/01-database.md#precision)。

## 字符串、二进制与空间类型

### BINARY 和 VARCHAR

`BINARY` 用于存储单字节字符串，`VARCHAR` 是 `BINARY` 的别名。建议只在 `BINARY`/`VARCHAR` 中存储 ASCII 可见字符，中文等多字节字符请使用 `NCHAR`。如果强行使用 `BINARY` 保存中文字符，虽然有时可以读写，但由于不带字符集信息，容易出现乱码或数据损坏。

使用 `BINARY` 或 `VARCHAR` 时需要指定长度，例如 `BINARY(20)` 表示最多存储 20 个单字节字符。字符串两端使用单引号引用，字符串内的单引号可以使用转义字符 `\'`。

### NCHAR

`NCHAR` 用于存储包含多字节字符的字符串，例如中文字符。每个 `NCHAR` 字符占用 4 字节存储空间。使用时需要指定字符长度，例如 `NCHAR(10)` 表示最多存储 10 个 `NCHAR` 字符。如果写入字符串长度超过声明长度，将返回错误。

### VARBINARY

`VARBINARY` 用于存储可变长二进制数据。可以通过 SQL 或无模式写入（schemaless）方式写入，此时需要转换为 `\x` 开头的字符串；也可以通过 `STMT` 方式直接绑定二进制数据。查询显示时，结果以 `\x` 开头的十六进制形式返回。

### GEOMETRY

`GEOMETRY` 用于存储 2D 几何对象，支持 `POINT`、`LINESTRING` 和 `POLYGON` 子类型。长度计算方式如下表所示。

| 语法 | 最小长度 | 最大长度 | 每组坐标长度增长 |
| --- | --- | --- | --- |
| `POINT(1.0 1.0)` | 21 | 21 | 无 |
| `LINESTRING(1.0 1.0, 2.0 2.0)` | `9+2*16` | `9+4094*16` | `+16` |
| `POLYGON((1.0 1.0, 2.0 2.0, 1.0 1.0))` | `13+3*16` | `13+4094*16` | `+16` |

## 高精度数值类型 DECIMAL

`DECIMAL` 数据类型用于高精度数值存储。定义语法为 `DECIMAL(18, 2)`、`DECIMAL(38, 10)`，其中 `precision` 表示最大有效数字个数，`scale` 表示最大小数位数。例如，`DECIMAL(8, 4)` 可表示的范围为 `[-9999.9999, 9999.9999]`。

定义 `DECIMAL` 数据类型时，`precision` 的范围为 `[1, 38]`，`scale` 的范围为 `[0, precision]`。当 `scale` 为 0 时，仅表示整数。也可以不指定 `scale`，默认为 0，例如 `DECIMAL(18)` 与 `DECIMAL(18, 0)` 相同。

当 `precision` 不大于 18 时，内部使用 8 字节存储（`DECIMAL64`）；当 `precision` 范围为 `(18, 38]` 时，使用 16 字节存储（`DECIMAL`）。SQL 中写入 `DECIMAL` 类型数据时，可以直接使用数值写入。当写入值大于类型可表示的最大值时，会返回 `DECIMAL_OVERFLOW` 错误；当写入值未超过最大值但小数位数超过 `scale` 时，会自动四舍五入处理。例如，定义类型为 `DECIMAL(10, 2)`，写入 `10.987`，实际存储值为 `10.99`。

`DECIMAL` 类型支持 SQL 和 `STMT2` 写入，暂不支持无模式写入（schemaless）。

整数类型和 `DECIMAL` 类型一起运算时，会将整数类型转换为 `DECIMAL` 类型后再计算。`DECIMAL` 类型与 `DOUBLE`、`FLOAT`、`VARCHAR`、`NCHAR` 等类型一起运算时，会转换为 `DOUBLE` 类型后再计算。

查询 `DECIMAL` 类型表达式时，如果计算的中间结果超出当前类型可表示的最大值，将返回 `DECIMAL OVERFLOW` 错误。

## 大对象类型 BLOB

`BLOB` 用于存储较大的二进制数据，最大长度为 4,194,304 字节。可以通过 SQL 或 `STMT2` 写入二进制数据，也可以转换为 `\x` 开头的字符串写入。

通过 shell 查询 `BLOB` 数据时，显示为以 `\x` 开头的十六进制字符串。

限制：

- 仅支持在普通数据列中使用 `BLOB` 类型，`BLOB` 列数目不能超过 1 个。
- 不支持对 `BLOB` 列进行条件过滤。

其他限制：

- 不支持虚拟表、流式计算等功能。

## JSON 标签

`JSON` 类型只能用于标签列。如果使用 JSON 标签，则标签列只能有一个。

### 语法说明

1. 创建 JSON 类型标签。

   ```sql
   CREATE STABLE s1 (ts TIMESTAMP, v1 INT) TAGS (info JSON);

   CREATE TABLE s1_1 USING s1 TAGS ('{"k1": "v1"}');
   ```

2. 使用 JSON 取值操作符 `->`。

   ```sql
   SELECT * FROM s1 WHERE info->'k1' = 'v1';

   SELECT info->'k1' FROM s1;
   ```

3. 使用 `CONTAINS` 判断 JSON key 是否存在。

   ```sql
   SELECT * FROM s1 WHERE info CONTAINS 'k2';

   SELECT * FROM s1 WHERE info CONTAINS 'k1';
   ```

### 支持的操作

- 在 `WHERE` 条件中，支持 `MATCH`、`NMATCH`、`BETWEEN ... AND`、`LIKE`、`AND`、`OR`、`IS NULL`、`IS NOT NULL`，不支持 `IN`。

  ```sql
  SELECT * FROM s1 WHERE info->'k1' MATCH 'v*';

  SELECT * FROM s1 WHERE info->'k1' LIKE 'v%' AND info CONTAINS 'k2';

  SELECT * FROM s1 WHERE info IS NULL;

  SELECT * FROM s1 WHERE info->'k1' IS NOT NULL;
  ```

- 支持 JSON 标签放在 `GROUP BY`、`ORDER BY`、`JOIN` 子句、`UNION ALL` 以及子查询中，例如 `GROUP BY info->'key'`。
- 支持 `DISTINCT` 操作。
- 支持全量覆盖 JSON 标签值。
- 支持修改 JSON 标签名。
- 不支持添加 JSON 标签、删除 JSON 标签、修改 JSON 标签列宽。

### 其他约束

1. JSON 中 key 的长度不能超过 256 字节，并且必须为可打印 ASCII 字符；JSON 字符串总长度不能超过 4096 字节。
2. JSON 输入字符串可以为空（`""`、`"\t"`、`" "` 或 `NULL`）或 object，不能为非空字符串、布尔值或数组。
3. object 可以为 `{}`。如果 object 为 `{}`，则整个 JSON 串记为空。key 可以为 `""`，如果 key 为 `""`，则 JSON 串中忽略该 key-value 对。
4. value 可以为数字（int/double）、字符串、bool 或 null，暂不支持数组，不允许嵌套。
5. 如果 JSON 字符串中出现两个相同的 key，则第一个生效。
6. JSON 字符串暂不支持转义。
7. 查询 JSON 中不存在的 key 时，返回 NULL。
8. 当 JSON 标签作为子查询结果时，不再支持上层查询继续对子查询中的 JSON 串做解析查询。

例如暂不支持以下写法：

```sql
SELECT jtag->'key' FROM (SELECT jtag FROM stable);

SELECT jtag->'key' FROM (SELECT jtag FROM stable) WHERE jtag->'key' > 0;
```

## 常量

TDengine 支持以下常量写法。

- 整数字面量：例如 `123`、`+123`、`-123`，类型为 `BIGINT`。如果输入超过 `BIGINT` 表示范围，TDengine 按 `BIGINT` 对数值进行截断。
- 浮点数字面量：例如 `123.45`，类型为 `DOUBLE`。
- 科学计数法字面量：例如 `1.2E3`，类型为 `DOUBLE`。
- 字符串字面量：例如 `'abc'` 或 `"abc"`，类型为 `BINARY`，长度为实际字符个数。字符串内的单引号可以用 `\'` 表示，双引号可以用 `\"` 表示。
- 时间戳字面量：例如 `TIMESTAMP '2017-08-12 18:25:58.128'`，类型为 `TIMESTAMP`。字符串需要满足 `YYYY-MM-DD HH:mm:ss.MS` 格式，时间分辨率为当前数据库的时间分辨率。
- 布尔字面量：`TRUE` 或 `FALSE`，类型为 `BOOL`。
- 空值字面量：空字符串、制表符、空格或 `NULL`，可以用于任意类型。

SQL 语句中的数值类型会依据是否存在小数点或是否使用科学计数法来判断为整型或浮点型。使用时需要注意类型越界。例如，`9999999999999999999` 会被认为超过长整型上边界而溢出，而 `9999999999999999999.0` 会被认为是有效的浮点数。
