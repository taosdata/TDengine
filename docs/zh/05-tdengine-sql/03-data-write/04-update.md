---
sidebar_label: 更新
title: 数据更新
description: 使用 UPDATE 语句修改表、子表或超级表中的数据和标签
---

`UPDATE` 语句用于修改已经写入的时序数据，或修改子表的标签值（点位信息）。典型场景包括：修正设备上报的错误读数、按条件批量校正某段时间内的数据、补录缺失的列值，以及维护测点的位置、分组等属性。

下面的例子沿用 [数据写入](./01-insert.md) 中的表：超级表 `meters` 的数据列是 `ts`、`current`、`voltage`、`phase`，标签列是 `location`、`groupId`；`d1001` 和 `d1002` 是它的子表。

## 更新语法

```sql
UPDATE [db_name.]tb_name
  SET col_name = expr [, col_name = expr] ...
  [WHERE condition];
```

### 参数说明

- `db_name`：可选，指定表所在的数据库；不指定时使用当前数据库。
- `tb_name`：必填，要更新的表，可以是普通表、子表或超级表。对超级表执行更新时，作用于满足条件的所有子表。
- `col_name`：要修改的列，可以是普通数据列或标签列，规则见 [可更新的列](#可更新的列)。
- `expr`：新值表达式，可以是：
  - 常量，例如 `220`、`'California.SanJose'`、`NULL`。
  - 同一行中其他列或标签的引用，例如 `current * 1.05`、`voltage + 1`。
  - 标量函数与运算符的组合，例如 `ROUND(current, 2)`、`CAST(voltage AS DOUBLE) / 1000`。
  - 不支持聚合函数、选择函数、窗口函数、时序特有函数和子查询。
- `condition`：可选，指定要更新哪些行。支持的条件与对同一张表执行 `SELECT` 时的 `WHERE` 条件一致，可以组合使用主键时间列条件、标签列条件、`tbname` 条件和普通列条件，也可以使用 `AND`、`OR` 和括号。不支持子查询。省略 `WHERE` 时更新表中的全部数据，请谨慎使用。

### 可更新的列

| 列类型 | 是否可更新 | 说明 |
| --- | --- | --- |
| 主键时间戳列（第一列） | 否 | 时间戳是行的标识。如需移动数据的时间，请先写入新时间戳的数据，再删除旧数据。 |
| 复合主键列 | 否 | 复合主键与时间戳共同标识一行，不能修改。 |
| 普通数据列 | 是 | 修改命中行的列值，未出现在 `SET` 中的列保持原值。 |
| 标签列 | 是（仅超级表、子表） | 修改命中子表的标签值，见 [更新标签](#更新标签)。 |

更新主键列时返回错误 `Cannot update primary key column 'col_name'`。同一个 `SET` 子句中不能同时出现普通数据列和标签列，也不能重复出现同一列。

新值的类型转换规则与 `INSERT` 写入相同：数值可以写入范围足够的数值列，字符串长度不能超过列定义的长度，超出范围或长度时返回错误，不会截断。不能把 `NULL` 写入不允许为空的列。

## 执行语义

### 更新数据列

`UPDATE` 按以下步骤执行：

1. 按 `WHERE` 条件找到要更新的行。
2. 对每一行，以该行更新前的列值计算 `SET` 中的表达式。同一语句中的多个表达式都使用更新前的值，与列在 `SET` 中的先后顺序无关。
3. 保持主键不变，用新值替换 `SET` 中的列，其他列保持原值，然后写回该行。

`UPDATE` 只修改已存在的行，不会新增行。没有行满足条件时，语句执行成功，更新行数为 0。

### 更新标签

当 `SET` 中只包含标签列时，`UPDATE` 修改满足条件的子表的标签值，等价于对这些子表逐一执行 `ALTER TABLE ... SET TAG`（见 [数据表 · 修改标签值](../02-ddl/02-table.md#修改标签值)）。此时：

- `WHERE` 条件只能使用标签列和 `tbname`，用于选择子表；不能使用时间列和普通数据列。
- 更新数量按子表计算。
- 新标签值对之后的查询立即生效，依赖标签分组的查询、流计算和订阅使用新值。
- `JSON` 类型标签不能通过 `UPDATE` 修改，请使用 `ALTER TABLE ... SET TAG`。

### 原子性与一致性

- **单行原子**：同一行的所有新值一次写入，查询不会读到只更新了一部分列的行。
- **语句不保证全局原子**：一条 `UPDATE` 命中的子表可能分布在多个 vnode 上，每个 vnode 独立执行。某个 vnode 失败时，不会回滚其他 vnode 已完成的更新，语句返回错误。行为与 `INSERT` 的 [部分写入成功](./01-insert.md#部分写入成功) 一致。
- **重试**：`SET` 中只使用常量时，重新执行同一语句得到相同结果，失败后可以直接重试。`SET` 引用被更新列自身时（例如 `SET current = current * 1.05`），重复执行会再次计算，请先用 `SELECT` 确认哪些行已经更新，再缩小 `WHERE` 范围重试。
- **并发写入**：`UPDATE` 与针对同一行的 `INSERT` 并发执行时，以后完成的写入为准。

### 与 INSERT 覆盖写入的区别

写入相同主键的数据时，`INSERT` 会用新行覆盖旧行。两者的区别如下：

| 对比项 | `INSERT` 覆盖写入 | `UPDATE` |
| --- | --- | --- |
| 定位行 | 由写入的主键值确定，一次只覆盖指定的行 | 由 `WHERE` 条件确定，可以一次修改大量行 |
| 行不存在时 | 新增一行 | 不新增，更新行数为 0 |
| 基于原值计算 | 不支持 | 支持，例如 `SET current = current * 1.05` |
| 修改标签 | 不支持 | 支持 |

### 对存储、订阅和流计算的影响

- 更新后的行按相同主键的新数据写入存储，旧版本在数据重整时清理，与 `INSERT` 覆盖写入一致。
- 数据订阅会收到更新后的行。
- 流计算把更新视为对已有时间点的数据修改，按流的乱序与更新处理选项重新计算，见 [流式计算](../../07-stream-processing/index.md)。

## 返回结果

执行成功后返回更新的行数和涉及的表数：

```text
Updated 120 row(s) from 3 table(s) (0.035210s)
```

更新标签时返回更新的子表数：

```text
Updated tags of 2 table(s) (0.012034s)
```

## 权限

| 操作 | 所需权限 |
| --- | --- |
| 更新数据列 | 表的 `SELECT` 权限和 `INSERT` 权限。若 `INSERT` 权限限定了列，`SET` 中的列必须都在授权列中；若权限带有行条件（`WITH`），只能更新满足行条件的数据。 |
| 更新标签列 | 表的 `ALTER` 权限，与执行 `ALTER TABLE ... SET TAG` 相同。 |

授权语法见 [权限管理](../07-user-and-privilege/02-grant.md)。

## 示例

### 修正单个时间点

将 `d1001` 在 `2021-07-13 14:06:32.272` 的电流修正为 `10.3`：

```sql
UPDATE d1001 SET current = 10.3 WHERE ts = '2021-07-13 14:06:32.272';
```

### 同时修改多列

```sql
UPDATE d1001
SET current = 10.3, voltage = 219, phase = 0.31
WHERE ts = '2021-07-13 14:06:32.272';
```

### 按条件批量修正

将 `groupId` 为 2 的设备在指定一小时内电压超过 250V 的异常读数统一修正为 220V：

```sql
UPDATE meters
SET voltage = 220
WHERE groupId = 2
  AND ts >= '2021-10-01 10:00:00'
  AND ts <  '2021-10-01 11:00:00'
  AND voltage > 250;
```

### 基于原值计算

传感器校准后发现 `d1002` 某天的电流偏低 5%，按原值校正：

```sql
UPDATE d1002
SET current = current * 1.05
WHERE ts >= '2021-10-01 00:00:00' AND ts < '2021-10-02 00:00:00';
```

### 将列值置空

```sql
UPDATE d1001 SET phase = NULL WHERE ts = '2021-07-13 14:06:32.272';
```

### 更新点位信息

将测点 `d1001` 的位置改为 `California.SanJose`：

```sql
UPDATE meters SET location = 'California.SanJose' WHERE tbname = 'd1001';
```

将位于 `California.SanFrancisco` 的所有测点调整到第 3 组：

```sql
UPDATE meters SET groupId = 3 WHERE location = 'California.SanFrancisco';
```

更新后可以查询点位信息确认结果：

```sql
SELECT
  DISTINCT tbname,
  location,
  groupId
FROM meters;
```

## 通过参数绑定执行更新

使用 STMT / STMT2 接口时，可以预编译 `UPDATE` 语句，再批量绑定参数执行，适合高频修正单点数据的程序：

```sql
UPDATE meters SET current = ?, voltage = ? WHERE tbname = ? AND ts = ?
```

参数绑定方式的限制：

- `SET` 中的新值必须是 `?` 占位符。
- `WHERE` 中必须用 `?` 给出全部主键：时间戳列，以及复合主键列（如有）。
- `WHERE` 中的其他条件与 SQL 中的 `UPDATE` 含义相同，只有满足全部条件的行才会被更新。
- 不支持更新标签列。

接口用法见 [参数绑定](../../10-developer-guide/03-stmt.md)。

## 使用限制

- 不支持对虚拟表、视图、系统表和数据库 `information_schema`、`performance_schema` 中的表执行 `UPDATE`。
- 不支持 `ORDER BY`、`LIMIT`、`JOIN` 和多表 `UPDATE`。
- 不能修改主键时间戳列和复合主键列。
- `UPDATE` 需要先读取再写回命中的行，耗时约等于同条件的 `SELECT` 加上同等数据量的写入。大批量修正建议按时间范围分批执行，并尽量在 `WHERE` 中给出时间范围和标签条件，缩小扫描范围。

## 建议

- 执行前先用相同的 `WHERE` 条件执行 `SELECT COUNT(*)`，确认命中行数符合预期。
- 对重要数据，先用 `SELECT` 把待更新的行导出备份，再执行 `UPDATE`。
- 需要追踪修改记录时，开启 [审计日志](../../11-security-guide/07-audit-and-compliance.md)。`auditLevel` 为 5 时，`UPDATE` 语句与 `INSERT`、`DELETE` 一样记录到审计日志中。
