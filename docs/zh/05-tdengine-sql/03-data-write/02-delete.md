---
sidebar_label: 删除
title: 数据删除
description: 删除指定表或超级表中的数据
---

## 删除语法

`DELETE` 语句用于删除指定表或超级表中的数据，便于清理因设备故障等原因产生的异常数据。

```sql
DELETE FROM [db_name.]tb_name [WHERE condition] [SECURE_DELETE];
```

**注意**：删除数据并不会立即释放磁盘空间，而是将数据标记为已删除。查询时这些数据不会再出现，但磁盘空间的释放会延迟到系统自动清理（建库参数 [`KEEP`](../02-ddl/01-database.md#keep) 生效）或用户手动进行 [数据重整](../../12-operations-and-tooling/02-operations/04-maintenance.md#数据重整) 时（企业版功能 `COMPACT`）。若需在删除标记之外对落盘数据块做物理覆写，可使用库级 / 语句级 `SECURE_DELETE`，行为见 [数据安全 · 安全删除](../../11-security-guide/03-data-security.md#安全删除)。

### 参数说明

- `db_name`：可选参数，指定表所在的数据库名；不指定时使用当前数据库。
- `tb_name`：必填参数，指定要删除数据的表名，可以是普通表、子表或超级表。
- `condition`：可选参数，指定删除数据的过滤条件。不指定过滤条件时会删除表中的全部数据，请慎重使用。`WHERE` 条件仅支持对第一列主键时间列进行过滤。
- `SECURE_DELETE`：可选关键字。指定后，本次删除在写入删除标记之外，会对命中区间的落盘数据块做物理覆写；也可通过数据库选项 `SECURE_DELETE 1` 对库内删除默认开启（见 [数据库 · SECURE_DELETE](../02-ddl/01-database.md#secure_delete)）。

### 特别说明

数据删除后不可恢复，请慎重使用。为确保待删除数据符合预期，建议先使用 `SELECT` 语句加上与删除操作相同的 `WHERE` 条件查看数据内容，确认无误后再执行 `DELETE` 语句。

## 示例

`meters` 是一个超级表，`groupId` 是 `INT` 类型的标签列。删除 `meters` 中时间戳小于 `2021-10-01 10:40:00.100` 的全部数据，SQL 如下：

```sql
DELETE FROM meters WHERE ts < '2021-10-01 10:40:00.100';
```

执行后显示结果为：

```text
Deleted 102000 row(s) from 1020 table(s) (0.421950s)
```

表示从 1020 个子表中共删除了 102000 行数据。
