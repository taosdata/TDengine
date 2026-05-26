# TDengine EXPLAIN 快速上手

当用户主要想了解 `EXPLAIN` 的基本用法时，读取此参考文档。

## 语法

```sql
EXPLAIN [ANALYZE] [VERBOSE {true | false}] query_or_subquery;
```

核心含义：

- `EXPLAIN`：以 `QUERY_PLAN` 的形式展示执行计划
- `ANALYZE`：实际执行查询并附带运行时指标
- `VERBOSE true`：展示算子的详细属性，例如 `Output`、`Filter`、`Tag Index Filter`、`Time Range`、`Time Window`、`Network`、`Exec cost`

## 何时推荐使用哪种形式

- 当用户只想看静态计划形态时，先用 `EXPLAIN`。
- 当用户需要知道时间到底花在哪里时，用 `EXPLAIN ANALYZE`。
- 当用户需要确认谓词下推、排序键、窗口参数或网络行为时，再加 `VERBOSE true`。

## 最小示例

```sql
EXPLAIN SELECT * FROM meters WHERE ts >= now - 1h;
```

```sql
EXPLAIN ANALYZE SELECT tbname, avg(current) FROM meters INTERVAL(1m);
```

```sql
EXPLAIN ANALYZE VERBOSE true
SELECT * FROM (SELECT ts, current FROM meters WHERE location = 'beijing') t;
```

## 长结果输出

- taos 客户端默认可能折叠过长的查询结果，导致 `QUERY_PLAN` 或 `EXPLAIN ANALYZE` 输出不完整。
- 这时可在 SQL 末尾、分号前追加 `\G`，强制按纵向完整显示结果。

示例：

```sql
EXPLAIN ANALYZE VERBOSE true SELECT * FROM meters WHERE ts >= now - 1h \G;
```

如果用户贴来的输出明显被截断、折叠或缺少后续字段，优先提示其使用 `\G` 重新获取完整结果。

## 快速阅读规则

- 自顶向下阅读，先理解最终结果是如何生成的。
- 自底向上回看，定位真正的成本驱动因素。
- 优先关注 `Table Scan`、`Filter`、`Sort`、`Data Exchange` 和 `Exec cost`。

## 回答模板

当用户只是问用法时，按以下顺序回答：

1. 用一句话说明 `EXPLAIN` 的用途
2. 解释 `EXPLAIN`、`ANALYZE`、`VERBOSE` 的区别
3. 给出两到三条可执行示例
4. 给出一条解读 `QUERY_PLAN` 的简短规则
