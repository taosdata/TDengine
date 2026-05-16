# HAVING子句

优化目标：HAVING 子句 可用在PARTITION BY和Window子句之后

### 1. 语法

```sql
SELECT [DISTINCT] select_list
    from_clause
    [WHERE condition]
    [partition_by_clause ]
    [window_clause]
    [group_by_clause]
    [having_clause]
    [order_by_clasue]
    [SLIMIT limit_val [SOFFSET offset_val]]
    [LIMIT limit_val [OFFSET offset_val]]
```

将 having_clause 从 group_by_clause 中提取出来，使其可以与 partition_by_clause 和 window_clause 一起使用，增强单层 SQL 的功能完整性。

### 2. 语义

having_clause 在语义上位于 partition_by_clause、window_clause 和 group_by_clause 等子句之后，即表示在进行上述子句之后再进行过滤。
当 having_clause 用在 group_by_clause 之后时，其中只能包含：
- 常量。
- 聚集函数。
- 与 GROUP BY 后表达式相同的表达式。
- 包含前面表达式的表达式。
当 having_clause 用在 window_clause 之后时，其中只能包含：
- 常量。
- 聚集函数。
- 包含上面表达式的表达式。

### 3. 举例

在数据切分后过滤数据，例如：
```sql
taos> select tbname, i from st1 partition by tbname having i > 5;
             tbname             |      i      |
===============================================
 st1s2                          |          10 |
 st1s3                          |          10 |
```

在窗口查询后过滤数据，例如：
```sql
taos> select tbname, count(*) from st1 partition by tbname interval(10s) having count(*) = 1;
             tbname             |       count(*)        |
=========================================================
 st1s2                          |                     1 |
 st1s3                          |                     1 |
```
