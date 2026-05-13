# group by 子句支持使用位置语法和结果集列名

## 1. 背景

TS-5137

在 FineBI 的适配过程中发现的问题。现在的 group by 子句只支持列名，不支持位置语法，如 group by 1,2 这种形式，也不支持使用 SELECT 的结果集列名进行 group by ，如下列语句：
```javascript
select
   t1.`stat` as `__fcol_0`,
   min(t1.`hig`) as `__fcol_1`,
   sum(t1.`val`) as `__fcol_2`,
   count(1) as `__fcol_3`
from `db`.`tb` as t1 
group by __fcol_0;
```

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/7/5 | 0.1 | 司马靖 |  |

## 3. 定义

无。

## 4. 行为说明

#### 4.0.1 语法

group by 的查询语法从原先的
```javascript
group_by_clause:  
    GROUP BY expr [, expr] ... HAVING condition
```

变更为
```javascript
group_by_clause:        
    GROUP BY group_expr [, group_expr] ... HAVING condition

group_epxr:
    {expr | position | c_alias}
```

并且由于 partition by 完全兼容 group by，所以对于 partition by 的语法也从原先的
```javascript
partition_by_clause:
    PARTITION BY expr [, expr] ...
```

修改为
```javascript
partition_by_clause:
    PARTITION BY partition_expr [, partition_expr] ...

partition_expr:
    {expr | position | c_alias}
```

#### 4.0.2 支持的行为

支持**位置语法**
group_expr 接受的表达式为正整数的位置标识，从 1 开始，表示使用 SELECT 列表的第几个表达式进行 GROUP BY。
支持**结果集列名**
group_expr 接受的表达式为 SELECT 语句的结果集列名。可以根据结果集列名对 SELECT 列表里的某一列进行 GROUP BY。
支持**非聚集函数表达式** 
group_expr 接受的表达式为任意的标量表达式，包括列、常量、标量函数和它们的组合。可以根据标量表达式的值进行 GROUP BY。

#### 4.0.3 不支持的行为

不支持位置语法/结果集列名对应的 SELECT 列表里面的表达式是聚集函数。
不支持 group by 子句中包含聚集函数。

## 5. 性能

无。

## 6. 兼容性

破坏原有的行为。
原行为：
GROUP BY 子句中的正整数会被当成 expr 来处理，所以对于以下语句
```javascript
select 1, min(t1.`current`) 
    from `db`.`meters` as t1 
group by 1;
```

原先也是可以成功执行的，但是对于语句
```javascript
select t1.voltage, min(t1.`current`) 
    from `db`.`meters` as t1 
group by 1;
```

就不能成功执行，因为出现在 select 语句中的 expr 必须出现在 group by 子句中，而 `t1.voltage` 在 group by 子句中没有对应的 expr，所以会报错。
新行为：
原行为：
GROUP BY 子句中的正整数会被当成 select 列表中的位置来处理，所以对于以下语句
```javascript
select t1.voltage, min(t1.`current`) 
    from `db`.`meters` as t1 
group by 1;
```

是可以成功执行的，因为 group by 1 代表按照 select 列表中的 `t1.voltage` 分组，select 列表中的非聚集函数表达式都出现在 group by 子句里，所以不会报错。

## 7. 运维

无

## 8. 使用场景

使用如下建表语句创建超级表 `meters`
```plaintext {wrap}
create stable meters (ts timestamp, current float, voltage int, phase float) TAGS (groupid int, location VARCHAR(24));
```

使用如下的 group by 查询应该是等价的：
**正常查询**
```plaintext
select 
   t1.`current` as `__fcol_0`,
   min(t1.`voltage`) as `__fcol_1`,
   sum(t1.`phase`) as `__fcol_2`,
   count(1) as `__fcol_3`
from `db`.`meters` as t1 
group by t1.`current`;
```

**使用位置语法**
```plaintext
select 
   t1.`current` as `__fcol_0`,
   min(t1.`voltage`) as `__fcol_1`,
   sum(t1.`phase`) as `__fcol_2`,
   count(1) as `__fcol_3`
from `db`.`meters` as t1 
group by 1;
```

**使用别名**
```plaintext
select 
   t1.`current` as `__fcol_0`,
   min(t1.`voltage`) as `__fcol_1`,
   sum(t1.`phase`) as `__fcol_2`,
   count(1) as `__fcol_3`
from `db`.`meters` as t1 
group by `__fcol_0`;
```

**不支持的场景**
还是以查询：
```plaintext
select 
   t1.`current` as `__fcol_0`,
   min(t1.`voltage`) as `__fcol_1`,
   sum(t1.`phase`) as `__fcol_2`,
   count(1) as `__fcol_3`
from `db`.`meters` as t1 
group by t1.`current`;
```

为例。
```plaintext
select 
   t1.`current` as `__fcol_0`,
   min(t1.`voltage`) as `__fcol_1`,
   sum(t1.`phase`) as `__fcol_2`,
   count(1) as `__fcol_3`
from `db`.`meters` as t1 
group by 1;
```

以 SELECT 列表里第一列进行 GROUP BY 是可以的，因为这一列的表达式是列名。
但是
```plaintext
select 
   t1.`current` as `__fcol_0`,
   min(t1.`voltage`) as `__fcol_1`,
   sum(t1.`phase`) as `__fcol_2`,
   count(1) as `__fcol_3`
from `db`.`meters` as t1 
group by 2;
```

就不可以了，因为 SELECT 列表里第二列是聚集函数，不能以聚集函数的结果来进行 GROUP BY。

## 9. 约束和限制

约束：不支持位置语法/结果集列名对应的 SELECT 列表里面的表达式是聚集函数。

## 10. 常见错误和排查

无。

## 11. 可观测性

无。

## 12. 安装和卸载

无。

## 13. 文档

需要修改企业版文档
需要修改官网文档
需要修改文档中 **SQL手册-数据查询-查询语法** 和  **SQL手册-数据查询-GROUP BY** 两部分。

**查询语法** 
```javascript
SELECT {DATABASE() | CLIENT_VERSION() | SERVER_VERSION() | SERVER_STATUS() | NOW() | TODAY() | TIMEZONE() | CURRENT_USER() | USER() }

SELECT [hints] [DISTINCT] [TAGS] select_list
    from_clause
    [WHERE condition]
    [partition_by_clause]
    [interp_clause]
    [window_clause]
    [group_by_clause]
    [order_by_clasue]
    [SLIMIT limit_val [SOFFSET offset_val]]
    [LIMIT limit_val [OFFSET offset_val]]
    [>> export_file]

中间部分没有变化，省略掉了

- group_by_clause:
-     GROUP BY expr [, expr] ... HAVING condition
+ group_by_clause：
+     GROUP BY group_expr [, group_expr] ... HAVING condition
+ 
+ group_expr:
+     {expr | position | c_alias}

order_by_clasue:
    ORDER BY order_expr [, order_expr] ...

order_expr:
    {expr | position | c_alias} [DESC | ASC] [NULLS FIRST | NULLS LAST]
```

**GROUP BY**  
如果在语句中同时指定了 GROUP BY 子句，那么 SELECT 列表只能包含如下表达式：
1. 常量
2. 聚集函数
3. 与 GROUP BY 后表达式相同的表达式。
4. 包含前面表达式的表达式
GROUP BY 子句对每行数据按 GROUP BY 后的表达式的值进行分组，并为每个组返回一行汇总信息。
- GROUP BY 子句中的表达式可以包含表或视图中的任何列，这些列不需要出现在 SELECT 列表中。
+ GROUP BY 子句中可以通过指定表或视图的列名来按照表或视图中的任何列分组，这些列不需要出现在 SELECT 列表中。
+ GROUP BY 子句中可以使用位置语法，位置标识为正整数，从 1 开始，表示使用 SELECT 列表的第几个表达式进行分组。
+ GROUP BY 子句中可以使用结果集列名，表示使用 SELECT 列表的指定表达式进行分组。
+ GROUP BY 子句中在使用位置语法和结果集列名进行分组时，其对应的 SELECT 列表中的表达式不能是聚集函数。
该子句对行进行分组，但不保证结果集的顺序。若要对分组进行排序，请使用 ORDER BY 子句
 
## 14. 参考文档

Mysql group by：
https://dev.mysql.com/doc/refman/5.7/en/select.html
https://dev.mysql.com/doc/refman/8.0/en/group-by-handling.html

## 15. 附录

无。
