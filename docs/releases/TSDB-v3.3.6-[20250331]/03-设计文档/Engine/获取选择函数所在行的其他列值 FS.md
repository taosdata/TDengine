# 获取选择函数所在行的其他列值 FS

## 1. 会议信息

会议主题：获取选择函数所在行的其他列值
会议时间：Nov 27 (Wed) 17:30 - 17:55 (GMT+08)
参会人：@任新胜 @陈玉 @关胜亮 @刘艺博 @潘魏 @肖波 @张心治

## 2. 背景

需求见：[TS-5255](https://jira.taosdata.com:18080/browse/TS-5255)
对多行选择函数也可以同时选择所在行的其他列（本次不实现）。
类似现有的多行选择函数执行效果，多行的每一行可同时输出对应行的其他列值。如下：
```sql
taos> select tbname, bottom(current, 3), ts from meters;
    tbname  |  bottom(current, 3)  |           ts            |
==================================================================================
 d19        |            6.0298152 | 2017-07-14 10:40:00.023 |
 d76        |            6.0298152 | 2017-07-14 10:40:00.023 |
 d2         |            6.0298152 | 2017-07-14 10:40:00.023 |
```

当前版本： SELECT top(current, 5), bottom(current, 5) from meters; 不合法；本jiara 任务实现的同时，需要同时实现语句的正确查询。
可参考 [15.1 小节](https://taosdata.feishu.cn/wiki/BIBNwoaLAiwp6ikXigAcO0gQnKh#share-BRngdMETOo6Pw6xDfVccnoRNnMc)

## 3. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024-11-26 | 1.0 | 任新胜 | 新建 |

## 4. 定义

### 4.1 选择函数分类

单行选择函数，输出结果为一行：FIRST、LAST、LAST_ROW、MAX、MIN、MODE
多行选择函数，输出结果为多行:   BOTTOM、SAMPLE、TAIL、TOP、UNIQUE
特殊选择函数：INTERP , 插值函数，也是多行选择函数，但是输出结果中会对没有数据的时间点填充数据，因此不存在对应行和对应行的其他列
本次实现同时针对单行选择函数和多行选择函数，不包括 INTERP 函数

### 4.2 COLS 函数与选择函数关系

COLS 函数的第一个参数是选择函数，可以是多行选择函数，也可以是单行选择函数。
COLS 也是选择函数，当它的第一个参数 func 是单行选择函数时，COLS 本身是单行选择函数，当 func 是多行选择函数时，COLS 是多行选择函数

### 4.3 选择函数的选择列冲突

1. 单行选择函数的列冲突说明
select 多个选择函数时，不同选择函数输出的可能并不是同一行，此时如果想要在没有 COLS 函数的情况下输出具体的某一列值，则无法决定输出那行的该列，可称为“单行选择函数的选择列冲突”。
例如： select first(ts), last(ts), current from meters;
因为 first(ts), last(ts) 大概率不是同一行，无法决定输出哪个 current, 会导致冲突；因此会导致语法报错。
同样 select COLS(first(ts), current), COLS(last(ts), current), voltage from meters;  这个 sql 想要不使用 COLS 函数输出 voltage ， 也有选择冲突，会语法报错
1. 多行选择函数的列冲突说明
当使用多行选择函数时，结果输出多行，此时如果有输出行数不一样的多行选择函数或者单行选择函数，每行数据无法正确匹配，导致冲突。可称为“多行选择函数的选择列冲突”，语法层面禁止。
多行选择函数另外一种形式的“选择列冲突”是存在多个多行选择函数时，如果 select 具体某一列值，无法确定使用哪个选择函数结果的行来选择列值，导致冲突。语法层面禁止，可以通过 COLS 函数来限定使用哪一个选择函数的结果行来解决该冲突。
示例：
SELECT top(current, 5), bottom(current, 5), voltage from meters; 不合法
SELECT top(current, 5), bottom(current, 5), COLS(top(current, 5), voltage) from meters; 合法

## 5. 行为说明

本文档功能通过新增函数来完成，以下为相关说明。

### 5.1 新增函数说明

**关键字**：新增函数 COLS
**表达式**：COLS(select_function(expr), output_expr1, [, output_expr2] ... )
**选择函数使用别名的表达式: ** func(expr1) alias1, COLS(alias1,  expr2 [as alias2], expr3 [as alias3] [...])
**功能说明**：在选择函数 func(expr1) 执行结果所在数据行上，执行表达式 expr2, expr3 ……，返回其结果（func结果不输出）
**返回数据类型**：返回多列数据，每列数据类型为对应表达式返回结果的类型。
**适用于**：表和超级表。
**使用说明**:
1. func 函数类型：func 函数类型：必须是单行选择函数（输出结果为一行的选择函数，例如 last， 但 top 是多行选择函数）
2. func 可以是多列函数或者单列函数
3. 注意, 参数 func 的结果并没有返回，如果需要输出 func 结果，需要在 select 中加上相应的 func expr

使用规则附加说明（如果要放在官网，需要先加上[定义](https://taosdata.feishu.cn/wiki/BIBNwoaLAiwp6ikXigAcO0gQnKh#share-GgBMdBHZMoWwgAxeBQkc7JgtnBh)部分）：
1. COLS 函数参数的选择函数是单行选择函数时，自身可以视作单行选择函数；进行 [“选择函数的选择列冲突”  ](https://taosdata.feishu.cn/wiki/BIBNwoaLAiwp6ikXigAcO0gQnKh#share-HlWTdbQKeos5iDxZyUIclQZhn5Y)判断时，当存在多个 COLS 函数时，如果他们的第一个参数 func 表示的选择函数不相同时（函数名或者参数不相同），则认为存在“单行选择函数的列选择冲突”，此时 select 中不允许直接输出列。
   - 示例：select COLS(first(ts), current), COLS(last(ts), current), voltage from meters;  不允许
2. 当 COLS 函数的第一个参数 func 是多列选择函数时，同一个 select  不允许再有其他不一样的 COLS 函数，也不能有和 func 不一样的选择函数(包括函数名和参数)
   - select COLS(top(ts, 5), current), COLS(top(ts, 5), voltage) from meters; 合法
   - select top(ts, 5), COLS(top(ts, 5), current), COLS(top(ts, 5), voltage) from meters; 合法
   - select top(ts, 6), COLS(top(ts, 5), current), COLS(top(ts, 5), voltage) from meters; 不合法
   - select bottom(ts, 5), COLS(top(ts, 5), current), COLS(top(ts, 5), voltage) from meters; 不合法
   - select COLS(top(ts, 5), current), COLS(top(ts, 6), voltage) from meters; 不合法
   - select COLS(top(ts, 5), current), COLS(bottom(ts, 5), voltage) from meters;  不合法

### 5.2 详细使用说明

#### 5.2.1 在 Select 中使用 COLS 函数

##### 5.2.1.1 典型场景示例

1. 输出 last(*) 结果对应的每列数据以及所在行的时间列 ts；
数据如下：
```sql
taos> select * from tt1;
           ts            |     c1      |     c2      |     c3      |     c4      |
==================================================================================
 2024-11-27 18:41:40.895 |           1 | NULL        | NULL        | NULL        |
 2024-11-27 18:41:49.944 | NULL        |           2 | NULL        | NULL        |
 2024-11-27 18:41:58.652 | NULL        | NULL        |           3 | NULL        |
 2024-11-27 18:42:06.014 | NULL        | NULL        | NULL        |           4 |
Query OK, 4 row(s) in set (0.004054s)

taos> select last(*) from tt1;
        last(ts)         |  last(c1)   |  last(c2)   |  last(c3)   |  last(c4)   |
==================================================================================
 2024-11-27 18:42:06.014 |           1 |           2 |           3 |           4 |
Query OK, 1 row(s) in set (0.005694s)
```

查询语句及结果如下：
```sql
taos> Select last(c1) c1, COLS(c1, ts) c1_ts, last(c2) c2, COLS(c2, ts), last(c3) c3, COLS(c3, ts), last(c4) c4, COLS(c4, ts) from tt1;
   c1   |            c1_ts        |  c2   |           c2_ts          |  c3  |              c3_ts       |  c4 |        c4_ts             |
=========================================================================================================================================
    1   | 2024-11-27 18:41:40.895 |   2   |  2024-11-27 18:41:49.944 |   3  | 2024-11-27 18:41:58.652  |   4 |  2024-11-27 18:42:06.014 |
Query OK, 1 row(s) in set (0.005694s)
```

1. 数据如上，输出 max(c1) 及 max(c4) 的值及所在的行的 ts 与 c2 列的值；
```sql
taos> Select max(c1) c1, COLS(c1, ts as max_c1_ts, c2 as as max_c1_c2), max(c4) c4, COLS(c4, ts as as max_c4_ts, c2 as max_c4_c2) from tt1;
   c1   |           max_c1_ts       |  max_c1_c2   |      c4      |        max_c4_ts        |    max_c4_c2   |
=============================================================================================================
    1   |  2024-11-27 18:41:40.895  |   NULL       |       4      | 2024-11-27 18:42:06.014 |     NULL       |
```

##### 5.2.1.2 支持的单表查询

Select 中使用 COLS 函数，需要保证不会触发 [“选择函数的选择列冲突”](https://taosdata.feishu.cn/wiki/BIBNwoaLAiwp6ikXigAcO0gQnKh#share-HlWTdbQKeos5iDxZyUIclQZhn5Y)
以下实际使用举例，a.v1 可替换为常量或者列相关的表达，first(a.ts) 也可替换为 其他列
1. SELECT COLS(first(a.ts), a.v1) from a;  合法
2. SELECT COLS(first(a.ts), a.v1), a.v1, a.v2, a.tag from a;  合法
3. SELECT COLS(first(a.ts), a.v1),  ts from a;  合法
4. SELECT COLS(first(a.ts), a.v1), COLS(first(a.ts), a.v2), a.v3 from a;   合法
5. SELECT COLS(first(a.ts), a.v1), first(a.ts)  from a;  合法
6. SELECT COLS(first(a.ts), a.v1), COLS(first(a.ts), a.v2), v3  from a;  合法
7. SELECT COLS(first(a.ts), a.v1), first(a.ts), v2  from a;  合法
8. SELECT COLS(first(a.ts), a.v1), COLS(last(a.ts), a.v2)  from a;   合法
9. SELECT COLS(top(ts, 5), current), COLS(top(ts, 5), voltage) from meters; 合法
10. SELECT top(ts, 5), COLS(top(ts, 5), current), COLS(top(ts, 5), voltage) from meters; 合法
11. SELECT top(current, 5), COLStop(current, 5), voltage) , bottom(current, 5) from meters; 合法
12. SELECT top(current, 5), COLS(top(current, 5), voltage) from meters; 合法 等效于 SELECT top(current, 5),  voltage from meters;
13. SELECT top(current, 5), bottom(current, 5) from meters 合法（之前不合法）

##### 5.2.1.3 不合法的单表查询

1. SELECT COLS(first(a.ts), a.v1), last(ts), a.v3 from a;   不合法（a.v3替换为常量表达式时合法）
2. SELECT COLS(first(a.ts), a.v1), COLS(last(a.ts), a.v2), a.v3 from a;   不合法（a.v3替换为常量表达式时合法）
3. SELECT top(current, 5), bottom(current, 5), voltage from meters; 不合法
4. SELECT COLS(top(current, 5), current), bottom(current, 5), voltage from meters; 不合法

##### 5.2.1.4 子查询

中间表需要满足选择函数的要求, 同时不会触发 [“选择函数的选择列冲突”](https://taosdata.feishu.cn/wiki/BIBNwoaLAiwp6ikXigAcO0gQnKh#share-HlWTdbQKeos5iDxZyUIclQZhn5Y)，例:
1. select COLS(last(current), phase) from (select ts, current, phase from meters where current > 7.5 limit 10); 合法
2. select COLS(last(current), phase) from (select current, phase from meters where current > 7.5 limit 10);  单列函数last 要求有中间表有时间列，该查询中间表没有时间列，不合法

##### 5.2.1.5 Join 查询

示例：
1. select COLS(last(a.ts), a.current), b.ts  from d75 a join d70 b on a.ts = b.ts and a.current = b.current;
2. SELECT COLS(first(a.ts), a.voltage), COLS(last(a.ts), b.voltage)  FROM d11 a LEFT ASOF JOIN d12 b ON a.ts >= b.ts where a.voltage > 220 and b.voltage > 220 ;
当 first / last 执行结果如下
```sql
taos> SELECT last(a.ts), a.voltage, b.ts, b.voltage FROM d11 a LEFT ASOF JOIN d12 b ON a.ts >= b.ts where a.voltage > 220 and b.voltage > 220  ;
       last(a.ts)        |   voltage   |           ts            |   voltage   |
================================================================================
 2017-07-14 10:40:00.199 |         226 | 2017-07-14 10:40:00.196 |         236 |

taos> SELECT first(a.ts), a.voltage, b.ts, b.voltage FROM d11 a LEFT ASOF JOIN d12 b ON a.ts >= b.ts where a.voltage > 220 and b.voltage > 220  ;
       first(a.ts)       |   voltage   |           ts            |   voltage   |
================================================================================
 2017-07-14 10:40:00.000 |         246 | 2017-07-14 10:40:00.000 |         256 |

```

则 COLS(first(a.ts), a.voltage) 执行结果应如下
```sql
taos> SELECT COLS(first(a.ts), a.voltage), COLS(last(a.ts), b.voltage)  FROM d11 a LEFT ASOF JOIN d12 b ON a.ts >= b.ts where a.voltage > 220 and b.voltage > 220  ;
   COLS(first(a.ts), a.voltage)  |   COLSlast(a.ts), b.voltage)   |
=================================================================
            246                 |           236                  |
```

##### 5.2.1.6 和切分窗口同时使用

保证不会触发 [“选择函数的选择列冲突”](https://taosdata.feishu.cn/wiki/BIBNwoaLAiwp6ikXigAcO0gQnKh#share-HlWTdbQKeos5iDxZyUIclQZhn5Y) 即可
示例：
select tbname, _wstart,_wend, max(voltage), max(current), COLS(max(voltage), ts) from meters partition by tbname  count_window(60000) order by tbname;

#### 5.2.2 Where 字句

本来不支持选择函数，也不支持 COLS 函数
```sql
select first(current+1), first(current) from meters where first(current) > 1;

DB error: There mustn't be aggregation (0.000391s)
```

#### 5.2.3 Order by  

支持选择函数，也将支持 COLS 函数： select first(current+1), first(current) from meters partition by tbname order by first(current+2);

#### 5.2.4 Group by / partition by 

不支持选择函数

#### 5.2.5 其他

1. 关于别名
   - 和一般选择函数一样，可以对COLS 函数结果设置别名；例: SELECT COLSfirst(a.ts), a.v1) as first_ts from a; 
   - COLS 不会没有默认别名，设置 keepColumnName 没有作用；
2. 支持在流计算中使用 COLS 函数，规则与普通查询中一致；需要增加测例验证 (低优先级)
3. 在其他表达式中像使用其他普通选择函数一样，使用 COLS。需要增加测例子，例如:
   - Select cocat(COLS(last(ts), str), "_tail") from table;
   - Select COLS(last(ts), current) + 2 from meters;

### 5.3 异常说明

1. 表或者超级表没有数据：输出结果为空。
2. 没有支持的功能需要报错

## 6. 性能

压力测试，应和单列选择函数执行性能相当。

## 7. 兼容性

无兼容性问题

## 8. 运维

客户环境升级前，确认新增的函数名不会有影响，比如作为 UDF 函数名已经使用。

## 9. 使用场景

1. 普通查询
2. 嵌套查询
3. Join 查询
4. 流计算

## 10. 约束和限制

从新版本之后开始支持，无其他约束，具体使用中的注意事项见函数使用说明 4.1 

## 11. 常见错误和排查

## 12. 可观测性

无

## 13. 安装和卸载

无

## 14. 文档

官方文档需增加新函数，见[函数说明部分  ](https://taosdata.feishu.cn/wiki/BIBNwoaLAiwp6ikXigAcO0gQnKh#share-JaKndpeReoCz9ZxKpstcneDDn2d)

## 15. 参考文档

[需求说明：获取单行选择函数所在行的其他列值](https://taosdata.feishu.cn/wiki/MMTHwrfcviGvxBkhF16cx1rjnyb)

## 16. 附录

### 16.1 是否支持多行选择函数的讨论

**评审结果：本次未实现，需要能合理报错提示。**
起因是在讨论 "单行选择函数输出其他列" 时，思考了多行选择函数，考虑是否一起实现。
当前多行选择函数的实现，单个选择函数支持输出其他列，不支持多个多列选择函数。如下（现状）
```sql
taos> select tbname, bottom(current, 3), ts from meters;
    tbname  |  bottom(current, 3)  |           ts            |
==================================================================================
 d19        |            6.0298152 | 2017-07-14 10:40:00.023 |
 d76        |            6.0298152 | 2017-07-14 10:40:00.023 |
 d2         |            6.0298152 | 2017-07-14 10:40:00.023 |
 
 taos> select bottom(current, 3), last(current, 3) ts from meters;

DB error: Some functions are allowed only in the SELECT list of a query. And, cannot be mixed with other non scalar functions or columns. (0.000421s)
taos>
taos> select bottom(current, 3), bottom(current, 3) ts from meters;

DB error: Some functions are allowed only in the SELECT list of a query. And, cannot be mixed with other non scalar functions or columns. (0.000591s)
```

如果支持多个多列选择函数，多列选择函数需要要求行数一致，输出结果按行拼接：示例(蓝图)
```sql
taos> select ts, top(current, 3) from meters;
           ts            |   top(current, 3)    |
=================================================
 2017-07-14 10:40:00.196 |           14.2811832 |
 2017-07-14 10:40:00.196 |           14.2811832 |
 2017-07-14 10:40:00.196 |           14.2811832 |
Query OK, 3 row(s) in set (0.102075s)

taos> select ts, bottom(current, 3) from meters;
           ts            |  bottom(current, 3)  |
=================================================
 2017-07-14 10:40:00.023 |            6.0298152 |
 2017-07-14 10:40:00.023 |            6.0298152 |
 2017-07-14 10:40:00.023 |            6.0298152 |
Query OK, 3 row(s) in set (0.075114s)
taos> select top(current, 3), bottom(current, 3) from meters;
   top(current, 3)    |  bottom(current, 3)  |
==============================================
           14.2811832 |            6.0298152 |
           14.2811832 |            6.0298152 |
           14.2811832 |            6.0298152 |
           
```

select top(current, 3), bottom(current, 3) ts from meters; 应该语法报错
使用 COLS 函数实现（蓝图）
```sql
taos> select COLS(top(current, 3), ts), top(current, 3), COLS(bottom(current, 3), ts), bottom(current, 3)  from meters;
 COLS(top(current, 3), ts)  |   top(current, 3)    | COLS(bottom(current, 3), ts)  |   bottom(current, 3)    |
=================================================
  2017-07-14 10:40:00.196  |           14.2811832 |   2017-07-14 10:40:00.023    |            6.0298152    |
  2017-07-14 10:40:00.196  |           14.2811832 |   2017-07-14 10:40:00.023    |            6.0298152    |
  2017-07-14 10:40:00.196  |           14.2811832 |   2017-07-14 10:40:00.023    |            6.0298152    |
Query OK, 3 row(s) in set (0.102075s)
```

### 15.2  COLS 函数不同格式的比较

**评审结果：  选择该风格2，当只有一个输出表达式时（func(expr1)不是输出表达式），退化为和 1 一样，可以比较灵活的使用。**
考虑两种不同实现
1. COLS(func(expr1),  expr2) 
2. COLS(func(expr1),  expr2 [, expr3]...)    选择该风格，当只有一个输出表达式时（func(expr1)不是输出表达式），退化为和 1 一样，可以比较灵活的使用。

### 16.2 函数名确认

**评审结果：COLS  ， 和其他数据库不会有冲突，书写简单，语句不会太长；不是保留关键字，可以使用**
