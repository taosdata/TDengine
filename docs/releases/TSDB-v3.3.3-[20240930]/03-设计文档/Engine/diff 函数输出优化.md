# diff 函数输出优化

## 1. 背景

[TD-29154](https://jira.taosdata.com:18080/browse/TD-29154)
diff 函数在碰到 null  时，输出大量无效的null, 没有实现 null 值过滤的规则，因此需要做一些优化。
关系数据库没有类似 diff 的功能，时序数据库 influxdb 的 diference 和我们的 diff 功能类似，对其做了一些测试研究，以期取长补短。测试结果见附录
[TD-24514](https://jira.taosdata.com:18080/browse/TD-24514) 当select 多个 diff 时，当前版本不允许 diff 设置为忽略负值，这个 sql 应当被允许；
另外当前实现，忽略负值只是将负值转为了null， 依然会在结果中显示，优化后，如果该行diff的结果都是 null， 该行根据设置需进行过滤。
```c {wrap}
select ts,  diff(v1, 1), diff(v1, 1)  from st3;

DB error: Some functions are allowed only in the SELECT list of a query. And, cannot be mixed with other non scalar functions or columns. (0.002160s)
```

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/06/21 | 0.1 | 任新胜 | 创建 |
| 2024/7/9 | 0.2 | 任新胜 | 关于返回类型的优化说明 |

## 3. 定义

无

## 4. 行为说明

### 4.1 基本说明

见官网说明：
https://docs.taosdata.com/taos-sql/function/#diff
改动较多，此处需重点注意！修改后如下：
```sql
DIFF(expr [, ignore_option])

ignore_option: {
    0
  | 1
  | 2
  | 3
}
```

**功能说明**：统计表中某列的值与之前行有效值的差。 ignore_option 取值为 0|1|2|3 , 可以不填，默认值为 0. 
为 0 表示不忽略(diff结果)负值不忽略 null 值
为 1 时则表示(diff结果)负值作为 null 值
为 2 表示不忽略(diff结果)负值但忽略 null 值
为 3 时则表示忽略(diff结果)负值且忽略 null 值
**返回数据类型**：bool、时间戳类型及整型数值类型均返回 int_64，浮点类型返回 double, 若 diff 结果溢出则按溢出返回
**适用数据类型**：数值类型、时间戳和 bool 类型 
**适用于**：表和超级表。
**使用说明**:
- diff 是本行本列数据和之前最近的第一个有效数据做 diff，之前最近的第一个有效数据：指的是按照时间戳排序，从本行向较小的时间戳方向寻找，查看其他行的同一列，寻找到的第一个非 null 数据。
- 数值类型 diff 结果为对应差值；时间戳根据创建数据库的精度类型的时间戳进行差值计算；bool型 true 为1， false 为0 进行差值计算
- 当本行列数据不存在（为null）时，或者没找到有效的比较数据，diff 结果为 null
- 忽略负值时（ ignore_option 为 1 / 3 ），如果 diff 结果为负值，则结果设置为 null，  然后根据 null 值过滤规则过滤
- 支持单个语句中同时存在单个或者多个 diff，支持不同的diff函数指定相同或不同的ignore_option ，当存在多个diff时只有当某行所有 diff 结果都为 null 且每个 diff 设置的 ignore_option 都为 2 或 3 时，该行从结果集中剔除
- 不使用复合主键时，超级表的子表可能会有相同时间戳数据，有相同时间戳时，会提示"Duplicate timestamps not allowed"
- 使用复合主键时，超级表的子表可能有相同的复合主键，以首先找到的行数据为准
- 当使用order by 主键时间戳时，正序与倒序 diff 结果集应该保持一致，只有输出顺序的变化

### 4.2 ignore_option 配置为0 / 1 

原有逻辑 ，行为无变化，需要修复表中全 null 时，diff 结果行数等于表数据行数的  bug

### 4.3 ignore_option 配置为2 / 3

配置为 2 时，diff 结果负值保留，只根据规则对 null 值进行过滤
配置为 3 时，diff 结果的为负首先设置 null， 之后根据整行结果判断是否从结果集剔除；该行结果保留的充要条件是：至少有一个 diff 的结果不是 null 值
示例：
1. select diff(一个或者多个 )，该行结果均为null 时，该行从结果集剔除
如下示例：前两行结果应该剔除，但三四行需要保留
```c
taos> select ts, diff(v1), diff(v2) from st3;
           ts            |       diff(v1)        |         diff(v2)          |
==============================================================================
 2024-06-21 09:25:23.956 | NULL                  | NULL                      |
 2024-06-21 09:25:31.834 | NULL                  | NULL                      |
 2024-06-21 09:25:35.882 |                     4 | NULL                      |
 2024-06-21 09:25:42.594 |                     0 | NULL                      |
Query OK, 4 row(s) in set (0.035773s)
```

1. 允许同时 select diff(column) 和  column （influxdb 不支持这种语法）
   - 当某行 diff 结果为Null， 但column 有值时，也需要从结果集剔除，如下第1行，第4行
```c
taos> select ts, diff(v2), v1 from st3;
           ts            |         diff(v2)          |     v1      |
====================================================================
 2024-06-21 09:25:23.956 | NULL                      |           1 |
 2024-06-21 09:25:31.834 | NULL                      | NULL        |
 2024-06-21 09:25:35.882 | NULL                      | NULL        |
 2024-06-21 09:25:42.594 | NULL                      |           5 |
Query OK, 4 row(s) in set (0.031186s)
```

### 4.4 排序不影响结果

- 当使用order by 主键时间戳时，正序与倒序 diff 结果集应该保持一致，只有输出顺序的变化
- Group by tbname 时按照子表 diff
- Diff 在子查询嵌套使用，和在普通表上行为一致；

### 4.5 适用类型

Diff 可用于数值类型，时间戳类型和bool类型

### 4.6 异常处理

超级表中不同子表有相同时间戳时，语句会有提示，不返回结果
提示为："Duplicate timestamps not allowed"

## 5. 性能

注意过滤空数据的逻辑耗时，预期不会引起性能变化

## 6. 兼容性

无

## 7. 运维

无

## 8. 使用场景

无

## 9. 约束和限制

之前的约束不变，没有新增约束和限制：

## 10. 常见错误和排查

无

## 11. 可观测性

无

## 12. 安装和卸载

无

## 13. 文档

需要修改官方文档关于 diff  的说明 
需要修改官网文档关于 diff 的说明
最终文档描述
```sql
DIFF(expr [, ignore_option])

ignore_option: {
    0
  | 1
  | 2
  | 3
}
```

**功能说明**：统计表中某列的值与之前行有效值的差。 ignore_option 取值为 0|1|2|3 , 可以不填，默认值为 0. 
为 0 表示不忽略(diff结果)负值不忽略 null 值
为 1 时则表示(diff结果)负值作为 null 值
为 2 表示不忽略(diff结果)负值但忽略 null 值
为 3 时则表示忽略(diff结果)负值且忽略 null 值
对于存在复合主键的表的查询，若时间戳相同的数据存在多条，则只有对应的复合主键最小的数据参与运算。
**返回数据类型**：bool、时间戳类型及整型数值类型均返回 int_64，浮点类型返回 double, 若 diff 结果溢出则按溢出返回
**适用数据类型**：数值类型、时间戳和 bool 类型 
**适用于**：表和超级表。
**使用说明**:
- diff 是本行本列数据和之前最近的第一个有效数据做 diff，之前最近的第一个有效数据：指的是按照时间戳排序，从本行向较小的时间戳方向寻找，查看其他行的同一列，寻找到的第一个非 null 数据。
- 数值类型 diff 结果为对应差值；时间戳根据创建数据库的精度类型的时间戳进行差值计算；bool型 true 为1， false 为0 进行差值计算
- 当本行列数据不存在（为null）时，或者没找到有效的比较数据，diff 结果为 null
- 忽略负值时（ ignore_option 为 1 / 3 ），如果 diff 结果为负值，则结果设置为 null，  然后根据 null 值过滤规则过滤
- 当 diff 结果有类型溢出时，根据逻辑运算结果的正负进行判定是否忽略负值，例如 9223372036854775800  - (-9223372036854775806) 的值超出 BIGINT 的范围 ，diff 结果会显示溢出值 -10，但并不会被作为负值忽略
- 支持单个语句中同时存在单个或者多个 diff，支持不同的diff函数指定相同或不同的 ignore_option ，当存在多个diff时只有当某行所有 diff 结果都为 null 且 ignore_option 为 2或3 时，该行从结果集中剔除
- 可以与选择相关联的列一起使用。 例如: select _rowts, DIFF() from。
- 不使用复合主键时，超级表的子表可能会有相同时间戳数据，有相同的时间戳时，会提示"Duplicate timestamps not allowed"
- 使用复合主键时，超级表的子表可能有相同的复合主键，以首先找到的行数据为准

## 14. 参考文档

无

## 15. 附录

### 15.1 influx db 行为说明

测试并参考 ：https://docs.influxdata.com/flux/v0/stdlib/universe/difference/
1. difference 作用在具体的一列上，使用当前行的值和上一个有效(非 null )值作比较，计算得到一个差值；如果没有找到上一个有效值，则结果为无效值(null)
2. 结果集时间列取当前行的时间列的值
3. Influxdb 默认结果集第一列是时间列
4. difference 计算差值时，如果当前行列的值无效，或者该行之前（时间升序）的所有行都没有有效值，则结果也是无效值 
5. 当结果集的一行除了时间 外没有其他有效值，从结果集中剔除
```c
> select * from  temperature
name: temperature
time                external internal machine type
----                -------- -------- ------- ----
1719193545369357593 25       37       unit42  assembly
1719193577280273733 26       38       unit42  assembly
1719195137849859044          39       unit42  assembly
1719195535293969969          39       unit42  assembly
1719195569826316618 1                 unit42  assembly
1719196306995290440 1                 unit42  assembly
1719196335602472519 1                 unit43  assembly
1719196353319386534 1                 unit43  assembly2
> select difference(external) from temperature
name: temperature
time                difference
----                ----------
1719193577280273733 1
1719195569826316618 -25
1719196306995290440 0
1719196335602472519 0
1719196353319386534 0
```

1. 不支持difference 和 普通列一起select
<quote-container>
select difference(internal),external from temperature
ERR: mixing aggregate and non-aggregate queries is not supported
</quote-container>

### 15.2 tdengine 赘述(无变更) {folded="true"}

1. diff(ts) 除了第一行之外，永远有值，但注意，diff(ts)的结果永远是和上一行ts 的差值，可视为消息的时间间隔；但其他列的diff 却不一定是这样，其他列会寻找上一行有效值进行差值计算，上一行有效值不一定是时间序列的前一个消息。
2. 超级表的各个子表很容易出现相同时间戳，相同时间戳时会报错
```c
taos> select tbname, ts,  diff(v1), diff(v2)  from st3;
DB error: Duplicate timestamps not allowed in function (0.280746s)
```
