# 获取选择函数所在行的其他列值 TS

## 1. 测试目标

需求见：[TS-5255](https://jira.taosdata.com:18080/browse/TS-5255)
FS:          [获取选择函数所在行的其他列值](https://taosdata.feishu.cn/wiki/BIBNwoaLAiwp6ikXigAcO0gQnKh)
<quote-container>
对 FS 涉及到 sql 和场景覆盖测试
- 保证执行结果正确
- 性能测试达到预期（新增 COlS 函数执行效率和直接执行选择函数应该相当）
</quote-container>

## 2. 变更历史

| 日期 | 版本 | 作者 | 备忘 |
| --- | --- | --- | --- |
| 12月27日 | V1.0 | 任新胜 |  |
|  |  |  |  |

## 3. 测试范围

<quote-container>
不同场景不同方式使用 COlS 函数测试
- 不同子句中使用
  - Select
  - Order by
  - Where / group by / partition by 报错验证
- 不同场景
  - 普通表查询
  - 子表查询
  - 超级表查询
  - 嵌套查询
  - Join 查询
  - 配合切分窗口使用
  - 流计算中使用，时间来不及的话，本期可能不实现，需要测试错误提示
- 不同的 func 参数
  - 单行选择函数测试
  - 多行选择函数测试
  - Interp 函数测试，不支持，报错校验
- 不同的输出表达式测试
  - 单个表达式输出
  - 多个表达式输出
- 不同位置使用别名测试
  - 不使用别名
  - 参数中使用别名
  - 整体使用别名
  - 混合使用别名 需要报错
- 多个 COLS 函数混合测试
  - 相同的 COLS 函数测试
  - 不同的 COLS 函数测试
- 选择函数的列冲突测试
  - COLS 使用单行选择函数，列冲突测试
  - COLS 使用多行选择函数，列冲突测试
- 边界测试
  - 数据均为 NULL 的测试
  - 空子表/超级表
  - 临时表没有结果
- 异常场景测试
  - 别名超过 64 个字符的测试
- 性能测试
</quote-container>

## 4. 测试结论

<quote-container>
自测通过，加入 CI 流程
</quote-container>

## 5. 已知问题和限制

- 无

## 6. 测试环境

- CI 环境
- 性能测试待确定

## 7. 测试数据

1. 基本测试数据1
taosBenchamark 产生数据，执行命令如下：
   taosBenchmark -t 10 -n 100  -b INT,FLOAT,NCHAR,BOOL
1. 基本测试数据2
普通表的测试数据
Create table  normal_table (ts timestamp, c0 int, c1 float, c2 nchar(30), c3 bool);
insert into normal_table (select * from d0);
1. 边界测试数据3
2. 压力测试数据4
taosBenchmark -t 10000 -n 10000  -b INT,FLOAT,NCHAR,BOOL

## 8. 测试用例

### 8.1 基本 case

#### 8.1.1 基本 case 列表

| case 序号 | 类型/场景 | case 覆盖点说明 | 测试数据集 | 测试 SQl | 测点说明 | 预期结果 | 扩展测试 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 基本测试: select | 单个 COLS 函数
使用单行选择函数 | 基本测试数据1
基本测试数据2 | select COLS(last(ts), c0) from meters;
select last(ts), c0 from meters; | 单列 | 结果 c0 列相等 | 1. 聚合函数替换为其他单行选择函数测试
1. 在子表上进行这些 case 测试  
2. 在普通表上进行这些 case 测试
4.单行选择函数参数使用表达式，输出列使用表达式 | Pass |  |
| 2 |  |  |  | select last(ts), COLS(last(ts), c0) from meters;
select last(ts), c0 from meters; | 双列 | 两列结果相等 |  | Pass |  |
| 3 |  |  |  | select  COLS(last(ts), ts) from meters tbname;
select last(ts) from meters tbname; | 与func 相同参数列，ts | 结果相等 |  | Pass |  |
| 4 |  |  |  | select  COLS(last(ts), ts) from meters group by tbname;
select last(ts) from meters group by tbname; | with group by | 结果相等 |  | Pass |  |
| 5 |  |  |  | select  COLS(last(ts), ts) from meters partiton by tbname;
select last(ts) from meters partiton by tbname; | with group by | 结果相等 |  | Pass |  |
| 6 |  |  |  | select  COLS(last(c0), c0) from meters group by t0;
select last(c0) from meters group by t0; |  | 结果相等 |  | Pass |  |
| 7 |  |  |  | select  COLS(last(c0), 5) from meters group by t0; | 与常量组合 | 结果为常量 |  | Pass |  |
| 8 |  |  |  | select  COLS(last(ts), ts) from meters group by t0;
select last(ts) from meters group by t0; |  | 结果相等 |  | Pass |  |
| 9 |  |  |  | select  COLS(last(ts), t0) from meters group by t0; |  | 结果应为 t0 的tag值 |  | Pass |  |
| 10 |  |  |  | select  COLS(last(ts), tbname) from meters group by t0; |  | 结果应为 tbname 值 |  | Pass |  |
| 11 |  |  |  | select  COLS(last(ts), t1) from meters group by t0; |  | 结果应为 t1 的tag值 |  | Pass |  |
| 12 |  | 单个 COLS 函数 | 基本测试数据1
基本测试数据2 | select COLS(top(c0, 5), c0) from meters;
select top(c0, 5), ts from meters; |  | 结果相等 | 1. COLS 函数的参数 func 选择函数替换为其他多行选择函数测试
1. 在子表上进行这些 case 测试  
2. 在普通表上进行这些 case 测试
4.多行选择函数参数使用表达式，输出列使用表达式 | Pass |  |
| 13 |  |  |  | select top(c0,5), COLS(top(c0,5), ts) from meters;
select top(c0,5), ts from meters; |  | 两列结果相等 |  | Pass |  |
| 14 |  |  |  | select  COLS(top(c0,5), c0) from meters tbname;
select top(c0,5) from meters tbname; |  | 结果相等 |  | Pass |  |
| 15 |  |  |  | select  COLS(top(c0,5), c0) from meters group by tbname;
select top(c0) from meters group by tbname; |  | 结果相等 |  | Pass |  |
| 16 |  |  |  | select  COLS(top(c0, 5), c0) from meters partiton by tbname;
select top(c0, 5) from meters partiton by tbname; |  | 结果相等 |  | Pass |  |
| 17 |  |  |  | select  COLS(top(c0, 5), c0) from meters group by t0;
select top(c0, 5) from meters group by t0; |  | 结果相等 |  | Pass |  |
| 18 |  |  |  | select  COLS(top(c0, 5), c0) from meters group by t0;
select top(c0, 5) from meters group by t0; |  | 结果相等 |  | Pass |  |
| 19 |  |  |  | select COLS(last(ts), ts) , c0, c1, c3, c3  from meters;
select c0, last(ts), c1, c2, c3 from meters; |  | 结果相等 |  | Pass |  |
| 20 |  |  |  | select  COLS(top(c0, 5), t0) from meters group by t0; |  | 结果应为 t0 的tag值 |  | Pass |  |
| 21 |  |  |  | select  COLS(top(c0, 5), tbname) from meters group by t0; |  | 结果应为 tbname 值 |  | Pass |  |
| 22 |  |  |  | select  COLS(top(c0, 5), t1) from meters group by t0; |  | 结果应为 t1 的tag值 |  | Pass |  |
| 23 |  | 多个 COLS 函数，聚合函数 func 包括参数完全相同
func 为单行选择函数 | 基本测试数据1
基本测试数据2 | select COLS(last(ts), ts), COLS(last(ts), c0) from meters;
select last(ts), c0 from meters; |  | 两列结果相等 | 1. 聚合函数替换为其他单行选择函数测试
1. 在子表上进行这些 case 测试  
2. 在普通表上进行这些 case 测试
4.单行选择函数参数使用表达式，输出列使用表达式 | Pass |  |
| 24 |  |  |  | select last(ts), COLS(last(ts), c0) ,COLS(last(ts), c1) ,COLS(last(ts), c2) ,COLS(last(ts), c3)  from meters;
select last(ts), c0, c1, c2, c3 from meters; |  | 结果相等 |  | Pass |  |
| 25 |  |  |  | select last(ts), COLS(last(ts), c0) ,COLS(last(ts), c1) ,COLS(last(ts), c2) ,COLS(last(ts), c3), COLS(last(ts), tbname)  from meters;
select last(ts), c0, c1, c2, c3, tbname from meters; |  | 结果相等 |  | Pass |  |
| 26 |  |  |  | select last(ts), COLS(last(ts), c0) ,COLS(last(ts), c1) ,COLS(last(ts), c2) ,COLS(last(ts), c3), COLS(last(ts), t0)  from meters;
select last(ts), c0, c1, c2, c3, t0 from meters; |  | 结果相等 |  | Pass |  |
| 27 |  |  |  | select last(ts), COLS(last(ts), c0) ,COLS(last(ts), c1) ,COLS(last(ts), c2) ,COLS(last(ts), c3), COLS(last(ts), t0)  from meters group by tbname;
select last(ts), c0, c1, c2, c3, t0 from meters  group by tbname; |  | 结果相等 |  | Pass |  |
| 29 |  | 多个 COLS 函数，聚合函数为单行选择函数 func 不同（类型或者参数不同） | 基本测试数据1
基本测试数据2 | 1. select last(ts), COLS(last(ts), c0), first(ts), COLS(first(ts), c0) from meters;
1. select last(ts), c0 from meters;
2. select fisrt(ts), c0 from meters; |  | 语句 1 的结果等于语句 2,3 结果的 append | 1. 聚合函数替换为其他单行选择函数测试2. 在子表上进行这些 case 测试 
3. 在普通表上进行这些 case 测试
4.单行选择函数参数使用表达式，输出列使用表达式
1. 增加 group/partition by  tbname 测试
2. 增加 group/partition by tag 测试 | Pass |  |
| 30 |  |  |  | 1. select last(ts), COLS(last(ts), c0), COLS(last(ts), t1),  first(ts), COLS(first(ts), c0),  COLS(first(ts), t1) from meters;
1. select last(ts), c0, t1 from meters;
2. select fisrt(ts), c0, t1 from meters; |  | 语句 1 的结果等于语句 2,3 结果的 append |  | Pass |  |
| 31 |  |  |  | 1. select last(ts), COLS(last(ts), c0),  COLS(last(ts), c1), COLS(last(ts), c2),  COLS(last(ts), c3), first(ts), COLS(first(ts), c0),  COLS(first(ts), c1) ,  COLS(first(ts), c2) ,  COLS(first(ts), c3)  from meters;
1. select last(ts), c0, c1, c2, c3 from meters;
2. select fisrt(ts), c0, c1, c2, c3 from meters; |  | 语句 1 的结果等于语句 2,3 结果的 append |  | Pass |  |
| 32 |  |  |  | 1. select COLS(last(c0), ts), last(c0),  COLS(last(c1), ts), last(c1), COLS(last(c2), ts), last(c2), COLS(last(c3), ts), last(c3) from meters;
1. select ts, last(c0) from meters;
2. select ts, last(c1) from meters;
3. select ts, last(c2) from meters;
4. select ts, last(c3) from meters; |  | 语句 1 的结果等于语句 2,3, 4, 5 结果的 append |  | Pass |  |
| 33 |  |  |  | 1. select COLS(last(c0), ts), last(c0),  COLS(last(c1), ts), last(c1), COLS(last(c2), ts), last(c2), COLS(last(c3), ts), last(c3),  COLS(first(c0), ts), first(c0),  COLS(first(c1), ts), first(c1), COLS(first(c2), ts), first(c2), COLS(first(c3), ts), first(c3)  from meters;
1. select ts, last(c0) from meters;
2. select ts, last(c1) from meters;
3. select ts, last(c2) from meters;
4. select ts, last(c3) from meters;
5. select ts, first(c0) from meters;
6. select ts, first(c1) from meters;
7. select ts, first(c2) from meters;
8. select ts, first(c3) from meters; |  | 语句 1 的结果等于语句 2,3, 4, 5 结果的 append |  | Pass |  |
| 34 |  |  |  | select COLS(last(c0), ts), last(c0),  COLS(last(c1), ts), last(c1), COLS(last(c2), ts), last(c2), COLS(last(c3), ts), last(c3), tbname from meters; |  | 语法报错 |  | Pass |  |
| 35 |  |  |  | select COLS(last(c0), ts), last(c0),  COLS(last(c1), ts), last(c1), COLS(last(c2), ts), last(c2), COLS(last(c3), ts), last(c3), c1 from meters; |  | 语法报错 |  | Pass |  |
| 36 |  |  |  | select COLS(last(c0), ts), last(c0),  COLS(last(c1), ts), last(c1), COLS(last(c2), ts), last(c2), COLS(last(c3), ts), last(c3), t1 from meters; |  | 语法报错 |  | Pass |  |
| 37 |  | 多个 COLS 函数，聚合函数为多行选择函数 func 不同（类型或者参数不同） |  | 多行选择函数未实现 |  |  |  |  | 多行选择函数本次未实现 |
| 38 | where 字句 | 不支持场景报错检测 |  | select  COLS(last(c0), c0) from meters group by t0 where  COLS(last(c0), c0) >1 ; |  | 语法报错 |  | Pass |  |
| 39 | group by / partiton by | 不支持场景报错检测 |  | select  count(*),  COLS(last(c0), c0) from meters group by COLS(last(c0), c0); |  | 语法报错 |  | Pass |  |
| 40 |  | 不支持场景报错检测 |  | select  count(*),  COLS(last(c0), c0) from meters interval(1m) COLS(last(c0), c0); |  | 语法报错 |  | Pass |  |
| 41 | order by COLS |  |  | select COLS(first(c0, ts), COLS(last(c1), ts) from meters partition by tbname order by COLS(first(c0, ts); |  | 排序正确 |  | Pass |  |
| 42 |  |  |  | select COLS(first(c0, ts), COLS(last(c1), ts) from meters partition by tbname order by COLS(first(c0, ts), COLS(last(c1), ts) ; |  | 排序正确 |  | Pass |  |
| 43 |  |  |  | select COLS(first(c0, ts) from meters partition by tbname order by COLS(first(c0, ts), COLS(last(c1), ts) ; |  | 排序正确 |  | Pass |  |
| 44 |  |  |  | select tbname, count(*) from meters partition by tbname order by COLS(first(c0, ts), COLS(last(c1), ts) ; |  | 排序正确 |  | Pass |  |
|  |  | order by cols 函数（单列函数） |  | select count(1), cols(last({col_name}),c2)  {t1} from {from_table} group by tbname order by cols(last({col_name}), c2) desc |  | 排序正确 |  | Pass |  |
|  |  | orderby pos   单列输出的 cols |  | select count(1), cols(last({col_name}),c2)  {t1} from {from_table} group by tbname order by 1 |  | 排序正确 |  | Pass |  |
| 45 | COLS 嵌套测试 |  | 基本测试数据1 | select COLS(COLS(last(ts),ts), c0) from meters; |  | sql 报错 |  | Pass |  |

#### 8.1.2 不同 From 场景

| 类型/场景 | case 覆盖点说明 | 测试数据集 | 测试 SQl | 测点说明 | 测试结果 |
| --- | --- | --- | --- | --- | --- |
| 子查询使用 COLS | 子查询 | 基本测试数据1基本测试数据2 | 修改基本测试 case 的 from 条件 在子查询上执行 |  | Pass |
| Join 查询使用 COLS | 配置 join 测试 | 基本测试数据1基本测试数据2 | 修改基本测试 case 的 from 条件 在join 查询结果上执行 |  | Pass |
| 切分窗口使用 COLS | 切分窗口上测试 | 基本测试数据1基本测试数据2 | 修改基本测试 case 的 from 条件 在切分窗口上上执行 |  | Pass |
| 系统表查询 | 系统表查询(部分支持，不支持first, last) |  |  |  | Pass |

#### 8.1.3 别名测试

不使用 COLS 输出多列时，每列 head 需要重命名，sql 不使用重命名时，默认的命名为输出参数的表达式原始字符串
例如：
| 类型/场景 | case 覆盖点说明 | 测试数据集 | 测试 SQl | 测点说明 | 测试结果 |
| --- | --- | --- | --- | --- | --- |
| 别名测试 | 对 COLS 函数整体使用别名测试 | 基本测试数据1基本测试数据2 | 1. select tbname, cols(last(ts), ts as ts1), cols(last(f1), ts as ts2) from tba5 interval(1s);
1. select tbname, cols(last(ts), ts as ts1), cols(last(f1), ts as ts2) from {dbname}.sta group by tbname;
2. select tbname, cols(last(ts), ts as ts1), cols(last(f1), ts as ts2) from {dbname}.sta group by tbname order by ts1;
3. select t1, cols(last(ts), ts as ts1), cols(last(f1), ts as ts2) from {dbname}.sta group by t1;
4. select t1, cols(last(ts), ts as ts1), cols(last(f1), ts as ts2) from {dbname}.sta group by t1 order by ts1;'
5. select cols(last(ts), ts) from {self.dbname}.meters
6. select cols(last(ts), ts) as t1 from {self.dbname}.meters
7. select cols(last(ts), ts as t1) from {self.dbname}.meters |  | Pass |
|  | 对 COLS 函数表达式参数使用别名测试 |  |  |  | Pass |
|  | 不使用别名的默认名设置 |  |  |  | Pass |
|  | 部分使用别名，部分不使用笔名 |  |  |  | Pass |
|  | 错误场景 |  | select cols(ts) from {self.dbname}.meters group by tbname
select cols(ts) from {self.dbname}.meters
select last(cols(ts)) from {self.dbname}.meters
select last(cols(ts, ts)) from {self.dbname}.meters
select last(cols(ts, ts), ts) from {self.dbname}.meters
select cols(last(ts), ts as t1) as t1 from {self.dbname}.meters
select cols(last(ts), ts, c0) t1 from {self.dbname}.meters
select cols(last(ts), ts t1) tt from {self.dbname}.meters
select cols(last(ts), c0 cc0, c1 cc1) cc from {self.dbname}.meters
select cols(last(ts), c0 as cc0) as cc from {self.dbname}.meters
select cols(ts) + 1 from {self.dbname}.meters group by tbname
select last(cols(ts)+1) from {self.dbname}.meters
select last(cols(ts+1, ts)) from {self.dbname}.meters
select last(cols(ts, ts), ts+1) from {self.dbname}.meters
select last(cols(last(ts+1), ts+1), ts) from {self.dbname}.meters
select cols(last(ts), ts+1 as t1) as t1 from {self.dbname}.meters
select cols(last(ts+1), ts, c0) t1 from {self.dbname}.meters
select cols(last(ts), ts t1) tt from {self.dbname}.meters
select cols(first(ts+1), c0+2 cc0, c1 cc1) cc from {self.dbname}.meters
select cols(last(ts)+1, c0+2 as cc0) as cc from {self.dbname}.meters
select cols(ABS(c0), c1) from {self.dbname}.meters group by tbname |  | Pass |

#### 8.1.4 条件组合说明

基本 case 和 from  表/子表/切分窗口，可以组合，产生不同的测试 case， 测试脚本应该支持部分条件替换执行，减少重复测试代码的书写。

#### 8.1.5 边界测试

<quote-container>
- 数据均为 NULL 的测试：已测试
- 空子表/超级表：已测试
- 临时表没有结果的测试：已测试
- 别名超过 65 个字符长度的测试： 截断，已测试
</quote-container>

### 8.2 可用性

无

### 8.3 可靠性

无

### 8.4 性能

#### 8.4.1 测试准备

使用 taosBenchmark 产生数据   taosBenchmark -t 10000 -n 10000  -b INT,FLOAT,NCHAR,BOOL
1. 测试原有的选择函数相关的 sql 语句，版本前后不应该有性能降低；
2. 测试能够用以前的 sql 语句实现的 cols 函数，用 cols 函数完成查询，和用旧 sql 完成查询，其性能不应降低；
3. 对于之前一条sql 无法完成，使用 COLS 可以在一条 sql 完成的查询，使用 benchmark 测试其性能，完成查询目标总耗时至少不应比以前长。

#### 8.4.2 测试结果

测试环境： 192.168.1.120 开发机，8C16G， 新旧版本均使用该机器测试，无机器差异
数据： 使用 taosBenchmark 产生数据   taosBenchmark -t 10000 -n 10000  -b INT,FLOAT,NCHAR,BOOL
测试版本，使用当前 3.0 (git commit:  79190b31c9977baad80906562360a9e6604a7610) 进行测试，
然后 merge cols func 的分支，进行测试比较，不受其他代码影响。

##### 8.4.2.1 原有 sql 测试无 group

```c
{
    "filetype": "query",
    "cfgdir": "/etc/taos",
    "host": "127.0.0.1",
    "port": 6030,
    "user": "root",
    "password": "taosdata",
    "confirm_parameter_prompt": "no",
    "continue_if_fail": "yes",
    "databases": "test",
    "query_times": 10,
    "query_mode": "taosc",
    "specified_table_query": {
        "query_interval": 1,
        "concurrent": 10,
        "sqls": [
            {
                "sql": "select last(ts) from meters;",
                "result": "/root/test/colsReslut/query_res0.0.txt"
            },
            {
                "sql": "select last(ts) , c0, c1, c3, c3, tbname  from meters;",
                "result": "/root/test/colsReslut/query_res0.1.txt"
            },
            {
                "sql": "select last(ts), first(ts) from meters;",
                "result": "/root/test/colsReslut/query_res0.2.txt"
            }
        ]
    }
}
```

新版测试结果1：
```c
complete query with 10 threads and 1000 sql 1 spend 7.375472s QPS: 135.585 query delay avg: 0.072884s min: 0.030764s max: 0.135689s p90: 0.089145s p95: 0.096158s p99: 0.113435s SQL command: select last(ts) from meters;
complete query with 10 threads and 1000 sql 2 spend 7.811146s QPS: 128.022 query delay avg: 0.077166s min: 0.036801s max: 0.133105s p90: 0.095665s p95: 0.102219s p99: 0.115755s SQL command: select last(ts) , c0, c1, c3, c3, tbname  from meters;
complete query with 10 threads and 1000 sql 3 spend 7.588967s QPS: 131.770 query delay avg: 0.075464s min: 0.033701s max: 0.130520s p90: 0.091300s p95: 0.097406s p99: 0.109738s SQL command: select last(ts), first(ts) from meters;
[02/24 01:07:00.448994] INFO: Spend 22.9760 second completed total queries: 3000, the QPS of all threads:    130.571 ,error 0 (rate:0.000%)
```

旧版测试结果：
```c
complete query with 10 threads and 1000 sql 1 spend 7.545271s QPS: 132.533 query delay avg: 0.074997s min: 0.029866s max: 0.142189s p90: 0.093045s p95: 0.100411s p99: 0.110854s SQL command: select last(ts) from meters;
complete query with 10 threads and 1000 sql 2 spend 7.977287s QPS: 125.356 query delay avg: 0.078709s min: 0.030900s max: 0.126491s p90: 0.096398s p95: 0.101911s p99: 0.113989s SQL command: select last(ts) , c0, c1, c3, c3, tbname  from meters;
complete query with 10 threads and 1000 sql 3 spend 7.682413s QPS: 130.167 query delay avg: 0.075979s min: 0.030733s max: 0.127720s p90: 0.093718s p95: 0.100146s p99: 0.112794s SQL command: select last(ts), first(ts) from meters;
[02/24 00:47:00.861430] INFO: Spend 23.3990 second completed total queries: 3000, the QPS of all threads:    128.211 ,error 0 (rate:0.000%)
```

结论：原本 sql 性能略有提升 1~2% 之间

##### 8.4.2.2 原有 sql 测试有 group

```c
{
    "filetype": "query",
    "cfgdir": "/etc/taos",
    "host": "127.0.0.1",
    "port": 6030,
    "user": "root",
    "password": "taosdata",
    "confirm_parameter_prompt": "no",
    "continue_if_fail": "yes",
    "databases": "test",
    "query_times": 100,
    "query_mode": "taosc",
    "specified_table_query": {
        "query_interval": 1,
        "concurrent": 8,
        "sqls": [
            {
                "sql": "select first(ts), last(ts) from meters group by  tbname;",
                "result": "/root/test/colsReslut/query_res2.2.txt"
            },
            {
                "sql": "select last(c0), ts from meters group by  tbname;",
                "result": "/root/test/colsReslut/query_res2.3.txt"
            },
            {
                "sql": "select last(c1), ts from meters group by  tbname;",
                "result": "/root/test/colsReslut/query_res2.4.txt"
            },
            {
                "sql": "select last(c2), ts from meters group by  tbname;",
                "result": "/root/test/colsReslut/query_res2.5.txt"
            },
            {
                "sql": "select last(c3), ts from meters group by  tbname;",
                "result": "/root/test/colsReslut/query_res2.6.txt"
            }
        ]
    }
}

```

新版测试结果：
```c
complete query with 8 threads and 800 sql 2 spend 5.931265s QPS: 134.878 query delay avg: 0.058964s min: 0.033246s max: 0.091949s p90: 0.069567s p95: 0.073585s p99: 0.082383s SQL command: select first(ts), last(ts) from meters group by  tbname;
complete query with 8 threads and 800 sql 3 spend 6.070957s QPS: 131.775 query delay avg: 0.060084s min: 0.030095s max: 0.161591s p90: 0.070156s p95: 0.073602s p99: 0.133601s SQL command: select last(c0), ts from meters group by  tbname;
complete query with 8 threads and 800 sql 4 spend 6.032619s QPS: 132.612 query delay avg: 0.059735s min: 0.030700s max: 0.158150s p90: 0.071136s p95: 0.074659s p99: 0.134829s SQL command: select last(c1), ts from meters group by  tbname;
complete query with 8 threads and 800 sql 5 spend 5.954162s QPS: 134.360 query delay avg: 0.059053s min: 0.031463s max: 0.093592s p90: 0.069233s p95: 0.072722s p99: 0.081147s SQL command: select last(c2), ts from meters group by  tbname;
complete query with 8 threads and 800 sql 6 spend 5.970106s QPS: 134.001 query delay avg: 0.058766s min: 0.029491s max: 0.092027s p90: 0.068393s p95: 0.072367s p99: 0.079080s SQL command: select last(c3), ts from meters group by  tbname;
[02/24 01:09:18.512766] INFO: Spend 36.3780 second completed total queries: 4800, the QPS of all threads:    131.948 ,error 0 (rate:0.000%)
```

旧版测试结果：
```c
complete query with 8 threads and 800 sql 2 spend 5.956560s QPS: 134.306 query delay avg: 0.058654s min: 0.029604s max: 0.100764s p90: 0.070212s p95: 0.074503s p99: 0.082329s SQL command: select first(ts), last(ts) from meters group by  tbname;
complete query with 8 threads and 800 sql 3 spend 5.954080s QPS: 134.362 query delay avg: 0.059060s min: 0.030550s max: 0.100623s p90: 0.069953s p95: 0.074237s p99: 0.081280s SQL command: select last(c0), ts from meters group by  tbname;
complete query with 8 threads and 800 sql 4 spend 5.944226s QPS: 134.584 query delay avg: 0.058698s min: 0.029507s max: 0.090726s p90: 0.067983s p95: 0.073099s p99: 0.080116s SQL command: select last(c1), ts from meters group by  tbname;
complete query with 8 threads and 800 sql 5 spend 5.944573s QPS: 134.577 query delay avg: 0.059328s min: 0.032484s max: 0.097727s p90: 0.069756s p95: 0.073370s p99: 0.088013s SQL command: select last(c2), ts from meters group by  tbname;
complete query with 8 threads and 800 sql 6 spend 5.971395s QPS: 133.972 query delay avg: 0.059221s min: 0.031972s max: 0.096754s p90: 0.069760s p95: 0.074333s p99: 0.082309s SQL command: select last(c3), ts from meters group by  tbname;
[02/24 00:50:23.029799] INFO: Spend 32.5650 second completed total queries: 4800, the QPS of all threads:    147.398 ,error 800 (rate:16.667%)
```

结论：性能基本无变化 （大概不到 1% 波动）

##### 8.4.2.3 cols 函数测试1（和原来等效 sql 比较）

```c
{
    "filetype": "query",
    "cfgdir": "/etc/taos",
    "host": "127.0.0.1",
    "port": 6030,
    "user": "root",
    "password": "taosdata",
    "confirm_parameter_prompt": "no",
    "continue_if_fail": "yes",
    "databases": "test",
    "query_times": 10,
    "query_mode": "taosc",
    "specified_table_query": {
            "query_interval": 1,
            "concurrent": 10,
            "sqls": [
                    {
                        "sql": "select last(ts), COLS(last(ts), c0) from meters;",
                        "result": "/root/test/colsReslut/query_res0.txt"
                    },
                    {
                        "sql": "select  COLS(last(ts), ts, c0) from meters group by tbname;",
                        "result": "/root/test/colsReslut/query_res1.txt"
                    },
                    {
                        "sql": "select COLS(last(ts), ts) , c0, c1, c3, c3  from meters;",
                        "result": "/root/test/colsReslut/query_res3.txt"
                    },
                    {
                        "sql": "select COLS(last(ts), ts, c0, c1, c3, c3, tbname) from meters;",
                        "result": "/root/test/colsReslut/query_res3.txt"
                    },
                    {
                        "sql": "select COLS(last(ts), ts), COLS(first(ts), ts) from meters;",
                        "result": "/root/test/colsReslut/query_res4.txt"
                    },
                    {
                        "sql": "select last(ts), COLS(last(ts), c0) ,COLS(last(ts), c1) ,COLS(last(ts), c2) ,COLS(last(ts), c3)  from meters;",
                        "result": "/root/test/colsReslut/query_res5.txt"
                    }
            ]
    }
}


```

测试结果：
```c
complete query with 10 threads and 100 sql 1 spend 0.803186s QPS: 124.504 query delay avg: 0.077477s min: 0.031528s max: 0.120772s p90: 0.095746s p95: 0.106363s p99: 0.120772s SQL command: select last(ts), COLS(last(ts), c0) from meters;
complete query with 10 threads and 100 sql 2 spend 0.762008s QPS: 131.232 query delay avg: 0.076481s min: 0.036571s max: 0.111997s p90: 0.094137s p95: 0.102713s p99: 0.111997s SQL command: select  COLS(last(ts), ts, c0) from meters group by tbname;
complete query with 10 threads and 100 sql 3 spend 0.761911s QPS: 131.249 query delay avg: 0.075963s min: 0.041216s max: 0.108404s p90: 0.092369s p95: 0.099814s p99: 0.108404s SQL command: select COLS(last(ts), ts) , c0, c1, c3, c3  from meters;
complete query with 10 threads and 100 sql 4 spend 0.763622s QPS: 130.955 query delay avg: 0.078585s min: 0.044220s max: 0.106171s p90: 0.095753s p95: 0.100930s p99: 0.106171s SQL command: select COLS(last(ts), ts, c0, c1, c3, c3, tbname) from meters;
complete query with 10 threads and 100 sql 5 spend 0.723652s QPS: 138.188 query delay avg: 0.074546s min: 0.040161s max: 0.116456s p90: 0.091108s p95: 0.097477s p99: 0.116456s SQL command: select COLS(last(ts), ts), COLS(first(ts), ts) from meters;
complete query with 10 threads and 100 sql 6 spend 0.775318s QPS: 128.979 query delay avg: 0.078883s min: 0.041775s max: 0.123649s p90: 0.097067s p95: 0.101873s p99: 0.123649s SQL command: select last(ts), COLS(last(ts), c0) ,COLS(last(ts), c1) ,COLS(last(ts), c2) ,COLS(last(ts), c3)  from meters;
[02/24 01:07:45.271458] INFO: Spend 5.0680 second completed total queries: 600, the QPS of all threads:    118.390 ,error 0 (rate:0.000%)
```

结论：和 8.4.2.1 比较，功能相同，性能基本无变化

##### 8.4.2.4 cols 函数测试 2：（等效多次选择函数查询）

```c
{
    "filetype": "query",
    "cfgdir": "/etc/taos",
    "host": "127.0.0.1",
    "port": 6030,
    "user": "root",
    "password": "taosdata",
    "confirm_parameter_prompt": "no",
    "continue_if_fail": "yes",
    "databases": "test",
    "query_times": 3,
    "query_mode": "taosc",
    "specified_table_query": {
        "query_interval": 1,
        "concurrent": 8,
        "sqls": [
            {
                "sql": "select COLS(last(c0), ts), last(c0),  COLS(last(c1), ts), last(c1), COLS(last(c2), ts), last(c2), COLS(last(c3), ts), last(c3), tbname from meters group by  tbname;",
                "result": "/root/test/colsReslut/query_res1.txt"
            },            
            {
                "sql": "select last(ts), COLS(last(ts), c0, c1, c2, c3, tbname) from meters group by  tbname;",
                "result": "/root/test/colsReslut/query_res1.txt"
            },            
            {
                "sql": "select COLS(last(ts), c0, c1, c2, c3, tbname) from meters group by  tbname;",
                "result": "/root/test/colsReslut/query_res1.txt"
            }
        ]
    }
}
```

测试结果：
```c
complete query with 8 threads and 800 sql 1 spend 5.848353s QPS: 136.791 query delay avg: 0.057660s min: 0.029330s max: 0.094363s p90: 0.067617s p95: 0.071640s p99: 0.080149s SQL command: select COLS(last(c0), ts), last(c0),  COLS(last(c1), ts), last(c1), COLS(last(c2), ts), last(c2), COLS(last(c3), ts), last(c3), tbname from meters group by  tbname;
complete query with 8 threads and 800 sql 2 spend 5.708227s QPS: 140.149 query delay avg: 0.056639s min: 0.033684s max: 0.090792s p90: 0.066881s p95: 0.069866s p99: 0.078712s SQL command: select last(ts), COLS(last(ts), c0, c1, c2, c3, tbname) from meters group by  tbname;
complete query with 8 threads and 800 sql 3 spend 5.713102s QPS: 140.029 query delay avg: 0.056575s min: 0.030920s max: 0.092555s p90: 0.066580s p95: 0.070178s p99: 0.078014s SQL command: select COLS(last(ts), c0, c1, c2, c3, tbname) from meters group by  tbname;
[02/24 00:17:40.187228] INFO: Spend 17.4180 second completed total queries: 2400, the QPS of all threads:    137.788 ,error 0 (rate:0.000%)
```

结论：性能无变化，对于复杂 sql, 完成原来 sql 无法单独sql 完成的查询，耗时和一个 sql 差不多，符合预期。

### 8.5 安全性

无

### 8.6 兼容性

- 没有兼容性问题

### 8.7 本地化

无

## 9. Jira（可选）

需求见：[TS-5255](https://jira.taosdata.com:18080/browse/TS-5255)

## 10. 测试计划

暂无

## 11. 风险评估

1. 无

## 12. 测试备忘（可选）

无

## 13. 参考文档（可选）

[获取选择函数所在行的其他列值](https://taosdata.feishu.cn/wiki/BIBNwoaLAiwp6ikXigAcO0gQnKh)
