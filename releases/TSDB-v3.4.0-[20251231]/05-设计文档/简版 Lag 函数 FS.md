# 简版 Lag 函数 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-12-19 | - | 0.1 | 金明磊 | 初稿 |
| 2025-12-24 | 2025-12-24 | 1.0 | 金明磊 | 发布 |

## 2. 背景

物联网场景中，不同的度量往往由不同的传感器在不同的时刻上传，所以在某一时刻，数据表的一行数据中，往往有一列的值是不空的，而其它列均为空值。比如下面这个例子：
```sql {wrap}
taos> select * from db.meters;
           ts            |     current |     voltage |
======================================================
 2025-12-02 10:28:08.000 |           1 |           2 |
 2025-12-02 10:28:18.000 |        NULL |           4 |
 2025-12-02 10:28:21.000 |           5 |        NULL |
 2025-12-02 10:28:27.000 |           7 |           8 |
```

当计算每行的功率时，第二行及第三行的结果是空值：
```sql {wrap}
taos> select _c0, current * voltage as power from db.meters;
           ts            |     power   |
========================================
 2025-12-02 10:28:08.000 |           2 |
 2025-12-02 10:28:18.000 |        NULL |
 2025-12-02 10:28:21.000 |        NULL |
 2025-12-02 10:28:27.000 |          56 |
```

合理的计算应该是，当列数据为空时，自动向前查找最近的非空值参与计算。
需求 [简版 Lag 函数 RS](https://taosdata.feishu.cn/wiki/Iq3KwD9DKifFKVkg40lcLGEynD2)

## 3. 定义

无

## 4. 行为说明

### 4.1 语法

简化 LAG 函数调用方式，无需参数及 OVER 子句，语法格式如下：
```plaintext {wrap}
lag(expr)
```

### 4.2 填充规则

1. 对查询中使用 LAG 函数的列，自动向前追溯最近的非 NULL 值填充；
2. 若当前行之前无数据（如首行 NULL 值），保持 NULL 值；
3. 填充逻辑优先于列运算执行（如先填充 v1、v2，再计算 LAG (v1)*LAG (v2)）。

### 4.3 场景适配

1. 超级表：按子表维度分别执行 LAG 函数插值逻辑；
2. 虚拟表：适配虚拟表数据源映射后的 NULL 值填充；
3. PARTITION BY：按分区维度独立执行插值逻辑。

### 4.4 示例

```sql {wrap}
taos> select *, lag(current) * lag(voltage) as power from db.meters;
           ts            |     current |     voltage |    power |
=================================================================
 2025-12-02 10:28:08.000 |           1 |           2 |        2 |
 2025-12-02 10:28:18.000 |        NULL |           4 |        4 |
 2025-12-02 10:28:21.000 |           5 |        NULL |       20 |
 2025-12-02 10:28:27.000 |           7 |           8 |       56 |
```

## 5. 安全

1. 查询用功能函数，不涉及安全特性

## 6. 性能

1. 虚拟表 / 超级表场景下，填充相同行数的数据，性能应该和 Interp 函数类似；
2. 扫描处理所有列，性能相比 DIFF 函数无下降。

## 7. 兼容性

1. 与现有 SQL 语法兼容，不影响未使用简化 LAG 函数的查询逻辑
2. 后续扩展参数配置时，需与窗口函数体系兼容，无功能冲突
3. 新增功能，原来不支持的语法改为支持。

## 8. 运维

无。

## 9. 使用场景

| 场景分类 | 场景描述 | SQL 示例 |
| --- | --- | --- |
| 正常 | 查询子表current列 | select lag(current) from test.d1; |
| 正常 | 查询子表多个列 | select lag(current), lag(phase) from test.d1; |
| 正常 | 查询子表current列，和sum函数一起使用 | select sum(current), lag(phase) from test.d1; |
| 正常 | 查询超级表，和partition一起使用 | select tbname, lag(current) from test.meters partition by tbname; |
| 正常 | 查询子表与时间窗口一起使用 | SELECT lag(current) FROM d1 WHERE _rowts >= '2017-07-14 10:40:00.005' INTERVAL (1m, AUTO); |
| 异常 | 和查询列一起使用 | select current, lag(phase) from test.d1; |
| 异常 | 和非聚合函数一起使用 | select diff(current), lag(phase) from test.d1; |
| 异常 | 不指定具体列，使用 * | SELECT lag(*) FROM d1 WHERE _rowts >= '2017-07-14 10:40:00.005' INTERVAL (1m, AUTO); |

## 10. 约束和限制

- 企业版和社区版都支持

## 11. 常见错误和排查

无

## 12. 可观测性

无

## 13. 安装和卸载

无

## 14. 文档

需要修改官网文档

## 15. 参考文档

[简版 Lag 函数 RS](https://taosdata.feishu.cn/wiki/Iq3KwD9DKifFKVkg40lcLGEynD2)
[Understanding the LAG() Function in SQL: A Comprehensive Guide](https://www.datacamp.com/tutorial/sql-lag)
[MySQL LAG()](https://dev.mysql.com/doc/refman/9.4/en/window-function-descriptions.html#function_lag)

## 16. 附录

无。
