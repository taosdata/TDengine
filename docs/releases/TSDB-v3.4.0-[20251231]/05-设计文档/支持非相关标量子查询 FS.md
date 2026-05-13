# 支持非相关标量子查询 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-12-18 | - | 0.1 | 潘魏 | 初稿 |
| 2025-12-19 | 2025-12-19 | 1.0 | 潘魏 | 发布 |

## 2. 背景

非相关标量子查询作为一类会被经常使用的子查询需要在 TDengine 中获得支持，因此开发该功能。

## 3. 定义

1. 标量子查询：Scalar Subquery，子查询返回的是单个值，可以作为查询语句中的一个常量使用
2. 非相关子查询：子查询中不会引用父查询的列

## 4. 行为说明

### 4.1 功能

1. 支持非相关标量子查询，子查询只能输出单行单列数据，子查询可以为任意查询语句只要满足输出要求即可，每个子查询只会被执行一次。
2. 可以在查询语句的任意子句、函数、表达式中使用非相关标量子查询，只要语法定义为表达式的部分均可以使用非相关标量子查询。
3. 对于目前产品中未被定义为表达式但是有使用需求的场景，可以后续修改支持，目前版本表达式定义范围不会扩大。
4. 非相关标量子查询可以嵌套使用。

### 4.2 范围

1. 第一个版本只在查询中支持，可以在普通查询、嵌套查询、UNION/UNION ALL、INSERT INTO SELECT、STMT 查询、视图中使用非相关标量子查询
2. 流计算、订阅、DDL、DML 语句中暂不支持

## 5. 性能

对既有功能性能没有影响，非相关标量子查询性能与嵌套查询相当。

## 6. 安全

不涉及

## 7. 兼容性

不涉及

## 8. 运维

不涉及

## 9. 使用场景

一些使用示例语句：
```plaintext {wrap}
select (select avg(col1) from tb1), col1 from tb2;
select a.ts from tb1 a join tb2 b on a.ts = b.ts and a.f1 = (select col1 from tb1 union select col1 from tb2);
select col1 from tb1 where f1 = (select avg(f1) from tb2 interval(1s));
select f1 from tb1 partition by (select f2 from tb2);
select avg(f1) from tb1 group by f1, (select sum(f1) from tb2);
select avg(f1) from tb1 group by f1 having(avg((select f1 from tb2)) > 0);
select avg(f1), sum((select 1 from tb2)) from tb1 group by f1 order by (select f1 from tb2);
select abs((select f1 from tb1)), case when (select f2 from tb1) > 0 then 0 else 1 end from tb1;
```

## 10. 约束和限制

1. 只在查询语句中支持，流计算、订阅、DDL、DML 语句中暂不支持；
2. 不带 FROM 的查询暂不支持使用非相关子查询，也不支持作为相关子查询使用；
3. Query policy 4 暂不支持非相关标量子查询；
4. 相关标量子查询暂不支持。

## 11. 常见错误和排查

可以根据错误提示信息判断出错原因。

## 12. 可观测性

无

## 13. 安装和卸载

无

## 14. 文档

1. 不需要修改企业版文档
2. 需要修改官网文档

## 15. 参考文档

[需求说明：支持更多子查询](https://taosdata.feishu.cn/wiki/Gi6HwAWcIimFpjkpAhXcMdNYn3N)

## 16. 附录

无。
