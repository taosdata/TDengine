# 支持 IN + 非相关子查询 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-01-19 | - | 0.1 | 潘魏 | 初稿 |
| 2026-01-21 | 修订格式 | 1.0 | 关胜亮 | 修订格式 |

## 2. 背景

IN + 非相关子查询作为一类会被经常使用的子查询需要在 TDengine 中获得支持，因此开发该功能。

## 3. 定义

1. 非相关子查询：子查询中不会引用父查询的列

## 4. 行为说明

### 4.1 语法

支持下列两种语法格式：
```plaintext {wrap}
IN (subquery)
or
IN ((subquery))
```

### 4.2 功能

1. 支持 IN + 非相关子查询，子查询只能输出单列数据，子查询可以为任意查询语句只要满足输出要求即可，每个子查询只会被执行一次。
2. IN + 子查询可以在查询语句的任意子句、函数、表达式中使用，只要语法定义支持即可。
3. 对于目前产品中未支持 IN + 子查询的使用但是有使用需求的场景，可以后续修改支持，目前版本 IN + 子查询的定义适用范围不会扩大。
4. 非相关子查询可以嵌套使用。

### 4.3 范围

1. 第一个版本只在查询中支持，可以在普通查询、嵌套查询、UNION/UNION ALL、INSERT INTO SELECT、STMT 查询、视图中使用 IN + 非相关子查询
2. 流计算、订阅、DDL、DML 语句中暂不支持

## 5. 性能

对既有功能性能没有影响，非相关子查询性能与嵌套查询相当。

## 6. 安全

不涉及

## 7. 兼容性

不涉及

## 8. 运维

不涉及

## 9. 使用场景

一些使用示例语句：
```plaintext {wrap}
select col1 in (select col1 from tb1) from tb2;
select a.ts from tb1 a join tb2 b on a.ts = b.ts and a.f1 in (select col1 from tb1 union select col1 from tb2);
select col1 from tb1 where f1 in (select avg(f1) from tb2 interval(1s));
select avg(f1) from tb1 group by f1 having(f1 in ((select f1 from tb2)));
select case when f1 in (select f2 from tb1) then 0 else 1 end from tb1;
```

## 10. 约束和限制

1. 只在查询语句中支持，流计算、订阅、DDL、DML 语句中暂不支持；
2. 不带 FROM 的查询暂不支持使用 IN + 非相关子查询，也不支持作为相关子查询使用；
3. Query policy 4 暂不支持 IN + 非相关子查询；
4. 相关子查询暂不支持。

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
