# 子表聚合查询（或group by tbname）中支持查询tag列

## 1. 背景

当查询结果按照 tbname 分组或者本身就是在对单独一张子表查询的时候，这个时候应当支持 select tag 列，本任务目标即是支持该场景。
[TD-29093](https://jira.taosdata.com:18080/browse/TD-29093)

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/04/11 | 0.1 | 任新胜 |  |
|  |  |  |  |

## 3. 定义

tag 作用在子表上，按照子表分组或者单独查询子表，都应支持

## 4. 行为说明

### 4.1 基本说明

1. 场景：不影响之前语法和逻辑的基础上，本次新增功能主要在两个场景上
- 子表查询，
- 超级表按照 tbname 分组查询
1. tag 出现的位置：可能出现在 select, group by, where, order by 等位置，或者多个位置同时出现，均应该支持； 
2. tag 出现的形式, 支持运算和或者函数，例如 t2 为 tag
   - cast(t2 as binary(24))
   - t2 + 1

### 4.2 Sql 分场景举例说明

1. 子表查询，当 t1, t2, t3 为 tag 时候
   - 一般场景支持
      - 支持：select t1, t2, t3,count(*) from {tbname}；
   - 函数或者混合运算场景:
      - 支持：select cast(t2 as binary(12)),count(*) from {tbname};
      - 支持：select t2 + 1, count(*) from {tbname};  t2 为int 类型
   - 分组场景（因为作用在子表，group by tbname 是冗余的，但是不应影响查询）
      - 支持：select t1, t2, t3, count(*) from {tbname} group by tbname;
      - 支持：select t1, t2, t3, count(*) from {tbname} partition by tbname;
      - select t1, t2, t3, count(*) from {tbname} group by tbname, c1, t4;
   - 出现在其他位置支持（因为作用在子表，group by tbname 是冗余的，但是不应影响查询）：
      - 支持：select t1, t2, t3, count(*) from {tbname} group by tbname order by t1;
      - 支持：select t1, t2, t3, count(*) from {tbname} where t2 > 2 group by tbname;
      - 混合支持：select t1, t2, t3, count(*) from {tbname} where t2 > 2 group by tbname order by t2;
      - 混合支持：select t1, t2 + 1, t3, count(*) from {tbname} where t2 > 2 group by tbname order by t2;
   - 出现在其他位置支持：
      - 支持：select t1, t2, t3, count(*) from {tbname} order by t1;
      - 支持：select t1, t2, t3, count(*) from {tbname} where t2 > 2;
      - 混合支持：select t1, t2, t3, count(*) from {tbname} where t2 > 2 order by t2;
      - 混合支持：select t1, t2 + 1, t3, count(*) from {tbname} where t2 > 2 order by t1, t2;
2. 超级表表查询，t1, t2, t3 为 tag，没有按照 tbname 分组应当报错
   - 不支持场景：
      - 不支持：select t1, t2, t3,count(*) from {stbname}；
      - 不支持：select cast(t2 as binary(12)),count(*) from {stbname};
      - 不支持：select t2 + 1, count(*) from {stbname};  
   - 分组场景支持：
      - 支持：select t1, t2, t3, count(*) from {stbname} group by tbname;
      - 支持：select t1, t2, t3, count(*) from {stbname} partition by tbname;
      - 支持：select t1, t2, t3, count(*) from {stbname} group by tbname, c1, t4;
   - 出现在其他位置支持：
      - 支持：select t1, t2, t3, count(*) from {tbname} group by tbname order by t1;
      - 支持：select t1, t2, t3, count(*) from {tbname} where t2 > 2 group by tbname;
      - 混合支持：select t1, t2, t3, count(*) from {tbname} where t2 > 2 group by tbname order by t2;
      - 混合支持：select t1, t2 + 1, t3, count(*) from {tbname} where t2 > 2 group by tbname order by t2;

## 5. 性能

     无影响

## 6. 兼容性

    无影响

## 7. 运维

   无影响

## 8. 使用场景

  基础场景

## 9. 约束和限制

   无

## 10. 常见错误和排查

   不支持语法应该报错

## 11. 可观测性

  taos shell 直接执行 sql 确认结果

## 12. 安装和卸载

   无

## 13. 文档

  不需要

## 14. 参考文档

  无
