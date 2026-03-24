# 支持 ALL/SOME/ANY/[NOT]EXISTS 运算符+子查询 TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-03 | 2026-03-06 | 0.1 | 潘魏 | 新建，定义ALL/SOME/ANY/EXISTS+子查询测试范围、用例、场景等 |

## 2. 测试目标

本次测试的主要目标：验证 TDengine 中 SQL ALL/SOME/ANY/EXISTS/NOT EXISTS 运算符与非相关子查询组合的功能正确性、兼容性及性能表现，确保其语法与功能符合 SQL 标准，满足业务查询需求。

## 3. 参考文档

- JIRA: 
- RS：[需求说明：支持更多子查询](https://taosdata.feishu.cn/wiki/Gi6HwAWcIimFpjkpAhXcMdNYn3N)
- FS：[支持 ALL/ANY/SOME/[NOT]EXISTS 运算符与子查询 FS](https://taosdata.feishu.cn/wiki/OGnMwyFB8i1PRqkNM4Kca0b0nCe)

## 4. 测试结论

功能、性能符合预期，测试通过。

## 5. 测试环境

- OS: Linux
- TDengine 版本: v3.4.1.0
- 测试工具：new_test_framework（含 tdLog、tdSql、tdCom 组件）

## 6. 功能测试

在测试用例目录 test/cases/09-DataQuerying/08-SubQuery 下通过以下测试文件开展功能测试，覆盖全场景验证：

| 测试文件 | 核心测试场景 |
| --- | --- |
| test_quantified_sub1.py | SELECT 子句中的数据类型与运算符+子查询的组合正确性测试： SELECT 子句中不同类型列、常量、表达式、子查询与运算符+子查询的组合测试； 支持 EXISTS/NOT EXISTS； 验证标签列（tags）查询 |
| test_quantified_sub2.py | WHERE 子句中的数据类型与运算符+子查询的组合正确性测试： WHERE 子句中不同类型列、常量、表达式、子查询与运算符+子查询的组合测试； 覆盖不同过滤条件（空过滤、NULL 过滤等） |
| test_quantified_sub3.py | 复杂查询场景（DISTINCT、聚合函数、窗口函数、JOIN、GROUP BY、UNION 等）与运算符+子查询组合 |
| test_quantified_sub4.py | 全覆盖边界测试： 函数参数（单参数、多参数、字符串/日期/数学函数等）、流计算/订阅/DDL/DML 不支持场景、视图查询、INSERT INTO SELECT 等场景 |
| test_quantified_sub5a.py ~ test_quantified_sub5d.py | 嵌套查询场景（子查询中嵌套子查询）； 覆盖不同目标表（tb1、tb3、tbe、st1） |
| test_quantified_sub6a1.py ~ test_quantified_sub6d2.py | EXPLAIN/EXPLAIN VERBOSE/EXPLAIN ANALYZE 语句与运算符+子查询组合； 验证执行计划正确性； |

### 6.1 测试覆盖内容详情

| 分类 | 测试场景 | 编号 | 测试用例说明 | 预期行为 | 测试结果 | 说明 |
| --- | --- | --- | --- | --- | --- | --- |
| 基础查询功能 | SELECT 子句组合 | 1 | 列/常量/标签列与 ALL/ANY/SOME/EXISTS/NOT EXISTS 结合子查询； 支持 DISTINCT、多列组合查询 | 语法正确，查询结果与预期一致； 不支持的语句报错； | 通过 | 覆盖 f1~f19 全数据类型列、JSON 标签列（tg1->'k1'） |
| 基础查询功能 | WHERE 子句组合 | 2 | 列/常量与 ALL/ANY/SOME 结合子查询； 支持多条件逻辑（AND/OR） | 过滤结果正确，符合运算符逻辑； 不支持的语句报错； | 通过 | 包含空过滤（WHERE 1=0）、NULL 过滤（IS NULL/IS NOT NULL）等 |
| 复杂查询功能 | JOIN 关联查询 | 3 | INNER JOIN/LEFT JOIN/RIGHT JOIN 等关联条件中嵌入运算符+子查询 | 关联逻辑正确，结果行数与预期一致 | 通过 | 覆盖等值关联、非等值关联场景 |
| 复杂查询功能 | 聚合与窗口函数 | 4 | GROUP BY/HAVING、INTERVAL/SESSION/STATE_WINDOW 等窗口函数与运算符+子查询组合 | 聚合结果、窗口计算结果正确 | 通过 | 支持聚合函数（SUM/AVG/COUNT 等）与运算符+子查询嵌套 |
| 复杂查询功能 | 嵌套查询 | 5 | 子查询中嵌套子查询（双层嵌套）； 多运算符组合（如 ANY 嵌套 EXISTS） | 嵌套逻辑执行正确，结果符合预期 | 通过 | 覆盖 SELECT/FROM/WHERE 等不同子句嵌套 |
| 复杂查询功能 | 集合运算 | 6 | UNION/UNION ALL 与运算符+子查询组合 | 集合合并逻辑正确，去重/不去重结果符合预期 | 通过 | 验证运算符+子查询在集合运算中的兼容性 |
| 特殊场景功能 | 函数参数嵌入 | 7 | 数学函数、字符串函数、日期函数等参数中嵌入运算符+子查询 | 支持的函数计算结果正确，无语法报错； 不支持的函数报错； | 通过 | 覆盖所有函数，包括单参数（abs/ceil等）、多参数（pow/concat等）函数 |
| 特殊场景功能 | 视图与虚拟表 | 8 | 视图（VIEW）、虚拟表（VTABLE）查询中使用运算符+子查询 | 视图查询结果正确，支持嵌套引用 | 通过 | 验证视图定义与查询时的兼容性 |
| 特殊场景功能 | 插入查询 | 9 | INSERT INTO SELECT 语句中嵌入运算符+子查询 | 数据插入成功，插入内容与预期一致 | 通过 | 支持从运算符+子查询结果集插入目标表 |
| 异常场景功能 | 不支持语句验证 | 10 | 流计算（CREATE STREAM）、订阅（CREATE TOPIC）、DDL/DML 语句中使用运算符+子查询 | 执行报错，符合功能约束 | 通过 | 验证不支持场景的错误处理逻辑 |
| 执行计划功能 | EXPLAIN 验证 | 11 | EXPLAIN/EXPLAIN VERBOSE/EXPLAIN ANALYZE 与运算符+子查询组合 | 执行计划生成正常，无语法报错 | 通过 | 验证执行计划对运算符+子查询的解析能力 |

以上所有测试在 ASAN 模式下执行，验证内存安全性。

## 7. 易用性测试

不涉及

## 8. 长期稳定性测试

无

## 9. 性能测试

### 9.1 测试场景

1. 大数据量单表查询：基于 1 亿行智能电表（meters）数据，验证 `WHERE column ALL/ANY/SOME (select column from table limit N)` 与等价常量查询的耗时对比；

| 分类 | SQL 语句 | 耗时 |
| --- | --- | --- |
| select count(*) from meters where voltage = all (select voltage from d0 limit 2); | 1.0s |
| select count(*) from meters where voltage = (select voltage from d0 limit 1) and voltage = (select voltage from d0 limit 1, 1); | 4.1s |
| select count(*) from meters where voltage != all (select voltage from d0 limit 3); | 3.4s |
| select count(*) from meters where voltage not in (select voltage from d0 limit 3); | 3.4s |
| select count(*) from meters where voltage > all (select voltage from d0 limit 2); | 2.4s |
| select count(*) from meters where voltage > (select voltage from d0 limit 1) and voltage > (select voltage from d0 limit 1, 1); | 5.1s |
| select count(*) from meters where voltage = any (select voltage from d0 limit 2); | 3.1s |
| select count(*) from meters where voltage in (select voltage from d0 limit 2); | 3.1s |
| select count(*) from meters where voltage != any (select voltage from d0 limit 2); | 1.0s |
| select count(*) from meters where voltage != (select voltage from d0 limit 1) or voltage != (select voltage from d0 limit 1, 1); | 4.1s |
| select count(*) from meters where voltage > any (select voltage from d0 limit 2); | 2.2s |
| select count(*) from meters where voltage > (select voltage from d0 limit 1) or voltage > (select voltage from d0 limit 1, 1); | 4.7s |

1. EXISTS 短路求值性能：验证子查询返回第一行后是否立即终止执行

| SQL 语句 | 耗时 |
| --- | --- |
| select count(*) from meters where exists (select * from meters where voltage > 230); | 1.3s |
| select count(*) from (select * from meters where voltage > 230); | 2.5s |

## 10. 安全性测试

无

## 11. 兼容性测试

不涉及兼容性测试。

## 12. 已知问题和限制

无
