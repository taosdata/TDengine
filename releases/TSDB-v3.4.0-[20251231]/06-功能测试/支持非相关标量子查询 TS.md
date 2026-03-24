# 支持非相关标量子查询 TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-12-19 | 2025-12-23 | 1.0 | 潘魏 | 新建 |

## 2. 测试目标

本次测试的主要目标：测试非相关标量子查询功能与性能

## 3. 参考文档

JIRA: 
RS：[需求说明：支持更多子查询](https://taosdata.feishu.cn/wiki/Gi6HwAWcIimFpjkpAhXcMdNYn3N)
FS：[支持非相关标量子查询 FS](https://taosdata.feishu.cn/wiki/RP6Qw4PqNiqGNjkQbQIcXENFnAh)

## 4. 测试结论

功能、性能符合预期，测试通过。

## 5. 测试环境

- OS: Linux

## 6. 功能测试

在测试用例目录test/cases/09-DataQuerying/08-SubQuery下增加下列文件进行功能测试
-rw-r--r--  1 pw pw 10697 Dec 18 10:19 test_scalar_sub1.py
-rw-r--r--  1 pw pw 27882 Dec 19 10:08 test_scalar_sub2.py
-rw-r--r--  1 pw pw 10756 Dec 18 10:24 test_scalar_sub3a.py
-rw-r--r--  1 pw pw 10756 Dec 18 10:48 test_scalar_sub3b.py
-rw-r--r--  1 pw pw 10756 Dec 18 10:50 test_scalar_sub3c.py
-rw-r--r--  1 pw pw 10768 Dec 18 14:57 test_scalar_sub3d.py
-rw-r--r--  1 pw pw 10649 Dec 18 17:07 test_scalar_sub4a.py
-rw-r--r--  1 pw pw 10649 Dec 18 17:08 test_scalar_sub4b.py
-rw-r--r--  1 pw pw 10649 Dec 18 17:44 test_scalar_sub4c.py
-rw-r--r--  1 pw pw 10649 Dec 18 17:44 test_scalar_sub4d.py
主要测试覆盖内容包括：

| 分类 | 测试场景 | 编号 | 测试用例 | 预期行为 | 测试结果 | 说明 |
| --- | --- | --- | --- | --- | --- | --- |
| 查询功能 | 查询语句 | 1 | 查询各子句与各种非相关标量子查询的组合测试 | 定义为表达式的部分执行成功，非定位为表达式部分执行失败 | 通过 |  |
| 查询功能 | 视图语句 | 2 | 视图语句含子查询，子查询中使用视图 | 正常执行 | 通过 |  |
| 查询功能 | 嵌套查询 | 3 | 嵌套查询与嵌套非相关标量子查询的组合测试 | 正常执行 | 通过 |  |
| 查询功能 | INSERT INTO SELECT | 4 | 测试INSERT INTO SELECT语句中含非相关标量子查询 | 正常执行 | 通过 |  |
| 查询功能 | STMT查询 | 5 | 测试STMT查询语句中含非相关标量子查询 | 正常执行 | 通过 |  |
| 查询功能 | 函数、表达式 | 6 | 测试函数参数、表达式中使用非相关标量子查询 | 参数允许为表达式部分执行成功，其他报错 | 通过 |  |
| 异常功能 | 不支持的语句 | 7 | 流计算、订阅、DDL、DML语句中使用非相关标量子查询 | 报错 | 通过 |  |
| explain功能 | 查询语句explain | 8 | Explain 各模式与查询语句的组合测试 | 正常执行 | 通过 |  |

以上所有测试在 ASAN 模式下测试通过。

## 7. 易用性测试

不涉及

## 8. 长期稳定性测试

无

## 9. 性能测试

以智能电表 meters 100000000 行数据进行对比测试，排除 SMA 影响，以下两个测试语句耗时基本相当，符合预期。
select avg(current + 1) from meters where current > 0
select avg(current + (select 1 from tbx)) from meters where current > 0

## 10. 安全性测试

无

## 11. 兼容性测试

不涉及兼容性测试。

## 12. 已知问题和限制

无
