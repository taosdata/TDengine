# Feature - TS-5667 - Interp 扩展 - Test Spec

测试目标
<quote-container>
测试Interp 扩展的几个功能的正确性, 包括FILL NEAR, _irowts_origin, range扩展
- FILL NEAR, 测试FILL NEAR的支持范围, 仅在查询的INTERP中支持, 测试FILL NEAR的查询结果正确性
- 新的interp 伪列_irowts_origin, 测试其支持范围, 以及查询结果正确性.
- range扩展, 测试新增时间点模式下的范围FILL, 测试其支持范围和查询结果的正确性
</quote-container>

## 1. 变更历史

| 日期 | 版本 | 作者 | 备忘 |
| --- | --- | --- | --- |
| 2024/11/27 | 1.0 | @王加明 |  |

## 2. 测试范围

<quote-container>
主要包括
- interp查询测试, 包括不影响已有功能, 新功能支持条件检查, 以及查询结果正确性.
- 带有FILL语法的查询测试, 如窗口查询
- 其他支持interp查询的场景如流计算
</quote-container>

## 3. 测试结论

<quote-container>
测试通过
</quote-container>

## 4. 已知问题和限制

无

## 5. 测试环境

- OS:  Debian GNU/Linux 11 (bullseye)

## 6. 测试数据（可选）

库test, 4 vnode, 10张子表, 每张表10000行数据, 开始时间戳: 1537146000000, step: 500ms
10列, 6个tag.

## 7. 测试用例

### 7.1 功能

| 类型 | 测试目的 | 测试步骤 | 预期结果 | 是否为基础用例 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 使用限制校验 | 明确interp fill near的使用范围 | 1. select count(*) from test.meters interval(1s) fill(near)
1. create stream s1 trigger force_window_close into s_res_tb as select _irowts, interp(c1), interp(c2)from meters partition by tbname every(1s) fill(near) | 1. 报告语法错误, 窗口查询的FILL不支持NEAR
2. 报告错误号为:INVALID_STREAM_QUERY, 信息为: FILL NEAR is not supported by stream | Y | Pass |  |
|  | 明确伪列_irowts_origin使用范围 | 1. select _irowts, _irowts_origin, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:01:00') every(1s) fill(NULL); (备注: NULL替换为LINEAR, NULL_F以及任何其他非PREV/NEXT/NEAR的fill类型都会报错)
1. create stream s1 trigger force_window_close into s_res_tb as select _irowts_origin, interp(c1), interp(c2)from meters partition by tbname every(1s) fill(prev);
2. select _irowts_origin, count(*) from meters interval(10m) | 1. 报告错误: PAR_FILL_NOT_ALLOWED_FUNC, 错误信息为: _irowts_origin can only be used with FILL PREV/NEXT/NEAR
3. 错误号: PAR_INVALID_STREAM_QUERY, 错误信息为: _irowts_origin is not supported by stream
4. 列错误, _irosts_origin在窗口查询中不支持 | Y | Pass |  |
|  | 明确interp range中带时间范围区间的使用限制 | 1. select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) fill(near)
1. select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) fill(near, 1)
2. select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:02:00', 1h) fill(near, 1, 1)
3. select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) every(1s) fill(prev, 1, 1)
4. select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) fill(NULL, 1, 1)
5. select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) fill(NULL)
6. select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 0h) fill(near, 1, 1)
7. select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1y) fill(near, 1, 1)
8. select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1n) fill(near, 1, 1) | 1. 报告错误: PAR_WRONG_VALUE_TYPE, 错误信息为: Must specify values.
9. 报告错误: PAR_WRONG_VALUE_TYPE, 错误信息为: num of fill value mismatch
10. 语法错误(目前还不支持range内区间上再加interval范围的用法, 只能是时间点的方式)
11. 报告错误: PAR_INVALID_INTERP_CLAUSE, 错误信息为: Range clause with around interval can't be used with EVERY clause
12. 语法错误, 当range中带时间区间时, 只能使用 fill PREV/NEXT/NEAR, FILL NULL没有意义
13. 报告错误: PAR_INVALID_FILL_TIME_RANGE, 原因同上
14. 报告错误: PAR_INVALID_FILL_TIME_RANGE, range内的时间区间不能是0
15. 报告错误: PAR_WRONG_VALUE_TYPE, 不支持时间范围单位为年
16. 报告错误: PAR_WRONG_VALUE_TYPE, 不支持时间范围单位为月 | Y | Pass |  |
|  | 当不使用interp扩展功能时, 非法的查询语句应正常报错 | 1. select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:02:00') fill(NULL, 1, 1)
1. select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:02:00') fill(linear, 1, 1)
2. select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:01:00') every(1s) fill(near, 1, 1); (备注: 替换为其他非PREV/NEXT/NEAR模式都报错)
3. select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00') every(1s) fill(near, 1, 1); (备注: 替换为其他非PREV/NEXT/NEAR模式都报错) | 1. 报告语法错误, 使用正常的range时, 除FILL VALUE外, 不能指定值
4. 报告语法错误, 同上
5. 报告错误: PAR_WRONG_VALUE_TYPE, 错误信息为: Can't specify fill values, 同上
6. 同上, 使用单时间点模式的range时, 非FILL VALUE模式时也不能指定值. | Y | Pass |  |
| 查询结果校验 | NEAR和_irowts_origin正确性校验 | 1. 分别测试interp FILL NEAR查询中在前, 后, 前后没有数据以及前后都有数据时的查询结果.
根据数据时间区间, 前后扩大不超过1h后, 生成range范围, 使用SQL 模板:
select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.t0 range({range_start}, {range_end}) every({every}s) fill(near)生成SQL查询.
1. 与1.类似, 但是interp查询中range使用单时间点模式, sql模板为:
select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.t0 range({range_point}, 1h) fill(near, 1, 2)
1. 使用带Where 条件的查询, 随机生成where条件, select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.t0 where ts between {range_where_start} and {range_where_end} range({range_start}, {range_end}) every({every}s) fill(near)
2. 使用带WHERE条件的查询, 随机生成where条件, select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.t0 where ts between {range_where_start} and {range_where_end} range({range_point}, 1h) fill(near, 1, 2) | 1. 校验每次查询查询结果的每一行数据, 若该行数据的_isfilled为true, 则_irowts_origin列在该表的原始结果中可以查找到, 并且该时间戳是与_irowts值最接近的时间戳. (若某一行数据存在前后两个时间戳, 且都和当前行时间戳一样近, 则取的小时间戳.), 且interp(c1), interp(c2)的值为原始数据中查找到的改行c1, c2的值.
3. 由于range时间范围给的1h, 因此都能找到结果, FILL中填的(1,2)不会被使用, 检查逻辑同上.
4. 同1.
5. 同2. | Y | Pass |  |
|  | interp range中使用单时间戳和时间范围查询方式的结果正确性校验 | 1. select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1d) fill(near, 1, 2);
1. select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2018-09-18 10:25:00', 1d) fill(prev, 3, 4);
2. select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2018-09-16 08:25:00', 1d) fill(next, 5, 6);
3. select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2018-09-17 11:23:20', 1d) fill(next, 5, 6);
4. select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2018-09-16 09:00:01', 1d) fill(next, 1, 2);
5. select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2018-09-18 10:23:19', 1d) fill(prev, 1, 2);
6. select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.meters range('{last_ts}', 1a) fill(next, 1, 2) | 1. 当range内带时间范围时, 且前后范围内没有查找到数据时, _irowts_origin范围NULL, interp的列范围指定的值, 即1,2, _isfilled范围true
7. 同上, 但是只在当前时间戳之前的数据中查找.
8. 同上, 但是只在当前时间戳之后的数据中查找.
9. 指定时间点之后已无数据, 此时返回空结果集, prev同理.
10. 不适用fill指定的填充数据, _irowts_origin为表内的first(ts)时间戳, interp列的值为first(ts)对应c1,c2列的值.
11. 类似5, 不是first(ts)行, 而是last(ts)对应的行.
12. 当range时间点有数据时, 使用时间行的数据, 此处查询结果都为last(ts)对应的行的数据. | Y | Pass |  |

### 7.2 可用性

无

### 7.3 可靠性

无

### 7.4 性能

无

### 7.5 安全性

无

### 7.6 兼容性

无

### 7.7 本地化

无

## 8. 待讨论（可选）

无

## 9. Jira（可选）

[TS-5667](https://jira.taosdata.com:18080/browse/TS-5667)

## 10. 测试计划（可选）

无

## 11. 风险评估

无

## 12. 测试备忘（可选）

无

## 13. 参考文档（可选）

功能扩展的简单说明: [Interp Fill扩展](https://taosdata.feishu.cn/docx/IOL4dHJXmoKNn8xBLPgc7Lv9ndf)
