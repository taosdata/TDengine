# Interp 函数支持插值时间范围 TS

测试目标
测试Interp 函数支持插值时间范围功能的正确性及性能影响。
JIRA: [TS-5486](https://jira.taosdata.com:18080/browse/TS-5486)

## 1. 变更历史

| 日期 | 版本 | 作者 | 备忘 |
| --- | --- | --- | --- |
| 2025/02/12 | 1.0 | @潘魏 |  |

## 2. 测试范围

主要包括
Interp 函数支持插值时间范围功能测试, 包括不影响已有功能、 新功能边界以及查询结果正确性。
Interp 函数在指定插值时间范围时的性能对比。

## 3. 测试结论

测试通过

## 4. 已知问题和限制

无

## 5. 测试环境

- OS:  Ubuntu 22.04 x64

## 6. 测试用例

### 6.1 功能

| 类型 | 测试目的 | 测试步骤 | 预期结果 | 测试结果 |
| --- | --- | --- | --- | --- |
| 正常用例 | 范围插值prev且含有插值范围限制结果正确性，含单列、多列、伪列、不同数据类型等场景 | select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(prev, 99);
select interp(c1), _isfilled from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(prev, 99);
select _isfilled, _irowts, interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(prev, 99);
select interp(c1), _irowts_origin from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(prev, 99);
select interp(c1), interp(c4) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(prev, 99, 98);
select interp(c1), _isfilled, interp(c6) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(prev, 99, false);
select _isfilled, _irowts, interp(c1), interp(c5) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(prev, 99, 98);
select interp(c1), _irowts_origin, interp(c4) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(prev, 99, 'a');
select _irowts_origin, interp(c1), _irowts, interp(c6) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(prev, 99, 0); | 插值结果正确，其他伪列结果正确 | Pass |
|  | 范围插值prev且含有插值范围限制且含排序结果正确性，含插值排序、伪列排序、多列排序等场景 | select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(prev, 99) order by _irowts;
select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(prev, 99) order by interp(c1) desc;
select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(prev, 99) order by _irowts desc;
select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(prev, 99) order by _irowts_origin;
select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(prev, 99) order by _irowts_origin desc;
select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(prev, 99) order by interp(c1), _irowts_origin desc;
select _irowts, interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(prev, 99) order by interp(c1) desc, _irowts desc;
select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(prev, 99) order by _irowts, _irowts_origin desc;
select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(prev, 99) order by _irowts desc, _irowts_origin;
select interp(c1), interp(c4) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(prev, 99, NULL) order by _irowts;
select interp(c1), interp(c6) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(prev, 99, 0) order by _irowts desc;
select interp(c1), interp(c4), interp(c5) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(prev, 99, 9.9, 9) order by _irowts_origin;
select interp(c1), interp(c2) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(prev, 99, 9) order by _irowts_origin desc;
select interp(c6), interp(c1) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(prev, 3, 99) order by _irowts, _irowts_origin desc;
select interp(c1), interp(c4) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(prev, 100, 99) order by _irowts desc, _irowts_origin;
select _irowts_origin, interp(c6), interp(c1) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(prev, 99, 99) order by _irowts;
select _irowts_origin, interp(c4), interp(c1), _irowts from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(prev, 99, 9) order by _irowts_origin desc, _irowts;
select _isfilled, interp(c4), _irowts_origin, _irowts, interp(c1), tbname from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(prev, 99, 1) order by _irowts_origin desc, _irowts desc; | 插值结果正确，其他伪列结果正确，排序结果正确 | Pass |
|  | 范围插值next且含有插值范围限制结果正确性，含单列、多列、伪列、不同数据类型等场景 | select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(next, 99);
select interp(c1), _isfilled from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(next, 99);
select _isfilled, _irowts, interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(next, 99);
select interp(c1), _irowts_origin from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(next, 99);
select _irowts_origin, interp(c1), _irowts from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(next, 99);
select interp(c1), interp(c4) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(next, 99, 98);
select interp(c1), _isfilled, interp(c6) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(next, 99, false);
select _isfilled, _irowts, interp(c1), interp(c5) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(next, 99, 98);
select interp(c1), _irowts_origin, interp(c4) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(next, 99, 'a');
select _irowts_origin, interp(c1), _irowts, interp(c6) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(next, 99, 0); | 插值结果正确，其他伪列结果正确 | Pass |
|  | 范围插值next且含有插值范围限制且含排序结果正确性，含插值排序、伪列排序、多列排序等场景 | select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(next, 99) order by _irowts;
select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(next, 99) order by interp(c1) desc;
select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(next, 99) order by _irowts desc;
select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(next, 99) order by _irowts_origin;
select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(next, 99) order by _irowts_origin desc;
select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(next, 99) order by interp(c1), _irowts_origin desc;
select _irowts, interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(next, 99) order by interp(c1) desc, _irowts desc;
select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(next, 99) order by _irowts, _irowts_origin desc;
select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(next, 99) order by _irowts desc, _irowts_origin;
select _irowts_origin, interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(next, 99) order by _irowts;
select _irowts_origin, interp(c1), _irowts from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(next, 99) order by _irowts_origin desc, _irowts;
select _isfilled, _irowts_origin, _irowts, interp(c1), tbname from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(next, 99)order by _irowts_origin desc, _irowts desc;
select interp(c1), interp(c4) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(next, 99, NULL) order by _irowts;
select interp(c1), interp(c6) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(next, 99, 0) order by _irowts desc;
select interp(c1), interp(c4), interp(c5) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(next, 99, 9.9, 9) order by _irowts_origin;
select interp(c1), interp(c2) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(next, 99, 9) order by _irowts_origin desc;
select interp(c6), interp(c1) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(next, 3, 99) order by _irowts, _irowts_origin desc;
select interp(c1), interp(c4) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(next, 100, 99) order by _irowts desc, _irowts_origin;
select _irowts_origin, interp(c6), interp(c1) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(next, 99, 99) order by _irowts;
select _irowts_origin, interp(c4), interp(c1), _irowts from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(next, 99, 9) order by _irowts_origin desc, _irowts;
select _isfilled, interp(c4), _irowts_origin, _irowts, interp(c1), tbname from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(next, 99, 1) order by _irowts_origin desc, _irowts desc; | 插值结果正确，其他伪列结果正确，排序结果正确 | Pass |
|  | 范围插值near且含有插值范围限制结果正确性，含单列、多列、伪列、不同数据类型等场景 | select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(near, 99);
select interp(c1), _isfilled from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(near, 99);
select _isfilled, _irowts, interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(near, 99);
select interp(c1), _irowts_origin from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(near, 99);
select _irowts_origin, interp(c1), _irowts from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(near, 99);
select interp(c1), interp(c4) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(near, 99, 98);
select interp(c1), _isfilled, interp(c6) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(near, 99, false);
select _isfilled, _irowts, interp(c1), interp(c5) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(near, 99, 98);
select interp(c1), _irowts_origin, interp(c4) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(near, 99, 'a');
select _irowts_origin, interp(c1), _irowts, interp(c6) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(near, 99, 0); | 插值结果正确，其他伪列结果正确 | Pass |
|  | 范围插值near且含有插值范围限制且含排序结果正确性，含插值排序、伪列排序、多列排序等场景 | select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(near, 99) order by _irowts;
select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(near, 99) order by interp(c1) desc;
select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(near, 99) order by _irowts desc;
select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(near, 99) order by _irowts_origin;
select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(near, 99) order by _irowts_origin desc;
select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(near, 99) order by interp(c1), _irowts_origin desc;
select _irowts, interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(near, 99) order by interp(c1) desc, _irowts desc;
select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(near, 99) order by _irowts, _irowts_origin desc;
select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(near, 99) order by _irowts desc, _irowts_origin;
select _irowts_origin, interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(near, 99) order by _irowts;
select _irowts_origin, interp(c1), _irowts from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(near, 99) order by _irowts_origin desc, _irowts;
select _isfilled, _irowts_origin, _irowts, interp(c1), tbname from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(near, 99)order by _irowts_origin desc, _irowts desc;
select interp(c1), interp(c4) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(near, 99, NULL) order by _irowts;
select interp(c1), interp(c6) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(near, 99, 0) order by _irowts desc;
select interp(c1), interp(c4), interp(c5) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(near, 99, 9.9, 9) order by _irowts_origin;
select interp(c1), interp(c2) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(near, 99, 9) order by _irowts_origin desc;
select interp(c6), interp(c1) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(near, 3, 99) order by _irowts, _irowts_origin desc;
select interp(c1), interp(c4) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(near, 100, 99) order by _irowts desc, _irowts_origin;
select _irowts_origin, interp(c6), interp(c1) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(near, 99, 99) order by _irowts;
select _irowts_origin, interp(c4), interp(c1), _irowts from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(near, 99, 9) order by _irowts_origin desc, _irowts;
select _isfilled, interp(c4), _irowts_origin, _irowts, interp(c1), tbname from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(near, 99, 1) order by _irowts_origin desc, _irowts desc; | 插值结果正确，其他伪列结果正确，排序结果正确 | Pass |
| 异常用例 | 指定的插值value个数不足 | select interp(c1), interp(c4) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', 1s) every(2s) fill(prev, 99); | DB error: Filled values number mismatch | Pass |
|  | 非法插值范围值 | select interp(c1) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', -1s) every(2s) fill(prev, 99); | DB error: Range interval must be greater than | Pass |
|  | 没有interp函数 | select _irowts from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(near); | DB error: Invalid column name: _irowts | Pass |
|  | 缺少FILL语句 | select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s); | DB error: Missing FILL clause | Pass |
|  | 没有EVERY | select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) fill(near, 2); | DB error: Missing RANGE clause, EVERY clause or FILL clause | Pass |
|  | 缺少默认FILL值 | select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(near); | DB error: Filled values number mismatch | Pass |
|  | 默认FILL值非常量 | select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(near, c1); | DB error: Fill value can only accept constant | Pass |
|  | 插值范围值非法 | select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', 1s, '2020-01-01 00:00:30.000') every(2s) fill(near, 99);
select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1) every(2s) fill(prev, 99);
 select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', '1s') every(2s) fill(near, 99); | DB error: Invalid range interval | Pass |
|  | 不支持的FILL选项 | select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(linear, 99);
select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(null, 99);
select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(null_f, 99); | DB error: syntax error near ", 99);" | Pass |
|  | 不支持的FILL选项 | select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(value, 99);
select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(2s) fill(value_f, 99); | DB error: Range with interval can only used with fill PREV/NEXT/NEAR | Pass |
|  | 不支持的插值范围单位 | select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1n) every(2s) fill(prev, 99);
select interp(c1) from test.td32861 range('2020-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1y) every(2s) fill(prev, 99); | DB error: Unsupported time unit in RANGE clause | Pass |
|  | 流计算不支持范围插值 | create stream s1 trigger force_window_close into test.s1res as select _irowts, interp(c1), interp(c2)from test.td32727 partition by tbname range('20
20-01-01 00:00:00.000', '2020-01-01 00:00:30.000', 1s) every(1s) fill(near, 1, 1); | DB error: Stream Unsupported RANGE clause | Pass |

### 6.2 可用性

无

### 6.3 可靠性

无

### 6.4 性能

通过单表1亿条数据作为输入进行 interp 插值，重复运行10次查询，得出测试验证性能对比如下：

|  | 不带插值时间范围限制 | 带插值时间范围限制 |
| --- | --- | --- |
| 查询平均耗时（单位：秒） | 12.547398 | 12.548613 |

结论：性能基本没有变化。
附 taosBenchmark 测试配置文件：
<view type="1">

  > ⚠ 嵌入文件，需在飞书中查看 (token: AaMHbaPNKoFbIYxmyHWcVOtlnFd)

</view>

<view type="1">

  > ⚠ 嵌入文件，需在飞书中查看 (token: HawVb9RFFoojhjxKf1qciqgnnLc)

</view>

<view type="1">

  > ⚠ 嵌入文件，需在飞书中查看 (token: CokGbWwumoe9y1xcqgwcu6lvn4d)

</view>

### 6.5 安全性

无

### 6.6 兼容性

无

### 6.7 本地化

无

## 7. 风险评估

无

## 8. 参考文档

[Interp 函数支持插值时间范围](https://taosdata.feishu.cn/wiki/Vojlwl1KJirmE0kR55oc2FeLnnJ)
