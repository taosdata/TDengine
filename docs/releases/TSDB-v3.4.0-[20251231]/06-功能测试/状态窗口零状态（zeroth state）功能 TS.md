# 状态窗口零状态（zeroth state）功能 TS

## 1. 测试目标

- 在批查询和流计算中，状态窗口支持零状态参数，结果正确

## 2. 参考文档

JIRA: [TD-37942](https://jira.taosdata.com:18080/browse/TD-37942)
[状态窗口零状态（zeroth state）功能 FS](https://taosdata.feishu.cn/wiki/Q9LwwP5AXi0nIJkTnjbcKzIfnVe)

## 3. 变更历史

| 日期 | 版本 | 作者 | 备忘 |
| --- | --- | --- | --- |
| 2025/10/14 | 0.1 | @张天毅 | 初稿 |
| 2025/10/22 | 1.0 | @张天毅 | 增加历史数据触发测试 |

## 4. 测试结论

测试通过

## 5. 测试环境

- OS: Windows, Linux, macOS

## 6. 功能测试

### 6.1 零状态参数

#### 6.1.1 测试要点

1. 状态列数据类型可以是int、bool和varchar类型，对这三种类型，零状态参数分别取正常值、字符串、异常值（列名、函数名、null、表达式等）
2. 零状态参数向状态列数据类型转换时规则与cast函数一致
3. 状态窗口出现在批查询，流计算触发和流计算子查询三个不同场景，源表可以是普通表、子表和超级表三类

#### 6.1.2 用例列表

| 测试用例 | 测试内容 | 测试语句 | 预期结果 | 测试结果 |
| --- | --- | --- | --- | --- |
| 异常值测试 | select _wstart, _wend, count(*) from ntb state_window(cint, 0, null) select _wstart, _wend, count(*) from ntb state_window(cint, 0, cint) select _wstart, _wend, count(*) from ntb state_window(cint, 0, cint+1) select _wstart, _wend, count(*) from ntb state_window(cint, 0, 1/1) select _wstart, _wend, count(*) from ntb state_window(cint, 0, 1.5) select _wstart, _wend, count(*) from ntb state_window(cbool, 0, null) | 异常报错 | PASS |
| 正常值测试 | select _wstart, _wend, count(*) from ntb/stb state_window(cint, 0, 0) select _wstart, _wend, count(*) from ntb/stb state_window(cint, 0, 1) select _wstart, _wend, count(*) from ntb/stb state_window(cint, 0, -1) select _wstart, _wend, count(*) from ntb/stb state_window(cbool, 0, true) select _wstart, _wend, count(*) from ntb/stb state_window(cbool, 0, false) select _wstart, _wend, count(*) from ntb/stb state_window(cstr, 0, "a") select _wstart, _wend, count(*) from ntb/stb state_window(cstr, 0, "A") | 结果正确 | PASS |
| 转换值测试 | select _wstart, _wend, count(*) from ntb state_window(cint, 0, "2") select _wstart, _wend, count(*) from ntb state_window(cint, 0, "A") select _wstart, _wend, count(*) from ntb state_window(cint, 0, true) select _wstart, _wend, count(*) from ntb state_window(cint, 0, "1.5") select _wstart, _wend, count(*) from ntb state_window(cint, 0, "100A") select _wstart, _wend, count(*) from ntb state_window(cbool, 0, "true") select _wstart, _wend, count(*) from ntb state_window(cbool, 0, "false") select _wstart, _wend, count(*) from ntb state_window(cbool, 0, 0) select _wstart, _wend, count(*) from ntb state_window(cbool, 0, 10) select _wstart, _wend, count(*) from ntb state_window(cstr, 0, 97) select _wstart, _wend, count(*) from ntb state_window(cstr, 0, true) select _wstart, _wend, count(*), cnchar from ntb state_window(cnchar, 0, '未知') | 结果正确 | PASS |
| check_zeroth_state_stream_compute | 流计算中计算子句功能测试 | create stream st1 count_window(1) from ctb1 into res_st1 as select _wstart, _wduration, _wend, count(*) cnt_all, sum(cfloat) sum_cfloat from ctb1 state_window(cint, 0, 1); create stream st2 count_window(1) from ctb1 into res_st2 as select _wstart, _wduration, _wend, count(*) cnt_all, sum(cfloat) sum_cfloat from ctb1 state_window(cbool, 0, false); create stream st3 count_window(1) from ctb1 into res_st3 as select _wstart, _wduration, _wend, count(*) cnt_all, sum(cfloat) sum_cfloat, cstr from ctb1 state_window(cstr, 0, 'b'); create stream st4 count_window(1) from stb partition by tbname into res_st4 as select _wstart, _wduration, _wend, count(*) cnt_all, sum(cfloat) sum_cfloat from %%tbname state_window(cint, 0, 2); create stream st5 count_window(1) from stb partition by tbname into res_st5 as select _wstart, _wduration, _wend, count(*) cnt_all, sum(cfloat) sum_cfloat from %%tbname state_window(cbool, 0, true); create stream st6 count_window(1) from stb partition by tbname into res_st6 as select _wstart, _wduration, _wend, count(*) cnt_all, sum(cfloat) sum_cfloat, cstr from %%tbname state_window(cstr, 0, 'a'); | 结果正确，与having子句一致 | PASS |
| check_zeroth_state_stream_trigger | 流计算中触发子句功能测试 | create stream st6 state_window(cint, 0, 3) from ctb1 into res_st6 as select _twstart wstart, _twduration wdur, _twend wend, count(*) cnt_all, sum(cfloat) sum_cfloat from %%trows; create stream st7 state_window(cbool, 0, true) from ctb1 into res_st7 as select _twstart wstart, _twduration wdur, _twend wend, count(*) cnt_all, sum(cfloat) sum_cfloat from %%trows; create stream st8 state_window(cstr, 0, 'c') from ctb1 into res_st8 as select _twstart wstart, _twduration wdur, _twend wend, count(*) cnt_all, sum(cfloat) sum_cfloat from %%trows; create stream st9 state_window(cint, 0, 1) from stb partition by tbname into res_st9 as select _twstart wstart, _twduration wdur, _twend wend, count(*) cnt_all, sum(cfloat) sum_cfloat from %%tbname where ts >= _twstart and ts <= _twend; create stream st10 state_window(cbool, 0, true) from stb partition by tbname into res_st10 as select _twstart wstart, _twduration wdur, _twend wend, count(*) cnt_all, sum(cfloat) sum_cfloat from %%tbname where ts >= _twstart and ts <= _twend; create stream st11 state_window(cstr, 0, 'd') from stb partition by tbname into res_st11 as select _twstart wstart, _twduration wdur, _twend wend, count(*) cnt_all, sum(cfloat) sum_cfloat from %%tbname where ts >= _twstart and ts <= _twend; | 结果正确，与having子句一致 | PASS |
| check_zeroth_state_stream_trigger_history | 流计算中触发子句历史计算功能测试 | create stream st12 state_window(cbool, 0, true) from ctb2 STREAM_OPTIONS(fill_history) into res_st12 as select _twstart wstart, _twduration wdur, _twend wend, count(*) cnt_all, sum(cfloat) sum_cfloat from %%trows; | 历史数据计算结果正确 | PASS |

## 7. 性能测试

无

## 8. 安全测试

无

## 9. 兼容性测试

兼容extend 指定窗口在开始结束时的扩展策略，可选值为0（默认值）、1、2，分别代表无扩展、向后扩展、向前扩展；TRUE_FOR 指定窗口最小持续时长，时间范围为正值，精度可选 1n、1u、1a、1s、1m、1h、1d、1w，如 TRUE_FOR(1a)。
