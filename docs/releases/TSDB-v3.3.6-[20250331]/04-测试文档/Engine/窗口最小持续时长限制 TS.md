# 窗口最小持续时长限制 TS

## 1. 测试目标

测试窗口最小持续时长限制的功能正确性和性能影响。

## 2. 变更历史

| 日期 | 版本 | 作者 | 备忘 |
| --- | --- | --- | --- |
| 2025-02-21 | 1.0 | 邝金清 | 初稿 |
|  |  |  |  |

## 3. 测试范围

1. 测试窗口最小持续时长限制的功能正确性，包括：
   - 窗口类型：条件窗口，状态窗口
   - 计算场景：流计算、查询语句
   - 相关边界情况
2. 测试对比带和不带最小持续时长限制时窗口查询的性能。

## 4. 测试结论

测试通过。

## 5. 已知问题和限制

1. true_for_duration 不能小于 0
2. true_for_duration 的时间单位不支持 n(月), y(年)

## 6. 测试环境

- OS: Ubuntu 24.02 LTS

## 7. 测试用例

### 7.1 功能

| 类型 | 测试目的 | 测试步骤 | 预期结果 | 测试结果 |
| --- | --- | --- | --- | --- |
| 正常用例 | 测试批量查询场景的事件窗口，使用的不同形式的 true_for 限制，同时测试边界值 | select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(3s);
select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(2999);
select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for('3001a'); | 计算结果正确 | Pass |
|  | 测试批量查询场景的状态窗口，使用的不同形式的 true_for 限制，同时测试边界值 | select _wstart, _wend, count(*) from ct_1 state_window(c1) true_for(3s);
select _wstart, _wend, count(*) from ct_1 state_window(c1) true_for(2999);
select _wstart, _wend, count(*) from ct_1 state_window(c1) true_for('3001a'); | 计算结果正确 | Pass |
|  | 测试流计算场景的事件窗口，使用的不同形式的 true_for 限制，同时测试边界值 | create stream s_event_1 into d_event_1 as select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(3s);
create stream s_event_3 into d_event_3 as select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(2999);
create stream s_event_5 into d_event_5 as select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for('3001a'); | 计算结果正确 | Pass |
|  | 测试流计算场景的状态窗口，使用的不同形式的 true_for 限制，同时测试边界值 | create stream s_state_1 into d_state_1 as select _wstart, _wend, count(*) from ct_1 state_window(c1) true_for (3s);
create stream s_state_3 into d_state_3 as select _wstart, _wend, count(*) from ct_1 state_window(c1) true_for (2999);
create stream s_state_5 into d_state_5 as select _wstart, _wend, count(*) from ct_1 state_window(c1) true_for ('3001a'); | 计算结果正确 | Pass |
|  | 测试流计算场景的事件窗口，更新数据，以触发窗口切分和窗口合并，测试对切分/合并产生的窗口依旧有效 | create stream s_event_2 ignore update 0 ignore expired 0 into d_event_2 as select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(3s);
create stream s_event_4 ignore update 0 ignore expired 0 into d_event_4 as select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(2999);
create stream s_event_6 ignore update 0 ignore expired 0 into d_event_6 as select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for('3001a');

insert into ct_0 values ('2025-01-01 00:00:00.000', 1), ('2025-01-01 00:00:22.000', 1), ('2025-01-01 00:00:28.000', -1);  触发多个窗口的切分与合并 | 计算结果正确 | Pass |
|  | 测试流计算场景的状态窗口，更新数据，以触发窗口切分和窗口合并，测试对切分/合并产生的窗口依旧有效 | create stream s_state_2 ignore update 0 ignore expired 0 into d_state_2 as select _wstart, _wend, count(*) from ct_1 state_window(c1) true_for (3s);
create stream s_state_4 ignore update 0 ignore expired 0 into d_state_4 as select _wstart, _wend, count(*) from ct_1 state_window(c1) true_for (2999);
create stream s_state_6 ignore update 0 ignore expired 0 into d_state_6 as select _wstart, _wend, count(*) from ct_1 state_window(c1) true_for ('3001a');

insert into ct_1 values ('2025-01-01 00:00:00.000', 1), ('2025-01-01 00:00:23.000', 6), ('2025-01-01 00:00:29.000', 8), ('2025-01-01 00:00:30.000', 8); 触发多个窗口的切分与合并 | 计算结果正确 | Pass |
| 异常用例 | 使用 n(月), y(年) 作为 true_for 的时间单位 | select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(3n);
select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(3y);
create stream s_ab into dst as select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(3n);
create stream s_ab into dst as select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(3y); | DB error: Cannot use 'year' or 'month' as true_for duration[0x80002688] | Pass |
|  | 使用负数作为 true_for 的持续时间 | select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(-1);
select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(-1a);
select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for('-1a');
create stream s_ab into dst as select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(-1);
create stream s_ab into dst as select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for(-1a);
create stream s_ab into dst as select _wstart, _wend, count(*) from ct_0 event_window start with c1 > 0 end with c1 < 0 true_for('-1a'); | DB error: syntax error near "-1);"[0x80002600] | Pass |

### 7.2 性能

#### 7.2.1 数据导入

<view type="2">

  > ⚠ 嵌入文件，需在飞书中查看 (token: CZ1lbMiUAo04HwxpPGecf8banRe)

</view>

#### 7.2.2 测试语句

1. Event Window:
   - Without True_For: `select _wstart, _wend, count(*) from st partition by tbname event_window start with c0 > 0 end with c0 < 0;`
   - With True_For: `select _wstart, _wend, count(*) from st partition by tbname event_window start with c0 > 0 end with c0 < 0 true_for(10a);`
2. State Window:
   - Without True_For: `select _wstart, _wend, count(*) from st partition by tbname state_window(c0);`
   - With True_For: `select _wstart, _wend, count(*) from st partition by tbname state_window(c0) true_for (3);`

#### 7.2.3 测试结果

|  | Without True_For | With True_For |
| --- | --- | --- |
| Event Window | 4.347612s | 4.399303s |
| State Window | 7.477480s | 6.654950s |

带和不带最小持续时长限制时窗口查询的性能基本相同。

## 8. 参考文档

- [支持为事件窗口设置最小持续时长 RS](https://taosdata.feishu.cn/wiki/TJ0Lw2tVdiPnEnk1HYtczVq8nYT)
- [窗口最小持续时长限制 FS](https://taosdata.feishu.cn/wiki/O7x9wvOA5ieZ3ckbKZIceZBun9e)
- 
  TS-5470
