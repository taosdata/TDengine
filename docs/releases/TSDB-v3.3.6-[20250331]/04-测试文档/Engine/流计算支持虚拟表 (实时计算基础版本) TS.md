# 流计算支持虚拟表 (实时计算基础版本) TS

## 1. 测试目标

JIRA：[TS-5467](https://jira.taosdata.com:18080/browse/TS-5467)
测试流计算使用虚拟表作为数据源的功能正确性。

## 2. 变更历史

| 日期 | 版本 | 作者 | 备忘 |
| --- | --- | --- | --- |
| 2025.03.21 | 0.1 | 邝金清 | 初稿 |
|  |  |  |  |

## 3. 测试结论

功能测试通过。

## 4. 已知问题和限制

由于工程实现难度，虚拟表的流计算有以下限制：
1. 流计算中涉及的虚拟普通表/虚拟子表的 schema 不允许更改。
2. 流计算过程中，如果修改虚拟表某一列对应的数据源，对流计算来说不生效。即：流计算仍只读取老的数据源。
3. 流计算过程中，如果虚拟表某一列对应的原始表被删除，之后新建了同名的表和同名的列，流计算不会读取新表的数据。
4. 流计算的 watermark 只能是 0，否则创建时就报错。

由于功能开发周期较短，2025-03-15 前只能完成基础版本。基础版本将引入以下短期限制：
1. 如果流计算的数据源是虚拟超级表，流计算任务启动后新增的子表不参与计算。
2. 虚拟表的不同原始表的时间戳不完全一致，数据合并后可能会产生空值，暂不支持插值处理(虚拟表 FS 中提到的 padding 函数)。
3. 暂不支持重算，即：不处理数据的乱序、更新或删除。(流创建时只能`ignore update 1 ignore expired 1`)
4. 暂不支持历史数据计算。(流创建时只能`fill_history 0`)
5. 暂不支持触发模式：MAX_DELAY, FORCE_WINDOW_CLOSE
6. 暂不支持窗口类型：COUNT_WINDOW

## 5. 测试环境

1. 硬件环境：
   - CPU：36C72T
   - 内存：126GB
2. 软件环境：
   - 操作系统：Ubuntu 24.04

## 6. 测试数据

建表语句：
<view type="2">

  > ⚠ 嵌入文件，需在飞书中查看 (token: HInrbFqiBonxeVxfIW0cSghWndf)

</view>

数据导入配置：
<view type="2">

  > ⚠ 嵌入文件，需在飞书中查看 (token: SjWMb7rf5o7AgLxwVmDcM1ovndf)

</view>

## 7. 测试用例

### 7.1 功能测试

本节用例的流程完全相同，区别仅在于第2步的建流语句和第5步的批量查询语句：
1. 使用上一节的建表语句，创建原始表和虚拟表
2. 创建流
3. 等待所有流计算任务 ready， 再使用 taosBenchmark 导入上一节的测试数据
4. 等待所有流计算任务 input queue 清零，意味着所有数据计算完成
5. 将目标表的数据，与同语句的批量查询结果做对比，期待两者完全一致
本节测试场景主要覆盖以下测试维度：
1. 虚拟表类型：
   - 虚拟超级表
   - 虚拟子表
   - 虚拟普通表
2. 查询类型：
   - 投影查询
   - 时间窗口
   - 事件窗口
   - 会话窗口
   - 状态窗口
| 类型 | 测试目的 | 建流语句（测试步骤见前文描述） | 预期结果 | 测试结果 |
| --- | --- | --- | --- | --- |
| 正常用例 | 测试虚拟普通表的投影查询 | 1. CREATE STREAM s_proj_1 TRIGGER AT_ONCE INTO dst_proj_1 AS select * from test_stream_vtable.vtb_virtual_ntb_full;
1. CREATE STREAM s_proj_2 TRIGGER AT_ONCE INTO dst_proj_2 AS select * from test_stream_vtable.vtb_virtual_ntb_half_full; | 流计算目标表数据与批量查询结果一致 | Pass |
|  | 测试虚拟超级表的投影查询 | 1. CREATE STREAM s_proj_3 TRIGGER AT_ONCE INTO dst_proj_3 AS select * from test_stream_vtable.vtb_virtual_stb PARTITION BY tbname; | 流计算目标表数据与批量查询结果一致 | Pass |
|  | 测试虚拟子表的投影查询 | 1. CREATE STREAM s_proj_4 TRIGGER AT_ONCE INTO dst_proj_4 AS select * from test_stream_vtable.vtb_virtual_ctb_full;
1. CREATE STREAM s_proj_5 TRIGGER AT_ONCE INTO dst_proj_5 AS select * from test_stream_vtable.vtb_virtual_ctb_half_full; | 流计算目标表数据与批量查询结果一致 | Pass |
|  | 测试虚拟普通表的时间窗口 | 1. CREATE STREAM s_interval_1 INTO dst_interval_1 AS select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ntb_full interval(1s);
1. CREATE STREAM s_interval_2 INTO dst_interval_2 AS select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ntb_half_full interval(1s) sliding(100a); | 流计算目标表数据与批量查询结果一致 | Pass |
|  | 测试虚拟超级表的时间窗口 | 1. CREATE STREAM s_interval_3 INTO dst_interval_3 AS select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_stb partition by tbname interval(1s) sliding(200a); | 流计算目标表数据与批量查询结果一致 | Pass |
|  | 测试虚拟子表的时间窗口 | 1. CREATE STREAM s_interval_4 INTO dst_interval_4 AS select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ctb_full interval(1s) sliding(100a);
1. CREATE STREAM s_interval_5 INTO dst_interval_5 AS select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ctb_half_full interval(1s); | 流计算目标表数据与批量查询结果一致 | Pass |
|  | 测试虚拟普通表的事件窗口 | 1. CREATE STREAM s_event_1 INTO dst_event_1 AS select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ntb_full event_window start with u_tinyint_col > 50 end with u_smallint_col > 10000;
1. CREATE STREAM s_event_2 INTO dst_event_2 AS select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ntb_half_full event_window start with u_tinyint_col > 50 end with u_smallint_col > 10000; | 流计算目标表数据与批量查询结果一致 | Pass |
|  | 测试虚拟超级表的事件窗口 | 1. CREATE STREAM s_event_3 INTO dst_event_3 AS select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_stb partition by tbname event_window start with u_tinyint_col > 50 end with u_smallint_col > 10000; | 流计算目标表数据与批量查询结果一致 | Pass |
|  | 测试虚拟子表的事件窗口 | 1. CREATE STREAM s_event_4 INTO dst_event_4 AS select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ctb_full event_window start with u_tinyint_col > 50 end with u_smallint_col > 10000;
1. CREATE STREAM s_event_5 INTO dst_event_5 AS select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ctb_half_full event_window start with u_tinyint_col > 50 end with u_smallint_col > 10000; | 流计算目标表数据与批量查询结果一致 | Pass |
|  | 测试虚拟普通表的会话窗口 | 1. CREATE STREAM s_session_1 INTO dst_session_1 AS select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ntb_full session(ts, 10a);
1. CREATE STREAM s_session_2 INTO dst_session_2 AS select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ntb_half_full session(ts, 10a); | 流计算目标表数据与批量查询结果一致 | Pass |
|  | 测试虚拟超级表的会话窗口 | 1. CREATE STREAM s_session_3 INTO dst_session_3 AS select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_stb partition by tbname session(ts, 10a); | 流计算目标表数据与批量查询结果一致 | Pass |
|  | 测试虚拟子表的会话窗口 | 1. CREATE STREAM s_session_4 INTO dst_session_4 AS select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ctb_full session(ts, 10a);
1. CREATE STREAM s_session_5 INTO dst_session_5 AS select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ctb_half_full session(ts, 10a); | 流计算目标表数据与批量查询结果一致 | Pass |
|  | 测试虚拟普通表的状态窗口 | 1. CREATE STREAM s_state_1 INTO dst_state_1 AS select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ntb_full state_window(bool_col);
1. CREATE STREAM s_state_2 INTO dst_state_2 AS select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ntb_half_full state_window(bool_col); | 流计算目标表数据与批量查询结果一致 | Pass |
|  | 测试虚拟超级表的状态窗口 | 1. CREATE STREAM s_state_3 INTO dst_state_3 AS select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_stb partition by tbname state_window(bool_col); | 流计算目标表数据与批量查询结果一致 | Pass |
|  | 测试虚拟子表的状态窗口 | 1. CREATE STREAM s_state_4 INTO dst_state_4 AS select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ctb_full state_window(bool_col);
1. CREATE STREAM s_state_5 INTO dst_state_5 AS select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ctb_half_full state_window(bool_col); | 流计算目标表数据与批量查询结果一致 | Pass |
| 异常用例 | 测试流任务处理数据乱序/更新/删除 | 1. CREATE STREAM s_interval_1 IGNORE EXPIRED 0 INTO dst_interval_1 AS select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ntb_full interval(1s);
1. CREATE STREAM s_interval_2 IGNORE UPDATE 0 INTO dst_interval_1 AS select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ntb_full interval(1s); | 1. 建流报错：For virtual table IGNORE EXPIRED must be 1
2. 建流报错：For virtual table IGNORE UPDATE must be 1 | Pass |
|  | 测试历史计算任务 | 1. CREATE STREAM s_interval_1 FILL_HISTORY 1 INTO dst_interval_1 AS select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ntb_full interval(1s); | 1. 建流报错：For virtual table FILL HISTORY must be 0 | Pass |
|  | 测试不支持的触发模式 | 1. CREATE STREAM s_interval_1 TRIGGER MAX_DELAY 1a INTO dst_interval_1 AS select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ntb_full interval(1s);
1. CREATE STREAM s_interval_1 TRIGGER FORCE_WINDOW_CLOSE INTO dst_interval_1 AS select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ntb_full interval(1s);
2. CREATE STREAM s_interval_1 TRIGGER CONTINUOUS_WINDOW_CLOSE INTO dst_interval_1 AS sele
ct _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtu
al_ntb_full interval(1s); | 1. 建流报错：Not supported virtual table stream query or trigger mode
1. 建流报错：Not supported virtual table stream query or trigger mode
2. 建流报错：Not supported virtual table stream query or trigger mode | Pass |
|  | 测试不支持的窗口类型 | 1. CREATE STREAM s_count_1 INTO dst_count_1 AS select _wstart, _wend, first(u_tinyint_col), last(tinyint_col) from test_stream_vtable.vtb_virtual_ntb_full count_window(20); | 1. 建流报错：Watermark of Count window must exceed 0 | Pass |

## 8. 测试计划

测试时间为 2025/03/17 - 2025/03/20

## 9. 参考文档

- 功能文档：[流计算支持虚拟表 (实时计算基础版本) FS](https://taosdata.feishu.cn/wiki/Wx12wLv77iQ3cVkXa5tc8JLRnRb)
- 设计文档：[流计算支持虚拟表 DS](https://taosdata.feishu.cn/wiki/MKcowKH6diriFOk7WYRcWVDOnrc)
- JIRA：
  TS-5467
