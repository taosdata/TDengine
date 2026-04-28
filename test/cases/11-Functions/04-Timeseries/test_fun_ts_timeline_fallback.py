###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

"""
Test: Timeline Fallback — 无主键时自动回退到第一个可用 TIMESTAMP 列

当输入 schema 中无主键时间列时（子查询、UNION ALL 等场景），
时间线函数和窗口函数自动回退到第一个可用的 TIMESTAMP 列。

覆盖范围:
- A类: 选择函数 (last, first, last_row, tail, unique)
- B类: 时间线函数 (diff, csum, derivative, mavg, statecount, stateduration,
        lag, lead, irate, twa, elapsed, fill_forward)
- C类: UNION ALL + ORDER BY 非主键时间列 (含 derivative, lag, lead, mavg,
        irate, twa, statecount, elapsed)
- D类: 窗口函数 (INTERVAL, SESSION, STATE_WINDOW, EVENT_WINDOW, COUNT_WINDOW)
- E类: 多 TIMESTAMP 列选择、别名
- F类: 嵌套子查询 (last, first, last_row, diff, csum, derivative, fill_forward,
        lag/lead, interval, session, event_window, count_window)
- G类: NULL 时间列
- H类: 向后兼容回归测试 (主键正常用例)
- I类: 无 TIMESTAMP 列 — last/first/last_row 按输入顺序返回
- J类: 无 TIMESTAMP 列报错 (diff, fill_forward, tail, unique 等仍需 TS)
- K类: 全 NULL 值边界
- L类: elapsed 类型检查
- M类: 第二时间列 create_time ORDER BY
- N类: 超级表 (STable) 时间线退化
- O类: PARTITION BY + 时间线退化 (普通表 grp 列、超级表 tbname,
        含 interval/session/count_window/state_window/event_window)
- P类: 大数据量 (10 行, 乱序 event_time)
- Q类: WHERE 过滤 + 时间线退化 (含 PARTITION BY)
- R类: 聚合管道 (interval 聚合结果作为子查询再做时间线函数)
- S类: 重复退化时间戳
- T类: 混合数据类型 (float, nchar)
- U类: LIMIT/OFFSET 子查询 + 时间线
- V类: 同一 SELECT 多时间线函数组合
- W类: 复杂表达式子查询 (val*2, CASE WHEN)
- X类: 子表 UNION ALL (超级表上下文)
- Y类: 深层嵌套 + PARTITION BY
- Z类: 边界 (单行表、空结果、无 TS + PARTITION BY)
- AA类: NULL 退化时间戳 — 选择函数 (last/first/last_row/tail)
- AB类: NULL 退化时间戳 — 时间线函数 (NULLs 视为重复时间戳报错)
- AC类: NULL 退化时间戳 — lag/lead/fill_forward (无重复检查)
- AD类: NULL 退化时间戳 — 窗口函数 (NULL→epoch 分窗)
- AE类: 全 NULL event_time 边界
- AF类: 倒序退化时间戳 — 选择函数
- AG类: 倒序退化时间戳 — diff 有/无 ORDER BY 对比
- AH类: 倒序退化时间戳 — csum/derivative/mavg/twa/irate/elapsed
- AI类: 倒序退化时间戳 — 窗口函数
- AJ类: 乱序退化时间戳 — 选择函数
- AK类: 乱序退化时间戳 — diff 有/无 ORDER BY 对比
- AL类: 乱序退化时间戳 — 多时间线函数
- AM类: 乱序退化时间戳 — 窗口函数
- AN类: 乱序退化时间戳 — lag/lead
- AO类: NULL 退化时间戳 + PARTITION BY
- AP类: 倒序退化时间戳 + PARTITION BY
- AQ类: 倒序退化时间戳 — statecount/stateduration/fill_forward
- AR类: NULL 退化时间戳 — 无 TS 子查询 (last/first/last_row)

Since:  v3.4.1.0
Labels: function, timeseries, timeline_fallback, subquery
Jira:   TS-5791
"""

import os
from new_test_framework.utils import tdLog, tdSql, tdCom


class TestTimelineFallback:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_timeline_fallback(self):
        """Timeline fallback: secondary timestamp column used when primary key unavailable.

        When a subquery has no primary key (_rowts), the engine falls back to the first
        available TIMESTAMP column for timeline/window functions (diff, csum, elapsed,
        INTERVAL, SESSION, etc.). Covers selection functions, timeline functions,
        UNION ALL with ORDER BY, window functions, and error cases.

        Catalog:
            - Functions/TimeSeries

        Since: v3.4.1.0

        Labels: common,ci

        Jira: TS-5791

        History:
            - 2026-04-21 Created
        """
        case_dir = os.path.dirname(os.path.abspath(__file__))
        input_file = os.path.join(case_dir, "in", "test_timeline_fallback.in")
        expected_file = os.path.join(case_dir, "ans", "test_timeline_fallback.ans")
        tdCom.compare_testcase_result(
            input_file, expected_file, "test_timeline_fallback"
        )

    def test_primary_key_matrix(self):
        """Primary key behavior matrix: verify original PK and degraded TS behavior under all scenarios.

        Covers all cells in the 4.4 behavior matrix:
        - Selection functions, elapsed, derivative/twa/irate, diff/csum/mavg/statecount/stateduration,
          lag/lead/fill_forward, INTERVAL/SESSION/STATE_WINDOW/EVENT_WINDOW/COUNT_WINDOW, interp
        - Scenarios: ascending, descending, random order, duplicate timestamps, NULL timestamps
        - Both original primary key and degraded timestamp paths

        Catalog:
            - Functions/TimeSeries

        Since: v3.4.1.0

        Labels: common,ci

        Jira: TS-5791

        History:
            - 2026-04-28 Created — baseline behavior matrix coverage
        """
        case_dir = os.path.dirname(os.path.abspath(__file__))
        input_file = os.path.join(case_dir, "in", "test_primary_key_matrix.in")
        expected_file = os.path.join(case_dir, "ans", "test_primary_key_matrix.ans")
        tdCom.compare_testcase_result(
            input_file, expected_file, "test_primary_key_matrix"
        )
