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
        lag, lead, irate, twa, elapsed)
- C类: UNION ALL + ORDER BY 非主键时间列
- D类: 窗口函数 (INTERVAL, SESSION, STATE_WINDOW, EVENT_WINDOW)
- E类: 边界情况 (多 TIMESTAMP 列、无 TIMESTAMP 列、嵌套子查询、NULL 值)
- F类: 向后兼容回归测试
- G类: 仍然报错的场景

Since:  v3.4.1.0
Labels: function, timeseries, timeline_fallback, subquery
Jira:   TS-XXXX
"""

import os
from new_test_framework.utils import tdLog, tdSql, tdCom


class TestTimelineFallback:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_timeline_fallback(self):
        """Timeline fallback: secondary timestamp column used when primary key unavailable.

        Catalog:
            - Functions/TimeSeries

        Since: v3.4.1.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-21 Created
        """
        case_dir = os.path.dirname(os.path.abspath(__file__))
        input_file = os.path.join(case_dir, "in", "test_timeline_fallback.in")
        expected_file = os.path.join(case_dir, "ans", "test_timeline_fallback.ans")
        tdCom.compare_testcase_result(
            input_file, expected_file, "test_timeline_fallback"
        )
