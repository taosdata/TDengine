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

import os
from new_test_framework.utils import tdLog, tdSql, tdCom


class TestFunTsIndefWithSelect:
    """Test indefinite-row functions used together with selection functions."""

    db = "test_indef_sel"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        # Prevent ASAN ODR violation when taos client is invoked via os.system()
        os.environ.setdefault("ASAN_OPTIONS", "detect_odr_violation=0")

    def teardown_class(cls):
        tdSql.execute("drop database if exists test_indef_sel")

    def test_indef_with_select(self):
        """Indefinite-row functions mixed with selection functions.

        Indefinite-row functions execute after selection functions.
        The selection function reduces to a single row first, so the
        indefinite-row function operates on just that one row.

        Catalog:
            - Function:Timeseries

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-21 Created

        """

        self.db = "test_indef_sel"

        # positive cases via file comparison (data setup included in .in file)
        sqlFile = os.path.join(os.path.dirname(__file__),
                               "in", "test_fun_ts_indef_with_select.in")
        ansFile = os.path.join(os.path.dirname(__file__),
                               "ans", "test_fun_ts_indef_with_select.ans")
        tdCom.compare_testcase_result(sqlFile, ansFile,
                                      "test_fun_ts_indef_with_select")

        # negative: non-select agg funcs still blocked with indef-row funcs
        table = f"{self.db}.ctb1"
        for agg in ["count", "sum", "avg"]:
            for func in ["diff(c1)", "csum(c1)"]:
                tdSql.error(f"select {agg}(c1), {func} from {table}")

        # negative: top/bottom/sample (MULTI_ROWS) + indef should error
        for multi in ["top(c1, 3)", "bottom(c1, 3)", "sample(c1, 3)"]:
            for func in ["diff(c1)", "csum(c1)", "fill_forward(c1)"]:
                tdSql.error(f"select {multi}, {func} from {table}")

        # negative: apercentile (AGG only, no SELECT flag) + indef should error
        for func in ["diff(c1)", "csum(c1)"]:
            tdSql.error(f"select apercentile(c1, 50), {func} from {table}")

        # negative: indef functions with different return row counts should error
        tdSql.error(f"select diff(c1), csum(c1) from {table}")
        tdSql.error(f"select diff(c1), fill_forward(c1) from {table}")

        # negative: group by + select + indef should error
        tdSql.error(f"select first(c1), diff(c1) from {self.db}.stb group by tbname")

        # negative: multiple selection funcs + indef-row func should error
        # With multiple selection funcs the column value is ambiguous for indef
        for indef in ["diff(c1)", "csum(c1)", "fill_forward(c1)", "lag(c1,1)",
                       "lead(c1,1)", "statecount(c1,'GE',0)", "stateduration(c1,'GE',0,1s)",
                       "unique(c1)", "tail(c1,3)", "mavg(c1,2)"]:
            tdSql.error(f"select first(c1), last(c1), {indef} from {table}")
        # two select funcs of same type
        tdSql.error(f"select first(c1), first(c2), diff(c1) from {table}")
        # three+ select funcs
        tdSql.error(f"select first(c1), last(c1), min(c1), diff(c1) from {table}")
        tdSql.error(f"select first(c1), last(c1), min(c1), max(c1), csum(c1) from {table}")
        # mixed select func types with various indef funcs
        tdSql.error(f"select first(c1), mode(c1), lag(c1,1) from {table}")
        tdSql.error(f"select first(c1), last_row(c1), unique(c1) from {table}")
        tdSql.error(f"select first(c1), max(c3), fill_forward(c1), fill_forward(c3) from {table}")

        # partition by tbname: non-deterministic ordering, check row counts
        stb = f"{self.db}.stb"
        tdSql.query(f"select first(c1), csum(c1) from {stb} partition by tbname")
        # ctb1(1 row) + ctb2(1 row) + ctb_null(1 row) + ctb_single(1 row) = 4
        assert tdSql.queryRows == 4, f"expected 4 rows, got {tdSql.queryRows}"

        tdSql.query(f"select first(c1), fill_forward(c1) from {stb} partition by tbname")
        assert tdSql.queryRows == 4, f"expected 4 rows, got {tdSql.queryRows}"

        tdSql.query(f"select first(c1), lag(c1, 1) from {stb} partition by tbname")
        assert tdSql.queryRows == 4, f"expected 4 rows, got {tdSql.queryRows}"

        # union all: non-deterministic ordering, check row counts
        tdSql.query(f"select first(c1), csum(c1) from {self.db}.ctb1 union all select first(c1), csum(c1) from {self.db}.ctb2")
        assert tdSql.queryRows == 2, f"expected 2 rows, got {tdSql.queryRows}"

        # supertable without partition by: check runs without error
        tdSql.query(f"select first(c1), csum(c1) from {stb}")
        assert tdSql.queryRows == 1, f"expected 1 row, got {tdSql.queryRows}"
        tdSql.query(f"select first(c1), fill_forward(c1) from {stb}")
        assert tdSql.queryRows == 1, f"expected 1 row, got {tdSql.queryRows}"

        # mavg on single row returns non-deterministic value (pre-existing),
        # so verify it runs without error rather than comparing output
        for sel in ["first", "last", "min", "max", "mode", "last_row"]:
            tdSql.query(f"select {sel}(c1), mavg(c1, 2) from {table}")
