# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

from new_test_framework.utils import tdLog, tdSql


class TestCaseWhenConcatCastLen:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def prepare_data(self):
        tdSql.prepare("db_concat_cast", drop=True)
        tdSql.execute("create table t_in (ts timestamp, temp double, lim1 double, lim2 double)")
        tdSql.execute("create table t_out (ts timestamp, temp double, lim1 double, lim2 double)")
        tdSql.execute("insert into t_in values (now(), 10.5, 8, 12)")
        tdSql.execute("insert into t_out values (now(), 25.5, 8, 12)")

    def check_case_when_concat(self):
        # Regression: an unbounded CAST(... AS VARCHAR) declares the maximum field width, so a
        # CONCAT containing one or more of them used to exceed TSDB_MAX_FIELD_LEN. Wrapping the
        # CONCAT in CASE WHEN made the translation fail with:
        #   CAST function converted length should be in range (0, 65517] bytes [0x2800]
        # Out of range: the THEN branch is returned.
        tdSql.query(
            "select case when last(temp) < last(lim1) or last(temp) > last(lim2) then 'dddd' else "
            "concat('temp=', cast(last(temp) as varchar), ', range[', cast(last(lim1) as varchar), ',', "
            "cast(last(lim2) as varchar), ']') end as v from t_out"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "dddd")

        # In range: the ELSE branch, which carries the oversized CONCAT, is returned.
        tdSql.query(
            "select case when last(temp) < last(lim1) or last(temp) > last(lim2) then 'dddd' else "
            "concat('temp=', cast(last(temp) as varchar), ', range[', cast(last(lim1) as varchar), ',', "
            "cast(last(lim2) as varchar), ']') end as v from t_in"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "temp=10.5, range[8,12]")

        # Multiple THEN branches are unified with the same CONCAT type.
        tdSql.query(
            "select case when last(temp) > last(lim2) then 'high' when last(temp) < last(lim1) then 'low' "
            "else concat('temp=', cast(last(temp) as varchar), ', range[', cast(last(lim1) as varchar), ',', "
            "cast(last(lim2) as varchar), ']') end as v from t_out"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "high")

    def check_case_when_cast_variants(self):
        # A single unbounded CAST is enough to overflow the old summed CONCAT length.
        tdSql.query(
            "select case when last(temp) < last(lim1) then 'dddd' else "
            "concat('a=', cast(last(temp) as varchar)) end as v from t_out"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "a=25.5")

        # CAST without CONCAT was already valid and must keep its result.
        tdSql.query(
            "select case when last(temp) < last(lim1) then 'dddd' else cast(last(temp) as varchar) end as v "
            "from t_out"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "25.5")

        # An explicitly sized CAST keeps the same result.
        tdSql.query(
            "select case when last(temp) < last(lim1) then 'dddd' else "
            "concat('a=', cast(last(temp) as varchar(32))) end as v from t_out"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "a=25.5")

    def check_case_when_concat_ws(self):
        # concat_ws shares translateConcatImpl with concat.
        tdSql.query(
            "select case when last(temp) < last(lim1) then 'dddd' else "
            "concat_ws(',', 'temp=', cast(last(temp) as varchar), cast(last(lim1) as varchar)) end as v "
            "from t_out"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "temp=,25.5,8")

    def check_case_when_concat_nchar(self):
        # NCHAR results are capped on a UCS4 boundary and must also pass CASE type unification.
        tdSql.query(
            "select case when last(temp) < last(lim1) or last(temp) > last(lim2) then 'dddd' else "
            "concat('temp=', cast(last(temp) as nchar)) end as v from t_in"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "temp=10.5")

        # The THEN branch is implicitly cast to NCHAR here.
        tdSql.query(
            "select case when last(temp) < last(lim1) or last(temp) > last(lim2) then 'dddd' else "
            "concat('temp=', cast(last(temp) as nchar)) end as v from t_out"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "dddd")

    def check_concat_without_case_when(self):
        # Same CONCAT without CASE WHEN keeps working.
        tdSql.query(
            "select concat('temp=', cast(last(temp) as varchar), ', range[', cast(last(lim1) as varchar), ',', "
            "cast(last(lim2) as varchar), ']') as v from t_out"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "temp=25.5, range[8,12]")

    def check_concat_overflow_still_errors(self):
        # Saturating the declared length must not silently truncate: a result that really does
        # not fit into one field still fails at execution time.
        tdSql.error("select concat(repeat('a', 33000), repeat('b', 33000)) from t_out")

    def test_case_when_concat_cast_len(self):
        """Case when with concat of unbounded cast

        1. CASE WHEN over CONCAT containing unbounded CAST(... AS VARCHAR)
        2. CONCAT with one or more max-width CAST parameters inside CASE WHEN
        3. Explicitly sized CAST and CAST without CONCAT variants
        4. CONCAT_WS and NCHAR result types
        5. Same CONCAT outside CASE WHEN
        6. Genuinely oversized CONCAT still reports an error

        Since: v3.4.3.10

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-18 Fixed CASE WHEN branch unification rejecting CONCAT result lengths above
              TSDB_MAX_FIELD_LEN (0x2800 CAST function converted length should be in range)
        """
        self.prepare_data()
        self.check_case_when_concat()
        self.check_case_when_cast_variants()
        self.check_case_when_concat_ws()
        self.check_case_when_concat_nchar()
        self.check_concat_without_case_when()
        self.check_concat_overflow_still_errors()
