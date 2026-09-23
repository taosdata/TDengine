# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

import sys

import pytest

from new_test_framework.utils import tdLog, tdSql, tdStream


pytestmark = pytest.mark.skipif(
    sys.platform.startswith("win"),
    reason="natural stream windows require the POSIX timezone backend",
)


class TestStreamNaturalTimeUnits:
    DB = "stream_natural_units"
    CONFIG = "config(timezone='America/New_York', firstDayOfWeek=0)"
    CUTOFF = 1767243600000  # 2026-01-01 00:00:00 America/New_York
    SLIDING_CUTOFF = 1767225600000  # 2026-01-01 00:00:00 UTC

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.dropAllStreamsAndDbs()
        tdStream.createSnode()
        tdSql.prepare(dbname=cls.DB, drop=True, vgroups=1)
        tdSql.execute(f"use {cls.DB}")

    def _create_interval_stream(self, name, window):
        tdSql.execute(
            f"create stream {name} {window} from natural_src "
            f"stream_options({self.CONFIG}) into {name}_out as "
            "select _twstart as wstart, count(*) as cnt from natural_src "
            f"where ts >= _twstart and ts < _twend and ts < {self.CUTOFF}"
        )
        tdStream.checkStreamStatus(name)

    def _test_interval_query_parity(self):
        tdSql.execute("create table natural_src (ts timestamp, v int)")
        rows = (
            1735705800000,
            1736960400000,
            1739638800000,
            1741494600000,
            1741501800000,
            1741505400000,
            1741581000000,
            1744732800000,
            1747324800000,
            1750003200000,
            1752595200000,
            1755273600000,
            1757952000000,
            1760544000000,
            1762054200000,
            1762061400000,
            1762068600000,
            1762147800000,
            1765818000000,
        )
        values = ",".join(f"({timestamp}, {index})" for index, timestamp in enumerate(rows))

        cases = (
            ("s_int_1n", "interval(1n)"),
            ("s_int_1n_fixed", "interval(1n) sliding(7d)"),
            ("s_int_1n_offset", "interval(1n, 7d)"),
            ("s_int_1q", "interval(1q)"),
            ("s_int_1q_fixed", "interval(1q) sliding(7d)"),
            ("s_int_2q_offset", "interval(2q, 1n)"),
            ("s_int_1y", "interval(1y)"),
            ("s_int_1y_fixed", "interval(1y) sliding(7d)"),
        )
        for name, window in cases:
            self._create_interval_stream(name, window)

        tdSql.execute(f"insert into natural_src values {values},(1801458000000, 999)")
        tdSql.execute("set timezone 'America/New_York'")
        tdSql.execute("set first_day_of_week 0")

        for name, window in cases:
            tdSql.query(
                "select cast(_wstart as bigint), count(*) from natural_src "
                f"where ts < {self.CUTOFF} {window} order by _wstart"
            )
            expected = list(tdSql.queryResult)
            assert expected, f"ordinary query returned no rows for {window}"
            tdSql.checkResultsByFunc(
                sql=(
                    f"select cast(wstart as bigint), cnt from {name}_out "
                    "where cnt > 0 order by wstart"
                ),
                func=lambda expected=expected: list(tdSql.queryResult) == expected,
                retry=90,
            )

    def _test_independent_sliding_history(self):
        tdSql.execute("create table sliding_src (ts timestamp, v int)")
        tdSql.execute(
            "insert into sliding_src values "
            "(1735689600000, 1),"
            "(1738368000000, 2),"
            "(1740787200000, 3),"
            "(1743465600000, 4),"
            "(1751328000000, 5),"
            "(1767225600000, 6)"
        )
        cases = (
            ("s_slide_1n", "1n"),
            ("s_slide_1q", "1q"),
            ("s_slide_3n", "3n"),
            ("s_slide_1y", "1y"),
        )
        for name, duration in cases:
            tdSql.execute(
                f"create stream {name} sliding({duration}) from sliding_src "
                "stream_options(fill_history('2025-01-01 00:00:00') | "
                "config(timezone='UTC', firstDayOfWeek=0)) "
                f"into {name}_out as "
                "select _tprev_ts as prev_ts, _tcurrent_ts as current_ts, "
                "count(*) as cnt from sliding_src "
                "where ts > _tprev_ts and ts <= _tcurrent_ts"
            )
            tdStream.checkStreamStatus(name)

        expected_results = {
            "s_slide_1n": [
                (1733011200000, 1735689600000, 1),
                (1735689600000, 1738368000000, 1),
                (1738368000000, 1740787200000, 1),
                (1740787200000, 1743465600000, 1),
                (1743465600000, 1746057600000, 0),
                (1746057600000, 1748736000000, 0),
                (1748736000000, 1751328000000, 1),
                (1751328000000, 1754006400000, 0),
                (1754006400000, 1756684800000, 0),
                (1756684800000, 1759276800000, 0),
                (1759276800000, 1761955200000, 0),
                (1761955200000, 1764547200000, 0),
                (1764547200000, 1767225600000, 1),
            ],
            "s_slide_1q": [
                (1727740800000, 1735689600000, 1),
                (1735689600000, 1743465600000, 3),
                (1743465600000, 1751328000000, 1),
                (1751328000000, 1759276800000, 0),
                (1759276800000, 1767225600000, 1),
            ],
            "s_slide_3n": [
                (1727740800000, 1735689600000, 1),
                (1735689600000, 1743465600000, 3),
                (1743465600000, 1751328000000, 1),
                (1751328000000, 1759276800000, 0),
                (1759276800000, 1767225600000, 1),
            ],
            "s_slide_1y": [
                (1704067200000, 1735689600000, 1),
                (1735689600000, 1767225600000, 5),
            ],
        }
        for name, expected in expected_results.items():
            tdSql.checkResultsByFunc(
                sql=(
                    "select cast(prev_ts as bigint), "
                    "cast(current_ts as bigint), cnt "
                    f"from {name}_out where cast(current_ts as bigint) "
                    f"<= {self.SLIDING_CUTOFF} "
                    "order by current_ts"
                ),
                func=lambda expected=expected: list(tdSql.queryResult) == expected,
                retry=90,
            )

    def _test_weekly_first_day_of_week(self):
        tdSql.execute("create table week_src (ts timestamp, v int)")
        tdSql.execute(
            "insert into week_src values "
            "(1745107200000, 1),"
            "(1745193600000, 2),"
            "(1745712000000, 3),"
            "(1745798400000, 4)"
        )
        for fdow in (0, 1):
            name = f"s_week_{fdow}"
            tdSql.execute(
                f"create stream {name} sliding(1w) from week_src "
                "stream_options(fill_history('2025-04-19 00:00:00') | "
                f"config(timezone='UTC', firstDayOfWeek={fdow})) "
                f"into {name}_out as "
                "select _tcurrent_ts as current_ts, count(*) as cnt "
                "from week_src where ts > _tprev_ts and ts <= _tcurrent_ts"
            )
            tdStream.checkStreamStatus(name)

        tdSql.checkResultsByFunc(
            sql=(
                "select cast(current_ts as bigint), cnt "
                "from s_week_0_out order by current_ts"
            ),
            func=lambda: tdSql.getRows() >= 2,
            retry=90,
        )
        sunday_results = list(tdSql.queryResult)
        tdSql.checkResultsByFunc(
            sql=(
                "select cast(current_ts as bigint), cnt "
                "from s_week_1_out order by current_ts"
            ),
            func=lambda: tdSql.getRows() >= 2,
            retry=90,
        )
        monday_results = list(tdSql.queryResult)
        assert sunday_results != monday_results
        assert any(row[0] == 1745107200000 for row in sunday_results)
        assert any(row[0] == 1745193600000 for row in monday_results)

    def _test_stream_specific_fixed_sliding(self):
        tdSql.execute("create table gap_src (ts timestamp, v int)")
        tdSql.execute(
            "create stream s_gap interval(1s) sliding(2s) from gap_src "
            "stream_options(config(timezone='UTC', firstDayOfWeek=0)) "
            "into s_gap_out as "
            "select _twstart as wstart, count(*) as cnt from gap_src "
            "where ts >= _twstart and ts < _twend"
        )
        tdStream.checkStreamStatus("s_gap")
        tdSql.execute(
            "insert into gap_src values "
            "(1735689600250, 1),"
            "(1735689601250, 2),"
            "(1735689602250, 3),"
            "(1735689603250, 4),"
            "(1735689604000, 5)"
        )
        tdSql.checkResultsByFunc(
            sql=(
                "select cast(wstart as bigint), cnt from s_gap_out "
                "order by wstart"
            ),
            func=lambda: list(tdSql.queryResult)
            == [(1735689600000, 1), (1735689602000, 1)],
            retry=60,
        )

        tdSql.execute("create table threshold_src (ts timestamp, v int)")
        tdSql.execute(
            "create stream s_below_threshold interval(100s) sliding(500a) "
            "from threshold_src "
            "stream_options(config(timezone='UTC', firstDayOfWeek=0)) "
            "into s_below_threshold_out as "
            "select _twstart, count(*) from threshold_src "
            "where ts >= _twstart and ts < _twend"
        )
        tdStream.checkStreamStatus("s_below_threshold")
        tdSql.error(
            "select _wstart, count(*) from threshold_src "
            "interval(100s) sliding(500a)"
        )

    def _test_offset_and_sliding_validation(self):
        valid_triggers = (
            ("interval(1n, 27d)", "_twstart"),
            ("interval(1q, 83d)", "_twstart"),
            ("sliding(1n, 27d)", "_tcurrent_ts"),
            ("sliding(1q, 83d)", "_tcurrent_ts"),
            (
                "period(1q, 83d)",
                "cast(_tlocaltime/1000000 as timestamp)",
            ),
        )
        for index, (trigger, trigger_timestamp) in enumerate(valid_triggers):
            name = f"s_valid_offset_{index}"
            tdSql.execute(
                f"create stream {name} {trigger} from natural_src "
                "stream_options(config(timezone='UTC', firstDayOfWeek=0)) "
                f"into {name}_out as select {trigger_timestamp}, count(*) "
                "from natural_src"
            )
            tdStream.checkStreamStatus(name)
            tdSql.execute(f"drop stream {name}")

        invalid_triggers = (
            ("interval(1n, 28d)", "_twstart"),
            ("interval(1q, 84d)", "_twstart"),
            ("sliding(1n, 28d)", "_tcurrent_ts"),
            ("sliding(1q, 84d)", "_tcurrent_ts"),
            (
                "period(1q, 84d)",
                "cast(_tlocaltime/1000000 as timestamp)",
            ),
            ("interval(1n, auto)", "_twstart"),
            ("sliding(1n, auto)", "_tcurrent_ts"),
            (
                "period(1n, auto)",
                "cast(_tlocaltime/1000000 as timestamp)",
            ),
            ("interval(1n, -1d)", "_twstart"),
            ("sliding(1n, -1d)", "_tcurrent_ts"),
        )
        for index, (trigger, trigger_timestamp) in enumerate(invalid_triggers):
            tdSql.error(
                f"create stream s_invalid_offset_{index} {trigger} "
                "from natural_src into invalid_offset_out "
                f"as select {trigger_timestamp}, count(*) from natural_src"
            )

        for index, duration in enumerate(("1n", "1q", "1y")):
            tdSql.error(
                f"create stream s_invalid_sliding_{index} interval(1n) "
                f"sliding({duration}) from natural_src "
                "into invalid_sliding_out as select _twstart, count(*) "
                "from natural_src"
            )

    def test_stream_natural_time_units(self):
        """Natural-unit stream triggers use frozen query-compatible semantics.

        1. Compare INTERVAL n/q/y output and offset semantics with ordinary queries.
        2. Cover fixed sliding, DST data, independent SLIDING n/q/y, and fdow.
        3. Verify gap windows, the stream-specific 1% exception, and offsets.

        Catalog:
            - Streams:TriggerMode

        Since: v3.4.3.0

        Labels: common,ci,stream,integration,functional,timezone
        Feishu: None

        History:
            - 2026-09-04 OpenAI GPT-5 Created
        """
        self._test_interval_query_parity()
        self._test_independent_sliding_history()
        self._test_weekly_first_day_of_week()
        self._test_stream_specific_fixed_sliding()
        self._test_offset_and_sliding_validation()
