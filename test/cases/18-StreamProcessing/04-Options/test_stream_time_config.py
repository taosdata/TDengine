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

import os
import sys
import time

import pytest

from new_test_framework.utils import clusterComCheck, sc, tdLog, tdSql, tdStream


pytestmark = pytest.mark.skipif(
    sys.platform.startswith("win"),
    reason="stream timezone persistence requires the POSIX timezone backend",
)


class TestStreamTimeConfig:
    DB = "stream_time_config"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.dropAllStreamsAndDbs()
        tdStream.createSnode()
        tdSql.prepare(dbname=cls.DB, drop=True, vgroups=1)
        tdSql.execute(f"use {cls.DB}")
        tdSql.execute("create table src (ts timestamp, v int)")

    def _create_stream(self, name, options):
        option_clause = f" stream_options({options})" if options else ""
        tdSql.execute(
            f"create stream {name} interval(1w) from src{option_clause} "
            f"into {name}_out as "
            "select _twstart as wstart, count(*) as cnt, "
            "timezone() as tz, first_day_of_week() as fdow, "
            "cast('2025-04-27 00:00:00' as timestamp) as literal_ts, "
            "timetruncate('2025-04-30 12:00:00', 1w) as week_start "
            "from src where ts >= _twstart and ts < _twend"
        )
        tdStream.checkStreamStatus(name)

    def _check_metadata(self, name, timezone, first_day_of_week):
        tdSql.query(
            "select `timezone`, `first_day_of_week`, `sql` "
            "from information_schema.ins_streams "
            f"where stream_name='{name}'"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, timezone)
        tdSql.checkData(0, 1, first_day_of_week)
        sql = tdSql.getData(0, 2)
        assert sql.lower().count("config(") == 1, sql
        expected = (
            f"CONFIG(timezone = '{timezone}', "
            f"firstDayOfWeek = {first_day_of_week})"
        )
        assert expected in sql, sql

    def _drop_stream(self, name):
        tdSql.execute(f"drop stream {name}")

    def test_stream_time_config(self):
        """Frozen stream time CONFIG survives normalization and lifecycle.

        1. Verify defaulted, one-key, duplicate, and invalid CONFIG forms.
        2. Verify persisted metadata and normalized exported CREATE SQL.
        3. Change session, client-global, server, and process timezone defaults.
        4. STOP/START and restart taosd, then verify frozen window semantics.

        Catalog:
            - Streams:Options

        Since: v3.4.3.0

        Labels: common,ci,stream,integration,functional,timezone
        Feishu: None

        History:
            - 2026-09-04 OpenAI GPT-5 Created
        """
        tdSql.execute("set timezone 'UTC'")
        tdSql.execute("set first_day_of_week 1")

        self._create_stream("s_default", "")
        self._check_metadata("s_default", "UTC", 1)
        self._drop_stream("s_default")

        self._create_stream("s_timezone_only", "config(timezone='+08:00')")
        self._check_metadata("s_timezone_only", "UTC+8", 1)
        self._drop_stream("s_timezone_only")

        self._create_stream("s_fdow_only", "config(firstDayOfWeek=6)")
        self._check_metadata("s_fdow_only", "UTC", 6)
        self._drop_stream("s_fdow_only")

        self._create_stream(
            "s_block_duplicate",
            "config(timezone='UTC', TIMEZONE='Asia/Shanghai', "
            "firstDayOfWeek=1, FIRSTDAYOFWEEK=0)",
        )
        self._check_metadata("s_block_duplicate", "Asia/Shanghai", 0)
        self._drop_stream("s_block_duplicate")

        self._create_stream(
            "s_cfg",
            "config(timezone='Invalid/Zone', firstDayOfWeek=8) | "
            "CoNfIg(TIMEZONE='Asia/Shanghai', FIRSTDAYOFWEEK=0)",
        )
        self._check_metadata("s_cfg", "Asia/Shanghai", 0)

        invalid_statements = (
            "create stream bad_tz interval(1w) from src "
            "stream_options(config(timezone='Invalid/Zone')) "
            "into bad_tz_out as select _twstart, count(*) from src",
            "create stream bad_fdow interval(1w) from src "
            "stream_options(config(firstDayOfWeek=7)) "
            "into bad_fdow_out as select _twstart, count(*) from src",
            "create stream bad_unknown interval(1w) from src "
            "stream_options(config(tz='UTC')) "
            "into bad_unknown_out as select _twstart, count(*) from src",
            "create stream bad_empty interval(1w) from src "
            "stream_options(config()) "
            "into bad_empty_out as select _twstart, count(*) from src",
            "create stream bad_tz_type interval(1w) from src "
            "stream_options(config(timezone=1)) "
            "into bad_tz_type_out as select _twstart, count(*) from src",
            "create stream bad_fdow_type interval(1w) from src "
            "stream_options(config(firstDayOfWeek='1')) "
            "into bad_fdow_type_out as select _twstart, count(*) from src",
        )
        for statement in invalid_statements:
            tdSql.error(statement)
        # the rejected value is reported instead of a bare '%s' placeholder
        # (tdSql strips single quotes from the error text)
        tdSql.error(invalid_statements[0], expectedErrno=0x26B2,
                    expectErrInfo="Invalid timezone: Invalid/Zone",
                    fullMatched=False)

        previous_process_timezone = os.environ.get("TZ")
        try:
            os.environ["TZ"] = "America/New_York"
            if hasattr(time, "tzset"):
                time.tzset()

            tdSql.execute("alter local 'timezone America/New_York'")
            tdSql.execute("alter local 'firstDayOfWeek' '6'")
            tdSql.execute("alter all dnodes 'timezone America/New_York'")
            tdSql.execute("set timezone 'America/New_York'")
            tdSql.execute("set first_day_of_week 6")

            tdSql.execute("stop stream s_cfg")
            tdStream.waitStreamStatus(self.DB, "s_cfg", "Stopped")
            tdSql.execute("start stream s_cfg")
            tdStream.waitStreamStatus(self.DB, "s_cfg", "Running")

            sc.dnodeStop(1)
            sc.dnodeStart(1)
            clusterComCheck.checkDnodes(1)
            tdSql.connect()
            tdSql.execute(f"use {self.DB}")
            tdStream.checkStreamStatus("s_cfg")
            self._check_metadata("s_cfg", "Asia/Shanghai", 0)

            tdSql.execute(
                "insert into src values "
                "(1745679600000, 1),"
                "(1745686800000, 2),"
                "(1746288000001, 3)"
            )

            def frozen_result_matches():
                if tdSql.getRows() != 2:
                    return False
                expected = (
                    (1745078400000, 1),
                    (1745683200000, 1),
                )
                for row, (wstart, count) in enumerate(expected):
                    if tdSql.getData(row, 0) != wstart:
                        return False
                    if tdSql.getData(row, 1) != count:
                        return False
                    if "Asia/Shanghai" not in tdSql.getData(row, 2):
                        return False
                    if tdSql.getData(row, 3) != 0:
                        return False
                    if tdSql.getData(row, 4) != 1745683200000:
                        return False
                    if tdSql.getData(row, 5) != 1745683200000:
                        return False
                return True

            tdSql.checkResultsByFunc(
                sql=(
                    "select cast(wstart as bigint), cnt, tz, fdow, "
                    "cast(literal_ts as bigint), cast(week_start as bigint) "
                    "from s_cfg_out order by wstart"
                ),
                func=frozen_result_matches,
                retry=60,
            )
        finally:
            if previous_process_timezone is None:
                os.environ.pop("TZ", None)
            else:
                os.environ["TZ"] = previous_process_timezone
            if hasattr(time, "tzset"):
                time.tzset()
