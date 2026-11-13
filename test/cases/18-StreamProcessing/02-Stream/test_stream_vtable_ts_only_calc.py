# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import time

from new_test_framework.utils import tdCom, tdLog, tdSql, tdStream


class TestStreamVtableTsOnlyCalc:
    precision = "ms"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def _prepare(self, db, src_db):
        tdCom.create_snode_if_not_exists()
        tdSql.execute(f"create database if not exists {src_db} vgroups 1")
        tdSql.execute(f"create database if not exists {db} vgroups 1")
        tdSql.execute(
            f"create stable if not exists {src_db}.meters "
            "(ts timestamp, `当前有故障码` int, `温度` int, `转速` int) "
            "tags (`产品型号Tag` varchar(32))"
        )
        tdSql.execute(
            f"create table if not exists {src_db}.dev01 using {src_db}.meters "
            "tags ('2Qe7C9')"
        )
        tdSql.execute(
            f"create stable if not exists {db}.vst "
            "(ts timestamp, `当前有故障码` int, `温度` int, `转速` int) "
            "tags (`产品型号Tag` varchar(32)) virtual 1"
        )
        tdSql.execute(
            f"create vtable if not exists {db}.vt01 "
            "(`当前有故障码` from "
            f"{src_db}.dev01.`当前有故障码`, "
            f"`温度` from {src_db}.dev01.`温度`, "
            f"`转速` from {src_db}.dev01.`转速`) "
            f"using {db}.vst tags (from {src_db}.dev01.`产品型号Tag`)"
        )

    def _drop(self, db, src_db):
        tdSql.execute(f"drop database if exists {db} force")
        tdSql.execute(f"drop database if exists {src_db} force")

    def _wait_ready(self, stream, timeout=90):
        """Wait until the trigger is running and its reader tasks are deployed.

        The trigger readers of a virtual-table stream are deployed asynchronously
        after the stream reports Running; rows written before that are not seen by
        the window, so the test must not insert too early.
        """
        tdStream.checkStreamStatus(stream)
        deadline = time.time() + timeout
        while time.time() < deadline:
            tdSql.query(
                "select count(*) from information_schema.ins_stream_tasks "
                f"where stream_name='{stream}' and `type`='Reader'"
            )
            if int(tdSql.getData(0, 0)) > 0:
                return
            time.sleep(1)
        tdLog.exit(f"stream reader tasks not deployed in {timeout}s")

    def test_event_window_count_star_keeps_stream_running(self):
        """Stream: event window over virtual super table with count(*) only

        The calculation projects nothing but the window boundaries, a constant
        and count(*), so the scan plan only references the primary timestamp
        while the dynamic scanner maps every virtual column. Each closed window
        must still be calculated instead of failing the Reader task with
        "Virtual table source column not found" and disabling the stream.

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.3.1

        Labels: common,ci,stream,vtable

        History:
            - 2026-10-29 Created for the ts-only calc column mapping failure.
        """
        db = "stream_vtb_ts_only"
        src_db = "stream_vtb_ts_only_src"
        stream = "s_ts_only"
        try:
            self._prepare(db, src_db)
            tdSql.execute(
                f"create stream {db}.{stream} "
                "event_window(start with `当前有故障码` = 1 "
                "end with `当前有故障码` = 0) "
                f"from {db}.vst partition by tbname "
                "stream_options(ignore_disorder) "
                f"into {db}.out nodelay_create_subtable "
                "output_subtable(concat('out_', tbname)) "
                "as select _twstart as output_timestamp, "
                "1 as `系统故障_status`, "
                "_twend as `系统故障_end_ts`, "
                "count(*) as `系统故障_obs_count` "
                "from %%tbname where _c0 >= _twstart and _c0 < _twend",
                queryTimes=1,
            )
            self._wait_ready(stream)

            ts = int(time.time() * 1000)
            tdSql.execute(
                f"insert into {src_db}.dev01 values "
                f"({ts}, 1, 20, 100) ({ts + 1000}, 1, 21, 101) "
                f"({ts + 2000}, 1, 22, 102) ({ts + 3000}, 0, 20, 100)"
            )
            tdSql.checkResultsByArray(
                "select cast(output_timestamp as bigint), "
                "`系统故障_status`, cast(`系统故障_end_ts` as bigint), "
                f"`系统故障_obs_count` from {db}.out_vt01",
                [(ts, 1, ts + 3000, 3)],
                retry=60,
            )

            # A second closed window must be calculated as well: the failure used to
            # disable the whole stream after the first window.
            tdSql.execute(
                f"insert into {src_db}.dev01 values "
                f"({ts + 10000}, 1, 20, 100) ({ts + 11000}, 1, 21, 101) "
                f"({ts + 12000}, 0, 22, 102)"
            )
            tdSql.checkResultsByArray(
                "select cast(output_timestamp as bigint), "
                "`系统故障_status`, cast(`系统故障_end_ts` as bigint), "
                f"`系统故障_obs_count` from {db}.out_vt01 order by output_timestamp",
                [(ts, 1, ts + 3000, 3), (ts + 10000, 1, ts + 12000, 2)],
                retry=60,
            )
            tdStream.checkStreamStatus(stream)
        finally:
            self._drop(db, src_db)

    def test_event_window_with_data_column_calc(self):
        """Stream: event window over virtual super table referencing a data column

        Same stream shape, but the calculation also aggregates a regular data
        column so the scan plan requests more than the primary timestamp. This
        guards the mapped-column subset logic from over-pruning.

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.3.1

        Labels: common,ci,stream,vtable

        History:
            - 2026-10-29 Created together with the ts-only calc regression.
        """
        db = "stream_vtb_ts_col"
        src_db = "stream_vtb_ts_col_src"
        stream = "s_ts_col"
        try:
            self._prepare(db, src_db)
            tdSql.execute(
                f"create stream {db}.{stream} "
                "event_window(start with `当前有故障码` = 1 "
                "end with `当前有故障码` = 0) "
                f"from {db}.vst partition by tbname "
                "stream_options(ignore_disorder) "
                f"into {db}.out nodelay_create_subtable "
                "output_subtable(concat('out_', tbname)) "
                "as select _twstart as output_timestamp, "
                "1 as `系统故障_status`, "
                "_twend as `系统故障_end_ts`, "
                "count(`当前有故障码`) as `系统故障_obs_count`, "
                "max(`温度`) as `系统故障_max_temp` "
                "from %%tbname where _c0 >= _twstart and _c0 < _twend",
                queryTimes=1,
            )
            self._wait_ready(stream)

            ts = int(time.time() * 1000)
            tdSql.execute(
                f"insert into {src_db}.dev01 values "
                f"({ts}, 1, 20, 100) ({ts + 1000}, 1, 25, 101) "
                f"({ts + 2000}, 0, 22, 102)"
            )
            tdSql.checkResultsByArray(
                "select cast(output_timestamp as bigint), "
                "`系统故障_status`, cast(`系统故障_end_ts` as bigint), "
                f"`系统故障_obs_count`, `系统故障_max_temp` from {db}.out_vt01",
                [(ts, 1, ts + 2000, 2, 25)],
                retry=60,
            )
            tdStream.checkStreamStatus(stream)
        finally:
            self._drop(db, src_db)
