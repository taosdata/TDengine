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

import time

import pytest

from new_test_framework.utils import tdCom, tdLog, tdSql, tdStream


class TestStreamVtableTagRefRefresh:
    precision = "ms"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    @pytest.mark.parametrize("table_kind", ["child", "normal"])
    @pytest.mark.parametrize("fill_history", [True, False])
    def test_explicit_vtable_tag_ref_calculation(self, table_kind, fill_history):
        """Explicit virtual-table calculations resolve referenced tags.

        Create an event-window stream over a virtual child or normal table,
        aggregate a data column and two tags from the same source, and check
        historical and realtime window results.

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.3.0

        Labels: common,ci,stream,vtable
        Feishu: None

        History:
            - 2026-10-10 Added regression for missing calc tag-ref sources.
        """
        db = "stream_tagref_calc"
        src_db = "stream_tagref_calc_src"
        vtable = f"{db}.`1号熔化炉 FM-01`"
        tdCom.create_snode_if_not_exists()
        try:
            tdSql.execute(f"create database {src_db} vgroups 1")
            tdSql.execute(f"create database {db} vgroups 1")
            tdSql.execute(
                f"create stable {src_db}.meters "
                "(ts timestamp, status int, card_no varchar(32)) "
                "tags (deviceCode varchar(32), namingPattern varchar(32))"
            )
            tdSql.execute(
                f"create table {src_db}.fm01 using {src_db}.meters "
                "tags ('FM-01', 'melting')"
            )
            if table_kind == "child":
                tdSql.execute(
                    f"create stable {db}.vst "
                    "(ts timestamp, `运行状态` int, `配料卡号` varchar(32)) "
                    "tags (`设备号` varchar(32), `命名模式` varchar(32)) virtual 1"
                )
                tdSql.execute(
                    f"create vtable {vtable} "
                    f"(`运行状态` from {src_db}.fm01.status, "
                    f"`配料卡号` from {src_db}.fm01.card_no) using {db}.vst "
                    f"tags (from {src_db}.fm01.deviceCode, "
                    f"from {src_db}.fm01.namingPattern)"
                )
            else:
                tdSql.execute(
                    f"create vtable {vtable} (ts timestamp, "
                    f"`运行状态` int from {src_db}.fm01.status, "
                    f"`配料卡号` varchar(32) from {src_db}.fm01.card_no) "
                    f"tags (`设备号` varchar(32) from {src_db}.fm01.deviceCode, "
                    f"`命名模式` varchar(32) from {src_db}.fm01.namingPattern)"
                )

            ts = int(time.time() * 1000) - (300000 if fill_history else 0)
            insert_sql = (
                f"insert into {src_db}.fm01 values "
                f"({ts}, 0, 'idle') ({ts + 1000}, 1, 'card-a') "
                f"({ts + 61000}, 1, 'card-b') ({ts + 62000}, 0, 'closed') "
                f"({ts + 63000}, 0, 'idle')"
            )
            if fill_history:
                tdSql.execute(insert_sql)
            history = f"|fill_history({ts})" if fill_history else ""
            tdSql.execute(
                f"create stream {db}.s_tagref_calc "
                "event_window(start with `运行状态` > 0 end with `运行状态` <= 0) "
                f"true_for(1m) from {vtable} "
                f"stream_options(ignore_disorder{history}) "
                f"into {db}.out_calc nodelay_create_subtable as "
                "select _twend + 0s as output_timestamp, "
                "last(`配料卡号`) as card_no, last(`设备号`) as device_code, "
                f"last(`命名模式`) as naming_pattern from {vtable} "
                "where ts >= _twstart and ts < _twend",
                queryTimes=1,
            )
            tdStream.checkStreamStatus("s_tagref_calc")
            if not fill_history:
                tdSql.execute(insert_sql)
            tdSql.checkResultsByArray(
                "select cast(output_timestamp as bigint), card_no, device_code, "
                f"naming_pattern from {db}.out_calc",
                [(ts + 62000, "card-b", "FM-01", "melting")],
                retry=60,
            )
        finally:
            tdSql.execute(f"drop database if exists {db} force")
            tdSql.execute(f"drop database if exists {src_db} force")

    @pytest.mark.parametrize("tag_source_kind", ["child", "normal"])
    def test_unused_virtual_tag_source_namespace(self, tag_source_kind):
        """Unused virtual TAG sources preserve unqualified calculation columns.

        Reference an owned tag on a virtual child or normal table, omit that
        tag from the calculation, and verify an event window using ts and val.

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.3.0

        Labels: common,ci,stream,vtable

        Feishu: None

        History:
            - 2026-10-10 Added regression for virtual TAG source namespaces.
        """
        db = "stream_tagref_namespace"
        tdCom.create_snode_if_not_exists()
        try:
            tdSql.execute(f"create database {db} vgroups 1")
            tdSql.execute(f"create table {db}.raw (ts timestamp, val int)")
            if tag_source_kind == "child":
                tdSql.execute(
                    f"create stable {db}.vst (ts timestamp, val int) "
                    "tags (local_tag int) virtual 1"
                )
                tdSql.execute(
                    f"create vtable {db}.v_source (val from {db}.raw.val) "
                    f"using {db}.vst tags (7)"
                )
            else:
                tdSql.execute(
                    f"create vtable {db}.v_source "
                    f"(ts timestamp, val int from {db}.raw.val) "
                    "tags (local_tag int = 7)"
                )
            tdSql.execute(
                f"create vtable {db}.v_outer "
                f"(ts timestamp, val int from {db}.raw.val) "
                f"tags (unused_tag int from {db}.v_source.local_tag)"
            )
            tdSql.execute(
                f"create stream {db}.s_tagref_namespace "
                "event_window(start with val > 0 end with val <= 0) "
                f"from {db}.v_outer stream_options(ignore_disorder) "
                f"into {db}.out_calc nodelay_create_subtable as "
                "select _twend + 0s as output_timestamp, last(val) as value "
                f"from {db}.v_outer where ts >= _twstart and ts < _twend",
                queryTimes=1,
            )
            tdStream.checkStreamStatus("s_tagref_namespace")
            ts = int(time.time() * 1000)
            tdSql.execute(
                f"insert into {db}.raw values ({ts}, 0) ({ts + 1000}, 1) "
                f"({ts + 2000}, 2) ({ts + 3000}, 0) ({ts + 4000}, 0)"
            )
            tdSql.checkResultsByArray(
                f"select cast(output_timestamp as bigint), value from {db}.out_calc",
                [(ts + 3000, 2)],
                retry=60,
            )
        finally:
            tdSql.execute(f"drop database if exists {db} force")

    def _wait_for_value(self, db, expected, timeout=45):
        deadline = time.time() + timeout
        last_values = []
        while time.time() < deadline:
            tdSql.query(f"select value from {db}.out_b order by ts", queryTimes=1)
            last_values = [tdSql.getData(index, 0) for index in range(tdSql.getRows())]
            if expected in last_values:
                return
            time.sleep(1)
        tdLog.exit(
            f"timed out waiting for stream value {expected}, observed values: {last_values}"
        )

    def do_tag_ref_source_tag_change_refreshes_stream_calculation(self):
        db = "stream_vtb_tagref_refresh"
        tdCom.create_snode_if_not_exists()
        tdSql.execute(f"drop database if exists {db}")

        try:
            tdSql.execute(f"create database {db} precision '{self.precision}' vgroups 1")
            tdSql.execute(f"use {db}")
            tdSql.execute(
                "create stable src_st (ts timestamp, vol double, st tinyint) "
                "tags (element_name varchar(64))"
            )
            tdSql.execute(
                "create stable cfg_st (ts timestamp, dummy int) "
                "tags (oil_thresh double, water_thresh double)"
            )
            tdSql.execute("create table src_oil using src_st tags ('Well 01 - Oil')")
            tdSql.execute("create table c_cfg using cfg_st tags (25.0, 30.0)")
            tdSql.execute("insert into c_cfg values (now, 0)")
            tdSql.execute(
                "create stable vst (ts timestamp, vol double, st tinyint) "
                "tags (element_name varchar(64), oil_thresh double, water_thresh double) virtual 1"
            )
            tdSql.execute(
                "create vtable vtb_oil (vol from src_oil.vol, st from src_oil.st) "
                "using vst (element_name, oil_thresh, water_thresh) "
                "tags ('Well 01 - Oil', c_cfg.oil_thresh, c_cfg.water_thresh)"
            )

            tdSql.execute(
                "create stream s_tagref_refresh count_window(1) from vst partition by tbname "
                "into out_b as "
                "select cast(_tlocaltime/1000000 as timestamp) as ts, "
                "case when last(st) != 0 then null "
                "when last(element_name) like '%Oil%' then floor(last(vol)/last(oil_thresh)) "
                "else floor(last(vol)/last(water_thresh)) end as value, "
                "cast(case when last(st) != 0 then -1 else 0 end as tinyint) as status, "
                "last(element_name) as element_name from %%tbname where st >= -1"
            )

            tdLog.info("wait for stream to become ready before inserting source data")
            tdStream.checkStreamStatus("s_tagref_refresh")

            tdLog.info("verify the initial count-window calculation uses the referenced threshold")
            tdSql.execute("insert into src_oil values (now, 100.5, 0)")
            self._wait_for_value(db, 4.0)

            tdLog.info("change the tag-ref source and verify direct virtual-table visibility")
            tdSql.execute("alter table c_cfg set tag oil_thresh = 40.0")
            tdSql.query("select distinct oil_thresh from vst")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 40.0)

            tdLog.info("verify the next %%tbname calculation uses the refreshed referenced threshold")
            tdSql.execute("insert into src_oil values (now, 100.5, 0)")
            self._wait_for_value(db, 2.0)
        finally:
            tdSql.execute(f"drop database if exists {db} force")

        print("tag-ref count-window refresh .............. [ passed ]")

    def test_tag_ref_source_tag_change_refreshes_stream_calculation(self):
        """Tag-ref source tag changes refresh a running %%tbname count-window calculation.

        1. Creates a virtual child whose threshold tags reference a config child table
        2. Confirms direct virtual-table queries observe the updated config tag
        3. Requires the next count-window calculation to use the updated threshold

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.2.5

        Labels: common,ci,stream,vtable

        Jira: None

        History:
            - 2026-08-26 Wang Mingming Added tag-ref refresh regression coverage

        """
        self.do_tag_ref_source_tag_change_refreshes_stream_calculation()
