# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import time

import pytest

from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStreamVtableOwnedColumns:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.ensureSnode()
        tdSql.execute("alter all dnodes 'debugflag 131'")

    @staticmethod
    def _wait_running(db):
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            tdSql.query(
                "select status, message from information_schema.ins_streams "
                f"where db_name='{db}' and stream_name='s_owned'"
            )
            assert tdSql.getRows() == 1
            assert tdSql.getData(0, 0) != "Failed", tdSql.queryResult
            tdSql.query(
                "select status from information_schema.ins_stream_tasks "
                "where stream_name='s_owned' and `type`='Trigger'"
            )
            if tdSql.getRows() == 1 and tdSql.getData(0, 0) == "Running":
                return
            time.sleep(0.2)
        raise AssertionError("owned-column stream did not start")

    @staticmethod
    def _prepare(db, rebind=False, own_column=True):
        tdSql.execute(f"create database {db} vgroups 1 buffer 8")
        tdSql.execute(f"use {db}")
        tdSql.execute("create table inherited_src (ts timestamp, v int)")
        tdSql.execute("create table owned_src (ts timestamp, v int)")
        reference = f"{db}.owned_src.v"
        if rebind:
            # ALTER of an owned column's ref is not supported. Rebind a normal
            # vtable in its source chain to exercise runtime stream refresh.
            tdSql.execute(f"create vtable owned_link (ts timestamp, v int from {reference})")
            reference = f"{db}.owned_link.v"
        # The inherited-column control uses the same SQL column names, but both
        # columns live in the parent schema and have ordinary low column IDs.
        extra_column = "" if own_column else ", owned int"
        extra_ref = "" if own_column else f", owned from {reference}"
        tdSql.execute(f"create stable vst (ts timestamp, inherited int{extra_column}) tags (id int) virtual 1")
        tdSql.execute(
            f"create vtable vc (inherited from {db}.inherited_src.v{extra_ref}) using vst tags (1)"
        )
        if own_column:
            tdSql.execute(f"alter vtable vc add column owned int from {reference}")

    @staticmethod
    def _wait_result(db, stream, expected, after=None, min_rows=1):
        deadline = time.monotonic() + 40
        rows = []
        while time.monotonic() < deadline:
            tdSql.query(
                "select status, message from information_schema.ins_streams "
                f"where db_name='{db}' and stream_name='{stream}'"
            )
            assert tdSql.getRows() == 1
            assert tdSql.getData(0, 0) != "Failed", tdSql.queryResult
            condition = "" if after is None else f"where ts > '{after}'"
            tdSql.query(f"select n from {db}.result {condition} order by ts desc limit {min_rows}")
            rows = list(tdSql.queryResult)
            if rows == [(expected,)] * min_rows:
                return
            time.sleep(0.2)
        raise AssertionError(f"expected {expected}, got {rows}")

    @pytest.mark.parametrize("window", ["event", "state", "chain"])
    def test_owned_trigger(self, window):
        """Stream: event and state windows read virtual-child owned columns

        Use an owned column in the trigger and calculation. The event closes
        on zero; the state window closes when the two positive rows change.
        Also cover an inherited reference through another child's owned column
        and an event trigger rebound to a newly created source vgroup.

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.3.7

        Labels: common

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7149920805

        History:
            - 2026-11-01 Created
        """
        db = f"owned_trigger_{window}"
        src_db = f"{db}_src"
        try:
            self._prepare(db, rebind=window == "event")
            if window != "chain":
                tdSql.execute("create table result (ts timestamp, n bigint)")
            source, column = "vc", "owned"
            if window == "chain":
                tdSql.execute("create stable link (ts timestamp, inherited int) tags (id int) virtual 1")
                tdSql.execute(f"create vtable link_child (inherited from {db}.vc.owned) using link tags (1)")
                source, column = "link", "inherited"
            clause = (
                f"state_window({column})" if window == "state" else
                f"event_window(start with {column} > 0 end with {column} = 0)"
            )
            tdSql.execute(
                f"create stream s_owned {clause} from {source} "
                f"{'partition by tbname ' if window == 'chain' else ''}into result "
                f"as select _twstart ts, sum({column}) n from {source} "
                "where ts >= _twstart and ts <= _twend"
            )
            self._wait_running(db)
            ts = int(time.time() * 1000)
            tdSql.execute(
                f"insert into owned_src values ({ts}, 2) ({ts + 1000}, 2) ({ts + 2000}, 0)"
            )
            self._wait_result(db, "s_owned", 4)
            if window == "event":
                tdSql.execute(f"create database {src_db} vgroups 1 buffer 8")
                tdSql.execute(f"create table {src_db}.src (ts timestamp, v int)")
                tdSql.query(f"show {src_db}.vgroups")
                vg_id = tdSql.getData(0, 0)
                tdSql.execute(f"alter vtable owned_link alter column v set {src_db}.src.v")
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    tdSql.query(
                        "select node_id from information_schema.ins_stream_tasks "
                        "where stream_name='s_owned' and `type`='Reader'"
                    )
                    if (vg_id,) in tdSql.queryResult:
                        break
                    time.sleep(0.2)
                else:
                    raise AssertionError("owned trigger rebind did not deploy the new Reader")
                tdSql.execute(
                    f"insert into {src_db}.src values "
                    f"({ts + 10000}, 3) ({ts + 11000}, 3) ({ts + 12000}, 0)"
                )
                self._wait_result(db, "s_owned", 6)
        finally:
            tdSql.execute(f"drop database if exists {db} force")
            tdSql.execute(f"drop database if exists {src_db} force")

    @pytest.mark.parametrize("source, expression, expected", [
        ("vc", "sum(owned)", 30),
        ("vc", "cast(sum(inherited) + sum(owned) as bigint)", 33),
        ("vc", "count(*)", 4),
        ("inherited_child", "count(*)", 4),
        ("vst", "count(*)", 2),
        ("link", "sum(inherited)", 30),
    ])
    def test_owned_calculation(self, source, expression, expected):
        """Stream: calculation honors the virtual query source schema

        Direct children include owned values and their timestamps. Super
        tables exclude them. An inherited reference may still resolve through
        another virtual child's owned column. Rebinding the owned source must
        refresh the direct-child calculation across databases. Dropping an
        invisible owned source must not interrupt supertable calculation.
        An inherited-column control checks the shared chain-refresh path.

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.3.7

        Labels: common

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7149920805

        History:
            - 2026-11-01 Created
        """
        db = f"owned_calc_{source}_{expected}"
        src_db = f"{db}_src"
        try:
            direct_child = source in ("vc", "inherited_child")
            self._prepare(db, rebind=direct_child, own_column=source != "inherited_child")
            tdSql.execute("create table trigger_tb (ts timestamp, v int)")
            tdSql.execute("create table result (ts timestamp, n bigint)")
            ts = 1704067200000
            tdSql.execute(f"insert into inherited_src values ({ts}, 1) ({ts + 2000}, 2)")
            tdSql.execute(f"insert into owned_src values ({ts + 1000}, 10) ({ts + 3000}, 20)")
            if source == "link":
                tdSql.execute("create stable link (ts timestamp, inherited int) tags (id int) virtual 1")
                tdSql.execute(f"create vtable link_child (inherited from {db}.vc.owned) using link tags (1)")
            tdSql.execute(
                f"create stream s_owned period(1s) from trigger_tb into result "
                f"as select cast(_tlocaltime / 1000000 as timestamp) ts, {expression} n "
                f"from {'vc' if direct_child else source}"
            )
            self._wait_running(db)
            tdSql.execute("insert into trigger_tb values (now, 1)")
            self._wait_result(db, "s_owned", expected)
            if direct_child:
                tdSql.execute(f"create database {src_db} vgroups 1 buffer 8")
                tdSql.execute(f"create table {src_db}.src (ts timestamp, v int)")
                tdSql.execute(
                    f"insert into {src_db}.src values ({ts + 1000}, 40) ({ts + 3000}, 50) ({ts + 4000}, 60)"
                )
                tdSql.execute(f"alter vtable owned_link alter column v set {src_db}.src.v")
                rebound = {30: 150, 33: 153, 4: 5}[expected]
                self._wait_result(db, "s_owned", rebound)
            elif source == "vst":
                tdSql.execute("drop table owned_src")
                tdSql.query("select now()")
                after = tdSql.getData(0, 0)
                self._wait_result(db, "s_owned", 2, after=after)
        finally:
            tdSql.execute(f"drop database if exists {db} force")
            tdSql.execute(f"drop database if exists {src_db} force")

    @pytest.mark.parametrize("dependency, condition, expected", [
        ("unused_agg", "", 3),
        ("unused_scan", "where inherited > 0", 3),
        ("filter", "where owned > 15", 2),
    ])
    def test_owned_calculation_dependencies(self, dependency, condition, expected):
        """Stream: resolve only required virtual-child column sources

        Drop an unused owned source after calculation starts and require three
        fresh results through both aggregate pushdown and filtered scans.
        An owned column used only by WHERE must still affect the result.

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.3.7

        Labels: common

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7149920805

        History:
            - 2026-11-01 Created
        """
        db = f"owned_dependency_{dependency}"
        try:
            self._prepare(db)
            tdSql.execute("create table trigger_tb (ts timestamp, v int)")
            tdSql.execute("create table result (ts timestamp, n bigint)")
            ts = 1704067200000
            tdSql.execute(f"insert into inherited_src values ({ts}, 1) ({ts + 1000}, 2)")
            tdSql.execute(f"insert into owned_src values ({ts}, 10) ({ts + 1000}, 20)")
            tdSql.execute(
                "create stream s_owned period(1s) from trigger_tb into result "
                "as select cast(_tlocaltime / 1000000 as timestamp) ts, sum(inherited) n "
                f"from vc {condition}"
            )
            self._wait_running(db)
            tdSql.execute("insert into trigger_tb values (now, 1)")
            self._wait_result(db, "s_owned", expected)
            if dependency == "filter":
                tdSql.execute(f"insert into owned_src values ({ts}, 30)")
            else:
                tdSql.execute("drop table owned_src")
            tdSql.query("select now()")
            after = tdSql.getData(0, 0)
            self._wait_result(db, "s_owned", 3, after=after, min_rows=3)
        finally:
            tdSql.execute(f"drop database if exists {db} force")

    @pytest.mark.parametrize("source", ["vc", "vst"])
    def test_timestamp_only_trigger(self, source):
        """Stream: timestamp-only triggers use the query source's visible columns

        Interleave inherited and owned timestamps. A two-row count window
        closes twice on the direct child and once on the super table. The
        supertable continues producing windows after the owned source drops.

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.3.7

        Labels: common

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7149920805

        History:
            - 2026-11-01 Created
        """
        db = f"owned_ts_{source}"
        try:
            self._prepare(db)
            if source == "vc":
                tdSql.execute("create table result (ts timestamp, te timestamp, n bigint)")
            tdSql.execute(
                f"create stream s_owned count_window(2) from {source} "
                f"{'partition by tbname ' if source == 'vst' else ''}into result "
                f"as select _twstart ts, _twend te, count(*) n from {source} "
                "where ts >= _twstart and ts <= _twend"
            )
            self._wait_running(db)
            ts = int(time.time() * 1000)
            # One multi-table submit makes all four timestamps visible together.
            tdSql.execute(
                f"insert into inherited_src values ({ts}, 1) ({ts + 2000}, 2) "
                f"owned_src values ({ts + 1000}, 10) ({ts + 3000}, 20)"
            )
            expected = ([(ts, ts + 1000, 2), (ts + 2000, ts + 3000, 2)]
                        if source == "vc" else [(ts, ts + 2000, 2)])
            tdSql.checkResultsByArray(
                f"select cast(ts as bigint), cast(te as bigint), n from {db}.result order by ts",
                expected, retry=40,
            )
            if source == "vst":
                tdSql.execute("drop table owned_src")
                tdSql.execute(f"insert into inherited_src values ({ts + 4000}, 3) ({ts + 6000}, 4)")
                tdSql.checkResultsByArray(
                    f"select cast(ts as bigint), cast(te as bigint), n from {db}.result order by ts",
                    expected + [(ts + 4000, ts + 6000, 2)], retry=40,
                )
        finally:
            tdSql.execute(f"drop database if exists {db} force")
