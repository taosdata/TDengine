# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
# SPDX-License-Identifier: AGPL-3.0-or-later

import time

import pytest

from new_test_framework.utils import tdLog, tdSql, tdStream


class TestRecalcMixedSourceSchema:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    @staticmethod
    def _wait(sql, expected):
        deadline = time.monotonic() + 60
        rows = []
        while time.monotonic() < deadline:
            tdSql.query(sql, queryTimes=1)
            rows = list(tdSql.queryResult)
            if rows == expected:
                return
            time.sleep(0.2)
        raise AssertionError(f"{sql}: expected {expected}, got {rows}")

    @staticmethod
    def _insert(db, ts, value):
        doubles = ",".join(str(value * 10) for _ in range(11))
        tdSql.execute(
            f"insert into {db}.wide_child values "
            f"({ts},{doubles},'reason','detail',{ts},{ts})"
        )
        tdSql.execute(f"insert into {db}.narrow_child values ({ts},{value},{value},{ts})")
        tdSql.execute(f"insert into {db}.normal_a values ({ts},{value + 1})")
        tdSql.execute(f"insert into {db}.normal_b values ({ts},{value + 2},'text')")

    @pytest.mark.parametrize("flush", [False, True], ids=["memory", "disk_and_memory"])
    def test_recalc_mixed_source_schema(self, flush):
        """Stream metadata scans preserve each source table's schema identity.

        1. Put two different stable schemas and two ordinary schemas in one vnode.
        2. Merge duplicate timestamps with identical schema version numbers.
        3. Check history output, then update and recalculate the same interval.
        4. Exercise memory rows and a mixture of flushed and memory rows.

        Catalog: Streams:Recalculation

        Since: v3.4.3.11

        Labels: stream,recalc,virtual-table,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7170958580

        History:
            - 2026-11-17 OpenAI Created
        """
        tdStream.ensureSnode(1)
        db = f"recalc_schema_{int(flush)}"
        stream = f"schema_stream_{int(flush)}"
        base = (int(time.time() * 1000) // 60000 - 60) * 60000
        tdSql.executes([
            f"drop database if exists {db}",
            f"create database {db} vgroups 1",
            f"create stable {db}.wide (ts timestamp,"
            + ",".join(f"d{i} double" for i in range(1, 12))
            + ",reason nchar(254),detail nchar(254),begin_ts timestamp,end_ts timestamp) tags(id int)",
            f"create stable {db}.narrow "
            "(ts timestamp,v smallint unsigned,wduration bigint,last_ts timestamp) tags(id int)",
            f"create table {db}.wide_child using {db}.wide tags(1)",
            f"create table {db}.narrow_child using {db}.narrow tags(1)",
            f"create table {db}.normal_a (ts timestamp,v int)",
            f"create table {db}.normal_b (ts timestamp,v double,note nchar(32))",
            f"create vtable {db}.vt (ts timestamp,x double from {db}.wide_child.d1,"
            f"y smallint unsigned from {db}.narrow_child.v,"
            f"z int from {db}.normal_a.v,w double from {db}.normal_b.v)",
        ])
        for i in range(1, 4):
            self._insert(db, base + i * 1000, 99)
        if flush:
            tdSql.execute(f"flush database {db}")
        for i in range(1, 4):
            self._insert(db, base + i * 1000, i)
        self._insert(db, base + 60000, 4)
        tdSql.execute(
            f"create stream {db}.{stream} interval(1m) from {db}.vt "
            f"stream_options(fill_history({base})) into {db}.result "
            "as select _twstart as ts,count(*) as n,sum(x) as sx,sum(y) as sy,"
            "sum(z) as sz,sum(w) as sw from %%trows"
        )
        tdStream.checkStreamStatus()
        result = f"select n,sx,sy,sz,sw from {db}.result where ts={base}"
        self._wait(result, [(3, 60.0, 6, 9, 12.0)])

        # Establish realtime progress before requesting manual history replay.
        recent = (int(time.time() * 1000) // 60000 + 1) * 60000
        self._insert(db, recent, 5)
        self._insert(db, recent + 60000, 6)
        self._wait(f"select count(*) from {db}.result where ts={recent}", [(1,)])
        for i in range(1, 4):
            self._insert(db, base + i * 1000, i * 2)
        tdSql.execute(f"recalculate stream {db}.{stream} from {base} to {base + 60000}")
        self._wait(result, [(3, 120.0, 12, 15, 18.0)])
        self._wait(
            "select status,progress from information_schema.ins_stream_recalculates "
            f"where stream_name='{stream}'",
            [("Finished", "100%")],
        )
        tdSql.execute(f"drop database {db}")
