# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import time

import pytest

from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStreamVtableUnusedRef:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.ensureSnode()
        tdSql.execute("alter all dnodes 'debugflag 131'")

    @staticmethod
    def _readers(stream):
        tdSql.query(
            "select node_id from information_schema.ins_stream_tasks "
            f"where stream_name='{stream}' and `type`='Reader'"
        )
        return {tdSql.getData(i, 0) for i in range(tdSql.getRows())}

    @staticmethod
    def _rebind(column, reference):
        tdSql.execute(f"alter vtable vt alter column {column} set {reference}")
        # Results produced during fixture preparation must not satisfy the
        # post-ALTER check when the expected projection remains unchanged.
        tdSql.query("select now()")
        return tdSql.getData(0, 0)

    @staticmethod
    def _wait_result(db, expected, after=None):
        deadline = time.monotonic() + 40
        rows = []
        while time.monotonic() < deadline:
            tdSql.query(
                f"select ws, n from {db}.result order by ws desc limit 3"
            )
            rows = list(tdSql.queryResult)
            # Observe several completed calculations after ALTER, not an old
            # result whose value happens to match the unchanged projection.
            if len(rows) == 3 and all(
                value == expected and (after is None or stamp > after)
                for stamp, value in rows
            ):
                return rows[0][0]
            time.sleep(0.2)
        raise AssertionError(f"expected three fresh results {expected}, got {rows}")

    @pytest.mark.parametrize("shape", ["normal", "child", "stable", "chain"])
    @pytest.mark.parametrize("dependency", ["unused", "filter", "all"])
    def test_rebind_deployment_scope(self, shape, dependency):
        """Stream: redeploy only for required virtual-column sources

        Rebind a column within an existing vgroup and then to a new vgroup.
        Assert both results and Reader deployment for an unused column, a
        WHERE-only dependency, and COUNT(*) requiring the full timestamp union.
        Cover normal, child and super virtual tables plus a two-hop reference.

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.3.1

        Labels: common

        Feishu: https://project.feishu.cn/68d89cfef9d6e3a06e5fe454/68d89fef3cf13be3ff3878ca/detail/7144346803

        History:
            - 2026-10-23 Created
        """
        db = f"unused_ref_{shape}_{dependency}"
        src = f"{db}_src"
        stream = f"s_{shape}_{dependency}"
        try:
            tdSql.execute(f"create database {db} vgroups 1 buffer 8")
            tdSql.execute(f"use {db}")
            tdSql.execute("create table trigger_tb (ts timestamp, v int)")
            # Keep projection and filter sources distinct in the initial plan.
            tdSql.execute("create table original (ts timestamp, v int)")
            tdSql.execute("create table initial_b (ts timestamp, v int)")
            tdSql.execute("create table same_vg (ts timestamp, v int)")
            ts = 1704067200000
            tdSql.execute(f"insert into original values ({ts}, 10) ({ts + 1000}, 20)")
            tdSql.execute(f"insert into initial_b values ({ts}, 10) ({ts + 1000}, 20)")
            tdSql.execute(f"insert into same_vg values ({ts}, 30) ({ts + 1000}, 40)")
            old_ref = f"{db}.original.v"
            old_b_ref = f"{db}.initial_b.v"
            same_ref = f"{db}.same_vg.v"
            new_ref = f"{src}.original.v"
            if shape == "chain":
                for name, ref in (("old_link", old_ref), ("old_b_link", old_b_ref), ("same_link", same_ref)):
                    tdSql.execute(f"create vtable {name} (ts timestamp, v int from {ref})")
                old_ref = f"{db}.old_link.v"
                old_b_ref = f"{db}.old_b_link.v"
                same_ref = f"{db}.same_link.v"
                new_ref = f"{db}.new_link.v"
            if shape == "normal":
                tdSql.execute(
                    "create vtable vt (ts timestamp, "
                    f"a int from {old_ref}, b int from {old_b_ref})"
                )
            else:
                tdSql.execute("create stable vst (ts timestamp, a int, b int) tags (id int) virtual 1")
                tdSql.execute(f"create vtable vt (a from {old_ref}, b from {old_b_ref}) using vst tags (1)")
            table = "vst" if shape in ("stable", "chain") else "vt"
            expr = "count(*)" if dependency == "all" else "sum(a)"
            condition = "where b > 15" if dependency == "filter" else ""
            # Pre-create the destination so polling does not depend on the
            # asynchronous creation of the first output table.
            tdSql.execute("create table result (ws timestamp, n bigint)")
            tdSql.execute(
                f"create stream {stream} period(1s) from trigger_tb into result "
                f"as select cast(_tlocaltime / 1000000 as timestamp) ws, {expr} n "
                f"from {table} {condition}"
            )
            tdStream.checkStreamStatus(stream)
            tdSql.execute("insert into trigger_tb values (now, 1)")
            self._wait_result(db, {"unused": 30, "filter": 20, "all": 2}[dependency])
            readers = self._readers(stream)
            assert readers

            before = self._rebind("b", same_ref)
            self._wait_result(db, 2 if dependency == "all" else 30, after=before)
            assert self._readers(stream) == readers, "same-vgroup rebind changed Reader deployment"

            # Create the vgroup after deployment: initial metadata readers may
            # already cover every vgroup that existed when the stream was planned.
            tdSql.execute(f"create database {src} vgroups 1 buffer 8")
            tdSql.execute(f"create table {src}.original (ts timestamp, v int)")
            tdSql.execute(
                f"insert into {src}.original values "
                f"({ts}, 30) ({ts + 1000}, 5) ({ts + 2000}, 50)"
            )
            if shape == "chain":
                tdSql.execute(f"create vtable new_link (ts timestamp, v int from {src}.original.v)")
            tdSql.query(f"show {src}.vgroups")
            target_vg = tdSql.getData(0, 0)
            assert target_vg not in self._readers(stream)
            before = self._rebind("b", new_ref)
            self._wait_result(db, {"unused": 30, "filter": 10, "all": 3}[dependency], after=before)
            current = self._readers(stream)
            if dependency == "unused":
                assert current == readers, f"unused column deployed new Readers: {current - readers}"
                # The same new vgroup must still be deployed when a required
                # column is subsequently rebound to it.
                before = self._rebind("a", new_ref)
                self._wait_result(db, 85, after=before)
                assert target_vg in self._readers(stream)
            else:
                assert target_vg in current, "required source Reader was not deployed"
        finally:
            tdSql.execute(f"drop stream if exists {db}.{stream}")
            tdSql.execute(f"drop database if exists {db}")
            tdSql.execute(f"drop database if exists {src}")
