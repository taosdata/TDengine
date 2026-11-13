# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import time

import pytest

from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStreamVtableOwnedTags:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.ensureSnode()

    @pytest.mark.parametrize("source, tag_type, value, predicate", [
        ("inherited", "nchar(16)", "'温度控制'", "mode = '温度控制'"),
        ("owned", "nchar(16)", "'温度控制'", "mode = '温度控制'"),
        ("owned", "int", "7", "mode = 7"),
        ("owned", "int", "null", "mode is null"),
        ("ref", "nchar(16)", "'温度控制'", "mode = '温度控制'"),
        ("chain", "nchar(16)", "'温度控制'", "mode = '温度控制'"),
        ("chain", "int", "null", "mode is null"),
        ("chain_ref", "nchar(16)", "'温度控制'", "mode = '温度控制'"),
        *[
            pytest.param(source, tag_type, value, predicate, id=f"{source}-{kind}-{label}")
            for source in ("owned", "chain")
            for kind, tag_type in (("varchar", "varchar(16)"), ("nchar", "nchar(16)"))
            for label, value, predicate in (
                ("empty", "''", "(mode = '' and mode is not null) or v = 3"),
                ("null", "null", "mode is null or v = 3"),
            )
        ],
        *[
            pytest.param(source, tag_type, value, predicate, id=f"{source}-{label}")
            for source in ("owned_prefilter", "ref_prefilter")
            for label, tag_type, value, predicate in (
                ("int", "int", "7", "mode is not null and mode = 7"),
                ("empty", "nchar(16)", "''", "mode is not null and mode = ''"),
                ("null", "nchar(16)", "null", "mode is null"),
            )
        ],
    ])
    def test_owned_tag_event_window(self, source, tag_type, value, predicate):
        """Stream: virtual-child owned tags open event windows

        Check inherited and owned literals, fixed-width NULLs, direct references,
        and references through another virtual child's owned tag. The latter
        crosses vgroups and exercises name lookup as well as local cid lookup.
        Empty/NULL string contrasts open at v=2 when resolved correctly, or v=3
        when confused with each other, producing total=5 versus total=3.
        PRE_FILTER cases combine an inherited tag with an owned literal or
        reference, checking that tag reconstruction preserves both values.

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.3.11

        Labels: common,ci,stream,vtable

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7167765571

        History:
            - 2026-11-13 Created
        """
        db = "owned_tag_trigger"
        src_db = f"{db}_src"
        ts = int(time.time() * 1000) - 60000
        prefilter = source.endswith("_prefilter")
        source = source.removesuffix("_prefilter")
        try:
            tdSql.execute(f"create database {src_db} vgroups 1 buffer 8")
            tdSql.execute(f"create database {db} vgroups 1 buffer 8")
            tdSql.execute(f"use {db}")
            tdSql.execute(
                f"create stable {src_db}.readings (ts timestamp, v int) tags (mode {tag_type})"
            )
            tdSql.execute(f"create table {src_db}.src using {src_db}.readings tags ({value})")
            tdSql.execute(
                f"insert into {src_db}.src values ({ts}, 2) ({ts + 1000}, 3) ({ts + 2000}, 0)"
            )
            # Owned tags are stored separately from the parent schema, including
            # their cid. Keep an inherited tag on the child to distinguish them.
            owner_db = src_db if source.startswith("chain") else db
            schema = f"mode {tag_type}" if source == "inherited" else "id int"
            tag = value if source == "inherited" else "1"
            tdSql.execute(f"create stable {owner_db}.vst (ts timestamp, v int) tags ({schema}) virtual 1")
            tdSql.execute(
                f"create vtable {owner_db}.vc (v from {src_db}.src.v) using {owner_db}.vst tags ({tag})"
            )
            if source != "inherited":
                tdSql.execute(f"alter vtable {owner_db}.vc add tag mode {tag_type}")
                assignment = f"{src_db}.src.mode" if source in ("ref", "chain_ref") else value
                tdSql.execute(f"alter vtable {owner_db}.vc set tag mode = {assignment}")
            if source.startswith("chain"):
                tdSql.execute(f"create stable vst (ts timestamp, v int) tags (mode {tag_type}) virtual 1")
                tdSql.execute(
                    f"create vtable vc (v from {src_db}.src.v) using vst tags (from {src_db}.vc.mode)"
                )

            tdSql.execute("create table result (ts timestamp, total bigint)")
            options = f"fill_history({ts - 1000})"
            if prefilter:
                options += f" | pre_filter(id = 1 and ({predicate}))"
            tdSql.execute(
                f"create stream s_owned_tag event_window(start with ({predicate}) and v > 0 end with v = 0) "
                f"from vc stream_options({options}) into result "
                "as select _twstart ts, sum(v) total from vc where ts >= _twstart and ts < _twend",
                queryTimes=1,
            )
            deadline = time.monotonic() + 40
            rows = []
            while time.monotonic() < deadline:
                tdSql.query(
                    "select status, message from information_schema.ins_streams "
                    f"where db_name='{db}' and stream_name='s_owned_tag'"
                )
                assert tdSql.getRows() == 1
                assert tdSql.getData(0, 0) != "Failed", tdSql.queryResult
                tdSql.query("select total from result order by ts")
                rows = list(tdSql.queryResult)
                if rows:
                    assert rows == [(5,)], rows
                    return
                time.sleep(0.2)
            raise AssertionError(f"owned-tag event window expected [(5,)], got {rows}")
        finally:
            tdSql.execute(f"drop database if exists {db} force")
            tdSql.execute(f"drop database if exists {src_db} force")
