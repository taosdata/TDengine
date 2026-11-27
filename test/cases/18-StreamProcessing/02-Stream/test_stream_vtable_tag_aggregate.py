# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import time

import pytest

from new_test_framework.utils import tdCom, tdLog, tdSql, tdStream


class TestStreamVtableTagAggregate:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_period_tag_aggregate_isolates_virtual_children(self):
        """Periodic tag aggregates calculate every virtual-child partition.

        Two virtual children share one source and the same non-NULL stored tag.
        Aggregate without a residual filter or calculation grouping, and check
        both children after successive writes. An ordinary stable aggregate
        must still include both children.

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.3.0

        Labels: common,ci,stream,vtable

        Feishu: https://project.feishu.cn/taosdata_td/issue/detail/7150292758

        History:
            - 2026-11-13 Cover tag dispatch isolation for dynamic aggregates.
        """
        db = "stream_vtag_period"
        tdCom.create_snode_if_not_exists()
        try:
            tdSql.execute(f"create database {db} vgroups 1")
            tdSql.execute(f"create table {db}.raw (ts timestamp, val int)")
            tdSql.execute(
                f"create stable {db}.vst (ts timestamp, x int) "
                "tags (device int) virtual 1"
            )
            for child in ("v1", "v2"):
                tdSql.execute(
                    f"create vtable {db}.{child} (x from {db}.raw.val) "
                    f"using {db}.vst tags (7)"
                )
            tdSql.execute(f"insert into {db}.raw values (now - 1m, 1)")
            tdSql.checkResultsByArray(
                f"select count(*), sum(x), last(device) from {db}.vst",
                [(2, 2, 7)],
            )
            tdSql.execute(
                f"create stream {db}.s_vtag_period period(1s) "
                f"from {db}.vst partition by tbname "
                "stream_options(ignore_disorder|ignore_nodata_trigger) "
                f"into {db}.out_calc nodelay_create_subtable "
                "output_subtable(concat('out_', tbname)) as "
                "select cast(_tlocaltime / 1000000 as timestamp) as ts, "
                "last(x) as last_x, last(device) as device from %%tbname",
                queryTimes=1,
            )
            tdStream.checkStreamStatus("s_vtag_period")
            for value in (10, 20):
                tdSql.execute(f"insert into {db}.raw values (now, {value})")
                for child in ("v1", "v2"):
                    tdSql.checkResultsByArray(
                        f"select last_x, device from {db}.out_{child} "
                        "order by ts desc limit 1",
                        [(value, 7)],
                        retry=60,
                    )
            tdStream.checkStreamStatus("s_vtag_period")
        finally:
            tdSql.execute(f"drop database if exists {db} force")

    @pytest.mark.parametrize("fill_history", [False, True])
    @pytest.mark.parametrize("aggregate_tags", [True, False])
    def test_aggregate_preserves_source_and_window(self, fill_history, aggregate_tags):
        """Virtual-table tag aggregates retain source vgroups and window bounds.

        Aggregate stored tags and columns from two source vgroups without a
        residual data filter, with and without tag aggregates. Check closed
        windows and a later calculation, including job-only rows whose state
        column is NULL.

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.3.0

        Labels: common,ci,stream,vtable

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7160044070

        History:
            - 2026-11-06 Added regression for tag aggregate reader deployment.
        """
        db = "stream_vtag_agg"
        state_db = "stream_vtag_state"
        job_db = "stream_vtag_job"
        tdCom.create_snode_if_not_exists()
        try:
            for name in (state_db, job_db, db):
                tdSql.execute(f"create database {name} vgroups 1")
            tdSql.execute(
                f"create table {state_db}.raw (ts timestamp, val smallint unsigned)"
            )
            tdSql.execute(f"create table {job_db}.raw (ts timestamp, val nchar(64))")
            tdSql.execute(
                f"create stable {db}.vst (ts timestamp, state_val smallint unsigned, "
                "job nchar(64)) tags (standard nchar(32), code nchar(32)) virtual 1"
            )
            tdSql.execute(
                f"create vtable {db}.vt (state_val from {state_db}.raw.val, "
                f"job from {job_db}.raw.val) using {db}.vst tags ('24', '123000000')"
            )
            ts = int(time.time() * 1000) - (300000 if fill_history else 0)
            job_sql = (
                f"insert into {job_db}.raw values ({ts}, 'job-a') "
                f"({ts + 30000}, 'job-late1') ({ts + 62000}, 'job-b') "
                f"({ts + 90000}, 'job-late2')"
            )
            state_sql = (
                f"insert into {state_db}.raw values ({ts}, 1) ({ts + 61000}, 1) "
                f"({ts + 62000}, 2) ({ts + 123000}, 2) ({ts + 124000}, 3)"
            )
            if fill_history:
                tdSql.execute(job_sql)
                tdSql.execute(state_sql)
            history = f"|fill_history({ts})" if fill_history else ""
            standard = "last(standard)" if aggregate_tags else "'24'"
            code = "last(code)" if aggregate_tags else "'123000000'"
            tdSql.execute(
                f"create stream {db}.s_vtag_agg state_window(state_val) "
                f"extend(1) true_for(60s) from {db}.vst partition by tbname "
                f"stream_options(ignore_disorder{history}) "
                f"into {db}.out_calc nodelay_create_subtable as "
                "select cast(_twstart as timestamp) as output_timestamp, "
                f"last(state_val) as state_val, {standard} as standard, "
                f"{code} as code, last(job) as job, "
                "case when last(state_val) = 1 then _twduration / 1000 "
                f"- (case when _twduration > {standard} * 60000 "
                f"then {standard} * 60 else 0 end) end as run_duration "
                "from %%tbname where ts >= cast(_twstart as timestamp) "
                "and ts <= cast(_twend as timestamp)",
                queryTimes=1,
            )
            tdStream.checkStreamStatus("s_vtag_agg")
            if not fill_history:
                tdSql.execute(job_sql)
                tdSql.execute(state_sql)
            query = (
                "select cast(output_timestamp as bigint), state_val, standard, "
                f"code, job, run_duration from {db}.out_calc order by output_timestamp"
            )
            expected = [
                (ts, 1, "24", "123000000", "job-late1", 61.999),
                (ts + 62000, 2, "24", "123000000", "job-late2", None),
            ]
            tdSql.checkResultsByArray(query, expected, retry=60)

            # Realtime input has already opened state 3 at ts + 124000.
            later_ts = ts + 124000
            if fill_history:
                # Subsequent input must be beyond the completed backfill.
                later_ts = max(later_ts, int(time.time() * 1000) + 1000)
            tdSql.execute(f"insert into {job_db}.raw values ({later_ts + 30000}, 'job-late3')")
            tdSql.execute(
                f"insert into {state_db}.raw values "
                f"({later_ts}, 3) ({later_ts + 61000}, 3) ({later_ts + 62000}, 4)"
            )
            expected.append((later_ts, 3, "24", "123000000", "job-late3", None))
            tdSql.checkResultsByArray(query, expected, retry=60)
        finally:
            for name in (db, state_db, job_db):
                tdSql.execute(f"drop database if exists {name} force")
