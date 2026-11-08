# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import time

import pytest

from new_test_framework.utils import tdCom, tdLog, tdSql, tdStream


class TestStreamVtableTagAggregate:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

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
