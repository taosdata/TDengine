# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import time

import pytest

from new_test_framework.utils import tdSql, tdStream


class TestStreamVtableTagRedeploy:
    def setup_class(cls):
        tdStream.ensureSnode()

    @pytest.mark.parametrize("feedback", [False, True])
    def test_tag_output_after_rebind(self, feedback):
        """Stream: preserve virtual tags after calculation reader redeployment

        Bind a virtual column to its stream output in another vgroup after
        deployment; compare against the same stream without that reference.
        Two partitions and two windows must keep their own literal tag values.

        Catalog:
            - Streams:VirtualTable

        Since: v3.4.3.12

        Labels: common,ci,stream,vtable

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7169474316

        History:
            - 2026-11-16 Created
        """
        db = f"vtag_realtime_{int(feedback)}"
        src = f"{db}_src"
        try:
            tdSql.execute(f"create database {src} vgroups 1 buffer 8")
            tdSql.execute(f"create database {db} vgroups 1 buffer 8")
            tdSql.execute(f"use {db}")
            tdSql.execute(
                f"create stable {src}.meters (ts timestamp, rate float, pressure int) "
                "tags (location varchar(100))"
            )
            tdSql.execute(
                "create stable vst (ts timestamp, unbound tinyint, `流量` float, `水压` int, `属性1` varchar(256)) "
                "tags (element varchar(256), category varchar(256), new tinyint, "
                "minimum int, maximum int, `地址` varchar(100), path1 varchar(512)) virtual 1"
            )
            for i in (1, 2):
                tdSql.execute(f"create table {src}.m{i} using {src}.meters tags ('location-{i}')")
                tdSql.execute(f"insert into {src}.m{i} values (now-1m, 0, 100)")
                tdSql.execute(
                    f"create vtable v{i} (`流量` from {src}.m{i}.rate, `水压` from {src}.m{i}.pressure) "
                    f"using vst tags ('meter-{i}', null, 0, 0, 400, from {src}.m{i}.location, '公共事业.北京.西城')"
                )
            tdSql.execute(
                "create stream s_tags event_window(start with `流量` > 10 end with `流量` <= 10) "
                "true_for(start(5s)) from vst partition by tbname "
                "stream_options(config(timezone='Asia/Shanghai', firstDayOfWeek=0) | "
                "event_type(window_close) | ignore_disorder) "
                "into out nodelay_create_subtable output_subtable(concat('out_', tbname)) "
                "as select _twstart+0s as output_timestamp, element as `属性1` "
                "from %%tbname where ts >= _twstart and ts < _twend",
                queryTimes=1,
            )
            tdStream.checkStreamStatus("s_tags")
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                tdSql.query(
                    "select table_name from information_schema.ins_tables "
                    f"where db_name='{db}' and stable_name='out'"
                )
                if {row[0] for row in tdSql.queryResult} == {"out_v1", "out_v2"}:
                    break
                time.sleep(1)
            else:
                raise AssertionError("nodelay output child tables were not created")
            if feedback:
                for i in (1, 2):
                    tdSql.execute(f"alter vtable v{i} alter column `属性1` set {db}.out_v{i}.`属性1`")
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                tdSql.query(
                    "select count(*) from information_schema.ins_stream_tasks "
                    "where stream_name='s_tags' and `type`='Reader'"
                )
                if tdSql.getData(0, 0) > 0:
                    break
                time.sleep(1)
            else:
                raise AssertionError("stream Reader tasks were not deployed")
            time.sleep(3)
            ts = int(time.time() * 1000)
            expected = []
            for window in (0, 1):
                start = ts + window * 20000
                for i in (1, 2):
                    tdSql.execute(
                        f"insert into {src}.m{i} values ({start}, 11, 100) ({start+3000}, 12, 101) "
                        f"({start+6000}, 13, 102) ({start+9000}, 0, 103)"
                    )
                    expected.append((start, f"meter-{i}"))
                tdSql.checkResultsByArray(
                    "select cast(output_timestamp as bigint), `属性1` from out order by output_timestamp, `属性1`",
                    expected,
                    retry=40,
                )
            tdStream.checkStreamStatus("s_tags")
        finally:
            tdSql.execute(f"drop database if exists {db} force")
            tdSql.execute(f"drop database if exists {src} force")
