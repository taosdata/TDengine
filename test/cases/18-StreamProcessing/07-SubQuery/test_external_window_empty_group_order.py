# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import time

import pytest

from new_test_framework.utils import tdLog, tdSql, tdStream


class TestExternalWindowEmptyGroupOrder:
    BASE = 1767196800000  # 2026-01-01 00:00:00 Asia/Shanghai
    DAY = 86400000

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.createSnode(1)

    @pytest.mark.parametrize("with_count", [True, False])
    def test_history_aggregate_with_empty_vgroup_inputs(self, with_count):
        """Merge daily aggregates when a group has no input on other vgroups.

        1. Distribute 48 meter groups over four vgroups, filtering out 16 groups.
        2. Replay three historical daily windows with and without COUNT.
        3. Check every group's timestamps, tags, aggregates and empty results.

        Catalog:
            - Streams:SubQuery:ExternalWindow

        Since: v3.4.3.1

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7138745449

        History:
            - 2026-10-19 OpenAI Added empty partial-aggregate ordering regression
        """
        db = f"ext_empty_order_{int(with_count)}"
        tdSql.prepare(dbname=db, vgroups=4, keep=3650)
        try:
            tdSql.execute(
                f"create stable {db}.src (ts timestamp, voltage double, data_number bigint) "
                "tags(meter_code nchar(30), project_code nchar(30))"
            )
            for meter in range(48):
                tdSql.execute(
                    f"create table {db}.m{meter} using {db}.src "
                    f"tags('m{meter:03}', 'p{meter % 4}')"
                )
                rows = []
                for day in range(3):
                    for hour, voltage in ((1, 10 + meter), (2, 20 + meter)):
                        value = 0 if meter % 3 == 0 else voltage
                        ts = self.BASE + day * self.DAY + hour * 3600000
                        rows.append(f"({ts},{value},60)")
                tdSql.execute(f"insert into {db}.m{meter} values " + " ".join(rows))

            tdSql.query(
                "select vgroup_id from information_schema.ins_tables "
                f"where db_name='{db}' group by vgroup_id"
            )
            tdSql.checkRows(4)
            tdSql.execute(f"flush database {db}")
            count_expr = ", count(*) as hour_number" if with_count else ""
            tdSql.execute(
                f"create stream {db}.daily interval(1d) sliding(1d) "
                f"from {db}.src partition by meter_code, project_code "
                "stream_options(config(timezone='Asia/Shanghai', firstDayOfWeek=0) | "
                "fill_history('2025-12-15 00:00:00')) "
                f"into {db}.result "
                "output_subtable(concat('daily_', cast(meter_code as binary))) "
                "tags(meter_code nchar(30) as meter_code, project_code nchar(30) as project_code) "
                "as select _twstart as ts, _twend as ts_end, "
                "avg(voltage) as avg_voltage, min(voltage) as min_voltage, "
                "max(voltage) as max_voltage, sum(data_number) as data_number, "
                f"first(ts) as data_start_time, last(ts) as data_end_time{count_expr} "
                f"from {db}.src where meter_code=%%1 and voltage>0 "
                "and _c0>=_twstart and _c0<=_twend"
            )

            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                tdSql.query(
                    "select status,message,history_progress_pct from information_schema.ins_streams "
                    f"where db_name='{db}' and stream_name='daily'"
                )
                tdSql.checkRows(1)
                assert tdSql.getData(0, 0) != "Failed", tdSql.queryResult
                if tdSql.getData(0, 2) == 100:
                    break
                time.sleep(0.2)
            else:
                tdLog.exit(f"historical daily aggregation did not finish: {tdSql.queryResult}")

            count_column = ",hour_number" if with_count else ""
            tdSql.query(
                "select meter_code,project_code,cast(ts as bigint),cast(ts_end as bigint),"
                "avg_voltage,min_voltage,max_voltage,data_number,"
                f"cast(data_start_time as bigint),cast(data_end_time as bigint){count_column} "
                f"from {db}.result order by meter_code,ts"
            )
            tdSql.checkRows(144 if with_count else 96)
            row = 0
            for meter in range(48):
                empty = meter % 3 == 0
                if empty and not with_count:
                    continue
                for day in range(3):
                    start = self.BASE + day * self.DAY
                    expected = [f"m{meter:03}", f"p{meter % 4}", start, start + self.DAY]
                    if empty:
                        expected += [None] * 6
                    else:
                        expected += [15 + meter, 10 + meter, 20 + meter, 120, start + 3600000, start + 7200000]
                    if with_count:
                        expected.append(0 if empty else 2)
                    for column, value in enumerate(expected):
                        tdSql.checkData(row, column, value)
                    row += 1
        finally:
            tdSql.execute(f"drop stream if exists {db}.daily")
            tdSql.execute(f"drop database if exists {db}")
