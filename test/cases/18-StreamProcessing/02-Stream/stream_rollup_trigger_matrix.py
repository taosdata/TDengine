from new_test_framework.utils import StreamCheckItem, tdLog, tdSql, tdStream


class TestStreamRollupTriggerMatrix:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_rollup_trigger_matrix(self):
        """Stream rollup trigger matrix

        1. Create rollup streams for period, sliding, interval, and session triggers
        2. Cover both stable and virtual stable trigger tables
        3. Verify each result table row and column

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215

        History:
            - 2026-05-19 Created
        """

        tdStream.ensureSnode()

        streams = []
        streams.append(self.RollupTriggerCase(1, "period", "stb", "period(1s)", "cast(1700000000000 as timestamp)"))
        streams.append(self.RollupTriggerCase(2, "period", "vstb", "period(1s)", "cast(1700000000000 as timestamp)"))
        streams.append(self.RollupTriggerCase(3, "sliding", "stb", "sliding(1h)", "_tcurrent_ts"))
        streams.append(self.RollupTriggerCase(4, "sliding", "vstb", "sliding(1h)", "_tcurrent_ts"))
        streams.append(self.RollupTriggerCase(5, "interval", "stb", "interval(1h) sliding(1h)", "_twstart"))
        streams.append(self.RollupTriggerCase(6, "interval", "vstb", "interval(1h) sliding(1h)", "_twstart"))
        streams.append(self.RollupTriggerCase(7, "session", "stb", "session(ts, 5s)", "_twstart"))
        streams.append(self.RollupTriggerCase(8, "session", "vstb", "session(ts, 5s)", "_twstart"))

        tdStream.checkAll(streams)

    class RollupTriggerCase(StreamCheckItem):
        def __init__(self, case_id, suffix, table_kind, trigger_clause, ts_expr):
            self.db = f"db_rollup_matrix_{case_id}"
            self.stream_name = f"s_{suffix}_{table_kind}"
            self.out_name = f"rs_{suffix}_{table_kind}"
            self.table_kind = table_kind
            self.trigger_clause = trigger_clause
            self.ts_expr = ts_expr
            self.insert_table = "t1" if table_kind == "stb" else "src1"
            self.expected_ts = {
                "period": "2023-11-15 06:13:20.000",
                "sliding": "2023-11-15 07:00:00.000",
                "interval": "2023-11-15 06:00:00.000",
                "session": "2023-11-15 06:13:20.000",
            }[suffix]
            self.expected_count = 0 if suffix == "period" else 4

        def create(self):
            tdSql.executes(
                [
                    f"drop database if exists {self.db} force",
                    f"create database {self.db} vgroups 1",
                    f"use {self.db}",
                ]
            )
            if self.table_kind == "stb":
                trigger_table = self.create_stable()
            else:
                trigger_table = self.create_virtual_stable()

            tdSql.execute(
                f"create stream {self.stream_name} {self.trigger_clause} from {trigger_table} "
                f"rollup by location stream_options(max_delay(3s)) into {self.out_name} "
                "tags (loc nchar(256) as %%1) "
                f"as select {self.ts_expr} as ts, count(*) as cnt from %%trows"
            )

        def create_stable(self):
            tdSql.executes(
                [
                    "create stable meters (ts timestamp, current float) tags (location nchar(64))",
                    "create table t1 using meters tags ('A.B.C')",
                ]
            )
            return "meters"

        def create_virtual_stable(self):
            tdSql.executes(
                [
                    "create table src1 (ts timestamp, current float)",
                    "create stable vmeters (ts timestamp, current float) tags (location nchar(64)) virtual 1",
                    "create vtable vt1 (src1.current) using vmeters tags ('A.B.C')",
                ]
            )
            return "vmeters"

        def insert1(self):
            base = 1700000000000
            for i in range(4):
                tdSql.execute(f"insert into {self.insert_table} values ({base + i * 1000}, {1.0 + i})")

        def check1(self):
            expected = [
                [self.expected_ts, "A", self.expected_count],
                [self.expected_ts, "A.B", self.expected_count],
                [self.expected_ts, "A.B.C", self.expected_count],
            ]
            tdSql.checkResultsByArray(
                sql=f"select ts, loc, cnt from {self.out_name} order by loc",
                exp_result=expected,
                retry=60,
            )
