import time
from new_test_framework.utils import (tdLog, tdSql, tdStream, StreamCheckItem,)


class TestPreFilterTrowsScanCols:
    """Regression for stream trigger/calc scan-cols optimization.

    Trigger AST must NOT include calc-only columns (c3, t2).
    Calc AST MUST include pre_filter columns (c2) and apply pre_filter as WHERE,
    so calc-side independent re-scan returns exactly the rows pre_filter allows.
    """

    precision = 'ms'

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_pre_filter_trows_scan_cols(self):
        """%%trows scan cols optimization

        Trigger only scans state_window + pre_filter cols; calc independently
        re-scans with injected pre_filter WHERE, producing identical rows.

        Catalog:
            - Streams:Options

        Since: v3.3.x

        Labels: common,ci

        Jira: None
        """

        tdStream.createSnode()
        streams = []
        streams.append(self.PreFilterTrows())
        tdStream.checkAll(streams)

    class PreFilterTrows(StreamCheckItem):
        def __init__(self):
            self.db = "pf_trows_db"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(
                f"create stable stb (ts timestamp, c1 int, c2 int, c3 int) "
                f"tags (t1 int, t2 int)"
            )
            tdSql.execute(f"create table ct1 using stb tags(1, 100)")
            tdSql.execute(f"create table ct2 using stb tags(2, 200)")
            # Stream from the example in the design spec:
            #   trigger: state_window(c1) + pre_filter(c2>2)
            #   calc:    select _c0, sum(c3), avg(t2) from %%trows
            tdSql.execute(
                f"create stream s_pf state_window(c1) from stb "
                f"partition by t1 stream_options(pre_filter(c2 > 2)) "
                f"into res_stb (firstts, sum_c3, avg_t2) as "
                f"select first(_c0), sum(c3), avg(t2) from %%trows;"
            )

        def insert1(self):
            sqls = [
                # ct1 (t1=1, t2=100): c1 alternates to drive state windows;
                # c2 values include some <=2 (must be filtered out by pre_filter).
                "insert into ct1 values ('2025-01-01 00:00:00', 1, 1, 10);",  # c2<=2 -> filtered
                "insert into ct1 values ('2025-01-01 00:00:01', 1, 5, 20);",
                "insert into ct1 values ('2025-01-01 00:00:02', 1, 7, 30);",
                "insert into ct1 values ('2025-01-01 00:00:03', 2, 8, 40);",  # state change closes window
                "insert into ct1 values ('2025-01-01 00:00:04', 2, 2, 50);",  # c2<=2 -> filtered
                "insert into ct1 values ('2025-01-01 00:00:05', 2, 9, 60);",
                "insert into ct1 values ('2025-01-01 00:00:06', 1, 4, 70);",  # state change
            ]
            tdSql.executes(sqls)

        def check1(self):
            # Expect at least one closed window for ct1 with c1==1 spanning ts 1..2
            # (the ts 0 row is dropped by pre_filter).
            # sum(c3) over rows kept = 20+30 = 50; avg(t2) = 100.
            tdSql.checkResultsByFunc(
                sql=f"select sum_c3, avg_t2 from {self.db}.res_stb "
                    f"where firstts = '2025-01-01 00:00:01';",
                func=lambda: tdSql.getRows() == 1
                             and tdSql.getData(0, 0) == 50
                             and abs(tdSql.getData(0, 1) - 100.0) < 1e-9,
            )

        def check2(self):
            # Negative: writing WHERE on %%trows must still be rejected.
            tdSql.error(
                f"create stream s_neg state_window(c1) from {self.db}.stb "
                f"partition by t1 stream_options(pre_filter(c2 > 2)) "
                f"into {self.db}.res_neg (firstts, s) as "
                f"select first(_c0), sum(c3) from %%trows where c2 > 5;",
                expectErrInfo="trows can not be used with WHERE clause",
            )
