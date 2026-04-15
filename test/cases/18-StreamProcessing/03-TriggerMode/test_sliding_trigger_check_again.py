import os

from new_test_framework.utils import tdLog, tdSql, tdStream


class TestSlidingTriggerCheckAgain:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_sliding_trigger_check_again(self):
        """Sliding trigger should continue checking after one calc batch

        1. Create a sliding stream with 1ms(step unit `a`) and calc based on %%trows.
        2. Insert trigger rows spanning more than STREAM_CALC_REQ_MAX_WIN_NUM windows.
        3. Verify all windows are eventually materialized instead of stopping after the first batch.

        Since: v3.4.1.2

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/6945766706

        History:
            - 2026-4-14 Jinqing Kuang Created
        """

        tdStream.dropAllStreamsAndDbs()
        tdSql.query("show snodes")
        if tdSql.getRows() == 0:
            tdStream.createSnode()

        tdSql.prepare(dbname="sdb", vgroups=1)
        tdSql.execute("create table trigger_tb (ts timestamp, v int)")

        tdSql.execute(
            "create stream s_check_again interval(1a) sliding(1a) from trigger_tb into out_check_again "
            "as select _twstart ts, _twrownum tc, count(*) c from trigger_tb where ts >= _twstart and ts < _twend"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(
            "insert into trigger_tb values ('2025-01-01 00:00:00.000', 0), ('2025-01-01 00:01:00.000', 1)"
        )

        tdSql.checkResultsByFunc(
            sql="select first(ts), last(ts), count(*), min(tc), max(tc), sum(tc), min(c), max(c), sum(c) from sdb.out_check_again",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:01:00.000")
            and tdSql.compareData(0, 2, 60001)
            and tdSql.compareData(0, 3, 0)
            and tdSql.compareData(0, 4, 1)
            and tdSql.compareData(0, 5, 2)
            and tdSql.compareData(0, 6, 0)
            and tdSql.compareData(0, 7, 1)
            and tdSql.compareData(0, 8, 2)
        )

        tdSql.execute("insert into trigger_tb values ('2025-01-01 00:02:00.000', 1)")

        tdSql.checkResultsByFunc(
            sql="select first(ts), last(ts), count(*), min(tc), max(tc), sum(tc), min(c), max(c), sum(c) from sdb.out_check_again",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:02:00.000")
            and tdSql.compareData(0, 2, 120001)
            and tdSql.compareData(0, 3, 0)
            and tdSql.compareData(0, 4, 1)
            and tdSql.compareData(0, 5, 3)
            and tdSql.compareData(0, 6, 0)
            and tdSql.compareData(0, 7, 1)
            and tdSql.compareData(0, 8, 3)
        )
