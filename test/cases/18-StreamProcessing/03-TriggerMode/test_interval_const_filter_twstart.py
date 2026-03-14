import time
from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStreamIntervalConstFilterTwstart:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_interval_const_filter_twstart(self):
        """Trigger mode sliding: const lower-bound with _twstart

        Reproduce bug where stream runtime fails when the query filter mixes:
          - constant timestamp lower-bound
          - window placeholder upper-bound (_twstart)

        Catalog:
            - Streams:03-TriggerMode

        Since: v3.4.0.9

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/6766024000

        History:
            - 2026-03-03 Jinqing Kuang Created

        """
        tdStream.dropAllStreamsAndDbs()
        tdStream.createSnode()

        tdSql.prepare(dbname="sdb_const_twstart", vgroups=1)
        tdSql.execute("use sdb_const_twstart")
        tdSql.execute("create stable stb (ts timestamp, c1 int) tags(gid int);")

        tdSql.execute(
            "create stream s_const_twstart interval(1s) sliding(1s) from stb into s_const_twstart_res "
            "as select _twstart, _twend, _twduration, count(*) from stb "
            "where ts >= '2026-01-01 00:00:00.000' and ts < _twstart;"
        )
        tdStream.checkStreamStatus("s_const_twstart")

        tdSql.execute("create table ctb_1 using stb tags (1);")
        tdSql.execute(
            "insert into ctb_1 values "
            "('2026-01-01 00:00:00.000', 0),"
            "('2026-01-01 00:00:01.000', 1),"
            "('2026-01-01 00:00:02.000', 2),"
            "('2026-01-01 00:00:03.000', 3),"
            "('2026-01-01 00:00:04.000', 4),"
            "('2026-01-01 00:00:05.000', 5);"
        )

        tdSql.checkResultsByFunc(
            sql="select * from s_const_twstart_res;",
            func=lambda: tdSql.getRows() == 5
            and tdSql.compareData(0, 0, "2026-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, "2026-01-01 00:00:01.000")
            and tdSql.compareData(0, 2, 1000)
            and tdSql.compareData(0, 3, 0)
            and tdSql.compareData(1, 0, "2026-01-01 00:00:01.000")
            and tdSql.compareData(1, 1, "2026-01-01 00:00:02.000")
            and tdSql.compareData(1, 2, 1000)
            and tdSql.compareData(1, 3, 1)
            and tdSql.compareData(2, 0, "2026-01-01 00:00:02.000")
            and tdSql.compareData(2, 1, "2026-01-01 00:00:03.000")
            and tdSql.compareData(2, 2, 1000)
            and tdSql.compareData(2, 3, 2)
            and tdSql.compareData(3, 0, "2026-01-01 00:00:03.000")
            and tdSql.compareData(3, 1, "2026-01-01 00:00:04.000")
            and tdSql.compareData(3, 2, 1000)
            and tdSql.compareData(3, 3, 3)
            and tdSql.compareData(4, 0, "2026-01-01 00:00:04.000")
            and tdSql.compareData(4, 1, "2026-01-01 00:00:05.000")
            and tdSql.compareData(4, 2, 1000)
            and tdSql.compareData(4, 3, 4)
        )

        tdSql.checkResultsByFunc(
            sql=(
                "select status from information_schema.ins_stream_tasks "
                "where stream_name='s_const_twstart' and type='Runner';"
            ),
            func=lambda: tdSql.getRows() > 0
            and all(tdSql.getData(i, 0) != "Failed" for i in range(tdSql.getRows())),
        )

