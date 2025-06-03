import time
import math
from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStreamDevBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_dev_basic(self):
        """basic test 3

        Verification testing during the development process.

        Catalog:
            - Streams:Others

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-26 Simon Guan Created

        """

        self.basic1()
        # self.basic2()

    def basic1(self):
        tdLog.info(f"basic test 1")
        tdStream.dropAllStreamsAndDbs()
        tdStream.createSnode()

        tdLog.info(f"=============== create database")
        tdSql.prepare(dbname="qdb", vgroups=1)

        tdLog.info(f"=============== create super table")
        tdSql.execute(f"create stable meters (cts timestamp, cint int, cuint int) tags(tint int);")
        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== write query data")
        sqls = [
            "insert into t1 using meters tags(1) values ('2025-01-01 00:00:00'    , 0, 0);",
            "insert into t2 using meters tags(2) values ('2025-01-01 00:00:00.102', 1, 0);",
            "insert into t1 using meters tags(1) values ('2025-01-01 00:00:01'    , 1, 1);",
            "insert into t2 using meters tags(2) values ('2025-01-01 00:00:01.400', 2, 1);",
            "insert into t1 using meters tags(1) values ('2025-01-01 00:00:02'    , 2, 2);",
            "insert into t2 using meters tags(2) values ('2025-01-01 00:00:02.600', 3, 2);",
        ]

        tdLog.info(f"=============== create trigger table")
        tdSql.execute("create table stream_trigger (ts timestamp, c1 int, c2 int);")
        tdSql.query(f"show tables")
        tdSql.checkKeyExist("stream_trigger")

        tdLog.info(f"=============== create stream")
        sql = "create stream s9 interval(1s) sliding(1s) from stream_trigger partition by tbname into out9                                 as select _twstart ts, _twend te, _twduration td, _twrownum tw, _tgrpid tg, _tlocaltime tl, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twend;"
        tdSql.execute(sql)

        tdStream.checkStreamStatus()

        tdLog.info(f"=============== write trigger data")
        tdSql.execute(
            "insert into stream_trigger values ('2025-01-01 00:00:00', 0, 0), ('2025-01-01 00:00:01', 1, 1), ('2025-01-01 00:00:02', 2, 2);"
        )

        tdLog.info(f"=============== check stream result")

        result_sql9 = "select * from qdb.out9"
        tdSql.checkResultsByFunc(sql=result_sql9, func=lambda: tdSql.getRows() > 0)
