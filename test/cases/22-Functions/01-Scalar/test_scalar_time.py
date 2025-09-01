from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestTime:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_time(self):
        """Scalar: Time

        Test time functions, including TIMETRUNCATE, TIMEDIFF, and their combined usage.

        Catalog:
            - Function:Sclar

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/scalar/tsConvert.sim

        """

        tdLog.info(f"======== step1")
        tdSql.prepare("db1", drop=True)
        tdSql.execute(f"use db1;")
        tdSql.execute(
            f"create stable st1 (ts timestamp, f1 int, f2 binary(30)) tags(t1 int, t2 binary(30));"
        )
        tdSql.execute(f"create table tb1 using st1 tags(1, '1');")
        tdSql.execute(f"insert into tb1 values ('2022-07-10 16:31:00', 1, '1');")
        tdSql.execute(f"insert into tb1 values ('2022-07-10 16:32:00', 2, '2');")
        tdSql.execute(f"insert into tb1 values ('2022-07-10 16:33:00', 3, '3');")
        tdSql.execute(f"insert into tb1 values ('2022-07-10 16:34:00', 4, '4');")
        tdSql.query(
            f"select * from (select ts,TIMETRUNCATE(ts,1d),TIMETRUNCATE(ts,1h), abs(timediff(TIMETRUNCATE(ts,1d),TIMETRUNCATE(ts,1h),1h)) as td from tb1 where ts >='2022-06-01 00:00:00' and ts <='2022-11-3 23:59:59' ) t where td >12;"
        )

        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2022-07-10 16:31:00.000")
        tdSql.checkData(1, 0, "2022-07-10 16:32:00.000")
        tdSql.checkData(2, 0, "2022-07-10 16:33:00.000")
        tdSql.checkData(3, 0, "2022-07-10 16:34:00.000")

        tdSql.query(f"select * from tb1 where ts > '2022-07-10 16:32:00';")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2022-07-10 16:33:00.000")
        tdSql.checkData(1, 0, "2022-07-10 16:34:00.000")

        tdSql.query(f"select * from tb1 where ts + 1 > '2022-07-10 16:32:00';")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2022-07-10 16:32:00.000")
        tdSql.checkData(1, 0, "2022-07-10 16:33:00.000")
        tdSql.checkData(2, 0, "2022-07-10 16:34:00.000")

        tdSql.query(
            f"select * from tb1 where '2022-07-10 16:32:00' > timestamp '2022-07-10 16:31:59';"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2022-07-10 16:31:00.000")
        tdSql.checkData(1, 0, "2022-07-10 16:32:00.000")
        tdSql.checkData(2, 0, "2022-07-10 16:33:00.000")
        tdSql.checkData(3, 0, "2022-07-10 16:34:00.000")

        tdSql.query(
            f"select case f1 when '1' then 1 when '2022-07-10 16:32:00' then '2' end from tb1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(
            f"select case ts when '2022-07-10 16:31:00' then 1 when '2022-07-10 16:32:00' then '2' end from tb1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(
            f"select case '2022-07-10 16:31:00' when ts then 1 when 2022 then '2' else 3 end from tb1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 3)
