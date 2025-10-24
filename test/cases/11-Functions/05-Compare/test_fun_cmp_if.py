from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestFunIf:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_fun_cmp_if(self):
        """ Fun: If

        1. Using in data columns and scalar functions within SELECT statements
        2. Using in data columns within WHERE conditions
        3. Using in data columns within GROUP BY statements
        4. Using in data columns within STATE WINDOW
        5. Using in aggregate functions while including the IS NULL operator

        Catalog:
            - Operator

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-9-10 Stephen Jin: Initial if operator related cases.

        """

        self.If()
        tdStream.dropAllStreamsAndDbs()
        self.IfWithSum()
        tdStream.dropAllStreamsAndDbs()
        self.IfNull()
        self.Nvl()
        self.Nvl2()
        self.NullIf()
        self.IsNull()
        self.IsNotNull()
        self.Coalesce()
        tdStream.dropAllStreamsAndDbs()

    def If(self):
        tdLog.info(f"======== prepare data")

        tdSql.prepare("db1", drop=True, vgroups=3)
        tdSql.execute(f"use db1;")
        tdSql.execute(
            f"create stable sta (ts timestamp, f1 int, f2 binary(10), f3 bool) tags(t1 int, t2 bool, t3 binary(10));"
        )
        tdSql.execute(f"create table tba1 using sta tags(0, false, '0');")
        tdSql.execute(f"create table tba2 using sta tags(1, true, '1');")
        tdSql.execute(f"create table tba3 using sta tags(null, null, '');")
        tdSql.execute(f"create table tba4 using sta tags(1, false, null);")
        tdSql.execute(f"create table tba5 using sta tags(3, true, 'aa');")
        tdSql.execute(
            f"insert into tba1 values ('2022-09-26 15:15:01', 0, 'a', false);"
        )
        tdSql.execute(f"insert into tba1 values ('2022-09-26 15:15:02', 1, '0', true);")
        tdSql.execute(
            f"insert into tba1 values ('2022-09-26 15:15:03', 5, '5', false);"
        )
        tdSql.execute(
            f"insert into tba1 values ('2022-09-26 15:15:04', null, null, null);"
        )
        tdSql.execute(
            f"insert into tba2 values ('2022-09-27 15:15:01', 0, 'a', false);"
        )
        tdSql.execute(f"insert into tba2 values ('2022-09-27 15:15:02', 1, '0', true);")
        tdSql.execute(
            f"insert into tba2 values ('2022-09-27 15:15:03', 5, '5', false);"
        )
        tdSql.execute(
            f"insert into tba2 values ('2022-09-27 15:15:04', null, null, null);"
        )
        tdSql.execute(
            f"insert into tba3 values ('2022-09-28 15:15:01', 0, 'a', false);"
        )
        tdSql.execute(f"insert into tba3 values ('2022-09-28 15:15:02', 1, '0', true);")
        tdSql.execute(
            f"insert into tba3 values ('2022-09-28 15:15:03', 5, '5', false);"
        )
        tdSql.execute(
            f"insert into tba3 values ('2022-09-28 15:15:04', null, null, null);"
        )
        tdSql.execute(
            f"insert into tba4 values ('2022-09-29 15:15:01', 0, 'a', false);"
        )
        tdSql.execute(f"insert into tba4 values ('2022-09-29 15:15:02', 1, '0', true);")
        tdSql.execute(
            f"insert into tba4 values ('2022-09-29 15:15:03', 5, '5', false);"
        )
        tdSql.execute(
            f"insert into tba4 values ('2022-09-29 15:15:04', null, null, null);"
        )
        tdSql.execute(
            f"insert into tba5 values ('2022-09-30 15:15:01', 0, 'a', false);"
        )
        tdSql.execute(f"insert into tba5 values ('2022-09-30 15:15:02', 1, '0', true);")
        tdSql.execute(
            f"insert into tba5 values ('2022-09-30 15:15:03', 5, '5', false);"
        )
        tdSql.execute(
            f"insert into tba5 values ('2022-09-30 15:15:04', null, null, null);"
        )

        tdLog.info(f"======== if with null else")

        tdSql.query(f"select if(3, 4, null) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(1, 0, 4)
        tdSql.checkData(2, 0, 4)
        tdSql.checkData(3, 0, 4)

        tdSql.query(f"select if(0, 4, null) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(f"select if(null, 4, null) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(f"select if(1, 4+1, null) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 5.000000000)
        tdSql.checkData(1, 0, 5.000000000)
        tdSql.checkData(2, 0, 5.000000000)
        tdSql.checkData(3, 0, 5.000000000)

        tdSql.query(f"select if(1-1, 0, null) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(f"select if(1+1, 0, null) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(2, 0, 0)
        tdSql.checkData(3, 0, 0)

        tdSql.query(f"select if(1, 1-1+2, null) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 2.000000000)
        tdSql.checkData(1, 0, 2.000000000)
        tdSql.checkData(2, 0, 2.000000000)
        tdSql.checkData(3, 0, 2.000000000)

        tdSql.query(f"select if(1 > 0, 1, null) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)

        tdSql.query(f"select if(1 > 0, 2, null) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(3, 0, 2)

        tdSql.query(f"select if(1 > 0, 1 < 2, null) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)

        tdSql.query(f"select if(1 > 2, 1 < 2, null) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(f"select if(abs(3), abs(-1), null) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)

        tdSql.query(f"select if(abs(1+1), abs(-1)+abs(3), null) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 4.000000000)
        tdSql.checkData(1, 0, 4.000000000)
        tdSql.checkData(2, 0, 4.000000000)
        tdSql.checkData(3, 0, 4.000000000)

        tdSql.query(f"select if(0, 1, 3) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 3)

        tdSql.query(f"select if(0, 1, if(1, 0, 3)) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(2, 0, 0)
        tdSql.checkData(3, 0, 0)

        tdSql.query(
            f"select if(0, 1, if(1, 0, if(2, 3, null))) from tba1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(2, 0, 0)
        tdSql.checkData(3, 0, 0)

        tdSql.query(f"select if('a', 'b', if(null, 0, null)) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(f"select if('2', 'b', if(null, 0, null)) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "b")
        tdSql.checkData(1, 0, "b")
        tdSql.checkData(2, 0, "b")
        tdSql.checkData(3, 0, "b")

        tdSql.query(f"select if(0, 'b', null) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(f"select if(0, 'b', 2) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(3, 0, 2)

        tdSql.query(f"select if(sum(2), sum(2)-sum(1), null) from tba1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4.000000000)

        tdSql.query(f"select if(sum(2), abs(-2), null) from tba1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        tdSql.query(f"select if(ts, ts, null) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2022-09-26 15:15:01")
        tdSql.checkData(1, 0, "2022-09-26 15:15:02")
        tdSql.checkData(2, 0, "2022-09-26 15:15:03")
        tdSql.checkData(3, 0, "2022-09-26 15:15:04")

        tdSql.query(f"select if(f1, ts, null) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, "2022-09-26 15:15:02")
        tdSql.checkData(2, 0, "2022-09-26 15:15:03")
        tdSql.checkData(3, 0, None)

        tdSql.query(
            f"select if(f1, f1, if(f1 + 1, f1 + 1, f1 is null)) from tba1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 5)
        #tdSql.checkData(3, 0, "true")
        tdSql.checkData(3, 0, 1)

        tdSql.query(f"select if(f1, 3, if(ts, ts, null)) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1664176501000)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 1664176504000)

        tdSql.query(f"select if(3, f1, null) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(3, 0, None)

        tdSql.query(f"select if(f1, 3, if(1, 2, null)) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 2)

        tdSql.query(f"select if(sum(f1), sum(f1)-abs(-1), null) from tba1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5.000000000)

        tdSql.query(
            f"select if(sum(f1), sum(f1)-abs(f1), null) from tba1 group by f1 order by f1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, 0.000000000)
        tdSql.checkData(3, 0, 0.000000000)

        tdSql.query(
            f"select if(f1, sum(f1), if(f1 is not null, 9, 8)) from tba1 group by f1 order by f1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 8)
        tdSql.checkData(1, 0, 9)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 5)

        tdSql.query(f"select f1 from tba1 where f1 > if(f1, 0, 3);")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 5)

        tdSql.query(f"select f1 from tba1 where ts > if(ts, ts, null);")
        tdSql.checkRows(0)

        tdSql.query(
            f"select sum(f1) v,count(f1) from tba1 partition by if(f1, f1, if(1, 1, null)) order by v;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(1, 1, 1)

        tdSql.query(
            f"select if(f1 < 3, 1, if(f1 >= 3, 2, 3)) caseWhen, sum(f1),count(f1) from tba1 group by if(f1 < 3, 1, if(f1 >= 3, 2, 3)) order by caseWhen;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(1, 1, 5)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(2, 1, None)
        tdSql.checkData(2, 2, 0)

        tdSql.query(
            f"select f1 from tba1 order by if(f1 <= 0, 3, if(f1 = 1, 4, if(f1 >= 3, 2, 1))) desc;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(3, 0, None)

        tdSql.query(
            f"select cast(if(f1 == f1, f1 + 1, f1 is null) as double) from tba1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1.000000000)
        tdSql.checkData(1, 0, 2.000000000)
        tdSql.checkData(2, 0, 6.000000000)
        tdSql.checkData(3, 0, 1.000000000)

        tdSql.query(
            f"select sum(if(f1 == f1, f1 + 1, f1 is null) + 1) from tba1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 14.000000000)

        tdSql.query(
            f"select if(f1 < 3, 1, if(f1 >= 3, 2, 3)),sum(f1),count(f1) from tba1 state_window(if(f1 < 3, 1, if(f1 >= 3, 2, 3)));"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(1, 1, 5)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(2, 1, None)
        tdSql.checkData(2, 2, 0)

        tdSql.query(
            f"select f1 from tba1 where if(if(f1 <= 0, 3, if(f1 = 1, 4, if(f1 >= 3, 2, 1))) > 2, 1, 0) > 0;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 1)

        tdSql.query(
            f"select if(f1 is not null, if(f1 <= 0, f1, f1 * 10), -1) from tba1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 10)
        tdSql.checkData(2, 0, 50)
        tdSql.checkData(3, 0, -1)

        tdLog.info(f"======== use if instead of case xx when xx")

        tdSql.query(f"select if(3 == 3, 4, null) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(1, 0, 4)
        tdSql.checkData(2, 0, 4)
        tdSql.checkData(3, 0, 4)

        tdSql.query(f"select if(3 == 1, 4, null) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(f"select if(3 == 1, 4, 2) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(3, 0, 2)

        tdSql.query(f"select if(3 is null, 4, if(3 == '3', 1, null)) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)

        tdSql.query(f"select if('3' is null, 4, if('3' == 3, 1, null)) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)

        tdSql.query(f"select if(null, if(null, 4, null), if(null == 3, 1, null)) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(f"select if(3.0 is null, 4, if(3.0 == '3', 1, null)) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)

        tdSql.query(f"select if(f2 == 'a', 4, if(f2 == '0', 1, null)) from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(
            f"select if(f2 == f1, f1, if(f2 == f1 - 1, f1, 99)) from tba1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(3, 0, 99)

        tdSql.query(
            f"select if(cast(f2 as int) == 0, f2, if(cast(f2 as int) == f1, 11, ts)) from tba1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "a")
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(2, 0, 11)
        tdSql.checkData(3, 0, 1664176504000)

        tdSql.query(
            f"select if(f1 + 1 == 1, 1, if(f1 + 1 == 2, 2, 3)) from tba1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 3)

        tdSql.query(
            f"select if(f1 == sum(f1), sum(f1)-abs(f1), null) from tba1 group by f1 order by f1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 0.000000000)
        tdSql.checkData(2, 0, 0.000000000)
        tdSql.checkData(3, 0, 0.000000000)

        tdSql.query(
            f"select f1, if(sum(f1) == 1, f1 + 99, if(sum(f1) == f1, f1 -99, f1)) from tba1 group by f1 order by f1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(1, 1, -99)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(2, 1, 100)
        tdSql.checkData(3, 0, 5)
        tdSql.checkData(3, 1, -94)

        tdSql.query(f"select if(3, 4, null) from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select if(0, 4, null) from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select if(null, 4, null) from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select if(1, 4+1, null) from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select if(1-1, 0, null) from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select if(1+1, 0, null) from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select if(abs(3), abs(-1), null) from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select if(abs(1+1), abs(-1)+abs(3), null) from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select if(0, 1, 3) from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select if(0, 1, if(1, 0, 3)) from sta;")
        tdSql.checkRows(20)

        tdSql.query(
            f"select if(0, 1, if(1, 0, if(2, 3, null))) from sta;"
        )
        tdSql.checkRows(20)

        tdSql.query(f"select if('a', 'b', if(null, 0, null)) from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select if('2', 'b', if(null, 0, null)) from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select if(0, 'b', null) from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select if(0, 'b', 2+abs(-2)) from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select if(3 == 3, 4, null) from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select if(3 == 1, 4, null) from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select if(3 == 1, 4, 2) from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select if('3' is null, 4, if('3' == 3, 1, null)) from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select if(null, if(null, 4, null), if(null == 3, 1, null)) from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select if(3.0 is null, 4, if(3.0 == '3', 1, null)) from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select f2,if(f2 == 'a', 4, if(f2 == '0', 1, null)) from sta order by f2;")
        tdSql.checkRows(20)

        tdSql.query(
            f"select f2,if(f2 == f1, f1, if(f2 == f1 - 1, f1, 99)) from sta order by f2;"
        )
        tdSql.checkRows(20)

        tdSql.query(
            f"select if(cast(f2 as int) == 0, f2, if(cast(f2 as int) == f1, 11, ts)) from sta;"
        )
        tdSql.checkRows(20)

        tdSql.query(
            f"select f1, if(f1 + 1 == 1, 1, if(f1 + 1 == 2, 2, 3)) from sta order by f1;"
        )
        tdSql.checkRows(20)

        tdSql.query(
            f"select if(f1 == sum(f1), sum(f1)-abs(f1), null) from sta group by f1 order by f1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 0.000000000)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(
            f"select if(sum(f1) == 1, f1 + 99, if(sum(f1) == f1, f1 - 99, f1)) from sta group by f1 order by f1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, -99)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 5)

        tdSql.query(
            f"select distinct tbname, if(t1 == t2, t1, t1 + 100) from sta order by tbname;"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, None)
        tdSql.checkData(3, 1, 101)
        tdSql.checkData(4, 1, 103)

        tdSql.error(f"select if(sum(f1), sum(f1)-abs(f1), null) from tba1;")

        tdSql.execute(f"drop database if exists test_db;")
        tdSql.execute(f"create database test_db vgroups 5;")
        tdSql.execute(f"use test_db;")
        tdSql.execute(
            f"create stable test_stable (ts TIMESTAMP,c_int INT,c_uint INT UNSIGNED, c_bigint BIGINT, c_ubigint BIGINT UNSIGNED, c_float FLOAT, c_double DOUBLE, c_binary BINARY(20), c_smallint SMALLINT, c_usmallint SMALLINT UNSIGNED, c_tinyint TINYINT,c_utinyint TINYINT UNSIGNED,c_bool BOOL,c_nchar NCHAR(20), c_varchar VARCHAR(20), c_varbinary VARBINARY(20), c_geometry GEOMETRY(50)) tags(tag_id JSON);"
        )

        str = '{\\"tag1\\":5}'
        tdSql.execute(f'create table t_test using test_stable tags("{str}");')
        tdSql.execute(
            f"insert into t_test values ('2022-09-30 15:15:01',123,456,1234567890,9876543210,123.45,678.90,'binary_val',32767,65535,127,255,true,'涛思数据','varchar_val', '1101', 'point(10 10)');"
        )

        tdSql.query(
            f"select if(c_int > 100, c_float, c_int) as result from t_test;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 123.45)

        tdSql.query(
            f"select if(c_bigint > 100000, c_double, c_bigint) as result from t_test;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 678.9)

        tdSql.query(
            f"select if(c_bool, c_bool, c_utinyint) as result from t_test;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query(
            f"select if(c_smallint > 30000, c_usmallint, c_smallint) as result from t_test;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 65535)

        tdSql.query(
            f"select if(c_binary = 'binary_val', c_nchar, c_binary) as result from t_test;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '涛思数据')

        tdSql.query(
            f"select if(c_bool, c_int, c_bool) as result from t_test;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 123)

        tdSql.query(
            f"select if(ts > '2022-01-01 00:00:00', c_bool, ts) as result from t_test;"
        )
        tdSql.checkData(0, 0, 1)

        tdSql.query(
            f"select if(c_double > 100, c_nchar, c_double) as result from t_test;"
        )
        tdSql.checkData(0, 0, '涛思数据')

        tdSql.query(
            f"select if(c_double > 100, c_varchar, c_double) as result from t_test;"
        )
        tdSql.checkData(0, 0, "varchar_val")

        tdSql.query(
            f"select if(1, 1234567890987654, 'abcertyuiojhgfddhjgfcvbn');"
        )
        tdSql.checkData(0, 0, 1234567890987654)

        tdSql.query(
            f"select if(0, 1234567890987654, 'abcertyuiojhgfddhjgfcvbn');"
        )
        tdSql.checkData(0, 0, "abcertyuiojhgfddhjgfcvbn")

        tdSql.query(
            f"select if(0, 1234567890987654, c_nchar) from t_test;"
        )
        tdSql.checkData(0, 0, '涛思数据')

        tdSql.query(
            f"select if(1, 1234567890987654, c_nchar) from t_test;"
        )
        tdSql.checkData(0, 0, 1234567890987654)

        tdSql.query(
            f"select if(1, c_varchar, c_varbinary) from t_test;"
        )
        tdLog.info(f"==========> {tdSql.getData(0, 0)}")
        tdSql.checkData(0, 0, b'varchar_val')

        tdSql.error(
            f"select if(ts > '2022-01-01 00:00:00', c_varchar, c_geometry) as result from t_test;"
        )
        tdSql.error(
            f"select if(ts > '2022-01-01 00:00:00', c_bool, c_geometry) as result from t_test;"
        )
        tdSql.error(
            f"select if(0, tag_id, c_geometry) as result from t_test;"
        )
        tdSql.error(
            f"select if(0, tag_id, c_nchar) as result from t_test;"
        )
        tdSql.error(
            f"select if(0, tag_id, c_int) as result from t_test;"
        )
        tdSql.error(
            f"select if(0, tag_id, c_float) as result from t_test;"
        )
        tdSql.error(
            f"select if(c_double > 100, c_varbinary, c_geometry) as result from t_test;"
        )
        tdSql.error(
            f"select if(c_bool, c_double, c_varbinary) as result from t_test;"
        )

    def IfWithSum(self):
        tdLog.info(f'=============== create database')
        tdSql.execute(f"create database test")

        tdLog.info(f'=============== create super table and child table')
        tdSql.execute(f"use test")

        tdSql.execute(f"CREATE STABLE st (day timestamp, c2 int) TAGS (vin binary(32))")

        tdSql.execute(f'insert into test.g using st TAGS ("TAG1") values("2023-05-03 00:00:00.000", 1)')
        tdSql.execute(f'insert into test.t using st TAGS ("TAG1") values("2023-05-03 00:00:00.000", 1)')
        tdSql.execute(f'insert into test.tg using st TAGS ("TAG1") values("2023-05-03 00:00:00.000", 1)')

        tdSql.query(f"select sum(if(t.c2 is NULL, 0, 1) + if(t.c2 is NULL, 0, 1)), sum(if(t.c2 is NULL, 0, 1) + if(t.c2 is NULL, 0, 1) + if(t.c2 is NULL, 0, 1)) from test.t t, test.g g, test.tg tg where t.day = g.day and t.day = tg.day and t.day between '2021-05-03' and '2023-05-04' and t.vin = 'TAG1' and t.vin = g.vin and t.vin = tg.vin group by t.day;")

        tdLog.info(f'{tdSql.getRows()}) {tdSql.getData(0,0)} {tdSql.getData(0,1)}')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2.000000000)
        tdSql.checkData(0, 1, 3.000000000)

    def IfNull(self):
        tdSql.query(f"select ifnull(1, 0);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select ifnull(null, 10);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)

        tdSql.query(f"select ifnull(1/0, 10);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)

        tdSql.query(f"select ifnull(1/0, 'yes');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'yes')

    def Nvl(self):
        tdSql.query(f"select nvl(1, 0);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select nvl(null, 10);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)

        tdSql.query(f"select nvl(1/0, 10);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10)

        tdSql.query(f"select nvl(1/0, 'yes');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'yes')

    def Nvl2(self):
        tdSql.query(f"select nvl2(null, 1, 2);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        tdSql.query(f"select nvl2('x', 1, 2);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

    def NullIf(self):
        tdSql.query(f"select nullif(1, 1);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select nullif(1, 2);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

    def IsNull(self):
        tdSql.query(f"SELECT 1 IS NULL, 0 IS NULL, NULL IS NULL;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, 1)

    def IsNotNull(self):
        tdSql.query(f"SELECT 1 IS NOT NULL, 0 IS NOT NULL, NULL IS NOT NULL;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 0)

    def Coalesce(self):
        tdSql.query(f"select coalesce(null, 1);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select coalesce(null, null, null);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

    def test_fun_cmp_ifnull(self):
        """ Fun: ifnull()

        1. Check "select ifnull(1, 0)";
        2. Check "select ifnull(null, 10)";
        3. Check "select ifnull(1/0, 10)";
        4. Check "select ifnull(1/0, 'yes')";

        Since: v3.3.0.0

        Labels: common,ci

        History:
            - 2025-10-16 Alex Duan add doc

        """
        pass

    def test_fun_cmp_nvl(self):
        """ Fun: nvl()

        same with ifnull() 

        Since: v3.0.0.0

        Labels: common,ci

        """
        pass

    def test_fun_cmp_nullif(self):
        """ Fun: nullif()

        1. Check "select nullif(1, 1)";
        2. Check "select nullif(1, 2)";

        Since: v3.3.0.0

        Labels: common,ci

        History:
            - 2025-10-16 Alex Duan add doc

        """
        pass

    def test_fun_cmp_nvl2(self):
        """ Fun: nvl2()

        1. Check "select nvl2(null, 1, 2)";
        2. Check "select nvl2('x', 1, 2)";
        
        Since: v3.3.0.0

        Labels: common,ci

        History:
            - 2025-10-16 Alex Duan add doc

        """
        pass