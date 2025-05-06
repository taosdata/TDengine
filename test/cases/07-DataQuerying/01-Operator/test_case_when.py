from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestCaseWhen:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_case_when(self):
        """And Or 运算符

        1. 创建多种数据类型的超级表和子表
        2. 写入数据
        3. case when 结果中包含数据列
        4. case when 结果中包含聚合函数

        Catalog:
            - Query:Operator

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/scalar/caseWhen.sim

        """

        tdLog.info(f"======== prepare data")

        tdSql.prepare("db1", drop=True, vgroups=5)
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

        tdLog.info(f"======== case when xx")

        tdSql.query(f"select case when 3 then 4 end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(1, 0, 4)
        tdSql.checkData(2, 0, 4)
        tdSql.checkData(3, 0, 4)

        tdSql.query(f"select case when 0 then 4 end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(f"select case when null then 4 end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(f"select case when 1 then 4+1 end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 5.000000000)
        tdSql.checkData(1, 0, 5.000000000)
        tdSql.checkData(2, 0, 5.000000000)
        tdSql.checkData(3, 0, 5.000000000)

        tdSql.query(f"select case when 1-1 then 0 end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(f"select case when 1+1 then 0 end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(2, 0, 0)
        tdSql.checkData(3, 0, 0)

        tdSql.query(f"select case when 1 then 1-1+2 end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 2.000000000)
        tdSql.checkData(1, 0, 2.000000000)
        tdSql.checkData(2, 0, 2.000000000)
        tdSql.checkData(3, 0, 2.000000000)

        tdSql.query(f"select case when 1 > 0 then 1 < 2 end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)

        tdSql.query(f"select case when 1 > 2 then 1 < 2 end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(f"select case when abs(3) then abs(-1) end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)

        tdSql.query(f"select case when abs(1+1) then abs(-1)+abs(3) end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 4.000000000)
        tdSql.checkData(1, 0, 4.000000000)
        tdSql.checkData(2, 0, 4.000000000)
        tdSql.checkData(3, 0, 4.000000000)

        tdSql.query(f"select case when 0 then 1 else 3 end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 3)

        tdSql.query(f"select case when 0 then 1 when 1 then 0 else 3 end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(2, 0, 0)
        tdSql.checkData(3, 0, 0)

        tdSql.query(
            f"select case when 0 then 1 when 1 then 0 when 2 then 3 end from tba1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(2, 0, 0)
        tdSql.checkData(3, 0, 0)

        tdSql.query(f"select case when 'a' then 'b' when null then 0 end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(f"select case when '2' then 'b' when null then 0 end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "b")
        tdSql.checkData(1, 0, "b")
        tdSql.checkData(2, 0, "b")
        tdSql.checkData(3, 0, "b")

        tdSql.query(f"select case when 0 then 'b' else null end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(f"select case when 0 then 'b' else 2 end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(3, 0, 2)

        tdSql.query(f"select case when sum(2) then sum(2)-sum(1) end from tba1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4.000000000)

        tdSql.query(f"select case when sum(2) then abs(-2) end from tba1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        tdSql.query(f"select case when ts then ts end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2022-09-26 15:15:01")
        tdSql.checkData(1, 0, "2022-09-26 15:15:02")
        tdSql.checkData(2, 0, "2022-09-26 15:15:03")
        tdSql.checkData(3, 0, "2022-09-26 15:15:04")

        tdSql.query(f"select case when f1 then ts end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, "2022-09-26 15:15:02")
        tdSql.checkData(2, 0, "2022-09-26 15:15:03")
        tdSql.checkData(3, 0, None)

        tdSql.query(
            f"select case when f1 then f1 when f1 + 1 then f1 + 1 else f1 is null end from tba1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(3, 0, "true")

        tdSql.query(f"select case when f1 then 3 when ts then ts end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1664176501000)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 1664176504000)

        tdSql.query(f"select case when 3 then f1 end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(3, 0, None)

        tdSql.query(f"select case when f1 then 3 when 1 then 2 end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 2)

        tdSql.query(f"select case when sum(f1) then sum(f1)-abs(-1) end from tba1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5.000000000)

        tdSql.query(
            f"select case when sum(f1) then sum(f1)-abs(f1) end from tba1 group by f1 order by f1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, 0.000000000)
        tdSql.checkData(3, 0, 0.000000000)

        tdSql.query(
            f"select case when f1 then sum(f1) when f1 is not null then 9 else 8 end from tba1 group by f1 order by f1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 8)
        tdSql.checkData(1, 0, 9)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 5)

        tdSql.query(f"select f1 from tba1 where f1 > case when f1 then 0 else 3 end;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 5)

        tdSql.query(f"select f1 from tba1 where ts > case when ts then ts end;")
        tdSql.checkRows(0)

        tdSql.query(
            f"select sum(f1) v,count(f1) from tba1 partition by case when f1 then f1 when 1 then 1 end order by v;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(1, 1, 1)

        tdSql.query(
            f"select case when f1 < 3 then 1 when f1 >= 3 then 2 else 3 end caseWhen, sum(f1),count(f1) from tba1 group by case when f1 < 3 then 1 when f1 >= 3 then 2 else 3 end order by caseWhen;"
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
            f"select f1 from tba1 order by case when f1 <= 0 then 3 when f1 = 1 then 4 when f1 >= 3 then 2 else 1 end desc;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(3, 0, None)

        tdSql.query(
            f"select cast(case f1 when f1 then f1 + 1 else f1 is null end as double) from tba1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1.000000000)
        tdSql.checkData(1, 0, 2.000000000)
        tdSql.checkData(2, 0, 6.000000000)
        tdSql.checkData(3, 0, 1.000000000)

        tdSql.query(
            f"select sum(case f1 when f1 then f1 + 1 else f1 is null end + 1) from tba1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 14.000000000)

        tdSql.query(
            f"select case when f1 < 3 then 1 when f1 >= 3 then 2 else 3 end,sum(f1),count(f1) from tba1 state_window(case when f1 < 3 then 1 when f1 >= 3 then 2 else 3 end);"
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
            f"select f1 from tba1 where case when case when f1 <= 0 then 3 when f1 = 1 then 4 when f1 >= 3 then 2 else 1 end > 2 then 1 else 0 end > 0;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 1)

        tdSql.query(
            f"select case when f1 is not null then case when f1 <= 0 then f1 else f1 * 10 end else -1 end from tba1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 10)
        tdSql.checkData(2, 0, 50)
        tdSql.checkData(3, 0, -1)

        tdLog.info(f"======== case xx when xx")

        tdSql.query(f"select case 3 when 3 then 4 end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(1, 0, 4)
        tdSql.checkData(2, 0, 4)
        tdSql.checkData(3, 0, 4)

        tdSql.query(f"select case 3 when 1 then 4 end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(f"select case 3 when 1 then 4 else 2 end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(3, 0, 2)

        tdSql.query(f"select case 3 when null then 4 when '3' then 1 end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)

        tdSql.query(f"select case '3' when null then 4 when 3 then 1 end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)

        tdSql.query(f"select case null when null then 4 when 3 then 1 end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(f"select case 3.0 when null then 4 when '3' then 1 end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)

        tdSql.query(f"select case f2 when 'a' then 4 when '0' then 1 end from tba1;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(
            f"select case f2 when f1 then f1 when f1 - 1 then f1 else 99 end from tba1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(3, 0, 99)

        tdSql.query(
            f"select case cast(f2 as int) when 0 then f2 when f1 then 11 else ts end from tba1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "a")
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(2, 0, 11)
        tdSql.checkData(3, 0, 1664176504000)

        tdSql.query(
            f"select case f1 + 1 when 1 then 1 when 2 then 2 else 3 end from tba1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 3)

        tdSql.query(
            f"select case f1 when sum(f1) then sum(f1)-abs(f1) end from tba1 group by f1 order by f1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 0.000000000)
        tdSql.checkData(2, 0, 0.000000000)
        tdSql.checkData(3, 0, 0.000000000)

        tdSql.query(
            f"select f1, case sum(f1) when 1 then f1 + 99 when f1 then f1 -99 else f1 end from tba1 group by f1 order by f1;"
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

        tdSql.query(f"select case when 3 then 4 end from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select case when 0 then 4 end from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select case when null then 4 end from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select case when 1 then 4+1 end from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select case when 1-1 then 0 end from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select case when 1+1 then 0 end from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select case when abs(3) then abs(-1) end from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select case when abs(1+1) then abs(-1)+abs(3) end from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select case when 0 then 1 else 3 end from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select case when 0 then 1 when 1 then 0 else 3 end from sta;")
        tdSql.checkRows(20)

        tdSql.query(
            f"select case when 0 then 1 when 1 then 0 when 2 then 3 end from sta;"
        )
        tdSql.checkRows(20)

        tdSql.query(f"select case when 'a' then 'b' when null then 0 end from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select case when '2' then 'b' when null then 0 end from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select case when 0 then 'b' else null end from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select case when 0 then 'b' else 2+abs(-2) end from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select case 3 when 3 then 4 end from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select case 3 when 1 then 4 end from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select case 3 when 1 then 4 else 2 end from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select case 3 when null then 4 when '3' then 1 end from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select case null when null then 4 when 3 then 1 end from sta;")
        tdSql.checkRows(20)

        tdSql.query(f"select case 3.0 when null then 4 when '3' then 1 end from sta;")
        tdSql.checkRows(20)

        tdSql.query(
            f"select f2,case f2 when 'a' then 4 when '0' then 1 end from sta order by f2;"
        )
        tdSql.checkRows(20)

        tdSql.query(
            f"select f2,f1,case f2 when f1 then f1 when f1 - 1 then f1 else 99 end from sta order by f2;"
        )
        tdSql.checkRows(20)

        tdSql.query(
            f"select case cast(f2 as int) when 0 then f2 when f1 then 11 else ts end from sta;"
        )
        tdSql.checkRows(20)

        tdSql.query(
            f"select f1, case f1 + 1 when 1 then 1 when 2 then 2 else 3 end from sta order by f1;"
        )
        tdSql.checkRows(20)

        tdSql.query(
            f"select case f1 when sum(f1) then sum(f1)-abs(f1) end from sta group by f1 order by f1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 0.000000000)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(
            f"select case sum(f1) when 1 then f1 + 99 when f1 then f1 -99 else f1 end from sta group by f1 order by f1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, -99)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 5)

        tdSql.query(
            f"select distinct tbname, case t1 when t2 then t1 else t1 + 100 end from sta order by tbname;"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, None)
        tdSql.checkData(3, 1, 101)
        tdSql.checkData(4, 1, 103)

        tdSql.error(f"select case when sum(f1) then sum(f1)-abs(f1) end from tba1;")

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
            f"select case when c_int > 100 then c_float else c_int end as result from t_test;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 123.45)

        tdSql.query(
            f"select case when c_bigint > 100000 then c_double else c_bigint end as result from t_test;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 678.9)

        tdSql.query(
            f"select case when c_bool then c_bool else c_utinyint end as result from t_test;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query(
            f"select case when c_smallint > 30000 then c_usmallint else c_smallint end as result from t_test;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 65535)

        tdSql.query(
            f"select case when c_binary = 'binary_val' then c_nchar else c_binary end as result from t_test;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '涛思数据')

        tdSql.query(
            f"select case when c_bool then c_int else c_bool end as result from t_test;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 123)

        tdSql.query(
            f"select case when ts > '2022-01-01 00:00:00' then c_bool else ts end as result from t_test;"
        )
        tdSql.checkData(0, 0, 1)

        tdSql.query(
            f"select case when c_double > 100 then c_nchar else c_double end as result from t_test;"
        )
        tdSql.checkData(0, 0, '涛思数据')

        tdSql.query(
            f"select case when c_double > 100 then c_varchar else c_double end as result from t_test;"
        )
        tdSql.checkData(0, 0, "varchar_val")

        tdSql.query(
            f"select case when 1 then 1234567890987654 else 'abcertyuiojhgfddhjgfcvbn' end;"
        )
        tdSql.checkData(0, 0, 1234567890987654)

        tdSql.query(
            f"select case when 0 then 1234567890987654 else 'abcertyuiojhgfddhjgfcvbn' end;"
        )
        tdSql.checkData(0, 0, "abcertyuiojhgfddhjgfcvbn")

        tdSql.query(
            f"select case when 0 then 1234567890987654 else c_nchar end from t_test;"
        )
        tdSql.checkData(0, 0, '涛思数据')

        tdSql.query(
            f"select case when 1 then 1234567890987654 else c_nchar end from t_test;"
        )
        tdSql.checkData(0, 0, 1234567890987654)

        tdSql.query(
            f"select case when 1 then c_varchar else c_varbinary end from t_test;"
        )
        tdLog.info(f"==========> {tdSql.getData(0, 0)}")
        tdSql.checkData(0, 0, b'varchar_val')

        tdSql.error(
            f"select case when ts > '2022-01-01 00:00:00' then c_varchar else c_geometry end as result from t_test;"
        )
        tdSql.error(
            f"select case when ts > '2022-01-01 00:00:00' then c_bool else c_geometry end as result from t_test;"
        )
        tdSql.error(
            f"select case when 0 then tag_id else c_geometry end as result from t_test;"
        )
        tdSql.error(
            f"select case when 0 then tag_id else c_nchar end as result from t_test;"
        )
        tdSql.error(
            f"select case when 0 then tag_id else c_int end as result from t_test;"
        )
        tdSql.error(
            f"select case when 0 then tag_id else c_float end as result from t_test;"
        )
        tdSql.error(
            f"select case when c_double > 100 then c_varbinary else c_geometry end as result from t_test;"
        )
        tdSql.error(
            f"select case when c_bool then c_double else c_varbinary end as result from t_test;"
        )
