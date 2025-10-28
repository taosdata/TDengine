from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck, etool, tdCom
import random
import os
import time
import taos
import subprocess
from faker import Faker

class TestQueryCaseWhen:
    updatecfgDict = {'maxSQLLength':1048576,'debugFlag': 143 ,"querySmaOptimize":1}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
    
    #
    # ---------------- sim case ---------------------
    #    
    def do_sim_case_when(self):
        self.CaseWhen()
        tdStream.dropAllStreamsAndDbs()
        self.Bug3398()
        tdStream.dropAllStreamsAndDbs()
        print("")
        print("do sim case when ...................... [passed]")
        
    def CaseWhen(self):
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

    def Bug3398(self):
        tdLog.info(f'=============== create database')
        tdSql.execute(f"create database test")

        tdLog.info(f'=============== create super table and child table')
        tdSql.execute(f"use test")

        tdSql.execute(f"CREATE STABLE st (day timestamp, c2 int) TAGS (vin binary(32))")

        tdSql.execute(f'insert into test.g using st TAGS ("TAG1") values("2023-05-03 00:00:00.000", 1)')
        tdSql.execute(f'insert into test.t using st TAGS ("TAG1") values("2023-05-03 00:00:00.000", 1)')
        tdSql.execute(f'insert into test.tg using st TAGS ("TAG1") values("2023-05-03 00:00:00.000", 1)')

        tdSql.query(f"select sum(case when t.c2 is NULL then 0 else 1 end + case when t.c2 is NULL then 0 else 1 end), sum(case when t.c2 is NULL then 0 else 1 end + case when t.c2 is NULL then 0 else 1 end + case when t.c2 is NULL then 0 else 1 end) from test.t t, test.g g, test.tg tg where t.day = g.day and t.day = tg.day and t.day between '2021-05-03' and '2023-05-04' and t.vin = 'TAG1' and t.vin = g.vin and t.vin = tg.vin group by t.day;")

        tdLog.info(f'{tdSql.getRows()}) {tdSql.getData(0,0)} {tdSql.getData(0,1)}')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2.000000000)
        tdSql.checkData(0, 1, 3.000000000)

    def init_data1(self):
        self.stable_schema = {
            "columns": {
                "ts": "timestamp",
                "c_null": "int",
                "c_bool": "bool",
                "c_tinyint": "tinyint",
                "c_smallint": "smallint",
                "c_int": "int",
                "c_bigint": "bigint",
                "c_float": "float",
                "c_double": "double",
                "c_varchar": "varchar(16)",
                "c_timestamp": "timestamp",
                "c_nchar": "nchar(16)",
                "c_utinyint": "tinyint unsigned",
                "c_usmallint": "smallint unsigned",
                "c_uint": "int unsigned",
                "c_ubigint": "bigint unsigned",
                "c_varbinary": "varbinary(16)",
                "c_geometry": "geometry(32)"
            },
            "tags": {
                "t_null": "int",
                "t_bool": "bool",
                "t_tinyint": "tinyint",
                "t_smallint": "smallint",
                "t_int": "int",
                "t_bigint": "bigint",
                "t_float": "float",
                "t_double": "double",
                "t_varchar": "varchar(16)",
                "t_timestamp": "timestamp",
                "t_nchar": "nchar(16)",
                "t_utinyint": "tinyint unsigned",
                "t_usmallint": "smallint unsigned",
                "t_uint": "int unsigned",
                "t_ubigint": "bigint unsigned",
                "t_varbinary": "varbinary(16)",
                "t_geometry": "geometry(32)"
            }
        }

    def prepare_data(self):
        # create database
        tdSql.execute("create database test_case_when;")
        tdSql.execute("use test_case_when;")
        # create stable
        columns = ",".join([f"{k} {v}" for k, v in self.stable_schema["columns"].items()])
        tags = ",".join([f"{k} {v}" for k, v in self.stable_schema["tags"].items()])
        st_sql = f"create stable st1 ({columns}) tags ({tags});"
        tdSql.execute(st_sql)
        st_sql_json_tag = f"create stable st2 ({columns}) tags (t json);"
        tdSql.execute(st_sql_json_tag)
        # create child table
        tdSql.execute("create table ct1 using st1 tags(NULL, True, 1, 1, 1, 1, 1.1, 1.11, 'aaaaaaaa', '2021-09-01 00:00:00.000', 'aaaaaaaa', 1, 1, 1, 1, \"0x06\",'POINT(1 1)');")
        tdSql.execute("""create table ct2 using st2 tags('{"name": "test", "location": "beijing"}');""")
        # insert data
        ct1_data = [
            """'2024-10-01 00:00:00.000', NULL, True, 2, 2, 2, 2, 2.2, 2.22, 'bbbbbbbb', '2021-09-01 00:00:00.000', 'bbbbbbbb', 2, 2, 2, 2, "0x07",'POINT(2 2)'""",
            """'2024-10-01 00:00:01.000', NULL, False, 3, 3, 3, 3, 3.3, 3.33, 'cccccccc', '2021-09-01 00:00:00.000', 'cccccccc', 3, 3, 3, 3, "0x08",'POINT(3 3)'""",
            """'2024-10-01 00:00:02.000', NULL, True, 4, 4, 4, 4, 4.4, 4.44, 'dddddddd', '2021-09-01 00:00:00.000', 'dddddddd', 4, 4, 4, 4, "0x09",'POINT(4 4)'""",
            """'2024-10-01 00:00:03.000', NULL, False, 5, 5, 5, 5, 5.5, 5.55, 'eeeeeeee', '2021-09-01 00:00:00.000', 'eeeeeeee', 5, 5, 5, 5, "0x0A",'POINT(5 5)'""",
            """'2024-10-01 00:00:04.000', NULL, True, 6, 6, 6, 6, 6.6, 6.66, 'ffffffff', '2021-09-01 00:00:00.000', 'ffffffff', 6, 6, 6, 6, "0x0B",'POINT(6 6)'""",
            """'2024-10-01 00:00:05.000', NULL, False, 7, 7, 7, 7, 7.7, 7.77, 'gggggggg', '2021-09-01 00:00:00.000', 'gggggggg', 7, 7, 7, 7, "0x0C",'POINT(7 7)'""",
            """'2024-10-01 00:00:06.000', NULL, True, 8, 8, 8, 8, 8.8, 8.88, 'hhhhhhhh', '2021-09-01 00:00:00.000', 'hhhhhhhh', 8, 8, 8, 8, "0x0D",'POINT(8 8)'""",
            """'2024-10-01 00:00:07.000', NULL, False, 9, 9, 9, 9, 9.9, 9.99, 'iiiiiiii', '2021-09-01 00:00:00.000', 'iiiiiiii', 9, 9, 9, 9, "0x0E",'POINT(9 9)'""",
            """'2024-10-01 00:00:08.000', NULL, True, 10, 10, 10, 10, 10.10, 10.1010, 'jjjjjjjj', '2021-09-01 00:00:00.000', 'jjjjjjjj', 10, 10, 10, 10, "0x0F",'POINT(10 10)'""",
            """'2024-10-01 00:00:09.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL"""
        ]
        ct1_insert_sql = "insert into ct1 values(%s);" % "),(".join(ct1_data)
        tdSql.execute(ct1_insert_sql)
        ct2_data = [
            """'2024-10-01 00:00:00.000', NULL, True, 2, 2, 2, 2, 2.2, 2.22, 'bbbbbbbb', '2021-09-01 00:00:00.000', 'bbbbbbbb', 2, 2, 2, 2, "0x07",'POINT(2 2)'""",
            """'2024-10-01 00:00:01.000', NULL, False, 3, 3, 3, 3, 3.3, 3.33, 'cccccccc', '2021-09-01 00:00:00.000', 'cccccccc', 3, 3, 3, 3, "0x08",'POINT(3 3)'""",
            """'2024-10-01 00:00:02.000', NULL, True, 4, 4, 4, 4, 4.4, 4.44, 'dddddddd', '2021-09-01 00:00:00.000', 'dddddddd', 4, 4, 4, 4, "0x09",'POINT(4 4)'""",
            """'2024-10-01 00:00:03.000', NULL, False, 5, 5, 5, 5, 5.5, 5.55, 'eeeeeeee', '2021-09-01 00:00:00.000', 'eeeeeeee', 5, 5, 5, 5, "0x0A",'POINT(5 5)'""",
            """'2024-10-01 00:00:04.000', NULL, True, 6, 6, 6, 6, 6.6, 6.66, 'ffffffff', '2021-09-01 00:00:00.000', 'ffffffff', 6, 6, 6, 6, "0x0B",'POINT(6 6)'""",
            """'2024-10-01 00:00:05.000', NULL, False, 7, 7, 7, 7, 7.7, 7.77, 'gggggggg', '2021-09-01 00:00:00.000', 'gggggggg', 7, 7, 7, 7, "0x0C",'POINT(7 7)'""",
            """'2024-10-01 00:00:06.000', NULL, True, 8, 8, 8, 8, 8.8, 8.88, 'hhhhhhhh', '2021-09-01 00:00:00.000', 'hhhhhhhh', 8, 8, 8, 8, "0x0D",'POINT(8 8)'""",
            """'2024-10-01 00:00:07.000', NULL, False, 9, 9, 9, 9, 9.9, 9.99, 'iiiiiiii', '2021-09-01 00:00:00.000', 'iiiiiiii', 9, 9, 9, 9, "0x0E",'POINT(9 9)'""",
            """'2024-10-01 00:00:08.000', NULL, True, 10, 10, 10, 10, 10.10, 10.1010, 'jjjjjjjj', '2021-09-01 00:00:00.000', 'jjjjjjjj', 10, 10, 10, 10, "0x0F",'POINT(10 10)'""",
            """'2024-10-01 00:00:09.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL"""
        ]
        ct2_insert_sql = "insert into ct2 values(%s);" % "),(".join(ct2_data)
        tdSql.execute(ct2_insert_sql)

    def run_case_when_statements(self):
        tdSql.execute("use test_case_when;")
        tdSql.query("select case when c_null is null then c_null else t_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.queryResult]))
        
        tdSql.query("select case when c_null is not null then c_null else t_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.queryResult]))
        
        tdSql.query("select case when c_bool is null then c_bool else c_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.queryResult]))
        
        tdSql.query("select case when c_bool is not null then c_bool else c_null end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(1,), (0,), (1,), (0,), (1,), (0,), (1,), (0,), (1,), (None,)])

        tdSql.query("select case when c_tinyint is null then c_tinyint else c_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.queryResult]))

        tdSql.query("select case when c_tinyint is not null then c_tinyint else c_null end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_smallint is null then c_smallint else c_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.queryResult]))
        
        tdSql.query("select case when c_smallint is not null then c_smallint else c_null end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])

        tdSql.query("select case when c_int is null then c_int else c_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.queryResult]))

        tdSql.query("select case when c_int is not null then c_int else c_null end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_bigint is null then c_bigint else c_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.queryResult]))
        
        tdSql.query("select case when c_bigint is not null then c_bigint else c_null end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_float is null then c_float else c_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.queryResult]))
        
        tdSql.query("select case when c_float is not null then c_float else c_null end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [('2.2',), ('3.3',), ('4.4',), ('5.5',), ('6.6',), ('7.7',), ('8.8',), ('9.9',), ('10.1',), (None,)])

        tdSql.query("select case when c_double is null then c_double else c_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.queryResult]))

        tdSql.query("select case when c_double is not null then c_double else c_null end from st1;")
        
        assert(tdSql.checkRows(10) and tdSql.queryResult == [('2.22',), ('3.33',), ('4.44',), ('5.55',), ('6.66',), ('7.77',), ('8.88',), ('9.99',), ('10.101',), (None,)])
        
        tdSql.query("select case when c_varchar is null then c_varchar else c_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.queryResult]))
        
        tdSql.query("select case when c_varchar is not null then c_varchar else c_null end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [('bbbbbbbb',), ('cccccccc',), ('dddddddd',), ('eeeeeeee',), ('ffffffff',), ('gggggggg',), ('hhhhhhhh',), ('iiiiiiii',), ('jjjjjjjj',), (None,)])

        tdSql.query("select case when c_nchar is null then c_nchar else c_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.queryResult]))
        
        tdSql.query("select case when c_nchar is not null then c_nchar else c_null end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [('bbbbbbbb',), ('cccccccc',), ('dddddddd',), ('eeeeeeee',), ('ffffffff',), ('gggggggg',), ('hhhhhhhh',), ('iiiiiiii',), ('jjjjjjjj',), (None,)])
        
        tdSql.query("select case when c_utinyint is null then c_utinyint else c_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.queryResult]))
        
        tdSql.query("select case when c_utinyint is not null then c_utinyint else c_null end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_usmallint is null then c_usmallint else c_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.queryResult]))
        
        tdSql.query("select case when c_usmallint is not null then c_usmallint else c_null end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_uint is null then c_uint else c_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.queryResult]))
        
        tdSql.query("select case when c_uint is not null then c_uint else c_null end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_ubigint is null then c_ubigint else c_null end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.queryResult]))
        
        tdSql.query("select case when c_ubigint is not null then c_ubigint else c_null end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [('2',), ('3',), ('4',), ('5',), ('6',), ('7',), ('8',), ('9',), ('10',), (None,)])
        
        tdSql.error("select case when c_varbinary is null then c_varbinary else c_null end from st1;")
        tdSql.error("select case when c_varbinary is not null then c_varbinary else c_null end from st1;")
        
        tdSql.query("select case when c_null is null then NULL else c_bool end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.queryResult]))
        
        tdSql.query("select case when c_null is not null then NULL else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(True,), (False,), (True,), (False,), (True,), (False,), (True,), (False,), (True,), (None,)])
        
        tdSql.query("select case when c_bool=true then NULL else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(None,), (False,), (None,), (False,), (None,), (False,), (None,), (False,), (None,), (None,)])
        
        tdSql.query("select case when c_bool!=true then NULL else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(True,), (None,), (True,), (None,), (True,), (None,), (True,), (None,), (True,), (None,)])
        
        tdSql.query("select case when c_tinyint=2 then c_tinyint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(2,), (0,), (1,), (0,), (1,), (0,), (1,), (0,), (1,), (None,)])
        
        tdSql.query("select case when c_tinyint!=2 then c_tinyint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(1,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])

        tdSql.query("select case when c_smallint=2 then c_smallint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(2,), (0,), (1,), (0,), (1,), (0,), (1,), (0,), (1,), (None,)])

        tdSql.query("select case when c_smallint!=2 then c_smallint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(1,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_int=2 then c_int else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(2,), (0,), (1,), (0,), (1,), (0,), (1,), (0,), (1,), (None,)])
        
        tdSql.query("select case when c_int!=2 then c_int else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(1,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_bigint=2 then c_bigint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(2,), (0,), (1,), (0,), (1,), (0,), (1,), (0,), (1,), (None,)])
        
        tdSql.query("select case when c_bigint!=2 then c_bigint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(1,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_float=2.2 then c_float else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult[1:] == [(0.0,), (1.0,), (0.0,), (1.0,), (0.0,), (1.0,), (0.0,), (1.0,), (None,)])
        
        tdSql.query("select case when c_float!=2.2 then c_float else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult[0] == (1.0,))
        
        tdSql.query("select case when c_double=2.22 then c_double else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(2.22,), (0.0,), (1.0,), (0.0,), (1.0,), (0.0,), (1.0,), (0.0,), (1.0,), (None,)])
        
        tdSql.query("select case when c_double!=2.2 then c_double else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(2.22,), (3.33,), (4.44,), (5.55,), (6.66,), (7.77,), (8.88,), (9.99,), (10.101,), (None,)])
        
        tdSql.query("select case when c_varchar='bbbbbbbb' then c_varchar else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [('bbbbbbbb',), ('false',), ('true',), ('false',), ('true',), ('false',), ('true',), ('false',), ('true',), (None,)])
        
        tdSql.query("select case when c_varchar!='bbbbbbbb' then c_varchar else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [('true',), ('cccccccc',), ('dddddddd',), ('eeeeeeee',), ('ffffffff',), ('gggggggg',), ('hhhhhhhh',), ('iiiiiiii',), ('jjjjjjjj',), (None,)])
        
        tdSql.query("select case when c_timestamp='2021-09-01 00:00:00.000' then c_timestamp else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(1630425600000,), (1630425600000,), (1630425600000,), (1630425600000,), (1630425600000,), (1630425600000,), (1630425600000,), (1630425600000,), (1630425600000,), (None,)])
        
        tdSql.query("select case when c_timestamp!='2021-09-01 00:00:00.000' then c_timestamp else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(1,), (0,), (1,), (0,), (1,), (0,), (1,), (0,), (1,), (None,)])

        tdSql.query("select case when c_nchar='bbbbbbbb' then c_nchar else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [('bbbbbbbb',), ('false',), ('true',), ('false',), ('true',), ('false',), ('true',), ('false',), ('true',), (None,)])

        tdSql.query("select case when c_nchar!='bbbbbbbb' then c_nchar else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [('true',), ('cccccccc',), ('dddddddd',), ('eeeeeeee',), ('ffffffff',), ('gggggggg',), ('hhhhhhhh',), ('iiiiiiii',), ('jjjjjjjj',), (None,)])
        
        tdSql.query("select case when c_utinyint=2 then c_utinyint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(2,), (0,), (1,), (0,), (1,), (0,), (1,), (0,), (1,), (None,)])
        
        tdSql.query("select case when c_utinyint!=2 then c_utinyint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(1,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_usmallint=2 then c_usmallint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(2,), (0,), (1,), (0,), (1,), (0,), (1,), (0,), (1,), (None,)])
        
        tdSql.query("select case when c_usmallint!=2 then c_usmallint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(1,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_uint=2 then c_uint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(2,), (0,), (1,), (0,), (1,), (0,), (1,), (0,), (1,), (None,)])

        tdSql.query("select case when c_uint!=2 then c_uint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(1,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_ubigint=2 then c_ubigint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(2,), (0,), (1,), (0,), (1,), (0,), (1,), (0,), (1,), (None,)])
        
        tdSql.query("select case when c_ubigint!=2 then c_ubigint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(1,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_ubigint=2 then c_ubigint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(2,), (0,), (1,), (0,), (1,), (0,), (1,), (0,), (1,), (None,)])

        tdSql.query("select case when c_ubigint!=2 then c_ubigint else c_bool end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(1,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])

        tdSql.error("select case when c_varbinary='\x30783037' then c_varbinary else c_bool end from st1;")
        tdSql.error("select case when c_varbinary!='\x30783037' then c_varbinary else c_bool end from st1;")
        
        tdSql.query("select case when c_null is null then NULL else c_tinyint end from st1;")
        assert(tdSql.checkRows(10) and all([item[0] is None for item in tdSql.queryResult]))
        
        tdSql.query("select case when c_null is not null then NULL else c_tinyint end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_bool=true then false else c_tinyint end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(0,), (3,), (0,), (5,), (0,), (7,), (0,), (9,), (0,), (None,)])
        
        tdSql.query("select case when c_bool!=true then false else c_tinyint end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(2,), (0,), (4,), (0,), (6,), (0,), (8,), (0,), (10,), (None,)])
        
        tdSql.query("select case when c_smallint=2 then c_smallint else c_tinyint end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_smallint!=2  then c_smallint else c_tinyint end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_int=2 then c_smallint else c_int end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_int!=2  then c_smallint else c_int end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [(2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,), (None,)])
        
        tdSql.query("select case when c_float=2.2 then 387897 else 'test message' end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [('387897',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',)])
        
        tdSql.query("select case when c_double=2.22 then 387897 else 'test message' end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [('387897',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',)])

        tdSql.query("select case when c_varchar='cccccccc' then 'test' when c_varchar='bbbbbbbb' then 'bbbb' else 'test message' end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [('bbbb',), ('test',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',), ('test message',)])
        
        tdSql.query("select case when ts='2024-10-01 00:00:04.000' then 456646546 when ts>'2024-10-01 00:00:04.000' then 'after today' else 'before today or unknow date' end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [('before today or unknow date',), ('before today or unknow date',), ('before today or unknow date',), ('before today or unknow date',), ('456646546',), ('after today',), ('after today',), ('after today',), ('after today',), ('after today',)])

        tdSql.error("select case when c_geometry is null then c_geometry else c_null end from st1;")
        tdSql.error("select case when c_geometry is not null then c_geometry else c_null end from st1;")
        tdSql.error("select case when c_geometry='POINT(2 2)' then c_geometry else c_bool end from st1;")
        tdSql.error("select case when c_geometry!='POINT(2 2)' then c_geometry else c_bool end from st1;")

        tdSql.error("select case when t is null then t else c_null end from st2;")
        tdSql.error("select case when t is not null then t else c_null end from st2;")
        tdSql.error("select case when t->'location'='beijing' then t->'location' else c_bool end from st2;")
        tdSql.error("select case when t->'location'!='beijing' then t->'location' else c_bool end from st1;")

        tdSql.query("select case when c_float!=2.2 then 387897 else 'test message' end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [('test message',), ('387897',), ('387897',), ('387897',), ('387897',), ('387897',), ('387897',), ('387897',), ('387897',), ('test message',)])

        tdSql.query("select case when c_double!=2.22 then 387897 else 'test message' end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [('test message',), ('387897',), ('387897',), ('387897',), ('387897',), ('387897',), ('387897',), ('387897',), ('387897',), ('test message',)])

        tdSql.query("select case c_tinyint when 2 then -2147483648 when 3 then 'three' else '4294967295' end from st1;")
        assert(tdSql.checkRows(10) and tdSql.queryResult == [('-2147483648',), ('three',), ('4294967295',), ('4294967295',), ('4294967295',), ('4294967295',), ('4294967295',), ('4294967295',), ('4294967295',), ('4294967295',)])

        tdSql.query("select case c_float when 2.2 then 9.2233720e+18 when 3.3 then -9.2233720e+18 else 'aa' end from st1;")
        print(tdSql.queryResult)
        assert(tdSql.checkRows(10) and tdSql.queryResult == [('9.223372e+18',), ('-9.223372e+18',), ('aa',), ('aa',), ('aa',), ('aa',), ('aa',), ('aa',), ('aa',), ('aa',)])

        tdSql.query("select case t1.c_int when 2 then 'run' when t1.c_int is null then 'other' else t2.c_varchar end from st1 t1, st2 t2 where t1.ts=t2.ts;")
        print(tdSql.queryResult)
        assert(tdSql.checkRows(10) and tdSql.queryResult == [('run',), ('cccccccc',), ('dddddddd',), ('eeeeeeee',), ('ffffffff',), ('gggggggg',), ('hhhhhhhh',), ('iiiiiiii',), ('jjjjjjjj',), (None,)])

        tdSql.query("select avg(case when c_tinyint>=2 then c_tinyint else c_null end) from st1;")
        assert(tdSql.checkRows(1) and tdSql.queryResult == [(6.0,)])
        
        tdSql.query("select sum(case when c_tinyint>=2 then c_tinyint else c_null end) from st1;")
        assert(tdSql.checkRows(1) and tdSql.queryResult == [(54,)])
        
        tdSql.query("select first(case when c_int >=2 then 'abc' else 0 end) from st1;")
        assert(tdSql.checkRows(1) and tdSql.queryResult == [('abc',)])
        
        tdSql.query("select last(case when c_int >=2 then c_int else 0 end) from st1;")
        assert(tdSql.checkRows(1) and tdSql.queryResult == [(0,)])

    def do_army_case_when(self):
        self.init_data1()
        self.prepare_data()
        self.run_case_when_statements()
        print("do army case when ..................... [passed]")

    #
    # ---------------- system-test ---------------------
    #
    def init_data2(self):
        self.testcasePath = os.path.split(__file__)[0]
        self.testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf %s/%s.sql" % (self.testcasePath, self.testcaseFilename))
        self.db = "case_when"

    def dropandcreateDB_random(self,database,n):
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        tdSql.execute('''drop database if exists %s ;''' %database)
        tdSql.execute('''create database %s keep 36500 ;'''%(database))
        tdSql.execute('''use %s;'''%database)

        tdSql.execute('''create stable %s.stable_1 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);'''%database)
        tdSql.execute('''create stable %s.stable_2 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);'''%database)
        
        for i in range(num_random):
            tdSql.execute('''create table %s.table_%d \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ) ;'''%(database,i))
            tdSql.execute('''create table %s.stable_1_%d using %s.stable_1 tags('stable_1_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(database,i,database,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 
         
            tdSql.execute('''create table %s.stable_%d_a using %s.stable_2 tags('stable_2_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(database,i,database,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 
            tdSql.execute('''create table %s.stable_%d_b using %s.stable_2 tags('stable_2_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(database,i,database,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))

        # insert data
        for i in range(num_random):   
            for j in range(n):         
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
                
                tdSql.execute('''insert into %s.stable_%d_a (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j, fake.random_int(min=0, max=2147483647, step=1), 
                            fake.random_int(min=0, max=9223372036854775807, step=1), 
                            fake.random_int(min=0, max=32767, step=1) , fake.random_int(min=0, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))
            
        tdSql.query("select count(*) from %s.stable_1;" %database)
        tdSql.checkData(0,0,num_random*n)
        tdSql.query("select count(*) from %s.table_0;"%database)
        tdSql.checkData(0,0,n)
        
        
    def users_bug(self,database):    
        sql1 = "select (case when `q_smallint` >0 then 'many--' when `q_smallint`<0 then 'little' end),q_int,loc from %s.stable_1 where tbname = 'stable_1_1' limit 100;" %database
        sql2 = "select (case when `q_smallint` >0 then 'many--' when `q_smallint`<0 then 'little' end),q_int,loc from %s.stable_1_1 limit 100;"  %database
        self.constant_check(database,sql1,sql2,0)
        
        sql1 = "select (case when `q_smallint` >0 then 'many![;;][][]]' when `q_smallint`<0 then 'little' end),q_int,loc from %s.stable_1 where tbname = 'stable_1_1' limit 100;" %database
        sql2 = "select (case when `q_smallint` >0 then 'many![;;][][]]' when `q_smallint`<0 then 'little' end),q_int,loc from %s.stable_1_1 limit 100;"  %database
        self.constant_check(database,sql1,sql2,0)
        
        sql1 = "select (case when sum(q_smallint)=0 then null else sum(q_smallint) end) from %s.stable_1 where tbname = 'stable_1_1' limit 100;"  %database
        sql2 = "select (case when sum(q_smallint)=0 then null else sum(q_smallint) end) from %s.stable_1_1 limit 100;"  %database
        self.constant_check(database,sql1,sql2,0)
        
        #TD-20257
        sql1 = "select tbname,first(ts),q_int,q_smallint,q_bigint,case when q_int <0 then 1 else 0 end from %s.stable_1 where tbname = 'stable_1_1' and ts < now partition by tbname state_window(case when q_int <0 then 1 else 0 end);"  %database
        sql2 = "select tbname,first(ts),q_int,q_smallint,q_bigint,case when q_int <0 then 1 else 0 end from %s.stable_1_1 where ts < now partition by tbname state_window(case when q_int <0 then 1 else 0 end);"  %database
        self.constant_check(database,sql1,sql2,0)
        self.constant_check(database,sql1,sql2,1)
        self.constant_check(database,sql1,sql2,2)
        self.constant_check(database,sql1,sql2,3)
        self.constant_check(database,sql1,sql2,4)
        self.constant_check(database,sql1,sql2,5)
        
        #TD-20260
        sql1 = "select _wstart,avg(q_int),min(q_smallint) from %s.stable_1 where tbname = 'stable_1_1' and ts < now state_window(case when q_smallint <0 then 1 else 0 end);"  %database
        sql2 = "select _wstart,avg(q_int),min(q_smallint) from %s.stable_1_1 where ts < now state_window(case when q_smallint <0 then 1 else 0 end);"  %database
        self.constant_check(database,sql1,sql2,0)
        self.constant_check(database,sql1,sql2,1)
        self.constant_check(database,sql1,sql2,2)

        #TS-5677
        sql1 = "select tbname, _wstart, case when (q_tinyint >= 0) THEN 1 ELSE 0 END status from %s.stable_1  \
            where ts < now partition by tbname  state_window(case when q_tinyint>=0 then 1 else 0 end);"  %database
        sql2 = "select count(*), _wstart, case when (q_tinyint >= 0) THEN 1 ELSE 0 END status from %s.stable_1  \
            where ts < now partition by tbname  state_window(case when q_tinyint>=0 then 1 else 0 end);"  %database
        sql3 = "select tbname, max(q_int), _wstart, case when (q_tinyint >= 0) THEN 1 ELSE 0 END status from %s.stable_1  \
            where ts < now partition by tbname  state_window(case when q_tinyint>=0 then 1 else 0 end);"  %database
        sql3 = "select t_int, tbname, max(q_int), _wstart, case when (q_tinyint >= 0) THEN 1 ELSE 0 END status from %s.stable_1  \
            where ts < now partition by tbname  state_window(case when q_tinyint>=0 then 1 else 0 end);"  %database
        tdSql.query(sql1)
        rows = tdSql.queryRows
        tdSql.query(sql2)
        tdSql.checkRows(rows)
        tdSql.query(sql3)
        tdSql.checkRows(rows)

        sql1 = "select t_int, _wstart, case when (q_tinyint >= 0) THEN 1 ELSE 0 END status from %s.stable_1  \
            where ts < now partition by t_int  state_window(case when q_tinyint>=0 then 1 else 0 end);"  %database
        sql2 = "select count(*), _wstart, case when (q_tinyint >= 0) THEN 1 ELSE 0 END status from %s.stable_1  \
            where ts < now partition by t_int  state_window(case when q_tinyint>=0 then 1 else 0 end);"  %database
        tdSql.query(sql1)
        rows = tdSql.queryRows
        tdSql.query(sql2)
        tdSql.checkRows(rows)

        sql2 = "select t_int, count(*), _wstart, case when (q_tinyint >= 0) THEN 1 ELSE 0 END status from %s.stable_1  \
            where ts < now partition by tbname, t_int  state_window(case when q_tinyint>=0 then 1 else 0 end);"  %database
        sql2 = "select tbname, count(*), _wstart, case when (q_tinyint >= 0) THEN 1 ELSE 0 END status from %s.stable_1  \
            where ts < now partition by tbname, t_int  state_window(case when q_tinyint>=0 then 1 else 0 end);"  %database
        tdSql.query(sql1)
        tdSql.query(sql1)
        rows = tdSql.queryRows
        tdSql.query(sql2)
        tdSql.checkRows(rows)

    def casewhen_list(self):
        a1,a2,a3 = random.randint(-2147483647,2147483647),random.randint(-2147483647,2147483647),random.randint(-2147483647,2147483647)
        casewhen_lists = ['first  case when %d then %d end last' %(a1,a2) ,     #'first  case when 3 then 4 end last' , 
                        'first  case when 0 then %d end last' %(a1),            #'first  case when 0 then 4 end last' ,
                        'first  case when null then %d end last' %(a1) ,        #'first  case when null then 4 end last' ,
                        'first  case when 1 then %d+(%d) end last' %(a1,a2) ,     #'first  case when 1 then 4+1 end last' ,
                        'first  case when %d-(%d) then 0 end last' %(a1,a1) ,     #'first  case when 1-1 then 0 end last' ,
                        'first  case when %d+(%d) then 0 end last' %(a1,a1),      #'first  case when 1+1 then 0 end last' ,  
                        'first  case when 1 then %d-(%d)+(%d) end last' %(a1,a1,a2),  #'first  case when 1 then 1-1+2 end last' ,
                        'first  case when %d > 0 then %d < %d end last'  %(a1,a1,a2),   #'first  case when 1 > 0 then 1 < 2 end last' ,
                        'first  case when %d > %d then %d < %d end last'  %(a1,a2,a1,a2),   #'first  case when 1 > 2 then 1 < 2 end last' ,
                        'first  case when abs(%d) then abs(-(%d)) end last'  %(a1,a2) ,#'first  case when abs(3) then abs(-1) end last' ,
                        'first  case when abs(%d+(%d)) then abs(-(%d))+abs(%d) end last' %(a1,a2,a1,a2) , #'first  case when abs(1+1) then abs(-1)+abs(3) end last' ,
                        'first  case when 0 then %d else %d end last'  %(a1,a2),  #'first  case when 0 then 1 else 3 end last' ,
                        'first  case when 0 then %d when 1 then %d else %d end last'  %(a1,a1,a3),  #'first  case when 0 then 1 when 1 then 0 else 3 end last' ,
                        'first  case when 0 then %d when 1 then %d when 2 then %d end last' %(a1,a1,a3), #'first  case when 0 then 1 when 1 then 0 when 2 then 3 end last' ,
                        'first  case when \'a\' then \'b\' when null then 0 end last' ,   #'first  case when \'a\' then \'b\' when null then 0 end last' ,
                        'first  case when \'%d\' then \'b\' when null then %d end last' %(a1,a2),   #'first  case when \'2\' then \'b\' when null then 0 end last' ,
                        'first  case when \'%d\' then \'b\' else null end last' %(a1), #'first  case when \'0\' then \'b\' else null end last',
                        'first  case when \'%d\' then \'b\' else %d end last' %(a1,a2), #'first  case when \'0\' then \'b\' else 2 end last',
                        'first  case when sum(%d) then sum(%d)-sum(%d) end last'  %(a1,a1,a3), #'first  case when sum(2) then sum(2)-sum(1) end last' ,
                        'first  case when sum(%d) then abs(-(%d)) end last'  %(a1,a2), #'first  case when sum(2) then abs(-2) end last' ,
                        'first  case when q_int then ts end last' ,
                        'first  case when q_int then q_int when q_int + (%d) then q_int + (%d) else q_int is null end last' %(a1,a2) , #'first  case when q_int then q_int when q_int + 1 then q_int + 1 else q_int is null end last' ,
                        'first  case when q_int then %d when ts then ts end last'  %(a1),  #'first  case when q_int then 3 when ts then ts end last' ,
                        'first  case when %d then q_int end last'  %(a1),  #'first  case when 3 then q_int end last' ,
                        'first  case when q_int then %d when %d then %d end last'  %(a1,a1,a3),  #'first  case when q_int then 3 when 1 then 2 end last' ,
                        'first  case when sum(q_int) then sum(q_int)-abs(-(%d)) end last'  %(a1),  #'first  case when sum(q_int) then sum(q_int)-abs(-1) end last' ,
                        'first  case when q_int < %d then %d when q_int >= %d then %d else %d end last' %(a1,a2,a1,a2,a3), #'first  case when q_int < 3 then 1 when q_int >= 3 then 2 else 3 end last' ,
                        'first  cast(case q_int when q_int then q_int + (%d) else q_int is null end as double) last' %(a1), #'first  cast(case q_int when q_int then q_int + 1 else q_int is null end as double) last' ,
                        'first  sum(case q_int when q_int then q_int + (%d) else q_int is null end + (%d)) last'  %(a1,a2), #'first  sum(case q_int when q_int then q_int + 1 else q_int is null end + 1) last' ,
                        'first  case when q_int is not null then case when q_int <= %d then q_int else q_int * (%d) end else -(%d) end last'  %(a1,a1,a3),  #'first  case when q_int is not null then case when q_int <= 0 then q_int else q_int * 10 end else -1 end last' ,
                        'first  case %d when %d then %d end last'  %(a1,a2,a3),  # 'first  case 3 when 3 then 4 end last' ,
                        'first  case %d when %d then %d end last'  %(a1,a2,a3),  # 'first  case 3 when 1 then 4 end last' ,
                        'first  case %d when %d then %d else %d end last'  %(a1,a1,a2,a3),  # 'first  case 3 when 1 then 4 else 2 end last' ,
                        'first  case %d when null then %d when \'%d\' then %d end last' %(a1,a1,a2,a3) , # 'first  case 3 when null then 4 when \'3\' then 1 end last' ,
                        'first  case \'%d\' when null then %d when %d then %d end last'  %(a1,a1,a2,a3), # 'first  case \'3\' when null then 4 when 3 then 1 end last' ,
                        'first  case null when null then %d when %d then %d end last' %(a1,a2,a3), # 'first  case null when null then 4 when 3 then 1 end last' ,
                        'first  case %d.0 when null then %d when \'%d\' then %d end last' %(a1,a1,a2,a3) ,  # 'first  case 3.0 when null then 4 when \'3\' then 1 end last' ,
                        'first  case q_double when \'a\' then %d when \'%d\' then %d end last' %(a1,a2,a3) , # 'first  case q_double when \'a\' then 4 when \'0\' then 1 end last' ,
                        'first  case q_double when q_int then q_int when q_int - (%d) then q_int else %d end last' %(a1,a2),  # 'first  case q_double when q_int then q_int when q_int - 1 then q_int else 99 end last' ,
                        'first  case cast(q_double as int) when %d then q_double when q_int then %d else ts end last' %(a1,a2), #'first  case cast(q_double as int) when 0 then q_double when q_int then 11 else ts end last' ,
                        'first  case q_int + (%d) when %d then %d when %d then %d else %d end last'  %(a1,a2,a3,a1,a2,a3), #'first  case q_int + 1 when 1 then 1 when 2 then 2 else 3 end last' ,
                        'first  case when \'a\' then \'b\' when null then %d end last'  %(a1), # 'first  case when \'a\' then \'b\' when null then 0 end last' ,
                        'first  case when \'%d\' then \'b\' when null then %d end last'  %(a1,a2), # 'first  case when \'2\' then \'b\' when null then 0 end last' ,
                        'first  case when %d then \'b\' else null end last'  %(a1), # 'first  case when 0 then \'b\' else null end last' ,
                        'first  case when %d then \'b\' else %d+abs(%d) end last'  %(a1,a2,a3), # 'first  case when 0 then \'b\' else 2+abs(-2) end last' ,
                        'first  case when %d then %d end last'  %(a1,a2), # 'first  case when 3 then 4 end last' ,
                        'first  case when %d then %d end last'  %(a1,a2), # 'first  case when 0 then 4 end last' ,
                        'first  case when null then %d end last'  %(a1), # 'first  case when null then 4 end last' ,
                        'first  case when %d then %d+(%d) end last'  %(a1,a2,a3), # 'first  case when 1 then 4+1 end last' ,
                        'first  case when %d-(%d) then %d end last'  %(a1,a2,a3), # 'first  case when 1-1 then 0 end last' ,
                        'first  case when %d+(%d) then %d end last'  %(a1,a2,a3), # 'first  case when 1+1 then 0 end last' ,
                        'first  case when abs(%d) then abs(%d) end last' %(a1,a2), # 'first  case when abs(3) then abs(-1) end last' ,
                        'first  case when abs(%d+(%d)) then abs(%d)+abs(%d) end last'  %(a1,a2,a3,a1), # 'first  case when abs(1+1) then abs(-1)+abs(3) end last' ,
                        'first  case when %d then %d else %d end last' %(a1,a2,a3), # 'first  case when 0 then 1 else 3 end last' ,
                        'first  case when %d then %d when %d then %d else %d end last' %(a1,a2,a3,a1,a2), # 'first  case when 0 then 1 when 1 then 0 else 3 end last' ,
                        'first  case when %d then %d when %d then %d when %d then %d end last' %(a1,a2,a3,a1,a2,a3), # 'first  case when 0 then 1 when 1 then 0 when 2 then 3 end last' ,
                        'first  case %d when %d then %d end last'  %(a1,a1,a3),  # 'first  case 3 when 3 then 4 end last' ,
                        'first  case %d when %d then %d end last'  %(a1,a2,a3),  # 'first  case 3 when 1 then 4 end last' ,
                        'first  case %d when %d then %d else %d end last'  %(a1,a2,a3,a1),  # 'first  case 3 when 1 then 4 else 2 end last' ,
                        'first  case %d when null then %d when \'%d\' then %d end last'  %(a1,a2,a1,a3),  # 'first  case 3 when null then 4 when \'3\' then 1 end last' ,
                        'first  case null when null then %d when %d then %d end last'  %(a1,a2,a3),  # 'first  case null when null then 4 when 3 then 1 end last' ,
                        'first  case %d.0 when null then %d when \'%d\' then %d end last' %(a1,a2,a1,a3),  # 'first  case 3.0 when null then 4 when \'3\' then 1 end last' ,
                        'first  q_double,case q_double when \'a\' then %d when \'%d\' then %d end last' %(a1,a2,a3), #'first  q_double,case q_double when \'a\' then 4 when \'0\' then 1 end last' ,
                        'first  case null when null then %d when %d then %d end last'  %(a1,a2,a3), #'first  case null when null then 4 when 3 then 1 end last' ,
                        'first  q_double,q_int,case q_double when q_int then q_int when q_int - (%d ) then q_int else %d  end last'  %(a1,a2), # 'first  q_double,q_int,case q_double when q_int then q_int when q_int - 1 then q_int else 99 end last' ,
                        'first  case cast(q_double as int) when %d then q_double when q_int then %d  else ts end last'  %(a1,a2), # 'first  case cast(q_double as int) when 0 then q_double when q_int then 11 else ts end last' ,
                        'first  q_int, case q_int + (%d) when %d then %d when %d then %d else %d end last' %(a1,a1,a1,a2,a2,a3), #'first  q_int, case q_int + 1 when 1 then 1 when 2 then 2 else 3 end last' ,
                        'first  distinct loc, case t_int when t_bigint then t_ts else t_smallint + (%d) end last' %(a1), #'first  distinct loc, case t_int when t_bigint then t_ts else t_smallint + 100 end last' ,
                        ]
        #num = len(casewhen_lists)
        
        casewhen_list = str(random.sample(casewhen_lists,50)).replace("[","").replace("]","").replace("'first","").replace("last'","").replace("\"first","").replace("last\"","")
        
        return casewhen_list
    
    def base_case(self,database):
        for i in range(30):
            cs = self.casewhen_list().split(',')[i] 
            sql1 = "select %s from %s.stable_1 where tbname = 'stable_1_1';" % (cs ,database)
            sql2 = "select %s from %s.stable_1_1 ;" % (cs ,database)
            self.constant_check(database,sql1,sql2,0)       

    def state_window_list(self):
        a1,a2,a3 = random.randint(-2147483647,2147483647),random.randint(-2147483647,2147483647),random.randint(-2147483647,2147483647)
        state_window_lists = ['first  case when %d then %d end last' %(a1,a2) ,     #'first  case when 3 then 4 end last' , 
                        'first  case when 0 then %d end last' %(a1),            #'first  case when 0 then 4 end last' ,
                        'first  case when null then %d end last' %(a1) ,        #'first  case when null then 4 end last' ,
                        'first  case when %d-(%d) then 0 end last' %(a1,a1) ,     #'first  case when 1-1 then 0 end last' ,
                        'first  case when %d+(%d) then 0 end last' %(a1,a1),      #'first  case when 1+1 then 0 end last' ,  
                        'first  case when %d > 0 then %d < %d end last'  %(a1,a1,a2),   #'first  case when 1 > 0 then 1 < 2 end last' ,
                        'first  case when %d > %d then %d < %d end last'  %(a1,a2,a1,a2),   #'first  case when 1 > 2 then 1 < 2 end last' ,
                        'first  case when abs(%d) then abs(-(%d)) end last'  %(a1,a2) ,#'first  case when abs(3) then abs(-1) end last' ,
                        'first  case when 0 then %d else %d end last'  %(a1,a2),  #'first  case when 0 then 1 else 3 end last' ,
                        'first  case when 0 then %d when 1 then %d else %d end last'  %(a1,a1,a3),  #'first  case when 0 then 1 when 1 then 0 else 3 end last' ,
                        'first  case when 0 then %d when 1 then %d when 2 then %d end last' %(a1,a1,a3), #'first  case when 0 then 1 when 1 then 0 when 2 then 3 end last' ,
                        'first  case when \'a\' then \'b\' when null then 0 end last' ,   #'first  case when \'a\' then \'b\' when null then 0 end last' ,
                        'first  case when \'%d\' then \'b\' when null then %d end last' %(a1,a2) ,   #'first  case when \'2\' then \'b\' when null then 0 end last' ,
                        'first  case when \'%d\' then \'b\' else null end last' %(a1), #'first  case when \'0\' then \'b\' else null end last',
                        'first  case when \'%d\' then \'b\' else %d end last' %(a1,a2), #'first  case when \'0\' then \'b\' else 2 end last',
                        'first  case when q_int then q_int when q_int + (%d) then cast(q_int + (%d) as int) else q_int is null end last' %(a1,a2) , #'first  case when q_int then q_int when q_int + 1 then q_int + 1 else q_int is null end last' ,
                        'first  case when q_int then %d when ts then cast(ts as int) end last'  %(a1),  #'first  case when q_int then 3 when ts then ts end last' ,
                        'first  case when %d then q_int end last'  %(a1),  #'first  case when 3 then q_int end last' ,
                        'first  case when q_int then %d when %d then %d end last'  %(a1,a1,a3),  #'first  case when q_int then 3 when 1 then 2 end last' ,
                        'first  case when q_int < %d then %d when q_int >= %d then %d else %d end last' %(a1,a2,a1,a2,a3), #'first  case when q_int < 3 then 1 when q_int >= 3 then 2 else 3 end last' ,
                        'first  case when q_int is not null then case when q_int <= %d then q_int else cast(q_int * (%d) as int) end else -(%d) end last'  %(a1,a1,a3),  #'first  case when q_int is not null then case when q_int <= 0 then q_int else q_int * 10 end else -1 end last' ,
                        'first  case %d when %d then %d end last'  %(a1,a2,a3),  # 'first  case 3 when 3 then 4 end last' ,
                        'first  case %d when %d then %d end last'  %(a1,a2,a3),  # 'first  case 3 when 1 then 4 end last' ,
                        'first  case %d when %d then %d else %d end last'  %(a1,a1,a2,a3),  # 'first  case 3 when 1 then 4 else 2 end last' ,
                        'first  case %d when null then %d when \'%d\' then %d end last' %(a1,a1,a2,a3) , # 'first  case 3 when null then 4 when \'3\' then 1 end last' ,
                        'first  case \'%d\' when null then %d when %d then %d end last'  %(a1,a1,a2,a3), # 'first  case \'3\' when null then 4 when 3 then 1 end last' ,
                        'first  case null when null then %d when %d then %d end last' %(a1,a2,a3), # 'first  case null when null then 4 when 3 then 1 end last' ,
                        'first  case %d.0 when null then %d when \'%d\' then %d end last' %(a1,a1,a2,a3) ,  # 'first  case 3.0 when null then 4 when \'3\' then 1 end last' ,
                        'first  case q_double when \'a\' then %d when \'%d\' then %d end last' %(a1,a2,a3) , # 'first  case q_double when \'a\' then 4 when \'0\' then 1 end last' ,
                        'first  case q_double when q_int then q_int when q_int - (%d) then q_int else %d end last' %(a1,a2),  # 'first  case q_double when q_int then q_int when q_int - 1 then q_int else 99 end last' ,
                        'first  case q_int + (%d) when %d then %d when %d then %d else %d end last'  %(a1,a2,a3,a1,a2,a3), #'first  case q_int + 1 when 1 then 1 when 2 then 2 else 3 end last' ,
                        'first  case when \'a\' then \'b\' when null then %d end last'  %(a1), # 'first  case when \'a\' then \'b\' when null then 0 end last' ,
                        'first  case when \'%d\' then \'b\' when null then %d end last'  %(a1,a2), # 'first  case when \'2\' then \'b\' when null then 0 end last' ,
                        'first  case when %d then \'b\' else null end last'  %(a1), # 'first  case when 0 then \'b\' else null end last' ,
                        'first  case when %d then \'b\' else cast(%d+abs(%d) as int) end last'  %(a1,a2,a3), # 'first  case when 0 then \'b\' else 2+abs(-2) end last' ,
                        'first  case when %d then %d end last'  %(a1,a2), # 'first  case when 3 then 4 end last' ,
                        'first  case when %d then %d end last'  %(a1,a2), # 'first  case when 0 then 4 end last' ,
                        'first  case when null then %d end last'  %(a1), # 'first  case when null then 4 end last' ,
                        'first  case when %d then cast(%d+(%d) as int) end last'  %(a1,a2,a3), # 'first  case when 1 then 4+1 end last' ,
                        'first  case when %d-(%d) then %d end last'  %(a1,a2,a3), # 'first  case when 1-1 then 0 end last' ,
                        'first  case when %d+(%d) then %d end last'  %(a1,a2,a3), # 'first  case when 1+1 then 0 end last' ,
                        'first  case when abs(%d) then abs(%d) end last' %(a1,a2), # 'first  case when abs(3) then abs(-1) end last' ,
                        'first  case when abs(%d+(%d)) then cast(abs(%d)+abs(%d) as int) end last'  %(a1,a2,a3,a1), # 'first  case when abs(1+1) then abs(-1)+abs(3) end last' ,
                        'first  case when %d then %d else %d end last' %(a1,a2,a3), # 'first  case when 0 then 1 else 3 end last' ,
                        'first  case when %d then %d when %d then %d else %d end last' %(a1,a2,a3,a1,a2), # 'first  case when 0 then 1 when 1 then 0 else 3 end last' ,
                        'first  case when %d then %d when %d then %d when %d then %d end last' %(a1,a2,a3,a1,a2,a3), # 'first  case when 0 then 1 when 1 then 0 when 2 then 3 end last' ,
                        'first  case %d when %d then %d end last'  %(a1,a1,a3),  # 'first  case 3 when 3 then 4 end last' ,
                        'first  case %d when %d then %d end last'  %(a1,a2,a3),  # 'first  case 3 when 1 then 4 end last' ,
                        'first  case %d when %d then %d else %d end last'  %(a1,a2,a3,a1),  # 'first  case 3 when 1 then 4 else 2 end last' ,
                        'first  case %d when null then %d when \'%d\' then %d end last'  %(a1,a2,a1,a3),  # 'first  case 3 when null then 4 when \'3\' then 1 end last' ,
                        'first  case null when null then %d when %d then %d end last'  %(a1,a2,a3),  # 'first  case null when null then 4 when 3 then 1 end last' ,
                        'first  case %d.0 when null then %d when \'%d\' then %d end last' %(a1,a2,a1,a3),  # 'first  case 3.0 when null then 4 when \'3\' then 1 end last' ,
                        'first  case null when null then %d when %d then %d end last'  %(a1,a2,a3), #'first  case null when null then 4 when 3 then 1 end last' ,
                        'first  q_int, case q_int + (%d) when %d then %d when %d then %d else %d end last' %(a1,a1,a1,a2,a2,a3), #'first  q_int, case q_int + 1 when 1 then 1 when 2 then 2 else 3 end last' ,
                        ]
        
        state_window_list = str(random.sample(state_window_lists,50)).replace("[","").replace("]","").replace("'first","").replace("last'","").replace("\"first","").replace("last\"","")
        
        return state_window_list
           
    def state_window_case(self,database):    
        for i in range(30):
            cs = self.state_window_list().split(',')[i] 
            sql1 = "select _wstart,avg(q_int),min(q_smallint) from %s.stable_1 where tbname = 'stable_1_1' state_window(%s);" % (database,cs)
            sql2 = "select _wstart,avg(q_int),min(q_smallint) from %s.stable_1_1 state_window(%s) ;" % (database,cs)
            self.constant_check(database,sql1,sql2,0)
            self.constant_check(database,sql1,sql2,1)
            self.constant_check(database,sql1,sql2,2)               
        
    def constant_check(self,database,sql1,sql2,column):   
        #column =0 代表0列， column = n代表n-1列 
        tdLog.info("\n=============sql1:(%s)___sql2:(%s) ====================\n" %(sql1,sql2)) 
        tdSql.query(sql1) 
        queryRows = len(tdSql.queryResult) 
      
        for i in range(queryRows):
            tdSql.query(sql1) 
            sql1_value = tdSql.getData(i,column)
            tdSql.execute(" flush database %s;" %database)
            tdSql.query(sql2) 
            sql2_value = tdSql.getData(i,column)
            self.value_check(sql1_value,sql2_value)       
                      
    def value_check(self,base_value,check_value):
        if base_value==check_value:
            tdLog.info(f"checkEqual success, base_value={base_value},check_value={check_value}") 
        else :
            tdLog.exit(f"checkEqual error, base_value=={base_value},check_value={check_value}") 
                            
    def do_system_test_case_when(self):
        self.init_data2()
        fake = Faker('zh_CN')
        fake_data =  fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1)
        fake_float = fake.pyfloat()
        fake_str = fake.pystr()
        
        startTime = time.time()  
                  
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename)) 
        self.dropandcreateDB_random("%s" %self.db, 10)
        self.users_bug("%s" %self.db)
        self.base_case("%s" %self.db)
        self.state_window_case("%s" %self.db)
        
        #taos -f sql 
        print("taos -f sql start!")
        taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
        _ = subprocess.check_output(taos_cmd1, shell=True)
        print("taos -f sql over!")     
                

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))
        
        print("do system-test when ................... [passed]")


    #
    # ---------------- main ---------------------
    #
    def test_query_case_when(self):
        """Query case when basic

        1. Using in data columns and scalar functions within SELECT statements
        2. Using in data columns within WHERE conditions
        3. Using in data columns within GROUP BY statements
        4. Using in data columns within STATE WINDOW
        5. Using in aggregate functions while including the IS NULL operator

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/scalar/caseWhen.sim
            - 2025-5-8 Simon Guan Migrated from tsim/query/bug3398.sim
            - 2025-10-21 Alex Duan Migrated from uncatalog/army/query/test_case_when.py
            - 2025-10-21 Alex Duan Migrated from uncatalog/system-test/2-query/test_case_when.py

        """
        self.do_sim_case_when()
        self.do_army_case_when()
        self.do_system_test_case_when()
