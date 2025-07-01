###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import sys
import taos
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        
    def td29092(self, dbname="db"):
        tdSql.execute(f'use {dbname}')
        tdSql.execute('CREATE STABLE `st` (`ts` TIMESTAMP, `v1` INT) TAGS (`t1` INT);')
        tdSql.execute('CREATE STABLE `st2` (`ts` TIMESTAMP, `v1` INT) TAGS (`t1` INT);')
        tdSql.execute('CREATE TABLE `t1` USING `st` (`t1`) TAGS (1);')
        tdSql.execute('CREATE TABLE `t2` USING `st` (`t1`) TAGS (2);')
        tdSql.execute('CREATE TABLE `t21` USING `st2` (`t1`) TAGS (21);')
        tdSql.execute('CREATE TABLE `nt` (`ts` TIMESTAMP, `v1` INT);')
        
        now_time = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)
        for i in range(3):
            tdSql.execute(
                f"insert into {dbname}.t1 values ( { now_time + i * 1000 }, {i} )"
            )
            tdSql.execute(
                f"insert into {dbname}.t2 values ( { now_time + i * 1000 }, {i} )"
            )
            tdSql.execute(
                f"insert into {dbname}.nt values ( { now_time + i * 1000 }, {i} )"
            )
        
        tdLog.debug(f"--------------  step1:  normal table test   ------------------")
        tdSql.query("select tbname, count(*) from nt;")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "nt")
        tdSql.checkData(0, 1, 3) 
  
        tdSql.query("select nt.tbname, count(*) from nt;")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "nt")
        tdSql.checkData(0, 1, 3) 
        
        tdSql.query("select tbname, count(*) from nt group by tbname")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "nt")
        tdSql.checkData(0, 1, 3) 
        
        tdSql.query("select nt.tbname, count(*) from nt group by tbname")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "nt")
        tdSql.checkData(0, 1, 3) 

        tdSql.query("select nt.tbname, count(*) from nt group by nt.tbname")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "nt")
        tdSql.checkData(0, 1, 3) 

        tdLog.debug(f"--------------  step2:  system table test   ------------------")
        tdSql.query("select tbname, count(*) from information_schema.ins_dnodes")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        tdSql.query("select ins_dnodes.tbname, count(*) from information_schema.ins_dnodes")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        
        tdSql.query("select tbname, count(*) from information_schema.ins_dnodes group by tbname")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        
        tdSql.query("select ins_dnodes.tbname, count(*) from information_schema.ins_dnodes group by tbname")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        
        tdSql.query("select ins_dnodes.tbname, count(*) from information_schema.ins_dnodes group by ins_dnodes.tbname")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        
        tdLog.debug(f"--------------  step3:  subtable test   ------------------")
        tdSql.query("select tbname, count(*) from t1")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)

        tdSql.query("select t1.tbname, count(*) from t1")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)
        
        tdSql.query("select tbname, count(*) from t1 group by tbname")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)
        
        tdSql.query("select t1.tbname, count(*) from t1 group by tbname")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)
        
        tdSql.query("select t1.tbname, count(*) from t1 group by t1.tbname")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)
        
        tdSql.error("select t1.tbname, count(*) from t2 group by t1.tbname")
        tdSql.error("select t1.tbname, count(*) from t1 group by t2.tbname")
        tdSql.error("select t2.tbname, count(*) from t1 group by t1.tbname")
        
        tdLog.debug(f"--------------  step4:  super table test   ------------------")
        tdSql.query("select tbname, count(*) from st group by tbname")  
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 3)
        
        tdSql.query("select tbname, count(*) from st partition by tbname")  
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 3)
        
        tdSql.query("select ts, t1 from st where st.tbname=\"t1\"")  
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 1)
        
        tdSql.query("select tbname, ts from st where tbname=\"t2\"")  
        tdSql.checkRows(3)
        
        tdSql.query("select tbname, ts from st where tbname=\"t2\" order by tbname")  
        tdSql.checkRows(3)
        
        tdSql.query("select tbname, ts from st where tbname=\"t2\" order by st.tbname")  
        tdSql.checkRows(3)
        
        tdSql.query("select tbname, count(*) from st where tbname=\"t2\" group by tbname order by tbname")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)
        
        tdSql.query("select tbname, count(*) from st group by tbname order by tbname")  
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 3)
        
        tdSql.query("select tbname, count(*) from st group by st.tbname order by st.tbname")  
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 3)

        tdLog.debug(f"--------------  step4:  join test   ------------------")
        tdSql.query("select t1.tbname, t2.tbname from t1, t2 where t1.ts=t2.ts and t1.tbname!=t2.tbname")  
        tdSql.checkRows(3)
        
        tdSql.query("select t1.tbname, t2.tbname from t1, t2 where t1.ts=t2.ts and t1.tbname!=t2.tbname order by t1.tbname")  
        tdSql.checkRows(3)
        
        tdSql.query("select st.tbname, st2.tbname from st, st2 where st.ts=st2.ts and st.tbname!=st2.tbname order by st.tbname")  
        tdSql.checkRows(0)
        
        tdSql.execute(f"insert into t21 values ( { now_time + 1000 }, 1 )")
        tdSql.query("select st.tbname, st2.tbname from st, st2 where st.ts=st2.ts and st.tbname!=st2.tbname order by st.tbname")  
        tdSql.checkRows(2)
        
        tdSql.query("select t1.tbname, st2.tbname from t1, st2 where t1.ts=st2.ts and t1.tbname!=st2.tbname order by t1.tbname")  
        tdSql.checkRows(1)
        
        tdSql.query("select nt.ts, st.tbname from nt, st where nt.ts=st.ts order by st.tbname")  
        tdSql.checkRows(6)
        
        tdSql.query("select nt.ts, t1.tbname from nt, t1 where nt.ts=t1.ts order by t1.tbname")  
        tdSql.checkRows(3)

    def ts6532(self, dbname="db"):
        tdSql.execute(f'use {dbname}')
        tdSql.execute('CREATE STABLE ```s``t``` (`ts` TIMESTAMP, ```v1``` INT) TAGS (```t``1``` INT);')
        tdSql.execute('CREATE STABLE ```s``t2``` (`ts` TIMESTAMP, ```v1``` INT) TAGS (```t``1``` INT);')
        tdSql.execute('CREATE TABLE ```t1``` USING db.```s``t``` TAGS (1);')
        tdSql.execute('CREATE TABLE `t2``` USING ```s``t``` (```t``1```) TAGS (2);')
        tdSql.execute('CREATE TABLE ```t21` USING ```s``t2``` (```t``1```) TAGS (21);')
        tdSql.execute('CREATE TABLE ```n``t``` (```t``s``` TIMESTAMP, ```v1` INT);')

        now_time = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)
        for i in range(3):
            tdSql.execute(f"insert into {dbname}.```t1``` values({now_time + i * 1000}, {i})")
            tdSql.execute(f"insert into {dbname}.`t2``` values({now_time + i * 1000}, {i})")
            tdSql.execute(f"insert into {dbname}.```n``t``` values ({now_time + i * 1000}, {i})")

        tdLog.debug(f"--------------  step1:  normal table test   ------------------")
        tdSql.query("select tbname, count(*) from ```n``t```;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`n`t`")
        tdSql.checkData(0, 1, 3)

        tdSql.query("select ```n``t```.tbname, count(*) from ```n``t```;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`n`t`")
        tdSql.checkData(0, 1, 3)

        tdSql.query("select ```n``t```.tbname, count(*) from ```n``t``` group by ```n``t```.tbname")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`n`t`")
        tdSql.checkData(0, 1, 3)

        tdLog.debug(f"--------------  step3:  child table test   ------------------")
        tdSql.query("select tbname, count(*) from ```t1```")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)

        tdSql.query("select ```t1```.tbname, count(*) from ```t1```")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)

        tdSql.query("select tbname, count(*) from ```t1``` group by tbname")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)

        tdSql.query("select ```t1```.tbname, count(*) from ```t1``` group by tbname")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)

        tdSql.query("select ```t1```.tbname, count(*) from ```t1``` group by ```t1```.tbname")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)
        tdSql.error("select ```t1```.tbname, count(*) from `t2``` group by ```t1```.tbname")
        tdSql.error("select ```t1```.tbname, count(*) from ```t1``` group by `t2```.tbname")
        tdSql.error("select `t2```.tbname, count(*) from ```t1``` group by ```t1```.tbname")
        tdLog.debug(f"--------------  step4:  super table test   ------------------")
        tdSql.query("select tbname, count(*) from ```s``t``` group by tbname")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 3)
        tdSql.query("select tbname, count(*) from ```s``t``` partition by tbname")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 3)
        tdSql.query("select ts, ```t``1``` from ```s``t``` where ```s``t```.tbname=\"`t1`\"")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 1)
        tdSql.query("select tbname, ts from ```s``t``` where tbname=\"t2`\"")
        tdSql.checkRows(3)
        tdSql.query("select tbname, ts from ```s``t``` where tbname=\"t2`\" order by tbname")
        tdSql.checkRows(3)
        tdSql.query("select tbname, ts from ```s``t``` where tbname=\"t2`\" order by ```s``t```.tbname")
        tdSql.checkRows(3)
        tdSql.query("select tbname, count(*) from ```s``t``` where tbname=\"t2`\" group by tbname order by tbname")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)
        tdSql.query("select tbname, count(*) from ```s``t``` group by tbname order by tbname")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 3)
        tdSql.query("select tbname, count(*) from ```s``t``` group by ```s``t```.tbname order by ```s``t```.tbname")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 3)

        tdLog.debug(f"--------------  step4:  join test   ------------------")
        tdSql.query("select ```t1```.tbname, `t2```.tbname from ```t1```, `t2``` where ```t1```.ts=`t2```.ts and ```t1```.tbname!=`t2```.tbname")
        tdSql.checkRows(3)

        tdSql.query("select ```t1```.tbname, `t2```.tbname from ```t1```, `t2``` where ```t1```.ts=`t2```.ts and ```t1```.tbname!=`t2```.tbname order by ```t1```.tbname")
        tdSql.checkRows(3)

        tdSql.query("select ```s``t```.tbname, ```s``t2```.tbname from ```s``t```, ```s``t2``` where ```s``t```.ts=```s``t2```.ts and ```s``t```.tbname!=```s``t2```.tbname order by ```s``t```.tbname")
        tdSql.checkRows(0)

        tdSql.execute(f"insert into ```t21` values ( { now_time + 1000 }, 1 )")
        tdSql.query("select ```s``t```.tbname, ```s``t2```.tbname from ```s``t```, ```s``t2``` where ```s``t```.ts=```s``t2```.ts and ```s``t```.tbname!=```s``t2```.tbname order by ```s``t```.tbname")
        tdSql.checkRows(2)
        
        tdSql.query("select ```t1```.tbname, ```s``t2```.tbname from ```t1```, ```s``t2``` where ```t1```.ts=```s``t2```.ts and ```t1```.tbname!=```s``t2```.tbname order by ```t1```.tbname")
        tdSql.checkRows(1)
        
        tdSql.query("select ```n``t```.```t``s```, ```s``t```.tbname from ```n``t```, ```s``t``` where ```n``t```.```t``s```=```s``t```.`ts` order by ```s``t```.tbname")
        tdSql.checkRows(6)
        
        tdSql.query("select ```n``t```.```t``s```, ```t1```.tbname from ```n``t```, ```t1``` where ```n``t```.```t``s```=```t1```.`ts` order by ```t1```.tbname")
        tdSql.checkRows(3)

        tdLog.debug(f"--------------  step5:  auto create table/show create table/drop/recreate with show result  ------------------")
        tdSql.execute('INSERT INTO `t30``` USING ```s``t``` (```t``1```) TAGS (30) VALUES(now+30s,30);')
        tdSql.execute('INSERT INTO ```t31` USING ```s``t``` (```t``1```) TAGS (31) (`ts`,```v1```) VALUES(now+31s,31);')
        tdSql.execute('INSERT INTO `t3``2` USING ```s``t``` (```t``1```) TAGS (32) (```v1```,`ts`) VALUES(32,now+32s);')
        tdSql.execute('INSERT INTO ```t3``3``` USING ```s``t``` (```t``1```) TAGS (33) (```v1```,ts) VALUES(33,now+33s);')
        tdSql.execute('INSERT INTO `````t3````4````` USING ```s``t``` (```t``1```) TAGS (34) (ts,```v1```) VALUES(now+34s,34);')
        tdSql.query("select tbname, count(*) from ```s``t``` partition by tbname")  
        tdSql.checkRows(7)
        self.column_dict = {'`t30```':'t30`','```t31`':'`t31','`t3``2`':'t3`2','```t3``3```':'`t3`3`','`````t3````4`````':'``t3``4``'}
        self.show_create_result = []
        for key, value in self.column_dict.items():
            tdSql.query(f"select tbname, count(*) from {key}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, value)
            tdSql.checkData(0, 1, 1)
            tdSql.query(f"select {key}.tbname, count(*) from {key}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, value)
            tdSql.checkData(0, 1, 1)
            tdSql.query(f"select tbname, count(*) from {key} group by tbname")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, value)
            tdSql.checkData(0, 1, 1)
            tdSql.query(f"select {key}.tbname, count(*) from {key} group by tbname")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, value)
            tdSql.checkData(0, 1, 1)
            tdSql.query(f"show create table {key}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, value)
            self.show_create_result.append(tdSql.getData(0, 1))
            tdSql.query(f"drop table {key}")

        tdSql.query("select tbname, count(*) from ```s``t``` partition by tbname")
        tdSql.checkRows(2)
        for i in range(len(self.show_create_result)):
            tdLog.info(f"{self.show_create_result[i]}")
            tdSql.query(f"{self.show_create_result[i]}")
        tdSql.query("select tbname, count(*) from ```s``t``` partition by tbname")
        tdSql.checkRows(7)

        i=0
        for key, value in self.column_dict.items():
            tdSql.query(f"show create table {key}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, value)
            tdSql.checkData(0, 1, self.show_create_result[i])
            i+=1

        tdLog.debug(f"--------------  step6:  show create normal table/drop/recreate with show result ------------------")
        tdSql.query("show create table ```n``t```")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`n`t`")
        tdSql.checkData(0, 1, "CREATE TABLE ```n``t``` (```t``s``` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', ```v1` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium')")
        showCreateResult = tdSql.getData(0, 1)
        tdSql.query("show tables like '`n`t`'")
        tdSql.checkRows(1)
        tdSql.query("drop table ```n``t```")
        tdSql.query("show tables like '`n`t`'")
        tdSql.checkRows(0)
        tdSql.query(f"{showCreateResult}")
        tdSql.query("show create table ```n``t```")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`n`t`")
        tdSql.checkData(0, 1, f"{showCreateResult}")
        tdSql.query("show tables like '`n`t`'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`n`t`")

        tdLog.debug(f"--------------  step7:  show create super table/drop/recreate with show result ------------------")
        tdSql.query("show create table ```s``t```")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`s`t`")
        tdSql.checkData(0, 1, "CREATE STABLE ```s``t``` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', ```v1``` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium') TAGS (```t``1``` INT)")
        showCreateResult = tdSql.getData(0, 1)
        tdSql.query("show stables like '`s`t`'")
        tdSql.checkRows(1)
        tdSql.query("drop table ```s``t```")
        tdSql.query("show stables like '`s`t`'")
        tdSql.checkRows(0)
        tdSql.query(f"{showCreateResult}")
        tdSql.query("show create table ```s``t```")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`s`t`")
        tdSql.checkData(0, 1, f"{showCreateResult}")
        tdSql.query("show stables like '`s`t`'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`s`t`")

        tdLog.debug(f"--------------  step8:  show create virtual table/drop/recreate with show result ------------------")
        tdSql.execute("create vtable db.```vntb``100```(```ts``` timestamp, ```v0_``0` int from db.```n``t```.```v1`, ```v0_``1` int from db.```n``t```.```v1`)")
        tdSql.execute("create stable db.```vstb``100```(```ts``` timestamp, ```c``0``` int, ```c``1``` int) tags(```t0``` int, `t``1``` varchar(20)) virtual 1")
        tdSql.execute("create vtable db.```vctb``100```(```c``0``` from db.```n``t```.```v1`, ```c``1``` from db.```n``t```.```v1`) using db.```vstb``100``` tags(0, \"0\")")
        # normal table
        tdSql.query("show create vtable db.```vntb``100```")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`vntb`100`")
        tdSql.checkData(0, 1, "CREATE VTABLE ```vntb``100``` (```ts``` TIMESTAMP, ```v0_``0` INT FROM `db`.```n``t```.```v1`, ```v0_``1` INT FROM `db`.```n``t```.```v1`)")
        showCreateVntb = tdSql.getData(0, 1)
        tdSql.query("show vtables like '`vntb`100`'")
        tdSql.checkRows(1)
        tdSql.query("drop vtable db.```vntb``100```")
        tdSql.query("show vtables like '`vntb`100`'")
        tdSql.checkRows(0)
        tdSql.query(f"{showCreateVntb}")
        tdSql.query("show create table db.```vntb``100```")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`vntb`100`")
        tdSql.checkData(0, 1, f"{showCreateVntb}")
        tdSql.query("show vtables like '`vntb`100`'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`vntb`100`")
        # child tble
        tdSql.query("show create vtable db.```vctb``100```")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`vctb`100`")
        tdSql.checkData(0, 1, "CREATE VTABLE ```vctb``100``` (```c``0``` FROM `db`.```n``t```.```v1`, ```c``1``` FROM `db`.```n``t```.```v1`) USING ```vstb``100``` (```t0```, `t``1```) TAGS (0, \"0\")")
        showCreateVctb = tdSql.getData(0, 1)
        tdSql.query("show vtables like '`vctb`100`'")
        tdSql.checkRows(1)
        tdSql.query("drop vtable db.```vctb``100```")
        tdSql.query("show vtables like '`vctb`100`'")
        tdSql.checkRows(0)
        tdSql.query(f"{showCreateVctb}")
        tdSql.query("show create table db.```vctb``100```")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`vctb`100`")
        tdSql.checkData(0, 1, f"{showCreateVctb}")
        tdSql.query("show vtables like '`vctb`100`'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`vctb`100`")
        #super table
        tdSql.query("show create vtable db.```vstb``100```")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`vstb`100`")
        tdSql.checkData(0, 1, "CREATE STABLE ```vstb``100``` (```ts``` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', ```c``0``` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium', ```c``1``` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium') TAGS (```t0``` INT, `t``1``` VARCHAR(20)) VIRTUAL 1")
        showCreateVstb = tdSql.getData(0, 1)
        tdSql.query("show stables like '`vstb`100`'")
        tdSql.checkRows(1)
        tdSql.query("drop table db.```vstb``100```")
        tdSql.query("show stables like '`vstb`100`'")
        tdSql.checkRows(0)
        tdSql.query(f"{showCreateVstb}")
        tdSql.query("show create table db.```vstb``100```")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`vstb`100`")
        tdSql.checkData(0, 1, f"{showCreateVstb}")
        tdSql.query("show stables like '`vstb`100`'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`vstb`100`")

    def run(self):
        tdSql.prepare()
        self.ts6532()
        self.td29092()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
