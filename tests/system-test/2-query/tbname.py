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
        tdSql.execute('CREATE TABLE ```t1``` USING ```s``t``` (```t``1```) TAGS (1);')
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
        
        tdLog.debug(f"--------------  step3:  subtable test   ------------------")
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
        
        tdSql.query("select ts, ```t1``` from ```s``t``` where ```s``t```.tbname=\"`t1`\"")  
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 1)
        
        # tdSql.query("select tbname, ts from st where tbname=\"t2\"")  
        # tdSql.checkRows(3)
        
        # tdSql.query("select tbname, ts from st where tbname=\"t2\" order by tbname")  
        # tdSql.checkRows(3)
        
        # tdSql.query("select tbname, ts from st where tbname=\"t2\" order by st.tbname")  
        # tdSql.checkRows(3)
        
        # tdSql.query("select tbname, count(*) from st where tbname=\"t2\" group by tbname order by tbname")  
        # tdSql.checkRows(1)
        # tdSql.checkData(0, 1, 3)
        
        # tdSql.query("select tbname, count(*) from st group by tbname order by tbname")  
        # tdSql.checkRows(2)
        # tdSql.checkData(0, 1, 3)
        # tdSql.checkData(1, 1, 3)
        
        # tdSql.query("select tbname, count(*) from st group by st.tbname order by st.tbname")  
        # tdSql.checkRows(2)
        # tdSql.checkData(0, 1, 3)
        # tdSql.checkData(1, 1, 3)

        # tdLog.debug(f"--------------  step4:  join test   ------------------")
        # tdSql.query("select t1.tbname, t2.tbname from t1, t2 where t1.ts=t2.ts and t1.tbname!=t2.tbname")  
        # tdSql.checkRows(3)
        
        # tdSql.query("select t1.tbname, t2.tbname from t1, t2 where t1.ts=t2.ts and t1.tbname!=t2.tbname order by t1.tbname")  
        # tdSql.checkRows(3)
        
        # tdSql.query("select st.tbname, st2.tbname from st, st2 where st.ts=st2.ts and st.tbname!=st2.tbname order by st.tbname")  
        # tdSql.checkRows(0)
        
        # tdSql.execute(f"insert into t21 values ( { now_time + 1000 }, 1 )")
        # tdSql.query("select st.tbname, st2.tbname from st, st2 where st.ts=st2.ts and st.tbname!=st2.tbname order by st.tbname")  
        # tdSql.checkRows(2)
        
        # tdSql.query("select t1.tbname, st2.tbname from t1, st2 where t1.ts=st2.ts and t1.tbname!=st2.tbname order by t1.tbname")  
        # tdSql.checkRows(1)
        
        # tdSql.query("select nt.ts, st.tbname from nt, st where nt.ts=st.ts order by st.tbname")  
        # tdSql.checkRows(6)
        
        # tdSql.query("select nt.ts, t1.tbname from nt, t1 where nt.ts=t1.ts order by t1.tbname")  
        # tdSql.checkRows(3)

    def run(self):
        tdSql.prepare()
        self.ts6532()
        # self.td29092()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
