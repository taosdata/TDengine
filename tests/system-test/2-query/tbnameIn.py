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
        
    def inTest(self, dbname="db"):
        tdSql.execute(f'drop database if exists {dbname}')
        tdSql.execute(f'create database {dbname}')
        tdSql.execute(f'use {dbname}')
        tdSql.execute(f'CREATE STABLE {dbname}.`st1` (`ts` TIMESTAMP, `v1` INT) TAGS (`t1` INT);')
        tdSql.execute(f'CREATE STABLE {dbname}.`st2` (`ts` TIMESTAMP, `v1` INT) TAGS (`t1` INT);')
        tdSql.execute(f'CREATE TABLE {dbname}.`t11` USING {dbname}.`st1` (`t1`) TAGS (11);')
        tdSql.execute(f'CREATE TABLE {dbname}.`t12` USING {dbname}.`st1` (`t1`) TAGS (12);')
        tdSql.execute(f'CREATE TABLE {dbname}.`t21` USING {dbname}.`st2` (`t1`) TAGS (21);')
        tdSql.execute(f'CREATE TABLE {dbname}.`t22` USING {dbname}.`st2` (`t1`) TAGS (22);')
        tdSql.execute(f'CREATE TABLE {dbname}.`ta` (`ts` TIMESTAMP, `v1` INT);')
        
        tdSql.execute(f"insert into {dbname}.t11 values ( '2025-01-21 00:11:01', 111 )")
        tdSql.execute(f"insert into {dbname}.t11 values ( '2025-01-21 00:11:02', 112 )")
        tdSql.execute(f"insert into {dbname}.t11 values ( '2025-01-21 00:11:03', 113 )")
        tdSql.execute(f"insert into {dbname}.t12 values ( '2025-01-21 00:12:01', 121 )")
        tdSql.execute(f"insert into {dbname}.t12 values ( '2025-01-21 00:12:02', 122 )")
        tdSql.execute(f"insert into {dbname}.t12 values ( '2025-01-21 00:12:03', 123 )")

        tdSql.execute(f"insert into {dbname}.t21 values ( '2025-01-21 00:21:01', 211 )")
        tdSql.execute(f"insert into {dbname}.t21 values ( '2025-01-21 00:21:02', 212 )")
        tdSql.execute(f"insert into {dbname}.t21 values ( '2025-01-21 00:21:03', 213 )")
        tdSql.execute(f"insert into {dbname}.t22 values ( '2025-01-21 00:22:01', 221 )")
        tdSql.execute(f"insert into {dbname}.t22 values ( '2025-01-21 00:22:02', 222 )")
        tdSql.execute(f"insert into {dbname}.t22 values ( '2025-01-21 00:22:03', 223 )")

        tdSql.execute(f"insert into {dbname}.ta values ( '2025-01-21 00:00:01', 1 )")
        tdSql.execute(f"insert into {dbname}.ta values ( '2025-01-21 00:00:02', 2 )")
        tdSql.execute(f"insert into {dbname}.ta values ( '2025-01-21 00:00:03', 3 )")
        
        tdLog.debug(f"--------------  step1:  normal table test   ------------------")
        tdSql.query(f"select last(*) from {dbname}.ta where tbname in ('ta');")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2025-01-21 00:00:03')
        tdSql.checkData(0, 1, 3) 
  
        tdSql.query(f"select last(*) from {dbname}.ta where tbname in ('ta', 't21');")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2025-01-21 00:00:03')
        tdSql.checkData(0, 1, 3) 
        
        tdSql.query(f"select last(*) from {dbname}.ta where tbname in ('t21');")  
        tdSql.checkRows(0)
        
        tdSql.query(f"select last(*) from {dbname}.ta where tbname in ('ta') and tbname in ('ta');")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2025-01-21 00:00:03')
        tdSql.checkData(0, 1, 3) 
  
        tdSql.query(f"select last(*) from {dbname}.ta where tbname in ('ta') or tbname in ('ta');")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2025-01-21 00:00:03')
        tdSql.checkData(0, 1, 3) 

        tdSql.query(f"select last(*) from {dbname}.ta where tbname in ('ta') or tbname in ('tb');")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2025-01-21 00:00:03')
        tdSql.checkData(0, 1, 3) 

        tdSql.query(f"select last(*) from {dbname}.ta where tbname in ('ta', 't21') and tbname in ('ta');")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2025-01-21 00:00:03')
        tdSql.checkData(0, 1, 3) 

        tdSql.query(f"select last(*) from {dbname}.ta where tbname in ('ta', 't21') and tbname in ('t21');")  
        tdSql.checkRows(0)
        
        tdSql.query(f"select last(*) from {dbname}.ta where tbname in ('t21') or tbname in ('ta');")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2025-01-21 00:00:03')
        tdSql.checkData(0, 1, 3) 

        tdLog.debug(f"--------------  step2:  super table test   ------------------")
        tdSql.query(f"select last(*) from {dbname}.st1 where tbname in ('t11');")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2025-01-21 00:11:03')
        tdSql.checkData(0, 1, 113)

        tdSql.query(f"select last(*) from {dbname}.st1 where tbname in ('t21');")  
        tdSql.checkRows(0)

        tdSql.query(f"select last(*) from {dbname}.st1 where tbname in ('ta', 't21');")
        tdSql.checkRows(0)

        tdSql.query(f"select last(*) from {dbname}.st1 where tbname in ('t21', 't12');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2025-01-21 00:12:03')
        tdSql.checkData(0, 1, 123)

        tdSql.query(f"select last(*) from {dbname}.st1 where tbname in ('ta') and tbname in ('t12');")
        tdSql.checkRows(0)

        tdSql.query(f"select last(*) from {dbname}.st1 where tbname in ('t12') or tbname in ('t11');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2025-01-21 00:12:03')
        tdSql.checkData(0, 1, 123)

        tdSql.query(f"select last(*) from {dbname}.st1 where tbname in ('ta') or tbname in ('t21');")
        tdSql.checkRows(0)

        tdSql.query(f"select last(*) from {dbname}.st1 where tbname in ('t12', 't21') and tbname in ('t21');")
        tdSql.checkRows(0)

        tdSql.query(f"select last(*) from {dbname}.st1 where tbname in ('t12', 't11') and tbname in ('t11');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2025-01-21 00:11:03')
        tdSql.checkData(0, 1, 113)


    def run(self):
        self.inTest()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
