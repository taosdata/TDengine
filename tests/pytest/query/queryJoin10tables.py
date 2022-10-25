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

import taos
import sys

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *

class TDTestCase:

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def createtable(self):

        # create stbles
        tdSql.execute("create table if not exists stb1 (ts timestamp, c1 int) tags(t11 int, t12 int)")
        tdSql.execute("create table if not exists stb2 (ts timestamp, c2 int) tags(t21 int, t22 int)")
        tdSql.execute("create table if not exists stb3 (ts timestamp, c3 int) tags(t31 int, t32 int)")
        tdSql.execute("create table if not exists stb4 (ts timestamp, c4 int) tags(t41 int, t42 int)")
        tdSql.execute("create table if not exists stb5 (ts timestamp, c5 int) tags(t51 int, t52 int)")
        tdSql.execute("create table if not exists stb6 (ts timestamp, c6 int) tags(t61 int, t62 int)")
        tdSql.execute("create table if not exists stb7 (ts timestamp, c7 int) tags(t71 int, t72 int)")
        tdSql.execute("create table if not exists stb8 (ts timestamp, c8 int) tags(t81 int, t82 int)")
        tdSql.execute("create table if not exists stb9 (ts timestamp, c9 int) tags(t91 int, t92 int)")
        tdSql.execute("create table if not exists stb10 (ts timestamp, c10 int) tags(t101 int, t102 int)")
        tdSql.execute("create table if not exists stb11 (ts timestamp, c11 int) tags(t111 int, t112 int)")

        # create normal tables
        tdSql.execute("create table t10 using stb1 tags(0, 9)")
        tdSql.execute("create table t11 using stb1 tags(1, 8)")
        tdSql.execute("create table t12 using stb1 tags(2, 7)")
        tdSql.execute("create table t13 using stb1 tags(3, 6)")
        tdSql.execute("create table t14 using stb1 tags(4, 5)")
        tdSql.execute("create table t15 using stb1 tags(5, 4)")
        tdSql.execute("create table t16 using stb1 tags(6, 3)")
        tdSql.execute("create table t17 using stb1 tags(7, 2)")
        tdSql.execute("create table t18 using stb1 tags(8, 1)")
        tdSql.execute("create table t19 using stb1 tags(9, 0)")
        tdSql.execute("create table t110 using stb1 tags(10, 10)")

        tdSql.execute("create table t20 using stb2 tags(0, 9)")
        tdSql.execute("create table t21 using stb2 tags(1, 8)")
        tdSql.execute("create table t22 using stb2 tags(2, 7)")

        tdSql.execute("create table t30 using stb3 tags(0, 9)")
        tdSql.execute("create table t31 using stb3 tags(1, 8)")
        tdSql.execute("create table t32 using stb3 tags(2, 7)")

    def inserttable(self):
        for i in range(100):
            if i<60:
                tdSql.execute(f"insert into t20 values('2020-10-01 00:00:{i}.000', {i})")
                tdSql.execute(f"insert into t21 values('2020-10-01 00:00:{i}.000', {i})")
                tdSql.execute(f"insert into t22 values('2020-10-01 00:00:{i}.000', {i})")
                tdSql.execute(f"insert into t30 values('2020-10-01 00:00:{i}.000', {i})")
                tdSql.execute(f"insert into t31 values('2020-10-01 00:00:{i}.000', {i})")
                tdSql.execute(f"insert into t32 values('2020-10-01 00:00:{i}.000', {i})")
            else:
                tdSql.execute(f"insert into t20 values('2020-10-01 00:01:{i-60}.000', {i})")
                tdSql.execute(f"insert into t21 values('2020-10-01 00:01:{i-60}.000', {i})")
                tdSql.execute(f"insert into t22 values('2020-10-01 00:01:{i-60}.000', {i})")
                tdSql.execute(f"insert into t30 values('2020-10-01 00:01:{i-60}.000', {i})")
                tdSql.execute(f"insert into t31 values('2020-10-01 00:01:{i-60}.000', {i})")
                tdSql.execute(f"insert into t32 values('2020-10-01 00:01:{i-60}.000', {i})")
            for j in range(11):
                if i<60:
                    tdSql.execute(f"insert into t1{j} values('2020-10-01 00:00:{i}.000', {i})")
                else:
                    tdSql.execute(f"insert into t1{j} values('2020-10-01 00:01:{i-60}.000', {i})")

    def queryjointable(self):
        tdSql.error(
            '''select  from t10,t11,t12,t13,t14,t15,t16,t17,t18,t19 
            where t10.ts=t11.ts and t10.ts=t12.ts and t10.ts=t13.ts and t10.ts=t14.ts and t10.ts=t15.ts 
            and t10.ts=t16.ts and t10.ts=t17.ts and t10.ts=t18.ts and t10.ts=t19.ts'''
        )
        tdSql.error("select * from t10 where t10.ts=t11.ts")
        tdSql.error("select * from where t10.ts=t11.ts")
        tdSql.error("select * from t10,t11,t12,t13,t14,t15,t16,t17,t18,t19")
        tdSql.error("select * from stb1, stb2, stb3 where stb1.ts=stb2.ts and stb1.ts=stb3.ts")
        tdSql.error("select * from stb1, stb2, stb3 where stb1.t11=stb2.t21 and stb1.t11=stb3.t31")
        tdSql.error("select * from stb1, stb2, stb3")
        tdSql.error(
            '''select  * from stb1 
            join stb2 on stb1.ts=stb2.ts and stb1.t11=stb2.t21 
            join stb3 on stb1.ts=stb3.ts and stb1.t11=stb3.t31'''
        )
        tdSql.error("select * from t10 join t11 on t10.ts=t11.ts join t12 on t11.ts=t12.ts")
        tdSql.query(
            '''select * from stb1,stb2,stb3 
            where stb1.ts=stb2.ts and stb1.ts=stb3.ts and stb1.t11=stb2.t21 and stb1.t11 =stb3.t31'''
        )
        tdSql.checkRows(300)
        tdSql.query("select * from t11,t12,t13 where t11.ts=t12.ts and t11.ts=t13.ts")
        tdSql.checkRows(100)
        tdSql.error("selec * from t11,t12,t13 where t11.ts=t12.ts and t11.ts=t13.ts")
        tdSql.error("select * form t11,t12,t13 where t11.ts=t12.ts and t11.ts=t13.ts")
        tdSql.error("select * from t11,t12,t13 when t11.ts=t12.ts and t11.ts=t13.ts")
        tdSql.error("select * from t11,t12,t13 when t11.ts <> t12.ts and t11.ts=t13.ts")
        tdSql.error("select * from t11,t12,t13 when t11.ts != t12.ts and t11.ts=t13.ts")
        tdSql.error("select * from t11,t12,t13 when t11.ts=t12.ts or t11.ts=t13.ts")
        tdSql.error("select * from t11,t12,t13 when t11.ts=t12.ts=t13.ts")
        tdSql.error("select * from t11,t12,t13 when t11.c1=t12.c2 and t11.c1=t13.c3")
        tdSql.error("select * from t11,t12,t13 when t11.ts=t12.ts and t11.ts=t13.c3 and t11.c1=t13.ts")
        tdSql.error("select ts from t11,t12,t13 when t11.ts=t12.ts and t11.ts=t13.ts")
        tdSql.error("select * from t11,t12,t13 when t11.ts=ts and t11.ts=t13.ts")
        tdSql.error("select * from t11,t12,t13 when t11.ts=t12.ts and t11.ts=t13.ts and ts>100")
        tdSql.error("select * from t11,t12,stb1 when t11.ts=t12.ts and t11.ts=stb1.ts")
        tdSql.error("select t14.ts from t11,t12,t13 when t11.ts=t12.ts and t11.ts=t13.ts")
        tdSql.error("select * from t11,t12,t13 when t11.ts=t12.ts and t11.ts=t13.ts1")
        tdSql.error("select * from t11,t12,t13 when t11.ts=t12.ts and t11.ts=t14.ts")
        tdSql.error("select * from t11,t12,t13 when t11.ts=t12.ts")
        tdSql.error("select * from t11,t12,t13 when t11.ts=t12.ts and t11.ts=t13.ts and t11.c1=t13.c3")
        tdSql.error(
            '''select * from t10,t11,t12,t13,t14,t15,t16,t17,t18,t19,t20 
            where t10.ts=t11.ts and t10.ts=t12.ts and t10.ts=t13.ts and t10.ts=t14.ts and t10.ts=t15.ts 
            and t10.ts=t16.ts and t10.ts=t17.ts and t10.ts=t18.ts and t10.ts=t19.ts and t10.ts=t20.ts'''
        )
        tdSql.error(
            '''select * from t10,t11,t12,t13,t14,t15,t16,t17,t18,t19,t20 
            where t10.ts=t11.ts and t10.ts=t12.ts and t10.ts=t13.ts and t10.ts=t14.ts and t10.ts=t15.ts 
            and t10.ts=t16.ts and t10.ts=t17.ts and t10.ts=t18.ts and t10.ts=t19.ts'''
        )
        tdSql.error(
            '''select * from t10,t11,t12,t13,t14,t15,t16,t17,t18,t19
            where t10.ts=t11.ts and t10.ts=t12.ts and t10.ts=t13.ts and t10.ts=t14.ts and t10.ts=t15.ts 
            and t10.ts=t16.ts and t10.ts=t17.ts and t10.ts=t18.ts and t10.ts=t19.ts and t10.c1=t19.c1'''
        )
        tdSql.error(
            '''select * from stb1,stb2,stb3 
                        where stb1.ts=stb2.ts and stb1.ts=stb3.ts and stb1.t11=stb2.t21'''
        )
        tdSql.error(
            '''select * from stb1,stb2,stb3 
                        where stb1.ts=stb2.ts and stb1.t11=stb2.t21 and stb1.t11=stb3.t31'''
        )
        tdSql.error(
            '''select * from stb1,stb2,stb3 
                        where stb1.ts=stb2.ts and stb1.ts=stb3.ts and stb1.t11=stb2.t21 and stb1.t11=stb3.t31
                        and stb1.t12=stb3=t32'''
        )
        tdSql.error(
            '''select * from stb1,stb2,stb3,stb4,stb5,stb6,stb7,stb8,stb9,stb10,stb11
            where stb1.ts=stb2.ts and stb1.ts=stb3.ts and stb1.ts=stb4.ts and stb1.ts=stb5.ts and stb1.ts=stb6.ts 
            and stb1.ts=stb7.ts and stb1.ts=stb8.ts and stb1.ts=stb9.ts and stb1.ts=stb10.ts and stb1.ts=stb11.ts 
            and stb1.t11=stb2.t21 and stb1.t11=stb3.t31 and stb1.t11=stb4.t41 and stb1.t11=stb5.t51 
            and stb1.t11=stb6.t61 and stb1.t11=stb7.t71 and stb1.t11=stb8.t81 and stb1.t11=stb9.t91 
            and stb1.t11=stb10.t101 and stb1.t11=stb11.t111'''
        )
        tdSql.error(
            '''select * from stb1,stb2,stb3,stb4,stb5,stb6,stb7,stb8,stb9,stb10
            where stb1.ts=stb2.ts and stb1.ts=stb3.ts and stb1.ts=stb4.ts and stb1.ts=stb5.ts and stb1.ts=stb6.ts 
            and stb1.ts=stb7.ts and stb1.ts=stb8.ts and stb1.ts=stb9.ts and stb1.ts=stb10.ts and stb1.t11=stb2.t21 
            and stb1.t11=stb3.t31 and stb1.t11=stb4.t41 and stb1.t11=stb5.t51 and stb1.t11=stb6.t61 
            and stb1.t11=stb7.t71 and stb1.t11=stb8.t81 and stb1.t11=stb9.t91 and stb1.t11=stb10.t101 
            and stb1.t12=stb11.t102'''
        )

    def run(self):
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")
        self.createtable()

        tdLog.printNoPrefix("==========step2:insert data")
        self.inserttable()

        tdLog.printNoPrefix("==========step3:query timestamp type")
        self.queryjointable()

        # after wal and sync, check again
        tdSql.query("select * from information_schema.ins_dnodes")
        index = tdSql.getData(0, 0)
        tdDnodes.stop(index)
        tdDnodes.start(index)

        tdLog.printNoPrefix("==========step4:query again after wal")
        self.queryjointable()


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())