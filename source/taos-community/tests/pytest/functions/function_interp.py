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

from util.log import *
from util.cases import *
from util.sql import *
class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def run(self):
        tdSql.prepare()
        tdSql.execute("create table ap1 (ts timestamp, pav float)")
        tdSql.execute("create table ap2 (ts timestamp, pav float) tags (t1 float)")
        tdSql.execute("create table ap2_sub1 using ap2 tags (2.90799)")
        tdSql.execute("create table ap2_sub2 using ap2 tags (2.90799)")
        tdSql.execute("create table ap3 (ts timestamp, pav float) tags (t1 float)")
        tdSql.execute("create table ap3_sub1 using ap3 tags (2.90799)")
        for tb_name in ["ap1", "ap2_sub1", "ap3_sub1"]:
            tdSql.execute(f"insert into {tb_name} values ('2021-07-25 02:19:54.119', 2.90799)")
            tdSql.execute(f"insert into {tb_name} values ('2021-07-25 02:19:54.317', 3.07399)")
            tdSql.execute(f"insert into {tb_name} values ('2021-07-25 02:19:54.517', 0.58117)")
            tdSql.execute(f"insert into {tb_name} values ('2021-07-25 02:19:54.717', 0.16150)")
            tdSql.execute(f"insert into {tb_name} values ('2021-07-25 02:19:54.918', 1.47885)")
            tdSql.execute(f"insert into {tb_name} values ('2021-07-25 02:19:56.569', 1.76472)")
            tdSql.execute(f"insert into {tb_name} values ('2021-07-25 02:19:57.381', 2.13722)")
            tdSql.execute(f"insert into {tb_name} values ('2021-07-25 02:19:57.574', 4.10256)")
            tdSql.execute(f"insert into {tb_name} values ('2021-07-25 02:19:57.776', 3.55345)")
            tdSql.execute(f"insert into {tb_name} values ('2021-07-25 02:19:57.976', 1.46624)")
            tdSql.execute(f"insert into {tb_name} values ('2021-07-25 02:19:58.187', 0.17943)")
            tdSql.execute(f"insert into {tb_name} values ('2021-07-25 02:19:58.372', 2.04101)")
            tdSql.execute(f"insert into {tb_name} values ('2021-07-25 02:19:58.573', 3.20924)")
            tdSql.execute(f"insert into {tb_name} values ('2021-07-25 02:19:58.768', 1.71807)")
            tdSql.execute(f"insert into {tb_name} values ('2021-07-25 02:19:58.964', 4.60900)")
            tdSql.execute(f"insert into {tb_name} values ('2021-07-25 02:19:59.155', 4.33907)")
            tdSql.execute(f"insert into {tb_name} values ('2021-07-25 02:19:59.359', 0.76940)")
            tdSql.execute(f"insert into {tb_name} values ('2021-07-25 02:19:59.553', 0.06458)")
            tdSql.execute(f"insert into {tb_name} values ('2021-07-25 02:19:59.742', 4.59857)")
            tdSql.execute(f"insert into {tb_name} values ('2021-07-25 02:19:59.938', 1.55081)")

        tdSql.query("select interp(pav) from ap1 where ts = '2021-07-25 02:19:54' FILL (PREV)")
        tdSql.checkRows(0)
        tdSql.query("select interp(pav) from ap1 where ts = '2021-07-25 02:19:54' FILL (NEXT)")
        tdSql.checkRows(0)
        tdSql.query("select interp(pav) from ap1 where ts = '2021-07-25 02:19:54' FILL (LINEAR)")
        tdSql.checkRows(0)
        # check None
        tdSql.query("select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:20:00' every(1000a) FILL (None)")
        tdSql.checkRows(0)
        # check NULL
        tdSql.query("select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:20:00' every(1000a) FILL (NULL)")
        tdSql.checkRows(6)
        for i in range(5):
            tdSql.checkData(i,1,None)
        # checkout VALUE
        tdSql.query("select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:20:00' every(1000a) FILL (VALUE, 1)")
        tdSql.checkRows(6)
        for i in range(5):
            tdSql.checkData(i,1,1.00000)
        # check tag group by
        tdSql.query("select interp(pav) from ap2 where ts>= '2021-07-25 02:19:54' and ts<'2021-07-25 02:20:00' every(1000a) FILL (VALUE,1) group by t1;")
        for i in range(5):
            tdSql.checkData(i,1,1.00000)
            tdSql.checkData(i,2,2.90799)
        # check multi ts lines
        tdSql.query("select z1.ts,z1.val1,z2.val2 from (select interp(pav) val1 from ap2 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:20:00' every(1000a) FILL (value,1)) z1,(select interp(pav) val2 from ap3 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:20:00' every(1000a) FILL (value,2)) z2 where z1.ts=z2.ts ;")
        for i in range(5):
            tdSql.checkData(i,1,1.00000)
            tdSql.checkData(i,2,2.00000)
        tdSql.query("select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:20:00' every(1000a) FILL (LINEAR)")
        tdSql.checkRows(6)
        tdSql.query("select interp(pav) from ap1 where ts>= '2021-07-25 02:19:54' and ts<'2021-07-25 02:20:00' every(1000a) FILL (NEXT)")
        tdSql.checkRows(6)
        tdSql.checkData(0,1,2.90799)
        tdSql.query("select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts <= '2021-07-25 02:20:00' every(1000a) FILL (PREV)")
        tdSql.checkRows(7)
        tdSql.checkData(1,1,1.47885)
        tdSql.query("select interp(pav) from ap1 where ts>= '2021-07-25 02:19:54' and ts <= '2021-07-25 02:20:00' every(1000a) FILL (LINEAR)")
        tdSql.checkRows(7)

        # check desc order
        tdSql.error("select interp(pav) from ap1 where ts = '2021-07-25 02:19:54' FILL (PREV) order by ts desc")
        tdSql.query("select interp(pav) from ap1 where ts = '2021-07-25 02:19:54' FILL (NEXT) order by ts desc")
        tdSql.checkRows(0)
        tdSql.query("select interp(pav) from ap1 where ts = '2021-07-25 02:19:54' FILL (LINEAR) order by ts desc")
        tdSql.checkRows(0)
        tdSql.query("select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:20:00' every(1000a) FILL (LINEAR) order by ts desc")
        tdSql.checkRows(6)
        tdSql.query("select interp(pav) from ap1 where ts>= '2021-07-25 02:19:54' and ts<'2021-07-25 02:20:00' every(1000a) FILL (NEXT) order by ts desc")
        tdSql.checkRows(6)
        tdSql.checkData(0,1,4.60900)
        tdSql.error("select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts <= '2021-07-25 02:20:00' every(1000a) FILL (PREV) order by ts desc")
        tdSql.query("select interp(pav) from ap1 where ts>= '2021-07-25 02:19:54' and ts <= '2021-07-25 02:20:00' every(1000a) FILL (LINEAR) order by ts desc")
        tdSql.checkRows(7)

        # check exception
        tdSql.error("select interp(*) from ap1")
        tdSql.error("select interp(*) from ap1 FILL(NEXT)")
        tdSql.error("select interp(*) from ap1 ts >= '2021-07-25 02:19:54' FILL(NEXT)")
        tdSql.error("select interp(*) from ap1 ts <= '2021-07-25 02:19:54' FILL(NEXT)")
        tdSql.error("select interp(*) from ap1 where ts >'2021-07-25 02:19:59.938' and ts < now every(1s) fill(next)")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
