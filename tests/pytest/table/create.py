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
import time
import os
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        now = time.time()
        self.ts = int(round(now * 1000))

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root)-len("/build/bin")]
                    break
        return buildPath

    def run(self):
        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        binPath = buildPath+ "/build/bin/"

        os.system("rm -rf table/create.py.sql")
        tdSql.prepare()

        print("==============step1")
        print("prepare data")
        tdSql.execute("create table db.st (ts timestamp, i int) tags(j int)")
        tdSql.execute("create table db.tb using st tags(1)")
        tdSql.execute("insert into db.tb values(now, 1)")

        print("==============step2")
        print("create table as select")
        try:
            tdSql.execute("create table db.test as select * from db.st")
        except Exception as e:
            tdLog.exit(e)

        # case for defect: https://jira.taosdata.com:18080/browse/TD-2560
        tdSql.execute("create table db.tb02 using st tags(2)")
        tdSql.execute("create table db.tb03 using st tags(3)")
        tdSql.execute("create table db.tb04 using st tags(4)")

        tdSql.query("show tables like 'tb%' ")
        tdSql.checkRows(4)

        tdSql.query("show tables like 'tb0%' ")
        tdSql.checkRows(3)

        tdSql.execute("create table db.st0 (ts timestamp, i int) tags(j int)")
        tdSql.execute("create table db.st1 (ts timestamp, i int, c2 int) tags(j int, loc nchar(20))")

        tdSql.query("show stables like 'st%' ")
        tdSql.checkRows(3)

        # case for defect: https://jira.taosdata.com:18080/browse/TD-2693
        tdSql.execute("create database db2")
        tdSql.execute("use db2")
        tdSql.execute("create table stb(ts timestamp, c int) tags(t int)")
        tdSql.error("insert into db2.tb6 using db2.stb tags(1) values(now 1) tb2 using db2. tags( )values(now 2)")


        print("==============new version [escape character] for stable==============")
        print("==============step1,#create db.stable,db.table; insert db.table; show db.table; select db.table; drop db.table;")
        print("prepare data")
        
        self.stb1 = "stable_1~!@#$%^&*()-_+=[]{}':,<.>/?stST13579"
        self.tb1 = "table_1~!@#$%^&*()-_+=[]{}':,<.>/?stST13579"

        tdSql.execute("create stable db.`%s` (ts timestamp, i int) tags(j int)" %self.stb1)
        tdSql.query("describe db.`%s` ; " %self.stb1)
        tdSql.checkRows(3)

        tdSql.query("select _block_dist() from db.`%s` ; " %self.stb1)
        tdSql.checkRows(0)

        tdSql.query("show create stable db.`%s` ; " %self.stb1)
        tdSql.checkData(0, 0, self.stb1)
        tdSql.checkData(0, 1, "create table `%s` (ts TIMESTAMP,i INT) TAGS (j INT)" %self.stb1)

        tdSql.execute("create table db.`table!1` using db.`%s` tags(1)" %self.stb1)
        tdSql.query("describe db.`table!1` ; ")
        tdSql.checkRows(3)

        time.sleep(10)
        tdSql.query("show create table db.`table!1` ; ")
        tdSql.checkData(0, 0, "table!1")
        tdSql.checkData(0, 1, "CREATE TABLE `table!1` USING `%s` TAGS (1)" %self.stb1)
        tdSql.execute("insert into db.`table!1` values(now, 1)")
        tdSql.query("select * from  db.`table!1`; ")
        tdSql.checkRows(1)
        tdSql.query("select count(*) from  db.`table!1`; ")
        tdSql.checkData(0, 0, 1)       
        tdSql.query("select _block_dist() from db.`%s` ; " %self.stb1)
        tdSql.checkRows(1)

        tdSql.execute("create table db.`%s` using db.`%s` tags(1)" %(self.tb1,self.stb1))
        tdSql.query("describe db.`%s` ; " %self.tb1)
        tdSql.checkRows(3)
        tdSql.query("show create table db.`%s` ; " %self.tb1)
        tdSql.checkData(0, 0, self.tb1)
        tdSql.checkData(0, 1, "CREATE TABLE `%s` USING `%s` TAGS (1)" %(self.tb1,self.stb1))
        tdSql.execute("insert into db.`%s`  values(now, 1)" %self.tb1)
        tdSql.query("select * from  db.`%s` ; " %self.tb1)
        tdSql.checkRows(1)
        tdSql.query("select count(*) from  db.`%s`; "  %self.tb1)
        tdSql.checkData(0, 0, 1)
        #time.sleep(10)
        tdSql.query("select * from  db.`%s` ; "  %self.stb1)
        tdSql.checkRows(2)
        tdSql.query("select count(*) from  db.`%s`; " %self.stb1)
        tdSql.checkData(0, 0, 2)

        tdSql.query("select * from (select * from db.`%s`) ; " %self.stb1)
        tdSql.checkRows(2)
        tdSql.query("select count(*) from (select * from db.`%s`) ; " %self.stb1)
        tdSql.checkData(0, 0, 2)

        tdSql.query("show db.stables like 'stable_1%' ")
        tdSql.checkRows(1)
        tdSql.query("show db.tables like 'table%' ")
        tdSql.checkRows(2)

        #TD-10531 tbname is not support
        # tdSql.execute("select * from db.`%s` where tbname = db.`%s`;"  %(self.stb1,self.tb1))
        # tdSql.checkRows(1)
        # tdSql.execute("select count(*) from db.`%s` where tbname in (db.`%s`,db.`table!1`);"  %(self.stb1,self.tb1))
        # tdSql.checkRows(4)

        print("==============old scene is not change, max length : database.name + table.name <= 192")
        self.tb192old = "table192table192oldtable192oldtable192oldtable192oldtable192oldtable192oldtable192oldtable192oldtable192oldtable192oldtable192oldtable192oldtable192oldtable192oldtable192oldtable192oldtable192"
        tdSql.execute("create table db.%s using db.st tags(1)" %self.tb192old)
        tdSql.query("describe db.%s ; " %self.tb192old)
        tdSql.checkRows(3)
        tdSql.query("show db.tables like 'table192%' ")
        tdSql.checkRows(1)
        self.tb193old = "table193table192oldtable192oldtable192oldtable192oldtable192oldtable192oldtable192oldtable192oldtable192oldtable192oldtable192oldtable192oldtable192oldtable192oldtable192oldtable192oldtable192o"
        tdSql.error("create table db.%s using db.st tags(1)" %self.tb193old)
        
        print("==============new scene `***` is change, max length : `table.name` <= 192 ,not include database.name")
        self.tb192new = "table_192~!@#$%^&*()-_+=[]{}':,<.>/?stST0123456789table_192~!@#$%^&*()-_+=[]{}':,<.>/?stST0123456789table_192~!@#$%^&*()-_+=[]{}':,<.>/?stST0123456789table_192~!@#$%^&*()-_+=[]{}':,<.>/?stST12"
        tdSql.execute("create table db.`%s` using db.`%s` tags(1)" %(self.tb192new,self.stb1))
        tdSql.query("describe db.`%s` ; " %self.tb192new)
        tdSql.checkRows(3)
        tdSql.query("show db.tables like 'table_192%' ")
        tdSql.checkRows(1)
        self.tb193new = "table_193~!@#$%^&*()-_+=[]{}':,<.>/?stST0123456789table_192~!@#$%^&*()-_+=[]{}':,<.>/?stST0123456789table_192~!@#$%^&*()-_+=[]{}':,<.>/?stST0123456789table_192~!@#$%^&*()-_+=[]{}':,<.>/?stST123"
        tdSql.error("create table db.`%s` using db.`%s` tags(1)" %(self.tb193new,self.stb1))
        # case for TD-10691
        tdSql.error("create table ttb1(ts timestamp, file int )")
        


        self.cr_tb1 = "create_table_1~!@#$%^&*()-_+=[]{}':,<.>/?stST13579"
        tdSql.execute("create table db.`%s` as select avg(i) from db.`%s` where ts > now interval(1m) sliding(30s);" %(self.cr_tb1,self.stb1))
        tdSql.query("show db.tables like 'create_table_%' ")
        tdSql.checkRows(1)

        print("==============drop table\stable")
        try:
            tdSql.execute("drop table db.`%s` " %self.tb1)
        except Exception as e:
            tdLog.exit(e)

        tdSql.error("select * from db.`%s`" %self.tb1)
        tdSql.query("show db.stables like 'stable_1%' ")
        tdSql.checkRows(1)

        try:
            tdSql.execute("drop table db.`%s` " %self.stb1)
        except Exception as e:
            tdLog.exit(e)

        tdSql.error("select * from db.`%s`" %self.tb1)
        tdSql.error("select * from db.`%s`" %self.stb1)
       
        
        print("==============step2,#create stable,table; insert table; show table; select table; drop table")
        
        self.stb2 = "stable_2~!@#$%^&*()-_+=[]{}';:,<.>/?stST24680~!@#$%^&*()-_+=[]{}"
        self.tb2 = "table_2~!@#$%^&*()-_+=[]{}';:,<.>/?stST24680~!@#$%^&*()-_+=[]{}"

        tdSql.execute("create stable `%s` (ts timestamp, i int) tags(j int);" %self.stb2)
        tdSql.query("describe `%s` ; "%self.stb2)
        tdSql.checkRows(3)

        tdSql.query("select _block_dist() from `%s` ; " %self.stb2)
        tdSql.checkRows(0)

        tdSql.query("show create stable `%s` ; " %self.stb2)
        tdSql.checkData(0, 0, self.stb2)
        tdSql.checkData(0, 1, "create table `%s` (ts TIMESTAMP,i INT) TAGS (j INT)" %self.stb2)

        tdSql.execute("create table `table!2` using `%s` tags(1)" %self.stb2)
        tdSql.query("describe `table!2` ; ")
        tdSql.checkRows(3)

        time.sleep(10)

        tdSql.query("show create table `table!2` ; ")
        tdSql.checkData(0, 0, "table!2")
        tdSql.checkData(0, 1, "CREATE TABLE `table!2` USING `%s` TAGS (1)" %self.stb2)
        tdSql.execute("insert into `table!2` values(now, 1)")
        tdSql.query("select * from  `table!2`; ")
        tdSql.checkRows(1)
        tdSql.query("select count(*) from  `table!2`; ")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select _block_dist() from `%s` ; " %self.stb2)
        tdSql.checkRows(1)

        tdSql.execute("create table `%s` using `%s` tags(1)" %(self.tb2,self.stb2))
        tdSql.query("describe `%s` ; " %self.tb2)
        tdSql.checkRows(3)
        tdSql.query("show create table `%s` ; " %self.tb2)
        tdSql.checkData(0, 0, self.tb2)
        tdSql.checkData(0, 1, "CREATE TABLE `%s` USING `%s` TAGS (1)" %(self.tb2,self.stb2))
        tdSql.execute("insert into `%s`  values(now, 1)" %self.tb2)
        tdSql.query("select * from  `%s` ; " %self.tb2)
        tdSql.checkRows(1)
        tdSql.query("select count(*) from  `%s`; " %self.tb2)
        tdSql.checkData(0, 0, 1)
        tdSql.query("select * from  `%s` ; " %self.stb2)
        tdSql.checkRows(2)
        tdSql.query("select count(*) from  `%s`; " %self.stb2)
        tdSql.checkData(0, 0, 2)

        tdSql.query("select * from (select * from `%s`) ; "  %self.stb2)
        tdSql.checkRows(2)
        tdSql.query("select count(*) from (select * from `%s` ); "  %self.stb2)
        tdSql.checkData(0, 0, 2)

        tdSql.query("show stables like 'stable_2%' ")
        tdSql.checkRows(1)
        tdSql.query("show tables like 'table%' ")
        tdSql.checkRows(2)


        #TD-10531 tbname is not support
        # tdSql.execute("select * from db.`%s` where tbname = db.`%s`;"  %(self.stb1,self.tb1))
        # tdSql.checkRows(1)
        # tdSql.execute("select count(*) from db.`%s` where tbname in (db.`%s`,db.`table!1`);"  %(self.stb1,self.tb1))
        # tdSql.checkRows(4)

        #TD-10536
        self.cr_tb2 = "create_table_2~!@#$%^&*()-_+=[]{}';:,<.>/?stST24680~!@#$%^&*()-_+=[]{}"
        tdSql.execute("create table `%s` as select * from `%s` ;" %(self.cr_tb2,self.stb2))
        tdSql.query("show db.tables like 'create_table_%' ")
        tdSql.checkRows(1)

        print("==============drop table\stable")
        try:
            tdSql.execute("drop table `%s` " %self.tb2)
        except Exception as e:
            tdLog.exit(e)

        tdSql.error("select * from `%s`" %self.tb2)
        tdSql.query("show stables like 'stable_2%' ")
        tdSql.checkRows(1)

        try:
            tdSql.execute("drop table `%s` " %self.stb2)
        except Exception as e:
            tdLog.exit(e)

        tdSql.error("select * from `%s`" %self.tb2)
        tdSql.error("select * from `%s`" %self.stb2)


        print("==============step3,#create regular_table; insert regular_table; show regular_table; select regular_table; drop regular_table")
        self.regular_table = "regular_table~!@#$%^&*()-_+=[]{}';:,<.>/?stST24680~!@#$%^&*()-_+=[]{}"
        
        tdSql.execute("create table `%s` (ts timestamp,i int) ;" %self.regular_table)
        tdSql.query("describe `%s` ; "%self.regular_table)
        tdSql.checkRows(2)

        tdSql.query("select _block_dist() from `%s` ; " %self.regular_table)
        tdSql.checkRows(1)

        tdSql.query("show create table `%s` ; " %self.regular_table)
        tdSql.checkData(0, 0, self.regular_table)
        tdSql.checkData(0, 1, "CREATE TABLE `%s` (ts TIMESTAMP,i INT)" %self.regular_table)

        tdSql.execute("insert into `%s`  values(now, 1)" %self.regular_table)
        tdSql.query("select * from  `%s` ; " %self.regular_table)
        tdSql.checkRows(1)
        tdSql.query("select count(*) from  `%s`; " %self.regular_table)
        tdSql.checkData(0, 0, 1)
        tdSql.query("select _block_dist() from `%s` ; " %self.regular_table)
        tdSql.checkRows(1)

        tdSql.query("select * from (select * from `%s`) ; "  %self.regular_table)
        tdSql.checkRows(1)
        tdSql.query("select count(*) from (select * from `%s` ); "  %self.regular_table)
        tdSql.checkData(0, 0, 1)

        tdSql.query("show tables like 'regular_table%' ")
        tdSql.checkRows(1)

        self.crr_tb = "create_r_table~!@#$%^&*()-_+=[]{}';:,<.>/?stST24680~!@#$%^&*()-_+=[]{}"
        tdSql.execute("create table `%s` as select * from `%s` ;" %(self.crr_tb,self.regular_table))
        tdSql.query("show db2.tables like 'create_r_table%' ")
        tdSql.checkRows(1)

        print("==============drop table\stable")
        try:
            tdSql.execute("drop table `%s` " %self.regular_table)
        except Exception as e:
            tdLog.exit(e)

        tdSql.error("select * from `%s`" %self.regular_table)
        
   


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
