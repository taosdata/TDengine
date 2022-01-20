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
import os
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.ts = 1538548685000
        self.numberOfTables = 10000
        self.numberOfRecords = 100

    def checkCommunity(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))
        if ("community" in selfPath):
            return False
        else:
            return True

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosdump" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

    def insert_data(self, tbname, ts_start, count):
        pre_insert = "insert into %s values"%tbname
        sql = pre_insert
        tdLog.debug("doing insert table %s rows=%d ..."%(tbname, count))
        for i in range(count):
            sql += " (%d,%d)"%(ts_start + i*1000, i)
            if i >0 and i%30000 == 0:
                tdSql.execute(sql)
                sql = pre_insert
        # end sql        
        if sql != pre_insert:
            tdSql.execute(sql)

        tdLog.debug("INSERT TABLE DATA ............ [OK]")
        return

    def run(self):
        if not os.path.exists("./taosdumptest"):
            os.makedirs("./taosdumptest")
        else:
            os.system("rm -rf ./taosdumptest")
            os.makedirs("./taosdumptest")            

        for i in range(2):
            if not os.path.exists("./taosdumptest/tmp%d"%i):
                os.makedirs("./taosdumptest/tmp%d"%i)
            else:
                os.system("rm -rf ./taosdumptest/tmp%d"%i)
                os.makedirs("./taosdumptest/tmp%d"%i)

        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosdump not found!")
        else:
            tdLog.info("taosdump found in %s" % buildPath)
        binPath = buildPath + "/build/bin/"

        # create db1 , one stables and one table ;  create general tables
        tdSql.execute("drop database if  exists dp1")
        tdSql.execute("drop database  if exists dp2")
        tdSql.execute("create database if not exists dp1")
        tdSql.execute("use dp1")
        tdSql.execute('''create table st0(ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 float, c6 double, 
                    c7 bool, c8 binary(20), c9 nchar(20), c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned, c15 timestamp ) 
                    tags(t1 tinyint, t2 smallint, t3 int, t4 bigint, t5 float, t6 double, t7 bool, t8 binary(20), t9 nchar(20), t11 tinyint unsigned, 
                    t12 smallint unsigned, t13 int unsigned, t14 bigint unsigned, t15 timestamp)''')
        tdSql.execute('''create table st1(ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 float, c6 double, 
                    c7 bool, c8 binary(20), c9 nchar(20), c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned, c15 timestamp ) tags(jtag json)''')

        intData = []        
        floatData = []
        rowNum = 10
        tabNum = 10
        ts = 1537146000000
        for j in range(tabNum):
            tdSql.execute("create  table st0_%d using st0 tags( %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d, %d);" 
                            % (j, j + 1, j + 1, j + 1, j + 1, j + 0.1, j + 0.1, j % 2, j + 1, j + 1, j + 1, j + 1, j + 1, j + 1, ts))
            for i in range(rowNum):
                tdSql.execute("insert into st0_%d values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d, %d)" 
                            % (j, ts + i, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, ts))
                intData.append(i + 1)            
                floatData.append(i + 0.1)    
        rowNum = 20
        tabNum = 20
        for j in range(tabNum):
            tdSql.execute("create  table st1_%d using st1 tags('{\"nv\":null,\"tea\":true,\"\":false,\" \":123%d,\"tea\":false}');" % (j, j + 1))       
            for i in range(rowNum):
                tdSql.execute("insert into st1_%d values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d, %d)" 
                            % (j, self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, self.ts))
                intData.append(i + 1)            
                floatData.append(i + 0.1)    
        # os.system("%staosBenchmark -f tools/taosdump-insert-dp1.json -y " % binPath)


        # create db1 , three stables:stb0,include ctables stb0_0 \ stb0_1,stb1 include ctables stb1_0 and stb1_1 
        # \stb3,include ctables stb3_0 and stb3_1 
        # create general three tables gt0 gt1 gt2
        tdSql.execute("create database if not  exists dp2")
        tdSql.execute("use dp2")
        tdSql.execute("create stable st0(ts timestamp, c01 int, c02 nchar(10)) tags(t1 int)")
        tdSql.execute("create table st0_0 using st0 tags(0) st0_1 using st0 tags(1) ")
        tdSql.execute("insert into st0_0 values(1614218412000,8600,'R')(1614218422000,8600,'E')")
        tdSql.execute("insert into st0_1 values(1614218413000,8601,'A')(1614218423000,8601,'D')")
        tdSql.execute("create stable st1(ts timestamp, c11 float, c12 nchar(10)) tags(t1 int)")
        tdSql.execute("create table st1_0 using st1 tags(0) st1_1 using st1 tags(1) ")
        tdSql.execute("insert into st1_0 values(1614218412000,8610.1,'R')(1614218422000,8610.1,'E')")
        tdSql.execute("insert into st1_1 values(1614218413000,8611.2,'A')(1614218423000,8611.1,'D')")
        tdSql.execute("create stable st2(ts timestamp, c21 float, c22 nchar(10)) tags(t1 int)")
        tdSql.execute("create table st20 using st2 tags(0) st21 using st2 tags(1) ")
        tdSql.execute("insert into st20 values(1614218412000,8620.3,'R')(1614218422000,8620.3,'E')")
        tdSql.execute("insert into st21 values(1614218413000,8621.4,'A')(1614218423000,8621.4,'D')")
        tdSql.execute("create table  if not exists gt0 (ts timestamp, c00 int, c01 float)  ")
        tdSql.execute("create table  if not exists gt1 (ts timestamp, c10 int, c11 double)  ")
        tdSql.execute("create table  if not exists gt2 (ts timestamp, c20 int, c21 float)  ")
        tdSql.execute("insert into gt0 values(1614218412700,8637,78.86155)")
        tdSql.execute("insert into gt1 values(1614218413800,8638,78.862020199)")
        tdSql.execute("insert into gt2 values(1614218413900,8639,78.863)")
        # self.insert_data("t", self.ts, 300*10000);
        # os.system("%staosBenchmark -f tools/taosdump-insert-dp2.json -y " % binPath)




        # #  taosdump data
        # os.system("%staosdump  -o ./taosdumptest/tmp1 taosdump -h  -ptaosdata -P 6030 -u root  -o taosdumptest \
        #  -D dp1,dp3  -N -c  /home/chr/TDinternal/community/sim/dnode1/cfg/taos.cfg  -s -d  deflate" % binPath)
        os.system("%staosdump  -o ./taosdumptest/tmp0 -D dp2,dp1  -T 8 -B 100000" % binPath)
        os.system("%staosdump  -o ./taosdumptest/tmp1 dp2 st0 st1_0 gt0  -T 8 -B 1000"  % binPath)


        #check taosdumptest/tmp0
        tdSql.execute("drop database  dp1")
        tdSql.execute("drop database  dp2")
        os.system("%staosdump -i ./taosdumptest/tmp0 -T 8   " % binPath)
        tdSql.execute("reset query cache")
       
        tdSql.execute("use dp1")
        tdSql.query("show stables")
        tdSql.checkRows(3)
        for i in range(3):
            for  j in range(3):
                if j < 2:
                    if tdSql.queryResult[i][0] == 'st%d'%j:
                        tdSql.checkData(i, 4, (j+1)*10)
                else:
                    if tdSql.queryResult[i][0] == 'st%d'%j:
                        tdSql.checkData(i, 4, 100002)      

        tdSql.query("select count(*) from st0")
        tdSql.checkData(0, 0, 100)  
        tdSql.query("select count(*) from st1")
        tdSql.checkData(0, 0, 400)  
        tdSql.query("select count(*) from st2")
        tdSql.checkData(0, 0, 1000020)  


        tdSql.execute("use dp2")
        tdSql.query("show stables")
        tdSql.checkRows(3)
        for i in range(3):
            for  j in range(3):
                if j < 2:
                    if tdSql.queryResult[i][0] == 'st%d'%j:
                        # print(i,"stb%d"%j)
                        tdSql.checkData(i, 4, 2)
                else:
                    if tdSql.queryResult[i][0] == 'st%d'%j:
                        tdSql.checkData(i, 4, 100002)
        tdSql.query("select count(*) from st0")
        tdSql.checkData(0, 0, 4)  
        tdSql.query("select count(*) from st1")
        tdSql.checkData(0, 0, 4)  
        tdSql.query("select count(*) from st2")
        tdSql.checkData(0, 0, 1000024)  
        tdSql.query("select ts from gt0")
        tdSql.checkData(0,0,'2021-02-25 10:00:12.700')
        tdSql.query("select c10 from gt1")
        tdSql.checkData(0, 0, 8638)
        tdSql.query("select c20 from gt2")
        tdSql.checkData(0, 0, 8639)

        #check taosdumptest/tmp1
        tdSql.execute("drop database  dp1")
        tdSql.execute("drop database  dp2")
        os.system("%staosdump  -i ./taosdumptest/tmp1 -T 8 " % binPath)  
        tdSql.execute("reset query cache")
        tdSql.execute("use dp2")
        tdSql.query("show stables")
        tdSql.checkRows(2)
        tdSql.query("show tables")
        tdSql.checkRows(4)
        tdSql.query("select count(*) from st1_0")
        tdSql.checkData(0,0,2)
        tdSql.query("select ts from gt0")
        tdSql.checkData(0,0,'2021-02-25 10:00:12.700')
        tdSql.error("use dp1")
        tdSql.error("select count(*) from st2_0")
        tdSql.error("select count(*) from gt2")


        # #check taosdumptest/tmp2
        # tdSql.execute("drop database  dp1")
        # tdSql.execute("drop database  dp2")
        # os.system("%staosdump -i ./taosdumptest/tmp2 -T 8   " % binPath)
        # tdSql.execute("use dp1")
        # tdSql.query("show stables")
        # tdSql.checkRows(1)
        # tdSql.query("show tables")
        # tdSql.checkRows(3)
        # tdSql.query("select c1 from st0_0 order by ts")
        # tdSql.checkData(0,0,8537)
        # tdSql.query("select c2 from st0_1 order by ts")
        # tdSql.checkData(1,0,"D")
        # tdSql.query("select * from gt0")
        # tdSql.checkData(0,0,'2021-02-25 10:00:12.000')
        # tdSql.checkData(0,1,637)
        # tdSql.error("select count(*) from gt1")
        # tdSql.error("use dp2")


        # #check taosdumptest/tmp3
        # tdSql.execute("drop database  dp1")
        # os.system("%staosdump  -i ./taosdumptest/tmp3 -T 8 " % binPath)  
        # tdSql.execute("use dp2")
        # tdSql.query("show stables")
        # tdSql.checkRows(2)
        # tdSql.query("show tables")
        # tdSql.checkRows(4)
        # tdSql.query("select count(*) from st1_0")
        # tdSql.checkData(0,0,2)
        # tdSql.query("select ts from gt0")
        # tdSql.checkData(0,0,'2021-02-25 10:00:12.700')
        # tdSql.error("use dp1")
        # tdSql.error("select count(*) from st2_0")
        # tdSql.error("select count(*) from gt2")

        # #check taosdumptest/tmp4
        # tdSql.execute("drop database  dp2")
        # os.system("%staosdump  -i ./taosdumptest/tmp4 -T 8 " % binPath)  
        # tdSql.execute("use dp2")
        # tdSql.query("show stables")
        # tdSql.checkRows(2)
        # tdSql.query("show tables")
        # tdSql.checkRows(6)
        # tdSql.query("select c20 from gt2")
        # tdSql.checkData(0, 0, 8639)
        # tdSql.query("select count(*) from st0_0")
        # tdSql.checkData(0, 0, 2)
        # tdSql.query("select count(*) from st0_1")
        # tdSql.checkData(0, 0, 2)
        # tdSql.query("select count(*) from st2_1")
        # tdSql.checkData(0, 0, 2)
        # tdSql.query("select count(*) from st2_0")
        # tdSql.checkData(0, 0, 2)
        # tdSql.error("use dp1")
        # tdSql.error("select count(*) from st1_0")
        # tdSql.error("select count(*) from st1_1")
        # tdSql.error("select count(*) from gt3")


        # #check taosdumptest/tmp5
        # tdSql.execute("drop database  dp2")
        # os.system("%staosdump  -i ./taosdumptest/tmp5 -T 8 " % binPath)  
        # tdSql.execute("use dp2")
        # tdSql.query("show stables")
        # tdSql.checkRows(3)
        # tdSql.query("show tables")
        # tdSql.checkRows(9)
        # tdSql.query("select c20 from gt2")
        # tdSql.checkData(0, 0, 8639)
        # tdSql.query("select count(*) from st0_0")
        # tdSql.checkData(0, 0, 2)
        # tdSql.query("select count(*) from st0_1")
        # tdSql.checkData(0, 0, 2)
        # tdSql.query("select count(*) from st2_1")
        # tdSql.checkData(0, 0, 2)
        # tdSql.query("select count(*) from st2_0")
        # tdSql.checkData(0, 0, 2)
        # tdSql.query("select count(*) from st1_1")
        # tdSql.checkData(0, 0, 2)
        # tdSql.query("select count(*) from st1_0")
        # tdSql.checkData(0, 0, 2)
        # tdSql.execute("use dp1")
        # tdSql.query("show stables")
        # tdSql.checkRows(1)
        # tdSql.query("show tables")
        # tdSql.checkRows(4)
        # tdSql.query("select c1 from st0_0 order by ts")
        # tdSql.checkData(0,0,8537)
        # tdSql.query("select c2 from st0_1 order by ts")
        # tdSql.checkData(1,0,"D")
        # tdSql.query("select * from gt0")
        # tdSql.checkData(0,0,'2021-02-25 10:00:12.000')
        # tdSql.checkData(0,1,637)

        # # check taosdumptest/tmp6
        # tdSql.execute("drop database dp1")
        # tdSql.execute("drop database dp2")
        # tdSql.execute("drop database dp3")
        # os.system("%staosdump  -i ./taosdumptest/tmp6 -T 8 " % binPath)  
        # tdSql.execute("use dp3")
        # tdSql.query("show databases")
        # tdSql.checkRows(1)
        # tdSql.checkData(0,16,'ns')
        # tdSql.query("show stables")
        # tdSql.checkRows(1)
        # tdSql.query("show tables")
        # tdSql.checkRows(1)
        # tdSql.query("select count(*) from st0_0")
        # tdSql.checkData(0, 0, 2)        
        # tdSql.query("select * from st0 order by ts")
        # tdSql.checkData(0,0,'2021-02-25 10:00:12.000000001')
        # tdSql.checkData(0,1,8600)

        # # check taosdumptest/tmp7
        # tdSql.execute("drop database dp3")
        # os.system("%staosdump  -i ./taosdumptest/tmp7 -T 8 " % binPath)  
        # tdSql.execute("use dp3")
        # tdSql.query("show databases")
        # tdSql.checkRows(1)
        # tdSql.checkData(0,16,'ms')
        # tdSql.query("show stables")
        # tdSql.checkRows(1)
        # tdSql.query("show tables")
        # tdSql.checkRows(1)
        # tdSql.query("select count(*) from st0_0")
        # tdSql.checkRows(0)
        # # tdSql.query("select * from st0 order by ts")
        # # tdSql.checkData(0,0,'2021-02-25 10:00:12.000000001')
        # # tdSql.checkData(0,1,8600)

        # # check taosdumptest/tmp8
        # tdSql.execute("drop database dp3")
        # os.system("%staosdump  -i ./taosdumptest/tmp8 -T 8 " % binPath)  
        # tdSql.execute("use dp3")
        # tdSql.query("show stables")
        # tdSql.checkRows(1)
        # tdSql.query("show tables")
        # tdSql.checkRows(1)
        # tdSql.query("select count(*) from st0_0")
        # tdSql.checkRows(0) 
        # # tdSql.query("select * from st0 order by ts")
        # # tdSql.checkData(0,0,'2021-02-25 10:00:12.000000001')
        # # tdSql.checkData(0,1,8600)

        # os.system("rm -rf ./taosdumptest/tmp1")
        # os.system("rm -rf ./taosdumptest/tmp2")
        # os.system("rm -rf ./taosdumptest/tmp3")
        # os.system("rm -rf ./taosdumptest/tmp4")
        # os.system("rm -rf ./taosdumptest/tmp5")
        # os.system("rm -rf ./dump_result.txt")
        # os.system("rm -rf ./db.csv")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
