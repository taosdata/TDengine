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
import os
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)        
        tdSql.init(conn.cursor())

        self.ts = 1627750800000
        self.numberOfRecords = 10000

    def pre_stable(self):
        os.system("gcc -g -O0 -fPIC -shared ../script/sh/abs_max.c -o /tmp/abs_max.so")
        os.system("gcc -g -O0 -fPIC -shared ../script/sh/add_one.c -o /tmp/add_one.so")
        os.system("gcc -g -O0 -fPIC -shared ../script/sh/sum_double.c -o /tmp/sum_double.so")
        tdSql.execute("create table stb(ts timestamp ,c1 int, c2 bigint) tags(t1 int)") 
        for i in range(50):
            for j in range(200):
                sql = "insert into t%d using stb tags(%d) values(%s,%d,%d)" % (i, i, self.ts + j, 1e2+j, 1e10+j)
                tdSql.execute(sql)
        for i in range(50):
            for j in range(200):
                sql = "insert into t%d using stb tags(%d) values(%s,%d,%d)" % (i, i, self.ts + j + 200 , -1e2-j, -j-1e10)
                tdSql.execute(sql)
                
    def test_udf_null(self):
        tdLog.info("test missing parameters")
        tdSql.error("create aggregate function  as '/tmp/abs_maxw.so' outputtype bigint;")
        tdSql.error("create aggregate function abs_max as '' outputtype bigint;")
        tdSql.error("create aggregate function abs_max as  outputtype bigint;")
        tdSql.error("create aggregate function abs_max as '/tmp/abs_maxw.so' ;")
        tdSql.error("create aggregate  abs_max as '/tmp/abs_maxw.so' outputtype bigint;")
        tdSql.execute("create aggregate function abs_max as '/tmp/abs_max.so' outputtype bigint;") 
        tdSql.error("select abs_max() from stb")
        tdSql.error("select abs_max(c2) from ")
        tdSql.execute("drop function abs_max")

    def test_udf_format(self):
        # tdSql.error("create aggregate function avg as '/tmp/abs_max.so' outputtype bigint;")
        tdSql.error("create aggregate function .a as '/tmp/abs_max.so' outputtype bigint;")
        tdSql.error("create aggregate function .11 as '/tmp/abs_max.so' outputtype bigint;")
        tdSql.error("create aggregate function 1a as '/tmp/abs_max.so' outputtype bigint;")
        tdSql.error("create aggregate function \"1+1\" as '/tmp/abs_max.so' outputtype bigint;")
        # tdSql.error("create aggregate function [avg] as '/tmp/abs_max.so' outputtype bigint;")
        tdSql.execute("create aggregate function abs_max as '/tmp/abs_max.so' outputtype bigint;")
        # tdSql.error("create aggregate function abs_max2 as '/tmp/abs_max.so' outputtype bigint;")
        tdSql.execute("drop function abs_max;")
        tdSql.error("create aggregate function abs_max as '/tmp/add_onew.so' outputtype bigint;")

    def test_udf_test(self):
        tdSql.execute("create aggregate function abs_max as '/tmp/abs_max.so' outputtype bigint;")
        tdSql.error("create aggregate function abs_max as '/tmp/add_onew.so' outputtype bigint;")
        sql = 'select abs_max() from db.stb'
        tdSql.error(sql)
        sql = 'select abs_max(c2) from db.stb'
        tdSql.query(sql)
        tdSql.checkData(0,0,1410065607)

    def test_udf_values(self):
        tdSql.execute("drop function abs_max")        
        tdSql.execute("create function add_one as '/tmp/add_one.so' outputtype int")
        tdSql.execute("create aggregate function abs_max as '/tmp/abs_max.so' outputtype bigint bufsize 128;")
        tdSql.execute("create aggregate function sum_double as '/tmp/sum_double.so' outputtype int bufsize 128;")

        # UDF bug no 1 -> follow 3 cases about this bug ;
        # tdSql.error("create aggregate function max as '/tmp/abs_max.so' outputtype bigint bufsize 128;")
        # tdSql.error("create aggregate function avg as '/tmp/abs_max.so' outputtype bigint bufsize 128;")  
        # tdSql.error("create aggregate function dbs as '/tmp/abs_max.so' outputtype bigint bufsize 128;")

    

        tdSql.execute("drop database if exists test")
        tdSql.execute("create database test")
        tdSql.execute("use test")
        tdSql.execute("create stable st (ts timestamp,id int , val double , number bigint, chars binary(200)) tags (ind int)")
        tdSql.execute("create table tb1 using st tags(1)")
        tdSql.execute("create table tb2 using st tags(2)")
        start_time = 1604298064000
        rows = 5
        tb_nums = 2
        for i in range(1, tb_nums + 1):
             for j in range(rows):
                 start_time += 10
                 tdSql.execute(
                     "insert into tb%d values(%d, %d,%f,%d,%s) " % (i, start_time, j, float(j),j*100, "'str" + str(j) + "'"))
        tdSql.query("select count(*) from st")
        tdSql.execute("create table bound using st tags(3)")
        epoch_time=1604298064000
        intdata1 = -2**31+2
        intdata2 = 2**31-2
        bigintdata1 = -2**63+2
        bigintdata2 = 2**63-2  
        print("insert into bound values(%d, %d , %f, %d , %s)"%(epoch_time,intdata1,float(intdata1),bigintdata1,"'binary"+str(intdata1)+"'"))
        tdSql.execute("insert into bound values(%d, %d , %f, %d , %s)"%(epoch_time,intdata1,float(intdata1),bigintdata1,"'binary"+str(intdata1)+"'"))
        
        tdSql.execute("insert into bound values(%d, %d , %f, %d , %s)"%(epoch_time+10,intdata1+1,float(intdata1+1),bigintdata1+1,"'binary"+str(intdata1+1)+"'"))
        tdSql.execute("insert into bound values(%d, %d , %f, %d , %s)"%(epoch_time+100,intdata2,float(intdata2),bigintdata2,"'binary"+str(intdata2)+"'"))
        tdSql.execute("insert into bound values(%d, %d , %f, %d , %s)"%(epoch_time+1000,intdata2+1,float(intdata2+1),bigintdata2+1,"'binary"+str(intdata2+1)+"'"))
        
        # check super table calculation results
        tdSql.query("select add_one(id) from st")
        tdSql.checkData(0,0,1)
        tdSql.checkData(1,0,2)
        tdSql.checkData(4,0,5)
        tdSql.checkData(5,0,1)
        tdSql.checkData(9,0,5)
        tdSql.checkData(10,0,-2147483645)
        tdSql.checkData(13,0,None)
        # check common table calculation results
        tdSql.query("select add_one(id) from tb1")
        tdSql.checkData(0,0,1)
        tdSql.checkData(1,0,2)
        tdSql.checkData(4,0,5)

        tdSql.error("select add_one(col) from st")

        sqls= ["select add_one(chars) from st",
               "select add_one(val) from st",
               "select add_one(ts) from st"]
        for sql in sqls:
            res = tdSql.getResult(sql)
            if res == []:
                tdLog.info("====== this col not support use UDF , because datatype not match defind in UDF function ======")
            else:
                tdLog.info(" ====== unexpected error occured about UDF function =====")
                sys.exit()

        tdLog.info("======= UDF function abs_max check ======")

        sqls= ["select abs_max(val) from st",
               "select abs_max(id) from st",
               "select abs_max(ts) from st"]
        for sql in sqls:
            res = tdSql.getResult(sql)
            if res == []:
                tdLog.info("====== this col not support use UDF , because datatype not match defind in UDF function ======")
            else:
                tdLog.info(" ====== unexpected error occured about UDF function =====")
                sys.exit()

        # UDF bug no 2 -> values of abs_max not inconsistent from common table and stable.
        # tdSql.query("select abs_max(val) from st") # result is 0 rows
        # tdSql.query("select abs_max(val) from tb1")
        # tdSql.checkData(0,0,0)  # this is error result
        # tdSql.query("select sum_double(val) from st") # result is 0 rows
        # tdSql.query("select sum_double(val) from tb1")
        # tdSql.checkData(0,0,0)  # this is error result
  
        # UDF bug no 3 -> values of abs_max will error for boundary number 

        # check super table calculation results
        # tdSql.query("select abs_max(number) from st")
        # tdSql.checkData(0,0,9223372036854775807) 

        # check common table calculation results 
        tdSql.query("select abs_max(number) from tb1")
        tdSql.checkData(0,0,400)
        tdSql.query("select abs_max(number) from tb2")
        tdSql.checkData(0,0,400)

        # check boundary
        # tdSql.query("select abs_max(number) from bound")
        # tdSql.checkData(0,0,9223372036854775807)

        tdLog.info("======= UDF function sum_double check =======")

        
        tdSql.query("select sum_double(id) from st")
        tdSql.checkData(0,0,44)
        tdSql.query("select sum_double(id) from tb1")
        tdSql.checkData(0,0,20)

        # UDF bug no 4 -> values error while two function work : it is limit that udf can't work with build-in functions.
        # tdSql.query("select sum_double(id) , abs_max(number) from tb1")
        # tdSql.checkData(0,0,20)
        # tdSql.checkData(0,0,400)

        # tdSql.query("select sum_double(id) , abs_max(number) from st")
        # tdSql.checkData(0,0,44)
        # tdSql.checkData(0,0,9223372036854775807)

        # UDF not support mix up with build-in functions
        # it seems like not support  scalar_function mix up with aggregate functions
        tdSql.error("select sum_double(id) ,add_one(id) from st")
        tdSql.error("select sum_double(id) ,add_one(id) from tb1")
        tdSql.error("select sum_double(id) ,max(id) from st")
        tdSql.error("select sum_double(id) ,max(id) from tb1")

        # UDF function not support Arithmetic ===================
        tdSql.query("select max(id) + 5 from st")
        tdSql.query("select max(id) + 5 from tb1")
        tdSql.query("select max(id) + avg(val) from st")
        tdSql.query("select max(id) + avg(val) from tb1")
        tdSql.error("select abs_max(number) + 5 from st")
        tdSql.error("select abs_max(number) + 5 from tb1")
        tdSql.error("select abs_max(number) + max(id) from st")
        tdSql.error("select abs_max(number)*abs_max(val) from st")     

        tdLog.info("======= UDF Nested query test =======")
        tdSql.query("select sum(id) from (select id from st)")
        tdSql.checkData(0,0,22)

        #UDF bug no 5 -> not support Nested query

        # tdSql.query("select abs_max(number) from (select number from st)")
        # tdSql.checkData(0,0,9223372036854775807)
        # tdSql.query("select abs_max(number) from (select number from bound)")
        # tdSql.checkData(0,0,9223372036854775807)
        # tdSql.query("select sum_double(id) from (select id from st)")
        # tdSql.checkData(0,0,44)
        # tdSql.query("select sum_double(id) from (select id from tb1)")
        # tdSql.checkData(0,0,10)

        # UDF bug no 6 -> group by work error
        tdLog.info("======= UDF work with group by =======")

        # tdSql.query("select sum_double(id) from st group by tbname;")
        # tdSql.checkData(0,0,6)
        # tdSql.checkData(0,1,'tb1')
        # tdSql.checkData(1,0,2)
        # tdSql.checkData(1,1,'tb2')
        # tdSql.query("select sum_double(id) from st group by id;")
        # tdSql.checkRows(2)
        # tdSql.query("select sum_double(id) from st group by tbname order by ts asc;")


        tdSql.query("select sum_double(id) from st where ts < now and ind =1 interval(1s)")
        tdSql.checkData(0,1,20)
        tdSql.error("select sum_double(id) from st session(ts, 1s) interval (10s,1s) sliding(10s) fill (NULL) ")
        tdSql.error("select sum_double(id) from st session(ts, 1s)")
        tdSql.query("select sum_double(id) from tb1 session(ts, 1s)")
        tdSql.checkData(0,1,20)

        # UDF -> bug no 7 : intervals sliding values calculation error
        # tdSql.query("select sum_double(id) from st where ts < now and ind =1 interval(3s) sliding (1s) limit 2")
        # tdSql.checkData(0,1,20)
        # tdSql.checkData(1,1,20)
        
        # scalar_function can't work when using interval and sliding =========
        tdSql.error("select add_one(id) from st where ts < now and ind =1 interval(3s) sliding (1s) limit 2 ")

        tdLog.info(" =====================test illegal creation method =====================")
        
        tdSql.execute("drop function add_one")
        tdSql.execute("drop function abs_max")
        tdSql.execute("drop function sum_double")

        tdSql.execute("create aggregate function error_use1 as '/tmp/abs_max.so' outputtype bigint bufsize 128")
        tdSql.error("select error_use1(number) from st")

        # UDF -> bug no 8: error return values when create aggregate functions as an scalar_function
        # with no aggregate
        # tdSql.execute("create function abs_max as '/tmp/abs_max.so' outputtype bigint bufsize 128")
        # tdSql.query("select abs_max(number) from st")    # this bug will return 3 rows
        # tdSql.checkRows(1)
        # tdSql.execute("create function sum_double as '/tmp/sum_double.so' outputtype bigint bufsize 128")
        # tdSql.execute("select sum_double(id) from st")    
        # tdSql.checkRows(1)
        
        # UDF -> bug  no 9: give bufsize for scalar_function add_one;
        # UDF -> need improve : when outputtype is not match datatype which is defined in function codes 
        tdSql.execute("create function add_one as '/tmp/add_one.so' outputtype bigint bufsize 128")
        # tdSql.error("select add_one(val) from st")  # it should return error not [] for not match col datatype
        # tdSql.query("select add_one(id) from st")   # return error query result
        # tdSql.checkData(0,0,1)
        # tdSql.checkData(1,0,2)
        # tdSql.checkData(5,0,1)
        # tdSql.checkData(10,0,-2147483645)
        # tdSql.checkData(13,0,None)


        # UDF -> improve : aggregate function with no bufsize : it seems with no affect 
        # tdSql.execute("drop function abs_max")
        # tdSql.execute("drop function sum_double")
        tdSql.execute("create aggregate function abs_max as '/tmp/abs_max.so' outputtype bigint ")
        tdSql.execute("create aggregate function sum_double as '/tmp/sum_double.so' outputtype int ")
        tdSql.query("select sum_double(id) from st")
        tdSql.checkData(0,0,44)
        tdSql.query("select sum_double(id) from tb1")
        tdSql.checkData(0,0,20)
        # tdSql.query("select abs_max(number) from st")
        # tdSql.checkData(0,0,9223372036854775807) 
        tdSql.query("select abs_max(number) from tb1")
        tdSql.checkData(0,0,400)

        #UDF bug no 10 -> create function datatype of outputtype not match col datatype
        tdSql.execute("drop function abs_max")
        tdSql.execute("drop function sum_double")
        tdSql.execute("drop function add_one")
        tdSql.execute("create function add_one as '/tmp/add_one.so' outputtype bigint;")
        tdSql.execute("create aggregate function abs_max as '/tmp/abs_max.so' outputtype int bufsize 128;")
        tdSql.execute("create aggregate function sum_double as '/tmp/sum_double.so' outputtype double bufsize 128;")
        # tdSql.query("select sum_double(id) from st")   this bug will return 0.000000
        # tdSql.checkData(0,0,44)
        # tdSql.query("select sum_double(id) from tb1")
        # tdSql.checkData(0,0,20)                        this bug will return 0.000000
        # tdSql.query("select add_one(id) from st")      this bug will return series error values
        # tdSql.checkData(0,0,1)
        # tdSql.checkData(1,0,2)
        # tdSql.checkData(5,0,1)
        # tdSql.checkData(10,0,-2147483645)
        # tdSql.checkData(13,0,None)
        # tdSql.query("select add_one(id) from tb1")     this bug will return series error values
        # tdSql.checkData(0,0,1)
        # tdSql.checkData(2,0,3)
        # tdSql.query("select abs_max(id) from st")
        # tdSql.checkData(0,0,9223372036854775807)     
        tdSql.query("select abs_max(number) from tb1")       # it seems work well
        tdSql.checkData(0,0,400)     



        # UDF bug no 11 -> follow test case will coredump for taosd  and let data lost
        # tdSql.query("select add_one(id) from st group by tbname")

        # UDF -> bug  no 12: give aggregate for scalar_function add_one ,it will let taosd coredump as data lost
        # tdSql.execute("drop function add_one")
        # tdSql.execute("create  aggregate function add_one as '/tmp/add_one.so' outputtype bigint bufsize 128")
        # tdSql.query("select add_one(id) from st")

        # UDF bug no 13 -> follow test case will coredump for taosc
        # tdSql.query("select add_one(*) from st ")
        # tdSql.query("select add_one(*) from tb1 ")

        # UDF bug no 14 -> follow test case will coredump for taosc
        # tdSql.query("select abs_max(id),abs_max(number) from st ")
        # tdSql.query("select abs_max(number),abs_max(number) from st ")
        # tdSql.query("select sum_double(id),sum_double(id) from st ")
        
    def run(self):
        tdSql.prepare()

        tdLog.info("==============step1 prepare udf build=============")
        self.pre_stable()       
        tdLog.info("==============step2 prepare udf null =============")
        self.test_udf_null()
        tdLog.info("==============step3 prepare udf format ===========")
        self.test_udf_format()
        tdLog.info("==============step4 test udf functions============")
        self.test_udf_test()
        tdLog.info("==============step4 test udf values ============")
        self.test_udf_values()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
