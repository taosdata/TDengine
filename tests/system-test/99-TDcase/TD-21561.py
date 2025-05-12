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

import random
import os
import time
import taos
import subprocess
from faker import Faker
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
from util.dnodes import tdDnodes
from util.dnodes import *

class TDTestCase:
    updatecfgDict = {'maxSQLLength':1048576,'debugFlag': 143 ,"querySmaOptimize":1}

    def init(self, conn, logSql, replicaVar):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.testcasePath = os.path.split(__file__)[0]
        self.testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))

        self.db = "hyp"

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

        tdSql.query("select count(*) from %s.stable_1;" %database)
        tdSql.checkData(0,0,num_random*n)
        tdSql.query("select count(*) from %s.table_0;"%database)
        tdSql.checkData(0,0,n)



    def sqls(self,database,function):
        sql0 = f"select {function}(q_tinyint) from {database}.stable_1 where tbname in ('stable_1_1') group by tbname ;"

        sql1 = f"select {function}(q_tinyint) from {database}.stable_1 where tbname in ('stable_1_1') group by tbname;"

        sql2 = f"select {function}(q_tinyint) from  {database}.stable_1 where tbname in ('stable_1_1') and _C0 is not null and  _C0 between 1600000000000 and now +1h  and  (q_nchar like 'nchar%' or q_binary = '0'  or q_nchar = 'nchar_' ) and q_bool in (0 , 1)  group by tbname ;"

        sql3 = f"select * from (select {function}(q_tinyint) from  {database}.stable_1 where tbname in ('stable_1_1') and _C0 is not null and  _C0 between 1600000000000 and now +1h  and  (q_nchar like 'nchar%' or q_binary = '0'  or q_nchar = 'nchar_' ) and q_bool in (0 , 1)  group by tbname) ;"

        self.constant_check(sql1,sql0)
        self.constant_check(sql1,sql2)
        self.constant_check(sql2,sql3)

    def check_flushdb(self,database):
        functions = ['COUNT', 'HYPERLOGLOG']
        for f in functions:
            for i in range(20):
                self.sqls(database, f);

            tdSql.execute(" flush database %s;" %database)
            tdLog.info("flush database success")

            for i in range(20):
                self.sqls(database, f);


    def constant_check(self,sql1,sql2):
        tdLog.info("\n=============sql1:(%s)___sql2:(%s) ====================\n" %(sql1,sql2))
        tdSql.query(sql1)
        sql1_value = tdSql.getData(0,0)
        tdSql.query("reset query cache")
        tdSql.query(sql2)
        sql2_value = tdSql.getData(0,0)
        self.value_check(sql1_value,sql2_value)

    def value_check(self,base_value,check_value):
        if base_value==check_value:
            tdLog.info(f"checkEqual success, base_value={base_value},check_value={check_value}")
        else :
            tdLog.exit(f"checkEqual error, base_value=={base_value},check_value={check_value}")

    def run(self):

        startTime = time.time()

        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))

        self.dropandcreateDB_random("%s" %self.db, 100)

        self.check_flushdb("%s" %self.db)

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))



    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
