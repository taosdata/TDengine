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
from faker import Faker
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
from util.dnodes import tdDnodes
from util.dnodes import *

class TDTestCase:
    updatecfgDict = {'maxSQLLength':1048576,'debugFlag': 143 ,"cDebugFlag":143,"uDebugFlag":143 ,"rpcDebugFlag":143 , "tmrDebugFlag":143 ,
    "jniDebugFlag":143 ,"simDebugFlag":143,"dDebugFlag":143, "dDebugFlag":143,"vDebugFlag":143,"mDebugFlag":143,"qDebugFlag":143,
    "wDebugFlag":143,"sDebugFlag":143,"tsdbDebugFlag":143,"tqDebugFlag":143 ,"fsDebugFlag":143 ,"udfDebugFlag":143}

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        testcasePath = os.path.split(__file__)[0]
        testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf %s/%s.sql" % (testcasePath,testcaseFilename))

        now = time.time()
        self.ts = int(round(now * 1000))
        self.num = 10
        self.fornum = 5

    # def case_common(self):
    #     db = "nested"
    #     self.dropandcreateDB("%s" % db, 1)

    #     conn1 = taos.connect(host="127.0.0.1", user="root", password="taosdata", config="/etc/taos/")
    #     cur1 = conn1.cursor()
    #     cur1.execute('use "%s";' %self.db)
    #     sql = 'select * from stable_1 limit 5;'
    #     cur1.execute(sql)

    #     return(conn1,cur1)

    def data_matrix_equal(self, sql1,row1_s,row1_e,col1_s,col1_e, sql2,row2_s,row2_e,col2_s,col2_e):
        #  ----row1_start----col1_start----
        #  - - - - 是一个矩阵内的数据相等- - -
        #  - - - - - - - - - - - - - - - -
        #  ----row1_end------col1_end------
        self.sql1 = sql1
        list1 =[]
        tdSql.query(sql1)
        for i1 in range(row1_s-1,row1_e):
            #print("iiii=%d"%i1)
            for j1 in range(col1_s-1,col1_e):
                #print("jjjj=%d"%j1)
                #print("data=%s" %(tdSql.getData(i1,j1)))
                list1.append(tdSql.getData(i1,j1))
        print("=====list1-------list1---=%s" %set(list1))

        tdSql.execute("reset query cache;")
        self.sql2 = sql2
        list2 =[]
        tdSql.query(sql2)
        for i2 in range(row2_s-1,row2_e):
            #print("iiii222=%d"%i2)
            for j2 in range(col2_s-1,col2_e):
                #print("jjjj222=%d"%j2)
                #print("data=%s" %(tdSql.getData(i2,j2)))
                list2.append(tdSql.getData(i2,j2))
        print("=====list2-------list2---=%s" %set(list2))

        if  (list1 == list2) and len(list2)>0:
            # print(("=====matrix===sql1.list1:'%s',sql2.list2:'%s'") %(list1,list2))
            tdLog.info(("===matrix===sql1:'%s' matrix_result = sql2:'%s' matrix_result") %(sql1,sql2))
        elif (set(list2)).issubset(set(list1)):
            # 解决不同子表排列结果乱序
            # print(("=====list_issubset==matrix2in1-true===sql1.list1:'%s',sql2.list2:'%s'") %(list1,list2))
            tdLog.info(("===matrix_issubset===sql1:'%s' matrix_set_result = sql2:'%s' matrix_set_result") %(sql1,sql2))
        #elif abs(float(str(list1).replace("]","").replace("[","").replace("e+","")) - float(str(list2).replace("]","").replace("[","").replace("e+",""))) <= 0.0001:
        elif abs(float(str(list1).replace("datetime.datetime","").replace("]","").replace("[","").replace("e+","").replace(", ","").replace("(","").replace(")","").replace("-","")) - float(str(list2).replace("datetime.datetime","").replace("]","").replace("[","").replace("e+","").replace(", ","").replace("(","").replace(")","").replace("-",""))) <= 0.0001:
            print(("=====matrix_abs+e+===sql1.list1:'%s',sql2.list2:'%s'") %(list1,list2))
            print(("=====matrix_abs+e+replace_after===sql1.list1:'%s',sql2.list2:'%s'") %(float(str(list1).replace("datetime.datetime","").replace("]","").replace("[","").replace("e+","").replace(", ","").replace("(","").replace(")","").replace("-","")),float(str(list2).replace("datetime.datetime","").replace("]","").replace("[","").replace("e+","").replace(", ","").replace("(","").replace(")","").replace("-",""))))
            tdLog.info(("===matrix_abs+e+===sql1:'%s' matrix_result = sql2:'%s' matrix_result") %(sql1,sql2))
        elif abs(float(str(list1).replace("datetime.datetime","").replace("]","").replace("[","").replace(", ","").replace("(","").replace(")","").replace("-","")) - float(str(list2).replace("datetime.datetime","").replace("]","").replace("[","").replace(", ","").replace("(","").replace(")","").replace("-",""))) <= 0.1:
            #{datetime.datetime(2021, 8, 27, 1, 46, 40), -441.46841430664057}replace
            print(("=====matrix_abs+replace===sql1.list1:'%s',sql2.list2:'%s'") %(list1,list2))
            print(("=====matrix_abs+replace_after===sql1.list1:'%s',sql2.list2:'%s'") %(float(str(list1).replace("datetime.datetime","").replace("]","").replace("[","").replace(", ","").replace("(","").replace(")","").replace("-","")),float(str(list1).replace("datetime.datetime","").replace("]","").replace("[","").replace(", ","").replace("(","").replace(")","").replace("-",""))))
            tdLog.info(("===matrix_abs+replace===sql1:'%s' matrix_result = sql2:'%s' matrix_result") %(sql1,sql2))
        elif abs(float(str(list1).replace("datetime.datetime","").replace("]","").replace("[","").replace(", ","").replace("(","").replace(")","").replace("-","")) - float(str(list2).replace("datetime.datetime","").replace("]","").replace("[","").replace(", ","").replace("(","").replace(")","").replace("-",""))) <= 0.5:
            print(("=====matrix_abs===sql1.list1:'%s',sql2.list2:'%s'") %(list1,list2))
            print(("=====matrix_abs===sql1.list1:'%s',sql2.list2:'%s'") %(float(str(list1).replace("datetime.datetime","").replace("]","").replace("[","").replace(", ","").replace("(","").replace(")","").replace("-","")),float(str(list2).replace("datetime.datetime","").replace("]","").replace("[","").replace(", ","").replace("(","").replace(")","").replace("-",""))))
            tdLog.info(("===matrix_abs======sql1:'%s' matrix_result = sql2:'%s' matrix_result") %(sql1,sql2))
        else:
            print(("=====matrix_error===sql1.list1:'%s',sql2.list2:'%s'") %(list1,list2))
            tdLog.info(("sql1:'%s' matrix_result != sql2:'%s' matrix_result") %(sql1,sql2))
            return tdSql.checkEqual(list1,list2)

    def restartDnodes(self):
        pass
        # tdDnodes.stop(1)
        # tdDnodes.start(1)

    def dropandcreateDB_random(self,database,n):
        ts = 1630000000000
        num_random = 100
        fake = Faker('zh_CN')
        tdSql.execute('''drop database if exists %s ;''' %database)
        tdSql.execute('''create database %s keep 36500;'''%database)
        tdSql.execute('''use %s;'''%database)

        tdSql.execute('''create stable stable_1 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')
        tdSql.execute('''create stable stable_2 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')

        tdSql.execute('''create stable stable_null_data (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')

        tdSql.execute('''create stable stable_null_childtable (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')

        #tdSql.execute('''create table stable_1_1 using stable_1 tags('stable_1_1', '0' , '0' , '0' , '0' , 0 , 'binary1' , 'nchar1' , '0' , '0' ,'0') ;''')
        tdSql.execute('''create table stable_1_1 using stable_1 tags('stable_1_1', '%d' , '%d', '%d' , '%d' , 0 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
        tdSql.execute('''create table stable_1_2 using stable_1 tags('stable_1_2', '2147483647' , '9223372036854775807' , '32767' , '127' , 1 , 'binary2' , 'nchar2' , '2' , '22' , \'1999-09-09 09:09:09.090\') ;''')
        tdSql.execute('''create table stable_1_3 using stable_1 tags('stable_1_3', '-2147483647' , '-9223372036854775807' , '-32767' , '-127' , false , 'binary3' , 'nchar3nchar3' , '-3.3' , '-33.33' , \'2099-09-09 09:09:09.090\') ;''')
        #tdSql.execute('''create table stable_1_4 using stable_1 tags('stable_1_4', '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0') ;''')
        tdSql.execute('''create table stable_1_4 using stable_1 tags('stable_1_4', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))

        # tdSql.execute('''create table stable_2_1 using stable_2 tags('stable_2_1' , '0' , '0' , '0' , '0' , 0 , 'binary21' , 'nchar21' , '0' , '0' ,'0') ;''')
        # tdSql.execute('''create table stable_2_2 using stable_2 tags('stable_2_2' , '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0') ;''')

        # tdSql.execute('''create table stable_null_data_1 using stable_null_data tags('stable_null_data_1', '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0') ;''')

        tdSql.execute('''create table stable_2_1 using stable_2 tags('stable_2_1' , '0' , '0' , '0' , '0' , 0 , 'binary21' , 'nchar21' , '0' , '0' ,\'2099-09-09 09:09:09.090\') ;''')
        tdSql.execute('''create table stable_2_2 using stable_2 tags('stable_2_2' , '%d' , '%d', '%d' , '%d' , 0 , 'binary2.%s' , 'nchar2.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))

        tdSql.execute('''create table stable_null_data_1 using stable_null_data tags('stable_null_data_1', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))

        #regular table
        tdSql.execute('''create table regular_table_1 \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')
        tdSql.execute('''create table regular_table_2 \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')
        tdSql.execute('''create table regular_table_3 \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')

        tdSql.execute('''create table regular_table_null \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')


        for i in range(num_random*n):
            tdSql.execute('''insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;'''
                        % (ts + i*1000, fake.random_int(min=-2147483647, max=2147483647, step=1),
                        fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , ts + i))
            tdSql.execute('''insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;'''
                        % (ts + i*1000, fake.random_int(min=-2147483647, max=2147483647, step=1) ,
                        fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1) ,
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , ts + i))

            tdSql.execute('''insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;'''
                        % (ts + i*1000, fake.random_int(min=0, max=2147483647, step=1),
                        fake.random_int(min=0, max=9223372036854775807, step=1),
                        fake.random_int(min=0, max=32767, step=1) , fake.random_int(min=0, max=127, step=1) ,
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , ts + i))
            tdSql.execute('''insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;'''
                        % (ts + i*1000, fake.random_int(min=0, max=2147483647, step=1),
                        fake.random_int(min=0, max=9223372036854775807, step=1),
                        fake.random_int(min=0, max=32767, step=1) , fake.random_int(min=0, max=127, step=1) ,
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , ts + i))

            tdSql.execute('''insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;'''
                        % (ts + i*1000 +1, fake.random_int(min=-2147483647, max=0, step=1),
                        fake.random_int(min=-9223372036854775807, max=0, step=1),
                        fake.random_int(min=-32767, max=0, step=1) , fake.random_int(min=-127, max=0, step=1) ,
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , ts + i +1))
            tdSql.execute('''insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;'''
                        % (ts + i*1000 +1, fake.random_int(min=-2147483647, max=0, step=1),
                        fake.random_int(min=-9223372036854775807, max=0, step=1),
                        fake.random_int(min=-32767, max=0, step=1) , fake.random_int(min=-127, max=0, step=1) ,
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , ts + i +1))

            tdSql.execute('''insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;'''
                        % (ts + i*1000, fake.random_int(min=-2147483647, max=2147483647, step=1),
                        fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , ts + i))

            # tdSql.execute('''insert into regular_table_3 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d)'''
            #             % (ts + i*1000, fake.random_int(min=-2147483647, max=0, step=1),
            #             fake.random_int(min=-9223372036854775807, max=0, step=1),
            #             fake.random_int(min=-32767, max=0, step=1) , fake.random_int(min=-127, max=0, step=1) ,
            #             fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , ts + i))

        tdSql.query("select count(*) from stable_1;")
        tdSql.checkData(0,0,3*num_random*n)
        tdSql.query("select count(*) from regular_table_1;")
        tdSql.checkData(0,0,num_random*n)


    def run(self):
        tdSql.prepare()
        os.system("rm -rf nestedQuery3.py.sql")

        startTime = time.time()

        db = "nest"
        self.dropandcreateDB_random("%s" %db, 1)

        # regular column select
        q_select= ['ts' , '*' , 'q_int', 'q_bigint' , 'q_bigint' , 'q_smallint' , 'q_tinyint' , 'q_bool' , 'q_binary' , 'q_nchar' ,'q_float' , 'q_double' ,'q_ts ']
        q_select= ['ts' , 'q_int', 'q_bigint' , 'q_bigint' , 'q_smallint' , 'q_tinyint' , 'q_bool' , 'q_binary' , 'q_nchar' ,'q_float' , 'q_double' ,'q_ts ', 'q_int_null ', 'q_bigint_null ' , 'q_bigint_null ' , 'q_smallint_null ' , 'q_tinyint_null ' , 'q_bool_null ' , 'q_binary_null ' , 'q_nchar_null ' ,'q_float_null ' , 'q_double_null ' ,'q_ts_null ']

        # tag column select
        t_select= ['*' , 'loc' ,'t_int', 't_bigint' , 't_bigint' , 't_smallint' , 't_tinyint' , 't_bool' , 't_binary' , 't_nchar' ,'t_float' , 't_double' ,'t_ts ']
        t_select= ['loc' ,'tbname','t_int', 't_bigint' , 't_bigint' , 't_smallint' , 't_tinyint' , 't_bool' , 't_binary' , 't_nchar' ,'t_float' , 't_double' ,'t_ts ']

        # regular and tag column select
        qt_select=  q_select + t_select

        # distinct regular column select
        dq_select= ['distinct q_int', 'distinct q_bigint' , 'distinct q_smallint' , 'distinct q_tinyint' ,
                'distinct q_bool' , 'distinct q_binary' , 'distinct q_nchar' ,'distinct q_float' , 'distinct q_double' ,'distinct q_ts ']

        # distinct tag column select
        dt_select= ['distinct loc', 'distinct t_int', 'distinct t_bigint'  , 'distinct t_smallint' , 'distinct t_tinyint' ,
                'distinct t_bool' , 'distinct t_binary' , 'distinct t_nchar' ,'distinct t_float' , 'distinct t_double' ,'distinct t_ts ']

        # distinct regular and tag column select
        dqt_select= dq_select + dt_select

        # special column select
        s_r_select= ['_c0', '_rowts' , '_C0' ]
        s_s_select= ['tbname' , '_rowts' , '_c0', '_C0' ]
        unionall_or_union= [ ' union ' , ' union all ' ]

        # regular column where
        q_where = ['ts < now +1s','q_bigint >= -9223372036854775807 and q_bigint <= 9223372036854775807', 'q_int <= 2147483647 and q_int >= -2147483647',
        'q_smallint >= -32767 and q_smallint <= 32767','q_tinyint >= -127 and q_tinyint <= 127','q_float >= -1.7E308 and q_float <= 1.7E308',
        'q_double >= -1.7E308 and q_double <= 1.7E308', 'q_binary like \'binary%\'  or q_binary = \'0\' ' , 'q_nchar like \'nchar%\' or q_nchar = \'0\' ' ,
        'q_bool = true or  q_bool = false' , 'q_bool in (0 , 1)' , 'q_bool in ( true , false)' , 'q_bool = 0 or q_bool = 1',
        'q_bigint between  -9223372036854775807 and 9223372036854775807',' q_int between -2147483647 and 2147483647','q_smallint between -32767 and 32767',
        'q_tinyint between -127 and 127 ','q_float >= -3.4E38  ','q_float <= 3.4E38  ','q_double >= -1.7E308  ',
        'q_double <= 1.7E308  ','q_float between -3.4E38 and 3.4E38  ','q_double between -1.7E308 and 1.7E308  ' ,
        'q_float is not null  ' ,'q_double is not null  ' ,]
        #TD-6201 ,'q_bool between 0 and 1'

        # regular column where for test union,join
        q_u_where = ['t1.ts < now +1s' , 't2.ts < now +1s','t1.q_bigint >= -9223372036854775807 and t1.q_bigint <= 9223372036854775807 and t2.q_bigint >= -9223372036854775807 and t2.q_bigint <= 9223372036854775807',
        't1.q_int <= 2147483647 and t1.q_int >= -2147483647 and t2.q_int <= 2147483647 and t2.q_int >= -2147483647',
        't1.q_smallint >= -32767 and t1.q_smallint <= 32767 and t2.q_smallint >= -32767 and t2.q_smallint <= 32767',
        't1.q_tinyint >= -127 and t1.q_tinyint <= 127 and t2.q_tinyint >= -127 and t2.q_tinyint <= 127',
        't1.q_float >= - 1.7E308 and t1.q_float <=  1.7E308 and t2.q_float >= - 1.7E308 and t2.q_float <=  1.7E308',
        't1.q_double >= - 1.7E308 and t1.q_double <=  1.7E308 and t2.q_double >= - 1.7E308 and t2.q_double <=  1.7E308',
        't1.q_binary like \'binary%\'  and t2.q_binary like \'binary%\'  ' ,
        't1.q_nchar like \'nchar%\' and t2.q_nchar like \'nchar%\' ' ,
        't1.q_bool in (0 , 1) and t2.q_bool in (0 , 1)' , 't1.q_bool in ( true , false) and t2.q_bool in ( true , false)' ,
        't1.q_bigint between  -9223372036854775807 and 9223372036854775807 and t2.q_bigint between  -9223372036854775807 and 9223372036854775807',
        't1.q_int between -2147483647 and 2147483647 and t2.q_int between -2147483647 and 2147483647',
        't1.q_smallint between -32767 and 32767 and t2.q_smallint between -32767 and 32767',
        't1.q_tinyint between -127 and 127 and t2.q_tinyint between -127 and 127 ','t1.q_float between -1.7E308 and  1.7E308 and t2.q_float between -1.7E308 and  1.7E308',
        't1.q_double between -1.7E308 and  1.7E308 and t2.q_double between -1.7E308 and  1.7E308']
        #TD-6201 ,'t1.q_bool between 0 and 1 or t2.q_bool between 0 and 1']
        #'t1.q_bool = true and  t1.q_bool = false and t2.q_bool = true and  t2.q_bool = false' , 't1.q_bool = 0 and t1.q_bool = 1 and t2.q_bool = 0 and t2.q_bool = 1' ,

        q_u_or_where = ['(t1.q_binary like \'binary%\'  or t1.q_binary = \'0\'  or t2.q_binary like \'binary%\'  or t2.q_binary = \'0\' )' ,
        '(t1.q_nchar like \'nchar%\' or t1.q_nchar = \'0\' or t2.q_nchar like \'nchar%\' or t2.q_nchar = \'0\' )' , '(t1.q_bool = true or  t1.q_bool = false or t2.q_bool = true or  t2.q_bool = false)' ,
        '(t1.q_bool in (0 , 1) or t2.q_bool in (0 , 1))' , '(t1.q_bool in ( true , false) or t2.q_bool in ( true , false))' , '(t1.q_bool = 0 or t1.q_bool = 1 or t2.q_bool = 0 or t2.q_bool = 1)' ,
        '(t1.q_bigint between  -9223372036854775807 and 9223372036854775807 or t2.q_bigint between  -9223372036854775807 and 9223372036854775807)',
        '(t1.q_int between -2147483647 and 2147483647 or t2.q_int between -2147483647 and 2147483647)',
        '(t1.q_smallint between -32767 and 32767 or t2.q_smallint between -32767 and 32767)',
        '(t1.q_tinyint between -127 and 127 or t2.q_tinyint between -127 and 127 )','(t1.q_float between -1.7E308 and 1.7E308 or t2.q_float between -1.7E308 and 1.7E308)',
        '(t1.q_double between -1.7E308 and 1.7E308 or t2.q_double between -1.7E308 and 1.7E308)']

        # tag column where
        t_where = ['ts < now +1s','t_bigint >= -9223372036854775807 and t_bigint <= 9223372036854775807','t_int <= 2147483647 and t_int >= -2147483647',
        't_smallint >= -32767 and t_smallint <= 32767','q_tinyint >= -127 and t_tinyint <= 127','t_float >= -1.7E308 and t_float <= 1.7E308',
        't_double >= -1.7E308 and t_double <= 1.7E308', 't_binary like \'binary%\'   or t_binary = \'0\' ' , 't_nchar like \'nchar%\' or t_nchar = \'0\'' ,
        't_bool = true or  t_bool = false' , 't_bool in (0 , 1)' , 't_bool in ( true , false)' , 't_bool = 0 or t_bool = 1',
        't_bigint between  -9223372036854775807 and 9223372036854775807',' t_int between -2147483647 and 2147483647','t_smallint between -32767 and 32767',
        't_tinyint between -127 and 127 ','t_float between -1.7E308 and 1.7E308','t_double between -1.7E308 and 1.7E308']
        #TD-6201,'t_bool between 0 and 1'

        # tag column where for test  union,join | this is not support
        t_u_where = ['t1.ts < now +1s' , 't2.ts < now +1s','t1.t_bigint >= -9223372036854775807 and t1.t_bigint <= 9223372036854775807 and t2.t_bigint >= -9223372036854775807 and t2.t_bigint <= 9223372036854775807',
        't1.t_int <= 2147483647 and t1.t_int >= -2147483647 and t2.t_int <= 2147483647 and t2.t_int >= -2147483647',
        't1.t_smallint >= -32767 and t1.t_smallint <= 32767 and t2.t_smallint >= -32767 and t2.t_smallint <= 32767',
        't1.t_tinyint >= -127 and t1.t_tinyint <= 127 and t2.t_tinyint >= -127 and t2.t_tinyint <= 127',
        't1.t_float >= -1.7E308 and t1.t_float <= 1.7E308 and t2.t_float >= -1.7E308 and t2.t_float <= 1.7E308',
        't1.t_double >= -1.7E308 and t1.t_double <= 1.7E308 and t2.t_double >= -1.7E308 and t2.t_double <= 1.7E308',
        '(t1.t_binary like \'binary%\'  or t1.t_binary = \'0\'  or t2.t_binary like \'binary%\'  or t2.t_binary = \'0\') ' ,
        '(t1.t_nchar like \'nchar%\' or t1.t_nchar = \'0\' or t2.t_nchar like \'nchar%\' or t2.t_nchar = \'0\' )' , '(t1.t_bool = true or  t1.t_bool = false or t2.t_bool = true or  t2.t_bool = false)' ,
        't1.t_bool in (0 , 1) and t2.t_bool in (0 , 1)' , 't1.t_bool in ( true , false) and t2.t_bool in ( true , false)' , '(t1.t_bool = 0 or t1.t_bool = 1 or t2.t_bool = 0 or t2.t_bool = 1)',
        't1.t_bigint between  -9223372036854775807 and 9223372036854775807 and t2.t_bigint between  -9223372036854775807 and 9223372036854775807',
        't1.t_int between -2147483647 and 2147483647 and t2.t_int between -2147483647 and 2147483647',
        't1.t_smallint between -32767 and 32767 and t2.t_smallint between -32767 and 32767',
        '(t1.t_tinyint between -127 and 127 and t2.t_tinyint between -127 and 127) ','t1.t_float between -1.7E308 and 1.7E308 and t2.t_float between -1.7E308 and 1.7E308',
        '(t1.t_double between -1.7E308 and 1.7E308 and t2.t_double between -1.7E308 and 1.7E308)']
        #TD-6201,'t1.t_bool between 0 and 1 or t2.q_bool between 0 and 1']

        t_u_or_where = ['(t1.t_binary like \'binary%\'  or t1.t_binary = \'0\'  or t2.t_binary like \'binary%\'  or t2.t_binary = \'0\' )' ,
        '(t1.t_nchar like \'nchar%\' or t1.t_nchar = \'0\' or t2.t_nchar like \'nchar%\' or t2.t_nchar = \'0\' )' , '(t1.t_bool = true or  t1.t_bool = false or t2.t_bool = true or  t2.t_bool = false)' ,
        '(t1.t_bool in (0 , 1) or t2.t_bool in (0 , 1))' , '(t1.t_bool in ( true , false) or t2.t_bool in ( true , false))' , '(t1.t_bool = 0 or t1.t_bool = 1 or t2.t_bool = 0 or t2.t_bool = 1)',
        '(t1.t_bigint between  -9223372036854775807 and 9223372036854775807 or t2.t_bigint between  -9223372036854775807 and 9223372036854775807)',
        '(t1.t_int between -2147483647 and 2147483647 or t2.t_int between -2147483647 and 2147483647)',
        '(t1.t_smallint between -32767 and 32767 or t2.t_smallint between -32767 and 32767)',
        '(t1.t_tinyint between -127 and 127 or t2.t_tinyint between -127 and 127 )','(t1.t_float between -1.7E308 and 1.7E308 or t2.t_float between -1.7E308 and 1.7E308)',
        '(t1.t_double between -1.7E308 and 1.7E308 or t2.t_double between -1.7E308 and 1.7E308)']

        # regular and tag column where
        qt_where = q_where + t_where
        qt_u_where = q_u_where + t_u_where
        # now,qt_u_or_where is not support
        qt_u_or_where = q_u_or_where + t_u_or_where

        # tag column where for test super join | this is  support  , 't1.t_bool = t2.t_bool ' ？？？
        t_join_where = ['t1.t_bigint = t2.t_bigint ', 't1.t_int = t2.t_int ', 't1.t_smallint = t2.t_smallint ', 't1.t_tinyint = t2.t_tinyint ',
                    't1.t_float = t2.t_float ', 't1.t_double = t2.t_double ', 't1.t_binary = t2.t_binary ' , 't1.t_nchar = t2.t_nchar  ' ]

        # session && fill
        session_where = ['session(ts,10a)' , 'session(ts,10s)', 'session(ts,10m)' , 'session(ts,10h)','session(ts,10d)' , 'session(ts,10w)']
        session_u_where = ['session(t1.ts,10a)' , 'session(t1.ts,10s)', 'session(t1.ts,10m)' , 'session(t1.ts,10h)','session(t1.ts,10d)' , 'session(t1.ts,10w)',
                    'session(t2.ts,10a)' , 'session(t2.ts,10s)', 'session(t2.ts,10m)' , 'session(t2.ts,10h)','session(t2.ts,10d)' , 'session(t2.ts,10w)']

        fill_where = ['FILL(NONE)','FILL(PREV)','FILL(NULL)','FILL(LINEAR)','FILL(NEXT)','FILL(VALUE, 1.23)']

        state_window = ['STATE_WINDOW(q_tinyint)','STATE_WINDOW(q_bigint)','STATE_WINDOW(q_int)','STATE_WINDOW(q_bool)','STATE_WINDOW(q_smallint)']
        state_u_window = ['STATE_WINDOW(t1.q_tinyint)','STATE_WINDOW(t1.q_bigint)','STATE_WINDOW(t1.q_int)','STATE_WINDOW(t1.q_bool)','STATE_WINDOW(t1.q_smallint)',
                    'STATE_WINDOW(t2.q_tinyint)','STATE_WINDOW(t2.q_bigint)','STATE_WINDOW(t2.q_int)','STATE_WINDOW(t2.q_bool)','STATE_WINDOW(t2.q_smallint)']

        # order by where
        order_where = ['order by ts' , 'order by ts asc']
        order_u_where = ['order by t1.ts' , 'order by t1.ts asc' , 'order by t2.ts' , 'order by t2.ts asc']
        order_desc_where = ['order by ts' , 'order by ts asc' , 'order by ts desc' ]
        orders_desc_where = ['order by ts' , 'order by ts asc' , 'order by ts desc' , 'order by loc' , 'order by loc asc' , 'order by loc desc']

        group_where = ['group by tbname , loc' , 'group by tbname', 'group by tbname, t_bigint', 'group by tbname,t_int', 'group by tbname, t_smallint', 'group by tbname,t_tinyint',
                    'group by tbname,t_float', 'group by tbname,t_double' , 'group by tbname,t_binary', 'group by tbname,t_nchar', 'group by tbname,t_bool' ,'group by tbname ,loc ,t_bigint',
                    'group by tbname,t_binary ,t_nchar ,t_bool' , 'group by tbname,t_int ,t_smallint ,t_tinyint' , 'group by tbname,t_float ,t_double ' ,
                    'PARTITION BY tbname , loc' , 'PARTITION BY tbname', 'PARTITION BY tbname, t_bigint', 'PARTITION BY tbname,t_int', 'PARTITION BY tbname, t_smallint', 'PARTITION BY tbname,t_tinyint',
                    'PARTITION BY tbname,t_float', 'PARTITION BY tbname,t_double' , 'PARTITION BY tbname,t_binary', 'PARTITION BY tbname,t_nchar', 'PARTITION BY tbname,t_bool' ,'PARTITION BY tbname ,loc ,t_bigint',
                    'PARTITION BY tbname,t_binary ,t_nchar ,t_bool' , 'PARTITION BY tbname,t_int ,t_smallint ,t_tinyint' , 'PARTITION BY tbname,t_float ,t_double ']
        group_where_j = ['group by  t1.loc' , 'group by t1.t_bigint', 'group by t1.t_int', 'group by t1.t_smallint', 'group by t1.t_tinyint',
                    'group by t1.t_float', 'group by t1.t_double' , 'group by t1.t_binary', 'group by t1.t_nchar', 'group by t1.t_bool' ,'group by t1.loc ,t1.t_bigint',
                    'group by t1.t_binary ,t1.t_nchar ,t1.t_bool' , 'group by t1.t_int ,t1.t_smallint ,t1.t_tinyint' , 'group by t1.t_float ,t1.t_double ' ,
                    'PARTITION BY t1.loc' , 'PARTITION by t1.t_bigint', 'PARTITION by t1.t_int', 'PARTITION by t1.t_smallint', 'PARTITION by t1.t_tinyint',
                    'PARTITION by t1.t_float', 'PARTITION by t1.t_double' , 'PARTITION by t1.t_binary', 'PARTITION by t1.t_nchar', 'PARTITION by t1.t_bool' ,'PARTITION BY t1.loc ,t1.t_bigint',
                    'PARTITION by t1.t_binary ,t1.t_nchar ,t1.t_bool' , 'PARTITION by t1.t_int ,t1.t_smallint ,t1.t_tinyint' , 'PARTITION by t1.t_float ,t1.t_double ',
                    'group by  t2.loc' , 'group by t2.t_bigint', 'group by t2.t_int', 'group by t2.t_smallint', 'group by t2.t_tinyint',
                    'group by t2.t_float', 'group by t2.t_double' , 'group by t2.t_binary', 'group by t2.t_nchar', 'group by t2.t_bool' ,'group by t2.loc ,t2.t_bigint',
                    'group by t2.t_binary ,t2.t_nchar ,t2.t_bool' , 'group by t2.t_int ,t2.t_smallint ,t2.t_tinyint' , 'group by t2.t_float ,t2.t_double ' ,
                    'PARTITION BY t2.loc' , 'PARTITION by t2.t_bigint', 'PARTITION by t2.t_int', 'PARTITION by t2.t_smallint', 'PARTITION by t2.t_tinyint',
                    'PARTITION by t2.t_float', 'PARTITION by t2.t_double' , 'PARTITION by t2.t_binary', 'PARTITION by t2.t_nchar', 'PARTITION by t2.t_bool' ,'PARTITION BY t2.loc ,t2.t_bigint',
                    'PARTITION by t2.t_binary ,t2.t_nchar ,t2.t_bool' , 'PARTITION by t2.t_int ,t2.t_smallint ,t2.t_tinyint' , 'PARTITION by t2.t_float ,t2.t_double ']

        partiton_where = ['PARTITION BY tbname , loc' , 'PARTITION BY tbname', 'PARTITION BY tbname, t_bigint', 'PARTITION BY tbname,t_int', 'PARTITION BY tbname, t_smallint', 'PARTITION BY tbname,t_tinyint',
                    'PARTITION BY tbname,t_float', 'PARTITION BY tbname,t_double' , 'PARTITION BY tbname,t_binary', 'PARTITION BY tbname,t_nchar', 'PARTITION BY tbname,t_bool' ,'PARTITION BY tbname ,loc ,t_bigint',
                    'PARTITION BY tbname,t_binary ,t_nchar ,t_bool' , 'PARTITION BY tbname,t_int ,t_smallint ,t_tinyint' , 'PARTITION BY tbname,t_float ,t_double ']
        partiton_where_j = ['PARTITION BY t1.loc' , 'PARTITION by t1.t_bigint', 'PARTITION by t1.t_int', 'PARTITION by t1.t_smallint', 'PARTITION by t1.t_tinyint',
                    'PARTITION by t1.t_float', 'PARTITION by t1.t_double' , 'PARTITION by t1.t_binary', 'PARTITION by t1.t_nchar', 'PARTITION by t1.t_bool' ,'PARTITION BY t1.loc ,t1.t_bigint',
                    'PARTITION by t1.t_binary ,t1.t_nchar ,t1.t_bool' , 'PARTITION by t1.t_int ,t1.t_smallint ,t1.t_tinyint' , 'PARTITION by t1.t_float ,t1.t_double ',
                    'PARTITION BY t2.loc' , 'PARTITION by t2.t_bigint', 'PARTITION by t2.t_int', 'PARTITION by t2.t_smallint', 'PARTITION by t2.t_tinyint',
                    'PARTITION by t2.t_float', 'PARTITION by t2.t_double' , 'PARTITION by t2.t_binary', 'PARTITION by t2.t_nchar', 'PARTITION by t2.t_bool' ,'PARTITION BY t2.loc ,t2.t_bigint',
                    'PARTITION by t2.t_binary ,t2.t_nchar ,t2.t_bool' , 'PARTITION by t2.t_int ,t2.t_smallint ,t2.t_tinyint' , 'PARTITION by t2.t_float ,t2.t_double ']


        group_where_regular = ['group by tbname ' , 'group by tbname', 'group by tbname, q_bigint', 'group by tbname,q_int', 'group by tbname, q_smallint', 'group by tbname,q_tinyint',
                    'group by tbname,q_float', 'group by tbname,q_double' , 'group by tbname,q_binary', 'group by tbname,q_nchar', 'group by tbname,q_bool' ,'group by tbname ,q_bigint',
                    'group by tbname,q_binary ,q_nchar ,q_bool' , 'group by tbname,q_int ,q_smallint ,q_tinyint' , 'group by tbname,q_float ,q_double ' ,
                    'PARTITION BY tbname ' , 'PARTITION BY tbname', 'PARTITION BY tbname, q_bigint', 'PARTITION BY tbname,q_int', 'PARTITION BY tbname, q_smallint', 'PARTITION BY tbname,q_tinyint',
                    'PARTITION BY tbname,q_float', 'PARTITION BY tbname,q_double' , 'PARTITION BY tbname,q_binary', 'PARTITION BY tbname,q_nchar', 'PARTITION BY tbname,q_bool' ,'PARTITION BY tbname ,q_bigint',
                    'PARTITION BY tbname,q_binary ,q_nchar ,q_bool' , 'PARTITION BY tbname,q_int ,q_smallint ,q_tinyint' , 'PARTITION BY tbname,q_float ,q_double ']
        group_where_regular_j = ['group by t1.q_bigint', 'group by t1.q_int', 'group by t1.q_smallint', 'group by t1.q_tinyint',
                    'group by t1.q_float', 'group by t1.q_double' , 'group by t1.q_binary', 'group by t1.q_nchar', 'group by t1.q_bool' ,'group by t1.q_bigint',
                    'group by t1.q_binary ,t1.q_nchar ,t1.q_bool' , 'group by t1.q_int ,t1.q_smallint ,t1.q_tinyint' , 'group by t1.q_float ,t1.q_double ' ,
                    'PARTITION by t1.q_bigint', 'PARTITION by t1.q_int', 'PARTITION by t1.q_smallint', 'PARTITION by t1.q_tinyint',
                    'PARTITION by t1.q_float', 'PARTITION by t1.q_double' , 'PARTITION by t1.q_binary', 'PARTITION by t1.q_nchar', 'PARTITION by t1.q_bool' ,'PARTITION BY t1.q_bigint',
                    'PARTITION by t1.q_binary ,t1.q_nchar ,t1.q_bool' , 'PARTITION by t1.q_int ,t1.q_smallint ,t1.q_tinyint' , 'PARTITION by t1.q_float ,t1.q_double ',
                    'group by t2.q_bigint', 'group by t2.q_int', 'group by t2.q_smallint', 'group by t2.q_tinyint',
                    'group by t2.q_float', 'group by t2.q_double' , 'group by t2.q_binary', 'group by t2.q_nchar', 'group by t2.q_bool' ,'group by t2.q_bigint',
                    'group by t2.q_binary ,t2.q_nchar ,t2.q_bool' , 'group by t2.q_int ,t2.q_smallint ,t2.q_tinyint' , 'group by t2.q_float ,t2.q_double ' ,
                    'PARTITION by t2.q_bigint', 'PARTITION by t2.q_int', 'PARTITION by t2.q_smallint', 'PARTITION by t2.q_tinyint',
                    'PARTITION by t2.q_float', 'PARTITION by t2.q_double' , 'PARTITION by t2.q_binary', 'PARTITION by t2.q_nchar', 'PARTITION by t2.q_bool' ,'PARTITION BY t2.q_bigint',
                    'PARTITION by t2.q_binary ,t2.q_nchar ,t2.q_bool' , 'PARTITION by t2.q_int ,t2.q_smallint ,t2.q_tinyint' , 'PARTITION by t2.q_float ,t2.q_double ']

        partiton_where_regular = ['PARTITION BY tbname ' , 'PARTITION BY tbname', 'PARTITION BY tbname, q_bigint', 'PARTITION BY tbname,q_int', 'PARTITION BY tbname, q_smallint', 'PARTITION BY tbname,q_tinyint',
                    'PARTITION BY tbname,q_float', 'PARTITION BY tbname,q_double' , 'PARTITION BY tbname,q_binary', 'PARTITION BY tbname,q_nchar', 'PARTITION BY tbname,q_bool' ,'PARTITION BY tbname ,q_bigint',
                    'PARTITION BY tbname,q_binary ,q_nchar ,q_bool' , 'PARTITION BY tbname,q_int ,q_smallint ,q_tinyint' , 'PARTITION BY tbname,q_float ,q_double ']
        partiton_where_regular_j = ['PARTITION by t1.q_bigint', 'PARTITION by t1.q_int', 'PARTITION by t1.q_smallint', 'PARTITION by t1.q_tinyint',
                    'PARTITION by t1.q_float', 'PARTITION by t1.q_double' , 'PARTITION by t1.q_binary', 'PARTITION by t1.q_nchar', 'PARTITION by t1.q_bool' ,'PARTITION BY t1.q_bigint',
                    'PARTITION by t1.q_binary ,t1.q_nchar ,t1.q_bool' , 'PARTITION by t1.q_int ,t1.q_smallint ,t1.q_tinyint' , 'PARTITION by t1.q_float ,t1.q_double ',
                    'PARTITION by t2.q_bigint', 'PARTITION by t2.q_int', 'PARTITION by t2.q_smallint', 'PARTITION by t2.q_tinyint',
                    'PARTITION by t2.q_float', 'PARTITION by t2.q_double' , 'PARTITION by t2.q_binary', 'PARTITION by t2.q_nchar', 'PARTITION by t2.q_bool' ,'PARTITION BY t2.q_bigint',
                    'PARTITION by t2.q_binary ,t2.q_nchar ,t2.q_bool' , 'PARTITION by t2.q_int ,t2.q_smallint ,t2.q_tinyint' , 'PARTITION by t2.q_float ,t2.q_double ']

        having_support = ['having count(q_int) > 0','having count(q_bigint) > 0','having count(q_smallint) > 0','having count(q_tinyint) > 0','having count(q_float) > 0','having count(q_double) > 0','having count(q_bool) > 0',
                    'having avg(q_int) > 0','having avg(q_bigint) > 0','having avg(q_smallint) > 0','having avg(q_tinyint) > 0','having avg(q_float) > 0','having avg(q_double) > 0',
                    'having sum(q_int) > 0','having sum(q_bigint) > 0','having sum(q_smallint) > 0','having sum(q_tinyint) > 0','having sum(q_float) > 0','having sum(q_double) > 0',
                    'having STDDEV(q_int) > 0','having STDDEV(q_bigint) > 0','having STDDEV(q_smallint) > 0','having STDDEV(q_tinyint) > 0','having STDDEV(q_float) > 0','having STDDEV(q_double) > 0',
                    'having TWA(q_int) > 0','having  TWA(q_bigint) > 0','having  TWA(q_smallint) > 0','having  TWA(q_tinyint) > 0','having  TWA(q_float) > 0','having  TWA(q_double) > 0',
                    'having IRATE(q_int) > 0','having IRATE(q_bigint) > 0','having IRATE(q_smallint) > 0','having IRATE(q_tinyint) > 0','having IRATE(q_float) > 0','having IRATE(q_double) > 0',
                    'having MIN(q_int) > 0','having MIN(q_bigint) > 0','having MIN(q_smallint) > 0','having MIN(q_tinyint) > 0','having MIN(q_float) > 0','having MIN(q_double) > 0',
                    'having MAX(q_int) > 0','having MAX(q_bigint) > 0','having MAX(q_smallint) > 0','having MAX(q_tinyint) > 0','having MAX(q_float) > 0','having MAX(q_double) > 0',
                    'having FIRST(q_int) > 0','having FIRST(q_bigint) > 0','having FIRST(q_smallint) > 0','having FIRST(q_tinyint) > 0','having FIRST(q_float) > 0','having FIRST(q_double) > 0',
                    'having LAST(q_int) > 0','having LAST(q_bigint) > 0','having LAST(q_smallint) > 0','having LAST(q_tinyint) > 0','having LAST(q_float) > 0','having LAST(q_double) > 0',
                    'having APERCENTILE(q_int,10) > 0','having APERCENTILE(q_bigint,10) > 0','having APERCENTILE(q_smallint,10) > 0','having APERCENTILE(q_tinyint,10) > 0','having APERCENTILE(q_float,10) > 0','having APERCENTILE(q_double,10) > 0']
        having_not_support = ['having TOP(q_int,10) > 0','having TOP(q_bigint,10) > 0','having TOP(q_smallint,10) > 0','having TOP(q_tinyint,10) > 0','having TOP(q_float,10) > 0','having TOP(q_double,10) > 0','having TOP(q_bool,10) > 0',
                    'having BOTTOM(q_int,10) > 0','having BOTTOM(q_bigint,10) > 0','having BOTTOM(q_smallint,10) > 0','having BOTTOM(q_tinyint,10) > 0','having BOTTOM(q_float,10) > 0','having BOTTOM(q_double,10) > 0','having BOTTOM(q_bool,10) > 0',
                    'having LEASTSQUARES(q_int) > 0','having  LEASTSQUARES(q_bigint) > 0','having  LEASTSQUARES(q_smallint) > 0','having  LEASTSQUARES(q_tinyint) > 0','having  LEASTSQUARES(q_float) > 0','having  LEASTSQUARES(q_double) > 0','having  LEASTSQUARES(q_bool) > 0',
                    'having FIRST(q_bool) > 0','having IRATE(q_bool) > 0','having PERCENTILE(q_bool,10) > 0','having avg(q_bool) > 0','having LAST_ROW(q_bool) > 0','having sum(q_bool) > 0','having STDDEV(q_bool) > 0','having APERCENTILE(q_bool,10) > 0','having  TWA(q_bool) > 0','having LAST(q_bool) > 0',
                    'having PERCENTILE(q_int,10) > 0','having PERCENTILE(q_bigint,10) > 0','having PERCENTILE(q_smallint,10) > 0','having PERCENTILE(q_tinyint,10) > 0','having PERCENTILE(q_float,10) > 0','having PERCENTILE(q_double,10) > 0']
        having_tagnot_support = ['having LAST_ROW(q_int) > 0','having LAST_ROW(q_bigint) > 0','having LAST_ROW(q_smallint) > 0','having LAST_ROW(q_tinyint) > 0','having LAST_ROW(q_float) > 0','having LAST_ROW(q_double) > 0']

        having_support_j = ['having count(t1.q_int) > 0','having count(t1.q_bigint) > 0','having count(t1.q_smallint) > 0','having count(t1.q_tinyint) > 0','having count(t1.q_float) > 0','having count(t1.q_double) > 0','having count(t1.q_bool) > 0',
                    'having avg(t1.q_int) > 0','having avg(t1.q_bigint) > 0','having avg(t1.q_smallint) > 0','having avg(t1.q_tinyint) > 0','having avg(t1.q_float) > 0','having avg(t1.q_double) > 0',
                    'having sum(t1.q_int) > 0','having sum(t1.q_bigint) > 0','having sum(t1.q_smallint) > 0','having sum(t1.q_tinyint) > 0','having sum(t1.q_float) > 0','having sum(t1.q_double) > 0',
                    'having STDDEV(t1.q_int) > 0','having STDDEV(t1.q_bigint) > 0','having STDDEV(t1.q_smallint) > 0','having STDDEV(t1.q_tinyint) > 0','having STDDEV(t1.q_float) > 0','having STDDEV(t1.q_double) > 0',
                    'having TWA(t1.q_int) > 0','having  TWA(t1.q_bigint) > 0','having  TWA(t1.q_smallint) > 0','having  TWA(t1.q_tinyint) > 0','having  TWA(t1.q_float) > 0','having  TWA(t1.q_double) > 0',
                    'having IRATE(t1.q_int) > 0','having IRATE(t1.q_bigint) > 0','having IRATE(t1.q_smallint) > 0','having IRATE(t1.q_tinyint) > 0','having IRATE(t1.q_float) > 0','having IRATE(t1.q_double) > 0',
                    'having MIN(t1.q_int) > 0','having MIN(t1.q_bigint) > 0','having MIN(t1.q_smallint) > 0','having MIN(t1.q_tinyint) > 0','having MIN(t1.q_float) > 0','having MIN(t1.q_double) > 0',
                    'having MAX(t1.q_int) > 0','having MAX(t1.q_bigint) > 0','having MAX(t1.q_smallint) > 0','having MAX(t1.q_tinyint) > 0','having MAX(t1.q_float) > 0','having MAX(t1.q_double) > 0',
                    'having FIRST(t1.q_int) > 0','having FIRST(t1.q_bigint) > 0','having FIRST(t1.q_smallint) > 0','having FIRST(t1.q_tinyint) > 0','having FIRST(t1.q_float) > 0','having FIRST(t1.q_double) > 0',
                    'having LAST(t1.q_int) > 0','having LAST(t1.q_bigint) > 0','having LAST(t1.q_smallint) > 0','having LAST(t1.q_tinyint) > 0','having LAST(t1.q_float) > 0','having LAST(t1.q_double) > 0',
                    'having APERCENTILE(t1.q_int,10) > 0','having APERCENTILE(t1.q_bigint,10) > 0','having APERCENTILE(t1.q_smallint,10) > 0','having APERCENTILE(t1.q_tinyint,10) > 0','having APERCENTILE(t1.q_float,10) > 0','having APERCENTILE(t1.q_double,10) > 0']

        # limit offset where
        limit_where = ['limit 1 offset 1' , 'limit 1' , 'limit 2 offset 1' , 'limit 2', 'limit 12 offset 1' , 'limit 20', 'limit 20 offset 10' , 'limit 200']
        limit1_where = ['limit 1 offset 1' , 'limit 1' ]
        limit_u_where = ['limit 100 offset 10' , 'limit 50' , 'limit 100' , 'limit 10' ]

        # slimit soffset where
        slimit_where = ['slimit 1 soffset 1' , 'slimit 1' , 'slimit 2 soffset 1' , 'slimit 2']
        slimit1_where = ['slimit 2 soffset 1' , 'slimit 1' ]

        # aggregate function include [all:count(*)\avg\sum\stddev ||regualr:twa\irate\leastsquares ||group by tbname:twa\irate\]
        # select function include [all: min\max\first(*)\last(*)\top\bottom\apercentile\last_row(*)(not with interval)\interp(*)(FILL) ||regualr: percentile]
        # calculation function include [all:spread\+-*/ ||regualr:diff\derivative ||group by tbname:diff\derivative\]
        # **_ns_**  express is not support stable, therefore, separated from regular tables
        # calc_select_all   calc_select_regular  calc_select_in_ts  calc_select_fill  calc_select_not_interval
        # calc_aggregate_all   calc_aggregate_regular   calc_aggregate_groupbytbname
        # calc_calculate_all   calc_calculate_regular   calc_calculate_groupbytbname

        # calc_select_all   calc_select_regular  calc_select_in_ts  calc_select_fill  calc_select_not_interval
        # select function include [all: min\max\first(*)\last(*)\top\bottom\apercentile\last_row(*)(not with interval)\interp(*)(FILL) ||regualr: percentile]

        calc_select_all = ['bottom(q_int,20)' , 'bottom(q_bigint,20)' , 'bottom(q_smallint,20)' , 'bottom(q_tinyint,20)' ,'bottom(q_float,20)' , 'bottom(q_double,20)' ,
                    'top(q_int,20)' , 'top(q_bigint,20)' , 'top(q_smallint,20)' ,'top(q_tinyint,20)' ,'top(q_float,20)' ,'top(q_double,20)' ,
                    'first(q_int)' , 'first(q_bigint)' , 'first(q_smallint)' , 'first(q_tinyint)' , 'first(q_float)' ,'first(q_double)' ,'first(q_binary)' ,'first(q_nchar)' ,'first(q_bool)' ,'first(q_ts)' ,
                    'last(q_int)' ,  'last(q_bigint)' , 'last(q_smallint)'  , 'last(q_tinyint)' , 'last(q_float)'  ,'last(q_double)' , 'last(q_binary)' ,'last(q_nchar)' ,'last(q_bool)' ,'last(q_ts)' ,
                    'min(q_int)' , 'min(q_bigint)' , 'min(q_smallint)' , 'min(q_tinyint)' , 'min(q_float)' ,'min(q_double)' ,
                    'max(q_int)' ,  'max(q_bigint)' , 'max(q_smallint)' , 'max(q_tinyint)' ,'max(q_float)' ,'max(q_double)' ,
                    'apercentile(q_int,20)' ,  'apercentile(q_bigint,20)'  ,'apercentile(q_smallint,20)'  ,'apercentile(q_tinyint,20)' ,'apercentile(q_float,20)'  ,'apercentile(q_double,20)' ,
                    'last_row(q_int)' ,  'last_row(q_bigint)' , 'last_row(q_smallint)' , 'last_row(q_tinyint)' , 'last_row(q_float)' ,
                    'last_row(q_double)' , 'last_row(q_bool)' ,'last_row(q_binary)' ,'last_row(q_nchar)' ,'last_row(q_ts)']

        calc_select_in_ts = ['bottom(q_int,20)' , 'bottom(q_bigint,20)' , 'bottom(q_smallint,20)' , 'bottom(q_tinyint,20)' ,'bottom(q_float,20)' , 'bottom(q_double,20)' ,
                    'top(q_int,20)' , 'top(q_bigint,20)' , 'top(q_smallint,20)' ,'top(q_tinyint,20)' ,'top(q_float,20)' ,'top(q_double,20)' ,
                    'first(q_int)' , 'first(q_bigint)' , 'first(q_smallint)' , 'first(q_tinyint)' , 'first(q_float)' ,'first(q_double)' ,'first(q_binary)' ,'first(q_nchar)' ,'first(q_bool)' ,'first(q_ts)' ,
                    'last(q_int)' ,  'last(q_bigint)' , 'last(q_smallint)'  , 'last(q_tinyint)' , 'last(q_float)'  ,'last(q_double)' , 'last(q_binary)' ,'last(q_nchar)' ,'last(q_bool)' ,'last(q_ts)' ]

        calc_select_in = ['min(q_int)' , 'min(q_bigint)' , 'min(q_smallint)' , 'min(q_tinyint)' , 'min(q_float)' ,'min(q_double)' ,
                    'max(q_int)' ,  'max(q_bigint)' , 'max(q_smallint)' , 'max(q_tinyint)' ,'max(q_float)' ,'max(q_double)' ,
                    'apercentile(q_int,20)' ,  'apercentile(q_bigint,20)'  ,'apercentile(q_smallint,20)'  ,'apercentile(q_tinyint,20)' ,'apercentile(q_float,20)'  ,'apercentile(q_double,20)' ,
                    'last_row(q_int)' ,  'last_row(q_bigint)' , 'last_row(q_smallint)' , 'last_row(q_tinyint)' , 'last_row(q_float)' ,
                    'last_row(q_double)' , 'last_row(q_bool)' ,'last_row(q_binary)' ,'last_row(q_nchar)' ,'last_row(q_ts)']

        calc_select_not_support_ts = ['first(q_int)' , 'first(q_bigint)' , 'first(q_smallint)' , 'first(q_tinyint)' , 'first(q_float)' ,'first(q_double)' ,'first(q_binary)' ,'first(q_nchar)' ,'first(q_bool)' ,'first(q_ts)' ,
                    'last(q_int)' ,  'last(q_bigint)' , 'last(q_smallint)'  , 'last(q_tinyint)' , 'last(q_float)'  ,'last(q_double)' , 'last(q_binary)' ,'last(q_nchar)' ,'last(q_bool)' ,'last(q_ts)' ,
                    'last_row(q_int)' ,  'last_row(q_bigint)' , 'last_row(q_smallint)' , 'last_row(q_tinyint)' , 'last_row(q_float)' ,
                    'last_row(q_double)' , 'last_row(q_bool)' ,'last_row(q_binary)' ,'last_row(q_nchar)' ,'last_row(q_ts)',
                    'apercentile(q_int,20)' ,  'apercentile(q_bigint,20)'  ,'apercentile(q_smallint,20)'  ,'apercentile(q_tinyint,20)' ,'apercentile(q_float,20)'  ,'apercentile(q_double,20)']

        calc_select_support_ts = ['bottom(q_int,20)' , 'bottom(q_bigint,20)' , 'bottom(q_smallint,20)' , 'bottom(q_tinyint,20)' ,'bottom(q_float,20)' , 'bottom(q_double,20)' ,
                    'top(q_int,20)' , 'top(q_bigint,20)' , 'top(q_smallint,20)' ,'top(q_tinyint,20)' ,'top(q_float,20)' ,'top(q_double,20)' ,
                    'min(q_int)' , 'min(q_bigint)' , 'min(q_smallint)' , 'min(q_tinyint)' , 'min(q_float)' ,'min(q_double)' ,
                    'max(q_int)' ,  'max(q_bigint)' , 'max(q_smallint)' , 'max(q_tinyint)' ,'max(q_float)' ,'max(q_double)'  ]

        calc_select_regular = [ 'PERCENTILE(q_int,10)' ,'PERCENTILE(q_bigint,20)' , 'PERCENTILE(q_smallint,30)' ,'PERCENTILE(q_tinyint,40)' ,'PERCENTILE(q_float,50)' ,'PERCENTILE(q_double,60)']


        calc_select_fill = ['INTERP(q_int)' ,'INTERP(q_bigint)' ,'INTERP(q_smallint)' ,'INTERP(q_tinyint)', 'INTERP(q_float)' ,'INTERP(q_double)']
        interp_where = ['ts = now' , 'ts = \'2020-09-13 20:26:40.000\'' , 'ts = \'2020-09-13 20:26:40.009\'' ,'tbname in (\'table_1\') and ts = now' ,'tbname in (\'table_0\' ,\'table_1\',\'table_2\',\'table_3\',\'table_4\',\'table_5\') and ts =  \'2020-09-13 20:26:40.000\'','tbname like \'table%\'  and ts =  \'2020-09-13 20:26:40.002\'']

        #two table join
        calc_select_in_ts_j = ['bottom(t1.q_int,20)' , 'bottom(t1.q_bigint,20)' , 'bottom(t1.q_smallint,20)' , 'bottom(t1.q_tinyint,20)' ,'bottom(t1.q_float,20)' , 'bottom(t1.q_double,20)' ,
                    'top(t1.q_int,20)' , 'top(t1.q_bigint,20)' , 'top(t1.q_smallint,20)' ,'top(t1.q_tinyint,20)' ,'top(t1.q_float,20)' ,'top(t1.q_double,20)' ,
                    'first(t1.q_int)' , 'first(t1.q_bigint)' , 'first(t1.q_smallint)' , 'first(t1.q_tinyint)' , 'first(t1.q_float)' ,'first(t1.q_double)' ,'first(t1.q_binary)' ,'first(t1.q_nchar)' ,'first(t1.q_bool)' ,'first(t1.q_ts)' ,
                    'last(t1.q_int)' ,  'last(t1.q_bigint)' , 'last(t1.q_smallint)'  , 'last(t1.q_tinyint)' , 'last(t1.q_float)'  ,'last(t1.q_double)' , 'last(t1.q_binary)' ,'last(t1.q_nchar)' ,'last(t1.q_bool)' ,'last(t1.q_ts)' ,
                    'bottom(t2.q_int,20)' , 'bottom(t2.q_bigint,20)' , 'bottom(t2.q_smallint,20)' , 'bottom(t2.q_tinyint,20)' ,'bottom(t2.q_float,20)' , 'bottom(t2.q_double,20)' ,
                    'top(t2.q_int,20)' , 'top(t2.q_bigint,20)' , 'top(t2.q_smallint,20)' ,'top(t2.q_tinyint,20)' ,'top(t2.q_float,20)' ,'top(t2.q_double,20)' ,
                    'first(t2.q_int)' , 'first(t2.q_bigint)' , 'first(t2.q_smallint)' , 'first(t2.q_tinyint)' , 'first(t2.q_float)' ,'first(t2.q_double)' ,'first(t2.q_binary)' ,'first(t2.q_nchar)' ,'first(t2.q_bool)' ,'first(t2.q_ts)' ,
                    'last(t2.q_int)' ,  'last(t2.q_bigint)' , 'last(t2.q_smallint)'  , 'last(t2.q_tinyint)' , 'last(t2.q_float)'  ,'last(t2.q_double)' , 'last(t2.q_binary)' ,'last(t2.q_nchar)' ,'last(t2.q_bool)' ,'last(t2.q_ts)']

        calc_select_in_support_ts_j = ['bottom(t1.q_int,20)' , 'bottom(t1.q_bigint,20)' , 'bottom(t1.q_smallint,20)' , 'bottom(t1.q_tinyint,20)' ,'bottom(t1.q_float,20)' , 'bottom(t1.q_double,20)' ,
                    'top(t1.q_int,20)' , 'top(t1.q_bigint,20)' , 'top(t1.q_smallint,20)' ,'top(t1.q_tinyint,20)' ,'top(t1.q_float,20)' ,'top(t1.q_double,20)' ,
                    'min(t1.q_int)' , 'min(t1.q_bigint)' , 'min(t1.q_smallint)' , 'min(t1.q_tinyint)' , 'min(t1.q_float)' ,'min(t1.q_double)' ,
                    'max(t1.q_int)' ,  'max(t1.q_bigint)' , 'max(t1.q_smallint)' , 'max(t1.q_tinyint)' ,'max(t1.q_float)' ,'max(t1.q_double)' ,
                    'bottom(t2.q_int,20)' , 'bottom(t2.q_bigint,20)' , 'bottom(t2.q_smallint,20)' , 'bottom(t2.q_tinyint,20)' ,'bottom(t2.q_float,20)' , 'bottom(t2.q_double,20)' ,
                    'top(t2.q_int,20)' , 'top(t2.q_bigint,20)' , 'top(t2.q_smallint,20)' ,'top(t2.q_tinyint,20)' ,'top(t2.q_float,20)' ,'top(t2.q_double,20)' ,
                    'min(t2.q_int)' , 'min(t2.q_bigint)' , 'min(t2.q_smallint)' , 'min(t2.q_tinyint)' , 'min(t2.q_float)' ,'min(t2.q_double)' ,
                    'max(t2.q_int)' ,  'max(t2.q_bigint)' , 'max(t2.q_smallint)' , 'max(t2.q_tinyint)' ,'max(t2.q_float)' ,'max(t2.q_double)' ,
                    ]

        calc_select_in_not_support_ts_j = ['apercentile(t1.q_int,20)' ,  'apercentile(t1.q_bigint,20)'  ,'apercentile(t1.q_smallint,20)'  ,'apercentile(t1.q_tinyint,20)' ,'apercentile(t1.q_float,20)'  ,'apercentile(t1.q_double,20)' ,
                    'last_row(t1.q_int)' ,  'last_row(t1.q_bigint)' , 'last_row(t1.q_smallint)' , 'last_row(t1.q_tinyint)' , 'last_row(t1.q_float)' ,
                    'last_row(t1.q_double)' , 'last_row(t1.q_bool)' ,'last_row(t1.q_binary)' ,'last_row(t1.q_nchar)' ,'last_row(t1.q_ts)' ,
                    'apercentile(t2.q_int,20)' ,  'apercentile(t2.q_bigint,20)'  ,'apercentile(t2.q_smallint,20)'  ,'apercentile(t2.q_tinyint,20)' ,'apercentile(t2.q_float,20)'  ,'apercentile(t2.q_double,20)' ,
                    'last_row(t2.q_int)' ,  'last_row(t2.q_bigint)' , 'last_row(t2.q_smallint)' , 'last_row(t2.q_tinyint)' , 'last_row(t2.q_float)' ,
                    'last_row(t2.q_double)' , 'last_row(t2.q_bool)' ,'last_row(t2.q_binary)' ,'last_row(t2.q_nchar)' ,'last_row(t2.q_ts)']

        calc_select_in_j = ['min(t1.q_int)' , 'min(t1.q_bigint)' , 'min(t1.q_smallint)' , 'min(t1.q_tinyint)' , 'min(t1.q_float)' ,'min(t1.q_double)' ,
                    'max(t1.q_int)' ,  'max(t1.q_bigint)' , 'max(t1.q_smallint)' , 'max(t1.q_tinyint)' ,'max(t1.q_float)' ,'max(t1.q_double)' ,
                    'apercentile(t1.q_int,20)' ,  'apercentile(t1.q_bigint,20)'  ,'apercentile(t1.q_smallint,20)'  ,'apercentile(t1.q_tinyint,20)' ,'apercentile(t1.q_float,20)'  ,'apercentile(t1.q_double,20)' ,
                    'last_row(t1.q_int)' ,  'last_row(t1.q_bigint)' , 'last_row(t1.q_smallint)' , 'last_row(t1.q_tinyint)' , 'last_row(t1.q_float)' ,
                    'last_row(t1.q_double)' , 'last_row(t1.q_bool)' ,'last_row(t1.q_binary)' ,'last_row(t1.q_nchar)' ,'last_row(t1.q_ts)' ,
                    'min(t2.q_int)' , 'min(t2.q_bigint)' , 'min(t2.q_smallint)' , 'min(t2.q_tinyint)' , 'min(t2.q_float)' ,'min(t2.q_double)' ,
                    'max(t2.q_int)' ,  'max(t2.q_bigint)' , 'max(t2.q_smallint)' , 'max(t2.q_tinyint)' ,'max(t2.q_float)' ,'max(t2.q_double)' ,
                    'apercentile(t2.q_int,20)' ,  'apercentile(t2.q_bigint,20)'  ,'apercentile(t2.q_smallint,20)'  ,'apercentile(t2.q_tinyint,20)' ,'apercentile(t2.q_float,20)'  ,'apercentile(t2.q_double,20)' ,
                    'last_row(t2.q_int)' ,  'last_row(t2.q_bigint)' , 'last_row(t2.q_smallint)' , 'last_row(t2.q_tinyint)' , 'last_row(t2.q_float)' ,
                    'last_row(t2.q_double)' , 'last_row(t2.q_bool)' ,'last_row(t2.q_binary)' ,'last_row(t2.q_nchar)' ,'last_row(t2.q_ts)']
        calc_select_all_j = calc_select_in_ts_j + calc_select_in_j

        calc_select_regular_j = [ 'PERCENTILE(t1.q_int,10)' ,'PERCENTILE(t1.q_bigint,20)' , 'PERCENTILE(t1.q_smallint,30)' ,'PERCENTILE(t1.q_tinyint,40)' ,'PERCENTILE(t1.q_float,50)' ,'PERCENTILE(t1.q_double,60)' ,
                    'PERCENTILE(t2.q_int,10)' ,'PERCENTILE(t2.q_bigint,20)' , 'PERCENTILE(t2.q_smallint,30)' ,'PERCENTILE(t2.q_tinyint,40)' ,'PERCENTILE(t2.q_float,50)' ,'PERCENTILE(t2.q_double,60)']


        calc_select_fill_j = ['INTERP(t1.q_int)' ,'INTERP(t1.q_bigint)' ,'INTERP(t1.q_smallint)' ,'INTERP(t1.q_tinyint)', 'INTERP(t1.q_float)' ,'INTERP(t1.q_double)' ,
                    'INTERP(t2.q_int)' ,'INTERP(t2.q_bigint)' ,'INTERP(t2.q_smallint)' ,'INTERP(t2.q_tinyint)', 'INTERP(t2.q_float)' ,'INTERP(t2.q_double)']
        interp_where_j = ['t1.ts = now' , 't1.ts = \'2020-09-13 20:26:40.000\'' , 't1.ts = \'2020-09-13 20:26:40.009\'' ,'t2.ts = now' , 't2.ts = \'2020-09-13 20:26:40.000\'' , 't2.ts = \'2020-09-13 20:26:40.009\'' ,
                    't1.tbname in (\'table_1\') and t1.ts = now' ,'t1.tbname in (\'table_0\' ,\'table_1\',\'table_2\',\'table_3\',\'table_4\',\'table_5\') and t1.ts =  \'2020-09-13 20:26:40.000\'','t1.tbname like \'table%\'  and t1.ts =  \'2020-09-13 20:26:40.002\'',
                    't2.tbname in (\'table_1\') and t2.ts = now' ,'t2.tbname in (\'table_0\' ,\'table_1\',\'table_2\',\'table_3\',\'table_4\',\'table_5\') and t2.ts =  \'2020-09-13 20:26:40.000\'','t2.tbname like \'table%\'  and t2.ts =  \'2020-09-13 20:26:40.002\'']

        # calc_aggregate_all   calc_aggregate_regular   calc_aggregate_groupbytbname  APERCENTILE\PERCENTILE
        # aggregate function include [all:count(*)\avg\sum\stddev ||regualr:twa\irate\leastsquares ||group by tbname:twa\irate\]
        calc_aggregate_all = ['count(*)' , 'count(q_int)' ,'count(q_bigint)' , 'count(q_smallint)' ,'count(q_tinyint)' ,'count(q_float)' ,
                    'count(q_double)' ,'count(q_binary)' ,'count(q_nchar)' ,'count(q_bool)' ,'count(q_ts)' ,
                    'avg(q_int)' ,'avg(q_bigint)' , 'avg(q_smallint)' ,'avg(q_tinyint)' ,'avg(q_float)' ,'avg(q_double)' ,
                    'sum(q_int)' ,'sum(q_bigint)' , 'sum(q_smallint)' ,'sum(q_tinyint)' ,'sum(q_float)' ,'sum(q_double)' ,
                    'STDDEV(q_int)' ,'STDDEV(q_bigint)' , 'STDDEV(q_smallint)' ,'STDDEV(q_tinyint)' ,'STDDEV(q_float)' ,'STDDEV(q_double)',
                    'APERCENTILE(q_int,10)' ,'APERCENTILE(q_bigint,20)' , 'APERCENTILE(q_smallint,30)' ,'APERCENTILE(q_tinyint,40)' ,'APERCENTILE(q_float,50)' ,'APERCENTILE(q_double,60)']

        calc_aggregate_regular = ['twa(q_int)' ,'twa(q_bigint)' , 'twa(q_smallint)' ,'twa(q_tinyint)' ,'twa (q_float)' ,'twa(q_double)' ,
                    'IRATE(q_int)' ,'IRATE(q_bigint)' , 'IRATE(q_smallint)' ,'IRATE(q_tinyint)' ,'IRATE (q_float)' ,'IRATE(q_double)' ,
                    'LEASTSQUARES(q_int,15,3)' , 'LEASTSQUARES(q_bigint,10,1)' , 'LEASTSQUARES(q_smallint,20,3)' ,'LEASTSQUARES(q_tinyint,10,4)' ,'LEASTSQUARES(q_float,6,4)' ,'LEASTSQUARES(q_double,3,1)' ,
                    'PERCENTILE(q_int,10)' ,'PERCENTILE(q_bigint,20)' , 'PERCENTILE(q_smallint,30)' ,'PERCENTILE(q_tinyint,40)' ,'PERCENTILE(q_float,50)' ,'PERCENTILE(q_double,60)']

        calc_aggregate_groupbytbname = ['twa(q_int)' ,'twa(q_bigint)' , 'twa(q_smallint)' ,'twa(q_tinyint)' ,'twa (q_float)' ,'twa(q_double)' ,
                    'IRATE(q_int)' ,'IRATE(q_bigint)' , 'IRATE(q_smallint)' ,'IRATE(q_tinyint)' ,'IRATE (q_float)' ,'IRATE(q_double)' ]

        #two table join
        calc_aggregate_all_j = ['count(t1.*)' , 'count(t1.q_int)' ,'count(t1.q_bigint)' , 'count(t1.q_smallint)' ,'count(t1.q_tinyint)' ,'count(t1.q_float)' ,
                    'count(t1.q_double)' ,'count(t1.q_binary)' ,'count(t1.q_nchar)' ,'count(t1.q_bool)' ,'count(t1.q_ts)' ,
                    'avg(t1.q_int)' ,'avg(t1.q_bigint)' , 'avg(t1.q_smallint)' ,'avg(t1.q_tinyint)' ,'avg(t1.q_float)' ,'avg(t1.q_double)' ,
                    'sum(t1.q_int)' ,'sum(t1.q_bigint)' , 'sum(t1.q_smallint)' ,'sum(t1.q_tinyint)' ,'sum(t1.q_float)' ,'sum(t1.q_double)' ,
                    'STDDEV(t1.q_int)' ,'STDDEV(t1.q_bigint)' , 'STDDEV(t1.q_smallint)' ,'STDDEV(t1.q_tinyint)' ,'STDDEV(t1.q_float)' ,'STDDEV(t1.q_double)',
                    'APERCENTILE(t1.q_int,10)' ,'APERCENTILE(t1.q_bigint,20)' , 'APERCENTILE(t1.q_smallint,30)' ,'APERCENTILE(t1.q_tinyint,40)' ,'APERCENTILE(t1.q_float,50)' ,'APERCENTILE(t1.q_double,60)' ,
                    'count(t2.*)' , 'count(t2.q_int)' ,'count(t2.q_bigint)' , 'count(t2.q_smallint)' ,'count(t2.q_tinyint)' ,'count(t2.q_float)' ,
                    'count(t2.q_double)' ,'count(t2.q_binary)' ,'count(t2.q_nchar)' ,'count(t2.q_bool)' ,'count(t2.q_ts)' ,
                    'avg(t2.q_int)' ,'avg(t2.q_bigint)' , 'avg(t2.q_smallint)' ,'avg(t2.q_tinyint)' ,'avg(t2.q_float)' ,'avg(t2.q_double)' ,
                    'sum(t2.q_int)' ,'sum(t2.q_bigint)' , 'sum(t2.q_smallint)' ,'sum(t2.q_tinyint)' ,'sum(t2.q_float)' ,'sum(t2.q_double)' ,
                    'STDDEV(t2.q_int)' ,'STDDEV(t2.q_bigint)' , 'STDDEV(t2.q_smallint)' ,'STDDEV(t2.q_tinyint)' ,'STDDEV(t2.q_float)' ,'STDDEV(t2.q_double)',
                    'APERCENTILE(t2.q_int,10)' ,'APERCENTILE(t2.q_bigint,20)' , 'APERCENTILE(t2.q_smallint,30)' ,'APERCENTILE(t2.q_tinyint,40)' ,'APERCENTILE(t2.q_float,50)' ,'APERCENTILE(t2.q_double,60)']

        calc_aggregate_regular_j = ['twa(t1.q_int)' ,'twa(t1.q_bigint)' , 'twa(t1.q_smallint)' ,'twa(t1.q_tinyint)' ,'twa (t1.q_float)' ,'twa(t1.q_double)' ,
                    'IRATE(t1.q_int)' ,'IRATE(t1.q_bigint)' , 'IRATE(t1.q_smallint)' ,'IRATE(t1.q_tinyint)' ,'IRATE (t1.q_float)' ,'IRATE(t1.q_double)' ,
                    'LEASTSQUARES(t1.q_int,15,3)' , 'LEASTSQUARES(t1.q_bigint,10,1)' , 'LEASTSQUARES(t1.q_smallint,20,3)' ,'LEASTSQUARES(t1.q_tinyint,10,4)' ,'LEASTSQUARES(t1.q_float,6,4)' ,'LEASTSQUARES(t1.q_double,3,1)' ,
                    'PERCENTILE(t1.q_int,10)' ,'PERCENTILE(t1.q_bigint,20)' , 'PERCENTILE(t1.q_smallint,30)' ,'PERCENTILE(t1.q_tinyint,40)' ,'PERCENTILE(t1.q_float,50)' ,'PERCENTILE(t1.q_double,60)' ,
                    'twa(t2.q_int)' ,'twa(t2.q_bigint)' , 'twa(t2.q_smallint)' ,'twa(t2.q_tinyint)' ,'twa (t2.q_float)' ,'twa(t2.q_double)' ,
                    'IRATE(t2.q_int)' ,'IRATE(t2.q_bigint)' , 'IRATE(t2.q_smallint)' ,'IRATE(t2.q_tinyint)' ,'IRATE (t2.q_float)' ,'IRATE(t2.q_double)',
                    'LEASTSQUARES(t2.q_int,15,3)' , 'LEASTSQUARES(t2.q_bigint,10,1)' , 'LEASTSQUARES(t2.q_smallint,20,3)' ,'LEASTSQUARES(t2.q_tinyint,10,4)' ,'LEASTSQUARES(t2.q_float,6,4)' ,'LEASTSQUARES(t2.q_double,3,1)' ,
                    'PERCENTILE(t2.q_int,10)' ,'PERCENTILE(t2.q_bigint,20)' , 'PERCENTILE(t2.q_smallint,30)' ,'PERCENTILE(t2.q_tinyint,40)' ,'PERCENTILE(t2.q_float,50)' ,'PERCENTILE(t2.q_double,60)']

        calc_aggregate_groupbytbname_j = ['twa(t1.q_int)' ,'twa(t1.q_bigint)' , 'twa(t1.q_smallint)' ,'twa(t1.q_tinyint)' ,'twa (t1.q_float)' ,'twa(t1.q_double)' ,
                    'IRATE(t1.q_int)' ,'IRATE(t1.q_bigint)' , 'IRATE(t1.q_smallint)' ,'IRATE(t1.q_tinyint)' ,'IRATE (t1.q_float)' ,'IRATE(t1.q_double)' ,
                    'twa(t2.q_int)' ,'twa(t2.q_bigint)' , 'twa(t2.q_smallint)' ,'twa(t2.q_tinyint)' ,'twa (t2.q_float)' ,'twa(t2.q_double)' ,
                    'IRATE(t2.q_int)' ,'IRATE(t2.q_bigint)' , 'IRATE(t2.q_smallint)' ,'IRATE(t2.q_tinyint)' ,'IRATE (t2.q_float)' ,'IRATE(t2.q_double)' ]

        # calc_calculate_all   calc_calculate_regular   calc_calculate_groupbytbname
        # calculation function include [all:spread\+-*/ ||regualr:diff\derivative ||group by tbname:diff\derivative\]
        calc_calculate_all = ['SPREAD(ts)'  , 'SPREAD(q_ts)'  , 'SPREAD(q_int)' ,'SPREAD(q_bigint)' , 'SPREAD(q_smallint)' ,'SPREAD(q_tinyint)' ,'SPREAD(q_float)' ,'SPREAD(q_double)' ,
                     '(SPREAD(q_int) + SPREAD(q_bigint))' , '(SPREAD(q_smallint) - SPREAD(q_float))', '(SPREAD(q_double) * SPREAD(q_tinyint))' , '(SPREAD(q_double) / SPREAD(q_float))']
        calc_calculate_regular = ['DIFF(q_int)' ,'DIFF(q_bigint)' , 'DIFF(q_smallint)' ,'DIFF(q_tinyint)' ,'DIFF(q_float)' ,'DIFF(q_double)' ,
                                  'DIFF(q_int,0)' ,'DIFF(q_bigint,0)' , 'DIFF(q_smallint,0)' ,'DIFF(q_tinyint,0)' ,'DIFF(q_float,0)' ,'DIFF(q_double,0)' ,
                                  'DIFF(q_int,1)' ,'DIFF(q_bigint,1)' , 'DIFF(q_smallint,1)' ,'DIFF(q_tinyint,1)' ,'DIFF(q_float,1)' ,'DIFF(q_double,1)' ,
                    'DERIVATIVE(q_int,15s,0)' , 'DERIVATIVE(q_bigint,10s,1)' , 'DERIVATIVE(q_smallint,20s,0)' ,'DERIVATIVE(q_tinyint,10s,1)' ,'DERIVATIVE(q_float,6s,0)' ,'DERIVATIVE(q_double,3s,1)' ]
        calc_calculate_groupbytbname = calc_calculate_regular

        #two table join
        calc_calculate_all_j = ['SPREAD(t1.ts)'  , 'SPREAD(t1.q_ts)'  , 'SPREAD(t1.q_int)' ,'SPREAD(t1.q_bigint)' , 'SPREAD(t1.q_smallint)' ,'SPREAD(t1.q_tinyint)' ,'SPREAD(t1.q_float)' ,'SPREAD(t1.q_double)' ,
                    'SPREAD(t2.ts)'  , 'SPREAD(t2.q_ts)'  , 'SPREAD(t2.q_int)' ,'SPREAD(t2.q_bigint)' , 'SPREAD(t2.q_smallint)' ,'SPREAD(t2.q_tinyint)' ,'SPREAD(t2.q_float)' ,'SPREAD(t2.q_double)' ,
                    '(SPREAD(t1.q_int) + SPREAD(t1.q_bigint))' , '(SPREAD(t1.q_tinyint) - SPREAD(t1.q_float))', '(SPREAD(t1.q_double) * SPREAD(t1.q_tinyint))' , '(SPREAD(t1.q_double) / SPREAD(t1.q_tinyint))',
                    '(SPREAD(t2.q_int) + SPREAD(t2.q_bigint))' , '(SPREAD(t2.q_smallint) - SPREAD(t2.q_float))', '(SPREAD(t2.q_double) * SPREAD(t2.q_tinyint))' , '(SPREAD(t2.q_double) / SPREAD(t2.q_tinyint))',
                    '(SPREAD(t1.q_int) + SPREAD(t1.q_smallint))' , '(SPREAD(t2.q_smallint) - SPREAD(t2.q_float))', '(SPREAD(t1.q_double) * SPREAD(t1.q_tinyint))' , '(SPREAD(t1.q_double) / SPREAD(t1.q_float))']
        calc_calculate_regular_j = ['DIFF(t1.q_int)' ,'DIFF(t1.q_bigint)' , 'DIFF(t1.q_smallint)' ,'DIFF(t1.q_tinyint)' ,'DIFF(t1.q_float)' ,'DIFF(t1.q_double)' ,
                    'DIFF(t1.q_int,0)' ,'DIFF(t1.q_bigint,0)' , 'DIFF(t1.q_smallint,0)' ,'DIFF(t1.q_tinyint,0)' ,'DIFF(t1.q_float,0)' ,'DIFF(t1.q_double,0)' ,
                    'DIFF(t1.q_int,1)' ,'DIFF(t1.q_bigint,1)' , 'DIFF(t1.q_smallint,1)' ,'DIFF(t1.q_tinyint,1)' ,'DIFF(t1.q_float,1)' ,'DIFF(t1.q_double,1)' ,
                    'DERIVATIVE(t1.q_int,15s,0)' , 'DERIVATIVE(t1.q_bigint,10s,1)' , 'DERIVATIVE(t1.q_smallint,20s,0)' ,'DERIVATIVE(t1.q_tinyint,10s,1)' ,'DERIVATIVE(t1.q_float,6s,0)' ,'DERIVATIVE(t1.q_double,3s,1)' ,
                    'DIFF(t2.q_int)' ,'DIFF(t2.q_bigint)' , 'DIFF(t2.q_smallint)' ,'DIFF(t2.q_tinyint)' ,'DIFF(t2.q_float)' ,'DIFF(t2.q_double)' ,
                    'DIFF(t2.q_int,0)' ,'DIFF(t2.q_bigint,0)' , 'DIFF(t2.q_smallint,0)' ,'DIFF(t2.q_tinyint,0)' ,'DIFF(t2.q_float,0)' ,'DIFF(t2.q_double,0)' ,
                    'DIFF(t2.q_int,1)' ,'DIFF(t2.q_bigint,1)' , 'DIFF(t2.q_smallint,1)' ,'DIFF(t2.q_tinyint,1)' ,'DIFF(t2.q_float,1)' ,'DIFF(t2.q_double,1)' ,
                    'DERIVATIVE(t2.q_int,15s,0)' , 'DERIVATIVE(t2.q_bigint,10s,1)' , 'DERIVATIVE(t2.q_smallint,20s,0)' ,'DERIVATIVE(t2.q_tinyint,10s,1)' ,'DERIVATIVE(t2.q_float,6s,0)' ,'DERIVATIVE(t2.q_double,3s,1)' ]
        calc_calculate_groupbytbname_j = calc_calculate_regular_j

        #inter  && calc_aggregate_all\calc_aggregate_regular\calc_select_all
        interval_sliding = ['interval(4w) sliding(1w) ','interval(1w) sliding(1d) ','interval(1d) sliding(1h) ' ,
                    'interval(1h) sliding(1m) ','interval(1m) sliding(1s) ','interval(1s) sliding(10a) ',
                    'interval(1y) ','interval(1n) ','interval(1w) ','interval(1d) ','interval(1h) ','interval(1m) ','interval(1s) ' ,'interval(10a)',
                    'interval(1y,1n) ','interval(1n,1w) ','interval(1w,1d) ','interval(1d,1h) ','interval(1h,1m) ','interval(1m,1s) ','interval(1s,10a) ' ,'interval(100a,30a)']

        #1 select * from (select column form regular_table where <\>\in\and\or order by)
        tdSql.query("select 1-1 from stable_1;")
        for i in range(self.fornum):
            #sql = "select  ts , * from  ( select  "   ===暂时不支持select * ,用下面这一行
            sql = "select  ts  from  ( select  "
            sql += "%s, " % random.choice(s_s_select)
            sql += "%s, " % random.choice(q_select)
            sql += "ts from regular_table_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(order_where)
            sql += ");"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(100)

        #1 outer union not support
        #self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select 1-2 from stable_1;")
        for i in range(self.fornum):
            #sql = "select  ts , * from  ( select  "
            sql = "select  ts  from  ( select  "
            sql += "%s, " % random.choice(s_r_select)
            sql += "%s, " % random.choice(q_select)
            #sql += "%s, " % q_select[len(q_select) -i-1]
            sql += "ts from regular_table_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(order_where)
            sql += ") union "
            #sql += "select  ts , * from  ( select  "
            sql += "select  ts  from  ( select  "
            sql += "%s, " % random.choice(s_r_select)
            sql += "%s, " % random.choice(q_select)
            sql += "ts from regular_table_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(order_where)
            sql += ")"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(100)

        #self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select 1-2 from stable_1;")
        for i in range(self.fornum):
            #sql = "select  ts , * from  ( select  "
            sql = "select  ts  from  ( select  "
            sql += "%s, " % random.choice(s_r_select)
            sql += "%s, " % random.choice(q_select)
            #sql += "%s, " % q_select[len(q_select) -i-1]
            sql += "ts from regular_table_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(order_where)
            sql += ") union all "
            #sql += "select  ts , * from  ( select  "
            sql += "select  ts  from  ( select  "
            sql += "%s, " % random.choice(s_r_select)
            sql += "%s, " % random.choice(q_select)
            sql += "ts from regular_table_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(order_where)
            sql += ")"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(200)

        #1 inter union not support
        tdSql.query("select 1-3 from stable_1;")
        for i in range(self.fornum):
            #sql = "select  ts , * from  ( select  "
            sql = "select  ts  from  ( select  "
            sql += "%s, " % random.choice(s_r_select)
            sql += "%s, " % random.choice(q_select)
            sql += "ts from regular_table_1 where "
            sql += "%s " % random.choice(q_where)
            sql += ""
            sql += " union select  "
            sql += "%s, " % random.choice(s_r_select)
            sql += "%s, " % random.choice(q_select)
            sql += "ts from regular_table_2 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(order_where)
            sql += ")"
            tdLog.info(sql)
            tdLog.info(len(sql))
            #TD-15606 tdSql.query(sql)
            # tdSql.checkRows(200)
        tdSql.query("select 1-3 from stable_1;")
        for i in range(self.fornum):
            #sql = "select  ts , * from  ( select  "
            sql = "select  ts  from  ( select  "
            sql += "%s, " % random.choice(s_r_select)
            sql += "%s, " % random.choice(q_select)
            sql += "ts from regular_table_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "  union all select  "
            sql += "%s, " % random.choice(s_r_select)
            sql += "%s, " % random.choice(q_select)
            sql += "ts from regular_table_2 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(order_where)
            sql += ")"
            tdLog.info(sql)
            tdLog.info(len(sql))
            #TD-15607 tdSql.query(sql)
            # tdSql.checkRows(300)

        #join:TD-6020\TD-6149 select * from (select column form regular_table1，regular_table2 where  t1.ts=t2.ts and <\>\in\and\or order by)
        #self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select 1-4 from stable_1;")
        for i in range(self.fornum):
            #sql = "select  ts , * from  ( select  t1.ts ,"
            sql = "select  *  from  ( select  t1.ts ,"
            sql += "t1.%s, " % random.choice(q_select)
            sql += "t1.%s, " % random.choice(q_select)
            sql += "t2.%s, " % random.choice(q_select)
            sql += "t2.%s, " % random.choice(q_select)
            sql += "t2.ts from regular_table_1 t1 , regular_table_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(q_u_where)
            sql += "%s " % random.choice(order_u_where)
            sql += ");"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(100)

        tdSql.query("select 1-5 from stable_1;")
        for i in range(self.fornum):
            sql = "select  ts , * from  ( select  t1.ts ,"
            sql += "t1.%s, " % random.choice(q_select)
            sql += "t1.%s, " % random.choice(q_select)
            sql += "t2.%s, " % random.choice(q_select)
            sql += "t2.%s, " % random.choice(q_select)
            sql += "t2.ts from regular_table_1 t1 , regular_table_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(q_u_or_where)
            sql += "%s " % random.choice(order_u_where)
            sql += ");"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.error(sql)
            # TD-15587 tdSql.query(sql)
            # tdSql.checkRows(100)

        #2 select column from (select * form regular_table ) where <\>\in\and\or order by
        #self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select 2-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select  ts ,"
            sql += "%s, " % random.choice(s_r_select)
            sql += "%s  " % random.choice(q_select)
            sql += " from  ( select  * from regular_table_1 ) where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(order_where)
            sql += " ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(100)

        #join: select column from (select column form regular_table1，regular_table2 )where  t1.ts=t2.ts and <\>\in\and\or order by
        #cross join not supported yet
        tdSql.query("select 2-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select  ts , * from  ( select  t1.ts ,"
            sql += "t1.%s, " % random.choice(q_select)
            sql += "t1.%s, " % random.choice(q_select)
            sql += "t2.%s, " % random.choice(q_select)
            sql += "t2.%s, " % random.choice(q_select)
            sql += "t2.ts from regular_table_1 t1 , regular_table_2 t2 ) where t1.ts = t2.ts and "
            sql += "%s " % random.choice(q_u_where)
            sql += "%s " % random.choice(order_u_where)
            #sql += ");"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.error(sql)

        #3 select * from (select column\tag form stable  where <\>\in\and\or order by )
        #self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select 3-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select   *  from  ( select  "
            sql += "%s, " % random.choice(s_s_select)
            sql += "%s, " % random.choice(q_select)
            sql += "%s, " % random.choice(t_select)
            sql += "ts from stable_1 where "
            sql += "%s " % random.choice(qt_where)
            sql += "%s " % random.choice(order_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(300)
        tdSql.query("select 3-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select  ts, "
            sql += "%s  " % random.choice(s_r_select)
            sql += "from  ( select  "
            sql += "%s, " % random.choice(s_s_select)
            sql += "%s, " % random.choice(q_select)
            sql += "%s, " % random.choice(t_select)
            sql += "ts from stable_1 where "
            sql += "%s " % random.choice(qt_where)
            sql += "%s " % random.choice(order_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(300)

        # select ts,* from (select column\tag form stable1,stable2  where t1.ts = t2.ts and <\>\in\and\or order by )
        #self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select 3-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select  ts , *  from  ( select  t1.ts , "
            sql += "t1.%s, " % random.choice(s_s_select)
            sql += "t1.%s, " % random.choice(q_select)
            sql += "t2.%s, " % random.choice(s_s_select)
            sql += "t2.%s, " % random.choice(q_select)
            sql += "t2.ts from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(t_join_where)
            sql += "%s " % random.choice(order_u_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            # TD-15609 tdSql.query(sql)
            # tdSql.checkRows(100)

        #3 outer union not support
        rsDn = self.restartDnodes()
        tdSql.query("select 3-3 from stable_1;")
        for i in range(self.fornum):
            #sql = "select  ts , * from  ( select  "
            sql = "select  ts  from  ( select  "
            sql += "%s, " % random.choice(s_r_select)
            sql += "%s, " % random.choice(q_select)
            sql += "ts from stable_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(order_where)
            sql += ") union "
            sql += "select  ts  from  ( select  "
            sql += "%s, " % random.choice(s_r_select)
            sql += "%s, " % random.choice(q_select)
            sql += "ts from stable_2 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(order_where)
            sql += ")"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(200)
        for i in range(self.fornum):
            #sql = "select  ts , * from  ( select  "
            sql = "select  ts  from  ( select  "
            sql += "%s, " % random.choice(s_r_select)
            sql += "%s, " % random.choice(q_select)
            sql += "ts from stable_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(order_where)
            sql += ") union all "
            sql += "select  ts  from  ( select  "
            sql += "%s, " % random.choice(s_r_select)
            sql += "%s, " % random.choice(q_select)
            sql += "ts from stable_2 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(order_where)
            sql += ")"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(400)

        #3 inter union not support
        tdSql.query("select 3-4 from stable_1;")
        for i in range(self.fornum):
            sql = "select  ts , * from  ( select  "
            sql += "%s, " % random.choice(s_r_select)
            sql += "%s, " % random.choice(q_select)
            sql += "ts from stable_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(order_where)
            sql += " %s "  % random.choice(unionall_or_union)
            sql += " select  "
            sql += "%s, " % random.choice(s_r_select)
            sql += "%s, " % random.choice(q_select)
            sql += "ts from stable_2 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(order_where)
            sql += ")"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.error(sql)

        #join:select * from (select column form stable1，stable2 where  t1.ts=t2.ts and <\>\in\and\or order by)
        tdSql.query("select 3-5 from stable_1;")
        for i in range(self.fornum):
            sql = "select  *  from  ( select  t1.ts ,"
            sql += "t1.%s, " % random.choice(q_select)
            sql += "t1.%s, " % random.choice(q_select)
            sql += "t2.%s, " % random.choice(q_select)
            sql += "t2.%s, " % random.choice(q_select)
            sql += "t2.ts from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(t_u_where)
            sql += "%s " % random.choice(order_u_where)
            sql += ");"
            tdLog.info(sql)
            tdLog.info(len(sql))
            # TD-15609 tdSql.query(sql)
            # tdSql.checkRows(100)

        tdSql.query("select 3-6 from stable_1;")
        for i in range(self.fornum):
            sql = "select  * from  ( select  t1.ts ,"
            sql += "t1.%s, " % random.choice(q_select)
            sql += "t1.%s, " % random.choice(q_select)
            sql += "t2.%s, " % random.choice(q_select)
            sql += "t2.%s, " % random.choice(q_select)
            sql += "t2.ts from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(t_u_or_where)
            sql += "%s " % random.choice(order_u_where)
            sql += ");"
            tdLog.info(sql)
            tdLog.info(len(sql))
             # TD-15609 同上 tdSql.query(sql)
            # tdSql.checkRows(100)

        #4 select column from (select * form stable  where <\>\in\and\or order by )
        #self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select 4-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select  ts , "
            sql += "%s, " % random.choice(s_r_select)
            sql += "%s, " % random.choice(q_select)
            sql += "%s  " % random.choice(t_select)
            sql += " from  ( select  * from stable_1 where "
            sql += "%s " % random.choice(qt_where)
            sql += "%s " % random.choice(order_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            #TD-15616 tdSql.query(sql)
            # tdSql.checkRows(300)

        #5 select distinct column\tag from (select * form stable  where <\>\in\and\or order by limit offset )
        tdSql.query("select 5-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select   "
            sql += "%s " % random.choice(dqt_select)
            sql += " from  ( select  * from stable_1 where "
            sql += "%s " % random.choice(qt_where)
            sql += "%s " % random.choice(order_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            #TD-15500 tdSql.query(sql)

        #5-1 select distinct column\tag from (select calc form stable  where <\>\in\and\or order by limit offset )
        tdSql.query("select 5-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select  distinct  c5_1 "
            sql += " from  ( select   "
            sql += "%s " % random.choice(calc_select_in_ts)
            sql += " as c5_1 from stable_1 where "
            sql += "%s " % random.choice(qt_where)
            #sql += "%s " % random.choice(order_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            #tdSql.checkRows(1)有的函数还没有提交，会不返回结果，先忽略

        #6-error select * from (select distinct(tag) form stable  where <\>\in\and\or order by limit  )
        tdSql.query("select 6-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select  * from  ( select  "
            sql += "%s " % random.choice(dt_select)
            sql += "  from stable_1 where "
            sql += "%s " % random.choice(qt_where)
            sql += "%s " % random.choice(order_desc_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.error(sql)
        tdSql.query("select 6-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select  * from  ( select  "
            sql += "%s " % random.choice(dt_select)
            sql += "  from stable_1 where "
            sql += "%s ) ;" % random.choice(qt_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            #tdSql.checkRows(1)#数量不一致，不在校验

        #7-error select * from (select distinct(tag) form stable  where <\>\in\and\or order by limit  )
        tdSql.query("select 7-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select  * from  ( select  "
            sql += "%s " % random.choice(dq_select)
            sql += "  from stable_1 where "
            sql += "%s " % random.choice(qt_where)
            sql += "%s " % random.choice(order_desc_where)
            sql += "%s " % random.choice([limit_where[0] , limit_where[1]] )
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.error(sql)  #distinct 和 order by 不能混合使用
        tdSql.query("select 7-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select  * from  ( select  "
            sql += "%s " % random.choice(dq_select)
            sql += "  from stable_1 where "
            sql += "%s " % random.choice(qt_where)
            #sql += "%s " % random.choice(order_desc_where)
            sql += "%s " % random.choice([limit_where[0] , limit_where[1]] )
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(1)

        #calc_select,TWA/Diff/Derivative/Irate are not allowed to apply to super table directly
        #8 select * from (select ts,calc form ragular_table  where <\>\in\and\or order by   )

        # dcDB = self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select 8-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select  *  from  ( select  ts ,"
            sql += "%s " % random.choice(calc_select_support_ts)
            sql += "from regular_table_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(order_where)
            sql += "%s " % random.choice(limit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql) # 聚合函数不在可以和ts一起使用了 DB error: Not a single-group group function
        tdSql.query("select 8-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select  *  from  ( select  "
            sql += "%s " % random.choice(calc_select_not_support_ts)
            sql += "from regular_table_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(limit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            #TD-15651 tdSql.query(sql) # 聚合函数不在可以和ts一起使用了 DB error: Not a single-group group function

        for i in range(self.fornum):
            sql = "select  *  from  ( select  "
            sql += "%s " % random.choice(calc_select_in_ts)
            sql += "from regular_table_1 where "
            sql += "%s " % random.choice(q_where)
            #sql += "%s " % random.choice(order_where)
            sql += "%s " % random.choice(limit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            ##top返回结果有问题nest.sql tdSql.checkRows(1)

        tdSql.query("select 8-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select  *  from  ( select t1.ts, "
            sql += "%s " % random.choice(calc_select_in_support_ts_j)
            sql += "from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(q_u_where)
            sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)# 聚合函数不在可以和ts一起使用了 DB error: Not a single-group group function
        for i in range(self.fornum):
            sql = "select  *  from  ( select  "
            sql += "%s " % random.choice(calc_select_in_not_support_ts_j)
            sql += "from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(q_u_where)
            sql += "%s " % random.choice(limit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            #TD-15651 tdSql.query(sql)
            ##top返回结果有问题 tdSql.checkRows(1)

        #9 select * from (select ts,calc form stable  where <\>\in\and\or order by   )
        # self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select 9-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select  *  from  ( select  "
            sql += "%s " % random.choice(calc_select_not_support_ts)
            sql += "from stable_1 where "
            sql += "%s " % random.choice(qt_where)
            sql += "%s " % random.choice(limit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            #TD-15651 tdSql.query(sql)
        tdSql.query("select 9-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select  *  from  ( select  ts ,"
            sql += "%s " % random.choice(calc_select_support_ts)
            sql += "from stable_1 where "
            sql += "%s " % random.choice(qt_where)
            sql += "%s " % random.choice(order_where)
            sql += "%s " % random.choice(limit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        tdSql.query("select 9-3 from stable_1;")
        for i in range(self.fornum):
            sql = "select  *  from  ( select  "
            sql += "%s " % random.choice(calc_select_in_not_support_ts_j)
            sql += "from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(t_join_where)
            sql += " and %s " % random.choice(qt_u_or_where)
            sql += "%s " % random.choice(limit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            #TD-15651 tdSql.query(sql)
        tdSql.query("select 9-4 from stable_1;")
        for i in range(self.fornum):
            sql = "select  *  from  ( select  t1.ts,"
            sql += "%s " % random.choice(calc_select_in_support_ts_j)
            sql += "from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(t_join_where)
            sql += " and %s " % random.choice(qt_u_or_where)
            sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        #10 select calc from (select * form regualr_table  where <\>\in\and\or order by   )
        tdSql.query("select 10-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select  "
            sql += "%s " % random.choice(calc_select_in_ts)
            sql += "as calc10_1 from ( select * from regular_table_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(order_desc_where)
            sql += "%s " % random.choice(limit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(1)

        #10-1 select calc from (select * form regualr_table  where <\>\in\and\or order by   )
        # rsDn = self.restartDnodes()
        # self.dropandcreateDB_random("%s" %db, 1)
        # rsDn = self.restartDnodes()
        tdSql.query("select 10-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select  "
            sql += "%s " % random.choice(calc_select_all)
            sql += "as calc10_2 from ( select * from regular_table_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(order_desc_where)
            sql += "%s " % random.choice(limit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            #TD-15651 tdSql.query(sql)
            # tdSql.checkRows(1)

        #10-2 select calc from (select * form regualr_tables  where <\>\in\and\or order by   )
        tdSql.query("select 10-3 from stable_1;")
        for i in range(self.fornum):
            sql = "select  "
            sql += "%s as calc10_3 " % random.choice(calc_select_all)
            sql += " from ( select * from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(q_u_where)
            sql += " and %s " % random.choice(q_u_or_where)
            sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            sql += "%s ;" % random.choice(limit1_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            #TD-15651  tdSql.query(sql)

        tdSql.query("select 10-4 from stable_1;")
        for i in range(self.fornum):
            sql = "select  "
            sql += "%s as calc10_4 " % random.choice(calc_select_all)
            sql += " from ( select * from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(q_u_or_where)
            sql += " and %s " % random.choice(q_u_or_where)
            sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            sql += "%s ;" % random.choice(limit_u_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            #TD-15651  tdSql.query(sql)
            # tdSql.checkRows(1)

        #11 select calc from (select * form stable  where <\>\in\and\or order by limit  )
        tdSql.query("select 11-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select   "
            sql += "%s " % random.choice(calc_select_in_ts)
            sql += "as calc11_1 from ( select * from stable_1 where "
            sql += "%s " % random.choice(qt_where)
            sql += "%s " % random.choice(order_desc_where)
            sql += "%s " % random.choice(limit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(1)

        #11-1 select calc from (select * form stable  where <\>\in\and\or order by limit  )
        tdSql.query("select 11-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select   "
            sql += "%s " % random.choice(calc_select_all)
            sql += "as calc11_1 from ( select * from stable_1 where "
            sql += "%s " % random.choice(qt_where)
            sql += "%s " % random.choice(order_desc_where)
            sql += "%s " % random.choice(limit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            #TD-15651 tdSql.query(sql)
            #不好计算结果 tdSql.checkRows(1)

        #11-2 select calc from (select * form stables  where <\>\in\and\or order by limit  )
        tdSql.query("select 11-3 from stable_1;")
        for i in range(self.fornum):
            sql = "select   "
            sql += "%s " % random.choice(calc_select_all)
            sql += "as calc11_1 from ( select * from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(t_join_where)
            sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            sql += "%s ;" % random.choice(limit_u_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            #TD-15651 tdSql.query(sql)

        tdSql.query("select 11-4 from stable_1;")
        for i in range(self.fornum):
            sql = "select   "
            sql += "%s " % random.choice(calc_select_all)
            sql += "as calc11_1 from ( select * from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(qt_u_or_where)
            sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            sql += "%s ;" % random.choice(limit_u_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdLog.info(len(sql))
            #TD-15651 tdSql.query(sql)

        #12 select calc-diff from (select * form regualr_table  where <\>\in\and\or order by limit  )
        ##self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select 12-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select  "
            sql += "%s " % random.choice(calc_calculate_regular)
            sql += " from ( select * from regular_table_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(order_desc_where)
            sql += "%s " % random.choice([limit_where[2] , limit_where[3]] )
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            ##目前derivative不支持  tdSql.query(sql)
            # tdSql.checkRows(1)

        tdSql.query("select 12-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select  "
            sql += "%s " % random.choice(calc_calculate_regular)
            sql += " from ( select * from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(q_u_where)
            sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice([limit_where[2] , limit_where[3]] )
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            #目前derivative不支持 tdSql.query(sql)
            # tdSql.checkRows(1)

        tdSql.query("select 12-2.2 from stable_1;")
        for i in range(self.fornum):
            sql = "select  "
            sql += "%s " % random.choice(calc_calculate_regular)
            sql += " from ( select * from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(q_u_or_where)
            sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice([limit_where[2] , limit_where[3]] )
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            #目前derivative不支持 tdSql.query(sql)

        #12-1 select calc-diff from (select * form stable  where <\>\in\and\or order by limit  )
        tdSql.query("select 12-3 from stable_1;")
        rsDn = self.restartDnodes()
        for i in range(self.fornum):
            sql = "select  * from ( select "
            sql += "%s " % random.choice(calc_calculate_regular)
            sql += "  from stable_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(group_where)
            sql += ") "
            sql += "%s " % random.choice(order_desc_where)
            sql += "%s " % random.choice([limit_where[2] , limit_where[3]] )
            sql += " ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            #目前derivative不支持 tdSql.query(sql)

        tdSql.query("select 12-4 from stable_1;")
        #join query does not support group by
        for i in range(self.fornum):
            sql = "select  * from ( select "
            sql += "%s " % random.choice(calc_calculate_regular_j)
            sql += "  from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(t_join_where)
            sql += "%s " % random.choice(group_where_j)
            sql += ") "
            #sql += "%s " % random.choice(order_desc_where)
            sql += "%s " % random.choice([limit_where[2] , limit_where[3]] )
            sql += " ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            #tdSql.query(sql) 目前de函数不支持，另外看看需要不需要将group by和pari by分开

        tdSql.query("select 12-5 from stable_1;")
        #join query does not support group by
        for i in range(self.fornum):
            sql = "select  * from ( select "
            sql += "%s " % random.choice(calc_calculate_regular_j)
            sql += "  from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(qt_u_or_where)
            sql += "%s " % random.choice(group_where_j)
            sql += ") "
            sql += "%s " % random.choice(order_desc_where)
            sql += "%s " % random.choice([limit_where[2] , limit_where[3]] )
            sql += " ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            #derivative not support  tdSql.query(sql)


        #13 select calc-diff as diffns from (select * form stable  where <\>\in\and\or order by limit  )
        tdSql.query("select 13-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select   "
            sql += "%s " % random.choice(calc_calculate_regular)
            sql += " as calc13_1 from ( select * from stable_1 where "
            sql += "%s " % random.choice(qt_where)
            sql += "%s " % random.choice(orders_desc_where)
            sql += "%s " % random.choice([limit_where[2] , limit_where[3]] )
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            #derivative not support tdSql.query(sql)

        #14 select * from (select calc_aggregate_alls as agg from stable  where <\>\in\and\or group by order by slimit soffset )
        # TD-5955 select   * from  ( select count (q_double)  from stable_1 where t_bool = true or  t_bool = false group by loc order by ts asc slimit 1 ) ;
        tdSql.query("select 14-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select "
            sql += "%s as calc14_1, " % random.choice(calc_aggregate_all)
            sql += "%s as calc14_2, " % random.choice(calc_aggregate_all)
            sql += "%s " % random.choice(calc_aggregate_all)
            sql += " as calc14_3 from stable_1 where "
            sql += "%s " % random.choice(qt_where)
            sql += "%s " % random.choice(group_where)
            sql += "%s " % random.choice(order_desc_where)
            sql += "%s " % random.choice(slimit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            #TD-15678 tdSql.query(sql)
            # tdSql.checkRows(1)

        # error group by in out query
        tdSql.query("select 14-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select "
            sql += "%s as calc14_1, " % random.choice(calc_aggregate_all)
            sql += "%s as calc14_2, " % random.choice(calc_aggregate_all)
            sql += "%s " % random.choice(calc_aggregate_all)
            sql += " as calc14_3 from stable_1 where "
            sql += "%s " % random.choice(qt_where)
            sql += "%s " % random.choice(group_where)
            sql += "%s " % random.choice(having_support)
            sql += "%s " % random.choice(orders_desc_where)
            sql += "%s " % random.choice(slimit1_where)
            sql += ") "
            sql += "%s " % random.choice(group_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            #TD-15678 tdSql.query(sql)
            # tdSql.checkRows(1)

        #14-2 select * from (select calc_aggregate_all_js as agg from stables  where <\>\in\and\or group by order by slimit soffset )
        tdSql.query("select 14-3 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select "
            sql += "%s as calc14_1, " % random.choice(calc_aggregate_all_j)
            sql += "%s as calc14_2, " % random.choice(calc_aggregate_all_j)
            sql += "%s " % random.choice(calc_aggregate_all_j)
            sql += " as calc14_3 from stable_1  t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(t_join_where)
            sql += "%s " % random.choice(partiton_where_j)
            sql += "%s " % random.choice(slimit1_where)
            sql += ") "
            sql += "%s ;" % random.choice(limit_u_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        tdSql.query("select 14-4 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select "
            sql += "%s as calc14_1, " % random.choice(calc_aggregate_all_j)
            sql += "%s as calc14_2, " % random.choice(calc_aggregate_all_j)
            sql += "%s " % random.choice(calc_aggregate_all_j)
            sql += " as calc14_3 from stable_1  t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(qt_u_or_where)
            sql += "%s " % random.choice(partiton_where_j)
            sql += "%s " % random.choice(slimit1_where)
            sql += ") "
            sql += "%s ;" % random.choice(limit_u_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        #15 TD-6320 select * from (select calc_aggregate_regulars as agg from regular_table  where <\>\in\and\or  order by slimit soffset )
        tdSql.query("select 15-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select "
            sql += "%s as calc15_1, " % random.choice(calc_aggregate_regular)
            sql += "%s as calc15_2, " % random.choice(calc_aggregate_regular)
            sql += "%s " % random.choice(calc_aggregate_regular)
            sql += " as calc15_3 from regular_table_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(group_where_regular)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            #tdSql.query(sql) #Invalid function name: twa'
            # tdSql.checkRows(1)

        tdSql.query("select 15-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select "
            sql += "%s as calc15_1, " % random.choice(calc_aggregate_regular_j)
            sql += "%s as calc15_2, " % random.choice(calc_aggregate_regular_j)
            sql += "%s " % random.choice(calc_aggregate_regular_j)
            sql += " as calc15_3 from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(q_u_where)
            sql += "%s " % random.choice(group_where_regular_j)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            sql += "%s ;" % random.choice(limit_u_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            #tdSql.query(sql) #Invalid function name: twa'

        tdSql.query("select 15-2.2 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select "
            sql += "%s as calc15_1, " % random.choice(calc_aggregate_regular_j)
            sql += "%s as calc15_2, " % random.choice(calc_aggregate_regular_j)
            sql += "%s " % random.choice(calc_aggregate_regular_j)
            sql += " as calc15_3 from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(q_u_or_where)
            sql += "%s " % random.choice(group_where_regular_j)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            sql += "%s ;" % random.choice(limit_u_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            #tdSql.query(sql) #Invalid function name: twa'

        rsDn = self.restartDnodes()
        tdSql.query("select 15-3 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select "
            sql += "%s as calc15_1, " % random.choice(calc_aggregate_groupbytbname)
            sql += "%s as calc15_2, " % random.choice(calc_aggregate_groupbytbname)
            sql += "%s " % random.choice(calc_aggregate_groupbytbname)
            sql += " as calc15_3 from stable_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(group_where)
            sql += "%s " % random.choice(having_support)
            sql += "%s " % random.choice(order_desc_where)
            sql += ") "
            sql += "order by  calc15_1  "
            sql += "%s " % random.choice(limit_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            #tdSql.query(sql) #Invalid function name: twa',可能还的去掉order by

        tdSql.query("select 15-4 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select "
            sql += "%s as calc15_1, " % random.choice(calc_aggregate_groupbytbname_j)
            sql += "%s as calc15_2, " % random.choice(calc_aggregate_groupbytbname_j)
            sql += "%s " % random.choice(calc_aggregate_groupbytbname_j)
            sql += " as calc15_3 from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and   "
            sql += "%s " % random.choice(t_join_where)
            sql += "%s " % random.choice(group_where_j)
            sql += "%s " % random.choice(having_support_j)
            #sql += "%s " % random.choice(orders_desc_where)
            sql += ") "
            sql += "order by  calc15_1  "
            sql += "%s " % random.choice(limit_u_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            #tdSql.query(sql)  #'Invalid function name: irate'

        tdSql.query("select 15-4.2 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select "
            sql += "%s as calc15_1, " % random.choice(calc_aggregate_groupbytbname_j)
            sql += "%s as calc15_2, " % random.choice(calc_aggregate_groupbytbname_j)
            sql += "%s " % random.choice(calc_aggregate_groupbytbname_j)
            sql += " as calc15_3 from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and   "
            sql += "%s " % random.choice(qt_u_or_where)
            sql += "%s " % random.choice(group_where_j)
            sql += "%s " % random.choice(having_support_j)
            sql += "%s " % random.choice(orders_desc_where)
            sql += ") "
            sql += "order by  calc15_1  "
            sql += "%s " % random.choice(limit_u_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            #TD-15678 #tdSql.query(sql)

        tdSql.query("select 15-5 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select "
            sql += "%s as calc15_1, " % random.choice(calc_aggregate_groupbytbname)
            sql += "%s as calc15_2, " % random.choice(calc_aggregate_groupbytbname)
            sql += "%s " % random.choice(calc_aggregate_groupbytbname)
            sql += " as calc15_3 from stable_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(group_where)
            sql += ") "
            sql += "order by calc15_1  "
            sql += "%s " % random.choice(limit_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            #tdSql.query(sql) #'Invalid function name: irate'

        #16 select * from (select calc_aggregate_regulars as agg from regular_table  where <\>\in\and\or  order by limit offset )
        #self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select 16-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select  "
            sql += "%s as calc16_0 , " % random.choice(calc_calculate_all)
            sql += "%s as calc16_1 , " % random.choice(calc_aggregate_all)
            sql += "%s as calc16_2 " % random.choice(calc_select_in)
            sql += "  from stable_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(group_where)
            #sql += "%s " % random.choice(having_support)having和 partition不能混合使用
            sql += ") "
            sql += "order by calc16_0  "
            sql += "%s " % random.choice(limit1_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            #TD-15651 tdSql.query(sql)

        tdSql.query("select 16-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select  "
            sql += "%s as calc16_0  " % random.choice(calc_calculate_all_j)
            sql += ", %s as calc16_1  " % random.choice(calc_aggregate_all_j)
            #sql += ", %s as calc16_2  " % random.choice(calc_select_in_j)
            sql += "  from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(t_join_where)
            sql += ") "
            sql += "order by calc16_0  "
            sql += "%s " % random.choice(limit1_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        tdSql.query("select 16-2.2 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select  "
            sql += "%s as calc16_0  " % random.choice(calc_calculate_all_j)
            sql += ", %s as calc16_1  " % random.choice(calc_aggregate_all_j)
            sql += "  from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(qt_u_or_where)
            sql += ") "
            sql += "order by calc16_0  "
            sql += "%s " % random.choice(limit1_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        tdSql.query("select 16-3 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select  "
            sql += "%s as calc16_1  " % random.choice(calc_calculate_regular)
            sql += "  from regular_table_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "limit 2 ) "
            sql += "%s " % random.choice(limit1_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            #tdSql.query(sql)#Invalid function name: derivative'

        tdSql.query("select 16-4 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select  "
            sql += "%s as calc16_1  " % random.choice(calc_calculate_regular_j)
            sql += "  from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(q_u_where)
            sql += "limit 2 ) "
            sql += "%s " % random.choice(limit1_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            #tdSql.query(sql)#Invalid function name: derivative'

        tdSql.query("select 16-4.2 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select  "
            sql += "%s as calc16_1  " % random.choice(calc_calculate_regular_j)
            sql += "  from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(q_u_or_where)
            sql += "limit 2 ) "
            sql += "%s " % random.choice(limit1_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            #tdSql.query(sql)#Invalid function name: derivative'

        tdSql.query("select 16-5 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select  "
            sql += "%s as calc16_1 , " % random.choice(calc_calculate_all)
            sql += "%s as calc16_1 , " % random.choice(calc_calculate_regular)
            sql += "%s as calc16_2 " % random.choice(calc_select_all)
            sql += "  from stable_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(group_where)
            #sql += "%s " % random.choice(having_support)
            sql += ") "
            sql += "order by calc16_1  "
            sql += "%s " % random.choice(limit1_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
           # tdSql.query(sql)

        tdSql.query("select 16-6 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select  "
            sql += "%s as calc16_1  " % random.choice(calc_calculate_groupbytbname)
            sql += "  from stable_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(group_where)
            sql += "limit 2 ) "
            sql += "%s " % random.choice(limit1_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            #Invalid function name: derivative'  tdSql.query(sql)

        tdSql.query("select 16-7 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select  "
            sql += "%s as calc16_1  " % random.choice(calc_calculate_groupbytbname_j)
            sql += "  from stable_1  t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(t_join_where)
            sql += "limit 2 ) "
            sql += "%s " % random.choice(limit1_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            #Invalid function name: derivative' tdSql.query(sql)

        tdSql.query("select 16-8 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select  "
            sql += "%s as calc16_1  " % random.choice(calc_calculate_groupbytbname_j)
            sql += "  from stable_1  t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(qt_u_or_where)
            sql += "limit 2 ) "
            sql += "%s " % random.choice(limit1_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            #Invalid function name: derivative' tdSql.query(sql)

        #17 select apercentile from (select calc_aggregate_alls form regualr_table or stable  where <\>\in\and\or interval_sliding group by having order by limit offset  )interval_sliding
        #self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select 17-1 from stable_1;")
        for i in range(self.fornum):
            #this is having_support , but tag-select cannot mix with last_row,other select can
            sql = "select   apercentile(cal17_0, %d)/10 ,apercentile(cal17_1, %d)/1000 ,apercentile(cal17_2, %d)*10+%d from  ( select  " %(random.randint(0,100) , random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal17_0 , " % random.choice(calc_calculate_all)
            sql += "%s as cal17_1 ," % random.choice(calc_aggregate_all)
            sql += "%s as cal17_2 " % random.choice(calc_aggregate_all)
            sql += " from stable_1 where "
            sql += "%s " % random.choice(qt_where)
            sql += "%s " % random.choice(partiton_where)
            sql += "%s " % random.choice(interval_sliding)
            #sql += "%s " % random.choice(having_support)
            #sql += "%s " % random.choice(order_where)
            sql += "%s " % random.choice(limit1_where)
            sql += ") "
            #sql += "%s " % random.choice(interval_sliding)
            tdLog.info(sql)
            tdLog.info(len(sql))
            #TD-15719 tdSql.query(sql)

        tdSql.query("select 17-2 from stable_1;")
        for i in range(self.fornum):
            #this is having_support , but tag-select cannot mix with last_row,other select can
            sql = "select   apercentile(cal17_0, %d)/10 ,apercentile(cal17_1, %d)/1000 ,apercentile(cal17_2, %d)*10+%d from  ( select  " %(random.randint(0,100) , random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal17_0 , " % random.choice(calc_calculate_all_j)
            sql += "%s as cal17_1 ," % random.choice(calc_aggregate_all_j)
            sql += "%s as cal17_2 " % random.choice(calc_aggregate_all_j)
            sql += " from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(t_join_where)
            sql += "%s " % random.choice(interval_sliding)
            #sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            #sql += "%s " % random.choice(interval_sliding)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        tdSql.query("select 17-2.2 from stable_1;")
        for i in range(self.fornum):
            #this is having_support , but tag-select cannot mix with last_row,other select can
            sql = "select   apercentile(cal17_0, %d)/10 ,apercentile(cal17_1, %d)/1000 ,apercentile(cal17_2, %d)*10+%d from  ( select  " %(random.randint(0,100) , random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal17_0 , " % random.choice(calc_calculate_all_j)
            sql += "%s as cal17_1 ," % random.choice(calc_aggregate_all_j)
            sql += "%s as cal17_2 " % random.choice(calc_aggregate_all_j)
            sql += " from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(qt_u_or_where)
            sql += "%s " % random.choice(interval_sliding)
            #sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            #sql += "%s " % random.choice(interval_sliding)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        self.restartDnodes()
        tdSql.query("select 17-3 from stable_1;")
        for i in range(self.fornum):
            #this is having_tagnot_support , because tag-select cannot mix with last_row...
            sql = "select   apercentile(cal17_1, %d)/1000 ,apercentile(cal17_2, %d)*10+%d from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal17_1 ," % random.choice(calc_aggregate_all)
            sql += "%s as cal17_2 " % random.choice(calc_aggregate_all)
            sql += " from stable_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(partiton_where)
            sql += "%s " % random.choice(interval_sliding)
            #sql += "%s " % random.choice(having_tagnot_support)
            #sql += "%s " % random.choice(order_where)
            sql += "%s " % random.choice(limit1_where)
            sql += ") "
            #sql += "%s " % random.choice(interval_sliding)
            tdLog.info(sql)
            tdLog.info(len(sql))
            #TD-15770 tdSql.query(sql)

        tdSql.query("select 17-4 from stable_1;")
        for i in range(self.fornum):
            #this is having_tagnot_support , because tag-select cannot mix with last_row...
            sql = "select   apercentile(cal17_1, %d)/1000 ,apercentile(cal17_2, %d)*10+%d from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal17_1 ," % random.choice(calc_aggregate_all_j)
            sql += "%s as cal17_2 " % random.choice(calc_aggregate_all_j)
            sql += " from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(t_join_where)
            sql += "%s " % random.choice(interval_sliding)
            #sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            #sql += "%s " % random.choice(interval_sliding)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        tdSql.query("select 17-4.2 from stable_1;")
        for i in range(self.fornum):
            #this is having_tagnot_support , because tag-select cannot mix with last_row...
            sql = "select   apercentile(cal17_1, %d)/1000 ,apercentile(cal17_2, %d)*10+%d from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal17_1 ," % random.choice(calc_aggregate_all_j)
            sql += "%s as cal17_2 " % random.choice(calc_aggregate_all_j)
            sql += " from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(qt_u_or_where)
            sql += "%s " % random.choice(interval_sliding)
            #sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            #sql += "%s " % random.choice(interval_sliding)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        tdSql.query("select 17-5 from stable_1;")
        for i in range(self.fornum):
            #having_not_support
            sql = "select   apercentile(cal17_1, %d)/1000 ,apercentile(cal17_2, %d)*10+%d from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal17_1 ," % random.choice(calc_aggregate_all)
            sql += "%s as cal17_2 " % random.choice(calc_aggregate_all)
            sql += " from stable_1 where "
            sql += "%s " % random.choice(qt_where)
            sql += "%s " % random.choice(partiton_where)
            sql += "%s " % random.choice(interval_sliding)
            # sql += "%s " % random.choice(having_not_support)
            # sql += "%s " % random.choice(order_where)
            sql += "%s " % random.choice(limit1_where)
            sql += ") "
            #sql += "%s " % random.choice(interval_sliding)
            tdLog.info(sql)
            tdLog.info(len(sql))
           #TD-15719 tdSql.query(sql)

        tdSql.query("select 17-6 from stable_1;")
        for i in range(self.fornum):
            sql = "select   apercentile(cal17_1, %d)/1000 ,apercentile(cal17_2, %d)*10+%d from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal17_1 ," % random.choice(calc_aggregate_all)
            sql += "%s as cal17_2 " % random.choice(calc_aggregate_all)
            sql += " from stable_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(interval_sliding)
            #sql += "%s " % random.choice(order_where)
            sql += "%s " % random.choice(limit1_where)
            sql += ") "
            #sql += "%s " % random.choice(interval_sliding)
            tdLog.info(sql)
            tdLog.info(len(sql))
            #TD-15770 tdSql.query(sql)

        tdSql.query("select 17-7 from stable_1;")
        for i in range(self.fornum):
            sql = "select   apercentile(cal17_1, %d)/1000 ,apercentile(cal17_2, %d)*10+%d from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal17_1 ," % random.choice(calc_aggregate_all_j)
            sql += "%s as cal17_2 " % random.choice(calc_aggregate_all_j)
            sql += " from stable_1_1 t1, stable_1_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(q_u_where)
            sql += "%s " % random.choice(interval_sliding)
            #sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit1_where)
            sql += ") "
            #sql += "%s " % random.choice(interval_sliding)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        tdSql.query("select 17-7.2 from stable_1;")
        for i in range(self.fornum):
            sql = "select   apercentile(cal17_1, %d)/1000 ,apercentile(cal17_2, %d)*10+%d from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal17_1 ," % random.choice(calc_aggregate_all_j)
            sql += "%s as cal17_2 " % random.choice(calc_aggregate_all_j)
            sql += " from stable_1_1 t1, stable_1_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(q_u_or_where)
            sql += "%s " % random.choice(interval_sliding)
            #sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit1_where)
            sql += ") "
            #sql += "%s " % random.choice(interval_sliding)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        self.restartDnodes()
        tdSql.query("select 17-8 from stable_1;")
        for i in range(self.fornum):
            sql = "select   apercentile(cal17_1, %d)/1000 ,apercentile(cal17_2, %d)*10+%d  from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal17_1 ," % random.choice(calc_aggregate_all)
            sql += "%s as cal17_2 " % random.choice(calc_aggregate_all)
            sql += " from regular_table_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(interval_sliding)
            #sql += "%s " % random.choice(order_where)
            sql += "%s " % random.choice(limit1_where)
            sql += ") "
            #sql += "%s " % random.choice(interval_sliding)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        tdSql.query("select 17-9 from stable_1;")
        for i in range(self.fornum):
            sql = "select   apercentile(cal17_1, %d)/1000 ,apercentile(cal17_2, %d)*10+%d  from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal17_1 ," % random.choice(calc_aggregate_all_j)
            sql += "%s as cal17_2 " % random.choice(calc_aggregate_all_j)
            sql += " from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(q_u_where)
            sql += "%s " % random.choice(interval_sliding)
            #sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            #sql += "%s " % random.choice(interval_sliding)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        tdSql.query("select 17-10 from stable_1;")
        for i in range(self.fornum):
            sql = "select   apercentile(cal17_1, %d)/1000 ,apercentile(cal17_2, %d)*10+%d  from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal17_1 ," % random.choice(calc_aggregate_all_j)
            sql += "%s as cal17_2 " % random.choice(calc_aggregate_all_j)
            sql += " from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(q_u_or_where)
            sql += "%s " % random.choice(interval_sliding)
            #sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            #sql += "%s " % random.choice(interval_sliding)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        #18 select apercentile from (select calc_aggregate_alls form regualr_table or stable  where <\>\in\and\or session order by  limit )interval_sliding
        tdSql.query("select 18-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select   apercentile(cal18_1, %d)/1000 ,apercentile(cal18_2, %d)*10+%d  from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal18_1 ," % random.choice(calc_aggregate_all)
            sql += "%s as cal18_2 " % random.choice(calc_aggregate_all)
            sql += " from regular_table_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(session_where)
            #sql += "%s " % random.choice(fill_where)
            #sql += "%s " % random.choice(order_where)
            sql += "%s " % random.choice(limit1_where)
            sql += ") "
            #sql += "%s " % random.choice(interval_sliding)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        tdSql.query("select 18-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select   apercentile(cal18_1, %d)/1000 ,apercentile(cal18_2, %d)*10+%d  from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal18_1 ," % random.choice(calc_aggregate_all_j)
            sql += "%s as cal18_2 " % random.choice(calc_aggregate_all_j)
            sql += " from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and   "
            sql += "%s " % random.choice(q_u_where)
            sql += "%s " % random.choice(session_u_where)
            #sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            #sql += "%s " % random.choice(interval_sliding)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        tdSql.query("select 18-2.2 from stable_1;")
        for i in range(self.fornum):
            sql = "select   apercentile(cal18_1, %d)/1000 ,apercentile(cal18_2, %d)*10+%d  from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal18_1 ," % random.choice(calc_aggregate_all_j)
            sql += "%s as cal18_2 " % random.choice(calc_aggregate_all_j)
            sql += " from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and   "
            sql += "%s " % random.choice(q_u_or_where)
            sql += "%s " % random.choice(session_u_where)
            #sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            #sql += "%s " % random.choice(interval_sliding)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        self.restartDnodes()
        tdSql.query("select 18-3 from stable_1;")
        for i in range(self.fornum):
            sql = "select   apercentile(cal18_1, %d)/1000 ,apercentile(cal18_2, %d)*10+%d  from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal18_1 ," % random.choice(calc_aggregate_all)
            sql += "%s as cal18_2 " % random.choice(calc_aggregate_all)
            sql += " from stable_1_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(session_where)
            #sql += "%s " % random.choice(fill_where)
            #sql += "%s " % random.choice(order_where)
            sql += "%s " % random.choice(limit1_where)
            sql += ") "
            #sql += "%s " % random.choice(interval_sliding)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        tdSql.query("select 18-4 from stable_1;")
        for i in range(self.fornum):
            sql = "select   apercentile(cal18_1, %d)/1000 ,apercentile(cal18_2, %d)*10+%d  from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal18_1 ," % random.choice(calc_aggregate_all_j)
            sql += "%s as cal18_2 " % random.choice(calc_aggregate_all_j)
            sql += " from stable_1_1 t1, regular_table_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(q_u_where)
            sql += "%s " % random.choice(session_u_where)
            #sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            #sql += "%s " % random.choice(interval_sliding)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        tdSql.query("select 18-4.2 from stable_1;")
        for i in range(self.fornum):
            sql = "select   apercentile(cal18_1, %d)/1000 ,apercentile(cal18_2, %d)*10+%d  from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal18_1 ," % random.choice(calc_aggregate_all_j)
            sql += "%s as cal18_2 " % random.choice(calc_aggregate_all_j)
            sql += " from stable_1_1 t1, regular_table_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(q_u_or_where)
            sql += "%s " % random.choice(session_u_where)
            #sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            #sql += "%s " % random.choice(interval_sliding)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        tdSql.query("select 18-5 from stable_1;")
        for i in range(self.fornum):
            sql = "select   apercentile(cal18_1, %d)/1000 ,apercentile(cal18_2, %d)*10+%d  from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal18_1 ," % random.choice(calc_aggregate_all)
            sql += "%s as cal18_2 " % random.choice(calc_aggregate_all)
            sql += " from stable_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(session_where)
            #sql += "%s " % random.choice(fill_where)
            #sql += "%s " % random.choice(order_where)
            sql += "%s " % random.choice(limit1_where)
            sql += ") "
            #sql += "%s " % random.choice(interval_sliding)
            tdLog.info(sql)
            tdLog.info(len(sql))
            #TD-15770 tdSql.query(sql)

        tdSql.query("select 18-6 from stable_1;")
        for i in range(self.fornum):
            sql = "select   apercentile(cal18_1, %d)/1000 ,apercentile(cal18_2, %d)*10+%d  from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal18_1 ," % random.choice(calc_aggregate_all_j)
            sql += "%s as cal18_2 " % random.choice(calc_aggregate_all_j)
            sql += " from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and   "
            sql += "%s " % random.choice(t_join_where)
            sql += "%s " % random.choice(session_u_where)
            #sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            #sql += "%s " % random.choice(interval_sliding)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        tdSql.query("select 18-7 from stable_1;")
        for i in range(self.fornum):
            sql = "select   apercentile(cal18_1, %d)/1000 ,apercentile(cal18_2, %d)*10+%d  from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal18_1 ," % random.choice(calc_aggregate_all_j)
            sql += "%s as cal18_2 " % random.choice(calc_aggregate_all_j)
            sql += " from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and   "
            sql += "%s " % random.choice(qt_u_or_where)
            sql += "%s " % random.choice(session_u_where)
            #sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            #sql += "%s " % random.choice(interval_sliding)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        #19 select apercentile from (select calc_aggregate_alls form regualr_table or stable  where <\>\in\and\or session order by  limit )interval_sliding
        #self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select 19-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select   apercentile(cal19_1, %d)/1000 ,apercentile(cal19_2, %d)*10+%d  from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal19_1 ," % random.choice(calc_aggregate_all)
            sql += "%s as cal19_2 " % random.choice(calc_aggregate_all)
            sql += " from regular_table_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(state_window)
            #sql += "%s " % random.choice(order_where)
            sql += "%s " % random.choice(limit1_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        tdSql.query("select 19-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select   apercentile(cal19_1, %d)/1000 ,apercentile(cal19_2, %d)*10+%d  from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal19_1 ," % random.choice(calc_aggregate_all_j)
            sql += "%s as cal19_2 " % random.choice(calc_aggregate_all_j)
            sql += " from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and   "
            sql += "%s " % random.choice(q_u_where)
            sql += "%s " % random.choice(state_u_window)
            #sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        tdSql.query("select 19-2.2 from stable_1;")
        for i in range(self.fornum):
            sql = "select   apercentile(cal19_1, %d)/1000 ,apercentile(cal19_2, %d)*10+%d  from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal19_1 ," % random.choice(calc_aggregate_all_j)
            sql += "%s as cal19_2 " % random.choice(calc_aggregate_all_j)
            sql += " from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and   "
            sql += "%s " % random.choice(q_u_or_where)
            sql += "%s " % random.choice(state_u_window)
            #sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        tdSql.query("select 19-3 from stable_1;")
        for i in range(self.fornum):
            sql = "select   apercentile(cal19_1, %d)/1000 ,apercentile(cal19_2, %d)*10+%d  from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal19_1 ," % random.choice(calc_aggregate_all)
            sql += "%s as cal19_2 " % random.choice(calc_aggregate_all)
            sql += " from stable_1_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(state_window)
            #sql += "%s " % random.choice(order_where)
            sql += "%s " % random.choice(limit1_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        tdSql.query("select 19-4 from stable_1;")
        for i in range(self.fornum):
            sql = "select   apercentile(cal19_1, %d)/1000 ,apercentile(cal19_2, %d)*10+%d  from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal19_1 ," % random.choice(calc_aggregate_all_j)
            sql += "%s as cal19_2 " % random.choice(calc_aggregate_all_j)
            sql += " from stable_1_1  t1, stable_1_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(q_u_where)
            #sql += "%s " % random.choice(state_window)
            #sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        tdSql.query("select 19-4.2 from stable_1;")
        for i in range(self.fornum):
            sql = "select   apercentile(cal19_1, %d)/1000 ,apercentile(cal19_2, %d)*10+%d  from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal19_1 ," % random.choice(calc_aggregate_all_j)
            sql += "%s as cal19_2 " % random.choice(calc_aggregate_all_j)
            sql += " from stable_1_1  t1, stable_1_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(q_u_or_where)
            #sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        tdSql.query("select 19-5 from stable_1;")
        for i in range(self.fornum):
            sql = "select   apercentile(cal19_1, %d)/1000 ,apercentile(cal19_2, %d)*10+%d  from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal19_1 ," % random.choice(calc_aggregate_all)
            sql += "%s as cal19_2 " % random.choice(calc_aggregate_all)
            sql += " from stable_1 where "
            sql += "%s " % random.choice(q_where)
            sql += "%s " % random.choice(state_window)
            sql += "%s " % random.choice(order_where)
            sql += "%s " % random.choice(limit1_where)
            sql += ") "
            sql += "%s " % random.choice(interval_sliding)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.error(sql)   #'STATE_WINDOW not support for super table query'

        tdSql.query("select 19-6 from stable_1;")
        for i in range(self.fornum):
            sql = "select   apercentile(cal19_1, %d)/1000 ,apercentile(cal19_2, %d)*10+%d  from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal19_1 ," % random.choice(calc_aggregate_all_j)
            sql += "%s as cal19_2 " % random.choice(calc_aggregate_all_j)
            sql += " from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(q_u_where)
            #sql += "%s " % random.choice(state_window)
            #sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            #sql += "%s " % random.choice(interval_sliding)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        tdSql.query("select 19-7 from stable_1;")
        for i in range(self.fornum):
            sql = "select   apercentile(cal19_1, %d)/1000 ,apercentile(cal19_2, %d)*10+%d  from  ( select  " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal19_1 ," % random.choice(calc_aggregate_all_j)
            sql += "%s as cal19_2 " % random.choice(calc_aggregate_all_j)
            sql += " from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(qt_u_or_where)
            #sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            #sql += "%s " % random.choice(interval_sliding)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        #20 select * from (select calc_select_fills form regualr_table or stable  where <\>\in\and\or fill_where group by  order by limit offset  )
        #self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select 20-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select  "
            sql += "%s , " % random.choice(calc_select_fill)
            sql += "%s ," % random.choice(calc_select_fill)
            sql += "%s  " % random.choice(calc_select_fill)
            sql += " from stable_1 where  "
            sql += "%s " % random.choice(interp_where)
            sql += "%s " % random.choice(fill_where)
            sql += "%s " % random.choice(group_where)
            sql += "%s " % random.choice(order_where)
            sql += "%s " % random.choice(limit_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            #interp不支持 tdSql.query(sql)

        rsDn = self.restartDnodes()
        tdSql.query("select 20-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select  "
            sql += "%s , " % random.choice(calc_select_fill_j)
            sql += "%s ," % random.choice(calc_select_fill_j)
            sql += "%s  " % random.choice(calc_select_fill_j)
            sql += " from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s and " % random.choice(t_join_where)
            sql += "%s " % random.choice(interp_where_j)
            sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            #interp不支持 tdSql.query(sql)

        tdSql.query("select 20-2.2 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select  "
            sql += "%s , " % random.choice(calc_select_fill_j)
            sql += "%s ," % random.choice(calc_select_fill_j)
            sql += "%s  " % random.choice(calc_select_fill_j)
            sql += " from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s and " % random.choice(qt_u_or_where)
            sql += "%s " % random.choice(interp_where_j)
            sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            #interp不支持 tdSql.query(sql)

        tdSql.query("select 20-3 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select  "
            sql += "%s , " % random.choice(calc_select_fill)
            sql += "%s ," % random.choice(calc_select_fill)
            sql += "%s  " % random.choice(calc_select_fill)
            sql += " from stable_1 where  "
            sql += "%s " % interp_where[2]
            sql += "%s " % random.choice(fill_where)
            sql += "%s " % random.choice(order_where)
            sql += "%s " % random.choice(limit_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            #interp不支持 tdSql.query(sql)

        tdSql.query("select 20-4 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select  "
            sql += "%s , " % random.choice(calc_select_fill_j)
            sql += "%s ," % random.choice(calc_select_fill_j)
            sql += "%s  " % random.choice(calc_select_fill_j)
            sql += " from stable_1 t1, table_1 t2 where t1.ts = t2.ts and    "
            #sql += "%s and " % random.choice(t_join_where)
            sql += "%s " % interp_where_j[random.randint(0,5)]
            sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            #interp不支持 tdSql.query(sql)

        tdSql.query("select 20-4.2 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select  "
            sql += "%s , " % random.choice(calc_select_fill_j)
            sql += "%s ," % random.choice(calc_select_fill_j)
            sql += "%s  " % random.choice(calc_select_fill_j)
            sql += " from stable_1 t1, stable_1_1 t2 where t1.ts = t2.ts and    "
            sql += "%s and " % random.choice(qt_u_or_where)
            sql += "%s " % interp_where_j[random.randint(0,5)]
            sql += "%s " % random.choice(fill_where)
            sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
         ##interp不支持    tdSql.error(sql)

        tdSql.query("select 20-5 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select  "
            sql += "%s , " % random.choice(calc_select_fill)
            sql += "%s ," % random.choice(calc_select_fill)
            sql += "%s  " % random.choice(calc_select_fill)
            sql += " from regular_table_1 where  "
            sql += "%s " % interp_where[1]
            sql += "%s " % random.choice(fill_where)
            sql += "%s " % random.choice(order_where)
            sql += "%s " % random.choice(limit_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            ##interp不支持 tdSql.query(sql)

        tdSql.query("select 20-6 from stable_1;")
        for i in range(self.fornum):
            sql = "select   * from  ( select  "
            sql += "%s , " % random.choice(calc_select_fill_j)
            sql += "%s ," % random.choice(calc_select_fill_j)
            sql += "%s  " % random.choice(calc_select_fill_j)
            sql += " from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and   "
            #sql += "%s " % random.choice(interp_where_j)
            sql += "%s " % interp_where_j[random.randint(0,5)]
            sql += "%s " % random.choice(order_u_where)
            sql += "%s " % random.choice(limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            ##interp不支持 tdSql.query(sql)

        #1 select * from (select * from (select * form regular_table  where <\>\in\and\or order by limit  ))
        tdSql.query("select 1-1 from stable_1;")
        for i in range(self.fornum):
            # sql_start = "select  *  from ( "
            # sql_end = ")"
            for_num = random.randint(1, 15);
            sql = "select  *  from ("  * for_num
            sql += "select  *  from  ( select * from ( select  "
            sql += "%s, " % random.choice(s_r_select)
            sql += "%s, " % random.choice(q_select)
            sql += "ts from regular_table_1 where "
            sql += "%s " % random.choice(q_where)
            sql += ")) "
            sql += ")"  * for_num
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

            sql2 =  "select  *  from  ( select * from ( select  "
            sql2 += "%s, " % random.choice(s_r_select)
            sql2 += "%s, " % random.choice(q_select)
            sql2 += "ts from regular_table_1 where "
            sql2 += "%s " % random.choice(q_where)
            sql2 += ")) "
            tdLog.info(sql2)
            tdLog.info(len(sql2))

            self.data_matrix_equal('%s' %sql ,1,10,1,1,'%s' %sql2 ,1,10,1,1)
            self.data_matrix_equal('%s' %sql ,1,10,1,1,'%s' %sql ,1,10,3,3)
            self.data_matrix_equal('%s' %sql ,1,10,3,3,'%s' %sql2 ,1,10,3,3)

        for i in range(self.fornum):
            for_num = random.randint(1, 15);
            sql = "select  ts  from ("  * for_num
            sql += "select  *  from  ( select * from ( select  "
            sql += "%s, " % random.choice(s_r_select)
            sql += "%s, " % random.choice(q_select)
            sql += "ts from regular_table_1 where "
            sql += "%s " % random.choice(q_where)
            sql += ")) "
            sql += ")"  * for_num
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

            sql2 =  "select  *  from  ( select * from ( select  "
            sql2 += "%s, " % random.choice(s_r_select)
            sql2 += "%s, " % random.choice(q_select)
            sql2 += "ts from regular_table_1 where "
            sql2 += "%s " % random.choice(q_where)
            sql2 += ")) "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

            self.data_matrix_equal('%s' %sql ,1,10,1,1,'%s' %sql2 ,1,10,1,1)

        #2 select * from (select * from (select * form stable  where <\>\in\and\or order by limit  ))
        tdSql.query("select 2-1 from stable_1;")
        for i in range(self.fornum):
            for_num = random.randint(1, 15);
            sql = "select  *  from ("  * for_num
            sql += "select  *  from  ( select * from ( select  "
            sql += "%s, " % random.choice(s_s_select)
            sql += "%s, " % random.choice(qt_select)
            sql += "ts from stable_1 where "
            sql += "%s " % random.choice(q_where)
            sql += ")) "
            sql += ")"  * for_num
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

            sql2 = "select  *  from  ( select * from ( select  "
            sql2 += "%s, " % random.choice(s_s_select)
            sql2 += "%s, " % random.choice(qt_select)
            sql2 += "ts from stable_1 where "
            sql2 += "%s " % random.choice(q_where)
            sql2 += ")) "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

            self.data_matrix_equal('%s' %sql ,1,10,3,3,'%s' %sql2 ,1,10,3,3)

        for i in range(self.fornum):
            for_num = random.randint(1, 15);
            sql = "select  ts  from ("  * for_num
            sql += "select  *  from  ( select * from ( select  "
            sql += "%s, " % random.choice(s_s_select)
            sql += "%s, " % random.choice(qt_select)
            sql += "ts from stable_1 where "
            sql += "%s " % random.choice(q_where)
            sql += ")) "
            sql += ")"  * for_num
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

            sql2 = "select  ts  from  ( select * from ( select  "
            sql2 += "%s, " % random.choice(s_s_select)
            sql2 += "%s, " % random.choice(qt_select)
            sql2 += "ts from stable_1 where "
            sql2 += "%s " % random.choice(q_where)
            sql2 += ")) "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

            self.data_matrix_equal('%s' %sql ,1,10,1,1,'%s' %sql2 ,1,10,1,1)

        #3 select ts ,calc from  (select * form stable  where <\>\in\and\or order by limit  )
        #self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select 3-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select   "
            sql += "%s " % random.choice(calc_calculate_regular)
            sql += " from ( select * from stable_1 where "
            sql += "%s " % random.choice(qt_where)
            sql += "%s " % random.choice(orders_desc_where)
            sql += "%s " % random.choice(limit_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            #'Invalid function name: derivative'  tdSql.query(sql)

        #4 select * from  (select calc form stable  where <\>\in\and\or order by limit  )
        tdSql.query("select 4-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select  *  from  ( select  "
            sql += "%s " % random.choice(calc_select_in_ts)
            sql += "from stable_1 where "
            sql += "%s " % random.choice(qt_where)
            #sql += "%s " % random.choice(order_desc_where)
            sql += "%s " % random.choice(limit_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)

        #5 select ts ,tbname from  (select * form stable  where <\>\in\and\or order by limit  )
        tdSql.query("select 5-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select   ts , tbname , "
            sql += "%s ," % random.choice(calc_calculate_regular)
            sql += "%s ," % random.choice(dqt_select)
            sql += "%s " % random.choice(qt_select)
            sql += " from ( select * from stable_1 where "
            sql += "%s " % random.choice(qt_where)
            sql += "%s " % random.choice(orders_desc_where)
            sql += "%s " % random.choice(limit_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.error(sql)

        #special sql
        tdSql.query("select 6-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select * from ( select _block_dist() from stable_1);"
            # tdSql.query(sql)
            # tdSql.checkRows(1)
            sql = "select _block_dist() from (select * from stable_1);"
            tdSql.error(sql)
            sql = "select * from (select database());"
            tdSql.error(sql)
            sql = "select * from (select client_version());"
            tdSql.error(sql)
            sql = "select * from (select client_version() as version);"
            tdSql.error(sql)
            sql = "select * from (select server_version());"
            tdSql.error(sql)
            sql = "select * from (select server_version() as version);"
            tdSql.error(sql)
            sql = "select * from (select server_status());"
            tdSql.error(sql)
            sql = "select * from (select server_status() as status);"
            tdSql.error(sql)


        endTime = time.time()
        print("total time %ds" % (endTime - startTime))




    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
