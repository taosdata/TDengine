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


from itertools import product
from itertools import combinations
from faker import Faker
import operator
import numpy as np
import pandas as pd
import time, datetime
from taostest import TDCase
import subprocess
import os
import taos
import random
from taostest.util.common import TDCom
from taostest.util.remote import Remote

class TDCreateData():
    def __init__(self, tdSql, logger):
        self.tdSql =  tdSql
        self.logger = logger
        
        self._remote: Remote = Remote(self.logger)
        self.tdCommon = TDCom(self.tdSql)
        self._remote._logger.info("********")
        

    def desc(self) -> str:
        case_description = '''
        case1<xyguo>:data create 
        ''' 
        return case_description

    def tags(self) :
		
        return ""

    def author(self) -> str:

        return "Guo Xiangyang"
        
    def restartDnodes(self):
        self.tdDnodes.stop(1)
        self.tdDnodes.start(1)
        
    def drop_db(self,database):
        #delete:
        table_list = ['stable_1','stable_2','stable_null_data','stable_null_childtable','stable_1','stable_2','regular_table_1','stable_1_1','regular_table_2',\
            'regular_table_3','regular_table_1','regular_table_2','regular_table_null','stable_2_1','stable_2_2','stable_2_2','stable_1_3','stable_1_4',]
        for i in table_list:
            self.tdSql.execute("delete from {}.{};".format(database, i))
            #self.tdSql.execute("flush database {};".format(database)) TD-24856
            self.tdSql.execute("reset query cache;")
            self.tdSql.query("select * from {}.{};".format(database, i))
            self.tdSql.checkRow(0)
        
        #drop:
        self.tdSql.execute('''drop database if exists %s ;''' %database)

    def data_check(self, elm, expect_elm , throw=True) -> bool:
        """
        用途：用于在比较元素相等
        输入：两元素
        返回：正常，失败
        """
        if elm == expect_elm:
            self.logger.debug(f"checkEqual success, elm={elm} expect_elm={expect_elm}")
            return True
        else:
            if throw:
                raise AssertionError(f"checkEqual error, elm={elm} expect_elm={expect_elm}")
            else:
                self._set_error_msg(f"checkEqual error, elm={elm} expect_elm={expect_elm}")
                return False
                    
    def alter_local_slowlogthreshold(self):
        #local variables 修改验证
        show_sql = 'show local variables;'
        
        self.tdSql.query('''alter local 'slowlogthreshold' '10';''') #慢查询门限值，大于等于门限值认为是慢查询，单位：秒        
        self.tdSql.query(show_sql)        
        for i in range(self.tdSql.query_row):
            if (self.tdSql.query_data[i][0] == 'slowLogThreshold'):      
                self.data_check(self.tdSql.query_data[i][1],'10')
        
        #ALL: 启动所有类型记录 \ QUERY: 只记录查询 \ INSERT:只记录写入 \ OTHERS:除写入、查询外其他语句 \ NONE:不记录
        self.tdSql.query('''alter local 'slowLogScope' 'NONE';''')
        self.tdSql.query(show_sql)        
        for i in range(self.tdSql.query_row):
            if (self.tdSql.query_data[i][0] == 'slowLogScope'):      
                self.data_check(self.tdSql.query_data[i][1],'NONE')
                
        self.tdSql.query('''alter local 'slowLogScope' 'INSERT';''')
        self.tdSql.query(show_sql)        
        for i in range(self.tdSql.query_row):
            if (self.tdSql.query_data[i][0] == 'slowLogScope'):      
                self.data_check(self.tdSql.query_data[i][1],'INSERT')
                
        self.tdSql.query('''alter local 'slowLogScope' 'QUERY';''')
        self.tdSql.query(show_sql)        
        for i in range(self.tdSql.query_row):
            if (self.tdSql.query_data[i][0] == 'slowLogScope'):      
                self.data_check(self.tdSql.query_data[i][1],'QUERY')
                
        self.tdSql.query('''alter local 'slowLogScope' 'OTHERS';''')
        self.tdSql.query(show_sql)        
        for i in range(self.tdSql.query_row):
            if (self.tdSql.query_data[i][0] == 'slowLogScope'):      
                self.data_check(self.tdSql.query_data[i][1],'OTHERS')
                
        self.tdSql.query('''alter local 'slowLogScope' 'ALL';''')
        self.tdSql.query(show_sql)        
        for i in range(self.tdSql.query_row):
            if (self.tdSql.query_data[i][0] == 'slowLogScope'):      
                self.data_check(self.tdSql.query_data[i][1],'ALL')
        
            
    def show_local_variables(self):
        self.alter_local_slowlogthreshold()
        self.tdSql.query('''show local variables;''')
        for i in range(self.tdSql.query_row):
            self.logger.info("%s - %s"% (self.tdSql.query_data[i][0], self.tdSql.query_data[i][1]))

    def dropandcreateDB_random(self,database,n):
        self.ts = 1630000000000
        self.num_random = 100
        fake = Faker('zh_CN')
        # self.tdSql.execute('''drop database if exists %s ;''' %database)
        # self.tdSql.execute('''create database %s keep 36500;'''%database)
        self.show_local_variables()
        self.tdCommon.createDb(database, True, keep=36500)
        self.tdSql.execute('''use %s;'''%database)

        self.tdSql.execute('''create stable stable_1 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')
        self.tdSql.execute('''create stable stable_2 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')
        
        self.tdSql.execute('''create stable stable_null_data (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')

        self.tdSql.execute('''create stable stable_null_childtable (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')
        
        self.tdSql.execute('''create stable stable_null_data_insert_data (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')

        #self.tdSql.execute('''create table stable_1_1 using stable_1 tags('stable_1_1', '0' , '0' , '0' , '0' , 0 , 'binary1' , 'nchar1' , '0' , '0' ,'0') ;''')
        self.tdSql.execute('''create table stable_1_1 using stable_1 tags('stable_1_1', '%d' , '%d', '%d' , '%d' , 0 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'0') ;''' 
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat())) 
        #self.tdSql.execute('''create table stable_1_2 using stable_1 tags('stable_1_2', '2147483647' , '9223372036854775807' , '32767' , '127' , 1 , 'binary2' , 'nchar2' , '2' , '22' , \'1999-09-09 09:09:09.090\') ;''')
        self.tdSql.execute('''create table stable_1_2 using stable_1 tags('stable_1_2', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' , \'1999-09-09 09:09:09.090\') ;''' 
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat())) 
        self.tdSql.execute('''create table stable_1_3 using stable_1 tags('stable_1_3', '-2147483647' , '-9223372036854775807' , '-32767' , '-127' , false , 'binary3' , 'nchar3nchar3' , '-3.3' , '-33.33' , \'2099-09-09 09:09:09.090\') ;''')
        self.tdSql.execute('''create table stable_1_4 using stable_1 tags('stable_1_4', '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0') ;''')
        self.tdSql.execute('''create table stable_1_5 using stable_1 tags('stable_1_5', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 
        self.tdSql.execute('''create table stable_1_6 using stable_1 tags('stable_1_6', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 

        #self.tdSql.execute('''create table stable_2_1 using stable_2 tags('stable_2_1' , '0' , '0' , '0' , '0' , 0 , 'binary21' , 'nchar21' , '0' , '0' ,'0') ;''')
        self.tdSql.execute('''create table stable_2_1 using stable_2 tags('stable_2_1', '%d' , '%d', '%d' , '%d' , 1 , 'binary2.%s' , 'nchar2.%s' , '%f', '%f' ,'0') ;''' 
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat())) 
        self.tdSql.execute('''create table stable_2_2 using stable_2 tags('stable_2_2' , '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0') ;''')
        self.tdSql.execute('''create table stable_2_3 using stable_2 tags('stable_2_3', '%d' , '%d', '%d' , '%d' , 1 , 'binary2.%s' , 'nchar2.%s' , '%f', '%f' ,'%d') ;''' 
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 
        self.tdSql.execute('''create table stable_2_4 using stable_2 tags('stable_2_4', '%d' , '%d', '%d' , '%d' , 1 , 'binary2.%s' , 'nchar2.%s' , '%f', '%f' ,'%d') ;''' 
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 
        self.tdSql.execute('''create table stable_2_5 using stable_2 tags('stable_2_5', '%d' , '%d', '%d' , '%d' , 1 , 'binary2.%s' , 'nchar2.%s' , '%f', '%f' ,'%d') ;''' 
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 
        self.tdSql.execute('''create table stable_2_6 using stable_2 tags('stable_2_6', '%d' , '%d', '%d' , '%d' , 1 , 'binary2.%s' , 'nchar2.%s' , '%f', '%f' ,'%d') ;''' 
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 

        self.tdSql.execute('''create table stable_null_data_1 using stable_null_data tags('stable_null_data_1', '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0') ;''')

        self.tdSql.execute('''create table stable_null_data_insert_data_1 using stable_null_data_insert_data tags('stable_null_data_insert_data_1', '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0') ;''')

        #regular table
        self.tdSql.execute('''create table regular_table_1 \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')
        self.tdSql.execute('''create table regular_table_2 \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')
        self.tdSql.execute('''create table regular_table_3 \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')

        self.tdSql.execute('''create table regular_table_null \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')


        for i in range(self.num_random*n):        
            self.tdSql.execute('''insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (self.ts + i*15000000, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                        fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))
            self.tdSql.execute('''insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (self.ts + i*15000000, fake.random_int(min=-2147483647, max=2147483647, step=1) , 
                        fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1) , 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))

            self.tdSql.execute('''insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (self.ts + i*15000000 -1, fake.random_int(min=0, max=2147483647, step=1), 
                        fake.random_int(min=0, max=9223372036854775807, step=1), 
                        fake.random_int(min=0, max=32767, step=1) , fake.random_int(min=0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))
            self.tdSql.execute('''insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (self.ts + i*15000000, fake.random_int(min=0, max=2147483647, step=1), 
                        fake.random_int(min=0, max=9223372036854775807, step=1), 
                        fake.random_int(min=0, max=32767, step=1) , fake.random_int(min=0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))
            
            self.tdSql.execute('''insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (self.ts + i*15000000 +1, fake.random_int(min=-2147483647, max=0, step=1), 
                        fake.random_int(min=-9223372036854775807, max=0, step=1), 
                        fake.random_int(min=-32767, max=0, step=1) , fake.random_int(min=-127, max=0, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i +1))
            self.tdSql.execute('''insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (self.ts + i*15000000 +1, fake.random_int(min=-2147483647, max=0, step=1), 
                        fake.random_int(min=-9223372036854775807, max=0, step=1), 
                        fake.random_int(min=-32767, max=0, step=1) , fake.random_int(min=-127, max=0, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i +1))

            self.tdSql.execute('''insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (self.ts + i*15000000, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                        fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))

            # self.tdSql.execute('''insert into regular_table_3 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d)''' 
            #             % (self.ts + i*1000, fake.random_int(min=-2147483647, max=0, step=1), 
            #             fake.random_int(min=-9223372036854775807, max=0, step=1), 
            #             fake.random_int(min=-32767, max=0, step=1) , fake.random_int(min=-127, max=0, step=1) , 
            #             fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))

        i = random.randint(0,4)
        if i ==0:
            self.logger.info("======this case test use flush database =========")
            self.tdSql.execute("flush database %s;" %database) 
        elif i ==1:  
            self.logger.info("======this case test use flush database =========")
            self.tdSql.execute("flush database %s;" %database)   
            self.logger.info("======this case test keepcolumnname = 1 =========")  
            self.tdSql.execute("alter local 'keepcolumnname' '1';")  
        elif i ==2:  
            self.logger.info("======this case test use flush database =========")
            self.tdSql.execute("flush database %s;" %database)   
            self.logger.info("======this case test keepcolumnname = 0 =========")  
            self.tdSql.execute("alter local 'keepcolumnname' '0';")  
        else:
            self.logger.info("===!!!===this case test not use flush database =====!!!====")
        
        self.tdSql.query("select count(*) from stable_1;")
        self.tdSql.checkData(0,0,3*self.num_random*n)
        self.tdSql.query("select count(*) from regular_table_1;")
        self.tdSql.checkData(0,0,self.num_random*n)
        
        self.alter_cachemodel(database)
        
        self.tdSql.query("select count(*) from stable_1;")
        self.tdSql.checkData(0,0,3*self.num_random*n)
        self.tdSql.query("select count(*) from regular_table_1;")
        self.tdSql.checkData(0,0,self.num_random*n)
        
        self.add_data_random(database,n,1630000000000)
        self.add_data_random(database,n,1640000000000)
    
    def alter_cachemodel(self,database):
        i = random.randint(0,5)
        cachesize = random.randint(1,666)
        if i ==0:
            self.logger.info("======this case test cachemodel none =========")
            #sql = "alter database last_60w cachemodel 'both' cachesize 600;"
            #self.tdSql.execute("flush database %s;" %database)   
            #self.tdSql.query(sql1,queryTimes=1)    
        elif i ==1:
            self.logger.info("======this case test cachemodel last_row =========")
            sql = "alter database %s cachemodel 'last_row' cachesize %d;"  %(database,cachesize)
            self.tdSql.query(sql,queryTimes=1)  
        elif i ==2:
            self.logger.info("======this case test cachemodel last_value =========")
            sql = "alter database %s cachemodel 'last_value' cachesize %d;"  %(database,cachesize)
            self.tdSql.query(sql,queryTimes=1)
        else:
            self.logger.info("======this case test cachemodel both =========")
            sql = "alter database %s cachemodel 'both' cachesize %d;"  %(database,cachesize)
            self.tdSql.query(sql,queryTimes=1)
        
    def add_data_random(self,database,n,ts):
        #增加数据稀疏
        self.ts = ts
        self.num_random = 100
        fake = Faker('zh_CN')
        random_num = random.randint(0,500)
        
        for i in range(self.num_random*n):        
            self.tdSql.execute('''insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (self.ts + i*10000*random_num, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                        fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))
            self.tdSql.execute('''insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (self.ts + i*10000*random_num, fake.random_int(min=-2147483647, max=2147483647, step=1) , 
                        fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1) , 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))

            self.tdSql.execute('''insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (self.ts + i*10000*random_num -1, fake.random_int(min=0, max=2147483647, step=1), 
                        fake.random_int(min=0, max=9223372036854775807, step=1), 
                        fake.random_int(min=0, max=32767, step=1) , fake.random_int(min=0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))
            self.tdSql.execute('''insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (self.ts + i*10000*random_num, fake.random_int(min=0, max=2147483647, step=1), 
                        fake.random_int(min=0, max=9223372036854775807, step=1), 
                        fake.random_int(min=0, max=32767, step=1) , fake.random_int(min=0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))
            
            self.tdSql.execute('''insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (self.ts + i*10000*random_num +1, fake.random_int(min=-2147483647, max=0, step=1), 
                        fake.random_int(min=-9223372036854775807, max=0, step=1), 
                        fake.random_int(min=-32767, max=0, step=1) , fake.random_int(min=-127, max=0, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i +1))
            self.tdSql.execute('''insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (self.ts + i*10000*random_num +1, fake.random_int(min=-2147483647, max=0, step=1), 
                        fake.random_int(min=-9223372036854775807, max=0, step=1), 
                        fake.random_int(min=-32767, max=0, step=1) , fake.random_int(min=-127, max=0, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i +1))

            self.tdSql.execute('''insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (self.ts + i*10000*random_num, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                        fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))
            
    def dropandcreateDB_tsbs(self,database,n):
        self.ts = 1630000000000
        self.num_random = 10
        self.stable_child_num = 10
        fake = Faker('zh_CN')
        # self.tdSql.execute('''drop database if exists %s ;''' %database)
        # self.tdSql.execute('''create database %s keep 36500;'''%database)
        self.show_local_variables()
        self.tdCommon.createDb(database, True, keep=36500)
        self.tdSql.execute('''use %s;'''%database)
        # 1 = readings 2 = diagnostics
        self.tdSql.execute('''create stable stable_1 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp,\
                latitude double , longitude double , elevation double , velocity double , heading double , grade double , fuel_consumption double , load_capacity double , fuel_capacity double , nominal_fuel_consumption double ) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp,\
                    name binary(30) , fleet binary(30) , driver binary(30) , model binary(30) , device_version binary(30));''')
        self.tdSql.execute('''create stable stable_2 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp, \
                fuel_state double , current_load double  ,status tinyint , load_capacity double , fuel_capacity double , nominal_fuel_consumption double ) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp ,\
                    name binary(30) , fleet binary(30) , driver binary(30) , model binary(30) , device_version binary(30));''')
        
        self.tdSql.execute('''create stable stable_null_data (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')

        self.tdSql.execute('''create stable stable_null_childtable (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')
        
        self.tdSql.execute('''create stable stable_null_data_insert_data (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')
        
        self.tdSql.execute('''create table stable_null_data_insert_data_1 using stable_null_data_insert_data tags('stable_null_data_insert_data_1', '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0') ;''')

        for i in range(self.stable_child_num):
            if i == 1 or i == 2 :
                self.tdSql.execute('''create table stable_1_%d using stable_1 tags('stable_1_%d', '%d' , '%d', '%d' , '%d' , 0 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'0', 'truck_%d', 'South%d', 'Trish%d', NULL, 'v2.3') ;''' 
                      %(i , i , fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(), i , i , i )) 
            else :
                self.tdSql.execute('''create table stable_1_%d using stable_1 tags('stable_1_%d', '%d' , '%d', '%d' , '%d' , 0 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'0', 'truck_%d', 'South%d', 'Trish%d', 'H-%d', 'v2.3') ;''' 
                      %(i , i , fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(), i , i , i , i )) 
        
        for i in range(self.stable_child_num):
            if i /2 == 0 :
                self.tdSql.execute('''create table stable_2_%d using stable_2 tags('stable_2_%d', '%d' , '%d', '%d' , '%d' , 0 , 'binary2.%s' , 'nchar2.%s' , '%f', '%f' ,'0', 'truck_%d', 'South%d', 'Trish%d', NULL, 'v2.3') ;''' 
                      %(i , i , fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(), i , i , i )) 
            else :
                self.tdSql.execute('''create table stable_2_%d using stable_2 tags('stable_2_%d', '%d' , '%d', '%d' , '%d' , 0 , 'binary2.%s' , 'nchar2.%s' , '%f', '%f' ,'0', 'truck_%d', 'South%d', 'Trish%d', 'H-%d', 'v2.3') ;''' 
                      %(i , i , fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(), i , i , i , i )) 
        

        self.tdSql.execute('''create table stable_null_data_1 using stable_null_data tags('stable_null_data_1', '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0') ;''')

        #regular table
        self.tdSql.execute('''create table regular_table_1 \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')
        self.tdSql.execute('''create table regular_table_2 \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')
        self.tdSql.execute('''create table regular_table_3 \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')

        self.tdSql.execute('''create table regular_table_null \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')


        for i in range(self.num_random*n):        
            self.tdSql.execute('''insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (self.ts + i*15000000, fake.random_int(min=-2147483647, max=2147483647, step=1) , 
                        fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1) , 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))

            self.tdSql.execute('''insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (self.ts + i*15000000, fake.random_int(min=0, max=2147483647, step=1), 
                        fake.random_int(min=0, max=9223372036854775807, step=1), 
                        fake.random_int(min=0, max=32767, step=1) , fake.random_int(min=0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))

            self.tdSql.execute('''insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (self.ts + i*15000000 +1, fake.random_int(min=-2147483647, max=0, step=1), 
                        fake.random_int(min=-9223372036854775807, max=0, step=1), 
                        fake.random_int(min=-32767, max=0, step=1) , fake.random_int(min=-127, max=0, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i +1))
            
        for i in range(self.num_random*n): 
            for j in range(self.stable_child_num):       
                self.tdSql.execute('''insert into stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts ,\
                                latitude ,longitude ,elevation ,velocity ,heading ,grade ,fuel_consumption ,load_capacity ,fuel_capacity ,nominal_fuel_consumption) \
                                values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f) ;''' 
                            % ( j, self.ts + i*15000000, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i ,
                            fake.random_int(min=100, max=1000, step=1),fake.random_int(min=100, max=1000, step=1),fake.random_int(min=100, max=1000, step=1),fake.random_int(min=100, max=1000, step=1),fake.random_int(min=100, max=1000, step=1),
                            fake.random_int(min=100, max=1000, step=1),fake.random_int(min=100, max=1000, step=1),fake.random_int(min=100, max=1000, step=1),fake.random_int(min=1000, max=10000, step=1),fake.random_int(min=100, max=1000, step=1)))
                
                self.tdSql.execute('''insert into stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts ,\
                                latitude ,longitude ,elevation ,velocity ,heading ,grade ,fuel_consumption ,load_capacity ,fuel_capacity ,nominal_fuel_consumption) \
                                values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f) ;''' 
                            % ( j, self.ts + i*15000000-1, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i -1,
                            fake.random_int(min=100, max=1000, step=1),fake.random_int(min=100, max=1000, step=1),fake.random_int(min=100, max=1000, step=1),fake.random_int(min=100, max=1000, step=1),fake.random_int(min=100, max=1000, step=1),
                            fake.random_int(min=100, max=1000, step=1),fake.random_int(min=100, max=1000, step=1),fake.random_int(min=100, max=1000, step=1),fake.random_int(min=1000, max=10000, step=1),fake.random_int(min=100, max=1000, step=1)))
                
                self.tdSql.execute('''insert into stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts ,\
                                  latitude ,longitude ,elevation ,velocity ,heading ,grade ,fuel_consumption ,load_capacity ,fuel_capacity ,nominal_fuel_consumption) \
                                  values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f) ;''' 
                            % ( j, self.ts + i*15000000+1, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i +1,
                            fake.random_int(min=100, max=1000, step=1),fake.random_int(min=100, max=1000, step=1),fake.random_int(min=100, max=1000, step=1),fake.random_int(min=100, max=1000, step=1),fake.random_int(min=100, max=1000, step=1),
                            fake.random_int(min=100, max=1000, step=1),fake.random_int(min=100, max=1000, step=1),fake.random_int(min=100, max=1000, step=1),fake.random_int(min=1000, max=10000, step=1),fake.random_int(min=100, max=1000, step=1)))
                
                self.tdSql.execute('''insert into stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                                  values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % ( j, self.ts + i*15000000+10, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i + 10))
                
                # self.tdSql.execute('''insert into stable_1_%d  (ts , q_binary , q_nchar, q_ts  ) values(%d, 'binary.%s', 'nchar.%s' , %d) ;''' 
                #             % ( j, self.ts + i*15000000+5 , fake.pystr() , fake.address() , self.ts + i))

                status= random.randint(0,1)
                self.tdSql.execute('''insert into stable_2_%d (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts ,fuel_state , current_load ,status , load_capacity , fuel_capacity , nominal_fuel_consumption) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, %f, %f, %d, %f, %f, %f) ;''' 
                            % ( j ,self.ts + i*15000000, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i ,
                            fake.random_int(min=100, max=32767, step=1),fake.random_int(min=100, max=32767, step=1),status,fake.random_int(min=100, max=32767, step=1),fake.random_int(min=100, max=32767, step=1),fake.random_int(min=100, max=32767, step=1)))
                
                self.tdSql.execute('''insert into stable_2_%d (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts ,status ) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, %d) ;''' 
                            % ( j ,self.ts + i*15000000 + 1, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i +1,status))
            
                self.tdSql.execute('''insert into stable_2_%d (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts  ) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % ( j ,self.ts + i*15000000 + 9, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i +9))
                
                self.tdSql.execute('''insert into stable_2_%d (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts ,fuel_state , current_load ,status , load_capacity , fuel_capacity , nominal_fuel_consumption) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, %f, %f, %d, %f, %f, %f) ;''' 
                            % ( j ,self.ts + i*15000000 + 20, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i +20,
                            fake.random_int(min=100, max=32767, step=1),fake.random_int(min=100, max=32767, step=1),status,fake.random_int(min=100, max=32767, step=1),fake.random_int(min=100, max=32767, step=1),fake.random_int(min=100, max=32767, step=1)))
                                    
                # self.tdSql.execute('''insert into stable_2_%d  (ts , q_binary , q_nchar, q_ts  ) values(%d, 'binary.%s', 'nchar.%s' , %d) ;''' 
                #             % ( j, self.ts + i*15000000+5 , fake.pystr() , fake.address() , self.ts + i))

        i = random.randint(0,1)
        if i ==0:
            self.logger.info("======this case test use flush database =========")
            self.tdSql.execute("flush database %s;" %database)       
        elif i ==1:
            self.logger.info("===!!!===this case test not use flush database =====!!!====")

        self.tdSql.query("select count(*) from stable_2;")
        self.tdSql.checkData(0,0,self.stable_child_num*self.num_random*n*4)
        self.tdSql.query("select count(*) from regular_table_1;")
        self.tdSql.checkData(0,0,self.num_random*n)
        
        self.alter_cachemodel(database)
        
        self.tdSql.query("select count(*) from stable_1;")
        self.tdSql.checkData(0,0,3*self.num_random*n)
        self.tdSql.query("select count(*) from regular_table_1;")
        self.tdSql.checkData(0,0,self.num_random*n)
        
        self.add_data_random(database,n,1630000000000)
        self.add_data_random(database,n,1640000000000)
        
    def dropandcreateDB_random_diff(self,database,n):
        self.ts = 1630000000000
        self.num_random = 100
        fake = Faker('zh_CN')
        # self.tdSql.execute('''drop database if exists %s ;''' %database)
        # self.tdSql.execute('''create database %s keep 36500;'''%database)
        self.show_local_variables()
        self.tdCommon.createDb(database, True, keep=36500)
        self.tdSql.execute('''use %s;'''%database)

        self.tdSql.execute('''create stable stable_1 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')
        self.tdSql.execute('''create stable stable_2 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')
        
        self.tdSql.execute('''create stable stable_null_data (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')

        self.tdSql.execute('''create stable stable_null_childtable (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')
        
        self.tdSql.execute('''create stable stable_null_data_insert_data (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')
        
        #self.tdSql.execute('''create table stable_1_1 using stable_1 tags('stable_1_1', '0' , '0' , '0' , '0' , 0 , 'binary1' , 'nchar1' , '0' , '0' ,'0') ;''')
        self.tdSql.execute('''create table stable_1_1 using stable_1 tags('stable_1_1', '%d' , '%d', '%d' , '%d' , 0 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'0') ;''' 
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat())) 
        #self.tdSql.execute('''create table stable_1_2 using stable_1 tags('stable_1_2', '2147483647' , '9223372036854775807' , '32767' , '127' , 1 , 'binary2' , 'nchar2' , '2' , '22' , \'1999-09-09 09:09:09.090\') ;''')
        self.tdSql.execute('''create table stable_1_2 using stable_1 tags('stable_1_2', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' , \'1999-09-09 09:09:09.090\') ;''' 
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat())) 
        self.tdSql.execute('''create table stable_1_3 using stable_1 tags('stable_1_3', '-2147483647' , '-9223372036854775807' , '-32767' , '-127' , false , 'binary3' , 'nchar3nchar3' , '-3.3' , '-33.33' , \'2099-09-09 09:09:09.090\') ;''')
        self.tdSql.execute('''create table stable_1_4 using stable_1 tags('stable_1_4', '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0') ;''')

        #self.tdSql.execute('''create table stable_2_1 using stable_2 tags('stable_2_1' , '0' , '0' , '0' , '0' , 0 , 'binary21' , 'nchar21' , '0' , '0' ,'0') ;''')
        self.tdSql.execute('''create table stable_2_1 using stable_2 tags('stable_2_1', '%d' , '%d', '%d' , '%d' , 1 , 'binary2.%s' , 'nchar2.%s' , '%f', '%f' ,'0') ;''' 
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat())) 
        #self.tdSql.execute('''create table stable_2_2 using stable_2 tags('stable_2_2' , '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0') ;''')
        self.tdSql.execute('''create table stable_2_2 using stable_2 tags('stable_2_2', '%d' , '%d', '%d' , '%d' , 0 , 'binary2.%s' , 'nchar2.%s' , '%f', '%f' ,'0') ;''' 
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat())) 

        self.tdSql.execute('''create table stable_null_data_1 using stable_null_data tags('stable_null_data_1', '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0') ;''')
        
        self.tdSql.execute('''create table stable_null_data_insert_data_1 using stable_null_data_insert_data tags('stable_null_data_insert_data_1', '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0') ;''')

        #regular table
        self.tdSql.execute('''create table regular_table_1 \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')
        self.tdSql.execute('''create table regular_table_2 \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')
        self.tdSql.execute('''create table regular_table_3 \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')

        self.tdSql.execute('''create table regular_table_null \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')


        for i in range(self.num_random*n):        
            self.tdSql.execute('''insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (self.ts + i*15000000, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                        fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))
            self.tdSql.execute('''insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (self.ts + i*15000000, fake.random_int(min=-2147483647, max=2147483647, step=1) , 
                        fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1) , 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))

            self.tdSql.execute('''insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (self.ts + i*15000000 -1, fake.random_int(min=0, max=2147483647, step=1), 
                        fake.random_int(min=0, max=9223372036854775807, step=1), 
                        fake.random_int(min=0, max=32767, step=1) , fake.random_int(min=0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))
            self.tdSql.execute('''insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (self.ts + i*15000000 -1, fake.random_int(min=0, max=2147483647, step=1), 
                        fake.random_int(min=0, max=9223372036854775807, step=1), 
                        fake.random_int(min=0, max=32767, step=1) , fake.random_int(min=0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))
            
            self.tdSql.execute('''insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (self.ts + i*15000000 +1, fake.random_int(min=-2147483647, max=0, step=1), 
                        fake.random_int(min=-9223372036854775807, max=0, step=1), 
                        fake.random_int(min=-32767, max=0, step=1) , fake.random_int(min=-127, max=0, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i +1))
            self.tdSql.execute('''insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (self.ts + i*15000000 +1, fake.random_int(min=-2147483647, max=0, step=1), 
                        fake.random_int(min=-9223372036854775807, max=0, step=1), 
                        fake.random_int(min=-32767, max=0, step=1) , fake.random_int(min=-127, max=0, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i +1))

            self.tdSql.execute('''insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (self.ts + i*15000000, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                        fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))

            # self.tdSql.execute('''insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
            #             % (self.ts + i*15000000 +1, fake.random_int(min=-0, max=2147483647, step=1), 
            #             fake.random_int(min=-0, max=9223372036854775807, step=1), 
            #             fake.random_int(min=-0, max=32767, step=1) , fake.random_int(min=-0, max=127, step=1) , 
            #             fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))

            # self.tdSql.execute('''insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
            #             % (self.ts + i*15000000 +10, fake.random_int(min=-0, max=2147483647, step=1), 
            #             fake.random_int(min=-0, max=9223372036854775807, step=1), 
            #             fake.random_int(min=-0, max=32767, step=1) , fake.random_int(min=-0, max=127, step=1) , 
            #             fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))

        i = random.randint(0,1)
        if i ==0:
            self.logger.info("======this case test use flush database =========")
            self.tdSql.execute("flush database %s;" %database)       
        elif i ==1:
            self.logger.info("===!!!===this case test not use flush database =====!!!====")

        self.tdSql.query("select count(*) from stable_1;")
        self.tdSql.checkData(0,0,3*self.num_random*n)
        self.tdSql.query("select count(*) from regular_table_1;")
        self.tdSql.checkData(0,0,self.num_random*n)
        
        self.alter_cachemodel(database)
        
        self.tdSql.query("select count(*) from stable_1;")
        self.tdSql.checkData(0,0,3*self.num_random*n)
        self.tdSql.query("select count(*) from regular_table_1;")
        self.tdSql.checkData(0,0,self.num_random*n)
        
        # self.add_data_random(database,n,1630000000000)
        # self.add_data_random(database,n,1640000000000)

    def dropandcreateDB_random_concat(self,database,n):
        #为concat函数定制的，多binary和多nchar
        self.ts = 1630000000000
        self.num_random = 100
        fake = Faker('zh_CN')
        # self.tdSql.execute('''drop database if exists %s ;''' %database)
        # self.tdSql.execute('''create database %s keep 36500;'''%database)
        self.show_local_variables()
        self.tdCommon.createDb(database, True, keep=36500)
        self.tdSql.execute('''use %s;'''%database)

        self.tdSql.execute('''create stable stable_1 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_binary1 binary(100) , q_nchar1 nchar(100) ,q_binary2 binary(100) , q_nchar2 nchar(100) ,q_binary3 binary(100) , q_nchar3 nchar(100) ,q_binary4 binary(100) , q_nchar4 nchar(100) ,\
                q_binary5 binary(100) , q_nchar5 nchar(100) ,q_binary6 binary(100) , q_nchar6 nchar(100) ,q_binary7 binary(100) , q_nchar7 nchar(100) ,q_binary8 binary(100) , q_nchar8 nchar(100) ,\
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')
        self.tdSql.execute('''create stable stable_2 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_binary1 binary(100) , q_nchar1 nchar(100) ,q_binary2 binary(100) , q_nchar2 nchar(100) ,q_binary3 binary(100) , q_nchar3 nchar(100) ,q_binary4 binary(100) , q_nchar4 nchar(100) ,\
                q_binary5 binary(100) , q_nchar5 nchar(100) ,q_binary6 binary(100) , q_nchar6 nchar(100) ,q_binary7 binary(100) , q_nchar7 nchar(100) ,q_binary8 binary(100) , q_nchar8 nchar(100) ,\
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')
        
        self.tdSql.execute('''create stable stable_null_data (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_binary1 binary(100) , q_nchar1 nchar(100) ,q_binary2 binary(100) , q_nchar2 nchar(100) ,q_binary3 binary(100) , q_nchar3 nchar(100) ,q_binary4 binary(100) , q_nchar4 nchar(100) ,\
                q_binary5 binary(100) , q_nchar5 nchar(100) ,q_binary6 binary(100) , q_nchar6 nchar(100) ,q_binary7 binary(100) , q_nchar7 nchar(100) ,q_binary8 binary(100) , q_nchar8 nchar(100) ,\
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')

        self.tdSql.execute('''create stable stable_null_childtable (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_binary1 binary(100) , q_nchar1 nchar(100) ,q_binary2 binary(100) , q_nchar2 nchar(100) ,q_binary3 binary(100) , q_nchar3 nchar(100) ,q_binary4 binary(100) , q_nchar4 nchar(100) ,\
                q_binary5 binary(100) , q_nchar5 nchar(100) ,q_binary6 binary(100) , q_nchar6 nchar(100) ,q_binary7 binary(100) , q_nchar7 nchar(100) ,q_binary8 binary(100) , q_nchar8 nchar(100) ,\
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')
        
        self.tdSql.execute('''create stable stable_null_data_insert_data (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')
        
        self.tdSql.execute('''create table stable_1_1 using stable_1 tags('stable_1_1', '0' , '0' , '0' , '0' , 0 , 'binary1' , 'nchar1' , '0' , '0' ,'0') ;''')
        self.tdSql.execute('''create table stable_1_2 using stable_1 tags('stable_1_2', '2147483647' , '9223372036854775807' , '32767' , '127' , 1 , 'binary2' , 'nchar2' , '2' , '22' , \'1999-09-09 09:09:09.090\') ;''')
        self.tdSql.execute('''create table stable_1_3 using stable_1 tags('stable_1_3', '-2147483647' , '-9223372036854775807' , '-32767' , '-127' , false , 'binary3' , 'nchar3nchar3' , '-3.3' , '-33.33' , \'2099-09-09 09:09:09.090\') ;''')
        self.tdSql.execute('''create table stable_1_4 using stable_1 tags('stable_1_4', '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0') ;''')

        self.tdSql.execute('''create table stable_2_1 using stable_2 tags('stable_2_1' , '0' , '0' , '0' , '0' , 0 , 'binary21' , 'nchar21' , '0' , '0' ,'0') ;''')
        self.tdSql.execute('''create table stable_2_2 using stable_2 tags('stable_2_2' , '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0') ;''')

        self.tdSql.execute('''create table stable_null_data_1 using stable_null_data tags('stable_null_data_1', '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0') ;''')

        self.tdSql.execute('''create table stable_null_data_insert_data_1 using stable_null_data_insert_data tags('stable_null_data_insert_data_1', '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0') ;''')

        #regular table
        self.tdSql.execute('''create table regular_table_1 \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_binary1 binary(100) , q_nchar1 nchar(100) ,q_binary2 binary(100) , q_nchar2 nchar(100) ,q_binary3 binary(100) , q_nchar3 nchar(100) ,q_binary4 binary(100) , q_nchar4 nchar(100) ,\
                    q_binary5 binary(100) , q_nchar5 nchar(100) ,q_binary6 binary(100) , q_nchar6 nchar(100) ,q_binary7 binary(100) , q_nchar7 nchar(100) ,q_binary8 binary(100) , q_nchar8 nchar(100) ,\
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')
        self.tdSql.execute('''create table regular_table_2 \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_binary1 binary(100) , q_nchar1 nchar(100) ,q_binary2 binary(100) , q_nchar2 nchar(100) ,q_binary3 binary(100) , q_nchar3 nchar(100) ,q_binary4 binary(100) , q_nchar4 nchar(100) ,\
                    q_binary5 binary(100) , q_nchar5 nchar(100) ,q_binary6 binary(100) , q_nchar6 nchar(100) ,q_binary7 binary(100) , q_nchar7 nchar(100) ,q_binary8 binary(100) , q_nchar8 nchar(100) ,\
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')
        self.tdSql.execute('''create table regular_table_3 \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_binary1 binary(100) , q_nchar1 nchar(100) ,q_binary2 binary(100) , q_nchar2 nchar(100) ,q_binary3 binary(100) , q_nchar3 nchar(100) ,q_binary4 binary(100) , q_nchar4 nchar(100) ,\
                    q_binary5 binary(100) , q_nchar5 nchar(100) ,q_binary6 binary(100) , q_nchar6 nchar(100) ,q_binary7 binary(100) , q_nchar7 nchar(100) ,q_binary8 binary(100) , q_nchar8 nchar(100) ,\
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')

        self.tdSql.execute('''create table regular_table_null \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_binary1 binary(100) , q_nchar1 nchar(100) ,q_binary2 binary(100) , q_nchar2 nchar(100) ,q_binary3 binary(100) , q_nchar3 nchar(100) ,q_binary4 binary(100) , q_nchar4 nchar(100) ,\
                    q_binary5 binary(100) , q_nchar5 nchar(100) ,q_binary6 binary(100) , q_nchar6 nchar(100) ,q_binary7 binary(100) , q_nchar7 nchar(100) ,q_binary8 binary(100) , q_nchar8 nchar(100) ,\
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')


        for i in range(self.num_random*n):        
            self.tdSql.execute('''insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts, \
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8)  \
                        values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s',  \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (self.ts + i*15000000, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                        fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i , fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address() , 
                        fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address()))
            self.tdSql.execute('''insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts, \
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8)  \
                        values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s',  \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (self.ts + i*15000000, fake.random_int(min=-2147483647, max=2147483647, step=1) , 
                        fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1) , 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i, fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address() , 
                        fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address()))

            self.tdSql.execute('''insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts, \
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8) \
                        values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s',  \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (self.ts + i*15000000 -1, fake.random_int(min=0, max=2147483647, step=1), 
                        fake.random_int(min=0, max=9223372036854775807, step=1), 
                        fake.random_int(min=0, max=32767, step=1) , fake.random_int(min=0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i, fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address() , 
                        fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address()))
            self.tdSql.execute('''insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts, \
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8)  \
                        values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s',  \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (self.ts + i*15000000 -1, fake.random_int(min=0, max=2147483647, step=1), 
                        fake.random_int(min=0, max=9223372036854775807, step=1), 
                        fake.random_int(min=0, max=32767, step=1) , fake.random_int(min=0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i, fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address() , 
                        fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address()))
            
            self.tdSql.execute('''insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts, \
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8)  \
                        values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s',  \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (self.ts + i*15000000 +1, fake.random_int(min=-2147483647, max=0, step=1), 
                        fake.random_int(min=-9223372036854775807, max=0, step=1), 
                        fake.random_int(min=-32767, max=0, step=1) , fake.random_int(min=-127, max=0, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i +1, fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address() , 
                        fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address()))
            self.tdSql.execute('''insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts, \
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8)  \
                        values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s',  \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (self.ts + i*15000000 +1, fake.random_int(min=-2147483647, max=0, step=1), 
                        fake.random_int(min=-9223372036854775807, max=0, step=1), 
                        fake.random_int(min=-32767, max=0, step=1) , fake.random_int(min=-127, max=0, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i +1, fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address() , 
                        fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address()))

            self.tdSql.execute('''insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts, \
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8)  \
                        values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s',  \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (self.ts + i*15000000, fake.random_int(min=-0, max=2147483647, step=1), 
                        fake.random_int(min=-0, max=9223372036854775807, step=1), 
                        fake.random_int(min=-0, max=32767, step=1) , fake.random_int(min=-0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i, fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address() , 
                        fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address()))

            self.tdSql.execute('''insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts, \
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8)  \
                        values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s',  \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (self.ts + i*15000000 +1, fake.random_int(min=-0, max=2147483647, step=1), 
                        fake.random_int(min=-0, max=9223372036854775807, step=1), 
                        fake.random_int(min=-0, max=32767, step=1) , fake.random_int(min=-0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i, fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address() , 
                        fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address()))

            self.tdSql.execute('''insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts, \
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8)  \
                        values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s',  \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (self.ts + i*15000000 +10, fake.random_int(min=-0, max=2147483647, step=1), 
                        fake.random_int(min=-0, max=9223372036854775807, step=1), 
                        fake.random_int(min=-0, max=32767, step=1) , fake.random_int(min=-0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i, fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address() , 
                        fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address() , fake.pystr() , fake.address()))

        i = random.randint(0,1)
        if i ==0:
            self.logger.info("======this case test use flush database =========")
            self.tdSql.execute("flush database %s;" %database)       
        elif i ==1:
            self.logger.info("===!!!===this case test not use flush database =====!!!====")

        self.tdSql.query("select count(*) from stable_1;")
        self.tdSql.checkData(0,0,3*self.num_random*n)
        self.tdSql.query("select count(*) from regular_table_1;")
        self.tdSql.checkData(0,0,self.num_random*n)
        
        self.alter_cachemodel(database)
        
        self.tdSql.query("select count(*) from stable_1;")
        self.tdSql.checkData(0,0,3*self.num_random*n)
        self.tdSql.query("select count(*) from regular_table_1;")
        self.tdSql.checkData(0,0,self.num_random*n)
        
        # self.add_data_random(database,n,1630000000000)
        # self.add_data_random(database,n,1640000000000)
        
                               
    def dropandcreateDB_null(self,database,n):
        self.num_null = 100
        self.ts = 1630000000000
        # self.tdSql.execute('''drop database if exists db ;''')
        # self.tdSql.execute('''create database db keep 36500;''')
        self.show_local_variables()
        self.tdCommon.createDb(database, True, keep=36500)
        self.tdSql.execute('''use db;''')

        self.tdSql.execute('''create stable stable_1 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(20) , q_nchar nchar(20) , q_ts timestamp ,  \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp)  \
                    tags(loc nchar(20) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(20) , t_nchar nchar(20) ,t_float float , t_double double , t_ts timestamp);''')
        self.tdSql.execute('''create stable stable_2 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(20) , q_nchar nchar(20) , q_ts timestamp ,  \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp)  \
                    tags(loc nchar(20) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(20) , t_nchar nchar(20) ,t_float float , t_double double , t_ts timestamp);''')
        
        self.tdSql.execute('''create table table_1 using stable_1 tags('table_1', '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0') ;''')
        self.tdSql.execute('''create table table_2 using stable_1 tags('table_2', '2147483647' , '9223372036854775807' , '32767' , '127' , 1 , 'binary2' , 'nchar2' , '2' , '22' , \'1999-09-09 09:09:09.090\') ;''')
        self.tdSql.execute('''create table table_3 using stable_1 tags('table_3', '-2147483647' , '-9223372036854775807' , '-32767' , '-127' , false , 'binary3' , 'nchar3nchar3' , '-3.3' , '-33.33' , \'2099-09-09 09:09:09.090\') ;''')
        self.tdSql.execute('''create table table_21 using stable_2 tags('table_21' , '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0') ;''')
        
        self.tdSql.execute('''create table regular_table_1 \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(20) , q_nchar nchar(20) , q_ts timestamp ,  \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')
        self.tdSql.execute('''create table regular_table_2 \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(20) , q_nchar nchar(20) , q_ts timestamp ,  \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')
        self.tdSql.execute('''create table regular_table_3 \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(20) , q_nchar nchar(20) , q_ts timestamp ,  \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')
        
        for i in range(self.num_null):        
            self.tdSql.execute('''insert into table_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (self.ts + i*10000, i, i, i, i, i, i, i, i, self.ts + i))
            self.tdSql.execute('''insert into table_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (self.ts + i*3000, i, i, i, i, i, i, i, i, self.ts + i))
            self.tdSql.execute('''insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (self.ts + i*10000, i, i, i, i, i, i, i, i, self.ts + i))
            self.tdSql.execute('''insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (self.ts + i*3000 , i, i, i, i, i, i, i, i, self.ts + i))

            self.tdSql.execute('''insert into table_21  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (self.ts + i*10000, i, i, i, i, i, i, i, i, self.ts + i))
            self.tdSql.execute('''insert into table_21  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (self.ts + i*3000, i, i, i, i, i, i, i, i, self.ts + i))

            self.tdSql.execute('''insert into table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (self.ts + i*10000, 2147483647-i, 9223372036854775807-i, 32767-i, 127-i, i, i, i, i, self.ts + i))
            self.tdSql.execute('''insert into table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (self.ts + i*3000, 2147483647-i, 9223372036854775807-i, 32767-i, 127-i, i, i, i, i, self.ts + i))
            self.tdSql.execute('''insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (self.ts + i*10000, 2147483647-i, 9223372036854775807-i, 32767-i, 127-i, i, i, i, i, self.ts + i))
            self.tdSql.execute('''insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (self.ts + i*3000 , 2147483647-i, 9223372036854775807-i, 32767-i, 127-i, i, i, i, i, self.ts + i))

            self.tdSql.execute('''insert into table_3 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (self.ts + i*10000, -2147483647+i, -9223372036854775807+i, -32767+i, -127+i, -i, -i, i, i, self.ts + i))
            self.tdSql.execute('''insert into table_3 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (self.ts + i*3000, -2147483647+i, -9223372036854775807+i, -32767+i, -127+i, -i, -i, i, i, self.ts + i))
            self.tdSql.execute('''insert into regular_table_3 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (self.ts + i*10000, -2147483647+i, -9223372036854775807+i, -32767+i, -127+i, -i, -i, i, i, self.ts + i))
            self.tdSql.execute('''insert into regular_table_3 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (self.ts + i*3000 , -2147483647+i, -9223372036854775807+i, -32767+i, -127+i, -i, -i, i, i, self.ts + i))

        i = random.randint(0,1)
        if i ==0:
            self.logger.info("======this case test use flush database =========")
            self.tdSql.execute("flush database %s;" %database)       
        elif i ==1:
            self.logger.info("===!!!===this case test not use flush database =====!!!====")

        self.tdSql.query("select count(*) from stable_1;")
        self.tdSql.checkData(0,0,570)
        self.tdSql.query("select count(*) from regular_table_1;")
        self.tdSql.checkData(0,0,190)
        
        self.alter_cachemodel(database)
        
        self.tdSql.query("select count(*) from stable_1;")
        self.tdSql.checkData(0,0,570)
        self.tdSql.query("select count(*) from regular_table_1;")
        self.tdSql.checkData(0,0,190)
        
        self.add_data_random(database,n,1630000000000)
        self.add_data_random(database,n,1640000000000)

    def alter_column(self,sql):
        pass

    def alter_tag(self,sql):
        pass


    def explain_sql(self,sql):   
        #执行sql解析  
        ratio = random.uniform(0.001,1)
        sql_verbose_true = sql  
        sql_verbose_false = sql 
        
        sql_ratio = sql 
        sql_ratio_verbose_true = sql 
        sql_ratio_verbose_false = sql 
        
        sql_analyze = sql 
        sql_analyze_verbose_true = sql 
        sql_analyze_verbose_false = sql 
        
        sql_analyze_ratio = sql 
        sql_analyze_ratio_verbose_true = sql 
        sql_analyze_ratio_verbose_false = sql 
        
        insert_into_sql = sql
        
        i = random.randint(1,9)
        
        if i==1:
        
            sql = "explain " + sql 
            self.tdSql.query(sql) 
            sql_verbose_true = "explain verbose true " + sql_verbose_true 
            self.tdSql.query(sql_verbose_true) 
            # sql_verbose_false = "explain verbose false " + sql_verbose_false 
            # self.tdSql.query(sql_verbose_false)
            
            insert_into_sql = "insert into stable_null_data_insert_data_1 "  + insert_into_sql 
            try:
                self.tdSql.query(insert_into_sql,queryTimes=1) 
                row = self.tdSql.query(insert_into_sql).row_count 
                insert_sql = "select * from stable_null_data_insert_data;"
                self.tdSql.query(insert_sql,queryTimes=1) 
                #row1 = self.tdSql.query(insert_sql).row_count 
                if (row>=0):
                    # sql_verbose_true = "explain verbose true " + insert_into_sql 
                    # self.tdSql.query(sql_verbose_true) 
                    sql_verbose_false = "explain verbose false " + insert_into_sql 
                    self.tdSql.query(sql_verbose_false)
            except:
                self.logger.info("sql insert into explain is not support :=====; ")
                
        elif i==2:
        
            sql = "explain " + sql 
            self.tdSql.query(sql)  
            sql_verbose_false = "explain verbose false " + sql_verbose_false 
            self.tdSql.query(sql_verbose_false)
            
            insert_into_sql = "insert into stable_null_data_insert_data_1 "  + insert_into_sql 
            try:
                self.tdSql.query(insert_into_sql,queryTimes=1) 
                row = self.tdSql.query(insert_into_sql).row_count 
                insert_sql = "select * from stable_null_data_insert_data;"
                self.tdSql.query(insert_sql,queryTimes=1) 
                if (row>=0): 
                    sql_verbose_true = "explain verbose true " + insert_into_sql 
                    self.tdSql.query(sql_verbose_true)
            except:
                self.logger.info("sql insert into explain is not support :=====; ")
        
        elif i ==3:
            
            sql_ratio = "explain ratio {} ".format(ratio) + sql_ratio 
            self.tdSql.query(sql_ratio) 
            sql_ratio_verbose_true = "explain ratio 0.05 verbose true " + sql_ratio_verbose_true 
            self.tdSql.query(sql_ratio_verbose_true) 
            # sql_ratio_verbose_false = "explain ratio 0.05 verbose false " + sql_ratio_verbose_false 
            # self.tdSql.query(sql_ratio_verbose_false)
                       
            insert_into_sql = "insert into stable_null_data_insert_data_1 "  + insert_into_sql 
            try:
                self.tdSql.query(insert_into_sql,queryTimes=1) 
                row = self.tdSql.query(insert_into_sql).row_count 
                insert_sql = "select * from stable_null_data_insert_data;"
                self.tdSql.query(insert_sql,queryTimes=1) 
                #row1 = self.tdSql.query(insert_sql).row_count 
                if (row>=0):
                    # sql_ratio_verbose_true = "explain ratio 0.05 verbose true "  + insert_into_sql 
                    # self.tdSql.query(sql_ratio_verbose_true) 
                    sql_ratio_verbose_false = "explain ratio 0.05 verbose false " + insert_into_sql 
                    self.tdSql.query(sql_ratio_verbose_false)
            except:
                self.logger.info("sql insert into explain is not support :=====; " )
                
        elif i ==4:
            
            sql_ratio = "explain ratio {} ".format(ratio) + sql_ratio 
            self.tdSql.query(sql_ratio) 
            sql_ratio_verbose_false = "explain ratio 0.05 verbose false " + sql_ratio_verbose_false 
            self.tdSql.query(sql_ratio_verbose_false)
                       
            insert_into_sql = "insert into stable_null_data_insert_data_1 "  + insert_into_sql 
            try:
                self.tdSql.query(insert_into_sql,queryTimes=1) 
                row = self.tdSql.query(insert_into_sql).row_count 
                insert_sql = "select * from stable_null_data_insert_data;"
                self.tdSql.query(insert_sql,queryTimes=1) 
                if (row>=0):
                    sql_ratio_verbose_true = "explain ratio 0.05 verbose true "  + insert_into_sql 
                    self.tdSql.query(sql_ratio_verbose_true) 
            except:
                self.logger.info("sql insert into explain is not support :=====; " )
        
        elif i ==5:
            
            sql_analyze = "explain analyze " + sql_analyze 
            self.tdSql.query(sql_analyze)
            sql_analyze_verbose_true = "explain analyze verbose true " + sql_analyze_verbose_true 
            self.tdSql.query(sql_analyze_verbose_true)
            # sql_analyze_verbose_false = "explain analyze verbose false " + sql_analyze_verbose_false 
            # self.tdSql.query(sql_analyze_verbose_false)
                       
            insert_into_sql = "insert into stable_null_data_insert_data_1 "  + insert_into_sql 
            try:
                self.tdSql.query(insert_into_sql,queryTimes=1) 
                row = self.tdSql.query(insert_into_sql).row_count 
                insert_sql = "select * from stable_null_data_insert_data;"
                self.tdSql.query(insert_sql,queryTimes=1) 
                #row1 = self.tdSql.query(insert_sql).row_count 
                if (row>=0):
                    # sql_analyze_verbose_true = "explain analyze verbose true " + insert_into_sql 
                    # self.tdSql.query(sql_analyze_verbose_true) 
                    sql_analyze_verbose_false = "explain analyze verbose false " + insert_into_sql 
                    self.tdSql.query(sql_analyze_verbose_false)
            except:
                self.logger.info("sql insert into explain is not support :=====; ")
                
        elif i ==6:
            
            sql_analyze = "explain analyze " + sql_analyze 
            self.tdSql.query(sql_analyze)
            sql_analyze_verbose_false = "explain analyze verbose false " + sql_analyze_verbose_false 
            self.tdSql.query(sql_analyze_verbose_false)
                       
            insert_into_sql = "insert into stable_null_data_insert_data_1 "  + insert_into_sql 
            try:
                self.tdSql.query(insert_into_sql,queryTimes=1) 
                row = self.tdSql.query(insert_into_sql).row_count 
                insert_sql = "select * from stable_null_data_insert_data;"
                self.tdSql.query(insert_sql,queryTimes=1) 
                if (row>=0):
                    sql_analyze_verbose_true = "explain analyze verbose true " + insert_into_sql 
                    self.tdSql.query(sql_analyze_verbose_true) 
            except:
                self.logger.info("sql insert into explain is not support :=====; ")
        
        elif i ==7:
            
            sql_analyze_ratio = "explain analyze ratio {} ".format(ratio) + sql_analyze_ratio 
            self.tdSql.query(sql_analyze_ratio)
            sql_analyze_ratio_verbose_true = "explain analyze ratio {} verbose true ".format(ratio) + sql_analyze_ratio_verbose_true 
            self.tdSql.query(sql_analyze_ratio_verbose_true)
            # sql_analyze_ratio_verbose_false = "explain analyze ratio {} verbose false ".format(ratio) + sql_analyze_ratio_verbose_false 
            # self.tdSql.query(sql_analyze_ratio_verbose_false)
                       
            insert_into_sql = "insert into stable_null_data_insert_data_1 "  + insert_into_sql 
            try:
                self.tdSql.query(insert_into_sql,queryTimes=1) 
                row = self.tdSql.query(insert_into_sql).row_count 
                insert_sql = "select * from stable_null_data_insert_data;"
                self.tdSql.query(insert_sql,queryTimes=1) 
                #row1 = self.tdSql.query(insert_sql).row_count 
                if (row>=0):
                    # sql_analyze_ratio_verbose_true = "explain analyze ratio {} verbose true ".format(ratio) + insert_into_sql 
                    # self.tdSql.query(sql_analyze_ratio_verbose_true) 
                    sql_analyze_ratio_verbose_false = "explain analyze ratio {} verbose false ".format(ratio) + insert_into_sql 
                    self.tdSql.query(sql_analyze_ratio_verbose_false)
            except:
                self.logger.info("sql insert into explain is not support :=====; " )
                
        else:
            
            sql_analyze_ratio = "explain analyze ratio {} ".format(ratio) + sql_analyze_ratio 
            self.tdSql.query(sql_analyze_ratio)
            sql_analyze_ratio_verbose_false = "explain analyze ratio {} verbose false ".format(ratio) + sql_analyze_ratio_verbose_false 
            self.tdSql.query(sql_analyze_ratio_verbose_false)
                       
            insert_into_sql = "insert into stable_null_data_insert_data_1 "  + insert_into_sql 
            try:
                self.tdSql.query(insert_into_sql,queryTimes=1) 
                row = self.tdSql.query(insert_into_sql).row_count 
                insert_sql = "select * from stable_null_data_insert_data;"
                self.tdSql.query(insert_sql,queryTimes=1) 
                if (row>=0):
                    sql_analyze_ratio_verbose_true = "explain analyze ratio {} verbose true ".format(ratio) + insert_into_sql 
                    self.tdSql.query(sql_analyze_ratio_verbose_true) 
            except:
                self.logger.info("sql insert into explain is not support :=====; " )
        
    def taos_f(self,service_host,testcasePath,testcaseFilename):   
        #执行taos_f 导入解析            
        #taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
        #taos_cmd1 = "taos -h %s -f %s/%s.sql" % (self.service_host,self.testcasePath,self.testcaseFilename)
        # service_host = "ceph01"
        # testcasePath = os.path.split(__file__)[0]
        # testcaseFilename = os.path.split(__file__)[-1]
        # taos_cmd1 = "taos -h %s -f %s/%s.sql" % (service_host,testcasePath,testcaseFilename)
        # _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
        #service_host = "ceph01"
        
        taos_cmd1 = "taos -h %s -f %s/%s.sql" % (service_host,testcasePath,testcaseFilename)
        _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
        self.logger.info("sqlname :============= %s/%s.sql"% (testcasePath,testcaseFilename))

    def case_sql_subprocess_execute(self,service_host,db):
        #service_host = "ceph01"
        
        conn1 = taos.connect(host="%s" %service_host, user="root", password="taosdata", config="/etc/taos/")
        cur1 = conn1.cursor()        
        cur1.execute('use %s;' %db)
        sql = 'select * from regular_table_1 limit 5;'
        cur1.execute(sql)
                       
        #cur1.close()
        #conn1.close()

        return(conn1,cur1)  
             
    def result_0(self,sql):
        self.logger.info(sql) 
        self.tdSql.query(sql)
        self.tdSql.checkRow(0)
        
        sql = sql.replace(";","")
        nest_sql = "select * from (%s);"%sql
        self.tdSql.query(nest_sql)
        self.tdSql.checkRow(0)
                
        nest_nest_sql = nest_sql.replace(";","")
        nest_nest_sql = "select * from (%s);"%nest_nest_sql
        self.tdSql.query(nest_nest_sql)
        self.tdSql.checkRow(0)
        
    def value_check(self,base_value,check_value,sql1,sql2):
        #两个sql及执行数据检查
        #self.logger.debug(f"sql1={sql1},sql2={sql2}")
        if (base_value == check_value) :
            #self.logger.info(("sql1:'%s' result '%s' = sql2:'%s' result '%s' ") %(sql1,base_value,sql2,check_value))
            self.logger.info(("sql1 row nums result '%s' = sql2 row nums result '%s' ") %(base_value,check_value))
        elif (base_value > check_value) :
            self.logger.info(("sql1:'%s' result '%s' > sql2:'%s' result '%s' ,only limit query can cause") %(sql1,base_value,sql2,check_value))
        else:
            self.logger.info(("sql1:'%s' result '%s' != sql2:'%s' result '%s'") %(sql1,base_value,sql2,check_value))
            return self.tdSql.checkEqual(base_value,check_value)
        
    def dataequal_notcheckrow(self, sql1,row1,col1, sql2,row2,col2):
        self.sql1 = sql1
        list1 =[]
        self.tdSql.query(sql1)
        #sql1_row = self.tdSql.query(sql1).row_count
        for i1 in range(row1):
            for j1 in range(col1):
                list1.append(self.tdSql.getData(i1,j1))
        
        self.tdSql.execute("reset query cache;") 
        self.sql2 = sql2  
        list2 =[]
        self.tdSql.query(sql2)
        #sql2_row = self.tdSql.query(sql2).row_count
        for i2 in range(row2):
            for j2 in range(col2):
                list2.append(self.tdSql.getData(i2,j2))
                
        #self.value_check(sql1_row,sql2_row,sql1,sql2)
       
        if  (list1 == list2) and len(list2)>0:
            #self.logger.info(("===list=_===sql1:'%s' result = sql2:'%s' result") %(sql1,sql2))
            self.logger.info(("===list=_===sql1 data result = sql2 data result"))
        elif  (list1.sort() == list2.sort()):
            self.logger.info(("===list=sort_===sql1:'%s' result = sql2:'%s' result") %(sql1,sql2))
        elif str(list1).replace("]","").replace("[","") == str(list2).replace("]","").replace("[",""):
            #result is NAN -NAN
            self.logger.info(("===list_nan===sql1:'%s' result = sql2:'%s' result") %(sql1,sql2))
        elif (list1 == None) and (list2 == None):
            #result is None -None
            self.logger.info(("===list_none===sql1:'%s' result = sql2:'%s' result") %(sql1,sql2))
        elif (list1 == 'NULL') and (list2 == 'NULL'):
            #result is NULL -NULL
            self.logger.info(("===list_none===sql1:'%s' result = sql2:'%s' result") %(sql1,sql2))
        elif str(list1) == str(list2) :
            self.logger.info(("===list=str_===sql1:'%s' result = sql2:'%s' result") %(sql1,sql2))
        elif abs(float(str(list1).replace("]","").replace("[","")) - float(str(list2).replace("]","").replace("[",""))) <= 0.5:
            #self.logger.info(("=====list_abs===sql1.list1:'%s',sql2.list2:'%s'") %(list1,list2))
            self.logger.info(("===list_abs===sql1:'%s' result = sql2:'%s' result") %(sql1,sql2))
        elif (abs(list1-list2)/list1 <= 0.01) or (abs(list1-list2)/list2 <= 0.01):
            #self.logger.info(("=====list_abs+e+===sql1.list1:'%s',sql2.list2:'%s'") %(list1,list2))
            self.logger.info(("===list_abs+sub/list===sql1:'%s' result = sql2:'%s' result") %(sql1,sql2))
        elif abs(float(str(list1).replace("]","").replace("[","").replace("e+","")) - float(str(list2).replace("]","").replace("[","").replace("e+",""))) <= 0.0001:
            #self.logger.info(("=====list_abs+e+===sql1.list1:'%s',sql2.list2:'%s'") %(list1,list2))
            self.logger.info(("===list_abs+e+===sql1:'%s' result = sql2:'%s' result") %(sql1,sql2))
        elif str(list1).replace("datetime.datetime","").replace("]","").replace("[","").replace(", ","").replace("(","").replace(")","").replace("-","").replace("None","")==str(list2).replace("datetime.datetime","").replace("]","").replace("[","").replace(", ","").replace("(","").replace(")","").replace("-","").replace("None",""):
            #self.logger.info(("=====list_abs+e+===sql1.list1:'%s',sql2.list2:'%s'") %(list1,list2))
            self.logger.info(("===list_time===sql1:'%s' result = sql2:'%s' result") %(sql1,sql2))
        else:
            self.logger.info(("sql1:'%s' result != sql2:'%s' result") %(sql1,sql2))
            #self.logger.info(("=====list_error===sql1.list1:'%s',sql2.list2:'%s'") %(list1,list2))
            return self.tdSql.checkEqual(list1,list2)
        
    def dataequal(self, sql1,row1,col1, sql2,row2,col2):
        self.dataequal_notcheckrow(sql1,row1,col1, sql2,row2,col2)
        sql1_row = self.tdSql.query(sql1).row_count
        sql2_row = self.tdSql.query(sql2).row_count
        self.value_check(sql1_row,sql2_row,sql1,sql2)
        
        sql2 = sql2.replace(";","")
        nest_sq2 = "select * from (%s) " %sql2
        nest_sql2_row = self.tdSql.query(nest_sq2).row_count
        self.value_check(sql1_row,nest_sql2_row,sql1,nest_sq2)
        
        nest_nest_sq2 = nest_sq2.replace(";","")
        nest_nest_sq2 = "select * from (%s) " %nest_nest_sq2
        nest_nest_sql2_row = self.tdSql.query(nest_nest_sq2).row_count
        self.value_check(sql1_row,nest_nest_sql2_row,sql1,nest_nest_sq2)
        

    def dataequal_hyperloglog(self, sql1,row1,col1, sql2,row2,col2):
        #hyperloglog函数结果允许误差，因此放大误差数值
        self.sql1 = sql1
        list1 =[]
        self.tdSql.query(sql1)
        for i1 in range(row1):
            for j1 in range(col1):
                list1.append(self.tdSql.getData(i1,j1))
        
        self.tdSql.execute("reset query cache;") #TD=16766
        self.sql2 = sql2  
        list2 =[]
        self.tdSql.query(sql2)
        for i2 in range(row2):
            for j2 in range(col2):
                list2.append(self.tdSql.getData(i2,j2))
       
        if  (list1 == list2) and len(list2)>0:
            #self.logger.info(("=====list_hyperlog===sql1.list1:'%s',sql2.list2:'%s'") %(list1,list2))
            self.logger.info(("===list=_hyperlog===sql1:'%s' result = sql2:'%s' result") %(sql1,sql2))
        elif abs(float(str(list1).replace("]","").replace("[","")) - float(str(list2).replace("]","").replace("[",""))) < 10:
            #self.logger.info(("=====list_abs_hyperlog===sql1.list1:'%s',sql2.list2:'%s'") %(list1,list2))
            self.logger.info(("===list_abs_hyperlog===sql1:'%s' result = sql2:'%s' result") %(sql1,sql2))
        else:
            self.logger.info(("sql1:'%s' hyperlog result != sql2:'%s' result") %(sql1,sql2))
            #self.logger.info(("=====list_error_hyperlog===sql1.list1:'%s',sql2.list2:'%s'") %(list1,list2))
            return self.tdSql.checkEqual(list1,list2)
         
    def data_matrix_equal(self, sql1,row1_s,row1_e,col1_s,col1_e, sql2,row2_s,row2_e,col2_s,col2_e):
        #  ----row1_start----col1_start----
        #  - - - - 是一个矩阵内的数据相等- - - 
        #  - - - - - - - - - - - - - - - - 
        #  ----row1_end------col1_end------
        self.sql1 = sql1
        list1 =[]
        self.tdSql.query(sql1)
        for i1 in range(row1_s-1,row1_e):
            #self.logger.info("iiii=%d"%i1)
            for j1 in range(col1_s-1,col1_e):
                #self.logger.info("jjjj=%d"%j1)
                #self.logger.info("data=%s" %(self.tdSql.getData(i1,j1)))
                list1.append(self.tdSql.getData(i1,j1))
            #self.logger.info("=====list1-------list1---=%s" %set(list1))
        
        self.tdSql.execute("reset query cache;") #TD=16766
        self.sql2 = sql2  
        list2 =[]
        self.tdSql.query(sql2)
        for i2 in range(row2_s-1,row2_e):
            #self.logger.info("iiii222=%d"%i2)
            for j2 in range(col2_s-1,col2_e):
                #self.logger.info("jjjj222=%d"%j2)
                #self.logger.info("data=%s" %(self.tdSql.getData(i2,j2)))
                list2.append(self.tdSql.getData(i2,j2))
            #self.logger.info("=====list2-------list2---=%s" %set(list2)) 
       
        if  (list1 == list2) and len(list2)>0:
            # self.logger.info(("=====matrix===sql1.list1:'%s',sql2.list2:'%s'") %(list1,list2))
            self.logger.info(("===matrix===sql1:'%s' matrix_result = sql2:'%s' matrix_result") %(sql1,sql2))
        elif (set(list2)).issubset(set(list1)):
            # 解决不同子表排列结果乱序
            # self.logger.info(("=====list_issubset==matrix2in1-true===sql1.list1:'%s',sql2.list2:'%s'") %(list1,list2))
            self.logger.info(("===matrix_issubset===sql1:'%s' matrix_set_result = sql2:'%s' matrix_set_result") %(sql1,sql2))
        elif  (list1.sort() == list2.sort()):
            self.logger.info(("===matrix=sort_===sql1:'%s' result = sql2:'%s' result") %(sql1,sql2))
        elif abs(float(str(list1).replace("datetime.datetime","").replace("]","").replace("[","").replace("e+","").replace(", ","").replace("(","").replace(")","").replace("-","").replace("None","")) - float(str(list2).replace("datetime.datetime","").replace("]","").replace("[","").replace("e+","").replace(", ","").replace("(","").replace(")","").replace("-","").replace("None",""))) <= 0.0001:
            #self.logger.info(("=====matrix_abs+e+===sql1.list1:'%s',sql2.list2:'%s'") %(list1,list2))
            #self.logger.info(("=====matrix_abs+e+replace_after===sql1.list1:'%s',sql2.list2:'%s'") %(float(str(list1).replace("datetime.datetime","").replace("]","").replace("[","").replace("e+","").replace(", ","").replace("(","").replace(")","").replace("-","").replace("None","")),float(str(list2).replace("datetime.datetime","").replace("]","").replace("[","").replace("e+","").replace(", ","").replace("(","").replace(")","").replace("-","").replace("None",""))))
            self.logger.info(("===matrix_abs+e+===sql1:'%s' matrix_result = sql2:'%s' matrix_result") %(sql1,sql2))
        elif abs(float(str(list1).replace("datetime.datetime","").replace("]","").replace("[","").replace(", ","").replace("(","").replace(")","").replace("-","").replace("None","")) - float(str(list2).replace("datetime.datetime","").replace("]","").replace("[","").replace(", ","").replace("(","").replace(")","").replace("-","").replace("None",""))) <= 0.1:
            #{datetime.datetime(2021, 8, 27, 1, 46, 40), -441.46841430664057}replace
            #self.logger.info(("=====matrix_abs+replace===sql1.list1:'%s',sql2.list2:'%s'") %(list1,list2))
            #self.logger.info(("=====matrix_abs+replace_after===sql1.list1:'%s',sql2.list2:'%s'") %(float(str(list1).replace("datetime.datetime","").replace("]","").replace("[","").replace(", ","").replace("(","").replace(")","").replace("-","").replace("None","")),float(str(list1).replace("datetime.datetime","").replace("]","").replace("[","").replace(", ","").replace("(","").replace(")","").replace("-","").replace("None",""))))
            self.logger.info(("===matrix_abs+replace===sql1:'%s' matrix_result = sql2:'%s' matrix_result") %(sql1,sql2))
        elif abs(float(str(list1).replace("datetime.datetime","").replace("]","").replace("[","").replace(", ","").replace("(","").replace(")","").replace("-","").replace("None","")) - float(str(list2).replace("datetime.datetime","").replace("]","").replace("[","").replace(", ","").replace("(","").replace(")","").replace("-","").replace("None",""))) <= 0.5:
            #self.logger.info(("=====matrix_abs===sql1.list1:'%s',sql2.list2:'%s'") %(list1,list2))
            #self.logger.info(("=====matrix_abs===sql1.list1:'%s',sql2.list2:'%s'") %(float(str(list1).replace("datetime.datetime","").replace("]","").replace("[","").replace(", ","").replace("(","").replace(")","").replace("-","").replace("None","")),float(str(list2).replace("datetime.datetime","").replace("]","").replace("[","").replace(", ","").replace("(","").replace(")","").replace("-","").replace("None",""))))
            self.logger.info(("===matrix_abs======sql1:'%s' matrix_result = sql2:'%s' matrix_result") %(sql1,sql2))
        else:
            self.logger.info(("sql1:'%s' matrix_result != sql2:'%s' matrix_result") %(sql1,sql2))
            #self.logger.info(("=====matrix_error===sql1.list1:'%s',sql2.list2:'%s'") %(list1,list2))
            return self.tdSql.checkEqual(list1,list2)
                          
    def data2in1(self, sql1,row1_s,row1_e,col1_s,col1_e, sql2,row2_s,row2_e,col2_s,col2_e):
        #  ----row1_start----col1_start----
        #  - - - - - - - - - - - - - - - - 
        #  - - - - - - - - - - - - - - - - 
        #  ----row1_end------col1_end------
        self.sql1 = sql1
        list1 =[]
        self.tdSql.query(sql1)
        for i1 in range(row1_s-1,row1_e):
            for j1 in range(col1_s-1,col1_e):
                list1.append(self.tdSql.getData(i1,j1))
        #self.logger.info("-----list1-------list1---=%s" %list1) 

        self.tdSql.execute("reset query cache;") 
        self.sql2 = sql2  
        list2 =[]
        self.tdSql.query(sql2)
        for i2 in range(row2_s-1,row2_e):
            for j2 in range(col2_s-1,col2_e):                
                list2.append(self.tdSql.getData(i2,j2))
        #self.logger.info("-----list2-------list2---=%s" %list2)                
        
        if len(list2) == 0 :
            self.logger.info(("=====data2in1-0===sql1.list1:'%s',sql2.list2:'%s'") %(list1,list2))           
            self.result_0(sql2)
        #测试是否 set(list2) 中的每一个元素都在 set(list1) 中 's <= t ' == ' s.issubset(t)'   ' s.issuperset(t) ' == ' s >= t '
        elif (set(list2)).issubset(set(list1)) :
            #self.logger.info(("=====data2in1-true===sql1.list1:'%s',sql2.list2:'%s'") %(list1,list2))
            self.logger.info(("===data2in1-true-num===sql1:'%s' result include sql2:'%s' result") %(sql1,sql2))
        elif (set(list1)).issubset(set(list2)) :
            self.logger.info(("===data2in1-true-str===sql1:'%s' result include sql2:'%s' result") %(sql1,sql2))
        else:
            #self.logger.info(("=====data2in1-false===sql1.list1:'%s',sql2.list2:'%s'") %(list1,list2))
            #self.logger.info(("\n\n\n=====data2in1-list1-list2===sql1.list2 in list1:'%s'") %(set(list1)-set(list2)))
            #self.logger.info(("\n\n\n=====data2in1-list2-list1===sql2.list2 not in list1:'%s'") %(set(list2)-set(list1)))
            self.logger.info(("sql1:'%s' result not include sql2:'%s' row:'%s' col'%s' result '%s'") %(sql1,sql2,i2,j2,self.tdSql.getData(i2,j2)))
            #return self.tdSql.checkEqual(list1,self.tdSql.getData(i2,j2))
            return self.tdSql.checkEqual(list1,list2)

    
    def ignore_error_check(self,service_host,db,sql1,sql2):            
        rows = -1;
        case_common = self.case_sql_subprocess_execute(service_host,db)
        cur1 = case_common[1]
        
        try:
            (self.tdSql.query(sql1,queryTimes=1)) and (self.tdSql.query(sql2,queryTimes=1))           
            rows = self.tdSql.query(sql1).row_count 
            rows2 = self.tdSql.query(sql2).row_count  
            if rows>=0 and rows2>=0:
                rows_1 = rows 
                rows_2 = self.tdSql.query(sql2).row_count 
                if (rows_1 == 0) and (rows_2 == 0):
                    self.logger.info(("=====sql1.rows:'%s',=====sql2.rows:'%s'") %(rows_1,rows_2))
                    self.explain_sql(sql2) 
                    cur1.execute(sql2)           
                elif (rows_1 > 0 and rows_1 <= 10) and (rows_2 > 0 and rows_2 <= 10):
                    self.logger.info(("=====sql1.rows:'%s',=====sql2.rows:'%s'") %(rows_1,rows_2))
                    self.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                    #self.data_matrix_equal('%s' %sql1 ,1,int('%d' %rows_1),1,1,'%s' %sql2 ,1,int('%d' %rows_2),1,1)
                    for i in range(rows_1):
                        self.dataequal('%s' %sql1 , i ,1,'%s' %sql2 , i ,1)
                    self.explain_sql(sql2)    
                    cur1.execute(sql2)      
                elif (rows_1 > 10 ) and (rows_2 > 10 ) and (rows_1 == rows_2) :
                    self.logger.info(("=====sql1.rows:'%s',=====sql2.rows:'%s'") %(rows_1,rows_2))
                    #self.data_matrix_equal('%s' %sql1 ,1,10,1,1,'%s' %sql2 ,1,10,1,1)
                    for i in range(11):
                        self.dataequal('%s' %sql1 , i ,1,'%s' %sql2 , i ,1)
                    self.explain_sql(sql2)
                    cur1.execute(sql2)
        except:
            # (self.tdSql.error(sql1)) or (self.tdSql.query(sql1,queryTimes=1))
            # (self.tdSql.error(sql2)) or (self.tdSql.query(sql2,queryTimes=1))
            self.logger.info("sql1 is not support :=====%s; sql2 is not support :=====%s; " %(sql1,sql2))

    def check_one_row_one_col_value(self, sql, row, col, oper, value, throw=True) -> bool:
        # oper : LT (小于)、GT（大于）、LE（小于等于）、GE（大于等于）、NE（不等于）、EQ（等于）。不区分大小写 val : 数值型
        # 检查某行（row）某列（col）的值和value比对
        self.sql = sql
        self.tdSql.query(sql)
        self.value = value
        data = self.tdSql.getData(row, col)
                   
        if oper == "EQ":
            if data == None:
                self.logger.debug(f"EQ（等于）!!!）elm={data} checkEqual success")                 
                return True 
            elif operator.eq(data,value):  
                self.logger.debug(f"EQ（等于）checkEqual success, elm={data} expect_elm={value}")                 
                return True 
            else:
                if throw:
                    raise AssertionError(f"EQ（等于）checkEqual error, elm={data} expect_elm={value}")
                else:
                    self._set_error_msg(f"EQ（等于）checkEqual error, elm={data} expect_elm={value}")
                    return False
        
        elif oper == "NE":
            if data == None:
                self.logger.debug(f"NE（不等于!!!）elm={data} checkEqual success")                 
                return True 
            elif operator.ne(data,value):  
                self.logger.debug(f"NE（不等于）checkEqual success, elm={data} expect_elm={value}")                 
                return True 
            else:
                if throw:
                    raise AssertionError(f"NE（不等于）checkEqual error, elm={data} expect_elm={value}")
                else:
                    self._set_error_msg(f"NE（不等于）checkEqual error, elm={data} expect_elm={value}")
                    return False
                
        elif oper == "GT":
            if data == None:
                self.logger.debug(f"GT（大于!!!）elm={data} checkEqual success")                 
                return True 
            elif operator.gt(data,value):  
                self.logger.debug(f"GT（大于）checkEqual success, elm={data} expect_elm={value}")                 
                return True 
            else:
                if throw:
                    raise AssertionError(f"GT（大于）checkEqual error, elm={data} expect_elm={value}")
                else:
                    self._set_error_msg(f"GT（大于）checkEqual error, elm={data} expect_elm={value}")
                    return False
                
        elif oper == "LT":
            if data == None:
                self.logger.debug(f"LT (小于!!!）elm={data} checkEqual success")                 
                return True 
            elif operator.lt(data,value):  
                self.logger.debug(f"LT (小于) checkEqual success, elm={data} expect_elm={value}")                 
                return True 
            else:
                if throw:
                    raise AssertionError(f"LT (小于) checkEqual error, elm={data} expect_elm={value}")
                else:
                    self._set_error_msg(f"LT (小于) checkEqual error, elm={data} expect_elm={value}")
                    return False
                
        elif oper == "LE":
            if data == None:
                self.logger.debug(f"LE（小于等于!!!）elm={data} checkEqual success")                 
                return True 
            elif operator.le(data,value):  
                self.logger.debug(f"LE（小于等于）checkEqual success, elm={data} expect_elm={value}")                 
                return True 
            else:
                if throw:
                    raise AssertionError(f"LE（小于等于）checkEqual error, elm={data} expect_elm={value}")
                else:
                    self._set_error_msg(f"LE（小于等于）checkEqual error, elm={data} expect_elm={value}")
                    return False
                
        elif oper == "GE":                         
            if data == None:
                self.logger.debug(f"GE（大于等于!!!）elm={data} checkEqual success")                 
                return True 
            elif operator.ge(data,value):  
                self.logger.debug(f"GE（大于等于）checkEqual success, elm={data} expect_elm={value}")                 
                return True 
            else:
                if throw:
                    raise AssertionError(f"GE（大于等于）checkEqual error, elm={data} expect_elm={value}")
                else:
                    self._set_error_msg(f"GE（大于等于）checkEqual error, elm={data} expect_elm={value}")
                    return False

                
    def check_mult_rows_one_col_value(self, sql, row1, row2, col, oper, value, throw=True) -> bool:
        # oper : LT (小于)、GT（大于）、LE（小于等于）、GE（大于等于）、NE（不等于）、EQ（等于）。不区分大小写 val : 数值型
        # 检查多行（row1--row2）某列（col）的值和value比对
        self.sql = sql
        self.oper = oper
        self.value = value
        
        for i in range(row1, row2):
            self.logger.info("===row: %d col: %d=====data_d=%s"%(i,col,self.tdSql.getData(i, col)))
            self.check_one_row_one_col_value(sql, i , col, oper, value)
                                            

    def check_one_row_one_col_str_value(self, sql, row, col, oper, value, throw=True) -> bool:
        # oper : UPPER (全大写字母)、LOWER（全小写字母）、RTRIM（清除右边空格）、LTRIM（清除左边空格）、CONCAT（字符串连接）、CONCAT_WS（带分隔符字符串连接）。
        # 检查某行（row）某列（col）的值和value比对
        self.sql = sql
        self.tdSql.query(sql)
        self.value = value
        data = self.tdSql.getData(row, col)
                   
        if oper == "UPPER":
            if data == value:  
                self.logger.debug(f"UPPER（全大写字母）checkEqual success, elm={data} expect_elm={value}")                 
                return True 
            else:
                if throw:
                    raise AssertionError(f"UPPER（全大写字母）checkEqual error, elm={data} expect_elm={value}")
                else:
                    self._set_error_msg(f"UPPER（全大写字母）checkEqual error, elm={data} expect_elm={value}")
                    return False       
        elif oper == "LOWER":
            if data == value:  
                self.logger.debug(f"LOWER（全小写字母）checkEqual success, elm={data} expect_elm={value}")                 
                return True 
            else:
                if throw:
                    raise AssertionError(f"LOWER（全小写字母）checkEqual error, elm={data} expect_elm={value}")
                else:
                    self._set_error_msg(f"LOWER（全小写字母）checkEqual error, elm={data} expect_elm={value}")
                    return False
        elif oper == "LTRIM":
            if data == value:  
                self.logger.debug(f"LTRIM（清除左边空格）checkEqual success, elm={data} expect_elm={value}")                 
                return True 
            else:
                if throw:
                    raise AssertionError(f"LTRIM（清除左边空格）checkEqual error, elm={data} expect_elm={value}")
                else:
                    self._set_error_msg(f"LTRIM（清除左边空格）checkEqual error, elm={data} expect_elm={value}")
                    return False                
        elif oper == "RTRIM":
            if data == value:  
                self.logger.debug(f"RTRIM（清除右边空格）checkEqual success, elm={data} expect_elm={value}")                 
                return True 
            else:
                if throw:
                    raise AssertionError(f"RTRIM（清除右边空格）checkEqual error, elm={data} expect_elm={value}")
                else:
                    self._set_error_msg(f"RTRIM（清除右边空格）checkEqual error, elm={data} expect_elm={value}")
                    return False              
        elif oper == "CONCAT":
            if data == value:  
                self.logger.debug(f"CONCAT（字符串连接）checkEqual success, elm={data} expect_elm={value}")                 
                return True 
            else:
                if throw:
                    raise AssertionError(f"CONCAT（字符串连接）checkEqual error, elm={data} expect_elm={value}")
                else:
                    self._set_error_msg(f"CONCAT（字符串连接）checkEqual error, elm={data} expect_elm={value}")
                    return False   
        elif oper == "CONCAT_WS":
            if data == value:  
                self.logger.debug(f"CONCAT_WS（带分隔符字符串连接）checkEqual success, elm={data} expect_elm={value}")                 
                return True 
            else:
                if throw:
                    raise AssertionError(f"CONCAT_WS（带分隔符字符串连接）checkEqual error, elm={data} expect_elm={value}")
                else:
                    self._set_error_msg(f"CONCAT_WS（带分隔符字符串连接）checkEqual error, elm={data} expect_elm={value}")
                    return False                                   
                                                                
    def check_mult_rows_one_col_str_value(self, sql, row1, row2, col, oper, value, throw=True) -> bool:
        #  oper : UPPER (全大写字母)、LOWER（全小写字母）、RTRIM（清除右边空格）、LTRIM（清除左边空格）。
        # 检查多行（row1--row2）某列（col）的值和value比对
        self.sql = sql
        self.oper = oper
        self.value = value
        
        for i in range(row1, row2):
            self.logger.info("===row: %d col: %d=====data_s=%s"%(i,col,self.tdSql.getData(i, col)))
            self.check_one_row_one_col_str_value(sql, i , col, oper, value)



    def check_one_row_one_col_time_value(self, sql, row, col, oper, value, throw=True) -> bool:
        # oper : TIME、SYS_TIME (时间对比)、TODAY、SYS_TODAY（时间对比）、TIMEZONE、SYS_TIMEZONE（时间对比）
        # oper : TO_ISO8601、SYS_TO_ISO8601 (时间对比)、TO_UNIXTIMESTAMP、SYS_TO_UNIXTIMESTAMP（时间对比）、TIMEZONE、SYS_TIMEZONE（时间对比）
        # 检查某行（row）某列（col）的值和value比对
        self.sql = sql
        self.tdSql.query(sql)
        self.value = value
        data = self.tdSql.getData(row, col)
                   
        if oper == "TIME":
            #self.logger.info(f"{data},{value}")
            self.logger.info(f"pd.to_datetime({data}) , pd.to_datetime({value})")
            chazhi = pd.to_datetime(data) - pd.to_datetime(value)
            # self.logger.info(chazhi)
            # self.logger.info(chazhi.total_seconds())
            
            if pd.to_datetime(data) == pd.to_datetime(value):  
                self.logger.debug(f"TIME（时间对比=）checkEqual success, elm={data} expect_elm={value}")                 
                return True 
            #做了差值比对，控制在10s之内
            elif (float(chazhi.total_seconds())<10):
                self.logger.debug(f"TIME（时间对比-）checkEqual success, elm={data} expect_elm={value}")                 
                return True
            else:
                if throw:
                    raise AssertionError(f"TIME（时间对比）checkEqual error, elm={data} expect_elm={value}")
                else:
                    self._set_error_msg(f"TIME（时间对比）checkEqual error, elm={data} expect_elm={value}")
                    return False    
        elif oper == "SYS_TIME":
            self.logger.info(f"{data},{value}")
            self.logger.info(f"pd.to_datetime({data}) , pd.to_datetime({value})")
            chazhi = pd.to_datetime(data) - pd.to_datetime(value)
            self.logger.info(chazhi)
            
            if pd.to_datetime(data) == pd.to_datetime(value):  
                self.logger.debug(f"SYS_TIME（时间对比=）checkEqual success, elm={data} expect_elm={value}")                 
                return True 
            #做了差值比对，控制在5s之内
            elif (float(chazhi.total_seconds())<10):
                self.logger.debug(f"SYS_TIME（时间对比-）checkEqual success, elm={data} expect_elm={value}")                 
                return True
            else:
                if throw:
                    raise AssertionError(f"SYS_TIME（时间对比）checkEqual error, elm={data} expect_elm={value}")
                else:
                    self._set_error_msg(f"SYS_TIME（时间对比）checkEqual error, elm={data} expect_elm={value}")
                    return False  
                                
        elif oper == "TODAY":
            #self.logger.info(f"{data},{value}")
            self.logger.info(f"pd.to_datetime({data}),pd.to_datetime({value})")
            chazhi = pd.to_datetime(data) - pd.to_datetime(value)
            # self.logger.info(chazhi)
            # self.logger.info(chazhi.total_seconds())
            
            if pd.to_datetime(data) == pd.to_datetime(value):  
                self.logger.debug(f"TODAY（时间对比=）checkEqual success, elm={data} expect_elm={value}")                 
                return True 
            #做了差值比对，控制在1天86400s+30s之内
            elif (abs(float(chazhi.total_seconds()))<86430):
                self.logger.debug(f"TODAY（时间对比-）checkEqual success, elm={data} expect_elm={value}")                 
                return True
            else:
                if throw:
                    raise AssertionError(f"TODAY（时间对比）checkEqual error, elm={data} expect_elm={value}")
                else:
                    self._set_error_msg(f"TODAY（时间对比）checkEqual error, elm={data} expect_elm={value}")
                    return False    
        elif oper == "SYS_TODAY":
            self.logger.info(f"pd.to_datetime({data}) , pd.to_datetime({value})")
            chazhi = pd.to_datetime(data) - pd.to_datetime(value)
            self.logger.info(f"chazhi,abs(float(chazhi.total_seconds()))")
            
            if pd.to_datetime(data) == pd.to_datetime(value):  
                self.logger.debug(f"SYS_TODAY（时间对比=）checkEqual success, elm={data} expect_elm={value}")                 
                return True 
            #做了差值比对，因为TODAY取的是当前时间，精确到秒，而不是天，所以控制在1天86400s+30s（sql运行时间）之内
            elif (abs(float(chazhi.total_seconds()))<=86430):
                self.logger.debug(f"SYS_TODAY（时间对比-）checkEqual success, elm={data} expect_elm={value}")                 
                return True
            else:
                if throw:
                    raise AssertionError(f"SYS_TODAY（时间对比）checkEqual error, elm={data} expect_elm={value}")
                else:
                    self._set_error_msg(f"SYS_TODAY（时间对比）checkEqual error, elm={data} expect_elm={value}")
                    return False   
                  
        elif oper == "TIMEZONE":
            #self.logger.info(f"{data},{value}")            
            if data == value:  
                self.logger.debug(f"TIMEZONE（时间对比=）checkEqual success, elm={data} expect_elm={value}")                 
                return True 
            else:
                if throw:
                    raise AssertionError(f"TIMEZONE（时间对比）checkEqual error, elm={data} expect_elm={value}")
                else:
                    self._set_error_msg(f"TIMEZONE（时间对比）checkEqual error, elm={data} expect_elm={value}")
                    return False    
        elif oper == "SYS_TIMEZONE":
            self.logger.info(f"{data},{value}")            
            if str(data).split()[0] == str(value):  
                self.logger.debug(f"SYS_TIMEZONE（时间对比=）checkEqual success, elm={data} expect_elm={value}")                 
                return True 
            else:
                if throw:
                    raise AssertionError(f"SYS_TIMEZONE（时间对比）checkEqual error, elm={data} expect_elm={value}")
                else:
                    self._set_error_msg(f"SYS_TIMEZONE（时间对比）checkEqual error, elm={data} expect_elm={value}")
                    return False                  
                  
        elif oper == "TO_ISO8601":
            #处理data格式，只保留+之前的，eg：2022-03-30T15:11:36.432+0800 保留2022-03-30T15:11:36.432
            self.logger.info(f"{data},{value}")
            data = str(data).split("+")[0]
            value = str(value).split("+")[0]
            chazhi =(datetime.datetime.strptime(data, "%Y-%m-%dT%H:%M:%S.%f") - datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f")).total_seconds()            
            self.logger.info(datetime.datetime.strptime(data, "%Y-%m-%dT%H:%M:%S.%f"))
            self.logger.info(datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f"))
            self.logger.info(f"{chazhi},float({chazhi})")
            
            if str(data).split(".")[0] == str(value).split(".")[0]:  
                self.logger.debug(f"TO_ISO8601（时间对比=.）checkEqual success, elm={data} expect_elm={value}")                 
                return True                 
            elif float(chazhi)<28900:  #有的机器有8个小时时差，因此增加8个小时28800s+100s
                self.logger.debug(f"TO_ISO8601（时间对比差值=:）checkEqual success, elm={data} expect_elm={value}")                 
                return True 
            else:
                if throw:
                    raise AssertionError(f"TO_ISO8601（时间对比）checkEqual error, elm={data} expect_elm={value}")
                else:
                    self._set_error_msg(f"TO_ISO8601（时间对比）checkEqual error, elm={data} expect_elm={value}")
                    return False    
        elif oper == "SYS_TO_ISO8601":
            self.logger.info(f"{data},{value}")
            data = str(data).split("+")[0]
            value = str(value).split("+")[0]
            chazhi =(datetime.datetime.strptime(data, "%Y-%m-%dT%H:%M:%S.%f") - datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f")).total_seconds()                                    
            #有的机器有8个小时时差，因此增加8个小时28800s+100s
            if float(chazhi) < 28900:  
                self.logger.debug(f"SYS_TO_ISO8601（时间对比差值=:）checkEqual success, elm={data} expect_elm={value}")                 
                return True 
            else:
                if throw:
                    raise AssertionError(f"SYS_TO_ISO8601（时间对比）checkEqual error, elm={data} expect_elm={value}")
                else:
                    self._set_error_msg(f"SYS_TO_ISO8601（时间对比）checkEqual error, elm={data} expect_elm={value}")
                    return False                     
                  
        elif oper == "TO_UNIXTIMESTAMP":
            #处理data格式，只保留+之前的，eg：2022-03-30T15:11:36.432+0800 保留2022-03-30T15:11:36.432
            self.logger.info(f"{data},{value}")
            data = str(data).split("+")[0]
            value = str(value).split("+")[0]
            if data == value:  
                self.logger.debug(f"TO_UNIXTIMESTAMP（时间对比=.）checkEqual success, elm={data} expect_elm={value}")                 
                return True                 
            else:
                if throw:
                    raise AssertionError(f"TO_UNIXTIMESTAMP（时间对比）checkEqual error, elm={data} expect_elm={value}")
                else:
                    self._set_error_msg(f"TO_UNIXTIMESTAMP（时间对比）checkEqual error, elm={data} expect_elm={value}")
                    return False    
        elif oper == "SYS_TO_UNIXTIMESTAMP":
            self.logger.info(f"{data},{value}")
            data = str(data).split("+")[0]
            value = str(value).split("+")[0]                                    
            #有的机器有8个小时时差，因此增加8个小时28800s
            if data == value:  
                self.logger.debug(f"SYS_TO_UNIXTIMESTAMP（时间对比=.）checkEqual success, elm={data} expect_elm={value}")                 
                return True                 
            #elif float(datetime.datetime.strptime(data, "%Y-%m-%dT%H:%M:%S.%f") - datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f")).total_seconds() < 28805:  
            elif float((data) - datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f")).total_seconds() < 28805:  
                self.logger.debug(f"SYS_TO_UNIXTIMESTAMP（时间对比差值=:）checkEqual success, elm={data} expect_elm={value}")                 
                return True 
            else:
                if throw:
                    raise AssertionError(f"SYS_TO_UNIXTIMESTAMP（时间对比）checkEqual error, elm={data} expect_elm={value}")
                else:
                    self._set_error_msg(f"SYS_TO_UNIXTIMESTAMP（时间对比）checkEqual error, elm={data} expect_elm={value}")
                    return False                     
                                                                                                   
    def check_mult_rows_one_col_time_value(self, sql, row1, row2, col, oper, value, throw=True) -> bool:
        #  oper : UPPER (全大写字母)、LOWER（全小写字母）。
        # 检查多行（row1--row2）某列（col）的值和value比对
        self.sql = sql
        self.oper = oper
        self.value = value
        
        for i in range(row1, row2):
            self.logger.info("===row: %d col: %d=====data_s=%s"%(i,col,self.tdSql.getData(i, col)))
            self.check_one_row_one_col_time_value(sql, i , col, oper, value)
