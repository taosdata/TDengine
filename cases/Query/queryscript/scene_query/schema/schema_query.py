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
import sys
from itertools import combinations
from faker import Faker
import subprocess
from taostest import TDCase
from Query.queryutil.createdata import *
import threading
import multiprocessing

class TDTestQuery(TDCase):
    def init(self):
        super(TDTestQuery, self).init()
        self.tdCreateData = TDCreateData(self.tdSql, self.logger)
        
        #basic_param
        self.db_tb = "`information_schema`.`ins_dnodes`"
        
        self.testcasePath = os.path.split(__file__)[0]
        self.testcaseFilename = os.path.split(__file__)[-1]
        
        self.firstEP = []
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.firstEP.append(
                    self.taosd_setting['spec']['config']['firstEP'])
        self.target_taosd = self.firstEP[-1].split(':')
        print(self.target_taosd[0])
        self.service_host = self.target_taosd[0]

    def tags(self) :
	
        return ""

    def author(self) -> str:

        return "Guo Xiangyang"

    def desc(self) -> str:
        case_description = '''
        case1:# schema all query
        '''
        return case_description
    
    def random_column_tag(self,db_tb):
        self.tdSql.query("alter local 'schedulePolicy' '%d';" %random.randint(1,3))
        describe_sql = "describe %s;" %db_tb
        self.tdSql.query(describe_sql)  
        rows = self.tdSql.query_row
        column_tag_list=[]
        for i in range(1,random.randint(0,rows)):
            column_tag_list.append(self.tdSql.getData(i,0))
        column_tag_list = str(column_tag_list).replace("[","").replace("]","").replace("'","`")
        
        return column_tag_list
    
    def random_column_tag_where(self,db_tb):
        self.tdSql.query("alter local 'schedulePolicy' '%d';" %random.randint(1,3))
        fake = Faker('zh_CN')
        random_int = random.randint(-100,10000000)
        random_float = fake.pyfloat()
        print(random_float)
        
        describe_sql = "describe %s;" %db_tb
        self.tdSql.query(describe_sql)  
        rows = self.tdSql.query_row
        column_tag_list_where=[]
        
        column_ts,column_bool,column_int,column_float,column_char = '','','','',''
        
        tinyint_list=[]
        for num in range(-random.randint(0,500),random.randint(0,500)):
            tinyint_list.append(num)
        
        i = random.randint(1,rows)
        column_type = self.tdSql.getData(i - 1,1)        
        column_field = self.tdSql.getData(i - 1,0)
        
        i1 = random.randint(1,rows)
        column_type_1 = self.tdSql.getData(i1 - 1,1) 
        column_field_1 = self.tdSql.getData(i1 - 1,0)
        
        column_null = '(`%s`' %self.tdSql.getData(random.randint(1,rows) - 1,0)  + ' is null or ' +  '`%s`' %self.tdSql.getData(random.randint(1,rows) - 1,0) + ' is not null ) and '
        
        if column_type == 'TIMESTAMP'  and column_type_1 == 'TIMESTAMP':
            column_ts = '`%s`' %column_field + ' < now and '
        elif column_type_1 == 'TIMESTAMP'  :
            column_ts = '`%s`' %column_field_1 + ' between 0 and now and '
        elif column_type == 'TIMESTAMP'  :
            column_ts =  ' _rowts between 0 and now and '
        
        if column_type  == 'INT' :
            column_int = '`%s`' %column_field + ' <= %d and ' %random_int
        elif column_type  == 'SMALLINT':
            column_int = '`%s`' %column_field + ' > %d and ' %random_int
        elif column_type  == 'BIGINT' :
            column_int = '`%s`' %column_field + ' between %d and  %d and ' %(random_float,random_int)
        elif column_type  == 'TINYINT':
            column_int = '`%s`' %column_field + ' != %d and ' %random_int
        elif column_type_1  == 'TINYINT':
            column_int = '`%s`' %column_field_1 + ' in ( ' + str(tinyint_list).replace(",","|").replace("[","").replace("]","") + ") and "
            
        if column_type  == 'FLOAT':
            column_float = '`%s`' %column_field + ' >= %d and ' %random_float
        elif column_type  == 'DOUBLE' :
            column_float = '`%s`' %column_field + ' between %d and  %d and ' %(random_float,random_int)
        elif column_type_1  == 'FLOAT':
            column_float = '`%s`' %column_field_1 + ' < %d and ' %random_float
        elif column_type_1  == 'DOUBLE' :
            column_float = '`%s`' %column_field_1 + ' != %d and ' %(random_float)
        
        if column_type  == 'BOOL' and column_type_1  == 'BOOL':
            column_bool = '`%s`' %column_field + ' = true and ' 
        elif column_type_1  == 'BOOL':
            column_bool = '`%s`' %column_field_1 + ' = false and '
        elif column_type  == 'BOOL' :
            column_bool = '`%s`' %column_field + ' in (0 | 1)  and '    # | replace ,
        
        if column_type == 'VARCHAR' and column_type_1 == 'VARCHAR':
            column_char = '`%s`' %column_field + ' in ( ' + str(tinyint_list).replace(",","|").replace("[","").replace("]","") + ") and "
        elif column_type == 'VARCHAR' :
            column_char = '`%s`' %column_field + ' match\'{a-zA-Z}\' and '
        elif column_type == 'NCHAR' :
            column_char = '`%s`' %column_field + ' nmatch\'{a-z}\' and '
        elif column_type_1 == 'VARCHAR' :
            column_char = '`%s`' %column_field_1 + ' like \'《%》\' and '
        elif column_type_1 == 'NCHAR' :
            column_char = '`%s`' %column_field_1 + ' not like \'《%》\' and '
        
        
        column_tag_list_where.append(column_null)
        column_tag_list_where.append(column_bool)
        column_tag_list_where.append(column_ts)
        column_tag_list_where.append(column_int)
        column_tag_list_where.append(column_float)
        column_tag_list_where.append(column_char)
                
        return column_tag_list_where

    def limit_slimit(self,db_tb,num):
        limit_num = random.randint(0,1000000)
        limit_offset = random.randint(0,1000000)
        slimit_num = random.randint(0,1000000)
        slimit_offset = random.randint(0,1000000)
        
        if num == 1:
            #只返回limit,
            i = random.randint(0,4)
            if i ==1:
                limit_slimit = ' limit %d ' %limit_num 
            elif i ==2:
                limit_slimit = ' limit %d ,%d ' %(limit_num, limit_offset)  
            elif i ==3:
                limit_slimit = ' limit %d offset %d ' %(limit_num, limit_offset)
            else:
                limit_slimit = ' '  
        else:
            #返回limit+slimit，适合group by,partition by
            i = random.randint(0,17)
            if i ==1:
                limit_slimit = ' limit %d ' %limit_num 
            elif i ==2:
                limit_slimit = ' limit %d ,%d ' %(limit_num, limit_offset)  
            elif i ==3:
                limit_slimit = ' limit %d offset %d ' %(limit_num, limit_offset)
            elif i ==4:
                limit_slimit = ' slimit %d ' %slimit_num 
            elif i ==5:
                limit_slimit = ' slimit %d ,%d ' %(slimit_num, slimit_offset)  
            elif i ==6:
                limit_slimit = ' slimit %d soffset %d ' %(slimit_num, slimit_offset) 
            elif i ==7:
                limit_slimit = ' slimit %d limit %d ' %(slimit_num,limit_num) 
            elif i ==8:
                limit_slimit = ' slimit %d ,%d limit %d ' %(slimit_num, slimit_offset,limit_num)  
            elif i ==9:
                limit_slimit = ' slimit %d soffset %d limit %d ' %(slimit_num, slimit_offset,limit_num)   
            elif i ==10:
                limit_slimit = ' slimit %d limit %d,%d ' %(slimit_num,limit_num, limit_offset) 
            elif i ==11:
                limit_slimit = ' slimit %d ,%d limit %d,%d ' %(slimit_num, slimit_offset,limit_num, limit_offset)  
            elif i ==12:
                limit_slimit = ' slimit %d soffset %d limit %d,%d ' %(slimit_num, slimit_offset,limit_num, limit_offset)   
            elif i ==13:
                limit_slimit = ' slimit %d limit %d offset %d ' %(slimit_num,limit_num, limit_offset) 
            elif i ==14:
                limit_slimit = ' slimit %d ,%d limit %d offset %d ' %(slimit_num, slimit_offset,limit_num, limit_offset)  
            elif i ==15:
                limit_slimit = ' slimit %d soffset %d limit %d offset %d ' %(slimit_num, slimit_offset,limit_num, limit_offset)   
            else:
                limit_slimit = ' '   
            
        return limit_slimit
        
    def event_window_i(self,i):   
        trigger_condition = ''
        fake = Faker('zh_CN')
        data_bigint = fake.random_int(min=-1000000000, max=1000000000, step=1);
        data_int = fake.random_int(min=-1000000, max=1000000, step=1);
        data_smallint = fake.random_int(min=-1000, max=1000, step=1);
        data_tinyint = fake.random_int(min=-10, max=10, step=1);
        data_float = fake.pyfloat()/1000000;
        data_str = fake.pystr();
        
        event_window_support_data_all_types = ['q_bigint','q_smallint','q_tinyint','q_int','q_bigint_null','q_smallint_null','q_tinyint_null','q_int_null'] 
        event_window_support_float_all_types = ['q_double','q_float','q_double_null','q_float_null'] 
        
        
        event_window_support_data_types = ['q_bigint','q_smallint','q_tinyint','q_int'] 
        event_window_support_data_bigint = ['q_bigint']
        event_window_support_data_smallint = ['q_smallint']
        event_window_support_data_int = ['q_int']
        event_window_support_data_tinyint = ['q_tinyint']
        
        event_window_support_float_types = ['q_double','q_float'] 
        event_window_support_str_types = ['q_nchar','q_binary'] 
        
        event_window_support_data_operators = ['>','<','!=','>=','<='] #,'='
        
        event_window_support_is_not_null_table = [" q_bigint is not null "," q_int is not null "," q_tinyint is not null "," q_smallint is not null "," q_bool is not null ",
                                            " q_binary is not null "," q_nchar is not null "," q_double is not null "," q_float is not null "," q_ts is not null ",
                                            " _c0 is not null "," _c0 is not null ",
                                            " q_bigint_null is null "," q_int_null is null "," q_tinyint_null is null "," q_smallint_null is null "," q_bool_null is null ",
                                            " q_binary_null is null "," q_nchar_null is null "," q_double_null is null "," q_float_null is null "," q_ts_null is null ",]
        
        event_window_support_is_not_null_stable = [" q_bigint is not null "," q_int is not null "," q_tinyint is not null "," q_smallint is not null "," q_bool is not null ",
                                            " q_binary is not null "," q_nchar is not null "," q_double is not null "," q_float is not null "," q_ts is not null ",
                                            " _c0 is not null "," _c0 is not null ",
                                            " q_bigint_null is null "," q_int_null is null "," q_tinyint_null is null "," q_smallint_null is null "," q_bool_null is null ",
                                            " q_binary_null is null "," q_nchar_null is null "," q_double_null is null "," q_float_null is null "," q_ts_null is null ",
                                            " t_bigint is not null "," t_int is not null "," t_tinyint is not null "," t_smallint is not null "," t_bool is not null ",
                                            " t_binary is not null "," t_nchar is not null "," t_double is not null "," t_float is not null "," t_ts is not null ",]
        
        event_window_support_str_operators_table = ["q_binary not like STAbinary_END","q_binary like STAbinaryENND","q_nchar not like STAnchar_END","q_nchar like STAncharENND",
                                              "q_binary match STAbinaryEND","q_binary nmatch STAncharEND","q_nchar match STAncharEND","q_nchar nmatch STAbinaryEND",]
        
        event_window_support_str_operators_stable = ["q_binary not like STAbinary_END","q_binary like STAbinaryENND","q_nchar not like STAnchar_END","q_nchar like STAncharENND",
                                              "q_binary match STAbinaryEND","q_binary nmatch STAncharEND","q_nchar match STAncharEND","q_nchar nmatch STAbinaryEND",
                                              "t_binary not like STAbinary_END","t_binary like STAbinaryENND","t_nchar not like STAnchar_END","t_nchar like STAncharENND",
                                              "t_binary match STAbinaryEND","t_binary nmatch STAncharEND","t_nchar match STAncharEND","t_nchar nmatch STAbinaryEND",
                                              "loc match STA<table>END","loc match STA<^qwryuiop>END","loc nmatch STA<^>END","loc nmatch STA<qwryuiop>END",
                                              "t_binary match STA<binary>END","t_binary match STA<^爨龘>END","t_binary nmatch STA<爨龘>END","t_binary nmatch STA<^>END",
                                              "t_nchar match STA<nchar>END","t_nchar match STA<^爨龘>END","t_nchar nmatch STA<爨龘>END","t_nchar nmatch STA<^>END",
                                              "loc match STA<a-z>END","t_binary match STA<a-z>END","t_nchar match STA<a-z>END",
                                              "loc match STA<a-zA-Z>END","t_binary match STA<a-zA-Z>END","t_nchar match STA<a-zA-Z>END",
                                              "loc match STA.END","t_binary match STA.END","t_nchar match STA.END",
                                              "loc match STA.*END","t_binary match STA.*END","t_nchar match STA.*END",
                                              "loc match STAa|b|c|d|e|f|g|h|i|j|k|l|m|n|o|p|q|r|s|t|u|v|w|x|y|zEND","loc match STAa|b|c|d|e|f|g|h|i|j|k|l|m|n|o|p|q|r|s|t|u|v|w|x|y|z|A|B|C|D|E|F|G|H|I|J|K|L|M|N|O|P|Q|R|S|T|U|V|W|X|Y|ZEND",
                                              "t_binary match STAa|b|c|d|e|f|g|h|i|j|k|l|m|n|o|p|q|r|s|t|u|v|w|x|y|zEND","t_binary match STAa|b|c|d|e|f|g|h|i|j|k|l|m|n|o|p|q|r|s|t|u|v|w|x|y|z|A|B|C|D|E|F|G|H|I|J|K|L|M|N|O|P|Q|R|S|T|U|V|W|X|Y|ZEND",
                                              "t_nchar match STAa|b|c|d|e|f|g|h|i|j|k|l|m|n|o|p|q|r|s|t|u|v|w|x|y|zEND","t_nchar match STAa|b|c|d|e|f|g|h|i|j|k|l|m|n|o|p|q|r|s|t|u|v|w|x|y|z|A|B|C|D|E|F|G|H|I|J|K|L|M|N|O|P|Q|R|S|T|U|V|W|X|Y|ZEND",
                                              "( loc match STA\sEND or loc match STA\SEND )","( t_binary match STA\sEND or t_binary match STA\SEND )","( t_nchar match STA\sEND or t_nchar match STA\SEND )",
                                              "loc nmatch STA\sEND "," t_binary nmatch STA\sEND "," t_nchar nmatch STA\sEND ",
                                              "( loc match STA\wEND or loc match STA\WEND )","( t_binary match STA\wEND or t_binary match STA\WEND )","( t_nchar match STA\wEND or t_nchar match STA\WEND )",
                                              "loc match STA\wEND "," t_binary match STA\wEND "," t_nchar match STA\wEND ",]
               
        
        trigger_condition_data_common = random.sample(event_window_support_data_types,1) + random.sample(event_window_support_data_operators,1)         
        trigger_condition_data = str(trigger_condition_data_common).replace("[","").replace("]","").replace("'","").replace(", ","") + str(data_int)
        
        trigger_condition_data_bigint = random.sample(event_window_support_data_bigint,1) + random.sample(event_window_support_data_operators,1)         
        trigger_condition_data_bigint = str(trigger_condition_data_bigint).replace("[","").replace("]","").replace("'","").replace(", ","") + str(data_bigint)
        trigger_condition_data_smallint = random.sample(event_window_support_data_smallint,1) + random.sample(event_window_support_data_operators,1)         
        trigger_condition_data_smallint = str(trigger_condition_data_smallint).replace("[","").replace("]","").replace("'","").replace(", ","") + str(data_smallint)
        trigger_condition_data_int = random.sample(event_window_support_data_int,1) + random.sample(event_window_support_data_operators,1)         
        trigger_condition_data_int = str(trigger_condition_data_int).replace("[","").replace("]","").replace("'","").replace(", ","") + str(data_int)
        trigger_condition_data_tinyint = random.sample(event_window_support_data_types,1) + random.sample(event_window_support_data_operators,1)         
        trigger_condition_data_tinyint = str(trigger_condition_data_tinyint).replace("[","").replace("]","").replace("'","").replace(", ","") + str(data_tinyint)
        
        trigger_condition_float_common = random.sample(event_window_support_float_types,1) + random.sample(event_window_support_data_operators,1)
        trigger_condition_float = str(trigger_condition_float_common).replace("[","").replace("]","").replace("'","").replace(", ","") + str(data_float)
        
        trigger_condition_str_error_common = random.sample(event_window_support_str_types,1) + random.sample(event_window_support_data_operators,1)
        trigger_condition_str_error = str(trigger_condition_str_error_common).replace("[","").replace("]","").replace("'","").replace(", ","") + str(data_str)
        
        trigger_condition_str_right_table = str(random.sample(event_window_support_str_operators_table,1)).replace("[","").replace("]","").replace("(","").replace(")","").replace("'","").replace("STA","'").replace("END","'").replace("ENND","%'").replace(",","")
        trigger_condition_str_right_stable = str(random.sample(event_window_support_str_operators_stable,1)).replace("[","").replace("]","").replace("(","").replace(")","").replace("'","").replace("STA","'").replace("END","'").replace("ENND","%'").replace(",","").replace("<","[").replace(">","]")#< > repalce [正则 ]
               
        trigger_condition_is_not_null_table = str(random.sample(event_window_support_is_not_null_table,1)).replace("[","").replace("]","").replace("(","").replace(")","").replace("'","").replace(",","")
        trigger_condition_is_not_null_stable = str(random.sample(event_window_support_is_not_null_stable,1)).replace("[","").replace("]","").replace("(","").replace(")","").replace("'","").replace(",","")
              
              
        q_tinyint_list=[]
        for q_list_i in range(-300,300):
            q_tinyint_list.append(q_list_i)
        q_tinyint_list = "q_tinyint in (" + str(q_tinyint_list).replace("[","").replace("]","") + ")"
        
        t_tinyint_list=[]
        for t_list_i in range(-300,300):
            t_tinyint_list.append(t_list_i)
        t_tinyint_list = "t_tinyint in (" + str(t_tinyint_list).replace("[","").replace("]","") + ")"

        trigger_condition_in_table = [q_tinyint_list, "q_bool in (0 , 1) " ,  "q_bool in ( true , false) " ," (q_bool = true or  q_bool = false)" , "(q_bool = 0 or q_bool = 1)",]
        trigger_condition_in_table = str(random.sample(trigger_condition_in_table,1)).replace("[","").replace("]","").replace("'","")

        trigger_condition_in_stable = [t_tinyint_list, "t_bool in (0 , 1) " ,  "t_bool in ( true , false) " ," (t_bool = true or  t_bool = false)" , "(t_bool = 0 or t_bool = 1)",
                                       q_tinyint_list, "q_bool in (0 , 1) " ,  "q_bool in ( true , false) " ," (q_bool = true or  q_bool = false)" , "(q_bool = 0 or q_bool = 1)",]
        trigger_condition_in_stable = str(random.sample(trigger_condition_in_stable,1)).replace("[","").replace("]","").replace("'","")
        
        if i==1:
            trigger_condition = trigger_condition_data_bigint;
        elif i==2:
            trigger_condition = trigger_condition_data_smallint;
        elif i==3:
            trigger_condition = trigger_condition_data_int;
        elif i==4:
            trigger_condition = trigger_condition_data_tinyint;        
        elif i==5:
            trigger_condition = trigger_condition_float;
            
        elif i==6:
            trigger_condition = trigger_condition_str_right_stable;  
        elif i==61:
            trigger_condition = trigger_condition_str_error;
        elif i==7:
            trigger_condition = trigger_condition_is_not_null_stable; 
        elif i==8:
            trigger_condition = trigger_condition_in_stable; 
            
            
        elif i==11:
            trigger_condition = trigger_condition_is_not_null_table; 
        elif i==12:
            trigger_condition = trigger_condition_is_not_null_table; 
        elif i==13:
            trigger_condition = trigger_condition_in_table; 
            
        return trigger_condition   

    def time_window(self,db_tb,i):  
        #数字后面的时间单位可以是 u(微秒)、a(毫秒)、s(秒)、m(分)、h(小时)、d(天)、w(周)。 
        #在指定降频操作（down sampling）的时间窗口（interval）时，时间单位还可以使用 n(自然月) 和 y(自然年)。     
        interval_n, offset_n, sliding_n = [random.randrange(10,20)]  , [random.randrange(1,10)] , [random.randrange(1,10)] 
        time_window = ''
                
        #单interval
        interval_units = ['s','m','h','d','w','n','a','y']
        unit = random.sample(interval_units,1)
        interval_base = str(interval_n + unit).replace("[","").replace("]","").replace("'","").replace(", ","")
        single_interval = 'interval'+'(' +interval_base + ')'
        
        #单interval+offset
        offset_base = str(offset_n + unit).replace("[","").replace("]","").replace("'","").replace(", ","")
        single_interval_offset = 'interval'+'(' +interval_base + ',' + offset_base + ')'

        #interval + sliding
        interval_sliding_units = ['s','m','h','d','w'] #有限制，所以需要删除几个
        interval_sliding_unit = random.sample(interval_sliding_units,1)
        
        sliding_base = str(sliding_n + interval_sliding_unit).replace("[","").replace("]","").replace("'","").replace(", ","")
        single_sliding = 'sliding'+'(' +sliding_base + ')'

        sliding_interval_no_offset = str(interval_n + interval_sliding_unit).replace("[","").replace("]","").replace("'","").replace(", ","")
        sliding_interval = 'interval'+'(' +sliding_interval_no_offset + ')'
        
        sliding_interval_offset = str(offset_n + interval_sliding_unit).replace("[","").replace("]","").replace("'","").replace(", ","")
        sliding_interval_offset = 'interval'+'(' + sliding_interval_no_offset + ',' + sliding_interval_offset + ')'
        
        #单fill,对时间强要求
        fill_random_num = random.randint(-100000000,100000000)
        fill_random_nu2 = random.randint(-100000000,100000000)
        fill_random_nu3 = random.randint(-100000000,100000000)
        fills_all = ['NONE','VALUE,100','VALUE_F,100','PREV','NULL','NULL_F','LINEAR','NEXT','VALUE,10000','VALUE_F,10000','VALUE,fill_random_num','VALUE_F,fill_random_num',
                     'VALUE,fill_random_num + fill_random_nu2','VALUE_F,fill_random_num + fill_random_nu2','VALUE_F,fill_random_num + fill_random_nu2 + fill_random_nu3',
                     'VALUE,fill_random_num - fill_random_nu2','VALUE_F,fill_random_num - fill_random_nu2','VALUE_F,fill_random_num - fill_random_nu2 - fill_random_nu3',
                     'VALUE,fill_random_num * fill_random_nu2','VALUE_F,fill_random_num * fill_random_nu2','VALUE_F,fill_random_num * fill_random_nu2 * fill_random_nu3',
                     'VALUE,fill_random_num / fill_random_nu2','VALUE_F,fill_random_num / fill_random_nu2','VALUE_F,fill_random_num * fill_random_nu2 / fill_random_nu3']
        fill_base = str(random.sample(fills_all,1)).replace("[","").replace("]","").replace("'","").replace(", ","").replace("10000","'10000'").replace("fill_random_num",str(fill_random_num)).replace("fill_random_nu2",str(fill_random_nu2)).replace("fill_random_nu3",str(fill_random_nu3))
        single_fill = 'Fill' +'(' +fill_base + ')'
        
        #强制fill,对时间强要求
        #fill_random_num = random.randint(-1000000,1000000)
        fills_f = ['VALUE_F,fill_random_num','NULL_F','VALUE_F,10000',
                'VALUE_F,fill_random_num + fill_random_nu2','VALUE_F,fill_random_num + fill_random_nu2 + fill_random_nu3',
                'VALUE_F,fill_random_num - fill_random_nu2','VALUE_F,fill_random_num - fill_random_nu2 - fill_random_nu3',
                'VALUE_F,fill_random_num * fill_random_nu2','VALUE_F,fill_random_num * fill_random_nu2 * fill_random_nu3',
                'VALUE_F,fill_random_num - fill_random_nu2','VALUE_F,fill_random_num / fill_random_nu2 / fill_random_nu3']
        fill_f_base = str(random.sample(fills_f,1)).replace("[","").replace("]","").replace("'","").replace(", ","").replace("10000","'10000'").replace("fill_random_num",str(fill_random_num)).replace("fill_random_nu2",str(fill_random_nu2)).replace("fill_random_nu3",str(fill_random_nu3))
        single_fill_f = 'Fill' +'(' +fill_f_base + ')'
        
        #单fill,对时间强要求
        #fill_random_num = random.randint(-1000000,1000000)
        fills_not_f = ['NONE','VALUE,100','PREV','NULL','LINEAR','NEXT','VALUE,10000','VALUE,fill_random_num',
                    'VALUE,fill_random_num + fill_random_nu2','VALUE,fill_random_num + fill_random_nu2 + fill_random_nu3',
                    'VALUE,fill_random_num - fill_random_nu2','VALUE,fill_random_num - fill_random_nu2 - fill_random_nu3',
                    'VALUE,fill_random_num * fill_random_nu2','VALUE,fill_random_num * fill_random_nu2 * fill_random_nu3',
                    'VALUE,fill_random_num / fill_random_nu2','VALUE,fill_random_num / fill_random_nu2 / fill_random_nu3']
        fill_not_f_base = str(random.sample(fills_not_f,1)).replace("[","").replace("]","").replace("'","").replace(", ","").replace("10000","'10000'").replace("fill_random_num",str(fill_random_num)).replace("fill_random_nu2",str(fill_random_nu2)).replace("fill_random_nu3",str(fill_random_nu3))
        single_fill_not_f = 'Fill' +'(' +fill_not_f_base + ')'

        #超级表，不支持session，state_window
        session_units = ['s','m','h','d','w','a'] #不支持n(自然月) 和 y(自然年)
        session_unit = random.sample(session_units,1)
        session_base = str(interval_n + session_unit).replace("[","").replace("]","").replace("'","").replace(", ","")
        single_session = 'SESSION'+'(ts,'+ session_base + ')'
        
        #单state_window
        func = ['STATE_WINDOW']
        window_support_types = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_bool)'] #其余不支持
        state_window = random.sample(func,1)+random.sample(window_support_types,1)
        single_state_window = str(state_window).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        #event_window
        #stable = 1\2\3\4\5\6\7
        # table = 1\2\3\4\5\11\12
        stable_event_list = (1,2,3,4,5,6,7,8,)
        event_num1,event_num2 = random.choice(stable_event_list),random.choice(stable_event_list)
        start_trigger_condition1,start_trigger_condition2 =  self.event_window_i(event_num1),self.event_window_i(event_num1)
        end_trigger_condition1,end_trigger_condition2 =  self.event_window_i(event_num2),self.event_window_i(event_num2)      
        single_event_window_stable_1 = ' EVENT_WINDOW START WITH '  + ' %s ' %start_trigger_condition1 + ' END WITH ' + ' %s ' %end_trigger_condition1
        single_event_window_stable_2 = ' EVENT_WINDOW START WITH '  + ' %s ' %start_trigger_condition1 + ' END WITH ' + ' %s ' %end_trigger_condition1 + ' or %s ' %end_trigger_condition2
        single_event_window_stable_3 = ' EVENT_WINDOW START WITH '  + ' %s ' %start_trigger_condition1 + ' or %s ' %start_trigger_condition2 + ' END WITH ' + '%s ' %end_trigger_condition1
        single_event_window_stable_4 = ' EVENT_WINDOW START WITH '  + ' %s ' %start_trigger_condition1 + ' or %s ' %start_trigger_condition2 + ' END WITH ' + '%s ' %end_trigger_condition1 + ' or %s ' %end_trigger_condition2
        single_event_window_stable_5 = ' EVENT_WINDOW START WITH '  + self.event_window_i(random.randint(1,5)) + ' and '  + self.event_window_i(random.randint(6,8)) + ' END WITH ' + self.event_window_i(random.randint(1,5)) + ' and '  + self.event_window_i(random.randint(6,8))


        table_event_list = (1,2,3,4,5,11,12,13,)
        event_num1,event_num2 = random.choice(table_event_list),random.choice(table_event_list)
        start_trigger_condition1,start_trigger_condition2 =  self.event_window_i(event_num1),self.event_window_i(event_num1)
        end_trigger_condition1,end_trigger_condition2 =  self.event_window_i(event_num2),self.event_window_i(event_num2) 
        single_event_window_table_1 = ' EVENT_WINDOW START WITH '  + ' %s ' %start_trigger_condition1 + ' END WITH ' + ' %s ' %end_trigger_condition1
        single_event_window_table_2 = ' EVENT_WINDOW START WITH '  + ' %s ' %start_trigger_condition1 + ' END WITH ' + ' %s ' %end_trigger_condition1 + ' or %s ' %end_trigger_condition2
        single_event_window_table_3 = ' EVENT_WINDOW START WITH '  + ' %s ' %start_trigger_condition1 + ' or %s ' %start_trigger_condition2 + ' END WITH ' + '%s ' %end_trigger_condition1
        single_event_window_table_4 = ' EVENT_WINDOW START WITH '  + ' %s ' %start_trigger_condition1 + ' or %s ' %start_trigger_condition2 + ' END WITH ' + '%s ' %end_trigger_condition1 + ' or %s ' %end_trigger_condition2
        single_event_window_table_5 = ' EVENT_WINDOW START WITH '  + self.event_window_i(random.randint(1,5)) + ' and '  + self.event_window_i(random.randint(11,13)) + ' END WITH ' + self.event_window_i(random.randint(1,5)) + ' and '  + self.event_window_i(random.randint(11,13))

        if i == 1:
            time_window = single_interval
        elif i == 2:
            time_window = single_interval_offset
        elif i == 3:
            time_window = sliding_interval + ' ' + single_sliding
        elif i == 4:
            time_window = sliding_interval_offset + ' ' + single_sliding
                        
        # elif i == 6:
        #     time_window = sliding_interval + ' ' + single_fill 
        # elif i == 7:
        #     time_window = sliding_interval + ' ' + single_sliding + ' ' + single_fill 
        # elif i == 8:
        #     time_window = sliding_interval_offset + ' ' + single_fill 
        # elif i == 9:
        #     time_window = sliding_interval_offset + ' ' + single_sliding + ' ' + single_fill 
            
        # elif i == 61:
        #     time_window = sliding_interval + ' ' + single_fill_f 
        # elif i == 71:
        #     time_window = sliding_interval + ' ' + single_sliding + ' ' + single_fill_f 
        # elif i == 81:
        #     time_window = sliding_interval_offset + ' ' + single_fill_f 
        # elif i == 91:
        #     time_window = sliding_interval_offset + ' ' + single_sliding + ' ' + single_fill_f 
            
        # elif i == 62:
        #     time_window = sliding_interval + ' ' + single_fill_not_f 
        # elif i == 72:
        #     time_window = sliding_interval + ' ' + single_sliding + ' ' + single_fill_not_f 
        # elif i == 82:
        #     time_window = sliding_interval_offset + ' ' + single_fill_not_f 
        # elif i == 92:
        #     time_window = sliding_interval_offset + ' ' + single_sliding + ' ' + single_fill_not_f 
                                    
        # #下面是错误的
        # elif i == 11:
        #     time_window = single_sliding
        # elif i == 12:
        #     time_window = single_fill 
        # elif i == 13:
        #     time_window = single_sliding + ' ' + single_fill    
        # elif i == 14:
        #     time_window = single_session + ' ' + single_state_window  
        # elif i == 15:
        #     time_window = single_sliding + ' ' + single_session  
        # elif i == 16:
        #     time_window = single_sliding + ' ' + single_state_window  
        # elif i == 17:
        #     time_window = single_fill + ' ' + single_session  
        # elif i == 18:
        #     time_window = single_fill + ' ' + single_state_window  
        # elif i == 19:
        #     time_window = single_fill + ' ' + single_session  + ' ' + single_state_window
        # elif i == 20:
        #     time_window = single_sliding + ' ' + single_fill + ' ' + single_session  + ' ' + single_state_window
                                    
        # #部分正确的，超级表错误，子表，普通表正确     
        # elif i == 21:
        #     time_window = single_session
        # elif i == 22:
        #     time_window = single_state_window
            
        # #event_window    #stable-right
        # elif i == 31:
        #     time_window = single_event_window_stable_1
        # elif i == 32:
        #     time_window = single_event_window_stable_2
        # elif i == 33:
        #     time_window = single_event_window_stable_3
        # elif i == 34:
        #     time_window = single_event_window_stable_4
        # elif i == 35:
        #     time_window = single_event_window_stable_5
            
        # #event_window    #table-right
        # elif i == 41:
        #     time_window = single_event_window_table_1
        # elif i == 42:
        #     time_window = single_event_window_table_2
        # elif i == 43:
        #     time_window = single_event_window_table_3
        # elif i == 44:
        #     time_window = single_event_window_table_4
        # elif i == 45:
        #     time_window = single_event_window_table_5
         
        # #event_window_error    
        # elif i == 50:
        #     time_window = single_event_window_stable_1 + ' ' + single_state_window 
        # elif i == 51:
        #     time_window = single_event_window_stable_1 + ' ' + single_interval  
        # elif i == 52:
        #     time_window = single_event_window_stable_1 + ' ' + single_interval_offset  
        # elif i == 53:
        #     time_window = single_event_window_stable_1 + ' ' + single_sliding  
        # elif i == 54:
        #     time_window = single_event_window_stable_1 + ' ' + single_session  
        # elif i == 55:
        #     time_window = single_state_window + ' '  +  single_event_window_stable_1  
        # elif i == 56:
        #     time_window = single_interval + ' '  +  single_event_window_stable_1  
        # elif i == 57:
        #     time_window = single_interval_offset + ' '  +  single_event_window_stable_1     
        # elif i == 58:
        #     time_window = single_sliding + ' '  +  single_event_window_stable_1    
        # elif i == 59:
        #     time_window = single_session + ' '  +  single_event_window_stable_1  

                               
        return time_window        
        
    def describe_table(self,db_tb):
        self.tdSql.query("alter local 'schedulePolicy' '%d';" %random.randint(1,3))
        random_num1 = random.randint(0,1000)
        random_num2 = random.randint(0,100)
        describe_sql = "describe %s;" %db_tb
        self.tdSql.query(describe_sql)  
        rows = self.tdSql.query_row
        
        # # column
        # for i in range(rows):
        #     self.tdSql.query(describe_sql) 
        #     self.basic_query_sql(self.tdSql.getData(i,0),db_tb,'FUNCTION','',',NUM','')
        
        #count    
        for i in range(rows):
            self.tdSql.query(describe_sql) 
            self.basic_query_sql(self.tdSql.getData(i,0),db_tb,'FUNCTION','count',',NUM','')
        for i in range(rows):
            self.tdSql.query(describe_sql) 
            self.interval_query_sql(self.tdSql.getData(i,0),db_tb,'FUNCTION','count',',NUM','')
        
        # #sample    
        # for i in range(rows):
        #     self.tdSql.query(describe_sql) 
        #     self.basic_query_sql(self.tdSql.getData(i,0),db_tb,'FUNCTION','sample','NUM',random_num1)
            
        # #top bottom    
        # for i in range(rows):
        #     self.tdSql.query(describe_sql) 
        #     self.basic_query_sql(self.tdSql.getData(i,0),db_tb,'FUNCTION','TOP','NUM',random_num2)    
        # for i in range(rows):
        #     self.tdSql.query(describe_sql) 
        #     self.basic_query_sql(self.tdSql.getData(i,0),db_tb,'FUNCTION','BOTTOM','NUM',random_num2)
            
        # #last last_row first    
        # for i in range(rows):
        #     self.tdSql.query(describe_sql) 
        #     self.basic_query_sql(self.tdSql.getData(i,0),db_tb,'FUNCTION','LAST',',NUM','')
        # for i in range(rows):
        #     self.tdSql.query(describe_sql)
        #     self.basic_query_sql(self.tdSql.getData(i,0),db_tb,'FUNCTION','FIRST',',NUM','')
        # for i in range(rows):
        #     self.tdSql.query(describe_sql)
        #     self.basic_query_sql(self.tdSql.getData(i,0),db_tb,'FUNCTION','LAST_ROW',',NUM','')
    
    def basic_query_util_common(self,sql,data_col,db_tb,base_fun,replace_fun,base_num,replace_num):
        self.time_cost(sql)   
        sql_union_all = "(" + sql + ") UNION ALL (" +sql + ")";
        self.time_cost(sql_union_all)  
        sql_union = sql_union_all.replace('ALL','')
        self.time_cost(sql_union)  
        sql = "SELECT count(*) FROM (%s);" %sql  #统计上面sql的nest
        self.time_cost(sql)   
        sql_union_all = "SELECT count(*) FROM (%s);" %sql_union_all  #统计上面sql的nest
        self.time_cost(sql_union_all)  
        sql_union = sql_union_all.replace('ALL','')
        self.time_cost(sql_union) 
        
            
    def basic_query_util(self,sql,data_col,db_tb,base_fun,replace_fun,base_num,replace_num):
        sql_base = sql
        distinct_sql = sql_base.replace('%s' %base_fun,'%s' %replace_fun).replace('%s' %base_num,'%s' %replace_num)
        self.basic_query_util_common(distinct_sql,data_col,db_tb,base_fun,replace_fun,base_num,replace_num)
                
        no_distinct_sql = sql_base.replace('%s' %base_fun,'%s' %replace_fun).replace('DISTINCT','').replace('%s' %base_num,'%s' %replace_num)        
        self.basic_query_util_common(no_distinct_sql,data_col,db_tb,base_fun,replace_fun,base_num,replace_num)
        
        rowts_sql = sql_base.replace('%s' %base_fun,'%s' %replace_fun).replace('%s' %base_num,'%s' %replace_num).replace('_ROWTS,' ,'')
        self.basic_query_util_common(rowts_sql,data_col,db_tb,base_fun,replace_fun,base_num,replace_num)
                
        no_rowts_sql = sql_base.replace('%s' %base_fun,'%s' %replace_fun).replace('DISTINCT','').replace('%s' %base_num,'%s' %replace_num).replace('_ROWTS,' ,'')        
        self.basic_query_util_common(no_rowts_sql,data_col,db_tb,base_fun,replace_fun,base_num,replace_num)
        
        no_tbname_distinct_sql = sql_base.replace('%s' %base_fun,'%s' %replace_fun).replace('%s' %base_num,'%s' %replace_num).replace('TBNAME,' ,'')
        self.basic_query_util_common(no_tbname_distinct_sql,data_col,db_tb,base_fun,replace_fun,base_num,replace_num)
                
        no_tbname_no_distinct_sql = sql_base.replace('%s' %base_fun,'%s' %replace_fun).replace('DISTINCT','').replace('%s' %base_num,'%s' %replace_num).replace('TBNAME,' ,'')        
        self.basic_query_util_common(no_tbname_no_distinct_sql,data_col,db_tb,base_fun,replace_fun,base_num,replace_num)
        
        no_tbname_rowts_sql = sql_base.replace('%s' %base_fun,'%s' %replace_fun).replace('%s' %base_num,'%s' %replace_num).replace('_ROWTS,' ,'').replace('TBNAME,' ,'')
        self.basic_query_util_common(no_tbname_rowts_sql,data_col,db_tb,base_fun,replace_fun,base_num,replace_num)
                
        no_tbname_no_rowts_sql = sql_base.replace('%s' %base_fun,'%s' %replace_fun).replace('DISTINCT','').replace('%s' %base_num,'%s' %replace_num).replace('_ROWTS,' ,'').replace('TBNAME,' ,'')        
        self.basic_query_util_common(no_tbname_no_rowts_sql,data_col,db_tb,base_fun,replace_fun,base_num,replace_num)
        
        # if i == 1:
        #     sql = distinct_sql
        # elif i ==2:
        #     sql = no_distinct_sql
            
        # return sql
        
            
    def basic_query_sql(self,data_col,db_tb,base_fun,replace_fun,base_num,replace_num):
        column_tag_list = self.random_column_tag(db_tb)
        column_tag_list_where = str(self.random_column_tag_where(db_tb)).replace("[","").replace("]","").replace("'","").replace("\"","").replace(",","").replace("{","'[").replace("}","]'").replace("《","'").replace("》","'").replace("|",",")
        column_tag_list_where_1 = str(self.random_column_tag_where(db_tb)).replace("[","").replace("]","").replace("'","").replace("\"","").replace(",","").replace("{","'[").replace("}","]'").replace("《","'").replace("》","'").replace("|",",")
        limit = self.limit_slimit(db_tb,1)
        limit_slimit = self.limit_slimit(db_tb,2)  #适合group by,partition by
        
        sql_num = "SELECT DISTINCT FUNCTION('%s',NUM) FROM %s " %(data_col,db_tb)
        self.basic_query_util(sql_num,data_col,db_tb,base_fun,replace_fun,base_num,replace_num)
        
        # sql_str = "SELECT DISTINCT FUNCTION('%s',NUM) FROM %s " %(data_col,db_tb)
        # self.basic_query_util(sql_str,data_col,db_tb,base_fun,replace_fun,base_num,replace_num)
        
        # sql_base = "SELECT DISTINCT FUNCTION(`%s`,NUM) FROM %s " %(data_col,db_tb)       #单列
        # self.basic_query_util(sql_base,data_col,db_tb,base_fun,replace_fun,base_num,replace_num)
        
        # sql_base = "SELECT DISTINCT FUNCTION %s,NUM FROM %s " %(column_tag_list,db_tb)     #多列
        # self.basic_query_util(sql_base,column_tag_list,db_tb,base_fun,replace_fun,base_num,replace_num)
        
        # sql_base_where = "SELECT DISTINCT FUNCTION(`%s`,NUM) FROM %s WHERE 1=1" %(data_col,db_tb) #单列
        # self.basic_query_util(sql_base_where,data_col,db_tb,base_fun,replace_fun,base_num,replace_num)
        # #sql_base_where = self.basic_query_util(sql_base_where,data_col,db_tb,base_fun,replace_fun,base_num,replace_num,1)
        # sql_base_where_null = "SELECT DISTINCT FUNCTION(`%s`,NUM) FROM %s WHERE `%s` is null and 1=1" %(data_col,db_tb,data_col)
        # sql_base_where_notnull = "SELECT DISTINCT FUNCTION(`%s`,NUM) FROM %s WHERE `%s` is not null and 1=1" %(data_col,db_tb,data_col)
        # sql_base_where_union_all = "(" + sql_base_where_null + ") UNION ALL (" +sql_base_where_notnull + ")";
        # self.basic_query_util(sql_base_where_union_all,data_col,db_tb,base_fun,replace_fun,base_num,replace_num)
        # #sql_base_where_union_all = self.basic_query_util(sql_base_where_union_all,data_col,db_tb,base_fun,replace_fun,base_num,replace_num,1)
        # #self.tdCreateData.dataequal('%s' %sql_base_where ,1,1,'%s' %sql_base_where_union_all ,1,1)  #没有合适的校验手段
        
        # sql_base_where = "SELECT DISTINCT FUNCTION %s,NUM FROM %s WHERE 1=1" %(column_tag_list,db_tb)  #多列
        # self.basic_query_util(sql_base_where,column_tag_list,db_tb,base_fun,replace_fun,base_num,replace_num)
        # sql_base_where_null = "SELECT DISTINCT FUNCTION %s,NUM FROM %s WHERE `%s` is null and 1=1" %(column_tag_list,db_tb,data_col)
        # sql_base_where_notnull = "SELECT DISTINCT FUNCTION %s,NUM FROM %s WHERE `%s` is not null and 1=1" %(column_tag_list,db_tb,data_col)
        # sql_base_where_union_all = "(" + sql_base_where_null + ") UNION ALL (" +sql_base_where_notnull + ")";
        # self.basic_query_util(sql_base_where_union_all,column_tag_list,db_tb,base_fun,replace_fun,base_num,replace_num)
        
        # sql_base_where = "SELECT DISTINCT FUNCTION %s,NUM FROM %s WHERE %s 1=1" %(column_tag_list,db_tb,column_tag_list_where)  #多列
        # self.basic_query_util(sql_base_where,column_tag_list,db_tb,base_fun,replace_fun,base_num,replace_num)
        # sql_base_where = "SELECT DISTINCT FUNCTION %s,NUM FROM %s WHERE %s  1=1" %(column_tag_list,db_tb,column_tag_list_where)
        # sql_base_where_1 = "SELECT DISTINCT FUNCTION %s,NUM FROM %s WHERE %s  1=1" %(column_tag_list,db_tb,column_tag_list_where_1)
        # sql_base_where_union_all = "(" + sql_base_where + ") UNION ALL (" +sql_base_where_1 + ")";
        # self.basic_query_util(sql_base_where_union_all,column_tag_list,db_tb,base_fun,replace_fun,base_num,replace_num)
        # sql_base_where = "SELECT DISTINCT FUNCTION %s,NUM FROM %s WHERE %s  1=1 %s" %(column_tag_list,db_tb,column_tag_list_where,limit)
        # sql_base_where_1 = "SELECT DISTINCT FUNCTION %s,NUM FROM %s WHERE %s  1=1 %s" %(column_tag_list,db_tb,column_tag_list_where_1,limit)
        # sql_base_where_union_all = "(" + sql_base_where + ") UNION ALL (" +sql_base_where_1 + ")";
        # self.basic_query_util(sql_base_where_union_all,column_tag_list,db_tb,base_fun,replace_fun,base_num,replace_num)
        
        # sql_orderby = "SELECT DISTINCT FUNCTION(`%s`,NUM) FROM %s ORDER BY _ROWTS,`%s`" %(data_col,db_tb,data_col) #单列
        # self.basic_query_util(sql_orderby,data_col,db_tb,base_fun,replace_fun,base_num,replace_num)
        
        # sql_orderby = "SELECT DISTINCT FUNCTION(`%s`,NUM) FROM %s ORDER BY %s" %(data_col,db_tb,column_tag_list) #多列
        # self.basic_query_util(sql_orderby,data_col,db_tb,base_fun,replace_fun,base_num,replace_num)
        
        # sql_groupby = "SELECT DISTINCT FUNCTION(`%s`,NUM) FROM %s GROUP BY TBNAME,`%s`" %(data_col,db_tb,data_col)   #单列
        # self.basic_query_util(sql_groupby,data_col,db_tb,base_fun,replace_fun,base_num,replace_num) 
        
        # sql_groupby = "SELECT DISTINCT FUNCTION(`%s`,NUM) FROM %s GROUP BY %s" %(data_col,db_tb,column_tag_list)   #多列
        # self.basic_query_util(sql_groupby,data_col,db_tb,base_fun,replace_fun,base_num,replace_num) 
        
        # sql_partitionby = "SELECT DISTINCT FUNCTION(`%s`,NUM) FROM %s PARTITION BY TBNAME,`%s`" %(data_col,db_tb,data_col)   #单列
        # self.basic_query_util(sql_partitionby,data_col,db_tb,base_fun,replace_fun,base_num,replace_num) 
        
        # sql_partitionby = "SELECT DISTINCT FUNCTION(`%s`,NUM) FROM %s PARTITION BY %s" %(data_col,db_tb,column_tag_list)   #多列
        # self.basic_query_util(sql_partitionby,data_col,db_tb,base_fun,replace_fun,base_num,replace_num) 
        

    def interval_query_sql(self,data_col,db_tb,base_fun,replace_fun,base_num,replace_num):
        self.tdSql.query("alter local 'schedulePolicy' '%d';" %random.randint(1,3))
        column_tag_list = self.random_column_tag(db_tb)
        column_tag_list_where = str(self.random_column_tag_where(db_tb)).replace("[","").replace("]","").replace("'","").replace("\"","").replace(",","").replace("{","'[").replace("}","]'").replace("《","'").replace("》","'").replace("|",",")
        column_tag_list_where_1 = str(self.random_column_tag_where(db_tb)).replace("[","").replace("]","").replace("'","").replace("\"","").replace(",","").replace("{","'[").replace("}","]'").replace("《","'").replace("》","'").replace("|",",")
        limit = self.limit_slimit(db_tb,1)
        limit_slimit = self.limit_slimit(db_tb,2)  #适合group by,partition by
        
        for i in (1,2,3,4,21,41,42,43,44,45,):                        
            time_window = self.time_window(i)
            
            sql_num = "SELECT DISTINCT FUNCTION('%s',NUM) FROM %s %s " %(data_col,db_tb,time_window)
            self.basic_query_util(sql_num,data_col,db_tb,base_fun,replace_fun,base_num,replace_num)
        
                    
    def basic_query_util_bak(self,data_col,db_tb,base_fun,replace_fun,base_num,replace_num):
        sql_base = "select DISTINCT FUNCTION(`%s`,NUM) from %s " %(data_col,db_tb)
        sql = sql_base.replace('%s' %base_fun,'%s' %replace_fun).replace('%s' %base_num,'%s' %replace_num)
        self.time_cost(sql)   
        sql_union_all = sql + " UNION ALL " +sql ;
        self.time_cost(sql_union_all)  
        sql_union = sql_union_all.replace('ALL','')
        self.time_cost(sql_union)  
        sql = "select count(*) from (%s);" %sql  #统计上面sql的nest
        self.time_cost(sql)   
        sql_union_all = "select count(*) from (%s);" %sql_union_all  #统计上面sql的nest
        self.time_cost(sql_union_all)  
        sql_union = sql_union_all.replace('ALL','')
        self.time_cost(sql_union) 
                
        sql = sql_base.replace('%s' %base_fun,'%s' %replace_fun).replace('DISTINCT','').replace('%s' %base_num,'%s' %replace_num)
        self.time_cost(sql)  
        sql_union_all = sql + " UNION ALL " +sql ;
        self.time_cost(sql_union_all)  
        sql_union = sql_union_all.replace('ALL','')
        self.time_cost(sql_union) 
        sql = "select count(*) from (%s);" %sql  #统计上面sql的nest
        self.time_cost(sql) 
        sql_union_all = "select count(*) from (%s);" %sql_union_all  #统计上面sql的nest
        self.time_cost(sql_union_all)  
        sql_union = sql_union_all.replace('ALL','')
        self.time_cost(sql_union) 
        
        #order by 
        sql_orderby = "select DISTINCT FUNCTION(`%s`,NUM) from %s ORDER BY _ROWTS,`%s`" %(data_col,db_tb,data_col)        
        sql = sql_orderby.replace('%s' %base_fun,'%s' %replace_fun).replace('%s' %base_num,'%s' %replace_num)
        self.time_cost(sql)          
        sql_union_all = "(" + sql + ") UNION ALL (" +sql + ")";
        self.time_cost(sql_union_all)  
        sql_union = sql_union_all.replace('ALL','')
        self.time_cost(sql_union)  
        sql = "select count(*) from (%s);" %sql  
        self.time_cost(sql)  
        sql_union_all = "select count(*) from (%s);" %sql_union_all  #统计上面sql的nest
        self.time_cost(sql_union_all)  
        sql_union = sql_union_all.replace('ALL','')
        self.time_cost(sql_union) 
            
        sql = sql_orderby.replace('%s' %base_fun,'%s' %replace_fun).replace('%s' %base_num,'%s' %replace_num).replace('_ROWTS,' ,'')
        self.time_cost(sql)         
        sql_union_all = "(" + sql + ") UNION ALL (" +sql + ")";
        self.time_cost(sql_union_all)  
        sql_union = sql_union_all.replace('ALL','')
        self.time_cost(sql_union)  
        sql = "select count(*) from (%s);" %sql  
        self.time_cost(sql)  
        sql_union_all = "select count(*) from (%s);" %sql_union_all  #统计上面sql的nest
        self.time_cost(sql_union_all)    
        sql_union = sql_union_all.replace('ALL','')
        self.time_cost(sql_union)   
        
        sql = sql_orderby.replace('%s' %base_fun,'%s' %replace_fun).replace('DISTINCT','').replace('%s' %base_num,'%s' %replace_num)
        self.time_cost(sql)   
        sql_union_all = "(" + sql + ") UNION ALL (" +sql + ")";
        self.time_cost(sql_union_all)  
        sql_union = sql_union_all.replace('ALL','')
        self.time_cost(sql_union)  
        sql = "select count(*) from (%s);" %sql  
        self.time_cost(sql)  
        sql_union_all = "select count(*) from (%s);" %sql_union_all  #统计上面sql的nest
        self.time_cost(sql_union_all)   
        sql_union = sql_union_all.replace('ALL','')
        self.time_cost(sql_union) 
         
        sql = sql_orderby.replace('%s' %base_fun,'%s' %replace_fun).replace('DISTINCT','').replace('%s' %base_num,'%s' %replace_num).replace('_ROWTS,' ,'')
        self.time_cost(sql)  
        sql_union_all = "(" + sql + ") UNION ALL (" +sql + ")";
        self.time_cost(sql_union_all)  
        sql_union = sql_union_all.replace('ALL','')
        self.time_cost(sql_union)   
        sql = "select count(*) from (%s);" %sql  
        self.time_cost(sql)   
        sql_union_all = "select count(*) from (%s);" %sql_union_all  #统计上面sql的nest
        self.time_cost(sql_union_all)   
        sql_union = sql_union_all.replace('ALL','')
        self.time_cost(sql_union) 
        
        #group by , partition by 
        sql_groupby = "select DISTINCT FUNCTION(`%s`,NUM) from %s GROUP BY TBNAME,`%s`" %(data_col,db_tb,data_col)        
        sql = sql_groupby.replace('%s' %base_fun,'%s' %replace_fun).replace('%s' %base_num,'%s' %replace_num)
        self.time_cost(sql)   
        sql = "select count(*) from (%s);" %sql  
        self.time_cost(sql)     
        sql = sql_groupby.replace('%s' %base_fun,'%s' %replace_fun).replace('%s' %base_num,'%s' %replace_num).replace('TBNAME,' ,'')
        self.time_cost(sql)   
        sql = "select count(*) from (%s);" %sql  
        self.time_cost(sql) 
        
        # #TD-24781    
        # sql = sql_groupby.replace('%s' %base_fun,'%s' %replace_fun).replace('%s' %base_num,'%s' %replace_num).replace('GROUP','PARTITION')
        # self.time_cost(sql)   
        # sql = "select count(*) from (%s);" %sql  
        # self.time_cost(sql)    
        # sql = sql_groupby.replace('%s' %base_fun,'%s' %replace_fun).replace('%s' %base_num,'%s' %replace_num).replace('GROUP','PARTITION').replace('TBNAME,' ,'')
        # self.time_cost(sql)   
        # sql = "select count(*) from (%s);" %sql  
        # self.time_cost(sql) 
                
        sql = sql_groupby.replace('%s' %base_fun,'%s' %replace_fun).replace('DISTINCT','').replace('%s' %base_num,'%s' %replace_num)
        self.time_cost(sql)  
        sql = "select count(*) from (%s);" %sql  
        self.time_cost(sql)                 
        sql = sql_groupby.replace('%s' %base_fun,'%s' %replace_fun).replace('DISTINCT','').replace('%s' %base_num,'%s' %replace_num).replace('TBNAME,' ,'')
        self.time_cost(sql)  
        sql = "select count(*) from (%s);" %sql  
        self.time_cost(sql) 
        
        # #TD-24781   
        # sql = sql_groupby.replace('%s' %base_fun,'%s' %replace_fun).replace('DISTINCT','').replace('%s' %base_num,'%s' %replace_num).replace('GROUP','PARTITION')
        # self.time_cost(sql)  
        # sql = "select count(*) from (%s);" %sql  
        # self.time_cost(sql)               
        # sql = sql_groupby.replace('%s' %base_fun,'%s' %replace_fun).replace('DISTINCT','').replace('%s' %base_num,'%s' %replace_num).replace('GROUP','PARTITION').replace('TBNAME,' ,'')
        # self.time_cost(sql)  
        # sql = "select count(*) from (%s);" %sql  
        # self.time_cost(sql) 
        

    def time_cost(self,sql):
        startTime = time.time()*1000  
        #self.tdSql.query(sql,queryTimes=1)
        self.data_check(sql)
        #self.tdCreateData.explain_sql(sql)
        endTime = time.time()*1000        
        self.logger.info("total time %d ms" % (endTime - startTime))
              
       
    def data_check(self,sql) :
        #判断sql执行结果，如果执行成功，判断返回rows，>0记录sql到文件， =0提示退出， sql执行不成功，则记录sql，不进入sql文件
        rows = 0;
        succ_flag = 0
        t = time.time()
        t_to_s =  time.strftime('%Y-%m-%d', time.localtime(t)) 
        
        try:
            self.tdSql.query(sql,queryTimes=1)
            rows = self.tdSql.query_row
            succ_flag = 1
        except:
            self.logger.info("sql is not support :=====%s; " %sql)
            self.tdSql.error(sql)
            
        if rows:
            self.explain_sql(sql) if rows > 0 else sys.exit("data rows = 0")
        
        if succ_flag:            
            result_file_name = self.testcasePath + '/sqls/schema.sql_%s' %t_to_s        
            f = open(result_file_name, 'a') 
            f.write(str(sql) + "; \n")
            f.close()
        else:
            result_file_name = self.testcasePath + '/sqls/error/schema_error.sql_%s' %t_to_s        
            f = open(result_file_name, 'a') 
            f.write(str(sql) + "; \n")
            #f.write(str(self.tdSql.error(sql)) + "; \n")
            f.close()
                    
    def explain_sql(self,sql): 
        self.tdSql.query("alter local 'schedulePolicy' '%d';" %random.randint(1,3))
        self.tdSql.execute("reset query cache;")
        sql = "explain " + sql 
        self.tdSql.query(sql,queryTimes=1) 
        
    
    def where_filter(self): 
        fake = Faker('zh_CN') 
        data_filters = ['c1 >= -127 and ' , 'c1 <= 127 and ' , 'c0 <= 2147483647 and ' , 'c0 >= -2147483647 and ',  'c2 >= -1.7E308 and ','c2 <= 1.7E308 and ', 't0 >= -127 and ' , 't0 <= 127 and ' ,
                    'c0 between -2147483647 and 2147483647 and ','c1 between -127 and 127  and ','c2 between -1.7E308 and 1.7E308 and ' ,'t0 between -127 and 127  and ',
                    'c0 is not null and ', 'c1 is not null and ' ,'c2 is not null and ' ,'t0 is not null and ' ,
                    'c3 is not null and ' , 'c4 is not null and ', 't0 is not null and '
                    'ts is not null and ' ,'_c0 is not null and ' ,'_C0 is not null and ' ,'_rowts is not null and ' ,
                    'ts <= now and ' , 'ts >= 1651334400000 and ' ,' ts between 1651330000000 and now +1h  and ', 
                    '_c0 <= now +100h and ' , '_c0 >= 1651334400000 and ' , ' _c0 between 1651330000000 and now +1h  and ' ,
                    '_C0 <= now +1h and ' ,  '_C0 >= 1651330000000 and ' ,' _C0 between 1651330000000 and now +1h  and ',
                    '_rowts <= now +1h and ' ,'_rowts >= 1651330000000 and ' ,' _rowts between 1651330000000 and now +1h  and ']        
        data_filter = random.sample(data_filters,6)

        like_filters = ['c3 like \'varchar%\' and ','(c3 like \'varchar%\'  or c3 = \'0\'  or c3 = \'varchar_\' or c3 is not null ) and ','c4 like \'nchar%\' and ','(c4 like \'nchar%\' or c4 = \'0\'  or c4 = \'nchar_\' or c4 is not null  ) and ','t1 like \'varchar%\' and ','(t1 like \'varchar%\' or t1 = \'0\'  or t1 = \'varchar_\'  or t1 is not null ) and ',]
        match_filters = ['c3 match \'va\' and ','c4 nmatch \'varcharnchar\' and ','c4 match \'nc\' and ','c3 nmatch \'varcharnchar\' and ','t1 match \'va\' and ','t1 nmatch \'ncharvarchar\' and ',]
        like_match_filters = random.sample(random.sample(like_filters,1) + random.sample(match_filters,1),1)
        like_match_filter = str(like_match_filters).replace("[","").replace("]","").replace("\"","")

        q_tinyint_list,t_tinyint_list=[],[]
        for i in range(-100,100):
            q_tinyint_list.append(i)
            t_tinyint_list.append(i)
            
            
        # and ts >=1651334400000 and ts <=1651338000000            
        time_units = ['s','m','h','d','w'] #有限制，所以需要删除几个
        time_unit = random.sample(time_units,1)
        ts_range = " ts >= now - %d%s and ts <= now " %(fake.random_int(min=0, max=12, step=1),time_unit)
        
        q_tinyint_list = " c1 in (" + str(q_tinyint_list).replace("[","").replace("]","") + ") and " + '%s' %ts_range
        t_tinyint_list = " t0 in (" + str(t_tinyint_list).replace("[","").replace("]","") + ") and " + '%s'  %ts_range      
        in_filters = [q_tinyint_list , t_tinyint_list, '%s' %ts_range]        
        in_filter = str(random.sample(in_filters,1)).replace("[","").replace("]","").replace("'","").replace("\" ","").replace(" \"","")
        
        orderby_filters = ['ts','_c0','_C0','_rowts','c1','c2','c3','c4','t0','t1']
        i = random.randint(1,8)
        orderby_filter = str(random.sample(orderby_filters,i)).replace("[","").replace("]","").replace("'","")
        orderby_filter = str('order by ' + orderby_filter).replace("[","").replace("]","").replace("'","")
        
        groupby_filters = ['ts','_c0','_C0','_rowts','c1','c2','c3','c4','t0','t1']
        i = random.randint(1,8)
        groupby_filter = str(random.sample(groupby_filters,i)).replace("[","").replace("]","").replace("'","")
        groupby_filter = str('group by ' + groupby_filter).replace("[","").replace("]","").replace("'","")
        
        partitionby_filters = ['ts','_c0','_C0','_rowts','c1','c2','c3','c4','t0','t1']
        i = random.randint(1,8)
        partitionby_filter = str(random.sample(partitionby_filters,i)).replace("[","").replace("]","").replace("'","")
        partitionby_filter = str('partition by ' + partitionby_filter).replace("[","").replace("]","").replace("'","")
        
        limit_filters = ['limit 100000','limit 100000,1000','limit 100000 offset 10000','slimit 100000','slimit 100000,1000','slimit 100000 soffset 10000']
        limit_filter = str(random.sample(limit_filters,1)).replace("[","").replace("]","").replace("'","")
        
        return(data_filter,like_match_filter,in_filter,orderby_filter,groupby_filter,partitionby_filter,limit_filter)

    def time_window(self,i):  
        #数字后面的时间单位可以是 u(微秒)、a(毫秒)、s(秒)、m(分)、h(小时)、d(天)、w(周)。 
        #在指定降频操作（down sampling）的时间窗口（interval）时，时间单位还可以使用 n(自然月) 和 y(自然年)。     
        interval_n, offset_n, sliding_n = [random.randrange(10,20)]  , [random.randrange(1,10)] , [random.randrange(1,10)] 
        time_window = ''
                
        #单interval
        interval_units = ['s','m','h','d','w','n','a','y']
        unit = random.sample(interval_units,1)
        interval_base = str(interval_n + unit).replace("[","").replace("]","").replace("'","").replace(", ","")
        single_interval = 'interval'+'(' +interval_base + ')'
        
        #单interval+offset
        offset_base = str(offset_n + unit).replace("[","").replace("]","").replace("'","").replace(", ","")
        single_interval_offset = 'interval'+'(' +interval_base + ',' + offset_base + ')'

        #interval + sliding
        interval_sliding_units = ['s','m','h','d','w'] #有限制，所以需要删除几个
        interval_sliding_unit = random.sample(interval_sliding_units,1)
        
        sliding_base = str(sliding_n + interval_sliding_unit).replace("[","").replace("]","").replace("'","").replace(", ","")
        single_sliding = 'sliding'+'(' +sliding_base + ')'

        sliding_interval_no_offset = str(interval_n + interval_sliding_unit).replace("[","").replace("]","").replace("'","").replace(", ","")
        sliding_interval = 'interval'+'(' +sliding_interval_no_offset + ')'
        
        sliding_interval_offset = str(offset_n + interval_sliding_unit).replace("[","").replace("]","").replace("'","").replace(", ","")
        sliding_interval_offset = 'interval'+'(' + sliding_interval_no_offset + ',' + sliding_interval_offset + ')'
        
        #单fill,对时间强要求
        fills = ['NONE','VALUE,100','PREV','NULL','LINEAR','NEXT']
        fill_base = str(random.sample(fills,1)).replace("[","").replace("]","").replace("'","").replace(", ","")
        single_fill = 'Fill' +'(' +fill_base + ')'

        #超级表，不支持session，state_window
        session_units = ['s','m','h','d','w','a'] #不支持n(自然月) 和 y(自然年)
        session_unit = random.sample(session_units,1)
        session_base = str(interval_n + session_unit).replace("[","").replace("]","").replace("'","").replace(", ","")
        single_session = 'SESSION'+'(ts,'+ session_base + ')'
        
        #单state_window
        func = ['STATE_WINDOW']
        window_support_types = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_bool)'] #其余不支持
        state_window = random.sample(func,1)+random.sample(window_support_types,1)
        single_state_window = str(state_window).replace("[","").replace("]","").replace("'","").replace(", ","")

        if i == 1:
            time_window = single_interval
        elif i == 2:
            time_window = single_interval_offset
        elif i == 3:
            time_window = sliding_interval + ' ' + single_sliding
        elif i == 4:
            time_window = sliding_interval_offset + ' ' + single_sliding
                        
        elif i == 6:
            time_window = sliding_interval + ' ' + single_fill 
        elif i == 7:
            time_window = sliding_interval + ' ' + single_sliding + ' ' + single_fill 
        elif i == 8:
            time_window = sliding_interval_offset + ' ' + single_fill 
        elif i == 9:
            time_window = sliding_interval_offset + ' ' + single_sliding + ' ' + single_fill 
                        
        #下面是错误的
        elif i == 11:
            time_window = single_sliding
        elif i == 12:
            time_window = single_fill 
        elif i == 13:
            time_window = single_sliding + ' ' + single_fill    
        elif i == 14:
            time_window = single_session + ' ' + single_state_window  
        elif i == 15:
            time_window = single_sliding + ' ' + single_session  
        elif i == 16:
            time_window = single_sliding + ' ' + single_state_window  
        elif i == 17:
            time_window = single_fill + ' ' + single_session  
        elif i == 18:
            time_window = single_fill + ' ' + single_state_window  
        elif i == 19:
            time_window = single_fill + ' ' + single_session  + ' ' + single_state_window
        elif i == 20:
            time_window = single_sliding + ' ' + single_fill + ' ' + single_session  + ' ' + single_state_window
                                    
        #部分正确的，超级表错误，子表，普通表正确     
        elif i == 21:
            time_window = single_session
        elif i == 22:
            time_window = single_state_window
                               
        return time_window
            
    def column_select(self,num):
        column = ''
        column_lists = ['ts','_c0','_C0','_rowts','c1','c2','c3','c4','t0','t1',]
        if num == 0:    
            column = '*'
        elif num == 1:    
            column = str(column_lists).replace("[","").replace("]","").replace("'","")
        elif num == 2:            
            i = random.randint(1,10)
            column = str(random.sample(column_lists,i)).replace("[","").replace("]","").replace("'","")
        elif num == 3:            
            column = str(random.sample(column_lists,1)).replace("[","").replace("]","").replace("'","")
            
        return column    
                              
    def select_column(self):
        self.logger.info("\n==========================select_column==========================\n")
                          
        for i in (1,):
            func = self.base_function_all(i)
            try:                
                self.tdSql.execute('use %s;' %self.db)            
                self.logger.info("\n\n\n=======hanshu num = %d======select_column======\n\n\n" %i)                
                where_filters = self.where_filter()
                for i in range(2,len(where_filters[0])+1):
                    data_filter = list(combinations(where_filters[0],i))
                    for data_filter in data_filter:
                        data_filter = str(data_filter).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        like_match_filter = where_filters[1]
                        in_filter = where_filters[2] 
                        orderby_filter = where_filters[3]  
                        groupby_filter = where_filters[4] 
                        partitonby_filter = where_filters[5] 
                        limit_filter = where_filters[6]                       

                        sql2 = "select %s from %s where  %s %s %s " %(self.column_select(0),self.table,data_filter,like_match_filter,in_filter)
                        self.data_check(sql2)
                        

            except Exception as e:
                raise e   
                    
    def select_column_union(self):
        self.logger.info("\n==========================select_column==========================\n")       
                          
        for i in (1,):
            func = self.base_function_all(i)
            try:                
                self.tdSql.execute('use %s;' %self.db)            
                self.logger.info("\n\n\n=======hanshu num = %d======select_column======\n\n\n" %i)                
                where_filters = self.where_filter()
                where_filters_2 = self.where_filter()
                for i in range(2,len(where_filters[0])+1):
                    data_filter = list(combinations(where_filters[0],i))
                    data_filter_2 = list(combinations(where_filters_2[0],i))
                    for data_filter in data_filter:
                        data_filter = str(data_filter).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        data_filter_2 = str(data_filter_2).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","").replace("[","").replace("]","")
                        like_match_filter = where_filters[1]
                        in_filter = where_filters[2]
                        orderby_filter = where_filters[3]  
                        groupby_filter = where_filters[4] 
                        partitonby_filter = where_filters[5] 
                        limit_filter = where_filters[6]      
                        
                        like_match_filter_2 = where_filters_2[1]
                        in_filter_2 = where_filters_2[2]
                        column_select = self.column_select(2) #针对union多列返回的个数不一样

                        sql2 = "select %s from %s where  %s %s %s " %(self.column_select(0),self.table,data_filter,like_match_filter,in_filter)
                        sql2 += " union select %s from %s where  %s %s %s " %(self.column_select(0),self.table,data_filter_2,like_match_filter_2,in_filter_2)
                        self.data_check(sql2)

            except Exception as e:
                raise e  
                        
                                        
    def run(self):
        startTime = time.time() 
        
        self.tdSql.query("alter local 'schedulePolicy' '%d';" %random.randint(1,3))
         
        #self.describe_table(self.db_tb) 
        
        #self.describe_table("`table_sample_1`.`stable_1`") 
        self.describe_table("`information_schema`.`ins_dnodes`") 
        self.describe_table("`information_schema`.`ins_mnodes`")
        #self.describe_table("`information_schema`.`ins_modules`")  #TD-24684
        self.describe_table("`information_schema`.`ins_qnodes`")
        self.describe_table("`information_schema`.`ins_snodes`")
        # self.describe_table("`information_schema`.`ins_cluster`")
        # self.describe_table("`information_schema`.`ins_databases`")
        # self.describe_table("`information_schema`.`ins_functions`")
        # self.describe_table("`information_schema`.`ins_indexes`")
        # # #self.describe_table("`information_schema`.`ins_stables`")  #TD-24784
        # # #self.describe_table("`information_schema`.`ins_tables`")  #TD-24707
        # # #self.describe_table("`information_schema`.`ins_tags`")  #TD-24707
        # # #self.describe_table("`information_schema`.`ins_columns`")  #TD-24705 man
        # # self.describe_table("`information_schema`.`ins_users`")
        # # self.describe_table("`information_schema`.`ins_grants`")
        # # # self.describe_table("`information_schema`.`ins_vgroups`")
        # # # self.describe_table("`information_schema`.`ins_configs`")
        # # # self.describe_table("`information_schema`.`ins_dnode_variables`")
        # # # #self.describe_table("`information_schema`.`ins_topics`")  #TD-24716
        # # # self.describe_table("`information_schema`.`ins_subscriptions`")
        # # # self.describe_table("`information_schema`.`ins_streams`")
        # # # self.describe_table("`information_schema`.`ins_stream_tasks`")
        # # # self.describe_table("`information_schema`.`ins_vnodes`")
        # # # self.describe_table("`information_schema`.`ins_user_privileges`")
        
        # self.describe_table("`performance_schema`.`perf_connections`")
        # self.describe_table("`performance_schema`.`perf_queries`")
        # self.describe_table("`performance_schema`.`perf_consumers`")
        # self.describe_table("`performance_schema`.`perf_trans`")
        # self.describe_table("`performance_schema`.`perf_apps`")
         
        
        # self.describe_table("`statistics`.`ag`")
        # self.describe_table("`statistics`.`e_yx`")
        # self.describe_table("`statistics`.`esg`")
        # self.describe_table("`statistics`.`ptg`")
        # self.describe_table("`statistics`.`pg`")
        # self.describe_table("`statistics`.`g`")
        # self.describe_table("`statistics`.`b`")
        # self.describe_table("`statistics`.`tg`")
           

        endTime = time.time()
        
        self.logger.info("total time %ds" % (endTime - startTime))
  

