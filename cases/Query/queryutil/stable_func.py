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
from itertools import product
from itertools import combinations
import time
from faker import Faker

from taostest import TDCase

class TDFunction():
    def desc(self) -> str:
        case_description = '''
        case1<xyguo>:function query 
        ''' 
        return case_description

    def tags(self) :
		
        return ""

    def author(self) -> str:

        return "Guo Xiangyang"

    def all_column(self):  
        # support all table, support all data type     
        hanshu = ['COUNT']
        column = ['(*)','(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)','(_c0)','(_C0)','(q_ts)','(q_bool)','(q_binary)','(q_nchar)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        all_column = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return all_column

    def all_column_tbname(self):  
        # support all table, support all data type  
        # 解决多个子表时，last_row\last\first返回值可能不一样的问题   
        hanshu = ['COUNT','FIRST','LAST','LAST_ROW']
        column = ['(*)','(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)','(_c0)','(_C0)','(q_ts)','(q_bool)','(q_binary)','(q_nchar)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        all_column = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return all_column
    def all_column_tbname_0(self):  
        # support all table, support all data type  
        # 解决多个子表时，last_row\last\first返回值可能不一样的问题   
        hanshu = ['COUNT']
        column = ['(*)','(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)','(_c0)','(_C0)','(q_ts)','(q_bool)','(q_binary)','(q_nchar)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        all_column = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return all_column
    def all_column_tbname_1(self):  
        # support all table, support all data type  
        # 解决多个子表时，last_row\last\first返回值可能不一样的问题   
        hanshu = ['FIRST']
        column = ['(*)','(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)','(_c0)','(_C0)','(q_ts)','(q_bool)','(q_binary)','(q_nchar)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        all_column = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return all_column
    def all_column_tbname_1_1(self):  
        # support all table, support all data type  
        # 解决多个子表时，last_row\last\first返回值可能不一样的问题   
        hanshu = ['FIRST']
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)','(_c0)','(_C0)','(q_ts)','(q_bool)','(q_binary)','(q_nchar)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        all_column = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return all_column
    def all_column_tbname_2(self):    
        hanshu = ['LAST']
        column = ['(*)','(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)','(_c0)','(_C0)','(q_ts)','(q_bool)','(q_binary)','(q_nchar)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        all_column = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return all_column
    def all_column_tbname_2_1(self):  
        #* 不能和order by 使用   
        hanshu = ['LAST']
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)','(_c0)','(_C0)','(q_ts)','(q_bool)','(q_binary)','(q_nchar)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        all_column = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return all_column
    def all_column_tbname_3(self):  
        # LAST_ROW() 不能与 INTERVAL 一起使用   
        hanshu = ['LAST_ROW']
        column = ['(*)','(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)','(_c0)','(_C0)','(q_ts)','(q_bool)','(q_binary)','(q_nchar)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        all_column = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return all_column
    def all_column_tbname_3_1(self):  
        # LAST_ROW() 不能与 INTERVAL 一起使用   
        hanshu = ['LAST_ROW']
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)','(_c0)','(_C0)','(q_ts)','(q_bool)','(q_binary)','(q_nchar)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        all_column = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return all_column
       
    def int_cloumn(self):  
        # support all int type \ double type              
        hanshu = ['AVG','SUM','MIN','MAX']   
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return int_cloumn
    
    def int_cloumn_0(self):  
        # support all int type \ double type              
        hanshu = ['SUM']   
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return int_cloumn
    
    def int_cloumn_1(self):  
        # support all int type \ double type              
        hanshu = ['AVG']   
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return int_cloumn
    
    def int_cloumn_1_tsbs(self):  
        # support all int type \ double type              
        hanshu = ['AVG']   
        column = ['(velocity)','(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)']
              #    '(q_bigint_null)','(q_smallint_null)','(q_tinyint_null)','(q_int_null)','(q_float_null)','(q_double_null)' 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return int_cloumn
                
    def int_cloumn_2(self):  
        # support all int type \ double type              
        hanshu = ['MIN','MAX']   
        column = ['(q_bigint)','(q_smallint)','(q_int)','(q_float)','(q_double)'] #q_tinyint 出现重复的概率较高，忽略
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return int_cloumn
    
    def int_ts_cloumn(self):  
        # support all int type \ double type \ ts type        
        hanshu = ['SPREAD']       
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)','(_c0)','(_C0)','(q_ts)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_ts_cloumn = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return int_ts_cloumn  

    def func_stable_all(self,i):   
        func_stable_all = ''
        if i == 1:    #['COUNT']
            func_stable_all = self.all_column()
        elif i == 2:  #['AVG','SUM','MIN','MAX'] 
            func_stable_all = self.int_cloumn()
        elif i == 21: #['AVG','SUM']
            func_stable_all = self.int_cloumn_1()
        elif i == 22: #['MIN','MAX'] 
            func_stable_all = self.int_cloumn_2()
        elif i == 3: #['SPREAD'] 
            func_stable_all = self.int_ts_cloumn()      

        return func_stable_all
    
    def func_stable_tbname_all(self,i):   
        func_stable_tbname_all = ''
        if i == 1:     #['COUNT','FIRST','LAST','LAST_ROW']
            func_stable_tbname_all = self.all_column_tbname()
        elif i == 10:    #['COUNT']
            func_stable_tbname_all = self.all_column_tbname_0()
        elif i == 11:    #['FIRST']
            func_stable_tbname_all = self.all_column_tbname_1()
        elif i == 111:    #['FIRST']
            func_stable_tbname_all = self.all_column_tbname_1_1()
        elif i == 12:    #['LAST']
            func_stable_tbname_all = self.all_column_tbname_2()    
        elif i == 121:    #['LAST']
            func_stable_tbname_all = self.all_column_tbname_2_1()        
        elif i == 13:    #['LAST_ROW']
            func_stable_tbname_all = self.all_column_tbname_3()        
        elif i == 131:    #['LAST_ROW']
            func_stable_tbname_all = self.all_column_tbname_3_1()
        elif i == 2:     #['AVG','SUM','MIN','MAX'] 
            func_stable_tbname_all = self.int_cloumn()
        elif i == 20:    #['SUM']
            func_stable_tbname_all = self.int_cloumn_0()
        elif i == 21:    #['AVG']
            func_stable_tbname_all = self.int_cloumn_1()
        elif i == 211:    #['AVG']
            func_stable_tbname_all = self.int_cloumn_1_tsbs()
        elif i == 22:   #['MIN','MAX']
            func_stable_tbname_all = self.int_cloumn_2()
        elif i == 3:    #['SPREAD']
            func_stable_tbname_all = self.int_ts_cloumn()      

        return func_stable_tbname_all  



    # special   
    
    def int_cloumn_n(self):  
        # support all int type \ double type              
        hanshu = ['TOP','BOTTOM']       
        column = ['(q_bigint,num)','(q_smallint,num)','(q_int,num)','(q_float,num)','(q_double,num)'] #,'(q_tinyint,num)'避免数据过小而重复
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return int_cloumn
    
    def only_inter_query(self):    
        # not support stddev/percentile in the outer query   
        # percentile functions not support for super table query 
        hanshu = ['STDDEV']   
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        only_inter_query = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return only_inter_query   
 
    def int_cloumn_regular_only(self):   
        # not support stable, if support should together with groupby tbname.  support all int type \ double type \ 
        # TWA/Diff/Derivative/Irate/CSUM/MAVG/SAMPLE/INTERP/Elapsed are not allowed to apply to super table directly 
        hanshu = ['TWA','DIFF','IRATE','CSUM','INTERP']  
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn_regular_only = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return int_cloumn_regular_only
    
    def int_cloumn_regular_only_1(self):   
        # 为了支持interval，分开#diff放后面 #CSUM可以和STATE_WINDOW结合 , 有单独的int_cloumn_csum，暂时不考虑csum
        hanshu = ['TWA']      
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn_regular_only = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return int_cloumn_regular_only
    def int_cloumn_regular_only_2(self):   
        hanshu = ['IRATE']      
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn_regular_only = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return int_cloumn_regular_only
    def int_cloumn_regular_only_3(self):   
        hanshu = ['INTERP']  
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)','(q_bool)',
                  '(q_bigint_null)','(q_smallint_null)','(q_tinyint_null)','(q_int_null)','(q_float_null)','(q_double_null)','(q_bool_null)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn_regular_only = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return int_cloumn_regular_only

    def floor_ceil_round(self):  
        # CEIL:获得指定列的向上取整数的结果   FLOOR:获得指定列的向下取整数的结果      ROUND:获得指定列的四舍五入的结果  
        # 不能应用在 timestamp、binary、nchar、bool 类型字段上；在超级表查询中使用时，不能应用在 tag 列，无论 tag 列的类型是什么类型。
        hanshu = ['FLOOR','CEIL','ROUND']        
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)'] 
        
        hanshu_column1 = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn1 = str(hanshu_column1).replace("[","").replace("]","").replace("'","").replace(", ","")
        hanshu_column2 = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn2 = str(hanshu_column2).replace("[","").replace("]","").replace("'","").replace(", ","")
        calculator = [' + ' , ' - ' ,' * ',' / ']
        
        i = random.randint(1,3)
        if i == 1:
            int_cloumn = int_cloumn1;
        else:
            int_cloumn = int_cloumn1 + str(random.sample(calculator,1)).replace("[","").replace("]","").replace("'","") + int_cloumn2;

        return int_cloumn

    def int_cloumn_csum(self):   
        # csum not support  ' +、-、*、/ '
        hanshu = ['CSUM']      
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)'] 
        
        hanshu_column1 = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn = str(hanshu_column1).replace("[","").replace("]","").replace("'","").replace(", ","")
                    
        return int_cloumn
 
    def int_cloumn_mavg(self):   
        # mavg not support  ' +、-、*、/ ' 
        hanshu = ['MAVG']             
        column = ['(q_bigint,num)','(q_smallint,num)','(q_tinyint,num)','(q_int,num)','(q_float,num)','(q_double,num)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn_mavg = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
                    
        return int_cloumn_mavg

    def all_cloumn_tail(self):   
        hanshu = ['TAIL']             
        column = ['(q_bigint,num1,num2)','(q_smallint,num1,num2)','(q_tinyint,num1,num2)','(q_int,num1,num2)','(q_float,num1,num2)','(q_binary,num1,num2)','(q_nchar,num1,num2)','(q_double,num1,num2)','(q_bool,num1,num2)','(q_ts,num1,num2)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        all_cloumn_tail = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
                    
        return all_cloumn_tail
    
    def all_cloumn_sample(self):   
        hanshu = ['SAMPLE']             
        column = ['(q_bigint,num)','(q_smallint,num)','(q_tinyint,num)','(q_int,num)','(q_float,num)','(q_binary,num)','(q_nchar,num)','(q_double,num)','(q_bool,num)','(q_ts,num)'] 
        
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        all_cloumn_sample = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
                    
        return all_cloumn_sample

    def all_cloumn_unique(self):   
        hanshu = ['UNIQUE']             
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_binary)','(q_nchar)','(q_double)','(q_ts)'] #,'(q_bool)'只有一条，不方便对比
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        all_cloumn_unique = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
                    
        return all_cloumn_unique
 
    def all_cloumn_mode(self):   
        hanshu = ['MODE']             
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_binary)','(q_nchar)','(q_double)','(q_bool)','(q_ts)']
        #'(q_bigint_null)','(q_smallint_null)','(q_tinyint_null)','(q_int_null)','(q_float_null)','(q_binary_null)','(q_nchar_null)','(q_double_null)','(q_bool_null)','(q_ts_null)']        
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        all_cloumn_mode = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
                    
        return all_cloumn_mode

    def int_cloumn_state(self):   
        hanshu = ['statecount','stateduration']
        hanshu_select = random.sample(hanshu,1)
        print(hanshu_select)
                
        column = ['(q_bigint,oper,num,time)','(q_smallint,oper,num,time)','(q_tinyint,oper,num,time)','(q_int,oper,num,time)','(q_float,oper,num,time)','(q_double,oper,num,time)'] 
        hanshu_column = hanshu_select+random.sample(column,1)
        int_cloumn_state = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        operator = ['LT' , 'GT' ,'GE','NE','EQ']  
        oper = str(random.sample(operator,1)).replace("[","").replace("]","")#.replace("'","")        
                         
        if str(hanshu_select).replace("[","").replace("]","").replace("'","") == 'statecount':
            int_cloumn_state = int_cloumn_state.replace("oper","%s" %oper).replace(",time","")
        elif str(hanshu_select).replace("[","").replace("]","").replace("'","") == 'stateduration':
            timeunit = ['1s' , '1m' ,'1h']  
            time = str(random.sample(timeunit,1)).replace("[","").replace("]","").replace("'","") 
            int_cloumn_state = int_cloumn_state.replace("oper","%s" %oper).replace("time","%s" %time) 
                    
        return int_cloumn_state

    def all_cloumn_hyperloglog(self):   
        hanshu = ['HYPERLOGLOG']             
        column = ['(ts)','(_C0)','(_c0)','(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_binary)','(q_nchar)','(q_double)','(q_bool)','(q_ts)',
        '(q_bigint_null)','(q_smallint_null)','(q_tinyint_null)','(q_int_null)','(q_float_null)','(q_binary_null)','(q_nchar_null)','(q_double_null)','(q_bool_null)','(q_ts_null)'] 
        column = ['(ts)','(_C0)','(_c0)','(_rowts)','(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_binary)','(q_nchar)','(q_double)','(q_bool)','(q_ts)']
     
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        all_cloumn_hyperloglog = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
                    
        return all_cloumn_hyperloglog

    def int_cloumn_diff(self):   
        # not support stable, if support should together with groupby tbname.  support all int type \ double type \ 
        # TWA/Diff/Derivative/Irate/CSUM/MAVG/SAMPLE/INTERP/Elapsed are not allowed to apply to super table directly 
        # diff(field_name, ignore_negative)获取给定行的数据相对于前一行的值之间的差。ignore_negative 取值为 0|1 , 可以不填，默认值为 0. 不忽略负值。
        hanshu = ['DIFF']        
        column = ['(q_bigint,num)','(q_smallint,num)','(q_tinyint,num)','(q_int,num)','(q_float,num)','(q_double,num)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn_diff = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        return int_cloumn_diff
                                            
    def func_stable_special(self,i):   
        func_stable_special = ''
        if i == 1:    
            func_stable_special = self.int_cloumn_n()
        elif i == 2:
            func_stable_special = self.only_inter_query()              
        elif i == 3:
            func_stable_special = self.int_cloumn_regular_only()  
        elif i == 31: #TWA
            func_stable_special = self.int_cloumn_regular_only_1() 
        elif i == 32: #IRATE
            func_stable_special = self.int_cloumn_regular_only_2()    
        elif i == 33: #INTERP
            func_stable_special = self.int_cloumn_regular_only_3() 
        elif i == 4:
            func_stable_special = self.floor_ceil_round()
        elif i == 5:
            func_stable_special = self.int_cloumn_csum()
        elif i == 6:
            func_stable_special = self.int_cloumn_mavg()
        elif i == 7:
            func_stable_special = self.all_cloumn_tail()
        elif i == 8:
            func_stable_special = self.all_cloumn_sample()
        elif i == 9:
            func_stable_special = self.all_cloumn_unique()
        elif i == 10:
            func_stable_special = self.all_cloumn_mode()
        elif i == 11:
            func_stable_special = self.int_cloumn_state()
        elif i == 12:
            func_stable_special = self.all_cloumn_hyperloglog()
        elif i == 13:
            func_stable_special = self.int_cloumn_diff()
                                                                         
        return func_stable_special    

    #math
    def int_sin_cos_tan(self):   
        hanshu = ['SIN','COS','TAN','ASIN','ACOS','ATAN']             
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)',
        '(q_bigint_null)','(q_smallint_null)','(q_tinyint_null)','(q_int_null)','(q_float_null)','(q_double_null)']        
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_sin_cos_tan = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        head1 = str(random.sample(hanshu,1)).replace("[","").replace("]","").replace("'","")
        head2 = str(random.sample(hanshu,1)).replace("[","").replace("]","").replace("'","")
        head3 = str(random.sample(hanshu,1)).replace("[","").replace("]","").replace("'","")
        head4 = str(random.sample(hanshu,1)).replace("[","").replace("]","").replace("'","")
        head5 = str(random.sample(hanshu,1)).replace("[","").replace("]","").replace("'","")
        head6 = str(random.sample(hanshu,1)).replace("[","").replace("]","").replace("'","")

        i = random.randint(1,10)
        if i == 1:
            int_sin_cos_tan = int_sin_cos_tan;
        elif i == 2:
            int_sin_cos_tan = head1 + '(' + int_sin_cos_tan + ')';
        elif i == 3:
            int_sin_cos_tan = head2 + '(' + head1 + '(' + int_sin_cos_tan + '))';
        elif i == 4:
            int_sin_cos_tan = head3 + '(' + head2 + '(' + head1 + '(' + int_sin_cos_tan + ')))';
        elif i == 5:
            int_sin_cos_tan = head4 + '(' + head3 + '(' + head2 + '(' + head1 + '(' + int_sin_cos_tan + '))))';
        elif i == 6:
            int_sin_cos_tan = head5 + '(' + head4 + '(' + head3 + '(' + head2 + '(' + head1 + '(' + int_sin_cos_tan + ')))))';
        elif i == 7:
            int_sin_cos_tan = head6 + '(' + head5 + '(' + head4 + '(' + head3 + '(' + head2 + '(' + head1 + '(' + int_sin_cos_tan + '))))))';
        elif i == 8:
            int_sin_cos_tan = head1 + '(' + head6 + '(' + head5 + '(' + head4 + '(' + head3 + '(' + head2 + '(' + head1 + '(' + int_sin_cos_tan + ')))))))';
        elif i == 9:
            int_sin_cos_tan = head5 + '(' + head1 + '(' + head6 + '(' + head5 + '(' + head4 + '(' + head3 + '(' + head2 + '(' + head1 + '(' + int_sin_cos_tan + '))))))))';
                    
        return int_sin_cos_tan

    def int_pow_log(self):   
        hanshu = ['POW','LOG']        
        column = ['(q_bigint,num)','(q_smallint,num)','(q_tinyint,num)','(q_int,num)','(q_float,num)','(q_double,num)',
        '(q_bigint_null,num)','(q_smallint_null,num)','(q_tinyint_null,num)','(q_int_null,num)','(q_float_null,num)','(q_double_null,num)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_pow_log = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        return int_pow_log

    def int_pow_log_interval(self):   
        hanshu = ['POW','LOG']        
        column = ['(q_bigint,num)','(q_smallint,num)','(q_tinyint,num)','(q_int,num)','(q_float,num)','(q_double,num)',
        '(q_bigint_null,num)','(q_smallint_null,num)','(q_tinyint_null,num)','(q_int_null,num)','(q_float_null,num)','(q_double_null,num)'] 
        num = random.randrange(2,100) 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_pow_log = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","").replace("num","%d" %num)
        
        return int_pow_log
    
    def int_abs(self):   
        #因为要校验值》0，所以和sqrt分开
        hanshu = ['ABS']        
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_abs = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        return int_abs
    
    def int_sqrt(self):   
        hanshu = ['SQRT']        
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_sqrt = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        return int_sqrt
    
    def int_histogram(self): 
        # HISTOGRAM(column_name, bin_type, bin_description, normalized)  ,'(q_tinyint)','(q_int)','(q_float)','(q_double)'
        # 后期重写json生成
        hanshu = ['HISTOGRAM']         
        columns = ['(q_bigint','(q_smallint','(q_tinyint','(q_int','(q_float','(q_double'] 
        column = random.sample(columns,1)
        int_histogram = []
        normalized = random.randint(0, 1)
        for i in range(4):
            if i == 1:
                bin_type = 'user_input'                
                bin_description = {-11111119395555977777}  #9一会转译成，
                # self.logger.info(hanshu,column,int_histogram,normalized,bin_description)
                # self.logger.info(type(hanshu),type(column),type(int_histogram),type(normalized),type(bin_description))
                hanshu_column = [hanshu , column, ',',"'%s'" %bin_type, ',',"'%s'" % bin_description, ',', "%d" %normalized,')']
                int_histogram = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","").replace("{","[").replace("}","]").replace("9",",")
                
            elif i == 2:
                bin_type = 'linear_bin'   
                true_false = random.randint(10, 11)             
                bin_description = {"ZstartZ": -333339, "ZwidthZ":559, "ZcountZ":59, "ZinfinityZ":'%d' %true_false}  #Z一会转译成" ，9一会转译成 ，
                hanshu_column = [hanshu , column, ',',"'%s'" %bin_type, ',','%s' % bin_description, ',', "%d" %normalized,')']
                int_histogram = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","").replace("9",",").replace("Z","\"").replace("10","false").replace("11","true").replace("\"{","'{").replace("}\"","}'")
                
            elif i == 3:
                bin_type = 'log_bin'   
                true_false = random.randint(10, 11)             
                bin_description = {"ZstartZ": -333339, "ZfactorZ":559, "ZcountZ":59, "ZinfinityZ":'%d' %true_false}  #Z一会转译成" ，9一会转译成 ，
                hanshu_column = [hanshu , column, ',',"'%s'" %bin_type, ',','%s' % bin_description, ',', "%d" %normalized,')']
                int_histogram = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","").replace("9",",").replace("Z","\"").replace("10","false").replace("11","true").replace("\"{","'{").replace("}\"","}'")                
        
        return int_histogram
        

    def int_percentile(self):   
        #不能用在超级表,num值取值范围0≤num≤100，为0的时候等同于MIN，为100的时候等同于MAX。
        hanshu = ['PERCENTILE']        
        column = ['(q_bigint,num)','(q_smallint,num)','(q_tinyint,num)','(q_int,num)','(q_float,num)','(q_double,num)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_percentile = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        return int_percentile

    def int_percentile_interval(self):   
        #不能用在超级表,num值取值范围0≤num≤100，为0的时候等同于MIN，为100的时候等同于MAX。
        hanshu = ['PERCENTILE']        
        column = ['(q_bigint,num)','(q_smallint,num)','(q_tinyint,num)','(q_int,num)','(q_float,num)','(q_double,num)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        num = random.randrange(0,100) 
        int_percentile = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","").replace("num","%d" %num)
        
        return int_percentile
    
    def int_apercentile(self):  
        #APERCENTILE(field_name, P[, algo_type])  P值有效取值范围0≤P≤100，为 0 的时候等同于 MIN，为 100 的时候等同于MAX；
        #algo_type的有效输入：default 和 t-digest。 不提供第三个参数的输入，此时将使用 default 的算法进行计算，即 apercentile(column_name, 50, "default") 与 apercentile(column_name, 50) 等价。
        hanshu = ['APERCENTILE']        
        column = ['(q_bigint,num)','(q_smallint,num)','(q_tinyint,num)','(q_int,num)','(q_float,num)','(q_double,num)',
        '(q_bigint,num,algo_type)','(q_smallint,num,algo_type)','(q_tinyint,num,algo_type)','(q_int,num,algo_type)','(q_float,num,algo_type)','(q_double,num,algo_type)'] 
        for i in range(3):
            if i == 1:
                algo_type = 'default'                
                hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
                int_apercentile = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","").replace("algo_type",'"%s"'%algo_type)
            elif i == 2:
                algo_type = 't-digest'                
                hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
                int_apercentile = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","").replace("algo_type",'"%s"'%algo_type)                
        
        return int_apercentile
        
    
    def int_apercentile_interval(self):  
        #APERCENTILE(field_name, P[, algo_type])  P值有效取值范围0≤P≤100，为 0 的时候等同于 MIN，为 100 的时候等同于MAX；
        #algo_type的有效输入：default 和 t-digest。 不提供第三个参数的输入，此时将使用 default 的算法进行计算，即 apercentile(column_name, 50, "default") 与 apercentile(column_name, 50) 等价。
        hanshu = ['APERCENTILE']        
        column = ['(q_bigint,num)','(q_smallint,num)','(q_tinyint,num)','(q_int,num)','(q_float,num)','(q_double,num)',
        '(q_bigint,num,algo_type)','(q_smallint,num,algo_type)','(q_tinyint,num,algo_type)','(q_int,num,algo_type)','(q_float,num,algo_type)','(q_double,num,algo_type)'] 
        num = random.randrange(0,100) 
        for i in range(3):
            if i == 1:
                algo_type = 'default'                
                hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
                int_apercentile = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","").replace("algo_type",'"%s"'%algo_type).replace("num","%d" %num)
            elif i == 2:
                algo_type = 't-digest'                
                hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
                int_apercentile = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","").replace("algo_type",'"%s"'%algo_type).replace("num","%d" %num)                
        
        return int_apercentile
        
    def int_leastsquares(self):   
        #不能用在超级表,统计表中某列的值是主键（时间戳）的拟合直线方程。start_val是自变量初始值，step_val是自变量的步长值。
        hanshu = ['LEASTSQUARES']        
        column = ['(q_bigint,start_val,step_val)','(q_smallint,start_val,step_val)','(q_tinyint,start_val,step_val)','(q_int,start_val,step_val)','(q_float,start_val,step_val)','(q_double,start_val,step_val)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_leastsquares = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        return int_leastsquares

    def int_derivative(self):   
        hanshu = ['DERIVATIVE']                
        column = ['(q_bigint,time_interval,ignore_negative)','(q_smallint,time_interval,ignore_negative)','(q_tinyint,time_interval,ignore_negative)','(q_int,time_interval,ignore_negative)','(q_float,time_interval,ignore_negative)','(q_double,time_interval,ignore_negative)'] 
        hanshu_column = random.sample(hanshu,1) + random.sample(column,1)
        int_cloumn_state = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        #time_intervals = ['1s', '1m' ,'1h', '1d']         # derivative duration should be greater than 1 Second  
        time_units = ['nums','numm','numh','numd']      
        time_interval = str(random.sample(time_units,1)).replace("[","").replace("]","").replace("'","")  
        
        time_num = random.randint(1, 1000)  
        int_derivative = int_cloumn_state.replace("time_interval","%s" %time_interval).replace("num","%d" %time_num)   
                    
        return int_derivative

    def int_derivative_interval(self):   
        hanshu = ['DERIVATIVE']                
        column = ['(q_bigint,time_interval)','(q_smallint,time_interval)','(q_tinyint,time_interval)','(q_int,time_interval)','(q_float,time_interval)','(q_double,time_interval)'] 
        hanshu_column = random.sample(hanshu,1) + random.sample(column,1)
        int_cloumn_state = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        #time_intervals = ['1s', '1m' ,'1h', '1d']         # derivative duration should be greater than 1 Second  
        time_units = ['nums','numm','numh','numd']      
        time_interval = str(random.sample(time_units,1)).replace("[","").replace("]","").replace("'","")  
        
        time_num = random.randint(1, 1000)  
        int_derivative = int_cloumn_state.replace("time_interval","%s" %time_interval).replace("num","%d" %time_num)   
                    
        return int_derivative
                                     
    def func_stable_math(self,i):   
        #后期可以结合numpy去验证数据
        func_stable_math = ''
        if i == 1:    
            func_stable_math = self.int_sin_cos_tan()
        elif i == 2:
            func_stable_math = self.int_pow_log() 
        elif i == 21:
            func_stable_math = self.int_pow_log_interval() 
        elif i == 3:
            func_stable_math = self.int_abs() 
        elif i == 4:
            func_stable_math = self.int_sqrt() 
        elif i == 5:
            func_stable_math = self.int_histogram() 
        elif i == 6:
            func_stable_math = self.int_percentile() 
        elif i == 61:
            func_stable_math = self.int_percentile_interval() 
        elif i == 7:
            func_stable_math = self.int_apercentile() 
        elif i == 71:
            func_stable_math = self.int_apercentile_interval() 
        elif i == 8:
            func_stable_math = self.int_leastsquares() 
        elif i == 9:
            func_stable_math = self.int_derivative() 
        elif i == 91:
            func_stable_math = self.int_derivative_interval() 
                                                                         
        return func_stable_math 

    #str    
    def str_upper(self):   
        hanshu = ['UPPER']      
        column = ['(q_nchar)','(q_binary)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        str_upper = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        return str_upper
    
    def str_lower(self):   
        hanshu = ['LOWER']      
        column = ['(q_nchar)','(q_binary)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        str_lower = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        return str_lower
    
    def str_ltrim(self):   
        hanshu = ['LTRIM']      
        column = ['(q_nchar)','(q_binary)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        str_rtrim = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        return str_rtrim
        
    def str_rtrim(self):   
        hanshu = ['RTRIM']      
        column = ['(q_nchar)','(q_binary)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        str_rtrim = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        return str_rtrim
        
    def str_base(self): 
        #np不支持对null值的处理，因此此脚本需要去掉对np的校验，增加对null值的验证  
        hanshu = ['LOWER','UPPER','LTRIM','RTRIM']      
        column = ['(q_nchar)','(q_binary)','(q_nchar_null)','(q_binary_null)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        str_upper_lower = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        return str_upper_lower

    def str_length(self):   
        hanshu = ['LENGTH','CHAR_LENGTH']      
        column = ['(q_nchar)','(q_binary)','(q_nchar_null)','(q_binary_null)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        str_length = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        return str_length
 
    def str_substr(self):   
        hanshu = ['SUBSTR']      
        column = ['(q_nchar, pos)','(q_binary, pos)','(q_nchar_null, pos)','(q_binary_null, pos)',
        '(q_nchar, pos, len)','(q_binary, pos, len)','(q_nchar_null, pos, len)','(q_binary_null, pos, len)']
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        str_substr = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", (","(")
            
        return str_substr
 
    def str_substr_interval(self):   
        hanshu = ['SUBSTR']      
        column = ['(q_nchar, pos)','(q_binary, pos)','(q_nchar_null, pos)','(q_binary_null, pos)',
        '(q_nchar, pos, len)','(q_binary, pos, len)','(q_nchar_null, pos, len)','(q_binary_null, pos, len)']
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        
        pos_list = (-10,-9,-8,-7,-6,-5,-4,-3,-2,-1,1,2,3,4,5,6,7,8,9,10);
        substr_pos,substr_len = random.choice(pos_list) , random.randrange(0,5) 
        str_substr = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", (","(").replace("pos","%d" %substr_pos).replace("len","%d" %substr_len)
            
        return str_substr
        
    def str_concat_nchar(self):   
        hanshu = ['CONCAT']  
        i = random.randint(2,8)
        #null（q_nchar_null列）和第三方对比时无法处理，因此不放进来
        columns = ['q_nchar','q_nchar1','q_nchar2','q_nchar3','q_nchar4','q_nchar5','q_nchar6','q_nchar7','q_nchar8']     
        column = str(random.sample(columns,i)).replace("[","").replace("]","").replace("'","") 
        hanshu_column = str(random.sample(hanshu,1))+'('+column+')'
        str_concat_nchar = str(hanshu_column).replace("[","").replace("]","").replace("'","")
            
        return str_concat_nchar
    
    def str_concat_binary(self):   
        hanshu = ['CONCAT']  
        i = random.randint(2,8)
        columns = ['q_binary','q_binary1','q_binary2','q_binary3','q_binary4','q_binary5','q_binary6','q_binary7','q_binary8']     
        column = str(random.sample(columns,i)).replace("[","").replace("]","").replace("'","") 
        hanshu_column = str(random.sample(hanshu,1))+'('+column+')'
        str_concat_binary = str(hanshu_column).replace("[","").replace("]","").replace("'","")
            
        return str_concat_binary
    
    def str_concat_ws_nchar(self):   
        hanshu = ['CONCAT_WS']  
        i = random.randint(2,8)
        #null（q_nchar_null列）和第三方对比时无法处理，因此不放进来
        columns = ['q_nchar','q_nchar1','q_nchar2','q_nchar3','q_nchar4','q_nchar5','q_nchar6','q_nchar7','q_nchar8']     
        column = str(random.sample(columns,i)).replace("[","").replace("]","").replace("'","") 
        separators = ['',' ','abc','123','!','@','#','$','%','^','&','*','(',')','-','_','+','=','{',
                      '[','}',']','|',';',':',',','.','<','>','?','/','~','`','taos','涛思']
        separator = str(random.sample(separators,i)).replace("[","").replace("]","") 
        hanshu_column = str(random.sample(hanshu,1))+'('+'\"'+separator+'\",'+column+')'
        str_concat_ws_nchar = str(hanshu_column).replace("[","").replace("]","").replace("'","")
            
        return str_concat_ws_nchar
    
    def str_concat_ws_binary(self):   
        hanshu = ['CONCAT_WS']  
        i = random.randint(2,8)
        columns = ['q_binary','q_binary1','q_binary2','q_binary3','q_binary4','q_binary5','q_binary6','q_binary7','q_binary8']     
        column = str(random.sample(columns,i)).replace("[","").replace("]","").replace("'","") 
        separators = ['',' ','abc','123','!','@','#','$','%','^','&','*','(',')','-','_','+','=','{',
                      '[','}',']','|',';',':',',','.','<','>','?','/','~','`','taos','涛思']
        separator = str(random.sample(separators,i)).replace("[","").replace("]","") 
        hanshu_column = str(random.sample(hanshu,1))+'('+'\"'+separator+'\",'+column+')'
        str_concat_ws_binary = str(hanshu_column).replace("[","").replace("]","").replace("'","")
            
        return str_concat_ws_binary

    def str_cast(self):   
        hanshu = ['CAST']  
        for i in range(1,5):
            if i ==1:
                column = ['q_bool','q_bool_null','q_bigint','q_bigint_null','q_smallint','q_smallint_null',
                'q_tinyint','q_tinyint_null','q_int','q_int_null','q_float','q_float_null','q_double','q_double_null']
                type_names = ['BIGINT','BINARY(300)','TIMESTAMP','NCHAR(300)','BIGINT UNSIGNED']
                type_name = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
                hanshu_column = str(random.sample(hanshu,1))+'('+str(random.sample(column,1))+' AS '+type_name+')'
                str_cast = str(hanshu_column).replace("[","").replace("]","").replace("'","")
            elif i==2:
                column = ['q_binary','q_binary_null']
                type_names = ['BIGINT','BINARY(300)','NCHAR(300)','BIGINT UNSIGNED']
                type_name = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
                hanshu_column = str(random.sample(hanshu,1))+'('+str(random.sample(column,1))+' AS '+type_name+')'
                str_cast = str(hanshu_column).replace("[","").replace("]","").replace("'","")
            elif i==3:
                column = ['q_nchar','q_nchar_null']
                type_names = ['BIGINT','NCHAR(300)','BIGINT UNSIGNED']
                type_name = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
                hanshu_column = str(random.sample(hanshu,1))+'('+str(random.sample(column,1))+' AS '+type_name+')'
                str_cast = str(hanshu_column).replace("[","").replace("]","").replace("'","")                
            elif i==4:
                column = ['q_ts','q_ts_null','_C0','_c0']
                type_names = ['BIGINT','TIMESTAMP','BIGINT UNSIGNED']
                type_name = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
                hanshu_column = str(random.sample(hanshu,1))+'('+str(random.sample(column,1))+' AS '+type_name+')'
                str_cast = str(hanshu_column).replace("[","").replace("]","").replace("'","")  
                            
        return str_cast

                                                       
    def func_stable_str(self,i):  
        func_stable_str = ''
        if i == 1:    
            func_stable_str = self.str_base()
        elif i == 2:
            func_stable_str = self.str_upper() 
        elif i == 3:
            func_stable_str = self.str_lower() 
        elif i == 4:
            func_stable_str = self.str_ltrim() 
        elif i == 5:
            func_stable_str = self.str_rtrim()    
        elif i == 6:
            func_stable_str = self.str_length()  
        elif i == 7:
            func_stable_str = self.str_substr()  
        elif i == 71:
            func_stable_str = self.str_substr_interval()   
        elif i == 8:
            func_stable_str = self.str_concat_nchar()   
        elif i == 9:
            func_stable_str = self.str_concat_binary()  
        elif i == 10:
            func_stable_str = self.str_concat_ws_nchar()   
        elif i == 11:
            func_stable_str = self.str_concat_ws_binary()  
        elif i == 12:
            func_stable_str = self.str_cast()                                               
                                             
        return func_stable_str 

    def time_now(self):   
        hanshu = ['NOW']      
        column = ['()'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        time_now = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        return time_now

    def time_today(self):   
        hanshu = ['TODAY']      
        column = ['()'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        time_today = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        return time_today    

    def time_zone(self):   
        hanshu = ['TIMEZONE']      
        column = ['()'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        time_zone = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        return time_zone   

    def time_to_iso8601(self):   
        hanshu = ['TO_ISO8601']      
        column = ['(now())','(ts)','(q_ts)','(_c0)','(_C0)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        time_to_iso8601 = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        return time_to_iso8601   
    def time_to_iso8601_1(self):   
        hanshu = ['TO_ISO8601']      
        column = ['(now())'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        time_to_iso8601 = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        return time_to_iso8601  
    
    def time_to_unixtimestamp(self):   
        hanshu = ['TO_UNIXTIMESTAMP']  
        #增加日期时间字符串须符合 ISO8601/RFC3339 标准，无法转换的字符串格式将返回0。
        t = time.time()  
        t_to_s =  time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t))     
        column = ['(q_nchar)','(q_binary)','(t_to_s)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        time_to_unixtimestamp = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","").replace("\"","").replace("t_to_s","'t_to_s'")
        time_to_unixtimestamp = str(time_to_unixtimestamp).replace("t_to_s","%s" %t_to_s)
        
        return time_to_unixtimestamp  
    def time_to_unixtimestamp_1(self):   
        hanshu = ['TO_UNIXTIMESTAMP']  
        #增加日期时间字符串须符合 ISO8601/RFC3339 标准，无法转换的字符串格式将返回0。
        t = time.time()  
        t_to_s =  time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t))     
        column = ['(t_to_s)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        time_to_unixtimestamp = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","").replace("\"","").replace("t_to_s","'t_to_s'")
        time_to_unixtimestamp = str(time_to_unixtimestamp).replace("t_to_s","%s" %t_to_s)
        
        return time_to_unixtimestamp  
        
    def time_truncate(self):  
        #TIMETRUNCATE(ts_val | datetime_string | ts_col, time_unit) 
        hanshu = ['TIMETRUNCATE'] 
        t = time.time()  
        t_to_s =  time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t)) 
        column_select = ['q_ts','ts','_c0','_C0','1600000000000','1600000000000000','1600000000000000000',
        '%d' %t, '%d000' %t, '%d000000' %t,'t_to_s']    
        column_1 = random.sample(column_select,1)
        column = ['(%s,timeutil)'%(column_1)]
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        time_truncate = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","").replace("\"","").replace("t_to_s","'t_to_s'")
        
        timeunits = ['1a' ,'1s', '1m' ,'1h', '1d']  #暂时去掉 1u
        timeunit = str(random.sample(timeunits,1)).replace("[","").replace("]","").replace("'","") 
            
        time_truncate = str(time_truncate).replace("timeutil","%s" %timeunit).replace("t_to_s","%s" %t_to_s) 
        
        return time_truncate          
    def time_truncate_1(self):  
        #TIMETRUNCATE(ts_val | datetime_string | ts_col, time_unit) 
        hanshu = ['TIMETRUNCATE'] 
        t = time.time()  
        t_to_s =  time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t)) 
        column_select = ['1600000000000','1600000000000000','1600000000000000000',
        '%d' %t, '%d000' %t, '%d000000' %t,'t_to_s']    
        column_1 = random.sample(column_select,1)
        column = ['(%s,timeutil)'%(column_1)]
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        time_truncate = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","").replace("\"","").replace("t_to_s","'t_to_s'")
        
        timeunits = ['1a' ,'1s', '1m' ,'1h', '1d']  #暂时去掉 1u
        timeunit = str(random.sample(timeunits,1)).replace("[","").replace("]","").replace("'","") 
            
        time_truncate = str(time_truncate).replace("timeutil","%s" %timeunit).replace("t_to_s","%s" %t_to_s) 
        
        return time_truncate  
    
    def time_diff_1(self):  
        #TIMEDIFF(ts_val1 | datetime_string1 | ts_col1, ts_val2 | datetime_string2 | ts_col2 [, time_unit]) 
        hanshu = ['TIMEDIFF'] 
        #增加指定格式的时间 ts_val
        t = time.time()          
        column_select = ['q_ts','ts','_c0','_C0','1600000000000','1600000000000000','1600000000000000000',
        '%d' %t, '%d000' %t, '%d000000' %t,'t_to_s'] 
        column_1,column_2 = random.sample(column_select,1),random.sample(column_select,1)
        column = ['(%s,%s,timeutil)'%(column_1,column_2)]
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        time_diff = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","").replace("\"","").replace("t_to_s","'t_to_s'")
        #增加字符串格式的时间 datetime_string
        t_to_s =  time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t)) 
        timeunits = ['1a' ,'1s', '1m' ,'1h', '1d']   #暂时去掉 1u
        timeunit = str(random.sample(timeunits,1)).replace("[","").replace("]","").replace("'","") 
            
        time_diff_1 = str(time_diff).replace("timeutil","%s" %timeunit).replace("t_to_s","%s" %t_to_s)   
        
        return time_diff_1  
    def time_diff_1_1(self):  
        #TIMEDIFF(ts_val1 | datetime_string1 | ts_col1, ts_val2 | datetime_string2 | ts_col2 [, time_unit]) 
        hanshu = ['TIMEDIFF'] 
        #增加指定格式的时间 ts_val
        t = time.time()          
        column_select = ['1600000000000','1600000000000000','1600000000000000000',
        '%d' %t, '%d000' %t, '%d000000' %t,'t_to_s'] 
        column_1,column_2 = random.sample(column_select,1),random.sample(column_select,1)
        column = ['(%s,%s,timeutil)'%(column_1,column_2)]
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        time_diff = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","").replace("\"","").replace("t_to_s","'t_to_s'")
        #增加字符串格式的时间 datetime_string
        t_to_s =  time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t)) 
        timeunits = ['1a' ,'1s', '1m' ,'1h', '1d']   #暂时去掉 1u
        timeunit = str(random.sample(timeunits,1)).replace("[","").replace("]","").replace("'","") 
            
        time_diff_1 = str(time_diff).replace("timeutil","%s" %timeunit).replace("t_to_s","%s" %t_to_s)   
        
        return time_diff_1  
    
    def time_diff_2(self): 
        #TIMEDIFF(ts_val1 | datetime_string1 | ts_col1, ts_val2 | datetime_string2 | ts_col2) 
        hanshu = ['TIMEDIFF']  
        t = time.time()  
        column_select = ['q_ts','ts','_c0','_C0','1600000000000','1600000000000000','1600000000000000000',
        '%d' %t, '%d000' %t, '%d000000' %t,'t_to_s']
        column_1,column_2 = random.sample(column_select,1),random.sample(column_select,1)
        column = ['(%s,%s)'%(column_1,column_2)]
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        time_diff = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","").replace("\"","").replace("t_to_s","'t_to_s'")
        #增加字符串格式的时间   datetime_string              
        t_to_s =  time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t)) 
        time_diff_2 = str(time_diff).replace("t_to_s","%s" %t_to_s) 
        
        return time_diff_2  
    def time_diff_2_1(self): 
        #TIMEDIFF(ts_val1 | datetime_string1 | ts_col1, ts_val2 | datetime_string2 | ts_col2) 
        hanshu = ['TIMEDIFF']  
        t = time.time()  
        column_select = ['1600000000000','1600000000000000','1600000000000000000',
        '%d' %t, '%d000' %t, '%d000000' %t,'t_to_s']
        column_1,column_2 = random.sample(column_select,1),random.sample(column_select,1)
        column = ['(%s,%s)'%(column_1,column_2)]
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        time_diff = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","").replace("\"","").replace("t_to_s","'t_to_s'")
        #增加字符串格式的时间   datetime_string              
        t_to_s =  time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t)) 
        time_diff_2 = str(time_diff).replace("t_to_s","%s" %t_to_s) 
        
        return time_diff_2  
     
    def time_elapsed(self):   
        #ELAPSED(ts_primary_key [, time_unit])
        hanshu = ['ELAPSED'] 
        column = ['(ts)','(_c0)','(_C0)','(ts,time_unit)','(_c0,time_unit)','(_C0,time_unit)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
          
        # time_units = ['nums','numm','numh','numd','numa']   # ELAPSED function time unit parameter should be one of the following: [1b, 1u, 1a, 1s, 1m, 1h, 1d, 1w]   
        # time_unit = str(random.sample(time_units,1)).replace("[","").replace("]","").replace("'","")          
        # time_num = random.randint(0, 1000)  
        # time_unit = time_unit.replace("num","%d" %time_num)      
        
        time_units = ['1s','1m','1h','1d','1a']       #暂时去掉 1u
        time_unit = str(random.sample(time_units,1)).replace("[","").replace("]","").replace("'","")   
        
        time_elapsed = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","").replace("time_unit","%s" %time_unit)
        
        return time_elapsed   
                                                       
    def func_stable_time(self,i):  
        func_stable_time = ''
        if i == 1:    
            func_stable_time = self.time_now()
        elif i == 2:
            func_stable_time = self.time_today() 
        elif i == 3:
            func_stable_time = self.time_zone() 
        elif i == 4:
            func_stable_time = self.time_to_iso8601()
        elif i == 41:
            func_stable_time = self.time_to_iso8601_1()  
        elif i == 5:
            func_stable_time = self.time_to_unixtimestamp()  
        elif i == 51:
            func_stable_time = self.time_to_unixtimestamp_1()   
        elif i == 6:
            func_stable_time = self.time_truncate()  
        elif i == 61:
            func_stable_time = self.time_truncate_1()
        elif i == 7:
            func_stable_time = self.time_diff_1()   
        elif i == 8:
            func_stable_time = self.time_diff_2()   
        elif i == 71:
            func_stable_time = self.time_diff_1_1()   
        elif i == 81:
            func_stable_time = self.time_diff_2_1()             
        elif i == 9:
            func_stable_time = self.time_elapsed()   
                                                                                        
        return func_stable_time 
    
                             
    # error      
    def int_min_max_error(self):  
        # not support all int type \ double type \        
        hanshu = ['MIN','MAX']      
        column = ['(*)','(q_bool)','(q_binary)','(q_nchar)','(q_ts)','(ts)','(_c0)','(_C0)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn_error = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        return int_cloumn_error  
    
    def int_cloumn_error(self):  
        # not support all int type \ double type \        
        hanshu = ['AVG','SUM','CEIL','FLOOR','ROUND']      
        column = ['(*)','(_c0)','(_C0)','(q_ts)','(q_bool)','(q_binary)','(q_nchar)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn_error = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        return int_cloumn_error  

    def int_cloumn_error_n(self):  
        # not support all int type \ double type  
        # int parameter is out of range [1, 100]            
        hanshu = ['TOP','BOTTOM']        
        column = ['(q_bigint,0)','(q_smallint,101)','(*,1)','(_c0,20)','(_C0,40)','(q_ts,50)','(q_bool,60)','(q_binary,80)','(q_nchar,100)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        return int_cloumn

    def int_ts_cloumn_error(self):  
        # not support all int type \ double type \ ts type        
        hanshu = ['SPREAD']       
        column = ['(*)','(q_bool)','(q_binary)','(q_nchar)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_ts_cloumn = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        return int_ts_cloumn

    def only_inter_query_2(self):    
        # not support stddev/percentile in the outer query 
        # functions not support for super table query
        hanshu = ['PERCENTILE']      
        column = ['(q_bigint,0)','(q_smallint,20)','(q_tinyint,40)','(q_int,60)','(q_float,80)','(q_double,100)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        only_inter_query = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        return only_inter_query 

    def int_cloumn_regular_only_error_0(self):   
        # not support stable, if support should together with groupby tbname.  support all int type \ double type \ 
        # TWA/Diff/Derivative/Irate/CSUM/MAVG/SAMPLE/INTERP/Elapsed are not allowed to apply to super table directly 
        hanshu = ['IRATE','INTERP']  #3.0 support ,'DIFF','TWA','CSUM'
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn_regular_only = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        return int_cloumn_regular_only
        
    def int_cloumn_regular_only_error_1(self):   
        # not support stable,  support all int type \ double type \ 
        hanshu = ['LEASTSQUARES']        
        column = ['(q_bigint,1,1)','(q_smallint,10,10)','(q_tinyint,100,100)','(q_int,1,10)','(q_float,10,100)','(q_double,1,100)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn_regular_only = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        return int_cloumn_regular_only    
    
    def func_stable_error_all(self,i):   
        func_stable_error_all = ''
        if i == 0:    
            func_stable_error_all = self.int_min_max_error()
        if i == 1:    
            func_stable_error_all = self.int_cloumn_error()
        elif i == 2:
            func_stable_error_all = self.int_cloumn_error_n()
        elif i == 3:
            func_stable_error_all = self.int_ts_cloumn_error()        
        elif i == 4:
            func_stable_error_all = self.only_inter_query_2()
        elif i == 5:
            func_stable_error_all = self.int_cloumn_regular_only_error_0()
        elif i == 6:
            func_stable_error_all = self.int_cloumn_regular_only_error_1()

        return func_stable_error_all
   
    

    # not test
    def int1_cloumn_other(self):   
        hanshu = ['']
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return int_cloumn        


tdFunction = TDFunction()