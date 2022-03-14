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
# import os
# import itertools
from itertools import product
from itertools import combinations
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
        hanshu = ['COUNT','FIRST','LAST','LAST_ROW']
        column = ['(*)','(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)','(_c0)','(_C0)','(q_ts)','(q_bool)','(q_binary)','(q_nchar)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        all_column = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return all_column
    
    def int_ts_cloumn(self):  
        # support all int type \ double type \ ts type        
        hanshu = ['SPREAD']       
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)','(_c0)','(_C0)','(q_ts)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_ts_cloumn = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return int_ts_cloumn
    
    def int_cloumn(self):  
        # support all int type \ double type              
        hanshu = ['AVG','SUM','MIN','MAX','SPREAD','CEIL','FLOOR','ROUND']        
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return int_cloumn

    def int_cloumn_n(self):  
        # support all int type \ double type              
        hanshu = ['TOP','BOTTOM']        
        column = ['(q_bigint,1)','(q_smallint,20)','(q_tinyint,40)','(q_int,60)','(q_float,80)','(q_double,100)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return int_cloumn

    def int_cloumn_regular_only(self):  
        # diff 不能和order by使用   'DIFF',  
        # not support stable, if support should together with groupby tbname.  support all int type \ double type \ 
        hanshu = ['TWA','IRATE','SPREAD','CEIL','FLOOR','ROUND']        
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn_regular_only = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return int_cloumn_regular_only

    def int_cloumn_regular_only_1(self):   
        # not support stable,  support all int type \ double type \ 
        hanshu = ['LEASTSQUARES']        
        column = ['(q_bigint,1,1)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn_regular_only = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return int_cloumn_regular_only

    def int_cloumn_stable_groupby(self):    
        # not support stable, if support should together with groupby tbname.  support all int type \ double type \    
        hanshu = ['TWA','IRATE','DIFF','SPREAD','CEIL','FLOOR','ROUND']        
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn_stable_groupby = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return int_cloumn_stable_groupby    

    def only_inter_query(self):    
        # not support stddev/percentile/interp in the outer query    
        hanshu = ['STDDEV','INTERP']  # TD-13412
        hanshu = ['STDDEV']      
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        only_inter_query = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return only_inter_query  

    def only_inter_query_2(self):    
        # not support stddev/percentile/interp in the outer query 
        hanshu = ['PERCENTILE']      
        column = ['(q_bigint,0)','(q_smallint,20)','(q_tinyint,40)','(q_int,60)','(q_float,80)','(q_double,100)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        only_inter_query = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return only_inter_query  

    def int1_cloumn_other(self):    
        # # order by not supported in nested interp query   
        hanshu = ['LEASTSQUARES','DERIVATIVE',' APERCENTILE']
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return int_cloumn  

    def int_cloumn_error(self):  
        # not support all int type \ double type \        
        hanshu = ['AVG','SUM','MIN','MAX','CEIL','FLOOR','ROUND']      
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

    def func_regular_all(self,i):   
        func_regular_all = ''
        if i == 1:    
            func_regular_all = self.all_column()
        elif i == 2:
            func_regular_all = self.int_cloumn()
        elif i == 3:
            func_regular_all = self.int_cloumn_regular_only()
        elif i == 5:
            func_regular_all = self.int_cloumn_regular_only_1()
        elif i == 4:
            func_regular_all = self.int_ts_cloumn()

        return func_regular_all

    def func_regular_special(self,i):   
        func_regular_special = ''
        if i == 1:    
            func_regular_special = self.only_inter_query()
        elif i == 2:
            func_regular_special = self.only_inter_query_2()
        # elif i == 3:
        #     func_regular_special = self.int_cloumn_regular_only()
        # elif i == 4:
        #     func_regular_special = self.int_ts_cloumn()
        elif i == 11:
            func_regular_special = self.int_cloumn_n()

        return func_regular_special

    def func_regular_error_all(self,i):   
        func_regular_error_all = ''
        if i == 1:    
            func_regular_error_all = self.int_cloumn_error()
        elif i == 2:
            func_regular_error_all = self.int_cloumn_error_n()
        elif i == 3:
            func_regular_error_all = self.int_ts_cloumn_error()

        return func_regular_error_all

    def func_stable_all(self,i):   
        func_stable_all = ''
        if i == 1:    
            func_stable_all = self.all_column()
        elif i == 2:
            func_stable_all = self.int_cloumn()
        elif i == 3:
            func_stable_all = self.int_cloumn_regular_only()
        elif i == 4:
            func_stable_all = self.int_ts_cloumn()

        return func_stable_all
    
    def func_stable_error_all(self,i):   
        func_stable_error_all = ''
        if i == 1:    
            func_stable_error_all = self.int_cloumn_error()
        elif i == 2:
            func_stable_error_all = self.int_cloumn_error()

        return func_stable_error_all



tdFunction = TDFunction()