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
from Query.queryutil.createdata import *
from Query.queryutil.where import *
from Query.queryutil.stable_func import *
from itertools import product
from itertools import combinations
import subprocess

from taostest import TDCase

class TDTestQuery(TDCase):
    def init(self):
        super(TDTestQuery, self).init()
        self.tdCreateData = TDCreateData(self.tdSql, self.logger)
        
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
        case1:# support all int type \ double type  [hanshu = ['INTERP']]
        case2:
        '''
        return case_description

    #basic_param
    db = "stable_interp_0"
    db_1 = "stable_interp_1"
    db_2 = "stable_interp_2"
    
    table_list = ['stable_1','stable_2',]
    table = str(random.sample(table_list,1)).replace("[","").replace("]","").replace("'","")
    table_null_list = ['stable_null_data','stable_null_childtable']
    table_null = str(random.sample(table_null_list,1)).replace("[","").replace("]","").replace("'","")
    testcasePath = os.path.split(__file__)[0]
    testcaseFilename = os.path.split(__file__)[-1]

    # def case_common(self):
    #     #os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))    
    #     os.system("touch %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
    #     self.tdCreateData.dropandcreateDB_random("%s" % self.db, 1) 

    #     # conn1 = taos.connect(host="127.0.0.1", user="root", password="taosdata", config="/etc/taos/")
    #     conn1 = taos.connect(host="%s" %self.service_host, user="root", password="taosdata", config="/etc/taos/")
    #     cur1 = conn1.cursor()        
    #     cur1.execute('use %s;' %self.db)
    #     sql = 'select * from regular_table_1 limit 5;'
    #     cur1.execute(sql)

    #     return(conn1,cur1)  
    
    def data_create(self,db):
        #os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))    
        os.system("touch %s/%s.sql" % (self.testcasePath,self.testcaseFilename))  
        self.tdCreateData.dropandcreateDB_random("%s" % db, 1) 
         
    def right_case_1(self):
        self.logger.info("\n==========================right case 1==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         
                           
        # 1: support all int type \ double type  [hanshu = ['INTERP']]
        for i in (33,):
            func = tdFunction.func_stable_special(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)              

                self.logger.info("\n\n\n=======hanshu num = %d======right case========case1======\n\n\n" %i)
                
                stable_where = tdWhere.regular_where()
                sql1 = 'select %s from %s;'  % (func,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s;" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s);" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where %s %s %s;" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

            except Exception as e:
                raise e   

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        self.logger.info("sqlnum1 %d" % num1) 
        cur1.close()
        conn1.close() 
        
    def interp_range_fill_every(self,i):
        
        interval_n, offset_n, sliding_n, every_n = [random.randrange(10,20)]  , [random.randrange(1,10)] , [random.randrange(1,10)]  , [random.randrange(1,30)] 
        range_fill_every = ''
                
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
        
        
        #单fill
        fill_random_num = random.randint(-100000000,100000000)
        fill_random_nu2 = random.randint(-100000000,100000000)
        fill_random_nu3 = random.randint(-100000000,100000000)
        fills = ['VALUE,100','VALUE,fill_random_num','PREV','NULL','LINEAR','NEXT',
                'VALUE,fill_random_num + (fill_random_nu2)','VALUE,fill_random_num + (fill_random_nu2) + (fill_random_nu3)','VALUE,fill_random_num + (fill_random_nu2) + (fill_random_nu3)',
                'VALUE,fill_random_num - (fill_random_nu2)','VALUE,fill_random_num - (fill_random_nu2) * (fill_random_nu3)','VALUE,fill_random_num - (fill_random_nu2) - (fill_random_nu3)',
                'VALUE,fill_random_num * (fill_random_nu2)','VALUE,fill_random_num * (fill_random_nu2) / (fill_random_nu3)','VALUE,fill_random_num * (fill_random_nu2) * (fill_random_nu3)',
                'VALUE,fill_random_num / (fill_random_nu2)','VALUE,fill_random_num / (fill_random_nu2) * (fill_random_nu3)','VALUE,fill_random_num * (fill_random_nu2) / (fill_random_nu3)']#'NONE',
        fill_base = str(random.sample(fills,1)).replace("[","").replace("]","").replace("'","").replace(", ","").replace("fill_random_num",str(fill_random_num)).replace("fill_random_nu2",str(fill_random_nu2)).replace("fill_random_nu3",str(fill_random_nu3))
        single_fill = 'Fill' +'(' +fill_base + ')'

        #单every
        everys = ['nums','numm','numh','numd','numw','numn','numy','numa']
        every_base = str(random.sample(everys,1)).replace("num",'%s' %every_n).replace("[","").replace("]","").replace("'","").replace(", ","")
        single_every = 'every' +'(' +every_base + ')'
        
        everys = ['numn','numy']
        every_base = str(random.sample(everys,1)).replace("num",'%s' %every_n).replace("[","").replace("]","").replace("'","").replace(", ","")
        single_every_error = 'every' +'(' +every_base + ')'
        
        everys = ['numa','nums','numm','numh','numd','numw']
        every_base = str(random.sample(everys,1)).replace("num",'%s' %every_n).replace("[","").replace("]","").replace("'","").replace(", ","")
        single_every_right = 'every' +'(' +every_base + ')'
        
        #单range
        range_starts = ['2021-08-01 00:00:00',1625000000000]
        range_start = str(random.sample(range_starts,1)).replace("[","").replace("]","").replace(", ","")
        range_ends = ['2022-08-15 00:00:00',1629000000000]
        range_end = str(random.sample(range_ends,1)).replace("[","").replace("]","").replace(", ","").replace("' (","").replace(") '","")
        single_range_right_11 = 'range' +'(' +range_start + ','+ range_end + ')'
        single_range_right_111 = 'range' +'(' + range_start + ','+ range_start + ')'
        single_range_right_112 = 'range' +'(' + range_end + ')'
         
        range_starts = ['2021-08-01 00:00:00',1625000000000]
        range_start = str(random.sample(range_starts,1)).replace("[","").replace("]","").replace(", ","")
        range_ends = ['2022-09-01 00:00:00',1631000000000]
        range_end = str(random.sample(range_ends,1)).replace("[","").replace("]","").replace(", ","").replace("' (","").replace(") '","")
        single_range_right_12 = 'range' +'(' + range_start + ','+ range_end + ')'
        single_range_right_121 = 'range' +'(' + range_start + ','+ range_start + ')'
        single_range_right_122 = 'range' +'(' + range_end + ')'
         
        range_starts = ['2021-08-30 00:00:00',1630090000000]
        range_start = str(random.sample(range_starts,1)).replace("[","").replace("]","").replace(", ","")
        range_ends = ['2022-09-10 00:00:00',1631000000000]
        range_end = str(random.sample(range_ends,1)).replace("[","").replace("]","").replace(", ","").replace("' (","").replace(") '","")
        single_range_right_13 = 'range' +'(' + range_start + ','+ range_end + ')'
        single_range_right_131 = 'range' +'(' + range_start + ','+ range_start + ')'
        single_range_right_132 = 'range' +'(' + range_end + ')'
         
        t = time.time()  
        t_to_s =  time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t))  
        range_starts = ['2021-10-01 00:00:00',1640000000000]
        range_start = str(random.sample(range_starts,1)).replace("[","").replace("]","").replace(", ","")
        range_ends = ['2022-01-01 00:00:00',1640000100000,' (now()) ','t_to_s']
        range_end = str(random.sample(range_ends,1)).replace("[","").replace("]","").replace(", ","").replace("' (","").replace(") '","").replace("t_to_s","%s" %t_to_s)
        single_range_right_14 = 'range' +'(' + range_start + ','+ range_end + ')'
        single_range_right_141 = 'range' +'(' + range_start + ','+ range_start + ')'
        single_range_right_142 = 'range' +'(' + range_end + ')'
                
        range_starts = ['2021-08-01 00:00:00',1625000000000]
        range_start = str(random.sample(range_starts,1)).replace("[","").replace("]","").replace(", ","")
        range_ends = ['2022-01-01 00:00:00',1640000100000,' (now()) ','t_to_s']
        range_end = str(random.sample(range_ends,1)).replace("[","").replace("]","").replace(", ","").replace("' (","").replace(") '","").replace("t_to_s","%s" %t_to_s)
        single_range_right_15 = 'range' +'(' + range_start + ','+ range_end + ')'
        single_range_right_151 = 'range' +'(' + range_start + ','+ range_start + ')'
        single_range_right_152 = 'range' +'(' + range_end + ')'

        if i == 1:
            range_fill_every = single_fill
        elif i == 2:
            range_fill_every = single_every
        elif i == 3:
            range_fill_every = single_range_right_15
        elif i == 4:
            range_fill_every = single_range_right_15 + ' ' + single_every
        elif i == 5:
            range_fill_every = single_range_right_15 + ' ' + single_fill
        elif i == 6:
            range_fill_every = single_every + ' ' + single_fill
        elif i == 7:
            range_fill_every = single_range_right_15 + ' ' + single_every_error + ' ' + single_fill
        #right
        elif i == 11:
            range_fill_every = single_range_right_11 + ' ' + single_every_right + ' ' + single_fill
        elif i == 12:
            range_fill_every = single_range_right_12 + ' ' + single_every_right + ' ' + single_fill
        elif i == 13:
            range_fill_every = single_range_right_13 + ' ' + single_every_right + ' ' + single_fill
        elif i == 14:
            range_fill_every = single_range_right_14 + ' ' + single_every_right + ' ' + single_fill
        elif i == 15:
            range_fill_every = single_range_right_15 + ' ' + single_every_right + ' ' + single_fill
            
        #right INTERP支持单点查询
        elif i == 21:
            range_fill_every = single_range_right_111 + ' ' + single_every_right + ' ' + single_fill
        elif i == 22:
            range_fill_every = single_range_right_121 + ' ' + single_every_right + ' ' + single_fill
        elif i == 23:
            range_fill_every = single_range_right_131 + ' ' + single_every_right + ' ' + single_fill
        elif i == 24:
            range_fill_every = single_range_right_141 + ' ' + single_every_right + ' ' + single_fill
        elif i == 25:
            range_fill_every = single_range_right_151 + ' ' + single_every_right + ' ' + single_fill
        elif i == 26:
            range_fill_every = single_range_right_111 + ' ' + single_fill
        elif i == 27:
            range_fill_every = single_range_right_121 + ' ' + single_fill
        elif i == 28:
            range_fill_every = single_range_right_131 + ' ' + single_fill
        elif i == 29:
            range_fill_every = single_range_right_141 + ' ' + single_fill
        elif i == 30:
            range_fill_every = single_range_right_151 + ' ' + single_fill
        elif i == 31:
            range_fill_every = single_range_right_112 + ' ' + single_every_right + ' ' + single_fill
        elif i == 32:
            range_fill_every = single_range_right_122 + ' ' + single_every_right + ' ' + single_fill
        elif i == 33:
            range_fill_every = single_range_right_132 + ' ' + single_every_right + ' ' + single_fill
        elif i == 34:
            range_fill_every = single_range_right_142 + ' ' + single_every_right + ' ' + single_fill
        elif i == 35:
            range_fill_every = single_range_right_152 + ' ' + single_every_right + ' ' + single_fill
        elif i == 36:
            range_fill_every = single_range_right_112 + ' ' + single_fill
        elif i == 37:
            range_fill_every = single_range_right_122 + ' ' + single_fill
        elif i == 38:
            range_fill_every = single_range_right_132 + ' ' + single_fill
        elif i == 39:
            range_fill_every = single_range_right_142 + ' ' + single_fill
        elif i == 40:
            range_fill_every = single_range_right_152 + ' ' + single_fill
                               
        return range_fill_every    
 
    
    def interp_check(self,db,sql1,sql2):            
        rows = -1;
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,db)
        cur1 = case_common[1]
        
        try:
            # self.tdSql.query(sql1,queryTimes=1)
            # self.tdSql.query(sql2,queryTimes=1)            
            rows = self.tdSql.query(sql1,queryTimes=1).row_count   
            if rows>=0:
                rows_1 = rows 
                rows_2 = self.tdSql.query(sql2,queryTimes=1).row_count 
                if (rows_1 == 0) and (rows_2 == 0):
                    self.logger.info(("=====sql1.rows:'%s',=====sql2.rows:'%s'") %(rows_1,rows_2))
                    self.tdCreateData.explain_sql(sql2) 
                    cur1.execute(sql2)           
                elif (rows_1 > 0 and rows_1 <= 10) and (rows_2 > 0 and rows_2 <= 10):
                    self.logger.info(("=====sql1.rows:'%s',=====sql2.rows:'%s'") %(rows_1,rows_2))
                    self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                    self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,int('%d' %rows_1),1,1,'%s' %sql2 ,1,int('%d' %rows_2),1,1)
                    self.tdCreateData.explain_sql(sql2)    
                    cur1.execute(sql2)      
                elif (rows_1 > 10 ) and (rows_2 > 10 ) and (rows_1 == rows_2) :
                    self.logger.info(("=====sql1.rows:'%s',=====sql2.rows:'%s'") %(rows_1,rows_2))
                    self.tdCreateData.data_matrix_equal('%s' %sql1 ,1,10,1,1,'%s' %sql2 ,1,10,1,1)
                    self.tdCreateData.explain_sql(sql2)
                    cur1.execute(sql2)
        except:
            self.tdSql.error(sql1)
            #self.tdSql.error(sql2)
            self.logger.info("sql1 is not support :=====%s; or sql2 is not support :=====%s; " %(sql1,sql2))
                                                   
         
    def right_case_1_range(self):
        self.logger.info("\n==========================right case 1==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_1)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         
                           
        # 1: support all int type \ double type  [hanshu = ['INTERP']]
        for i in (33,):
            func = tdFunction.func_stable_special(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db_1)              

                self.logger.info("\n\n\n=======hanshu num = %d======right case========case1======\n\n\n" %i)
                
                stable_where = tdWhere.regular_where()
                sql1 = "select %s from %s where tbname in ('%s_1') ;"  % (func,self.table,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]
             
                        interval_fill = ' where ts between 1630000001000 and 1630100001000 '
                        interval_fill_and = ' ts between 1630000001000 and 1630100001000 and '
                                
                        list_intervals = [1,2,3,4,5,6,7,]
                        for i in list_intervals:                        
                            range_fill_every = self.interp_range_fill_every(i)
                            self.logger.info("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            
                            sql2 = "select %s from %s  %s ;"  % (func,self.table,range_fill_every)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s %s %s %s);" %(func,self.table,qt_where,qt_like_match,qt_in_where,range_fill_every)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                                       
                            sql2 = "select %s from %s where tbname in ('%s') %s ;"  % (func,self.table,self.table,range_fill_every)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s %s %s %s;" %(func,self.table,qt_where,qt_like_match,qt_in_where,range_fill_every)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select %s from %s where %s %s %s %s ;" %(func,self.table,qt_where,qt_like_match,qt_in_where,range_fill_every)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                        list_intervals = [11,12,13,14,15,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,]
                        for i in list_intervals:                        
                            range_fill_every = self.interp_range_fill_every(i)
                            self.logger.info("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            sql1 = "select %s from %s  %s ;"  % (func,self.table,range_fill_every)

                            sql2 = "select %s from %s where  %s %s %s ;" %(func,self.table,qt_like_match,qt_in_where,range_fill_every)
                            self.interp_check(self.db_1,sql1,sql2)
                            sql= sql + sql2
                            
                            sql2 = "select %s from %s where tbname in ('%s_1') %s ;"  % (func,self.table,self.table,range_fill_every)
                            self.interp_check(self.db_1,sql1,sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s %s %s %s);" %(func,self.table,qt_where,qt_like_match,qt_in_where,range_fill_every)
                            self.interp_check(self.db_1,sql1,sql2)
                            sql= sql + sql2
                            
                            sql1 = "select %s as ii from %s  %s order by ii;"  % (func,self.table,range_fill_every)

                            sql2 = "select %s as ii from %s where  %s %s %s order by ii ;" %(func,self.table,qt_like_match,qt_in_where,range_fill_every)
                            self.interp_check(self.db_1,sql1,sql2)
                            sql= sql + sql2
                            
                            sql2 = "select %s as ii from %s where tbname in ('%s_1') %s order by ii ;"  % (func,self.table,self.table,range_fill_every)
                            self.interp_check(self.db_1,sql1,sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s as ii from %s where %s %s %s %s order by ii);" %(func,self.table,qt_where,qt_like_match,qt_in_where,range_fill_every)
                            self.interp_check(self.db_1,sql1,sql2)
                            sql= sql + sql2
                            
            except Exception as e:
                raise e   
                        
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        self.logger.info("sqlnum1 %d" % num1) 
        cur1.close()
        conn1.close() 
         
    def right_case_2_range(self):
        self.logger.info("\n==========================right case 1==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db_2)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         
                           
        # 1: support all int type \ double type  [hanshu = ['INTERP']]
        for i in (33,):
            func = tdFunction.func_stable_special(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db_2)              

                self.logger.info("\n\n\n=======hanshu num = %d======right case========case1======\n\n\n" %i)
                
                stable_where = tdWhere.regular_where()
                sql1 = 'select %s from %s;'  % (func,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]
             
                        interval_fill = ' where ts between 1630000001000 and 1630100001000 '
                        interval_fill_and = ' ts between 1630000001000 and 1630100001000 and '
                            
                        list_intervals = [11,12,13,14,15,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,]
                        for i in list_intervals:                        
                            range_fill_every = self.interp_range_fill_every(i)
                            self.logger.info("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            sql1 = "select %s,_irowts,_isfilled  from %s  where tbname in ('%s_1')  %s ;"  % (func,self.table,self.table,range_fill_every)

                            sql2 = "select %s,_irowts,_isfilled from %s where  %s %s %s ;" %(func,self.table,qt_like_match,qt_in_where,range_fill_every)
                            self.interp_check(self.db_2,sql1,sql2)
                            sql= sql + sql2
                            
                            sql2 = "select %s,_irowts,_isfilled  from %s where tbname in ('%s_1') %s ;"  % (func,self.table,self.table,range_fill_every)
                            self.interp_check(self.db_2,sql1,sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s,_irowts,_isfilled  from %s where %s %s %s %s);" %(func,self.table,qt_where,qt_like_match,qt_in_where,range_fill_every)
                            self.interp_check(self.db_2,sql1,sql2)
                            sql= sql + sql2
                            
                            sql1 = "select %s,_irowts,_isfilled  as ii from %s  %s order by ii;"  % (func,self.table,range_fill_every)

                            sql2 = "select %s,_irowts,_isfilled  as ii from %s where  %s %s %s order by ii ;" %(func,self.table,qt_like_match,qt_in_where,range_fill_every)
                            self.interp_check(self.db_2,sql1,sql2)
                            sql= sql + sql2
                            
                            sql2 = "select %s,_irowts,_isfilled  as ii from %s where tbname in ('%s_1') %s order by ii ;"  % (func,self.table,self.table,range_fill_every)
                            self.interp_check(self.db_2,sql1,sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s,_irowts,_isfilled as ii from %s where %s %s %s %s order by ii);" %(func,self.table,qt_where,qt_like_match,qt_in_where,range_fill_every)
                            self.interp_check(self.db_2,sql1,sql2)
                            sql= sql + sql2
                            
            except Exception as e:
                raise e   
                        
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        self.logger.info("sqlnum1 %d" % num1) 
        cur1.close()
        conn1.close() 
        
                
    def right_case_1_tbname(self):
        self.logger.info("\n==========================right case 1_tbname==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         
                           
        # 1: support all int type \ double type  [hanshu = ['INTERP']]
        for i in (33,):
            func = tdFunction.func_stable_special(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)               

                self.logger.info("\n\n\n=======hanshu num = %d======right case_tbname========case1======\n\n\n" %i)
                
                stable_where = tdWhere.regular_where()
                sql1 = "select %s from %s where tbname in ('%s') group by tbname;"  % (func,self.table,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where tbname in ('%s') and %s %s %s group by tbname" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where tbname in ('%s') and %s %s %s group by tbname)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where tbname in ('%s') and %s %s %s group by tbname" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

            except Exception as e:
                raise e  

        #self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        self.logger.info("sqlnum1_tbname %d" % num1)
        cur1.close()
        conn1.close() 

    def error_case_1(self):
        self.logger.info("\n==========================error case 1==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         
                           
        # 1: support all int type \ double type  [hanshu = ['INTERP']]
   
        for i in (33,):
            func = tdFunction.func_stable_special(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)                  

                self.logger.info("\n\n\n=======hanshu num = %d======error case========case1======\n\n\n" %i)
                
                stable_where = tdWhere.regular_where()
                sql1 = 'select %s from %s;'  % (func,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s group by tbname" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s group by tbname)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where %s %s %s group by tbname" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

            except Exception as e:
                raise e  

        for i in (33,):
            func = tdFunction.func_stable_special(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)                 

                self.logger.info("\n\n\n=======hanshu num = %d======right case_tbname_interval========case1======\n\n\n" %i)
                
                stable_where = tdWhere.regular_where()
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]        
                        
                        interval_fill = ' where ts between 1630000001000 and 1630100001000 '
                        interval_fill_and = ' ts between 1630000001000 and 1630100001000 and '
                            
                        list_intervals = [1,2,3,4,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22]
                        list_interval = random.sample(list_intervals,10) 
                        for i in list_interval:                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            sql1 = "select %s from %s where tbname in ('%s') %s group by tbname;"  % (func,self.table,self.table,time_window_new)

                            sql2 = "select %s from %s where  %s %s %s group by tbname;" %(func,self.table,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s %s %s %s group by tbname);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s %s %s %s group by tbname;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select %s from %s where tbname in ('%s') and %s %s %s %s group by tbname;" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where tbname in ('%s') and %s %s %s %s group by tbname);" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where tbname in ('%s') and %s %s %s %s group by tbname;" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select %s from %s where tbname in ('%s') and %s %s %s %s %s group by tbname;" %(func,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where tbname in ('%s') and %s %s %s %s %s group by tbname);" %(func,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where tbname in ('%s') and %s %s %s %s %s group by tbname;" %(func,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
            except Exception as e:
                raise e   
                        
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        self.logger.info("sqlnum1_error %d" % num1) 
        cur1.close()
        conn1.close() 
 
    def right_case_1_interval(self):
        self.logger.info("\n==========================right case 1==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'         
                                                    
        for i in (33,):
            func = tdFunction.func_stable_special(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)                

                self.logger.info("\n\n\n=======hanshu num = %d======right case_interval========case1======\n\n\n" %i)
                
                stable_where = tdWhere.regular_where()
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]         
                        
                        for i in (12,):                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s;'  % (func,self.table,time_window_new)

                            sql2 = "select %s from %s where  %s %s %s %s;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s %s %s %s);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s %s %s %s;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                        list_intervals = [1,2,3,4,6,7,8,9,11,13,14,15,16,17,18,19,20,21,22]
                        list_interval = random.sample(list_intervals,10) 
                        for i in list_interval:                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s;'  % (func,self.table,time_window_new)

                            sql2 = "select %s from %s where  %s %s %s %s;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where %s %s %s %s);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s %s %s %s;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                                                          
            except Exception as e:
                raise e  
            
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num1 = sql.count('where')
        self.logger.info("sqlnum1_interval %d" % num1)
        cur1.close()
        conn1.close()                          
                
    def right_case_2(self):
        self.logger.info("\n==========================right case 2==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]        
        sql = 'Count the number of sqls'       

        # 1: support all int type \ double type  [hanshu = ['INTERP']]
        for i in (33,):
            func = tdFunction.func_stable_special(i)
            func_desc = func # for desc
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)                 

                self.logger.info("\n\n\n=======hanshu num = %d======right case========case2======\n\n\n" %i)

                stable_where = tdWhere.regular_where()
                sql1 = 'select %s from %s;'  % (func,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select %s from %s where  %s %s %s order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where  %s %s %s order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s order by ts ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                
                stable_where = tdWhere.regular_where()
                sql1 = 'select %s from (select * from %s order by ts desc);'  % (func_desc,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where  %s %s %s order by ts desc)" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where  %s %s %s order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select %s from (select * from %s where  %s %s %s order by ts desc)" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s ) order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s order by ts desc ) order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
            

            except Exception as e:
                raise e   
            
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num2 = sql.count('where')
        self.logger.info("sqlnum2 %d" % num2) 
        cur1.close()
        conn1.close() 
        
    def right_case_2_tbname(self):
        self.logger.info("\n==========================right case 2_tbname==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]        
        sql = 'Count the number of sqls'       

        # 1: support all int type \ double type  [hanshu = ['INTERP']]
        for i in (33,):
            func = tdFunction.func_stable_special(i)
            func_desc = func # for desc
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)                 

                self.logger.info("\n\n\n=======hanshu num = %d======right case_tbname========case2======\n\n\n" %i)

                stable_where = tdWhere.regular_where()
                sql1 = "select %s from %s where tbname in ('%s') group by tbname;"  % (func,self.table,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where tbname in ('%s') and  %s %s %s group by tbname order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s') and  %s %s %s group by tbname order by ts)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select * from (select %s from %s where tbname in ('%s') and  %s %s %s group by tbname order by ts)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where tbname in ('%s') and  %s %s %s group by tbname order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s') and  %s %s %s ) group by tbname order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select %s from (select * from %s where tbname in ('%s') and  %s %s %s group by tbname) order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s') and  %s %s %s order by ts ) group by tbname order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select %s from (select * from %s where tbname in ('%s') and  %s %s %s group by tbname order by ts ) group by tbname order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                
                stable_where = tdWhere.regular_where()
                sql1 = "select %s from %s where tbname in ('%s')  group by tbname order by ts desc;"  % (func_desc,self.table,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where tbname in ('%s') and  %s %s %s group by tbname order by ts desc" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where tbname in ('%s') and  %s %s %s group by tbname order by ts desc)" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where tbname in ('%s') and  %s %s %s group by tbname order by ts desc" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s') and  %s %s %s ) group by tbname order by ts desc" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select %s from (select * from %s where tbname in ('%s') and  %s %s %s group by tbname) order by ts desc" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where tbname in ('%s') and  %s %s %s order by ts desc ) group by tbname order by ts desc" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select %s from (select * from %s where tbname in ('%s') and  %s %s %s group by tbname order by ts desc ) group by tbname order by ts desc" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
            

            except Exception as e:
                raise e    

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num2 = sql.count('where')
        self.logger.info("sqlnum2_tbname %d" % num2) 
        cur1.close()
        conn1.close() 

    def error_case_2(self):
        self.logger.info("\n==========================error case 2==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]        
        sql = 'Count the number of sqls'       

        # 1: support all int type \ double type  [hanshu = ['INTERP']]
        for i in (33,):
            func = tdFunction.func_stable_special(i)
            func_desc = func # for desc
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)                 

                self.logger.info("\n\n\n=======hanshu num = %d======right case========case2======\n\n\n" %i)

                stable_where = tdWhere.regular_where()
                sql1 = 'select %s from %s group by tbname;'  % (func,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s group by tbname order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s group by tbname order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                       
                        sql2 = "select * from (select %s from %s where  %s %s %s group by tbname order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where  %s %s %s group by tbname order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s group by tbname ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s group by tbname order by ts ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                
                stable_where = tdWhere.regular_where()
                sql1 = 'select %s from %s order by ts desc;'  % (func_desc,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s group by tbname order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where  %s %s %s group by tbname order by ts desc)" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where  %s %s %s group by tbname order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s group by tbname ) order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where  %s %s %s group by tbname order by ts desc ) order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
            

            except Exception as e:
                raise e  
              
        for i in (33,):
            func = tdFunction.func_stable_special(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)                 
                    
                self.logger.info("\n\n\n=======hanshu num = %d======error case_interval========case1======\n\n\n" %i)
                
                stable_where = tdWhere.regular_where()
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]         
                        
                        list_intervals = [1,2,3,4,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22]
                        list_interval = random.sample(list_intervals,10) 
                        for i in list_interval:                       
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case1=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s group by tbname;'  % (func,self.table,time_window_new)

                            sql2 = "select %s from %s where  %s %s %s group by tbname order by ts;" %(func,self.table,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from %s where  %s %s %s group by tbname order by ts desc;" %(func,self.table,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select * from (select %s from %s where %s %s %s %s group by tbname order by ts);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select * from (select %s from %s where %s %s %s %s group by tbname order by ts desc);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                                        
                            sql2 = "select %s from (select * from %s where %s %s %s %s group by tbname order by ts);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                                        
                            sql2 = "select %s from (select * from %s where %s %s %s %s group by tbname order by ts desc);" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s %s %s %s group by tbname order by ts;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where %s %s %s %s group by tbname order by ts desc;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where %s %s %s %s group by tbname ) order by ts;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where %s %s %s %s ) group by tbname order by ts desc;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %swhere %s %s %s %s group by tbname order by ts) order by ts ;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select %s from (select * from %swhere %s %s %s %s order by ts) group by tbname order by ts desc ;" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                                                                                                                 
            except Exception as e:
                raise e   
            
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num2 = sql.count('where')
        self.logger.info("sqlnum2_error %d" % num2) 
        cur1.close()
        conn1.close() 
        
                        
    def right_case_2_interval(self):
        self.logger.info("\n==========================right case 2==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]        
        sql = 'Count the number of sqls'       

        for i in (33,):
            func = tdFunction.func_stable_special(i)
            func_desc = func # for desc
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)                  

                self.logger.info("\n\n\n=======hanshu num = %d======right case_interval========case2======\n\n\n" %i)

                stable_where = tdWhere.regular_where()
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]      
                        
                        interval_fill = ' where ts between 1630000001000 and 1630100001000 '
                        interval_fill_and = ' ts between 1630000001000 and 1630100001000 and '
                                                
                        list_intervals = [1,2,3,4,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22]
                        list_interval = random.sample(list_intervals,10) 
                        for i in list_interval:                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            sql1 = 'select %s from %s %s;'  % (func,self.table,time_window_new)

                            sql2 = "select %s from %s where  %s %s %s %s order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s order by ts ) order by ts" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                                                                    
                stable_where = tdWhere.regular_where()                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]           
                                                                        
                        list_intervals = [1,2,3,4,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22]
                        list_interval = random.sample(list_intervals,10) 
                        for i in list_interval:                            
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            n = random.randrange(2,101) 
                            func_desc = func_desc.replace("num","%d" %n)
                            sql1 = 'select %s from %s %s order by ts desc;'  % (func_desc,self.table,time_window_new)

                            sql2 = "select %s from %s where  %s %s %s %s order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s order by ts desc)" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s ) order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where  %s %s %s %s order by ts desc ) order by ts desc" %(func_desc,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
            except Exception as e:
                raise e  
 
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num2 = sql.count('where')
        self.logger.info("sqlnum2_interval %d" % num2) 
        cur1.close()
        conn1.close() 
        
    def right_case_2_tbname_interval(self):
        self.logger.info("\n==========================right case 2_tbname==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]        
        sql = 'Count the number of sqls'       

        for i in (33,):
            func = tdFunction.func_stable_special(i)
            func_desc = func # for desc
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)                

                self.logger.info("\n\n\n=======hanshu num = %d======right case_tbname_interval========case2======\n\n\n" %i)

                stable_where = tdWhere.regular_where()
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]      
                        
                        interval_fill = ' where ts between 1630000001000 and 1630100001000 '
                        interval_fill_and = ' ts between 1630000001000 and 1630100001000 and '
                        
                        list_intervals = [1,2,3,4,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22]
                        list_interval = random.sample(list_intervals,10) 
                        for i in list_interval:                           
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            sql1 = "select %s from %s where tbname in ('%s') %s group by tbname;"  % (func,self.table,self.table,time_window_new)

                            sql2 = "select %s from %s where tbname in ('%s') and  %s %s %s %s group by tbname order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where tbname in ('%s') and  %s %s %s %s group by tbname order by ts)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select * from (select %s from %s where tbname in ('%s') and  %s %s %s %s group by tbname order by ts)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where tbname in ('%s') and  %s %s %s %s group by tbname order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where tbname in ('%s') and  %s %s %s %s  group by tbname) order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where tbname in ('%s') and  %s %s %s %s  group by tbname order by ts ) order by ts" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                        
                        list_intervals = [1,2,3,4,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22]
                        list_interval = random.sample(list_intervals,10) 
                        for i in list_interval:                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            sql1 = "select %s from %s where %s tbname in ('%s') %s group by tbname ;"  % (func,self.table,interval_fill_and,self.table,time_window_new)

                            sql2 = "select %s from %s where tbname in ('%s') and  %s %s %s %s %s group by tbname order by ts" %(func,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where tbname in ('%s') and  %s %s %s %s %s group by tbname order by ts)" %(func,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                            sql2 = "select * from (select %s from %s where tbname in ('%s') and  %s %s %s %s %s group by tbname order by ts)" %(func,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where tbname in ('%s') and  %s %s %s %s %s group by tbname order by ts" %(func,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where tbname in ('%s') and  %s %s %s %s %s  group by tbname) order by ts" %(func,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where tbname in ('%s') and  %s %s %s %s %s  group by tbname order by ts ) order by ts" %(func,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                                                                               
                stable_where = tdWhere.regular_where()
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]         
                        
                        interval_fill = ' where ts between 1630000001000 and 1630100001000 '
                        interval_fill_and = ' ts between 1630000001000 and 1630100001000 and '
                        
                        list_intervals = [1,2,3,4,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22]
                        list_interval = random.sample(list_intervals,10) 
                        for i in list_interval:                           
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            sql1 = "select %s from %s  where tbname in ('%s')  %s group by tbname order by ts desc;"  % (func_desc,self.table,self.table,time_window_new)

                            sql2 = "select %s from %s where tbname in ('%s') and  %s %s %s %s group by tbname order by ts desc"  %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where tbname in ('%s') and  %s %s %s %s group by tbname order by ts desc)" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where tbname in ('%s') and  %s %s %s %s group by tbname order by ts desc" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where tbname in ('%s') and  %s %s %s %s group by tbname ) order by ts desc" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where tbname in ('%s') and  %s %s %s %s group by tbname order by ts desc ) order by ts desc" %(func_desc,self.table,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                            
                        list_intervals = [1,2,3,4,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22]
                        list_interval = random.sample(list_intervals,10) 
                        for i in list_interval:                       
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case2=====time num = %d======interval======\n\n\n" %i)
                            sql1 = "select %s from %s  where %s tbname in ('%s') %s group by tbname  order by ts desc;"  % (func_desc,self.table,interval_fill_and,self.table,time_window_new)

                            sql2 = "select %s from %s where tbname in ('%s') and  %s %s %s %s %s group by tbname order by ts desc"  %(func_desc,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where tbname in ('%s') and  %s %s %s %s %s group by tbname order by ts desc)" %(func_desc,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where tbname in ('%s') and  %s %s %s %s %s group by tbname order by ts desc" %(func_desc,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where tbname in ('%s') and  %s %s %s %s %s group by tbname ) order by ts desc" %(func_desc,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s where tbname in ('%s') and  %s %s %s %s %s group by tbname order by ts desc ) order by ts desc" %(func_desc,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
 
            except Exception as e:
                raise e    

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num2 = sql.count('where')
        self.logger.info("sqlnum2_tbname_interval %d" % num2) 
        cur1.close()
        conn1.close() 
                                                                                          
    def right_case_3(self):
        self.logger.info("\n==========================right case 3==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]       
        sql = 'Count the number of sqls'
                   
        # 1: support all int type \ double type  [hanshu = ['INTERP']]
        for i in (33,):
            func = tdFunction.func_stable_special(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)                 

                self.logger.info("\n\n\n=======hanshu num = %d======right case========case3======\n\n\n" %i)
                
                stable_where = tdWhere.regular_where()
                sql1 = 'select %s from %s;'  % (func,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s order by ts limit 1000 " %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where  %s %s %s order by ts limit 1000)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select %s from (select * from %s) where  %s %s %s order by ts limit 1000" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
            
            except Exception as e:
                raise e  
             
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num3 = sql.count('where')
        self.logger.info("sqlnum3 %d" % num3) 
        cur1.close()
        conn1.close() 
 
    def right_case_3_tbname(self):
        self.logger.info("\n==========================right case 3_tbname==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]       
        sql = 'Count the number of sqls'
                   
        # 1: support all int type \ double type  [hanshu = ['INTERP']]
        for i in (33,):
            func = tdFunction.func_stable_special(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)                 

                self.logger.info("\n\n\n=======hanshu num = %d======right case_tbname========case3======\n\n\n" %i)
                
                stable_where = tdWhere.regular_where()
                sql1 = "select %s from %s where tbname in ('%s') group by tbname;"  % (func,self.table,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where tbname in ('%s') and  %s %s %s group by tbname order by ts limit 1000" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where tbname in ('%s') and %s %s %s group by tbname order by ts limit 1000)" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where tbname in ('%s') and %s %s %s group by tbname order by ts limit 1000" %(func,self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
            
            except Exception as e:
                raise e           
            
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num3 = sql.count('where')
        self.logger.info("sqlnum3_tbname %d" % num3)  
        cur1.close()
        conn1.close()        

    def error_case_3(self):
        self.logger.info("\n==========================error case 3==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]       
        sql = 'Count the number of sqls'
                   
        # 1: support all int type \ double type  [hanshu = ['INTERP']]
        for i in (33,):
            func = tdFunction.func_stable_special(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)                 

                self.logger.info("\n\n\n=======hanshu num = %d======error case========case3======\n\n\n" %i)
                
                stable_where = tdWhere.regular_where()
                sql1 = 'select %s from %s group by tbname ;'  % (func,self.table)
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select %s from %s where  %s %s %s group by tbname order by ts limit 1000" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where  %s %s %s group by tbname order by ts limit 1000)" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
                        
                        sql2 = "select %s from (select * from %s) where  %s %s %s group by tbname order by ts limit 1000" %(func,self.table,qt_where,qt_like_match,qt_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2
            
            except Exception as e:
                raise e           

        for i in (33,):
            func = tdFunction.func_stable_special(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)                 

                self.logger.info("\n\n\n=======hanshu num = %d======error case_interval========case3======\n\n\n" %i)
                
                stable_where = tdWhere.regular_where()
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]  
                             
                        interval_fill = ' where ts between 1630000001000 and 1630100001000 '
                        interval_fill_and = ' ts between 1630000001000 and 1630100001000 and ' 
                                                   
                        list_intervals = [1,2,3,4,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22]
                        list_interval = random.sample(list_intervals,5) 
                        for i in list_interval:                                                 
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====error case========case3=====time num = %d======interval======\n\n\n" %i)

                            sql2 = "select %s from %s where  %s %s %s %s group by tbname order by ts limit 1000" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s group by tbname order by ts limit 1000)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s group by tbname order by ts limit 1000" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2 
                                                  
                            sql2 = "select %s from %s where tbname in ('%s') and  %s %s %s %s %s  group by tbname order by ts limit 1000" %(func,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where tbname in ('%s') and %s %s %s %s %s  group by tbname order by ts limit 1000)" %(func,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where tbname in ('%s') and %s %s %s %s %s  group by tbname order by ts limit 1000" %(func,self.table,self.table,interval_fill_and,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2  
                                                                                                                          
            except Exception as e:
                raise e           
                        
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num3 = sql.count('where')
        self.logger.info("sqlnum3_error %d" % num3) 
        cur1.close()
        conn1.close() 
                        
    def right_case_3_interval(self):
        self.logger.info("\n==========================right case 3==========================\n")
        case_common = self.tdCreateData.case_sql_subprocess_execute(self.service_host,self.db)
        conn1 = case_common[0]
        cur1 = case_common[1]       
        sql = 'Count the number of sqls'
                    
        for i in (33,):
            func = tdFunction.func_stable_special(i)
            try:
                self.tdCreateData.taos_f(self.service_host,self.testcasePath,self.testcaseFilename)                  
                cur1.execute('use %s;' %self.db)                 

                self.logger.info("\n\n\n=======hanshu num = %d======right case_interval========case3======\n\n\n" %i)
                
                stable_where = tdWhere.regular_where()
                
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]       
                        
                        interval_fill = ' where ts between 1630000001000 and 1630100001000 '
                        interval_fill_and = ' ts between 1630000001000 and 1630100001000 and '
                                                                            
                        list_intervals = [1,2,3,4,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22]
                        list_interval = random.sample(list_intervals,10) 
                        for i in list_interval:                        
                            time_window_new = tdWhere.time_window_new(i)
                            self.logger.info("\n\n\n====right case========case3=====time num = %d======interval======\n\n\n" %i)

                            sql2 = "select %s from %s where  %s %s %s %s  order by ts limit 1000" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select * from (select %s from %s where  %s %s %s %s  order by ts limit 1000)" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2

                            sql2 = "select %s from (select * from %s) where  %s %s %s %s  order by ts limit 1000" %(func,self.table,qt_where,qt_like_match,qt_in_where,time_window_new)
                            self.tdSql.error(sql2)
                            sql= sql + sql2
                                                                                                
            except Exception as e:
                raise e                

             
        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)
        
        num3 = sql.count('where')
        self.logger.info("sqlnum3_interval %d" % num3) 
        cur1.close()
        conn1.close() 
   
         
    def rm_sql(self,db):
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename)) 
        self.tdCreateData.drop_db("%s" % db)  
                  
    def run(self):
        startTime = time.time() 
        
        self.data_create(self.db)
        
        #self.right_case_1_range()
        #self.right_case_2_range()
          
        startTime1 = time.time()
        self.right_case_1()
        self.right_case_1_tbname()
        self.error_case_1()       
        self.right_case_1_interval()
        endTime1 = time.time()       
        self.logger.info("total time1 %d s" % (endTime1 - startTime1))
    
        startTime2 = time.time()
        self.right_case_2()
        self.right_case_2_tbname()
        self.error_case_2()        
        self.right_case_2_interval()
        self.right_case_2_tbname_interval()
        endTime2 = time.time()       
        self.logger.info("total time2 %d s" % (endTime2 - startTime2))
        
        startTime3 = time.time()
        self.right_case_3()
        self.right_case_3_tbname()
        self.error_case_3()       
        self.right_case_3_interval()
        endTime3 = time.time()
        self.logger.info("total time3 %ds" % (endTime3 - startTime3))     

        endTime = time.time()
        self.rm_sql(self.db)
        self.logger.info("total time %ds" % (endTime - startTime))


