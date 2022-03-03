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
import string
import os
import sys
import time
import itertools
from itertools import product
from itertools import combinations
from faker import Faker

from taostest import TDCase

class TDWhere_makesql():
    updatecfgDict={'maxSQLLength':1048576}
    NUM = random.randint(0, 30)
    # print(NUM)
    
    def __init__(self, tdSql, logger):
        self.tdSql =  tdSql
        self.logger = logger

    def execution_sql(self, sql):
        expectErrNotOccured = True
        try:            
            self.tdSql.execute(sql)
            self.logger.info("sql:%s, no error occured" % (sql))
        except BaseException:            
            expectErrNotOccured = False
            #self.logger.info("sql:%s, error occured" % (sql))    

    def column_tag(self):
        int_column = ['(q_int)','(q_bigint)','(q_smallint)','(q_tinyint)','(q_float)','(q_double)','(q_int_null)','(q_bigint_null)','(q_smallint_null)','(q_tinyint_null)','(q_float_null)','(q_double_null)']
        bia_column = ['(*)','(_c0)','(_C0)','(q_bool)','(q_binary)','(q_nchar)','(q_ts)','(q_bool_null)','(q_binary_null)','(q_nchar_null)','(q_ts_null)']
        tag_column = ['(tbname)','(loc)','(t_int)','(t_bigint)','(t_smallint)','(t_tinyint)','(t_float)','(t_double)','(t_bool)','(t_binary)','(t_nchar)','(t_ts)']
        column_tag = int_column + bia_column + tag_column    

        return  column_tag
    # def column_tag(self):
    #     int_column = ["(q_int)","(q_bigint)","(q_smallint)","(q_tinyint)","(q_float)","(q_double)","(q_int_null)","(q_bigint_null)","(q_smallint_null)","(q_tinyint_null)","(q_float_null)","(q_double_null)"]
    #     bia_column = ["(*)","(_c0)","(_C0)","(q_bool)","(q_binary)","(q_nchar)","(q_ts)","(q_bool_null)","(q_binary_null)","(q_nchar_null)","(q_ts_null)"]
    #     tag_column = ["(tbname)","(loc)","(t_int)","(t_bigint)","(t_smallint)","(t_tinyint)","(t_float)","(t_double)","(t_bool)","(t_binary)","(t_nchar)","(t_ts)"]
    #     column_tag = int_column + bia_column + tag_column    

    #     return  column_tag

    def q_where(self):       
        q_int_where = ['q_bigint >= -9223372036854775807 and ' , 'q_bigint <= 9223372036854775807 and ','q_smallint >= -32767 and ', 'q_smallint <= 32767 and ',
        'q_tinyint >= -127 and ' , 'q_tinyint <= 127 and ' , 'q_int <= 2147483647 and ' , 'q_int >= -2147483647 and ','q_tinyint != 128 and ','q_bigint <= 0 and ' , 'q_smallint <= 0 and ', 'q_tinyint <= 0 and ' ,  'q_int <= 0 and ',
        'q_bigint between -9223372036854775807 and 0 and ',' q_int between -2147483647 and 0 and ','q_smallint between -32767 and 0 and ', 'q_tinyint between -127 and 0 and ','q_smallint is not null and ' , 'q_tinyint is not null and ' ,
        'q_bigint between  -9223372036854775807 and 9223372036854775807 and ',' q_int between -2147483647 and 2147483647 and ','q_bigint < -9223372036854775807 and ' , 'q_bigint > 9223372036854775807 and ','q_smallint < -32767 and ', 'q_smallint > 32767 and ',
        'q_smallint between -32767 and 32767 and ', 'q_tinyint between -127 and 127  and ','q_bigint is not null and ' , 'q_int is not null and ' , 'q_smallint is not null and ' , 'q_tinyint is not null and ' ,
        'q_tinyint < -127 and ' , 'q_tinyint > 127 and ' , 'q_int > 2147483647 and ' , 'q_int < -2147483647 and ','q_bigint between  9223372036854775807 and -9223372036854775807 and ',' q_int between 2147483647 and -2147483647 and ',
        'q_smallint between 32767 and -32767 and ', 'q_tinyint between 127 and -127  and ','q_bigint is null and ' , 'q_int is null and ' , 'q_smallint is null and ' , 'q_tinyint is null and ' ,
        'q_bigint >= 0 and ' , 'q_smallint >= 0 and ', 'q_tinyint >= 0 and ' ,  'q_int >= 0 and ','q_bigint between  0 and 9223372036854775807 and ',' q_int between 0 and 2147483647 and ',
        'q_smallint between 0 and 32767 and ', 'q_tinyint between 0 and 127  and ','q_bigint is not null and ' , 'q_int is not null and ' ,]

        q_fl_do_where = ['q_float >= -3.4E38 and ','q_float <= 3.4E38 and ', 'q_double >= -1.7E308 and ','q_double <= 1.7E308 and ', 'q_float between -3.4E38 and 3.4E38 and ','q_double between -1.7E308 and 1.7E308 and ' ,
        'q_float is not null and ' ,'q_double is not null and ' ,'q_float >= 0 and ', 'q_double >= 0 and ' , 'q_float between 0 and 3.4E38 and ',
        'q_double between 0 and 1.7E308 and ' ,'q_float is not null and ' ,'q_float <= 0 and ', 'q_double <= 0 and ' , 'q_float between -3.4E38 and 0 and ','q_double between -1.7E308 and 0 and ' ,
        'q_double is not null and ' ,'q_float < -3.4E38 and ','q_float > 3.4E38 and ', 'q_double < -1.7E308 and ','q_double > 1.7E308 and ', 
        'q_float between 3.4E38 and -3.4E38 and ','q_double between 1.7E308 and -1.7E308 and ' ,'q_float is null and ' ,'q_double is null and ' ,]

        q_nc_bi_bo_ts_where = [ 'q_bool is not null and ' ,'q_binary is not null and ' ,'q_nchar is not null and ' ,'q_ts is not null and ' ,
        'q_bool is null and ' ,'q_binary is null and ' ,'q_nchar is null and ' ,'q_ts is null and ' ,]
        
        q_where = random.sample(q_int_where,4) + random.sample(q_fl_do_where,3) + random.sample(q_nc_bi_bo_ts_where,2)

        q_like = ['q_binary like \'123_\' and','q_binary like \'abc_\' and','q_nchar like \'123_\' and','q_nchar like \'abc_\' and','q_binary like \'123%\' and','q_binary like \'abc%\' and','q_nchar like \'123_\' and','q_nchar like \'abc%\' and',
        't_binary like \'123_\' and','t_binary like \'abc_\' and','t_nchar like \'123_\' and','t_nchar like \'abc_\' and','t_binary like \'123%\' and','t_binary like \'abc%\' and','t_nchar like \'123_\' and','t_nchar like \'abc%\' and',]
        q_match = ['q_binary match \'123_\' and','q_binary match \'abc_\' and','q_nchar match \'123_\' and','q_nchar match \'abc_\' and','q_binary match \'123_\' and','q_binary match \'abc_\' and','q_nchar match \'123_\' and','q_nchar match \'abc_\' and',
        'q_binary nmatch \'123_\' and','q_binary nmatch \'abc_\' and','q_nchar nmatch \'123_\' and','q_nchar nmatch \'abc_\' and','q_binary nmatch \'123_\' and','q_binary nmatch \'abc_\' and','q_nchar nmatch \'123_\' and','q_nchar nmatch \'abc_\' and',
        't_binary match \'123_\' and','t_binary match \'abc_\' and','t_nchar match \'123_\' and','t_nchar match \'abc_\' and','t_binary match \'123_\' and','t_binary match \'abc_\' and','t_nchar match \'123_\' and','t_nchar match \'abc_\' and',
        't_binary nmatch \'123_\' and','t_binary nmatch \'abc_\' and','t_nchar nmatch \'123_\' and','t_nchar nmatch \'abc_\' and','t_binary nmatch \'123_\' and','t_binary nmatch \'abc_\' and','t_nchar nmatch \'123_\' and','t_nchar nmatch \'abc_\' and',]
        q_like_match = random.sample(q_like,1) + random.sample(q_match,1)

        q_in_where = ['q_bool in (0 , 1) ' ,  'q_bool in ( true , false) ' ,' (q_bool = true or q_bool = false)' , '(q_bool = 0 or q_bool = 1)',]
        q_in = random.sample(q_in_where,1)        
        
        return(q_where,q_like_match,q_in)

    def t_where(self):   
        t_int_where = ['t_bigint >= -9223372036854775807 and ' , 't_bigint <= 9223372036854775807 and ','t_smallint >= -32767 and ', 't_smallint <= 32767 and ',
        't_tinyint >= -127 and ' , 't_tinyint <= 127 and ' , 't_int <= 2147483647 and ' , 't_int >= -2147483647 and ', 't_tinyint != 128 and ','t_bigint >= 0 and ' , 't_smallint >= 0 and ', 't_tinyint >= 0 and ' ,  't_int >= 0 and ',
        't_bigint between  0 and 9223372036854775807 and ',' t_int between 0 and 2147483647 and ','t_smallint between 0 and 32767 and ', 't_tinyint between 0 and 127  and ',
        't_bigint is not null and ' , 't_int is not null and ' ,'t_bigint <= 0 and ' , 't_smallint <= 0 and ', 't_tinyint <= 0 and ' ,  't_int <= 0 and ',
        't_bigint between -9223372036854775807 and 0 and ',' t_int between -2147483647 and 0 and ','t_smallint between -32767 and 0 and ', 't_tinyint between -127 and 0 and ', 't_smallint is not null and ' , 't_tinyint is not null and ' ,
        't_bigint between  -9223372036854775807 and 9223372036854775807 and ',' t_int between -2147483647 and 2147483647 and ','t_smallint between -32767 and 32767 and ', 't_tinyint between -127 and 127  and ',
        't_bigint is not null and ' , 't_int is not null and ' , 't_smallint is not null and ' , 't_tinyint is not null and ' ,'t_bigint < -9223372036854775807 and ' , 't_bigint > 9223372036854775807 and ','t_smallint < -32767 and ', 't_smallint > 32767 and ',
        't_tinyint < -127 and ' , 't_tinyint > 127 and ' , 't_int > 2147483647 and ' , 't_int < -2147483647 and ','t_bigint between  9223372036854775807 and -9223372036854775807 and ',' t_int between 2147483647 and -2147483647 and ',
        't_smallint between 32767 and -32767 and ', 't_tinyint between 127 and -127  and ','t_bigint is null and ' , 't_int is null and ' , 't_smallint is null and ' , 't_tinyint is null and ' ,]

        t_fl_do_where = ['t_float >= -3.4E38 and ','t_float <= 3.4E38 and ', 't_double >= -1.7E308 and ','t_double <= 1.7E308 and ', 't_float between -3.4E38 and 3.4E38 and ','t_double between -1.7E308 and 1.7E308 and ' ,
        't_float is not null and ' ,'t_double is not null and ' ,'t_float >= 0 and ', 't_double >= 0 and ' , 't_float between 0 and 3.4E38 and ','t_double between 0 and 1.7E308 and ' ,
        't_float is not null and ' ,'t_float <= 0 and ', 't_double <= 0 and ' , 't_float between -3.4E38 and -1 and ','t_double between -1.7E308 and -1 and ' ,
        't_double is not null and ' ,'t_float < -3.4E38 and ','t_float > 3.4E38 and ', 't_double < -1.7E308 and ','t_double > 1.7E308 and ', 
        't_float between 3.4E38 and -3.4E38 and ','t_double between 1.7E308 and -1.7E308 and ' ,'t_float is null and ' ,'t_double is null and ' ,]

        t_nc_bi_bo_ts_where = [ 't_bool is null and ' ,'t_binary is null and ' ,'t_nchar is null and ' ,'t_ts is null and ' ,'loc is null and ' ,'tbname is null and ' , 
        't_bool is not null and ' ,'t_binary is not null and ' ,'t_nchar is not null and ' ,'t_ts is not null and ' ,'loc is not null and ' ,'tbname is not null and ' ,]

        t_where = random.sample(t_int_where,4) + random.sample(t_fl_do_where,3) + random.sample(t_nc_bi_bo_ts_where,2)

        column_tag = self.column_tag()
        column = str(random.sample(column_tag,1)).replace("[","").replace("]","").replace("\"","").replace("(","").replace(")","").replace("'","")
        likes = [' LIKE ' , ' MATCH ' ,' NMATCH ',' CONTAINS ']
        like = str(random.sample(likes,1)).replace("[","").replace("]","").replace("\"","").replace("'","")
        conditions = ['\'1234_\' and ' , '\'abc4_\' and' , '\'1234%\' and ' , '\'a_bc4%\' and', '\'12aada@#!!34%\' and ' , '\'ab#%&%^&^*^(c4%\' and']
        condition = str(random.sample(conditions,1)).replace("[","").replace("]","").replace("\"","")
        t_like_match = column + like  + condition

        t_in_where = ['t_bool in (0 , 1) ' ,  't_bool in ( true , false) ' ,' (t_bool = true or t_bool = false)' , '(t_bool = 0 or t_bool = 1)',]
        t_in = random.sample(t_in_where,1)
        
        return(t_where,t_like_match,t_in)


    def hanshu(self):      
        column_tag = self.column_tag()
        hanshus = ['','MIN','AVG','MAX','COUNT','SUM','STDDEV','FIRST','LAST','LAST_ROW','SPREAD','CEIL','FLOOR','ROUND','TWA','IRATE','STDDEV','INTERP','DIFF']
        column = column_tag
        hanshu_column = random.sample(hanshus,1)+random.sample(column,1)
        hanshu_column = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return hanshu_column

    def hanshu_two(self):
        hanshu1=self.hanshu()
        hanshu2=self.hanshu()
        char =[' + ' , ' - ' ,'  * ' ,' / ' ,' % ' ,' ! ' ,' = ' ,' == ' ,' != ' ,' > ' ,' >= ' ,' < ' ,' <= ' ,' <> ' ,' LIKE ' , ' MATCH ' ,' NMATCH ',' CONTAINS ',' IN ',' NOT IN ']
        hanshu_two = hanshu1 + str(random.sample(char,1)).replace("[","").replace("]","").replace("'","") + hanshu2
        return hanshu_two
        
    def column(self):
        int_column = ['(q_int)','(q_bigint)','(q_smallint)','(q_tinyint)','(q_float)','(q_double)','(q_int_null)','(q_bigint_null)','(q_smallint_null)','(q_tinyint_null)','(q_float_null)','(q_double_null)']
        bia_column = ['(*)','(_c0)','(_C0)','(q_bool)','(q_binary)','(q_nchar)','(q_ts)','(q_bool_null)','(q_binary_null)','(q_nchar_null)','(q_ts_null)']
        tag_column = ['(tbname)','(loc)','(t_int)','(t_bigint)','(t_smallint)','(t_tinyint)','(t_float)','(t_double)','(t_bool)','(t_binary)','(t_nchar)','(t_ts)']
        columns = int_column + bia_column + tag_column         

        if self.NUM%5 == 1:
            columns = str(random.sample(int_column,1)+random.sample(bia_column,1)).replace("[","").replace("]","").replace("(","").replace(")","").replace("'","")
        elif self.NUM%5 == 2:
            columns = str(random.sample(bia_column,2)+random.sample(tag_column,2)).replace("[","").replace("]","").replace("(","").replace(")","").replace("'","")
        elif self.NUM%5 == 3:
            columns = str(random.sample(int_column,3)+random.sample(tag_column,3)).replace("[","").replace("]","").replace("(","").replace(")","").replace("'","")
        elif self.NUM%5 == 4:
            columns = str(random.sample(columns,10)).replace("[","").replace("]","").replace("(","").replace(")","").replace("'","")
        else:
            columns = " * "
        
        return columns

    def column_hanshu(self):
        int_column = ['(q_int)','(q_bigint)','(q_smallint)','(q_tinyint)','(q_float)','(q_double)','(q_int_null)','(q_bigint_null)','(q_smallint_null)','(q_tinyint_null)','(q_float_null)','(q_double_null)']
        bia_column = ['(*)','(_c0)','(_C0)','(q_bool)','(q_binary)','(q_nchar)','(q_ts)','(q_bool_null)','(q_binary_null)','(q_nchar_null)','(q_ts_null)']
        tag_column = ['(tbname)','(loc)','(t_int)','(t_bigint)','(t_smallint)','(t_tinyint)','(t_float)','(t_double)','(t_bool)','(t_binary)','(t_nchar)','(t_ts)']
        columns = int_column + bia_column + tag_column

        hanshu_1 = self.hanshu()

        hanshu_s = ''
        for i in range(3):
            hanshu_1 = self.hanshu()
            hanshu_s += hanshu_1 + ','            

        if self.NUM%7 == 1:
            columns = str(random.sample(int_column,1)+random.sample(bia_column,1)).replace("[","").replace("]","").replace("(","").replace(")","").replace("'","")
        elif self.NUM%7 == 2:
            columns = str(random.sample(bia_column,2)+random.sample(tag_column,2)).replace("[","").replace("]","").replace("(","").replace(")","").replace("'","")
        elif self.NUM%7 == 3:
            columns = str(random.sample(int_column,3)+random.sample(tag_column,3)).replace("[","").replace("]","").replace("(","").replace(")","").replace("'","")
        elif self.NUM%7 == 4:
            columns = str(random.sample(columns,10)).replace("[","").replace("]","").replace("(","").replace(")","").replace("'","")
        elif self.NUM%7 == 5 :
            columns = hanshu_1
        elif self.NUM%7 == 6 :
            columns = hanshu_s + hanshu_1
        else:
            columns = " * "
        
        return columns

    def time_window(self):       
        time = ['1','2','3','4','5','6','7','8','9','10']
        unit = ['a','s','m','h','d','w','n','y']
        td_base = str(random.sample(time,1)+random.sample(unit,1)).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        td_interval = td_base
        td_interval = 'interval'+'(' +td_interval + ')'

        td_sliding = td_base
        td_sliding = 'sliding'+'(' +td_sliding + ')'

        fill = ['NONE','VALUE,100','PREV','NULL','LINEAR','NEXT']
        td_fill = str(random.sample(fill,1)).replace("[","").replace("]","").replace("'","").replace(", ","")
        td_fill = 'Fill' +'(' +td_fill + ')'

        td_session = td_base
        td_session = 'SESSION'+'(ts,'+td_session + ')'

        if self.NUM%8 == 1:
            time_window = td_interval
        elif self.NUM%8 == 2:
            time_window = td_interval + ' ' + td_sliding
        elif self.NUM%8 == 3:
            time_window = td_fill 
        elif self.NUM%8 == 4:
            time_window = td_interval + ' ' + td_fill 
        elif self.NUM%8 == 5 :
            time_window = td_interval + ' ' + td_sliding + ' ' + td_fill 
        elif self.NUM%8 == 6 :
            time_window = td_session
        else:
            time_window =  "  "
        
        return time_window

    def orderby_groupby(self):    
        int_column = ['(q_int)','(q_bigint)','(q_smallint)','(q_tinyint)','(q_float)','(q_double)','(q_int_null)','(q_bigint_null)','(q_smallint_null)','(q_tinyint_null)','(q_float_null)','(q_double_null)']
        bia_column = ['(*)','(_c0)','(_C0)','(q_bool)','(q_binary)','(q_nchar)','(q_ts)','(q_bool_null)','(q_binary_null)','(q_nchar_null)','(q_ts_null)']
        tag_column = ['(tbname)','(loc)','(t_int)','(t_bigint)','(t_smallint)','(t_tinyint)','(t_float)','(t_double)','(t_bool)','(t_binary)','(t_nchar)','(t_ts)']
        columns = int_column + bia_column + tag_column
        column = str(random.sample(columns,1)).replace("[","").replace("]","").replace("(","").replace(")","").replace("'","")

        if self.NUM%10 == 1:
            og_by = " order by ts "
        elif self.NUM%10 == 2:
            og_by = " order by %s " %column
        elif self.NUM%10 == 3:
            og_by = " order by %s desc " %column
        elif self.NUM%10 == 4:
            og_by = " group by %s " %column
        elif self.NUM%10 == 5 :
            og_by = " group by tbname , %s " %column
        elif self.NUM%10 == 6 :
            og_by = " group by tbname , %s order by %s " %(column,column)
        elif self.NUM%10 == 7 :
            og_by = " group by tbname , %s order by %s desc" %(column,column)
        else:
            og_by = "  "
        
        return og_by

    def limit_offset(self):       
        if self.NUM%8 == 1:
            limit_offset = " limit 10 offset 10 slimit 10 offset 10 "
        elif self.NUM%8 == 2:
            limit_offset = " limit 10 "
        elif self.NUM%8 == 3:
            limit_offset = " limit 10 offset 10 " 
        elif self.NUM%8 == 4:
            limit_offset = " slimit 10 "
        elif self.NUM%8 == 5 :
            limit_offset = " slimit 10 soffset 10 "
        elif self.NUM%8 == 6 :
            limit_offset = " slimit 10 offset 10 "
        else:
            limit_offset = " "
        
        return limit_offset

    def regular_where(self):       
        query_where = self.q_where()
        tag_where = self.t_where()

        table_list = ['regular_table_1','stable_1_1','regular_table_2','stable_1_2','regular_table_3','stable_1_3','stable_1_4','stable_2_1','stable_2_2','regular_table_null','stable_null_data_1','stable_1','stable_2']
        stable_list = ['stable_1','stable_2','stable_null_data']
        table = str(random.sample(table_list,1)).replace("[","").replace("]","").replace("'","")
        if table not in stable_list:
            q_where = random.sample(query_where[0],6)
        else:
            q_where = random.sample(query_where[0],4) + random.sample(tag_where[0],4)
        
        if self.NUM%3 ==0:
            q_like_match = str(random.sample(query_where[1],1)).replace("[","").replace("]","").replace("\"","")
        elif self.NUM%3 ==1:
            q_like_match = tag_where[1]
        else :
            q_like_match = " "

        if self.NUM%2 ==0:
            q_in_where = str(query_where[2]).replace("[","").replace("]","").replace("'","")
        else:
            q_in_where = str(tag_where[2]).replace("[","").replace("]","").replace("'","")

        column_hanshu = self.column_hanshu()
        time_window = self.time_window()
        og_by = self.orderby_groupby()
        limit_offset = self.limit_offset()
               
        return(column_hanshu,table,q_where,q_like_match,q_in_where,time_window,og_by,limit_offset)

    def regular_where2(self):
        where2 = self.regular_where()
        hanshu_two = self.hanshu_two()

        table = where2[1]
        q_where = where2[2]
        q_in_where = where2[3]
        time_window = where2[4]
        og_by = where2[5]
        limit_offset = where2[6]
        return(hanshu_two,table,q_where,q_in_where,time_window,og_by,limit_offset)

    def q_join_where(self):
        q_int_where = [ 't1._c0 = t2._C0 and ' ,'t1.ts = t2.ts and ' ,'t1.q_int = t2.q_int and ' , 't1.q_bigint = t2.q_bigint and ' ,'t1.q_smallint = t2.q_smallint and ' ,'t1.q_tinyint = t2.q_tinyint and ', 
        't1.q_float = t2.q_float and ' ,'t1.q_double = t2.q_double and ' , 't1.q_bool = t2.q_bool and ' ,'t1.q_binary = t2.q_binary and ' ,'t1.q_nchar = t2.q_nchar and ', 't1.q_ts = t2.q_ts and ' ,
        't1.q_ts_null = t2.q_ts and ' ,'t1.q_int_null = t2.q_int and ' , 't1.q_bigint_null = t2.q_bigint and ' ,'t1.q_smallint_null = t2.q_smallint and ' ,'t1.q_tinyint_null = t2.q_tinyint and ', 
        't1.q_float_null = t2.q_float and ' ,'t1.q_double_null = t2.q_double and ' , 't1.q_bool_null = t2.q_bool and ' ,'t1.q_binary_null = t2.q_binary and ' ,'t1.q_nchar_null = t2.q_nchar and ', ]
        q_join_where = random.sample(q_int_where,1) 
        return q_join_where

    def t_join_where(self):
        t_int_where = ['t1.loc = t2.loc and ' ,'t1.t_int = t2.t_int and ' , 't1.t_bigint = t2.t_bigint and ' ,'t1.t_smallint = t2.t_smallint and ' ,'t1.t_tinyint = t2.t_tinyint and ', 't1.tbname = t2.tbname and ' ,
        't1.t_float = t2.t_float and ' ,'t1.t_double = t2.t_double and ' , 't1.t_bool = t2.t_bool and ' ,'t1.t_binary = t2.t_binary and ' ,'t1.t_nchar = t2.t_nchar and ', 't1.t_ts = t2.t_ts and ' , ]
        t_join_where = random.sample(t_int_where,1) 
        return t_join_where

    def in_join_where(self):
        t_int_where = ['t1.t_bool in (0 , 1) ' ,  't1.t_bool in ( true , false) ' ,' (t1.t_bool = true or t1.t_bool = false)' , '(t1.t_bool = 0 or t1.t_bool = 1)', 
        't1.q_bool in (0 , 1) ' ,  't1.q_bool in ( true , false) ' ,' (t1.q_bool = true or t1.q_bool = false)' , '(t1.q_bool = 0 or t1.q_bool = 1)',
        't2.t_bool in (0 , 1) ' ,  't2.t_bool in ( true , false) ' ,' (t2.t_bool = true or t2.t_bool = false)' , '(t2.t_bool = 0 or t2.t_bool = 1)', 
        't2.q_bool in (0 , 1) ' ,  't2.q_bool in ( true , false) ' ,' (t2.q_bool = true or t2.q_bool = false)' , '(t2.q_bool = 0 or t2.q_bool = 1)',]
        in_join_where = str(random.sample(t_int_where,1)).replace("[","").replace("]","").replace("'","")
        return in_join_where
    
    def regular_2table(self):
        query_where = self.q_join_where()
        tag_where = self.t_join_where()

        table_list = ['regular_table_1','stable_1_1','regular_table_2','stable_1_2','regular_table_3','stable_1_3','stable_1_4','stable_2_1','stable_2_2','regular_table_null','stable_null_data_1','stable_1','stable_2']
        stable_list = ['stable_1','stable_2','stable_null_data']
        table1 = str(random.sample(table_list,1)).replace("[","").replace("]","").replace("'","")
        table2 = str(random.sample(table_list,1)).replace("[","").replace("]","").replace("'","")
        if (table1 not in stable_list) and (table2 not in stable_list):
            q_where = query_where
        else:
            q_where = tag_where + query_where

        q_in_where = self.in_join_where()

        where2 = self.regular_where()
        column_hanshu = where2[0]

        q_like_match = where2[3]
        time_window = where2[5]
        og_by = where2[6]
        limit_offset = where2[7]
        return(column_hanshu,table1,table2,q_where,q_like_match,q_in_where,time_window,og_by,limit_offset)

    def altertable(self):
        int_column = ['(q_int)','(q_bigint)','(q_smallint)','(q_tinyint)','(q_float)','(q_double)','(q_bool)','(q_bool_null)','(q_ts_null)','(q_ts)','(q_int_null)','(q_bigint_null)','(q_smallint_null)','(q_tinyint_null)','(q_float_null)','(q_double_null)']
        bia_column = ['(q_binary)','(q_nchar)','(q_binary_null)','(q_nchar_null)']
        
        columns = int_column + bia_column     
        column = str(random.sample(columns,1)).replace("[","").replace("]","").replace("(","").replace(")","").replace("'","")  

        tag_column = ['(t_int)','(t_bigint)','(t_smallint)','(t_tinyint)','(t_float)','(t_double)','(t_bool)','(t_binary)','(t_nchar)','(t_ts)']
        tag = str(random.sample(tag_column,1)).replace("[","").replace("]","").replace("(","").replace(")","").replace("'","") 
        
        al_column = str(random.sample(bia_column,1)).replace("[","").replace("]","").replace("(","").replace(")","").replace("'","") 
        al_tag = ['(t_binary)','(t_nchar)']
        al_tag = str(random.sample(al_tag,1)).replace("[","").replace("]","").replace("(","").replace(")","").replace("'","") 

        table_list = ['regular_table_1','regular_table_2','regular_table_null']
        r_table = str(random.sample(table_list,1)).replace("[","").replace("]","").replace("'","")   

        stable_list = ['stable_1','stable_2']
        s_table = str(random.sample(stable_list,1)).replace("[","").replace("]","").replace("'","")  

        if self.NUM%15 == 0:
            self.execution_sql("ALTER TABLE %s DROP COLUMN %s" %(r_table,column))
        elif self.NUM%15 == 1: 
            self.execution_sql("ALTER STABLE %s DROP COLUMN %s" %(s_table,column))
        elif self.NUM%15 == 2:
            self.execution_sql("ALTER STABLE %s DROP TAG %s" %(s_table,tag))
        elif self.NUM%15 == 3:
            self.execution_sql("ALTER TABLE %s MODIFY COLUMN %s nchar(200)" %(r_table,al_column)) 
        elif self.NUM%15 == 4:
            self.execution_sql("ALTER STABLE %s MODIFY COLUMN %s binary(200)" %(s_table,al_column)) 
        elif self.NUM%15 == 5:
            self.execution_sql("ALTER STABLE %s MODIFY TAG %s binary(200)" %(s_table,al_tag)) 
        else:
            self.execution_sql("ALTER TABLE %s DROP TAG %s" %(s_table,tag))


#tdWhere_makesql = TDWhere_makesql()