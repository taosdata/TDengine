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
from faker import Faker

from taostest import TDCase

class TDWhere():
    def tags(self) :
		
        return ""

    def author(self) -> str:

        return "Guo Xiangyang"

    def desc(self) -> str:
        case_description = '''
        case1<xyguo>:where condition 
        '''
        return case_description
        
    #basic_param
    updatecfgDict={'maxSQLLength':1048576}
    NUM = random.randint(1, 30)
    print(NUM) 

    def column_tag(self):
        int_column = ['(q_int)','(q_bigint)','(q_smallint)','(q_tinyint)','(q_float)','(q_double)','(q_int_null)','(q_bigint_null)','(q_smallint_null)','(q_tinyint_null)','(q_float_null)','(q_double_null)']
        bia_column = ['(*)','(_c0)','(_C0)','(q_bool)','(q_binary)','(q_nchar)','(q_ts)','(q_bool_null)','(q_binary_null)','(q_nchar_null)','(q_ts_null)']
        tag_column = ['(tbname)','(loc)','(t_int)','(t_bigint)','(t_smallint)','(t_tinyint)','(t_float)','(t_double)','(t_bool)','(t_binary)','(t_nchar)','(t_ts)']
        column_tag = int_column + bia_column + tag_column  

        return  column_tag

    # column and tag query
    # **int + floot_dou + other
    # q_where + t_where | q_where_null + t_where_null [前2组union] | q_where_all + t_where_all[自己union]
    def q_where(self):  
        q_int_where = ['q_bigint >= -9223372036854775807 and ' , 'q_bigint <= 9223372036854775807 and ',
        'q_smallint >= -32767 and ', 'q_smallint <= 32767 and ',
        'q_tinyint >= -127 and ' , 'q_tinyint <= 127 and ' , 
        'q_int <= 2147483647 and ' , 'q_int >= -2147483647 and ',
        'q_smallint between -32767 and 32767 and ', ' q_int between -2147483647 and 2147483647 and ',
        'q_bigint between  -9223372036854775807 and 9223372036854775807 and ','q_tinyint between -127 and 127  and ',
        'q_tinyint != 128 and ','q_smallint != 88888 and ','q_int != 8888888888 and ','q_bigint != 9999972036854775807 and ',
        'q_bigint is not null and ' , 'q_int is not null and ' , 'q_smallint is not null and ' , 'q_tinyint is not null and ' ,]

        q_fl_do_where = ['q_float >= -3.4E38 and ','q_float <= 3.4E38 and ', 'q_double >= -1.7E308 and ','q_double <= 1.7E308 and ', 
        'q_float between -3.4E38 and 3.4E38 and ','q_double between -1.7E308 and 1.7E308 and ' ,
        'q_float is not null and ' ,'q_double is not null and ' ,]

        q_nc_bi_bo_ts_where = [ 'q_bool is not null and ' ,'q_binary is not null and ' ,'q_nchar is not null and ' ,
        'ts is not null and' ,  'ts <= now +1h and ' , 'ts >= 1600000000000 and ' , ' ts between 1600000000000 and now +1h  and ' ,
        '_c0 is not null and ' ,  '_c0 <= now +1h and ' , '_c0 >= 1600000000000 and ' , ' _c0 between 1600000000000 and now +1h  and ' ,
        '_C0 is not null and' ,  '_C0 <= now +1h and ' ,  '_C0 >= 1600000000000 and ', ' _C0 between 1600000000000 and now +1h  and ' ,
        'q_ts is not null and' ,  'q_ts <= now +1h and ' , 'q_ts >= 1600000000000 and ', ' q_ts between 1600000000000 and now +1h  and ' ,]
        
        q_where = random.sample(q_int_where,4) + random.sample(q_fl_do_where,2) + random.sample(q_nc_bi_bo_ts_where,3)

        q_like = ['q_binary like \'binary%\' and','(q_binary like \'binary%\'  or q_nchar = \'0\'  or q_binary = \'binary_\' ) and','q_nchar like \'nchar%\' and','(q_nchar like \'nchar%\' or q_binary = \'0\'  or q_nchar = \'nchar_\' ) and',]
        q_match = ['q_binary match \'binary\' and','q_binary nmatch \'binarynchar\' and','q_nchar match \'nchar\' and','q_nchar nmatch \'binarynchar\' and',]
        q_like_match = random.sample(q_like,1) + random.sample(q_match,1)
        q_like_match = random.sample(q_like_match,1)

        q_tinyint_list=[]
        for i in range(-300,300):
            q_tinyint_list.append(i)
        q_tinyint_list = "q_tinyint in (" + str(q_tinyint_list).replace("[","").replace("]","") + ")"

        q_in_where = [q_tinyint_list,'q_bool in (0 , 1) ' ,  'q_bool in ( true , false) ' ,' (q_bool = true or  q_bool = false or q_bool is null)' , '(q_bool = 0 or q_bool = 1  or q_bool is null)',]
        #q_in_where = ['q_bool in (0 , 1) ' ,  'q_bool in ( true , false) ' ,' (q_bool = true or  q_bool = false)' , '(q_bool = 0 or q_bool = 1)',]#TD-16400
        q_in = random.sample(q_in_where,1)
        
        return(q_where,q_like_match,q_in)

    def q_where_null(self):  

        q_int_where = ['q_bigint < -9223372036854775807 and ' , 'q_bigint > 9223372036854775807 and ',
        'q_smallint < -32767 and ', 'q_smallint > 32767 and ',
        'q_tinyint < -127 and ' , 'q_tinyint > 127 and ' , 
        'q_int > 2147483647 and ' , 'q_int < -2147483647 and ',
        'q_bigint between  9223372036854775807 and -9223372036854775807 and ','q_smallint between 32767 and -32767 and ',
        'q_int between 2147483647 and -2147483647 and ','q_tinyint between 127 and -127  and ',
        'q_tinyint = 128 and ','q_smallint = 88888 and ','q_int = 8888888888 and ','q_bigint = 9999972036854775807 and ',
        'q_tinyint == 128 and ','q_smallint == 88888 and ','q_int == 8888888888 and ','q_bigint == 9999972036854775807 and ',
        'q_bigint is null and ' , 'q_int is null and ' , 'q_smallint is null and ' , 'q_tinyint is null and ' ,
        'q_bigint_null is not null and ' , 'q_int_null is not null and ' , 'q_smallint_null is not null and ' , 'q_tinyint_null is not null and ' ,]

        q_fl_do_where = ['q_float < -3.4E38 and ','q_float > 3.4E38 and ', 'q_double < -1.7E308 and ','q_double > 1.7E308 and ', 
        'q_float between 3.4E38 and -3.4E38 and ','q_double between 1.7E308 and -1.7E308 and ' ,
        'q_float_null is not null and ' ,'q_double_null is not null and ' ,]

        q_nc_bi_bo_ts_where = [ 'q_bool is null and ' ,'q_binary is null and ' ,'q_nchar is null and ' ,
        'ts is null and' ,  'ts >= now +100h and ' , 'ts <= 1600000000000 and ' ,
        '_c0 is null and ' ,  '_c0 >= now +100h and ' , '_c0 <= 1600000000000 and ' , ' _c0 between now +1h and 1600000000000 and ' ,
        '_C0 is null and' ,  '_C0 >= now +100h and ' ,  '_C0 <= 1600000000000 and ', ' _C0 between now +1h and 1600000000000 and ' ,
        'q_ts is null and' ,  'q_ts >= now +100h and ' , 'q_ts <= 1600000000000 and ', ' q_ts between now +1h and 1600000000000 and ' ,
        'q_bool_null is not null and ' ,'q_binary_null is not null and ' ,'q_nchar_null is not null and ' ,]
        
        q_where_null = random.sample(q_int_where,4) + random.sample(q_fl_do_where,2) + random.sample(q_nc_bi_bo_ts_where,3)

        q_like = ['q_binary like \'binary_\' and','q_binary like \'binarynchar%\' and','q_nchar like \'nchar_\' and','q_nchar like \'binarynchar%\' and',]
        q_match = ['q_binary nmatch \'binary\' and','q_binary match \'binarynchar\' and','q_nchar nmatch \'nchar\' and','q_nchar match \'binarynchar\' and',]
        q_like_match = random.sample(q_like,1) + random.sample(q_match,1)
        q_like_match_null = random.sample(q_like_match,1)

        q_tinyint_list=[]
        for i in range(1000,2000):
            q_tinyint_list.append(i)
        q_tinyint_list = "q_tinyint in (" + str(q_tinyint_list).replace("[","").replace("]","") + ")"
            
        q_in_where = [q_tinyint_list,'q_bool in (0 , 1) ' ,  'q_bool in ( true , false) ' ,' (q_bool = true or  q_bool = false or q_bool is null)' , '(q_bool = 0 or q_bool = 1 or q_bool is null)' ,]
        #q_in_where = ['q_bool in (0 , 1) ' ,  'q_bool in ( true , false) ' ,' (q_bool = true or  q_bool = false)' , '(q_bool = 0 or q_bool = 1)' ,]#TD-16400
        q_in_null = random.sample(q_in_where,1)

        return(q_where_null,q_like_match_null,q_in_null)

    def t_where(self):   
        t_int_where = ['t_bigint >= -9223372036854775807 and ' , 't_bigint <= 9223372036854775807 and ',
        't_smallint >= -32767 and ', 't_smallint <= 32767 and ',
        't_tinyint >= -127 and ' , 't_tinyint <= 127 and ' , 
        't_int <= 2147483647 and ' , 't_int >= -2147483647 and ',
        't_bigint between  -9223372036854775807 and 9223372036854775807 and ','t_smallint between -32767 and 32767 and ', 
        't_int between -2147483647 and 2147483647 and ','t_tinyint between -127 and 127  and ',        
        't_tinyint != 128 and ','t_smallint != 88888 and ','t_int != 8888888888 and ',
        't_bigint is not null and ' , 't_int is not null and ' , 't_smallint is not null and ' , 't_tinyint is not null and ' ,]
        #not support:TD-16661  't_bigint != 9999972036854775807 and ',

        t_fl_do_where = ['t_float >= -3.4E38 and ','t_float <= 3.4E38 and ', 't_double >= -1.7E308 and ','t_double <= 1.7E308 and ', 
        't_float between -3.4E38 and 3.4E38 and ','t_double between -1.7E308 and 1.7E308 and ' ,
        't_float is not null and ' ,'t_double is not null and ' ,]

        t_nc_bi_bo_ts_where = [ 't_bool is not null and ' ,'t_binary is not null and ' ,'t_nchar is not null and ' ,
        't_ts is not null and' ,  't_ts <= now +1h and ' , 't_ts >= 0 and ',' t_ts between 0 and now +1h  and ' ,]

        t_where = random.sample(t_int_where,4) + random.sample(t_fl_do_where,2) + random.sample(t_nc_bi_bo_ts_where,2)
        
        # later
        # column_tag = self.column_tag()
        # column = str(random.sample(column_tag,1)).replace("[","").replace("]","").replace("\"","").replace("(","").replace(")","").replace("'","")
        # likes = [' LIKE ' , ' MATCH ' ,' NMATCH ',' CONTAINS ']
        # like = str(random.sample(likes,1)).replace("[","").replace("]","").replace("\"","").replace("'","")
        # conditions = ['\'1234_\' and ' , '\'abc4_\' and' , '\'1234%\' and ' , '\'a_bc4%\' and', '\'12aada@#!!34%\' and ' , '\'ab#%&%^&^*^(c4%\' and']
        # condition = str(random.sample(conditions,1)).replace("[","").replace("]","").replace("\"","")
        # t_like_match = column + like  + condition
        t_like = ['t_binary like \'binary%\' and','t_nchar like \'nchar%\' and','(t_binary like \'binary%\'  or t_nchar = \'0\' ) and','(t_nchar like \'nchar%\' or t_binary = \'0\' ) and',]
        t_match = ['t_binary match \'binary\' and','t_binary nmatch \'binarynchar\' and','t_nchar match \'nchar\' and','t_nchar nmatch \'binarynchar\' and',]
        t_match_regular = ['loc match \'<table>\' and', 'loc match \'<^qwryuiop>\' and','loc nmatch \'<qwryuiop>\' and', 'loc nmatch \'<^>\' and', #[abc] 匹配[...]的所有字符
                           't_binary match \'<binary>\' and', 't_binary match \'<^爨龘>\' and','t_binary nmatch \'<爨龘>\' and', 't_binary nmatch \'<^>\' and', #[^abc] 取反，除了[...]的其他字符
                           't_nchar match \'<nchar>\' and', 't_nchar match \'<^爨龘>\' and','t_nchar nmatch \'<爨龘>\' and', 't_nchar nmatch \'<^>\' and',
                           'loc match \'<a-z>\' and','t_binary match \'<a-z>\' and','t_nchar match \'<a-z>\' and', #[A-Z] 区间字母A到Z
                           'loc match \'<a-zA-Z>\' and','t_binary match \'<a-zA-Z>\' and','t_nchar match \'<a-zA-Z>\' and',
                           'loc match \'.\' and','t_binary match \'.\' and','t_nchar match \'.\' and',  # . 匹配除（\n换行符 \r 回车符）的任何单个字符
                           'loc match \'.*\' and','t_binary match \'.*\' and','t_nchar match \'.*\' and',  # . 匹配除（\n换行符 \r 回车符）的任何单个字符                           
                           #'loc match \'<.\n>\' and','t_binary match \'<.\n>\' and','t_nchar match \'<.\n>\' and',  # 要匹配包括 '\n' 在内的任何字符，请使用象 '[.\n]' 的模式。目前不支持
                           'loc match \'a|b|c|d|e|f|g|h|i|j|k|l|m|n|o|p|q|r|s|t|u|v|w|x|y|z\' and','loc match \'a|b|c|d|e|f|g|h|i|j|k|l|m|n|o|p|q|r|s|t|u|v|w|x|y|z|A|B|C|D|E|F|G|H|I|J|K|L|M|N|O|P|Q|R|S|T|U|V|W|X|Y|Z\' and',  #  | 两项间的一个
                           't_binary match \'a|b|c|d|e|f|g|h|i|j|k|l|m|n|o|p|q|r|s|t|u|v|w|x|y|z\' and','t_binary match \'a|b|c|d|e|f|g|h|i|j|k|l|m|n|o|p|q|r|s|t|u|v|w|x|y|z|A|B|C|D|E|F|G|H|I|J|K|L|M|N|O|P|Q|R|S|T|U|V|W|X|Y|Z\' and',
                           't_nchar match \'a|b|c|d|e|f|g|h|i|j|k|l|m|n|o|p|q|r|s|t|u|v|w|x|y|z\' and','t_nchar match \'a|b|c|d|e|f|g|h|i|j|k|l|m|n|o|p|q|r|s|t|u|v|w|x|y|z|A|B|C|D|E|F|G|H|I|J|K|L|M|N|O|P|Q|R|S|T|U|V|W|X|Y|Z\' and',
                           '(loc match \'\\s\' or loc match \'\\S\' ) and','(t_binary match \'\\s\' or t_binary match \'\\S\' ) and','(t_nchar match \'\\s\' or t_nchar match \'\\S\' ) and', # \s \S 匹配所有，\s所有空白符，包括换行 \S非空白符，不包括换行
                           '(loc nmatch \'\\s\' ) and','(t_binary nmatch \'\\s\') and','(t_nchar nmatch \'\\s\' ) and', 
                           '(loc match \'\\w\' or loc match \'\\W\' ) and','(t_binary match \'\\w\' or t_binary match \'\\W\' ) and','(t_nchar match \'\\w\' or t_nchar match \'\\W\' ) and', # \w 匹配字母数字及下划线  \W 匹配非字母数字及下划线
                           '(loc match \'\\w\') and','(t_binary match \'\\w\') and','(t_nchar match \'\\w\') and',] # \w 匹配字母数字及下划线  \W 匹配非字母数字及下划线
                           #'(loc match \'\d\' or loc match \'\D\' ) and','(t_binary match \'\d\' or t_binary match \'\D\' ) and','(t_nchar match \'\d\' or t_nchar match \'\D\' ) and',] # \d匹配任意数字，等价于 [0-9]. \D	匹配任意非数字
                            # < > repalce [ ]
                            #我们现在支持的是POSIX标准的正则表达式，POSXI标准里是没有/s/d/w等的支持，同时因为我们用的是GNU的库，GNU又有自己的正则表达式扩展，在它的扩展里支持/s/w，但是不支持/d，所以目前就是这个样子
        t_like_match = random.sample(t_like,1) + random.sample(t_match,1) + random.sample(t_match_regular,2)
        t_like_match = random.sample(t_like_match,1)

        t_tinyint_list=[]
        for i in range(-300,300):
            t_tinyint_list.append(i)
        t_tinyint_list = "t_tinyint in (" + str(t_tinyint_list).replace("[","").replace("]","") + ")"

        t_in_where = [t_tinyint_list, 't_bool in (0 , 1) ' ,  't_bool in ( true , false) ' ,' (t_bool = true or  t_bool = false)' , '(t_bool = 0 or t_bool = 1)',]
        #t_in_where = [ 't_bool in (0 , 1) ' ,  't_bool in ( true , false) ' ,' (t_bool = true or  t_bool = false)' , '(t_bool = 0 or t_bool = 1)',]#TD-16400
        t_in = random.sample(t_in_where,1)

        return(t_where,t_like_match,t_in)

    def t_where_null(self):   
        t_int_where = ['t_bigint < -9223372036854775807 and ' , 't_bigint > 9223372036854775807 and ',
        't_smallint < -32767 and ', 't_smallint > 32767 and ',
        't_tinyint < -127 and ' , 't_tinyint > 127 and ' , 
        't_int > 2147483647 and ' , 't_int < -2147483647 and ',
        't_bigint between  9223372036854775807 and -9223372036854775807 and ','t_smallint between 32767 and -32767 and ',
        't_int between 2147483647 and -2147483647 and ', 't_tinyint between 127 and -127  and ',
        't_tinyint = 128 and ','t_smallint = 88888 and ','t_int = 8888888888 and ','t_bigint = 9999972036854775807 and ',
        't_tinyint == 128 and ','t_smallint == 88888 and ','t_int == 8888888888 and ','t_bigint == 9999972036854775807 and ',
        't_bigint is null and ' , 't_int is null and ' , 't_smallint is null and ' , 't_tinyint is null and ' ,]

        t_fl_do_where = ['t_float < -3.4E38 and ','t_float > 3.4E38 and ', 't_double < -1.7E308 and ','t_double > 1.7E308 and ', 
        't_float between 3.4E38 and -3.4E38 and ','t_double between 1.7E308 and -1.7E308 and ' ,
        't_float is null and ' ,'t_double is null and ' ,]

        t_nc_bi_bo_ts_where = [ 't_bool is null and ' ,'t_binary is null and ' ,'t_nchar is null and ' ,
        't_ts is null and' ,  't_ts >= now +100h and ' , 't_ts < 0 and ',  't_ts between now +100h and 0 and ',]

        t_where_null = random.sample(t_int_where,4) + random.sample(t_fl_do_where,2) + random.sample(t_nc_bi_bo_ts_where,2)
        
        t_like = ['t_binary like \'binary_\' and','t_binary like \'0%\' and','t_nchar like \'nchar_\' and','t_nchar like \'0%\' and',]
        t_match = ['t_binary nmatch \'binary\' and','t_binary match \'binarynchar\' and','t_nchar nmatch \'nchar\' and','t_nchar match \'binarynchar\' and',]
        t_like_match = random.sample(t_like,1) + random.sample(t_match,1)
        t_match_regular = ['loc match \'(elbats)\' and', 'loc match \'(^qwertyuiopzxcvnmdfghj)\' and',]
        t_like_match = random.sample(t_like,1) + random.sample(t_match,1) + random.sample(t_match_regular,2)
        t_like_match_null = random.sample(t_like_match,1)

        t_tinyint_list=[]
        for i in range(1000,2000):
            t_tinyint_list.append(i)
        t_tinyint_list = "t_tinyint in (" + str(t_tinyint_list).replace("[","").replace("]","") + ")"
        
        t_in_where = [t_tinyint_list , 't_bool in (0 , 1) ' ,  't_bool in ( true , false) ' ,' (t_bool = true or  t_bool = false)' , '(t_bool = 0 or t_bool = 1)',]
        #t_in_where = ['t_bool in (0 , 1) ' ,  't_bool in ( true , false) ' ,' (t_bool = true or  t_bool = false)' , '(t_bool = 0 or t_bool = 1)',] #TD-16400
        t_in_null = random.sample(t_in_where,1)

        return(t_where_null,t_like_match_null,t_in_null)

    def hanshu_int(self):       
        hanshu = ['MIN','AVG','MAX','COUNT','SUM','STDDEV','FIRST','LAST','LAST_ROW','','SPREAD','CEIL','FLOOR','ROUND']
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double_null)','(q_bigint_null)','(q_smallint_null)','(q_tinyint_null)','(q_int_null)','(q_float_null)','(q_double_null)']        
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        hanshu_column = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return hanshu_column

    def hanshu_all(self):       
        hanshu = ['COUNT','SUM','STDDEV','FIRST','LAST','LAST_ROW','','SPREAD','CEIL','FLOOR','ROUND']
        column = ['(*)','(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double_null)','(q_bigint_null)','(q_smallint_null)','(q_tinyint_null)','(q_int_null)','(q_float_null)','(q_double_null)']        
        hanshu_column_all = random.sample(hanshu,1)+random.sample(column,1)
        return hanshu_column_all

    # stable_group by.  table_ok
    def hanshu_stable(self):       
        hanshu = ['TWA','IRATE','STDDEV','INTERP','DIFF']
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double_null)','(q_bigint_null)','(q_smallint_null)','(q_tinyint_null)','(q_int_null)','(q_float_null)','(q_double_null)']        
        hanshu_column_stable = random.sample(hanshu,1)+random.sample(column,1)
        return hanshu_column_stable

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

        hanshu_1 = self.hanshu_int()

        hanshu_s = ''
        for i in range(3):
            hanshu_1 = self.hanshu_int()
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
        #方法重新，因此pass
        pass    
        # time = ['1','2','3','4','5','6','7','8','9','10']
        # unit = ['a','s','m','h','d','w','n','y']
        # td_base = str(random.sample(time,1)+random.sample(unit,1)).replace("[","").replace("]","").replace("'","").replace(", ","")
        
        # td_interval = td_base
        # td_interval = 'interval'+'(' +td_interval + ')'

        # td_sliding = td_base
        # td_sliding = 'sliding'+'(' +td_sliding + ')'

        # fill = ['NONE','VALUE,100','PREV','NULL','LINEAR','NEXT']
        # td_fill = str(random.sample(fill,1)).replace("[","").replace("]","").replace("'","").replace(", ","")
        # td_fill = 'Fill' +'(' +td_fill + ')'

        # td_session = td_base
        # td_session = 'SESSION'+'(ts,'+td_session + ')'

        # if self.NUM == 1:
        #     time_window = td_interval
        # elif self.NUM == 2:
        #     time_window = td_interval + ' ' + td_sliding
        # elif self.NUM == 3:
        #     time_window = td_fill 
        # elif self.NUM == 4:
        #     time_window = td_interval + ' ' + td_fill 
        # elif self.NUM == 5 :
        #     time_window = td_interval + ' ' + td_sliding + ' ' + td_fill 
        # else:
        #     time_window = td_session
        
        # return time_window
        
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

    def time_window_new(self,i):  
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
                        
        elif i == 6:
            time_window = sliding_interval + ' ' + single_fill 
        elif i == 7:
            time_window = sliding_interval + ' ' + single_sliding + ' ' + single_fill 
        elif i == 8:
            time_window = sliding_interval_offset + ' ' + single_fill 
        elif i == 9:
            time_window = sliding_interval_offset + ' ' + single_sliding + ' ' + single_fill 
            
        elif i == 61:
            time_window = sliding_interval + ' ' + single_fill_f 
        elif i == 71:
            time_window = sliding_interval + ' ' + single_sliding + ' ' + single_fill_f 
        elif i == 81:
            time_window = sliding_interval_offset + ' ' + single_fill_f 
        elif i == 91:
            time_window = sliding_interval_offset + ' ' + single_sliding + ' ' + single_fill_f 
            
        elif i == 62:
            time_window = sliding_interval + ' ' + single_fill_not_f 
        elif i == 72:
            time_window = sliding_interval + ' ' + single_sliding + ' ' + single_fill_not_f 
        elif i == 82:
            time_window = sliding_interval_offset + ' ' + single_fill_not_f 
        elif i == 92:
            time_window = sliding_interval_offset + ' ' + single_sliding + ' ' + single_fill_not_f 
                                    
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
            
        #event_window    #stable-right
        elif i == 31:
            time_window = single_event_window_stable_1
        elif i == 32:
            time_window = single_event_window_stable_2
        elif i == 33:
            time_window = single_event_window_stable_3
        elif i == 34:
            time_window = single_event_window_stable_4
        elif i == 35:
            time_window = single_event_window_stable_5
            
        #event_window    #table-right
        elif i == 41:
            time_window = single_event_window_table_1
        elif i == 42:
            time_window = single_event_window_table_2
        elif i == 43:
            time_window = single_event_window_table_3
        elif i == 44:
            time_window = single_event_window_table_4
        elif i == 45:
            time_window = single_event_window_table_5
         
        #event_window_error    
        elif i == 50:
            time_window = single_event_window_stable_1 + ' ' + single_state_window 
        elif i == 51:
            time_window = single_event_window_stable_1 + ' ' + single_interval  
        elif i == 52:
            time_window = single_event_window_stable_1 + ' ' + single_interval_offset  
        elif i == 53:
            time_window = single_event_window_stable_1 + ' ' + single_sliding  
        elif i == 54:
            time_window = single_event_window_stable_1 + ' ' + single_session  
        elif i == 55:
            time_window = single_state_window + ' '  +  single_event_window_stable_1  
        elif i == 56:
            time_window = single_interval + ' '  +  single_event_window_stable_1  
        elif i == 57:
            time_window = single_interval_offset + ' '  +  single_event_window_stable_1     
        elif i == 58:
            time_window = single_sliding + ' '  +  single_event_window_stable_1    
        elif i == 59:
            time_window = single_session + ' '  +  single_event_window_stable_1  

                               
        return time_window
    
    
    def time_window_orderby(self,i):  
        #同time_window_new，但为了增加统计，减少大单位的，数字后面的时间单位可以是 u(微秒)、a(毫秒)、s(秒)、m(分)、h(小时)、d(天)、w(周)。 
        #在指定降频操作（down sampling）的时间窗口（interval）时，时间单位还可以使用 n(自然月) 和 y(自然年)。     
        interval_n, offset_n, sliding_n = [random.randrange(20,100)]  , [random.randrange(1,20)] , [random.randrange(1,20)] 
        time_window = ''
                
        #单interval
        interval_units = ['s','m','a']
        unit = random.sample(interval_units,1)
        interval_base = str(interval_n + unit).replace("[","").replace("]","").replace("'","").replace(", ","")
        single_interval = 'interval'+'(' +interval_base + ')'
        
        #单interval+offset
        offset_base = str(offset_n + unit).replace("[","").replace("]","").replace("'","").replace(", ","")
        single_interval_offset = 'interval'+'(' +interval_base + ',' + offset_base + ')'

        #interval + sliding
        interval_sliding_units = ['s','m','a'] #有限制，所以需要删除几个
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
        session_units = ['s','a']
        session_unit = random.sample(session_units,1)
        session_base = str(interval_n + session_unit).replace("[","").replace("]","").replace("'","").replace(", ","")
        single_session = 'SESSION'+'(ts,'+ session_base + ')'
        
        #单state_window
        func = ['STATE_WINDOW']
        window_support_types = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)'] #其余不支持
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
                        
        elif i == 6:
            time_window = sliding_interval + ' ' + single_fill 
        elif i == 7:
            time_window = sliding_interval + ' ' + single_sliding + ' ' + single_fill 
        elif i == 8:
            time_window = sliding_interval_offset + ' ' + single_fill 
        elif i == 9:
            time_window = sliding_interval_offset + ' ' + single_sliding + ' ' + single_fill 
            
        elif i == 61:
            time_window = sliding_interval + ' ' + single_fill_f 
        elif i == 71:
            time_window = sliding_interval + ' ' + single_sliding + ' ' + single_fill_f 
        elif i == 81:
            time_window = sliding_interval_offset + ' ' + single_fill_f 
        elif i == 91:
            time_window = sliding_interval_offset + ' ' + single_sliding + ' ' + single_fill_f 
            
        elif i == 62:
            time_window = sliding_interval + ' ' + single_fill_not_f 
        elif i == 72:
            time_window = sliding_interval + ' ' + single_sliding + ' ' + single_fill_not_f 
        elif i == 82:
            time_window = sliding_interval_offset + ' ' + single_fill_not_f 
        elif i == 92:
            time_window = sliding_interval_offset + ' ' + single_sliding + ' ' + single_fill_not_f 
                                    
        #部分正确的，超级表错误，子表，普通表正确     
        elif i == 21:
            time_window = single_session
        elif i == 22:
            time_window = single_state_window
            
        #event_window    #stable-right
        elif i == 31:
            time_window = single_event_window_stable_1
        elif i == 32:
            time_window = single_event_window_stable_2
        elif i == 33:
            time_window = single_event_window_stable_3
        elif i == 34:
            time_window = single_event_window_stable_4
        elif i == 35:
            time_window = single_event_window_stable_5
            
        #event_window    #table-right
        elif i == 41:
            time_window = single_event_window_table_1
        elif i == 42:
            time_window = single_event_window_table_2
        elif i == 43:
            time_window = single_event_window_table_3
        elif i == 44:
            time_window = single_event_window_table_4
        elif i == 45:
            time_window = single_event_window_table_5
                               
        return time_window
    
    
    def groupby(self):    
        int_column = ['(q_int)','(q_bigint)','(q_smallint)','(q_tinyint)','(q_float)','(q_double)','(q_int_null)','(q_bigint_null)','(q_smallint_null)','(q_tinyint_null)','(q_float_null)','(q_double_null)']
        bia_column = ['(*)','(_c0)','(_C0)','(q_bool)','(q_binary)','(q_nchar)','(q_ts)','(q_bool_null)','(q_binary_null)','(q_nchar_null)','(q_ts_null)']
        tag_column = ['(tbname)','(loc)','(t_int)','(t_bigint)','(t_smallint)','(t_tinyint)','(t_float)','(t_double)','(t_bool)','(t_binary)','(t_nchar)','(t_ts)']
        columns = int_column + bia_column + tag_column
        column = str(random.sample(columns,1)).replace("[","").replace("]","").replace("(","").replace(")","").replace("'","")

        if self.NUM%10 == 4:
            groupby = " group by %s " %column
        elif self.NUM%10 == 5 :
            groupby = " group by tbname , %s " %column
        elif self.NUM%10 == 6 :
            groupby = " group by tbname , %s order by %s " %(column,column)
        elif self.NUM%10 == 7 :
            groupby = " group by tbname , %s order by %s desc" %(column,column)
        else:
            groupby = " group by "
        
        return groupby

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

    def having(self):    
        int_column = ['(q_int)','(q_bigint)','(q_smallint)','(q_tinyint)','(q_float)','(q_double)','(q_int_null)','(q_bigint_null)','(q_smallint_null)','(q_tinyint_null)','(q_float_null)','(q_double_null)']
        bia_column = ['(*)','(_c0)','(_C0)','(q_bool)','(q_binary)','(q_nchar)','(q_ts)','(q_bool_null)','(q_binary_null)','(q_nchar_null)','(q_ts_null)']
        tag_column = ['(tbname)','(loc)','(t_int)','(t_bigint)','(t_smallint)','(t_tinyint)','(t_float)','(t_double)','(t_bool)','(t_binary)','(t_nchar)','(t_ts)']
        columns = int_column + bia_column + tag_column
        column = str(random.sample(columns,1)).replace("[","").replace("]","").replace("(","").replace(")","").replace("'","")

        if self.NUM%10 == 1:
            having = " order by ts "
        elif self.NUM%10 == 2:
            having = " order by %s " %column
        elif self.NUM%10 == 3:
            having = " order by %s desc " %column
        elif self.NUM%10 == 4:
            having = " group by %s " %column
        elif self.NUM%10 == 5 :
            having = " group by tbname , %s " %column
        elif self.NUM%10 == 6 :
            having = " group by tbname , %s order by %s " %(column,column)
        elif self.NUM%10 == 7 :
            having = " group by tbname , %s order by %s desc" %(column,column)
        else:
            having = "  "
        
        return having

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
        #return all data     
        regular_q_where = self.q_where()
        
        q_where = random.sample(regular_q_where[0],5)        

        if self.NUM%3 ==0:
            q_like_match = str(regular_q_where[1]).replace("[","").replace("]","").replace("\"","").replace("<","[").replace(">","]")
        else :
            q_like_match = " "

        q_in_where = str(regular_q_where[2]).replace("[","").replace("]","").replace("'","")

        column = self.column()
        hanshu_column = self.hanshu_int()
        time_window = self.time_window()
        og_by = self.orderby_groupby()
        having = self.having()
        limit_offset = self.limit_offset()
        
        return(column,hanshu_column,q_where,q_like_match,q_in_where,time_window,og_by,having,limit_offset)

    def regular_where_null(self):  
        #return null data      
        regular_q_where_null = self.q_where_null()
        
        q_where_null = random.sample(regular_q_where_null[0],5) 

        if self.NUM%3 ==0:
            q_like_match_null = str(regular_q_where_null[1]).replace("[","").replace("]","").replace("\"","").replace("<","[").replace(">","]")
        else :
            q_like_match_null = " "

        q_in_where_null = str(regular_q_where_null[2]).replace("[","").replace("]","").replace("'","")

        column = self.column()
        hanshu_column = self.hanshu_int()
        time_window = self.time_window()
        og_by = self.orderby_groupby()
        limit_offset = self.limit_offset()

        return(column,hanshu_column,q_where_null,q_like_match_null,q_in_where_null,time_window,og_by,limit_offset)

    def regular_where_all_and_null(self):  
        #return all data + #return null data  
        regular_q_where = self.q_where()        
        q_where = random.sample(regular_q_where[0],5) 

        if self.NUM%3 ==0:
            q_like_match = str(regular_q_where[1]).replace("[","").replace("]","").replace("\"","").replace("<","[").replace(">","]")
        else :
            q_like_match = " "

        q_in_where = str(regular_q_where[2]).replace("[","").replace("]","").replace("'","")


        regular_q_where_null = self.q_where_null()        
        q_where_null = random.sample(regular_q_where_null[0],5) 

        if self.NUM%3 ==0:
            q_like_match_null = " "
        else :
            q_like_match_null = str(regular_q_where_null[1]).replace("[","").replace("]","").replace("\"","").replace("<","[").replace(">","]")
            
        q_in_where_null = str(regular_q_where_null[2]).replace("[","").replace("]","").replace("'","")

        column = self.column()
        hanshu_column = self.hanshu_int()
        time_window = self.time_window()
        og_by = self.orderby_groupby()
        limit_offset = self.limit_offset()

        return(column,hanshu_column,q_where,q_like_match,q_in_where,q_where_null,q_like_match_null,q_in_where_null,time_window,og_by,limit_offset)

    def stable_where(self):  
        #return all data      
        stable_q_where = self.q_where()
        stable_t_where = self.t_where()

        qt_where = random.sample(stable_q_where[0],3) + random.sample(stable_t_where[0],3)
        
        if self.NUM%3 ==0:
            qt_like_match = str(stable_q_where[1]).replace("[","").replace("]","").replace("\"","").replace("<","[").replace(">","]")
        elif self.NUM%3 ==1:
            qt_like_match = str(stable_t_where[1]).replace("[","").replace("]","").replace("\"","").replace("<","[").replace(">","]")
        else :
            qt_like_match = " "

        qt_in_where = random.sample(stable_q_where[2],1) + random.sample(stable_t_where[2],1)
        qt_in_where = str(random.sample(qt_in_where,1)).replace("[","").replace("]","").replace("'","")

        column = self.column()
        hanshu_column = self.hanshu_int()
        time_window = self.time_window()
        og_by = self.orderby_groupby()
        limit_offset = self.limit_offset()

        return(column,hanshu_column,qt_where,qt_like_match,qt_in_where,time_window,og_by,limit_offset)

    def stable_where_null(self):  
        #return null data      
        stable_q_where_null = self.q_where_null()
        stable_t_where_null = self.t_where_null()
        
        qt_where_null = random.sample(stable_q_where_null[0],3) + random.sample(stable_t_where_null[0],3) 

        if self.NUM%3 ==0:
            qt_like_match_null = str(stable_q_where_null[1]).replace("[","").replace("]","").replace("\"","").replace("<","[").replace(">","]")
        elif self.NUM%3 ==1:
            qt_like_match_null = str(stable_t_where_null[1]).replace("[","").replace("]","").replace("\"","").replace("<","[").replace(">","]")
        else :
            qt_like_match_null = " "

        qt_in_where_null = random.sample(stable_q_where_null[2],1) + random.sample(stable_t_where_null[2],1)
        qt_in_where_null = str(random.sample(qt_in_where_null,1)).replace("[","").replace("]","").replace("'","")

        column = self.column()
        hanshu_column = self.hanshu_int()
        time_window = self.time_window()
        og_by = self.orderby_groupby()
        limit_offset = self.limit_offset()

        return(column,hanshu_column,qt_where_null,qt_like_match_null,qt_in_where_null,time_window,og_by,limit_offset)

    def stable_where_all_and_null(self):  
        #return all data + #return null data  
        stable_q_where = self.q_where()
        stable_t_where = self.t_where()

        qt_where = random.sample(stable_q_where[0],3) + random.sample(stable_t_where[0],3)
        
        if self.NUM%3 ==0:
            qt_like_match = str(stable_q_where[1]).replace("[","").replace("]","").replace("\"","").replace("<","[").replace(">","]")
        elif self.NUM%3 ==1:
            qt_like_match = str(stable_t_where[1]).replace("[","").replace("]","").replace("\"","").replace("<","[").replace(">","]")
        else :
            qt_like_match = " "

        qt_in_where = random.sample(stable_q_where[2],1) + random.sample(stable_t_where[2],1)
        qt_in_where = str(random.sample(qt_in_where,1)).replace("[","").replace("]","").replace("'","")


        stable_q_where_null = self.q_where_null()
        stable_t_where_null = self.t_where_null()
        
        qt_where_null = random.sample(stable_q_where_null[0],3) + random.sample(stable_t_where_null[0],3) 

        if self.NUM%3 ==0:
            qt_like_match_null = str(stable_q_where_null[1]).replace("[","").replace("]","").replace("\"","").replace("<","[").replace(">","]")
        elif self.NUM%3 ==1:
            qt_like_match_null = str(stable_t_where_null[1]).replace("[","").replace("]","").replace("\"","").replace("<","[").replace(">","]")
        else :
            qt_like_match_null = " "

        qt_in_where_null = random.sample(stable_q_where_null[2],1) + random.sample(stable_t_where_null[2],1)
        qt_in_where_null = str(random.sample(qt_in_where_null,1)).replace("[","").replace("]","").replace("'","")

        column = self.column()
        hanshu_column = self.hanshu_int()
        time_window = self.time_window()
        og_by = self.orderby_groupby()
        limit_offset = self.limit_offset()

        return(column,hanshu_column,qt_where,qt_like_match,qt_in_where,qt_where_null,qt_like_match_null,qt_in_where_null,time_window,og_by,limit_offset)

    # test >=0 <=0,later
    def regular_where_all_null(self):   
        q_where = self.q_where()
        
        q_where = random.sample(q_where[0],5) 
        q_in_where = random.sample(q_where[1],1)

        q_where_null = self.q_where()
        
        q_where = random.sample(q_where[0],5) 
        q_in_where = random.sample(q_where[1],1)

        q_int_where_add = ['q_bigint >= 0 and ' , 'q_smallint >= 0 and ', 'q_tinyint >= 0 and ' ,  'q_int >= 0 and ',
        'q_bigint between  0 and 9223372036854775807 and ',' q_int between 0 and 2147483647 and ',
        'q_smallint between 0 and 32767 and ', 'q_tinyint between 0 and 127  and ',
        'q_bigint is not null and ' , 'q_int is not null and ' ,]

        q_fl_do_where_add = ['q_float >= 0 and ', 'q_double >= 0 and ' , 'q_float between 0 and 3.4E38 and ','q_double between 0 and 1.7E308 and ' ,
        'q_float is not null and ' ,]

        q_nc_bi_bo_ts_where_add = ['q_nchar is not null and ' ,'q_ts is not null and ' ,]

        q_where_add = random.sample(q_int_where_add,2) + random.sample(q_fl_do_where_add,1) + random.sample(q_nc_bi_bo_ts_where_add,1)
        
        q_int_where_sub = ['q_bigint <= 0 and ' , 'q_smallint <= 0 and ', 'q_tinyint <= 0 and ' ,  'q_int <= 0 and ',
        'q_bigint between -9223372036854775807 and 0 and ',' q_int between -2147483647 and 0 and ',
        'q_smallint between -32767 and 0 and ', 'q_tinyint between -127 and 0 and ',
        'q_smallint is not null and ' , 'q_tinyint is not null and ' ,]

        q_fl_do_where_sub = ['q_float <= 0 and ', 'q_double <= 0 and ' , 'q_float between -3.4E38 and 0 and ','q_double between -1.7E308 and 0 and ' ,
        'q_double is not null and ' ,]

        q_nc_bi_bo_ts_where_sub = ['q_bool is not null and ' ,'q_binary is not null and ' ,]

        q_where_sub = random.sample(q_int_where_sub,2) + random.sample(q_fl_do_where_sub,1) + random.sample(q_nc_bi_bo_ts_where_sub,1)

        return(q_where_add,q_where_sub)

    # test all and null,later
    def stable_where_all(self):  
        regular_where_all = self.regular_where_all()

        t_int_where_add = ['t_bigint >= 0 and ' , 't_smallint >= 0 and ', 't_tinyint >= 0 and ' ,  't_int >= 0 and ',
        't_bigint between  0 and 9223372036854775807 and ',' t_int between 0 and 2147483647 and ',
        't_smallint between 0 and 32767 and ', 't_tinyint between 0 and 127  and ',
        't_bigint is not null and ' , 't_int is not null and ' ,]

        t_fl_do_where_add = ['t_float >= 0 and ', 't_double >= 0 and ' , 't_float between 0 and 3.4E38 and ','t_double between 0 and 1.7E308 and ' ,
        't_float is not null and ' ,]

        t_nc_bi_bo_ts_where_add = ['t_nchar is not null and ' ,'t_ts is not null and ' ,]

        qt_where_add = random.sample(t_int_where_add,1) + random.sample(t_fl_do_where_add,1) + random.sample(t_nc_bi_bo_ts_where_add,1) + random.sample(regular_where_all[0],2)
        
        t_int_where_sub = ['t_bigint <= 0 and ' , 't_smallint <= 0 and ', 't_tinyint <= 0 and ' ,  't_int <= 0 and ',
        't_bigint between -9223372036854775807 and 0 and ',' t_int between -2147483647 and 0 and ',
        't_smallint between -32767 and 0 and ', 't_tinyint between -127 and 0 and ',
        't_smallint is not null and ' , 't_tinyint is not null and ' ,]

        t_fl_do_where_sub = ['t_float <= 0 and ', 't_double <= 0 and ' , 't_float between -3.4E38 and -1 and ','t_double between -1.7E308 and -1 and ' ,
        't_double is not null and ' ,]

        t_nc_bi_bo_ts_where_sub = ['t_bool is not null and ' ,'t_binary is not null and ' ,]

        qt_where_sub = random.sample(t_int_where_sub,1) + random.sample(t_fl_do_where_sub,1) + random.sample(t_nc_bi_bo_ts_where_sub,1) + random.sample(regular_where_all[1],2)

        qt_in = ['q_bool in (0 , 1) ' ,  'q_bool in ( true , false) ' ,' (q_bool = true or  q_bool = false)' , '(q_bool = 0 or q_bool = 1)', 't_bool in (0 , 1) ' ,  't_bool in ( true , false) ' ,' (t_bool = true or  t_bool = false)' , '(t_bool = 0 or t_bool = 1)',]

        hanshu_column = self.hanshu_int()

        return(qt_where_add,qt_where_sub,qt_in,hanshu_column)

    # test all and null,later
    def regular_where_all(self):     
        q_where = self.q_where()  

        q_where = random.sample(q_where[0],5) 
        q_in_where = random.sample(q_where[1],1)

        q_int_where_add = ['q_bigint >= 0 and ' , 'q_smallint >= 0 and ', 'q_tinyint >= 0 and ' ,  'q_int >= 0 and ',
        'q_bigint between  0 and 9223372036854775807 and ',' q_int between 0 and 2147483647 and ',
        'q_smallint between 0 and 32767 and ', 'q_tinyint between 0 and 127  and ',
        'q_bigint is not null and ' , 'q_int is not null and ' ,]

        q_fl_do_where_add = ['q_float >= 0 and ', 'q_double >= 0 and ' , 'q_float between 0 and 3.4E38 and ','q_double between 0 and 1.7E308 and ' ,
        'q_float is not null and ' ,]

        q_nc_bi_bo_ts_where_add = ['q_nchar is not null and ' ,'q_ts is not null and ' ,]

        q_where_add = random.sample(q_int_where_add,2) + random.sample(q_fl_do_where_add,1) + random.sample(q_nc_bi_bo_ts_where_add,1)
        
        q_int_where_sub = ['q_bigint <= 0 and ' , 'q_smallint <= 0 and ', 'q_tinyint <= 0 and ' ,  'q_int <= 0 and ',
        'q_bigint between -9223372036854775807 and 0 and ',' q_int between -2147483647 and 0 and ',
        'q_smallint between -32767 and 0 and ', 'q_tinyint between -127 and 0 and ',
        'q_smallint is not null and ' , 'q_tinyint is not null and ' ,]

        q_fl_do_where_sub = ['q_float <= 0 and ', 'q_double <= 0 and ' , 'q_float between -3.4E38 and 0 and ','q_double between -1.7E308 and 0 and ' ,
        'q_double is not null and ' ,]

        q_nc_bi_bo_ts_where_sub = ['q_bool is not null and ' ,'q_binary is not null and ' ,]

        q_where_sub = random.sample(q_int_where_sub,2) + random.sample(q_fl_do_where_sub,1) + random.sample(q_nc_bi_bo_ts_where_sub,1)

        return(q_where_add,q_where_sub)

    # test all and null,later
    def stable_where_all(self):  
        regular_where_all = self.regular_where_all()

        t_int_where_add = ['t_bigint >= 0 and ' , 't_smallint >= 0 and ', 't_tinyint >= 0 and ' ,  't_int >= 0 and ',
        't_bigint between  0 and 9223372036854775807 and ',' t_int between 0 and 2147483647 and ',
        't_smallint between 0 and 32767 and ', 't_tinyint between 0 and 127  and ',
        't_bigint is not null and ' , 't_int is not null and ' ,]

        t_fl_do_where_add = ['t_float >= 0 and ', 't_double >= 0 and ' , 't_float between 0 and 3.4E38 and ','t_double between 0 and 1.7E308 and ' ,
        't_float is not null and ' ,]

        t_nc_bi_bo_ts_where_add = ['t_nchar is not null and ' ,'t_ts is not null and ' ,]

        qt_where_add = random.sample(t_int_where_add,1) + random.sample(t_fl_do_where_add,1) + random.sample(t_nc_bi_bo_ts_where_add,1) + random.sample(regular_where_all[0],2)
        
        t_int_where_sub = ['t_bigint <= 0 and ' , 't_smallint <= 0 and ', 't_tinyint <= 0 and ' ,  't_int <= 0 and ',
        't_bigint between -9223372036854775807 and 0 and ',' t_int between -2147483647 and 0 and ',
        't_smallint between -32767 and 0 and ', 't_tinyint between -127 and 0 and ',
        't_smallint is not null and ' , 't_tinyint is not null and ' ,]

        t_fl_do_where_sub = ['t_float <= 0 and ', 't_double <= 0 and ' , 't_float between -3.4E38 and -1 and ','t_double between -1.7E308 and -1 and ' ,
        't_double is not null and ' ,]

        t_nc_bi_bo_ts_where_sub = ['t_bool is not null and ' ,'t_binary is not null and ' ,]

        qt_where_sub = random.sample(t_int_where_sub,1) + random.sample(t_fl_do_where_sub,1) + random.sample(t_nc_bi_bo_ts_where_sub,1) + random.sample(regular_where_all[1],2)

        qt_in = ['q_bool in (0 , 1) ' ,  'q_bool in ( true , false) ' ,' (q_bool = true or  q_bool = false)' , '(q_bool = 0 or q_bool = 1)', 't_bool in (0 , 1) ' ,  't_bool in ( true , false) ' ,' (t_bool = true or  t_bool = false)' , '(t_bool = 0 or t_bool = 1)',]

        hanshu_column = self.hanshu_int()

        return(qt_where_add,qt_where_sub,qt_in,hanshu_column)        



tdWhere = TDWhere()
