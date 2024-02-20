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
    updatecfgDict = {'maxSQLLength':1048576,'debugFlag': 131 ,"cDebugFlag":131,"uDebugFlag":131 ,"rpcDebugFlag":131 , "tmrDebugFlag":131 ,
    "jniDebugFlag":131 ,"simDebugFlag":131,"dDebugFlag":131, "dDebugFlag":131,"vDebugFlag":131,"mDebugFlag":131,"qDebugFlag":131,
    "wDebugFlag":131,"sDebugFlag":131,"tsdbDebugFlag":131,"tqDebugFlag":131 ,"fsDebugFlag":131 ,"fnDebugFlag":131}

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.testcasePath = os.path.split(__file__)[0]
        self.testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))

        self.num = 10
        self.fornum = 15

        self.db_nest = "nest"
        self.dropandcreateDB_random("%s" %self.db_nest, 1)

        # regular column select
        #q_select= ['ts' , '*' , 'q_int', 'q_bigint' , 'q_bigint' , 'q_smallint' , 'q_tinyint' , 'q_bool' , 'q_binary' , 'q_nchar' ,'q_float' , 'q_double' ,'q_ts ']
        self.q_select= ['q_int', 'q_bigint' , 'q_bigint' , 'q_smallint' , 'q_tinyint' , 'q_bool' , 'q_binary' , 'q_nchar' ,'q_float' , 'q_double' ,'q_ts ', 'q_int_null ', 'q_bigint_null ' , 'q_bigint_null ' , 'q_smallint_null ' , 'q_tinyint_null ' , 'q_bool_null ' , 'q_binary_null ' , 'q_nchar_null ' ,'q_float_null ' , 'q_double_null ' ,'q_ts_null ']

        # tag column select
        #t_select= ['*' , 'loc' ,'t_int', 't_bigint' , 't_bigint' , 't_smallint' , 't_tinyint' , 't_bool' , 't_binary' , 't_nchar' ,'t_float' , 't_double' ,'t_ts ']
        self.t_select= ['loc','t_int', 't_bigint' , 't_bigint' , 't_smallint' , 't_tinyint' , 't_bool' , 't_binary' , 't_nchar' ,'t_float' , 't_double' ,'t_ts ']

        # regular and tag column select
        self.qt_select=  self.q_select + self.t_select

        # distinct regular column select
        self.dq_select= ['distinct q_int', 'distinct q_bigint' , 'distinct q_smallint' , 'distinct q_tinyint' ,
                'distinct q_bool' , 'distinct q_binary' , 'distinct q_nchar' ,'distinct q_float' , 'distinct q_double' ,'distinct q_ts ']

        # distinct tag column select
        self.dt_select= ['distinct loc', 'distinct t_int', 'distinct t_bigint'  , 'distinct t_smallint' , 'distinct t_tinyint' ,
                'distinct t_bool' , 'distinct t_binary' , 'distinct t_nchar' ,'distinct t_float' , 'distinct t_double' ,'distinct t_ts ']

        # distinct regular and tag column select
        self.dqt_select= self.dq_select + self.dt_select

        # special column select
        self.s_r_select= ['_c0', '_rowts' , '_C0' ]
        self.s_s_select= ['tbname' , '_rowts' , '_c0', '_C0' ]
        self.unionall_or_union= [ ' union ' , ' union all ' ]

        # regular column where
        self.q_where = ['ts < now +1s','q_bigint >= -9223372036854775807 and q_bigint <= 9223372036854775807', 'q_int <= 2147483647 and q_int >= -2147483647',
        'q_smallint >= -32767 and q_smallint <= 32767','q_tinyint >= -127 and q_tinyint <= 127','q_float >= -1.7E308 and q_float <= 1.7E308',
        'q_double >= -1.7E308 and q_double <= 1.7E308', 'q_binary like \'binary%\'  or q_binary = \'0\' ' , 'q_nchar like \'nchar%\' or q_nchar = \'0\' ' ,
        'q_bool = true or  q_bool = false' , 'q_bool in (0 , 1)' , 'q_bool in ( true , false)' , 'q_bool = 0 or q_bool = 1',
        'q_bigint between  -9223372036854775807 and 9223372036854775807',' q_int between -2147483647 and 2147483647','q_smallint between -32767 and 32767',
        'q_bigint not between  9223372036854775807 and -9223372036854775807','q_int not between 2147483647 and -2147483647','q_smallint not between 32767 and -32767',
        'q_tinyint between -127 and 127 ','q_float >= -3.4E38  ','q_float <= 3.4E38  ','q_double >= -1.7E308  ',
        'q_double <= 1.7E308  ','q_float between -3.4E38 and 3.4E38  ','q_double between -1.7E308 and 1.7E308  ' ,'q_float not between 3.4E38 and -3.4E38  ','q_double not between 1.7E308 and -1.7E308  ',
        'q_float is not null  ' ,'q_double is not null  ' ,'q_binary match \'binary\' ','q_binary nmatch \'binarynchar\' ','q_nchar match \'nchar\' ','q_nchar nmatch \'binarynchar\' ',
        'q_binary like \'binary%\' ','(q_binary like \'binary%\'  or q_nchar = \'0\'  or q_binary = \'binary_\' ) ','q_nchar like \'nchar%\' ','(q_nchar like \'nchar%\' or q_binary = \'0\'  or q_nchar = \'nchar_\' ) ',]
        #TD-6201 ,'q_bool between 0 and 1'

        # regular column where for test union,join
        self.q_u_where = ['t1.ts < now +1s' , 't2.ts < now +1s','t1.q_bigint >= -9223372036854775807 and t1.q_bigint <= 9223372036854775807 and t2.q_bigint >= -9223372036854775807 and t2.q_bigint <= 9223372036854775807',
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
        't1.q_double between -1.7E308 and  1.7E308 and t2.q_double between -1.7E308 and  1.7E308',
        't1.q_bigint not between  9223372036854775807 and -9223372036854775807 and t2.q_bigint not between  9223372036854775807 and -9223372036854775807',
        't1.q_int not between 2147483647 and -2147483647 and t2.q_int not between 2147483647 and -2147483647',
        't1.q_smallint not between 32767 and -32767 and t2.q_smallint not between 32767 and -32767',
        't1.q_tinyint not between 127 and -127 and t2.q_tinyint not between 127 and -127 ','t1.q_float not between -1.7E308 and  -1.7E308 and t2.q_float not between 1.7E308 and  -1.7E308',
        't1.q_double not between 1.7E308 and  -1.7E308 and t2.q_double not between 1.7E308 and  -1.7E308']
        #TD-6201 ,'t1.q_bool between 0 and 1 or t2.q_bool between 0 and 1']
        #'t1.q_bool = true and  t1.q_bool = false and t2.q_bool = true and  t2.q_bool = false' , 't1.q_bool = 0 and t1.q_bool = 1 and t2.q_bool = 0 and t2.q_bool = 1' ,

        self.q_u_or_where = ['(t1.q_binary like \'binary%\'  or t1.q_binary = \'0\'  or t2.q_binary like \'binary%\'  or t2.q_binary = \'0\' )' ,
        '(t1.q_nchar like \'nchar%\' or t1.q_nchar = \'0\' or t2.q_nchar like \'nchar%\' or t2.q_nchar = \'0\' )' , '(t1.q_bool = true or  t1.q_bool = false or t2.q_bool = true or  t2.q_bool = false)' ,
        '(t1.q_bool in (0 , 1) or t2.q_bool in (0 , 1))' , '(t1.q_bool in ( true , false) or t2.q_bool in ( true , false))' , '(t1.q_bool = 0 or t1.q_bool = 1 or t2.q_bool = 0 or t2.q_bool = 1)' ,
        '(t1.q_bigint between  -9223372036854775807 and 9223372036854775807 or t2.q_bigint between  -9223372036854775807 and 9223372036854775807)',
        '(t1.q_int between -2147483647 and 2147483647 or t2.q_int between -2147483647 and 2147483647)',
        '(t1.q_smallint between -32767 and 32767 or t2.q_smallint between -32767 and 32767)',
        '(t1.q_tinyint between -127 and 127 or t2.q_tinyint between -127 and 127 )','(t1.q_float between -1.7E308 and 1.7E308 or t2.q_float between -1.7E308 and 1.7E308)',
        '(t1.q_double between -1.7E308 and 1.7E308 or t2.q_double between -1.7E308 and 1.7E308)']

        # tag column where
        self.t_where = ['ts < now +1s','t_bigint >= -9223372036854775807 and t_bigint <= 9223372036854775807','t_int <= 2147483647 and t_int >= -2147483647',
        't_smallint >= -32767 and t_smallint <= 32767','q_tinyint >= -127 and t_tinyint <= 127','t_float >= -1.7E308 and t_float <= 1.7E308',
        't_double >= -1.7E308 and t_double <= 1.7E308', 't_binary like \'binary%\'   or t_binary = \'0\' ' , 't_nchar like \'nchar%\' or t_nchar = \'0\'' ,
        't_bool = true or  t_bool = false' , 't_bool in (0 , 1)' , 't_bool in ( true , false)' , 't_bool = 0 or t_bool = 1',
        't_bigint between  -9223372036854775807 and 9223372036854775807',' t_int between -2147483647 and 2147483647','t_smallint between -32767 and 32767',
        't_tinyint between -127 and 127 ','t_float between -1.7E308 and 1.7E308','t_double between -1.7E308 and 1.7E308',
        't_binary match \'binary\' ','t_binary nmatch \'binarynchar\' ','t_nchar match \'nchar\' ','t_nchar nmatch \'binarynchar\' ',
        't_binary like \'binary%\' ','t_nchar like \'nchar%\' ','(t_binary like \'binary%\'  or t_nchar = \'0\' ) ','(t_nchar like \'nchar%\' or t_binary = \'0\' ) ',]
        #TD-6201,'t_bool between 0 and 1'

        # tag column where for test  union,join | this is not support
        self.t_u_where = ['t1.ts < now +1s' , 't2.ts < now +1s','t1.t_bigint >= -9223372036854775807 and t1.t_bigint <= 9223372036854775807 and t2.t_bigint >= -9223372036854775807 and t2.t_bigint <= 9223372036854775807',
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

        self.t_u_or_where = ['(t1.t_binary like \'binary%\'  or t1.t_binary = \'0\'  or t2.t_binary like \'binary%\'  or t2.t_binary = \'0\' )' ,
        '(t1.t_nchar like \'nchar%\' or t1.t_nchar = \'0\' or t2.t_nchar like \'nchar%\' or t2.t_nchar = \'0\' )' , '(t1.t_bool = true or  t1.t_bool = false or t2.t_bool = true or  t2.t_bool = false)' ,
        '(t1.t_bool in (0 , 1) or t2.t_bool in (0 , 1))' , '(t1.t_bool in ( true , false) or t2.t_bool in ( true , false))' , '(t1.t_bool = 0 or t1.t_bool = 1 or t2.t_bool = 0 or t2.t_bool = 1)',
        '(t1.t_bigint between  -9223372036854775807 and 9223372036854775807 or t2.t_bigint between  -9223372036854775807 and 9223372036854775807)',
        '(t1.t_int between -2147483647 and 2147483647 or t2.t_int between -2147483647 and 2147483647)',
        '(t1.t_smallint between -32767 and 32767 or t2.t_smallint between -32767 and 32767)',
        '(t1.t_tinyint between -127 and 127 or t2.t_tinyint between -127 and 127 )','(t1.t_float between -1.7E308 and 1.7E308 or t2.t_float between -1.7E308 and 1.7E308)',
        '(t1.t_double between -1.7E308 and 1.7E308 or t2.t_double between -1.7E308 and 1.7E308)']

        # self.t_u_where = ['t1.ts < now +1s'] # 超级表tag不支持，暂时注掉
        # self.t_u_or_where = ['(t1.q_bool in (0 , 1))'] #超级表tag不支持，暂时注掉

        # regular and tag column where
        self.qt_where = self.q_where + self.t_where
        #self.qt_where = self.q_where #超级表tag不支持，暂时注掉
        self.qt_u_where = self.q_u_where + self.t_u_where
        # now,qt_u_or_where is not support
        self.qt_u_or_where = self.q_u_or_where + self.t_u_or_where

        # tag column where for test super join | this is  support  , 't1.t_bool = t2.t_bool ' ？？？
        self.t_join_where = ['t1.t_bigint = t2.t_bigint ', 't1.t_int = t2.t_int ', 't1.t_smallint = t2.t_smallint ', 't1.t_tinyint = t2.t_tinyint ',
                    't1.t_float = t2.t_float ', 't1.t_double = t2.t_double ', 't1.t_binary = t2.t_binary ' , 't1.t_nchar = t2.t_nchar  ' ]
        #self.t_join_where = ['t1.ts = t2.ts'] # 超级表tag不支持，暂时注掉

        # session && fill
        self.session_where = ['session(ts,10a)' , 'session(ts,10s)', 'session(ts,10m)' , 'session(ts,10h)','session(ts,10d)' , 'session(ts,10w)']
        self.session_u_where = ['session(t1.ts,10a)' , 'session(t1.ts,10s)', 'session(t1.ts,10m)' , 'session(t1.ts,10h)','session(t1.ts,10d)' , 'session(t1.ts,10w)',
                    'session(t2.ts,10a)' , 'session(t2.ts,10s)', 'session(t2.ts,10m)' , 'session(t2.ts,10h)','session(t2.ts,10d)' , 'session(t2.ts,10w)']

        self.fill_where = ['FILL(NONE)','FILL(PREV)','FILL(NULL)','FILL(LINEAR)','FILL(NEXT)','FILL(VALUE, 1.23)']

        self.state_window = ['STATE_WINDOW(q_tinyint)','STATE_WINDOW(q_bigint)','STATE_WINDOW(q_int)','STATE_WINDOW(q_bool)','STATE_WINDOW(q_smallint)']
        self.state_u_window = ['STATE_WINDOW(t1.q_tinyint)','STATE_WINDOW(t1.q_bigint)','STATE_WINDOW(t1.q_int)','STATE_WINDOW(t1.q_bool)','STATE_WINDOW(t1.q_smallint)',
                    'STATE_WINDOW(t2.q_tinyint)','STATE_WINDOW(t2.q_bigint)','STATE_WINDOW(t2.q_int)','STATE_WINDOW(t2.q_bool)','STATE_WINDOW(t2.q_smallint)']

        # order by where
        self.order_where = ['order by ts' , 'order by ts asc']
        self.order_u_where = ['order by t1.ts' , 'order by t1.ts asc' , 'order by t2.ts' , 'order by t2.ts asc']
        self.order_desc_where = ['order by ts' , 'order by ts asc' , 'order by ts desc' ]
        self.orders_desc_where = ['order by ts' , 'order by ts asc' , 'order by ts desc' , 'order by ts,loc' , 'order by ts,loc asc' , 'order by ts,loc desc']

        self.group_where = ['group by tbname , loc' , 'group by tbname', 'group by tbname, t_bigint', 'group by tbname,t_int', 'group by tbname, t_smallint', 'group by tbname,t_tinyint',
                    'group by tbname,t_float', 'group by tbname,t_double' , 'group by tbname,t_binary', 'group by tbname,t_nchar', 'group by tbname,t_bool' ,'group by tbname ,loc ,t_bigint',
                    'group by tbname,t_binary ,t_nchar ,t_bool' , 'group by tbname,t_int ,t_smallint ,t_tinyint' , 'group by tbname,t_float ,t_double ' ,
                    'PARTITION BY tbname , loc' , 'PARTITION BY tbname', 'PARTITION BY tbname, t_bigint', 'PARTITION BY tbname,t_int', 'PARTITION BY tbname, t_smallint', 'PARTITION BY tbname,t_tinyint',
                    'PARTITION BY tbname,t_float', 'PARTITION BY tbname,t_double' , 'PARTITION BY tbname,t_binary', 'PARTITION BY tbname,t_nchar', 'PARTITION BY tbname,t_bool' ,'PARTITION BY tbname ,loc ,t_bigint',
                    'PARTITION BY tbname,t_binary ,t_nchar ,t_bool' , 'PARTITION BY tbname,t_int ,t_smallint ,t_tinyint' , 'PARTITION BY tbname,t_float ,t_double ']
        self.group_where_j = ['group by  t1.loc' , 'group by t1.t_bigint', 'group by t1.t_int', 'group by t1.t_smallint', 'group by t1.t_tinyint',
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

        self.group_only_where = ['group by tbname , loc' , 'group by tbname', 'group by tbname, t_bigint', 'group by tbname,t_int', 'group by tbname, t_smallint', 'group by tbname,t_tinyint',
                    'group by tbname,t_float', 'group by tbname,t_double' , 'group by tbname,t_binary', 'group by tbname,t_nchar', 'group by tbname,t_bool' ,'group by tbname ,loc ,t_bigint',
                    'group by tbname,t_binary ,t_nchar ,t_bool' , 'group by tbname,t_int ,t_smallint ,t_tinyint' , 'group by tbname,t_float ,t_double ' ]
        self.group_only_where_j = ['group by  t1.loc' , 'group by t1.t_bigint', 'group by t1.t_int', 'group by t1.t_smallint', 'group by t1.t_tinyint',
                    'group by t1.t_float', 'group by t1.t_double' , 'group by t1.t_binary', 'group by t1.t_nchar', 'group by t1.t_bool' ,'group by t1.loc ,t1.t_bigint',
                    'group by t1.t_binary ,t1.t_nchar ,t1.t_bool' , 'group by t1.t_int ,t1.t_smallint ,t1.t_tinyint' , 'group by t1.t_float ,t1.t_double ' ,
                    'group by  t2.loc' , 'group by t2.t_bigint', 'group by t2.t_int', 'group by t2.t_smallint', 'group by t2.t_tinyint',
                    'group by t2.t_float', 'group by t2.t_double' , 'group by t2.t_binary', 'group by t2.t_nchar', 'group by t2.t_bool' ,'group by t2.loc ,t2.t_bigint',
                    'group by t2.t_binary ,t2.t_nchar ,t2.t_bool' , 'group by t2.t_int ,t2.t_smallint ,t2.t_tinyint' , 'group by t2.t_float ,t2.t_double ' ]

        self.partiton_where = ['PARTITION BY tbname , loc' , 'PARTITION BY tbname', 'PARTITION BY tbname, t_bigint', 'PARTITION BY tbname,t_int', 'PARTITION BY tbname, t_smallint', 'PARTITION BY tbname,t_tinyint',
                    'PARTITION BY tbname,t_float', 'PARTITION BY tbname,t_double' , 'PARTITION BY tbname,t_binary', 'PARTITION BY tbname,t_nchar', 'PARTITION BY tbname,t_bool' ,'PARTITION BY tbname ,loc ,t_bigint',
                    'PARTITION BY tbname,t_binary ,t_nchar ,t_bool' , 'PARTITION BY tbname,t_int ,t_smallint ,t_tinyint' , 'PARTITION BY tbname,t_float ,t_double ']
        self.partiton_where_j = ['PARTITION BY t1.loc' , 'PARTITION by t1.t_bigint', 'PARTITION by t1.t_int', 'PARTITION by t1.t_smallint', 'PARTITION by t1.t_tinyint',
                    'PARTITION by t1.t_float', 'PARTITION by t1.t_double' , 'PARTITION by t1.t_binary', 'PARTITION by t1.t_nchar', 'PARTITION by t1.t_bool' ,'PARTITION BY t1.loc ,t1.t_bigint',
                    'PARTITION by t1.t_binary ,t1.t_nchar ,t1.t_bool' , 'PARTITION by t1.t_int ,t1.t_smallint ,t1.t_tinyint' , 'PARTITION by t1.t_float ,t1.t_double ',
                    'PARTITION BY t2.loc' , 'PARTITION by t2.t_bigint', 'PARTITION by t2.t_int', 'PARTITION by t2.t_smallint', 'PARTITION by t2.t_tinyint',
                    'PARTITION by t2.t_float', 'PARTITION by t2.t_double' , 'PARTITION by t2.t_binary', 'PARTITION by t2.t_nchar', 'PARTITION by t2.t_bool' ,'PARTITION BY t2.loc ,t2.t_bigint',
                    'PARTITION by t2.t_binary ,t2.t_nchar ,t2.t_bool' , 'PARTITION by t2.t_int ,t2.t_smallint ,t2.t_tinyint' , 'PARTITION by t2.t_float ,t2.t_double ']


        self.group_where_regular = ['group by tbname ' , 'group by tbname', 'group by tbname, q_bigint', 'group by tbname,q_int', 'group by tbname, q_smallint', 'group by tbname,q_tinyint',
                    'group by tbname,q_float', 'group by tbname,q_double' , 'group by tbname,q_binary', 'group by tbname,q_nchar', 'group by tbname,q_bool' ,'group by tbname ,q_bigint',
                    'group by tbname,q_binary ,q_nchar ,q_bool' , 'group by tbname,q_int ,q_smallint ,q_tinyint' , 'group by tbname,q_float ,q_double ' ,
                    'PARTITION BY tbname ' , 'PARTITION BY tbname', 'PARTITION BY tbname, q_bigint', 'PARTITION BY tbname,q_int', 'PARTITION BY tbname, q_smallint', 'PARTITION BY tbname,q_tinyint',
                    'PARTITION BY tbname,q_float', 'PARTITION BY tbname,q_double' , 'PARTITION BY tbname,q_binary', 'PARTITION BY tbname,q_nchar', 'PARTITION BY tbname,q_bool' ,'PARTITION BY tbname ,q_bigint',
                    'PARTITION BY tbname,q_binary ,q_nchar ,q_bool' , 'PARTITION BY tbname,q_int ,q_smallint ,q_tinyint' , 'PARTITION BY tbname,q_float ,q_double ']
        self.group_where_regular_j = ['group by t1.q_bigint', 'group by t1.q_int', 'group by t1.q_smallint', 'group by t1.q_tinyint',
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

        self.partiton_where_regular = ['PARTITION BY tbname ' , 'PARTITION BY tbname', 'PARTITION BY tbname, q_bigint', 'PARTITION BY tbname,q_int', 'PARTITION BY tbname, q_smallint', 'PARTITION BY tbname,q_tinyint',
                    'PARTITION BY tbname,q_float', 'PARTITION BY tbname,q_double' , 'PARTITION BY tbname,q_binary', 'PARTITION BY tbname,q_nchar', 'PARTITION BY tbname,q_bool' ,'PARTITION BY tbname ,q_bigint',
                    'PARTITION BY tbname,q_binary ,q_nchar ,q_bool' , 'PARTITION BY tbname,q_int ,q_smallint ,q_tinyint' , 'PARTITION BY tbname,q_float ,q_double ']
        self.partiton_where_regular_j = ['PARTITION by t1.q_bigint', 'PARTITION by t1.q_int', 'PARTITION by t1.q_smallint', 'PARTITION by t1.q_tinyint',
                    'PARTITION by t1.q_float', 'PARTITION by t1.q_double' , 'PARTITION by t1.q_binary', 'PARTITION by t1.q_nchar', 'PARTITION by t1.q_bool' ,'PARTITION BY t1.q_bigint',
                    'PARTITION by t1.q_binary ,t1.q_nchar ,t1.q_bool' , 'PARTITION by t1.q_int ,t1.q_smallint ,t1.q_tinyint' , 'PARTITION by t1.q_float ,t1.q_double ',
                    'PARTITION by t2.q_bigint', 'PARTITION by t2.q_int', 'PARTITION by t2.q_smallint', 'PARTITION by t2.q_tinyint',
                    'PARTITION by t2.q_float', 'PARTITION by t2.q_double' , 'PARTITION by t2.q_binary', 'PARTITION by t2.q_nchar', 'PARTITION by t2.q_bool' ,'PARTITION BY t2.q_bigint',
                    'PARTITION by t2.q_binary ,t2.q_nchar ,t2.q_bool' , 'PARTITION by t2.q_int ,t2.q_smallint ,t2.q_tinyint' , 'PARTITION by t2.q_float ,t2.q_double ']

        self.having_support = ['having count(q_int) > 0','having count(q_bigint) > 0','having count(q_smallint) > 0','having count(q_tinyint) > 0','having count(q_float) > 0','having count(q_double) > 0','having count(q_bool) > 0',
                    'having avg(q_int) > 0','having avg(q_bigint) > 0','having avg(q_smallint) > 0','having avg(q_tinyint) > 0','having avg(q_float) > 0','having avg(q_double) > 0',
                    'having sum(q_int) > 0','having sum(q_bigint) > 0','having sum(q_smallint) > 0','having sum(q_tinyint) > 0','having sum(q_float) > 0','having sum(q_double) > 0',
                    'having STDDEV(q_int) > 0','having STDDEV(q_bigint) > 0','having STDDEV(q_smallint) > 0','having STDDEV(q_tinyint) > 0','having STDDEV(q_float) > 0','having STDDEV(q_double) > 0',
                    'having TWA(q_int) > 0','having  TWA(q_bigint) > 0','having  TWA(q_smallint) > 0','having  TWA(q_tinyint) > 0','having  TWA(q_float) > 0','having  TWA(q_double) > 0',
                    'having IRATE(q_int) > 0','having IRATE(q_bigint) > 0','having IRATE(q_smallint) > 0','having IRATE(q_tinyint) > 0','having IRATE(q_float) > 0','having IRATE(q_double) > 0',
                    'having MIN(q_int) > 0','having MIN(q_bigint) > 0','having MIN(q_smallint) > 0','having MIN(q_tinyint) > 0','having MIN(q_float) > 0','having MIN(q_double) > 0',
                    'having MAX(q_int) > 0','having MAX(q_bigint) > 0','having MAX(q_smallint) > 0','having MAX(q_tinyint) > 0','having MAX(q_float) > 0','having MAX(q_double) > 0',
                    'having FIRST(q_int) > 0','having FIRST(q_bigint) > 0','having FIRST(q_smallint) > 0','having FIRST(q_tinyint) > 0','having FIRST(q_float) > 0','having FIRST(q_double) > 0',
                    'having LAST(q_int) > 0','having LAST(q_bigint) > 0','having LAST(q_smallint) > 0','having LAST(q_tinyint) > 0','having LAST(q_float) > 0','having LAST(q_double) > 0',
                    'having APERCENTILE(q_int,10) > 0','having APERCENTILE(q_bigint,10) > 0','having APERCENTILE(q_smallint,10) > 0','having APERCENTILE(q_tinyint,10) > 0','having APERCENTILE(q_float,10) > 0','having APERCENTILE(q_double,10) > 0',
                    'having count(q_int_null) > 0','having count(q_bigint_null) > 0','having count(q_smallint_null) > 0','having count(q_tinyint_null) > 0','having count(q_float_null) > 0','having count(q_double_null) > 0','having count(q_bool_null) > 0',
                    'having avg(q_int_null) > 0','having avg(q_bigint_null) > 0','having avg(q_smallint_null) > 0','having avg(q_tinyint_null) > 0','having avg(q_float_null) > 0','having avg(q_double_null) > 0',
                    'having sum(q_int_null) > 0','having sum(q_bigint_null) > 0','having sum(q_smallint_null) > 0','having sum(q_tinyint_null) > 0','having sum(q_float_null) > 0','having sum(q_double_null) > 0',
                    'having STDDEV(q_int_null) > 0','having STDDEV(q_bigint_null) > 0','having STDDEV(q_smallint_null) > 0','having STDDEV(q_tinyint_null) > 0','having STDDEV(q_float_null) > 0','having STDDEV(q_double_null) > 0',
                    'having TWA(q_int_null) > 0','having  TWA(q_bigint_null) > 0','having  TWA(q_smallint_null) > 0','having  TWA(q_tinyint_null) > 0','having  TWA(q_float_null) > 0','having  TWA(q_double_null) > 0',
                    'having IRATE(q_int_null) > 0','having IRATE(q_bigint_null) > 0','having IRATE(q_smallint_null) > 0','having IRATE(q_tinyint_null) > 0','having IRATE(q_float_null) > 0','having IRATE(q_double_null) > 0',
                    'having MIN(q_int_null) > 0','having MIN(q_bigint_null) > 0','having MIN(q_smallint_null) > 0','having MIN(q_tinyint_null) > 0','having MIN(q_float_null) > 0','having MIN(q_double_null) > 0',
                    'having MAX(q_int_null) > 0','having MAX(q_bigint_null) > 0','having MAX(q_smallint_null) > 0','having MAX(q_tinyint_null) > 0','having MAX(q_float_null) > 0','having MAX(q_double_null) > 0',
                    'having FIRST(q_int_null) > 0','having FIRST(q_bigint_null) > 0','having FIRST(q_smallint_null) > 0','having FIRST(q_tinyint_null) > 0','having FIRST(q_float_null) > 0','having FIRST(q_double_null) > 0',
                    'having LAST(q_int_null) > 0','having LAST(q_bigint_null) > 0','having LAST(q_smallint_null) > 0','having LAST(q_tinyint_null) > 0','having LAST(q_float_null) > 0','having LAST(q_double_null) > 0',
                    'having APERCENTILE(q_int_null,10) > 0','having APERCENTILE(q_bigint_null,10) > 0','having APERCENTILE(q_smallint_null,10) > 0','having APERCENTILE(q_tinyint_null,10) > 0','having APERCENTILE(q_float_null,10) > 0','having APERCENTILE(q_double_null,10) > 0']
        self.having_not_support = ['having TOP(q_int,10) > 0','having TOP(q_bigint,10) > 0','having TOP(q_smallint,10) > 0','having TOP(q_tinyint,10) > 0','having TOP(q_float,10) > 0','having TOP(q_double,10) > 0','having TOP(q_bool,10) > 0',
                    'having BOTTOM(q_int,10) > 0','having BOTTOM(q_bigint,10) > 0','having BOTTOM(q_smallint,10) > 0','having BOTTOM(q_tinyint,10) > 0','having BOTTOM(q_float,10) > 0','having BOTTOM(q_double,10) > 0','having BOTTOM(q_bool,10) > 0',
                    'having LEASTSQUARES(q_int) > 0','having  LEASTSQUARES(q_bigint) > 0','having  LEASTSQUARES(q_smallint) > 0','having  LEASTSQUARES(q_tinyint) > 0','having  LEASTSQUARES(q_float) > 0','having  LEASTSQUARES(q_double) > 0','having  LEASTSQUARES(q_bool) > 0',
                    'having FIRST(q_bool) > 0','having IRATE(q_bool) > 0','having PERCENTILE(q_bool,10) > 0','having avg(q_bool) > 0','having LAST_ROW(q_bool) > 0','having sum(q_bool) > 0','having STDDEV(q_bool) > 0','having APERCENTILE(q_bool,10) > 0','having  TWA(q_bool) > 0','having LAST(q_bool) > 0',
                    'having PERCENTILE(q_int,10) > 0','having PERCENTILE(q_bigint,10) > 0','having PERCENTILE(q_smallint,10) > 0','having PERCENTILE(q_tinyint,10) > 0','having PERCENTILE(q_float,10) > 0','having PERCENTILE(q_double,10) > 0',
                    'having TOP(q_int_null,10) > 0','having TOP(q_bigint_null,10) > 0','having TOP(q_smallint_null,10) > 0','having TOP(q_tinyint_null,10) > 0','having TOP(q_float_null,10) > 0','having TOP(q_double_null,10) > 0','having TOP(q_bool_null,10) > 0',
                    'having BOTTOM(q_int_null,10) > 0','having BOTTOM(q_bigint_null,10) > 0','having BOTTOM(q_smallint_null,10) > 0','having BOTTOM(q_tinyint_null,10) > 0','having BOTTOM(q_float_null,10) > 0','having BOTTOM(q_double_null,10) > 0','having BOTTOM(q_bool_null,10) > 0',
                    'having LEASTSQUARES(q_int_null) > 0','having  LEASTSQUARES(q_bigint_null) > 0','having  LEASTSQUARES(q_smallint_null) > 0','having  LEASTSQUARES(q_tinyint_null) > 0','having  LEASTSQUARES(q_float_null) > 0','having  LEASTSQUARES(q_double_null) > 0','having  LEASTSQUARES(q_bool_null) > 0',
                    'having FIRST(q_bool_null) > 0','having IRATE(q_bool_null) > 0','having PERCENTILE(q_bool_null,10) > 0','having avg(q_bool_null) > 0','having LAST_ROW(q_bool_null) > 0','having sum(q_bool_null) > 0','having STDDEV(q_bool_null) > 0','having APERCENTILE(q_bool_null,10) > 0','having  TWA(q_bool_null) > 0','having LAST(q_bool_null) > 0',
                    'having PERCENTILE(q_int_null,10) > 0','having PERCENTILE(q_bigint_null,10) > 0','having PERCENTILE(q_smallint_null,10) > 0','having PERCENTILE(q_tinyint_null,10) > 0','having PERCENTILE(q_float_null,10) > 0','having PERCENTILE(q_double_null,10) > 0']
        self.having_tagnot_support = ['having LAST_ROW(q_int) > 0','having LAST_ROW(q_bigint) > 0','having LAST_ROW(q_smallint) > 0','having LAST_ROW(q_tinyint) > 0','having LAST_ROW(q_float) > 0','having LAST_ROW(q_double) > 0',
                                      'having LAST_ROW(q_int_null) > 0','having LAST_ROW(q_bigint_null) > 0','having LAST_ROW(q_smallint_null) > 0','having LAST_ROW(q_tinyint_null) > 0','having LAST_ROW(q_float_null) > 0','having LAST_ROW(q_double_null) > 0']

        self.having_support_j = ['having count(t1.q_int) > 0','having count(t1.q_bigint) > 0','having count(t1.q_smallint) > 0','having count(t1.q_tinyint) > 0','having count(t1.q_float) > 0','having count(t1.q_double) > 0','having count(t1.q_bool) > 0',
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
        self.limit_where = ['limit 1 offset 1' , 'limit 1' , 'limit 2 offset 1' , 'limit 2', 'limit 12 offset 1' , 'limit 20', 'limit 20 offset 10' , 'limit 200']
        self.limit1_where = ['limit 1 offset 1' , 'limit 1' ]
        self.limit_u_where = ['limit 100 offset 10' , 'limit 50' , 'limit 100' , 'limit 10' ]

        # slimit soffset where
        self.slimit_where = ['slimit 1 soffset 1' , 'slimit 1' , 'slimit 2 soffset 1' , 'slimit 2']
        self.slimit1_where = ['slimit 2 soffset 1' , 'slimit 1' ]

        # aggregate function include [all:count(*)\avg\sum\stddev ||regualr:twa\irate\leastsquares ||group by tbname:twa\irate\]
        # select /*+ para_tables_sort() */function include [all: min\max\first(*)\last(*)\top\bottom\apercentile\last_row(*)(not with interval)\interp(*)(FILL) ||regualr: percentile]
        # calculation function include [all:spread\+-*/ ||regualr:diff\derivative ||group by tbname:diff\derivative\]
        # **_ns_**  express is not support stable, therefore, separated from regular tables
        # calc_select_all   calc_select_regular  calc_select_in_ts  calc_select_fill  calc_select_not_interval
        # calc_aggregate_all   calc_aggregate_regular   calc_aggregate_groupbytbname
        # calc_calculate_all   calc_calculate_regular   calc_calculate_groupbytbname

        # calc_select_all   calc_select_regular  calc_select_in_ts  calc_select_fill  calc_select_not_interval
        # select /*+ para_tables_sort() */function include [all: min\max\first(*)\last(*)\top\bottom\apercentile\last_row(*)(not with interval)\interp(*)(FILL) ||regualr: percentile]

        self.calc_select_all = ['bottom(q_int,20)' , 'bottom(q_bigint,20)' , 'bottom(q_smallint,20)' , 'bottom(q_tinyint,20)' ,'bottom(q_float,20)' , 'bottom(q_double,20)' ,
                    'top(q_int,20)' , 'top(q_bigint,20)' , 'top(q_smallint,20)' ,'top(q_tinyint,20)' ,'top(q_float,20)' ,'top(q_double,20)' ,
                    'first(q_int)' , 'first(q_bigint)' , 'first(q_smallint)' , 'first(q_tinyint)' , 'first(q_float)' ,'first(q_double)' ,'first(q_binary)' ,'first(q_nchar)' ,'first(q_bool)' ,'first(q_ts)' ,
                    'last(q_int)' ,  'last(q_bigint)' , 'last(q_smallint)'  , 'last(q_tinyint)' , 'last(q_float)'  ,'last(q_double)' , 'last(q_binary)' ,'last(q_nchar)' ,'last(q_bool)' ,'last(q_ts)' ,
                    'min(q_int)' , 'min(q_bigint)' , 'min(q_smallint)' , 'min(q_tinyint)' , 'min(q_float)' ,'min(q_double)' ,
                    'max(q_int)' ,  'max(q_bigint)' , 'max(q_smallint)' , 'max(q_tinyint)' ,'max(q_float)' ,'max(q_double)' ,
                    'apercentile(q_int,20)' ,  'apercentile(q_bigint,20)'  ,'apercentile(q_smallint,20)'  ,'apercentile(q_tinyint,20)' ,'apercentile(q_float,20)'  ,'apercentile(q_double,20)' ,
                    'last_row(q_int)' ,  'last_row(q_bigint)' , 'last_row(q_smallint)' , 'last_row(q_tinyint)' , 'last_row(q_float)' ,
                    'last_row(q_double)' , 'last_row(q_bool)' ,'last_row(q_binary)' ,'last_row(q_nchar)' ,'last_row(q_ts)',
                    'bottom(q_int_null,20)' , 'bottom(q_bigint_null,20)' , 'bottom(q_smallint_null,20)' , 'bottom(q_tinyint_null,20)' ,'bottom(q_float_null,20)' , 'bottom(q_double_null,20)' ,
                    'top(q_int_null,20)' , 'top(q_bigint_null,20)' , 'top(q_smallint_null,20)' ,'top(q_tinyint_null,20)' ,'top(q_float_null,20)' ,'top(q_double_null,20)' ,
                    'first(q_int_null)' , 'first(q_bigint_null)' , 'first(q_smallint_null)' , 'first(q_tinyint_null)' , 'first(q_float_null)' ,'first(q_double_null)' ,'first(q_binary_null)' ,'first(q_nchar_null)' ,'first(q_bool_null)' ,'first(q_ts_null)' ,
                    'last(q_int_null)' ,  'last(q_bigint_null)' , 'last(q_smallint_null)'  , 'last(q_tinyint_null)' , 'last(q_float_null)'  ,'last(q_double_null)' , 'last(q_binary_null)' ,'last(q_nchar_null)' ,'last(q_bool_null)' ,'last(q_ts_null)' ,
                    'min(q_int_null)' , 'min(q_bigint_null)' , 'min(q_smallint_null)' , 'min(q_tinyint_null)' , 'min(q_float_null)' ,'min(q_double_null)' ,
                    'max(q_int_null)' ,  'max(q_bigint_null)' , 'max(q_smallint_null)' , 'max(q_tinyint_null)' ,'max(q_float_null)' ,'max(q_double_null)' ,
                    'last_row(q_int_null)' ,  'last_row(q_bigint_null)' , 'last_row(q_smallint_null)' , 'last_row(q_tinyint_null)' , 'last_row(q_float_null)' ,
                    'last_row(q_double_null)' , 'last_row(q_bool_null)' ,'last_row(q_binary_null)' ,'last_row(q_nchar_null)' ,'last_row(q_ts_null)',
                    'apercentile(q_int_null,20)' ,  'apercentile(q_bigint_null,20)'  ,'apercentile(q_smallint_null,20)'  ,'apercentile(q_tinyint_null,20)' ,'apercentile(q_float_null,20)'  ,'apercentile(q_double_null,20)' ,]

        self.calc_select_in_ts = ['bottom(q_int,20)' , 'bottom(q_bigint,20)' , 'bottom(q_smallint,20)' , 'bottom(q_tinyint,20)' ,'bottom(q_float,20)' , 'bottom(q_double,20)' ,
                    'top(q_int,20)' , 'top(q_bigint,20)' , 'top(q_smallint,20)' ,'top(q_tinyint,20)' ,'top(q_float,20)' ,'top(q_double,20)' ,
                    'bottom(q_int_null,20)' , 'bottom(q_bigint_null,20)' , 'bottom(q_smallint_null,20)' , 'bottom(q_tinyint_null,20)' ,'bottom(q_float_null,20)' , 'bottom(q_double_null,20)' ,
                    'top(q_int_null,20)' , 'top(q_bigint_null,20)' , 'top(q_smallint_null,20)' ,'top(q_tinyint_null,20)' ,'top(q_float_null,20)' ,'top(q_double_null,20)' ,
                    'first(q_int)' , 'first(q_bigint)' , 'first(q_smallint)' , 'first(q_tinyint)' , 'first(q_float)' ,'first(q_double)' ,'first(q_binary)' ,'first(q_nchar)' ,'first(q_bool)' ,'first(q_ts)' ,
                    'last(q_int)' ,  'last(q_bigint)' , 'last(q_smallint)'  , 'last(q_tinyint)' , 'last(q_float)'  ,'last(q_double)' , 'last(q_binary)' ,'last(q_nchar)' ,'last(q_bool)' ,'last(q_ts)' ,
                    'first(q_int_null)' , 'first(q_bigint_null)' , 'first(q_smallint_null)' , 'first(q_tinyint_null)' , 'first(q_float_null)' ,'first(q_double_null)' ,'first(q_binary_null)' ,'first(q_nchar_null)' ,'first(q_bool_null)' ,'first(q_ts_null)' ,
                    'last(q_int_null)' ,  'last(q_bigint_null)' , 'last(q_smallint_null)'  , 'last(q_tinyint_null)' , 'last(q_float_null)'  ,'last(q_double_null)' , 'last(q_binary_null)' ,'last(q_nchar_null)' ,'last(q_bool_null)' ,'last(q_ts_null)' ]

        self.calc_select_in = ['min(q_int)' , 'min(q_bigint)' , 'min(q_smallint)' , 'min(q_tinyint)' , 'min(q_float)' ,'min(q_double)' ,
                    'max(q_int)' ,  'max(q_bigint)' , 'max(q_smallint)' , 'max(q_tinyint)' ,'max(q_float)' ,'max(q_double)' ,
                    'apercentile(q_int,20)' ,  'apercentile(q_bigint,20)'  ,'apercentile(q_smallint,20)'  ,'apercentile(q_tinyint,20)' ,'apercentile(q_float,20)'  ,'apercentile(q_double,20)' ,
                    'last_row(q_int)' ,  'last_row(q_bigint)' , 'last_row(q_smallint)' , 'last_row(q_tinyint)' , 'last_row(q_float)' ,
                    'last_row(q_double)' , 'last_row(q_bool)' ,'last_row(q_binary)' ,'last_row(q_nchar)' ,'last_row(q_ts)',
                    'min(q_int_null)' , 'min(q_bigint_null)' , 'min(q_smallint_null)' , 'min(q_tinyint_null)' , 'min(q_float_null)' ,'min(q_double_null)' ,
                    'max(q_int_null)' ,  'max(q_bigint_null)' , 'max(q_smallint_null)' , 'max(q_tinyint_null)' ,'max(q_float_null)' ,'max(q_double_null)' ,
                    'apercentile(q_int_null,20)' ,  'apercentile(q_bigint_null,20)'  ,'apercentile(q_smallint_null,20)'  ,'apercentile(q_tinyint_null,20)' ,'apercentile(q_float_null,20)'  ,'apercentile(q_double_null,20)' ,
                    'last_row(q_int_null)' ,  'last_row(q_bigint_null)' , 'last_row(q_smallint_null)' , 'last_row(q_tinyint_null)' , 'last_row(q_float_null)' ,
                    'last_row(q_double_null)' , 'last_row(q_bool_null)' ,'last_row(q_binary_null)' ,'last_row(q_nchar_null)' ,'last_row(q_ts_null)']

        self.calc_select_not_support_ts = ['first(q_int)' , 'first(q_bigint)' , 'first(q_smallint)' , 'first(q_tinyint)' , 'first(q_float)' ,'first(q_double)' ,'first(q_binary)' ,'first(q_nchar)' ,'first(q_bool)' ,'first(q_ts)' ,
                    'last(q_int)' ,  'last(q_bigint)' , 'last(q_smallint)'  , 'last(q_tinyint)' , 'last(q_float)'  ,'last(q_double)' , 'last(q_binary)' ,'last(q_nchar)' ,'last(q_bool)' ,'last(q_ts)' ,
                    'last_row(q_int)' ,  'last_row(q_bigint)' , 'last_row(q_smallint)' , 'last_row(q_tinyint)' , 'last_row(q_float)' ,
                    'last_row(q_double)' , 'last_row(q_bool)' ,'last_row(q_binary)' ,'last_row(q_nchar)' ,'last_row(q_ts)',
                    'apercentile(q_int,20)' ,  'apercentile(q_bigint,20)'  ,'apercentile(q_smallint,20)'  ,'apercentile(q_tinyint,20)' ,'apercentile(q_float,20)'  ,'apercentile(q_double,20)',
                    'first(q_int_null)' , 'first(q_bigint_null)' , 'first(q_smallint_null)' , 'first(q_tinyint_null)' , 'first(q_float_null)' ,'first(q_double_null)' ,'first(q_binary_null)' ,'first(q_nchar_null)' ,'first(q_bool_null)' ,'first(q_ts_null)' ,
                    'last(q_int_null)' ,  'last(q_bigint_null)' , 'last(q_smallint_null)'  , 'last(q_tinyint_null)' , 'last(q_float_null)'  ,'last(q_double_null)' , 'last(q_binary_null)' ,'last(q_nchar_null)' ,'last(q_bool_null)' ,'last(q_ts_null)' ,
                    'last_row(q_int_null)' ,  'last_row(q_bigint_null)' , 'last_row(q_smallint_null)' , 'last_row(q_tinyint_null)' , 'last_row(q_float_null)' ,
                    'last_row(q_double_null)' , 'last_row(q_bool_null)' ,'last_row(q_binary_null)' ,'last_row(q_nchar_null)' ,'last_row(q_ts_null)',
                    'apercentile(q_int_null,20)' ,  'apercentile(q_bigint_null,20)'  ,'apercentile(q_smallint_null,20)'  ,'apercentile(q_tinyint_null,20)' ,'apercentile(q_float_null,20)'  ,'apercentile(q_double_null,20)']

        self.calc_select_support_ts = ['bottom(q_int,20)' , 'bottom(q_bigint,20)' , 'bottom(q_smallint,20)' , 'bottom(q_tinyint,20)' ,'bottom(q_float,20)' , 'bottom(q_double,20)' ,
                    'top(q_int,20)' , 'top(q_bigint,20)' , 'top(q_smallint,20)' ,'top(q_tinyint,20)' ,'top(q_float,20)' ,'top(q_double,20)' ,
                    'bottom(q_int_null,20)' , 'bottom(q_bigint_null,20)' , 'bottom(q_smallint_null,20)' , 'bottom(q_tinyint_null,20)' ,'bottom(q_float_null,20)' , 'bottom(q_double_null,20)' ,
                    'top(q_int_null,20)' , 'top(q_bigint_null,20)' , 'top(q_smallint_null,20)' ,'top(q_tinyint_null,20)' ,'top(q_float_null,20)' ,'top(q_double_null,20)' ,
                    'min(q_int)' , 'min(q_bigint)' , 'min(q_smallint)' , 'min(q_tinyint)' , 'min(q_float)' ,'min(q_double)' ,
                    'max(q_int)' ,  'max(q_bigint)' , 'max(q_smallint)' , 'max(q_tinyint)' ,'max(q_float)' ,'max(q_double)'  ,
                    'min(q_int_null)' , 'min(q_bigint_null)' , 'min(q_smallint_null)' , 'min(q_tinyint_null)' , 'min(q_float_null)' ,'min(q_double_null)' ,
                    'max(q_int_null)' ,  'max(q_bigint_null)' , 'max(q_smallint_null)' , 'max(q_tinyint_null)' ,'max(q_float_null)' ,'max(q_double_null)']

        self.calc_select_regular = [ 'PERCENTILE(q_int,10)' ,'PERCENTILE(q_bigint,20)' , 'PERCENTILE(q_smallint,30)' ,'PERCENTILE(q_tinyint,40)' ,'PERCENTILE(q_float,50)' ,'PERCENTILE(q_double,60)',
                                    'PERCENTILE(q_int_null,10)' ,'PERCENTILE(q_bigint_null,20)' , 'PERCENTILE(q_smallint_null,30)' ,'PERCENTILE(q_tinyint_null,40)' ,'PERCENTILE(q_float_null,50)' ,'PERCENTILE(q_double_null,60)']


        self.calc_select_fill = ['INTERP(q_int)' ,'INTERP(q_bigint)' ,'INTERP(q_smallint)' ,'INTERP(q_tinyint)', 'INTERP(q_float)' ,'INTERP(q_double)']
        self.interp_where = ['ts = now' , 'ts = \'2020-09-13 20:26:40.000\'' , 'ts = \'2020-09-13 20:26:40.009\'' ,'tbname in (\'table_1\') and ts = now' ,'tbname in (\'table_0\' ,\'table_1\',\'table_2\',\'table_3\',\'table_4\',\'table_5\') and ts =  \'2020-09-13 20:26:40.000\'','tbname like \'table%\'  and ts =  \'2020-09-13 20:26:40.002\'']

        #two table join
        self.calc_select_in_ts_j = ['bottom(t1.q_int,20)' , 'bottom(t1.q_bigint,20)' , 'bottom(t1.q_smallint,20)' , 'bottom(t1.q_tinyint,20)' ,'bottom(t1.q_float,20)' , 'bottom(t1.q_double,20)' ,
                    'top(t1.q_int,20)' , 'top(t1.q_bigint,20)' , 'top(t1.q_smallint,20)' ,'top(t1.q_tinyint,20)' ,'top(t1.q_float,20)' ,'top(t1.q_double,20)' ,
                    'first(t1.q_int)' , 'first(t1.q_bigint)' , 'first(t1.q_smallint)' , 'first(t1.q_tinyint)' , 'first(t1.q_float)' ,'first(t1.q_double)' ,'first(t1.q_binary)' ,'first(t1.q_nchar)' ,'first(t1.q_bool)' ,'first(t1.q_ts)' ,
                    'last(t1.q_int)' ,  'last(t1.q_bigint)' , 'last(t1.q_smallint)'  , 'last(t1.q_tinyint)' , 'last(t1.q_float)'  ,'last(t1.q_double)' , 'last(t1.q_binary)' ,'last(t1.q_nchar)' ,'last(t1.q_bool)' ,'last(t1.q_ts)' ,
                    'bottom(t2.q_int,20)' , 'bottom(t2.q_bigint,20)' , 'bottom(t2.q_smallint,20)' , 'bottom(t2.q_tinyint,20)' ,'bottom(t2.q_float,20)' , 'bottom(t2.q_double,20)' ,
                    'top(t2.q_int,20)' , 'top(t2.q_bigint,20)' , 'top(t2.q_smallint,20)' ,'top(t2.q_tinyint,20)' ,'top(t2.q_float,20)' ,'top(t2.q_double,20)' ,
                    'first(t2.q_int)' , 'first(t2.q_bigint)' , 'first(t2.q_smallint)' , 'first(t2.q_tinyint)' , 'first(t2.q_float)' ,'first(t2.q_double)' ,'first(t2.q_binary)' ,'first(t2.q_nchar)' ,'first(t2.q_bool)' ,'first(t2.q_ts)' ,
                    'last(t2.q_int)' ,  'last(t2.q_bigint)' , 'last(t2.q_smallint)'  , 'last(t2.q_tinyint)' , 'last(t2.q_float)'  ,'last(t2.q_double)' , 'last(t2.q_binary)' ,'last(t2.q_nchar)' ,'last(t2.q_bool)' ,'last(t2.q_ts)',
                    'bottom(t1.q_int_null,20)' , 'bottom(t1.q_bigint_null,20)' , 'bottom(t1.q_smallint_null,20)' , 'bottom(t1.q_tinyint_null,20)' ,'bottom(t1.q_float_null,20)' , 'bottom(t1.q_double_null,20)' ,
                    'top(t1.q_int_null,20)' , 'top(t1.q_bigint_null,20)' , 'top(t1.q_smallint_null,20)' ,'top(t1.q_tinyint_null,20)' ,'top(t1.q_float_null,20)' ,'top(t1.q_double_null,20)' ,
                    'first(t1.q_int_null)' , 'first(t1.q_bigint_null)' , 'first(t1.q_smallint_null)' , 'first(t1.q_tinyint_null)' , 'first(t1.q_float_null)' ,'first(t1.q_double_null)' ,'first(t1.q_binary_null)' ,'first(t1.q_nchar_null))' ,'first(t1.q_bool_null)' ,'first(t1.q_ts_null)' ,
                    'last(t1.q_int_null)' ,  'last(t1.q_bigint_null)' , 'last(t1.q_smallint_null)'  , 'last(t1.q_tinyint_null)' , 'last(t1.q_float_null)'  ,'last(t1.q_double_null)' , 'last(t1.q_binary_null)' ,'last(t1.q_nchar_null))' ,'last(t1.q_bool_null)' ,'last(t1.q_ts_null)' ,
                    'bottom(t2.q_int_null,20)' , 'bottom(t2.q_bigint_null,20)' , 'bottom(t2.q_smallint_null,20)' , 'bottom(t2.q_tinyint_null,20)' ,'bottom(t2.q_float_null,20)' , 'bottom(t2.q_double_null,20)' ,
                    'top(t2.q_int_null,20)' , 'top(t2.q_bigint_null,20)' , 'top(t2.q_smallint_null,20)' ,'top(t2.q_tinyint_null,20)' ,'top(t2.q_float_null,20)' ,'top(t2.q_double_null,20)' ,
                    'first(t2.q_int_null)' , 'first(t2.q_bigint_null)' , 'first(t2.q_smallint_null)' , 'first(t2.q_tinyint_null)' , 'first(t2.q_float_null)' ,'first(t2.q_double_null)' ,'first(t2.q_binary_null)' ,'first(t2.q_nchar_null))' ,'first(t2.q_bool_null)' ,'first(t2.q_ts_null)' ,
                    'last(t2.q_int_null)' ,  'last(t2.q_bigint_null)' , 'last(t2.q_smallint_null)'  , 'last(t2.q_tinyint_null)' , 'last(t2.q_float_null)'  ,'last(t2.q_double_null)' , 'last(t2.q_binary_null)' ,'last(t2.q_nchar_null))' ,'last(t2.q_bool_null)' ,'last(t2.q_ts_null)']

        self.calc_select_in_support_ts_j = ['bottom(t1.q_int,20)' , 'bottom(t1.q_bigint,20)' , 'bottom(t1.q_smallint,20)' , 'bottom(t1.q_tinyint,20)' ,'bottom(t1.q_float,20)' , 'bottom(t1.q_double,20)' ,
                    'top(t1.q_int,20)' , 'top(t1.q_bigint,20)' , 'top(t1.q_smallint,20)' ,'top(t1.q_tinyint,20)' ,'top(t1.q_float,20)' ,'top(t1.q_double,20)' ,
                    'min(t1.q_int)' , 'min(t1.q_bigint)' , 'min(t1.q_smallint)' , 'min(t1.q_tinyint)' , 'min(t1.q_float)' ,'min(t1.q_double)' ,
                    'max(t1.q_int)' ,  'max(t1.q_bigint)' , 'max(t1.q_smallint)' , 'max(t1.q_tinyint)' ,'max(t1.q_float)' ,'max(t1.q_double)' ,
                    'bottom(t2.q_int,20)' , 'bottom(t2.q_bigint,20)' , 'bottom(t2.q_smallint,20)' , 'bottom(t2.q_tinyint,20)' ,'bottom(t2.q_float,20)' , 'bottom(t2.q_double,20)' ,
                    'top(t2.q_int,20)' , 'top(t2.q_bigint,20)' , 'top(t2.q_smallint,20)' ,'top(t2.q_tinyint,20)' ,'top(t2.q_float,20)' ,'top(t2.q_double,20)' ,
                    'min(t2.q_int)' , 'min(t2.q_bigint)' , 'min(t2.q_smallint)' , 'min(t2.q_tinyint)' , 'min(t2.q_float)' ,'min(t2.q_double)' ,
                    'max(t2.q_int)' ,  'max(t2.q_bigint)' , 'max(t2.q_smallint)' , 'max(t2.q_tinyint)' ,'max(t2.q_float)' ,'max(t2.q_double)' ,
                    'bottom(t1.q_int_null,20)' , 'bottom(t1.q_bigint_null,20)' , 'bottom(t1.q_smallint_null,20)' , 'bottom(t1.q_tinyint_null,20)' ,'bottom(t1.q_float_null,20)' , 'bottom(t1.q_double_null,20)' ,
                    'top(t1.q_int_null,20)' , 'top(t1.q_bigint_null,20)' , 'top(t1.q_smallint_null,20)' ,'top(t1.q_tinyint_null,20)' ,'top(t1.q_float_null,20)' ,'top(t1.q_double_null,20)' ,
                    'bottom(t2.q_int_null,20)' , 'bottom(t2.q_bigint_null,20)' , 'bottom(t2.q_smallint_null,20)' , 'bottom(t2.q_tinyint_null,20)' ,'bottom(t2.q_float_null,20)' , 'bottom(t2.q_double_null,20)' ,
                    'top(t2.q_int_null,20)' , 'top(t2.q_bigint_null,20)' , 'top(t2.q_smallint_null,20)' ,'top(t2.q_tinyint_null,20)' ,'top(t2.q_float_null,20)' ,'top(t2.q_double_null,20)' ,
                    'min(t1.q_int_null)' , 'min(t1.q_bigint_null)' , 'min(t1.q_smallint_null)' , 'min(t1.q_tinyint_null)' , 'min(t1.q_float_null)' ,'min(t1.q_double_null)' ,
                    'max(t1.q_int_null)' ,  'max(t1.q_bigint_null)' , 'max(t1.q_smallint_null)' , 'max(t1.q_tinyint_null)' ,'max(t1.q_float_null)' ,'max(t1.q_double_null)' ,
                    'min(t2.q_int_null)' , 'min(t2.q_bigint_null)' , 'min(t2.q_smallint_null)' , 'min(t2.q_tinyint_null)' , 'min(t2.q_float_null)' ,'min(t2.q_double_null)' ,
                    'max(t2.q_int_null)' ,  'max(t2.q_bigint_null)' , 'max(t2.q_smallint_null)' , 'max(t2.q_tinyint_null)' ,'max(t2.q_float_null)' ,'max(t2.q_double_null)' ]

        self.calc_select_in_not_support_ts_j = ['apercentile(t1.q_int,20)' ,  'apercentile(t1.q_bigint,20)'  ,'apercentile(t1.q_smallint,20)'  ,'apercentile(t1.q_tinyint,20)' ,'apercentile(t1.q_float,20)'  ,'apercentile(t1.q_double,20)' ,
                    'apercentile(t1.q_int_null,20)' ,  'apercentile(t1.q_bigint_null,20)'  ,'apercentile(t1.q_smallint_null,20)'  ,'apercentile(t1.q_tinyint_null,20)' ,'apercentile(t1.q_float_null,20)'  ,'apercentile(t1.q_double_null,20)' ,
                    'last_row(t1.q_int)' ,  'last_row(t1.q_bigint)' , 'last_row(t1.q_smallint)' , 'last_row(t1.q_tinyint)' , 'last_row(t1.q_float)' ,
                    'last_row(t1.q_double)' , 'last_row(t1.q_bool)' ,'last_row(t1.q_binary)' ,'last_row(t1.q_nchar)' ,'last_row(t1.q_ts)' ,
                    'last_row(t1.q_int_null)' ,  'last_row(t1.q_bigint_null)' , 'last_row(t1.q_smallint_null)' , 'last_row(t1.q_tinyint_null)' , 'last_row(t1.q_float_null)' ,
                    'last_row(t1.q_double_null)' , 'last_row(t1.q_bool_null)' ,'last_row(t1.q_binary_null)' ,'last_row(t1.q_nchar_null)' ,'last_row(t1.q_ts_null)' ,
                    'apercentile(t2.q_int,20)' ,  'apercentile(t2.q_bigint,20)'  ,'apercentile(t2.q_smallint,20)'  ,'apercentile(t2.q_tinyint,20)' ,'apercentile(t2.q_float,20)'  ,'apercentile(t2.q_double,20)' ,
                    'apercentile(t2.q_int_null,20)' ,  'apercentile(t2.q_bigint_null,20)'  ,'apercentile(t2.q_smallint_null,20)'  ,'apercentile(t2.q_tinyint_null,20)' ,'apercentile(t2.q_float_null,20)'  ,'apercentile(t2.q_double_null,20)' ,
                    'last_row(t2.q_int)' ,  'last_row(t2.q_bigint)' , 'last_row(t2.q_smallint)' , 'last_row(t2.q_tinyint)' , 'last_row(t2.q_float)' ,
                    'last_row(t2.q_double)' , 'last_row(t2.q_bool)' ,'last_row(t2.q_binary)' ,'last_row(t2.q_nchar)' ,'last_row(t2.q_ts)',
                    'last_row(t2.q_int_null)' ,  'last_row(t2.q_bigint_null)' , 'last_row(t2.q_smallint_null)' , 'last_row(t2.q_tinyint_null)' , 'last_row(t2.q_float_null)' ,
                    'last_row(t2.q_double_null)' , 'last_row(t2.q_bool_null)' ,'last_row(t2.q_binary_null)' ,'last_row(t2.q_nchar_null)' ,'last_row(t2.q_ts_null)']

        self.calc_select_in_j = ['min(t1.q_int)' , 'min(t1.q_bigint)' , 'min(t1.q_smallint)' , 'min(t1.q_tinyint)' , 'min(t1.q_float)' ,'min(t1.q_double)' ,
                    'max(t1.q_int)' ,  'max(t1.q_bigint)' , 'max(t1.q_smallint)' , 'max(t1.q_tinyint)' ,'max(t1.q_float)' ,'max(t1.q_double)' ,
                    'apercentile(t1.q_int,20)' ,  'apercentile(t1.q_bigint,20)'  ,'apercentile(t1.q_smallint,20)'  ,'apercentile(t1.q_tinyint,20)' ,'apercentile(t1.q_float,20)'  ,'apercentile(t1.q_double,20)' ,
                    'min(t1.q_int_null)' , 'min(t1.q_bigint_null)' , 'min(t1.q_smallint_null)' , 'min(t1.q_tinyint_null)' , 'min(t1.q_float_null)' ,'min(t1.q_double_null)' ,
                    'max(t1.q_int_null)' ,  'max(t1.q_bigint_null)' , 'max(t1.q_smallint_null)' , 'max(t1.q_tinyint_null)' ,'max(t1.q_float_null)' ,'max(t1.q_double_null)' ,
                    'apercentile(t1.q_int_null,20)' ,  'apercentile(t1.q_bigint_null,20)'  ,'apercentile(t1.q_smallint_null,20)'  ,'apercentile(t1.q_tinyint_null,20)' ,'apercentile(t1.q_float_null,20)'  ,'apercentile(t1.q_double_null,20)' ,
                    'last_row(t1.q_int)' ,  'last_row(t1.q_bigint)' , 'last_row(t1.q_smallint)' , 'last_row(t1.q_tinyint)' , 'last_row(t1.q_float)' ,
                    'last_row(t1.q_double)' , 'last_row(t1.q_bool)' ,'last_row(t1.q_binary)' ,'last_row(t1.q_nchar)' ,'last_row(t1.q_ts)' ,
                    'min(t2.q_int)' , 'min(t2.q_bigint)' , 'min(t2.q_smallint)' , 'min(t2.q_tinyint)' , 'min(t2.q_float)' ,'min(t2.q_double)' ,
                    'max(t2.q_int)' ,  'max(t2.q_bigint)' , 'max(t2.q_smallint)' , 'max(t2.q_tinyint)' ,'max(t2.q_float)' ,'max(t2.q_double)' ,
                    'last_row(t1.q_int_null)' ,  'last_row(t1.q_bigint_null)' , 'last_row(t1.q_smallint_null)' , 'last_row(t1.q_tinyint_null)' , 'last_row(t1.q_float_null)' ,
                    'last_row(t1.q_double_null)' , 'last_row(t1.q_bool_null)' ,'last_row(t1.q_binary_null)' ,'last_row(t1.q_nchar_null)' ,'last_row(t1.q_ts_null)' ,
                    'min(t2.q_int_null)' , 'min(t2.q_bigint_null)' , 'min(t2.q_smallint_null)' , 'min(t2.q_tinyint_null)' , 'min(t2.q_float_null)' ,'min(t2.q_double_null)' ,
                    'max(t2.q_int_null)' ,  'max(t2.q_bigint_null)' , 'max(t2.q_smallint_null)' , 'max(t2.q_tinyint_null)' ,'max(t2.q_float_null)' ,'max(t2.q_double_null)' ,
                    'apercentile(t2.q_int,20)' ,  'apercentile(t2.q_bigint,20)'  ,'apercentile(t2.q_smallint,20)'  ,'apercentile(t2.q_tinyint,20)' ,'apercentile(t2.q_float,20)'  ,'apercentile(t2.q_double,20)' ,
                    'apercentile(t2.q_int_null,20)' ,  'apercentile(t2.q_bigint_null,20)'  ,'apercentile(t2.q_smallint_null,20)'  ,'apercentile(t2.q_tinyint_null,20)' ,'apercentile(t2.q_float_null,20)'  ,'apercentile(t2.q_double_null,20)' ,
                    'last_row(t2.q_int)' ,  'last_row(t2.q_bigint)' , 'last_row(t2.q_smallint)' , 'last_row(t2.q_tinyint)' , 'last_row(t2.q_float)' ,
                    'last_row(t2.q_double)' , 'last_row(t2.q_bool)' ,'last_row(t2.q_binary)' ,'last_row(t2.q_nchar)' ,'last_row(t2.q_ts)',
                    'last_row(t2.q_int_null)' ,  'last_row(t2.q_bigint_null)' , 'last_row(t2.q_smallint_null)' , 'last_row(t2.q_tinyint_null)' , 'last_row(t2.q_float_null)' ,
                    'last_row(t2.q_double_null)' , 'last_row(t2.q_bool_null)' ,'last_row(t2.q_binary_null)' ,'last_row(t2.q_nchar_null)' ,'last_row(t2.q_ts_null)']
        self.calc_select_all_j = self.calc_select_in_ts_j + self.calc_select_in_j

        self.calc_select_regular_j = [ 'PERCENTILE(t1.q_int,10)' ,'PERCENTILE(t1.q_bigint,20)' , 'PERCENTILE(t1.q_smallint,30)' ,'PERCENTILE(t1.q_tinyint,40)' ,'PERCENTILE(t1.q_float,50)' ,'PERCENTILE(t1.q_double,60)' ,
                    'PERCENTILE(t2.q_int,10)' ,'PERCENTILE(t2.q_bigint,20)' , 'PERCENTILE(t2.q_smallint,30)' ,'PERCENTILE(t2.q_tinyint,40)' ,'PERCENTILE(t2.q_float,50)' ,'PERCENTILE(t2.q_double,60)',
                    'PERCENTILE(t1.q_int_null,10)' ,'PERCENTILE(t1.q_bigint_null,20)' , 'PERCENTILE(t1.q_smallint_null,30)' ,'PERCENTILE(t1.q_tinyint_null,40)' ,'PERCENTILE(t1.q_float_null,50)' ,'PERCENTILE(t1.q_double_null,60)' ,
                    'PERCENTILE(t2.q_int_null,10)' ,'PERCENTILE(t2.q_bigint_null,20)' , 'PERCENTILE(t2.q_smallint_null,30)' ,'PERCENTILE(t2.q_tinyint_null,40)' ,'PERCENTILE(t2.q_float_null,50)' ,'PERCENTILE(t2.q_double_null,60)']


        self.calc_select_fill_j = ['INTERP(t1.q_int)' ,'INTERP(t1.q_bigint)' ,'INTERP(t1.q_smallint)' ,'INTERP(t1.q_tinyint)', 'INTERP(t1.q_float)' ,'INTERP(t1.q_double)' ,
                    'INTERP(t2.q_int)' ,'INTERP(t2.q_bigint)' ,'INTERP(t2.q_smallint)' ,'INTERP(t2.q_tinyint)', 'INTERP(t2.q_float)' ,'INTERP(t2.q_double)']
        self.interp_where_j = ['t1.ts = now' , 't1.ts = \'2020-09-13 20:26:40.000\'' , 't1.ts = \'2020-09-13 20:26:40.009\'' ,'t2.ts = now' , 't2.ts = \'2020-09-13 20:26:40.000\'' , 't2.ts = \'2020-09-13 20:26:40.009\'' ,
                    't1.tbname in (\'table_1\') and t1.ts = now' ,'t1.tbname in (\'table_0\' ,\'table_1\',\'table_2\',\'table_3\',\'table_4\',\'table_5\') and t1.ts =  \'2020-09-13 20:26:40.000\'','t1.tbname like \'table%\'  and t1.ts =  \'2020-09-13 20:26:40.002\'',
                    't2.tbname in (\'table_1\') and t2.ts = now' ,'t2.tbname in (\'table_0\' ,\'table_1\',\'table_2\',\'table_3\',\'table_4\',\'table_5\') and t2.ts =  \'2020-09-13 20:26:40.000\'','t2.tbname like \'table%\'  and t2.ts =  \'2020-09-13 20:26:40.002\'']

        # calc_aggregate_all   calc_aggregate_regular   calc_aggregate_groupbytbname  APERCENTILE\PERCENTILE
        # aggregate function include [all:count(*)\avg\sum\stddev ||regualr:twa\irate\leastsquares ||group by tbname:twa\irate\]
        self.calc_aggregate_all = ['count(*)' , 'count(q_int)' ,'count(q_bigint)' , 'count(q_smallint)' ,'count(q_tinyint)' ,'count(q_float)' ,
                    'count(q_double)' ,'count(q_binary)' ,'count(q_nchar)' ,'count(q_bool)' ,'count(q_ts)' ,
                    'avg(q_int)' ,'avg(q_bigint)' , 'avg(q_smallint)' ,'avg(q_tinyint)' ,'avg(q_float)' ,'avg(q_double)' ,
                    'sum(q_int)' ,'sum(q_bigint)' , 'sum(q_smallint)' ,'sum(q_tinyint)' ,'sum(q_float)' ,'sum(q_double)' ,
                    'STDDEV(q_int)' ,'STDDEV(q_bigint)' , 'STDDEV(q_smallint)' ,'STDDEV(q_tinyint)' ,'STDDEV(q_float)' ,'STDDEV(q_double)',
                    'APERCENTILE(q_int,10)' ,'APERCENTILE(q_bigint,20)' , 'APERCENTILE(q_smallint,30)' ,'APERCENTILE(q_tinyint,40)' ,'APERCENTILE(q_float,50)' ,'APERCENTILE(q_double,60)',
                    'count(q_int_null)' ,'count(q_bigint_null)' , 'count(q_smallint_null)' ,'count(q_tinyint_null)' ,'count(q_float_null)' ,
                    'count(q_double_null)' ,'count(q_binary_null)' ,'count(q_nchar_null)' ,'count(q_bool_null)' ,'count(q_ts_null)' ,
                    'avg(q_int_null)' ,'avg(q_bigint_null)' , 'avg(q_smallint_null)' ,'avg(q_tinyint_null)' ,'avg(q_float_null)' ,'avg(q_double_null)' ,
                    'sum(q_int_null)' ,'sum(q_bigint_null)' , 'sum(q_smallint_null)' ,'sum(q_tinyint_null)' ,'sum(q_float_null)' ,'sum(q_double_null)' ,
                    'STDDEV(q_int_null)' ,'STDDEV(q_bigint_null)' , 'STDDEV(q_smallint_null)' ,'STDDEV(q_tinyint_null)' ,'STDDEV(q_float_null)' ,'STDDEV(q_double_null)',
                    'APERCENTILE(q_int_null,10)' ,'APERCENTILE(q_bigint_null,20)' , 'APERCENTILE(q_smallint_null,30)' ,'APERCENTILE(q_tinyint_null,40)' ,'APERCENTILE(q_float_null,50)' ,'APERCENTILE(q_double_null,60)']

        self.calc_aggregate_regular = ['twa(q_int)' ,'twa(q_bigint)' , 'twa(q_smallint)' ,'twa(q_tinyint)' ,'twa (q_float)' ,'twa(q_double)' ,
                    'IRATE(q_int)' ,'IRATE(q_bigint)' , 'IRATE(q_smallint)' ,'IRATE(q_tinyint)' ,'IRATE (q_float)' ,'IRATE(q_double)' ,
                    'twa(q_int_null)' ,'twa(q_bigint_null)' , 'twa(q_smallint_null)' ,'twa(q_tinyint_null)' ,'twa (q_float_null)' ,'twa(q_double_null)' ,
                    'IRATE(q_int_null)' ,'IRATE(q_bigint_null)' , 'IRATE(q_smallint_null)' ,'IRATE(q_tinyint_null)' ,'IRATE (q_float_null)' ,'IRATE(q_double_null)' ,
                    'LEASTSQUARES(q_int,15,3)' , 'LEASTSQUARES(q_bigint,10,1)' , 'LEASTSQUARES(q_smallint,20,3)' ,'LEASTSQUARES(q_tinyint,10,4)' ,'LEASTSQUARES(q_float,6,4)' ,'LEASTSQUARES(q_double,3,1)' ,
                    'PERCENTILE(q_int,10)' ,'PERCENTILE(q_bigint,20)' , 'PERCENTILE(q_smallint,30)' ,'PERCENTILE(q_tinyint,40)' ,'PERCENTILE(q_float,50)' ,'PERCENTILE(q_double,60)',
                    'LEASTSQUARES(q_int_null,15,3)' , 'LEASTSQUARES(q_bigint_null,10,1)' , 'LEASTSQUARES(q_smallint_null,20,3)' ,'LEASTSQUARES(q_tinyint_null,10,4)' ,'LEASTSQUARES(q_float_null,6,4)' ,'LEASTSQUARES(q_double_null,3,1)' ,
                    'PERCENTILE(q_int_null,10)' ,'PERCENTILE(q_bigint_null,20)' , 'PERCENTILE(q_smallint_null,30)' ,'PERCENTILE(q_tinyint_null,40)' ,'PERCENTILE(q_float_null,50)' ,'PERCENTILE(q_double_null,60)']

        self.calc_aggregate_groupbytbname = ['twa(q_int)' ,'twa(q_bigint)' , 'twa(q_smallint)' ,'twa(q_tinyint)' ,'twa (q_float)' ,'twa(q_double)' ,
                    'IRATE(q_int)' ,'IRATE(q_bigint)' , 'IRATE(q_smallint)' ,'IRATE(q_tinyint)' ,'IRATE (q_float)' ,'IRATE(q_double)',
                    'twa(q_int_null)' ,'twa(q_bigint_null)' , 'twa(q_smallint_null)' ,'twa(q_tinyint_null)' ,'twa (q_float_null)' ,'twa(q_double_null)' ,
                    'IRATE(q_int_null)' ,'IRATE(q_bigint_null)' , 'IRATE(q_smallint_null)' ,'IRATE(q_tinyint_null)' ,'IRATE (q_float_null)' ,'IRATE(q_double_null)']

        #two table join
        self.calc_aggregate_all_j = ['count(t1.*)' , 'count(t1.q_int)' ,'count(t1.q_bigint)' , 'count(t1.q_smallint)' ,'count(t1.q_tinyint)' ,'count(t1.q_float)' ,
                    'count(t1.q_double)' ,'count(t1.q_binary)' ,'count(t1.q_nchar)' ,'count(t1.q_bool)' ,'count(t1.q_ts)' ,
                    'avg(t1.q_int)' ,'avg(t1.q_bigint)' , 'avg(t1.q_smallint)' ,'avg(t1.q_tinyint)' ,'avg(t1.q_float)' ,'avg(t1.q_double)' ,
                    'sum(t1.q_int)' ,'sum(t1.q_bigint)' , 'sum(t1.q_smallint)' ,'sum(t1.q_tinyint)' ,'sum(t1.q_float)' ,'sum(t1.q_double)' ,
                    'STDDEV(t1.q_int)' ,'STDDEV(t1.q_bigint)' , 'STDDEV(t1.q_smallint)' ,'STDDEV(t1.q_tinyint)' ,'STDDEV(t1.q_float)' ,'STDDEV(t1.q_double)',
                    'APERCENTILE(t1.q_int,10)' ,'APERCENTILE(t1.q_bigint,20)' , 'APERCENTILE(t1.q_smallint,30)' ,'APERCENTILE(t1.q_tinyint,40)' ,'APERCENTILE(t1.q_float,50)' ,'APERCENTILE(t1.q_double,60)' ,
                    'count(t1.q_int_null)' ,'count(t1.q_bigint_null)' , 'count(t1.q_smallint_null)' ,'count(t1.q_tinyint_null)' ,'count(t1.q_float_null)' ,
                    'count(t1.q_double_null)' ,'count(t1.q_binary_null)' ,'count(t1.q_nchar_null)' ,'count(t1.q_bool_null)' ,'count(t1.q_ts_null)' ,
                    'avg(t1.q_int_null)' ,'avg(t1.q_bigint_null)' , 'avg(t1.q_smallint_null)' ,'avg(t1.q_tinyint_null)' ,'avg(t1.q_float_null)' ,'avg(t1.q_double_null)' ,
                    'sum(t1.q_int_null)' ,'sum(t1.q_bigint_null)' , 'sum(t1.q_smallint_null)' ,'sum(t1.q_tinyint_null)' ,'sum(t1.q_float_null)' ,'sum(t1.q_double_null)' ,
                    'STDDEV(t1.q_int_null)' ,'STDDEV(t1.q_bigint_null)' , 'STDDEV(t1.q_smallint_null)' ,'STDDEV(t1.q_tinyint_null)' ,'STDDEV(t1.q_float_null)' ,'STDDEV(t1.q_double_null)',
                    'APERCENTILE(t1.q_int_null,10)' ,'APERCENTILE(t1.q_bigint_null,20)' , 'APERCENTILE(t1.q_smallint_null,30)' ,'APERCENTILE(t1.q_tinyint_null,40)' ,'APERCENTILE(t1.q_float_null,50)' ,'APERCENTILE(t1.q_double,60)' ,
                    'count(t2.*)' , 'count(t2.q_int)' ,'count(t2.q_bigint)' , 'count(t2.q_smallint)' ,'count(t2.q_tinyint)' ,'count(t2.q_float)' ,
                    'count(t2.q_double)' ,'count(t2.q_binary)' ,'count(t2.q_nchar)' ,'count(t2.q_bool)' ,'count(t2.q_ts)' ,
                    'avg(t2.q_int)' ,'avg(t2.q_bigint)' , 'avg(t2.q_smallint)' ,'avg(t2.q_tinyint)' ,'avg(t2.q_float)' ,'avg(t2.q_double)' ,
                    'sum(t2.q_int)' ,'sum(t2.q_bigint)' , 'sum(t2.q_smallint)' ,'sum(t2.q_tinyint)' ,'sum(t2.q_float)' ,'sum(t2.q_double)' ,
                    'STDDEV(t2.q_int)' ,'STDDEV(t2.q_bigint)' , 'STDDEV(t2.q_smallint)' ,'STDDEV(t2.q_tinyint)' ,'STDDEV(t2.q_float)' ,'STDDEV(t2.q_double)',
                    'APERCENTILE(t2.q_int,10)' ,'APERCENTILE(t2.q_bigint,20)' , 'APERCENTILE(t2.q_smallint,30)' ,'APERCENTILE(t2.q_tinyint,40)' ,'APERCENTILE(t2.q_float,50)' ,'APERCENTILE(t2.q_double,60)',
                    'count(t2.q_int_null)' ,'count(t2.q_bigint_null)' , 'count(t2.q_smallint_null)' ,'count(t2.q_tinyint_null)' ,'count(t2.q_float_null)' ,
                    'count(t2.q_double_null)' ,'count(t2.q_binary_null)' ,'count(t2.q_nchar_null)' ,'count(t2.q_bool_null)' ,'count(t2.q_ts_null)' ,
                    'avg(t2.q_int_null)' ,'avg(t2.q_bigint_null)' , 'avg(t2.q_smallint_null)' ,'avg(t2.q_tinyint_null)' ,'avg(t2.q_float_null)' ,'avg(t2.q_double_null)' ,
                    'sum(t2.q_int_null)' ,'sum(t2.q_bigint_null)' , 'sum(t2.q_smallint_null)' ,'sum(t2.q_tinyint_null)' ,'sum(t2.q_float_null)' ,'sum(t2.q_double_null)' ,
                    'STDDEV(t2.q_int_null)' ,'STDDEV(t2.q_bigint_null)' , 'STDDEV(t2.q_smallint_null)' ,'STDDEV(t2.q_tinyint_null)' ,'STDDEV(t2.q_float_null)' ,'STDDEV(t2.q_double_null)',
                    'APERCENTILE(t2.q_int_null,10)' ,'APERCENTILE(t2.q_bigint_null,20)' , 'APERCENTILE(t2.q_smallint_null,30)' ,'APERCENTILE(t2.q_tinyint_null,40)' ,'APERCENTILE(t2.q_float_null,50)' ,'APERCENTILE(t2.q_double,60)']

        self.calc_aggregate_regular_j = ['twa(t1.q_int)' ,'twa(t1.q_bigint)' , 'twa(t1.q_smallint)' ,'twa(t1.q_tinyint)' ,'twa (t1.q_float)' ,'twa(t1.q_double)' ,
                    'IRATE(t1.q_int)' ,'IRATE(t1.q_bigint)' , 'IRATE(t1.q_smallint)' ,'IRATE(t1.q_tinyint)' ,'IRATE (t1.q_float)' ,'IRATE(t1.q_double)' ,
                    'LEASTSQUARES(t1.q_int,15,3)' , 'LEASTSQUARES(t1.q_bigint,10,1)' , 'LEASTSQUARES(t1.q_smallint,20,3)' ,'LEASTSQUARES(t1.q_tinyint,10,4)' ,'LEASTSQUARES(t1.q_float,6,4)' ,'LEASTSQUARES(t1.q_double,3,1)' ,
                    'twa(t2.q_int)' ,'twa(t2.q_bigint)' , 'twa(t2.q_smallint)' ,'twa(t2.q_tinyint)' ,'twa (t2.q_float)' ,'twa(t2.q_double)' ,
                    'IRATE(t2.q_int)' ,'IRATE(t2.q_bigint)' , 'IRATE(t2.q_smallint)' ,'IRATE(t2.q_tinyint)' ,'IRATE (t2.q_float)' ,'IRATE(t2.q_double)',
                    'LEASTSQUARES(t2.q_int,15,3)' , 'LEASTSQUARES(t2.q_bigint,10,1)' , 'LEASTSQUARES(t2.q_smallint,20,3)' ,'LEASTSQUARES(t2.q_tinyint,10,4)' ,'LEASTSQUARES(t2.q_float,6,4)' ,'LEASTSQUARES(t2.q_double,3,1)' ,
                    'twa(t1.q_int_null)' ,'twa(t1.q_bigint_null)' , 'twa(t1.q_smallint_null)' ,'twa(t1.q_tinyint_null)' ,'twa (t1.q_float_null)' ,'twa(t1.q_double_null)' ,
                    'IRATE(t1.q_int_null)' ,'IRATE(t1.q_bigint_null)' , 'IRATE(t1.q_smallint_null)' ,'IRATE(t1.q_tinyint_null)' ,'IRATE (t1.q_float_null)' ,'IRATE(t1.q_double_null)' ,
                    'LEASTSQUARES(t1.q_int_null,15,3)' , 'LEASTSQUARES(t1.q_bigint_null,10,1)' , 'LEASTSQUARES(t1.q_smallint_null,20,3)' ,'LEASTSQUARES(t1.q_tinyint_null,10,4)' ,'LEASTSQUARES(t1.q_float_null,6,4)' ,'LEASTSQUARES(t1.q_double_null,3,1)' ,
                    'twa(t2.q_int_null)' ,'twa(t2.q_bigint_null)' , 'twa(t2.q_smallint_null)' ,'twa(t2.q_tinyint_null)' ,'twa (t2.q_float_null)' ,'twa(t2.q_double_null)' ,
                    'IRATE(t2.q_int_null)' ,'IRATE(t2.q_bigint_null)' , 'IRATE(t2.q_smallint_null)' ,'IRATE(t2.q_tinyint_null)' ,'IRATE (t2.q_float_null)' ,'IRATE(t2.q_double_null)',
                    'LEASTSQUARES(t2.q_int_null,15,3)' , 'LEASTSQUARES(t2.q_bigint_null,10,1)' , 'LEASTSQUARES(t2.q_smallint_null,20,3)' ,'LEASTSQUARES(t2.q_tinyint_null,10,4)' ,'LEASTSQUARES(t2.q_float_null,6,4)' ,'LEASTSQUARES(t2.q_double_null,3,1)' ]

        self.calc_aggregate_groupbytbname_j = ['twa(t1.q_int)' ,'twa(t1.q_bigint)' , 'twa(t1.q_smallint)' ,'twa(t1.q_tinyint)' ,'twa (t1.q_float)' ,'twa(t1.q_double)' ,
                    'IRATE(t1.q_int)' ,'IRATE(t1.q_bigint)' , 'IRATE(t1.q_smallint)' ,'IRATE(t1.q_tinyint)' ,'IRATE (t1.q_float)' ,'IRATE(t1.q_double)' ,
                    'twa(t2.q_int)' ,'twa(t2.q_bigint)' , 'twa(t2.q_smallint)' ,'twa(t2.q_tinyint)' ,'twa (t2.q_float)' ,'twa(t2.q_double)' ,
                    'IRATE(t2.q_int)' ,'IRATE(t2.q_bigint)' , 'IRATE(t2.q_smallint)' ,'IRATE(t2.q_tinyint)' ,'IRATE (t2.q_float)' ,'IRATE(t2.q_double)' ,
                    'twa(t1.q_int_null)' ,'twa(t1.q_bigint_null)' , 'twa(t1.q_smallint_null)' ,'twa(t1.q_tinyint_null)' ,'twa (t1.q_float_null)' ,'twa(t1.q_double_null)' ,
                    'IRATE(t1.q_int_null)' ,'IRATE(t1.q_bigint_null)' , 'IRATE(t1.q_smallint_null)' ,'IRATE(t1.q_tinyint_null)' ,'IRATE (t1.q_float_null)' ,'IRATE(t1.q_double_null)' ,
                    'twa(t2.q_int_null)' ,'twa(t2.q_bigint_null)' , 'twa(t2.q_smallint_null)' ,'twa(t2.q_tinyint_null)' ,'twa (t2.q_float_null)' ,'twa(t2.q_double_null)' ,
                    'IRATE(t2.q_int_null)' ,'IRATE(t2.q_bigint_null)' , 'IRATE(t2.q_smallint_null)' ,'IRATE(t2.q_tinyint_null)' ,'IRATE (t2.q_float_null)' ,'IRATE(t2.q_double_null)'  ]

        # calc_calculate_all   calc_calculate_regular   calc_calculate_groupbytbname
        # calculation function include [all:spread\+-*/ ||regualr:diff\derivative ||group by tbname:diff\derivative\]
        self.calc_calculate_all = ['SPREAD(ts)'  , 'SPREAD(q_ts)'  , 'SPREAD(q_int)' ,'SPREAD(q_bigint)' , 'SPREAD(q_smallint)' ,'SPREAD(q_tinyint)' ,'SPREAD(q_float)' ,'SPREAD(q_double)' ,
                     '(SPREAD(q_int) + SPREAD(q_bigint))' , '(SPREAD(q_smallint) - SPREAD(q_float))', '(SPREAD(q_double) * SPREAD(q_tinyint))' , '(SPREAD(q_double) / SPREAD(q_float))',
                     'SPREAD(q_ts_null)'  , 'SPREAD(q_int_null)' ,'SPREAD(q_bigint_null)' , 'SPREAD(q_smallint_null)' ,'SPREAD(q_tinyint_null)' ,'SPREAD(q_float_null)' ,'SPREAD(q_double_null)' ,
                     '(SPREAD(q_int_null) + SPREAD(q_bigint_null))' , '(SPREAD(q_smallint_null) - SPREAD(q_float_null))', '(SPREAD(q_double_null) * SPREAD(q_tinyint_null))' , '(SPREAD(q_double_null) / SPREAD(q_float_null))']
        self.calc_calculate_regular = ['DIFF(q_int)' ,'DIFF(q_bigint)' , 'DIFF(q_smallint)' ,'DIFF(q_tinyint)' ,'DIFF(q_float)' ,'DIFF(q_double)' ,
                                'DIFF(q_int,0)' ,'DIFF(q_bigint,0)' , 'DIFF(q_smallint,0)' ,'DIFF(q_tinyint,0)' ,'DIFF(q_float,0)' ,'DIFF(q_double,0)' ,
                                'DIFF(q_int,1)' ,'DIFF(q_bigint,1)' , 'DIFF(q_smallint,1)' ,'DIFF(q_tinyint,1)' ,'DIFF(q_float,1)' ,'DIFF(q_double,1)' ,
                                'DERIVATIVE(q_int,15s,0)' , 'DERIVATIVE(q_bigint,10s,1)' , 'DERIVATIVE(q_smallint,20s,0)' ,'DERIVATIVE(q_tinyint,10s,1)' ,'DERIVATIVE(q_float,6s,0)' ,'DERIVATIVE(q_double,3s,1)',
                                'DIFF(q_int_null)' ,'DIFF(q_bigint_null)' , 'DIFF(q_smallint_null)' ,'DIFF(q_tinyint_null)' ,'DIFF(q_float_null)' ,'DIFF(q_double_null)' ,
                                'DIFF(q_int_null,0)' ,'DIFF(q_bigint_null,0)' , 'DIFF(q_smallint_null,0)' ,'DIFF(q_tinyint_null,0)' ,'DIFF(q_float_null,0)' ,'DIFF(q_double_null,0)' ,
                                'DIFF(q_int_null,1)' ,'DIFF(q_bigint_null,1)' , 'DIFF(q_smallint_null,1)' ,'DIFF(q_tinyint_null,1)' ,'DIFF(q_float_null,1)' ,'DIFF(q_double_null,1)' ,
                                'DERIVATIVE(q_int_null,15s,0)' , 'DERIVATIVE(q_bigint_null,10s,1)' , 'DERIVATIVE(q_smallint_null,20s,0)' ,'DERIVATIVE(q_tinyint_null,10s,1)' ,'DERIVATIVE(q_float_null,6s,0)' ,'DERIVATIVE(q_double_null,3s,1)']
        self.calc_calculate_groupbytbname = self.calc_calculate_regular

        #two table join
        self.calc_calculate_all_j = ['SPREAD(t1.ts)'  , 'SPREAD(t1.q_ts)'  , 'SPREAD(t1.q_int)' ,'SPREAD(t1.q_bigint)' , 'SPREAD(t1.q_smallint)' ,'SPREAD(t1.q_tinyint)' ,'SPREAD(t1.q_float)' ,'SPREAD(t1.q_double)' ,
                    'SPREAD(t2.ts)'  , 'SPREAD(t2.q_ts)'  , 'SPREAD(t2.q_int)' ,'SPREAD(t2.q_bigint)' , 'SPREAD(t2.q_smallint)' ,'SPREAD(t2.q_tinyint)' ,'SPREAD(t2.q_float)' ,'SPREAD(t2.q_double)' ,
                    '(SPREAD(t1.q_int) + SPREAD(t1.q_bigint))' , '(SPREAD(t1.q_tinyint) - SPREAD(t1.q_float))', '(SPREAD(t1.q_double) * SPREAD(t1.q_tinyint))' , '(SPREAD(t1.q_double) / SPREAD(t1.q_tinyint))',
                    '(SPREAD(t2.q_int) + SPREAD(t2.q_bigint))' , '(SPREAD(t2.q_smallint) - SPREAD(t2.q_float))', '(SPREAD(t2.q_double) * SPREAD(t2.q_tinyint))' , '(SPREAD(t2.q_double) / SPREAD(t2.q_tinyint))',
                    '(SPREAD(t1.q_int) + SPREAD(t1.q_smallint))' , '(SPREAD(t2.q_smallint) - SPREAD(t2.q_float))', '(SPREAD(t1.q_double) * SPREAD(t1.q_tinyint))' , '(SPREAD(t1.q_double) / SPREAD(t1.q_float))',
                    'SPREAD(t1.q_ts_null)'  , 'SPREAD(t1.q_int_null)' ,'SPREAD(t1.q_bigint_null)' , 'SPREAD(t1.q_smallint_null)' ,'SPREAD(t1.q_tinyint_null)' ,'SPREAD(t1.q_float_null)' ,'SPREAD(t1.q_double_null)' ,
                    'SPREAD(t2.q_ts_null)'  , 'SPREAD(t2.q_int_null)' ,'SPREAD(t2.q_bigint_null)' , 'SPREAD(t2.q_smallint_null)' ,'SPREAD(t2.q_tinyint_null)' ,'SPREAD(t2.q_float_null)' ,'SPREAD(t2.q_double_null)' ,
                    '(SPREAD(t1.q_int_null) + SPREAD(t1.q_bigint_null))' , '(SPREAD(t1.q_tinyint_null) - SPREAD(t1.q_float_null))', '(SPREAD(t1.q_double_null) * SPREAD(t1.q_tinyint_null))' , '(SPREAD(t1.q_double_null) / SPREAD(t1.q_tinyint_null))',
                    '(SPREAD(t2.q_int_null) + SPREAD(t2.q_bigint_null))' , '(SPREAD(t2.q_smallint_null) - SPREAD(t2.q_float_null))', '(SPREAD(t2.q_double_null) * SPREAD(t2.q_tinyint_null))' , '(SPREAD(t2.q_double_null) / SPREAD(t2.q_tinyint_null))',
                    '(SPREAD(t1.q_int_null) + SPREAD(t1.q_smallint_null))' , '(SPREAD(t2.q_smallint_null) - SPREAD(t2.q_float_null))', '(SPREAD(t1.q_double_null) * SPREAD(t1.q_tinyint_null))' , '(SPREAD(t1.q_double_null) / SPREAD(t1.q_float_null))']
        self.calc_calculate_regular_j = ['DIFF(t1.q_int)' ,'DIFF(t1.q_bigint)' , 'DIFF(t1.q_smallint)' ,'DIFF(t1.q_tinyint)' ,'DIFF(t1.q_float)' ,'DIFF(t1.q_double)' ,
                    'DIFF(t1.q_int,0)' ,'DIFF(t1.q_bigint,0)' , 'DIFF(t1.q_smallint,0)' ,'DIFF(t1.q_tinyint,0)' ,'DIFF(t1.q_float,0)' ,'DIFF(t1.q_double,0)' ,
                    'DIFF(t1.q_int,1)' ,'DIFF(t1.q_bigint,1)' , 'DIFF(t1.q_smallint,1)' ,'DIFF(t1.q_tinyint,1)' ,'DIFF(t1.q_float,1)' ,'DIFF(t1.q_double,1)' ,
                    'DERIVATIVE(t1.q_int,15s,0)' , 'DERIVATIVE(t1.q_bigint,10s,1)' , 'DERIVATIVE(t1.q_smallint,20s,0)' ,'DERIVATIVE(t1.q_tinyint,10s,1)' ,'DERIVATIVE(t1.q_float,6s,0)' ,'DERIVATIVE(t1.q_double,3s,1)' ,
                    'DIFF(t2.q_int)' ,'DIFF(t2.q_bigint)' , 'DIFF(t2.q_smallint)' ,'DIFF(t2.q_tinyint)' ,'DIFF(t2.q_float)' ,'DIFF(t2.q_double)' ,
                    'DIFF(t2.q_int,0)' ,'DIFF(t2.q_bigint,0)' , 'DIFF(t2.q_smallint,0)' ,'DIFF(t2.q_tinyint,0)' ,'DIFF(t2.q_float,0)' ,'DIFF(t2.q_double,0)' ,
                    'DIFF(t2.q_int,1)' ,'DIFF(t2.q_bigint,1)' , 'DIFF(t2.q_smallint,1)' ,'DIFF(t2.q_tinyint,1)' ,'DIFF(t2.q_float,1)' ,'DIFF(t2.q_double,1)' ,
                    'DERIVATIVE(t2.q_int,15s,0)' , 'DERIVATIVE(t2.q_bigint,10s,1)' , 'DERIVATIVE(t2.q_smallint,20s,0)' ,'DERIVATIVE(t2.q_tinyint,10s,1)' ,'DERIVATIVE(t2.q_float,6s,0)' ,'DERIVATIVE(t2.q_double,3s,1)' ,
                    'DIFF(t1.q_int_null)' ,'DIFF(t1.q_bigint_null)' , 'DIFF(t1.q_smallint_null)' ,'DIFF(t1.q_tinyint_null)' ,'DIFF(t1.q_float_null)' ,'DIFF(t1.q_double_null)' ,
                    'DIFF(t1.q_int_null,0)' ,'DIFF(t1.q_bigint_null,0)' , 'DIFF(t1.q_smallint_null,0)' ,'DIFF(t1.q_tinyint_null,0)' ,'DIFF(t1.q_float_null,0)' ,'DIFF(t1.q_double_null,0)' ,
                    'DIFF(t1.q_int_null,1)' ,'DIFF(t1.q_bigint_null,1)' , 'DIFF(t1.q_smallint_null,1)' ,'DIFF(t1.q_tinyint_null,1)' ,'DIFF(t1.q_float_null,1)' ,'DIFF(t1.q_double_null,1)' ,
                    'DERIVATIVE(t1.q_int_null,15s,0)' , 'DERIVATIVE(t1.q_bigint_null,10s,1)' , 'DERIVATIVE(t1.q_smallint_null,20s,0)' ,'DERIVATIVE(t1.q_tinyint_null,10s,1)' ,'DERIVATIVE(t1.q_float_null,6s,0)' ,'DERIVATIVE(t1.q_double_null,3s,1)' ,
                    'DIFF(t2.q_int_null)' ,'DIFF(t2.q_bigint_null)' , 'DIFF(t2.q_smallint_null)' ,'DIFF(t2.q_tinyint_null)' ,'DIFF(t2.q_float_null)' ,'DIFF(t2.q_double_null)' ,
                    'DIFF(t2.q_int_null,0)' ,'DIFF(t2.q_bigint_null,0)' , 'DIFF(t2.q_smallint_null,0)' ,'DIFF(t2.q_tinyint_null,0)' ,'DIFF(t2.q_float_null,0)' ,'DIFF(t2.q_double_null,0)' ,
                    'DIFF(t2.q_int_null,1)' ,'DIFF(t2.q_bigint_null,1)' , 'DIFF(t2.q_smallint_null,1)' ,'DIFF(t2.q_tinyint_null,1)' ,'DIFF(t2.q_float_null,1)' ,'DIFF(t2.q_double_null,1)' ,
                    'DERIVATIVE(t2.q_int_null,15s,0)' , 'DERIVATIVE(t2.q_bigint_null,10s,1)' , 'DERIVATIVE(t2.q_smallint_null,20s,0)' ,'DERIVATIVE(t2.q_tinyint_null,10s,1)' ,'DERIVATIVE(t2.q_float_null,6s,0)' ,'DERIVATIVE(t2.q_double_null,3s,1)']
        self.calc_calculate_groupbytbname_j = self.calc_calculate_regular_j

        #inter  && calc_aggregate_all\calc_aggregate_regular\calc_select_all
        self.interval_sliding = ['interval(4w) sliding(1w) ','interval(1w) sliding(1d) ','interval(1d) sliding(1h) ' ,
                    'interval(1h) sliding(1m) ','interval(1m) sliding(1s) ','interval(1s) sliding(10a) ',
                    'interval(1y) ','interval(1n) ','interval(1w) ','interval(1d) ','interval(1h) ','interval(1m) ','interval(1s) ' ,'interval(10a)',
                    'interval(1y,1n) ','interval(1n,1w) ','interval(1w,1d) ','interval(1d,1h) ','interval(1h,1m) ','interval(1m,1s) ','interval(1s,10a) ' ,'interval(100a,30a)']

        self.conn1 = taos.connect(host="127.0.0.1", user="root", password="taosdata", config="/etc/taos/")
        self.cur1 = self.conn1.cursor()
        print(self.cur1)
        self.cur1.execute("use %s ;" %self.db_nest)
        sql = 'select /*+ para_tables_sort() */* from stable_1 limit 5;'
        self.cur1.execute(sql)


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
                q_binary1 binary(100) , q_nchar1 nchar(100) ,q_binary2 binary(100) , q_nchar2 nchar(100) ,q_binary3 binary(100) , q_nchar3 nchar(100) ,q_binary4 binary(100) , q_nchar4 nchar(100) ,\
                q_binary5 binary(100) , q_nchar5 nchar(100) ,q_binary6 binary(100) , q_nchar6 nchar(100) ,q_binary7 binary(100) , q_nchar7 nchar(100) ,q_binary8 binary(100) , q_nchar8 nchar(100) ,\
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')
        tdSql.execute('''create stable stable_2 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_binary1 binary(100) , q_nchar1 nchar(100) ,q_binary2 binary(100) , q_nchar2 nchar(100) ,q_binary3 binary(100) , q_nchar3 nchar(100) ,q_binary4 binary(100) , q_nchar4 nchar(100) ,\
                q_binary5 binary(100) , q_nchar5 nchar(100) ,q_binary6 binary(100) , q_nchar6 nchar(100) ,q_binary7 binary(100) , q_nchar7 nchar(100) ,q_binary8 binary(100) , q_nchar8 nchar(100) ,\
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')

        tdSql.execute('''create stable stable_null_data (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_binary1 binary(100) , q_nchar1 nchar(100) ,q_binary2 binary(100) , q_nchar2 nchar(100) ,q_binary3 binary(100) , q_nchar3 nchar(100) ,q_binary4 binary(100) , q_nchar4 nchar(100) ,\
                q_binary5 binary(100) , q_nchar5 nchar(100) ,q_binary6 binary(100) , q_nchar6 nchar(100) ,q_binary7 binary(100) , q_nchar7 nchar(100) ,q_binary8 binary(100) , q_nchar8 nchar(100) ,\
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')

        tdSql.execute('''create stable stable_null_childtable (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_binary1 binary(100) , q_nchar1 nchar(100) ,q_binary2 binary(100) , q_nchar2 nchar(100) ,q_binary3 binary(100) , q_nchar3 nchar(100) ,q_binary4 binary(100) , q_nchar4 nchar(100) ,\
                q_binary5 binary(100) , q_nchar5 nchar(100) ,q_binary6 binary(100) , q_nchar6 nchar(100) ,q_binary7 binary(100) , q_nchar7 nchar(100) ,q_binary8 binary(100) , q_nchar8 nchar(100) ,\
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
                    q_binary1 binary(100) , q_nchar1 nchar(100) ,q_binary2 binary(100) , q_nchar2 nchar(100) ,q_binary3 binary(100) , q_nchar3 nchar(100) ,q_binary4 binary(100) , q_nchar4 nchar(100) ,\
                    q_binary5 binary(100) , q_nchar5 nchar(100) ,q_binary6 binary(100) , q_nchar6 nchar(100) ,q_binary7 binary(100) , q_nchar7 nchar(100) ,q_binary8 binary(100) , q_nchar8 nchar(100) ,\
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')
        tdSql.execute('''create table regular_table_2 \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_binary1 binary(100) , q_nchar1 nchar(100) ,q_binary2 binary(100) , q_nchar2 nchar(100) ,q_binary3 binary(100) , q_nchar3 nchar(100) ,q_binary4 binary(100) , q_nchar4 nchar(100) ,\
                    q_binary5 binary(100) , q_nchar5 nchar(100) ,q_binary6 binary(100) , q_nchar6 nchar(100) ,q_binary7 binary(100) , q_nchar7 nchar(100) ,q_binary8 binary(100) , q_nchar8 nchar(100) ,\
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')
        tdSql.execute('''create table regular_table_3 \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_binary1 binary(100) , q_nchar1 nchar(100) ,q_binary2 binary(100) , q_nchar2 nchar(100) ,q_binary3 binary(100) , q_nchar3 nchar(100) ,q_binary4 binary(100) , q_nchar4 nchar(100) ,\
                    q_binary5 binary(100) , q_nchar5 nchar(100) ,q_binary6 binary(100) , q_nchar6 nchar(100) ,q_binary7 binary(100) , q_nchar7 nchar(100) ,q_binary8 binary(100) , q_nchar8 nchar(100) ,\
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')

        tdSql.execute('''create table regular_table_null \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_binary1 binary(100) , q_nchar1 nchar(100) ,q_binary2 binary(100) , q_nchar2 nchar(100) ,q_binary3 binary(100) , q_nchar3 nchar(100) ,q_binary4 binary(100) , q_nchar4 nchar(100) ,\
                    q_binary5 binary(100) , q_nchar5 nchar(100) ,q_binary6 binary(100) , q_nchar6 nchar(100) ,q_binary7 binary(100) , q_nchar7 nchar(100) ,q_binary8 binary(100) , q_nchar8 nchar(100) ,\
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')


        for i in range(1, num_random*n + 1):
            tdSql.execute('''insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8) \
                        values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;'''
                        % (ts + i*1000, fake.random_int(min=-2147483647, max=2147483647, step=1),
                        fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() ,
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))
            tdSql.execute('''insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8) \
                        values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;'''
                        % (ts + i*1000, fake.random_int(min=-2147483647, max=2147483647, step=1) ,
                        fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1) ,
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i, fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() ,
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))

            tdSql.execute('''insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8)\
                        values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;'''
                        % (ts + i*1000*60*60*2, fake.random_int(min=0, max=2147483647, step=1),
                        fake.random_int(min=0, max=9223372036854775807, step=1),
                        fake.random_int(min=0, max=32767, step=1) , fake.random_int(min=0, max=127, step=1) ,
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i, fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() ,
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))
            tdSql.execute('''insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8) \
                        values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;'''
                        % (ts + i*1000, fake.random_int(min=0, max=2147483647, step=1),
                        fake.random_int(min=0, max=9223372036854775807, step=1),
                        fake.random_int(min=0, max=32767, step=1) , fake.random_int(min=0, max=127, step=1) ,
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i, fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() ,
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))

            tdSql.execute('''insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8) \
                        values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;'''
                        % (ts + i*1000*60*60*2 +1, fake.random_int(min=-2147483647, max=0, step=1),
                        fake.random_int(min=-9223372036854775807, max=0, step=1),
                        fake.random_int(min=-32767, max=0, step=1) , fake.random_int(min=-127, max=0, step=1) ,
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i +1, fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() ,
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))
            tdSql.execute('''insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8) \
                        values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;'''
                        % (ts + i*1000 +1, fake.random_int(min=-2147483647, max=0, step=1),
                        fake.random_int(min=-9223372036854775807, max=0, step=1),
                        fake.random_int(min=-32767, max=0, step=1) , fake.random_int(min=-127, max=0, step=1) ,
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i +1, fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() ,
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))

            tdSql.execute('''insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8) \
                        values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;'''
                        % (ts + i*1000*60*60*4, fake.random_int(min=-0, max=2147483647, step=1),
                        fake.random_int(min=-0, max=9223372036854775807, step=1),
                        fake.random_int(min=-0, max=32767, step=1) , fake.random_int(min=-0, max=127, step=1) ,
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i, fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() ,
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))

            tdSql.execute('''insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8) \
                        values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;'''
                        % (ts + i*1000*60*60*4 +1, fake.random_int(min=-0, max=2147483647, step=1),
                        fake.random_int(min=-0, max=9223372036854775807, step=1),
                        fake.random_int(min=-0, max=32767, step=1) , fake.random_int(min=-0, max=127, step=1) ,
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i, fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() ,
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))

            tdSql.execute('''insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8) \
                        values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;'''
                        % (ts + i*1000*60*60*4 +10, fake.random_int(min=-0, max=2147483647, step=1),
                        fake.random_int(min=-0, max=9223372036854775807, step=1),
                        fake.random_int(min=-0, max=32767, step=1) , fake.random_int(min=-0, max=127, step=1) ,
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i, fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() ,
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))

        tdSql.query("select /*+ para_tables_sort() */count(*) from stable_1;")
        tdSql.checkData(0,0,3*num_random*n)
        tdSql.query("select /*+ para_tables_sort() */count(*) from regular_table_1;")
        tdSql.checkData(0,0,num_random*n)

    def explain_sql(self,sql):
        # #执行sql解析
        sql = "explain " + sql
        tdLog.info(sql)
        tdSql.query(sql)
        #pass

    def data_check(self,sql,mark='mark') :
        tdLog.info("========mark==%s==="% mark);
        try:
            tdSql.query(sql,queryTimes=1)
            self.explain_sql(sql)
        except:
            tdLog.info("sql is not support :=====%s; " %sql)
            tdSql.error(sql)


    def math_nest(self,mathlist):

        print("==========%s===start=============" %mathlist)
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))

        self.dropandcreateDB_random("%s" %self.db_nest, 1)

        if (mathlist == ['ABS','SQRT']) or (mathlist == ['SIN','COS','TAN','ASIN','ACOS','ATAN']) or (mathlist == ['FLOOR','CEIL','ROUND']) \
            or (mathlist == ['CSUM']) :
            math_functions = mathlist
            fun_fix_column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)','(q_bigint_null)','(q_smallint_null)','(q_tinyint_null)','(q_int_null)','(q_float_null)','(q_double_null)']
            fun_column_1 = random.sample(math_functions,1)+random.sample(fun_fix_column,1)
            math_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            fun_column_2 = random.sample(math_functions,1)+random.sample(fun_fix_column,1)
            math_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","").replace(", ","")

            fun_fix_column_j = ['(t1.q_bigint)','(t1.q_smallint)','(t1.q_tinyint)','(t1.q_int)','(t1.q_float)','(t1.q_double)','(t1.q_bigint_null)','(t1.q_smallint_null)','(t1.q_tinyint_null)','(t1.q_int_null)','(t1.q_float_null)','(t1.q_double_null)',
                            '(t2.q_bigint)','(t2.q_smallint)','(t2.q_tinyint)','(t2.q_int)','(t2.q_float)','(t2.q_double)','(t2.q_bigint_null)','(t2.q_smallint_null)','(t2.q_tinyint_null)','(t2.q_int_null)','(t2.q_float_null)','(t2.q_double_null)']
            fun_column_join_1 = random.sample(math_functions,1)+random.sample(fun_fix_column_j,1)
            math_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            fun_column_join_2 = random.sample(math_functions,1)+random.sample(fun_fix_column_j,1)
            math_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","").replace(", ","")

        elif (mathlist == ['UNIQUE']) or (mathlist == ['HYPERLOGLOG']) or (mathlist == ['MODE'])  :
            math_functions = mathlist
            fun_fix_column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)','(q_binary)','(q_nchar)','(q_bool)','(q_ts)',
                '(q_bigint_null)','(q_smallint_null)','(q_tinyint_null)','(q_int_null)','(q_float_null)','(q_double_null)','(q_binary_null)','(q_nchar_null)','(q_bool_null)','(q_ts_null)']
            fun_column_1 = random.sample(math_functions,1)+random.sample(fun_fix_column,1)
            math_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            fun_column_2 = random.sample(math_functions,1)+random.sample(fun_fix_column,1)
            math_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","").replace(", ","")

            fun_fix_column_j = ['(t1.q_bigint)','(t1.q_smallint)','(t1.q_tinyint)','(t1.q_int)','(t1.q_float)','(t1.q_double)','(t1.q_bigint_null)','(t1.q_smallint_null)','(t1.q_tinyint_null)','(t1.q_int_null)','(t1.q_float_null)','(t1.q_double_null)','(t1.q_ts)','(t1.q_ts_null)',
                            '(t2.q_bigint)','(t2.q_smallint)','(t2.q_tinyint)','(t2.q_int)','(t2.q_float)','(t2.q_double)','(t2.q_bigint_null)','(t2.q_smallint_null)','(t2.q_tinyint_null)','(t2.q_int_null)','(t2.q_float_null)','(t2.q_double_null)','(t2.q_ts)','(t2.q_ts_null)']
            fun_column_join_1 = random.sample(math_functions,1)+random.sample(fun_fix_column_j,1)
            math_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            fun_column_join_2 = random.sample(math_functions,1)+random.sample(fun_fix_column_j,1)
            math_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","").replace(", ","")

        elif (mathlist == ['TAIL']):
            math_functions = mathlist
            num = random.randint(1, 100)
            offset_rows = random.randint(0, 100)
            fun_fix_column = ['(q_bigint,num)','(q_smallint,num)','(q_tinyint,num)','(q_int,num)','(q_float,num)','(q_double,num)','(q_binary,num)','(q_nchar,num)','(q_bool,num)','(q_ts,num)',
                '(q_bigint_null,num)','(q_smallint_null,num)','(q_tinyint_null,num)','(q_int_null,num)','(q_float_null,num)','(q_double_null,num)','(q_binary_null,num)','(q_nchar_null,num)','(q_bool_null,num)','(q_ts_null,num)',
                '(q_bigint,num,offset_rows)','(q_smallint,num,offset_rows)','(q_tinyint,num,offset_rows)','(q_int,num,offset_rows)','(q_float,num,offset_rows)','(q_double,num,offset_rows)','(q_binary,num,offset_rows)','(q_nchar,num,offset_rows)','(q_bool,num,offset_rows)','(q_ts,num,offset_rows)',
                '(q_bigint_null,num,offset_rows)','(q_smallint_null,num,offset_rows)','(q_tinyint_null,num,offset_rows)','(q_int_null,num,offset_rows)','(q_float_null,num,offset_rows)','(q_double_null,num,offset_rows)','(q_binary_null,num,offset_rows)','(q_nchar_null,num,offset_rows)','(q_bool_null,num,offset_rows)','(q_ts_null,num,offset_rows)']
            fun_column_1 = random.sample(math_functions,1)+random.sample(fun_fix_column,1)
            math_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","").replace(", ","").replace("num",str(num)).replace("offset_rows",str(offset_rows))
            fun_column_2 = random.sample(math_functions,1)+random.sample(fun_fix_column,1)
            math_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","").replace(", ","").replace("num",str(num)).replace("offset_rows",str(offset_rows))

            fun_fix_column_j = ['(t1.q_bigint,num)','(t1.q_smallint,num)','(t1.q_tinyint,num)','(t1.q_int,num)','(t1.q_float,num)','(t1.q_double,num)','(t1.q_binary,num)','(t1.q_nchar,num)','(t1.q_bool,num)','(t1.q_ts,num)',
                    '(t1.q_bigint_null,num)','(t1.q_smallint_null,num)','(t1.q_tinyint_null,num)','(t1.q_int_null,num)','(t1.q_float_null,num)','(t1.q_double_null,num)','(t1.q_binary_null,num)','(t1.q_nchar_null,num)','(t1.q_bool_null,num)','(t1.q_ts_null,num)',
                    '(t2.q_bigint,num)','(t2.q_smallint,num)','(t2.q_tinyint,num)','(t2.q_int,num)','(t2.q_float,num)','(t2.q_double,num)','(t2.q_binary,num)','(t2.q_nchar,num)','(t2.q_bool,num)','(t2.q_ts,num)',
                    '(t2.q_bigint_null,num)','(t2.q_smallint_null,num)','(t2.q_tinyint_null,num)','(t2.q_int_null,num)','(t2.q_float_null,num)','(t2.q_double_null,num)','(t2.q_binary_null,num)','(t2.q_nchar_null,num)','(t2.q_bool_null,num)','(t2.q_ts_null,num)',
                    '(t1.q_bigint,num,offset_rows)','(t1.q_smallint,num,offset_rows)','(t1.q_tinyint,num,offset_rows)','(t1.q_int,num,offset_rows)','(t1.q_float,num,offset_rows)','(t1.q_double,num,offset_rows)','(t1.q_binary,num,offset_rows)','(t1.q_nchar,num,offset_rows)','(t1.q_bool,num,offset_rows)','(t1.q_ts,num,offset_rows)',
                    '(t1.q_bigint_null,num,offset_rows)','(t1.q_smallint_null,num,offset_rows)','(t1.q_tinyint_null,num,offset_rows)','(t1.q_int_null,num,offset_rows)','(t1.q_float_null,num,offset_rows)','(t1.q_double_null,num,offset_rows)','(t1.q_binary_null,num,offset_rows)','(t1.q_nchar_null,num,offset_rows)','(t1.q_bool_null,num,offset_rows)','(t1.q_ts_null,num,offset_rows)',
                    '(t2.q_bigint,num,offset_rows)','(t2.q_smallint,num,offset_rows)','(t2.q_tinyint,num,offset_rows)','(t2.q_int,num,offset_rows)','(t2.q_float,num,offset_rows)','(t2.q_double,num,offset_rows)','(t2.q_binary,num,offset_rows)','(t2.q_nchar,num,offset_rows)','(t2.q_bool,num,offset_rows)','(t2.q_ts,num,offset_rows)',
                    '(t2.q_bigint_null,num,offset_rows)','(t2.q_smallint_null,num,offset_rows)','(t2.q_tinyint_null,num,offset_rows)','(t2.q_int_null,num,offset_rows)','(t2.q_float_null,num,offset_rows)','(t2.q_double_null,num,offset_rows)','(t2.q_binary_null,num,offset_rows)','(t2.q_nchar_null,num,offset_rows)','(t2.q_bool_null,num,offset_rows)','(t2.q_ts_null,num,offset_rows)']
            fun_column_join_1 = random.sample(math_functions,1)+random.sample(fun_fix_column_j,1)
            math_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","").replace(", ","").replace("num",str(num)).replace("offset_rows",str(offset_rows))
            fun_column_join_2 = random.sample(math_functions,1)+random.sample(fun_fix_column_j,1)
            math_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","").replace(", ","").replace("num",str(num)).replace("offset_rows",str(offset_rows))

        elif (mathlist == ['POW','LOG']) or (mathlist == ['MAVG']) or (mathlist == ['SAMPLE']) :
            math_functions = mathlist
            num = random.randint(1, 1000)
            fun_fix_column = ['(q_bigint,num)','(q_smallint,num)','(q_tinyint,num)','(q_int,num)','(q_float,num)','(q_double,num)',
                              '(q_bigint_null,num)','(q_smallint_null,num)','(q_tinyint_null,num)','(q_int_null,num)','(q_float_null,num)','(q_double_null,num)']
            fun_column_1 = random.sample(math_functions,1)+random.sample(fun_fix_column,1)
            math_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","").replace(", ","").replace("num",str(num))
            fun_column_2 = random.sample(math_functions,1)+random.sample(fun_fix_column,1)
            math_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","").replace(", ","").replace("num",str(num))

            fun_fix_column_j = ['(t1.q_bigint,num)','(t1.q_smallint,num)','(t1.q_tinyint,num)','(t1.q_int,num)','(t1.q_float,num)','(t1.q_double,num)',
                    '(t1.q_bigint_null,num)','(t1.q_smallint_null,num)','(t1.q_tinyint_null,num)','(t1.q_int_null,num)','(t1.q_float_null,num)','(t1.q_double_null,num)',
                    '(t2.q_bigint,num)','(t2.q_smallint,num)','(t2.q_tinyint,num)','(t2.q_int,num)','(t2.q_float,num)','(t2.q_double,num)',
                    '(t2.q_bigint_null,num)','(t2.q_smallint_null,num)','(t2.q_tinyint_null,num)','(t2.q_int_null,num)','(t2.q_float_null,num)','(t2.q_double_null,num)']
            fun_column_join_1 = random.sample(math_functions,1)+random.sample(fun_fix_column_j,1)
            math_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","").replace(", ","").replace("num",str(num))
            fun_column_join_2 = random.sample(math_functions,1)+random.sample(fun_fix_column_j,1)
            math_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","").replace(", ","").replace("num",str(num))

        elif (mathlist == ['statecount','stateduration']):
            math_functions = mathlist
            num = random.randint(-1000, 1000)

            operator = ['LT' , 'GT' ,'GE','NE','EQ']
            oper = str(random.sample(operator,1)).replace("[","").replace("]","")#.replace("'","")

            fun_fix_column = ['(q_bigint,oper,num,time)','(q_smallint,oper,num,time)','(q_tinyint,oper,num,time)','(q_int,oper,num,time)','(q_float,oper,num,time)','(q_double,oper,num,time)',
                              '(q_bigint_null,oper,num,time)','(q_smallint_null,oper,num,time)','(q_tinyint_null,oper,num,time)','(q_int_null,oper,num,time)','(q_float_null,oper,num,time)','(q_double_null,oper,num,time)']

            hanshu_select1 = random.sample(math_functions,1)
            fun_column_1 = random.sample(hanshu_select1,1)+random.sample(fun_fix_column,1)
            math_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","").replace(", ","")

            if str(hanshu_select1).replace("[","").replace("]","").replace("'","") == 'statecount':
                math_fun_1 = math_fun_1.replace("oper","%s" %oper).replace(",time","").replace("num",str(num))
            elif str(hanshu_select1).replace("[","").replace("]","").replace("'","") == 'stateduration':
                timeunit = ['1s' , '1m' ,'1h']
                time = str(random.sample(timeunit,1)).replace("[","").replace("]","").replace("'","")
                math_fun_1 = math_fun_1.replace("oper","%s" %oper).replace("time","%s" %time).replace("num",str(num))

            hanshu_select2 = random.sample(math_functions,1)
            fun_column_2 = random.sample(hanshu_select2,1)+random.sample(fun_fix_column,1)
            math_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","").replace(", ","")

            if str(hanshu_select2).replace("[","").replace("]","").replace("'","") == 'statecount':
                math_fun_2 = math_fun_2.replace("oper","%s" %oper).replace(",time","").replace("num",str(num))
            elif str(hanshu_select2).replace("[","").replace("]","").replace("'","") == 'stateduration':
                timeunit = ['1s' , '1m' ,'1h']
                time = str(random.sample(timeunit,1)).replace("[","").replace("]","").replace("'","")
                math_fun_2 = math_fun_2.replace("oper","%s" %oper).replace("time","%s" %time).replace("num",str(num))

            fun_fix_column_j = ['(t1.q_bigint,oper,num,time)','(t1.q_smallint,oper,num,time)','(t1.q_tinyint,oper,num,time)','(t1.q_int,oper,num,time)','(t1.q_float,oper,num,time)','(t1.q_double,oper,num,time)',
                              '(t1.q_bigint_null,oper,num,time)','(t1.q_smallint_null,oper,num,time)','(t1.q_tinyint_null,oper,num,time)','(t1.q_int_null,oper,num,time)','(t1.q_float_null,oper,num,time)','(t1.q_double_null,oper,num,time)',
                              '(t2.q_bigint,oper,num,time)','(t2.q_smallint,oper,num,time)','(t2.q_tinyint,oper,num,time)','(t2.q_int,oper,num,time)','(t2.q_float,oper,num,time)','(t2.q_double,oper,num,time)',
                              '(t2.q_bigint_null,oper,num,time)','(t2.q_smallint_null,oper,num,time)','(t2.q_tinyint_null,oper,num,time)','(t2.q_int_null,oper,num,time)','(t2.q_float_null,oper,num,time)','(t2.q_double_null,oper,num,time)']

            hanshu_select_join_1 = random.sample(math_functions,1)
            fun_column_join_1 = random.sample(hanshu_select_join_1,1)+random.sample(fun_fix_column_j,1)
            math_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","").replace(", ","")

            if str(hanshu_select_join_1).replace("[","").replace("]","").replace("'","") == 'statecount':
                math_fun_join_1 = math_fun_join_1.replace("oper","%s" %oper).replace(",time","").replace("num",str(num))
            elif str(hanshu_select_join_1).replace("[","").replace("]","").replace("'","") == 'stateduration':
                timeunit = ['1s' , '1m' ,'1h']
                time = str(random.sample(timeunit,1)).replace("[","").replace("]","").replace("'","")
                math_fun_join_1 = math_fun_join_1.replace("oper","%s" %oper).replace("time","%s" %time).replace("num",str(num))

            hanshu_select_join_2 = random.sample(math_functions,1)
            fun_column_join_2 = random.sample(hanshu_select_join_2,1)+random.sample(fun_fix_column_j,1)
            math_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","").replace(", ","")

            if str(hanshu_select_join_2).replace("[","").replace("]","").replace("'","") == 'statecount':
                math_fun_join_2 = math_fun_join_2.replace("oper","%s" %oper).replace(",time","").replace("num",str(num))
            elif str(hanshu_select_join_2).replace("[","").replace("]","").replace("'","") == 'stateduration':
                timeunit = ['1s' , '1m' ,'1h']
                time = str(random.sample(timeunit,1)).replace("[","").replace("]","").replace("'","")
                math_fun_join_2 = math_fun_join_2.replace("oper","%s" %oper).replace("time","%s" %time).replace("num",str(num))

        elif(mathlist == ['HISTOGRAM']) :
            math_functions = mathlist
            fun_fix_column = ['(q_bigint','(q_smallint','(q_tinyint','(q_int','(q_float','(q_double','(q_bigint_null','(q_smallint_null','(q_tinyint_null','(q_int_null','(q_float_null','(q_double_null']

            fun_fix_column_j = ['(t1.q_bigint','(t1.q_smallint','(t1.q_tinyint','(t1.q_int','(t1.q_float','(t1.q_double','(t1.q_bigint_null','(t1.q_smallint_null','(t1.q_tinyint_null','(t1.q_int_null','(t1.q_float_null','(t1.q_double_null',
                             '(t2.q_bigint','(t2.q_smallint','(t2.q_tinyint','(t2.q_int','(t2.q_float','(t2.q_double','(t2.q_bigint_null','(t2.q_smallint_null','(t2.q_tinyint_null','(t2.q_int_null','(t2.q_float_null','(t2.q_double_null']

            normalized = random.randint(0, 1)

            i = random.randint(1,3)
            if i == 1:
                bin_type = 'user_input'
                bin_description = {-11111119395555977777}  #9一会转译成，
                fun_column_1 = [math_functions , random.sample(fun_fix_column,1), ',',"'%s'" %bin_type, ',',"'%s'" % bin_description, ',', "%d" %normalized,')']
                math_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","").replace(", ","").replace("{","[").replace("}","]").replace("9",",")

                fun_column_2 = [math_functions , random.sample(fun_fix_column,1), ',',"'%s'" %bin_type, ',',"'%s'" % bin_description, ',', "%d" %normalized,')']
                math_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","").replace(", ","").replace("{","[").replace("}","]").replace("9",",")

                fun_column_join_1 = [math_functions , random.sample(fun_fix_column_j,1), ',',"'%s'" %bin_type, ',',"'%s'" % bin_description, ',', "%d" %normalized,')']
                math_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","").replace(", ","").replace("{","[").replace("}","]").replace("9",",")

                fun_column_join_2 = [math_functions , random.sample(fun_fix_column_j,1), ',',"'%s'" %bin_type, ',',"'%s'" % bin_description, ',', "%d" %normalized,')']
                math_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","").replace(", ","").replace("{","[").replace("}","]").replace("9",",")

            elif i == 2:
                bin_type = 'linear_bin'
                true_false = random.randint(10, 11)
                bin_description = {"ZstartZ": -333339, "ZwidthZ":559, "ZcountZ":59, "ZinfinityZ":'%d' %true_false}  #Z一会转译成" ，9一会转译成 ，
                fun_column_1 = [math_functions , random.sample(fun_fix_column,1), ',',"'%s'" %bin_type, ',','%s' % bin_description, ',', "%d" %normalized,')']
                math_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","").replace(", ","").replace("9",",").replace("Z","\"").replace("10","false").replace("11","true").replace("\"{","'{").replace("}\"","}'")

                fun_column_2 = [math_functions , random.sample(fun_fix_column,1), ',',"'%s'" %bin_type, ',','%s' % bin_description, ',', "%d" %normalized,')']
                math_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","").replace(", ","").replace("9",",").replace("Z","\"").replace("10","false").replace("11","true").replace("\"{","'{").replace("}\"","}'")

                fun_column_join_1 = [math_functions , random.sample(fun_fix_column_j,1), ',',"'%s'" %bin_type, ',','%s' % bin_description, ',', "%d" %normalized,')']
                math_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","").replace(", ","").replace("9",",").replace("Z","\"").replace("10","false").replace("11","true").replace("\"{","'{").replace("}\"","}'")

                fun_column_join_2 = [math_functions , random.sample(fun_fix_column_j,1), ',',"'%s'" %bin_type, ',','%s' % bin_description, ',', "%d" %normalized,')']
                math_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","").replace(", ","").replace("9",",").replace("Z","\"").replace("10","false").replace("11","true").replace("\"{","'{").replace("}\"","}'")

            elif i == 3:
                bin_type = 'log_bin'
                true_false = random.randint(10, 11)
                bin_description = {"ZstartZ": -333339, "ZfactorZ":559, "ZcountZ":59, "ZinfinityZ":'%d' %true_false}  #Z一会转译成" ，9一会转译成 ，
                fun_column_1 = [math_functions , random.sample(fun_fix_column,1), ',',"'%s'" %bin_type, ',','%s' % bin_description, ',', "%d" %normalized,')']
                math_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","").replace(", ","").replace("9",",").replace("Z","\"").replace("10","false").replace("11","true").replace("\"{","'{").replace("}\"","}'")

                fun_column_2 = [math_functions , random.sample(fun_fix_column,1), ',',"'%s'" %bin_type, ',','%s' % bin_description, ',', "%d" %normalized,')']
                math_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","").replace(", ","").replace("9",",").replace("Z","\"").replace("10","false").replace("11","true").replace("\"{","'{").replace("}\"","}'")

                fun_column_join_1 = [math_functions , random.sample(fun_fix_column_j,1), ',',"'%s'" %bin_type, ',','%s' % bin_description, ',', "%d" %normalized,')']
                math_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","").replace(", ","").replace("9",",").replace("Z","\"").replace("10","false").replace("11","true").replace("\"{","'{").replace("}\"","}'")

                fun_column_join_2 = [math_functions , random.sample(fun_fix_column_j,1), ',',"'%s'" %bin_type, ',','%s' % bin_description, ',', "%d" %normalized,')']
                math_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","").replace(", ","").replace("9",",").replace("Z","\"").replace("10","false").replace("11","true").replace("\"{","'{").replace("}\"","}'")

        tdSql.query("select /*+ para_tables_sort() */1-1 as math_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (mathlist == ['SIN','COS','TAN','ASIN','ACOS','ATAN']) or (mathlist == ['ABS','SQRT'])  \
                or (mathlist == ['POW','LOG']) or (mathlist == ['FLOOR','CEIL','ROUND']):
                sql = "select /*+ para_tables_sort() */ ts1 , floor(asct1)  from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1, " % math_fun_1
                sql += "%s as asct2, " % math_fun_2
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts as ts1 from regular_table_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ");"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                tdSql.checkRows(100)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (mathlist == ['MAVG']) or (mathlist == ['SAMPLE']) or  (mathlist == ['TAIL']) or (mathlist == ['CSUM']) or (mathlist == ['HISTOGRAM'])  \
                or (mathlist == ['HYPERLOGLOG']) or (mathlist == ['UNIQUE']) or (mathlist == ['MODE']) or (mathlist == ['statecount','stateduration']) :
                sql = "select /*+ para_tables_sort() */  count(asct1)  from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1 " % math_fun_1
                sql += " from regular_table_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += ");"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-2 as math_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (mathlist == ['SIN','COS','TAN','ASIN','ACOS','ATAN']) or (mathlist == ['ABS','SQRT'])  \
                or (mathlist == ['POW','LOG']) or (mathlist == ['FLOOR','CEIL','ROUND']) :
                sql = "select /*+ para_tables_sort() */ ts , abs(asct1) from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1,  " % math_fun_1
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts ts from regular_table_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s )" % random.choice(self.order_where)
                sql += "%s " % random.choice(self.unionall_or_union)
                sql += "select /*+ para_tables_sort() */ ts , asct2 from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct2,  " % math_fun_2
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts ts from regular_table_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (mathlist == ['MAVG']) or (mathlist == ['SAMPLE']) or (mathlist == ['TAIL']) or (mathlist == ['CSUM']) or (mathlist == ['HISTOGRAM']) \
                or (mathlist == ['HYPERLOGLOG'])  or (mathlist == ['UNIQUE']) or (mathlist == ['MODE']) or (mathlist == ['statecount','stateduration']):
                sql = "select /*+ para_tables_sort() */ count(asct1) from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1  " % math_fun_1
                sql += "from regular_table_1 where "
                sql += "%s )" % random.choice(self.q_where)
                sql += "%s " % random.choice(self.unionall_or_union)
                sql += "select /*+ para_tables_sort() */  count(asct2) from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct2  " % math_fun_2
                sql += " from regular_table_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-3 as math_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (mathlist == ['SIN','COS','TAN','ASIN','ACOS','ATAN']) or (mathlist == ['ABS','SQRT'])  \
                or (mathlist == ['POW','LOG']) or (mathlist == ['FLOOR','CEIL','ROUND']) :
                sql = "select /*+ para_tables_sort() */ ts , min(asct1) from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1, ts ," % math_fun_1
                sql += "%s as asct2, " % math_fun_2
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s " % random.choice(self.q_select)
                sql += " from regular_table_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s select /*+ para_tables_sort() */" % random.choice(self.unionall_or_union)
                sql += "%s as asct2, ts ," % math_fun_2
                sql += "%s as asct1, " % math_fun_1
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s " % random.choice(self.q_select)
                sql += " from regular_table_2 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (mathlist == ['MODE']) :
                sql = "select /*+ para_tables_sort() */ count(asct1) from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1," % math_fun_1
                sql += "%s as asct2 " % math_fun_2
                sql += " from regular_table_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s select /*+ para_tables_sort() */" % random.choice(self.unionall_or_union)
                sql += "%s as asct2 ," % math_fun_2
                sql += "%s as asct1 " % math_fun_1
                sql += " from regular_table_2 where "
                sql += "%s " % random.choice(self.q_where)
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (mathlist == ['MAVG']) or (mathlist == ['statecount','stateduration']) or (mathlist == ['CSUM']) or (mathlist == ['HISTOGRAM']) \
                or (mathlist == ['TAIL']) or (mathlist == ['HYPERLOGLOG']) or (mathlist == ['UNIQUE']) or (mathlist == ['SAMPLE']):
                sql = "select /*+ para_tables_sort() */ count(asct1) from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1 " % math_fun_1
                sql += " from regular_table_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s select /*+ para_tables_sort() */" % random.choice(self.unionall_or_union)
                sql += "%s as asct2 " % math_fun_2
                sql += " from regular_table_2 where "
                sql += "%s " % random.choice(self.q_where)
                sql += " order by asct1 asc "
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-4 as math_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (mathlist == ['SIN','COS','TAN','ASIN','ACOS','ATAN']) or (mathlist == ['ABS','SQRT'])  \
                or (mathlist == ['POW','LOG']) or (mathlist == ['FLOOR','CEIL','ROUND']) :
                sql = "select /*+ para_tables_sort() */ ts1,ts2 ,timediff(ts1,ts2), asct1 from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
                sql += "%s as asct0, " % math_fun_join_1
                sql += "%s as asct1, " % math_fun_join_2
                sql += "%s as asct2, " % math_fun_join_1
                sql += "t1.%s as asct12, " % random.choice(self.q_select)
                sql += "t2.%s as asct22, " % random.choice(self.q_select)
                sql += "t2.ts as ts2 from regular_table_1 t1 , regular_table_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.q_u_where)
                sql += "and %s " % random.choice(self.q_u_or_where)
                sql += "%s " % random.choice(self.order_u_where)
                sql += ");"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                tdSql.checkRows(100)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (mathlist == ['MAVG']) or (mathlist == ['SAMPLE'])or (mathlist == ['TAIL']) or (mathlist == ['CSUM']) or (mathlist == ['HISTOGRAM']) \
                or (mathlist == ['HYPERLOGLOG']) or (mathlist == ['UNIQUE']) or (mathlist == ['MODE']) or (mathlist == ['statecount','stateduration']) :
                sql = "select /*+ para_tables_sort() */ count(asct1) from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1 " % math_fun_join_2
                sql += "from regular_table_1 t1 , regular_table_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.q_u_where)
                sql += "and %s " % random.choice(self.q_u_or_where)
                sql += ");"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-5 as math_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (mathlist == ['SIN','COS','TAN','ASIN','ACOS','ATAN']) or (mathlist == ['ABS','SQRT'])  \
                or (mathlist == ['POW','LOG']) or (mathlist == ['FLOOR','CEIL','ROUND']) :
                sql = "select /*+ para_tables_sort() */ ts ,"
                sql += "%s, " % math_fun_1
                sql += "%s as asct1, " % random.choice(self.q_select)
                sql += "%s as asct2, " % random.choice(self.q_select)
                sql += "%s  " % math_fun_2
                sql += " from  ( select /*+ para_tables_sort() */ * from regular_table_1 ) where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += " ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                tdSql.checkRows(100)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (mathlist == ['MAVG']) or (mathlist == ['SAMPLE'])or (mathlist == ['TAIL']) or (mathlist == ['CSUM']) or (mathlist == ['HISTOGRAM']) \
                or (mathlist == ['HYPERLOGLOG']) or (mathlist == ['UNIQUE']) or (mathlist == ['MODE']) or (mathlist == ['statecount','stateduration']) :
                sql = "select /*+ para_tables_sort() */"
                sql += "%s  " % math_fun_2
                sql += " from  ( select /*+ para_tables_sort() */ * from regular_table_1 ) where "
                sql += "%s " % random.choice(self.q_where)
                sql += " ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-6 as math_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (mathlist == ['SIN','COS','TAN','ASIN','ACOS','ATAN']) or (mathlist == ['ABS','SQRT'])  \
                or (mathlist == ['POW','LOG']) or (mathlist == ['FLOOR','CEIL','ROUND']) :
                sql = "select /*+ para_tables_sort() */ ts1 ,timediff(ts1,ts2), max(asct1) from  ( select /*+ para_tables_sort() */ t1.ts,t1.ts as ts1,"
                sql += "%s as asct0, " % math_fun_join_1
                sql += "%s as asct1, " % math_fun_join_2
                sql += "t1.%s as asct11, " % random.choice(self.q_select)
                sql += "t2.%s as asct12, " % random.choice(self.q_select)
                sql += "%s as asct13, " % math_fun_join_1
                sql += "t2.ts as ts2 from regular_table_1 t1 , regular_table_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.q_u_where)
                sql += "and %s  )" % random.choice(self.q_u_or_where)
                sql += "%s " % random.choice(self.order_where)
                sql += "%s " % random.choice(self.limit1_where)
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (mathlist == ['MAVG']) or (mathlist == ['SAMPLE'])or (mathlist == ['TAIL']) or (mathlist == ['CSUM']) or (mathlist == ['HISTOGRAM'])  \
                or (mathlist == ['HYPERLOGLOG']) or (mathlist == ['UNIQUE']) or (mathlist == ['MODE']) or (mathlist == ['statecount','stateduration']) :
                sql = "select /*+ para_tables_sort() */ count(asct1) from  ( select /*+ para_tables_sort() */"
                sql += "%s as asct1 " % math_fun_join_2
                sql += "from regular_table_1 t1 , regular_table_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.q_u_where)
                sql += "and %s  )" % random.choice(self.q_u_or_where)
                sql += "%s " % random.choice(self.limit1_where)
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-7 as math_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (mathlist == ['SIN','COS','TAN','ASIN','ACOS','ATAN']) or (mathlist == ['ABS','SQRT'])  \
                or (mathlist == ['POW','LOG']) or (mathlist == ['FLOOR','CEIL','ROUND']) :
                sql = "select /*+ para_tables_sort() */ ts1,ts2 , abs(asct1) from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1, ts as ts1," % math_fun_1
                sql += "%s as asct2, " % math_fun_2
                sql += "%s, " % random.choice(self.q_select)
                sql += "%s, " % random.choice(self.t_select)
                sql += "ts as ts2 from stable_1 where "
                sql += "%s " % random.choice(self.qt_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                tdSql.checkRows(300)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (mathlist == ['MAVG']) or (mathlist == ['SAMPLE']) or (mathlist == ['TAIL']) or (mathlist == ['CSUM']) or (mathlist == ['HISTOGRAM'])  \
                or (mathlist == ['HYPERLOGLOG']) or (mathlist == ['UNIQUE']) or (mathlist == ['MODE']) or (mathlist == ['statecount','stateduration']) :
                sql = "select /*+ para_tables_sort() */  count(asct1) from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1 " % math_fun_1
                sql += "from stable_1 where "
                sql += "%s " % random.choice(self.qt_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-8 as math_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (mathlist == ['SIN','COS','TAN','ASIN','ACOS','ATAN']) or (mathlist == ['ABS','SQRT'])  \
                or (mathlist == ['POW','LOG']) or (mathlist == ['FLOOR','CEIL','ROUND']) :
                sql = "select /*+ para_tables_sort() */ ts1,floor(asct1),ts2 "
                sql += "from  ( select /*+ para_tables_sort() */ "
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s as asct1, ts as ts1," % math_fun_1
                sql += "%s as asct2, " % math_fun_2
                sql += "%s, " % random.choice(self.q_select)
                sql += "%s, " % random.choice(self.t_select)
                sql += "ts as ts2 from stable_1 where "
                sql += "%s " % random.choice(self.qt_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                tdSql.checkRows(300)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (mathlist == ['MAVG']) or (mathlist == ['SAMPLE'])or (mathlist == ['TAIL']) or (mathlist == ['CSUM']) or (mathlist == ['HISTOGRAM'])  \
                or (mathlist == ['HYPERLOGLOG']) or (mathlist == ['UNIQUE']) or (mathlist == ['MODE']) or (mathlist == ['statecount','stateduration']) :
                sql = "select /*+ para_tables_sort() */ count(asct1) "
                sql += "from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1 " % math_fun_1
                sql += " from stable_1 where "
                sql += "%s " % random.choice(self.qt_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-9 as math_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (mathlist == ['SIN','COS','TAN','ASIN','ACOS','ATAN']) or (mathlist == ['ABS','SQRT'])  \
                or (mathlist == ['POW','LOG']) or (mathlist == ['FLOOR','CEIL','ROUND']) :
                sql = "select /*+ para_tables_sort() */ ts1,ts2 ,timediff(ts1,ts2) , max(asct1) from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
                sql += "%s, " % math_fun_join_1
                sql += "%s as asct1, " % math_fun_join_2
                sql += "t1.%s as asct11, " % random.choice(self.q_select)
                sql += "t1.%s as asct12, " % random.choice(self.q_select)
                sql += "t2.%s as asct21, " % random.choice(self.q_select)
                sql += "t2.%s as asct22, " % random.choice(self.q_select)
                sql += "t2.ts as ts2  from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.t_join_where)
                sql += "and %s " % random.choice(self.t_u_where)
                sql += "and %s " % random.choice(self.t_u_or_where)
                sql += "%s " % random.choice(self.order_u_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (mathlist == ['MAVG']) or (mathlist == ['SAMPLE'])or (mathlist == ['TAIL']) or (mathlist == ['CSUM']) or (mathlist == ['HISTOGRAM'])  \
                or (mathlist == ['HYPERLOGLOG']) or (mathlist == ['UNIQUE']) or (mathlist == ['MODE']) or (mathlist == ['statecount','stateduration']) :
                sql = "select /*+ para_tables_sort() */ count(asct1) from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1 " % math_fun_join_2
                sql += "from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.t_join_where)
                sql += "and %s " % random.choice(self.t_u_where)
                sql += "and %s " % random.choice(self.t_u_or_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        self.restartDnodes()
        tdSql.query("select /*+ para_tables_sort() */1-10 as math_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (mathlist == ['SIN','COS','TAN','ASIN','ACOS','ATAN']) or (mathlist == ['ABS','SQRT'])  \
                or (mathlist == ['POW','LOG']) or (mathlist == ['FLOOR','CEIL','ROUND']) :
                sql = "select /*+ para_tables_sort() */ ts , min(asct1) from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1 ," % math_fun_1
                sql += "%s as asct2, " % math_fun_2
                sql += "%s, " % random.choice(self.s_r_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts from stable_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ") %s " % random.choice(self.unionall_or_union)
                sql += "select /*+ para_tables_sort() */ ts , max(asct2) from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1 ," % math_fun_1
                sql += "%s as asct2, " % math_fun_2
                sql += "%s, " % random.choice(self.s_r_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts from stable_2 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (mathlist == ['MAVG']) or (mathlist == ['SAMPLE'])or (mathlist == ['TAIL']) or (mathlist == ['CSUM']) or (mathlist == ['HISTOGRAM'])   \
                or (mathlist == ['HYPERLOGLOG']) or (mathlist == ['UNIQUE']) or (mathlist == ['MODE']) or (mathlist == ['statecount','stateduration']) :
                sql = "select /*+ para_tables_sort() */ count(asct1) from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1 " % math_fun_1
                sql += " from stable_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += ") %s " % random.choice(self.unionall_or_union)
                sql += "select /*+ para_tables_sort() */ count(asct2) from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct2 " % math_fun_2
                sql += "from stable_2 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        #3 inter union not support
        tdSql.query("select /*+ para_tables_sort() */1-11 as math_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (mathlist == ['SIN','COS','TAN','ASIN','ACOS','ATAN']) or (mathlist == ['ABS','SQRT'])  \
                or (mathlist == ['POW','LOG']) or (mathlist == ['FLOOR','CEIL','ROUND']) :
                sql = "select /*+ para_tables_sort() */  min(asct1), max(asct2) from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1, ts ," % math_fun_1
                sql += "%s as asct2, " % math_fun_2
                sql += "%s, " % random.choice(self.s_r_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts as t2ts from stable_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += " %s "  % random.choice(self.unionall_or_union)
                sql += " select /*+ para_tables_sort() */ "
                sql += "%s as asct1, ts as t1ts," % math_fun_1
                sql += "%s as asct2, " % math_fun_2
                sql += "%s, " % random.choice(self.s_r_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts as t2ts from stable_2 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (mathlist == ['MAVG']) or (mathlist == ['SAMPLE'])or (mathlist == ['TAIL']) or (mathlist == ['CSUM']) or (mathlist == ['HISTOGRAM'])  \
                or (mathlist == ['HYPERLOGLOG']) or (mathlist == ['UNIQUE']) or (mathlist == ['MODE']) or (mathlist == ['statecount','stateduration']) :
                sql = "select /*+ para_tables_sort() */  count(asct1) from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1 " % math_fun_1
                sql += " from stable_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += " %s "  % random.choice(self.unionall_or_union)
                sql += " select /*+ para_tables_sort() */ "
                sql += "%s as asct2 " % math_fun_2
                sql += " from stable_2 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-12 as math_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (mathlist == ['SIN','COS','TAN','ASIN','ACOS','ATAN']) or (mathlist == ['ABS','SQRT'])  \
                or (mathlist == ['POW','LOG']) or (mathlist == ['FLOOR','CEIL','ROUND']) :
                sql = "select /*+ para_tables_sort() */ ts1,ts2 ,timediff(ts1,ts2), max(asct1) from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
                sql += "%s, " % math_fun_join_1
                sql += "%s as asct1, " % math_fun_join_2
                sql += "t1.%s as asct11, " % random.choice(self.q_select)
                sql += "t1.%s as asct12, " % random.choice(self.q_select)
                sql += "t2.%s as asct21, " % random.choice(self.q_select)
                sql += "t2.%s as asct111, " % random.choice(self.q_select)
                sql += "t2.ts as ts2  from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.t_u_where)
                sql += "and %s " % random.choice(self.t_u_or_where)
                sql += "%s " % random.choice(self.order_u_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ");"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (mathlist == ['MAVG']) or (mathlist == ['SAMPLE'])or (mathlist == ['TAIL']) or (mathlist == ['CSUM']) or (mathlist == ['HISTOGRAM']) \
                or (mathlist == ['HYPERLOGLOG']) or (mathlist == ['UNIQUE']) or (mathlist == ['MODE']) or (mathlist == ['statecount','stateduration']) :
                sql = "select /*+ para_tables_sort() */count(asct1) from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1 " % math_fun_join_2
                sql += " from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.t_u_where)
                sql += "and %s " % random.choice(self.t_u_or_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ");"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-13 as math_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (mathlist == ['SIN','COS','TAN','ASIN','ACOS','ATAN']) or (mathlist == ['ABS','SQRT'])  \
                or (mathlist == ['POW','LOG']) or (mathlist == ['FLOOR','CEIL','ROUND']) :
                sql = "select /*+ para_tables_sort() */ ts ,"
                sql += "%s as asct11, " % math_fun_1
                sql += "%s as asct12, " % random.choice(self.q_select)
                sql += "%s as asct13, " % random.choice(self.q_select)
                sql += "%s  as asct14, " % math_fun_2
                sql += "%s  as asct15 " % random.choice(self.t_select)
                sql += " from  ( select /*+ para_tables_sort() */ * from stable_1 where "
                sql += "%s " % random.choice(self.qt_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                tdSql.checkRows(300)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (mathlist == ['MAVG']) or (mathlist == ['SAMPLE'])or (mathlist == ['TAIL']) or (mathlist == ['CSUM']) or (mathlist == ['HISTOGRAM'])  \
                or (mathlist == ['HYPERLOGLOG']) or (mathlist == ['UNIQUE']) or (mathlist == ['MODE']) or (mathlist == ['statecount','stateduration']) :
                sql = "select /*+ para_tables_sort() */ "
                sql += "%s  " % math_fun_2
                sql += "%s  " % random.choice(self.t_select)
                sql += " from  ( select /*+ para_tables_sort() */ * from stable_1 where "
                sql += "%s " % random.choice(self.qt_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-14 as math_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (mathlist == ['SIN','COS','TAN','ASIN','ACOS','ATAN']) or (mathlist == ['ABS','SQRT'])  \
                or (mathlist == ['POW','LOG']) or (mathlist == ['FLOOR','CEIL','ROUND']) :
                sql = "select /*+ para_tables_sort() */ avg(asct1),count(asct2) from ( select /*+ para_tables_sort() */"
                sql += "%s as asct1, " % math_fun_1
                sql += "%s as asct2" % math_fun_2
                sql += "  from stable_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.partiton_where)
                sql += "%s " % random.choice(self.order_desc_where)
                sql += "%s " % random.choice([self.limit_where[2] , self.limit_where[3]] )
                sql += " ) ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (mathlist == ['MAVG']) or (mathlist == ['SAMPLE'])or (mathlist == ['TAIL']) or (mathlist == ['CSUM']) or (mathlist == ['HISTOGRAM']) \
                or (mathlist == ['HYPERLOGLOG']) or (mathlist == ['UNIQUE']) or (mathlist == ['MODE']) or (mathlist == ['statecount','stateduration']) :
                sql = "select /*+ para_tables_sort() */ count(asct1) from ( select /*+ para_tables_sort() */"
                sql += "%s as asct1 " % math_fun_1
                sql += "  from stable_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.partiton_where)
                sql += "%s " % random.choice([self.limit_where[2] , self.limit_where[3]] )
                sql += " ) ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-15 as math_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (mathlist == ['SIN','COS','TAN','ASIN','ACOS','ATAN']) or (mathlist == ['ABS','SQRT'])  \
                or (mathlist == ['POW','LOG']) or (mathlist == ['FLOOR','CEIL','ROUND']) :
                sql = "select /*+ para_tables_sort() */ ts1,ts ,timediff(ts1,ts), max(asct1) from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
                sql += "%s, " % math_fun_join_1
                sql += "%s as asct1, " % math_fun_join_2
                sql += "t1.%s as asct11, " % random.choice(self.q_select)
                sql += "t1.%s as asct12, " % random.choice(self.q_select)
                sql += "t2.%s as asct13, " % random.choice(self.q_select)
                sql += "t2.%s as asct14, " % random.choice(self.q_select)
                sql += "t2.ts   from stable_1  t1, stable_2 t2 where t1.ts = t2.ts and  "
                sql += "%s " % random.choice(self.t_join_where)
                sql += " and %s " % random.choice(self.qt_u_or_where)
                sql += "%s " % random.choice(self.partiton_where_j)
                sql += "%s " % random.choice(self.slimit1_where)
                sql += ") "
                sql += "%s " % random.choice(self.order_desc_where)
                sql += "%s ;" % random.choice(self.limit_u_where)
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (mathlist == ['MAVG']) or (mathlist == ['SAMPLE']) or (mathlist == ['TAIL']) or (mathlist == ['CSUM']) or (mathlist == ['HISTOGRAM']) \
                or (mathlist == ['HYPERLOGLOG']) or (mathlist == ['UNIQUE']) or (mathlist == ['MODE']) or (mathlist == ['statecount','stateduration']) :
                sql = "select /*+ para_tables_sort() */ count(asct1) from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1 " % math_fun_join_2
                sql += "from stable_1  t1, stable_2 t2 where t1.ts = t2.ts and  "
                sql += "%s " % random.choice(self.t_join_where)
                sql += " and %s " % random.choice(self.qt_u_or_where)
                sql += "%s " % random.choice(self.partiton_where_j)
                sql += "%s " % random.choice(self.slimit1_where)
                sql += ") "
                sql += "%s ;" % random.choice(self.limit_u_where)
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        #taos -f sql
        # startTime_taosf = time.time()
        print("taos -f %s sql start!" %mathlist)
        taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
        #_ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
        _ = subprocess.check_output(taos_cmd1, shell=True)
        print("taos -f %s sql over!" %mathlist)
        # endTime_taosf = time.time()
        # print("taos_f total time %ds" % (endTime_taos_f - startTime_taos_f))

        print("=========%s====over=============" %mathlist)


    def str_nest(self,strlist):

        print("==========%s===start=============" %strlist)
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))

        self.dropandcreateDB_random("%s" %self.db_nest, 1)

        if (strlist == ['LTRIM','RTRIM','LOWER','UPPER']) or (strlist == ['LENGTH','CHAR_LENGTH'])  \
            or (strlist == ['']):
            str_functions = strlist
            fun_fix_column = ['(q_nchar)','(q_binary)','(q_nchar_null)','(q_binary_null)']
            fun_column_1 = random.sample(str_functions,1)+random.sample(fun_fix_column,1)
            str_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            fun_column_2 = random.sample(str_functions,1)+random.sample(fun_fix_column,1)
            str_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","").replace(", ","")

            fun_fix_column_j = ['(t1.q_nchar)','(t1.q_binary)','(t1.q_nchar_null)','(t1.q_binary_null)',
                            '(t2.q_nchar)','(t2.q_binary)','(t2.q_nchar_null)','(t2.q_binary_null)']
            fun_column_join_1 = random.sample(str_functions,1)+random.sample(fun_fix_column_j,1)
            str_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            fun_column_join_2 = random.sample(str_functions,1)+random.sample(fun_fix_column_j,1)
            str_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","").replace(", ","")

            fun_fix_column_s = ['(q_nchar)','(q_binary)','(q_nchar_null)','(q_binary_null)','(loc)','(tbname)']
            fun_column_s_1 = random.sample(str_functions,1)+random.sample(fun_fix_column_s,1)
            str_fun_s_1 = str(fun_column_s_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            fun_column_s_2 = random.sample(str_functions,1)+random.sample(fun_fix_column_s,1)
            str_fun_s_2 = str(fun_column_s_2).replace("[","").replace("]","").replace("'","").replace(", ","")

            fun_fix_column_s_j = ['(t1.q_nchar)','(t1.q_binary)','(t1.q_nchar_null)','(t1.q_binary_null)','(t1.loc)','(t1.tbname)',
                            '(t2.q_nchar)','(t2.q_binary)','(t2.q_nchar_null)','(t2.q_binary_null)','(t2.loc)','(t2.tbname)']
            fun_column_join_s_1 = random.sample(str_functions,1)+random.sample(fun_fix_column_j,1)
            str_fun_join_s_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            fun_column_join_s_2 = random.sample(str_functions,1)+random.sample(fun_fix_column_j,1)
            str_fun_join_s_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","").replace(", ","")

        elif (strlist == ['SUBSTR']) :
            str_functions = strlist
            pos = random.randint(1, 20)
            sub_len = random.randint(1, 10)
            fun_fix_column = ['(q_nchar,pos)','(q_binary,pos)','(q_nchar_null,pos)','(q_binary_null,pos)',
                '(q_nchar,pos,sub_len)','(q_binary,pos,sub_len)','(q_nchar_null,pos,sub_len)','(q_binary_null,pos,sub_len)',]
            fun_column_1 = random.sample(str_functions,1)+random.sample(fun_fix_column,1)
            str_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","").replace(", ","").replace("pos",str(pos)).replace("sub_len",str(sub_len))
            fun_column_2 = random.sample(str_functions,1)+random.sample(fun_fix_column,1)
            str_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","").replace(", ","").replace("pos",str(pos)).replace("sub_len",str(sub_len))

            fun_fix_column_j = ['(t1.q_nchar,pos)','(t1.q_binary,pos)','(t1.q_nchar_null,pos)','(t1.q_binary_null,pos)',
                    '(t1.q_nchar,pos,sub_len)','(t1.q_binary,pos,sub_len)','(t1.q_nchar_null,pos,sub_len)','(t1.q_binary_null,pos,sub_len)',
                    '(t2.q_nchar,pos)','(t2.q_binary,pos)','(t2.q_nchar_null,pos)','(t2.q_binary_null,pos)',
                    '(t2.q_nchar,pos,sub_len)','(t2.q_binary,pos,sub_len)','(t2.q_nchar_null,pos,sub_len)','(t2.q_binary_null,pos,sub_len)']
            fun_column_join_1 = random.sample(str_functions,1)+random.sample(fun_fix_column_j,1)
            str_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","").replace(", ","").replace("pos",str(pos)).replace("sub_len",str(sub_len))
            fun_column_join_2 = random.sample(str_functions,1)+random.sample(fun_fix_column_j,1)
            str_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","").replace(", ","").replace("pos",str(pos)).replace("sub_len",str(sub_len))

            fun_fix_column_s = ['(q_nchar,pos)','(q_binary,pos)','(q_nchar_null,pos)','(q_binary_null,pos)','(loc,pos)',
                '(q_nchar,pos,sub_len)','(q_binary,pos,sub_len)','(q_nchar_null,pos,sub_len)','(q_binary_null,pos,sub_len)','(loc,pos,sub_len)',]
            fun_column_s_1 = random.sample(str_functions,1)+random.sample(fun_fix_column_s,1)
            str_fun_s_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","").replace(", ","").replace("pos",str(pos)).replace("sub_len",str(sub_len))
            fun_column_s_2 = random.sample(str_functions,1)+random.sample(fun_fix_column_s,1)
            str_fun_s_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","").replace(", ","").replace("pos",str(pos)).replace("sub_len",str(sub_len))

            fun_fix_column_s_j = ['(t1.q_nchar,pos)','(t1.q_binary,pos)','(t1.q_nchar_null,pos)','(t1.q_binary_null,pos)','(t1.loc,pos)',
                    '(t1.q_nchar,pos,sub_len)','(t1.q_binary,pos,sub_len)','(t1.q_nchar_null,pos,sub_len)','(t1.q_binary_null,pos,sub_len)','(t1.loc,pos,sub_len)',
                    '(t2.q_nchar,pos)','(t2.q_binary,pos)','(t2.q_nchar_null,pos)','(t2.q_binary_null,pos)','(t2.loc,pos)',
                    '(t2.q_nchar,pos,sub_len)','(t2.q_binary,pos,sub_len)','(t2.q_nchar_null,pos,sub_len)','(t2.q_binary_null,pos,sub_len)','(t2.loc,pos,sub_len)']
            fun_column_join_s_1 = random.sample(str_functions,1)+random.sample(fun_fix_column_s_j,1)
            str_fun_join_s_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","").replace(", ","").replace("pos",str(pos)).replace("sub_len",str(sub_len))
            fun_column_join_s_2 = random.sample(str_functions,1)+random.sample(fun_fix_column_s_j,1)
            str_fun_join_s_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","").replace(", ","").replace("pos",str(pos)).replace("sub_len",str(sub_len))

        elif (strlist == ['CONCAT']) :
            str_functions = strlist
            i = random.randint(2,4)
            fun_fix_column = ['q_nchar','q_nchar1','q_nchar2','q_nchar3','q_nchar4','q_nchar5','q_nchar6','q_nchar7','q_nchar8','q_nchar_null',
                              'q_binary','q_binary1','q_binary2','q_binary3','q_binary4','q_binary5','q_binary6','q_binary7','q_binary8','q_binary_null']

            column1 = str(random.sample(fun_fix_column,i)).replace("[","").replace("]","").replace("'","")
            fun_column_1 = str(random.sample(str_functions,1))+'('+column1+')'
            str_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","")

            column2 = str(random.sample(fun_fix_column,i)).replace("[","").replace("]","").replace("'","")
            fun_column_2 = str(random.sample(str_functions,1))+'('+column2+')'
            str_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","")

            fun_fix_column_j = ['(t1.q_nchar)','(t1.q_nchar1)','(t1.q_nchar2)','(t1.q_nchar3)','(t1.q_nchar4)','(t1.q_nchar5)','(t1.q_nchar6)','(t1.q_nchar7)','(t1.q_nchar8)','(t1.q_nchar_null)',
                    '(t2.q_nchar)','(t2.q_nchar1)','(t2.q_nchar2)','(t2.q_nchar3)','(t2.q_nchar4)','(t2.q_nchar5)','(t2.q_nchar6)','(t2.q_nchar7)','(t2.q_nchar8)','(t2.q_nchar_null)',
                    '(t1.q_binary)','(t1.q_binary1)','(t1.q_binary2)','(t1.q_binary3)','(t1.q_binary4)','(t1.q_binary5)','(t1.q_binary6)','(t1.q_binary7)','(t1.q_binary8)','(t1.q_binary_null)',
                    '(t2.q_binary)','(t2.q_binary1)','(t2.q_binary2)','(t2.q_binary3)','(t2.q_binary4)','(t2.q_binary5)','(t2.q_binary6)','(t2.q_binary7)','(t2.q_binary8)','(t2.q_binary_null)']

            column_j1 = str(random.sample(fun_fix_column_j,i)).replace("[","").replace("]","").replace("'","")
            fun_column_join_1 = str(random.sample(str_functions,1))+'('+column_j1+')'
            str_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","")

            column_j2 = str(random.sample(fun_fix_column_j,i)).replace("[","").replace("]","").replace("'","")
            fun_column_join_2 = str(random.sample(str_functions,1))+'('+column_j2+')'
            str_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","")

            fun_fix_column_s = ['q_nchar','q_nchar1','q_nchar2','q_nchar3','q_nchar4','q_nchar5','q_nchar6','q_nchar7','q_nchar8','loc','q_nchar_null',
                              'q_binary','q_binary1','q_binary2','q_binary3','q_binary4','q_binary5','q_binary6','q_binary7','q_binary8','q_binary_null']

            column_s1 = str(random.sample(fun_fix_column,i)).replace("[","").replace("]","").replace("'","")
            fun_column_s_1 = str(random.sample(str_functions,1))+'('+column_s1+')'
            str_fun_s_1 = str(fun_column_s_1).replace("[","").replace("]","").replace("'","")

            column_s2 = str(random.sample(fun_fix_column,i)).replace("[","").replace("]","").replace("'","")
            fun_column_s_2 = str(random.sample(str_functions,1))+'('+column_s2+')'
            str_fun_s_2 = str(fun_column_s_2).replace("[","").replace("]","").replace("'","")

            fun_fix_column_s_j = ['(t1.q_nchar)','(t1.q_nchar1)','(t1.q_nchar2)','(t1.q_nchar3)','(t1.q_nchar4)','(t1.q_nchar5)','(t1.q_nchar6)','(t1.q_nchar7)','(t1.q_nchar8)','(t1.q_nchar_null)','(t1.loc)',
                    '(t2.q_nchar)','(t2.q_nchar1)','(t2.q_nchar2)','(t2.q_nchar3)','(t2.q_nchar4)','(t2.q_nchar5)','(t2.q_nchar6)','(t2.q_nchar7)','(t2.q_nchar8)','(t2.q_nchar_null)','(t2.loc)',
                    '(t1.q_binary)','(t1.q_binary1)','(t1.q_binary2)','(t1.q_binary3)','(t1.q_binary4)','(t1.q_binary5)','(t1.q_binary6)','(t1.q_binary7)','(t1.q_binary8)','(t1.q_binary_null)',
                    '(t2.q_binary)','(t2.q_binary1)','(t2.q_binary2)','(t2.q_binary3)','(t2.q_binary4)','(t2.q_binary5)','(t2.q_binary6)','(t2.q_binary7)','(t2.q_binary8)','(t2.q_binary_null)']

            column_j_s1 = str(random.sample(fun_fix_column_s_j,i)).replace("[","").replace("]","").replace("'","")
            fun_column_join_s_1 = str(random.sample(str_functions,1))+'('+column_j_s1+')'
            str_fun_join_s_1 = str(fun_column_join_s_1).replace("[","").replace("]","").replace("'","")

            column_j_s2 = str(random.sample(fun_fix_column_s_j,i)).replace("[","").replace("]","").replace("'","")
            fun_column_join_s_2 = str(random.sample(str_functions,1))+'('+column_j_s2+')'
            str_fun_join_s_2 = str(fun_column_join_s_2).replace("[","").replace("]","").replace("'","")

        elif (strlist == ['CONCAT_WS']):
            str_functions = strlist
            i = random.randint(2,4)
            fun_fix_column = ['q_nchar','q_nchar1','q_nchar2','q_nchar3','q_nchar4','q_nchar5','q_nchar6','q_nchar7','q_nchar8','q_nchar_null',
                              'q_binary','q_binary1','q_binary2','q_binary3','q_binary4','q_binary5','q_binary6','q_binary7','q_binary8','q_binary_null']

            separators = ['',' ','abc','123','!','@','#','$','%','^','&','*','(',')','-','_','+','=','{',
                        '[','}',']','|',';',':',',','.','<','>','?','/','~','`','taos','涛思']
            separator = str(random.sample(separators,i)).replace("[","").replace("]","")

            column1 = str(random.sample(fun_fix_column,i)).replace("[","").replace("]","").replace("'","")
            fun_column_1 = str(random.sample(str_functions,1))+'('+'\"'+separator+'\",'+column1+')'
            str_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","")

            column2 = str(random.sample(fun_fix_column,i)).replace("[","").replace("]","").replace("'","")
            fun_column_2 = str(random.sample(str_functions,1))+'('+'\"'+separator+'\",'+column2+')'
            str_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","")

            fun_fix_column_j = ['(t1.q_nchar)','(t1.q_nchar1)','(t1.q_nchar2)','(t1.q_nchar3)','(t1.q_nchar4)','(t1.q_nchar5)','(t1.q_nchar6)','(t1.q_nchar7)','(t1.q_nchar8)','(t1.q_nchar_null)',
                    '(t2.q_nchar)','(t2.q_nchar1)','(t2.q_nchar2)','(t2.q_nchar3)','(t2.q_nchar4)','(t2.q_nchar5)','(t2.q_nchar6)','(t2.q_nchar7)','(t2.q_nchar8)','(t2.q_nchar_null)',
                    '(t1.q_binary)','(t1.q_binary1)','(t1.q_binary2)','(t1.q_binary3)','(t1.q_binary4)','(t1.q_binary5)','(t1.q_binary6)','(t1.q_binary7)','(t1.q_binary8)','(t1.q_binary_null)',
                    '(t2.q_binary)','(t2.q_binary1)','(t2.q_binary2)','(t2.q_binary3)','(t2.q_binary4)','(t2.q_binary5)','(t2.q_binary6)','(t2.q_binary7)','(t2.q_binary8)','(t2.q_binary_null)']

            column_j1 = str(random.sample(fun_fix_column_j,i)).replace("[","").replace("]","").replace("'","")
            fun_column_join_1 = str(random.sample(str_functions,1))+'('+'\"'+separator+'\",'+column_j1+')'
            str_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","")

            column_j2 = str(random.sample(fun_fix_column_j,i)).replace("[","").replace("]","").replace("'","")
            fun_column_join_2 = str(random.sample(str_functions,1))+'('+'\"'+separator+'\",'+column_j2+')'
            str_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","")

            fun_fix_column_s = ['q_nchar','q_nchar1','q_nchar2','q_nchar3','q_nchar4','q_nchar5','q_nchar6','q_nchar7','q_nchar8','loc','q_nchar_null',
                              'q_binary','q_binary1','q_binary2','q_binary3','q_binary4','q_binary5','q_binary6','q_binary7','q_binary8','q_binary_null']

            column_s1 = str(random.sample(fun_fix_column,i)).replace("[","").replace("]","").replace("'","")
            fun_column_s_1 = str(random.sample(str_functions,1))+'('+'\"'+separator+'\",'+column_s1+')'
            str_fun_s_1 = str(fun_column_s_1).replace("[","").replace("]","").replace("'","")

            column_s2 = str(random.sample(fun_fix_column,i)).replace("[","").replace("]","").replace("'","")
            fun_column_s_2 = str(random.sample(str_functions,1))+'('+'\"'+separator+'\",'+column_s2+')'
            str_fun_s_2 = str(fun_column_s_2).replace("[","").replace("]","").replace("'","")

            fun_fix_column_s_j = ['(t1.q_nchar)','(t1.q_nchar1)','(t1.q_nchar2)','(t1.q_nchar3)','(t1.q_nchar4)','(t1.q_nchar5)','(t1.q_nchar6)','(t1.q_nchar7)','(t1.q_nchar8)','(t1.q_nchar_null)','(t1.loc)',
                    '(t2.q_nchar)','(t2.q_nchar1)','(t2.q_nchar2)','(t2.q_nchar3)','(t2.q_nchar4)','(t2.q_nchar5)','(t2.q_nchar6)','(t2.q_nchar7)','(t2.q_nchar8)','(t2.q_nchar_null)','(t2.loc)',
                    '(t1.q_binary)','(t1.q_binary1)','(t1.q_binary2)','(t1.q_binary3)','(t1.q_binary4)','(t1.q_binary5)','(t1.q_binary6)','(t1.q_binary7)','(t1.q_binary8)','(t1.q_binary_null)',
                    '(t2.q_binary)','(t2.q_binary1)','(t2.q_binary2)','(t2.q_binary3)','(t2.q_binary4)','(t2.q_binary5)','(t2.q_binary6)','(t2.q_binary7)','(t2.q_binary8)','(t2.q_binary_null)']

            column_j_s1 = str(random.sample(fun_fix_column_s_j,i)).replace("[","").replace("]","").replace("'","")
            fun_column_join_s_1 = str(random.sample(str_functions,1))+'('+'\"'+separator+'\",'+column_j_s1+')'
            str_fun_join_s_1 = str(fun_column_join_s_1).replace("[","").replace("]","").replace("'","")

            column_j_s2 = str(random.sample(fun_fix_column_s_j,i)).replace("[","").replace("]","").replace("'","")
            fun_column_join_s_2 = str(random.sample(str_functions,1))+'('+'\"'+separator+'\",'+column_j_s2+')'
            str_fun_join_s_2 = str(fun_column_join_s_2).replace("[","").replace("]","").replace("'","")


        tdSql.query("select /*+ para_tables_sort() */1-1 as str_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (strlist == ['LTRIM','RTRIM','LOWER','UPPER']) or (strlist == ['SUBSTR']) or (strlist == ['CONCAT']) or (strlist == ['CONCAT_WS']) :
                sql = "select /*+ para_tables_sort() */ t1s , LTRIM(asct1), LOWER(asct1), RTRIM(asct2), UPPER(asct2)  from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1, " % str_fun_1
                sql += "%s as asct2, " % str_fun_2
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts as t1s from regular_table_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ");"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                tdSql.checkRows(100)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (strlist == ['LENGTH','CHAR_LENGTH']):
                sql = "select /*+ para_tables_sort() */ sum(asct1), min(asct1), max(asct2), avg(asct2)  from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1, " % str_fun_1
                sql += "%s as asct2, " % str_fun_2
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts from regular_table_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ");"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-2 as str_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (strlist == ['LTRIM','RTRIM','LOWER','UPPER']) or (strlist == ['SUBSTR']) or (strlist == ['CONCAT']) or (strlist == ['CONCAT_WS']) :
                sql = "select /*+ para_tables_sort() */ ts ,  asct1  from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1,  " % str_fun_1
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts ts from regular_table_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s )" % random.choice(self.order_where)
                sql += "%s " % random.choice(self.unionall_or_union)
                sql += "select /*+ para_tables_sort() */ ts , asct2 from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct2,  " % str_fun_2
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts ts from regular_table_1 where "
                sql += "%s " % random.choice(self.q_where)
                #sql += "%s " % random.choice(having_support)
                sql += "%s " % random.choice(self.order_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (strlist == ['LENGTH','CHAR_LENGTH']):
                sql = "select /*+ para_tables_sort() */ sum(asct1), min(asct1)  from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1,  " % str_fun_1
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts ts from regular_table_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s )" % random.choice(self.order_where)
                sql += "%s " % random.choice(self.unionall_or_union)
                sql += "select /*+ para_tables_sort() */ sum(asct2), min(asct2) from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct2,  " % str_fun_2
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts ts from regular_table_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-3 as str_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (strlist == ['LTRIM','RTRIM','LOWER','UPPER']) or (strlist == ['SUBSTR']) or (strlist == ['CONCAT']) or (strlist == ['CONCAT_WS']):
                sql = "select /*+ para_tables_sort() */ ts ,  LTRIM(asct1), LOWER(asct1), RTRIM(asct2), UPPER(asct2)  from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1 ," % str_fun_1
                sql += "%s as asct2, " % str_fun_2
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts from regular_table_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s select /*+ para_tables_sort() */" % random.choice(self.unionall_or_union)
                sql += "%s as asct2 ," % str_fun_2
                sql += "%s as asct1, " % str_fun_1
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts from regular_table_2 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                #tdSql.query(sql) #'unexpected end of data'
                # self.cur1.execute(sql)
                # self.explain_sql(sql)
            elif (strlist == ['LENGTH','CHAR_LENGTH']):
                sql = "select /*+ para_tables_sort() */ sum(asct1), min(asct1), max(asct2), avg(asct2)  from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1 ," % str_fun_1
                sql += "%s as asct2, " % str_fun_2
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts from regular_table_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s select /*+ para_tables_sort() */" % random.choice(self.unionall_or_union)
                sql += "%s as asct2 ," % str_fun_2
                sql += "%s as asct1, " % str_fun_1
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts from regular_table_2 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-4 as str_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (strlist == ['LTRIM','RTRIM','LOWER','UPPER']) or (strlist == ['SUBSTR']) or (strlist == ['CONCAT']) or (strlist == ['CONCAT_WS']):
                sql = "select /*+ para_tables_sort() */ ts1,ts2 ,timediff(ts1,ts2),  LTRIM(asct1), LOWER(asct1), RTRIM(asct2), UPPER(asct2)  from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
                sql += "%s as asct2, " % str_fun_join_1
                sql += "%s as asct1, " % str_fun_join_2
                sql += "%s, " % str_fun_join_1
                sql += "t1.%s as asct11, " % random.choice(self.q_select)
                sql += "t2.%s as asct12, " % random.choice(self.q_select)
                sql += "t2.ts as ts2  from regular_table_1 t1 , regular_table_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.q_u_where)
                sql += "and %s " % random.choice(self.q_u_or_where)
                sql += "%s " % random.choice(self.order_u_where)
                sql += ");"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                tdSql.checkRows(100)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (strlist == ['LENGTH','CHAR_LENGTH']):
                sql = "select /*+ para_tables_sort() */ sum(asct1), min(asct1), max(asct2), avg(asct2)  from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
                sql += "%s as asct2, " % str_fun_join_1
                sql += "%s as asct1, " % str_fun_join_2
                sql += "%s, " % str_fun_join_1
                sql += "t1.%s as asct21, " % random.choice(self.q_select)
                sql += "t2.%s as asct22, " % random.choice(self.q_select)
                sql += "t2.ts as ts2  from regular_table_1 t1 , regular_table_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.q_u_where)
                sql += "and %s " % random.choice(self.q_u_or_where)
                sql += "%s " % random.choice(self.order_u_where)
                sql += ");"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-5 as str_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (strlist == ['LTRIM','RTRIM','LOWER','UPPER']) or (strlist == ['SUBSTR']) or (strlist == ['CONCAT']) or (strlist == ['CONCAT_WS']):
                sql = "select /*+ para_tables_sort() */ ts ,"
                sql += "%s, " % str_fun_1
                sql += "%s as asct21, " % random.choice(self.q_select)
                sql += "%s as asct22, " % random.choice(self.q_select)
                sql += "%s  " % str_fun_2
                sql += " from  ( select /*+ para_tables_sort() */ * from regular_table_1 ) where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += " ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                tdSql.checkRows(100)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (strlist == ['LENGTH','CHAR_LENGTH']):
                sql = "select /*+ para_tables_sort() */ ts ,"
                sql += "%s, " % str_fun_1
                sql += "%s as asct22, " % random.choice(self.q_select)
                sql += "%s as asct21, " % random.choice(self.q_select)
                sql += "%s  " % str_fun_2
                sql += " from  ( select /*+ para_tables_sort() */ * from regular_table_1 ) where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += " ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                tdSql.checkRows(100)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-6 as str_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (strlist == ['LTRIM','RTRIM','LOWER','UPPER']) or (strlist == ['SUBSTR']) or (strlist == ['CONCAT']) or (strlist == ['CONCAT_WS']):
                sql = "select /*+ para_tables_sort() */ ts1,ts ,timediff(ts1,ts),  LTRIM(asct1), LOWER(asct1), RTRIM(asct2), UPPER(asct2)  from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
                sql += "%s as asct2, " % str_fun_join_1
                sql += "%s as asct1, " % str_fun_join_2
                sql += "t1.%s as asct22, " % random.choice(self.q_select)
                sql += "t2.%s as asct21, " % random.choice(self.q_select)
                sql += "%s, " % str_fun_join_1
                sql += "t2.ts  from regular_table_1 t1 , regular_table_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.q_u_where)
                sql += "and %s  )" % random.choice(self.q_u_or_where)
                sql += "%s " % random.choice(self.order_where)
                sql += "%s " % random.choice(self.limit1_where)
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (strlist == ['LENGTH','CHAR_LENGTH']):
                sql = "select /*+ para_tables_sort() */ sum(asct1), min(asct1), max(asct2), avg(asct2)  from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
                sql += "%s as asct2, " % str_fun_join_1
                sql += "%s as asct1, " % str_fun_join_2
                sql += "t1.%s as asct22, " % random.choice(self.q_select)
                sql += "t2.%s as asct21, " % random.choice(self.q_select)
                sql += "%s, " % str_fun_join_1
                sql += "t2.ts as ts2  from regular_table_1 t1 , regular_table_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.q_u_where)
                sql += "and %s  )" % random.choice(self.q_u_or_where)
                sql += "%s " % random.choice(self.limit1_where)
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-7 as str_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (strlist == ['LTRIM','RTRIM','LOWER','UPPER']) or (strlist == ['SUBSTR']) or (strlist == ['CONCAT']) or (strlist == ['CONCAT_WS']):
                sql = "select /*+ para_tables_sort() */ t1s ,ts1,  LTRIM(asct1), LOWER(asct1), RTRIM(asct2), UPPER(asct2)  from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1, ts as t1s," % str_fun_s_1
                sql += "%s as asct2, " % str_fun_s_2
                sql += "%s, " % random.choice(self.q_select)
                sql += "%s, " % random.choice(self.t_select)
                sql += "ts as ts1 from stable_1 where "
                sql += "%s " % random.choice(self.qt_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                tdSql.checkRows(300)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (strlist == ['LENGTH','CHAR_LENGTH']):
                sql = "select /*+ para_tables_sort() */ sum(asct1), min(asct1), max(asct2), avg(asct2)  from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1, ts as ts1," % str_fun_s_1
                sql += "%s as asct2, " % str_fun_s_2
                sql += "%s, " % random.choice(self.q_select)
                sql += "%s, " % random.choice(self.t_select)
                sql += "ts as t1s from stable_1 where "
                sql += "%s " % random.choice(self.qt_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-8 as str_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (strlist == ['LTRIM','RTRIM','LOWER','UPPER']) or (strlist == ['SUBSTR']) or (strlist == ['CONCAT']) or (strlist == ['CONCAT_WS']):
                sql = "select /*+ para_tables_sort() */ ts1,st1, LTRIM(asct1), LOWER(asct1), RTRIM(asct2), UPPER(asct2)  "
                sql += "from  ( select /*+ para_tables_sort() */ "
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s as asct1, ts as st1," % str_fun_s_1
                sql += "%s as asct2, " % str_fun_s_2
                sql += "%s, " % random.choice(self.q_select)
                sql += "%s, " % random.choice(self.t_select)
                sql += "ts as ts1 from stable_1 where "
                sql += "%s " % random.choice(self.qt_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                tdSql.checkRows(300)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (strlist == ['LENGTH','CHAR_LENGTH']):
                sql = "select /*+ para_tables_sort() */ sum(asct1), min(asct1), max(asct2), avg(asct2)  "
                sql += "from  ( select /*+ para_tables_sort() */ "
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s as asct1, ts as ts1," % str_fun_s_1
                sql += "%s as asct2, " % str_fun_s_2
                sql += "%s, " % random.choice(self.q_select)
                sql += "%s, " % random.choice(self.t_select)
                sql += "ts as st1 from stable_1 where "
                sql += "%s " % random.choice(self.qt_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-9 as str_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (strlist == ['LTRIM','RTRIM','LOWER','UPPER']) or (strlist == ['SUBSTR']) or (strlist == ['CONCAT']) or (strlist == ['CONCAT_WS']):
                sql = "select /*+ para_tables_sort() */ ts1,ts2 ,timediff(ts1,ts2),  LTRIM(asct1), LOWER(asct1), RTRIM(asct2), UPPER(asct2)  from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
                sql += "%s as asct2, " % str_fun_join_s_1
                sql += "%s as asct1, " % str_fun_join_s_2
                sql += "t1.%s as asct21, " % random.choice(self.q_select)
                sql += "t1.%s as asct22, " % random.choice(self.q_select)
                sql += "t2.%s as asct23, " % random.choice(self.q_select)
                sql += "t2.%s as asct24, " % random.choice(self.q_select)
                sql += "t2.ts as ts2  from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.t_join_where)
                sql += "and %s " % random.choice(self.t_u_where)
                sql += "and %s " % random.choice(self.t_u_or_where)
                sql += "%s " % random.choice(self.order_u_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (strlist == ['LENGTH','CHAR_LENGTH']):
                sql = "select /*+ para_tables_sort() */ sum(asct1), min(asct1), max(asct2), avg(asct2)  from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
                sql += "%s as asct2, " % str_fun_join_s_1
                sql += "%s as asct1, " % str_fun_join_s_2
                sql += "t1.%s as asct21, " % random.choice(self.q_select)
                sql += "t1.%s as asct22, " % random.choice(self.q_select)
                sql += "t2.%s as asct23, " % random.choice(self.q_select)
                sql += "t2.%s as asct24, " % random.choice(self.q_select)
                sql += "t2.ts as ts2  from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.t_join_where)
                sql += "and %s " % random.choice(self.t_u_where)
                sql += "and %s " % random.choice(self.t_u_or_where)
                sql += "%s " % random.choice(self.order_u_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        self.restartDnodes()
        tdSql.query("select /*+ para_tables_sort() */1-10 as str_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (strlist == ['LTRIM','RTRIM','LOWER','UPPER']) or (strlist == ['SUBSTR']) or (strlist == ['CONCAT']) or (strlist == ['CONCAT_WS']):
                sql = "select /*+ para_tables_sort() */ ts ,  LTRIM(asct1), LOWER(asct1), RTRIM(asct2), UPPER(asct2)  from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1 ," % str_fun_s_1
                sql += "%s as asct2, " % str_fun_s_2
                sql += "%s, " % random.choice(self.s_r_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts from stable_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ") %s " % random.choice(self.unionall_or_union)
                sql += "select /*+ para_tables_sort() */ ts ,  LTRIM(asct1), LOWER(asct1), RTRIM(asct2), UPPER(asct2) from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1 ," % str_fun_1
                sql += "%s as asct2, " % str_fun_2
                sql += "%s, " % random.choice(self.s_r_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts from stable_2 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (strlist == ['LENGTH','CHAR_LENGTH']):
                sql = "select /*+ para_tables_sort() */ sum(asct1), min(asct1), max(asct2), avg(asct2)  from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1 ," % str_fun_s_1
                sql += "%s as asct2, " % str_fun_s_2
                sql += "%s, " % random.choice(self.s_r_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts from stable_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ") %s " % random.choice(self.unionall_or_union)
                sql += "select /*+ para_tables_sort() */ sum(asct1), min(asct1), max(asct2), avg(asct2) from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1 ," % str_fun_1
                sql += "%s as asct2, " % str_fun_2
                sql += "%s, " % random.choice(self.s_r_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts from stable_2 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        #3 inter union not support
        tdSql.query("select /*+ para_tables_sort() */1-11 as str_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (strlist == ['LTRIM','RTRIM','LOWER','UPPER']) or (strlist == ['SUBSTR']) or (strlist == ['CONCAT']) or (strlist == ['CONCAT_WS']):
                sql = "select /*+ para_tables_sort() */ ts ,  LTRIM(asct1), LOWER(asct1), RTRIM(asct2), UPPER(asct2)  from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1, ts ," % str_fun_s_1
                sql += "%s as asct2, " % str_fun_s_2
                sql += "%s, " % random.choice(self.s_r_select)
                sql += "%s " % random.choice(self.q_select)
                sql += " from stable_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += " %s "  % random.choice(self.unionall_or_union)
                sql += " select /*+ para_tables_sort() */ "
                sql += "%s as asct1, ts ," % str_fun_1
                sql += "%s as asct2, " % str_fun_2
                sql += "%s, " % random.choice(self.s_r_select)
                sql += "%s " % random.choice(self.q_select)
                sql += " from stable_2 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (strlist == ['LENGTH','CHAR_LENGTH']):
                sql = "select /*+ para_tables_sort() */sum(asct1), min(asct1), max(asct2), avg(asct2)  from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1 ," % str_fun_s_1
                sql += "%s as asct2, " % str_fun_s_2
                sql += "%s, " % random.choice(self.s_r_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts from stable_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += " %s "  % random.choice(self.unionall_or_union)
                sql += " select /*+ para_tables_sort() */ "
                sql += "%s as asct1 ," % str_fun_1
                sql += "%s as asct2, " % str_fun_2
                sql += "%s, " % random.choice(self.s_r_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts from stable_2 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-12 as str_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (strlist == ['LTRIM','RTRIM','LOWER','UPPER']) or (strlist == ['SUBSTR']) or (strlist == ['CONCAT']) or (strlist == ['CONCAT_WS']):
                sql = "select /*+ para_tables_sort() */ ts1,ts2 ,timediff(ts1,ts2),  LTRIM(asct1), LOWER(asct1), RTRIM(asct2), UPPER(asct2)  from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
                sql += "%s as asct2, " % str_fun_join_s_1
                sql += "%s as asct1, " % str_fun_join_s_2
                sql += "t1.%s as asct21, " % random.choice(self.q_select)
                sql += "t1.%s as asct22, " % random.choice(self.q_select)
                sql += "t2.%s as asct23, " % random.choice(self.q_select)
                sql += "t2.%s as asct24, " % random.choice(self.q_select)
                sql += "t2.ts as ts2  from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.t_u_where)
                sql += "and %s " % random.choice(self.t_u_or_where)
                sql += "%s " % random.choice(self.order_u_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ");"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (strlist == ['LENGTH','CHAR_LENGTH']):
                sql = "select /*+ para_tables_sort() */ sum(asct1), min(asct1), max(asct2), avg(asct2)  from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
                sql += "%s as asct2, " % str_fun_join_s_1
                sql += "%s as asct1, " % str_fun_join_s_2
                sql += "t1.%s as asct21, " % random.choice(self.q_select)
                sql += "t1.%s as asct22, " % random.choice(self.q_select)
                sql += "t2.%s as asct23, " % random.choice(self.q_select)
                sql += "t2.%s as asct24, " % random.choice(self.q_select)
                sql += "t2.ts as ts2  from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.t_u_where)
                sql += "and %s " % random.choice(self.t_u_or_where)
                sql += "%s " % random.choice(self.order_u_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ");"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-13 as str_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (strlist == ['LTRIM','RTRIM','LOWER','UPPER']) or (strlist == ['SUBSTR']) or (strlist == ['CONCAT']) or (strlist == ['CONCAT_WS']):
                sql = "select /*+ para_tables_sort() */ ts ,"
                sql += "%s as asct10, " % str_fun_1
                sql += "%s as asct1, " % random.choice(self.q_select)
                sql += "%s as asct12, " % random.choice(self.q_select)
                sql += "%s as asct13, " % str_fun_2
                sql += "%s  as asct14 " % random.choice(self.t_select)
                sql += " from  ( select /*+ para_tables_sort() */ * from stable_1 where "
                sql += "%s " % random.choice(self.qt_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                tdSql.checkRows(300)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (strlist == ['LENGTH','CHAR_LENGTH']):
                sql = "select /*+ para_tables_sort() */ ts ,"
                sql += "%s as asct1, " % str_fun_1
                sql += "%s as asct11, " % random.choice(self.q_select)
                sql += "%s as asct12, " % random.choice(self.q_select)
                sql += "%s as asct13, " % str_fun_2
                sql += "%s as asct14  " % random.choice(self.t_select)
                sql += " from  ( select /*+ para_tables_sort() */ * from stable_1 where "
                sql += "%s " % random.choice(self.qt_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                tdSql.checkRows(300)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-14 as str_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (strlist == ['LTRIM','RTRIM','LOWER','UPPER']) or (strlist == ['SUBSTR']) or (strlist == ['CONCAT']) or (strlist == ['CONCAT_WS']):
                sql = "select /*+ para_tables_sort() */  LTRIM(asct1), LOWER(asct1), RTRIM(asct2), UPPER(asct2)  from ( select /*+ para_tables_sort() */"
                sql += "%s as asct1, " % str_fun_s_1
                sql += "%s as asct2" % str_fun_s_2
                sql += "  from stable_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.partiton_where)
                sql += "%s " % random.choice(self.order_desc_where)
                sql += "%s " % random.choice([self.limit_where[2] , self.limit_where[3]] )
                sql += " ) ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (strlist == ['LENGTH','CHAR_LENGTH']):
                sql = "select /*+ para_tables_sort() */  sum(asct1), min(asct1), max(asct2), avg(asct2)  from ( select /*+ para_tables_sort() */"
                sql += "%s as asct1, " % str_fun_s_1
                sql += "%s as asct2" % str_fun_s_2
                sql += "  from stable_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.partiton_where)
                sql += "%s " % random.choice(self.order_desc_where)
                sql += "%s " % random.choice([self.limit_where[2] , self.limit_where[3]] )
                sql += " ) ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-15 as str_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (strlist == ['LTRIM','RTRIM','LOWER','UPPER']) or (strlist == ['SUBSTR']) or (strlist == ['CONCAT']) or (strlist == ['CONCAT_WS']):
                sql = "select /*+ para_tables_sort() */ ts,ts2 ,timediff(ts,ts2), LTRIM(asct1), LOWER(asct1), RTRIM(asct2), UPPER(asct2)  from  ( select /*+ para_tables_sort() */ t1.ts ,"
                sql += "%s as asct2, " % str_fun_join_s_1
                sql += "%s as asct1, " % str_fun_join_s_2
                sql += "t1.%s as asct11, " % random.choice(self.q_select)
                sql += "t1.%s as asct12, " % random.choice(self.q_select)
                sql += "t2.%s as asct13, " % random.choice(self.q_select)
                sql += "t2.%s as asct14, " % random.choice(self.q_select)
                sql += "t2.ts as ts2 from stable_1  t1, stable_2 t2 where t1.ts = t2.ts and  "
                sql += "%s " % random.choice(self.t_join_where)
                sql += " and %s " % random.choice(self.qt_u_or_where)
                sql += "%s " % random.choice(self.partiton_where_j)
                sql += "%s " % random.choice(self.slimit1_where)
                sql += ") "
                sql += "%s " % random.choice(self.order_desc_where)
                sql += "%s ;" % random.choice(self.limit_u_where)
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (strlist == ['LENGTH','CHAR_LENGTH']):
                sql = "select /*+ para_tables_sort() */ sum(asct1), min(asct1), max(asct2), avg(asct2)  from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
                sql += "%s as asct2, " % str_fun_join_s_1
                sql += "%s as asct1, " % str_fun_join_s_2
                sql += "t1.%s as asct11, " % random.choice(self.q_select)
                sql += "t1.%s as asct12, " % random.choice(self.q_select)
                sql += "t2.%s as asct13, " % random.choice(self.q_select)
                sql += "t2.%s as asct14 " % random.choice(self.q_select)
                sql += "from stable_1  t1, stable_2 t2 where t1.ts = t2.ts and  "
                sql += "%s " % random.choice(self.t_join_where)
                sql += " and %s " % random.choice(self.qt_u_or_where)
                sql += "%s " % random.choice(self.partiton_where_j)
                sql += "%s " % random.choice(self.slimit1_where)
                sql += ") "
                sql += "%s ;" % random.choice(self.limit_u_where)
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        #taos -f sql
        startTime_taos_f = time.time()
        print("taos -f %s sql start!" %strlist)
        taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
        #_ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
        _ = subprocess.check_output(taos_cmd1, shell=True)
        print("taos -f %s sql over!" %strlist)
        endTime_taos_f = time.time()
        print("taos_f total time %ds" % (endTime_taos_f - startTime_taos_f))

        print("=========%s====over=============" %strlist)

    def time_nest(self,timelist):

        print("==========%s===start=============" %timelist)
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))

        self.dropandcreateDB_random("%s" %self.db_nest, 1)

        if (timelist == ['NOW','TODAY']) or (timelist == ['TIMEZONE']):
            time_functions = timelist
            fun_fix_column = ['()']
            fun_column_1 = random.sample(time_functions,1)+random.sample(fun_fix_column,1)
            time_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            fun_column_2 = random.sample(time_functions,1)+random.sample(fun_fix_column,1)
            time_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","").replace(", ","")

            fun_fix_column_j = ['()']
            fun_column_join_1 = random.sample(time_functions,1)+random.sample(fun_fix_column_j,1)
            time_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            fun_column_join_2 = random.sample(time_functions,1)+random.sample(fun_fix_column_j,1)
            time_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","").replace(", ","")

        elif (timelist == ['TIMETRUNCATE']):
            time_functions = timelist

            t = time.time()
            t_to_s =  time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t))
            fun_fix_column = ['q_ts','ts','_c0','_C0','_rowts','1600000000000','1600000000000000','1600000000000000000',
                        '%d' %t, '%d000' %t, '%d000000' %t,'t_to_s']

            timeunits = ['1a' ,'1s', '1m' ,'1h', '1d']
            timeunit = str(random.sample(timeunits,1)).replace("[","").replace("]","").replace("'","")

            column_1 = ['(%s,timeutil)'%(random.sample(fun_fix_column,1))]
            fun_column_1 = random.sample(time_functions,1)+random.sample(column_1,1)
            time_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","").replace(", ","").replace("\"","").replace("t_to_s","'t_to_s'")
            time_fun_1 = str(time_fun_1).replace("timeutil","%s" %timeunit).replace("t_to_s","%s" %t_to_s)

            column_2 = ['(%s,timeutil)'%(random.sample(fun_fix_column,1))]
            fun_column_2 = random.sample(time_functions,1)+random.sample(column_2,1)
            time_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","").replace(", ","").replace("\"","").replace("t_to_s","'t_to_s'")
            time_fun_2 = str(time_fun_2).replace("timeutil","%s" %timeunit).replace("t_to_s","%s" %t_to_s)


            fun_fix_column_j = ['(t1.q_ts)','(t1.ts)', '(t2.q_ts)','(t2.ts)','(1600000000000)','(1600000000000000)','(1600000000000000000)',
                        '(%d)' %t, '(%d000)' %t, '(%d000000)' %t,'t_to_s']

            column_j1 = ['(%s,timeutil)'%(random.sample(fun_fix_column_j,1))]
            fun_column_join_1 = random.sample(time_functions,1)+random.sample(column_j1,1)
            time_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","").replace(", ","").replace("\"","").replace("t_to_s","'t_to_s'")
            time_fun_join_1 = str(time_fun_join_1).replace("timeutil","%s" %timeunit).replace("t_to_s","%s" %t_to_s)

            column_j2 = ['(%s,timeutil)'%(random.sample(fun_fix_column_j,1))]
            fun_column_join_2 = random.sample(time_functions,1)+random.sample(column_j2,1)
            time_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","").replace(", ","").replace("\"","").replace("t_to_s","'t_to_s'")
            time_fun_join_2 = str(time_fun_join_2).replace("timeutil","%s" %timeunit).replace("t_to_s","%s" %t_to_s)

        elif (timelist == ['TO_ISO8601']):
            time_functions = timelist

            t = time.time()
            fun_fix_column = ['(now())','(ts)','(q_ts)','(_rowts)','(_c0)','(_C0)',
                              '(1600000000000)','(1600000000000000)','(1600000000000000000)',
                              '(%d)' %t, '(%d000)' %t, '(%d000000)' %t]

            fun_column_1 = random.sample(time_functions,1)+random.sample(fun_fix_column,1)
            time_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","").replace(", ","")

            fun_column_2 = random.sample(time_functions,1)+random.sample(fun_fix_column,1)
            time_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","").replace(", ","")

            fun_fix_column_j = ['(t1.q_ts)','(t1.ts)', '(t2.q_ts)','(t2.ts)','(1600000000000)','(1600000000000000)','(1600000000000000000)','(now())',
                                '(%d)' %t, '(%d000)' %t, '(%d000000)' %t]

            fun_column_join_1 = random.sample(time_functions,1)+random.sample(fun_fix_column_j,1)
            time_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","").replace(", ","")

            fun_column_join_2 = random.sample(time_functions,1)+random.sample(fun_fix_column_j,1)
            time_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","").replace(", ","")

        elif (timelist == ['TO_UNIXTIMESTAMP']):
            time_functions = timelist

            t = time.time()
            t_to_s =  time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t))
            fun_fix_column = ['(q_nchar)','(q_nchar1)','(q_nchar2)','(q_nchar3)','(q_nchar4)','(q_nchar_null)','(q_binary)','(q_binary5)','(q_binary6)','(q_binary7)','(q_binary8)','(q_binary_null)','(t_to_s)']

            fun_column_1 = random.sample(time_functions,1)+random.sample(fun_fix_column,1)
            time_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","").replace(", ","").replace("t_to_s","'t_to_s'")
            time_fun_1 = str(time_fun_1).replace("t_to_s","%s" %t_to_s)

            fun_column_2 = random.sample(time_functions,1)+random.sample(fun_fix_column,1)
            time_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","").replace(", ","").replace("t_to_s","'t_to_s'")
            time_fun_2 = str(time_fun_2).replace("t_to_s","%s" %t_to_s)

            fun_fix_column_j = ['(t1.q_nchar)','(t1.q_binary)', '(t2.q_nchar)','(t2.q_binary)','(t_to_s)']

            fun_column_join_1 = random.sample(time_functions,1)+random.sample(fun_fix_column_j,1)
            time_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","").replace(", ","").replace("t_to_s","'t_to_s'")
            time_fun_join_1 = str(time_fun_join_1).replace("t_to_s","%s" %t_to_s)

            fun_column_join_2 = random.sample(time_functions,1)+random.sample(fun_fix_column_j,1)
            time_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","").replace(", ","").replace("t_to_s","'t_to_s'")
            time_fun_join_2 = str(time_fun_join_2).replace("t_to_s","%s" %t_to_s)

        elif (timelist == ['TIMEDIFF_1']):
            time_functions = timelist

            t = time.time()
            t_to_s =  time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t))
            timeunits = [ '1a' ,'1s', '1m' ,'1h', '1d']
            timeunit = str(random.sample(timeunits,1)).replace("[","").replace("]","").replace("'","")

            fun_fix_column = ['q_ts','ts','_c0','_C0','_rowts','1600000000000','1600000000000000','1600000000000000000',
                              '%d' %t, '%d000' %t, '%d000000' %t,'t_to_s']

            column_1,column_2 = random.sample(fun_fix_column,1),random.sample(fun_fix_column,1)
            column_12 = ['(%s,%s,timeutil)'%(column_1,column_2)]
            fun_column_1 = random.sample(time_functions,1)+random.sample(column_12,1)
            time_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","").replace(", ","").replace("\"","").replace("t_to_s","'t_to_s'")
            time_fun_1 = str(time_fun_1).replace("timeutil","%s" %timeunit).replace("t_to_s","%s" %t_to_s).replace("_1","")

            column_3,column_4 = random.sample(fun_fix_column,1),random.sample(fun_fix_column,1)
            column_34 = ['(%s,%s,timeutil)'%(column_3,column_4)]
            fun_column_2 = random.sample(time_functions,1)+random.sample(column_34,1)
            time_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","").replace(", ","").replace("\"","").replace("t_to_s","'t_to_s'")
            time_fun_2 = str(time_fun_2).replace("timeutil","%s" %timeunit).replace("t_to_s","%s" %t_to_s).replace("_1","")

            fun_fix_column_j = ['(t1.q_ts)','(t1.ts)', '(t2.q_ts)','(t2.ts)','1600000000000','1600000000000000','1600000000000000000',
                              '%d' %t, '%d000' %t, '%d000000' %t,'t_to_s']

            column_j1,column_j2 = random.sample(fun_fix_column_j,1),random.sample(fun_fix_column_j,1)
            column_j12 = ['(%s,%s,timeutil)'%(column_j1,column_j2)]
            fun_column_join_1 = random.sample(time_functions,1)+random.sample(column_j12,1)
            time_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","").replace(", ","").replace("\"","").replace("t_to_s","'t_to_s'")
            time_fun_join_1 = str(time_fun_join_1).replace("timeutil","%s" %timeunit).replace("t_to_s","%s" %t_to_s).replace("_1","")

            column_j3,column_j4 = random.sample(fun_fix_column_j,1),random.sample(fun_fix_column_j,1)
            column_j34 = ['(%s,%s,timeutil)'%(column_j3,column_j4)]
            fun_column_join_2 = random.sample(time_functions,1)+random.sample(column_j34,1)
            time_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","").replace(", ","").replace("\"","").replace("t_to_s","'t_to_s'")
            time_fun_join_2 = str(time_fun_join_2).replace("timeutil","%s" %timeunit).replace("t_to_s","%s" %t_to_s).replace("_1","")

        elif (timelist == ['TIMEDIFF_2']):
            time_functions = timelist

            t = time.time()
            t_to_s =  time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t))

            fun_fix_column = ['q_ts','ts','_c0','_C0','_rowts','1600000000000','1600000000000000','1600000000000000000',
                              '%d' %t, '%d000' %t, '%d000000' %t,'t_to_s']

            column_1,column_2 = random.sample(fun_fix_column,1),random.sample(fun_fix_column,1)
            column_12 = ['(%s,%s)'%(column_1,column_2)]
            fun_column_1 = random.sample(time_functions,1)+random.sample(column_12,1)
            time_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","").replace(", ","").replace("\"","").replace("t_to_s","'t_to_s'")
            time_fun_1 = str(time_fun_1).replace("t_to_s","%s" %t_to_s).replace("_2","")

            column_3,column_4 = random.sample(fun_fix_column,1),random.sample(fun_fix_column,1)
            column_34 = ['(%s,%s)'%(column_3,column_4)]
            fun_column_2 = random.sample(time_functions,1)+random.sample(column_34,1)
            time_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","").replace(", ","").replace("\"","").replace("t_to_s","'t_to_s'")
            time_fun_2 = str(time_fun_2).replace("t_to_s","%s" %t_to_s).replace("_2","")

            fun_fix_column_j = ['(t1.q_ts)','(t1.ts)', '(t2.q_ts)','(t2.ts)','1600000000000','1600000000000000','1600000000000000000',
                              '%d' %t, '%d000' %t, '%d000000' %t,'t_to_s']

            column_j1,column_j2 = random.sample(fun_fix_column_j,1),random.sample(fun_fix_column_j,1)
            column_j12 = ['(%s,%s)'%(column_j1,column_j2)]
            fun_column_join_1 = random.sample(time_functions,1)+random.sample(column_j12,1)
            time_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","").replace(", ","").replace("\"","").replace("t_to_s","'t_to_s'")
            time_fun_join_1 = str(time_fun_join_1).replace("t_to_s","%s" %t_to_s).replace("_2","")

            column_j3,column_j4 = random.sample(fun_fix_column_j,1),random.sample(fun_fix_column_j,1)
            column_j34 = ['(%s,%s)'%(column_j3,column_j4)]
            fun_column_join_2 = random.sample(time_functions,1)+random.sample(column_j34,1)
            time_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","").replace(", ","").replace("\"","").replace("t_to_s","'t_to_s'")
            time_fun_join_2 = str(time_fun_join_2).replace("t_to_s","%s" %t_to_s).replace("_2","")

        elif (timelist == ['ELAPSED']):
            time_functions = timelist

            fun_fix_column = ['(ts)','(_c0)','(_C0)','(_rowts)','(ts,time_unit)','(_c0,time_unit)','(_C0,time_unit)','(_rowts,time_unit)']

            time_units = ['1s','1m','1h','1d','1a']
            time_unit1 = str(random.sample(time_units,1)).replace("[","").replace("]","").replace("'","")
            time_unit2 = str(random.sample(time_units,1)).replace("[","").replace("]","").replace("'","")

            fun_column_1 = random.sample(time_functions,1)+random.sample(fun_fix_column,1)
            time_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","").replace(", ","").replace("time_unit","%s" %time_unit1)

            fun_column_2 = random.sample(time_functions,1)+random.sample(fun_fix_column,1)
            time_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","").replace(", ","").replace("time_unit","%s" %time_unit2)


            fun_fix_column_j = ['(t1.ts)', '(t2.ts)','(t1.ts,time_unit)','(t1.ts,time_unit)','(t2.ts,time_unit)','(t2.ts,time_unit)']

            fun_column_join_1 = random.sample(time_functions,1)+random.sample(fun_fix_column_j,1)
            time_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","").replace(", ","").replace("time_unit","%s" %time_unit1)

            fun_column_join_2 = random.sample(time_functions,1)+random.sample(fun_fix_column_j,1)
            time_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","").replace(", ","").replace("time_unit","%s" %time_unit2)


        elif (timelist == ['CAST']) :
            str_functions = timelist
            #下面的4个是全的，这个只是1个
            i = random.randint(1,4)
            if i ==1:
                print('===========cast_1===========')
                fun_fix_column = ['q_bool','q_bool_null','q_bigint','q_bigint_null','q_smallint','q_smallint_null',
                'q_tinyint','q_tinyint_null','q_int','q_int_null','q_float','q_float_null','q_double','q_double_null']
                type_names = ['BIGINT','BINARY(100)','TIMESTAMP','NCHAR(100)','BIGINT UNSIGNED']

                type_name1 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
                fun_column_1 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column,1))+' AS '+type_name1+')'
                time_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","")

                type_name2 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
                fun_column_2 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column,1))+' AS '+type_name2+')'
                time_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","")

                fun_fix_column_j = ['t1.q_bool','t1.q_bool_null','t1.q_bigint','t1.q_bigint_null','t1.q_smallint','t1.q_smallint_null',
                't1.q_tinyint','t1.q_tinyint_null','t1.q_int','t1.q_int_null','t1.q_float','t1.q_float_null','t1.q_double','t1.q_double_null',
                't2.q_bool','t2.q_bool_null','t2.q_bigint','t2.q_bigint_null','t2.q_smallint','t2.q_smallint_null',
                't2.q_tinyint','t2.q_tinyint_null','t2.q_int','t2.q_int_null','t2.q_float','t2.q_float_null','t2.q_double','t2.q_double_null']

                type_name_j1 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
                fun_column_join_1 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column_j,1))+' AS '+type_name_j1+')'
                time_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","")

                type_name_j2 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
                fun_column_join_2 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column_j,1))+' AS '+type_name_j2+')'
                time_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","")

            elif i==2:
                print('===========cast_2===========')
                fun_fix_column = ['q_binary','q_binary_null','q_binary1','q_binary2','q_binary3','q_binary4']
                type_names = ['BIGINT','BINARY(100)','NCHAR(100)','BIGINT UNSIGNED']

                type_name1 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
                fun_column_1 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column,1))+' AS '+type_name1+')'
                time_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","")

                type_name2 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
                fun_column_2 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column,1))+' AS '+type_name2+')'
                time_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","")

                fun_fix_column_j = ['t1.q_binary','t1.q_binary_null','t1.q_binary1','t1.q_binary2','t1.q_binary3','t1.q_binary4',
                        't2.q_binary','t2.q_binary_null','t2.q_binary1','t2.q_binary2','t2.q_binary3','t2.q_binary4']

                type_name_j1 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
                fun_column_join_1 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column_j,1))+' AS '+type_name_j1+')'
                time_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","")

                type_name_j2 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
                fun_column_join_2 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column_j,1))+' AS '+type_name_j2+')'
                time_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","")

            elif i==3:
                print('===========cast_3===========')
                fun_fix_column = ['q_nchar','q_nchar_null','q_nchar5','q_nchar6','q_nchar7','q_nchar8']
                type_names = ['BIGINT','NCHAR(100)','BIGINT UNSIGNED']

                type_name1 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
                fun_column_1 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column,1))+' AS '+type_name1+')'
                time_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","")

                type_name2 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
                fun_column_2 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column,1))+' AS '+type_name2+')'
                time_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","")

                fun_fix_column_j = ['t1.q_nchar','t1.q_nchar_null','t1.q_nchar5','t1.q_nchar6','t1.q_nchar7','t1.q_nchar8',
                        't2.q_nchar','t2.q_nchar_null','t2.q_nchar5','t2.q_nchar6','t2.q_nchar7','t2.q_nchar8']

                type_name_j1 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
                fun_column_join_1 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column_j,1))+' AS '+type_name_j1+')'
                time_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","")

                type_name_j2 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
                fun_column_join_2 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column_j,1))+' AS '+type_name_j2+')'
                time_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","")

            elif i==4:
                print('===========cast_4===========')
                fun_fix_column = ['q_ts','q_ts_null','_C0','_c0','ts','_rowts']
                type_names = ['BIGINT','TIMESTAMP','BIGINT UNSIGNED']

                type_name1 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
                fun_column_1 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column,1))+' AS '+type_name1+')'
                time_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","")

                type_name2 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
                fun_column_2 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column,1))+' AS '+type_name2+')'
                time_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","")

                fun_fix_column_j = ['t1.q_ts','t1.q_ts_null','t1.ts','t2.q_ts','t2.q_ts_null','t2.ts']

                type_name_j1 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
                fun_column_join_1 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column_j,1))+' AS '+type_name_j1+')'
                time_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","")

                type_name_j2 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
                fun_column_join_2 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column_j,1))+' AS '+type_name_j2+')'
                time_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","")

        elif (timelist == ['CAST_1']) :
            str_functions = timelist

            print('===========cast_1===========')
            fun_fix_column = ['q_bool','q_bool_null','q_bigint','q_bigint_null','q_smallint','q_smallint_null',
            'q_tinyint','q_tinyint_null','q_int','q_int_null','q_float','q_float_null','q_double','q_double_null']
            type_names = ['BIGINT','BINARY(100)','TIMESTAMP','NCHAR(100)','BIGINT UNSIGNED']

            type_name1 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
            fun_column_1 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column,1))+' AS '+type_name1+')'
            time_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","").replace("_1","")

            type_name2 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
            fun_column_2 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column,1))+' AS '+type_name2+')'
            time_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","").replace("_1","")

            fun_fix_column_j = ['t1.q_bool','t1.q_bool_null','t1.q_bigint','t1.q_bigint_null','t1.q_smallint','t1.q_smallint_null',
            't1.q_tinyint','t1.q_tinyint_null','t1.q_int','t1.q_int_null','t1.q_float','t1.q_float_null','t1.q_double','t1.q_double_null',
            't2.q_bool','t2.q_bool_null','t2.q_bigint','t2.q_bigint_null','t2.q_smallint','t2.q_smallint_null',
            't2.q_tinyint','t2.q_tinyint_null','t2.q_int','t2.q_int_null','t2.q_float','t2.q_float_null','t2.q_double','t2.q_double_null']

            type_name_j1 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
            fun_column_join_1 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column_j,1))+' AS '+type_name_j1+')'
            time_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","").replace("_1","")

            type_name_j2 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
            fun_column_join_2 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column_j,1))+' AS '+type_name_j2+')'
            time_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","").replace("_1","")

        elif (timelist == ['CAST_2']) :
            str_functions = timelist
            print('===========cast_2===========')
            fun_fix_column = ['q_binary','q_binary_null','q_binary1','q_binary2','q_binary3','q_binary4']
            type_names = ['BIGINT','BINARY(100)','NCHAR(100)','BIGINT UNSIGNED']

            type_name1 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
            fun_column_1 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column,1))+' AS '+type_name1+')'
            time_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","").replace("_2","")

            type_name2 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
            fun_column_2 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column,1))+' AS '+type_name2+')'
            time_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","").replace("_2","")

            fun_fix_column_j = ['t1.q_binary','t1.q_binary_null','t1.q_binary1','t1.q_binary2','t1.q_binary3','t1.q_binary4',
                    't2.q_binary','t2.q_binary_null','t2.q_binary1','t2.q_binary2','t2.q_binary3','t2.q_binary4']

            type_name_j1 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
            fun_column_join_1 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column_j,1))+' AS '+type_name_j1+')'
            time_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","").replace("_2","")

            type_name_j2 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
            fun_column_join_2 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column_j,1))+' AS '+type_name_j2+')'
            time_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","").replace("_2","")

        elif (timelist == ['CAST_3']) :
            str_functions = timelist
            print('===========cast_3===========')
            fun_fix_column = ['q_nchar','q_nchar_null','q_nchar5','q_nchar6','q_nchar7','q_nchar8']
            type_names = ['BIGINT','NCHAR(100)','BIGINT UNSIGNED']

            type_name1 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
            fun_column_1 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column,1))+' AS '+type_name1+')'
            time_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","").replace("_3","")

            type_name2 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
            fun_column_2 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column,1))+' AS '+type_name2+')'
            time_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","").replace("_3","")

            fun_fix_column_j = ['t1.q_nchar','t1.q_nchar_null','t1.q_nchar5','t1.q_nchar6','t1.q_nchar7','t1.q_nchar8',
                    't2.q_nchar','t2.q_nchar_null','t2.q_nchar5','t2.q_nchar6','t2.q_nchar7','t2.q_nchar8']

            type_name_j1 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
            fun_column_join_1 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column_j,1))+' AS '+type_name_j1+')'
            time_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","").replace("_3","")

            type_name_j2 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
            fun_column_join_2 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column_j,1))+' AS '+type_name_j2+')'
            time_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","").replace("_3","")

        elif (timelist == ['CAST_4']) :
            str_functions = timelist
            print('===========cast_4===========')
            fun_fix_column = ['q_ts','q_ts_null','_C0','_c0','ts','_rowts']
            type_names = ['BIGINT','TIMESTAMP','BIGINT UNSIGNED']

            type_name1 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
            fun_column_1 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column,1))+' AS '+type_name1+')'
            time_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","").replace("_4","")

            type_name2 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
            fun_column_2 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column,1))+' AS '+type_name2+')'
            time_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","").replace("_4","")

            fun_fix_column_j = ['t1.q_ts','t1.q_ts_null','t1.ts','t2.q_ts','t2.q_ts_null','t2.ts']

            type_name_j1 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
            fun_column_join_1 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column_j,1))+' AS '+type_name_j1+')'
            time_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","").replace("_4","")

            type_name_j2 = str(random.sample(type_names,1)).replace("[","").replace("]","").replace("'","")
            fun_column_join_2 = str(random.sample(str_functions,1))+'('+str(random.sample(fun_fix_column_j,1))+' AS '+type_name_j2+')'
            time_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","").replace("_4","")

        tdSql.query("select /*+ para_tables_sort() */1-1 as time_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (timelist == ['NOW','TODAY']) or (timelist == ['TIMETRUNCATE']) or (timelist == ['TO_ISO8601'])\
                or (timelist == ['TO_UNIXTIMESTAMP']) or (timelist == ['TIMEDIFF_1']) or (timelist == ['TIMEDIFF_2']):
                sql = "select /*+ para_tables_sort() */ ts1 , timediff(asct1,now)  from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1, " % time_fun_1
                sql += "%s as asct2, " % time_fun_2
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts as ts1 from regular_table_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ");"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                tdSql.checkRows(100)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['TIMEZONE']) \
                or (timelist == ['CAST']) or (timelist == ['CAST_1']) or (timelist == ['CAST_2']) or (timelist == ['CAST_3']) or (timelist == ['CAST_4']):
                sql = "select /*+ para_tables_sort() */ ts2 , asct1,now(),today(),timezone()  from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1, " % time_fun_1
                sql += "%s as asct2, " % time_fun_2
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts as ts2 from regular_table_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ");"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                tdSql.checkRows(100)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['ELAPSED']) :
                sql = "select /*+ para_tables_sort() */ max(asct1),now(),today(),timezone()  from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1, " % time_fun_1
                sql += "%s as asct2 " % time_fun_2
                sql += "from regular_table_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += ");"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-2 as time_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (timelist == ['NOW','TODAY']) or (timelist == ['TIMETRUNCATE']) or (timelist == ['TO_ISO8601'])\
                or (timelist == ['TO_UNIXTIMESTAMP']) or (timelist == ['TIMEDIFF_1']) or (timelist == ['TIMEDIFF_2']):
                sql = "select /*+ para_tables_sort() */ ts , timediff(asct1,now),now(),today(),timezone()  from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1,  " % time_fun_1
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts ts from regular_table_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s )" % random.choice(self.order_where)
                sql += "%s " % random.choice(self.unionall_or_union)
                sql += "select /*+ para_tables_sort() */ ts ,  timediff(asct2,now),now(),today(),timezone() from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct2,  " % time_fun_2
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts ts from regular_table_1 where "
                sql += "%s " % random.choice(self.q_where)
                #sql += "%s " % random.choice(having_support)
                sql += "%s " % random.choice(self.order_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['TIMEZONE']) or (timelist == ['CAST']) or (timelist == ['CAST_1']) or (timelist == ['CAST_2']) or (timelist == ['CAST_3']) or (timelist == ['CAST_4']):
                sql = "select /*+ para_tables_sort() */ ts , (asct1),now(),today(),timezone()  from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1,  " % time_fun_1
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts ts from regular_table_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s )" % random.choice(self.order_where)
                sql += "%s " % random.choice(self.unionall_or_union)
                sql += "select /*+ para_tables_sort() */ ts , asct2,now(),today(),timezone() from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct2,  " % time_fun_2
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s, " % random.choice(self.q_select)
                sql += "ts ts from regular_table_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['ELAPSED']) :
                sql = "select /*+ para_tables_sort() */ min(asct1),now(),today(),timezone()  from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1  " % time_fun_1
                sql += " from regular_table_1 where "
                sql += "%s )" % random.choice(self.q_where)
                sql += "%s " % random.choice(self.unionall_or_union)
                sql += "select /*+ para_tables_sort() */  avg(asct2),now(),today(),timezone()  from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct2  " % time_fun_2
                sql += " from regular_table_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-3 as time_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (timelist == ['NOW','TODAY']) or (timelist == ['TIMETRUNCATE']) or (timelist == ['TO_ISO8601'])\
                or (timelist == ['TO_UNIXTIMESTAMP']) or (timelist == ['TIMEDIFF_1']) or (timelist == ['TIMEDIFF_2']):
                sql = "select /*+ para_tables_sort() */ ts , timediff(asct1,now) from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1, ts ," % time_fun_1
                sql += "%s as asct2, " % time_fun_2
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s " % random.choice(self.q_select)
                sql += "from regular_table_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s select /*+ para_tables_sort() */" % random.choice(self.unionall_or_union)
                sql += "%s as asct2, ts ," % time_fun_2
                sql += "%s as asct1, " % time_fun_1
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s " % random.choice(self.q_select)
                sql += "from regular_table_2 where "
                sql += "%s " % random.choice(self.q_where)
                sql += " order by asct1 desc  "
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['TIMEZONE']) or (timelist == ['CAST']) or (timelist == ['CAST_1']) or (timelist == ['CAST_2']) or (timelist == ['CAST_3']) or (timelist == ['CAST_4']):
                sql = "select /*+ para_tables_sort() */ ts , (asct1),now(),today(),timezone()  from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1, ts ," % time_fun_1
                sql += "%s as asct2, " % time_fun_2
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s " % random.choice(self.q_select)
                sql += " from regular_table_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s select /*+ para_tables_sort() */" % random.choice(self.unionall_or_union)
                sql += "%s as asct2, ts ," % time_fun_2
                sql += "%s as asct1, " % time_fun_1
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s " % random.choice(self.q_select)
                sql += "from regular_table_2 where "
                sql += "%s " % random.choice(self.q_where)
                sql += " order by asct1 desc  "
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['ELAPSED']) :
                sql = "select /*+ para_tables_sort() */ abs(asct1),now(),today(),timezone()  from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1," % time_fun_1
                sql += "%s as asct2 " % time_fun_2
                sql += "from regular_table_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s select /*+ para_tables_sort() */" % random.choice(self.unionall_or_union)
                sql += "%s as asct2," % time_fun_2
                sql += "%s as asct1  " % time_fun_1
                sql += "from regular_table_2 where "
                sql += "%s " % random.choice(self.q_where)
                sql += " order by asct1 asc "
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-4 as time_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (timelist == ['NOW','TODAY']) or (timelist == ['TIMETRUNCATE']) or (timelist == ['TO_ISO8601'])\
                or (timelist == ['TO_UNIXTIMESTAMP']) or (timelist == ['TIMEDIFF_1']) or (timelist == ['TIMEDIFF_2']):
                sql = "select /*+ para_tables_sort() */ ts1,ts2 ,timediff(ts1,ts2), timediff(asct1,now) from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
                sql += "%s as asct11, " % time_fun_join_1
                sql += "%s as asct1, " % time_fun_join_2
                sql += "%s as asct12, " % time_fun_join_1
                sql += "t1.%s as asct111, " % random.choice(self.q_select)
                sql += "t2.%s as asct121, " % random.choice(self.q_select)
                sql += "t2.ts as ts2  from regular_table_1 t1 , regular_table_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.q_u_where)
                sql += "and %s " % random.choice(self.q_u_or_where)
                sql += "%s " % random.choice(self.order_u_where)
                sql += ");"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                tdSql.checkRows(100)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['TIMEZONE']) or (timelist == ['CAST']) or (timelist == ['CAST_1']) or (timelist == ['CAST_2']) or (timelist == ['CAST_3']) or (timelist == ['CAST_4']):
                sql = "select /*+ para_tables_sort() */ ts1,ts2 ,timediff(ts1,ts2), (asct1) from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
                sql += "%s as asct10, " % time_fun_join_1
                sql += "%s as asct1, " % time_fun_join_2
                sql += "%s as asct11, " % time_fun_join_1
                sql += "t1.%s as asct12, " % random.choice(self.q_select)
                sql += "t2.%s as asct13, " % random.choice(self.q_select)
                sql += "t2.ts as ts2  from regular_table_1 t1 , regular_table_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.q_u_where)
                sql += "and %s " % random.choice(self.q_u_or_where)
                sql += "%s " % random.choice(self.order_u_where)
                sql += ");"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                tdSql.checkRows(100)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['ELAPSED']) :
                sql = "select /*+ para_tables_sort() */ floor(asct1) from  ( select /*+ para_tables_sort() */"
                sql += "%s as asct10, " % time_fun_join_1
                sql += "%s as asct1, " % time_fun_join_2
                sql += "%s as asct11" % time_fun_join_1
                sql += " from regular_table_1 t1 , regular_table_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.q_u_where)
                sql += "and %s " % random.choice(self.q_u_or_where)
                sql += ");"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-5 as time_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (timelist == ['ELAPSED']) :
                sql = "select /*+ para_tables_sort() */ now(),today(),timezone(), "
                sql += "%s, " % time_fun_1
                sql += "%s  " % time_fun_2
                sql += " from  ( select /*+ para_tables_sort() */ * from regular_table_1 ) where "
                sql += "%s " % random.choice(self.q_where)
                sql += " ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            else:
                sql = "select /*+ para_tables_sort() */ ts ,now(),today(),timezone(), "
                sql += "%s as asct11, " % time_fun_1
                sql += "%s as asct12, " % random.choice(self.q_select)
                sql += "%s as asct13, " % random.choice(self.q_select)
                sql += "%s as asct14  " % time_fun_2
                sql += " from  ( select /*+ para_tables_sort() */ * from regular_table_1 ) where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += " ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                tdSql.checkRows(100)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-6 as time_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (timelist == ['NOW','TODAY']) or (timelist == ['TIMETRUNCATE']) or (timelist == ['TO_ISO8601'])\
                or (timelist == ['TO_UNIXTIMESTAMP']) or (timelist == ['TIMEDIFF_1']) or (timelist == ['TIMEDIFF_2']):
                sql = "select /*+ para_tables_sort() */ ts1,ts ,timediff(ts1,ts), timediff(asct1,now) from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
                sql += "%s as asct121, " % time_fun_join_1
                sql += "%s as asct1, " % time_fun_join_2
                sql += "t1.%s as asct11, " % random.choice(self.q_select)
                sql += "t2.%s as asct12, " % random.choice(self.q_select)
                sql += "%s as asct13, " % time_fun_join_1
                sql += "t2.ts   from regular_table_1 t1 , regular_table_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.q_u_where)
                sql += "and %s  )" % random.choice(self.q_u_or_where)
                sql += "%s " % random.choice(self.order_where)
                sql += "%s " % random.choice(self.limit1_where)
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['TIMEZONE']) or (timelist == ['CAST']) or (timelist == ['CAST_1']) or (timelist == ['CAST_2']) or (timelist == ['CAST_3']) or (timelist == ['CAST_4']):
                sql = "select /*+ para_tables_sort() */ ts1,ts ,timediff(ts1,ts), (asct1) from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
                sql += "%s as asct121, " % time_fun_join_1
                sql += "%s as asct1, " % time_fun_join_2
                sql += "t1.%s as asct11, " % random.choice(self.q_select)
                sql += "t2.%s as asct12, " % random.choice(self.q_select)
                sql += "%s as asct13, " % time_fun_join_1
                sql += "t2.ts  from regular_table_1 t1 , regular_table_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.q_u_where)
                sql += "and %s  )" % random.choice(self.q_u_or_where)
                sql += "%s " % random.choice(self.order_where)
                sql += "%s " % random.choice(self.limit1_where)
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['ELAPSED']) :
                sql = "select /*+ para_tables_sort() */  (asct1)*111 from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct121, " % time_fun_join_1
                sql += "%s as asct1, " % time_fun_join_2
                sql += "%s as asct122 " % time_fun_join_1
                sql += " from regular_table_1 t1 , regular_table_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.q_u_where)
                sql += "and %s  )" % random.choice(self.q_u_or_where)
                sql += "%s " % random.choice(self.limit1_where)
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-7 as time_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (timelist == ['NOW','TODAY']) or (timelist == ['TIMETRUNCATE']) or (timelist == ['TO_ISO8601'])\
                or (timelist == ['TO_UNIXTIMESTAMP']) or (timelist == ['TIMEDIFF_1']) or (timelist == ['TIMEDIFF_2']):
                sql = "select /*+ para_tables_sort() */ ts1,m1 , timediff(asct1,now) from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1, ts as m1," % time_fun_1
                sql += "%s as asct2, " % time_fun_2
                sql += "%s as asct11, " % random.choice(self.q_select)
                sql += "%s as asct12, " % random.choice(self.t_select)
                sql += "ts as ts1 from stable_1 where "
                sql += "%s " % random.choice(self.qt_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                tdSql.checkRows(300)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['TIMEZONE']) or (timelist == ['CAST']) or (timelist == ['CAST_1']) or (timelist == ['CAST_2']) or (timelist == ['CAST_3']) or (timelist == ['CAST_4']):
                sql = "select /*+ para_tables_sort() */ tm1,tm2 , (asct1),now(),today(),timezone()  from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1, ts as tm1," % time_fun_1
                sql += "%s as asct2, " % time_fun_2
                sql += "%s as asct11, " % random.choice(self.q_select)
                sql += "%s as asct12, " % random.choice(self.t_select)
                sql += "ts as tm2 from stable_1 where "
                sql += "%s " % random.choice(self.qt_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                tdSql.checkRows(300)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['ELAPSED']) :
                sql = "select /*+ para_tables_sort() */ (asct1)/asct2 ,now(),today(),timezone()  from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1, " % time_fun_1
                sql += "%s as asct2 " % time_fun_2
                sql += "from stable_1 where "
                sql += "%s " % random.choice(self.qt_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-8 as time_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (timelist == ['NOW','TODAY']) or (timelist == ['TIMETRUNCATE']) or (timelist == ['TO_ISO8601'])\
                or (timelist == ['TO_UNIXTIMESTAMP']) or (timelist == ['TIMEDIFF_1']) or (timelist == ['TIMEDIFF_2']):
                sql = "select /*+ para_tables_sort() */ tm1,tm2 , timediff(asct1,now) "
                sql += "from  ( select /*+ para_tables_sort() */ "
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s as asct1, ts as tm1," % time_fun_1
                sql += "%s as asct2, " % time_fun_2
                sql += "%s as asct11, " % random.choice(self.q_select)
                sql += "%s as asct12, " % random.choice(self.t_select)
                sql += "ts as tm2 from stable_1 where "
                sql += "%s " % random.choice(self.qt_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                tdSql.checkRows(300)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['TIMEZONE']) or (timelist == ['CAST']) or (timelist == ['CAST_1']) or (timelist == ['CAST_2']) or (timelist == ['CAST_3']) or (timelist == ['CAST_4']):
                sql = "select /*+ para_tables_sort() */ ts1,ts2 , (asct1),now(),today(),timezone()  "
                sql += "from  ( select /*+ para_tables_sort() */ "
                sql += "%s, " % random.choice(self.s_s_select)
                sql += "%s as asct1, ts as ts1," % time_fun_1
                sql += "%s as asct2, " % time_fun_2
                sql += "%s as asct11, " % random.choice(self.q_select)
                sql += "%s as asct12, " % random.choice(self.t_select)
                sql += "ts as ts2 from stable_1 where "
                sql += "%s " % random.choice(self.qt_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                tdSql.checkRows(300)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['ELAPSED']) :
                sql = "select /*+ para_tables_sort() */ floor(abs(asct1)),now(),today(),timezone()  "
                sql += "from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1, " % time_fun_1
                sql += "%s as asct2 " % time_fun_2
                sql += "from stable_1 where "
                sql += "%s " % random.choice(self.qt_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-9 as time_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (timelist == ['NOW','TODAY']) or (timelist == ['TIMETRUNCATE']) or (timelist == ['TO_ISO8601'])\
                or (timelist == ['TO_UNIXTIMESTAMP']) or (timelist == ['TIMEDIFF_1']) or (timelist == ['TIMEDIFF_2']):
                sql = "select /*+ para_tables_sort() */ ts1,ts2 ,timediff(ts1,ts2), timediff(asct1,now) from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
                sql += "%s as asct121, " % time_fun_join_1
                sql += "%s as asct1, " % time_fun_join_2
                sql += "t1.%s as asct11, " % random.choice(self.q_select)
                sql += "t1.%s as asct12, " % random.choice(self.q_select)
                sql += "t2.%s as asct13, " % random.choice(self.q_select)
                sql += "t2.%s as asct14, " % random.choice(self.q_select)
                sql += "t2.ts as ts2  from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.t_join_where)
                sql += "and %s " % random.choice(self.t_u_where)
                sql += "and %s " % random.choice(self.t_u_or_where)
                sql += "%s " % random.choice(self.order_u_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['TIMEZONE']) or (timelist == ['CAST']) or (timelist == ['CAST_1']) or (timelist == ['CAST_2']) or (timelist == ['CAST_3']) or (timelist == ['CAST_4']):
                sql = "select /*+ para_tables_sort() */ ts1,ts2 ,timediff(ts1,ts2), asct1 from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
                sql += "%s as asct121, " % time_fun_join_1
                sql += "%s as asct1, " % time_fun_join_2
                sql += "t1.%s as asct11, " % random.choice(self.q_select)
                sql += "t1.%s as asct12, " % random.choice(self.q_select)
                sql += "t2.%s as asct13, " % random.choice(self.q_select)
                sql += "t2.%s as asct14, " % random.choice(self.q_select)
                sql += "t2.ts as ts2  from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.t_join_where)
                sql += "and %s " % random.choice(self.t_u_where)
                sql += "and %s " % random.choice(self.t_u_or_where)
                sql += "%s " % random.choice(self.order_u_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['ELAPSED']) :
                sql = "select /*+ para_tables_sort() */ min(asct1*110) from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct121, " % time_fun_join_1
                sql += "%s as asct1 " % time_fun_join_2
                sql += "from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.t_join_where)
                sql += "and %s " % random.choice(self.t_u_where)
                sql += "and %s " % random.choice(self.t_u_or_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        self.restartDnodes()
        tdSql.query("select /*+ para_tables_sort() */1-10 as time_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (timelist == ['NOW','TODAY']) or (timelist == ['TIMETRUNCATE']) or (timelist == ['TO_ISO8601'])\
                or (timelist == ['TO_UNIXTIMESTAMP']) or (timelist == ['TIMEDIFF_1']) or (timelist == ['TIMEDIFF_2']):
                sql = "select /*+ para_tables_sort() */ ts , timediff(asct1,now)  from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1 ," % time_fun_1
                sql += "%s as asct2, " % time_fun_2
                sql += "%s as asct121, " % random.choice(self.s_r_select)
                sql += "%s as asct122, " % random.choice(self.q_select)
                sql += "ts from stable_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ") %s " % random.choice(self.unionall_or_union)
                sql += "select /*+ para_tables_sort() */ ts , timediff(asct1,now)  from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1 ," % time_fun_1
                sql += "%s as asct2, " % time_fun_2
                sql += "%s as asct121, " % random.choice(self.s_r_select)
                sql += "%s as asct122, " % random.choice(self.q_select)
                sql += "ts from stable_2 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['TIMEZONE']) or (timelist == ['CAST']) or (timelist == ['CAST_1']) or (timelist == ['CAST_2']) or (timelist == ['CAST_3']) or (timelist == ['CAST_4']):
                sql = "select /*+ para_tables_sort() */ ts , (asct1),now(),today(),timezone()   from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1 ," % time_fun_1
                sql += "%s as asct2, " % time_fun_2
                sql += "%s as asct121, " % random.choice(self.s_r_select)
                sql += "%s as asct122, " % random.choice(self.q_select)
                sql += "ts from stable_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ") %s " % random.choice(self.unionall_or_union)
                sql += "select /*+ para_tables_sort() */ ts , (asct2),now(),today(),timezone() from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1 ," % time_fun_1
                sql += "%s as asct2, " % time_fun_2
                sql += "%s as asct121, " % random.choice(self.s_r_select)
                sql += "%s as asct122, " % random.choice(self.q_select)
                sql += "ts from stable_2 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['ELAPSED']) :
                sql = "select /*+ para_tables_sort() */ abs(asct1),now(),today(),timezone()   from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1 ," % time_fun_1
                sql += "%s as asct2 " % time_fun_2
                sql += " from stable_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += ") %s " % random.choice(self.unionall_or_union)
                sql += "select /*+ para_tables_sort() */ max(asct2),now(),today(),timezone() from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1 ," % time_fun_1
                sql += "%s as asct2 " % time_fun_2
                sql += "from stable_2 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        #3 inter union not support
        tdSql.query("select /*+ para_tables_sort() */1-11 as time_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (timelist == ['NOW','TODAY']) or (timelist == ['TIMETRUNCATE']) or (timelist == ['TO_ISO8601'])\
                or (timelist == ['TO_UNIXTIMESTAMP']) or (timelist == ['TIMEDIFF_1']) or (timelist == ['TIMEDIFF_2']):
                sql = "select /*+ para_tables_sort() */ ts , timediff(asct1,now), timediff(now,asct2) from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1 ," % time_fun_1
                sql += "%s as asct2, " % time_fun_2
                sql += "%s as asct121, " % random.choice(self.s_r_select)
                sql += "%s as asct122, " % random.choice(self.q_select)
                sql += "ts from stable_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += " %s "  % random.choice(self.unionall_or_union)
                sql += " select /*+ para_tables_sort() */ "
                sql += "%s as asct1 ," % time_fun_1
                sql += "%s as asct2, " % time_fun_2
                sql += "%s as asct121, " % random.choice(self.s_r_select)
                sql += "%s as asct122, " % random.choice(self.q_select)
                sql += "ts from stable_2 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['TIMEZONE']) or (timelist == ['CAST']) or (timelist == ['CAST_1']) or (timelist == ['CAST_2']) or (timelist == ['CAST_3']) or (timelist == ['CAST_4']):
                sql = "select /*+ para_tables_sort() */ ts , asct1,now(),now(),asct2 from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1 ," % time_fun_1
                sql += "%s as asct2, " % time_fun_2
                sql += "%s as asct121, " % random.choice(self.s_r_select)
                sql += "%s as asct122, " % random.choice(self.q_select)
                sql += "ts from stable_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += " %s "  % random.choice(self.unionall_or_union)
                sql += " select /*+ para_tables_sort() */ "
                sql += "%s as asct1 ," % time_fun_1
                sql += "%s as asct2, " % time_fun_2
                sql += "%s as asct121, " % random.choice(self.s_r_select)
                sql += "%s as asct122, " % random.choice(self.q_select)
                sql += "ts from stable_2 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.order_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['ELAPSED']) :
                sql = "select /*+ para_tables_sort() */ asct1+asct2,now(),today(),timezone() from  ( select /*+ para_tables_sort() */ "
                sql += "%s as asct1, " % time_fun_1
                sql += "%s as asct2 " % time_fun_2
                sql += " from stable_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += " %s "  % random.choice(self.unionall_or_union)
                sql += " select /*+ para_tables_sort() */ "
                sql += "%s as asct1 ," % time_fun_1
                sql += "%s as asct2 " % time_fun_2
                sql += " from stable_2 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "order by asct1 "
                sql += "%s " % random.choice(self.limit1_where)
                sql += ")"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-12 as time_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (timelist == ['NOW','TODAY']) or (timelist == ['TIMETRUNCATE']) or (timelist == ['TO_ISO8601'])\
                or (timelist == ['TO_UNIXTIMESTAMP']) or (timelist == ['TIMEDIFF_1']) or (timelist == ['TIMEDIFF_2']):
                sql = "select /*+ para_tables_sort() */ ts1,ts2 ,timediff(ts1,ts2), timediff(asct1,now) from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
                sql += "%s, " % time_fun_join_1
                sql += "%s as asct1, " % time_fun_join_2
                sql += "t1.%s as asct11, " % random.choice(self.q_select)
                sql += "t1.%s as asct12, " % random.choice(self.q_select)
                sql += "t2.%s as asct13, " % random.choice(self.q_select)
                sql += "t2.%s as asct14, " % random.choice(self.q_select)
                sql += "t2.ts as ts2  from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.t_u_where)
                sql += "and %s " % random.choice(self.t_u_or_where)
                sql += "%s " % random.choice(self.order_u_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ");"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['TIMEZONE']) or (timelist == ['CAST']) or (timelist == ['CAST_1']) or (timelist == ['CAST_2']) or (timelist == ['CAST_3']) or (timelist == ['CAST_4']):
                sql = "select /*+ para_tables_sort() */ ts1,ts2 ,timediff(ts1,ts2), asct1,now() from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
                sql += "%s, " % time_fun_join_1
                sql += "%s as asct1, " % time_fun_join_2
                sql += "t1.%s as asct11, " % random.choice(self.q_select)
                sql += "t1.%s as asct12, " % random.choice(self.q_select)
                sql += "t2.%s as asct13, " % random.choice(self.q_select)
                sql += "t2.%s as asct14, " % random.choice(self.q_select)
                sql += "t2.ts as ts2  from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.t_u_where)
                sql += "and %s " % random.choice(self.t_u_or_where)
                sql += "%s " % random.choice(self.order_u_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ");"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['ELAPSED']) :
                sql = "select /*+ para_tables_sort() */min(floor(asct1)),now() from  ( select /*+ para_tables_sort() */"
                sql += "%s as asct121, " % time_fun_join_1
                sql += "%s as asct1 " % time_fun_join_2
                sql += " from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and "
                sql += "%s " % random.choice(self.t_u_where)
                sql += "and %s " % random.choice(self.t_u_or_where)
                sql += "%s " % random.choice(self.limit1_where)
                sql += ");"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-13 as time_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (timelist == ['NOW','TODAY']) or (timelist == ['TIMETRUNCATE']) or (timelist == ['TO_ISO8601'])\
                or (timelist == ['TO_UNIXTIMESTAMP']) or (timelist == ['TIMEDIFF_1']) or (timelist == ['TIMEDIFF_2']):
                sql = "select /*+ para_tables_sort() */ ts , timediff(%s,now)," % time_fun_2
                sql += "%s as asct1, " % time_fun_1
                sql += "%s as asct11, " % random.choice(self.q_select)
                sql += "%s as asct12, " % random.choice(self.q_select)
                sql += "%s as asct13, " % time_fun_2
                sql += "%s as asct122  " % random.choice(self.t_select)
                sql += " from  ( select /*+ para_tables_sort() */ * from stable_1 where "
                sql += "%s " % random.choice(self.qt_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                tdSql.checkRows(300)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['TIMEZONE']) or (timelist == ['CAST']) or (timelist == ['CAST_1']) or (timelist == ['CAST_2']) or (timelist == ['CAST_3']) or (timelist == ['CAST_4']):
                sql = "select /*+ para_tables_sort() */ ts ,now(),today(),timezone(), "
                sql += "%s as asct1, " % time_fun_1
                sql += "%s as asct11, " % random.choice(self.q_select)
                sql += "%s as asct12, " % random.choice(self.q_select)
                sql += "%s as asct13, " % time_fun_2
                sql += "%s as asct122  " % random.choice(self.t_select)
                sql += " from  ( select /*+ para_tables_sort() */ * from stable_1 where "
                sql += "%s " % random.choice(self.qt_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                tdSql.checkRows(300)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['ELAPSED']) :
                sql = "select /*+ para_tables_sort() */ now(),today(),timezone(), "
                sql += "%s as asct1, " % time_fun_1
                sql += "%s as asct12  " % time_fun_2
                sql += " from  ( select /*+ para_tables_sort() */ * from stable_1 where "
                sql += "%s " % random.choice(self.qt_where)
                sql += "%s " % random.choice(self.order_where)
                sql += ") ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-14 as time_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (timelist == ['NOW','TODAY']) or (timelist == ['TIMETRUNCATE']) or (timelist == ['TO_ISO8601'])\
                or (timelist == ['TO_UNIXTIMESTAMP']) or (timelist == ['TIMEDIFF_1']) or (timelist == ['TIMEDIFF_2']):
                sql = "select /*+ para_tables_sort() */ ts , timediff(asct1,now),timediff(now,asct2) from ( select /*+ para_tables_sort() */ts ts ,"
                sql += "%s as asct1, " % time_fun_1
                sql += "%s as asct2" % time_fun_2
                sql += "  from stable_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.partiton_where)
                sql += "%s " % random.choice(self.order_desc_where)
                sql += "%s " % random.choice([self.limit_where[2] , self.limit_where[3]] )
                sql += " ) ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['TIMEZONE']) or (timelist == ['CAST']) or (timelist == ['CAST_1']) or (timelist == ['CAST_2']) or (timelist == ['CAST_3']) or (timelist == ['CAST_4']):
                sql = "select /*+ para_tables_sort() */ ts , (asct1),now(),(now()),asct2 from ( select /*+ para_tables_sort() */ts ts ,"
                sql += "%s as asct1, " % time_fun_1
                sql += "%s as asct2" % time_fun_2
                sql += "  from stable_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.partiton_where)
                sql += "%s " % random.choice(self.order_desc_where)
                sql += "%s " % random.choice([self.limit_where[2] , self.limit_where[3]] )
                sql += " ) ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['ELAPSED']) :
                sql = "select /*+ para_tables_sort() */ (asct1)*asct2,now(),(now()) from ( select /*+ para_tables_sort() */"
                sql += "%s as asct1, " % time_fun_1
                sql += "%s as asct2" % time_fun_2
                sql += "  from stable_1 where "
                sql += "%s " % random.choice(self.q_where)
                sql += "%s " % random.choice(self.partiton_where)
                sql += "%s " % random.choice([self.limit_where[2] , self.limit_where[3]] )
                sql += " ) ;"
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-15 as time_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            if (timelist == ['NOW','TODAY']) or (timelist == ['TIMETRUNCATE']) or (timelist == ['TO_ISO8601'])\
                or (timelist == ['TO_UNIXTIMESTAMP']) or (timelist == ['TIMEDIFF_1']) or (timelist == ['TIMEDIFF_2']):
                sql = "select /*+ para_tables_sort() */ ts1,ts ,timediff(ts1,ts), timediff(asct1,now),timediff(now,asct2) from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
                sql += "%s as asct2, " % time_fun_join_1
                sql += "%s as asct1, " % time_fun_join_2
                sql += "t1.%s as asct11, " % random.choice(self.q_select)
                sql += "t1.%s as asct12, " % random.choice(self.q_select)
                sql += "t2.%s as asct13, " % random.choice(self.q_select)
                sql += "t2.%s as asct14, " % random.choice(self.q_select)
                sql += "t2.ts  from stable_1  t1, stable_2 t2 where t1.ts = t2.ts and  "
                sql += "%s " % random.choice(self.t_join_where)
                sql += " and %s " % random.choice(self.qt_u_or_where)
                sql += "%s " % random.choice(self.partiton_where_j)
                sql += "%s " % random.choice(self.slimit1_where)
                sql += ") "
                sql += "%s " % random.choice(self.order_desc_where)
                sql += "%s ;" % random.choice(self.limit_u_where)
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['TIMEZONE']) or (timelist == ['CAST']) or (timelist == ['CAST_1']) or (timelist == ['CAST_2']) or (timelist == ['CAST_3']) or (timelist == ['CAST_4']):
                sql = "select /*+ para_tables_sort() */ ts1,ts ,timediff(ts1,ts), asct1,(now()),(now()),asct2 ,now(),today(),timezone() from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
                sql += "%s as asct2, " % time_fun_join_1
                sql += "%s as asct1, " % time_fun_join_2
                sql += "t1.%s as asct11, " % random.choice(self.q_select)
                sql += "t1.%s as asct12, " % random.choice(self.q_select)
                sql += "t2.%s as asct13, " % random.choice(self.q_select)
                sql += "t2.%s as asct14, " % random.choice(self.q_select)
                sql += "t2.ts  from stable_1  t1, stable_2 t2 where t1.ts = t2.ts and  "
                sql += "%s " % random.choice(self.t_join_where)
                sql += " and %s " % random.choice(self.qt_u_or_where)
                sql += "%s " % random.choice(self.partiton_where_j)
                sql += "%s " % random.choice(self.slimit1_where)
                sql += ") "
                sql += "%s " % random.choice(self.order_desc_where)
                sql += "%s ;" % random.choice(self.limit_u_where)
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)
            elif (timelist == ['ELAPSED']) :
                sql = "select /*+ para_tables_sort() */  asct1,(now()),(now()),asct2 ,now(),today(),timezone() from  ( select /*+ para_tables_sort() */"
                sql += "%s as asct2, " % time_fun_join_1
                sql += "%s as asct1 " % time_fun_join_2
                sql += "from stable_1  t1, stable_2 t2 where t1.ts = t2.ts and  "
                sql += "%s " % random.choice(self.t_join_where)
                sql += " and %s " % random.choice(self.qt_u_or_where)
                sql += "%s " % random.choice(self.partiton_where_j)
                sql += "%s " % random.choice(self.slimit1_where)
                sql += ") "
                sql += "%s ;" % random.choice(self.limit_u_where)
                tdLog.info(sql)
                tdLog.info(len(sql))
                tdSql.query(sql)
                self.cur1.execute(sql)
                self.explain_sql(sql)

        #taos -f sql
        startTime_taos_f = time.time()
        print("taos -f %s sql start!" %timelist)
        taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
        #_ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
        _ = subprocess.check_output(taos_cmd1, shell=True)
        print("taos -f %s sql over!" %timelist)
        endTime_taos_f = time.time()
        print("taos_f total time %ds" % (endTime_taos_f - startTime_taos_f))

        print("=========%s====over=============" %timelist)

    def base_nest(self,baselist):

        print("==========%s===start=============" %baselist)
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))

        self.dropandcreateDB_random("%s" %self.db_nest, 1)

        if (baselist == ['A']) or (baselist == ['S']) or (baselist == ['F']) \
            or (baselist == ['C']):
            base_functions = baselist
            fun_fix_column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)','(q_bigint_null)','(q_smallint_null)','(q_tinyint_null)','(q_int_null)','(q_float_null)','(q_double_null)']
            fun_column_1 = random.sample(base_functions,1)+random.sample(fun_fix_column,1)
            base_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            fun_column_2 = random.sample(base_functions,1)+random.sample(fun_fix_column,1)
            base_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","").replace(", ","")

            fun_fix_column_j = ['(t1.q_bigint)','(t1.q_smallint)','(t1.q_tinyint)','(t1.q_int)','(t1.q_float)','(t1.q_double)','(t1.q_bigint_null)','(t1.q_smallint_null)','(t1.q_tinyint_null)','(t1.q_int_null)','(t1.q_float_null)','(t1.q_double_null)',
                            '(t2.q_bigint)','(t2.q_smallint)','(t2.q_tinyint)','(t2.q_int)','(t2.q_float)','(t2.q_double)','(t2.q_bigint_null)','(t2.q_smallint_null)','(t2.q_tinyint_null)','(t2.q_int_null)','(t2.q_float_null)','(t2.q_double_null)']
            fun_column_join_1 = random.sample(base_functions,1)+random.sample(fun_fix_column_j,1)
            base_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","").replace(", ","")
            fun_column_join_2 = random.sample(base_functions,1)+random.sample(fun_fix_column_j,1)
            base_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","").replace(", ","")

        elif (baselist == ['P']) or (baselist == ['M']) or (baselist == ['S'])or (baselist == ['T']):
            base_functions = baselist
            num = random.randint(0, 1000)
            fun_fix_column = ['(q_bigint,num)','(q_smallint,num)','(q_tinyint,num)','(q_int,num)','(q_float,num)','(q_double,num)',
                              '(q_bigint_null,num)','(q_smallint_null,num)','(q_tinyint_null,num)','(q_int_null,num)','(q_float_null,num)','(q_double_null,num)']
            fun_column_1 = random.sample(base_functions,1)+random.sample(fun_fix_column,1)
            base_fun_1 = str(fun_column_1).replace("[","").replace("]","").replace("'","").replace(", ","").replace("num",base(num))
            fun_column_2 = random.sample(base_functions,1)+random.sample(fun_fix_column,1)
            base_fun_2 = str(fun_column_2).replace("[","").replace("]","").replace("'","").replace(", ","").replace("num",base(num))

            fun_fix_column_j = ['(t1.q_bigint,num)','(t1.q_smallint,num)','(t1.q_tinyint,num)','(t1.q_int,num)','(t1.q_float,num)','(t1.q_double,num)',
                    '(t1.q_bigint_null,num)','(t1.q_smallint_null,num)','(t1.q_tinyint_null,num)','(t1.q_int_null,num)','(t1.q_float_null,num)','(t1.q_double_null,num)',
                    '(t2.q_bigint,num)','(t2.q_smallint,num)','(t2.q_tinyint,num)','(t2.q_int,num)','(t2.q_float,num)','(t2.q_double,num)',
                    '(t2.q_bigint_null,num)','(t2.q_smallint_null,num)','(t2.q_tinyint_null,num)','(t2.q_int_null,num)','(t2.q_float_null,num)','(t2.q_double_null,num)']
            fun_column_join_1 = random.sample(base_functions,1)+random.sample(fun_fix_column_j,1)
            base_fun_join_1 = str(fun_column_join_1).replace("[","").replace("]","").replace("'","").replace(", ","").replace("num",base(num))
            fun_column_join_2 = random.sample(base_functions,1)+random.sample(fun_fix_column_j,1)
            base_fun_join_2 = str(fun_column_join_2).replace("[","").replace("]","").replace("'","").replace(", ","").replace("num",base(num))

        tdSql.query("select /*+ para_tables_sort() */1-1 as base_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ ts , floor(asct1)  from  ( select /*+ para_tables_sort() */ "
            sql += "%s as asct1, " % base_fun_1
            sql += "%s as asct2, " % base_fun_2
            sql += "%s, " % random.choice(self.s_s_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "ts from regular_table_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.order_where)
            sql += ");"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(100)
            self.cur1.execute(sql)

        tdSql.query("select /*+ para_tables_sort() */1-2 as base_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ ts , abs(asct1) from  ( select /*+ para_tables_sort() */ "
            sql += "%s as asct1,  " % base_fun_1
            sql += "%s, " % random.choice(self.s_s_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "ts ts from regular_table_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s )" % random.choice(self.order_where)
            sql += "%s " % random.choice(self.unionall_or_union)
            sql += "select /*+ para_tables_sort() */ ts , asct2 from  ( select /*+ para_tables_sort() */ "
            sql += "%s as asct2,  " % base_fun_2
            sql += "%s, " % random.choice(self.s_s_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "ts ts from regular_table_1 where "
            sql += "%s " % random.choice(self.q_where)
            #sql += "%s " % random.choice(having_support)
            sql += "%s " % random.choice(self.order_where)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ")"
            tdLog.info(sql)
            tdLog.info(len(sql))
            #tdSql.query(sql)
            #self.cur1.execute(sql)

        tdSql.query("select /*+ para_tables_sort() */1-3 as base_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ ts , min(asct1) from  ( select /*+ para_tables_sort() */ "
            sql += "%s as asct1, ts ," % base_fun_1
            sql += "%s as asct2, " % base_fun_2
            sql += "%s, " % random.choice(self.s_s_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "ts from regular_table_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s select /*+ para_tables_sort() */" % random.choice(self.unionall_or_union)
            sql += "%s as asct2, ts ," % base_fun_2
            sql += "%s as asct1, " % base_fun_1
            sql += "%s, " % random.choice(self.s_s_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "ts from regular_table_2 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.order_where)
            sql += ")"
            tdLog.info(sql)
            tdLog.info(len(sql))
            #tdSql.query(sql)
            #self.cur1.execute(sql)

        tdSql.query("select /*+ para_tables_sort() */1-4 as base_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ ts1,ts2 ,timediff(ts1,ts2), asct1 from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
            sql += "%s, " % base_fun_join_1
            sql += "%s as asct1, " % base_fun_join_2
            sql += "%s, " % base_fun_join_1
            sql += "t1.%s, " % random.choice(self.q_select)
            sql += "t2.%s, " % random.choice(self.q_select)
            sql += "t2.ts from regular_table_1 t1 , regular_table_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(self.q_u_where)
            sql += "and %s " % random.choice(self.q_u_or_where)
            sql += "%s " % random.choice(self.order_u_where)
            sql += ");"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(100)
            self.cur1.execute(sql)

        tdSql.query("select /*+ para_tables_sort() */1-5 as base_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ ts ,"
            sql += "%s, " % base_fun_1
            sql += "%s, " % random.choice(self.q_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "%s  " % base_fun_2
            sql += " from  ( select /*+ para_tables_sort() */ * from regular_table_1 ) where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.order_where)
            sql += " ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(100)
            self.cur1.execute(sql)

        tdSql.query("select /*+ para_tables_sort() */1-6 as base_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ ts , max(asct1) from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
            sql += "%s, " % base_fun_join_1
            sql += "%s as asct1, " % base_fun_join_2
            sql += "t1.%s, " % random.choice(self.q_select)
            sql += "t2.%s, " % random.choice(self.q_select)
            sql += "%s, " % base_fun_join_1
            sql += "t2.ts from regular_table_1 t1 , regular_table_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(self.q_u_where)
            sql += "and %s  )" % random.choice(self.q_u_or_where)
            sql += "%s " % random.choice(self.order_where)
            sql += "%s " % random.choice(self.limit1_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)

        tdSql.query("select /*+ para_tables_sort() */1-7 as base_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ ts , abs(asct1) from  ( select /*+ para_tables_sort() */ "
            sql += "%s as asct1, ts ," % base_fun_1
            sql += "%s as asct2, " % base_fun_2
            sql += "%s, " % random.choice(self.q_select)
            sql += "%s, " % random.choice(self.t_select)
            sql += "ts from stable_1 where "
            sql += "%s " % random.choice(self.qt_where)
            sql += "%s " % random.choice(self.order_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(300)
            self.cur1.execute(sql)

        tdSql.query("select /*+ para_tables_sort() */1-8 as base_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ ts,floor(asct1) "
            sql += "from  ( select /*+ para_tables_sort() */ "
            sql += "%s, " % random.choice(self.s_s_select)
            sql += "%s as asct1, ts ," % base_fun_1
            sql += "%s as asct2, " % base_fun_2
            sql += "%s, " % random.choice(self.q_select)
            sql += "%s, " % random.choice(self.t_select)
            sql += "ts from stable_1 where "
            sql += "%s " % random.choice(self.qt_where)
            sql += "%s " % random.choice(self.order_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(300)
            self.cur1.execute(sql)

        tdSql.query("select /*+ para_tables_sort() */1-9 as base_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ ts , max(asct1) from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
            sql += "%s, " % base_fun_join_1
            sql += "%s as asct1, " % base_fun_join_2
            sql += "t1.%s, " % random.choice(self.q_select)
            sql += "t1.%s, " % random.choice(self.q_select)
            sql += "t2.%s, " % random.choice(self.q_select)
            sql += "t2.%s, " % random.choice(self.q_select)
            sql += "t2.ts from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(self.t_join_where)
            sql += "and %s " % random.choice(self.t_u_where)
            sql += "and %s " % random.choice(self.t_u_or_where)
            sql += "%s " % random.choice(self.order_u_where)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)

        self.restartDnodes()
        tdSql.query("select /*+ para_tables_sort() */1-10 as base_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ ts , min(asct1) from  ( select /*+ para_tables_sort() */ "
            sql += "%s as asct1, ts ," % base_fun_1
            sql += "%s as asct2, " % base_fun_2
            sql += "%s, " % random.choice(self.s_r_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "ts from stable_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.order_where)
            sql += ") %s " % random.choice(self.unionall_or_union)
            sql += "select /*+ para_tables_sort() */ ts , max(asct2) from  ( select /*+ para_tables_sort() */ "
            sql += "%s as asct1, ts ," % base_fun_1
            sql += "%s as asct2, " % base_fun_2
            sql += "%s, " % random.choice(self.s_r_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "ts from stable_2 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.order_where)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ")"
            tdLog.info(sql)
            tdLog.info(len(sql))
            #tdSql.query(sql)
            #self.cur1.execute(sql)

        #3 inter union not support
        tdSql.query("select /*+ para_tables_sort() */1-11 as base_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ ts , min(asct1), max(asct2) from  ( select /*+ para_tables_sort() */ "
            sql += "%s as asct1, ts ," % base_fun_1
            sql += "%s as asct2, " % base_fun_2
            sql += "%s, " % random.choice(self.s_r_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "ts from stable_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.order_where)
            #sql += "%s " % random.choice(limit1_where)
            sql += " %s "  % random.choice(self.unionall_or_union)
            sql += " select /*+ para_tables_sort() */ "
            sql += "%s as asct1, ts ," % base_fun_1
            sql += "%s as asct2, " % base_fun_2
            sql += "%s, " % random.choice(self.s_r_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "ts from stable_2 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.order_where)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ")"
            tdLog.info(sql)
            tdLog.info(len(sql))
            #TD-15837 tdSql.query(sql)
            # self.cur1.execute(sql)

        tdSql.query("select /*+ para_tables_sort() */1-12 as base_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ ts , max(asct1) from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
            sql += "%s, " % base_fun_join_1
            sql += "%s as asct1, " % base_fun_join_2
            sql += "t1.%s, " % random.choice(self.q_select)
            sql += "t1.%s, " % random.choice(self.q_select)
            sql += "t2.%s, " % random.choice(self.q_select)
            sql += "t2.%s, " % random.choice(self.q_select)
            sql += "t2.ts from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(self.t_u_where)
            sql += "and %s " % random.choice(self.t_u_or_where)
            sql += "%s " % random.choice(self.order_u_where)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ");"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)

        tdSql.query("select /*+ para_tables_sort() */1-13 as base_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ ts ,"
            sql += "%s, " % base_fun_1
            sql += "%s, " % random.choice(self.q_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "%s  " % base_fun_2
            sql += "%s  " % random.choice(self.t_select)
            sql += " from  ( select /*+ para_tables_sort() */ * from stable_1 where "
            sql += "%s " % random.choice(self.qt_where)
            sql += "%s " % random.choice(self.order_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(300)
            self.cur1.execute(sql)

        tdSql.query("select /*+ para_tables_sort() */1-14 as base_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ avg(asct1),count(asct2) from ( select /*+ para_tables_sort() */"
            sql += "%s as asct1, " % base_fun_1
            sql += "%s as asct2" % base_fun_2
            sql += "  from stable_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.partiton_where)
            sql += "%s " % random.choice(self.order_desc_where)
            sql += "%s " % random.choice([self.limit_where[2] , self.limit_where[3]] )
            sql += " ) ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)

        tdSql.query("select /*+ para_tables_sort() */1-15 as base_nest from stable_1 limit 1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ ts , max(asct1) from  ( select /*+ para_tables_sort() */ t1.ts as ts1,"
            sql += "%s, " % base_fun_join_1
            sql += "%s as asct1, " % base_fun_join_2
            sql += "t1.%s, " % random.choice(self.q_select)
            sql += "t1.%s, " % random.choice(self.q_select)
            sql += "t2.%s, " % random.choice(self.q_select)
            sql += "t2.%s " % random.choice(self.q_select)
            sql += "from stable_1  t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.t_join_where)
            sql += " and %s " % random.choice(self.qt_u_or_where)
            sql += "%s " % random.choice(self.partiton_where_j)
            sql += "%s " % random.choice(self.slimit1_where)
            sql += ") "
            sql += "%s " % random.choice(self.order_desc_where)
            sql += "%s ;" % random.choice(self.limit_u_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)

        #taos -f sql
        startTime_taos_f = time.time()
        print("taos -f %s sql start!" %baselist)
        taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
        _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
        print("taos -f %s sql over!" %baselist)
        endTime_taos_f = time.time()
        print("taos_f total time %ds" % (endTime_taos_f - startTime_taos_f))

        print("=========%s====over=============" %baselist)

    def function_before_26(self):

        print('=====================2.6 old function start ===========')
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))

        self.dropandcreateDB_random("%s" %self.db_nest, 1)

        #1 select /*+ para_tables_sort() */* from (select /*+ para_tables_sort() */column form regular_table where <\>\in\and\or order by)
        tdSql.query("select /*+ para_tables_sort() */1-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ tas  from  ( select /*+ para_tables_sort() */ "
            sql += "%s, " % random.choice(self.s_s_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "ts as tas from regular_table_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.order_where)
            sql += ");"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql,queryTimes=1)
            tdSql.checkRows(100)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #1 outer union not support
        #self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select /*+ para_tables_sort() */1-2 from stable_1;")
        for i in range(self.fornum):
            #sql = "select /*+ para_tables_sort() */ ts , * from  ( select /*+ para_tables_sort() */ "
            sql = "select /*+ para_tables_sort() */ t1s  from  ( select /*+ para_tables_sort() */ "
            sql += "%s, " % random.choice(self.s_r_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "ts as t1s from regular_table_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.order_where)
            sql += ") union "
            sql += "select /*+ para_tables_sort() */ t2s  from  ( select /*+ para_tables_sort() */ "
            sql += "%s, " % random.choice(self.s_r_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "ts as t2s from regular_table_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.order_where)
            sql += ")"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(100)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select /*+ para_tables_sort() */1-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ ts  from  ( select /*+ para_tables_sort() */ "
            sql += "%s, " % random.choice(self.s_r_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "ts from regular_table_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.order_where)
            sql += ") union all "
            sql += "select /*+ para_tables_sort() */ ts  from  ( select /*+ para_tables_sort() */ "
            sql += "%s, " % random.choice(self.s_r_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "ts from regular_table_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.order_where)
            sql += ")"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(200)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #1 inter union not support
        tdSql.query("select /*+ para_tables_sort() */1-3 from stable_1;")
        for i in range(self.fornum):
            #sql = "select /*+ para_tables_sort() */ ts , * from  ( select /*+ para_tables_sort() */ "
            sql = "select /*+ para_tables_sort() */ ts  from  ( select /*+ para_tables_sort() */ "
            sql += "%s, " % random.choice(self.s_r_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "ts from regular_table_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += ""
            sql += " union all select /*+ para_tables_sort() */ "
            sql += "%s, " % random.choice(self.s_r_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "ts from regular_table_2 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.order_where)
            sql += ")"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(300)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */1-3 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ ts  from  ( select /*+ para_tables_sort() */ "
            sql += "%s, " % random.choice(self.s_r_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "ts from regular_table_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "  union all select /*+ para_tables_sort() */ "
            sql += "%s, " % random.choice(self.s_r_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "ts from regular_table_2 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.order_where)
            sql += ")"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(300)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #join:select /*+ para_tables_sort() */* from (select /*+ para_tables_sort() */column form regular_table1，regular_table2 where  t1.ts=t2.ts and <\>\in\and\or order by)
        #self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select /*+ para_tables_sort() */1-4 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ *  from  ( select /*+ para_tables_sort() */ t1.ts as t1ts,"
            sql += "t1.%s as t11, " % random.choice(self.q_select)
            sql += "t1.%s as t12, " % random.choice(self.q_select)
            sql += "t2.%s as t21, " % random.choice(self.q_select)
            sql += "t2.%s as t22, " % random.choice(self.q_select)
            sql += "t2.ts as t2ts from regular_table_1 t1 , regular_table_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(self.q_u_where)
            sql += "and %s " % random.choice(self.q_u_or_where)
            sql += "%s " % random.choice(self.order_u_where)
            sql += ");"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(100)
            self.cur1.execute(sql)
            self.explain_sql(sql)


        #2 select /*+ para_tables_sort() */column from (select /*+ para_tables_sort() */* form regular_table ) where <\>\in\and\or order by
        #self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select /*+ para_tables_sort() */2-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ ts ,"
            sql += "%s, " % random.choice(self.s_r_select)
            sql += "%s  " % random.choice(self.q_select)
            sql += " from  ( select /*+ para_tables_sort() */ * from regular_table_1 ) where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.order_where)
            sql += " ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(100)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #join: select /*+ para_tables_sort() */column from (select /*+ para_tables_sort() */column form regular_table1，regular_table2 )where  t1.ts=t2.ts and <\>\in\and\or order by
        #cross join not supported yet
        tdSql.query("select /*+ para_tables_sort() */2-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ ts , * from  ( select /*+ para_tables_sort() */ t1.ts ,"
            sql += "t1.%s, " % random.choice(self.q_select)
            sql += "t1.%s, " % random.choice(self.q_select)
            sql += "t2.%s, " % random.choice(self.q_select)
            sql += "t2.%s, " % random.choice(self.q_select)
            sql += "t2.ts from regular_table_1 t1 , regular_table_2 t2 ) where t1.ts = t2.ts and "
            sql += "%s " % random.choice(self.q_u_where)
            sql += "%s " % random.choice(self.order_u_where)
            #sql += ");"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.error(sql)

        #3 select /*+ para_tables_sort() */* from (select /*+ para_tables_sort() */column\tag form stable  where <\>\in\and\or order by )
        #self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select /*+ para_tables_sort() */3-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  *  from  ( select /*+ para_tables_sort() */ "
            sql += "%s, " % random.choice(self.s_s_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "%s, " % random.choice(self.t_select)
            sql += "ts from stable_1 where "
            sql += "%s " % random.choice(self.qt_where)
            sql += "%s " % random.choice(self.order_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(300)
            self.cur1.execute(sql)
            self.explain_sql(sql)
        tdSql.query("select /*+ para_tables_sort() */3-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ ts, "
            sql += "%s  " % random.choice(self.s_r_select)
            sql += "from  ( select /*+ para_tables_sort() */ "
            sql += "%s, " % random.choice(self.s_s_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "%s, " % random.choice(self.t_select)
            sql += "ts from stable_1 where "
            sql += "%s " % random.choice(self.qt_where)
            sql += "%s " % random.choice(self.order_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(300)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        # select /*+ para_tables_sort() */ts,* from (select /*+ para_tables_sort() */column\tag form stable1,stable2  where t1.ts = t2.ts and <\>\in\and\or order by )
        #self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select /*+ para_tables_sort() */3-2 from stable_1;")
        for i in range(self.fornum):
            #sql = "select /*+ para_tables_sort() */ ts , *  from  ( select /*+ para_tables_sort() */ t1.ts as t1ts , "
            sql = "select /*+ para_tables_sort() */ t1ts , t2ts  from  ( select /*+ para_tables_sort() */ t1.ts as t1ts , "
            sql += "t1.%s as t11, " % random.choice(self.t_select)
            sql += "t1.%s as t12, " % random.choice(self.q_select)
            sql += "t2.%s as t13, " % random.choice(self.t_select)
            sql += "t2.%s as t14, " % random.choice(self.q_select)
            sql += "t2.ts as t2ts from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(self.t_join_where)
            sql += "%s " % random.choice(self.order_u_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #3 outer union not support
        self.restartDnodes()
        tdSql.query("select /*+ para_tables_sort() */3-3 from stable_1;")
        for i in range(self.fornum):
            #sql = "select /*+ para_tables_sort() */ ts , * from  ( select /*+ para_tables_sort() */ "
            sql = "select /*+ para_tables_sort() */ ts1  from  ( select /*+ para_tables_sort() */ "
            sql += "%s, " % random.choice(self.s_r_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "ts as ts1 from stable_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.order_where)
            sql += ") union "
            sql += "select /*+ para_tables_sort() */ ts2  from  ( select /*+ para_tables_sort() */ "
            sql += "%s, " % random.choice(self.s_r_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "ts as ts2 from stable_2 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.order_where)
            sql += ")"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(500)
            self.cur1.execute(sql)
            self.explain_sql(sql)
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ ts1  from  ( select /*+ para_tables_sort() */ "
            sql += "%s, " % random.choice(self.s_r_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "ts as ts1 from stable_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.order_where)
            sql += ") union all "
            sql += "select /*+ para_tables_sort() */ ts2  from  ( select /*+ para_tables_sort() */ "
            sql += "%s, " % random.choice(self.s_r_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "ts as ts2 from stable_2 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.order_where)
            sql += ")"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(600)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #3 inter union not support
        tdSql.query("select /*+ para_tables_sort() */3-4 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ * from  ( select /*+ para_tables_sort() */ "
            sql += "%s, " % random.choice(self.s_r_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "ts from stable_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += " %s "  % random.choice(self.unionall_or_union)
            sql += " select /*+ para_tables_sort() */ "
            sql += "%s, " % random.choice(self.s_r_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "ts from stable_2 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.order_where)
            sql += ")"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #join:select /*+ para_tables_sort() */* from (select /*+ para_tables_sort() */column form stable1，stable2 where  t1.ts=t2.ts and <\>\in\and\or order by)
        tdSql.query("select /*+ para_tables_sort() */3-5 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ *  from  ( select /*+ para_tables_sort() */ t1.ts as t1ts,"
            sql += "t1.%s as t11, " % random.choice(self.q_select)
            sql += "t1.%s as t12, " % random.choice(self.q_select)
            sql += "t2.%s as t21, " % random.choice(self.q_select)
            sql += "t2.%s as t22, " % random.choice(self.q_select)
            sql += "t2.ts as t2ts from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(self.t_u_where)
            sql += "%s " % random.choice(self.order_u_where)
            sql += ");"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(100)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */3-6 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ * from  ( select /*+ para_tables_sort() */ t1.ts as t1ts ,"
            sql += "t1.%s as t11, " % random.choice(self.q_select)
            sql += "t1.%s as t12, " % random.choice(self.q_select)
            sql += "t2.%s as t21, " % random.choice(self.q_select)
            sql += "t2.%s as t22, " % random.choice(self.q_select)
            sql += "t2.ts as t2ts from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(self.t_u_or_where)
            sql += "%s " % random.choice(self.order_u_where)
            sql += ");"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(100)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #4 select /*+ para_tables_sort() */column from (select /*+ para_tables_sort() */* form stable  where <\>\in\and\or order by )
        #self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select /*+ para_tables_sort() */4-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ ts , "
            sql += "%s as t11, " % random.choice(self.q_select)
            sql += "%s as t12, " % random.choice(self.q_select)
            sql += "%s " % random.choice(self.t_select)
            sql += " from  ( select /*+ para_tables_sort() */ * from stable_1 where "
            sql += "%s " % random.choice(self.qt_where)
            sql += "%s " % random.choice(self.order_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(300)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #5 select /*+ para_tables_sort() */distinct column\tag from (select /*+ para_tables_sort() */* form stable  where <\>\in\and\or order by limit offset )
        tdSql.query("select /*+ para_tables_sort() */5-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  "
            sql += "%s " % random.choice(self.dqt_select)
            sql += " from  ( select /*+ para_tables_sort() */ * from stable_1 where "
            sql += "%s " % random.choice(self.qt_where)
            sql += "%s " % random.choice(self.order_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #5-1 select /*+ para_tables_sort() */distinct column\tag from (select /*+ para_tables_sort() */calc form stable  where <\>\in\and\or order by limit offset )
        tdSql.query("select /*+ para_tables_sort() */5-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ distinct  c5_1 "
            sql += " from  ( select /*+ para_tables_sort() */  "
            sql += "%s " % random.choice(self.calc_select_in_ts)
            sql += " as c5_1 from stable_1 where "
            sql += "%s " % random.choice(self.qt_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #6-error select /*+ para_tables_sort() */* from (select /*+ para_tables_sort() */distinct(tag) form stable  where <\>\in\and\or order by limit  )
        tdSql.query("select /*+ para_tables_sort() */6-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ * from  ( select /*+ para_tables_sort() */ "
            sql += "%s " % random.choice(self.dt_select)
            sql += "  from stable_1 where "
            sql += "%s " % random.choice(self.qt_where)
            sql += "%s " % random.choice(self.order_desc_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.error(sql)
        tdSql.query("select /*+ para_tables_sort() */6-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ * from  ( select /*+ para_tables_sort() */ "
            sql += "%s " % random.choice(self.dt_select)
            sql += "  from stable_1 where "
            sql += "%s ) ;" % random.choice(self.qt_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #7-error select /*+ para_tables_sort() */* from (select /*+ para_tables_sort() */distinct(tag) form stable  where <\>\in\and\or order by limit  )
        tdSql.query("select /*+ para_tables_sort() */7-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ * from  ( select /*+ para_tables_sort() */ "
            sql += "%s " % random.choice(self.dq_select)
            sql += "  from stable_1 where "
            sql += "%s " % random.choice(self.qt_where)
            sql += "%s " % random.choice(self.order_desc_where)
            sql += "%s " % random.choice([self.limit_where[0] , self.limit_where[1]] )
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.error(sql)
        tdSql.query("select /*+ para_tables_sort() */7-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ * from  ( select /*+ para_tables_sort() */ "
            sql += "%s " % random.choice(self.dq_select)
            sql += "  from stable_1 where "
            sql += "%s " % random.choice(self.qt_where)
            sql += "%s " % random.choice([self.limit_where[0] , self.limit_where[1]] )
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            tdSql.checkRows(1)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #calc_select,TWA/Diff/Derivative/Irate are not allowed to apply to super table directly
        #8 select /*+ para_tables_sort() */* from (select /*+ para_tables_sort() */ts,calc form ragular_table  where <\>\in\and\or order by   )

        # dcDB = self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select /*+ para_tables_sort() */8-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ *  from  ( select /*+ para_tables_sort() */ ts ,"
            sql += "%s " % random.choice(self.calc_select_support_ts)
            sql += "from regular_table_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.order_where)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)
        tdSql.query("select /*+ para_tables_sort() */8-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ *  from  ( select /*+ para_tables_sort() */ "
            sql += "%s " % random.choice(self.calc_select_not_support_ts)
            sql += "from regular_table_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ *  from  ( select /*+ para_tables_sort() */ "
            sql += "%s " % random.choice(self.calc_select_in_ts)
            sql += "from regular_table_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */8-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ *  from  ( select /*+ para_tables_sort() */t1.ts, "
            sql += "%s " % random.choice(self.calc_select_in_support_ts_j)
            sql += "from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(self.q_u_where)
            sql += "%s " % random.choice(self.order_u_where)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ *  from  ( select /*+ para_tables_sort() */ "
            sql += "%s " % random.choice(self.calc_select_in_not_support_ts_j)
            sql += "from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(self.q_u_where)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #9 select /*+ para_tables_sort() */* from (select /*+ para_tables_sort() */ts,calc form stable  where <\>\in\and\or order by   )
        # self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select /*+ para_tables_sort() */9-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ *  from  ( select /*+ para_tables_sort() */ "
            sql += "%s " % random.choice(self.calc_select_not_support_ts)
            sql += "from stable_1 where "
            sql += "%s " % random.choice(self.qt_where)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)
        tdSql.query("select /*+ para_tables_sort() */9-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ *  from  ( select /*+ para_tables_sort() */ ts ,"
            sql += "%s " % random.choice(self.calc_select_support_ts)
            sql += "from stable_1 where "
            sql += "%s " % random.choice(self.qt_where)
            sql += "%s " % random.choice(self.order_where)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */9-3 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ *  from  ( select /*+ para_tables_sort() */ "
            sql += "%s " % random.choice(self.calc_select_in_not_support_ts_j)
            sql += "from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.t_join_where)
            sql += " and %s " % random.choice(self.qt_u_or_where)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)
        tdSql.query("select /*+ para_tables_sort() */9-4 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ *  from  ( select /*+ para_tables_sort() */ t1.ts,"
            sql += "%s " % random.choice(self.calc_select_in_support_ts_j)
            sql += "from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.t_join_where)
            sql += " and %s " % random.choice(self.qt_u_or_where)
            sql += "%s " % random.choice(self.order_u_where)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #10 select /*+ para_tables_sort() */calc from (select /*+ para_tables_sort() */* form regualr_table  where <\>\in\and\or order by   )
        tdSql.query("select /*+ para_tables_sort() */10-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ "
            sql += "%s " % random.choice(self.calc_select_in_ts)
            sql += "as calc10_1 from ( select /*+ para_tables_sort() */* from regular_table_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.order_desc_where)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #10-1 select /*+ para_tables_sort() */calc from (select /*+ para_tables_sort() */* form regualr_table  where <\>\in\and\or order by   )
        # rsDn = self.restartDnodes()
        # self.dropandcreateDB_random("%s" %db, 1)
        # rsDn = self.restartDnodes()
        tdSql.query("select /*+ para_tables_sort() */10-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ "
            sql += "%s " % random.choice(self.calc_select_all)
            sql += "as calc10_2 from ( select /*+ para_tables_sort() */* from regular_table_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.order_desc_where)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #10-2 select /*+ para_tables_sort() */calc from (select /*+ para_tables_sort() */* form regualr_tables  where <\>\in\and\or order by   )
        tdSql.query("select /*+ para_tables_sort() */10-3 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ "
            sql += "count(*) as calc10_3 "
            sql += " from ( select /*+ para_tables_sort() */t1.ts as t11, t2.ts as t22 from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.q_u_where)
            sql += " and %s " % random.choice(self.q_u_or_where)
            sql += "%s " % random.choice(self.order_u_where)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            sql += "%s ;" % random.choice(self.limit1_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */10-4 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ "
            sql += "%s as calc10_4 " % random.choice(self.calc_select_all)
            sql += " from ( select /*+ para_tables_sort() */* from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.q_u_or_where)
            sql += " and %s " % random.choice(self.q_u_or_where)
            sql += "%s " % random.choice(self.order_u_where)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            sql += "%s ;" % random.choice(self.limit_u_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.error(sql)

        #11 select /*+ para_tables_sort() */calc from (select /*+ para_tables_sort() */* form stable  where <\>\in\and\or order by limit  )
        tdSql.query("select /*+ para_tables_sort() */11-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  "
            sql += "%s " % random.choice(self.calc_select_in_ts)
            sql += "as calc11_1 from ( select /*+ para_tables_sort() */* from stable_1 where "
            sql += "%s " % random.choice(self.qt_where)
            sql += "%s " % random.choice(self.order_desc_where)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #11-1 select /*+ para_tables_sort() */calc from (select /*+ para_tables_sort() */* form stable  where <\>\in\and\or order by limit  )
        tdSql.query("select /*+ para_tables_sort() */11-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  "
            sql += "%s " % random.choice(self.calc_select_all)
            sql += "as calc11_1 from ( select /*+ para_tables_sort() */* from stable_1 where "
            sql += "%s " % random.choice(self.qt_where)
            sql += "%s " % random.choice(self.order_desc_where)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #11-2 select /*+ para_tables_sort() */calc from (select /*+ para_tables_sort() */* form stables  where <\>\in\and\or order by limit  )
        tdSql.query("select /*+ para_tables_sort() */11-3 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  "
            sql += "%s " % random.choice(self.calc_select_all)
            sql += "as calc11_1 from ( select /*+ para_tables_sort() */* from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.t_join_where)
            sql += "%s " % random.choice(self.order_u_where)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            sql += "%s ;" % random.choice(self.limit_u_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.error(sql)

        tdSql.query("select /*+ para_tables_sort() */11-4 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  "
            sql += "%s " % random.choice(self.calc_select_all)
            sql += "as calc11_1 from ( select /*+ para_tables_sort() */* from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.qt_u_or_where)
            sql += "%s " % random.choice(self.order_u_where)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            sql += "%s ;" % random.choice(self.limit_u_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.error(sql)

        #12 select /*+ para_tables_sort() */calc-diff from (select /*+ para_tables_sort() */* form regualr_table  where <\>\in\and\or order by limit  )
        ##self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select /*+ para_tables_sort() */12-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ "
            sql += "%s " % random.choice(self.calc_calculate_regular)
            sql += " from ( select /*+ para_tables_sort() */* from regular_table_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.order_desc_where)
            sql += "%s " % random.choice([self.limit_where[2] , self.limit_where[3]] )
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */12-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ "
            sql += "%s " % random.choice(self.calc_calculate_regular)
            sql += " from ( select /*+ para_tables_sort() */* from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.q_u_where)
            sql += "%s " % random.choice(self.order_u_where)
            sql += "%s " % random.choice([self.limit_where[2] , self.limit_where[3]] )
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.error(sql)

        tdSql.query("select /*+ para_tables_sort() */12-2.2 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ "
            sql += "%s " % random.choice(self.calc_calculate_regular)
            sql += " from ( select /*+ para_tables_sort() */* from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.q_u_or_where)
            sql += "%s " % random.choice(self.order_u_where)
            sql += "%s " % random.choice([self.limit_where[2] , self.limit_where[3]] )
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.error(sql)

        #12-1 select /*+ para_tables_sort() */calc-diff from (select /*+ para_tables_sort() */* form stable  where <\>\in\and\or order by limit  )
        tdSql.query("select /*+ para_tables_sort() */12-3 from stable_1;")
        self.restartDnodes()
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ * from ( select /*+ para_tables_sort() */"
            sql += "%s " % random.choice(self.calc_calculate_regular)
            sql += "  from stable_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.partiton_where)
            sql += ") "
            sql += "%s " % random.choice([self.limit_where[2] , self.limit_where[3]] )
            sql += " ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */12-4 from stable_1;")
        #join query does not support group by
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ * from ( select /*+ para_tables_sort() */"
            sql += "%s " % random.choice(self.calc_calculate_regular_j)
            sql += "  from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.t_join_where)
            sql += "%s " % random.choice(self.partiton_where_j)
            sql += ") "
            sql += "%s " % random.choice([self.limit_where[2] , self.limit_where[3]] )
            sql += " ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */12-5 from stable_1;")
        #join query does not support group by
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ * from ( select /*+ para_tables_sort() */"
            sql += "%s " % random.choice(self.calc_calculate_regular_j)
            sql += "  from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.qt_u_or_where)
            sql += ") "
            sql += "%s " % random.choice([self.limit_where[2] , self.limit_where[3]] )
            sql += " ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)


        #13 select /*+ para_tables_sort() */calc-diff as diffns from (select /*+ para_tables_sort() */* form stable  where <\>\in\and\or order by limit  )
        tdSql.query("select /*+ para_tables_sort() */13-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  "
            sql += "%s " % random.choice(self.calc_calculate_regular)
            sql += " as calc13_1 from ( select /*+ para_tables_sort() */* from stable_1 where "
            sql += "%s " % random.choice(self.qt_where)
            sql += "%s " % random.choice(self.orders_desc_where)
            sql += "%s " % random.choice([self.limit_where[2] , self.limit_where[3]] )
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #14 select /*+ para_tables_sort() */* from (select /*+ para_tables_sort() */calc_aggregate_alls as agg from stable  where <\>\in\and\or group by order by slimit soffset )
        tdSql.query("select /*+ para_tables_sort() */14-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */"
            sql += "%s as calc14_1, " % random.choice(self.calc_aggregate_all)
            sql += "%s as calc14_2, " % random.choice(self.calc_aggregate_all)
            sql += "%s " % random.choice(self.calc_aggregate_all)
            sql += " as calc14_3 from stable_1 where "
            sql += "%s " % random.choice(self.qt_where)
            sql += "%s " % random.choice(self.group_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        # error group by in out query
        tdSql.query("select /*+ para_tables_sort() */14-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */"
            sql += "%s as calc14_1, " % random.choice(self.calc_aggregate_all)
            sql += "%s as calc14_2, " % random.choice(self.calc_aggregate_all)
            sql += "%s " % random.choice(self.calc_aggregate_all)
            sql += " as calc14_3 from stable_1 where "
            sql += "%s " % random.choice(self.qt_where)
            sql += "%s " % random.choice(self.partiton_where_regular)
            sql += "%s " % random.choice(self.slimit1_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #14-2 select /*+ para_tables_sort() */* from (select /*+ para_tables_sort() */calc_aggregate_all_js as agg from stables  where <\>\in\and\or group by order by slimit soffset )
        tdSql.query("select /*+ para_tables_sort() */14-3 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */"
            sql += "%s as calc14_1, " % random.choice(self.calc_aggregate_all_j)
            sql += "%s as calc14_2, " % random.choice(self.calc_aggregate_all_j)
            sql += "%s " % random.choice(self.calc_aggregate_all_j)
            sql += " as calc14_3 from stable_1  t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.t_join_where)
            sql += "%s " % random.choice(self.partiton_where_j)
            sql += "%s " % random.choice(self.slimit1_where)
            sql += ") "
            sql += "%s ;" % random.choice(self.limit_u_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */14-4 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */"
            sql += "%s as calc14_1, " % random.choice(self.calc_aggregate_all_j)
            sql += "%s as calc14_2, " % random.choice(self.calc_aggregate_all_j)
            sql += "%s " % random.choice(self.calc_aggregate_all_j)
            sql += " as calc14_3 from stable_1  t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.qt_u_or_where)
            sql += "%s " % random.choice(self.partiton_where_j)
            sql += "%s " % random.choice(self.slimit1_where)
            sql += ") "
            sql += "%s ;" % random.choice(self.limit_u_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #15 select /*+ para_tables_sort() */* from (select /*+ para_tables_sort() */calc_aggregate_regulars as agg from regular_table  where <\>\in\and\or  order by slimit soffset )
        tdSql.query("select /*+ para_tables_sort() */15-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */"
            sql += "%s as calc15_1, " % random.choice(self.calc_aggregate_regular)
            sql += "%s as calc15_2, " % random.choice(self.calc_aggregate_regular)
            sql += "%s " % random.choice(self.calc_aggregate_regular)
            sql += " as calc15_3 from regular_table_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.group_where_regular)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            self.data_check(sql,mark='15-1')

        tdSql.query("select /*+ para_tables_sort() */15-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */"
            sql += "%s as calc15_2 " % random.choice(self.calc_aggregate_regular_j)
            sql += "from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.q_u_where)
            sql += "%s " % random.choice(self.group_where_regular_j)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            sql += "%s ;" % random.choice(self.limit_u_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            self.data_check(sql,mark='15-2')

        tdSql.query("select /*+ para_tables_sort() */15-2.2 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */"
            sql += "%s as calc15_1, " % random.choice(self.calc_aggregate_regular_j)
            sql += "%s as calc15_2, " % random.choice(self.calc_aggregate_regular_j)
            sql += "%s " % random.choice(self.calc_aggregate_regular_j)
            sql += " as calc15_3 from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.q_u_or_where)
            sql += "%s " % random.choice(self.group_where_regular_j)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            sql += "%s ;" % random.choice(self.limit_u_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            self.data_check(sql,mark='15-2.2')

        self.restartDnodes()
        tdSql.query("select /*+ para_tables_sort() */15-3 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */"
            sql += "%s as calc15_1, " % random.choice(self.calc_aggregate_groupbytbname)
            sql += "%s as calc15_2, " % random.choice(self.calc_aggregate_groupbytbname)
            sql += "%s " % random.choice(self.calc_aggregate_groupbytbname)
            sql += " as calc15_3 from stable_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.group_only_where)
            sql += "%s " % random.choice(self.having_support)
            sql += ") "
            sql += "order by  calc15_1  "
            sql += "%s " % random.choice(self.limit_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            self.data_check(sql,mark='15-3')

        tdSql.query("select /*+ para_tables_sort() */15-4 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */"
            sql += "%s as calc15_1, " % random.choice(self.calc_aggregate_groupbytbname_j)
            sql += "%s as calc15_2, " % random.choice(self.calc_aggregate_groupbytbname_j)
            sql += "%s " % random.choice(self.calc_aggregate_groupbytbname_j)
            sql += " as calc15_3 from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and   "
            sql += "%s " % random.choice(self.t_join_where)
            sql += "%s " % random.choice(self.group_only_where_j)
            sql += "%s " % random.choice(self.having_support_j)
            sql += ") "
            sql += "order by  calc15_1  "
            sql += "%s " % random.choice(self.limit_u_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            self.data_check(sql,mark='15-4')

        tdSql.query("select /*+ para_tables_sort() */15-4.2 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */"
            sql += "%s as calc15_1, " % random.choice(self.calc_aggregate_groupbytbname_j)
            sql += "%s as calc15_2, " % random.choice(self.calc_aggregate_groupbytbname_j)
            sql += "%s " % random.choice(self.calc_aggregate_groupbytbname_j)
            sql += " as calc15_3 from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and   "
            sql += "%s " % random.choice(self.qt_u_or_where)
            sql += "%s " % random.choice(self.group_where_j)
            sql += ") "
            sql += "order by  calc15_1  "
            sql += "%s " % random.choice(self.limit_u_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            self.data_check(sql,mark='15-4.2')

        tdSql.query("select /*+ para_tables_sort() */15-5 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */"
            sql += "%s as calc15_1, " % random.choice(self.calc_aggregate_groupbytbname)
            sql += "%s as calc15_2, " % random.choice(self.calc_aggregate_groupbytbname)
            sql += "%s " % random.choice(self.calc_aggregate_groupbytbname)
            sql += " as calc15_3 from stable_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.group_where)
            sql += ") "
            sql += "order by calc15_1  "
            sql += "%s " % random.choice(self.limit_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.data_check(sql,mark='15-5')

        #16 select /*+ para_tables_sort() */* from (select /*+ para_tables_sort() */calc_aggregate_regulars as agg from regular_table  where <\>\in\and\or  order by limit offset )
        #self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select /*+ para_tables_sort() */16-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */ "
            sql += "%s as calc16_0 , " % random.choice(self.calc_calculate_all)
            sql += "%s as calc16_1 , " % random.choice(self.calc_aggregate_all)
            sql += "%s as calc16_2 " % random.choice(self.calc_select_in)
            sql += "  from stable_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.group_where)
            sql += ") "
            sql += "order by calc16_0  "
            sql += "%s " % random.choice(self.limit1_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */16-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */ "
            sql += "%s as calc16_0  " % random.choice(self.calc_calculate_all_j)
            sql += ", %s as calc16_1  " % random.choice(self.calc_aggregate_all_j)
            sql += "  from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.t_join_where)
            sql += ") "
            sql += "order by calc16_0  "
            sql += "%s " % random.choice(self.limit1_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */16-2.2 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */ "
            sql += "%s as calc16_0  " % random.choice(self.calc_calculate_all_j)
            sql += ", %s as calc16_1  " % random.choice(self.calc_aggregate_all_j)
            sql += "  from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.qt_u_or_where)
            sql += ") "
            sql += "order by calc16_0  "
            sql += "%s " % random.choice(self.limit1_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */16-3 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */ "
            sql += "%s as calc16_1  " % random.choice(self.calc_calculate_regular)
            sql += "  from regular_table_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "limit 2 ) "
            sql += "%s " % random.choice(self.limit1_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */16-4 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */ "
            sql += "%s as calc16_1  " % random.choice(self.calc_calculate_regular_j)
            sql += "  from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.q_u_where)
            sql += "limit 2 ) "
            sql += "%s " % random.choice(self.limit1_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */16-4.2 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */ "
            sql += "%s as calc16_1  " % random.choice(self.calc_calculate_regular_j)
            sql += "  from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.q_u_or_where)
            sql += "limit 2 ) "
            sql += "%s " % random.choice(self.limit1_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */16-5 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */ "
            sql += "%s as calc16_1 , " % random.choice(self.calc_calculate_all)
            sql += "%s as calc16_2 , " % random.choice(self.calc_calculate_all)
            sql += "%s as calc16_3 " % random.choice(self.calc_calculate_all)
            sql += "  from stable_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.group_where)
            sql += ") "
            sql += "order by calc16_1  "
            sql += "%s " % random.choice(self.limit1_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */16-6 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */ "
            sql += "%s as calc16_1  " % random.choice(self.calc_calculate_groupbytbname)
            sql += "  from stable_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.partiton_where)
            sql += "limit 2 ) "
            sql += "%s " % random.choice(self.limit1_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */16-7 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */ "
            sql += "%s as calc16_1  " % random.choice(self.calc_calculate_groupbytbname_j)
            sql += "  from stable_1  t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.t_join_where)
            sql += "limit 2 ) "
            sql += "%s " % random.choice(self.limit1_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */16-8 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */ "
            sql += "%s as calc16_1  " % random.choice(self.calc_calculate_groupbytbname_j)
            sql += "  from stable_1  t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.qt_u_or_where)
            sql += "limit 2 ) "
            sql += "%s " % random.choice(self.limit1_where)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #17 select /*+ para_tables_sort() */apercentile from (select /*+ para_tables_sort() */calc_aggregate_alls form regualr_table or stable  where <\>\in\and\or interval_sliding group by having order by limit offset  )interval_sliding
        #self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select /*+ para_tables_sort() */17-1 from stable_1;")
        for i in range(self.fornum):
            #this is having_support , but tag-select /*+ para_tables_sort() */cannot mix with last_row,other select /*+ para_tables_sort() */can
            sql = "select /*+ para_tables_sort() */  apercentile(cal17_0, %d)/10 ,apercentile(cal17_1, %d)/1000 ,apercentile(cal17_2, %d)*10+%d from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal17_0 , " % random.choice(self.calc_calculate_all)
            sql += "%s as cal17_1 ," % random.choice(self.calc_aggregate_all)
            sql += "%s as cal17_2 " % random.choice(self.calc_aggregate_all)
            sql += " from stable_1 where "
            sql += "%s " % random.choice(self.qt_where)
            sql += "%s " % random.choice(self.partiton_where)
            sql += "%s " % random.choice(self.interval_sliding)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */17-2 from stable_1;")
        for i in range(self.fornum):
            #this is having_support , but tag-select /*+ para_tables_sort() */cannot mix with last_row,other select /*+ para_tables_sort() */can
            sql = "select /*+ para_tables_sort() */  apercentile(cal17_0, %d)/10 ,apercentile(cal17_1, %d)/1000 ,apercentile(cal17_2, %d)*10+%d from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal17_0 , " % random.choice(self.calc_calculate_all_j)
            sql += "%s as cal17_1 ," % random.choice(self.calc_aggregate_all_j)
            sql += "%s as cal17_2 " % random.choice(self.calc_aggregate_all_j)
            sql += " from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(self.t_join_where)
            sql += "%s " % random.choice(self.interval_sliding)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */17-2.2 from stable_1;")
        for i in range(self.fornum):
            #this is having_support , but tag-select /*+ para_tables_sort() */cannot mix with last_row,other select /*+ para_tables_sort() */can
            sql = "select /*+ para_tables_sort() */  apercentile(cal17_0, %d)/10 ,apercentile(cal17_1, %d)/1000 ,apercentile(cal17_2, %d)*10+%d from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal17_0 , " % random.choice(self.calc_calculate_all_j)
            sql += "%s as cal17_1 ," % random.choice(self.calc_aggregate_all_j)
            sql += "%s as cal17_2 " % random.choice(self.calc_aggregate_all_j)
            sql += " from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(self.qt_u_or_where)
            sql += "%s " % random.choice(self.interval_sliding)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        self.restartDnodes()
        tdSql.query("select /*+ para_tables_sort() */17-3 from stable_1;")
        for i in range(self.fornum):
            #this is having_tagnot_support , because tag-select /*+ para_tables_sort() */cannot mix with last_row...
            sql = "select /*+ para_tables_sort() */  apercentile(cal17_1, %d)/1000 ,apercentile(cal17_2, %d)*10+%d from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal17_1 ," % random.choice(self.calc_aggregate_all)
            sql += "%s as cal17_2 " % random.choice(self.calc_aggregate_all)
            sql += " from stable_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.partiton_where)
            sql += "%s " % random.choice(self.interval_sliding)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */17-4 from stable_1;")
        for i in range(self.fornum):
            #this is having_tagnot_support , because tag-select /*+ para_tables_sort() */cannot mix with last_row...
            sql = "select /*+ para_tables_sort() */  apercentile(cal17_1, %d)/1000 ,apercentile(cal17_2, %d)*10+%d from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal17_1 ," % random.choice(self.calc_aggregate_all_j)
            sql += "%s as cal17_2 " % random.choice(self.calc_aggregate_all_j)
            sql += " from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.t_join_where)
            sql += "%s " % random.choice(self.interval_sliding)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */17-4.2 from stable_1;")
        for i in range(self.fornum):
            #this is having_tagnot_support , because tag-select /*+ para_tables_sort() */cannot mix with last_row...
            sql = "select /*+ para_tables_sort() */  apercentile(cal17_1, %d)/1000 ,apercentile(cal17_2, %d)*10+%d from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal17_1 ," % random.choice(self.calc_aggregate_all_j)
            sql += "%s as cal17_2 " % random.choice(self.calc_aggregate_all_j)
            sql += " from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.qt_u_or_where)
            sql += "%s " % random.choice(self.interval_sliding)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */17-5 from stable_1;")
        for i in range(self.fornum):
            #having_not_support
            sql = "select /*+ para_tables_sort() */  apercentile(cal17_1, %d)/1000 ,apercentile(cal17_2, %d)*10+%d from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal17_1 ," % random.choice(self.calc_aggregate_all)
            sql += "%s as cal17_2 " % random.choice(self.calc_aggregate_all)
            sql += " from stable_1 where "
            sql += "%s " % random.choice(self.qt_where)
            sql += "%s " % random.choice(self.partiton_where)
            sql += "%s " % random.choice(self.interval_sliding)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */17-6 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  apercentile(cal17_1, %d)/1000 ,apercentile(cal17_2, %d)*10+%d from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal17_1 ," % random.choice(self.calc_aggregate_all)
            sql += "%s as cal17_2 " % random.choice(self.calc_aggregate_all)
            sql += " from stable_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.interval_sliding)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */17-7 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  apercentile(cal17_1, %d)/1000 ,apercentile(cal17_2, %d)*10+%d from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal17_1 ," % random.choice(self.calc_aggregate_all_j)
            sql += "%s as cal17_2 " % random.choice(self.calc_aggregate_all_j)
            sql += " from stable_1_1 t1, stable_1_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(self.q_u_where)
            sql += "%s " % random.choice(self.interval_sliding)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */17-7.2 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  apercentile(cal17_1, %d)/1000 ,apercentile(cal17_2, %d)*10+%d from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal17_1 ," % random.choice(self.calc_aggregate_all_j)
            sql += "%s as cal17_2 " % random.choice(self.calc_aggregate_all_j)
            sql += " from stable_1_1 t1, stable_1_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(self.q_u_or_where)
            sql += "%s " % random.choice(self.interval_sliding)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        self.restartDnodes()
        tdSql.query("select /*+ para_tables_sort() */17-8 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  apercentile(cal17_1, %d)/1000 ,apercentile(cal17_2, %d)*10+%d  from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal17_1 ," % random.choice(self.calc_aggregate_all)
            sql += "%s as cal17_2 " % random.choice(self.calc_aggregate_all)
            sql += " from regular_table_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.interval_sliding)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */17-9 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  apercentile(cal17_1, %d)/1000 ,apercentile(cal17_2, %d)*10+%d  from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal17_1 ," % random.choice(self.calc_aggregate_all_j)
            sql += "%s as cal17_2 " % random.choice(self.calc_aggregate_all_j)
            sql += " from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.q_u_where)
            sql += "%s " % random.choice(self.interval_sliding)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */17-10 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  apercentile(cal17_1, %d)/1000 ,apercentile(cal17_2, %d)*10+%d  from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal17_1 ," % random.choice(self.calc_aggregate_all_j)
            sql += "%s as cal17_2 " % random.choice(self.calc_aggregate_all_j)
            sql += " from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.q_u_or_where)
            sql += "%s " % random.choice(self.interval_sliding)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #18 select /*+ para_tables_sort() */apercentile from (select /*+ para_tables_sort() */calc_aggregate_alls form regualr_table or stable  where <\>\in\and\or session order by  limit )interval_sliding
        tdSql.query("select /*+ para_tables_sort() */18-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  apercentile(cal18_1, %d)/1000 ,apercentile(cal18_2, %d)*10+%d  from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal18_1 ," % random.choice(self.calc_aggregate_all)
            sql += "%s as cal18_2 " % random.choice(self.calc_aggregate_all)
            sql += " from regular_table_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.session_where)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */18-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  apercentile(cal18_1, %d)/1000 ,apercentile(cal18_2, %d)*10+%d  from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal18_1 ," % random.choice(self.calc_aggregate_all_j)
            sql += "%s as cal18_2 " % random.choice(self.calc_aggregate_all_j)
            sql += " from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and   "
            sql += "%s " % random.choice(self.q_u_where)
            sql += "%s " % random.choice(self.session_u_where)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */18-2.2 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  apercentile(cal18_1, %d)/1000 ,apercentile(cal18_2, %d)*10+%d  from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal18_1 ," % random.choice(self.calc_aggregate_all_j)
            sql += "%s as cal18_2 " % random.choice(self.calc_aggregate_all_j)
            sql += " from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and   "
            sql += "%s " % random.choice(self.q_u_or_where)
            sql += "%s " % random.choice(self.session_u_where)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        self.restartDnodes()
        tdSql.query("select /*+ para_tables_sort() */18-3 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  apercentile(cal18_1, %d)/1000 ,apercentile(cal18_2, %d)*10+%d  from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal18_1 ," % random.choice(self.calc_aggregate_all)
            sql += "%s as cal18_2 " % random.choice(self.calc_aggregate_all)
            sql += " from stable_1_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.session_where)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */18-4 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  apercentile(cal18_1, %d)/1000 ,apercentile(cal18_2, %d)*10+%d  from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal18_1 ," % random.choice(self.calc_aggregate_all_j)
            sql += "%s as cal18_2 " % random.choice(self.calc_aggregate_all_j)
            sql += " from stable_1_1 t1, regular_table_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.q_u_where)
            sql += "%s " % random.choice(self.session_u_where)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */18-4.2 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  apercentile(cal18_1, %d)/1000 ,apercentile(cal18_2, %d)*10+%d  from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal18_1 ," % random.choice(self.calc_aggregate_all_j)
            sql += "%s as cal18_2 " % random.choice(self.calc_aggregate_all_j)
            sql += " from stable_1_1 t1, regular_table_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.q_u_or_where)
            sql += "%s " % random.choice(self.session_u_where)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */18-5 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  apercentile(cal18_1, %d)/1000 ,apercentile(cal18_2, %d)*10+%d  from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal18_1 ," % random.choice(self.calc_aggregate_all)
            sql += "%s as cal18_2 " % random.choice(self.calc_aggregate_all)
            sql += " from stable_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.session_where)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */18-6 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  apercentile(cal18_1, %d)/1000 ,apercentile(cal18_2, %d)*10+%d  from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal18_1 ," % random.choice(self.calc_aggregate_all_j)
            sql += "%s as cal18_2 " % random.choice(self.calc_aggregate_all_j)
            sql += " from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and   "
            sql += "%s " % random.choice(self.t_join_where)
            sql += "%s " % random.choice(self.session_u_where)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */18-7 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  apercentile(cal18_1, %d)/1000 ,apercentile(cal18_2, %d)*10+%d  from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal18_1 ," % random.choice(self.calc_aggregate_all_j)
            sql += "%s as cal18_2 " % random.choice(self.calc_aggregate_all_j)
            sql += " from stable_1 t1, stable_2 t2 where t1.ts = t2.ts and   "
            sql += "%s " % random.choice(self.qt_u_or_where)
            sql += "%s " % random.choice(self.session_u_where)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #19 select /*+ para_tables_sort() */apercentile from (select /*+ para_tables_sort() */calc_aggregate_alls form regualr_table or stable  where <\>\in\and\or session order by  limit )interval_sliding
        #self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select /*+ para_tables_sort() */19-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  apercentile(cal19_1, %d)/1000 ,apercentile(cal19_2, %d)*10+%d  from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal19_1 ," % random.choice(self.calc_aggregate_all)
            sql += "%s as cal19_2 " % random.choice(self.calc_aggregate_all)
            sql += " from regular_table_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.state_window)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */19-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  apercentile(cal19_1, %d)/1000 ,apercentile(cal19_2, %d)*10+%d  from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal19_1 ," % random.choice(self.calc_aggregate_all_j)
            sql += "%s as cal19_2 " % random.choice(self.calc_aggregate_all_j)
            sql += " from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and   "
            sql += "%s " % random.choice(self.q_u_where)
            sql += "%s " % random.choice(self.state_u_window)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */19-2.2 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  apercentile(cal19_1, %d)/1000 ,apercentile(cal19_2, %d)*10+%d  from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal19_1 ," % random.choice(self.calc_aggregate_all_j)
            sql += "%s as cal19_2 " % random.choice(self.calc_aggregate_all_j)
            sql += " from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and   "
            sql += "%s " % random.choice(self.q_u_or_where)
            sql += "%s " % random.choice(self.state_u_window)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */19-3 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  apercentile(cal19_1, %d)/1000 ,apercentile(cal19_2, %d)*10+%d  from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal19_1 ," % random.choice(self.calc_aggregate_all)
            sql += "%s as cal19_2 " % random.choice(self.calc_aggregate_all)
            sql += " from stable_1_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.state_window)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */19-4 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  apercentile(cal19_1, %d)/1000 ,apercentile(cal19_2, %d)*10+%d  from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal19_1 ," % random.choice(self.calc_aggregate_all_j)
            sql += "%s as cal19_2 " % random.choice(self.calc_aggregate_all_j)
            sql += " from stable_1_1  t1, stable_1_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.q_u_where)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */19-4.2 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  apercentile(cal19_1, %d)/1000 ,apercentile(cal19_2, %d)*10+%d  from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal19_1 ," % random.choice(self.calc_aggregate_all_j)
            sql += "%s as cal19_2 " % random.choice(self.calc_aggregate_all_j)
            sql += " from stable_1_1  t1, stable_1_2 t2 where t1.ts = t2.ts and  "
            sql += "%s " % random.choice(self.q_u_or_where)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */19-5 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  apercentile(cal19_1, %d)/1000 ,apercentile(cal19_2, %d)*10+%d  from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal19_1 ," % random.choice(self.calc_aggregate_all)
            sql += "%s as cal19_2 " % random.choice(self.calc_aggregate_all)
            sql += " from stable_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += "%s " % random.choice(self.state_window)
            sql += "%s " % random.choice(self.order_where)
            sql += "%s " % random.choice(self.limit1_where)
            sql += ") "
            sql += "%s " % random.choice(self.interval_sliding)
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.error(sql)

        tdSql.query("select /*+ para_tables_sort() */19-6 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  apercentile(cal19_1, %d)/1000 ,apercentile(cal19_2, %d)*10+%d  from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal19_1 ," % random.choice(self.calc_aggregate_all_j)
            sql += "%s as cal19_2 " % random.choice(self.calc_aggregate_all_j)
            sql += " from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(self.q_u_where)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */19-7 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  apercentile(cal19_1, %d)/1000 ,apercentile(cal19_2, %d)*10+%d  from  ( select /*+ para_tables_sort() */ " %(random.randint(0,100) , random.randint(0,100) ,random.randint(-1000,1000))
            sql += "%s as cal19_1 ," % random.choice(self.calc_aggregate_all_j)
            sql += "%s as cal19_2 " % random.choice(self.calc_aggregate_all_j)
            sql += " from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and "
            sql += "%s " % random.choice(self.qt_u_or_where)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #20 select /*+ para_tables_sort() */* from (select /*+ para_tables_sort() */calc_select_fills form regualr_table or stable  where <\>\in\and\or fill_where group by  order by limit offset  )
        #self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select /*+ para_tables_sort() */20-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */ "
            sql += "%s , " % random.choice(self.calc_select_fill)
            sql += "%s ," % random.choice(self.calc_select_fill)
            sql += "%s  " % random.choice(self.calc_select_fill)
            sql += " from stable_1 where  "
            sql += "%s " % random.choice(self.interp_where)
            sql += "%s " % random.choice(self.fill_where)
            sql += "%s " % random.choice(self.group_where)
            sql += "%s " % random.choice(self.order_where)
            sql += "%s " % random.choice(self.limit_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            #interp不支持 tdSql.query(sql)
            #self.cur1.execute(sql)
            #self.explain_sql(sql)

        rsDn = self.restartDnodes()
        tdSql.query("select /*+ para_tables_sort() */20-2 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */ "
            sql += "%s , " % random.choice(self.calc_select_fill_j)
            sql += "%s ," % random.choice(self.calc_select_fill_j)
            sql += "%s  " % random.choice(self.calc_select_fill_j)
            sql += " from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s and " % random.choice(self.t_join_where)
            sql += "%s " % random.choice(self.interp_where_j)
            sql += "%s " % random.choice(self.order_u_where)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            #interp不支持 tdSql.query(sql)
            #self.cur1.execute(sql)
            #self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */20-2.2 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */ "
            sql += "%s , " % random.choice(self.calc_select_fill_j)
            sql += "%s ," % random.choice(self.calc_select_fill_j)
            sql += "%s  " % random.choice(self.calc_select_fill_j)
            sql += " from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and  "
            sql += "%s and " % random.choice(self.qt_u_or_where)
            sql += "%s " % random.choice(self.interp_where_j)
            sql += "%s " % random.choice(self.order_u_where)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            #interp不支持 tdSql.query(sql)
            #self.cur1.execute(sql)
            #self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */20-3 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */ "
            sql += "%s , " % random.choice(self.calc_select_fill)
            sql += "%s ," % random.choice(self.calc_select_fill)
            sql += "%s  " % random.choice(self.calc_select_fill)
            sql += " from stable_1 where  "
            sql += "%s " % self.interp_where[2]
            sql += "%s " % random.choice(self.fill_where)
            sql += "%s " % random.choice(self.order_where)
            sql += "%s " % random.choice(self.limit_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            #interp不支持 tdSql.query(sql)
            #self.cur1.execute(sql)
            #self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */20-4 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */ "
            sql += "%s , " % random.choice(self.calc_select_fill_j)
            sql += "%s ," % random.choice(self.calc_select_fill_j)
            sql += "%s  " % random.choice(self.calc_select_fill_j)
            sql += " from stable_1 t1, table_1 t2 where t1.ts = t2.ts and    "
            #sql += "%s and " % random.choice(self.t_join_where)
            sql += "%s " % self.interp_where_j[random.randint(0,5)]
            sql += "%s " % random.choice(self.order_u_where)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            #interp不支持 tdSql.query(sql)
            #self.cur1.execute(sql)
            #self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */20-4.2 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */ "
            sql += "%s , " % random.choice(self.calc_select_fill_j)
            sql += "%s ," % random.choice(self.calc_select_fill_j)
            sql += "%s  " % random.choice(self.calc_select_fill_j)
            sql += " from stable_1 t1, stable_1_1 t2 where t1.ts = t2.ts and    "
            sql += "%s and " % random.choice(self.qt_u_or_where)
            sql += "%s " % self.interp_where_j[random.randint(0,5)]
            sql += "%s " % random.choice(self.fill_where)
            sql += "%s " % random.choice(self.order_u_where)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
         ##interp不支持    tdSql.error(sql)
            #self.cur1.execute(sql)
            #self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */20-5 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */ "
            sql += "%s , " % random.choice(self.calc_select_fill)
            sql += "%s ," % random.choice(self.calc_select_fill)
            sql += "%s  " % random.choice(self.calc_select_fill)
            sql += " from regular_table_1 where  "
            sql += "%s " % self.interp_where[1]
            sql += "%s " % random.choice(self.fill_where)
            sql += "%s " % random.choice(self.order_where)
            sql += "%s " % random.choice(self.limit_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            ##interp不支持 tdSql.query(sql)
            #self.cur1.execute(sql)
            #self.explain_sql(sql)

        tdSql.query("select /*+ para_tables_sort() */20-6 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  * from  ( select /*+ para_tables_sort() */ "
            sql += "%s , " % random.choice(self.calc_select_fill_j)
            sql += "%s ," % random.choice(self.calc_select_fill_j)
            sql += "%s  " % random.choice(self.calc_select_fill_j)
            sql += " from regular_table_1 t1, regular_table_2 t2 where t1.ts = t2.ts and   "
            #sql += "%s " % random.choice(self.interp_where_j)
            sql += "%s " % self.interp_where_j[random.randint(0,5)]
            sql += "%s " % random.choice(self.order_u_where)
            sql += "%s " % random.choice(self.limit_u_where)
            sql += ") "
            tdLog.info(sql)
            tdLog.info(len(sql))
            ##interp不支持 tdSql.query(sql)
            #self.cur1.execute(sql)
            #self.explain_sql(sql)

        #1 select /*+ para_tables_sort() */* from (select /*+ para_tables_sort() */* from (select /*+ para_tables_sort() */* form regular_table  where <\>\in\and\or order by limit  ))
        tdSql.query("select /*+ para_tables_sort() */1-1 from stable_1;")
        for i in range(self.fornum):
            # sql_start = "select /*+ para_tables_sort() */ *  from ( "
            # sql_end = ")"
            for_num = random.randint(1, 15);
            sql = "select /*+ para_tables_sort() */ *  from ("  * for_num
            sql += "select /*+ para_tables_sort() */ *  from  ( select /*+ para_tables_sort() */* from ( select /*+ para_tables_sort() */ "
            sql += "%s, " % random.choice(self.s_r_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "ts as ttt from regular_table_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += ")) "
            sql += ")"  * for_num
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

            sql2 =  "select /*+ para_tables_sort() */ *  from  ( select /*+ para_tables_sort() */* from ( select /*+ para_tables_sort() */ "
            sql2 += "%s, " % random.choice(self.s_r_select)
            sql2 += "%s, " % random.choice(self.q_select)
            sql2 += "ts as tin from regular_table_1 where "
            sql2 += "%s " % random.choice(self.q_where)
            sql2 += ")) "
            tdLog.info(sql2)
            tdLog.info(len(sql2))
            tdSql.query(sql2)
            self.cur1.execute(sql2)
            self.explain_sql(sql2)

            self.data_matrix_equal('%s' %sql ,1,10,1,1,'%s' %sql2 ,1,10,1,1)
            self.data_matrix_equal('%s' %sql ,1,10,1,1,'%s' %sql ,1,10,3,3)
            self.data_matrix_equal('%s' %sql ,1,10,3,3,'%s' %sql2 ,1,10,3,3)

            tdLog.info("=====1-1==over=========")

        for i in range(self.fornum):
            for_num = random.randint(1, 15);
            sql = "select /*+ para_tables_sort() */ ts2  from ("  * for_num
            sql += "select /*+ para_tables_sort() */ *  from  ( select /*+ para_tables_sort() */* from ( select /*+ para_tables_sort() */ "
            sql += "%s, " % random.choice(self.s_r_select)
            sql += "%s, " % random.choice(self.q_select)
            sql += "ts as ts2 from regular_table_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += ")) "
            sql += ")"  * for_num
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

            sql2 =  "select /*+ para_tables_sort() */ *  from  ( select /*+ para_tables_sort() */* from ( select /*+ para_tables_sort() */ "
            sql2 += "%s, " % random.choice(self.s_r_select)
            sql2 += "%s, " % random.choice(self.q_select)
            sql2 += "ts as tt from regular_table_1 where "
            sql2 += "%s " % random.choice(self.q_where)
            sql2 += ")) "
            tdLog.info(sql2)
            tdLog.info(len(sql2))
            tdSql.query(sql2)
            self.cur1.execute(sql2)
            self.explain_sql(sql2)

            self.data_matrix_equal('%s' %sql ,1,10,1,1,'%s' %sql2 ,1,10,1,1)
            tdLog.info("=====1-2==over=========")

        #2 select /*+ para_tables_sort() */* from (select /*+ para_tables_sort() */* from (select /*+ para_tables_sort() */* form stable  where <\>\in\and\or order by limit  ))
        tdSql.query("select /*+ para_tables_sort() */2-1 from stable_1;")
        for i in range(self.fornum):
            for_num = random.randint(1, 15);
            sql = "select /*+ para_tables_sort() */ *  from ("  * for_num
            sql += "select /*+ para_tables_sort() */ *  from  ( select /*+ para_tables_sort() */* from ( select /*+ para_tables_sort() */ "
            sql += "%s, " % random.choice(self.s_s_select)
            sql += "%s, " % random.choice(self.qt_select)
            sql += "ts as tss from stable_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += ")) "
            sql += ")"  * for_num
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

            sql2 = "select /*+ para_tables_sort() */ *  from  ( select /*+ para_tables_sort() */* from ( select /*+ para_tables_sort() */ "
            sql2 += "%s, " % random.choice(self.s_s_select)
            sql2 += "%s, " % random.choice(self.qt_select)
            sql2 += "ts as tst from stable_1 where "
            sql2 += "%s " % random.choice(self.q_where)
            sql2 += ")) "
            tdLog.info(sql2)
            tdLog.info(len(sql2))
            tdSql.query(sql2)
            self.cur1.execute(sql2)
            self.explain_sql(sql2)

            self.data_matrix_equal('%s' %sql ,1,10,3,3,'%s' %sql2 ,1,10,3,3)

            tdLog.info("=====2-1==over=========")

        for i in range(self.fornum):
            for_num = random.randint(1, 15);
            sql = "select /*+ para_tables_sort() */ tsn  from ("  * for_num
            sql += "select /*+ para_tables_sort() */ *  from  ( select /*+ para_tables_sort() */* from ( select /*+ para_tables_sort() */ "
            sql += "%s, " % random.choice(self.s_s_select)
            sql += "%s, " % random.choice(self.qt_select)
            sql += "ts as tsn from stable_1 where "
            sql += "%s " % random.choice(self.q_where)
            sql += ")) "
            sql += ")"  * for_num
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

            sql2 = "select /*+ para_tables_sort() */ ts1  from  ( select /*+ para_tables_sort() */* from ( select /*+ para_tables_sort() */ "
            sql2 += "%s, " % random.choice(self.s_s_select)
            sql2 += "%s, " % random.choice(self.qt_select)
            sql2 += "ts as ts1 from stable_1 where "
            sql2 += "%s " % random.choice(self.q_where)
            sql2 += ")) "
            tdLog.info(sql2)
            tdLog.info(len(sql2))
            tdSql.query(sql2)
            self.cur1.execute(sql2)
            self.explain_sql(sql2)

            self.data_matrix_equal('%s' %sql ,1,10,1,1,'%s' %sql2 ,1,10,1,1)
            tdLog.info("=====2-2==over=========")

        #3 select /*+ para_tables_sort() */ts ,calc from  (select /*+ para_tables_sort() */* form stable  where <\>\in\and\or order by limit  )
        #self.dropandcreateDB_random("%s" %db, 1)
        tdSql.query("select /*+ para_tables_sort() */3-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  "
            sql += "%s " % random.choice(self.calc_calculate_regular)
            sql += " from ( select /*+ para_tables_sort() */* from stable_1 where "
            sql += "%s " % random.choice(self.qt_where)
            sql += "%s " % random.choice(self.orders_desc_where)
            sql += "%s " % random.choice(self.limit_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #4 select /*+ para_tables_sort() */* from  (select /*+ para_tables_sort() */calc form stable  where <\>\in\and\or order by limit  )
        tdSql.query("select /*+ para_tables_sort() */4-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */ *  from  ( select /*+ para_tables_sort() */ "
            sql += "%s " % random.choice(self.calc_select_in_ts)
            sql += "from stable_1 where "
            sql += "%s " % random.choice(self.qt_where)
            #sql += "%s " % random.choice(self.order_desc_where)
            sql += "%s " % random.choice(self.limit_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #5 select /*+ para_tables_sort() */ts ,tbname from  (select /*+ para_tables_sort() */* form stable  where <\>\in\and\or order by limit  )
        tdSql.query("select /*+ para_tables_sort() */5-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */  ts , tbname , "
            sql += "%s ," % random.choice(self.calc_calculate_regular)
            sql += "%s ," % random.choice(self.dqt_select)
            sql += "%s " % random.choice(self.qt_select)
            sql += " from ( select /*+ para_tables_sort() */* from stable_1 where "
            sql += "%s " % random.choice(self.qt_where)
            sql += "%s " % random.choice(self.orders_desc_where)
            sql += "%s " % random.choice(self.limit_where)
            sql += ") ;"
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.error(sql)

        #6 
        tdSql.query("select /*+ para_tables_sort() */6-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */count(*) from (select /*+ para_tables_sort() */avg(q_int)/1000 from stable_1); "
            tdLog.info(sql)
            tdLog.info(len(sql))
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #special sql
        tdSql.query("select /*+ para_tables_sort() */7-1 from stable_1;")
        for i in range(self.fornum):
            sql = "select /*+ para_tables_sort() */* from ( select /*+ para_tables_sort() */_block_dist() from stable_1);"
            tdSql.error(sql)
            sql = "select /*+ para_tables_sort() */_block_dist() from (select /*+ para_tables_sort() */* from stable_1);"
            tdSql.error(sql)
            sql = "select /*+ para_tables_sort() */* from (select /*+ para_tables_sort() */database());"
            tdLog.info(sql)
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)
            sql = "select /*+ para_tables_sort() */* from (select /*+ para_tables_sort() */client_version());"
            tdLog.info(sql)
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)
            sql = "select /*+ para_tables_sort() */* from (select /*+ para_tables_sort() */client_version() as version);"
            tdLog.info(sql)
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)
            sql = "select /*+ para_tables_sort() */* from (select /*+ para_tables_sort() */server_version());"
            tdLog.info(sql)
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)
            sql = "select /*+ para_tables_sort() */* from (select /*+ para_tables_sort() */server_version() as version);"
            tdLog.info(sql)
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)
            sql = "select /*+ para_tables_sort() */* from (select /*+ para_tables_sort() */server_status());"
            tdLog.info(sql)
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)
            sql = "select /*+ para_tables_sort() */* from (select /*+ para_tables_sort() */server_status() as status);"
            tdLog.info(sql)
            tdSql.query(sql)
            self.cur1.execute(sql)
            self.explain_sql(sql)

        #taos -f sql
        startTime_taos_f = time.time()
        print("taos -f sql start!")
        taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
        #_ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
        _ = subprocess.check_output(taos_cmd1, shell=True)
        print("taos -f sql over!")
        endTime_taos_f = time.time()
        print("taos_f total time %ds" % (endTime_taos_f - startTime_taos_f))

        print('=====================2.6 old function end ===========')



    def run(self):
        tdSql.prepare()

        startTime = time.time()

        #self.function_before_26()

        self.math_nest(['UNIQUE'])
        self.math_nest(['MODE'])
        self.math_nest(['SAMPLE'])

        # self.math_nest(['ABS','SQRT'])
        # self.math_nest(['SIN','COS','TAN','ASIN','ACOS','ATAN'])
        # self.math_nest(['POW','LOG'])
        # self.math_nest(['FLOOR','CEIL','ROUND'])
        # self.math_nest(['MAVG'])
        # self.math_nest(['HYPERLOGLOG'])
        # self.math_nest(['TAIL'])
        self.math_nest(['CSUM'])
        self.math_nest(['statecount','stateduration'])
        self.math_nest(['HISTOGRAM'])

        # self.str_nest(['LTRIM','RTRIM','LOWER','UPPER'])
        # self.str_nest(['LENGTH','CHAR_LENGTH'])
        # self.str_nest(['SUBSTR'])
        # self.str_nest(['CONCAT'])
        # self.str_nest(['CONCAT_WS'])
        # self.time_nest(['CAST']) #放到time里起来弄
        # self.time_nest(['CAST_1'])
        # self.time_nest(['CAST_2'])
        # self.time_nest(['CAST_3'])
        # self.time_nest(['CAST_4'])



        # self.time_nest(['NOW','TODAY'])
        # self.time_nest(['TIMEZONE'])
        # self.time_nest(['TIMETRUNCATE'])
        # self.time_nest(['TO_ISO8601'])
        # self.time_nest(['TO_UNIXTIMESTAMP'])
        # self.time_nest(['ELAPSED'])
        self.time_nest(['TIMEDIFF_1'])
        self.time_nest(['TIMEDIFF_2'])


        endTime = time.time()
        print("total time %ds" % (endTime - startTime))




    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
