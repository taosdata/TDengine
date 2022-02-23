###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
import os
import random
import random
import os
import time
import taos
import datetime
from itertools import product
from itertools import combinations
import subprocess
from taostest import TDCase
from distutils.log import warn as printf

class TestDnodes(TDCase):
    def init(self):
        self.basic_dnodes = 5 # ->int
        self.loop_times = 100

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = '''
            this is an abnormal test case for create and drop dnodes loop runing and let the dnodes is quickly change ;
        '''
        return case_description
    def tags(self) -> str:

        return "abnormal"

    def author(self) -> str:

        return "wenzhouwww"

    def check_first_dnodes(self):
        result = self.tdSql.query("show dnodes")
        query_row = int(result.row_count)
        if self.basic_dnodes!=query_row:
            raise "please check test_env.yaml ,the begining should be 5 dnodes "
        else:
             printf("\033[1;33m%s %s\033[0m" % (datetime.datetime.now(), '* check begin dnodes pass *'))

    def get_dnodes_name(self):
        result = self.tdSql.query("show dnodes")
        dnodes_name = []
        for dnodes_row in range(result.field_count):
            EP = result.fetch_all()[dnodes_row][1]
            dnodes_name.append(EP)
        return dnodes_name

    def is_accept_EP(self,dnode_ids,dnodes_name):
        EP_accept_flag = True
        if dnode_ids in dnodes_name:
            EP_accept_flag = True
        else:
            EP_accept_flag = False
            raise "not an expected EP name"
        return EP_accept_flag

    def remain_dnodes(self,basic_dnodes_name):
        remain = self.get_dnodes_name()
        return remain

    def miss_dnodes(self,basic_dnodes_name):
        remain = self.get_dnodes_name()
        remain_dnodes = set(basic_dnodes_name) - set(remain)
        return tuple(remain_dnodes)

    def create_dnodes(self,dnode_ids):
        '''
         create dnodes_ids
        '''
        cmds = "taos -s 'create dnode \" %s \" ' ; "%(dnode_ids)
        
        os.system(cmds)

    def drop_dnodes(self,dnode_ids):
        '''
         drop dnodes_ids
        '''
        cmds = "taos -s 'drop dnode \" %s \" ' ; "%(dnode_ids)
        
        os.system(cmds)

    def case_common(self):
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))
        self.tdCreateData.dropandcreateDB_random("%s" % self.db, 1)

        conn1 = taos.connect(host="127.0.0.1", user="root", password="taosdata", config="/etc/taos/")
        cur1 = conn1.cursor()
        cur1.execute('use "%s";' %self.db)
        sql = 'select * from regular_table_1 limit 5;'
        cur1.execute(sql)

        return(conn1,cur1)

    def right_case(self):
        # all right function case
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'

        for hanshu in range(1,5):
            func = tdFunction.func_regular_all(hanshu)

            try:
                taos_cmd1 = "taos -f query_new/%s.sql" % self.testcaseFilename
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)

                print("\n\n\n=======================================right case=======================================\n\n\n")
                print("case1:select * from regular_table where condition && select * from ( select front )")
                print("\n\n\n==========================right case===============case1=========================================\n\n\n")

                regular_where = tdWhere.regular_where()
                sql1 = 'select %s from %s;'  % (func,self.table)
                for i in range(2,len(regular_where[2])+1):
                    q_where = list(combinations(regular_where[2],i))
                    for q_where in q_where:
                        q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        q_like_match = regular_where[3]
                        q_in_where = regular_where[4]

                        sql2 = "select %s from %s where %s %s %s" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s)" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where %s %s %s" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                print("case2:select * from regular_table where condition order by ts asc | desc && select * from ( select front )")
                print("\n\n\n==========================right case===============case2=========================================\n\n\n")

                regular_where = tdWhere.regular_where()
                sql1 = 'select %s from %s;'  % (func,self.table)
                for i in range(2,len(regular_where[2])+1):
                    q_where = list(combinations(regular_where[2],i))
                    for q_where in q_where:
                        q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        q_like_match = regular_where[3]
                        q_in_where = regular_where[4]

                        sql2 = "select %s from %s where %s %s %s order by ts" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s order by ts)" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where %s %s %s order by ts" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where %s %s %s ) order by ts" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where %s %s %s order by ts ) order by ts" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                regular_where = tdWhere.regular_where()
                sql1 = 'select %s from %s order by ts desc;'  % (func,self.table)
                for i in range(2,len(regular_where[2])+1):
                    q_where = list(combinations(regular_where[2],i))
                    for q_where in q_where:
                        q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        q_like_match = regular_where[3]
                        q_in_where = regular_where[4]

                        sql2 = "select %s from %s where %s %s %s order by ts desc" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s order by ts desc)" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where %s %s %s order by ts desc" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where %s %s %s ) order by ts desc" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s where %s %s %s order by ts desc ) order by ts desc" %(func,self.table,q_where,q_like_match,q_in_where)
                        #self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                print("case3:select * from regular_table where condition order by ts limit && select * from ( select front )")
                print("\n\n\n=========================right case================case3=========================================\n\n\n")

                regular_where = tdWhere.regular_where()
                sql1 = 'select %s from %s;'  % (func,self.table)
                for i in range(2,len(regular_where[2])+1):
                    q_where = list(combinations(regular_where[2],i))
                    for q_where in q_where:
                        q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        q_like_match = regular_where[3]
                        q_in_where = regular_where[4]

                        sql2 = "select %s from %s where %s %s %s order by ts limit 10" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s order by ts limit 10)" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where %s %s %s order by ts limit 10" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2


            except Exception as e:
                raise e

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)

        num1 = sql.count('where')
        print("sqlnum1 %d" % num1)

    def special_case(self):
	    # all special function case
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]

        sql = 'Count the number of sqls'

        for hanshu in range(1,3):
            func = tdFunction.func_regular_special(hanshu)

            try:
                taos_cmd1 = "taos -f query_new/%s.sql" % self.testcaseFilename
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)

                print("\n\n\n=======================================special case=======================================\n\n\n")
                print("case1:select * from regular_table where condition && select * from ( select front )")
                print("\n\n\n=========================special case================case1=========================================\n\n\n")

                regular_where = tdWhere.regular_where()
                sql1 = 'select %s from %s;'  % (func,self.table)
                for i in range(2,len(regular_where[2])+1):
                    q_where = list(combinations(regular_where[2],i))
                    for q_where in q_where:
                        q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        q_like_match = regular_where[3]
                        q_in_where = regular_where[4]

                        sql2 = "select %s from %s where %s %s %s" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s)" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where %s %s %s" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                print("case2:select * from regular_table where condition order by ts asc | desc && select * from ( select front )")
                print("\n\n\n==========================special case===============case2=========================================\n\n\n")

                regular_where = tdWhere.regular_where()
                sql1 = 'select %s from %s;'  % (func,self.table)
                for i in range(2,len(regular_where[2])+1):
                    q_where = list(combinations(regular_where[2],i))
                    for q_where in q_where:
                        q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        q_like_match = regular_where[3]
                        q_in_where = regular_where[4]

                        sql2 = "select %s from %s where %s %s %s order by ts" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s order by ts)" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s) order by ts" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where %s %s %s order by ts" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                regular_where = tdWhere.regular_where()
                sql1 = 'select %s from %s order by ts desc;'  % (func,self.table)
                for i in range(2,len(regular_where[2])+1):
                    q_where = list(combinations(regular_where[2],i))
                    for q_where in q_where:
                        q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        q_like_match = regular_where[3]
                        q_in_where = regular_where[4]

                        sql2 = "select %s from %s where %s %s %s order by ts desc" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s order by ts desc)" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where %s %s %s order by ts desc" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                print("case3:select * from regular_table where condition order by ts limit && select * from ( select front )")
                print("\n\n\n=========================special case================case3=========================================\n\n\n")

                regular_where = tdWhere.regular_where()
                sql1 = 'select %s from %s;'  % (func,self.table)
                for i in range(2,len(regular_where[2])+1):
                    q_where = list(combinations(regular_where[2],i))
                    for q_where in q_where:
                        q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        q_like_match = regular_where[3]
                        q_in_where = regular_where[4]

                        sql2 = "select %s from %s where %s %s %s order by ts limit 10" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s order by ts limit 10)" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where %s %s %s order by ts limit 10" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

            except Exception as e:
                raise e

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)

        num2 = sql.count('where')
        print("sqlnum2 %d" % num2)

    def special_case_1(self):
        # special_case for top and bottom
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'

        for hanshu in range(11,12):
            func = tdFunction.func_regular_special(hanshu)

            try:
                taos_cmd1 = "taos -f query_new/%s.sql" % self.testcaseFilename
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)

                print("\n\n\n=======================================special case 1=======================================\n\n\n")
                print("case1:select * from regular_table where condition && select * from ( select front )")
                print("\n\n\n=====================special case 1====================case1=========================================\n\n\n")

                regular_where = tdWhere.regular_where()
                sql1 = 'select %s from %s;'  % (func,self.table)
                for i in range(2,len(regular_where[2])+1):
                    q_where = list(combinations(regular_where[2],i))
                    for q_where in q_where:
                        q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        q_like_match = regular_where[3]
                        q_in_where = regular_where[4]

                        sql2 = "select %s from %s where %s %s %s" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s)" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where %s %s %s" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                print("case2:select * from regular_table where condition order by ts asc | desc && select * from ( select front )")
                print("\n\n\n======================special case 1===================case2=========================================\n\n\n")

                regular_where = tdWhere.regular_where()
                sql1 = 'select %s from %s order by ts;'  % (func,self.table)
                for i in range(2,len(regular_where[2])+1):
                    q_where = list(combinations(regular_where[2],i))
                    for q_where in q_where:
                        q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        q_like_match = regular_where[3]
                        q_in_where = regular_where[4]

                        sql2 = "select %s from %s where %s %s %s order by ts" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s order by ts)" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where %s %s %s order by ts" %(func,self.table,q_where,q_like_match,q_in_where)
                        # TD-13422
                        # self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                regular_where = tdWhere.regular_where()
                sql1 = 'select %s from %s order by ts desc;'  % (func,self.table)
                for i in range(2,len(regular_where[2])+1):
                    q_where = list(combinations(regular_where[2],i))
                    for q_where in q_where:
                        q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        q_like_match = regular_where[3]
                        q_in_where = regular_where[4]

                        sql2 = "select %s from %s where %s %s %s order by ts desc" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s order by ts desc)" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where %s %s %s order by ts desc" %(func,self.table,q_where,q_like_match,q_in_where)
                        # TD-13422
                        # self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                print("case3:select * from regular_table where condition order by ts limit && select * from ( select front )")
                print("\n\n\n========================special case 1=================case3=========================================\n\n\n")

                regular_where = tdWhere.regular_where()
                sql1 = 'select %s from %s order by ts limit 10;'  % (func,self.table)
                for i in range(2,len(regular_where[2])+1):
                    q_where = list(combinations(regular_where[2],i))
                    for q_where in q_where:
                        q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        q_like_match = regular_where[3]
                        q_in_where = regular_where[4]

                        sql2 = "select %s from %s where %s %s %s order by ts limit 10" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s order by ts limit 10)" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where %s %s %s order by ts limit 10" %(func,self.table,q_where,q_like_match,q_in_where)
                        # TD-13422
                        # self.tdCreateData.dataequal('%s' %sql1 ,1,1,'%s' %sql2 ,1,1)
                        cur1.execute(sql2)
                        sql= sql + sql2

            except Exception as e:
                raise e

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)

        num1 = sql.count('where')
        print("sqlnum3 %d" % num1)

    def false_case(self):
        # all false function case
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'

        for hanshu in range(1,4):
            func = tdFunction.func_regular_error_all(hanshu)

            try:
                taos_cmd1 = "taos -f query_new/%s.sql" % self.testcaseFilename
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)

                print("\n\n\n=======================================error case=======================================\n\n\n")
                print("case1:select * from regular_table where condition && select * from ( select front )")
                print("\n\n\n========================error case=================case1=========================================\n\n\n")

                regular_where = tdWhere.regular_where()
                sql1 = 'select %s from %s;'  % (func,self.table)
                for i in range(2,len(regular_where[2])+1):
                    q_where = list(combinations(regular_where[2],i))
                    for q_where in q_where:
                        q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        q_like_match = regular_where[3]
                        q_in_where = regular_where[4]

                        sql2 = "select %s from %s where %s %s %s" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s)" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select %s from (select * from %s) where %s %s %s" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                print("case2:select * from regular_table where condition interval | sliding | Fill && select * from ( select front )")
                print("\n\n\n===========================error case==============case2=========================================\n\n\n")

                regular_where = tdWhere.regular_where()
                sql1 = 'select * from %s interval(3s) sliding(3n) Fill(NEXT);'  % self.table
                for i in range(2,len(regular_where[2])+1):
                    q_where = list(combinations(regular_where[2],i))
                    for q_where in q_where:
                        q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        q_like_match = regular_where[3]
                        q_in_where = regular_where[4]
                        time_window = regular_where[5]

                        sql2 = "select %s from %s where %s %s %s %s" %(func,self.table,q_where,q_like_match,q_in_where,time_window)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s %s)" %(func,self.table,q_where,q_like_match,q_in_where,time_window)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s) where %s %s %s %s" %(func,self.table,q_where,q_like_match,q_in_where,time_window)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select distinct(*) from %s where %s %s %s" %(self.table,q_where,q_like_match,q_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                print("case3:select * from regular_table where condition order by ts limit offset && select * from ( select front )")
                print("\n\n\n=======================error case==================case3=========================================\n\n\n")

                regular_where = tdWhere.regular_where()
                sql1 = 'select %s from %s limit 10 offset 5;'  % (func,self.table)
                for i in range(2,len(regular_where[2])+1):
                    q_where = list(combinations(regular_where[2],i))
                    for q_where in q_where:
                        q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        q_like_match = regular_where[3]
                        q_in_where = regular_where[4]

                        sql2 = "select %s from %s where %s %s %s order by ts limit 10 offset 5" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s where %s %s %s order by ts limit 10 offset 5)" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

                        sql2 = "select * from (select %s from %s) where %s %s %s order by ts limit 10 offset 5" %(func,self.table,q_where,q_like_match,q_in_where)
                        self.tdSql.error(sql2)
                        sql= sql + sql2

            except Exception as e:
                raise e

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)

        num1 = sql.count('where')
        print("sqlnum4 %d" % num1)

    def basic_query(self):
        startTime = time.time()

        startTime1 = time.time()
        self.right_case()
        endTime1 = time.time()
        print("total time1 %d s" % (endTime1 - startTime1))

        startTime2 = time.time()
        self.special_case()
        endTime2 = time.time()
        print("total time2 %d s" % (endTime2 - startTime2))

        startTime3 = time.time()
        self.special_case_1()
        endTime3 = time.time()
        print("total time3 %ds" % (endTime3 - startTime3))

        startTime4 = time.time()
        self.false_case()
        endTime4 = time.time()
        print("total time4 %ds" % (endTime4 - startTime4))

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))

    def basic_alter_exec(self,dbname , dnode_nums):
        sqls = [
            "alter database  %s replica %d ;  " %(dbname , random.randint(0,dnode_nums)) ,
            "alter database  %s quorum 2 ;"  %(dbname ) ,
            "alter database  %s blocks 200 ;"  %(dbname )
        ]
        for sql in sqls:
            self.tdSql.execute(sql)


    db = "regular_fun_db"
    table_list = ['regular_table_1','stable_1_1','regular_table_2','stable_1_2','stable_2_1']
    table = str(random.sample(table_list,1)).replace("[","").replace("]","").replace("'","")
    table_null_list = ['regular_table_null','stable_1_3','stable_1_4','stable_2_2','stable_null_data_1']
    table_null = str(random.sample(table_null_list,1)).replace("[","").replace("]","").replace("'","")
    testcasePath = os.path.split(__file__)[0]
    testcaseFilename = os.path.split(__file__)[-1]

    def run(self):
        self.check_first_dnodes()
        self.basic_data()
        basic_dnodes_name = get_dnodes_name()
        self.basic_query()
        basic_query_seed = 6
        for loop in self.loop_times:
            if loop % basic_query_seed ==0:
                # self.basic_query()
                self.basic_alter_exec()

            if loop%10 ==0 and flag:
                current_dnodes = self.remain_dnodes(basic_dnodes_name)
                if  current_dnodes: # has no dnodes
                    dnode_ids = random.sample(current_dnodes,1)
                    flag = self.is_accept_EP(dnode_ids)
                    self.drop_dnodes(dnode_ids)
                else:
                    print("current dnodes list is empty")
                    os.system("taos -s 'show dnodes;'")
            else:
               miss_dnodes = self.miss_dnodes(basic_dnodes_name)
               if  miss_dnodes: # dnodes is full
                   dnode_ids = random.sample(miss_dnodes,1)
                   flag = self.is_accept_EP(dnode_ids)
                   self.create_dnodes(dnode_ids)
               else:
                   print("current dnodes list is full")
                   os.system("taos -s 'show dnodes;'")
