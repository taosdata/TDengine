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
from http import client
import os
import random
from ssl import PROTOCOL_TLS_SERVER
import time
import taos
import copy
import datetime
from itertools import product
from itertools import combinations
import subprocess
import logging
from taostest import TDCase
from distutils.log import warn as printf
from queryutil.createdata import *
from queryutil.where import *
from itertools import product
from itertools import combinations
import subprocess
from taostest.util.file import dict2file
from taostest.util.remote import Remote
import subprocess
import threading

class TestDnodes(TDCase):

    def init(self):
        self.ts = 1420041600000 # 2015-01-01 00:00:00  this is begin time for first record
        self.num = 10
        self.Loop = 100
        self.loop_alter = 100
        self.thread_nums = 100  #thread_nums for alter tags
        self.sleep_time = 10    #sleep time for taosd restart 

        self.conf = self.get_component_by_name(
            "taosd")[0]
        self.logger.handlers[-1].setLevel(logging.ERROR)
        self.logger.info("initialized " + self.name)
        self.tdCreateData = TDCreateData(self.tdSql, self.logger)
    
    def basic_restart_taosd(self ,sleep_seconds :int):
        
        self.tdSql.query(" show dnodes")
        endPoints = []
        for row in self.tdSql.query_data:
            endPoints.append(row[1])
        
        endPoint = random.sample(endPoints,1)[0]
        # restart taosd 
        self.envMgr.stopDnode(endPoint) 
        time.sleep(sleep_seconds)
        self.envMgr.startDnode(endPoint)

        print(" ===== restart dnode: %s ====="%endPoint)
    
    def basic_query_alter_tags(self):
        sqls = ["select count(*) from testdb.st group by tbname",
                "select max(value)*elapsed(ts)+100 from testdb.st group by tbname",
                "select * from testdb.st where ts >1000000",
                "select last(*) from testdb.st group by tbname order by ts limit 10 ",
                "select tbname from testdb.st where ind>1",
                "select stddev(value) from testdb.st ",
                "select top(value ,5) from testdb.st ",
                "select bottom(value , 5) from testdb.st group by tbname order by ts ",
                "select avg(value) from testdb.st interval(10s) sliding (2s) group by tbname",
                "select max(value)+count(*)+124 from testdb.st "
        ]
        conn = self.tdSql.get_connection(self.conf)

        for sql in sqls:
            # print(sql)
            result = conn.query(sql)

        alter_tags= [ "alter table testdb.st add tag extraTag int" , 
                    "alter table testdb.st drop tag extraTag",
                    "alter table testdb.st change tag extraTag newTag"
                    "alter table testdb.st drop tag extraTag",
        ]
        sql = random.sample(alter_tags,1)[0]
        # print(sql)
        try:
            result = conn.query(sql)
        except taos.error.ProgrammingError: 
            pass

    def threads_query_alter_tags(self):
        
        print(" ======= runing task : threads_query_alter_tags ======== " )
        thread_pools = []
        for _ in range(self.thread_nums):
            inst = threading.Thread(target=self.basic_query_alter_tags)
            thread_pools.append(inst)
        
        ind = 0 
        for thread_inst in thread_pools:

            thread_inst.start()
            # print(' this is the %s_th threading' %ind)
            ind+=1
        
        for thread_inst in thread_pools:
    
            thread_inst.join() 
    
    db = "regular_db"
    table_list = ['regular_table_1', 'stable_1_1',
                  'regular_table_2', 'stable_1_2', 'stable_2_1']
    table = str(random.sample(table_list, 1)).replace(
        "[", "").replace("]", "").replace("'", "")
    table_null_list = ['regular_table_null', 'stable_1_3',
                       'stable_1_4', 'stable_2_2', 'stable_null_data_1']
    table_null = str(random.sample(table_null_list, 1)).replace(
        "[", "").replace("]", "").replace("'", "")
    testcasePath = os.path.split(__file__)[0]
    testcaseFilename = os.path.split(__file__)[-1]

    def case_common(self):
        #os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))
        os.system("touch %s/%s.sql" %
                  (self.testcasePath, self.testcaseFilename))
        self.tdCreateData.dropandcreateDB_random("%s" % self.db, 1)

        conn1 = taos.connect(host="127.0.0.1", user="root",
                             password="taosdata", config="/etc/taos/")
        cur1 = conn1.cursor()
        cur1.execute('use "%s";' % self.db)
        sql = 'select * from regular_table_1 limit 5;'
        cur1.execute(sql)

        return(conn1, cur1)

    def right_case1(self):
        # print("case1:select * from regular_table where condition && select * from ( select front )")
        # print("\n\n\n=========================================case1=========================================\n\n\n")

        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'

        for i in range(2):
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (
                    self.testcasePath, self.testcaseFilename)
                _ = subprocess.check_output(
                    taos_cmd1, shell=True).decode("utf-8")
                # print(conn1)
                cur1.execute('use "%s";' % self.db)

                regular_where = tdWhere.regular_where()
                sql1 = 'select * from %s;' % self.table
                for i in range(2, len(regular_where[2])+1):
                    q_where = list(combinations(regular_where[2], i))
                    for q_where in q_where:
                        q_where = str(q_where).replace("(", "").replace(")", "").replace(
                            "'", "").replace("\"", "").replace(",", "")
                        q_like_match = regular_where[3]
                        q_in_where = regular_where[4]

                        sql2 = "select * from %s where %s %s %s" % (
                            self.table, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s where %s %s %s)" % (
                            self.table, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s) where %s %s %s" % (
                            self.table, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s where %s %s %s ) where %s %s %s" % (
                            self.table, q_where, q_like_match, q_in_where, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

            except Exception as e:
                raise e

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)

        num1 = sql.count('where')
        # print("sqlnum1 %d" % num1)

    def right_case2(self):
        # print("case2:select * from regular_table where condition order by ts asc | desc && select * from ( select front )")
        # print("\n\n\n=========================================case2=========================================\n\n\n")

        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'

        for i in range(2):
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (
                    self.testcasePath, self.testcaseFilename)
                _ = subprocess.check_output(
                    taos_cmd1, shell=True).decode("utf-8")
                # print(conn1)
                cur1.execute('use "%s";' % self.db)

                regular_where = tdWhere.regular_where()
                sql1 = 'select * from %s;' % self.table
                for i in range(2, len(regular_where[2])+1):
                    q_where = list(combinations(regular_where[2], i))
                    for q_where in q_where:
                        q_where = str(q_where).replace("(", "").replace(")", "").replace(
                            "'", "").replace("\"", "").replace(",", "")
                        q_like_match = regular_where[3]
                        q_in_where = regular_where[4]

                        sql2 = "select * from %s where %s %s %s order by ts" % (
                            self.table, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s where %s %s %s order by ts)" % (
                            self.table, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s) where %s %s %s order by ts" % (
                            self.table, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s where %s %s %s ) order by ts" % (
                            self.table, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s order by ts) where %s %s %s " % (
                            self.table, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s where %s %s %s order by ts ) order by ts" % (
                            self.table, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s where %s %s %s order by ts ) where %s %s %s order by ts" % (
                            self.table, q_where, q_like_match, q_in_where, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                regular_where = tdWhere.regular_where()
                sql1 = 'select * from %s order by ts desc;' % self.table
                for i in range(2, len(regular_where[2])+1):
                    q_where = list(combinations(regular_where[2], i))
                    for q_where in q_where:
                        q_where = str(q_where).replace("(", "").replace(")", "").replace(
                            "'", "").replace("\"", "").replace(",", "")
                        q_like_match = regular_where[3]
                        q_in_where = regular_where[4]

                        sql2 = "select * from %s where %s %s %s order by ts desc" % (
                            self.table, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s where %s %s %s order by ts desc)" % (
                            self.table, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s) where %s %s %s order by ts desc" % (
                            self.table, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s where %s %s %s ) order by ts desc" % (
                            self.table, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s order by ts desc ) where %s %s %s" % (
                            self.table, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s where %s %s %s order by ts desc ) order by ts desc" % (
                            self.table, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s where %s %s %s order by ts desc ) where %s %s %s order by ts desc" % (
                            self.table, q_where, q_like_match, q_in_where, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

            except Exception as e:
                raise e

        #self.tdSql.execute('''drop database if exists %s ;''' %self.db)

        num2 = sql.count('where')
        # print("sqlnum2 %d" % num2)

    def right_case3(self):
        print("case3:select * from regular_table where condition order by ts limit && select * from ( select front )")
        print("\n\n\n=========================================case3=========================================\n\n\n")

        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'

        for i in range(2):
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (
                    self.testcasePath, self.testcaseFilename)
                _ = subprocess.check_output(
                    taos_cmd1, shell=True).decode("utf-8")
                # print(conn1)
                cur1.execute('use "%s";' % self.db)

                regular_where = tdWhere.regular_where()
                sql1 = 'select * from %s;' % self.table
                for i in range(2, len(regular_where[2])+1):
                    q_where = list(combinations(regular_where[2], i))
                    for q_where in q_where:
                        q_where = str(q_where).replace("(", "").replace(")", "").replace(
                            "'", "").replace("\"", "").replace(",", "")
                        q_like_match = regular_where[3]
                        q_in_where = regular_where[4]

                        sql2 = "select * from %s where %s %s %s order by ts limit 10" % (
                            self.table, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s where %s %s %s order by ts limit 10)" % (
                            self.table, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s where %s %s %s ) where %s %s %s order by ts limit 10" % (
                            self.table, q_where, q_like_match, q_in_where, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s) where %s %s %s order by ts limit 10" % (
                            self.table, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s where %s %s %s order by ts ) limit 10" % (
                            self.table, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s where %s %s %s limit 10) order by ts " % (
                            self.table, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s where %s %s %s limit 10) where %s %s %s order by ts " % (
                            self.table, q_where, q_like_match, q_in_where, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s where %s %s %s order by ts limit 10) where %s %s %s order by ts " % (
                            self.table, q_where, q_like_match, q_in_where, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s where %s %s %s order by ts limit 10) where %s %s %s order by ts  limit 10" % (
                            self.table, q_where, q_like_match, q_in_where, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

            except Exception as e:
                raise e

        #self.tdSql.execute('''drop database if exists %s ;''' %self.db)

        num3 = sql.count('where')
        # print("sqlnum3 %d" % num3)

    def right_case4(self):
        # print("case4:select * from regular_table where condition order by ts limit offset && select * from ( select front )")
        # print("\n\n\n=========================================case4=========================================\n\n\n")

        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'

        for i in range(2):
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (
                    self.testcasePath, self.testcaseFilename)
                _ = subprocess.check_output(
                    taos_cmd1, shell=True).decode("utf-8")
                # print(conn1)
                cur1.execute('use "%s";' % self.db)

                regular_where = tdWhere.regular_where()
                sql1 = 'select * from %s limit 10 offset 5;' % self.table
                for i in range(2, len(regular_where[2])+1):
                    q_where = list(combinations(regular_where[2], i))
                    for q_where in q_where:
                        q_where = str(q_where).replace("(", "").replace(")", "").replace(
                            "'", "").replace("\"", "").replace(",", "")
                        q_like_match = regular_where[3]
                        q_in_where = regular_where[4]

                        sql2 = "select * from %s where %s %s %s order by ts limit 10 offset 5" % (
                            self.table, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s where %s %s %s order by ts limit 10 offset 5)" % (
                            self.table, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s where %s %s %s ) order by ts limit 10 offset 5" % (
                            self.table, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s) where %s %s %s order by ts limit 10 offset 5" % (
                            self.table, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s where %s %s %s ) where %s %s %s order by ts limit 10 offset 5" % (
                            self.table, q_where, q_like_match, q_in_where, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

                        sql2 = "select * from (select * from %s where %s %s %s order by ts limit 100) where %s %s %s order by ts limit 10 offset 5" % (
                            self.table, q_where, q_like_match, q_in_where, q_where, q_like_match, q_in_where)
                        self.tdCreateData.dataequal(
                            '%s' % sql1, 10, 10, '%s' % sql2, 10, 10)
                        cur1.execute(sql2)
                        sql = sql + sql2

            except Exception as e:
                raise e

        #self.tdSql.execute('''drop database if exists %s ;''' %self.db)

        num4 = sql.count('where')
        # print("sqlnum4 %d" % num4)

    def false_case1(self):
        # print("\n\n\n=======================================error case=======================================\n\n\n")
        # print("case1:select * from regular_table where condition interval | sliding | Fill && select * from ( select front )")
        # print("\n\n\n=========================================case1=========================================\n\n\n")

        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]

        for i in range(2):
            try:
                taos_cmd1 = "taos -f %s/%s.sql" % (
                    self.testcasePath, self.testcaseFilename)
                _ = subprocess.check_output(
                    taos_cmd1, shell=True).decode("utf-8")
                # print(conn1)
                cur1.execute('use "%s";' % self.db)

                regular_where = tdWhere.regular_where()
                sql1 = 'select * from %s interval(3s) sliding(3n) Fill(NEXT);' % self.table
                for i in range(2, len(regular_where[2])+1):
                    q_where = list(combinations(regular_where[2], i))
                    for q_where in q_where:
                        q_where = str(q_where).replace("(", "").replace(")", "").replace(
                            "'", "").replace("\"", "").replace(",", "")
                        q_like_match = regular_where[3]
                        q_in_where = regular_where[4]
                        time_window = regular_where[5]

                        sql2 = "select * from %s where %s %s %s %s" % (
                            self.table, q_where, q_like_match, q_in_where, time_window)
                        self.tdSql.error(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s %s)" % (
                            self.table, q_where, q_like_match, q_in_where, time_window)
                        self.tdSql.error(sql2)

                        sql2 = "select * from (select * from %s) where %s %s %s %s" % (
                            self.table, q_where, q_like_match, q_in_where, time_window)
                        self.tdSql.error(sql2)

                        sql2 = "select distinct(*) from %s where %s %s %s" % (
                            self.table, q_where, q_like_match, q_in_where)
                        self.tdSql.error(sql2)

            except Exception as e:
                raise e

        #self.tdSql.execute('''drop database if exists %s ;''' %self.db)

    def query_run(self) -> bool:

        startTime1 = time.time()
        self.right_case1()
        endTime1 = time.time()
        print("total time1 %d s" % (endTime1 - startTime1))

        startTime2 = time.time()
        self.right_case2()
        endTime2 = time.time()
        print("total time2 %d s" % (endTime2 - startTime2))

        startTime3 = time.time()
        self.right_case3()
        endTime3 = time.time()
        print("total time3 %ds" % (endTime3 - startTime3))

        startTime4 = time.time()
        self.right_case4()
        endTime4 = time.time()
        print("total time4 %ds" % (endTime4 - startTime4))

        self.false_case1()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = '''
            this is an abnormal test case for alter tags by threading and random alter ;
        '''
        return case_description

    def tags(self) -> str:

        return "abnormal"

    def author(self) -> str:

        return "wenzhouwww"

    def loop_task_total(self ,restart_flag : bool):

        # Loop 
        for _ in range(self.Loop):
            try :
                self.tdSql.execute("drop database if exists testdb")
                self.tdSql.execute("create database testdb")
                self.tdSql.execute("use testdb")
                self.tdSql.execute("create stable testdb.st (ts timestamp ,  value int) tags (ind int)")
                self.tdSql.query("describe testdb.st")
                
                # insert data
                for cur in range(self.num):
                    self.tdSql.execute("insert into tb_%d using st tags(%d) values(%d, %d)"%(cur,cur, self.ts+1000*cur,cur))
                    self.tdSql.execute("insert into tb_set using st tags(%d) values(%d, %d)"%(cur,self.ts+1000*cur,cur))
                self.threads_query_alter_tags()
                self.query_run()
                if restart_flag:
                    self.basic_restart_taosd(self.sleep_time)
            except taos.error.ProgrammingError as e:
                print(" restart is going and service error as expected, msg is :  " ,e.msg)
                pass
                
    def loop_task_sync(self):
        self.loop_task_total(True)

    def loop_task_unsyc(self):
        # Loop  unsyc restart taosd
        
        inst_query = threading.Thread(target=self.loop_task_total ,args= ((False,)))
        inst_restart = threading.Thread(target=self.basic_restart_taosd ,args=((3,)))
        inst_query.start()
        inst_restart.start()
        inst_query.join()
        inst_restart.join()

    def run(self):
        self.loop_task_sync()
        # self.loop_task_unsyc()
               
    def cleanup(self):
        pass

    def author(self):
        '''
        abstract about author
        '''
        return "wenzhouwww"

    def tags(self):
        '''
        set tags
        '''
        return "abnormal", "kill_start"

    def desc(self) -> str:
        case_description = '''
            [test]<wenzhouwww> test case for loop kill and start TDengine ;
        '''
        return case_description
