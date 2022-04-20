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

from concurrent.futures import thread
import datetime
import logging
# -*- coding: utf-8 -*-
import os
import random
import subprocess
import threading
import time
from distutils.log import warn as printf
from itertools import combinations
from tracemalloc import start

import taos
from queryutil.where import *
from queryutil.createdata import *

from faker import Faker
fake = Faker('zh_CN')

class TestDnodes(TDCase):

    def init(self):
        super().init()
        self.basic_dnodes = 5  # ->int
        self.loop_times = 100
        self.drop_sleep_time = 10
        self.thread_nums = 100
        # self.logger.handlers[-1].setLevel(logging.ERROR)

        self.logger.info("initialized " + self.name)
        self.tdCreateData = TDCreateData(self.tdSql, self.logger)
        self.dnodes_ids_extra = len(self.get_component_by_name("taosd")[0]['spec']['dnodes']) + 1
        self.ports_extra = 6030 + self.dnodes_ids_extra * 100
        self.tmp_dir = self.envMgr._tmp_dir
        self.component = self.get_component_by_name(
            "taosd")[0]
        self.conf = self.get_component_by_name(
            "taosd")[0]
        self.first_Ep = self.conf["spec"]["config"]["firstEP"]
        self.params = {"_ts" : 1420041600000 , "_ts_step":1 ,"_row_nums":1,"_col_nums":12 ,  "tables_of_per_stable":20 ,"_tags_nums" : 10 , "_replica" :3 }
        self.db_nums = 10
        self.stable_nums = 2
        self.table_nums = 2
        self.time_sleep = 0
        self.db_index = 0
        self.global_queue = []
    

    def get_db_names(self):
        
        dbname = "dbs_%d"%self.db_index
        self.db_index +=1
        return dbname

    def insert_datas(self , db_name , endpoint):

        client_conn = self.tdSql.get_connection( None , endpoint)

        client_conn.execute("drop database if exists {} ;".format(db_name))
        client_conn.execute("create database {} replica 3 ;".format(db_name))
        client_conn.execute("use {};".format(db_name))
        client_conn.execute("create stable {}.meters (ts timestamp , current int , voltage float ) tags (ind int ) ".format(db_name))
        client_conn.execute("create table {}.sub_table_1 using meters tags(1) ".format(db_name))
        client_conn.execute("create table {}.sub_table_2 using meters tags(2) ".format(db_name))

        start_ts = 1643644800000
        for i in range(1000):
            ts = start_ts + i * 10
            client_conn.execute("use {}; ".format(db_name))
            insert_sql1 = "insert into {}.sub_table_1 values ({} ,{} , {}) ".format(db_name , ts , i , float(i*0.1))
            insert_sql2 = "insert into {}.sub_table_2 values ({} ,{} , {}) ".format(db_name ,ts , i , float(i*0.1))
            client_conn.execute(insert_sql1)
            client_conn.execute(insert_sql2)

    def start_create_db(self,endpoint):

        db_name = self.get_db_names()
        print(endpoint)
        db_name = threading.Thread(target=self.insert_datas,args=(db_name ,endpoint ))
        db_name.start()
        db_name.join()
        self.global_queue.append(db_name)

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
        # os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))
        os.system("touch %s/%s.sql" %
                  (self.testcasePath, self.testcaseFilename))
        self.tdCreateData.dropandcreateDB_random("%s" % self.db, 1)

        conn1 = taos.connect(host=self.first_Ep.split(":")[0], user="root",
                             password="taosdata", config="/etc/taos/")
        cur1 = conn1.cursor()
        cur1.execute('use "%s";' % self.db)
        sql = 'select * from regular_table_1 limit 5;'
        cur1.execute(sql)

        return (conn1, cur1)

    def right_case1(self):
        # print("case1:select * from regular_table where condition && select * from ( select front )")
        # print("\n\n\n=========================================case1=========================================\n\n\n")

        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]
        sql = 'Count the number of sqls'

        for i in range(2):
            try:
                taos_cmd1 = "taos -h %s -f %s/%s.sql" % (self.first_Ep.split(":")[0],
                    self.testcasePath, self.testcaseFilename)
                _ = subprocess.check_output(
                    taos_cmd1, shell=True).decode("utf-8")
                # print(conn1)
                cur1.execute('use "%s";' % self.db)

                regular_where = tdWhere.regular_where()
                sql1 = 'select * from %s;' % self.table
                for i in range(2, len(regular_where[2]) + 1):
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
                taos_cmd1 = "taos -h %s -f %s/%s.sql" % (self.first_Ep.split(":")[0],
                    self.testcasePath, self.testcaseFilename)
                _ = subprocess.check_output(
                    taos_cmd1, shell=True).decode("utf-8")
                # print(conn1)
                cur1.execute('use "%s";' % self.db)

                regular_where = tdWhere.regular_where()
                sql1 = 'select * from %s;' % self.table
                for i in range(2, len(regular_where[2]) + 1):
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
                for i in range(2, len(regular_where[2]) + 1):
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

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)

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
                taos_cmd1 = "taos -h %s -f %s/%s.sql" % (self.first_Ep.split(":")[0],
                    self.testcasePath, self.testcaseFilename)
                _ = subprocess.check_output(
                    taos_cmd1, shell=True).decode("utf-8")
                # print(conn1)
                cur1.execute('use "%s";' % self.db)

                regular_where = tdWhere.regular_where()
                sql1 = 'select * from %s;' % self.table
                for i in range(2, len(regular_where[2]) + 1):
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

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)

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
                taos_cmd1 = "taos -h %s -f %s/%s.sql" % (self.first_Ep.split(":")[0],
                    self.testcasePath, self.testcaseFilename)
                _ = subprocess.check_output(
                    taos_cmd1, shell=True).decode("utf-8")
                # print(conn1)
                cur1.execute('use "%s";' % self.db)

                regular_where = tdWhere.regular_where()
                sql1 = 'select * from %s limit 10 offset 5;' % self.table
                for i in range(2, len(regular_where[2]) + 1):
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

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)

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
                taos_cmd1 = "taos -h %s -f %s/%s.sql" % (self.first_Ep.split(":")[0],
                    self.testcasePath, self.testcaseFilename)
                _ = subprocess.check_output(
                    taos_cmd1, shell=True).decode("utf-8")
                # print(conn1)
                cur1.execute('use "%s";' % self.db)

                regular_where = tdWhere.regular_where()
                sql1 = 'select * from %s interval(3s) sliding(3n) Fill(NEXT);' % self.table
                for i in range(2, len(regular_where[2]) + 1):
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

        # self.tdSql.execute('''drop database if exists %s ;''' %self.db)

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

    def check_first_dnodes(self):
        self.tdSql.query("show dnodes")
        self.tdSql.query_data
        query_row = int(self.tdSql.query_row)
        if self.basic_dnodes != query_row:
            raise "please check test_env.yaml ,the begining should be 5 dnodes "
        else:
            printf("\033[1;33m%s %s\033[0m" %
                   (datetime.datetime.now(), '* check begin dnodes pass *'))

    def get_dnodes_name(self):
        self.tdSql.query("show dnodes")
        dnodes_name = []
        query_data = self.tdSql.query_data
        for dnodes_row in range(len(query_data)):
            EP = query_data[dnodes_row][1]
            if query_data[dnodes_row][4] == "ready":
                dnodes_name.append(EP)
        return dnodes_name

    def generate_dnodes(self):
        '''
         create dnodes_ids
        '''
        dnodes_setting = self.get_component_by_name(
            "taosd")[0]['spec']['dnodes'][-1]
        self.dnodes_ids_extra += 1
        self.ports_extra += 100
        dnodename = dnodes_setting['config']['logDir'].split("/")[3][:5] + str(self.dnodes_ids_extra)  # dnode + str(dnodes_ids_extra)

        # generate new dnodes
        dnodes_dict = dnodes_setting.copy()
        dnodes_dict['endpoint'] = dnodes_dict['endpoint'].replace(dnodes_dict['endpoint'].split(":")[-1], str(self.ports_extra))
        dnodes_dict['config_dir'] = dnodes_dict['config_dir'].replace(dnodes_dict['config_dir'].split("/")[3], str(dnodename))
        dnodes_dict['config']['dataDir'] = dnodes_dict['config']['dataDir']. \
            replace(dnodes_dict['config']['dataDir'].split("/")[3], str(dnodename))
        dnodes_dict['config']['logDir'] = dnodes_dict['config']['logDir']. \
            replace(dnodes_dict['config']['logDir'].split("/")[3], str(dnodename))

        return dnodes_dict

    def basic_query(self):
        basic_sqls = ['select count(*) from test.meters',
                      'select last(*) from test.meters',
                      'select * from test.meters where ts >100000 limit 10  ',
                      'select elapsed(ts) from test.meters group by tbname',
                      'select avg(current) from test.meters group by tbname order by ts desc',
                      'show test.tables ',
                      'describe test.meters',
                      'select max(current)*avg(current) from test.meters group by tbname ',
                      'select current from test.meters where ind > 1 '
                      ]
        conn = self.tdSql.get_connection(None , self.first_Ep)

        for sql in basic_sqls:
            print(sql)
            result = conn.query(sql)
            # data = result.fetch_all()
            # print(data)

    def threads_query(self):

        thread_pools = []
        for _ in range(self.thread_nums):
            inst = threading.Thread(target=self.basic_query)
            thread_pools.append(inst)

        ind = 0
        for thread_inst in thread_pools:
            thread_inst.start()
            print(' this is the %s_th threading' % ind)
            ind += 1

        for thread_inst in thread_pools:
            thread_inst.join()

    def create_dnodes(self):
        dnodes_dict = self.generate_dnodes()
        self.envMgr.configure_extra_dnodes(dnodes_dict, self.component)
        self.envMgr.addDnode(dnodes_dict['endpoint'])
        self.envMgr.startDnode(dnodes_dict['endpoint'])

    def drop_dnodes(self, dnode_ids):
        '''
         drop dnodes_ids
        '''
        time.sleep(self.drop_sleep_time)
        cmds = "taos -s 'drop dnode \"%s\" ' ; " % (dnode_ids)
        os.system(cmds)

    def run(self):
        self.check_first_dnodes()
        basic_query_seed = 6
        print("run query ")

        # alter schema task for all 
        
        self.tdSql.execute("drop database if exists test ;")
        self.tdSql.execute("create database test replica 3 ;")
        self.tdSql.execute("use test;")
        self.tdSql.execute("create stable test.meters (ts timestamp , current int , voltage float ) tags (ind int ) ")
        self.tdSql.execute("create table test.sub_table_1 using meters tags(1) ")
        self.tdSql.execute("create table test.sub_table_2 using meters tags(2) ")

        start_ts = 1643644800000
        for i in range(10000):
            ts = start_ts + i * 10
            self.tdSql.execute("use test;")
            insert_sql1 = "insert into test.sub_table_1 values ({} ,{} , {}) ".format(ts , i , float(i*0.1))
            insert_sql2 = "insert into test.sub_table_2 values ({} ,{} , {}) ".format(ts , i , float(i*0.1))
            self.tdSql.execute(insert_sql1)
            self.tdSql.execute(insert_sql2)

        for loop in range(self.loop_times):

            print("========= this is %s_th drop or create dnodes ========\n" % loop)

            self.start_create_db(self.first_Ep)

            if loop % basic_query_seed == 0:
                self.query_run()
                self.tdSql.query(
                    "select count(*) from test.meters group by tbname;")
                self.tdSql.query(
                    "select last(*) from test.meters group by tbname;")
                self.threads_query()
            if loop % 2 == 0:
                dnodes_name = self.get_dnodes_name()
                dnode_ids = random.sample(dnodes_name, 1)[0]
                self.drop_dnodes(dnode_ids)
                os.system("taos -h {} -s 'show dnodes;'".format(self.first_Ep.split(":")[0]))
                time.sleep(3)
            else:
                self.create_dnodes()
                os.system("taos -h {} -s 'show dnodes;'".format(self.first_Ep.split(":")[0]))
                time.sleep(3)

        # for task in self.global_queue :
        #     task.join()

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
        return "cluster", "drop_dnodes" , "create_dnodes"

    def desc(self) -> str:
        case_description = '''
            [test]<wenzhouwww> test case 4.1 for loop drop dnodes and add dnodes ;
            it should run with env test_env.yaml
        '''
        return case_description
    
