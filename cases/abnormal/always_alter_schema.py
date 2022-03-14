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
import random
import threading

import taos
from queryutil.createdata import *
from queryutil.where import *
from taostest import TDCase


class TestDnodes(TDCase):

    def init(self):
        self.ts = 1420041600000  # 2015-01-01 00:00:00  this is begin time for first record
        self.num = 10
        self.Loop = 10
        self.loop_alter = 100
        self.thread_nums = 100
        self.conf = self.get_component_by_name(
            "taosd")[0]

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
            print(sql)
            result = conn.query(sql)

        alter_tags = ["alter table testdb.st add tag extraTag int",
                      "alter table testdb.st drop tag extraTag",
                      "alter table testdb.st change tag extraTag newTag"
                      "alter table testdb.st drop tag extraTag",
                      ]
        sql = random.sample(alter_tags, 1)[0]
        print(sql)
        try:
            result = conn.query(sql)
        except taos.error.ProgrammingError:
            pass

    def threads_query_alter_tags(self):

        thread_pools = []
        for _ in range(self.thread_nums):
            inst = threading.Thread(target=self.basic_query_alter_tags)
            thread_pools.append(inst)

        ind = 0
        for thread_inst in thread_pools:
            thread_inst.start()
            print(' this is the %s_th threading' % ind)
            ind += 1

        for thread_inst in thread_pools:
            thread_inst.join()

    def basic_alter_task(self):
        client_0 = self.tdSql.get_connection(self.conf)  # global conn
        client_1 = self.tdSql.get_connection(self.conf)
        client_2 = self.tdSql.get_connection(self.conf)

        add_tag_list = []
        for i in range(self.loop_alter):
            sql = "alter stable testdb.st add tag new_tag%d int" % i
            add_tag_list.append(sql)

        change_tag_list = []
        for i in range(self.loop_alter + 1):
            sql = " alter stable testdb.st  change tag new_tag%d new_tag_%d" % (i, i)
            change_tag_list.append(sql)

        set_tag_list = []
        for i in range(self.loop_alter):
            sql = "alter table testdb.tb_set set tag new_tag_%d=%d" % (i, i * 10)
            set_tag_list.append(sql)

        drop_tag_list = []
        for i in range(self.loop_alter):
            sql = "alter stable testdb.st drop tag new_tag_%d" % (i)
            drop_tag_list.append(sql)

        for i in range(self.loop_alter):
            add_sql = add_tag_list[i]
            change_sql = change_tag_list[i]
            set_sql = set_tag_list[i]
            drop_sql = drop_tag_list[i]

            execute_list = [add_sql, change_sql, set_sql, drop_sql]

            for ind, sql in enumerate(execute_list):
                if sql == drop_sql:
                    if i % 5 != 0:
                        continue
                    else:
                        pass

                if ind % 3 == 0:
                    # client_0.execute("reset query cache")
                    client_0.execute(sql)
                    print(" client_0 runing sqls : %s" % sql)
                elif ind % 3 == 1:
                    # client_1.execute("reset query cache")
                    client_1.execute(sql)
                    print(" client_1 runing sqls : %s" % sql)
                elif ind % 3 == 2:
                    # client_2.execute("reset query cache")
                    client_2.execute(sql)
                    print(" client_2 runing sqls : %s" % sql)
                else:
                    client_0.execute(sql)
                    print(" client_0 runing sqls : %s" % sql)

            query_sqls = ["select count(*) from testdb.st group by ind",
                          "describe testdb.st",
                          "select count(*) from testdb.st group by tbname"]
            reset_sql = "reset query cache"

            if i % 10 == 0:
                self.tdSql.execute(reset_sql)
                client_0.execute(reset_sql)
                client_1.execute(reset_sql)
                client_2.execute(reset_sql)

            for sql in query_sqls:
                if sql == "describe testdb.st":
                    print("==========================\n")
                    print("==========describe=======\n")
                    print("==========================\n")
                    res = client_0.query(sql)
                    res = res.fetch_all()
                    print("client 0 res :", res) if res else print("empty")

                    res = client_1.query(sql)
                    res = res.fetch_all()
                    print("client 1 res :", res) if res else print("empty")

                    res = client_2.query(sql)
                    res = res.fetch_all()
                    print("client 2 res :", res) if res else print("empty")
                else:
                    client_0.query(sql)
                    client_1.query(sql)
                    client_2.query(sql)

            print("===== this is the %d_th loop alter tags is going now ====" % i)

        client_1.close()
        client_2.close()

        client_0.close()

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

    def run(self):

        # Loop 
        self.tdSql.execute("drop database if exists testdb")
        self.tdSql.execute("create database testdb")
        self.tdSql.execute("use testdb")
        self.tdSql.execute("create stable testdb.st (ts timestamp ,  value int) tags (ind int)")
        self.tdSql.query("describe testdb.st")

        # insert data
        for cur in range(self.num):
            self.tdSql.execute("insert into tb_%d using st tags(%d) values(%d, %d)" % (cur, cur, self.ts + 1000 * cur, cur))
            self.tdSql.execute("insert into tb_set using st tags(%d) values(%d, %d)" % (cur, self.ts + 1000 * cur, cur))
        self.basic_alter_task()
        self.threads_query_alter_tags()

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
        return "abnormal", "drop_dnodes"

    def desc(self) -> str:
        case_description = '''
            [test]<wenzhouwww> test case for loop drop dnodes and add dnodes ;
        '''
        return case_description
