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
import taos
import time

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *
import random

class TDTestCase:
    def __init__(self):
        self.ts = 1420041600000 # 2015-01-01 00:00:00  this is begin time for first record
        self.num = 10
        self.Loop = 100
        global client

    def caseDescription(self):

        '''
        case1 <wenzhouwww>: this is an abnormal case for loop restart taosd
        between restart taosd ,there is an query and is insert is going on 
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

 
    def run(self):
        
        # Loop 
        tdSql.execute("drop database if exists testdb")
        client = taos.connect().cursor()  # global conn

        # tdSql.execute("create database testdb")
        # tdSql.execute("create stable testdb.st (ts timestamp ,  value int) tags (ind int)")
        # tdSql.query("describe testdb.st")
        # result = tdSql.getResult("describe testdb.st")
        # print(result)
        # tdSql.execute("alter stable testdb.st add tag new_tags int")
        # result = tdSql.getResult("describe testdb.st")
        # print(result)
        # tdSql.execute("alter stable testdb.st change tag new_tags alter_tag")
        # result = tdSql.getResult("describe testdb.st")
        # print(result)
        
        
        for loop_step in range(self.Loop):

            # run basic query and insert
            # kill all 
            os.system("ps -aux |grep 'taosd'  |awk '{print $2}'|xargs kill -9 >/dev/null 2>&1")
            tdDnodes.start(1)
            
            tdSql.execute("create database if not exists testdb")
            tdSql.execute("use testdb")

            if loop_step <1:
                tdSql.execute("create stable st (ts timestamp ,  value int) tags (ind int)")

            tdSql.execute("create stable st%d (ts timestamp ,  value int) tags (ind int)"%loop_step)
            for cur in range(self.num):
                tdSql.execute("insert into tb_%d using st%d tags(%d) values(now, %d)"%(loop_step,loop_step,loop_step, cur))
            
            os.system('taos -s "insert into testdb.sub_100 using testdb.st tags(100) values(now ,100);"')
            os.system('taos -s "select count(*) from testdb.sub_100;"')
            os.system('taos -s "describe testdb.sub_100;"')
            os.system('show databases\G')
            # another client 
            client_1 = taos.connect().cursor()
            client_2 = taos.connect().cursor() 

            alter_dict = {"days" : int(random.randint(1,5)) , 
            "keep":int(random.randint(10,20)) ,
            "precision":"ns",
            "blocks" : int(random.randint(1,6)*2),
            "quorum": int(random.randint(0,3)),
            "comp":int(random.randint(0,3)),
            "minrows":int(random.randint(1,3)*100),
            "replica":int(random.randint(1,3))
            }
            alter_list = ['days', 'keep', 'precision', 'blocks', 'quorum', 'comp', 'minrows', 'replica']
            random_key = random.sample(alter_list, 1)[0]
            
            sql =  "alter database {} {} {}".format("testdb", random_key, alter_dict[random_key])

            for alter_db in range(100):
                alter_list = ['days', 'keep', 'precision', 'blocks', 'quorum', 'comp', 'minrows', 'replica']
                random_key = random.sample(alter_list, 1)[0]
                sql =  "alter database {} {} {}".format("testdb", random_key, alter_dict[random_key])
         
                if alter_db%3==0:
                    # client_0.execute("reset query cache")
                    try:
                        client.execute(sql)
                        client.fetchall()
                        os.system('taos -s "insert into testdb.sub_100 using testdb.st tags(100) values(now ,100);"')
                    except  :
                        pass
                    print(" client runing sqls : %s" %sql )
                elif alter_db%3==1:
                    # client_1.execute("reset query cache")
                    try:
                        client_1.execute(sql)
                        client_1.fetchall()
                        os.system('taos -s "insert into testdb.sub_100 using testdb.st tags(100) values(now ,100);"')
                    except :
                        pass
                    print(" client_1 runing sqls : %s" %sql )
                elif alter_db%3==2:
                    try:
                        # client_2.execute("reset query cache")
                        client_2.execute(sql)
                        client_2.fetchall()
                        os.system('taos -s "insert into testdb.sub_100 using testdb.st tags(100) values(now ,100);"')
                    except:
                        pass
                    print(" client_2 runing sqls : %s" %sql )
                else:
                    try:
                        client.execute(sql)
                        client.fetchall()
                        os.system('taos -s "insert into testdb.sub_100 using testdb.st tags(100) values(now ,100);"')
                    except:
                        pass
                    print(" client runing sqls : %s" %sql )
            os.system("taos -s 'show databases;'")

            sleep(2)
            tdDnodes.stopAll()
            
            tdLog.notice(" this is the %s_th loop restart taosd going " % loop_step)
        
            client_1.close()
            client_2.close()
        
        client.close()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
