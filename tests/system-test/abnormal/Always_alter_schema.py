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
        self.Loop = 10
        self.loop_alter = 100

    def caseDescription(self):

        '''
        case1 <wenzhouwww>: this is an abnormal case for always change schema
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

 
    def run(self):
        
        # Loop 
        tdSql.execute("drop database if exists testdb")
        tdSql.execute("create database testdb")
        tdSql.execute("use testdb")
        tdSql.execute("create stable testdb.st (ts timestamp ,  value int) tags (ind int)")
        tdSql.query("describe testdb.st")
        
        # insert data
        for cur in range(self.num):
            tdSql.execute("insert into tb_%d using st tags(%d) values(%d, %d)"%(cur,cur, self.ts+1000*cur,cur))
            tdSql.execute("insert into tb_set using st tags(%d) values(%d, %d)"%(cur,self.ts+1000*cur,cur))

        client_0 = taos.connect().cursor()  # global conn
        
        client_1 = taos.connect().cursor()
        client_2 = taos.connect().cursor() 

        add_tag_list = []
        for i in range(self.loop_alter):
            sql= "alter stable testdb.st add tag new_tag%d int"%i
            add_tag_list.append(sql)

        change_tag_list = []
        for i in range(self.loop_alter+1):
            sql =   " alter stable testdb.st  change tag new_tag%d new_tag_%d"%(i,i)  
            change_tag_list.append(sql)

        set_tag_list = []
        for i in range(self.loop_alter):
            sql = "alter table testdb.tb_set set tag new_tag_%d=%d"%(i,i*10)
            set_tag_list.append(sql)

        drop_tag_list = []
        for i in range(self.loop_alter):
            sql = "alter stable testdb.st drop tag new_tag_%d"%(i)
            drop_tag_list.append(sql)

        for i in range(self.loop_alter):
            add_sql = add_tag_list[i]
            change_sql = change_tag_list[i]
            set_sql = set_tag_list[i]
            drop_sql = drop_tag_list[i]

            execute_list= [add_sql,change_sql,set_sql,drop_sql]

            for ind,sql in enumerate(execute_list):
                if sql ==drop_sql:
                    if i%5 !=0:
                        continue
                    else:
                        pass

                if ind%3==0:
                    # client_0.execute("reset query cache")
                    client_0.execute(sql)
                    print(" client_0 runing sqls : %s" %sql )
                elif ind%3==1:
                    # client_1.execute("reset query cache")
                    client_1.execute(sql)
                    print(" client_1 runing sqls : %s" %sql )
                elif ind%3==2:
                    # client_2.execute("reset query cache")
                    client_2.execute(sql)
                    print(" client_2 runing sqls : %s" %sql )
                else:
                    client_0.execute(sql)
                    print(" client_0 runing sqls : %s" %sql )
            
            query_sqls  = ["select count(*) from testdb.st group by ind",
                           "describe testdb.st",
                           "select count(*) from testdb.st group by tbname"]
            reset_sql = "reset query cache"

            if i%10 ==0:
                tdSql.execute(reset_sql) 
                client_0.execute(reset_sql)
                client_1.execute(reset_sql)
                client_2.execute(reset_sql)

            for sql in query_sqls:
                if sql =="describe testdb.st":
                    print("==========================\n")
                    print("==========describe=======\n")
                    print("==========================\n")
                    client_0.execute(sql)
                    res = client_0.fetchall()
                    print("client 0 res :", res) if res  else print("empty")

                    client_1.execute(sql)
                    res = client_0.fetchall()
                    print("client 1 res :", res) if res  else print("empty")

                    client_2.execute(sql)
                    res = client_2.fetchall()
                    print("client 2 res :", res) if res  else print("empty")
                else:
                    client_0.execute(sql)
                    client_1.execute(sql)
                    client_2.execute(sql)
                             
            tdLog.notice("===== this is the %d_th loop alter tags is going now ===="%i )
        




        client_1.close()
        client_2.close()

        client_0.close()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
