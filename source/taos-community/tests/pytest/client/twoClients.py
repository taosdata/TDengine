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

import os
import sys
sys.path.insert(0, os.getcwd())
from util.log import *
from util.sql import *
from util.dnodes import *
import taos


class TwoClients:
    def initConnection(self):
        self.host = "127.0.0.1"
        self.user = "root"
        self.password = "taosdata"
        self.config = "/home/xp/git/TDengine/sim/dnode1/cfg"        
    
    def run(self):
        tdDnodes.init("")
        tdDnodes.setTestCluster(False)
        tdDnodes.setValgrind(False)

        tdDnodes.stopAll()
        tdDnodes.deploy(1)
        tdDnodes.start(1)
        
        # first client create a stable and insert data
        conn1 = taos.connect(self.host, self.user, self.password, self.config)
        cursor1 = conn1.cursor()
        cursor1.execute("drop database if exists db")
        cursor1.execute("create database db")
        cursor1.execute("use db")
        cursor1.execute("create table tb (ts timestamp, id int) tags(loc nchar(30))")
        cursor1.execute("insert into t0 using tb tags('beijing') values(now, 1)")

        # second client alter the table created by cleint
        conn2 = taos.connect(self.host, self.user, self.password, self.config)
        cursor2 = conn2.cursor() 
        cursor2.execute("use db")       
        cursor2.execute("alter table tb add column name nchar(30)")
        
        # first client should not be able to use the origin metadata
        tdSql.init(cursor1, True)
        tdSql.error("insert into t0 values(now, 2)")

        # first client should be able to insert data with udpated medadata
        tdSql.execute("insert into t0 values(now, 2, 'test')")
        tdSql.query("select * from tb")
        tdSql.checkRows(2)

        # second client drop the table 
        cursor2.execute("drop table t0")
        cursor2.execute("create table t0 using tb tags('beijing')")

        tdSql.execute("insert into t0 values(now+1s, 2, 'test')")
        tdSql.query("select * from tb")
        tdSql.checkRows(1)

        # error expected for two clients drop the same cloumn
        cursor2.execute("alter table tb drop column name")
        tdSql.error("alter table tb drop column name")

        cursor2.execute("alter table tb add column speed int")
        tdSql.error("alter table tb add column speed int")


        tdSql.execute("alter table tb add column size int")
        tdSql.query("describe tb")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "id")
        tdSql.checkData(2, 0, "speed")
        tdSql.checkData(3, 0, "size")
        tdSql.checkData(4, 0, "loc")


        cursor1.close()
        cursor2.close()
        conn1.close()
        conn2.close()
        
clients = TwoClients()
clients.initConnection()
clients.run()