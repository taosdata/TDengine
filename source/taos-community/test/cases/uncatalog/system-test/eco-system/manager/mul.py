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
import random
import time

from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import *

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.setsql = TDSetSql()

    # prepareEnv
    def prepareEnv(self):
        self.dbName = "mullevel"
        self.stbName = "meters"
        self.topicName = "topic"
        self.topicNum = 100
        self.loop = 50000

        sql = f"use {self.dbName}"
        tdSql.execute(sql)

        # generate topic sql
        self.sqls = [
            f"select * from {self.stbName}",
            f"select * from {self.stbName} where ui < 200",
            f"select * from {self.stbName} where fc > 20.1",
            f"select * from {self.stbName} where nch like '%%a%%'",
            f"select * from {self.stbName} where fc > 20.1",
            f"select lower(bin) from {self.stbName} where length(bin) < 10;",
            f"select upper(bin) from {self.stbName} where length(nch) > 10;",
            f"select upper(bin) from {self.stbName} where ti > 10 or ic < 40;",
            f"select * from {self.stbName} where ic < 100 "
        ]

        

    # prepareEnv
    def createTopics(self):
        for i in range(self.topicNum):
            topicName = f"{self.topicName}{i}"
            sql = random.choice(self.sqls)
            createSql = f"create topic if not exists {topicName} as {sql}"
            try:
               tdSql.execute(createSql, 3, True)
            except:
                tdLog.info(f" create topic {topicName} failed.")


    # random del topic
    def managerTopics(self):
        
        for i in range(self.loop):
            tdLog.info(f"start modify loop={i}")
            idx = random.randint(0, self.topicNum - 1)
            # delete
            topicName = f"{self.topicName}{idx}"
            sql = f"drop topic if exist {topicName}"
            try:
                tdSql.execute(sql, 3, True)
            except:
                tdLog.info(f" drop topic {topicName} failed.")
                     

            # create topic
            sql = random.choice(self.sqls)
            createSql = f"create topic if not exists {topicName} as {sql}"
            try:
               tdSql.execute(createSql, 3, True)
            except:
                tdLog.info(f" create topic {topicName} failed.")

            seconds = [0.1, 0.5, 3, 2.5, 1.5, 0.4, 5.2, 2.6, 0.4, 0.2]
            time.sleep(random.choice(seconds))


    # run
    def run(self):    
        # prepare env
        self.prepareEnv()

        # create topic
        self.createTopics()

        # modify topic
        self.managerTopics()
        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
