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


from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import *
import glob
import os

import threading

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):

        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.setsql = TDSetSql()

    def trim_database(self):

        tdSql.execute('create database dbtest vgroups 12 replica 3 duration 60m keep 1d,365d,3650d;')
        tdSql.execute('use dbtest')
        tdSql.execute('create table stb (ts timestamp,c0 int) tags(t0 int)')
        tdSql.execute('create table tb1 using stb tags(1)')

        count = 0
        while count < 1000:
            newTdSql=tdCom.newTdSql()
            threading.Thread(target=self.checkRunTimeError, args=(newTdSql, count)).start()
            count += 1


    def checkRunTimeError(self, newtdSql, i):
        tdLog.info("Thread %d" % i)
        newtdSql.execute(f'insert into dbtest.tb1 values(now-{i}d,10)')
        

    def run(self):
        self.trim_database()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
