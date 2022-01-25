
# -*- coding: utf-8 -*-

import random
import string
import subprocess
import sys
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
    def run(self):
        tdLog.debug("check databases")
        tdSql.prepare()
 
        tdSql.execute("create database if not exists db")
        tdSql.execute("use db")
        tdSql.execute("create stable `sch.job.create` (`ts` TIMESTAMP,`value` DOUBLE) TAGS (`endpoint` NCHAR(7),`task.type` NCHAR(3))")
        tdSql.execute("alter table `sch.job.create` modify tag `task.type` NCHAR(4)")
        tdSql.execute("drop database db") 

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
