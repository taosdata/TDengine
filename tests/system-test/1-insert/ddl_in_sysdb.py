import taos
import sys
import datetime
import inspect
import random
from util.dnodes import TDDnode
from util.dnodes import tdDnodes

from util.log import *
from util.sql import *
from util.cases import *

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

        self.testcasePath = os.path.split(__file__)[0]
        self.testcasePath = self.testcasePath.replace('\\', '//')
        self.sysdb = "information_schema"

    def test_ddl(self):
        tdSql.execute(f"use {self.sysdb}")
        tdSql.error(f'create stable ins_test (ts timestamp, a int) tags (b int);', expectErrInfo="Cannot create table of system database", fullMatched=False)
        tdSql.error(f'create table ins_test (ts timestamp, a int);', expectErrInfo="Cannot create table of system database", fullMatched=False)
        tdSql.error(f'drop table ins_users;', expectErrInfo="Cannot drop table of system database", fullMatched=False)

    def run(self):
        tdSql.prepare()
        
        startTime_all = time.time()
        self.test_ddl()
        endTime_all = time.time()
        print("total time %ds" % (endTime_all - startTime_all))

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
        

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
