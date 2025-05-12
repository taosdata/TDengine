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
        self.database = "test_insert_csv_db"
        self.table = "test_insert_csv_tbl"

    def insert_from_csv(self):
        tdSql.execute(f"drop database if exists {self.database}")
        tdSql.execute(f"create database {self.database}")
        tdSql.execute(f"use {self.database}")
        tdSql.execute(f"create table {self.table} (ts timestamp, c1 nchar(16), c2 double, c3 int)")
        tdSql.execute(f"insert into {self.table} file '{self.testcasePath}//test_insert_from_csv.csv'")
        tdSql.query(f"select count(*) from {self.table}")
        tdSql.checkData(0, 0, 5)

    def run(self):
        tdSql.prepare()
        
        startTime_all = time.time() 
        self.insert_from_csv()
        endTime_all = time.time()
        print("total time %ds" % (endTime_all - startTime_all))        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
        

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
