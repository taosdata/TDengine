import taos
import sys
import time
import socket
import os
import threading

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *

class TDTestCase:
    hostname = socket.gethostname()

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor())
        tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def create_databases(self):
        tdSql.execute("create database db_ms precision 'ms'")
        tdSql.execute("create database db_us precision 'us'")
        tdSql.execute("create database db_ns precision 'ns'")

    def create_stables(self):
        tdSql.execute("CREATE STABLE db_ms.`meters` (`ts` TIMESTAMP, `c0` INT, `c1` TINYINT, `c2` DOUBLE, `c3` VARCHAR(64), `c4` NCHAR(64)) TAGS (`cc` VARCHAR(16))")
        tdSql.execute("CREATE STABLE db_us.`meters` (`ts` TIMESTAMP, `c0` INT, `c1` TINYINT, `c2` DOUBLE, `c3` VARCHAR(64), `c4` NCHAR(64)) TAGS (`cc` VARCHAR(16))")
        tdSql.execute("CREATE STABLE db_ns.`meters` (`ts` TIMESTAMP, `c0` INT, `c1` TINYINT, `c2` DOUBLE, `c3` VARCHAR(64), `c4` NCHAR(64)) TAGS (`cc` VARCHAR(16))")

    def create_sma_index(self):
        tdSql.execute("create sma index sma_index_ms on db_ms.meters function(max(c1), max(c2), min(c1)) interval(6m, 10s) sliding(6m)" )
        tdSql.execute("create sma index sma_index_us on db_us.meters function(max(c1), max(c2), min(c1)) interval(6m, 10s) sliding(6m)" )
        tdSql.execute("create sma index sma_index_ns on db_ns.meters function(max(c1), max(c2), min(c1)) interval(6m, 10s) sliding(6m)" )

    def run(self):
        tdSql.prepare()
        self.create_databases()
        self.create_stables()
        self.create_sma_index()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
