import taos
import sys
import time
import socket
import os
import threading
import random
import string

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *

class TDTestCase:
    hostname = socket.gethostname()

    def init(self, conn, logSql, replicaVar=1):
        random.seed(1)
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), logSql)  # output sql.txt file
    
    def run(self):
        count = 100000
        tdSql.prepare("db", False, vgroups=1)
        #tdLog.info("create database..................")
        #tdSql.execute("drop database db");
        #tdSql.execute("create database if not exists db vgroups 1")

        tdLog.info("create super table")
        tdSql.execute("create stable db.stb (ts timestamp, c1 int, c2 int) tags (tag1 varchar(1026))")

        tdLog.info("create tables")
        for i in range(1, count):
            if i % 1000 == 0:
                tdLog.info(f"create table db.t{i}")
            tag = ''.join(random.choice(string.ascii_letters) for _ in range(1026))
            tdSql.execute(f"create table db.t{i} using db.stb tags ('{tag}')")

        tdLog.info("drop tables")
        for i in range(1, count):
            if i % 1000 == 0:
                tdLog.info(f"drop table db.t{i}")
            tdSql.execute(f"drop table db.t{i}")


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())