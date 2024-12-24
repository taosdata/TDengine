from ssl import ALERT_DESCRIPTION_CERTIFICATE_UNOBTAINABLE
from numpy import row_stack
import taos
import sys
import time
import os

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import TDDnodes
from util.dnodes import TDDnode
from util.cluster import *
sys.path.append("./6-cluster")
from clusterCommonCreate import *
from clusterCommonCheck import clusterComCheck

import time
import socket
import subprocess
from multiprocessing import Process
import threading
import time
import inspect
import ctypes

class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to excute {__file__}")
        self.TDDnodes = None
        tdSql.init(conn.cursor())
        self.host = socket.gethostname()


    def run(self):
        tdSql.execute('create database if not exists db');
        tdSql.execute('use db')
        tdSql.execute('create table st (ts timestamp, i int, j float, k double) tags(a int)')
        
        for i in range(0, 2):
            tdSql.execute("create table if not exists db.t%d using db.st tags(%d)" % (i, i))
        

        for i in range(2, 4):
            tdSql.execute("create table if not exists db.t%d using db.st tags(%d)" % (i, i))

        sql = "show db.tables"
        tdSql.query(sql)
        tdSql.checkRows(4)
        
        timestamp = 1530374400000
        for i in range (4) :
            val = i
            sql = "insert into db.t%d values(%d, %d, %d, %d)" % (i, timestamp, val, val, val)
            tdSql.execute(sql)

        for i in range ( 4) :
            val = i
            sql = "select * from db.t%d" % (i)
            tdSql.query(sql)
            tdSql.checkRows(1)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
