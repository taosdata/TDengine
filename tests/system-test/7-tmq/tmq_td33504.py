
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
from util.common import *
from taos.tmq import *
from taos import *

sys.path.append("./7-tmq")
from tmqCommon import *

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        #tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def test(self):
        tdSql.execute(f'create database if not exists db')
        tdSql.execute(f'use db')
        tdSql.execute(f'CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)')
        tdSql.execute("INSERT INTO d1001 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-05 14:38:05.000',10.30000,219,0.31000)")
        tdSql.execute("INSERT INTO d1002 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-05 14:38:05.000',10.30000,219,0.31000)")
        tdSql.execute("INSERT INTO d1003 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-05 14:38:05.000',10.30000,219,0.31000)")
        tdSql.execute("INSERT INTO d1004 USING meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-05 14:38:05.000',10.30000,219,0.31000)")

        tdSql.execute(f'create topic t0 as select * from meters')
        tdSql.execute(f'create topic t1 as select * from meters')

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["t0"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        try:
            res = consumer.poll(1)
            print(res)

            consumer.unsubscribe()

            try:
                consumer.subscribe(["t1"])
            except TmqError:
                tdLog.exit(f"subscribe error")


            res = consumer.poll(1)
            print(res)
            if res == None and taos_errno(None) != 0:
                tdLog.exit(f"poll error %d" % taos_errno(None))

        except TmqError:
            tdLog.exit(f"poll error")
        finally:
            consumer.close()


    def run(self):
        self.test()


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
