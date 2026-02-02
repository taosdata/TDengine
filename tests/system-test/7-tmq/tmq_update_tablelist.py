import random
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

sys.path.append("./7-tmq")

class TDTestCase:
    updatecfgDict = {'debugFlag': 135, 'asynclog': 1}
    clientCfgDict = {'debugFlag': 131, 'asynclog': 1}
    updatecfgDict["clientCfg"] = clientCfgDict

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def consume1(self):
        tdSql.execute(f'use db_alter_tag')
        tdSql.execute(f'create topic t0 as stable stb where id > 2')

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

        tdLog.info("subscribe success")
        tdSql.execute("ALTER TABLE tb1 SET TAG id = 10, t1 = 0")

        cnt = 0
        try:
            while True:
                res = consumer.poll(5)
                if not res:
                    break
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    cnt += len(block.fetchall())
        finally:
            consumer.close()

        if cnt != 3:
            tdLog.exit(f"consume 1 error, cnt: {cnt}")
        tdLog.info(f"consume1 completed, total rows: {cnt}")

    def consume2(self):
        tdSql.execute(f'use db_alter_tag')
        tdSql.execute(f'create topic t1 as stable stb where t1 > 2')

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["t1"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        tdLog.info("subscribe success")
        tdSql.execute("ALTER TABLE tb1 SET TAG id = 6")

        cnt = 0
        try:
            while True:
                res = consumer.poll(5)
                if not res:
                    break
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    cnt += len(block.fetchall())
                    
        finally:
            consumer.close()
        if cnt != 2:
            tdLog.exit(f"consume 2 error, cnt: {cnt}")
        tdLog.info(f"consume2 completed, total rows: {cnt}")


    def insertData(self):
        tdSql.execute("create database db_alter_tag vgroups 1")
        tdSql.execute("use db_alter_tag")
        tdSql.execute("create table stb (ts timestamp, val double) tags (id int, t1 int)")
        tdSql.execute("insert into tb1 using stb tags (1, 1) values (now, 10.5)")
        tdSql.execute("insert into tb2 using stb tags (2, 2) values (now, 20.5)")
        tdSql.execute("insert into tb3 using stb tags (3, 3) values (now, 30.5)")
        tdSql.execute("insert into tb4 using stb tags (4, 4) values (now, 40.5)")

    def run(self):
        self.insertData()
        self.consume1()
        self.consume2()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
