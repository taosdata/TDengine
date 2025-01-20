
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
from tmqCommon import *

class TDTestCase:
    clientCfgDict = {'debugFlag': 135}
    updatecfgDict = {'debugFlag': 135, 'clientCfg':clientCfgDict}
    # updatecfgDict = {'debugFlag': 135, 'clientCfg':clientCfgDict, 'tmqRowSize':1}

    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def consume_test(self):

        tdSql.execute(f'create database if not exists d1')
        tdSql.execute(f'use d1')
        tdSql.execute(f'create table st(ts timestamp, i int) tags(t int)')
        tdSql.execute(f'insert into t1 using st tags(1) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into t2 using st tags(2) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into t3 using st tags(3) values(now, 1) (now+1s, 2)')


        tdSql.execute(f'create topic topic_all as select * from st')
        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.unsubscribe()
            consumer.unsubscribe()
            consumer.subscribe(["topic_all"])
            consumer.subscribe(["topic_all"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        cnt = 0
        try:
            while True:
                res = consumer.poll(2)
                if not res:
                    break
                val = res.value()
                if val is None:
                    print(f"null val")
                    continue
                for block in val:
                    cnt += len(block.fetchall())

                print(f"block {cnt} rows")

        finally:
            consumer.unsubscribe();
            consumer.close()
    def run(self):
        self.consume_test()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
