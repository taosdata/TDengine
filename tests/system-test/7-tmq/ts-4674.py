
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

    #tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    # def consume_TS_4674_Test(self):
    #
    #     os.system("nohup taosBenchmark -y -B 1 -t 4 -S 1000 -n 1000000 -i 1000 -v 1 -a 3 > /dev/null 2>&1 &")
    #     time.sleep()
    #     tdSql.execute(f'create topic topic_all with meta as database test')
    #     consumer_dict = {
    #         "group.id": "g1",
    #         "td.connect.user": "root",
    #         "td.connect.pass": "taosdata",
    #         "auto.offset.reset": "earliest",
    #     }
    #     consumer = Consumer(consumer_dict)
    #
    #     try:
    #         consumer.subscribe(["topic_all"])
    #     except TmqError:
    #         tdLog.exit(f"subscribe error")
    #
    #     try:
    #         while True:
    #             res = consumer.poll(5)
    #             if not res:
    #                 print(f"null")
    #                 continue
    #             val = res.value()
    #             if val is None:
    #                 print(f"null")
    #                 continue
    #             cnt = 0;
    #             for block in val:
    #                 cnt += len(block.fetchall())
    #
    #             print(f"block {cnt} rows")
    #
    #     finally:
    #         consumer.close()

    def get_leader(self):
        tdLog.debug("get leader")
        tdSql.query("show vnodes")
        for result in tdSql.queryResult:
            if result[3] == 'leader':
                tdLog.debug("leader is %d"%(result[0]))
                return result[0]
        return -1

    def balance_vnode(self):
        leader_before = self.get_leader()

        while True:
            leader_after = -1
            tdSql.query("balance vgroup leader")
            while True:
                leader_after = self.get_leader()
                if leader_after != -1 :
                    break;
                else:
                    time.sleep(1)
            if leader_after != leader_before:
                tdLog.debug("leader changed")
                break;
            else :
                time.sleep(1)


    def consume_TS_4674_Test(self):

        tdSql.execute(f'create database if not exists d1 replica 3 vgroups 1')
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
            consumer.subscribe(["topic_all"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        cnt = 0;
        balance = False
        try:
            while True:
                res = consumer.poll(2)
                if not res:
                    print(f"null res")
                    if balance == False and cnt != 6 :
                        tdLog.exit(f"subscribe num != 6")
                    if balance == True :
                        if cnt != 8 :
                            tdLog.exit(f"subscribe num != 8")
                            # tdLog.debug(f"subscribe num != 8")
                            # continue
                        else :
                            break
                    self.balance_vnode()
                    balance = True
                    tdSql.execute(f'insert into t1 using st tags(1) values(now+5s, 11) (now+10s, 12)')
                    continue
                val = res.value()
                if val is None:
                    print(f"null val")
                    continue
                for block in val:
                    cnt += len(block.fetchall())

                print(f"block {cnt} rows")

        finally:
            consumer.close()
    def run(self):
        self.consume_TS_4674_Test()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
