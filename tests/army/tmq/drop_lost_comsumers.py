
import taos
import sys
import time
import socket
import os
import threading

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from taos.tmq import *
import frame.etool

class TDTestCase:
    # updatecfgDict = {'debugFlag': 135, 'asynclog': 0}

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        #tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def td_31283_test(self):
        tdSql.execute(f'create database if not exists d1 vgroups 1')
        tdSql.execute(f'use d1')
        tdSql.execute(f'create table st(ts timestamp, i int) tags(t int)')
        tdSql.execute(f'insert into t1 using st tags(1) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into t2 using st tags(2) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into t3 using st tags(3) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into t1 using st tags(1) values(now+5s, 11) (now+10s, 12)')

        tdSql.query("select * from st")
        tdSql.checkRows(8)
        
        tdSql.error(f'create topic t1 with meta as database d2', expectErrInfo="Database not exist")
        tdSql.error(f'create topic t1 as database d2', expectErrInfo="Database not exist")
        tdSql.error(f'create topic t2 as select * from st2', expectErrInfo="Fail to get table info, error: Table does not exist")
        tdSql.error(f'create topic t3 as stable st2', expectErrInfo="STable not exist")
        tdSql.error(f'create topic t3 with meta as stable st2', expectErrInfo="STable not exist")

        tdSql.execute(f'create topic t1 with meta as database d1')
        
        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
            # "msg.enable.batchmeta": "true",
            "experimental.snapshot.enable": "true",
        }
        consumer1 = Consumer(consumer_dict)

        try:
            consumer1.subscribe(["t1"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        index = 0
        try:
            while True:
                res = consumer1.poll(1)
                if not res:
                    if index != 1:
                        tdLog.exit("consume error")
                    break
                val = res.value()
                if val is None:
                    continue
                cnt = 0;
                for block in val:
                    cnt += len(block.fetchall())

                if cnt != 8:
                    tdLog.exit("consume error")

                index += 1
        finally:
            consumer1.close()


        tdSql.query(f'show consumers')
        tdSql.checkRows(0)

        tdSql.execute(f'drop topic t1')
        tdSql.execute(f'drop database d1')

    def run(self):
        self.td_31283_test()
        

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
