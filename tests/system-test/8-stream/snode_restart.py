
import taos
import sys
import time
import socket
import os
import threading
import math

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
from util.cluster import *

class TDTestCase:
    updatecfgDict = {'checkpointInterval': 1100}
    print("===================: ", updatecfgDict)

    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)


    def case1(self):
        tdLog.debug("========case1 start========")

        os.system("nohup taosBenchmark -y -B 1 -t 4 -S 1000 -n 1000 -i 1000 -v 2  > /dev/null 2>&1 &")
        time.sleep(4)
        tdSql.query("use test")
        tdSql.query("create snode on dnode 4")
        tdSql.query("create stream if not exists s1 trigger at_once  ignore expired 0 ignore update 0  fill_history 1 into st1 as select _wstart,sum(voltage),groupid from meters partition by groupid interval(2s)")
        tdLog.debug("========create stream useing snode and insert data ok========")
        time.sleep(4)

        tdDnodes = cluster.dnodes
        tdDnodes[3].stoptaosd()
        time.sleep(2)
        tdDnodes[3].starttaosd()
        tdLog.debug("========snode restart ok========")

        time.sleep(30)
        os.system("kill -9 `pgrep taosBenchmark`")
        tdLog.debug("========stop insert ok========")
        time.sleep(2)

        tdSql.query("select _wstart,sum(voltage),groupid from meters partition by groupid interval(2s) order by groupid,_wstart")
        rowCnt = tdSql.getRows()
        results = []
        for i in range(rowCnt):
            results.append(tdSql.getData(i,1))

        tdSql.query("select * from st1 order by groupid,_wstart")
        tdSql.checkRows(rowCnt)
        for i in range(rowCnt):
            data1 = tdSql.getData(i,1)
            data2 = results[i]
            if data1 != data2:
                tdLog.info("num: %d, act data: %d, expect data: %d"%(i, data1, data2))
                tdLog.exit("check data error!")

        # tdLog.debug("========sleep 500s========")
        # time.sleep(500)

        tdLog.debug("case1 end")

    def run(self):
        self.case1()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
