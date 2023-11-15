
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
    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), False)


    def case1(self):
        tdLog.debug("case1 start")

        os.system("nohup taosBenchmark -y -B 1 -t 4 -S 1000 -n 1000 -i 1000 -v 2  > /dev/null 2>&1 &")
        time.sleep(1)
        tdSql.query("use test")
        tdSql.query("create snode on dnode 4")
        tdSql.query("create stream if not exists s1 trigger at_once  ignore expired 0 ignore update 0  fill_history 1 into st1 as select _wstart, sum(voltage)  from meters partition by groupid interval(4s)")
        tdLog.debug("create stream use snode and insert data")
        time.sleep(10)

        tdDnodes = cluster.dnodes
        tdDnodes[3].stoptaosd()
        time.sleep(2)
        tdDnodes[3].starttaosd()
        tdLog.debug("snode restart ok")

        time.sleep(500)
        os.system("kill -9 `pgrep taosBenchmark`")

        tdSql.query("select sum(voltage) from meters partition by groupid interval(4s)")
        # tdSql.checkRows(1)
        #
        # tdSql.checkData(0, 0, '2016-01-01 08:00:07.000')
        # tdSql.checkData(0, 1, 2000)

        tdSql.query("drop snode on dnode 4")
        tdSql.query("drop stream if exists s1")
        tdSql.query("drop database test")

        tdLog.debug("case1 end")

    def case2(self):
        tdLog.debug("case2 start")

        updatecfgDict = {'checkpointInterval': 5}
        print("===================: ", updatecfgDict)

        tdDnodes = cluster.dnodes
        tdDnodes[0].stoptaosd()
        tdDnodes[1].stoptaosd()
        tdDnodes[2].stoptaosd()
        tdDnodes[3].stoptaosd()
        time.sleep(2)
        tdDnodes[0].starttaosd()
        tdDnodes[1].starttaosd()
        tdDnodes[2].starttaosd()
        tdDnodes[3].starttaosd()
        tdLog.debug("taosd restart ok")

        os.system("taosBenchmark -y -B 1 -t 4 -S 1000 -n 1000 -i 1000 -v 2 &" )
        time.sleep(1)
        tdSql.query("use test")
        tdSql.query("create snode on dnode 4")
        tdSql.query("create stream if not exists s1 trigger at_once  ignore expired 0 ignore update 0  fill_history 1 into st1 as select sum(voltage)  from meters partition by groupid interval(4s)")
        tdLog.debug("create stream use snode and insert data")
        time.sleep(10)

        tdDnodes[3].stoptaosd()
        time.sleep(2)
        tdDnodes[3].starttaosd()
        tdLog.debug("snode restart ok")

        time.sleep(5)
        os.system("kill -9 `pgrep taosBenchmark`")

        tdSql.query("select sum(voltage) from meters partition by groupid interval(4s)")
        # tdSql.checkRows(1)
        #
        # tdSql.checkData(0, 0, '2016-01-01 08:00:07.000')
        # tdSql.checkData(0, 1, 2000)

        tdLog.debug("case2 end")

    def run(self):
        self.case1()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
