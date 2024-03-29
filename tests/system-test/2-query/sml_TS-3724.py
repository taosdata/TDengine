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
sys.path.append("./7-tmq")
from tmqCommon import *

class TDTestCase:
    updatecfgDict = {'clientCfg': {'smlChildTableName': 'dataModelName', 'fqdn': 'localhost', 'smlTsDefaultName': "times"}, 'fqdn': 'localhost'}
    print("===================: ", updatecfgDict)

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)
        #tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def checkContent(self, dbname="sml_db"):
        simClientCfg="%s/taos.cfg"%tdDnodes.getSimCfgPath()
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/sml_test %s'%(buildPath, simClientCfg)
        print("cmdStr:", cmdStr)
        tdLog.info(cmdStr)
        ret = os.system(cmdStr)
        if ret != 0:
            tdLog.info("sml_test ret != 0")

        tdSql.query(f"select * from ts3303.stb2")
        tdSql.query(f"select * from ts3303.meters")

        # tdSql.execute('use sml_db')
        tdSql.query(f"select * from {dbname}.t_b7d815c9222ca64cdf2614c61de8f211")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, '2016-01-01 08:00:07.000')
        tdSql.checkData(0, 1, 2000)
        tdSql.checkData(0, 2, 200)
        tdSql.checkData(0, 3, 15)
        tdSql.checkData(0, 4, 24.5208)
        tdSql.checkData(0, 5, 28.09377)
        tdSql.checkData(0, 6, 428)
        tdSql.checkData(0, 7, 0)
        tdSql.checkData(0, 8, 304)
        tdSql.checkData(0, 9, 0)
        tdSql.checkData(0, 10, 25)

        tdSql.query(f"select * from {dbname}.readings")
        tdSql.checkRows(9)

        tdSql.query(f"select distinct tbname from {dbname}.readings")
        tdSql.checkRows(4)

        tdSql.query(f"select * from {dbname}.t_0799064f5487946e5d22164a822acfc8 order by times")
        tdSql.checkRows(2)
        tdSql.checkData(0, 3, "kk")
        tdSql.checkData(1, 3, "")


        tdSql.query(f"select distinct tbname from {dbname}.`sys_if_bytes_out`")
        tdSql.checkRows(2)

        tdSql.query(f"select * from {dbname}.t_f67972b49aa8adf8bca5d0d54f0d850d order by times")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1.300000000)
        tdSql.checkData(1, 1, 13.000000000)

        tdSql.query(f"select * from {dbname}.`sys_procs_running`")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 42.000000000)
        tdSql.checkData(0, 2, "web01")

        tdSql.query(f"select distinct tbname from {dbname}.`sys_cpu_nice`")
        tdSql.checkRows(3)

        tdSql.query(f"select * from {dbname}.`sys_cpu_nice` order by times")
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 13.000000000)
        tdSql.checkData(0, 2, "web01")
        tdSql.checkData(0, 3, None)
        tdSql.checkData(0, 4, 1)

        tdSql.checkData(1, 1, 9.000000000)
        tdSql.checkData(1, 2, "web02")
        tdSql.checkData(3, 3, "t1")
        tdSql.checkData(2, 4, 4)

        tdSql.query(f"select * from {dbname}.macylr")
        tdSql.checkRows(2)

        tdSql.query(f"select * from {dbname}.qelhxo")
        tdSql.checkRows(5)

        tdSql.query(f"desc {dbname}.macylr")
        tdSql.checkRows(25)

        tdSql.query(f"select * from ts3724._stb2")
        tdSql.checkRows(1)

        tdSql.query(f"select * from ts3724.stb_2")
        tdSql.checkRows(1)

        tdSql.query(f"select * from ts3724.stb2_")
        tdSql.checkRows(1)
        return

    def run(self):
        tdSql.prepare()
        self.checkContent()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())

