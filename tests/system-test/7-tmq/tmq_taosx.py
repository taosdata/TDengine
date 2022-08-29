
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
    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        #tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def checkFileContent(self):
        buildPath = tdCom.getBuildPath()
        cfgPath = tdCom.getClientCfgPath()
        cmdStr = '%s/build/bin/tmq_taosx_ci -c %s'%(buildPath, cfgPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        srcFile = '%s/../log/tmq_taosx_tmp.source'%(cfgPath)
        dstFile = '%s/../log/tmq_taosx_tmp.result'%(cfgPath)
        tdLog.info("compare file: %s, %s"%(srcFile, dstFile))

        consumeFile = open(srcFile, mode='r')
        queryFile = open(dstFile, mode='r')

        while True:
            dst = queryFile.readline()
            src = consumeFile.readline()

            if dst:
                if dst != src:
                    tdLog.exit("compare error: %s != %s"%src, dst)
            else:
                break

        tdSql.execute('use db_taosx')
        tdSql.query("select * from ct3 order by c1 desc")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 51)
        tdSql.checkData(0, 4, 940)
        tdSql.checkData(1, 1, 23)
        tdSql.checkData(1, 4, None)

        tdSql.query("select * from st1 order by ts")
        tdSql.checkRows(8)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(4, 1, 4)
        tdSql.checkData(6, 1, 23)

        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(4, 2, 3)
        tdSql.checkData(6, 2, 32)

        tdSql.checkData(0, 3, 'a')
        tdSql.checkData(1, 3, 'b')
        tdSql.checkData(4, 3, 'hwj')
        tdSql.checkData(6, 3, 's21ds')

        tdSql.checkData(0, 4, None)
        tdSql.checkData(1, 4, None)
        tdSql.checkData(5, 4, 940)
        tdSql.checkData(6, 4, None)

        tdSql.checkData(0, 5, 1000)
        tdSql.checkData(1, 5, 2000)
        tdSql.checkData(4, 5, 1000)
        tdSql.checkData(6, 5, 5000)

        tdSql.checkData(0, 6, 'ttt')
        tdSql.checkData(1, 6, None)
        tdSql.checkData(4, 6, 'ttt')
        tdSql.checkData(6, 6, None)

        tdSql.checkData(0, 7, True)
        tdSql.checkData(1, 7, None)
        tdSql.checkData(4, 7, True)
        tdSql.checkData(6, 7, None)

        tdSql.checkData(0, 8, None)
        tdSql.checkData(1, 8, None)
        tdSql.checkData(4, 8, None)
        tdSql.checkData(6, 8, None)

        tdSql.query("select * from ct1")
        tdSql.checkRows(4)

        tdSql.query("select * from ct2")
        tdSql.checkRows(0)

        tdSql.query("select * from ct0 order by c1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 3, "a")
        tdSql.checkData(1, 4, None)

        tdSql.query("select * from n1 order by cc3 desc")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, "eeee")
        tdSql.checkData(1, 2, 940)

        tdSql.query("select * from jt order by i desc")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 11)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, '{"k1":1,"k2":"hello"}')

        tdSql.execute('drop topic if exists topic_ctb_column')
        return

    def checkFileContentSnapshot(self):
        buildPath = tdCom.getBuildPath()
        cfgPath = tdCom.getClientCfgPath()
        cmdStr = '%s/build/bin/tmq_taosx_snapshot_ci -c %s'%(buildPath, cfgPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        srcFile = '%s/../log/tmq_taosx_tmp_snapshot.source'%(cfgPath)
        dstFile = '%s/../log/tmq_taosx_tmp_snapshot.result'%(cfgPath)
        tdLog.info("compare file: %s, %s"%(srcFile, dstFile))

        consumeFile = open(srcFile, mode='r')
        queryFile = open(dstFile, mode='r')

        while True:
            dst = queryFile.readline()
            src = consumeFile.readline()

            if dst:
                if dst != src:
                    tdLog.exit("compare error: %s != %s"%src, dst)
            else:
                break

        tdSql.execute('use db_taosx')
        tdSql.query("select * from ct3 order by c1 desc")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 51)
        tdSql.checkData(0, 4, 940)
        tdSql.checkData(1, 1, 23)
        tdSql.checkData(1, 4, None)

        tdSql.query("select * from st1 order by ts")
        tdSql.checkRows(8)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(4, 1, 4)
        tdSql.checkData(6, 1, 23)

        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(4, 2, 3)
        tdSql.checkData(6, 2, 32)

        tdSql.checkData(0, 3, 'a')
        tdSql.checkData(1, 3, 'b')
        tdSql.checkData(4, 3, 'hwj')
        tdSql.checkData(6, 3, 's21ds')

        tdSql.checkData(0, 4, None)
        tdSql.checkData(1, 4, None)
        tdSql.checkData(5, 4, 940)
        tdSql.checkData(6, 4, None)

        tdSql.checkData(0, 5, 1000)
        tdSql.checkData(1, 5, 2000)
        tdSql.checkData(4, 5, 1000)
        tdSql.checkData(6, 5, 5000)

        tdSql.checkData(0, 6, 'ttt')
        tdSql.checkData(1, 6, None)
        tdSql.checkData(4, 6, 'ttt')
        tdSql.checkData(6, 6, None)

        tdSql.checkData(0, 7, True)
        tdSql.checkData(1, 7, None)
        tdSql.checkData(4, 7, True)
        tdSql.checkData(6, 7, None)

        tdSql.checkData(0, 8, None)
        tdSql.checkData(1, 8, None)
        tdSql.checkData(4, 8, None)
        tdSql.checkData(6, 8, None)

        tdSql.query("select * from ct1")
        tdSql.checkRows(4)

        tdSql.query("select * from ct2")
        tdSql.checkRows(0)

        tdSql.query("select * from ct0 order by c1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 3, "a")
        tdSql.checkData(1, 4, None)

        tdSql.query("select * from n1 order by cc3 desc")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, "eeee")
        tdSql.checkData(1, 2, 940)

        tdSql.query("select * from jt order by i desc")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 11)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, '{"k1":1,"k2":"hello"}')

        return

    def run(self):
        tdSql.prepare()
        self.checkFileContent()
        self.checkFileContentSnapshot()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
