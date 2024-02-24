import taos
import sys
import os
import subprocess
import glob
import shutil
import time

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.srvCtl import *
from frame.caseBase import *
from frame import *
from frame.autogen import *
# from frame.server.dnodes import *
# from frame.server.cluster import *


class TDTestCase(TBase):
    updatecfgDict = {
        'slowLogScope':"query"
    }

    def init(self, conn, logSql, replicaVar=3):
        super(TDTestCase, self).init(conn, logSql, replicaVar=3, db="snapshot", checkColName="c1")
        self.valgrind = 0
        self.childtable_count = 10
        # tdSql.init(conn.cursor())
        tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def run(self):
        tdSql.prepare()
        autoGen = AutoGen()
        autoGen.create_db(self.db, 2, 3)
        tdSql.execute(f"use {self.db}")
        autoGen.create_stable(self.stb, 5, 10, 8, 8)
        autoGen.create_child(self.stb, "d", self.childtable_count)
        autoGen.insert_data(1000)
        tdSql.execute(f"flush database {self.db}")
        sc.dnodeStop(3)
        # clusterDnodes.stoptaosd(1)
        # clusterDnodes.starttaosd(3)
        # time.sleep(5)
        # clusterDnodes.stoptaosd(2)
        # clusterDnodes.starttaosd(1)
        # time.sleep(5)
        autoGen.insert_data(5000, True)
        tdSql.execute(f"flush database {self.db}")

        # sql = 'show vnodes;'
        # while True:
        #     bFinish = True
        #     param_list = tdSql.query(sql, row_tag=True)
        #     for param in param_list:
        #         if param[3] == 'leading' or param[3] == 'following':
        #             bFinish = False
        #             break
        #     if bFinish:
        #         break
        self.snapshotAgg()
        time.sleep(10)
        sc.dnodeStopAll()
        for i in range(1, 4):
            path = clusterDnodes.getDnodeDir(i)
            dnodesRootDir = os.path.join(path,"data","vnode", "vnode*")
            dirs = glob.glob(dnodesRootDir)
            for dir in dirs:
                if os.path.isdir(dir):
                    tdLog.debug("delete dir: %s " % (dnodesRootDir))
                    self.remove_directory(os.path.join(dir, "wal"))

        sc.dnodeStart(1)
        sc.dnodeStart(2)
        sc.dnodeStart(3)
        sql = "show vnodes;"
        time.sleep(10)
        while True:
            bFinish = True
            param_list = tdSql.query(sql, row_tag=True)
            for param in param_list:
                if param[3] == 'offline':
                    tdLog.exit(
                        "dnode synchronous fail dnode id: %d, vgroup id:%d status offline" % (param[0], param[1]))
                if param[3] == 'leading' or param[3] == 'following':
                    bFinish = False
                    break
            if bFinish:
                break

        self.timestamp_step = 1
        self.insert_rows = 6000
        self.checkInsertCorrect()
        self.checkAggCorrect()

    def remove_directory(self, directory):
        try:
            shutil.rmtree(directory)
            tdLog.debug("delete dir: %s " % (directory))
        except OSError as e:
            tdLog.exit("delete fail dir: %s " % (directory))

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
