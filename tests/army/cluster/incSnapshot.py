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
from frame.clusterCommonCheck import *

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
        autoGen.insert_data(5000, True)
        self.flushDb(True)
        # wait flush operation over
        time.sleep(5)

        self.snapshotAgg()       
        sc.dnodeStopAll()
        for i in range(1, 4):
            path = clusterDnodes.getDnodeDir(i)
            dnodesRootDir = os.path.join(path,"data","vnode", "vnode*")
            dirs = glob.glob(dnodesRootDir)
            for dir in dirs:
                if os.path.isdir(dir):
                    self.remove_directory(os.path.join(dir, "wal"))

        sc.dnodeStart(1)
        sc.dnodeStart(2)
        sc.dnodeStart(3)
        sql = "show vnodes;"
        clusterComCheck.check_vgroups_status(vgroup_numbers=2,db_replica=3,db_name=f"{self.db}",count_number=60)

        self.timestamp_step = 1000
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
