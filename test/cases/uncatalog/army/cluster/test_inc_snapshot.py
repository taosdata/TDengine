from new_test_framework.utils import tdLog, tdSql, epath, sc, AutoGen, clusterDnodes, clusterComCheck
import time
import os
import glob
import shutil

class TestIncSnapshot:
    updatecfgDict = {
        'slowLogScope':"query"
    }

    def setup_class(cls):
        cls.init(cls, replicaVar=3, db="snapshot", checkColName="c1")
        cls.valgrind = 0
        cls.childtable_count = 10

    def test_inc_snapshot(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx
        """
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

        tdLog.success(f"{__file__} successfully executed")

    def remove_directory(self, directory):
        try:
            shutil.rmtree(directory)
            tdLog.debug("delete dir: %s " % (directory))
        except OSError as e:
            tdLog.exit("delete fail dir: %s " % (directory))


