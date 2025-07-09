import taos
import sys
import time
import socket
import os
import platform
import threading
from enum import Enum

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.cluster import *
from taos.tmq import *
from taos import *

sys.path.append("./7-tmq")
from tmqCommon import *


class TDTestCase:
    global cmd_list
    cmd_list = []

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files or "taosd.exe" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath
    
    def prepareData(self):
        tdLog.info("create database db_repl_1 and insert data")
        cmd1 = "taosBenchmark -y -a 1 -n 100 -t 100 -v 1 -d %s" % ("db_repl_1")
        os.system(cmd1)

        tdLog.info("create database db_repl_2 and insert data")
        cmd2 = "taosBenchmark -y -a 2 -n 100 -t 100 -v 1 -d %s" % ("db_repl_2")
        os.system(cmd2)

        tdLog.info("create database db_repl_3 and insert data")
        cmd3 = "taosBenchmark -y -a 3 -n 100 -t 100 -v 1 -d %s" % ("db_repl_3")
        os.system(cmd3)

    def insertData(self):
        tdLog.info("insert one record into db_repl_*.d0")
        tdSql.execute("insert into db_repl_1.d0 values(now(),6.8358979,250,148.5000000);")
        tdSql.execute("insert into db_repl_2.d0 values(now(),6.8358979,250,148.5000000);")
        tdSql.execute("insert into db_repl_3.d0 values(now(),6.8358979,250,148.5000000);")

    def flushDatabase(self):
        tdLog.info("flush database db_repl_1") 
        tdSql.execute("flush database db_repl_1")

        tdLog.info("flush database db_repl_2")
        tdSql.execute("flush database db_repl_2")

        tdLog.info("flush database db_repl_3")
        tdSql.execute("flush database db_repl_3")

    def checkDatacount(self,expCount):
        tdLog.info("select data count from db_repl_1")
        tdSql.query("select count(*) from db_repl_1.meters")
        actCount = tdSql.getData(0, 0)
        assert actCount == expCount, f"db_repl_1.meters count is {actCount}, expect {expCount}"

        tdLog.info("select data count from db_repl_2")
        tdSql.query("select count(*) from db_repl_2.meters")
        actCount = tdSql.getData(0, 0)
        assert actCount == expCount, f"db_repl_2.meters count is {actCount}, expect {expCount}"


        tdLog.info("select data count from db_repl_3")
        tdSql.query("select count(*) from db_repl_3.meters")
        actCount = tdSql.getData(0, 0)
        assert actCount == expCount, f"db_repl_3.meters count is {actCount}, expect {expCount}"


    def collect_rm_wal_cmds(self):
        global cmd_list
        buildPath = self.getBuildPath()
        rowLen = tdSql.query('show vnodes on dnode 1')
        for i in range(rowLen):
            vgroupId = tdSql.getData(i, 1)
            walPath = buildPath + "/../sim/dnode1/data/vnode/vnode{}/wal/*".format(vgroupId)
            cmd = "rm -rf %s" % walPath
            cmd_list.append(cmd)

        rowLen = tdSql.query('show vnodes on dnode 2')
        for i in range(rowLen):
            vgroupId = tdSql.getData(i, 1)
            walPath = buildPath + "/../sim/dnode2/data/vnode/vnode{}/wal/*".format(vgroupId)
            cmd = "rm -rf %s" % walPath
            cmd_list.append(cmd)

        rowLen = tdSql.query('show vnodes on dnode 3')
        for i in range(rowLen):
            vgroupId = tdSql.getData(i, 1)
            walPath = buildPath + "/../sim/dnode3/data/vnode/vnode{}/wal/*".format(vgroupId)
            cmd = "rm -rf %s" % walPath
            cmd_list.append(cmd)

    def execute_rm_wal_cmds(self):
        for cmd in cmd_list:
            print(cmd)
            os.system(cmd)

    def run(self):  
        print("======== run remove wal test ========")
        self.prepareData()
        self.flushDatabase()

        self.collect_rm_wal_cmds()
        tdSql.execute(f'create topic data_repl_1 as select ts,current from db_repl_1.meters')
        tdSql.execute(f'create topic data_repl_2 as select ts,current from db_repl_2.meters')
        tdSql.execute(f'create topic data_repl_3 as select ts,current from db_repl_3.meters')

        tdDnodes=cluster.dnodes
        tdDnodes[0].stoptaosd()
        tdDnodes[1].stoptaosd()
        tdDnodes[2].stoptaosd()


        time.sleep(10)
        
        self.execute_rm_wal_cmds()

        tdDnodes[0].starttaosd()
        tdDnodes[1].starttaosd()
        tdDnodes[2].starttaosd()      

        self.checkDatacount(10000)
        self.insertData()
        self.checkDatacount(10001)
        
        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["data_repl_1", "data_repl_2", "data_repl_3"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        cnt = 0
        try:
            while True:
                res = consumer.poll(1)
                print(res)
                if not res:
                    print("cnt:",cnt)
                    if cnt == 0 or cnt != 3:
                        tdLog.exit("consume error")
                    break
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    print(block.fetchall(),len(block.fetchall()))
                    cnt += len(block.fetchall())
        finally:
            consumer.close()
        
    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
