
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
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        #tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def checkJson(self, cfgPath, name):
        srcFile = '%s/../log/%s.source'%(cfgPath, name)
        dstFile = '%s/../log/%s.result'%(cfgPath, name)
        tdLog.info("compare file: %s, %s"%(srcFile, dstFile))

        consumeFile = open(srcFile, mode='r')
        queryFile = open(dstFile, mode='r')

        while True:
            dst = queryFile.readline()
            src = consumeFile.readline()
            if src:
                if dst != src:
                    tdLog.exit("compare error: %s != %s"%(src, dst))
            else:
                break
        return

    def checkDropData(self, drop):
        tdSql.execute('use db_taosx')
        tdSql.query("show tables")
        if drop:
            tdSql.checkRows(11)
        else:
            tdSql.checkRows(16)
        tdSql.query("select * from jt order by i")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 11)
        tdSql.checkData(0, 2, '{"k1":1,"k2":"hello"}')
        tdSql.checkData(1, 2, None)

        tdSql.query("select * from sttb order by ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 13)
        tdSql.checkData(1, 1, 16)
        tdSql.checkData(0, 2, 22)
        tdSql.checkData(1, 2, 25)
        tdSql.checkData(0, 5, "sttb3")
        tdSql.checkData(1, 5, "sttb4")

        tdSql.query("select * from stt order by ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(2, 1, 21)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(2, 2, 21)
        tdSql.checkData(0, 5, "stt3")
        tdSql.checkData(2, 5, "stt4")

        tdSql.execute('use abc1')
        tdSql.query("show tables")
        if drop:
            tdSql.checkRows(11)
        else:
            tdSql.checkRows(16)
        tdSql.query("select * from jt order by i")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 11)
        tdSql.checkData(0, 2, '{"k1":1,"k2":"hello"}')
        tdSql.checkData(1, 2, None)

        tdSql.query("select * from sttb order by ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 13)
        tdSql.checkData(1, 1, 16)
        tdSql.checkData(0, 2, 22)
        tdSql.checkData(1, 2, 25)
        tdSql.checkData(0, 5, "sttb3")
        tdSql.checkData(1, 5, "sttb4")

        tdSql.query("select * from stt order by ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(2, 1, 21)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(2, 2, 21)
        tdSql.checkData(0, 5, "stt3")
        tdSql.checkData(2, 5, "stt4")

        return

    def checkDataTable(self):
        tdSql.execute('use db_taosx')
        tdSql.query("select * from meters_summary")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 120)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, "San Francisco")

        tdSql.execute('use abc1')
        tdSql.query("select * from meters_summary")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 120)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, "San Francisco")

        return

    def checkData(self):
        tdSql.execute('use db_taosx')
        tdSql.query("select * from tb1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, 1)

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

        time.sleep(10)
        tdSql.query("select * from information_schema.ins_tables where table_name = 'stt4'")
        uid1 = tdSql.getData(0, 5)
        uid2 = tdSql.getData(1, 5)
        tdSql.checkNotEqual(uid1, uid2)
        return

    def checkWal1Vgroup(self):
        buildPath = tdCom.getBuildPath()
        cfgPath = tdCom.getClientCfgPath()
        cmdStr = '%s/build/bin/tmq_taosx_ci -c %s -sv 1 -dv 1'%(buildPath, cfgPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        self.checkJson(cfgPath, "tmq_taosx_tmp")
        self.checkData()
        self.checkDropData(False)

        return

    def checkWal1VgroupOnlyMeta(self):
        buildPath = tdCom.getBuildPath()
        cfgPath = tdCom.getClientCfgPath()
        cmdStr = '%s/build/bin/tmq_taosx_ci -c %s -sv 1 -dv 1 -d -onlymeta'%(buildPath, cfgPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        self.checkJson(cfgPath, "tmq_taosx_tmp")

        return

    def checkWal1VgroupTable(self):
        buildPath = tdCom.getBuildPath()
        cfgPath = tdCom.getClientCfgPath()
        cmdStr = '%s/build/bin/tmq_taosx_ci -c %s -sv 1 -dv 1 -t'%(buildPath, cfgPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        self.checkJson(cfgPath, "tmq_taosx_tmp")
        self.checkDataTable()

        return

    def checkWalMultiVgroups(self):
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_taosx_ci -sv 3 -dv 5'%(buildPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        self.checkData()
        self.checkDropData(False)

        return

    def checkWalMultiVgroupsWithDropTable(self):
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_taosx_ci -sv 3 -dv 5 -d'%(buildPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        self.checkDropData(True)

        return

    def checkSnapshot1Vgroup(self):
        buildPath = tdCom.getBuildPath()
        cfgPath = tdCom.getClientCfgPath()
        cmdStr = '%s/build/bin/tmq_taosx_ci -c %s -sv 1 -dv 1 -s'%(buildPath, cfgPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        self.checkJson(cfgPath, "tmq_taosx_tmp_snapshot")
        self.checkData()
        self.checkDropData(False)

        return

    def checkSnapshot1VgroupTable(self):
        buildPath = tdCom.getBuildPath()
        cfgPath = tdCom.getClientCfgPath()
        cmdStr = '%s/build/bin/tmq_taosx_ci -c %s -sv 1 -dv 1 -s -t'%(buildPath, cfgPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        self.checkJson(cfgPath, "tmq_taosx_tmp_snapshot")
        self.checkDataTable()

        return

    def checkSnapshotMultiVgroups(self):
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_taosx_ci -sv 2 -dv 4 -s'%(buildPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        self.checkData()
        self.checkDropData(False)

        return

    def checkSnapshotMultiVgroupsWithDropTable(self):
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_taosx_ci -sv 2 -dv 4 -s -d'%(buildPath)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        self.checkDropData(True)

        return

    def consumeTest(self):
        tdSql.execute(f'create database if not exists d1 vgroups 1')
        tdSql.execute(f'use d1')
        tdSql.execute(f'create table st(ts timestamp, i int) tags(t int)')
        tdSql.execute(f'insert into t1 using st tags(1) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into t2 using st tags(2) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into t3 using st tags(3) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into t1 using st tags(1) values(now+5s, 11) (now+10s, 12)')

        tdSql.query("select * from st")
        tdSql.checkRows(8)

        tdSql.execute(f'create topic topic_all with meta as database d1')
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

        index = 0
        try:
            while True:
                res = consumer.poll(1)
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
            consumer.close()

    def run(self):
        self.consumeTest()

        tdSql.prepare()
        self.checkWal1VgroupOnlyMeta()

        self.checkWal1Vgroup()
        self.checkSnapshot1Vgroup()

        self.checkWal1VgroupTable()
        self.checkSnapshot1VgroupTable()

        self.checkWalMultiVgroups()
        self.checkSnapshotMultiVgroups()

        self.checkWalMultiVgroupsWithDropTable()

        self.checkSnapshotMultiVgroupsWithDropTable()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
