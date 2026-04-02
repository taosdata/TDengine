import threading
import time

from new_test_framework.utils import tdLog, tdSql, tdCom, clusterComCheck, cluster, tmqCom
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


class TestCase:
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
        cls.vgroups    = 1
        cls.ctbNum     = 10
        cls.rowsPerTbl = 1000

    def prepareTestEnv(self):
        tdLog.printNoPrefix("======== prepare test env include database, stable, ctables, and insert data: ")
        paraDict = {'dbName':     'dbt',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    1,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1},{'type': 'TIMESTAMP', 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbStartIdx': 0,
                    'ctbNum':     10,
                    'rowsPerTbl': 1000,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  120,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   0}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        tdCom.drop_all_db()
        tmqCom.initConsumerTable()
        tdCom.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], wal_retention_period=36000,vgroups=paraDict["vgroups"],replica=self.replicaVar)
        tdLog.info("create stb")
        tdSql.query("create table dbt.t(ts timestamp, v int)")
        tdSql.query("insert into dbt.t values('2022-01-01 00:00:00.000', 0)")
        tdSql.query("insert into dbt.t values('2022-01-01 00:00:02.000', 0)")
        tdSql.query("insert into dbt.t values('2022-01-01 00:00:03.000', 0)")
        return

    def tmqCase1(self, deleteWal=False):
        tdLog.printNoPrefix("======== test case 1: ")
        paraDict = {'dbName':     'dbt',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    1,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1},{'type': 'TIMESTAMP', 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1}],
                    'ctbPrefix':  'ctb1',
                    'ctbStartIdx': 0,
                    'ctbNum':     10,
                    'rowsPerTbl': 1000,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  2,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   0}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        topicNameList = ['topic1']
        # expectRowsList = []
        tmqCom.initConsumerTable()

        tdLog.info("create topics from ntb with filter")
        queryString = "select * from %s.t"%(paraDict['dbName'])
        sqlString = "create topic %s as %s" %(topicNameList[0], queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)

        # init consume info, and start tmq_sim, then check consume result
        tdLog.info("insert consume info to consume processor")
        consumerId   = 0
        expectrowcnt = paraDict["rowsPerTbl"] * paraDict["ctbNum"] * 2
        topicList    = topicNameList[0]
        ifcheckdata  = 1
        ifManualCommit = 1
        keyList      = 'group.id:cgrp1, enable.auto.commit:true, auto.commit.interval.ms:200, auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor")
        tmqCom.startTmqSimProcess(pollDelay=paraDict['pollDelay'],dbName=paraDict["dbName"],showMsg=paraDict['showMsg'], showRow=paraDict['showRow'],snapshot=paraDict['snapshot'])
        tdLog.info("wait the consume result")


        tmqCom.getStartConsumeNotifyFromTmqsim()
        tmqCom.getStartCommitNotifyFromTmqsim()

        #restart dnode & remove wal
        tmqCom.restartAndRemoveWal(deleteWal)

        # split vgroup
        tmqCom.splitVgroups()
        tmqCom.checkSplitVgroups()

        clusterComCheck.check_vgroups_status(vgroup_numbers=2,db_replica=self.replicaVar,db_name="dbt",count_number=240)

        time.sleep(3)
        for i in range(len(topicNameList)):
            tdSql.query("drop topic %s"%topicNameList[i])
        tdLog.printNoPrefix("======== test case 1 end ...... ")

    def test_tmq_vnode_split_ntb_sel(self):
        """Cluster: vnode split ntb
        
        1. Create normal table subscription
        2. Insert data
        3. Trigger vnode split
        4. Continue consumption
        5. Verify normal table unaffected
        6. Check data integrity
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_tmqVnodeSplit_ntb_select.py

        """
        self.prepareTestEnv()
        self.tmqCase1(True)



event = threading.Event()

