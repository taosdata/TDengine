
import threading
from enum import Enum

from new_test_framework.utils import tdLog, tdSql, tdCom
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from tmqCommon import tmqCom

class TestCase:
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
        cls.snapshot   = 0
        cls.vgroups    = 4
        cls.ctbNum     = 1000
        cls.rowsPerTbl = 10

    # drop some ntbs
    def tmqCase1(self):
        tdLog.printNoPrefix("======== test case 1: ")
        paraDict = {'dbName':     'dbt',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    4,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'TIMESTAMP', 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1}],
                    'ctbPrefix':  'ntb',
                    'ctbStartIdx': 0,
                    'ctbNum':     1000,
                    'rowsPerTbl': 100,
                    'batchNum':   100,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'endTs': 0,
                    'pollDelay':  5,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   0}
        paraDict['snapshot'] = self.snapshot
        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        tmqCom.initConsumerTable()
        tdLog.info("start create database....")
        tdCom.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], vgroups=paraDict["vgroups"],replica=1)
        tdLog.info("start create normal tables....")
        tmqCom.create_ntable(tsql=tdSql, dbname=paraDict["dbName"], tbname_prefix=paraDict["ctbPrefix"], tbname_index_start_num = 1, column_elm_list=paraDict["colSchema"], colPrefix='c', tblNum=paraDict["ctbNum"])
        tdLog.info("start insert data into normal tables....")
        tmqCom.insert_rows_into_ntbl(tsql=tdSql, dbname=paraDict["dbName"], tbname_prefix=paraDict["ctbPrefix"], tbname_index_start_num = 1, column_ele_list=paraDict["colSchema"],startTs=paraDict["startTs"], tblNum=paraDict["ctbNum"], rows=paraDict["rowsPerTbl"])

        tdLog.info("create topics from database")
        topicFromDb = 'topic_dbt'
        tdSql.execute("create topic %s as database %s" %(topicFromDb, paraDict['dbName']))

        if self.snapshot == 0:
            consumerId     = 0
        elif self.snapshot == 1:
            consumerId     = 1

        expectrowcnt   = int(paraDict["rowsPerTbl"] * paraDict["ctbNum"])
        topicList      = topicFromDb
        ifcheckdata    = 1
        ifManualCommit = 1
        keyList        = 'group.id:cgrp1,\
                        enable.auto.commit:true,\
                        auto.commit.interval.ms:1000,\
                        auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor")
        tmqCom.startTmqSimProcess(pollDelay=paraDict['pollDelay'],dbName=paraDict["dbName"],showMsg=paraDict['showMsg'], showRow=paraDict['showRow'],snapshot=paraDict['snapshot'])

        tmqCom.getStartConsumeNotifyFromTmqsim()
        tdLog.info("drop some ntables")
        # drop 1/4 ctbls from half offset
        paraDict["ctbStartIdx"] = paraDict["ctbStartIdx"] + int(paraDict["ctbNum"] * 1 / 2)
        paraDict["ctbNum"] = int(paraDict["ctbNum"] / 4)
        tmqCom.drop_ctable(tdSql, dbname=paraDict['dbName'], count=paraDict["ctbNum"], default_ctbname_prefix=paraDict["ctbPrefix"], ctbStartIdx=paraDict["ctbStartIdx"])

        tdLog.info("start to check consume result")
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        tdLog.info("act consume rows: %d, expect consume rows: %d"%(totalConsumeRows, expectrowcnt))

        # if not ((totalConsumeRows >= expectrowcnt * 3/4) and (totalConsumeRows < expectrowcnt)):
            # tdLog.exit("tmq consume rows error with snapshot = 0!")

        tdLog.info("wait subscriptions exit ....")
        tmqCom.waitSubscriptionExit(tdSql, topicFromDb)

        tdSql.query("drop topic %s"%topicFromDb)
        tdLog.info("success dorp topic: %s"%topicFromDb)
        tdLog.printNoPrefix("======== test case 1 end ...... ")



    # drop some ntbs and create some new ntbs
    def tmqCase2(self):
        tdLog.printNoPrefix("======== test case 2: ")
        paraDict = {'dbName':     'dbt',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    4,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'TIMESTAMP', 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1}],
                    'ctbPrefix':  'ntb',
                    'ctbStartIdx': 0,
                    'ctbNum':     1000,
                    'rowsPerTbl': 100,
                    'batchNum':   100,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'endTs': 0,
                    'pollDelay':  10,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   0}
        paraDict['snapshot'] = self.snapshot
        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        tmqCom.initConsumerTable()
        tdLog.info("start create database....")
        tdCom.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], vgroups=paraDict["vgroups"],replica=1)
        tdLog.info("start create normal tables....")
        tmqCom.create_ntable(tsql=tdSql, dbname=paraDict["dbName"], tbname_prefix=paraDict["ctbPrefix"], tbname_index_start_num = 1, column_elm_list=paraDict["colSchema"], colPrefix='c', tblNum=paraDict["ctbNum"])
        tdLog.info("start insert data into normal tables....")
        tmqCom.insert_rows_into_ntbl(tsql=tdSql, dbname=paraDict["dbName"], tbname_prefix=paraDict["ctbPrefix"], tbname_index_start_num = 1, column_ele_list=paraDict["colSchema"],startTs=paraDict["startTs"], tblNum=paraDict["ctbNum"], rows=paraDict["rowsPerTbl"])

        tdLog.info("create topics from database")
        topicFromDb = 'topic_dbt'
        tdSql.execute("create topic %s as database %s" %(topicFromDb, paraDict['dbName']))

        if self.snapshot == 0:
            consumerId     = 2
        elif self.snapshot == 1:
            consumerId     = 3

        expectrowcnt   = int(paraDict["rowsPerTbl"] * paraDict["ctbNum"] * 2)
        topicList      = topicFromDb
        ifcheckdata    = 1
        ifManualCommit = 1
        keyList        = 'group.id:cgrp1,\
                        enable.auto.commit:true,\
                        auto.commit.interval.ms:1000,\
                        auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor")
        tmqCom.startTmqSimProcess(pollDelay=paraDict['pollDelay'],dbName=paraDict["dbName"],showMsg=paraDict['showMsg'], showRow=paraDict['showRow'],snapshot=paraDict['snapshot'])

        tmqCom.getStartConsumeNotifyFromTmqsim()
        tdLog.info("drop some ntables")
        # drop 1/4 ctbls from half offset
        paraDict["ctbStartIdx"] = paraDict["ctbStartIdx"] + int(paraDict["ctbNum"] * 1 / 2)
        paraDict["ctbNum"] = int(paraDict["ctbNum"] / 4)
        tmqCom.drop_ctable(tdSql, dbname=paraDict['dbName'], count=paraDict["ctbNum"], default_ctbname_prefix=paraDict["ctbPrefix"], ctbStartIdx=paraDict["ctbStartIdx"])

        tdLog.info("start create some new normal tables....")
        paraDict["ctbPrefix"] = 'newCtb'
        paraDict["ctbNum"] = self.ctbNum
        tmqCom.create_ntable(tsql=tdSql, dbname=paraDict["dbName"], tbname_prefix=paraDict["ctbPrefix"], tbname_index_start_num = 1, column_elm_list=paraDict["colSchema"], colPrefix='c', tblNum=paraDict["ctbNum"])
        tdLog.info("start insert data into these new normal tables....")
        tmqCom.insert_rows_into_ntbl(tsql=tdSql, dbname=paraDict["dbName"], tbname_prefix=paraDict["ctbPrefix"], tbname_index_start_num = 1, column_ele_list=paraDict["colSchema"],startTs=paraDict["startTs"], tblNum=paraDict["ctbNum"], rows=paraDict["rowsPerTbl"])

        tdLog.info("start to check consume result")
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        tdLog.info("act consume rows: %d, expect consume rows: %d"%(totalConsumeRows, expectrowcnt))

        # if not ((totalConsumeRows >= expectrowcnt / 2 * (1 + 3/4)) and (totalConsumeRows < expectrowcnt)):
        #     tdLog.exit("tmq consume rows error with snapshot = 0!")

        tdLog.info("wait subscriptions exit ....")
        tmqCom.waitSubscriptionExit(tdSql, topicFromDb)

        tdSql.query("drop topic %s"%topicFromDb)
        tdLog.info("success dorp topic: %s"%topicFromDb)
        tdLog.printNoPrefix("======== test case 2 end ...... ")

    def test_tmq_drop_ntb_snapshot1(self):
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
        # tdLog.printNoPrefix("=============================================")
        # tdLog.printNoPrefix("======== snapshot is 0: only consume from wal")
        # self.snapshot = 0
        # self.tmqCase1()
        # self.tmqCase2()

        tdLog.printNoPrefix("====================================================================")
        tdLog.printNoPrefix("======== snapshot is 1: firstly consume from tsbs, and then from wal")
        self.snapshot = 1
        self.tmqCase1()
        self.tmqCase2()

        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()
