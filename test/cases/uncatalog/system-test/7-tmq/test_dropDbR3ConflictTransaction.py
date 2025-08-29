from distutils.log import error
import taos
import sys
import time
import socket
import os
import threading
import subprocess
import platform

from new_test_framework.utils import tdLog, tdSql, tdCom, tmqCom
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

class TestCase:
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
        cls.snapshot   = 0
        cls.replica    = 3
        cls.vgroups    = 3
        cls.ctbNum     = 2
        cls.rowsPerTbl = 2

    def prepareTestEnv(self):
        tdLog.printNoPrefix("======== prepare test env include database, stable, ctables, and insert data: ")
        paraDict = {'dbName':     'dbt',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    4,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbStartIdx': 0,
                    'ctbNum':     2,
                    'rowsPerTbl': 1000,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  3,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   0}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        tmqCom.initConsumerTable()
        tdCom.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], vgroups=paraDict["vgroups"],replica=self.replica)
        tdLog.info("create stb")
        tmqCom.create_stable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"])
        tdLog.info("create ctb")
        tmqCom.create_ctable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"],ctbPrefix=paraDict['ctbPrefix'],
                             ctbNum=paraDict["ctbNum"],ctbStartIdx=paraDict['ctbStartIdx'])
        tdLog.info("insert data")
        tmqCom.insert_data_interlaceByMultiTbl(tsql=tdSql,dbName=paraDict["dbName"],ctbPrefix=paraDict["ctbPrefix"],
                                               ctbNum=paraDict["ctbNum"],rowsPerTbl=paraDict["rowsPerTbl"],batchNum=paraDict["batchNum"],
                                               startTs=paraDict["startTs"],ctbStartIdx=paraDict['ctbStartIdx'])
        # tmqCom.insert_data_with_autoCreateTbl(tsql=tdSql,dbName=paraDict["dbName"],stbName=paraDict["stbName"],ctbPrefix="ctbx",
        #                                       ctbNum=paraDict["ctbNum"],rowsPerTbl=paraDict["rowsPerTbl"],batchNum=paraDict["batchNum"],
        #                                       startTs=paraDict["startTs"],ctbStartIdx=paraDict['ctbStartIdx'])
        # tmqCom.asyncInsertDataByInterlace(paraDict)
        tmqCom.create_ntable(tdSql, dbname=paraDict["dbName"], tbname_prefix="ntb", tbname_index_start_num = 1, column_elm_list=paraDict["colSchema"], colPrefix='c', tblNum=1)
        tmqCom.insert_rows_into_ntbl(tdSql, dbname=paraDict["dbName"], tbname_prefix="ntb", tbname_index_start_num = 1, column_ele_list=paraDict["colSchema"], startTs=paraDict["startTs"], tblNum=1, rows=2)        # tdLog.info("restart taosd to ensure that the data falls into the disk")
        tdSql.query("drop database %s"%paraDict["dbName"])
        return

    def tmqCase1(self):
        tdLog.printNoPrefix("======== test case 1: ")

        # create and start thread
        paraDict = {'dbName':     'dbt',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    4,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1},{'type': 'TIMESTAMP', 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbStartIdx': 0,
                    'ctbNum':     100,
                    'rowsPerTbl': 1000,
                    'batchNum':   100,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  3,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   1}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        tdLog.info("create topics from stb1")
        topicFromStb1 = 'topic_stb1'
        queryString = "select ts, c1, c2 from %s.%s  where t4 == 'beijing' or t4 == 'changsha' "%(paraDict['dbName'], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topicFromStb1, queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)

        consumerId     = 0
        expectrowcnt   = paraDict["rowsPerTbl"] * paraDict["ctbNum"]
        topicList      = topicFromStb1
        ifcheckdata    = 0
        ifManualCommit = 0
        keyList        = 'group.id:cgrp1,\
                        enable.auto.commit:false,\
                        auto.commit.interval.ms:6000,\
                        auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor")
        pollDelay = 100
        showMsg   = 1
        showRow   = 1
        tmqCom.startTmqSimProcess(pollDelay=paraDict['pollDelay'],dbName=paraDict["dbName"],showMsg=paraDict['showMsg'], showRow=paraDict['showRow'],snapshot=paraDict['snapshot'])

        tdLog.info("start to check consume result")
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        tdSql.query(queryString)
        totalRowsInserted = tdSql.getRows()

        tdLog.info("act consume rows: %d, act insert rows: %d, expect consume rows: %d, "%(totalConsumeRows, totalRowsInserted, expectrowcnt))

        if totalConsumeRows != expectrowcnt:
            tdLog.exit("tmq consume rows error!")

        # tmqCom.checkFileContent(consumerId, queryString)

        tmqCom.waitSubscriptionExit(tdSql, topicFromStb1)
        tdSql.query("drop topic %s"%topicFromStb1)

        tdLog.printNoPrefix("======== test case 1 end ...... ")

    def test_dropdb(self):
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
        self.prepareTestEnv()
        # self.tmqCase1()

        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()
