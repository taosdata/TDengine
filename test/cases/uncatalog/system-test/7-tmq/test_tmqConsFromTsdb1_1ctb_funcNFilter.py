import time
import threading
import math

from new_test_framework.utils import tdLog, tdSql, tdCom
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from tmqCommon import tmqCom

class TestCase:
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
        cls.vgroups    = 1
        cls.ctbNum     = 1
        cls.rowsPerTbl = 1000000

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
                    'rowsPerTbl': 10000,
                    'batchNum':   1000,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  3,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   1}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        tmqCom.initConsumerTable()
        tdCom.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], vgroups=paraDict["vgroups"],replica=1)
        tdLog.info("create stb")
        tmqCom.create_stable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"])
        tdLog.info("create ctb")
        tmqCom.create_ctable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"],ctbPrefix=paraDict['ctbPrefix'],
                             ctbNum=paraDict["ctbNum"],ctbStartIdx=paraDict['ctbStartIdx'])
        tdLog.info("insert data")
        tmqCom.insert_data_interlaceByMultiTbl(tsql=tdSql,dbName=paraDict["dbName"],ctbPrefix=paraDict["ctbPrefix"],
                                               ctbNum=paraDict["ctbNum"],rowsPerTbl=paraDict["rowsPerTbl"],batchNum=paraDict["batchNum"],
                                               startTs=paraDict["startTs"],ctbStartIdx=paraDict['ctbStartIdx'])

        tdLog.info("restart taosd to ensure that the data falls into the disk")
        # tdDnodes.stop(1)
        # tdDnodes.start(1)
        tdSql.query("flush database %s"%(paraDict['dbName']))
        return

    def tmqCase3(self):
        tdLog.printNoPrefix("======== test case 3: ")
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
                    'ctbNum':     1,
                    'rowsPerTbl': 10000,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  15,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   1}
        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        topicNameList = ['topic1']
        expectRowsList = []
        tmqCom.initConsumerTable()

        tdLog.info("create topics from stb with filter")
        # queryString = "select * from %s.%s"%(paraDict['dbName'], paraDict['stbName'])
        queryString = "select ts, acos(c1), ceil(pow(c1,3)) from %s.%s where (sin(c2) >= 0) and (c1 %% 4 == 0) and (ts >= %d) and (t4 like 'shanghai')"%(paraDict['dbName'], paraDict['stbName'], paraDict["startTs"]+9379)
        # sqlString = "create topic %s as stable %s" %(topicNameList[0], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topicNameList[0], queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)
        tdSql.query(queryString)
        expectRowsList.append(tdSql.getRows())
        totalRowsInserted = expectRowsList[0]

        # init consume info, and start tmq_sim, then check consume result
        tdLog.info("insert consume info to consume processor")
        consumerId   = 3
        expectrowcnt = math.ceil(totalRowsInserted/3)
        topicList    = topicNameList[0]
        ifcheckdata  = 1
        ifManualCommit = 1
        keyList      = 'group.id:cgrp1, enable.auto.commit:true, auto.commit.interval.ms:1000, auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        consumerId   = 4
        expectrowcnt = math.ceil(totalRowsInserted * 2/3)
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor 0")
        tmqCom.startTmqSimProcess(pollDelay=paraDict['pollDelay'],dbName=paraDict["dbName"],showMsg=paraDict['showMsg'], showRow=paraDict['showRow'],snapshot=paraDict['snapshot'])
        tdLog.info("wait the consume result")

        expectRows = 2
        resultList = tmqCom.selectConsumeResult(expectRows)
        actConsumeTotalRows = resultList[0] + resultList[1]

        if not (totalRowsInserted == actConsumeTotalRows):
            tdLog.info("sum of two consume rows: %d should be equal to total inserted rows: %d"%(actConsumeTotalRows, totalRowsInserted))
            tdLog.exit("%d tmq consume rows error!"%consumerId)

        time.sleep(10)
        for i in range(len(topicNameList)):
            tdSql.query("drop topic %s"%topicNameList[i])

        tdLog.printNoPrefix("======== test case 3 end ...... ")

    def tmqCase4(self):
        tdLog.printNoPrefix("======== test case 4: ")
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
                    'ctbNum':     1,
                    'rowsPerTbl': 10000,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  25,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   1}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        topicNameList = ['topic1']
        expectRowsList = []
        tmqCom.initConsumerTable()

        tdLog.info("create topics from stb with filter")
        queryString = "select ts, acos(c1), ceil(pow(c1,3)) from %s.%s where (sin(c2) >= 0) and (c1 %% 4 == 0) and (ts >= %d) and (t4 like 'shanghai')"%(paraDict['dbName'], paraDict['stbName'], paraDict["startTs"]+9379)
        # queryString = "select * from %s.%s"%(paraDict['dbName'], paraDict['stbName'])
        # sqlString = "create topic %s as stable %s" %(topicNameList[0], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topicNameList[0], queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)
        tdSql.query(queryString)
        expectRowsList.append(tdSql.getRows())
        totalRowsInserted = expectRowsList[0]

        # init consume info, and start tmq_sim, then check consume result
        tdLog.info("insert consume info to consume processor")
        consumerId   = 5
        expectrowcnt = math.ceil(totalRowsInserted)
        topicList    = topicNameList[0]
        ifcheckdata  = 1
        ifManualCommit = 1
        keyList      = 'group.id:cgrp1, enable.auto.commit:true, auto.commit.interval.ms:500, auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor 0")
        tmqCom.startTmqSimProcess(pollDelay=paraDict['pollDelay'],dbName=paraDict["dbName"],showMsg=paraDict['showMsg'], showRow=paraDict['showRow'],snapshot=paraDict['snapshot'])

        tdLog.info("wait commit notify")
        tmqCom.getStartCommitNotifyFromTmqsim()

        tdLog.info("pkill consume processor")
        tdCom.killProcessor("tmq_sim")

        # time.sleep(10)

        # reinit consume info, and start tmq_sim, then check consume result
        tmqCom.initConsumerTable()
        consumerId   = 6
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor 1")
        tmqCom.startTmqSimProcess(pollDelay=paraDict['pollDelay'],dbName=paraDict["dbName"],showMsg=paraDict['showMsg'], showRow=paraDict['showRow'],snapshot=paraDict['snapshot'])
        tdLog.info("wait the consume result")

        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)

        actConsumeTotalRows = resultList[0]

        if not (actConsumeTotalRows >= 0 and actConsumeTotalRows <= totalRowsInserted):
            tdLog.info("act consume rows: %d"%(actConsumeTotalRows))
            tdLog.info("and second consume rows should be between 0 and %d"%(totalRowsInserted))
            tdLog.exit("%d tmq consume rows error!"%consumerId)

        time.sleep(10)
        for i in range(len(topicNameList)):
            tdSql.query("drop topic %s"%topicNameList[i])

        tdLog.printNoPrefix("======== test case 4 end ...... ")

    def test_tmq_cons_from_tsdb1_1ctb_filter(self):
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
        self.prepareTestEnv()
        self.tmqCase3()
        self.tmqCase4()

        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

