
import taos
import sys
import time
import socket
import os
import threading
import math

from new_test_framework.utils import tdLog, tdSql, tdCom, tmqCom
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

class TestCase:

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def tmqCase1(self):
        tdLog.printNoPrefix("======== test case 1: ")
        paraDict = {'dbName':     'db1',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    4,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbNum':     1,
                    'rowsPerTbl': 10000,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  10,
                    'showMsg':    1,
                    'showRow':    1}

        topicNameList = ['topic1', 'topic2', 'topic3']
        expectRowsList = []
        tmqCom.initConsumerTable()
        tdCom.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], vgroups=4,replica=1)
        tdLog.info("create stb")
        tdCom.create_stable(tdSql, dbname=paraDict["dbName"],stbname=paraDict["stbName"], column_elm_list=paraDict['colSchema'], tag_elm_list=paraDict['tagSchema'])
        tdLog.info("create ctb")
        tdCom.create_ctable(tdSql, dbname=paraDict["dbName"],stbname=paraDict["stbName"],tag_elm_list=paraDict['tagSchema'],count=paraDict["ctbNum"], default_ctbname_prefix=paraDict['ctbPrefix'])
        tdLog.info("insert data")
        tmqCom.insert_data(tdSql,paraDict["dbName"],paraDict["ctbPrefix"],paraDict["ctbNum"],paraDict["rowsPerTbl"],paraDict["batchNum"],paraDict["startTs"])

        tdLog.info("create topics from stb with filter")
        queryString = "select ts, log(c1), ceil(pow(c1,3)) from %s.%s where c1 %% 7 == 0" %(paraDict['dbName'], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topicNameList[0], queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)
        tdSql.query(queryString)
        expectRowsList.append(tdSql.getRows())

        # init consume info, and start tmq_sim, then check consume result
        tdLog.info("insert consume info to consume processor")
        consumerId   = 0
        expectrowcnt = paraDict["rowsPerTbl"] * paraDict["ctbNum"]
        topicList    = topicNameList[0]
        ifcheckdata  = 1
        ifManualCommit = 1
        keyList      = 'group.id:cgrp1, enable.auto.commit:false, auto.commit.interval.ms:6000, auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor")
        tmqCom.startTmqSimProcess(paraDict['pollDelay'],paraDict["dbName"],paraDict['showMsg'], paraDict['showRow'])

        tdLog.info("wait the consume result")
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)

        if expectRowsList[0] != resultList[0]:
            tdLog.info("expect consume rows: %d, act consume rows: %d"%(expectRowsList[0], resultList[0]))
            tdLog.exit("0 tmq consume rows error!")

        tmqCom.checkFileContent(consumerId, queryString)

        # reinit consume info, and start tmq_sim, then check consume result
        tmqCom.initConsumerTable()

        queryString = "select ts, log(c1), cos(c1) from %s.%s where c1 > 3169" %(paraDict['dbName'], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topicNameList[1], queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)
        tdSql.query(queryString)
        expectRowsList.append(tdSql.getRows())

        consumerId   = 1
        topicList    = topicNameList[1]
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor")
        tmqCom.startTmqSimProcess(paraDict['pollDelay'],paraDict["dbName"],paraDict['showMsg'], paraDict['showRow'])

        tdLog.info("wait the consume result")
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)
        if expectRowsList[1] != resultList[0]:
            tdLog.info("expect consume rows: %d, act consume rows: %d"%(expectRowsList[1], resultList[0]))
            tdLog.exit("1 tmq consume rows error!")

        tmqCom.checkFileContent(consumerId, queryString)

        # reinit consume info, and start tmq_sim, then check consume result
        tmqCom.initConsumerTable()

        queryString = "select ts, log(c1), atan(c1) from %s.%s where ts >= %d" %(paraDict['dbName'], paraDict['stbName'], paraDict["startTs"]+6137)
        sqlString = "create topic %s as %s" %(topicNameList[2], queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)
        tdSql.query(queryString)
        expectRowsList.append(tdSql.getRows())

        consumerId   = 2
        topicList    = topicNameList[2]
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor")
        tmqCom.startTmqSimProcess(paraDict['pollDelay'],paraDict["dbName"],paraDict['showMsg'], paraDict['showRow'])

        tdLog.info("wait the consume result")
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)
        # if expectRowsList[2] != resultList[0]:
        #     tdLog.info("expect consume rows: %d, act consume rows: %d"%(expectRowsList[2], resultList[0]))
        #     tdLog.exit("2 tmq consume rows error!")

        # tmqCom.checkFileContent(consumerId, queryString)

        time.sleep(10)
        for i in range(len(topicNameList)):
            tdSql.query("drop topic %s"%topicNameList[i])

        tdLog.printNoPrefix("======== test case 1 end ...... ")

    def test_tmq_check_data(self):
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
        self.tmqCase1()

        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

