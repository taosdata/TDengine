import sys
import re
import time
import threading
from taos.tmq import *
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
sys.path.append("./7-tmq")
from tmqCommon import *


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)
        
        self.db_name = "tmq_db"
        self.topic_name = "tmq_topic"
        self.stable_name = "stb"
        self.rows_per_table = 1000
        self.ctb_num = 100

    def prepareData(self, precisionUnit="ms"):
        tdLog.printNoPrefix("======== prepare test env include database, stable, ctables, and insert data: ")
        startTS = 1672502400000
        if precisionUnit == "us":
            startTS = 1672502400000000
        elif precisionUnit == "ns":
            startTS = 1672502400000000000

        paraDict = {
                        'dbName':     self.db_name,
                        'dropFlag':   1,
                        'event':      '',
                        'vgroups':    4,
                        'stbName':    self.stable_name,
                        'colPrefix':  'c',
                        'tagPrefix':  't',
                        'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1},{'type': 'TIMESTAMP', 'count':1}],
                        'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1}],
                        'ctbPrefix':  'ctb',
                        'ctbStartIdx': 0,
                        'ctbNum':     self.ctb_num,
                        'rowsPerTbl': self.rows_per_table,
                        'batchNum':   100,
                        'startTs':    startTS,  # 2023-01-01 00:00:00.000
                        'pollDelay':  3,
                        'showMsg':    1,
                        'showRow':    1,
                        'snapshot':   0
                    }

        # init the consumer database
        tmqCom.initConsumerTable()

        # create testing database、stable、ctables
        tdCom.create_database(tdSql, paraDict["dbName"], paraDict["dropFlag"], vgroups=paraDict["vgroups"], replica=self.replicaVar, precision=precisionUnit)
        tdLog.info("create database %s successfully" % paraDict["dbName"])
        tmqCom.create_stable(tdSql, dbName=paraDict["dbName"], stbName=paraDict["stbName"])
        tdLog.info("create stable %s successfully" % paraDict["stbName"])
        tmqCom.create_ctable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"], ctbPrefix=paraDict['ctbPrefix'],
                             ctbNum=paraDict["ctbNum"], ctbStartIdx=paraDict['ctbStartIdx'])
        tdLog.info("create child tables successfully")

        # insert data into tables and wait the async thread exit
        tdLog.info("insert data into tables")
        pThread = tmqCom.asyncInsertDataByInterlace(paraDict)
        pThread.join()

    def run(self):
        """Check tmq feature for different data precision unit like "ms、us、ns"
        """
        precision_unit = ["ms", "us", "ns"]
        for unit in precision_unit:
            tdLog.info(f"start to test precision unit {unit}")
            self.prepareData(precisionUnit=unit)
            # drop database if exists
            tdSql.execute(f"drop database if exists {self.db_name}")
            self.prepareData(unit)

            # create topic
            tdLog.info("create topic from %s" % self.stable_name)
            queryString = "select ts, c1, c2 from %s.%s  where t4 == 'beijing' or t4 == 'changsha' "%(self.db_name, self.stable_name)
            sqlString = "create topic %s as %s" %(self.topic_name, queryString)
            tdLog.info("create topic sql: %s"%sqlString)
            tdSql.execute(sqlString)

            # save consumer info
            consumerId     = 0
            expectrowcnt   = self.rows_per_table * self.ctb_num
            topicList      = self.topic_name
            ifcheckdata    = 0
            ifManualCommit = 0
            keyList        = 'group.id:cgrp1,\
                            enable.auto.commit:false,\
                            auto.commit.interval.ms:6000,\
                            auto.offset.reset:earliest'
            tmqCom.insertConsumerInfo(consumerId, expectrowcnt, topicList, keyList, ifcheckdata, ifManualCommit)

            # start consume processor
            paraDict = {
            'pollDelay':  15,
            'showMsg':    1,
            'showRow':    1,
            'snapshot':   0
            }
            tdLog.info("start consume processor")
            tmqCom.startTmqSimProcess(pollDelay=paraDict['pollDelay'], dbName=self.db_name, showMsg=paraDict['showMsg'], showRow=paraDict['showRow'], snapshot=paraDict['snapshot'])

            tdLog.info("start to check consume result")
            expectRows = 1
            resultList = tmqCom.selectConsumeResult(expectRows)
            totalConsumeRows = 0
            for i in range(expectRows):
                totalConsumeRows += resultList[i]

            tdSql.query(queryString)
            totalRowsFromQuery = tdSql.getRows()
            tdLog.info("act consume rows: %d, act query rows: %d "%(totalConsumeRows, totalRowsFromQuery))

            if totalConsumeRows < totalRowsFromQuery:
                tdLog.exit("tmq consume rows error!")

            tmqCom.waitSubscriptionExit(tdSql, self.topic_name)
            tdSql.query("drop topic %s" % self.topic_name)
            tdSql.execute("drop database %s" % self.db_name)
        
    def stop(self):
        tdSql.execute(f"drop database if exists {self.db_name}")
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
