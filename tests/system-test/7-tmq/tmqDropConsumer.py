
import sys
import time
import threading
from taos.tmq import Consumer
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
sys.path.append("./7-tmq")
from tmqCommon import *

class TDTestCase:
    clientCfgDict = {'debugFlag': 135}
    updatecfgDict = {'debugFlag': 135, 'clientCfg':clientCfgDict}
    
    def __init__(self):
        self.vgroups    = 2
        self.ctbNum     = 10
        self.rowsPerTbl = 10
        self.tmqMaxTopicNum = 2
        self.tmqMaxGroups = 2

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    def getPath(self, tool="taosBenchmark"):
        if (platform.system().lower() == 'windows'):
            tool = tool + ".exe"
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        paths = []
        for root, dirs, files in os.walk(projPath):
            if ((tool) in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    paths.append(os.path.join(root, tool))
                    break
        if (len(paths) == 0):
            tdLog.exit("taosBenchmark not found!")
            return
        else:
            tdLog.info("taosBenchmark found in %s" % paths[0])
            return paths[0]

    def prepareTestEnv(self):
        tdLog.printNoPrefix("======== prepare test env include database, stable, ctables, and insert data: ")
        paraDict = {'dbName':     'dbt',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    2,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1},{'type': 'TIMESTAMP', 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbStartIdx': 0,
                    'ctbNum':     10,
                    'rowsPerTbl': 10,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  10,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   1}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        tmqCom.initConsumerTable()
        tdCom.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], vgroups=paraDict["vgroups"],replica=1)
        tdSql.execute("alter database %s wal_retention_period 360000" % (paraDict['dbName']))
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

    def tmqSubscribe(self, topicName, newGroupId, expectResult):
        # create new connector for new tdSql instance in my thread
        # newTdSql = tdCom.newTdSql()
        # topicName = inputDict['topic_name']
        # group_id = inputDict['group_id']
        
        consumer_dict = {
            "group.id": newGroupId,
            "client.id": "client",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.commit.interval.ms": "1000",
            "enable.auto.commit": "true",
            "auto.offset.reset": "earliest",
            "experimental.snapshot.enable": "false",
            "msg.with.table.name": "false"
        }
        
        ret = 'success'
        consumer = Consumer(consumer_dict)
        # print("======%s"%(inputDict['topic_name']))
        try:
            consumer.subscribe([topicName])
        except Exception as e:
            tdLog.info("consumer.subscribe() fail ")
            tdLog.info("%s"%(e))
            if (expectResult == "fail"):
                consumer.close()
                return 'success'
            else: 
                consumer.close()
                return 'fail'
    
        tdLog.info("consumer.subscribe() success ")
        if (expectResult == "success"):
            consumer.close()
            return 'success'
        else: 
            consumer.close()
            return 'fail'

    def tmqCase1(self):
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
                    'ctbPrefix':  'ctb',
                    'ctbStartIdx': 0,
                    'ctbNum':     10,
                    'rowsPerTbl': 100000000,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  3,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   1}

        topicNameList = ['dbtstb_0001','dbtstb_0002']
        tdLog.info("create topics from stb")
        queryString = "select * from %s.%s"%(paraDict['dbName'], paraDict['stbName'])
        for i in range(len(topicNameList)):
            sqlString = "create topic %s as %s" %(topicNameList[i], queryString)
            tdLog.info("create topic sql: %s"%sqlString)
            tdSql.execute(sqlString)

        # tdSql.query('show topics;')
        # topicNum = tdSql.queryRows
        # tdLog.info(" topic count: %d"%(topicNum))
        # if topicNum != len(topicNameList):
        #     tdLog.exit("show topics %d not equal expect num: %d"%(topicNum, len(topicNameList)))
        
        pThread = tmqCom.asyncInsertDataByInterlace(paraDict)
        
        # use taosBenchmark to subscribe  
        binPath = self.getPath()
        tmqCom.startProcess(binPath, "-f ./7-tmq/tmqDropConsumer.json")        
                
        expectTopicNum = len(topicNameList)
        consumerThreadNum = 2
        expectConsumerNUm = expectTopicNum * consumerThreadNum
        expectSubscribeNum = self.vgroups * expectTopicNum * consumerThreadNum
                
        tdSql.query('show topics;')
        topicNum = tdSql.queryRows
        tdLog.info(" get topic count: %d"%(topicNum))
        if topicNum != expectTopicNum:
            tdLog.exit("show topics %d not equal expect num: %d"%(topicNum, expectTopicNum))
        
        flag = 0
        while (1):        
            tdSql.query('show consumers;')
            consumerNUm = tdSql.queryRows
            tdLog.info(" get consumers count: %d"%(consumerNUm))
            if consumerNUm == expectConsumerNUm:
                flag = 1
                break
            else:
                time.sleep(1)

        if (0 == flag):
            tmqCom.g_end_insert_flag = 1
            tdLog.exit("show consumers %d not equal expect num: %d"%(topicNum, expectConsumerNUm))

        flag = 0
        for i in range(10):                  
            tdSql.query('show subscriptions;')
            subscribeNum = tdSql.queryRows
            tdLog.info(" get subscriptions count: %d"%(subscribeNum))
            if subscribeNum == expectSubscribeNum:
                flag = 1
                break
            else:
                time.sleep(1)
                
        if (0 == flag):
            tmqCom.g_end_insert_flag = 1
            tdLog.exit("show subscriptions %d not equal expect num: %d"%(subscribeNum, expectSubscribeNum))
                
        # get all consumer group id
        tdSql.query('show consumers;')
        consumerNUm = tdSql.queryRows 
        groupIdList = []
        for i in range(consumerNUm): 
            groupId = tdSql.getData(i,1)
            existFlag = 0
            for j in range(len(groupIdList)): 
                if (groupId == groupIdList[j]):
                    existFlag = 1
                    break
            if (0 == existFlag):
                groupIdList.append(groupId)
        
        # kill taosBenchmark        
        tmqCom.killProcesser("taosBenchmark")
        tdLog.info("kill taosBenchmak end")
        
        # wait the status to "lost"
        while (1):
            exitFlag = 1
            tdSql.query('show consumers;')
            consumerNUm = tdSql.queryRows 
            for i in range(consumerNUm): 
                status = tdSql.getData(i,3)
                if (status != "lost"):
                    exitFlag = 0
                    time.sleep(2)
                    break
            if (1 == exitFlag):
                break
        
        tdLog.info("all consumers status into 'lost'")
        # drop consumer groups
        tdLog.info("drop all consumers")
        for i in range(len(groupIdList)): 
            for j in range(len(topicNameList)): 
                sqlCmd = f"drop consumer group `%s` on %s"%(groupIdList[i], topicNameList[j])
                tdLog.info("drop consumer cmd: %s"%(sqlCmd))
                tdSql.execute(sqlCmd)
                
        tmqCom.g_end_insert_flag = 1
        tdLog.debug("notify sub-thread to stop insert data")
        pThread.join()
        
        tdSql.query('show consumers;')
        consumerNUm = tdSql.queryRows

        tdSql.query('show subscriptions;')
        subscribeNum = tdSql.queryRows
        
        if (0 != consumerNUm or 0 != subscribeNum):
            tdLog.exit("drop consumer fail! consumerNUm %d, subscribeNum: %d"%(consumerNUm, subscribeNum))

        tdLog.info("drop consuer success, there is no consumers and subscribes")
        tdLog.printNoPrefix("======== test case 1 end ...... ")

    def run(self):
        self.prepareTestEnv()
        self.tmqCase1()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
