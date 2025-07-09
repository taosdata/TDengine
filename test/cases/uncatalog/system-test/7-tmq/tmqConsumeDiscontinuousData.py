
import sys
import time
import datetime
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
    updatecfgDict = {'debugFlag': 135}    
    
    def __init__(self):
        self.vgroups    = 1
        self.ctbNum     = 10
        self.rowsPerTbl = 100
        self.tmqMaxTopicNum = 1
        self.tmqMaxGroups = 1
        self.walRetentionPeriod = 3
        self.actConsumeTotalRows = 0
        self.retryPoll = 0
        self.lock = threading.Lock()

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
                    'vgroups':    1,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1},{'type': 'TIMESTAMP', 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbStartIdx': 0,
                    'ctbNum':     10,
                    'rowsPerTbl': 10,
                    'batchNum':   1,
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
        tdSql.execute("alter database %s wal_retention_period %d" % (paraDict['dbName'], self.walRetentionPeriod))
        tdLog.info("create stb")
        tmqCom.create_stable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"])
        tdLog.info("create ctb")
        tmqCom.create_ctable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"],ctbPrefix=paraDict['ctbPrefix'],
                             ctbNum=paraDict["ctbNum"],ctbStartIdx=paraDict['ctbStartIdx'])
        # tdLog.info("insert data")
        # tmqCom.insert_data_interlaceByMultiTbl(tsql=tdSql,dbName=paraDict["dbName"],ctbPrefix=paraDict["ctbPrefix"],
        #                                        ctbNum=paraDict["ctbNum"],rowsPerTbl=paraDict["rowsPerTbl"],batchNum=paraDict["batchNum"],
        #                                        startTs=paraDict["startTs"],ctbStartIdx=paraDict['ctbStartIdx'])

        # tdLog.info("restart taosd to ensure that the data falls into the disk")
        # tdDnodes.stop(1)
        # tdDnodes.start(1)
        # tdSql.query("flush database %s"%(paraDict['dbName']))
        return

    def tmqSubscribe(self, **inputDict):
        consumer_dict = {
            "group.id": inputDict['group_id'],
            "client.id": "client",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.commit.interval.ms": "100",
            "enable.auto.commit": "true",
            "auto.offset.reset": "earliest",
            "experimental.snapshot.enable": "false",
            "msg.with.table.name": "false"
        }
        
        consumer = Consumer(consumer_dict)
        consumer.subscribe([inputDict['topic_name']])
        onceFlag = 0
        try:
            while True:
                if (1 == self.retryPoll):
                    time.sleep(2)
                    continue
                res = consumer.poll(inputDict['pollDelay'])        
                if not res:
                    break
                err = res.error()
                if err is not None:
                    raise err
                
                val = res.value()
                for block in val:
                    # print(block.fetchall())
                    data = block.fetchall()
                    for row in data:
                        # print("===================================")
                        # print(row)
                        self.actConsumeTotalRows += 1
                    if (0 == onceFlag):
                        onceFlag = 1
                        with self.lock:
                            self.retryPoll = 1
                            currentTime = datetime.now()      
                            print("%s temp stop consume"%(str(currentTime)))
                    
                currentTime = datetime.now()        
                print("%s already consume rows: %d, and sleep for a while"%(str(currentTime), self.actConsumeTotalRows))
                # time.sleep(self.walRetentionPeriod * 3)
        finally:
            consumer.unsubscribe()
            consumer.close()

        return

    def asyncSubscribe(self, inputDict):
        pThread = threading.Thread(target=self.tmqSubscribe, kwargs=inputDict)
        pThread.start()
        return pThread
    
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
                    'rowsPerTbl': 100,
                    'batchNum':   1,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  3,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   1}

        # create topic
        topicNameList = ['dbtstb_0001']
        tdLog.info("create topics from stb")
        queryString = "select * from %s.%s"%(paraDict['dbName'], paraDict['stbName'])
        for i in range(len(topicNameList)):
            sqlString = "create topic %s as %s" %(topicNameList[i], queryString)
            tdLog.info("create topic sql: %s"%sqlString)
            tdSql.execute(sqlString)



        # start consumer
        inputDict = {'group_id':   "grpid_0001",
                     'topic_name': topicNameList[0],
                     'pollDelay':  10
        }
        
        pThread2 = self.asyncSubscribe(inputDict)
        
        pThread1 = tmqCom.asyncInsertDataByInterlace(paraDict)
        pThread1.join()
        tdLog.info("firstly call to flash database")
        tdSql.query("flush database %s"%(paraDict['dbName']))
        time.sleep(self.walRetentionPeriod + 1)
        tdLog.info("secondely call to flash database")
        tdSql.query("flush database %s"%(paraDict['dbName']))

        # wait the consumer to complete one poll
        while (0 == self.retryPoll):
            time.sleep(1)
            continue

        # write data again when consumer stopped to make sure some data aren't consumed
        pInsertDataAgainThread = tmqCom.asyncInsertDataByInterlace(paraDict)
        pInsertDataAgainThread.join()
        tdLog.info("firstly call to flash database when writing data second time")
        tdSql.query("flush database %s"%(paraDict['dbName']))
        time.sleep(self.walRetentionPeriod + 1)
        tdLog.info("secondely call to flash database when writing data second time")
        tdSql.query("flush database %s"%(paraDict['dbName']))

        with self.lock:
            self.retryPoll = 0            
            currentTime = datetime.now()      
            print("%s restart consume"%(str(currentTime)))
        
        paraDict["startTs"] = 1640966400000 + paraDict["ctbNum"] * paraDict["rowsPerTbl"]
        pThread3 = tmqCom.asyncInsertDataByInterlace(paraDict)

        tdLog.debug("wait sub-thread to end insert data")        
        pThread3.join()

        totalInsertRows = paraDict["ctbNum"] * paraDict["rowsPerTbl"] * 3
        tdLog.debug("wait sub-thread to end consume data")
        pThread2.join()

        tdLog.info("act consume total rows: %d, act insert total rows: %d"%(self.actConsumeTotalRows, totalInsertRows))
        
        if (self.actConsumeTotalRows >= totalInsertRows):
            tdLog.exit("act consume rows: %d not equal expect: %d"%(self.actConsumeTotalRows, totalInsertRows))
        
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
