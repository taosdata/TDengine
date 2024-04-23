
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
    updatecfgDict = {'debugFlag': 135}    

    def __init__(self):
        self.vgroups    = 1
        self.ctbNum     = 10
        self.rowsPerTbl = 10
        self.tmqMaxTopicNum = 20
        self.tmqMaxGroups = 100

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    def modifyMaxTopics(self, tmqMaxTopicNum):
        # single dnode
        cfgDir = tdDnodes.dnodes[0].cfgDir
        
        # cluster dnodes
        # tdDnodes[1].dataDir
        # tdDnodes[1].logDir
        # tdDnodes[1].cfgDir
        
        cfgFile = f"%s/taos.cfg"%(cfgDir)
        shellCmd = 'echo tmqMaxTopicNum    %d >> %s'%(tmqMaxTopicNum, cfgFile)
        tdLog.info(" shell cmd: %s"%(shellCmd))
        os.system(shellCmd)       
        tdDnodes.stoptaosd(1)
        tdDnodes.starttaosd(1)
        time.sleep(5)

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

    def tmqSubscribe(self, **inputDict):
        # create new connector for new tdSql instance in my thread
        # newTdSql = tdCom.newTdSql()
        # topicName = inputDict['topic_name']
        # group_id = inputDict['group_id']
        
        consumer_dict = {
            "group.id": inputDict['group_id_prefix'],
            "client.id": "client",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.commit.interval.ms": "1000",
            "enable.auto.commit": "true",
            "auto.offset.reset": "earliest",
            "experimental.snapshot.enable": "false",
            "msg.with.table.name": "false"
        }        
        
        for j in range(self.tmqMaxGroups):
            consumer_dict["group.id"] =  f"%s_%d"%(inputDict['group_id_prefix'], j)
            consumer_dict["client.id"] =  f"%s_%d"%(inputDict['group_id_prefix'], j)
            print("======grpid: %s"%(consumer_dict["group.id"]))
            consumer = Consumer(consumer_dict)
            # print("======%s"%(inputDict['topic_name']))
            consumer.subscribe([inputDict['topic_name']])
            # res = consumer.poll(inputDict['pollDelay'])
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
                    'rowsPerTbl': 10,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  3,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   1}
        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        topicNamePrefix = 'topicname_'
        tdLog.info("create topics from stb")
        queryString = "select * from %s.%s"%(paraDict['dbName'], paraDict['stbName'])
        for i in range(self.tmqMaxTopicNum):
            sqlString = "create topic %s%d as %s" %(topicNamePrefix, i, queryString)
            tdLog.info("create topic sql: %s"%sqlString)
            tdSql.execute(sqlString)

        sqlString = "create topic %s%s as %s" %(topicNamePrefix, 'xyz', queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.error(sqlString)

        tdSql.query('show topics;')
        topicNum = tdSql.queryRows
        tdLog.info(" topic count: %d"%(topicNum))
        if topicNum != self.tmqMaxTopicNum:
            tdLog.exit("show topics %d not equal expect num: %d"%(topicNum, self.tmqMaxTopicNum))
        
        # self.updatecfgDict = {'tmqMaxTopicNum': 22}        
        # tdDnodes.stoptaosd(1)
        # tdDnodes.deploy(1, self.updatecfgDict)
        # tdDnodes.starttaosd(1)
        # time.sleep(5)
        
        newTmqMaxTopicNum = 22
        self.modifyMaxTopics(newTmqMaxTopicNum)

        sqlString = "create topic %s%s as %s" %(topicNamePrefix, 'x', queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)  

        sqlString = "create topic %s%s as %s" %(topicNamePrefix, 'y', queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)  

        sqlString = "create topic %s%s as %s" %(topicNamePrefix, 'xyz', queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.error(sqlString)

        tdSql.query('show topics;')
        topicNum = tdSql.queryRows
        tdLog.info(" topic count: %d"%(topicNum))
        if topicNum != newTmqMaxTopicNum:
            tdLog.exit("show topics %d not equal expect num: %d"%(topicNum, newTmqMaxTopicNum))
        
        newTmqMaxTopicNum = 18
        self.modifyMaxTopics(newTmqMaxTopicNum)
        
        i = 0
        sqlString = "drop topic %s%d" %(topicNamePrefix, i)
        tdLog.info("drop topic sql: %s"%sqlString)
        tdSql.execute(sqlString)

        i = 1
        sqlString = "drop topic %s%d" %(topicNamePrefix, i)
        tdLog.info("drop topic sql: %s"%sqlString)
        tdSql.execute(sqlString)

        sqlString = "drop topic %s%s" %(topicNamePrefix, "x")
        tdLog.info("drop topic sql: %s"%sqlString)
        tdSql.execute(sqlString)

        sqlString = "drop topic %s%s" %(topicNamePrefix, "y")
        tdLog.info("drop topic sql: %s"%sqlString)
        tdSql.execute(sqlString)

        sqlString = "create topic %s%s as %s" %(topicNamePrefix, 'xyz', queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.error(sqlString)
        
        tdLog.info("drop database when there are topic") 
        sqlString = "drop database %s" %(paraDict['dbName'])
        tdLog.info("drop database sql: %s"%sqlString)
        tdSql.error(sqlString)        
        
        tdLog.info("drop all topic for re-create") 
        tdSql.query('show topics;')
        topicNum = tdSql.queryRows
        tdLog.info(" topic count: %d"%(topicNum))
        for i in range(topicNum):
            sqlString = "drop topic %s" %(tdSql.getData(i, 0))
            tdLog.info("drop topic sql: %s"%sqlString)
            tdSql.execute(sqlString)
        
        time.sleep(1)
        
        tdLog.info("re-create topics")
        topicNamePrefix = 'newTopic_'
        queryString = "select * from %s.%s"%(paraDict['dbName'], paraDict['stbName'])
        for i in range(topicNum):
            sqlString = "create topic %s%d as %s" %(topicNamePrefix, i, queryString)
            tdLog.info("create topic sql: %s"%sqlString)
            tdSql.execute(sqlString)        
        
        #=================================================#
        tdLog.info("drop all topic for testcase2") 
        tdSql.query('show topics;')
        topicNum = tdSql.queryRows
        tdLog.info(" topic count: %d"%(topicNum))
        for i in range(topicNum):
            sqlString = "drop topic %s" %(tdSql.getData(i, 0))
            tdLog.info("drop topic sql: %s"%sqlString)
            tdSql.execute(sqlString)
    
        tdLog.printNoPrefix("======== test case 1 end ...... ")


    def tmqCase2(self):
        tdLog.printNoPrefix("======== test case 2: test topic name len")
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
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  3,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   1}
        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl
        
        queryString = "select * from %s.%s"%(paraDict['dbName'], paraDict['stbName'])
        totalTopicNum = 0
     
        topicName = 'a'
        sqlString = "create topic %s as %s" %(topicName, queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.query(sqlString)
        totalTopicNum += 1
        
        topicName = '3'
        sqlString = "create topic %s as %s" %(topicName, queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.error(sqlString)
        totalTopicNum += 0

        topicName = '_1'
        sqlString = "create topic %s as %s" %(topicName, queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.query(sqlString)
        totalTopicNum += 1
        
        topicName = 'a\\'
        sqlString = "create topic %s as %s" %(topicName, queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.error(sqlString)
        totalTopicNum += 0
   
        topicName = 'a\*\&\^'
        sqlString = "create topic %s as %s" %(topicName, queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.error(sqlString)
        totalTopicNum += 0

       
        str191char = 'a'
        for i in range(190):            
            str191char = ('%s%d'%(str191char, 1))        
        
        topicName = str191char + 'a'
        
        if (192 != len(topicName)):
            tdLog.exit("topicName len error")
        
        sqlString = "create topic %s as %s" %(topicName, queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.query(sqlString)
        totalTopicNum += 1

        topicName = str191char + '12'
        sqlString = "create topic %s as %s" %(topicName, queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.error(sqlString)
        totalTopicNum += 0

        # topicName = str192char + '12'
        # sqlString = "create topic %s as %s" %(topicName, queryString)
        # tdLog.info("create topic sql: %s"%sqlString)
        # tdSql.error(sqlString)
        # totalTopicNum += 0

        # check topic count
        tdSql.query('show topics;')
        topicNum = tdSql.queryRows
        tdLog.info(" topic count: %d"%(topicNum))
        if topicNum != totalTopicNum:
            tdLog.exit("show topics %d not equal expect num: %d"%(topicNum, totalTopicNum))
        
    
    tdLog.printNoPrefix("======== test case 2 end ...... ")
    
    def run(self):
        self.prepareTestEnv()
        self.tmqCase1()
        self.tmqCase2()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
