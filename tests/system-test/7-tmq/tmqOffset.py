
import sys
import re
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
        self.vgroups    = 2
        self.ctbNum     = 1
        self.rowsPerTbl = 10000
        self.tmqMaxTopicNum = 10
        self.tmqMaxGroups = 10
        
        self.TSDB_CODE_TMQ_VERSION_OUT_OF_RANGE = '0x4007'
        self.TSDB_CODE_TMQ_INVALID_VGID         = '0x4008'
        self.TSDB_CODE_TMQ_INVALID_TOPIC        = '0x4009'
        
        

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
                    # 'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1},{'type': 'TIMESTAMP', 'count':1}],
                    # 'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1}],

                    'colSchema':   [{'type': 'INT', 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1}],


                    'ctbPrefix':  'ctb',
                    'ctbStartIdx': 0,
                    'ctbNum':     1,
                    'rowsPerTbl': 10,
                    'batchNum':   100,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  10,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   1}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        # tmqCom.initConsumerTable()
        tdCom.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], vgroups=paraDict["vgroups"],replica=1,wal_retention_period=36000)
        # tdSql.execute("alter database %s wal_retention_period 360000" % (paraDict['dbName']))
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
    
    def tmqPollAllRows(self, consumer):
        totalRows = 0        
        
        res = consumer.poll(10)
        while (res):
            if not res:
                break
            err = res.error()
            if err is not None:
                raise err
            
            val = res.value()
            # print(len(val))            
            for block in val:
                # print(block.fetchall())
                # print(len(block.fetchall()))
                totalRows += len(block.fetchall())
            
            res = consumer.poll(10)
        
        tdLog.info("poll total rows: %d"%(totalRows))
        return totalRows
    
    def tmqPollRowsByOne(self, consumer):
        rows = 0
        res = consumer.poll(3)
        if not res:
            return rows
        err = res.error()
        if err is not None:
            raise err
        val = res.value()

        # print(len(val))
        
        for block in val:
            # print(block.fetchall())
            # print(len(block.fetchall()))
            rows += len(block.fetchall())
        
        return rows
    
    def tmqOffsetTest(self, consumer):        
        # get topic assignment
        tdLog.info("before poll get offset status:")
        assignments = consumer.assignment()
        for assignment in assignments:
            print(assignment)

        # poll
        # consumer.poll(5)
        rows = self.tmqPollRowsByOne(consumer)
        tdLog.info("poll rows: %d"%(rows))

        # get topic assignment
        tdLog.info("after first poll get offset status:")
        assignments = consumer.assignment()
        for assignment in assignments:
            print(assignment)
            

        rows = self.tmqPollRowsByOne(consumer)
        tdLog.info("poll rows: %d"%(rows))

        # get topic assignment
        tdLog.info("after second poll get offset status:")
        assignments = consumer.assignment()
        for assignment in assignments:
            print(assignment)

                    
        return

    def tmqSubscribe(self, inputDict):
        consumer_dict = {
            "group.id": inputDict['group_id'],
            "client.id": "client",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.commit.interval.ms": "1000",
            "enable.auto.commit": inputDict['auto_commit'],
            "auto.offset.reset": inputDict['offset_reset'],
            "experimental.snapshot.enable": "false",
            "msg.with.table.name": "false"
        }
        
        consumer = Consumer(consumer_dict)
        try:
            consumer.subscribe([inputDict['topic_name']])
        except Exception as e:
            tdLog.info("consumer.subscribe() fail ")
            tdLog.info("%s"%(e))        
        
        # rows = self.tmqPollAllRows(consumer)
        tdLog.info("create consumer success!")
        return consumer

    def tmqConsumer(self, **inputDict):        
        consumer = self.tmqSubscribe(inputDict)
        self.tmqPollAllRows(consumer)
        # consumer.unsubscribe()
        # consumer.close()
        return
    
    def asyncSubscribe(self, inputDict):
        pThread = threading.Thread(target=self.tmqConsumer, kwargs=inputDict)
        pThread.start()
        return pThread
    
    def seekErrorVgid(self, consumer, assignment):
        ####################### test1: error vgid
        assignmentNew = assignment
        # assignment.topic
        assignmentNew.partition = assignment.partition + self.vgroups + self.vgroups
        # assignment.offset
        # consumer.seek(assignment)
        
        errCodeStr = ''
        try:
            print("seek parameters:", assignmentNew)
            consumer.seek(assignmentNew)
        except Exception as e:
            tdLog.info("error: %s"%(e))
            
            rspString = str(e)
            start = "["
            end = "]"

            start_index = rspString.index(start) + len(start)
            end_index = rspString.index(end)

            errCodeStr = rspString[start_index:end_index]
            # print(errCodeStr)                
            tdLog.info("error code: %s"%(errCodeStr))
    
            if (self.TSDB_CODE_TMQ_INVALID_VGID != errCodeStr):
                tdLog.exit("tmq seek should return error code: %s"%(self.TSDB_CODE_TMQ_INVALID_VGID))
    
    def seekErrorTopic(self, consumer, assignment):
        assignmentNew = assignment
        assignmentNew.topic = 'errorToipcName'
        # assignment.partition
        # assignment.offset
        # consumer.seek(assignment)
        
        errCodeStr = ''
        try:
            print("seek parameters:", assignmentNew)
            consumer.seek(assignmentNew)
        except Exception as e:
            tdLog.info("error: %s"%(e))
            
            rspString = str(e)
            start = "["
            end = "]"

            start_index = rspString.index(start) + len(start)
            end_index = rspString.index(end)

            errCodeStr = rspString[start_index:end_index]
            # print(errCodeStr)                
            tdLog.info("error code: %s"%(errCodeStr))
    
            if (self.TSDB_CODE_TMQ_INVALID_TOPIC != errCodeStr):
                tdLog.exit("tmq seek should return error code: %s"%(self.TSDB_CODE_TMQ_INVALID_TOPIC))
    
    def seekErrorVersion(self, consumer, assignment):
        assignmentNew = assignment
        # print(assignment.topic, assignment.partition, assignment.offset)
        # assignment.topic
        # assignment.partition
        assignmentNew.offset = assignment.offset + self.rowsPerTbl * 100000
        # consumer.seek(assignment)
        
        errCodeStr = ''
        try:
            # print(assignmentNew.topic, assignmentNew.partition, assignmentNew.offset)
            print("seek parameters:", assignmentNew)
            consumer.seek(assignmentNew)
        except Exception as e:
            tdLog.info("error: %s"%(e))
            
            rspString = str(e)
            start = "["
            end = "]"

            start_index = rspString.index(start) + len(start)
            end_index = rspString.index(end)

            errCodeStr = rspString[start_index:end_index]
            # print(errCodeStr)                
            tdLog.info("error code: %s"%(errCodeStr))
    
        if (self.TSDB_CODE_TMQ_VERSION_OUT_OF_RANGE != errCodeStr):
            tdLog.exit("tmq seek should return error code: %s"%(self.TSDB_CODE_TMQ_VERSION_OUT_OF_RANGE))
        
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
                    'ctbNum':     1,
                    'rowsPerTbl': 100000000,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  3,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   1}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl
        
        # ntbName = 'ntb'
        # sqlString = "create table %s.%s (ts timestamp, c int)"%(paraDict['dbName'], ntbName)
        # tdLog.info("create ntb sql: %s"%sqlString)
        # tdSql.execute(sqlString)

        topicName = 'offset_tp'
        # queryString = "select * from %s.%s"%(paraDict['dbName'], ntbName)
        queryString = "select * from %s.%s"%(paraDict['dbName'], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topicName, queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)
        
        inputDict = {
            "topic_name": topicName,
            "group_id": "offsetGrp",
            "auto_commit": "true",
            "offset_reset": "earliest"
        }

        pThread = self.asyncSubscribe(inputDict)
        # pThread.join()
        
        consumer = self.tmqSubscribe(inputDict)
        # get topic assignment
        assignments = consumer.assignment()
        # print(type(assignments))
        for assignment in assignments:
            print(assignment)
        
        assignment = assignments[0]
        topic = assignment.topic
        partition = assignment.partition
        offset = assignment.offset
        
        tdLog.info("======== test error vgid =======")
        print("current assignment: ", assignment)
        self.seekErrorVgid(consumer, assignment)
        
        tdLog.info("======== test error topic =======")
        assignment.topic = topic
        assignment.partition = partition
        assignment.offset = offset
        print("current assignment: ", assignment)
        self.seekErrorTopic(consumer, assignment)
        
        tdLog.info("======== test error version =======")
        assignment.topic = topic
        assignment.partition = partition
        assignment.offset = offset
        print("current assignment: ", assignment)
        self.seekErrorVersion(consumer, assignment)
        
        pThread.join()
                    
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
