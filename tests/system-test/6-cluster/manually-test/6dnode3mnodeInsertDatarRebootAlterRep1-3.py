import taos
import sys
import time
import os

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import TDDnodes
from util.dnodes import TDDnode
from util.cluster import *
sys.path.append("./6-cluster")
from clusterCommonCreate import *
from clusterCommonCheck import clusterComCheck

import time
import socket
import subprocess
from multiprocessing import Process
import threading
import time
import inspect
import ctypes

class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to excute {__file__}")
        self.TDDnodes = None
        tdSql.init(conn.cursor())
        self.host = socket.gethostname()


    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files or "taosd.exe" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

    def _async_raise(self, tid, exctype):
        """raises the exception, performs cleanup if needed"""
        if not inspect.isclass(exctype):
            exctype = type(exctype)
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
        if res == 0:
            raise ValueError("invalid thread id")
        elif res != 1:
            # """if it returns a number greater than one, you're in trouble,
            # and you should call it again with exc=NULL to revert the effect"""
            ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
            raise SystemError("PyThreadState_SetAsyncExc failed")

    def stopThread(self,thread):
        self._async_raise(thread.ident, SystemExit)


    def insertData(self,countstart,countstop):
        # fisrt add data : db\stable\childtable\general table

        for couti in range(countstart,countstop):
            tdLog.debug("drop database if exists db%d" %couti)
            tdSql.execute("drop database if exists db%d" %couti)
            print("create database if not exists db%d replica 1 duration 300" %couti)
            tdSql.execute("create database if not exists db%d replica 1 duration 300" %couti)
            tdSql.execute("use db%d" %couti)
            tdSql.execute(
            '''create table stb1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            tags (t1 int)
            '''
            )
            tdSql.execute(
                '''
                create table t1
                (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
                '''
            )
            for i in range(4):
                tdSql.execute(f'create table ct{i+1} using stb1 tags ( {i+1} )')


    def fiveDnodeThreeMnode(self,dnodeNumbers,mnodeNums,restartNumbers,stopRole):
        tdLog.printNoPrefix("======== test case 1: ")
        paraDict = {'dbName':     'db0_0',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    4,
                    'replica':    1,
                    'stbName':    'stb',
                    'stbNumbers': 2,
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbNum':     1000,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    "rowsPerTbl": 100,
                    "batchNum": 5000
                    }

        dnodeNumbers = int(dnodeNumbers)
        mnodeNums = int(mnodeNums)
        vnodeNumbers = int(dnodeNumbers-mnodeNums)
        allctbNumbers = (paraDict['stbNumbers']*paraDict["ctbNum"])
        rowsPerStb = paraDict["ctbNum"]*paraDict["rowsPerTbl"]
        rowsall = rowsPerStb*paraDict['stbNumbers']
        dbNumbers = 1
        replica3 = 3
        tdLog.info("first check dnode and mnode")
        tdSql.query("select * from information_schema.ins_dnodes;")
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(4,1,'%s:6430'%self.host)
        clusterComCheck.checkDnodes(dnodeNumbers)
        
        #check mnode status
        tdLog.info("check mnode status")
        clusterComCheck.checkMnodeStatus(mnodeNums)

        # add some error operations and
        tdLog.info("Confirm the status of the dnode again")
        tdSql.error("create mnode on dnode 2")
        tdSql.query("select * from information_schema.ins_dnodes;")
        print(tdSql.queryResult)
        clusterComCheck.checkDnodes(dnodeNumbers)

        # create database and stable
        clusterComCreate.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], paraDict["vgroups"],paraDict['replica'])
        tdLog.info("Take turns stopping Mnodes ")

        tdDnodes=cluster.dnodes
        stopcount =0
        threads=[]

        # create stable:stb_0
        stableName= paraDict['stbName']
        newTdSql=tdCom.newTdSql()
        clusterComCreate.create_stables(newTdSql, paraDict["dbName"],stableName,paraDict['stbNumbers'])
        #create child table:ctb_0
        for i in range(paraDict['stbNumbers']):
            stableName= '%s_%d'%(paraDict['stbName'],i)
            newTdSql=tdCom.newTdSql()
            clusterComCreate.create_ctable(newTdSql, paraDict["dbName"],stableName,stableName, paraDict['ctbNum'])
        #insert date
        for i in range(paraDict['stbNumbers']):
            stableName= '%s_%d'%(paraDict['stbName'],i)
            newTdSql=tdCom.newTdSql()
            threads.append(threading.Thread(target=clusterComCreate.insert_data, args=(newTdSql, paraDict["dbName"],stableName,paraDict["ctbNum"],paraDict["rowsPerTbl"],paraDict["batchNum"],paraDict["startTs"])))
        for tr in threads:
            tr.start()
        TdSqlEx=tdCom.newTdSql()
        tdLog.info("alter database db0_0 replica 3")
        TdSqlEx.execute('alter database db0_0 replica 3')
        while stopcount < restartNumbers:
            tdLog.info(" restart loop: %d"%stopcount )
            if stopRole == "mnode":
                for i in range(mnodeNums):
                    tdDnodes[i].stoptaosd()
                    # sleep(10)
                    tdDnodes[i].starttaosd()
                    # sleep(10)
            elif stopRole == "vnode":
                for i in range(vnodeNumbers):
                    tdDnodes[i+mnodeNums].stoptaosd()
                    # sleep(10)
                    tdDnodes[i+mnodeNums].starttaosd()
                    # sleep(10)
            elif stopRole == "dnode":
                for i in range(dnodeNumbers):
                    tdDnodes[i].stoptaosd()
                    # tdLog.info('select  cast(c2 as nchar(10)) from db0_0.stb_1;')
                    # TdSqlEx.execute('select  cast(c2 as nchar(10)) from db0_0.stb_1;')
                    # tdLog.info('select  avg(c1)  from db0_0.stb_0 interval(10s);')
                    # TdSqlEx.execute('select  avg(c1)  from db0_0.stb_0 interval(10s);')
                    # sleep(10)
                    tdDnodes[i].starttaosd()
                    # sleep(10)
            # dnodeNumbers don't include database of schema
            if clusterComCheck.checkDnodes(dnodeNumbers):
                tdLog.info("123")
            else:
                print("456")

                self.stopThread(threads)
                tdLog.exit("one or more of dnodes failed to start ")
                # self.check3mnode()
            stopcount+=1

        for tr in threads:
            tr.join()
        clusterComCheck.checkDnodes(dnodeNumbers)
        clusterComCheck.checkDbRows(dbNumbers)
        # clusterComCheck.checkDb(dbNumbers,1,paraDict["dbName"])

        # tdSql.execute("use %s" %(paraDict["dbName"]))
        tdSql.query("show %s.stables"%(paraDict["dbName"]))
        tdSql.checkRows(paraDict["stbNumbers"])
        # for i in range(paraDict['stbNumbers']):
        #     stableName= '%s.%s_%d'%(paraDict["dbName"],paraDict['stbName'],i)
        #     tdSql.query("select count(*) from %s"%stableName)
        #     tdSql.checkData(0,0,rowsPerStb)
        clusterComCheck.check_vgroups_status(vgroup_numbers=paraDict["vgroups"],db_replica=replica3,db_name=paraDict["dbName"],count_number=240)        
    def run(self):
        # print(self.master_dnode.cfgDict)
        self.fiveDnodeThreeMnode(dnodeNumbers=6,mnodeNums=3,restartNumbers=4,stopRole='dnode')

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
