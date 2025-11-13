import pytest
import taos
import sys
import time
import os
import socket
import threading
import inspect

from new_test_framework.utils import tdLog, tdSql, tdDnodes, cluster, tdCom
#sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from clusterCommonCreate import *
from clusterCommonCheck import clusterComCheck

import ctypes

class Test5dnode3mnodeAdd1Dnode:

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")


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
            tdSql.execute("drop database if exists db%d" %couti, 20)
            print("create database if not exists db%d replica 1 duration 100" %couti)
            tdSql.execute("create database if not exists db%d replica 1 duration 100" %couti)
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

    @pytest.mark.cluster
    def test_five_dnode_three_mnode(self):
        """Cluster 5 dnodes 3 mnodes

        1. Create 5 dnode 3 mnode cluster
        2. Create database and stables
        3. Insert data into stables
        4. Add 1 dnode to cluster during data insertion
        5. Restart all dnodes one by one
        6. Verify data integrity

        Since: v3.3.0.0

        Labels: cluster

        History:
            - 2022-7-10 tomchon Created
            - 2025-5-13 Huo Hong Migrated to new test framework

        """
        dnodeNumbers=6
        mnodeNums=3
        restartNumbers=1
        stopRole='dnode'
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
                    'ctbNum':     200,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    "rowsPerTbl": 1000,
                    "batchNum": 5000
                    }
        hostname = socket.gethostname()
        dnodeNumbers=int(dnodeNumbers)
        mnodeNums=int(mnodeNums)
        vnodeNumbers = int(dnodeNumbers-mnodeNums)
        allctbNumbers=(paraDict['stbNumbers']*paraDict["ctbNum"])
        rowsPerStb=paraDict["ctbNum"]*paraDict["rowsPerTbl"]
        rowsall=rowsPerStb*paraDict['stbNumbers']
        dbNumbers = 1
        paraDict['replica'] = self.replicaVar

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
        # dnode6=cluster.addDnode(6)
        # tdDnodes.append(dnode6)
        # tdDnodes = ClusterDnodes(tdDnodes)
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
        #insert data
        for i in range(paraDict['stbNumbers']):
            stableName= '%s_%d'%(paraDict['stbName'],i)
            newTdSql=tdCom.newTdSql()
            threads.append(threading.Thread(target=clusterComCreate.insert_data, args=(newTdSql, paraDict["dbName"],stableName,paraDict["ctbNum"],paraDict["rowsPerTbl"],paraDict["batchNum"],paraDict["startTs"])))
        for tr in threads:
            tr.start()
        dnode7Port=int(6030+6*100)
        tdSql.execute("create dnode '%s:%d'"%(hostname,dnode7Port))
        clusterComCheck.checkDnodes(dnodeNumbers)
        for tr in threads:
            tr.join()
        while stopcount < restartNumbers:
            tdLog.info(" restart loop: %d"%stopcount )
            if stopRole == "mnode":
                for i in range(mnodeNums):
                    tdDnodes[i].stoptaosd()
                    tdDnodes[i].starttaosd()
            elif stopRole == "vnode":
                for i in range(vnodeNumbers):
                    tdDnodes[i+mnodeNums].stoptaosd()
                    tdDnodes[i+mnodeNums].starttaosd()
            elif stopRole == "dnode":
                for i in range(dnodeNumbers):
                    tdDnodes[i].stoptaosd()
                    tdDnodes[i].starttaosd()

            # dnodeNumbers don't include database of schema
            if clusterComCheck.checkDnodes(dnodeNumbers):
                tdLog.info("123")
            else:
                print("456")

                self.stopThread(threads)
                tdLog.exit("one or more of dnodes failed to start ")
                # self.check3mnode()
            stopcount+=1

        clusterComCheck.checkDnodes(dnodeNumbers)
        clusterComCheck.checkDbRows(dbNumbers)
        # clusterComCheck.checkDb(dbNumbers,1,paraDict["dbName"])

        tdSql.execute("use %s" %(paraDict["dbName"]))
        tdSql.query("show stables")
        tdSql.checkRows(paraDict["stbNumbers"])
        for i in range(paraDict['stbNumbers']):
            stableName= '%s_%d'%(paraDict['stbName'],i)
            tdSql.query("select * from %s"%stableName)
            tdSql.checkRows(rowsPerStb)
        tdLog.success(f"{__file__} successfully executed")

