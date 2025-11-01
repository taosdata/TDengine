from new_test_framework.utils import tdLog, tdSql, cluster, tdCom
import sys
import time
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from cluster_common_create import *
from cluster_common_check import *
import threading
import inspect
import ctypes

class Test6dnode3mnodeInsertLessDataAlterRep3to1to3:

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
        cls.TDDnodes = None


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


    def fiveDnodeThreeMnode(self,dnodeNumbers,mnodeNums,restartNumbers,stopRole):
        tdLog.printNoPrefix("======== test case 1: ")
        paraDict = {'dbName':     'db0_0',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    4,
                    'replica':    3,
                    'stbName':    'stb',
                    'stbNumbers': 2,
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbNum':     1,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    "rowsPerTbl": 1,
                    "batchNum": 5000
                    }

        dnodeNumbers=int(dnodeNumbers)
        mnodeNums=int(mnodeNums)
        vnodeNumbers = int(dnodeNumbers-mnodeNums)
        replica1 = 1
        replica3 = 3
        allctbNumbers=(paraDict['stbNumbers']*paraDict["ctbNum"])
        rowsPerStb=paraDict["ctbNum"]*paraDict["rowsPerTbl"]
        rowsall=rowsPerStb*paraDict['stbNumbers']
        dbNumbers = 1

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
        tdLog.info(f"alter database db0_0 replica {replica1}")
        TdSqlEx.execute(f'alter database db0_0 replica {replica1}')
        for tr in threads:
            tr.join()
        clusterComCheck.checkDnodes(dnodeNumbers)
        clusterComCheck.checkDbRows(dbNumbers)
        # clusterComCheck.checkDb(dbNumbers,1,paraDict["dbName"])

        # tdSql.execute("use %s" %(paraDict["dbName"]))
        tdSql.query("show %s.stables"%(paraDict["dbName"]))
        tdSql.checkRows(paraDict["stbNumbers"])
        for i in range(paraDict['stbNumbers']):
            stableName= '%s.%s_%d'%(paraDict["dbName"],paraDict['stbName'],i)
            tdSql.query("select count(*) from %s"%stableName)
            tdSql.checkData(0,0,rowsPerStb)
        
        clusterComCheck.check_vgroups_status(vgroup_numbers=paraDict["vgroups"],db_replica=replica1,db_name=paraDict["dbName"],count_number=100)
        time.sleep(5)
        tdLog.info(f"show transactions;alter database db0_0 replica {replica3};")
        TdSqlEx.execute(f'show transactions;')
        TdSqlEx.execute(f'alter database db0_0 replica {replica3};')
        clusterComCheck.check_vgroups_status(vgroup_numbers=paraDict["vgroups"],db_replica=replica3,db_name=paraDict["dbName"],count_number=180)

    def test_6dnode3mnode_insert_less_data_alter_rep3to1to3(self):
        """Cluster 6 dnodes 3 mnode rep3to1to3

        1. Create 6 node and 3 mnode cluster
        2. Ensure above cluster setup success
        3. Create database with replica 3
        4. Create stable and child table, insert less data
        5. Alter database replica to 1
        6. Ensure cluster work well
        7. Alter database replica to 3
        8. Ensure cluster work well
        9. Except check some error operations

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-11-01 Alex Duan Migrated from uncatalog/system-test/6-cluster/test_6dnode3mnode_insert_less_data_alter_rep3to1to3.py

        """
        # print(self.master_dnode.cfgDict)
        self.fiveDnodeThreeMnode(dnodeNumbers=6,mnodeNums=3,restartNumbers=4,stopRole='dnode')

        tdLog.success(f"{__file__} successfully executed")