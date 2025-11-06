from new_test_framework.utils import tdLog, tdSql, cluster, tdCom
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from cluster_common_create import *
from cluster_common_check import *
import inspect
import ctypes

class Test5dnode3mnodeRecreateMnode:

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
                    "rowsPerTbl": 100,
                    "batchNum": 5000
                    }
        username="user1"
        passwd="test123@#$"

        dnodeNumbers=int(dnodeNumbers)
        mnodeNums=int(mnodeNums)
        vnodeNumbers = int(dnodeNumbers-mnodeNums)
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
        # print(tdSql.queryResult)
        clusterComCheck.checkDnodes(dnodeNumbers)

        # recreate mnode
        tdSql.execute("drop dnode 2;")
        tdSql.execute('create dnode "%s:6130";'%self.host)
        tdDnodes=cluster.dnodes
        tdDnodes[1].stoptaosd()
        tdDnodes[1].deploy()

        tdDnodes[1].starttaosd()
        clusterComCheck.checkDnodes(dnodeNumbers)

        tdSql.execute("create mnode on dnode 6")
        tdSql.error("drop dnode 1;")

        # check status of clusters
        clusterComCheck.checkMnodeStatus(3)
        tdSql.execute("create user %s pass '%s' ;"%(username,passwd))
        tdSql.query("select * from information_schema.ins_users")
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][0] == "%s"%username :
                tdLog.info("create user:%s successfully"%username)

        """ case for TS-3524 and test 'taos -h' """
        bPath = tdCom.getBuildPath()
        for i in range(6):
            nodePort = 6030 + i*100
            newTdSql=tdCom.newTdSql(port=nodePort)

        tdDnodes[1].stoptaosd()

        dataPath = tdDnodes[1].dataDir
        os.system(f"rm -rf {dataPath}/*")
        os.system(f"rm -rf {dataPath}/.runing")

        tdDnodes[1].starttaosd()
        time.sleep(5)
        for i in range(6):
            nodePort = 6030 + i*100 
            newTdSql=tdCom.newTdSql(port=nodePort)

        tdDnodes[0].stoptaosd()
        dataPath = tdDnodes[0].dataDir
        os.system(f"rm -rf {dataPath}/*")
        os.system(f"rm -rf {dataPath}/.runing")
        tdDnodes[0].starttaosd()
        time.sleep(5)
        for i in range(6):
            nodePort = 6030 + i*100 
            newTdSql=tdCom.newTdSql(port=nodePort)

    def test_5dnode3mnode_recreate_mnode(self):
        """Cluster 5 dnodes 3 mnode recreate mnode

        1. Create 5 node and 3 mnode cluster
        2. Ensure above cluster setup success
        3. Check mnode is leader and only 1 mnode
        4. Except check some error operations
        5. Stop dnode 1
        6. delete dnode 1 dataDir
        7. Start dnode 1
        8. Ensure cluster work well

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-11-01 Alex Duan Migrated from uncatalog/system-test/6-cluster/test_5dnode3mnode_recreate_mnode.py

        """
        # print(self.master_dnode.cfgDict)
        self.fiveDnodeThreeMnode(dnodeNumbers=6,mnodeNums=3,restartNumbers=1,stopRole='dnode')

        tdLog.success(f"{__file__} successfully executed")