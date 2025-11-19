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

class Test5dnode3mnodeSep1VnodeStopMnodeCreateDb:

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


    def fiveDnodeThreeMnode(self,dnodeNumbers,mnodeNums,restartNumbers,stopRole):
        tdLog.printNoPrefix("======== test case 1: ")
        paraDict = {'dbName':     'db',
                    'dbNumbers':   6,
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    2,
                    'replica':    1,
                    'stbName':    'stb',
                    'stbNumbers': 100,
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbNum':     1,
                    }

        dnodeNumbers=int(dnodeNumbers)
        mnodeNums=int(mnodeNums)
        repeatNumber = 2
        dbNumbers=int(paraDict['dbNumbers'])

        vnodeNumbers = int(dnodeNumbers-mnodeNums)
        allDbNumbers=dbNumbers
        allStbNumbers=(paraDict['stbNumbers']*restartNumbers)
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

        tdLog.info("create database and stable")
        tdDnodes=cluster.dnodes
        stopcount =0
        threads=[]
        for i in range(dbNumbers):
            dbNameIndex = '%s%d'%(paraDict["dbName"],i)
            newTdSql=tdCom.newTdSql()
            threads.append(threading.Thread(target=clusterComCreate.createDeltedatabases, args=(newTdSql, dbNameIndex,repeatNumber,paraDict["dropFlag"], paraDict["vgroups"],paraDict['replica'])))

        for tr in threads:
            tr.start()

        tdLog.info("Take turns stopping Mnodes ")
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
                    # sleep(10)
                    tdDnodes[i].starttaosd()
                    # sleep(10)

            # dnodeNumbers don't include database of schema
            if clusterComCheck.checkDnodes(dnodeNumbers):
                tdLog.info("check dnodes status is ready")
            else:
                tdLog.info("check dnodes status is not ready")
                self.stopThread(threads)
                tdLog.exit("one or more of dnodes failed to start ")
                # self.check3mnode()
            stopcount+=1

        for tr in threads:
            tr.join()
        tdLog.info("check dnode number:")
        clusterComCheck.checkDnodes(dnodeNumbers)
        tdSql.query("select * from information_schema.ins_databases")
        tdLog.debug("we find %d databases but exepect to create %d  databases "%(tdSql.queryRows-2,allDbNumbers))

        # tdLog.info("check DB Rows:")
        # clusterComCheck.checkDbRows(allDbNumbers)
        # tdLog.info("check DB Status on by on")
        # for i in range(restartNumbers):
        #     clusterComCheck.checkDb(paraDict['dbNumbers'],restartNumbers,dbNameIndex = '%s%d'%(paraDict["dbName"],i))


    def test_5dnode3mnode_sep1_vnode_stop_mnode_create_db(self):
        """Cluster 5 dnodes 3 mnode create db mnode

        1. Create 5 node and 3 mnode cluster
        2. Ensure above cluster setup success
        3. Check mnode is leader and only 1 mnode
        4. Except check some error operations
        5. Create database for four times
        6. Stop mnode 1
        7. Start mnode 1
        8. Ensure cluster work well

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-11-01 Alex Duan Migrated from uncatalog/system-test/6-cluster/test_5dnode3mnode_sep1_vnode_stop_mnode_create_db.py

        """
        # print(self.master_dnode.cfgDict)
        self.fiveDnodeThreeMnode(dnodeNumbers=6,mnodeNums=3,restartNumbers=2,stopRole='mnode')

        tdLog.success(f"{__file__} successfully executed")

