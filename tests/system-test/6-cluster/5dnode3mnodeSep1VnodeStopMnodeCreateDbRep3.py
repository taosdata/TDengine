from ssl import ALERT_DESCRIPTION_CERTIFICATE_UNOBTAINABLE
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
        self.replicaVar = replicaVar

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


    def fiveDnodeThreeMnode(self,dnodeNumbers,mnodeNums,restartNumbers,stopRole):
        tdLog.printNoPrefix("======== test case 1: ")
        paraDict = {'dbName':     'db',
                    'dbNumbers':   6,
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    4,
                    'replica':    3,
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
        vnodeNumbers = int(dnodeNumbers-mnodeNums)
        allDbNumbers=(paraDict['dbNumbers']*restartNumbers)
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
        for i in range(restartNumbers):
            dbNameIndex = '%s%d'%(paraDict["dbName"],i)
            newTdSql=tdCom.newTdSql()
            threads.append(threading.Thread(target=clusterComCreate.create_databases, args=(newTdSql, dbNameIndex,paraDict["dbNumbers"],paraDict["dropFlag"], paraDict["vgroups"],paraDict['replica'])))

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


    def run(self):
        # print(self.master_dnode.cfgDict)
        self.fiveDnodeThreeMnode(dnodeNumbers=6,mnodeNums=3,restartNumbers=3,stopRole='mnode')

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
