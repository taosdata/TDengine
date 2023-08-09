
import taos
import sys
import time
import socket
import os
import platform
if platform.system().lower() == 'windows':
    import wexpect as taosExpect
else:
    import pexpect as taosExpect

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *

def taos_command (buildPath, key, value, expectString, sqlString=''):
    if len(key) == 0:
        tdLog.exit("taos test key is null!")

    if platform.system().lower() == 'windows':
        taosCmd = buildPath + '\\build\\bin\\taos.exe '
        taosCmd = taosCmd.replace('\\','\\\\')
    else:
        taosCmd = buildPath + '/build/bin/taos '

    cfgPath = buildPath + "/../sim/psim/cfg"
    taosCmd = taosCmd + ' -c ' + cfgPath + ' -' + key
    if len(value) != 0:
        taosCmd = taosCmd + ' ' + value

    tdLog.info ("taos cmd: %s" % taosCmd)

    child = taosExpect.spawn(taosCmd, timeout=20)
    #output = child.readline()
    #print (output.decode())
    if len(expectString) != 0:
        i = child.expect([expectString, taosExpect.TIMEOUT, taosExpect.EOF], timeout=20)
    else:
        i = child.expect([taosExpect.TIMEOUT, taosExpect.EOF], timeout=20)

    if platform.system().lower() == 'windows':
        retResult = child.before
    else:
        retResult = child.before.decode()
    print(retResult)
    #print(child.after.decode())
    if i == 0:
        print ('taos login success! Here can run sql, taos> ')
        return  "TAOS_OK"
    else:
        return "TAOS_FAIL"

class TDTestCase:
    #updatecfgDict = {'clientCfg': {'serverPort': 7080, 'firstEp': 'trd02:7080', 'secondEp':'trd02:7080'},\
    #                 'serverPort': 7080, 'firstEp': 'trd02:7080'}
    hostname = socket.gethostname()
    if (platform.system().lower() == 'windows' and not tdDnodes.dnodes[0].remoteIP == ""):
        try:
            config = eval(tdDnodes.dnodes[0].remoteIP)
            hostname = config["host"]
        except Exception:
            hostname = tdDnodes.dnodes[0].remoteIP
    serverPort = '7080'
    rpcDebugFlagVal = '143'
    clientCfgDict = {'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    clientCfgDict["serverPort"]    = serverPort
    clientCfgDict["firstEp"]       = hostname + ':' + serverPort
    clientCfgDict["secondEp"]      = hostname + ':' + serverPort
    clientCfgDict["rpcDebugFlag"]  = rpcDebugFlagVal
    clientCfgDict["fqdn"] = hostname

    updatecfgDict = {'clientCfg': {}, 'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    updatecfgDict["clientCfg"]  = clientCfgDict
    updatecfgDict["serverPort"] = serverPort
    updatecfgDict["firstEp"]    = hostname + ':' + serverPort
    updatecfgDict["secondEp"]   = hostname + ':' + serverPort
    updatecfgDict["fqdn"] = hostname

    print ("===================: ", updatecfgDict)

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

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

    def run(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdSql.prepare()
        # time.sleep(2)
        tdSql.query("create user testpy pass 'testpy'")

        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        cfgPath = buildPath + "/../sim/psim/cfg"
        tdLog.info("cfgPath: %s" % cfgPath)

        checkNetworkStatus = ['0: unavailable', '1: network ok', '2: service ok', '3: service degraded', '4: exiting']
        netrole            = ['client', 'server']

        keyDict = {'h':'', 'P':'6030', 'p':'testpy', 'u':'testpy', 'a':'', 'A':'', 'c':'', 'C':'', 's':'', 'r':'', 'f':'', \
                   'k':'', 't':'', 'n':'', 'l':'1024', 'N':'100', 'V':'', 'd':'db', 'w':'30', '-help':'', '-usage':'', '?':''}

        keyDict['h'] = self.hostname
        keyDict['c'] = cfgPath
        keyDict['P'] = self.serverPort

        tdSql.query("drop database if exists db1")
        tdSql.query("create database if not exists db1 vgroups 1")
        tdSql.query("use db1")
        tdSql.query("create table tba (ts timestamp, f1 binary(2))")
        tdSql.query("insert into tba values (now, '22')")
        tdSql.query("select * from tba")
        tdSql.checkData(0, 1, '22')

        keyDict['s'] = "\"alter table db1.tba modify column f1 binary(5) \""
        retCode = taos_command(buildPath, "s", keyDict['s'], "Query OK", '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -s fail")

        keyDict['s'] = "\"insert into db1.tba values (now, '55555')\""
        retCode = taos_command(buildPath, "s", keyDict['s'], "Insert OK", '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -s fail")

        tdSql.query("select * from tba order by ts")
        tdSql.checkData(0, 1, '22')
        tdSql.checkData(1, 1, '55555')



        keyDict['s'] = "\"alter table db1.tba add column f2 binary(5) \""
        retCode = taos_command(buildPath, "s", keyDict['s'], "Query OK", '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -s fail")

        tdSql.query("select * from tba order by ts")
        tdSql.query("select * from tba order by ts")
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 2, None)




        keyDict['s'] = "\"alter table db1.tba add column f3 binary(5) \""
        retCode = taos_command(buildPath, "s", keyDict['s'], "Query OK", '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -s fail")

        tdSql.query("select f3 from tba order by ts")
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)


        tdSql.query("create table stb (ts timestamp, f1 int, f2 binary(2)) tags (tg1 binary(2))")
        tdSql.query("create table tb1 using stb tags('bb')")
        tdSql.query("insert into tb1 values (now, 2,'22')")
        tdSql.query("select count(*) from stb group by tg1")
        tdSql.checkData(0, 0, 1)

        keyDict['s'] = "\"alter table db1.stb modify tag tg1 binary(5) \""
        retCode = taos_command(buildPath, "s", keyDict['s'], "Query OK", '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -s fail")

        keyDict['s'] = "\"create table db1.tb2 using db1.stb tags('bbbbb')\""
        retCode = taos_command(buildPath, "s", keyDict['s'], "Create OK", '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -s fail")

        keyDict['s'] = "\"insert into db1.tb2 values (now, 2,'22')\""
        retCode = taos_command(buildPath, "s", keyDict['s'], "Insert OK", '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -s fail")

        keyDict['s'] = "\"alter table db1.stb modify column f2 binary(5) \""
        retCode = taos_command(buildPath, "s", keyDict['s'], "Query OK", '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -s fail")

        keyDict['s'] = "\"insert into db1.tb2 values (now, 3,'55555')\""
        retCode = taos_command(buildPath, "s", keyDict['s'], "Insert OK", '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -s fail")

        tdSql.query("select count(*) from stb group by tg1 order by count(*) desc")
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 1)


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
