
import taos
import sys
import time
import socket
import platform
if platform.system().lower() == 'windows':
    import wexpect as taosExpect
else:
    import pexpect as taosExpect
import os

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *

def taos_command (buildPath, key, value, expectString, cfgDir, sqlString='', key1='', value1=''):
    if len(key) == 0:
        tdLog.exit("taos test key is null!")

    if platform.system().lower() == 'windows':
        taosCmd = buildPath + '\\build\\bin\\taos.exe '
        taosCmd = taosCmd.replace('\\','\\\\')
    else:
        taosCmd = buildPath + '/build/bin/taos '
    if len(cfgDir) != 0:
        taosCmd = taosCmd + '-c ' + cfgDir

    taosCmd = taosCmd + ' -' + key
    if len(value) != 0:
        if key == 'p':
            taosCmd = taosCmd + value
        else:
            taosCmd = taosCmd + ' ' + value

    if len(key1) != 0:
        taosCmd = taosCmd + ' -' + key1
        if key1 == 'p':
            taosCmd = taosCmd + value1
        else:
            if len(value1) != 0:
                taosCmd = taosCmd + ' ' + value1

    tdLog.info ("taos cmd: %s" % taosCmd)

    child = taosExpect.spawn(taosCmd, timeout=3)
    #output = child.readline()
    #print (output.decode())
    if len(expectString) != 0:
        i = child.expect([expectString, taosExpect.TIMEOUT, taosExpect.EOF], timeout=6)
    else:
        i = child.expect([taosExpect.TIMEOUT, taosExpect.EOF], timeout=6)

    if platform.system().lower() == 'windows':
        retResult = child.before
    else:
        retResult = child.before.decode()
    print("expect() return code: %d, content:\n %s\n"%(i, retResult))
    #print(child.after.decode())
    if i == 0:
        print ('taos login success! Here can run sql, taos> ')
        if len(sqlString) != 0:
            child.sendline (sqlString)
            w = child.expect(["Query OK", taosExpect.TIMEOUT, taosExpect.EOF], timeout=1)
            if platform.system().lower() == 'windows':
                retResult = child.before
            else:
                retResult = child.before.decode()
            if w == 0:
                return "TAOS_OK", retResult
            else:
                return "TAOS_FAIL", retResult
        else:
            if key == 'A' or key1 == 'A' or key == 'C' or key1 == 'C' or key == 'V' or key1 == 'V':
                return "TAOS_OK", retResult
            else:
                return  "TAOS_OK", retResult
    else:
        if key == 'A' or key1 == 'A' or key == 'C' or key1 == 'C' or key == 'V' or key1 == 'V':
            return "TAOS_OK", retResult
        else:
            return "TAOS_FAIL", retResult

class TDTestCase:
    #updatecfgDict = {'clientCfg': {'serverPort': 7080, 'firstEp': 'trd02:7080', 'secondEp':'trd02:7080'},\
    #                 'serverPort': 7080, 'firstEp': 'trd02:7080'}
    hostname = socket.gethostname()
    if (platform.system().lower() == 'windows' and not tdDnodes.dnodes[0].remoteIP == ""):
        try:
            config = eval(tdDnodes.dnodes[0].remoteIP )
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
        tdSql.init(conn.cursor())

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

        tdLog.printNoPrefix("================================ parameter: -k")
        sqlString = ''
        retCode, retVal = taos_command(buildPath, "k", '', "", keyDict['c'], sqlString)
        if "2: service ok" in retVal:
            tdLog.info("taos -k success")
        else:
            tdLog.info(retVal)
            tdLog.exit("taos -k fail 1")

        # stop taosd
        tdDnodes.stop(1)
        #sleep(10)
        #tdDnodes.start(1)
        #sleep(5)
        retCode, retVal = taos_command(buildPath, "k", '', "", keyDict['c'], sqlString)
        if "0: unavailable" in retVal:
            tdLog.info("taos -k success")
        else:
            tdLog.info(retVal)
            tdLog.exit("taos -k fail 2")

        # restart taosd
        tdDnodes.start(1)
        #sleep(5)
        retCode, retVal = taos_command(buildPath, "k", '', "", keyDict['c'], sqlString)
        if "2: service ok" in retVal:
            tdLog.info("taos -k success")
        else:
            tdLog.info(retVal)
            tdLog.exit("taos -k fail 3")

        tdLog.printNoPrefix("================================ parameter: -n")
        # stop taosd
        tdDnodes.stop(1)

        try:
            role   = 'server'
            if platform.system().lower() == 'windows':
                taosCmd = 'mintty -h never -w hide ' + buildPath + '\\build\\bin\\taos.exe -c ' + keyDict['c']
                taosCmd = taosCmd.replace('\\','\\\\')
                taosCmd = taosCmd + ' -n ' + role
            else:
                taosCmd = 'nohup ' + buildPath + '/build/bin/taos -c ' + keyDict['c']
                taosCmd = taosCmd + ' -n ' + role + ' > /dev/null 2>&1 &'
            print (taosCmd)
            os.system(taosCmd)

            pktLen = '2000'
            pktNum = '10'
            role   = 'client'
            if platform.system().lower() == 'windows':
                taosCmd = buildPath + '\\build\\bin\\taos.exe -h 127.0.0.1 -c ' + keyDict['c']
                taosCmd = taosCmd.replace('\\','\\\\')
            else:
                taosCmd = buildPath + '/build/bin/taos -c ' + keyDict['c']
            taosCmd = taosCmd + ' -n ' + role + ' -l ' + pktLen + ' -N ' +  pktNum
            print (taosCmd)
            child = taosExpect.spawn(taosCmd, timeout=3)
            i = child.expect([taosExpect.TIMEOUT, taosExpect.EOF], timeout=6)

            if platform.system().lower() == 'windows':
                retResult = child.before
            else:
                retResult = child.before.decode()
            print("expect() return code: %d, content:\n %s\n"%(i, retResult))
            #print(child.after.decode())
            if i == 0:
                tdLog.exit('taos -n server fail!')

            expectString1 = 'response is received, size:' + pktLen
            expectSTring2 = pktNum + '/' + pktNum
            if expectString1 in retResult and expectSTring2 in retResult:
                tdLog.info("taos -n client success")
            else:
                tdLog.exit('taos -n client fail!')
        finally:
            if platform.system().lower() == 'windows':
                tdLog.info("ps -a | grep taos | awk \'{print $2}\' | xargs kill -9")
                os.system('ps -a | grep taos | awk \'{print $2}\' | xargs kill -9')
            else:
                tdLog.info("pkill -9 taos")
                # os.system('pkill -9 taos')

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
