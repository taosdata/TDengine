
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
        if len(sqlString) != 0:
            child.sendline (sqlString)
            w = child.expect(["Query OK", "Create OK", "Insert OK", "Drop OK", taosExpect.TIMEOUT, taosExpect.EOF], timeout=10)
            if w == 0 or w == 1 or w == 2:
                return "TAOS_OK"
            else:
                print(1)
                print(retResult)
                return "TAOS_FAIL"
        else:
            if key == 'A' or key1 == 'A' or key == 'C' or key1 == 'C' or key == 'V' or key1 == 'V':
                return "TAOS_OK", retResult
            else:
                return  "TAOS_OK"
    else:
        if key == 'A' or key1 == 'A' or key == 'C' or key1 == 'C' or key == 'V' or key1 == 'V':
            return "TAOS_OK", retResult
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
        # time.sleep(2)
        tdSql.query("create user testpy pass 'testpy'")

        #hostname = socket.gethostname()
        #tdLog.info ("hostname: %s" % hostname)

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

        tdLog.printNoPrefix("================================ parameter: -h")
        newDbName="dbh"
        sqlString = 'create database ' + newDbName + ';'
        retCode = taos_command(buildPath, "h", keyDict['h'], "taos>", keyDict['c'], sqlString)
        if retCode != "TAOS_OK":
            tdLog.exit("taos -h %s fail"%keyDict['h'])
        else:
            #dataDbName = ["information_schema", "performance_schema", "db", newDbName]
            tdSql.query("select * from information_schema.ins_databases")
            #tdSql.getResult("select * from information_schema.ins_databases")
            for i in range(tdSql.queryRows):
                if tdSql.getData(i, 0) == newDbName:
                    break
            else:
                tdLog.exit("create db fail after taos -h %s fail"%keyDict['h'])

            tdSql.query('drop database %s'%newDbName)

        tdLog.printNoPrefix("================================ parameter: -P")
        #tdDnodes.stop(1)
        #sleep(3)
        #tdDnodes.start(1)
        #sleep(3)
        #keyDict['P'] = 6030
        newDbName = "dbpp"
        sqlString = 'create database ' + newDbName + ';'
        retCode = taos_command(buildPath, "P", keyDict['P'], "taos>", keyDict['c'], sqlString)
        if retCode != "TAOS_OK":
            tdLog.exit("taos -P %s fail"%keyDict['P'])
        else:
            tdSql.query("select * from information_schema.ins_databases")
            for i in range(tdSql.queryRows):
                if tdSql.getData(i, 0) == newDbName:
                    break
            else:
                tdLog.exit("create db fail after taos -P %s fail"%keyDict['P'])

            tdSql.query('drop database %s'%newDbName)

        tdLog.printNoPrefix("================================ parameter: -u")
        newDbName="dbu"
        sqlString = 'create database ' + newDbName + ';'
        retCode = taos_command(buildPath, "u", keyDict['u'], "taos>", keyDict['c'], sqlString, "p", keyDict['p'])
        if retCode != "TAOS_OK":
            tdLog.exit("taos -u %s -p%s fail"%(keyDict['u'], keyDict['p']))
        else:
            tdSql.query("select * from information_schema.ins_databases")
            for i in range(tdSql.queryRows):
                if tdSql.getData(i, 0) == newDbName:
                    break
            else:
                tdLog.exit("create db fail after taos -u %s -p%s fail"%(keyDict['u'], keyDict['p']))

            tdSql.query('drop database %s'%newDbName)

        tdLog.printNoPrefix("================================ parameter: -A")
        newDbName="dbaa"
        retCode, retVal = taos_command(buildPath, "p", keyDict['p'], "taos>", keyDict['c'], '', "A", '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -A fail")

        sqlString = 'create database ' + newDbName + ';'
        retCode = taos_command(buildPath, "u", keyDict['u'], "taos>", keyDict['c'], sqlString, 'a', retVal)
        if retCode != "TAOS_OK":
            tdLog.exit("taos -u %s -a %s"%(keyDict['u'], retVal))

        tdSql.query("select * from information_schema.ins_databases")
        for i in range(tdSql.queryRows):
            if tdSql.getData(i, 0) == newDbName:
                break
        else:
            tdLog.exit("create db fail after taos -u %s -a %s fail"%(keyDict['u'], retVal))

        tdSql.query('drop database %s'%newDbName)

        tdLog.printNoPrefix("================================ parameter: -s")
        newDbName="dbss"
        keyDict['s'] = "\"create database " + newDbName + "\""
        retCode = taos_command(buildPath, "s", keyDict['s'], "Create OK", keyDict['c'], '', '', '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -s fail")

        print ("========== check new db ==========")
        tdSql.query("select * from information_schema.ins_databases")
        for i in range(tdSql.queryRows):
            if tdSql.getData(i, 0) == newDbName:
                break
        else:
            tdLog.exit("create db fail after taos -s %s fail"%(keyDict['s']))

        keyDict['s'] = "\"create table " + newDbName + ".stb (ts timestamp, c int) tags (t int)\""
        retCode = taos_command(buildPath, "s", keyDict['s'], "Create OK", keyDict['c'], '', '', '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -s create table fail")

        keyDict['s'] = "\"create table " + newDbName + ".ctb0 using " + newDbName + ".stb tags (0) " + newDbName + ".ctb1 using " + newDbName + ".stb tags (1)\""
        retCode = taos_command(buildPath, "s", keyDict['s'], "Create OK", keyDict['c'], '', '', '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -s create table fail")

        keyDict['s'] = "\"insert into " + newDbName + ".ctb0 values('2021-04-01 08:00:00.000', 10)('2021-04-01 08:00:01.000', 20) " + newDbName + ".ctb1 values('2021-04-01 08:00:00.000', 11)('2021-04-01 08:00:01.000', 21)\""
        retCode = taos_command(buildPath, "s", keyDict['s'], "Insert OK", keyDict['c'], '', '', '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -s insert data fail")

        sqlString = "select * from " + newDbName + ".ctb0"
        tdSql.query(sqlString)
        tdSql.checkData(0, 0, '2021-04-01 08:00:00.000')
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(1, 0, '2021-04-01 08:00:01.000')
        tdSql.checkData(1, 1, 20)
        sqlString = "select * from " + newDbName + ".ctb1"
        tdSql.query(sqlString)
        tdSql.checkData(0, 0, '2021-04-01 08:00:00.000')
        tdSql.checkData(0, 1, 11)
        tdSql.checkData(1, 0, '2021-04-01 08:00:01.000')
        tdSql.checkData(1, 1, 21)

        keyDict['s'] = "\"select * from " + newDbName + ".ctb0\""
        retCode = taos_command(buildPath, "s", keyDict['s'], "2021-04-01 08:00:01.000", keyDict['c'], '', '', '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -r show fail")

        tdLog.printNoPrefix("================================ parameter: -r")
        keyDict['s'] = "\"select * from " + newDbName + ".ctb0\""
        retCode = taos_command(buildPath, "s", keyDict['s'], "1617235200000", keyDict['c'], '', 'r', '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -r show fail")

        keyDict['s'] = "\"select * from " + newDbName + ".ctb1\""
        retCode = taos_command(buildPath, "s", keyDict['s'], "1617235201000", keyDict['c'], '', 'r', '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -r show fail")

        tdSql.query('drop database %s'%newDbName)

        tdLog.printNoPrefix("================================ parameter: -f")
        pwd=os.getcwd()
        newDbName="dbf"
        sqlFile = pwd + "/0-others/sql.txt"
        sql1 = "echo create database " + newDbName + " > " + sqlFile
        sql2 = "echo use " + newDbName + " >> " + sqlFile
        if platform.system().lower() == 'windows':
            sql3 = "echo create table ntbf (ts timestamp, c binary(40)) >> " + sqlFile
            sql4 = "echo insert into ntbf values (\"2021-04-01 08:00:00.000\", \"test taos -f1\")(\"2021-04-01 08:00:01.000\", \"test taos -f2\") >> " + sqlFile
        else:
            sql3 = "echo 'create table ntbf (ts timestamp, c binary(40))' >> " + sqlFile
            sql4 = "echo 'insert into ntbf values (\"2021-04-01 08:00:00.000\", \"test taos -f1\")(\"2021-04-01 08:00:01.000\", \"test taos -f2\")' >> " + sqlFile
        sql5 = "echo show databases >> " + sqlFile
        os.system(sql1)
        os.system(sql2)
        os.system(sql3)
        os.system(sql4)
        os.system(sql5)

        keyDict['f'] = pwd + "/0-others/sql.txt"
        retCode = taos_command(buildPath, "f", keyDict['f'], 'performance_schema', keyDict['c'], '', '', '')
        print("============ ret code: ", retCode)
        if retCode != "TAOS_OK":
            tdLog.exit("taos -f fail")

        print ("========== check new db ==========")
        tdSql.query("select * from information_schema.ins_databases")
        for i in range(tdSql.queryRows):
            #print ("dbseq: %d, dbname: %s"%(i, tdSql.getData(i, 0)))
            if tdSql.getData(i, 0) == newDbName:
                break
        else:
            tdLog.exit("create db fail after taos -f fail")

        sqlString = "select * from " + newDbName + ".ntbf"
        tdSql.query(sqlString)
        tdSql.checkData(0, 0, '2021-04-01 08:00:00.000')
        tdSql.checkData(0, 1, 'test taos -f1')
        tdSql.checkData(1, 0, '2021-04-01 08:00:01.000')
        tdSql.checkData(1, 1, 'test taos -f2')

        shellCmd = "rm -f " + sqlFile
        os.system(shellCmd)
        tdSql.query('drop database %s'%newDbName)

        tdLog.printNoPrefix("================================ parameter: -C")
        #newDbName="dbcc"
        retCode, retVal = taos_command(buildPath, "C", keyDict['C'], "buildinfo", keyDict['c'], '', '', '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -C fail")


        #print ("-C return content:\n ", retVal)
        totalCfgItem = {"firstEp":['', '', ''], }
        for line in retVal.splitlines():
            strList = line.split()
            if (len(strList) > 2):
                totalCfgItem[strList[1]] = strList

        #print ("dict content:\n ", totalCfgItem)
        firstEp = keyDict["h"] + ':' + keyDict['P']
        if (totalCfgItem["firstEp"][2] != firstEp) and (totalCfgItem["firstEp"][0] != 'cfg_file'):
            tdLog.exit("taos -C return firstEp error!")

        if (totalCfgItem["rpcDebugFlag"][2] != self.rpcDebugFlagVal) and (totalCfgItem["rpcDebugFlag"][0] != 'cfg_file'):
            tdLog.exit("taos -C return rpcDebugFlag error!")

        count = os.cpu_count()
        if (totalCfgItem["numOfCores"][2] != count) and (totalCfgItem["numOfCores"][0] != 'default'):
            tdLog.exit("taos -C return numOfCores error!")

        version = totalCfgItem["version"][2]

        tdLog.printNoPrefix("================================ parameter: -V")
        #newDbName="dbvv"
        retCode, retVal = taos_command(buildPath, "V", keyDict['V'], "", keyDict['c'], '', '', '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -V fail")

        version = 'version: ' + version
        retVal = retVal.replace("\n", "")
        retVal = retVal.replace("\r", "")
        if retVal.startswith(version) == False:
            print ("return version: [%s]"%retVal)
            print ("dict version: [%s]"%version)
            tdLog.exit("taos -V version not match")

        tdLog.printNoPrefix("================================ parameter: -d")
        newDbName="dbd"
        sqlString = 'create database ' + newDbName + ';'
        retCode = taos_command(buildPath, "d", keyDict['d'], "taos>", keyDict['c'], sqlString, '', '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -d %s fail"%(keyDict['d']))
        else:
            tdSql.query("select * from information_schema.ins_databases")
            for i in range(tdSql.queryRows):
                if tdSql.getData(i, 0) == newDbName:
                    break
            else:
                tdLog.exit("create db fail after taos -d %s fail"%(keyDict['d']))

            tdSql.query('drop database %s'%newDbName)

        retCode = taos_command(buildPath, "d", 'dbno', "taos>", keyDict['c'], sqlString, '', '')
        if retCode != "TAOS_FAIL":
            tdLog.exit("taos -d dbno fail")

        tdLog.printNoPrefix("================================ parameter: -w")
        newDbName="dbw"
        keyDict['s'] = "\"create database " + newDbName + "\""
        retCode = taos_command(buildPath, "s", keyDict['s'], "Create OK", keyDict['c'], '', '', '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -w fail")

        keyDict['s'] = "\"create table " + newDbName + ".ntb (ts timestamp, c binary(128))\""
        retCode = taos_command(buildPath, "s", keyDict['s'], "Create OK", keyDict['c'], '', '', '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -w create table fail")

        keyDict['s'] = "\"insert into " + newDbName + ".ntb values('2021-04-01 08:00:00.001', 'abcd0123456789')('2021-04-01 08:00:00.002', 'abcd012345678901234567890123456789') \""
        retCode = taos_command(buildPath, "s", keyDict['s'], "Insert OK", keyDict['c'], '', '', '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -w insert data fail")

        keyDict['s'] = "\"insert into " + newDbName + ".ntb values('2021-04-01 08:00:00.003', 'aaaaaaaaaaaaaaaaaaaa')('2021-04-01 08:00:01.004', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb') \""
        retCode = taos_command(buildPath, "s", keyDict['s'], "Insert OK", keyDict['c'], '', '', '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -w insert data fail")

        keyDict['s'] = "\"insert into " + newDbName + ".ntb values('2021-04-01 08:00:00.005', 'cccccccccccccccccccc')('2021-04-01 08:00:01.006', 'dddddddddddddddddddddddddddddddddddddddd') \""
        retCode = taos_command(buildPath, "s", keyDict['s'], "Insert OK", keyDict['c'], '', '', '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -w insert data fail")

        keyDict['s'] = "\"select * from " + newDbName + ".ntb \""
        retCode = taos_command(buildPath, "s", keyDict['s'], "aaaaaaaaaaaaaaaaaaaa", keyDict['c'], '', '', '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -w insert data fail")

        keyDict['s'] = "\"select * from " + newDbName + ".ntb \""
        retCode = taos_command(buildPath, "s", keyDict['s'], "dddddddddddddddddddddddddddddddddddddddd", keyDict['c'], '', '', '')
        if retCode != "TAOS_FAIL":
            tdLog.exit("taos -w insert data fail")

        keyDict['s'] = "\"select * from " + newDbName + ".ntb \""
        retCode = taos_command(buildPath, "s", keyDict['s'], "dddddddddddddddddddddddddddddddddddddddd", keyDict['c'], '', 'w', '60')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -w insert data fail")

        tdSql.query('drop database %s'%newDbName)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
