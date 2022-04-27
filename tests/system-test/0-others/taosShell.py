
import taos
import sys
import time
import socket
import pexpect
import os

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *

def taos_command (key, value, expectString, cfgDir, dbName, key1='', value1=''):
    if len(key) == 0:
        tdLog.exit("taos test key is null!")

    if len(cfgDir) != 0:
        taosCmd = 'taos -c ' + cfgDir + ' -' + key

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

    child = pexpect.spawn(taosCmd, timeout=3)
    #output = child.readline()
    #print (output.decode())
    i = child.expect([expectString, pexpect.TIMEOUT, pexpect.EOF], timeout=1)
    retResult = child.before.decode()
    print(retResult)
    #print(child.after.decode())
    if i == 0:
        print ('taos login success! Here can run sql, taos> ')
        if len(dbName) != 0:
            child.sendline ('create database %s;'%(dbName))
            w = child.expect(["Query OK", pexpect.TIMEOUT, pexpect.EOF], timeout=1)
            if w == 0:
                return "TAOS_OK"
            else:
                return "TAOS_FAIL"
        else:
            return  "TAOS_OK"
    else:
        if key == 'A' or key1 == 'A':
            return "TAOS_OK", retResult
        else:
            return "TAOS_FAIL"

class TDTestCase:

    #updatecfgDict = {'serverPort': 7080, 'firstEp': 'localhost:7080'}

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

    def run(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdSql.prepare()
        # time.sleep(2)
        tdSql.query("create user testpy pass 'testpy'")

        hostname = socket.gethostname()
        tdLog.info ("hostname: %s" % hostname)

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

        keyDict['h'] = hostname
        keyDict['c'] = cfgPath

        tdLog.printNoPrefix("================================ parameter: -h")
        newDbName="dbh"
        retCode = taos_command("h", keyDict['h'], "taos>", keyDict['c'], newDbName)
        if retCode != "TAOS_OK":
            tdLog.exit("taos -h %s fail"%keyDict['h'])
        else:
            #dataDbName = ["information_schema", "performance_schema", "db", newDbName]
            tdSql.query("show databases")
            #tdSql.getResult("show databases")
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
        retCode = taos_command("P", keyDict['P'], "taos>", keyDict['c'], newDbName)
        if retCode != "TAOS_OK":
            tdLog.exit("taos -P %s fail"%keyDict['P'])
        else:
            tdSql.query("show databases")
            for i in range(tdSql.queryRows):
                if tdSql.getData(i, 0) == newDbName:
                    break
            else:
                tdLog.exit("create db fail after taos -P %s fail"%keyDict['P'])

            tdSql.query('drop database %s'%newDbName)

        tdLog.printNoPrefix("================================ parameter: -u")
        newDbName="dbu"
        retCode = taos_command("u", keyDict['u'], "taos>", keyDict['c'], newDbName, "p", keyDict['p'])
        if retCode != "TAOS_OK":
            tdLog.exit("taos -u %s -p%s fail"%(keyDict['u'], keyDict['p']))
        else:
            tdSql.query("show databases")
            for i in range(tdSql.queryRows):
                if tdSql.getData(i, 0) == newDbName:
                    break
            else:
                tdLog.exit("create db fail after taos -u %s -p%s fail"%(keyDict['u'], keyDict['p']))

            tdSql.query('drop database %s'%newDbName)

        tdLog.printNoPrefix("================================ parameter: -A")
        newDbName="dbaa"
        retCode, retVal = taos_command("p", keyDict['p'], "taos>", keyDict['c'], '', "A", '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -A fail")
        
        retCode = taos_command("u", keyDict['u'], "taos>", keyDict['c'], newDbName, 'a', retVal)
        if retCode != "TAOS_OK":
            tdLog.exit("taos -u %s -a %s"%(keyDict['u'], retVal))

        tdSql.query("show databases")
        for i in range(tdSql.queryRows):
            if tdSql.getData(i, 0) == newDbName:
                break
        else:
            tdLog.exit("create db fail after taos -u %s -a %s fail"%(keyDict['u'], retVal))

        tdSql.query('drop database %s'%newDbName)

        tdLog.printNoPrefix("================================ parameter: -s")
        newDbName="dbss"
        keyDict['s'] = "\"create database " + newDbName + "\""
        retCode = taos_command("s", keyDict['s'], "Query OK", keyDict['c'], '', '', '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -s fail")

        print ("========== check new db ==========")
        tdSql.query("show databases")        
        for i in range(tdSql.queryRows):
            if tdSql.getData(i, 0) == newDbName:
                break
        else:
            tdLog.exit("create db fail after taos -s %s fail"%(keyDict['s']))

        keyDict['s'] = "\"create table " + newDbName + ".stb (ts timestamp, c int) tags (t int)\""
        retCode = taos_command("s", keyDict['s'], "Query OK", keyDict['c'], '', '', '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -s create table fail")

        keyDict['s'] = "\"create table " + newDbName + ".ctb0 using " + newDbName + ".stb tags (0) " + newDbName + ".ctb1 using " + newDbName + ".stb tags (1)\""
        retCode = taos_command("s", keyDict['s'], "Query OK", keyDict['c'], '', '', '')
        if retCode != "TAOS_OK":
            tdLog.exit("taos -s create table fail")

        keyDict['s'] = "\"insert into " + newDbName + ".ctb0 values('2021-04-01 08:00:00.000', 10)('2021-04-01 08:00:01.000', 20) " + newDbName + ".ctb1 values('2021-04-01 08:00:00.000', 11)('2021-04-01 08:00:01.000', 21)\""
        retCode = taos_command("s", keyDict['s'], "Query OK", keyDict['c'], '', '', '')
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

        #tdSql.query('drop database %s'%newDbName)



    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
