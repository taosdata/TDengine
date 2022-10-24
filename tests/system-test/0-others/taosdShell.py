
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

class TDTestCase:
    #updatecfgDict = {'clientCfg': {'serverPort': 7080, 'firstEp': 'trd02:7080', 'secondEp':'trd02:7080'},\
    #                 'serverPort': 7080, 'firstEp': 'trd02:7080'}
    # hostname = socket.gethostname()
    # if (platform.system().lower() == 'windows' and not tdDnodes.dnodes[0].remoteIP == ""):
    #     try:
    #         config = eval(tdDnodes.dnodes[0].remoteIP)
    #         hostname = config["host"]
    #     except Exception:
    #         hostname = tdDnodes.dnodes[0].remoteIP
    # serverPort = '7080'
    # rpcDebugFlagVal = '143'
    # clientCfgDict = {'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    # clientCfgDict["serverPort"]    = serverPort
    # clientCfgDict["firstEp"]       = hostname + ':' + serverPort
    # clientCfgDict["secondEp"]      = hostname + ':' + serverPort
    # clientCfgDict["rpcDebugFlag"]  = rpcDebugFlagVal
    # clientCfgDict["fqdn"] = hostname

    # updatecfgDict = {'clientCfg': {}, 'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    # updatecfgDict["clientCfg"]  = clientCfgDict
    # updatecfgDict["serverPort"] = serverPort
    # updatecfgDict["firstEp"]    = hostname + ':' + serverPort
    # updatecfgDict["secondEp"]   = hostname + ':' + serverPort
    # updatecfgDict["fqdn"] = hostname

    # print ("===================: ", updatecfgDict)

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
            if ("taosd" in files or "taosd.exe" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

    def get_process_pid(self,processname):
        #origin artical link：https://blog.csdn.net/weixin_45623536/article/details/122099062
        process_info_list = []
        process = os.popen('ps -A | grep %s'% processname)
        process_info = process.read()
        for i in process_info.split(' '):
            if i != "":
                process_info_list.append(i)
        print(process_info_list)
        if len(process_info_list) != 0 :
            pid = int(process_info_list[0])
        else :
            pid = 0
        return pid

    def checkAndstopPro(self,processName,startAction):
        i = 1
        count = 10
        for i in range(count):
            taosdPid=self.get_process_pid(processName)
            if taosdPid != 0  and   taosdPid != ""  :
                tdLog.info("stop taosd %s ,kill pid :%s "%(startAction,taosdPid))
                os.system("kill -9 %d"%taosdPid) 
                break
            else:
                tdLog.info( "wait start taosd ,times: %d "%i)
            sleep
            i+= 1
        else :
            tdLog.exit("taosd %s is not running "%startAction)    

    def taosdCommandStop(self,startAction,taosdCmdRun):
        processName="taosd"
        taosdCmd = taosdCmdRun + startAction
        tdLog.printNoPrefix("%s"%taosdCmd)
        os.system(f"nohup {taosdCmd}  & ")
        self.checkAndstopPro(processName,startAction)

    def taosdCommandExe(self,startAction,taosdCmdRun):
        taosdCmd = taosdCmdRun + startAction
        tdLog.printNoPrefix("%s"%taosdCmd)
        os.system(f"{taosdCmd}")

    def run(self):  
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
        taosdCfgPath = buildPath + "/../sim/dnode1/cfg"


        taosdCmdRun= buildPath + '/build/bin/taosd'
        tdLog.info("cfgPath: %s" % cfgPath)
        # keyDict['h'] = self.hostname
        # keyDict['c'] = cfgPath
        # keyDict['P'] = self.serverPort
        tdDnodes.stop(1)        

        startAction = " --help"
        tdLog.printNoPrefix("================================ parameter: %s"%startAction)
        self.taosdCommandExe(startAction,taosdCmdRun)

        startAction = " -h"
        tdLog.printNoPrefix("================================ parameter: %s"%startAction)
        self.taosdCommandExe(startAction,taosdCmdRun)

        startAction=" -a  jsonFile:./taosdCaseTmp.json"
        tdLog.printNoPrefix("================================ parameter: %s"%startAction)
        os.system("echo \'{\"queryPolicy\":\"3\"}\' > taosdCaseTmp.json")
        self.taosdCommandStop(startAction,taosdCmdRun)

        startAction = " -a  jsonFile:./taosdCaseTmp.json -C "
        tdLog.printNoPrefix("================================ parameter: %s"%startAction)
        self.taosdCommandExe(startAction,taosdCmdRun)

        os.system("rm -rf  taosdCaseTmp.json") 

        startAction = " -c " + taosdCfgPath 
        tdLog.printNoPrefix("================================ parameter: %s"%startAction)
        self.taosdCommandStop(startAction,taosdCmdRun)

        startAction = " -s"
        tdLog.printNoPrefix("================================ parameter: %s"%startAction)
        self.taosdCommandExe(startAction,taosdCmdRun)

        startAction = " -e  TAOS_QUERY_POLICY=2 "
        tdLog.printNoPrefix("================================ parameter: %s"%startAction)
        self.taosdCommandStop(startAction,taosdCmdRun)


        startAction=" -E taosdCaseTmp/.env"
        tdLog.printNoPrefix("================================ parameter: %s"%startAction)
        os.system(" mkdir -p taosdCaseTmp/.env ") 
        os.system("echo \'TAOS_QUERY_POLICY=3\' > taosdCaseTmp/.env ")
        self.taosdCommandStop(startAction,taosdCmdRun)
        os.system(" rm -rf taosdCaseTmp/.env ") 

        startAction = " -V"
        tdLog.printNoPrefix("================================ parameter: %s"%startAction)
        self.taosdCommandExe(startAction,taosdCmdRun)

        startAction = " -k"
        tdLog.printNoPrefix("================================ parameter: %s"%startAction)
        self.taosdCommandExe(startAction,taosdCmdRun)
    
    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
