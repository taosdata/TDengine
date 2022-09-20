from urllib.parse import uses_relative
import taos
import sys
import os
import time


from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.dnodes import TDDnodes
from util.dnodes import TDDnode
from util.cluster import *


class TDTestCase:
    def caseDescription(self):
        '''
        3.0 data compatibility test 
        case1: basedata version is 3.0.1.0
        '''
        return

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
                    buildPath = root[:len(root)-len("/build/bin")]
                    break
        return buildPath

    def getCfgPath(self):
        buildPath = self.getBuildPath()
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            cfgPath = buildPath + "/../community/sim/dnode1/cfg/"
        else:
            cfgPath = buildPath + "/../sim/dnode1/cfg/"

        return cfgPath

    def installTaosd(self,bPath,cPath):
        # os.system(f"rmtaos && mkdir -p {self.getBuildPath()}/build/lib/temp &&  mv {self.getBuildPath()}/build/lib/libtaos.so*  {self.getBuildPath()}/build/lib/temp/ ")
        # os.system(f" mv {bPath}/build  {bPath}/build_bak ")
        # os.system(f"mv {self.getBuildPath()}/build/lib/libtaos.so  {self.getBuildPath()}/build/lib/libtaos.so_bak ")
        # os.system(f"mv {self.getBuildPath()}/build/lib/libtaos.so.1  {self.getBuildPath()}/build/lib/libtaos.so.1_bak ")

        packagePath="/usr/local/src/"
        packageName="TDengine-server-3.0.1.0-Linux-x64.tar.gz"
        os.system(f"cd {packagePath} &&  tar xvf  TDengine-server-3.0.1.0-Linux-x64.tar.gz && cd TDengine-server-3.0.1.0 && ./install.sh  -e no  " )
        tdDnodes.stop(1)
        print(f"start taosd: nohup taosd -c {cPath} & ")
        os.system(f" nohup taosd -c {cPath} & " )
        sleep(1)


        
    def buildTaosd(self,bPath):
        # os.system(f"mv {bPath}/build_bak  {bPath}/build ")
        os.system(f" cd {bPath}  &&  make install ")


    def run(self):
        print(f"buildpath:{self.getBuildPath()}")
        bPath=self.getBuildPath()
        cPath=self.getCfgPath()
        dbname = "test"
        stb = f"{dbname}.meters"
        self.installTaosd(bPath,cPath)
        tableNumbers=100
        recordNumbers1=100
        recordNumbers2=1000
        # print(tdSql)
        tdsqlF=tdCom.newTdSql()
        print(tdsqlF)
        tdsqlF.query(f"SELECT SERVER_VERSION();")
        print(tdsqlF.query(f"SELECT SERVER_VERSION();"))
        oldServerVersion=tdsqlF.queryResult[0][0]
        tdLog.info(f"Base server version is {oldServerVersion}")
        tdsqlF.query(f"SELECT CLIENT_VERSION();")
        # the oldClientVersion can't be updated in the same python process,so the version is new compiled verison
        oldClientVersion=tdsqlF.queryResult[0][0]
        tdLog.info(f"Base client version is {oldClientVersion}")

        tdLog.printNoPrefix(f"==========step1:prepare and check data in old version-{oldServerVersion}")
        tdLog.info(f"taosBenchmark -t {tableNumbers} -n {recordNumbers1} -y  ")
        os.system(f"taosBenchmark -t {tableNumbers} -n {recordNumbers1} -y  ")
        sleep(3)

        # tdsqlF.query(f"select count(*) from {stb}")
        # tdsqlF.checkData(0,0,tableNumbers*recordNumbers1)
        os.system("pkill taosd")
        sleep(1)

        tdLog.printNoPrefix("==========step2:update new version ")
        self.buildTaosd(bPath)
        tdDnodes.start(1)
        sleep(1)
        tdsql_new=tdCom.newTdSql()
        print(tdsql_new)


        tdsql_new.query(f"SELECT SERVER_VERSION();")
        nowServerVersion=tdsql_new.queryResult[0][0]
        tdLog.info(f"New server version is {nowServerVersion}")
        tdsql_new.query(f"SELECT CLIENT_VERSION();")
        nowClientVersion=tdsql_new.queryResult[0][0]
        tdLog.info(f"New client version is {nowClientVersion}")

        tdLog.printNoPrefix(f"==========step3:prepare and check data in new version-{nowServerVersion}")
        tdsql_new.query(f"select count(*) from {stb}")
        tdsql_new.checkData(0,0,tableNumbers*recordNumbers1)        
        os.system(f"taosBenchmark -t {tableNumbers} -n {recordNumbers2} -y  ")
        tdsql_new.query(f"select count(*) from {stb}")
        tdsql_new.checkData(0,0,tableNumbers*recordNumbers2)


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
