from ssl import ALERT_DESCRIPTION_CERTIFICATE_UNOBTAINABLE
import taos
import sys
import time
import os

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.dnodes import TDDnodes
from util.dnodes import TDDnode
import time
import socket
import subprocess

class MyDnodes(TDDnodes):
    def __init__(self ,dnodes_lists):
        super(MyDnodes,self).__init__()
        self.dnodes = dnodes_lists  # dnode must be TDDnode instance
        if platform.system().lower() == 'windows':
            self.simDeployed = True
        else:
            self.simDeployed = False

class TDTestCase:
    noConn = True
    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to excute {__file__}")
        self.TDDnodes = None
        self.depoly_cluster(5)
        self.master_dnode = self.TDDnodes.dnodes[0]
        self.host=self.master_dnode.cfgDict["fqdn"]
        conn1 = taos.connect(self.master_dnode.cfgDict["fqdn"] , config=self.master_dnode.cfgDir)
        tdSql.init(conn1.cursor(), True)


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


    def depoly_cluster(self ,dnodes_nums):

        testCluster = False
        valgrind = 0
        hostname = socket.gethostname()
        dnodes = []
        start_port = 6030
        for num in range(1, dnodes_nums+1):
            dnode = TDDnode(num)
            dnode.addExtraCfg("firstEp", f"{hostname}:{start_port}")
            dnode.addExtraCfg("fqdn", f"{hostname}")
            dnode.addExtraCfg("serverPort", f"{start_port + (num-1)*100}")
            dnode.addExtraCfg("monitorFqdn", hostname)
            dnode.addExtraCfg("monitorPort", 7043)
            dnodes.append(dnode)

        self.TDDnodes = MyDnodes(dnodes)
        self.TDDnodes.init("")
        self.TDDnodes.setTestCluster(testCluster)
        self.TDDnodes.setValgrind(valgrind)

        self.TDDnodes.setAsan(tdDnodes.getAsan())
        self.TDDnodes.stopAll()
        for dnode in self.TDDnodes.dnodes:
            self.TDDnodes.deploy(dnode.index,{})

        for dnode in self.TDDnodes.dnodes:
            self.TDDnodes.starttaosd(dnode.index)

        # create cluster
        for dnode in self.TDDnodes.dnodes[1:]:
            # print(dnode.cfgDict)
            dnode_id = dnode.cfgDict["fqdn"] +  ":" +dnode.cfgDict["serverPort"]
            dnode_first_host = dnode.cfgDict["firstEp"].split(":")[0]
            dnode_first_port = dnode.cfgDict["firstEp"].split(":")[-1]
            cmd = f"{self.getBuildPath()}/build/bin/taos -h {dnode_first_host} -P {dnode_first_port} -s \"create dnode \\\"{dnode_id}\\\"\""
            print(cmd)
            os.system(cmd)

        time.sleep(2)
        tdLog.info(" create cluster done! ")

    def five_dnode_one_mnode(self):
        tdSql.query("select * from information_schema.ins_dnodes;")
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(4,1,'%s:6430'%self.host)
        tdSql.checkData(0,4,'ready')
        tdSql.checkData(4,4,'ready')
        tdSql.query("select * from information_schema.ins_mnodes;")
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(0,2,'leader')
        tdSql.checkData(0,3,'ready')


        tdSql.error("create mnode on dnode 1;")
        tdSql.error("drop mnode on dnode 1;")

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database if not exists db replica 1 duration 300")
        tdSql.execute("use db")
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

        tdSql.query('select * from information_schema.ins_databases;')
        tdSql.checkData(2,5,'on')
        tdSql.error("alter database db strict 'off'")
        # tdSql.execute('alter database db strict 'on'')
        # tdSql.query('select * from information_schema.ins_databases;')
        # tdSql.checkData(2,5,'on')

    def getConnection(self, dnode):
        host = dnode.cfgDict["fqdn"]
        port = dnode.cfgDict["serverPort"]
        config_dir = dnode.cfgDir
        return taos.connect(host=host, port=int(port), config=config_dir)

    def check_alive(self):
        # check cluster alive
        tdLog.printNoPrefix("======== test cluster alive: ")
        tdSql.checkDataLoop(0, 0, 1, "show cluster alive;", 20, 0.5)

        tdSql.query("show db.alive;")
        tdSql.checkData(0, 0, 1)

        # stop 3 dnode
        self.TDDnodes.stoptaosd(3)
        tdSql.checkDataLoop(0, 0, 2, "show cluster alive;", 20, 0.5)
        
        tdSql.query("show db.alive;")
        tdSql.checkData(0, 0, 2)

        # stop 2 dnode
        self.TDDnodes.stoptaosd(2)
        tdSql.checkDataLoop(0, 0, 0, "show cluster alive;", 20, 0.5)

        tdSql.query("show db.alive;")
        tdSql.checkData(0, 0, 0)
        

    def run(self):
        # print(self.master_dnode.cfgDict)
        self.five_dnode_one_mnode()
        # check cluster and db alive
        self.check_alive()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
