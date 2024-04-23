from ssl import ALERT_DESCRIPTION_CERTIFICATE_UNOBTAINABLE
from paramiko import HostKeys
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
from multiprocessing import Process
class MyDnodes(TDDnodes):
    def __init__(self ,dnodes_lists):
        super(MyDnodes,self).__init__()
        self.dnodes = dnodes_lists  # dnode must be TDDnode instance
        if platform.system().lower() == 'windows':
            self.simDeployed = True
        else:
            self.simDeployed = False

class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to excute {__file__}")
        self.TDDnodes = None
        self.replicaVar =  int(replicaVar)

    def buildcluster(self,dnodenumber):
        self.depoly_cluster(dnodenumber)
        self.master_dnode = self.TDDnodes.dnodes[0]
        self.host=self.master_dnode.cfgDict["fqdn"]
        conn1 = taos.connect(self.master_dnode.cfgDict["fqdn"] , config=self.master_dnode.cfgDir)
        tdSql.init(conn1.cursor())

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

    def insert_data(self,count):
        # fisrt add data : db\stable\childtable\general table
        for couti in count:
            tdSql.execute("drop database if exists db%d" %couti)
            tdSql.execute("create database if not exists db%d replica 1 duration 300" %couti)
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

    def depoly_cluster(self ,dnodes_nums):

        testCluster = False
        valgrind = 0
        hostname = socket.gethostname()
        tdLog.debug(hostname)
        dnodes = []
        start_port = 6030
        start_port_sec = 6130
        for num in range(1, dnodes_nums+1):
            dnode = TDDnode(num)
            dnode.addExtraCfg("firstEp", f"{hostname}:{start_port}")
            dnode.addExtraCfg("fqdn", f"{hostname}")
            dnode.addExtraCfg("serverPort", f"{start_port + (num-1)*100}")
            dnode.addExtraCfg("monitorFqdn", hostname)
            dnode.addExtraCfg("monitorPort", 7043)
            dnode.addExtraCfg("secondEp", f"{hostname}:{start_port_sec}")
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
            # tdLog.debug(dnode.cfgDict)
            dnode_id = dnode.cfgDict["fqdn"] +  ":" +dnode.cfgDict["serverPort"]
            dnode_first_host = dnode.cfgDict["firstEp"].split(":")[0]
            dnode_first_port = dnode.cfgDict["firstEp"].split(":")[-1]
            cmd = f" taos -h {dnode_first_host} -P {dnode_first_port} -s ' create dnode \"{dnode_id} \" ' ;"
            if platform.system().lower() == 'windows':
                cmd = f" taos -h {dnode_first_host} -P {dnode_first_port} -s \"create dnode \\\"{dnode_id}\\\""
            tdLog.debug(cmd)
            if os.system(cmd) != 0:
                raise Exception("failed to execute system command. cmd: %s" % cmd)

        time.sleep(2)
        tdLog.info(" create cluster with %d dnode  done! " %dnodes_nums)

    def check3mnode(self):
        count=0
        while count < 10:
            time.sleep(0.1)
            tdSql.query("select * from information_schema.ins_mnodes;")
            if tdSql.checkRows(3) :
                tdLog.debug("mnode is  three nodes")
            if  tdSql.queryResult[0][2]=='leader' :
                if  tdSql.queryResult[1][2]=='follower':
                    if  tdSql.queryResult[2][2]=='follower':
                        tdLog.debug("three mnodes is ready in 10s")
                        break
            elif tdSql.queryResult[0][2]=='follower' :
                if  tdSql.queryResult[1][2]=='leader':
                    if  tdSql.queryResult[2][2]=='follower':
                        tdLog.debug("three mnodes is ready in 10s")
                        break
            elif tdSql.queryResult[0][2]=='follower' :
                if  tdSql.queryResult[1][2]=='follower':
                    if  tdSql.queryResult[2][2]=='leader':
                        tdLog.debug("three mnodes is ready in 10s")
                        break
            count+=1
        else:
            tdLog.debug(tdSql.queryResult)
            tdLog.debug("three mnodes is not ready in 10s ")
            return -1

        tdSql.query("select * from information_schema.ins_mnodes;")
        tdSql.checkRows(3)
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(0,3,'ready')
        tdSql.checkData(1,1,'%s:6130'%self.host)
        tdSql.checkData(1,3,'ready')
        tdSql.checkData(2,1,'%s:6230'%self.host)
        tdSql.checkData(2,3,'ready')

    def check3mnode1off(self):
        count=0
        while count < 10:
            time.sleep(0.1)
            tdSql.query("select * from information_schema.ins_mnodes;")
            if tdSql.checkRows(3) :
                tdLog.debug("mnode is  three nodes")
            if  tdSql.queryResult[0][2]=='offline' :
                if  tdSql.queryResult[1][2]=='leader':
                    if  tdSql.queryResult[2][2]=='follower':
                        tdLog.debug("stop mnodes  on dnode 2 successfully in 10s")
                        break
                elif tdSql.queryResult[1][2]=='follower':
                    if  tdSql.queryResult[2][2]=='leader':
                        tdLog.debug("stop mnodes  on dnode 2 successfully in 10s")
                        break
            count+=1
        else:
            tdLog.debug("stop mnodes  on dnode 2 failed in 10s ")
            return -1
        tdSql.error("drop mnode on dnode 1;")

        tdSql.query("select * from information_schema.ins_mnodes;")
        tdSql.checkRows(3)
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(0,2,'offline')
        tdSql.checkData(0,3,'ready')
        tdSql.checkData(1,1,'%s:6130'%self.host)
        tdSql.checkData(1,3,'ready')
        tdSql.checkData(2,1,'%s:6230'%self.host)
        tdSql.checkData(2,3,'ready')

    def check3mnode2off(self):
        count=0
        while count < 40:
            time.sleep(0.1)
            tdSql.query("select * from information_schema.ins_mnodes;")
            if tdSql.checkRows(3) :
                tdLog.debug("mnode is  three nodes")
            if  tdSql.queryResult[0][2]=='leader' :
                if  tdSql.queryResult[1][2]=='offline':
                    if  tdSql.queryResult[2][2]=='follower':
                        tdLog.debug("stop mnodes  on dnode 2 successfully in 10s")
                        break
            count+=1
        else:
            tdLog.debug("stop mnodes  on dnode 2 failed in 10s ")
            return -1
        tdSql.error("drop mnode on dnode 2;")

        tdSql.query("select * from information_schema.ins_mnodes;")
        tdSql.checkRows(3)
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(0,2,'leader')
        tdSql.checkData(0,3,'ready')
        tdSql.checkData(1,1,'%s:6130'%self.host)
        tdSql.checkData(1,2,'offline')
        tdSql.checkData(1,3,'ready')
        tdSql.checkData(2,1,'%s:6230'%self.host)
        tdSql.checkData(2,2,'follower')
        tdSql.checkData(2,3,'ready')

    def check3mnode3off(self):
        count=0
        while count < 10:
            time.sleep(0.1)
            tdSql.query("select * from information_schema.ins_mnodes;")
            if tdSql.checkRows(3) :
                tdLog.debug("mnode is  three nodes")
            if  tdSql.queryResult[0][2]=='leader' :
                if  tdSql.queryResult[2][2]=='offline':
                    if  tdSql.queryResult[1][2]=='follower':
                        tdLog.debug("stop mnodes  on dnode 3 successfully in 10s")
                        break
            count+=1
        else:
            tdLog.debug("stop mnodes  on dnode 3 failed in 10s")
            return -1
        tdSql.error("drop mnode on dnode 3;")
        tdSql.query("select * from information_schema.ins_mnodes;")
        tdSql.checkRows(3)
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(0,2,'leader')
        tdSql.checkData(0,3,'ready')
        tdSql.checkData(1,1,'%s:6130'%self.host)
        tdSql.checkData(1,2,'follower')
        tdSql.checkData(1,3,'ready')
        tdSql.checkData(2,1,'%s:6230'%self.host)
        tdSql.checkData(2,2,'offline')
        tdSql.checkData(2,3,'ready')



    def five_dnode_three_mnode(self):
        tdSql.query("select * from information_schema.ins_dnodes;")
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(4,1,'%s:6430'%self.host)
        tdSql.checkData(0,4,'ready')
        tdSql.checkData(1,4,'ready')
        tdSql.checkData(2,4,'ready')
        tdSql.checkData(3,4,'ready')
        tdSql.checkData(4,4,'ready')
        tdSql.query("select * from information_schema.ins_mnodes;")
        tdSql.checkRows(1)
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(0,2,'leader')
        tdSql.checkData(0,3,'ready')

        # fisr add three mnodes;
        tdSql.execute("create mnode on dnode 2")
        tdSql.execute("create mnode on dnode 3")

        # fisrt check statut ready
        self.check3mnode()
        tdSql.error("create mnode on dnode 2")
        tdSql.error("create mnode on dnode 1")
        tdSql.error("create mnode on dnode 3")

        tdSql.error("drop mnode on dnode 1")

        tdSql.query("select * from information_schema.ins_dnodes;")
        tdLog.debug(tdSql.queryResult)

        #  drop follower of mnode
        dropcount =0
        while dropcount <= 5:
            for i in range(1,3):
                tdLog.debug("drop mnode on dnode %d"%(i+1))
                tdSql.execute("drop mnode on dnode %d"%(i+1))
                tdSql.query("select * from information_schema.ins_mnodes;")
                count=0
                while count<10:
                    time.sleep(0.1)
                    tdSql.query("select * from information_schema.ins_mnodes;")
                    if tdSql.queryRows == 2:
                        tdLog.debug("drop mnode %d successfully"%(i+1))
                        break
                    count+=1
                self.wait_for_transactions(100)

                tdLog.debug("create mnode on dnode %d"%(i+1))
                tdSql.execute("create mnode on dnode %d"%(i+1))
                count=0
                while count<10:
                    time.sleep(0.1)
                    tdSql.query("select * from information_schema.ins_mnodes;")
                    if tdSql.queryRows == 3:
                        tdLog.debug("create mnode %d successfully"%(i+1))
                        break
                    count+=1
                self.wait_for_transactions(100)
            dropcount+=1
        self.check3mnode()

    def wait_for_transactions(self, timeout):
        count=0
        while count<timeout:
            time.sleep(1)
            tdSql.query("show transactions;")
            # print(tdSql.queryRows)
            if tdSql.queryRows == 0 :
                tdLog.debug("transactions completed successfully")
                break
            count+=1
        if count >= timeout:
            tdLog.debug("transactions not finished before timeout (%d secs)"%timeout)

    def getConnection(self, dnode):
        host = dnode.cfgDict["fqdn"]
        port = dnode.cfgDict["serverPort"]
        config_dir = dnode.cfgDir
        return taos.connect(host=host, port=int(port), config=config_dir)


    def run(self):
        # tdLog.debug(self.master_dnode.cfgDict)
        self.buildcluster(5)
        self.five_dnode_three_mnode()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
