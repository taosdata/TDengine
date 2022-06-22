from ssl import ALERT_DESCRIPTION_CERTIFICATE_UNOBTAINABLE
import taos
import sys
import time
import os 
import  socket

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *

class ClusterDnodes(TDDnodes):
    """rewrite TDDnodes and make MyDdnodes as TDDnodes child class"""
    def __init__(self ,dnodes_lists):

        super(ClusterDnodes,self).__init__()
        self.dnodes = dnodes_lists  # dnode must be TDDnode instance
        self.simDeployed = False
        self.testCluster = False
        self.valgrind = 0
        self.killValgrind = 1
        self.independent = True
        self.dnodeNums = 5

    # def getTDDnodes(dnodeNums):

    #     return 


class ConfigureyCluster:
    """configure dnodes and return TDDnodes list, it can """

    def __init__(self):
        self.dnodes = None      
        self.dnodes_nums = 5
        self.independent = True
        self.start_port = 6030
        self.portStep = 100
    hostname1= socket.gethostname()

    def configure_cluster(self ,dnodes_nums=5,independent=True,start_port=6030,portStep=100,hostname="%s"%hostname1): 
        self.start_port=int(start_port)
        self.portStep=int(portStep)
        self.hostname=hostname
        self.dnodes_nums = int(dnodes_nums)
        self.independent = independent
        self.dnodes = []
        start_port_sec = 6130
        for num in range(1, (self.dnodes_nums+1)):
            dnode = TDDnode(num)
            dnode.addExtraCfg("firstEp", f"{hostname}:{self.start_port}")
            dnode.addExtraCfg("fqdn", f"{hostname}")
            dnode.addExtraCfg("serverPort", f"{self.start_port + (num-1)*self.portStep}")
            # dnode.addExtraCfg("monitorFqdn", hostname)
            # dnode.addExtraCfg("monitorPort", 7043)
            dnode.addExtraCfg("secondEp", f"{hostname}:{start_port_sec}")
            # configure three dnoe don't support vnodes
            if self.dnodes_nums > 4 :
                if self.independent and (num < 4):
                    dnode.addExtraCfg("supportVnodes", 0)
            # print(dnode)
            self.dnodes.append(dnode)
        return self.dnodes
        
    def create_dnode(self,conn):
        tdSql.init(conn.cursor())
        for dnode in self.dnodes[1:]:
            # print(dnode.cfgDict)
            dnode_id = dnode.cfgDict["fqdn"] +  ":" +dnode.cfgDict["serverPort"]
            print(dnode_id)
            tdSql.execute(" create dnode '%s';"%dnode_id)
        # count=0
        # while count < 10:
        #     time.sleep(1)
        #     tdSql.query("show dnodes;")
        #     if tdSql.checkRows(self.dnodes_nums) :
        #         print("mnode is  three nodes")
        #     if  tdSql.queryResult[0][4]=='leader' :
        #         if  tdSql.queryResult[2][4]=='offline':
        #             if  tdSql.queryResult[1][2]=='follower':
        #                 print("stop mnodes  on dnode 3 successfully in 10s")
        #                 break
        #     count+=1
        # else:
        #     print("stop mnodes  on dnode 3 failed in 10s")
        #     return -1
        checkstatus=False

        
        
    def check_dnode(self,conn):
        tdSql.init(conn.cursor())
        count=0
        while count < 5:
            tdSql.query("show dnodes")
            # tdLog.debug(tdSql.queryResult)
            status=0
            for i in range(self.dnodes_nums):
                if tdSql.queryResult[i][4] == "ready":
                    status+=1
            tdLog.debug(status)
            
            if status == self.dnodes_nums:
                tdLog.debug(" create cluster with %d dnode and check cluster dnode all ready within 5s! " %self.dnodes_nums)
                break 
            count+=1
            time.sleep(1)
        else:
            tdLog.debug("create cluster with %d dnode but  check dnode not ready within 5s ! "%self.dnodes_nums)
            return -1


cluster = ConfigureyCluster()

    # def start(self ,dnodes_nums): 

    #     self.TDDnodes = ClusterDnodes(dnodes)
    #     self.TDDnodes.init("")
    #     self.TDDnodes.setTestCluster(testCluster)
    #     self.TDDnodes.setValgrind(valgrind)
    #     self.TDDnodes.stopAll()
    #     for dnode in self.TDDnodes.dnodes:
    #         self.TDDnodes.deploy(dnode.index,{})
            
    #     for dnode in self.TDDnodes.dnodes:
    #         self.TDDnodes.starttaosd(dnode.index)

    #     # create cluster 
    #     for dnode in self.TDDnodes.dnodes[1:]:
    #         # print(dnode.cfgDict)
    #         dnode_id = dnode.cfgDict["fqdn"] +  ":" +dnode.cfgDict["serverPort"]
    #         dnode_first_host = dnode.cfgDict["firstEp"].split(":")[0]
    #         dnode_first_port = dnode.cfgDict["firstEp"].split(":")[-1]
    #         cmd = f" taos -h {dnode_first_host} -P {dnode_first_port} -s ' create dnode \"{dnode_id} \" ' ;"
    #         print(cmd)
    #         os.system(cmd)
        
    #     time.sleep(2)
    #     tdLog.info(" create cluster with %d dnode  done! " %dnodes_nums)

    # def buildcluster(self,dnodenumber):
    #     self.depoly_cluster(dnodenumber)
    #     self.master_dnode = self.TDDnodes.dnodes[0]
    #     self.host=self.master_dnode.cfgDict["fqdn"]
    #     conn1 = taos.connect(self.master_dnode.cfgDict["fqdn"] , config=self.master_dnode.cfgDir)
    #     tdSql.init(conn1.cursor())
        
    # def checkdnodes(self,dnodenumber):
    #     count=0
    #     while count < 10:
    #         time.sleep(1)
    #         statusReadyBumber=0
    #         tdSql.query("show dnodes;")
    #         if tdSql.checkRows(dnodenumber) :     
    #             print("dnode is %d nodes"%dnodenumber)   
    #         for i in range(dnodenumber):
    #             if tdSql.queryResult[i][4] !='ready'  :
    #                 status=tdSql.queryResult[i][4]
    #                 print("dnode:%d status is %s "%(i,status))
    #                 break
    #             else:
    #                 statusReadyBumber+=1
    #         print(statusReadyBumber)
    #         if statusReadyBumber == dnodenumber :
    #             print("all of %d mnodes is ready in 10s "%dnodenumber)
    #             return True
    #             break
    #         count+=1
    #     else:
    #         print("%d mnodes is not ready in 10s "%dnodenumber)
    #         return False
           

    # def check3mnode(self):
    #     count=0
    #     while count < 10:
    #         time.sleep(1)
    #         tdSql.query("show mnodes;")
    #         if tdSql.checkRows(3) :     
    #             print("mnode is  three nodes")           
    #         if  tdSql.queryResult[0][2]=='leader' :
    #             if  tdSql.queryResult[1][2]=='follower':
    #                 if  tdSql.queryResult[2][2]=='follower':
    #                     print("three mnodes is ready in 10s")
    #                     break
    #         elif tdSql.queryResult[0][2]=='follower' :
    #             if  tdSql.queryResult[1][2]=='leader':
    #                 if  tdSql.queryResult[2][2]=='follower':
    #                     print("three mnodes is ready in 10s")
    #                     break      
    #         elif tdSql.queryResult[0][2]=='follower' :
    #             if  tdSql.queryResult[1][2]=='follower':
    #                 if  tdSql.queryResult[2][2]=='leader':
    #                     print("three mnodes is ready in 10s")
    #                     break                   
    #         count+=1
    #     else:
    #         print("three mnodes is not ready in 10s ")
    #         return -1

    #     tdSql.query("show mnodes;")       
    #     tdSql.checkRows(3) 
    #     tdSql.checkData(0,1,'%s:6030'%self.host)
    #     tdSql.checkData(0,3,'ready')
    #     tdSql.checkData(1,1,'%s:6130'%self.host)
    #     tdSql.checkData(1,3,'ready')
    #     tdSql.checkData(2,1,'%s:6230'%self.host)
    #     tdSql.checkData(2,3,'ready')

    # def check3mnode1off(self):
    #     count=0
    #     while count < 10:
    #         time.sleep(1)
    #         tdSql.query("show mnodes;")
    #         if tdSql.checkRows(3) :
    #             print("mnode is  three nodes")
    #         if  tdSql.queryResult[0][2]=='offline' :
    #             if  tdSql.queryResult[1][2]=='leader':
    #                 if  tdSql.queryResult[2][2]=='follower':
    #                     print("stop mnodes  on dnode 2 successfully in 10s")
    #                     break
    #             elif tdSql.queryResult[1][2]=='follower':
    #                 if  tdSql.queryResult[2][2]=='leader':
    #                     print("stop mnodes  on dnode 2 successfully in 10s")
    #                     break
    #         count+=1
    #     else:
    #         print("stop mnodes  on dnode 2 failed in 10s ")
    #         return -1
    #     tdSql.error("drop mnode on dnode 1;")

    #     tdSql.query("show mnodes;")       
    #     tdSql.checkRows(3) 
    #     tdSql.checkData(0,1,'%s:6030'%self.host)
    #     tdSql.checkData(0,2,'offline')
    #     tdSql.checkData(0,3,'ready')
    #     tdSql.checkData(1,1,'%s:6130'%self.host)
    #     tdSql.checkData(1,3,'ready')
    #     tdSql.checkData(2,1,'%s:6230'%self.host)
    #     tdSql.checkData(2,3,'ready')

    # def check3mnode2off(self):
    #     count=0
    #     while count < 40:
    #         time.sleep(1)
    #         tdSql.query("show mnodes;")
    #         if tdSql.checkRows(3) :
    #             print("mnode is  three nodes")
    #         if  tdSql.queryResult[0][2]=='leader' :
    #             if  tdSql.queryResult[1][2]=='offline':
    #                 if  tdSql.queryResult[2][2]=='follower':
    #                     print("stop mnodes  on dnode 2 successfully in 10s")
    #                     break
    #         count+=1
    #     else:
    #         print("stop mnodes  on dnode 2 failed in 10s ")
    #         return -1
    #     tdSql.error("drop mnode on dnode 2;")

    #     tdSql.query("show mnodes;")       
    #     tdSql.checkRows(3) 
    #     tdSql.checkData(0,1,'%s:6030'%self.host)
    #     tdSql.checkData(0,2,'leader')
    #     tdSql.checkData(0,3,'ready')
    #     tdSql.checkData(1,1,'%s:6130'%self.host)
    #     tdSql.checkData(1,2,'offline')
    #     tdSql.checkData(1,3,'ready')
    #     tdSql.checkData(2,1,'%s:6230'%self.host)
    #     tdSql.checkData(2,2,'follower')
    #     tdSql.checkData(2,3,'ready')

    # def check3mnode3off(self):
    #     count=0
    #     while count < 10:
    #         time.sleep(1)
    #         tdSql.query("show mnodes;")
    #         if tdSql.checkRows(3) :
    #             print("mnode is  three nodes")
    #         if  tdSql.queryResult[0][2]=='leader' :
    #             if  tdSql.queryResult[2][2]=='offline':
    #                 if  tdSql.queryResult[1][2]=='follower':
    #                     print("stop mnodes  on dnode 3 successfully in 10s")
    #                     break
    #         count+=1
    #     else:
    #         print("stop mnodes  on dnode 3 failed in 10s")
    #         return -1
    #     tdSql.error("drop mnode on dnode 3;")
    #     tdSql.query("show mnodes;")       
    #     tdSql.checkRows(3) 
    #     tdSql.checkData(0,1,'%s:6030'%self.host)
    #     tdSql.checkData(0,2,'leader')
    #     tdSql.checkData(0,3,'ready')
    #     tdSql.checkData(1,1,'%s:6130'%self.host)
    #     tdSql.checkData(1,2,'follower')
    #     tdSql.checkData(1,3,'ready')
    #     tdSql.checkData(2,1,'%s:6230'%self.host)
    #     tdSql.checkData(2,2,'offline')
    #     tdSql.checkData(2,3,'ready')

