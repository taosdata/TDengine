from new_test_framework.utils import tdLog, tdSql, cluster, tdCom
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from cluster_common_create import *
from cluster_common_check import *


class Test5dnode3mnodeStopFollowerLeader:

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
        cls.TDDnodes = None


    def fiveDnodeThreeMnode(self,dnodenumbers,mnodeNums,restartNumber):
        tdLog.printNoPrefix("======== test case 1: ")
        paraDict = {'dbName':     'db0_0',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    4,
                    'replica':    1,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbNum':     1,
                    'rowsPerTbl': 10000,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  10,
                    'showMsg':    1,
                    'showRow':    1}
        dnodenumbers=int(dnodenumbers)
        mnodeNums=int(mnodeNums)
        dbNumbers = 1

        tdLog.info("first check dnode and mnode")
        tdSql.query("select * from information_schema.ins_dnodes;")
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(4,1,'%s:6430'%self.host)
        clusterComCheck.checkDnodes(dnodenumbers)
        
        #check mnode status
        tdLog.info("check mnode status")
        clusterComCheck.checkMnodeStatus(mnodeNums)
        
        # add some error operations and
        tdLog.info("Confirm the status of the dnode again")
        tdSql.error("create mnode on dnode 2")
        tdSql.query("select * from information_schema.ins_dnodes;")
        # print(tdSql.queryResult)
        clusterComCheck.checkDnodes(dnodenumbers)
        # restart all taosd
        tdDnodes=cluster.dnodes
        tdLog.info("stop two mnode ")

        tdDnodes[0].stoptaosd()
        tdDnodes[1].stoptaosd()

        # tdLog.info("check  whether 2 mnode status is  offline")
        # clusterComCheck.check3mnode2off()
        # tdSql.error("create user user1 pass '123';")

        tdLog.info("start one mnode" )
        tdDnodes[0].starttaosd()
        clusterComCheck.check3mnodeoff(2)

        clusterComCreate.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], paraDict["vgroups"],paraDict['replica'])
        clusterComCheck.checkDb(dbNumbers,1,'db0')



    def test_5dnode3mnode_stop_follower_leader(self):
        """Cluster 5 dnodes 3 mnode stop follower

        1. Create 5 node and 3 mnode cluster
        2. Ensure above cluster setup success
        3. Check mnode is leader and only 1 mnode
        4. Except check some error operations
        5. Stop dnode 0/1
        6. Start dnode 0
        7. Check mnode 2 status is offline
        8. Create some database 
        9. Check database created successfully

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-11-01 Alex Duan Migrated from uncatalog/system-test/6-cluster/test_5dnode3mnode_stop_follower_leader.py

        """
        # print(self.master_dnode.cfgDict)
        self.fiveDnodeThreeMnode(dnodenumbers=5,mnodeNums=3,restartNumber=1)

        tdLog.success(f"{__file__} successfully executed")

