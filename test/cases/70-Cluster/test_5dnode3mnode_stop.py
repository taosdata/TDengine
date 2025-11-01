from new_test_framework.utils import tdLog, tdSql, cluster
from ssl import ALERT_DESCRIPTION_CERTIFICATE_UNOBTAINABLE
import taos
import sys
import time
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from cluster_common_create import *
from cluster_common_check import *



class Test5dnode3mnodeStop:

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
        #cls.host = socket.gethostname()


    def fiveDnodeThreeMnode(self,dnodenumbers,mnodeNums,restartNumber):
        tdLog.printNoPrefix("======== test case 1: ")
        paraDict = {'dbName':     'db',
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
        dbNumbers = int(dnodenumbers * restartNumber)

        tdLog.info("first check dnode and mnode")
        tdSql.query("select * from information_schema.ins_dnodes;")
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(4,1,'%s:6430'%self.host)
        clusterComCheck.checkDnodes(dnodenumbers)
        
        #check mnode status
        tdLog.info("check mnode status")
        clusterComCheck.checkMnodeStatus(mnodeNums)

        # # fisr add three mnodes;
        # tdLog.info("check mnode status")
        # # tdSql.execute("create mnode on dnode 2")
        # clusterComCheck.checkMnodeStatus(2)
        # # tdSql.execute("create mnode on dnode 3")
        # clusterComCheck.checkMnodeStatus(3)

        # add some error operations and
        tdLog.info("Confirm the status of the dnode again")
        tdSql.error("create mnode on dnode 2")
        tdSql.query("select * from information_schema.ins_dnodes;")
        # print(tdSql.queryResult)
        clusterComCheck.checkDnodes(dnodenumbers)
        # restart all taosd
        tdDnodes=cluster.dnodes

        tdDnodes[1].stoptaosd()
        clusterComCheck.check3mnodeoff(2,3)
        tdDnodes[1].starttaosd()
        clusterComCheck.checkMnodeStatus(3)

        tdDnodes[2].stoptaosd()
        clusterComCheck.check3mnodeoff(3,3)
        tdDnodes[2].starttaosd()
        clusterComCheck.checkMnodeStatus(3)

        tdDnodes[0].stoptaosd()
        clusterComCheck.check3mnodeoff(1,3)
        tdDnodes[0].starttaosd()
        clusterComCheck.checkMnodeStatus(3)


    def test_5dnode3mnode_stop(self):
        """Cluster 5 dnodes 3 mnode stop

        1. Create 5 node and 3 mnode cluster
        2. Ensure above cluster setup success
        3. Stop dnode 2 check mnode have 3
        4. Start dnode 2
        5. Stop dnode 3 check mnode have 3
        6. Start dnode 3
        7. Stop dnode 1 check mnode have 3
        8. Start dnode 1

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-11-01 Alex Duan Migrated from uncatalog/system-test/6-cluster/test_5dnode2mnode_stop.py

        """
        # print(self.master_dnode.cfgDict)
        self.fiveDnodeThreeMnode(dnodenumbers=5,mnodeNums=3,restartNumber=1)

        tdLog.success(f"{__file__} successfully executed")

