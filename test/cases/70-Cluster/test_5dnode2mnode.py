from new_test_framework.utils import tdLog, tdSql, cluster, tdCom
import sys
import time
import os


sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from cluster_common_create import *
from cluster_common_check import *

class Test5dnode2mnode:
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")



    def five_dnode_two_mnode(self):
        tdSql.query("select * from information_schema.ins_dnodes;")
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(4,1,'%s:6430'%self.host)
        tdSql.checkData(0,4,'ready')
        tdSql.checkData(4,4,'ready')
        time.sleep(1)
        tdSql.query("select * from information_schema.ins_mnodes;")
        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(0,2,'leader')
        tdSql.checkData(0,3,'ready')


        # fisr add two mnodes;
        tdSql.error("create mnode on dnode 1;")
        tdSql.error("drop mnode on dnode 1;")

        tdSql.execute("create mnode on dnode 2")
        count=0
        while count < 10:
            time.sleep(1)
            tdSql.query("select * from information_schema.ins_mnodes;")
            tdSql.checkRows(2)
            if  tdSql.queryResult[0][2]=='leader' :
                if  tdSql.queryResult[1][2]=='follower':
                    print("two mnodes is ready")
                    break
            count+=1
        else:
            print("two mnodes is not ready in 10s ")

        # fisrt check statut ready

        tdSql.checkData(0,1,'%s:6030'%self.host)
        tdSql.checkData(0,2,'leader')
        tdSql.checkData(0,3,'ready')
        tdSql.checkData(1,1,'%s:6130'%self.host)
        tdSql.checkData(1,2,'follower')
        tdSql.checkData(1,3,'ready')

        # fisrt add data : db\stable\childtable\general table

        tdSql.execute("drop database if exists db2")
        tdSql.execute("create database if not exists db2 replica 1 duration 100")
        tdSql.execute("use db2")
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
        tdSql.error("create mnode on dnode 2")
        tdSql.query("select * from information_schema.ins_dnodes;")
        print(tdSql.queryResult)
        clusterComCheck.checkDnodes(5)
        # restart all taosd
        tdDnodes=cluster.dnodes

        # stop follower
        tdLog.info("stop follower")
        tdDnodes[1].stoptaosd()
        if cluster.checkConnectStatus(0) :
            print("cluster also work")

        # start follower
        tdLog.info("start follower")
        tdDnodes[1].starttaosd()
        if clusterComCheck.checkMnodeStatus(2) :
            print("both mnodes are ready")

        # stop leader
        tdLog.info("stop leader")
        tdDnodes[0].stoptaosd()
        try:
            cluster.checkConnectStatus(2)
            tdLog.notice(" The election  still  succeeds  when leader of both mnodes are killed ")
        except Exception:
            pass
        # tdSql.error("create user user1 pass '123';")
        tdLog.info("start leader")
        tdDnodes[0].starttaosd()
        if clusterComCheck.checkMnodeStatus(2) :
            print("both mnodes are ready")

    def test_5dnode2mnode(self):
        """Cluster 5 dnodes 2 mnode

        1. Create 5 node and 2 mnode cluster
        2. Ensure above cluster setup success
        3. Check mnode is leader and only 1 mnode
        4. Stop dnode 2 
        5. Start dnode 2
        6. Stop dnode 1
        7. Start dnode 1
        8. Check the cluster is alive

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-11-01 Alex Duan Migrated from uncatalog/system-test/6-cluster/test_5dnode2mnode.py

        """
        self.five_dnode_two_mnode()


        tdLog.success(f"{__file__} successfully executed")

