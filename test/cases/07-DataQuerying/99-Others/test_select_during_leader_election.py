from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck
import os

class TestSelectDuringLeaderElection:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_select_during_leader_election(self):
        """Leader Election Select Test

        1.Create 3 dnodes
        2.Create db with 10 vgroups 3 replications
        2.Create supper table and sub table
        3.Stop one of the dnodes with leader vnode on it
        4.Select data from super table, it should be success
    
        Catalog:
            - Select

        Since: v3.0.0.0

        Labels: common,ci

        Jira: TS-7003

        History:
            - 2025-8-15 Ethan liu adds test for select during leader election
        """

        tdLog.info(f"========== start select during leader election test")
        clusterComCheck.checkDnodes(3)

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(3)
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "ready")

        tdSql.execute(f"drop database if exists test_select_leader_election")
        tdSql.execute(f"create database test_select_leader_election vgroups 10 replica 3")
        tdSql.execute(f"use test_select_leader_election")

        # create super table and sub table
        tdSql.execute(f"create table super_t (ts timestamp, flag int) tags (t1 VARCHAR(10))")

        # create sub table
        tdLog.info(f"begin create sub table")
        for i in range(0,10,1):
            tdSql.execute(f"create table sub_t{i} using super_t tags('t{i}')")
        
        tdLog.info(f"finish create 10 sub table")
        
        # insert data to sub table
        tdLog.info(f"begin insert data")
        deta = 1
        for i in range(0,10,1):
            for j in range(0,100,1):
                deta += j
                tdSql.execute(f"insert into sub_t{i} values (now+{deta}s,{j})")

        tdLog.info(f"finish insert data")

        # stop one dnode
        os.system("ps -ef |grep taosd |grep -v 'grep' |sort |awk 'NR<=2'|awk '{print $2}'|xargs kill -2")

        # select data, it should be error since the leader election has not finsh
        tdSql.error(f"select last_row(*) from jddb.meters")

        tdLog.info(f"end select during leader election test successfully")