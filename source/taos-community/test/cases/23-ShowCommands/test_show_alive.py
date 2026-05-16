import pytest
import sys
import time
import random
import taos
from new_test_framework.utils import tdLog, tdSql, cluster, sc, clusterComCheck


class TestShowAlive:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_show_alive(self):
        """Show alive

        Continuously start and stop multiple dnodes to verify the returned results of show alive.

        Catalog:
            - Show

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/show/showalive.sim

        """

        clusterComCheck.checkDnodes(4)
        clusterComCheck.checkClusterAlive(1, 20)

        tdLog.info(f"=============== create database, stable, table")
        tdSql.execute(f"create database test  vgroups 6;")
        tdSql.execute(f"use test;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(f"insert into t1 values(1648791211000,1,2,3);")
        tdSql.execute(f"insert into t1 values(1648791222001,2,2,3);")
        tdSql.execute(f"insert into t2 values(1648791211000,1,2,3);")
        tdSql.execute(f"insert into t2 values(1648791222001,2,2,3);")

        clusterComCheck.checkClusterAlive(1, 20)
        clusterComCheck.checkDbAlive("test", 1, 20)

        tdLog.info(f"stop dnode3")
        tdLog.info(f"stop dnode4")

        sc.dnodeForceStop(3)
        sc.dnodeForceStop(4)
        clusterComCheck.checkDnodes(2)

        clusterComCheck.checkClusterAlive(2, 20)
        clusterComCheck.checkDbAlive("test", 2, 20)

        tdSql.execute(f"create database test1 vgroups 2;")
        clusterComCheck.checkClusterAlive(2, 20)
        clusterComCheck.checkDbAlive("test", 2, 20)
        clusterComCheck.checkDbAlive("test1", 1, 20)

        tdLog.info(f"stop dnode2")
        sc.dnodeForceStop(2)
        clusterComCheck.checkDnodes(1)

        clusterComCheck.checkClusterAlive(2, 20)
        clusterComCheck.checkDbAlive("test", 2, 20)
        clusterComCheck.checkDbAlive("test1", 2, 20)
