import pytest
import sys
import time
import random
import taos
from new_test_framework.utils import tdLog, tdSql, cluster, sc, clusterComCheck


class TestShowBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_show_basic(self):
        """Show Basic

        1. build cluster with 3 dnodes
        2. execute show vgroups commands

        Catalog:
            - Show

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-5 dmchen init

        """

        tdLog.info(f"=============== create database, stable, table")
        tdSql.execute(f"create database db1 vgroups 1 replica 1")
        tdSql.execute(f"use db1")

        tdSql.query(f"show vgroups")
        tdSql.checkRows(1)
        tdSql.checkData(0, 15, 1) 

        tdLog.info(f"=============== create database, stable, table")
        tdSql.execute(f"create database db2 vgroups 1 replica 2")
        tdSql.execute(f"use db2")

        tdSql.query(f"show vgroups")
        tdSql.checkRows(1)
        tdSql.checkData(0, 15, 1)

        tdLog.info(f"=============== create database, stable, table")
        tdSql.execute(f"create database db3 vgroups 1 replica 3")
        tdSql.execute(f"use db3")

        tdSql.query(f"show vgroups")
        tdSql.checkRows(1)
        tdSql.checkData(0, 15, 1) 
