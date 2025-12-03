import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestMnodeBasic2:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_mnode_basic2(self):
        """Mnode stop all then start

        1. Create an mnode on the dnode2
        2. Create a user (update mnode)
        3. Restart dnode1 and dnode2
        4. Check if the user exists

        Catalog:
            - ManageNodes:Mnode

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/mnode/basic2.sim

        """

        clusterComCheck.checkDnodes(2)
        tdSql.query(f"select * from information_schema.ins_mnodes")
        tdSql.checkRows(1)
        tdSql.checkKeyData(1, 0, 1)
        tdSql.checkKeyData(1, 2, "leader")

        tdLog.info(f"=============== create mnode 2")
        tdSql.execute(f"create mnode on dnode 2")

        checkFailed = True
        for i in range(20):
            time.sleep(1)
            tdSql.query(f"select * from information_schema.ins_mnodes")
            tdSql.checkRows(2)
            tdSql.checkKeyData(1, 0, 1)
            tdSql.checkKeyData(1, 2, "leader")
            tdSql.checkKeyData(2, 0, 2)
            if tdSql.expectKeyData(2, 2, "follower"):
                checkFailed = False
                break
        if checkFailed:
            tdSql.checkAssert(False)

        tdLog.info(f"=============== create user")
        tdSql.execute(f"create user user1 PASS 'user1@#xy'")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(2)

        tdSql.execute(f"create database db")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdLog.info(f"=============== restart")
        sc.dnodeStop(1)
        sc.dnodeStop(2)
        sc.dnodeStart(1)
        sc.dnodeStart(2)
        clusterComCheck.checkDnodes(2)

        tdSql.query(f"select * from information_schema.ins_mnodes")
        tdSql.checkRows(2)

        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(2)

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdLog.info(f"=============== insert data")
        tdSql.execute(
            f'create table db.stb (ts timestamp, c1 int, c2 binary(4)) tags(t1 int, t2 float, t3 binary(16)) comment "abd"'
        )
        tdSql.execute(f'create table db.ctb using db.stb tags(101, 102, "103")')
        tdSql.execute(f'insert into db.ctb values(now, 1, "2")')

        tdSql.query(f"select * from db.ctb")
        tdSql.checkRows(1)
