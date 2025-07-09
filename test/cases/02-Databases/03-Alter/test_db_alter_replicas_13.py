import time

from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseAlterReplica13:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_alter_replica_13(self):
        """alter database replica 13

        1. -

        Catalog:
            - Database:Alter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/db/alter_replica_13.sim

        """

        clusterComCheck.checkDnodes(4)
        sc.dnodeForceStop(3)
        sc.dnodeForceStop(4)
        clusterComCheck.checkDnodes(2)
        tdSql.execute(f"alter dnode 1 'supportVnodes' '0'")
        clusterComCheck.checkDnodeSupportVnodes(1, 0)

        tdLog.info(f"=============== step2: create database")

        tdSql.execute(f"create database db vgroups 4")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkKeyData("db", 4, 1)

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkKeyData(2, 2, 4)

        tdSql.query(f"show db.vgroups")
        tdSql.checkKeyData(2, 3, 2)

        tdSql.error(f"alter database db replica 3")

        tdSql.execute(
            f'create table db.stb (ts timestamp, c1 int, c2 binary(4)) tags(t1 int, t2 binary(16)) comment "abd"'
        )
        tdSql.execute(f'create table db.ctb0 using db.stb tags(100, "100")')
        tdSql.execute(f'create table db.ctb1 using db.stb tags(101, "101")')
        tdSql.execute(f'create table db.ctb2 using db.stb tags(102, "102")')
        tdSql.execute(f'create table db.ctb3 using db.stb tags(103, "103")')
        tdSql.execute(f'create table db.ctb4 using db.stb tags(104, "104")')
        tdSql.execute(f'create table db.ctb5 using db.stb tags(105, "105")')
        tdSql.execute(f'create table db.ctb6 using db.stb tags(106, "106")')
        tdSql.execute(f'create table db.ctb7 using db.stb tags(107, "107")')
        tdSql.execute(f'create table db.ctb8 using db.stb tags(108, "108")')
        tdSql.execute(f'create table db.ctb9 using db.stb tags(109, "109")')
        tdSql.execute(f'insert into db.ctb0 values(now, 0, "0")')
        tdSql.execute(f'insert into db.ctb1 values(now, 1, "1")')
        tdSql.execute(f'insert into db.ctb2 values(now, 2, "2")')
        tdSql.execute(f'insert into db.ctb3 values(now, 3, "3")')
        tdSql.execute(f'insert into db.ctb4 values(now, 4, "4")')
        tdSql.execute(f'insert into db.ctb5 values(now, 5, "5")')
        tdSql.execute(f'insert into db.ctb6 values(now, 6, "6")')
        tdSql.execute(f'insert into db.ctb7 values(now, 7, "7")')
        tdSql.execute(f'insert into db.ctb8 values(now, 8, "8")')
        tdSql.execute(f'insert into db.ctb9 values(now, 9, "9")')
        tdSql.execute(f"flush database db;")

        tdLog.info(f"=============== step3: create dnodes")
        sc.dnodeStart(3)
        sc.dnodeStart(4)
        clusterComCheck.checkDnodes(4)

        tdLog.info(f"============= step4: alter database")
        tdSql.execute(f"alter database db replica 3")
        clusterComCheck.checkTransactions(300)

        tdSql.query(f"show db.vgroups")
        clusterComCheck.checkDbReady("db")

        tdLog.info(f"============= step5: check data")
        tdSql.query(f"select * from db.ctb0")
        tdSql.checkRows(1)

        tdSql.query(f"select * from db.ctb1")
        tdSql.checkRows(1)

        tdSql.query(f"select * from db.ctb2")
        tdSql.checkRows(1)

        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(10)
