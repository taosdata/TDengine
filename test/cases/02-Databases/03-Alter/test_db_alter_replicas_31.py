from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseAlterReplica13:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_alter_replica_31(self):
        """Alter replica from 3 to 1

        1. Create a database with 3 replica
        2. Create tables and insert data
        3. Flush the database
        4. Alter the replica count from 3 to 1
        5. Verify that all data remains intact

        Catalog:
            - Database:Alter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/db/alter_replica_31.sim

        """

        clusterComCheck.checkDnodes(4)
        tdSql.execute(f"alter dnode 1 'supportVnodes' '0'")
        clusterComCheck.checkDnodeSupportVnodes(1, 0)

        tdLog.info(f"=============== step2: create database")
        tdSql.execute(f"create database db vgroups 4 replica 3")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkKeyData("db", 4, 3)

        # vnodes
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkKeyData(2, 2, 4)
        tdSql.checkKeyData(3, 2, 4)
        tdSql.checkKeyData(4, 2, 4)

        clusterComCheck.checkDbReady("db")

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

        tdSql.query(f"show db.vgroups")
        tdLog.info(
            f"===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)} {tdSql.getData(0,7)} {tdSql.getData(0,8)}"
        )

        tdSql.query(f"select * from db.ctb0")
        tdSql.checkRows(1)

        tdSql.query(f"select * from db.ctb1")
        tdSql.checkRows(1)

        tdSql.query(f"select * from db.ctb2")
        tdSql.checkRows(1)

        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(10)

        tdLog.info(f"============= step3: alter database")
        tdSql.execute(f"alter database db replica 1")
        clusterComCheck.checkTransactions()

        tdSql.query(f"show db.vgroups")
        clusterComCheck.checkDbReady("db")

        tdLog.info(f"============= step5: stop dnode 2")
        tdSql.query(f"select * from db.ctb0")
        tdSql.checkRows(1)

        tdSql.query(f"select * from db.ctb1")
        tdSql.checkRows(1)

        tdSql.query(f"select * from db.ctb2")
        tdSql.checkRows(1)

        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(10)
