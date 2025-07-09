from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNormalTableHash:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_hash(self):
        """create normal table (hash)

        1. create normal table
        2. insert data
        3. drop table
        4. show tables

        Catalog:
            - Table:NormalTable:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/table/hash.sim

        """

        tdSql.execute(f"create database d1 vgroups 2 table_prefix 3 table_suffix 2")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkKeyData("d1", 25, 3)
        tdSql.checkKeyData("d1", 26, 2)

        tdSql.execute(f"use d1;")
        tdSql.execute(f"create table st (ts timestamp, i int) tags (j int);")
        tdSql.execute(
            f"create table st_ct_1 using st tags(3) st_ct_2 using st tags(4) st_ct_3 using st tags(5) st_ct_4 using st tags(6) st_ct_5 using st tags(7)"
        )
        tdSql.execute(f"insert into st_ct_1 values(now+1s, 1)")
        tdSql.execute(f"insert into st_ct_1 values(now+2s, 2)")
        tdSql.execute(f"insert into st_ct_1 values(now+3s, 3)")
        tdSql.execute(f"insert into st_ct_2 values(now+1s, 1)")
        tdSql.execute(f"insert into st_ct_2 values(now+2s, 2)")
        tdSql.execute(f"insert into st_ct_2 values(now+3s, 3)")
        tdSql.execute(f"insert into st_ct_3 values(now+1s, 1)")
        tdSql.execute(f"insert into st_ct_3 values(now+2s, 2)")
        tdSql.execute(f"insert into st_ct_3 values(now+3s, 2)")
        tdSql.execute(f"insert into st_ct_4 values(now+1s, 1)")
        tdSql.execute(f"insert into st_ct_4 values(now+2s, 2)")
        tdSql.execute(f"insert into st_ct_4 values(now+3s, 2)")
        tdSql.execute(f"insert into st_ct_5 values(now+1s, 1)")
        tdSql.execute(f"insert into st_ct_5 values(now+2s, 2)")
        tdSql.execute(f"insert into st_ct_5 values(now+3s, 2)")

        # check query
        tdSql.query(f"select * from st")
        tdSql.checkRows(15)

        # check table vgroup
        tdSql.query(
            f"select *  from information_schema.ins_tables where db_name = 'd1'"
        )
        tdSql.checkKeyData("st_ct_1", 6, 2)
        tdSql.checkKeyData("st_ct_2", 6, 2)
        tdSql.checkKeyData("st_ct_3", 6, 2)
        tdSql.checkKeyData("st_ct_4", 6, 2)
        tdSql.checkKeyData("st_ct_5", 6, 2)

        # check invalid table name
        tdSql.execute(f"create table c1 using st tags(3)")
        tdSql.execute(f"create table c12 using st tags(3)")
        tdSql.execute(f"create table c123 using st tags(3)")
        tdSql.execute(f"create table c1234 using st tags(3)")
        tdSql.execute(f"create table c12345 using st tags(3)")
        tdSql.query(
            f"select *  from information_schema.ins_tables where db_name = 'd1'"
        )

        tdSql.checkKeyData("c1", 6, 2)
        tdSql.checkKeyData("c12", 6, 3)
        tdSql.checkKeyData("c123", 6, 2)
        tdSql.checkKeyData("c1234", 6, 3)
        tdSql.checkKeyData("c12345", 6, 3)
