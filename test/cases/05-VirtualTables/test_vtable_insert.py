from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck

class TestVtableInsert:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_vtable_insert(self):
        """Vtable Insert Test

        1.Create db
        2.Create supper table and sub table
        3.Create virtual supper table and sub table
        4.Insert data into virtual super table and virtual sub table, it should be return error

        Catalog:
            - Insert

        Since: v3.0.0.0

        Labels: common,ci

        Jira: TD-34509

        History:
            - 2025-6-28 Ethan liu adds test for insert data to virtual table

        """

        tdLog.info(f"========== start virtual table insert test")
        tdSql.execute(f"drop database if exists test_vdb")
        tdSql.execute(f"create database test_vdb")
        tdSql.execute(f"use test_vdb")
        tdSql.execute(f"create table st (ts timestamp, flag int) tags (t1 VARCHAR(10))")
        tdSql.execute(f"create table tb using st tags('t1')")

        i = 10
        while i > 0:
            tdSql.execute(f"insert into tb values (now,{i})")
            i = i - 1

        tdSql.execute(f"CREATE STABLE `vst` (ts timestamp, flag int) TAGS (t1 VARCHAR(10)) VIRTUAL 1")
        tdSql.execute(f"CREATE VTABLE `sct0`(tb.flag) USING vst TAGS (1)")
        tdSql.execute(f"CREATE VTABLE `nsct0`(ts timestamp,flag int from tb.flag)")

        tdSql.error(f"insert into vst values(now, 1)")
        tdSql.error(f"insert into sct0 values(now, 1)")
        tdSql.error(f"insert into nsct0 values(now, 1)")


        tdLog.info(f"end virtual table insert test successfully")