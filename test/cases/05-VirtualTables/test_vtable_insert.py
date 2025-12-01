from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck

class TestVtableInsert:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_vtable_insert(self):
        """Insert: virtual table

        1. Create db
        2. Create supper table and sub table
        3. Create virtual supper table and sub table
        4. Create normal virtual table and normal table
        5. Insert data into virtual super table or virtual sub table or virtual normal table, it should be return error

        Catalog:
            - VirtualTable

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

        # create super table and sub table
        tdSql.execute(f"create table super_t (ts timestamp, flag int) tags (t1 VARCHAR(10))")
        tdSql.execute(f"create table sub_t0 using super_t tags('t1')")

        # create normal table and insert data
        tdSql.execute(f"create table normal_t (ts timestamp, flag int)")

        i = 10
        while i > 0:
            tdSql.execute(f"insert into sub_t0 values (now,{i})")
            tdSql.execute(f"insert into normal_t values (now,{i})")
            i = i - 1

        # create virtual super table and sub table
        tdSql.execute(f"create stable v_super_t (ts timestamp, flag int) tags (t1 VARCHAR(10)) virtual 1")
        tdSql.execute(f"create vtable v_sub_t0 (sub_t0.flag) using v_super_t TAGS (1)")

        # create normal virtual table
        tdSql.execute(f"CREATE VTABLE v_normal_t (ts timestamp,flag int from sub_t0.flag)")

        # insert data into virtual super table and sub table, it should be return error

        # single table insert
        tdSql.error(f"insert into v_super_t values(now, 1)")
        tdSql.error(f"insert into v_sub_t0 values(now, 1)")
        tdSql.error(f"insert into v_normal_t values(now, 1)")

        # multiple table insert
        tdSql.error(f"insert into normal_t values(now, 1) v_sub_t0 values(now, 1)")
        tdSql.error(f"insert into v_sub_t0 values(now, 1) normal_t values(now, 1)")
        tdSql.error(f"insert into normal_t values(now, 1) v_super_t values(now, 1)")
        tdSql.error(f"insert into v_super_t values(now, 1) normal_t values(now, 1)")
        tdSql.error(f"insert into v_normal_t values(now, 1) normal_t values(now, 1)")
        tdSql.error(f"insert into normal_t values(now, 1) v_normal_t values(now, 1)")
        tdSql.error(f"insert into normal_t values(now, 1) v_sub_t0 values(now, 1) v_super_t values(now, 1)")
        tdSql.error(f"insert into normal_t values(now, 1) v_sub_t0 values(now, 1) v_super_t values(now, 1)")
        tdSql.error(f"insert into v_sub_t0 values(now, 1) normal_t values(now, 1) v_normal_t values(now, 1)")
        tdSql.error(f"insert into v_sub_t1 using v_super_t tags (2) values(now, 1)")
        tdSql.error(f"insert into v_super_t(tbname, t1, ts, flag) values ('v_sub_t0','t1',now, 1)")

        tdLog.info(f"end virtual table insert test successfully")