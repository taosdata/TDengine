from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck

class TestVtableShowTag:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_vtable_show_tag(self):
        """Vtable Show Tag Test

        1.Create db
        2.Create supper table and sub table
        3.Create virtual supper table and sub table
        4.Show tag of virtual table and check the result

        Catalog:
            - Query

        Since: v3.0.0.0

        Labels: common,ci

        Jira: TD-35567

        History:
            - 2025-7-12 Ethan liu adds test for test show tag on virtual super and sub table

        """

        tdLog.info(f"========== start virtual table show tag test")
        tdSql.execute(f"drop database if exists test_vdb_tag")
        tdSql.execute(f"create database test_vdb_tag")
        tdSql.execute(f"use test_vdb_tag")

        # create super table and sub table
        tdSql.execute(f"create table super_t (ts timestamp, flag int) tags (t1 VARCHAR(10))")
        tdSql.execute(f"create table sub_t0 using super_t tags('t1')")

        # create virtual super table and sub table
        tdSql.execute(f"create stable v_super_t (ts timestamp, flag int) tags (t1 VARCHAR(10)) virtual 1")
        tdSql.execute(f"create vtable v_sub_t0 (sub_t0.flag) using v_super_t TAGS (1)")

        # show tag of virtual table
        tdSql.execute(f"show tags from v_sub_t0")
        tdSql.checkRows(1)

        tdSql.execute(f"show tags from v_super_t")
        tdSql.checkRows(0)

        tdLog.info(f"end virtual table show tag test successfully")