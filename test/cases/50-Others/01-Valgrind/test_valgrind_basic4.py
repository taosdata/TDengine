import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestValgrindBasic4:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_valgrind_basic4(self):
        """valgrind basic 4

        1. -

        Catalog:
            - Others:Valgrind

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/valgrind/basic4.sim

        """

        tdLog.info(
            f"=============== step1: create drop select * from information_schema.ins_dnodes"
        )
        clusterComCheck.checkDnodes(1)

        tbPrefix = "tb"
        tbNum = 5
        rowNum = 10

        tdLog.info(f"=============== step2: prepare data")
        tdSql.execute(f"create database db vgroups 2")
        tdSql.execute(f"use db")
        tdSql.execute(
            f"create table if not exists stb (ts timestamp, tbcol int, tbcol2 float, tbcol3 double, tbcol4 binary(30), tbcol5 binary(30)) tags (tgcol int unsigned)"
        )

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using stb tags( {i} )")
            x = 0
            while x < rowNum:
                cc = x * 60000
                ms = 1601481600000 + cc
                tdSql.execute(
                    f'insert into {tb} values ({ms} , {x} , {x} , {x} , "abcd1234=-+*" , "123456 0" )'
                )
                x = x + 1
            cc = x * 60000
            ms = 1601481600000 + cc
            tdSql.execute(
                f"insert into {tb} values ({ms} , NULL , NULL , NULL , NULL , NULL )"
            )
            i = i + 1

        tdLog.info(f"=============== step3: tb")
        tdSql.query(f"select distinct(tbname), tgcol from stb;")
