from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestParserOp:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_parser_op(self):
        """Alter Stable

        1. -

        Catalog:
            - SuperTable:Alter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/parser/stableOp.sim

        """

        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "fi_in_db"
        tbPrefix = "fi_in_tb"
        stbPrefix = "fi_in_stb"
        mtPrefix = "fi_in_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"create_tb test")
        tdLog.info(f"=============== set up")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        # case1: create stable test
        tdLog.info(f"=========== stableOp.sim case1: create/alter/drop stable test")
        tdSql.execute(f"CREATE STABLE {stb} (TS TIMESTAMP, COL1 INT) TAGS (ID INT);")
        tdSql.query(f"show stables")

        tdSql.checkRows(1)

        tdLog.info(f"tdSql.getData(0,0) = {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, stb)

        tdSql.error(f"CREATE STABLE {tb} using {stb} tags (1);")

        tdSql.execute(f"create table {tb} using {stb} tags (2);")
        tdSql.query(f"show tables")

        tdSql.checkRows(1)

        tdSql.execute(f"alter stable {stb} add column COL2 DOUBLE;")

        tdSql.execute(f"insert into {tb} values (now, 1, 2.0);")

        tdSql.query(f"select * from {tb} ;")

        tdSql.checkRows(1)

        tdSql.execute(f"alter stable {stb} drop column COL2;")

        tdSql.error(f"insert into {tb} values (now, 1, 2.0);")

        tdSql.execute(f"alter stable {stb} add tag tag2 int;")

        tdSql.execute(f"alter stable {stb} rename tag tag2 tag3;")

        tdSql.error(f"drop stable {tb}")

        tdSql.execute(f"drop table {tb} ;")

        tdSql.query(f"show tables")

        tdSql.checkRows(0)

        tdSql.execute(f"DROP STABLE {stb}")
        tdSql.query(f"show stables")

        tdSql.checkRows(0)

        tdLog.info(f"create/alter/drop stable test passed")

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
