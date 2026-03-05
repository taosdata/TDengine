from new_test_framework.utils import tdSql, tdLog, tdStream, StreamItem
from new_test_framework.utils.eutil import findTaosdLog

class TestStreamAutoCreateOutputTable:

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")

    def test_output_table_schema_validation(self):
        """Stream result match schema

        1. Verify error is raised when calculation result and output table column type do not match
        2. Verify error is raised when calculation result and output table column name do not match
        3. Verify error is raised when calculation result and output table Tag column type do not match
        4. Verify error is raised when calculation result and output table Tag column name do not match

        Since: v3.4.7.0

        Catalog:
            - Streams: 02-Stream

        Labels: common,ci

        Jira: ID-6490870739

        History:
            - 2025-03-04 Created by Peng Rongkun

        """

        # tdStream.createSnode()

        self.prepareData()
        self.check_auto_create_out_ctb()
        self.check_auto_create_out_ntb()

    def prepareData(self):
        tdLog.info(f"prepare data")

        sqls = [
            "drop database if exists db;",
            "create database db vgroups 1;",
            "use db;",
            "create table stb (ts timestamp, c1 int) tags(t1 int);",
            "create table tb1 using stb tags (1);",
            "create table tb2 using stb tags (2);",
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create successfully.")

    def check_auto_create_out_ctb(self):
        tdSql.execute(f"use db")

        sql1 ="create stream s1 state_window(c1) from stb partition by tbname into out1 NODELAY_CREATE_SUBTABLE as select * from %%tbname where c1 > 10000;"
        sql2 ="create stream s2 state_window(c1) from stb partition by tbname into out2 as select * from %%tbname where c1 > 10000;"

        streams = [
            self.StreamItem(sql1, self.checks1),
            self.StreamItem(sql2, self.checks2)
        ]
        for stream in streams:
            tdSql.execute(stream.sql)
        tdStream.checkStreamStatus()

    def check_auto_create_out_ntb(self):
        tdSql.execute(f"use db")

        sql1 ="create stream s3 state_window(c1) from tb1 into out3 as select * from tb1 where c1 > 10000;"

        streams = [
            self.StreamItem(sql1, self.checks3)
        ]
        for stream in streams:
            tdSql.execute(stream.sql)
        tdStream.checkStreamStatus()

    def checks1(self):
        result_sql = f"select * from information_schema.ins_stables where stable_name like 'out1';"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "out1")
        )

        result_sql = f"select tags tag_tbname from out1 order by tag_tbname;"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "tb1")
            and tdSql.compareData(1, 0, "tb2")
        )

    def checks2(self):
        result_sql = f"select * from information_schema.ins_stables where stable_name like 'out2';"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "out2")
        )

        result_sql = f"select tags tag_tbname from out2 order by tag_tbname;"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 0
        )

    def checks3(self):
        result_sql = f"select * from information_schema.ins_tables where stable_name like 'out3';"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "out3")
        )

        result_sql = f"select * from out3;"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 0
        )

    class StreamItem:
        def __init__(self, sql, checkfunc):
            self.sql = sql
            self.checkfunc = checkfunc

        def check(self):
            self.checkfunc()