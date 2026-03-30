from new_test_framework.utils import tdSql, tdLog, tdStream, StreamItem
from new_test_framework.utils.eutil import findTaosdLog

class TestStreamAutoCreateOutputTable:

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")

    def test_auto_create_output_table(self):
        """summary: test auto create output table

        description:
            - check_auto_create_out_ctb:
                test auto create output ctable
            - check_auto_create_out_ntb:
                test auto create output ntable

        Since: v3.4.7.0

        Catalog:
            - StreamProcessing:Others

        Labels: common,ci

        Jira: ID-6490870739

        History:
            - 2025-03-04 Created by Peng Rongkun

        """

        tdStream.createSnode()
        self.prepareData()
        self.check_auto_create_out_ctb()
        self.check_auto_create_out_ntb()
        self.insertDataAndCheck()

    def prepareData(self):
        tdLog.info(f"prepare data")

        sqls = [
            "drop database if exists db;",
            "create database db vgroups 1;",
            "use db;",
            "create table stb (`ts` timestamp, `c1` int) tags(`t1` int);",
            "create table tb1 using stb tags (1);",
            "create table tb2 using stb tags (2);",
            "create table out_exists (`ts` timestamp, `c1` int, `t1` int) tags(`tag_tbname` varchar(128));",
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create successfully.")

    def check_auto_create_out_ctb(self):
        tdSql.execute(f"use db")

        sql1 ="create stream s1 count_window(1) from stb partition by tbname into out_ctb1 NODELAY_CREATE_SUBTABLE as select * from %%tbname where c1 > 10000;"
        sql2 ="create stream s2 count_window(1) from stb partition by tbname into out_ctb2 as select * from %%tbname where c1 > 10000;"
        sql3 ="create stream s3 count_window(1) from stb partition by tbname into out_ctb3 NODELAY_CREATE_SUBTABLE OUTPUT_SUBTABLE(CONCAT('out3_', tbname))tags (`nameoftbl` varchar(128) as tbname) as select * from %%tbname where c1 > 10000;"
        sql4 ="create stream s4 count_window(1) from stb partition by tbname,t1 into out_ctb4 NODELAY_CREATE_SUBTABLE OUTPUT_SUBTABLE(CONCAT('out4_', tbname))tags (`nameoftbl` varchar(128) as tbname, tagt1 int as t1) as select * from %%tbname where c1 > 10000;"
        sql5 ="create stream s5 count_window(1) from stb partition by tbname into out_exists NODELAY_CREATE_SUBTABLE OUTPUT_SUBTABLE(CONCAT('out_exists_', tbname)) as select * from %%tbname where c1 > 10000;"
        streams = [
            self.StreamItem(sql1, self.checks1),
            self.StreamItem(sql2, self.checks2),
            self.StreamItem(sql3, self.checks3),
            self.StreamItem(sql4, self.checks4),
            self.StreamItem(sql5, self.checks5)
        ]
        for stream in streams:
            tdSql.execute(stream.sql)
        tdStream.checkStreamStatus()
        for stream in streams:
            stream.check()

    def check_auto_create_out_ntb(self):
        tdSql.execute(f"use db")

        sql1 ="create stream s10 count_window(1) from tb1 into out_normal as select * from tb1 where c1 > 10000;"

        streams = [
            self.StreamItem(sql1, self.checks10)
        ]
        for stream in streams:
            tdSql.execute(stream.sql)
        tdStream.checkStreamStatus()
        for stream in streams:
            stream.check()

    def checks1(self):
        tdLog.info(f"start to check nodelay create output ctb")
        result_sql = f"select * from information_schema.ins_stables where stable_name like 'out_ctb1';"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "out_ctb1")
        )

        result_sql = f"select tags tag_tbname from out_ctb1 order by tag_tbname;"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "tb1")
            and tdSql.compareData(1, 0, "tb2")
        )

    def checks2(self):
        tdLog.info(f"start to check delay create output ctb")
        result_sql = f"select * from information_schema.ins_stables where stable_name like 'out_ctb2';"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "out_ctb2")
        )

        result_sql = f"select tags tag_tbname from out_ctb2 order by tag_tbname;"
        res_tbl_num = tdSql.query(result_sql)
        if res_tbl_num != 0:
            tdLog.exit(f"check_auto_create_out_ctb fail to exit[res_tbl_num: {res_tbl_num}]")

    def checks3(self):
        tdLog.info(f"start to check nodelay create output ctb with custom tbname and tag")
        result_sql = f"select * from information_schema.ins_stables where stable_name like 'out_ctb3';"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "out_ctb3")
        )

        result_sql = f"select tags tbname from out_ctb3 order by tbname;"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "out3_tb1")
            and tdSql.compareData(1, 0, "out3_tb2")
        )

        result_sql = f"select tags nameoftbl from out_ctb3 order by nameoftbl;"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "tb1")
            and tdSql.compareData(1, 0, "tb2")
        )

    def checks4(self):
        tdLog.info(f"start to check nodelay create output ctb with multiple tags")
        result_sql = f"select * from information_schema.ins_stables where stable_name like 'out_ctb4';"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "out_ctb4")
        )

        result_sql = f"select tags tbname,nameoftbl,tagt1 from out_ctb4 order by tbname;"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "out4_tb1")
            and tdSql.compareData(0, 1, "tb1")
            and tdSql.compareData(0, 2, "1")
            and tdSql.compareData(1, 0, "out4_tb2")
            and tdSql.compareData(1, 1, "tb2")
            and tdSql.compareData(1, 2, "2")
        )
    
    def checks5(self):
        tdLog.info(f"start to check nodelay create output ctb with exists table")
        result_sql = f"select tags tbname from out_exists order by tbname;"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "out_exists_tb1")
            and tdSql.compareData(1, 0, "out_exists_tb2")
        )

    def checks10(self):
        result_sql = f"select * from information_schema.ins_tables where table_name like 'out_normal';"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "out_normal")
        )

        result_sql = f"select * from out_normal;"
        res_tbl_num = tdSql.query(result_sql)
        if res_tbl_num != 0:
            tdLog.exit(f"check_auto_create_out_ntb fail to exit[res_tbl_num: {res_tbl_num}]")
    
    def insertDataAndCheck(self):
        tdLog.info(f"insert data and check")
        sqls = [
            "insert into tb1 values ('2025-01-01 00:00:00', 10001);",
            "insert into tb2 values ('2025-01-01 00:00:01', 10002);",
        ]
        tdSql.executes(sqls)
        tdLog.info(f"insert data successfully")
        tdLog.info(f"start to check data")
        result_sql = f"select c1,tag_tbname from out_ctb1 order by ts;"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "10001")
            and tdSql.compareData(0, 1, "tb1")
            and tdSql.compareData(1, 0, "10002")
            and tdSql.compareData(1, 1, "tb2")
        )
        result_sql = f"select c1,tag_tbname from out_ctb2 order by ts;"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "10001")
            and tdSql.compareData(0, 1, "tb1")
            and tdSql.compareData(1, 0, "10002")
            and tdSql.compareData(1, 1, "tb2")
        )
        result_sql = f"select c1,tbname,nameoftbl from out_ctb3 order by ts;"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "10001")
            and tdSql.compareData(0, 1, "out3_tb1")
            and tdSql.compareData(0, 2, "tb1")
            and tdSql.compareData(1, 0, "10002")
            and tdSql.compareData(1, 1, "out3_tb2")
            and tdSql.compareData(1, 2, "tb2")
        )
        result_sql = f"select c1,tbname,nameoftbl,tagt1 from out_ctb4 order by ts;"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "10001")
            and tdSql.compareData(0, 1, "out4_tb1")
            and tdSql.compareData(0, 2, "tb1")
            and tdSql.compareData(0, 3, "1")
            and tdSql.compareData(1, 0, "10002")
            and tdSql.compareData(1, 1, "out4_tb2")
            and tdSql.compareData(1, 2, "tb2")
            and tdSql.compareData(1, 3, "2")
        )
        result_sql = f"select * from out_normal order by ts;"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
            and tdSql.compareData(0, 1, "10001")
        )

    class StreamItem:
        def __init__(self, sql, checkfunc):
            self.sql = sql
            self.checkfunc = checkfunc

        def check(self):
            self.checkfunc()