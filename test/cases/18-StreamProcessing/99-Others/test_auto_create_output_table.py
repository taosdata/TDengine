import time
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

        Since: v3.4.2.0

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

    def test_auto_create_output_table_for_vtable_period(self):
        """summary: test nodelay output subtable creation for virtual table period stream

        description:
            - check period stream from virtual stable creates one output
              subtable per tbname partition when output_subtable uses tbname

        Since: v3.4.2.0

        Catalog:
            - StreamProcessing:Others

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7026933541

        History:
            - 2026-06-25 Created by regression

        """

        tdStream.ensureSnode()
        self.prepareVtablePeriodData()
        tdSql.execute("use db_vtable_period")
        tdSql.execute(
            "create stream s_vtable_period "
            "period(5s) "
            "from vst_period partition by tbname "
            "stream_options(ignore_disorder|ignore_nodata_trigger) "
            "into out_vtable_period nodelay_create_subtable "
            "output_subtable(concat('out_', tbname, '_period')) "
            "as select cast(_tlocaltime / 1000000 as timestamp) as ts, "
            "max(current) as current from %%tbname "
            "where _c0 >= now() - 10s and _c0 < now();"
        )

        tdStream.checkStreamStatus("s_vtable_period")
        tdSql.checkResultsByFunc(
            sql="select tags tbname from out_vtable_period order by tbname;",
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "out_vt_d6_ajrj26_period")
            and tdSql.compareData(1, 0, "out_vt_d9_q5ox8n_period"),
            retry=60,
        )

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
            "create table out_normal_exists (`ts` timestamp, `c1` int);",
            "create table tb_decimal (`ts` timestamp, `c1` decimal(10, 2));",
            "create table out_decimal_exists (`ts` timestamp, `c1` decimal(10, 2));",
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create successfully.")

    def prepareVtablePeriodData(self):
        tdLog.info(f"prepare virtual table period stream data")

        sqls = [
            "drop database if exists db_vtable_period;",
            "create database db_vtable_period vgroups 8;",
            "use db_vtable_period;",
            "create table meters (`ts` timestamp, `current` float, `phase` float, `voltage` int) tags(`t1` int);",
            "create table d6 using meters tags (6);",
            "create table d9 using meters tags (9);",
            "insert into d6 values (now - 5s, 10.0, 1.0, 220);",
            "insert into d9 values (now - 5s, 20.0, 2.0, 221);",
            "create stable vst_period (`ts` timestamp, `current` float, `phase` float, `voltage` int) "
            "tags(`element` varchar(256)) virtual 1;",
            "create vtable vt_d6_ajrj26 (`current` from d6.current, `phase` from d6.phase, `voltage` from d6.voltage) "
            "using vst_period tags ('d6');",
            "create vtable vt_d9_q5ox8n (`current` from d9.current, `phase` from d9.phase, `voltage` from d9.voltage) "
            "using vst_period tags ('d9');",
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create virtual table period data successfully.")

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

        sql1 ="create stream s10 count_window(1) from tb1 into out_normal NODELAY_CREATE_SUBTABLE as select * from tb1 where c1 > 10000;"
        sql2 = "create stream s11 state_window(c1) from tb1 into out_normal_2 NODELAY_CREATE_SUBTABLE as select * from tb1 where c1 > 10000;"
        sql3 = "create stream s12 state_window(c1) from tb1 into out_normal_exists NODELAY_CREATE_SUBTABLE as select * from tb1 where c1 > 10000;"
        sql4 = "create stream s13 count_window(1) from tb_decimal into out_decimal_exists NODELAY_CREATE_SUBTABLE as select * from tb_decimal where c1 > 0;"

        streams = [
            self.StreamItem(sql1, self.checks10),
            self.StreamItem(sql2, self.checks11),
            self.StreamItem(sql3, self.checks12),
            self.StreamItem(sql4, self.checks13),
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

    def checks13(self):
        result_sql = f"select * from information_schema.ins_tables where table_name like 'out_decimal_exists';"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "out_decimal_exists")
        )
        result_sql = f"select * from out_decimal_exists;"
        res_tbl_num = tdSql.query(result_sql)
        if res_tbl_num != 0:
            tdLog.exit(f"check_auto_create_out_ntb fail to exit[res_tbl_num: {res_tbl_num}]")
    
    def checks11(self):
        result_sql = f"select * from information_schema.ins_tables where table_name like 'out_normal_2';"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "out_normal_2")
        )
        result_sql = f"select * from out_normal_2;"
        res_tbl_num = tdSql.query(result_sql)
        if res_tbl_num != 0:
            tdLog.exit(f"check_auto_create_out_ntb fail to exit[res_tbl_num: {res_tbl_num}]")

    def checks12(self):
        result_sql = f"select * from information_schema.ins_tables where table_name like 'out_normal_exists';"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "out_normal_exists")
        )
        result_sql = f"select * from out_normal_exists;"
        res_tbl_num = tdSql.query(result_sql)
        if res_tbl_num != 0:
            tdLog.exit(f"check_auto_create_out_ntb fail to exit[res_tbl_num: {res_tbl_num}]")
    
    def insertDataAndCheck(self):
        tdLog.info(f"insert data and check")
        sqls = [
            "insert into tb1 values ('2025-01-01 00:00:00', 10001);",
            "insert into tb2 values ('2025-01-01 00:00:01', 10002);",
            "insert into tb_decimal values ('2025-01-01 00:00:00', 100.12);",
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

        tdSql.execute("insert into tb1 values ('2025-01-01 00:00:02', 10003);")
        time.sleep(5)

        result_sql = f"select * from out_normal_2 order by ts;"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
            and tdSql.compareData(0, 1, "10001")
            and tdSql.compareData(1, 0, "2025-01-01 00:00:02")
            and tdSql.compareData(1, 1, "10003")
        )
        result_sql = f"select * from out_normal_exists order by ts;"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
            and tdSql.compareData(0, 1, "10001")
            and tdSql.compareData(1, 0, "2025-01-01 00:00:02")
            and tdSql.compareData(1, 1, "10003")
        )
        result_sql = f"select * from out_decimal_exists order by ts;"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
            and tdSql.compareData(0, 1, "100.12")
        )

    class StreamItem:
        def __init__(self, sql, checkfunc):
            self.sql = sql
            self.checkfunc = checkfunc

        def check(self):
            self.checkfunc()
