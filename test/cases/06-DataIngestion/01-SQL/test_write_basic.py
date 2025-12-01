from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestWriteBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_write_basic(self):
        """Write basic

        1. Write data to a nanosecond-precision database
        2. Write data to regular tables and child tables
        3. Write data to specified columns
        4. Batch write multiple records to different child tables in a single operation

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-12 Simon Guan Migrated from tsim/insert/basic.sim
            - 2025-8-12 Simon Guan Migrated from tsim/insert/basic0.sim
            - 2025-8-12 Simon Guan Migrated from tsim/insert/basic1.sim
            - 2025-8-12 Simon Guan Migrated from tsim/insert/insert_stb.sim
            - 2025-8-12 Simon Guan Migrated from tsim/parser/insert_multiTbl.sim
            - 2025-8-12 Simon Guan Migrated from tsim/stable/values.sim
            
        """

        self.Basic()
        tdStream.dropAllStreamsAndDbs()
        self.Basic0()
        tdStream.dropAllStreamsAndDbs()
        self.Basic1()
        tdStream.dropAllStreamsAndDbs()
        self.InsertStb()
        tdStream.dropAllStreamsAndDbs()
        self.InsertMultiTb()
        tdStream.dropAllStreamsAndDbs()
        self.Values()
        tdStream.dropAllStreamsAndDbs()

    def Basic(self):
        i = 0
        dbPrefix = "d"
        tbPrefix = "t"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1")
        tdSql.execute(f"create database {db} vgroups 2 precision 'ns'")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, speed int)")

        x = 0
        while x < 110:
            cc = x * 60000
            ms = 1601481600000000000 + cc

            tdSql.execute(f"insert into {tb} values ({ms} , {x} )")
            x = x + 1

        tdLog.info(f"=============== step 2")
        x = 0
        while x < 110:
            cc = x * 60000
            ms = 1551481600000000000 + cc

            tdSql.execute(f"insert into {tb} values ({ms} , {x} )")
            x = x + 1

        tdSql.query(f"select * from {tb}")

        tdLog.info(f"{tdSql.getRows()}) points data are retrieved")
        tdSql.checkRows(220)


    def Basic0(self):
        tdLog.info(f"=============== create database")
        tdSql.prepare(dbname="d0")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.execute(f"use d0")

        tdLog.info(
            f"=============== create super table, include column type for count/sum/min/max/first"
        )
        tdSql.execute(
            f"create table if not exists stb (ts timestamp, c1 int, c2 float, c3 double) tags (t1 int unsigned)"
        )

        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== create child table")
        tdSql.execute(f"create table ct1 using stb tags(1000)")
        tdSql.execute(f"create table ct2 using stb tags(2000)")
        tdSql.execute(f"create table ct3 using stb tags(3000)")

        tdSql.query(f"show tables")
        tdSql.checkRows(3)

        tdLog.info(f"=============== insert data, mode1: one row one table in sql")
        tdLog.info(f"=============== insert data, mode1: mulit rows one table in sql")
        # print =============== insert data, mode1: one rows mulit table in sql
        # print =============== insert data, mode1: mulit rows mulit table in sql
        tdSql.execute(f"insert into ct1 values(now+0s, 10, 2.0, 3.0)")
        tdSql.execute(
            f"insert into ct1 values(now+1s, 11, 2.1, 3.1)(now+2s, -12, -2.2, -3.2)(now+3s, -13, -2.3, -3.3)"
        )
        tdSql.execute(f"insert into ct2 values(now+0s, 10, 2.0, 3.0)")
        tdSql.execute(
            f"insert into ct2 values(now+1s, 11, 2.1, 3.1)(now+2s, -12, -2.2, -3.2)(now+3s, -13, -2.3, -3.3)"
        )
        tdSql.execute(
            f"insert into ct3 values('2021-01-01 00:00:00.000', 10, 2.0, 3.0)"
        )

        # ===================================================================
        # ===================================================================
        tdLog.info(f"=============== query data from child table")
        tdSql.query(f"select * from ct1")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 2.00000)
        tdSql.checkData(0, 3, 3.000000000)

        tdLog.info(f"=============== select count(*) from child table")
        tdSql.query(f"select count(*) from ct1")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 4)

        tdLog.info(f"=============== select count(column) from child table")
        tdSql.query(f"select count(ts), count(c1), count(c2), count(c3) from ct1")
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(0, 3, 4)

        # print =============== select first(*)/first(column) from child table
        tdSql.query(f"select first(*) from ct1")
        tdLog.info(f"====> select first(*) from ct1")
        tdLog.info(f"rows: {tdSql.getRows()})")

        tdSql.query(f"select first(ts), first(c1), first(c2), first(c3) from ct1")
        tdLog.info(f"====> select first(ts), first(c1), first(c2), first(c3) from ct1")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 2.00000)
        tdSql.checkData(0, 3, 3.000000000)

        tdLog.info(f"=============== select min(column) from child table")
        tdSql.query(f"select min(c1), min(c2), min(c3) from ct1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, -13)
        tdSql.checkData(0, 1, -2.30000)
        tdSql.checkData(0, 2, -3.300000000)

        tdLog.info(f"=============== select max(column) from child table")
        tdSql.query(f"select max(c1), max(c2), max(c3) from ct1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 11)
        tdSql.checkData(0, 1, 2.10000)
        tdSql.checkData(0, 2, 3.100000000)

        tdLog.info(f"=============== select sum(column) from child table")
        tdSql.query(f"select sum(c1), sum(c2), sum(c3) from ct1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, -4)
        tdSql.checkData(0, 1, -0.400000095)
        tdSql.checkData(0, 2, -0.400000000)

        tdLog.info(f"=============== select column without timestamp, from child table")
        tdSql.query(f"select c1, c2, c3 from ct1")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(0, 1, 2.00000)
        tdSql.checkData(0, 2, 3.000000000)
        tdSql.checkData(1, 0, 11)
        tdSql.checkData(1, 1, 2.10000)
        tdSql.checkData(1, 2, 3.100000000)
        tdSql.checkData(3, 0, -13)
        tdSql.checkData(3, 1, -2.30000)
        tdSql.checkData(3, 2, -3.300000000)

        # ===================================================================

        # print =============== query data from stb
        tdSql.query(f"select * from stb")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(9)

        # print =============== select count(*) from supter table
        tdSql.query(f"select count(*) from stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 9)

        tdLog.info(f"=============== select count(column) from supter table")
        tdSql.query(f"select ts, c1, c2, c3 from stb")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkRows(9)

        # The order of data from different sub tables in the super table is random,
        # so this detection may fail randomly
        tdSql.checkData(0, 1, 10)

        tdSql.checkData(0, 2, 2.00000)

        tdSql.checkData(0, 3, 3.000000000)

        # print =============== select count(column) from supter table
        tdSql.query(f"select count(ts), count(c1), count(c2), count(c3) from stb")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkData(0, 0, 9)
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(0, 2, 9)
        tdSql.checkData(0, 3, 9)

        # ===================================================================
        tdLog.info(f"=============== stop and restart taosd, then again do query above")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=============== query data from child table")
        tdSql.query(f"select * from ct1")
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 2.00000)
        tdSql.checkData(0, 3, 3.000000000)

        tdLog.info(f"=============== select count(*) from child table")
        tdSql.query(f"select count(*) from ct1")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4)

        tdLog.info(f"=============== select count(column) from child table")
        tdSql.query(f"select count(ts), count(c1), count(c2), count(c3) from ct1")
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(0, 3, 4)

        # print =============== select first(*)/first(column) from child table
        tdSql.query(f"select first(*) from ct1")
        tdLog.info(f"====> select first(*) from ct1")
        tdLog.info(f"rows: {tdSql.getRows()})")

        tdSql.query(f"select first(ts), first(c1), first(c2), first(c3) from ct1")
        tdLog.info(f"====> select first(ts), first(c1), first(c2), first(c3) from ct1")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 2.00000)
        tdSql.checkData(0, 3, 3.000000000)

        tdLog.info(f"=============== select min(column) from child table")
        tdSql.query(f"select min(c1), min(c2), min(c3) from ct1")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, -13)
        tdSql.checkData(0, 1, -2.30000)
        tdSql.checkData(0, 2, -3.300000000)

        tdLog.info(f"=============== select max(column) from child table")
        tdSql.query(f"select max(c1), max(c2), max(c3) from ct1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 11)
        tdSql.checkData(0, 1, 2.10000)
        tdSql.checkData(0, 2, 3.100000000)

        tdLog.info(f"=============== select sum(column) from child table")
        tdSql.query(f"select sum(c1), sum(c2), sum(c3) from ct1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, -4)
        tdSql.checkData(0, 1, -0.400000095)
        tdSql.checkData(0, 2, -0.400000000)

        tdLog.info(f"=============== select column without timestamp, from child table")
        tdSql.query(f"select c1, c2, c3 from ct1")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(0, 1, 2.00000)
        tdSql.checkData(0, 2, 3.000000000)
        tdSql.checkData(1, 0, 11)
        tdSql.checkData(1, 1, 2.10000)
        tdSql.checkData(1, 2, 3.100000000)
        tdSql.checkData(3, 0, -13)
        tdSql.checkData(3, 1, -2.30000)
        tdSql.checkData(3, 2, -3.300000000)

        # ===================================================================
        tdLog.info(f"=============== query data from stb")
        tdSql.query(f"select * from stb")
        tdSql.checkRows(9)

        tdLog.info(f"=============== select count(*) from supter table")
        tdSql.query(f"select count(*) from stb")

        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 9)

        tdLog.info(f"=============== select count(column) from supter table")
        tdSql.query(f"select ts, c1, c2, c3 from stb")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkRows(9)

        # The order of data from different sub tables in the super table is random,
        # so this detection may fail randomly
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 2.00000)
        tdSql.checkData(0, 3, 3.000000000)

        # print =============== select count(column) from supter table
        tdSql.query(f"select count(ts), count(c1), count(c2), count(c3) from stb")
        tdSql.checkData(0, 0, 9)
        tdSql.checkData(0, 1, 9)
        tdSql.checkData(0, 2, 9)
        tdSql.checkData(0, 3, 9)

    def Basic1(self):
        tdLog.info(f"=============== create database")
        tdSql.prepare(dbname="d1")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}")

        tdSql.execute(f"use d1")

        tdLog.info(f"=============== create super table, include all type")
        tdSql.execute(
            f"create table if not exists stb (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint, c6 float, c7 double, c8 binary(16), c9 nchar(16), c10 timestamp, c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned) tags (t1 bool, t2 tinyint, t3 smallint, t4 int, t5 bigint, t6 float, t7 double, t8 binary(16), t9 nchar(16), t10 timestamp, t11 tinyint unsigned, t12 smallint unsigned, t13 int unsigned, t14 bigint unsigned)"
        )
        tdSql.execute(
            f"create stable if not exists stb_1 (ts timestamp, i int) tags (j int)"
        )
        tdSql.execute(f"create table stb_2 (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create stable stb_3 (ts timestamp, i int) tags (j int)")

        tdSql.query(f"show stables")
        tdSql.checkRows(4)

        tdLog.info(f"=============== create child table")
        tdSql.execute(
            f"create table c1 using stb tags(true, -1, -2, -3, -4, -6.0, -7.0, 'child tbl 1', 'child tbl 1', '2022-02-25 18:00:00.000', 10, 20, 30, 40)"
        )
        tdSql.execute(
            f"create table c2 using stb tags(false, -1, -2, -3, -4, -6.0, -7.0, 'child tbl 2', 'child tbl 2', '2022-02-25 18:00:00.000', 10, 20, 30, 40)"
        )

        tdSql.query(f"show tables")
        tdSql.checkRows(2)

        tdSql.execute(
            f"insert into c1 values(now-1s, true, -1, -2, -3, -4, -6.0, -7.0, 'child tbl 1', 'child tbl 1', '2022-02-25 18:00:00.000', 10, 20, 30, 40)"
        )
        tdSql.execute(
            f"insert into c1 values(now+0s, true, -1, -2, -3, -4, -6.0, -7.0, 'child tbl 1', 'child tbl 1', '2022-02-25 18:00:00.000', 10, 20, 30, 40) (now+1s, true, -1, -2, -3, -4, -6.0, -7.0, 'child tbl 1', 'child tbl 1', '2022-02-25 18:00:00.000', 10, 20, 30, 40) (now+2s, true, -1, -2, -3, -4, -6.0, -7.0, 'child tbl 1', 'child tbl 1', '2022-02-25 18:00:00.000', 10, 20, 30, 40)"
        )

        tdLog.info(f"=============== query data")
        tdSql.query(f"select * from c1")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdLog.info(f"{tdSql.getData(0,0)}  {tdSql.getData(0,1)}")
        tdLog.info(f"{tdSql.getData(1,0)}  {tdSql.getData(1,1)}")
        tdLog.info(f"{tdSql.getData(2,0)}  {tdSql.getData(2,1)}")
        tdLog.info(f"{tdSql.getData(3,0)}  {tdSql.getData(3,1)}")
        tdSql.checkRows(4)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, -1)

        tdSql.checkData(0, 3, -2)

        tdLog.info(
            f"=============== query data from st, but not support select * from super table, waiting fix"
        )
        tdSql.query(f"select * from stb")
        tdSql.checkRows(4)

        tdLog.info(f"=============== stop and restart taosd")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=============== query data")
        tdSql.query(f"select * from c1")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdLog.info(f"{tdSql.getData(0,0)}  {tdSql.getData(0,1)}")
        tdLog.info(f"{tdSql.getData(1,0)}  {tdSql.getData(1,1)}")
        tdLog.info(f"{tdSql.getData(2,0)}  {tdSql.getData(2,1)}")
        tdLog.info(f"{tdSql.getData(3,0)}  {tdSql.getData(3,1)}")
        tdSql.checkRows(4)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, -1)

        tdSql.checkData(0, 3, -2)

        tdLog.info(
            f"=============== query data from st, but not support select * from super table, waiting fix"
        )
        tdSql.query(f"select * from stb")
        tdSql.checkRows(4)

    def InsertStb(self):
        tdSql.execute(f"create database d1")
        tdSql.execute(f"create database d2")

        tdSql.execute(f"use d1;")
        tdSql.execute(f"create table st(ts timestamp, f int) tags(t int);")
        tdSql.execute(f"insert into ct1 using st tags(1) values('2021-04-19 00:00:00', 1);")
        tdSql.execute(f"insert into ct2 using st tags(2) values('2021-04-19 00:00:01', 2);")
        tdSql.execute(f"insert into ct1 values('2021-04-19 00:00:02', 2);")
        tdSql.execute(f"create table st2(ts timestamp, f int) tags(t int);")

        tdSql.execute(f"use d2;")
        tdSql.execute(f"create table st(ts timestamp, f int) tags(t int);")
        tdSql.execute(f"insert into ct1 using st tags(1) values('2021-04-19 00:00:00', 1);")
        tdSql.execute(f"insert into ct2 using st tags(2) values('2021-04-19 00:00:01', 2);")

        tdSql.execute(f"create database db1 vgroups 1;")
        tdSql.execute(f"create table db1.stb (ts timestamp, c1 int, c2 int) tags(t1 int, t2 int);")

        tdSql.execute(f"use d1;")
        tdSql.execute(f"insert into st (tbname, ts, f, t) values('ct3', '2021-04-19 08:00:03', 3, 3);")
        tdSql.execute(f"insert into d1.st (tbname, ts, f) values('ct6', '2021-04-19 08:00:04', 6);")
        tdSql.execute(f"insert into d1.st (tbname, ts, f) values('ct6', '2021-04-19 08:00:05', 7)('ct8', '2021-04-19 08:00:06', 8);")
        tdSql.execute(f"insert into d1.st (tbname, ts, f, t) values('ct6', '2021-04-19 08:00:07', 9, 9)('ct8', '2021-04-19 08:00:08', 10, 10);")
        tdSql.execute(f"insert into d1.st (tbname, ts, f, t) values('ct6', '2021-04-19 08:00:09', 9, 9)('ct8', '2021-04-19 08:00:10', 10, 10) d2.st (tbname, ts, f, t) values('ct6', '2021-04-19 08:00:11', 9, 9)('ct8', '2021-04-19 08:00:12', 10, 10);")

        tdSql.query(f"select * from d1.st")
        tdLog.info(f'{tdSql.getRows()})')
        tdSql.checkRows(11)

        tdSql.query(f"select * from d2.st;")
        tdLog.info(f'{tdSql.getRows()})')
        tdSql.checkRows(4)

        tdSql.execute(f"insert into d2.st(ts, f, tbname) values('2021-04-19 08:00:13', 1, 'ct1') d1.ct1 values('2021-04-19 08:00:14', 1);")

        tdSql.query(f"select * from d1.st")
        tdLog.info(f'{tdSql.getRows()})')
        tdSql.checkRows(12)

        tdSql.query(f"select * from d2.st;")
        tdLog.info(f'{tdSql.getRows()})')
        tdSql.checkRows(5)

        tdSql.execute(f"create database dgxy;")
        tdSql.execute(f"use dgxy;")
        tdSql.execute(f"create table st(ts timestamp, f int) tags(t int);")
        tdSql.execute(f"insert into ct1 using st tags(1) values('2021-04-19 08:00:03', 1);")
        tdSql.execute(f"insert into st(tbname, ts, f) values('ct1', '2021-04-19 08:00:04', 2);")
        tdSql.query(f"select * from ct1;")
        tdSql.checkRows(2)

        tdSql.query(f"show tables like 'ct1';")
        tdSql.checkRows(1)

        tdSql.execute(f"insert into st(tbname, ts, f, t) values('ct2',now,20,NULL)('ct3',now,30,NULL)")
        tdSql.execute(f"insert into st(tbname, t, ts, f) values('ct4',NULL, now,20)('ct5',NULL, now,30)")
        tdSql.query(f"show create table ct2")
        tdSql.query(f"show create table ct3")
        tdSql.query(f"show create table ct4")
        tdSql.query(f"show create table ct5")
        tdSql.query(f"show tags from ct2")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"show tags from ct3")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"show tags from ct4")
        tdSql.checkData(0, 5, None)

        tdSql.query(f"show tags from ct5")
        tdSql.checkData(0, 5, None)

        tdSql.error(f"insert into d2.st values(now, 1, 1)")
        tdSql.error(f"insert into d2.st(ts, f) values(now, 1);")
        tdSql.error(f"insert into d2.st(ts, f, tbname) values(now, 1);")
        tdSql.error(f"insert into d2.st(ts, f, tbname) values(now, 1, '');")
        tdSql.error(f"insert into d2.st(ts, f, tbname) values(now, 1, 'd2.ct2');")
        tdSql.error(f"insert into d2.st(ts, tbname) values(now, 1, 34)")
        tdSql.error(f"insert into st using st2 tags(2) values(now,1);")
        # TS-7696
        tdSql.execute(f"create table d2.st3(ts timestamp, col64_abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdef int) tags(t int);")
        tdSql.execute(f"create table d2.st4(`ts` timestamp, `col64_abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdef` int) tags(`t` int);")

        tdSql.execute(f"insert into d2.st3(tbname, ts ,col64_abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdef)values('ct10', '2021-04-19 08:00:03',1);")
        tdSql.execute(f"insert into d2.st3(tbname, ts ,`col64_abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdef`)values('ct11', '2021-04-19 08:00:03',1);")
        tdSql.execute(f"insert into d2.ct10(ts ,col64_abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdef)values('2021-04-19 08:00:04',2);")
        tdSql.execute(f"insert into d2.ct10(`ts` ,`col64_abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdef`)values('2021-04-19 08:00:05',3);")

        tdSql.error(f"create table d2.st4(ts timestamp, col65_abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdef int) tags(t int);")
        tdSql.error(f"create table d2.st4(ts timestamp, `col65_abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdef` int) tags(t int);")
        tdSql.error(f"insert into d2.st3(tbname, ts ,col65_abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefg)values('ct12', '2021-04-19 08:00:03',1);")
        tdSql.error(f"insert into d2.st3(tbname, ts ,col65_abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefg)values('ct12', '2021-04-19 08:00:03',1);")

    def InsertMultiTb(self):
        tdLog.info(f"======================== dnode1 start")

        tdSql.execute(f"create database mul_db")
        tdSql.execute(f"use mul_db")
        tdSql.execute(f"create table mul_st (ts timestamp, col1 int) tags (tag1 int)")

        # case: insert multiple recordes for multiple table in a query
        tdLog.info(
            f"=========== insert_multiTbl.sim case: insert multiple records for multiple table in a query"
        )
        ts = 1600000000000
        tdSql.execute(
            f"insert into mul_t0 using mul_st tags(0) values ( {ts} , 0) ( {ts} + 1s, 1) ( {ts} + 2s, 2) mul_t1 using mul_st tags(1) values ( {ts} , 10) ( {ts} + 1s, 11) ( {ts} + 2s, 12) mul_t2 using mul_st tags(2) values ( {ts} , 20) ( {ts} + 1s, 21) ( {ts} + 2s, 22) mul_t3 using mul_st tags(3) values ( {ts} , 30) ( {ts} + 1s, 31) ( {ts} + 2s, 32)"
        )
        tdSql.query(f"select * from mul_st order by ts, col1 ;")
        tdLog.info(f"rows = {tdSql.getRows()})")
        tdSql.checkRows(12)
        tdSql.checkData(0, 1, 0)

        tdSql.execute(f"create table mul_b0 (ts timestamp, col1 int)")
        tdSql.execute(f"create table mul_b1 (ts timestamp, col1 int)")
        tdSql.execute(f"create table mul_b2 (ts timestamp, col1 int)")
        tdSql.execute(f"create table mul_b3 (ts timestamp, col1 int)")

        tdSql.execute(
            f"insert into mul_b0 values ( {ts} , 0) ( {ts} + 1s, 1) ( {ts} + 2s, 2) mul_b1 values ( {ts} , 10) ( {ts} + 1s, 11) ( {ts} + 2s, 12) mul_b2 values ( {ts} , 20) ( {ts} + 1s, 21) ( {ts} + 2s, 22) mul_b3 values ( {ts} , 30) ( {ts} + 1s, 31) ( {ts} + 2s, 32)"
        )
        tdSql.query(f"select * from mul_b3")
        tdLog.info(f"rows = {tdSql.getRows()})")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 30)

        # insert values for specified columns
        tdSql.execute(
            f"create table mul_st1 (ts timestamp, col1 int, col2 float, col3 binary(10)) tags (tag1 int, tag2 int, tag3 binary(8))"
        )
        tdLog.info(
            f"=========== insert values for specified columns for multiple table in a query"
        )
        ts = 1600000000000
        tdSql.execute(
            f"insert into mul_t10 (ts, col1, col3) using mul_st1 (tag1, tag3) tags(0, 'tag3-0') values ( {ts} , 00, 'binary00') ( {ts} + 1s, 01, 'binary01') ( {ts} + 2s, 02, 'binary02') mul_t11 (ts, col1, col3) using mul_st1 (tag1, tag3) tags(1, 'tag3-0') values ( {ts} , 10, 'binary10') ( {ts} + 1s, 11, 'binary11') ( {ts} + 2s, 12, 'binary12') mul_t12 (ts, col1, col3) using mul_st1 (tag1, tag3) tags(2, 'tag3-0') values ( {ts} , 20, 'binary20') ( {ts} + 1s, 21, 'binary21') ( {ts} + 2s, 22, 'binary22') mul_t13 (ts, col1, col3) using mul_st1 (tag1, tag3) tags(3, 'tag3-0') values ( {ts} , 30, 'binary30') ( {ts} + 1s, 31, 'binary31') ( {ts} + 2s, 32, 'binary32')"
        )

        tdSql.query(f"select * from mul_st1 order by ts, col1 ;")
        tdLog.info(f"rows = {tdSql.getRows()})")
        tdSql.checkRows(12)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, "binary00")
        tdSql.checkData(9, 2, None)
        tdSql.checkData(9, 3, "binary12")

    def Values(self):
        tdLog.info(f"======================== dnode1 start")
        tdSql.prepare(dbname="vdb0")
        tdSql.execute(f"create table vdb0.mt (ts timestamp, tbcol int) TAGS(tgcol int)")

        tdSql.execute(f"create table vdb0.vtb00 using vdb0.mt tags( 0 )")
        tdSql.execute(f"create table vdb0.vtb01 using vdb0.mt tags( 0 )")

        tdSql.prepare(dbname="vdb1")
        tdSql.execute(f"create table vdb1.mt (ts timestamp, tbcol int) TAGS(tgcol int)")
        tdSql.error(f"create table vdb1.vtb10 using vdb0.mt tags( 1 )")
        tdSql.error(f"create table vdb1.vtb11 using vdb0.mt tags( 1 )")
        tdSql.execute(f"create table vdb1.vtb10 using vdb1.mt tags( 1 )")
        tdSql.execute(f"create table vdb1.vtb11 using vdb1.mt tags( 1 )")

        tdSql.prepare(dbname="vdb2")
        tdSql.execute(f"create table vdb2.mt (ts timestamp, tbcol int) TAGS(tgcol int)")
        tdSql.error(f"create table vdb2.vtb20 using vdb0.mt tags( 2 )")
        tdSql.error(f"create table vdb2.vtb21 using vdb0.mt tags( 2 )")
        tdSql.execute(f"create table vdb2.vtb20 using vdb2.mt tags( 2 )")
        tdSql.execute(f"create table vdb2.vtb21 using vdb2.mt tags( 2 )")

        tdSql.prepare(dbname="vdb3")
        tdSql.execute(f"create table vdb3.mt (ts timestamp, tbcol int) TAGS(tgcol int)")
        tdSql.error(f"create table vdb3.vtb20 using vdb0.mt tags( 2 )")
        tdSql.error(f"create table vdb3.vtb21 using vdb0.mt tags( 2 )")
        tdSql.execute(f"create table vdb3.vtb30 using vdb3.mt tags( 3 )")
        tdSql.execute(f"create table vdb3.vtb31 using vdb3.mt tags( 3 )")

        tdLog.info(f"=============== step2")
        tdSql.execute(
            f"insert into vdb0.vtb00 values (1519833600000 , 10) (1519833600001, 20) (1519833600002, 30)"
        )
        tdSql.execute(
            f"insert into vdb0.vtb01 values (1519833600000 , 10) (1519833600001, 20) (1519833600002, 30)"
        )
        tdSql.execute(
            f"insert into vdb1.vtb10 values (1519833600000 , 11) (1519833600001, 21) (1519833600002, 31)"
        )
        tdSql.execute(
            f"insert into vdb1.vtb11 values (1519833600000 , 11) (1519833600001, 21) (1519833600002, 31)"
        )
        tdSql.execute(
            f"insert into vdb2.vtb20 values (1519833600000 , 12) (1519833600001, 22) (1519833600002, 32)"
        )
        tdSql.execute(
            f"insert into vdb2.vtb21 values (1519833600000 , 12) (1519833600001, 22) (1519833600002, 32)"
        )
        tdSql.execute(
            f"insert into vdb3.vtb30 values (1519833600000 , 13) (1519833600001, 23) (1519833600002, 33)"
        )
        tdSql.execute(
            f"insert into vdb3.vtb31 values (1519833600000 , 13) (1519833600001, 23) (1519833600002, 33)"
        )
        tdSql.query(f"select * from vdb0.mt")
        tdSql.query(f"select ts from vdb0.mt")

        tdSql.checkRows(6)

        tdLog.info(f"=============== step3")
        tdSql.execute(
            f"insert into vdb0.vtb00 values (1519833600003 , 40) (1519833600005, 50) (1519833600004, 60)"
        )
        tdSql.execute(
            f"insert into vdb0.vtb01 values (1519833600003 , 40) (1519833600005, 50) (1519833600004, 60)"
        )
        tdSql.execute(
            f"insert into vdb1.vtb10 values (1519833600003 , 41) (1519833600005, 51) (1519833600004, 61)"
        )
        tdSql.execute(
            f"insert into vdb1.vtb11 values (1519833600003 , 41) (1519833600005, 51) (1519833600004, 61)"
        )
        tdSql.execute(
            f"insert into vdb2.vtb20 values (1519833600003 , 42) (1519833600005, 52) (1519833600004, 62)"
        )
        tdSql.execute(
            f"insert into vdb2.vtb21 values (1519833600003 , 42) (1519833600005, 52) (1519833600004, 62)"
        )
        tdSql.execute(
            f"insert into vdb3.vtb30 values (1519833600003 , 43) (1519833600005, 53) (1519833600004, 63)"
        )
        tdSql.execute(
            f"insert into vdb3.vtb31 values (1519833600003 , 43) (1519833600005, 53) (1519833600004, 63)"
        )
        tdSql.query(f"select * from vdb0.mt")
        tdSql.query(f"select ts from vdb0.mt")

        tdSql.checkRows(12)

        tdLog.info(f"=============== step4")
        tdSql.execute(
            f"insert into vdb0.vtb00 values(1519833600006, 60) (1519833600007, 70) vdb0.vtb01 values(1519833600006, 60) (1519833600007, 70)"
        )
        tdSql.execute(
            f"insert into vdb1.vtb10 values(1519833600006, 61) (1519833600007, 71) vdb1.vtb11 values(1519833600006, 61) (1519833600007, 71)"
        )
        tdSql.execute(
            f"insert into vdb2.vtb20 values(1519833600006, 62) (1519833600007, 72) vdb2.vtb21 values(1519833600006, 62) (1519833600007, 72)"
        )
        tdSql.execute(
            f"insert into vdb3.vtb30 values(1519833600006, 63) (1519833600007, 73) vdb3.vtb31 values(1519833600006, 63) (1519833600007, 73)"
        )
        tdSql.query(f"select * from vdb0.mt")
        tdSql.query(f"select ts from vdb0.mt")

        tdSql.checkRows(16)

        tdLog.info(f"=============== step5")
        tdSql.execute(
            f"insert into vdb0.vtb00 values(1519833600008, 80) (1519833600007, 70) vdb0.vtb01 values(1519833600006, 80) (1519833600007, 70)"
        )
        tdSql.execute(
            f"insert into vdb1.vtb10 values(1519833600008, 81) (1519833600007, 71) vdb1.vtb11 values(1519833600006, 81) (1519833600007, 71)"
        )
        tdSql.execute(
            f"insert into vdb2.vtb20 values(1519833600008, 82) (1519833600007, 72) vdb2.vtb21 values(1519833600006, 82) (1519833600007, 72)"
        )
        tdSql.execute(
            f"insert into vdb3.vtb30 values(1519833600008, 83) (1519833600007, 73) vdb3.vtb31 values(1519833600006, 83) (1519833600007, 73)"
        )
        tdSql.query(f"select * from vdb0.mt")
        tdSql.query(f"select ts from vdb0.mt")

        tdSql.checkRows(17)

        tdLog.info(f"=============== step6")
        tdSql.execute(
            f"insert into vdb0.vtb00 values(1519833600009, 90) (1519833600010, 100) vdb1.vtb10 values(1519833600009, 90) (1519833600010, 100) vdb2.vtb20 values(1519833600009, 90) (1519833600010, 100) vdb3.vtb30 values(1519833600009, 90) (1519833600010, 100)"
        )
        tdSql.execute(
            f"insert into vdb0.vtb01 values(1519833600009, 90) (1519833600010, 100) vdb1.vtb11 values(1519833600009, 90) (1519833600010, 100) vdb2.vtb21 values(1519833600009, 90) (1519833600010, 100) vdb3.vtb31 values(1519833600009, 90) (1519833600010, 100)"
        )

        tdSql.query(f"select * from vdb0.mt")
        tdSql.query(f"select ts from vdb0.mt")

        tdSql.checkRows(21)

        tdLog.info(f"=============== step7")
        tdSql.execute(
            f"insert into vdb0.vtb00 values(1519833600012, 120) (1519833600011, 110) vdb1.vtb10 values(1519833600012, 120) (1519833600011, 110) vdb2.vtb20 values(1519833600012, 120) (1519833600011, 110) vdb3.vtb30 values(1519833600012, 120) (1519833600011, 110)"
        )
        tdSql.execute(
            f"insert into vdb0.vtb01 values(1519833600012, 120) (1519833600011, 110) vdb1.vtb11 values(1519833600012, 120) (1519833600011, 110) vdb2.vtb21 values(1519833600012, 120) (1519833600011, 110) vdb3.vtb31 values(1519833600012, 120) (1519833600011, 110)"
        )

        tdSql.query(f"select * from vdb0.mt")
        tdSql.query(f"select ts from vdb0.mt")

        tdSql.checkRows(25)
