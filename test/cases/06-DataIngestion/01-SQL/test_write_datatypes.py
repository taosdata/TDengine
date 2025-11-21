from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestWriteDatatypes:

    def setup_class(cls):
        cls.database = "db1"
        tdLog.debug(f"start to excute {__file__}")

    def prepare_db(self):
        tdSql.execute(f"drop database if exists {self.database}")
        tdSql.execute(f"create database {self.database}")
        tdSql.execute(f"use {self.database}")

    def run_value(self, table_name, dtype, bits):
        tdSql.execute(f"drop table if exists {table_name}")
        tdSql.execute(f"create table {table_name}(ts timestamp, i1 {dtype}, i2 {dtype} unsigned)")

        tdSql.execute(f"insert into {table_name} values(1717122943000, -16, +6)")
        tdSql.execute(f"insert into {table_name} values(1717122944000, 80.99, +0042)")
        tdSql.execute(f"insert into {table_name} values(1717122945000, -0042, +80.99)")
        tdSql.execute(f"insert into {table_name} values(1717122946000, 52.34354, 18.6)")
        tdSql.execute(f"insert into {table_name} values(1717122947000, -12., +3.)")
        tdSql.execute(f"insert into {table_name} values(1717122948000, -0.12, +3.0)")
        tdSql.execute(f"insert into {table_name} values(1717122949000, -2.3e1, +2.324e2)")
        tdSql.execute(f"insert into {table_name} values(1717122950000, -2e1,  +2e2)")
        tdSql.execute(f"insert into {table_name} values(1717122951000, -2.e1, +2.e2)")
        tdSql.execute(f"insert into {table_name} values(1717122952000, -0x40, +0b10000)")
        tdSql.execute(f"insert into {table_name} values(1717122953000, -0b10000, +0x40)")

        # str support
        tdSql.execute(f"insert into {table_name} values(1717122954000, '-16', '+6')")
        tdSql.execute(f"insert into {table_name} values(1717122955000, ' -80.99', ' +0042')")
        tdSql.execute(f"insert into {table_name} values(1717122956000, ' -0042', ' +80.99')")
        tdSql.execute(f"insert into {table_name} values(1717122957000, '52.34354', '18.6')")
        tdSql.execute(f"insert into {table_name} values(1717122958000, '-12.', '+5.')")
        tdSql.execute(f"insert into {table_name} values(1717122959000, '-.12', '+.5')")
        tdSql.execute(f"insert into {table_name} values(1717122960000, '-2.e1', '+2.e2')")
        tdSql.execute(f"insert into {table_name} values(1717122961000, '-2e1',  '+2e2')")
        tdSql.execute(f"insert into {table_name} values(1717122962000, '-2.3e1', '+2.324e2')")
        tdSql.execute(f"insert into {table_name} values(1717122963000, '-0x40', '+0b10010')")
        tdSql.execute(f"insert into {table_name} values(1717122964000, '-0b10010', '+0x40')")

        tdSql.query(f"select * from {table_name}")
        tdSql.checkRows(22)

        baseval = 2**(bits/2)
        negval = -baseval + 1.645
        posval = baseval + 4.323
        bigval = 2**(bits-1)
        max_i = bigval - 1
        min_i = -bigval
        max_u = 2*bigval - 1
        min_u = 0
        print("val:", baseval, negval, posval, max_i)

        tdSql.execute(f"insert into {table_name} values(1717122965000, {negval}, {posval})")
        tdSql.execute(f"insert into {table_name} values(1717122966000, -{baseval}, {baseval})")
        tdSql.execute(f"insert into {table_name} values(1717122967000, {max_i}, {max_u})")
        tdSql.execute(f"insert into {table_name} values(1717122968000, {min_i}, {min_u})")

        tdSql.query(f"select * from {table_name}")
        tdSql.checkRows(26)
        
        # error case
        tdSql.error(f"insert into {table_name} values(1717122969000, 0, {max_u+1})")
        tdSql.error(f"insert into {table_name} values(1717122970000, 0, -1)")
        tdSql.error(f"insert into {table_name} values(1717122971000, 0, -2.0)")
        tdSql.error(f"insert into {table_name} values(1717122972000, 0, '-2.0')")
        tdSql.error(f"insert into {table_name} values(1717122973000, {max_i+1}, 0)")
        tdSql.error(f"insert into {table_name} values(1717122974000, {min_i-1}, 0)")
        tdSql.error(f"insert into {table_name} values(1717122975000, '{min_i-1}', 0)")

    def run_tags(self, stable_name, dtype, bits):
        tdSql.execute(f"create stable {stable_name}(ts timestamp, i1 {dtype}, i2 {dtype} unsigned) tags(id {dtype})")

        baseval = 2**(bits/2)
        negval = -baseval + 1.645
        posval = baseval + 4.323
        bigval = 2**(bits-1)
        max_i = bigval - 1
        min_i = -bigval
        max_u = 2*bigval - 1
        min_u = 0

        tdSql.execute(f"insert into {stable_name}_1 using {stable_name} tags('{negval}') values(1717122976000, {negval}, {posval})")
        tdSql.execute(f"insert into {stable_name}_2 using {stable_name} tags({posval}) values(1717122977000, -{baseval} , {baseval})")
        tdSql.execute(f"insert into {stable_name}_3 using {stable_name} tags('0x40') values(1717122978000, {max_i}, {max_u})")
        tdSql.execute(f"insert into {stable_name}_4 using {stable_name} tags(0b10000) values(1717122979000, {min_i}, {min_u})")
        
        tdSql.execute(f"insert into {stable_name}_5 using {stable_name} tags({max_i}) values(1717122980000, '{negval}', '{posval}')")
        tdSql.execute(f"insert into {stable_name}_6 using {stable_name} tags('{min_i}') values(1717122981000, '-{baseval}' , '{baseval}')")
        tdSql.execute(f"insert into {stable_name}_7 using {stable_name} tags(-0x40) values(1717122982000, '{max_i}', '{max_u}')")
        tdSql.execute(f"insert into {stable_name}_8 using {stable_name} tags('-0b10000') values(1717122983000, '{min_i}', '{min_u}')")

        tdSql.execute(f"insert into {stable_name}_9 using {stable_name} tags(12.) values(1717122984000, {negval}, {posval})")
        tdSql.execute(f"insert into {stable_name}_10 using {stable_name} tags('-8.3') values(1717122985000, -{baseval} , {baseval})")
        tdSql.execute(f"insert into {stable_name}_11 using {stable_name} tags(2.e1) values(1717122986000, {max_i}, {max_u})")
        tdSql.execute(f"insert into {stable_name}_12 using {stable_name} tags('-2.3e1') values(1717122987000, {min_i}, {min_u})")

        tdSql.query(f"select * from {stable_name}")
        tdSql.checkRows(12)

    def InsertDouble(self):
        tdSql.prepare(replica = self.replicaVar)
        self.prepare_db()

        self.run_value("t1", "bigint", 64)
        self.run_value("t2", "int", 32)
        self.run_value("t3", "smallint", 16)
        self.run_value("t4", "tinyint", 8)
        tdLog.printNoPrefix("==========end case1 run ...............")

        self.run_tags("t_big", "bigint", 64)
        self.run_tags("t_int", "int", 32)
        self.run_tags("t_small", "smallint", 16)
        self.run_tags("t_tiny", "tinyint", 8)
        tdLog.printNoPrefix("==========end case2 run ...............")
        tdLog.info(f"{__file__} successfully executed")

    def test_write_datatypes(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        """Write data types

        1. Write data with NULL values
        2. Write data using different floating-point representations
        3. Write Chinese character data
        4. Write data with different timestamp representations
        5. Write data with backquote
        
        Catalog:
            - DataIngestion
    
        Since: v3.0.0.0

        History:
            - 2023-12-1 Bob Liu Created, Huo Hong Migrated to new test framework
            - 2025-8-12 Simon Guan Migrated from tsim/insert/basic2.sim
            - 2025-8-12 Simon Guan Migrated from tsim/parser/insert_tb.sim
            - 2025-8-12 Simon Guan Migrated from tsim/query/time_process.sim
            - 2025-8-12 Simon Guan Migrated from tsim/insert/backquote.sim
            - 2025-8-12 Simon Guan Migrated from tsim/insert/null.sim

        """

        self.InsertDouble()
        tdStream.dropAllStreamsAndDbs()
        self.Basic2()
        tdStream.dropAllStreamsAndDbs()
        self.InsertTb()
        tdStream.dropAllStreamsAndDbs()
        self.TimeProcess()
        tdStream.dropAllStreamsAndDbs()
        self.Null()
        tdStream.dropAllStreamsAndDbs()
        self.Backquote()
        
    def Basic2(self):
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database d0 keep 365000d,365000d,365000d")
        tdSql.execute(f"use d0")

        tdLog.info(f"=============== create super table")
        tdSql.execute(
            f"create table if not exists stb (ts timestamp, c1 int unsigned, c2 double, c3 binary(10), c4 nchar(10), c5 double) tags (city binary(20),district binary(20));"
        )

        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== create child table")
        tdSql.execute(f'create table ct1 using stb tags("BeiJing", "ChaoYang")')
        tdSql.execute(f'create table ct2 using stb tags("BeiJing", "HaiDian")')

        tdSql.query(f"show tables")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step3-1 insert records into ct1")
        tdSql.execute(
            f"insert into ct1 values('2022-05-03 16:59:00.010', 10, 20, 'n','n',30);"
        )
        tdSql.execute(
            f"insert into ct1 values('2022-05-03 16:59:00.011', 'null', 'null', 'N',\"N\",30);"
        )
        tdSql.execute(
            f"insert into ct1 values('2022-05-03 16:59:00.012', 'null', 'null', 'Nul','NUL',30);"
        )
        tdSql.execute(
            f"insert into ct1 values('2022-05-03 16:59:00.013', NULL, 'null', 'Null',null,30);"
        )
        tdSql.execute(
            f"insert into ct1 values('2022-05-03 16:59:00.014', NULL, 'NuLL', 'Null',NULL,30);"
        )

        tdSql.error(
            f"insert into ct1 values('2022-05-03 16:59:00.015', NULL, 20, 'Null',NUL,30);"
        )
        tdSql.error(
            f"insert into ct1 values('2022-05-03 16:59:00.015', NULL, 20, 'Null',NU,30);"
        )
        tdSql.error(
            f"insert into ct1 values('2022-05-03 16:59:00.015', NULL, 20, 'Null',Nu,30);"
        )
        tdSql.error(
            f"insert into ct1 values('2022-05-03 16:59:00.015', NULL, 20, 'Null',N,30);"
        )
        tdSql.error(
            f"insert into ct1 values('2022-05-03 16:59:00.015', N, 20, 'Null',NULL,30);"
        )
        tdSql.error(
            f"insert into ct1 values('2022-05-03 16:59:00.015', Nu, 20, 'Null',NULL,30);"
        )
        tdSql.error(
            f"insert into ct1 values('2022-05-03 16:59:00.015', Nul, 20, 'Null',NULL,30);"
        )

        tdLog.info(f"=============== step3-1 query records of ct1 from memory")
        tdSql.query(f"select * from ct1;")

        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 20.000000000)
        tdSql.checkData(0, 3, "n")
        tdSql.checkData(0, 4, "n")
        tdSql.checkData(0, 5, 30.000000000)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(1, 2, None)
        tdSql.checkData(1, 3, "N")
        tdSql.checkData(1, 4, "N")
        tdSql.checkData(1, 5, 30.000000000)
        tdSql.checkData(2, 1, None)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(2, 3, "Nul")
        tdSql.checkData(2, 4, "NUL")
        tdSql.checkData(2, 5, 30.000000000)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(3, 2, None)
        tdSql.checkData(3, 3, "Null")
        tdSql.checkData(3, 4, None)
        tdSql.checkData(3, 5, 30.000000000)
        tdSql.checkData(4, 1, None)
        tdSql.checkData(4, 2, None)
        tdSql.checkData(4, 3, "Null")
        tdSql.checkData(4, 4, None)
        tdSql.checkData(4, 5, 30.000000000)

        # ==================== reboot to trigger commit data to file
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=============== step3-2 query records of ct1 from file")
        tdSql.query(f"select * from ct1;")

        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 20.000000000)
        tdSql.checkData(0, 3, "n")
        tdSql.checkData(0, 4, "n")
        tdSql.checkData(0, 5, 30.000000000)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(1, 2, None)
        tdSql.checkData(1, 3, "N")
        tdSql.checkData(1, 4, "N")
        tdSql.checkData(1, 5, 30.000000000)
        tdSql.checkData(2, 1, None)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(2, 3, "Nul")
        tdSql.checkData(2, 4, "NUL")
        tdSql.checkData(2, 5, 30.000000000)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(3, 2, None)
        tdSql.checkData(3, 3, "Null")
        tdSql.checkData(3, 4, None)
        tdSql.checkData(3, 5, 30.000000000)
        tdSql.checkData(4, 1, None)
        tdSql.checkData(4, 2, None)
        tdSql.checkData(4, 3, "Null")
        tdSql.checkData(4, 4, None)
        tdSql.checkData(4, 5, 30.000000000)

        tdSql.error(
            f"insert into ct1 using stb tags('a', 'b') values ('2022-06-26 13:00:00', 1) ct11 using sta tags('c', 'b#) values ('2022-06-26 13:00:01', 2);"
        )

    def InsertTb(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "fi_in_db"
        tbPrefix = "fi_in_tb"
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

        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, col1 int, col2 bigint, col3 float, col4 double, col5 binary(20), col6 bool, col7 smallint, col8 tinyint, col9 nchar(10)) tags (tag1 int)"
        )

        # case: insert multiple records in a query
        tdLog.info(
            f"=========== create_tb.sim case: insert multiple records in a query"
        )
        ts = 1500000000000
        col1 = 1
        col2 = 1
        col3 = "1.1e3"
        col4 = "1.1e3"
        col5 = '"Binary"'
        col6 = "true"
        col7 = 1
        col8 = 1
        col9 = '"Nchar"'
        tag1 = 1
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"create table {tb} using {mt} tags( {tag1} )")
        tdSql.execute(
            f"insert into {tb} values ( {ts} , {col1} , {col2} , {col3} , {col4} , {col5} , {col6} , {col7} , {col8} , {col9} ) ( {ts} + 1000a, {col1} , {col2} , {col3} , {col4} , {col5} , {col6} , {col7} , {col8} , {col9} )"
        )
        tdSql.query(f"select * from {tb} order by ts desc")
        tdLog.info(f"rows = {tdSql.getRows()})")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, col1)

        # sql drop table $tb

        # insert values for specified columns
        col1 = 2
        col3 = 3
        col5 = 5
        tdSql.execute(f"create table if not exists {tb} using {mt} tags( {tag1} )")
        tdSql.execute(
            f"insert into {tb} ( ts, col1, col3, col5) values ( {ts} + 2000a, {col1} , {col3} , {col5} )"
        )
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, col1)

        tdLog.info(f"tdSql.getData(0,3) = {tdSql.getData(0,3)}")
        tdSql.checkData(0, 3, 3.00000)
        tdSql.checkData(0, 5, col5)

        tdSql.execute(
            f"insert into {tb} (ts, col1, col2, col3, col4, col5, col6, col7, col8, col9) values ( {ts} + 3000a, {col1} , {col2} , {col3} , {col4} , {col5} , {col6} , {col7} , {col8} , {col9} ) ( {ts} + 4000a, {col1} , {col2} , {col3} , {col4} , {col5} , {col6} , {col7} , {col8} , {col9} )"
        )
        tdSql.query(f"select * from {tb} order by ts desc")
        tdLog.info(f"rows = {tdSql.getRows()})")
        tdSql.checkRows(5)

        # case: insert records from .csv files
        # manually test
        # sql insert into $tb file parser/parser_test_data.csv
        # sql select * from $tb
        # if $rows != 10 then
        #  return -1
        # endi

        tdSql.execute(f"drop table {tb}")
        tdSql.execute(f"create table tb1 (ts timestamp, c1 int)")
        tdSql.execute(f"create table tb2 (ts timestamp, c1 int)")
        tdSql.execute(f"insert into tb1 values(now, 1) tb2 values (now, 2)")
        tdSql.query(f"select count(*) from tb1")
        tdSql.checkRows(1)

        tdSql.query(f"select count(*) from tb2")
        tdSql.checkRows(1)

        tdSql.execute(f"drop database {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table stb1 (ts timestamp, c1 int) tags(t1 int)")
        tdSql.execute(
            f"create table stb2 (ts timestamp, c1 double, c2 binary(10)) tags(t1 binary(10))"
        )
        tdSql.execute(f"create table tb1 using stb1 tags(1)")
        tdSql.execute(f"create table tb2 using stb2 tags('tb2')")
        tdSql.execute(
            f"insert into tb1 (ts, c1) values (now-1s, 1) (now, 2) tb2 (ts, c1) values (now-2s, 1) (now-1s, 2) (now, 3)"
        )
        tdSql.query(f"select * from tb1 order by ts asc")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 2)

        tdSql.query(f"select * from tb2 order by ts desc")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 3.000000000)
        tdSql.checkData(1, 1, 2.000000000)
        tdSql.checkData(2, 1, 1.000000000)

        tdSql.execute(f"drop database {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table stb (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 nchar(10), c6 binary(20)) tags(t1 int, t2 bigint, t3 double, t4 float, t5 nchar(10))"
        )
        tdSql.execute(f"create table tb0 using stb tags(0, 0, 0, 0, '涛思')")
        tdSql.execute(f"create table tb1 using stb tags('1', 1, 1, 1, '涛思')")
        tdSql.execute(f"create table tb2 using stb tags(2, '2', 2, 2, '涛思')")
        tdSql.execute(f"create table tb3 using stb tags(3, 3, '3', 3, '涛思')")
        tdSql.execute(f"create table tb4 using stb tags(4, 4, 4, '4', '涛思')")
        tdSql.execute(
            f"insert into tb0 values ('2018-09-17 09:00:00.000', 1, 1, 1, 1, '涛思nchar', 'none quoted')"
        )
        tdSql.execute(
            f"insert into tb1 values ('2018-09-17 09:00:00.000', '1', 1, 1, 1, '涛思nchar', 'quoted int')"
        )
        tdSql.execute(
            f"insert into tb2 values ('2018-09-17 09:00:00.000', 1, '1', 1, 1, '涛思nchar', 'quoted bigint')"
        )
        tdSql.execute(
            f"insert into tb3 values ('2018-09-17 09:00:00.000', 1, 1, '1', 1, '涛思nchar', 'quoted float')"
        )
        tdSql.execute(
            f"insert into tb4 values ('2018-09-17 09:00:00.000', 1, 1, 1, '1', '涛思nchar', 'quoted double')"
        )
        tdSql.query(f"select * from stb order by t1")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2018-09-17 09:00:00")
        tdSql.checkData(1, 0, "2018-09-17 09:00:00")
        tdSql.checkData(2, 0, "2018-09-17 09:00:00")
        tdSql.checkData(3, 0, "2018-09-17 09:00:00")
        tdSql.checkData(4, 0, "2018-09-17 09:00:00")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1.00000)
        tdSql.checkData(0, 4, 1.000000000)
        tdSql.checkData(0, 5, "涛思nchar")
        tdSql.checkData(0, 6, "none quoted")
        tdSql.checkData(1, 3, 1.00000)
        tdSql.checkData(1, 4, 1.000000000)
        tdSql.checkData(1, 5, "涛思nchar")
        tdSql.checkData(1, 6, "quoted int")
        tdSql.checkData(2, 5, "涛思nchar")
        tdSql.checkData(2, 6, "quoted bigint")
        tdSql.checkData(3, 5, "涛思nchar")
        tdSql.checkData(3, 6, "quoted float")
        tdSql.checkData(4, 5, "涛思nchar")
        tdSql.checkData(4, 6, "quoted double")

        # case: support NULL char of the binary field [TBASE-660]
        tdSql.execute(
            f"create table NULLb (ts timestamp, c1 binary(20), c2 binary(20), c3 float)"
        )
        tdSql.execute(
            f"insert into NULLb values ('2018-09-17 09:00:00.000', '', '', 3.746)"
        )
        tdSql.query(f"select * from NULLb")
        tdSql.checkRows(1)

    def TimeProcess(self):
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database db")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.execute(f"use db")

        tdLog.info(f"=============== create super table and child table")
        tdSql.execute(
            f"create table stb1 (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp) tags (t1 int)"
        )
        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdSql.execute(f"create table ct1 using stb1 tags ( 1 )")
        tdSql.execute(f"create table ct2 using stb1 tags ( 2 )")
        tdSql.execute(f"create table ct3 using stb1 tags ( 3 )")
        tdSql.execute(f"create table ct4 using stb1 tags ( 4 )")
        tdSql.query(f"show tables")
        tdLog.info(
            f"{tdSql.getRows()}) {tdSql.getData(0,0)} {tdSql.getData(1,0)} {tdSql.getData(2,0)}"
        )
        tdSql.checkRows(4)

        tdSql.execute(
            f"create table t1 (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)"
        )

        tdLog.info(f"=============== insert data into child table ct1 (s)")
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:01.000\', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now+1a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:06.000\', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now+2a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:10.000\', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now+3a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:16.000\', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now+4a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:20.000\', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now+5a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:26.000\', 6, 66666, 666, 66, 6.66, 66.66, 1, "binary6", "nchar6", now+6a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:30.000\', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", now+7a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:36.000\', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", now+8a )'
        )

        tdLog.info(f"=============== insert data into child table ct4 (y)")
        tdSql.execute(
            f"insert into ct4 values ( '2019-01-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )"
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2019-10-21 01:01:01.000\', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now+1a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2019-12-31 01:01:01.000\', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now+2a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2020-01-01 01:01:06.000\', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now+3a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2020-05-07 01:01:10.000\', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now+4a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2020-09-30 01:01:16.000\', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now+5a )'
        )
        tdSql.execute(
            f"insert into ct4 values ( '2020-12-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )"
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2021-02-01 01:01:20.000\', 6, 66666, 666, 66, 6.66, 66.66, 1, "binary6", "nchar6", now+6a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2021-10-28 01:01:26.000\', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", "1970-01-01 08:00:00.000" )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2021-12-01 01:01:30.000\', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", "1969-01-01 01:00:00.000" )'
        )
        # tdSql.execute(f"insert into ct4 values ( '2022-02-31 01:01:36.000', 9, -99999999999999999, -999, -99, -9.99, -999999999999999999999.99, 1, \"binary9\", \"nchar9\", \"1900-01-01 00:00:00.000\" )")
        tdSql.execute(
            f"insert into ct4 values ( '2022-05-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )"
        )

        tdLog.info(f"=============== insert data into child table t1")
        tdSql.execute(
            f'insert into t1 values ( \'2020-10-21 01:01:01.000\', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now+1a )'
        )
        tdSql.execute(
            f'insert into t1 values ( \'2020-12-31 01:01:01.000\', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now+2a )'
        )
        tdSql.execute(
            f'insert into t1 values ( \'2021-01-01 01:01:06.000\', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now+3a )'
        )
        tdSql.execute(
            f'insert into t1 values ( \'2021-05-07 01:01:10.000\', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now+4a )'
        )
        tdSql.execute(
            f'insert into t1 values ( \'2021-09-30 01:01:16.000\', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now+5a )'
        )
        tdSql.execute(
            f'insert into t1 values ( \'2022-02-01 01:01:20.000\', 6, 66666, 666, 66, 6.66, 66.66, 1, "binary6", "nchar6", now+6a )'
        )
        tdSql.execute(
            f'insert into t1 values ( \'2022-10-28 01:01:26.000\', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", "1970-01-01 08:00:00.000" )'
        )
        tdSql.execute(
            f'insert into t1 values ( \'2022-12-01 01:01:30.000\', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", "1969-01-01 01:00:00.000" )'
        )
        # tdSql.execute(f"insert into t1 values ( '2022-12-31 01:01:36.000', 9, -99999999999999999, -999, -99, -9.99, -999999999999999999999.99, 1, \"binary9\", \"nchar9\", \"1900-01-01 00:00:00.000\" )")

        tdLog.info(f"================ start query ======================")

        tdLog.info(f"=============== step1")
        tdLog.info(f"=====sql : select timediff(ts , c10) from ct4")
        tdSql.query(f"select cast(c1 as bigint) as b from ct4")
        tdLog.info(f"===> {tdSql.getRows()})")
        # tdSql.checkRows(1)

        # =================================================
        tdLog.info(f"=============== stop and restart taosd")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=============== step2 after wal")

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database db")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def Backquote(self):
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database `database`")
        tdSql.execute(f"create database `DataBase`")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdLog.info(f"{tdSql.getData(0,0)}  {tdSql.getData(0,1)}")
        tdLog.info(f"{tdSql.getData(1,0)}  {tdSql.getData(1,1)}")
        tdLog.info(f"{tdSql.getData(2,0)}  {tdSql.getData(2,1)}")
        tdSql.checkRows(4)

        tdSql.checkData(2, 0, "database")
        tdSql.checkData(3, 0, "DataBase")
        tdSql.checkData(0, 0, "information_schema")

        dbCnt = 0
        while dbCnt < 2:
            if dbCnt == 0:
                tdSql.execute(f"use `database`")
            else:
                tdSql.execute(f"use `DataBase`")

            dbCnt = dbCnt + 1

            tdLog.info(f"=============== create super table, include all type")
            tdLog.info(f"notes: after nchar show ok, modify binary to nchar")
            tdSql.execute(
                f"create table `stable` (`timestamp` timestamp, `int` int, `binary` binary(16), `nchar` nchar(16)) tags (`float` float, `Binary` binary(16), `Nchar` nchar(16))"
            )
            tdSql.execute(
                f"create table `Stable` (`timestamp` timestamp, `int` int, `Binary` binary(32), `Nchar` nchar(32)) tags (`float` float, `binary` binary(16), `nchar` nchar(16))"
            )

            tdSql.query(f"show stables")
            tdLog.info(f"rows: {tdSql.getRows()})")
            tdSql.checkRows(2)

            if tdSql.getData(0, 0) != "Stable" and tdSql.getData(0, 0) != "stable":
                tdLog.exit(f"data:{tdSql.getData(0, 0)} check failed")

            if tdSql.getData(1, 0) != "Stable" and tdSql.getData(1, 0) != "stable":
                tdLog.exit(f"data:{tdSql.getData(1, 0)} check failed")

            tdLog.info(f"=============== create child table")
            tdSql.execute(
                f"create table `table` using `stable` tags(100.0, 'stable+table', 'stable+table')"
            )
            tdSql.execute(
                f"create table `Table` using `stable` tags(100.1, 'stable+Table', 'stable+Table')"
            )

            tdSql.execute(
                f"create table `TAble` using `Stable` tags(100.0, 'Stable+TAble', 'Stable+TAble')"
            )
            tdSql.execute(
                f"create table `TABle` using `Stable` tags(100.1, 'Stable+TABle', 'Stable+TABle')"
            )

            tdSql.query(f"show tables")
            tdLog.info(f"rows: {tdSql.getRows()})")
            tdSql.checkRows(4)

            tdLog.info(f"=============== insert data")
            tdSql.execute(
                f"insert into `table` values(now+0s, 10, 'table', 'table')(now+1s, 11, 'table', 'table')"
            )
            tdSql.execute(
                f"insert into `Table` values(now+0s, 20, 'Table', 'Table')(now+1s, 21, 'Table', 'Table')"
            )
            tdSql.execute(
                f"insert into `TAble` values(now+0s, 30, 'TAble', 'TAble')(now+1s, 31, 'TAble', 'TAble')"
            )
            tdSql.execute(
                f"insert into `TABle` values(now+0s, 40, 'TABle', 'TABle')(now+4s, 41, 'TABle', 'TABle')"
            )

            tdLog.info(f"=============== query data")
            tdSql.query(f"select * from `table`")
            tdLog.info(f"rows: {tdSql.getRows()})")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 10)
            tdSql.checkData(0, 2, "table")
            tdSql.checkData(0, 3, "table")

            tdLog.info(f"=================> 1")
            tdSql.query(f"select * from `Table`")
            tdLog.info(f"rows: {tdSql.getRows()})")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 20)
            tdSql.checkData(0, 2, "Table")
            tdSql.checkData(0, 3, "Table")

            tdLog.info(f"================>2")
            tdSql.query(f"select * from `TAble`")
            tdLog.info(f"rows: {tdSql.getRows()})")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 30)
            tdSql.checkData(0, 2, "TAble")
            tdSql.checkData(0, 3, "TAble")

            tdSql.query(f"select * from `TABle`")
            tdLog.info(f"rows: {tdSql.getRows()})")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 40)
            tdSql.checkData(0, 2, "TABle")
            tdSql.checkData(0, 3, "TABle")

        tdLog.info(f"=============== stop and restart taosd")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.query(f"select * from information_schema.ins_databases")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkRows(4)
        tdSql.checkData(2, 0, "database")
        tdSql.checkData(3, 0, "DataBase")
        tdSql.checkData(0, 0, "information_schema")

        dbCnt = 0
        while dbCnt < 2:
            if dbCnt == 0:
                tdSql.execute(f"use `database`")
            else:
                tdSql.execute(f"use `DataBase`")
            dbCnt = dbCnt + 1

            tdSql.query(f"show stables")
            tdLog.info(f"rows: {tdSql.getRows()})")
            tdSql.checkRows(2)

            if tdSql.getData(0, 0) != "Stable" and tdSql.getData(0, 0) != "stable":
                tdLog.exit(f"data:{tdSql.getData(0, 0)} check failed")

            if tdSql.getData(1, 0) != "Stable" and tdSql.getData(1, 0) != "stable":
                tdLog.exit(f"data:{tdSql.getData(1, 0)} check failed")

            tdSql.query(f"show tables")
            tdLog.info(f"rows: {tdSql.getRows()})")
            tdSql.checkRows(4)

            tdLog.info(f"=============== query data")
            tdSql.query(f"select * from `table`")
            tdLog.info(f"rows: {tdSql.getRows()})")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 10)
            tdSql.checkData(0, 2, "table")
            tdSql.checkData(0, 3, "table")

            tdSql.query(f"select * from `Table`")
            tdLog.info(f"rows: {tdSql.getRows()})")
            tdSql.checkRows(2)

            tdSql.checkData(0, 1, 20)

            tdSql.checkData(0, 2, "Table")

            tdSql.checkData(0, 3, "Table")

            tdSql.query(f"select * from `TAble`")
            tdLog.info(f"rows: {tdSql.getRows()})")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 30)
            tdSql.checkData(0, 2, "TAble")
            tdSql.checkData(0, 3, "TAble")

            tdSql.query(f"select * from `TABle`")
            tdLog.info(f"rows: {tdSql.getRows()})")
            tdSql.checkRows(2)
            tdSql.checkData(0, 1, 40)
            tdSql.checkData(0, 2, "TABle")
            tdSql.checkData(0, 3, "TABle")

    def Null(self):
        tdLog.info(f"=============== create database")
        tdSql.prepare(dbname="d0")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)


        tdSql.execute(f"use d0")

        tdLog.info(
            f"=============== create super table, include column type for count/sum/min/max/first"
        )
        tdSql.execute(
            f"create table if not exists stb (ts timestamp, c1 int, c2 float, c3 double, c4 bigint) tags (t1 int unsigned)"
        )

        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== create child table")
        tdSql.execute(f"create table ct1 using stb tags(1000)")
        tdSql.execute(f"create table ct2 using stb tags(2000)")
        tdSql.execute(f"create table ct3 using stb tags(3000)")

        tdSql.query(f"show tables")
        tdSql.checkRows(3)

        tdLog.info(f"=============== insert data, include NULL")
        tdSql.execute(
            f"insert into ct1 values (now+0s, 10, 2.0, 3.0, 90)(now+1s, NULL, NULL, NULL, NULL)(now+2s, NULL, 2.1, 3.1, 91)(now+3s, 11, NULL, 3.2, 92)(now+4s, 12, 2.2, NULL, 93)(now+5s, 13, 2.3, 3.3, NULL)"
        )
        tdSql.execute(f"insert into ct1 values (now+6s, NULL, 2.4, 3.4, 94)")
        tdSql.execute(f"insert into ct1 values (now+7s, 14, NULL, 3.5, 95)")
        tdSql.execute(f"insert into ct1 values (now+8s, 15, 2.5, NULL, 96)")
        tdSql.execute(f"insert into ct1 values (now+9s, 16, 2.6, 3.6, NULL)")
        tdSql.execute(f"insert into ct1 values (now+10s, NULL, NULL, NULL, NULL)")
        tdSql.execute(f"insert into ct1 values (now+11s, -2147483647, 2.7, 3.7, 97)")

        # ===================================================================
        # ===================================================================
        tdLog.info(f"=============== query data from child table")
        tdSql.query(f"select * from ct1")
        tdLog.info(f"===> select * from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdLog.info(
            f"===> rows1: {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}"
        )
        tdLog.info(
            f"===> rows2: {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}"
        )
        tdLog.info(
            f"===> rows3: {tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}"
        )
        tdLog.info(
            f"===> rows4: {tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}"
        )
        tdSql.checkRows(12)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 2.00000)
        tdSql.checkData(0, 3, 3.000000000)

        # if $tdSql.getData(4,1) != -14 then
        #  return -1
        # endi
        # if $tdSql.getData(4,2) != -2.40000 then
        #  return -1
        # endi
        # if $tdSql.getData(4,3) != -3.400000000 then
        #  return -1
        # endi

        tdLog.info(f"=============== select count(*) from child table")
        tdSql.query(f"select count(*) from ct1")
        tdLog.info(f"===> select count(*) from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 12)

        tdLog.info(f"=============== select count(column) from child table")
        tdSql.query(f"select count(ts), count(c1), count(c2), count(c3) from ct1")
        tdLog.info(f"===> select count(ts), count(c1), count(c2), count(c3) from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkData(0, 0, 12)

        tdSql.checkData(0, 1, 8)
        tdSql.checkData(0, 2, 8)
        tdSql.checkData(0, 3, 8)

        # print =============== select first(*)/first(column) from child table
        # sql select first(*) from ct1
        # sql select first(ts), first(c1), first(c2), first(c3) from ct1

        tdLog.info(f"=============== select min(column) from child table")
        tdSql.query(f"select min(c1), min(c2), min(c3) from ct1")
        tdLog.info(f"===> select min(c1), min(c2), min(c3) from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkRows(1)
        tdSql.checkData(0, 0, -2147483647)
        tdSql.checkData(0, 1, 2.00000)
        tdSql.checkData(0, 2, 3.000000000)

        tdLog.info(f"=============== select max(column) from child table")
        tdSql.query(f"select max(c1), max(c2), max(c3) from ct1")
        tdLog.info(f"===> select max(c1), max(c2), max(c3) from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 16)
        tdSql.checkData(0, 1, 2.70000)
        tdSql.checkData(0, 2, 3.700000000)

        tdLog.info(f"=============== select sum(column) from child table")
        tdSql.query(f"select sum(c1), sum(c2), sum(c3) from ct1")
        tdLog.info(f"===> select sum(c1), sum(c2), sum(c3) from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkRows(1)
        tdSql.checkData(0, 0, -2147483556)
        tdSql.checkData(0, 1, 18.799999952)
        tdSql.checkData(0, 2, 26.800000000)

        tdLog.info(f"=============== select column, from child table")
        tdSql.query(f"select c1, c2, c3 from ct1")
        tdLog.info(f"===> select c1, c2, c3 from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkRows(12)
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(0, 1, 2.00000)
        tdSql.checkData(0, 2, 3.000000000)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(1, 2, None)
        tdSql.checkData(3, 0, 11)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(3, 2, 3.200000000)
        tdSql.checkData(9, 0, 16)
        tdSql.checkData(9, 1, 2.60000)
        tdSql.checkData(9, 2, 3.600000000)

        # ===================================================================
        # ===================================================================

        # print =============== query data from stb
        tdSql.query(f"select * from stb")
        tdLog.info(f"===>")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkRows(12)

        # print =============== select count(*) from supter table
        tdSql.query(f"select count(*) from stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 12)

        # print =============== select count(column) from supter table
        tdSql.query(f"select count(ts), count(c1), count(c2), count(c3) from stb")
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}  {tdSql.getData(0,3)}"
        )
        tdSql.checkData(0, 0, 12)
        tdSql.checkData(0, 1, 8)
        tdSql.checkData(0, 2, 8)
        tdSql.checkData(0, 3, 8)

        # ===================================================================

        tdLog.info(f"=============== stop and restart taosd, then again do query above")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        # ===================================================================

        tdLog.info(f"=============== query data from child table")
        tdSql.query(f"select * from ct1")
        tdLog.info(f"===> select * from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")
        tdSql.checkRows(12)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 2.00000)
        tdSql.checkData(0, 3, 3.000000000)
        tdSql.checkData(4, 1, 12)
        tdSql.checkData(4, 2, 2.20000)
        tdSql.checkData(4, 3, None)

        tdLog.info(f"=============== select count(*) from child table")
        tdSql.query(f"select count(*) from ct1")
        tdLog.info(f"===> select count(*) from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 12)

        tdLog.info(f"=============== select count(column) from child table")
        tdSql.query(f"select count(ts), count(c1), count(c2), count(c3) from ct1")
        tdLog.info(f"===> select count(ts), count(c1), count(c2), count(c3) from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkData(0, 0, 12)
        tdSql.checkData(0, 1, 8)
        tdSql.checkData(0, 2, 8)
        tdSql.checkData(0, 3, 8)

        # print =============== select first(*)/first(column) from child table
        # sql select first(*) from ct1
        # sql select first(ts), first(c1), first(c2), first(c3) from ct1

        tdLog.info(f"=============== select min(column) from child table")
        tdSql.query(f"select min(c1), min(c2), min(c3) from ct1")
        tdLog.info(f"===> select min(c1), min(c2), min(c3) from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkRows(1)
        tdSql.checkData(0, 0, -2147483647)
        tdSql.checkData(0, 1, 2.00000)
        tdSql.checkData(0, 2, 3.000000000)

        tdLog.info(f"=============== select max(column) from child table")
        tdSql.query(f"select max(c1), max(c2), max(c3) from ct1")
        tdLog.info(f"===> select max(c1), max(c2), max(c3) from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 16)
        tdSql.checkData(0, 1, 2.70000)
        tdSql.checkData(0, 2, 3.700000000)

        tdLog.info(f"=============== select sum(column) from child table")
        tdSql.query(f"select sum(c1), sum(c2), sum(c3) from ct1")
        tdLog.info(f"===> select sum(c1), sum(c2), sum(c3) from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkRows(1)
        tdSql.checkData(0, 0, -2147483556)
        tdSql.checkData(0, 1, 18.799999952)
        tdSql.checkData(0, 2, 26.800000000)

        tdLog.info(f"=============== select column, from child table")
        tdSql.query(f"select c1, c2, c3 from ct1")
        tdLog.info(f"===> select c1, c2, c3 from ct1")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkRows(12)
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(0, 1, 2.00000)
        tdSql.checkData(0, 2, 3.000000000)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(1, 2, None)
        tdSql.checkData(3, 0, 11)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(3, 2, 3.200000000)
        tdSql.checkData(9, 0, 16)
        tdSql.checkData(9, 1, 2.60000)
        tdSql.checkData(9, 2, 3.600000000)

        # ===================================================================

        tdLog.info(f"=============== query data from stb")
        tdSql.query(f"select * from stb")
        tdLog.info(f"===>")
        tdLog.info(f"===> rows: {tdSql.getRows()})")

        tdSql.checkRows(12)

        tdLog.info(f"=============== select count(*) from supter table")
        tdSql.query(f"select count(*) from stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 12)

        tdLog.info(f"=============== select count(column) from supter table")
        tdSql.query(f"select count(ts), count(c1), count(c2), count(c3) from stb")
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}  {tdSql.getData(0,3)}"
        )
        tdSql.checkData(0, 0, 12)
        tdSql.checkData(0, 1, 8)
        tdSql.checkData(0, 2, 8)
        tdSql.checkData(0, 3, 8)
