from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestWriteCommit:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_write_commit(self):
        """Write commit scenarios

        1. Data exists across multiple files
        2. Data distributed across multiple blocks
        3. Data coexists in both memory and files
        4. Restart the dnode (force kill)
        5. Verify data integrity through queries

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-12 Simon Guan Migrated from tsim/insert/commit-merge0.sim
            - 2025-8-12 Simon Guan Migrated from tsim/column/commit.sim
            - 2025-8-12 Simon Guan Migrated from tsim/insert/query_block1_file.sim
            - 2025-8-12 Simon Guan Migrated from tsim/insert/query_block1_memory.sim
            - 2025-8-12 Simon Guan Migrated from tsim/insert/query_block2_file.sim
            - 2025-8-12 Simon Guan Migrated from tsim/insert/query_block2_memory.sim
            - 2025-8-12 Simon Guan Migrated from tsim/insert/query_file_memory.sim
            - 2025-8-12 Simon Guan Migrated from tsim/insert/query_multi_file.sim
            - 2025-8-12 Simon Guan Migrated from tsim/stable/disk.sim
            - 2025-8-12 Simon Guan Migrated from tsim/parser/commit.sim
            - 2025-8-12 Simon Guan Migrated from tests/script/tsim/wal/kill.sim

        """

        self.CommitMerge()
        tdStream.dropAllStreamsAndDbs()
        self.CommitFile()
        tdStream.dropAllStreamsAndDbs()
        self.Block1File()
        tdStream.dropAllStreamsAndDbs()
        self.Block1Mem()
        tdStream.dropAllStreamsAndDbs()
        self.Block2File()
        tdStream.dropAllStreamsAndDbs()
        self.Block2Mem()
        tdStream.dropAllStreamsAndDbs()
        self.FileMem()
        tdStream.dropAllStreamsAndDbs()
        self.MultiFile()
        tdStream.dropAllStreamsAndDbs()
        self.Disk()
        tdStream.dropAllStreamsAndDbs()
        self.ParserCommit()
        tdStream.dropAllStreamsAndDbs()
        self.WalKill()
        tdStream.dropAllStreamsAndDbs()

    def CommitMerge(self):
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database db  duration 120 keep 365000d,365000d,365000d")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.execute(f"use db")
        tdSql.execute(f"create table stb1(ts timestamp, c6 double) tags (t1 int);")
        tdSql.execute(f"create table ct1 using stb1 tags ( 1 );")
        tdSql.execute(f"create table ct2 using stb1 tags ( 2 );")
        tdSql.execute(f"create table ct3 using stb1 tags ( 3 );")
        tdSql.execute(f"create table ct4 using stb1 tags ( 4 );")
        tdSql.execute(f"insert into ct1 values ('2022-05-01 18:30:27.001', 0.0);")
        tdSql.execute(f"insert into ct4 values ('2022-04-28 18:30:27.002', 0.0);")
        tdSql.execute(f"insert into ct1 values ('2022-05-01 18:30:17.003', 11.11);")
        tdSql.execute(f"insert into ct4 values ('2022-02-01 18:30:27.004', 11.11);")
        tdSql.execute(f"insert into ct1 values ('2022-05-01 18:30:07.005', 22.22);")
        tdSql.execute(f"insert into ct4 values ('2021-11-01 18:30:27.006', 22.22);")
        tdSql.execute(f"insert into ct1 values ('2022-05-01 18:29:27.007', 33.33);")
        tdSql.execute(f"insert into ct4 values ('2022-08-01 18:30:27.008', 33.33);")
        tdSql.execute(f"insert into ct1 values ('2022-05-01 18:20:27.009', 44.44);")
        tdSql.execute(f"insert into ct4 values ('2021-05-01 18:30:27.010', 44.44);")
        tdSql.execute(f"insert into ct1 values ('2022-05-01 18:21:27.011', 55.55);")
        tdSql.execute(f"insert into ct4 values ('2021-01-01 18:30:27.012', 55.55);")
        tdSql.execute(f"insert into ct1 values ('2022-05-01 18:22:27.013', 66.66);")
        tdSql.execute(f"insert into ct4 values ('2020-06-01 18:30:27.014', 66.66);")
        tdSql.execute(f"insert into ct1 values ('2022-05-01 18:28:37.015', 77.77);")
        tdSql.execute(f"insert into ct4 values ('2020-05-01 18:30:27.016', 77.77);")
        tdSql.execute(f"insert into ct1 values ('2022-05-01 18:29:17.017', 88.88);")
        tdSql.execute(f"insert into ct4 values ('2019-05-01 18:30:27.018', 88.88);")
        tdSql.execute(f"insert into ct1 values ('2022-05-01 18:30:20.019', 0);")
        tdSql.execute(f"insert into ct1 values ('2022-05-01 18:30:47.020', -99.99);")
        tdSql.execute(f"insert into ct1 values ('2022-05-01 18:30:49.021', NULL);")
        tdSql.execute(f"insert into ct1 values ('2022-05-01 18:30:51.022', -99.99);")
        tdSql.execute(f"insert into ct4 values ('2018-05-01 18:30:27.023', NULL) ;")
        tdSql.execute(f"insert into ct4 values ('2021-03-01 18:30:27.024', NULL) ;")
        tdSql.execute(f"insert into ct4 values ('2022-08-01 18:30:27.025', NULL) ;")

        tdLog.info(f"=============== select * from ct1 - memory")
        tdSql.query(f"select * from stb1;")
        tdSql.checkRows(25)

        tdLog.info(f"=============== stop and restart taosd")

        reboot_max = 10
        reboot_cnt = 0
        reboot_and_check = 1

        while reboot_and_check:

            sc.dnodeStop(1)
            sc.dnodeStart(1)
            clusterComCheck.checkDnodes(1)

            tdLog.info(
                f"=============== insert duplicated records to memory - loop {reboot_max} - {reboot_cnt}"
            )
            tdSql.execute(f"use db")
            tdSql.execute(f"insert into ct1 values ('2022-05-01 18:30:27.001', 0.0);")
            tdSql.execute(f"insert into ct4 values ('2022-04-28 18:30:27.002', 0.0);")
            tdSql.execute(f"insert into ct1 values ('2022-05-01 18:30:17.003', 11.11);")
            tdSql.execute(f"insert into ct4 values ('2022-02-01 18:30:27.004', 11.11);")
            tdSql.execute(f"insert into ct1 values ('2022-05-01 18:30:07.005', 22.22);")
            tdSql.execute(f"insert into ct4 values ('2021-11-01 18:30:27.006', 22.22);")
            tdSql.execute(f"insert into ct1 values ('2022-05-01 18:29:27.007', 33.33);")
            tdSql.execute(f"insert into ct4 values ('2022-08-01 18:30:27.008', 33.33);")
            tdSql.execute(f"insert into ct1 values ('2022-05-01 18:20:27.009', 44.44);")
            tdSql.execute(f"insert into ct4 values ('2021-05-01 18:30:27.010', 44.44);")
            tdSql.execute(f"insert into ct1 values ('2022-05-01 18:21:27.011', 55.55);")
            tdSql.execute(f"insert into ct4 values ('2021-01-01 18:30:27.012', 55.55);")
            tdSql.execute(f"insert into ct1 values ('2022-05-01 18:22:27.013', 66.66);")
            tdSql.execute(f"insert into ct4 values ('2020-06-01 18:30:27.014', 66.66);")
            tdSql.execute(f"insert into ct1 values ('2022-05-01 18:28:37.015', 77.77);")
            tdSql.execute(f"insert into ct4 values ('2020-05-01 18:30:27.016', 77.77);")
            tdSql.execute(f"insert into ct1 values ('2022-05-01 18:29:17.017', 88.88);")
            tdSql.execute(f"insert into ct4 values ('2019-05-01 18:30:27.018', 88.88);")
            tdSql.execute(f"insert into ct1 values ('2022-05-01 18:30:20.019', 0);")
            tdSql.execute(
                f"insert into ct1 values ('2022-05-01 18:30:47.020', -99.99);"
            )
            tdSql.execute(f"insert into ct1 values ('2022-05-01 18:30:49.021', NULL);")
            tdSql.execute(
                f"insert into ct1 values ('2022-05-01 18:30:51.022', -99.99);"
            )
            tdSql.execute(f"insert into ct4 values ('2018-05-01 18:30:27.023', NULL) ;")
            tdSql.execute(f"insert into ct4 values ('2021-03-01 18:30:27.024', NULL) ;")
            tdSql.execute(f"insert into ct4 values ('2022-08-01 18:30:27.025', NULL) ;")

            tdLog.info(
                f"=============== select * from ct1 - merge memory and file - loop {reboot_max} - {reboot_cnt}"
            )
            tdSql.query(f"select * from ct1;")
            tdSql.checkRows(13)
            tdSql.checkData(0, 1, 44.440000000)
            tdSql.checkData(1, 1, 55.550000000)
            tdSql.checkData(2, 1, 66.660000000)
            tdSql.checkData(3, 1, 77.770000000)
            tdSql.checkData(4, 1, 88.880000000)
            tdSql.checkData(5, 1, 33.330000000)
            tdSql.checkData(6, 1, 22.220000000)
            tdSql.checkData(7, 1, 11.110000000)
            tdSql.checkData(8, 1, 0.000000000)
            tdSql.checkData(9, 1, 0.000000000)
            tdSql.checkData(10, 1, -99.990000000)
            tdSql.checkData(11, 1, None)
            tdSql.checkData(12, 1, -99.990000000)

            tdLog.info(
                f"=============== select * from ct4 - merge memory and file - loop {reboot_max} - {reboot_cnt}"
            )
            tdSql.query(f"select * from ct4;")
            tdSql.checkRows(12)
            tdSql.checkData(0, 1, None)
            tdSql.checkData(1, 1, 88.880000000)
            tdSql.checkData(2, 1, 77.770000000)
            tdSql.checkData(3, 1, 66.660000000)
            tdSql.checkData(4, 1, 55.550000000)
            tdSql.checkData(5, 1, None)
            tdSql.checkData(6, 1, 44.440000000)
            tdSql.checkData(7, 1, 22.220000000)
            tdSql.checkData(8, 1, 11.110000000)
            tdSql.checkData(9, 1, 0.000000000)
            tdSql.checkData(10, 1, 33.330000000)
            tdSql.checkData(11, 1, None)

            reboot_cnt = reboot_cnt + 1
            if reboot_cnt > reboot_max:
                reboot_and_check = 0
                tdLog.info(f"reboot_cnt {reboot_cnt} > reboot_max {reboot_max}")

    def CommitFile(self):
        tdLog.info(f"=============== step1")
        tdSql.prepare("d3", drop=True)
        tdSql.execute(f"use d3")
        tdSql.execute(
            f"create table d3.mt (ts timestamp, c000 int, c001 int, c002 int, c003 int, c004 int, c005 int, c006 int, c007 int, c008 int, c009 int, c010 int, c011 int, c012 int, c013 int, c014 int, c015 int, c016 int, c017 int, c018 int, c019 int, c020 int, c021 int, c022 int, c023 int, c024 int, c025 int, c026 int, c027 int, c028 int, c029 int, c030 int, c031 int, c032 int, c033 int, c034 int, c035 int, c036 int, c037 int, c038 int, c039 int, c040 int, c041 int, c042 int, c043 int, c044 int, c045 int, c046 int, c047 int, c048 int, c049 int, c050 int, c051 int, c052 int, c053 int, c054 int, c055 int, c056 int, c057 int, c058 int, c059 int, c060 int, c061 int, c062 int, c063 int, c064 int, c065 int, c066 int, c067 int, c068 int, c069 int, c070 int, c071 int, c072 int, c073 int, c074 int, c075 int, c076 int, c077 int, c078 int, c079 int, c080 int, c081 int, c082 int, c083 int, c084 int, c085 int, c086 int, c087 int, c088 int, c089 int, c090 int, c091 int, c092 int, c093 int, c094 int, c095 int, c096 int, c097 int, c098 int, c099 int, c100 int, c101 int, c102 int, c103 int, c104 int, c105 int, c106 int, c107 int, c108 int, c109 int, c110 int, c111 int, c112 int, c113 int, c114 int, c115 int, c116 int, c117 int, c118 int, c119 int, c120 int, c121 int, c122 int, c123 int, c124 int, c125 int, c126 int, c127 int, c128 int, c129 int, c130 int, c131 int, c132 int, c133 int, c134 int, c135 int, c136 int, c137 int, c138 int, c139 int, c140 int, c141 int, c142 int, c143 int, c144 int, c145 int, c146 int, c147 int, c148 int, c149 int, c150 int, c151 int, c152 int, c153 int, c154 int, c155 int, c156 int, c157 int, c158 int, c159 int, c160 int, c161 int, c162 int, c163 int, c164 int, c165 int, c166 int, c167 int, c168 int, c169 int, c170 int, c171 int, c172 int, c173 int, c174 int, c175 int, c176 int, c177 int, c178 int, c179 int, c180 int, c181 int, c182 int, c183 int, c184 int, c185 int, c186 int, c187 int, c188 int, c189 int, c190 int, c191 int, c192 int, c193 int, c194 int, c195 int, c196 int, c197 int, c198 int, c199 int, c200 int, c201 int, c202 int, c203 int, c204 int, c205 int, c206 int, c207 int, c208 int, c209 int, c210 int, c211 int, c212 int, c213 int, c214 int, c215 int, c216 int, c217 int, c218 int, c219 int, c220 int, c221 int, c222 int, c223 int, c224 int, c225 int, c226 int, c227 int, c228 int, c229 int, c230 int, c231 int, c232 int, c233 int, c234 int, c235 int, c236 int, c237 int, c238 int, c239 int, c240 int, c241 int, c242 int, c243 int, c244 int, c245 int, c246 int, c247 int, c248 int) tags(a int, b smallint, c binary(20), d float, e double, f bigint)"
        )
        tdSql.execute(f"create table d3.t1 using d3.mt tags(1, 2, '3', 4, 5, 6)")

        tdSql.query(f"show tables")
        tdSql.checkRows(1)

        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step2")
        tdSql.execute(
            f"insert into d3.t1 values (now -300d,0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 );"
        )
        tdSql.execute(
            f"insert into d3.t1 values (now-200d,1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1  );"
        )
        tdSql.execute(
            f"insert into d3.t1 values (now-150d,2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2  );"
        )
        tdSql.execute(
            f"insert into d3.t1 values (now-100d,3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3  );"
        )
        tdSql.execute(
            f"insert into d3.t1 values (now-50d,4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4  );"
        )
        tdSql.execute(
            f"insert into d3.t1 values (now-20d,5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5  );"
        )
        tdSql.execute(
            f"insert into d3.t1 values (now-10d,6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6  );"
        )
        tdSql.execute(
            f"insert into d3.t1 values (now-1d,7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 );"
        )
        tdSql.execute(
            f"insert into d3.t1 values (now,8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8  );"
        )
        tdSql.execute(
            f"insert into d3.t1 values (now+1d,9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9  );"
        )

        tdLog.info(f"=============== step3")
        tdSql.query(f"select * from d3.mt")
        tdSql.checkRows(10)

        tdSql.query(f"select * from d3.mt where c001 = 1")
        tdSql.checkRows(1)

        tdSql.query(f"select * from d3.mt where c002 = 2 and c003 = 2")
        tdSql.checkRows(1)

        tdSql.query(
            f"select count(c001), count(c248), avg(c001), avg(c248), sum(c001), max(c001), min(c248), avg(c235), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*) from d3.mt"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)}  {tdSql.getData(0,1)}  {tdSql.getData(0,2)}  {tdSql.getData(0,3)}  {tdSql.getData(0,4)}  {tdSql.getData(0,5)}  {tdSql.getData(0,6)}  {tdSql.getData(0,7)}  {tdSql.getData(0,8)} {tdSql.getData(0,9)}"
        )
        tdSql.checkData(0, 0, 10)

        tdSql.checkData(0, 1, 10)

        tdSql.checkData(0, 2, 4.500000000)

        tdSql.checkData(0, 3, 4.500000000)

        tdSql.checkData(0, 4, 45)

        tdSql.checkData(0, 5, 9)

        tdSql.checkData(0, 6, 0)

        tdSql.checkData(0, 7, 4.500000000)

        tdSql.checkData(0, 8, 10)

        tdSql.checkData(0, 9, 10)

        tdLog.info(f"=============== step4")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from d3.mt")
        tdSql.checkRows(10)

        tdSql.query(f"select * from d3.mt where c001 = 1")
        tdSql.checkRows(1)

        tdSql.query(f"select * from d3.mt where c002 = 2 and c003 = 2")
        tdSql.checkRows(1)

        tdSql.query(f"select count(*) from d3.mt")
        tdSql.checkData(0, 0, 10)

        tdSql.query(
            f"select count(c001), count(c248), avg(c001), avg(c248), sum(c001), max(c001), min(c248), avg(c128), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*) from d3.mt"
        )
        tdLog.info(
            f"{tdSql.getData(0,0)}  {tdSql.getData(0,1)}  {tdSql.getData(0,2)}  {tdSql.getData(0,3)}  {tdSql.getData(0,4)}  {tdSql.getData(0,5)}  {tdSql.getData(0,6)}  {tdSql.getData(0,7)}  {tdSql.getData(0,8)} {tdSql.getData(0,9)}"
        )
        tdSql.checkData(0, 0, 10)

        tdSql.checkData(0, 1, 10)

        tdSql.checkData(0, 2, 4.500000000)

        tdSql.checkData(0, 3, 4.500000000)

        tdSql.checkData(0, 4, 45)

        tdSql.checkData(0, 5, 9)

        tdSql.checkData(0, 6, 0)

        tdSql.checkData(0, 7, 4.500000000)

        tdSql.checkData(0, 8, 10)

        tdSql.checkData(0, 9, 10)

    def Block1File(self):
        i = 0
        dbPrefix = "tb_1f_db"
        tbPrefix = "tb_1f_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1")
        tdSql.prepare(dbname=db)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, speed int)")

        # commit to file will trigger if insert 82 rows

        N = 82

        tdLog.info(f"=============== step 1")
        x = N
        y = N / 2
        while x > y:
            ms = str(int(x)) + "m"
            tdSql.execute(f"insert into {tb} values (now - {ms} , -{x} )")
            x = x - 1

        tdSql.query(f"select * from {tb}")
        tdLog.info(f"sql select * from {tb} -> {tdSql.getRows()}) points")
        tdSql.checkRows(y)

        x = N / 2
        y = N
        while x < y:
            ms = str(int(x)) + "m"
            tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
            x = x + 1

        tdSql.query(f"select * from {tb}")
        tdLog.info(f"sql select * from {tb} -> {tdSql.getRows()}) points")
        tdSql.checkRows(N)

        tdLog.info(f"=============== step 2")

        R = 4
        x = N * 2
        y = N * R
        expect = y + N
        y = y + x
        while x < y:
            ms = str(int(x)) + "m"
            tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
            x = x + 1

        tdSql.query(f"select * from {tb}")
        tdLog.info(f"sql select * from {tb} -> {tdSql.getRows()}) points")
        tdSql.checkRows(expect)

        tdLog.info(f"=============== step 3")

        N1 = N + 1
        result1 = N / 2
        result2 = N
        step = str(N1) + "m"

        start1 = "now-" + str(step)
        start2 = "now"
        start3 = "now+" + str(step)
        end1 = "now-" + str(step)
        end2 = "now"
        end3 = "now+" + str(step)

        tdSql.query(f"select * from {tb} where ts < {start1} and ts > {end1}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start1} and ts > {end2}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start1} and ts > {end3}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start2} and ts > {end1}")
        tdLog.info(
            f"select * from {tb} where ts < {start2} and ts > {end1} ->  {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result1)

        tdSql.query(f"select * from {tb} where ts < {start2} and ts > {end2}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start2} and ts > {end3}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start3} and ts > {end1}")
        tdLog.info(
            f"sql select * from {tb} where ts < {start3} and ts > {end1} -> {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result2)

        tdSql.query(f"select * from {tb} where ts < {start3} and ts > {end2}")
        tdLog.info(
            f"sql select * from {tb} where ts < {start3} and ts > {end2} -> {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result1)

        tdSql.query(f"select * from {tb} where ts < {start3} and ts > {end3}")
        tdSql.checkRows(0)

        tdLog.info(f"================= order by ts desc")

        tdSql.query(
            f"select * from {tb} where ts < {start1} and ts > {end1} order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start1} and ts > {end2} order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start1} and ts > {end3} order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start2} and ts > {end1} order by ts desc"
        )
        tdLog.info(
            f"select * from {tb} where ts < {start2} and ts > {end1}  order by ts desc  ->  {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result1)

        tdSql.query(
            f"select * from {tb} where ts < {start2} and ts > {end2}  order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start2} and ts > {end3} order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start3} and ts > {end1} order by ts desc"
        )
        tdLog.info(
            f"sql select * from {tb} where ts < {start3} and ts > {end1}  order by ts desc  -> {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result2)

        tdSql.query(
            f"select * from {tb} where ts < {start3} and ts > {end2} order by ts desc"
        )
        tdLog.info(
            f"sql select * from {tb} where ts < {start3} and ts > {end2}  order by ts desc  -> {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result1)

        tdSql.query(
            f"select * from {tb} where ts < {start3} and ts > {end3}   order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def Block1Mem(self):
        i = 0
        dbPrefix = "tb_1m_db"
        tbPrefix = "tb_1m_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1")
        tdSql.prepare(dbname=db)
        tdSql.execute(f"use {db}")

        tdSql.execute(f"create table {tb} (ts timestamp, speed int)")

        # commit to file will trigger if insert 82 rows

        N = 82

        tdLog.info(f"=============== step 1")
        x = N
        y = N / 2
        while x > y:
            z = x * 60000
            ms = 1601481600000 - z
            tdSql.execute(f"insert into {tb} values ({ms} , -{x} )")
            x = x - 1

        tdSql.query(f"select * from {tb}")
        tdLog.info(f"sql select * from {tb} -> {tdSql.getRows()}) points")
        tdSql.checkRows(y)

        x = N / 2
        y = N
        while x < y:
            z = int(x) * 60000
            ms = 1601481600000 + z

            tdSql.execute(f"insert into {tb} values ({ms} , {x} )")
            x = x + 1

        tdSql.query(f"select * from {tb}")
        tdLog.info(f"sql select * from {tb} -> {tdSql.getRows()}) points")
        tdSql.checkRows(N)

        tdLog.info(f"=============== step 2")

        N1 = N + 1
        result1 = N / 2
        result2 = N
        step = N1 * 60000

        start1 = 1601481600000 - step
        start2 = 1601481600000
        start3 = 1601481600000 + step
        end1 = 1601481600000 - step
        end2 = 1601481600000
        end3 = 1601481600000 + step

        tdSql.query(f"select * from {tb} where ts < {start1} and ts > {end1}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start1} and ts > {end2}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start1} and ts > {end3}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start2} and ts > {end1}")
        tdLog.info(
            f"select * from {tb} where ts < {start2} and ts > {end1} ->  {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result1)

        tdSql.query(f"select * from {tb} where ts < {start2} and ts > {end2}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start2} and ts > {end3}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start3} and ts > {end1}")
        tdLog.info(
            f"sql select * from {tb} where ts < {start3} and ts > {end1} -> {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result2)

        tdSql.query(f"select * from {tb} where ts < {start3} and ts > {end2}")
        tdLog.info(
            f"sql select * from {tb} where ts < {start3} and ts > {end2} -> {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result1)

        tdSql.query(f"select * from {tb} where ts < {start3} and ts > {end3}")
        tdSql.checkRows(0)

        tdLog.info(f"================= order by ts desc")

        tdSql.query(
            f"select * from {tb} where ts < {start1} and ts > {end1} order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start1} and ts > {end2} order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start1} and ts > {end3} order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start2} and ts > {end1} order by ts desc"
        )
        tdLog.info(
            f"select * from {tb} where ts < {start2} and ts > {end1}  order by ts desc  ->  {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result1)

        tdSql.query(
            f"select * from {tb} where ts < {start2} and ts > {end2}  order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start2} and ts > {end3} order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start3} and ts > {end1} order by ts desc"
        )
        tdLog.info(
            f"sql select * from {tb} where ts < {start3} and ts > {end1}  order by ts desc  -> {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result2)

        tdSql.query(
            f"select * from {tb} where ts < {start3} and ts > {end2} order by ts desc"
        )
        tdLog.info(
            f"sql select * from {tb} where ts < {start3} and ts > {end2}  order by ts desc  -> {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result1)

        tdSql.query(
            f"select * from {tb} where ts < {start3} and ts > {end3}   order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def Block2File(self):
        i = 0
        dbPrefix = "tb_2f_db"
        tbPrefix = "tb_2f_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1")
        tdSql.prepare(dbname=db)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, speed int)")

        # commit to file will trigger if insert 82 rows
        N = 82

        tdLog.info(f"=============== step 1")
        x = N * 2
        y = N
        expect = N
        while x > y:
            ms = str(x) + "m"
            xt = "-" + str(x)
            tdSql.execute(f"insert into {tb} values (now - {ms} , {xt} )")
            x = x - 1

        tdSql.query(f"select * from {tb}")
        tdLog.info(f"sql select * from {tb} -> {tdSql.getRows()}) points")
        tdSql.checkRows(expect)

        x = N
        y = N * 2
        expect = N * 2
        while x < y:
            ms = str(x) + "m"
            tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
            x = x + 1

        tdSql.query(f"select * from {tb}")
        tdLog.info(f"sql select * from {tb} -> {tdSql.getRows()}) points")
        tdSql.checkRows(expect)

        tdLog.info(f"=============== step 2")

        R = 4
        y = N * R

        expect = y + N
        expect = expect + N

        x = N * 3
        y = y + x

        while x < y:
            ms = str(x) + "m"
            tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
            x = x + 1

        tdSql.query(f"select * from {tb}")
        tdLog.info(f"sql select * from {tb} -> {tdSql.getRows()}) points")
        tdSql.checkRows(expect)

        tdLog.info(f"=============== step 2")

        N2 = N
        result1 = N
        result2 = 2 * N
        N1 = result2 + 1
        step = str(N1) + "m"

        start1 = "now-" + str(step)
        start2 = "now"
        start3 = "now+" + str(step)
        end1 = "now-" + str(step)
        end2 = "now"
        end3 = "now+" + str(step)

        tdSql.query(f"select * from {tb} where ts < {start1} and ts > {end1}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start1} and ts > {end2}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start1} and ts > {end3}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start2} and ts > {end1}")
        tdLog.info(
            f"select * from {tb} where ts < {start2} and ts > {end1} ->  {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result1)

        tdSql.query(f"select * from {tb} where ts < {start2} and ts > {end2}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start2} and ts > {end3}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start3} and ts > {end1}")
        tdLog.info(
            f"sql select * from {tb} where ts < {start3} and ts > {end1} -> {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result2)

        tdSql.query(f"select * from {tb} where ts < {start3} and ts > {end2}")
        tdLog.info(
            f"sql select * from {tb} where ts < {start3} and ts > {end2} -> {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result1)

        tdSql.query(f"select * from {tb} where ts < {start3} and ts > {end3}")
        tdSql.checkRows(0)

        tdLog.info(f"================= order by ts desc")

        tdSql.query(
            f"select * from {tb} where ts < {start1} and ts > {end1} order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start1} and ts > {end2} order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start1} and ts > {end3} order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start2} and ts > {end1} order by ts desc"
        )
        tdLog.info(
            f"select * from {tb} where ts < {start2} and ts > {end1}  order by ts desc  ->  {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result1)

        tdSql.query(
            f"select * from {tb} where ts < {start2} and ts > {end2}  order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start2} and ts > {end3} order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start3} and ts > {end1} order by ts desc"
        )
        tdLog.info(
            f"sql select * from {tb} where ts < {start3} and ts > {end1}  order by ts desc  -> {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result2)

        tdSql.query(
            f"select * from {tb} where ts < {start3} and ts > {end2} order by ts desc"
        )
        tdLog.info(
            f"sql select * from {tb} where ts < {start3} and ts > {end2}  order by ts desc  -> {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result1)

        tdSql.query(
            f"select * from {tb} where ts < {start3} and ts > {end3}   order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def Block2Mem(self):
        i = 0
        dbPrefix = "tb_2m_db"
        tbPrefix = "tb_2m_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1")
        tdSql.error(f"drop database {db}")
        tdSql.prepare(dbname=db)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, speed int)")

        N = 82

        x = N * 2
        y = N
        while x > y:
            ms = str(x) + "m"
            xt = "-" + str(x)
            tdSql.execute(f"insert into {tb} values (now - {ms} , {xt} )")
            x = x - 1

        tdSql.query(f"select * from {tb}")
        tdLog.info(f"sql select * from {tb} -> {tdSql.getRows()}) points")
        tdSql.checkRows(y)

        x = N
        y = N * 2
        expect = N * 2
        while x < y:
            ms = str(x) + "m"
            tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
            x = x + 1

        tdSql.query(f"select * from {tb}")
        tdLog.info(f"sql select * from {tb} -> {tdSql.getRows()}) points")
        tdSql.checkRows(expect)

        tdLog.info(f"=============== step 2")

        result1 = N
        result2 = N * 2

        N1 = result2 + 1
        step = str(N1) + "m"

        start1 = "now-" + str(step)
        start2 = "now"
        start3 = "now+" + str(step)
        end1 = "now-" + str(step)
        end2 = "now"
        end3 = "now+" + str(step)

        tdSql.query(f"select * from {tb} where ts < {start1} and ts > {end1}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start1} and ts > {end2}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start1} and ts > {end3}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start2} and ts > {end1}")
        tdLog.info(
            f"select * from {tb} where ts < {start2} and ts > {end1} ->  {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result1)

        tdSql.query(f"select * from {tb} where ts < {start2} and ts > {end2}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start2} and ts > {end3}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start3} and ts > {end1}")
        tdLog.info(
            f"sql select * from {tb} where ts < {start3} and ts > {end1} -> {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result2)

        tdSql.query(f"select * from {tb} where ts < {start3} and ts > {end2}")
        tdLog.info(
            f"sql select * from {tb} where ts < {start3} and ts > {end2} -> {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result1)

        tdSql.query(f"select * from {tb} where ts < {start3} and ts > {end3}")
        tdSql.checkRows(0)

        tdLog.info(f"================= order by ts desc")

        tdSql.query(
            f"select * from {tb} where ts < {start1} and ts > {end1} order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start1} and ts > {end2} order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start1} and ts > {end3} order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start2} and ts > {end1} order by ts desc"
        )
        tdLog.info(
            f"select * from {tb} where ts < {start2} and ts > {end1}  order by ts desc  ->  {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result1)

        tdSql.query(
            f"select * from {tb} where ts < {start2} and ts > {end2}  order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start2} and ts > {end3} order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start3} and ts > {end1} order by ts desc"
        )
        tdLog.info(
            f"sql select * from {tb} where ts < {start3} and ts > {end1}  order by ts desc  -> {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result2)

        tdSql.query(
            f"select * from {tb} where ts < {start3} and ts > {end2} order by ts desc"
        )
        tdLog.info(
            f"sql select * from {tb} where ts < {start3} and ts > {end2}  order by ts desc  -> {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result1)

        tdSql.query(
            f"select * from {tb} where ts < {start3} and ts > {end3}   order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def FileMem(self):
        i = 0
        dbPrefix = "tb_fm_db"
        tbPrefix = "tb_fm_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1")
        tdSql.prepare(dbname=db)
        tdSql.execute(f"use {db}")

        tdSql.execute(f"create table {tb} (ts timestamp, speed int)")

        # commit to file will trigger if insert 82 rows

        N = 82

        x = N * 2
        y = N
        expect = y
        while x > y:
            ms = str(x) + "m"
            xt = "-" + str(x)
            tdSql.execute(f"insert into {tb} values (now - {ms} , {xt} )")
            x = x - 1

        tdSql.query(f"select * from {tb}")
        tdLog.info(f"sql select * from {tb} -> {tdSql.getRows()}) points")
        tdSql.checkRows(expect)

        x = N
        y = N * 2
        expect = N * 2
        while x < y:
            ms = str(x) + "m"
            tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
            x = x + 1

        tdSql.query(f"select * from {tb}")
        tdLog.info(f"sql select * from {tb} -> {tdSql.getRows()}) points")
        tdSql.checkRows(expect)

        R = 4
        R = R - 1

        y = N * R
        expect = y + N
        expect = expect + N

        x = N * 3
        y = y + x
        while x < y:
            ms = str(x) + "m"
            tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
            x = x + 1

        tdSql.query(f"select * from {tb}")
        tdLog.info(f"sql select * from {tb} -> {tdSql.getRows()}) points")
        tdSql.checkRows(expect)

        tdLog.info(f"=============== step 2")

        result1 = N
        result2 = N * 2
        N1 = result2 + 1
        step = str(N1) + "m"

        start1 = "now-" + str(step)
        start2 = "now"
        start3 = "now+" + str(step)
        end1 = "now-" + str(step)
        end2 = "now"
        end3 = "now+" + str(step)

        tdSql.query(f"select * from {tb} where ts < {start1} and ts > {end1}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start1} and ts > {end2}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start1} and ts > {end3}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start2} and ts > {end1}")
        tdLog.info(
            f"select * from {tb} where ts < {start2} and ts > {end1} ->  {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result1)

        tdSql.query(f"select * from {tb} where ts < {start2} and ts > {end2}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start2} and ts > {end3}")
        tdSql.checkRows(0)

        tdSql.query(f"select * from {tb} where ts < {start3} and ts > {end1}")
        tdLog.info(
            f"sql select * from {tb} where ts < {start3} and ts > {end1} -> {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result2)

        tdSql.query(f"select * from {tb} where ts < {start3} and ts > {end2}")
        tdLog.info(
            f"sql select * from {tb} where ts < {start3} and ts > {end2} -> {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result1)

        tdSql.query(f"select * from {tb} where ts < {start3} and ts > {end3}")
        tdSql.checkRows(0)

        tdLog.info(f"================= order by ts desc")

        tdSql.query(
            f"select * from {tb} where ts < {start1} and ts > {end1} order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start1} and ts > {end2} order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start1} and ts > {end3} order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start2} and ts > {end1} order by ts desc"
        )
        tdLog.info(
            f"select * from {tb} where ts < {start2} and ts > {end1}  order by ts desc  ->  {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result1)

        tdSql.query(
            f"select * from {tb} where ts < {start2} and ts > {end2}  order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start2} and ts > {end3} order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from {tb} where ts < {start3} and ts > {end1} order by ts desc"
        )
        tdLog.info(
            f"sql select * from {tb} where ts < {start3} and ts > {end1}  order by ts desc  -> {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result2)

        tdSql.query(
            f"select * from {tb} where ts < {start3} and ts > {end2} order by ts desc"
        )
        tdLog.info(
            f"sql select * from {tb} where ts < {start3} and ts > {end2}  order by ts desc  -> {tdSql.getRows()}) points"
        )
        tdSql.checkRows(result1)

        tdSql.query(
            f"select * from {tb} where ts < {start3} and ts > {end3}  order by ts desc"
        )
        tdSql.checkRows(0)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def MultiFile(self):
        i = 0
        dbPrefix = "tb_mf_db"
        tbPrefix = "tb_mf_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1")
        tdSql.prepare(dbname=db)
        tdSql.execute(f"use {db}")

        tdSql.execute(f"create table {tb} (ts timestamp, speed int)")

        N = 20000

        x = 0

        while x < N:
            ms = str(x) + "s"
            # print insert into $tb values (now + $ms , $x )
            tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
            x = x + 1

        tdSql.query(f"select * from {tb}")
        tdLog.info(f"{tdSql.getRows()}) points data are retrieved -> exepct {N} rows")
        tdSql.checkAssert(tdSql.getRows() >= N)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def Disk(self):
        tdLog.info(f"======================== dnode1 start")
        dbPrefix = "d_db"
        tbPrefix = "d_tb"
        mtPrefix = "d_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol int)")

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")
            x = 0
            while x < rowNum:
                val = x * 60000
                ms = 1519833600000 + val
                tdSql.execute(f"insert into {tb} values ({ms} , {x} )")
                x = x + 1
            i = i + 1

        tdSql.query(f"show vgroups")
        tdSql.checkRows(2)

        tdSql.query(f"select count(tbcol) from {mt}")
        tdLog.info(f"select count(tbcol) from {mt} ===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, totalNum)

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.execute(f"use {db}")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select count(tbcol) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, rowNum)

        tdSql.query(f"select count(tbcol) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, rowNum)

        tdLog.info(f"=============== step3")
        tdSql.query(f"select count(tbcol) from {tb} where ts <= 1519833840000")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 5)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select count(tbcol) as b from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, rowNum)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select count(tbcol) as b from {tb} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select count(tbcol) as b from {tb} interval(1d)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, rowNum)

        tdLog.info(f"=============== step6")
        tdSql.query(
            f"select count(tbcol) as b from {tb} where ts <= 1519833840000 interval(1m)"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1)

        tdSql.checkRows(5)

        tdLog.info(f"=============== step7")
        tdSql.query(f"select count(*) from {mt}")
        tdLog.info(f"select count(*) from {mt} ===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, totalNum)

        tdLog.info(
            f"==========> block opt will cause this crash, table scan need to fix this during plan gen ===============>"
        )
        tdSql.query(f"select count(tbcol) from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, totalNum)

        tdLog.info(f"=============== step8")
        tdSql.query(f"select count(tbcol) as c from {mt} where ts <= 1519833840000")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 50)

        tdSql.query(f"select count(tbcol) as c from {mt} where tgcol < 5")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 100)

        tdSql.query(
            f"select count(tbcol) as c from {mt} where tgcol < 5 and ts <= 1519833840000"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step9")
        tdSql.query(f"select count(tbcol) as b from {mt} interval(1m)")
        tdSql.checkData(0, 0, 10)

        tdSql.query(f"select count(tbcol) as b from {mt} interval(1d)")
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step10")
        tdLog.info(f"select count(tbcol) as b from {mt} group by tgcol")
        tdSql.query(f"select count(tbcol) as b from {mt} group by tgcol")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, rowNum)

        tdSql.checkRows(tbNum)

        tdLog.info(f"=============== step11")
        tdSql.query(
            f"select count(tbcol) as b from {mt}  where ts <= 1519833840000 partition by tgcol interval(1m)"
        )
        tdSql.checkData(0, 0, 1)

        tdSql.checkRows(50)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def ParserCommit(self):
        dbPrefix = "sc_db"
        tbPrefix = "sc_tb"
        stbPrefix = "sc_stb"
        tbNum = 10
        rowNum = 1000
        totalNum = tbNum * rowNum
        loops = 5
        log = 1
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== commit.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"create database {db} maxrows 255")
        tdLog.info(f"====== create tables")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {stb} (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 smallint, c6 tinyint, c7 bool, c8 binary(10), c9 nchar(10)) tags(t1 int)"
        )

        i = 0
        ts = ts0
        halfNum = tbNum / 2
        while i < halfNum:
            tbId = i + halfNum
            tb = tbPrefix + str(i)
            tb1 = tbPrefix + str(int(tbId))
            tdSql.execute(f"create table {tb} using {stb} tags( {i} )")
            tdSql.execute(f"create table {tb1} using {stb} tags( {tbId} )")

            x = 0
            while x < rowNum:
                xs = x * delta
                ts = int(ts0 + xs)
                c = x % 10
                binary = "'binary" + str(int(c)) + "'"
                nchar = "'nchar" + str(int(c)) + "'"
                tdSql.execute(
                    f"insert into {tb} values ( {ts} , {c} , {c} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} )  {tb1} values ( {ts} , {c} , NULL , {c} , NULL , {c} , {c} , true, {binary} , {nchar} )"
                )
                x = x + 1
            i = i + 1

        tdLog.info(f"====== tables created")

        tdSql.execute(f"use {db}")
        ##### select from table
        tdLog.info(f"====== select from table and check num of rows returned")
        loop = 1
        i = 0
        while loop <= loops:
            tdLog.info(f"repeat = {loop}")
            while i < 10:
                tdSql.query(f"select count(*) from {stb} where t1 = {i}")
                tdSql.checkData(0, 0, rowNum)
                i = i + 1

            tdSql.query(f"select count(*) from {stb}")
            tdSql.checkData(0, 0, totalNum)
            loop = loop + 1

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        tdLog.info(f"====== select from table and check num of rows returned")
        tdSql.execute(f"use {db}")
        loop = 1
        i = 0
        while loop <= loops:
            tdLog.info(f"repeat = {loop}")
            while i < 10:
                tdSql.query(f"select count(*) from {stb} where t1 = {i}")
                tdSql.checkData(0, 0, rowNum)
                i = i + 1
            tdSql.query(f"select count(*) from {stb}")
            tdSql.checkData(0, 0, totalNum)
            loop = loop + 1

    def WalKill(self):
        tdLog.info(f"============== deploy")
        tdSql.execute(f"create database d1")
        tdSql.execute(f"use d1")

        tdSql.execute(f"create table t1 (ts timestamp, i int)")
        tdSql.execute(f"insert into t1 values(now, 1);")

        tdLog.info(f"===============  step3")
        tdSql.query(f"select * from t1;")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkRows(1)

        sc.dnodeForceStop(1)

        tdLog.info(f"===============  step4")
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.query(f"select * from t1;")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkRows(1)

        sc.dnodeForceStop(1)

        tdLog.info(f"===============  step5")
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)
        tdSql.query(f"select * from t1;")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkRows(1)

        sc.dnodeForceStop(1)

        tdLog.info(f"===============  step6")
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)
        tdSql.query(f"select * from t1;")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkRows(1)

        sc.dnodeForceStop(1)

        tdLog.info(f"===============  step7")
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)
        tdSql.query(f"select * from t1;")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkRows(1)

        sc.dnodeForceStop(1)

        tdLog.info(f"===============  step8")
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)
        tdSql.query(f"select * from t1;")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkRows(1)
