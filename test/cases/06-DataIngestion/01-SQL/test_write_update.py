from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestWriteUpdate:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_write_update(self):
        """Write update

        1. Update data in memory
        2. Update data in files
        3. Write to update multiple records at once
        4. Update data using NULL values
        5. Restart the dnode
        6. Check data integrity

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-12 Simon Guan Migrated from tsim/insert/update0.sim
            - 2025-8-12 Simon Guan Migrated from tsim/insert/update1_sort_merge.sim
            - 2025-8-12 Simon Guan Migrated from tsim/insert/update2.sim

        """
        
        self.Update0()
        tdStream.dropAllStreamsAndDbs()
        self.Update1()
        tdStream.dropAllStreamsAndDbs()
        self.Update2()
        tdStream.dropAllStreamsAndDbs()

    def Update0(self):
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database d0 keep 365000d,365000d,365000d")
        tdSql.execute(f"use d0")

        tdLog.info(f"=============== create super table")
        tdSql.execute(
            f"create table if not exists stb (ts timestamp, c1 int) tags (city binary(20),district binary(20));"
        )

        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== create child table")
        tdSql.execute(f'create table ct1 using stb tags("BeiJing", "ChaoYang")')
        tdSql.execute(f'create table ct2 using stb tags("BeiJing", "HaiDian")')

        tdSql.query(f"show tables")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step3-1 insert records into ct1")
        tdSql.execute(f"insert into ct1 values('2022-05-03 16:59:00.010', 10);")
        tdSql.execute(f"insert into ct1 values('2022-05-03 16:59:00.011', 11);")
        tdSql.execute(f"insert into ct1 values('2022-05-03 16:59:00.016', 16);")
        tdSql.execute(f"insert into ct1 values('2022-05-03 16:59:00.016', 17);")
        tdSql.execute(f"insert into ct1 values('2022-05-03 16:59:00.020', 20);")
        tdSql.execute(f"insert into ct1 values('2022-05-03 16:59:00.016', 18);")
        tdSql.execute(f"insert into ct1 values('2022-05-03 16:59:00.021', 21);")
        tdSql.execute(f"insert into ct1 values('2022-05-03 16:59:00.022', 22);")

        tdLog.info(f"=============== step3-1 query records of ct1 from memory")
        tdSql.query(f"select * from ct1;")
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(2, 1, 18)
        tdSql.checkData(5, 1, 22)

        tdLog.info(f"=============== step3-1 insert records into ct2")
        tdSql.execute(
            f"insert into ct2 values('2022-03-02 16:59:00.010', 1),('2022-03-02 16:59:00.010',11),('2022-04-01 16:59:00.011',2),('2022-04-01 16:59:00.011',5),('2022-03-06 16:59:00.013',7);"
        )
        tdSql.execute(
            f"insert into ct2 values('2022-03-02 16:59:00.010', 3),('2022-03-02 16:59:00.010',33),('2022-04-01 16:59:00.011',4),('2022-04-01 16:59:00.011',6),('2022-03-06 16:59:00.013',8);"
        )
        tdSql.execute(
            f"insert into ct2 values('2022-03-02 16:59:00.010', 103),('2022-03-02 16:59:00.010',303),('2022-04-01 16:59:00.011',40),('2022-04-01 16:59:00.011',60),('2022-03-06 16:59:00.013',80);"
        )

        tdLog.info(f"=============== step3-1 query records of ct2 from memory")
        tdSql.query(f"select * from ct2;")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 303)
        tdSql.checkData(1, 1, 80)
        tdSql.checkData(2, 1, 60)

        # ==================== reboot to trigger commit data to file
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=============== step3-2 query records of ct1 from file")
        tdSql.query(f"select * from ct1;")
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(2, 1, 18)
        tdSql.checkData(5, 1, 22)

        tdLog.info(f"=============== step3-2 query records of ct2 from file")
        tdSql.query(f"select * from ct2;")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 303)
        tdSql.checkData(1, 1, 80)
        tdSql.checkData(2, 1, 60)

        tdLog.info(
            f"=============== step3-3 query records of ct1 from memory and file(merge)"
        )
        tdSql.execute(f"insert into ct1 values('2022-05-03 16:59:00.010', 100);")
        tdSql.execute(f"insert into ct1 values('2022-05-03 16:59:00.022', 200);")
        tdSql.execute(f"insert into ct1 values('2022-05-03 16:59:00.016', 160);")

        tdSql.query(f"select * from ct1;")
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, 100)
        tdSql.checkData(2, 1, 160)
        tdSql.checkData(5, 1, 200)

        tdLog.info(
            f"=============== step3-3 query records of ct2 from memory and file(merge)"
        )
        tdSql.execute(f"insert into ct2(ts) values('2022-04-02 16:59:00.016');")
        tdSql.execute(f"insert into ct2 values('2022-03-06 16:59:00.013', NULL);")
        tdSql.execute(f"insert into ct2 values('2022-03-01 16:59:00.016', 10);")
        tdSql.execute(f"insert into ct2(ts) values('2022-04-01 16:59:00.011');")
        tdSql.query(f"select * from ct2;")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(1, 1, 303)
        tdSql.checkData(2, 1, None)
        tdSql.checkData(3, 1, 60)
        tdSql.checkData(4, 1, None)

    def Update1(self):
        tdLog.info(
            f"=============== test case: merge duplicated rows in taosc and taosd"
        )
        tdLog.info(f"=============== create database")
        tdSql.execute(f"drop database if exists d0")
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
        tdSql.execute(f'create table ct3 using stb tags("BeiJing", "PingGu")')
        tdSql.execute(f'create table ct4 using stb tags("BeiJing", "YanQing")')

        tdSql.query(f"show tables")
        tdSql.checkRows(4)

        tdLog.info(f"=============== step 1 insert records into ct1 - taosd merge")
        tdSql.execute(
            f"insert into ct1(ts,c1,c2) values('2022-05-03 16:59:00.010', 10, 20);"
        )
        tdSql.execute(
            f"insert into ct1(ts,c1,c2,c3,c4) values('2022-05-03 16:59:00.011', 11, NULL, 'binary', 'nchar');"
        )
        tdSql.execute(
            f"insert into ct1 values('2022-05-03 16:59:00.016', 16, NULL, NULL, 'nchar', NULL);"
        )
        tdSql.execute(
            f"insert into ct1 values('2022-05-03 16:59:00.016', 17, NULL, NULL, 'nchar', 170);"
        )
        tdSql.execute(
            f"insert into ct1 values('2022-05-03 16:59:00.020', 20, NULL, NULL, 'nchar', 200);"
        )
        tdSql.execute(
            f"insert into ct1 values('2022-05-03 16:59:00.016', 18, NULL, NULL, 'nchar', 180);"
        )
        tdSql.execute(
            f"insert into ct1 values('2022-05-03 16:59:00.021', 21, NULL, NULL, 'nchar', 210);"
        )
        tdSql.execute(
            f"insert into ct1 values('2022-05-03 16:59:00.022', 22, NULL, NULL, 'nchar', 220);"
        )

        tdLog.info(
            f"=============== step 2 insert records into ct1/ct2 - taosc merge for 2022-05-03 16:59:00.010"
        )
        tdSql.execute(
            f"insert into ct1(ts,c1,c2) values('2022-05-03 16:59:00.010', 10,10), ('2022-05-03 16:59:00.010',20,10.0), ('2022-05-03 16:59:00.010',30,NULL) ct2(ts,c1) values('2022-05-03 16:59:00.010',10), ('2022-05-03 16:59:00.010',20) ct1(ts,c2) values('2022-05-03 16:59:00.010',10), ('2022-05-03 16:59:00.010',100) ct1(ts,c3) values('2022-05-03 16:59:00.010','bin1'), ('2022-05-03 16:59:00.010','bin2') ct1(ts,c4,c5) values('2022-05-03 16:59:00.010',NULL,NULL), ('2022-05-03 16:59:00.010','nchar4',1000.01) ct2(ts,c2,c3,c4,c5) values('2022-05-03 16:59:00.010',20,'xkl','zxc',10);"
        )

        tdLog.info(f"=============== step 3 insert records into ct3")
        tdSql.execute(
            f"insert into ct3(ts,c1,c5) values('2022-05-03 16:59:00.020', 10,10);"
        )
        tdSql.execute(
            f"insert into ct3(ts,c1,c5) values('2022-05-03 16:59:00.021', 10,10), ('2022-05-03 16:59:00.021',20,20.0);"
        )
        tdSql.execute(
            f"insert into ct3(ts,c1,c5) values('2022-05-03 16:59:00.022', 30,30), ('2022-05-03 16:59:00.022',40,40.0),('2022-05-03 16:59:00.022',50,50.0);"
        )
        tdSql.execute(
            f"insert into ct3(ts,c1,c5) values('2022-05-03 16:59:00.023', 60,60), ('2022-05-03 16:59:00.023',70,70.0),('2022-05-03 16:59:00.023',80,80.0), ('2022-05-03 16:59:00.023',90,90.0);"
        )
        tdSql.execute(
            f"insert into ct3(ts,c1,c5) values('2022-05-03 16:59:00.024', 100,100), ('2022-05-03 16:59:00.025',110,110.0),('2022-05-03 16:59:00.025',120,120.0), ('2022-05-03 16:59:00.025',130,130.0);"
        )
        tdSql.execute(
            f"insert into ct3(ts,c1,c5) values('2022-05-03 16:59:00.030', 140,140), ('2022-05-03 16:59:00.030',150,150.0),('2022-05-03 16:59:00.031',160,160.0), ('2022-05-03 16:59:00.030',170,170.0), ('2022-05-03 16:59:00.031',180,180.0);"
        )
        tdSql.execute(
            f"insert into ct3(ts,c1,c5) values('2022-05-03 16:59:00.042', 190,190), ('2022-05-03 16:59:00.041',200,200.0),('2022-05-03 16:59:00.040',210,210.0);"
        )
        tdSql.execute(
            f"insert into ct3(ts,c1,c5) values('2022-05-03 16:59:00.050', 220,220), ('2022-05-03 16:59:00.051',230,230.0),('2022-05-03 16:59:00.052',240,240.0);"
        )

        tdLog.info(f"=============== step 4 insert records into ct4")
        tdSql.execute(
            f"insert into ct4(ts,c1,c3,c4) values('2022-05-03 16:59:00.020', 10,'b0','n0');"
        )
        tdSql.execute(
            f"insert into ct4(ts,c1,c3,c4) values('2022-05-03 16:59:00.021', 20,'b1','n1'), ('2022-05-03 16:59:00.021',30,'b2','n2');"
        )
        tdSql.execute(
            f"insert into ct4(ts,c1,c3,c4) values('2022-05-03 16:59:00.022', 40,'b3','n3'), ('2022-05-03 16:59:00.022',40,'b4','n4'),('2022-05-03 16:59:00.022',50,'b5','n5');"
        )
        tdSql.execute(
            f"insert into ct4(ts,c1,c3,c4) values('2022-05-03 16:59:00.023', 60,'b6','n6'), ('2022-05-03 16:59:00.024',70,'b7','n7'),('2022-05-03 16:59:00.024',80,'b8','n8'), ('2022-05-03 16:59:00.023',90,'b9','n9');"
        )

        tdLog.info(
            f"=============== step 5 query records of ct1 from memory(taosc and taosd merge)"
        )
        tdSql.query(f"select * from ct1;")
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, 30)
        tdSql.checkData(0, 2, 100.000000000)
        tdSql.checkData(0, 3, "bin2")
        tdSql.checkData(0, 4, "nchar4")
        tdSql.checkData(0, 5, 1000.010000000)
        tdSql.checkData(1, 1, 11)
        tdSql.checkData(1, 2, None)

        tdSql.checkData(1, 3, "binary")
        tdSql.checkData(1, 4, "nchar")
        tdSql.checkData(1, 5, None)
        tdSql.checkData(5, 1, 22)
        tdSql.checkData(5, 2, None)
        tdSql.checkData(5, 3, None)
        tdSql.checkData(5, 4, "nchar")
        tdSql.checkData(5, 5, 220.000000000)

        tdLog.info(
            f"=============== step 6 query records of ct2 from memory(taosc and taosd merge)"
        )
        tdSql.query(f"select * from ct2;")
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}"
        )

        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 20)
        tdSql.checkData(0, 2, 20.000000000)
        tdSql.checkData(0, 3, "xkl")
        tdSql.checkData(0, 4, "zxc")
        tdSql.checkData(0, 5, 10.000000000)

        tdLog.info(f"=============== step 7 query records of ct3 from memory")
        tdSql.query(f"select * from ct3;")
        tdSql.checkRows(14)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(1, 1, 20)
        tdSql.checkData(2, 1, 50)
        tdSql.checkData(3, 1, 90)
        tdSql.checkData(4, 1, 100)
        tdSql.checkData(5, 1, 130)
        tdSql.checkData(6, 1, 170)
        tdSql.checkData(7, 1, 180)
        tdSql.checkData(8, 1, 210)
        tdSql.checkData(9, 1, 200)
        tdSql.checkData(10, 1, 190)
        tdSql.checkData(11, 1, 220)
        tdSql.checkData(12, 1, 230)
        tdSql.checkData(13, 1, 240)

        tdSql.checkData(0, 5, 10.000000000)
        tdSql.checkData(1, 5, 20.000000000)
        tdSql.checkData(2, 5, 50.000000000)
        tdSql.checkData(3, 5, 90.000000000)
        tdSql.checkData(4, 5, 100.000000000)
        tdSql.checkData(5, 5, 130.000000000)
        tdSql.checkData(6, 5, 170.000000000)
        tdSql.checkData(7, 5, 180.000000000)
        tdSql.checkData(8, 5, 210.000000000)
        tdSql.checkData(9, 5, 200.000000000)

        tdSql.checkData(10, 5, 190.000000000)
        tdSql.checkData(11, 5, 220.000000000)
        tdSql.checkData(12, 5, 230.000000000)
        tdSql.checkData(13, 5, 240.000000000)

        tdLog.info(f"=============== step 8 query records of ct4 from memory")
        tdSql.query(f"select * from ct4;")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(1, 1, 30)
        tdSql.checkData(2, 1, 50)
        tdSql.checkData(3, 1, 90)
        tdSql.checkData(4, 1, 80)
        tdSql.checkData(0, 3, "b0")
        tdSql.checkData(1, 3, "b2")
        tdSql.checkData(2, 3, "b5")
        tdSql.checkData(3, 3, "b9")
        tdSql.checkData(4, 3, "b8")
        tdSql.checkData(0, 4, "n0")
        tdSql.checkData(1, 4, "n2")
        tdSql.checkData(2, 4, "n5")
        tdSql.checkData(3, 4, "n9")
        tdSql.checkData(4, 4, "n8")

        # ==================== reboot to trigger commit data to file
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=============== step 9 query records of ct1 from file")
        tdSql.query(f"select * from ct1;")
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, 30)
        tdSql.checkData(0, 2, 100.000000000)
        tdSql.checkData(0, 3, "bin2")
        tdSql.checkData(0, 4, "nchar4")
        tdSql.checkData(0, 5, 1000.010000000)
        tdSql.checkData(1, 1, 11)
        tdSql.checkData(1, 2, None)
        tdSql.checkData(1, 3, "binary")
        tdSql.checkData(1, 4, "nchar")
        tdSql.checkData(1, 5, None)
        tdSql.checkData(5, 1, 22)
        tdSql.checkData(5, 2, None)
        tdSql.checkData(5, 3, None)
        tdSql.checkData(5, 4, "nchar")
        tdSql.checkData(5, 5, 220.000000000)

        tdLog.info(f"=============== step 10 query records of ct2 from file")
        tdSql.query(f"select * from ct2;")
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}"
        )

        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 20)
        tdSql.checkData(0, 2, 20.000000000)
        tdSql.checkData(0, 3, "xkl")
        tdSql.checkData(0, 4, "zxc")
        tdSql.checkData(0, 5, 10.000000000)

        tdLog.info(f"=============== step 11 query records of ct3 from file")
        tdSql.query(f"select * from ct3;")
        tdSql.checkRows(14)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(1, 1, 20)
        tdSql.checkData(2, 1, 50)
        tdSql.checkData(3, 1, 90)
        tdSql.checkData(4, 1, 100)
        tdSql.checkData(5, 1, 130)
        tdSql.checkData(6, 1, 170)
        tdSql.checkData(7, 1, 180)
        tdSql.checkData(8, 1, 210)
        tdSql.checkData(9, 1, 200)
        tdSql.checkData(10, 1, 190)
        tdSql.checkData(11, 1, 220)
        tdSql.checkData(12, 1, 230)
        tdSql.checkData(13, 1, 240)

        tdSql.checkData(0, 5, 10.000000000)
        tdSql.checkData(1, 5, 20.000000000)
        tdSql.checkData(2, 5, 50.000000000)
        tdSql.checkData(3, 5, 90.000000000)
        tdSql.checkData(4, 5, 100.000000000)
        tdSql.checkData(5, 5, 130.000000000)
        tdSql.checkData(6, 5, 170.000000000)
        tdSql.checkData(7, 5, 180.000000000)
        tdSql.checkData(8, 5, 210.000000000)
        tdSql.checkData(9, 5, 200.000000000)
        tdSql.checkData(10, 5, 190.000000000)
        tdSql.checkData(11, 5, 220.000000000)
        tdSql.checkData(12, 5, 230.000000000)
        tdSql.checkData(13, 5, 240.000000000)

        tdLog.info(f"=============== step 12 query records of ct4 from file")
        tdSql.query(f"select * from ct4;")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(1, 1, 30)
        tdSql.checkData(2, 1, 50)
        tdSql.checkData(3, 1, 90)
        tdSql.checkData(4, 1, 80)
        tdSql.checkData(0, 3, "b0")
        tdSql.checkData(1, 3, "b2")
        tdSql.checkData(2, 3, "b5")
        tdSql.checkData(3, 3, "b9")
        tdSql.checkData(4, 3, "b8")
        tdSql.checkData(0, 4, "n0")
        tdSql.checkData(1, 4, "n2")
        tdSql.checkData(2, 4, "n5")
        tdSql.checkData(3, 4, "n9")
        tdSql.checkData(4, 4, "n8")

    def Update2(self):
        ### 4096*0.8 - 1 = 3275
        rowSuperBlk = 3275
        ### 4096 - 3275 -1 - 1 = 819
        rowSubBlk = 819
        ts0 = 1752372800000
        ts1 = 1752372900000
        ts2 = 1752686800000

        i = 0
        db = "db0"
        stb1 = "stb1"
        tb1 = "tb1"
        stb2 = "stb2"
        tb2 = "tb2"

        tdLog.info(f"====== create database")
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} keep 10000 duration 10")
        tdLog.info(f"====== create tables")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {stb1} (ts timestamp, c1 bigint, c2 bigint, c3 bigint) tags(t1 int)"
        )
        tdSql.execute(
            f"create table {stb2} (ts timestamp, c1 bigint, c2 bigint, c3 bigint, c4 bigint, c5 bigint, c6 bigint, c7 bigint, c8 bigint, c9 bigint, c10 bigint, c11 bigint, c12 bigint, c13 bigint, c14 bigint, c15 bigint, c16 bigint, c17 bigint, c18 bigint, c19 bigint, c20 bigint, c21 bigint, c22 bigint, c23 bigint, c24 bigint, c25 bigint, c26 bigint, c27 bigint, c28 bigint, c29 bigint, c30 bigint) tags(t1 int)"
        )
        tdSql.execute(f"create table {tb1} using {stb1} tags(1)")
        tdSql.execute(f"create table {tb2} using {stb2} tags(2)")
        tdLog.info(f"====== tables created")

        tdLog.info(f"========== step 1: merge dataRow in mem")

        i = 0
        while i < rowSuperBlk:
            xs = i * 10
            ts = ts2 + xs
            tdSql.execute(f"insert into {tb1} (ts,c1) values ( {ts} , {i} )")
            i = i + 1

        tdSql.execute(f"insert into {tb1} values ( {ts0} , 1,NULL,0)")
        tdSql.execute(f"insert into {tb1} (ts,c2,c3) values ( {ts0} , 1,1)")

        tdSql.query(f"select * from {tb1} where ts = {ts0}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1)

        tdLog.info(f"========== step 2: merge kvRow in mem")
        i = 0
        while i < rowSuperBlk:
            xs = i * 10
            ts = ts2 + xs
            tdSql.execute(f"insert into {tb2} (ts,c1) values ( {ts} , {i} )")
            i = i + 1

        tdSql.execute(f"insert into {tb2} (ts,c3,c8,c10) values ( {ts0} , 1,NULL,0)")
        tdSql.execute(f"insert into {tb2} (ts,c8,c10) values ( {ts0} , 1,1)")

        tdSql.query(f"select ts,c1,c3,c8,c10 from {tb2} where ts = {ts0}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, 1)

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        tdLog.info(f"========== step 3: merge dataRow in file")
        tdSql.execute(f"insert into {tb1} (ts,c1) values ( {ts0} , 2)")
        tdLog.info(f"========== step 4: merge kvRow in file")
        tdSql.execute(f"insert into {tb2} (ts,c3) values ( {ts0} , 2)")

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        tdSql.query(f"select * from {tb1} where ts = {ts0}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1)

        tdSql.query(f"select ts,c1,c3,c8,c10 from {tb2} where ts = {ts0}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, 1)

        tdLog.info(f"========== step 5: merge dataRow in file/mem")
        i = 0
        while i < rowSubBlk:
            xs = i * 1
            ts = ts1 + xs
            tdSql.execute(f"insert into {tb1} (ts,c1) values ( {ts} , {i} )")
            i = i + 1

        tdLog.info(f"========== step 6: merge kvRow in file/mem")
        i = 0
        while i < rowSubBlk:
            xs = i * 1
            ts = ts1 + xs
            tdSql.execute(f"insert into {tb2} (ts,c1) values ( {ts} , {i} )")
            i = i + 1

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        tdSql.execute(f"insert into {tb1} (ts,c3) values ( {ts0} , 3)")
        tdSql.execute(f"insert into {tb2} (ts,c3) values ( {ts0} , 3)")
        tsN = ts0 + 1
        tdSql.execute(f"insert into {tb1} (ts,c1,c3) values ( {tsN} , 1,0)")
        tdSql.execute(f"insert into {tb2} (ts,c3,c8) values ( {tsN} , 100,200)")
        tsN = ts0 + 2
        tdSql.execute(f"insert into {tb1} (ts,c1,c3) values ( {tsN} , 1,0)")
        tdSql.execute(f"insert into {tb2} (ts,c3,c8) values ( {tsN} , 100,200)")

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        tdSql.query(f"select * from {tb1} where ts = {ts0}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 3)

        tdSql.query(f"select ts,c1,c3,c8,c10 from {tb2} where ts = {ts0}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, 1)
