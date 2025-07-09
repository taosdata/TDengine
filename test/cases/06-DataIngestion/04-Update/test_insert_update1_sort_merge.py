from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestInsertUpdate1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_insert_update1(self):
        """update sub table data

        1. create table
        2. insert data
        3. query data

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-29 Simon Guan Migrated from tsim/insert/update1_sort_merge.sim

        """

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
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)} {tdSql.getData(4,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)} {tdSql.getData(5,5)}"
        )

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
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)} {tdSql.getData(4,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)} {tdSql.getData(5,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(6,0)} {tdSql.getData(6,1)} {tdSql.getData(6,2)} {tdSql.getData(6,3)} {tdSql.getData(6,4)} {tdSql.getData(6,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(7,0)} {tdSql.getData(7,1)} {tdSql.getData(7,2)} {tdSql.getData(7,3)} {tdSql.getData(7,4)} {tdSql.getData(7,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(8,0)} {tdSql.getData(8,1)} {tdSql.getData(8,2)} {tdSql.getData(8,3)} {tdSql.getData(8,4)} {tdSql.getData(8,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(9,0)} {tdSql.getData(9,1)} {tdSql.getData(9,2)} {tdSql.getData(9,3)} {tdSql.getData(9,4)} {tdSql.getData(9,5)}"
        )

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
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)} {tdSql.getData(4,5)}"
        )

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
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)} {tdSql.getData(4,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)} {tdSql.getData(5,5)}"
        )

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
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)} {tdSql.getData(4,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)} {tdSql.getData(5,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(6,0)} {tdSql.getData(6,1)} {tdSql.getData(6,2)} {tdSql.getData(6,3)} {tdSql.getData(6,4)} {tdSql.getData(6,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(7,0)} {tdSql.getData(7,1)} {tdSql.getData(7,2)} {tdSql.getData(7,3)} {tdSql.getData(7,4)} {tdSql.getData(7,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(8,0)} {tdSql.getData(8,1)} {tdSql.getData(8,2)} {tdSql.getData(8,3)} {tdSql.getData(8,4)} {tdSql.getData(8,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(9,0)} {tdSql.getData(9,1)} {tdSql.getData(9,2)} {tdSql.getData(9,3)} {tdSql.getData(9,4)} {tdSql.getData(9,5)}"
        )

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
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)} {tdSql.getData(2,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)} {tdSql.getData(3,5)}"
        )
        tdLog.info(
            f"{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)} {tdSql.getData(4,5)}"
        )

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
