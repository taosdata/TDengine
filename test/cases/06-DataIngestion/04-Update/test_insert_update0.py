from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestInsertUpdate0:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_insert_update0(self):
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
            - 2025-4-29 Simon Guan Migrated from tsim/insert/update0.sim

        """

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
        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdLog.info(f"{tdSql.getData(1,0)} {tdSql.getData(1,1)}")
        tdLog.info(f"{tdSql.getData(2,0)} {tdSql.getData(2,1)}")
        tdLog.info(f"{tdSql.getData(3,0)} {tdSql.getData(3,1)}")
        tdLog.info(f"{tdSql.getData(4,0)} {tdSql.getData(4,1)}")
        tdLog.info(f"{tdSql.getData(5,0)} {tdSql.getData(5,1)}")

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
        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdLog.info(f"{tdSql.getData(1,0)} {tdSql.getData(1,1)}")
        tdLog.info(f"{tdSql.getData(2,0)} {tdSql.getData(2,1)}")

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
        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdLog.info(f"{tdSql.getData(1,0)} {tdSql.getData(1,1)}")
        tdLog.info(f"{tdSql.getData(2,0)} {tdSql.getData(2,1)}")
        tdLog.info(f"{tdSql.getData(3,0)} {tdSql.getData(3,1)}")
        tdLog.info(f"{tdSql.getData(4,0)} {tdSql.getData(4,1)}")
        tdLog.info(f"{tdSql.getData(5,0)} {tdSql.getData(5,1)}")

        tdSql.checkRows(6)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(2, 1, 18)
        tdSql.checkData(5, 1, 22)

        tdLog.info(f"=============== step3-2 query records of ct2 from file")
        tdSql.query(f"select * from ct2;")
        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdLog.info(f"{tdSql.getData(1,0)} {tdSql.getData(1,1)}")
        tdLog.info(f"{tdSql.getData(2,0)} {tdSql.getData(2,1)}")

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
        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdLog.info(f"{tdSql.getData(1,0)} {tdSql.getData(1,1)}")
        tdLog.info(f"{tdSql.getData(2,0)} {tdSql.getData(2,1)}")
        tdLog.info(f"{tdSql.getData(3,0)} {tdSql.getData(3,1)}")
        tdLog.info(f"{tdSql.getData(4,0)} {tdSql.getData(4,1)}")
        tdLog.info(f"{tdSql.getData(5,0)} {tdSql.getData(5,1)}")

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
        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdLog.info(f"{tdSql.getData(1,0)} {tdSql.getData(1,1)}")
        tdLog.info(f"{tdSql.getData(2,0)} {tdSql.getData(2,1)}")
        tdLog.info(f"{tdSql.getData(3,0)} {tdSql.getData(3,1)}")
        tdLog.info(f"{tdSql.getData(4,0)} {tdSql.getData(4,1)}")

        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(1, 1, 303)
        tdSql.checkData(2, 1, None)
        tdSql.checkData(3, 1, 60)
        tdSql.checkData(4, 1, None)
