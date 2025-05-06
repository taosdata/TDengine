from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestInsertCommitMerge:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_insert_commit_merge(self):
        """insert sub table (commit)

        1. create table
        2. insert data
        3. query data

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/insert/commit-merge0.sim

        """

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database db  duration 120 keep 365000d,365000d,365000d")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}")

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

            tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)}")
            tdLog.info(f"{tdSql.getData(1,0)} {tdSql.getData(1,1)}")
            tdLog.info(f"{tdSql.getData(2,0)} {tdSql.getData(2,1)}")
            tdLog.info(f"{tdSql.getData(3,0)} {tdSql.getData(3,1)}")
            tdLog.info(f"{tdSql.getData(4,0)} {tdSql.getData(4,1)}")
            tdLog.info(f"{tdSql.getData(5,0)} {tdSql.getData(5,1)}")
            tdLog.info(f"{tdSql.getData(6,0)} {tdSql.getData(6,1)}")
            tdLog.info(f"{tdSql.getData(7,0)} {tdSql.getData(7,1)}")
            tdLog.info(f"{tdSql.getData(8,0)} {tdSql.getData(8,1)}")
            tdLog.info(f"{tdSql.getData(9,0)} {tdSql.getData(9,1)}")

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

            tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)}")
            tdLog.info(f"{tdSql.getData(1,0)} {tdSql.getData(1,1)}")
            tdLog.info(f"{tdSql.getData(2,0)} {tdSql.getData(2,1)}")
            tdLog.info(f"{tdSql.getData(3,0)} {tdSql.getData(3,1)}")
            tdLog.info(f"{tdSql.getData(4,0)} {tdSql.getData(4,1)}")
            tdLog.info(f"{tdSql.getData(5,0)} {tdSql.getData(5,1)}")
            tdLog.info(f"{tdSql.getData(6,0)} {tdSql.getData(6,1)}")
            tdLog.info(f"{tdSql.getData(7,0)} {tdSql.getData(7,1)}")
            tdLog.info(f"{tdSql.getData(8,0)} {tdSql.getData(8,1)}")
            tdLog.info(f"{tdSql.getData(9,0)} {tdSql.getData(9,1)}")

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
