from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNormalTableAlter2:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_alter_2(self):
        """Alter: then insert

        1. Execute ADD COLUMN, DROP COLUMN, MODIFY COLUMN operations.
        2. Insert data and run SELECT COUNT queries to verify.
        3. Restart the database.
        4. Continue modifying columns and verify the changes.


        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-12 Simon Guan Migrated from tsim/stable/alter_count.sim

        """

        tdLog.info(f"========= start dnode1 as master")

        tdLog.info(f"======== step1")
        tdSql.execute(f"create database d1 replica 1 duration 7 keep 50")
        tdSql.execute(f"use d1")
        tdSql.execute(f"create table tb (ts timestamp, a int)")
        tdSql.execute(f"insert into tb values(now-28d, -28)")
        tdSql.execute(f"insert into tb values(now-27d, -27)")
        tdSql.execute(f"insert into tb values(now-26d, -26)")
        tdSql.query(f"select count(a) from tb")
        tdSql.checkData(0, 0, 3)

        tdLog.info(f"======== step2")
        tdSql.execute(f"alter table tb add column b smallint")
        tdSql.execute(f"insert into tb values(now-25d, -25, 0)")
        tdSql.execute(f"insert into tb values(now-24d, -24, 1)")
        tdSql.execute(f"insert into tb values(now-23d, -23, 2)")
        tdSql.query(f"select count(b) from tb")
        tdSql.checkData(0, 0, 3)

        tdLog.info(f"======== step3")
        # sql alter table tb(ts timestamp, a int, b smallint, c tinyint)
        tdSql.execute(f"alter table tb add column c tinyint")
        tdSql.execute(f"insert into tb values(now-22d, -22, 3, 0)")
        tdSql.execute(f"insert into tb values(now-21d, -21, 4, 1)")
        tdSql.execute(f"insert into tb values(now-20d, -20, 5, 2)")
        tdSql.query(f"select count(c) from tb")
        tdSql.checkData(0, 0, 3)

        tdLog.info(f"======== step4")
        # sql alter table tb(ts timestamp, a int, b smallint, c tinyint, d int)
        tdSql.execute(f"alter table tb add column d int")
        tdSql.execute(f"insert into tb values(now-19d, -19, 6, 0, 0)")
        tdSql.execute(f"insert into tb values(now-18d, -18, 7, 1, 1)")
        tdSql.execute(f"insert into tb values(now-17d, -17, 8, 2, 2)")
        tdSql.query(f"select count(d) from tb")
        tdSql.checkData(0, 0, 3)

        tdLog.info(f"======== step5")
        # sql alter table tb(ts timestamp, a int, b smallint, c tinyint, d int, e bigint)
        tdSql.execute(f"alter table tb add column e bigint")
        tdSql.execute(f"insert into tb values(now-16d, -16, 9, 0, 0, 0)")
        tdSql.execute(f"insert into tb values(now-15d, -15, 10, 1, 1, 1)")
        tdSql.execute(f"insert into tb values(now-14d, -14, 11, 2, 2, 2)")
        tdSql.query(f"select count(e) from tb")
        tdSql.checkData(0, 0, 3)

        tdLog.info(f"======== step6")
        # sql alter table tb(ts timestamp, a int, b smallint, c tinyint, d int, e bigint, f float)
        tdSql.execute(f"alter table tb add column f float")
        tdSql.execute(f"insert into tb values(now-13d, -13, 12, 0, 0, 0, 0)")
        tdSql.execute(f"insert into tb values(now-12d, -12, 13, 1, 1, 1, 1)")
        tdSql.execute(f"insert into tb values(now-11d, -11, 24, 2, 2, 2, 2)")
        tdSql.query(f"select count(f) from tb")
        tdSql.checkData(0, 0, 3)

        tdLog.info(f"======== step7")
        # sql alter table tb(ts timestamp, a int, b smallint, c tinyint, d int, e bigint, f float, g double)
        tdSql.execute(f"alter table tb add column g double")
        tdSql.execute(f"insert into tb values(now-10d, -10, 15, 0, 0, 0, 0, 0)")
        tdSql.execute(f"insert into tb values(now-9d, -9, 16, 1, 1, 1, 1, 1)")
        tdSql.execute(f"insert into tb values(now-8d, -8, 17, 2, 2, 2, 2, 2)")
        tdSql.query(f"select count(g) from tb")
        tdSql.checkData(0, 0, 3)

        tdLog.info(f"======== step8")
        # sql alter table tb(ts timestamp, a int, b smallint, c tinyint, d int, e bigint, f float, g double, h binary(10) )
        tdSql.execute(f"alter table tb add column h binary(10)")
        tdSql.execute(f"insert into tb values(now-7d, -7, 18, 0, 0, 0, 0, 0, '0')")
        tdSql.execute(f"insert into tb values(now-6d, -6, 19, 1, 1, 1, 1, 1, '1')")
        tdSql.execute(f"insert into tb values(now-5d, -5, 20, 2, 2, 2, 2, 2, '2')")
        tdSql.query(f"select count(h) from tb")
        tdSql.checkData(0, 0, 3)

        tdLog.info(f"======== step9")
        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 0, 24)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 1, 21)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 2, 18)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 3, 15)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 4, 12)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 5, 9)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 6, 6)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 7, 3)

        tdLog.info(f"============= step10")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from d1.tb;"
        )
        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from d1.tb;"
        )

        tdSql.execute(f"use d1")
        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 0, 24)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 1, 21)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 2, 18)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 3, 15)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 4, 12)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 5, 9)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 6, 6)

        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.checkData(0, 7, 3)

        tdLog.info(f"======== step11")
        # sql alter table tb(ts timestamp, b smallint, c tinyint, d int, e bigint, f float, g double, h binary(20))
        tdSql.execute(f"alter table tb drop column a")
        tdSql.execute(f"insert into tb values(now-4d, 1, 1, 1, 1, 1, 1, '1')")
        tdSql.query(f"select count(g) from tb")
        tdSql.checkData(0, 0, 7)

        tdLog.info(f"======== step12")
        # sql alter table tb(ts timestamp, c tinyint, d int, e bigint, f float, g double, h binary(20))
        tdSql.execute(f"alter table tb drop column b")
        tdSql.execute(f"insert into tb values(now-3d, 1, 1, 1, 1, 1, '1')")
        tdSql.query(f"select count(g) from tb")
        tdSql.checkData(0, 0, 8)

        tdLog.info(f"======== step13")
        # sql alter table tb(ts timestamp, d int, e bigint, f float, g double, h binary(20))
        tdSql.execute(f"alter table tb drop column c")
        tdSql.execute(f"insert into tb values(now-2d, 1, 1, 1, 1, '1')")
        tdSql.query(f"select count(g) from tb")
        tdSql.checkData(0, 0, 9)

        tdLog.info(f"======== step14")
        # sql alter table tb(ts timestamp, e bigint, f float, g double, h binary(20))
        tdSql.execute(f"alter table tb drop column d")
        tdSql.execute(f"insert into tb values(now-1d, 1, 1, 1, '1')")
        tdSql.query(f"select count(g) from tb")
        tdSql.checkData(0, 0, 10)

        tdLog.info(f"======== step15")
        # sql alter table tb(ts timestamp, f float, g double, h binary(20))
        tdSql.execute(f"alter table tb drop column e")
        tdSql.execute(f"insert into tb values(now, 1, 1, '1')")
        tdSql.query(f"select count(g) from tb")
        tdSql.checkData(0, 0, 11)

        tdLog.info(f"======== step16")
        # sql alter table tb(ts timestamp, g double, h binary(20))
        tdSql.execute(f"alter table tb drop column f")
        tdSql.execute(f"insert into tb values(now+1d, 1, '1')")
        tdSql.query(f"select count(g) from tb")
        tdSql.checkData(0, 0, 12)

        tdLog.info(f"======== step17")
        # sql alter table tb(ts timestamp, h binary(20))
        tdSql.execute(f"alter table tb drop column g")
        tdSql.execute(f"insert into tb values(now+2d, '1')")
        tdSql.query(f"select count(h) from tb")
        tdSql.checkData(0, 0, 10)

        tdLog.info(f"============== step18")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        # sql select count(g) from tb
        # if $tdSql.getData(0,0) != 12 then
        #  return -1
        # endi
        tdSql.query(f"select count(*) from tb")
        tdSql.checkData(0, 0, 31)
