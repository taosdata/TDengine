from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestInsertTbMulti:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_insert_tb_multi(self):
        """insert tb multi

        1. 待补充

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/parser/insert_multiTbl.sim

        """

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
