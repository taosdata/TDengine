from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSelectResNum:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_select_res_num(self):
        """Select Res Num

        1. -

        Catalog:
            - Query:SelectList

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/query/read.sim

        """

        tdSql.execute(f"create database abc1 vgroups 2;")
        tdSql.execute(f"use abc1;")
        tdSql.execute(
            f"create table st1 (ts timestamp, k int, x int, y int, z binary(12), u nchar(12)) tags(a int, b nchar(12), c varchar(24), d bool) sma(x);"
        )
        tdSql.execute(f"create table tu using st1 tags(1, 'abc', 'binary1', true);")
        tdSql.execute(f"create table tu1 using st1 tags(2, '水木', 'binary2', false);")
        tdSql.execute(f"create table tu2 using st1 tags(3, '水木1', 'binary3', true);")
        tdSql.execute(f"create table tu3 using st1 tags(4, 'abc', '12', false);")
        tdSql.execute(
            f"insert into tu values('2022-01-01 1:1:1', 1, 10, 9, 'a', '水3木') ('2022-07-02 22:46:53.294', 2, 10, 8, 'a', '水1木') ('2022-07-02 22:47:53.294', 1, 10, 7, 'b', '水2木')('2022-07-02 22:48:53.294', 1, 10, null, 'd', '3')('2022-07-02 22:50:53.294', 1, 10, null, null, '322');"
        )
        tdSql.execute(
            f"insert into tu1 values('2022-01-01 1:1:1', 11, 101, 91, 'aa', '3水木');"
        )
        tdSql.execute(
            f"insert into tu2 values('2022-01-01 1:1:1', 111, 1010, 919, 'aaa', '3水木3');"
        )

        tdSql.query(f"select * from tu;")
        tdSql.checkRows(5)

        tdSql.query(f"select * from tu order by ts desc;")
        tdSql.checkRows(5)

        tdSql.execute(
            f"create table st2 (ts timestamp, k int, x int, y int, z binary(12), u nchar(12)) tags(a int) sma(x);"
        )
        tdSql.execute(f"create table tuu1 using st2 tags(2);")
        tdSql.execute(
            f"insert into tuu1 values('2022-01-01 1:1:1', 11, 101, 911, 'aa', '3水木33');"
        )
        tdSql.execute(
            f"insert into tuu1 values('2022-01-01 1:1:2', NULL, 101, 911, 'aa', '3水木33');"
        )
        tdSql.execute(
            f"insert into tu values('2022-01-01 1:1:1', NULL, NULL, NULL, NULL, '水3木');"
        )
        tdSql.execute(
            f"insert into tu values('2022-01-01 1:1:1', NULL, 911, NULL, NULL, '');"
        )
        tdSql.execute(f"flush database abc1;")

        tdSql.execute(f"insert into tu values('2021-12-1 1:1:1', 1,1,1,'a', 12);")
        tdSql.execute(f"insert into tu values('2022-6-1 1:1:1', 1,1,1,'a', 12);")
        tdSql.execute(f"insert into tu values('2022-6-1 1:1:2', 1,1,1,'a', 12);")
        tdSql.execute(f"insert into tu values('2022-6-1 1:1:3', 1,1,1,'a', 12);")

        tdSql.query(f"select * from tu order by ts desc;")
        tdSql.checkRows(9)

        tdSql.query(f"select * from tu order by ts asc;")
        tdSql.checkRows(9)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<'2022-1-1 1:1:0' order by ts asc;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<'2022-1-1 1:1:0' order by ts desc;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<'2022-1-1 1:1:2' order by ts asc;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<'2022-1-1 1:1:2' order by ts desc;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<'2022-1-1 1:1:2' order by ts asc;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<'2022-1-1 1:10:2' order by ts desc;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<'2022-1-1 1:10:2' order by ts asc;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<'2022-1-9 1:10:2' order by ts desc;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<'2022-1-9 1:10:2' order by ts asc;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-1-9 1:10:2' order by ts asc;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-1-9 1:10:2' order by ts desc;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-6-9 1:10:2' order by ts asc;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-6-9 1:10:2' order by ts desc;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-6-1 1:10:2' order by ts asc;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-6-1 1:10:2' order by ts desc;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-6-1 1:1:1' order by ts asc;"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-6-1 1:1:1' order by ts desc;"
        )
        tdSql.checkRows(2)

        tdSql.query(f"select * from tu where ts>='2021-12-31 1:1:1' order by ts asc;")
        tdSql.checkRows(8)

        tdSql.query(f"select * from tu where ts>='2021-12-31 1:1:1' order by ts desc;")
        tdSql.checkRows(8)

        tdSql.query(f"select * from tu where ts>='2021-12-1 1:1:1' order by ts asc;")
        tdSql.checkRows(9)

        tdSql.query(f"select * from tu where ts>='2021-12-1 1:1:1' order by ts desc;")
        tdSql.checkRows(9)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-6-1 1:1:2' order by ts asc;"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-6-1 1:1:2' order by ts desc;"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-6-1 1:1:1' order by ts asc;"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-6-1 1:1:1' order by ts desc;"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-6-1 1:1:0' order by ts asc;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from tu where ts>='2021-12-31 1:1:1' and ts<='2022-6-1 1:1:0' order by ts desc;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from tu where ts>='2021-12-1 1:1:1' and ts<='2022-6-1 1:1:0' order by ts asc;"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select * from tu where ts>='2021-12-1 1:1:1' and ts<='2022-6-1 1:1:0' order by ts desc;"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select * from tu where ts>='2021-12-1 1:1:1' and ts<='2022-7-1 1:1:0' order by ts asc;"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from tu where ts>='2021-12-1 1:1:1' and ts<='2022-7-1 1:1:0' order by ts desc;"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from tu where ts>='2021-12-1 1:1:1' and ts<='2022-7-7 1:1:0' order by ts desc;"
        )
        tdSql.checkRows(9)

        tdSql.query(
            f"select * from tu where ts>='2021-12-1 1:1:1' and ts<='2022-7-7 1:1:0' order by ts asc;"
        )
        tdSql.checkRows(9)

        tdSql.query(
            f"select * from tu where ts>='2021-12-1 1:1:1' and ts<='2022-7-2 1:1:0' order by ts desc;"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from tu where ts>='2021-12-1 1:1:1' and ts<='2022-7-2 1:1:0' order by ts asc;"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from tu where ts>='2021-12-1 1:1:1' and ts<='2022-7-2 22:47:0' order by ts desc;"
        )
        tdSql.checkRows(6)

        tdSql.query(
            f"select * from tu where ts>='2021-12-1 1:1:1' and ts<='2022-7-2 22:47:0' order by ts asc;"
        )
        tdSql.checkRows(6)

        tdSql.query(
            f"select * from tu where ts>='2022-7-1 1:1:1' and ts<='2022-7-2 22:47:0' order by ts asc;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from tu where ts>='2022-7-1 1:1:1' and ts<='2022-7-2 22:47:0' order by ts desc;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select * from tu where ts>='2022-7-1 1:1:1' and ts<='2022-8-2 22:47:0' order by ts asc;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select * from tu where ts>='2022-7-1 1:1:1' and ts<='2022-8-2 22:47:0' order by ts desc;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select * from tu where ts>='2022-7-1 1:1:1' and ts<='2022-7-2 22:48:53.299' order by ts asc;"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select * from tu where ts>='2022-7-1 1:1:1' and ts<='2022-7-2 22:48:53.299' order by ts desc;"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select * from tu where ts>='2022-7-1 1:1:1' and ts<='2022-7-2 22:48:53.293';"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select * from tu where ts>='2022-7-1 1:1:1' and ts<='2022-7-2 22:48:53.293' order by ts desc;"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select * from tu where ts>='2022-7-1 1:1:1' and ts<='2022-7-2 22:48:53.292' order by ts asc;"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select * from tu where ts>='2022-7-1 1:1:1' and ts<='2022-7-2 22:48:53.292' order by ts desc;"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select * from tu where ts>='2022-7-1 1:1:1' and ts<='2022-7-2 22:48:53.294';"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-1 1:1:1' and ts<'2022-7-2 22:48:53.294';"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<'2022-7-2 22:48:53.294';"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<='2022-7-2 22:47:53.294';"
        )

        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<='2022-7-2 22:47:53.293' order by ts asc;"
        )

        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<='2022-7-2 22:47:53.293' order by ts desc;"
        )

        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<='2022-7-2 22:48:53.293' order by ts asc;"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<='2022-7-2 22:48:53.293' order by ts desc;"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<='2022-7-2 22:48:53.293' order by ts desc;"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-1 22:46:55' and ts<='2022-7-2 22:48:53.293' order by ts desc;"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-1 22:46:55' and ts<='2022-7-2 22:48:53.293' order by ts asc;"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-1 22:46:55' and ts<='2022-7-2 22:48:53.293' order by ts desc;"
        )

        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<'2022-7-2 22:47:53.294';"
        )

        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<'2022-7-2 22:48:53.294';"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<'2022-7-2 22:48:53.294' order by ts desc;"
        )

        tdSql.query(
            f"select * from tu where ts>='2022-7-1 22:46:55' and ts<'2022-7-2 22:48:53.294' order by ts desc;"
        )
        tdSql.query(
            f"select * from tu where ts>='2021-12-2 22:46:55' and ts<'2022-7-2 22:48:53.294' order by ts desc;"
        )

        tdSql.query(
            f"select * from tu where ts>='2021-12-2 22:46:55' and ts<'2022-7-2 22:48:53.294' order by ts asc;"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<'2022-7-2 22:46:59.294' order by ts asc;"
        )

        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<'2022-7-2 22:46:59.294' order by ts asc;"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<'2022-7-2 22:46:59.294' order by ts asc;"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<'2022-7-2 22:46:59.294' order by ts desc;"
        )

        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<'2022-7-2 22:46:59.294' order by ts desc;"
        )

        tdSql.query(
            f"select * from tu where ts>='2021-7-2 22:46:55' and ts<'2022-7-2 22:46:54.294' order by ts desc;"
        )

        tdSql.query(
            f"select * from tu where ts>='2021-12-2 22:46:55' and ts<'2022-7-2 22:46:54.294' order by ts desc;"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:55' and ts<'2022-7-2 22:46:54.294' order by ts desc;"
        )

        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:51' and ts<'2022-7-2 22:48:54.294' order by ts desc;"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:51' and ts<'2022-7-2 22:48:54.294' order by ts asc;"
        )
        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:51' and ts<'2022-7-2 22:58:54.294' order by ts asc;"
        )

        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:51' and ts<'2022-7-2 22:58:54.294' order by ts asc;"
        )

        tdSql.query(
            f"select * from tu where ts>='2022-7-2 22:46:51' and ts<'2022-7-2 22:58:54.294' order by ts desc;"
        )
