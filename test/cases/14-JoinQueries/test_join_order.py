from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestJoinOrder:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_join_order(self):
        """Join with order by

        1. Create database with vgroup 1
        2. Create 1 stable 'sta' and 1 child table 'tba1'
        3. Insert 4 rows data into child table 'tba1'
        4. child query as left join table
        5. stba1 as right join table
        6. Join two tables on timestamp column with different order by combinations
        7. Check the result of join correctly

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-7 Simon Guan migrated from tsim/query/join_order.sim

        """

        tdSql.execute(f"drop database if exists db1;")
        tdSql.execute(f"create database db1 vgroups 1;")
        tdSql.execute(f"use db1;")
        tdSql.execute(f"create stable sta (ts timestamp, col1 int) tags(t1 int);")
        tdSql.execute(f"create table tba1 using sta tags(1);")

        tdSql.execute(f"insert into tba1 values ('2023-11-17 16:29:00', 1);")
        tdSql.execute(f"insert into tba1 values ('2023-11-17 16:29:02', 3);")
        tdSql.execute(f"insert into tba1 values ('2023-11-17 16:29:03', 4);")
        tdSql.execute(f"insert into tba1 values ('2023-11-17 16:29:04', 5);")

        tdSql.query(
            f"select a.*,b.* from tba1 a, (select * from tba1 order by ts) b where a.ts=b.ts;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select a.*,b.* from (select * from tba1 order by ts) a, tba1 b where a.ts=b.ts;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select a.*,b.* from tba1 a, (select * from tba1 order by ts desc) b where a.ts=b.ts;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select a.*,b.* from (select * from tba1 order by ts desc) a, tba1 b where a.ts=b.ts;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select a.*,b.* from (select * from tba1 order by ts) a, (select * from tba1 order by ts) b where a.ts=b.ts;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select a.*,b.* from (select * from tba1 order by ts desc) a, (select * from tba1 order by ts desc) b where a.ts=b.ts;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select a.*,b.* from (select * from tba1 order by ts) a, (select * from tba1 order by ts desc) b where a.ts=b.ts;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select a.*,b.* from (select * from tba1 order by ts desc) a, (select * from tba1 order by ts) b where a.ts=b.ts;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select a.*,b.* from (select * from tba1 order by ts desc) a, (select * from tba1 order by ts) b where a.ts=b.ts order by a.ts;"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2023-11-17 16:29:00.000")

        tdSql.query(
            f"select a.*,b.* from (select * from tba1 order by ts desc) a, (select * from tba1 order by ts) b where a.ts=b.ts order by a.ts desc;"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2023-11-17 16:29:04.000")

        tdSql.query(
            f"select a.*,b.* from (select * from tba1 order by ts) a, (select * from tba1 order by ts) b where a.ts=b.ts order by a.ts;"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2023-11-17 16:29:00.000")

        tdSql.query(
            f"select a.*,b.* from (select * from tba1 order by ts) a, (select * from tba1 order by ts) b where a.ts=b.ts order by a.ts desc;"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2023-11-17 16:29:04.000")

        tdSql.query(
            f"select a.*,b.* from (select * from tba1 order by ts limit 2) a, (select * from tba1 order by ts desc limit 2) b where a.ts=b.ts order by a.ts desc;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select a.*,b.* from (select * from tba1 order by ts limit 3) a, (select * from tba1 order by ts desc limit 3) b where a.ts=b.ts order by a.ts desc;"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2023-11-17 16:29:03.000")

        tdSql.query(
            f"select a.*,b.* from (select * from tba1 order by ts limit 3) a, (select * from tba1 order by ts desc limit 3) b where a.ts=b.ts order by a.ts;"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2023-11-17 16:29:02.000")

        tdSql.query(
            f"select a.*,b.* from tba1 a, (select * from tba1 order by ts desc limit 3) b where a.ts=b.ts order by a.ts;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2023-11-17 16:29:02.000")

        tdSql.query(
            f"select a.*,b.* from tba1 a, (select * from tba1 order by ts limit 3) b where a.ts=b.ts order by a.ts desc limit 2;"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2023-11-17 16:29:03.000")

        tdSql.query(
            f"select a.*,b.* from (select * from tba1 order by ts limit 3) a, tba1 b where a.ts=b.ts order by a.ts desc;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2023-11-17 16:29:03.000")

        tdSql.query(
            f"select a.*,b.* from (select * from tba1 order by ts desc limit 3) a, tba1 b where a.ts=b.ts order by a.ts desc;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2023-11-17 16:29:04.000")

        tdSql.query(
            f"select a.*,b.* from (select * from tba1 order by ts desc limit 3) a, tba1 b where a.ts=b.ts order by a.ts;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2023-11-17 16:29:02.000")
