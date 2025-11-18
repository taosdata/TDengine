from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestJoinFull:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_join_full(self):
        """Join mode

        1. test full_join
        2. test inner_join
        3. test join_boundary
        4. test join_explain
        5. test join_nested
        6. test join_scalar1
        7. test join_scalar2
        8. test join_timeline
        9. test left_anti_join
        10. test left_asof_join
        11. test left_join
        12. test left_semi_join
        13. test left_win_join
        14. test right_anti_join
        15. test right_asof_join
        16. test right_join
        17. test right_semi_join
        18. test right_win_join
        19. restart and test again

        Catalog:
            - Query:Join

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan migrated from tsim/join/join.sim

        """

        tdSql.prepare(dbname="test0", vgroups=3)
        tdSql.execute(f"use test0;")

        tdSql.execute(f"create stable sta (ts timestamp, col1 int) tags(t1 int);")
        tdSql.execute(f"create table tba1 using sta tags(1);")
        tdSql.execute(f"create table tba2 using sta tags(2);")

        tdSql.execute(f"insert into tba1 values ('2023-11-17 16:29:00', 1);")
        tdSql.execute(f"insert into tba1 values ('2023-11-17 16:29:02', 3);")
        tdSql.execute(f"insert into tba1 values ('2023-11-17 16:29:03', 4);")
        tdSql.execute(f"insert into tba1 values ('2023-11-17 16:29:04', 5);")

        tdSql.execute(f"insert into tba2 values ('2023-11-17 16:29:00', 2);")
        tdSql.execute(f"insert into tba2 values ('2023-11-17 16:29:01', 3);")
        tdSql.execute(f"insert into tba2 values ('2023-11-17 16:29:03', 5);")
        tdSql.execute(f"insert into tba2 values ('2023-11-17 16:29:05', 7);")

        tdSql.prepare(dbname="testa", vgroups=3)
        tdSql.execute(f"use testa;")

        tdSql.execute(f"create table sta1(ts timestamp, f int, g bigint) tags (t int);")
        tdSql.execute(
            f"insert into cta11 using sta1 tags(1) values('2023-10-16 09:10:11.001', 100111, 1001110);"
        )
        tdSql.execute(
            f"insert into cta12 using sta1 tags(2) values('2023-10-16 09:10:12.001', 100112, 1001120);"
        )
        tdSql.execute(
            f"insert into cta13 using sta1 tags(3) values('2023-10-16 09:10:13.001', 100113, 1001130);"
        )
        tdSql.execute(
            f"insert into cta14 using sta1 tags(4) values('2023-10-16 09:10:14.001', 100114, 1001140);"
        )

        tdSql.execute(f"create table sta2(ts timestamp, f int, g bigint) tags (t int);")
        tdSql.execute(
            f"insert into cta21 using sta2 tags(1) values('2023-10-16 09:10:11.002', 100221, 1002210);"
        )
        tdSql.execute(
            f"insert into cta22 using sta2 tags(2) values('2023-10-16 09:10:12.002', 100222, 1002220);"
        )
        tdSql.execute(
            f"insert into cta23 using sta2 tags(3) values('2023-10-16 09:10:13.002', 100223, 1002230);"
        )
        tdSql.execute(
            f"insert into cta24 using sta2 tags(4) values('2023-10-16 09:10:14.002', 100224, 1002240);"
        )

        tdSql.execute(f"create table stt(ts timestamp, f int, g int) tags (t int);")
        tdSql.execute(f"create table tt using stt tags(99);")

        tdSql.execute(f"create table stv(ts timestamp, h int) tags (t1 int);")
        tdSql.execute(
            f"insert into ctv1 using stv tags(1) values('2023-10-16 10:10:10', 1);"
        )

        tdSql.prepare(dbname="testb", vgroups=1, PRECISION="us")
        tdSql.execute(f"use testb;")

        tdSql.execute(f"create table stb1(ts timestamp, f int,g int) tags (t int);")
        tdSql.execute(
            f"insert into ctb11 using stb1 tags(1) values('2023-10-16 09:10:11', 110111, 1101110);"
        )
        tdSql.execute(
            f"insert into ctb12 using stb1 tags(2) values('2023-10-16 09:10:12', 110112, 1101120);"
        )
        tdSql.execute(
            f"insert into ctb13 using stb1 tags(3) values('2023-10-16 09:10:13', 110113, 1101130);"
        )
        tdSql.execute(
            f"insert into ctb14 using stb1 tags(4) values('2023-10-16 09:10:14', 110114, 1101140);"
        )

        tdSql.execute(f"create table st2(ts timestamp, f int, g int) tags (t int);")
        tdSql.execute(
            f"insert into ctb21 using st2 tags(1) values('2023-10-16 09:10:11', 110221, 1102210);"
        )
        tdSql.execute(
            f"insert into ctb22 using st2 tags(2) values('2023-10-16 09:10:12', 110222, 1102220);"
        )
        tdSql.execute(
            f"insert into ctb23 using st2 tags(3) values('2023-10-16 09:10:13', 110223, 1102230);"
        )
        tdSql.execute(
            f"insert into ctb24 using st2 tags(4) values('2023-10-16 09:10:14', 110224, 1102240);"
        )

        tdSql.prepare(dbname="testb1", vgroups=1, PRECISION="us")
        tdSql.execute(f"use testb1;")

        tdSql.execute(f"create table stb21(ts timestamp, f int,g int) tags (t int);")
        tdSql.execute(
            f"insert into ctb11 using stb21 tags(1) values('2023-10-16 09:10:11', 110111, 1101110);"
        )
        tdSql.execute(
            f"insert into ctb12 using stb21 tags(2) values('2023-10-16 09:10:12', 110112, 1101120);"
        )
        tdSql.execute(
            f"insert into ctb13 using stb21 tags(3) values('2023-10-16 09:10:13', 110113, 1101130);"
        )
        tdSql.execute(
            f"insert into ctb14 using stb21 tags(4) values('2023-10-16 09:10:14', 110114, 1101140);"
        )

        tdSql.execute(f"create table st22(ts timestamp, f int, g int) tags (t int);")
        tdSql.execute(
            f"insert into ctb21 using st22 tags(1) values('2023-10-16 09:10:11', 110221, 1102210);"
        )
        tdSql.execute(
            f"insert into ctb22 using st22 tags(2) values('2023-10-16 09:10:12', 110222, 1102220);"
        )
        tdSql.execute(
            f"insert into ctb23 using st22 tags(3) values('2023-10-16 09:10:13', 110223, 1102230);"
        )
        tdSql.execute(
            f"insert into ctb24 using st22 tags(4) values('2023-10-16 09:10:14', 110224, 1102240);"
        )

        tdSql.prepare(dbname="testc", vgroups=3)
        tdSql.execute(f"use testc")

        tdSql.execute(f"create table stc1(ts timestamp, f int) tags (t json);")
        json11 = '{\\"tag1\\":\\"1-11\\",\\"tag2\\":1}'
        json12 = '{\\"tag1\\":\\"1-12\\",\\"tag2\\":2}'
        json21 = '{\\"tag1\\":\\"1-21\\",\\"tag2\\":1}'
        json22 = '{\\"tag1\\":\\"1-22\\",\\"tag2\\":2}'
        tdSql.execute(
            f'insert into ctb11 using stc1 tags("{json11}") values("2023-10-16 09:10:11", 11);'
        )
        tdSql.execute(
            f'insert into ctb11 using stc1 tags("{json11}") values("2023-10-16 09:10:12", 12);'
        )
        tdSql.execute(
            f'insert into ctb12 using stc1 tags("{json12}") values("2023-10-16 09:10:11", 21);'
        )
        tdSql.execute(
            f'insert into ctb12 using stc1 tags("{json12}") values("2023-10-16 09:10:12", 22);'
        )

        tdSql.execute(f"create table stc2(ts timestamp, f int) tags (t json);")
        tdSql.execute(
            f'insert into ctb21 using stc2 tags("{json21}") values("2023-10-16 09:10:11", 110);'
        )
        tdSql.execute(
            f'insert into ctb21 using stc2 tags("{json21}") values("2023-10-16 09:10:12", 120);'
        )
        tdSql.execute(
            f'insert into ctb22 using stc2 tags("{json22}") values("2023-10-16 09:10:11", 210);'
        )
        tdSql.execute(
            f'insert into ctb22 using stc2 tags("{json22}") values("2023-10-16 09:10:12", 220);'
        )

        self.inner_join()
        self.left_join()
        self.right_join()
        self.full_join()
        self.left_semi_join()
        self.right_semi_join()
        self.left_anti_join()
        self.right_anti_join()
        self.left_asof_join()
        self.right_asof_join()
        self.left_win_join()
        self.right_win_join()
        self.join_scalar1()
        self.join_scalar2()
        self.join_timeline()
        self.join_nested()
        self.join_boundary()
        self.join_explain()
        self.join_json()

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        self.inner_join()
        self.left_join()
        self.right_join()
        self.full_join()
        self.left_semi_join()
        self.right_semi_join()
        self.left_anti_join()
        self.right_anti_join()
        self.left_asof_join()
        self.right_asof_join()
        self.left_win_join()
        self.right_win_join()
        self.join_scalar1()
        self.join_scalar2()
        self.join_timeline()
        self.join_nested()
        self.join_boundary()
        self.join_explain()
        self.join_json()

    def inner_join(self):
        tdSql.execute(f"use test0;")

        tdSql.query(
            f"select a.col1, b.col1 from sta a inner join sta b on a.ts = b.ts and a.ts < '2023-11-17 16:29:02' order by a.col1, b.col1;"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(3, 1, 2)
        tdSql.checkData(4, 0, 3)
        tdSql.checkData(4, 1, 3)

        tdSql.query(
            f"select a.col1, b.col1 from sta a join sta b on a.ts = b.ts where a.ts < '2023-11-17 16:29:02' and b.ts < '2023-11-17 16:29:01' order by a.col1, b.col1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(3, 1, 2)

        tdSql.query(f"select a.col1, b.col1 from sta a join sta b on a.ts = b.ts;")
        tdSql.checkRows(12)

        tdSql.query(
            f"select a.col1, b.col1 from tba1 a join tba2 b on a.ts = b.ts order by a.col1, b.col1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 0, 4)
        tdSql.checkData(1, 1, 5)

        tdSql.query(
            f"select a.col1, b.col1 from tba2 a join tba1 b on a.ts = b.ts order by a.col1, b.col1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(1, 1, 4)

        tdSql.query(
            f"select a.ts, a.col1, b.ts,b.col1 from sta a join sta b on a.ts = b.ts and a.t1=b.t1 order by a.t1, a.ts;"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, "2023-11-17 16:29:00")
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(1, 2, "2023-11-17 16:29:02")
        tdSql.checkData(1, 3, 3)

        tdSql.query(
            f"select a.ts, b.ts from sta a join sta b on a.ts=b.ts order by a.ts desc;"
        )
        tdSql.checkRows(12)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, "2023-11-17 16:29:05")
        tdSql.checkData(1, 0, "2023-11-17 16:29:04")
        tdSql.checkData(1, 1, "2023-11-17 16:29:04")

        tdSql.query(
            f"select a.ts, b.ts from sta a join sta b on a.ts=b.ts order by b.ts desc;"
        )
        tdSql.checkRows(12)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, "2023-11-17 16:29:05")
        tdSql.checkData(1, 0, "2023-11-17 16:29:04")
        tdSql.checkData(1, 1, "2023-11-17 16:29:04")

        tdSql.query(
            f"select a.ts, b.ts from sta a join sta b on a.ts=b.ts order by a.ts desc, b.ts;"
        )
        tdSql.checkRows(12)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, "2023-11-17 16:29:05")
        tdSql.checkData(1, 0, "2023-11-17 16:29:04")
        tdSql.checkData(1, 1, "2023-11-17 16:29:04")

        tdSql.query(
            f"select c.ts from ((select a.ts from sta a join sta b where a.ts=b.ts order by a.ts) union (select b.ts from sta a join sta b where a.ts=b.ts) order by 1) c join ((select a.ts from sta a join sta b where a.ts=b.ts order by a.ts) union (select b.ts from sta a join sta b where a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )
        tdSql.checkRows(6)

        tdSql.query(
            f"select c.ts from ((select a.ts from sta a join sta b where a.ts=b.ts order by a.ts) union all (select b.ts from sta a join sta b where a.ts=b.ts) order by 1) c join ((select a.ts from sta a join sta b where a.ts=b.ts order by a.ts) union all (select b.ts from sta a join sta b where a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )
        tdSql.checkRows(144)

        tdSql.query(
            f"select a.ts, b.ts from tba1 a join sta b on a.ts = b.ts and a.t1 = b.t1;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select a.ts, b.ts from sta a join sta b on a.ts = b.ts and a.t1 = b.t1;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select * from testb.stb1 a join testb.st2 b where a.ts = b.ts and a.t = b.t;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select * from testb.stb1 a join testb1.stb21 b where a.ts = b.ts and a.t = b.t;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select * from testb.stb1 b join testb1.stb21 a where a.ts = b.ts and a.t = b.t;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select * from testb.stb1 a join testb1.stb21 b where b.ts = a.ts and b.t = a.t;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select * from testb.stb1 b join testb1.stb21 a where b.ts = a.ts and b.t = a.t;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select * from testb.stb1 b join testb1.stb21 a where a.ts = b.ts and b.t = a.t;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select * from testb.stb1 b join testb1.stb21 a where b.ts = a.ts and a.t = b.t;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select * from testb.stb1 a, testb1.stb21 b where a.ts = b.ts and a.t = b.t;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select * from testb.stb1 a join testb1.stb21 b on a.ts = b.ts and a.t = b.t;"
        )
        tdSql.checkRows(4)

    def left_join(self):
        tdSql.execute(f"use test0;")

        tdSql.query(
            f"select a.col1, b.col1 from sta a left join sta b on a.ts = b.ts and a.ts < '2023-11-17 16:29:02' and b.ts < '2023-11-17 16:29:01' order by a.col1, b.col1;"
        )
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(3, 1, 2)
        tdSql.checkData(4, 0, 3)
        tdSql.checkData(4, 1, None)
        tdSql.checkData(5, 0, 3)
        tdSql.checkData(5, 1, None)
        tdSql.checkData(6, 0, 4)
        tdSql.checkData(6, 1, None)
        tdSql.checkData(7, 0, 5)
        tdSql.checkData(7, 1, None)
        tdSql.checkData(8, 0, 5)
        tdSql.checkData(8, 1, None)
        tdSql.checkData(9, 0, 7)
        tdSql.checkData(9, 1, None)

        tdSql.query(
            f"select a.col1, b.col1 from sta a left join sta b on a.ts = b.ts where a.ts < '2023-11-17 16:29:02' and b.ts < '2023-11-17 16:29:01' order by a.col1, b.col1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(3, 1, 2)

        tdSql.query(f"select a.col1, b.col1 from sta a left join sta b on a.ts = b.ts;")
        tdSql.checkRows(12)

        tdSql.query(
            f"select a.col1, b.col1 from tba1 a left join tba2 b on a.ts = b.ts order by a.col1, b.col1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 0, 4)
        tdSql.checkData(2, 1, 5)
        tdSql.checkData(3, 0, 5)
        tdSql.checkData(3, 1, None)

        tdSql.query(
            f"select a.col1, b.col1 from tba2 a left join tba1 b on a.ts = b.ts order by a.col1, b.col1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(2, 1, 4)
        tdSql.checkData(3, 0, 7)
        tdSql.checkData(3, 1, None)

        tdSql.query(
            f"select a.ts, b.ts from tba2 a left join tba1 b on a.ts = b.ts order by a.ts desc;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "2023-11-17 16:29:03")
        tdSql.checkData(1, 1, "2023-11-17 16:29:03")
        tdSql.checkData(2, 0, "2023-11-17 16:29:01")
        tdSql.checkData(2, 1, None)
        tdSql.checkData(3, 0, "2023-11-17 16:29:00")
        tdSql.checkData(3, 1, "2023-11-17 16:29:00")

        tdSql.query(
            f"select a.ts, b.ts from sta a left join sta b on a.ts=b.ts order by a.ts desc;"
        )
        tdSql.checkRows(12)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, "2023-11-17 16:29:05")
        tdSql.checkData(1, 0, "2023-11-17 16:29:04")
        tdSql.checkData(1, 1, "2023-11-17 16:29:04")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left join tba2 b on a.ts=b.ts order by b.ts desc;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, "2023-11-17 16:29:03")
        tdSql.checkData(1, 1, "2023-11-17 16:29:00")
        tdSql.checkData(2, 1, None)
        tdSql.checkData(3, 1, None)

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left join tba2 b on a.ts=b.ts order by b.ts desc, a.ts desc;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:03")
        tdSql.checkData(0, 1, "2023-11-17 16:29:03")
        tdSql.checkData(1, 0, "2023-11-17 16:29:00")
        tdSql.checkData(1, 1, "2023-11-17 16:29:00")
        tdSql.checkData(2, 0, "2023-11-17 16:29:04")
        tdSql.checkData(2, 1, None)
        tdSql.checkData(3, 0, "2023-11-17 16:29:02")
        tdSql.checkData(3, 1, None)

        tdSql.query(f"select count(*) from tba1 a left join tba2 b on a.ts=b.ts;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4)

        tdSql.query(f"select count(a.*) from tba1 a left join tba2 b on a.ts=b.ts;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4)

        tdSql.query(f"select count(b.*) from tba1 a left join tba2 b on a.ts=b.ts;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

    def right_join(self):
        tdSql.execute(f"use test0;")

        tdSql.query(
            f"select a.col1, b.col1 from sta a right join sta b on a.ts = b.ts and a.ts < '2023-11-17 16:29:02' and b.ts < '2023-11-17 16:29:01' order by b.col1, a.col1;"
        )
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(3, 1, 2)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(4, 1, 3)
        tdSql.checkData(5, 0, None)
        tdSql.checkData(5, 1, 3)
        tdSql.checkData(6, 0, None)
        tdSql.checkData(6, 1, 4)
        tdSql.checkData(7, 0, None)
        tdSql.checkData(7, 1, 5)
        tdSql.checkData(8, 0, None)
        tdSql.checkData(8, 1, 5)
        tdSql.checkData(9, 0, None)
        tdSql.checkData(9, 1, 7)

        tdSql.query(
            f"select a.col1, b.col1 from sta a right join sta b on a.ts = b.ts where a.ts < '2023-11-17 16:29:02' and b.ts < '2023-11-17 16:29:01' order by a.col1, b.col1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(3, 1, 2)

        tdSql.query(
            f"select a.col1, b.col1 from sta a right join sta b on a.ts = b.ts;"
        )
        tdSql.checkRows(12)

        tdSql.query(
            f"select a.col1, b.col1 from tba1 a right join tba2 b on a.ts = b.ts order by a.col1, b.col1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(1, 1, 7)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(3, 0, 4)
        tdSql.checkData(3, 1, 5)

        tdSql.query(
            f"select a.col1, b.col1 from tba2 a right join tba1 b on a.ts = b.ts order by a.col1, b.col1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(1, 1, 5)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 0, 5)
        tdSql.checkData(3, 1, 4)

        tdSql.query(f"select count(*) from tba1 a right join tba2 b on a.ts=b.ts;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4)

        tdSql.query(f"select count(a.*) from tba1 a right join tba2 b on a.ts=b.ts;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        tdSql.query(f"select count(b.*) from tba1 a right join tba2 b on a.ts=b.ts;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4)

    def full_join(self):
        tdSql.execute(f"use test0;")

        tdSql.query(
            f"select a.col1, b.col1 from sta a full join sta b on a.ts = b.ts and a.ts < '2023-11-17 16:29:02' and b.ts < '2023-11-17 16:29:01' order by a.col1, b.col1;"
        )
        tdSql.checkRows(16)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(2, 1, 4)

        tdSql.query(
            f"select a.col1, b.col1 from sta a full join sta b on a.ts = b.ts where a.ts < '2023-11-17 16:29:02' and b.ts < '2023-11-17 16:29:01' order by a.col1, b.col1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(3, 1, 2)

        tdSql.query(f"select a.col1, b.col1 from sta a full join sta b on a.ts = b.ts;")
        tdSql.checkRows(12)

        tdSql.query(
            f"select a.col1, b.col1 from tba1 a full join tba2 b on a.ts = b.ts order by a.col1, b.col1;"
        )
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(1, 1, 7)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(3, 0, 3)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 0, 4)
        tdSql.checkData(4, 1, 5)
        tdSql.checkData(5, 0, 5)
        tdSql.checkData(5, 1, None)

        tdSql.query(
            f"select a.col1, b.col1 from tba2 a full join tba1 b on a.ts = b.ts order by a.col1, b.col1;"
        )
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(1, 1, 5)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 0, 3)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 0, 5)
        tdSql.checkData(4, 1, 4)
        tdSql.checkData(5, 0, 7)
        tdSql.checkData(5, 1, None)

        tdSql.query(
            f"select a.ts, b.ts from tba1 a full join tba2 b on a.ts = b.ts and a.ts < '2023-11-17 16:29:03' and b.ts < '2023-11-17 16:29:03' order by a.ts;"
        )
        tdSql.checkRows(7)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, "2023-11-17 16:29:00")
        tdSql.checkData(4, 0, "2023-11-17 16:29:02")
        tdSql.checkData(5, 0, "2023-11-17 16:29:03")
        tdSql.checkData(6, 0, "2023-11-17 16:29:04")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a full join tba2 b on a.ts = b.ts and a.ts < '2023-11-17 16:29:03' and b.ts < '2023-11-17 16:29:03' order by b.ts desc;"
        )
        tdSql.checkRows(7)
        tdSql.checkData(0, 1, "2023-11-17 16:29:05")
        tdSql.checkData(1, 1, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:01")
        tdSql.checkData(3, 1, "2023-11-17 16:29:00")
        tdSql.checkData(4, 1, None)
        tdSql.checkData(5, 1, None)
        tdSql.checkData(6, 1, None)

        tdSql.query(
            f"select b.ts, a.ts from tba1 a full join tba2 b on a.ts = b.ts and a.ts < '2023-11-17 16:29:03' and b.ts < '2023-11-17 16:29:03' order by b.ts desc, a.ts;"
        )
        tdSql.checkRows(7)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(1, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 0, "2023-11-17 16:29:01")
        tdSql.checkData(3, 0, "2023-11-17 16:29:00")
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, None)
        tdSql.checkData(6, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 1, None)
        tdSql.checkData(3, 1, "2023-11-17 16:29:00")
        tdSql.checkData(4, 1, "2023-11-17 16:29:02")
        tdSql.checkData(5, 1, "2023-11-17 16:29:03")
        tdSql.checkData(6, 1, "2023-11-17 16:29:04")

        tdSql.query(f"select count(*) from tba1 a full join tba2 b on a.ts=b.ts;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 6)

        tdSql.query(f"select count(a.*) from tba1 a full join tba2 b on a.ts=b.ts;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4)

        tdSql.query(f"select count(b.*) from tba1 a full join tba2 b on a.ts=b.ts;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4)

    def left_semi_join(self):
        tdSql.execute(f"use test0;")

        tdSql.query(
            f"select a.ts, b.ts from sta a left semi join sta b on a.ts = b.ts and a.ts < '2023-11-17 16:29:02' order by a.ts"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:00")
        tdSql.checkData(1, 1, "2023-11-17 16:29:00")
        tdSql.checkData(2, 0, "2023-11-17 16:29:01")
        tdSql.checkData(2, 1, "2023-11-17 16:29:01")

        tdSql.query(
            f"select a.col1, b.col1 from sta a left semi join sta b on a.ts = b.ts where a.ts < '2023-11-17 16:29:02' and b.ts < '2023-11-17 16:29:01' order by a.col1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 2)

        tdSql.query(
            f"select a.col1, b.col1 from sta a left semi join sta b on a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select a.col1, b.col1 from tba1 a left semi join tba2 b on a.ts = b.ts order by a.col1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 0, 4)
        tdSql.checkData(1, 1, 5)

        tdSql.query(
            f"select a.col1, b.col1 from tba2 a left semi join tba1 b on a.ts = b.ts order by a.col1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(1, 1, 4)

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left semi join tba2 b on a.ts = b.ts order by a.ts desc;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2023-11-17 16:29:03")
        tdSql.checkData(0, 1, "2023-11-17 16:29:03")
        tdSql.checkData(1, 0, "2023-11-17 16:29:00")
        tdSql.checkData(1, 1, "2023-11-17 16:29:00")

        tdSql.query(
            f"select a.ts, b.ts from sta a left semi join sta b on a.ts = b.ts order by a.ts desc;"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, "2023-11-17 16:29:05")
        tdSql.checkData(1, 0, "2023-11-17 16:29:04")
        tdSql.checkData(1, 1, "2023-11-17 16:29:04")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left semi join tba2 b on a.ts = b.ts order by b.ts desc;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2023-11-17 16:29:03")
        tdSql.checkData(0, 1, "2023-11-17 16:29:03")
        tdSql.checkData(1, 0, "2023-11-17 16:29:00")
        tdSql.checkData(1, 1, "2023-11-17 16:29:00")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left semi join tba2 b on a.ts = b.ts order by b.ts desc, a.ts;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2023-11-17 16:29:03")
        tdSql.checkData(0, 1, "2023-11-17 16:29:03")
        tdSql.checkData(1, 0, "2023-11-17 16:29:00")
        tdSql.checkData(1, 1, "2023-11-17 16:29:00")

        tdSql.error(
            f"select a.ts, b.ts from sta a left semi join sta b jlimit 3 where a.ts > b.ts;"
        )
        tdSql.error(
            f"select a.ts, b.ts from sta a left semi join sta b where a.ts > b.ts;"
        )
        tdSql.error(
            f"select a.ts, b.ts from sta a left semi join sta b on a.ts > 1 where a.ts = b.ts;"
        )

    def right_semi_join(self):
        tdSql.execute(f"use test0;")

        tdSql.query(
            f"select a.ts, b.ts from sta a right semi join sta b on a.ts = b.ts and b.ts < '2023-11-17 16:29:02' order by a.ts"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:00")
        tdSql.checkData(1, 1, "2023-11-17 16:29:00")
        tdSql.checkData(2, 0, "2023-11-17 16:29:01")
        tdSql.checkData(2, 1, "2023-11-17 16:29:01")

        tdSql.query(
            f"select a.col1, b.col1 from sta a right semi join sta b on a.ts = b.ts where a.ts < '2023-11-17 16:29:02' and b.ts < '2023-11-17 16:29:01' order by b.col1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 2)

        tdSql.query(
            f"select a.col1, b.col1 from sta a right semi join sta b on a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select a.col1, b.col1 from tba1 a right semi join tba2 b on a.ts = b.ts order by b.col1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 0, 4)
        tdSql.checkData(1, 1, 5)

        tdSql.query(
            f"select a.col1, b.col1 from tba2 a right semi join tba1 b on a.ts = b.ts order by a.col1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(1, 1, 4)

    def left_anti_join(self):
        tdSql.execute(f"use test0;")

        tdSql.query(
            f"select a.ts, b.ts from sta a left anti join sta b on a.ts = b.ts and a.ts < '2023-11-17 16:29:02' order by a.ts"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2023-11-17 16:29:02")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "2023-11-17 16:29:03")
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, None)
        tdSql.checkData(3, 0, "2023-11-17 16:29:04")
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 0, "2023-11-17 16:29:05")
        tdSql.checkData(4, 1, None)

        tdSql.query(
            f"select a.col1, b.col1 from sta a left anti join sta b on a.ts = b.ts and a.col1 != b.col1 where a.ts < '2023-11-17 16:29:02' order by a.col1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(0, 1, None)

        tdSql.query(
            f"select a.col1, b.col1 from sta a left anti join sta b on a.ts = b.ts;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left anti join tba2 b on a.ts = b.ts order by a.ts;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2023-11-17 16:29:02")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "2023-11-17 16:29:04")
        tdSql.checkData(1, 1, None)

        tdSql.query(
            f"select a.col1, b.col1 from tba2 a left anti join tba1 b on a.ts = b.ts order by a.col1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, 7)
        tdSql.checkData(1, 1, None)

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left anti join tba2 b on a.ts = b.ts order by a.ts desc;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2023-11-17 16:29:04")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(1, 1, None)

        tdSql.query(
            f"select a.ts, b.ts from sta a left anti join sta b on a.ts = b.ts and b.ts < '2023-11-17 16:29:03.000' order by a.ts desc;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "2023-11-17 16:29:04")
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, None)
        tdSql.checkData(3, 0, "2023-11-17 16:29:03")
        tdSql.checkData(3, 1, None)

        tdSql.query(
            f"select a.ts, b.ts from sta a left anti join sta b on a.ts = b.ts and b.ts < '2023-11-17 16:29:03.000' order by b.ts desc;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:03")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "2023-11-17 16:29:03")
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 0, "2023-11-17 16:29:04")
        tdSql.checkData(2, 1, None)
        tdSql.checkData(3, 0, "2023-11-17 16:29:05")
        tdSql.checkData(3, 1, None)

        tdSql.query(
            f"select a.ts, b.ts from sta a left anti join sta b on a.ts = b.ts and b.ts < '2023-11-17 16:29:03.000' order by a.ts desc, b.ts;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "2023-11-17 16:29:04")
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, None)
        tdSql.checkData(3, 0, "2023-11-17 16:29:03")
        tdSql.checkData(3, 1, None)

    def right_anti_join(self):
        tdSql.execute(f"use test0;")

        tdSql.query(
            f"select a.ts, b.ts from sta a right anti join sta b on a.ts = b.ts and a.ts < '2023-11-17 16:29:02' order by a.ts"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, "2023-11-17 16:29:02")
        tdSql.checkData(1, 0, None)
        tdSql.checkData(1, 1, "2023-11-17 16:29:03")
        tdSql.checkData(2, 0, None)
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, None)
        tdSql.checkData(3, 1, "2023-11-17 16:29:04")
        tdSql.checkData(4, 0, None)
        tdSql.checkData(4, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.col1, b.col1 from sta a right anti join sta b on a.ts = b.ts and a.col1 != b.col1 where a.ts < '2023-11-17 16:29:02' order by a.col1;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select a.col1, b.col1 from sta a right anti join sta b on a.ts = b.ts and a.col1 != b.col1 where b.ts < '2023-11-17 16:29:02' order by a.col1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, 3)

        tdSql.query(
            f"select a.col1, b.col1 from sta a right anti join sta b on a.ts = b.ts;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select a.ts, b.ts from tba1 a right anti join tba2 b on a.ts = b.ts order by a.ts;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, "2023-11-17 16:29:01")
        tdSql.checkData(1, 0, None)
        tdSql.checkData(1, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.col1, b.col1 from tba2 a right anti join tba1 b on a.ts = b.ts order by a.col1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(1, 1, 5)

    def left_asof_join(self):
        tdSql.execute(f"use test0;")

        tdSql.error(
            f"select a.col1, b.col1 from sta a left asof join sta b on a.ts = b.ts and a.ts < '2023-11-17 16:29:02' and b.ts < '2023-11-17 16:29:01' order by a.col1, b.col1;"
        )
        tdSql.query(
            f"select a.col1, b.col1 from sta a left asof join sta b on a.ts = b.ts order by a.col1, b.col1;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select a.col1, b.col1 from sta a left asof join sta b on a.ts >= b.ts order by a.col1, b.col1;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select a.col1, b.col1 from sta a left asof join sta b on a.ts = b.ts where a.ts < '2023-11-17 16:29:02' and b.ts < '2023-11-17 16:29:01' order by a.col1, b.col1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 2)

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left asof join tba2 b on a.ts = b.ts;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, "2023-11-17 16:29:04")
        tdSql.checkData(3, 1, None)

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left asof join tba2 b on b.ts = a.ts;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, "2023-11-17 16:29:04")
        tdSql.checkData(3, 1, None)

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left asof join tba2 b on a.ts >= b.ts ;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(1, 1, "2023-11-17 16:29:01")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, "2023-11-17 16:29:04")
        tdSql.checkData(3, 1, "2023-11-17 16:29:03")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left asof join tba2 b on b.ts <= a.ts ;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(1, 1, "2023-11-17 16:29:01")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, "2023-11-17 16:29:04")
        tdSql.checkData(3, 1, "2023-11-17 16:29:03")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left asof join tba2 b on a.ts > b.ts ;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(1, 1, "2023-11-17 16:29:01")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:01")
        tdSql.checkData(3, 0, "2023-11-17 16:29:04")
        tdSql.checkData(3, 1, "2023-11-17 16:29:03")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left asof join tba2 b on b.ts < a.ts ;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(1, 1, "2023-11-17 16:29:01")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:01")
        tdSql.checkData(3, 0, "2023-11-17 16:29:04")
        tdSql.checkData(3, 1, "2023-11-17 16:29:03")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left asof join tba2 b on a.ts <= b.ts ;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(1, 1, "2023-11-17 16:29:03")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, "2023-11-17 16:29:04")
        tdSql.checkData(3, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left asof join tba2 b on b.ts >= a.ts ;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(1, 1, "2023-11-17 16:29:03")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, "2023-11-17 16:29:04")
        tdSql.checkData(3, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left asof join tba2 b on a.ts < b.ts ;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:01")
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(1, 1, "2023-11-17 16:29:03")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:05")
        tdSql.checkData(3, 0, "2023-11-17 16:29:04")
        tdSql.checkData(3, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left asof join tba2 b on b.ts > a.ts ;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:01")
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(1, 1, "2023-11-17 16:29:03")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:05")
        tdSql.checkData(3, 0, "2023-11-17 16:29:04")
        tdSql.checkData(3, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, b.ts from tba2 a left asof join tba1 b on a.ts >= b.ts;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:01")
        tdSql.checkData(1, 1, "2023-11-17 16:29:00")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, "2023-11-17 16:29:05")
        tdSql.checkData(3, 1, "2023-11-17 16:29:04")

        tdSql.query(
            f"select a.ts, b.ts from tba2 a left asof join tba1 b on a.ts > b.ts;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "2023-11-17 16:29:01")
        tdSql.checkData(1, 1, "2023-11-17 16:29:00")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:02")
        tdSql.checkData(3, 0, "2023-11-17 16:29:05")
        tdSql.checkData(3, 1, "2023-11-17 16:29:04")

        tdSql.query(
            f"select a.ts, b.ts from tba2 a left asof join tba1 b on a.ts <= b.ts;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:01")
        tdSql.checkData(1, 1, "2023-11-17 16:29:02")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, "2023-11-17 16:29:05")
        tdSql.checkData(3, 1, None)

        tdSql.query(
            f"select a.ts, b.ts from tba2 a left asof join tba1 b on a.ts < b.ts;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:02")
        tdSql.checkData(1, 0, "2023-11-17 16:29:01")
        tdSql.checkData(1, 1, "2023-11-17 16:29:02")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:04")
        tdSql.checkData(3, 0, "2023-11-17 16:29:05")
        tdSql.checkData(3, 1, None)

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left asof join tba2 b on a.ts > b.ts jlimit 2"
        )
        tdSql.checkRows(7)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(1, 1, "2023-11-17 16:29:00")
        tdSql.checkData(2, 0, "2023-11-17 16:29:02")
        tdSql.checkData(2, 1, "2023-11-17 16:29:01")
        tdSql.checkData(3, 0, "2023-11-17 16:29:03")
        tdSql.checkData(3, 1, "2023-11-17 16:29:00")
        tdSql.checkData(4, 0, "2023-11-17 16:29:03")
        tdSql.checkData(4, 1, "2023-11-17 16:29:01")
        tdSql.checkData(5, 0, "2023-11-17 16:29:04")
        tdSql.checkData(5, 1, "2023-11-17 16:29:01")
        tdSql.checkData(6, 0, "2023-11-17 16:29:04")
        tdSql.checkData(6, 1, "2023-11-17 16:29:03")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left asof join tba2 b on a.ts > b.ts and a.col1=b.col1 jlimit 2 order by a.ts"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(1, 1, "2023-11-17 16:29:01")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, None)
        tdSql.checkData(3, 0, "2023-11-17 16:29:04")
        tdSql.checkData(3, 1, "2023-11-17 16:29:03")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left asof join tba2 b on a.ts > b.ts and a.col1=b.col1 jlimit 2 order by a.ts desc;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:04")
        tdSql.checkData(0, 1, "2023-11-17 16:29:03")
        tdSql.checkData(1, 0, "2023-11-17 16:29:03")
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 0, "2023-11-17 16:29:02")
        tdSql.checkData(2, 1, "2023-11-17 16:29:01")
        tdSql.checkData(3, 0, "2023-11-17 16:29:00")
        tdSql.checkData(3, 1, None)

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left asof join tba2 b on a.ts > b.ts and a.col1=b.col1 jlimit 2 order by b.ts desc;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, "2023-11-17 16:29:03")
        tdSql.checkData(1, 1, "2023-11-17 16:29:01")
        tdSql.checkData(2, 1, None)
        tdSql.checkData(3, 1, None)

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left asof join tba2 b on a.ts > b.ts and a.col1=b.col1 jlimit 2 order by b.ts desc, a.ts;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:04")
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(2, 0, "2023-11-17 16:29:00")
        tdSql.checkData(3, 0, "2023-11-17 16:29:03")

        tdSql.query(
            f"select a.ts, b.ts from sta a left asof join sta b on a.ts > b.ts and a.col1=b.col1 jlimit 2 order by a.ts"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "2023-11-17 16:29:00")
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 0, "2023-11-17 16:29:01")
        tdSql.checkData(2, 1, None)
        tdSql.checkData(3, 0, "2023-11-17 16:29:02")
        tdSql.checkData(3, 1, "2023-11-17 16:29:01")
        tdSql.checkData(4, 0, "2023-11-17 16:29:03")
        tdSql.checkData(4, 1, None)
        tdSql.checkData(5, 0, "2023-11-17 16:29:03")
        tdSql.checkData(5, 1, None)
        tdSql.checkData(6, 0, "2023-11-17 16:29:04")
        tdSql.checkData(6, 1, "2023-11-17 16:29:03")
        tdSql.checkData(7, 0, "2023-11-17 16:29:05")
        tdSql.checkData(7, 1, None)

        tdSql.query(
            f"select a.ts, b.ts from sta a left asof join sta b on a.ts >= b.ts and a.col1=b.col1 jlimit 2 order by a.ts, b.ts;"
        )
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:00")
        tdSql.checkData(1, 1, "2023-11-17 16:29:00")
        tdSql.checkData(2, 0, "2023-11-17 16:29:01")
        tdSql.checkData(2, 1, "2023-11-17 16:29:01")
        tdSql.checkData(3, 0, "2023-11-17 16:29:02")
        tdSql.checkData(3, 1, "2023-11-17 16:29:01")
        tdSql.checkData(4, 0, "2023-11-17 16:29:02")
        tdSql.checkData(4, 1, "2023-11-17 16:29:02")
        tdSql.checkData(5, 0, "2023-11-17 16:29:03")
        tdSql.checkData(5, 1, "2023-11-17 16:29:03")
        tdSql.checkData(6, 0, "2023-11-17 16:29:03")
        tdSql.checkData(6, 1, "2023-11-17 16:29:03")
        tdSql.checkData(7, 0, "2023-11-17 16:29:04")
        tdSql.checkData(7, 1, "2023-11-17 16:29:03")
        tdSql.checkData(8, 0, "2023-11-17 16:29:04")
        tdSql.checkData(8, 1, "2023-11-17 16:29:04")
        tdSql.checkData(9, 0, "2023-11-17 16:29:05")
        tdSql.checkData(9, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, b.ts from sta a left asof join sta b on a.ts < b.ts and a.col1=b.col1 jlimit 2 order by a.ts, b.ts"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "2023-11-17 16:29:00")
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 0, "2023-11-17 16:29:01")
        tdSql.checkData(2, 1, "2023-11-17 16:29:02")
        tdSql.checkData(3, 0, "2023-11-17 16:29:02")
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 0, "2023-11-17 16:29:03")
        tdSql.checkData(4, 1, None)
        tdSql.checkData(5, 0, "2023-11-17 16:29:03")
        tdSql.checkData(5, 1, "2023-11-17 16:29:04")
        tdSql.checkData(6, 0, "2023-11-17 16:29:04")
        tdSql.checkData(6, 1, None)
        tdSql.checkData(7, 0, "2023-11-17 16:29:05")
        tdSql.checkData(7, 1, None)

        tdSql.query(
            f"select a.ts, b.ts from sta a left asof join sta b on a.ts <= b.ts and a.col1=b.col1 jlimit 2 order by a.ts, b.ts"
        )
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:00")
        tdSql.checkData(1, 1, "2023-11-17 16:29:00")
        tdSql.checkData(2, 0, "2023-11-17 16:29:01")
        tdSql.checkData(2, 1, "2023-11-17 16:29:01")
        tdSql.checkData(3, 0, "2023-11-17 16:29:01")
        tdSql.checkData(3, 1, "2023-11-17 16:29:02")
        tdSql.checkData(4, 0, "2023-11-17 16:29:02")
        tdSql.checkData(4, 1, "2023-11-17 16:29:02")
        tdSql.checkData(5, 0, "2023-11-17 16:29:03")
        tdSql.checkData(5, 1, "2023-11-17 16:29:03")
        tdSql.checkData(6, 0, "2023-11-17 16:29:03")
        tdSql.checkData(6, 1, "2023-11-17 16:29:03")
        tdSql.checkData(7, 0, "2023-11-17 16:29:03")
        tdSql.checkData(7, 1, "2023-11-17 16:29:04")
        tdSql.checkData(8, 0, "2023-11-17 16:29:04")
        tdSql.checkData(8, 1, "2023-11-17 16:29:04")
        tdSql.checkData(9, 0, "2023-11-17 16:29:05")
        tdSql.checkData(9, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, b.ts from sta a left asof join sta b on a.ts = b.ts and a.col1=b.col1 jlimit 2 order by a.ts, b.ts"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:00")
        tdSql.checkData(1, 1, "2023-11-17 16:29:00")
        tdSql.checkData(2, 0, "2023-11-17 16:29:01")
        tdSql.checkData(2, 1, "2023-11-17 16:29:01")
        tdSql.checkData(3, 0, "2023-11-17 16:29:02")
        tdSql.checkData(3, 1, "2023-11-17 16:29:02")
        tdSql.checkData(4, 0, "2023-11-17 16:29:03")
        tdSql.checkData(4, 1, "2023-11-17 16:29:03")
        tdSql.checkData(5, 0, "2023-11-17 16:29:03")
        tdSql.checkData(5, 1, "2023-11-17 16:29:03")
        tdSql.checkData(6, 0, "2023-11-17 16:29:04")
        tdSql.checkData(6, 1, "2023-11-17 16:29:04")
        tdSql.checkData(7, 0, "2023-11-17 16:29:05")
        tdSql.checkData(7, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.t1, a.ts, b.ts from sta a left asof join sta b on a.ts <= b.ts and a.t1=b.t1 jlimit 2 order by a.t1, a.ts, b.ts;"
        )
        tdSql.checkRows(14)

        tdSql.query(
            f"select a.ts, b.ts from sta a left asof join sta b on a.ts <= b.ts and a.t1=b.t1 and a.col1=b.col1 jlimit 2 order by a.t1, a.ts, b.ts;"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(1, 1, "2023-11-17 16:29:02")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, "2023-11-17 16:29:04")
        tdSql.checkData(3, 1, "2023-11-17 16:29:04")
        tdSql.checkData(4, 0, "2023-11-17 16:29:00")
        tdSql.checkData(4, 1, "2023-11-17 16:29:00")
        tdSql.checkData(5, 0, "2023-11-17 16:29:01")
        tdSql.checkData(5, 1, "2023-11-17 16:29:01")
        tdSql.checkData(6, 0, "2023-11-17 16:29:03")
        tdSql.checkData(6, 1, "2023-11-17 16:29:03")
        tdSql.checkData(7, 0, "2023-11-17 16:29:05")
        tdSql.checkData(7, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, b.ts from sta a left asof join sta b on a.ts <= b.ts and a.t1=b.t1 and a.col1=b.col1 jlimit 2 where a.ts > '2023-11-17 16:29:00.000' order by a.t1, a.ts, b.ts;"
        )
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, "2023-11-17 16:29:02")
        tdSql.checkData(0, 1, "2023-11-17 16:29:02")
        tdSql.checkData(1, 0, "2023-11-17 16:29:03")
        tdSql.checkData(1, 1, "2023-11-17 16:29:03")
        tdSql.checkData(2, 0, "2023-11-17 16:29:04")
        tdSql.checkData(2, 1, "2023-11-17 16:29:04")
        tdSql.checkData(3, 0, "2023-11-17 16:29:01")
        tdSql.checkData(3, 1, "2023-11-17 16:29:01")
        tdSql.checkData(4, 0, "2023-11-17 16:29:03")
        tdSql.checkData(4, 1, "2023-11-17 16:29:03")
        tdSql.checkData(5, 0, "2023-11-17 16:29:05")
        tdSql.checkData(5, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select count(*) from sta a left asof join sta b on a.ts <= b.ts and a.t1=b.t1 and a.col1=b.col1 jlimit 2 where a.ts > '2023-11-17 16:29:00.000';"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 6)

        tdSql.query(
            f"select _wstart, count(*) from sta a left asof join sta b on a.ts <= b.ts and a.t1=b.t1 and a.col1=b.col1 jlimit 2 interval(1s);"
        )
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 0, "2023-11-17 16:29:01")
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 0, "2023-11-17 16:29:02")
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 0, "2023-11-17 16:29:03")
        tdSql.checkData(3, 1, 2)
        tdSql.checkData(4, 0, "2023-11-17 16:29:04")
        tdSql.checkData(4, 1, 1)
        tdSql.checkData(5, 0, "2023-11-17 16:29:05")
        tdSql.checkData(5, 1, 1)

        tdSql.query(
            f"select _wstart, count(*) from sta a left asof join sta b on a.ts <= b.ts and a.t1=b.t1 and a.col1=b.col1 jlimit 2 interval(1s) having(count(*) > 1);"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 0, "2023-11-17 16:29:03")
        tdSql.checkData(1, 1, 2)

        tdSql.query(f"select a.ts, b.ts from sta a left asof join sta b;")
        tdSql.checkRows(8)

        tdSql.query(f"select a.ts, b.ts from sta a left asof join sta b jlimit 3;")
        tdSql.checkRows(22)

        tdSql.query(
            f"select a.ts, b.ts from sta a left asof join sta b where a.ts > b.ts;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select a.ts, b.ts from sta a left asof join sta b jlimit 3 where a.ts > b.ts;"
        )
        tdSql.checkRows(10)

        tdSql.query(
            f"select a.ts, b.ts from sta a left asof join sta b order by a.ts desc;"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, "2023-11-17 16:29:05")
        tdSql.checkData(1, 0, "2023-11-17 16:29:04")
        tdSql.checkData(1, 1, "2023-11-17 16:29:04")

        tdSql.query(
            f"select a.ts, b.ts from sta a left asof join sta b jlimit 2 order by a.ts desc;"
        )
        tdSql.checkRows(16)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, "2023-11-17 16:29:05")
        tdSql.checkData(1, 0, "2023-11-17 16:29:05")
        tdSql.checkData(1, 1, "2023-11-17 16:29:04")
        tdSql.checkData(2, 0, "2023-11-17 16:29:04")
        tdSql.checkData(2, 1, "2023-11-17 16:29:04")
        tdSql.checkData(3, 0, "2023-11-17 16:29:04")
        tdSql.checkData(3, 1, "2023-11-17 16:29:03")

        tdSql.query(
            f"select a.col1 from sta a left asof join sta b on a.col1 = b.col1 and a.t1 = b.t1;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select a.ts, b.ts from sta a left asof join sta b on a.ts > b.ts order by a.ts desc;"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, "2023-11-17 16:29:04")
        tdSql.checkData(1, 0, "2023-11-17 16:29:04")
        tdSql.checkData(1, 1, "2023-11-17 16:29:03")

        tdSql.query(
            f"select a.ts, b.ts from sta a left asof join sta b on a.ts > b.ts jlimit 2 order by a.ts desc;"
        )
        tdSql.checkRows(14)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, "2023-11-17 16:29:04")
        tdSql.checkData(1, 0, "2023-11-17 16:29:05")
        tdSql.checkData(1, 1, "2023-11-17 16:29:03")

        tdSql.query(
            f"select a.ts, b.ts from sta a left asof join sta b on a.ts <= b.ts order by a.ts desc;"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, "2023-11-17 16:29:05")
        tdSql.checkData(1, 0, "2023-11-17 16:29:04")
        tdSql.checkData(1, 1, "2023-11-17 16:29:04")

        tdSql.query(
            f"select a.ts, b.ts from sta a left asof join sta b on a.ts <= b.ts jlimit 2 order by a.ts desc;"
        )
        tdSql.checkRows(15)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, "2023-11-17 16:29:05")
        tdSql.checkData(1, 0, "2023-11-17 16:29:04")
        tdSql.checkData(1, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, b.ts from sta a left asof join sta b on a.ts < b.ts order by a.ts desc;"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "2023-11-17 16:29:04")
        tdSql.checkData(1, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, b.ts from sta a left asof join sta b on a.ts < b.ts jlimit 2 order by a.ts desc;"
        )
        tdSql.checkRows(14)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "2023-11-17 16:29:04")
        tdSql.checkData(1, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, b.ts from sta a left asof join sta b on a.ts < b.ts jlimit 2 order by b.ts desc;"
        )
        tdSql.checkRows(14)
        tdSql.checkData(0, 1, "2023-11-17 16:29:05")
        tdSql.checkData(1, 1, "2023-11-17 16:29:05")
        tdSql.checkData(2, 1, "2023-11-17 16:29:05")
        tdSql.checkData(3, 1, "2023-11-17 16:29:04")
        tdSql.checkData(4, 1, "2023-11-17 16:29:04")

        tdSql.query(
            f"select a.ts, b.ts from sta a left asof join sta b on a.ts < b.ts jlimit 2 order by b.ts desc, a.ts;"
        )
        tdSql.checkRows(14)
        tdSql.checkData(0, 0, "2023-11-17 16:29:03")
        tdSql.checkData(1, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 0, "2023-11-17 16:29:04")
        tdSql.checkData(3, 0, "2023-11-17 16:29:03")
        tdSql.checkData(4, 0, "2023-11-17 16:29:03")
        tdSql.checkData(5, 0, "2023-11-17 16:29:01")
        tdSql.checkData(6, 0, "2023-11-17 16:29:02")
        tdSql.checkData(7, 0, "2023-11-17 16:29:02")

        tdSql.query(
            f"select a.ts, b.ts from sta a left asof join sta b on a.ts = b.ts order by a.ts desc;"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, "2023-11-17 16:29:05")
        tdSql.checkData(1, 0, "2023-11-17 16:29:04")
        tdSql.checkData(1, 1, "2023-11-17 16:29:04")

        tdSql.query(
            f"select a.ts, b.ts from sta a left asof join sta b on a.ts = b.ts jlimit 2 order by a.ts desc;"
        )
        tdSql.checkRows(12)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, "2023-11-17 16:29:05")
        tdSql.checkData(1, 0, "2023-11-17 16:29:04")
        tdSql.checkData(1, 1, "2023-11-17 16:29:04")

        tdSql.error(f"select a.ts, b.ts from sta a asof join sta b on a.ts = b.ts;")
        tdSql.error(
            f"select a.ts, b.ts from sta a full asof join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, b.ts from sta a left asof join sta b on a.ts != b.ts;"
        )
        tdSql.error(
            f"select a.ts, b.ts from sta a left asof join sta b on a.ts >=b.ts and a.col1=a.ts;"
        )
        tdSql.error(
            f"select a.ts, b.ts from sta a left asof join sta b on a.ts >=b.ts and a.col1 > 1;"
        )
        tdSql.error(
            f"select a.ts, b.ts from tba1 a left asof join tba2 b on a.ts > b.ts and a.col1 > b.col1 jlimit 2 order by a.ts;"
        )
        tdSql.error(
            f"select a.ts, b.ts from tba1 a left asof join tba2 b on a.ts > b.ts and a.col1 = 1 jlimit 2 order by a.ts;"
        )
        tdSql.error(
            f"select a.t1, a.ts, b.ts from sta a left asof join sta b on a.ts <= b.ts and a.t1=b.t1 and a.col1=b.col1 jlimit 2 having(a.ts>0) order by a.t1, a.ts, b.ts;"
        )
        tdSql.error(
            f"select count(*) from sta a left asof join sta b on a.ts <= b.ts and a.t1=b.t1 and a.col1=b.col1 jlimit 2 where a.ts > '2023-11-17 16:29:00.000' slimit 1;"
        )
        tdSql.error(
            f"select a.ts, b.ts from (select * from sta) a left asof join sta b where a.ts = b.ts;"
        )
        tdSql.error(f"select a.ts, b.ts from tba1 a left asof join tba2 b jlimit 1025;")

    def right_asof_join(self):
        tdSql.execute(f"use test0;")

        tdSql.error(
            f"select a.col1, b.col1 from sta a right asof join sta b on a.ts = b.ts and a.ts < '2023-11-17 16:29:02' and b.ts < '2023-11-17 16:29:01' order by a.col1, b.col1;"
        )
        tdSql.query(
            f"select a.col1, b.col1 from sta a right asof join sta b on a.ts = b.ts order by a.col1, b.col1;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select a.col1, b.col1 from sta a right asof join sta b on a.ts >= b.ts order by a.col1, b.col1;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select a.col1, b.col1 from sta a right asof join sta b on a.ts = b.ts where a.ts < '2023-11-17 16:29:02' and b.ts < '2023-11-17 16:29:01' order by b.col1;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 2)

        tdSql.query(
            f"select a.ts, b.ts from tba1 a right asof join tba2 b on a.ts = b.ts;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, None)
        tdSql.checkData(1, 1, "2023-11-17 16:29:01")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, None)
        tdSql.checkData(3, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a right asof join tba2 b on b.ts = a.ts;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, None)
        tdSql.checkData(1, 1, "2023-11-17 16:29:01")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, None)
        tdSql.checkData(3, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a right asof join tba2 b on a.ts >= b.ts ;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(1, 1, "2023-11-17 16:29:01")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, None)
        tdSql.checkData(3, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a right asof join tba2 b on b.ts <= a.ts ;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(1, 1, "2023-11-17 16:29:01")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, None)
        tdSql.checkData(3, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a right asof join tba2 b on a.ts > b.ts ;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:02")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(1, 1, "2023-11-17 16:29:01")
        tdSql.checkData(2, 0, "2023-11-17 16:29:04")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, None)
        tdSql.checkData(3, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a right asof join tba2 b on b.ts < a.ts ;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:02")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(1, 1, "2023-11-17 16:29:01")
        tdSql.checkData(2, 0, "2023-11-17 16:29:04")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, None)
        tdSql.checkData(3, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a right asof join tba2 b on a.ts <= b.ts ;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:00")
        tdSql.checkData(1, 1, "2023-11-17 16:29:01")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, "2023-11-17 16:29:04")
        tdSql.checkData(3, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a right asof join tba2 b on b.ts >= a.ts ;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:00")
        tdSql.checkData(1, 1, "2023-11-17 16:29:01")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, "2023-11-17 16:29:04")
        tdSql.checkData(3, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a right asof join tba2 b on a.ts < b.ts ;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:00")
        tdSql.checkData(1, 1, "2023-11-17 16:29:01")
        tdSql.checkData(2, 0, "2023-11-17 16:29:02")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, "2023-11-17 16:29:04")
        tdSql.checkData(3, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a right asof join tba2 b on b.ts > a.ts ;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:00")
        tdSql.checkData(1, 1, "2023-11-17 16:29:01")
        tdSql.checkData(2, 0, "2023-11-17 16:29:02")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, "2023-11-17 16:29:04")
        tdSql.checkData(3, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, b.ts from tba2 a right asof join tba1 b on a.ts >= b.ts;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:03")
        tdSql.checkData(1, 1, "2023-11-17 16:29:02")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, "2023-11-17 16:29:05")
        tdSql.checkData(3, 1, "2023-11-17 16:29:04")

        tdSql.query(
            f"select a.ts, b.ts from tba2 a right asof join tba1 b on a.ts > b.ts;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:01")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:03")
        tdSql.checkData(1, 1, "2023-11-17 16:29:02")
        tdSql.checkData(2, 0, "2023-11-17 16:29:05")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, "2023-11-17 16:29:05")
        tdSql.checkData(3, 1, "2023-11-17 16:29:04")

        tdSql.query(
            f"select a.ts, b.ts from tba2 a right asof join tba1 b on a.ts <= b.ts;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:01")
        tdSql.checkData(1, 1, "2023-11-17 16:29:02")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, "2023-11-17 16:29:03")
        tdSql.checkData(3, 1, "2023-11-17 16:29:04")

        tdSql.query(
            f"select a.ts, b.ts from tba2 a right asof join tba1 b on a.ts < b.ts;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:01")
        tdSql.checkData(1, 1, "2023-11-17 16:29:02")
        tdSql.checkData(2, 0, "2023-11-17 16:29:01")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, "2023-11-17 16:29:03")
        tdSql.checkData(3, 1, "2023-11-17 16:29:04")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a right asof join tba2 b on a.ts > b.ts jlimit 2"
        )
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, "2023-11-17 16:29:02")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:03")
        tdSql.checkData(1, 1, "2023-11-17 16:29:00")
        tdSql.checkData(2, 0, "2023-11-17 16:29:02")
        tdSql.checkData(2, 1, "2023-11-17 16:29:01")
        tdSql.checkData(3, 0, "2023-11-17 16:29:03")
        tdSql.checkData(3, 1, "2023-11-17 16:29:01")
        tdSql.checkData(4, 0, "2023-11-17 16:29:04")
        tdSql.checkData(4, 1, "2023-11-17 16:29:03")
        tdSql.checkData(5, 0, None)
        tdSql.checkData(5, 1, "2023-11-17 16:29:05")

    def left_win_join(self):
        tdSql.execute(f"use test0;")

        tdSql.error(
            f"select a.col1, b.col1 from sta a left window join sta b on a.ts = b.ts and a.ts < '2023-11-17 16:29:02' and b.ts < '2023-11-17 16:29:01' window_offset(-1s, 1s) order by a.col1, b.col1;"
        )
        tdSql.error(
            f"select a.col1, b.col1 from sta a left window join sta b on a.ts = b.ts order by a.col1, b.col1;"
        )
        tdSql.error(
            f"select a.col1, b.col1 from sta a left window join sta b on a.ts = b.ts window_offset(-1s, 1s) order by a.col1, b.col1;"
        )
        tdSql.query(
            f"select a.col1, b.col1 from sta a left window join sta b window_offset(-1s, 1s) order by a.col1, b.col1;"
        )
        tdSql.checkRows(28)

        tdSql.query(
            f"select a.col1, b.col1 from sta a left window join sta b window_offset(-1s, 1s) jlimit 2 order by a.col1, b.col1;"
        )
        tdSql.checkRows(16)

        tdSql.query(
            f"select a.col1, b.col1 from sta a left window join sta b window_offset(1s, 1s) order by a.col1, b.col1;"
        )
        tdSql.checkRows(9)

        tdSql.query(
            f"select a.col1, b.col1 from sta a left window join sta b window_offset(-1s, 1s) where a.ts < '2023-11-17 16:29:02' and b.ts < '2023-11-17 16:29:01' order by a.col1, b.col1;"
        )
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(3, 1, 2)
        tdSql.checkData(4, 0, 3)
        tdSql.checkData(4, 1, 1)
        tdSql.checkData(5, 0, 3)
        tdSql.checkData(5, 1, 2)

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left window join tba2 b window_offset(-1s, 1s)"
        )
        tdSql.checkRows(7)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:00")
        tdSql.checkData(1, 1, "2023-11-17 16:29:01")
        tdSql.checkData(2, 0, "2023-11-17 16:29:02")
        tdSql.checkData(2, 1, "2023-11-17 16:29:01")
        tdSql.checkData(3, 0, "2023-11-17 16:29:02")
        tdSql.checkData(3, 1, "2023-11-17 16:29:03")
        tdSql.checkData(4, 0, "2023-11-17 16:29:03")
        tdSql.checkData(4, 1, "2023-11-17 16:29:03")
        tdSql.checkData(5, 0, "2023-11-17 16:29:04")
        tdSql.checkData(5, 1, "2023-11-17 16:29:03")
        tdSql.checkData(6, 0, "2023-11-17 16:29:04")
        tdSql.checkData(6, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left window join tba2 b window_offset(-1s, 1s) jlimit 1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(1, 1, "2023-11-17 16:29:01")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, "2023-11-17 16:29:04")
        tdSql.checkData(3, 1, "2023-11-17 16:29:03")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left window join tba2 b window_offset(-1a, 1a) jlimit 1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, "2023-11-17 16:29:04")
        tdSql.checkData(3, 1, None)

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left window join tba2 b window_offset(-1h, 1h);"
        )
        tdSql.checkRows(16)

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left window join tba2 b window_offset(1h, -1h);"
        )
        tdSql.checkRows(16)

        tdSql.query(
            f"select a.ts, b.ts from tba1 a left window join tba2 b window_offset(1a, -1h);"
        )
        tdSql.checkRows(9)

        tdSql.query(
            f"select count(*) from sta a left window join sta b window_offset(-1s, 1s);"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 3)

        tdSql.query(
            f"select a.ts, count(*) from sta a left window join sta b window_offset(-1s, 1s);"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(2, 0, "2023-11-17 16:29:01")
        tdSql.checkData(2, 1, 4)

        tdSql.query(
            f"select a.ts, count(*) from sta a left window join sta b window_offset(-1s, 1s) where b.col1 between 2 and 4;"
        )
        tdSql.checkRows(7)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(2, 0, "2023-11-17 16:29:01")
        tdSql.checkData(2, 1, 3)

        tdSql.query(
            f"select a.ts, count(*) from sta a left window join sta b window_offset(-1s, 1s) where b.col1 between 2 and 4 having(count(*) != 2);"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2023-11-17 16:29:01")
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(2, 0, "2023-11-17 16:29:04")
        tdSql.checkData(2, 1, 1)

        tdSql.query(
            f"select a.ts, count(*) from sta a left window join sta b window_offset(-1s, 1s) where b.col1 between 2 and 4 having(count(*) != 2) order by count(*);"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2023-11-17 16:29:04")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(2, 1, 3)

        tdSql.query(
            f"select a.ts from sta a left window join sta b window_offset(-1s, 1s) where b.col1 between 2 and 4 order by count(*);"
        )
        tdSql.checkRows(7)

        tdSql.query(
            f"select a.ts from sta a left window join sta b window_offset(-1s, 1s) where b.col1 between 2 and 4 order by count(*), a.ts;"
        )
        tdSql.checkRows(7)
        tdSql.checkData(0, 0, "2023-11-17 16:29:04")
        tdSql.checkData(1, 0, "2023-11-17 16:29:00")
        tdSql.checkData(2, 0, "2023-11-17 16:29:00")
        tdSql.checkData(3, 0, "2023-11-17 16:29:03")
        tdSql.checkData(4, 0, "2023-11-17 16:29:03")
        tdSql.checkData(5, 0, "2023-11-17 16:29:01")
        tdSql.checkData(6, 0, "2023-11-17 16:29:02")

        tdSql.query(
            f"select a.ts, count(*),last(b.ts) from sta a left window join sta b window_offset(-1s, 1s);"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 2, "2023-11-17 16:29:01")
        tdSql.checkData(1, 0, "2023-11-17 16:29:00")
        tdSql.checkData(1, 2, "2023-11-17 16:29:01")
        tdSql.checkData(2, 0, "2023-11-17 16:29:01")
        tdSql.checkData(2, 2, "2023-11-17 16:29:02")
        tdSql.checkData(3, 0, "2023-11-17 16:29:02")
        tdSql.checkData(3, 2, "2023-11-17 16:29:03")
        tdSql.checkData(4, 0, "2023-11-17 16:29:03")
        tdSql.checkData(4, 2, "2023-11-17 16:29:04")
        tdSql.checkData(5, 0, "2023-11-17 16:29:03")
        tdSql.checkData(5, 2, "2023-11-17 16:29:04")
        tdSql.checkData(6, 0, "2023-11-17 16:29:04")
        tdSql.checkData(6, 2, "2023-11-17 16:29:05")
        tdSql.checkData(7, 0, "2023-11-17 16:29:05")
        tdSql.checkData(7, 2, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, count(*),last(b.ts) from sta a left window join sta b window_offset(-1s, 1s) limit 1;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select timetruncate(a.ts, 1m), count(*) from sta a left window join sta b window_offset(-1s, 1s);"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(2, 0, "2023-11-17 16:29:00")
        tdSql.checkData(2, 1, 4)

        tdSql.query(
            f"select a.ts+1s, count(*) from sta a left window join sta b window_offset(-1s, 1s);"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2023-11-17 16:29:01")
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(2, 0, "2023-11-17 16:29:02")
        tdSql.checkData(2, 1, 4)

        tdSql.query(
            f"select a.ts, b.ts from sta a left window join sta b on a.col1=b.col1 window_offset(-1s, 1s) order by a.col1, a.ts;"
        )
        tdSql.checkRows(12)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:00")
        tdSql.checkData(2, 0, "2023-11-17 16:29:01")
        tdSql.checkData(3, 0, "2023-11-17 16:29:01")

        tdSql.query(
            f"select a.ts, b.ts from sta a left window join sta b on a.t1=b.t1 window_offset(-1s, 1s) order by a.t1, a.ts, b.ts;"
        )
        tdSql.checkRows(14)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(1, 1, "2023-11-17 16:29:02")
        tdSql.checkData(2, 0, "2023-11-17 16:29:02")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, "2023-11-17 16:29:03")
        tdSql.checkData(3, 1, "2023-11-17 16:29:02")
        tdSql.checkData(4, 0, "2023-11-17 16:29:03")
        tdSql.checkData(4, 1, "2023-11-17 16:29:03")
        tdSql.checkData(5, 0, "2023-11-17 16:29:03")
        tdSql.checkData(5, 1, "2023-11-17 16:29:04")
        tdSql.checkData(6, 0, "2023-11-17 16:29:04")
        tdSql.checkData(6, 1, "2023-11-17 16:29:03")
        tdSql.checkData(7, 0, "2023-11-17 16:29:04")
        tdSql.checkData(7, 1, "2023-11-17 16:29:04")
        tdSql.checkData(8, 0, "2023-11-17 16:29:00")
        tdSql.checkData(8, 1, "2023-11-17 16:29:00")
        tdSql.checkData(9, 0, "2023-11-17 16:29:00")
        tdSql.checkData(9, 1, "2023-11-17 16:29:01")

        tdSql.query(
            f"select a.ts, b.ts from sta a left window join sta b on a.t1=b.t1 and a.col1=b.col1 window_offset(-1s, 1s) order by a.t1, a.ts, b.ts;"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(1, 1, "2023-11-17 16:29:02")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, "2023-11-17 16:29:04")
        tdSql.checkData(3, 1, "2023-11-17 16:29:04")
        tdSql.checkData(4, 0, "2023-11-17 16:29:00")
        tdSql.checkData(4, 1, "2023-11-17 16:29:00")
        tdSql.checkData(5, 0, "2023-11-17 16:29:01")
        tdSql.checkData(5, 1, "2023-11-17 16:29:01")
        tdSql.checkData(6, 0, "2023-11-17 16:29:03")
        tdSql.checkData(6, 1, "2023-11-17 16:29:03")
        tdSql.checkData(7, 0, "2023-11-17 16:29:05")
        tdSql.checkData(7, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, b.ts from sta a left window join sta b on a.t1=b.t1 and a.col1=b.col1 window_offset(-2s, -1s) order by a.t1, a.ts, b.ts;"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, None)
        tdSql.checkData(3, 0, "2023-11-17 16:29:04")
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 0, "2023-11-17 16:29:00")
        tdSql.checkData(4, 1, None)
        tdSql.checkData(5, 0, "2023-11-17 16:29:01")
        tdSql.checkData(5, 1, None)
        tdSql.checkData(6, 0, "2023-11-17 16:29:03")
        tdSql.checkData(6, 1, None)
        tdSql.checkData(7, 0, "2023-11-17 16:29:05")
        tdSql.checkData(7, 1, None)

        tdSql.query(
            f"SELECT a.ts, count(b.*) FROM tba1 a LEFT WINDOW JOIN tba2 b ON a.col1 = b.col1 WINDOW_OFFSET(-1s, 1s) where a.col1 > 1 order by a.ts;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2023-11-17 16:29:02")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, "2023-11-17 16:29:03")
        tdSql.checkData(1, 1, 0)
        tdSql.checkData(2, 0, "2023-11-17 16:29:04")
        tdSql.checkData(2, 1, 1)

        tdSql.query(
            f"SELECT a.ts, count(b.*) FROM tba1 a LEFT WINDOW JOIN tba2 b ON a.col1 = b.col1 WINDOW_OFFSET(-1s, 1s) order by a.col1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, 0)
        tdSql.checkData(3, 0, "2023-11-17 16:29:04")
        tdSql.checkData(3, 1, 1)

        tdSql.query(
            f"select a.ts, b.ts from sta a left window join sta b window_offset(-2s, -1s) order by a.ts desc;"
        )
        tdSql.checkRows(17)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, "2023-11-17 16:29:04")
        tdSql.checkData(1, 0, "2023-11-17 16:29:05")
        tdSql.checkData(1, 1, "2023-11-17 16:29:03")
        tdSql.checkData(2, 0, "2023-11-17 16:29:05")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")

        tdSql.query(
            f"select a.ts, b.ts from sta a left window join sta b window_offset(-2s, -1s) jlimit 1 order by a.ts desc;"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, "2023-11-17 16:29:03")
        tdSql.checkData(1, 0, "2023-11-17 16:29:04")
        tdSql.checkData(1, 1, "2023-11-17 16:29:02")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:01")

        tdSql.query(
            f"select a.ts, b.ts from sta a left window join sta b window_offset(-2s, -1s) jlimit 1;"
        )
        tdSql.checkRows(8)
        tdSql.checkData(5, 0, "2023-11-17 16:29:03")
        tdSql.checkData(5, 1, "2023-11-17 16:29:01")
        tdSql.checkData(6, 0, "2023-11-17 16:29:04")
        tdSql.checkData(6, 1, "2023-11-17 16:29:02")
        tdSql.checkData(7, 0, "2023-11-17 16:29:05")
        tdSql.checkData(7, 1, "2023-11-17 16:29:03")

        tdSql.query(
            f"select a.ts, b.ts from sta a left window join sta b window_offset(0s, 0s) order by a.ts desc;"
        )
        tdSql.checkRows(12)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, "2023-11-17 16:29:05")
        tdSql.checkData(1, 0, "2023-11-17 16:29:04")
        tdSql.checkData(1, 1, "2023-11-17 16:29:04")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")

        tdSql.query(
            f"select a.ts, b.ts from sta a left window join sta b window_offset(-1s, 1s) order by a.ts desc;"
        )
        tdSql.checkRows(28)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, "2023-11-17 16:29:05")
        tdSql.checkData(1, 0, "2023-11-17 16:29:05")
        tdSql.checkData(1, 1, "2023-11-17 16:29:04")
        tdSql.checkData(2, 0, "2023-11-17 16:29:04")
        tdSql.checkData(2, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, b.ts from sta a left window join sta b window_offset(1s, 2s) order by a.ts desc;"
        )
        tdSql.checkRows(16)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "2023-11-17 16:29:04")
        tdSql.checkData(1, 1, "2023-11-17 16:29:05")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, b.ts from sta a left window join sta b window_offset(1s, 2s) jlimit 1 order by a.ts desc;"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "2023-11-17 16:29:04")
        tdSql.checkData(1, 1, "2023-11-17 16:29:05")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:04")

        tdSql.query(
            f"select a.ts, b.ts from sta a left window join sta b window_offset(1s, 2s) order by b.ts desc;"
        )
        tdSql.checkRows(16)
        tdSql.checkData(0, 1, "2023-11-17 16:29:05")
        tdSql.checkData(1, 1, "2023-11-17 16:29:05")
        tdSql.checkData(2, 1, "2023-11-17 16:29:05")
        tdSql.checkData(3, 1, "2023-11-17 16:29:04")
        tdSql.checkData(4, 1, "2023-11-17 16:29:04")
        tdSql.checkData(5, 1, "2023-11-17 16:29:04")

        tdSql.query(
            f"select a.ts, b.ts from sta a left window join sta b window_offset(1s, 2s) order by b.ts;"
        )
        tdSql.checkRows(16)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 1, "2023-11-17 16:29:01")
        tdSql.checkData(2, 1, "2023-11-17 16:29:01")
        tdSql.checkData(3, 1, "2023-11-17 16:29:02")
        tdSql.checkData(4, 1, "2023-11-17 16:29:02")
        tdSql.checkData(5, 1, "2023-11-17 16:29:02")

        tdSql.query(
            f"select a.ts, b.ts from sta a left window join sta b on a.t1=b.t1 window_offset(-2s, -1s) order by a.ts desc, b.ts;"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, "2023-11-17 16:29:03")
        tdSql.checkData(1, 0, "2023-11-17 16:29:04")
        tdSql.checkData(1, 1, "2023-11-17 16:29:02")
        tdSql.checkData(2, 0, "2023-11-17 16:29:04")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")

        tdSql.query(
            f"select a.ts, b.ts from sta a left window join sta b on a.t1=b.t1 window_offset(0s, 0s) order by a.ts desc;"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, "2023-11-17 16:29:05")
        tdSql.checkData(1, 0, "2023-11-17 16:29:04")
        tdSql.checkData(1, 1, "2023-11-17 16:29:04")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")

        tdSql.query(
            f"select a.ts, b.ts from sta a left window join sta b on a.t1=b.t1 window_offset(-1s, 1s) order by a.ts desc, b.ts desc;"
        )
        tdSql.checkRows(14)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, "2023-11-17 16:29:05")
        tdSql.checkData(1, 0, "2023-11-17 16:29:04")
        tdSql.checkData(1, 1, "2023-11-17 16:29:04")
        tdSql.checkData(2, 0, "2023-11-17 16:29:04")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, "2023-11-17 16:29:03")
        tdSql.checkData(3, 1, "2023-11-17 16:29:04")

        tdSql.query(
            f"select a.ts, b.ts from sta a left window join sta b on a.t1=b.t1 window_offset(1s, 2s) order by a.ts desc, b.ts desc;"
        )
        tdSql.checkRows(9)
        tdSql.checkData(0, 0, "2023-11-17 16:29:05")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, "2023-11-17 16:29:04")
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:05")
        tdSql.checkData(3, 0, "2023-11-17 16:29:03")
        tdSql.checkData(3, 1, "2023-11-17 16:29:04")

        tdSql.query(
            f"select count(*) from sta a left window join sta b on a.t1=b.t1 window_offset(1s, 2s) order by a.ts desc;"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(4, 0, 2)
        tdSql.checkData(5, 0, 1)
        tdSql.checkData(6, 0, 1)
        tdSql.checkData(7, 0, 1)

        tdSql.query(
            f"select count(b.ts) from sta a left window join sta b on a.t1=b.t1 window_offset(1s, 2s) order by a.ts desc;"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(4, 0, 2)
        tdSql.checkData(5, 0, 1)
        tdSql.checkData(6, 0, 1)
        tdSql.checkData(7, 0, 1)

        tdSql.query(
            f"select a.col1, count(*) from sta a left window join sta b window_offset(-1s, 1s);"
        )
        tdSql.checkRows(8)

        tdSql.error(
            f"select b.col1, count(*) from sta a left window join sta b window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select b.ts, count(*) from sta a left window join sta b window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select a.ts, b.ts from sta a left window join sta b window_offset(-1s, 1s) having(b.ts > 0);"
        )
        tdSql.error(
            f"select a.ts, b.ts from sta a left window join sta b window_offset(-1s, 1s) having(a.col1 > 0);"
        )
        tdSql.error(
            f"select a.ts, b.ts from sta a left window join sta b window_offset(-1s, 1s) having(a.col1 > 0);"
        )
        tdSql.error(
            f'select a.ts, b.ts from sta a left window join sta b window_offset(-1s, 1s) having(a.ts > "2023-11-17 16:29:00.000");'
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b window_offset(-1s, 1s) where b.col1 between 2 and 4 having(a.ts > 0) order by count(*);"
        )
        tdSql.error(
            f"select a.ts, count(*),last(b.ts) from sta a left window join sta b window_offset(-1s, 1s) slimit 1;"
        )
        tdSql.error(
            f"select a.ts, b.ts from sta a left window join sta b on a.t1=1 window_offset(-1s, 1s) order by a.t1, a.ts;"
        )
        tdSql.error(
            f"select a.ts, b.ts from sta a left window join sta b on a.ts > 1 window_offset(-1s, 1s) where a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, b.ts from sta a left window join sta b on a.ts =b.ts window_offset(-1s, 1s) where a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, b.ts from sta a left window join sta b window_offset(-1, 1) where a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, b.ts from (select * from sta) a left window join sta b window_offset(-1s, 1s) where a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, b.ts from tba1 a left window join tba2 b window_offset(-10a, 1a) jlimit 1025;"
        )

    def right_win_join(self):
        tdSql.execute(f"use test0;")

        tdSql.error(
            f"select a.col1, b.col1 from sta a right window join sta b on a.ts = b.ts and a.ts < '2023-11-17 16:29:02' and b.ts < '2023-11-17 16:29:01' window_offset(-1s, 1s) order by a.col1, b.col1;"
        )
        tdSql.error(
            f"select a.col1, b.col1 from sta a right window join sta b on a.ts = b.ts order by a.col1, b.col1;"
        )
        tdSql.error(
            f"select a.col1, b.col1 from sta a right window join sta b on a.ts = b.ts window_offset(-1s, 1s) order by a.col1, b.col1;"
        )
        tdSql.query(
            f"select a.col1, b.col1 from sta a right window join sta b window_offset(-1s, 1s) order by a.col1, b.col1;"
        )
        tdSql.checkRows(28)

        tdSql.query(
            f"select a.col1, b.col1 from sta a right window join sta b window_offset(-1s, 1s) jlimit 2 order by a.col1, b.col1;"
        )
        tdSql.checkRows(16)

        tdSql.query(
            f"select a.col1, b.col1 from sta a right window join sta b window_offset(1s, 1s) order by a.col1, b.col1;"
        )
        tdSql.checkRows(9)

        tdSql.query(
            f"select a.col1, b.col1 from sta a right window join sta b window_offset(-1s, 1s) where a.ts < '2023-11-17 16:29:02' and b.ts < '2023-11-17 16:29:01' order by a.col1, b.col1;"
        )
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(3, 1, 2)
        tdSql.checkData(4, 0, 3)
        tdSql.checkData(4, 1, 1)
        tdSql.checkData(5, 0, 3)
        tdSql.checkData(5, 1, 2)

        tdSql.query(
            f"select a.ts, b.ts from tba1 a right window join tba2 b window_offset(-1s, 1s)"
        )
        tdSql.checkRows(7)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:00")
        tdSql.checkData(1, 1, "2023-11-17 16:29:01")
        tdSql.checkData(2, 0, "2023-11-17 16:29:02")
        tdSql.checkData(2, 1, "2023-11-17 16:29:01")
        tdSql.checkData(3, 0, "2023-11-17 16:29:02")
        tdSql.checkData(3, 1, "2023-11-17 16:29:03")
        tdSql.checkData(4, 0, "2023-11-17 16:29:03")
        tdSql.checkData(4, 1, "2023-11-17 16:29:03")
        tdSql.checkData(5, 0, "2023-11-17 16:29:04")
        tdSql.checkData(5, 1, "2023-11-17 16:29:03")
        tdSql.checkData(6, 0, "2023-11-17 16:29:04")
        tdSql.checkData(6, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a right window join tba2 b window_offset(-1s, 1s) order by b.ts desc;"
        )
        tdSql.checkRows(7)
        tdSql.checkData(0, 1, "2023-11-17 16:29:05")
        tdSql.checkData(1, 1, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 1, "2023-11-17 16:29:03")
        tdSql.checkData(4, 1, "2023-11-17 16:29:01")
        tdSql.checkData(5, 1, "2023-11-17 16:29:01")
        tdSql.checkData(6, 1, "2023-11-17 16:29:00")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a right window join tba2 b window_offset(-1s, 1s) order by a.ts desc;"
        )
        tdSql.checkRows(7)
        tdSql.checkData(0, 0, "2023-11-17 16:29:04")
        tdSql.checkData(1, 0, "2023-11-17 16:29:04")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, "2023-11-17 16:29:02")
        tdSql.checkData(4, 0, "2023-11-17 16:29:02")
        tdSql.checkData(5, 0, "2023-11-17 16:29:00")
        tdSql.checkData(6, 0, "2023-11-17 16:29:00")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a right window join tba2 b window_offset(-1s, 1s) order by b.ts desc, a.ts;"
        )
        tdSql.checkRows(7)
        tdSql.checkData(0, 0, "2023-11-17 16:29:04")
        tdSql.checkData(0, 1, "2023-11-17 16:29:05")
        tdSql.checkData(1, 0, "2023-11-17 16:29:02")
        tdSql.checkData(1, 1, "2023-11-17 16:29:03")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, "2023-11-17 16:29:04")
        tdSql.checkData(3, 1, "2023-11-17 16:29:03")
        tdSql.checkData(4, 0, "2023-11-17 16:29:00")
        tdSql.checkData(4, 1, "2023-11-17 16:29:01")
        tdSql.checkData(5, 0, "2023-11-17 16:29:02")
        tdSql.checkData(5, 1, "2023-11-17 16:29:01")
        tdSql.checkData(6, 0, "2023-11-17 16:29:00")
        tdSql.checkData(6, 1, "2023-11-17 16:29:00")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a right window join tba2 b window_offset(-1s, 1s) jlimit 1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:00")
        tdSql.checkData(1, 1, "2023-11-17 16:29:01")
        tdSql.checkData(2, 0, "2023-11-17 16:29:02")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, "2023-11-17 16:29:04")
        tdSql.checkData(3, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a right window join tba2 b window_offset(-1a, 1a) jlimit 1;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(0, 1, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, None)
        tdSql.checkData(1, 1, "2023-11-17 16:29:01")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(2, 1, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, None)
        tdSql.checkData(3, 1, "2023-11-17 16:29:05")

        tdSql.query(
            f"select a.ts, b.ts from tba1 a right window join tba2 b window_offset(-1h, 1h);"
        )
        tdSql.checkRows(16)

        tdSql.query(
            f"select a.ts, b.ts from tba1 a right window join tba2 b window_offset(1h, -1h);"
        )
        tdSql.checkRows(16)

        tdSql.query(
            f"select a.ts, b.ts from tba1 a right window join tba2 b window_offset(1a, -1h);"
        )
        tdSql.checkRows(9)

    def join_scalar1(self):
        tdSql.execute(f"use test0;")

        # timetruncate
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b on timetruncate(a.ts, 1h) = timetruncate(b.ts, 1h);"
        )
        tdSql.checkRows(64)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b on timetruncate(a.ts, 1h) = timetruncate(b.ts, 1h);"
        )
        tdSql.checkRows(64)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b on timetruncate(a.ts, 1h) = timetruncate(b.ts, 1h) and a.col1=b.col1;"
        )
        tdSql.checkRows(12)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b on timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.checkRows(16)

        tdSql.query(
            f"select a.ts, b.col1 from sta a left join sta b on timetruncate(b.ts, 1h) = a.ts;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select a.ts, b.col1,timetruncate(a.col1, 1h) from sta a left join sta b on a.ts = b.ts and timetruncate(a.col1, 1h) = timetruncate(a.col1, 1h);"
        )
        tdSql.checkRows(12)

        tdSql.query(
            f"select a.ts, b.col1 from sta a left semi join sta b on timetruncate(a.ts, 1h) = timetruncate(b.ts, 1h);"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select a.ts, b.col1 from sta a left anti join sta b on timetruncate(a.ts, 1h) = timetruncate(b.ts, 1h);"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select a.ts, b.col1 from sta a left asof join sta b on timetruncate(a.ts, 1h) > timetruncate(b.ts, 1h);"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select a.ts, b.col1 from sta a left asof join sta b on timetruncate(a.ts, 1h) >= timetruncate(b.ts, 1h) jlimit 0;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select a.ts, b.col1 from sta a left asof join sta b on timetruncate(a.ts, 1h) >= timetruncate(b.ts, 1h) jlimit 2;"
        )
        tdSql.checkRows(16)

        tdSql.query(
            f"select a.ts, b.col1 from sta a left asof join sta b on timetruncate(a.ts, 1h) >= timetruncate(b.ts, 1h) jlimit 8;"
        )
        tdSql.checkRows(64)

        tdSql.query(
            f"select a.ts, b.col1 from sta a left asof join sta b on timetruncate(a.ts, 1h) >= timetruncate(b.ts, 1h) jlimit 9;"
        )
        tdSql.checkRows(64)

        tdSql.query(
            f"select a.ts, b.ts from sta a left asof join sta b on a.col1 =b.col1 and timetruncate(a.ts, 1h) > timetruncate(b.ts, 1h) jlimit 2;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select a.ts, b.ts from sta a left asof join sta b on timetruncate(a.ts, 1h) >= timetruncate(b.ts, 1h) jlimit 2 where timetruncate(a.ts, 1h) >= timetruncate(b.ts, 1h);"
        )
        tdSql.checkRows(16)

        tdSql.error(
            f"select a.ts, b.ts from sta a left asof join sta b on a.ts=b.ts and a.ts=b.ts;"
        )
        tdSql.error(
            f"select a.ts, b.ts from sta a left asof join sta b on timetruncate(a.ts, 1h) > timetruncate(b.ts, 1h) and timetruncate(a.ts, 1h) > timetruncate(b.ts, 1h);"
        )
        tdSql.error(
            f"select a.ts, b.ts from sta a left asof join sta b on timetruncate(a.ts, 1h) >= timetruncate(b.ts, 1h) and a.ts > timetruncate(b.ts, 1s) jlimit 2;"
        )
        tdSql.error(
            f"select a.ts, b.ts from sta a left asof join sta b on a.ts > timetruncate(b.ts, 1s) and timetruncate(a.ts, 1h) >= timetruncate(b.ts, 1h) jlimit 2;"
        )
        tdSql.error(
            f"select a.ts, b.ts from sta a left asof join sta b on timetruncate(a.ts, 1h) >= timetruncate(b.ts, 1h) and a.ts =b.col1 jlimit 2;"
        )
        tdSql.error(
            f"select a.ts, b.col1 from sta a left join sta b on timetruncate(b.ts, 1h) + 1 = a.ts;"
        )
        tdSql.error(
            f"select a.ts, b.col1 from sta a left join sta b on timetruncate(b.ts, 1h) = a.ts + 1;"
        )
        tdSql.error(
            f"select a.ts, b.col1 from sta a left join sta b on b.ts + 1 = a.ts + 1;"
        )

        tdSql.execute(f"use testa;")

        tdSql.query(
            f"select sta1.ts, sta2.ts from sta1 full join sta2 on timetruncate(sta1.ts, 1a) = timetruncate(sta2.ts, 1a);"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2023-10-16 09:10:11.001")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(1, 1, "2023-10-16 09:10:11.002")

        tdSql.query(
            f"select sta1.ts, sta2.ts from sta1 full join sta2 on timetruncate(sta1.ts, 1a) = timetruncate(sta2.ts, 1a) and sta1.ts=sta2.ts;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select sta1.ts, sta2.ts from sta1 full join sta2 on timetruncate(sta1.ts, 1s) = timetruncate(sta2.ts, 1s);"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select sta1.ts, sta2.ts from sta1 full join sta2 on timetruncate(sta1.ts, 1m) = timetruncate(sta2.ts, 1m);"
        )
        tdSql.checkRows(16)

        tdSql.query(
            f"select sta1.ts, sta2.ts from sta1 full join sta2 on timetruncate(sta1.ts, 1h) = timetruncate(sta2.ts, 1h) order by timetruncate(sta1.ts, 1s) desc, timetruncate(sta2.ts, 1s);"
        )
        tdSql.checkRows(16)
        tdSql.checkData(0, 0, "2023-10-16 09:10:14.001")
        tdSql.checkData(0, 1, "2023-10-16 09:10:11.002")

        tdSql.query(
            f"select sta1.ts, sta2.ts from sta1 full join sta2 on timetruncate(sta1.ts, 1h) = timetruncate(sta2.ts, 1h, 0) order by timetruncate(sta1.ts, 1s) desc, timetruncate(sta2.ts, 1s);"
        )
        tdSql.checkRows(16)
        tdSql.checkData(0, 0, "2023-10-16 09:10:14.001")
        tdSql.checkData(0, 1, "2023-10-16 09:10:11.002")

        tdSql.query(
            f"select sta1.ts, sta2.ts from sta1 full join sta2 on timetruncate(sta1.ts, 1d) = timetruncate(sta2.ts, 1d, 0) order by timetruncate(sta1.ts, 1s) desc, timetruncate(sta2.ts, 1s);"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2023-10-16 09:10:14.001")
        tdSql.checkData(0, 1, None)

        tdSql.query(
            f"select sta1.ts,timetruncate(sta1.ts, 1d), sta2.ts, timetruncate(sta2.ts, 1d, 0) from sta1 left asof join sta2 on timetruncate(sta1.ts, 1d) >= timetruncate(sta2.ts, 1d, 0) order by timetruncate(sta1.ts, 1s) desc, timetruncate(sta2.ts, 1s);"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-10-16 09:10:14.001")
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, "2023-10-16 09:10:13.001")
        tdSql.checkData(1, 2, None)

        tdSql.query(
            f"select sta1.ts,timetruncate(sta1.ts, 1d, 0), sta2.ts, timetruncate(sta2.ts, 1d, 1) from sta1 left asof join sta2 on timetruncate(sta1.ts, 1d, 0) >= timetruncate(sta2.ts, 1d, 1) jlimit 4 order by timetruncate(sta1.ts, 1s) desc, timetruncate(sta2.ts, 1s);"
        )
        tdSql.checkRows(16)
        tdSql.checkData(0, 0, "2023-10-16 09:10:14.001")
        tdSql.checkData(0, 2, "2023-10-16 09:10:11.002")
        tdSql.checkData(1, 0, "2023-10-16 09:10:14.001")
        tdSql.checkData(1, 2, "2023-10-16 09:10:12.002")

        tdSql.query(
            f"select sta1.ts, sta2.ts from sta1 left asof join sta2 on timetruncate(sta1.ts, 1s, 0) > timetruncate(sta2.ts, 1s, 1) jlimit 4 order by timetruncate(sta1.ts, 1s) desc, timetruncate(sta2.ts, 1s);"
        )
        tdSql.checkRows(7)
        tdSql.checkData(0, 0, "2023-10-16 09:10:14.001")
        tdSql.checkData(0, 1, "2023-10-16 09:10:11.002")
        tdSql.checkData(1, 0, "2023-10-16 09:10:14.001")
        tdSql.checkData(1, 1, "2023-10-16 09:10:12.002")
        tdSql.checkData(2, 0, "2023-10-16 09:10:14.001")
        tdSql.checkData(2, 1, "2023-10-16 09:10:13.002")
        tdSql.checkData(3, 0, "2023-10-16 09:10:13.001")
        tdSql.checkData(3, 1, "2023-10-16 09:10:11.002")
        tdSql.checkData(4, 0, "2023-10-16 09:10:13.001")
        tdSql.checkData(4, 1, "2023-10-16 09:10:12.002")
        tdSql.checkData(5, 0, "2023-10-16 09:10:12.001")
        tdSql.checkData(5, 1, "2023-10-16 09:10:11.002")
        tdSql.checkData(6, 0, "2023-10-16 09:10:11.001")
        tdSql.checkData(6, 1, None)

        tdSql.query(
            f"select sta1.ts,timetruncate(sta1.ts, 1w, 0), sta2.ts,timetruncate(sta2.ts, 1w, 1) from sta1 left asof join sta2 on timetruncate(sta1.ts, 1w, 0) > timetruncate(sta2.ts, 1w, 1) jlimit 4 order by timetruncate(sta1.ts, 1s) desc, timetruncate(sta2.ts, 1s);"
        )
        tdSql.checkRows(16)
        tdSql.checkData(0, 0, "2023-10-16 09:10:14.001")
        tdSql.checkData(0, 2, "2023-10-16 09:10:11.002")
        tdSql.checkData(1, 0, "2023-10-16 09:10:14.001")
        tdSql.checkData(1, 2, "2023-10-16 09:10:12.002")

        tdSql.error(
            f"select count(*) from sta1 left window join sta2 on timetruncate(sta1.ts, 1h) = timetruncate(sta2.ts, 1h) window_offset(-1s, 1s);"
        )

        ##### conditions
        tdSql.execute(f"use test0;")

        # inner join
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b on timetruncate(a.ts, 1h) = timetruncate(b.ts, 1h) where a.ts = b.ts;"
        )
        tdSql.checkRows(12)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b on timetruncate(a.ts, 1h) = timetruncate(b.ts, 1h) where a.ts = b.ts or a.ts != b.ts;"
        )
        tdSql.checkRows(64)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1h);"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b on a.t1 = b.t1 where a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b on a.t1 = b.t1 or a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select count(*),last(a.col1) from sta a join sta b on (a.t1 = b.t1 or a.col1 > b.col1) and a.col1 > 1 where a.ts = b.ts and a.col1 > 2;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 7)
        tdSql.checkData(0, 1, 7)

        tdSql.query(
            f"select first(b.col1),count(b.t1),last(a.col1) from sta a join sta b on a.t1 = b.t1 and a.col1 > 1 and b.t1 is not null where a.ts = b.ts and b.col1 > 3 and a.t1 != 1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 7)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b on (a.t1 = b.t1 or a.col1 = b.col1) or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and (timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts);"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select a.tbname from sta a join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "tba2")

        tdSql.error(
            f"select a.tbname from sta a join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and count(a.col1) = 1;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b on a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is null;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or b.col1 > 3);"
        )
        tdSql.checkRows(6)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b where (a.t1 = b.t1 or a.col1 = b.col1) or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and (timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts);"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select a.tbname from sta a join sta b where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "tba2")

        tdSql.error(
            f"select a.tbname from sta a join sta b where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and count(a.col1) = 1;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b where a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is null;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a join sta b where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or b.col1 > 3);"
        )
        tdSql.checkRows(6)

        # left join
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b on timetruncate(a.ts, 1h) = timetruncate(b.ts, 1h) where a.ts = b.ts;"
        )
        tdSql.checkRows(12)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b on timetruncate(a.ts, 1h) = timetruncate(b.ts, 1h) where a.ts = b.ts or a.ts != b.ts;"
        )
        tdSql.checkRows(64)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1h);"
        )
        tdSql.checkRows(8)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b on a.t1 = b.t1 where a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b on a.t1 = b.t1 and a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b on a.t1 = b.t1 or a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.error(
            f"select count(*),last(a.col1) from sta a left join sta b on (a.t1 = b.t1 or a.col1 > b.col1) and a.col1 > 1 where a.ts = b.ts and a.col1 > 2;"
        )
        tdSql.query(
            f"select count(*),last(a.col1) from sta a left join sta b on (a.t1 = b.t1 or a.col1 > b.col1) and a.col1 > 1 and a.ts = b.ts and a.col1 > 2;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 9)
        tdSql.checkData(0, 1, 7)

        tdSql.error(
            f"select first(b.col1),count(b.t1),last(a.col1) from sta a left join sta b on a.t1 = b.t1 and a.col1 > 1 and b.t1 is not null where a.ts = b.ts and b.col1 > 3 and a.t1 != 1;"
        )
        tdSql.query(
            f"select first(b.col1),count(b.t1),last(a.col1),count(*) from sta a left join sta b on a.t1 = b.t1 and a.col1 > 1 and b.t1 is not null and a.ts = b.ts and b.col1 > 3 and a.t1 != 1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 7)
        tdSql.checkData(0, 3, 8)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b on (a.t1 = b.t1 or a.col1 = b.col1) or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and (timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts);"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select count(b.ts) from sta a left join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 8)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select count(b.col1) from sta a left join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select count(b.ts) from sta a left join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4)

        tdSql.query(
            f"select a.tbname from sta a left join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select count(b.*) from sta a left join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.error(
            f"select a.tbname from sta a left join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and count(a.col1) = 1;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b on a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select count(b.ts) from sta a left join sta b on a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select count(b.col1) from sta a left join sta b on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is null;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select count(b.col1) from sta a left join sta b on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is null;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or b.col1 > 3);"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select count(b.ts) from sta a left join sta b on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or b.col1 > 3);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 6)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b where (a.t1 = b.t1 or a.col1 = b.col1) or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and (timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts);"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.error(
            f"select a.tbname from sta a left join sta b where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.error(
            f"select a.tbname from sta a left join sta b where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and count(a.col1) = 1;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b where a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is null;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left join sta b where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or b.col1 > 3);"
        )

        # left semi join
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b on timetruncate(a.ts, 1h) = timetruncate(b.ts, 1h) where a.ts = b.ts;"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b on timetruncate(a.ts, 1h) = timetruncate(b.ts, 1h) where a.ts = b.ts or a.ts != b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1h);"
        )
        tdSql.checkRows(0)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b on a.t1 = b.t1 where a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b on a.t1 = b.t1 and a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b on a.t1 = b.t1 or a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.error(
            f"select count(*),last(a.col1) from sta a left semi join sta b on (a.t1 = b.t1 or a.col1 > b.col1) and a.col1 > 1 where a.ts = b.ts and a.col1 > 2;"
        )
        tdSql.query(
            f"select count(*),last(a.col1) from sta a left semi join sta b on (a.t1 = b.t1 or a.col1 > b.col1) and a.col1 > 1 and a.ts = b.ts and a.col1 > 2;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 6)
        tdSql.checkData(0, 1, 7)

        tdSql.error(
            f"select first(b.col1),count(b.t1),last(a.col1) from sta a left semi join sta b on a.t1 = b.t1 and a.col1 > 1 and b.t1 is not null where a.ts = b.ts and b.col1 > 3 and a.t1 != 1;"
        )
        tdSql.query(
            f"select first(b.col1),count(b.t1),last(a.col1),count(*) from sta a left semi join sta b on a.t1 = b.t1 and a.col1 > 1 and b.t1 is not null and a.ts = b.ts and b.col1 > 3 and a.t1 != 1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 7)
        tdSql.checkData(0, 3, 2)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b on (a.t1 = b.t1 or a.col1 = b.col1) or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and (timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts);"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select count(b.ts) from sta a left semi join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 8)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select count(b.col1) from sta a left semi join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select count(b.ts) from sta a left semi join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4)

        tdSql.query(
            f"select a.tbname from sta a left semi join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select count(b.*) from sta a left semi join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.error(
            f"select a.tbname from sta a left semi join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and count(a.col1) = 1;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b on a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select count(b.ts) from sta a left semi join sta b on a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select count(b.col1) from sta a left semi join sta b on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is null;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select count(b.col1) from sta a left semi join sta b on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is null;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or b.col1 > 3);"
        )
        tdSql.checkRows(6)

        tdSql.query(
            f"select count(b.ts) from sta a left semi join sta b on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or b.col1 > 3);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 6)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b where (a.t1 = b.t1 or a.col1 = b.col1) or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and (timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts);"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.error(
            f"select a.tbname from sta a left semi join sta b where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.error(
            f"select a.tbname from sta a left semi join sta b where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and count(a.col1) = 1;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b where a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is null;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left semi join sta b where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or b.col1 > 3);"
        )

        # left anti join
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b on timetruncate(a.ts, 1h) = timetruncate(b.ts, 1h) where a.ts = b.ts;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b on timetruncate(a.ts, 1h) = timetruncate(b.ts, 1h) where a.ts = b.ts or a.ts != b.ts;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1h);"
        )
        tdSql.checkRows(8)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b on a.t1 = b.t1 where a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b on a.t1 = b.t1 and a.ts = b.ts;"
        )
        tdSql.checkRows(0)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b on a.t1 = b.t1 or a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.checkRows(0)

        tdSql.error(
            f"select count(*),last(a.col1) from sta a left anti join sta b on (a.t1 = b.t1 or a.col1 > b.col1) and a.col1 > 1 where a.ts = b.ts and a.col1 > 2;"
        )
        tdSql.query(
            f"select count(*),count(a.col1) from sta a left anti join sta b on (a.t1 = b.t1 or a.col1 > b.col1) and a.col1 > 1 and a.ts = b.ts and a.col1 > 2;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(0, 1, 2)

        tdSql.error(
            f"select first(b.col1),count(b.t1),last(a.col1) from sta a left anti join sta b on a.t1 = b.t1 and a.col1 > 1 and b.t1 is not null where a.ts = b.ts and b.col1 > 3 and a.t1 != 1;"
        )
        tdSql.query(
            f"select first(b.col1),count(b.t1),last(a.col1),count(*) from sta a left anti join sta b on a.t1 = b.t1 and a.col1 > 1 and b.t1 is not null and a.ts = b.ts and b.col1 > 3 and a.t1 != 1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, 5)
        tdSql.checkData(0, 3, 6)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.checkRows(0)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b on (a.t1 = b.t1 or a.col1 = b.col1) or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and (timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts);"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select count(b.ts) from sta a left anti join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.checkRows(6)

        tdSql.query(
            f"select count(b.col1) from sta a left anti join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select count(b.ts) from sta a left anti join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query(
            f"select a.tbname from sta a left anti join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.checkRows(7)

        tdSql.query(
            f"select count(b.*) from sta a left anti join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.error(
            f"select a.tbname from sta a left anti join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and count(a.col1) = 1;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b on a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select count(b.ts) from sta a left anti join sta b on a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select count(b.col1) from sta a left anti join sta b on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is null;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select count(b.col1) from sta a left anti join sta b on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is null;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or b.col1 > 3);"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select count(b.ts) from sta a left anti join sta b on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or b.col1 > 3);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b where (a.t1 = b.t1 or a.col1 = b.col1) or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and (timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts);"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.error(
            f"select a.tbname from sta a left anti join sta b where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.error(
            f"select a.tbname from sta a left anti join sta b where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and count(a.col1) = 1;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b where a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is null;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left anti join sta b where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or b.col1 > 3);"
        )

        # left asof join
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b on timetruncate(a.ts, 1h) = timetruncate(b.ts, 1h) where a.ts = b.ts;"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b on timetruncate(a.ts, 1h) = timetruncate(b.ts, 1h) where a.ts = b.ts or a.ts != b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1h);"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b on a.t1 = b.t1 where a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b on a.t1 = b.t1 and a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b on a.t1 = b.t1 or a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b on a.ts >= b.ts and a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.error(
            f"select count(*),last(a.col1) from sta a left asof join sta b on (a.t1 = b.t1 or a.col1 > b.col1) and a.col1 > 1 where a.ts = b.ts and a.col1 > 2;"
        )
        tdSql.error(
            f"select count(*),count(a.col1) from sta a left asof join sta b on (a.t1 = b.t1 or a.col1 > b.col1) and a.col1 > 1 and a.ts = b.ts and a.col1 > 2;"
        )
        tdSql.error(
            f"select first(b.col1),count(b.t1),last(a.col1) from sta a left asof join sta b on a.t1 = b.t1 and a.col1 > 1 and b.t1 is not null where a.ts = b.ts and b.col1 > 3 and a.t1 != 1;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b on (a.t1 = b.t1 or a.col1 = b.col1) or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and (timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts);"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.error(
            f"select count(b.ts) from sta a left asof join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select count(b.col1) from sta a left asof join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts where b.col1 > 1 or a.col1 > 2;"
        )
        tdSql.checkRows(7)

        tdSql.error(
            f"select a.tbname from sta a left asof join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.query(
            f"select a.tbname from sta a left asof join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts where a.col1 = b.col1 + 1;"
        )
        tdSql.error(
            f"select a.tbname from sta a left asof join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and count(a.col1) = 1;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b on a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b on a.t1 = b.t1 and a.ts = b.ts where a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b on a.t1 = b.t1 and a.ts = b.ts where (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select count(b.col1) from sta a left asof join sta b on a.t1 = b.t1 and a.ts = b.ts where (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b on a.t1 = b.t1 and a.ts = b.ts where (a.col1 < 3 or b.col1 > 3);"
        )
        tdSql.checkRows(6)

        tdSql.query(
            f"select count(b.ts) from sta a left asof join sta b on a.t1 = b.t1 and a.ts = b.ts where (a.col1 < 3 or b.col1 > 3);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 6)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b where (a.t1 = b.t1 or a.col1 = b.col1) or a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and (timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts);"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.query(
            f"select a.tbname from sta a left asof join sta b where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.error(
            f"select a.tbname from sta a left asof join sta b where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and count(a.col1) = 1;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b where a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is null;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left asof join sta b where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or b.col1 > 3);"
        )

        # left window join
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b on timetruncate(a.ts, 1h) = timetruncate(b.ts, 1h) window_offset(-1s, 1s) where a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1h) window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) where a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b on a.t1 = b.t1 and a.ts = b.ts window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b on a.t1 = b.t1 or a.col1 = b.col1 window_offset(-1s, 1s) where a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b on a.ts >= b.ts and a.ts = b.ts window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select count(*),last(a.col1) from sta a left window join sta b on (a.t1 = b.t1 or a.col1 > b.col1) and a.col1 > 1 window_offset(-1s, 1s) where a.ts = b.ts and a.col1 > 2;"
        )
        tdSql.error(
            f"select count(*),count(a.col1) from sta a left window join sta b on (a.t1 = b.t1 or a.col1 > b.col1) and a.col1 > 1 and a.ts = b.ts and a.col1 > 2  window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select first(b.col1),count(b.t1),last(a.col1) from sta a left window join sta b on a.t1 = b.t1 and a.col1 > 1 and b.t1 is not null window_offset(-1s, 1s) where a.ts = b.ts and b.col1 > 3 and a.t1 != 1;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b on (a.t1 = b.t1 or a.col1 = b.col1) or a.ts = b.ts  window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and (timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts) window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select count(b.ts) from sta a left window join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select count(b.col1) from sta a left window join sta b on a.t1 = b.t1 and a.col1 = b.col1 window_offset(-1s, 1s);"
        )
        tdSql.checkRows(8)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b on a.t1 = b.t1 and b.col1 > 1 window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) where b.col1 > 1 or a.col1 > 2;"
        )
        tdSql.checkRows(13)

        tdSql.error(
            f"select a.tbname from sta a left window join sta b on a.t1 = b.t1 and a.col1 = b.col1 + 1 window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select a.tbname from sta a left window join sta b on a.t1 = b.t1 and count(a.col1) = 1;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b on a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5 window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b on a.t1 = b.t1 and a.ts = b.ts window_offset(-1s, 1s) where a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) where (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.checkRows(7)

        tdSql.query(
            f"select a.ts from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) where (a.col1 < 3 or a.col1 > 4) and b.col1 is not null having(count(a.ts) > 0);"
        )
        tdSql.query(
            f"select a.ts from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) where a.col1 < 3 or a.col1 > 4 having(count(a.ts) > 1);"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:04")

        tdSql.error(
            f"select a.ts from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) where a.col1 < 3 or a.col1 > 4 having(count(a.ts) > 1) order by b.col1;"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) where a.col1 < 3 or a.col1 > 4 having(count(a.ts) > 1) order by b.tbname;"
        )
        tdSql.query(
            f"select count(b.col1) from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) where (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.query(
            f"select count(b.col1),a.t1,a.ts from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) where a.col1 < 3 or a.col1 > 4 order by 2,3;"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(4, 0, 1)

        tdSql.error(
            f"select count(b.col1) from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) where a.col1 < 3 or a.col1 > 4 order by b.col1;"
        )
        tdSql.query(
            f"select count(b.col1) c from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) where a.col1 < 3 or a.col1 > 4 order by a.col1, c;"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(4, 0, 1)

        tdSql.query(
            f"select count(b.col1) from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) where a.ts > 0 and b.col1 is not null;"
        )
        tdSql.query(
            f"select count(b.col1) from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) where b.ts > 0;"
        )
        tdSql.query(
            f"select count(b.col1) from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) where b.tbname > 'a';"
        )
        tdSql.query(
            f"select count(b.col1) from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) where b.t1 > 1;"
        )
        tdSql.query(
            f"select count(b.col1) from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) where b.col1 > 1;"
        )
        tdSql.query(
            f"select count(b.col1) from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s, 1s) where b.col1 > 1;"
        )
        tdSql.query(
            f"select count(b.col1) from sta a left window join tba1 b window_offset(-1s, 1s) where b.tbname > 'a';"
        )
        tdSql.query(
            f"select count(b.col1) from sta a left window join tba1 b window_offset(-1s, 1s) where b.t1 > 1;"
        )
        tdSql.query(
            f"select count(b.col1) from sta a left window join tba1 b window_offset(-1s, 1s) where b.col1 > 1;"
        )
        tdSql.query(
            f"select count(b.col1) from sta a left window join tba1 b on a.col1 = b.col1 window_offset(-1s, 1s) where b.col1 > 1;"
        )
        tdSql.query(
            f"select count(b.col1) from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) where a.tbname > 'tba1';"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)

        tdSql.query(
            f"select count(b.col1) from sta a left window join tba2 b on a.t1 = b.t1 window_offset(-1s, 1s) where b.tbname < 'tba2';"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query(
            f"select count(b.col1) from sta a left window join tba2 b window_offset(-1s, 1s) where b.tbname < 'tba2';"
        )
        tdSql.query(
            f"select count(b.col1) from sta a left window join tba2 b window_offset(-1s, 1s) where b.t1 < 2;"
        )
        tdSql.query(
            f"select count(b.col1) from sta a left window join tba2 b window_offset(-1s, 1s) where b.col1 < 1;"
        )
        tdSql.error(
            f"select count(b.col1) from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) order by b.tbname;"
        )
        tdSql.error(
            f"select count(b.col1) from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) order by b.col1;"
        )
        tdSql.query(
            f"select count(b.col1) from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) order by b.t1;"
        )
        tdSql.error(
            f"select count(b.col1) from sta a left window join sta b window_offset(-1s, 1s) order by b.t1;"
        )
        tdSql.query(
            f"select count(b.col1) from sta a left window join tba1 b window_offset(-1s, 1s) order by b.tbname;"
        )
        tdSql.error(
            f"select count(b.col1) from sta a left window join tba1 b window_offset(-1s, 1s) order by b.col1;"
        )
        tdSql.query(
            f"select count(b.col1) from sta a left window join tba1 b window_offset(-1s, 1s) order by b.t1;"
        )
        tdSql.query(
            f"select count(b.col1) from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) order by a.tbname, a.ts;"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(4, 0, 2)
        tdSql.checkData(5, 0, 2)
        tdSql.checkData(6, 0, 1)
        tdSql.checkData(7, 0, 1)

        tdSql.query(
            f"select count(b.col1) from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) jlimit 0 order by a.tbname, a.ts;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select count(b.col1) from sta a left window join tba2 b on a.t1 = b.t1 window_offset(-1s, 1s) order by b.tbname, a.ts;"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(2, 0, 0)
        tdSql.checkData(3, 0, 0)
        tdSql.checkData(4, 0, 2)
        tdSql.checkData(5, 0, 2)
        tdSql.checkData(6, 0, 1)
        tdSql.checkData(7, 0, 1)

        tdSql.error(
            f"select count(b.col1), b.tbname from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select count(b.col1), b.t1 from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select count(b.col1), b.col1 from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select count(b.col1), b.col1 from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select count(b.col1), b.tbname from sta a left window join tba1 b on a.t1 = b.t1 window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select count(b.col1), b.t1 from sta a left window join tba1 b on a.t1 = b.t1 window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select count(b.col1), b.t1 from sta a left window join tba1 b window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select count(b.col1), b.col1 from sta a left window join tba1 b on a.t1 = b.t1 window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select count(b.col1), b.col1 from sta a left window join tba1 b on a.col1 = b.col1 window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select count(b.col1), b.col1 from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select count(b.col1), a.tbname from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select count(b.col1), a.col1 from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select count(b.col1), a.t1 from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select count(b.col1), b.tbname from sta a left window join tba2 b on a.t1 = b.t1 window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select first(b.col1) from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) where b.ts > 0;"
        )
        tdSql.query(
            f"select first(a.col1) from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) where b.ts > 0;"
        )
        tdSql.query(
            f"select count(b.col1) from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) where a.ts > 0;"
        )
        tdSql.query(
            f"select count(b.col1) from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) where a.col1 > 0;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b window_offset(-1s, 1s) where (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b window_offset(-1s, 1s) where (a.t1 = b.t1 or a.col1 = b.col1) or a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b window_offset(-1s, 1s) where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b window_offset(-1s, 1s) where (a.t1 = b.t1 or a.col1 = b.col1) and (timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts);"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b window_offset(-1s, 1s) where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b window_offset(-1s, 1s) where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b window_offset(-1s, 1s) where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b window_offset(-1s, 1s) where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.query(
            f"select a.tbname from sta a left window join sta b window_offset(-1s, 1s) where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.error(
            f"select a.tbname from sta a left window join sta b window_offset(-1s, 1s) where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and count(a.col1) = 1;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b window_offset(-1s, 1s) where a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b window_offset(-1s, 1s) where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b window_offset(-1s, 1s) where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is null;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a left window join sta b window_offset(-1s, 1s) where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or b.col1 > 3);"
        )
        tdSql.error(
            f"select distinct count(b.col1) from sta a left window join tba2 b on a.t1 = b.t1 window_offset(-1s, 1s) order by b.tbname, a.ts;"
        )
        tdSql.query(
            f"select distinct count(b.col1) c from sta a left window join tba2 b on a.t1 = b.t1 window_offset(-1s, 1s) order by c;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 2)

    def join_scalar2(self):
        tdSql.execute(f"use test0;")

        ##### conditions
        tdSql.execute(f"use test0;")

        # full join
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a full join sta b on timetruncate(a.ts, 1h) = timetruncate(b.ts, 1h) where a.ts = b.ts;"
        )
        tdSql.checkRows(12)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a full join sta b on timetruncate(a.ts, 1h) = timetruncate(b.ts, 1h) where a.ts = b.ts or a.ts != b.ts;"
        )
        tdSql.checkRows(64)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a full join sta b on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1h);"
        )
        tdSql.checkRows(16)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a full join sta b on a.t1 = b.t1 where a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a full join sta b on a.t1 = b.t1 or a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.error(
            f"select count(*),last(a.col1) from sta a full join sta b on (a.t1 = b.t1 or a.col1 > b.col1) and a.col1 > 1 where a.ts = b.ts and a.col1 > 2;"
        )
        tdSql.error(
            f"select first(b.col1),count(b.t1),last(a.col1) from sta a full join sta b on a.t1 = b.t1 and a.col1 > 1 and b.t1 is not null where a.ts = b.ts and b.col1 > 3 and a.t1 != 1;"
        )

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a full join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a full join sta b on (a.t1 = b.t1 or a.col1 = b.col1) or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a full join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a full join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and (timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts);"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a full join sta b on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.checkRows(14)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a full join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.checkRows(14)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a full join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.checkRows(15)

        tdSql.query(
            f"select a.tbname from sta a full join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.checkRows(15)

        tdSql.error(
            f"select a.tbname from sta a full join sta b on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and count(a.col1) = 1;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a full join sta b on a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.checkRows(13)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a full join sta b on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.checkRows(11)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a full join sta b on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is null;"
        )
        tdSql.checkRows(16)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a full join sta b on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or b.col1 > 3);"
        )
        tdSql.checkRows(10)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a full join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a full join sta b where (a.t1 = b.t1 or a.col1 = b.col1) or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a full join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a full join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and (timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts);"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a full join sta b where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a full join sta b where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a full join sta b where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.error(
            f"select a.tbname from sta a full join sta b where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.error(
            f"select a.tbname from sta a full join sta b where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and count(a.col1) = 1;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a full join sta b where a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a full join sta b where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a full join sta b where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is null;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta a full join sta b where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or b.col1 > 3);"
        )

        # right join
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a on timetruncate(a.ts, 1h) = timetruncate(b.ts, 1h) where a.ts = b.ts;"
        )
        tdSql.checkRows(12)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a on timetruncate(a.ts, 1h) = timetruncate(b.ts, 1h) where a.ts = b.ts or a.ts != b.ts;"
        )
        tdSql.checkRows(64)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1h);"
        )
        tdSql.checkRows(8)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a on a.t1 = b.t1 where a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a on a.t1 = b.t1 and a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a on a.t1 = b.t1 or a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.error(
            f"select count(*),last(a.col1) from sta b right join sta a on (a.t1 = b.t1 or a.col1 > b.col1) and a.col1 > 1 where a.ts = b.ts and a.col1 > 2;"
        )
        tdSql.query(
            f"select count(*),last(a.col1) from sta b right join sta a on (a.t1 = b.t1 or a.col1 > b.col1) and a.col1 > 1 and a.ts = b.ts and a.col1 > 2;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 9)
        tdSql.checkData(0, 1, 7)

        tdSql.error(
            f"select first(b.col1),count(b.t1),last(a.col1) from sta b right join sta a on a.t1 = b.t1 and a.col1 > 1 and b.t1 is not null where a.ts = b.ts and b.col1 > 3 and a.t1 != 1;"
        )
        tdSql.query(
            f"select first(b.col1),count(b.t1),last(a.col1),count(*) from sta b right join sta a on a.t1 = b.t1 and a.col1 > 1 and b.t1 is not null and a.ts = b.ts and b.col1 > 3 and a.t1 != 1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 7)
        tdSql.checkData(0, 3, 8)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a on (a.t1 = b.t1 or a.col1 = b.col1) or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and (timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts);"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select count(b.ts) from sta b right join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 8)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select count(b.col1) from sta b right join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select count(b.ts) from sta b right join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4)

        tdSql.query(
            f"select a.tbname from sta b right join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select count(b.*) from sta b right join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.error(
            f"select a.tbname from sta b right join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and count(a.col1) = 1;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a on a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select count(b.ts) from sta b right join sta a on a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select count(b.col1) from sta b right join sta a on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is null;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select count(b.col1) from sta b right join sta a on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is null;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or b.col1 > 3);"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select count(b.ts) from sta b right join sta a on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or b.col1 > 3);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 6)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a where (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a where (a.t1 = b.t1 or a.col1 = b.col1) or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a where (a.t1 = b.t1 or a.col1 = b.col1) and (timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts);"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.error(
            f"select a.tbname from sta b right join sta a where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.error(
            f"select a.tbname from sta b right join sta a where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and count(a.col1) = 1;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a where a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is null;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right join sta a where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or b.col1 > 3);"
        )

        # right semi join
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a on timetruncate(a.ts, 1h) = timetruncate(b.ts, 1h) where a.ts = b.ts;"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a on timetruncate(a.ts, 1h) = timetruncate(b.ts, 1h) where a.ts = b.ts or a.ts != b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1h);"
        )
        tdSql.checkRows(0)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a on a.t1 = b.t1 where a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a on a.t1 = b.t1 and a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a on a.t1 = b.t1 or a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.error(
            f"select count(*),last(a.col1) from sta b right semi join sta a on (a.t1 = b.t1 or a.col1 > b.col1) and a.col1 > 1 where a.ts = b.ts and a.col1 > 2;"
        )
        tdSql.query(
            f"select count(*),last(a.col1) from sta b right semi join sta a on (a.t1 = b.t1 or a.col1 > b.col1) and a.col1 > 1 and a.ts = b.ts and a.col1 > 2;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 6)
        tdSql.checkData(0, 1, 7)

        tdSql.error(
            f"select first(b.col1),count(b.t1),last(a.col1) from sta b right semi join sta a on a.t1 = b.t1 and a.col1 > 1 and b.t1 is not null where a.ts = b.ts and b.col1 > 3 and a.t1 != 1;"
        )
        tdSql.query(
            f"select first(b.col1),count(b.t1),last(a.col1),count(*) from sta b right semi join sta a on a.t1 = b.t1 and a.col1 > 1 and b.t1 is not null and a.ts = b.ts and b.col1 > 3 and a.t1 != 1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 7)
        tdSql.checkData(0, 3, 2)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a on (a.t1 = b.t1 or a.col1 = b.col1) or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and (timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts);"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select count(b.ts) from sta b right semi join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 8)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select count(b.col1) from sta b right semi join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select count(b.ts) from sta b right semi join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4)

        tdSql.query(
            f"select a.tbname from sta b right semi join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select count(b.*) from sta b right semi join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.error(
            f"select a.tbname from sta b right semi join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and count(a.col1) = 1;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a on a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select count(b.ts) from sta b right semi join sta a on a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select count(b.col1) from sta b right semi join sta a on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is null;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select count(b.col1) from sta b right semi join sta a on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is null;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or b.col1 > 3);"
        )
        tdSql.checkRows(6)

        tdSql.query(
            f"select count(b.ts) from sta b right semi join sta a on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or b.col1 > 3);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 6)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a where (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a where (a.t1 = b.t1 or a.col1 = b.col1) or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a where (a.t1 = b.t1 or a.col1 = b.col1) and (timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts);"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.error(
            f"select a.tbname from sta b right semi join sta a where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.error(
            f"select a.tbname from sta b right semi join sta a where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and count(a.col1) = 1;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a where a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is null;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right semi join sta a where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or b.col1 > 3);"
        )

        # right anti join
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a on timetruncate(a.ts, 1h) = timetruncate(b.ts, 1h) where a.ts = b.ts;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a on timetruncate(a.ts, 1h) = timetruncate(b.ts, 1h) where a.ts = b.ts or a.ts != b.ts;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1h);"
        )
        tdSql.checkRows(8)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a on a.t1 = b.t1 where a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a on a.t1 = b.t1 and a.ts = b.ts;"
        )
        tdSql.checkRows(0)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a on a.t1 = b.t1 or a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.checkRows(0)

        tdSql.error(
            f"select count(*),last(a.col1) from sta b right anti join sta a on (a.t1 = b.t1 or a.col1 > b.col1) and a.col1 > 1 where a.ts = b.ts and a.col1 > 2;"
        )
        tdSql.query(
            f"select count(*),count(a.col1) from sta b right anti join sta a on (a.t1 = b.t1 or a.col1 > b.col1) and a.col1 > 1 and a.ts = b.ts and a.col1 > 2;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(0, 1, 2)

        tdSql.error(
            f"select first(b.col1),count(b.t1),last(a.col1) from sta b right anti join sta a on a.t1 = b.t1 and a.col1 > 1 and b.t1 is not null where a.ts = b.ts and b.col1 > 3 and a.t1 != 1;"
        )
        tdSql.query(
            f"select first(b.col1),count(b.t1),last(a.col1),count(*) from sta b right anti join sta a on a.t1 = b.t1 and a.col1 > 1 and b.t1 is not null and a.ts = b.ts and b.col1 > 3 and a.t1 != 1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, 5)
        tdSql.checkData(0, 3, 6)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.checkRows(0)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a on (a.t1 = b.t1 or a.col1 = b.col1) or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and (timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts);"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select count(b.ts) from sta b right anti join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.checkRows(6)

        tdSql.query(
            f"select count(b.col1) from sta b right anti join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select count(b.ts) from sta b right anti join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query(
            f"select a.tbname from sta b right anti join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.checkRows(7)

        tdSql.query(
            f"select count(b.*) from sta b right anti join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.error(
            f"select a.tbname from sta b right anti join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and count(a.col1) = 1;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a on a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select count(b.ts) from sta b right anti join sta a on a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select count(b.col1) from sta b right anti join sta a on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is null;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select count(b.col1) from sta b right anti join sta a on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is null;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or b.col1 > 3);"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select count(b.ts) from sta b right anti join sta a on a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or b.col1 > 3);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a where (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a where (a.t1 = b.t1 or a.col1 = b.col1) or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a where (a.t1 = b.t1 or a.col1 = b.col1) and (timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts);"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.error(
            f"select a.tbname from sta b right anti join sta a where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.error(
            f"select a.tbname from sta b right anti join sta a where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and count(a.col1) = 1;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a where a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is null;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right anti join sta a where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or b.col1 > 3);"
        )

        # right asof join
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a on timetruncate(a.ts, 1h) = timetruncate(b.ts, 1h) where a.ts = b.ts;"
        )
        tdSql.checkRows(2)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a on timetruncate(a.ts, 1h) = timetruncate(b.ts, 1h) where a.ts = b.ts or a.ts != b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1h);"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a on a.t1 = b.t1 where a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a on a.t1 = b.t1 and a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a on a.t1 = b.t1 or a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a on a.ts >= b.ts and a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.error(
            f"select count(*),last(a.col1) from sta b right asof join sta a on (a.t1 = b.t1 or a.col1 > b.col1) and a.col1 > 1 where a.ts = b.ts and a.col1 > 2;"
        )
        tdSql.error(
            f"select count(*),count(a.col1) from sta b right asof join sta a on (a.t1 = b.t1 or a.col1 > b.col1) and a.col1 > 1 and a.ts = b.ts and a.col1 > 2;"
        )
        tdSql.error(
            f"select first(b.col1),count(b.t1),last(a.col1) from sta b right asof join sta a on a.t1 = b.t1 and a.col1 > 1 and b.t1 is not null where a.ts = b.ts and b.col1 > 3 and a.t1 != 1;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a on (a.t1 = b.t1 or a.col1 = b.col1) or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and (timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts);"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.error(
            f"select count(b.ts) from sta b right asof join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select count(b.col1) from sta b right asof join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts where b.col1 > 1 or a.col1 > 2;"
        )
        tdSql.checkRows(7)

        tdSql.error(
            f"select a.tbname from sta b right asof join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.query(
            f"select a.tbname from sta b right asof join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts where a.col1 = b.col1 + 1;"
        )
        tdSql.error(
            f"select a.tbname from sta b right asof join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and count(a.col1) = 1;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a on a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a on a.t1 = b.t1 and a.ts = b.ts where a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a on a.t1 = b.t1 and a.ts = b.ts where (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select count(b.col1) from sta b right asof join sta a on a.t1 = b.t1 and a.ts = b.ts where (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a on a.t1 = b.t1 and a.ts = b.ts where (a.col1 < 3 or b.col1 > 3);"
        )
        tdSql.checkRows(6)

        tdSql.query(
            f"select count(b.ts) from sta b right asof join sta a on a.t1 = b.t1 and a.ts = b.ts where (a.col1 < 3 or b.col1 > 3);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 6)

        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a where (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a where (a.t1 = b.t1 or a.col1 = b.col1) or a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a where (a.t1 = b.t1 or a.col1 = b.col1) and (timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts);"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.query(
            f"select a.tbname from sta b right asof join sta a where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.error(
            f"select a.tbname from sta b right asof join sta a where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and count(a.col1) = 1;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a where a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is null;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right asof join sta a where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or b.col1 > 3);"
        )

        # right window join
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a on timetruncate(a.ts, 1h) = timetruncate(b.ts, 1h) window_offset(-1s, 1s) where a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1h) window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) where a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a on a.t1 = b.t1 and a.ts = b.ts window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a on a.t1 = b.t1 or a.col1 = b.col1 window_offset(-1s, 1s) where a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a on a.ts >= b.ts and a.ts = b.ts window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select count(*),last(a.col1) from sta b right window join sta a on (a.t1 = b.t1 or a.col1 > b.col1) and a.col1 > 1 window_offset(-1s, 1s) where a.ts = b.ts and a.col1 > 2;"
        )
        tdSql.error(
            f"select count(*),count(a.col1) from sta b right window join sta a on (a.t1 = b.t1 or a.col1 > b.col1) and a.col1 > 1 and a.ts = b.ts and a.col1 > 2  window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select first(b.col1),count(b.t1),last(a.col1) from sta b right window join sta a on a.t1 = b.t1 and a.col1 > 1 and b.t1 is not null window_offset(-1s, 1s) where a.ts = b.ts and b.col1 > 3 and a.t1 != 1;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a on (a.t1 = b.t1 or a.col1 = b.col1) or a.ts = b.ts  window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and (timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts) window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select count(b.ts) from sta b right window join sta a on (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a on a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select count(b.col1) from sta b right window join sta a on a.t1 = b.t1 and a.col1 = b.col1 window_offset(-1s, 1s);"
        )
        tdSql.checkRows(8)

        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a on a.t1 = b.t1 and b.col1 > 1 window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) where b.col1 > 1 or a.col1 > 2;"
        )
        tdSql.checkRows(13)

        tdSql.error(
            f"select a.tbname from sta b right window join sta a on a.t1 = b.t1 and a.col1 = b.col1 + 1 window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select a.tbname from sta b right window join sta a on a.t1 = b.t1 and count(a.col1) = 1;"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a on a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5 window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a on a.t1 = b.t1 and a.ts = b.ts window_offset(-1s, 1s) where a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) where (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.checkRows(7)

        tdSql.query(
            f"select a.ts from sta b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) where (a.col1 < 3 or a.col1 > 4) and b.col1 is not null having(count(a.ts) > 0);"
        )
        tdSql.query(
            f"select a.ts from sta b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) where a.col1 < 3 or a.col1 > 4 having(count(a.ts) > 1);"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:04")

        tdSql.error(
            f"select a.ts from sta b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) where a.col1 < 3 or a.col1 > 4 having(count(a.ts) > 1) order by b.col1;"
        )
        tdSql.error(
            f"select a.ts from sta b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) where a.col1 < 3 or a.col1 > 4 having(count(a.ts) > 1) order by b.tbname;"
        )
        tdSql.query(
            f"select count(b.col1) from sta b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) where (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.query(
            f"select count(b.col1),a.t1,a.ts from sta b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) where a.col1 < 3 or a.col1 > 4 order by 2,3;"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(4, 0, 1)

        tdSql.error(
            f"select count(b.col1) from sta b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) where a.col1 < 3 or a.col1 > 4 order by b.col1;"
        )
        tdSql.query(
            f"select count(b.col1) c from sta b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) where a.col1 < 3 or a.col1 > 4 order by a.col1, c;"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(4, 0, 1)

        tdSql.query(
            f"select count(b.col1) from sta b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) where a.ts > 0 and b.col1 is not null;"
        )
        tdSql.query(
            f"select count(b.col1) from sta b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) where b.ts > 0;"
        )
        tdSql.query(
            f"select count(b.col1) from sta b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) where b.tbname > 'a';"
        )
        tdSql.query(
            f"select count(b.col1) from sta b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) where b.t1 > 1;"
        )
        tdSql.query(
            f"select count(b.col1) from sta b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) where b.col1 > 1;"
        )
        tdSql.query(
            f"select count(b.col1) from sta b right window join sta a on a.col1 = b.col1 window_offset(-1s, 1s) where b.col1 > 1;"
        )
        tdSql.query(
            f"select count(b.col1) from tba1 b right window join sta a window_offset(-1s, 1s) where b.tbname > 'a';"
        )
        tdSql.query(
            f"select count(b.col1) from tba1 b right window join sta a window_offset(-1s, 1s) where b.t1 > 1;"
        )
        tdSql.query(
            f"select count(b.col1) from tba1 b right window join sta a window_offset(-1s, 1s) where b.col1 > 1;"
        )
        tdSql.query(
            f"select count(b.col1) from tba1 b right window join sta a on a.col1 = b.col1 window_offset(-1s, 1s) where b.col1 > 1;"
        )
        tdSql.query(
            f"select count(b.col1) from sta b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) where a.tbname > 'tba1';"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)

        tdSql.query(
            f"select count(b.col1) from tba2 b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) where b.tbname < 'tba2';"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query(
            f"select count(b.col1) from tba2 b right window join sta a window_offset(-1s, 1s) where b.tbname < 'tba2';"
        )
        tdSql.query(
            f"select count(b.col1) from tba2 b right window join sta a window_offset(-1s, 1s) where b.t1 < 2;"
        )
        tdSql.query(
            f"select count(b.col1) from tba2 b right window join sta a window_offset(-1s, 1s) where b.col1 < 1;"
        )
        tdSql.error(
            f"select count(b.col1) from sta b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) order by b.tbname;"
        )
        tdSql.error(
            f"select count(b.col1) from sta b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) order by b.col1;"
        )
        tdSql.query(
            f"select count(b.col1) from sta b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) order by b.t1;"
        )
        tdSql.error(
            f"select count(b.col1) from sta b right window join sta a window_offset(-1s, 1s) order by b.t1;"
        )
        tdSql.query(
            f"select count(b.col1) from tba1 b right window join sta a window_offset(-1s, 1s) order by b.tbname;"
        )
        tdSql.error(
            f"select count(b.col1) from tba1 b right window join sta a window_offset(-1s, 1s) order by b.col1;"
        )
        tdSql.query(
            f"select count(b.col1) from tba1 b right window join sta a window_offset(-1s, 1s) order by b.t1;"
        )
        tdSql.query(
            f"select count(b.col1) from sta b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) order by a.tbname, a.ts;"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(4, 0, 2)
        tdSql.checkData(5, 0, 2)
        tdSql.checkData(6, 0, 1)
        tdSql.checkData(7, 0, 1)

        tdSql.query(
            f"select count(b.col1) from tba2 b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) order by b.tbname, a.ts;"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(2, 0, 0)
        tdSql.checkData(3, 0, 0)
        tdSql.checkData(4, 0, 2)
        tdSql.checkData(5, 0, 2)
        tdSql.checkData(6, 0, 1)
        tdSql.checkData(7, 0, 1)

        tdSql.error(
            f"select count(b.col1), b.tbname from sta b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select count(b.col1), a.tbname from sta b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select count(b.col1), b.tbname from tba2 b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select first(b.col1) from sta b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) where b.ts > 0;"
        )
        tdSql.query(
            f"select first(a.col1) from sta b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) where b.ts > 0;"
        )
        tdSql.query(
            f"select count(b.col1) from sta b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) where a.ts > 0;"
        )
        tdSql.query(
            f"select count(b.col1) from sta b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) where a.col1 > 0;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a window_offset(-1s, 1s) where (a.t1 = b.t1 or a.col1 = b.col1) and a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a window_offset(-1s, 1s) where (a.t1 = b.t1 or a.col1 = b.col1) or a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a window_offset(-1s, 1s) where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a window_offset(-1s, 1s) where (a.t1 = b.t1 or a.col1 = b.col1) and (timetruncate(a.ts, 1h) = b.ts or a.ts = b.ts);"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a window_offset(-1s, 1s) where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a window_offset(-1s, 1s) where (a.t1 = b.t1 or a.col1 = b.col1) and timetruncate(a.ts, 1m) = b.ts;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a window_offset(-1s, 1s) where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a window_offset(-1s, 1s) where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and b.col1 > 1;"
        )
        tdSql.query(
            f"select a.tbname from sta b right window join sta a window_offset(-1s, 1s) where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and a.col1 = b.col1 + 1;"
        )
        tdSql.error(
            f"select a.tbname from sta b right window join sta a window_offset(-1s, 1s) where a.t1 = b.t1 and timetruncate(a.ts, 1m) = b.ts and count(a.col1) = 1;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a window_offset(-1s, 1s) where a.t1 = b.t1 and a.ts = b.ts and a.col1 > 2 and b.col1 < 5;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a window_offset(-1s, 1s) where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is not null;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a window_offset(-1s, 1s) where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or a.col1 > 4) and b.col1 is null;"
        )
        tdSql.query(
            f"select a.ts, a.col1, b.ts, b.col1 from sta b right window join sta a window_offset(-1s, 1s) where a.t1 = b.t1 and a.ts = b.ts and (a.col1 < 3 or b.col1 > 3);"
        )
        tdSql.error(
            f"select distinct count(b.col1) from tba2 b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) order by b.tbname, a.ts;"
        )
        tdSql.query(
            f"select distinct count(b.col1) c from tba2 b right window join sta a on a.t1 = b.t1 window_offset(-1s, 1s) order by c;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 2)

    def join_timeline(self):
        tdSql.execute(f"use test0;")

        # inner join + join group
        tdSql.query(
            f"select sum(a.col1) c1 from sta a join sta b on a.ts = b.ts and a.t1 = b.t1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 30)

        tdSql.error(
            f"select diff(a.col1) c1 from sta a join sta b on a.ts = b.ts and a.t1 = b.t1;"
        )
        tdSql.error(
            f"select csum(b.col1) from sta a join sta b on a.ts = b.ts and a.t1 = b.t1;"
        )
        tdSql.query(
            f"select tail(b.col1, 1) from sta a join sta b on a.ts = b.ts and a.t1 = b.t1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 7)

        tdSql.query(
            f"select tail(b.col1, 1) from sta a join sta b on a.ts = b.ts and a.t1 = b.t1 and a.t1 > 0;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 7)

        tdSql.query(
            f"select count(a.col1) c1 from sta a join sta b on a.ts = b.ts and a.t1 = b.t1 interval(1s);"
        )
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(5, 0, 1)

        tdSql.error(
            f"select count(a.col1) c1 from sta a join sta b on a.ts = b.ts and a.t1 = b.t1 session(a.ts, 1s);"
        )
        tdSql.error(
            f"select count(a.col1) c1 from sta a join sta b on a.ts = b.ts and a.t1 = b.t1 session(b.ts, 1s);"
        )
        tdSql.error(
            f"select count(a.col1) c1 from sta a join sta b on a.ts = b.ts and a.t1 = b.t1 state_window(b.col1);"
        )
        tdSql.error(
            f"select csum(b.col1) from sta a join sta b on a.ts = b.ts and a.t1 = b.t1 interval(1s);"
        )

        # inner join + no join group
        tdSql.query(f"select sum(a.col1) c1 from sta a join sta b on a.ts = b.ts;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 42)

        tdSql.query(f"select diff(a.col1) c1 from tba1 a join tba2 b on a.ts = b.ts;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.error(f"select csum(b.col1) from sta a join sta b on a.ts = b.ts;")
        tdSql.query(f"select csum(b.col1) from tba1 a join tba2 b on a.ts = b.ts;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 7)

        tdSql.query(
            f"select count(a.col1) c1 from sta a join sta b on a.ts = b.ts interval(1s);"
        )
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 4)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(5, 0, 1)

        tdSql.query(
            f"select count(a.col1) c1 from sta a join sta b on a.ts = b.ts session(a.ts, 1s);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 12)

        tdSql.query(
            f"select count(a.col1) c1 from sta a join sta b on a.ts = b.ts session(b.ts, 1s);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 12)

        tdSql.error(
            f"select count(a.col1) c1 from sta a join sta b on a.ts = b.ts state_window(b.col1);"
        )

        # left join
        tdSql.query(f"select sum(a.col1) c1 from sta a left join sta b on a.ts = b.ts;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 42)

        tdSql.query(
            f"select diff(a.col1) c1 from tba1 a left join tba2 b on a.ts = b.ts;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)

        tdSql.query(
            f"select diff(b.col1) c1 from tba1 a left join tba2 b on a.ts = b.ts;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, None)

        tdSql.error(f"select csum(b.col1) from sta a left join sta b on a.ts = b.ts;")
        tdSql.query(f"select csum(b.col1) from tba1 a left join tba2 b on a.ts = b.ts;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 7)

        tdSql.query(
            f"select count(a.col1) c1 from sta a left join sta b on a.ts = b.ts interval(1s);"
        )
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 4)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(5, 0, 1)

        tdSql.query(
            f"select count(a.col1) c1 from sta a left join sta b on a.ts = b.ts session(a.ts, 1s);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 12)

        tdSql.error(
            f"select count(a.col1) c1 from sta a left join sta b on a.ts = b.ts session(b.ts, 1s);"
        )

        tdSql.error(
            f"select count(a.col1) c1 from sta a left join sta b on a.ts = b.ts state_window(b.col1);"
        )

        # left semi join
        tdSql.query(
            f"select sum(a.col1) c1 from sta a left semi join sta b on a.ts = b.ts;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 30)

        tdSql.query(
            f"select diff(a.col1) c1 from tba1 a left semi join tba2 b on a.ts = b.ts;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.query(
            f"select diff(b.col1) c1 from tba1 a left semi join tba2 b on a.ts = b.ts;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.error(
            f"select csum(b.col1) from sta a left semi join sta b on a.ts = b.ts;"
        )
        tdSql.query(
            f"select csum(b.col1) from tba1 a left semi join tba2 b on a.ts = b.ts;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 7)

        tdSql.query(
            f"select count(a.col1) c1 from sta a left semi join sta b on a.ts = b.ts interval(1s);"
        )
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(5, 0, 1)

        tdSql.query(
            f"select count(a.col1) c1 from sta a left semi join sta b on a.ts = b.ts session(a.ts, 1s);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 8)

        tdSql.query(
            f"select count(a.col1) c1 from sta a left semi join sta b on a.ts = b.ts session(b.ts, 1s);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 8)

        tdSql.error(
            f"select count(a.col1) c1 from sta a left semi join sta b on a.ts = b.ts state_window(b.col1);"
        )

        # left anti join
        tdSql.query(
            f"select sum(a.col1) c1 from sta a left anti join sta b on a.ts = b.ts;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select diff(a.col1) c1 from tba1 a left anti join tba2 b on a.ts = b.ts;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        # ???????
        tdSql.query(
            f"select diff(b.col1) c1 from tba1 a left anti join tba2 b on a.ts = b.ts;"
        )

        tdSql.query(
            f"select csum(b.col1) from sta a left anti join sta b on a.ts = b.ts;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select csum(b.col1) from tba1 a left anti join tba2 b on a.ts = b.ts;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select count(a.col1) c1 from sta a left anti join sta b on a.ts = b.ts interval(1s);"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select count(a.col1) c1 from tba1 a left anti join tba2 b on a.ts = b.ts interval(1s);"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)

        tdSql.query(
            f"select count(a.col1) c1 from sta a left anti join sta b on a.ts = b.ts session(a.ts, 1s);"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select count(a.col1) c1 from tba1 a left anti join tba2 b on a.ts = b.ts session(a.ts, 1s);"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)

        tdSql.error(
            f"select count(a.col1) c1 from sta a left anti join sta b on a.ts = b.ts session(b.ts, 1s);"
        )

        tdSql.error(
            f"select count(a.col1) c1 from sta a left semi join sta b on a.ts = b.ts state_window(b.col1);"
        )

        # left asof join + join group
        tdSql.query(
            f"select sum(a.col1) c1 from sta a left asof join sta b on a.ts > b.ts and a.t1 = b.t1;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 30)

        tdSql.error(
            f"select diff(a.col1) c1 from sta a left asof join sta b on a.ts > b.ts and a.t1 = b.t1;"
        )
        tdSql.error(
            f"select diff(b.col1) c1 from sta a left asof join sta b on a.ts > b.ts and a.t1 = b.t1;"
        )

        tdSql.error(
            f"select csum(a.col1) from sta a left asof join sta b on a.ts > b.ts and a.t1 = b.t1;"
        )
        tdSql.error(
            f"select csum(b.col1) from sta a left asof join sta b on a.ts > b.ts and a.t1 = b.t1;"
        )

        tdSql.query(
            f"select count(a.col1) c1 from sta a left asof join sta b on a.ts > b.ts and a.t1 = b.t1 interval(1s);"
        )
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 2)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(5, 0, 1)

        tdSql.error(
            f"select count(a.col1) c1 from sta a left asof join sta b on a.ts > b.ts and a.t1 = b.t1 session(a.ts, 1s);"
        )
        tdSql.error(
            f"select count(a.col1) c1 from sta a left asof join sta b on a.ts > b.ts and a.t1 = b.t1 session(b.ts, 1s);"
        )

        tdSql.error(
            f"select count(a.col1) c1 from sta a left asof join sta b on a.ts > b.ts and a.t1 = b.t1 state_window(b.col1);"
        )

        # left asof join + no join group
        tdSql.query(
            f"select sum(a.col1) c1 from sta a left asof join sta b on a.ts > b.ts;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 30)

        tdSql.error(
            f"select diff(a.col1) c1 from sta a left asof join sta b on a.ts > b.ts;"
        )
        tdSql.query(
            f"select diff(a.col1) c1 from tba1 a left asof join tba2 b on a.ts > b.ts;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)

        tdSql.query(
            f"select diff(b.col1) c1 from tba1 a left asof join tba2 b on a.ts > b.ts;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(2, 0, 2)

        tdSql.query(
            f"select csum(a.col1) from tba1 a left asof join tba2 b on a.ts > b.ts;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 4)
        tdSql.checkData(2, 0, 8)
        tdSql.checkData(3, 0, 13)

        tdSql.query(
            f"select csum(b.col1) from tba1 a left asof join tba2 b on a.ts > b.ts;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 6)
        tdSql.checkData(2, 0, 11)

        tdSql.query(
            f"select count(a.col1) c1 from tba1 a left asof join tba2 b on a.ts > b.ts interval(1s);"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)

        tdSql.query(
            f"select count(a.col1) c1 from tba1 a left asof join tba2 b on a.ts > b.ts session(a.ts, 1s);"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 3)

        tdSql.error(
            f"select count(a.col1) c1 from tba1 a left asof join tba2 b on a.ts > b.ts session(b.ts, 1s);"
        )

        tdSql.query(
            f"select count(a.col1) c1 from tba1 a left asof join tba2 b on a.ts > b.ts state_window(b.col1);"
        )

        # left win join + join group
        tdSql.query(
            f"select sum(a.col1) c1 from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s);"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(1, 0, 6)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(3, 0, 7)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(5, 0, 6)
        tdSql.checkData(6, 0, 12)
        tdSql.checkData(7, 0, 10)

        tdSql.error(
            f"select diff(a.col1) c1 from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select csum(a.col1) from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s);"
        )

        tdSql.error(
            f"select count(a.col1) c1 from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) interval(1s);"
        )
        tdSql.error(
            f"select count(a.col1) c1 from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) session(a.ts, 1s);"
        )
        tdSql.error(
            f"select count(a.col1) c1 from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) session(b.ts, 1s);"
        )
        tdSql.error(
            f"select count(a.col1) c1 from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s, 1s) state_window(b.col1);"
        )

        # left win join + no join group
        tdSql.query(
            f"select sum(a.col1) c1 from sta a left window join sta b window_offset(-1s, 1s) order by c1;"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 6)
        tdSql.checkData(2, 0, 12)
        tdSql.checkData(3, 0, 12)
        tdSql.checkData(4, 0, 14)
        tdSql.checkData(5, 0, 16)
        tdSql.checkData(6, 0, 20)
        tdSql.checkData(7, 0, 20)

        tdSql.error(
            f"select diff(a.col1) c1 from sta a left window join sta b window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select diff(a.col1) c1 from tba1 a left window join tba2 b window_offset(-1s, 0s);"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select diff(b.col1) c1 from tba1 a left window join tba2 b window_offset(-1s, 0s);"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select diff(b.col1) c1 from sta a left window join tba1 b window_offset(-1s, 1s);"
        )
        tdSql.checkRows(7)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(5, 0, 1)
        tdSql.checkData(6, 0, 1)

        tdSql.error(
            f"select csum(a.col1) from sta a left window join sta b window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select csum(a.col1) from tba1 a left window join tba2 b window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select csum(a.col1) from tba1 a left window join tba2 b window_offset(-1s, 0s);"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 4)
        tdSql.checkData(3, 0, 5)

        tdSql.query(
            f"select csum(b.col1) from tba1 a left window join tba2 b window_offset(-1s, 1s);"
        )
        tdSql.checkRows(7)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 8)
        tdSql.checkData(4, 0, 5)
        tdSql.checkData(5, 0, 5)
        tdSql.checkData(6, 0, 12)

        tdSql.error(
            f"select count(a.col1) c1 from sta a left window join sta b window_offset(-1s, 1s) interval(1s);"
        )
        tdSql.error(
            f"select count(a.col1) c1 from sta a left window join sta b window_offset(-1s, 1s) session(a.ts, 1s);"
        )
        tdSql.error(
            f"select count(a.col1) c1 from sta a left window join sta b window_offset(-1s, 1s) session(b.ts, 1s);"
        )
        tdSql.error(
            f"select count(a.col1) c1 from sta a left window join sta b window_offset(-1s, 1s) state_window(b.col1);"
        )

        # full join
        tdSql.query(f"select sum(a.col1) c1 from sta a full join sta b on a.ts = b.ts;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 42)

        tdSql.error(
            f"select diff(a.col1) c1 from tba1 a full join tba2 b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select diff(b.col1) c1 from tba1 a full join tba2 b on a.ts = b.ts;"
        )
        tdSql.error(f"select csum(b.col1) from sta a full join sta b on a.ts = b.ts;")
        tdSql.error(f"select csum(b.col1) from tba1 a full join tba2 b on a.ts = b.ts;")
        tdSql.error(
            f"select count(a.col1) c1 from sta a full join sta b on a.ts = b.ts interval(1s);"
        )
        tdSql.error(
            f"select count(a.col1) c1 from sta a full join sta b on a.ts = b.ts session(a.ts, 1s);"
        )
        tdSql.error(
            f"select count(a.col1) c1 from sta a full join sta b on a.ts = b.ts session(b.ts, 1s);"
        )
        tdSql.error(
            f"select count(a.col1) c1 from sta a full join sta b on a.ts = b.ts state_window(b.col1);"
        )

    def join_nested(self):
        tdSql.execute(f"use test0;")

        tdSql.query(
            f"select a.ts from sta a ,(select TIMETRUNCATE(ts,1d) ts from sta) b where timetruncate(a.ts, 1d) = b.ts;"
        )
        tdSql.checkRows(64)

        # inner join + non join
        tdSql.error(
            f"select tail(col1, 1) from (select b.col1 from sta a join sta b on a.ts = b.ts and a.t1 = b.t1 and a.t1 > 0) c;"
        )
        tdSql.error(
            f"select tail(col1, 1) from (select b.col1 from sta a join sta b on a.ts = b.ts and a.t1 = b.t1 and a.t1 > 0 order by b.ts) c;"
        )
        tdSql.query(
            f"select tail(col1, 1) from (select b.col1, a.ts from sta a join sta b on a.ts = b.ts and a.t1 = b.t1 and a.t1 > 0 order by b.ts) c;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 7)

        tdSql.error(
            f"select a.*,b.* from (select * from sta partition by tbname) a join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.t1 from (select * from sta partition by tbname order by col1) a join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.t1 from (select * from sta partition by tbname order by ts) a join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.checkRows(12)

        tdSql.query(
            f"select a.t1 from (select * from sta partition by tbname order by ts desc) a join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.checkRows(12)

        tdSql.query(
            f"select a.t1 from (select * from sta partition by tbname order by ts desc) a join (select * from sta partition by tbname order by ts) b on a.ts = b.ts order by b.ts desc;"
        )
        tdSql.checkRows(12)

        tdSql.query(
            f"select a.ts from (select last(tba1.ts) as ts from tba1, tba2 where tba1.ts = tba2.ts) as a join (select ts from tba2) as tba2 on a.ts = tba2.ts;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2023-11-17 16:29:03")

        # left join + non join
        tdSql.error(
            f"select a.*,b.* from (select * from sta partition by tbname) a left join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.t1 from (select * from sta partition by tbname order by col1) a left join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.t1 from (select * from sta partition by tbname order by ts) a left join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.checkRows(12)

        tdSql.query(
            f"select a.t1 from (select * from sta partition by tbname order by ts desc) a left join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.checkRows(12)

        tdSql.query(
            f"select a.t1 from (select * from sta partition by tbname order by ts desc) a left join (select * from sta partition by tbname order by ts) b on a.ts = b.ts order by b.ts desc;"
        )
        tdSql.checkRows(12)

        # left semi join + non join
        tdSql.error(
            f"select a.*,b.* from (select * from sta partition by tbname) a left semi join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.t1 from (select * from sta partition by tbname order by col1) a left semi join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.t1 from (select * from sta partition by tbname order by ts) a left semi join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select a.t1 from (select * from sta partition by tbname order by ts desc) a left semi join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select a.t1 from (select * from sta partition by tbname order by ts desc) a left semi join (select * from sta partition by tbname order by ts) b on a.ts = b.ts order by b.ts desc;"
        )
        tdSql.checkRows(8)

        # left anti join + non join
        tdSql.error(
            f"select a.*,b.* from (select * from sta partition by tbname) a left anti join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.t1 from (select * from sta partition by tbname order by col1) a left anti join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.t1 from (select * from sta partition by tbname order by ts) a left anti join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select a.t1 from (select * from sta partition by tbname order by ts desc) a left anti join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select a.t1 from (select * from sta partition by tbname order by ts desc) a left anti join (select * from sta partition by tbname order by ts) b on a.ts = b.ts order by b.ts desc;"
        )
        tdSql.checkRows(0)

        # left asof join + non join
        tdSql.error(
            f"select a.*,b.* from (select * from sta partition by tbname) a left asof join (select * from sta partition by tbname order by ts) b;"
        )
        tdSql.error(
            f"select a.t1 from (select * from sta partition by tbname order by col1) a left asof join (select * from sta partition by tbname order by ts) b;"
        )
        tdSql.error(
            f"select a.t1 from (select * from sta partition by tbname order by ts) a left asof join (select * from sta partition by tbname order by ts) b;"
        )
        tdSql.error(
            f"select a.t1 from (select * from sta partition by tbname order by ts desc) a left asof join (select * from sta partition by tbname order by ts) b;"
        )
        tdSql.error(
            f"select a.t1 from (select * from sta partition by tbname order by ts desc) a left asof join (select * from sta partition by tbname order by ts) b order by b.ts desc;"
        )

        # left window join + non join
        tdSql.error(
            f"select a.*,b.* from (select * from sta partition by tbname) a left window join (select * from sta partition by tbname order by ts) b window_offset(-1s,1s);"
        )
        tdSql.error(
            f"select a.t1 from (select * from sta partition by tbname order by col1) a left window join (select * from sta partition by tbname order by ts) b window_offset(-1s,1s);"
        )
        tdSql.error(
            f"select a.t1 from (select * from sta partition by tbname order by ts) a left window join (select * from sta partition by tbname order by ts) b window_offset(-1s,1s);"
        )
        tdSql.error(
            f"select a.t1 from (select * from sta partition by tbname order by ts desc) a left window join (select * from sta partition by tbname order by ts) b window_offset(-1s,1s);"
        )
        tdSql.error(
            f"select a.t1 from (select * from sta partition by tbname order by ts desc) a left window join (select * from sta partition by tbname order by ts) b window_offset(-1s,1s) order by b.ts desc;"
        )

        # inner join + inner join
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts and tb1.t1=tb2.t1) a join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts from (select tba1.ts from tba1 join tba2 on tba1.ts=tba2.ts) a join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2023-11-17 16:29:00")
        tdSql.checkData(1, 0, "2023-11-17 16:29:00")
        tdSql.checkData(2, 0, "2023-11-17 16:29:03")
        tdSql.checkData(3, 0, "2023-11-17 16:29:03")

        tdSql.query(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts) a join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.checkRows(20)

        tdSql.query(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts) a join (select * from sta partition by tbname order by ts desc) b on a.ts = b.ts;"
        )
        tdSql.checkRows(20)

        tdSql.query(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts order by tb2.ts desc) a join (select * from sta partition by tbname order by ts desc) b  on a.ts = b.ts;"
        )
        tdSql.checkRows(20)

        tdSql.query(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts order by tb2.ts desc) a join (select * from sta partition by tbname order by ts) b  on a.ts = b.ts;"
        )
        tdSql.checkRows(20)

        tdSql.query(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts order by tb2.ts desc) a join (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts order by tb2.ts desc) b on a.ts = b.ts;"
        )
        tdSql.checkRows(36)

        tdSql.query(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts order by tb2.ts desc) a join (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts order by tb2.ts) b on a.ts = b.ts;"
        )
        tdSql.checkRows(36)

        tdSql.query(
            f"select count(a.col1) from (select tb1.ts,tb1.col1 from sta tb1 join sta tb2 on tb1.ts=tb2.ts order by tb2.ts desc) a join (select * from sta partition by tbname order by ts desc) b on a.ts = b.ts session(a.ts, 1s);;"
        )

        # inner join + left join
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left join sta tb2 on tb1.ts=tb2.ts order by tb2.ts) a join sta b on a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts from (select tb1.ts from sta tb1 left join sta tb2 on tb1.ts=tb2.ts) a join sta b on a.ts = b.ts;"
        )
        tdSql.checkRows(20)

        tdSql.query(
            f"select a.ts from (select tb1.ts from sta tb1 left join sta tb2 on tb1.ts=tb2.ts) a join (select tb1.ts from sta tb1 left join sta tb2 on tb1.ts=tb2.ts order by tb1.ts desc) b on a.ts = b.ts;"
        )
        tdSql.checkRows(36)

        tdSql.error(
            f"select count(a.col1) from (select tb1.ts,tb1.col1 from sta tb1 left join sta tb2 on tb1.ts=tb2.ts order by tb2.ts desc) a join (select * from sta partition by tbname order by ts desc) b on a.ts = b.ts session(a.ts, 1s);;"
        )
        tdSql.query(
            f"select count(a.col1) from (select tb1.ts,tb1.col1 from sta tb1 left join sta tb2 on tb1.ts=tb2.ts) a join (select * from sta partition by tbname order by ts desc) b on a.ts = b.ts session(a.ts, 1s);;"
        )

        # inner join + left semi join
        tdSql.query(
            f"select a.ts from (select tb1.ts from sta tb1 left semi join sta tb2 on tb1.ts=tb2.ts order by tb2.ts) a join sta b on a.ts = b.ts;"
        )
        tdSql.checkRows(12)

        tdSql.query(
            f"select a.ts from (select tb1.ts from sta tb1 left semi join sta tb2 on tb1.ts=tb2.ts) a join sta b on a.ts = b.ts;"
        )
        tdSql.checkRows(12)

        tdSql.query(
            f"select a.ts from (select tb1.ts from sta tb1 left semi join sta tb2 on tb1.ts=tb2.ts) a join (select tb1.ts from sta tb1 left semi join sta tb2 on tb1.ts=tb2.ts order by tb1.ts desc) b on a.ts = b.ts;"
        )
        tdSql.checkRows(12)

        tdSql.query(
            f"select count(a.col1) from (select tb1.ts,tb1.col1 from sta tb1 left semi join sta tb2 on tb1.ts=tb2.ts order by tb2.ts desc) a join (select * from sta partition by tbname order by ts desc) b on a.ts = b.ts session(a.ts, 1s);;"
        )
        tdSql.query(
            f"select count(a.col1) from (select tb1.ts,tb1.col1 from sta tb1 left semi join sta tb2 on tb1.ts=tb2.ts) a join (select * from sta partition by tbname order by ts desc) b on a.ts = b.ts session(a.ts, 1s);;"
        )

        # inner join + left anti join
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left anti join sta tb2 on tb1.ts=tb2.ts order by tb2.ts) a join sta b on a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts from (select tb1.ts from sta tb1 left anti join sta tb2 on tb1.ts=tb2.ts) a join sta b on a.ts = b.ts;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select a.ts from (select tb1.ts from sta tb1 left anti join sta tb2 on tb1.ts=tb2.ts) a join (select tb1.ts from sta tb1 left anti join sta tb2 on tb1.ts=tb2.ts order by tb1.ts desc) b on a.ts = b.ts;"
        )
        tdSql.checkRows(0)

        tdSql.error(
            f"select count(a.col1) from (select tb1.ts,tb1.col1 from sta tb1 left anti join sta tb2 on tb1.ts=tb2.ts order by tb2.ts desc) a join (select * from sta partition by tbname order by ts desc) b on a.ts = b.ts session(a.ts, 1s);;"
        )
        tdSql.query(
            f"select count(a.col1) from (select tb1.ts,tb1.col1 from sta tb1 left anti join sta tb2 on tb1.ts=tb2.ts) a join (select * from sta partition by tbname order by ts desc) b on a.ts = b.ts session(a.ts, 1s);;"
        )

        # inner join + left asof join
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left asof join sta tb2 order by tb2.ts) a join sta b on a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts from (select tb1.ts from sta tb1 left asof join sta tb2) a join sta b on a.ts = b.ts;"
        )
        tdSql.checkRows(12)

        tdSql.query(
            f"select a.ts from (select tb1.ts from sta tb1 left asof join sta tb2) a join (select tb1.ts from sta tb1 left asof join sta tb2 order by tb1.ts desc) b on a.ts = b.ts;"
        )
        tdSql.checkRows(12)

        tdSql.error(
            f"select count(a.col1) from (select tb1.ts,tb1.col1 from sta tb1 left asof join sta tb2 order by tb2.ts desc) a join (select * from sta partition by tbname order by ts desc) b on a.ts = b.ts session(a.ts, 1s);;"
        )
        tdSql.query(
            f"select count(a.col1) from (select tb1.ts,tb1.col1 from sta tb1 left asof join sta tb2) a join (select * from sta partition by tbname order by ts desc) b on a.ts = b.ts session(a.ts, 1s);;"
        )

        # inner join + left window join
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left window join sta tb2 window_offset(-1s,1s) order by tb2.ts) a join sta b on a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts from (select tb1.ts from sta tb1 left window join sta tb2 window_offset(-1s,1s)) a join sta b on a.ts = b.ts;"
        )
        tdSql.checkRows(42)

        tdSql.query(
            f"select a.ts from (select tb1.ts from sta tb1 left window join sta tb2 window_offset(-1s,1s)) a join (select tb1.ts from sta tb1 left window join sta tb2 window_offset(-1s,1s) order by tb1.ts desc) b on a.ts = b.ts;"
        )
        tdSql.checkRows(152)

        tdSql.error(
            f"select count(a.col1) from (select tb1.ts,tb1.col1 from sta tb1 left window join sta tb2 window_offset(-1s,1s) order by tb2.ts desc) a join (select * from sta partition by tbname order by ts desc) b on a.ts = b.ts session(a.ts, 1s);;"
        )
        tdSql.query(
            f"select count(a.col1) from (select tb1.ts,tb1.col1 from sta tb1 left window join sta tb2 window_offset(-1s,1s)) a join (select * from sta partition by tbname order by ts desc) b on a.ts = b.ts session(a.ts, 1s);;"
        )

        # left join + inner join
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts and tb1.t1=tb2.t1) a left join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tba1.ts from tba1 join tba2 on tba1.ts=tba2.ts) a left join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts) a left join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts) a left join (select * from sta partition by tbname order by ts desc) b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts order by tb2.ts desc) a left join (select * from sta partition by tbname order by ts desc) b  on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts order by tb2.ts desc) a left join (select * from sta partition by tbname order by ts) b  on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts order by tb2.ts desc) a left join (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts order by tb2.ts desc) b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts order by tb2.ts desc) a left join (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts order by tb2.ts) b on a.ts = b.ts;"
        )

        # left join + left join
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left join sta tb2 on tb1.ts=tb2.ts order by tb2.ts) a left join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left join sta tb2 on tb1.ts=tb2.ts) a left join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left join sta tb2 on tb1.ts=tb2.ts) a left join (select tb1.ts from sta tb1 left join sta tb2 on tb1.ts=tb2.ts order by tb1.ts desc) b on a.ts = b.ts;"
        )

        # left join + left semi join
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left semi join sta tb2 on tb1.ts=tb2.ts order by tb2.ts) a left join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left semi join sta tb2 on tb1.ts=tb2.ts) a left join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left semi join sta tb2 on tb1.ts=tb2.ts) a left join (select tb1.ts from sta tb1 left semi join sta tb2 on tb1.ts=tb2.ts order by tb1.ts desc) b on a.ts = b.ts;"
        )

        # left join + left anti join
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left anti join sta tb2 on tb1.ts=tb2.ts order by tb2.ts) a left join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left anti join sta tb2 on tb1.ts=tb2.ts) a left join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left anti join sta tb2 on tb1.ts=tb2.ts) a left join (select tb1.ts from sta tb1 left anti join sta tb2 on tb1.ts=tb2.ts order by tb1.ts desc) b on a.ts = b.ts;"
        )

        # left join + left asof join
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left asof join sta tb2 order by tb2.ts) a left join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left asof join sta tb2) a left join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left asof join sta tb2) a left join (select tb1.ts from sta tb1 left asof join sta tb2 order by tb1.ts desc) b on a.ts = b.ts;"
        )

        # left join + left window join
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left window join sta tb2 window_offset(-1s,1s) order by tb2.ts) a left join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left window join sta tb2 window_offset(-1s,1s)) a left join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left window join sta tb2 window_offset(-1s,1s)) a left join (select tb1.ts from sta tb1 left window join sta tb2 window_offset(-1s,1s) order by tb1.ts desc) b on a.ts = b.ts;"
        )

        # left semi join + inner join
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts and tb1.t1=tb2.t1) a left semi join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tba1.ts from tba1 join tba2 on tba1.ts=tba2.ts) a left semi join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts) a left semi join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts) a left semi join (select * from sta partition by tbname order by ts desc) b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts order by tb2.ts desc) a left semi join (select * from sta partition by tbname order by ts desc) b  on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts order by tb2.ts desc) a left semi join (select * from sta partition by tbname order by ts) b  on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts order by tb2.ts desc) a left semi join (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts order by tb2.ts desc) b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts order by tb2.ts desc) a left semi join (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts order by tb2.ts) b on a.ts = b.ts;"
        )

        # left semi join + left join
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left join sta tb2 on tb1.ts=tb2.ts order by tb2.ts) a left semi join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left join sta tb2 on tb1.ts=tb2.ts) a left semi join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left join sta tb2 on tb1.ts=tb2.ts) a left semi join (select tb1.ts from sta tb1 left join sta tb2 on tb1.ts=tb2.ts order by tb1.ts desc) b on a.ts = b.ts;"
        )

        # left semi join + left semi join
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left semi join sta tb2 on tb1.ts=tb2.ts order by tb2.ts) a left semi join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left semi join sta tb2 on tb1.ts=tb2.ts) a left semi join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left semi join sta tb2 on tb1.ts=tb2.ts) a left semi join (select tb1.ts from sta tb1 left semi join sta tb2 on tb1.ts=tb2.ts order by tb1.ts desc) b on a.ts = b.ts;"
        )

        # left semi join + left anti join
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left anti join sta tb2 on tb1.ts=tb2.ts order by tb2.ts) a left semi join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left anti join sta tb2 on tb1.ts=tb2.ts) a left semi join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left anti join sta tb2 on tb1.ts=tb2.ts) a left semi join (select tb1.ts from sta tb1 left anti join sta tb2 on tb1.ts=tb2.ts order by tb1.ts desc) b on a.ts = b.ts;"
        )

        # left semi join + left asof join
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left asof join sta tb2 order by tb2.ts) a left semi join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left asof join sta tb2) a left semi join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left asof join sta tb2) a left semi join (select tb1.ts from sta tb1 left asof join sta tb2 order by tb1.ts desc) b on a.ts = b.ts;"
        )

        # left semi join + left window join
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left window join sta tb2 window_offset(-1s,1s) order by tb2.ts) a left semi join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left window join sta tb2 window_offset(-1s,1s)) a left semi join sta b on a.ts = b.ts;"
        )

        # left anti join + inner join
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts and tb1.t1=tb2.t1) a left anti join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tba1.ts from tba1 join tba2 on tba1.ts=tba2.ts) a left anti join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts) a left anti join (select * from sta partition by tbname order by ts) b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts) a left anti join (select * from sta partition by tbname order by ts desc) b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts order by tb2.ts desc) a left anti join (select * from sta partition by tbname order by ts desc) b  on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts order by tb2.ts desc) a left anti join (select * from sta partition by tbname order by ts) b  on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts order by tb2.ts desc) a left anti join (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts order by tb2.ts desc) b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts order by tb2.ts desc) a left anti join (select tb1.ts from sta tb1 join sta tb2 on tb1.ts=tb2.ts order by tb2.ts) b on a.ts = b.ts;"
        )

        # left anti join + left join
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left join sta tb2 on tb1.ts=tb2.ts order by tb2.ts) a left anti join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left join sta tb2 on tb1.ts=tb2.ts) a left anti join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left join sta tb2 on tb1.ts=tb2.ts) a left anti join (select tb1.ts from sta tb1 left join sta tb2 on tb1.ts=tb2.ts order by tb1.ts desc) b on a.ts = b.ts;"
        )

        # left anti join + left semi join
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left semi join sta tb2 on tb1.ts=tb2.ts order by tb2.ts) a left anti join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left semi join sta tb2 on tb1.ts=tb2.ts) a left anti join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left semi join sta tb2 on tb1.ts=tb2.ts) a left anti join (select tb1.ts from sta tb1 left semi join sta tb2 on tb1.ts=tb2.ts order by tb1.ts desc) b on a.ts = b.ts;"
        )

        # left anti join + left anti join
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left anti join sta tb2 on tb1.ts=tb2.ts order by tb2.ts) a left anti join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left anti join sta tb2 on tb1.ts=tb2.ts) a left anti join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left anti join sta tb2 on tb1.ts=tb2.ts) a left anti join (select tb1.ts from sta tb1 left anti join sta tb2 on tb1.ts=tb2.ts order by tb1.ts desc) b on a.ts = b.ts;"
        )

        # left anti join + left asof join
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left asof join sta tb2 order by tb2.ts) a left anti join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left asof join sta tb2) a left anti join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left asof join sta tb2) a left anti join (select tb1.ts from sta tb1 left asof join sta tb2 order by tb1.ts desc) b on a.ts = b.ts;"
        )

        # left anti join + left window join
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left window join sta tb2 window_offset(-1s,1s) order by tb2.ts) a left anti join sta b on a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left window join sta tb2 window_offset(-1s,1s)) a left anti join sta b on a.ts = b.ts;"
        )

        # left asof join + left join
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left join sta tb2 on tb1.ts=tb2.ts order by tb2.ts) a left asof join sta b;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left join sta tb2 on tb1.ts=tb2.ts) a left asof join sta b;"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left join sta tb2 on tb1.ts=tb2.ts) a left asof join (select tb1.ts from sta tb1 left join sta tb2 on tb1.ts=tb2.ts order by tb1.ts desc) b;"
        )

        # left window join + left join
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left join sta tb2 on tb1.ts=tb2.ts order by tb2.ts) a left window join sta b window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left join sta tb2 on tb1.ts=tb2.ts) a left window join sta b window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select a.ts from (select tb1.ts from sta tb1 left join sta tb2 on tb1.ts=tb2.ts) a left window join (select tb1.ts from sta tb1 left join sta tb2 on tb1.ts=tb2.ts order by tb1.ts desc) b window_offset(-1s, 1s);"
        )

        # multi level join
        tdSql.query(
            f"select a.ts from sta a join sta b on a.ts = b.ts join sta c on b.ts = c.ts;"
        )
        tdSql.checkRows(20)

        tdSql.error(
            f"select a.ts from sta a join sta b on a.ts = b.ts left join sta c on b.ts = c.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a join sta b on a.ts = b.ts left semi join sta c on b.ts = c.ts;"
        )
        tdSql.query(
            f"select a.ts from sta a left join sta b on a.ts = b.ts join sta c on b.ts = c.ts;"
        )
        tdSql.query(
            f"select a.ts from sta a left semi join sta b on a.ts = b.ts join sta c on b.ts = c.ts;"
        )
        tdSql.query(
            f"select a.ts from sta a left anti join sta b on a.ts = b.ts join sta c on b.ts = c.ts;"
        )
        tdSql.query(
            f"select a.ts from (sta a left asof join sta b) join sta c on b.ts = c.ts;"
        )
        tdSql.query(
            f"select a.ts from sta a left window join sta b window_offset(-1s,1s) join sta c on b.ts = c.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a left join sta b on a.ts = b.ts left join sta c on b.ts = c.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a left semi join sta b on a.ts = b.ts left join sta c on b.ts = c.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a left anti join sta b on a.ts = b.ts left semi join sta c on b.ts = c.ts;"
        )
        tdSql.query(
            f"select a.ts from sta a full join sta b on a.ts = b.ts join sta c on b.ts = c.ts;"
        )

        # timeline + inner join
        tdSql.query(
            f"select sum(c1) from (select a.col1 c1 from sta a join sta b on a.ts = b.ts and a.t1 = b.t1);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 30)

        tdSql.query(
            f"select sum(c1) from (select a.col1 c1 from sta a join sta b on a.ts = b.ts);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 42)

        tdSql.query(
            f"select diff(c1) from (select a.ts, a.col1 c1 from tba1 a join sta b on a.ts = b.ts where b.t1 > 1);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.query(
            f"select diff(c1) from (select a.ts, a.col1 c1 from tba1 a left join sta b on a.ts = b.ts where b.t1 > 1);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.query(
            f"select diff(c1) from (select a.ts, a.col1 c1 from tba1 a left semi join sta b on a.ts = b.ts where b.t1 > 1);"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select diff(c1) from (select a.ts, a.col1 c1 from tba1 a left anti join sta b on a.ts = b.ts where b.t1 > 1);"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select diff(c1) from (select a.ts, a.col1 c1 from tba1 a left asof join sta b);"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select diff(c1) from (select distinct a.ts, a.col1 c1 from tba1 a left window join sta b window_offset(-1s, 0s) order by a.ts);"
        )
        tdSql.checkRows(3)

        tdSql.query(
            f"select diff(c1) from (select b.col1 c1, a.ts from sta a join sta b on a.ts = b.ts and a.t1 = b.t1 where a.t1 > 1 order by a.ts);"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(2, 0, 2)

        tdSql.query(
            f"select count(c1) from (select a.ts, a.col1 c1 from tba1 a join sta b on a.ts = b.ts) interval(1s);"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(3, 0, 1)

        tdSql.query(
            f"select count(c1) from (select a.ts ts1, a.col1 c1 from tba1 a join sta b on a.ts = b.ts) c session(c.ts1, 1s);"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 4)

        tdSql.query(
            f"select count(c1) from (select b.ts, a.col1 c1 from tba1 a join sta b on a.ts = b.ts) c session(c.ts, 1s);"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 4)

        tdSql.query(
            f"select count(c1) from (select b.ts, a.col1 c1 from tba1 a join sta b on a.ts = b.ts partition by a.col1 order by a.ts) c session(c.ts, 1s);"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 4)

        tdSql.error(
            f"select diff(c1) from (select a.ts, a.col1 c1 from sta a join sta b on a.ts = b.ts);"
        )
        tdSql.error(
            f"select diff(c1) from (select a.col1 c1 from sta a join sta b on a.ts = b.ts and a.t1 = b.t1);"
        )
        tdSql.error(
            f"select diff(c1) from (select a.col1 c1 from sta a join sta b on a.ts = b.ts);"
        )
        tdSql.error(
            f"select diff(c1) from (select a.col1 c1 from sta a join sta b on a.ts = b.ts and a.t1 = b.t1 order by a.ts);"
        )
        tdSql.error(
            f"select diff(c1) from (select a.col1 c1, a.ts from sta a join sta b on a.ts = b.ts and a.t1 = b.t1 order by a.ts);"
        )
        tdSql.error(
            f"select count(c1) from (select a.col1 c1 from tba1 a join sta b on a.ts = b.ts) interval(1s);"
        )
        tdSql.error(
            f"select count(c1) from (select b.ts, a.col1 c1 from tba1 a join sta b on a.ts = b.ts partition by a.col1) c session(c.ts, 1s);"
        )

        # timeline + left join
        tdSql.query(
            f"select diff(c1) from (select a.ts, a.col1 c1 from tba1 a left join tba2 b on a.ts = b.ts);"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)

        tdSql.error(
            f"select diff(c1) from (select b.col1 c1 from tba1 a left join tba2 b on a.ts = b.ts);"
        )
        tdSql.error(
            f"select diff(c1) from (select b.ts, b.col1 c1 from tba1 a left join tba2 b on a.ts = b.ts);"
        )
        tdSql.query(
            f"select diff(c1) from (select a.ts, b.col1 c1 from tba1 a left join tba2 b on a.ts = b.ts);"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, None)

        tdSql.error(
            f"select count(c1) from (select a.col1 c1 from tba1 a left join sta b on a.ts = b.ts) interval(1s);"
        )
        tdSql.error(
            f"select count(c1) from (select b.ts, a.col1 c1 from tba1 a left join sta b on a.ts = b.ts) interval(1s);"
        )
        tdSql.error(
            f"select count(c1) from (select b.ts, a.col1 c1 from tba1 a left join sta b on a.ts = b.ts order by b.ts) interval(1s);"
        )
        tdSql.query(
            f"select count(c1) from (select a.ts, a.col1 c1 from tba1 a left join sta b on a.ts = b.ts) interval(1s);"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 2)
        tdSql.checkData(3, 0, 1)

        tdSql.error(
            f"select count(c1) from (select b.ts ts1, a.col1 c1 from tba1 a left join sta b on a.ts = b.ts) c session(c.ts1, 1s);"
        )
        tdSql.error(
            f"select count(c1) from (select b.ts ts1, a.col1 c1 from tba1 a left join sta b on a.ts = b.ts order by ts1) c session(c.ts1, 1s);"
        )
        tdSql.query(
            f"select count(c1) from (select a.ts ts1, a.col1 c1 from tba1 a left join sta b on a.ts = b.ts) c session(c.ts1, 1s);"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 4)

        # timeline + left semi join
        tdSql.query(
            f"select diff(c1) from (select a.ts, a.col1 c1 from tba1 a left semi join tba2 b on a.ts = b.ts);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.error(
            f"select diff(c1) from (select b.col1 c1 from tba1 a left semi join tba2 b on a.ts = b.ts);"
        )
        tdSql.query(
            f"select diff(c1) from (select b.ts, b.col1 c1 from tba1 a left semi join tba2 b on a.ts = b.ts);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.query(
            f"select diff(c1) from (select a.ts, b.col1 c1 from tba1 a left semi join tba2 b on a.ts = b.ts);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.error(
            f"select count(c1) from (select a.col1 c1 from tba1 a left semi join sta b on a.ts = b.ts) interval(1s);"
        )
        tdSql.query(
            f"select count(c1) from (select b.ts, a.col1 c1 from tba1 a left semi join sta b on a.ts = b.ts) interval(1s);"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)

        tdSql.query(
            f"select count(c1) from (select b.ts, a.col1 c1 from tba1 a left semi join sta b on a.ts = b.ts order by b.ts) interval(1s);"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)

        tdSql.query(
            f"select count(c1) from (select a.ts, a.col1 c1 from tba1 a left semi join sta b on a.ts = b.ts) interval(1s);"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)

        tdSql.query(
            f"select count(c1) from (select b.ts ts1, a.col1 c1 from tba1 a left semi join sta b on a.ts = b.ts) c session(c.ts1, 1s);"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 3)

        tdSql.query(
            f"select count(c1) from (select b.ts ts1, a.col1 c1 from tba1 a left semi join sta b on a.ts = b.ts order by ts1) c session(c.ts1, 1s);"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 3)

        tdSql.query(
            f"select count(c1) from (select a.ts ts1, a.col1 c1 from tba1 a left semi join sta b on a.ts = b.ts) c session(c.ts1, 1s);"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 3)

        # timeline + left anti join
        tdSql.query(
            f"select diff(c1) from (select a.ts, a.col1 c1 from tba1 a left anti join tba2 b on a.ts = b.ts);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        tdSql.error(
            f"select diff(c1) from (select b.col1 c1 from tba1 a left anti join tba2 b on a.ts = b.ts);"
        )
        tdSql.error(
            f"select diff(c1) from (select b.ts, b.col1 c1 from tba1 a left anti join tba2 b on a.ts = b.ts);"
        )

        # ????
        tdSql.query(
            f"select diff(c1) from (select b.ts, b.col1 c1,a.ts from tba1 a left anti join tba2 b on a.ts = b.ts);"
        )
        tdSql.query(
            f"select diff(c1) from (select a.ts, b.col1 c1 from tba1 a left anti join tba2 b on a.ts = b.ts);"
        )

        tdSql.error(
            f"select count(c1) from (select a.col1 c1 from tba1 a left anti join sta b on a.ts = b.ts) interval(1s);"
        )
        tdSql.error(
            f"select count(c1) from (select b.ts, a.col1 c1 from tba1 a left anti join sta b on a.ts = b.ts) interval(1s);"
        )
        tdSql.error(
            f"select count(c1) from (select b.ts, a.col1 c1 from tba1 a left anti join sta b on a.ts = b.ts order by b.ts) interval(1s);"
        )
        tdSql.query(
            f"select count(c1) from (select a.ts, a.col1 c1 from tba1 a left anti join sta b on a.ts = b.ts) interval(1s);"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select count(c1) from (select a.col1 c1,a.ts from tba1 a left anti join sta b on a.ts = b.ts) interval(1s);"
        )
        tdSql.checkRows(0)

        tdSql.error(
            f"select count(c1) from (select b.ts ts1, a.col1 c1 from tba1 a left anti join sta b on a.ts = b.ts) c session(c.ts1, 1s);"
        )
        tdSql.error(
            f"select count(c1) from (select b.ts ts1, a.col1 c1 from tba1 a left anti join sta b on a.ts = b.ts order by ts1) c session(c.ts1, 1s);"
        )
        tdSql.query(
            f"select count(c1) from (select a.ts ts1, a.col1 c1 from tba1 a left anti join sta b on a.ts = b.ts) c session(c.ts1, 1s);"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select count(c1) from (select a.col1 c1, timetruncate(a.ts, 1d) ts1 from tba1 a left anti join sta b on a.ts = b.ts) c session(c.ts1, 1s);"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select count(c1) from (select a.col1 c1, a.ts + 1s ts1 from tba1 a left anti join sta b on a.ts = b.ts) c session(c.ts1, 1s);"
        )
        tdSql.checkRows(0)

        # timeline + left asof join
        tdSql.query(
            f"select diff(c1) from (select a.ts, a.col1 c1 from tba1 a left asof join tba2 b);"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)

        tdSql.error(
            f"select diff(c1) from (select a.ts, a.col1 c1 from tba1 a left asof join tba2 b on a.col1 = b.col1);"
        )
        tdSql.error(
            f"select diff(c1) from (select b.col1 c1 from tba1 a left asof join tba2 b on a.ts = b.ts);"
        )
        tdSql.error(
            f"select diff(c1) from (select b.ts, b.col1 c1 from tba1 a left asof join tba2 b on a.ts = b.ts);"
        )

        tdSql.query(
            f"select diff(c1) from (select b.ts, b.col1 c1,a.ts from tba1 a left asof join tba2 b on a.ts = b.ts);"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, None)

        tdSql.error(
            f"select count(c1) from (select a.col1 c1 from tba1 a left asof join sta b on a.ts = b.ts) interval(1s);"
        )
        tdSql.error(
            f"select count(c1) from (select b.ts, a.col1 c1 from tba1 a left asof join sta b on a.ts = b.ts) interval(1s);"
        )
        tdSql.error(
            f"select count(c1) from (select b.ts, a.col1 c1 from tba1 a left asof join sta b on a.ts = b.ts order by b.ts) interval(1s);"
        )
        tdSql.query(
            f"select count(c1) from (select a.ts, a.col1 c1 from tba1 a left asof join sta b on a.col1 = b.col1) interval(1s);"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)

        tdSql.query(
            f"select count(c1) from (select a.ts, a.col1 c1 from tba1 a left asof join sta b on a.ts = b.ts) interval(1s);"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)

        tdSql.query(
            f"select count(c1) from (select a.col1 c1,a.ts from tba1 a left asof join sta b on a.ts = b.ts) interval(1s);"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)

        tdSql.error(
            f"select count(c1) from (select b.ts ts1, a.col1 c1 from tba1 a left asof join sta b on a.ts = b.ts) c session(c.ts1, 1s);"
        )
        tdSql.error(
            f"select count(c1) from (select b.ts ts1, a.col1 c1 from tba1 a left asof join sta b on a.ts = b.ts order by ts1) c session(c.ts1, 1s);"
        )
        tdSql.error(
            f"select count(c1) from (select a.ts ts1, a.col1 c1 from tba1 a left asof join sta b on a.col1 = b.col1) c session(c.ts1, 1s);"
        )
        tdSql.query(
            f"select count(c1) from (select a.ts ts1, a.col1 c1 from tba1 a left asof join sta b on a.ts = b.ts) c session(c.ts1, 1s);"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 3)

        tdSql.error(
            f"select count(c1) from (select a.col1 c1, timetruncate(a.ts, 1d) ts1 from tba1 a left asof join sta b on a.col1 = b.col1) c session(c.ts1, 1s);"
        )
        tdSql.query(
            f"select count(c1) from (select a.col1 c1, timetruncate(a.ts, 1d) ts1 from tba1 a left asof join sta b on a.ts = b.ts) c session(c.ts1, 1s);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4)

        tdSql.error(
            f"select count(c1) from (select a.col1 c1, a.ts + 1s ts1 from tba1 a left asof join sta b on a.col1 = b.col1) c session(c.ts1, 1s);"
        )
        tdSql.query(
            f"select count(c1) from (select a.col1 c1, a.ts + 1s ts1 from tba1 a left asof join sta b on a.ts = b.ts) c session(c.ts1, 1s);"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 3)

        # timeline + left window join
        tdSql.query(
            f"select diff(c1) from (select a.ts, a.col1 c1 from tba1 a left window join tba2 b window_offset(1s,1s));"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)

        tdSql.error(
            f"select diff(c1) from (select a.ts, a.col1 c1 from tba1 a left window join tba2 b on a.col1 = b.col1 window_offset(1s,1s));"
        )
        tdSql.error(
            f"select diff(c1) from (select b.col1 c1 from tba1 a left window join tba2 b window_offset(1s,1s));"
        )
        tdSql.error(
            f"select diff(c1) from (select b.ts, b.col1 c1 from tba1 a left window join tba2 b window_offset(1s,1s));"
        )

        tdSql.query(
            f"select diff(c1) from (select b.ts, b.col1 c1,a.ts from tba1 a left window join tba2 b window_offset(1s,1s));"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, 2)

        tdSql.error(
            f"select count(c1) from (select a.col1 c1 from tba1 a left window join sta b window_offset(-1s,1s)) interval(1s);"
        )
        tdSql.error(
            f"select count(c1) from (select b.ts, a.col1 c1 from tba1 a left window join sta b window_offset(-1s,1s)) interval(1s);"
        )
        tdSql.error(
            f"select count(c1) from (select b.ts, a.col1 c1 from tba1 a left window join sta b window_offset(-1s,1s) order by b.ts) interval(1s);"
        )
        tdSql.query(
            f"select count(c1) from (select a.ts, a.col1 c1 from tba1 a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s)) interval(1s);"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 2)

        tdSql.query(
            f"select count(c1) from (select a.ts, a.col1 c1 from tba1 a left window join sta b window_offset(-1s,1s)) interval(1s);"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 4)
        tdSql.checkData(2, 0, 4)
        tdSql.checkData(3, 0, 4)

        tdSql.query(
            f"select count(c1) from (select a.col1 c1,a.ts from tba1 a left window join sta b window_offset(-1s,1s)) interval(1s);"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 4)
        tdSql.checkData(2, 0, 4)
        tdSql.checkData(3, 0, 4)

        tdSql.error(
            f"select count(c1) from (select b.ts ts1, a.col1 c1 from tba1 a left window join sta b window_offset(-1s,1s)) c session(c.ts1, 1s);"
        )
        tdSql.error(
            f"select count(c1) from (select b.ts ts1, a.col1 c1 from tba1 a left window join sta b window_offset(-1s,1s) order by ts1) c session(c.ts1, 1s);"
        )
        tdSql.error(
            f"select count(c1) from (select a.ts ts1, a.col1 c1 from tba1 a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s)) c session(c.ts1, 1s);"
        )
        tdSql.query(
            f"select count(c1) from (select a.ts ts1, a.col1 c1 from tba1 a left window join sta b window_offset(-1s,1s)) c session(c.ts1, 1s);"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 12)

        tdSql.error(
            f"select count(c1) from (select a.col1 c1, timetruncate(a.ts, 1d) ts1 from tba1 a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s)) c session(c.ts1, 1s);"
        )
        tdSql.query(
            f"select count(c1) from (select a.col1 c1, timetruncate(a.ts, 1d) ts1 from tba1 a left window join sta b window_offset(-1s,1s)) c session(c.ts1, 1s);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 15)

        tdSql.error(
            f"select count(c1) from (select a.col1 c1, a.ts + 1s ts1 from tba1 a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s)) c session(c.ts1, 1s);"
        )
        tdSql.query(
            f"select count(c1) from (select a.col1 c1, a.ts + 1s ts1 from tba1 a left window join sta b window_offset(-1s,1s)) c session(c.ts1, 1s);"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 12)

        # timeline + full join
        tdSql.error(
            f"select diff(c1) from (select a.ts, a.col1 c1 from tba1 a full join tba2 b on a.ts = b.ts);"
        )
        tdSql.error(
            f"select diff(c1) from (select b.col1 c1 from tba1 a full join tba2 b on a.ts = b.ts);"
        )
        tdSql.error(
            f"select diff(c1) from (select b.ts, b.col1 c1 from tba1 a full join tba2 b on a.ts = b.ts);"
        )
        tdSql.error(
            f"select diff(c1) from (select a.ts, b.col1 c1 from tba1 a full join tba2 b on a.ts = b.ts);"
        )
        tdSql.error(
            f"select count(c1) from (select a.col1 c1 from tba1 a full join sta b on a.ts = b.ts) interval(1s);"
        )
        tdSql.error(
            f"select count(c1) from (select b.ts, a.col1 c1 from tba1 a full join sta b on a.ts = b.ts) interval(1s);"
        )
        tdSql.error(
            f"select count(c1) from (select b.ts, a.col1 c1 from tba1 a full join sta b on a.ts = b.ts order by b.ts) interval(1s);"
        )
        tdSql.error(
            f"select count(c1) from (select a.ts, a.col1 c1 from tba1 a full join sta b on a.ts = b.ts) interval(1s);"
        )
        tdSql.error(
            f"select count(c1) from (select b.ts ts1, a.col1 c1 from tba1 a full join sta b on a.ts = b.ts) c session(c.ts1, 1s);"
        )
        tdSql.error(
            f"select count(c1) from (select b.ts ts1, a.col1 c1 from tba1 a full join sta b on a.ts = b.ts order by ts1) c session(c.ts1, 1s);"
        )
        tdSql.error(
            f"select count(c1) from (select a.ts ts1, a.col1 c1 from tba1 a full join sta b on a.ts = b.ts) c session(c.ts1, 1s);"
        )

    def join_boundary(self):
        tdSql.execute(f"use test0;")

        # join type
        tdSql.error(f"select a.ts from sta a outer join sta b on b.ts = b.ts;")
        tdSql.error(f"select a.ts from sta a semi join sta b on b.ts = b.ts;")
        tdSql.error(f"select a.ts from sta a anti join sta b on b.ts = b.ts;")
        tdSql.error(f"select a.ts from sta a asof join sta b on b.ts = b.ts;")
        tdSql.error(f"select a.ts from sta a window join sta b window_offset(-1s, 1s);")
        tdSql.error(f"select a.ts from sta a inner outer join sta b on b.ts = b.ts;")
        tdSql.error(f"select a.ts from sta a inner semi join sta b on b.ts = b.ts;")
        tdSql.error(f"select a.ts from sta a inner anti join sta b on b.ts = b.ts;")
        tdSql.error(f"select a.ts from sta a inner asof join sta b on b.ts = b.ts;")
        tdSql.error(f"select a.ts from sta a inner window join sta b on b.ts = b.ts;")
        tdSql.error(f"select a.ts from sta a left inner join sta b on b.ts = b.ts;")
        tdSql.error(f"select a.ts from sta a right inner join sta b on b.ts = b.ts;")
        tdSql.error(f"select a.ts from sta a full inner join sta b on b.ts = b.ts;")
        tdSql.error(f"select a.ts from sta a full semi join sta b on b.ts = b.ts;")
        tdSql.error(f"select a.ts from sta a full anti join sta b on b.ts = b.ts;")
        tdSql.error(f"select a.ts from sta a full asof join sta b on b.ts = b.ts;")
        tdSql.error(
            f"select a.ts from sta a full window join sta b window_offset(-1s, 1s);"
        )

        # inner join
        tdSql.error(f"select a.ts from sta a join sta b;")
        tdSql.error(f"select a.ts from sta a join sta b on a.ts = b.ts or a.ts = b.ts;")
        tdSql.error(
            f"select a.ts from sta a join sta b where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(f"select a.ts from sta a join sta b on a.col1 = b.col1;")
        tdSql.error(f"select a.ts from sta a join sta b on a.col1 is null;")
        tdSql.error(f"select a.ts from sta a join sta b on a.ts + 1 = b.col1;")
        tdSql.error(
            f"select a.ts from sta a join sta b on timetruncate(a.ts, 1d) > b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a join sta b on timetruncate(a.ts, 1d) = b.ts + 1;"
        )
        tdSql.query(f"select a.ts from sta a join sta b on a.ts = b.ts;")
        tdSql.query(
            f"select a.ts from sta a join sta b on timetruncate(a.ts, 1d) = b.ts;"
        )
        tdSql.query(
            f"select a.ts from sta a join sta b on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1w);"
        )
        tdSql.error(
            f"select a.ts from sta a join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1;"
        )
        tdSql.error(
            f"select a.ts from sta a join sta b on timetruncate(a.ts, 1d) = b.ts window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"select a.ts from sta a join sta b on a.col1 = b.col1 where a.col1=b.col1;"
        )
        tdSql.query(
            f"select a.ts from sta a join sta b on a.col1 = b.col1 where a.ts=b.ts;"
        )
        tdSql.query(f"select a.ts from sta a join sta b where a.ts=b.ts;")
        tdSql.error(f"select a.ts from sta a ,sta b on a.ts=b.ts;")
        tdSql.query(f"select a.ts from sta a ,sta b where a.ts=b.ts;")
        tdSql.query(
            f"select a.ts from sta a ,sta b where a.ts=b.ts and a.col1 + 1 = b.col1;"
        )
        tdSql.query(
            f"select b.col1 from sta a ,sta b where a.ts=b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"select b.col1 from sta a join sta b join sta c where a.ts=b.ts and b.ts = c.ts order by a.ts;"
        )
        tdSql.query(
            f"select b.col1 from (select ts from sta) a join (select ts, col1 from sta) b join sta c where a.ts=b.ts and b.ts = c.ts order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from sta a join sta b join sta c where a.ts=b.ts and a.ts = b.ts order by a.ts;"
        )
        tdSql.query(
            f"(select b.col1 from sta a join sta b where a.ts=b.ts order by a.ts) union (select a.col1 from sta a join sta b where a.ts=b.ts order by a.ts);"
        )
        tdSql.query(
            f"(select b.col1 from sta a join sta b where a.ts=b.ts order by a.ts) union all (select a.col1 from sta a join sta b where a.ts=b.ts order by a.ts);"
        )
        tdSql.error(
            f"(select b.col1 from sta a join sta b where a.ts=b.ts order by a.ts) union (select a.col1 from sta a join sta b where a.ts=b.ts) order by a.ts;"
        )
        tdSql.error(
            f"(select b.col1 from sta a join sta b where a.ts=b.ts order by a.ts) union all (select a.col1 from sta a join sta b where a.ts=b.ts) order by a.ts;"
        )
        tdSql.query(
            f"(select b.col1 from sta a join sta b where a.ts=b.ts order by a.ts) union (select a.col1 from sta a join sta b where a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a join sta b where a.ts=b.ts order by a.ts) union all (select a.col1 from sta a join sta b where a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a join sta b where a.ts=b.ts order by a.ts) union (select a.col1 from sta a join sta b where a.ts=b.ts) order by 1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a join sta b where a.ts=b.ts order by a.ts) union all (select a.col1 from sta a join sta b where a.ts=b.ts) order by 1;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a join sta b where a.ts=b.ts order by a.ts) union (select b.ts from sta a join sta b where a.ts=b.ts)) c join ((select a.ts from sta a join sta b where a.ts=b.ts order by a.ts) union all (select b.ts from sta a join sta b where a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a join sta b where a.ts=b.ts order by a.ts) union all (select b.ts from sta a join sta b where a.ts=b.ts)) c join ((select a.ts from sta a join sta b where a.ts=b.ts order by a.ts) union all (select b.ts from sta a join sta b where a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.query(
            f"select c.ts from ((select a.ts from sta a join sta b where a.ts=b.ts order by a.ts) union (select b.ts from sta a join sta b where a.ts=b.ts) order by 1) c join ((select a.ts from sta a join sta b where a.ts=b.ts order by a.ts) union (select b.ts from sta a join sta b where a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )
        tdSql.query(
            f"select c.ts from ((select a.ts from sta a join sta b where a.ts=b.ts order by a.ts) union all (select b.ts from sta a join sta b where a.ts=b.ts) order by 1) c join ((select a.ts from sta a join sta b where a.ts=b.ts order by a.ts) union all (select b.ts from sta a join sta b where a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )
        tdSql.query(f"select a.ts from test0.sta a ,testb.stb1 b where a.ts=b.ts;")

        # left join
        tdSql.error(f"select a.ts from sta a left join sta b;")
        tdSql.error(
            f"select a.ts from sta a left join sta b on a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a left join sta b where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(f"select a.ts from sta a left join sta b on a.col1 = b.col1;")
        tdSql.error(f"select a.ts from sta a left join sta b on a.col1 is not NULL;")
        tdSql.error(f"select a.ts from sta a left join sta b on a.ts + 1 = b.col1;")
        tdSql.error(
            f"select a.ts from sta a left join sta b on timetruncate(a.ts, 1d) > b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a left join sta b on timetruncate(a.ts, 1d) = b.ts + 1;"
        )
        tdSql.error(
            f"select a.ts from sta a left join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1;"
        )
        tdSql.error(
            f"select a.ts from sta a left join sta b on timetruncate(a.ts, 1d) = b.ts window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select a.ts from sta a left join sta b on timetruncate(a.ts, 1d) = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a left join sta b on a.col1 = b.col1 where a.col1 = b.col1;"
        )
        tdSql.error(
            f"select a.ts from sta a left join sta b on a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.error(f"select a.ts from sta a left join sta b where a.ts = b.ts;")
        tdSql.error(
            f"select b.col1 from sta a left join sta b where a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"select b.col1 from sta a left join sta b on a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"select b.col1 from (select ts from sta) a left join (select ts, col1 from sta) b on a.ts = b.ts order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from (select ts from sta) a left join (select ts, col1 from sta) b left join sta c on a.ts=b.ts and b.ts = c.ts order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from sta a left join sta b left join sta c on a.ts=b.ts and a.ts = b.ts order by a.ts;"
        )
        tdSql.query(
            f"select a.ts from test0.sta a left join testb.stb1 b on a.ts = b.ts;"
        )
        tdSql.query(
            f"(select b.col1 from sta a left join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.query(
            f"(select b.col1 from sta a left join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.error(
            f"(select b.col1 from sta a left join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.error(
            f"(select b.col1 from sta a left join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.query(
            f"(select b.col1 from sta a left join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a left join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a left join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a left join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a left join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a left join sta b on a.ts=b.ts)) c left join ((select a.ts from sta a left join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a left join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left join sta b on a.ts=b.ts)) c left join ((select a.ts from sta a left join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a left join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a left join sta b on a.ts=b.ts) order by 1) c left join ((select a.ts from sta a left join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a left join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a left join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left join sta b on a.ts=b.ts) order by 1) c left join ((select a.ts from sta a left join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )

        # left semi join
        tdSql.error(f"select a.ts from sta a left semi join sta b;")
        tdSql.error(
            f"select a.ts from sta a left semi join sta b on a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a left semi join sta b where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(f"select a.ts from sta a left semi join sta b on a.col1 = b.col1;")
        tdSql.error(f"select a.ts from sta a left semi join sta b on a.col1 like '1';")
        tdSql.error(f"select a.ts from sta a left semi join sta b on a.col1 is null;")
        tdSql.error(
            f"select a.ts from sta a left semi join sta b on a.ts + 1 = b.col1;"
        )
        tdSql.error(
            f"select a.ts from sta a left semi join sta b on timetruncate(a.ts, 1d) > b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a left semi join sta b on timetruncate(a.ts, 1d) = b.ts + 1;"
        )
        tdSql.error(
            f"select a.ts from sta a left semi join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1;"
        )
        tdSql.error(
            f"select a.ts from sta a left semi join sta b on timetruncate(a.ts, 1d) = b.ts window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select a.ts from sta a left semi join sta b on timetruncate(a.ts, 1d) = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a left semi join sta b on a.col1 = b.col1 where a.col1 = b.col1;"
        )
        tdSql.error(
            f"select a.ts from sta a left semi join sta b on a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.error(f"select a.ts from sta a left semi join sta b where a.ts = b.ts;")
        tdSql.error(
            f"select b.col1 from sta a left semi join sta b where a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"select b.col1 from sta a left semi join sta b on a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"select b.col1 from (select ts from sta) a left semi join (select ts, col1 from sta) b on a.ts=b.ts order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from (select ts from sta) a left semi join (select ts, col1 from sta) b left semi join sta c on a.ts=b.ts and b.ts = c.ts order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from sta a left semi join sta b left semi join sta c on a.ts=b.ts and a.ts = b.ts order by a.ts;"
        )
        tdSql.query(
            f"select a.ts from test0.sta a left semi join testb.stb1 b on a.ts = b.ts;"
        )
        tdSql.query(
            f"(select b.col1 from sta a left semi join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left semi join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.query(
            f"(select b.col1 from sta a left semi join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left semi join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.error(
            f"(select b.col1 from sta a left semi join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left semi join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.error(
            f"(select b.col1 from sta a left semi join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left semi join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.query(
            f"(select b.col1 from sta a left semi join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left semi join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a left semi join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left semi join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a left semi join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left semi join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a left semi join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left semi join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a left semi join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a left semi join sta b on a.ts=b.ts)) c left semi join ((select a.ts from sta a left semi join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left semi join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a left semi join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left semi join sta b on a.ts=b.ts)) c left semi join ((select a.ts from sta a left semi join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left semi join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a left semi join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a left semi join sta b on a.ts=b.ts) order by 1) c left semi join ((select a.ts from sta a left semi join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a left semi join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a left semi join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left semi join sta b on a.ts=b.ts) order by 1) c left semi join ((select a.ts from sta a left semi join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left semi join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )

        # left anti join
        tdSql.error(f"select a.ts from sta a left anti join sta b;")
        tdSql.error(
            f"select a.ts from sta a left anti join sta b on a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a left anti join sta b where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(f"select a.ts from sta a left anti join sta b on a.col1 = b.col1;")
        tdSql.error(f"select a.ts from sta a left anti join sta b on a.col1 / 1;")
        tdSql.error(f"select a.ts from sta a left anti join sta b on a.col1 is null;")
        tdSql.error(
            f"select a.ts from sta a left anti join sta b on a.ts + 1 = b.col1;"
        )
        tdSql.error(
            f"select a.ts from sta a left anti join sta b on timetruncate(a.ts, 1d) > b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a left anti join sta b on timetruncate(a.ts, 1d) = b.ts + 1;"
        )
        tdSql.error(
            f"select a.ts from sta a left anti join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1;"
        )
        tdSql.error(
            f"select a.ts from sta a left anti join sta b on timetruncate(a.ts, 1d) = b.ts window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select a.ts from sta a left anti join sta b on timetruncate(a.ts, 1d) = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a left anti join sta b on a.col1 = b.col1 where a.col1 = b.col1;"
        )
        tdSql.error(
            f"select a.ts from sta a left anti join sta b on a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.error(f"select a.ts from sta a left anti join sta b where a.ts = b.ts;")
        tdSql.error(
            f"select b.col1 from sta a left anti join sta b where a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"select b.col1 from sta a left anti join sta b on a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"select b.col1 from (select ts from sta) a left anti join (select ts, col1 from sta) b on a.ts=b.ts order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from (select ts from sta) a left anti join (select ts, col1 from sta) b left anti join sta c on a.ts=b.ts and b.ts = c.ts order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from sta a left anti join sta b left anti join sta c on a.ts=b.ts and a.ts = b.ts order by a.ts;"
        )
        tdSql.query(
            f"select a.ts from test0.sta a left anti join testb.stb1 b on a.ts = b.ts;"
        )
        tdSql.query(
            f"(select b.col1 from sta a left anti join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left anti join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.query(
            f"(select b.col1 from sta a left anti join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left anti join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.error(
            f"(select b.col1 from sta a left anti join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left anti join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.error(
            f"(select b.col1 from sta a left anti join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left anti join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.query(
            f"(select b.col1 from sta a left anti join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left anti join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a left anti join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left anti join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a left anti join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left anti join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a left anti join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left anti join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a left anti join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a left anti join sta b on a.ts=b.ts)) c left anti join ((select a.ts from sta a left anti join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left anti join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a left anti join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left anti join sta b on a.ts=b.ts)) c left anti join ((select a.ts from sta a left anti join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left anti join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a left anti join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a left anti join sta b on a.ts=b.ts) order by 1) c left anti join ((select a.ts from sta a left anti join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a left anti join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a left anti join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left anti join sta b on a.ts=b.ts) order by 1) c left anti join ((select a.ts from sta a left anti join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left anti join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )

        # left asof join
        tdSql.query(f"select a.ts from sta a left asof join sta b;")
        tdSql.error(
            f"select a.ts from sta a left asof join sta b on a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a left asof join sta b on a.ts = b.ts and a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a left asof join sta b on a.ts = b.ts or a.col1 = b.col1;"
        )
        tdSql.query(
            f"select a.ts from sta a left asof join sta b on a.ts = b.ts and a.col1 = b.col1;"
        )
        tdSql.query(
            f"select a.ts from sta a left asof join sta b on a.col1 = b.col1 and a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.col1 from sta a left asof join sta b on a.col1=b.col1 and a.t1 = b.t1;"
        )
        tdSql.query(f"select a.ts from sta a left asof join sta b where a.ts = b.ts;")
        tdSql.query(
            f"select a.ts from sta a left asof join sta b where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.query(f"select a.ts from sta a left asof join sta b on a.col1 = b.col1;")
        tdSql.error(f"select a.ts from sta a left asof join sta b on a.ts != b.ts;")
        tdSql.error(f"select a.ts from sta a left asof join sta b on a.ts is null;")
        tdSql.error(
            f"select a.ts from sta a left asof join sta b on a.ts + 1 = b.col1;"
        )
        tdSql.query(
            f"select a.ts from sta a left asof join sta b on timetruncate(a.ts, 1d) > b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a left asof join sta b on timetruncate(a.ts, 1d) = b.ts + 1;"
        )
        tdSql.query(
            f"select a.ts from sta a left asof join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1;"
        )
        tdSql.query(
            f"select a.ts from sta a left asof join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 0;"
        )
        tdSql.query(
            f"select a.ts from sta a left asof join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1024;"
        )
        tdSql.error(
            f"select a.ts from sta a left asof join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1025;"
        )
        tdSql.error(
            f"select a.ts from sta a left asof join sta b on timetruncate(a.ts, 1d) = b.ts jlimit -1;"
        )
        tdSql.error(
            f"select a.ts from sta a left asof join sta b on timetruncate(a.ts, 1d) = b.ts window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select a.ts from sta a left asof join sta b on timetruncate(a.ts, 1d) = b.ts;"
        )
        tdSql.query(
            f"select a.ts from sta a left asof join sta b on a.col1 = b.col1 where a.col1 = b.col1;"
        )
        tdSql.query(
            f"select a.ts from sta a left asof join sta b on a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.query(f"select a.ts from sta a left asof join sta b where a.ts = b.ts;")
        tdSql.query(
            f"select b.col1 from sta a left asof join sta b where a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from sta a left asof join sta b on a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from sta a left asof join sta b on a.ts = b.ts and 1 = 2 order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from (select ts from sta) a left asof join (select ts, col1 from sta) b on a.ts=b.ts order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from (select ts from sta) a left asof join (select ts, col1 from sta) b left asof join sta c on a.ts=b.ts and b.ts = c.ts order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from sta a left asof join sta b left asof join sta c on a.ts=b.ts and a.ts = b.ts order by a.ts;"
        )
        tdSql.query(
            f"select a.ts from test0.sta a left asof join testb.stb1 b on a.ts = b.ts;"
        )
        tdSql.query(
            f"(select b.col1 from sta a left asof join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left asof join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.query(
            f"(select b.col1 from sta a left asof join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left asof join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.error(
            f"(select b.col1 from sta a left asof join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left asof join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.error(
            f"(select b.col1 from sta a left asof join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left asof join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.query(
            f"(select b.col1 from sta a left asof join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left asof join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a left asof join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left asof join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a left asof join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left asof join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a left asof join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left asof join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a left asof join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a left asof join sta b on a.ts=b.ts)) c left asof join ((select a.ts from sta a left asof join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left asof join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a left asof join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left asof join sta b on a.ts=b.ts)) c left asof join ((select a.ts from sta a left asof join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left asof join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a left asof join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a left asof join sta b on a.ts=b.ts) order by 1) c left asof join ((select a.ts from sta a left asof join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a left asof join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a left asof join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left asof join sta b on a.ts=b.ts) order by 1) c left asof join ((select a.ts from sta a left asof join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left asof join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )

        # left window join
        tdSql.error(f"select a.ts from sta a left window join sta b;")
        tdSql.error(
            f"select a.ts from sta a left window join sta b on a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b on a.ts = b.ts and a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b on a.ts = b.ts or a.col1 = b.col1;"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b on a.ts = b.ts and a.col1 = b.col1;"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b on a.ts = b.ts and a.col1 = b.col1 window_offset(-1s,1s);"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b on a.col1 = b.col1 and a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b on a.col1 = b.col1 and a.ts = b.ts window_offset(-1s,1s);"
        )
        tdSql.error(f"select a.ts from sta a left window join sta b where a.ts = b.ts;")
        tdSql.query(
            f"select a.ts from sta a left window join sta b window_offset(-1s,1s) where a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts from sta a left window join sta b window_offset(-1s,1s) where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b on a.col1 = b.col1;"
        )
        tdSql.query(
            f"select a.ts from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s);"
        )
        tdSql.query(
            f"select a.ts from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s,1s);"
        )
        tdSql.query(
            f"select a.ts from sta a left window join sta b on a.t1 = b.t1 and a.col1 = b.col1 window_offset(-1s,1s);"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b on a.t1 = b.t1 or a.col1 = b.col1 window_offset(-1s,1s);"
        )
        tdSql.error(f"select a.ts from sta a left window join sta b on a.ts != b.ts;")
        tdSql.error(
            f"select a.ts from sta a left window join sta b on a.ts != b.ts window_offset(-1s,1s);"
        )
        tdSql.error(f"select a.ts from sta a left window join sta b on a.ts is null;")
        tdSql.error(
            f"select a.ts from sta a left window join sta b on a.ts is null window_offset(-1s,1s);"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b on a.ts + 1 = b.col1;"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b on a.ts + 1 = b.col1 window_offset(-1s,1s);"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b on timetruncate(a.ts, 1d) > b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b on timetruncate(a.ts, 1d) > b.ts window_offset(-1s,1s);"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b on timetruncate(a.ts, 1d) = b.ts + 1;"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b on timetruncate(a.ts, 1d) = b.ts + 1 window_offset(-1s,1s);"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1;"
        )
        tdSql.query(
            f"select a.ts from sta a left window join sta b window_offset(-1s,1s) jlimit 1;"
        )
        tdSql.query(
            f"select a.ts from sta a left window join sta b window_offset(-1s,1s) jlimit 0;"
        )
        tdSql.query(
            f"select a.ts from sta a left window join sta b window_offset(-1s,1s) jlimit 1024;"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b window_offset(-1s,1s) jlimit 1025;"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b window_offset(-1s,1s) jlimit -1;"
        )
        tdSql.query(
            f"select a.ts from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s, 1s) jlimit 1;"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b on timetruncate(a.ts, 1d) = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b on a.col1 = b.col1 where a.col1 = b.col1;"
        )
        tdSql.query(
            f"select a.ts from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 = b.col1;"
        )
        tdSql.query(
            f"select a.ts from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.ts = b.ts;"
        )
        tdSql.error(f"select a.ts from sta a left window join sta b where a.ts = b.ts;")
        tdSql.error(
            f"select b.col1 from sta a left window join sta b where a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"select b.col1 from sta a left window join sta b window_offset(-1s,1s) where a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from sta a left window join sta b on a.ts = b.ts window_offset(-1s,1s) order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from sta a left window join sta b on a.ts = b.ts and a.col1 + 1 = b.col1 window_offset(-1s,1s) order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from sta a left window join sta b on a.ts = b.ts and 1 = 2 window_offset(-1s,1s) order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from (select ts from sta) a left window join (select ts, col1 from sta) b window_offset(-1s,1s) order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from (select ts from sta) a left window join (select ts, col1 from sta) b left window join sta c window_offset(-1s,1s) order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from sta a left window join sta b left window join sta c window_offset(-1s,1s) order by a.ts;"
        )
        tdSql.error(
            f"select a.ts from test0.sta a left window join testb.stb1 b window_offset(-1s,1s);"
        )
        tdSql.query(
            f"select a.ts from testb.stb1 a left window join testb.stb1 b window_offset(-1s,1s);"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b window_offset(1s,-1s) jlimit a.col1;"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b window_offset(1s,-1s) jlimit 1 + 1;"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b window_offset(1s,1s-1s) jlimit 1;"
        )
        tdSql.query(
            f"select a.ts from sta a left window join sta b window_offset(1s,-1s) jlimit 1;"
        )
        tdSql.query(
            f"select a.ts from sta a left window join sta b window_offset(-1a,1a) jlimit 1;"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b window_offset(-1b,1b) jlimit 1;"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b window_offset(-1b,1s) jlimit 1;"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b window_offset(-1u,1u) jlimit 1;"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b window_offset(-1u,1s) jlimit 1;"
        )
        tdSql.query(
            f"select a.ts from sta a left window join sta b window_offset(-1h,1m) jlimit 1;"
        )
        tdSql.query(
            f"select a.ts from sta a left window join sta b window_offset(-1d,1w) jlimit 1;"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b window_offset(-1n,1n) jlimit 1;"
        )
        tdSql.error(
            f"select a.ts from sta a left window join sta b window_offset(-1y,1y) jlimit 1;"
        )

        tdSql.execute(f"use testb;")
        tdSql.error(
            f"select a.ts from stb1 a left window join stb1 b window_offset(-1b,1b) jlimit 1;"
        )
        tdSql.error(
            f"select a.ts from stb1 a left window join stb1 b window_offset(-1b,1s) jlimit 1;"
        )
        tdSql.query(
            f"select a.ts from stb1 a left window join stb1 b window_offset(-1u,1u) jlimit 1;"
        )
        tdSql.query(
            f"select a.ts from stb1 a left window join stb1 b window_offset(-1s,1s) jlimit 1;"
        )

        tdSql.execute(f"use test0;")
        tdSql.error(
            f"select a.col1 from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 = b.col1 group by a.col1;"
        )
        tdSql.error(
            f"select a.col1 from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 = b.col1 partition by a.col1;"
        )
        tdSql.error(
            f"select count(a.col1) from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 = b.col1 interval(1s);"
        )
        tdSql.error(
            f"select count(a.col1) from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 = b.col1 session(a.ts, 1s);"
        )
        tdSql.error(
            f"select count(a.col1) from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 = b.col1 state_window(a.col1);"
        )
        tdSql.query(
            f"select count(a.col1) from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s) having(count(a.col1) > 0);"
        )
        tdSql.error(
            f"select count(a.col1) from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s) having(a.col1 > 0);"
        )
        tdSql.query(
            f"select a.col1, b.col1, count(a.col1) from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 > 0;"
        )
        tdSql.error(
            f"select a.col1, b.col1, count(a.col1) from sta a left window join sta b window_offset(-1s,1s) where a.col1 > 0;"
        )
        tdSql.error(
            f"select diff(a.col1) from sta a left window join sta b window_offset(-1s,1s);"
        )
        tdSql.error(
            f"select csum(a.col1) from sta a left window join sta b window_offset(-1s,1s);"
        )
        tdSql.query(
            f"select diff(a.col1) from tba1 a left window join tba1 b window_offset(0s,0s);"
        )
        tdSql.query(
            f"select csum(a.col1) from tba1 a left window join tba1 b window_offset(0s,0s);"
        )
        tdSql.error(
            f"select interp(a.col1) from tba1 a left window join tba1 b window_offset(0s,0s) RANGE(now -1d, now) every(1s) fill(null);"
        )
        tdSql.error(
            f"select a.col1, b.col1, count(a.col1) from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where count(a.col1) > 0;"
        )
        tdSql.query(
            f"(select b.col1 from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union (select a.col1 from sta a left window join sta b window_offset(-1s,1s) order by a.ts);"
        )
        tdSql.query(
            f"(select b.col1 from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union all (select a.col1 from sta a left window join sta b window_offset(-1s,1s) order by a.ts);"
        )
        tdSql.error(
            f"(select b.col1 from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union (select a.col1 from sta a left window join sta b window_offset(-1s,1s)) order by a.ts;"
        )
        tdSql.error(
            f"(select b.col1 from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union all (select a.col1 from sta a left window join sta b window_offset(-1s,1s)) order by a.ts;"
        )
        tdSql.query(
            f"(select b.col1 from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union (select a.col1 from sta a left window join sta b window_offset(-1s,1s)) order by col1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union all (select a.col1 from sta a left window join sta b window_offset(-1s,1s)) order by col1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union (select a.col1 from sta a left window join sta b window_offset(-1s,1s)) order by 1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union all (select a.col1 from sta a left window join sta b window_offset(-1s,1s)) order by 1;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union (select b.ts from sta a left window join sta b window_offset(-1s,1s))) c left window join ((select a.ts from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union all (select b.ts from sta a left window join sta b window_offset(-1s,1s))) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union all (select b.ts from sta a left window join sta b window_offset(-1s,1s))) c left window join ((select a.ts from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union all (select b.ts from sta a left window join sta b window_offset(-1s,1s))) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union (select b.ts from sta a left window join sta b window_offset(-1s,1s)) order by 1) c left window join ((select a.ts from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union (select b.ts from sta a left window join sta b window_offset(-1s,1s)) order by 1) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union all (select b.ts from sta a left window join sta b window_offset(-1s,1s)) order by 1) c left window join ((select a.ts from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union all (select b.ts from sta a left window join sta b window_offset(-1s,1s)) order by 1) d on c.ts = d.ts;"
        )

        # right join
        tdSql.error(f"select a.ts from sta a right join sta b;")
        tdSql.error(
            f"select a.ts from sta a right join sta b on a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a right join sta b where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(f"select a.ts from sta a right join sta b on a.col1 = b.col1;")
        tdSql.error(f"select a.ts from sta a right join sta b on a.col1 is not NULL;")
        tdSql.error(f"select a.ts from sta a right join sta b on a.ts + 1 = b.col1;")
        tdSql.error(
            f"select a.ts from sta a right join sta b on timetruncate(a.ts, 1d) > b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a right join sta b on timetruncate(a.ts, 1d) = b.ts + 1;"
        )
        tdSql.error(
            f"select a.ts from sta a right join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1;"
        )
        tdSql.error(
            f"select a.ts from sta a right join sta b on timetruncate(a.ts, 1d) = b.ts window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select a.ts from sta a right join sta b on timetruncate(a.ts, 1d) = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a right join sta b on a.col1 = b.col1 where a.col1 = b.col1;"
        )
        tdSql.error(
            f"select a.ts from sta a right join sta b on a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.error(f"select a.ts from sta a right join sta b where a.ts = b.ts;")
        tdSql.error(
            f"select b.col1 from sta a right join sta b where a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"select b.col1 from sta a right join sta b on a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"select b.col1 from (select ts from sta) a right join (select ts, col1 from sta) b on a.ts = b.ts order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from (select ts from sta) a right join (select ts, col1 from sta) b right join sta c on a.ts=b.ts and b.ts = c.ts order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from sta a right join sta b right join sta c on a.ts=b.ts and a.ts = b.ts order by a.ts;"
        )
        tdSql.query(
            f"select a.ts from test0.sta a right join testb.stb1 b on a.ts = b.ts;"
        )
        tdSql.query(
            f"(select b.col1 from sta a right join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.query(
            f"(select b.col1 from sta a right join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.error(
            f"(select b.col1 from sta a right join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.error(
            f"(select b.col1 from sta a right join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.query(
            f"(select b.col1 from sta a right join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a right join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a right join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a right join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a right join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a right join sta b on a.ts=b.ts)) c right join ((select a.ts from sta a right join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a right join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right join sta b on a.ts=b.ts)) c right join ((select a.ts from sta a right join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a right join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a right join sta b on a.ts=b.ts) order by 1) c right join ((select a.ts from sta a right join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a right join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a right join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right join sta b on a.ts=b.ts) order by 1) c right join ((select a.ts from sta a right join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )

        # right semi join
        tdSql.error(f"select a.ts from sta a right semi join sta b;")
        tdSql.error(
            f"select a.ts from sta a right semi join sta b on a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a right semi join sta b where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(f"select a.ts from sta a right semi join sta b on a.col1 = b.col1;")
        tdSql.error(f"select a.ts from sta a right semi join sta b on a.col1 like '1';")
        tdSql.error(f"select a.ts from sta a right semi join sta b on a.col1 is null;")
        tdSql.error(
            f"select a.ts from sta a right semi join sta b on a.ts + 1 = b.col1;"
        )
        tdSql.error(
            f"select a.ts from sta a right semi join sta b on timetruncate(a.ts, 1d) > b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a right semi join sta b on timetruncate(a.ts, 1d) = b.ts + 1;"
        )
        tdSql.error(
            f"select a.ts from sta a right semi join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1;"
        )
        tdSql.error(
            f"select a.ts from sta a right semi join sta b on timetruncate(a.ts, 1d) = b.ts window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select a.ts from sta a right semi join sta b on timetruncate(a.ts, 1d) = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a right semi join sta b on a.col1 = b.col1 where a.col1 = b.col1;"
        )
        tdSql.error(
            f"select a.ts from sta a right semi join sta b on a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.error(f"select a.ts from sta a right semi join sta b where a.ts = b.ts;")
        tdSql.error(
            f"select b.col1 from sta a right semi join sta b where a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"select b.col1 from sta a right semi join sta b on a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"select b.col1 from (select ts from sta) a right semi join (select ts, col1 from sta) b on a.ts=b.ts order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from (select ts from sta) a right semi join (select ts, col1 from sta) b right semi join sta c on a.ts=b.ts and b.ts = c.ts order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from sta a right semi join sta b right semi join sta c on a.ts=b.ts and a.ts = b.ts order by a.ts;"
        )
        tdSql.query(
            f"select a.ts from test0.sta a right semi join testb.stb1 b on a.ts = b.ts;"
        )
        tdSql.query(
            f"(select b.col1 from sta a right semi join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right semi join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.query(
            f"(select b.col1 from sta a right semi join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right semi join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.error(
            f"(select b.col1 from sta a right semi join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right semi join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.error(
            f"(select b.col1 from sta a right semi join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right semi join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.query(
            f"(select b.col1 from sta a right semi join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right semi join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a right semi join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right semi join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a right semi join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right semi join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a right semi join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right semi join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a right semi join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a right semi join sta b on a.ts=b.ts)) c right semi join ((select a.ts from sta a right semi join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right semi join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a right semi join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right semi join sta b on a.ts=b.ts)) c right semi join ((select a.ts from sta a right semi join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right semi join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a right semi join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a right semi join sta b on a.ts=b.ts) order by 1) c right semi join ((select a.ts from sta a right semi join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a right semi join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a right semi join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right semi join sta b on a.ts=b.ts) order by 1) c right semi join ((select a.ts from sta a right semi join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right semi join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )

        # right anti join
        tdSql.error(f"select a.ts from sta a right anti join sta b;")
        tdSql.error(
            f"select a.ts from sta a right anti join sta b on a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a right anti join sta b where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(f"select a.ts from sta a right anti join sta b on a.col1 = b.col1;")
        tdSql.error(f"select a.ts from sta a right anti join sta b on a.col1 / 1;")
        tdSql.error(f"select a.ts from sta a right anti join sta b on a.col1 is null;")
        tdSql.error(
            f"select a.ts from sta a right anti join sta b on a.ts + 1 = b.col1;"
        )
        tdSql.error(
            f"select a.ts from sta a right anti join sta b on timetruncate(a.ts, 1d) > b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a right anti join sta b on timetruncate(a.ts, 1d) = b.ts + 1;"
        )
        tdSql.error(
            f"select a.ts from sta a right anti join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1;"
        )
        tdSql.error(
            f"select a.ts from sta a right anti join sta b on timetruncate(a.ts, 1d) = b.ts window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select a.ts from sta a right anti join sta b on timetruncate(a.ts, 1d) = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a right anti join sta b on a.col1 = b.col1 where a.col1 = b.col1;"
        )
        tdSql.error(
            f"select a.ts from sta a right anti join sta b on a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.error(f"select a.ts from sta a right anti join sta b where a.ts = b.ts;")
        tdSql.error(
            f"select b.col1 from sta a right anti join sta b where a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"select b.col1 from sta a right anti join sta b on a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"select b.col1 from (select ts from sta) a right anti join (select ts, col1 from sta) b on a.ts=b.ts order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from (select ts from sta) a right anti join (select ts, col1 from sta) b right anti join sta c on a.ts=b.ts and b.ts = c.ts order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from sta a right anti join sta b right anti join sta c on a.ts=b.ts and a.ts = b.ts order by a.ts;"
        )
        tdSql.query(
            f"select a.ts from test0.sta a right anti join testb.stb1 b on a.ts = b.ts;"
        )
        tdSql.query(
            f"(select b.col1 from sta a right anti join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right anti join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.query(
            f"(select b.col1 from sta a right anti join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right anti join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.error(
            f"(select b.col1 from sta a right anti join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right anti join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.error(
            f"(select b.col1 from sta a right anti join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right anti join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.query(
            f"(select b.col1 from sta a right anti join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right anti join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a right anti join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right anti join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a right anti join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right anti join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a right anti join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right anti join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a right anti join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a right anti join sta b on a.ts=b.ts)) c right anti join ((select a.ts from sta a right anti join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right anti join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a right anti join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right anti join sta b on a.ts=b.ts)) c right anti join ((select a.ts from sta a right anti join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right anti join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a right anti join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a right anti join sta b on a.ts=b.ts) order by 1) c right anti join ((select a.ts from sta a right anti join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a right anti join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a right anti join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right anti join sta b on a.ts=b.ts) order by 1) c right anti join ((select a.ts from sta a right anti join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right anti join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )

        # right asof join
        tdSql.query(f"select a.ts from sta a right asof join sta b;")
        tdSql.error(
            f"select a.ts from sta a right asof join sta b on a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a right asof join sta b on a.ts = b.ts and a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a right asof join sta b on a.ts = b.ts or a.col1 = b.col1;"
        )
        tdSql.query(
            f"select a.ts from sta a right asof join sta b on a.ts = b.ts and a.col1 = b.col1;"
        )
        tdSql.query(
            f"select a.ts from sta a right asof join sta b on a.col1 = b.col1 and a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.col1 from sta a right asof join sta b on a.col1 = b.col1 and a.t1 = b.t1;"
        )
        tdSql.query(f"select a.ts from sta a right asof join sta b where a.ts = b.ts;")
        tdSql.query(
            f"select a.ts from sta a right asof join sta b where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.query(f"select a.ts from sta a right asof join sta b on a.col1 = b.col1;")
        tdSql.error(f"select a.ts from sta a right asof join sta b on a.ts != b.ts;")
        tdSql.error(f"select a.ts from sta a right asof join sta b on a.ts is null;")
        tdSql.error(
            f"select a.ts from sta a right asof join sta b on a.ts + 1 = b.col1;"
        )
        tdSql.query(
            f"select a.ts from sta a right asof join sta b on timetruncate(a.ts, 1d) > b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a right asof join sta b on timetruncate(a.ts, 1d) = b.ts + 1;"
        )
        tdSql.query(
            f"select a.ts from sta a right asof join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1;"
        )
        tdSql.query(
            f"select a.ts from sta a right asof join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 0;"
        )
        tdSql.query(
            f"select a.ts from sta a right asof join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1024;"
        )
        tdSql.error(
            f"select a.ts from sta a right asof join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1025;"
        )
        tdSql.error(
            f"select a.ts from sta a right asof join sta b on timetruncate(a.ts, 1d) = b.ts jlimit -1;"
        )
        tdSql.error(
            f"select a.ts from sta a right asof join sta b on timetruncate(a.ts, 1d) = b.ts window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select a.ts from sta a right asof join sta b on timetruncate(a.ts, 1d) = b.ts;"
        )
        tdSql.query(
            f"select a.ts from sta a right asof join sta b on a.col1 = b.col1 where a.col1 = b.col1;"
        )
        tdSql.query(
            f"select a.ts from sta a right asof join sta b on a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.query(f"select a.ts from sta a right asof join sta b where a.ts = b.ts;")
        tdSql.query(
            f"select b.col1 from sta a right asof join sta b where a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from sta a right asof join sta b on a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from sta a right asof join sta b on a.ts = b.ts and 1 = 2 order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from (select ts from sta) a right asof join (select ts, col1 from sta) b on a.ts=b.ts order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from (select ts from sta) a right asof join (select ts, col1 from sta) b right asof join sta c on a.ts=b.ts and b.ts = c.ts order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from sta a right asof join sta b right asof join sta c on a.ts=b.ts and a.ts = b.ts order by a.ts;"
        )
        tdSql.query(
            f"select a.ts from test0.sta a right asof join testb.stb1 b on a.ts = b.ts;"
        )
        tdSql.query(
            f"(select b.col1 from sta a right asof join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right asof join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.query(
            f"(select b.col1 from sta a right asof join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right asof join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.error(
            f"(select b.col1 from sta a right asof join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right asof join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.error(
            f"(select b.col1 from sta a right asof join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right asof join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.query(
            f"(select b.col1 from sta a right asof join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right asof join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a right asof join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right asof join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a right asof join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right asof join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a right asof join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right asof join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a right asof join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a right asof join sta b on a.ts=b.ts)) c right asof join ((select a.ts from sta a right asof join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right asof join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a right asof join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right asof join sta b on a.ts=b.ts)) c right asof join ((select a.ts from sta a right asof join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right asof join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a right asof join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a right asof join sta b on a.ts=b.ts) order by 1) c right asof join ((select a.ts from sta a right asof join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a right asof join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a right asof join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right asof join sta b on a.ts=b.ts) order by 1) c right asof join ((select a.ts from sta a right asof join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right asof join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )

        # right window join
        tdSql.error(f"select a.ts from sta a right window join sta b;")
        tdSql.error(
            f"select a.ts from sta a right window join sta b on a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b on a.ts = b.ts and a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b on a.ts = b.ts or a.col1 = b.col1;"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b on a.ts = b.ts and a.col1 = b.col1;"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b on a.ts = b.ts and a.col1 = b.col1 window_offset(-1s,1s);"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b on a.col1 = b.col1 and a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b on a.col1 = b.col1 and a.ts = b.ts window_offset(-1s,1s);"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b where a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts from sta a right window join sta b window_offset(-1s,1s) where a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.query(
            f"select a.ts from sta a right window join sta b window_offset(-1s,1s) where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b on a.col1 = b.col1;"
        )
        tdSql.query(
            f"select a.ts from sta a right window join sta b on a.col1 = b.col1 window_offset(-1s,1s);"
        )
        tdSql.query(
            f"select a.ts from sta a right window join sta b on a.t1 = b.t1 window_offset(-1s,1s);"
        )
        tdSql.query(
            f"select a.ts from sta a right window join sta b on a.t1 = b.t1 and a.col1 = b.col1 window_offset(-1s,1s);"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b on a.t1 = b.t1 or a.col1 = b.col1 window_offset(-1s,1s);"
        )
        tdSql.error(f"select a.ts from sta a right window join sta b on a.ts != b.ts;")
        tdSql.error(
            f"select a.ts from sta a right window join sta b on a.ts != b.ts window_offset(-1s,1s);"
        )
        tdSql.error(f"select a.ts from sta a right window join sta b on a.ts is null;")
        tdSql.error(
            f"select a.ts from sta a right window join sta b on a.ts is null window_offset(-1s,1s);"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b on a.ts + 1 = b.col1;"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b on a.ts + 1 = b.col1 window_offset(-1s,1s);"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b on timetruncate(a.ts, 1d) > b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b on timetruncate(a.ts, 1d) > b.ts window_offset(-1s,1s);"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b on timetruncate(a.ts, 1d) = b.ts + 1;"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b on timetruncate(a.ts, 1d) = b.ts + 1 window_offset(-1s,1s);"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1;"
        )
        tdSql.query(
            f"select a.ts from sta a right window join sta b window_offset(-1s,1s) jlimit 1;"
        )
        tdSql.query(
            f"select a.ts from sta a right window join sta b window_offset(-1s,1s) jlimit 0;"
        )
        tdSql.query(
            f"select a.ts from sta a right window join sta b window_offset(-1s,1s) jlimit 1024;"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b window_offset(-1s,1s) jlimit 1025;"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b window_offset(-1s,1s) jlimit -1;"
        )
        tdSql.query(
            f"select a.ts from sta a right window join sta b on a.col1 = b.col1 window_offset(-1s, 1s) jlimit 1;"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b on timetruncate(a.ts, 1d) = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b on a.col1 = b.col1 where a.col1 = b.col1;"
        )
        tdSql.query(
            f"select a.ts from sta a right window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 = b.col1;"
        )
        tdSql.query(
            f"select a.ts from sta a right window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b where a.ts = b.ts;"
        )
        tdSql.error(
            f"select b.col1 from sta a right window join sta b where a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"select b.col1 from sta a right window join sta b window_offset(-1s,1s) where a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from sta a right window join sta b on a.ts = b.ts window_offset(-1s,1s) order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from sta a right window join sta b on a.ts = b.ts and a.col1 + 1 = b.col1 window_offset(-1s,1s) order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from sta a right window join sta b on a.ts = b.ts and 1 = 2 window_offset(-1s,1s) order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from (select ts from sta) a right window join (select ts, col1 from sta) b window_offset(-1s,1s) order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from (select ts from sta) a right window join (select ts, col1 from sta) b right window join sta c window_offset(-1s,1s) order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from sta a right window join sta b right window join sta c window_offset(-1s,1s) order by a.ts;"
        )
        tdSql.error(
            f"select a.ts from test0.sta a right window join testb.stb1 b window_offset(-1s,1s);"
        )
        tdSql.query(
            f"select a.ts from testb.stb1 a right window join testb.stb1 b window_offset(-1s,1s);"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b window_offset(1s,-1s) jlimit a.col1;"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b window_offset(1s,-1s) jlimit 1 + 1;"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b window_offset(1s,1s-1s) jlimit 1;"
        )
        tdSql.query(
            f"select a.ts from sta a right window join sta b window_offset(1s,-1s) jlimit 1;"
        )
        tdSql.query(
            f"select a.ts from sta a right window join sta b window_offset(-1a,1a) jlimit 1;"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b window_offset(-1b,1b) jlimit 1;"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b window_offset(-1b,1s) jlimit 1;"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b window_offset(-1u,1u) jlimit 1;"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b window_offset(-1u,1s) jlimit 1;"
        )
        tdSql.query(
            f"select a.ts from sta a right window join sta b window_offset(-1h,1m) jlimit 1;"
        )
        tdSql.query(
            f"select a.ts from sta a right window join sta b window_offset(-1d,1w) jlimit 1;"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b window_offset(-1n,1n) jlimit 1;"
        )
        tdSql.error(
            f"select a.ts from sta a right window join sta b window_offset(-1y,1y) jlimit 1;"
        )

        tdSql.execute(f"use testb;")
        tdSql.error(
            f"select a.ts from stb1 a right window join stb1 b window_offset(-1b,1b) jlimit 1;"
        )
        tdSql.error(
            f"select a.ts from stb1 a right window join stb1 b window_offset(-1b,1s) jlimit 1;"
        )
        tdSql.query(
            f"select a.ts from stb1 a right window join stb1 b window_offset(-1u,1u) jlimit 1;"
        )
        tdSql.query(
            f"select a.ts from stb1 a right window join stb1 b window_offset(-1s,1s) jlimit 1;"
        )

        tdSql.execute(f"use test0;")
        tdSql.error(
            f"select a.col1 from sta a right window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 = b.col1 group by a.col1;"
        )
        tdSql.error(
            f"select a.col1 from sta a right window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 = b.col1 partition by a.col1;"
        )
        tdSql.error(
            f"select count(a.col1) from sta a right window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 = b.col1 interval(1s);"
        )
        tdSql.error(
            f"select count(a.col1) from sta a right window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 = b.col1 session(a.ts, 1s);"
        )
        tdSql.error(
            f"select count(a.col1) from sta a right window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 = b.col1 state_window(a.col1);"
        )
        tdSql.query(
            f"select count(a.col1) from sta a right window join sta b on a.col1 = b.col1 window_offset(-1s,1s) having(count(a.col1) > 0);"
        )
        tdSql.error(
            f"select count(a.col1) from sta a right window join sta b on a.col1 = b.col1 window_offset(-1s,1s) having(a.col1 > 0);"
        )
        tdSql.query(
            f"select a.col1, b.col1, count(a.col1) from sta a right window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 > 0;"
        )
        tdSql.error(
            f"select a.col1, b.col1, count(a.col1) from sta a right window join sta b window_offset(-1s,1s) where a.col1 > 0;"
        )
        tdSql.error(
            f"select diff(a.col1) from sta a right window join sta b window_offset(-1s,1s);"
        )
        tdSql.error(
            f"select csum(a.col1) from sta a right window join sta b window_offset(-1s,1s);"
        )
        tdSql.query(
            f"select diff(a.col1) from tba1 a right window join tba1 b window_offset(0s,0s);"
        )
        tdSql.query(
            f"select csum(a.col1) from tba1 a right window join tba1 b window_offset(0s,0s);"
        )
        tdSql.error(
            f"select interp(a.col1) from tba1 a right window join tba1 b window_offset(0s,0s) RANGE(now -1d, now) every(1s) fill(null);"
        )
        tdSql.error(
            f"select a.col1, b.col1, count(a.col1) from sta a right window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where count(a.col1) > 0;"
        )
        tdSql.query(
            f"(select b.col1 from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union (select a.col1 from sta a right window join sta b window_offset(-1s,1s) order by a.ts);"
        )
        tdSql.query(
            f"(select b.col1 from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union all (select a.col1 from sta a right window join sta b window_offset(-1s,1s) order by a.ts);"
        )
        tdSql.error(
            f"(select b.col1 from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union (select a.col1 from sta a right window join sta b window_offset(-1s,1s)) order by a.ts;"
        )
        tdSql.error(
            f"(select b.col1 from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union all (select a.col1 from sta a right window join sta b window_offset(-1s,1s)) order by a.ts;"
        )
        tdSql.query(
            f"(select b.col1 from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union (select a.col1 from sta a right window join sta b window_offset(-1s,1s)) order by col1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union all (select a.col1 from sta a right window join sta b window_offset(-1s,1s)) order by col1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union (select a.col1 from sta a right window join sta b window_offset(-1s,1s)) order by 1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union all (select a.col1 from sta a right window join sta b window_offset(-1s,1s)) order by 1;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union (select b.ts from sta a right window join sta b window_offset(-1s,1s))) c right window join ((select a.ts from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union all (select b.ts from sta a right window join sta b window_offset(-1s,1s))) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union all (select b.ts from sta a right window join sta b window_offset(-1s,1s))) c right window join ((select a.ts from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union all (select b.ts from sta a right window join sta b window_offset(-1s,1s))) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union (select b.ts from sta a right window join sta b window_offset(-1s,1s)) order by 1) c right window join ((select a.ts from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union (select b.ts from sta a right window join sta b window_offset(-1s,1s)) order by 1) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union all (select b.ts from sta a right window join sta b window_offset(-1s,1s)) order by 1) c right window join ((select a.ts from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union all (select b.ts from sta a right window join sta b window_offset(-1s,1s)) order by 1) d on c.ts = d.ts;"
        )

        # full join
        tdSql.error(f"select a.ts from sta a full join sta b;")
        tdSql.error(
            f"select a.ts from sta a full join sta b on a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a full join sta b where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(f"select a.ts from sta a full join sta b on a.col1 = b.col1;")
        tdSql.error(f"select a.ts from sta a full join sta b on a.col1 is not NULL;")
        tdSql.error(f"select a.ts from sta a full join sta b on a.ts + 1 = b.col1;")
        tdSql.error(
            f"select a.ts from sta a full join sta b on timetruncate(a.ts, 1d) > b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a full join sta b on timetruncate(a.ts, 1d) = b.ts + 1;"
        )
        tdSql.error(
            f"select a.ts from sta a full join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1;"
        )
        tdSql.error(
            f"select a.ts from sta a full join sta b on timetruncate(a.ts, 1d) = b.ts window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"select a.ts from sta a full join sta b on timetruncate(a.ts, 1d) = b.ts;"
        )
        tdSql.error(
            f"select a.ts from sta a full join sta b on a.col1 = b.col1 where a.col1 = b.col1;"
        )
        tdSql.error(
            f"select a.ts from sta a full join sta b on a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.error(f"select a.ts from sta a full join sta b where a.ts = b.ts;")
        tdSql.error(
            f"select b.col1 from sta a full join sta b where a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"select b.col1 from sta a full join sta b on a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"select b.col1 from (select ts from sta) a full join (select ts, col1 from sta) b on a.ts = b.ts order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from (select ts from sta) a full join (select ts, col1 from sta) b full join sta c on a.ts=b.ts and b.ts = c.ts order by a.ts;"
        )
        tdSql.error(
            f"select b.col1 from sta a full join sta b full join sta c on a.ts=b.ts and a.ts = b.ts order by a.ts;"
        )
        tdSql.query(
            f"select a.ts from test0.sta a full join testb.stb1 b on a.ts = b.ts;"
        )
        tdSql.query(
            f"(select b.col1 from sta a full join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a full join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.query(
            f"(select b.col1 from sta a full join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a full join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.error(
            f"(select b.col1 from sta a full join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a full join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.error(
            f"(select b.col1 from sta a full join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a full join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.query(
            f"(select b.col1 from sta a full join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a full join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a full join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a full join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a full join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a full join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.query(
            f"(select b.col1 from sta a full join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a full join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a full join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a full join sta b on a.ts=b.ts)) c full join ((select a.ts from sta a full join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a full join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a full join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a full join sta b on a.ts=b.ts)) c full join ((select a.ts from sta a full join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a full join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a full join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a full join sta b on a.ts=b.ts) order by 1) c full join ((select a.ts from sta a full join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a full join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"select c.ts from ((select a.ts from sta a full join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a full join sta b on a.ts=b.ts) order by 1) c full join ((select a.ts from sta a full join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a full join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )

    def join_explain(self):
        tdSql.execute(f"use test0;")

        # join type
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a outer join sta b on b.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a semi join sta b on b.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a anti join sta b on b.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a asof join sta b on b.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a window join sta b window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a inner outer join sta b on b.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a inner semi join sta b on b.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a inner anti join sta b on b.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a inner asof join sta b on b.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a inner window join sta b on b.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left inner join sta b on b.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right inner join sta b on b.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a full inner join sta b on b.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a full semi join sta b on b.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a full anti join sta b on b.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a full asof join sta b on b.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a full window join sta b window_offset(-1s, 1s);"
        )

        # inner join
        tdSql.error(f"explain analyze verbose true select a.ts from sta a join sta b;")
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a join sta b on a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a join sta b where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a join sta b on a.col1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a join sta b on a.col1 is null;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a join sta b on a.ts + 1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a join sta b on timetruncate(a.ts, 1d) > b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a join sta b on timetruncate(a.ts, 1d) = b.ts + 1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a join sta b on a.ts = b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a join sta b on timetruncate(a.ts, 1d) = b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a join sta b on timetruncate(a.ts, 1d) = timetruncate(b.ts, 1w);"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a join sta b on timetruncate(a.ts, 1d) = b.ts window_offset(-1s, 1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a join sta b on a.col1 = b.col1 where a.col1=b.col1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a join sta b on a.col1 = b.col1 where a.ts=b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a join sta b where a.ts=b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a ,sta b on a.ts=b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a ,sta b where a.ts=b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a ,sta b where a.t1 = b.t1 and a.ts=b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a ,sta b where a.ts=b.ts and a.col1 + 1 = b.col1;"
        )
        tdSql.query(
            f"explain analyze verbose true select b.col1 from sta a ,sta b where a.ts=b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select b.col1 from sta a join sta b join sta c where a.ts=b.ts and b.ts = c.ts order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select b.col1 from (select ts from sta) a join (select ts, col1 from sta) b join sta c where a.ts=b.ts and b.ts = c.ts order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a join sta b join sta c where a.ts=b.ts and a.ts = b.ts order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from test0.sta a ,testb.stb1 b where a.ts=b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a join sta b where a.ts=b.ts order by a.ts) union (select a.col1 from sta a join sta b where a.ts=b.ts order by a.ts);"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a join sta b where a.ts=b.ts order by a.ts) union all (select a.col1 from sta a join sta b where a.ts=b.ts order by a.ts);"
        )
        tdSql.error(
            f"analyze verbose true (select b.col1 from sta a join sta b where a.ts=b.ts order by a.ts) union (select a.col1 from sta a join sta b where a.ts=b.ts) order by a.ts;"
        )
        tdSql.error(
            f"analyze verbose true (select b.col1 from sta a join sta b where a.ts=b.ts order by a.ts) union all (select a.col1 from sta a join sta b where a.ts=b.ts) order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a join sta b where a.ts=b.ts order by a.ts) union (select a.col1 from sta a join sta b where a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a join sta b where a.ts=b.ts order by a.ts) union all (select a.col1 from sta a join sta b where a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a join sta b where a.ts=b.ts order by a.ts) union (select a.col1 from sta a join sta b where a.ts=b.ts) order by 1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a join sta b where a.ts=b.ts order by a.ts) union all (select a.col1 from sta a join sta b where a.ts=b.ts) order by 1;"
        )
        tdSql.error(
            f"analyze verbose true select c.ts from ((select a.ts from sta a join sta b where a.ts=b.ts order by a.ts) union (select b.ts from sta a join sta b where a.ts=b.ts)) c join ((select a.ts from sta a join sta b where a.ts=b.ts order by a.ts) union all (select b.ts from sta a join sta b where a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"analyze verbose true select c.ts from ((select a.ts from sta a join sta b where a.ts=b.ts order by a.ts) union all (select b.ts from sta a join sta b where a.ts=b.ts)) c join ((select a.ts from sta a join sta b where a.ts=b.ts order by a.ts) union all (select b.ts from sta a join sta b where a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a join sta b where a.ts=b.ts order by a.ts) union (select b.ts from sta a join sta b where a.ts=b.ts) order by 1) c join ((select a.ts from sta a join sta b where a.ts=b.ts order by a.ts) union (select b.ts from sta a join sta b where a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a join sta b where a.ts=b.ts order by a.ts) union all (select b.ts from sta a join sta b where a.ts=b.ts) order by 1) c join ((select a.ts from sta a join sta b where a.ts=b.ts order by a.ts) union all (select b.ts from sta a join sta b where a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )

        # left join
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left join sta b;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left join sta b on a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left join sta b where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left join sta b on a.col1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left join sta b on a.col1 is not NULL;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left join sta b on a.ts + 1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left join sta b on timetruncate(a.ts, 1d) > b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left join sta b on timetruncate(a.ts, 1d) = b.ts + 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left join sta b on timetruncate(a.ts, 1d) = b.ts window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left join sta b on timetruncate(a.ts, 1d) = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left join sta b on a.col1 = b.col1 where a.col1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left join sta b on a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left join sta b where a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a left join sta b where a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select b.col1 from sta a left join sta b on a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select b.col1 from (select ts from sta) a left join (select ts, col1 from sta) b on a.ts = b.ts order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from (select ts from sta) a left join (select ts, col1 from sta) b left join sta c on a.ts=b.ts and b.ts = c.ts order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a left join sta b left join sta c on a.ts=b.ts and a.ts = b.ts order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from test0.sta a left join testb.stb1 b on a.ts = b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.error(
            f"explain analyze verbose true (select b.col1 from sta a left join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true (select b.col1 from sta a left join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a left join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a left join sta b on a.ts=b.ts)) c left join ((select a.ts from sta a left join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a left join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left join sta b on a.ts=b.ts)) c left join ((select a.ts from sta a left join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a left join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a left join sta b on a.ts=b.ts) order by 1) c left join ((select a.ts from sta a left join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a left join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a left join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left join sta b on a.ts=b.ts) order by 1) c left join ((select a.ts from sta a left join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )

        # left semi join
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left semi join sta b;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left semi join sta b on a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left semi join sta b where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left semi join sta b on a.col1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left semi join sta b on a.col1 like '1';"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left semi join sta b on a.col1 is null;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left semi join sta b on a.ts + 1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left semi join sta b on timetruncate(a.ts, 1d) > b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left semi join sta b on timetruncate(a.ts, 1d) = b.ts + 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left semi join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left semi join sta b on timetruncate(a.ts, 1d) = b.ts window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left semi join sta b on timetruncate(a.ts, 1d) = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left semi join sta b on a.col1 = b.col1 where a.col1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left semi join sta b on a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left semi join sta b where a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a left semi join sta b where a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select b.col1 from sta a left semi join sta b on a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select b.col1 from (select ts from sta) a left semi join (select ts, col1 from sta) b on a.ts=b.ts order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from (select ts from sta) a left semi join (select ts, col1 from sta) b left semi join sta c on a.ts=b.ts and b.ts = c.ts order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a left semi join sta b left semi join sta c on a.ts=b.ts and a.ts = b.ts order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from test0.sta a left semi join testb.stb1 b on a.ts = b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left semi join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left semi join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left semi join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left semi join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.error(
            f"explain analyze verbose true (select b.col1 from sta a left semi join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left semi join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true (select b.col1 from sta a left semi join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left semi join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left semi join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left semi join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left semi join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left semi join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left semi join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left semi join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left semi join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left semi join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a left semi join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a left semi join sta b on a.ts=b.ts)) c left semi join ((select a.ts from sta a left semi join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left semi join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a left semi join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left semi join sta b on a.ts=b.ts)) c left semi join ((select a.ts from sta a left semi join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left semi join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a left semi join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a left semi join sta b on a.ts=b.ts) order by 1) c left semi join ((select a.ts from sta a left semi join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a left semi join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a left semi join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left semi join sta b on a.ts=b.ts) order by 1) c left semi join ((select a.ts from sta a left semi join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left semi join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )

        # left anti join
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left anti join sta b;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left anti join sta b on a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left anti join sta b where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left anti join sta b on a.col1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left anti join sta b on a.col1 / 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left anti join sta b on a.col1 is null;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left anti join sta b on a.ts + 1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left anti join sta b on timetruncate(a.ts, 1d) > b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left anti join sta b on timetruncate(a.ts, 1d) = b.ts + 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left anti join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left anti join sta b on timetruncate(a.ts, 1d) = b.ts window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left anti join sta b on timetruncate(a.ts, 1d) = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left anti join sta b on a.col1 = b.col1 where a.col1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left anti join sta b on a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left anti join sta b where a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a left anti join sta b where a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select b.col1 from sta a left anti join sta b on a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select b.col1 from (select ts from sta) a left anti join (select ts, col1 from sta) b on a.ts=b.ts order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from (select ts from sta) a left anti join (select ts, col1 from sta) b left anti join sta c on a.ts=b.ts and b.ts = c.ts order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a left anti join sta b left anti join sta c on a.ts=b.ts and a.ts = b.ts order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from test0.sta a left anti join testb.stb1 b on a.ts = b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left anti join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left anti join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left anti join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left anti join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.error(
            f"explain analyze verbose true (select b.col1 from sta a left anti join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left anti join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true (select b.col1 from sta a left anti join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left anti join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left anti join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left anti join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left anti join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left anti join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left anti join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left anti join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left anti join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left anti join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a left anti join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a left anti join sta b on a.ts=b.ts)) c left anti join ((select a.ts from sta a left anti join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left anti join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a left anti join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left anti join sta b on a.ts=b.ts)) c left anti join ((select a.ts from sta a left anti join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left anti join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a left anti join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a left anti join sta b on a.ts=b.ts) order by 1) c left anti join ((select a.ts from sta a left anti join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a left anti join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a left anti join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left anti join sta b on a.ts=b.ts) order by 1) c left anti join ((select a.ts from sta a left anti join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left anti join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )

        # left asof join
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left asof join sta b;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left asof join sta b on a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left asof join sta b on a.ts = b.ts and a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left asof join sta b on a.ts = b.ts or a.col1 = b.col1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left asof join sta b on a.ts = b.ts and a.col1 = b.col1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left asof join sta b on a.col1 = b.col1 and a.ts = b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.col1 from sta a left asof join sta b on a.col1 = b.col1 and a.t1 = b.t1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left asof join sta b where a.ts = b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left asof join sta b where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left asof join sta b on a.col1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left asof join sta b on a.ts != b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left asof join sta b on a.ts is null;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left asof join sta b on a.ts + 1 = b.col1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left asof join sta b on timetruncate(a.ts, 1d) > b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left asof join sta b on timetruncate(a.ts, 1d) = b.ts + 1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left asof join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left asof join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 0;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left asof join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1024;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left asof join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1025;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left asof join sta b on timetruncate(a.ts, 1d) = b.ts jlimit -1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left asof join sta b on timetruncate(a.ts, 1d) = b.ts window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left asof join sta b on timetruncate(a.ts, 1d) = b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left asof join sta b on a.col1 = b.col1 where a.col1 = b.col1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left asof join sta b on a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left asof join sta b where a.ts = b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select b.col1 from sta a left asof join sta b where a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a left asof join sta b on a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a left asof join sta b on a.ts = b.ts and 1 = 2 order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from (select ts from sta) a left asof join (select ts, col1 from sta) b on a.ts=b.ts order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from (select ts from sta) a left asof join (select ts, col1 from sta) b left asof join sta c on a.ts=b.ts and b.ts = c.ts order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a left asof join sta b left asof join sta c on a.ts=b.ts and a.ts = b.ts order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from test0.sta a left asof join testb.stb1 b on a.ts = b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left asof join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left asof join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left asof join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left asof join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.error(
            f"explain analyze verbose true (select b.col1 from sta a left asof join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left asof join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true (select b.col1 from sta a left asof join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left asof join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left asof join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left asof join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left asof join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left asof join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left asof join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a left asof join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left asof join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a left asof join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a left asof join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a left asof join sta b on a.ts=b.ts)) c left asof join ((select a.ts from sta a left asof join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left asof join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a left asof join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left asof join sta b on a.ts=b.ts)) c left asof join ((select a.ts from sta a left asof join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left asof join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a left asof join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a left asof join sta b on a.ts=b.ts) order by 1) c left asof join ((select a.ts from sta a left asof join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a left asof join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a left asof join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left asof join sta b on a.ts=b.ts) order by 1) c left asof join ((select a.ts from sta a left asof join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a left asof join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )

        # left window join
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on a.ts = b.ts and a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on a.ts = b.ts or a.col1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on a.ts = b.ts and a.col1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on a.ts = b.ts and a.col1 = b.col1 window_offset(-1s,1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on a.col1 = b.col1 and a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on a.col1 = b.col1 and a.ts = b.ts window_offset(-1s,1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b where a.ts = b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left window join sta b window_offset(-1s,1s) where a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left window join sta b window_offset(-1s,1s) where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on a.col1 = b.col1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s);"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on a.t1 = b.t1 window_offset(-1s,1s);"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on a.t1 = b.t1 and a.col1 = b.col1 window_offset(-1s,1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on a.t1 = b.t1 or a.col1 = b.col1 window_offset(-1s,1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on a.ts != b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on a.ts != b.ts window_offset(-1s,1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on a.ts is null;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on a.ts is null window_offset(-1s,1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on a.ts + 1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on a.ts + 1 = b.col1 window_offset(-1s,1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on timetruncate(a.ts, 1d) > b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on timetruncate(a.ts, 1d) > b.ts window_offset(-1s,1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on timetruncate(a.ts, 1d) = b.ts + 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on timetruncate(a.ts, 1d) = b.ts + 1 window_offset(-1s,1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left window join sta b window_offset(-1s,1s) jlimit 1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left window join sta b window_offset(-1s,1s) jlimit 0;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left window join sta b window_offset(-1s,1s) jlimit 1024;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b window_offset(-1s,1s) jlimit 1025;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b window_offset(-1s,1s) jlimit -1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s, 1s) jlimit 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on timetruncate(a.ts, 1d) = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on a.col1 = b.col1 where a.col1 = b.col1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 = b.col1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b where a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a left window join sta b where a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select b.col1 from sta a left window join sta b window_offset(-1s,1s) where a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a left window join sta b on a.ts = b.ts window_offset(-1s,1s) order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a left window join sta b on a.ts = b.ts and a.col1 + 1 = b.col1 window_offset(-1s,1s) order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a left window join sta b on a.ts = b.ts and 1 = 2 window_offset(-1s,1s) order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from (select ts from sta) a left window join (select ts, col1 from sta) b window_offset(-1s,1s) order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from (select ts from sta) a left window join (select ts, col1 from sta) b left window join sta c window_offset(-1s,1s) order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a left window join sta b left window join sta c window_offset(-1s,1s) order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from test0.sta a left window join testb.stb1 b window_offset(-1s,1s);"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from testb.stb1 a left window join testb.stb1 b window_offset(-1s,1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b window_offset(1s,-1s) jlimit a.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b window_offset(1s,-1s) jlimit 1 + 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b window_offset(1s,1s-1s) jlimit 1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left window join sta b window_offset(1s,-1s) jlimit 1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left window join sta b window_offset(-1a,1a) jlimit 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b window_offset(-1b,1b) jlimit 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b window_offset(-1b,1s) jlimit 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b window_offset(-1u,1u) jlimit 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b window_offset(-1u,1s) jlimit 1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left window join sta b window_offset(-1h,1m) jlimit 1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a left window join sta b window_offset(-1d,1w) jlimit 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b window_offset(-1n,1n) jlimit 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a left window join sta b window_offset(-1y,1y) jlimit 1;"
        )

        tdSql.execute(f"use testb;")
        tdSql.error(
            f"explain analyze verbose true select a.ts from stb1 a left window join stb1 b window_offset(-1b,1b) jlimit 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from stb1 a left window join stb1 b window_offset(-1b,1s) jlimit 1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from stb1 a left window join stb1 b window_offset(-1u,1u) jlimit 1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from stb1 a left window join stb1 b window_offset(-1s,1s) jlimit 1;"
        )

        tdSql.execute(f"use test0;")
        tdSql.error(
            f"explain analyze verbose true select a.col1 from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 = b.col1 group by a.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.col1 from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 = b.col1 partition by a.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select count(a.col1) from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 = b.col1 interval(1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select count(a.col1) from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 = b.col1 session(a.ts, 1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select count(a.col1) from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 = b.col1 state_window(a.col1);"
        )
        tdSql.query(
            f"explain analyze verbose true select count(a.col1) from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s) having(count(a.col1) > 0);"
        )
        tdSql.error(
            f"explain analyze verbose true select count(a.col1) from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s) having(a.col1 > 0);"
        )
        tdSql.query(
            f"explain analyze verbose true select a.col1, b.col1, count(a.col1) from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 > 0;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.col1, b.col1, count(a.col1) from sta a left window join sta b window_offset(-1s,1s) where a.col1 > 0;"
        )
        tdSql.error(
            f"explain analyze verbose true select diff(a.col1) from sta a left window join sta b window_offset(-1s,1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select csum(a.col1) from sta a left window join sta b window_offset(-1s,1s);"
        )
        tdSql.query(
            f"explain analyze verbose true select diff(a.col1) from tba1 a left window join tba1 b window_offset(0s,0s);"
        )
        tdSql.query(
            f"explain analyze verbose true select csum(a.col1) from tba1 a left window join tba1 b window_offset(0s,0s);"
        )
        tdSql.error(
            f"explain analyze verbose true select interp(a.col1) from tba1 a left window join tba1 b window_offset(0s,0s) RANGE(now -1d, now) every(1s) fill(null);"
        )
        tdSql.error(
            f"explain analyze verbose true select a.col1, b.col1, count(a.col1) from sta a left window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where count(a.col1) > 0;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union (select a.col1 from sta a left window join sta b window_offset(-1s,1s) order by a.ts);"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union all (select a.col1 from sta a left window join sta b window_offset(-1s,1s) order by a.ts);"
        )
        tdSql.error(
            f"explain analyze verbose true (select b.col1 from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union (select a.col1 from sta a left window join sta b window_offset(-1s,1s)) order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true (select b.col1 from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union all (select a.col1 from sta a left window join sta b window_offset(-1s,1s)) order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union (select a.col1 from sta a left window join sta b window_offset(-1s,1s)) order by col1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union all (select a.col1 from sta a left window join sta b window_offset(-1s,1s)) order by col1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union (select a.col1 from sta a left window join sta b window_offset(-1s,1s)) order by 1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union all (select a.col1 from sta a left window join sta b window_offset(-1s,1s)) order by 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union (select b.ts from sta a left window join sta b window_offset(-1s,1s))) c left window join ((select a.ts from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union all (select b.ts from sta a left window join sta b window_offset(-1s,1s))) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union all (select b.ts from sta a left window join sta b window_offset(-1s,1s))) c left window join ((select a.ts from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union all (select b.ts from sta a left window join sta b window_offset(-1s,1s))) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union (select b.ts from sta a left window join sta b window_offset(-1s,1s)) order by 1) c left window join ((select a.ts from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union (select b.ts from sta a left window join sta b window_offset(-1s,1s)) order by 1) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union all (select b.ts from sta a left window join sta b window_offset(-1s,1s)) order by 1) c left window join ((select a.ts from sta a left window join sta b window_offset(-1s,1s) order by a.ts) union all (select b.ts from sta a left window join sta b window_offset(-1s,1s)) order by 1) d on c.ts = d.ts;"
        )

        # right join
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right join sta b;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right join sta b on a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right join sta b where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right join sta b on a.col1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right join sta b on a.col1 is not NULL;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right join sta b on a.ts + 1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right join sta b on timetruncate(a.ts, 1d) > b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right join sta b on timetruncate(a.ts, 1d) = b.ts + 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right join sta b on timetruncate(a.ts, 1d) = b.ts window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right join sta b on timetruncate(a.ts, 1d) = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right join sta b on a.col1 = b.col1 where a.col1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right join sta b on a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right join sta b where a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a right join sta b where a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select b.col1 from sta a right join sta b on a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select b.col1 from (select ts from sta) a right join (select ts, col1 from sta) b on a.ts = b.ts order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from (select ts from sta) a right join (select ts, col1 from sta) b right join sta c on a.ts=b.ts and b.ts = c.ts order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a right join sta b right join sta c on a.ts=b.ts and a.ts = b.ts order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from test0.sta a right join testb.stb1 b on a.ts = b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.error(
            f"explain analyze verbose true (select b.col1 from sta a right join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true (select b.col1 from sta a right join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a right join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a right join sta b on a.ts=b.ts)) c right join ((select a.ts from sta a right join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a right join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right join sta b on a.ts=b.ts)) c right join ((select a.ts from sta a right join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a right join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a right join sta b on a.ts=b.ts) order by 1) c right join ((select a.ts from sta a right join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a right join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a right join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right join sta b on a.ts=b.ts) order by 1) c right join ((select a.ts from sta a right join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )

        # right semi join
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right semi join sta b;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right semi join sta b on a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right semi join sta b where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right semi join sta b on a.col1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right semi join sta b on a.col1 like '1';"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right semi join sta b on a.col1 is null;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right semi join sta b on a.ts + 1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right semi join sta b on timetruncate(a.ts, 1d) > b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right semi join sta b on timetruncate(a.ts, 1d) = b.ts + 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right semi join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right semi join sta b on timetruncate(a.ts, 1d) = b.ts window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right semi join sta b on timetruncate(a.ts, 1d) = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right semi join sta b on a.col1 = b.col1 where a.col1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right semi join sta b on a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right semi join sta b where a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a right semi join sta b where a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select b.col1 from sta a right semi join sta b on a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select b.col1 from (select ts from sta) a right semi join (select ts, col1 from sta) b on a.ts=b.ts order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from (select ts from sta) a right semi join (select ts, col1 from sta) b right semi join sta c on a.ts=b.ts and b.ts = c.ts order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a right semi join sta b right semi join sta c on a.ts=b.ts and a.ts = b.ts order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from test0.sta a right semi join testb.stb1 b on a.ts = b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right semi join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right semi join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right semi join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right semi join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.error(
            f"explain analyze verbose true (select b.col1 from sta a right semi join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right semi join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true (select b.col1 from sta a right semi join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right semi join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right semi join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right semi join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right semi join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right semi join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right semi join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right semi join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right semi join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right semi join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a right semi join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a right semi join sta b on a.ts=b.ts)) c right semi join ((select a.ts from sta a right semi join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right semi join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a right semi join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right semi join sta b on a.ts=b.ts)) c right semi join ((select a.ts from sta a right semi join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right semi join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a right semi join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a right semi join sta b on a.ts=b.ts) order by 1) c right semi join ((select a.ts from sta a right semi join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a right semi join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a right semi join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right semi join sta b on a.ts=b.ts) order by 1) c right semi join ((select a.ts from sta a right semi join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right semi join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )

        # right anti join
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right anti join sta b;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right anti join sta b on a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right anti join sta b where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right anti join sta b on a.col1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right anti join sta b on a.col1 / 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right anti join sta b on a.col1 is null;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right anti join sta b on a.ts + 1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right anti join sta b on timetruncate(a.ts, 1d) > b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right anti join sta b on timetruncate(a.ts, 1d) = b.ts + 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right anti join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right anti join sta b on timetruncate(a.ts, 1d) = b.ts window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right anti join sta b on timetruncate(a.ts, 1d) = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right anti join sta b on a.col1 = b.col1 where a.col1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right anti join sta b on a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right anti join sta b where a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a right anti join sta b where a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select b.col1 from sta a right anti join sta b on a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select b.col1 from (select ts from sta) a right anti join (select ts, col1 from sta) b on a.ts=b.ts order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from (select ts from sta) a right anti join (select ts, col1 from sta) b right anti join sta c on a.ts=b.ts and b.ts = c.ts order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a right anti join sta b right anti join sta c on a.ts=b.ts and a.ts = b.ts order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from test0.sta a right anti join testb.stb1 b on a.ts = b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right anti join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right anti join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right anti join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right anti join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.error(
            f"explain analyze verbose true (select b.col1 from sta a right anti join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right anti join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true (select b.col1 from sta a right anti join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right anti join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right anti join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right anti join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right anti join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right anti join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right anti join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right anti join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right anti join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right anti join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a right anti join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a right anti join sta b on a.ts=b.ts)) c right anti join ((select a.ts from sta a right anti join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right anti join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a right anti join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right anti join sta b on a.ts=b.ts)) c right anti join ((select a.ts from sta a right anti join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right anti join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a right anti join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a right anti join sta b on a.ts=b.ts) order by 1) c right anti join ((select a.ts from sta a right anti join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a right anti join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a right anti join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right anti join sta b on a.ts=b.ts) order by 1) c right anti join ((select a.ts from sta a right anti join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right anti join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )

        # right asof join
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right asof join sta b;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right asof join sta b on a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right asof join sta b on a.ts = b.ts and a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right asof join sta b on a.ts = b.ts or a.col1 = b.col1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right asof join sta b on a.ts = b.ts and a.col1 = b.col1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right asof join sta b on a.col1 = b.col1 and a.ts = b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.col1 from sta a right asof join sta b on a.col1 = b.col1 and a.t1 = b.t1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right asof join sta b where a.ts = b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right asof join sta b where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right asof join sta b on a.col1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right asof join sta b on a.ts != b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right asof join sta b on a.ts is null;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right asof join sta b on a.ts + 1 = b.col1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right asof join sta b on timetruncate(a.ts, 1d) > b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right asof join sta b on timetruncate(a.ts, 1d) = b.ts + 1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right asof join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right asof join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 0;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right asof join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1024;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right asof join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1025;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right asof join sta b on timetruncate(a.ts, 1d) = b.ts jlimit -1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right asof join sta b on timetruncate(a.ts, 1d) = b.ts window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right asof join sta b on timetruncate(a.ts, 1d) = b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right asof join sta b on a.col1 = b.col1 where a.col1 = b.col1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right asof join sta b on a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right asof join sta b where a.ts = b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select b.col1 from sta a right asof join sta b where a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a right asof join sta b on a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a right asof join sta b on a.ts = b.ts and 1 = 2 order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from (select ts from sta) a right asof join (select ts, col1 from sta) b on a.ts=b.ts order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from (select ts from sta) a right asof join (select ts, col1 from sta) b right asof join sta c on a.ts=b.ts and b.ts = c.ts order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a right asof join sta b right asof join sta c on a.ts=b.ts and a.ts = b.ts order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from test0.sta a right asof join testb.stb1 b on a.ts = b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right asof join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right asof join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right asof join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right asof join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.error(
            f"explain analyze verbose true (select b.col1 from sta a right asof join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right asof join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true (select b.col1 from sta a right asof join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right asof join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right asof join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right asof join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right asof join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right asof join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right asof join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a right asof join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right asof join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a right asof join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a right asof join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a right asof join sta b on a.ts=b.ts)) c right asof join ((select a.ts from sta a right asof join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right asof join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a right asof join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right asof join sta b on a.ts=b.ts)) c right asof join ((select a.ts from sta a right asof join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right asof join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a right asof join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a right asof join sta b on a.ts=b.ts) order by 1) c right asof join ((select a.ts from sta a right asof join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a right asof join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a right asof join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right asof join sta b on a.ts=b.ts) order by 1) c right asof join ((select a.ts from sta a right asof join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a right asof join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )

        # right window join
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on a.ts = b.ts and a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on a.ts = b.ts or a.col1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on a.ts = b.ts and a.col1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on a.ts = b.ts and a.col1 = b.col1 window_offset(-1s,1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on a.col1 = b.col1 and a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on a.col1 = b.col1 and a.ts = b.ts window_offset(-1s,1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b where a.ts = b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right window join sta b window_offset(-1s,1s) where a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right window join sta b window_offset(-1s,1s) where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on a.col1 = b.col1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on a.col1 = b.col1 window_offset(-1s,1s);"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on a.t1 = b.t1 window_offset(-1s,1s);"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on a.t1 = b.t1 and a.col1 = b.col1 window_offset(-1s,1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on a.t1 = b.t1 or a.col1 = b.col1 window_offset(-1s,1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on a.ts != b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on a.ts != b.ts window_offset(-1s,1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on a.ts is null;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on a.ts is null window_offset(-1s,1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on a.ts + 1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on a.ts + 1 = b.col1 window_offset(-1s,1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on timetruncate(a.ts, 1d) > b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on timetruncate(a.ts, 1d) > b.ts window_offset(-1s,1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on timetruncate(a.ts, 1d) = b.ts + 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on timetruncate(a.ts, 1d) = b.ts + 1 window_offset(-1s,1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right window join sta b window_offset(-1s,1s) jlimit 1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right window join sta b window_offset(-1s,1s) jlimit 0;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right window join sta b window_offset(-1s,1s) jlimit 1024;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b window_offset(-1s,1s) jlimit 1025;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b window_offset(-1s,1s) jlimit -1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on a.col1 = b.col1 window_offset(-1s, 1s) jlimit 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on timetruncate(a.ts, 1d) = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on a.col1 = b.col1 where a.col1 = b.col1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 = b.col1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b where a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a right window join sta b where a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select b.col1 from sta a right window join sta b window_offset(-1s,1s) where a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a right window join sta b on a.ts = b.ts window_offset(-1s,1s) order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a right window join sta b on a.ts = b.ts and a.col1 + 1 = b.col1 window_offset(-1s,1s) order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a right window join sta b on a.ts = b.ts and 1 = 2 window_offset(-1s,1s) order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from (select ts from sta) a right window join (select ts, col1 from sta) b window_offset(-1s,1s) order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from (select ts from sta) a right window join (select ts, col1 from sta) b right window join sta c window_offset(-1s,1s) order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a right window join sta b right window join sta c window_offset(-1s,1s) order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from test0.sta a right window join testb.stb1 b window_offset(-1s,1s);"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from testb.stb1 a right window join testb.stb1 b window_offset(-1s,1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b window_offset(1s,-1s) jlimit a.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b window_offset(1s,-1s) jlimit 1 + 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b window_offset(1s,1s-1s) jlimit 1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right window join sta b window_offset(1s,-1s) jlimit 1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right window join sta b window_offset(-1a,1a) jlimit 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b window_offset(-1b,1b) jlimit 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b window_offset(-1b,1s) jlimit 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b window_offset(-1u,1u) jlimit 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b window_offset(-1u,1s) jlimit 1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right window join sta b window_offset(-1h,1m) jlimit 1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a right window join sta b window_offset(-1d,1w) jlimit 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b window_offset(-1n,1n) jlimit 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a right window join sta b window_offset(-1y,1y) jlimit 1;"
        )

        tdSql.execute(f"use testb;")
        tdSql.error(
            f"explain analyze verbose true select a.ts from stb1 a right window join stb1 b window_offset(-1b,1b) jlimit 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from stb1 a right window join stb1 b window_offset(-1b,1s) jlimit 1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from stb1 a right window join stb1 b window_offset(-1u,1u) jlimit 1;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from stb1 a right window join stb1 b window_offset(-1s,1s) jlimit 1;"
        )

        tdSql.execute(f"use test0;")
        tdSql.error(
            f"explain analyze verbose true select a.col1 from sta a right window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 = b.col1 group by a.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.col1 from sta a right window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 = b.col1 partition by a.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select count(a.col1) from sta a right window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 = b.col1 interval(1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select count(a.col1) from sta a right window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 = b.col1 session(a.ts, 1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select count(a.col1) from sta a right window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 = b.col1 state_window(a.col1);"
        )
        tdSql.query(
            f"explain analyze verbose true select count(a.col1) from sta a right window join sta b on a.col1 = b.col1 window_offset(-1s,1s) having(count(a.col1) > 0);"
        )
        tdSql.error(
            f"explain analyze verbose true select count(a.col1) from sta a right window join sta b on a.col1 = b.col1 window_offset(-1s,1s) having(a.col1 > 0);"
        )
        tdSql.query(
            f"explain analyze verbose true select a.col1, b.col1, count(a.col1) from sta a right window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where a.col1 > 0;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.col1, b.col1, count(a.col1) from sta a right window join sta b window_offset(-1s,1s) where a.col1 > 0;"
        )
        tdSql.error(
            f"explain analyze verbose true select diff(a.col1) from sta a right window join sta b window_offset(-1s,1s);"
        )
        tdSql.error(
            f"explain analyze verbose true select csum(a.col1) from sta a right window join sta b window_offset(-1s,1s);"
        )
        tdSql.query(
            f"explain analyze verbose true select diff(a.col1) from tba1 a right window join tba1 b window_offset(0s,0s);"
        )
        tdSql.query(
            f"explain analyze verbose true select csum(a.col1) from tba1 a right window join tba1 b window_offset(0s,0s);"
        )
        tdSql.error(
            f"explain analyze verbose true select interp(a.col1) from tba1 a right window join tba1 b window_offset(0s,0s) RANGE(now -1d, now) every(1s) fill(null);"
        )
        tdSql.error(
            f"explain analyze verbose true select a.col1, b.col1, count(a.col1) from sta a right window join sta b on a.col1 = b.col1 window_offset(-1s,1s) where count(a.col1) > 0;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union (select a.col1 from sta a right window join sta b window_offset(-1s,1s) order by a.ts);"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union all (select a.col1 from sta a right window join sta b window_offset(-1s,1s) order by a.ts);"
        )
        tdSql.error(
            f"explain analyze verbose true (select b.col1 from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union (select a.col1 from sta a right window join sta b window_offset(-1s,1s)) order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true (select b.col1 from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union all (select a.col1 from sta a right window join sta b window_offset(-1s,1s)) order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union (select a.col1 from sta a right window join sta b window_offset(-1s,1s)) order by col1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union all (select a.col1 from sta a right window join sta b window_offset(-1s,1s)) order by col1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union (select a.col1 from sta a right window join sta b window_offset(-1s,1s)) order by 1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union all (select a.col1 from sta a right window join sta b window_offset(-1s,1s)) order by 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union (select b.ts from sta a right window join sta b window_offset(-1s,1s))) c right window join ((select a.ts from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union all (select b.ts from sta a right window join sta b window_offset(-1s,1s))) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union all (select b.ts from sta a right window join sta b window_offset(-1s,1s))) c right window join ((select a.ts from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union all (select b.ts from sta a right window join sta b window_offset(-1s,1s))) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union (select b.ts from sta a right window join sta b window_offset(-1s,1s)) order by 1) c right window join ((select a.ts from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union (select b.ts from sta a right window join sta b window_offset(-1s,1s)) order by 1) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union all (select b.ts from sta a right window join sta b window_offset(-1s,1s)) order by 1) c right window join ((select a.ts from sta a right window join sta b window_offset(-1s,1s) order by a.ts) union all (select b.ts from sta a right window join sta b window_offset(-1s,1s)) order by 1) d on c.ts = d.ts;"
        )

        # full join
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a full join sta b;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a full join sta b on a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a full join sta b where a.ts = b.ts or a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a full join sta b on a.col1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a full join sta b on a.col1 is not NULL;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a full join sta b on a.ts + 1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a full join sta b on timetruncate(a.ts, 1d) > b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a full join sta b on timetruncate(a.ts, 1d) = b.ts + 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a full join sta b on timetruncate(a.ts, 1d) = b.ts jlimit 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a full join sta b on timetruncate(a.ts, 1d) = b.ts window_offset(-1s, 1s);"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from sta a full join sta b on timetruncate(a.ts, 1d) = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a full join sta b on a.col1 = b.col1 where a.col1 = b.col1;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a full join sta b on a.col1 = b.col1 where a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select a.ts from sta a full join sta b where a.ts = b.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a full join sta b where a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select b.col1 from sta a full join sta b on a.ts = b.ts and a.col1 + 1 = b.col1 order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select b.col1 from (select ts from sta) a full join (select ts, col1 from sta) b on a.ts = b.ts order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from (select ts from sta) a full join (select ts, col1 from sta) b full join sta c on a.ts=b.ts and b.ts = c.ts order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select b.col1 from sta a full join sta b full join sta c on a.ts=b.ts and a.ts = b.ts order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true select a.ts from test0.sta a full join testb.stb1 b on a.ts = b.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a full join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a full join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a full join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a full join sta b on a.ts=b.ts order by a.ts);"
        )
        tdSql.error(
            f"explain analyze verbose true (select b.col1 from sta a full join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a full join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true (select b.col1 from sta a full join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a full join sta b on a.ts=b.ts) order by a.ts;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a full join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a full join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a full join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a full join sta b on a.ts=b.ts) order by col1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a full join sta b on a.ts=b.ts order by a.ts) union (select a.col1 from sta a full join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.query(
            f"explain analyze verbose true (select b.col1 from sta a full join sta b on a.ts=b.ts order by a.ts) union all (select a.col1 from sta a full join sta b on a.ts=b.ts) order by 1;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a full join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a full join sta b on a.ts=b.ts)) c full join ((select a.ts from sta a full join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a full join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a full join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a full join sta b on a.ts=b.ts)) c full join ((select a.ts from sta a full join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a full join sta b on a.ts=b.ts)) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a full join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a full join sta b on a.ts=b.ts) order by 1) c full join ((select a.ts from sta a full join sta b on a.ts=b.ts order by a.ts) union (select b.ts from sta a full join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )
        tdSql.error(
            f"explain analyze verbose true select c.ts from ((select a.ts from sta a full join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a full join sta b on a.ts=b.ts) order by 1) c full join ((select a.ts from sta a full join sta b on a.ts=b.ts order by a.ts) union all (select b.ts from sta a full join sta b on a.ts=b.ts) order by 1) d on c.ts = d.ts;"
        )

    def join_json(self):
        tdSql.execute(f"use testc;")

        tdSql.query(
            f"select a.ts, a.t->'tag1', b.ts, b.t->'tag1' from stc1 a join stc2 b on a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select a.ts, a.t->'tag1', b.ts, b.t->'tag1' from stc1 a join stc2 b on a.ts = b.ts and a.t->'tag1' = b.t->'tag1';"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select a.ts, a.t->'tag1', b.ts, b.t->'tag1' from stc1 a join stc2 b on a.ts = b.ts and a.t->'tag2' = b.t->'tag2';"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select a.ts, a.t->'tag1', b.ts, b.t->'tag1' from stc1 a left join stc2 b on a.ts = b.ts;"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select a.ts, a.t->'tag1', b.ts, b.t->'tag1' from stc1 a left join stc2 b on a.ts = b.ts and a.t->'tag1' = b.t->'tag1';"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select a.ts, a.t->'tag1', b.ts, b.t->'tag1' from stc1 a left join stc2 b on a.ts = b.ts and a.t->'tag2' = b.t->'tag2';"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select a.ts, a.t->'tag1', b.ts, b.t->'tag1' from stc1 a left semi join stc2 b on a.ts = b.ts;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select a.ts, a.t->'tag1', b.ts, b.t->'tag1' from stc1 a left semi join stc2 b on a.ts = b.ts and a.t->'tag1' = b.t->'tag1';"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select a.ts, a.t->'tag1', b.ts, b.t->'tag1' from stc1 a left semi join stc2 b on a.ts = b.ts and a.t->'tag2' = b.t->'tag2';"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select a.ts, a.t->'tag1', b.ts, b.t->'tag1' from stc1 a left anti join stc2 b on a.ts = b.ts;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select a.ts, a.t->'tag1', b.ts, b.t->'tag1' from stc1 a left anti join stc2 b on a.ts = b.ts and a.t->'tag1' = b.t->'tag1';"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select a.ts, a.t->'tag1', b.ts, b.t->'tag1' from stc1 a left anti join stc2 b on a.ts = b.ts and a.t->'tag2' = b.t->'tag2';"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select a.ts, a.t->'tag1', b.ts, b.t->'tag1' from stc1 a left asof join stc2 b;"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select a.ts, a.t->'tag1', b.ts, b.t->'tag1' from stc1 a left asof join stc2 b on a.t->'tag1' = b.t->'tag1';"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 2, None)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(3, 2, None)

        tdSql.query(
            f"select a.ts, a.t->'tag1', b.ts, b.t->'tag1' from stc1 a left asof join stc2 b on a.t->'tag2' = b.t->'tag2';"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 2, "2023-10-16 09:10:11.000")
        tdSql.checkData(1, 2, "2023-10-16 09:10:12.000")
        tdSql.checkData(2, 2, "2023-10-16 09:10:11.000")
        tdSql.checkData(3, 2, "2023-10-16 09:10:12.000")

        tdSql.query(
            f"select a.ts, a.t->'tag1', b.ts, b.t->'tag1' from stc1 a left window join stc2 b window_offset(0s, 1s);"
        )
        tdSql.checkRows(12)

        tdSql.query(
            f"select a.ts, a.t->'tag1', b.ts, b.t->'tag1' from stc1 a left window join stc2 b on a.t->'tag1' = b.t->'tag1' window_offset(0s, 1s);"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 2, None)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(3, 2, None)

        tdSql.query(
            f"select a.ts, a.t->'tag1', b.ts, b.t->'tag1' from stc1 a left window join stc2 b on a.t->'tag2' = b.t->'tag2' window_offset(0s, 1s);"
        )
        tdSql.checkRows(6)
        tdSql.checkData(0, 2, "2023-10-16 09:10:11.000")
        tdSql.checkData(1, 2, "2023-10-16 09:10:12.000")
        tdSql.checkData(2, 2, "2023-10-16 09:10:12.000")
        tdSql.checkData(3, 2, "2023-10-16 09:10:11.000")
        tdSql.checkData(4, 2, "2023-10-16 09:10:12.000")
        tdSql.checkData(5, 2, "2023-10-16 09:10:12.000")
