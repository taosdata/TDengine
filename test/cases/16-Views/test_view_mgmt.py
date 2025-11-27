from new_test_framework.utils import (
    tdLog,
    tdSql,
    sc,
    clusterComCheck,
    cluster,
    sc,
    clusterComCheck,
)


class TestViewMgmt:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_view_mgmt(self):
        """View management

        1. Create 3 super tables
        2. Create child tables and insert data
        3. Create view with root user
        4. Grant /revoke privilege on view to normal users and test
        5. Nested view privilege test
        6. Query view test
        7. show/desc view test
        8. Same name table and view test
        9. Test keepColumnName is 1 and 0
        10. Restart server test
        11. Drop view test


        Catalog:
            - View

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/view/view.sim

        """

        self.prepare_data()
        self.privilege_basic_view()
        self.privilege_nested_view()
        self.create_drop_view()
        self.query_view()
        self.insert_view()
        self.stream_view()
        self.show_desc_view()
        self.same_name_tb_view()

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        tdLog.info(f"================== server restart completed")

        self.privilege_basic_view()
        self.privilege_nested_view()
        self.create_drop_view()
        self.query_view()
        self.insert_view()
        self.stream_view()
        self.show_desc_view()
        self.same_name_tb_view()

        tdSql.execute(f"alter local 'keepColumnName' '1'")
        self.privilege_basic_view()
        self.privilege_nested_view()
        self.create_drop_view()
        self.query_view()
        self.insert_view()
        self.stream_view()
        self.show_desc_view()
        self.same_name_tb_view()

    def prepare_data(self):
        tdSql.prepare("testa", drop=True, vgroups=3)

        tdSql.execute(f"create table sta1(ts timestamp, f int, g int) tags (t int);")
        tdSql.execute(
            f"insert into cta11 using sta1 tags(1) values('2023-10-16 09:10:11', 100111, 1001110);"
        )
        tdSql.execute(
            f"insert into cta12 using sta1 tags(2) values('2023-10-16 09:10:12', 100112, 1001120);"
        )
        tdSql.execute(
            f"insert into cta13 using sta1 tags(3) values('2023-10-16 09:10:13', 100113, 1001130);"
        )
        tdSql.execute(
            f"insert into cta14 using sta1 tags(4) values('2023-10-16 09:10:14', 100114, 1001140);"
        )

        tdSql.execute(f"create table st2(ts timestamp, f int, g int) tags (t int);")
        tdSql.execute(
            f"insert into cta21 using st2 tags(1) values('2023-10-16 09:10:11', 100221, 1002210);"
        )
        tdSql.execute(
            f"insert into cta22 using st2 tags(2) values('2023-10-16 09:10:12', 100222, 1002220);"
        )
        tdSql.execute(
            f"insert into cta23 using st2 tags(3) values('2023-10-16 09:10:13', 100223, 1002230);"
        )
        tdSql.execute(
            f"insert into cta24 using st2 tags(4) values('2023-10-16 09:10:14', 100224, 1002240);"
        )

        tdSql.execute(f"create table stt(ts timestamp, f int, g int) tags (t int);")
        tdSql.execute(f"create table tt using stt tags(99);")

        tdSql.execute(f"create table stv(ts timestamp, h int) tags (t1 int);")
        tdSql.execute(
            f"insert into ctv1 using stv tags(1) values('2023-10-16 10:10:10', 1);"
        )

        tdSql.prepare("testb", drop=True, vgroups=1)

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

        tdSql.execute(
            f"create table st3(dt timestamp, ts timestamp, f int, g int) tags (t int);"
        )
        tdSql.execute(
            f"insert into ctb31 using st3 tags(1) values('2023-10-16 09:10:11', 0, 110221, 1102210);"
        )
        tdSql.execute(
            f"insert into ctb32 using st3 tags(2) values('2023-10-16 09:10:12', 1, 110222, 1102220);"
        )
        tdSql.execute(
            f"insert into ctb33 using st3 tags(3) values('2023-10-16 09:10:13', 2, 110223, 1102230);"
        )
        tdSql.execute(
            f"insert into ctb34 using st3 tags(4) values('2023-10-16 09:10:14', 3, 110224, 1102240);"
        )

    def privilege_basic_view(self):
        tdSql.connect("root")
        tdSql.execute(f"use testa;")

        tdSql.execute(f'create user u1 pass "taosdata"')
        tdSql.execute(f'create user u2 pass "taosdata"')
        tdSql.execute(f'create user u3 pass "taosdata"')

        tdLog.info(f"== root create views ==")
        tdSql.execute(f"create view view1 as select * from sta1;")
        tdSql.execute(f"create view view2 as select * from view1;")
        tdSql.execute(f"create view view3 as select * from view2;")

        tdSql.error(f"grant all on view1 to root;")
        tdSql.error(f"revoke all on view1 from root;")

        tdSql.error(f"grant read on view1 to u1;")
        tdSql.execute(f"grant read on testa.view1 to u1;")

        tdSql.query(
            f"select * from information_schema.ins_user_privileges order by user_name, privilege;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(1, 0, "u1")
        tdSql.checkData(1, 1, "read")

        tdSql.connect("u1")
        tdSql.execute(f"use testa")
        tdSql.error(f"select * from sta1;")
        tdSql.query(f"select * from view1;")
        tdSql.error(f"select * from view2;")
        tdSql.error(f"select * from testb.view1;")
        tdSql.error(f"insert into view1 values (now, 1);")
        tdSql.error(f"create or replace view1 as select * from st2;")
        tdSql.error(f"create viewa as select * from sta1;")
        tdSql.error(f"drop view view1;")
        tdSql.query(f"show views;")
        tdSql.query(f"show create view view1;")
        tdSql.query(f"desc view1;")
        tdSql.query(f"select * from information_schema.ins_views;")
        tdSql.checkRows(3)

        tdSql.error(f"grant read on testa.view1 to u2;")
        tdSql.error(f"revoke read on testa.view1 from u1;")

        tdSql.connect("root")
        tdSql.execute(f"use testa")
        tdSql.execute(f"drop view testa.view1;")
        tdSql.query(
            f"select * from information_schema.ins_user_privileges order by user_name, privilege;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "root")

        tdSql.execute(f"grant all on testa.* to u1;")
        tdSql.execute(f"reset query cache")

        tdLog.info(f"== u1 create view1 ==")
        tdSql.connect("u1")
        tdSql.execute(f"use testa")
        tdSql.query(f"select * from sta1;")
        tdSql.error(f"insert into view1 values (now, 1);")
        tdSql.execute(f"create view view1 as select * from sta1;")

        tdSql.connect("root")
        tdSql.execute(f"grant read on testa.view1 to u2;")
        tdSql.error(f"insert into view1 values (now, 1);")

        tdSql.connect("u2")
        tdSql.execute(f"use testa")
        tdSql.error(f"select * from sta1;")
        tdSql.error(f"insert into view1 values (now, 1);")
        tdSql.query(f"select * from view1;")

        tdSql.connect("root")
        tdSql.execute(f"revoke all on testa.* from u1")
        tdSql.execute(f"reset query cache")

        tdSql.connect("u1")
        tdSql.execute(f"use testa")
        tdSql.error(f"select * from sta1;")
        tdSql.error(f"select * from view1;")

        tdSql.connect("u2")
        tdSql.execute(f"use testa")
        tdSql.error(f"select * from view1;")

        tdSql.connect("root")
        tdSql.execute(f"grant all on testa.* to u2")
        tdSql.execute(f"reset query cache")

        tdSql.connect("u2")
        tdSql.execute(f"use testa")
        tdSql.query(f"select * from view1;")
        tdSql.error(f"create or replace view1 as select * from st2;")

        tdSql.connect("u1")
        tdSql.execute(f"use testa")
        tdSql.error(f"create or replace view1 as select * from st2;")

        tdSql.connect("root")
        tdSql.execute(f"grant all on testa.* to u1")
        tdSql.execute(f"reset query cache")

        tdSql.connect("u1")
        tdSql.execute(f"use testa")
        tdSql.execute(f"create or replace view view1 as select * from st2;")

        tdSql.connect("root")
        tdSql.execute(f"grant alter on testa.view1 to u2")
        tdSql.execute(f"revoke all on testa.* from u1")
        tdSql.execute(f"reset query cache")

        tdLog.info(f"== u2 replace view1 ==")
        tdSql.connect("u2")
        tdSql.execute(f"use testa")
        tdSql.query(f"select * from view1;")
        tdSql.execute(f"create or replace view view1 as select * from sta1;")
        tdSql.error(f"insert into view1 values (now, 1);")
        tdSql.query(
            f"select * from information_schema.ins_views where view_name = 'view1';"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "testa")
        tdSql.checkData(0, 2, "u2")

        tdSql.connect("root")
        tdSql.execute(f"grant all on testa.view1 to u3;")

        tdSql.connect("u3")
        tdSql.execute(f"use testa")
        tdSql.error(f"select * from sta1")
        tdSql.error(f"insert into view1 values (now, 1);")
        tdSql.query(f"select * from view1")

        tdSql.connect("root")
        tdSql.execute(f"revoke all on testa.* from u2")
        tdSql.execute(f"reset query cache")

        tdSql.connect("u3")
        tdSql.execute(f"use testa")
        tdSql.error(f"select * from view1")
        tdSql.error(f"insert into view1 values (now, 1);")

        tdSql.connect("root")
        tdSql.execute(f"grant all on testa.* to u3")
        tdSql.execute(f"drop user u1;")
        tdSql.execute(f"drop user u2;")
        tdSql.execute(f"reset query cache")

        tdSql.connect("u3")
        tdSql.execute(f"use testa")
        tdSql.query(f"select * from view1")
        tdSql.error(f"insert into view1 values (now, 1);")

        tdSql.connect("root")
        tdSql.execute(f"drop user u3;")
        tdSql.execute(f"drop view testa.view1;")
        tdSql.execute(f"drop view testa.view2;")
        tdSql.execute(f"drop view testa.view3;")

    def privilege_nested_view(self):
        tdSql.connect("root")
        tdSql.execute(f"use testa;")

        tdSql.execute(f'create user u1 pass "taosdata"')
        tdSql.execute(f'create user u2 pass "taosdata"')
        tdSql.execute(f'create user u3 pass "taosdata"')

        tdSql.execute(f"grant all on testa.* to u1;")
        tdSql.execute(f"grant all on testb.* to u2;")
        tdSql.execute(f"grant all on testa.stt to u3;")

        tdSql.connect("u1")
        tdSql.execute(f"use testa")
        tdSql.execute(f"create view view1 as select ts, f from st2;")

        tdSql.connect("u2")
        tdSql.execute(f"use testb")
        tdSql.execute(f"create view view1 as select ts, f from st2;")

        tdSql.connect("root")
        tdSql.execute(f"use testa")
        tdSql.error(
            f"create view view2 as select * from view1 union all select * from view2;"
        )
        tdSql.execute(
            f"create view view2 as select * from view1 union all select * from testb.view1;"
        )
        tdSql.execute(f"use testb")
        tdSql.execute(
            f"create view view2 as select a.ts, a.f, b.f from testa.view1 a, view1 b where a.ts=b.ts;"
        )
        tdSql.execute(f"grant all on testa.view2 to u3;")
        tdSql.execute(f"grant all on testb.view2 to u3;")

        tdLog.info(f"== start to query ==")
        tdSql.connect("u3")
        tdSql.execute(f"reset query cache")
        tdSql.query(f"select * from testa.view2 order by f;")
        tdSql.checkRows(8)
        tdSql.checkData(0, 1, 100221)
        tdSql.checkData(1, 1, 100222)
        tdSql.checkData(2, 1, 100223)
        tdSql.checkData(3, 1, 100224)
        tdSql.checkData(4, 1, 110221)
        tdSql.checkData(5, 1, 110222)
        tdSql.checkData(6, 1, 110223)
        tdSql.checkData(7, 1, 110224)

        tdSql.error(f"insert into tt (ts, f) select * from testa.view1;")
        tdSql.error(f"insert into tt (ts, f) select * from testb.view1;")
        tdSql.execute(
            f"insert into testa.tt (ts, f) select * from testa.view2 order by ts, f;"
        )
        # tdSql.checkRows(4)

        tdSql.execute(f"delete from testa.tt;")
        tdSql.error(f"select * from testa.st2;")
        tdSql.error(f"select * from testb.st2;")

        tdSql.connect("root")
        tdSql.execute(f"revoke all on testa.* from u1;")

        tdSql.connect("u3")
        tdSql.execute(f"reset query cache")
        tdSql.error(f"select * from testa.view2;")
        tdSql.error(f"select * from testa.view1;")

        tdSql.connect("root")
        tdSql.execute(f"use testb;")
        tdSql.execute(f"create or replace view testa.view1 as select ts, f from st2;")
        tdSql.query(f"select * from testa.view1 order by ts;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, 100221)
        tdSql.checkData(1, 1, 100222)

        tdSql.connect("u3")
        tdSql.execute(f"reset query cache")
        tdSql.query(f"select * from testa.view2;")
        tdSql.error(f"select * from testa.view1;")

        tdLog.info(f"== drop user and views ==")
        tdSql.connect("root")
        tdSql.execute(f"drop user u1;")
        tdSql.execute(f"drop user u2;")
        tdSql.execute(f"drop user u3;")
        tdSql.execute(f"drop view testa.view1;")
        tdSql.execute(f"drop view testb.view1;")
        tdSql.execute(f"drop view testa.view2;")
        tdSql.execute(f"drop view testb.view2;")

    def create_drop_view(self):
        ## \brief Test create view and drop view functions
        tdSql.connect("root")
        tdSql.execute(f"use testa;")

        tdSql.execute(f"create view view1 as select * from sta1;")
        tdSql.execute(f"drop view view1;")

        tdSql.execute(f"create or replace view view2 as select f from cta11;")
        tdSql.error(f"create view view2 as select * from cta11;")
        tdSql.execute(f"drop view if exists view2;")
        tdSql.execute(f"drop view if exists view3;")
        tdSql.error(f"drop view view2;")
        tdSql.error(f"drop view view3;")

        tdSql.execute(f"create view view3 as select avg(f) from st2;")
        tdSql.execute(f"create or replace view view3 as select f fa from st2;")
        tdSql.error(f"create view view3 as select * from st2;")
        tdSql.execute(f"create view view4 as select * from view3;")
        tdSql.execute(f"create or replace view view4 as select fa from view3;")
        tdSql.execute(f"drop view view3;")
        tdSql.error(f"create view view5 as select * from view3;")
        tdSql.execute(f"drop view view4;")

        tdSql.execute(f"create view testa.view1 as select * from testa.sta1;")
        tdSql.execute(f"create view testa.view2 as select * from testb.st2;")
        tdSql.execute(f"create view testb.view1 as select * from testb.stb1;")
        tdSql.execute(f"create view testb.view2 as select * from testa.st2;")
        tdSql.execute(f"drop view view1;")
        tdSql.execute(f"drop view view2;")
        tdSql.execute(f"drop view testb.view1;")
        tdSql.execute(f"drop view testb.view2;")

        tdSql.error(f"create view view1 as show tables;")
        tdSql.error(f"create view view1 as desc sta1;")
        tdSql.error(f"create view view1 as select * from st;")
        tdSql.error(
            f"create view view1 as select count(*) from sta1 group by f interval(1s);"
        )

        tdSql.execute(f"use information_schema;")
        tdSql.execute(f"create view view1 as select * from ins_tables;")
        tdSql.execute(f"drop view view1;")
        tdSql.execute(
            f"create view information_schema.view1 as select * from ins_tags;"
        )
        tdSql.execute(f"drop view information_schema.view1;")

        tdSql.execute(f"use testa")
        tdSql.error(f"create view testb.view1 as select * from sta1;")
        tdSql.execute(f"create view testb.view1 as select * from stb1;")
        tdSql.error(f"drop view view1")
        tdSql.execute(f"drop view testb.view1")

    def query_view(self):
        tdSql.connect("root")
        tdSql.execute(f"use testa;")

        tdSql.execute(f"create view view1 as select * from sta1;")
        tdSql.query(f"explain select * from view1 order by ts;")
        tdSql.query(f"explain analyze select * from view1 order by ts;")
        tdSql.query(f"select * from view1 order by ts;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, '2023-10-16 09:10:11.000')
        tdSql.checkData(0, 1, 100111)

        tdSql.query(f"select ts from view1 order by ts;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, '2023-10-16 09:10:11.000')

        tdSql.query(f"select view1.ts from view1 order by view1.ts;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, '2023-10-16 09:10:11.000')

        tdSql.execute(f"create or replace view view1 as select 1, 2;")
        tdSql.query(f"explain select * from view1;")
        tdSql.query(f"explain analyze select * from view1;")
        tdSql.query(f"select * from view1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 2)

        tdSql.execute(
            f"create or replace view view1 as select tbname as a, f from sta1;"
        )
        tdSql.query(
            f"explain select cast(avg(f) as int) b  from view1 group by a having avg(f) > 100111 order by b;"
        )
        tdSql.query(
            f"explain analyze select cast(avg(f) as int) b  from view1 group by a having avg(f) > 100111 order by b;"
        )
        tdSql.query(
            f"select cast(avg(f) as int) b  from view1 group by a having avg(f) > 100111 order by b;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 100112)
        tdSql.checkData(1, 0, 100113)
        tdSql.checkData(2, 0, 100114)

        tdSql.execute(
            f"create or replace view view1 as select tbname, avg(f) from sta1 partition by tbname;"
        )
        tdSql.query(f"explain select * from view1 partition by view1.tbname;")
        tdSql.query(f"explain analyze select * from view1 partition by view1.tbname;")
        tdSql.query(f"select * from view1 partition by view1.tbname;")
        tdSql.checkRows(4)

        tdSql.execute(f"create or replace view view1 as select * from sta1;")
        tdSql.execute(
            f"create or replace view testb.view2 as select * from testb.stb1;"
        )
        tdSql.error(
            f"explain select avg(t1.f), avg(t2.f) from view1 t1, view2 t2 where t1.ts = t2.ts and t1.f < 100114;"
        )
        tdSql.error(
            f"explain analyze select avg(t1.f), avg(t2.f) from view1 t1, view2 t2 where t1.ts = t2.ts and t1.f < 100114;"
        )
        tdSql.error(
            f"select avg(t1.f), avg(t2.f) from view1 t1, view2 t2 where t1.ts = t2.ts and t1.f < 100114;"
        )
        tdSql.query(
            f"explain select avg(t1.f), avg(t2.f) from view1 t1, testb.view2 t2 where t1.ts = t2.ts and t1.f < 100114;"
        )
        tdSql.query(
            f"explain analyze select avg(t1.f), avg(t2.f) from view1 t1, testb.view2 t2 where t1.ts = t2.ts and t1.f < 100114;"
        )
        tdSql.query(
            f"select avg(t1.f), avg(t2.f) from view1 t1, testb.view2 t2 where t1.ts = t2.ts and t1.f < 100114;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 100112)
        tdSql.checkData(0, 1, 110112)

        tdSql.execute(
            f"create or replace view view3 as select t1.ts ts, t1.f a1, t2.f a2 from view1 t1, testb.view2 t2 where t1.ts = t2.ts;"
        )
        tdSql.execute(
            f"create or replace view view4 as select t1.ts ts, t1.f a1, t2.f a2 from testa.st2 t1, testb.st2 t2 where t1.ts = t2.ts;"
        )
        tdSql.execute(
            f"create view view5 as select t3.ts, cast((t3.a1 + t4.a1) as bigint), cast((t3.a2 - t4.a2) as bigint) from view3 t3, view4 t4 where t3.ts = t4.ts order by t3.ts;"
        )
        tdSql.query(f"explain select * from view5;")
        tdSql.query(f"explain analyze select * from view5;")
        tdSql.query(f"select * from view5;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, '2023-10-16 09:10:11.000')
        tdSql.checkData(0, 1, 200332)
        tdSql.checkData(0, 2, -110)
        tdSql.checkData(1, 1, 200334)
        tdSql.checkData(2, 1, 200336)
        tdSql.checkData(3, 1, 200338)

        tdSql.execute(f"drop view view1;")
        tdSql.execute(f"drop view testb.view2;")
        tdSql.execute(f"drop view view3;")
        tdSql.execute(f"drop view view4;")
        tdSql.execute(f"drop view view5;")

        tdSql.execute(f"create or replace view view1 as select * from sta1;")
        tdSql.execute(f"create or replace view view2 as select * from st2;")
        tdSql.query(
            f"explain select avg(view1.f), avg(view2.f) from view1, view2 where view1.ts = view2.ts and view1.f < 100114;"
        )
        tdSql.query(
            f"explain analyze select avg(view1.f), avg(view2.f) from view1, view2 where view1.ts = view2.ts and view1.f < 100114;"
        )
        tdSql.query(
            f"select avg(view1.f), avg(view2.f) from view1, view2 where view1.ts = view2.ts and view1.f < 100114;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 100112)
        tdSql.checkData(0, 1, 100222)

        tdSql.execute(f"drop view view1;")
        tdSql.execute(f"drop view view2;")

        tdSql.execute(f"create view view1 as select * from sta1;")
        tdSql.execute(f"create view view2 as select * from st2;")
        tdSql.execute(
            f"create view view3 as select a.ts ts, a.f af, b.f bf from view1 a join view2 b on a.ts = b.ts;"
        )
        tdSql.execute(
            f"create view view3a as select a.ts ts, a.f, b.f from view1 a join view2 b on a.ts = b.ts;"
        )
        tdSql.execute(
            f"create view view4 as select _wstart, avg(bf) - avg(af) as b from view3 interval(1s);"
        )
        tdSql.error(
            f"create view view4a as select _wstart, avg(b.f) - avg(a.f) as b from view3 interval(1s);"
        )
        tdSql.execute(
            f"create view view5 as select count(*),avg(b) from view4 interval(1s) having avg(b) > 0;"
        )
        tdSql.query(f"explain select * from view5;")
        tdSql.query(f"explain analyze select * from view5;")
        tdSql.query(f"select * from view5;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 110)

        tdSql.execute(f"drop view view1;")
        tdSql.execute(f"drop view view2;")
        tdSql.execute(f"drop view view3;")
        tdSql.execute(f"drop view view3a;")
        tdSql.execute(f"drop view view4;")
        tdSql.execute(f"drop view view5;")

        tdSql.execute(f"use information_schema;")
        tdSql.execute(f"create view view1 as select * from ins_views;")
        tdSql.query(f"explain select * from view1;")
        tdSql.query(f"explain analyze select * from view1;")
        tdSql.query(f"select * from view1;")
        tdSql.checkRows(1)

        tdSql.execute(f"drop view view1;")
        tdSql.execute(f"use testa;")
        tdSql.execute(
            f"create view information_schema.view1 as select * from information_schema.ins_views;"
        )
        tdSql.query(f"explain select * from information_schema.view1;")
        tdSql.query(f"explain analyze select * from information_schema.view1;")
        tdSql.query(f"select * from information_schema.view1;")
        tdSql.checkRows(1)

        tdSql.execute(f"drop view information_schema.view1;")

        tdSql.execute(f"use testa;")
        tdSql.execute(f"create view view1 as select * from st2;")
        tdSql.execute(f"use testb;")
        tdSql.query(f"explain select f from testa.view1 order by f;")
        tdSql.query(f"explain analyze select f from testa.view1 order by f;")
        tdSql.query(f"select f from testa.view1 order by f;")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 100221)
        tdSql.checkData(1, 0, 100222)

        tdSql.execute(f"drop view testa.view1;")

        tdSql.execute(f"use performance_schema;")
        tdSql.execute(f"create view view1 as select 1;")
        tdSql.execute(f"create view view2 as select 2;")
        tdSql.execute(f"create view view3 as select server_status();")
        tdSql.execute(
            f"create view view4 as select conn_id from perf_connections where 0>1;"
        )
        tdSql.execute(f"create view view5 as select abs(-1) a;")
        tdSql.execute(
            f"create view view6 as select 1 union select conn_id from perf_connections;"
        )
        tdSql.execute(
            f"create view view7 as select 1 union select conn_id from perf_connections where 0>1;"
        )
        tdSql.execute(
            f"create view view8 as select 1 union all select case when conn_id != 1 then conn_id else conn_id + 1 end from perf_connections;"
        )
        tdSql.query(
            f"explain select * from view1 union all select * from view2 union all select * from view3 union all select * from view4 union all select a from view5 union all select * from view6 union all select * from view7 union all select * from view8;"
        )
        tdSql.query(
            f"explain analyze select * from view1 union all select * from view2 union all select * from view3 union all select * from view4 union all select a from view5 union all select * from view6 union all select * from view7 union all select * from view8;"
        )
        tdSql.query(
            f"select * from view1 union all select * from view2 union all select * from view3 union all select * from view4 union all select a from view5 union all select * from view6 union all select * from view7 union all select * from view8;"
        )
        tdSql.query(f"explain select * from view1 union select a from view5;")
        tdSql.query(f"explain analyze select * from view1 union select a from view5;")
        tdSql.query(f"select * from view1 union select a from view5;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.execute(f"drop view view1;")
        tdSql.execute(f"drop view view2;")
        tdSql.execute(f"drop view view3;")
        tdSql.execute(f"drop view view4;")
        tdSql.execute(f"drop view view5;")
        tdSql.execute(f"drop view view6;")
        tdSql.execute(f"drop view view7;")
        tdSql.execute(f"drop view view8;")

        tdSql.execute(f"use testb;")
        tdSql.execute(
            f"create view viewx1 as select ts, t from (select last(ts) as ts, last(f) as f, t from st3 partition by t order by ts desc);"
        )
        tdSql.execute(
            f"create view viewx2 as select ts, t from (select last(dt) as ts, last(f) as f, t from st3 partition by t order by ts desc);"
        )
        tdSql.execute(
            f"create view viewx3 as select ts1, t from (select last(ts) as ts1, last(f) as f, t from st3 partition by t order by ts1 desc);"
        )
        tdSql.execute(
            f"create view viewx4 as select f, t from (select last(ts) as f, last(g) as g, t from st3 partition by t order by f desc);"
        )
        tdSql.query(f"select * from viewx1;")
        tdSql.checkRows(4)

        tdSql.query(f"select * from viewx2;")
        tdSql.checkRows(4)

        tdSql.query(f"select * from viewx3;")
        tdSql.checkRows(4)

        tdSql.query(f"select * from viewx4;")
        tdSql.checkRows(4)

        tdSql.execute(f"drop view viewx1;")
        tdSql.execute(f"drop view viewx2;")
        tdSql.execute(f"drop view viewx3;")
        tdSql.execute(f"drop view viewx4;")

    def insert_view(self):
        tdSql.connect("root")
        tdSql.execute(f"use testa;")

        tdSql.execute(f"create view view1 as select * from cta11;")
        tdSql.error(f"insert into view1 values (now, 1);")
        tdSql.execute(f"create table ctat using sta1 tags(1);")
        tdSql.execute(f"insert into ctat select * from view1;")
        tdSql.execute(f"drop view view1;")
        tdSql.execute(f"drop table ctat;")

        tdSql.execute(f"use information_schema;")
        tdSql.execute(f"create view view1 as select * from ins_dnodes;")
        tdSql.error(f"insert into ins_dnodes select * from view1;")
        tdSql.execute(f"drop view view1;")

    def stream_view(self):
        tdSql.connect("root")
        tdSql.execute(f"use testa;")

        tdSql.execute(f"create view view1 as select * from sta1;")
        tdSql.error(
            f"CREATE STREAM s1 INTO s1t AS SELECT _wstart, count(*) FROM view1 PARTITION BY f INTERVAL(1m);"
        )

        tdSql.execute(f"drop view view1;")

    def show_desc_view(self):
        tdSql.connect("root")
        tdSql.execute(f"use testa;")

        tdSql.execute(f"create view view1 as select * from sta1;")
        tdSql.query(f"show views;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "view1")
        tdSql.checkData(0, 1, "testa")
        tdSql.checkData(0, 2, "root")
        tdSql.checkData(0, 4, "NORMAL")
        tdSql.checkData(0, 5, 'select * from sta1;')
        tdSql.checkData(0, 6, None)
        tdSql.checkData(0, 7, None)
        tdSql.checkData(0, 8, None)

        tdSql.query(f"desc view1")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(0, 2, 8)
        tdSql.checkData(0, 3, 'VIEW COL')
        tdSql.checkData(1, 0, "f")
        tdSql.checkData(2, 0, "g")
        tdSql.checkData(3, 0, "t")

        tdSql.query(f"show create view view1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '`testa`.`view1`')
        tdSql.checkData(
            0, 1, 'CREATE VIEW `testa`.`view1` AS select * from sta1;'
        )

        tdSql.execute(f"create or replace view view2 as select null;")
        tdSql.query(f"desc view2;")
        tdSql.checkRows(1)
        # tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, "VARCHAR")
        tdSql.checkData(0, 2, 0)

        tdSql.execute(f"create or replace view view2 as select null a;")
        tdSql.query(f"desc view2;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "a")
        tdSql.checkData(0, 1, "VARCHAR")
        tdSql.checkData(0, 2, 0)

        tdSql.query(f"show views;")
        tdSql.checkRows(2)

        tdSql.execute(f"create view testb.view1 as select * from stb1;")
        tdSql.query(f"show views;")
        tdSql.checkRows(2)

        tdSql.query(f"show testb.views;")
        tdSql.checkRows(1)

        tdSql.query(f"show create view testb.view1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '`testb`.`view1`')
        tdSql.checkData(
            0, 1, 'CREATE VIEW `testb`.`view1` AS select * from stb1;'
        )

        tdSql.query(f"desc testb.view1;")
        tdSql.checkRows(4)

        tdSql.execute(f"use information_schema;")
        tdSql.execute(f"create view view1 as select * from ins_views;")
        tdSql.query(f"select * from view1;")
        tdSql.checkRows(4)

        tdSql.query(f"select * from view1 where db_name = 'testa';")
        tdSql.checkRows(2)

        tdSql.query(f"select * from view1 where db_name like 'test%';")
        tdSql.checkRows(3)

        tdSql.query(f"select * from view1 where view_name='view1';")
        tdSql.checkRows(3)

        tdSql.query(
            f"select concat(db_name, '.', view_name) from view1 where view_name='view1';"
        )
        tdSql.checkRows(3)

        tdSql.execute(f"drop view testa.view1;")
        tdSql.execute(f"drop view testa.view2;")
        tdSql.execute(f"drop view testb.view1;")
        tdSql.execute(f"drop view information_schema.view1;")

    def same_name_tb_view(self):
        tdSql.connect("root")
        tdSql.execute(f"use testa;")

        tdLog.info(f"== create view sta1")
        tdSql.execute(f"create view sta1 as select * from stv;")
        tdSql.query(f"select * from sta1;")
        tdSql.checkRows(4)

        tdSql.query(f"desc sta1;")
        tdSql.checkRows(4)

        tdSql.query(f"show create table sta1;")
        tdSql.query(f"show create view sta1;")
        tdSql.execute(f"create view view1 as select * from sta1;")
        tdSql.query(f"select * from view1;")
        tdSql.checkRows(4)

        tdLog.info(f"== drop view sta1")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"drop view sta1;")
        tdSql.query(f"select * from sta1;")
        tdSql.checkRows(4)

        tdSql.query(f"desc sta1;")
        tdSql.checkRows(4)

        tdSql.query(f"show create table sta1;")
        tdSql.error(f"show create view sta1;")
        tdSql.query(f"select * from view1;")
        tdSql.checkRows(4)

        tdLog.info(f"== create view sta1, drop table sta1")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"create view sta1 as select * from stv;")
        tdSql.execute(f"drop table sta1;")
        tdSql.query(f"select * from sta1;")
        tdSql.checkRows(1)

        tdSql.query(f"desc sta1;")
        tdSql.checkRows(3)

        tdSql.error(f"show create table sta1;")
        tdSql.query(f"show create view sta1;")
        tdSql.query(f"select * from view1;")
        tdSql.checkRows(1)

        tdSql.query(f"desc view1;")
        tdSql.checkRows(3)

        tdLog.info(f"== restore data")
        tdSql.execute(f"drop view sta1;")
        tdSql.execute(f"drop view view1;")
        tdSql.execute(f"create table sta1(ts timestamp, f int, g int) tags (t int);")
        tdSql.execute(
            f"insert into cta11 using sta1 tags(1) values('2023-10-16 09:10:11', 100111, 1001110);"
        )
        tdSql.execute(
            f"insert into cta12 using sta1 tags(2) values('2023-10-16 09:10:12', 100112, 1001120);"
        )
        tdSql.execute(
            f"insert into cta13 using sta1 tags(3) values('2023-10-16 09:10:13', 100113, 1001130);"
        )
        tdSql.execute(
            f"insert into cta14 using sta1 tags(4) values('2023-10-16 09:10:14', 100114, 1001140);"
        )
