from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestBiStarTable:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_bi_star_table(self):
        """Bi mode

        1. In BI mode, querying a supertable with SELECT *, LAST(*), or FIRST(*) will return an additional tbname column
        2. In BI mode, querying only tag columns (without data columns) returns a number of records equal to the number of subtables
        3. In BI mode, backticks can be added to tbname

        Catalog:
            - Query:BiMode

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-20 Simon Guan Migrated from tsim/query/bi_star_table.sim
            - 2025-8-20 Simon Guan Migrated from tsim/query/bi_tag_scan.sim
            - 2025-8-20 Simon Guan Migrated from tsim/query/bi_tbname_col.sim

        """

        self.BiStarTable()
        tdStream.dropAllStreamsAndDbs()
        self.BiTagScan()
        tdStream.dropAllStreamsAndDbs()
        self.BiTbnameCol()
        tdStream.dropAllStreamsAndDbs()

    def BiStarTable(self):
        tdSql.prepare(dbname="db1", vgroups=3)
        tdSql.execute(f"use db1;")
        tdSql.execute(
            f"create stable sta (ts timestamp, f1 int, f2 binary(200)) tags(t1 int, t2 int, t3 int);"
        )
        tdSql.execute(
            f"create stable stb (ts timestamp, f1 int, f2 binary(200)) tags(t1 int, t2 int, t3 int);"
        )
        tdSql.execute(f"create table tba1 using sta tags(1, 1, 1);")
        tdSql.execute(f"create table tba2 using sta tags(2, 2, 2);")
        tdSql.execute(f'insert into tba1 values(now, 1, "1");')
        tdSql.execute(f'insert into tba2 values(now + 1s, 2, "2");')
        tdSql.execute(f"create table tbn1 (ts timestamp, f1 int);")

        tdSql.prepare(dbname="db2", vgroups=3)
        tdSql.execute(f"use db2;")
        tdSql.execute(
            f"create stable sta (ts timestamp, f1 int, f2 binary(200)) tags(t1 int, t2 int, t3 int);"
        )
        tdSql.execute(
            f"create stable stb (ts timestamp, f1 int, f2 binary(200)) tags(t1 int, t2 int, t3 int);"
        )
        tdSql.execute(f"create table tba1 using sta tags(1, 1, 1);")
        tdSql.execute(f"create table tba2 using sta tags(2, 2, 2);")

        # set_bi_mode 1
        tdSql.setConnMode(0, 1)
        tdSql.query(f"select * from db1.sta order by ts;")
        tdSql.checkCols(7)

        tdSql.checkData(0, 6, "tba1")

        tdSql.query(f"select last(*) from db1.sta;")
        tdSql.checkCols(4)

        tdSql.checkData(0, 3, "tba2")

        tdSql.query(f"select last_row(*) from db1.sta;")
        tdSql.checkCols(4)

        tdSql.checkData(0, 3, "tba2")

        tdSql.query(f"select first(*) from db1.sta;")
        tdSql.checkCols(4)

        tdSql.checkData(0, 3, "tba1")

        tdLog.info(f'"=====table star ====================="')

        tdSql.query(f"select b.* from db1.sta b order by ts;")
        tdSql.checkCols(7)

        tdSql.checkData(0, 6, "tba1")

        tdSql.query(f"select last(b.*) from db1.sta b;")
        tdSql.checkCols(4)

        tdSql.checkData(0, 3, "tba2")

        tdSql.query(f"select last_row(b.*) from db1.sta b;")
        tdSql.checkCols(4)

        tdSql.checkData(0, 3, "tba2")

        tdSql.query(f"select first(b.*) from db1.sta b;")
        tdSql.checkCols(4)

        tdSql.checkData(0, 3, "tba1")

        tdSql.query(f"select * from (select f1 from db1.sta);")
        tdSql.checkCols(1)

        # set_bi_mode 0
        tdSql.setConnMode(0, 0)
        tdSql.query(f"select * from db1.sta order by ts;")
        tdSql.checkCols(6)

    def BiTagScan(self):
        tdSql.execute(f"drop database if exists db1;")
        tdSql.execute(f"create database db1 vgroups 3;")
        tdSql.execute(f"use db1;")
        tdSql.execute(
            f"create stable sta (ts timestamp, f1 int, f2 binary(200)) tags(t1 int, t2 int, t3 int);"
        )
        tdSql.execute(
            f"create stable stb (ts timestamp, f1 int, f2 binary(200)) tags(t1 int, t2 int, t3 int);"
        )
        tdSql.execute(f"create table tba1 using sta tags(1, 1, 1);")
        tdSql.execute(f"create table tba2 using sta tags(2, 2, 2);")
        tdSql.execute(
            f'insert into tba1 values(now, 1, "1")(now+3s, 3, "3")(now+5s, 5, "5");'
        )
        tdSql.execute(
            f'insert into tba2 values(now + 1s, 2, "2")(now+2s, 2, "2")(now+4s, 4, "4");'
        )
        tdSql.execute(f"create table tbn1 (ts timestamp, f1 int);")

        # set_bi_mode 1
        tdSql.setConnMode(0, 1)
        tdSql.query(f"select t1,t2,t3 from db1.sta order by t1;")
        tdLog.info(f"{tdSql.getRows()}) {tdSql.getData(0,0)} {tdSql.getData(1,0)}")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 2)

    def BiTbnameCol(self):
        tdSql.execute(f"drop database if exists db1;")
        tdSql.execute(f"create database db1 vgroups 3;")
        tdSql.execute(f"use db1;")
        tdSql.execute(
            f"create stable sta (ts timestamp, f1 int, f2 binary(200)) tags(t1 int, t2 int, t3 int);"
        )
        tdSql.execute(
            f"create stable stb (ts timestamp, f1 int, f2 binary(200)) tags(t1 int, t2 int, t3 int);"
        )
        tdSql.execute(f"create table tba1 using sta tags(1, 1, 1);")
        tdSql.execute(f"create table tba2 using sta tags(2, 2, 2);")
        tdSql.execute(
            f'insert into tba1 values(now, 1, "1")(now+3s, 3, "3")(now+5s, 5, "5");'
        )
        tdSql.execute(
            f'insert into tba2 values(now + 1s, 2, "2")(now+2s, 2, "2")(now+4s, 4, "4");'
        )
        tdSql.execute(f"create table tbn1 (ts timestamp, f1 int);")

        # set_bi_mode 1
        tdSql.setConnMode(0, 1)
        tdSql.query(f"select `tbname`, f1, f2 from sta order by ts")
        tdLog.info(f"{tdSql.getRows()})")
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)}"
        )
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, "tba1")
        tdSql.checkData(1, 0, "tba2")

        tdSql.error(f"create table stc(ts timestamp, `tbname` binary(200));")
        tdSql.error(
            f"create table std(ts timestamp, f1 int) tags(`tbname` binary(200));"
        )
