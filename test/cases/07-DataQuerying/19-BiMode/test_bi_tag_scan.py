from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestBiTagScan:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_bi_tag_scan(self):
        """Bi Tag Scan

        1.

        Catalog:
            - Query:BiMode

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/query/bi_tag_scan.sim

        """

        tdSql.execute(f"drop database if exists db1;")
        tdSql.execute(f"create database db1 vgroups 3;")
        tdSql.execute(f"create database db1;")
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
        tdSql.setConnMode(1)
        tdSql.query(f"select t1,t2,t3 from db1.sta order by t1;")
        tdLog.info(f"{tdSql.getRows()}) {tdSql.getData(0,0)} {tdSql.getData(1,0)}")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 2)
