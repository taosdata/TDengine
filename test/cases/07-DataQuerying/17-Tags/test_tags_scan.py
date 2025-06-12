from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestTagScan:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_tag_scan(self):
        """tag scan

        1. -

        Catalog:
            - Query:Tags

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/query/tag_scan.sim

        """

        tdSql.connect("root")
        tdSql.execute(f"drop database if exists test")
        tdSql.execute(f"create database test;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table st(ts timestamp, f int) tags (t int);")
        tdSql.execute(f"insert into ct1 using st tags(1) values(now, 1);")
        tdSql.execute(f"insert into ct2 using st tags(2) values(now, 2);")
        tdSql.execute(f"insert into ct3 using st tags(3) values(now, 3);")
        tdSql.execute(f"insert into ct4 using st tags(4) values(now, 4);")

        tdSql.execute(f"create table st2(ts timestamp, f int) tags (t int);")
        tdSql.execute(f"insert into ct21 using st2 tags(1) values(now, 1);")
        tdSql.execute(f"insert into ct22 using st2 tags(2) values(now, 2);")
        tdSql.execute(f"insert into ct23 using st2 tags(3) values(now, 3);")
        tdSql.execute(f"insert into ct24 using st2 tags(4) values(now, 4);")

        tdSql.query(f"select tbname, 1 from st group by tbname order by tbname;")
        tdLog.info(
            f"{tdSql.getRows()})  {tdSql.getData(0,0)} {tdSql.getData(1,0)} {tdSql.getData(2,0)}"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "ct1")

        tdSql.checkData(1, 0, "ct2")

        tdSql.query(f"select tbname, 1 from st group by tbname slimit 0, 1;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(1)

        tdSql.query(f"select tbname, 1 from st group by tbname slimit 2, 2;")
        tdLog.info(f"{tdSql.getRows()}) {tdSql.getData(0,0)} {tdSql.getData(1,0)}")
        tdSql.checkRows(2)

        tdSql.query(
            f"select tbname, 1 from st group by tbname order by tbname slimit 0, 1;"
        )
        tdLog.info(
            f"{tdSql.getRows()})  {tdSql.getData(0,0)} {tdSql.getData(1,0)} {tdSql.getData(2,0)}"
        )
        tdSql.checkRows(4)

        tdSql.execute(
            f"create table stt1(ts timestamp, f int) tags (t int, b varchar(10));"
        )
        tdSql.execute(f"insert into ctt11 using stt1 tags(1, '1aa') values(now, 1);")
        tdSql.execute(f"insert into ctt12 using stt1 tags(2, '1bb') values(now, 2);")
        tdSql.execute(f"insert into ctt13 using stt1 tags(3, '1cc') values(now, 3);")
        tdSql.execute(f"insert into ctt14 using stt1 tags(4, '1dd') values(now, 4);")
        tdSql.execute(f"insert into ctt14 values(now, 5);")

        tdSql.execute(
            f"create table stt2(ts timestamp, f int) tags (t int, b varchar(10));"
        )
        tdSql.execute(f"insert into ctt21 using stt2 tags(1, '2aa') values(now, 1);")
        tdSql.execute(f"insert into ctt22 using stt2 tags(2, '2bb') values(now, 2);")
        tdSql.execute(f"insert into ctt23 using stt2 tags(3, '2cc') values(now, 3);")
        tdSql.execute(f"insert into ctt24 using stt2 tags(4, '2dd') values(now, 4);")

        tdSql.query(f"select tags t, b from stt1 order by t")
        tdLog.info(f"{tdSql.getRows()})")
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(3,0)} {tdSql.getData(3,1)}"
        )
        tdSql.checkRows(4)

        tdSql.checkData(3, 1, "1dd")

        tdSql.query(f"select tags t, b from stt2 order by t")
        tdLog.info(f"{tdSql.getRows()})")
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(3,0)} {tdSql.getData(3,1)}"
        )
        tdSql.checkRows(4)

        tdSql.checkData(3, 1, "2dd")

        tdSql.query(f"select tags t,b,f from stt1 order by t")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(5)

        tdSql.checkData(4, 2, 5)

        tdSql.query(f"select tags tbname,t,b from stt1 order by t")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(4)

        tdSql.checkData(3, 0, "ctt14")

        tdSql.checkData(3, 2, "1dd")

        tdSql.query(f"select tags t,b from stt1 where t=1")
        tdLog.info(f"{tdSql.getRows()})")
        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(0, 1, "1aa")

        tdSql.query(f"select tags t,b from stt1 where tbname='ctt11'")
        tdLog.info(f"{tdSql.getRows()})")
        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(0, 1, "1aa")

        tdSql.query(f"select tags t,b from ctt11")
        tdLog.info(f"{tdSql.getRows()})")
        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)}")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(0, 1, "1aa")

        tdSql.execute(f"create table stb34 (ts timestamp, f int) tags(t int);")
        tdSql.execute(
            f"insert into ctb34 using stb34 tags(1) values(now, 1)(now+1s, 2);"
        )
        tdSql.query(f"select 1 from (select tags t from stb34 order by t)")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(1)

        tdSql.query(f"select count(*) from (select tags t from stb34)")
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select 1 from (select tags ts from stb34)")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(2)

        tdSql.query(f"select count(*) from (select tags ts from stb34)")
        tdSql.checkData(0, 0, 2)

        tdSql.query(f"select tags 2 from stb34")
        tdSql.checkRows(1)
