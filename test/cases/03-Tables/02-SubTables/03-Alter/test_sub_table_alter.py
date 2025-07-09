from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSubTableAlter:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_sub_table_alter(self):
        """alter sub table

        1. add column
        2. insert data
        3. query data
        4. kill then restart

        Catalog:
            - Table:SubTable:Alter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/alter/cached_schema_after_alter.sim

        """

        db = "csaa_db"
        stb = "csaastb"
        tb1 = "csaatb1"
        tb2 = "csaatb2"

        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== cached_schema_after_alter.sim")

        tdSql.prepare(db, drop=True)
        tdLog.info(f"====== create tables")
        tdSql.execute(f"use {db}")

        tdSql.execute(f"create table {stb} (ts timestamp, c1 int) tags(t1 int)")
        tdSql.execute(f"create table {tb1} using {stb} tags( 1 )")
        tdSql.execute(f"create table {tb2} using {stb} tags( 2 )")

        tdSql.error(f"alter table {tb1} add column c0 int")
        tdSql.execute(f"alter table {stb} add column c2 int")

        tdSql.execute(f"insert into {tb2} values ( {ts0} , 1, 1)")
        tdSql.query(f"select * from {stb}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)

        tdSql.query(f"select * from {tb2}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        tdSql.execute(f"use {db}")
        tdSql.query(f"select * from {stb}")
        tdLog.info(
            f"select * from {stb} ==> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)

        tdSql.query(f"select * from {tb2}")
        tdLog.info(
            f"select * from {tb2} ==> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)

        ts = ts0 + delta
        tdSql.execute(f"insert into {tb2} values ( {ts} , 2, 2)")
        tdSql.query(f"select * from {stb} order by ts asc")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 2)

        tdSql.query(f"select * from {tb2} order by ts asc")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 2)
