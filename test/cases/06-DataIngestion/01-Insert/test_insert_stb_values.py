from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSubtableInsertRows:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_subtable_insert_rows(self):
        """insert sub table with multi values

        1. 待补充

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/stable/values.sim

        """

        tdLog.info(f"======================== dnode1 start")
        tdSql.prepare(dbname="vdb0")
        tdSql.execute(f"create table vdb0.mt (ts timestamp, tbcol int) TAGS(tgcol int)")

        tdSql.execute(f"create table vdb0.vtb00 using vdb0.mt tags( 0 )")
        tdSql.execute(f"create table vdb0.vtb01 using vdb0.mt tags( 0 )")

        tdSql.prepare(dbname="vdb1")
        tdSql.execute(f"create table vdb1.mt (ts timestamp, tbcol int) TAGS(tgcol int)")
        tdSql.error(f"create table vdb1.vtb10 using vdb0.mt tags( 1 )")
        tdSql.error(f"create table vdb1.vtb11 using vdb0.mt tags( 1 )")
        tdSql.execute(f"create table vdb1.vtb10 using vdb1.mt tags( 1 )")
        tdSql.execute(f"create table vdb1.vtb11 using vdb1.mt tags( 1 )")

        tdSql.prepare(dbname="vdb2")
        tdSql.execute(f"create table vdb2.mt (ts timestamp, tbcol int) TAGS(tgcol int)")
        tdSql.error(f"create table vdb2.vtb20 using vdb0.mt tags( 2 )")
        tdSql.error(f"create table vdb2.vtb21 using vdb0.mt tags( 2 )")
        tdSql.execute(f"create table vdb2.vtb20 using vdb2.mt tags( 2 )")
        tdSql.execute(f"create table vdb2.vtb21 using vdb2.mt tags( 2 )")

        tdSql.prepare(dbname="vdb3")
        tdSql.execute(f"create table vdb3.mt (ts timestamp, tbcol int) TAGS(tgcol int)")
        tdSql.error(f"create table vdb3.vtb20 using vdb0.mt tags( 2 )")
        tdSql.error(f"create table vdb3.vtb21 using vdb0.mt tags( 2 )")
        tdSql.execute(f"create table vdb3.vtb30 using vdb3.mt tags( 3 )")
        tdSql.execute(f"create table vdb3.vtb31 using vdb3.mt tags( 3 )")

        tdLog.info(f"=============== step2")
        tdSql.execute(
            f"insert into vdb0.vtb00 values (1519833600000 , 10) (1519833600001, 20) (1519833600002, 30)"
        )
        tdSql.execute(
            f"insert into vdb0.vtb01 values (1519833600000 , 10) (1519833600001, 20) (1519833600002, 30)"
        )
        tdSql.execute(
            f"insert into vdb1.vtb10 values (1519833600000 , 11) (1519833600001, 21) (1519833600002, 31)"
        )
        tdSql.execute(
            f"insert into vdb1.vtb11 values (1519833600000 , 11) (1519833600001, 21) (1519833600002, 31)"
        )
        tdSql.execute(
            f"insert into vdb2.vtb20 values (1519833600000 , 12) (1519833600001, 22) (1519833600002, 32)"
        )
        tdSql.execute(
            f"insert into vdb2.vtb21 values (1519833600000 , 12) (1519833600001, 22) (1519833600002, 32)"
        )
        tdSql.execute(
            f"insert into vdb3.vtb30 values (1519833600000 , 13) (1519833600001, 23) (1519833600002, 33)"
        )
        tdSql.execute(
            f"insert into vdb3.vtb31 values (1519833600000 , 13) (1519833600001, 23) (1519833600002, 33)"
        )
        tdSql.query(f"select * from vdb0.mt")
        tdSql.query(f"select ts from vdb0.mt")

        tdSql.checkRows(6)

        tdLog.info(f"=============== step3")
        tdSql.execute(
            f"insert into vdb0.vtb00 values (1519833600003 , 40) (1519833600005, 50) (1519833600004, 60)"
        )
        tdSql.execute(
            f"insert into vdb0.vtb01 values (1519833600003 , 40) (1519833600005, 50) (1519833600004, 60)"
        )
        tdSql.execute(
            f"insert into vdb1.vtb10 values (1519833600003 , 41) (1519833600005, 51) (1519833600004, 61)"
        )
        tdSql.execute(
            f"insert into vdb1.vtb11 values (1519833600003 , 41) (1519833600005, 51) (1519833600004, 61)"
        )
        tdSql.execute(
            f"insert into vdb2.vtb20 values (1519833600003 , 42) (1519833600005, 52) (1519833600004, 62)"
        )
        tdSql.execute(
            f"insert into vdb2.vtb21 values (1519833600003 , 42) (1519833600005, 52) (1519833600004, 62)"
        )
        tdSql.execute(
            f"insert into vdb3.vtb30 values (1519833600003 , 43) (1519833600005, 53) (1519833600004, 63)"
        )
        tdSql.execute(
            f"insert into vdb3.vtb31 values (1519833600003 , 43) (1519833600005, 53) (1519833600004, 63)"
        )
        tdSql.query(f"select * from vdb0.mt")
        tdSql.query(f"select ts from vdb0.mt")

        tdSql.checkRows(12)

        tdLog.info(f"=============== step4")
        tdSql.execute(
            f"insert into vdb0.vtb00 values(1519833600006, 60) (1519833600007, 70) vdb0.vtb01 values(1519833600006, 60) (1519833600007, 70)"
        )
        tdSql.execute(
            f"insert into vdb1.vtb10 values(1519833600006, 61) (1519833600007, 71) vdb1.vtb11 values(1519833600006, 61) (1519833600007, 71)"
        )
        tdSql.execute(
            f"insert into vdb2.vtb20 values(1519833600006, 62) (1519833600007, 72) vdb2.vtb21 values(1519833600006, 62) (1519833600007, 72)"
        )
        tdSql.execute(
            f"insert into vdb3.vtb30 values(1519833600006, 63) (1519833600007, 73) vdb3.vtb31 values(1519833600006, 63) (1519833600007, 73)"
        )
        tdSql.query(f"select * from vdb0.mt")
        tdSql.query(f"select ts from vdb0.mt")

        tdSql.checkRows(16)

        tdLog.info(f"=============== step5")
        tdSql.execute(
            f"insert into vdb0.vtb00 values(1519833600008, 80) (1519833600007, 70) vdb0.vtb01 values(1519833600006, 80) (1519833600007, 70)"
        )
        tdSql.execute(
            f"insert into vdb1.vtb10 values(1519833600008, 81) (1519833600007, 71) vdb1.vtb11 values(1519833600006, 81) (1519833600007, 71)"
        )
        tdSql.execute(
            f"insert into vdb2.vtb20 values(1519833600008, 82) (1519833600007, 72) vdb2.vtb21 values(1519833600006, 82) (1519833600007, 72)"
        )
        tdSql.execute(
            f"insert into vdb3.vtb30 values(1519833600008, 83) (1519833600007, 73) vdb3.vtb31 values(1519833600006, 83) (1519833600007, 73)"
        )
        tdSql.query(f"select * from vdb0.mt")
        tdSql.query(f"select ts from vdb0.mt")

        tdSql.checkRows(17)

        tdLog.info(f"=============== step6")
        tdSql.execute(
            f"insert into vdb0.vtb00 values(1519833600009, 90) (1519833600010, 100) vdb1.vtb10 values(1519833600009, 90) (1519833600010, 100) vdb2.vtb20 values(1519833600009, 90) (1519833600010, 100) vdb3.vtb30 values(1519833600009, 90) (1519833600010, 100)"
        )
        tdSql.execute(
            f"insert into vdb0.vtb01 values(1519833600009, 90) (1519833600010, 100) vdb1.vtb11 values(1519833600009, 90) (1519833600010, 100) vdb2.vtb21 values(1519833600009, 90) (1519833600010, 100) vdb3.vtb31 values(1519833600009, 90) (1519833600010, 100)"
        )

        tdSql.query(f"select * from vdb0.mt")
        tdSql.query(f"select ts from vdb0.mt")

        tdSql.checkRows(21)

        tdLog.info(f"=============== step7")
        tdSql.execute(
            f"insert into vdb0.vtb00 values(1519833600012, 120) (1519833600011, 110) vdb1.vtb10 values(1519833600012, 120) (1519833600011, 110) vdb2.vtb20 values(1519833600012, 120) (1519833600011, 110) vdb3.vtb30 values(1519833600012, 120) (1519833600011, 110)"
        )
        tdSql.execute(
            f"insert into vdb0.vtb01 values(1519833600012, 120) (1519833600011, 110) vdb1.vtb11 values(1519833600012, 120) (1519833600011, 110) vdb2.vtb21 values(1519833600012, 120) (1519833600011, 110) vdb3.vtb31 values(1519833600012, 120) (1519833600011, 110)"
        )

        tdSql.query(f"select * from vdb0.mt")
        tdSql.query(f"select ts from vdb0.mt")

        tdSql.checkRows(25)
