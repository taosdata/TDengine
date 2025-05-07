from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSTableAlter2:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stable_alter_2(self):
        """alter super table 2

        1. insert out-of-order data
        2. add column
        2. describe sub table
        4. drop column
        5. describe sub table
        6. loop for 7 times
        7. kill then restart

        Catalog:
            - SuperTables:Alter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/stable/alter_metrics.sim

        """

        tdLog.info(f"======== step1")
        tdSql.prepare("d2", drop=True)
        tdSql.execute(f"use d2")
        tdSql.execute(f"create table mt (ts timestamp, a int) TAGS (t int)")
        tdSql.execute(f"create table tb using mt tags (1)")
        tdSql.execute(f"insert into tb values(now-28d, -28)")
        tdSql.execute(f"insert into tb values(now-27d, -27)")
        tdSql.execute(f"insert into tb values(now-26d, -26)")
        tdSql.query(f"select * from tb")
        tdSql.checkRows(3)

        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "t")

        tdSql.checkData(2, 1, "INT")

        tdLog.info(f"======== step2")
        tdSql.execute(f"alter table mt add column b smallint")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "b")

        tdSql.checkData(2, 1, "SMALLINT")

        tdSql.checkData(3, 0, "t")

        tdSql.checkData(3, 1, "INT")

        tdLog.info(f"======== step3")
        tdSql.execute(f"alter table mt add column c tinyint")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "b")

        tdSql.checkData(2, 1, "SMALLINT")

        tdSql.checkData(3, 0, "c")

        tdSql.checkData(3, 1, "TINYINT")

        tdSql.checkData(4, 0, "t")

        tdSql.checkData(4, 1, "INT")

        tdLog.info(f"======== step4")
        tdSql.execute(f"alter table mt add column d int")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "b")

        tdSql.checkData(2, 1, "SMALLINT")

        tdSql.checkData(3, 0, "c")

        tdSql.checkData(3, 1, "TINYINT")

        tdSql.checkData(4, 0, "d")

        tdSql.checkData(4, 1, "INT")

        tdSql.checkData(5, 0, "t")

        tdSql.checkData(5, 1, "INT")

        tdLog.info(f"======== step5")
        tdSql.execute(f"alter table mt add column e bigint")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "b")

        tdSql.checkData(2, 1, "SMALLINT")

        tdSql.checkData(3, 0, "c")

        tdSql.checkData(3, 1, "TINYINT")

        tdSql.checkData(4, 0, "d")

        tdSql.checkData(4, 1, "INT")

        tdSql.checkData(5, 0, "e")

        tdSql.checkData(5, 1, "BIGINT")

        tdSql.checkData(6, 0, "t")

        tdSql.checkData(6, 1, "INT")

        tdLog.info(f"======== step6")
        tdSql.execute(f"alter table mt add column f float")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "b")

        tdSql.checkData(2, 1, "SMALLINT")

        tdSql.checkData(3, 0, "c")

        tdSql.checkData(3, 1, "TINYINT")

        tdSql.checkData(4, 0, "d")

        tdSql.checkData(4, 1, "INT")

        tdSql.checkData(5, 0, "e")

        tdSql.checkData(5, 1, "BIGINT")

        tdSql.checkData(6, 0, "f")

        tdSql.checkData(6, 1, "FLOAT")

        tdSql.checkData(7, 0, "t")

        tdSql.checkData(7, 1, "INT")

        tdLog.info(f"======== step7")
        tdSql.execute(f"alter table mt add column g double")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "b")

        tdSql.checkData(2, 1, "SMALLINT")

        tdSql.checkData(3, 0, "c")

        tdSql.checkData(3, 1, "TINYINT")

        tdSql.checkData(4, 0, "d")

        tdSql.checkData(4, 1, "INT")

        tdSql.checkData(5, 0, "e")

        tdSql.checkData(5, 1, "BIGINT")

        tdSql.checkData(6, 0, "f")

        tdSql.checkData(6, 1, "FLOAT")

        tdSql.checkData(7, 0, "g")

        tdSql.checkData(7, 1, "DOUBLE")

        tdSql.checkData(8, 0, "t")

        tdSql.checkData(8, 1, "INT")

        tdLog.info(f"======== step8")
        tdSql.execute(f"alter table mt add column h binary(10)")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "b")

        tdSql.checkData(2, 1, "SMALLINT")

        tdSql.checkData(3, 0, "c")

        tdSql.checkData(3, 1, "TINYINT")

        tdSql.checkData(4, 0, "d")

        tdSql.checkData(4, 1, "INT")

        tdSql.checkData(5, 0, "e")

        tdSql.checkData(5, 1, "BIGINT")

        tdSql.checkData(6, 0, "f")

        tdSql.checkData(6, 1, "FLOAT")

        tdSql.checkData(7, 0, "g")

        tdSql.checkData(7, 1, "DOUBLE")

        tdSql.checkData(8, 0, "h")

        tdSql.checkData(8, 1, "VARCHAR")

        tdSql.checkData(8, 2, 10)

        tdSql.checkData(9, 0, "t")

        tdSql.checkData(9, 1, "INT")

        tdLog.info(f"======== step9")
        tdLog.info(f"======== step10")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.execute(f"use d2")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "b")

        tdSql.checkData(2, 1, "SMALLINT")

        tdSql.checkData(3, 0, "c")

        tdSql.checkData(3, 1, "TINYINT")

        tdSql.checkData(4, 0, "d")

        tdSql.checkData(4, 1, "INT")

        tdSql.checkData(5, 0, "e")

        tdSql.checkData(5, 1, "BIGINT")

        tdSql.checkData(6, 0, "f")

        tdSql.checkData(6, 1, "FLOAT")

        tdSql.checkData(7, 0, "g")

        tdSql.checkData(7, 1, "DOUBLE")

        tdSql.checkData(8, 0, "h")

        tdSql.checkData(8, 1, "VARCHAR")

        tdSql.checkData(8, 2, 10)

        tdSql.checkData(9, 0, "t")

        tdSql.checkData(9, 1, "INT")

        tdLog.info(f"======== step11")
        # sql alter table mt drop column a -x step111
        #  return -1
        # step111

        # sql alter table mt drop column ts -x step112
        #  return -1
        # step112

        # sql alter table mt drop column cdfg -x step113
        #  return -1
        # step113

        # sql alter table mt add column a -x step114
        #  return -1
        # step114

        # sql alter table mt add column b -x step115
        #  return -1
        # step115

        tdLog.info(f"======== step12")
        tdSql.execute(f"alter table mt drop column b")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "c")

        tdSql.checkData(2, 1, "TINYINT")

        tdSql.checkData(3, 0, "d")

        tdSql.checkData(3, 1, "INT")

        tdSql.checkData(4, 0, "e")

        tdSql.checkData(4, 1, "BIGINT")

        tdSql.checkData(5, 0, "f")

        tdSql.checkData(5, 1, "FLOAT")

        tdSql.checkData(6, 0, "g")

        tdSql.checkData(6, 1, "DOUBLE")

        tdSql.checkData(7, 0, "h")

        tdSql.checkData(7, 1, "VARCHAR")

        tdSql.checkData(7, 2, 10)

        tdSql.checkData(8, 0, "t")

        tdSql.checkData(8, 1, "INT")

        tdLog.info(f"======== step13")
        tdSql.execute(f"alter table mt drop column c")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "d")

        tdSql.checkData(2, 1, "INT")

        tdSql.checkData(3, 0, "e")

        tdSql.checkData(3, 1, "BIGINT")

        tdSql.checkData(4, 0, "f")

        tdSql.checkData(4, 1, "FLOAT")

        tdSql.checkData(5, 0, "g")

        tdSql.checkData(5, 1, "DOUBLE")

        tdSql.checkData(6, 0, "h")

        tdSql.checkData(6, 1, "VARCHAR")

        tdSql.checkData(6, 2, 10)

        tdSql.checkData(7, 0, "t")

        tdSql.checkData(7, 1, "INT")

        tdLog.info(f"======== step14")
        tdSql.execute(f"alter table mt drop column d")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "e")

        tdSql.checkData(2, 1, "BIGINT")

        tdSql.checkData(3, 0, "f")

        tdSql.checkData(3, 1, "FLOAT")

        tdSql.checkData(4, 0, "g")

        tdSql.checkData(4, 1, "DOUBLE")

        tdSql.checkData(5, 0, "h")

        tdSql.checkData(5, 1, "VARCHAR")

        tdSql.checkData(5, 2, 10)

        tdSql.checkData(6, 0, "t")

        tdSql.checkData(6, 1, "INT")

        tdLog.info(f"======== step15")
        tdSql.execute(f"alter table mt drop column e")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "f")

        tdSql.checkData(2, 1, "FLOAT")

        tdSql.checkData(3, 0, "g")

        tdSql.checkData(3, 1, "DOUBLE")

        tdSql.checkData(4, 0, "h")

        tdSql.checkData(4, 1, "VARCHAR")

        tdSql.checkData(4, 2, 10)

        tdSql.checkData(5, 0, "t")

        tdSql.checkData(5, 1, "INT")

        tdLog.info(f"======== step16")
        tdSql.execute(f"alter table mt drop column f")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "g")

        tdSql.checkData(2, 1, "DOUBLE")

        tdSql.checkData(3, 0, "h")

        tdSql.checkData(3, 1, "VARCHAR")

        tdSql.checkData(3, 2, 10)

        tdSql.checkData(4, 0, "t")

        tdSql.checkData(4, 1, "INT")

        tdLog.info(f"======== step17")
        tdSql.execute(f"alter table mt drop column g")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "h")

        tdSql.checkData(2, 1, "VARCHAR")

        tdSql.checkData(2, 2, 10)

        tdSql.checkData(3, 0, "t")

        tdSql.checkData(3, 1, "INT")

        tdLog.info(f"============= step18")
        tdSql.execute(f"alter table mt drop column h")
        tdSql.query(f"describe tb")
        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(1, 0, "a")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(2, 0, "t")

        tdSql.checkData(2, 1, "INT")

        tdSql.checkCols(7)

        tdLog.info(f"======= over")
        tdSql.execute(f"drop database d2")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
