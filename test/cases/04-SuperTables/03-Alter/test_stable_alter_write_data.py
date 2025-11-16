from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestStableAlterThenWriteData:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stable_alter_then_write_data(self):
        """Alter then write data

        1. Create a table and insert data
        2. Alter the table and insert data
        3. Alter the table and insert expired data
        4. Repeat the above operations
        5. Restart and verify that all data remains intact

        Catalog:
            - SuperTables:Alter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-11 Simon Guan Migrated from tsim/stable/alter_metrics.sim
            - 2025-8-11 Simon Guan Migrated from tsim/alter/cached_schema_after_alter.sim

        """

        self.AlterMetrics()
        tdStream.dropAllStreamsAndDbs()
        self.AlterStable()
        tdStream.dropAllStreamsAndDbs()

    def AlterMetrics(self):
        tdLog.info(f"======== tsim/stable/alter_metrics.sim")
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

    def CachedSchemaAfterAlter(self):
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

    def AlterStable(self):
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
