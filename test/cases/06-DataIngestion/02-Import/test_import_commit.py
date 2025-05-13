from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestImportCommit:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_import_commit(self):
        """import data commit

        1. create table
        2. insert data
        3. query data

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/import/commit.sim

        """

        tdLog.info(f"========= step1")
        tdSql.execute(f"create database ic1db duration 7;")
        tdSql.execute(f"create table ic1db.tb(ts timestamp, s int);")
        tdSql.execute(f"insert into ic1db.tb values(now-30d, -30);")
        tdSql.execute(f"insert into ic1db.tb values(now-20d, -20);")
        tdSql.execute(f"insert into ic1db.tb values(now-10d, -10);")
        tdSql.execute(f"insert into ic1db.tb values(now-5d, -5);")
        tdSql.execute(f"insert into ic1db.tb values(now+1m, 1);")
        tdSql.execute(f"insert into ic1db.tb values(now+2m, 2);")
        tdSql.execute(f"insert into ic1db.tb values(now+3m, 6);")
        tdSql.execute(f"insert into ic1db.tb values(now+4m, 8);")
        tdSql.execute(f"insert into ic1db.tb values(now+5m, 10);")
        tdSql.execute(f"insert into ic1db.tb values(now+6m, 12);")
        tdSql.execute(f"insert into ic1db.tb values(now+7m, 14);")
        tdSql.execute(f"insert into ic1db.tb values(now+8m, 16);")
        tdSql.query(f"select * from ic1db.tb;")
        tdSql.checkRows(12)

        tdLog.info(f"========= step2")
        tdSql.execute(f"create database ic2db duration 7;")
        tdSql.execute(f"create table ic2db.tb(ts timestamp, s int);")
        tdSql.execute(f"insert into ic2db.tb values(now, 0);")
        tdSql.execute(f"import into ic2db.tb values(now-30d, -30);")
        tdSql.execute(f"import into ic2db.tb values(now-20d, -20);")
        tdSql.execute(f"import into ic2db.tb values(now-10d, -10);")
        tdSql.execute(f"import into ic2db.tb values(now-5d, -5);")
        tdSql.execute(f"import into ic2db.tb values(now+1m, 1);")
        tdSql.execute(f"import into ic2db.tb values(now+2m, 2);")
        tdSql.execute(f"import into ic2db.tb values(now+3m, 6);")
        tdSql.execute(f"import into ic2db.tb values(now+4m, 8);")
        tdSql.execute(f"import into ic2db.tb values(now+5m, 10);")
        tdSql.execute(f"import into ic2db.tb values(now+6m, 12);")
        tdSql.execute(f"import into ic2db.tb values(now+7m, 14);")
        tdSql.execute(f"import into ic2db.tb values(now+8m, 16);")
        tdSql.query(f"select * from ic2db.tb;")
        tdSql.checkRows(13)

        tdLog.info(f"========= step3")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"========= step4")
        tdSql.query(f"select * from ic2db.tb;")
        tdSql.checkRows(13)

        tdSql.query(f"select * from ic1db.tb;")
        tdSql.checkRows(12)
