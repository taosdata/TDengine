import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseLen:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_len(self):
        """DB Name: length

        1. Create database with an excessively long name
        2. Test with invalid values

        Catalog:
            - Database:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/db/len.sim

        """

        tdLog.info(f"=============== step1")
        tdSql.error(f"drop database dd")
        tdSql.error(f"create database ")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step2")
        tdSql.execute(f"create database a")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.execute(f"drop database a")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step3")
        tdSql.execute(f"create database a12345678")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.execute(f"drop database a12345678")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step4")
        tdSql.error(
            f"create database a012345678901201234567890120123456789012a012345678901201234567890120123456789012"
        )

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step5")
        tdSql.execute(f"create database a;1")
        tdSql.execute(f"drop database a")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step6")
        tdSql.error(f"create database a'1")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step7")
        tdSql.error(f"create database (a)")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step8")
        tdSql.error(f"create database a.1")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
