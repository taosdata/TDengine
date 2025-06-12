from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestTableCount:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_table_count(self):
        """Table Count

        1.

        Catalog:
            - MetaData

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-9 Simon Guan Migrated from tsim/stable/metrics_idx.sim

        """

        dbPrefix = "m_me_db_idx"
        tbPrefix = "m_me_tb_idx"
        mtPrefix = "m_me_mt_idx"

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix
        mt = mtPrefix

        tdSql.execute(f"drop database  if exists {db}")
        tdSql.execute(f"create database {db}")

        tdSql.execute(f"use {db}")

        tdSql.execute(f"create table {mt} (ts timestamp, speed int) TAGS(sp int)")

        tbNum = 10000

        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"insert into {tb} using {mt} tags( {i} ) values(now,  {i} )")
            i = i + 1

        tdSql.execute(f"use information_schema")
        tdSql.query(
            f'select count(*) from ins_tables where db_name = "m_me_db_idx" and create_time > now() - 10m'
        )
        tdSql.checkData(0, 0, tbNum)

        tdSql.query(
            f'select count(*) from ins_tables where db_name = "m_me_db_idx" and create_time < now();'
        )
        tdSql.checkData(0, 0, tbNum)

        tdSql.execute(f"use {db}")
        doubletbNum = 20000

        while i < doubletbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"insert into {tb} using {mt} tags( {i} ) values(now,  {i} )")
            i = i + 1

        tdSql.execute(f"use information_schema")

        tdSql.query(
            f'select count(*) from ins_tables where db_name = "m_me_db_idx" and create_time < now();'
        )
        tdSql.checkData(0, 0, doubletbNum)
