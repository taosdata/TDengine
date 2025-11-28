from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck
import random
from datetime import datetime as datatime


class TestNormalTableColumnNumLimit:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_column_num_limit(self):
        """Column num limit

        1. Create normal table
        2. Add or delete columns
        3. Check column count, the count should not exceed 32767

        Since: v3.0.0.0

        Labels: common,ci

        Jira: TS-5953

        History:
            - 2025-6-18 Ethan liu adds this case for test if one table cloumns can exceed limit

        """

        db = "column_limit_db"
        tb = "column_limit_tb"

        totalColumnsCount = 32000

        tdLog.info(f"=============== step1")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        createsql = f"create table {tb}(ts timestamp"
        for i in range(totalColumnsCount - 1):
            createsql += f", a{i} bool"
        createsql += ")"

        tdSql.execute(createsql)

        tdSql.query(f"show tables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step2")
        tdSql.query(f"desc {tb}")
        tdSql.checkRows(totalColumnsCount)


        tdLog.info(f"=============== step3")

        while totalColumnsCount < 32767:
            tdSql.execute(f"alter table {tb} add column a{totalColumnsCount} bool")
            totalColumnsCount += 1

        tdSql.query(f"desc {tb}")
        tdSql.checkRows(32767)

        tdSql.error(f"alter table {tb} add column a{totalColumnsCount} bool")

        tdLog.info(f"=============== step4")

        dropCount = random.randint(1, 30)
        finalCount = totalColumnsCount - dropCount
        tdLog.info(f"dropCount: {dropCount}")
        while dropCount > 0:
            dropColumn = random.randint(1, totalColumnsCount - 1)
            tdSql.execute(f"alter table {tb} drop column a{dropColumn}")
            totalColumnsCount -= 1
            dropCount -= 1

        tdSql.query(f"desc {tb}")
        tdSql.checkRows(totalColumnsCount)

        tdLog.info(f"=============== step5")

        tdSql.query(f"desc {tb}")
        tdSql.checkRows(finalCount)

        tdSql.error(f"alter table {tb} add column a{32767+i} bool")
