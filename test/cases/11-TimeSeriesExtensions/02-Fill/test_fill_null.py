# -*- coding: utf-8 -*-
from new_test_framework.utils import tdLog, tdSql


class TestFillNull:

    def test_fill_null(self):
        """test fill(NULL) without order by

        test fill(NULL) without order by

        Since: v3.3.0.0

        Labels: fill

        Jira: TS-5054

        History:
            - 2024-7-2 Zhi Yong Created
            - 2025-5-08 Huo Hong Migrated to new test framework

        """
        self.dbname = "ts_5054"
        etool.benchMark(command=f"-d {self.dbname} -t 1 -n 1000 -S 10 -y")
        tdSql.execute(f"use {self.dbname}")
        tdSql.execute("select database();")
        tdSql.query(
            "select _wstart, first(ts), last(ts) from meters where ts >= '2017-07-14 10:40:00.000' and ts < '2017-07-14 10:40:10.000' partition by groupid interval(3s) fill(NULL);"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, "2017-07-14 10:40:00.000")
        tdSql.checkData(0, 2, "2017-07-14 10:40:02.990")

