# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql, etool, tdCom


class TestFillNull:
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.dbname = "ts_5054"

    def test_fill_null(self):
        etool.benchMark(command=f"-d {self.dbname} -t 1 -n 1000 -S 10 -y")
        tdSql.execute(f"use {self.dbname}")
        tdSql.execute("select database();")
        tdSql.query(
            "select _wstart, first(ts), last(ts) from meters where ts >= '2017-07-14 10:40:00.000' and ts < '2017-07-14 10:40:10.000' partition by groupid interval(3s) fill(NULL);"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, "2017-07-14 10:40:00.000")
        tdSql.checkData(0, 2, "2017-07-14 10:40:02.990")

        tdLog.success("%s successfully executed" % __file__)
