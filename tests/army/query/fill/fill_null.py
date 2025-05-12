# -*- coding: utf-8 -*-

import frame.etool

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        self.dbname = "ts_5054"
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        etool.benchMark(command=f"-d {self.dbname} -t 1 -n 1000 -S 10 -y")
        tdSql.execute(f"use {self.dbname}")
        tdSql.execute("select database();")
        tdSql.query(
            "select _wstart, first(ts), last(ts) from meters where ts >= '2017-07-14 10:40:00.000' and ts < '2017-07-14 10:40:10.000' partition by groupid interval(3s) fill(NULL);"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 1, "2017-07-14 10:40:00.000")
        tdSql.checkData(0, 2, "2017-07-14 10:40:02.990")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
