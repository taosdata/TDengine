# -*- coding: utf-8 -*-

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from frame.autogen import *

class TDTestCase(TBase):

    def run(self):
        tdSql.query("select count(*) from performance_schema.perf_queries limit 1;")
        tdSql.checkData(0, 0, 0)
        tdLog.info("Table [perf_queries] exist in schema [performance_schema]")

        tdSql.error("select count(*) from  information_schema.perf_queries limit 1;")
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
