# -*- coding: utf-8 -*-

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from frame.autogen import *

'''
    TS-5349: https://jira.taosdata.com:18080/browse/TS-5349
    查询 performance_schema.perf_queries 后, 再查询 information_schema.perf_queries,
    正常情况下在 information_schema 中不存在表 perf_queries
'''

class TDTestCase(TBase):

    def run(self):
        tdSql.query("select * from performance_schema.perf_queries;")
        tdLog.info("Table [perf_queries] exist in schema [performance_schema]")

        tdSql.error("select * from  information_schema.perf_queries;")
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
