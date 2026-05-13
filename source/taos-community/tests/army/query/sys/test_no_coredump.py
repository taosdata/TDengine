# -*- coding: utf-8 -*-

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from frame.autogen import *

"""
    TS-5437: https://jira.taosdata.com:18080/browse/TS-5437
    No coredump occurs when querying metadata.
"""

class TDTestCase(TBase):
    def run(self):
        tdSql.prepare("test")
        tdSql.execute("create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.query("select * from information_schema.ins_tables where create_time < now;")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
