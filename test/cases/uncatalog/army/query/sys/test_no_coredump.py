# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql, etool, tdCom

"""
    TS-5437: https://jira.taosdata.com:18080/browse/TS-5437
    No coredump occurs when querying metadata.
"""

class TestNoCoredump:
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def test_no_coredump(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """
        tdSql.prepare("test")
        tdSql.execute("create table t1(ts timestamp, a int, b int, c int, d double);")
        tdSql.query("select * from information_schema.ins_tables where create_time < now;")

