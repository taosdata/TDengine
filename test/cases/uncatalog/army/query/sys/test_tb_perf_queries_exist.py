# -*- coding: utf-8 -*-
f
from new_test_framework.utils import tdLog, tdSql, etool, tdCom

'''
    TS-5349: https://jira.taosdata.com:18080/browse/TS-5349
    查询 performance_schema.perf_queries 后, 再查询 information_schema.perf_queries,
    正常情况下在 information_schema 中不存在表 perf_queries
'''

class TestTbPerfQueriesExist:
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def test_tb_perf_queries_exist(self):
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
        tdSql.query("select * from performance_schema.perf_queries;")
        tdLog.info("Table [perf_queries] exist in schema [performance_schema]")

        tdSql.error("select * from  information_schema.perf_queries;")
        tdLog.success(f"{__file__} successfully executed")
