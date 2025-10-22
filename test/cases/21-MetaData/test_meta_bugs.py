# -*- coding: utf-8 -*-
from new_test_framework.utils import tdLog, tdSql, etool, tdCom


class TestMetaBugs:
    def setup_class(cls):
        pass

    '''
        TS-5394: https://jira.taosdata.com:18080/browse/TS-5349
        查询 performance_schema.perf_queries 后, 再查询 information_schema.perf_queries,
        正常情况下在 information_schema 中不存在表 perf_queries
    '''    
    def do_ts5394(self):
        tdSql.query("select * from performance_schema.perf_queries;")
        tdLog.info("Table [perf_queries] exist in schema [performance_schema]")
        tdSql.error("select * from  information_schema.perf_queries;")
        print("\n")
        print("do TS-5394 ............................ [passed]")

    def test_meta_bugs(self):
        """Meta bugs
        
        1. Verify bug TS-5394

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-21 Alex Duan Migrated from uncatalog/army/query/sys/test_tb_perf_queries_exist.py
    
        """
        self.do_ts5394()