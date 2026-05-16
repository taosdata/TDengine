# -*- coding: utf-8 -*-
from new_test_framework.utils import tdLog, tdSql, etool, tdCom
from new_test_framework.utils.sqlset import TDSetSql
import time


class TestMetaBugs:
    def setup_class(cls):
        cls.setup_class1(cls)        

    #
    # ------------------- TS-5394 ----------------
    #
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
    
    #
    # ------------------- TS-5580 ----------------
    #
    def setup_class1(cls):
        cls.setsql = TDSetSql()
        cls.dbname = 'db'
        cls.stbname = 'stb'
        cls.binary_length = 20 # the length of binary for column_dict
        cls.nchar_length = 20  # the length of nchar for column_dict
        cls.ts = 1537146000000
        cls.column_dict = {
            'ts'  : 'timestamp',
            'col1': 'tinyint',
            'col2': 'smallint',
            'col3': 'int',
            'col4': 'bigint',
            'col5': 'float',
            'col6': 'double',
            'col7': 'double',
            'col8': 'double',
            'col9': 'double',
            'col10': 'double',
            'col11': 'double',
            'col12': 'double',
            'col13': 'double',
            'col14': 'double',
            'col15': 'double',
            'col16': 'double',
            'col17': 'double',
            'col18': 'double',
            'col19': 'double'
        }
        cls.tbnum = 500
        cls.rowNum = 10
        cls.tag_dict = {
            't0':'int',
            't1':'bigint',
            't2':'float',
            't3':'double',
            't4':'bool',
            't5':'bool',
            't6':'bool',
            't7':'bool',
            't8':'bool',
            't9':'bool',
            't10':'bool',
            't11':'bool',
            't12':'bool',
            't13':'bool',
            't14':'bool',
            't15':'bool',
            't16':'bool',
            't17':'bool',
            't18':'bool',
            't19':'bool',
        }
        cls.tag_values = [
            '1','1','1','1','true','true','true','true','true','true','true','true','true','true','true','true','true',
            'true','true','true'
        ]
    def prepare_data(self):
        tdSql.execute(f"create database if not exists {self.dbname} vgroups 2")
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(self.setsql.set_create_stable_sql(self.stbname,self.column_dict,self.tag_dict))
        sql = "create table "
        tags =  ", ".join(self.tag_values)
        for i in range(self.tbnum):
            sql += f" {self.stbname}_{i} using {self.stbname} tags({tags})"
        tdSql.execute(sql)

    def check_query_ins_tags(self):
        for i in range(self.tbnum):
            sql = f'select tag_name, tag_value from information_schema.ins_tags where table_name = "{self.stbname}_{i}"'
            tdSql.query(sql)
            tdSql.checkRows(20)

    def check_query_ins_columns(self):
        for i in range(self.tbnum):
            sql = f'select col_name from information_schema.ins_columns where table_name = "{self.stbname}_{i}"'
            tdSql.query(sql)
            tdSql.checkRows(20)

    def do_ts5580(self):
        self.prepare_data()
        self.check_query_ins_tags()
        self.check_query_ins_columns()
        print("do TS-5580 ............................ [passed]")
    
    #
    # ------------------- main ----------------
    #
    def test_meta_bugs(self):
        """Meta bugs
        
        1. Verify bug TS-5394(perf_queries should not exist in information_schema on init)
        2. Verify bug TS-5580(ins_tags and ins_columns query performance issue)

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-21 Alex Duan Migrated from uncatalog/army/query/sys/test_tb_perf_queries_exist.py
            - 2025-10-31 Alex Duan Migrated from uncatalog/system-test/99-TDcase/test_TS_5580.py
    
        """
        self.do_ts5394()
        self.do_ts5580()