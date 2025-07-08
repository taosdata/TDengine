###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import time
from new_test_framework.utils.log import tdLog
from new_test_framework.utils.sql import tdSql
from new_test_framework.utils.sqlset import TDSetSql

class TestTS_5580:
    updatecfgDict = {'qDebugFlag':135 , 'mDebugFlag':135}

    def setup_class(cls):
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
            f'1','1','1','1','true','true','true','true','true','true','true','true','true','true','true','true','true',
            'true','true','true'
        ]
    def prepare_data(self):
        tdSql.execute(f"create database if not exists {self.dbname} vgroups 2")
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(self.setsql.set_create_stable_sql(self.stbname,self.column_dict,self.tag_dict))
        for i in range(self.tbnum):
            tdSql.execute(f"create table {self.stbname}_{i} using {self.stbname} tags({self.tag_values[0]}, {self.tag_values[1]}, "
                          f"{self.tag_values[2]}, {self.tag_values[3]}, {self.tag_values[4]}, {self.tag_values[5]}, "
                          f"{self.tag_values[6]}, {self.tag_values[7]}, {self.tag_values[8]}, {self.tag_values[9]}, "
                          f"{self.tag_values[10]}, {self.tag_values[11]}, {self.tag_values[12]}, {self.tag_values[13]}, "
                          f"{self.tag_values[14]}, {self.tag_values[15]}, {self.tag_values[16]}, {self.tag_values[17]}, "
                          f"{self.tag_values[18]}, {self.tag_values[19]})")

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
    def test_ts_5580(self):
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
        self.prepare_data()
        self.check_query_ins_tags()
        self.check_query_ins_columns()


        # Cleanup from original stop method
        tdLog.success("%s successfully executed" % __file__)


    