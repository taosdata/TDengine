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

import os
import json
import csv
import datetime

import frame
import frame.eos
import frame.etool
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):
    def caseDescription(self):
        """
        [TD-33588] taosBenchmark supports decimal data type
        """


    def exec_benchmark(self, benchmark, json_file, options=""):
        cmd = f"{benchmark} {options} -f {json_file}"
        eos.exe(cmd)


    def get_scale_value(self, number_str):
        dot_position = number_str.find('.')

        if dot_position == -1:
            return 0
        else:
            return len(number_str) - dot_position - 1


    def check_decimal_scale(self, db_name, tbl_name, col_config):
        sql = "select max(%s) from %s.%s" % (col_config['name'], db_name, tbl_name)
        tdSql.query(sql)
        decimal = tdSql.getData(0, 0)
        scale   = self.get_scale_value(decimal)

        if scale != col_config['scale']:
            tdLog.exit(f"scale value is not as expected. actual: {scale}, expected: {col_config['scale']}, col_config: {col_config}")


    def format_out_of_bounds_sql(self, db_name, tbl_name, col_config, min_key=None, max_key=None):
        sql = "select * from %s.%s" % (db_name, tbl_name)
    
        if min_key is not None and max_key is not None:
            sql += " where %s>=%s or %s<%s" % (col_config['name'], col_config[max_key], col_config['name'], col_config[min_key])

        return sql


    def check_within_bounds(self, db_name, tbl_name, col_config, min_key=None, max_key=None):
        sql = self.format_out_of_bounds_sql(db_name, tbl_name, col_config, min_key, max_key)
        tdSql.query(sql)
        tdSql.checkRows(0)


    def check_json_normal(self, benchmark, json_file, options=""):
        # exec
        self.exec_benchmark(benchmark, json_file, options)

        # check result
        with open(json_file) as file:
             data = json.load(file)

        db          = data["databases"][0]
        db_name     = db['dbinfo']['name']
        stb         = db["super_tables"][0]
        stb_name    = stb['name']
        columns     = stb['columns']


        self.check_decimal_scale(db_name, stb_name, columns[1])
        self.check_decimal_scale(db_name, stb_name, columns[9])

        self.check_within_bounds(db_name, stb_name, columns[0], "min", "max")
        self.check_within_bounds(db_name, stb_name, columns[1])
        self.check_within_bounds(db_name, stb_name, columns[2], "min", "max")
        self.check_within_bounds(db_name, stb_name, columns[3], "min", "max")
        self.check_within_bounds(db_name, stb_name, columns[4], "min", "max")
        self.check_within_bounds(db_name, stb_name, columns[5], "dec_min", "dec_max")
        self.check_within_bounds(db_name, stb_name, columns[6], "dec_min", "dec_max")
        self.check_within_bounds(db_name, stb_name, columns[7], "dec_min", "dec_max")
        self.check_within_bounds(db_name, stb_name, columns[8])
        self.check_within_bounds(db_name, stb_name, columns[9])
        self.check_within_bounds(db_name, stb_name, columns[10], "min", "max")
        self.check_within_bounds(db_name, stb_name, columns[11], "min", "max")
        self.check_within_bounds(db_name, stb_name, columns[12], "min", "max")
        self.check_within_bounds(db_name, stb_name, columns[13], "dec_min", "dec_max")
        self.check_within_bounds(db_name, stb_name, columns[14], "dec_min", "dec_max")
        self.check_within_bounds(db_name, stb_name, columns[15], "dec_min", "dec_max")
        self.check_within_bounds(db_name, stb_name, columns[16])


    def check_json_others(self, benchmark, json_file, options=""):
        pass


    def run(self):
        # path
        benchmark = etool.benchMarkFile()

        # check normal
        json_file = "tools/benchmark/basic/json/insert-decimal.json"
        self.check_json_normal(benchmark, json_file)

        # check others
        # self.check_json_others(benchmark, json_file)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
