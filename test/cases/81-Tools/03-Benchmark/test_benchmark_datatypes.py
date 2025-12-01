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

from new_test_framework.utils import tdLog, tdSql, etool, eos
import json
import os

class TestBenchmarkDatatypes:
    def caseDescription(self):
        """
        [TD-33588] taosBenchmark supports decimal data type
        """

    #
    # ------------------- test_insert_decimal.py ----------------
    #

    def exec_benchmark(self, benchmark, json_file, options=""):
        cmd = f"{benchmark} {options} -f {json_file}"
        output, error, code = eos.run(cmd, ret_code=True)
        tdLog.info("output: >>>%s<<<" % output)
        tdLog.info("error: >>>%s<<<" % error)
        tdLog.info("code: >>>%s<<<" % code)


    def exec_benchmark_and_check(self, benchmark, json_file, expect_info, options=""):
        cmd = f"{benchmark} {options} -f {json_file}"
        rlist = eos.runRetList(cmd, True, False, True)
        self.checkListString(rlist, expect_info)


    def get_decimal_scale(self, dec):
        _, _, exponent = dec.as_tuple()
        return max(0, -exponent)


    def check_decimal_scale(self, db_name, tbl_name, col_config):
        sql = "select count(*) from %s.%s" % (db_name, tbl_name)
        tdSql.query(sql)
        count = tdSql.getData(0, 0)
        tdLog.info("in func check_decimal_scale, sql: %s, rows: %d, cols: %d, count: %d" % (sql, tdSql.queryRows, tdSql.queryCols, count))

        sql = "select max(%s) from %s.%s" % (col_config['name'], db_name, tbl_name)
        tdSql.query(sql)
        tdLog.info("in func check_decimal_scale, sql: %s, rows: %d, cols: %d" % (sql, tdSql.queryRows, tdSql.queryCols))
        decimal = tdSql.getData(0, 0)
        tdLog.info("in func check_decimal_scale, type(decimal): %s, decimal: %s" % (type(decimal), decimal))
        scale   = self.get_decimal_scale(decimal)

        if scale != col_config['scale']:
            tdLog.exit(f"scale value is not as expected. actual: {scale}, expected: {col_config['scale']}, col_config: {col_config}")


    def generate_min_max_values(self, precision, scale):
        int_part_digits = precision - scale
        
        max_value = '9' * int_part_digits
        if scale > 0:
            max_value += '.' + '9' * scale
        
        min_value = '-' + max_value

        return min_value, max_value


    def format_out_of_bounds_sql(self, db_name, tbl_name, col_config, min_key=None, max_key=None):
        sql = "select * from %s.%s" % (db_name, tbl_name)
    
        if min_key is not None and max_key is not None:
            sql += " where %s>=%s or %s<%s" % (col_config['name'], col_config[max_key], col_config['name'], col_config[min_key])
        else:
            min_value, max_value = self.generate_min_max_values(col_config['precision'], col_config['scale'])
            sql += " where %s>=%s or %s<%s" % (col_config['name'], max_value, col_config['name'], min_value)

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

        tdLog.info("begin to fetch meta info")
        db          = data["databases"][0]
        db_name     = db['dbinfo']['name']
        stb         = db["super_tables"][0]
        stb_name    = stb['name']
        columns     = stb['columns']

        tdLog.info("begin to check json decimal, columns: %s" % columns)
        self.check_decimal_scale(db_name, stb_name, columns[1])
        self.check_decimal_scale(db_name, stb_name, columns[9])
        tdLog.info("check json decimal scale successfully")

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
        # self.check_within_bounds(db_name, stb_name, columns[16])


    def check_json_others(self, benchmark, json_file, options=""):
        # check precision
        new_json_file = self.genNewJson(json_file, self.func_precision_zero)
        self.exec_benchmark_and_check(benchmark, new_json_file, "Invalid precision value of decimal type in json", options)
        self.deleteFile(new_json_file)

        new_json_file = self.genNewJson(json_file, self.func_precision_negative)
        self.exec_benchmark_and_check(benchmark, new_json_file, "Invalid precision value of decimal type in json", options)
        self.deleteFile(new_json_file)

        new_json_file = self.genNewJson(json_file, self.func_precision_exceed_max)
        self.exec_benchmark_and_check(benchmark, new_json_file, "Invalid precision value of decimal type in json", options)
        self.deleteFile(new_json_file)

        # check scale
        new_json_file = self.genNewJson(json_file, self.func_scale_negative)
        self.exec_benchmark_and_check(benchmark, new_json_file, "Invalid scale value of decimal type in json", options)
        self.deleteFile(new_json_file)

        new_json_file = self.genNewJson(json_file, self.func_scale_exceed_precision)
        self.exec_benchmark_and_check(benchmark, new_json_file, "Invalid scale value of decimal type in json", options)
        self.deleteFile(new_json_file)

        # check dec_min/dec_max
        new_json_file = self.genNewJson(json_file, self.func_dec64_min_max)
        self.exec_benchmark_and_check(benchmark, new_json_file, "Invalid dec_min/dec_max value of decimal type in json", options)
        self.deleteFile(new_json_file)

        new_json_file = self.genNewJson(json_file, self.func_dec128_min_max)
        self.exec_benchmark_and_check(benchmark, new_json_file, "Invalid dec_min/dec_max value of decimal type in json", options)
        self.deleteFile(new_json_file)


    def func_precision_zero(self, data):
        db  = data['databases'][0]
        stb = db["super_tables"][0]
        stb['columns'].clear()
        stb['columns'].append({ "type": "decimal", "name": "dec64a", "precision": 0, "scale": 0})


    def func_precision_negative(self, data):
        db  = data['databases'][0]
        stb = db["super_tables"][0]
        stb['columns'].clear()
        stb['columns'].append({ "type": "decimal", "name": "dec64a", "precision": -3, "scale": 0})


    def func_precision_exceed_max(self, data):
        db  = data['databases'][0]
        stb = db["super_tables"][0]
        stb['columns'].clear()
        stb['columns'].append({ "type": "decimal", "name": "dec64a", "precision": 39, "scale": 0})


    def func_scale_negative(self, data):
        db  = data['databases'][0]
        stb = db["super_tables"][0]
        stb['columns'].clear()
        stb['columns'].append({ "type": "decimal", "name": "dec64a", "precision": 10, "scale": -3})


    def func_scale_exceed_precision(self, data):
        db  = data['databases'][0]
        stb = db["super_tables"][0]
        stb['columns'].clear()
        stb['columns'].append({ "type": "decimal", "name": "dec64a", "precision": 10, "scale": 11})


    def func_dec64_min_max(self, data):
        db  = data['databases'][0]
        stb = db["super_tables"][0]
        stb['columns'].clear()
        stb['columns'].append({ "type": "decimal", "name": "dec64f", "precision": 10, "scale": 6, "dec_max": "555.456789", "dec_min": "555.456789"})


    def func_dec128_min_max(self, data):
        db  = data['databases'][0]
        stb = db["super_tables"][0]
        stb['columns'].clear()
        stb['columns'].append({ "type": "decimal", "name": "dec128f", "precision": 24, "scale": 10, "dec_max": "1234567890.5678912345", "dec_min": "1234567890.5678912345"})


    def check_cmd_normal(self, benchmark, options=""):
        # exec
        cmd = f'{benchmark} {options} -b "int,decimal(10,6),decimal(24,10)" -t 10 -y'
        eos.exe(cmd)

        db_name     = 'test'
        stb_name    = 'meters'
        col_config  = {'name': 'c1', 'scale': 6}
        self.check_decimal_scale(db_name, stb_name, {'name': 'c1', 'scale': 6})
        self.check_decimal_scale(db_name, stb_name, {'name': 'c2', 'scale': 10})
        tdLog.info("check cmd decimal scale successfully")

        self.check_within_bounds(db_name, stb_name, {'name': 'c1', 'precision': 10, 'scale': 6})
        self.check_within_bounds(db_name, stb_name, {'name': 'c2', 'precision': 24, 'scale': 10})


    def check_cmd_others(self, benchmark, options=""):
        # check precision
        cmd = f'{benchmark} {options} -b "int,decimal(10,6),decimal(0,0)" -t 10 -y'
        rlist = eos.runRetList(cmd, True, False, True)
        self.checkListString(rlist, "Invalid precision value of decimal type in args")

        cmd = f'{benchmark} {options} -b "int,decimal(10,6),decimal(-3,0)" -t 10 -y'
        rlist = eos.runRetList(cmd, True, False, True)
        self.checkListString(rlist, "Invalid precision value of decimal type in args")

        cmd = f'{benchmark} {options} -b "int,decimal(10,6),decimal(39,0)" -t 10 -y'
        rlist = eos.runRetList(cmd, True, False, True)
        self.checkListString(rlist, "Invalid precision value of decimal type in args")

        # check scale
        cmd = f'{benchmark} {options} -b "int,decimal(10,6),decimal(10,-3)" -t 10 -y'
        rlist = eos.runRetList(cmd, True, False, True)
        self.checkListString(rlist, "Invalid scale value of decimal type in args")

        cmd = f'{benchmark} {options} -b "int,decimal(10,6),decimal(10,11)" -t 10 -y'
        rlist = eos.runRetList(cmd, True, False, True)
        self.checkListString(rlist, "Invalid scale value of decimal type in args")


    def do_insert_decimal(self):
        # check env
        cmd = f"pip3 list"
        output, error = eos.run(cmd)
        tdLog.info("output: >>>%s<<<" % output)

        # path
        benchmark = etool.benchMarkFile()

        # check normal
        json_file = f"{os.path.dirname(__file__)}/json/insert-decimal.json"
        self.check_json_normal(benchmark, json_file)

        # check others
        self.check_json_others(benchmark, json_file)

        # check cmd normal
        self.check_cmd_normal(benchmark)

        # check cmd others
        self.check_cmd_others(benchmark)


        tdLog.success("%s successfully executed" % __file__)



    #
    # ------------------- main ----------------
    #
    def test_benchmark_datatypes(self):
        """taosBenchmark datatypes

        1. Check decimal data type
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/test_insert_decimal.py
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/
            - 2025-10-28 Alex Duan Migrated from uncatalog/army/tools/benchmark/basic/

        """
        self.do_insert_decimal()