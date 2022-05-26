###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from taostest import TDCase, T
from taostest.util.common import TDCom
from taostest.util.remote import Remote
import time
import sys

class TestVgroups(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.case_name = None
        self.date_time = self.tdCom.genTs()[0]
        self.latency_log = self.run_log_dir + "/latency.log"

    def prepare_stream_data(self):
        self.tdCom.drop_all_db()
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdCom.createDb(dbname=dbname, vgroups=1)
        self.tdSql.execute(f'create table if not exists downsampling_stb (ts timestamp, c1 int, c2 double, c3 varchar(100), c4 bool) tags (t1 int, t2 double, t3 varchar(100), t4 bool);')
        self.tdSql.execute(f'create table downsampling_ct1 using downsampling_stb tags(10, 10.1, "Beijing", True);')
        # self.tdSql.execute(f'create table ownsampling_ct2 using downsampling_stb tags(20, 20.2, "TIANJIN", False);')
        # self.tdSql.execute(f'create table ownsampling_ct3 using downsampling_stb tags(30, 30.3, "HeBei", False);')
        
        self.tdSql.execute(f'create table if not exists scalar_stb (ts timestamp, c1 int, c2 double, c3 binary(20), c4 nchar(20), c5 nchar(20)) tags (t1 int);')
        self.tdSql.execute(f'create table scalar_ct1 using scalar_stb tags(10);')
        # self.tdSql.execute(f'create table scalar_ct2 using scalar_stb tags(-20);')
        # self.tdSql.execute(f'create table scalar_ct3 using scalar_stb tags(0);')



    def write_latency(self, msg):
        with open(self.latency_log, 'a') as f:
            f.write(f'{msg}\n')

    def check_stream_res(self, sql, expected_res):
        self.tdSql.query(sql)
        latency = 0
        if self.tdSql.query_row == expected_res:
            self.write_latency(latency)

        while self.tdSql.query_row != expected_res:
            self.tdSql.query(sql)
            if latency < 2:
                latency += 0.01
                time.sleep(0.01)
            else:
                self.tdSql.checkEqual(self.tdSql.query_row, expected_res)
            if self.tdSql.query_row == expected_res:
                self.write_latency(latency)
                return latency
    
    def check_query_data(self, sql1, sql2):
        self.tdSql.query(sql1)
        res1 = self.tdSql.query_data
        self.tdSql.query(sql2)
        res2 = self.tdSql.query_data
        self.tdSql.checkEqual(res1, res2)

    def check_stream_field_type(self, sql, input_function):
        self.tdSql.query(sql)
        res = self.tdSql.query_data
        if input_function in ["acos", "asin", "atan", "cos", "log", "pow", "sin", "sqrt", "tan"]:
            self.tdSql.checkEqual(res[1][1], "DOUBLE")
            self.tdSql.checkEqual(res[2][1], "DOUBLE")
        elif input_function in ["lower", "ltrim", "rtrim", "upper"]:
            self.tdSql.checkEqual(res[1][1], "VARCHAR")
            self.tdSql.checkEqual(res[2][1], "NCHAR")
            self.tdSql.checkEqual(res[3][1], "NCHAR")
        elif input_function in ["char_length", "length"]:
            self.tdSql.checkEqual(res[1][1], "INT")
            self.tdSql.checkEqual(res[2][1], "INT")
            self.tdSql.checkEqual(res[3][1], "INT")
        elif input_function in ["concat", "concat_ws"]:
            self.tdSql.checkEqual(self.tdSql.query_data[1][1], "VARCHAR")
            self.tdSql.checkEqual(self.tdSql.query_data[2][1], "VARCHAR")
            self.tdSql.checkEqual(self.tdSql.query_data[3][1], "NCHAR")
            self.tdSql.checkEqual(self.tdSql.query_data[4][1], "VARCHAR")
        elif input_function in ["substr"]:
            self.tdSql.checkEqual(res[1][1], "VARCHAR")
            self.tdSql.checkEqual(res[2][1], "VARCHAR")
            self.tdSql.checkEqual(res[3][1], "NCHAR")
            self.tdSql.checkEqual(res[4][1], "NCHAR")
        else:
            self.tdSql.checkEqual(res[1][1], "INT")
            self.tdSql.checkEqual(res[2][1], "DOUBLE")
            

    def check_stream(self, sql1, sql2, expected_count):
        self.check_stream_res(sql1, expected_count)
        self.check_query_data(sql1, sql2)

    def downsampling(self):
        self.case_name = sys._getframe().f_code.co_name
        self.write_latency(self.case_name)
        self.tdSql.execute(f'create stream downsampling_stream into output_downsampling_stb as select _wstartts AS start, min(c1), max(c2), sum(c1) from downsampling_stb interval(10m);')
        self.tdSql.execute(f'insert into downsampling_ct1 values (1653547828591, 100, 100.1, "Beijing", True);')
        self.tdSql.execute(f'insert into downsampling_ct1 values (1653547828591+1s, -100, -100.1, "Tianjin", False);')
        self.tdSql.execute(f'insert into downsampling_ct1 values (1653547828591+2s, 50, 50.3, "HeBei", False);')
        self.write_latency('sql: select * from output_downsampling_stb;')

        self.check_stream_res('select * from output_downsampling_stb;', 1)
        self.check_query_data('select start, `min(c1)`, `max(c2)`, `sum(c1)` from output_downsampling_stb;', 'select _wstartts AS start, min(c1), max(c2), sum(c1) from downsampling_stb interval(10m);')
        self.tdSql.execute(f'insert into downsampling_ct1 values (1653547828591+10m, 60, 60.3, "heilongjiang", True);')
        self.tdSql.execute(f'insert into downsampling_ct1 values (1653547828591+11m, 70, 70.3, "JiLin", True);')
        self.write_latency('sql: select * from output_downsampling_stb;')
        self.check_stream_res('select * from output_downsampling_stb;', 2)
        self.check_query_data('select start, `min(c1)`, `max(c2)`, `sum(c1)` from output_downsampling_stb;', 'select _wstartts AS start, min(c1), max(c2), sum(c1) from downsampling_stb interval(10m);')
        self.tdSql.execute(f'insert into downsampling_ct1 values (1653547828591+21m, 70, 70.3, "JiLin", True);')
        self.write_latency('sql: select * from output_downsampling_stb;')
        self.check_stream_res('select * from output_downsampling_stb;', 3)
        self.check_query_data('select start, `min(c1)`, `max(c2)`, `sum(c1)` from output_downsampling_stb;', 'select _wstartts AS start, min(c1), max(c2), sum(c1) from downsampling_stb interval(10m);')

    def scalar_function(self):
        # self.prepare_stream_data()
        self.case_name = sys._getframe().f_code.co_name
        self.write_latency(self.case_name)
        math_function_list = ["abs", "acos", "asin", "atan", "ceil", "cos", "floor", "log", "pow", "round", "sin", "sqrt", "tan"]
        string_function_list = ["lower", "ltrim", "rtrim", "substr", "upper"]
        # string_function_list = ["char_length", "concat", "concat_ws", "length", "lower", "ltrim", "rtrim", "substr", "upper"]
        for math_function in math_function_list:
            if math_function in ["log", "pow"]:
                self.tdSql.execute(f'create stream {math_function}_stream into output_{math_function}_stb as select ts, {math_function}(c1, 2), {math_function}(c2, 2), c3 from scalar_stb;')
            else:
                self.tdSql.execute(f'create stream {math_function}_stream into output_{math_function}_stb as select ts, {math_function}(c1), {math_function}(c2), c3 from scalar_stb;')
            self.check_stream_field_type(f"describe output_{math_function}_stb", math_function)
        for string_function in string_function_list:
            if string_function == "concat":
                self.tdSql.execute(f'create stream {string_function}_stream into output_{string_function}_stb as select ts, {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_stb;')
            elif string_function == "concat_ws":
                self.tdSql.execute(f'create stream {string_function}_stream into output_{string_function}_stb as select ts, {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_stb;')
            elif string_function == "substr":
                self.tdSql.execute(f'create stream {string_function}_stream into output_{string_function}_stb as select ts, {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_stb;')
            else:
                self.tdSql.execute(f'create stream {string_function}_stream into output_{string_function}_stb as select ts, {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_stb;')
            self.check_stream_field_type(f"describe output_{string_function}_stb", string_function)

        self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}, 100, 100.1, "beijing", "taos", "Taos");')
        self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}+1s, -50, -50.1, "tianjin", "taosdata", "Taosdata");')
        self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}+2s, 0, Null, "hebei", "TDengine", Null);')
        for math_function in math_function_list:
            if math_function == "log" or math_function == "pow":
                self.check_stream(f'select `{math_function}(c1, 2)`, `{math_function}(c2, 2)` from output_{math_function}_stb;', f'select {math_function}(c1, 2), {math_function}(c2, 2) from scalar_stb;', 3)
            else:
                self.check_stream(f'select `{math_function}(c1)`, `{math_function}(c2)` from output_{math_function}_stb;', f'select {math_function}(c1), {math_function}(c2) from scalar_stb;', 3)
        for string_function in string_function_list:
            if string_function == "concat":
                self.check_stream(f'select `{string_function}(c3, c4)`, `{string_function}(c3, c5)`, `{string_function}(c4, c5)`, `{string_function}(c3, c4, c5)` from output_{string_function}_stb;', f'select {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_stb;', 3)
            elif string_function == "concat_ws":
                self.check_stream(f'select `{string_function}("aND", c3, c4)`, `{string_function}("and", c3, c5)`, `{string_function}("And", c4, c5)`, `{string_function}("AND", c3, c4, c5)` from output_{string_function}_stb;', f'select {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_stb;', 3)
            elif string_function == "substr":
                self.check_stream(f'select `{string_function}(c3, 2)`, `{string_function}(c3, 2, 2)`, `{string_function}(c4, 5, 1)`, `{string_function}(c5, 3, 4)` from output_{string_function}_stb;', f'select {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_stb;', 3)
            else:
                self.check_stream(f'select `{string_function}(c3)`, `{string_function}(c4)`, `{string_function}(c5)` from output_{string_function}_stb;', f'select {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_stb;', 3)
        
        self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}+3s, -1, 1, "hebei", Null, "Bigdata");')
        for math_function in math_function_list:
            if math_function == "log" or math_function == "pow":
                self.check_stream(f'select `{math_function}(c1, 2)`, `{math_function}(c2, 2)` from output_{math_function}_stb;', f'select {math_function}(c1, 2), {math_function}(c2, 2) from scalar_stb;', 4)
            else:
                self.check_stream(f'select `{math_function}(c1)`, `{math_function}(c2)` from output_{math_function}_stb;', f'select {math_function}(c1), {math_function}(c2) from scalar_stb;', 4)
        
        for string_function in string_function_list:
            if string_function == "concat":
                self.check_stream(f'select `{string_function}(c3, c4)`, `{string_function}(c3, c5)`, `{string_function}(c4, c5)`, `{string_function}(c3, c4, c5)` from output_{string_function}_stb;', f'select {string_function}(c3, c4), {string_function}(c3, c5), {string_function}(c4, c5), {string_function}(c3, c4, c5) from scalar_stb;', 4)
            elif string_function == "concat_ws":
                self.check_stream(f'select `{string_function}("aND", c3, c4)`, `{string_function}("and", c3, c5)`, `{string_function}("And", c4, c5)`, `{string_function}("AND", c3, c4, c5)` from output_{string_function}_stb;', f'select {string_function}("aND", c3, c4), {string_function}("and", c3, c5), {string_function}("And", c4, c5), {string_function}("AND", c3, c4, c5) from scalar_stb;', 4)
            elif string_function == "substr":
                self.check_stream(f'select `{string_function}(c3, 2)`, `{string_function}(c3, 2, 2)`, `{string_function}(c4, 5, 1)`, `{string_function}(c5, 3, 4)` from output_{string_function}_stb;', f'select {string_function}(c3, 2), {string_function}(c3, 2, 2), {string_function}(c4, 5, 1), {string_function}(c5, 3, 4) from scalar_stb;', 4)
            else:
                self.check_stream(f'select `{string_function}(c3)`, `{string_function}(c4)`, `{string_function}(c5)` from output_{string_function}_stb;', f'select {string_function}(c3), {string_function}(c4), {string_function}(c5) from scalar_stb;', 4)

    def run(self) -> bool:
        self.prepare_stream_data()
        self.downsampling()
        self.scalar_function()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            vgroups check <jayden>: [TD-14991] : vgroups check;
            """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Database.Create, T.Write.TaoscSql.Database.Alter

