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

from frame import etool
from frame.etool import *
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame.common import *

class TDTestCase(TBase):
    updatecfgDict = {
        "slowLogScope": "none"
    }

    def insert_data(self):
        tdLog.printNoPrefix("==========step1:create table")

        tdSql.execute("drop database if exists test")
        tdSql.execute("create database test keep 36500")
        tdSql.execute("use test")
        tdSql.execute("create table sta(ts timestamp, f int, g int) tags (tg1 int, tg2 int, tg3 int);")
        tdSql.execute("create table stb(ts timestamp, f int, g int) tags (tg1 int, tg2 int, tg3 int);")
        tdSql.query("select today();")
        today_ts = tdSql.res[0][0].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        tdSql.query("select today() + 1d;")
        tomorrow_ts = tdSql.res[0][0].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        tdLog.printNoPrefix("==========step2:insert data")
        tdSql.execute(f"insert into a1 using sta tags(1, 1, 1) values('{today_ts}',         101, 1011);")
        tdSql.execute(f"insert into a1 using sta tags(1, 1, 1) values('{today_ts}' + 1s,    102, 1012);")
        tdSql.execute(f"insert into a1 using sta tags(1, 1, 1) values('{tomorrow_ts}',      103, 1013);")
        tdSql.execute(f"insert into a1 using sta tags(1, 1, 1) values('{tomorrow_ts}' + 2s, 104, 1014);")

        tdSql.execute(f"insert into a2 using sta tags(1, 2, 2) values('{today_ts}',         201, 2011);")
        tdSql.execute(f"insert into a2 using sta tags(1, 2, 2) values('{today_ts}' + 1s,    202, 2012);")
        tdSql.execute(f"insert into a2 using sta tags(1, 2, 2) values('{tomorrow_ts}',      203, 2013);")
        tdSql.execute(f"insert into a2 using sta tags(1, 2, 2) values('{tomorrow_ts}' + 3s, 204, 2014);")

        tdSql.execute(f"insert into b1 using stb tags(1, 1, 3) values('{today_ts}',         301, 3011);")
        tdSql.execute(f"insert into b1 using stb tags(1, 1, 3) values('{today_ts}' + 1s,    302, 3012);")
        tdSql.execute(f"insert into b1 using stb tags(1, 1, 3) values('{tomorrow_ts}',      303, 3013);")
        tdSql.execute(f"insert into b1 using stb tags(1, 1, 3) values('{tomorrow_ts}' + 2s, 304, 3014);")

        tdSql.execute(f"insert into b2 using stb tags(1, 2, 4) values('{today_ts}',         401, 4011);")
        tdSql.execute(f"insert into b2 using stb tags(1, 2, 4) values('{today_ts}' + 1s,    402, 4012);")
        tdSql.execute(f"insert into b2 using stb tags(1, 2, 4) values('{tomorrow_ts}',      403, 4013);")
        tdSql.execute(f"insert into b2 using stb tags(1, 2, 4) values('{tomorrow_ts}' + 3s, 404, 4014);")

    def replace_string(self, input_file, output_file, old_str, new_str):
        print("当前工作目录:", os.getcwd())
        script_dir = os.path.dirname(os.path.abspath(__file__))
        with open(f"{script_dir}/joinConst/{input_file}", 'r') as f_in, open(f"{script_dir}/joinConst/{output_file}", 'w') as f_out:
            for line in f_in:
                modified_line = line.replace(old_str, new_str)
                f_out.write(modified_line)

    def test_normal_case(self, testCase):
        self.replace_string(f'{testCase}.in', f'{testCase}.in.today_', '__today__', 'today()')
        # read sql from .sql file and execute
        self.sqlFile = etool.curFile(__file__, f"joinConst/{testCase}.in.today_")
        self.ansFile = etool.curFile(__file__, f"joinConst/{testCase}.today_.csv")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

        self.replace_string(f'{testCase}.in', f'{testCase}.in.today', '__today__', 'today')
        # read sql from .sql file and execute
        self.sqlFile = etool.curFile(__file__, f"joinConst/{testCase}.in.today")
        self.ansFile = etool.curFile(__file__, f"joinConst/{testCase}.today.csv")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

#        self.replace_string(f'{testCase}.in', f'{testCase}.in.now_', '__today__', 'now()')
#        # read sql from .sql file and execute
#        self.sqlFile = etool.curFile(__file__, f"joinConst/{testCase}.in.now_")
#        self.ansFile = etool.curFile(__file__, f"joinConst/{testCase}.now_.csv")
#        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)
#
#        self.replace_string(f'{testCase}.in', f'{testCase}.in.now', '__today__', 'now')
#        # read sql from .sql file and execute
#        self.sqlFile = etool.curFile(__file__, f"joinConst/{testCase}.in.now")
#        self.ansFile = etool.curFile(__file__, f"joinConst/{testCase}.now.csv")
#        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)
#
#        self.replace_string(f'{testCase}.in', f'{testCase}.in.today_ts', '__today__', f'{today_ts}')
#        # read sql from .sql file and execute
#        self.sqlFile = etool.curFile(__file__, f"joinConst/{testCase}.in.today_ts")
#        self.ansFile = etool.curFile(__file__, f"joinConst/{testCase}.today_ts.csv")
#        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)
#
#        self.replace_string(f'{testCase}.in', f'{testCase}.in.tomorrow_ts', '__today__', f'{tomorrow_ts}')
#        # read sql from .sql file and execute
#        self.sqlFile = etool.curFile(__file__, f"joinConst/{testCase}.in.tomorrow_ts")
#        self.ansFile = etool.curFile(__file__, f"joinConst/{testCase}.tomorrow_ts.csv")
#        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

    def test_abnormal_case(self):
        tdLog.info("test abnormal case.")
        tdSql.error("select interp(c1) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', -1s) every(2s) fill(prev, 99);")

    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        self.insert_data()
        self.test_normal_case("inner")
        self.test_abnormal_case()

        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
