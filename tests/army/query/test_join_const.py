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

    today_ts = ""
    tomorrow_ts = ""

    def insert_data(self):
        tdLog.printNoPrefix("==========step1:create table")

        tdSql.execute("drop database if exists test")
        tdSql.execute("create database test keep 36500")
        tdSql.execute("use test")
        tdSql.execute("create table sta(ts timestamp, f int, g int) tags (tg1 int, tg2 int, tg3 int);")
        tdSql.execute("create table stb(ts timestamp, f int, g int) tags (tg1 int, tg2 int, tg3 int);")
        tdSql.query("select today();")
        self.today_ts = tdSql.res[0][0].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        self.today_date = tdSql.res[0][0].strftime('%Y-%m-%d')
        tdSql.query("select today() + 1d;")
        self.tomorrow_ts = tdSql.res[0][0].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        self.tomorrow_date = tdSql.res[0][0].strftime('%Y-%m-%d')

        tdLog.printNoPrefix("==========step2:insert data")
        tdSql.execute(f"insert into a1 using sta tags(1, 1, 1) values('{self.today_ts}',         101, 1011);")
        tdSql.execute(f"insert into a1 using sta tags(1, 1, 1) values('{self.today_ts}' + 1s,    102, 1012);")
        tdSql.execute(f"insert into a1 using sta tags(1, 1, 1) values('{self.tomorrow_ts}',      103, 1013);")
        tdSql.execute(f"insert into a1 using sta tags(1, 1, 1) values('{self.tomorrow_ts}' + 2s, 104, 1014);")

        tdSql.execute(f"insert into a2 using sta tags(1, 2, 2) values('{self.today_ts}',         201, 2011);")
        tdSql.execute(f"insert into a2 using sta tags(1, 2, 2) values('{self.today_ts}' + 1s,    202, 2012);")
        tdSql.execute(f"insert into a2 using sta tags(1, 2, 2) values('{self.tomorrow_ts}',      203, 2013);")
        tdSql.execute(f"insert into a2 using sta tags(1, 2, 2) values('{self.tomorrow_ts}' + 3s, 204, 2014);")

        tdSql.execute(f"insert into b1 using stb tags(1, 1, 3) values('{self.today_ts}',         301, 3011);")
        tdSql.execute(f"insert into b1 using stb tags(1, 1, 3) values('{self.today_ts}' + 1s,    302, 3012);")
        tdSql.execute(f"insert into b1 using stb tags(1, 1, 3) values('{self.tomorrow_ts}',      303, 3013);")
        tdSql.execute(f"insert into b1 using stb tags(1, 1, 3) values('{self.tomorrow_ts}' + 2s, 304, 3014);")

        tdSql.execute(f"insert into b2 using stb tags(1, 2, 4) values('{self.today_ts}',         401, 4011);")
        tdSql.execute(f"insert into b2 using stb tags(1, 2, 4) values('{self.today_ts}' + 1s,    402, 4012);")
        tdSql.execute(f"insert into b2 using stb tags(1, 2, 4) values('{self.tomorrow_ts}',      403, 4013);")
        tdSql.execute(f"insert into b2 using stb tags(1, 2, 4) values('{self.tomorrow_ts}' + 3s, 404, 4014);")

    def replace_string(self, input_file, output_file, old_str, new_str):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        with open(f"{script_dir}/joinConst/{input_file}", 'r') as f_in, open(f"{script_dir}/joinConst/{output_file}", 'w') as f_out:
            for line in f_in:
                modified_line = line.replace(old_str, new_str)
                f_out.write(modified_line)

    def replace_string2(self, input_file, output_file, old_str, new_str):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        with open(f"{script_dir}/joinConst/{input_file}", 'r') as f_in, open(f"{script_dir}/joinConst/{output_file}", 'w', newline="\r\n") as f_out:
            for line in f_in:
                modified_line = line.replace(old_str, new_str)
                f_out.write(modified_line)

    def test_today_case(self, testCase):
        tdLog.printNoPrefix(f"==========step:{testCase} + today() test")
        self.replace_string(f'{testCase}.today.in', f'{testCase}.today_.in.tmp1', '__today__', 'today()')
        self.replace_string(f'{testCase}.today_.csv', f'{testCase}.today_.csv.tmp1', '__today__', f'{self.today_date}')
        self.replace_string2(f'{testCase}.today_.csv.tmp1', f'{testCase}.today_.csv.tmp2', '__tomorrow__', f'{self.tomorrow_date}')
        # read sql from .sql file and execute
        self.sqlFile = etool.curFile(__file__, f"joinConst/{testCase}.today_.in.tmp1")
        self.ansFile = etool.curFile(__file__, f"joinConst/{testCase}.today_.csv.tmp2")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

        tdLog.printNoPrefix(f"==========step:{testCase} + today test")
        self.replace_string(f'{testCase}.today.in', f'{testCase}.today.in.tmp1', '__today__', 'today')
        self.replace_string(f'{testCase}.today.csv', f'{testCase}.today.csv.tmp1', '__today__', f'{self.today_date}')
        self.replace_string2(f'{testCase}.today.csv.tmp1', f'{testCase}.today.csv.tmp2', '__tomorrow__', f'{self.tomorrow_date}')
        # read sql from .sql file and execute
        self.sqlFile = etool.curFile(__file__, f"joinConst/{testCase}.today.in.tmp1")
        self.ansFile = etool.curFile(__file__, f"joinConst/{testCase}.today.csv.tmp2")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

    def test_now_case(self, testCase):
        tdLog.printNoPrefix(f"==========step:{testCase} + now() test")
        self.replace_string(f'{testCase}.now.in', f'{testCase}.now_.in.tmp1', '__const__', 'now()')
        self.replace_string(f'{testCase}.now_.csv', f'{testCase}.now_.csv.tmp1', '__today__', f'{self.today_date}')
        self.replace_string2(f'{testCase}.now_.csv.tmp1', f'{testCase}.now_.csv.tmp2', '__tomorrow__', f'{self.tomorrow_date}')
        # read sql from .sql file and execute
        self.sqlFile = etool.curFile(__file__, f"joinConst/{testCase}.now_.in.tmp1")
        self.ansFile = etool.curFile(__file__, f"joinConst/{testCase}.now_.csv.tmp2")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

        tdLog.printNoPrefix(f"==========step:{testCase} + now test")
        self.replace_string(f'{testCase}.now.in', f'{testCase}.now.in.tmp1', '__const__', 'now')
        self.replace_string(f'{testCase}.now.csv', f'{testCase}.now.csv.tmp1', '__today__', f'{self.today_date}')
        self.replace_string2(f'{testCase}.now.csv.tmp1', f'{testCase}.now.csv.tmp2', '__tomorrow__', f'{self.tomorrow_date}')
        # read sql from .sql file and execute
        self.sqlFile = etool.curFile(__file__, f"joinConst/{testCase}.now.in.tmp1")
        self.ansFile = etool.curFile(__file__, f"joinConst/{testCase}.now.csv.tmp2")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

    def test_constts_case(self, testCase):
        tdLog.printNoPrefix(f"==========step:{testCase} + ts:{self.today_ts} test")
        self.replace_string(f'{testCase}.constts.in', f'{testCase}.constts.in.tmp1', '__today__', f'"{self.today_ts}"')
        # read sql from .sql file and execute
        self.sqlFile = etool.curFile(__file__, f"joinConst/{testCase}.constts.in.tmp1")
        self.ansFile = etool.curFile(__file__, f"joinConst/{testCase}.today_ts.csv")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

        tdLog.printNoPrefix(f"==========step:{testCase} + ts:{self.tomorrow_ts} test")
        self.replace_string(f'{testCase}.constts.in', f'{testCase}.constts.in.tmp2', '__today__', f'"{self.tomorrow_ts}"')
        # read sql from .sql file and execute
        self.sqlFile = etool.curFile(__file__, f"joinConst/{testCase}.constts.in.tmp2")
        self.ansFile = etool.curFile(__file__, f"joinConst/{testCase}.tomorrow_ts.csv")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

    def test_nocheck_case(self):
        tdLog.printNoPrefix(f"==========step:nocheck test")
        tdSql.execute("select * from a1 a join (select now() as ts1, ts, f, g, 'a' c from b1) b on b.ts = a.ts;")
        #tdSql.execute("select * from a1 a join (select now as ts1, ts, f, g, 'a' c from b1) b on b.ts = a.ts;")
        #select b.* from a1 a join (select __const__ as ts1, ts, f, g, 'a' c from b1) b on b.ts = a.ts;
        #select * from a1 a join (select __const__ as ts1, ts, f, g, 'a' from b1) b on a.ts = b.ts;
        #select b.* from a1 a join (select __const__ as ts1, ts, f, g, 'a' from b1) b on a.ts = b.ts;
        #select * from (select __const__ as ts1, ts, f, g, 'a' from b1) b join a1 a  on a.ts = b.ts;
        #select b.* from (select __const__ as ts1, ts, f, g, 'a' from b1) b join a1 a  on a.ts = b.ts;
        #select * from a1 a , (select __const__ as ts1, ts, f, g, 'a' from b1) b where a.ts = b.ts;
        #select b.* from a1 a , (select __const__ as ts1, ts, f, g, 'a' from b1) b where a.ts = b.ts;
        #select * from (select __const__ as ts1, ts, f, g, 'a' from b1) b , a1 a  where a.ts = b.ts;
        #select b.* from (select __const__ as ts1, ts, f, g, 'a' from b1) b , a1 a  where a.ts = b.ts;

    def test_abnormal_case(self):
        tdLog.printNoPrefix(f"==========step:abnormal case test")
        tdSql.error("select interp(c1) from test.td32727 range('2020-02-01 00:00:00.000', '2020-02-01 00:00:30.000', -1s) every(2s) fill(prev, 99);")

    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        self.insert_data()

        self.test_today_case("inner")
        self.test_now_case("inner")
        #self.test_constts_case("inner")

        self.test_today_case("left_outer")

        #self.test_abnormal_case()

        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
