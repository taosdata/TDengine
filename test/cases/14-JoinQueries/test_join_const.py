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
from new_test_framework.utils import tdLog, tdSql, etool, tdCom

class TestJoinConst:

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
        self.today_ts = tdSql.queryResult[0][0].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        self.today_date = tdSql.queryResult[0][0].strftime('%Y-%m-%d')
        tdSql.query("select today() + 1d;")
        self.tomorrow_ts = tdSql.queryResult[0][0].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        self.tomorrow_date = tdSql.queryResult[0][0].strftime('%Y-%m-%d')

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

        tdSql.execute(f"create view view1 as select today() as ts1, * from a1;")

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

    def run_today_case(self, testCase):
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

    def run_now_case(self, testCase):
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

    def run_constts_case(self, testCase):
        tdLog.printNoPrefix(f"==========step:{testCase} + ts:{self.today_ts} test")
        self.replace_string(f'{testCase}.constts.in', f'{testCase}.constts.in.tmp1', '__today__', f'"{self.today_ts}"')
        self.replace_string(f'{testCase}.constts.csv', f'{testCase}.constts.csv.tmp1', '__today__', f'{self.today_date}')
        self.replace_string2(f'{testCase}.constts.csv.tmp1', f'{testCase}.constts.csv.tmp2', '__tomorrow__', f'{self.tomorrow_date}')
        # read sql from .sql file and execute
        self.sqlFile = etool.curFile(__file__, f"joinConst/{testCase}.constts.in.tmp1")
        self.ansFile = etool.curFile(__file__, f"joinConst/{testCase}.constts.csv.tmp2")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

        tdLog.printNoPrefix(f"==========step:{testCase} + ts:{self.tomorrow_ts} test")
        self.replace_string(f'{testCase}.constts.in', f'{testCase}.constts2.in.tmp2', '__today__', f'"{self.tomorrow_ts}"')
        self.replace_string(f'{testCase}.constts2.csv', f'{testCase}.constts2.csv.tmp1', '__today__', f'{self.today_date}')
        self.replace_string2(f'{testCase}.constts2.csv.tmp1', f'{testCase}.constts2.csv.tmp2', '__tomorrow__', f'{self.tomorrow_date}')
        # read sql from .sql file and execute
        self.sqlFile = etool.curFile(__file__, f"joinConst/{testCase}.constts2.in.tmp2")
        self.ansFile = etool.curFile(__file__, f"joinConst/{testCase}.constts2.csv.tmp2")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

        tdLog.printNoPrefix(f"==========step:{testCase} + ts:{self.today_ts} + 1s test")
        self.replace_string(f'{testCase}.constts.in', f'{testCase}.constts3.in.tmp1', '__today__', f'"{self.today_ts}" + 1s')
        self.replace_string(f'{testCase}.constts3.csv', f'{testCase}.constts3.csv.tmp1', '__today__', f'{self.today_date}')
        self.replace_string2(f'{testCase}.constts3.csv.tmp1', f'{testCase}.constts3.csv.tmp2', '__tomorrow__', f'{self.tomorrow_date}')
        # read sql from .sql file and execute
        self.sqlFile = etool.curFile(__file__, f"joinConst/{testCase}.constts3.in.tmp1")
        self.ansFile = etool.curFile(__file__, f"joinConst/{testCase}.constts3.csv.tmp2")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)


    def run_nocheck_case(self):
        tdLog.printNoPrefix(f"==========step:nocheck test")
        tdSql.execute("select * from a1 a join (select now() as ts1, ts, f, g, 'a' c from b1) b on b.ts = a.ts;")
        tdSql.execute("select * from a1 a join (select now as ts1, ts, f, g, 'a' c from b1) b on b.ts = a.ts;")
        tdSql.execute("select b.* from a1 a join (select today as ts1, ts, f, g, 'a' c from b1) b on b.ts = a.ts;")
        tdSql.execute("select * from a1 a join (select today as ts1, ts, f, g, 'a' from b1) b on a.ts = b.ts;")
        tdSql.execute("select b.* from a1 a join (select today as ts1, ts, f, g, 'a' from b1) b on a.ts = b.ts;")
        tdSql.execute("select * from (select today as ts1, ts, f, g, 'a' from b1) b join a1 a  on a.ts = b.ts;")
        tdSql.execute("select b.* from (select today as ts1, ts, f, g, 'a' from b1) b join a1 a  on a.ts = b.ts;")
        tdSql.execute("select * from a1 a , (select today as ts1, ts, f, g, 'a' from b1) b where a.ts = b.ts;")
        tdSql.execute("select b.* from a1 a , (select today as ts1, ts, f, g, 'a' from b1) b where a.ts = b.ts;")
        tdSql.execute("select * from (select today as ts1, ts, f, g, 'a' from b1) b , a1 a  where a.ts = b.ts;")
        tdSql.execute("select b.* from (select today as ts1, ts, f, g, 'a' from b1) b , a1 a  where a.ts = b.ts;")
        tdSql.execute("select b.ts from (select now as ts1, ts, f, g, 'a' c from a1 order by f) a , (select now as ts1, ts, f, g, 'a' c from b1) b where b.ts1 = a.ts1 and a.ts = b.ts;")
        tdSql.execute("select b.ts from (select today as ts1, ts, f, g, 'a' c from a1) a join (select today as ts1, ts, f, g, 'a' c from b1) b on b.ts = a.ts and a.ts = b.ts1;")
        tdSql.execute("select b.ts from (select today as ts1, ts, f, g, 'a' c from a1) a join (select today as ts1, ts, f, g, 'a' c from b1) b on b.ts1 = a.ts1 and a.ts = b.ts;")
        tdSql.execute("select tb.val,tb.tg2,ta.* from (select now() ts, last(f) val, tg2 from stb where tg2 >= 0 partition by tg2 interval(1s) order by ts desc) tb left join (select last(ts) as `ts`,last(f) as `val`,tg2 from sta where tg2>0 partition by tg2 interval(1s) order by ts) ta on ta.ts=tb.ts and ta.tg2=tb.tg2 and tb.ts=now() where tb.ts=now() order by tb.val;")

    def run_abnormal_case(self):
        tdLog.printNoPrefix(f"==========step:abnormal case test")
        tdSql.error(f"select * from a1 a join (select '{self.today_ts}' as ts from b1) b on a.ts = b.ts;")
        tdSql.error(f"select * from a1 a join (select '{self.today_ts}' + 1s as ts from b1) b on a.ts = b.ts;")
        tdSql.error(f"select b.ts from (select now() as ts1, ts, f, g, 'a' c from a1 order by f) a join (select now() as ts1, ts, f, g, 'a' c from b1) b on b.ts = a.ts and a.ts = b.ts1;")
        tdSql.error(f"select b.ts from (select today() as ts1, ts, f, g, 'a' c from a1 order by f) a join (select today() as ts1, ts, f, g, 'a' c from b1) b on b.ts = a.ts and a.ts = b.ts1;")
        tdSql.error(f"select b.ts from (select today as ts1, ts, f, g, 'a' c from a1 order by f) a , (select today as ts1, ts, f, g, 'a' c from b1) b where b.ts = a.ts and a.ts = b.ts1;")
        tdSql.error(f"select * from a1 a left asof join (select now as ts1, ts, f, g, 'a' c from b1) b on b.ts = a.ts;")
        tdSql.error(f"select * from a1 a left window join (select now as ts1, ts, f, g, 'a' c from b1) b window_offset(-1s, 1s);")
        tdSql.error(f"select * from a1 a join (select timestamp '{self.today_ts}' + 1d as ts, f, g, 'a' from b1) b on timetruncate(a.ts + 1s, 1d) = timetruncate(b.ts, 1d);")
        tdSql.error(f"select * from a1 a join (select timestamp '{self.today_ts}' + 1d as ts, f, g, 'a' from b1) b on a.ts + 1s = timetruncate(b.ts, 1d);")
        tdSql.error(f"select t1.* from sta t1, (select _wstart as ts, count(*) from sta partition by tbname interval(1d)) t2 where t1.ts = t2.ts;")
        tdSql.error(f"select * from (select tg1, sum(f) s from sta group by tg1) t1, sta t2 where t1.tg1 = t2.tg1;")
        tdSql.error(f"select * from (select * from a1 order by f desc) a join (select today as ts1, * from b1 order by f) b on a.ts = b.ts;")
        tdSql.error(f"select * from (select * from a1 order by f) a join (select today as ts1, * from b1 order by f desc) b on a.ts = b.ts1;")
        tdSql.error(f"select * from (select * from a1 order by f desc) a join (select today as ts1, * from b1 order by f) b on a.ts = b.ts1;")
        tdSql.error(f"select * from a1 a join (select * from b1 order by f) b on a.ts = b.ts;")
        tdSql.error(f"select * from a1 a join (select today() as ts1, * from b1 order by f) b on a.ts = b.ts;")
        tdSql.error(f"select * from (select * from a1 order by ts) a join (select * from b1 order by f) b on a.ts = b.ts;")
        tdSql.error(f"select * from (select * from a1 order by ts desc) a join (select * from b1 order by f) b on a.ts = b.ts;")
        tdSql.error(f"select * from (select * from a1 order by f) a join (select * from b1 order by f) b on a.ts = b.ts;")
        tdSql.error(f"select * from (select * from a1 order by ts) a join (select today() as ts1, * from b1 order by f) b on a.ts = b.ts;")
        tdSql.error(f"select * from (select * from a1 order by ts desc) a join (select today() as ts1, * from b1 order by f) b on a.ts = b.ts;")
        tdSql.error(f"select * from (select * from a1 order by f desc) a join (select today() as ts1, * from b1 order by ts) b on a.ts = b.ts;")
        tdSql.error(f"select * from (select * from a1 order by f desc) a join (select today() as ts1, * from b1 order by ts desc) b on a.ts = b.ts;")
        tdSql.error(f"select * from (select * from a1 order by f desc) a join (select today() as ts1, * from b1 order by f) b on a.ts = b.ts;")
        tdSql.error(f"select * from (select * from a1 order by f desc) a join (select today() as ts1, * from b1 order by ts) b on a.ts = b.ts1;")
        tdSql.error(f"select * from (select * from a1 order by f desc) a join (select today() as ts1, * from b1 order by ts desc) b on a.ts = b.ts1;")
        tdSql.error(f"select * from (select * from a1 order by f desc) a join (select today() as ts1, * from b1 order by f) b on a.ts = b.ts1;")
        tdSql.error(f"select * from (select today() as ts1, * from a1 order by ts) a join (select today() as ts1, * from b1 order by f) b on a.ts = b.ts;")
        tdSql.error(f"select * from (select today() as ts1, * from a1 order by ts desc) a join (select today() as ts1, * from b1 order by f) b on a.ts = b.ts;")
        tdSql.error(f"select * from (select today() as ts1, * from a1 order by f desc) a join (select today() as ts1, * from b1 order by ts) b on a.ts = b.ts;")
        tdSql.error(f"select * from (select today() as ts1, * from a1 order by f desc) a join (select today() as ts1, * from b1 order by ts desc) b on a.ts = b.ts;")
        tdSql.error(f"select * from (select today() as ts1, * from a1 order by f desc) a join (select today() as ts1, * from b1 order by f) b on a.ts = b.ts;")
        tdSql.error(f"select * from (select today() as ts1, * from a1 order by ts) a join (select today() as ts1, * from b1 order by f) b on a.ts1 = b.ts;")
        tdSql.error(f"select * from (select today() as ts1, * from a1 order by ts desc) a join (select today() as ts1, * from b1 order by f) b on a.ts1 = b.ts;")
        tdSql.error(f"select * from (select today() as ts1, * from a1 order by ts desc) a join (select today() as ts1, * from b1 order by f) b on a.ts1 = b.ts;")

    def run_others_case(self, testCase):
        tdLog.printNoPrefix(f"==========step:{testCase} test")
        self.replace_string(f'{testCase}.in', f'{testCase}.in.tmp1', '__today__', f'{self.today_date}')
        self.replace_string(f'{testCase}.in.tmp1', f'{testCase}.in.tmp2', '__tomorrow__', f'{self.tomorrow_date}')
        self.replace_string(f'{testCase}.csv', f'{testCase}.csv.tmp1', '__today__', f'{self.today_date}')
        self.replace_string2(f'{testCase}.csv.tmp1', f'{testCase}.csv.tmp2', '__tomorrow__', f'{self.tomorrow_date}')
        # read sql from .sql file and execute
        self.sqlFile = etool.curFile(__file__, f"joinConst/{testCase}.in.tmp2")
        self.ansFile = etool.curFile(__file__, f"joinConst/{testCase}.csv.tmp2")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

    def test_join_const(self):
        """Join full data types
        
        1. Create stable sta/stb
        2. Create child table a1/a2 from sta, b1/b2 from stb
        3. Insert 4 rows data into a1/a2, b1/b2
        4. Read query sql from .in file and execute
        5. Validate the query result with expected .csv file
        6. File .in sql include now()/today() and constant timestamps
        7. File .in sql include inner/outer/semi/anti join
        8. Check abnormal cases
        

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-22 Alex Duan Migrated from uncatalog/army/query/test_compare.py

        """
        tdLog.debug(f"start to excute {__file__}")

        self.insert_data()

        self.run_today_case("inner")
        self.run_now_case("inner")
        self.run_constts_case("inner")

        self.run_today_case("left_outer")
        self.run_now_case("left_outer")
        self.run_today_case("right_outer")

        self.run_today_case("full_outer")
        self.run_today_case("left_semi")
        self.run_today_case("left_anti")

        self.run_others_case("others")

        self.run_nocheck_case()
        self.run_abnormal_case()

        tdLog.success(f"{__file__} successfully executed")
