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
        self.tdSql.execute(f'create table ownsampling_ct1 using downsampling_stb tags(10, 10.1, "beijing", True);')
        self.tdSql.execute(f'create table ownsampling_ct2 using downsampling_stb tags(20, 20.2, "tianjin", False);')
        self.tdSql.execute(f'create table ownsampling_ct3 using downsampling_stb tags(30, 30.3, "hebei", False);')
        
        self.tdSql.execute(f'create table if not exists scalar_stb (ts timestamp, c1 int, c2 double, c3 binary(20)) tags (t1 int);')
        self.tdSql.execute(f'create table scalar_ct1 using scalar_stb tags(10);')
        self.tdSql.execute(f'create table scalar_ct2 using scalar_stb tags(-20);')
        self.tdSql.execute(f'create table scalar_ct3 using scalar_stb tags(0);')



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

    def downsampling(self):
        self.case_name = sys._getframe().f_code.co_name
        self.write_latency(self.case_name)
        self.prepare_stream_data()
        self.tdSql.execute(f'create stream downsampling_stream into output_downsampling_stb as select _wstartts AS start, min(c1), max(c2), sum(c1) from downsampling_stb interval(10m);')
        self.tdSql.execute(f'insert into ownsampling_ct1 values ({self.date_time}, 100, 100.1, "beijing", True);')
        self.tdSql.execute(f'insert into ownsampling_ct1 values ({self.date_time}+1s, -100, -100.1, "tianjin", False);')
        self.tdSql.execute(f'insert into ownsampling_ct1 values ({self.date_time}+2s, 50, 50.3, "hebei", False);')
        self.write_latency('sql: select * from output_downsampling_stb;')
        self.check_stream_res('select * from output_downsampling_stb;', 1)
        self.check_query_data('select start, `min(c1)`, `max(c2)`, `sum(c1)` from output_downsampling_stb;', 'select _wstartts AS start, min(c1), max(c2), sum(c1) from downsampling_stb interval(10m);')
        self.tdSql.execute(f'insert into ownsampling_ct1 values ({self.date_time}+10m, 60, 60.3, "heilongjiang", True);')
        self.tdSql.execute(f'insert into ownsampling_ct1 values ({self.date_time}+11m, 70, 70.3, "jilin", True);')
        self.write_latency('sql: select * from output_downsampling_stb;')
        self.check_stream_res('select * from output_downsampling_stb;', 2)
        self.check_query_data('select start, `min(c1)`, `max(c2)`, `sum(c1)` from output_downsampling_stb;', 'select _wstartts AS start, min(c1), max(c2), sum(c1) from downsampling_stb interval(10m);')
        self.tdSql.execute(f'insert into ownsampling_ct1 values ({self.date_time}+21m, 70, 70.3, "jilin", True);')
        self.write_latency('sql: select * from output_downsampling_stb;')
        self.check_stream_res('select * from output_downsampling_stb;', 3)
        self.check_query_data('select start, `min(c1)`, `max(c2)`, `sum(c1)` from output_downsampling_stb;', 'select _wstartts AS start, min(c1), max(c2), sum(c1) from downsampling_stb interval(10m);')

    def scalar_function(self):
        # ABS
        # self.prepare_stream_data()
        self.tdSql.execute(f'create stream scalar_stream into output_scalar_stb as select ts, abs(c1) a1 , abs(c2) a2 from scalar_stb;')
        self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}, 100, 100.1, "beijing");')
        self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}+1s, -50, -50.1, "tianjin");')
        self.tdSql.execute(f'insert into scalar_ct1 values ({self.date_time}+2s, 0, 0, "hebei");')


    def run(self) -> bool:
        self.downsampling()
        # self.scalar_function()

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

