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
import time

class TestTs2920(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.rows_count = 10
        self.query_interval = 1
        self.time_out = 10

    def run(self):
        self.tdCom.createDb("test_ts2920")
        self.tdSql.execute('create stable test_ts2920.nginx (ts timestamp, short_request1 varchar(256), upstream_response double) tags (short_request varchar(256));')
        self.tdSql.execute(f'create table test_ts2920.ctb using test_ts2920.nginx tags ("/gdmall/auction_syn/{self.tdCom.get_long_name(self.tdCom.Boundary.STBNAME_MAX_LENGTH)}");')
        self.tdSql.execute('create stream nginx_avg ignore update 0 ignore expired 0 fill_history 1 into test_ts2920.nginx_avg_output subtable(short_request) as select _wstart as start_time, _wend as end_time, short_request, short_request1, avg(upstream_response) as avg_response from test_ts2920.nginx partition by short_request,short_request1 interval(1m);')
        for i in range(self.rows_count):
            self.tdSql.execute(f'insert into test_ts2920.ctb values (now+{i}m, "/gdmall/auction_syn/{self.tdCom.get_long_name(self.tdCom.Boundary.STBNAME_MAX_LENGTH)}", {i}.{i});')
        self.tdSql.execute('drop stream nginx_avg')
        self.tdSql.execute('alter stable test_ts2920.nginx_avg_output add column sum_response double')
        self.tdSql.execute('create stream nginx_avg ignore update 0 ignore expired 0 fill_history 1 into test_ts2920.nginx_avg_output subtable(short_request) as select _wstart as start_time, _wend as end_time, short_request, short_request1, avg(upstream_response) as avg_response, sum(upstream_response) as sum_response from test_ts2920.nginx partition by short_request,short_request1 interval(1m);')
        for i in range(self.rows_count, self.rows_count*2):
            self.tdSql.execute(f'insert into test_ts2920.ctb values (now+{i}m, "/gdmall/auction_syn/{self.tdCom.get_long_name(self.tdCom.Boundary.STBNAME_MAX_LENGTH)}", {i}.{i});')
        self.tdSql.query(f'select count(*) from test_ts2920.nginx_avg_output')
        time_counter = 1
        while self.tdSql.query_data[0][0] != self.rows_count*2:
            if time_counter < self.time_out:
                time_counter += self.query_interval
                time.sleep(self.query_interval)
                self.tdSql.query(f'select count(*) from test_ts2920.nginx_avg_output')
            else:
                return
        self.tdSql.checkEqual(self.tdSql.query_data[0][0], self.rows_count*2)

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            bug-ts2920
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write