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

class TestTs2919(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def run(self):
        self.tdCom.createDb("test_ts2919")
        self.tdSql.execute('create stable test_ts2919.nginx (ts timestamp, short_request1 varchar(256), upstream_response double) tags (short_request varchar(256));')
        for i in range(20):
            self.tdSql.execute(f'create table test_ts2919.ctb{i} using test_ts2919.nginx tags ("/gdmall/auction_syn/{self.tdCom.get_long_name(self.tdCom.Boundary.STBNAME_MAX_LENGTH)}");')
        self.tdSql.execute('create stream nginx_avg into test_ts2919.nginx_avg_output subtable(short_request) as select _wstart as start_time, _wend as end_time, short_request, short_request1, avg(upstream_response) as avg_response from test_ts2919.nginx partition by short_request,short_request1 interval(1m);')
        for i in range(25):
            for j in range(20):
                self.tdSql.execute(f'insert into test_ts2919.ctb{j} values (now+{i}m, "/gdmall/auction_syn/{self.tdCom.get_long_name(self.tdCom.Boundary.STBNAME_MAX_LENGTH)}", {i}.{i});')
        self.tdSql.query(f'select * from test_ts2919.nginx_avg_output')

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            bug-ts2919
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write