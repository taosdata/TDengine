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

class TestTs5354(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.rows_count = 10
        self.query_interval = 1
        self.time_out = 10

    def run(self):
        self.tdCom.createDb("test_ts5354")
        self.tdSql.execute('create stable test_ts5354.st (ts timestamp, c1 int, c2 float) tags(groupid int);')
        self.tdSql.execute('create table test_ts5354.ct1 using test_ts5354.st tags (1);')
        self.tdSql.execute('create table test_ts5354.ct2 using test_ts5354.st tags (2);')
        self.tdSql.execute('create stream s1 trigger at_once fill_history 0 watermark 0d ignore update 0 ignore expired 0 into test_ts5354.s1_st as select count(*) from test_ts5354.st interval(5m);')
        self.tdSql.execute('insert into ct1 values("2024-09-05 13:49:01.000", 1, 1.1)("2024-09-06 13:49:02.000", 2, 2.2);')
        for i in range(1000):
            self.tdSql.execute('insert into test_ts5354.ct1 values("2024-09-05 13:49:01.000", 1, 1.1)')
            time.sleep(0.5)

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            bug-ts5354
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write