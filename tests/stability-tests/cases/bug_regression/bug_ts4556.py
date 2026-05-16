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
from taostest.components import TaosD
import time

class TestTs4556(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def run(self):
        self.tdCom.createDb(dbname="power")
        self.tdSql.execute('CREATE STABLE meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);')
        self.tdSql.execute('CREATE TABLE d1001 USING meters TAGS ("d1", 1);')
        self.tdSql.execute('CREATE TABLE d1002 USING meters TAGS ("d2", 2);')
        self.tdSql.execute('CREATE TABLE d1003 USING meters TAGS ("d3", 3);')
        
        self.tdSql.execute('create stream current_stream trigger at_once into current_stream_output_stb as select _wstart as wstart, _wend as wend, max(current) as max_current from meters where 1=1 and groupId = 3 interval (5s);')
        time.sleep(3)
        self.tdSql.execute('insert into d1001 values("2018-10-03 14:38:05.000", 10.30000, 219, 0.31000);')
        self.tdSql.execute('insert into d1001 values("2018-10-03 14:38:15.000", 12.60000, 218, 0.33000);')
        self.tdSql.execute('insert into d1001 values("2018-10-03 14:38:16.800", 12.30000, 221, 0.31000);')
        self.tdSql.execute('insert into d1002 values("2018-10-03 14:38:16.650", 10.30000, 218, 0.25000);')
        self.tdSql.execute('insert into d1003 values("2018-10-03 14:38:05.500", 11.80000, 221, 0.28000);')
        self.tdCom.check_query_data('select _wstart as wstart, _wend as wend, max(current) as max_current from meters where 1=1 and groupId = 3 interval (5s);', 'select wstart, wend, `max_current` from current_stream_output_stb;')
        self.tdSql.execute('insert into d1001 values("2018-10-04 14:38:05.000", 10.30000, 219, 0.31000);')
        self.tdSql.execute('insert into d1001 values("2018-10-05 14:38:15.000", 12.60000, 218, 0.33000);')
        self.tdSql.execute('insert into d1001 values("2018-10-06 14:38:16.800", 12.30000, 221, 0.31000);')
        self.tdSql.execute('insert into d1002 values("2018-10-07 14:38:16.650", 10.30000, 218, 0.25000);')
        self.tdSql.execute('insert into d1003 values("2018-10-08 14:38:05.500", 11.80000, 221, 0.28000);')
        self.tdSql.execute('insert into d1003 values("2018-10-09 14:38:05.500", 11.80000, 221, 0.28000);')
        self.tdSql.execute('insert into d1003 values("2018-10-10 14:38:05.500", 11.80000, 221, 0.28000);')
        self.tdCom.check_query_data('select _wstart as wstart, _wend as wend, max(current) as max_current from meters where 1=1 and groupId = 3 interval (5s);', 'select wstart, wend, `max_current` from current_stream_output_stb;')
        
    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            bug-ts4556
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write

