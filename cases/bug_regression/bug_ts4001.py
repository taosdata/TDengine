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

class TestTs4001(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def run(self):
        self.tdCom.createDb(dbname="bug4001_test")
        self.tdSql.execute('use bug4001_test;')
        self.tdSql.execute('CREATE STABLE IF NOT EXISTS bug4001_test.test(create_time timestamp, record_time timestamp, s1 int) TAGS (t1 nchar(50));')
        self.tdSql.execute('CREATE STREAM IF NOT EXISTS test_s FILL_HISTORY 1 IGNORE EXPIRED 0 TRIGGER MAX_DELAY 6s INTO test_s1 SUBTABLE(CONCAT(tbname, "_s1")) AS SELECT _wstart AS alarm_begin_time, _wend AS alarm_end_time, _wduration AS alarm_duration, count(*) AS record_count,t1, s1 FROM test PARTITION BY tbname, t1, s1 STATE_WINDOW(s1);')
        self.tdSql.execute('INSERT INTO test_001 USING test TAGS("001") VALUES("2023-09-15 09:00:00", "2023-09-15 09:00:00", 1) ("2023-09-15 09:00:01", "2023-09-15 09:00:01", 2);')
        self.tdCom.check_query_data("SELECT _wstart AS alarm_begin_time, _wend AS alarm_end_time, _wduration AS alarm_duration, count(*) AS record_count,t1, s1 FROM test PARTITION BY tbname, t1, s1 STATE_WINDOW(s1);", "select alarm_begin_time, alarm_end_time, alarm_duration, record_count, t1, s1 from test_s1")

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            bug-ts4001
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write