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

class TestTs4730(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def run(self):
        self.tdCom.createDb(dbname="test")
        self.tdSql.execute('use test;')
        self.tdSql.execute('CREATE STABLE IF NOT EXISTS test.meters(ts timestamp, c1 int) TAGS (t1 int);')
        self.tdSql.error('CREATE STREAM IF NOT EXISTS stream_count FILL_HISTORY 1 INTO stcount SUBTABLE(CONCATE("new-", tname)) AS SELECT _wstart AS ts, count(*) as cnt FROM test.meters PARTITION BY tbname tname interval(10s);')
        self.tdSql.checkIn("Func not exists", str(self.tdSql.error_msg))

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            bug-ts4730
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write