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
from taostest.util.rest import TDRest

class TestTs2989(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.tdRest = TDRest(env_setting=self.env_setting)

    def run(self):
        self.tdCom.createDb("test_ts2989")
        self.tdRest.request('create stable test_ts2989.a (ts timestamp, i int) tags (t1 int);')
        self.tdRest.request('insert into test_ts2989.a2 using test_ts2989.a tags(12) (ts,i ) values(now,11));')
        self.tdSql.checkEqual(self.tdRest.resp, {'code': 534, 'desc': "syntax error near ');' (table_name is expected)"})

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            bug-ts2989
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write