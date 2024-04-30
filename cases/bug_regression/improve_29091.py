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

class TestTD29091(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        self.count = 100
        self.dbname = "test"
        self.stbname = "stb"
        self.ctbname = "ctb"
        self.check_col_name = "c3"
        
        self.ts_value = self.tdCom.genTs()[0]

    def prepare_data(self):
        self.tdCom.createDb(self.dbname)
        self.tdCom.create_stable(dbname=self.dbname, stbname=self.stbname)
        self.tdCom.create_ctable(dbname=self.dbname, stbname=self.stbname, ctbname=self.ctbname)
        for i in range(self.count):
            self.tdSql.execute(f'insert into {self.dbname}.{self.ctbname} (ts, {self.check_col_name}) values (now+{i+1}s, {i})')

    def run(self):
        self.prepare_data()
        self.tdSql.query(f'select ts, {self.check_col_name} from {self.stbname} where cast({self.check_col_name} as binary) like "1%"')
        res1 = self.tdSql.query_data
        self.tdSql.query(f'select ts, {self.check_col_name} from {self.stbname} where cast({self.check_col_name} as binary({self.count})) like "1%"')
        res2 = self.tdSql.query_data
        self.tdSql.checkEqual(res1, res2)

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            test_TD-29091
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Query