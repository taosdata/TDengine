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

class TestTD29092(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        self.count = 100
        self.dbname = "test"
        self.stbname1 = "stb1"
        self.ctbname1 = "ctb1"
        
        self.stbname2 = "stb2"
        self.ctbname2 = "ctb2"
        
        self.ts_value = self.tdCom.genTs()[0]

    def prepare_data(self):
        self.tdCom.createDb(self.dbname)
        self.tdCom.create_stable(dbname=self.dbname, stbname=self.stbname1)
        self.tdCom.create_ctable(dbname=self.dbname, stbname=self.stbname1, ctbname=self.ctbname1)

        self.tdCom.create_stable(dbname=self.dbname, stbname=self.stbname2)
        self.tdCom.create_ctable(dbname=self.dbname, stbname=self.stbname2, ctbname=self.ctbname2)

        for i in range(self.count):
            self.tdCom.insert_rows(dbname=self.dbname, tbname=self.ctbname1, ts_value=self.ts_value)
            self.tdCom.insert_rows(dbname=self.dbname, tbname=self.ctbname2, ts_value=self.ts_value)
            self.ts_value += 1

    def run(self):
        self.prepare_data()
        self.tdSql.query(f'select A.tbname, count(A.c1) from {self.stbname1} A group by A.tbname')
        self.tdSql.checkEqual(self.tdSql.query_data, [(self.ctbname1, self.count)])
        self.tdSql.query(f'select A.ts, A.tbname, B.tbname, count(A.c1) from {self.stbname1} as A join {self.stbname2} as B on B.ts=A.ts partition by A.ts,A.tbname')
        self.tdSql.checkEqual(self.tdSql.query_row, self.count)
        ctb1_tbname_val = [i[1] for i in self.tdSql.query_data]
        self.tdSql.checkEqual(set(ctb1_tbname_val), {self.ctbname1})
        ctb2_tbname_val = [i[2] for i in self.tdSql.query_data]
        self.tdSql.checkEqual(set(ctb2_tbname_val), {self.ctbname2})
        cnt_val = [i[3] for i in self.tdSql.query_data]
        self.tdSql.checkEqual(set(cnt_val), {1})

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            test_TD-29092
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Query