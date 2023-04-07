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
class TestTs3099(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def run(self):
        self.tdCom.createDb("test_ts3099")
        self.tdSql.error('create stable test_ts3099.stb (ts timestamp, i int comment "test") tags (t1 int);')
        self.tdSql.error('create stable test_ts3099.stb (ts timestamp, i int) tags (t1 int  comment "test");')
        self.tdSql.error('create table test_ts3099.tb (ts timestamp, i int comment "test");')

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            bug-ts3099
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write
