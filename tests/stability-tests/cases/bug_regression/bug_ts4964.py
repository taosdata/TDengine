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
from taostest.util.remote import Remote

class TestTS4964(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        self._remote: Remote = Remote(self.logger)

    def run(self):
        self.tdSql.execute('drop DATABASE if exists ts4964_test;')
        self.tdSql.execute('CREATE DATABASE ts4964_test stt_trigger 1;')
        self.tdSql.execute('CREATE STABLE IF NOT EXISTS ts4964_test.stb (ts timestamp, ident int, content NCHAR(16), model NCHAR(128)) TAGS (equip NCHAR(16));')
        self.tdSql.execute('use ts4964_test;')
        self.tdSql.execute('create table ctb1 using stb tags("abc");')
        for i in range(1, 101):
            self.tdSql.execute(f'insert into ctb1 values(now+{i}s, {i}, "content{i}", "model{i}");')
        self.tdSql.execute('flush database ts4964_test;')
        self.tdSql.execute(f'create stream stream_test fill_history 1 into ts4964_test.sample as select avg(ident) from ts4964_test.stb interval(1n);')
        self.tdCom.check_query_data('select `avg(ident)` from ts4964_test.sample', 'select avg(ident) from ts4964_test.stb interval(1n)')
        self.tdSql.query('select `avg(ident)` from ts4964_test.sample')
        res = self._remote.cmd(self.taosd_setting["spec"]["dnodes"][0]["endpoint"].split(":")[0], f'grep -ri "Timestamp data out of range" {self.taosd_setting["spec"]["dnodes"][0]["config"]["logDir"]}/taoslog0.0 | wc -l')
        self.tdSql.checkEqual(int(res), 0)

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            bug-ts-4964
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.Stream