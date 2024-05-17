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

class TestTD30044(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def run(self):
        self.tdSql.execute('drop DATABASE if exists scada_iot;')
        self.tdSql.execute('CREATE DATABASE scada_iot KEEP 1825 stt_trigger 1;')
        self.tdSql.execute('CREATE STABLE IF NOT EXISTS scada_iot.model_original_super (ts timestamp, ident NCHAR(32), content NCHAR(16), model NCHAR(128)) TAGS (equip NCHAR(16));')
        self.tdSql.execute('use scada_iot;')
        self.tdSql.execute('create table ctb1 using model_original_super tags("ZR_9");')
        for i in range(1, 101):
            self.tdSql.execute(f'insert into ctb1 values(now+{i}s, "ident{i}", "content{i}", "model{i}");')
        self.tdSql.execute('flush database scada_iot;')
        # self.tdSql.query('select _wend as endtime ,count(*) as modelcount from scada_iot.model_original_super where equip = "ZR_9" and ts >= "2024-05-14 00:00:00" and ts <= "2024-05-14 23:59:59" partition by ident interval(1h)')
        time.sleep(5)
        self.tdSql.query('select _wend as endtime ,count(*) as modelcount from scada_iot.model_original_super where equip = "ZR_9" partition by ident interval(1h)')

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            bug-td30044
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Query