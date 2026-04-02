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
from taostest.util.sml_types import TDSmlProtocolType, TDSmlTimestampType
from taostest.util.remote import Remote

class TestTs2918(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.tdCom.env_setting = self.env_setting
        self.tdCom.sml_type = "influxdb"
        self.tdCom.drop_all_db()
        self.dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname=self.dbname, precision="us")
        self._remote: Remote = Remote(self.logger)
        self.taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")

    def run(self):
        input_sql1 = 'm92195e05_9fe5_4ca6_b1cb_bb07165c12a9,type=Measurement,device=eb34e870-0a47-495b-bfc5-7be519cfd9c5,line=03e72b18-3970-4066-a842-9b17eeabe32b,location=7b92ad4a-0884-4da5-af6e-bb45d151b11e,model=92195e05-9fe5-4ca6-b1cb-bb07165c12a9 eid="2eb07713-a3a3-4893-97c2-cb5861c5d87c",altid="DEV_ST14_DYSN_BAS_TTBL_001-BAS_TTBL_DIOST_025-1678859688757",rcvdate=1678859635755i64,BAS_TTBL_DIOST_025=28890.0f64,BAS_TTBL_DIOST_025_METADATA=L"{\\"serverTime\\":\\"1678859688558\\"/,\\"sourceTime\\":\\"1678859688051\\"/,\\"statusCode\\":\\"1\\"}" 1678859788561000000'
        input_sql2 = 'm92195e05_9fe5_4ca6_b1cb_bb07165c12a9,type=Measurement,device=eb34e870-0a47-495b-bfc5-7be519cfd9c5,line=03e72b18-3970-4066-a842-9b17eeabe32b,location=7b92ad4a-0884-4da5-af6e-bb45d151b11e,model=92195e05-9fe5-4ca6-b1cb-bb07165c12a9 eid="1eb07713-a3a3-4893-97c2-cb5861c5d87c",altid="DEV_ST14_DYSN_BAS_TTBL_001-BAS_TTBL_DIOST_025-1678859688757",rcvdate=1678859635755i64,BAS_TTBL_DIOST_025=28890.0f64,BAS_TTBL_DIOST_025_METADATA=L"{\\"serverTime\\":\\"1678859688558\\"/,\\"sourceTime\\":\\"1678859688051\\"/,\\"statusCode\\":\\"1\\"}" 1678859788561000001'
        self.tdSql._conn.schemaless_insert([input_sql1], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        self.tdSql._conn.schemaless_insert([input_sql2], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        res = self._remote.cmd(self.taosd_setting["spec"]["dnodes"][0]["endpoint"].split(":")[0], f'grep -ri "code:Syntax error in SQL" {self.taosd_setting["spec"]["dnodes"][0]["config"]["logDir"]}/taoslog0.0 | wc -l')
        self.tdSql.checkEqual(int(res), 0)

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = '''
            bug-ts2918
        '''
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.Schemaless.Taosc.InfluxDB
