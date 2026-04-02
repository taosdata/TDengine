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

class TestTs2988(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql, env_setting=self.env_setting)
        self.tdRest = TDRest(env_setting=self.env_setting)
        self.dbname = "test_ts2988"
        self.tdSql.execute(f'drop database if exists {self.dbname}')
        self.tdCom.sml_type = "influxdb_restful"
        self.tdCom.createDb(dbname=self.dbname, precision="ns")

    def run(self):
        input_sql1 = f'm92195e05_9fe5_4ca6_b1cb_bb07165c12a9,t0=t c0=f,c1="{self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"])}",c2="{self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"])}",c3="{self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"])}",c4="{self.tdCom.get_long_name(16374)}" 1626006833639000000'
        res = self.tdRest.schemalessApiPost(sql=input_sql1, dbname=self.dbname)
        input_sql2 = f'm92195e05_9fe5_4ca6_b1cb_bb07165c12a9,t0=t c0=f,c1="{self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"])}",c2="{self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"])}",c3="{self.tdCom.get_long_name(self.tdCom.boundary_config["BINARY_MAX_LENGTH"])}",c4="{self.tdCom.get_long_name(16374)}",c5="{self.tdCom.get_long_name(16374)}" 1626006833639000001'
        res = self.tdRest.schemalessApiPost(sql=input_sql2, dbname=self.dbname)
        self.tdSql.checkIn("Columns total length exceeds row bytes", res.text)

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = '''
            bug-ts2988
        '''
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.Schemaless.Taosc.InfluxDB
