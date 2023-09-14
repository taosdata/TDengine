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
import taos

class TestTs2918(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.tdCom.env_setting = self.env_setting
        self.tdCom.sml_type = "influxdb"
        self.tdCom.drop_all_db()
        self.dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname=self.dbname)
        self.ts = self.tdCom.genTs()[0]
        self.taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        self._remote: Remote = Remote(self.logger)
        self.lines1 = [('d1001', self.ts, 10.30000, 518, "bb", 'California.SanFrancisco', 2)]

    def create_stable(self):
        conn = taos.connect(host=self.taosd_setting["fqdn"][0])
        try:
            conn.execute("DROP DATABASE if exists power")
            conn.execute("CREATE DATABASE if not exists power")
            conn.execute("CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, var_val binary(8)) "
                        "TAGS (location BINARY(64), groupId INT)")
            conn.execute('CREATE TABLE power.d1001 using power.meters TAGS ("a", 1)')
        finally:
            conn.close()

    def bind_row_by_row(self, stmt: taos.TaosStmt, lines):
        tb_name = None
        for row in lines:
            if tb_name != row[0]:
                tb_name = row[0]
                tags: taos.TaosBind = taos.new_bind_params(2)  # 2 is count of tags
                tags[0].binary(row[5])  # location
                tags[1].int(row[6])  # groupId
                stmt.set_tbname_tags(tb_name, tags)
            values: taos.TaosBind = taos.new_bind_params(4)  # 4 is count of columns
            values[0].timestamp(self.ts)
            values[1].float(row[2])
            values[2].int(row[3])
            values[3].binary(row[4])
            stmt.bind_param(values)

    def insert_data(self):
        conn = taos.connect(host=self.taosd_setting["fqdn"][0], database="power")
        try:
            stmt = conn.statement("INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)")
            self.bind_row_by_row(stmt, self.lines1)
            stmt.execute()
            stmt.close()
            conn.execute(f'alter stable power.meters modify column var_val binary(15)')
            self.tdSql.execute(f'insert into power.{self.lines1[0][0]} values ({self.lines1[0][1]}, 1, 1, "aabbbbbbcccc")')
            conn.execute(f'alter stable power.meters modify column var_val binary(30)')
            self.tdSql.query(f'select * from power.meters')
        finally:
            conn.close()

    def run(self):
        self.create_stable()
        self.insert_data()
        # input_sql = f'stb,type=insert eid=1 {self.ts}'
        # print("=====",input_sql)
        # self.tdSql._conn.schemaless_insert([input_sql], TDSmlProtocolType.LINE.value, TDSmlTimestampType.MILLI_SECOND.value)
        # self.tdSql.query(f'show {self.dbname}.tables')
        # tbname = self.tdSql.query_data[0][0]
        # self.tdSql.execute(f'insert into {tbname} values ({self.ts}, 2)')
        # self.tdSql.query(f'select * from stb')
        # self.tdSql.query(f'select * from {tbname}')
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
