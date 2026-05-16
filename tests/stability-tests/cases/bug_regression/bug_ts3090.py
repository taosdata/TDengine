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
import taos
from datetime import datetime

class TestTs3090(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        self.lines = [('d1001', '2018-10-03 14:38:05.000', 10.30000, 219, "aa", 'California.SanFrancisco', 2),
                    ('d1001', '2018-10-03 14:38:15.000', 12.60000, 218, "bb", 'California.SanFrancisco', 2),
                    ('d1001', '2018-10-03 14:38:16.800', 12.30000, 221, "cc", 'California.SanFrancisco', 2),
                    ('d1002', '2018-10-03 14:38:16.650', 10.30000, 218, "dd", 'California.SanFrancisco', 3)]

    def get_ts(self, ts: str):
        dt = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S.%f')
        return int(dt.timestamp() * 1000)

    def create_stable(self):
        conn = taos.connect(host=self.taosd_setting["fqdn"][0])
        try:
            conn.execute("DROP DATABASE if exists power")
            conn.execute("CREATE DATABASE if not exists power")
            conn.execute("CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, var_val binary(8)) "
                        "TAGS (location BINARY(64), groupId INT)")
            conn.execute('CREATE TABLE power.d1001 using power.meters tags ("a", 1);')
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
                try:
                    stmt.set_tbname_tags(tb_name, tags)
                    raise Exception("should not reach here")
                except Exception:
                    pass
            values: taos.TaosBind = taos.new_bind_params(4)  # 4 is count of columns
            values[0].timestamp(self.get_ts(row[1]))
            values[1].float(row[2])
            values[2].int(row[3])
            values[3].binary(row[4])
            try:
                stmt.bind_param(values)
                raise Exception("should not reach here")
            except Exception:
                pass

    def insert_data(self):
        conn = taos.connect(host=self.taosd_setting["fqdn"][0], database="power")
        try:
            stmt = conn.statement("INSERT INTO ? VALUES(?, ?, ?, ?)")
            self.bind_row_by_row(stmt, self.lines)
            try:
                stmt.execute()
                raise Exception("should not reach here")
            except Exception:
                pass
            stmt.close()
        finally:
            conn.close()

    def run(self):
        self.create_stable()
        self.insert_data()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            bug-ts3090
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write