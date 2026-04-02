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

class TestTs2912(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        self.lines1 = [('d1001', '2018-10-03 14:38:05.000', 10.30000, 219, "aa", 'California.SanFrancisco', 2),
                    ('d1001', '2018-10-03 14:38:15.000', 12.60000, 218, "bb", 'California.SanFrancisco', 2),
                    ('d1001', '2018-10-03 14:38:16.800', 12.30000, 221, "cc", 'California.SanFrancisco', 2),
                    ('d1002', '2018-10-03 14:38:16.650', 10.30000, 218, "dd", 'California.SanFrancisco', 3)]
        self.lines2 = [('d1003', '2018-10-03 14:38:05.500', 11.80000, 221, "ee", 'California.LosAngeles', 2),
                    ('d1003', '2018-10-03 14:38:16.600', 13.40000, 223, "ff", 'California.LosAngeles', 2),
                    ('d1004', '2018-10-03 14:38:05.000', 10.80000, 223, "gg", 'California.LosAngeles', 3),
                    ('d1004', '2018-10-03 14:38:06.500', 11.50000, 221, 'hh', 'California.LosAngeles', 3)]
        self.lines3 = [('2018-10-03 14:38:05.000', 10.30000, 219, "aa"),
                    ('2018-10-03 14:38:15.000', 12.60000, 218, "bb"),
                    ('2018-10-03 14:38:16.800', 12.30000, 221, "cc"),
                    ('2018-10-03 14:38:16.650', 10.30000, 218, "dd")]
        self.lines4 = [('2018-10-03 14:38:05.500', 11.80000, 221, "ee"),
                    ('2018-10-03 14:38:16.600', 13.40000, 223, "ff"),
                    ('2018-10-03 14:38:05.000', 10.80000, 223, "gg"),
                    ('2018-10-03 14:38:06.500', 11.50000, 221, 'hh')]

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
            values[0].timestamp(self.get_ts(row[1]))
            values[1].float(row[2])
            values[2].int(row[3])
            values[3].binary(row[4])
            stmt.bind_param(values)

    def bind_row_by_row_notb(self, stmt: taos.TaosStmt, lines):
        for row in lines:
            values: taos.TaosBind = taos.new_bind_params(4)  # 4 is count of columns
            values[0].timestamp(self.get_ts(row[0]))
            values[1].float(row[1])
            values[2].int(row[2])
            values[3].binary(row[3])
            stmt.bind_param(values)

    def insert_data(self):
        conn = taos.connect(host=self.taosd_setting["fqdn"][0], database="power")
        try:
            stmt = conn.statement("INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)")
            self.bind_row_by_row(stmt, self.lines1)
            stmt.execute()
            conn.execute(f'alter stable power.meters modify column var_val binary(10)')
            self.bind_row_by_row(stmt, self.lines2)
            stmt.execute()
            stmt.close()
        finally:
            conn.close()

    def insert_data_notb(self):
        conn = taos.connect(host=self.taosd_setting["fqdn"][0], database="power")
        try:
            stmt = conn.statement("INSERT INTO d_1 USING meters TAGS('California.SanFrancisco', 2) VALUES(?, ?, ?, ?)")
            self.bind_row_by_row_notb(stmt, self.lines3)
            stmt.execute()
            conn.execute(f'alter stable power.meters modify column var_val binary(15)')
            self.bind_row_by_row_notb(stmt, self.lines4)
            stmt.execute()
            stmt.close()
        finally:
            conn.close()

    def run(self):
        self.create_stable()
        self.insert_data()
        self.insert_data_notb()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            bug-ts2912
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write