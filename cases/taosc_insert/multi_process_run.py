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
from taostest.components import TaosD
from taostest.util.remote import Remote

class TestMultiProcessRun(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.endpoint = self.taosd_setting["spec"]["config"]["firstEP"]
                self.vnodeShmSize = self.taosd_setting["spec"]["dnodes"][0]["config"]["vnodeShmSize"]
                self.mnodeShmSize = self.taosd_setting["spec"]["dnodes"][0]["config"]["mnodeShmSize"]

    def gen_tb_batch_sql(self, batch, col_type, data_type, data_length, ts=None):
        """
        batch_sql
        """
        batch_sqls = ""
        for row_num in range(batch):
            base_sql = ""
            if ts is not None:
                base_sql += f'now-{row_num}s, '
            if col_type == "col":
                if data_type == "binary":
                    base_sql += f'"{self.tdCom.get_long_name(length=data_length, mode="letters")}"'
                else:
                    pass
            batch_sqls += f'({base_sql}),'
        return batch_sqls[:-1]

    def init_env(self):
        self.taosd.update_cfg('/tmp', self.taosd_setting, {"mnodeShmSize": self.mnodeShmSize}, self.endpoint, True)
        self.taosd.update_cfg('/tmp', self.taosd_setting, {"vnodeShmSize": self.vnodeShmSize}, self.endpoint, True)

    def multi_process_batch_insert(self, batch, col_type="col", data_type="binary", data_length=10000):
        self.init_env()
        self.tdCom.drop_all_db()
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.execute(f'create table if not exists {dbname}.tb (ts timestamp, c1 binary({data_length}))')
        self.tdSql.execute(f'insert into {dbname}.tb values {self.gen_tb_batch_sql(batch, col_type, data_type, data_length, True)};')
        self.tdSql.query(f'select * from {dbname}.tb')
        self.tdSql.checkEqual(self.tdSql.query_row, batch)
        self.tdSql.execute(f'drop database if exists {dbname}')

        self.taosd.update_cfg('/tmp', self.taosd_setting, {"mnodeShmSize": 100000, "vnodeShmSize": self.vnodeShmSize}, self.endpoint, True)
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.execute(f'create table if not exists {dbname}.tb (ts timestamp, c1 binary({data_length}))')
        self.tdSql.error(f'insert into {dbname}.tb values {self.gen_tb_batch_sql(batch, col_type, data_type, data_length, True)};')
        self.tdSql.execute(f'drop database if exists {dbname}')

        self.taosd.update_cfg('/tmp', self.taosd_setting, {"vnodeShmSize": 100000, "mnodeShmSize": self.vnodeShmSize}, self.endpoint, True)
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.execute(f'create table if not exists {dbname}.tb (ts timestamp, c1 binary({data_length}))')
        self.tdSql.error(f'insert into {dbname}.tb values {self.gen_tb_batch_sql(batch, col_type, data_type, data_length, True)};')
        self.tdSql.execute(f'drop database if exists {dbname}')



    def run(self):
        # self.stb_batch_insert()
        # self.tb_batch_insert()
        self.multi_process_batch_insert(batch=100, data_length=10160)
    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            batch_insert <jayden>: [TD-13419] : batch_insert;
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Insert.BatchInsert