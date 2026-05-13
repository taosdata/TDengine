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
import os
from taostest.util.remote import Remote
from taostest.components.taosd import TaosD

class TestBatchInsert(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.dbname = "test_db"
        self.stbname = "test_stb"
        self.ctbname = "test_ctb"
        self.audit_dbname = "audit"
        self.audit_stbname = "operations"
        self.user = "user1"
        self.topic_name = "topic_test"
        self.tdSql.execute(f'drop topic if exists {self.topic_name}')
        self.tdSql.execute(f'drop database if exists {self.dbname}')
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.dnode = self.taosd_setting["spec"]["reserve_dnodes"][0]["endpoint"]
        self._remote: Remote = Remote(self.logger)
        self._tmp_dir: str = os.path.join(self.run_log_dir, "tmp")
        self.taosd = TaosD(self._remote)
        self.taosd.configure_and_start_specified_dnode(self._tmp_dir, self.taosd_setting, self.taosd_setting["spec"]["reserve_dnodes"][0])

    def audit_sql(self):
        db_sql_list = [f'create database if not exists {self.dbname}',
                       f'alter database {self.dbname} buffer 257',
                       f'compact database {self.dbname};',
                       f'drop database {self.dbname}'
                       ]
        stb_sql_list = [f'create database if not exists {self.dbname}',
                        f'create stable if not exists {self.dbname}.{self.stbname} (ts timestamp, c1 int) tags (t1 int)',
                        f'alter stable {self.dbname}.{self.stbname} add column c2 varchar(10)',
                        f'alter stable {self.dbname}.{self.stbname} modify column c2 varchar(20)',
                        f'alter stable {self.dbname}.{self.stbname} drop column c2',
                        f'alter stable {self.dbname}.{self.stbname} add tag t2 varchar(10)',
                        f'alter stable {self.dbname}.{self.stbname} modify tag t2 varchar(20)',
                        f'alter stable {self.dbname}.{self.stbname} drop tag t2',
                        f'drop stable {self.dbname}.{self.stbname}',
                        f'drop database {self.dbname}'
                        ]
        privileges_sql_list = [f'create USER {self.user} PASS "tbase125!" sysinfo 1',
                               f'GRANT ALL ON *.* TO {self.user}',
                               f'REVOKE ALL ON *.* FROM {self.user}',
                               f'alter user {self.user} enable 0',
                               f'DROP USER {self.user};' ]

        stream_sql_list = [f'create database if not exists {self.dbname}',
                          f'create stable if not exists {self.dbname}.{self.stbname} (ts timestamp, c1 int) tags (t1 int)',
                          f'create stream `current_s#$tream` trigger at_once into {self.dbname}.current_stream_output_stb as select _wstart as wstart, max(c1) from {self.dbname}.{self.stbname} interval (5s);',
                          f'drop stream `current_s#$tream`',
                          ]
        node_handle_list = [
                            'balance vgroup',
                            'restore dnode 1;',
                            'restore vnode on dnode 1;',
                            'restore qnode on dnode 1;',
                            'restore qnode on dnode 1;',
                            'REDISTRIBUTE VGROUP 2 dnode 1;'
                            ]
        topic_sql_list = [f'create database if not exists {self.dbname}',
                          f'create stable if not exists {self.dbname}.{self.stbname} (ts timestamp, c1 int) tags (t1 int)',
                          f'CREATE TOPIC {self.topic_name} AS SELECT ts, c1 FROM {self.dbname}.{self.stbname}  WHERE c1 > 1;'
                          f'drop topic {self.topic_name}'
                          ]
        node_sql_list = [
                          'alter dnode 1 "debugflag 131"',
                          'create mnode on dnode 2;',
                          'drop mnode on dnode 2;',
                          'create qnode on dnode 2;',
                          'drop qnode on dnode 2;',
                          'drop dnode 2'
                        #   f'create dnode "{self.dnode}"',
                        #   'drop dnode 3'
                         ]
        return db_sql_list + stb_sql_list + privileges_sql_list + topic_sql_list + stream_sql_list + node_handle_list + node_sql_list
    def auditlog(self):
        """
        alditlog test
        """
        sql_list = self.audit_sql()
        for sql in sql_list:
            self.tdSql.execute(sql)
            res_list = self.gen_res_list()
            confirm_sql = sql.lower().replace("  ", " ").replace(";", "")
            self.tdSql.checkIn(confirm_sql, res_list)

    def gen_res_list(self):
        self.tdSql.query(f'select `details` from {self.audit_dbname}.{self.audit_stbname};')
        return list(map(lambda x: x[0], self.tdSql.query_data))

    def run(self):
        self.gen_res_list()
        self.auditlog()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            auditlog <jayden>: : auditlog;
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql