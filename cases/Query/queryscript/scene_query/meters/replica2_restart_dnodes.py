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
from taostest.components.taosd import TaosD
from taostest.util.remote import Remote
import os
import time

class RestartDnodes(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.dbname = 'db_test_replica2'
        self.ntbname = 'ntb'
        self.stbname = 'stb'
        self.ctbname = 'ctb'
        self.ts = 1537146000000
        self.str_length = 20
        self.default_replica = 2
        self.ready_sleep = 60
        self._remote: Remote = Remote(self.logger)
        self.column_dict = {
            'col1': 'bigint'
        }
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        self.endpoint_list = self.taosd_setting["spec"]["dnodes"]
        self.fqdn_list = self.taosd_setting["fqdn"]

    def restart_dnodes(self):
        self.tdSql.execute(f'drop database if exists {self.dbname}')
        if int(os.environ[TDCom.taostest_database_replicas_variable]) == self.default_replica:
            replica_value = self.default_replica
        else:
            replica_value = 1
        self.tdSql.execute(f'create database {self.dbname} replica {replica_value}')
        self.tdSql.execute(f'use {self.dbname}')
        for col_name,col_type in self.column_dict.items():
            self.tdSql.execute(f'create table {self.ntbname} (ts timestamp,{col_name} {col_type})')
            for i in range(1, 1000):
                self.tdSql.execute(f'insert into {self.ntbname} values(now+{i}s, {i})')
        print(self.endpoint_list)
        last_endpoint_port = self.endpoint_list[-1]["endpoint"].split(":")[1]
        self._remote.cmd(self.fqdn_list[-1], [f"netstat -ntlp | grep {last_endpoint_port} | awk \'{{print $7}}\' | cut -d '/' -f 1 | xargs kill -TERM"])
        for i in range(1001, 2000):
            self.tdSql.execute(f'insert into {self.ntbname} values(now+{i}s, {i})',queryTimes=30)
        self.taosd.restart(self.endpoint_list[-1], self.ready_sleep)
        time.sleep(self.ready_sleep*5)
        self.tdSql.query("show dnodes")
        # for query_data in self.tdSql.query_data:
        #     self.tdSql.checkEqual(query_data[4], "ready")
        self.tdSql.execute(f'drop database if exists {self.dbname}',queryTimes=30)


    def run(self):
            self.restart_dnodes()

    def cleanup(self):
        pass

    def desc(self):
        case_description = """
            restart_dnodes <jayden>
            """
        return case_description

    def author(self):
        return "jayden"

    def tags(self):
        return T.Write