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
import copy
from taostest.components import TaosD
import time

class DupInsert(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.dbname = 'dup_insert_db'
        self.stbname = "stb"
        self.ctbname = "ctb"
        self.row_count = 1000
        self.range_count = int(10000/self.row_count)
        self.ts = self.tdCom.genTs()[0]
        self.taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        self._remote: Remote = Remote(self.logger)
        self.host = self.taosd_setting["fqdn"][0]
        self.taosd = TaosD(self._remote)
        self.restart_wait = 3

    def restart_taosd(self):
        for i in range(len(self.taosd_setting['spec']['dnodes'])):
            endpoint = self.taosd_setting['spec']['dnodes'][i]['endpoint']
            taosd_setting = copy.deepcopy(self.taosd_setting)
            self.taosd.update_cfg('/tmp',taosd_setting , {"supportVnodes": self.taosd_setting["spec"]["dnodes"][0]["config"]["supportVnodes"]}, endpoint, True)
        time.sleep(self.restart_wait)

    def init_version_install(self):
        kill_cmd = "ps -ef|grep -wi /etc/taos | grep -v grep | awk '{print $2}' | xargs kill -9 > /dev/null 2>&1"
        self._remote.cmd(self.host, [kill_cmd])
        rm_cmd = "rm -rf /var/lib/taos/* /var/log/taos/* "
        self._remote.cmd(self.host, [rm_cmd])
        cmds = [f'cd {self.taosd_setting["spec"]["dnodes"][0]["init_version_path"]}/', f"./install.sh -e no"]
        self._remote.cmd(self.host, cmds)
        self.restart_taosd()

    def fatal_version_install(self):
        kill_cmd = "ps -ef|grep -wi /etc/taos | grep -v grep | awk '{print $2}' | xargs kill -9 > /dev/null 2>&1"
        self._remote.cmd(self.host, [kill_cmd])
        cmds = [f'cd {self.taosd_setting["spec"]["dnodes"][0]["fatal_version_path"]}/', f"./install.sh -e no"]
        self._remote.cmd(self.host, cmds)
        self.restart_taosd()

    def upgrade_new_version(self):
        kill_cmd = "ps -ef|grep -wi /etc/taos | grep -v grep | awk '{print $2}' | xargs kill -9 > /dev/null 2>&1"
        self._remote.cmd(self.host, [kill_cmd])
        self._remote.cmd(self.host,
                         [f'cd {self.taosd_setting["spec"]["dnodes"][0]["code_dir"]}',
                          'git reset --hard FETCH_HEAD',
                          f'git checkout {self.taosd_setting["spec"]["dnodes"][0]["update_branch"]}',
                          f'git pull origin {self.taosd_setting["spec"]["dnodes"][0]["update_branch"]}',
                          f'mkdir -p {self.taosd_setting["spec"]["dnodes"][0]["code_dir"]}/debug',
                         f'cd {self.taosd_setting["spec"]["dnodes"][0]["code_dir"]}/debug',
                         f'rm -rf *',
                         'cmake .. &&  make -j32   && make install'])
        self.restart_taosd()

    def dup_insert(self):
        """
        insert duplicate-ts rows more than 1 bokck
        """
        self._remote.cmd(self.host, [f'taos -s "drop database if exists {self.dbname}"'])
        self._remote.cmd(self.host, [f'taos -s "create database if not exists {self.dbname}"'])
        self._remote.cmd(self.host, [f'taos -s "create stable if not exists {self.dbname}.{self.stbname} (ts timestamp, c1 int) tags (t1 int)"'])
        self._remote.cmd(self.host, [f'taos -s "create table if not exists {self.dbname}.{self.ctbname} using {self.dbname}.{self.stbname} tags (1)"'])
        sql = ""
        for i in range(self.row_count):
            sql += f'insert into {self.dbname}.{self.ctbname} values ({self.ts}, 1);'
        for i in range(self.range_count):
            self._remote.cmd(self.host, [f'taos -s "{sql}"'])
        res = self._remote.cmd(self.host, [f'taos -s "flush database {self.dbname}"'])
        res = self._remote.cmd(self.host, [f'taos -s "select * from {self.dbname}.{self.ctbname}"'])
        self.tdSql.checkIn("Query OK, 1 row(s) in set", res)

    def run(self):
        self.init_version_install()
        self.dup_insert()
        self.fatal_version_install()
        res = self._remote.cmd(self.host, [f'taos -s "select * from {self.dbname}.{self.ctbname}"'])
        self.tdSql.checkNotIn("Query OK, 1 row(s) in set", res)
        self.upgrade_new_version()
        res = self._remote.cmd(self.host, [f'taos -s "select * from {self.dbname}.{self.ctbname}"'])
        self.tdSql.checkIn("Query OK, 1 row(s) in set", res)

    def desc(self) -> str:
        case_description = """
            dup_insert <jayden>: [TS-3943] : insert duplicate-ts rows more than 1 bokck;\n
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Insert, T.Query