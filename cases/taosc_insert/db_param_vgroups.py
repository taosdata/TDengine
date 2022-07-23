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
from taostest.components import TaosD

class TestVgroups(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.cfg = self.tdCom.Boundary.DB_PARAM_VGROUPS_CONFIG
        self.buffer_min = self.tdCom.Boundary.DB_PARAM_BUFFER_CONFIG["boundary"][0]
        self.taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        self.fqdn = self.taosd_setting["fqdn"][0]
        self.vnode_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + "/vnode"
        self.endpoint = self.taosd_setting["spec"]["config"]["firstEP"]

    def get_vnode_count(self):
        return int(self._remote.cmd(self.fqdn, [f'ls {self.vnode_dir} | grep -v vnodes.json | grep -v shmfile | wc -l']))

    def vgroups_check(self):
        """
        vgroups check
        """
        self.taosd.update_cfg('/tmp', self.taosd_setting, {"supportVnodes": self.cfg["boundary"][-1]}, self.endpoint, True)
        self.tdCom.drop_all_db()
        test_param = self.cfg["create_name"]
        dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname)
        self.tdSql.query('show databases')
        db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
        # default
        self.tdSql.checkEqual(db_field_kv_dict[test_param], self.cfg["default"])
        self.tdSql.execute(f'drop database {dbname}')
        # boundary
        for param_value in self.cfg["boundary"]:
            dbname = self.tdCom.get_long_name()
            kv_dict = {test_param: param_value, "buffer": self.buffer_min}
            self.tdCom.createDb(dbname, **kv_dict)
            self.tdSql.query('show databases')
            db_field_kv_dict = self.tdSql.get_db_field_kv(0, dbname)
            self.tdSql.checkEqual(db_field_kv_dict[test_param], param_value)
            if param_value == self.cfg["boundary"][-1]:
                kv_dict = {test_param: 1, "buffer": self.buffer_min}
                self.tdSql.error(f'create database if not exists {dbname}_error {test_param} 1 buffer {self.buffer_min}')
            self.tdSql.execute(f'drop database {dbname}')
        self.tdSql.error(f'create database if not exists {dbname} {test_param} {self.cfg["boundary"][0] - 1} buffer {self.buffer_min}')
        self.tdSql.error(f'create database if not exists {dbname} vgroups {self.cfg["boundary"][-1] + 1} buffer {self.buffer_min}')
        # check logic
        dbname1 = self.tdCom.get_long_name()
        kv_dict = {test_param: param_value, "buffer": self.buffer_min, "vgroups": int(self.cfg["boundary"][-1]/4)}
        self.tdCom.createDb(dbname1, **kv_dict)
        self.tdSql.query(f'show {dbname1}.vgroups')
        self.tdSql.checkEqual(self.tdSql.query_row, int(self.cfg["boundary"][-1]/4))
        self.tdSql.checkEqual(self.get_vnode_count(), int(self.cfg["boundary"][-1]/4))
        dbname2 = self.tdCom.get_long_name()
        kv_dict = {test_param: param_value, "buffer": self.buffer_min, "vgroups": int(self.cfg["boundary"][-1]/4) + 1}
        self.tdCom.createDb(dbname2, **kv_dict)
        self.tdSql.query(f'show {dbname2}.vgroups')
        self.tdSql.checkEqual(self.tdSql.query_row, int(self.cfg["boundary"][-1]/4) + 1)
        self.tdSql.checkEqual(self.get_vnode_count(), int(self.cfg["boundary"][-1]/4)*2 + 1)
        self.tdSql.execute(f'drop database {dbname1}')
        self.tdSql.checkEqual(self.get_vnode_count(), int(self.cfg["boundary"][-1]/4) + 1)
        self.tdSql.execute(f'drop database {dbname2}')
        self.tdSql.checkEqual(self.get_vnode_count(), 0)

    def run(self) -> bool:
        self.vgroups_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            vgroups check <jayden>: [TD-14991] : vgroups check;
            """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Database.Create, T.Write.TaoscSql.Database.Alter

