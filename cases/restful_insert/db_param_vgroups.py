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

import copy
import os
from taostest import TDCase, T
from taostest.util.common import TDCom
from taostest.util.remote import Remote
from taostest.components import TaosD
from taostest.util.rest import TDRest
class TestVgroups(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.tdRest = TDRest(env_setting=self.env_setting)
        self.cfg = self.tdCom.Boundary.DB_PARAM_VGROUPS_CONFIG
        self.buffer_min = self.tdCom.Boundary.DB_PARAM_BUFFER_CONFIG["boundary"][0]
        self.taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        self.fqdn = self.taosd_setting["fqdn"][0]
        self.vnode_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + "/vnode"
        self.endpoint = self.taosd_setting["spec"]["config"]["firstEP"]
        self.api_type = 'restful'
    def get_vnode_count(self):
        vnode_sum = 0
        for i in self.taosd_setting['spec']['dnodes']:
            self.fqdn = i['endpoint'].split(':')[0]
            vnode_dir = i['config']['dataDir']+ "/vnode"
            vnode_sum += int(self._remote.cmd(self.fqdn, [f'ls {vnode_dir} | grep -v vnodes.json | grep -v shmfile | wc -l']))
        if 'DATABASE_REPLICAS' in str(os.environ.keys()).upper():
            if os.environ.get('DATABASE_REPLICAS') == '1':
                return vnode_sum
            elif os.environ.get('DATABASE_REPLICAS') == '3':
                return vnode_sum / 3

    def vgroups_check(self):
        """
        vgroups check
        """

        for i in range(len(self.taosd_setting['spec']['dnodes'])):
            endpoint = self.taosd_setting['spec']['dnodes'][i]['endpoint']
            taosd_setting = copy.deepcopy(self.taosd_setting)
            self.taosd.update_cfg('/tmp',taosd_setting , {"supportVnodes": self.cfg["boundary"][-1]}, endpoint, True)
        self.tdCom.drop_all_db()
        test_param = self.cfg["create_name"]
        dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname)
        self.tdRest.request('show databases')
        db_field = self.tdRest.get_rest_db_field(self.tdRest.resp,test_param,dbname)
        # default
        self.tdSql.checkEqual(db_field, self.cfg["default"])
        self.tdSql.checkEqual(self.get_vnode_count(),db_field)
        self.tdRest.request(f'drop database {dbname}')
        # boundary
        for param_value in self.cfg["boundary"]:
            dbname = self.tdCom.get_long_name()
            kv_dict = {test_param: param_value, "buffer": self.buffer_min}
            self.tdCom.createDb(dbname, **kv_dict)
            self.tdRest.request('show databases')
            db_field = self.tdRest.get_rest_db_field(self.tdRest.resp,test_param,dbname)
            self.tdSql.checkEqual(db_field, param_value)
            self.tdSql.checkEqual(self.get_vnode_count(),db_field)
            if param_value == self.cfg["boundary"][-1]:
                self.tdRest.error(f'create database if not exists {dbname}_error {test_param} 1 buffer {self.buffer_min}')
            self.tdRest.request(f'drop database {dbname}')
        self.tdRest.error(f'create database if not exists {dbname} {test_param} {self.cfg["boundary"][0] - 1} buffer {self.buffer_min}')
        self.tdRest.error(f'create database if not exists {dbname} vgroups {self.cfg["boundary"][-1] + 1} buffer {self.buffer_min}')
        # check logic
        dbname1 = self.tdCom.get_long_name()
        kv_dict = {test_param: param_value, "buffer": self.buffer_min, "vgroups": int(self.cfg["boundary"][-1]/4)}
        self.tdCom.createDb(dbname1, **kv_dict)
        self.tdRest.request(f'show {dbname1}.vgroups')
        self.tdSql.checkEqual(len(self.tdRest.resp['data']), int(self.cfg["boundary"][-1]/4))
        self.tdSql.checkEqual(self.get_vnode_count(), int(self.cfg["boundary"][-1]/4))
        dbname2 = self.tdCom.get_long_name()
        kv_dict = {test_param: param_value, "buffer": self.buffer_min, "vgroups": int(self.cfg["boundary"][-1]/4) + 1}
        self.tdCom.createDb(dbname2, **kv_dict)
        self.tdRest.request(f'show {dbname2}.vgroups')
        self.tdSql.checkEqual(len(self.tdRest.resp['data']), int(self.cfg["boundary"][-1]/4) + 1)
        self.tdSql.checkEqual(self.get_vnode_count(), int(self.cfg["boundary"][-1]/4)*2 + 1)
        self.tdRest.request(f'drop database {dbname1}')
        self.tdSql.checkEqual(self.get_vnode_count(), int(self.cfg["boundary"][-1]/4) + 1)
        self.tdRest.request(f'drop database {dbname2}')
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

