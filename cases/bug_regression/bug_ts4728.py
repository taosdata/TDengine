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

class TestTs4728(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.host = self.get_fqdn("taosd")[0]
        self._remote: Remote = Remote(self.logger)
        self.dbname = "power"
        self.stbname = "meters"
        self.consumer_ip = self.taosd_setting["spec"]["config"]["firstEP"].split(":")[0]
        self.queryString = f"select * from {self.dbname}.{self.stbname}"
        self.str_len = 1024
        self.vnode_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"] + "/vnode"
        self.range_count = 5
        
    def loop_consumer(self):
        consumer = self.tdCom.tmq(self.queryString, self.consumer_ip)
        consumer.close()

    def check_tdb_disk_usage(self, main_tdb_file):
        return self._remote.cmd(self.host, [f'du -sh -k {main_tdb_file}']).split('\t')[0]
    
    def run(self):
        self.tdSql.execute(f'drop topic if exists {self.tdCom.topic_name}')
        self.tdCom.createDb(dbname=self.dbname)
        self.tdSql.execute(f'CREATE STABLE {self.stbname} (ts timestamp, c1 varchar(1024), c2 varchar(1024), c3 varchar(1024), c4 varchar(1024), c5 varchar(1024), c6 varchar(1024), c7 varchar(1024), c8 varchar(1024), c9 varchar(1024)) TAGS (location binary(64), groupId int);')
        self.tdSql.execute(f'CREATE TABLE d1001 USING {self.stbname} TAGS ("d1", 1);')
        ts = self.tdCom.genTs()[0]
        for j in range(self.range_count):
            self.tdSql.execute(f'insert into d1001 values ({ts+j}, "{self.tdCom.get_long_name(self.str_len)}", "{self.tdCom.get_long_name(self.str_len)}", "{self.tdCom.get_long_name(self.str_len)}", "{self.tdCom.get_long_name(self.str_len)}", "{self.tdCom.get_long_name(self.str_len)}", "{self.tdCom.get_long_name(self.str_len)}", "{self.tdCom.get_long_name(self.str_len)}", "{self.tdCom.get_long_name(self.str_len)}", "{self.tdCom.get_long_name(self.str_len)}");')
        vgid = self.tdCom.get_vgid_list(self.dbname)[0]
        main_tdb_file = f'{self.vnode_dir}/vnode{vgid}/tq/main.tdb'
        usage_list = list()
        for i in range(self.range_count):
            usage1 = self.check_tdb_disk_usage(main_tdb_file)
            self.loop_consumer()
            usage2 = self.check_tdb_disk_usage(main_tdb_file)
            usage_list.append(usage1)
            usage_list.append(usage2)
            self.tdSql.checkEqual(len(set(usage_list)) < self.range_count, True)

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            bug-ts4278
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write

