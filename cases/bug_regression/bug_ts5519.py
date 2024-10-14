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
from taostest.util.common import TDCom
from taostest.components.taosd import TaosD
from taostest.util.remote import Remote
import os

class TestTs5519(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.cfg = self.tdCom.Boundary.DB_PARAM_VGROUPS_CONFIG
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        
        self.restart_dnode = self.taosd_setting["spec"]["dnodes"]
        self.sorted_dnodes = sorted(self.restart_dnode, key=lambda x: int(x['endpoint'].split(':')[1]))
        self.replica = int(os.environ["DATABASE_REPLICAS"]) if "DATABASE_REPLICAS" in os.environ else 3
        self.create_table_thread_count = 40
        self.childtable_count = 100
        self.insert_rows1 = 2000000
        self.insert_rows2 = 200000
        
        self.keep = "1000d"
        self.duration = "200d"
        self.stt_trigger = 1
        self.wal_retention_period = 0
        self.today_zero_ts = self.tdCom.genTodayZeroTs()
        self.today_zero_dt = self.tdCom.genTs(ts=self.today_zero_ts/1000)[1]
        self.restore_ts = self.today_zero_ts + 12 * 60 * 60 * 1000
        self.restore_dt = self.tdCom.genTs(ts=self.restore_ts/1000)[1]
        
        self.dbname = "test"
        self.stbname = "stb"
        self.keep_trying = -1
        self.trying_interval = 10
        self.vgroups = 1
        self.host = self.get_fqdn("taosd")[0]
        self.thread_count = 40
        self.num_of_records_per_req = 1000
        
        self.column_info_list = [
            {
              "type": "BIGINT",
              "count": 1,
              "gen": "order",
              "fillNull": "false"
            },
            {
              "type": "INT",
              "count": 2
            }
        ]
        self.tag_info_list = [
            {
              "type": "INT",
              "count": 1
            }
        ]
        self.json_file_name1 = "insert0.json"
        self.json_file_name2 = "insert1.json"
        self.json_data_list = list()
        self.taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        self.taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")


    def insert_data(self, drop, child_table_exists, row_count, start_timestamp, json_file_name):
        json_filename_list = [json_file_name]
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop=drop, wal_retention_period=self.wal_retention_period, stt_trigger=self.stt_trigger, keep=self.keep, duration=self.duration)
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=row_count, start_timestamp=start_timestamp, child_table_exists=child_table_exists, name=self.stbname, keep_trying=self.keep_trying, trying_interval=self.trying_interval)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, json_file_name, json_info)
        self.json_data_list = [json_info]
        self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)
        self.tdSql.execute(f'flush database {self.dbname}')

    def delete_wal(self, host, data_dir):
        self._remote.cmd(host, [f"rm -rf {data_dir}/vnode/vnode*/wal"])
        
    def run(self):
        self.insert_data("yes", "no", self.insert_rows1, self.today_zero_ts, self.json_file_name1)
        self.taosd.kill_by_port(self.sorted_dnodes[2]["endpoint"])
        self.insert_data("no", "yes", self.insert_rows2, self.restore_ts, self.json_file_name2)
        self.taosd.kill_by_port(self.sorted_dnodes[0]["endpoint"])
        self.taosd.kill_by_port(self.sorted_dnodes[1]["endpoint"])
        self.delete_wal(self.sorted_dnodes[0]["endpoint"].split(":")[0], self.sorted_dnodes[0]["config"]["dataDir"])
        self.delete_wal(self.sorted_dnodes[1]["endpoint"].split(":")[0], self.sorted_dnodes[1]["config"]["dataDir"])
        self.taosd.update_cfg('/tmp', self.taosd_setting , {"supportVnodes": self.cfg["boundary"][-1]}, self.sorted_dnodes[0]["endpoint"], True)
        self.taosd.update_cfg('/tmp', self.taosd_setting , {"supportVnodes": self.cfg["boundary"][-1]}, self.sorted_dnodes[1]["endpoint"], True)
        self.taosd.update_cfg('/tmp', self.taosd_setting , {"supportVnodes": self.cfg["boundary"][-1]}, self.sorted_dnodes[2]["endpoint"], True)
        self.tdSql.query(f'select count(*) from (select distinct(restored) from information_schema.ins_vnodes where db_name = "{self.dbname}");', count_expected_res=1)
        self.tdSql.checkEqual(self.tdSql.query_row, 1)

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            bug-ts5519
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write