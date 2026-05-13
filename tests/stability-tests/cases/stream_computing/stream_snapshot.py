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
# -*- taostest --setup=cluster/redistribute_split_test.yaml --case=cluster/redistribute_test.py --keep -*-
# -*- taostest --setup=cluster/redistribute_split_test_rep3.yaml --case=cluster/redistribute_test.py --keep -*-

import os
from taostest.util.common import TDCom
from taostest import TDCase
from taostest.components.taosd import TaosD
from taostest.util.remote import Remote
import random
import time

class StreamRedistribute(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.cfg = self.tdCom.Boundary.DB_PARAM_VGROUPS_CONFIG
        # self.base_dnode_list = self.taosd_setting["spec"]["dnodes"]
        self.reserve_dnode_list = self.taosd_setting["spec"]["reserve_dnodes"]
        self._tmp_dir: str = os.path.join(self.run_log_dir, "tmp")
        self.replica = int(os.environ["DATABASE_REPLICAS"]) if "DATABASE_REPLICAS" in os.environ else 1
        self.vgroups = 2
        self.create_table_thread_count = 40
        self.thread_count = 40
        self.childtable_count = 1000
        self.insert_rows = 10000
        self.fill_history_start_timestamp = "2020-01-01 00:00:00"
        self.stbname = "stb"
        self.dbname = "stream_test"
        self.stream_stbname = "output_streamtb"
        self.stream_name = "test_stream"
        self.trigger_mode = "at_once"
        self.child_table_exists = "no"
        self.db_drop = "yes"
        self.stream_drop = "yes"
        self.keep_trying = -1
        self.trying_interval = 10000
        self.interlace_rows = 0
        self.stream_sql = f"select ts,max(c1) from {self.dbname}.{self.stbname} where c1>0 interval(1s)"
        self.fill_history_rows = 10000
        self.pre_num_of_records_per_req = 10000
        self.json_file_name1 = "insert0.json"
        self.json_data_list = list()
        self.taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        self.taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        self.host = self.get_fqdn("taosd")[0]
        self.restart_dnode_id_list = list()
        self.tdSql.query('show dnodes')
        self.source_dnode_id_list = list(map(lambda x:x[0], self.tdSql.query_data))
        self.cluster_to_redistribute_list = list()
        self.column_info_list = [
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

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def get_cluster_to_redistribute_list(self, reserver_dnode_count):
        tmp_dnode_id_list = self.tdCom.get_dnode_id_list()
        reserve_dnode_id_list = [x for x in tmp_dnode_id_list if x not in self.source_dnode_id_list]
        if self.replica == 1:
            self.cluster_to_redistribute_list = random.sample(reserve_dnode_id_list, self.replica)
        elif self.replica == 3:
            self.cluster_to_redistribute_list = random.sample(self.source_dnode_id_list, self.replica-reserver_dnode_count) + random.sample(reserve_dnode_id_list, reserver_dnode_count)

    def snapshot_prepare(self):
        self.tdSql.execute(f'flush database {self.dbname}')
        self.clean_wal()
        self.restart_dnode()

    def clean_wal(self):
        # killCmd = "ps -ef|grep -wi %s | grep -v grep | awk '{print $2}' | xargs kill -15 > /dev/null 2>&1" % (self.taosd_setting["spec"]["dnodes"][0]["config_dir"])
        vgid_list = self.tdCom.get_vgid_list(self.dbname)
        self.taosd.kill_by_config_dir(self.taosd_setting["spec"]["dnodes"][0])
        # self._remote.cmd(self.host, [killCmd])
        data_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"]
        for vgid in vgid_list:
            self._remote.cmd(self.host, f'rm -rf {data_dir}/vnode/vnode{vgid}/wal/*')

    def restart_dnode(self):
        self.taosd.update_cfg('/tmp', self.taosd_setting , {"supportVnodes": self.cfg["boundary"][-1]}, self.taosd_setting["spec"]["dnodes"][0]["endpoint"], True)

    def redistribute(self):
        self.restart_dnode_id_list = list()
        self.taosd.add_reserve_dnodes(self._tmp_dir, self.taosd_setting, self.reserve_dnode_list)
        self.get_cluster_to_redistribute_list(len(self.reserve_dnode_list))
        redistribute_dnode_id_str = str()
        for redistribute_dnode_id in self.cluster_to_redistribute_list:
            redistribute_dnode_id_str += f"dnode {redistribute_dnode_id} "
        for vgid in self.tdCom.get_vgid_list(self.dbname):
            self.tdSql.execute(f'redistribute vgroup {vgid} {redistribute_dnode_id_str}')

    def prepare_data(self):
        self.json_filename_list = [self.json_file_name1]
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop)
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.fill_history_rows, start_timestamp=self.fill_history_start_timestamp, child_table_exists=self.child_table_exists, name=self.stbname, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        stream_db_info = self.tdCom.setStreamDBinfo(name=self.dbname, vgroups=self.vgroups, drop=self.db_drop)
        stream_info = self.tdCom.setStreams(stream_name=self.stream_name, stream_stb=f'{self.dbname}.{self.stream_stbname}', trigger_mode=self.trigger_mode, drop=self.stream_drop, source_sql=self.stream_sql)
        json_info = self.tdCom.setJsoninfo(host=self.host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.pre_num_of_records_per_req, streams=stream_info, stream_db=stream_db_info)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name1, json_info)
        self.json_data_list = [json_info]
        self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)

    def run(self):
        self.prepare_data()
        time.sleep(self.taosd_setting["spec"]["dnodes"][0]["config"]["checkpointInterval"] + 1)
        self.snapshot_prepare()
        self.redistribute()
