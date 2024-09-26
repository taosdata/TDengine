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
# -*- taostest --setup=cluster/multi_stream_restart.yaml --case=cluster/multi_stream_restart.py --keep -*-

import os
from taostest.util.common import TDCom
from typing import List
from taostest import TDCase
from taostest.components.taosd import TaosD
from taostest.util.remote import Remote
from datetime import datetime
from copy import deepcopy
import pandas as pd
import copy

class MultiStreamRestart(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.cfg = self.tdCom.Boundary.DB_PARAM_VGROUPS_CONFIG
        # self.base_dnode_list = self.taosd_setting["spec"]["dnodes"]
        self.result_file_name = ""
        self._tmp_dir: str = os.path.join(self.run_log_dir, "tmp")
        self.replica = int(os.environ["DATABASE_REPLICAS"]) if "DATABASE_REPLICAS" in os.environ else 1
        self.vgroups = 3
        self.create_table_thread_count = 40
        self.thread_count = 200
        # self.thread_count = 10
        self.num_of_records_per_req = 100
        # self.num_of_records_per_req = 100
        self.childtable_count = 10000
        self.insert_rows = 1000000
        self.disorder_start_timestamp = "2018-01-01 00:00:00"
        self.fill_history_start_timestamp = "2020-01-01 00:00:00"
        self.stbname = "stb"
        self.dbname1 = "stream_test1"
        self.dbname2 = "stream_test2"
        self.dbname3 = "stream_test3"
        self.dbname4 = "stream_test4"
        self.dbname5 = "stream_test5"
        self.stream_stbname1 = "output_streamtb"
        self.stream_stbname2 = "output_streamtb"
        self.stream_stbname3 = "output_streamtb"
        self.stream_stbname4 = "output_streamtb"
        self.stream_stbname5 = "output_streamtb"
        self.stream_name1 = "test_stream1"
        self.stream_name2 = "test_stream2"
        self.stream_name3 = "test_stream3"
        self.stream_name4 = "test_stream4"
        self.stream_name5 = "test_stream5"
        self.trigger_mode = "at_once"
        self.child_table_exists = "no"
        self.db_drop = "yes"
        self.wal_retention_period = 1800
        self.stream_drop = "yes"
        self.keep_trying = -1
        self.trying_interval = 10000
        self.interlace_rows = 0

        self.primary_key = 0

        self.stream_sql1 = f"select _wstart,max(c0),min(c1) from {self.dbname1}.{self.stbname} where c1>0 interval(1s) sliding(1s)"
        self.stream_sql2 = f"select _wstart,max(c0),min(c1) from {self.dbname2}.{self.stbname} interval(10s)"
        self.stream_sql3 = f"select _wstart,max(c0),min(c1) from {self.dbname3}.{self.stbname} session(ts, 1s)"
        self.stream_sql4 = f"select _wstart,max(c0),min(c1) from {self.dbname4}.{self.stbname} partition by tbname state_window(c0)"
        self.stream_sql5 = f"select _wstart,max(c0),min(c1) from {self.dbname5}.{self.stbname} where c1>0 partition by tbname interval(1s) sliding(1s)"

        self.fill_history_rows = 100
        self.pre_num_of_records_per_req = 10000
        self.json_file_name1 = "insert0_0.json"
        self.json_file_name2 = "insert1_0.json"
        self.json_file_name3 = "insert2_0.json"
        self.json_file_name4 = "insert3_0.json"
        self.json_file_name5 = "insert4_0.json"
        self.json_file_name6 = "insert0_1.json"
        self.json_file_name7 = "insert1_1.json"
        self.json_file_name8 = "insert2_1.json"
        self.json_file_name9 = "insert3_1.json"
        self.json_file_name10 = "insert4_1.json"
        self.json_data_list = list()
        self.taosBenchmark_iplist: List = self.get_fqdn("taosBenchmark")
        self.taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        self.dnode_id_list = list()
        self.vgid = 1
        self.restart_dnode_id_list = list()
        self.query_vgid_interval = 60
        self.restart_dnode_interval = 60
        self.restore_timeout = 10800
        self.vgid_info_schedular = None
        self.restart_dnode_schedular = None
        self.use_stream = True
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

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def check_restored_true(self):
        """
        Check if the data is restored successfully.

        This method calls the `check_restored_true` method of the `tdCom` object
        to verify if the data is restored correctly. It also calls the `show_transactions`
        method to display the transactions.

        Args:
            self: The current object instance.
        """
        self.tdCom.check_restored_true(self._remote)
        self.show_transactions()

    def prepare_fill_history_data(self):
        """
        Prepares the data for filling history in multiple streams.

        This method sets up the necessary database and JSON configurations
        for filling history data in multiple streams. It generates JSON files
        containing the configuration information and sends them to the remote
        server. Finally, it runs the `taosBenchmark` tool on the remote server
        to fill history data.
        """
        self.json_filename_list = [self.json_file_name1, self.json_file_name2, self.json_file_name3, self.json_file_name4, self.json_file_name5]
        dbinfo1 = self.tdCom.setDBinfo(name=self.dbname1, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period)
        dbinfo2 = self.tdCom.setDBinfo(name=self.dbname2, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period)
        dbinfo3 = self.tdCom.setDBinfo(name=self.dbname3, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period)
        dbinfo4 = self.tdCom.setDBinfo(name=self.dbname4, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period)
        dbinfo5 = self.tdCom.setDBinfo(name=self.dbname5, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period)
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.fill_history_rows, start_timestamp=self.fill_history_start_timestamp, child_table_exists=self.child_table_exists, name=self.stbname, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, primary_key=self.primary_key)]
        database_info1 = [self.tdCom.setDatabases(dbinfo=dbinfo1, super_tables=stb_into)]
        database_info2 = [self.tdCom.setDatabases(dbinfo=dbinfo2, super_tables=stb_into)]
        database_info3 = [self.tdCom.setDatabases(dbinfo=dbinfo3, super_tables=stb_into)]
        database_info4 = [self.tdCom.setDatabases(dbinfo=dbinfo4, super_tables=stb_into)]
        database_info5 = [self.tdCom.setDatabases(dbinfo=dbinfo5, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info1 = self.tdCom.setJsoninfo(host=host, databases=database_info1, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.pre_num_of_records_per_req)
        json_info2 = self.tdCom.setJsoninfo(host=host, databases=database_info2, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.pre_num_of_records_per_req)
        json_info3 = self.tdCom.setJsoninfo(host=host, databases=database_info3, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.pre_num_of_records_per_req)
        json_info4 = self.tdCom.setJsoninfo(host=host, databases=database_info4, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.pre_num_of_records_per_req)
        json_info5 = self.tdCom.setJsoninfo(host=host, databases=database_info5, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.pre_num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name1, json_info1)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name2, json_info2)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name3, json_info3)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name4, json_info4)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name5, json_info5)
        self.json_data_list = [json_info1, json_info2, json_info3, json_info4, json_info5]
        self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)

    def insert_data(self):
        """
        Inserts data into the database using the specified parameters.
        """
        self.json_filename_list = [self.json_file_name6, self.json_file_name7, self.json_file_name8, self.json_file_name9, self.json_file_name10]
        self.start_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        self.child_table_exists = "yes"
        self.db_drop = "no"
        if self.use_stream:
            stream_db_info1 = self.tdCom.setStreamDBinfo(name=self.dbname1, vgroups=self.vgroups, drop=self.db_drop)
            stream_info1 = self.tdCom.setStreams(stream_name=self.stream_name1, stream_stb=f'{self.dbname1}.{self.stream_stbname1}', trigger_mode=self.trigger_mode, drop=self.stream_drop, source_sql=self.stream_sql1)
            stream_db_info2 = self.tdCom.setStreamDBinfo(name=self.dbname2, vgroups=self.vgroups, drop=self.db_drop)
            stream_info2 = self.tdCom.setStreams(stream_name=self.stream_name2, stream_stb=f'{self.dbname2}.{self.stream_stbname2}', trigger_mode=self.trigger_mode, drop=self.stream_drop, source_sql=self.stream_sql2)
            stream_db_info3 = self.tdCom.setStreamDBinfo(name=self.dbname3, vgroups=self.vgroups, drop=self.db_drop)
            stream_info3 = self.tdCom.setStreams(stream_name=self.stream_name3, stream_stb=f'{self.dbname3}.{self.stream_stbname3}', trigger_mode=self.trigger_mode, drop=self.stream_drop, source_sql=self.stream_sql3)
            stream_db_info4 = self.tdCom.setStreamDBinfo(name=self.dbname4, vgroups=self.vgroups, drop=self.db_drop)
            stream_info4 = self.tdCom.setStreams(stream_name=self.stream_name4, stream_stb=f'{self.dbname4}.{self.stream_stbname4}', trigger_mode=self.trigger_mode, drop=self.stream_drop, source_sql=self.stream_sql4)
            stream_db_info5 = self.tdCom.setStreamDBinfo(name=self.dbname5, vgroups=self.vgroups, drop=self.db_drop)
            stream_info5 = self.tdCom.setStreams(stream_name=self.stream_name5, stream_stb=f'{self.dbname5}.{self.stream_stbname5}', trigger_mode=self.trigger_mode, drop=self.stream_drop, source_sql=self.stream_sql5)
        dbinfo1 = self.tdCom.setDBinfo(name=self.dbname1, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop)
        dbinfo2 = self.tdCom.setDBinfo(name=self.dbname2, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop)
        dbinfo3 = self.tdCom.setDBinfo(name=self.dbname3, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop)
        dbinfo4 = self.tdCom.setDBinfo(name=self.dbname4, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop)
        dbinfo5 = self.tdCom.setDBinfo(name=self.dbname5, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop)
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, primary_key=self.primary_key)]
        database_info1 = [self.tdCom.setDatabases(dbinfo=dbinfo1, super_tables=stb_into)]
        database_info2 = [self.tdCom.setDatabases(dbinfo=dbinfo2, super_tables=stb_into)]
        database_info3 = [self.tdCom.setDatabases(dbinfo=dbinfo3, super_tables=stb_into)]
        database_info4 = [self.tdCom.setDatabases(dbinfo=dbinfo4, super_tables=stb_into)]
        database_info5 = [self.tdCom.setDatabases(dbinfo=dbinfo5, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        if self.use_stream:
            json_info1 = self.tdCom.setJsoninfo(host=host, databases=database_info1, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, streams=stream_info1, stream_db=stream_db_info1, num_of_records_per_req=self.num_of_records_per_req)
            json_info2 = self.tdCom.setJsoninfo(host=host, databases=database_info2, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, streams=stream_info2, stream_db=stream_db_info2, num_of_records_per_req=self.num_of_records_per_req)
            json_info3 = self.tdCom.setJsoninfo(host=host, databases=database_info3, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, streams=stream_info3, stream_db=stream_db_info3, num_of_records_per_req=self.num_of_records_per_req)
            json_info4 = self.tdCom.setJsoninfo(host=host, databases=database_info4, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, streams=stream_info4, stream_db=stream_db_info4, num_of_records_per_req=self.num_of_records_per_req)
            json_info5 = self.tdCom.setJsoninfo(host=host, databases=database_info5, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, streams=stream_info5, stream_db=stream_db_info5, num_of_records_per_req=self.num_of_records_per_req)
        else:
            json_info1 = self.tdCom.setJsoninfo(host=host, databases=database_info1, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
            json_info2 = self.tdCom.setJsoninfo(host=host, databases=database_info2, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
            json_info3 = self.tdCom.setJsoninfo(host=host, databases=database_info3, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
            json_info4 = self.tdCom.setJsoninfo(host=host, databases=database_info4, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
            json_info5 = self.tdCom.setJsoninfo(host=host, databases=database_info5, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name6, json_info1)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name7, json_info2)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name8, json_info3)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name9, json_info4)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name10, json_info5)
        self.json_data_list = [json_info1, json_info2, json_info3, json_info4, json_info5]
        self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)

    def show_transactions(self):
        """
        Retrieves and displays the transactions using the `show transactions` command.
        """
        self.tdSql.query('show transactions')
        self._remote._logger.info(pd.DataFrame(self.tdSql.query_data))

    def restart_dnodes(self):
        """
        Restarts the dnodes in the cluster.

        This method retrieves the list of endpoints for the dnodes to be restarted,
        creates a copy of the taosd setting, updates the taosd configuration with the
        new setting, and checks if the restoration is successful.
        """
        restart_endpoint_list = self.tdCom.get_fqdn_by_dnode_id(self.dnode_id_list)
        for endpoint in restart_endpoint_list:
            taosd_setting = copy.deepcopy(self.taosd_setting)
            self.taosd.update_cfg('/tmp', taosd_setting, {"supportVnodes": self.cfg["boundary"][-1]}, endpoint, True)
        self.check_restored_true()

    def run(self):
        self.prepare_fill_history_data()
        self.dnode_id_list = self.tdCom.get_dnode_id_list()
        self.restart_dnode_schedular = self.tdCom.add_back_ground_scheduler(self.restart_dnodes, "interval", seconds=self.restart_dnode_interval, max_instances=1, args=[])
        self.insert_data()
        self.check_restored_true()
