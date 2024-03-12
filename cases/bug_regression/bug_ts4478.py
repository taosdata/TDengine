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
from typing import List
from taostest import TDCase
from taostest.components.taosd import TaosD
from taostest.util.remote import Remote
from copy import deepcopy
import random

class TestTs4478(TDCase):
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
        self.replica = 3
        self.vgroups = 10
        self.create_table_thread_count = 40
        self.thread_count = 10
        # self.thread_count = 10
        self.num_of_records_per_req = 100
        # self.num_of_records_per_req = 100
        self.childtable_count = 50000
        self.insert_rows = 100000
        self.start_timestamp = "2020-01-01 00:00:00"
        self.stbname = "stb"
        self.dbname = "stream_test"
        self.stream_stbname = "output_streamtb"
        self.child_table_exists = "no"
        self.db_drop = "yes"
        self.wal_retention_period = 300
        self.keep_trying = -1
        self.trying_interval = 10000
        self.interlace_rows = 0
        self.disorder_ratio = 10
        self.update_ratio = 5
        self.delete_ratio = 1
        self.disorder_fill_interval = 300
        self.update_fill_interval = 25
        self.generate_row_rule = 2
        self.pre_num_of_records_per_req = 10000
        self.json_file_name1 = "insert0.json"
        self.json_file_name2 = "insert1.json"
        self.json_file_name3 = "insert2.json"
        self.json_data_list = list()
        self.taosBenchmark_iplist: List = self.get_fqdn("taosBenchmark")
        self.taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        self.query_schedular_interval = 100
        self.alter_schema_schedular_interval = 60
        self.column_info_list = [
            {
              "type": "INT",
              "count": 2
            }
        ]
        self.tag_info_list = [
            {
              "type": "INT",
              "count": 3
            }
        ]
        self.query_sql_list = [
            f'select tbname from {self.dbname}.{self.stbname} where t1 > 0',
            f'select count(t0) from {self.dbname}.{self.stbname} where t0 > 0',
            f'select last(t1) from {self.dbname}.{self.stbname} where t0 > 0',
            f'select last(t2) from {self.dbname}.{self.stbname} where t1 > 0',
            f'select last_row(t2) from {self.dbname}.{self.stbname} where t0 > 0',
            f'select last_row(t0) from {self.dbname}.{self.stbname} where t2 > 0',
            f'select count(tbname) from {self.dbname}.{self.stbname} interval(1s)',
            f'select count(tbname) from {self.dbname}.{self.stbname} interval(1m)',
            f'select count(tbname) from {self.dbname}.{self.stbname} interval(1d)',
            f'select count(tbname) from {self.dbname}.{self.stbname} interval(7d)',
            f'select count(tbname) from {self.dbname}.{self.stbname} partition by tbname interval(1s)',
            f'select count(tbname) from {self.dbname}.{self.stbname} partition by tbname interval(1m)',
            f'select count(tbname) from {self.dbname}.{self.stbname} partition by tbname interval(1d)',
            f'select count(tbname) from {self.dbname}.{self.stbname} partition by tbname interval(7d)',
            f'select tbname, t0, t1, t2 from {self.dbname}.{self.stbname} where t0 > 0',
            f'select tbname, t0, t1, t2 from {self.dbname}.{self.stbname} order by t0, t1',
            f'select tbname, t0, t1, t2 from {self.dbname}.{self.stbname} order by t0, t1 desc',
            f'select tbname, t0, t1, t2 from {self.dbname}.{self.stbname} limit 100',
            f'select tbname, t0, t1, t2 from {self.dbname}.{self.stbname} limit 100 offset 10000',
            f'select last(t1) from {self.dbname}.{self.stbname} group by tbname slimit 10000',
            f'select avg(t1), max(t0), min(t1) from {self.dbname}.{self.stbname} interval(1s)',
            f'select avg(t1), max(t0), min(t1) from {self.dbname}.{self.stbname} partition by tbname limit 10000',
            f'select sum(t1) from {self.dbname}.{self.stbname} group by t1',
            f'select avg(t1) from {self.dbname}.{self.stbname}',
            f'select max(t1) from {self.dbname}.{self.stbname}',
            f'select min(t1) from {self.dbname}.{self.stbname}',
        ]
        self.query_sql_list = [
            f'select tbname from {self.dbname}.{self.stbname} where t1 > 0',
            f'select tags t1 from {self.dbname}.{self.stbname} where t1 > 0',
            f'select last(t1) from {self.dbname}.{self.stbname} where t0 > 0',
            f'select last(t2) from {self.dbname}.{self.stbname} where t1 > 0',
            f'select last_row(t2) from {self.dbname}.{self.stbname} where t0 > 0',
            f'select last_row(t0) from {self.dbname}.{self.stbname} where t2 > 0',
            f'select count(t0) from {self.dbname}.{self.stbname} where t0 > 0',
            f'select count(tbname) from {self.dbname}.{self.stbname} interval(1s)',
            f'select count(tbname) from {self.dbname}.{self.stbname} interval(1m)',
            f'select count(tbname) from {self.dbname}.{self.stbname} interval(1d)',
            f'select count(tbname) from {self.dbname}.{self.stbname} interval(7d)',
            f'select tbname, t0, t1, t2 from {self.dbname}.{self.stbname} where t0 > 0',
            f'select tbname, t0, t1, t2 from {self.dbname}.{self.stbname} order by t0, t1',
            f'select tbname, t0, t1, t2 from {self.dbname}.{self.stbname} order by t0, t1 desc',
            f'select tbname, t0, t1, t2 from {self.dbname}.{self.stbname} limit 100',
            f'select tbname, t0, t1, t2 from {self.dbname}.{self.stbname} limit 100 offset 10000',
            f'select avg(t1), max(t0), min(t1) from {self.dbname}.{self.stbname} interval(1s)',
            f'select avg(t1) from {self.dbname}.{self.stbname}',
            f'select max(t1) from {self.dbname}.{self.stbname}',
            f'select min(t1) from {self.dbname}.{self.stbname}',
        ]
        self.concurrent = len(self.query_sql_list)
    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def alter_schema(self):
        self.tdSql.query('select count(*) from information_schema.ins_tables;')
        table_count = self.tdSql.query_data[0][0]
        ctbname = f'ctb{random.randint(1, table_count)}'
        self.tdSql.query(f'select count(*) from {self.dbname}.{self.stbname};')
        if len(self.tdSql.query_data) > 0:
            if self.tdSql.query_data[0][0] > 0:
                self.tdSql.execute(f'alter table {self.dbname}.{ctbname} set tag t0 = 1')

    def insert_datas(self):
        self.json_filename_list = [self.json_file_name1]
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period)
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, child_table_exists=self.child_table_exists, name=self.stbname, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, start_timestamp=self.start_timestamp)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.pre_num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name1, json_info)
        self.json_data_list = [json_info]
        self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)

    def thread_query(self):
        self.tdCom.multi_thread_query(tbname=None, query_sql_list=self.query_sql_list, concurrent=self.concurrent)

    def run(self):
        self.tdCom.add_back_ground_scheduler(self.thread_query, "interval", seconds=self.query_schedular_interval, max_instances=10, args=[])
        self.tdCom.add_back_ground_scheduler(self.alter_schema, "interval", seconds=self.alter_schema_schedular_interval, max_instances=100, args=[])
        self.insert_datas()
