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
# -*- taostest --setup=bug_regression/improve_td26412.yaml --case=bug_regression/improve_td26412.py --keep -*-

from taostest import TDCase, T
from taostest.util.common import TDCom
from datetime import datetime
import os
from taostest.util.remote import Remote

class TestTd20239(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        self.host = self.taosd_setting["spec"]["dnodes"][0]["endpoint"].split(":")[0]
        self._remote: Remote = Remote(self.logger)
        self.json_file_name1 = "insert0.json"
        self.replica = int(os.environ["DATABASE_REPLICAS"]) if "DATABASE_REPLICAS" in os.environ else 1
        self.vgroups = 10
        self.create_table_thread_count = 40
        self.thread_count = 40
        self.num_of_records_per_req = 1000
        self.childtable_count = 1000000
        self.insert_rows = 100000
        self.stbname = "mtstb"
        self.dbname = "mtdb"
        self.childtable_limit = 10
        self.childtable_offset = 10
        self.batch_create_tbl_num = 20000
        self.insert_mode = "sml"
        self.line_protocol = "line"
        self.stream_stbname1 = "sumvalue"
        self.stream_stbname2 = "sumvalueH5"
        self.stream_name1 = "sumstream"
        self.stream_name2 = "sumstreamh5"
        self.trigger_mode = "at_once"
        self.stream_sql1 = f'SELECT SUM(val) FROM {self.dbname}.{self.stbname} PARTITION BY tbname INTERVAL(1m);'
        self.stream_sql2 = f'SELECT SUM(val) FROM {self.dbname}.{self.stbname} where col1>=5 PARTITION BY tbname INTERVAL(1m);'
        self.child_table_exists = "no"
        self.db_drop = "yes"
        self.wal_retention_period = 3600
        self.stream_drop = "yes"
        self.taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        self.taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        self.keep_trying = -1
        self.trying_interval = 10000
        self.timestamp_step = 100000
        self.interlace_rows = 1
        self.column_info_list = [
            {"type": "INT", "name": "val", "min": 1, "max": 900000000},
            {"type": "INT", "name": "col1", "min": 1, "max": 10},
            {"type": "FLOAT", "name": "phase", "min": 1, "max": 8000, "count": 7},
            {"type": "DOUBLE", "name": "press", "min": -9000, "max": 9000, "count": 2},
            {"type": "BINARY", "name": "station", "values": ["tag1", "tag2", "tag3", "tag4", "tag5", "tag6"], "len": 8, "count": 10}
        ]
        self.tag_info_list = [
            {"name": "location", "type": "INT", "min": 1, "max": 10000001}
        ]

    def pre_insert(self):
        self.json_filename_list = [self.json_file_name1]
        self.start_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period)
        stb_into = [self.tdCom.setStbinfo(name=self.stbname, columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=1, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, insert_mode=self.insert_mode, line_protocol=self.line_protocol, childtable_limit=self.childtable_limit, childtable_offset=self.childtable_offset, batch_create_tbl_num=self.batch_create_tbl_num)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name1, json_info)
        self.json_data_list = [json_info]
        self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)

    def insert_data(self):
        self.child_table_exists = "yes"
        self.db_drop = "no"
        self.json_filename_list = [self.json_file_name1]
        self.start_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        stream_db_info = self.tdCom.setStreamDBinfo(name=self.dbname, vgroups=self.vgroups, drop=self.db_drop)
        stream_info = self.tdCom.setStreams(stream_name=self.stream_name1, stream_stb=f'{self.dbname}.{self.stream_stbname1}', trigger_mode=self.trigger_mode, drop=self.stream_drop, source_sql=self.stream_sql1)
        stream_info2 = self.tdCom.setStreams(stream_name=self.stream_name2, stream_stb=f'{self.dbname}.{self.stream_stbname2}', trigger_mode=self.trigger_mode, drop=self.stream_drop, source_sql=self.stream_sql2)
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period)
        stb_into = [self.tdCom.setStbinfo(name=self.stbname, columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, insert_mode=self.insert_mode, line_protocol=self.line_protocol, childtable_limit=self.childtable_limit, childtable_offset=self.childtable_offset, batch_create_tbl_num=self.batch_create_tbl_num)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, streams=stream_info, stream_db=stream_db_info, num_of_records_per_req=self.num_of_records_per_req)
        json_info["streams"].append(stream_info2)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name1, json_info)
        self.json_data_list = [json_info]
        self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)

    def run(self):
        self.pre_insert()
        self.insert_data()
        self.tdSql.query(f'select count(*) from ({self.stream_sql1})')
        expected_res1 = self.tdSql.query_data[0][0]
        self.tdSql.query(f'select count(*) from ({self.stream_stbname1})')
        self.tdSql.checkEqual(expected_res1, self.tdSql.query_data[0][0])
        self.tdSql.query(f'select count(*) from ({self.stream_sql2})')
        expected_res2 = self.tdSql.query_data[0][0]
        self.tdSql.query(f'select count(*) from ({self.stream_stbname2})')
        self.tdSql.checkEqual(expected_res2, self.tdSql.query_data[0][0])

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            test_td20239
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write