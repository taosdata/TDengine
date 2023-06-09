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

import os
from taostest.util.file import read_yaml
from taostest.util.common import TDCom
from taostest import TDCase
from taostest.util.remote import Remote
from datetime import datetime
from taostest.performance.result_reduction import Perf_Base_func
import sys
from taostest.util.msg import Msg, TaosBenchmark
import getpass
class NoPartitionStreamStabilityTest(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.host = self.get_fqdn("taosd")[0]
        
        self.current_dir = os.path.dirname(os.path.realpath(__file__))
        self.result_file_name = self.run_log_dir + '/perf_report.txt'
        self.json_file = os.path.join(self.current_dir, "stream_exist_stb_tag_prepare.json")
        # self.json_file = os.path.join(self.current_dir, "test1.json")
        self.json_info = self.tdCom.load_json(self.json_file)
        self.json_info["test_log"] = self.run_log_dir
        self.json_info["host"] = self.host
        self.taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        self.taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        self.dbname = self.json_info["databases"][0]["dbinfo"]["name"]
        self.vgroups = self.json_info["databases"][0]["dbinfo"]["vgroups"]
        self.childtable_count = self.json_info["databases"][0]["super_tables"][0]["childtable_count"]
        self.pre_rows = self.json_info["databases"][0]["super_tables"][0]["insert_rows"]
        self.stbname = self.json_info["databases"][0]["super_tables"][0]["name"]
        self.childtable_prefix = self.json_info["databases"][0]["super_tables"][0]["childtable_prefix"]
        self.stream_json_file = os.path.join(self.current_dir, "stream_exist_stb_tag_insert.json")
        # self.stream_json_file = os.path.join(self.current_dir, "test2.json")
        self.stream_json_info = self.tdCom.load_json(self.stream_json_file)
        self.insert_rows = self.stream_json_info["databases"][0]["super_tables"][0]["insert_rows"]
        self.stream_name = self.stream_json_info["streams"][0]["stream_name"]
        self.stream_sql = self.stream_json_info["streams"][0]["source_sql"]
        self.trigger_mode = self.stream_json_info["streams"][0]["trigger_mode"]
        self.stream_dbname, self.stream_stbname= self.stream_json_info["streams"][0]["stream_stb"].split(".")
        self.json_file_info_list = list()
        self.json_file_info_list.append({self.json_file: self.json_info})
        self.json_file_info_list.append({self.stream_json_file: self.stream_json_info})
        
        self.msg = Msg()
        self.taosbenchmark = TaosBenchmark()
        self.exec_cmd = ' '.join(sys.argv[::])

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def run(self):
        start_time = datetime.now()
        timestamp_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        for json_file_info in self.json_file_info_list:
            for json_file, json_info in json_file_info.items():
                with open(json_info["result_file"], "w") as f:
                    f.truncate()
                json_data_list = list()
                json_filename_list = list()
                json_filename = os.path.split(json_file)[1]
                json_filename_list.append(json_filename)
                json_info["test_log"] = os.path.split(json_file)[0] + "/"
                self.tdCom.dump_json(f'{self.run_log_dir}/{json_filename}', json_info)
                json_data_list.append(json_info)
                result_file_list = self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, json_data_list, json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)
                
        timestamp_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        env_setting = self.get_component_by_name("prometheus")
        Insert_file = Perf_Base_func(self.logger, self.run_log_dir)
        Insert_file.taosBenchmark_insert_summary_result(result_file_list, version="3.0")
        Insert_file.get_process_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
        Insert_file.get_node_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
        if self.trigger_mode.lower() == "at_once" and "ignore_expired 1" not in self.stream_sql:
            self.tdSql.query(self.stream_sql)
            expected_res = self.tdSql.query_row
            self.tdSql.query(f'select count(*) from {self.dbname}.{self.stream_stbname}')
            self.tdSql.checkEqual(self.tdSql.query_data[0][0], expected_res)
        end_time = datetime.now()
        
        res_msg = self.taosbenchmark.confirm_res(json_info["result_file"])
        text = f'''result: {res_msg}
test scope: stream stability test
owner: Jayden Jia
hostname: {self.host}
start time: {start_time}
end time: {end_time}
report dir: {getpass.getuser()}@{self.host}:{self.result_file_name}
cmd: {self.exec_cmd}
others: none'''
        self.msg.send_msg(self.msg.get_msg(text))