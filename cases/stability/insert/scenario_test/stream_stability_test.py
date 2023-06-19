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
import sys, getopt
from taostest.util.msg import Msg, TaosBenchmark
import getpass
import socket

class StreamStabilityTest(TDCase):
    prepare_param_file = "prepare-param-file"
    insert_param_file = "insert-param-file"
    def init(self):
        self.prepare_param_filename = str()
        self.insert_param_filename = str()

        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.host = self.get_fqdn("taosd")[0]
        self.host_list = list()
        self.host_list.append(self.host)
        self.json_file_info_list = list()

        self.current_dir = os.path.dirname(os.path.realpath(__file__))
        self.result_file_name = self.run_log_dir + '/perf_report.txt'
        self.taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        self.taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        self.host_list = self.taosBenchmark_iplist if self.host in self.taosBenchmark_iplist else self.host_list + self.taosBenchmark_iplist

        self.msg = Msg()
        self.taosbenchmark = TaosBenchmark()
        self.exec_cmd = ' '.join(sys.argv[::])

        self.taosBenchmark_json_path = "/tmp/"

    def help(self):
        print("case parameters:")
        print(f"\t--{StreamStabilityTest.prepare_param_file}")
        print(f"\t--{StreamStabilityTest.insert_param_file}")

    # parse case parameters
    def parse_case_param(self):
        try:
            if self.case_param is None:
                self.set_error_msg("no case parameter specified")
                return False
            self._remote._logger.debug(f"case parameters: [{self.case_param}]")
            param_array = self.case_param.split(" ")
            # parse parameters
            opts, _ = getopt.getopt(param_array, "h", ["help", f"{StreamStabilityTest.prepare_param_file}=", f"{StreamStabilityTest.insert_param_file}="])
            self._remote._logger.debug(str(opts))
            for key, val in opts:
                self._remote._logger.debug("key: {} value: {}".format(key, val))
                if key in (f"--{StreamStabilityTest.prepare_param_file}"):
                    self.prepare_param_file = val
                    self.prepare_param_filename = os.path.split(val)[1]
                elif key in (f"--{StreamStabilityTest.insert_param_file}"):
                    self.insert_param_file = val
                    self.insert_param_filename = os.path.split(val)[1]
                else:
                    self._remote._logger.error(f"invalid case parameter: {key}")
                    self.set_error_msg(f"invalid case parameter: {key}")
                    return False
            for case_file in [self.prepare_param_file, self.insert_param_file]:
                # check parameters
                if case_file is None:
                    self._remote._logger.error(f"case parameter {case_file} not specified")
                    self.set_error_msg(f"case parameter {case_file} not specified")
                    return False
                # get full path
                full_case_file = os.path.join(os.environ["TEST_ROOT"], case_file)
                # check file existance
                if not os.path.isfile(full_case_file):
                    self._remote._logger.error(f"{full_case_file} not exist")
                    self.set_error_msg(f"{full_case_file} not exist")
                    return False
                # if not case_file is None:
                #     # get full path
                #     case_file = os.path.join(os.environ["TEST_ROOT"], case_file)
                #     # check file existance
                #     if not os.path.isfile(case_file):
                #         self._remote._logger.error(f"{case_file} not exist")
                #         self.set_error_msg(f"{case_file} not exist")
                #         return False
        except getopt.GetoptError:
            self._remote._logger.error(f"parameter parse error [{self.case_param}]")
            self.set_error_msg(f"parameter parse error [{self.case_param}]")
            return False
        return True

    def init_params(self):
        self.json_file = os.path.join(self.current_dir, self.prepare_param_filename)
        # self.json_file = os.path.join(self.current_dir, "test1.json")
        self.json_info = self.tdCom.load_json(self.json_file)
        self.json_info["host"] = self.host
        self.dbname = self.json_info["databases"][0]["dbinfo"]["name"]
        self.vgroups = self.json_info["databases"][0]["dbinfo"]["vgroups"]
        self.childtable_count = self.json_info["databases"][0]["super_tables"][0]["childtable_count"]
        self.pre_rows = self.json_info["databases"][0]["super_tables"][0]["insert_rows"]
        self.stbname = self.json_info["databases"][0]["super_tables"][0]["name"]
        self.childtable_prefix = self.json_info["databases"][0]["super_tables"][0]["childtable_prefix"]
        self.stream_json_file = os.path.join(self.current_dir, self.insert_param_filename)
        # self.stream_json_file = os.path.join(self.current_dir, "test3.json")
        self.stream_json_info = self.tdCom.load_json(self.stream_json_file)
        self.stream_json_info["host"] = self.host
        self.insert_rows = self.stream_json_info["databases"][0]["super_tables"][0]["insert_rows"]
        self.stream_name = self.stream_json_info["streams"][0]["stream_name"]
        self.stream_sql = self.stream_json_info["streams"][0]["source_sql"]
        self.trigger_mode = self.stream_json_info["streams"][0]["trigger_mode"]
        self.stream_dbname, self.stream_stbname= self.stream_json_info["streams"][0]["stream_stb"].split(".")
        self.json_file_info_list.append({self.json_file: self.json_info})
        self.json_file_info_list.append({self.stream_json_file: self.stream_json_info})

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def run(self):
        ret = self.parse_case_param()
        self.init_params()
        if ret == False:
            self._remote._logger.info("error in case paramters")
            self.help()
            return False
        self._remote._logger.info("CONFIG FILE: %s", self.config_file)

        start_time = datetime.now()
        timestamp_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        for json_file_info in self.json_file_info_list:
            for json_file, json_info in json_file_info.items():
                json_info["result_file"] = self.taosBenchmark_json_path + "taosBenchmark_" + os.path.split(json_file)[1] + ".log"
                with open(json_info["result_file"], "w") as f:
                    f.truncate()
                json_data_list = list()
                json_filename_list = list()
                json_filename = os.path.split(json_file)[1]
                json_filename_list.append(json_filename)
                json_info["test_log"] = self.taosBenchmark_json_path
                self.tdCom.dump_json(f'{self.run_log_dir}/{json_filename}', json_info)
                self.tdCom.dump_json(f'{self.taosBenchmark_json_path}{json_filename}', json_info)
                json_data_list.append(json_info)
                self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, json_data_list, json_filename_list, self.run_log_dir)
                result_file_list = self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, json_data_list, json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)

        timestamp_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        env_setting = self.get_component_by_name("prometheus")
        Insert_file = Perf_Base_func(self._remote._logger, self.run_log_dir)
        Insert_file.taosBenchmark_insert_summary_result(result_file_list, version="3.0")
        Insert_file.get_process_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
        Insert_file.get_node_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
        if self.trigger_mode.lower() == "at_once" and "ignore_expired 1" not in self.stream_sql:
            self.tdSql.query(self.stream_sql)
            expected_res = self.tdSql.query_row
            self.tdSql.query(f'select count(*) from {self.dbname}.{self.stream_stbname}')
            self.tdSql.checkEqual(self.tdSql.query_data[0][0], expected_res)
        end_time = datetime.now()
        for host in self.taosBenchmark_iplist:
            self._remote.get(host, json_info["result_file"], self.run_log_dir)
        res_msg = self.taosbenchmark.confirm_res(f'{self.run_log_dir}/taosBenchmark_{os.path.split(json_file)[1]}.log')
        report_file = f'{getpass.getuser()}@{socket.gethostname()}:{self.result_file_name}'
        text = f'''result: {res_msg}
test scope: stream stability test
owner: Jayden Jia
hostname: {self.host_list}
start time: {start_time}
end time: {end_time}
report file: {report_file}
cmd: {self.exec_cmd}
others: none'''
        self.msg.send_msg(self.msg.get_msg(text))
        with open(self.result_file_name, "r") as file:
            file_content = file.read()
            self._remote._logger.info(f"final result:\n\n{file_content}")