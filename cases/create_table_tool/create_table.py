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
import os
import threading
from taostest.util.file import read_yaml, dict2file
from queue import Queue
from taostest.components.prometheus import PrometheusServer
from datetime import datetime

class CreateTable(TDCase):
    def __init__(self):
        self._remote: Remote = None
        self._fqdn: str = None
        self.case_configs = read_yaml(os.path.join(os.path.abspath(os.path.dirname(__file__)), "testcases.yaml"))
        self.create_table_cmd_dict = dict()
        # self.case_config_list = list()
        self.error_msg = None
        self.prometheus_settings = None

    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.prometheus = PrometheusServer(self._remote)
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "prometheus":
                self.prometheus_settings = env_setting

    def gen_create_table_cmd_dict(self):
        for case, case_config in self.case_configs["testcases"].items():
            case_config_list = list()
            for config in case_config:
                for fqdn in config["fqdn"]:
                    base_cmd = f'create_table -c {config["config_dir"]} '
                    case_config_dict = dict()
                    create_table_cmd_list = list()
                    case_config_dict["fqdn"] = fqdn
                    if "taos_config" in config:
                        dict2file(self.run_log_dir, "taos.cfg", config["taos_config"])
                        if "dataDir" in config["taos_config"]:
                            self._remote.mkdir(fqdn, config["taos_config"]["dataDir"])
                        if "logDir" in config["taos_config"]:
                            self._remote.mkdir(fqdn, config["taos_config"]["logDir"])
                        cfgPath = os.path.join(self.run_log_dir, "taos.cfg")
                        self._remote.mkdir(fqdn, config["config_dir"])
                        self._remote.put(fqdn, cfgPath, config["config_dir"])

                    if "db_name" in config:
                        base_cmd += f'-d {config["db_name"]} '
                    if "stb_name" in config:
                        base_cmd += f'-s {config["stb_name"]} '
                    if "thread_count" in config:
                        base_cmd += f'-t {config["thread_count"]} '
                    if "table_count" in config:
                        base_cmd += f'-n {config["table_count"]} '
                    if "start_offset" in config:
                        base_cmd += f'-g {config["start_offset"]} '
                    if "vgroup_count" in config:
                        base_cmd += f'-v {config["vgroup_count"]} '
                    if "create_table" in config:
                        base_cmd += f'-a {config["create_table"]} '
                    if "insert_data" in config:
                        base_cmd += f'-i {config["insert_data"]} '
                    if "batch_of_tbl" in config:
                        base_cmd += f'-b {config["batch_of_tbl"]} '
                    if "show_tables_flag" in config:
                        base_cmd += f'-w {config["show_tables_flag"]} '
                    if "query_flag" in config:
                        base_cmd += f'-q {config["query_flag"]} '
                    if "batch_of_row" in config:
                        base_cmd += f'-l {config["batch_of_row"]} '
                    if "total_rows_of_per_tbl" in config:
                        base_cmd += f'-q {config["total_rows_of_per_tbl"]} '
                    create_table_cmd_list.append(base_cmd)
                    case_config_dict["cmd_list"] = create_table_cmd_list
                    case_config_dict["case"] = case
                    case_config_list.append(case_config_dict)
                    self.create_table_cmd_dict[case] = case_config_list

    def threads_run_create_table(self):
        self.gen_create_table_cmd_dict()
        res_list = list()
        for case, case_config_list in self.create_table_cmd_dict.items():
            t = list()
            que = Queue()
            for case_config_dict in case_config_list:
                for cmd in case_config_dict["cmd_list"]:
                    t.append(threading.Thread(target=lambda q, arg1, arg2: q.put(self._remote.cmd(arg1, arg2)), args=(que, case_config_dict["fqdn"], [cmd])))
            import time
            for i in t:
                i.start()
            for i in t:
                start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                i.join()
                end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                while not que.empty():
                    res_dict = dict()
                    res_dict["casename"] = case
                    result = que.get()
                    res_dict["case_config"] = str(case_config_list[0])
                    case_config_list.pop(0)
                    res_dict["result"] = result.split('\n')[-1]
                    res_dict["start_time"] = start_time
                    res_dict["end_time"] = end_time
                    res_list.append(res_dict)

        for res_dict in res_list:
            f = open(f'{self.run_log_dir}/create_table.log', 'a')
            f.write("-------------------------------------------------------------------------\n\n")
            f.write(res_dict["case_config"] + "\n\n")
            f.write(res_dict["result"] + "\n\n")
            f.write("-------------------------------------------------------------------------\n\n\n\n")
            f.close()
            if self.prometheus_settings is not None:
                summary_res_dict, summary_dataframe_dict = self.prometheus.get_custom_query_range_datas(self.prometheus_settings, ["cpu_utilization"], res_dict["start_time"], res_dict["end_time"], 1)
                self.prometheus.export_res2md(f'{self.run_log_dir}/create_table.log', summary_res_dict, summary_dataframe_dict)

    def run(self) -> bool:
        self.threads_run_create_table()

    def cleanup(self):
        pass

    def desc(self) -> str:
        return """
            thread run create_table tool
        """

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Stable.Create

