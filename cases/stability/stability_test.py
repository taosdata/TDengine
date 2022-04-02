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
from taostest.util.rest import TDRest
from taostest.util.remote import Remote
import socket
import os
import random
from apscheduler.schedulers.background import BackgroundScheduler
from taostest.util.file import dict2yaml, read_yaml
import datetime
import re
import time
from taostest.performance.perfor_basic import InsertFile
from taostest.performance.result_reduction import Perf_Base_func

class TestStability(TDCase):
    def __init__(self):
        self._remote: Remote = None
        self._fqdn: str = None
        self.taosadapter_ip_port_dict = dict()
        self.env_setting = None
        self.agent_settings = None
        self.taosadapter_settings = None
        self.error_msg = None
        self.env_dir = None
        self.tmp_yaml = None
        self.stability_config = read_yaml(os.path.join(os.path.abspath(os.path.dirname(__file__)), "stability_config.yaml"))

    def init(self):
        self._remote: Remote = Remote(self.logger)
        self.tdCom = TDCom(self.tdSql)
        self.tdRest = TDRest()

        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "agent":
                self.agent_settings = env_setting
            elif env_setting["name"].lower() == "taosadapter":
                self.taosadapter_settings = env_setting
            elif env_setting["name"].lower() == "taospy":
                self.taospy_settings = env_setting
            elif env_setting["name"].lower() == "taosd":
                self.taosd_settings = env_setting
        # self.agent_settings = self.get_component_by_name("agent")[0]

        # self.taosadapter_settings = self.get_component_by_name("taosadapter")[0]

    def insert_data_with_taosBenchmark(self):
        taosBenchmark_fqdn_list = self.get_fqdn("taosBenchmark")
        taosd_fqdn = self.get_fqdn("taosd")[0]
        json_data = []
        file_name = []

        jfile = InsertFile()
        Insert_file = Perf_Base_func(self.logger, self.run_log_dir)
        col = jfile.schemacfg(intcount=4, doublecount=4, tscount=1)

        tag = jfile.schemacfg(intcount=1)

        dbname = self.stability_config["query_dbname"]
        for i in range(len(taosBenchmark_fqdn_list)):
            db = jfile.setDBinfo(name=dbname, drop="yes")
            stb = jfile.setStbinfo(name="stb", childtable_prefix="stb_" + str(i), childtable_count=self.stability_config["childtable_count"],
                                   insert_rows=self.stability_config["insert_rows"], columns=col, tags=tag)

            database1 = jfile.setDatabases(dbinfo=db, super_tables=[stb])
            json_info = jfile.setJsoninfo(host=taosd_fqdn, databases=[database1])
            json_info.update({"test_log": "/root/testlog/"})
            json_data.append({})
            json_data[i] = json_info
            file_name.append("insert" + str(i) + ".json")
            jfile.genBenchmarkJson(
                self.run_log_dir, file_name[i], json_info)

        # put the file to target
        Insert_file.put_file(taosBenchmark_fqdn_list, json_data,file_name)
        # run taosBenchmark
        result_filename = Insert_file.threads_run_taosBenchmark(taosBenchmark_fqdn_list, json_data, file_name)

    def make_query(self):
        sql_list = [f'select * from {self.stability_config["query_dbname"]}.stb limit 10000',
                    f'select first(*) from {self.stability_config["query_dbname"]}.stb',
                    f'select last(*) from {self.stability_config["query_dbname"]}.stb',
                    ]
        for sql in sql_list:
            self.tdSql.execute(sql)

    def re_write_stability_yaml(self):
        self.env_dir = os.path.join(os.environ["TEST_ROOT"], "env")
        rand_fqdn = random.choice(self.taospy_settings["fqdn"])
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taospy":
                env_setting["spec"]["config"]["firstEP"] = rand_fqdn + ":6030"
        self.tmp_yaml = os.path.join(os.environ["TEST_ROOT"], "env/stability_tmp.yaml")
        dict2yaml(self.env_setting, self.env_dir, "stability_tmp.yaml")

    def time2s(self, runtime):
        if "d" in str(runtime).lower():
            d_num = re.findall("\d+\.?\d*", runtime.replace(" ", ""))[0]
            s_num = float(d_num) * 24 * 60 * 60
        elif "h" in str(runtime).lower():
            h_num = re.findall("\d+\.?\d*", runtime.replace(" ", ""))[0]
            s_num = float(h_num) * 60 * 60
        elif "m" in str(runtime).lower():
            m_num = re.findall("\d+\.?\d*", runtime.replace(" ", ""))[0]
            s_num = float(m_num) * 60
        elif "s" in str(runtime).lower():
            s_num = re.findall("\d+\.?\d*", runtime.replace(" ", ""))[0]
        else:
            s_num = 60
        return int(s_num)

    def del_and_add_dnode(self):
        self.tdSql.execute(f'drop dnode "{self.taosd_settings["fqdn"][-1]}:6030"')
        time.sleep(10)
        self.tdSql.execute(f'create dnode "{self.taosd_settings["fqdn"][-1]}:6030"')

    def stability_test(self):
        self.insert_data_with_taosBenchmark()
        scheduler = BackgroundScheduler()

        scheduler.add_job(self.make_query, 'interval', seconds=10)
        scheduler.add_job(self.run_taosc_insert_cases, 'interval', seconds=60)
        scheduler.add_job(self.run_restful_insert_cases, 'interval', seconds=60)
        scheduler.add_job(self.run_schemaless_insert_cases, 'interval', seconds=60)
        scheduler.add_job(self.run_query_cases, 'interval', seconds=60)
        scheduler.add_job(self.del_and_add_dnode, 'interval', seconds=60)
        scheduler.start()

        start_datetime = datetime.datetime.now()
        start_time = start_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')
        end_time = (start_datetime + datetime.timedelta(seconds=self.time2s(self.stability_config["run_time"]))).strftime('%Y-%m-%d %H:%M:%S.%f')
        while start_time < end_time:
            start_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

    def get_taosadapter_ip_port_dict(self):
        if "opentsdb_telnet" in self.taosadapter_settings["spec"]["adapter_config"]:
            dbs_list = self.taosadapter_settings["spec"]["adapter_config"]["opentsdb_telnet"]["dbs"]
            ports = self.taosadapter_settings["spec"]["adapter_config"]["opentsdb_telnet"]["ports"]
            self.taosadapter_ip_port_dict = dict(zip(dbs_list, ports))

    def run_taosc_insert_cases(self):
        self.re_write_stability_yaml()
        os.system(f' ~/.local/bin/taostest --use={self.tmp_yaml} --group-dir=taosc_insert --keep')

    def run_restful_insert_cases(self):
        self.re_write_stability_yaml()
        os.system(f' ~/.local/bin/taostest --use={self.tmp_yaml} --group-dir=restful_insert --keep')

    def run_schemaless_insert_cases(self):
        self.re_write_stability_yaml()
        os.system(f' ~/.local/bin/taostest --use={self.tmp_yaml} --group-dir=schemaless_insert --keep')

    def run_query_cases(self):
        pass

    def run_all_agent(self):
        if self.agent_settings is not None:
            self.get_taosadapter_ip_port_dict()
            for agent_type in ["collectd", "node_exporter", "statsd", "telegraf", "icinga2", "tcollector"]:
                for key, value in self.taosadapter_ip_port_dict.items():
                    if agent_type in str(key.lower()):
                        taosadapter_port = int(value)
                if agent_type in self.agent_settings["spec"]:
                    for agent_fqdn in self.agent_settings["fqdn"]:
                        for taosadapter_fqdn in self.agent_settings["taosadapter_fqdn"]:
                            self._remote.cmd(agent_fqdn, [f'docker ps -a | grep {taosadapter_fqdn}_{agent_type}_agent | awk \'{{print $1}}\' | xargs docker rm -f > /dev/null 2>&1'])
                            taosadapter_ip = socket.gethostbyname(taosadapter_fqdn)
                            if "interval" in self.agent_settings["spec"][agent_type]:
                                interval = self.agent_settings["spec"][agent_type]["interval"]
                                if agent_type == "telegraf":
                                    dbname = agent_type
                                    taosadapter_port = 6041
                                    self._remote.cmd(agent_fqdn, [f'cd /opt/agent_dockerfile/{agent_type}', f'./run_{agent_type}.sh {self.agent_settings["spec"][agent_type]["count"]} {taosadapter_fqdn}_{agent_type}_agent* {taosadapter_ip} {taosadapter_port} {interval}s {taosadapter_fqdn}_{dbname}'])
                                else:
                                    self._remote.cmd(agent_fqdn, [f'cd /opt/agent_dockerfile/{agent_type}', f'./run_{agent_type}.sh {self.agent_settings["spec"][agent_type]["count"]} {taosadapter_fqdn}_{agent_type}_agent* {taosadapter_ip} {taosadapter_port} {interval}'])
                            else:
                                if agent_type == "node_exporter":
                                    port_range = f'{self.agent_settings["spec"][agent_type]["port_range"][0]}:{self.agent_settings["spec"][agent_type]["port_range"][1]}'
                                    self._remote.cmd(agent_fqdn, [f'cd /opt/agent_dockerfile/{agent_type}', f'./run_{agent_type}.sh {port_range} {taosadapter_fqdn}_{agent_type}_agent*'])
                                else:
                                    if agent_type == "statsd":
                                        taosadapter_port = 6044
                                    self._remote.cmd(agent_fqdn, [f'cd /opt/agent_dockerfile/{agent_type}', f'./run_{agent_type}.sh {self.agent_settings["spec"][agent_type]["count"]} {taosadapter_fqdn}_{agent_type}_agent* {taosadapter_ip} {taosadapter_port}'])

    def run(self) -> bool:
        self.run_all_agent()
        # self.stability_test()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            stability <jayden>: [TD-12533] : stability test;
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.Stable

