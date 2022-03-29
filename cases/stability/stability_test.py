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
class TestStability(TDCase):
    def __init__(self):
        self._remote: Remote = None
        self._fqdn: str = None
        self.taosadapter_ip_port_dict = dict()
        self.env_setting = None
        self.agent_settings = None
        self.taosadapter_settings = None
        self.error_msg = None

    def init(self):
        self._remote: Remote = Remote(self.logger)
        self.tdCom = TDCom(self.tdSql)
        self.tdRest = TDRest()
        # for env_setting in self.env_setting["settings"]:
        #     if env_setting["name"].lower() == "agent":
        self.agent_settings = self.get_component_by_name("agent")[0]
            # elif env_setting["name"].lower() == "taosadapter":
        self.taosadapter_settings = self.get_component_by_name("taosadapter")[0]

    def stability_test(self):
        pass

    def get_taosadapter_ip_port_dict(self):
        if "opentsdb_telnet" in self.taosadapter_settings["spec"]["adapter_config"]:
            dbs_list = self.taosadapter_settings["spec"]["adapter_config"]["opentsdb_telnet"]["dbs"]
            ports = self.taosadapter_settings["spec"]["adapter_config"]["opentsdb_telnet"]["ports"]
            self.taosadapter_ip_port_dict = dict(zip(dbs_list, ports))

    def run_taosc_insert_cases(self):
        pass

    def run_restful_insert_cases(self):
        pass

    def run_schemaless_insert_cases(self):
        pass

    def run_query_cases(self):
        pass

    def run_all_agent(self):
        if self.agent_settings is not None:
            self.get_taosadapter_ip_port_dict()
            for agent_type in ["collectd", "node_exporter", "statsd", "telegraf", "icinga2", "tcollector"]:
                for key, value in self.taosadapter_ip_port_dict.items():
                    if agent_type in str(key.lower()):
                        dbname = key
                        taosadapter_port = int(value)
                if agent_type in self.agent_settings["spec"]:
                    for agent_fqdn in self.agent_settings["fqdn"]:
                        for taosadapter_fqdn in self.agent_settings["taosadapter_fqdn"]:
                            self._remote.cmd(agent_fqdn, [f'docker ps -a | grep {taosadapter_fqdn}_{agent_type}_agent | awk \'{{print $1}}\' | xargs docker rm -f > /dev/null 2>&1'])
                            taosadapter_ip = socket.gethostbyname(taosadapter_fqdn)
                            if "interval" in self.agent_settings["spec"][agent_type]:
                                interval = self.agent_settings["spec"][agent_type]["interval"]
                                if agent_type == "telegraf":
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
        self.stability_test()

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

