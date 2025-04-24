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
import sys
from datetime import datetime
class Start(TDCase):
    def init(self):
        start_time = datetime.utcnow()
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.workflow_config = self.tdCom.load_workflow_json(self._remote, f'{os.environ["TEST_ROOT"]}/env/workflow_config.json')
        pass

    def start_mqtt_simulator(self):
        if "edge" in " ".join(sys.argv):
            mqtt_client_config = self.tdCom.get_components_setting(self.env_setting["settings"], "mqtt_client")
            edge_config = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
            edge_host = edge_config["fqdn"][0]
            mqtt_host = mqtt_client_config["fqdn"][0]
            # mqtt_pub_path = mqtt_client_config["spec"]["config"]
            mqtt_pub_path = mqtt_client_config["spec"]["config_file"]
            #mqtt_pub_interval= mqtt_client_config["spec"]["interval"]
            mqtt_pub_interval = self.workflow_config["source_interval"]
            exec_time = self.workflow_config["exec_time"]
            self._remote.cmd(mqtt_host,f"nohup mqtt_pub --schema {mqtt_pub_path} --host {edge_host} --interval {mqtt_pub_interval}ms --exec-duration ${exec_time}s > mqtt_pub.log 2>&1 &")

    def start_taosx_service(self,host):
        self._remote.cmd(host,"systemctl stop taos-explorer")
        self._remote.cmd(host,"systemctl start taos-explorer")
        self._remote.cmd(host,"systemctl stop taosx")
        self._remote.cmd(host,"systemctl start taosx")
    def run(self) -> bool:

        # start mqtt simulator
        self.start_mqtt_simulator()
        # start taosx service
        taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        taosx_host = taosd_setting["fqdn"][0]
        self.start_taosx_service(taosx_host)

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            just start env;
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T