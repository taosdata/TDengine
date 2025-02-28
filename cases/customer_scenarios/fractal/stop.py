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
import time
import sys
from datetime import datetime, timedelta

from taostest.util.rest import TDRest

class Start(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.workflow_config = self.tdCom.load_workflow_json(self._remote, f'{os.environ["TEST_ROOT"]}/env/workflow_config.json')
        self.taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        start_time = self.workflow_config["start_time"]
        self.tdRest = TDRest(env_setting=self.env_setting)
        print(self.workflow_config)
        end_time = start_time + timedelta(seconds=int(self.workflow_config["exec_time"]))
        url = (
            f"http://192.168.2.190:3000/d/dedq3n2zhlypsd/named-processes"
            f"?var-interval=10m&orgId=1&from={start_time}&to={end_time.isoformat(timespec='milliseconds')}Z"
            f"&timezone=browser&var-processes=$__all&refresh=5s"
        )
        print(url)
        self.host = self.taosd_setting["fqdn"][0]
        pass
    
    def stop_mqtt_simulator(self):
        if "edge" in " ".join(sys.argv):
            mqtt_client_config = self.tdCom.get_components_setting(self.env_setting["settings"], "mqtt_client")
            mqtt_host = mqtt_client_config["fqdn"][0]
            self._remote.cmd(mqtt_host,f"killall mqtt_pub")
        else:
            return
    def stop_mqtt_tasks_get_metrics(self,task_url=None,headers=None):
        # get task list
        response = self.tdRest.request(data=None, method='GET', url=task_url,header=headers)
        task_list = response.json()
        metrics_list = []
        for task_info in task_list:
            task_id = task_info["id"]
            # stop task
            self.tdRest.request(data=None, method='POST', url=f'http://{self.host}:6060/api/x/tasks/{task_id}/stop',header=headers)
            # get task metrics
            task_metrics = self.tdRest.request(data=None, method='GET', url=f'http://{self.host}:6060/api/x/tasks/{task_id}/metrics',header=headers)
            metrics_list.append(task_metrics)
            print(task_metrics)
        return metrics_list
    def run(self) -> bool:
        
        # stop mqtt simulator
        self.stop_mqtt_simulator()
        headers = {"Content-Type": "application/json"}
        task_url = f'http://{self.host}:6060/api/x/tasks'
        mqtt_task_result = self.stop_mqtt_tasks_get_metrics(task_url=task_url,headers=headers)
        # start taosx service
        # taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        # taosx_host = taosd_setting["fqdn"][0]

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