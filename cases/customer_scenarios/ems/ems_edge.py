import json
import os
from taostest import TDCase, T
from taostest.util.common import TDCom
from taostest.util.rest import TDRest
from taostest.util import file
import sys
class EMSEdge(TDCase):
    def init(self):
        self.yml_name = sys.argv[1].split("=")[1]
        self.numbers_str = ''.join(filter(str.isdigit, self.yml_name))
        self.env_root = os.path.join(os.environ["TEST_ROOT"], "env")
        self.case_config = json.load(open(os.path.join(self.env_root, "workflow_config.json")))
        self.db_config = json.load(open(os.path.join(self.env_root, "db_config.json")))

        # self.execute_time = int(case_config["exec_time"])
        self.execute_time = int(self.case_config["exec_time"])
        self.tdCom = TDCom(self.tdSql, self.env_setting)
        self.tdRest = TDRest(env_setting=self.env_setting)
        self.taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        self.host = self.taosd_setting["fqdn"][0]
        self.tdCom.api_type = 'restful'
        self.target_dbname = "mqtt_datain"

    def cleanup(self) -> None:
        pass

    def set_mqtt_datain_payload(self, hostname='localhost', target_dbname='mqtt_datain'):
        task_list = []
        case_data_org = file.read_yaml(f'{os.environ["TEST_ROOT"]}/env/config.yaml')
        case_data_from = case_data_org["from"]
        taosd_url = f'http://{self.host}:6041/rest/sql/information_schema'
        cluster_resp = self.tdRest.request(data="show cluster",method="POST",url=taosd_url)
        resp = cluster_resp.json()
        cluster_id = resp["data"][0][0]
        case_data_from["labels"][0] = f"cluster-id::{cluster_id}"
        mqtt_parser = file.read_yaml(f'{os.environ["TEST_ROOT"]}/env/parser.yaml')
        for topic_id,topic_name in case_data_from["topics"].items():
            task_data = {}
            mqtt_parser[topic_id]["parser"]["s_model"]["name"] = f'site_{topic_id}_mqtt_{self.numbers_str}'
            child_table_model = mqtt_parser[topic_id]["parser"]["model"]["name"]
            mqtt_parser[topic_id]["parser"]["model"]["using"] = f'site_{topic_id}_mqtt_{self.numbers_str}'
            mqtt_parser[topic_id]["parser"]["model"]["name"] = f"{child_table_model}_{self.numbers_str}"
            cliend_id = self.tdCom.get_long_name(4, mode="numbers")
            task_data["from"] = f'''mqtt://{hostname}:1883?version=5.0&client_id={cliend_id}&char_encoding=UTF_8&keep_alive=60&clean_session=true&topics={case_data_from["topics"][topic_id]}::0&topic_pattern={case_data_from["topic_patterns"][topic_id]}'''
            # mqtt_payload
            task_data["parser"] = mqtt_parser[topic_id]
            task_data["to"] = f"taos+ws://{hostname}:6041/{target_dbname}"
            task_data["labels"] = case_data_from["labels"]
            task_list.append(task_data)
        return task_list
    def run(self):
        headers = {"Content-Type": "application/json"}
        task_list = []
        cases_data = self.set_mqtt_datain_payload(hostname=self.host, target_dbname=self.target_dbname)
        # Create database mqtt_datain on the edge side
        self.tdCom.createDb(self.target_dbname, self.db_config)
        # Create 4 mqtt datain tasks

        for case_data in cases_data:
            case_data["name"] = self.tdCom.get_long_name(4)
            task_url = f'http://{self.host}:6060/api/x/tasks'
            response = self.tdRest.request(data=case_data, method='POST', url=task_url,header=headers)
            task_info = response.json()
            task_list.append(task_info["id"])

        # time.sleep(self.execute_time)

        # for task_id in task_list:
        #     self.tdRest.request(data=None, method='POST', url=f'http://{self.host}:6060/api/x/tasks/{task_id}/stop',header=headers)
        # # TODO: Wait for tasks to finish, later change to a method of getting task status

        # time.sleep(15)
        # # TODO: Get metrics for each task and save them

        # for task_id in task_list:
        #     response = self.tdRest.request(data=None, method='GET', url=f'http://{self.host}:6060/api/x/tasks/{task_id}/metrics',header=headers)
        #     print("response========",response.text)
        #     # metrics = response.text
        #     # print(metrics)

    def desc(self) -> str:
        case_description = """
            This test case is for testing the EMS customer scenario. The execution logic is:
            1. Create a database in each edge side taosd.
            2. Create 4 mqtt datain tasks on each edge side; each edge side's stable and table names need to be unique, passed through external parameters.
            3. Count the write rate of each mqtt datain task and retrieve it through the metrics interface.
        """
        return case_description

    def author(self) -> str:
        return "Chenyang Jia, Jayden Jia"

    def tags(self):
        return T.Query, T.Write.Table.Create, "private-tag1", "private-tag2"

    def get_report(self, start_time, stop_time) -> str:
        return """
        | CPU | Disk | Memory | Thread|
        | ----| ----  |------| -----|
        | 1   |     2 |   3  |   4  |
        """