import json
import os
from taostest import TDCase, T
from taostest.util.common import TDCom
from taostest.util.rest import TDRest
from taostest.util import file
class EMSEdge(TDCase):
    def init(self):
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

    def set_mqtt_datain_payload(self,hostname='localhost',target_dbname='mqtt_datain'):
        task_list = []
        case_data_org = file.read_yaml(f'{os.environ["TEST_ROOT"]}/cases/customer_scenarios/ems/config.yaml')
        case_data_from = case_data_org["from"]
        mqtt_parser = file.read_yaml(f'{os.environ["TEST_ROOT"]}/cases/customer_scenarios/ems/parser.yaml')
        for topic_id,topic_name in case_data_from["topics"].items():
            task_data = {}
            mqtt_parser[topic_id]["parser"]["s_model"]["name"] = f'site_{topic_id}_{hostname.replace("-", "_")}'
            child_table_model = mqtt_parser[topic_id]["parser"]["model"]["name"]
            mqtt_parser[topic_id]["parser"]["model"]["using"] = f'site_{topic_id}_{hostname.replace("-", "_")}'
            mqtt_parser[topic_id]["parser"]["model"]["name"] = f"{child_table_model}_{hostname.replace('-', '_')}"
            cliend_id = self.tdCom.get_long_name(4,mode="numbers")
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
        cases_data = self.set_mqtt_datain_payload(hostname=self.host,target_dbname=self.target_dbname)
        # 在edge侧创建数据库 mqtt_datain
        self.tdCom.createDb(self.target_dbname,self.db_config)
        # 创建4个mqtt datain任务

        for case_data in cases_data:
            case_data["name"] = self.tdCom.get_long_name(4)
            task_url = f'http://{self.host}:6060/api/x/tasks'
            response = self.tdRest.request(data=case_data, method='POST', url=task_url,header=headers)
            task_info = response.json()
            task_list.append(task_info["id"])

        # time.sleep(self.execute_time)

        # for task_id in task_list:
        #     self.tdRest.request(data=None, method='POST', url=f'http://{self.host}:6060/api/x/tasks/{task_id}/stop',header=headers)

        # # TODO 等待任务结束，后面换成获取任务状态的方式
        # time.sleep(15)
        # # TODO 获取每个任务的metrics并保存下来
        # for task_id in task_list:
        #     response = self.tdRest.request(data=None, method='GET', url=f'http://{self.host}:6060/api/x/tasks/{task_id}/metrics',header=headers)
        #     print("response========",response.text)
        #     # metrics = response.text
        #     # print(metrics)

    def desc(self) -> str:
        case_description = """
            本用例用于 EMS 的客户场景测试, 用例执行逻辑：
            1. 每个edge侧taosd中创建数据库
            2. 每个edge侧创建4个mqtt datain任务, 每个edge侧的stable和table名需要保持唯一, 通过外部参数传入
            3. 统计每个mqtt datain任务的写入速率, 通过metrics接口获取

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


