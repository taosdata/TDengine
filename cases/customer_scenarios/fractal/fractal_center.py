import json
import os
import time
from taostest import TDCase, T
from taostest.util.common import TDCom
from taostest.util.rest import TDRest
from taostest.util import file
class FractalCenter(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql, self.env_setting)
        self.tdRest = TDRest(env_setting=self.env_setting)
        # TODO 外部传入edge侧的hostname，可能有多个
        self.env_root = os.path.join(os.environ["TEST_ROOT"], "env")
        self.taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        self.fqdn = self.taosd_setting["fqdn"][0]
        self.case_config = json.load(open(os.path.join(self.env_root, "workflow_config.json")))
        self.db_config = self.case_config["db_config"]
        self.case_data_org = file.read_yaml(f'{os.environ["TEST_ROOT"]}/cases/customer_scenarios/fractal/config.yaml')
        self.edge_hosts = self.case_config["edge_dnode_hosts"]
        self.tdCom.api_type = 'restful'
        self.target_dbname = "center_db"
        self.execute_time = self.case_config["exec_time"]
        self.edge_db = 'mqtt_datain'
        self.tdRest = TDRest(env_setting=self.env_setting)
        if self.case_config["enable_compression"].lower() == "true":
            self.compression_param = self.case_config["enable_compression"]
        else:
            self.compression_param = ""
    def cleanup(self) -> None:
        pass

    def run(self):
        headers = {"Content-Type": "application/json"}
        task_list = []
        # 在center侧创建数据库 mqtt_datain
        self.tdCom.createDb(self.target_dbname,self.db_config)
        # 创建legacy datain任务,每个edge侧的mqtt_datain数据库都会有一个legacy datain任务
        for edge_host in self.edge_hosts:
            case_data = {
                "from": f"taos+ws://{edge_host}:6041/{self.edge_db}",
                "to": f"taos+ws://{self.fqdn}:6041/{self.target_dbname}?{self.compression_param}",
                "labels": self.case_data_org["from"]["labels"]
            }
            response = self.tdRest.request(data=case_data, method='POST', url=f'http://{self.fqdn}:6060/api/x/tasks',header=headers)
            print("response=====",response)
            task_info = response.json()
            task_list.append(task_info["id"])
        time.sleep(self.execute_time)
        for task_id in task_list:
           self.tdRest.request(data=None, method='POST', url=f'http://{self.fqdn}:6060/api/x/tasks/{task_id}/stop',header=headers)
        # TODO 获取每个任务的metrics并保存下来，生成报告
        for task_id in task_list:
            response = self.tdRest.request(data=None, method='GET', url=f'http://{self.fqdn}:6060/api/x/tasks/{task_id}/metrics',header=headers)
            metrics = response.json()
            # TODO 获取metrics并保存
            print(metrics)
        
        # kill taosBenchmark

    def desc(self) -> str:
        case_description = """
            本用例用于fractal的客户场景测试center侧的测试执行，用例执行逻辑：
            1. center侧taosd中创建数据库
            2. 创建legacy datain任务，每个edge侧的mqtt_datain数据库都会有一个legacy datain任务
            3. 统计每个legacy datain任务的写入速率，通过metrics接口获取

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