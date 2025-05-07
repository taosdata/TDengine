from datetime import datetime
import json
import os
from taostest import TDCase, T
from taostest.util.common import TDCom
from taostest.util.rest import TDRest
from taostest.util import file
class EMSCenter(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql, self.env_setting)
        self.tdRest = TDRest(env_setting=self.env_setting)
        # TODO: External input for edge side hostname, may have multiple
        self.env_root = os.path.join(os.environ["TEST_ROOT"], "env")
        self.taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        self.fqdn = self.taosd_setting["fqdn"][0]
        self.case_config = json.load(open(os.path.join(self.env_root, "workflow_config.json")))
        self.db_config = json.load(open(os.path.join(self.env_root, "db_config.json")))
        self.case_data_org = file.read_yaml(f'{os.environ["TEST_ROOT"]}/env/config.yaml')
        self.edge_hosts = self.case_config["edge_dnode_hosts"]
        self.tdCom.api_type = 'restful'
        self.target_dbname = "center_db"
        self.execute_time = int(self.case_config["exec_time"])
        self.edge_db = 'mqtt_datain'
        self.tdRest = TDRest(env_setting=self.env_setting)
        if self.case_config["enable_compression"].lower() == "true":
            self.compression_param = self.case_config["enable_compression"]
        else:
            self.compression_param = ""

    def cleanup(self) -> None:
        pass

    def run(self):
        taosd_url = f'http://{self.fqdn}:6041/rest/sql/information_schema'
        cluster_resp = self.tdRest.request(data="show cluster",method="POST",url=taosd_url)
        resp = cluster_resp.json()
        cluster_id = resp["data"][0][0]
        self.case_data_org["from"]["labels"][0] = f"cluster-id::{cluster_id}"
        headers = {"Content-Type": "application/json"}
        task_list = []
        # Create database mqtt_datain on the center side
        self.tdCom.createDb(self.target_dbname,self.db_config)
        # Create legacy datain task, each edge side's mqtt_datain database will have a legacy datain task
        for edge_host in self.edge_hosts:
                # "from": f"taos+ws://{edge_host}:6041/{self.edge_db}?mode=all&schema=always&schema-polling-interval=5s&compression={self.compression_param}",
            case_data = {
                "from": f"tmq+ws://{edge_host}:6041/{self.edge_db}?auto.offset.reset=earliest&client.id=test&experimental.snapshot.enable=true&compression={self.compression_param}",
                "to": f"taos+ws://{self.fqdn}:6041/{self.target_dbname}",
                "labels": self.case_data_org["from"]["labels"]
            }
            response = self.tdRest.request(data=case_data, method='POST', url=f'http://{self.fqdn}:6060/api/x/tasks',header=headers)
            task_info = response.json()
            task_list.append(task_info["id"])


    def desc(self) -> str:
        case_description = """
            This test case is for testing the center side of the EMS customer scenario. The execution logic is:
            1. Create a database on the center side taosd.
            2. Create legacy datain tasks; each edge side's mqtt_datain database will have a legacy datain task.
            3. Count the write rate of each legacy datain task and retrieve it through the metrics interface.
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