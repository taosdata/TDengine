import time
from taostest import TDCase, T
from taostest.util.common import TDCom
from taostest.util.rest import TDRest
from taostest.util import file
class FractakEdge(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.api_type = 'restful'
        self.hostname = self.get_hostname()
        self.target_dbname = "mqtt_datain"
        self.execute_time = 120
        pass

    def cleanup(self) -> None:
        pass

    def set_mqtt_datain_payload(self):
        case_data = []
        case_data_org = file.read_yaml("config.yaml")
        for mqtt_topic in case_data_org["topics"]:
            task_data = {}
            mqtt_payload = file.read_yaml("parser.yaml")
            task_data["from"] = f"mqtt://{self.hostname}:1883? \
                                client_id=taosxmqtt_client_1362& \
                                keep_alive=60& \
                                clean_session=true& \
                                topic={mqtt_topic}"
            task_data["parser"] = mqtt_payload["parser"]
            task_data["to"] = f"taos+ws://{self.hostname}:6041/{self.target_dbname}" 
            case_data.append(task_data)
            
        return case_data
    def run(self):
        headers = {"Content-Type": "application/json"}
        task_list = []
        cases_data = self.set_mqtt_datain_payload()
        # 在edge侧创建数据库 mqtt_datain
        self.tdCom.createDb(self.target_dbname)
        # 创建4个mqtt datain任务
        for case_data in cases_data:
            response = TDRest.request(data=case_data, method='POST', url=f'http://{self.hostname}:6060/api/x/tasks',header=headers)
            task_info = response.json()
            task_list.append(task_info["id"])
        time.sleep(self.execute_time)
        for task_id in task_list:
           TDRest.request(data=None, method='POST', url=f'http://{self.hostname}:6060/api/x/tasks/{task_id}/stop',header=headers)
        
        # TODO 获取每个任务的metrics并保存下来
        for task_id in task_list:
            response = TDRest.request(data=None, method='GET', url=f'http://{self.hostname}:6060/api/x/tasks/{task_id}/metrics',header=headers)
            metrics = response.json()
            
            print(metrics)
        
        
        pass

    def desc(self) -> str:
        case_description = """
            本用例用于fractal的客户场景测试，用例执行逻辑：
            1. 每个edge侧taosd中创建数据库
            2. 每个edge侧创建4个mqtt datain任务，每个edge侧的stable和table名需要保持唯一，通过外部参数传入
            3. 统计每个mqtt datain任务的写入速率，通过metrics接口获取
            
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


