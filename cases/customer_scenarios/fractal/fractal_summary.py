import json
import os
from taostest import TDCase, T
from taostest.util.remote import Remote
import re
import requests

class FractalQuery(TDCase):
    def init(self):
        self._remote: Remote = Remote(self.logger)
        self.env_root = os.path.join(os.environ["TEST_ROOT"], "env")
        self.case_config = json.load(open(os.path.join(self.env_root, "workflow_config.json")))
        self.log_path = f'{os.environ["TEST_ROOT"]}/run/workflow_logs/{self.case_config["test_start_time"]}'
        # self.log_path = f'{os.environ["TEST_ROOT"]}/run/workflow_logs/20250228_215853'
        self.detail_log_path = f'{self.log_path}/details'
        self.summary_log_path = f'{self.log_path}/summary'
        self._remote.cmd("localhost", [f'mkdir -p {self.detail_log_path}', f'mkdir -p {self.summary_log_path}'])
        self.report_file = f'{self.log_path}/perf_report_{self.case_config["test_start_time"]}.txt'
        self.test_robot_url = (
    "https://open.feishu.cn/open-apis/bot/v2/hook/11e9e452-34a0-4c88-b014-10e21cb521dd"
)

    def get_query_result(self):
        query_log = f'{self.log_path}/details/query_result.txt'
        with open(query_log, 'r') as file:
            log_content = file.read()

        qps_pattern = r"the QPS of all threads:\s*(\d+\.\d+)"
        qps_match = re.search(qps_pattern, log_content)
        qps = qps_match.group(1) if qps_match else "N/A"

        total_queries_pattern = r"Total specified queries:\s*(\d+)"
        total_queries_match = re.search(total_queries_pattern, log_content)
        total_queries = total_queries_match.group(1) if total_queries_match else "N/A"

        time_spend_pattern = r"Spend\s*(\d+\.\d+)\s*second"
        time_spend_match = re.search(time_spend_pattern, log_content)
        time_spend = time_spend_match.group(1) if time_spend_match else "N/A"
        query_res = {
    "QPS": qps,
    "Total Queries": total_queries,
    "Time Spend": time_spend
}
        with open(self.report_file, 'a') as output_file:
            output_file.write(f"Query Performance Summary:")
            output_file.write("\n")
            json.dump(query_res, output_file, indent=4)
            output_file.write("\n\n")


    def get_insert_result(self):
        with open(self.report_file, 'w') as output_file:
            output_file.write(f"Insert Performance Summary:\n")
            for filename in os.listdir(self.summary_log_path):
                if filename.endswith('.json'):
                    file_path = os.path.join(self.summary_log_path, filename)
                    print(file_path)
                    title = filename.replace('fractal-', '').replace('.json', '')
                    with open(file_path, 'r') as json_file:
                        data = json.load(json_file)
                        output_file.write(f"{title}:\n")
                        json.dump(data, output_file, indent=4)
                        output_file.write("\n\n")
                        output_file.flush()

    def get_grafana_url(self):
        grafana_url = self.case_config['grafana_url']
        with open(self.report_file, 'a') as output_file:
            output_file.write(f"Grafana URL: {grafana_url}")

    def send_msg(self, url:str, json:dict):
        headers = {
            'Content-Type': 'application/json'
        }

        req = requests.post(url=url, headers=headers, json=json)
        inf = req.json()
        if "StatusCode" in inf and inf["StatusCode"] == 0:
            pass
        else:
            print(inf)

    def get_msg(self, text):
        return {
            "msg_type": "post",
            "content": {
                "post": {
                    "zh_cn": {
                        "title": "Fractal-Test Report",
                        "content": [
                            [{
                                "tag": "text",
                                "text": text
                            }
                            ]]
                    }
                }
            }
        }

    def cleanup(self) -> None:
        pass

    def run(self):
        self.get_insert_result()
        self.get_query_result()
        self.get_grafana_url()
        with open(self.report_file, 'r') as file:
            log_content = file.read()
        self.send_msg(self.test_robot_url, self.get_msg(log_content))
        self._remote.cmd("localhost", f'cp {self.report_file} {self.env_root}')


    def desc(self) -> str:
        case_description = """
            summary report
        """
        return case_description

    def author(self) -> str:
        return "Jayden Jia"

    def tags(self):
        return T.Query
