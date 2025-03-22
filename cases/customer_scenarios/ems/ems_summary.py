import json
import os
from taostest import TDCase, T
from taostest.util.remote import Remote
from taostest.util.common import TDCom
import re
import requests
from taostest.util.rest import TDRest

class EMSQuery(TDCase):
    def init(self):
        self._remote: Remote = Remote(self.logger)
        self.tdRest = TDRest(env_setting=self.env_setting)
        self.env_root = os.path.join(os.environ["TEST_ROOT"], "env")
        self.tdCom = TDCom(self.tdSql, self.env_setting)
        self.taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        self.fqdn = self.taosd_setting["fqdn"][0]
        self.case_config = json.load(open(os.path.join(self.env_root, "workflow_config.json")))
        self.log_path = f'{os.environ["TEST_ROOT"]}/run/workflow_logs/{self.case_config["test_start_time"]}'
        # self.log_path = f'{os.environ["TEST_ROOT"]}/run/workflow_logs/20250228_215853'
        self.detail_log_path = f'{self.log_path}/details'
        self.summary_log_path = f'{self.log_path}/summary'
        self._remote.cmd("localhost", [f'mkdir -p {self.detail_log_path}', f'mkdir -p {self.summary_log_path}'])
        self.dbname = "center_db"
        self.report_file = f'{self.log_path}/perf_report_{self.case_config["test_start_time"]}.txt'
        self.test_robot_url = (
    "https://open.feishu.cn/open-apis/bot/v2/hook/11e9e452-34a0-4c88-b014-10e21cb521dd"
)


    def get_query_detail_result(self):
        query_log = f'{self.log_path}/details/query_result.txt'
        with open(query_log, 'r') as file:
            log_content = file.read()
        query_pattern = r"complete query with (\d+) threads and (\d+) query delay avg:\s+([\d.]+)s min:\s+([\d.]+)s max:\s+([\d.]+)s p90:\s+([\d.]+)s p95:\s+([\d.]+)s p99:\s+([\d.]+)s SQL command: (.+);"
        total_pattern = r"Spend ([\d.]+) second completed total queries: (\d+), the QPS of all threads:\s+([\d.]+)"

        query_matches = re.findall(query_pattern, log_content)
        total_match = re.search(total_pattern, log_content)

        results = []
        for match in query_matches:
            threads, query_times, avg, min_, max_, p90, p95, p99, sql = match
            results.append({
                "sql": sql,
                "query_times": int(query_times),
                "concurrent": int(threads),
                "avg": float(avg),
                "min": float(min_),
                "max": float(max_),
                "p90": float(p90),
                "p95": float(p95),
                "p99": float(p99)
            })

        if total_match:
            spent, total_query, qps = total_match.groups()
            summary = {
                "QPS": float(qps),
                "spent": float(spent),
                "total_query": int(total_query)
            }
        else:
            summary = {
                "QPS": None,
                "spent": None,
                "total_query": None
            }

        final_result = {
            "summary": summary,
            "queries": results
        }
        return final_result

    def get_insert_result(self):
        insert_res_list = list()
        for filename in os.listdir(self.summary_log_path):
            if filename.endswith('.json'):
                file_path = os.path.join(self.summary_log_path, filename)
                with open(file_path, 'r') as json_file:
                    data = json.load(json_file)
                    insert_res_list.append(data)
        return insert_res_list

    def get_compression_ratio(self):
        self.tdRest.request(f'flush database {self.dbname};')
        self.tdRest.request(data=f"show table distributed center_db.site_topic6_u2_193;")
        self.tdRest.request(f'show {self.dbname}.disk_info;')

        query_res = self.tdRest.resp['data'][0][0]
        compression_ratio = query_res.split("=")[1].replace("[", "").replace("]", "") + "%"
        return compression_ratio

    def get_grafana_url(self):
        return self.case_config['grafana_url']

    def get_test_specs(self):
        return {
            "td_version": self.case_config["td_version"],
            "edge_dnode_count": int(self.case_config["edge_dnode_count"]),
            "center_dnode_count": int(self.case_config["center_dnode_count"]),
            "exec_time": f'{self.case_config["exec_time"]}s',
            "source_interval": f'{self.case_config["source_interval"]}ms',
            "enable_compression": self.case_config["enable_compression"],
            "test_start_time": self.case_config["test_start_time"],
        }

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
                        "title": "EMS-Test Report",
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
        insert_perf = self.get_insert_result()
        query_perf = self.get_query_detail_result()
        compression_ratio = self.get_compression_ratio()
        grafana_url = self.get_grafana_url()
        test_specs = self.get_test_specs()
        final_res_dict = {
            "Test Specs": test_specs,
            "Insert Performance": insert_perf,
            "Query Performance": query_perf,
            "Compression Ratio": compression_ratio,
            "Grafana URL": grafana_url
        }

        with open(self.report_file, 'w') as file:
            json.dump(final_res_dict, file, indent=4)
        self.send_msg(self.test_robot_url, self.get_msg(json.dumps(final_res_dict, indent=4)))
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
