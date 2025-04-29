import json
import os
from taostest import TDCase, T
from taostest.util.remote import Remote
from taostest.util.common import TDCom
import re
import requests
from taostest.util.rest import TDRest
from typing import Dict, List
import time
class EMSSummary(TDCase):
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
        self.timeout = 20  # Maximum wait time in seconds
        self.retention_timeout = 300
        self.query_interval = 3
        self.retry_times = 3
        self.edge_dbname = "mqtt_datain"
        self.center_dbname = "center_db"
        self.stable_data = {}
        self.report_file = f'{self.log_path}/perf_report_{self.case_config["test_start_time"]}.txt'
        self.edge_host_list = self.case_config["edge_dnode_hosts"]
        self.center_first_ep_host = self.case_config["center_dnode_hosts"][0]
        self.taosd_url = f'http://{self.center_first_ep_host}:6041/rest/sql'
        self.taosd_headers = {"Authorization": "Basic cm9vdDp0YW9zZGF0YQ=="}
        self.mqtt_received_bytes = 0
        self.test_robot_url = (
    "https://open.feishu.cn/open-apis/bot/v2/hook/11e9e452-34a0-4c88-b014-10e21cb521dd"
)

    def _execute_query(self, node: str, sql: str) -> Dict:
        url = f"http://{node}:6041/rest/sql"
        for _ in range(self.retry_times):
            try:
                resp = requests.post(url, data=sql, headers=self.taosd_headers, timeout=self.timeout)
                if resp.status_code == 200:
                    return resp.json()
            except Exception as e:
                print(f"Query failed on {node}: {str(e)}")
                time.sleep(self.query_interval)
        return {"code": -1, "desc": "Max retries exceeded"}

    def _get_stables(self, node: str, dbname: str) -> List[str]:
        sql = f"SHOW {dbname}.STABLES"
        result = self._execute_query(node, sql)
        if result.get("code") == 0:
            return [row[0] for row in result.get("data", [])]
        return []

    def _get_table_count(self, node: str, stable: str, dbname: str) -> int:
        sql = f"SELECT COUNT(*) FROM {dbname}.`{stable}`"
        result = self._execute_query(node, sql)
        if result.get("code") == 0 and result.get("data"):
            return int(result["data"][0][0])
        return 0

    def _get_compression_data(self) -> float:
        sql = f'select sum(data1+data2+data3) from information_schema.ins_disk_usage where db_name = "{self.center_dbname}";'
        result = self._execute_query(self.center_first_ep_host, sql)
        if result.get("code") == 0 and result.get("data"):
            return float(result["data"][0][0])
        return 0

    def collect_edge_data(self):
        total = 0
        for node in self.edge_host_list:
            stables = self._get_stables(node, self.edge_dbname)
            node_total = sum(self._get_table_count(node, stable, self.edge_dbname) for stable in stables)
            self.stable_data[node] = {"stables": stables, "count": node_total}
            total += node_total
        return total

    def validate_sync(self):
        edge_total = self.collect_edge_data()
        center_total = self._get_center_data()

        if edge_total == center_total:
            return [f"100%", center_total, edge_total]

        start_time = time.time()
        last_center_count = 0
        stable_counter = 0

        while time.time() - start_time < self.timeout:
            current_center = self._get_center_data()

            if current_center == last_center_count:
                stable_counter += 1
                if stable_counter >= 10:
                    break
            else:
                stable_counter = 0
                last_center_count = current_center

            completeness = current_center / edge_total if edge_total > 0 else 0
            print(f"Current sync progress: {completeness*100}%")

            time.sleep(self.query_interval)

        final_center = self._get_center_data()
        ratio = final_center / edge_total if edge_total > 0 else 0
        return [f"{round(ratio*100, 2)}%", final_center, edge_total]

    def _get_center_data(self) -> int:
        stables = self._get_stables(self.center_first_ep_host, self.center_dbname)
        return sum(self._get_table_count(self.center_first_ep_host, stable, self.center_dbname) for stable in stables)

    def get_query_detail_result(self):
        query_log = f'{self.log_path}/details/query_result.txt'
        with open(query_log, 'r') as file:
            log_content = file.read()
        query_pattern = r"complete query with (\d+) threads and (\d+)(?: sql \d+ spend [\d.]+s QPS: [\d.]+)? query delay avg:\s+([\d.]+)s min:\s+([\d.]+)s max:\s+([\d.]+)s p90:\s+([\d.]+)s p95:\s+([\d.]+)s p99:\s+([\d.]+)s SQL command: (.+?);"
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
                    if "mqtt_received_bytes" in data:
                        self.mqtt_received_bytes += data["mqtt_received_bytes"]
                        del data["mqtt_received_bytes"]
                    insert_res_list.append(data)
        return insert_res_list

    # def get_compression_ratio(self):
    #     self.tdRest.request(f'flush database {self.center_dbname};')
    #     # self.tdRest.request(data=f"show table distributed center_db.site_topic6_mqtt_u2_193;")
    #     self.tdRest.request(f'show {self.center_dbname}.disk_info;')

    #     query_res = self.tdRest.resp['data'][0][0]
    #     compression_ratio = query_res.split("=")[1].replace("[", "").replace("]", "") + "%"
    #     return compression_ratio

    def get_compression_ratio(self):

        # Flush the database first
        self.tdRest.request(f'flush database {self.center_dbname};')

        # Retry logic with self.timeout
        start_time = time.time()
        stable_threshold = 3  # Number of consecutive stable readings required
        stable_count = 0
        last_ratio = None
        compression_ratio = None

        while time.time() - start_time < self.timeout:
            # Query disk info
            self.tdRest.request(f'show {self.center_dbname}.disk_info;')

            # Check response structure and data
            if self.tdRest.resp.get('code') == 0 and self.tdRest.resp.get('data'):
                query_res = self.tdRest.resp['data'][0][0]
                if 'Compress_radio' in query_res or 'Compress_ratio' in query_res:
                    ratio_str = query_res.split("=")[1].replace("[", "").replace("]", "")

                    # Skip NULL values
                    if ratio_str == 'NULL':
                        time.sleep(0.5)
                        continue

                    # Check if ratio has changed
                    if ratio_str != last_ratio:
                        last_ratio = ratio_str
                        stable_count = 0
                    else:
                        stable_count += 1

                    # Return when ratio is stable
                    if stable_count >= stable_threshold:
                        return f"{ratio_str}%"

            # Wait for next check (keeping your exponential backoff logic)
            time.sleep(min(1, self.timeout - (time.time() - start_time)))

        # Return final result (last seen ratio or NULL)
        return f"{last_ratio}%" if last_ratio is not None else "NULL%"

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
        compression_ratio_disk_info = self.get_compression_ratio()
        data_retention_ratio, center_total_rows, edge_total_rows = self.validate_sync()
        data_retention_info = dict()
        data_retention_info["data_retention_ratio"] = data_retention_ratio
        data_retention_info["center_total_rows"] = center_total_rows
        data_retention_info["edge_total_rows"] = edge_total_rows
        compression_data_size = self._get_compression_data()
        compression_ratio = f'{round(self.mqtt_received_bytes/(compression_data_size*1024), 2)}:1' if compression_data_size != 0 else 'Null'
        grafana_url = self.get_grafana_url()
        test_specs = self.get_test_specs()
        final_res_dict = {
            "Test Specs": test_specs,
            "Insert Performance": insert_perf,
            "Query Performance": query_perf,
            "Compression Ratio": compression_ratio,
            "Data Retention Info": data_retention_info,
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
