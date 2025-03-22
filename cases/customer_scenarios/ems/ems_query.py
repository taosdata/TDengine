import json
import os
from taostest import TDCase, T
from taostest.util.common import TDCom
from taostest.util.rest import TDRest
from taostest.util.remote import Remote
from taostest.performance.perfor_basic import QueryFile
from taostest.performance.result_reduction import Perf_Base_func

class EMSQuery(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql, self.env_setting)
        self.tdRest = TDRest(env_setting=self.env_setting)
        self._remote: Remote = Remote(self.logger)
        self.query_file_name = "query.json"
        self.env_root = os.path.join(os.environ["TEST_ROOT"], "env")
        self.taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        self.case_config = json.load(open(os.path.join(self.env_root, "workflow_config.json")))
        self.taosBenchmark_config = dict()
        self.json_file_path = os.path.join(self.env_root, self.query_file_name)

        self.query_host = self.taosd_setting["fqdn"][0]
        self.taosBenchmark_config["host"] = self.query_host

        self.taosBenchmark_config["test_log"] = "/root/testlog/"

        self.tdCom.config_query_json(self._remote, self.json_file_path, self.taosBenchmark_config)

        self.tdCom.api_type = 'restful'
        self.taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        self.jfile = QueryFile()
        self.query_file = Perf_Base_func(self.logger, self.run_log_dir)
        self.log_path = f'{os.environ["TEST_ROOT"]}/run/workflow_logs/{self.case_config["test_start_time"]}'
        self.detail_log_path = f'{self.log_path}/details'
        self.summary_log_path = f'{self.log_path}/summary'
        self._remote.cmd("localhost", [f'mkdir -p {self.detail_log_path}', f'mkdir -p {self.summary_log_path}'])

    def cleanup(self) -> None:
        pass

    def run(self):
        json_info = json.load(open(self.json_file_path))
        self.jfile.genBenchmarkJson(self.run_log_dir, self.query_file_name, json_info)
        self.query_file.put_file(self.taosBenchmark_iplist, [json_info], [self.query_file_name])
        result_file_name = self.run_log_dir + '/perf_report.txt'
        f = open(result_file_name, 'a')
        f.write("********** query result ***********\n")
        f.close()
        # run taosBenchmark and get result file
        taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        result_filename = self.query_file.threads_run_taosBenchmark(self.taosBenchmark_iplist, [json_info], [self.query_file_name], taosBenchmark_env_setting)
        self._remote.cmd("localhost", f'cp {result_filename[0]} {self.detail_log_path}/query_result.txt')

        # get query result
        self.query_file.get_summary_query_result(result_filename)


    def desc(self) -> str:
        case_description = """
            ems-query
        """
        return case_description

    def author(self) -> str:
        return "Jayden Jia"

    def tags(self):
        return T.Query
