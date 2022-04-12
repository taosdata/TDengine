import os
from datetime import datetime
from taostest import TDCase
from taostest.performance.perfor_basic import QueryFile
from taostest.performance.result_reduction import Perf_Base_func
from taostest.util.file import read_yaml


class QueryTest(TDCase):
    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def run(self):
        # create json_file
        taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        jfile = QueryFile()
        Query_file = Perf_Base_func(self.logger, self.run_log_dir)
        file_name = []
        json_data = []

        test_root = os.environ['TEST_ROOT']
        cfg = read_yaml(test_root + "/cases/Performance/Query/query.yaml")
        for cases in cfg:
            i = 0
            for json_file in cfg[cases]:
                json_data.append({})
                sql_info = jfile.setSqlInfo(sql=cfg[cases][json_file]["sql"])
                specify_query = jfile.setSpecifyQuery(concurrent=cfg[cases][json_file]["specify_query"]["concurrent"],
                                                      sqls=[sql_info])
                json_info = jfile.setJsoninfo(host=cfg[cases][json_file]["json_info"]["host"],
                                              query_times=cfg[cases][json_file]["json_info"]["query_times"],
                                              confirm_parameter_prompt=cfg[cases][json_file]["json_info"][
                                                  "confirm_parameter_prompt"],
                                              specified_table_query=specify_query,
                                              databases=cfg[cases][json_file]["json_info"]["databases"],
                                              reset_query_cache=cfg[cases][json_file]["json_info"]["reset_query_cache"])
                json_info.update(
                    {"query_mode": cfg[cases][json_file]["json_info"]["query_mode"], "test_log": "/root/testlog/"})
                json_data[i] = json_info
                file_name.append("query" + str(i) + ".json")
                jfile.genBenchmarkJson(self.run_log_dir, file_name[i], json_info)
                i += 1

                # put the file to target
            Query_file.put_file(taosBenchmark_iplist, json_data, file_name)
            result_file_name = self.run_log_dir + '/perf_report.txt'
            f = open(result_file_name, 'a')
            f.write("********query\t" + str(cases) + "\t:query result***********\n")
            f.close()
            # run taosBenchmark and get result file
            timestamp_start = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            result_filename = Query_file.threads_run_taosBenchmark(taosBenchmark_iplist, json_data, file_name)
            timestamp_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

            # get query result
            Query_file.get_summary_query_result(result_filename)
            Query_file.get_taosBenchmark_query_process_info(result_filename)

            # get node_exporter and process_exporter info
            env_setting = self.get_component_by_name("prometheus")
            Query_file.get_process_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
            Query_file.get_node_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
