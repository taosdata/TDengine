from datetime import datetime
from taostest import TDCase
from taostest.performance.perfor_basic import QueryFile
from taostest.performance.result_reduction import Perf_Base_func

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
        taosd_iplist = self.get_fqdn("taosd")
        jfile = QueryFile()
        Query_file = Perf_Base_func(self.logger,self.run_log_dir)
        file_name = []
        json_data = []

        for i in range(len(taosBenchmark_iplist)):
            json_data.append({})
            sql_info = jfile.setSqlInfo(sql="select last(*) from stb")
            specify_query = jfile.setSpecifyQuery(concurrent=1, sqls=[sql_info])
            json_info = jfile.setJsoninfo(host=taosd_iplist[0], query_times=100, confirm_parameter_prompt="no",
                                          specified_table_query=specify_query, databases="db1")
            json_info.update({"query_mode": "taosc","test_log": "/root/testlog/"})
            json_data[i] = json_info

            file_name.append("query" + str(i) + ".json")
            jfile.genBenchmarkJson(self.run_log_dir, file_name[i], json_info)


        # put the file to target
        Query_file.put_file(taosBenchmark_iplist, json_data,file_name)

        # run taosBenchmark and get result file
        timestamp_start = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        result_filename = Query_file.threads_run_taosBenchmark(taosBenchmark_iplist, json_data,file_name)
        timestamp_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')


        # get query result
        Query_file.get_summary_query_result(result_filename)
        Query_file.get_taosBenchmark_query_process_info(result_filename)

        # get node_exporter and process_exporter info
        env_setting = self.get_component_by_name("prometheus")
        Query_file.get_process_exporter_info(env_setting, 1,timestamp_start,timestamp_end)
        Query_file.get_node_exporter_info(env_setting, 1,timestamp_start,timestamp_end)
