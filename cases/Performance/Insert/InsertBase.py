# utf-8
from datetime import datetime
from typing import List
from taostest import TDCase
from taostest.performance.perfor_basic import InsertFile
from taostest.performance.result_reduction import Perf_Base_func



class InsertTest(TDCase):

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def run(self):

        taosBenchmark_iplist: List = self.get_fqdn("taosBenchmark")
        taosd_list = self.get_fqdn("taosd")
        json_data: List = []
        file_name = []


        jfile = InsertFile()
        Insert_file = Perf_Base_func(self.logger,self.run_log_dir)
        col = jfile.schemacfg(intcount=4, binarycount=(2, 16), doublecount=4, tscount=1)

        tag = jfile.schemacfg(intcount=1, binarycount=(1, 16))
        # set json_files for taosBenchmark
        for i in range(len(taosBenchmark_iplist)):
            if i == 0:
                db = jfile.setDBinfo(name="db1", drop="yes")
            else:
                db = jfile.setDBinfo(name="db1", drop="no")

            stb = jfile.setStbinfo(name="stb", childtable_prefix="stb_" + str(i), childtable_count=100,
                                   insert_rows=10000, columns=col, tags=tag)

            database1 = jfile.setDatabases(dbinfo=db, super_tables=[stb])
            json_info = jfile.setJsoninfo(host=taosd_list[0], databases=[database1])
            json_info.update({"test_log": "/root/testlog/"})
            json_data.append({})
            json_data[i] = json_info
            file_name.append("insert" + str(i) + ".json")
            jfile.genBenchmarkJson(
                self.run_log_dir, file_name[i], json_info)

        # put the file to target
        Insert_file.put_file(taosBenchmark_iplist, json_data,file_name)
        timestamp_start = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

        # run taosBenchmark
        result_filename = Insert_file.threads_run_taosBenchmark(taosBenchmark_iplist, json_data, file_name)

        timestamp_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

        # get insert result
        Insert_file.full_create_tb_result(result_filename)
        Insert_file.taosBenchmark_insert_summary_result(result_filename)
        Insert_file.taosBenchmark_id_insert_result(result_filename)


        # get node_info and process_info
        env_setting = self.get_component_by_name("prometheus")
        Insert_file.get_process_exporter_info(env_setting, 1,timestamp_start,timestamp_end)
        Insert_file.get_node_exporter_info(env_setting, 1,timestamp_start,timestamp_end)


