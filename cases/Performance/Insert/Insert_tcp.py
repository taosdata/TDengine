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

        # get taosBenchmark id and taosd id
        taosBenchmark_iplist: List = self.get_fqdn("taosBenchmark")
        taosd_list = self.get_fqdn("taosd")

        json_data: List = []
        file_name = []
        jfile = InsertFile()
        Insert_file = Perf_Base_func(self.logger, self.run_log_dir)

        # set column and tag info
        col = jfile.schemacfg(intcount=1)
        tag = jfile.schemacfg(intcount=1, binarycount=(1, 16))
        insert_mode = ["sml-rest"]
        insert_rows = [2000,20000]
        # set json_files for taosBenchmark
        for rows in insert_rows:
            for k in insert_mode:
                result_file_name = self.run_log_dir + '/perf_report.txt'
                f = open(result_file_name, 'a')
                f.write("-------- insert \t" + str(rows*10000) + "rows with\t" + k + "\tinsert mode--------\n")
                f.close()
                for i in range(len(taosBenchmark_iplist)):
                    if i == 0:
                        db = jfile.setDBinfo(name="db1", drop="yes")
                    else:
                        db = jfile.setDBinfo(name="db1", drop="no")
                    stb = jfile.setStbinfo(name="stb", childtable_prefix="stb_" + str(i), childtable_count=10000,
                                           insert_rows=rows, columns=col, tags=tag, batch_create_tbl_num=1, insert_mode=k,
                                           line_protocol="telnet",tcp_transfer="yes")
                    database1 = jfile.setDatabases(dbinfo=db, super_tables=[stb])
                    json_info = jfile.setJsoninfo(host=taosd_list[0], databases=[database1], thread_count=16,telnet_tcp_port=6046)
                    json_info.update({"test_log": "/root/testlog/"})
                    json_data.append({})
                    json_data[i] = json_info
                    file_name.append("insert" + str(i) + ".json")
                    jfile.genBenchmarkJson(self.run_log_dir, file_name[i], json_info)

                # put the file to target
                Insert_file.put_file(taosBenchmark_iplist, json_data, file_name)

                # run taosBenchmark
                timestamp_start = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                result_filename = Insert_file.threads_run_taosBenchmark(taosBenchmark_iplist, json_data, file_name)
                timestamp_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

                # get insert result
                Insert_file.full_create_tb_result(result_filename)
                Insert_file.taosBenchmark_insert_summary_result(result_filename)
                Insert_file.taosBenchmark_id_insert_result(result_filename)

                # get node_info and process_info
                env_setting = self.get_component_by_name("prometheus")
                Insert_file.get_process_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
                Insert_file.get_node_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
