# utf-8
import os
from taostest.util.file import read_yaml
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

        test_root = os.environ['TEST_ROOT']
        cfg = read_yaml(test_root + "./cases/Performance/Insert/insert.yaml")

        jfile = InsertFile()
        Insert_file = Perf_Base_func(self.logger, self.run_log_dir)
        col = jfile.schemacfg(intcount=cfg["stb_info"]["col_int_count"],
                              binarycount=(cfg["stb_info"]["col_binary_count"], cfg["stb_info"]["col_binary_length"]),
                              doublecount=cfg["stb_info"]["col_double_count"])
        tag = jfile.schemacfg(intcount=cfg["stb_info"]["tag_float_count"],
                              binarycount=(cfg["stb_info"]["tag_binary_count"], cfg["stb_info"]["tag_binary_length"]))
        # set json_files for taosBenchmark
        for i in range(len(taosBenchmark_iplist)):
            result_file_name = self.run_log_dir + '/perf_report.txt'
            f = open(result_file_name, 'a')
            f.write(
                "-------- insert\t" + str(
                    cfg["stb_info"]["childtable_count"] * cfg["stb_info"]["insert_rows"]) + " rows with\t" +
                cfg["stb_info"]["insert_mode"] + "\tinsert mode--------\n")
            f.close()
            db = jfile.setDBinfo(name=cfg["db_info"]["db_name"], drop=cfg["db_info"]["drop"])
            stb = jfile.setStbinfo(name=cfg["stb_info"]["stb_name"],
                                   childtable_prefix=cfg["stb_info"]["childtable_prefix"] + str(i),
                                   childtable_count=cfg["stb_info"]["childtable_count"],
                                   insert_rows=cfg["stb_info"]["insert_rows"], columns=col, tags=tag,
                                   timestamp_step=cfg["stb_info"]["timestamp_step"],
                                   start_timestamp=cfg["stb_info"]["start_timestamp"],
                                   insert_mode=cfg["stb_info"]["insert_mode"],
                                   line_protocol=cfg["stb_info"]["line_protocol"],
                                   tcp_transfer=cfg["stb_info"]["tcp_transfer"])

            database1 = jfile.setDatabases(dbinfo=db, super_tables=[stb])
            json_info = jfile.setJsoninfo(host=taosd_list[0], databases=[database1],
                                          thread_count=cfg["json_info"]["thread_count"])
            json_info.update({"test_log": "/root/testlog/"})
            json_data.append({})
            json_data[i] = json_info
            file_name.append("insert" + str(i) + ".json")
            jfile.genBenchmarkJson(
                self.run_log_dir, file_name[i], json_info)

        # put the file to target
        Insert_file.put_file(taosBenchmark_iplist, json_data, file_name)
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
        Insert_file.get_process_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
        Insert_file.get_node_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
