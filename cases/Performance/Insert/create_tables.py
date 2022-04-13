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
        json_data: List = []
        file_name = []

        test_root = os.environ['TEST_ROOT']
        cfg = read_yaml(test_root + "./cases/Performance/Insert/createtables.yaml")

        jfile = InsertFile()
        Insert_file = Perf_Base_func(self.logger, self.run_log_dir)
        for cases in cfg:
            i = 0
            for json_file in cfg[cases]:
                col = jfile.schemacfg(intcount=cfg[cases][json_file]["stb_info"]["col_int_count"],
                                      binarycount=(cfg[cases][json_file]["stb_info"]["col_binary_count"],
                                                   cfg[cases][json_file]["stb_info"]["col_binary_length"]),
                                      doublecount=cfg[cases][json_file]["stb_info"]["col_double_count"],
                                      floatcount=cfg[cases][json_file]["stb_info"]["col_float_count"],
                                      bcount=cfg[cases][json_file]["stb_info"]["col_bigint_count"],
                                      tcount=cfg[cases][json_file]["stb_info"]["col_tinyint_count"],
                                      scount=cfg[cases][json_file]["stb_info"]["col_smallint_count"],
                                      ncharcount=(cfg[cases][json_file]["stb_info"]["col_nchar_count"],
                                                  cfg[cases][json_file]["stb_info"]["col_nchar_length"]),
                                      tscount=cfg[cases][json_file]["stb_info"]["col_timestamp_count"])
                tag = jfile.schemacfg(intcount=cfg[cases][json_file]["stb_info"]["tag_int_count"],
                                      binarycount=(cfg[cases][json_file]["stb_info"]["tag_binary_count"],
                                                   cfg[cases][json_file]["stb_info"]["tag_binary_length"]),
                                      doublecount=cfg[cases][json_file]["stb_info"]["tag_double_count"],
                                      floatcount=cfg[cases][json_file]["stb_info"]["tag_float_count"],
                                      bcount=cfg[cases][json_file]["stb_info"]["tag_bigint_count"],
                                      tcount=cfg[cases][json_file]["stb_info"]["tag_tinyint_count"],
                                      scount=cfg[cases][json_file]["stb_info"]["tag_smallint_count"],
                                      ncharcount=(cfg[cases][json_file]["stb_info"]["tag_nchar_count"],
                                                  cfg[cases][json_file]["stb_info"]["tag_nchar_length"]),
                                      tscount=cfg[cases][json_file]["stb_info"]["tag_timestamp_count"])
                # set json_files for taosBenchmark

                db = jfile.setDBinfo(name=cfg[cases][json_file]["db_info"]["db_name"],
                                     drop=cfg[cases][json_file]["db_info"]["drop"],
                                     replica=cfg[cases][json_file]["db_info"]["replica"],
                                     days=cfg[cases][json_file]["db_info"]["days"],
                                     cache=cfg[cases][json_file]["db_info"]["cache"],
                                     blocks=cfg[cases][json_file]["db_info"]["blocks"],
                                     precision=cfg[cases][json_file]["db_info"]["precision"],
                                     keep=cfg[cases][json_file]["db_info"]["keep"],
                                     comp=cfg[cases][json_file]["db_info"]["comp"],
                                     walLevel=cfg[cases][json_file]["db_info"]["walLevel"],
                                     fsync=cfg[cases][json_file]["db_info"]["fsync"],
                                     update=cfg[cases][json_file]["db_info"]["update"])
                stb = jfile.setStbinfo(name=cfg[cases][json_file]["stb_info"]["stb_name"],
                                       childtable_prefix=cfg[cases][json_file]["stb_info"]["childtable_prefix"] + str(
                                           i),
                                       childtable_count=cfg[cases][json_file]["stb_info"]["childtable_count"],
                                       insert_rows=0, columns=col,
                                       tags=tag,
                                       timestamp_step=cfg[cases][json_file]["stb_info"]["timestamp_step"],
                                       start_timestamp=cfg[cases][json_file]["stb_info"]["start_timestamp"],
                                       insert_mode=cfg[cases][json_file]["stb_info"]["insert_mode"],
                                       line_protocol=cfg[cases][json_file]["stb_info"]["line_protocol"],
                                       tcp_transfer=cfg[cases][json_file]["stb_info"]["tcp_transfer"])

                database1 = jfile.setDatabases(dbinfo=db, super_tables=[stb])
                json_info = jfile.setJsoninfo(host=cfg[cases][json_file]["json_info"]["host"], databases=[database1],
                                              thread_count=cfg[cases][json_file]["json_info"]["thread_count"])
                json_info.update({"test_log": "/root/testlog/"})
                json_data.append({})
                json_data[i] = json_info
                file_name.append("insert" + str(i) + ".json")
                jfile.genBenchmarkJson(self.run_log_dir, file_name[i], json_info)
                i += 1
            # put the file to target
            Insert_file.put_file(taosBenchmark_iplist, json_data, file_name)
            result_file_name = self.run_log_dir + '/perf_report.txt'
            f = open(result_file_name, 'a')
            f.write(
                "-------- \tinsert\t" + str(cases) + ":\tinsert result--------\n")
            f.close()
            timestamp_start = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            # run taosBenchmark
            result_filename = Insert_file.threads_run_taosBenchmark(taosBenchmark_iplist, json_data, file_name)
            timestamp_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

            # get insert result
            Insert_file.full_create_tb_result(result_filename)
            # Insert_file.taosBenchmark_insert_summary_result(result_filename)
            # Insert_file.taosBenchmark_id_insert_result(result_filename)

            # get node_info and process_info
            env_setting = self.get_component_by_name("prometheus")
            Insert_file.get_process_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
            Insert_file.get_node_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
