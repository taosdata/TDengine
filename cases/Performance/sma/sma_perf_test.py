###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import os
from taostest.util.file import read_yaml
from taostest.util.common import TDCom
from datetime import datetime
from typing import List
from taostest import TDCase
from taostest.performance.perfor_basic import InsertFile
from taostest.performance.result_reduction import Perf_Base_func
import time
from taostest.util.remote import Remote


class StreamComputingPerfTest(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.stb_name = "stb"
        self.ctb_name = "ctb"
        self.tb_name = "tb"
        self.stream_timeout = 20
        self.restart_timeout = 10
        self._remote: Remote = Remote(self.logger)
        self.taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        self.fqdn = self.taosd_setting["fqdn"][0]
        self.firstEp = self.taosd_setting["spec"]["config"]["firstEP"]
        self.data_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"]
        self.log_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["logDir"]

        self.level0_interval = ""
        self.level1_interval = ""
        self.level2_interval = ""
        self.level0_keep = ""
        self.level1_keep = ""
        self.level2_keep = ""
        
    def get_batch_query_sql(self, ori_str, pos_str, str_add):
        str_list = ori_str.split(",")
        for i in str_list:
            if pos_str in i:
                insert_index = str_list.index(i)
        str_list.insert(insert_index, str_add)
        return ','.join(str_list)

    def clean_and_restart_taosd(self):
        killCmd = "systemctl stop taosd"
        startCmd = "systemctl start taosd"
        self._remote.cmd(self.fqdn, [killCmd])
        self._remote.cmd(self.fqdn, [f"rm -rf {self.data_dir} {self.log_dir}"])
        self._remote.cmd(self.fqdn, [startCmd])
        taosd_process_count = self._remote.cmd(self.fqdn, [f"ps -ef | grep taosd | grep -v grep | grep -v sudo | grep -v defunct | wc -l"])
        if int(taosd_process_count) > 0:
            ready_count = self._remote.cmd(self.fqdn, [f'taos -s "show dnodes" | grep {self.firstEp} | grep ready | wc -l'])
            ready_flag = 0
            while int(ready_count) != 1:
                taosd_process_count = self._remote.cmd(self.fqdn, [f"ps -ef | grep taosd | grep -v grep | grep -v sudo | grep -v defunct | wc -l"])
                if ready_flag < self.restart_timeout and int(taosd_process_count) > 0:
                    ready_flag += 0.5
                    time.sleep(0.5)
                    ready_count = self._remote.cmd(self.fqdn, [f'taos -s "show dnodes" | grep {self.firstEp} | grep ready | wc -l'])
                else:
                    return
                
    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def retention_parse(self, retentions):
        level_list = retentions.split(",")
        self.level0_interval = level_list[0].split(":")[0]
        self.level1_interval = level_list[1].split(":")[0]
        self.level2_interval = level_list[2].split(":")[0]
        self.level0_keep = level_list[0].split(":")[1]
        self.level1_keep = level_list[1].split(":")[1]
        self.level2_keep = level_list[2].split(":")[1]

    def query_last_res(self, stbname):
        self.tdSql.query(f'select count(*) from {stbname} where ts > now - {self.level0_keep}')
        level0_res = self.tdSql.query_data[0][0]
        self.tdSql.query(f'select count(*) from {stbname} where ts > now - {self.level1_keep}')
        level1_res = self.tdSql.query_data[0][0]
        self.tdSql.query(f'select count(*) from {stbname} where ts > now - {self.level2_keep}')
        level2_res = self.tdSql.query_data[0][0]
        return level0_res, level1_res, level2_res

    def manual_flush_db(self, dbname):
        self.tdSql.execute(f'flush database {dbname}')

    def get_vnode_disk(self):
        return self._remote.cmd(self.fqdn, [f'tree -h {self.data_dir}'])
        
    def run(self):
        taosBenchmark_iplist: List = self.get_fqdn("taosBenchmark")
        json_data: List = []
        file_name = []

        test_root = os.environ['TEST_ROOT']
        cfg = read_yaml(test_root + "/cases/Performance/sma/insert.yaml")
        
        jfile = InsertFile()
        Insert_file = Perf_Base_func(self.logger, self.run_log_dir)
        for cases in cfg:
            self.clean_and_restart_taosd()
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
                                     precision=cfg[cases][json_file]["db_info"]["precision"],
                                     duration=cfg[cases][json_file]["db_info"]["duration"],
                                     keep=cfg[cases][json_file]["db_info"]["keep"],
                                     retentions=cfg[cases][json_file]["db_info"]["retentions"],
                                     comp=cfg[cases][json_file]["db_info"]["comp"],
                                     vgroups=cfg[cases][json_file]["db_info"]["vgroups"]
                                     )
                stb = jfile.setStbinfo(name=cfg[cases][json_file]["stb_info"]["stb_name"],
                                       childtable_prefix=cfg[cases][json_file]["stb_info"]["childtable_prefix"] + str(
                                           i),
                                       childtable_count=cfg[cases][json_file]["stb_info"]["childtable_count"],
                                       insert_rows=cfg[cases][json_file]["stb_info"]["insert_rows"], columns=col,
                                       tags=tag,
                                       timestamp_step=cfg[cases][json_file]["stb_info"]["timestamp_step"],
                                       start_timestamp=cfg[cases][json_file]["stb_info"]["start_timestamp"],
                                       insert_mode=cfg[cases][json_file]["stb_info"]["insert_mode"],
                                       max_sql_len=cfg[cases][json_file]["stb_info"]["max_sql_len"],
                                       interlace_rows=cfg[cases][json_file]["stb_info"]["interlace_rows"],
                                       rollup=cfg[cases][json_file]["stb_info"]["rollup"],
                                       line_protocol=cfg[cases][json_file]["stb_info"]["line_protocol"],
                                       batch_create_tbl_num=cfg[cases][json_file]["stb_info"]["batch_create_tbl_num"])
                database = jfile.setDatabases(dbinfo=db, super_tables=[stb])
                json_info = jfile.setJsoninfo(host=cfg[cases][json_file]["json_info"]["host"], databases=[database],
                                            thread_count=cfg[cases][json_file]["json_info"]["thread_count"],
                                            create_table_thread_count=cfg[cases][json_file]["json_info"]["create_table_thread_count"],
                                            result_file=cfg[cases][json_file]["json_info"]["result_file"],
                                            num_of_records_per_req=cfg[cases][json_file]["json_info"][
                                                "num_of_records_per_req"])
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
                "-------- \t" + str(cases) + ": result--------\n")
            timestamp_start = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            # # run taosBenchmark
            taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
            result_filename = Insert_file.threads_run_taosBenchmark(taosBenchmark_iplist, json_data, file_name,taosBenchmark_env_setting)
            timestamp_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            # get insert result
            # Insert_file.full_create_tb_result(result_filename)
            Insert_file.taosBenchmark_insert_summary_result(result_filename, version="3.0")
            Insert_file.taosBenchmark_id_insert_result(result_filename)
            print("sleeping")
            f.write("--------    --------------- sleeping 300s ----------------------     --------\n")
            time.sleep(300)
            self.retention_parse(cfg[cases][json_file]["db_info"]["retentions"])
            # level0_res, level1_res, level2_res = self.query_last_res(f'{cfg[cases][json_file]["db_info"]["db_name"]}.{cfg[cases][json_file]["stb_info"]["stb_name"]}')
            # level0_res_expected = math.ceil(int(cfg[cases][json_file]["stb_info"]["insert_rows"])/(self.tdCom.trans_time_to_s(self.level0_interval)/(int(cfg[cases][json_file]["stb_info"]["timestamp_step"]) / 1000))) * int(cfg[cases][json_file]["stb_info"]["childtable_count"])
            # level1_res_expected = math.ceil(int(cfg[cases][json_file]["stb_info"]["insert_rows"])/(self.tdCom.trans_time_to_s(self.level1_interval)/(int(cfg[cases][json_file]["stb_info"]["timestamp_step"]) / 1000))) * int(cfg[cases][json_file]["stb_info"]["childtable_count"]) 
            # level2_res_expected = math.ceil(int(cfg[cases][json_file]["stb_info"]["insert_rows"])/(self.tdCom.trans_time_to_s(self.level2_interval)/(int(cfg[cases][json_file]["stb_info"]["timestamp_step"]) / 1000))) * int(cfg[cases][json_file]["stb_info"]["childtable_count"])
            # self.tdSql.checkEqual(level0_res, int(level0_res_expected))
            # self.tdSql.checkEqual(level1_res, int(level1_res_expected))
            # self.tdSql.checkEqual(level2_res, int(level2_res_expected))
            f.write("--------    --------------------------------------------     --------\n")
            for keep_value in [self.level0_keep, self.level1_keep, self.level2_keep]:
                f.write(self._remote.cmd(self.fqdn, [f'taos -s "select count(*) from {cfg[cases][json_file]["db_info"]["db_name"]}.{cfg[cases][json_file]["stb_info"]["stb_name"]} where ts > now - {keep_value}"']))
                f.write("\n")

            self.manual_flush_db(cfg[cases][json_file]["db_info"]["db_name"])
            f.write("--------    --------------------------------------------     --------\n")
            f.write(self.get_vnode_disk())
            f.close()


            # # get node_info and process_info
            env_setting = self.get_component_by_name("prometheus")
            Insert_file.get_process_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
            Insert_file.get_node_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
            print(result_file_name)
            
