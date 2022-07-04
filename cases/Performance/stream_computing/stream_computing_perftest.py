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


class StreamComputingPerfTest(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.downsampling_function_list = ["min(c1)", "max(c2)", "sum(c3)", "first(c4)", "last(c5)", 
            "avg(c7)", "count(c8)", "spread(c1)", "stddev(c2)", "hyperloglog(c9)", "now"]
        self.output_select_str = ','.join(list(map(lambda x:f'`{x}`', self.downsampling_function_list)))
        self.source_select_str = ','.join(self.downsampling_function_list)
        self.stb_name = "stb"
        self.ctb_name = "ctb"
        self.tb_name = "tb"
        self.des_table_suffix = "_output"
        self.stream_suffix = "_stream"
        self.stb_stream_des_table = f'{self.stb_name}{self.des_table_suffix}'
        self.ctb_stream_des_table = f'{self.ctb_name}{self.des_table_suffix}'
        self.tb_stream_des_table = f'{self.tb_name}{self.des_table_suffix}'

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
        cfg = read_yaml(test_root + "/cases/Performance/stream_computing/insert.yaml")
        jfile = InsertFile()
        Insert_file = Perf_Base_func(self.logger, self.run_log_dir)
        for cases in cfg:
            i = 0
            for json_file in cfg[cases]:
                # if "stream_info" in cfg[cases][json_file]:
                #     trigger_mode=cfg[cases][json_file]["stream_info"]["trigger_mode"]
                #     interval=cfg[cases][json_file]["stream_info"]["interval"]
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
                                     cache=cfg[cases][json_file]["db_info"]["cache"],
                                     blocks=cfg[cases][json_file]["db_info"]["blocks"],
                                     precision=cfg[cases][json_file]["db_info"]["precision"],
                                     keep=cfg[cases][json_file]["db_info"]["keep"],
                                     comp=cfg[cases][json_file]["db_info"]["comp"],
                                     walLevel=cfg[cases][json_file]["db_info"]["walLevel"],
                                     fsync=cfg[cases][json_file]["db_info"]["fsync"],
                                     update=cfg[cases][json_file]["db_info"]["update"]
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
                                       line_protocol=cfg[cases][json_file]["stb_info"]["line_protocol"],
                                       batch_create_tbl_num=cfg[cases][json_file]["stb_info"]["batch_create_tbl_num"])
                stream = jfile.setStreaminfo(stream_name=cfg[cases][json_file]["stream_info"]["stream_name"],
                                            stream_stb=cfg[cases][json_file]["stream_info"]["stream_stb"],
                                            trigger_mode=cfg[cases][json_file]["stream_info"]["trigger_mode"],
                                            watermark=cfg[cases][json_file]["stream_info"]["watermark"],
                                            source_sql=cfg[cases][json_file]["stream_info"]["source_sql"],
                                            drop=cfg[cases][json_file]["stream_info"]["drop"])
                database1 = jfile.setDatabases(dbinfo=db, streams=[stream], super_tables=[stb])
                json_info = jfile.setJsoninfo(host=cfg[cases][json_file]["json_info"]["host"], databases=[database1],
                                              thread_count=cfg[cases][json_file]["json_info"]["thread_count"],
                                              result_file=cfg[cases][json_file]["json_info"]["result_file"],
                                              num_of_records_per_req=cfg[cases][json_file]["json_info"][
                                                  "num_of_records_per_req"])
                json_info.update({"test_log": "/root/testlog/"})
                json_data.append({})
                json_data[i] = json_info
                if "stream_info" in cfg[cases][json_file]:
                    file_name.append("stream_insert" + str(i) + ".json")
                else:
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
            # # run taosBenchmark
            if "stream_info" in cfg[cases][json_file]:
                pass
            # self.tdCom.drop_all_streams()
            # self.tdCom.drop_all_db()
            # self.tdCom.createDb(dbname=db['name'])
            # column_elm_list = [{"type": "int", "count": 4}, {"type": "double", "count": 4}, {"type": "varchar", "count": 2, "len": 16}, {"type": "timestamp", "count": 1}]
            # tag_elm_list = [{"type": "int", "count": 1}, {"type": "varchar", "count": 1, "len": 16}]
            # self.tdCom.create_stable(dbname=db['name'], column_elm_list=column_elm_list, tag_elm_list=tag_elm_list, default_column_index_start_num=0, default_tag_index_start_num=0)
            # self.tdCom.create_ctable(dbname=db['name'], tag_elm_list=tag_elm_list, default_ctbname_prefix="stb_00", default_ctbname_index_start_num=0, count=10)
            # self.stb_stream_des_table = f'{self.stb_name}{self.des_table_suffix}'
            # self.ctb_stream_des_table = f'{self.ctb_name}{self.des_table_suffix}'
            # self.tb_stream_des_table = f'{self.tb_name}{self.des_table_suffix}'
            # self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstartts AS start, {self.source_select_str}  from {self.stb_name} interval({interval})', trigger_mode="at_once")
            # self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstartts AS start, {self.stb_source_select_str}  from {self.ctb_name} interval({interval})', trigger_mode="at_once")
            # self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstartts AS start, {self.tb_source_select_str}  from {self.tb_name} interval({interval})', trigger_mode="at_once")
            taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
            result_filename = Insert_file.threads_run_taosBenchmark(taosBenchmark_iplist, json_data, file_name,taosBenchmark_env_setting)
            timestamp_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            # get insert result
            # Insert_file.full_create_tb_result(result_filename)
            Insert_file.taosBenchmark_insert_summary_result(result_filename, version="3.0")
            Insert_file.taosBenchmark_id_insert_result(result_filename)

            # get node_info and process_info
            env_setting = self.get_component_by_name("prometheus")
            Insert_file.get_process_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
            Insert_file.get_node_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
            f = open(result_file_name, 'a')
            f.write(f'--------{cases}---- \select max(cast(c10 as bigint))  from {db["name"]}.stb;\t--------\n')
            self.tdSql.query(f'select max(cast(c10 as bigint))  from {db["name"]}.stb;')
            f.write(str(self.tdSql.query_data[0][0]))
            f.write(f'\n\n')

            f.write(f'--------{cases}---- \select cast(last(`now`) as bigint) from {db["name"]}.{cases}{self.des_table_suffix}_streamtb;\t--------\n')
            self.tdSql.query(f'select cast(last(`now`) as bigint) from {db["name"]}.{cases}{self.des_table_suffix}_streamtb;')
            f.write(str(self.tdSql.query_data[0][0]))
            f.write(f'\n\n')
            f.close()
            
