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
        # ! "hyperloglog(c9)" slow
        # self.downsampling_function_list = ["min(c1)", "max(c2)", "sum(c3)", "first(c4)", "last(c5)", 
        #     "avg(c7)", "count(c8)", "spread(c1)", "stddev(c2)", "hyperloglog(c9)", "now"]
        self.downsampling_function_list = ["min(c1)", "max(c2)", "sum(c3)", "avg(c7)", "now"]
        self.output_select_str = ','.join(list(map(lambda x:f'`{x}`', self.downsampling_function_list)))
        self.source_select_str = ','.join(self.downsampling_function_list)
        self.stb_name = "stb"
        self.ctb_name = "ctb"
        self.tb_name = "tb"
        self.des_table_suffix = "_output"
        self.stream_suffix = "_stream"
        self.non_prikey_ts_col_name = ""
        self.stb_stream_des_table = f'{self.stb_name}{self.des_table_suffix}'
        self.ctb_stream_des_table = f'{self.ctb_name}{self.des_table_suffix}'
        self.tb_stream_des_table = f'{self.tb_name}{self.des_table_suffix}'
        self.stream_timeout = 20
        self.restart_timeout = 10
        self._remote: Remote = Remote(self.logger)
        self.taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        self.fqdn = self.taosd_setting["fqdn"][0]
        self.firstEp = self.taosd_setting["spec"]["config"]["firstEP"]
        print(self.taosd_setting)
        self.data_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"]
        self.log_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["logDir"]
        
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

    def run(self):
        taosBenchmark_iplist: List = self.get_fqdn("taosBenchmark")
        json_data: List = []
        file_name = []

        test_root = os.environ['TEST_ROOT']
        cfg = read_yaml(test_root + "/cases/Performance/stream_computing/insert.yaml")
        
        jfile = InsertFile()
        Insert_file = Perf_Base_func(self.logger, self.run_log_dir)
        for cases in cfg:
            self.clean_and_restart_taosd()
            i = 0
            for json_file in cfg[cases]:
                if "stream_info" in cfg[cases][json_file]:
                    trigger_mode=cfg[cases][json_file]["stream_info"]["trigger_mode"]
                    stream_name = cfg[cases][json_file]["stream_info"]["stream_name"]
                    target_tb = cfg[cases][json_file]["stream_info"]["stream_stb"]
                    query_sql = cfg[cases][json_file]["stream_info"]["source_sql"]
                    target_db = target_tb.split(".")[0]
                    # self.tdSql.execute(f'create database if not exists {target_db} minrows 1 vgroups 1')
                    self.tdSql.execute(f'create database if not exists {target_db} minrows 1 vgroups {cfg[cases][json_file]["db_info"]["vgroups"]}')
                    # self.tdSql.execute(f'create database if not exists perf_db2 minrows 1 vgroups 1')
                    # interval=cfg[cases][json_file]["stream_info"]["interval"]

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
                                     update=cfg[cases][json_file]["db_info"]["update"],
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
                                       interlace_rows=cfg[cases][json_file]["stb_info"]["interlace_rows"],
                                       line_protocol=cfg[cases][json_file]["stb_info"]["line_protocol"],
                                       batch_create_tbl_num=cfg[cases][json_file]["stb_info"]["batch_create_tbl_num"])
                if "stream_info" in cfg[cases][json_file]:
                    if "watermark" in cfg[cases][json_file]["stream_info"]:
                        watermark = cfg[cases][json_file]["stream_info"]["watermark"]
                    else:
                        watermark = None
                    stream = jfile.setStreaminfo(stream_name=cfg[cases][json_file]["stream_info"]["stream_name"],
                                                stream_stb=cfg[cases][json_file]["stream_info"]["stream_stb"],
                                                trigger_mode=cfg[cases][json_file]["stream_info"]["trigger_mode"],
                                                watermark=watermark,
                                                source_sql=cfg[cases][json_file]["stream_info"]["source_sql"],
                                                drop=cfg[cases][json_file]["stream_info"]["drop"])
                    database1 = jfile.setDatabases(dbinfo=db, streams=[stream], super_tables=[stb])
                else:
                    database1 = jfile.setDatabases(dbinfo=db, streams=None, super_tables=[stb])
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
            print(taosBenchmark_iplist)
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
            self.tdCom.createDb(dbname=db['name'], minrows=1, vgroups=cfg[cases][json_file]["db_info"]["vgroups"])
            column_elm_list = [{"type": "int", "count": 2}, {"type": "double", "count": 2}, {"type": "timestamp", "count": 1}]
            tag_elm_list = [{"type": "int", "count": 1}, {"type": "varchar", "count": 1, "len": 16}]
            self.tdCom.create_stable(dbname=db['name'], column_elm_list=column_elm_list, tag_elm_list=tag_elm_list, default_column_index_start_num=0, default_tag_index_start_num=0)
            if "stream_info" in cfg[cases][json_file]:
                self.tdCom.create_stream(stream_name=stream_name, des_table=target_tb, source_sql=query_sql, trigger_mode=trigger_mode, watermark=watermark)
            # self.tdCom.create_ctable(dbname=db['name'], tag_elm_list=tag_elm_list, default_ctbname_prefix="stb_00", default_ctbname_index_start_num=0, count=10)
            # self.stb_stream_des_table = f'{self.stb_name}{self.des_table_suffix}'
            # self.ctb_stream_des_table = f'{self.ctb_name}{self.des_table_suffix}'
            # self.tb_stream_des_table = f'{self.tb_name}{self.des_table_suffix}'
            # self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstartts AS start, {self.source_select_str}  from {self.stb_name} interval({interval})', trigger_mode="at_once")
            # self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstartts AS start, {self.stb_source_select_str}  from {self.ctb_name} interval({interval})', trigger_mode="at_once")
            # self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstartts AS start, {self.tb_source_select_str}  from {self.tb_name} interval({interval})', trigger_mode="at_once")
            taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
            result_filename = Insert_file.threads_run_taosBenchmark(taosBenchmark_iplist, json_data, file_name,taosBenchmark_env_setting)
            self.tdSql.query(f'describe {cfg[cases][json_file]["db_info"]["db_name"]}.{cfg[cases][json_file]["stb_info"]["stb_name"]}')
            for query_data in self.tdSql.query_data:
                if query_data[1] == "TIMESTAMP" and query_data[0] != "ts":
                    non_prikey_ts_col_name = query_data[0]
            
            timestamp_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            # get insert result
            # Insert_file.full_create_tb_result(result_filename)
            Insert_file.taosBenchmark_insert_summary_result(result_filename, version="3.0")
            Insert_file.taosBenchmark_id_insert_result(result_filename)

            # get node_info and process_info
            env_setting = self.get_component_by_name("prometheus")
            Insert_file.get_process_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
            Insert_file.get_node_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
            self.tdSql.execute(f'use {db["name"]}')
            if "stream_info" in cfg[cases][json_file]:
                query_sql = f'select max(cast(`now` as bigint)) from {cfg[cases][json_file]["stream_info"]["stream_stb"]};'
                f = open(result_file_name, 'a')
                source_stb_query_sql = f'select max(cast({non_prikey_ts_col_name} as bigint))  from {db["name"]}.stb;'
                batch_query_sql = self.get_batch_query_sql(cfg[cases][json_file]["stream_info"]["source_sql"], "now", f" max(cast({non_prikey_ts_col_name} as bigint)) a")
                if cfg[cases][json_file]["stream_info"]["trigger_mode"] == "at_once":
                    source_stb_query_sql = f'select top(`max(a)`, 1) from (select max(a) from ({batch_query_sql}) order by a desc) limit 1;'
                elif cfg[cases][json_file]["stream_info"]["trigger_mode"] == "window_close":
                    source_stb_query_sql = f'select top(`max(a)`, 2) from (select max(a) from ({batch_query_sql}) order by a desc) limit 1;'
                else:
                    pass

                f.write(f'--------{cases}---- {source_stb_query_sql}\t--------\n')
                self.tdSql.query(source_stb_query_sql)
                f.write(str(self.tdSql.query_data[0][0]))
                f.write(f'\n\n')

                self.tdSql.query(query_sql)
                if len(self.tdSql.query_data) > 0:
                    init_res = self.tdSql.query_data[0][0]
                else:
                    init_res = 0
                time.sleep(1)
                self.tdSql.query(query_sql)
                if len(self.tdSql.query_data) > 0:
                    expected_res = self.tdSql.query_data[0][0]
                else:
                    expected_res = 0
                end_tag = 0
                latency = 0
                while init_res != expected_res:
                    self.tdSql.query(query_sql)
                    init_res = self.tdSql.query_data[0][0]
                    time.sleep(1)
                    self.tdSql.query(query_sql)
                    expected_res = self.tdSql.query_data[0][0]
                    if latency < self.stream_timeout:
                        latency += 1
                        time.sleep(1)
                    else:
                        return False

                f.write(f'--------{cases}---- {query_sql} \t--------\n')
                if len(self.tdSql.query_data) > 0:
                    self.tdSql.query(query_sql)
                    f.write(str(self.tdSql.query_data[0][0]))
                else:
                    f.write(str(0))
                f.write(f'\n\n')
                
                f.write(f'--------{cases}---- sub_query row_count \t--------\n')
                self.tdSql.query(cfg[cases][json_file]["stream_info"]["source_sql"])
                f.write(str(self.tdSql.query_row))
                f.write(f'\n\n')

                f.write(f'--------{cases}---- target_db row_count \t--------\n')
                self.tdSql.query(f'select count(*) from {cfg[cases][json_file]["stream_info"]["stream_stb"]};')
                if len(self.tdSql.query_data) > 0:
                    f.write(str(self.tdSql.query_data[0][0]))
                else:
                    f.write(str(0))
                f.write(f'\n\n')

                f.write(f'--------{cases}---- final result \t--------\n')
                self.tdSql.query(f'select avg(sp),max(sp),min(sp) from (select start,spread(cha) as sp from ((select _wstart as start  ,max(cast(c4 as bigint)) as cha from {cfg[cases][json_file]["stb_info"]["stb_name"]} interval(1s)) \
                union all (select start, cast(`now` as bigint)  from {cfg[cases][json_file]["stream_info"]["stream_stb"]}) order by start) partition by start order by start ) where sp>0;')
                if len(self.tdSql.query_data) > 0:
                    f.write(str([round(x, 1) for x in self.tdSql.query_data[0]]))
                else:
                    f.write(str(0))
                f.write(f'\n\n')

                f.close()
            print(result_file_name)
            
