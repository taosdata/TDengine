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
from datetime import datetime,timedelta
from typing import List
from taostest import TDCase
from taostest.performance.perfor_basic import InsertFile
from taostest.performance.result_reduction import Perf_Base_func
from taostest.components.taosd import TaosD
import time
from taostest.util.remote import Remote
from apscheduler.schedulers.background import BackgroundScheduler
import random


class LongTimeInsert(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.stb_name = "stb"
        self.ctb_name = "ctb"
        self.tb_name = "tb"
        self.des_table_suffix = "_output"
        self.non_prikey_ts_col_name = ""
        self.restart_timeout = 10
        # self.query_interval = 7200
        self.query_interval = 60
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.taosadapter_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosadapter"
        )
        self.fqdn_list = self.taosd_setting["fqdn"]
        self.counter = len(self.fqdn_list)
        self.firstEp = self.taosd_setting["spec"]["config"]["firstEP"]
        self.data_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"]
        self.log_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["logDir"]
        self.streams = None
        self.result_file_name = ""
        # now - 3d and now + 3d
        self.date_timespan = 6

    def drop_db_sync(self, dbname):
        self.tdSql.execute(f'drop database if exists {dbname}')
        # TODO
        
    def drop_table_sync(self, table_name):
        self.tdSql.execute(f'drop table if exists {table_name}')
        # TODO

    def write_log(self, msg):
        f = open(self.result_file_name, "a")
        f.write(msg)
        f.close

    def restart_sync(self, db_list):
        dnodes_out_mnodes = self.tdSql.get_dnodes_out_mnodes()
        random_endpoint = random.choice(dnodes_out_mnodes[1])
        self.tdSql.query(f'select name,ntables from information_schema.ins_databases;')
        self.write_log(f'--------- (dbname, ntables) --- {self.tdSql.query_data} \t--------\n')
        for dbname in db_list:
            self.tdSql.query(f'select count(*) from {dbname}.stb0;')
            if len(self.tdSql.query_data) > 0:
                self.write_log(f'--------- select count(*) from {dbname}.stb0 --- {self.tdSql.query_data[0][0]} rows \t--------\n')
            else:
                self.write_log(f'--------- select count(*) from {dbname}.stb0 --- 0 rows \t--------\n')

        self.write_log(f'--------- killing dnode: --- {random_endpoint} \t--------\n')
        self.taosd.kill_by_port(random_endpoint)
        for dbname in db_list:
            sync_time = self.tdSql.wait_sync_ready(dbname, sync_value=self.tdSql.get_db_vgroup_status(dbname, True))
            self.write_log(f'--------- dbname: {dbname} sync time --- {sync_time}s \t--------\n')


        for taosd_setting in self.taosd_setting["spec"]["dnodes"]:
            if taosd_setting["endpoint"] == random_endpoint:
                random_dnode = taosd_setting
        self.write_log(f'--------- starting dnode: --- {random_endpoint} \t--------\n')
        self.taosd.start(random_dnode)
        for dbname in db_list:
            sync_time = self.tdSql.wait_sync_ready(dbname, sync_value=self.tdSql.get_db_vgroup_status(dbname, False))
            self.write_log(f'--------- dbname: {dbname} sync time --- {sync_time}s \t--------\n')


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
        # print(self.tdCom.get_db_list())
        # print(self.tdSql.get_db_vgroup_status("test2", True))
        # self.tdCom.createDb("sml_line_rest", replica=3, vgroups=10)
        # while True:
        #     scheduler = BackgroundScheduler()
        #     scheduler.add_job(self.restart_sync, 'interval', seconds=self.query_interval, max_instances=1)
        #     scheduler.start()
        # self.tdCom.createDb("sml_line_rest", replica=3, vgroups=10)
        # print(self.tdSql.get_vnode_status_list("sml_line_rest"))
        # # print(self.tdSql.wait_sync_ready("sml_line_rest"))
        # print(self.tdSql.wait_sync_ready("sml_line_rest", sync_value=[("follower", "follower", "leader")]))
        # return
        json_data: List = []
        file_name = []
        test_root = os.environ['TEST_ROOT']
        cfg = read_yaml(test_root + "/cases/stability/insert/long_insert/insert.yaml")

        jfile = InsertFile()
        Insert_file = Perf_Base_func(self.logger, self.run_log_dir)
        # self.tdSql.execute(f'drop database if exists perf_db1')
        # self.tdSql.execute(f'create database if not exists perf_db1 vgroups 300')
        for cases in cfg:
            db_list = list()
            i = 0
            # self.clean_and_restart_taosd()
            # return
            for json_file in cfg[cases]:
                db_list.append(cfg[cases][json_file]["db_info"]["db_name"])
            
            for json_file in cfg[cases]:
                self.streams = None
                if cfg[cases][json_file]["stb_info"]["line_protocol"] == "telnet" or cfg[cases][json_file]["stb_info"]["line_protocol"] == "json":
                    col = jfile.schemacfg(doublecount=cfg[cases][json_file]["stb_info"]["col_double_count"])
                else:
                    col = jfile.schemacfg(tcount=cfg[cases][json_file]["stb_info"]["col_tinyint_count"],
                                        scount=cfg[cases][json_file]["stb_info"]["col_smallint_count"],
                                        intcount=cfg[cases][json_file]["stb_info"]["col_int_count"],
                                        bcount=cfg[cases][json_file]["stb_info"]["col_bigint_count"],
                                        utcount=cfg[cases][json_file]["stb_info"]["col_utinyint_count"],
                                        uscount=cfg[cases][json_file]["stb_info"]["col_usmallint_count"],
                                        uintcount=cfg[cases][json_file]["stb_info"]["col_uint_count"],
                                        ubcount=cfg[cases][json_file]["stb_info"]["col_ubigint_count"],
                                        floatcount=cfg[cases][json_file]["stb_info"]["col_float_count"],
                                        doublecount=cfg[cases][json_file]["stb_info"]["col_double_count"],
                                        varcharcount=(cfg[cases][json_file]["stb_info"]["col_varchar_count"],
                                                    cfg[cases][json_file]["stb_info"]["col_varchar_length"]),
                                        ncharcount=(cfg[cases][json_file]["stb_info"]["col_nchar_count"],
                                                    cfg[cases][json_file]["stb_info"]["col_nchar_length"],),
                                                    # cfg[cases][json_file]["stb_info"]["col_nchar_values"]),
                                        boolcount=cfg[cases][json_file]["stb_info"]["col_bool_count"])
                if cfg[cases][json_file]["stb_info"]["line_protocol"] == "json":
                    tag = jfile.schemacfg(intcount=cfg[cases][json_file]["stb_info"]["tag_int_count"],
                                        binarycount=(cfg[cases][json_file]["stb_info"]["tag_binary_count"],
                                                    cfg[cases][json_file]["stb_info"]["tag_binary_length"]),
                                        varcharcount=(cfg[cases][json_file]["stb_info"]["tag_varchar_count"],
                                                    cfg[cases][json_file]["stb_info"]["tag_varchar_length"]),
                                        doublecount=cfg[cases][json_file]["stb_info"]["tag_double_count"],
                                        floatcount=cfg[cases][json_file]["stb_info"]["tag_float_count"],
                                        bcount=cfg[cases][json_file]["stb_info"]["tag_bigint_count"],
                                        tcount=cfg[cases][json_file]["stb_info"]["tag_tinyint_count"],
                                        scount=cfg[cases][json_file]["stb_info"]["tag_smallint_count"],
                                        ncharcount=(cfg[cases][json_file]["stb_info"]["tag_nchar_count"],
                                                    cfg[cases][json_file]["stb_info"]["tag_nchar_length"]),
                                        boolcount=cfg[cases][json_file]["stb_info"]["tag_bool_count"],
                                        specified_elem="tag")
                else:
                    tag = jfile.schemacfg(intcount=cfg[cases][json_file]["stb_info"]["tag_int_count"],
                                        uintcount=cfg[cases][json_file]["stb_info"]["tag_uint_count"],
                                        binarycount=(cfg[cases][json_file]["stb_info"]["tag_binary_count"],
                                                    cfg[cases][json_file]["stb_info"]["tag_binary_length"]),
                                        varcharcount=(cfg[cases][json_file]["stb_info"]["tag_varchar_count"],
                                                    cfg[cases][json_file]["stb_info"]["tag_varchar_length"]),
                                        doublecount=cfg[cases][json_file]["stb_info"]["tag_double_count"],
                                        floatcount=cfg[cases][json_file]["stb_info"]["tag_float_count"],
                                        bcount=cfg[cases][json_file]["stb_info"]["tag_bigint_count"],
                                        tcount=cfg[cases][json_file]["stb_info"]["tag_tinyint_count"],
                                        scount=cfg[cases][json_file]["stb_info"]["tag_smallint_count"],
                                        ubcount=cfg[cases][json_file]["stb_info"]["tag_ubigint_count"],
                                        utcount=cfg[cases][json_file]["stb_info"]["tag_utinyint_count"],
                                        uscount=cfg[cases][json_file]["stb_info"]["tag_usmallint_count"],
                                        ncharcount=(cfg[cases][json_file]["stb_info"]["tag_nchar_count"],
                                                    cfg[cases][json_file]["stb_info"]["tag_nchar_length"]),
                                        tscount=cfg[cases][json_file]["stb_info"]["tag_timestamp_count"],
                                        boolcount=cfg[cases][json_file]["stb_info"]["tag_bool_count"],
                                        specified_elem="tag")
                # set json_files for taosBenchmark
                db = jfile.setDBinfo(name=cfg[cases][json_file]["db_info"]["db_name"],
                                     drop=cfg[cases][json_file]["db_info"]["drop"],
                                     replica=cfg[cases][json_file]["db_info"]["replica"],
                                     precision=cfg[cases][json_file]["db_info"]["precision"],
                                     vgroups=cfg[cases][json_file]["db_info"]["vgroups"],
                                     duration=cfg[cases][json_file]["db_info"]["duration"],
                                     keep=cfg[cases][json_file]["db_info"]["keep"]
                                     )
                if "retentions" in cfg[cases][json_file]["db_info"]:
                    db = jfile.setDBinfo(name=cfg[cases][json_file]["db_info"]["db_name"],
                                        drop=cfg[cases][json_file]["db_info"]["drop"],
                                        replica=cfg[cases][json_file]["db_info"]["replica"],
                                        precision=cfg[cases][json_file]["db_info"]["precision"],
                                        vgroups=cfg[cases][json_file]["db_info"]["vgroups"],
                                        duration=cfg[cases][json_file]["db_info"]["duration"],
                                        keep=cfg[cases][json_file]["db_info"]["keep"],
                                        retentions=cfg[cases][json_file]["db_info"]["retentions"],
                                        )
                if "stream_info" in cfg[cases][json_file]:
                    if "watermark" in cfg[cases][json_file]["stream_info"]:
                        watermark = cfg[cases][json_file]["stream_info"]["watermark"]
                    else:
                        watermark = None
                    self.streams = jfile.setStreams(stream_name=cfg[cases][json_file]["stream_info"]["stream_name"],
                                                stream_stb=cfg[cases][json_file]["stream_info"]["stream_stb"],
                                                trigger_mode=cfg[cases][json_file]["stream_info"]["trigger_mode"],
                                                watermark=watermark,
                                                source_sql=cfg[cases][json_file]["stream_info"]["source_sql"],
                                                drop=cfg[cases][json_file]["stream_info"]["drop"])
                # start_timestamp =  (datetime.now() + timedelta(days=-1)).strftime("%Y-%m-%d %H:%M:%S")
                # timestamp_step = int(self.date_timespan / 2 * 86400 / int(cfg[cases][json_file]["stb_info"]["insert_rows"]) * 1000)
                stb_name = cfg[cases][json_file]["stb_info"]["stb_name"]
                # if cfg[cases][json_file]["stb_info"]["tcp_transfer"] == "yes":
                #     stb_name = self.taosadapter_setting["spec"]["adapter_config"]["opentsdb_telnet"]["dbs"][0]
                stb = jfile.setStbinfo(name=stb_name,
                                       childtable_prefix=cfg[cases][json_file]["stb_info"]["childtable_prefix"] + str(
                                           i),
                                       childtable_count=cfg[cases][json_file]["stb_info"]["childtable_count"],
                                       insert_rows=cfg[cases][json_file]["stb_info"]["insert_rows"], columns=col,
                                       tags=tag,
                                       timestamp_step=cfg[cases][json_file]["stb_info"]["timestamp_step"],
                                       start_timestamp=cfg[cases][json_file]["stb_info"]["start_timestamp"],
                                       insert_mode=cfg[cases][json_file]["stb_info"]["insert_mode"],
                                       max_sql_len=cfg[cases][json_file]["stb_info"]["max_sql_len"],
                                       partial_col_num=cfg[cases][json_file]["stb_info"]["partial_col_num"],
                                       auto_create_table=cfg[cases][json_file]["stb_info"]["auto_create_table"],
                                       interlace_rows=cfg[cases][json_file]["stb_info"]["interlace_rows"],
                                       line_protocol=cfg[cases][json_file]["stb_info"]["line_protocol"],
                                       tcp_transfer=cfg[cases][json_file]["stb_info"]["tcp_transfer"],
                                       batch_create_tbl_num=cfg[cases][json_file]["stb_info"]["batch_create_tbl_num"])
                if "rollup" in cfg[cases][json_file]["stb_info"]:
                    stb = jfile.setStbinfo(name=cfg[cases][json_file]["stb_info"]["stb_name"],
                                        childtable_prefix=cfg[cases][json_file]["stb_info"]["childtable_prefix"] + str(
                                            i),
                                        childtable_count=cfg[cases][json_file]["stb_info"]["childtable_count"],
                                        insert_rows=cfg[cases][json_file]["stb_info"]["insert_rows"], columns=col,
                                        tags=tag,
                                        timestamp_step=cfg[cases][json_file]["stb_info"]["timestamp_step"],
                                        start_timestamp=cfg[cases][json_file]["stb_info"]["start_timestamp"],
                                        insert_mode=cfg[cases][json_file]["stb_info"]["insert_mode"],
                                        partial_col_num=cfg[cases][json_file]["stb_info"]["partial_col_num"],
                                        max_sql_len=cfg[cases][json_file]["stb_info"]["max_sql_len"],
                                        auto_create_table=cfg[cases][json_file]["stb_info"]["auto_create_table"],
                                        interlace_rows=cfg[cases][json_file]["stb_info"]["interlace_rows"],
                                        line_protocol=cfg[cases][json_file]["stb_info"]["line_protocol"],
                                        rollup=cfg[cases][json_file]["stb_info"]["rollup"],
                                        tcp_transfer=cfg[cases][json_file]["stb_info"]["tcp_transfer"],
                                        batch_create_tbl_num=cfg[cases][json_file]["stb_info"]["batch_create_tbl_num"])

                database = jfile.setDatabases(dbinfo=db, super_tables=[stb])
                json_info = jfile.setJsoninfo(host=cfg[cases][json_file]["json_info"]["host"], databases=[database],
                                            thread_count=cfg[cases][json_file]["json_info"]["thread_count"],
                                            rest_port=cfg[cases][json_file]["json_info"]["rest_port"],
                                            create_table_thread_count=cfg[cases][json_file]["json_info"]["create_table_thread_count"],
                                            streams=self.streams,
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
            self.result_file_name = self.run_log_dir + "/perf_report.txt"
            self.write_log("-------- \tinsert\t" + str(cases) + ":\tinsert result--------\n")
            scheduler = BackgroundScheduler()
            scheduler.add_job(self.restart_sync, 'interval', seconds=self.query_interval, max_instances=1, args=[db_list])
            # for stbname in self.stb_list:
            #     scheduler.add_job(self.query_last_row, 'interval', seconds=self.query_interval, max_instances=10, args=[f'{cfg[cases][json_file]["db_info"]["db_name"]}.{stbname}'])
            # for stbname in self.stb_list:
            #     scheduler.add_job(self.query_count, 'interval', seconds=self.query_interval, max_instances=10, args=[f'{cfg[cases][json_file]["db_info"]["db_name"]}.{stbname}'])
            scheduler.start()
            timestamp_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            # # run taosBenchmark
            taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
            result_filename = Insert_file.threads_run_taosBenchmark(
                taosBenchmark_iplist, json_data, file_name, taosBenchmark_env_setting
            )

            timestamp_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            # get insert result
            # Insert_file.full_create_tb_result(result_filename)
            Insert_file.taosBenchmark_insert_summary_result(
                result_filename, version="3.0"
            )
            Insert_file.taosBenchmark_id_insert_result(result_filename)

            # get node_info and process_info
            env_setting = self.get_component_by_name("prometheus")
            Insert_file.get_process_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
            Insert_file.get_node_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
            print(self.result_file_name)
