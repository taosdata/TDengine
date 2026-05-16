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
import sys


class LongTimeInsert(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.stb_name = "stb"
        self.ctb_name = "ctb"
        self.tb_name = "tb"
        self.des_table_suffix = "_output"
        self.non_prikey_ts_col_name = ""
        self.restart_timeout = 10
        self.syncing_drop_count = 10
        # self.query_interval = 7200
        # self.query_interval = 3600
        self.query_interval = 120
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
        self.drop_tag = False

        self.record_endpoint = None
        self.record_dnode = None

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

    def alter_replica(self, dbname, dnode_count=3, history_replica=None):
        self.write_log(f'--------- alter database: {dbname} replica 1 \t--------\n')
        self.tdSql.execute(f'alter database {dbname} replica 1')
        sync_time = self.tdSql.wait_sync_ready(dbname, sync_value=self.tdSql.get_db_vgroup_status(dbname, True, dnode_count=dnode_count, history_replica=history_replica))
        self.write_log(f'--------- alter {dbname} replica 1 sync time --- {sync_time}s \t--------\n')

        self.write_log(f'--------- alter database: {dbname} replica 3 \t--------\n')
        self.tdSql.execute(f'alter database {dbname} replica {dnode_count}')
        sync_time = self.tdSql.wait_sync_ready(dbname, sync_value=self.tdSql.get_db_vgroup_status(dbname, True, dnode_count=dnode_count, history_replica=history_replica))
        self.write_log(f'--------- alter {dbname} replica {dnode_count} sync time --- {sync_time}s \t--------\n')

    def drop_exist_table(self, dbname, ctbname_prefix, count=10):
        create_tb_sql_list = list()
        for i in range(count):
            self.tdSql.query(f'show create table {dbname}.{ctbname_prefix}0{i}')
            create_tb_sql_list.append(self.tdSql.query_data[0][1])
            self.tdSql.execute(f'drop table if exists {dbname}.{ctbname_prefix}0{i}')
        return create_tb_sql_list
        
    def create_dropped_table(self, dbname, create_tb_sql_list):
        self.tdSql.execute(f'use {dbname}')
        for i in create_tb_sql_list:
            self.tdSql.execute(i)

    def alter_table(self, dbname, stb_info_dict):
        if "alter_table" in stb_info_dict:
            self.tdSql.execute(f'alter stable {dbname}.{stb_info_dict["stb_name"]} rename tag `t1` `t1_update`')
            self.tdSql.execute(f'alter stable {dbname}.{stb_info_dict["stb_name"]} rename tag `t1_update` `t1`')

    def restart_sync(self, db_list, db_info_dict=None, stb_info_dict=None, dnode_count=3, stop_dnode=True, drop_count=10):
        dnodes_out_mnodes = self.tdSql.get_dnodes_out_mnodes()
        random_endpoint = random.choice(dnodes_out_mnodes[1])
        self.tdSql.query(f'select name,ntables,`replica` from information_schema.ins_databases;')
        db_field_kv = self.tdSql.get_db_field_kv(0, db_info_dict["db_name"])
        ntables_count = db_field_kv["ntables"]

        if stop_dnode and "alter_replica" not in stb_info_dict:
            self.write_log(f'--------- self.record_endpoint ----------- {self.record_endpoint} \t--------\n')
            self.write_log(f'--------- dnodes_out_mnodes ----------- {dnodes_out_mnodes} \t--------\n')
            if self.record_endpoint is not None:
                if "offline" in dnodes_out_mnodes[2]:
                    for i in range(len(dnodes_out_mnodes[2])):
                        if dnodes_out_mnodes[2][i] == "offline":
                            # get a random dnode
                            for taosd_setting in self.taosd_setting["spec"]["dnodes"]:
                                if taosd_setting["endpoint"] == dnodes_out_mnodes[2][i]:
                                    self.record_dnode = taosd_setting
                            self.write_log(f'--------- trying to restart dnode again: --- {self.record_endpoint} \t--------\n')
                            self.taosd.start(self.record_dnode)
                    for dbname in db_list:
                        sync_time = self.tdSql.wait_sync_ready(dbname, sync_value=self.tdSql.get_db_vgroup_status(dbname, False))
                        self.write_log(f'--------- dbname: {dbname} <start again > sync time --- {sync_time}s \t--------\n\n')

            if int(ntables_count) == stb_info_dict["childtable_count"]:
                self.write_log(f'--------- (dbname, ntables) --- {self.tdSql.query_data} \t--------\n')
                for dbname in db_list:
                    self.tdSql.query(f'select count(*) from {dbname}.stb0;')
                    if len(self.tdSql.query_data) > 0:
                        self.write_log(f'--------- select count(*) from {dbname}.stb0 --- {self.tdSql.query_data[0][0]} rows \t--------\n')
                    else:
                        self.write_log(f'--------- select count(*) from {dbname}.stb0 --- 0 rows \t--------\n')

                self.write_log(f'--------- killing dnode: --- {random_endpoint} \t--------\n')
                self.taosd.kill_by_port(random_endpoint)
                self.record_endpoint = random_endpoint
                for dbname in db_list:
                    select_leader_time = self.tdSql.wait_select_leader(dbname)
                    self.write_log(f'--------- dbname: {dbname} <stop> select leader time --- {select_leader_time}s \t--------\n')
                    sync_time = self.tdSql.wait_sync_ready(dbname, sync_value=self.tdSql.get_db_vgroup_status(dbname, True))
                    self.write_log(f'--------- dbname: {dbname} <stop> sync time --- {sync_time}s \t--------\n')

                # get a random dnode
                for taosd_setting in self.taosd_setting["spec"]["dnodes"]:
                    if taosd_setting["endpoint"] == random_endpoint:
                        random_dnode = taosd_setting
                self.record_dnode = random_dnode

                # get drop_kill_rows
                if "drop_kill_rows" in stb_info_dict:
                    drop_kill_rows = stb_info_dict["drop_kill_rows"]
                else:
                    drop_kill_rows = 1000000

                dbname = db_info_dict["db_name"]
                
                if "syncing_drop" in stb_info_dict and not self.drop_tag:
                    if stb_info_dict["syncing_drop"] == 0 or stb_info_dict["syncing_drop"] == 2:
                        self.write_log(f'---------  syncing_drop (=0): drop before sync will start when row_count > {drop_kill_rows} \t--------\n')
                        self.tdSql.query(f'select count(*) from {dbname}.stb0;')
                        self.write_log(f'--------- select count(*) from {dbname}.stb0 --- {self.tdSql.query_data[0][0]} rows (syncing_drop=0) \t--------\n')
                        if len(self.tdSql.query_data) > 0:
                            if int(self.tdSql.query_data[0][0]) >= drop_kill_rows:
                                create_tb_sql_list = self.drop_exist_table(dbname, ctbname_prefix=stb_info_dict["childtable_prefix"], count=drop_count)
                                self.create_dropped_table(dbname, create_tb_sql_list)
                                self.alter_table(dbname, stb_info_dict)
                                # self.tdSql.execute(f'drop database {dbname};')
                                # self.tdCom.createDb(db_info_dict["db_name"], replica=db_info_dict["replica"], vgroups=db_info_dict["vgroups"])
                                # self.drop_tag = True
                    else:
                        self.write_log(f'--------- syncing_drop != 0 and will not start drop operate before sync \t--------\n')
                else:
                    self.write_log(f'--------- no syncing_drop config in yaml \t--------\n')

                self.write_log(f'--------- starting dnode: --- {random_endpoint} \t--------\n')
                self.taosd.start(random_dnode)
                
                
                if "syncing_drop" in stb_info_dict and not self.drop_tag:
                    if stb_info_dict["syncing_drop"] == 1 or stb_info_dict["syncing_drop"] == 2:
                        self.write_log(f'--------- syncing_drop (=1) will start when row_count > {drop_kill_rows} \t--------\n')
                        self.tdSql.query(f'select count(*) from {dbname}.stb0;')
                        self.write_log(f'--------- select count(*) from {dbname}.stb0 --- {self.tdSql.query_data[0][0]} rows (syncing_drop=1) \t--------\n')
                        if len(self.tdSql.query_data) > 0:
                            if int(self.tdSql.query_data[0][0]) >= drop_kill_rows:
                                create_tb_sql_list = self.drop_exist_table(dbname, ctbname_prefix=stb_info_dict["childtable_prefix"], count=drop_count)
                                self.create_dropped_table(dbname, create_tb_sql_list)
                                self.alter_table(dbname, stb_info_dict)
                                # self.tdSql.execute(f'drop database {dbname};')
                                # self.tdCom.createDb(db_info_dict["db_name"], replica=db_info_dict["replica"], vgroups=db_info_dict["vgroups"])
                                # self.drop_tag = True

                    else:
                        self.write_log(f'--------- syncing_drop != 1 and will not start syncing_drop operate \t--------\n')
                else:
                    self.write_log(f'--------- no syncing_drop config in yaml \t--------\n')


                for dbname in db_list:
                    sync_time = self.tdSql.wait_sync_ready(dbname, sync_value=self.tdSql.get_db_vgroup_status(dbname, False))
                    self.write_log(f'--------- dbname: {dbname} <start> sync time --- {sync_time}s \t--------\n\n')
            else:
                self.write_log(f'--------- dbname: {db_info_dict["db_name"]} {ntables_count} != {stb_info_dict["childtable_count"]} stop restart-action: \t--------\n')
            
        if "alter_replica"  in stb_info_dict:
            history_replica = db_field_kv["replica"]
            for dbname in db_list:
                self.alter_replica(dbname, dnode_count, history_replica)


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
        cfg = read_yaml(test_root + "/cases/stability/insert/long_insert/insert.yaml")

        jfile = InsertFile()
        Insert_file = Perf_Base_func(self.logger, self.run_log_dir)
        self.tdSql.execute(f'drop database if exists perf_test')
        for cases in cfg:
            db_list = list()
            i = 0
            # self.clean_and_restart_taosd()
            # return
            for json_file in cfg[cases]:
                db_list.append(cfg[cases][json_file]["db_info"]["db_name"])

            for json_file in cfg[cases]:
                self.tdSql.query('select * from information_schema.ins_databases;')
                self.streams = None
                self.tdSql.execute(f'create database if not exists {cfg[cases][json_file]["db_info"]["db_name"]} replica {cfg[cases][json_file]["db_info"]["replica"]} vgroups {cfg[cases][json_file]["db_info"]["vgroups"]} stt_trigger {cfg[cases][json_file]["db_info"]["stt_trigger"]}')
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
                                     keep=cfg[cases][json_file]["db_info"]["keep"],
                                     stt_trigger=cfg[cases][json_file]["db_info"]["stt_trigger"]
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
                                        stt_trigger=cfg[cases][json_file]["db_info"]["stt_trigger"]
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
                                       keep_trying=cfg[cases][json_file]["stb_info"]["keep_trying"],
                                       trying_interval=cfg[cases][json_file]["stb_info"]["trying_interval"],
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
                                        keep_trying=cfg[cases][json_file]["stb_info"]["keep_trying"],
                                        trying_interval=cfg[cases][json_file]["stb_info"]["trying_interval"],
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

            if "repeat_delete_times" in cfg[cases][json_file]["stb_info"]:
                timestamp_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                # # run taosBenchmark
                taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
                for i in range(cfg[cases][json_file]["stb_info"]["repeat_delete_times"]):
                    self.write_log(f"-------- \loop time {i+1} create db&table, insert rows, drop table&db --------\n")
                    result_filename = Insert_file.threads_run_taosBenchmark(
                        taosBenchmark_iplist, json_data, file_name, taosBenchmark_env_setting
                    )
                    self.tdSql.query(f'select count(*) from {cfg[cases][json_file]["db_info"]["db_name"]}.{cfg[cases][json_file]["stb_info"]["stb_name"]}')
                    self.tdSql.checkEqual(self.tdSql.query_data[0][0], cfg[cases][json_file]["stb_info"]["childtable_count"] * cfg[cases][json_file]["stb_info"]["insert_rows"])
                    self.restart_sync(db_list=[cfg[cases][json_file]["db_info"]["db_name"]], db_info_dict=cfg[cases][json_file]["db_info"], stb_info_dict=cfg[cases][json_file]["stb_info"], drop_count=self.syncing_drop_count)
                    for j in range(int(cfg[cases][json_file]["stb_info"]["childtable_count"])):
                        self.tdSql.execute(f'drop table {cfg[cases][json_file]["db_info"]["db_name"]}.{cfg[cases][json_file]["stb_info"]["childtable_prefix"]}0{j}')
                    self.tdSql.execute(f'drop stable {cfg[cases][json_file]["db_info"]["db_name"]}.{cfg[cases][json_file]["stb_info"]["stb_name"]}')
                    self.tdSql.error(f'select * from {cfg[cases][json_file]["db_info"]["db_name"]}.{cfg[cases][json_file]["stb_info"]["stb_name"]}')
                    self.tdSql.execute(f'drop database {cfg[cases][json_file]["db_info"]["db_name"]}')
                    self.tdSql.error(f'use {cfg[cases][json_file]["db_info"]["db_name"]}')
            elif "no_wait_restart_times" in cfg[cases][json_file]["stb_info"]:
                timestamp_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                # # run taosBenchmark
                taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
                result_filename = Insert_file.threads_run_taosBenchmark(
                    taosBenchmark_iplist, json_data, file_name, taosBenchmark_env_setting
                )
                self.tdSql.query(f'select count(*) from {cfg[cases][json_file]["db_info"]["db_name"]}.{cfg[cases][json_file]["stb_info"]["stb_name"]}')
                self.tdSql.checkEqual(self.tdSql.query_data[0][0], cfg[cases][json_file]["stb_info"]["childtable_count"] * cfg[cases][json_file]["stb_info"]["insert_rows"])
                dnodes_out_mnodes = self.tdSql.get_dnodes_out_mnodes()
                random_endpoint = random.choice(dnodes_out_mnodes[1])
                for i in range(cfg[cases][json_file]["stb_info"]["no_wait_restart_times"]):
                    self.write_log(f'--------- killing dnode: --- {random_endpoint} \t--------\n')
                    self.taosd.kill_by_port(random_endpoint)
                    # get a random dnode
                    for taosd_setting in self.taosd_setting["spec"]["dnodes"]:
                        if taosd_setting["endpoint"] == random_endpoint:
                            random_dnode = taosd_setting
                    self.write_log(f'--------- starting dnode: --- {random_endpoint} \t--------\n')
                    self.taosd.start(random_dnode)
            
                self.tdSql.query(f'select name,ntables from information_schema.ins_databases;')
                self.write_log(f'--------- (dbname, ntables) --- {self.tdSql.query_data} \t--------\n')
                for dbname in db_list:
                    sync_time = self.tdSql.wait_sync_ready(dbname, sync_value=self.tdSql.get_db_vgroup_status(dbname, False))
                    self.write_log(f'--------- dbname: {dbname} sync time --- {sync_time}s \t--------\n')

                    self.tdSql.query(f'select count(*) from {dbname}.stb0;')
                    if len(self.tdSql.query_data) > 0:
                        self.write_log(f'--------- select count(*) from {dbname}.stb0 --- {self.tdSql.query_data[0][0]} rows \t--------\n')
                    else:
                        self.write_log(f'--------- select count(*) from {dbname}.stb0 --- 0 rows \t--------\n')
            else:
                if "restart_sync" in cfg[cases][json_file]["stb_info"]:
                    scheduler = BackgroundScheduler()
                    scheduler.add_job(self.restart_sync, 'interval', seconds=self.query_interval, max_instances=1, args=[db_list, cfg[cases][json_file]["db_info"], cfg[cases][json_file]["stb_info"], 3, True, self.syncing_drop_count])
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
