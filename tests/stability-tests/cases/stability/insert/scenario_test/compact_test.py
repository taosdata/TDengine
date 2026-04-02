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
from typing import List
from taostest import TDCase
from taostest.components.taosd import TaosD
from taostest.util.remote import Remote
import json
import time


class CompactTest(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.result_file_name = ""
        self.file_name1 = "insert0.json"
        self._tmp_dir: str = os.path.join(self.run_log_dir, "tmp")
        self.replica = 1
        self.childtable_count = 300000
        self.insert_rows = 2000
        self.pre_rows = 1
        self.num_of_records_per_req1 = 10000
        self.interlace_rows = 100
        self.dbname = "db_test"
        self.stbname = "stb"
        self.query_interval = 10
        self.query_timeout = 10
        self.json_file = "/root/taos-test-framework/TestNG/cases/stability/insert/scenario_test/insert_bcld.json"
        self.compacted = False
        self.compact_end = False
        with open(self.json_file, "r") as f:
          self.json_info = json.load(f)
        self.json_info = self.tdCom.load_json(self.json_file)
        self.childtable_count = self.json_info["databases"][0]["super_tables"][0]["childtable_count"]
        self.insert_rows = self.json_info["databases"][0]["super_tables"][0]["insert_rows"]
        self.json_info["databases"][0]["super_tables"][0]["insert_rows"] = self.pre_rows
        self.host = self.get_fqdn("taosd")[0]
        self.json_info["test_log"] = "/root/testlog///"
        self.json_info["host"] = self.host
        self.json_info["databases"][0]["dbinfo"]["name"] = self.dbname
        self.vgroups = self.json_info["databases"][0]["dbinfo"]["vgroups"]
        self.json_info["databases"][0]["super_tables"][0]["name"] = self.stbname
        self.childtable_prefix = self.json_info["databases"][0]["super_tables"][0]["childtable_prefix"]
        self.tsdbCommitCompact_count = 0
        self.expected_tsdbCommitCompact_count = self.vgroups * self.replica
        self.log_files = self.taosd_setting["spec"]["dnodes"][0]["config"]["logDir"] + "/" + "taosdlog*"
        
    def half_compact(self):
        if not self.compacted:
            self.tdSql.query(f'select count(*) from {self.dbname}.{self.stbname}')
            self.logger.info(f"rows: {self.tdSql.query_data[0][0]}")
            if self.tdSql.query_data[0][0] > int((self.childtable_count * self.insert_rows)/2):
                self.logger.info("compact start")
                self.tdSql.execute(f'compact database {self.dbname}')
                self.compacted = True
                
    def comfirm_log(self):
        if not self.compact_end:
            tmp_count = self._remote.cmd(self.host, [f'grep -ri "tsdbCommitCompact Done" {self.log_files} 2>/dev/null | wc -l'])
            self.tsdbCommitCompact_count = int(tmp_count.strip())
            self.logger.info(f'tsdbCommitCompact_count: {self.tsdbCommitCompact_count}')
            if self.tsdbCommitCompact_count == self.expected_tsdbCommitCompact_count:
                self.compact_end = True

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def run(self):
        for sql in [f'drop table {self.dbname}.{self.stbname}',
                    f'drop table {self.dbname}.{self.childtable_prefix}1',
                    f'delete from {self.dbname}.{self.childtable_prefix}1']:
            if sql == f'drop table {self.dbname}.{self.stbname}':
                expected_final_rows = self.childtable_count*self.insert_rows
            else:
                expected_final_rows = self.childtable_count*self.insert_rows - self.insert_rows
            json_data_list = list()
            json_filename_list = list()
            taosBenchmark_iplist: List = self.get_fqdn("taosBenchmark")
            taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
            json_filename_list.append(self.file_name1)
            self.tdSql.execute(f'drop database if exists {self.dbname}')
            self.tdSql.execute(f'CREATE DATABASE {self.dbname} BUFFER 256 CACHESIZE 10 CACHEMODEL "last_row" COMP 2 DURATION 1051200m WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 10 STT_TRIGGER 10 KEEP 5256000m,5256000m,5256000m PAGES 256 PAGESIZE 4 PRECISION "ms" REPLICA 1 WAL_LEVEL 1 VGROUPS 16 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 0 WAL_RETENTION_SIZE 0 WAL_ROLL_PERIOD 0 WAL_SEGMENT_SIZE 0')
            self.json_info["test_log"] = "/root/testlog/"
            self.tdCom.dump_json(f'{self.run_log_dir}/insert0.json', self.json_info)
            json_data_list.append(self.json_info)
            
            self.tdCom.put_file(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, self.run_log_dir)
            self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
            self.tdSql.execute(sql)
            
            self.tdCom.add_back_ground_scheduler(self.half_compact, 'interval', seconds=self.query_interval, max_instances=1, args=[])
            self.tdCom.add_back_ground_scheduler(self.comfirm_log, 'interval', seconds=self.query_interval, max_instances=1, args=[])
            with open(self.json_file, "r") as f:
                self.json_info = json.load(f)
            self.json_info["databases"][0]["super_tables"][0]["insert_rows"] = self.insert_rows
            
            self.tdCom.dump_json(f'{self.run_log_dir}/insert0.json', self.json_info)
            json_data_list.append(self.json_info)
            self.tdCom.put_file(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, self.run_log_dir)
            self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
            
            self.tdSql.query(f'select count(*) from {self.dbname}.{self.stbname}')
            self.logger.info(f"final rows: {self.tdSql.query_data[0][0]}")
            latency = 0
            # def check_stream_res(self, sql, expected_res, max_delay):
            #     self.tdSql.query(sql)
            #     latency = 0
            #     if self.tdSql.query_row == expected_res:
            #         self.write_latency(latency)

            #     while self.tdSql.query_row != expected_res:
            #         self.tdSql.query(sql)
            #         if latency < self.stream_timeout:
            #             latency += 0.5
            #             time.sleep(0.5)
            #         else:
            #             if max_delay is not None:
            #                 if latency == 0:
            #                     return False
            #             self.tdSql.checkEqual(self.tdSql.query_row, expected_res)
            #         if self.tdSql.query_row == expected_res:
            #             self.write_latency(latency)
            #             return latency
            
            while self.tdSql.query_data[0][0] != expected_final_rows:
                self.tdSql.query(f'select count(*) from {self.dbname}.{self.stbname}')
                if latency < self.query_timeout:
                    latency += 1
                    time.sleep(1)
                else:
                    return False
                self.tdSql.checkEqual(self.tdSql.query_data[0][0], expected_final_rows)
            self.tdSql.checkEqual(self.tsdbCommitCompact_count, self.expected_tsdbCommitCompact_count)
            self.expected_tsdbCommitCompact_count += self.vgroups * self.replica
            self.compacted = False
            self.compact_end = False