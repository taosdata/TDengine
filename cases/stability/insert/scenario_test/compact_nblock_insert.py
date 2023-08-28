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

from taostest.util.common import TDCom
from taostest import TDCase
from taostest.components.taosd import TaosD
from taostest.util.remote import Remote
import time
from datetime import datetime,timedelta


class CompactNblockInsertTest(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.json_filename = "insert0.json"
        self.replica = 1
        self.childtable_count = 10000
        self.insert_rows = 200000
        self.dbname = "db_test"
        self.stbname = "stb"
        self.query_interval = 30
        
        self.vgroups = 40
        self.host = self.get_fqdn("taosd")[0]
        self.thread_count = 40
        self.num_of_records_per_req = 1000
        self.interlace_rows = 1000
        self.tsdbCommitCompact_count = 0
        self.expected_tsdbCommitCompact_count = self.vgroups * self.replica
        self.log_files = self.taosd_setting["spec"]["dnodes"][0]["config"]["logDir"] + "/" + "taosdlog*"
        
        self.compacting = False
        self.compact_end = False
        self.compact_counter = 0
    
    def start_compact(self):
        self.logger.info("in start_compact scheduler")
        if not self.compacting:
            self.tdSql.query(f'select count(*) from {self.dbname}.{self.stbname}')
            self.logger.info(f"rows: {self.tdSql.query_data[0][0]}")
            if self.tdSql.query_data[0][0] > int((self.childtable_count * self.insert_rows)/100):
                self.logger.info("compact start")
                self.tdSql.execute(f'compact database {self.dbname}')
                self.compact_counter += 1
                self.compacting = True
                self.logger.info("already start compact")
                
    def comfirm(self):
        self.logger.info("in confirming scheduler")
        if self.compacting:
            tmp_count = self._remote.cmd(self.host, [f'grep -ri "tsdbCommitCompact Done" {self.log_files} 2>/dev/null | wc -l'])
            self.tsdbCommitCompact_count = int(tmp_count.strip())
            self.logger.info(f'expected_res: {self.expected_tsdbCommitCompact_count*self.compact_counter}')
            self.logger.info(f'tsdbCommitCompact_count: {self.tsdbCommitCompact_count}')
            if self.tsdbCommitCompact_count == self.expected_tsdbCommitCompact_count*self.compact_counter:
                self.compacting = False
                self.logger.info("compact successful and reset status")
            else:
                self.tdSql.query(f'select count(*) from {self.dbname}.{self.stbname}')
                res1 = self.tdSql.query_data[0][0]
                self.logger.info(f"confirming rows: {self.tdSql.query_data[0][0]}")
                time.sleep(self.query_interval)
                self.tdSql.query(f'select count(*) from {self.dbname}.{self.stbname}')
                res2 = self.tdSql.query_data[0][0]
                self.logger.info(f"confirming rows: {self.tdSql.query_data[0][0]}")
                self.tdSql.checkEqual(res2>res1, True)
                self.logger.info("confirm finished")

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def run(self):
        json_data_list = list()
        json_filename_list = list()
        taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        
        json_filename_list.append(self.json_filename)
        
        column_info_list = [
          {
            "type": "INT",
            "count": 2
          }
        ]
        tag_info_list = [
          {
            "type": "INT",
            "count": 1
          }
        ]
        start_timestamp = (datetime.now() + timedelta(days=0)).strftime("%Y-%m-%d %H:%M:%S")
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups)
        stb_into = [self.tdCom.setStbinfo(columns=column_info_list, tags=tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=start_timestamp, name=self.stbname, interlace_rows=self.interlace_rows)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        json_info = self.tdCom.setJsoninfo(host=self.host, databases=database_info, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)

        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_filename, json_info)
        json_data_list.append(json_info)
        self.tdCom.put_file(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, self.run_log_dir)
        
        self.tdCom.add_back_ground_scheduler(self.start_compact, 'interval', seconds=self.query_interval, max_instances=1, args=[])
        self.tdCom.add_back_ground_scheduler(self.comfirm, 'interval', seconds=self.query_interval, max_instances=1, args=[])
        self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
        self.tdSql.query(f'select count(*) from {self.dbname}.{self.stbname}')
        self.tdSql.checkEqual(self.tdSql.query_data[0][0], self.childtable_count*self.insert_rows)
        