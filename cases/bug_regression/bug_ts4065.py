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
from taostest.util.common import TDCom
from taostest import TDCase
from taostest.components.taosd import TaosD
from taostest.util.remote import Remote
from taosws import Consumer

class TestTs4065(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.taosadapter_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosadapter"
        )
        self.taosadapter_fqdn_list = self.taosadapter_setting["fqdn"]
        self.result_file_name = ""
        self.file_name1 = "insert0.json"
        self.file_name2 = "insert1.json"
        self.file_name3 = "insert2.json"
        self._tmp_dir: str = os.path.join(self.run_log_dir, "tmp")
        self.replica = 3
        self.vgroups = 10
        self.create_table_thread_count = 40
        self.thread_count = 40
        self.childtable_count = 10000
        self.insert_rows = 10000000
        self.num_of_records_per_req = 10000
        self.dbname = "test"
        self.stbname = "stb"
        self.loop_count = 10
        self.insert_mode = "rest"
        self.column_info_list = [
          {
            "type": "INT",
            "count": 1
          }
        ]
        self.tag_info_list = [
          {
            "type": "INT",
            "count": 1
          }
        ]

        self.offset_value = "earliest"
        self.topic_name = 'topic1'
        self.consumer_ip = "u1-54"
        self.consumer_port = "6041"
        self.consumer_connect_scheme = "ws"

        self.tmq_status = 0
        self.query_interval = 10
        self.start_timestamp = self.tdCom.genTs()[0]
        self.timestep = 1
        self.tdSql.execute(f'drop topic if exists {self.topic_name}')

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def tmq_subcribe(self):
        if self.tmq_status == 0:
            self.tdSql.query(f'show {self.dbname}.stables')
            if self.stbname in str(self.tdSql.query_data):
                queryString = "select ts, log(c0), ceil(pow(c0,3)) from %s.%s where c0 %% 7 >= 0" %(self.dbname, self.stbname)
                sqlString = "create topic %s as %s" %(self.topic_name, queryString)
                self.tdSql.execute(sqlString)
                consumer_dict = {
                                    "td.connect.websocket.scheme": self.consumer_connect_scheme,
                                    "td.connect.ip": self.consumer_ip,
                                    "td.connect.port": self.consumer_port,
                                    # consume options
                                    "group.id": "test_group_py",
                                    "client.id": "test_consumer_ws_py",
                                    "auto.offset.reset": self.offset_value
                                }
                self._remote._logger.info("create topic successful")
                consumer = Consumer(consumer_dict)
                consumer.subscribe([self.topic_name])
                self.tmq_status = 1

                while True:
                    res = consumer.poll(100)
                    if res:
                        for block in res:
                            nrows = block.nrows()
                            ncols = block.ncols()
                            for row in block:
                                print(row)
                            values = block.fetchall()
                            print(nrows, ncols)
                    else:
                        break

    def run(self):
        self.tdCom.createDb(dbname=self.dbname, replica=self.replica)
        taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        self.tdCom.add_back_ground_scheduler(self.tmq_subcribe, "interval", seconds=self.query_interval, max_instances=1, args=[])
        json_data_list = list()
        json_filename_list = list()
        json_filename_list.append(self.file_name1)
        json_filename_list.append(self.file_name2)
        json_filename_list.append(self.file_name3)

        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop="no")
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.start_timestamp, insert_mode=self.insert_mode, timestamp_step=self.timestep)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.taosadapter_fqdn_list[0]
        json_info1 = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name1, json_info1)
        json_data_list.append(json_info1)

        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop="no")
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.start_timestamp+31536000000, insert_mode=self.insert_mode, timestamp_step=self.timestep, child_table_exists="yes")]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.taosadapter_fqdn_list[1]
        json_info2 = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name2, json_info2)
        json_data_list.append(json_info2)

        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop="no")
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.start_timestamp+31536000000*2, insert_mode=self.insert_mode, timestamp_step=self.timestep, child_table_exists="yes")]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.taosadapter_fqdn_list[2]
        json_info3 = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name3, json_info3)
        json_data_list.append(json_info3)

        self.tdCom.put_file(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
