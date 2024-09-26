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
# -*- taostest --setup=local.yaml --case=bug_regression/bug_ts5393.py --keep -*-

import os
from taostest.util.common import TDCom
from typing import List
from taostest import TDCase
from taostest.components.taosd import TaosD
from taostest.util.remote import Remote
from datetime import datetime
from copy import deepcopy
import pandas as pd
import copy
import random
import concurrent.futures
import time

class BugTs5393(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.cfg = self.tdCom.Boundary.DB_PARAM_VGROUPS_CONFIG
        self.base_dnode_list = self.taosd_setting["spec"]["dnodes"]
        self.result_file_name = ""
        self._tmp_dir: str = os.path.join(self.run_log_dir, "tmp")
        self.replica = int(os.environ["DATABASE_REPLICAS"]) if "DATABASE_REPLICAS" in os.environ else 1
        self.vgroups = 40
        self.create_table_thread_count = 40
        self.thread_count = 80
        self.num_of_records_per_req = 10000
        self.childtable_count = 10000000
        self.insert_rows = 10
        self.start_timestamp = "2020-01-01 00:00:00"
        self.timewait = 10
        self.dbname = "test"
        self.stbname1 = "stb1"
        self.stbname2 = "stb2"
        self.stbname3 = "stb3"
        self.stbname4 = "stb4"
        self.stbname5 = "stb5"
        self.stbname6 = "stb6"
        self.childtable_prefix1 = "ctb1_"
        self.childtable_prefix2 = "ctb2_"
        self.childtable_prefix3 = "ctb3_"
        self.childtable_prefix4 = "ctb4_"
        self.childtable_prefix5 = "ctb5_"
        self.childtable_prefix6 = "ctb6_"
        self.batch_create_tbl_num = 1000
        self.stbname_list = [self.stbname1, self.stbname2, self.stbname3, self.stbname4, self.stbname5, self.stbname6]
        self.child_table_exists = "no"
        self.auto_create_table = "yes"
        self.db_drop = "no"
        self.wal_retention_period = 1800
        self.keep_trying = -1
        self.trying_interval = 10000
        self.interlace_rows = 0

        self.primary_key = 0

        self.pre_num_of_records_per_req = 10000
        self.json_file_name1 = "insert0.json"
        self.json_file_name2 = "insert1.json"
        self.json_file_name3 = "insert2.json"
        self.json_file_name4 = "insert3.json"
        self.json_file_name5 = "insert4.json"
        self.json_file_name6 = "insert5.json"
        self.json_data_list = list()
        self.taosBenchmark_iplist: List = self.get_fqdn("taosBenchmark")
        self.taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        self.dnode_id_list = list()
        self.vgid = 1
        self.column_info_list = [
            {
              "type": "BIGINT",
              "count": 1,
              "gen": "order",
              "fillNull": "false"
            },
            {
              "type": "BIGINT",
              "count": 2
            }
        ]
        self.tag_info_list = [
            {
              "type": "BIGINT",
              "count": 1
            }
        ]
        self.tag_field = "t0"
        self.col_field = "ts,c0,c1,c2"

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def get_dnode_id_list(self):
        """
        Retrieves a list of dnode IDs from the database.

        Returns:
            list: A list of dnode IDs.
        """
        self.tdSql.query('show dnodes')
        self.dnode_id_list = list(map(lambda x: x[0], self.tdSql.query_data))

    def check_restored_true(self):
        """
        Checks if the data is restored successfully.

        This method calls the 'check_restored_true' method from the 'tdCom' object
        and then displays the transactions.

        Args:
            self: The object instance.

        Returns:
            None
        """
        self.tdCom.check_restored_true(self._remote)
        self.show_transactions()

    def insert_data(self):
        """
        Insert data into the database using the provided parameters.

        This method sets up the necessary information for inserting data into the database.
        It creates JSON files, sets database information, sets super table information,
        sets host information, and sets JSON information.

        """
        self.json_filename_list = [self.json_file_name1, self.json_file_name2, self.json_file_name3, self.json_file_name4, self.json_file_name5, self.json_file_name6]
        dbinfo1 = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period)
        stb_info1 = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, name=self.stbname1, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, primary_key=self.primary_key, auto_create_table=self.auto_create_table, childtable_prefix=self.childtable_prefix1, batch_create_tbl_num=self.batch_create_tbl_num)]
        stb_info2 = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, name=self.stbname2, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, primary_key=self.primary_key, auto_create_table=self.auto_create_table, childtable_prefix=self.childtable_prefix2, batch_create_tbl_num=self.batch_create_tbl_num)]
        stb_info3 = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, name=self.stbname3, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, primary_key=self.primary_key, auto_create_table=self.auto_create_table, childtable_prefix=self.childtable_prefix3, batch_create_tbl_num=self.batch_create_tbl_num)]
        stb_info4 = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, name=self.stbname4, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, primary_key=self.primary_key, auto_create_table=self.auto_create_table, childtable_prefix=self.childtable_prefix4, batch_create_tbl_num=self.batch_create_tbl_num)]
        stb_info5 = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, name=self.stbname5, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, primary_key=self.primary_key, auto_create_table=self.auto_create_table, childtable_prefix=self.childtable_prefix5, batch_create_tbl_num=self.batch_create_tbl_num)]
        stb_info6 = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, name=self.stbname6, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, primary_key=self.primary_key, auto_create_table=self.auto_create_table, childtable_prefix=self.childtable_prefix6, batch_create_tbl_num=self.batch_create_tbl_num)]
        database_info1 = [self.tdCom.setDatabases(dbinfo=dbinfo1, super_tables=stb_info1)]
        database_info2 = [self.tdCom.setDatabases(dbinfo=dbinfo1, super_tables=stb_info2)]
        database_info3 = [self.tdCom.setDatabases(dbinfo=dbinfo1, super_tables=stb_info3)]
        database_info4 = [self.tdCom.setDatabases(dbinfo=dbinfo1, super_tables=stb_info4)]
        database_info5 = [self.tdCom.setDatabases(dbinfo=dbinfo1, super_tables=stb_info5)]
        database_info6 = [self.tdCom.setDatabases(dbinfo=dbinfo1, super_tables=stb_info6)]
        host = self.get_fqdn("taosd")[0]
        json_info1 = self.tdCom.setJsoninfo(host=host, databases=database_info1, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.pre_num_of_records_per_req)
        json_info2 = self.tdCom.setJsoninfo(host=host, databases=database_info2, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.pre_num_of_records_per_req)
        json_info3 = self.tdCom.setJsoninfo(host=host, databases=database_info3, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.pre_num_of_records_per_req)
        json_info4 = self.tdCom.setJsoninfo(host=host, databases=database_info4, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.pre_num_of_records_per_req)
        json_info5 = self.tdCom.setJsoninfo(host=host, databases=database_info5, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.pre_num_of_records_per_req)
        json_info6 = self.tdCom.setJsoninfo(host=host, databases=database_info6, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.pre_num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name1, json_info1)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name2, json_info2)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name3, json_info3)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name4, json_info4)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name5, json_info5)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name6, json_info6)
        self.json_data_list = [json_info1, json_info2, json_info3, json_info4, json_info5, json_info6]
        self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)
        self.tdSql.execute(f'flush database {self.dbname}')

    def reinsert(self):
        """
        Reinserts data into the database.

        This method performs the following steps:
        1. Sets up the necessary database information.
        2. Sets up the necessary super table information.
        3. Sets up the necessary JSON information.
        4. Generates a benchmark JSON file.
        5. Puts the JSON file on the remote server.
        6. Runs the TaosBenchmark tool with the specified configuration.
        7. Flushes the database.

        Note: Make sure to set the required instance variables before calling this method.

        Returns:
            None
        """
        self.json_filename_list = [self.json_file_name1]
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period)
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, name=self.stbname1, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, primary_key=self.primary_key, auto_create_table=self.auto_create_table, batch_create_tbl_num=self.batch_create_tbl_num)]
        database_info1 = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info1 = self.tdCom.setJsoninfo(host=host, databases=database_info1, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.pre_num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name1, json_info1)
        self.json_data_list = [json_info1]
        self.tdCom.put_file(self._remote, [self.taosBenchmark_iplist[0]], self.json_data_list, self.json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, [self.taosBenchmark_iplist[0]], self.json_data_list, self.json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)
        self.tdSql.execute(f'flush database {self.dbname}')

    def show_transactions(self):
        """
        Retrieves and logs the transactions using the 'show transactions' query.

        Returns:
            None
        """
        self.tdSql.query('show transactions')
        self._remote._logger.info(pd.DataFrame(self.tdSql.query_data))

    def restart_dnodes(self):
        """
        Restarts the dnodes.

        This method retrieves the dnode ID list, sorts the restart endpoint list,
        and updates the taosd configuration with the specified settings and restarts
        the dnodes.

        Args:
            None

        Returns:
            None
        """
        self.get_dnode_id_list()
        restart_endpoint_list = sorted(self.tdCom.get_fqdn_by_dnode_id(self.dnode_id_list))
        taosd_setting = copy.deepcopy(self.taosd_setting)
        self.taosd.update_cfg('/tmp', taosd_setting, {"supportVnodes": self.cfg["boundary"][-1]}, restart_endpoint_list[1], True)

    def deleteDnodeDataDir(self):
        """
        Deletes the data directory of a specific dnode.

        This method deletes the data directory of a specific dnode by executing the following steps:
        1. Retrieves the dnode information from the base_dnode_list.
        2. Extracts the dnode's fully qualified domain name (FQDN) from its endpoint.
        3. Constructs a command to kill any running processes related to the dnode.
        4. Executes the command to remove the dnode's data directory.
        5. Executes the command to kill any running processes related to the dnode.
        6. Waits for a specified amount of time.

        Note: This method assumes that the _remote object has been properly initialized.

        """
        dnode = self.base_dnode_list[1]
        dnode_fqdn = dnode["endpoint"].split(":")[0]
        kill_cmd = "ps -ef|grep -wi /root/jayden/config/dnode2 | grep -v grep | awk '{print $2}' | xargs kill -9 > /dev/null 2>&1"
        self._remote.cmd(dnode_fqdn, [f'rm -rf {dnode["config"]["dataDir"]}'])
        self._remote.cmd(dnode_fqdn, [kill_cmd])
        time.sleep(self.timewait)

    def auto_create_table_insert(self, args):
        """
        Inserts a row into a table using the specified arguments.

        Args:
            args (tuple): A tuple containing the following values:
                - ctbname (str): The name of the table to insert into.
                - stbname (str): The name of the source table.
                - tag_value (str): The tag value to use for the insert.

        Returns:
            None
        """
        ctbname, stbname, tag_value = args
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        c0, c1, c2 = random.randint(0, 100), random.randint(0, 100), random.randint(0, 100)
        self.tdSql.execute(f'insert into {self.dbname}.{ctbname} using {self.dbname}.{stbname} ({self.tag_field}) tags ({tag_value}) ({self.col_field}) values ("{ts}", {c0}, {c1}, {c2})')

    def thread_insert(self, ctb_prefix, stbname):
        """
        Perform multi-threaded table insertion.

        Args:
            ctb_prefix (str): The prefix for the child table name.
            stbname (str): The name of the parent table.

        Returns:
            None
        """
        self._remote._logger.info(f"thread_insert with {self.thread_count} threads")
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.thread_count) as executor:
            tasks = [(f"{ctb_prefix}{i}", stbname, i) for i in range(self.childtable_count)]
            executor.map(self.auto_create_table_insert, tasks)

    def run(self):
        """
        This method executes a series of steps to test bug regression for bug_ts5393.

        Steps:
        1. Creates mnodes on dnodes 2 and 3.
        2. Creates a database with specified parameters.
        3. Inserts data into the database.
        4. Deletes the data directory of a dnode.
        5. Restarts the dnodes.
        6. Queries and displays the list of dnodes.
        7. Waits for a specified time.
        8. Executes SQL statements on the database.
        9. Drops a table from the database.
        10. Creates a stable table with specified parameters.
        11. Reinserts data into the table.
        12. Executes a compact database command.
        13. Restores a dnode.
        14. Performs a count query on each table in the database.

        Returns:
        None
        """
        self.tdSql.execute("create mnode on dnode 2")
        self.tdSql.execute("create mnode on dnode 3")
        self.tdCom.createDb(dbname=self.dbname, replica=self.replica, vgroups=self.vgroups, wal_retention_period=self.wal_retention_period)
        self.insert_data()
        self.deleteDnodeDataDir()
        self.restart_dnodes()
        self.tdSql.query('show dnodes')
        print("--------", self.tdSql.query_data)
        time.sleep(self.timewait)
        self.tdSql.execute(f'use {self.dbname}')
        self.tdSql.execute(f'drop table {self.dbname}.{self.stbname1}')
        self.tdCom.create_stable(dbname=self.dbname, stbname=self.stbname1, column_elm_list=self.column_info_list, tag_elm_list=self.tag_info_list, default_column_index_start_num=0, default_tag_index_start_num=0)
        self.reinsert()
        # self.thread_insert(self.childtable_prefix1, self.stbname1)
        self.tdSql.execute(f'compact database {self.dbname}')
        self.tdSql.execute(f'restore dnode 2')
        for tbname in self.stbname_list:
            self.tdSql.query(f'select count(*) from {self.dbname}.{tbname}')
            self.tdSql.checkEqual(self.childtable_count * self.insert_rows, self.tdSql.query_data[0][0])