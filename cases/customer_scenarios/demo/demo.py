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
import datetime
from taostest import TDCase
from taostest.performance.result_reduction import Perf_Base_func
from taostest.util.remote import Remote
import random
from taostest.util.playwright_util import PlaywrightUtil
from taostest.util.jmeter_util import JMeterUtil


class Demo(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.perf = Perf_Base_func(self._remote._logger, self.run_log_dir)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.prom_env_setting = self.get_component_by_name("prometheus")
        # self.Prometheus = PrometheusServer(self._remote)
        self.json_filename = "insert0.json"
        self.replica = int(os.environ["DATABASE_REPLICAS"]) if "DATABASE_REPLICAS" in os.environ else 1
        self.start_timestamp = self.tdCom.genTodayZeroTs()
        self.create_table_thread_count = 40
        self.childtable_count = 10000
        self.insert_rows = 1000
        self.default_interval = 5
        self.range_count = 10
        self.precision = "ms"
        self.pk_test = False
        self.pk_dict_list = [{"pname": "pk", "ptype": "bigint"}, {"pname": "pk", "ptype": "int"}]
        self.pk_dict = random.choice(self.pk_dict_list) if self.pk_test else None
        self.stt_trigger = 8
        self.stbname = "stb"
        self.ctbname = "ctb"
        self.dbname = "test"
        self.child_table_exists = "no"
        self.db_drop = "yes"
        self.keep_trying = -1
        self.trying_interval = 10
        self.vgroups = 10
        self.host = self.get_fqdn("taosd")[0]
        self.thread_count = 40
        self.num_of_records_per_req = 1000
        self.interlace_rows = 0
        self.full_type_list = ["tinyint", "smallint", "int", "bigint", "tinyint unsigned", "smallint unsigned", "int unsigned", "bigint unsigned", "float", "double", "binary", "nchar", "bool"]
        self.offset = 1000
        self.date_time = self.tdCom.genTs(precision=self.precision)[0]
        self.date_time = int(datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()*self.offset)
        self.env_root = os.path.join(os.environ["TEST_ROOT"], "env")
        self.json_file = os.path.join(self.env_root, "pocs/gyrx/test.json")
        self.run_test_log_dir = "/root/testlog/"
        self.playwright = PlaywrightUtil(self.envMgr)

        # Initialize JMeter if configured
        self.jmeter_setting = None
        self.jmeter_util = None
        try:
            self.jmeter_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "jmeter")
            if self.jmeter_setting:
                self.jmeter_util = JMeterUtil(self.envMgr)
                self.logger.info("JMeter component found and initialized")
        except Exception as e:
            self.logger.info(f"JMeter component not configured or failed to initialize: {e}")
            self.jmeter_setting = None
            self.jmeter_util = None

        self.column_info_list = [
            {
              "type": "BIGINT",
              "count": 1,
              "gen": "order",
              "fillNull": "false"
            },
            {
              "type": "INT",
              "count": 2
            }
        ]
        self.tag_info_list = [
            {
              "type": "INT",
              "count": 1
            }
        ]
        self.json_file_name = "insert0.json"
        self.json_data_list = list()
        self.taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        self.taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        self.env_root = os.path.join(os.environ["TEST_ROOT"], "env")
        self.json_file = os.path.join(self.env_root, "pocs/gyrx/test.json")


    def insert_with_python_connector(self):
        # Using pre-packaged functions
        self.tdCom.createDb(dbname=self.dbname)
        self.tdCom.create_stable(dbname=self.dbname, stbname=self.stbname, pk_dict=self.pk_dict)
        self.tdCom.create_ctable(dbname=self.dbname, stbname=self.stbname, ctbname=self.ctbname)
        for i in range(self.range_count):
            ts_value = str(self.date_time)+f'-{self.default_interval*(i+1)}s'
            self.tdCom.insert_rows(tbname=self.ctbname, ts_value=ts_value, pk_dict=self.pk_dict)
        # Custom Write
        self.tdSql.execute(f'insert into {self.dbname}.{self.ctbname} (ts, c1) values (now, 1)')

    def query(self, sql, expected_count, expected_res=None):
        self.tdSql.query(sql)
        self.tdSql.checkEqual(self.tdSql.query_row, expected_count)
        if expected_res is not None:
            self.tdSql.checkEqual(self.tdSql.query_data[0][0], expected_res)

    def insert_with_taosBenchmark(self):
        json_filename_list = [self.json_file_name]
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, stt_trigger=self.stt_trigger)
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, name=self.stbname, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name, json_info)
        self.json_data_list = [json_info]
        self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, json_filename_list, self.run_log_dir)
        self.result_filename = self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)
        self.tdSql.execute(f'flush database {self.dbname}')

    def insert_with_load_json(self):
        json_filename_list = [self.json_file_name]
        json_info = self.tdCom.load_json(self.json_file)
        json_info["test_log"] = self.run_test_log_dir
        self.tdCom.dump_json(f'{self.run_log_dir}/{self.json_file_name}', json_info)
        self.json_data_list = [json_info]
        self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, json_filename_list, self.run_log_dir)
        self.result_filename = self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)
        self.tdSql.execute(f'flush database {self.dbname}')

    def jmeter_demo_test(self):
        """Run JMeter demo test if JMeter is configured"""
        if not self.jmeter_util or not self.jmeter_setting:
            self.logger.info("JMeter not configured, skipping JMeter demo test")
            return None

        try:
            self.logger.info("=== Starting JMeter Demo Test ===")

            # Setup JMeter environment
            if not self.jmeter_util.setup_jmeter(self.jmeter_setting):
                self.logger.error("Failed to setup JMeter environment")
                return None

            # Get configuration
            server_config = self.jmeter_setting.get("spec", {}).get("server", {})
            sql_file = server_config.get("sql_file")
            jmx_template = server_config.get("jmx_template")

            if not sql_file or not jmx_template:
                self.logger.error("SQL file or JMX template not specified in JMeter configuration")
                return None

            # Copy JMX template from env/jmeter to current run directory
            template_jmx_path = os.path.join(os.environ['TEST_ROOT'], f"env/jmeter/{jmx_template}")
            work_jmx_path = os.path.join(self.run_log_dir, jmx_template)

            if not os.path.exists(template_jmx_path):
                self.logger.error(f"JMX template not found: {template_jmx_path}")
                return None

            # Copy template to work directory
            import shutil
            shutil.copy2(template_jmx_path, work_jmx_path)
            self.logger.info(f"Copied JMX template from {template_jmx_path} to {work_jmx_path}")

            sql_file_path = os.path.join(os.environ['TEST_ROOT'], f"env/demo/{sql_file}")

            # Create JMeter results directory
            jmeter_results_dir = os.path.join(self.run_log_dir, "jmeter_demo_results")
            os.makedirs(jmeter_results_dir, exist_ok=True)

            # Modify JMeter setting to use work directory JMX
            import copy
            modified_jmeter_setting = copy.deepcopy(self.jmeter_setting)
            modified_jmeter_setting["spec"]["server"]["jmx_template_path"] = work_jmx_path

            # Temporarily replace the util's setting
            original_setting = self.jmeter_util._jmeter_setting
            self.jmeter_util._jmeter_setting = modified_jmeter_setting

            try:
                # Run JMeter test using utility
                results = self.jmeter_util.run_multi_concurrency_test(
                    sql_file_path=sql_file_path,
                    results_dir=jmeter_results_dir,
                    test_root=os.environ['TEST_ROOT']
                )
            finally:
                # Restore original setting
                self.jmeter_util._jmeter_setting = original_setting

            if results:
                self.logger.info(f"JMeter demo test completed with {len(results)} test runs")

                # Copy demo files to run directory for preservation
                demo_files_dir = os.path.join(self.run_log_dir, "demo_files")
                os.makedirs(demo_files_dir, exist_ok=True)

                # Copy SQL and JMX files
                if os.path.exists(sql_file_path):
                    shutil.copy2(sql_file_path, demo_files_dir)
                if os.path.exists(work_jmx_path):
                    shutil.copy2(work_jmx_path, demo_files_dir)

                self.logger.info(f"Demo files saved to: {demo_files_dir}")
                return jmeter_results_dir
            else:
                self.logger.warning("JMeter demo test failed to produce results")
                return None

        except Exception as e:
            self.logger.error(f"JMeter demo test failed: {e}")
            return None
        finally:
            if self.jmeter_util:
                self.jmeter_util.cleanup()


    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        if hasattr(self, 'jmeter_util') and self.jmeter_util:
            self.jmeter_util.cleanup()

    def run(self):
        # Insert
        self.insert_with_python_connector()

        # Query
        self.query(f'select * from {self.dbname}.{self.stbname}', self.range_count+1)
        self.query(f'select last(c1) from {self.dbname}.{self.stbname}', 1, 1)

        # taosBenchmark insert
        result_file_name = self.run_log_dir + '/perf_report.txt'
        timestamp_start = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        # taosBenchmark insert with loaded_json
        self.insert_with_load_json()
        # # taosBenchmark insert with custom parameters in case
        # self.insert_with_taosBenchmark()
        timestamp_end = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        self.perf.taosBenchmark_insert_summary_result(self.result_filename, version="3.0")
        self.perf.get_process_exporter_info(self.prom_env_setting, 1, timestamp_start, timestamp_end)
        self.perf.get_node_exporter_info(self.prom_env_setting, 1, timestamp_start, timestamp_end)

        print(result_file_name)

        # Playwright screenshot test
        self._remote._logger.info("Starting Playwright screenshot test...")

        # 1. Test screenshot of url
        self._remote._logger.info("Testing URL screenshot...")
        self.playwright.take_screenshot(
            target="https://www.baidu.com",
            output_file="baidu_homepage.png"
        )

        # 2. Test screenshot of text file
        self._remote._logger.info("Testing text file screenshot...")
        self.playwright.take_screenshot_text(
            text_file=result_file_name,
            output_file=f"{os.path.basename(result_file_name)}.png",
            title="Playwright Demo Test Results"
        )

        self._remote._logger.info("Collecting screenshots...")
        # JMeter demo test
        jmeter_results_dir = self.jmeter_demo_test()

        # Take screenshots of JMeter results if available
        if jmeter_results_dir and os.path.exists(jmeter_results_dir):
            self._remote._logger.info("Taking screenshots of JMeter results...")

            # Screenshot test_summary.txt - search in subdirectories
            summary_file = None
            for root, _, files in os.walk(jmeter_results_dir):
                if "test_summary.txt" in files:
                    summary_file = os.path.join(root, "test_summary.txt")
                    break
            
            if summary_file and os.path.exists(summary_file):
                self._remote._logger.info(f"Found test_summary.txt at: {summary_file}")
                self.playwright.take_screenshot_text(
                    text_file=summary_file,
                    output_file="jmeter_test_summary.png",
                    title="JMeter Demo Test Summary"
                )
            else:
                self._remote._logger.warning("test_summary.txt not found in JMeter results")

            # Screenshot HTML reports
            for root, _, files in os.walk(jmeter_results_dir):
                for file in files:
                    if file == "index.html":
                        html_file = os.path.join(root, file)
                        self.playwright.take_screenshot(
                            target=html_file,
                            output_file=f"jmeter_html_report_{os.path.basename(root)}.png"
                        )

            self._remote._logger.info("JMeter result screenshots completed")

        self.playwright.collect_screenshots(self.run_log_dir)
        self.playwright.reset()
        self._remote._logger.info("Playwright screenshot test completed successfully!")