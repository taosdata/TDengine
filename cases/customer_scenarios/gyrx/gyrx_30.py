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
import time
from playwright.sync_api import Playwright, sync_playwright


class Demo(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.perf = Perf_Base_func(self.logger, self.run_log_dir)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.prom_env_setting = self.get_component_by_name("prometheus")
        self.explorer_setting = self.get_component_by_name("taosExplorer")
        
        # 从 explorer_setting 中提取配置信息
        if self.explorer_setting:
            # explorer_setting 是一个列表，取第一个元素
            explorer_item = self.explorer_setting[0] if isinstance(self.explorer_setting, list) else self.explorer_setting
            self.explorer_config = {
                'fqdn': explorer_item.get('fqdn', [self.get_fqdn("taosd")[0]])[0],
                'port': explorer_item.get('spec', {}).get('config', {}).get('port', 6060)
            }
        else:
            # 如果没有 taosExplorer 配置，使用默认值
            self.explorer_config = {
                'fqdn': self.get_fqdn("taosd")[0],
                'port': 6060
            }
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
        self.json_log ={}

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

    def execute_sql(self,sql):
        self.tdSql.execute(sql)
        self.json_log[sql] = '执行成功'


    def query_sql(self,sql,expected_count=None,expected_res=None):
        self.tdSql.query(sql)
        self.json_log[sql] = self.tdSql.query_data


    def config_audit(self):
        
        self.execute_sql(f'drop database if exists {self.dbname}')
        

        self.execute_sql("alter all dnodes 'audit 1'")
        self.execute_sql(f'create database {self.dbname}')
  



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
        self.json_data_list = [json_info]
        self.result_filename = self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)
        self.tdSql.execute(f'flush database {self.dbname}')


    def login_and_screenshot(self, host, port):
        """打开taos-explorer网页，登录并截图"""
        try:
            with sync_playwright() as p:
                # 启动浏览器，设置更大的窗口尺寸
                browser = p.chromium.launch(
                    headless=True,
                    args=[
                        '--window-size=1920,1080',
                        '--disable-web-security',
                        '--disable-features=VizDisplayCompositor'
                    ]
                )
                context = browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                )
                page = context.new_page()
                
                # 访问 taos-explorer 网页
                url = f"http://{host}:{port}"
                print(f"正在访问: {url}")
                page.goto(url, timeout=30000)
                
                # 等待页面加载
                time.sleep(3)
                
                # 查找并填写用户名
                try:
                    username_input = page.locator('input[name="username"], input[placeholder*="用户名"], input[placeholder*="username"], input[type="text"]').first
                    username_input.fill("root")
                    print("已输入用户名: root")
                except Exception:
                    print("警告: 无法找到用户名输入框")
                
                # 查找并填写密码
                try:
                    password_input = page.locator('input[name="password"], input[placeholder*="密码"], input[placeholder*="password"], input[type="password"]').first
                    password_input.fill("taosdata")
                    print("已输入密码: taosdata")
                except Exception:
                    print("警告: 无法找到密码输入框")
                
                # 查找并点击登录按钮
                try:
                    login_button = page.locator('button:has-text("登录"), button:has-text("Login"), button:has-text("Sign In"), button.signin, button.el-button--primary:has-text("登录"), button.el-button--primary:has-text("Sign In"), input[type="submit"], button[type="submit"]').first
                    login_button.click()
                    print("已点击登录按钮")
                    
                    # 等待登录完成
                    time.sleep(5)
                except Exception:
                    print("警告: 无法找到登录按钮")
                
                # 登录后截图
                screenshot_path = os.path.join(self.run_log_dir, f"taos_explorer_login_{int(time.time())}.png")
                page.screenshot(path=screenshot_path, full_page=True)
                print(f"登录后截图已保存: {screenshot_path}")
                
                # 点击管理菜单
                try:
                    # 查找管理菜单项
                    management_menu = page.locator('#menu_7, div.menu-item-wrap[id="menu_7"], li.el-menu-item[aria-data="/management"], div[aria-data="/management"]').first
                    management_menu.click()
                    print("已点击管理菜单")
                    
                    # 等待页面跳转
                    time.sleep(3)
                except Exception as e:
                    print(f"警告: 无法找到或点击管理菜单: {str(e)}")
                
                # 点击审计标签页
                try:
                    # 查找审计标签页
                    audit_tab = page.locator('#tab-audit, div.el-tabs__item[id="tab-audit"], div:has-text("审计").el-tabs__item, .el-tabs__item:has-text("审计")').first
                    audit_tab.click()
                    print("已点击审计标签页")
                    
                    # 等待内容加载
                    time.sleep(3)
                except Exception as e:
                    print(f"警告: 无法找到或点击审计标签页: {str(e)}")
                
                # 最终截图（审计页面）
                final_screenshot_path = os.path.join(self.run_log_dir, f"taos_explorer_audit_{int(time.time())}.png")
                page.screenshot(path=final_screenshot_path, full_page=True)
                print(f"审计页面截图已保存: {final_screenshot_path}")
                
                # 记录到日志
                self.json_log["taos_explorer_login"] = f"成功访问并登录 {url}，登录截图: {screenshot_path}"
                self.json_log["taos_explorer_audit"] = f"成功导航到审计页面，截图: {final_screenshot_path}"
                
                browser.close()
            
        except Exception as e:
            print(f"网页登录截图失败: {str(e)}")
            # 记录错误但不抛出异常，避免影响主流程
            self.json_log["taos_explorer_login_error"] = str(e)

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def run(self):
        # 启动taos-explorer
        # self.start_taos_explorer()

        # config audit

        self.config_audit()
        self.login_and_screenshot(self.explorer_config['fqdn'], self.explorer_config['port'])
        # # Query
        # self.query_sql('select * from audit.operations')
        # print(self.json_log)


        # taosBenchmark insert
        result_file_name = self.run_log_dir + '/perf_report.txt'
        # timestamp_start = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        
        # timestamp_end = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        
        # self.perf.get_process_exporter_info(self.prom_env_setting, 1, timestamp_start, timestamp_end)
        # self.perf.get_node_exporter_info(self.prom_env_setting, 1, timestamp_start, timestamp_end)

        print(result_file_name)
