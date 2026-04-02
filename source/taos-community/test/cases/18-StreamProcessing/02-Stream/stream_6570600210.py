import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, streamUtil,StreamTableType, StreamTable, cluster
from random import randint
import os
import subprocess

class TestSnodeMgmt:
    
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_snode_mgmt(self):
        """Stream bug feishu-6570600210
        
        1. Check stream td37724 

        Catalog:
            - Streams:create stream

        Since: v3.3.8.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-10 mark wang Created

        """


        self.prepareData()
        self.check()

    def prepareData(self):
        tdLog.info(f"prepare data")

        sqls = [
            "create database proj_wsha_sn;",
            "create snode on dnode 1;",
            "use proj_wsha_sn;",
            "create stable temp_fto_purification_hyd_ele_heat_power (time timestamp, val double) tags (fto_id varchar(128),fto_name varchar(128), pur_id varchar(128), pur_name varchar(128), oxy_id varchar(128), oxy_name varchar(128));",
            "CREATE STABLE `glb_identify_purification_hot_total_power` (`time` TIMESTAMP, `val` DOUBLE) TAGS (`fto_id` VARCHAR(128), `fto_name` VARCHAR(128), `pur_id` VARCHAR(128), `pur_name` VARCHAR(128));",
            "CREATE STABLE `glb_identify_purification_oxy_ele_heat_power` (`time` TIMESTAMP, `val` DOUBLE) TAGS (`fto_id` VARCHAR(128), `fto_name` VARCHAR(128), `pur_id` VARCHAR(128), `pur_name` VARCHAR(128), `oxy_id` VARCHAR(128), `oxy_name` VARCHAR(128));",
            "create stream proj_wsha_sn.st_b_glb_identify_purification_hot_total_power count_window(1) from  proj_wsha_sn.glb_identify_purification_oxy_ele_heat_power partition by tbname, fto_id,fto_name stream_options (low_latency_calc) into proj_wsha_sn.glb_identify_purification_hot_total_power output_subtable (concat('giphtp_pur', substr(fto_id,1,1))) tags (fto_id varchar(128) as fto_id, fto_name varchar(128) as fto_name,pur_id varchar(128) as fto_id,pur_name varchar(128) as CONCAT(fto_id, '纯化')) as select _twstart as time,o.val+h.val as val from proj_wsha_sn.temp_fto_purification_hyd_ele_heat_power h inner join proj_wsha_sn.glb_identify_purification_oxy_ele_heat_power o on h.fto_id = o.fto_id and h.time = o.time where h.time = _twstart and h.fto_id = %%2;",
            "CREATE TABLE `d1` USING `glb_identify_purification_oxy_ele_heat_power` (`fto_id`, `fto_name`, `pur_id`, `pur_name`, `oxy_id`, `oxy_name`) TAGS ('11', '11', '11', '11', '11', '11');",
            "CREATE TABLE `st1` USING `temp_fto_purification_hyd_ele_heat_power` (`fto_id`, `fto_name`, `pur_id`, `pur_name`, `oxy_id`, `oxy_name`) TAGS ('11', '11', '11', '11', '11', '11');",
        ]

        tdSql.executes(sqls)

        cnt = 0
        while True:
            tdSql.query(    
                f"select status from information_schema.ins_streams"
            )
            streamRunning = tdSql.getColData(0)
            if streamRunning[0] == 'Running':
                break
            if cnt >= 100:
                tdLog.error(f"stream not running after waiting for 100s")
            cnt += 1
            time.sleep(1)
            
        
        sqls = [
            "insert into d1 values('2023-05-01 12:13:14', 1.2323);",
            "insert into d1 values('2023-05-01 12:13:16', 1.343);",
            "insert into d1 values('2023-05-01 12:13:18', 1.3243);",
            "insert into st1 values('2023-05-01 12:13:14', 1.434345);",
            "insert into st1 values('2023-05-01 12:13:16', 1.6565);",
            "insert into st1 values('2023-05-01 12:13:18', 1.45466);",
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create and insert successfully.")
    
    
    def check(self):
        tdLog.info(f"check results")
        cnt = 0
        while True:
            tdSql.query(    
                f"select * from proj_wsha_sn.glb_identify_purification_hot_total_power"
            )
            rows = tdSql.getRows()
            if rows == 3:
                break
            if cnt >= 100:
                tdLog.error(f"check failed. expected rows: 3, actual rows: {rows}")
            cnt += 1
            time.sleep(1)
        tdLog.info(f"check successfully.")