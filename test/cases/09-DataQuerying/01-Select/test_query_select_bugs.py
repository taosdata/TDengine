###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
from new_test_framework.utils import tdLog, tdSql, etool, tdCom
import time
import os
import datetime
import random

class TestQueryBugs:

    # fix
    def FIX_TD_30686(self):
        tdLog.info("check bug TD_30686 ...\n")
        sqls = [
            "create database db",
            "create table db.st(ts timestamp, age int) tags(area tinyint);",
            "insert into db.t1 using db.st tags(100) values('2024-01-01 10:00:01', 1);",
            "insert into db.t2 using db.st tags(110) values('2024-01-01 10:00:02', 2);",
            "insert into db.t3 using db.st tags(3) values('2024-01-01 10:00:03', 3);",
        ]
        tdSql.executes(sqls)

        sql = "select * from db.st where area < 139 order by ts;"
        results = [
            ["2024-01-01 10:00:01", 1, 100],
            ["2024-01-01 10:00:02", 2, 110],
            ["2024-01-01 10:00:03", 3, 3],
        ]
        tdSql.checkDataMem(sql, results)

    def FIX_TS_5105(self):
        tdLog.info("check bug TS_5105 ...\n")
        ts1 = "2024-07-03 10:00:00.000"
        ts2 = "2024-07-03 13:00:00.000"
        sqls = [
            "drop database if exists ts_5105",
            "create database ts_5105 cachemodel 'both';",
            "use ts_5105;",
            "CREATE STABLE meters (ts timestamp, current float) TAGS (location binary(64), groupId int);",
            "CREATE TABLE d1001 USING meters TAGS ('California.B', 2);",
            "CREATE TABLE d1002 USING meters TAGS ('California.S', 3);",
            f"INSERT INTO d1001 VALUES ('{ts1}', 10);",
            f"INSERT INTO d1002 VALUES ('{ts2}', 13);",
        ]
        tdSql.executes(sqls)

        sql = "select last(ts), last_row(ts) from meters;"

        # 执行多次，有些时候last_row(ts)会返回错误的值，详见TS-5105
        for i in range(1, 10):
            tdLog.debug(f"{i}th execute sql: {sql}")
            tdSql.query(sql)
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, ts2)
            tdSql.checkData(0, 1, ts2)

    def FIX_TS_5143(self):
        tdLog.info("check bug TS_5143 ...\n")
        # 2024-07-11 17:07:38
        base_ts = 1720688857000
        new_ts = base_ts + 10
        sqls = [
            "drop database if exists ts_5143",
            "create database ts_5143 cachemodel 'both';",
            "use ts_5143;",
            "create table stb1 (ts timestamp, vval varchar(50), ival2 int, ival3 int, ival4 int) tags (itag int);",
            "create table ntb1 using stb1 tags(1);",
            f"insert into ntb1 values({base_ts}, 'nihao1', 12, 13, 14);",
            f"insert into ntb1 values({base_ts + 2}, 'nihao2', NULL, NULL, NULL);",
            f"delete from ntb1 where ts = {base_ts};",
            f"insert into ntb1 values('{new_ts}', 'nihao3', 32, 33, 34);",
        ]
        tdSql.executes(sqls)

        last_sql = "select last(vval), last(ival2), last(ival3), last(ival4) from stb1;"
        tdLog.debug(f"execute sql: {last_sql}")
        tdSql.query(last_sql)

        for i in range(1, 10):
            new_ts = base_ts + i * 1000
            num = i * 100
            v1, v2 = i * 10, i * 11
            sqls = [
                f"insert into ntb1 values({new_ts}, 'nihao{num}', {v1}, {v1}, {v1});",
                f"insert into ntb1 values({new_ts + 1}, 'nihao{num + 1}', NULL, NULL, NULL);",
                f"delete from ntb1 where ts = {new_ts};",
                f"insert into ntb1 values({new_ts + 2}, 'nihao{num + 2}', {v2}, {v2}, {v2});",
            ]
            tdSql.executes(sqls)

            tdLog.debug(f"{i}th execute sql: {last_sql}")
            tdSql.query(last_sql)
            tdSql.checkData(0, 0, f"nihao{num + 2}")
            tdSql.checkData(0, 1, f"{11*i}")

    def FIX_TS_5239(self):
        tdLog.info("check bug TS_5239 ...\n")
        sqls = [
            "drop database if exists ts_5239",
            "create database ts_5239 cachemodel 'both' stt_trigger 1;",
            "use ts_5239;",
            "CREATE STABLE st (ts timestamp, c1 int) TAGS (groupId int);",
            "CREATE TABLE ct1 USING st TAGS (1);"
        ]
        tdSql.executes(sqls)
        # 2024-07-03 06:00:00.000
        start_ts = 1719957600000
        # insert 100 rows
        sql = "insert into ct1 values "
        for i in range(100):
            sql += f"('{start_ts+i * 100}', {i+1})"
        sql += ";"
        tdSql.execute(sql)
        tdSql.execute("flush database ts_5239;")
        tdSql.execute("alter database ts_5239 stt_trigger 3;")
        tdSql.execute(f"insert into ct1(ts) values({start_ts - 100 * 100})")
        tdSql.execute("flush database ts_5239;")
        tdSql.execute(f"insert into ct1(ts) values({start_ts + 100 * 200})")
        tdSql.execute("flush database ts_5239;")
        tdSql.query("select count(*) from ct1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 102)

    def FIX_TD_31684(self):
        tdLog.info("check bug TD_31684 ...\n")
        sqls = [
            "drop database if exists td_31684",
            "create database td_31684 cachemodel 'both' stt_trigger 1;",
            "use td_31684;",
            "create table t1 (ts timestamp, id int, name int) ;"
        ]
        tdSql.executes(sqls)
        sqls2 = [
            "insert into t1 values(now, 1, 1);",
            "insert into t1 values(now, 1, 2);",
            "insert into t1 values(now, 2, 3);",
            "insert into t1 values(now, 3, 4);"
        ]
        tdSql.executes(sqls2)

        sql1 = "select count(name) as total_name from t1 group by name"
        sql2 = "select id as new_id, last(name) as last_name from t1 group by id order by new_id"
        sql3 = "select id as new_id, count(id) as id from t1 group by id order by new_id"
        tdSql.query(sql1)
        tdSql.checkRows(4)

        tdSql.query(sql2)
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 1, 3)

        tdSql.query(sql3)
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(2, 1, 1)

    def ts5946(self):
        tdLog.info("check bug TD_xx ...\n")
        sqls = [
            "drop database if exists ctg_tsdb",
            "create database ctg_tsdb cachemodel 'both' stt_trigger 1;",
            "use ctg_tsdb;",
            "CREATE STABLE `stb_sxny_cn` (`dt` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', \
                `val` DOUBLE ENCODE 'delta-d' COMPRESS 'tsz' LEVEL 'medium') TAGS (`point` VARCHAR(50),  \
                `point_name` VARCHAR(64), `point_path` VARCHAR(2000), `index_name` VARCHAR(64),          \
                `country_equipment_code` VARCHAR(64), `index_code` VARCHAR(64), `ps_code` VARCHAR(50),   \
                `cnstationno` VARCHAR(255), `index_level` VARCHAR(10), `cz_flag` VARCHAR(255),           \
                `blq_flag` VARCHAR(255), `dcc_flag` VARCHAR(255))",
                
                
            "CREATE STABLE `stb_popo_power_station_all` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium',      \
                `assemble_capacity` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `ps_status` DOUBLE ENCODE         \
                'delta-d' COMPRESS 'lz4' LEVEL 'medium') TAGS (`ps_type` VARCHAR(255), `ps_type_code` VARCHAR(255),          \
                `belorg_name` VARCHAR(255), `belorg_code` VARCHAR(255), `country_code` VARCHAR(255), `country_name`          \
                VARCHAR(255), `area_name` VARCHAR(255), `area_code` VARCHAR(255), `ps_name` VARCHAR(255), `ps_code`          \
                VARCHAR(255), `ps_aab` VARCHAR(255), `ps_type_sec_lvl` VARCHAR(255), `ps_type_sec_lvl_name` VARCHAR(255),    \
                `ps_type_name` VARCHAR(255), `longitude` DOUBLE, `latitude` DOUBLE, `is_china` VARCHAR(255), `is_access`     \
                VARCHAR(255), `first_put_production_date` VARCHAR(255), `all_put_production_date` VARCHAR(255), `merge_date` \
                VARCHAR(255), `sold_date` VARCHAR(255), `cap_detail` VARCHAR(500), `ps_unit` VARCHAR(255), `region_name`     \
                VARCHAR(255))",
        ]
        
        
        tdSql.executes(sqls)
        
        ts = 1657146000000
        
        # create subtable and insert data for super table stb_sxny_cn
        for i in range(1, 1000):
            sql = f"CREATE TABLE `stb_sxny_cn_{i}` USING `stb_sxny_cn` (point, point_name, point_path, index_name, country_equipment_code,                      \
                index_code, ps_code, cnstationno, index_level, cz_flag, blq_flag, dcc_flag) TAGS('point{i}', 'point_name{i}', 'point_path{i}', 'index_name{i}', \
                'country_equipment_code{i}', 'index_code{i}', 'ps_code{i%500}', 'cnstationno{i}', '{i}', 'cz_flag{i}', 'blq_flag{i}', 'dcc_flag{i}');"
            tdSql.execute(sql)
            sql = f"INSERT INTO `stb_sxny_cn_{i}` VALUES "
            values = []
            for j in range(1, 100):
                values.append(f"({ts+(i%5)*86400000 + j}, {i%500 + j/20})")
            sql += ", ".join(values)
            tdSql.execute(sql)
    
                    
        # create subtable and insert data for super table stb_popo_power_station_all
        for i in range(1, 1000):
            sql = f"CREATE TABLE `stb_popo_power_station_all_{i}` USING `stb_popo_power_station_all` (ps_type, ps_type_code, belorg_name, belorg_code,          \
                country_code, country_name, area_name, area_code, ps_name, ps_code, ps_aab, ps_type_sec_lvl, ps_type_sec_lvl_name, ps_type_name,                \
                longitude, latitude, is_china, is_access, first_put_production_date, all_put_production_date, merge_date, sold_date, cap_detail, ps_unit,       \
                region_name) TAGS ('ps_type{i}', 'ps_type_code{i}', 'belorg_name{i}', 'belorg_code{i}', 'country_code{i}', 'country_name{i}', 'area_name{i}',   \
                'area_code{i}', 'ps_name{i}', 'ps_code{i}', 'ps_aab{i}', 'ps_type_sec_lvl{i}', 'ps_type_sec_lvl_name{i}', 'ps_type_name{i}', {i},               \
                {i}, 'is_china{i}', 'is_access{i}', 'first_put_production_date{i}', 'all_put_production_date{i}', 'merge_date{i}', 'sold_date{i}',              \
                'cap_detail{i}', 'ps_unit{i}', 'region_name{i}');"
            tdSql.execute(sql)
            sql = f"INSERT INTO `stb_popo_power_station_all_{i}` VALUES "
            values = []
            for j in range(1, 6):
                values.append(f"({ts+(j-1)*86400000}, {i*10 + j%10}, {j})")
            sql += ", ".join(values)
            tdSql.execute(sql)

        
        for i in range(1, 499, 20):
            pscode = f"ps_code{i}"
            
            querySql = f"select t2.ts ,tt.ps_code,t2.ps_code from   \
                    ( select TIMETRUNCATE(t1.dt, 1d, 1) dt,  t1.ps_code, first(dt)  \
                        from ctg_tsdb.stb_sxny_cn t1 where ps_code<>'0' and dt >= '2022-07-07 00:00:00.000' \
                        and t1.ps_code='{pscode}' partition by point state_window(cast(val as int)) order by \
                        TIMETRUNCATE(t1.dt, 1d, 0) ) tt \
                        left join ctg_tsdb.stb_popo_power_station_all t2   \
                        on TIMETRUNCATE(tt.dt, 1d, 1)=TIMETRUNCATE(t2.ts, 1d, 1)  \
                        and tt.ps_code = t2.ps_code "
            tdSql.query(querySql)
            tdSql.checkData(0, 1, pscode)
            tdSql.checkData(0, 2, pscode)
            tdLog.debug(f"execute sql: {pscode}")
            
            querySql = f"select t2.ts ,tt.ps_code,t2.ps_code from ( select last(t1.dt) dt,  t1.ps_code, first(dt)  \
                from ctg_tsdb.stb_sxny_cn t1 where ps_code<>'0' and dt >= '2022-07-07 00:00:00.000' and \
                t1.ps_code='{pscode}' group by tbname order by dt) tt left join \
                ctg_tsdb.stb_popo_power_station_all t2  on TIMETRUNCATE(tt.dt, 1d, 1)=TIMETRUNCATE(t2.ts, 1d, 1) \
                and tt.ps_code = t2.ps_code"
            tdSql.query(querySql)
            tdSql.checkData(0, 1, pscode)
            tdSql.checkData(0, 2, pscode)
            tdLog.debug(f"execute sql: {pscode}")
            
            querySql = f"select t2.ts ,tt.ps_code,t2.ps_code from ( select last(t1.dt) dt,  last(ps_code) ps_code \
                from ctg_tsdb.stb_sxny_cn t1 where ps_code<>'0' and dt >= '2022-07-07 00:00:00.000' and \
                t1.ps_code='{pscode}'  order by dt) tt left join ctg_tsdb.stb_popo_power_station_all t2  on \
                TIMETRUNCATE(tt.dt, 1d, 1)=TIMETRUNCATE(t2.ts, 1d, 1) and tt.ps_code = t2.ps_code"
            tdSql.query(querySql)
            tdSql.checkData(0, 1, pscode)
            tdSql.checkData(0, 2, pscode)
            tdLog.debug(f"execute sql: {pscode}")
            
            querySql = f"select t2.ts ,tt.ps_code,t2.ps_code from ( select _wstart dt,  t1.ps_code, first(dt)  \
                from ctg_tsdb.stb_sxny_cn t1 where ps_code<>'0' and dt >= '2022-07-07 00:00:00.000' and \
                t1.ps_code='{pscode}' interval(1m) order by dt) tt left join ctg_tsdb.stb_popo_power_station_all t2  \
                on TIMETRUNCATE(tt.dt, 1d, 1)=TIMETRUNCATE(t2.ts, 1d, 1) and tt.ps_code = t2.ps_code"
            tdSql.query(querySql)
            tdSql.checkData(0, 1, pscode)
            tdSql.checkData(0, 2, pscode)
            tdLog.debug(f"execute sql: {pscode}")

            querySql = f"select t2.ts ,tt.ps_code,t2.ps_code from (select first(dt) dt, t1.ps_code from \
                ctg_tsdb.stb_sxny_cn t1 where ps_code<>'0' and dt >= '2022-07-07 00:00:00.000' and t1.ps_code='{pscode}' \
                session(dt, 1m) order by dt) tt left join ctg_tsdb.stb_popo_power_station_all t2  on \
                TIMETRUNCATE(tt.dt, 1d, 1)=TIMETRUNCATE(t2.ts, 1d, 1) and tt.ps_code = t2.ps_code"
            tdSql.query(querySql)
            tdSql.checkData(0, 1, pscode)
            tdSql.checkData(0, 2, pscode)
            tdLog.debug(f"execute sql: {pscode}")

    def FIX_TS_5984(self):
        tdLog.info("check bug TS_5984 ...\n")
        # prepare data
        sqls = [
            "drop database if exists ts_5984;",
            "create database ts_5984 minrows 10;",
            "use ts_5984;",
            "create table t1 (ts timestamp, str varchar(10) primary key, c1 int);",
            """insert into t1 values
               ('2025-01-01 00:00:00', 'a', 1),
               ('2025-01-01 00:00:00', 'b', 2),
               ('2025-01-01 00:00:00', 'c', 3),
               ('2025-01-01 00:00:00', 'd', 4),
               ('2025-01-01 00:00:00', 'e', 5),
               ('2025-01-01 00:00:00', 'f', 6),
               ('2025-01-01 00:00:00', 'g', 7),
               ('2025-01-01 00:00:00', 'h', 8),
               ('2025-01-01 00:00:00', 'i', 9),
               ('2025-01-01 00:00:00', 'j', 10),
               ('2025-01-01 00:00:00', 'k', 11),
               ('2025-01-01 00:00:00', 'l', 12),
               ('2025-01-01 00:00:00', 'm', 13),
               ('2025-01-01 00:00:00', 'n', 14);"""
        ]
        tdSql.executes(sqls)
        # do flush and compact
        tdSql.execute("flush database ts_5984;")
        time.sleep(3)
        tdSql.execute("compact database ts_5984;")
        while True:
            tdSql.query("show compacts;")
            # break if no compact task
            if tdSql.getRows() == 0:
                break
            time.sleep(3)

        tdSql.query("select * from t1 where ts > '2025-01-01 00:00:00';")
        tdSql.checkRows(0)

    def FIX_TS_6058(self):
        tdSql.execute("create database iot_60j_production_eqp;")
        tdSql.execute("create table iot_60j_production_eqp.realtime_data_collections (device_time TIMESTAMP, item_value VARCHAR(64), \
            upload_time TIMESTAMP) tags(bu_id VARCHAR(64), district_id VARCHAR(64), factory_id VARCHAR(64), production_line_id VARCHAR(64), \
            production_processes_id VARCHAR(64), work_center_id VARCHAR(64), station_id VARCHAR(64), device_name VARCHAR(64), item_name VARCHAR(64));")
    
        sub1 = " SELECT '实际速度' as name, 0 as rank, '当月' as cycle,\
                 CASE \
                    WHEN COUNT(item_value) = 0 THEN NULL\
                    ELSE AVG(CAST(item_value AS double))\
                    END AS item_value\
                FROM iot_60j_production_eqp.realtime_data_collections\
                WHERE device_time >= TO_TIMESTAMP(CONCAT(substring(TO_CHAR(today  ,'YYYY-MM-dd'), 1,7), '-01 00:00:00'), 'YYYY-mm-dd')\
                AND item_name = 'Premixer_SpindleMotor_ActualSpeed' "
         
        sub2 = " SELECT  '实际速度' as name, 3 as rank, TO_CHAR(TODAY(),'YYYY-MM-dd') as cycle,\
                 CASE \
                    WHEN COUNT(item_value) = 0 THEN NULL\
                    ELSE AVG(CAST(item_value AS double))\
                    END AS item_value\
                FROM iot_60j_production_eqp.realtime_data_collections\
                WHERE device_time >= TODAY()-1d and device_time <= now()\
                AND item_name = 'Premixer_SpindleMotor_ActualSpeed' "
                  
        sub3 = " SELECT  '设定速度' as name, 1 as rank, CAST(CONCAT('WEEK-',CAST(WEEKOFYEAR(TODAY()-1w) as VARCHAR)) as VARCHAR) as cycle,\
                 CASE \
                    WHEN COUNT(item_value) = 0 THEN NULL\
                    ELSE AVG(CAST(item_value AS double))\
                    END AS item_value\
                FROM iot_60j_production_eqp.realtime_data_collections\
                where \
                item_name = 'Premixer_SpindleMotor_SettingSpeed'\
                      AND (\
                        (WEEKDAY(now) = 0 AND  device_time >= today()-8d and device_time <= today()-1d) OR\
                        (WEEKDAY(now) = 1 AND  device_time >= today()-9d and device_time <= today()-2d) OR\
                        (WEEKDAY(now) = 2 AND  device_time >= today()-10d and device_time <= today()-3d) OR\
                        (WEEKDAY(now) = 3 AND  device_time >= today()-11d and device_time <= today()-4d) OR\
                        (WEEKDAY(now) = 4 AND  device_time >= today()-12d and device_time <= today()-5d) OR\
                        (WEEKDAY(now) = 5 AND  device_time >= today()-13d and device_time <= today()-6d) OR\
                        (WEEKDAY(now) = 6 AND  device_time >= today()-14d and device_time <= today()-7d)\
                    ) "   
                    
        sub4 = " SELECT  '设定速度2' as name, 1 as rank, CAST(CONCAT('WEEK-',CAST(WEEKOFYEAR(TODAY()-1w) as VARCHAR)) as VARCHAR(5000)) as cycle,\
                 CASE \
                    WHEN COUNT(item_value) = 0 THEN NULL\
                    ELSE AVG(CAST(item_value AS double))\
                    END AS item_value\
                FROM iot_60j_production_eqp.realtime_data_collections\
                where \
                item_name = 'Premixer_SpindleMotor_SettingSpeed'\
                      AND (\
                        (WEEKDAY(now) = 0 AND  device_time >= today()-8d and device_time <= today()-1d) OR\
                        (WEEKDAY(now) = 1 AND  device_time >= today()-9d and device_time <= today()-2d) OR\
                        (WEEKDAY(now) = 2 AND  device_time >= today()-10d and device_time <= today()-3d) OR\
                        (WEEKDAY(now) = 3 AND  device_time >= today()-11d and device_time <= today()-4d) OR\
                        (WEEKDAY(now) = 4 AND  device_time >= today()-12d and device_time <= today()-5d) OR\
                        (WEEKDAY(now) = 5 AND  device_time >= today()-13d and device_time <= today()-6d) OR\
                        (WEEKDAY(now) = 6 AND  device_time >= today()-14d and device_time <= today()-7d)\
                    ) "      
        for uiontype in ["union" ,"union all"]:
            repeatLines = 1
            if uiontype == "union":
                repeatLines = 0
            for i in range(1, 10):
                tdLog.debug(f"test: realtime_data_collections {i} times...")
                tdSql.query(f"select name,cycle,item_value from ( {sub1} {uiontype} {sub2} {uiontype} {sub3}) order by rank,name,cycle;", queryTimes = 1)
                tdSql.checkRows(3)
                tdSql.query(f"select name,cycle,item_value from ( {sub1} {uiontype} {sub2} {uiontype} {sub4}) order by rank,name,cycle;", queryTimes = 1)
                tdSql.checkRows(3)
                tdSql.query(f"select name,cycle,item_value from ( {sub3} {uiontype} {sub2} {uiontype} {sub1}) order by rank,name,cycle;", queryTimes = 1)
                tdSql.checkRows(3)
                tdSql.query(f"select name,cycle,item_value from ( {sub3} {uiontype} {sub2} {uiontype} {sub1}) order by rank,name,cycle;", queryTimes = 1)
                tdSql.checkRows(3)
                tdSql.query(f"select name,cycle,item_value from ( {sub2} {uiontype} {sub4} {uiontype} {sub1}) order by rank,name,cycle;", queryTimes = 1)
                tdSql.checkRows(3)
                tdSql.query(f"select name,cycle,item_value from ( {sub3} {uiontype} {sub2} {uiontype} {sub1} {uiontype} {sub4}) order by rank,name,cycle;", queryTimes = 1)
                tdSql.checkRows(4)
                tdSql.query(f"select name,cycle,item_value from ( {sub2} {uiontype} {sub3} {uiontype} {sub1} {uiontype} {sub4}) order by rank,name,cycle;", queryTimes = 1)
                tdSql.checkRows(4)
                tdSql.query(f"select name,cycle,item_value from ( {sub3} {uiontype} {sub4} {uiontype} {sub1} {uiontype} {sub2}) order by rank,name,cycle;", queryTimes = 1)
                tdSql.checkRows(4)
                tdSql.query(f"select name,cycle,item_value from ( {sub3} {uiontype} {sub4} {uiontype} {sub1} {uiontype} {sub2}  {uiontype} {sub4}) order by rank,name,cycle;", queryTimes = 1)
                tdSql.checkRows(4 + repeatLines)
                tdSql.query(f"select name,cycle,item_value from ( {sub3} {uiontype} {sub2} {uiontype} {sub1} {uiontype} {sub2}  {uiontype} {sub4}) order by rank,name,cycle;", queryTimes = 1)
                tdSql.checkRows(4 + repeatLines)

    #
    # ------------------- 1 ----------------
    #
    def prepareData(self):
        # db
        tdSql.execute(f"drop database if exists db;")
        tdSql.execute(f"create database db;")
        tdSql.execute(f"use db")

        # create table and insert data

        tdSql.execute("CREATE STABLE st (ts timestamp, a int, b int, c int) TAGS (ta varchar(12))")
        tdSql.execute("insert into t1 using st tags('1') values('2025-03-06 11:17:29.202', 1, 1, 1) ('2025-03-07 11:17:30.361', 2, 2, 2)")
        tdSql.execute("insert into t2 using st tags('2') values('2025-03-06 11:17:29.202', 4, 4, 4) ('2025-03-07 11:17:30.361', 9, 9, 9)")


    def check(self):
        tdSql.query(f"select ts, count(*) from (select _wstart ts, timetruncate(ts, 1d, 1) dt, max(a)/10 from st partition by tbname interval(1s) order by dt) partition by ts order by ts")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, '2025-03-06 11:17:29')
        tdSql.checkData(1, 0, '2025-03-07 11:17:30')

        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 1, 2)

    def FIX_TS_5761(self):
        self.prepareData()
        self.check()

        print("do TS-5761 ............................ [passed]")

    #
    # ------------------- 2 ----------------
    #
    def FIX_TS_7058(self):
        # db
        tdSql.execute(f"drop database if exists db;")
        tdSql.execute(f"create database db;")
        tdSql.execute(f"use db")

        # create table and insert data

        tdSql.execute("CREATE STABLE st (ts timestamp, a int, b int, c int) TAGS (ta varchar(12))")
        tdSql.execute("insert into t1 using st tags('1') values('2025-03-06 11:17:29.202', 1, 1, 1) ('2025-03-07 11:17:30.361', 2, 2, 2)")
        tdSql.execute("insert into t2 using st tags('2') values('2025-03-06 11:17:29.202', 4, 4, 4) ('2025-03-07 11:17:30.361', 9, 9, 9)")

  
        tdSql.query(f"select ts, count(*) from (select _wstart ts, timetruncate(ts, 1d, 1) dt, max(a)/10 from st partition by tbname interval(1s) order by dt) partition by ts order by ts")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, '2025-03-06 11:17:29')
        tdSql.checkData(1, 0, '2025-03-07 11:17:30')

        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 1, 2)
        
        print("do TS-7058 ............................ [passed]")

    #
    # ------------------- 3 ----------------
    #
    def FIX_TS_5761_SCALEMODE(self):
        #
        # init data
        #
        
        # db
        tdSql.execute(f"drop database if exists db;")
        tdSql.execute(f"create database db;")
        tdSql.execute(f"use db")

        # super tableUNSIGNED
        tdSql.execute("CREATE TABLE st( time TIMESTAMP, c1 BIGINT, c2 smallint, c3 double, c4 int UNSIGNED, c5 bool, c6 binary(32), c7 nchar(32)) tags(t1 binary(32), t2 nchar(32))")
        tdSql.execute("create table t1 using st tags('1', '1.7')")
        tdSql.execute("create table t2 using st tags('0', '')")
        tdSql.execute("create table t3 using st tags('1', 'er')")

        # create index for all tags
        sql = "insert into "
        sql += " t1 VALUES (1641024000000, 1, 1, 1, 1, 1, '1', '1.7')"
        sql += " t1 VALUES (1641024000001, 0, 0, 1.7, 0, 0, '0', '')"
        sql += " t1 VALUES (1641024000002, 1, 1, 1, 1, 1, '1', 'er')"
        sql += " t2 VALUES (1641024000002, 1, 1, 1, 1, 1, '1', 'er')"
        sql += " t3 VALUES (1641024000002, 1, 1, 1, 1, 1, '1', 'er')"
        tdSql.execute(sql)

        tdSql.execute("CREATE TABLE stt( time TIMESTAMP, c1 BIGINT, c2 timestamp, c3 int, c4 int UNSIGNED, c5 bool, c6 binary(32), c7 nchar(32)) tags(t1 binary(32), t2 nchar(32))")
        tdSql.execute("create table tt1 using stt tags('1', '1.7')")

        # create index for all tags
        tdSql.execute("INSERT INTO tt1 VALUES (1641024000000, 9223372036854775807, 1641024000000, 1, 1, 1, '1', '1.7')")
        
        #
        # check
        #
        tdSql.query(f"SELECT * FROM tt1 WHERE c1 in (1.7, 9223372036854775803, '')")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM tt1 WHERE c1 = 9223372036854775803")
        tdSql.checkRows(0)

        tdSql.query(f"SELECT * FROM t1 WHERE c1 = 1.7")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM t1 WHERE c1 in (1.7, 2)")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM t1 WHERE c1 not in (1.7, 2)")
        tdSql.checkRows(3)

        tdSql.query(f"SELECT * FROM t1 WHERE c2 = 1.7")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM t1 WHERE c2 in (1.7, 2)")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM t1 WHERE c2 not in (1.7, 2)")
        tdSql.checkRows(3)

        tdSql.query(f"SELECT * FROM t1 WHERE c3 = 1.7")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM t1 WHERE c3 in (1.7, 2)")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM t1 WHERE c3 not in (1.7, 2)")
        tdSql.checkRows(2)

        tdSql.query(f"SELECT * FROM t1 WHERE c4 = 1.7")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM t1 WHERE c4 in (1.7, 2)")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM t1 WHERE c4 not in (1.7, 2)")
        tdSql.checkRows(3)

        tdSql.query(f"SELECT * FROM t1 WHERE c5 = 1.7")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM t1 WHERE c5 in (1.7, 2)")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM t1 WHERE c5 not in (1.7, 2)")
        tdSql.checkRows(3)

        tdSql.query(f"SELECT * FROM t1 WHERE c6 = 1.7")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM t1 WHERE c6 in (1.7, 2)")
        tdSql.checkRows(0)
        tdSql.query(f"SELECT * FROM t1 WHERE c6 not in (1.7, 2)")
        tdSql.checkRows(3)

        tdSql.query(f"SELECT * FROM t1 WHERE c6 = 1")
        tdSql.checkRows(2)
        tdSql.query(f"SELECT * FROM t1 WHERE c6 in (1, 2)")
        tdSql.checkRows(2)
        tdSql.query(f"SELECT * FROM t1 WHERE c6 not in (1, 2)")
        tdSql.checkRows(1)

        tdSql.query(f"SELECT * FROM t1 WHERE c6 = 0")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM t1 WHERE c6 in (0, 2)")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM t1 WHERE c6 not in (0, 2)")
        tdSql.checkRows(2)
        tdSql.query(f"SELECT * FROM t1 WHERE c6 not in (0, 2, 'sef')")
        tdSql.checkRows(2)

        tdSql.query(f"SELECT * FROM t1 WHERE c7 = 1.7")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM t1 WHERE c7 in (1.7, 2)")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM t1 WHERE c7 not in (1.7, 2)")
        tdSql.checkRows(2)

        tdSql.query(f"SELECT * FROM t1 WHERE c7 = 0")
        tdSql.checkRows(2)
        tdSql.query(f"SELECT * FROM t1 WHERE c7 in (0, 2)")
        tdSql.checkRows(2)
        tdSql.query(f"SELECT * FROM t1 WHERE c7 not in (0, 2)")
        tdSql.checkRows(1)

        tdSql.query(f"SELECT * FROM t1 WHERE c7 = ''")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM t1 WHERE c7 in ('', 2)")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM t1 WHERE c7 not in ('', 2)")
        tdSql.checkRows(2)

        tdSql.query(f"SELECT * FROM st WHERE t2 in ('', 2)")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM st WHERE t2 not in ('', 2)")
        tdSql.checkRows(4)

        tdSql.query(f"SELECT * FROM st WHERE t1 in ('d343', 0, 2)")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM st WHERE t1 in (0, 2)")
        tdSql.checkRows(1)
        tdSql.query(f"SELECT * FROM st WHERE t1 not in (0, 2)")
        tdSql.checkRows(4)
    
        print("do TS-5761 scalemode .................. [passed]")

    #
    # ------------------- 4 ----------------
    #
    def FIX_TS_5712(self):
        #
        # init data
        #
        
        # db
        tdSql.execute(f"drop database if exists db;")
        tdSql.execute(f"create database db;")
        tdSql.execute(f"use db")

        # super table
        tdSql.execute("CREATE TABLE t1( time TIMESTAMP, c1 BIGINT);")

        # create index for all tags
        tdSql.execute("INSERT INTO t1(time, c1) VALUES (1641024000000, 0)")
        tdSql.execute("INSERT INTO t1(time, c1) VALUES (1641024000001, 0)")
        
        #
        # check
        #
        tdSql.query(f"SELECT CAST(time AS BIGINT) FROM t1 WHERE (1 - time) > 0")
        tdSql.checkRows(0)

        print("do TS-5712 ............................ [passed]")

    #
    # ------------------- 5 ----------------
    #
    def FIX_TS_4348(self):
        tdSql.execute("create database ts_4338;")
        tdSql.execute("drop table if exists ts_4338.t;")
        tdSql.execute("create database if not exists ts_4338;")
        tdSql.execute("create table ts_4338.t (ts timestamp, i8 tinyint);")
        tdSql.execute("insert into ts_4338.t (ts, i8) values (now(), 1) (now()+1s, 2);")
        
        tdSql.query(f'select i8 from ts_4338.t;')
        tdSql.checkRows(2)

        tdSql.query(f'select i8 from ts_4338.t where 1 = 1;')
        tdSql.checkRows(2)

        tdSql.query(f'select i8 from ts_4338.t where i8 = 1;')
        tdSql.checkRows(1)

        tdSql.query(f'select * from (select * from ts_4338.t where i8 = 3);')
        tdSql.checkRows(0)

        # TD-27939
        tdSql.query(f'select * from (select * from ts_4338.t where 1 = 100);')
        tdSql.checkRows(0)

        tdSql.query(f'select * from (select * from (select * from ts_4338.t where 1 = 200));')
        tdSql.checkRows(0)

        tdSql.execute("drop database if exists ts_4338;")
    
        print("do TS-4348 ............................ [passed]")

    #
    # ------------------- 6 ----------------
    #
    def checksql(self, sql):
        result = os.popen(f"taos -s \"{sql}\" ")
        res = result.read()
        print(res)
        if ("Query OK" in res):
            tdLog.info(f"checkEqual success")
        else :
            tdLog.exit(f"checkEqual error")
    
    def FIX_TS_4233(self):        
        sql = "select 'a;b' as x"
        tdSql.query(f"%s" %sql)
        tdSql.checkRows(1)

        self.checksql('select \\\"a;b\\\" as x\\G')
        self.checksql('select \\\"a;b\\\" as x >> temp.txt')        
    
        print("do TS-4233 ............................ [passed]")

    #
    # ------------------- 7 ----------------
    #            
    def FIX_TS_3405(self):
        tdSql.execute("CREATE DATABASE IF NOT EXISTS statistics2 REPLICA {} DURATION 14400m KEEP 5256000m,5256000m,5256000m PRECISION 'ms' MINROWS 100 MAXROWS 4096 COMP 2;".format(self.replicaVar))
        tdSql.execute("use statistics2;")

        # create stable
        tdSql.execute("CREATE STABLE IF NOT EXISTS statistics2.`pg`(`day` timestamp,`lt_3` int,`c3_3` int,`c6_3` int,`c9_3` int,`c12_3` int,`c15_3` int,`c18_3` int,`c21_3` int,`c24_3` int,`c27_3` int,`ge_3` int) TAGS(`vin` binary(32));")
        tdSql.execute("CREATE STABLE IF NOT EXISTS statistics2.`b`(`day` timestamp, `month` int) TAGS(`group_path` binary(32),`vin` binary(32));")
        tdSql.execute("CREATE STABLE IF NOT EXISTS statistics2.`g`(`day` timestamp,`run_state` tinyint) TAGS(`vin` binary(32));")

        # insert the data to table
        times = 10
        insertRows = 3000
        pg_sql = "insert into d1001 using statistics2.`pg` tags('test') values"
        b_sql  = "insert into d2001 using statistics2.`b` tags('1#%', 'test') values"
        g_sql  = "insert into d3001 using statistics2.`g` tags('test') values"
        for t in range(times):
            for i in range(t * insertRows, t * insertRows + insertRows):
                ts = datetime.datetime.strptime('2023-05-01 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f') + datetime.timedelta(seconds=i)
                pg_sql += " ('{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})".format(ts, i, i, i+1, i+2, i+3, i+4, i+5, i+6, i+7, i+8, i+9)
                b_sql += " ('{}', {})".format(ts, 5)
                g_sql += " ('{}', {})".format(ts, 1)

            tdSql.execute(pg_sql)
            tdSql.execute(b_sql)
            tdSql.execute(g_sql)
            # reset the sql statements
            pg_sql = "insert into d1001 using statistics2.`pg` tags('test') values"
            b_sql  = "insert into d2001 using statistics2.`b` tags('1#%', 'test') values"
            g_sql  = "insert into d3001 using statistics2.`g` tags('test') values"
        tdLog.info("insert %d rows" % (insertRows * times))

        # execute the sql statements
        ret = tdSql.query("SELECT sum(pg.lt_3) es1,sum(pg.c3_3) es2,sum(pg.c6_3) es3,sum(pg.c9_3) es4,sum(pg.c12_3) es5,sum(pg.c15_3) es6,sum(pg.c18_3) es7,sum(pg.c21_3) es8,sum(pg.c24_3) es9,sum(pg.c27_3) es10,sum(pg.ge_3) es11 FROM statistics2.b b,statistics2.pg pg,statistics2.g g WHERE b.`day` = pg.`day` AND b.`day` = g.`day` AND b.vin = pg.vin AND b.vin = g.vin AND b.vin IS NOT NULL AND b.`group_path` LIKE '1#%';")
        # check the first query result
        if (449985000, 449985000, 450015000, 450045000, 450075000, 450105000, 450135000, 450165000, 450195000, 450225000, 450255000) in tdSql.queryResult:
            tdLog.info("first query result is correct")
        else:
            tdLog.info("first query result is wrong with res: {}".format(str(tdSql.queryResult)))

        ret = tdSql.query("SELECT sum(pg.lt_3) es1, sum(pg.c3_3) es2, sum(pg.c6_3) es3, sum(pg.c9_3) es4, sum(pg.c12_3) es5, sum(pg.c15_3) es6, sum(pg.c18_3) es7, sum(pg.c21_3) es8, sum(pg.c24_3) es9, sum(pg.c27_3) es10, sum(pg.ge_3) es11 FROM (select * from statistics2.b order by day,month) b, (select * from statistics2.pg order by day,lt_3 ) pg, (select * from statistics2.g order by day,run_state) g WHERE b.`day` = pg.`day` AND b.`day` = g.`day` AND b.vin = pg.vin AND b.vin = g.vin AND b.vin IS NOT NULL;")
        # check the second query result
        if (449985000, 449985000, 450015000, 450045000, 450075000, 450105000, 450135000, 450165000, 450195000, 450225000, 450255000) in tdSql.queryResult:
            tdLog.info("second query result is correct")
        else:
            tdLog.info("second query result is wrong with res: {}".format(str(tdSql.queryResult)))
        tdLog.info("Finish the test case for ts_3405 successfully")

        """This test case is used to verify the aliasName of Node structure is not truncated
        when sum clause is more than 65 bits.
        """
        # test case for https://jira.taosdata.com:18080/browse/TS-3398:
        # create db
        tdLog.info("Start the test case for ts_3398")
        tdSql.execute("CREATE DATABASE IF NOT EXISTS statistics1 REPLICA {} DURATION 14400m KEEP 5256000m,5256000m,5256000m PRECISION 'ms' MINROWS 100 MAXROWS 4096 COMP 2;".format(self.replicaVar))
        tdSql.execute("use statistics1;")

        # create stable
        tdSql.execute("CREATE STABLE IF NOT EXISTS statistics1.`g`(`day` timestamp,`run_state` tinyint) TAGS(`vin` binary(32));")
        tdSql.execute("CREATE STABLE IF NOT EXISTS statistics1.`b`(`day` timestamp, `total_heart` int) TAGS(`vin` binary(32));")
        tdSql.execute("CREATE STABLE IF NOT EXISTS statistics1.`tg`(`day` timestamp,`lt_4177` int,`f30_4177` int, `f35_4177` int) TAGS(`vin` binary(32));")

        # insert the data to table
        tdSql.execute("insert into d1001 using statistics1.`g` tags('NJHYNBSAS0000061') values (%s, %d)" % ("'2023-05-01'", 99))
        tdSql.execute("insert into d2001 using statistics1.`b` tags('NJHYNBSAS0000061') values (%s, %d)" % ("'2023-05-01'", 99))
        tdSql.execute("insert into d3001 using statistics1.`tg` tags('NJHYNBSAS0000061') values (%s, %d, %d, %d)" % ("'2023-05-01'", 99, 99, 99))

        # execute the sql statements
        tdSql.query("SELECT b.`day` `day`,sum(CASE WHEN tg.lt_4177 IS NULL THEN 0 ELSE tg.lt_4177 END \
            + CASE WHEN tg.f35_4177 IS NULL THEN 0 ELSE tg.f35_4177 END) / 3600 es0,sum(CASE WHEN tg.lt_4177 \
                IS NULL THEN 0 ELSE tg.lt_4177 END + CASE WHEN tg.f35_4177 IS NULL THEN 0 ELSE tg.f35_4177 \
                    END + CASE WHEN tg.f30_4177 IS NULL THEN 0 ELSE tg.f30_4177 END) / 3600 es1 FROM \
                        statistics1.b b,statistics1.tg tg,statistics1.g g WHERE b.`day` = tg.`day` AND g.`day` = b.`day` \
                            AND b.vin = tg.vin AND b.vin = g.vin AND b.`day` BETWEEN '2023-05-01' AND '2023-05-05' \
                                AND b.vin = 'NJHYNBSAS0000061' AND g.vin IS NOT NULL AND b.vin IS NOT NULL AND tg.vin IS NOT NULL \
                                    GROUP BY b.`day`;")
        # check the result
        if 0.055 in tdSql.queryResult[0] and 0.0825 in tdSql.queryResult[0]:
            tdLog.info("query result is correct")
        else:
            tdLog.info("query result is wrong")
        tdLog.info("Finish the test case for ts_3398 successfully")

        """This test case is used to verify last(*) query result is correct when the data
        is group by tag for stable
        """
        # test case for https://jira.taosdata.com:18080/browse/TS-3423:
        # create db
        tdLog.info("Start the test case for ts_3423")
        tdSql.execute("CREATE DATABASE IF NOT EXISTS ts_3423 REPLICA {} DURATION 14400m KEEP 5256000m,5256000m,5256000m PRECISION 'ms' MINROWS 100 MAXROWS 4096 COMP 2;".format(self.replicaVar))
        tdSql.execute("use ts_3423;")

        # create stable
        tdSql.execute("CREATE STABLE IF NOT EXISTS ts_3423.`st_last`(`ts` timestamp,`n1` int,`n2` float) TAGS(`groupname` binary(32));")

        # insert the data to table
        insertRows = 10
        child_table_num = 10
        sql = "insert into "
        for i in range(insertRows):
            ts = datetime.datetime.strptime('2023-05-01 00:00:00.000', '%Y-%m-%d %H:%M:%S.%f') + datetime.timedelta(seconds=i)
            for j in range(child_table_num):
                sql += " {} using ts_3423.`st_last` tags('{}') values ('{}', {}, {})".format("d" + str(j), "group" + str(j), str(ts), str(i+1), random.random())
        tdSql.execute(sql)        
        tdLog.info("insert %d rows for every child table" % (insertRows))

        # cache model list
        cache_model = ["none", "last_row", "last_value", "both"]
        query_res = []
        
        # execute the sql statements first
        tdSql.query("select `cachemodel` from information_schema.ins_databases where name='ts_3423'")
        current_cache_model = tdSql.queryResult[0][0]
        tdLog.info("query on cache model {}".format(current_cache_model))
        tdSql.query("select last(*) from st_last group by groupname;")
        # save the results
        query_res.append(len(tdSql.queryResult))
        # remove the current cache model
        cache_model.remove(current_cache_model)
        
        for item in cache_model:
            tdSql.execute("alter database ts_3423 cachemodel '{}';".format(item))
            # execute the sql statements
            tdSql.query("select last(*) from st_last group by groupname;")
            tdLog.info("query on cache model {}".format(item))
            query_res.append(len(tdSql.queryResult))
        # check the result
        res = True if query_res.count(child_table_num) == 4 else False
        if res:
            tdLog.info("query result is correct and same among different cache model")
        else:
            tdLog.info("query result is wrong")
        tdLog.info("Finish the test case for ts_3423 successfully")

        # clear the db
        tdSql.execute("drop database if exists statistics1;")
        tdSql.execute("drop database if exists statistics2;")
        tdSql.execute("drop database if exists ts_3423;")        
         
        print("do TS-3405 ............................ [passed]")

    #
    # ------------------- 8 ----------------
    #            
    def FIX_TD_32548(self): 
        tdSql.execute("create database td_32548 cachemodel 'last_row' keep 3650,3650,3650;")
        
        tdSql.execute("use td_32548;")

        tdSql.execute("create table ntb1 (ts timestamp, ival int);")
        tdSql.execute("insert into ntb1 values ('2024-07-08 17:54:49.675', 54);")

        tdSql.execute("flush database td_32548;")

        tdSql.execute("insert into ntb1 values ('2024-07-08 17:53:49.675', 53);")
        tdSql.execute("insert into ntb1 values ('2024-07-08 17:52:49.675', 52);")
        tdSql.execute("delete from ntb1 where ts = '2024-07-08 17:54:49.675';")

        tdSql.query('select last_row(ts) from ntb1;')
        tdSql.checkData(0, 0, '2024-07-08 17:53:49.675')
         
        print("do TD-32548 ............................ [passed]")

    #
    # ------------------- 9 ----------------
    #            
    def FIX_TD_28068(self):
        tdSql.execute("drop database if exists td_28068;")
        tdSql.execute("create database td_28068;")
        tdSql.execute("create database if not exists td_28068;")
        tdSql.execute("create stable td_28068.st (ts timestamp, test_case nchar(10), time_cost float, num float) tags (branch nchar(10), scenario nchar(10));")
        sql = "insert into "
        sql += " td_28068.ct1 using td_28068.st (branch, scenario) tags ('3.0', 'scenario1') values (1717122943000, 'query1', 1,2)"
        sql += " td_28068.ct1 using td_28068.st (branch, scenario) tags ('3.0', 'scenario1') values (1717122944000, 'query1', 2,3)"
        sql += " td_28068.ct2 using td_28068.st (branch, scenario) tags ('3.0', 'scenario2') values (1717122945000, 'query1', 10,1)"
        sql += " td_28068.ct2 using td_28068.st (branch, scenario) tags ('3.0', 'scenario2') values (1717122946000, 'query1', 11,5)"
        sql += " td_28068.ct3 using td_28068.st (branch, scenario) tags ('3.1', 'scenario1') values (1717122947000, 'query1', 20,4)"
        sql += " td_28068.ct3 using td_28068.st (branch, scenario) tags ('3.1', 'scenario1') values (1717122948000, 'query1', 30,1)"
        sql += " td_28068.ct4 using td_28068.st (branch, scenario) tags ('3.1', 'scenario2') values (1717122949000, 'query1', 8,8)"
        sql += " td_28068.ct4 using td_28068.st (branch, scenario) tags ('3.1', 'scenario2') values (1717122950000, 'query1', 9,10)"
        tdSql.execute(sql)

        tdSql.query('select last(ts) as ts, last(branch) as branch, last(scenario) as scenario, last(test_case) as test_case  from td_28068.st group by st.branch, st.scenario order by last(branch);')
        tdSql.checkRows(4)
        tdSql.query('select last(ts) as ts, last(branch) as branch1, last(scenario) as scenario, last(test_case) as test_case  from td_28068.st group by st.branch, st.scenario order by last(branch), last(scenario); ')
        tdSql.checkRows(4)
        tdSql.query('select last(ts) as ts, last(branch) as branch1, last(scenario) as scenario, last(test_case) as test_case  from td_28068.st group by st.branch, st.scenario order by last(branch); ')
        tdSql.checkRows(4)

        tdSql.query('select last(ts) as ts, last(branch) as branch1, last(scenario) as scenario, last(test_case)  from td_28068.st group by st.branch, st.scenario order by last(branch), last(test_case);')
        tdSql.checkRows(4)

        tdSql.query('select last(ts) as ts, last(branch) as branch1, last(scenario) as scenario1, last(test_case) as test_case  from td_28068.st group by st.branch, st.scenario order by last(branch), last(scenario);')
        tdSql.checkRows(4)

        tdSql.query('select last(ts) as ts, last(branch) as branch1, last(scenario) as scenario1, last(test_case) as test_case  from td_28068.st group by st.branch, st.scenario order by branch1, scenario1;')
        tdSql.checkRows(4)

        tdSql.query('select last(ts) as ts, last(branch) as branch1, last(scenario) as scenario1, last(test_case) as test_case  from td_28068.st group by tbname; ')
        tdSql.checkRows(4)

        tdSql.query('select last(ts) as ts, last(branch) as branch1, last(scenario) as scenario1, last(test_case) as test_case  from td_28068.st group by st.branch, st.scenario order by test_case;')
        tdSql.checkRows(4)

        tdSql.query('select last(ts) as ts, last(branch) as branch1, last(scenario) as scenario1, last(test_case) as test_case1  from td_28068.st group by st.branch, st.scenario order by last(test_case);')
        tdSql.checkRows(4)

        tdSql.query('select time_cost, num, time_cost + num as final_cost  from td_28068.st partition by st.branch; ')
        tdSql.checkRows(8)

        tdSql.query('select count(*) from td_28068.st partition by branch order by branch; ')
        tdSql.checkRows(2)

        tdSql.query('select time_cost, num, time_cost + num as final_cost from td_28068.st order by time_cost;')
        tdSql.checkRows(8)

        tdSql.query('select time_cost, num, time_cost + num as final_cost from td_28068.st order by final_cost;')
        tdSql.checkRows(8)

        tdSql.execute("drop database if exists td_28068;")
         
        print("do TD-28068 ............................ [passed]")

    #
    # ------------------- main ----------------
    #        
    def test_query_bugs(self):
        """Select bugs
        
        1. Verify jira TS-5946
        2. Verify jira TD-30686
        3. Verify jira TS-5105
        4. Verify jira TS-5143
        5. Verify jira TS-5239
        6. Verify jira TD-31684
        7. Verify jira TS-5984
        8. Verify jira TS-6058
        9. Verify jira TS-5761
        10. Verify jira TS-7058
        11. Verify jira TS-5761 scalemode
        12. Verify jira TS-5712
        13. Verify jira TS-4348
        14. Verify jira TS-4233
        15. Verify jira TS-3405
        16. Verify jira TD-32548
        17. Verify jira TD-28068

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-22 Alex Duan Migrated from uncatalog/army/query/test_query_basic.py
            - 2025-12-14 Alex Duan Migrated from cases/uncatalog/system-test/2-query/test_ts_5761.py
            - 2025-12-15 Alex Duan Migrated from cases/uncatalog/system-test/2-query/test_ts_7058.py
            - 2025-12-15 Alex Duan Migrated from cases/uncatalog/system-test/2-query/test_ts_5761_scalemode.py
            - 2025-12-15 Alex Duan Migrated from cases/uncatalog/system-test/2-query/test_ts_5712.py
            - 2025-12-15 Alex Duan Migrated from cases/uncatalog/system-test/2-query/test_ts_4348_td_27939.py
            - 2025-12-15 Alex Duan Migrated from cases/uncatalog/system-test/2-query/test_ts_4233.py
            - 2025-12-15 Alex Duan Migrated from cases/uncatalog/system-test/2-query/test_ts_3405_3398_3423.py
            - 2025-12-15 Alex Duan Migrated from cases/uncatalog/system-test/2-query/test_td_32548.py
            - 2025-12-15 Alex Duan Migrated from cases/uncatalog/system-test/2-query/test_td_28068.py

        """        
        self.ts5946()
        # TD BUGS
        self.FIX_TD_30686()
        self.FIX_TD_31684()

        # TS BUGS
        self.FIX_TS_5105()
        self.FIX_TS_5143()
        self.FIX_TS_5239()
        self.FIX_TS_5984()
        self.FIX_TS_6058()
        self.FIX_TS_5761()

