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
            tdLog.debug(f"create table stb_sxny_cn_{i} and insert data successfully")
                    
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
            tdLog.debug(f"create table stb_popo_power_station_all_{i} and insert data successfully")
        
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
            
    # run
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


        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-22 Alex Duan Migrated from uncatalog/army/query/test_query_basic.py

        """
        tdLog.debug(f"start to excute {__file__}")
        
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

        tdLog.success(f"{__file__} successfully executed")

