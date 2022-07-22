import taos
import sys
import datetime
import inspect
import random

from util.log import *
from util.sql import *
from util.cases import *

class TDTestCase:

    clientCfgDict = {'queryproxy': '1','debugFlag': 135}
    clientCfgDict["debugFlag"] = 131
    updatecfgDict = {'clientCfg': {}}
    updatecfgDict = {'debugFlag': 131}
    updatecfgDict = {'keepColumnName': 1}
    updatecfgDict["clientCfg"]  = clientCfgDict

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    def prepareData(self):
        database="db_tsbs"
        ts=1451606400000
        tdSql.execute(f"create database {database};")
        tdSql.execute(f"use {database} ")
        tdSql.execute('''
        create table readings (ts timestamp,latitude double,longitude double,elevation double,velocity double,heading double,grade double,fuel_consumption double,load_capacity double,fuel_capacity double,nominal_fuel_consumption double) tags (name binary(30),fleet binary(30),driver binary(30),model binary(30),device_version binary(30));
        ''')
        tdSql.execute('''
        create table diagnostics (ts timestamp,fuel_state double,current_load double,status bigint,load_capacity double,fuel_capacity double,nominal_fuel_consumption double) tags (name binary(30),fleet binary(30),driver binary(30),model binary(30),device_version binary(30)) ;
        ''')

        for i in range(10):
            if i == 1 or i == 2 :
                tdLog.debug(f"create table rct{i} using readings (name,fleet,driver,model,device_version) tags ('truck_{i}','South{i}','Trish{i}', NULL,'v2.3')")
                tdSql.execute(f"create table rct{i} using readings (name,fleet,driver,model,device_version) tags ('truck_{i}','South{i}','Trish{i}', NULL,'v2.3')")
            else :
                tdSql.execute(f"create table rct{i} using readings (name,fleet,driver,model,device_version) tags ('truck_{i}','South{i}','Trish{i}','H-{i}','v2.3')")
            if i == 1 or i == 2 :
                tdSql.execute(f"create table dct{i} using diagnostics (name,fleet,driver,model,device_version) tags ('truck_{i}','South{i}','Trish{i}',NULL ,'v2.3')")
            else:
                tdSql.execute(f"create table dct{i} using diagnostics (name,fleet,driver,model,device_version) tags ('truck_{i}','South{i}','Trish{i}','H-{i}','v2.3')")
        for j in range(10): 
            for i in range(100):
                tdSql.execute(
                    f"insert into rct{j} values ( {ts+i*60000}, {80+i}, {90+i}, {85+i}, {30+i*10}, {1.2*i}, {221+i*2}, {20+i*0.2}, {1500+i*20}, {150+i*2},{5+i} )"
                )
                status= random.randint(0,1)
                tdSql.execute(
                    f"insert into dct{j} values ( {ts+i*60000}, {1+i*0.1},{1400+i*15},  {status},{1500+i*20}, {150+i*2},{5+i} )"
                )
        tdSql.execute("insert into dct9 (ts,fuel_state) values('2021-07-13 14:06:33.123Z',1.2) ;")
    # def check_avg(self ,origin_query , check_query):
    #     avg_result = tdSql.getResult(origin_query)
    #     origin_result = tdSql.getResult(check_query)

    #     check_status = True
    #     for row_index , row in enumerate(avg_result):
    #         for col_index , elem in enumerate(row):
    #             if avg_result[row_index][col_index] != origin_result[row_index][col_index]:
    #                 check_status = False
    #     if not check_status:
    #         tdLog.notice("avg function value has not as expected , sql is \"%s\" "%origin_query )
    #         sys.exit(1)
    #     else:
    #         tdLog.info("avg value check pass , it work as expected ,sql is \"%s\"   "%check_query )


    def tsbsIotQuery(self):
        
        tdSql.execute("use db_tsbs")
        
        # test interval and partition
        tdSql.query(" SELECT avg(velocity) as mean_velocity ,name,driver,fleet FROM readings WHERE ts > 1451606400000 AND ts <= 1451606460000 partition BY name,driver,fleet; ")
        print(tdSql.queryResult)
        parRows=tdSql.queryRows
        tdSql.query(" SELECT avg(velocity) as mean_velocity ,name,driver,fleet FROM readings WHERE ts > 1451606400000 AND ts <= 1451606460000 partition BY name,driver,fleet interval(10m); ")
        tdSql.checkRows(parRows)
        
        
        # test insert into 
        tdSql.execute("create table testsnode (ts timestamp, c1 float,c2 binary(30),c3 binary(30),c4 binary(30)) ;")
        tdSql.query("insert into testsnode SELECT ts,avg(velocity) as mean_velocity,name,driver,fleet FROM readings WHERE ts > 1451606400000 AND ts <= 1451606460000 partition BY name,driver,fleet,ts interval(10m);")
        
        tdSql.query("insert into testsnode(ts,c1,c2,c3,c4)  SELECT ts,avg(velocity) as mean_velocity,name,driver,fleet FROM readings WHERE ts > 1451606400000 AND ts <= 1451606460000 partition BY name,driver,fleet,ts interval(10m);")


        # test paitition interval fill
        tdSql.query("SELECT name,floor(avg(velocity)/10)/floor(avg(velocity)/10) AS mv FROM readings   WHERE name!='' AND ts > '2016-01-01T00:00:00Z' AND ts < '2016-01-05T00:00:01Z'   partition by name interval(10m) fill(value,0) ;")


        # test partition interval limit  (PRcore-TD-17410)
        tdSql.query("select name,driver from (SELECT name,driver,fleet ,avg(velocity) as mean_velocity FROM readings partition BY name,driver,fleet interval (10m) limit 1);")
        tdSql.checkRows(10)

        # test partition interval Pseudo time-column
        tdSql.query("SELECT count(ms1)/144  FROM (SELECT _wstart as ts1,model, fleet,avg(status) AS ms1 FROM diagnostics WHERE ts >= '2016-01-01T00:00:00Z' AND ts < '2016-01-05T00:00:01Z'  partition by model, fleet interval(10m)) WHERE ts1 >= '2016-01-01T00:00:00Z' AND ts1 < '2016-01-05T00:00:01Z' AND ms1<1;")

        # 1 high-load:
        tdSql.query("SELECT ts,name,driver,current_load,load_capacity FROM (SELECT last(ts) as ts,name,driver, current_load,load_capacity FROM diagnostics  WHERE fleet = 'South'   partition by name,driver) WHERE current_load>= (0.9 * load_capacity) partition by name ORDER BY name desc, ts DESC;")

        tdSql.query("SELECT ts,name,driver,current_load,load_capacity FROM (SELECT last(ts) as ts,name,driver, current_load,load_capacity FROM diagnostics  WHERE fleet = 'South'   partition by name,driver) WHERE current_load>= (0.9 * load_capacity) partition by name ORDER BY name ;")

        # 2 stationary-trucks 
        tdSql.query("select name,driver from (SELECT name,driver,fleet ,avg(velocity) as mean_velocity FROM readings WHERE ts > '2016-01-01T15:07:21Z' AND ts <= '2016-01-01T16:17:21Z'    partition BY name,driver,fleet interval(10m) LIMIT 1)")
        tdSql.query("select name,driver from (SELECT name,driver,fleet ,avg(velocity) as mean_velocity FROM readings WHERE ts > '2016-01-01T15:07:21Z' AND ts <= '2016-01-01T16:17:21Z'    partition BY name,driver,fleet interval(10m) LIMIT 1) WHERE fleet = 'West' AND mean_velocity < 1000 partition BY name")

        # 3 long-driving-sessions
        tdSql.query("SELECT name,driver FROM(SELECT name,driver,count(*) AS ten_min FROM(SELECT _wstart as ts,name,driver,avg(velocity) as mean_velocity FROM readings  where ts > '2016-01-01T00:00:34Z' AND ts <= '2016-01-01T04:00:34Z'     partition BY name,driver interval(10m)) WHERE mean_velocity > 1 GROUP BY name,driver) WHERE ten_min > 22 ;")


        #4 long-daily-sessions
        tdSql.query("SELECT name,driver FROM(SELECT name,driver,count(*) AS ten_min FROM(SELECT name,driver,avg(velocity) as mean_velocity FROM readings WHERE fleet ='West' AND ts > '2016-01-01T12:31:37Z' AND ts <= '2016-01-05T12:31:37Z' partition BY name,driver  interval(10m) ) WHERE mean_velocity > 1 GROUP BY name,driver) WHERE ten_min > 60")

        # 5. avg-daily-driving-duration
        tdSql.query("select _wstart as ts,fleet,name,driver,count(mv)/6 as hours_driven from ( select _wstart as ts,fleet,name,driver,avg(velocity) as mv from readings where ts > '2016-01-01T00:00:00Z'     and ts < '2016-01-05T00:00:01Z'    partition by fleet,name,driver interval(10m)) where ts > '2016-01-01T00:00:00Z'  and ts < '2016-01-05T00:00:01Z'    partition by fleet,name,driver interval(1d) ;")


        # # 6. avg-daily-driving-session        
        # #taosc core dumped
        # tdSql.execute("create table random_measure2_1 (ts timestamp,ela float, name binary(40))")
        # tdSql.query("SELECT ts,diff(mv) AS difka  FROM (SELECT ts,name,floor(avg(velocity)/10)/floor(avg(velocity)/10) AS mv FROM readings   WHERE name!='' AND ts > '2016-01-01T00:00:00Z' AND ts < '2016-01-05T00:00:01Z'   partition by name,ts interval(10m) fill(value,0))  GROUP BY name,ts;")
        # tdSql.query("select name,diff(mv) AS difka  FROM (SELECT  ts,name,mv  FROM (SELECT _wstart as ts,name,floor(avg(velocity)/10)/floor(avg(velocity)/10) AS mv FROM readings   WHERE name!='' AND ts > '2016-01-01T00:00:00Z' AND ts < '2016-01-05T00:00:01Z'   partition by name interval(10m) fill(value,0)))  group  BY name ;")
        # tdSql.query("SELECT _wstart,name,floor(avg(velocity)/10)/floor(avg(velocity)/10) AS mv FROM readings   WHERE name!='' AND ts > '2016-01-01T00:00:00Z' AND ts < '2016-01-05T00:00:01Z'   partition by name interval(10m) fill(value,0)")

        # 7. avg-load
        tdSql.query("SELECT fleet, model,avg(ml) AS mean_load_percentage FROM (SELECT fleet, model,current_load/load_capacity AS ml FROM diagnostics partition BY name, fleet, model) partition BY fleet, model order by  fleet ;")

        # 8. daily-activity   
        tdSql.query(" SELECT model,ms1  FROM (SELECT _wstart as ts1,model, fleet,avg(status) AS ms1 FROM diagnostics WHERE ts >= '2016-01-01T00:00:00Z' AND ts < '2016-01-05T00:00:01Z'  partition by model, fleet interval(10m) fill(value,0)) WHERE ts1 >= '2016-01-01T00:00:00Z' AND ts1 < '2016-01-05T00:00:01Z'  AND ms1<1;")

        tdSql.query(" SELECT model,ms1  FROM (SELECT _wstart as ts1,model, fleet,avg(status) AS ms1 FROM diagnostics WHERE ts >= '2016-01-01T00:00:00Z' AND ts < '2016-01-05T00:00:01Z'  partition by model, fleet interval(10m) ) WHERE ts1 >= '2016-01-01T00:00:00Z' AND ts1 < '2016-01-05T00:00:01Z'  AND ms1<1;")

        tdSql.query("SELECT _wstart,model,fleet,count(ms1)/144  FROM (SELECT _wstart as ts1,model, fleet,avg(status) AS ms1 FROM diagnostics WHERE ts >= '2016-01-01T00:00:00Z' AND ts < '2016-01-05T00:00:01Z'  partition by model, fleet interval(10m) fill(value,0)) WHERE ts1 >= '2016-01-01T00:00:00Z' AND ts1 < '2016-01-05T00:00:01Z'  AND ms1<1 partition by model, fleet interval(1d) ;")

        # 9. breakdown-frequency
        # NULL ---count(NULL)=0 expect count(NULL)= 100
        tdSql.query("SELECT model,state_changed,count(state_changed)  FROM (SELECT model,diff(broken_down) AS state_changed   FROM (SELECT _wstart,model,cast(cast(floor(2*(sum(nzs)/count(nzs))) as bool) as int) AS broken_down FROM (SELECT ts,model, cast(cast(status as bool) as int) AS nzs FROM diagnostics WHERE  ts >= '2016-01-01T00:00:00Z' AND ts < '2016-01-05T00:00:01Z' ) WHERE ts >= '2016-01-01T00:00:00Z' AND ts < '2016-01-05T00:00:01Z'   partition BY model interval(10m)) partition BY model) where model is null  partition BY model,state_changed ")


        tdSql.query(" SELECT model,state_changed,count(state_changed)  FROM (SELECT model,diff(broken_down) AS state_changed   FROM (SELECT _wstart,model,cast(cast(floor(2*(sum(nzs)/count(nzs))) as bool) as int) AS broken_down FROM (SELECT ts,model, cast(cast(status as bool) as int) AS nzs FROM diagnostics WHERE  ts >= '2016-01-01T00:00:00Z' AND ts < '2016-01-05T00:00:01Z' ) WHERE ts >= '2016-01-01T00:00:00Z' AND ts < '2016-01-05T00:00:01Z'   partition BY model interval(10m)) partition BY model) where state_changed =1 partition BY model,state_changed ;")
        
        #it's already supported:
        # last-loc
        tdSql.query("SELECT last_row(ts),latitude,longitude,name,driver FROM readings WHERE fleet='South' and name IS NOT NULL partition BY name,driver order by name ;")


        #2. low-fuel
        tdSql.query("SELECT last_row(ts),name,driver,fuel_state,driver FROM diagnostics WHERE fuel_state <= 0.1 AND fleet = 'South' and name IS NOT NULL GROUP BY name,driver order by name;")
        
        # 3. avg-vs-projected-fuel-consumption
        tdSql.query("select avg(fuel_consumption) as avg_fuel_consumption,avg(nominal_fuel_consumption) as nominal_fuel_consumption from readings where velocity > 1 group by fleet")
        
    def run(self):  
        tdLog.printNoPrefix("==========step1:create database and table,insert data  ==============")
        self.prepareData()
        self.tsbsIotQuery()


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
