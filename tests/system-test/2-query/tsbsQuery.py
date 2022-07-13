import taos
import sys
import datetime
import inspect

from util.log import *
from util.sql import *
from util.cases import *

class TDTestCase:

    clientCfgDict = {'queryproxy': '1','debugFlag': 135}
    clientCfgDict["debugFlag"] = 131
    updatecfgDict = {'clientCfg': {}}
    updatecfgDict = {'debugFlag': 131}
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
            tdSql.execute(f"create table rct{i} using readings (name,fleet,driver,model,device_version) tags ('truck_{i}','South{i}','Trish{i}','H-{i}','v2.3')")
            tdSql.execute(f"create table dct{i} using diagnostics (name,fleet,driver,model,device_version) tags ('truck_{i}','South{i}','Trish{i}','H-{i}','v2.3')")
        for j in range(10):
            for i in range(10):
                tdSql.execute(
                    f"insert into rct{j} values ( {ts+i*10000}, {80+i}, {90+i}, {85+i}, {30+i*10}, {1.2*i}, {221+i*2}, {20+i*0.2}, {1500+i*20}, {150+i*2},{5+i} )"
                )
                tdSql.execute(
                    f"insert into dct{j} values ( {ts+i*10000}, {1+i*0.1},{1400+i*15}, {1+i},{1500+i*20}, {150+i*2},{5+i} )"
                )

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
        parRows=tdSql.queryRows
        tdSql.query(" SELECT avg(velocity) as mean_velocity ,name,driver,fleet FROM readings WHERE ts > 1451606400000 AND ts <= 1451606460000 partition BY name,driver,fleet interval(10m); ")
        # tdSql.checkRows(parRows)
        
        
        # test insert into 
        tdSql.execute("create table testsnode (ts timestamp, c1 float,c2 binary(30),c3 binary(30),c4 binary(30)) ;")
        tdSql.query("insert into testsnode SELECT ts,avg(velocity) as mean_velocity,name,driver,fleet FROM readings WHERE ts > 1451606400000 AND ts <= 1451606460000 partition BY name,driver,fleet,ts interval(10m);")
        
        tdSql.query("insert into testsnode(ts,c1,c2,c3,c4)  SELECT ts,avg(velocity) as mean_velocity,name,driver,fleet FROM readings WHERE ts > 1451606400000 AND ts <= 1451606460000 partition BY name,driver,fleet,ts interval(10m);")


       # test interval fill
        tdSql.query("SELECT name,floor(avg(velocity)/10)/floor(avg(velocity)/10) AS mv FROM readings   WHERE name!='' AND ts > '2016-01-01T00:00:00Z' AND ts < '2016-01-05T00:00:01Z'   partition by name interval(10m) fill(value,0) ;")

    def run(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdLog.printNoPrefix("==========step1:create database and table,insert data  ==============")
        self.prepareData()
        self.tsbsIotQuery()


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
