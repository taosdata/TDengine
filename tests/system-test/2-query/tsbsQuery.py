# from asyncio.windows_events import NULL
import taos
import sys
import datetime
import inspect
import random
from util.dnodes import TDDnode
from util.dnodes import tdDnodes

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

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    def create_ctable(self,tsql=None, dbName='db',stbName='stb',ctbPrefix='ctb',ctbNum=1):
            tsql.execute("use %s" %dbName)
            pre_create = "create table"
            sql = pre_create
            #tdLog.debug("doing create one  stable %s and %d  child table in %s  ..." %(stbname, count ,dbname))
            for i in range(ctbNum):
                tagValue = 'beijing'
                if (i % 10 == 0):
                    sql += f" {dbName}.%s%d using %s (name,fleet,driver,device_version,load_capacity,fuel_capacity,nominal_fuel_consumption)  tags('truck_%d', 'South%d','Trish%d','v2.%d', 1500+%d*20, 150+%d*2, 5+%d)"%(ctbPrefix,i,stbName,i,i,i,i,(1500+i*20),(150+i*2),(5+i))
                else:
                    model = 'H-%d'%i
                    sql += f" {dbName}.%s%d using %s tags('truck_%d', 'South%d','Trish%d','%s','v2.%d', %d, %d,%d)"%(ctbPrefix,i,stbName,i,i,i,model,i,(1500+i*20),(150+i*2),(5+i))
                if (i > 0) and (i%1000 == 0):
                    tsql.execute(sql)
                    sql = pre_create
            if sql != pre_create:
                tsql.execute(sql)

            tdLog.debug("complete to create %d child tables in %s.%s" %(ctbNum, dbName, stbName))
            return

    def insertData(self,startTs,tsql=None, dbName='db',stbName='stb',ctbPrefix='ctb',ctbNum=1,rowsPerTbl=100,batchNum=1000):
            tsql.execute("use %s" %dbName)
            pre_insert = "insert into "
            sql = pre_insert
            if startTs is None:
                t = time.time()
                startTs = int(round(t * 1000))

            for i in range(ctbNum):
                sql += f" {dbName}.%s%d values "%(ctbPrefix,i)
                for j in range(rowsPerTbl):
                    if(ctbPrefix=="rct"):
                        sql += f"({startTs+j*60000}, {80+j}, {90+j}, {85+j}, {30+j*10}, {1.2*j}, {221+j*2}, {20+j*0.2}) "
                    elif ( ctbPrefix=="dct"):
                        status= random.randint(0,1)
                        sql += f"( {startTs+j*60000}, {1+j*0.1},{1400+j*15},  {status} ) "
                    # tdLog.debug("1insert sql:%s"%sql)
                    if (j > 0) and ((j%batchNum == 0) or (j == rowsPerTbl - 1)):
                        # tdLog.debug("2insert sql:%s"%sql)
                        tsql.execute(sql)
                        if j < rowsPerTbl - 1:
                            sql = f"insert into {dbName}.%s%d values " %(ctbPrefix,i)
                        else:
                            sql = "insert into "
            if sql != pre_insert:
                # tdLog.debug("3insert sql:%s"%sql)
                tsql.execute(sql)
            tdLog.debug("insert data ............ [OK]")
            return

    def prepareData(self):
        dbname="db_tsbs"
        stabname1=f"{dbname}.readings"
        stabname2=f"{dbname}.diagnostics"
        ctbnamePre1="rct"
        ctbnamePre2="dct"
        ctbNums=50
        self.ctbNums=ctbNums
        rowNUms=5000
        ts=1451606400000
        tdSql.execute(f"create database {dbname} cachemodel 'both';")
        tdSql.execute(f"use {dbname} ")
        tdSql.execute(f'''
        create table {stabname1} (ts timestamp,latitude double,longitude double,elevation double,velocity double,heading double,grade double,fuel_consumption double) tags (name binary(30),fleet binary(30),driver binary(30),model binary(30),device_version binary(30),load_capacity double,fuel_capacity double,nominal_fuel_consumption double);
        ''')
        tdSql.execute(f'''
        create table {stabname2} (ts timestamp,fuel_state double,current_load double,status bigint) tags (name binary(30),fleet binary(30),driver binary(30),model binary(30),device_version binary(30),load_capacity double,fuel_capacity double,nominal_fuel_consumption double) ;
        ''')
        self.create_ctable(tsql=tdSql,dbName=dbname,stbName=stabname1,ctbPrefix=ctbnamePre1,ctbNum=ctbNums)
        self.create_ctable(tsql=tdSql,dbName=dbname,stbName=stabname2,ctbPrefix=ctbnamePre2,ctbNum=ctbNums)
        self.insertData(tsql=tdSql,dbName=dbname,stbName=stabname1,ctbPrefix=ctbnamePre1,ctbNum=ctbNums,rowsPerTbl=rowNUms,startTs=ts,batchNum=10000)
        self.insertData(tsql=tdSql,dbName=dbname,stbName=stabname2,ctbPrefix=ctbnamePre2,ctbNum=ctbNums,rowsPerTbl=rowNUms,startTs=ts,batchNum=10000)
        # for i in range(ctbNum):
        #     if i %10 == 0 :
        #         # tdLog.debug(f"create table rct{i} using readings (name,fleet,driver,model,device_version) tags ('truck_{i}','South{i}','Trish{i}', NULL,'v2.3')")
        #         tdSql.execute(f"create table rct{i} using readings (name,fleet,driver,model,device_version) tags ('truck_{i}','South{i}','Trish{i}', NULL,'v2.3')")
        #     else :
        #         tdSql.execute(f"create table rct{i} using readings (name,fleet,driver,model,device_version) tags ('truck_{i}','South{i}','Trish{i}','H-{i}','v2.3')")
        #     if i %10 == 0 :
        #         tdSql.execute(f"create table dct{i} using diagnostics (name,fleet,driver,model,device_version) tags ('truck_{i}','South{i}','Trish{i}',NULL ,'v2.3')")
        #     else:
        #         tdSql.execute(f"create table dct{i} using diagnostics (name,fleet,driver,model,device_version) tags ('truck_{i}','South{i}','Trish{i}','H-{i}','v2.3')")
        # for j in range(ctbNums):
        #     for i in range(rowNUms):
        #         tdSql.execute(
        #             f"insert into rct{j} values ( {ts+i*60000}, {80+i}, {90+i}, {85+i}, {30+i*10}, {1.2*i}, {221+i*2}, {20+i*0.2}, {1500+i*20}, {150+i*2},{5+i} )"
        #         )
        #         status= random.randint(0,1)
        #         tdSql.execute(
        #             f"insert into dct{j} values ( {ts+i*60000}, {1+i*0.1},{1400+i*15},  {status},{1500+i*20}, {150+i*2},{5+i} )"
        #         )
        # tdSql.execute("insert into dct9 (ts,fuel_state) values('2021-07-13 14:06:33.123Z',1.2) ;")
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


    def tsbsIotQuery(self,insertinto=True, dbname="db_tsbs"):

        tdSql.execute("use db_tsbs")

        # test interval and partition
        tdSql.query(f"select avg(velocity) as mean_velocity ,name,driver,fleet from {dbname}.readings WHERE ts > 1451606400000 AND ts <= 1451606460000 partition BY name,driver,fleet; ")
        parRows=tdSql.queryRows
        tdSql.query(f"select avg(velocity) as mean_velocity ,name,driver,fleet from {dbname}.readings WHERE ts > 1451606400000 AND ts <= 1451606460000 partition BY name,driver,fleet interval(10m); ")
        tdSql.checkRows(parRows)


        # test insert into
        if insertinto == True :
            tdSql.execute(f"create table {dbname}.testsnode (ts timestamp, c1 float,c2 binary(30),c3 binary(30),c4 binary(30)) ;")
            tdSql.query(f"insert into {dbname}.testsnode SELECT ts,avg(velocity) as mean_velocity,name,driver,fleet from {dbname}.readings WHERE ts > 1451606400000 AND ts <= 1451606460000 partition BY name,driver,fleet,ts interval(10m);")

            tdSql.query(f"insert into {dbname}.testsnode(ts,c1,c2,c3,c4)  SELECT ts,avg(velocity) as mean_velocity,name,driver,fleet from {dbname}.readings WHERE ts > 1451606400000 AND ts <= 1451606460000 partition BY name,driver,fleet,ts interval(10m);")


        # test paitition interval fill
        tdSql.query(f"select name,floor(avg(velocity)/10)/floor(avg(velocity)/10) AS mv from {dbname}.readings   WHERE name!='' AND ts > '2016-01-01T00:00:00Z' AND ts < '2016-01-05T00:00:01Z'   partition by name interval(10m) fill(value,0) ;")


        # test partition interval limit  (PRcore-TD-17410)
        tdSql.query(f"select name,driver from (SELECT name,driver,fleet ,avg(velocity) as mean_velocity from {dbname}.readings partition BY name,driver,fleet interval (10m) limit 1);")
        tdSql.checkRows(self.ctbNums)

        # test partition interval Pseudo time-column
        tdSql.query(f"select count(ms1)/144  FROM (SELECT _wstart as ts1,model, fleet,avg(status) AS ms1 from {dbname}.diagnostics WHERE ts >= '2016-01-01T00:00:00Z' AND ts < '2016-01-05T00:00:01Z'  partition by model, fleet interval(10m)) WHERE ts1 >= '2016-01-01T00:00:00Z' AND ts1 < '2016-01-05T00:00:01Z' AND ms1<1;")

        # 1 high-load:
        tdSql.query(f"select ts,name,driver,current_load,load_capacity FROM (SELECT last(ts) as ts,name,driver, current_load,load_capacity from {dbname}.diagnostics  WHERE fleet = 'South'   partition by name,driver) WHERE current_load>= (0.9 * load_capacity) partition by name ORDER BY name desc, ts DESC;")

        tdSql.query(f"select ts,name,driver,current_load,load_capacity FROM (SELECT last(ts) as ts,name,driver, current_load,load_capacity from {dbname}.diagnostics  WHERE fleet = 'South'   partition by name,driver) WHERE current_load>= (0.9 * load_capacity) partition by name ORDER BY name ;")

        # 2 stationary-trucks
        tdSql.query(f"select name,driver from (SELECT name,driver,fleet ,avg(velocity) as mean_velocity from {dbname}.readings WHERE ts > '2016-01-01T15:07:21Z' AND ts <= '2016-01-01T16:17:21Z'    partition BY name,driver,fleet interval(10m) LIMIT 1)")
        tdSql.query(f"select name,driver from (SELECT name,driver,fleet ,avg(velocity) as mean_velocity from {dbname}.readings WHERE ts > '2016-01-01T15:07:21Z' AND ts <= '2016-01-01T16:17:21Z'    partition BY name,driver,fleet interval(10m) LIMIT 1) WHERE fleet = 'West' AND mean_velocity < 1000 partition BY name")

        # 3 long-driving-sessions
        tdSql.query(f"select name,driver FROM(SELECT name,driver,count(*) AS ten_min FROM(SELECT _wstart as ts,name,driver,avg(velocity) as mean_velocity from {dbname}.readings  where ts > '2016-01-01T00:00:34Z' AND ts <= '2016-01-01T04:00:34Z'     partition BY name,driver interval(10m)) WHERE mean_velocity > 1 GROUP BY name,driver) WHERE ten_min > 22 ;")


        #4 long-daily-sessions
        tdSql.query(f"select name,driver FROM(SELECT name,driver,count(*) AS ten_min FROM(SELECT name,driver,avg(velocity) as mean_velocity from {dbname}.readings WHERE fleet ='West' AND ts > '2016-01-01T12:31:37Z' AND ts <= '2016-01-05T12:31:37Z' partition BY name,driver  interval(10m) ) WHERE mean_velocity > 1 GROUP BY name,driver) WHERE ten_min > 60")

        # 5. avg-daily-driving-duration
        tdSql.query(f"select _wstart as ts,fleet,name,driver,count(mv)/6 as hours_driven from ( select _wstart as ts,fleet,name,driver,avg(velocity) as mv from {dbname}.readings where ts > '2016-01-01T00:00:00Z'     and ts < '2016-01-05T00:00:01Z'    partition by fleet,name,driver interval(10m)) where ts > '2016-01-01T00:00:00Z'  and ts < '2016-01-05T00:00:01Z'    partition by fleet,name,driver interval(1d) ;")


        # # 6. avg-daily-driving-session
        # #taosc core dumped
        tdSql.query(f"select _wstart as ts,name,floor(avg(velocity)/5) AS mv from {dbname}.readings   WHERE name is not null  AND ts > '2016-01-01T00:00:00Z' AND ts < '2016-01-05T00:00:01Z'   partition by name interval(10m) fill(value,0);")
        # tdSql.query(f"select name,diff(mv) AS difka  FROM (SELECT  ts,name,mv  FROM (SELECT _wstart as ts,name,floor(avg(velocity)/10)/floor(avg(velocity)/10) AS mv from {dbname}.readings   WHERE name!='' AND ts > '2016-01-01T00:00:00Z' AND ts < '2016-01-05T00:00:01Z'   partition by name interval(10m) fill(value,0)))  group  BY name ;")
        # tdSql.query(f"select _wstart,name,floor(avg(velocity)/10)/floor(avg(velocity)/10) AS mv from {dbname}.readings   WHERE name!='' AND ts > '2016-01-01T00:00:00Z' AND ts < '2016-01-05T00:00:01Z'   partition by name interval(10m) fill(value,0)")

        # 7. avg-load
        tdSql.query(f"select fleet, model,avg(ml) AS mean_load_percentage FROM (SELECT fleet, model,current_load/load_capacity AS ml from {dbname}.diagnostics partition BY name, fleet, model) partition BY fleet, model order by  fleet ;")

        # 8. daily-activity
        tdSql.query(f"select model,ms1  FROM (SELECT _wstart as ts1,model, fleet,avg(status) AS ms1 from {dbname}.diagnostics WHERE ts >= '2016-01-01T00:00:00Z' AND ts < '2016-01-05T00:00:01Z'  partition by model, fleet interval(10m) fill(value,0)) WHERE ts1 >= '2016-01-01T00:00:00Z' AND ts1 < '2016-01-05T00:00:01Z'  AND ms1<1;")

        tdSql.query(f"select model,ms1  FROM (SELECT _wstart as ts1,model, fleet,avg(status) AS ms1 from {dbname}.diagnostics WHERE ts >= '2016-01-01T00:00:00Z' AND ts < '2016-01-05T00:00:01Z'  partition by model, fleet interval(10m) ) WHERE ts1 >= '2016-01-01T00:00:00Z' AND ts1 < '2016-01-05T00:00:01Z'  AND ms1<1;")

        tdSql.query(f"select _wstart,model,fleet,count(ms1)/144  FROM (SELECT _wstart as ts1,model, fleet,avg(status) AS ms1 from {dbname}.diagnostics WHERE ts >= '2016-01-01T00:00:00Z' AND ts < '2016-01-05T00:00:01Z'  partition by model, fleet interval(10m) fill(value,0)) WHERE ts1 >= '2016-01-01T00:00:00Z' AND ts1 < '2016-01-05T00:00:01Z'  AND ms1<1 partition by model, fleet interval(1d) ;")

        tdSql.query(f"select _wstart as ts,model,fleet,count(ms1)/144  FROM (SELECT _wstart as ts1,model, fleet,avg(status) AS ms1 from {dbname}.diagnostics WHERE ts >= '2016-01-01T00:00:00Z' AND ts < '2016-01-05T00:00:01Z'  partition by model, fleet interval(10m) ) WHERE ts1 >= '2016-01-01T00:00:00Z' AND ts1 < '2016-01-05T00:00:01Z'  AND ms1<1 partition by model, fleet interval(1d) ;")


        # 9. breakdown-frequency
        # NULL ---count(NULL)=0 expect count(NULL)= 100
        tdSql.query(f"select model,state_changed,count(state_changed)  FROM (SELECT model,diff(broken_down) AS state_changed   FROM (SELECT _wstart,model,cast(cast(floor(2*(sum(nzs)/count(nzs))) as bool) as int) AS broken_down FROM (SELECT ts,model, cast(cast(status as bool) as int) AS nzs from {dbname}.diagnostics WHERE  ts >= '2016-01-01T00:00:00Z' AND ts < '2016-01-05T00:00:01Z' ) WHERE ts >= '2016-01-01T00:00:00Z' AND ts < '2016-01-05T00:00:01Z'   partition BY model interval(10m)) partition BY model) where model is null  partition BY model,state_changed ")
        parRows=tdSql.queryRows
        assert parRows != 0 , "query result is wrong, query rows %d but expect > 0 " %parRows


        tdSql.query(f"select model,state_changed,count(state_changed)  FROM (SELECT model,diff(broken_down) AS state_changed   FROM (SELECT _wstart,model,cast(cast(floor(2*(sum(nzs)/count(nzs))) as bool) as int) AS broken_down FROM (SELECT ts,model, cast(cast(status as bool) as int) AS nzs from {dbname}.diagnostics WHERE  ts >= '2016-01-01T00:00:00Z' AND ts < '2016-01-05T00:00:01Z' ) WHERE ts >= '2016-01-01T00:00:00Z' AND ts < '2016-01-05T00:00:01Z'   partition BY model interval(10m)) partition BY model) where state_changed =1 partition BY model,state_changed ;")
        sql=f"SELECT model,count(state_changed) FROM (SELECT _rowts,model,diff(broken_down) AS state_changed FROM (SELECT ts,model,tb,cast(cast(floor(2*(nzs)) as bool) as int) AS broken_down FROM (SELECT _wstart as ts,model,tbname as tb, sum(cast(cast(status as bool) as int))/count(cast(cast(status as bool) as int)) AS nzs FROM {dbname}.diagnostics WHERE ts >= 1451606400000 AND ts < 1451952001000 partition BY tbname,model interval(10m))order by ts) partition BY tb,model ) WHERE state_changed = 1 partition BY model;"
        tdSql.query(f"{sql}")
        tdSql.checkRows(46)
        for i in range(2):
            tdSql.query("%s"%sql)
            quertR1=tdSql.queryResult
            for j in  range(50):
                tdSql.query("%s"%sql)
                quertR2=tdSql.queryResult
                assert quertR1 == quertR2 , "%s != %s ,The results of multiple queries are different" %(quertR1,quertR2)


        #it's already supported:
        # last-loc
        tdSql.query(f"select last_row(ts),latitude,longitude,name,driver from {dbname}.readings WHERE fleet='South' and name IS NOT NULL partition BY name,driver order by name ;")


        #2. low-fuel
        tdSql.query(f"select last_row(ts),name,driver,fuel_state,driver from {dbname}.diagnostics WHERE fuel_state <= 0.1 AND fleet = 'South' and name IS NOT NULL GROUP BY name,driver order by name;")

        # 3. avg-vs-projected-fuel-consumption
        tdSql.query(f"select avg(fuel_consumption) as avg_fuel_consumption,avg(nominal_fuel_consumption) as nominal_fuel_consumption from {dbname}.readings where velocity > 1 group by fleet")

    def run(self):
        tdLog.printNoPrefix("==========step1:create database and table,insert data  ==============")
        self.prepareData()
        self.tsbsIotQuery()
        tdDnodes.stop(1)
        tdDnodes.start(1)
        self.tsbsIotQuery(False)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
