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

from pickletools import long1
import sys
import os
import taos
import csv
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
from util.dnodes import tdDnodes


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.ts1 = 1593548685000    
        self.ts2 = 1593548785000    


    def run(self):
        # tdSql.execute("drop database db ")
        tdSql.prepare()
        tdSql.execute("create table st (ts timestamp, num int,  value int , t_instance int) tags (loc nchar(30))")
        node = 5
        number = 10
        for n in range(node):
            for m in range(number):
                dt= m*300000+n*60000  # collecting'frequency is 10s 
                args1=(n,n,self.ts1+dt,n,100+2*m+2*n,10+m+n)
                # args2=(n,self.ts2+dt,n,120+n,15+n)
                tdSql.execute("insert into t%d using st tags('beijing%d')  values(%d, %d, %d, %d)" % args1)
                # tdSql.execute("insert into t1 using st tags('shanghai') values(%d, %d, %d, %d)" % args2)             
                
        # interval function
        tdSql.query("select avg(value) from st interval(10m)")
        # print(tdSql.queryResult)
        tdSql.checkRows(6)        
        tdSql.checkData(0, 0, "2020-07-01 04:20:00")
        tdSql.checkData(1, 1, 107.4)
       
        # subquery with interval
        tdSql.query("select avg(avg_val) from(select avg(value) as avg_val from st where loc='beijing0' interval(10m));")
        tdSql.checkData(0, 0, 109.0)

        # subquery with interval and select two Column in parent query
        tdSql.error("select ts,avg(avg_val) from(select avg(value) as avg_val from st where loc='beijing0' interval(10m));")

        # subquery with interval and sliding 
        tdSql.query("select avg(value) as avg_val from st where loc='beijing0' interval(8m) sliding(30s) limit 1;")   
        tdSql.checkData(0, 0, "2020-07-01 04:17:00")
        tdSql.checkData(0, 1, 100) 
        tdSql.query("select avg(avg_val) from(select avg(value) as avg_val from st where loc='beijing1' interval(8m) sliding(30s));")
        tdSql.checkData(0, 0, 111)
        
        # subquery with interval and offset
        tdSql.query("select avg(value) as avg_val from st where loc='beijing0' interval(5m,1m);")
        tdSql.checkData(0, 0, "2020-07-01 04:21:00")
        tdSql.checkData(0, 1, 100)
        tdSql.query("select avg(avg_val) from(select avg(value) as avg_val from st where loc='beijing0' interval(5m,1m) group by loc);")
        tdSql.checkData(0, 0, 109)

        # subquery  with interval,sliding and group by ; parent query with interval
        tdSql.query("select avg(value) as avg_val from st where loc='beijing0' interval(8m) sliding(1m) group by loc limit 1 offset 52 ;")
        tdSql.checkData(0, 0, "2020-07-01 05:09:00")
        tdSql.checkData(0, 1, 118)
        tdSql.query("select avg(avg_val) as ncst from(select avg(value) as avg_val from st where loc!='beijing0' interval(8m) sliding(1m)  group by loc )  interval(5m);")
        tdSql.checkData(1, 1, 105)

        # #  subquery and parent query with interval and sliding 
        tdSql.query("select avg(avg_val) from(select avg(value) as avg_val from st where loc='beijing1' interval(8m) sliding(5m)) interval(10m) sliding(2m);")
        tdSql.checkData(29, 0, "2020-07-01 05:10:00.000")

        # subquery and parent query with top and bottom
        tdSql.query("select top(avg_val,2) from(select avg(value) as avg_val,num from st where loc!='beijing0' group by num) order by avg_val desc;")
        tdSql.checkData(0, 1, 117)
        tdSql.query("select bottom(avg_val,3) from(select avg(value) as avg_val,num from st where loc!='beijing0' group by num) order by avg_val asc;")
        tdSql.checkData(0, 1, 111)

        # 
        tdSql.query("select top(avg_val,2) from(select avg(value) as avg_val from st where loc='beijing1' interval(8m) sliding(3m));")
        tdSql.checkData(0, 1, 120)

        # TS-802
        tdSql.query("select first(*) from st interval(5m) limit 10")
        tdSql.checkRows(10)

        tdSql.query("select * from (select first(*) from st interval(5m) limit 10) order by ts")
        tdSql.checkRows(10)

        tdSql.query("select * from (select first(*) from st interval(5m) limit 10) order by ts desc")
        tdSql.checkRows(10)

        # clear env
        testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf ./insert_res.txt")
        os.system("rm -rf wal/%s.sql" % testcaseFilename ) 

        # TS-1584                
        os.system("tar -zxf %s/query/nestedQuery/data.tar.gz" % os.getcwd())
        tdSql.execute("create database artura")
        tdSql.execute("use artura")
        tdSql.execute("CREATE TABLE `machine_tool_metadata` (`ts` TIMESTAMP,`device_status` INT,`check_status` INT,`maintenance_status` INT,`repair_status` INT,`asset_status` INT,`device_status_1` INT,`device_status_2` INT,`device_status_3` INT,`composite_status_4` INT,`composite_status_5` INT,`risk_run_flag` NCHAR(30),`repair_stop_flag` NCHAR(30)) TAGS (`device_code` NCHAR(40),`id` BIGINT)")
        csv_reader = csv.reader(open("data/tags.csv"))
        for line in csv_reader:
            tdSql.execute("create table %s using machine_tool_metadata tags(%s, %d)" % (line[0], line[1], int(line[2])))          
            tdSql.execute("insert into %s file 'data/%s.csv'" % (line[0], line[0].replace("'", "")))

        tdSql.query("select count(1) - sum(device_status_3) as enableCount from ( select last(device_status_1) as device_status_1,last(device_status_2) as device_status_2,last(device_status_3) as device_status_3,last(composite_status_4) as composite_status_4,last(composite_status_5) as composite_status_5 from machine_tool_metadata where ts>= '2022-05-26 00:00:00.0' and ts <= '2022-06-01 23:59:59.0' interval(1d) group by id ) group by ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 55)
        tdSql.checkData(1, 0, 55)
        tdSql.checkData(2, 0, 55)
        os.system("rm -rf data")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
