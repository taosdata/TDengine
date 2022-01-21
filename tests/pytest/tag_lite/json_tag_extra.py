###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, db_test.stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import sys
import os
import taos
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
import time
import random

class TDTestCase:

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    # def assertCheck(self, filename, queryResult, expectResult):
    #     self.filename = filename
    #     self.queryResult = queryResult
    #     self.expectResult = expectResult
    #     args0 = (filename, queryResult, expectResult)
    #     assert queryResult == expectResult, "Queryfile:%s ,result is %s != expect: %s" % args0

    def assertfileDataExport(self, filename, expectResult):
        self.filename = filename
        self.expectResult = expectResult
        with open("%s" % filename, 'r+') as f1:
            for line in f1.readlines():
                queryResultTaosc = line.strip().split(',')[0]
                # self.assertCheck(filename, queryResultTaosc, expectResult)

    def run(self):
        tdSql.prepare()
        tdSql.execute("drop database if exists db_json;")
        print("==============step1 tag format =======")
        tdLog.info("create database   ")
        tdSql.execute("create database db_json")
        tdSql.execute("use db_json")    
        # test  tag format 
        tdSql.execute("create table if not exists  jsons1(ts timestamp, dataInt int, dataStr nchar(50)) tags(jtag json)")
        tdSql.error("create table if not exists  jsons1(ts timestamp, dataInt int, dataStr nchar(50)) tags(jtag json(10000000))")
        tdSql.error("create table if not exists  jsons1(ts timestamp, dataInt int, dataStr nchar(50)) tags(jtag json,jtag1 json)")
        tdSql.error("create table if not exists  jsons1(ts timestamp, dataInt int, dataStr nchar(50)) tags(jtag json,dataBool bool)")
        
        tdSql.execute("CREATE TABLE if not exists  jsons1_1 using  jsons1 tags('{\"loc\":\"fff\",\"id\":5}')")

        # two stables: jsons1 jsons2 ,test  tag's value  and  key  
        tdSql.execute("insert into  jsons1_1(ts,dataInt)  using  jsons1 tags('{\"loc+\":\"fff\",\"id\":5}') values (now,12)")

        tdSql.error("CREATE TABLE if not exists  jsons1_1 using  jsons1 tags('{oc:\"fff\",\"id\":5}')")
        tdSql.error("CREATE TABLE if not exists  jsons1_1 using  jsons1 tags('{\"loc\":fff,\"id\":5}')")
        tdSql.error("CREATE TABLE if not exists  jsons1_1 using  jsons1 tags('3333')")
        tdSql.error("CREATE TABLE if not exists  jsons1_1 using  jsons1 tags('{\"loc\":}')")
        tdSql.error("CREATE TABLE if not exists  jsons1_1 using  jsons1 tags('{\"loc\":bool)")
        tdSql.error("CREATE TABLE if not exists  jsons1_1 using  jsons1 tags(true)")
        tdSql.error("CREATE TABLE if not exists  jsons1_1 using  jsons1 tags('[{\"num\":5}]')")

        # test object and key max length. max key length is 256, max object length is 4096 include abcd.
        tdSql.execute("create table if not exists  jsons4(ts timestamp, dataInt int, dataStr nchar(50)) tags(jtag json)")

        char1= ''.join(['abcd']*64)
        char2=''.join(char1)
        char3= ''.join(['abcd']*1022)
        print(len(char3))   # 4088
        tdSql.execute("CREATE TABLE if not exists  jsons4_1 using  jsons4 tags('{\"%s\":5}')" % char1)  # len(key)=256
        tdSql.error("CREATE TABLE if not exists  jsons4_1 using  jsons4 tags('{\"%s1\":5}')" % char2)   # len(key)=257
        tdSql.execute("CREATE TABLE if not exists  jsons4_2 using  jsons4 tags('{\"T\":\"%s\"}')" % char3)  # len(object)=4096
        tdSql.error("CREATE TABLE if not exists  jsons4_2 using  jsons4 tags('{\"TS\":\"%s\"}')" % char3)   # len(object)=4097
        
        # test the  min/max length of double type , and int64  is not required 
        tdSql.error("CREATE TABLE if not exists  jsons4_3 using  jsons4 tags('{\"doublength\":-1.8e308}')")
        tdSql.error("CREATE TABLE if not exists  jsons4_3 using  jsons4 tags('{\"doublength\":1.8e308}')") 
        tdSql.execute("CREATE TABLE if not exists  jsons4_4 using  jsons4 tags('{\"doublength\":-1.7e308}')") 
        tdSql.execute("CREATE TABLE if not exists  jsons4_5 using  jsons4 tags('{\"doublength\":1.71e308}')") 
        tdSql.query("select jtag from  jsons4 where jtag->'doublength'<-1.69e+308;")
        tdSql.checkRows(1)        
        tdSql.query("select jtag from  jsons4 where jtag->'doublength'>1.7e+308;")
        tdSql.checkRows(1)    

        tdSql.execute("insert into  jsons1_1 values(now+2s, 1, 'json1')")
        tdSql.execute("insert into  jsons1_1 values(now+1s, 1, 'json1')")
        tdSql.execute("insert into  jsons1_2 using  jsons1 tags('{\"num\":5,\"location\":\"beijing\"}') values (now, 1, 'json2')")
        tdSql.execute("insert into  jsons1_3 using  jsons1 tags('{\"num\":34,\"location\":\"beijing\",\"level\":\"l1\"}') values (now, 1, 'json3')")
        tdSql.execute("insert into  jsons1_4 using  jsons1 tags('{\"class\":55,\"location\":\"beijing\",\"name\":\"name4\"}') values (now, 1, 'json4')")

        # test : json'vaule is null and 
        tdSql.execute("create table if not exists  jsons2(ts timestamp, dataInt2 int, dataStr2 nchar(50)) tags(jtag2 json)")
        tdSql.execute("CREATE TABLE if not exists  jsons2_1 using  jsons2 tags('{}')")
        tdSql.query("select jtag2 from  jsons2_1")
        tdSql.checkData(0, 0, None)
        tdSql.execute("CREATE TABLE if not exists  jsons2_2 using  jsons2 tags('')")
        tdSql.query("select jtag2 from  jsons2_2")
        tdSql.checkData(0, 0, None)
        tdSql.execute("CREATE TABLE if not exists  jsons2_3 using  jsons2 tags('null')")
        tdSql.query("select jtag2 from  jsons2_3")
        tdSql.checkData(0, 0, None) 
        tdSql.execute("CREATE TABLE if not exists  jsons2_4 using  jsons2 tags('\t')")
        tdSql.query("select jtag2 from  jsons2_4")
        tdSql.checkData(0, 0, None)
        tdSql.execute("CREATE TABLE if not exists  jsons2_5 using  jsons2 tags(' ')")
        tdSql.query("select jtag2 from  jsons2_5")
        tdSql.checkData(0, 0, None)
        tdSql.execute("CREATE TABLE if not exists  jsons2_6 using  jsons2 tags('{\"nv\":null,\"tea\":true,\"\":false,\"\":123,\"tea\":false}')")
        tdSql.query("select jtag2 from  jsons2_6")
        tdSql.checkData(0, 0, "{\"nv\":null,\"tea\":true}")
        tdSql.execute("CREATE TABLE if not exists  jsons2_7 using  jsons2 tags('{\"test7\":\"\"}')")
        tdSql.query("select jtag2 from  jsons2_7")
        tdSql.checkData(0, 0, "{\"test7\":\"\"}")
        tdSql.execute("CREATE TABLE if not exists  jsons2_8 using  jsons2 tags('{\"nv\":null,\"tea\":123,\"\":false,\"\":123,\"tea\":false}')")
        tdSql.query("select jtag2 from  jsons2_8")
        tdSql.checkData(0, 0, "{\"nv\":null,\"tea\":123}")

        print("==============step2 alter json table==")
        tdLog.info("alter stable add tag")
        tdSql.error("ALTER STABLE  jsons2 add tag jtag3 nchar(20)")
        tdSql.error("ALTER STABLE  jsons2 drop tag jtag2")
        tdSql.execute("ALTER STABLE jsons2 change tag jtag2 jtag3")
        tdSql.query("select jtag3->'tea' from  jsons2_6")
        tdSql.checkData(0, 0, "true")       
        tdSql.error("ALTER TABLE  jsons2_6 SET TAG jtag3='{\"tea-=[].;!@#$%^&*()/\":}'")
        tdSql.execute("ALTER TABLE  jsons2_6 SET TAG jtag3='{\"tea-=[].;!@#$%^&*()/\":false}'")
        tdSql.query("select jtag3 from  jsons2_6")
        tdSql.checkData(0, 0, "{\"tea-=[].;!@#$%^&*()/\":false}")
        tdSql.execute("ALTER TABLE  jsons1_1 SET TAG jtag='{\"sex\":\"femail\",\"age\":35}'")
        tdSql.query("select jtag from  jsons1_1")
        tdSql.checkData(0, 0, "{\"sex\":\"femail\",\"age\":35}")
       


        print("==============step3")
        tdLog.info("select table")

        tdSql.query("select jtag from  jsons1_1")
        tdSql.checkData(0, 0, "{\"sex\":\"femail\",\"age\":35}")

        tdSql.query("select jtag from  jsons1 where  jtag->'name'='name4'")
        tdSql.checkData(0, 0, "{\"class\":55,\"location\":\"beijing\",\"name\":\"name4\"}")


        tdSql.query("select * from  jsons1")
        tdSql.checkRows(6)

        tdSql.query("select * from  jsons1_1")
        tdSql.checkRows(3)

        tdSql.query("select * from  jsons1 where jtag->'location'='beijing'")
        tdSql.checkRows(3)

        tdSql.query("select jtag->'location' from  jsons1_2")
        tdSql.checkData(0, 0, "\"beijing\"")


        tdSql.query("select jtag->'num' from  jsons1 where jtag->'level'='l1'")
        tdSql.checkData(0, 0, 34)

        tdSql.query("select jtag->'location' from  jsons1")
        tdSql.checkRows(4)

        tdSql.query("select jtag from  jsons1_1")
        tdSql.checkRows(1)

        tdSql.query("select * from  jsons1 where jtag contains 'sex' or jtag contains 'num'")
        tdSql.checkRows(5)

        tdSql.query("select * from  jsons1 where jtag contains 'sex' and jtag contains 'num'")
        tdSql.checkRows(0)

        tdSql.query("select jtag->'sex' from  jsons1 where jtag contains 'sex' or jtag contains 'num'")
        tdSql.checkData(0, 0, "\"femail\"")
        tdSql.checkRows(3)

        tdSql.query("select *,tbname from  jsons1 where jtag->'location'='beijing'")
        tdSql.checkRows(3)

        tdSql.query("select *,tbname from  jsons1 where jtag->'num'=5 or jtag contains 'sex'")
        tdSql.checkRows(4)

        # test with tbname
        tdSql.query("select * from  jsons1 where tbname = 'jsons1_1'")
        tdSql.checkRows(3)

        tdSql.query("select * from  jsons1 where tbname = 'jsons1_1' or jtag contains 'num'")
        tdSql.checkRows(5)

        tdSql.query("select * from  jsons1 where tbname = 'jsons1_1' and jtag contains 'num'")
        tdSql.checkRows(0)

        tdSql.query("select * from  jsons1 where tbname = 'jsons1_1' or jtag->'num'=5")
        tdSql.checkRows(4)

        # test where condition like
        tdSql.query("select *,tbname from  jsons1 where jtag->'location' like 'bei%'")
        tdSql.checkRows(3)

        tdSql.query("select *,tbname from  jsons1 where jtag->'location' like 'bei%' and jtag->'location'='beijin'")
        tdSql.checkRows(0)

        tdSql.query("select *,tbname from  jsons1 where jtag->'location' like 'bei%' or jtag->'location'='beijin'")
        tdSql.checkRows(3)

        tdSql.query("select *,tbname from  jsons1 where jtag->'location' like 'bei%' and jtag->'num'=34")
        tdSql.checkRows(1)

        tdSql.query("select *,tbname from  jsons1 where (jtag->'location' like 'shanghai%' or jtag->'num'=34) and jtag->'class'=55")
        tdSql.checkRows(0)

        tdSql.query("select * from  jsons1 where jtag->'num' like '5%'")
        tdSql.checkRows(0)

        # # test where condition in
        tdSql.error("select * from  jsons1 where jtag->'location' in ('beijing')")
        # tdSql.checkRows(3)
        tdSql.error("select * from  jsons1 where jtag->'num' in (5,34)")
        # tdSql.checkRows(2)
        tdSql.error("select * from  jsons1 where jtag->'num' in ('5',34)")
        tdSql.error("select * from  jsons1 where jtag->'location' in ('beijing') and jtag->'class'=55")
        # tdSql.checkRows(1)

        # test where condition match
        tdSql.query("select * from  jsons1 where jtag->'location' match 'jin$'")
        tdSql.checkRows(0)

        tdSql.query("select * from  jsons1 where jtag->'location' match 'jin'")
        tdSql.checkRows(3)

        tdSql.query("select * from  jsons1 where datastr match 'json' and jtag->'location' match 'jin'")
        tdSql.checkRows(3)

        tdSql.query("select * from  jsons1 where jtag->'num' match '5'")
        tdSql.checkRows(0)

        # test json string parse
        tdSql.error("CREATE TABLE if not exists  jsons1_5 using  jsons1 tags('efwewf')")
        tdSql.execute("CREATE TABLE if not exists  jsons1_5 using  jsons1 tags('\t')")
        tdSql.execute("CREATE TABLE if not exists  jsons1_6 using  jsons1 tags('')")

        tdSql.query("select jtag from  jsons1_6")
        tdSql.checkData(0, 0, None)

        tdSql.execute("CREATE TABLE if not exists  jsons1_7 using  jsons1 tags('{}')")
        tdSql.query("select jtag from  jsons1_7")
        tdSql.checkData(0, 0, None)

        tdSql.execute("CREATE TABLE if not exists  jsons1_8 using  jsons1 tags('null')")
        tdSql.query("select jtag from  jsons1_8")
        tdSql.checkData(0, 0, None)

        tdSql.execute("CREATE TABLE if not exists  jsons1_9 using  jsons1 tags('{\"\":4,\"time\":null}')")
        tdSql.query("select jtag from  jsons1_9")
        tdSql.checkData(0, 0, "{\"time\":null}")

        tdSql.execute("CREATE TABLE if not exists  jsons1_10 using  jsons1 tags('{\"k1\":\"\",\"k1\":\"v1\",\"k2\":true,\"k3\":false,\"k4\":55}')")
        tdSql.query("select jtag from  jsons1_10")
        tdSql.checkData(0, 0, "{\"k1\":\"\",\"k2\":true,\"k3\":false,\"k4\":55}")

        tdSql.query("select jtag->'k2' from  jsons1_10")
        tdSql.checkData(0, 0, "true")

        tdSql.query("select jtag from  jsons1 where jtag->'k1'=''")
        tdSql.checkRows(1)

        tdSql.query("select jtag from  jsons1 where jtag->'k2'=true")
        tdSql.checkRows(1)

        tdSql.query("select jtag from  jsons1 where jtag is null")
        tdSql.checkRows(4)

        tdSql.query("select jtag from  jsons1 where jtag is not null")
        tdSql.checkRows(6)

        tdSql.query("select * from  jsons1 where jtag->'location' is not null")
        tdSql.checkRows(3)

        tdSql.query("select tbname,jtag from  jsons1 where jtag->'location' is null")
        tdSql.checkRows(7)

        tdSql.query("select * from  jsons1 where jtag->'num' is not null")
        tdSql.checkRows(2)

        tdSql.query("select * from  jsons1 where jtag->'location'='null'")
        tdSql.checkRows(0)

        tdSql.query("select * from  jsons1 where jtag->'num'=null")
        tdSql.checkRows(0)

        # test distinct
        tdSql.query("select distinct jtag from  jsons1")
        tdSql.checkRows(7)

        tdSql.query("select distinct jtag->'location' from  jsons1")
        tdSql.checkRows(2)

        # test chinese
        tdSql.execute("CREATE TABLE if not exists  jsons1_11 using  jsons1 tags('{\"k1\":\"中国\",\"k5\":\"是是是\"}')")

        tdSql.query("select tbname,jtag from  jsons1 where jtag->'k1' match '中'")
        tdSql.checkRows(1)

        tdSql.query("select tbname,jtag from  jsons1 where jtag->'k1'='中国'")
        tdSql.checkRows(1)

        #test dumplicate key with normal colomn
        tdSql.execute("INSERT INTO  jsons1_12 using  jsons1 tags('{\"tbname\":\"tt\",\"databool\":true,\"dataStr\":\"是是是\"}') values(now, 4, \"你就会\")")

        tdSql.query("select *,tbname,jtag from  jsons1 where jtag->'dataStr' match '是'")
        tdSql.checkRows(1)

        tdSql.query("select tbname,jtag->'tbname' from  jsons1 where jtag->'tbname'='tt'")
        tdSql.checkRows(1)

        # test  filter : and /or / in/ like
        tdSql.query("select * from jsons1 where jtag->'num' is not null or jtag contains 'class' and jtag contains 'databool'")
        tdSql.checkRows(2)

        tdSql.query("select * from jsons1 where jtag->'num' is not null and jtag contains 'class' or jtag contains 'databool'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 4)

        tdSql.query("select * from jsons1 where jtag->'num' is not null or jtag contains 'class' and jtag contains 'databool' and jtag->'k1' match '中'  and  jtag->'location' like 'bei%'")
        tdSql.checkRows(2)

        tdSql.query("select * from jsons1 where datastr like '你就会' and ( jtag->'num' is not null or jtag contains 'tbname' and jtag contains 'databool' )")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 4)

        tdSql.error("select * from jsons1 where datastr like '你就会' and jtag->'num' is not null or jtag contains 'class' and jtag contains 'databool'")


        tdSql.error("select * from jsons1 where datastr like '你就会' or jtag->'num' is not null or jtag contains 'class' and jtag contains 'databool' and jtag->'k1' match '中' or  jtag->'location' in ('beijing')  and  jtag->'location' like 'bei%' ")

        tdSql.query("select * from jsons1 where datastr like '你就会' and (jtag->'num' is not null or jtag contains 'class' and jtag contains 'databool' and jtag->'k1' match '中' and  jtag->'location' like 'bei%' )")
        tdSql.checkRows(0)
      
        tdSql.error("select *,tbname,jtag from  jsons1 where dataBool=true")

        # test error
        tdSql.error("CREATE TABLE if not exists  jsons1_13 using  jsons1 tags(3333)")
        tdSql.execute("CREATE TABLE if not exists  jsons1_13 using  jsons1 tags('{\"1loc\":\"fff\",\";id\":5}')")
        tdSql.error("CREATE TABLE if not exists  jsons1_13 using  jsons1 tags('{\"。loc\":\"fff\",\"fsd\":5}')")
        tdSql.error("CREATE TABLE if not exists  jsons1_13 using  jsons1 tags('{\"试试\":\"fff\",\";id\":5}')")
        tdSql.error("insert into  jsons1_13 using  jsons1 tags(3)")

        # test  query  normal column,tag and tbname 
        tdSql.execute("create stable if not exists  jsons3(ts timestamp, dataInt3 int, dataBool3  bool, dataStr3 nchar(50)) tags(jtag3 json)")
        tdSql.execute("create table jsons3_2 using  jsons3 tags('{\"t\":true,\"t123\":123,\"\":\"true\"}')")
        
        tdSql.execute("create table jsons3_3 using  jsons3 tags('{\"t\":true,\"t123\":456,\"k1\":true,\"str1\":\"111\"}')")
        tdSql.execute("insert into jsons3_3 values(now, 4, true, 'test')")

        tdSql.execute("insert into jsons3_4 using  jsons3 tags('{\"t\":true,\"t123\":789,\"k1\":false,\"s\":null,\"str1\":\"112\"}')  values(now, 5, true, 'test')")
        tdSql.query("select * from  jsons3 where jtag3->'k1'=true")
        tdSql.checkRows(1)
        tdSql.error("select  jtag3->k1 from  jsons3 ")
        tdSql.error("select  jtag3 from  jsons3 where jtag3->'k1'")
        tdSql.error("select  jtag3 from  jsons3 where jtag3 contains 'k1'=true")
        tdSql.error("select  jtag3 contains 'k1' from  jsons3;")
        tdSql.error("select  jtag3 contains 'k1'=true from  jsons3;")
        tdSql.error("select  jtag3->'k1'=true from  jsons3;")
        tdSql.execute("insert into jsons3_5 using  jsons3 tags('{\"t\":true,\"t123\":789,\"k1\":123,\"s\":null}')  values(now, 5, true, 'test')")
        tdSql.execute("insert into jsons3_5 using  jsons3 tags('{\"t\":true,\"t123\":012,\"k2\":null,\"s\":null}')  values(now+1s, 5, true, 'test')")
        tdSql.query("select jtag3 from  jsons3_5")
        tdSql.checkData(0, 0, '{\"t\":true,\"t123\":789,\"k1\":123,\"s\":null}')        
        tdSql.execute("insert into jsons3_6 using  jsons3 tags('{\"t\":true,\"t123\":789,\"k1\":false,\"s\":null}')  values(now, 5, true, 'test')")
        tdSql.query("select jtag3 from jsons3 where jtag3->'t123'=12 or jtag3 contains 'k1'")
        tdSql.checkRows(4)
        tdSql.query("select distinct jtag3 from jsons3 where jtag3->'t123'=12 or jtag3 contains 'k1'")
        tdSql.checkRows(4)


        tdSql.execute("INSERT INTO  jsons1_14 using  jsons1 tags('{\"tbname\":\"tt\",\"location\":\"tianjing\",\"dataStr\":\"是是是\"}') values(now,5, \"你就会\")")

        tdSql.query("select ts,jtag->'tbname',tbname from  jsons1 where dataint>=1 and jtag contains 'tbname'")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, '\"tt\"')

        tdSql.query("select ts,jtag->'tbname',jtag->'location',tbname from  jsons1 where dataint between 1 and 5 and jtag->'location'='tianjing'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 3, 'jsons1_14')

        tdSql.query("select ts,jtag3->'tbname', jtag3->'str1',tbname from  jsons3 where jtag3->'t123'  between 456 and 789 and jtag3->'str1' like '11%' ")
        tdSql.checkRows(2)
        for i in range(1):
            if tdSql.queryResult[i][1] == 'jsons3_3':
                tdSql.checkData(i, 2, 111) 

        tdSql.query("select  jtag3->'',dataint3 from  jsons3")
        tdSql.checkRows(5)
        for i in range(4):
            if tdSql.queryResult[i][1] == 4:
                tdSql.checkData(i, 0, None) 
        tdSql.query("select tbname,dataint3,jtag3->'k1' from jsons3;")
        tdSql.checkRows(5)
        for i in range(4):
            if tdSql.queryResult[i][1] == 4:
                tdSql.checkData(i, 2, 'true') 

        # Select_exprs is SQL function -Aggregation function  , tests includes group by and order by 
 
        tdSql.query("select  avg(dataInt),count(dataint),sum(dataint) from jsons1 group by jtag->'location' order by jtag->'location';")
        tdSql.checkData(2, 3, '\"tianjing\"')        
        tdSql.checkRows(3)
        for i in range(2):
            if tdSql.queryResult[i][3] == 'beijing':
                tdSql.checkData(i, 0, 1) 
                tdSql.checkData(i, 1, 3) 
        tdSql.error("select  avg(dataInt) as 123 ,count(dataint),sum(dataint)  from jsons1 group by jtag->'location' order by 123")
        tdSql.error("select avg(dataInt) as avgdata ,count(dataint),sum(dataint)  from jsons1 group by jtag->'location' order by  avgdata ;")
        tdSql.query("select  avg(dataInt),count(dataint),sum(dataint)   from jsons1 group by jtag->'location' order by ts;")
        tdSql.checkRows(3)
        tdSql.error("select  avg(dataInt),count(dataint),sum(dataint)   from jsons1 group by jtag->'age' order by tbname;")
        #notice,it should return error ****
        tdSql.error("select  avg(dataInt),count(dataint),sum(dataint)   from jsons1 group by jtag->'age' order by  jtag->'num' ;")
        tdSql.query("select  avg(dataInt),count(dataint),sum(dataint)   from jsons1 group by jtag->'age' order by  jtag->'age' ;")
        tdSql.checkRows(2)
        tdSql.error("select  avg(dataInt)   from jsons1 group by jtag->'location' order by dataInt;")
        tdSql.error("select  avg(dataInt),tbname   from jsons1 group by jtag->'location' order by tbname;")
        tdSql.execute("CREATE TABLE if not exists  jsons1_15 using  jsons1 tags('{\"tbname\":\"tt\",\"location\":\"beijing\"}')")
        tdSql.execute("insert into  jsons1_15 values(now+1s, 2, 'json1')")
        tdSql.error("select twa(dataint) from jsons1 group by jtag->'location' order by jtag->'location';")
        tdSql.error("select  irate(dataint) from jsons1 where jtag->'location' in ('beijing','tianjing') or jtag contains 'num' or jtag->'age'=35 ;")
        tdSql.query(" select stddev(dataint) from jsons1 group by jtag->'location';")
        tdSql.checkRows(3)
        tdSql.query(" select stddev(dataint) from jsons1  where  jtag->'location'='beijing';")
        tdSql.checkRows(1)
        tdSql.error(" select LEASTSQUARES(dataint,1,2) from jsons1_1 where  jtag->'location' ='beijing' ;")
        
        tdSql.query("select count(jtag) from jsons1 ;")
        tdSql.checkData(0, 0, 15) 
        tdSql.error("select count( jtag->'location'='beijing') from jsons1 ;")
        tdSql.error("select count( jtag contains 'age') from jsons1 ;")
        functionName = ['avg','twa','irate','stddev', 'stddev', 'leastsquares']
        print(functionName)
        for fn in functionName:
            tdSql.error("select %s( jtag) from jsons1 ;"%fn)
            tdSql.error("select %s( jtag->'location'='beijing') from jsons1 ;"%fn)
            tdSql.error("select %s( jtag contains 'age') from jsons1 ;"%fn)            
        # tdSql.error("select avg( jtag) from jsons1 ;")
        # tdSql.error("select avg( jtag->'location'='beijing') from jsons1 ;")
        # tdSql.error("select avg( jtag contains 'age') from jsons1 ;")


        
       


        # Select_exprs is SQL function -Selection function

        tdSql.query(" select  min(dataint),jtag from jsons1 where jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing' or jtag contains 'num' or jtag->'age'=35 ;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.query(" select  max(dataint),jtag from jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 12)
        tdSql.query(" select  first(*) from jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        tdSql.checkRows(1)
        tdSql.query(" select  last(*) from jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        tdSql.checkRows(1)
        tdSql.error(" select  last(*),jtag from jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        tdSql.query(" select  last_row(*) from jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        tdSql.checkRows(1)
        tdSql.query(" select  apercentile(dataint,0) from jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        tdSql.checkRows(1)
        tdSql.query(" select  apercentile(dataint,50) from jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        tdSql.checkRows(1)     
        tdSql.query(" select  apercentile(dataint,90) from jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        tdSql.checkRows(1)    
        tdSql.query(" select  apercentile(dataint,100) from jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        tdSql.checkRows(1)   
        tdSql.query(" select  apercentile(dataint,0,'t-digest') from jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        tdSql.checkRows(1)
        tdSql.query(" select  apercentile(dataint,50,'t-digest') from jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        tdSql.checkRows(1) 
        tdSql.query(" select  apercentile(dataint,100,'t-digest') from jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        tdSql.checkRows(1)   
        tdSql.query("select  top(dataint,1)  from jsons1 group by jtag->'location';")
        tdSql.query("select  tbname,top(dataint,1)  from jsons1 group by jtag->'location' order by jtag->'location' asc;")
        tdSql.query("select  tbname,top(dataint,1)  from jsons1 group by jtag->'location' order by jtag->'location'  desc;")
        tdSql.query("select  top(dataint,1)  from jsons1 group by jtag->'location' order by  ts desc;")
        tdSql.query("select  top(dataint,1)  from jsons1 group by jtag->'location' order by  ts asc;")
        tdSql.query("select  top(dataint,100)  from jsons1 group by jtag->'location';")
        tdSql.query("select  bottom(dataint,1)  from jsons1 group by jtag->'location';")
        tdSql.query("select  bottom(dataint,100)  from jsons1 group by jtag->'location';")

        tdSql.execute("create table if not exists jsons_interp(ts timestamp, dataInt int, dataBool bool, datafloat float, datadouble double,dataStr nchar(50)) tags(jtag json)")
        tdSql.execute("insert into jsons_interp_1 using jsons_interp tags('{\"nv\":null,\"tea\":true,\"rate\":456,\"tea\":false}') values ('2021-07-25 02:19:54.119',2,'true',0.9,0.1,'123')")
        tdSql.execute("insert into jsons_interp_1 values ('2021-07-25 02:19:54.219',3,'true',-4.8,-5.5,'123') ")
        tdSql.execute("insert into jsons_interp_2 using jsons_interp tags('{\"nv\":null,\"tea\":true,\"level\":\"123456\",\"rate\":123,\"tea\":false}') values ('2021-07-25 02:19:54.319',4,'true',0.9,0.1,'123')")
        tdSql.execute("insert into jsons_interp_2 values ('2021-07-25 02:19:54.419',5,'true',-5.1,1.3,'123') ")
        tdSql.query("select  interp(dataint) as itd from jsons_interp where (jtag->'rate'=123 or jtag->'rate'=456)   and ts >= '2021-07-25 02:19:53.19' and ts<= '2021-07-25 02:19:54.519'  every(100a) group by tbname order by ts desc ;")
        tdSql.checkRows(4)   
        tdSql.checkData(0,1,3)
        tdSql.checkData(2,1,5)

        tdSql.query("select  interp(dataint) as itd from jsons_interp where (jtag->'rate'=123 or jtag->'rate'=456)   and ts >= '2021-07-25 02:19:53.19' and ts<= '2021-07-25 02:19:54.519'  every(100a) group by tbname order by tbname asc;")
        tdSql.checkRows(4)   
        tdSql.checkData(0,1,2)
        tdSql.checkData(2,1,4)
        #error
        functionName = ['min','max','last','TOP','last_row','bottom','apercentile','interp']
        print(functionName)
        for fn in functionName:
            tdSql.error("select %s( jtag) from jsons1 ;"%fn)
            tdSql.error("select %s( jtag->'location'='beijing') from jsons1 ;"%fn)
            tdSql.error("select %s( jtag contains 'age') from jsons1 ;"%fn)         

        #  Select_exprs is SQL function -Calculation  function
        tdSql.error(" select  diff(dataint) from jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        tdSql.error(" select  Derivative(dataint) from jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        tdSql.query(" select  SPREAD(dataint) from jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        tdSql.checkData(0, 0, 11) 
        tdSql.query(" select  ceil(dataint) from jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        tdSql.query(" select  floor(dataint) from jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        tdSql.query(" select  round(dataint) from jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        #need insert new data --data type is double or float and tests ceil floor round .
        tdSql.execute("create table if not exists jsons7(ts timestamp, dataInt int, dataBool bool, datafloat float, datadouble double,dataStr nchar(50)) tags(jtag json)")
        tdSql.execute("insert into jsons7_1 using jsons7 tags('{\"nv\":null,\"tea\":true,\"\":false,\" \":123,\"tea\":false}') values (now+2s,2,'true',0.9,0.1,'123')")
        tdSql.execute("insert into jsons7_1 using jsons7 tags('{\"nv\":null,\"tea\":true,\"tea\":false}') values (now+3s,2,'true',0.9,0.1,'123')")
        tdSql.query("select * from jsons7 where jtag->'tea'=0 ;")
        tdSql.checkRows(0)
        tdSql.query("select * from jsons7 where jtag->'tea'=3;")
        # tdSql.checkRows(0)
        tdSql.execute("insert into jsons7_1 values (now+1s,3,'true',-4.8,-5.5,'123') ")
        tdSql.execute("insert into jsons7_1 values (now+2s,4,'true',1.9998,2.00001,'123') ")
        tdSql.execute("insert into jsons7_2 using jsons7 tags('{\"nv\":null,\"tea\":true,\"\":false,\"tag\":123,\"tea\":false}') values (now,5,'true',4.01,2.2,'123') ")
        tdSql.execute("insert into jsons7_2 using jsons7 tags('{\"nv\":null,\"tea\":true,\"tag\":123,\"tea\":false}') values (now+5s,5,'false',4.01,2.2,'123') ")
        tdSql.execute("insert into jsons7_2 (ts,datadouble) values (now+3s,-0.9) ")
        tdSql.execute("insert into jsons7_2 (ts,datadouble) values (now+4s,-2.9) ")
        tdSql.execute("insert into jsons7_2 (ts,datafloat) values (now+1s,-0.9) ")
        tdSql.execute("insert into jsons7_2 (ts,datafloat) values (now+2s,-1.9) ")
        tdSql.execute("CREATE TABLE if not exists jsons7_3 using jsons7 tags('{\"nv\":null,\"tea\":true,\"\":false,\"tag\":4569,\"tea\":false}') ")
        tdSql.query("select ts,ceil(dataint),ceil(datafloat),ceil(datadouble) from jsons7 where jtag contains 'tea';")
        tdSql.query("select ceil(dataint),ceil(datafloat),ceil(datadouble) from jsons7 where jtag contains 'tea';")
        tdSql.query("select ts,floor(dataint),floor(datafloat),floor(datadouble) from jsons7 where jtag contains 'tea';")
        tdSql.query("select floor(dataint),floor(datafloat),floor(datadouble) from jsons7 where jtag contains 'tea';")
        tdSql.query("select ts,round(dataint),round(datafloat),round(datadouble) from jsons7 where jtag contains 'tea';")
        tdSql.query("select round(dataint),round(datafloat),round(datadouble) from jsons7 where jtag contains 'tea';")

        functionName = ['diff','Derivative','SPREAD','ceil','round','floor']
        print(functionName)
        for fn in functionName:
            tdSql.error("select %s( jtag) from jsons1 ;"%fn)
            tdSql.error("select %s( jtag->'location'='beijing') from jsons1 ;"%fn)
            tdSql.error("select %s( jtag contains 'age') from jsons1 ;"%fn)       


        #modify one same key and diffirent data type,include negative number of double  
        tdSql.execute("insert into jsons7_4 using jsons7 tags('{\"nv\":null,\"tea\":123,\"tag\":123,\"tea\":false}') values (now+1s,5,'true',4.01,2.2,'abc'); ")
        tdSql.execute("insert into jsons7_5 using jsons7 tags('{\"nv\":null,\"tea\":\"app\",\"tag\":123,\"tea\":false}') values (now+2s,5,'true',4.01,2.2,'abc'); ")
        tdSql.error("insert into jsons7_6 using jsons7 tags('{\"nv\":null,\"tea\":-1.111111111111111111111111111111111111111111111111111111111111111111111,\"tag\":123,\"tea\":false}') values (now+3s,5,'true',4.01,2.2,'123'); ")
        tdSql.execute("insert into jsons7_6 using jsons7 tags('{\"nv\":null,\"tea\":-1.111111111,\"tag\":123,\"tea\":false}') values (now,5,'false',4.01,2.2,'t123'); ")
        tdSql.query("select  jtag from jsons7 where jtag->'tea'<-1.01;")
        tdSql.checkRows(1)   
        
        # test join
        tdSql.execute("create table if not exists jsons6(ts timestamp, dataInt int, dataBool bool, dataStr nchar(50)) tags(jtag json)")
        tdSql.execute("create table if not exists jsons5(ts timestamp, dataInt int, dataBool bool, dataStr nchar(50)) tags(jtag json)")
        tdSql.execute("CREATE TABLE if not exists jsons6_1 using jsons6 tags('{\"loc\":\"fff\",\"id\":6,\"user\":\"ffc\"}')")
        tdSql.execute("CREATE TABLE if not exists jsons6_2 using jsons6 tags('{\"loc\":\"ffc\",\"id\":5}')")
        tdSql.execute("insert into jsons6_1 values ('2020-04-18 15:00:00.000', 1, false, 'json1')")
        tdSql.execute("insert into jsons6_2 values ('2020-04-18 15:00:01.000', 2, false, 'json1')")
        tdSql.execute("insert into jsons5_1 using jsons5 tags('{\"loc\":\"fff\",\"num\":5,\"location\":\"beijing\"}') values ('2020-04-18 15:00:00.000', 2, true, 'json2')")
        tdSql.execute("insert into jsons5_2 using jsons5 tags('{\"loc\":\"fff\",\"id\":5,\"location\":\"beijing\"}') values ('2020-04-18 15:00:01.000', 2, true, 'json2')")
        tdSql.error("select 'sss',33,a.jtag->'loc' from jsons6 a,jsons5 b where a.ts=b.ts and a.jtag->'loc'=b.jtag->'loc'")
        tdSql.error("select 'sss',33,a.jtag->'loc' from jsons6 a,jsons5 b where a.ts=b.ts and a.jtag->'user'=b.jtag->'loc';")
        tdSql.query("select 'sss',33,a.jtag->'loc' from jsons6 a,jsons5 b where a.ts=b.ts and a.jtag->'id'=b.jtag->'id'")
        tdSql.checkData(0, 0, "sss")
        tdSql.checkData(0, 2, "\"ffc\"")



        # #nested query 
        tdSql.error("select jtag->'tag' from (select tbname,jtag,ts,ceil(dataint) as cdata,ceil(datafloat) ,ceil(datadouble) from jsons7 where jtag contains 'tea') where cdata=3 ") # not currently supported
        tdSql.error("select jtag from (select tbname,jtag,ts,ceil(dataint) as cdata,ceil(datafloat) ,ceil(datadouble) from jsons7 where jtag contains 'tea') where jtag->'tag'=123 ")  # not currently supported
        tdSql.query("select * from (select tbname,jtag->'tea',ts,ceil(dataint) as cdata,ceil(datafloat) ,ceil(datadouble) from jsons7 where jtag contains 'tea') where cdata=5 ")
        tdSql.checkRows(5)
        for i in range(5):
            if tdSql.queryResult[i][0] == 'jsons7_4':
                tdSql.checkData(i, 1, 123) 
                tdSql.checkData(i, 3, 5) 
            if tdSql.queryResult[i][0] == 'jsons7_5':
                tdSql.checkData(i, 1, "\"app\"") 

        # query  child table 
        tdSql.error("select * from  jsons3_2 where jtag3->'k1'=true;")
        # tdSql.checkData(0, 0, None)
        # tdSql.checkRows(3)

        # union all :max times is 100
        unioSql = "select ts,jtag->'tbname',jtag->'location',tbname from  jsons1 where dataint between 1 and 5 and jtag->'location'='tianjing' union all "
        for i in range(99):
            if (i < 98):
                unioSql +=  "select ts,jtag->'tbname',jtag->'location',tbname from  jsons1 where dataint between 1 and 5 and jtag->'location'='tianjing' union all "
            else:
                print(i)                
                unioSql +=  " select ts,jtag->'tbname',jtag->'location',tbname from  jsons1 where dataint between 1 and 5 and jtag->'location'='tianjing'"
        tdSql.query(unioSql)
        tdSql.checkRows(100)
        unioSql +=  " union all   select ts,jtag->'tbname',jtag->'location',tbname from  jsons1 where dataint between 1 and 5 and jtag->'location'='tianjing'"
        tdSql.error(unioSql)

        

        # fuction testcase : stddev, supported data type: int\str\bool unsupported data type: float\double
        tdSql.query(" select stddev(datafloat),dataint from jsons7 group by dataint;")
        tdSql.checkRows(5)
        tdSql.query(" select stddev(dataint) from jsons7 group by datastr;")
        tdSql.checkRows(4)
        tdSql.query(" select stddev(dataint) from jsons7 group by databool;")
        tdSql.checkRows(3)
        tdSql.error(" select stddev(dataint) from jsons7 group by datafloat;")
        tdSql.error(" select stddev(dataint) from jsons7 group by datadouble;")
        tdSql.execute("create table if not exists jsons8(ts timestamp, dataInt int, dataBool bool, datafloat float, datadouble double,dataStr nchar(50),datatime timestamp) tags(jtag json)")
        tdSql.execute("insert into jsons8_1 using jsons8 tags('{\"nv\":null,\"tea\":true,\"\":false,\" \":123,\"tea\":false}') values (now,2,'true',0.9,0.1,'abc',now+60s)")
        tdSql.execute("insert into jsons8_2 using jsons8 tags('{\"nv\":null,\"tea\":true,\"\":false,\" \":123,\"tea\":false}') values (now+5s,2,'true',0.9,0.1,'abc',now+65s)")
        tdSql.query(" select stddev(dataint) from jsons8 group by datatime;")
        tdSql.error(" select stddev(datatime) from jsons8 group by datadouble;")

        # # verify the tag length of the super table and the child table 
        # TD-12389
        # tdSql.query("describe jsons1;")
        # jtagLengthSup=tdSql.queryResult[3][2]
        # tdSql.query("describe jsons1_1;")
        # tdSql.checkData(3, 2, jtagLengthSup)

        
        # #test import and export
        # tdSql.execute("select * from jsons1 >> jsons1_data.csv;")
        # tdSql.query("select * from jsons1 ")
        # with open("./jsons1_data.csv", 'r+') as f1:
        #     # count=len(open("./jsons1_data.csv",'rU').readlines())
        #     # print(count)
        #     rows=0
        #     for line in f1.readlines():
        #             # for columns in range(4): # it will be replaced with column length later,but now  it is setted to a fixed value first
        #         queryResultInt = line.strip().split(',')[1]
        #         # queryResultTag = line.strip().split(',')[3]
        #        # for rows in range(9):
        #         # print(rows,1,queryResultInt,queryResultTag)
        #         tdSql.checkData(rows, 1, "%s" %queryResultInt)  
        #         # tdSql.checkData(rows, 3, "%s" %queryResultTag)  
        #         rows +=1

        # # test taos -f
        # os.system("taos -f stable/json_tag_extra.py.sql ")
        # tdSql.execute("use db_json")    
        # tdSql.query("select * from jsons1")
        # tdSql.checkRows(9)      

        # # test drop tables and databases
        # tdSql.execute("drop table jsons1_1")
        # tdSql.execute("drop stable jsons1")
        # tdSql.execute("drop stable jsons3")
        # tdSql.execute("drop stable jsons2")
        # tdSql.execute("drop database db_json")
        
        testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf ./insert_res.txt")
        os.system("rm -rf tools/taosdemoAllTest/%s.sql" % testcaseFilename )     


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
