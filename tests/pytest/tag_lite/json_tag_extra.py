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

    def run(self):
        tdSql.prepare()

        print("==============step1 tag format =======")
        tdLog.info("create database two stables and   ")
        tdSql.execute("create database db_json_tag_test")
        tdSql.execute("use db_json_tag_test")    
        # test  tag format 
        tdSql.execute("create table if not exists  jsons1(ts timestamp, dataInt int, dataStr nchar(50)) tags(jtag json(128))")
        tdSql.error("create table if not exists  jsons1(ts timestamp, dataInt int, dataStr nchar(50)) tags(jtag json(64),jtag1 json(100))")
        tdSql.error("create table if not exists  jsons1(ts timestamp, dataInt int, dataStr nchar(50)) tags(jtag json(64),dataBool bool)")
        
        tdSql.execute("CREATE TABLE if not exists  jsons1_1 using  jsons1 tags('{\"loc\":\"fff\",\"id\":5}')")
        tdSql.execute("use db_json_tag_test")


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
        tdSql.execute("create table if not exists  jsons4(ts timestamp, dataInt int, dataStr nchar(50)) tags(jtag json(128))")

        char1= ''.join(['abcd']*64)
        char2=''.join(char1)
        char3= ''.join(['abcd']*1022)
        print(len(char3))   # 4088
        tdSql.execute("CREATE TABLE if not exists  jsons4_1 using  jsons4 tags('{\"%s\":5}')" % char1)  # len(key)=256
        tdSql.error("CREATE TABLE if not exists  jsons4_1 using  jsons4 tags('{\"%s1\":5}')" % char2)   # len(key)=257
        tdSql.execute("CREATE TABLE if not exists  jsons4_2 using  jsons4 tags('{\"T\":\"%s\"}')" % char3)  # len(object)=4096
        tdSql.error("CREATE TABLE if not exists  jsons4_2 using  jsons4 tags('{\"TS\":\"%s\"}')" % char3)   # len(object)=4097

        tdSql.execute("insert into  jsons1_1 values(now, 1, 'json1')")
        tdSql.execute("insert into  jsons1_1 values(now+1s, 1, 'json1')")
        tdSql.execute("insert into  jsons1_2 using  jsons1 tags('{\"num\":5,\"location\":\"beijing\"}') values (now, 1, 'json2')")
        tdSql.execute("insert into  jsons1_3 using  jsons1 tags('{\"num\":34,\"location\":\"beijing\",\"level\":\"l1\"}') values (now, 1, 'json3')")
        tdSql.execute("insert into  jsons1_4 using  jsons1 tags('{\"class\":55,\"location\":\"beijing\",\"name\":\"name4\"}') values (now, 1, 'json4')")

        # test : json'vaule is null and 
        tdSql.execute("create table if not exists  jsons2(ts timestamp, dataInt2 int, dataStr2 nchar(50)) tags(jtag2 json(300))")
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
        tdSql.checkData(0, 0, "{\"tea\":true}")
        tdSql.error("CREATE TABLE if not exists  jsons2_7 using  jsons2 tags('{\"nv\":null,\"tea\":123,\"\":false,\"\":123,\"tea\":false}')")
        tdSql.execute("CREATE TABLE if not exists  jsons2_7 using  jsons2 tags('{\"test7\":\"\"}')")
        tdSql.query("select jtag2 from  jsons2_7")
        tdSql.checkData(0, 0, "{\"test7\":\"\"}")

        print("==============step2 alter json table==")
        tdLog.info("alter stable add tag")
        tdSql.error("ALTER STABLE  jsons2 add tag jtag3 nchar(20)")
        tdSql.error("ALTER STABLE  jsons2 drop tag jtag2")
        tdSql.execute("ALTER STABLE jsons2 change tag jtag2 jtag3")
        tdSql.query("select jtag3 from  jsons2_6")
        tdSql.checkData(0, 0, "{\"tea\":true}")       
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
        tdSql.checkData(0, 0, "beijing")


        tdSql.query("select jtag->'num' from  jsons1 where jtag->'level'='l1'")
        tdSql.checkData(0, 0, 34)

        tdSql.query("select jtag->'location' from  jsons1")
        tdSql.checkRows(4)

        tdSql.query("select jtag from  jsons1_1")
        tdSql.checkRows(1)

        tdSql.query("select * from  jsons1 where jtag?'sex' or jtag?'num'")
        tdSql.checkRows(5)

        tdSql.query("select * from  jsons1 where jtag?'sex' and jtag?'num'")
        tdSql.checkRows(0)

        tdSql.query("select jtag->'sex' from  jsons1 where jtag?'sex' or jtag?'num'")
        tdSql.checkData(0, 0, "femail")
        tdSql.checkRows(3)

        tdSql.query("select *,tbname from  jsons1 where jtag->'location'='beijing'")
        tdSql.checkRows(3)

        tdSql.query("select *,tbname from  jsons1 where jtag->'num'=5 or jtag?'sex'")
        tdSql.checkRows(4)

        # test with tbname
        tdSql.query("select * from  jsons1 where tbname = 'jsons1_1'")
        tdSql.checkRows(3)

        tdSql.query("select * from  jsons1 where tbname = 'jsons1_1' or jtag?'num'")
        tdSql.checkRows(5)

        tdSql.query("select * from  jsons1 where tbname = 'jsons1_1' and jtag?'num'")
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

        tdSql.error("select * from  jsons1 where jtag->'num' like '5%'")

        # test where condition in
        tdSql.query("select * from  jsons1 where jtag->'location' in ('beijing')")
        tdSql.checkRows(3)

        tdSql.query("select * from  jsons1 where jtag->'num' in (5,34)")
        tdSql.checkRows(2)

        tdSql.error("select * from  jsons1 where jtag->'num' in ('5',34)")

        tdSql.query("select * from  jsons1 where jtag->'location' in ('beijing') and jtag->'class'=55")
        tdSql.checkRows(1)

        # test where condition match
        tdSql.query("select * from  jsons1 where jtag->'location' match 'jin$'")
        tdSql.checkRows(0)

        tdSql.query("select * from  jsons1 where jtag->'location' match 'jin'")
        tdSql.checkRows(3)

        tdSql.query("select * from  jsons1 where datastr match 'json' and jtag->'location' match 'jin'")
        tdSql.checkRows(3)

        tdSql.error("select * from  jsons1 where jtag->'num' match '5'")

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

        tdSql.execute("CREATE TABLE if not exists  jsons1_9 using  jsons1 tags('{\"\":4, \"time\":null}')")
        tdSql.query("select jtag from  jsons1_9")
        tdSql.checkData(0, 0, None)

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
        tdSql.checkRows(5)

        tdSql.query("select jtag from  jsons1 where jtag is not null")
        tdSql.checkRows(5)

        tdSql.query("select * from  jsons1 where jtag->'location' is not null")
        tdSql.checkRows(3)

        tdSql.query("select tbname,jtag from  jsons1 where jtag->'location' is null")
        tdSql.checkRows(7)

        tdSql.query("select * from  jsons1 where jtag->'num' is not null")
        tdSql.checkRows(2)

        tdSql.query("select * from  jsons1 where jtag->'location'='null'")
        tdSql.checkRows(0)

        tdSql.error("select * from  jsons1 where jtag->'num'='null'")

        # test distinct
        tdSql.query("select distinct jtag from  jsons1")
        tdSql.checkRows(6)

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

        # tdSql.query("select * from jsons1 where jtag->'num' is not null or jtag?'class' and jtag?'databool'")
        # tdSql.checkRows(0)

        # tdSql.query("select * from jsons1 where jtag->'num' is not null or jtag?'class' and jtag?'databool' and jtag->'k1' match '中' or  jtag->'location' in ('beijing')  and  jtag->'location' like 'bei%'")

        # tdSql.query("select * from jsons1 where datastr like '你就会' and ( jtag->'num' is not null or jtag?'class' and jtag?'databool' )")


        tdSql.error("select * from jsons1 where datastr like '你就会' or jtag->'num' is not null or jtag?'class' and jtag?'databool' and jtag->'k1' match '中' or  jtag->'location' in ('beijing')  and  jtag->'location' like 'bei%' ")

        # tdSql.query("select * from jsons1 where datastr like '你就会' and (jtag->'num' is not null or jtag?'class' and jtag?'databool' and jtag->'k1' match '中' or  jtag->'location' in ('beijing')  and  jtag->'location' like 'bei%' )")
        # tdSql.checkRows(0)
      
        tdSql.error("select *,tbname,jtag from  jsons1 where dataBool=true")

        # test error
        tdSql.error("CREATE TABLE if not exists  jsons1_13 using  jsons1 tags(3333)")
        tdSql.execute("CREATE TABLE if not exists  jsons1_13 using  jsons1 tags('{\"1loc\":\"fff\",\";id\":5}')")
        tdSql.error("CREATE TABLE if not exists  jsons1_13 using  jsons1 tags('{\"。loc\":\"fff\",\"fsd\":5}')")
        tdSql.error("CREATE TABLE if not exists  jsons1_13 using  jsons1 tags('{\"试试\":\"fff\",\";id\":5}')")
        tdSql.error("insert into  jsons1_13 using  jsons1 tags(3)")

        # test  query normal column
        tdSql.execute("create stable if not exists  jsons3(ts timestamp, dataInt3 int(100), dataBool3  bool, dataStr3 nchar(50)) tags(jtag3 json)")
        tdSql.execute("create table jsons3_2 using  jsons3 tags('{\"t\":true,\"t123\":123,\"\":\"true\"}')")
        
        tdSql.execute("create table jsons3_3 using  jsons3 tags('{\"t\":true,\"t123\":456,\"k1\":true}')")
        tdSql.execute("insert into jsons3_3 values(now, 4, true, 'test')")

        tdSql.execute("insert into jsons3_4 using  jsons3 tags('{\"t\":true,\"t123\":789,\"k1\":false,\"s\":null}')  values(now, 5, true, 'test')")
        tdSql.query("select * from  jsons3 where jtag3->'k1'=true")
        tdSql.checkRows(1)
        tdSql.error("select  jtag3->k1 from  jsons3 ")
        tdSql.error("select  jtag3 from  jsons3 where jtag3->'k1'")
        tdSql.error("select  jtag3 from  jsons3 where jtag3?'k1'=true")
        tdSql.error("select  jtag3?'k1' from  jsons3;")
        tdSql.error("select  jtag3?'k1'=true from  jsons3;")
        tdSql.error("select  jtag3->'k1'=true from  jsons3;")
        tdSql.error("insert into jsons3_5 using  jsons3 tags('{\"t\":true,\"t123\":789,\"k1\":1,\"s\":null}')  values(now, 5, true, 'test')")
        tdSql.execute("insert into jsons3_5 using  jsons3 tags('{\"t\":true,\"t123\":012,\"k1\":null,\"s\":null}')  values(now, 5, true, 'test')")
        tdSql.execute("insert into jsons3_6 using  jsons3 tags('{\"t\":true,\"t123\":789,\"k1\":false,\"s\":null}')  values(now, 5, true, 'test')")
        # tdSql.execute("select distinct jtag3 from jsons3 where jtag3->'t123'=12 or jtag3?'k1'")
        # tdSql.checkRows(3)


        tdSql.execute("INSERT INTO  jsons1_14 using  jsons1 tags('{\"tbname\":\"tt\",\"location\":\"tianjing\",\"dataStr\":\"是是是\"}') values(now,5, \"你就会\")")

        # tdSql.execute("select ts,dataint3,jtag->tbname from  jsons1 where dataint>=1 and jtag->'location' in ('tianjing','123') and jtag?'tbname'")
        # tdSql.checkRows(1)
        # tdSql.checkData(0, 2, 'tt')
               
        # query normal column and tag column
        tdSql.query("select  jtag3->'',dataint3 from  jsons3")
        tdSql.checkRows(4)

        # query  child table 

        tdSql.error("select * from  jsons3_2 where jtag3->'k1'=true;")

        # tdSql.checkData(0, 0, None)
        # tdSql.checkRows(3)



        # # test drop tables and databases
        # tdSql.execute("drop table jsons1_1")
        # tdSql.execute("drop stable jsons1")
        # tdSql.execute("drop stable jsons3")
        # tdSql.execute("drop stable jsons2")
        # tdSql.execute("drop database db_json_tag_test")



    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
