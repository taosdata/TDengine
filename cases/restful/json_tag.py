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
# ported from tag_lite/json_tag_extra.py

from taostest import TDCase, T
from taostest.util.common import TDCom
from taostest.util.rest import TDRest
import json

class TestJsonTag(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.tdRest = TDRest()

    def insert_after_alter_column(self):
        """
        insert after alter column
        """
        dbname = self.tdCom.get_long_name(len=10, mode="letters")
        self.tdRest.request(f'create database if not exists {dbname}')
        self.tdRest.request(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 int, c2 int) tags (t1 int, t2 int)')
        self.tdRest.request(f'create table if not exists {dbname}.tb using {dbname}.stb tags (1, 1)')
        self.tdRest.request(f'insert into {dbname}.tb values (now, 1, 1)')
        # drop column
        self.tdRest.request(f'alter stable {dbname}.stb drop column c2')
        self.tdRest.request(f'insert into {dbname}.tb values (now-1m, 2)')
        self.tdRest.error(f'insert into {dbname}.tb values (now-1m, 2, 2)')
        self.tdRest.error(f'select t1, t2, c1, c2 from {dbname}.tb')
        self.tdRest.request(f'select t1, t2, c1 from {dbname}.tb where c1 = 2')
        self.tdSql.checkEqual(self.tdRest.resp["data"][0], [1, 1, 2])

        # add column
        self.tdRest.request(f'alter stable {dbname}.stb add column c2 int')
        self.tdRest.request(f'insert into {dbname}.tb values (now-1m, 2, 2)')
        self.tdRest.request(f'select t1, t2, c1, c2 from {dbname}.tb where c2 = 2')
        self.tdSql.checkEqual(self.tdRest.resp["data"][0], [1, 1, 2, 2])
        self.tdRest.request(f'drop database if exists {dbname}')

    def run(self) -> bool:
        self.tdRest.request("drop database if exists db_json;")
        print("==============step1 tag format =======")
        self.tdRest.request("create database db_json")
        # test  tag format 
        self.tdRest.request("create table if not exists  db_json.jsons1(ts timestamp, dataInt int, dataStr nchar(50)) tags(jtag json)")
        self.tdRest.error("create table if not exists  db_json.jsons1(ts timestamp, dataInt int, dataStr nchar(50)) tags(jtag json(10000000))")
        self.tdRest.error("create table if not exists  db_json.jsons1(ts timestamp, dataInt int, dataStr nchar(50)) tags(jtag json,jtag1 json)")
        self.tdRest.error("create table if not exists  db_json.jsons1(ts timestamp, dataInt int, dataStr nchar(50)) tags(jtag json,dataBool bool)")
        
        self.tdRest.request("create table if not exists  db_json.jsons1_1 using  db_json.jsons1 tags('{\"loc\":\"fff\",\"id\":5}')")

        # two stables: jsons1 jsons2 ,test  tag's value  and  key  
        self.tdRest.request("insert into  db_json.jsons1_1(ts,dataInt)  using  db_json.jsons1 tags('{\"loc+\":\"fff\",\"id\":5}') values (now,12)")

        self.tdRest.error("create table if not exists  db_json.jsons1_1 using  db_json.jsons1 tags('{oc:\"fff\",\"id\":5}')")
        self.tdRest.error("create table if not exists  db_json.jsons1_1 using  db_json.jsons1 tags('{\"loc\":fff,\"id\":5}')")
        self.tdRest.error("create table if not exists  db_json.jsons1_1 using  db_json.jsons1 tags('3333')")
        self.tdRest.error("create table if not exists  db_json.jsons1_1 using  db_json.jsons1 tags('{\"loc\":}')")
        self.tdRest.error("create table if not exists  db_json.jsons1_1 using  db_json.jsons1 tags('{\"loc\":bool)")
        self.tdRest.error("create table if not exists  db_json.jsons1_1 using  db_json.jsons1 tags(true)")
        self.tdRest.error("create table if not exists  db_json.jsons1_1 using  db_json.jsons1 tags('[{\"num\":5}]')")

        # test object and key max length. max key length is 256, max object length is 4096 include abcd.
        self.tdRest.request("create table if not exists  db_json.jsons4(ts timestamp, dataInt int, dataStr nchar(50)) tags(jtag json)")

        char1= ''.join(['abcd']*64)
        char2=''.join(char1)
        char3= ''.join(['abcd']*1022)
        self.tdRest.request("CREATE TABLE if not exists  db_json.jsons4_1 using  db_json.jsons4 tags('{\"%s\":5}')" % char1)  # len(key)=256
        self.tdRest.error("CREATE TABLE if not exists  db_json.jsons4_1 using  db_json.jsons4 tags('{\"%s1\":5}')" % char2)   # len(key)=257
        self.tdRest.request("CREATE TABLE if not exists  jsons4_2 using  db_json.jsons4 tags('{\"T\":\"%s\"}')" % char3)  # len(object)=4096
        self.tdRest.error("CREATE TABLE if not exists  jsons4_2 using  db_json.jsons4 tags('{\"TS\":\"%s\"}')" % char3)   # len(object)=4097
        
        # test the  min/max length of double type , and int64  is not required 
        self.tdRest.error("CREATE TABLE if not exists  db_json.jsons4_3 using  db_json.jsons4 tags('{\"doublength\":-1.8e308}')")
        self.tdRest.error("CREATE TABLE if not exists  db_json.jsons4_3 using  db_json.jsons4 tags('{\"doublength\":1.8e308}')") 
        self.tdRest.request("CREATE TABLE if not exists  db_json.jsons4_4 using  db_json.jsons4 tags('{\"doublength\":-1.7e308}')") 
        self.tdRest.request("CREATE TABLE if not exists  db_json.jsons4_5 using  db_json.jsons4 tags('{\"doublength\":1.71e308}')") 
        self.tdRest.request("select jtag from  db_json.jsons4 where jtag->'doublength'<-1.69e+308;")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1)        
        self.tdRest.request("select jtag from  db_json.jsons4 where jtag->'doublength'>1.7e+308;")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1)    

        self.tdRest.request("insert into  db_json.jsons1_1 values(now+2s, 1, 'json1')")
        self.tdRest.request("insert into  db_json.jsons1_1 values(now+1s, 1, 'json1')")
        self.tdRest.request("insert into  db_json.jsons1_2 using  db_json.jsons1 tags('{\"num\":5,\"location\":\"beijing\"}') values (now, 1, 'json2')")
        self.tdRest.request("insert into  db_json.jsons1_3 using  db_json.jsons1 tags('{\"num\":34,\"location\":\"beijing\",\"level\":\"l1\"}') values (now, 1, 'json3')")
        self.tdRest.request("insert into  db_json.jsons1_4 using  db_json.jsons1 tags('{\"class\":55,\"location\":\"beijing\",\"name\":\"name4\"}') values (now, 1, 'json4')")

        # test : json'vaule is null and 
        self.tdRest.request("create table if not exists  db_json.jsons2(ts timestamp, dataInt2 int, dataStr2 nchar(50)) tags(jtag2 json)")
        self.tdRest.request("CREATE TABLE if not exists  db_json.jsons2_1 using  db_json.jsons2 tags('{}')")
        self.tdRest.request("select jtag2 from  db_json.jsons2_1")
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], None)
        self.tdRest.request("CREATE TABLE if not exists  db_json.jsons2_2 using  db_json.jsons2 tags('')")
        self.tdRest.request("select jtag2 from  db_json.jsons2_2")
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], None)
        self.tdRest.request("CREATE TABLE if not exists  db_json.jsons2_3 using  db_json.jsons2 tags('null')")
        self.tdRest.request("select jtag2 from  db_json.jsons2_3")
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], None) 
        self.tdRest.request("CREATE TABLE if not exists  db_json.jsons2_4 using  db_json.jsons2 tags('\t')")
        self.tdRest.request("select jtag2 from  db_json.jsons2_4")
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], None)
        self.tdRest.request("CREATE TABLE if not exists  db_json.jsons2_5 using  db_json.jsons2 tags(' ')")
        self.tdRest.request("select jtag2 from  db_json.jsons2_5")
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], None)
        self.tdRest.request("CREATE TABLE if not exists  db_json.jsons2_6 using  db_json.jsons2 tags('{\"nv\":null,\"tea\":true,\"\":false,\"\":123,\"tea\":false}')")
        self.tdRest.request("select jtag2 from  db_json.jsons2_6")
        self.tdSql.checkEqual(json.dumps(self.tdRest.resp["data"][0][0]), '{"nv": null, "tea": true}')
        self.tdRest.request("CREATE TABLE if not exists  db_json.jsons2_7 using  db_json.jsons2 tags('{\"test7\":\"\"}')")
        self.tdRest.request("select jtag2 from  db_json.jsons2_7")
        self.tdSql.checkEqual(json.dumps(self.tdRest.resp["data"][0][0]), "{\"test7\": \"\"}")
        self.tdRest.request("CREATE TABLE if not exists  db_json.jsons2_8 using  db_json.jsons2 tags('{\"nv\":null,\"tea\":123,\"\":false,\"\":123,\"tea\":false}')")
        self.tdRest.request("select jtag2 from  db_json.jsons2_8")
        self.tdSql.checkEqual(json.dumps(self.tdRest.resp["data"][0][0]), "{\"nv\": null, \"tea\": 123}")

        print("==============step2 alter json table==")
        # "alter stable add tag"
        self.tdRest.error("ALTER STABLE  db_json.jsons2 add tag jtag3 nchar(20)")
        self.tdRest.error("ALTER STABLE  db_json.jsons2 drop tag jtag2")
        self.tdRest.request("ALTER STABLE db_json.jsons2 change tag jtag2 jtag3")
        self.tdRest.request("select jtag3->'tea' from  db_json.jsons2_6")
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], True)
        self.tdRest.error("ALTER TABLE  db_json.jsons2_6 SET TAG jtag3='{\"tea-=[].;!@#$%^&*()/\":}'")
        self.tdRest.request("ALTER TABLE  db_json.jsons2_6 SET TAG jtag3='{\"tea-=[].;!@#$%^&*()/\":false}'")
        self.tdRest.request("select jtag3 from  db_json.jsons2_6")
        self.tdSql.checkEqual(json.dumps(self.tdRest.resp["data"][0][0]), "{\"tea-=[].;!@#$%^&*()/\": false}")
        self.tdRest.request("ALTER TABLE  db_json.jsons1_1 SET TAG jtag='{\"sex\":\"femail\",\"age\":35}'")
        self.tdRest.request("select jtag from  db_json.jsons1_1")
        self.tdSql.checkEqual(json.dumps(self.tdRest.resp["data"][0][0]), "{\"sex\": \"femail\", \"age\": 35}")
       
        print("==============step3")
        print("select table")

        self.tdRest.request("select jtag from  db_json.jsons1_1")
        self.tdSql.checkEqual(json.dumps(self.tdRest.resp["data"][0][0]), "{\"sex\": \"femail\", \"age\": 35}")

        self.tdRest.request("select jtag from  db_json.jsons1 where  jtag->'name'='name4'")
        self.tdSql.checkEqual(json.dumps(self.tdRest.resp["data"][0][0]), "{\"class\": 55, \"location\": \"beijing\", \"name\": \"name4\"}")


        self.tdRest.request("select * from  db_json.jsons1")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 6)

        self.tdRest.request("select * from  db_json.jsons1_1")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 3)

        self.tdRest.request("select * from  db_json.jsons1 where jtag->'location'='beijing'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 3)

        self.tdRest.request("select jtag->'location' from  db_json.jsons1_2")
        self.tdSql.checkEqual(json.dumps(self.tdRest.resp["data"][0][0]), "\"beijing\"")


        self.tdRest.request("select jtag->'num' from  db_json.jsons1 where jtag->'level'='l1'")
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], 34)

        self.tdRest.request("select jtag->'location' from  db_json.jsons1")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 4)

        self.tdRest.request("select jtag from  db_json.jsons1_1")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1)

        self.tdRest.request("select * from  db_json.jsons1 where jtag contains 'sex' or jtag contains 'num'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 5)

        self.tdRest.request("select * from  db_json.jsons1 where jtag contains 'sex' and jtag contains 'num'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 0)

        self.tdRest.request("select jtag->'sex' from  db_json.jsons1 where jtag contains 'sex' or jtag contains 'num'")
        # self.tdSql.checkIn("\"femail\"", json.dumps(self.tdRest.resp["data"][0]))
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 3)

        self.tdRest.request("select *,tbname from  db_json.jsons1 where jtag->'location'='beijing'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 3)

        self.tdRest.request("select *,tbname from  db_json.jsons1 where jtag->'num'=5 or jtag contains 'sex'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 4)

        # test with tbname
        self.tdRest.request("select * from  db_json.jsons1 where tbname = 'jsons1_1'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 3)

        self.tdRest.request("select * from  db_json.jsons1 where tbname = 'jsons1_1' or jtag contains 'num'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 5)

        self.tdRest.request("select * from  db_json.jsons1 where tbname = 'jsons1_1' and jtag contains 'num'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 0)

        self.tdRest.request("select * from  db_json.jsons1 where tbname = 'jsons1_1' or jtag->'num'=5")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 4)

        # test where condition like
        self.tdRest.request("select *,tbname from  db_json.jsons1 where jtag->'location' like 'bei%'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 3)

        self.tdRest.request("select *,tbname from  db_json.jsons1 where jtag->'location' like 'bei%' and jtag->'location'='beijin'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 0)

        self.tdRest.request("select *,tbname from  db_json.jsons1 where jtag->'location' like 'bei%' or jtag->'location'='beijin'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 3)

        self.tdRest.request("select *,tbname from  db_json.jsons1 where jtag->'location' like 'bei%' and jtag->'num'=34")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1)

        self.tdRest.request("select *,tbname from  db_json.jsons1 where (jtag->'location' like 'shanghai%' or jtag->'num'=34) and jtag->'class'=55")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 0)

        self.tdRest.request("select * from  db_json.jsons1 where jtag->'num' like '5%'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 0)

        # # test where condition in
        self.tdRest.error("select * from  db_json.jsons1 where jtag->'location' in ('beijing')")
        self.tdRest.error("select * from  db_json.jsons1 where jtag->'num' in (5,34)")
        self.tdRest.error("select * from  db_json.jsons1 where jtag->'num' in ('5',34)")
        self.tdRest.error("select * from  db_json.jsons1 where jtag->'location' in ('beijing') and jtag->'class'=55")

        # test where condition match
        self.tdRest.request("select * from  db_json.jsons1 where jtag->'location' match 'jin$'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 0)

        self.tdRest.request("select * from  db_json.jsons1 where jtag->'location' match 'jin'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 3)

        self.tdRest.request("select * from  db_json.jsons1 where datastr match 'json' and jtag->'location' match 'jin'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 3)

        self.tdRest.request("select * from  db_json.jsons1 where jtag->'num' match '5'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 0)

        # test json string parse
        self.tdRest.error("create table if not exists  db_json.jsons1_5 using  db_json.jsons1 tags('efwewf')")
        self.tdRest.request("create table if not exists  db_json.jsons1_5 using  db_json.jsons1 tags('\t')")
        self.tdRest.request("create table if not exists  db_json.jsons1_6 using  db_json.jsons1 tags('')")

        self.tdRest.request("select jtag from  db_json.jsons1_6")
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], None)

        self.tdRest.request("create table if not exists  db_json.jsons1_7 using  db_json.jsons1 tags('{}')")
        self.tdRest.request("select jtag from  db_json.jsons1_7")
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], None)

        self.tdRest.request("create table if not exists  db_json.jsons1_8 using  db_json.jsons1 tags('null')")
        self.tdRest.request("select jtag from  db_json.jsons1_8")
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], None)

        self.tdRest.request("create table if not exists  db_json.jsons1_9 using  db_json.jsons1 tags('{\"\":4,\"time\":null}')")
        self.tdRest.request("select jtag from  db_json.jsons1_9")
        self.tdSql.checkEqual(json.dumps(self.tdRest.resp["data"][0][0]), "{\"time\": null}")

        self.tdRest.request("create table if not exists  db_json.jsons1_10 using  db_json.jsons1 tags('{\"k1\":\"\",\"k1\":\"v1\",\"k2\":true,\"k3\":false,\"k4\":55}')")
        self.tdRest.request("select jtag from  db_json.jsons1_10")
        self.tdSql.checkEqual(json.dumps(self.tdRest.resp["data"][0][0]), "{\"k1\": \"\", \"k2\": true, \"k3\": false, \"k4\": 55}")

        self.tdRest.request("select jtag->'k2' from  db_json.jsons1_10")
        self.tdSql.checkEqual(json.dumps(self.tdRest.resp["data"][0][0]), "true")

        self.tdRest.request("select jtag from db_json.jsons1 where jtag->'k1'=''")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1)

        self.tdRest.request("select jtag from db_json.jsons1 where jtag->'k2'=true")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1)

        self.tdRest.request("select jtag from db_json.jsons1 where jtag is null")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 4)

        self.tdRest.request("select jtag from db_json.jsons1 where jtag is not null")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 6)

        self.tdRest.request("select * from  db_json.jsons1 where jtag->'location' is not null")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 3)

        self.tdRest.request("select tbname,jtag from  db_json.jsons1 where jtag->'location' is null")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 7)

        self.tdRest.request("select * from  db_json.jsons1 where jtag->'num' is not null")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 2)

        self.tdRest.request("select * from  db_json.jsons1 where jtag->'location'='null'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 0)

        self.tdRest.request("select * from  db_json.jsons1 where jtag->'num'=null")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 0)

        # test distinct
        self.tdRest.request("select distinct jtag from  db_json.jsons1")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 7)

        self.tdRest.request("select distinct jtag->'location' from  db_json.jsons1")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 2)

        # test chinese
        self.tdRest.request("create table if not exists  db_json.jsons1_11 using  db_json.jsons1 tags('{\"k1\":\"中国\",\"k5\":\"是是是\"}')")

        self.tdRest.request("select tbname,jtag from  db_json.jsons1 where jtag->'k1' match '中'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1)

        self.tdRest.request("select tbname,jtag from  db_json.jsons1 where jtag->'k1'='中国'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1)

        #test dumplicate key with normal colomn
        self.tdRest.request("INSERT INTO  db_json.jsons1_12 using  db_json.jsons1 tags('{\"tbname\":\"tt\",\"databool\":true,\"dataStr\":\"是是是\"}') values(now, 4, \"你就会\")")

        self.tdRest.request("select *,tbname,jtag from  db_json.jsons1 where jtag->'dataStr' match '是'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1)

        self.tdRest.request("select tbname,jtag->'tbname' from  db_json.jsons1 where jtag->'tbname'='tt'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1)

        # test  filter : and /or / in/ like
        self.tdRest.request("select * from db_json.jsons1 where jtag->'num' is not null or jtag contains 'class' and jtag contains 'databool'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 2)

        self.tdRest.request("select * from db_json.jsons1 where jtag->'num' is not null and jtag contains 'class' or jtag contains 'databool'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1)
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][1], 4)

        self.tdRest.request("select * from db_json.jsons1 where jtag->'num' is not null or jtag contains 'class' and jtag contains 'databool' and jtag->'k1' match '中'  and  jtag->'location' like 'bei%'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 2)

        self.tdRest.request("select * from db_json.jsons1 where datastr like '你就会' and ( jtag->'num' is not null or jtag contains 'tbname' and jtag contains 'databool' )")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1)
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][1], 4)

        self.tdRest.error("select * from db_json.jsons1 where datastr like '你就会' and jtag->'num' is not null or jtag contains 'class' and jtag contains 'databool'")

        self.tdRest.error("select * from db_json.jsons1 where datastr like '你就会' or jtag->'num' is not null or jtag contains 'class' and jtag contains 'databool' and jtag->'k1' match '中' or  jtag->'location' in ('beijing')  and  jtag->'location' like 'bei%' ")

        self.tdRest.request("select * from db_json.jsons1 where datastr like '你就会' and (jtag->'num' is not null or jtag contains 'class' and jtag contains 'databool' and jtag->'k1' match '中' and  jtag->'location' like 'bei%' )")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 0)
      
        self.tdRest.error("select *,tbname,jtag from  db_json.jsons1 where dataBool=true")

        # test error
        self.tdRest.error("create table if not exists  db_json.jsons1_13 using  db_json.jsons1 tags(3333)")
        self.tdRest.request("create table if not exists  db_json.jsons1_13 using  db_json.jsons1 tags('{\"1loc\":\"fff\",\";id\":5}')")
        self.tdRest.error("create table if not exists  db_json.jsons1_13 using  db_json.jsons1 tags('{\"。loc\":\"fff\",\"fsd\":5}')")
        self.tdRest.error("create table if not exists  db_json.jsons1_13 using  db_json.jsons1 tags('{\"试试\":\"fff\",\";id\":5}')")
        self.tdRest.error("insert into  jsons1_13 using  db_json.jsons1 tags(3)")

        # test  query  normal column,tag and tbname 
        self.tdRest.request("create stable if not exists  db_json.jsons3(ts timestamp, dataInt3 int, dataBool3  bool, dataStr3 nchar(50)) tags(jtag3 json)")
        self.tdRest.request("create table db_json.jsons3_2 using  db_json.jsons3 tags('{\"t\":true,\"t123\":123,\"\":\"true\"}')")
        
        self.tdRest.request("create table db_json.jsons3_3 using  db_json.jsons3 tags('{\"t\":true,\"t123\":456,\"k1\":true,\"str1\":\"111\"}')")
        self.tdRest.request("insert into db_json.jsons3_3 values(now, 4, true, 'test')")

        self.tdRest.request("insert into db_json.jsons3_4 using  db_json.jsons3 tags('{\"t\":true,\"t123\":789,\"k1\":false,\"s\":null,\"str1\":\"112\"}')  values(now, 5, true, 'test')")
        self.tdRest.request("select * from  db_json.jsons3 where jtag3->'k1'=true")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1)
        self.tdRest.error("select  jtag3->k1 from  db_json.jsons3 ")
        self.tdRest.error("select  jtag3 from  db_json.jsons3 where jtag3->'k1'")
        self.tdRest.error("select  jtag3 from  db_json.jsons3 where jtag3 contains 'k1'=true")
        self.tdRest.error("select  jtag3 contains 'k1' from  db_json.jsons3;")
        self.tdRest.error("select  jtag3 contains 'k1'=true from  db_json.jsons3;")
        self.tdRest.error("select  jtag3->'k1'=true from  db_json.jsons3;")
        self.tdRest.request("insert into db_json.jsons3_5 using  db_json.jsons3 tags('{\"t\":true,\"t123\":789,\"k1\":123,\"s\":null}')  values(now, 5, true, 'test')")
        self.tdRest.request("insert into db_json.jsons3_5 using  db_json.jsons3 tags('{\"t\":true,\"t123\":012,\"k2\":null,\"s\":null}')  values(now+1s, 5, true, 'test')")
        self.tdRest.request("select jtag3 from  db_json.jsons3_5")
        self.tdSql.checkEqual(json.dumps(self.tdRest.resp["data"][0][0]), '{\"t\": true, \"t123\": 789, \"k1\": 123, \"s\": null}')        
        self.tdRest.request("insert into db_json.jsons3_6 using  db_json.jsons3 tags('{\"t\":true,\"t123\":789,\"k1\":false,\"s\":null}')  values(now, 5, true, 'test')")
        self.tdRest.request("select jtag3 from db_json.jsons3 where jtag3->'t123'=12 or jtag3 contains 'k1'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 4)
        self.tdRest.request("select distinct jtag3 from db_json.jsons3 where jtag3->'t123'=12 or jtag3 contains 'k1'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 4)

        self.tdRest.request("INSERT INTO  db_json.jsons1_14 using  db_json.jsons1 tags('{\"tbname\":\"tt\",\"location\":\"tianjing\",\"dataStr\":\"是是是\"}') values(now,5, \"你就会\")")

        self.tdRest.request("select ts,jtag->'tbname',tbname from  db_json.jsons1 where dataint>=1 and jtag contains 'tbname'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 2)
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][1], "tt")

        self.tdRest.request("select ts,jtag->'tbname',jtag->'location',tbname from  db_json.jsons1 where dataint between 1 and 5 and jtag->'location'='tianjing'")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1)
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][3], 'jsons1_14')

        self.tdRest.request("select ts,jtag3->'tbname', jtag3->'str1',tbname from  db_json.jsons3 where jtag3->'t123'  between 456 and 789 and jtag3->'str1' like '11%' ")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 2)
        for i in range(1):
            if self.tdRest.resp["data"][i][1] == 'jsons3_3':
                self.tdSql.checkEqual(self.tdRest.resp["data"][i][i], 111)

        self.tdRest.request("select  jtag3->'',dataint3 from  db_json.jsons3")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 5)
        for i in range(4):
            if self.tdRest.resp["data"][i][1] == 4:
                self.tdSql.checkEqual(self.tdRest.resp["data"][i][0], None)
        self.tdRest.request("select tbname,dataint3,jtag3->'k1' from db_json.jsons3;")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 5)
        for i in range(4):
            if self.tdRest.resp["data"][i][1] == 4:
                self.tdSql.checkEqual(self.tdRest.resp["data"][i][2], True)

        # Select_exprs is SQL function -Aggregation function  , tests includes group by and order by 
 
        self.tdRest.request("select  avg(dataInt),count(dataint),sum(dataint) from db_json.jsons1 group by jtag->'location' order by jtag->'location';")
        self.tdSql.checkEqual(self.tdRest.resp["data"][2][3], "tianjing")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 3)
        for i in range(2):
            if self.tdRest.resp["data"][i][3] == "beijing":
                self.tdSql.checkEqual(self.tdRest.resp["data"][i][0], 1)
                self.tdSql.checkEqual(self.tdRest.resp["data"][i][1], 3)
        self.tdRest.error("select  avg(dataInt) as 123 ,count(dataint),sum(dataint)  from db_json.jsons1 group by jtag->'location' order by 123")
        self.tdRest.error("select avg(dataInt) as avgdata ,count(dataint),sum(dataint)  from db_json.jsons1 group by jtag->'location' order by  avgdata ;")
        self.tdRest.request("select  avg(dataInt),count(dataint),sum(dataint)   from db_json.jsons1 group by jtag->'location' order by ts;")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 3)
        self.tdRest.error("select  avg(dataInt),count(dataint),sum(dataint)   from db_json.jsons1 group by jtag->'age' order by tbname;")
        # notice,it should return error ****
        self.tdRest.error("select  avg(dataInt),count(dataint),sum(dataint)   from db_json.jsons1 group by jtag->'age' order by  jtag->'num' ;")
        self.tdRest.request("select  avg(dataInt),count(dataint),sum(dataint)   from db_json.jsons1 group by jtag->'age' order by  jtag->'age' ;")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 2)
        self.tdRest.error("select  avg(dataInt)   from db_json.jsons1 group by jtag->'location' order by dataInt;")
        self.tdRest.error("select  avg(dataInt),tbname   from  db_json.jsons1 group by jtag->'location' order by tbname;")
        self.tdRest.request("create table if not exists  db_json.jsons1_15 using  db_json.jsons1 tags('{\"tbname\":\"tt\",\"location\":\"beijing\"}')")
        self.tdRest.request("insert into  jsons1_15 values(now+1s, 2, 'json1')")
        self.tdRest.error("select twa(dataint) from  db_json.jsons1 group by jtag->'location' order by jtag->'location';")
        self.tdRest.error("select  irate(dataint) from  db_json.jsons1 where jtag->'location' in ('beijing','tianjing') or jtag contains 'num' or jtag->'age'=35 ;")
        self.tdRest.request(" select stddev(dataint) from  db_json.jsons1 group by jtag->'location';")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 3)
        self.tdRest.request(" select stddev(dataint) from  db_json.jsons1  where  jtag->'location'='beijing';")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1)
        self.tdRest.error(" select LEASTSQUARES(dataint,1,2) from  db_json.jsons1_1 where  jtag->'location' ='beijing' ;")
        
        self.tdRest.request("select count(jtag) from  db_json.jsons1 ;")
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], 15) 
        self.tdRest.error("select count( jtag->'location'='beijing') from  db_json.jsons1 ;")
        self.tdRest.error("select count( jtag contains 'age') from  db_json.jsons1 ;")
        functionName = ['avg','twa','irate','stddev', 'stddev', 'leastsquares']
        for fn in functionName:
            self.tdRest.error("select %s( jtag) from  db_json.jsons1 ;"%fn)
            self.tdRest.error("select %s( jtag->'location'='beijing') from  db_json.jsons1 ;"%fn)
            self.tdRest.error("select %s( jtag contains 'age') from  db_json.jsons1 ;"%fn)            

        # Select_exprs is SQL function -Selection function

        self.tdRest.request(" select  min(dataint),jtag from  db_json.jsons1 where jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing' or jtag contains 'num' or jtag->'age'=35 ;")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1)
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], 1)
        self.tdRest.request(" select  max(dataint),jtag from  db_json.jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1)
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], 12)
        self.tdRest.request(" select  first(*) from  db_json.jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1)
        self.tdRest.request(" select  last(*) from  db_json.jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1)
        self.tdRest.error(" select  last(*),jtag from  db_json.jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        self.tdRest.request(" select  last_row(*) from  db_json.jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1)
        self.tdRest.request(" select  apercentile(dataint,0) from  db_json.jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1)
        self.tdRest.request(" select  apercentile(dataint,50) from  db_json.jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1)     
        self.tdRest.request(" select  apercentile(dataint,90) from  db_json.jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1)    
        self.tdRest.request(" select  apercentile(dataint,100) from  db_json.jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1)   
        self.tdRest.request(" select  apercentile(dataint,0,'t-digest') from  db_json.jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1)
        self.tdRest.request(" select  apercentile(dataint,50,'t-digest') from  db_json.jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1) 
        self.tdRest.request(" select  apercentile(dataint,100,'t-digest') from  db_json.jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1)   
        self.tdRest.request("select  top(dataint,1)  from  db_json.jsons1 group by jtag->'location';")
        self.tdRest.request("select  tbname,top(dataint,1)  from  db_json.jsons1 group by jtag->'location' order by jtag->'location' asc;")
        self.tdRest.request("select  tbname,top(dataint,1)  from  db_json.jsons1 group by jtag->'location' order by jtag->'location'  desc;")
        self.tdRest.request("select  top(dataint,1)  from  db_json.jsons1 group by jtag->'location' order by  ts desc;")
        self.tdRest.request("select  top(dataint,1)  from  db_json.jsons1 group by jtag->'location' order by  ts asc;")
        self.tdRest.request("select  top(dataint,100)  from  db_json.jsons1 group by jtag->'location';")
        self.tdRest.request("select  bottom(dataint,1)  from  db_json.jsons1 group by jtag->'location';")
        self.tdRest.request("select  bottom(dataint,100)  from  db_json.jsons1 group by jtag->'location';")

        self.tdRest.request("create table if not exists db_json.jsons_interp(ts timestamp, dataInt int, dataBool bool, datafloat float, datadouble double,dataStr nchar(50)) tags(jtag json)")
        self.tdRest.request("insert into db_json.jsons_interp_1 using db_json.jsons_interp tags('{\"nv\":null,\"tea\":true,\"rate\":456,\"tea\":false}') values ('2021-07-25 02:19:54.119',2,'true',0.9,0.1,'123')")
        self.tdRest.request("insert into db_json.jsons_interp_1 values ('2021-07-25 02:19:54.219',3,'true',-4.8,-5.5,'123') ")
        self.tdRest.request("insert into db_json.jsons_interp_2 using db_json.jsons_interp tags('{\"nv\":null,\"tea\":true,\"level\":\"123456\",\"rate\":123,\"tea\":false}') values ('2021-07-25 02:19:54.319',4,'true',0.9,0.1,'123')")
        self.tdRest.request("insert into db_json.jsons_interp_2 values ('2021-07-25 02:19:54.419',5,'true',-5.1,1.3,'123') ")
        self.tdRest.request("select  interp(dataint) as itd from db_json.jsons_interp where (jtag->'rate'=123 or jtag->'rate'=456)   and ts >= '2021-07-25 02:19:53.19' and ts<= '2021-07-25 02:19:54.519'  every(100a) group by tbname order by ts desc ;")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 4)   
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][1], 3)
        self.tdSql.checkEqual(self.tdRest.resp["data"][2][1], 5)

        self.tdRest.request("select  interp(dataint) as itd from db_json.jsons_interp where (jtag->'rate'=123 or jtag->'rate'=456)   and ts >= '2021-07-25 02:19:53.19' and ts<= '2021-07-25 02:19:54.519'  every(100a) group by tbname order by tbname asc;")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 4)   
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][1], 2)
        self.tdSql.checkEqual(self.tdRest.resp["data"][2][1], 4)
        #error
        functionName = ['min','max','last','TOP','last_row','bottom','apercentile','interp']
        for fn in functionName:
            self.tdRest.error("select %s( jtag) from  db_json.jsons1 ;"%fn)
            self.tdRest.error("select %s( jtag->'location'='beijing') from  db_json.jsons1 ;"%fn)
            self.tdRest.error("select %s( jtag contains 'age') from  db_json.jsons1 ;"%fn)         

        #  Select_exprs is SQL function -Calculation  function
        self.tdRest.error(" select  diff(dataint) from  db_json.jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        self.tdRest.error(" select  Derivative(dataint) from  db_json.jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        self.tdRest.request(" select  SPREAD(dataint) from  db_json.jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], 11) 
        self.tdRest.request(" select  ceil(dataint) from  db_json.jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        self.tdRest.request(" select  floor(dataint) from  db_json.jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        self.tdRest.request(" select  round(dataint) from  db_json.jsons1 where  jtag->'location'= 'beijing' or  jtag->'location'= 'tianjing'or jtag contains 'num' or jtag->'age'=35 ;")
        # need insert new data --data type is double or float and tests ceil floor round .
        self.tdRest.request("create table if not exists db_json.jsons7(ts timestamp, dataInt int, dataBool bool, datafloat float, datadouble double,dataStr nchar(50)) tags(jtag json)")
        self.tdRest.request("insert into db_json.jsons7_1 using db_json.jsons7 tags('{\"nv\":null,\"tea\":true,\"\":false,\" \":123,\"tea\":false}') values (now+2s,2,'true',0.9,0.1,'123')")
        self.tdRest.request("insert into db_json.jsons7_1 using db_json.jsons7 tags('{\"nv\":null,\"tea\":true,\"tea\":false}') values (now+3s,2,'true',0.9,0.1,'123')")
        self.tdRest.request("select * from db_json.jsons7 where jtag->'tea'=0 ;")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 0)
        self.tdRest.request("select * from jsons7 where jtag->'tea'=3;")
        # self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 0)
        self.tdRest.request("insert into db_json.jsons7_1 values (now+1s,3,'true',-4.8,-5.5,'123') ")
        self.tdRest.request("insert into db_json.jsons7_1 values (now+2s,4,'true',1.9998,2.00001,'123') ")
        self.tdRest.request("insert into db_json.jsons7_2 using jsons7 tags('{\"nv\":null,\"tea\":true,\"\":false,\"tag\":123,\"tea\":false}') values (now,5,'true',4.01,2.2,'123') ")
        self.tdRest.request("insert into db_json.jsons7_2 using jsons7 tags('{\"nv\":null,\"tea\":true,\"tag\":123,\"tea\":false}') values (now+5s,5,'false',4.01,2.2,'123') ")
        self.tdRest.request("insert into db_json.jsons7_2 (ts,datadouble) values (now+3s,-0.9) ")
        self.tdRest.request("insert into db_json.jsons7_2 (ts,datadouble) values (now+4s,-2.9) ")
        self.tdRest.request("insert into db_json.jsons7_2 (ts,datafloat) values (now+1s,-0.9) ")
        self.tdRest.request("insert into db_json.jsons7_2 (ts,datafloat) values (now+2s,-1.9) ")
        self.tdRest.request("CREATE TABLE if not exists db_json.jsons7_3 using db_json.jsons7 tags('{\"nv\":null,\"tea\":true,\"\":false,\"tag\":4569,\"tea\":false}') ")
        self.tdRest.request("select ts,ceil(dataint),ceil(datafloat),ceil(datadouble) from db_json.jsons7 where jtag contains 'tea';")
        self.tdRest.request("select ceil(dataint),ceil(datafloat),ceil(datadouble) from db_json.jsons7 where jtag contains 'tea';")
        self.tdRest.request("select ts,floor(dataint),floor(datafloat),floor(datadouble) from db_json.jsons7 where jtag contains 'tea';")
        self.tdRest.request("select floor(dataint),floor(datafloat),floor(datadouble) from db_json.jsons7 where jtag contains 'tea';")
        self.tdRest.request("select ts,round(dataint),round(datafloat),round(datadouble) from db_json.jsons7 where jtag contains 'tea';")
        self.tdRest.request("select round(dataint),round(datafloat),round(datadouble) from db_json.jsons7 where jtag contains 'tea';")

        functionName = ['diff','Derivative','SPREAD','ceil','round','floor']
        for fn in functionName:
            self.tdRest.error("select %s( jtag) from  db_json.jsons1 ;"%fn)
            self.tdRest.error("select %s( jtag->'location'='beijing') from  db_json.jsons1 ;"%fn)
            self.tdRest.error("select %s( jtag contains 'age') from  db_json.jsons1 ;"%fn)       


        #modify one same key and diffirent data type,include negative number of double  
        self.tdRest.request("insert into db_json.jsons7_4 using db_json.jsons7 tags('{\"nv\":null,\"tea\":123,\"tag\":123,\"tea\":false}') values (now+1s,5,'true',4.01,2.2,'abc'); ")
        self.tdRest.request("insert into db_json.jsons7_5 using db_json.jsons7 tags('{\"nv\":null,\"tea\":\"app\",\"tag\":123,\"tea\":false}') values (now+2s,5,'true',4.01,2.2,'abc'); ")
        self.tdRest.error("insert into db_json.jsons7_6 using db_json.jsons7 tags('{\"nv\":null,\"tea\":-1.111111111111111111111111111111111111111111111111111111111111111111111,\"tag\":123,\"tea\":false}') values (now+3s,5,'true',4.01,2.2,'123'); ")
        self.tdRest.request("insert into db_json.jsons7_6 using db_json.jsons7 tags('{\"nv\":null,\"tea\":-1.111111111,\"tag\":123,\"tea\":false}') values (now,5,'false',4.01,2.2,'t123'); ")
        self.tdRest.request("select  jtag from db_json.jsons7 where jtag->'tea'<-1.01;")
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 1)   
        
        # test join
        self.tdRest.request("create table if not exists db_json.jsons6(ts timestamp, dataInt int, dataBool bool, dataStr nchar(50)) tags(jtag json)")
        self.tdRest.request("create table if not exists db_json.jsons5(ts timestamp, dataInt int, dataBool bool, dataStr nchar(50)) tags(jtag json)")
        self.tdRest.request("CREATE TABLE if not exists db_json.jsons6_1 using db_json.jsons6 tags('{\"loc\":\"fff\",\"id\":6,\"user\":\"ffc\"}')")
        self.tdRest.request("CREATE TABLE if not exists db_json.jsons6_2 using db_json.jsons6 tags('{\"loc\":\"ffc\",\"id\":5}')")
        self.tdRest.request("insert into db_json.jsons6_1 values ('2020-04-18 15:00:00.000', 1, false, 'json1')")
        self.tdRest.request("insert into db_json.jsons6_2 values ('2020-04-18 15:00:01.000', 2, false, 'json1')")
        self.tdRest.request("insert into db_json.jsons5_1 using db_json.jsons5 tags('{\"loc\":\"fff\",\"num\":5,\"location\":\"beijing\"}') values ('2020-04-18 15:00:00.000', 2, true, 'json2')")
        self.tdRest.request("insert into db_json.jsons5_2 using db_json.jsons5 tags('{\"loc\":\"fff\",\"id\":5,\"location\":\"beijing\"}') values ('2020-04-18 15:00:01.000', 2, true, 'json2')")
        self.tdRest.error("select 'sss',33,a.jtag->'loc' from db_json.jsons6 a,db_json.jsons5 b where a.ts=b.ts and a.jtag->'loc'=b.jtag->'loc'")
        self.tdRest.error("select 'sss',33,a.jtag->'loc' from db_json.jsons6 a,db_json.jsons5 b where a.ts=b.ts and a.jtag->'user'=b.jtag->'loc';")
        self.tdRest.request("select 'sss',33,a.jtag->'loc' from db_json.jsons6 a,db_json.jsons5 b where a.ts=b.ts and a.jtag->'id'=b.jtag->'id'")
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], "sss")
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][2], "ffc")



        # #nested query 
        self.tdRest.error("select jtag->'tag' from (select tbname,jtag,ts,ceil(dataint) as cdata,ceil(datafloat) ,ceil(datadouble) from db_json.jsons7 where jtag contains 'tea') where cdata=3 ") # not currently supported
        self.tdRest.error("select jtag from (select tbname,jtag,ts,ceil(dataint) as cdata,ceil(datafloat) ,ceil(datadouble) from db_json.jsons7 where jtag contains 'tea') where jtag->'tag'=123 ")  # not currently supported
        self.tdRest.request("select * from (select tbname,jtag->'tea',ts,ceil(dataint) as cdata,ceil(datafloat) ,ceil(datadouble) from db_json.jsons7 where jtag contains 'tea') where cdata=5 ")
        # self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 5)
        # for i in range(5):
        #     if self.tdRest.resp["data"][i][0] == 'jsons7_4':
        #         self.tdSql.checkEqual(self.tdRest.resp["data"][i][1], 123)
        #         self.tdSql.checkEqual(self.tdRest.resp["data"][i][3], 5)
            # if self.tdRest.resp["data"][i][0] == 'jsons7_5':
            #     self.tdSql.checkEqual(self.tdRest.resp["data"][i][3], "app")

        # query  child table 
        self.tdRest.error("select * from  db_json.jsons3_2 where jtag3->'k1'=true;")

        # union all :max times is 100
        unioSql = "select ts,jtag->'tbname',jtag->'location',tbname from  db_json.jsons1 where dataint between 1 and 5 and jtag->'location'='tianjing' union all "
        for i in range(99):
            if (i < 98):
                unioSql +=  "select ts,jtag->'tbname',jtag->'location',tbname from  db_json.jsons1 where dataint between 1 and 5 and jtag->'location'='tianjing' union all "
            else:
                unioSql +=  " select ts,jtag->'tbname',jtag->'location',tbname from  db_json.jsons1 where dataint between 1 and 5 and jtag->'location'='tianjing'"
        self.tdRest.request(unioSql)
        self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 100)
        unioSql +=  " union all   select ts,jtag->'tbname',jtag->'location',tbname from  db_json.jsons1 where dataint between 1 and 5 and jtag->'location'='tianjing'"
        self.tdRest.error(unioSql)
        

        # fuction testcase : stddev, supported data type: int\str\bool unsupported data type: float\double
        # self.tdRest.request(" select stddev(datafloat),dataint from db_json.jsons7 group by dataint;")
        # self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 5)
        # self.tdRest.request(" select stddev(dataint) from db_json.jsons7 group by datastr;")
        # self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 4)
        # self.tdRest.request(" select stddev(dataint) from db_json.jsons7 group by databool;")
        # self.tdSql.checkEqual(int(self.tdRest.resp["rows"]), 3)
        self.tdRest.error(" select stddev(dataint) from db_json.jsons7 group by datafloat;")
        self.tdRest.error(" select stddev(dataint) from db_json.jsons7 group by datadouble;")
        self.tdRest.request("create table if not exists db_json.jsons8(ts timestamp, dataInt int, dataBool bool, datafloat float, datadouble double,dataStr nchar(50),datatime timestamp) tags(jtag json)")
        self.tdRest.request("insert into db_json.jsons8_1 using db_json.jsons8 tags('{\"nv\":null,\"tea\":true,\"\":false,\" \":123,\"tea\":false}') values (now,2,'true',0.9,0.1,'abc',now+60s)")
        self.tdRest.request("insert into db_json.jsons8_2 using db_json.jsons8 tags('{\"nv\":null,\"tea\":true,\"\":false,\" \":123,\"tea\":false}') values (now+5s,2,'true',0.9,0.1,'abc',now+65s)")
        self.tdRest.request(" select stddev(dataint) from db_json.jsons8 group by datatime;")
        self.tdRest.error(" select stddev(datatime) from db_json.jsons8 group by datadouble;")

    def cleanup(self):
        pass
        
    def desc(self) -> str:
        case_description = '''
            insert_after_alter_column <jayden>: [TD-12748] : insert after alter column;
        '''
        return case_description

    def author(self) -> str:
        return "Jayden"
    
    def tags(self):
        return T.Write.RestfulSql.Insert.JsonTag

