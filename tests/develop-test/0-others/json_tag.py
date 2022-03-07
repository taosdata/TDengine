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
import json


class TDTestCase:
    def caseDescription(self):
        '''
        Json tag test case, include create table with json tag, select json tag and query with json tag in where condition, besides, include json tag in group by/order by/join/subquery.
        case1: [TD-12452] fix error if json tag is NULL
        case2: [TD-12389] describe child table, tag length error if the tag is json tag
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        print("============== STEP 1 ===== prepare data & validate json string")
        tdSql.error("create table if not exists jsons1(ts timestamp, dataInt int, dataBool bool, dataStr nchar(50), dataStrBin binary(150)) tags(jtag json, tagint int)")
        tdSql.error("create table if not exists jsons1(ts timestamp, data json) tags(tagint int)")
        tdSql.execute("create table if not exists jsons1(ts timestamp, dataInt int, dataBool bool, dataStr nchar(50), dataStrBin binary(150)) tags(jtag json)")
        tdSql.execute("insert into jsons1_1 using jsons1 tags('{\"tag1\":\"fff\",\"tag2\":5, \"tag3\":true}') values(1591060618000, 1, false, 'json1', '你是') (1591060608000, 23, true, '等等', 'json')")
        tdSql.execute("insert into jsons1_2 using jsons1 tags('{\"tag1\":5,\"tag2\":\"beijing\"}') values (1591060628000, 2, true, 'json2', 'sss')")
        tdSql.execute("insert into jsons1_3 using jsons1 tags('{\"tag1\":false,\"tag2\":\"beijing\"}') values (1591060668000, 3, false, 'json3', 'efwe')")
        tdSql.execute("insert into jsons1_4 using jsons1 tags('{\"tag1\":null,\"tag2\":\"shanghai\",\"tag3\":\"hello\"}') values (1591060728000, 4, true, 'json4', '323sd')")
        tdSql.execute("insert into jsons1_5 using jsons1 tags('{\"tag1\":1.232, \"tag2\":null}') values(1591060928000, 1, false, '你就会', 'ewe')")
        tdSql.execute("insert into jsons1_6 using jsons1 tags('{\"tag1\":11,\"tag2\":\"\",\"tag2\":null}') values(1591061628000, 11, false, '你就会','')")
        tdSql.execute("insert into jsons1_7 using jsons1 tags('{\"tag1\":\"收到货\",\"tag2\":\"\",\"tag3\":null}') values(1591062628000, 2, NULL, '你就会', 'dws')")

        # test duplicate key using the first one. elimate empty key
        tdSql.execute("CREATE TABLE if not exists jsons1_8 using jsons1 tags('{\"tag1\":null, \"tag1\":true, \"tag1\":45, \"1tag$\":2, \" \":90, \"\":32}')")
        tdSql.query("select jtag from jsons1_8")
        tdSql.checkData(0, 0, '{"tag1":null,"1tag$":2," ":90}')

        # test empty json string, save as jtag is NULL
        tdSql.execute("insert into jsons1_9  using jsons1 tags('\t') values (1591062328000, 24, NULL, '你就会', '2sdw')")
        tdSql.execute("CREATE TABLE if not exists jsons1_10 using jsons1 tags('')")
        tdSql.execute("CREATE TABLE if not exists jsons1_11 using jsons1 tags(' ')")
        tdSql.execute("CREATE TABLE if not exists jsons1_12 using jsons1 tags('{}')")
        tdSql.execute("CREATE TABLE if not exists jsons1_13 using jsons1 tags('null')")

        # test invalidate json
        tdSql.error("CREATE TABLE if not exists jsons1_14 using jsons1 tags('\"efwewf\"')")
        tdSql.error("CREATE TABLE if not exists jsons1_14 using jsons1 tags('3333')")
        tdSql.error("CREATE TABLE if not exists jsons1_14 using jsons1 tags('33.33')")
        tdSql.error("CREATE TABLE if not exists jsons1_14 using jsons1 tags('false')")
        tdSql.error("CREATE TABLE if not exists jsons1_14 using jsons1 tags('[1,true]')")
        tdSql.error("CREATE TABLE if not exists jsons1_14 using jsons1 tags('{222}')")
        tdSql.error("CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"fe\"}')")

        # test invalidate json key, key must can be printed assic char
        tdSql.error("CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"tag1\":[1,true]}')")
        tdSql.error("CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"tag1\":{}}')")
        tdSql.error("CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"。loc\":\"fff\"}')")
        tdSql.error("CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"\t\":\"fff\"}')")
        tdSql.error("CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"试试\":\"fff\"}')")

        # test invalidate json value, value number can not be inf,nan TD-12166
        tdSql.error("CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"k\":1.8e308}')")
        tdSql.error("CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"k\":-1.8e308}')")

        #test length limit
        char1= ''.join(['abcd']*64)
        char3= ''.join(['abcd']*1022)
        print(len(char3))   # 4088
        tdSql.error("CREATE TABLE if not exists  jsons1_15 using  jsons1 tags('{\"%s1\":5}')" % char1)   # len(key)=257
        tdSql.execute("CREATE TABLE if not exists  jsons1_15 using  jsons1 tags('{\"%s\":5}')" % char1)  # len(key)=256
        tdSql.error("CREATE TABLE if not exists  jsons1_16 using  jsons1 tags('{\"TS\":\"%s\"}')" % char3)   # len(object)=4097
        tdSql.execute("CREATE TABLE if not exists  jsons1_16 using  jsons1 tags('{\"T\":\"%s\"}')" % char3)  # len(object)=4096
        tdSql.execute("drop table if exists jsons1_15")
        tdSql.execute("drop table if exists jsons1_16")

        print("============== STEP 2 ===== alter table json tag")
        tdSql.error("ALTER STABLE jsons1 add tag tag2 nchar(20)")
        tdSql.error("ALTER STABLE jsons1 drop tag jtag")
        tdSql.error("ALTER TABLE jsons1 MODIFY TAG jtag nchar(128)")

        tdSql.execute("ALTER TABLE jsons1_1 SET TAG jtag='{\"tag1\":\"femail\",\"tag2\":35,\"tag3\":true}'")
        tdSql.query("select jtag from jsons1_1")
        tdSql.checkData(0, 0, '{"tag1":"femail","tag2":35,"tag3":true}')
        tdSql.execute("ALTER TABLE jsons1 CHANGE TAG jtag jtag_new")
        tdSql.execute("ALTER TABLE jsons1 CHANGE TAG jtag_new jtag")

        print("============== STEP 3 ===== query table")
        # test error syntax
        tdSql.error("select * from jsons1 where jtag->tag1='beijing'")
        tdSql.error("select * from jsons1 where jtag->'location'")
        tdSql.error("select * from jsons1 where jtag->''")
        tdSql.error("select * from jsons1 where jtag->''=9")
        tdSql.error("select -> from jsons1")
        tdSql.error("select * from jsons1 where contains")
        tdSql.error("select * from jsons1 where jtag->")
        tdSql.error("select jtag->location from jsons1")
        tdSql.error("select jtag contains location from jsons1")
        tdSql.error("select * from jsons1 where jtag contains location")
        tdSql.error("select * from jsons1 where jtag contains''")
        tdSql.error("select * from jsons1 where jtag contains 'location'='beijing'")

        # test function error
        tdSql.error("select avg(jtag->'tag1') from jsons1")
        tdSql.error("select avg(jtag) from jsons1")
        tdSql.error("select min(jtag->'tag1') from jsons1")
        tdSql.error("select min(jtag) from jsons1")
        tdSql.error("select ceil(jtag->'tag1') from jsons1")
        tdSql.error("select ceil(jtag) from jsons1")

        # test select normal column
        tdSql.query("select dataint from jsons1")
        tdSql.checkRows(9)
        tdSql.checkData(1, 0, 1)

        # test select json tag
        tdSql.query("select * from jsons1")
        tdSql.checkRows(9)
        tdSql.query("select jtag from jsons1")
        tdSql.checkRows(13)
        tdSql.query("select jtag from jsons1 where jtag is null")
        tdSql.checkRows(5)
        tdSql.query("select jtag from jsons1 where jtag is not null")
        tdSql.checkRows(8)

        # test jtag is NULL
        tdSql.query("select jtag from jsons1_9")
        tdSql.checkData(0, 0, None)

        # test select json tag->'key', value is string
        tdSql.query("select jtag->'tag1' from jsons1_1")
        tdSql.checkData(0, 0, '"femail"')
        tdSql.query("select jtag->'tag2' from jsons1_6")
        tdSql.checkData(0, 0, '""')
        # test select json tag->'key', value is int
        tdSql.query("select jtag->'tag2' from jsons1_1")
        tdSql.checkData(0, 0, 35)
        # test select json tag->'key', value is bool
        tdSql.query("select jtag->'tag3' from jsons1_1")
        tdSql.checkData(0, 0, "true")
        # test select json tag->'key', value is null
        tdSql.query("select jtag->'tag1' from jsons1_4")
        tdSql.checkData(0, 0, "null")
        # test select json tag->'key', value is double
        tdSql.query("select jtag->'tag1' from jsons1_5")
        tdSql.checkData(0, 0, "1.232000000")
        # test select json tag->'key', key is not exist
        tdSql.query("select jtag->'tag10' from jsons1_4")
        tdSql.checkData(0, 0, None)

        tdSql.query("select jtag->'tag1' from jsons1")
        tdSql.checkRows(13)
        # test header name
        res = tdSql.getColNameList("select jtag->'tag1' from jsons1")
        cname_list = []
        cname_list.append("jtag->'tag1'")
        tdSql.checkColNameList(res, cname_list)



        # test where with json tag
        tdSql.error("select * from jsons1_1 where jtag is not null")
        tdSql.error("select * from jsons1 where jtag='{\"tag1\":11,\"tag2\":\"\"}'")
        tdSql.error("select * from jsons1 where jtag->'tag1'={}")

        # where json value is string
        tdSql.query("select * from jsons1 where jtag->'tag2'='beijing'")
        tdSql.checkRows(2)
        tdSql.query("select dataint,tbname,jtag->'tag1',jtag from jsons1 where jtag->'tag2'='beijing'")
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(0, 1, 'jsons1_2')
        tdSql.checkData(0, 2, 5)
        tdSql.checkData(0, 3, '{"tag1":5,"tag2":"beijing"}')
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(1, 1, 'jsons1_3')
        tdSql.checkData(1, 2, 'false')
        tdSql.query("select * from jsons1 where jtag->'tag1'='beijing'")
        tdSql.checkRows(0)
        tdSql.query("select * from jsons1 where jtag->'tag1'='收到货'")
        tdSql.checkRows(1)
        tdSql.query("select * from jsons1 where jtag->'tag2'>'beijing'")
        tdSql.checkRows(1)
        tdSql.query("select * from jsons1 where jtag->'tag2'>='beijing'")
        tdSql.checkRows(3)
        tdSql.query("select * from jsons1 where jtag->'tag2'<'beijing'")
        tdSql.checkRows(2)
        tdSql.query("select * from jsons1 where jtag->'tag2'<='beijing'")
        tdSql.checkRows(4)
        tdSql.query("select * from jsons1 where jtag->'tag2'!='beijing'")
        tdSql.checkRows(3)
        tdSql.query("select * from jsons1 where jtag->'tag2'=''")
        tdSql.checkRows(2)

        # where json value is int
        tdSql.query("select * from jsons1 where jtag->'tag1'=5")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 2)
        tdSql.query("select * from jsons1 where jtag->'tag1'=10")
        tdSql.checkRows(0)
        tdSql.query("select * from jsons1 where jtag->'tag1'<54")
        tdSql.checkRows(3)
        tdSql.query("select * from jsons1 where jtag->'tag1'<=11")
        tdSql.checkRows(3)
        tdSql.query("select * from jsons1 where jtag->'tag1'>4")
        tdSql.checkRows(2)
        tdSql.query("select * from jsons1 where jtag->'tag1'>=5")
        tdSql.checkRows(2)
        tdSql.query("select * from jsons1 where jtag->'tag1'!=5")
        tdSql.checkRows(2)
        tdSql.query("select * from jsons1 where jtag->'tag1'!=55")
        tdSql.checkRows(3)

        # where json value is double
        tdSql.query("select * from jsons1 where jtag->'tag1'=1.232")
        tdSql.checkRows(1)
        tdSql.query("select * from jsons1 where jtag->'tag1'<1.232")
        tdSql.checkRows(0)
        tdSql.query("select * from jsons1 where jtag->'tag1'<=1.232")
        tdSql.checkRows(1)
        tdSql.query("select * from jsons1 where jtag->'tag1'>1.23")
        tdSql.checkRows(3)
        tdSql.query("select * from jsons1 where jtag->'tag1'>=1.232")
        tdSql.checkRows(3)
        tdSql.query("select * from jsons1 where jtag->'tag1'!=1.232")
        tdSql.checkRows(2)
        tdSql.query("select * from jsons1 where jtag->'tag1'!=3.232")
        tdSql.checkRows(3)
        tdSql.error("select * from jsons1 where jtag->'tag1'/0=3")
        tdSql.error("select * from jsons1 where jtag->'tag1'/5=1")

        # where json value is bool
        tdSql.query("select * from jsons1 where jtag->'tag1'=true")
        tdSql.checkRows(0)
        tdSql.query("select * from jsons1 where jtag->'tag1'=false")
        tdSql.checkRows(1)
        tdSql.query("select * from jsons1 where jtag->'tag1'!=false")
        tdSql.checkRows(0)
        tdSql.error("select * from jsons1 where jtag->'tag1'>false")

        # where json value is null
        tdSql.query("select * from jsons1 where jtag->'tag1'=null")     # only json suport =null. This synatx will change later.
        tdSql.checkRows(1)

        # where json is null
        tdSql.query("select * from jsons1 where jtag is null")
        tdSql.checkRows(1)
        tdSql.query("select * from jsons1 where jtag is not null")
        tdSql.checkRows(8)

        # where json key is null
        tdSql.query("select * from jsons1 where jtag->'tag_no_exist'=3")
        tdSql.checkRows(0)

        # where json value is not exist
        tdSql.query("select * from jsons1 where jtag->'tag1' is null")
        tdSql.checkData(0, 0, 'jsons1_9')
        tdSql.checkRows(1)
        tdSql.query("select * from jsons1 where jtag->'tag4' is null")
        tdSql.checkRows(9)
        tdSql.query("select * from jsons1 where jtag->'tag3' is not null")
        tdSql.checkRows(4)

        # test contains
        tdSql.query("select * from jsons1 where jtag contains 'tag1'")
        tdSql.checkRows(8)
        tdSql.query("select * from jsons1 where jtag contains 'tag3'")
        tdSql.checkRows(4)
        tdSql.query("select * from jsons1 where jtag contains 'tag_no_exist'")
        tdSql.checkRows(0)

        # test json tag in where condition with and/or
        tdSql.query("select * from jsons1 where jtag->'tag1'=false and jtag->'tag2'='beijing'")
        tdSql.checkRows(1)
        tdSql.query("select * from jsons1 where jtag->'tag1'=false or jtag->'tag2'='beijing'")
        tdSql.checkRows(2)
        tdSql.query("select * from jsons1 where jtag->'tag1'=false and jtag->'tag2'='shanghai'")
        tdSql.checkRows(0)
        tdSql.query("select * from jsons1 where jtag->'tag1'=false and jtag->'tag2'='shanghai'")
        tdSql.checkRows(0)
        tdSql.query("select * from jsons1 where jtag->'tag1'=13 or jtag->'tag2'>35")
        tdSql.checkRows(0)
        tdSql.query("select * from jsons1 where jtag->'tag1'=13 or jtag->'tag2'>35")
        tdSql.checkRows(0)
        tdSql.query("select * from jsons1 where jtag->'tag1' is not null and jtag contains 'tag3'")
        tdSql.checkRows(4)
        tdSql.query("select * from jsons1 where jtag->'tag1'='femail' and jtag contains 'tag3'")
        tdSql.checkRows(2)


        # test with between and
        tdSql.query("select * from jsons1 where jtag->'tag1' between 1 and 30")
        tdSql.checkRows(3)
        tdSql.query("select * from jsons1 where jtag->'tag1' between 'femail' and 'beijing'")
        tdSql.checkRows(2)

        # test with tbname/normal column
        tdSql.query("select * from jsons1 where tbname = 'jsons1_1'")
        tdSql.checkRows(2)
        tdSql.query("select * from jsons1 where tbname = 'jsons1_1' and jtag contains 'tag3'")
        tdSql.checkRows(2)
        tdSql.query("select * from jsons1 where tbname = 'jsons1_1' and jtag contains 'tag3' and dataint=3")
        tdSql.checkRows(0)
        tdSql.query("select * from jsons1 where tbname = 'jsons1_1' and jtag contains 'tag3' and dataint=23")
        tdSql.checkRows(1)


        # test where condition like
        tdSql.query("select *,tbname from jsons1 where jtag->'tag2' like 'bei%'")
        tdSql.checkRows(2)
        tdSql.query("select *,tbname from jsons1 where jtag->'tag1' like 'fe%' and jtag->'tag2' is not null")
        tdSql.checkRows(2)

        # test where condition in  no support in
        tdSql.error("select * from jsons1 where jtag->'tag1' in ('beijing')")

        # test where condition match/nmath
        tdSql.query("select * from jsons1 where jtag->'tag1' match 'ma'")
        tdSql.checkRows(2)
        tdSql.query("select * from jsons1 where jtag->'tag1' match 'ma$'")
        tdSql.checkRows(0)
        tdSql.query("select * from jsons1 where jtag->'tag2' match 'jing$'")
        tdSql.checkRows(2)
        tdSql.query("select * from jsons1 where jtag->'tag1' match '收到'")
        tdSql.checkRows(1)
        tdSql.query("select * from jsons1 where jtag->'tag1' nmatch 'ma'")
        tdSql.checkRows(1)

        # test distinct
        tdSql.execute("insert into jsons1_14 using jsons1 tags('{\"tag1\":\"收到货\",\"tag2\":\"\",\"tag3\":null}') values(1591062628000, 2, NULL, '你就会', 'dws')")
        tdSql.query("select distinct jtag->'tag1' from jsons1")
        tdSql.checkRows(8)
        tdSql.query("select distinct jtag from jsons1")
        tdSql.checkRows(9)

        #test dumplicate key with normal colomn
        tdSql.execute("INSERT INTO jsons1_15 using jsons1 tags('{\"tbname\":\"tt\",\"databool\":true,\"datastr\":\"是是是\"}') values(1591060828000, 4, false, 'jjsf', \"你就会\")")
        tdSql.query("select *,tbname,jtag from jsons1 where jtag->'datastr' match '是' and datastr match 'js'")
        tdSql.checkRows(1)
        tdSql.query("select tbname,jtag->'tbname' from jsons1 where jtag->'tbname'='tt' and tbname='jsons1_14'")
        tdSql.checkRows(0)
        
        # test join
        tdSql.execute("create table if not exists jsons2(ts timestamp, dataInt int, dataBool bool, dataStr nchar(50), dataStrBin binary(150)) tags(jtag json)")
        tdSql.execute("insert into jsons2_1 using jsons2 tags('{\"tag1\":\"fff\",\"tag2\":5, \"tag3\":true}') values(1591060618000, 2, false, 'json2', '你是2')")
        tdSql.execute("insert into jsons2_2 using jsons2 tags('{\"tag1\":5,\"tag2\":null}') values (1591060628000, 2, true, 'json2', 'sss')")

        tdSql.execute("create table if not exists jsons3(ts timestamp, dataInt int, dataBool bool, dataStr nchar(50), dataStrBin binary(150)) tags(jtag json)")
        tdSql.execute("insert into jsons3_1 using jsons3 tags('{\"tag1\":\"fff\",\"tag2\":5, \"tag3\":true}') values(1591060618000, 3, false, 'json3', '你是3')")
        tdSql.execute("insert into jsons3_2 using jsons3 tags('{\"tag1\":5,\"tag2\":\"beijing\"}') values (1591060638000, 2, true, 'json3', 'sss')")
        tdSql.query("select 'sss',33,a.jtag->'tag3' from jsons2 a,jsons3 b where a.ts=b.ts and a.jtag->'tag1'=b.jtag->'tag1'")
        tdSql.checkData(0, 0, "sss")
        tdSql.checkData(0, 2, "true")

        res = tdSql.getColNameList("select 'sss',33,a.jtag->'tag3' from jsons2 a,jsons3 b where a.ts=b.ts and a.jtag->'tag1'=b.jtag->'tag1'")
        cname_list = []
        cname_list.append("sss")
        cname_list.append("33")
        cname_list.append("a.jtag->'tag3'")
        tdSql.checkColNameList(res, cname_list)
    
        # test group by & order by  json tag
        tdSql.error("select count(*) from jsons1 group by jtag->'tag1' order by jtag->'tag2'")
        tdSql.error("select count(*) from jsons1 group by jtag->'tag1' order by jtag")
        tdSql.query("select count(*) from jsons1 group by jtag->'tag1' order by jtag->'tag1' desc")
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(0, 1, '"femail"')
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(2, 1, 11)
        tdSql.checkData(5, 0, 1)
        tdSql.checkData(5, 1, "false")
        tdSql.checkData(6, 0, 1)
        tdSql.checkData(6, 1, "null")
        tdSql.checkData(7, 0, 2)
        tdSql.checkData(7, 1, None)

        tdSql.query("select count(*) from jsons1 group by jtag->'tag1' order by jtag->'tag1' asc")
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(2, 1, "false")
        tdSql.checkData(5, 0, 1)
        tdSql.checkData(5, 1, 11)
        tdSql.checkData(7, 0, 2)
        tdSql.checkData(7, 1, '"femail"')

        # test stddev with group by json tag
        tdSql.query("select stddev(dataint) from jsons1 group by jtag->'tag1'")
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(1, 1, "null")
        tdSql.checkData(7, 0, 11)
        tdSql.checkData(7, 1, '"femail"')

        res = tdSql.getColNameList("select stddev(dataint) from jsons1 group by jsons1.jtag->'tag1'")
        cname_list = []
        cname_list.append("stddev(dataint)")
        cname_list.append("jsons1.jtag->'tag1'")
        tdSql.checkColNameList(res, cname_list)

        # test top/bottom with group by json tag
        tdSql.query("select top(dataint,100) from jsons1 group by jtag->'tag1'")
        tdSql.checkRows(11)
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(1, 1, 24)
        tdSql.checkData(1, 2, None)
        tdSql.checkData(10, 1, 1)
        tdSql.checkData(10, 2, '"femail"')

        # test having
        tdSql.query("select stddev(dataint) from jsons1 group by jtag->'tag1' having stddev(dataint) > 0")
        tdSql.checkRows(2)

        # subquery with json tag
        tdSql.query("select * from (select jtag, dataint from jsons1)")
        tdSql.checkRows(11)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 0, '{"tag1":5,"tag2":"beijing"}')

        tdSql.query("select jtag->'tag1' from (select jtag->'tag1', dataint from jsons1)")
        tdSql.checkRows(11)
        tdSql.checkData(0, 0, '"femail"')
        tdSql.checkData(2, 0, 5)

        res = tdSql.getColNameList("select jtag->'tag1' from (select jtag->'tag1', dataint from jsons1)")
        cname_list = []
        cname_list.append("jtag->'tag1'")
        tdSql.checkColNameList(res, cname_list)

        tdSql.query("select ts,tbname,jtag->'tag1' from (select jtag->'tag1',tbname,ts from jsons1 order by ts)")
        tdSql.checkRows(11)
        tdSql.checkData(1, 1, "jsons1_1")
        tdSql.checkData(1, 2, '"femail"')

        # union all
        tdSql.error("select jtag->'tag1' from jsons1 union all select jtag->'tag2' from jsons2")
        tdSql.error("select jtag->'tag1' from jsons1_1 union all select jtag->'tag2' from jsons2_1")

        tdSql.query("select jtag->'tag1' from jsons1_1 union all select jtag->'tag1' from jsons2_1")
        tdSql.checkRows(2)
        tdSql.query("select dataint,jtag->'tag1',tbname from jsons1 union all select dataint,jtag->'tag1',tbname from jsons2")
        tdSql.checkRows(13)
        tdSql.query("select dataint,jtag,tbname from jsons1 union all select dataint,jtag,tbname from jsons2")
        tdSql.checkRows(13)

        #show create table
        tdSql.query("show create table jsons1")
        tdSql.checkData(0, 1, 'CREATE TABLE `jsons1` (`ts` TIMESTAMP,`dataint` INT,`databool` BOOL,`datastr` NCHAR(50),`datastrbin` BINARY(150)) TAGS (`jtag` JSON)')

        #test aggregate function:count/avg/twa/irate/sum/stddev/leastsquares
        tdSql.query("select count(*) from jsons1 where jtag is not null")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select avg(dataint) from jsons1 where jtag is not null")
        tdSql.checkData(0, 0, 5.3)
        tdSql.error("select twa(dataint) from jsons1 where jtag is not null")
        tdSql.error("select irate(dataint) from jsons1 where jtag is not null")
        tdSql.query("select sum(dataint) from jsons1 where jtag->'tag1' is not null")
        tdSql.checkData(0, 0, 49)
        tdSql.query("select stddev(dataint) from jsons1 where jtag->'tag1'>1")
        tdSql.checkData(0, 0, 4.496912521)
        tdSql.error("SELECT LEASTSQUARES(dataint, 1, 1) from jsons1 where jtag is not null")

        #test selection function:min/max/first/last/top/bottom/percentile/apercentile/last_row/interp
        tdSql.query("select min(dataint) from jsons1 where jtag->'tag1'>1")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select max(dataint) from jsons1 where jtag->'tag1'>1")
        tdSql.checkData(0, 0, 11)
        tdSql.query("select first(dataint) from jsons1 where jtag->'tag1'>1")
        tdSql.checkData(0, 0, 2)
        tdSql.query("select last(dataint) from jsons1 where jtag->'tag1'>1")
        tdSql.checkData(0, 0, 11)
        tdSql.query("select top(dataint,100) from jsons1 where jtag->'tag1'>1")
        tdSql.checkRows(3)
        tdSql.query("select bottom(dataint,100) from jsons1 where jtag->'tag1'>1")
        tdSql.checkRows(3)
        tdSql.error("select percentile(dataint,20) from jsons1 where jtag->'tag1'>1")
        tdSql.query("select apercentile(dataint, 50) from jsons1 where jtag->'tag1'>1")
        tdSql.checkData(0, 0, 1.5)
        tdSql.query("select last_row(dataint) from jsons1 where jtag->'tag1'>1")
        tdSql.checkData(0, 0, 11)
        tdSql.error("select interp(dataint) from jsons1 where ts = '2020-06-02 09:17:08.000' and jtag->'tag1'>1")

        #test calculation function:diff/derivative/spread/ceil/floor/round/
        tdSql.error("select diff(dataint) from jsons1 where jtag->'tag1'>1")
        tdSql.error("select derivative(dataint, 10m, 0) from jsons1 where jtag->'tag1'>1")
        tdSql.query("select spread(dataint) from jsons1 where jtag->'tag1'>1")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select ceil(dataint) from jsons1 where jtag->'tag1'>1")
        tdSql.checkRows(3)
        tdSql.query("select floor(dataint) from jsons1 where jtag->'tag1'>1")
        tdSql.checkRows(3)
        tdSql.query("select round(dataint) from jsons1 where jtag->'tag1'>1")
        tdSql.checkRows(3)

        #test TD-12077
        tdSql.execute("insert into jsons1_16 using jsons1 tags('{\"tag1\":\"收到货\",\"tag2\":\"\",\"tag3\":-2.111}') values(1591062628000, 2, NULL, '你就会', 'dws')")
        tdSql.query("select jtag->'tag3' from jsons1_16")
        tdSql.checkData(0, 0, '-2.111000000')

        # test TD-12452
        tdSql.execute("ALTER TABLE jsons1_1 SET TAG jtag=NULL")
        tdSql.query("select jtag from jsons1_1")
        tdSql.checkData(0, 0, None)
        tdSql.execute("CREATE TABLE if not exists jsons1_20 using jsons1 tags(NULL)")
        tdSql.query("select jtag from jsons1_20")
        tdSql.checkData(0, 0, None)
        tdSql.execute("insert into jsons1_21 using jsons1 tags(NULL) values(1591061628000, 11, false, '你就会','')")
        tdSql.query("select jtag from jsons1_21")
        tdSql.checkData(0, 0, None)

        #test TD-12389
        tdSql.query("describe jsons1")
        tdSql.checkData(5, 2, 4096)
        tdSql.query("describe jsons1_1")
        tdSql.checkData(5, 2, 4096)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())

