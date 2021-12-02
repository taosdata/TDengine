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


class TDTestCase:

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        print("============== STEP 1 ===== prepare data & validate json string")
        tdSql.execute("create table if not exists jsons1(ts timestamp, dataInt int, dataBool bool, dataStr nchar(50), dataStrBin binary(150)) tags(jtag json)")
        tdSql.execute("insert into jsons1_1 using jsons1 tags('{\"tag1\":\"fff\",\"tag2\":5, \"tag3\":true}') values(1591060618000, 1, false, 'json1', '你是') (1591060608000, 23, true, '等等', 'json')")
        tdSql.execute("insert into jsons1_2 using jsons1 tags('{\"tag1\":5,\"tag2\":\"beijing\"}') values (1591060628000, 2, true, 'json2', 'sss')")
        tdSql.execute("insert into jsons1_3 using jsons1 tags('{\"tag1\":false,\"tag2\":\"beijing\"}') values (1591060668000, 3, false, 'json3', 'efwe')")
        tdSql.execute("insert into jsons1_4 using jsons1 tags('{\"tag1\":null,\"tag2\":\"shanghai\",\"tag3\":\"hello\"}') values (1591060728000, 4, true, 'json4', '323sd')")
        tdSql.execute("insert into jsons1_5 using jsons1 tags('{\"tag1\":1.232, \"tag2\":null}') values(1591060928000, 1, false, '你就会', 'ewe')")
        tdSql.execute("insert into jsons1_6 using jsons1 tags('{\"tag1\":11,\"tag2\":\"\",\"tag2\":null}') values(1591061628000, 11, false, '你就会','')")
        tdSql.execute("insert into jsons1_7 using jsons1 tags('{\"tag1\":\"收到货\",\"tag2\":\"\",\"tag3\":null}') values(1591062628000, 2, NULL, '你就会', 'dws')")

        # test duplicate key using the first one. elimate empty key
        tdSql.execute("CREATE TABLE if not exists jsons1_8 using jsons1 tags('{\"tag1\":null, \"tag1\":true, \"tag1\":45, \"1tag$\":2, \" \":90}')")

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
        tdSql.error("CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"\":\"fff\"}')")
        tdSql.error("CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"\t\":\"fff\"}')")
        tdSql.error("CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"试试\":\"fff\"}')")

        print("============== STEP 2 ===== alter table json tag")
        tdSql.error("ALTER STABLE jsons1 add tag tag2 nchar(20)")
        tdSql.error("ALTER STABLE jsons1 drop tag jtag")
        tdSql.error("ALTER TABLE jsons1_1 SET TAG jtag=4")

        tdSql.execute("ALTER TABLE jsons1_1 SET TAG jtag='{\"tag1\":\"femail\",\"tag2\":35,\"tag3\":true}'")

        print("============== STEP 3 ===== query table")
        # test error syntax
        tdSql.error("select * from jsons1 where jtag->tag1='beijing'")
        tdSql.error("select * from jsons1 where jtag->'location'")
        tdSql.error("select * from jsons1 where jtag->''")
        tdSql.error("select * from jsons1 where jtag->''=9")
        tdSql.error("select -> from jsons1")
        tdSql.error("select ? from jsons1")
        tdSql.error("select * from jsons1 where ?")
        tdSql.error("select * from jsons1 where jtag->")
        tdSql.error("select jtag->location from jsons1")
        tdSql.error("select jtag?location from jsons1")
        tdSql.error("select * from jsons1 where jtag?location")
        tdSql.error("select * from jsons1 where jtag?''")
        tdSql.error("select * from jsons1 where jtag?'location'='beijing'")

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
        # test #line 41
        tdSql.query("select jtag from jsons1_8")
        tdSql.checkData(0, 0, '{"tag1":null,"1tag$":2," ":90}')
        # test #line 72
        tdSql.query("select jtag from jsons1_1")
        tdSql.checkData(0, 0, '{"tag1":"femail","tag2":35,"tag3":true}')
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
        # where json value is int
        tdSql.query("select * from jsons1 where jtag->'tag1'=5")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 2)
        tdSql.query("select * from jsons1 where jtag->'tag1'=10")
        tdSql.checkRows(0)
        tdSql.query("select * from jsons1 where jtag->'tag1'<54")
        tdSql.checkRows(3)
        # where json value is double
        tdSql.query("select * from jsons1 where jtag->'tag1'=1.232")
        tdSql.checkRows(1)
        # where json value is null
        tdSql.query("select * from jsons1 where jtag->'tag1'=null")
        tdSql.checkRows(1)
        # where json value is not exist
        tdSql.query("select * from jsons1 where jtag->'tag1' is null")
        tdSql.checkData(0, 0, 'jsons1_9')
        tdSql.checkRows(1)
        tdSql.query("select * from jsons1 where jtag->'tag4' is null")
        tdSql.checkRows(9)
        tdSql.query("select * from jsons1 where jtag->'tag3' is not null")
        tdSql.checkRows(4)


        # test json tag in where condition with and/or/?
        tdSql.query("select * from jsons1 where jtag->'location'!='beijing'")
        tdSql.checkRows(1)

        tdSql.query("select jtag->'num' from jsons1 where jtag->'level'='l1'")
        tdSql.checkData(0, 0, 34)

        # test json number value
        tdSql.query("select *,tbname from jsons1 where jtag->'class'>5 and jtag->'class'<9")
        tdSql.checkRows(0)

        tdSql.query("select *,tbname from jsons1 where jtag->'class'>5 and jtag->'class'<92")
        tdSql.checkRows(1)

        # test where condition
        tdSql.query("select * from jsons1 where jtag?'sex' or jtag?'num'")
        tdSql.checkRows(3)

        tdSql.query("select * from jsons1 where jtag?'sex' or jtag?'numww'")
        tdSql.checkRows(1)

        tdSql.query("select * from jsons1 where jtag?'sex' and jtag?'num'")
        tdSql.checkRows(0)

        tdSql.query("select jtag->'sex' from jsons1 where jtag?'sex' or jtag?'num'")
        tdSql.checkRows(3)

        tdSql.query("select *,tbname from jsons1 where jtag->'location'='beijing'")
        tdSql.checkRows(2)

        tdSql.query("select *,tbname from jsons1 where jtag->'num'=5 or jtag?'sex'")
        tdSql.checkRows(2)

        tdSql.query("select *,tbname from jsons1 where jtag->'num'=5")
        tdSql.checkRows(1)

        # test with tbname
        tdSql.query("select * from jsons1 where tbname = 'jsons1_1'")
        tdSql.checkRows(1)

        tdSql.query("select * from jsons1 where tbname = 'jsons1_1' or jtag?'num'")
        tdSql.checkRows(3)

        tdSql.query("select * from jsons1 where tbname = 'jsons1_1' and jtag?'num'")
        tdSql.checkRows(0)

        tdSql.query("select * from jsons1 where tbname = 'jsons1_1' or jtag->'num'=5")
        tdSql.checkRows(2)

        # test where condition like
        tdSql.query("select *,tbname from jsons1 where jtag->'location' like 'bei%'")
        tdSql.checkRows(2)

        tdSql.query("select *,tbname from jsons1 where jtag->'location' like 'bei%' and jtag->'location'='beijin'")
        tdSql.checkRows(0)

        tdSql.query("select *,tbname from jsons1 where jtag->'location' like 'bei%' or jtag->'location'='beijin'")
        tdSql.checkRows(2)

        tdSql.query("select *,tbname from jsons1 where jtag->'location' like 'bei%' and jtag->'num'=34")
        tdSql.checkRows(1)

        tdSql.query("select *,tbname from jsons1 where (jtag->'location' like 'bei%' or jtag->'num'=34) and jtag->'class'=55")
        tdSql.checkRows(0)

        tdSql.query("select * from jsons1 where jtag->'num' like '5%'")
        tdSql.checkRows(0)

        # test where condition in
        tdSql.error("select * from jsons1 where jtag->'location' in ('beijing')")
        tdSql.error("select * from jsons1 where jtag->'num' in ('5',34)")

        # test where condition match
        tdSql.query("select * from jsons1 where jtag->'location' match 'jin$'")
        tdSql.checkRows(0)

        tdSql.query("select * from jsons1 where jtag->'location' match 'jin'")
        tdSql.checkRows(2)

        tdSql.query("select * from jsons1 where datastr match 'json' and jtag->'location' match 'jin'")
        tdSql.checkRows(2)

        tdSql.query("select * from jsons1 where jtag->'num' match '5'")
        tdSql.checkRows(0)

        
        tdSql.query("select jtag from jsons1_6")
        tdSql.checkData(0, 0, None)

        
        tdSql.query("select jtag from jsons1_7")
        tdSql.checkData(0, 0, None)

        
        tdSql.query("select jtag from jsons1_8")
        tdSql.checkData(0, 0, None)

        
        tdSql.query("select jtag from jsons1_9")
        tdSql.checkData(0, 0, "{\"time\":null}")

        tdSql.query("select jtag from jsons1_10")
        tdSql.checkData(0, 0, "{\"k1\":\"\",\"k2\":true,\"k3\":false,\"k4\":55}")

        tdSql.query("select jtag->'k2' from jsons1_10")
        tdSql.checkData(0, 0, "true")

        tdSql.query("select jtag from jsons1 where jtag->'k1'=''")
        tdSql.checkRows(1)

        tdSql.query("select jtag from jsons1 where jtag->'k2'=true")
        tdSql.checkRows(1)

        tdSql.query("select jtag from jsons1 where jtag is null")
        tdSql.checkRows(4)

        tdSql.query("select jtag from jsons1 where jtag is not null")
        tdSql.checkRows(6)

        tdSql.query("select * from jsons1 where jtag->'location' is not null")
        tdSql.checkRows(3)

        tdSql.query("select tbname,jtag from jsons1 where jtag->'location' is null")
        tdSql.checkRows(7)

        tdSql.query("select * from jsons1 where jtag->'num' is not null")
        tdSql.checkRows(2)

        tdSql.query("select * from jsons1 where jtag->'location'='null'")
        tdSql.checkRows(0)

        tdSql.query("select * from jsons1 where jtag->'num'='null'")
        tdSql.checkRows(0)

        # test distinct
        tdSql.query("select distinct jtag from jsons1")
        tdSql.checkRows(7)

        tdSql.query("select distinct jtag->'location' from jsons1")
        tdSql.checkRows(3)

        # test chinese
        tdSql.query("select tbname,jtag from jsons1 where jtag->'k1' match '中'")
        tdSql.checkRows(1)

        tdSql.query("select tbname,jtag from jsons1 where jtag->'k1'='中国'")
        tdSql.checkRows(1)

        #test dumplicate key with normal colomn
        tdSql.execute("INSERT INTO jsons1_12 using jsons1 tags('{\"tbname\":\"tt\",\"databool\":true,\"dataStr\":\"是是是\"}') values(1591060828000, 4, false, \"你就会\")")

        tdSql.query("select *,tbname,jtag from jsons1 where jtag->'dataStr' match '是'")
        tdSql.checkRows(1)

        tdSql.query("select tbname,jtag->'tbname' from jsons1 where jtag->'tbname'='tt'")
        tdSql.checkRows(1)

        tdSql.query("select *,tbname,jtag from jsons1 where dataBool=true")
        tdSql.checkRows(2)

        # test error
        
        # test join
        tdSql.query("select 'sss',33,a.jtag->'loc' from jsons2 a,jsons3 b where a.ts=b.ts and a.jtag->'loc'=b.jtag->'loc'")
        tdSql.checkData(0, 0, "sss")
        tdSql.checkData(0, 2, "\"fff\"")

        res = tdSql.getColNameList("select 'sss',33,a.jtag->'loc' from jsons2 a,jsons3 b where a.ts=b.ts and a.jtag->'loc'=b.jtag->'loc'")
        cname_list = []
        cname_list.append("sss")
        cname_list.append("33")
        cname_list.append("a.jtag->'loc'")
        tdSql.checkColNameList(res, cname_list)
    
        # test group by & order by   string
        tdSql.query("select avg(dataint),count(*) from jsons1 group by jtag->'location' order by jtag->'location' desc")
        tdSql.checkData(1, 0, 2.5)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, "\"beijing\"")
        tdSql.checkData(2, 2, None)

        # test group by & order by   int
        tdSql.query("select avg(dataint),count(*) from jsons1 group by jtag->'tagint' order by jtag->'tagint' desc")
        tdSql.checkData(0, 0, 11)
        tdSql.checkData(0, 2, 11)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(3, 2, None)
        tdSql.checkData(3, 1, 5)
        tdSql.query("select avg(dataint),count(*) from jsons1 group by jtag->'tagint' order by jtag->'tagint'")
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(3, 2, 11)

        # test stddev with group by json tag sting
        tdSql.query("select stddev(dataint) from jsons1 group by jtag->'location'")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, 0.5)
        tdSql.checkData(2, 0, 0)

        tdSql.query("select stddev(dataint) from jsons1 group by jtag->'tagint'")
        tdSql.checkData(0, 0, 1.16619037896906)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(2, 1, 2)

        res = tdSql.getColNameList("select stddev(dataint) from jsons1 group by jsons1.jtag->'tagint'")
        cname_list = []
        cname_list.append("stddev(dataint)")
        cname_list.append("jsons1.jtag->'tagint'")
        tdSql.checkColNameList(res, cname_list)

        # test json->'key'=null
        
        tdSql.query("select * from jsons1")
        tdSql.checkRows(9)
        tdSql.query("select * from jsons1 where jtag->'time' is null")
        tdSql.checkRows(8)
        tdSql.query("select * from jsons1 where jtag->'time'=null")
        tdSql.checkRows(1)

        # subquery with json tag
        tdSql.query("select * from (select jtag, dataint from jsons1)")
        tdSql.checkRows(9)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(6, 0, "{\"tagint\":1}")

        tdSql.query("select jtag->'age' from (select jtag->'age', dataint from jsons1)")
        tdSql.checkRows(9)
        tdSql.checkData(0, 0, 35)
        tdSql.checkData(2, 0, None)

        res = tdSql.getColNameList("select jtag->'age' from (select jtag->'age', dataint from jsons1)")
        cname_list = []
        cname_list.append("jtag->'age'")
        tdSql.checkColNameList(res, cname_list)

        tdSql.query("select ts,tbname,jtag->'location' from (select jtag->'location',tbname,ts from jsons1 order by ts)")
        tdSql.checkRows(9)
        tdSql.checkData(1, 1, "jsons1_2")
        tdSql.checkData(3, 2, "\"beijing\"")

        # test different type of json value
        tdSql.query("select avg(dataint),count(*) from jsons1 group by jtag->'tagint' order by jtag->'tagint' desc")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
