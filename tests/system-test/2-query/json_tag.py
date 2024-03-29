# -*- coding: utf-8 -*-

from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql

class TDTestCase:
    def caseDescription(self):
        '''
        Json tag test case, include create table with json tag, select json tag and query with json tag in where condition, besides, include json tag in group by/order by/join/subquery.
        case1: [TD-12452] fix error if json tag is NULL
        case2: [TD-12389] describe child table, tag length error if the tag is json tag
        '''
        return

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)

    def run(self):
        # tdSql.prepare()
        dbname = "db"
        tdSql.execute(f'drop database if exists {dbname}')
        tdSql.execute(f'create database {dbname} vgroups 1')
        tdSql.execute(f'use {dbname}')
        print("============== STEP 1 ===== prepare data & validate json string")
        tdSql.error(f"create table if not exists {dbname}.jsons1(ts timestamp, dataInt int, dataBool bool, dataStr nchar(50), dataStrBin binary(150)) tags(jtag json, tagint int)")
        tdSql.error(f"create table if not exists {dbname}.jsons1(ts timestamp, data json) tags(tagint int)")
        tdSql.execute(f"create table if not exists {dbname}.jsons1(ts timestamp, dataInt int, dataBool bool, dataStr nchar(50), dataStrBin binary(150)) tags(jtag json)")
        tdSql.execute(f"insert into {dbname}.jsons1_1 using {dbname}.jsons1 tags('{{\"tag1\":\"fff\",\"tag2\":5, \"tag3\":true}}') values(1591060618000, 1, false, 'json1', '你是') (1591060608000, 23, true, '等等', 'json')")
        tdSql.execute(f"insert into {dbname}.jsons1_2 using {dbname}.jsons1 tags('{{\"tag1\":5,\"tag2\":\"beijing\"}}') values (1591060628000, 2, true, 'json2', 'sss')")
        tdSql.execute(f"insert into {dbname}.jsons1_3 using {dbname}.jsons1 tags('{{\"tag1\":false,\"tag2\":\"beijing\"}}') values (1591060668000, 3, false, 'json3', 'efwe')")
        tdSql.execute(f"insert into {dbname}.jsons1_4 using {dbname}.jsons1 tags('{{\"tag1\":null,\"tag2\":\"shanghai\",\"tag3\":\"hello\"}}') values (1591060728000, 4, true, 'json4', '323sd')")
        tdSql.execute(f"insert into {dbname}.jsons1_5 using {dbname}.jsons1 tags('{{\"tag1\":1.232, \"tag2\":null}}') values(1591060928000, 1, false, '你就会', 'ewe')")
        tdSql.execute(f"insert into {dbname}.jsons1_6 using {dbname}.jsons1 tags('{{\"tag1\":11,\"tag2\":\"\",\"tag2\":null}}') values(1591061628000, 11, false, '你就会','')")
        tdSql.execute(f"insert into {dbname}.jsons1_7 using {dbname}.jsons1 tags('{{\"tag1\":\"收到货\",\"tag2\":\"\",\"tag3\":null}}') values(1591062628000, 2, NULL, '你就会', 'dws')")

        # test duplicate key using the first one. elimate empty key
        tdSql.execute(f"create TABLE if not exists {dbname}.jsons1_8 using {dbname}.jsons1 tags('{{\"tag1\":null, \"tag1\":true, \"tag1\":45, \"1tag$\":2, \" \":90, \"\":32}}')")
        tdSql.query(f"select jtag from {dbname}.jsons1_8")
        tdSql.checkRows(0)

        tdSql.query(f"select ts,jtag from {dbname}.jsons1 order by ts limit 2,3")
        tdSql.checkData(0, 0, '2020-06-02 09:17:08.000')
        tdSql.checkData(0, 1, '{"tag1":5,"tag2":"beijing"}')
        tdSql.checkData(1, 0, '2020-06-02 09:17:48.000')
        tdSql.checkData(1, 1, '{"tag1":false,"tag2":"beijing"}')
        tdSql.checkData(2, 0, '2020-06-02 09:18:48.000')
        tdSql.checkData(2, 1, '{"tag1":null,"tag2":"shanghai","tag3":"hello"}')

        tdSql.query(f"select ts,jtag->'tag1' from {dbname}.jsons1 order by ts limit 2,3")
        tdSql.checkData(0, 0, '2020-06-02 09:17:08.000')
        tdSql.checkData(0, 1, '5.000000000')
        tdSql.checkData(1, 0, '2020-06-02 09:17:48.000')
        tdSql.checkData(1, 1, 'false')
        tdSql.checkData(2, 0, '2020-06-02 09:18:48.000')
        tdSql.checkData(2, 1, 'null')

        # test empty json string, save as jtag is NULL
        tdSql.execute(f"insert into {dbname}.jsons1_9  using {dbname}.jsons1 tags('\t') values (1591062328000, 24, NULL, '你就会', '2sdw')")
        tdSql.execute(f"create TABLE if not exists {dbname}.jsons1_10 using {dbname}.jsons1 tags('')")
        tdSql.execute(f"create TABLE if not exists {dbname}.jsons1_11 using {dbname}.jsons1 tags(' ')")
        tdSql.execute(f"create TABLE if not exists {dbname}.jsons1_12 using {dbname}.jsons1 tags('{{}}')")
        tdSql.execute(f"create TABLE if not exists {dbname}.jsons1_13 using {dbname}.jsons1 tags('null')")

        # test invalidate json
        tdSql.error(f"create TABLE if not exists {dbname}.jsons1_14 using {dbname}.jsons1 tags('\"efwewf\"')")
        tdSql.error(f"create TABLE if not exists {dbname}.jsons1_14 using {dbname}.jsons1 tags('3333')")
        tdSql.error(f"create TABLE if not exists {dbname}.jsons1_14 using {dbname}.jsons1 tags(76)")
        tdSql.error(f"create TABLE if not exists {dbname}.jsons1_14 using {dbname}.jsons1 tags(hell)")
        tdSql.error(f"create TABLE if not exists {dbname}.jsons1_14 using {dbname}.jsons1 tags('33.33')")
        tdSql.error(f"create TABLE if not exists {dbname}.jsons1_14 using {dbname}.jsons1 tags('false')")
        tdSql.error(f"create TABLE if not exists {dbname}.jsons1_14 using {dbname}.jsons1 tags('[1,true]')")
        tdSql.error(f"create TABLE if not exists {dbname}.jsons1_14 using {dbname}.jsons1 tags('{{222}}')")
        tdSql.error(f"create TABLE if not exists {dbname}.jsons1_14 using {dbname}.jsons1 tags('{{\"fe\"}}')")

        # test invalidate json key, key must can be printed assic char
        tdSql.error(f"create TABLE if not exists {dbname}.jsons1_14 using {dbname}.jsons1 tags('{{\"tag1\":[1,true]}}')")
        tdSql.error(f"create TABLE if not exists {dbname}.jsons1_14 using {dbname}.jsons1 tags('{{\"tag1\":{{}}}}')")
        tdSql.error(f"create TABLE if not exists {dbname}.jsons1_14 using {dbname}.jsons1 tags('{{\"。loc\":\"fff\"}}')")
        tdSql.error(f"create TABLE if not exists {dbname}.jsons1_14 using {dbname}.jsons1 tags('{{\"\t\":\"fff\"}}')")
        tdSql.error(f"create TABLE if not exists {dbname}.jsons1_14 using {dbname}.jsons1 tags('{{\"试试\":\"fff\"}}')")

        # test invalidate json value, value number can not be inf,nan TD-12166
        tdSql.error(f"create TABLE if not exists {dbname}.jsons1_14 using {dbname}.jsons1 tags('{{\"k\":1.8e308}}')")
        tdSql.error(f"create TABLE if not exists {dbname}.jsons1_14 using {dbname}.jsons1 tags('{{\"k\":-1.8e308}}')")

        #test length limit
        char1= ''.join(['abcd']*64)
        char3= ''.join(['abcd']*1021)
        print(len(char3))   # 4084
        tdSql.error(f"create TABLE if not exists {dbname}.jsons1_15 using {dbname}.jsons1 tags('{{\"%s1\":5}}')" % char1)   # len(key)=257
        tdSql.execute(f"create TABLE if not exists {dbname}.jsons1_15 using {dbname}.jsons1 tags('{{\"%s\":5}}')" % char1)  # len(key)=256
        tdSql.error(f"create TABLE if not exists {dbname}.jsons1_16 using {dbname}.jsons1 tags('{{\"TSSSS\":\"%s\"}}')" % char3)   # len(object)=4096
        tdSql.execute(f"create TABLE if not exists {dbname}.jsons1_16 using {dbname}.jsons1 tags('{{\"TSSS\":\"%s\"}}')" % char3)  # len(object)=4095
        tdSql.execute(f"drop table if exists {dbname}.jsons1_15")
        tdSql.execute(f"drop table if exists {dbname}.jsons1_16")

        print("============== STEP 2 ===== alter table json tag")
        tdSql.error(f"ALTER stable {dbname}.jsons1 add tag tag2 nchar(20)")
        tdSql.error(f"ALTER stable {dbname}.jsons1 drop tag jtag")
        tdSql.error(f"ALTER table {dbname}.jsons1 MODIFY TAG jtag nchar(128)")

        tdSql.execute(f"ALTER table {dbname}.jsons1_1 SET TAG jtag='{{\"tag1\":\"femail\",\"tag2\":35,\"tag3\":true}}'")
        tdSql.query(f"select jtag from {dbname}.jsons1_1")
        tdSql.checkData(0, 0, '{"tag1":"femail","tag2":35,"tag3":true}')
        tdSql.execute(f"ALTER table {dbname}.jsons1 rename TAG jtag jtag_new")
        tdSql.execute(f"ALTER table {dbname}.jsons1 rename TAG jtag_new jtag")

        tdSql.execute(f"create table {dbname}.st(ts timestamp, i int) tags(t int)")
        tdSql.error(f"ALTER stable {dbname}.st add tag jtag json")
        tdSql.error(f"ALTER stable {dbname}.st add column jtag json")

        print("============== STEP 3 ===== query table")
        # test error syntax
        tdSql.error(f"select * from {dbname}.jsons1 where jtag->tag1='beijing'")
        tdSql.error(f"select -> from {dbname}.jsons1")
        tdSql.error(f"select * from {dbname}.jsons1 where contains")
        tdSql.error(f"select * from {dbname}.jsons1 where jtag->")
        tdSql.error(f"select jtag->location from {dbname}.jsons1")
        tdSql.error(f"select jtag contains location from {dbname}.jsons1")
        tdSql.error(f"select * from {dbname}.jsons1 where jtag contains location")
        tdSql.query(f"select * from {dbname}.jsons1 where jtag contains''")
        tdSql.error(f"select * from {dbname}.jsons1 where jtag contains 'location'='beijing'")

        # test function error
        tdSql.error(f"select avg(jtag->'tag1') from {dbname}.jsons1")
        tdSql.error(f"select avg(jtag) from {dbname}.jsons1")
        tdSql.error(f"select min(jtag->'tag1') from {dbname}.jsons1")
        tdSql.error(f"select min(jtag) from {dbname}.jsons1")
        tdSql.error(f"select ceil(jtag->'tag1') from {dbname}.jsons1")
        tdSql.error(f"select ceil(jtag) from {dbname}.jsons1")


        #test scalar operation
        tdSql.query(f"select jtag contains 'tag1',jtag->'tag1' from {dbname}.jsons1 order by jtag->'tag1'")
        tdSql.checkRows(9)
        tdSql.query(f"select jtag->'tag1' like 'fe%',jtag->'tag1' from {dbname}.jsons1 order by jtag->'tag1'")
        tdSql.checkRows(9)
        tdSql.query(f"select jtag->'tag1' not like 'fe%',jtag->'tag1' from {dbname}.jsons1 order by jtag->'tag1'")
        tdSql.checkRows(9)
        tdSql.query(f"select jtag->'tag1' match 'fe',jtag->'tag1' from {dbname}.jsons1 order by jtag->'tag1'")
        tdSql.checkRows(9)
        tdSql.query(f"select jtag->'tag1' nmatch 'fe',jtag->'tag1' from {dbname}.jsons1 order by jtag->'tag1'")
        tdSql.checkRows(9)
        tdSql.query(f"select jtag->'tag1',jtag->'tag1'>='a' from {dbname}.jsons1 order by jtag->'tag1'")
        tdSql.checkRows(9)

        # test select normal column
        tdSql.query(f"select dataint from {dbname}.jsons1 order by dataint")
        tdSql.checkRows(9)
        tdSql.checkData(1, 0, 1)

        # test select json tag
        tdSql.query(f"select * from {dbname}.jsons1")
        tdSql.checkRows(9)
        tdSql.query(f"select jtag from {dbname}.jsons1")
        tdSql.checkRows(9)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag is null")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag is not null")
        tdSql.checkRows(8)

        # test jtag is NULL
        tdSql.query(f"select jtag from {dbname}.jsons1_9")
        tdSql.checkData(0, 0, None)

        # test select json tag->'key', value is string
        tdSql.query(f"select jtag->'tag1' from {dbname}.jsons1_1")
        tdSql.checkData(0, 0, '"femail"')
        tdSql.query(f"select jtag->'tag2' from {dbname}.jsons1_6")
        tdSql.checkData(0, 0, '""')
        # test select json tag->'key', value is int
        tdSql.query(f"select jtag->'tag2' from {dbname}.jsons1_1")
        tdSql.checkData(0, 0, "35.000000000")
        # test select json tag->'key', value is bool
        tdSql.query(f"select jtag->'tag3' from {dbname}.jsons1_1")
        tdSql.checkData(0, 0, "true")
        # test select json tag->'key', value is null
        tdSql.query(f"select jtag->'tag1' from {dbname}.jsons1_4")
        tdSql.checkData(0, 0, "null")
        # test select json tag->'key', value is double
        tdSql.query(f"select jtag->'tag1' from {dbname}.jsons1_5")
        tdSql.checkData(0, 0, "1.232000000")
        # test select json tag->'key', key is not exist
        tdSql.query(f"select jtag->'tag10' from {dbname}.jsons1_4")
        tdSql.checkData(0, 0, None)

        tdSql.query(f"select jtag->'tag1' from {dbname}.jsons1")
        tdSql.checkRows(9)
        # test header name
        res = tdSql.getColNameList(f"select jtag->'tag1' from {dbname}.jsons1")
        cname_list = []
        cname_list.append("jtag->'tag1'")
        tdSql.checkColNameList(res, cname_list)


        # test where with json tag
        tdSql.query(f"select * from {dbname}.jsons1_1 where jtag is not null")
        tdSql.error(f"select * from {dbname}.jsons1 where jtag='{{\"tag1\":11,\"tag2\":\"\"}}'")
        tdSql.error(f"select * from {dbname}.jsons1 where jtag->'tag1'={{}}")

        # test json error
        tdSql.error(f"select jtag + 1 from {dbname}.jsons1")
        tdSql.error(f"select jtag > 1 from {dbname}.jsons1")
        tdSql.error(f"select jtag like \"1\" from {dbname}.jsons1")
        tdSql.error(f"select jtag in  (\"1\") from {dbname}.jsons1")
        #tdSql.error(f"select jtag from {dbname}.jsons1 where jtag > 1")
        #tdSql.error(f"select jtag from {dbname}.jsons1 where jtag like 'fsss'")
        #tdSql.error(f"select jtag from {dbname}.jsons1 where jtag in (1)")


        # where json value is string
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag2'='beijing'")
        tdSql.checkRows(2)
        tdSql.query(f"select dataint,tbname,jtag->'tag1',jtag from {dbname}.jsons1 where jtag->'tag2'='beijing' order by dataint")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(0, 1, 'jsons1_2')
        tdSql.checkData(0, 2, "5.000000000")
        tdSql.checkData(0, 3, '{"tag1":5,"tag2":"beijing"}')
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(1, 1, 'jsons1_3')
        tdSql.checkData(1, 2, 'false')


        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'='beijing'")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'='收到货'")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag2'>'beijing'")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag2'>='beijing'")
        tdSql.checkRows(3)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag2'<'beijing'")
        tdSql.checkRows(2)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag2'<='beijing'")
        tdSql.checkRows(4)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag2'!='beijing'")
        tdSql.checkRows(3)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag2'=''")
        tdSql.checkRows(2)

        # where json value is int
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'=5")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 2)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'=10")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'<54")
        tdSql.checkRows(3)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'<=11")
        tdSql.checkRows(3)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'>4")
        tdSql.checkRows(2)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'>=5")
        tdSql.checkRows(2)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'!=5")
        tdSql.checkRows(2)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'!=55")
        tdSql.checkRows(3)

        # where json value is double
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'=1.232")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'<1.232")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'<=1.232")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'>1.23")
        tdSql.checkRows(3)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'>=1.232")
        tdSql.checkRows(3)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'!=1.232")
        tdSql.checkRows(2)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'!=3.232")
        tdSql.checkRows(3)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'/0=3")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'/5=1")
        tdSql.checkRows(1)

        # where json value is bool
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'=true")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'=false")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'!=false")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'>false")
        tdSql.checkRows(0)

        # where json value is null
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'=null")
        tdSql.checkRows(0)

        # where json key is null
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag_no_exist'=3")
        tdSql.checkRows(0)

        # where json value is not exist
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1' is null")
        # tdSql.checkData(0, 0, 'jsons1_9')
        tdSql.checkRows(2)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag4' is null")
        tdSql.checkRows(9)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag3' is not null")
        tdSql.checkRows(3)

        # test contains
        tdSql.query(f"select * from {dbname}.jsons1 where jtag contains 'tag1'")
        tdSql.checkRows(8)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag contains 'tag3'")
        tdSql.checkRows(4)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag contains 'tag_no_exist'")
        tdSql.checkRows(0)

        # test json tag in where condition with and/or
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'=false and jtag->'tag2'='beijing'")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'=false or jtag->'tag2'='beijing'")
        tdSql.checkRows(2)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'=false and jtag->'tag2'='shanghai'")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'=false and jtag->'tag2'='shanghai'")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'=13 or jtag->'tag2'>35")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'=13 or jtag->'tag2'>35")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1' is not null and jtag contains 'tag3'")
        tdSql.checkRows(3)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1'='femail' and jtag contains 'tag3'")
        tdSql.checkRows(2)


        # test with between and
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1' between 1 and 30")
        tdSql.checkRows(3)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1' between 'femail' and 'beijing'")
        tdSql.checkRows(0)

        # test with tbname/normal column
        tdSql.query(f"select * from {dbname}.jsons1 where tbname = 'jsons1_1'")
        tdSql.checkRows(2)
        tdSql.query(f"select * from {dbname}.jsons1 where tbname = 'jsons1_1' and jtag contains 'tag3'")
        tdSql.checkRows(2)
        tdSql.query(f"select * from {dbname}.jsons1 where tbname = 'jsons1_1' and jtag contains 'tag3' and dataint=3")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.jsons1 where tbname = 'jsons1_1' and jtag contains 'tag3' and dataint=23")
        tdSql.checkRows(1)


        # test where condition like
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag2' like 'bei%'")
        tdSql.checkRows(2)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1' like 'fe%' and jtag->'tag2' is not null")
        tdSql.checkRows(2)

        # test where condition in  no support in
        tdSql.error(f"select * from {dbname}.jsons1 where jtag->'tag1' in ('beijing')")

        # test where condition match/nmath
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1' match 'ma'")
        tdSql.checkRows(2)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1' match 'ma$'")
        tdSql.checkRows(0)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag2' match 'jing$'")
        tdSql.checkRows(2)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1' match '收到'")
        tdSql.checkRows(1)
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'tag1' nmatch 'ma'")
        tdSql.checkRows(1)

        # test distinct
        tdSql.execute(f"insert into {dbname}.jsons1_14 using {dbname}.jsons1 tags('{{\"tag1\":\"收到货\",\"tag2\":\"\",\"tag3\":null}}') values(1591062628000, 2, NULL, '你就会', 'dws')")
        tdSql.query(f"select distinct jtag->'tag1' from {dbname}.jsons1")
        tdSql.checkRows(8)
        tdSql.error(f"select distinct jtag from {dbname}.jsons1")

        #test dumplicate key with normal colomn
        tdSql.execute(f"insert into {dbname}.jsons1_15 using {dbname}.jsons1 tags('{{\"tbname\":\"tt\",\"databool\":true,\"datastr\":\"是是是\"}}') values(1591060828000, 4, false, 'jjsf', \"你就会\")")
        tdSql.query(f"select * from {dbname}.jsons1 where jtag->'datastr' match '是' and datastr match 'js'")
        tdSql.checkRows(1)
        tdSql.query(f"select tbname,jtag->'tbname' from {dbname}.jsons1 where jtag->'tbname'='tt' and tbname='jsons1_15'")
        tdSql.checkRows(1)

        # test join
        tdSql.execute(f"create table if not exists {dbname}.jsons2(ts timestamp, dataInt int, dataBool bool, dataStr nchar(50), dataStrBin binary(150)) tags(jtag json)")
        tdSql.execute(f"insert into {dbname}.jsons2_1 using {dbname}.jsons2 tags('{{\"tag1\":\"fff\",\"tag2\":5, \"tag3\":true}}') values(1591060618000, 2, false, 'json2', '你是2')")
        tdSql.execute(f"insert into {dbname}.jsons2_2 using {dbname}.jsons2 tags('{{\"tag1\":5,\"tag2\":null}}') values (1591060628000, 2, true, 'json2', 'sss')")

        tdSql.execute(f"create table if not exists {dbname}.jsons3(ts timestamp, dataInt int, dataBool bool, dataStr nchar(50), dataStrBin binary(150)) tags(jtag json)")
        tdSql.execute(f"insert into {dbname}.jsons3_1 using {dbname}.jsons3 tags('{{\"tag1\":\"fff\",\"tag2\":5, \"tag3\":true}}') values(1591060618000, 3, false, 'json3', '你是3')")
        tdSql.execute(f"insert into {dbname}.jsons3_2 using {dbname}.jsons3 tags('{{\"tag1\":5,\"tag2\":\"beijing\"}}') values (1591060638000, 2, true, 'json3', 'sss')")
        tdSql.execute(f"insert into {dbname}.jsons3_3 using {dbname}.jsons3 tags(NULL) values (1591060638000, 2, true, 'json3', 'sss')")
        tdSql.query(f"select 'sss',33,a.jtag->'tag3' from {dbname}.jsons2 a,jsons3 b where a.ts=b.ts and a.jtag->'tag1'=b.jtag->'tag1'")
        tdSql.checkData(0, 0, "sss")
        tdSql.checkData(0, 2, "true")
        tdSql.query(f"show create table jsons3_3")
        tdSql.checkNotEqual(tdSql.queryResult[0][1].find("TAGS (null)"), 0)

        res = tdSql.getColNameList(f"select 'sss',33,a.jtag->'tag3' from {dbname}.jsons2 a,jsons3 b where a.ts=b.ts and a.jtag->'tag1'=b.jtag->'tag1'")
        cname_list = []
        cname_list.append("'sss'")
        cname_list.append("33")
        cname_list.append("a.jtag->'tag3'")
        tdSql.checkColNameList(res, cname_list)
        #
        # test group by & order by  json tag
        tdSql.query(f"select ts,jtag->'tag1' from {dbname}.jsons1 partition by jtag->'tag1' order by jtag->'tag1' desc")
        tdSql.checkRows(11)
        tdSql.checkData(0, 1, '"收到货"')
        tdSql.checkData(2, 1, '"femail"')
        tdSql.checkData(7, 1, "false")


        tdSql.error(f"select count(*) from {dbname}.jsons1 group by jtag")
        tdSql.error(f"select count(*) from {dbname}.jsons1 partition by jtag")
        tdSql.error(f"select count(*) from {dbname}.jsons1 group by jtag order by jtag")
        tdSql.error(f"select count(*) from {dbname}.jsons1 group by jtag->'tag1' order by jtag->'tag2'")
        tdSql.error(f"select count(*) from {dbname}.jsons1 group by jtag->'tag1' order by jtag")
        tdSql.query(f"select count(*),jtag->'tag1' from {dbname}.jsons1 group by jtag->'tag1' order by jtag->'tag1' desc")
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(0, 1, '"收到货"')
        tdSql.checkData(1, 1, '"femail"')
        tdSql.checkData(1, 0, 2)
        
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(2, 1, "11.000000000")
        tdSql.checkData(5, 0, 1)
        tdSql.checkData(5, 1, "false")

        tdSql.query(f"select count(*),jtag->'tag1' from {dbname}.jsons1 group by jtag->'tag1' order by jtag->'tag1' asc")
        tdSql.checkRows(8)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(2, 1, "false")
        tdSql.checkData(5, 0, 1)
        tdSql.checkData(5, 1, "11.000000000")
        tdSql.checkData(7, 0, 2)
        tdSql.checkData(7, 1, '"收到货"')

        # test stddev with group by json tag
        tdSql.query(f"select stddev(dataint),jtag->'tag1' from {dbname}.jsons1 group by jtag->'tag1' order by jtag->'tag1'")
        tdSql.checkRows(8)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(4, 0, 0)
        tdSql.checkData(4, 1, "5.000000000")
        tdSql.checkData(6, 0, 11)
        tdSql.checkData(7, 1, '"收到货"')

        res = tdSql.getColNameList(f"select stddev(dataint),jsons1.jtag->'tag1' from {dbname}.jsons1 group by jsons1.jtag->'tag1' order by jtag->'tag1'")
        cname_list = []
        cname_list.append("stddev(dataint)")
        cname_list.append("jsons1.jtag->'tag1'")
        tdSql.checkColNameList(res, cname_list)

        # test top/bottom with group by json tag
        tdSql.query(f"select top(dataint,2),jtag->'tag1' from {dbname}.jsons1 group by jtag->'tag1' order by jtag->'tag1'")
        tdSql.checkRows(11)
        tdSql.checkData(0, 1, None)

        # test having
        tdSql.query(f"select count(*),jtag->'tag1' from {dbname}.jsons1 group by jtag->'tag1' having count(*) > 1")
        tdSql.checkRows(3)

        # subquery with json tag
        tdSql.query(f"select * from (select jtag, dataint from {dbname}.jsons1) order by dataint")
        tdSql.checkRows(11)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(5, 0, '{"tag1":false,"tag2":"beijing"}')

        tdSql.error(f"select jtag->'tag1' from (select jtag->'tag1', dataint from {dbname}.jsons1)")
        tdSql.error(f"select t->'tag1' from (select jtag->'tag1' as t, dataint from {dbname}.jsons1)")
        tdSql.error(f"select ts,jtag->'tag1' from (select jtag->'tag1',tbname,ts from {dbname}.jsons1 order by ts)")

        # union all
        tdSql.query(f"select jtag->'tag1' from {dbname}.jsons1 union all select jtag->'tag2' from {dbname}.jsons2")
        tdSql.checkRows(13)
        tdSql.query(f"select jtag->'tag1' from {dbname}.jsons1_1 union all select jtag->'tag2' from {dbname}.jsons2_1")
        tdSql.checkRows(3)

        tdSql.query(f"select jtag->'tag1' from {dbname}.jsons1_1 union all select jtag->'tag1' from {dbname}.jsons2_1")
        tdSql.checkRows(3)
        tdSql.query(f"select dataint,jtag->'tag1',tbname from {dbname}.jsons1 union all select dataint,jtag->'tag1',tbname from {dbname}.jsons2")
        tdSql.checkRows(13)
        tdSql.query(f"select dataint,jtag,tbname from {dbname}.jsons1 union all select dataint,jtag,tbname from {dbname}.jsons2")
        tdSql.checkRows(13)

        #show create table
        tdSql.query(f"show create table {dbname}.jsons1")
        tdSql.checkData(0, 1, 'CREATE STABLE `jsons1` (`ts` TIMESTAMP, `dataint` INT, `databool` BOOL, `datastr` NCHAR(50), `datastrbin` VARCHAR(150)) TAGS (`jtag` JSON)')

        #test aggregate function:count/avg/twa/irate/sum/stddev/leastsquares
        tdSql.query(f"select count(*) from {dbname}.jsons1 where jtag is not null")
        tdSql.checkData(0, 0, 10)
        tdSql.query(f"select avg(dataint) from {dbname}.jsons1 where jtag is not null")
        tdSql.checkData(0, 0, 5.3)
        # tdSql.query(f"select twa(dataint) from {dbname}.jsons1 where jtag is not null")
        # tdSql.checkData(0, 0, 28.386363636363637)
        # tdSql.query(f"select irate(dataint) from {dbname}.jsons1 where jtag is not null")

        tdSql.query(f"select sum(dataint) from {dbname}.jsons1 where jtag->'tag1' is not null")
        tdSql.checkData(0, 0, 45)
        tdSql.query(f"select stddev(dataint) from {dbname}.jsons1 where jtag->'tag1'>1")
        tdSql.checkData(0, 0, 4.496912521)
        tdSql.query(f"select LEASTSQUARES(dataint, 1, 1) from {dbname}.jsons1 where jtag is not null")

        #test selection function:min/max/first/last/top/bottom/percentile/apercentile/last_row/interp
        tdSql.query(f"select min(dataint) from {dbname}.jsons1 where jtag->'tag1'>1")
        tdSql.checkData(0, 0, 1)
        tdSql.query(f"select max(dataint) from {dbname}.jsons1 where jtag->'tag1'>1")
        tdSql.checkData(0, 0, 11)
        tdSql.query(f"select first(dataint) from {dbname}.jsons1 where jtag->'tag1'>1")
        tdSql.checkData(0, 0, 2)
        tdSql.query(f"select last(dataint) from {dbname}.jsons1 where jtag->'tag1'>1")
        tdSql.checkData(0, 0, 11)
        tdSql.query(f"select top(dataint,100) from {dbname}.jsons1 where jtag->'tag1'>1")
        tdSql.checkRows(3)
        tdSql.query(f"select bottom(dataint,100) from {dbname}.jsons1 where jtag->'tag1'>1")
        tdSql.checkRows(3)
        #tdSql.query(f"select percentile(dataint,20) from {dbname}.jsons1 where jtag->'tag1'>1")
        tdSql.query(f"select apercentile(dataint, 50) from {dbname}.jsons1 where jtag->'tag1'>1")
        tdSql.checkData(0, 0, 1.5)
        # tdSql.query(f"select last_row(dataint) from {dbname}.jsons1 where jtag->'tag1'>1")
        # tdSql.query(f"select interp(dataint) from {dbname}.jsons1 where ts = '2020-06-02 09:17:08.000' and jtag->'tag1'>1")

        #test calculation function:diff/derivative/spread/ceil/floor/round/
        tdSql.query(f"select diff(dataint) from {dbname}.jsons1 where jtag->'tag1'>1")
        tdSql.checkRows(2)
        # tdSql.checkData(0, 0, -1)
        # tdSql.checkData(1, 0, 10)
        tdSql.query(f"select derivative(dataint, 10m, 0) from {dbname}.jsons1 where jtag->'tag1'>1")
        # tdSql.checkData(0, 0, -2)
        tdSql.query(f"select spread(dataint) from {dbname}.jsons1 where jtag->'tag1'>1")
        tdSql.checkData(0, 0, 10)
        tdSql.query(f"select ceil(dataint) from {dbname}.jsons1 where jtag->'tag1'>1")
        tdSql.checkRows(3)
        tdSql.query(f"select floor(dataint) from {dbname}.jsons1 where jtag->'tag1'>1")
        tdSql.checkRows(3)
        tdSql.query(f"select round(dataint) from {dbname}.jsons1 where jtag->'tag1'>1")
        tdSql.checkRows(3)

        #math function
        tdSql.query(f"select sin(dataint) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select cos(dataint) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select tan(dataint) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select asin(dataint) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select acos(dataint) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select atan(dataint) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select ceil(dataint) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select floor(dataint) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select round(dataint) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select abs(dataint) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select pow(dataint,5) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select log(dataint,10) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select sqrt(dataint) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select HISTOGRAM(dataint,'user_input','[1, 33, 555, 7777]',1)  from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select csum(dataint) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select mavg(dataint,1) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select statecount(dataint,'GE',10) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select stateduration(dataint,'GE',0) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select sample(dataint,3) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select HYPERLOGLOG(dataint) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(1)
        tdSql.query(f"select twa(dataint) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(1)

        # function not ready
        tdSql.query(f"select tail(dataint,1) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(1)
        tdSql.query(f"select unique(dataint) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select mode(dataint) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(1)
        tdSql.query(f"select irate(dataint) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(1)

        #str function
        tdSql.query(f"select upper(dataStr) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select ltrim(dataStr) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select lower(dataStr) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select rtrim(dataStr) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select LENGTH(dataStr) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select CHAR_LENGTH(dataStr) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select SUBSTR(dataStr,5) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select CONCAT(dataStr,dataStrBin) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select CONCAT_ws('adad!@!@%$^$%$^$%^a',dataStr,dataStrBin) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select CAST(dataStr as bigint) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)

        #time function
        tdSql.query(f"select now() from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select today() from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select TIMEZONE() from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select TO_ISO8601(ts) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select TO_UNIXTIMESTAMP(datastr) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select TIMETRUNCATE(ts,1s) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select TIMEDIFF(ts,_c0) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select TIMEDIFF(ts,1u) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(3)
        tdSql.query(f"select ELAPSED(ts,1h) from {dbname}.jsons1 where jtag->'tag1'>1;")
        tdSql.checkRows(1)

        # to_json()
        tdSql.query(f"select to_json('{{\"abc\":123}}') from {dbname}.jsons1_1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, '{"abc":123}')
        tdSql.checkData(1, 0, '{"abc":123}')
        tdSql.query(f"select to_json('null') from {dbname}.jsons1_1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 'null')
        tdSql.checkData(1, 0, 'null')
        tdSql.query(f"select to_json('{{\"key\"}}') from {dbname}.jsons1_1")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 'null')
        tdSql.checkData(1, 0, 'null')

        #test TD-12077
        tdSql.execute(f"insert into {dbname}.jsons1_16 using {dbname}.jsons1 tags('{{\"tag1\":\"收到货\",\"tag2\":\"\",\"tag3\":-2.111}}') values(1591062628000, 2, NULL, '你就会', 'dws')")
        tdSql.query(f"select jtag->'tag3' from {dbname}.jsons1_16")
        tdSql.checkData(0, 0, '-2.111000000')

        # test TD-12452
        tdSql.execute(f"ALTER table {dbname}.jsons1_1 SET TAG jtag=NULL")
        tdSql.query(f"select jtag from {dbname}.jsons1_1")
        tdSql.checkData(0, 0, None)
        tdSql.execute(f"create TABLE if not exists {dbname}.jsons1_20 using {dbname}.jsons1 tags(NULL)")
        tdSql.query(f"select jtag from {dbname}.jsons1_20")
        tdSql.checkRows(0)
        tdSql.execute(f"insert into {dbname}.jsons1_21 using {dbname}.jsons1 tags(NULL) values(1591061628000, 11, false, '你就会','')")
        tdSql.query(f"select jtag from {dbname}.jsons1_21")
        tdSql.checkData(0, 0, None)
        #
        # #test TD-12389
        tdSql.query("describe jsons1")
        tdSql.checkData(5, 2, 4095)
        tdSql.query("describe jsons1_1")
        tdSql.checkData(5, 2, 4095)
        #
        # #test TD-13918
        tdSql.execute(f"drop table  if exists {dbname}.jsons_13918_1")
        tdSql.execute(f"drop table  if exists {dbname}.jsons_13918_2")
        tdSql.execute(f"drop table  if exists {dbname}.jsons_13918_3")
        tdSql.execute(f"drop table  if exists {dbname}.jsons_13918_4")
        tdSql.execute(f"drop table  if exists {dbname}.jsons_stb")
        tdSql.execute(f"create table {dbname}.jsons_stb (ts timestamp, dataInt int) tags (jtag json)")
        tdSql.error(f"create table {dbname}.jsons_13918_1 using {dbname}.jsons_stb tags ('nullx')")
        tdSql.error(f"create table {dbname}.jsons_13918_2 using {dbname}.jsons_stb tags (nullx)")
        tdSql.error(f"insert into {dbname}.jsons_13918_3 using {dbname}.jsons_stb tags('NULLx') values(1591061628001, 11)")
        tdSql.error(f"insert into {dbname}.jsons_13918_4 using {dbname}.jsons_stb tags(NULLx) values(1591061628002, 11)")
        tdSql.execute(f"create table {dbname}.jsons_13918_1 using {dbname}.jsons_stb tags ('null')")
        tdSql.execute(f"create table {dbname}.jsons_13918_2 using {dbname}.jsons_stb tags (null)")
        tdSql.execute(f"insert into {dbname}.jsons_13918_1 values(1591061628003, 11)")
        tdSql.execute(f"insert into {dbname}.jsons_13918_2 values(1591061628004, 11)")
        tdSql.execute(f"insert into {dbname}.jsons_13918_3 using {dbname}.jsons_stb tags('NULL') values(1591061628005, 11)")
        tdSql.execute(f"insert into {dbname}.jsons_13918_4 using {dbname}.jsons_stb tags(\"NULL\") values(1591061628006, 11)")
        tdSql.query(f"select * from {dbname}.jsons_stb")
        tdSql.checkRows(4)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
