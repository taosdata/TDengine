# -*- coding: utf-8 -*-

import sys
import time
import random

from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import *

class TDTestCase:
    """
        check values of  tables in information_schema
    """
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.dbname = 'dd'
        self.stable = 'meters'
        self.table = 'd0'
        self.ins_list_count = 32 #  table number  in information_schema db
        self.rename_tag = 'add_newtag_rename'
        self.drop_tag = 'drop_tag'
    def prepareData(self):
        # db
        tdSql.execute(f"create database {self.dbname};")
        tdSql.execute(f"use {self.dbname};")
        tdLog.debug(f"Create database {self.dbname}")

        # create super table
        tdSql.execute(
            f"create stable {self.stable} (ts timestamp, voltage int)tags(`bin2` binary(8),`var3` varchar(10),`varb4` varbinary(16),"
            f"`bool1` bool,`geometry2` geometry(21), `groupid` int, `bigint1` bigint, "
            f"`bigintunsigned1` bigint unsigned, `smallintunsigned1` smallint unsigned,`nchar1` nchar(1))")

        tdLog.debug("Create super table %s" % self.stable)


        #alter add different type tags nchar(1) /int
        tdSql.execute(f"alter stable  {self.stable} add tag add_newtag0 nchar(20);")
        tdSql.execute(f"alter stable  {self.stable} add tag {self.drop_tag} nchar(10);")
        tdLog.debug("alter super table add tag  %s" % self.stable)


        #insert into d0 all tag value is 0/null/'' except values which can't be null
        tdSql.execute(f''' insert into {self.table} using {self.stable} tags('','','1','true','point(1 1)',null,0,4,5,'中','','dr')
        values('2017-07-14T10:40:00.000+08:00',10);''')

    def test_alter_change_querey(self):
        #alter rename /set/ drop
        rename_flag, drop_flag= False,True
        tdSql.execute(f"use {self.dbname};")

        # rename
        tdSql.execute(f"alter stable {self.stable} rename tag add_newtag0 {self.rename_tag};")
        tdSql.query(f"select tag_name,tag_value from information_schema.ins_tags where db_name='{self.dbname}' and stable_name='{self.stable}';")
        # prepare data just 1 row, can insert more rows in the future
        for i in tdSql.queryResult:
            if i[0] == self.rename_tag:
                tdSql.checkEqual(i[1],'')
                rename_flag = True
        if not rename_flag:
            tdLog.exit("alter rename tag name is not expected name")

        #set
        #set stable table is not support
        sql_1= f"alter stable {self.stable} set tag {self.rename_tag}='涛思data';"
        tdSql.error(sql_1)

        # set child table value from null to not null, accurate search
        tdSql.execute(f'alter table {self.table}  set tag {self.rename_tag}="涛思data";')
        tdSql.query(f"select tag_value from information_schema.ins_tags where tag_name='{self.rename_tag}';")
        tdSql.checkData(0,0,"涛思data")
        tdLog.debug("alter set query end")

        # drop 1 tag, check total tag numbers
        # drop child table,is not support
        sql_2 = f"alter table  {self.table} drop tag {self.drop_tag};"
        tdSql.error(sql_2)

        # drop super table, iterate check drop tag is not exist
        tdSql.execute(f"alter stable  {self.stable} drop tag {self.drop_tag};")
        tdSql.query(f"select tag_name from information_schema.ins_tags where db_name='{self.dbname}' and stable_name='{self.stable}'")
        # 12 tags drop 1, result count = 11
        for i in tdSql.queryResult:
            if i[0] == self.drop_tag:
                drop_flag = False
        if not drop_flag:
            tdLog.exit("alter drop tag failure")

    def test_complex_query(self):
        #offset/nest/union/like/count/avg/max/distinct (other case can find in information_schema.py)
        #offset/limit
        tdSql.query(f"select tag_name,tag_value from information_schema.ins_tags where db_name='{self.dbname}' "
                      f"order by tag_value desc limit 3 offset 8;")  # only 3 tags value are null
        #check null value
        tdSql.checkData(0, 1, '') # nchar_tag is null
        tdSql.checkData(1, 1, '') # bin2 tag is null
        tdSql.checkData(2, 1, None) # var2 tag is null

        #nest/max/min
        tdSql.query(f" select max(b), min(b) from (select length(a) as b"
                     f" from (select tag_value as a from information_schema.ins_tags where db_name='{self.dbname}'));")
        tdSql.checkData(0,0,25) # point
        tdSql.checkData(0,1,0)
        #union delete duplicate / like
        tdSql.query(f"select tag_name from information_schema.ins_tags where db_name='{self.dbname}' union "
                      f" select tag_name from information_schema.ins_tags where db_name in ('{self.dbname}') and tag_name like '%nchar%';")
        result = tdSql.queryResult
        tdSql.checkEqual(len(result), 11)
        # distinct
        tdSql.query(f"select distinct(tag_value) from information_schema.ins_tags where db_name='{self.dbname}' ")
        result = tdSql.queryResult
        tdSql.checkEqual(len(result), 10)
        #avg
        tdSql.query(f"select avg(ntables) from information_schema.ins_databases where name='information_schema';")
        tdSql.checkData(0, 0, '32.0')
        # count(*)/count(1)
        tdSql.query(f" select count(*) from information_schema.ins_databases where name='{self.dbname}';")
        tdSql.checkData(0, 0, 1)
        tdSql.query(f" select count(1) from information_schema.ins_databases  where name='{self.dbname}';")
        tdSql.checkData(0, 0, 1)
        # compute
        tdSql.query(f" select length(tag_name) - 1 + 2 from information_schema.ins_tags where db_name='{self.dbname}';")
        tdSql.checkData(0, 0, 5.000000000000000)
        tdSql.checkData(1, 0, 5.000000000000000)
        # partition by
        tdSql.query(f"select tag_name,tag_value from information_schema.ins_tags where db_name ='{self.dbname}' partition by tag_value;")
        # can get all rows 11
        result = tdSql.queryResult
        tdSql.checkEqual(len(result), 11)
        #sum
        tdSql.query(f"select sum(columns) from information_schema.ins_tables where db_name = '{self.dbname}';")
        tdSql.checkData(0, 0, 2)

    def run(self):
        self.prepareData()
        self.test_alter_change_querey()
        self.test_complex_query()


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
