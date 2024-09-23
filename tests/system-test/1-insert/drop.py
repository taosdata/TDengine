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


import math
from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import *

class TDTestCase:
    updatecfgDict = {'stdebugflag':143}
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.setsql = TDSetSql()
        self.dbname = 'db'
        self.ntbname = f"{self.dbname}.ntb"
        self.rowNum = 10
        self.tbnum = 20
        self.ts = 1537146000000
        self.binary_str = 'taosdata'
        self.nchar_str = '涛思数据'
        self.column_dict = {
            'ts'  : 'timestamp',
            'col1': 'tinyint',
            'col2': 'smallint',
            'col3': 'int',
            'col4': 'bigint',
            'col5': 'tinyint unsigned',
            'col6': 'smallint unsigned',
            'col7': 'int unsigned',
            'col8': 'bigint unsigned',
            'col9': 'float',
            'col10': 'double',
            'col11': 'bool',
            'col12': 'binary(20)',
            'col13': 'nchar(20)'
        }
        self.db_names = [ f'dbtest_0', f'dbtest_1']
        self.stb_names = [ f'aa\u00bf\u200bstb0']
        self.ctb_names = [ f'ctb0', 'ctb1', f'aa\u00bf\u200bctb0', f'aa\u00bf\u200bctb1']
        self.ntb_names = [ f'ntb0', f'aa\u00bf\u200bntb0', f'ntb1', f'aa\u00bf\u200bntb1']
        self.vgroups_opt = f'vgroups 4'
    def insert_data(self,column_dict,tbname,row_num):
        insert_sql = self.setsql.set_insertsql(column_dict,tbname,self.binary_str,self.nchar_str)
        for i in range(row_num):
            insert_list = []
            self.setsql.insert_values(column_dict,i,insert_sql,insert_list,self.ts) 
    def drop_ntb_check(self):
        tdSql.execute(f'create database if not exists {self.dbname} replica {self.replicaVar} wal_retention_period 3600')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(self.setsql.set_create_normaltable_sql(self.ntbname,self.column_dict))
        self.insert_data(self.column_dict,self.ntbname,self.rowNum)
        for k,v in self.column_dict.items():
            if v.lower() == "timestamp":
                tdSql.query(f'select * from {self.ntbname} where {k} = {self.ts}')
                tdSql.checkRows(1)
        tdSql.execute(f'drop table {self.ntbname}')
        tdSql.execute(f'flush database {self.dbname}')
        tdSql.execute(self.setsql.set_create_normaltable_sql(self.ntbname,self.column_dict))
        self.insert_data(self.column_dict,self.ntbname,self.rowNum)
        for k,v in self.column_dict.items():
            if v.lower() == "timestamp":
                tdSql.query(f'select * from {self.ntbname} where {k} = {self.ts}')
                tdSql.checkRows(1)
        tdSql.execute(f'drop database {self.dbname}')
    
    def drop_stb_ctb_check(self):
        stbname = f'{self.dbname}.{tdCom.getLongName(5,"letters")}'
        tag_dict = {
            't0':'int'
        }
        tag_values = [
            f'1'
            ]
        tdSql.execute(f"create database if not exists {self.dbname} replica {self.replicaVar} wal_retention_period 3600")
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(self.setsql.set_create_stable_sql(stbname,self.column_dict,tag_dict))
        for i in range(self.tbnum):
            tdSql.execute(f"create table {stbname}_{i} using {stbname} tags({tag_values[0]})")
            self.insert_data(self.column_dict,f'{stbname}_{i}',self.rowNum)
        for k,v in self.column_dict.items():
            for i in range(self.tbnum):
                if v.lower() == "timestamp":
                    tdSql.query(f'select * from {stbname}_{i} where {k} = {self.ts}')
                    tdSql.checkRows(1)
                    tdSql.execute(f'drop table {stbname}_{i}')
        tdSql.execute(f'flush database {self.dbname}')
        for i in range(self.tbnum):
            tdSql.execute(f"create table {stbname}_{i} using {stbname} tags({tag_values[0]})")
            self.insert_data(self.column_dict,f'{stbname}_{i}',self.rowNum)
        for k,v in self.column_dict.items():
            for i in range(self.tbnum):
                if v.lower() == "timestamp":
                    tdSql.query(f'select * from {stbname}_{i} where {k} = {self.ts}')
                    tdSql.checkRows(1)
            if v.lower() == "timestamp":
                tdSql.query(f'select * from {stbname} where {k} = {self.ts}')
                tdSql.checkRows(self.tbnum) 
        tdSql.execute(f'drop table {stbname}')
        tdSql.execute(f'flush database {self.dbname}')
        tdSql.execute(self.setsql.set_create_stable_sql(stbname,self.column_dict,tag_dict))
        for i in range(self.tbnum):
            tdSql.execute(f"create table {stbname}_{i} using {stbname} tags({tag_values[0]})")
            self.insert_data(self.column_dict,f'{stbname}_{i}',self.rowNum)
        for k,v in self.column_dict.items():
            if v.lower() == "timestamp":
                tdSql.query(f'select * from {stbname} where {k} = {self.ts}')
                tdSql.checkRows(self.tbnum) 
        tdSql.execute(f'drop database {self.dbname}')
    def drop_table_check_init(self):
        for db_name in self.db_names:
            tdSql.execute(f'create database if not exists {db_name} {self.vgroups_opt}')
            tdSql.execute(f'use {db_name}')
            for stb_name in self.stb_names:
                tdSql.execute(f'create table `{stb_name}` (ts timestamp,c0 int) tags(t0 int)')
                for ctb_name in self.ctb_names:
                    tdSql.execute(f'create table `{ctb_name}` using `{stb_name}` tags(0)')
                    tdSql.execute(f'insert into `{ctb_name}` values (now,1)')
            for ntb_name in self.ntb_names:
                tdSql.execute(f'create table `{ntb_name}` (ts timestamp,c0 int)')
                tdSql.execute(f'insert into `{ntb_name}` values (now,1)')
    def drop_table_check_end(self):
        for db_name in self.db_names:
            tdSql.execute(f'drop database {db_name}')
    def drop_stable_with_check(self):
        self.drop_table_check_init()
        tdSql.query(f'select * from information_schema.ins_stables where db_name like "dbtest_%"')
        result = tdSql.queryResult
        print(result)
        tdSql.checkEqual(len(result),2)
        i = 0
        for stb_result in result:
            if i == 0:
                dropTable = f'drop table with `{stb_result[1]}`.`{stb_result[10]}`,'
                dropStable = f'drop stable with `{stb_result[1]}`.`{stb_result[10]}`,'
            else:
                dropTable += f'`{stb_result[1]}`.`{stb_result[10]}`,'
                dropStable += f'`{stb_result[1]}`.`{stb_result[10]}`,'
                tdLog.info(dropTable[:-1])
                tdLog.info(dropStable[:-1])
                tdSql.error(dropTable[:-1])
                tdSql.error(dropStable[:-1])
            i += 1
        i = 0
        for stb_result in result:
            if i == 0:
                tdSql.execute(f'drop table with `{stb_result[1]}`.`{stb_result[10]}`')
            else:
                tdSql.execute(f'drop stable with `{stb_result[1]}`.`{stb_result[10]}`')
            i += 1
        for i in range(30):
            tdSql.query(f'select * from information_schema.ins_stables where db_name like "dbtest_%"')
            if(len(tdSql.queryResult) == 0):
                break
            tdLog.info(f'ins_stables not empty, sleep 1s')
            time.sleep(1)
        tdSql.query(f'select * from information_schema.ins_stables where db_name like "dbtest_%"')
        tdSql.checkRows(0)
        tdSql.query(f'select * from information_schema.ins_tables where db_name like "dbtest_%"')
        tdSql.checkRows(8)
        tdSql.error(f'drop stable with information_schema.`ins_tables`;')
        tdSql.error(f'drop stable with performance_schema.`perf_connections`;')
        self.drop_table_check_end()    
    def drop_table_with_check(self):
        self.drop_table_check_init()
        tdSql.query(f'select * from information_schema.ins_tables where db_name like "dbtest_%"')
        result = tdSql.queryResult
        print(result)
        tdSql.checkEqual(len(result),16)
        dropTable = f'drop table with '
        for tb_result in result:
            dropTable += f'`{tb_result[1]}`.`{tb_result[5]}`,'
        tdLog.info(dropTable[:-1])
        tdSql.execute(dropTable[:-1])
        for i in range(30):
            tdSql.query(f'select * from information_schema.ins_tables where db_name like "dbtest_%"')
            if(len(tdSql.queryResult) == 0):
                break
            tdLog.info(f'ins_tables not empty, sleep 1s')
            time.sleep(1)
        tdSql.query(f'select * from information_schema.ins_tables where db_name like "dbtest_%"')
        tdSql.checkRows(0)
        tdSql.query(f'select * from information_schema.ins_stables where db_name like "dbtest_%"')
        tdSql.checkRows(2)
        tdSql.error(f'drop table with information_schema.`ins_tables`;')
        tdSql.error(f'drop table with performance_schema.`perf_connections`;')
        self.drop_table_check_end()
    def drop_table_with_check_tsma(self):
        tdSql.execute(f'create database if not exists {self.dbname} {self.vgroups_opt}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'create table {self.dbname}.stb (ts timestamp,c0 int) tags(t0 int)')
        tdSql.execute(f'create tsma stb_tsma on {self.dbname}.stb function(avg(c0),count(c0)) interval(1d)')
        tdSql.execute(f'create table {self.dbname}.ctb using {self.dbname}.stb tags(0)')
        tdSql.execute(f'insert into {self.dbname}.ctb values (now,1)')
        tdSql.execute(f'create table {self.dbname}.ntb (ts timestamp,c0 int)')
        tdSql.execute(f'create tsma ntb_tsma on {self.dbname}.ntb function(avg(c0),count(c0)) interval(1d)')
        tdSql.execute(f'insert into {self.dbname}.ntb values (now,1)')
        tdSql.query(f'select * from information_schema.ins_tsmas where db_name = "{self.dbname}"')
        tdSql.checkRows(2)
        tdSql.query(f'select * from information_schema.ins_tables where db_name = "{self.dbname}" and type="CHILD_TABLE"')
        tdSql.checkRows(1)
        tdSql.execute(f'drop table with {tdSql.queryResult[0][1]}.`{tdSql.queryResult[0][5]}`')
        tdSql.query(f'select * from information_schema.ins_tables where db_name = "{self.dbname}" and type="CHILD_TABLE"')
        tdSql.checkRows(0)
        tdSql.query(f'select * from information_schema.ins_stables where db_name = "{self.dbname}"')
        tdSql.checkRows(1)
        tdSql.error(f'drop table with {tdSql.queryResult[0][1]}.`{tdSql.queryResult[0][10]}`')
        tdSql.query(f'select * from information_schema.ins_stables where db_name = "{self.dbname}"')
        tdSql.error(f'drop stable with {tdSql.queryResult[0][1]}.`{tdSql.queryResult[0][10]}`')
        tdSql.query(f'select * from information_schema.ins_stables where db_name = "{self.dbname}"')
        tdSql.checkRows(1)
        tdSql.query(f'select * from information_schema.ins_tables where db_name = "{self.dbname}" and type="NORMAL_TABLE"')
        tdSql.checkRows(1)
        tdSql.execute(f'drop table with {tdSql.queryResult[0][1]}.`{tdSql.queryResult[0][5]}`')
        tdSql.query(f'select * from information_schema.ins_tables where db_name = "{self.dbname}" and type="NORMAL_TABLE"')
        tdSql.checkRows(0)
        tdSql.query(f'select * from information_schema.ins_tsmas where db_name = "{self.dbname}"')
        tsmas = tdSql.queryResult
        tdSql.checkEqual(len(tsmas),2)
        for tsma in tsmas:
            tdSql.execute(f'drop tsma {tsma[1]}.{tsma[0]}')
        tdSql.query(f'show tsmas')
        tdSql.checkRows(0)
        tdSql.execute(f'drop database {self.dbname}')
    def drop_topic_check(self):
        tdSql.execute(f'create database {self.dbname} replica {self.replicaVar} wal_retention_period 3600')
        tdSql.execute(f'use {self.dbname}')
        stbname = tdCom.getLongName(5,"letters")
        topic_name = tdCom.getLongName(5,"letters")
        tdSql.execute(f'create table {stbname} (ts timestamp,c0 int) tags(t0 int)')
        tdSql.execute(f'create topic {topic_name} as select * from {self.dbname}.{stbname}')
        tdSql.query(f'select * from information_schema.ins_topics where topic_name = "{topic_name}"')
        tdSql.checkEqual(tdSql.queryResult[0][3],f'create topic {topic_name} as select * from {self.dbname}.{stbname}')
        tdSql.execute(f'drop topic {topic_name}')
        tdSql.execute(f'create topic {topic_name} as select c0 from {self.dbname}.{stbname}')
        tdSql.query(f'select * from information_schema.ins_topics where topic_name = "{topic_name}"')
        tdSql.checkEqual(tdSql.queryResult[0][3],f'create topic {topic_name} as select c0 from {self.dbname}.{stbname}')
        tdSql.execute(f'drop topic {topic_name}')

        #TD-25222
        long_topic_name="hhhhjjhhhhqwertyuiasdfghjklzxcvbnmhhhhjjhhhhqwertyuiasdfghjklzxcvbnmhhhhjjhhhhqwertyuiasdfghjklzxcvbnm"
        tdSql.execute(f'create topic {long_topic_name} as select * from {self.dbname}.{stbname}')
        tdSql.execute(f'drop topic {long_topic_name}')

        tdSql.execute(f'drop database {self.dbname}')

    def drop_stream_check(self):
        tdSql.execute(f'create database {self.dbname} replica 1 wal_retention_period 3600')
        tdSql.execute(f'use {self.dbname}')
        stbname = tdCom.getLongName(5,"letters")
        stream_name = tdCom.getLongName(5,"letters")
        tdSql.execute(f'create table {stbname} (ts timestamp,c0 int) tags(t0 int)')
        tdSql.execute(f'create table tb using {stbname} tags(1)')
        tdSql.execute(f'create stream {stream_name} trigger at_once ignore expired 0 into stb as select * from {self.dbname}.{stbname} partition by tbname')
        time.sleep(5)

        tdSql.query(f'select * from information_schema.ins_streams where stream_name = "{stream_name}"')
        print(tdSql.queryResult)
        tdSql.checkEqual(tdSql.queryResult[0][4],f'create stream {stream_name} trigger at_once ignore expired 0 into stb as select * from {self.dbname}.{stbname} partition by tbname')
        tdSql.execute(f'drop stream {stream_name}')
        tdSql.execute(f'create stream {stream_name} trigger at_once ignore expired 0 into stb1 as select * from tb')
        time.sleep(5)

        tdSql.query(f'select * from information_schema.ins_streams where stream_name = "{stream_name}"')
        tdSql.checkEqual(tdSql.queryResult[0][4],f'create stream {stream_name} trigger at_once ignore expired 0 into stb1 as select * from tb')
        tdSql.execute(f'drop database {self.dbname}')
    def run(self):
        self.drop_ntb_check()
        self.drop_stb_ctb_check()
        self.drop_stable_with_check()
        self.drop_table_with_check()
        self.drop_table_with_check_tsma()
        self.drop_topic_check()
        if platform.system().lower() == 'windows':        
            self.drop_stream_check()
        pass
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
