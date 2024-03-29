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
        tdSql.query(f'select * from information_schema.ins_streams where stream_name = "{stream_name}"')
        print(tdSql.queryResult)
        tdSql.checkEqual(tdSql.queryResult[0][2],f'create stream {stream_name} trigger at_once ignore expired 0 into stb as select * from {self.dbname}.{stbname} partition by tbname')
        tdSql.execute(f'drop stream {stream_name}')
        tdSql.execute(f'create stream {stream_name} trigger at_once ignore expired 0 into stb1 as select * from tb')
        tdSql.query(f'select * from information_schema.ins_streams where stream_name = "{stream_name}"')
        tdSql.checkEqual(tdSql.queryResult[0][2],f'create stream {stream_name} trigger at_once ignore expired 0 into stb1 as select * from tb')
        tdSql.execute(f'drop database {self.dbname}')
    def run(self):
        self.drop_ntb_check()
        self.drop_stb_ctb_check()
        self.drop_topic_check()
        self.drop_stream_check()
        pass
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
