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


from util import constant
from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import *
from util.cluster import *
class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.dnode_num=len(cluster.dnodes)
        self.dbname = 'db_test'
        self.setsql = TDSetSql()
        self.stbname = f'{self.dbname}.stb'
        self.rowNum = 5
        self.tbnum = 10
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
        self.replica = [1,3]

    def insert_data(self,column_dict,tbname,row_num):
        insert_sql = self.setsql.set_insertsql(column_dict,tbname,self.binary_str,self.nchar_str)
        for i in range(row_num):
            insert_list = []
            self.setsql.insert_values(column_dict,i,insert_sql,insert_list,self.ts)
    def prepare_data(self,dbname,stbname,column_dict,tbnum,rowNum,replica):
        tag_dict = {
            't0':'int'
        }
        tag_values = [
            f'1'
            ]
        tdSql.execute(f"create database if not exists {dbname} vgroups 1 replica {replica} ")
        tdSql.execute(f'use {dbname}')
        tdSql.execute(self.setsql.set_create_stable_sql(stbname,column_dict,tag_dict))
        for i in range(tbnum):
            tdSql.execute(f"create table {stbname}_{i} using {stbname} tags({tag_values[0]})")
            self.insert_data(self.column_dict,f'{stbname}_{i}',rowNum)
    def redistribute_vgroups(self,replica,stbname,tbnum,rownum):
        tdSql.query('show vgroups')
        vnode_id = tdSql.queryResult[0][0]
        if replica == 1:
            for dnode_id in range(1,self.dnode_num+1) :
                tdSql.execute(f'redistribute vgroup {vnode_id} dnode {dnode_id}')
                tdSql.query(f'select count(*) from {stbname}')
                tdSql.checkEqual(tdSql.queryResult[0][0],tbnum*rownum)
        elif replica == 3:
            for dnode_id in range(1,self.dnode_num-1):
                tdSql.execute(f'redistribute vgroup {vnode_id} dnode {dnode_id} dnode {dnode_id+1} dnode {dnode_id+2}')
                tdSql.query(f'select count(*) from {stbname}')
                tdSql.checkEqual(tdSql.queryResult[0][0],tbnum*rownum)
        
    def run(self):
        for replica in self.replica:
            self.prepare_data(self.dbname,self.stbname,self.column_dict,self.tbnum,self.rowNum,replica)
            self.redistribute_vgroups(replica,self.stbname,self.tbnum,self.rowNum)
            tdSql.execute(f'drop database {self.dbname}')

           
       
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())