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

import random
import string

from util.log import *
from util.cases import *
from util.sql import *
from util.common import *

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        # prepare data
        self.ntbname = 'ntb'
        self.stbname = 'stb'
        self.column_dict = {
            'ts':'timestamp',
            'c1':'int',
            'c2':'float',
            'c3':'double',
            'c4':'timestamp'
        }
        self.tag_dict = {
            't0':'int'
        }
        self.comment_length = [0,1024]
        self.error_comment_length = [1025]
        self.table_type_list = ['normal_table','stable','child_table']
        self.comment_flag_list = [True,False]

    def __set_and_alter_comment(self,tb_type='',comment_flag= False):

        column_sql = ''
        tag_sql = ''
        for k,v in self.column_dict.items():
            column_sql += f"{k} {v},"
        for k,v in self.tag_dict.items():
            tag_sql += f"{k} {v},"
        if tb_type == 'normal_table' or tb_type == '':
            if comment_flag == False:
                tdSql.execute(f'create table {self.ntbname} ({column_sql[:-1]})')
                self.check_comment_info()
                self.alter_comment(self.ntbname)
                tdSql.execute(f'drop table {self.ntbname}')
            elif comment_flag == True:
                for i in self.comment_length:
                    comment_info = tdCom.getLongName(i)
                    tdSql.execute(f'create table {self.ntbname} ({column_sql[:-1]}) comment "{comment_info}"')
                    self.check_comment_info(comment_info)
                    self.alter_comment(self.ntbname)
                    tdSql.execute(f'drop table {self.ntbname}')
                for i in self.error_comment_length:
                    comment_info = tdCom.getLongName(i)
                    tdSql.error(f'create table {self.ntbname} ({column_sql[:-1]}) comment "{comment_info}"')
        elif tb_type == 'stable':
            for operation in ['table','stable']:
                if comment_flag == False:
                    tdSql.execute(f'create {operation} {self.stbname} ({column_sql[:-1]}) tags({tag_sql[:-1]})')
                    self.check_comment_info(None,'stable')
                    self.alter_comment(self.stbname,'stable')
                    tdSql.execute(f'drop table {self.stbname}')
                elif comment_flag == True:
                    for i in self.comment_length:
                        comment_info = tdCom.getLongName(i)
                        tdSql.execute(f'create {operation} {self.stbname} ({column_sql[:-1]}) tags({tag_sql[:-1]}) comment "{comment_info}"')
                        self.check_comment_info(comment_info,'stable')
                        self.alter_comment(self.stbname,'stable')
                        tdSql.execute(f'drop table {self.stbname}')
        elif tb_type == 'child_table':
            tdSql.execute(f'create table if not exists {self.stbname} ({column_sql[:-1]}) tags({tag_sql[:-1]})')
            if comment_flag == False:
                tdSql.execute(f'create table if not exists {self.stbname}_ctb using {self.stbname} tags(1)')
                self.check_comment_info()
                self.alter_comment(f'{self.stbname}_ctb')
                tdSql.execute(f'drop table {self.stbname}_ctb')
            elif comment_flag == True:
                for j in self.comment_length:
                    comment_info = tdCom.getLongName(j)
                    tdSql.execute(f'create table if not exists {self.stbname}_ctb using {self.stbname} tags(1) comment "{comment_info}"')
                    self.check_comment_info(comment_info)
                    self.alter_comment(f'{self.stbname}_ctb')
                    tdSql.execute(f'drop table {self.stbname}_ctb')
            tdSql.execute(f'drop table {self.stbname}')
    def alter_comment(self,tbname,tb_type=''):
        for i in self.comment_length:
            comment_info = tdCom.getLongName(i)
            print(comment_info)
            tdSql.execute(f'alter table {tbname} comment "{comment_info}"')
            self.check_comment_info(comment_info,tb_type)
        for i in self.error_comment_length:
            comment_info = tdCom.getLongName(i)
            tdSql.error(f'alter table {tbname} comment "{comment_info}"')
    def check_comment_info(self,comment_info=None,tb_type=''):
        if tb_type == '' or tb_type == 'normal_table' or tb_type == 'child_table':
            tdSql.query('select * from information_schema.ins_tables where db_name = \'db\'')
            if comment_info == None:
                tdSql.checkEqual(tdSql.queryResult[0][8],None)
            else :
                tdSql.checkEqual(tdSql.queryResult[0][8],comment_info)
        elif tb_type == 'stable':
            tdSql.query('select * from information_schema.ins_stables where db_name = \'db\'')
            if comment_info == None:
                tdSql.checkEqual(tdSql.queryResult[0][6],None)
            else :
                tdSql.checkEqual(tdSql.queryResult[0][6],comment_info)
    def comment_check_case(self,table_type,comment_flag):
        tdSql.prepare()
        for tb in table_type:
            for flag in comment_flag:
                self.__set_and_alter_comment(tb,flag)
        tdSql.execute('drop database db')

    def run(self):
        self.comment_check_case(self.table_type_list,self.comment_flag_list)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
