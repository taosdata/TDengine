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

import sys
import taos
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
import random


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        

    def run(self):
        tdSql.prepare()   
        # test case for https://jira.taosdata.com:18080/browse/TD-5021    
        
        tdLog.info("\n\n----------step1 : drop db and create db----------\n")
        tdSql.execute('''drop database if exists db ;''')
        tdSql.execute('''create database db ;''')
        sql = '''select * from information_schema.ins_databases;'''
        tdSql.query(sql)
        tdSql.checkRows(1)

        tdLog.info("\n\n----------step2 : create stable----------\n")
        tdSql.execute('''create stable 
                    db.stable_1 (ts timestamp, payload binary(256)) 
                    tags(t1 binary(16),t2 int);''')
        sql = '''select * from information_schema.ins_stables where db_name = 'db';'''
        tdSql.query(sql)
        tdSql.checkRows(1)

        tdLog.info("\n\n----------step3 : create table and insert----------\n")
        sql = '''insert into db.table1 using db.stable_1 (t1 , t2) tags ("table_1" , 111) ( values (now,  ;'''
        tdLog.info(sql)
        tdSql.error(sql)
        try:
            tdSql.execute(sql)
            tdLog.exit(" unexpected token")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("DB error: syntax error near ', ;' (unexpected token)")  

        sql = '''insert into db.table1(ts , payload) using db.stable_1 (t1 , t2) tags ("table_1" , 111) ( values (now,  ;'''
        tdLog.info(sql)
        tdSql.error(sql)
        try:
            tdSql.execute(sql)
            tdLog.exit(" bind columns again")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("DB error: syntax error near ', ;'  (bind columns again)") 

        sql = '''insert into db.table1 using db.stable_1 (t1 , t2) tags ("table_1",111) (ts , payload) ( values (now,  ;'''
        tdLog.info(sql)
        tdSql.error(sql)
        try:
            tdSql.execute(sql)
            tdLog.exit(" keyword VALUES or FILE required ")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("DB error: invalid SQL:   (keyword VALUES or FILE required)") 

        tdSql.execute('''insert into db.table1 using db.stable_1 (t1 , t2) 
                        tags ("table_1" , 111) values ( now , 1) ''')
        sql = '''select *  from db.stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0,1,1)
        tdSql.checkData(0,2,'table_1')

        tdLog.info("\n\n----------step4 : create table and insert again----------\n")
        sql = '''insert into db.table2 using db.stable_1 (t1) tags ("table_2") ( values (now,  ;'''
        tdLog.info(sql)
        tdSql.error(sql)
        try:
            tdSql.execute(sql)
            tdLog.exit(" unexpected token")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("DB error: syntax error near ', ;' (unexpected token)")  

        tdSql.execute('''insert into db.table2 using db.stable_1 (t1) 
                        tags ("table_2") values ( now , 2) ''')
        sql = '''select *  from db.stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(2)
        tdSql.checkData(1,1,2)
        tdSql.checkData(1,2,'table_2')

        tdLog.info("\n\n----------step5 : create table and insert without db----------\n")
        tdSql.execute('''use db''')
        sql = '''insert into table3 using stable_1 (t1) tags ("table_3") ( values (now,  ;'''
        tdLog.info(sql)
        tdSql.error(sql)
        try:
            tdSql.execute(sql)
            tdLog.exit(" unexpected token")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("DB error: syntax error near ', ;' (unexpected token)")  

        tdSql.execute('''insert into table3 using stable_1 (t1 , t2) 
                        tags ("table_3" , 333) values ( now , 3) ''')
        sql = '''select *  from stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(3)
        tdSql.checkData(2,1,3)
        tdSql.checkData(2,2,'table_3')

        tdLog.info("\n\n----------step6 : create tables in one sql ----------\n")
        sql = '''insert into table4 using stable_1 (t1) tags ("table_4")  values (now, 4) 
                             table5 using stable_1 (t1) tags ("table_5") ( values (now,  ;'''
        tdLog.info(sql)
        tdSql.error(sql)
        try:
            tdSql.execute(sql)
            tdLog.exit(" unexpected token")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("DB error: syntax error near ', ;' (unexpected token)")  

        tdSql.execute('''insert into table4 using stable_1 (t1) tags ("table_4")  values (now, 4) 
                                     table5 using stable_1 (t1) tags ("table_5")  values (now, 5) ''')
        sql = '''select *  from stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(5)
        tdSql.checkData(3,1,4)
        tdSql.checkData(3,2,'table_4')
        tdSql.checkData(4,1,5)
        tdSql.checkData(4,2,'table_5')


        sql = '''insert into table6 using stable_1 (t1) tags ("table_6")  ( values (now,  
                             table7 using stable_1 (t1) tags ("table_7")  values (now,  7);'''
        tdLog.info(sql)
        tdSql.error(sql)
        try:
            tdSql.execute(sql)
            tdLog.exit(" invalid SQL")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("invalid SQL")  

        tdSql.execute('''insert into table6 using stable_1 (t1 , t2) tags ("table_6" , 666)  values (now, 6) 
                                     table7 using stable_1 (t1) tags ("table_7")  values (now, 7) ''')
        sql = '''select *  from stable_1;'''
        tdSql.query(sql)
        tdSql.checkRows(7)
        tdSql.checkData(5,1,6)
        tdSql.checkData(5,2,'table_6')
        tdSql.checkData(6,1,7)
        tdSql.checkData(6,2,'table_7')
        


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())