import os
import sys
import random
import time
import datetime


# Add the path to test_nestedQuery module
nested_query_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../09-DataQuerying/08-SubQuery')
sys.path.insert(0, os.path.abspath(nested_query_path))

from test_nestedQuery import TestNestedquery as NestedQueryHelper
from faker import Faker
from new_test_framework.utils import tdLog, tdSql
from math import inf

class TestLastModel:
    #
    # ------------------- 1 ----------------
    #     
    def check_explain_res_has_row(self, plan_str_expect: str, rows, sql):
        plan_found = False
        for row in rows:
            if str(row).find(plan_str_expect) >= 0:
                tdLog.debug("plan: [%s] found in: [%s]" % (plan_str_expect, str(row)))
                plan_found = True
                break
        if not plan_found:
            tdLog.exit("plan: %s not found in res: [%s] in sql: %s" % (plan_str_expect, str(rows), sql))

    def check_explain_res_no_row(self, plan_str_not_expect: str, res, sql):
        for row in res:
            if str(row).find(plan_str_not_expect) >= 0:
                tdLog.exit('plan: [%s] found in: [%s] for sql: %s' % (plan_str_not_expect, str(row), sql))

    def explain_sql(self, sql: str):
        sql = "explain " + sql
        tdSql.query(sql, queryTimes=1)
        return tdSql.queryResult

    def explain_and_check_res(self, sqls, hasLastRowScanRes):
        for sql, has_last in zip(sqls, hasLastRowScanRes):
            res = self.explain_sql(sql)
            if has_last == 1:
                self.check_explain_res_has_row("Last Row Scan", res, sql)
            else:
                self.check_explain_res_no_row("Last Row Scan", res, sql)

    def none_model_test(self):
        tdSql.execute("drop database if exists last_test_none_model ;")
        tdSql.execute("create database last_test_none_model cachemodel 'none';")
        tdSql.execute("use last_test_none_model;")
        tdSql.execute("create stable last_test_none_model.st(ts timestamp, id int) tags(tid int);")
        tdSql.execute("create table last_test_none_model.test_t1 using last_test_none_model.st tags(1);")
        tdSql.execute("create table last_test_none_model.test_t2 using last_test_none_model.st tags(2);")
        tdSql.execute("create table last_test_none_model.test_t3 using last_test_none_model.st tags(3);")
        tdSql.execute("create table last_test_none_model.test_t4 using last_test_none_model.st tags(4);")
        
        maxRange = 100
        # 2023-11-13 00:00:00.000
        startTs = 1699804800000
        for i in range(maxRange):
            insertSqlString = "insert into last_test_none_model.test_t1 values(%d, %d);" % (startTs + i, i)
            tdSql.execute(insertSqlString)
        
        last_ts = startTs + maxRange
        tdSql.execute("insert into last_test_none_model.test_t1 (ts) values(%d)" % (last_ts))
        sql = f'select last_row(ts), last(*)  from last_test_none_model.test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, last_ts)     
        tdSql.checkData(0, 2, maxRange - 1)  

        explain_res = self.explain_sql(sql)
        self.check_explain_res_no_row("Last Row Scan", explain_res, sql)
        
        sql = f'select last_row(ts), last(ts), last_row(id), last(id) from last_test_none_model.test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, last_ts)  
        tdSql.checkData(0, 2, None)    
        tdSql.checkData(0, 3, maxRange - 1)  

        explain_res = self.explain_sql(sql)
        self.check_explain_res_no_row("Last Row Scan", explain_res, sql)

        sql = f'select last(*), last_row(ts), count(*) from last_test_none_model.test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, maxRange - 1)  
        tdSql.checkData(0, 2, last_ts)    
        tdSql.checkData(0, 3, maxRange + 1)

        explain_res = self.explain_sql(sql)
        self.check_explain_res_no_row("Last Row Scan", explain_res, sql)

        tdSql.error(f'select last(*), last_row(ts), ts from last_test_none_model.test_t1;')

        sql = f'select last_row(ts), last(ts), count(*) , last_row(id), last(id), last(*) from last_test_none_model.test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, last_ts)  
        tdSql.checkData(0, 2, maxRange + 1) 
        tdSql.checkData(0, 3, None)    
        tdSql.checkData(0, 4, maxRange - 1)
        tdSql.checkData(0, 5, last_ts)  
        tdSql.checkData(0, 6, maxRange - 1)  

       
        startTs2 = startTs + 86400000
        for i in range(maxRange):
            i = i + 2 * maxRange
            insertSqlString = "insert into last_test_none_model.test_t2 values(%d, %d);" % (startTs2 + i, i)
            tdSql.execute(insertSqlString)
        last_ts2 = startTs2 + maxRange

        startTs3 = startTs + 2 * 86400000
        for i in range(maxRange):
            i = i + 3 * maxRange
            insertSqlString = "insert into last_test_none_model.test_t3 values(%d, %d);" % (startTs3 + i, i)
            tdSql.execute(insertSqlString)
        last_ts3 = startTs3 + 4 * maxRange - 1

        startTs4 = startTs + 3 * 86400000
        for i in range(maxRange):
            i = i + 4 * maxRange
            insertSqlString = "insert into last_test_none_model.test_t4 values(%d, %d);" % (startTs4 + i, i)
            tdSql.execute(insertSqlString)

        last_ts4 = startTs4 + 5 * maxRange - 1
        sql = f'select last_row(ts), last(*)  from last_test_none_model.st;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts4)     
        tdSql.checkData(0, 1, last_ts4)     
        tdSql.checkData(0, 2, 5 * maxRange - 1)  

        explain_res = self.explain_sql(sql)
        self.check_explain_res_no_row("Last Row Scan", explain_res, sql)
        
        sql = f'select last_row(ts), last(ts), last_row(id), last(id) from last_test_none_model.st;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts4)     
        tdSql.checkData(0, 1, last_ts4)  
        tdSql.checkData(0, 2, 5 * maxRange - 1)    
        tdSql.checkData(0, 3, 5 * maxRange - 1)  

        explain_res = self.explain_sql(sql)
        self.check_explain_res_no_row("Last Row Scan", explain_res, sql)

        sql = f'select last(*), last_row(ts), count(*) from last_test_none_model.st;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts4)     
        tdSql.checkData(0, 1, 5 * maxRange - 1)  
        tdSql.checkData(0, 2, last_ts4)    
        tdSql.checkData(0, 3, 4 * maxRange + 1)

        explain_res = self.explain_sql(sql)
        self.check_explain_res_no_row("Last Row Scan", explain_res, sql)

        tdSql.error(f'select last(*), last_row(ts), ts from last_test_none_model.st;')

        sql = f'select last_row(ts), last(ts), count(*) , last_row(id), last(id), last(*) from last_test_none_model.st;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts4)     
        tdSql.checkData(0, 1, last_ts4)  
        tdSql.checkData(0, 2, 4 * maxRange + 1) 
        tdSql.checkData(0, 3, 5 * maxRange - 1)    
        tdSql.checkData(0, 4, 5 * maxRange - 1)
        tdSql.checkData(0, 5, last_ts4)  
        tdSql.checkData(0, 6, 5 * maxRange - 1)  

        sql = f'select last_row(1), last(2), count(*) , last_row(id), last(id), last(*) from last_test_none_model.st;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)     
        tdSql.checkData(0, 1, 2)  
        tdSql.checkData(0, 2, 4 * maxRange + 1) 
        tdSql.checkData(0, 3, 5 * maxRange - 1)    
        tdSql.checkData(0, 4, 5 * maxRange - 1)
        tdSql.checkData(0, 5, last_ts4)  
        tdSql.checkData(0, 6, 5 * maxRange - 1) 

        tdSql.execute("drop table if exists last_test_none_model.test_t4 ;")
        tdSql.execute("drop table if exists last_test_none_model.test_t3 ;")
        tdSql.execute("drop table if exists last_test_none_model.test_t2 ;")
        tdSql.execute("drop table if exists last_test_none_model.test_t1 ;")
        tdSql.execute("drop stable if exists last_test_none_model.st;")
        tdSql.execute("drop database if exists last_test_none_model;")

    def last_value_model_test(self):
        tdSql.execute("create database last_test_last_value_model cachemodel 'last_value' ;")
        tdSql.execute("use last_test_last_value_model;")
        tdSql.execute("create stable last_test_last_value_model.st(ts timestamp, id int) tags(tid int);")
        tdSql.execute("create table last_test_last_value_model.test_t1 using last_test_last_value_model.st tags(1);")
        tdSql.execute("create table last_test_last_value_model.test_t2 using last_test_last_value_model.st tags(2);")
        tdSql.execute("create table last_test_last_value_model.test_t3 using last_test_last_value_model.st tags(3);")
        tdSql.execute("create table last_test_last_value_model.test_t4 using last_test_last_value_model.st tags(4);")

        maxRange = 100
        # 2023-11-13 00:00:00.000
        startTs = 1699804800000
        for i in range(maxRange):
            insertSqlString = "insert into last_test_last_value_model.test_t1 values(%d, %d);" % (startTs + i, i)
            tdSql.execute(insertSqlString)
        
        last_ts = startTs + maxRange
        tdSql.execute("insert into last_test_last_value_model.test_t1 (ts) values(%d)" % (last_ts))
        sql = f'select last_row(ts), last(*)  from last_test_last_value_model.test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, last_ts)     
        tdSql.checkData(0, 2, maxRange - 1)  

        explain_res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", explain_res, sql)
        self.check_explain_res_has_row("Table Scan", explain_res, sql)
        
        sql = f'select last_row(ts), last(ts), last_row(id), last(id) from last_test_last_value_model.test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, last_ts)  
        tdSql.checkData(0, 2, None)    
        tdSql.checkData(0, 3, maxRange - 1)  

        explain_res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", explain_res, sql)
        self.check_explain_res_has_row("Table Scan", explain_res, sql)

        sql = f'select last(*), last_row(ts), count(*) from last_test_last_value_model.test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, maxRange - 1)  
        tdSql.checkData(0, 2, last_ts)    
        tdSql.checkData(0, 3, maxRange + 1)

        explain_res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", explain_res, sql)
        self.check_explain_res_has_row("Table Scan", explain_res, sql)

        sql = f'select last_row(ts), last(ts), count(*) , last_row(id), last(id), last(*) from last_test_last_value_model.test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, last_ts)  
        tdSql.checkData(0, 2, maxRange + 1) 
        tdSql.checkData(0, 3, None)    
        tdSql.checkData(0, 4, maxRange - 1)
        tdSql.checkData(0, 5, last_ts)  
        tdSql.checkData(0, 6, maxRange - 1)  

        startTs2 = startTs + 86400000
        for i in range(maxRange):
            i = i + 2 * maxRange
            insertSqlString = "insert into last_test_last_value_model.test_t2 values(%d, %d);" % (startTs2 + i, i)
            tdSql.execute(insertSqlString)
        last_ts2 = startTs2 + maxRange

        startTs3 = startTs + 2 * 86400000
        for i in range(maxRange):
            i = i + 3 * maxRange
            insertSqlString = "insert into last_test_last_value_model.test_t3 values(%d, %d);" % (startTs3 + i, i)
            tdSql.execute(insertSqlString)
        last_ts3 = startTs3 + 4 * maxRange - 1

        startTs4 = startTs + 3 * 86400000
        for i in range(maxRange):
            i = i + 4 * maxRange
            insertSqlString = "insert into last_test_last_value_model.test_t4 values(%d, %d);" % (startTs4 + i, i)
            tdSql.execute(insertSqlString)

        last_ts4 = startTs4 + 5 * maxRange - 1
        sql = f'select last_row(ts), last(*)  from last_test_last_value_model.st;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts4)     
        tdSql.checkData(0, 1, last_ts4)     
        tdSql.checkData(0, 2, 5 * maxRange - 1)  

        explain_res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", explain_res, sql)
        self.check_explain_res_has_row("Table Scan", explain_res, sql)
        
        sql = f'select last_row(ts), last(ts), last_row(id), last(id) from last_test_last_value_model.st;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts4)     
        tdSql.checkData(0, 1, last_ts4)  
        tdSql.checkData(0, 2, 5 * maxRange - 1)    
        tdSql.checkData(0, 3, 5 * maxRange - 1)  

        explain_res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", explain_res, sql)
        self.check_explain_res_has_row("Table Scan", explain_res, sql)

        sql = f'select last(*), last_row(ts), count(*) from last_test_last_value_model.st;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts4)     
        tdSql.checkData(0, 1, 5 * maxRange - 1)  
        tdSql.checkData(0, 2, last_ts4)    
        tdSql.checkData(0, 3, 4 * maxRange + 1)

        explain_res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", explain_res, sql)
        self.check_explain_res_has_row("Table Scan", explain_res, sql)

        tdSql.error(f'select last(*), last_row(ts), ts from last_test_last_value_model.st;')

        sql = f'select last_row(ts), last(ts), count(*) , last_row(id), last(id), last(*) from last_test_last_value_model.st;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts4)     
        tdSql.checkData(0, 1, last_ts4)  
        tdSql.checkData(0, 2, 4 * maxRange + 1) 
        tdSql.checkData(0, 3, 5 * maxRange - 1)    
        tdSql.checkData(0, 4, 5 * maxRange - 1)
        tdSql.checkData(0, 5, last_ts4)  
        tdSql.checkData(0, 6, 5 * maxRange - 1) 

        sql = f'select last_row(1), last(2), count(*) , last_row(id), last(id), last(*) from last_test_last_value_model.st;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)     
        tdSql.checkData(0, 1, 2)  
        tdSql.checkData(0, 2, 4 * maxRange + 1) 
        tdSql.checkData(0, 3, 5 * maxRange - 1)    
        tdSql.checkData(0, 4, 5 * maxRange - 1)
        tdSql.checkData(0, 5, last_ts4)  
        tdSql.checkData(0, 6, 5 * maxRange - 1) 

        tdSql.execute("drop table if exists last_test_last_value_model.test_t4 ;")
        tdSql.execute("drop table if exists last_test_last_value_model.test_t3 ;")
        tdSql.execute("drop table if exists last_test_last_value_model.test_t2 ;")
        tdSql.execute("drop table if exists last_test_last_value_model.test_t1 ;")
        tdSql.execute("drop stable if exists last_test_last_value_model.st;")
        tdSql.execute("drop database if exists last_test_last_value_model;")
    
    def last_row_model_test(self):
        tdSql.execute("create database last_test_last_row_model cachemodel 'last_row';")
        tdSql.execute("use last_test_last_row_model;")
        tdSql.execute("create stable last_test_last_row_model.st(ts timestamp, id int) tags(tid int);")
        tdSql.execute("create table last_test_last_row_model.test_t1 using last_test_last_row_model.st tags(1);")
        tdSql.execute("create table last_test_last_row_model.test_t2 using last_test_last_row_model.st tags(2);")
        tdSql.execute("create table last_test_last_row_model.test_t3 using last_test_last_row_model.st tags(3);")
        tdSql.execute("create table last_test_last_row_model.test_t4 using last_test_last_row_model.st tags(4);")

        maxRange = 100
        # 2023-11-13 00:00:00.000
        startTs = 1699804800000
        for i in range(maxRange):
            insertSqlString = "insert into last_test_last_row_model.test_t1 values(%d, %d);" % (startTs + i, i)
            tdSql.execute(insertSqlString)
        
        last_ts = startTs + maxRange
        tdSql.execute("insert into last_test_last_row_model.test_t1 (ts) values(%d)" % (last_ts))
        sql = f'select last_row(ts), last(*)  from last_test_last_row_model.test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, last_ts)     
        tdSql.checkData(0, 2, maxRange - 1)  

        explain_res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", explain_res, sql)
        self.check_explain_res_has_row("Table Scan", explain_res, sql)
        
        sql = f'select last_row(ts), last(ts), last_row(id), last(id) from last_test_last_row_model.test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, last_ts)  
        tdSql.checkData(0, 2, None)    
        tdSql.checkData(0, 3, maxRange - 1)  

        explain_res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", explain_res, sql)
        self.check_explain_res_has_row("Table Scan", explain_res, sql)

        sql = f'select last(*), last_row(ts), count(*) from last_test_last_row_model.test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, maxRange - 1)  
        tdSql.checkData(0, 2, last_ts)    
        tdSql.checkData(0, 3, maxRange + 1)

        explain_res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", explain_res, sql)
        self.check_explain_res_has_row("Table Scan", explain_res, sql)

        sql = f'select last_row(ts), last(ts), count(*) , last_row(id), last(id), last(*) from last_test_last_row_model.test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, last_ts)  
        tdSql.checkData(0, 2, maxRange + 1) 
        tdSql.checkData(0, 3, None)    
        tdSql.checkData(0, 4, maxRange - 1)
        tdSql.checkData(0, 5, last_ts)  
        tdSql.checkData(0, 6, maxRange - 1)  

        startTs2 = startTs + 86400000
        for i in range(maxRange):
            i = i + 2 * maxRange
            insertSqlString = "insert into last_test_last_row_model.test_t2 values(%d, %d);" % (startTs2 + i, i)
            tdSql.execute(insertSqlString)
        last_ts2 = startTs2 + maxRange

        startTs3 = startTs + 2 * 86400000
        for i in range(maxRange):
            i = i + 3 * maxRange
            insertSqlString = "insert into last_test_last_row_model.test_t3 values(%d, %d);" % (startTs3 + i, i)
            tdSql.execute(insertSqlString)
        last_ts3 = startTs3 + 4 * maxRange - 1

        startTs4 = startTs + 3 * 86400000
        for i in range(maxRange):
            i = i + 4 * maxRange
            insertSqlString = "insert into last_test_last_row_model.test_t4 values(%d, %d);" % (startTs4 + i, i)
            tdSql.execute(insertSqlString)

        last_ts4 = startTs4 + 5 * maxRange - 1
        sql = f'select last_row(ts), last(*)  from last_test_last_row_model.st;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts4)     
        tdSql.checkData(0, 1, last_ts4)     
        tdSql.checkData(0, 2, 5 * maxRange - 1)  

        explain_res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", explain_res, sql)
        self.check_explain_res_has_row("Table Scan", explain_res, sql)
        
        sql = f'select last_row(ts), last(ts), last_row(id), last(id) from last_test_last_row_model.st;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts4)     
        tdSql.checkData(0, 1, last_ts4)  
        tdSql.checkData(0, 2, 5 * maxRange - 1)    
        tdSql.checkData(0, 3, 5 * maxRange - 1)  

        explain_res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", explain_res, sql)
        self.check_explain_res_has_row("Table Scan", explain_res, sql)

        sql = f'select last(*), last_row(ts), count(*) from last_test_last_row_model.st;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts4)     
        tdSql.checkData(0, 1, 5 * maxRange - 1)  
        tdSql.checkData(0, 2, last_ts4)    
        tdSql.checkData(0, 3, 4 * maxRange + 1)

        explain_res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", explain_res, sql)
        self.check_explain_res_has_row("Table Scan", explain_res, sql)

        tdSql.error(f'select last(*), last_row(ts), ts from last_test_last_row_model.st;')

        sql = f'select last_row(ts), last(ts), count(*) , last_row(id), last(id), last(*) from last_test_last_row_model.st;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts4)     
        tdSql.checkData(0, 1, last_ts4)  
        tdSql.checkData(0, 2, 4 * maxRange + 1) 
        tdSql.checkData(0, 3, 5 * maxRange - 1)    
        tdSql.checkData(0, 4, 5 * maxRange - 1)
        tdSql.checkData(0, 5, last_ts4)  
        tdSql.checkData(0, 6, 5 * maxRange - 1) 

        sql = f'select last_row(1), last(2), count(*) , last_row(id), last(id), last(*) from last_test_last_row_model.st;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)     
        tdSql.checkData(0, 1, 2)  
        tdSql.checkData(0, 2, 4 * maxRange + 1) 
        tdSql.checkData(0, 3, 5 * maxRange - 1)    
        tdSql.checkData(0, 4, 5 * maxRange - 1)
        tdSql.checkData(0, 5, last_ts4)  
        tdSql.checkData(0, 6, 5 * maxRange - 1) 

        tdSql.execute("drop table if exists last_test_last_row_model.test_t4 ;")
        tdSql.execute("drop table if exists last_test_last_row_model.test_t3 ;")
        tdSql.execute("drop table if exists last_test_last_row_model.test_t2 ;")
        tdSql.execute("drop table if exists last_test_last_row_model.test_t1 ;")
        tdSql.execute("drop stable if exists last_test_last_row_model.st;")
        tdSql.execute("drop database if exists last_test_last_row_model;")

    def both_model_test(self):
        tdSql.execute("create database last_test_both_model cachemodel 'both';")
        tdSql.execute("use last_test_both_model;")
        tdSql.execute("create stable last_test_both_model.st(ts timestamp, id int) tags(tid int);")
        tdSql.execute("create table last_test_both_model.test_t1 using last_test_both_model.st tags(1);")
        tdSql.execute("create table last_test_both_model.test_t2 using last_test_both_model.st tags(2);")
        tdSql.execute("create table last_test_both_model.test_t3 using last_test_both_model.st tags(3);")
        tdSql.execute("create table last_test_both_model.test_t4 using last_test_both_model.st tags(4);")

        maxRange = 100
        # 2023-11-13 00:00:00.000
        startTs = 1699804800000
        for i in range(maxRange):
            insertSqlString = "insert into last_test_both_model.test_t1 values(%d, %d);" % (startTs + i, i)
            tdSql.execute(insertSqlString)
        
        last_ts = startTs + maxRange
        tdSql.execute("insert into last_test_both_model.test_t1 (ts) values(%d)" % (last_ts))
        sql = f'select last_row(ts), last(*)  from last_test_both_model.test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, last_ts)     
        tdSql.checkData(0, 2, maxRange - 1)  

        explain_res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", explain_res, sql)
        self.check_explain_res_no_row("Table Scan", explain_res, sql)
        
        sql = f'select last_row(ts), last(ts), last_row(id), last(id) from last_test_both_model.test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, last_ts)  
        tdSql.checkData(0, 2, None)    
        tdSql.checkData(0, 3, maxRange - 1)  

        explain_res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", explain_res, sql)
        self.check_explain_res_no_row("Table Scan", explain_res, sql)

        sql = f'select last(*), last_row(ts), count(*) from last_test_both_model.test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, maxRange - 1)  
        tdSql.checkData(0, 2, last_ts)    
        tdSql.checkData(0, 3, maxRange + 1)

        explain_res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", explain_res, sql)

        sql = f'select last_row(ts), last(ts), count(*) , last_row(id), last(id), last(*) from last_test_both_model.test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, last_ts)  
        tdSql.checkData(0, 2, maxRange + 1) 
        tdSql.checkData(0, 3, None)    
        tdSql.checkData(0, 4, maxRange - 1)
        tdSql.checkData(0, 5, last_ts)  
        tdSql.checkData(0, 6, maxRange - 1)  

        tdSql.error(f'select last(*), last_row(ts), ts from last_test_both_model.test_t1;')

        startTs2 = startTs + 86400000
        for i in range(maxRange):
            i = i + 2 * maxRange
            insertSqlString = "insert into last_test_both_model.test_t2 values(%d, %d);" % (startTs2 + i, i)
            tdSql.execute(insertSqlString)
        last_ts2 = startTs2 + maxRange

        startTs3 = startTs + 2 * 86400000
        for i in range(maxRange):
            i = i + 3 * maxRange
            insertSqlString = "insert into last_test_both_model.test_t3 values(%d, %d);" % (startTs3 + i, i)
            tdSql.execute(insertSqlString)
        last_ts3 = startTs3 + 4 * maxRange - 1

        startTs4 = startTs + 3 * 86400000
        for i in range(maxRange):
            i = i + 4 * maxRange
            insertSqlString = "insert into last_test_both_model.test_t4 values(%d, %d);" % (startTs4 + i, i)
            tdSql.execute(insertSqlString)

        last_ts4 = startTs4 + 5 * maxRange - 1
        
        sql = f'select last_row(ts), last(*)  from last_test_both_model.st;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts4)     
        tdSql.checkData(0, 1, last_ts4)     
        tdSql.checkData(0, 2, 5 * maxRange - 1)  

        explain_res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", explain_res, sql)
        self.check_explain_res_no_row("Table Scan", explain_res, sql)
        
        sql = f'select last_row(ts), last(ts), last_row(id), last(id) from last_test_both_model.st;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts4)     
        tdSql.checkData(0, 1, last_ts4)  
        tdSql.checkData(0, 2, 5 * maxRange - 1)    
        tdSql.checkData(0, 3, 5 * maxRange - 1)  

        explain_res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", explain_res, sql)
        self.check_explain_res_no_row("Table Scan", explain_res, sql)

        sql = f'select last(*), last_row(ts), count(*) from last_test_both_model.st;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts4)     
        tdSql.checkData(0, 1, 5 * maxRange - 1)  
        #tdSql.checkData(0, 2, last_ts4)   
        tdSql.checkData(0, 3, 4 * maxRange + 1)

        explain_res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", explain_res, sql)

        tdSql.error(f'select last(*), last_row(ts), ts from last_test_both_model.st;')

        sql = f'select last_row(ts), last(ts), count(*) , last_row(id), last(id), last(*) from last_test_both_model.st;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts4)     
        tdSql.checkData(0, 1, last_ts4)  
        tdSql.checkData(0, 2, 4 * maxRange + 1) 
        tdSql.checkData(0, 3, 5 * maxRange - 1)    
        tdSql.checkData(0, 4, 5 * maxRange - 1)
        tdSql.checkData(0, 5, last_ts4)  
        tdSql.checkData(0, 6, 5 * maxRange - 1) 

        sql = f'select last_row(1), last(2), count(*) , last_row(id), last(id), last(*) from last_test_both_model.st;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)     
        tdSql.checkData(0, 1, 2)  
        tdSql.checkData(0, 2, 4 * maxRange + 1) 
        tdSql.checkData(0, 3, 5 * maxRange - 1)    
        tdSql.checkData(0, 4, 5 * maxRange - 1)
        tdSql.checkData(0, 5, last_ts4)  
        tdSql.checkData(0, 6, 5 * maxRange - 1) 

        tdSql.execute("drop table if exists last_test_both_model.test_t4 ;")
        tdSql.execute("drop table if exists last_test_both_model.test_t3 ;")
        tdSql.execute("drop table if exists last_test_both_model.test_t2 ;")
        tdSql.execute("drop table if exists last_test_both_model.test_t1 ;")
        tdSql.execute("drop stable if exists last_test_both_model.st;")
        tdSql.execute("drop database if exists last_test_both_model;")

    def do_last_and_last_row(self):
        self.none_model_test()

        self.last_value_model_test()

        self.last_row_model_test()

        self.both_model_test()

        #tdSql.close()


        print("do last/last_row ...................... [passed]")

    #
    # ------------------- 2 ----------------
    # 
    def explain_scan_value(self,sql,cachemodel):    
        tdLog.info(cachemodel)  
        if cachemodel=='none':
            tdSql.query(sql)
            self.check_sql_result_include(sql,'Table Scan')
            self.check_sql_result_not_include(sql,'Last Row Scan')
            
        elif cachemodel=='last_row':
            tdSql.query(sql)
            self.check_sql_result_include(sql,'Last Row Scan')
            self.check_sql_result_not_include(sql,'Table Scan')
        elif cachemodel=='last_row_1':
            tdSql.query("reset query cache;")  
            tdSql.query(sql)
            self.check_sql_result_include(sql,'Last Row')
            self.check_sql_result_include(sql,'Table')
            
        elif cachemodel=='last_value':
            tdSql.query(sql)
            self.check_sql_result_not_include(sql,'Last Row Scan')
            self.check_sql_result_include(sql,'Table Scan')
        elif cachemodel=='last_value_1':
            tdSql.query(sql)
            self.check_sql_result_include(sql,'Last Row Scan')
            self.check_sql_result_include(sql,'Table Scan')
            
        elif cachemodel=='both':
            tdSql.query(sql)
            self.check_sql_result_include(sql,'Last Row Scan')
            self.check_sql_result_not_include(sql,'Table Scan')
        
        else:
            tdSql.query(sql)
            tdLog.info(sql)  
            tdLog.exit(f"explain_scan_value : checkEqual error")   

    def check_sql_result_include(self, sql,include_result):
        result = os.popen(f"taos -s \"reset query cache; {sql}\"" )
        res = result.read()
        if  res is None or res == '':
            tdLog.info(sql)  
            tdLog.exit(f"check_sql_result_include :  taos -s return null")
        else:
            if (include_result in res):
                tdLog.info(f"check_sql_result_include : checkEqual success")
            else :
                tdLog.info(res)
                tdLog.info(sql)  
                tdLog.exit(f"check_sql_result_include : checkEqual error")            
            
    def check_sql_result_not_include(self, sql,not_include_result):  
        result = os.popen(f"taos -s \"reset query cache; {sql}\"" )
        res = result.read()
        #tdLog.info(res)
        if  res is None or res == '':
            tdLog.info(sql)  
            tdLog.exit(f"check_sql_result_not_include :  taos -s return null")
        else:
            if (not_include_result in res):
                tdLog.info(res)
                tdLog.info(sql)  
                tdLog.exit(f"check_sql_result_not_include : checkEqual error") 
            else :
                tdLog.info(f"check_sql_result_not_include : checkEqual success") 
            
    def cachemodel_none(self, dbname="nested"):

        sql = f"explain select last(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none")         
        sql = f"explain select last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none") 
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last(*),last(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none")              
        
        sql = f"explain select last(*) from {dbname}.stable_1 group by tbname"
        self.explain_scan_value(sql,"none")         
        sql = f"explain select last_row(*) from {dbname}.stable_1  group by tbname"
        self.explain_scan_value(sql,"none") 
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1  group by tbname"
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last(*),last(*) from {dbname}.stable_1  group by tbname"
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1  group by tbname"
        self.explain_scan_value(sql,"none")   
         
        sql = f"explain select last(*) from {dbname}.stable_1  partition by tbname"
        self.explain_scan_value(sql,"none")         
        sql = f"explain select last_row(*) from {dbname}.stable_1 partition by tbname"
        self.explain_scan_value(sql,"none") 
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1 partition by tbname"
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last(*),last(*) from {dbname}.stable_1 partition by tbname"
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1 partition by tbname"
        self.explain_scan_value(sql,"none")    

        #  last(id+1)  
        sql = f"explain select last(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none")         
        sql = f"explain select last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none") 
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last(*),last(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none")              
        
        sql = f"explain select last(*) from {dbname}.stable_1 group by tbname"
        self.explain_scan_value(sql,"none")         
        sql = f"explain select last_row(*) from {dbname}.stable_1  group by tbname"
        self.explain_scan_value(sql,"none") 
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1  group by tbname"
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last(*),last(*) from {dbname}.stable_1  group by tbname"
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1  group by tbname"
        self.explain_scan_value(sql,"none")   
         
        sql = f"explain select last(*) from {dbname}.stable_1  partition by tbname"
        self.explain_scan_value(sql,"none")         
        sql = f"explain select last_row(*) from {dbname}.stable_1 partition by tbname"
        self.explain_scan_value(sql,"none") 
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1 partition by tbname"
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last(*),last(*) from {dbname}.stable_1 partition by tbname"
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1 partition by tbname"
        self.explain_scan_value(sql,"none")    

        #last(id)+1   
        sql = f"explain select last(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none")         
        sql = f"explain select last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none") 
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last(*),last(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"none")              
        
        sql = f"explain select last(*) from {dbname}.stable_1 group by tbname"
        self.explain_scan_value(sql,"none")         
        sql = f"explain select last_row(*) from {dbname}.stable_1  group by tbname"
        self.explain_scan_value(sql,"none") 
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1  group by tbname"
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last(*),last(*) from {dbname}.stable_1  group by tbname"
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1  group by tbname"
        self.explain_scan_value(sql,"none")   
         
        sql = f"explain select last(*) from {dbname}.stable_1  partition by tbname"
        self.explain_scan_value(sql,"none")         
        sql = f"explain select last_row(*) from {dbname}.stable_1 partition by tbname"
        self.explain_scan_value(sql,"none") 
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1 partition by tbname"
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last(*),last(*) from {dbname}.stable_1 partition by tbname"
        self.explain_scan_value(sql,"none")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1 partition by tbname"
        self.explain_scan_value(sql,"none")    

    def cachemodel_last_row(self, dbname="nested"):
    
        sql = f"explain select last(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_value")         
        sql = f"explain select last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_row") 
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_row_1") 
        sql = f"explain select last(*),last(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_value")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_row")  

    def cachemodel_last_value(self, dbname="nested"):
    
        sql = f"explain select last(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_row")         
        sql = f"explain select last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_value")
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_row_1") 
        sql = f"explain select last(*),last(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_row")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_value")    

        sql = f"explain select last(q_int)+1 from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_row")         
        sql = f"explain select last_row(q_int)+1 from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_value")
        sql = f"explain select last(q_int)+1,last_row(q_int)+1 from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_row_1") 
        sql = f"explain select last(q_int)+1,last(q_int)+1 from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_row")  
        sql = f"explain select last_row(q_int)+1,last_row(q_int)+1 from {dbname}.stable_1 "
        self.explain_scan_value(sql,"last_value")  

    def cachemodel_both(self, dbname="nested"):
   
        sql = f"explain select last(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"both")         
        sql = f"explain select last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"both")
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"both") 
        sql = f"explain select last(*),last(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"both")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1 "
        self.explain_scan_value(sql,"both")     
        
        sql = f"explain select last(*) from {dbname}.stable_1 group by tbname"
        self.explain_scan_value(sql,"both")         
        sql = f"explain select last_row(*) from {dbname}.stable_1 group by tbname"
        self.explain_scan_value(sql,"both")
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1 group by tbname"
        self.explain_scan_value(sql,"both") 
        sql = f"explain select last(*),last(*) from {dbname}.stable_1 group by tbname"
        self.explain_scan_value(sql,"both")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1 group by tbname"
        self.explain_scan_value(sql,"both") 
        
        sql = f"explain select last(*) from {dbname}.stable_1  partition by tbname"
        self.explain_scan_value(sql,"both")         
        sql = f"explain select last_row(*) from {dbname}.stable_1  partition by tbname"
        self.explain_scan_value(sql,"both")
        sql = f"explain select last(*),last_row(*) from {dbname}.stable_1  partition by tbname"
        self.explain_scan_value(sql,"both") 
        sql = f"explain select last(*),last(*) from {dbname}.stable_1  partition by tbname"
        self.explain_scan_value(sql,"both")  
        sql = f"explain select last_row(*),last_row(*) from {dbname}.stable_1  partition by tbname"
        self.explain_scan_value(sql,"both") 
        
    def modify_tables(self):
        fake = Faker('zh_CN')
        tdSql.execute('delete from stable_1_3;')
        tdSql.execute('delete from stable_1_4;')
        tdSql.execute('''create table stable_1_5 using stable_1 tags('stable_1_5', '%d' , '%d', '%d' , '%d' , 1 , 'binary6.%s' , 'nchar6.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
        tdSql.execute('''create table stable_1_6 using stable_1 tags('stable_1_6', '%d' , '%d', '%d' , '%d' , 1 , 'binary6.%s' , 'nchar6.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
        tdSql.execute('''create table stable_1_7 using stable_1 tags('stable_1_7', '%d' , '%d', '%d' , '%d' , 1 , 'binary6.%s' , 'nchar6.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
        tdSql.execute('''create table stable_1_8 using stable_1 tags('stable_1_8', '%d' , '%d', '%d' , '%d' , 1 , 'binary6.%s' , 'nchar6.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
        tdSql.execute('''create table stable_1_9 using stable_1 tags('stable_1_9', '%d' , '%d', '%d' , '%d' , 1 , 'binary6.%s' , 'nchar6.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
        tdSql.execute('''create table stable_1_90 using stable_1 tags('stable_1_90', '%d' , '%d', '%d' , '%d' , 1 , 'binary6.%s' , 'nchar6.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
        tdSql.execute('''create table stable_1_91 using stable_1 tags('stable_1_91', '%d' , '%d', '%d' , '%d' , 1 , 'binary6.%s' , 'nchar6.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
        tdSql.execute('''create table stable_1_92 using stable_1 tags('stable_1_92', '%d' , '%d', '%d' , '%d' , 1 , 'binary6.%s' , 'nchar6.%s' , '%f', '%f' ,'%d') ;'''
                      %(fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1),
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
                
        tdSql.execute('alter stable stable_1 add tag t_int_null INT;')
        tdSql.execute('alter stable stable_1 add tag t_bigint_null BIGINT;')
        tdSql.execute('alter stable stable_1 add tag t_smallint_null SMALLINT;')
        tdSql.execute('alter stable stable_1 add tag t_tinyint_null TINYINT;')
        tdSql.execute('alter stable stable_1 add tag t_bool_null BOOL;')
        tdSql.execute('alter stable stable_1 add tag t_binary_null VARCHAR(100);')
        tdSql.execute('alter stable stable_1 add tag t_nchar_null NCHAR(100);')
        tdSql.execute('alter stable stable_1 add tag t_float_null FLOAT;')
        tdSql.execute('alter stable stable_1 add tag t_double_null DOUBLE;')
        tdSql.execute('alter stable stable_1 add tag t_ts_null TIMESTAMP;')
               
        tdSql.execute('alter stable stable_1 drop column q_nchar8;')
        tdSql.execute('alter stable stable_1 drop column q_binary8;')
        tdSql.execute('alter stable stable_1 drop column q_nchar7;')
        tdSql.execute('alter stable stable_1 drop column q_binary7;')
        tdSql.execute('alter stable stable_1 drop column q_nchar6;')
        tdSql.execute('alter stable stable_1 drop column q_binary6;')
        tdSql.execute('alter stable stable_1 drop column q_nchar5;')
        tdSql.execute('alter stable stable_1 drop column q_binary5;')
        tdSql.execute('alter stable stable_1 drop column q_nchar4;')
        tdSql.execute('alter stable stable_1 drop column q_binary4;')
                        
    def do_last_and_last_row_nest(self):
        tdSql.prepare()
        startTime = time.time() 
        nested_query_test = NestedQueryHelper()
        nested_query_test.dropandcreateDB_random("nested", 1)
        self.modify_tables()
               
        for i in range(2):
            self.cachemodel_none() 
            tdLog.info("last_row")
            tdSql.query("alter database nested cachemodel 'last_row' ")  
            tdSql.query("reset query cache;")  
            self.cachemodel_last_row() 
            tdSql.query("alter database nested cachemodel 'last_value' ") 
            tdSql.query("reset query cache;")  
            self.cachemodel_last_value()  
            tdSql.query("alter database nested cachemodel 'both' ")  
            tdSql.query("reset query cache;")  
            self.cachemodel_both() 
            tdSql.query("alter database nested cachemodel 'none' ")  
            tdSql.query("reset query cache;")  

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))
        print("do last/last_row nest ................. [passed]")

    #
    # ------------------- main ----------------
    # 
    def test_select_last_model(self):
        """Last Row/Last model

        1. Check none model
        2. Check last Value model
        3. Check Last Row model
        4. Check Both model
        5. Check explain plan
        6. Check last/last row model on nested

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-13 Alex Duan Migrated from uncatalog/system-test/2-query/test_last_and_last_row.py
            - 2025-12-13 Alex Duan Migrated from uncatalog/system-test/2-query/test_last+last_row.py
        """
        self.do_last_and_last_row()
        self.do_last_and_last_row_nest()
        