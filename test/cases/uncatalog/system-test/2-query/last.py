import random
import string
from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
import numpy as np


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.rowNum = 10
        self.tbnum = 20
        self.ts = 1537146000000
        self.binary_str = 'taosdata'
        self.nchar_str = '涛思数据'
        self.cachemodel = None

    def generateString(self, length):
        chars = string.ascii_uppercase + string.ascii_lowercase
        v = ""
        for i in range(length):
            v += random.choice(chars)
        return v

    def set_create_normaltable_sql(self, ntbname, column_dict):
        column_sql = ''
        for k, v in column_dict.items():
            column_sql += f"{k} {v},"
        create_ntb_sql = f'create table {ntbname} (ts timestamp,{column_sql[:-1]})'
        return create_ntb_sql

    def set_create_stable_sql(self,stbname,column_dict,tag_dict):
        column_sql = ''
        tag_sql = ''
        for k,v in column_dict.items():
            column_sql += f"{k} {v},"
        for k,v in tag_dict.items():
            tag_sql += f"{k} {v},"
        create_stb_sql = f'create table {stbname} (ts timestamp,{column_sql[:-1]}) tags({tag_sql[:-1]})'
        return create_stb_sql

    def last_check_stb_tb_base(self):
        tdSql.execute(
            f'create database if not exists db cachemodel "{self.cachemodel}"')
        stbname = f'db.{tdCom.getLongName(5, "letters")}'
        column_dict = {
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
        tag_dict = {
            'loc':'nchar(20)'
        }
        tdSql.execute(self.set_create_stable_sql(stbname,column_dict,tag_dict))

        tdSql.execute(f"create table {stbname}_1 using {stbname} tags('beijing')")
        tdSql.execute(f"insert into {stbname}_1(ts) values(%d)" % (self.ts - 1))

        for i in [f'{stbname}_1']:
            tdSql.query(f"select last(*) from {i}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)
        #!bug TD-16561
        # for i in ['stb','db.stb','stb','db.stb']:
        #     tdSql.query(f"select last(*) from {i}")
        #     tdSql.checkRows(1)
        #     tdSql.checkData(0, 1, None)
        for i in column_dict.keys():
            for j in [f'{stbname}_1', f'{stbname}']:
                tdSql.query(f"select last({i}) from {j}")
                tdSql.checkRows(0)
        tdSql.query(f"select last({list(column_dict.keys())[0]}) from {stbname}_1 group by {list(column_dict.keys())[-1]}")
        tdSql.checkRows(1)
        for i in range(self.rowNum):
            tdSql.execute(f"insert into {stbname}_1 values(%d, %d, %d, %d, %d, %d, %d, %d, %d, %f, %f, %d, '{self.binary_str}%d', '{self.nchar_str}%d')"
                          % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1))
        for i in [f'{stbname}_1',f'{stbname}']:
            tdSql.query(f"select last(*) from {i}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 10)
        for k, v in column_dict.items():
            for j in [f'{stbname}_1', f'{stbname}']:
                tdSql.query(f"select last({k}) from {j}")
                tdSql.checkRows(1)
                # tinyint,smallint,int,bigint,tinyint unsigned,smallint unsigned,int unsigned,bigint unsigned
                if v.lower() == 'tinyint' or v.lower() == 'smallint' or v.lower() == 'int' or v.lower() == 'bigint' or v.lower() == 'tinyint unsigned' or v.lower() == 'smallint unsigned'\
                        or v.lower() == 'int unsigned' or v.lower() == 'bigint unsigned':
                    tdSql.checkData(0, 0, 10)
                # float,double
                elif v.lower() == 'float' or v.lower() == 'double':
                    tdSql.checkData(0, 0, 9.1)
                # bool
                elif v.lower() == 'bool':
                    tdSql.checkData(0, 0, True)
                # binary
                elif 'binary' in v.lower():
                    tdSql.checkData(0, 0, f'{self.binary_str}{self.rowNum}')
                # nchar
                elif 'nchar' in v.lower():
                    tdSql.checkData(0, 0, f'{self.nchar_str}{self.rowNum}')
        for i in [f'{stbname}_1', f'{stbname}']:
            tdSql.query(f"select last({list(column_dict.keys())[0]},{list(column_dict.keys())[1]},{list(column_dict.keys())[2]}) from {stbname}_1")
            tdSql.checkData(0, 2, 10)

        tdSql.error(f"select {list(column_dict.keys())[0]} from {stbname} where last({list(column_dict.keys())[12]})='涛思数据10'")
        tdSql.error(f"select {list(column_dict.keys())[0]} from {stbname}_1 where last({list(column_dict.keys())[12]})='涛思数据10'")
        tdSql.execute('drop database db')

    def last_check_ntb_base(self):
        tdSql.execute(
            f'create database if not exists db cachemodel "{self.cachemodel}"')
        ntbname = f'db.{tdCom.getLongName(5, "letters")}'
        column_dict = {
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
        create_ntb_sql = self.set_create_normaltable_sql(ntbname, column_dict)
        tdSql.execute(create_ntb_sql)
        tdSql.execute(f"insert into {ntbname}(ts) values(%d)" % (self.ts - 1))
        tdSql.query(f"select last(*) from {ntbname}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, None)
        for i in column_dict.keys():
            for j in [f'{ntbname}']:
                tdSql.query(f"select last({i}) from {j}")
                tdSql.checkRows(0)
        for i in range(self.rowNum):
            tdSql.execute(f"insert into {ntbname} values(%d, %d, %d, %d, %d, %d, %d, %d, %d, %f, %f, %d, '{self.binary_str}%d', '{self.nchar_str}%d')"
                          % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1))
        tdSql.query(f"select last(*) from {ntbname}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 10)
        for k, v in column_dict.items():
            for j in [f'{ntbname}']:
                tdSql.query(f"select last({k}) from {j}")
                tdSql.checkRows(1)
                # tinyint,smallint,int,bigint,tinyint unsigned,smallint unsigned,int unsigned,bigint unsigned
                if v.lower() == 'tinyint' or v.lower() == 'smallint' or v.lower() == 'int' or v.lower() == 'bigint' or v.lower() == 'tinyint unsigned' or v.lower() == 'smallint unsigned'\
                        or v.lower() == 'int unsigned' or v.lower() == 'bigint unsigned':
                    tdSql.checkData(0, 0, 10)
                # float,double
                elif v.lower() == 'float' or v.lower() == 'double':
                    tdSql.checkData(0, 0, 9.1)
                # bool
                elif v.lower() == 'bool':
                    tdSql.checkData(0, 0, True)
                # binary
                elif 'binary' in v.lower():
                    tdSql.checkData(0, 0, f'{self.binary_str}{self.rowNum}')
                # nchar
                elif 'nchar' in v.lower():
                    tdSql.checkData(0, 0, f'{self.nchar_str}{self.rowNum}')
        
        

        tdSql.error(
            f"select {list(column_dict.keys())[0]} from {ntbname} where last({list(column_dict.keys())[9]})='涛思数据10'")

    def last_check_stb_distribute(self):
        # prepare data for vgroup 4
        dbname = tdCom.getLongName(10, "letters")
        stbname = f'{dbname}.{tdCom.getLongName(5, "letters")}'
        vgroup_num = 2
        column_dict = {
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

        tdSql.execute(
            f'create database if not exists {dbname} vgroups {vgroup_num}  cachemodel "{self.cachemodel}"')
        tdSql.execute(f'use {dbname}')

        # build 20 child tables,every table insert 10 rows
        tdSql.execute(f'''create table {stbname}(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 tinyint unsigned, col6 smallint unsigned,
                    col7 int unsigned, col8 bigint unsigned, col9 float, col10 double, col11 bool, col12 binary(20), col13 nchar(20)) tags(loc nchar(20))''')
        for i in range(self.tbnum):
            tdSql.execute(
                f"create table {stbname}_{i} using {stbname} tags('beijing')")
            tdSql.execute(
                f"insert into {stbname}_{i}(ts) values(%d)" % (self.ts - 1-i))
        tdSql.query(f"select * from information_schema.ins_tables where db_name = '{dbname}'")
        vgroup_list = []
        for i in range(len(tdSql.queryResult)):
            vgroup_list.append(tdSql.queryResult[i][6])
        vgroup_list_set = set(vgroup_list)
        for i in vgroup_list_set:
            vgroups_num = vgroup_list.count(i)
            if vgroups_num >= 2:
                tdLog.info(f'This scene with {vgroups_num} vgroups is ok!')
                continue

        for i in range(self.tbnum):
            for j in range(self.rowNum):
                tdSql.execute(f"insert into {stbname}_{i} values(%d, %d, %d, %d, %d, %d, %d, %d, %d, %f, %f, %d, '{self.binary_str}%d', '{self.nchar_str}%d')"
                              % (self.ts + j + i, j + 1, j + 1, j + 1, j + 1, j + 1, j + 1, j + 1, j + 1, j + 0.1, j + 0.1, j % 2, j + 1, j + 1))
        for i in [f'{stbname}']:
            tdSql.query(f"select last(*) from {i}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 10)
        for k, v in column_dict.items():
            for j in [f'{stbname}']:
                tdSql.query(f"select last({k}) from {j}")
                tdSql.checkRows(1)
                # tinyint,smallint,int,bigint,tinyint unsigned,smallint unsigned,int unsigned,bigint unsigned
                if v.lower() == 'tinyint' or v.lower() == 'smallint' or v.lower() == 'int' or v.lower() == 'bigint' or v.lower() == 'tinyint unsigned' or v.lower() == 'smallint unsigned'\
                        or v.lower() == 'int unsigned' or v.lower() == 'bigint unsigned':
                    tdSql.checkData(0, 0, 10)
                # float,double
                elif v.lower() == 'float' or v.lower() == 'double':
                    tdSql.checkData(0, 0, 9.1)
                # bool
                elif v.lower() == 'bool':
                    tdSql.checkData(0, 0, True)
                # binary
                elif 'binary' in v.lower():
                    tdSql.checkData(0, 0, f'{self.binary_str}{self.rowNum}')
                # nchar
                elif 'nchar' in v.lower():
                    tdSql.checkData(0, 0, f'{self.nchar_str}{self.rowNum}')
        tdSql.execute(f'drop database {dbname}')

    def last_file_check(self):
        dbname = tdCom.getLongName(10, "letters")
        stbname = f'{dbname}.{tdCom.getLongName(5, "letters")}'
        vgroup_num = 10
        buffer_size = 3
        tables = 100
        rows = 50
        str = self.generateString(1024)
        column_dict = {
            'c1': 'int',
            'c2': 'binary(1024)',
            'c3': 'nchar(1024)'
        }
        tag_dict = {
            't1':'int'
        }                
        
        tdSql.execute(
            f"create database if not exists {dbname} vgroups {vgroup_num} buffer {buffer_size}")
        tdSql.execute(f'use {dbname}')

        create_ntb_sql = self.set_create_stable_sql(stbname, column_dict, tag_dict)
        tdSql.execute(create_ntb_sql)

        for i in range(tables):
            sql = f"create table {dbname}.sub_tb{i} using {stbname} tags({i})"
            tdSql.execute(sql)
            for j in range(rows):
                tdSql.execute(f"insert into {dbname}.sub_tb{i} values(%d, %d, '%s', '%s')" % (self.ts + j, i, str, str))
                
        tdSql.query(f"select * from {stbname}")
        tdSql.checkRows(tables * rows)

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

    def last_check_scan_type(self, cacheModel):
        tdSql.execute("create database test_last_tbname cachemodel '%s';" % cacheModel)
        tdSql.execute("use test_last_tbname;")
        tdSql.execute("create stable test_last_tbname.st(ts timestamp, id int) tags(tid int);")
        tdSql.execute("create table test_last_tbname.test_t1 using test_last_tbname.st tags(1);")

        maxRange = 100
        # 2023-11-13 00:00:00.000
        startTs = 1699804800000
        for i in range(maxRange):
            insertSqlString = "insert into test_last_tbname.test_t1 values(%d, %d);" % (startTs + i, i)
            tdSql.execute(insertSqlString)
        
        last_ts = startTs + maxRange
        tdSql.execute("insert into test_last_tbname.test_t1 (ts) values(%d)" % (last_ts))
        sql = f'select tbname, last(ts)  from test_last_tbname.test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "test_t1")     
        tdSql.checkData(0, 1, last_ts)

        explain_res = self.explain_sql(sql)
        if cacheModel == "both" or cacheModel == "last_value":
            self.check_explain_res_has_row("Last Row Scan", explain_res, sql)
        else:
            self.check_explain_res_has_row("Table Scan", explain_res, sql)
        
        
        sql = f'select last(ts), tbname from test_last_tbname.test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, "test_t1")  

        explain_res = self.explain_sql(sql)
        if cacheModel == "both" or cacheModel == "last_value":
            self.check_explain_res_has_row("Last Row Scan", explain_res, sql)
        else:
            self.check_explain_res_has_row("Table Scan", explain_res, sql)

        sql = f'select tbname, last(ts), tbname from test_last_tbname.test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "test_t1")     
        tdSql.checkData(0, 1, last_ts)  
        tdSql.checkData(0, 2, "test_t1") 

        explain_res = self.explain_sql(sql)
        if cacheModel == "both" or cacheModel == "last_value":
            self.check_explain_res_has_row("Last Row Scan", explain_res, sql)
        else:
            self.check_explain_res_has_row("Table Scan", explain_res, sql)

        tdSql.execute("drop table if exists test_last_tbname.test_t1 ;")
        tdSql.execute("drop stable if exists test_last_tbname.st;")
        tdSql.execute("drop database if exists test_last_tbname;")

    def run(self):
        self.last_check_stb_tb_base()
        self.last_check_ntb_base()
        self.last_check_stb_distribute()
        self.last_file_check()

        self.last_check_scan_type("none")
        self.last_check_scan_type("last_row")
        self.last_check_scan_type("last_value")
        self.last_check_scan_type("both")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
