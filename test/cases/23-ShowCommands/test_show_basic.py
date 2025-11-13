import pytest
import sys
import time
import random
import re
import os
import taos
from new_test_framework.utils import tdLog, tdSql, cluster, sc, clusterComCheck, etool, tdCom, AutoGen, TDSetSql


class TestShowBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
    
    #
    # ---------------- sim ---------------
    #
    def do_sim(self):        
        tdSql.query("select `precision`, `keep` from information_schema.ins_databases where name = database()")
        tdSql.checkRows(0)

        tdLog.info(f"=============== add dnode2 into cluster")
        clusterComCheck.checkDnodes(3)

        tdLog.info(f"=============== create database, stable, table")
        tdSql.execute(f"create database db vgroups 3")
        tdSql.execute(f"use db")
        tdSql.execute(f"create table stb (ts timestamp, c int) tags (t int)")
        tdSql.execute(f"create table t0 using stb tags (0)")
        tdSql.execute(f"create table tba (ts timestamp, c1 binary(10), c2 nchar(10));")

        tdLog.info(f"=============== run show xxxx")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(3)

        tdSql.error("show dnode 123 variables;")
        tdSql.query("show dnode 1 variables;")
        tdSql.query("show dnode 2 variables;")

        tdSql.query(f"select * from information_schema.ins_mnodes")
        tdSql.checkRows(1)

        tdSql.query("select * from information_schema.ins_qnodes")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.query("show functions")

        tdSql.execute(f"use db")
        tdSql.error("show indexes")
        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdSql.query("show streams")
        tdSql.query(f"show tables")
        tdSql.checkRows(2)

        tdSql.error("show user_table_distributed")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(1)

        tdSql.query(f"show vgroups")
        tdSql.checkRows(3)

        tdLog.info(f"=============== run select * from information_schema.xxxx")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(3)

        tdSql.query(f"select * from information_schema.ins_mnodes")
        tdSql.checkRows(1)

        tdSql.error("select * from information_schema.ins_modules")
        tdSql.query("select * from information_schema.ins_qnodes")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.query("select * from information_schema.ins_functions")
        tdSql.query("select * from information_schema.ins_indexes")
        tdSql.query(f"select * from information_schema.ins_stables")
        tdSql.checkRows(1)

        tdSql.query("select * from information_schema.ins_streams")
        tdSql.query(f"select * from information_schema.ins_tables")
        if tdSql.getRows() <= 0:
            tdLog.exit("checkAssert here")

        tdSql.error("select * from information_schema.ins_table_distributed")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(1)

        tdSql.query(f"select * from information_schema.ins_vgroups")
        tdSql.checkRows(3)

        tdLog.info(f"==== stop dnode1 and dnode2, and restart dnodes")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        sc.dnodeStop(2)
        sc.dnodeStart(2)
        clusterComCheck.checkDnodes(3)

        tdLog.info(f"==== again run show / select of above")
        tdLog.info(f"=============== run show xxxx")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(3)

        tdSql.query(f"select * from information_schema.ins_mnodes")
        tdSql.checkRows(1)

        tdSql.query("select * from information_schema.ins_qnodes")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.query("show functions")

        tdSql.error("show indexes")

        tdSql.execute(f"use db")
        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdSql.query("show streams")
        tdSql.query(f"show tables")
        tdSql.checkRows(2)

        tdSql.error("show user_table_distributed")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(1)

        tdSql.query(f"show vgroups")
        tdSql.checkRows(3)

        tdLog.info(f"=============== run select * from information_schema.xxxx")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(3)

        tdSql.query(f"select * from information_schema.ins_mnodes")
        tdSql.checkRows(1)

        tdSql.query("select * from information_schema.ins_qnodes")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.query("select * from information_schema.ins_functions")
        tdSql.query("select * from information_schema.ins_indexes")
        tdSql.query(f"select * from information_schema.ins_stables")
        tdSql.checkRows(1)

        tdSql.query("select * from information_schema.ins_streams")
        tdSql.query(f"select * from information_schema.ins_tables")
        if tdSql.getRows() <= 0:
            tdLog.exit("checkAssert here")

        tdSql.error("select * from information_schema.ins_table_distributed")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(1)

        tdSql.query(f"select * from information_schema.ins_vgroups")
        tdSql.checkRows(3)

        tdSql.error(f"select * from performance_schema.PERF_OFFSETS;")

        tdSql.query(f"show create stable stb;")
        tdSql.checkRows(1)

        tdSql.query(f"show create table t0;")
        tdSql.checkRows(1)

        tdSql.query(f"show create table tba;")
        tdSql.checkRows(1)

        tdSql.error(f"show create stable t0;")

        tdSql.query(f"show variables;")
        tdSql.checkAssert(tdSql.getRows() > 0)

        tdSql.query(f"show dnode 1 variables;")
        if tdSql.getRows() <= 0:
            tdLog.exit("checkAssert here")

        tdSql.query(f"show local variables;")
        if tdSql.getRows() <= 0:
            tdLog.exit("checkAssert here")

        tdSql.query(f"show cluster alive;")
        if tdSql.getRows() <= 0:
            tdLog.exit("checkAssert here")

        tdSql.query(f"show db.alive;")
        if tdSql.getRows() <= 0:
            tdLog.exit("checkAssert here")
        
        print("do show sim ........................... [passed]")

    #
    # ---------------- system-test ---------------
    #
    def init1(self):
        tdLog.debug("start to execute %s" % __file__)
        self.setsql = TDSetSql()
        self.ins_param_list = ['dnodes','mnodes','qnodes','cluster','functions','users','grants','topics','subscriptions','streams']
        self.perf_param = ['apps','connections','consumers','queries','transactions']
        self.perf_param_list = ['apps','connections','consumers','queries','trans']
        self.dbname = "db"
        self.vgroups = 4
        self.stbname = f'`{tdCom.getLongName(5)}`'
        self.tbname = f'`{tdCom.getLongName(3)}`'
        self.db_param = {
            "database":f"{self.dbname}",
            "buffer":100,
            "cachemodel":"'none'",
            "cachesize":1,
            "comp":2,
            "maxrows":1000,
            "minrows":200,
            "pages":512,
            "pagesize":16,
            "precision":"'ms'",
            "replica":1,
            "wal_level":1,
            "wal_fsync_period":6000,
            "vgroups":self.vgroups,
            "stt_trigger":1,
            "tsdb_pagesize":16
        }

    def ins_check(self):
        tdSql.prepare()
        for param in self.ins_param_list:
            if param.lower() == 'qnodes':
                tdSql.execute('create qnode on dnode 1')
            tdSql.query(f'show {param}')
            show_result = tdSql.queryResult
            tdSql.query(f'select * from information_schema.ins_{param}')
            select_result = tdSql.queryResult
            tdSql.checkEqual(show_result,select_result)
        tdSql.execute('drop database db')
    def perf_check(self):
        tdSql.prepare()
        for param in range(len(self.perf_param_list)):
            tdSql.query(f'show {self.perf_param[param]}')
            if len(tdSql.queryResult) != 0:
                show_result = tdSql.queryResult[0][0]
                tdSql.query(f'select * from performance_schema.perf_{self.perf_param_list[param]}')
                select_result = tdSql.queryResult[0][0]
                tdSql.checkEqual(show_result,select_result)
            else :
                continue
        tdSql.execute('drop database db')
    def set_stb_sql(self,stbname,column_dict,tag_dict):
        column_sql = ''
        tag_sql = ''
        for k,v in column_dict.items():
            column_sql += f"{k} {v}, "
        for k,v in tag_dict.items():
            tag_sql += f"{k} {v}, "
        create_stb_sql = f'create stable {stbname} ({column_sql[:-2]}) tags ({tag_sql[:-2]})'
        return create_stb_sql

    def set_create_database_sql(self,sql_dict):
        create_sql = 'create'
        for key,value in sql_dict.items():
            create_sql += f' {key} {value}'
        return create_sql

    def show_create_sysdb_sql(self):
        sysdb_list = {'information_schema', 'performance_schema'}
        for db in sysdb_list:
          tdSql.query(f'show create database {db}')
          tdSql.checkEqual(f'{db}',tdSql.queryResult[0][0])
          tdSql.checkEqual(f'CREATE DATABASE `{db}`',tdSql.queryResult[0][1])

    def show_create_systb_sql(self):
        for param in self.ins_param_list:
          tdSql.query(f'show create table information_schema.ins_{param}')
          tdSql.checkEqual(f'ins_{param}',tdSql.queryResult[0][0])

          tdSql.execute(f'use information_schema')
          tdSql.query(f'show create table ins_{param}')
          tdSql.checkEqual(f'ins_{param}',tdSql.queryResult[0][0])

        for param in self.perf_param_list:
          tdSql.query(f'show create table performance_schema.perf_{param}')
          tdSql.checkEqual(f'perf_{param}',tdSql.queryResult[0][0])

          tdSql.execute(f'use performance_schema')
          tdSql.query(f'show create table perf_{param}')
          tdSql.checkEqual(f'perf_{param}',tdSql.queryResult[0][0])

    def show_create_sql(self):
        create_db_sql = self.set_create_database_sql(self.db_param)
        print(create_db_sql)
        tdSql.execute(create_db_sql)
        tdSql.query(f'show create database {self.dbname}')
        tdSql.checkEqual(self.dbname,tdSql.queryResult[0][0])
        for key,value in self.db_param.items():
            if key == 'database':
                continue
            else:
                param = f'{key} {value}'
                if param in tdSql.queryResult[0][1].lower():
                    tdLog.info(f'show create database check success with {key} {value}')
                    continue
                else:
                    tdLog.exit(f"show create database check failed with {key} {value}")
        tdSql.query('show vnodes on dnode 1')
        tdSql.checkRows(1)
        tdSql.execute(f'use {self.dbname}')

        column_dict = {
            '`ts`': 'timestamp',
            '`col1`': 'tinyint',
            '`col2`': 'smallint',
            '`col3`': 'int',
            '`col4`': 'bigint',
            '`col5`': 'tinyint unsigned',
            '`col6`': 'smallint unsigned',
            '`col7`': 'int unsigned',
            '`col8`': 'bigint unsigned',
            '`col9`': 'float',
            '`col10`': 'double',
            '`col11`': 'bool',
            '`col12`': 'varchar(20)',
            '`col13`': 'nchar(20)'

        }
        tag_dict = {
            '`t1`': 'tinyint',
            '`t2`': 'smallint',
            '`t3`': 'int',
            '`t4`': 'bigint',
            '`t5`': 'tinyint unsigned',
            '`t6`': 'smallint unsigned',
            '`t7`': 'int unsigned',
            '`t8`': 'bigint unsigned',
            '`t9`': 'float',
            '`t10`': 'double',
            '`t11`': 'bool',
            '`t12`': 'varchar(20)',
            '`t13`': 'nchar(20)',
            '`t14`': 'timestamp'

        }
        create_table_sql = self.set_stb_sql(self.stbname,column_dict,tag_dict)
        tdSql.execute(create_table_sql)
        tdSql.query(f'show create stable {self.stbname}')
        query_result = tdSql.queryResult
        #tdSql.checkEqual(query_result[0][1].lower(),create_table_sql)
        tdSql.execute(f'create table {self.tbname} using {self.stbname} tags(1,1,1,1,1,1,1,1,1.000000e+00,1.000000e+00,true,"abc","abc123",0)')
        tag_sql = '('
        for tag_keys in tag_dict.keys():
            tag_sql += f'{tag_keys}, '
        tags = f'{tag_sql[:-2]})'
        sql = f'create table {self.tbname} using {self.stbname} {tags} tags (1, 1, 1, 1, 1, 1, 1, 1, 1.000000e+00, 1.000000e+00, true, "abc", "abc123", 0)'
        tdSql.query(f'show create table {self.tbname}')
        query_result = tdSql.queryResult
        #tdSql.checkEqual(query_result[0][1].lower(),sql)
        tdSql.execute(f'drop database {self.dbname}')

    def check_gitinfo(self):
        taosd_gitinfo_sql = ''
        tdSql.query('show dnode 1 variables')
        for i in tdSql.queryResult:
            if i[1].lower() == "gitinfo":
                taosd_gitinfo_sql = f"git: {i[2]}"
        taos_gitinfo_sql = ''
        tdSql.query('show local variables')
        for i in tdSql.queryResult:
            if i[0].lower() == "gitinfo":
                taos_gitinfo_sql = f"git: {i[1]}"
        taos_info = os.popen('taos -V').read()
        taos_gitinfo = re.findall("^git: .*",taos_info,re.M)
        tdSql.checkEqual(taos_gitinfo_sql,taos_gitinfo[0])
        taosd_info = os.popen('taosd -V').read()
        taosd_gitinfo = re.findall("^git: .*",taosd_info,re.M)
        tdSql.checkEqual(taosd_gitinfo_sql,taosd_gitinfo[0])

    def show_base(self):
        for sql in ['dnodes','mnodes','cluster']:
            tdSql.query(f'show {sql}')
            print(tdSql.queryResult)
            if sql == 'dnodes':
                tdSql.checkRows(3)
            else:
                tdSql.checkRows(1)
        tdSql.query('show grants')
        grants_info = tdSql.queryResult
        tdSql.query('show licences')
        licences_info = tdSql.queryResult
        tdSql.checkEqual(grants_info,licences_info)

    def show_column_name(self):
        tdSql.execute("create database db;")
        tdSql.execute("use db;")
        tdSql.execute("create table ta(ts timestamp, name nchar(16), age int , address int);")
        tdSql.execute("insert into ta values(now, 'jack', 19, 23);")
        
        colName1 = ["ts","name","age","address"]
        colName2 = tdSql.getColNameList("select last(*) from ta;")
        for i in range(len(colName1)):
            if colName2[i] != f"last({colName1[i]})":
                tdLog.exit(f"column name is different.  {colName2} != last({colName1[i]} ")
                return 

        # alter option        
        tdSql.execute("alter local 'keepColumnName' '1';")
        colName3 = tdSql.getColNameList("select last(*) from ta;")
        for col in colName3:
            if colName1 != colName3:
                tdLog.exit(f"column name is different. colName1= {colName1} colName2={colName3}")
                return

    def do_system_test_show(self):
        self.init1()
        # do
        self.check_gitinfo()
        self.show_base()
        self.ins_check()
        self.perf_check()
        self.show_create_sql()
        self.show_create_sysdb_sql()
        self.show_create_systb_sql()
        self.show_column_name()
        self.show_variables()
        print("do system-test show ................... [passed]")

    def get_variable(self, name: str, local: bool = True, dnode_id: int = 1):
        if local:
            sql = 'show local variables'
        else:
            sql = f'select `value` from information_schema.ins_dnode_variables where name like "{name}" and dnode_id = {dnode_id}'
        tdSql.query(sql, queryTimes=1)
        res = tdSql.queryResult
        if local:
            for row in res:
                if row[0] == name:
                    return row[1]
        else:
            if len(res) > 0:
                return res[0][0]
        raise Exception(f"variable {name} not found")

    def show_variables(self):
        epsion = 0.0000001
        var = 'minimalTmpDirGB'
        expect_val: float = 10.11
        sql = f'ALTER LOCAL "{var}" "{expect_val}"'
        tdSql.execute(sql)
        val: float = float(self.get_variable(var))
        if val != expect_val:
            tdLog.exit(f'failed to set local {var} to {expect_val} actually {val}')

        error_vals = ['a', '10a', '', '1.100r', '1.12  r']
        for error_val in error_vals:
            tdSql.error(f'ALTER LOCAL "{var}" "{error_val}"')

        var = 'supportVnodes'
        expect_val = 1240 ## 1.211111 * 1024
        sql = f'ALTER DNODE 1 "{var}" "1.211111k"'
        tdSql.execute(sql, queryTimes=1)
        val = int(self.get_variable(var, False, 1))
        if val != expect_val:
            tdLog.exit(f'failed to set dnode {var} to {expect_val} actually {val}')

        error_vals = ['a', '10a', '', '1.100r', '1.12  r', '5k']
        for error_val in error_vals:
            tdSql.error(f'ALTER DNODE 1 "{var}" "{error_val}"')

        var = 'randErrorDivisor'
        vals = ['9223372036854775807', '9223372036854775807.1', '9223372036854775806', '9223372036854775808', '9223372036854775808.1', '9223372036854775807.0', '9223372036854775806.1']
        expected_vals = ['9223372036854775807', 'err', '9223372036854775806', 'err', 'err', 'err', 'err']
        for val_str, expected_val in zip(vals, expected_vals):
            sql = f'ALTER all dnodes "{var}" "{val_str}"'
            if expected_val == 'err':
                tdSql.error(sql)
            else:
                tdSql.execute(sql, queryTimes=1)
                actual_val = self.get_variable(var, False)
                if expected_val != actual_val:
                    tdLog.exit(f"failed to set local {var} to {expected_val} actually {actual_val}")

    #
    # ---------------- army ---------------
    #
    def insertData(self):
        tdLog.info(f"create table and insert data.")
        self.stb = "stb"
        self.db = "db"
        self.childtable_count = 10
        self.insert_rows = 10000
        tdSql.execute(f"drop database if exists {self.db}")

        self.autoGen = AutoGen(startTs = 1600000000000*1000*1000, batch=500, genDataMode = "fillone")
        self.autoGen.create_db(self.db, 2, 3, "precision 'ns'")
        self.autoGen.create_stable(stbname = self.stb, tag_cnt = 5, column_cnt = 20, binary_len = 10, nchar_len = 5)
        self.autoGen.create_child(self.stb, "child", self.childtable_count)
        self.autoGen.insert_data(self.insert_rows, True)
        
        tdLog.info("create view.")
        tdSql.execute(f"use {self.db}")
        sqls = [
            "create view viewc0c1 as select c0,c1 from stb ",
            "create view viewc0c1c2 as select c0,c1,c2 from stb ",
            "create view viewc0c3 as select c0,c3 from stb where c3=1",
            "create view viewc0c4c5 as select c4,c5 from stb ",
            "create view viewc0c6 as select c0,c1,c6 from stb ",
            "create view viewc0c7 as select c0,c1 from stb ",
            "create view viewc0c7c8 as select c0,c7,c8 from stb where c8>0",
            "create view viewc0c3c1 as select c0,c3,c1 from stb ",
            "create view viewc2c4 as select c2,c4 from stb ",
            "create view viewc2c5 as select c2,c5 from stb ",
        ]
        tdSql.executes(sqls)

    def checkView(self):
        tdLog.info(f"check view like.")

        # like
        sql = f"show views like 'view%'"
        tdSql.query(sql)
        tdSql.checkRows(10)

        sql = f"show views like 'vie_c0c1c2'"
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0,0,"viewc0c1c2")

        sql = f"show views like '%c2c_'"
        tdSql.query(sql)
        tdSql.checkRows(2)
        tdSql.checkData(0,0, "viewc2c4")
        tdSql.checkData(1,0, "viewc2c5")

        sql = f"show views like '%' "
        tdSql.query(sql)
        tdSql.checkRows(10)
        
        # zero
        sql = "show views like '_' "
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = "show views like 'a%' "
        tdSql.query(sql)
        tdSql.checkRows(0)


    def doQuery(self):
        tdLog.info(f"do query.")

        # __group_key
        sql = f"select count(*) from {self.stb} "
        tdSql.query(sql)
        # column index 1 value same with 2
        allRows = self.insert_rows * self.childtable_count
        tdSql.checkFirstValue(sql, allRows)

    def checkShowTags(self):
        # verification for TD-29904
        tdSql.error("show tags from t100000", expectErrInfo='Fail to get table info, error: Table does not exist')

        sql = "show tags from child1"
        tdSql.query(sql)
        tdSql.checkRows(5)

        sql = f"show tags from child1 from {self.db}"
        tdSql.query(sql)
        tdSql.checkRows(5)

        sql = f"show tags from {self.db}.child1"
        tdSql.query(sql)
        tdSql.checkRows(5)

        # verification for TD-30030
        tdSql.execute("create table t100 (ts timestamp, pk varchar(20) primary key, c1 varchar(100)) tags (id int)")
        tdSql.execute("insert into ct1 using t100 tags(1) values('2024-05-17 14:58:52.902', 'a1', '100')")
        tdSql.execute("insert into ct1 using t100 tags(1) values('2024-05-17 14:58:52.902', 'a2', '200')")
        tdSql.execute("insert into ct1 using t100 tags(1) values('2024-05-17 14:58:52.902', 'a3', '300')")
        tdSql.execute("insert into ct2 using t100 tags(2) values('2024-05-17 14:58:52.902', 'a2', '200')")
        tdSql.execute("create view v100 as select * from t100")
        tdSql.execute("create view v200 as select * from ct1")

        tdSql.error("show tags from v100", expectErrInfo="Tags can only applied to super table and child table")
        tdSql.error("show tags from v200", expectErrInfo="Tags can only applied to super table and child table")

        tdSql.execute("create table t200 (ts timestamp, pk varchar(20) primary key, c1 varchar(100))")

        tdSql.error("show tags from t200", expectErrInfo="Tags can only applied to super table and child table")

    def checkShow(self):
        # not support
        sql = "show accounts;"
        tdSql.error(sql)

        # check result
        sql = "SHOW CLUSTER;"
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = "SHOW COMPACTS;"
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = "SHOW COMPACT 1;"
        tdSql.query(sql)
        tdSql.checkRows(0)
        sql = "SHOW CLUSTER MACHINES;"
        tdSql.query(sql)
        tdSql.checkRows(1)

        # run to check crash 
        sqls = [
            # "show scores;",
            "SHOW CLUSTER VARIABLES",
            # "SHOW BNODES;",
        ]
        tdSql.executes(sqls)

        self.checkShowTags()

    # run
    def do_army_show(self):
        # insert data
        self.insertData()
        # check view
        self.checkView()
        # do action
        self.doQuery()
        # check show
        self.checkShow()
        
        tdSql.execute(f"drop database if exists {self.db}")
        print("do army show .......................... [passed]")

    #
    # ------------------- test_show_tag_index.py ----------------
    #
    def check_tags(self):
        tdSql.checkRows(2)
        tdSql.checkCols(6)
        tdSql.checkData(0, 0, 'ctb1')
        tdSql.checkData(0, 1, 'db')
        tdSql.checkData(0, 2, 'stb')
        tdSql.checkData(0, 3, 't0')
        tdSql.checkData(0, 4, 'INT')
        tdSql.checkData(0, 5, 1)
        tdSql.checkData(1, 0, 'ctb1')
        tdSql.checkData(1, 1, 'db')
        tdSql.checkData(1, 2, 'stb')
        tdSql.checkData(1, 3, 't1')
        tdSql.checkData(1, 4, 'INT')
        tdSql.checkData(1, 5, 1)

    def check_table_tags(self, is_super_table):

        if is_super_table == False:
            tdSql.checkRows(1)
            tdSql.checkCols(3)
            tdSql.checkData(0, 0, 'ctb1')
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(0, 2, 1)
        else:
            tdSql.checkRows(2)
            tdSql.checkCols(3)
            tdSql.checkData(0, 0, 'ctb1')
            tdSql.checkData(1, 0, 'ctb2')
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(1, 1, 2)
            tdSql.checkData(0, 2, 1)
            tdSql.checkData(1, 2, 2)

    def check_indexes(self):
        tdSql.checkRows(2)
        for i in range(2):
            col_name = tdSql.getData(i, 5)
            if col_name == "t0":
                continue
            tdSql.checkCols(7)
            tdSql.checkData(i, 0, 'idx1')
            tdSql.checkData(i, 1, 'db')
            tdSql.checkData(i, 2, 'stb')
            tdSql.checkData(i, 3, None)
            tdSql.checkData(i, 5, 't1')
            tdSql.checkData(i, 6, 'tag_index')

    def do_show_tag_index(self):
        tdSql.execute(f'drop database if exists db')
        tdSql.execute(f'create database db')
        tdSql.execute(f'use db')
        tdSql.execute(f'create table stb (ts timestamp, c0 int) tags (t0 int, t1 int)')
        tdSql.execute(f'create table ctb1 using stb tags (1, 1)')
        tdSql.execute(f'create table ctb2 using stb tags (2, 2)')
        tdSql.execute(f'create table ntb (ts timestamp, c0 int)')
        tdSql.execute(f'create view vtb as select * from stb')
        tdSql.execute(f'create view vtb1 as select * from ctb1')
        tdSql.execute(f'create view vtb2 as select * from ctb2')
        tdSql.execute(f'create view vtbn as select * from ntb')
        tdSql.execute(f'insert into ctb1 values (now, 1)')
        tdSql.execute(f'insert into ctb2 values (now, 2)')

        # show tags
        tdSql.query(f'show tags from stb')
        tdSql.checkRows(0)
        tdSql.query(f'show tags from stb')
        tdSql.checkRows(0)
        tdSql.query(f'show tags from `stb`')
        tdSql.checkRows(0)
        tdSql.query(f'show tags from stb from db')
        tdSql.checkRows(0)
        tdSql.query(f'show tags from `stb` from `db`')
        tdSql.checkRows(0)
        tdSql.query(f'show tags from db.stb')
        tdSql.checkRows(0)
        tdSql.query(f'show tags from `db`.`stb`')
        tdSql.checkRows(0)
        tdSql.query(f'show tags from ctb1')
        self.check_tags()
        tdSql.query(f'show tags from `ctb1`')
        self.check_tags()
        tdSql.query(f'show tags from ctb1 from db')
        self.check_tags()
        tdSql.query(f'show tags from `ctb1` from `db`')
        self.check_tags()
        tdSql.query(f'show tags from db.ctb1')
        self.check_tags()
        tdSql.query(f'show tags from `db`.`ctb1`')
        self.check_tags()

        tdSql.error(f'show tags from db.stb from db')
        tdSql.error(f'show tags from `db`.`stb` from db')
        tdSql.error(f'show tags from db.ctb1 from db')
        tdSql.error(f'show tags from `db`.`ctb1` from db')
        tdSql.error(f'show tags from tb_undef from db', expectErrInfo='Fail to get table info, error: Table does not exist')
        tdSql.error(f'show tags from db.tb_undef', expectErrInfo='Fail to get table info, error: Table does not exist')
        tdSql.error(f'show tags from tb_undef', expectErrInfo='Fail to get table info, error: Table does not exist')
        tdSql.error(f'show tags from ntb', expectErrInfo='Tags can only applied to super table and child table')
        tdSql.error(f'show tags from vtb', expectErrInfo='Tags can only applied to super table and child table')
        tdSql.error(f'show tags from vtb1', expectErrInfo='Tags can only applied to super table and child table')
        tdSql.error(f'show tags from vtb2', expectErrInfo='Tags can only applied to super table and child table')
        tdSql.error(f'show tags from vtbn', expectErrInfo='Tags can only applied to super table and child table')

        # show table tags
        tdSql.query(f'show table tags from stb')
        self.check_table_tags(True)
        tdSql.query(f'show table tags from `stb`')
        self.check_table_tags(True)
        tdSql.query(f'show table tags from stb from db')
        self.check_table_tags(True)
        tdSql.query(f'show table tags from `stb` from `db`')
        self.check_table_tags(True)
        tdSql.query(f'show table tags from db.stb')
        self.check_table_tags(True)
        tdSql.query(f'show table tags from `db`.`stb`')
        self.check_table_tags(True)

        tdSql.query(f'show table tags from ctb1')
        self.check_table_tags(False)
        tdSql.query(f'show table tags from `ctb1`')
        self.check_table_tags(False)
        tdSql.query(f'show table tags from ctb1 from db')
        self.check_table_tags(False)
        tdSql.query(f'show table tags from `ctb1` from `db`')
        self.check_table_tags(False)
        tdSql.query(f'show table tags from db.ctb1')
        self.check_table_tags(False)
        tdSql.query(f'show table tags from `db`.`ctb1`')
        self.check_table_tags(False)

        tdSql.error(f'show table tags from db.stb from db')
        tdSql.error(f'show table tags from `db`.`stb` from db')
        tdSql.error(f'show table tags from db.ctb1 from db')
        tdSql.error(f'show table tags from `db`.`ctb1` from db')
        tdSql.error(f'show table tags from tb_undef from db', expectErrInfo='Table does not exist')
        tdSql.error(f'show table tags from db.tb_undef', expectErrInfo='Table does not exist')
        tdSql.error(f'show table tags from tb_undef', expectErrInfo='Table does not exist')
        tdSql.error(f'show table tags from ntb', expectErrInfo='Tags can only applied to super table and child table')
        tdSql.error(f'show table tags from vtb', expectErrInfo='Tags can only applied to super table and child table')
        tdSql.error(f'show table tags from vtb1', expectErrInfo='Tags can only applied to super table and child table')
        tdSql.error(f'show table tags from vtb2', expectErrInfo='Tags can only applied to super table and child table')
        tdSql.error(f'show table tags from vtbn', expectErrInfo='Tags can only applied to super table and child table')

        # show indexes
        tdSql.execute(f'create index idx1 on stb (t1)')

        tdSql.query(f'show indexes from stb')
        self.check_indexes()
        tdSql.query(f'show indexes from `stb`')
        self.check_indexes()
        tdSql.query(f'show indexes from stb from db')
        self.check_indexes()
        tdSql.query(f'show indexes from `stb` from `db`')
        self.check_indexes()
        tdSql.query(f'show indexes from db.stb')
        self.check_indexes()
        tdSql.query(f'show indexes from `db`.`stb`')
        self.check_indexes()

        tdSql.query(f'show indexes from ctb1')
        tdSql.checkRows(0)
        tdSql.query(f'show indexes from `ctb1`')
        tdSql.checkRows(0)
        tdSql.query(f'show indexes from ctb1 from db')
        tdSql.checkRows(0)
        tdSql.query(f'show indexes from `ctb1` from `db`')
        tdSql.checkRows(0)
        tdSql.query(f'show indexes from db.ctb1')
        tdSql.checkRows(0)
        tdSql.query(f'show indexes from `db`.`ctb1`')
        tdSql.checkRows(0)

        tdSql.error(f'show indexes from db.stb from db')
        tdSql.error(f'show indexes from `db`.`stb` from db')
        tdSql.error(f'show indexes from db.ctb1 from db')
        tdSql.error(f'show indexes from `db`.`ctb1` from db')

        # check error information
        tdSql.error(f'create index idx1 on db2.stb (t1);', expectErrInfo='Database not exist')
        tdSql.error(f'use db2;', expectErrInfo='Database not exist')
        tdSql.error(f' alter stable db2.stb add column c2 int;', expectErrInfo='Database not exist')

        print("do show tag index ..................... [passed]")

    #
    # ------------------- main ----------------
    #
    def test_show_basic(self):
        """Show basic

        1. Verify show commands result with information_schema database
        2. Verify show commands result after dnode restarts
        3. Checking error handling for invalid operations
        4. Check show command include:
           show dnodes/modes/qnodes/databases/functions/stables/tables/vgroups
           show apps/connections/consumers/queries/transactions/views/tags
           show variables/local variables/cluster variables/compacts/cluster
           show licences/grants/users
           show create database/stable/table
        5. Create super table/child table/view and insert data
        6. Verify show tags/table tags/indexes command
        7. Checking error handling for invalid operations
        8. Check show command include:
           show tags from super table/child table
           show table tags from super table/child table
           show indexes from super table/child table
    

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-17 Alex Duan Migrated from uncatalog/army/query/test_show.py
            - 2025-10-17 Alex Duan Migrated from uncatalog/system-test/0-others/test_show.py
            - 2025-4-28 Simon Guan Migrated from tsim/show/basic.sim
            - 2025-11-03 Alex Duan Migrated from uncatalog/system-test/0-others/test_show_tag_index.py
        
        """
        self.do_system_test_show()
        self.do_army_show()
        self.do_sim()
        self.do_show_tag_index()