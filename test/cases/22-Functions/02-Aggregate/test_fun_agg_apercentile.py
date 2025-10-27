import numpy as np
import random

from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck
from new_test_framework.utils.sqlset import TDSetSql

class TestFunApercentile:
    updatecfgDict = {"maxTablesPerVnode":2 ,"minTablesPerVnode":2,"tableIncStepPerVnode":2 }
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
    #
    # ------------------ sim case ------------------
    #
    def do_sim_apercentile(self):
        tdSql.execute(f"drop database if exists test2;")
        tdSql.execute(f"create database test2;")
        tdSql.execute(f"use test2;")
        tdSql.execute(f"create table s(ts timestamp,v double) tags(id nchar(16));")
        tdSql.execute(f"create table t using s tags('11') ;")
        tdSql.execute(f"insert into t values(now,null);")
        tdSql.query(
            f"select APERCENTILE(v,50,'t-digest') as k from s where ts > now-1d and ts < now interval(1h);"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, None)

        tdSql.query(
            f"select APERCENTILE(v,50) as k from s where ts > now-1d and ts < now interval(1h);"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, None)

        tdSql.query(
            f"select APERCENTILE(v,50) as k from s where ts > now-1d and ts < now interval(1h);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        print("\n")
        print("do sim apercentile .................... [passed]\n")

    #
    # ------------------ test_percentile.py ------------------
    #

    def init2(self):
        self.rowNum = 10
        self.ts = 1537146000000
        self.setsql = TDSetSql()
        self.dbname = "db"
        self.ntbname = f"{self.dbname}.ntb"
        self.stbname = f'{self.dbname}.stb'
        self.binary_length = 20 
        self.nchar_length = 20  
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
            'col12': f'binary({self.binary_length})',
            'col13': f'nchar({self.nchar_length})'
        }
        self.tag_dict = {
            'ts_tag'  : 'timestamp',
            't1': 'tinyint',
            't2': 'smallint',
            't3': 'int',
            't4': 'bigint',
            't5': 'tinyint unsigned',
            't6': 'smallint unsigned',
            't7': 'int unsigned',
            't8': 'bigint unsigned',
            't9': 'float',
            't10': 'double',
            't11': 'bool',
            't12': f'binary({self.binary_length})',
            't13': f'nchar({self.nchar_length})'
        }
        self.binary_str = 'taosdata'
        self.nchar_str = '涛思数据'
        self.tbnum = 2
        self.tag_ts = self.ts
        self.tag_tinyint = 1
        self.tag_smallint = 2
        self.tag_int = 3
        self.tag_bigint = 4
        self.tag_utint = 5
        self.tag_usint = 6
        self.tag_uint = 7
        self.tag_ubint = 8
        self.tag_float = 9.1
        self.tag_double = 10.1
        self.tag_bool = True
        self.tag_values = [
            f'{self.tag_ts},{self.tag_tinyint},{self.tag_smallint},{self.tag_int},{self.tag_bigint},\
            {self.tag_utint},{self.tag_usint},{self.tag_uint},{self.tag_ubint},{self.tag_float},{self.tag_double},{self.tag_bool},"{self.binary_str}","{self.nchar_str}"'

        ]
        self.percent = [1,50,100]
        self.param_list = ['default','t-digest']

    def insert_data(self,column_dict,tbname,row_num):
        insert_sql = self.setsql.set_insertsql(column_dict,tbname,self.binary_str,self.nchar_str)
        for i in range(row_num):
            insert_list = []
            self.setsql.insert_values(column_dict,i,insert_sql,insert_list,self.ts)

    def function_check_ntb(self):
        tdSql.prepare()
        tdSql.execute(self.setsql.set_create_normaltable_sql(self.ntbname,self.column_dict))
        self.insert_data(self.column_dict,self.ntbname,self.rowNum)
        for k,v in self.column_dict.items():
            for percent in self.percent:
                for param in self.param_list:
                    if v.lower() in ['timestamp','bool'] or 'binary' in v.lower() or 'nchar' in v.lower():
                        tdSql.error(f'select apercentile({k},{percent},"{param}") from {self.ntbname}')
                    else:
                        tdSql.query(f"select apercentile({k},{percent},'{param}') from {self.ntbname}")

    def function_check_stb(self):
        tdSql.prepare()
        tdSql.execute(self.setsql.set_create_stable_sql(self.stbname,self.column_dict,self.tag_dict))
        for i in range(self.tbnum):
            tdSql.execute(f"create table {self.stbname}_{i} using {self.stbname} tags({self.tag_values[0]})")
            self.insert_data(self.column_dict,f'{self.stbname}_{i}',self.rowNum)
        for i in range(self.tbnum):
            for k,v in self.column_dict.items():
                for percent in self.percent:
                    for param in self.param_list:
                        if v.lower() in ['timestamp','bool'] or 'binary' in v.lower() or 'nchar' in v.lower():
                            tdSql.error(f'select apercentile({k},{percent},"{param}") from {self.stbname}_{i}')
                        else:
                            tdSql.query(f"select apercentile({k},{percent},'{param}') from {self.stbname}_{i}")
        for k,v in self.column_dict.items():
                for percent in self.percent:
                    for param in self.param_list:
                        if v.lower() in ['timestamp','bool'] or 'binary' in v.lower() or 'nchar' in v.lower():
                            tdSql.error(f'select apercentile({k},{percent},"{param}") from {self.stbname}')
                        else:
                            tdSql.query(f"select apercentile({k},{percent},'{param}') from {self.stbname}")

    def do_apercentile(self):
        # init
        self.init2()
        
        # do
        self.function_check_ntb()
        self.function_check_stb()
        print("do apercentile ........................ [passed]\n")

    #
    # ------------------ test_distribute_agg_apercentile.py ------------------
    #

    def prepare_datas_of_distribute(self, dbname="testdb"):

        # prepate datas for  20 tables distributed at different vgroups
        tdSql.execute(f"create database if not exists {dbname} keep 3650 duration 100 ss_keeplocal 3000 vgroups 5")
        tdSql.execute(f" use {dbname} ")
        tdSql.execute(
            f'''create table {dbname}.stb1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            tags (t0 timestamp, t1 int, t2 bigint, t3 smallint, t4 tinyint, t5 float, t6 double, t7 bool, t8 binary(16),t9 nchar(32))
            '''
        )

        for i in range(20):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( now(), {1*i}, {11111*i}, {111*i}, {1*i}, {1.11*i}, {11.11*i}, {i%2}, "binary{i}", "nchar{i}" )')

        for i in range(9):
            tdSql.execute(
                f"insert into {dbname}.ct1 values ( now()-{i*10}s, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
            tdSql.execute(
                f"insert into {dbname}.ct4 values ( now()-{i*90}d, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )

        for i in range(1,21):
            if i ==1 or i == 4:
                continue
            else:
                tbname = f"{dbname}.ct{i}"
                for j in range(9):
                    tdSql.execute(
                f"insert into {tbname} values ( now()-{(i+j)*10}s, {1*(j+i)}, {11111*(j+i)}, {111*(j+i)}, {11*(j)}, {1.11*(j+i)}, {11.11*(j+i)}, {(j+i)%2}, 'binary{j}', 'nchar{j}', now()+{1*j}a )"
            )
        tdSql.execute(f"insert into {dbname}.ct1 values (now()-45s, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar0', now()+8a )")
        tdSql.execute(f"insert into {dbname}.ct1 values (now()+10s, 9, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute(f"insert into {dbname}.ct1 values (now()+15s, 9, -99999, -999, -99, -9.99, NULL, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute(f"insert into {dbname}.ct1 values (now()+20s, 9, -99999, -999, NULL, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")

        tdSql.execute(f"insert into {dbname}.ct4 values (now()-810d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(f"insert into {dbname}.ct4 values (now()-400d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(f"insert into {dbname}.ct4 values (now()+90d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL  ) ")

        tdLog.info(" prepare data for distributed_aggregate done! ")

    def check_distribute_datas(self, dbname="testdb"):
        # get vgroup_ids of all
        tdSql.query(f"show {dbname}.vgroups ")
        vgroups = tdSql.queryResult

        vnode_tables={}

        for vgroup_id in vgroups:
            vnode_tables[vgroup_id[0]]=[]


        # check sub_table of per vnode ,make sure sub_table has been distributed
        tdSql.query(f"select * from information_schema.ins_tables where db_name = '{dbname}' and table_name like 'ct%'")
        table_names = tdSql.queryResult
        tablenames = []
        for table_name in table_names:
            vnode_tables[table_name[6]].append(table_name[0])
        self.vnode_disbutes = vnode_tables

        count = 0
        for k ,v in vnode_tables.items():
            if len(v)>=2:
                count+=1
        if count < 2:
            tdLog.exit(" the datas of all not satisfy sub_table has been distributed ")

    def distribute_agg_query(self, dbname="testdb"):
        # basic filter
        tdSql.query(f"select apercentile(c1 , 20) from {dbname}.stb1 where c1 is null")
        tdSql.checkRows(1)

        tdSql.query(f"select apercentile(c1 , 20) from {dbname}.stb1 where t1=1")
        tdSql.checkData(0,0,2.800000000)

        tdSql.query(f"select apercentile(c1+c2 ,100) from {dbname}.stb1 where c1 =1 ")
        tdSql.checkData(0,0,11112.000000000)

        tdSql.query(f"select apercentile(c1 ,10 ) from {dbname}.stb1 where tbname=\"ct2\"")
        tdSql.checkData(0,0,2.000000000)

        tdSql.query(f"select apercentile(c1,20) from {dbname}.stb1 partition by tbname")
        tdSql.checkRows(20)

        tdSql.query(f"select apercentile(c1,20) from {dbname}.stb1 where t1> 4  partition by tbname")
        tdSql.checkRows(15)

        # union all
        tdSql.query(f"select apercentile(c1,20) from {dbname}.stb1 union all select apercentile(c1,20) from {dbname}.stb1 ")
        tdSql.checkRows(2)
        tdSql.checkData(0,0,7.389181281)

        # join

        tdSql.execute(" create database if not exists db ")
        tdSql.execute(" use db ")
        tdSql.execute(" create stable db.st (ts timestamp , c1 int ,c2 float) tags(t1 int) ")
        tdSql.execute(" create table db.tb1 using db.st tags(1) ")
        tdSql.execute(" create table db.tb2 using db.st tags(2) ")


        for i in range(10):
            ts = i*10 + self.ts
            tdSql.execute(f" insert into db.tb1 values({ts},{i},{i}.0)")
            tdSql.execute(f" insert into db.tb2 values({ts},{i},{i}.0)")

        tdSql.query(f"select apercentile(tb1.c1,100), apercentile(tb2.c2,100) from db.tb1 tb1, db.tb2 tb2 where tb1.ts=tb2.ts")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,9.000000000)
        tdSql.checkData(0,0,9.000000000)

        # group by
        tdSql.execute(f"use {dbname} ")
        tdSql.query(f" select max(c1),c1  from {dbname}.stb1 group by t1 ")
        tdSql.checkRows(20)
        tdSql.query(f" select max(c1),c1  from {dbname}.stb1 group by c1 ")
        tdSql.checkRows(30)
        tdSql.query(f" select max(c1),c2  from {dbname}.stb1 group by c2 ")
        tdSql.checkRows(31)

        # partition by tbname or partition by tag
        tdSql.query(f"select apercentile(c1 ,10)from {dbname}.stb1 partition by tbname")
        query_data = tdSql.queryResult

        # nest query for support max
        #tdSql.query(f"select apercentile(c2+2,10)+1 from (select max(c1) c2  from {dbname}.stb1)")
        #tdSql.checkData(0,0,31.000000000)
        tdSql.query(f"select apercentile(c1+2,10)+1  as c2 from (select ts ,c1 ,c2  from {dbname}.stb1)")
        tdSql.checkData(0,0,7.560701700)
        tdSql.query(f"select apercentile(a+2,10)+1  as c2 from (select ts ,abs(c1) a ,c2  from {dbname}.stb1)")
        tdSql.checkData(0,0,7.560701700)

        # mixup with other functions
        tdSql.query(f"select max(c1),count(c1),last(c2,c3),spread(c1), apercentile(c1,10) from {dbname}.stb1")
        tdSql.checkData(0,0,28)
        tdSql.checkData(0,1,184)
        tdSql.checkData(0,2,-99999)
        tdSql.checkData(0,3,-999)
        tdSql.checkData(0,4,28.000000000)
        tdSql.checkData(0,5,4.560701700)

    def do_distribute_apercentile(self):
        # init
        self.vnode_disbutes = None
        self.ts = 1537146000000
        # do 
        self.prepare_datas_of_distribute()
        self.check_distribute_datas()
        self.distribute_agg_query()
        print("do apercentile distribute ............. [passed]\n")

    #
    # ------------------ main ------------------
    #
    def test_func_agg_apercentile(self):
        """ Fun: apercentile()

        1. Sim case including time windows, t-digest input, null value
        2. Query on super/child/normal table
        3. Support types
        4. Error cases
        5. Query on distribute

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/query/apercentile.sim
            - 2025-9-24 Alex  Duan Migrated from uncatalog/system-test/2-query/test_apercentile.py
            - 2025-9-24 Alex  Duan Migrated from uncatalog/system-test/2-query/test_distribute_agg_apercentile

        """

        self.do_sim_apercentile()
        self.do_apercentile()
        self.do_distribute_apercentile()