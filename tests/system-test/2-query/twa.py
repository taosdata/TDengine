from util.log import *
from util.cases import *
from util.sql import *
import numpy as np
import random ,os ,sys
import platform
import math

class TDTestCase:
    updatecfgDict = {"maxTablesPerVnode":2 ,"minTablesPerVnode":2,"tableIncStepPerVnode":2 }

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.vnode_disbutes = None
        self.ts = 1537146000000
        self.tb_nums = 20
        self.row_nums = 100
        self.time_step = 1000

    def prepare_datas_of_distribute(self, dbname="testdb"):

        # prepate datas for  20 tables distributed at different vgroups
        tdSql.execute(f"create database if not exists {dbname} keep 3650 duration 1000 vgroups 5")
        tdSql.execute(
            f'''create table {dbname}.stb1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp,c11 int UNSIGNED, c12 bigint UNSIGNED,  c13 smallint UNSIGNED, c14 tinyint UNSIGNED)
            tags (t0 timestamp, t1 int, t2 bigint, t3 smallint, t4 tinyint, t5 float, t6 double, t7 bool, t8 binary(16),t9 nchar(32))
            '''
        )

        for i in range(self.tb_nums):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( now(), {1*i}, {11111*i}, {111*i}, {1*i}, {1.11*i}, {11.11*i}, {i%2}, "binary{i}", "nchar{i}" )')
            ts = self.ts
            for j in range(self.row_nums):
                ts+=j*self.time_step
                tdSql.execute(
                    f"insert into {dbname}.ct{i+1} values({ts}, 1, 11111, 111, 1, 1.11, 11.11, 2, 'binary{j}', 'nchar{j}', now()+{1*j}a, 1, 11111, 111, 1 )"
                )

        tdSql.execute(f"insert into {dbname}.ct1 values (now()-810d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(f"insert into {dbname}.ct1 values (now()-400d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(f"insert into {dbname}.ct1 values (now()+90d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL , NULL, NULL, NULL, NULL ) ")

        tdLog.info(" prepare data for distributed_aggregate done! ")

    def twa_support_types(self, dbname="testdb"):
        tdSql.query(f"desc {dbname}.stb1 ")
        schema_list = tdSql.queryResult
        for col_type in schema_list:
            if col_type[1] in ["TINYINT" ,"SMALLINT","BIGINT" ,"INT","FLOAT","DOUBLE","TINYINT UNSIGNED" ,"SMALLINT UNSIGNED","BIGINT UNSIGNED" ,"INT UNSIGNED"]:
                tdSql.query(f"select twa({col_type[0]}) from {dbname}.stb1 partition by tbname ")
            else:
                tdSql.error(f"select twa({col_type[0]}) from {dbname}.stb1 partition by tbname ")


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

    def distribute_twa_query(self, dbname="testdb"):
        # basic filter
        tdSql.query(f"select twa(c1) from {dbname}.ct1  ")
        tdSql.checkData(0,0,1.000000000)

        tdSql.query(f"select twa(c1) from {dbname}.stb1 partition by tbname  ")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkData(0,0,1.000000000)

        tdSql.query(f"select twa(c2) from {dbname}.stb1 group by tbname ")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkData(0,0,11111.000000000)

        tdSql.query(f"select twa(c1+c2) from {dbname}.stb1 partition by tbname ")
        tdSql.checkData(0,0,11112.000000000)

        tdSql.query(f"select twa(c1) from {dbname}.stb1 partition by t1")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkData(0,0,1.000000000)
        
        tdSql.query(f"select twa(c11) from {dbname}.ct1  ")
        tdSql.checkData(0,0,1.000000000)

        tdSql.query(f"select twa(c11) from {dbname}.stb1 partition by tbname  ")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkData(0,0,1.000000000)

        tdSql.query(f"select twa(c12) from {dbname}.stb1 group by tbname ")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkData(0,0,11111.000000000)

        tdSql.query(f"select twa(c11+c12) from {dbname}.stb1 partition by tbname ")
        tdSql.checkData(0,0,11112.000000000)

        tdSql.query(f"select twa(c11) from {dbname}.stb1 partition by t1")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkData(0,0,1.000000000)
        
        tdSql.query(f"select twa(c13) from {dbname}.stb1 partition by tbname  ")
        tdSql.checkRows(self.tb_nums)
        
        tdSql.query(f"select twa(c13) from {dbname}.stb1 group by tbname  ")
        tdSql.checkRows(self.tb_nums)
        
        tdSql.query(f"select twa(c14) from {dbname}.stb1 partition by tbname  ")
        tdSql.checkRows(self.tb_nums)
        
        tdSql.query(f"select twa(c14) from {dbname}.stb1 group by tbname  ")
        tdSql.checkRows(self.tb_nums)

        # union all
        tdSql.query(f"select twa(c1) from {dbname}.stb1 partition by tbname union all select twa(c1) from {dbname}.stb1 partition by tbname ")
        tdSql.checkRows(40)
        tdSql.checkData(0,0,1.000000000)
        tdSql.query(f"select twa(c11) from {dbname}.stb1 partition by tbname union all select twa(c11) from {dbname}.stb1 partition by tbname ")
        tdSql.checkRows(40)
        tdSql.checkData(0,0,1.000000000)
        
        tdSql.query(f"select twa(c2) from {dbname}.stb1 partition by tbname union all select twa(c2) from {dbname}.stb1 partition by tbname ")
        tdSql.checkRows(40)
        tdSql.query(f"select twa(c3) from {dbname}.stb1 partition by tbname union all select twa(c3) from {dbname}.stb1 partition by tbname ")
        tdSql.checkRows(40)
        tdSql.query(f"select twa(c4) from {dbname}.stb1 partition by tbname union all select twa(c4) from {dbname}.stb1 partition by tbname ")
        tdSql.checkRows(40)
        tdSql.query(f"select twa(c12) from {dbname}.stb1 partition by tbname union all select twa(c12) from {dbname}.stb1 partition by tbname ")
        tdSql.checkRows(40)
        tdSql.query(f"select twa(c13) from {dbname}.stb1 partition by tbname union all select twa(c13) from {dbname}.stb1 partition by tbname ")
        tdSql.checkRows(40)
        tdSql.query(f"select twa(c14) from {dbname}.stb1 partition by tbname union all select twa(c14) from {dbname}.stb1 partition by tbname ")
        tdSql.checkRows(40)

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

        tdSql.query(f"select twa(tb1.c1), twa(tb2.c2) from db.tb1 tb1, db.tb2 tb2 where tb1.ts=tb2.ts ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,4.500000000)
        tdSql.checkData(0,1,4.500000000)
        

        # mixup with other functions
        tdSql.query(f"select twa(c1),twa(c2),max(c1),elapsed(ts) from {dbname}.ct1 ")
        tdSql.checkData(0,0,1.000000000)
        tdSql.checkData(0,1,11111.000000000)
        tdSql.checkData(0,2,1)

    def run(self):
        self.prepare_datas_of_distribute()
        self.check_distribute_datas()
        self.twa_support_types()
        self.distribute_twa_query()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
