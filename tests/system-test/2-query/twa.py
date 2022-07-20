from util.log import *
from util.cases import *
from util.sql import *
import numpy as np
import random ,os ,sys
import platform
import math

class TDTestCase:
    updatecfgDict = {'debugFlag': 143 ,"cDebugFlag":143,"uDebugFlag":143 ,"rpcDebugFlag":143 , "tmrDebugFlag":143 ,
    "jniDebugFlag":143 ,"simDebugFlag":143,"dDebugFlag":143, "dDebugFlag":143,"vDebugFlag":143,"mDebugFlag":143,"qDebugFlag":143,
    "wDebugFlag":143,"sDebugFlag":143,"tsdbDebugFlag":143,"tqDebugFlag":143 ,"fsDebugFlag":143 ,"udfDebugFlag":143,
    "maxTablesPerVnode":2 ,"minTablesPerVnode":2,"tableIncStepPerVnode":2 }

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.vnode_disbutes = None
        self.ts = 1537146000000
        self.tb_nums = 20
        self.row_nums = 100
        self.time_step = 1000

    def prepare_datas_of_distribute(self):

        # prepate datas for  20 tables distributed at different vgroups
        tdSql.execute("create database if not exists testdb keep 3650 duration 1000 vgroups 5")
        tdSql.execute(" use testdb ")
        tdSql.execute(
            '''create table stb1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            tags (t0 timestamp, t1 int, t2 bigint, t3 smallint, t4 tinyint, t5 float, t6 double, t7 bool, t8 binary(16),t9 nchar(32))
            '''
        )

        for i in range(self.tb_nums):
            tdSql.execute(f'create table ct{i+1} using stb1 tags ( now(), {1*i}, {11111*i}, {111*i}, {1*i}, {1.11*i}, {11.11*i}, {i%2}, "binary{i}", "nchar{i}" )')
            ts = self.ts
            for j in range(self.row_nums):
                ts+=j*self.time_step
                tdSql.execute(
                    f"insert into ct{i+1} values({ts}, 1, 11111, 111, 1, 1.11, 11.11, 2, 'binary{j}', 'nchar{j}', now()+{1*j}a )"
                )

        tdSql.execute("insert into ct1 values (now()-810d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute("insert into ct1 values (now()-400d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute("insert into ct1 values (now()+90d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL  ) ")

        tdLog.info(" prepare data for distributed_aggregate done! ")

    def twa_support_types(self):
        tdSql.query("desc stb1 ")
        schema_list = tdSql.queryResult
        for col_type in schema_list:
            if col_type[1] in ["TINYINT" ,"SMALLINT","BIGINT" ,"INT","FLOAT","DOUBLE"]:
                tdSql.query(f" select twa({col_type[0]}) from stb1 partition by tbname ")
            else:
                tdSql.error(f" select twa({col_type[0]}) from stb1 partition by tbname ")


    def check_distribute_datas(self):
        # get vgroup_ids of all
        tdSql.query("show vgroups ")
        vgroups = tdSql.queryResult

        vnode_tables={}

        for vgroup_id in vgroups:
            vnode_tables[vgroup_id[0]]=[]

        # check sub_table of per vnode ,make sure sub_table has been distributed
        tdSql.query("show tables like 'ct%'")
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

    def distribute_twa_query(self):
        # basic filter
        tdSql.query(" select twa(c1) from ct1  ")
        tdSql.checkData(0,0,1.000000000)

        tdSql.query(" select twa(c1) from stb1 partition by tbname  ")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkData(0,0,1.000000000)

        tdSql.query(" select twa(c2) from stb1 group by tbname ")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkData(0,0,11111.000000000)

        tdSql.query("select twa(c1+c2) from stb1 partition by tbname ")
        tdSql.checkData(0,0,11112.000000000)

        tdSql.query("select twa(c1) from stb1 partition by t1")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkData(0,0,1.000000000)

        # union all
        tdSql.query(" select twa(c1) from stb1 partition by tbname union all select twa(c1) from stb1 partition by tbname ")
        tdSql.checkRows(40)
        tdSql.checkData(0,0,1.000000000)

        # join

        tdSql.execute(" create database if not exists db ")
        tdSql.execute(" use db ")
        tdSql.execute(" create stable st (ts timestamp , c1 int ,c2 float) tags(t1 int) ")
        tdSql.execute(" create table tb1 using st tags(1) ")
        tdSql.execute(" create table tb2 using st tags(2) ")


        for i in range(10):
            ts = i*10 + self.ts
            tdSql.execute(f" insert into tb1 values({ts},{i},{i}.0)")
            tdSql.execute(f" insert into tb2 values({ts},{i},{i}.0)")

        tdSql.query(" select twa(tb1.c1), twa(tb2.c2) from tb1, tb2 where tb1.ts=tb2.ts ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,4.500000000)
        tdSql.checkData(0,1,4.500000000)

        # group by
        tdSql.execute(" use testdb ")

        # mixup with other functions
        tdSql.query(" select twa(c1),twa(c2),max(c1),elapsed(ts) from stb1 ")
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
