import numpy as np
import random

from wsgiref.headers import tspecials
from new_test_framework.utils import tdLog, tdSql

class TestFunMin:
    updatecfgDict = {"maxTablesPerVnode":2 ,"minTablesPerVnode":2,"tableIncStepPerVnode":2 }
    #
    # ------------------ sim case ------------------
    #
    def do_sim_min(self):
        dbPrefix = "m_mi_db"
        tbPrefix = "m_mi_tb"
        mtPrefix = "m_mi_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol int)")

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")
            x = 0
            sql = f"insert into {tb} values"
            while x < rowNum:
                cc = x * 60000
                ms = 1601481600000 + cc
                sql += f" ({ms},{x})"
                x = x + 1
            i = i + 1
            tdSql.execute(sql)

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select min(tbcol) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdLog.info(f"=============== step3")
        cc = 4 * 60000
        ms = 1601481600000 + cc

        tdSql.query(f"select min(tbcol) from {tb} where ts < {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select min(tbcol) as b from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select min(tbcol) as b from {tb} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select min(tbcol) as b from {tb} interval(1d)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdLog.info(f"=============== step6")
        cc = 4 * 60000
        ms = 1601481600000 + cc

        tdSql.query(f"select min(tbcol) as b from {tb} where ts <= {ms} interval(1m)")
        tdLog.info(f"select min(tbcol) as b from {tb} where ts <= {ms} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)
        tdSql.checkRows(5)

        tdLog.info(f"=============== step7")
        tdSql.query(f"select min(tbcol) from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdLog.info(f"=============== step8")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(f"select min(tbcol) as c from {mt} where ts < {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select min(tbcol) as c from {mt} where tgcol < 5")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(f"select min(tbcol) as c from {mt} where tgcol < 5 and ts <= {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdLog.info(f"=============== step9")
        tdSql.query(f"select min(tbcol) as b from {mt} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select min(tbcol) as b from {mt} interval(1d)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdLog.info(f"=============== step10")
        tdSql.query(f"select min(tbcol) as b from {mt} group by tgcol")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)
        tdSql.checkRows(tbNum)

        tdLog.info(f"=============== step11")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(
            f"select min(tbcol) as b from {mt}  where ts <= {ms} partition by tgcol interval(1m)"
        )
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)
        tdSql.checkRows(50)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
        print("\n")
        print("do_sim_min ............................ [passed]")

    #
    # ------------------ test_min.py ------------------
    #

    def do_min(self):
        # init
        self.rowNum = 10
        self.ts = 1537146000000

        # do
        dbname = "db"
        tdSql.prepare()

        intData = []
        floatData = []

        tdSql.execute(f'''create table {dbname}.stb(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double,
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned) tags(loc nchar(20))''')
        tdSql.execute(f"create table {dbname}.stb_1 using {dbname}.stb tags('beijing')")
        tdSql.execute(f'''create table {dbname}.ntb(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double,
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned)''')
        sql = f"insert into {dbname}.ntb values"
        for i in range(self.rowNum):
            sql += f" (%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)" % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1)
            intData.append(i + 1)
            floatData.append(i + 0.1)
        tdSql.execute(sql)

        sql = f"insert into {dbname}.stb_1 values"
        for i in range(self.rowNum):
            sql += f" (%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)"% (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1)
            intData.append(i + 1)
            floatData.append(i + 0.1)
        tdSql.execute(sql)

        # max verifacation
        tdSql.error(f"select min(now()) from {dbname}.stb_1")
        tdSql.error(f"select min(ts) from {dbname}.stb_1")
        tdSql.error(f"select min(col7) from {dbname}.stb_1")
        tdSql.error(f"select min(a) from {dbname}.stb_1")
        tdSql.query(f"select min(1) from {dbname}.stb_1")
        tdSql.error(f"select min(count(c1),count(c2)) from {dbname}.stb_1")

        tdSql.query(f"select min(col1) from {dbname}.stb_1")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col2) from {dbname}.stb_1")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col3) from {dbname}.stb_1")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col4) from {dbname}.stb_1")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col11) from {dbname}.stb_1")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col12) from {dbname}.stb_1")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col13) from {dbname}.stb_1")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col14) from {dbname}.stb_1")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col5) from {dbname}.stb_1")
        tdSql.checkData(0, 0, np.min(floatData))
        tdSql.query(f"select min(col6) from {dbname}.stb_1")
        tdSql.checkData(0, 0, np.min(floatData))
        tdSql.query(f"select min(col1) from {dbname}.stb_1 where col2>=5")
        tdSql.checkData(0,0,5)
        tdSql.query(f"select min(col8) from {dbname}.stb_1")
        tdSql.checkData(0,0,'taosdata1')
        tdSql.query(f"select min(col9) from {dbname}.stb_1")
        tdSql.checkData(0,0,'涛思数据1')

        tdSql.error(f"select min(now()) from {dbname}.stb_1")
        tdSql.error(f"select min(ts) from {dbname}.stb_1")
        tdSql.error(f"select min(col7) from {dbname}.stb_1")
        tdSql.error(f"select min(a) from {dbname}.stb_1")
        tdSql.query(f"select min(1) from {dbname}.stb_1")
        tdSql.error(f"select min(count(c1),count(c2)) from {dbname}.stb_1")

        tdSql.query(f"select min(col1) from {dbname}.stb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col2) from {dbname}.stb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col3) from {dbname}.stb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col4) from {dbname}.stb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col11) from {dbname}.stb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col12) from {dbname}.stb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col13) from {dbname}.stb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col14) from {dbname}.stb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col5) from {dbname}.stb")
        tdSql.checkData(0, 0, np.min(floatData))
        tdSql.query(f"select min(col6) from {dbname}.stb")
        tdSql.checkData(0, 0, np.min(floatData))
        tdSql.query(f"select min(col1) from {dbname}.stb where col2>=5")
        tdSql.checkData(0,0,5)
        tdSql.query(f"select min(col8) from {dbname}.stb")
        tdSql.checkData(0,0,'taosdata1')
        tdSql.query(f"select min(col9) from {dbname}.stb")
        tdSql.checkData(0,0,'涛思数据1')

        tdSql.error(f"select min(now()) from {dbname}.stb_1")
        tdSql.error(f"select min(ts) from {dbname}.stb_1")
        tdSql.error(f"select min(col7) from {dbname}.ntb")
        tdSql.error(f"select min(a) from {dbname}.ntb")
        tdSql.query(f"select min(1) from {dbname}.ntb")
        tdSql.error(f"select min(count(c1),count(c2)) from {dbname}.ntb")

        tdSql.query(f"select min(col1) from {dbname}.ntb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col2) from {dbname}.ntb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col3) from {dbname}.ntb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col4) from {dbname}.ntb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col11) from {dbname}.ntb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col12) from {dbname}.ntb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col13) from {dbname}.ntb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col14) from {dbname}.ntb")
        tdSql.checkData(0, 0, np.min(intData))
        tdSql.query(f"select min(col5) from {dbname}.ntb")
        tdSql.checkData(0, 0, np.min(floatData))
        tdSql.query(f"select min(col6) from {dbname}.ntb")
        tdSql.checkData(0, 0, np.min(floatData))
        tdSql.query(f"select min(col1) from {dbname}.ntb where col2>=5")
        tdSql.checkData(0,0,5)
        tdSql.query(f"select min(col8) from {dbname}.ntb")
        tdSql.checkData(0,0,'taosdata1')
        tdSql.query(f"select min(col9) from {dbname}.ntb")
        tdSql.checkData(0,0,'涛思数据1')
        
        print("do_min ................................ [passed]")


    #
    # ------------------ test_distribute_min.py ------------------
    #
    def check_min_functions(self, tbname , col_name):

        min_sql = f"select min({col_name}) from {tbname};"

        same_sql = f"select {col_name} from {tbname} where {col_name} is not null order by {col_name} asc limit 1"

        tdSql.query(min_sql)
        min_result = tdSql.queryResult

        tdSql.query(same_sql)
        same_result = tdSql.queryResult

        if min_result !=same_result:
            tdLog.exit(" min function work not as expected, sql : %s "% min_sql)
        else:
            tdLog.info(" min function work as expected, sql : %s "% min_sql)

    def prepare_datas_of_distribute(self, dbname="testdb"):

        # prepate datas for  20 tables distributed at different vgroups
        tdSql.execute(f"create database if not exists {dbname} keep 3650 duration 100 vgroups 5")
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

    def check_min_distribute_diff_vnode(self,col_name, dbname="testdb"):

        vgroup_ids = []
        for k ,v in self.vnode_disbutes.items():
            if len(v)>=2:
                vgroup_ids.append(k)

        distribute_tbnames = []

        for vgroup_id in vgroup_ids:
            vnode_tables = self.vnode_disbutes[vgroup_id]
            distribute_tbnames.append(random.sample(vnode_tables,1)[0])
        tbname_ins = ""
        for tbname in distribute_tbnames:
            tbname_ins += "'%s' ,"%tbname

        tbname_filters = tbname_ins[:-1]

        min_sql = f"select min({col_name}) from {dbname}.stb1 where tbname in ({tbname_filters});"

        same_sql = f"select {col_name} from {dbname}.stb1 where tbname in ({tbname_filters}) and {col_name} is not null order by {col_name} asc limit 1"

        tdSql.query(min_sql)
        min_result = tdSql.queryResult

        tdSql.query(same_sql)
        same_result = tdSql.queryResult

        if min_result !=same_result:
            tdLog.exit(" min function work not as expected, sql : %s "% min_sql)
        else:
            tdLog.info(" min function work as expected, sql : %s "% min_sql)

    def check_min_status(self, dbname="testdb"):
        # check min function work status

        tdSql.query(f"show {dbname}.tables like 'ct%'")
        table_names = tdSql.queryResult
        tablenames = []
        for table_name in table_names:
            tablenames.append(f"{dbname}.{table_name[0]}")

        tdSql.query(f"desc {dbname}.stb1")
        col_names = tdSql.queryResult

        colnames = []
        for col_name in col_names:
            if col_name[1] in ["INT" ,"BIGINT" ,"SMALLINT" ,"TINYINT" , "FLOAT" ,"DOUBLE"]:
                colnames.append(col_name[0])

        for tablename in tablenames:
            for colname in colnames:
                self.check_min_functions(tablename,colname)

        # check min function for different vnode

        for colname in colnames:
            if colname.startswith("c"):
                self.check_min_distribute_diff_vnode(colname, dbname)
            else:
                # self.check_min_distribute_diff_vnode(colname, dbname) # bug for tag
                pass

    def distribute_agg_query(self, dbname="testdb"):
        # basic filter
        tdSql.query(f"select min(c1) from {dbname}.stb1 where c1 is null")
        tdSql.checkRows(1)

        tdSql.query(f"select min(c1) from {dbname}.stb1 where t1=1")
        tdSql.checkData(0,0,2)

        tdSql.query(f"select min(c1+c2) from {dbname}.stb1 where c1 =1 ")
        tdSql.checkData(0,0,11112.000000000)

        tdSql.query(f"select min(c1) from {dbname}.stb1 where tbname=\"ct2\"")
        tdSql.checkData(0, 0, 2)

        tdSql.query(f"select min(c1) from {dbname}.stb1 partition by tbname")
        tdSql.checkRows(20)

        tdSql.query(f"select min(c1) from {dbname}.stb1 where t1> 4  partition by tbname")
        tdSql.checkRows(15)

        # union all
        tdSql.query(f"select min(c1) from {dbname}.stb1 union all select min(c1) from {dbname}.stb1 ")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 0)

        # join

        tdSql.execute(" create database if not exists db ")
        tdSql.execute(" use db ")
        tdSql.execute(" create stable db.st (ts timestamp , c1 int ,c2 float) tags(t1 int) ")
        tdSql.execute(" create table db.tb1 using db.st tags(1) ")
        tdSql.execute(" create table db.tb2 using db.st tags(2) ")

        values = "values"
        for i in range(10):
            ts = i*10 + self.ts
            values += f" ({ts},{i},{i}.0)"
        sql = f"insert into db.tb1 {values} db.tb2 {values}"
        tdSql.execute(sql)    

        tdSql.query(f"select min(tb1.c1), tb2.c2 from db.tb1 tb1, db.tb2 tb2 where tb1.ts=tb2.ts")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,0)
        tdSql.checkData(0,0,0.00000)

        # group by
        tdSql.execute(f"use {dbname} ")
        tdSql.query(f"select min(c1),c1  from {dbname}.stb1 group by t1 ")
        tdSql.checkRows(20)
        tdSql.query(f"select min(c1),c1  from {dbname}.stb1 group by c1 ")
        tdSql.checkRows(30)
        tdSql.query(f"select min(c1),c2  from {dbname}.stb1 group by c2 ")
        tdSql.checkRows(31)

        # selective common cols of datas
        tdSql.query(f"select min(c1),c2,c3,c5 from {dbname}.stb1")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,0)
        tdSql.checkData(0,1,0)
        tdSql.checkData(0,2,0)
        tdSql.checkData(0,3,0)

        tdSql.query(f"select min(c1),t1,c2,t3 from {dbname}.stb1 where c1 > 5")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,6)
        tdSql.checkData(0,2,66666)

        tdSql.query(f"select min(c1),ceil(t1),pow(c2,1)+2,abs(t3) from {dbname}.stb1 where c1 > 12")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,13)
        tdSql.checkData(0,2,144445.000000000)

        # partition by tbname or partition by tag
        tdSql.query(f"select min(c1),tbname from {dbname}.stb1 partition by tbname")
        query_data = tdSql.queryResult

        for row in query_data:
            tbname = f"{dbname}.{row[1]}"
            tdSql.query(f"select min(c1) from %s "%tbname)
            tdSql.checkData(0,0,row[0])

        tdSql.query(f"select min(c1),tbname from {dbname}.stb1 partition by t1")
        query_data = tdSql.queryResult

        for row in query_data:
            tbname = f"{dbname}.{row[1]}"
            tdSql.query(f"select min(c1) from %s "%tbname)
            tdSql.checkData(0,0,row[0])

        # nest query for support min
        tdSql.query(f"select abs(c2+2)+1 from (select min(c1) c2  from {dbname}.stb1)")
        tdSql.checkData(0,0,3.000000000)
        tdSql.query(f"select min(c1+2)+1  as c2 from (select ts ,c1 ,c2  from {dbname}.stb1)")
        tdSql.checkData(0,0,3.000000000)
        tdSql.query(f"select min(a+2)+1  as c2 from (select ts ,abs(c1) a ,c2  from {dbname}.stb1)")
        tdSql.checkData(0,0,3.000000000)

        # mixup with other functions
        tdSql.query(f"select max(c1),count(c1),last(c2,c3) from {dbname}.stb1")
        tdSql.checkData(0,0,28)
        tdSql.checkData(0,1,184)
        tdSql.checkData(0,2,-99999)
        tdSql.checkData(0,3,-999)

    def do_distribute_min(self):
        # init
        self.vnode_disbutes = None
        self.ts = 1537146000000
        
        # do        
        self.prepare_datas_of_distribute()
        self.check_distribute_datas()
        self.check_min_status()
        self.distribute_agg_query()

        print("do_distribute_min ..................... [passed]")

    #
    # ------------------ main ------------------
    #
    def test_func_agg_min(self):
        """ Fun: min()

        1. Sim case including time windows, filtering on ordinary data columns, filtering on tag columns, GROUP BY, and PARTITION BY.
        2. Query on super/child/normal table
        3. Support types
        4. Error cases
        5. Query with filter conditions
        6. Query with group by
        7. Query with distribute aggregate
        8. Check function status


        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/compute/m.sim
            - 2025-9-28 Alex  Duan Migrated from uncatalog/system-test/2-query/test_min.py
            - 2025-9-28 Alex  Duan Migrated from uncatalog/system-test/2-query/test_distribute_agg_min.py

        """

        self.do_sim_min()
        self.do_min()
        self.do_distribute_min()