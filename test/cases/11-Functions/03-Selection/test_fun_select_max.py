import numpy as np
import random

from new_test_framework.utils import tdLog, tdSql

class TestFunMax:
    updatecfgDict = {"maxTablesPerVnode":2 ,"minTablesPerVnode":2,"tableIncStepPerVnode":2 }

    #
    # ------------------ sim case ------------------
    #

    def do_sim_max(self):
        dbPrefix = "m_ma_db"
        tbPrefix = "m_ma_tb"
        mtPrefix = "m_ma_mt"
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
                sql += f" ({ms} , {x} )"
                x = x + 1
            i = i + 1
            tdSql.execute(sql)

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select max(tbcol) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 19)

        tdLog.info(f"=============== step3")
        cc = 4 * 60000
        ms = 1601481600000 + cc

        tdSql.query(f"select max(tbcol) from {tb} where ts <= {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 4)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select max(tbcol) as b from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 19)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select max(tbcol) as b from {tb} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select max(tbcol) as b from {tb} interval(1d)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 19)

        tdLog.info(f"=============== step6")
        cc = 4 * 60000
        ms = 1601481600000 + cc

        tdSql.query(f"select max(tbcol) as b from {tb} where ts <= {ms} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)
        tdSql.checkRows(5)

        tdLog.info(f"=============== step7")
        tdSql.query(f"select max(tbcol) from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 19)

        tdLog.info(f"=============== step8")
        cc = 4 * 60000
        ms = 1601481600000 + cc

        tdSql.query(f"select max(tbcol) as c from {mt} where ts <= {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 4)

        tdSql.query(f"select max(tbcol) as c from {mt} where tgcol < 5")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 19)

        cc = 4 * 60000
        ms = 1601481600000 + cc

        tdSql.query(f"select max(tbcol) as c from {mt} where tgcol < 5 and ts <= {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 4)

        tdLog.info(f"=============== step9")
        tdSql.query(f"select max(tbcol) as b from {mt} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select max(tbcol) as b from {mt} interval(1d)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 19)

        tdLog.info(f"=============== step10")
        tdSql.query(f"select max(tbcol) as b from {mt} group by tgcol")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 19)
        tdSql.checkRows(tbNum)

        tdLog.info(f"=============== step11")
        cc = 4 * 60000
        ms = 1601481600000 + cc

        tdSql.query(
            f"select max(tbcol) as b from {mt}  where ts <= {ms} partition by tgcol interval(1m)"
        )
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)
        tdSql.checkRows(50)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2) 
        print("\n")
        print("do_sim_max ............................ [passed]")

    #
    # ------------------ test_max.py ------------------
    #

    def max_check_stb_and_tb_base(self, dbname="db"):
        tdSql.prepare()
        intData = []
        floatData = []
        tdSql.execute(f'''create table {dbname}.stb(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 tinyint unsigned, col6 smallint unsigned,
                    col7 int unsigned, col8 bigint unsigned, col9 float, col10 double, col11 bool, col12 binary(20), col13 nchar(20)) tags(t0 tinyint, t1 float, loc nchar(20))''')
        tdSql.execute(f"create table {dbname}.stb_1 using {dbname}.stb tags(5, 5.5, 'beijing')")
        sql = f"insert into {dbname}.stb_1 values"
        for i in range(self.rowNum):
            sql += f" (%d, %d, %d, %d, %d, %d, %d, %d, %d, %f, %f, %d, '{self.binary_str}%d', '{self.nchar_str}%d')"%(self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1)
            intData.append(i + 1)
            floatData.append(i + 0.1)
        tdSql.execute(sql)    

        for i in ['col11','col12','col13']:
            for j in ['stb','stb_1']:
                tdSql.error(f'select max({i} from {dbname}.{j} )')

        for i in range(1,11):
            for j in ['stb', 'stb_1']:
                tdSql.query(f"select max(col{i}) from {dbname}.{j}")
                if i<9:
                    tdSql.checkData(0, 0, np.max(intData))
                elif i>=9:
                    tdSql.checkData(0, 0, np.max(floatData))

        tdSql.error(f"select max(now()) from {dbname}.stb_1")

        tdSql.query(f"select max(col1) from {dbname}.stb_1 where col2<=5")
        tdSql.checkData(0,0,5)
        tdSql.query(f"select max(col1) from {dbname}.stb where col2<=5")
        tdSql.checkData(0,0,5)

        tdSql.query(f"select ts, max(col1) from {dbname}.stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, np.max(intData))

        tdSql.query(f"select ts, max(col1) from {dbname}.stb_1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, np.max(intData))

        tdSql.query(f"select ts, min(col9) from {dbname}.stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, np.min(floatData))

        tdSql.query(f"select ts, min(col9) from {dbname}.stb_1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, np.min(floatData))

        # check tags
        tdSql.query(f"select max(t0) from {dbname}.stb")
        tdSql.checkData(0,0,5)

        tdSql.query(f"select max(t1) from {dbname}.stb")
        tdSql.checkData(0,0,5.5)

    def max_check_ntb_base(self, dbname="db"):
        tdSql.prepare()
        intData = []
        floatData = []
        tdSql.execute(f'''create table {dbname}.ntb(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 tinyint unsigned, col6 smallint unsigned,
                    col7 int unsigned, col8 bigint unsigned, col9 float, col10 double, col11 bool, col12 binary(20), col13 nchar(20))''')
        sql = f"insert into {dbname}.ntb values"
        for i in range(self.rowNum):
            sql += f" (%d, %d, %d, %d, %d, %d, %d, %d, %d, %f, %f, %d, '{self.binary_str}%d', '{self.nchar_str}%d')"%(self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1)
            intData.append(i + 1)
            floatData.append(i + 0.1)
        tdSql.execute(sql)    
        for i in ['col11','col12','col13']:
            for j in ['ntb']:
                tdSql.error(f'select max({i} from {dbname}.{j} )')
        for i in range(1,11):
            for j in ['ntb']:
                tdSql.query(f"select max(col{i}) from {dbname}.{j}")
                if i<9:
                    tdSql.checkData(0, 0, np.max(intData))
                elif i>=9:
                    tdSql.checkData(0, 0, np.max(floatData))

        tdSql.error(f"select max(now()) from {dbname}.ntb")

        tdSql.query(f"select max(col1) from {dbname}.ntb where col2<=5")
        tdSql.checkData(0,0,5)

    def check_max_functions(self, tbname , col_name):

        max_sql = f"select max({col_name}) from {tbname};"

        same_sql = f"select {col_name} from {tbname} order by {col_name} desc limit 1"

        tdSql.query(max_sql)
        max_result = tdSql.queryResult

        tdSql.query(same_sql)
        same_result = tdSql.queryResult

        if max_result !=same_result:
            tdLog.exit(" max function work not as expected, sql : %s "% max_sql)
        else:
            tdLog.info(" max function work as expected, sql : %s "% max_sql)

    def support_distributed_aggregate(self, dbname="testdb"):

        # prepate datas for  20 tables distributed at different vgroups
        tdSql.execute(f"create database if not exists {dbname} keep 3650 duration 100 vgroups 5")
        tdSql.execute(f"use {dbname} ")
        tdSql.execute(
            f'''create table {dbname}.stb1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            tags (t0 timestamp, t1 int, t2 bigint, t3 smallint, t4 tinyint, t5 float, t6 double, t7 bool, t8 binary(16),t9 nchar(32))
            '''
        )

        tdSql.execute(
            f'''
            create table {dbname}.t1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
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
                sql = f"insert into {tbname} values "
                for j in range(9):
                    sql += f" (now()-{(i+j)*10}s, {1*(j+i)}, {11111*(j+i)}, {111*(j+i)}, {11*(j)}, {1.11*(j+i)}, {11.11*(j+i)}, {(j+i)%2}, 'binary{j}', 'nchar{j}', now()+{1*j}a )"
                tdSql.execute(sql)

        tdSql.execute(f"insert into {dbname}.ct1 values (now()-45s, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar0', now()+8a )")
        tdSql.execute(f"insert into {dbname}.ct1 values (now()+10s, 9, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute(f"insert into {dbname}.ct1 values (now()+15s, 9, -99999, -999, -99, -9.99, NULL, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute(f"insert into {dbname}.ct1 values (now()+20s, 9, -99999, -999, NULL, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")

        tdSql.execute(f"insert into {dbname}.ct4 values (now()-810d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(f"insert into {dbname}.ct4 values (now()-400d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(f"insert into {dbname}.ct4 values (now()+90d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL  ) ")

        tdSql.execute(
            f'''insert into {dbname}.t1 values
            ( '2020-04-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( '2020-10-21 01:01:01.000', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now()+1a )
            ( '2020-12-31 01:01:01.000', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now()+2a )
            ( '2021-01-01 01:01:06.000', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now()+3a )
            ( '2021-05-07 01:01:10.000', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now()+4a )
            ( '2021-07-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( '2021-09-30 01:01:16.000', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now()+5a )
            ( '2022-02-01 01:01:20.000', 6, 66666, 666, 66, 6.66, 66.66, 1, "binary6", "nchar6", now()+6a )
            ( '2022-10-28 01:01:26.000', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", "1970-01-01 08:00:00.000" )
            ( '2022-12-01 01:01:30.000', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", "1969-01-01 01:00:00.000" )
            ( '2022-12-31 01:01:36.000', 9, -99999999999999999, -999, -99, -9.99, -999999999999999999999.99, 1, "binary9", "nchar9", "1900-01-01 00:00:00.000" )
            ( '2023-02-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            '''
        )

        tdLog.info(" prepare data for distributed_aggregate done! ")

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

        count = 0
        for k ,v in vnode_tables.items():
            if len(v)>=2:
                count+=1
        if count < 2:
            tdLog.exit(" the datas of all not satisfy sub_table has been distributed ")

        # check max function work status

        tdSql.query(f"show {dbname}.tables like 'ct%'")
        table_names = tdSql.queryResult
        tablenames = []
        for table_name in table_names:
            tablenames.append(table_name[0])

        tdSql.query(f"desc {dbname}.stb1")
        col_names = tdSql.queryResult

        colnames = []
        for col_name in col_names:
            if col_name[1] in ["INT" ,"BIGINT" ,"SMALLINT" ,"TINYINT" , "FLOAT" ,"DOUBLE"]:
                colnames.append(col_name[0])

        for tablename in tablenames:
            for colname in colnames:
                self.check_max_functions(f"{dbname}.{tablename}", colname)

    def do_max(self):
        # init
        self.rowNum = 10
        self.ts = 1537146000000
        self.binary_str = 'taosdata'
        self.nchar_str = '涛思数据'

        # max verifacation
        self.max_check_stb_and_tb_base()
        self.max_check_ntb_base()

        self.support_distributed_aggregate()
        print("do_max ................................ [passed]")

    #
    # ------------------ test_distribute_min.py ------------------
    #

    def check_max_functions(self, tbname , col_name):

        max_sql = f"select max({col_name}) from {tbname};"

        same_sql = f"select {col_name} from {tbname} order by {col_name} desc limit 1"

        tdSql.query(max_sql)
        max_result = tdSql.queryResult

        tdSql.query(same_sql)
        same_result = tdSql.queryResult

        if max_result !=same_result:
            tdLog.exit(" max function work not as expected, sql : %s "% max_sql)
        else:
            tdLog.info(" max function work as expected, sql : %s "% max_sql)


    def prepare_datas_of_distribute(self, dbname="testdb"):

        # prepate datas for  20 tables distributed at different vgroups
        tdSql.execute(f"create database if not exists {dbname} keep 3650 duration 100 vgroups 5")
        tdSql.execute(f" use {dbname} ")
        tdSql.execute(f"drop table if exists {dbname}.stb1")
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
                sql = f"insert into {tbname} values"
                for j in range(9):
                    sql += f" (now()-{(i+j)*10}s, {1*(j+i)}, {11111*(j+i)}, {111*(j+i)}, {11*(j)}, {1.11*(j+i)}, {11.11*(j+i)}, {(j+i)%2}, 'binary{j}', 'nchar{j}', now()+{1*j}a )"
                tdSql.execute(sql)

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

    def check_max_distribute_diff_vnode(self,col_name, dbname="testdb"):

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

        max_sql = f"select max({col_name}) from {dbname}.stb1 where tbname in ({tbname_filters});"

        same_sql = f"select {col_name} from {dbname}.stb1 where tbname in ({tbname_filters}) order by {col_name} desc limit 1"

        tdSql.query(max_sql)
        max_result = tdSql.queryResult

        tdSql.query(same_sql)
        same_result = tdSql.queryResult

        if max_result !=same_result:
            tdLog.exit(" max function work not as expected, sql : %s "% max_sql)
        else:
            tdLog.info(" max function work as expected, sql : %s "% max_sql)

    def check_max_status(self, dbname="testdb"):
        # check max function work status

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
                self.check_max_functions(tablename,colname)

        # check max function for different vnode

        for colname in colnames:
            if colname.startswith("c"):
                self.check_max_distribute_diff_vnode(colname, dbname)
            else:
                # self.check_max_distribute_diff_vnode(colname, dbname) # bug for tag
                pass

    def distribute_agg_query(self, dbname="testdb"):
        # basic filter
        tdSql.query(f"select max(c1) from {dbname}.stb1 where c1 is null")
        tdSql.checkRows(1)

        tdSql.query(f"select max(c1) from {dbname}.stb1 where t1=1")
        tdSql.checkData(0,0,10)

        tdSql.query(f"select max(c1+c2) from {dbname}.stb1 where c1 =1 ")
        tdSql.checkData(0,0,11112.000000000)

        tdSql.query(f"select max(c1) from {dbname}.stb1 where tbname=\"ct2\"")
        tdSql.checkData(0,0,10)

        tdSql.query(f"select max(c1) from {dbname}.stb1 partition by tbname")
        tdSql.checkRows(20)

        tdSql.query(f"select max(c1) from {dbname}.stb1 where t1> 4  partition by tbname")
        tdSql.checkRows(15)

        # union all
        tdSql.query(f"select max(c1) from {dbname}.stb1 union all select max(c1) from {dbname}.stb1 ")
        tdSql.checkRows(2)
        tdSql.checkData(0,0,28)

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

        tdSql.query(f"select max(tb1.c1), tb2.c2 from db.tb1 tb1, db.tb2 tb2 where tb1.ts=tb2.ts")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,9)
        tdSql.checkData(0,0,9.00000)

        # group by
        tdSql.execute("use testdb ")
        tdSql.query(f"select max(c1),c1  from {dbname}.stb1 group by t1 ")
        tdSql.checkRows(20)
        tdSql.query(f"select max(c1),c1  from {dbname}.stb1 group by c1 ")
        tdSql.checkRows(30)
        tdSql.query(f"select max(c1),c2  from {dbname}.stb1 group by c2 ")
        tdSql.checkRows(31)

        # selective common cols of datas
        tdSql.query(f"select max(c1),c2,c3,c5 from {dbname}.stb1")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,28)
        tdSql.checkData(0,1,311108)
        tdSql.checkData(0,2,3108)
        tdSql.checkData(0,3,31.08000)

        tdSql.query(f"select max(c1),t1,c2,t3 from {dbname}.stb1")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,28)
        tdSql.checkData(0,1,19)
        tdSql.checkData(0,2,311108)

        tdSql.query(f"select max(c1),ceil(t1),pow(c2,1)+2,abs(t3) from {dbname}.stb1")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,28)
        tdSql.checkData(0,1,19)
        tdSql.checkData(0,2,311110.000000000)
        tdSql.checkData(0,3,2109)

        # partition by tbname or partition by tag
        tdSql.query(f"select max(c1),tbname from {dbname}.stb1 partition by tbname")
        query_data = tdSql.queryResult

        for row in query_data:
            tbname = f"{dbname}.{row[1]}"
            tdSql.query(f"select max(c1) from %s "%tbname)
            tdSql.checkData(0,0,row[0])

        tdSql.query(f"select max(c1),tbname from {dbname}.stb1 partition by t1")
        query_data = tdSql.queryResult

        for row in query_data:
            tbname = f"{dbname}.{row[1]}"
            tdSql.query(f"select max(c1) from %s "%tbname)
            tdSql.checkData(0,0,row[0])

        # nest query for support max
        tdSql.query(f"select abs(c2+2)+1 from (select max(c1) c2  from {dbname}.stb1)")
        tdSql.checkData(0,0,31.000000000)
        tdSql.query(f"select max(c1+2)+1  as c2 from (select ts ,c1 ,c2  from {dbname}.stb1)")
        tdSql.checkData(0,0,31.000000000)
        tdSql.query(f"select max(a+2)+1  as c2 from (select ts ,abs(c1) a ,c2  from {dbname}.stb1)")
        tdSql.checkData(0,0,31.000000000)

        # mixup with other functions
        tdSql.query(f"select max(c1),count(c1),last(c2,c3) from {dbname}.stb1")
        tdSql.checkData(0,0,28)
        tdSql.checkData(0,1,184)
        tdSql.checkData(0,2,-99999)
        tdSql.checkData(0,3,-999)

    def do_distribute_max(self):
        # init
        self.vnode_disbutes = None
        self.ts = 1537146000000

        # do
        self.prepare_datas_of_distribute()
        self.check_distribute_datas()
        self.check_max_status()
        self.distribute_agg_query()
        print("do_distribute_max ..................... [passed]")

    #
    # ------------------ main ------------------
    #
    def test_func_agg_max(self):
        """ Fun: max()

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
            - 2025-4-28 Simon Guan Migrated from tsim/compute/max.sim
            - 2025-9-28 Alex  Duan Migrated from uncatalog/system-test/2-query/test_max.py
            - 2025-9-28 Alex  Duan Migrated from uncatalog/system-test/2-query/test_distribute_agg_max.py
        """

        self.do_sim_max()
        self.do_max()
        self.do_distribute_max()