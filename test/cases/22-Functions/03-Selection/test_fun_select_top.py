import string
import time
from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck
from new_test_framework.utils.sqlset import TDSetSql


class TestSelectTop:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    #
    # ------------------ sim case ------------------
    #
    def do_sim_top(self):
        self.TopBot()
        tdStream.dropAllStreamsAndDbs()
        self.Top()
        tdStream.dropAllStreamsAndDbs()
        print("\n")
        print("do_sim_top ............................ [passed]\n")

    def TopBot(self):
        dbPrefix = "tb_db"
        tbPrefix = "tb_tb"
        stbPrefix = "tb_stb"
        tbNum = 10
        rowNum = 1000
        totalNum = tbNum * rowNum
        loops = 200000
        log = 10000
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== topbot.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"create database {db} maxrows 4096 keep 36500")
        tdLog.info(f"====== create tables")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {stb} (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 smallint, c6 tinyint, c7 bool, c8 binary(10), c9 nchar(10)) tags(t1 int)"
        )

        i = 0
        ts = ts0
        halfNum = tbNum / 2
        while i < halfNum:
            tbId = i + int(halfNum)
            tb = tbPrefix + str(i)
            tb1 = tbPrefix + str(tbId)
            tdSql.execute(f"create table {tb} using {stb} tags( {i} )")
            tdSql.execute(f"create table {tb1} using {stb} tags( {tbId} )")

            x = 0
            while x < rowNum:
                xs = x * delta
                ts = ts0 + xs
                c = x % 10
                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"
                tdSql.execute(
                    f"insert into {tb} values ( {ts} , {c} , {c} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} )"
                )
                tdSql.execute(
                    f"insert into {tb1} values ( {ts} , {c} , NULL , {c} , NULL , {c} , {c} , true, {binary} , {nchar} )"
                )
                x = x + 1

            i = i + 1

        tdLog.info(f"====== tables created")

        tdSql.execute(f"use {db}")
        ##### select from table
        tdLog.info(f"====== select top/bot from table and check num of rows returned")
        tdSql.query(f"select top(c1, 100) from tb_stb0")
        tdSql.checkRows(100)

        tdSql.query(f"select bottom(c1, 100) from tb_stb0")
        tdSql.checkRows(100)

        tdSql.query(f"select _wstart, bottom(c3, 5) from tb_tb1 interval(1y);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 0.00000)
        tdSql.checkData(1, 1, 0.00000)
        tdSql.checkData(2, 1, 0.00000)
        tdSql.checkData(3, 1, 0.00000)

        tdSql.query(f"select _wstart, top(c4, 5) from tb_tb1 interval(1y);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 9.000000000)
        tdSql.checkData(1, 1, 9.000000000)
        tdSql.checkData(2, 1, 9.000000000)
        tdSql.checkData(3, 1, 9.000000000)

        tdSql.query(f"select _wstart, top(c3, 5) from tb_tb1 interval(40h)")
        tdSql.checkRows(25)
        tdSql.checkData(0, 1, 9.00000)

        tdSql.query(f"select last(*) from tb_tb9")
        tdSql.checkRows(1)

        tdSql.query(f"select last(c2) from tb_tb9")
        tdSql.checkRows(0)

        tdSql.query(f"select first(c2), last(c2) from tb_tb9")
        tdSql.checkRows(0)

        tdSql.execute(
            f"create table test(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double, col7 bool, col8 binary(20), col9 nchar(20)) tags(loc nchar(20));"
        )
        tdSql.execute(f"create table test1 using test tags('beijing');")
        tdSql.execute(
            f"insert into test1 values(1537146000000, 1, 1, 1, 1, 0.100000, 0.100000, 0, 'taosdata1', '涛思数据1');"
        )
        tdSql.execute(
            f"insert into test1 values(1537146000001, 2, 2, 2, 2, 1.100000, 1.100000, 1, 'taosdata2', '涛思数据2');"
        )
        tdSql.execute(
            f"insert into test1 values(1537146000002, 3, 3, 3, 3, 2.100000, 2.100000, 0, 'taosdata3', '涛思数据3');"
        )
        tdSql.execute(
            f"insert into test1 values(1537146000003, 4, 4, 4, 4, 3.100000, 3.100000, 1, 'taosdata4', '涛思数据4');"
        )
        tdSql.execute(
            f"insert into test1 values(1537146000004, 5, 5, 5, 5, 4.100000, 4.100000, 0, 'taosdata5', '涛思数据5');"
        )
        tdSql.execute(
            f"insert into test1 values(1537146000005, 6, 6, 6, 6, 5.100000, 5.100000, 1, 'taosdata6', '涛思数据6');"
        )
        tdSql.execute(
            f"insert into test1 values(1537146000006, 7, 7, 7, 7, 6.100000, 6.100000, 0, 'taosdata7', '涛思数据7');"
        )
        tdSql.execute(
            f"insert into test1 values(1537146000007, 8, 8, 8, 8, 7.100000, 7.100000, 1, 'taosdata8', '涛思数据8');"
        )
        tdSql.execute(
            f"insert into test1 values(1537146000008, 9, 9, 9, 9, 8.100000, 8.100000, 0, 'taosdata9', '涛思数据9');"
        )
        tdSql.execute(
            f"insert into test1 values(1537146000009, 10, 10, 10, 10, 9.100000, 9.100000, 1, 'taosdata10', '涛思数据10');"
        )
        tdSql.query(f"select ts, bottom(col5, 10) from test order by col5;")
        tdSql.checkRows(10)
        tdSql.checkData(0, 1, 0.10000)
        tdSql.checkData(1, 1, 1.10000)
        tdSql.checkData(2, 1, 2.10000)

        tdLog.info(f"=====================td-1302 case")
        tdSql.execute(f"create database t1 keep 36500")
        tdSql.execute(f"use t1;")
        tdSql.execute(f"create table test(ts timestamp, k int);")
        tdSql.execute(f"insert into test values(29999, 1)(70000, 2)(80000, 3)")

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")
        tdSql.connect("root")

        tdSql.query(
            f"select count(*) from t1.test where ts > 10000 and ts < 90000 interval(5000a)"
        )
        tdSql.checkRows(3)

        tdLog.info(f"==============>td-1308")
        tdSql.execute(f"create database db keep 36500")
        tdSql.execute(f"use db;")

        tdSql.execute(
            f"create table stb (ts timestamp, c1 int, c2 binary(10)) tags(t1 binary(10));"
        )
        tdSql.execute(f"create table tb1 using stb tags('a1');")

        tdSql.execute(f"insert into tb1 values('2020-09-03 15:30:48.812', 0, 'tb1');")
        tdSql.query(
            f"select count(*) from stb where ts > '2020-09-03 15:30:44' interval(4s);"
        )
        tdSql.checkRows(1)

        tdSql.execute(f"create table tb4 using stb tags('a4');")
        tdSql.query(
            f"select count(*) from stb where ts > '2020-09-03 15:30:44' interval(4s);"
        )
        tdSql.checkRows(1)

        tdLog.info(f"=======================>td-1446")
        tdSql.execute(f"create table t(ts timestamp, k int)")
        ts = 6000
        sql = "insert into t values"
        while ts < 7000:
            sql += f" ({ts},{ts})"
            ts = ts + 1
        tdSql.execute(sql)

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.connect("root")
        tdSql.execute(f"use db;")

        ts = 1000
        sql = "insert into t values"
        while ts < 5096:
            sql += f" ({ts},{ts})"
            ts = ts + 1
        tdSql.execute(sql)

        tdSql.query(f"select * from t where ts < 6500")
        tdSql.checkRows(4596)

        tdSql.query(f"select * from t where ts < 7000")
        tdSql.checkRows(5096)

        tdSql.query(f"select * from t where ts <= 6000")
        tdSql.checkRows(4097)

        tdSql.query(f"select * from t where ts <= 6001")
        tdSql.checkRows(4098)

        tdLog.info(f"======================>td-1454")
        tdSql.query(f"select count(*)/10, count(*)+99 from t")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 509.600000000)
        tdSql.checkData(0, 1, 5195.000000000)

        tdLog.info(f"=======================>td-1596")
        tdSql.execute(f"create table t2(ts timestamp, k int)")
        tdSql.execute(f"insert into t2 values('2020-1-2 1:1:1', 1);")
        tdSql.execute(f"insert into t2 values('2020-2-2 1:1:1', 1);")

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.execute(f"use db")
        tdSql.query(
            f"select _wstart, count(*), first(ts), last(ts) from t2 interval(1d);"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2020-01-02 00:00:00.000")
        tdSql.checkData(1, 0, "2020-02-02 00:00:00.000")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(0, 2, "2020-01-02 01:01:01.000")
        tdSql.checkData(1, 2, "2020-02-02 01:01:01.000")
        tdSql.checkData(0, 3, "2020-01-02 01:01:01.000")
        tdSql.checkData(1, 3, "2020-02-02 01:01:01.000")

        tdLog.info(f"===============================>td-3361")
        tdSql.execute(f"create table ttm1(ts timestamp, k int) tags(a nchar(12));")
        tdSql.execute(f"create table ttm1_t1 using ttm1 tags('abcdef')")
        tdSql.execute(f"insert into ttm1_t1 values(now, 1)")
        tdSql.query(f"select * from ttm1 where a=123456789012")
        tdSql.checkRows(0)

        tdLog.info(f"===============================>td-3621")
        tdSql.execute(f"create table ttm2(ts timestamp, k bool);")
        tdSql.execute(f"insert into ttm2 values('2021-1-1 1:1:1', true)")
        tdSql.execute(f"insert into ttm2 values('2021-1-1 1:1:2', NULL)")
        tdSql.execute(f"insert into ttm2 values('2021-1-1 1:1:3', false)")
        tdSql.query(f"select * from ttm2 where k is not null")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "2021-01-01 01:01:01.000")

        tdSql.query(f"select * from ttm2 where k is null")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2021-01-01 01:01:02.000")

        tdSql.query(f"select * from ttm2 where k=true")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2021-01-01 01:01:01.000")

        tdSql.query(f"select * from ttm2 where k=false")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2021-01-01 01:01:03.000")

        tdSql.query(f"select * from ttm2 where k<>false")
        tdSql.checkRows(1)

        tdSql.query(f"select * from ttm2 where k=null")
        tdSql.query(f"select * from ttm2 where k<>null")
        tdSql.error(f"select * from ttm2 where k like null")
        tdSql.query(f"select * from ttm2 where k<null")

    def Top(self):
        dbPrefix = "m_to_db"
        tbPrefix = "m_to_tb"
        mtPrefix = "m_to_mt"
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
            tdSql.execute(sql)
            i = i + 1

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select top(tbcol, 1) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 19)

        tdLog.info(f"=============== step3")
        cc = 4 * 60000
        ms = 1601481600000 + cc

        tdSql.query(f"select top(tbcol, 1) from {tb} where ts <= {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 4)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select top(tbcol, 1) as b from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 19)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select top(tbcol, 2) as b from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}  {tdSql.getData(1,0)}")
        tdSql.checkData(0, 0, 18)
        tdSql.checkData(1, 0, 19)

        tdLog.info(f"=============== step6")
        cc = 4 * 60000
        ms = 1601481600000 + cc

        tdSql.query(f"select top(tbcol, 2) as b from {tb} where ts <= {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}  {tdSql.getData(1,0)}")
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 4)

        tdSql.error(f"select top(tbcol, 122) as b from {tb}")

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    #
    # ------------------ test_top.py ------------------
    #
    def insert_data(self,column_dict,tbname,row_num):
        insert_sql = self.setsql.set_insertsql(column_dict,tbname,self.binary_str,self.nchar_str)
        for i in range(row_num):
            insert_list = []
            self.setsql.insert_values(column_dict,i,insert_sql,insert_list,self.ts)

    def top_check_data(self,tbname,tb_type):
        new_column_dict = {}
        for param in self.param_list:
            for k,v in self.column_dict.items():
                if v.lower() in ['tinyint','smallint','int','bigint','tinyint unsigned','smallint unsigned','int unsigned','bigint unsigned']:
                    tdSql.query(f'select top({k},{param}) from {tbname}')
                    if param >= self.rowNum:
                        if tb_type in ['normal_table','child_table']:
                            tdSql.checkRows(self.rowNum)
                            values_list = []
                            for i in range(self.rowNum):
                                tp = (self.rowNum-i-1,)
                                values_list.insert(0,tp)
                            tdSql.checkEqual(tdSql.queryResult,values_list)
                        elif tb_type == 'stable':
                            tdSql.checkRows(param)
                    elif param < self.rowNum:
                        if tb_type in ['normal_table','child_table']:
                            tdSql.checkRows(param)
                            values_list = []
                            for i in range(param):
                                tp = (self.rowNum-i-1,)
                                values_list.insert(0,tp)
                            tdSql.checkEqual(tdSql.queryResult,values_list)
                        elif tb_type == 'stable':
                            tdSql.checkRows(param)
                    for i in [self.param_list[0]-1,self.param_list[-1]+1]:
                        tdSql.error(f'select top({k},{i}) from {tbname}')
                    new_column_dict.update({k:v})
                elif v.lower() == 'bool' or 'binary' in v.lower() or 'nchar' in v.lower():
                    tdSql.error(f'select top({k},{param}) from {tbname}')
                tdSql.error(f'select * from {tbname} where top({k},{param})=1')
        for key in new_column_dict.keys():
            for k in self.column_dict.keys():
                if key == k :
                    continue
                else:
                    tdSql.query(f'select top({key},2),{k} from {tbname} group by tbname')
                    if tb_type == 'normal_table' or tb_type == 'child_table':
                        tdSql.checkRows(2)
                    else:
                        tdSql.checkRows(2*self.tbnum)

    def top_check_stb(self):
        
        tag_dict = {
            't0':'int'
        }
        tag_values = [
            f'1'
            ]
        tdSql.execute(f"create database if not exists {self.dbname} vgroups 2")
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(self.setsql.set_create_stable_sql(self.stbname,self.column_dict,tag_dict))

        for i in range(self.tbnum):
            tdSql.execute(f"create table {self.stbname}_{i} using {self.stbname} tags({tag_values[0]})")
            self.insert_data(self.column_dict,f'{self.stbname}_{i}',self.rowNum)
        tdSql.query(f'select * from information_schema.ins_tables where db_name = "{self.dbname}"')
        vgroup_list = []
        for i in range(len(tdSql.queryResult)):
            vgroup_list.append(tdSql.queryResult[i][6])
        vgroup_list_set = set(vgroup_list)
        for i in vgroup_list_set:
            vgroups_num = vgroup_list.count(i)
            if vgroups_num >= 2:
                tdLog.info(f'This scene with {vgroups_num} vgroups is ok!')
            else:
                tdLog.exit(
                    'This scene does not meet the requirements with {vgroups_num} vgroup!\n')
        for i in range(self.tbnum):
            self.top_check_data(f'{self.stbname}_{i}','child_table')
        self.top_check_data(self.stbname,'stable')
        tdSql.execute(f'drop database {self.dbname}')

    def top_check_ntb(self):
        tdSql.execute(f"create database if not exists {self.dbname}")
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(self.setsql.set_create_normaltable_sql(self.ntbname,self.column_dict))
        self.insert_data(self.column_dict,self.ntbname,self.rowNum)
        self.top_check_data(self.ntbname,'normal_table')
        tdSql.execute(f'drop database {self.dbname}')

    def do_top(self):
        # init
        self.setsql = TDSetSql()
        self.dbname = 'db'
        self.stbname = f'{self.dbname}.stb'
        self.ntbname = f'{self.dbname}.ntb'
        self.rowNum = 10
        self.tbnum = 20
        self.ts = 1537146000000
        self.binary_str = 'taosdata'
        self.nchar_str = '涛思数据'
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
            'col12': 'binary(20)',
            'col13': 'nchar(20)'
        }
        self.param_list = [1,100]

        # do
        self.top_check_ntb()
        self.top_check_stb()
        print("do_top ................................ [passed]\n")


    #
    # ------------------ main ------------------
    #
    def test_func_select_top(self):
        """ Fun: top()

        1. Sim case
        2. Query on all data types
        3. Input parameter with different values
        4. Query on stable/normal table
        5. Query on null data
        6. Query on where clause
        7. Query with filter
        8. Error check

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-26 Simon Guan Migrated from tsim/parser/topbot.sim
            - 2025-8-26 Simon Guan Migrated from tsim/compute/top.sim
            - 2025-9-25 Alex  Duan Migrated from uncatalog/system-test/2-query/test_top.py

        """
        self.do_sim_top()
        self.do_top()
        tdLog.success("%s successfully executed" % __file__)