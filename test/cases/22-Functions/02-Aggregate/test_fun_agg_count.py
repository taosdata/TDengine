import numpy as np
import os
import platform
import random

from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdDnodes
from new_test_framework.utils.sqlset import TDSetSql

class TestFunCount:
    # distribute case
    updatecfgDict = {"maxTablesPerVnode":2 ,"minTablesPerVnode":2,"tableIncStepPerVnode":2 }

    # public
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        cls.setsql = TDSetSql()
        cls.rowNum = 10
        cls.ts = 1537146000000
        dbname = "db"
        cls.ntbname = f'{dbname}.ntb'
        cls.stbname = f'{dbname}.stb'
        cls.column_dict = {
            'ts':'timestamp',
            'c1':'int',
            'c2':'float',
            'c3':'double',
            'c4':'timestamp'
        }
        cls.tag_dict = {
            't0':'int'
        }
        cls.tbnum = 2
        cls.tag_values = [
            f'10',
            f'100'
        ]

    #
    # ------------------ sim count ------------------
    #
    def do_sim_count(self):
        dbPrefix = "m_co_db"
        tbPrefix = "m_co_tb"
        mtPrefix = "m_co_mt"
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
            while x < rowNum:
                cc = x * 60000
                ms = 1601481600000 + cc
                tdSql.execute(f"insert into {tb} values ({ms} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select count(*) from {tb}")
        tdLog.info(f"===> select count(*) from {tb} => {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, rowNum)

        tdSql.query(f"select count(1) from {tb}")
        tdLog.info(f"===> select count(1) from {tb} => {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, rowNum)

        tdSql.query(f"select count(tbcol) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, rowNum)

        tdLog.info(f"=============== step3")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(f"select count(tbcol) from {tb} where ts <= {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 5)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select count(tbcol) as b from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, rowNum)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select count(tbcol) as b from {tb} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select count(tbcol) as b from {tb} interval(1d)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, rowNum)

        tdLog.info(f"=============== step6")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(f"select count(tbcol) as b from {tb} where ts <= {ms} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 1)
        tdSql.checkRows(5)

        tdLog.info(f"=============== step7")
        tdSql.query(f"select count(*) from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, totalNum)

        tdLog.info(f"=============== step8")
        tdSql.query(f"select count(1) from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, totalNum)

        tdSql.query(f"select count(tbcol) from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, totalNum)

        tdLog.info(f"=============== step10")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(f"select count(tbcol) as c from {mt} where ts <= {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 50)

        tdSql.query(f"select count(tbcol) as c from {mt} where tgcol < 5")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 100)

        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(
            f"select count(tbcol) as c from {mt} where tgcol < 5 and ts <= {ms}"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 25)

        tdLog.info(f"=============== step9")
        tdSql.query(f"select count(tbcol) as b from {mt} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(1, 0, 10)

        tdSql.query(f"select count(tbcol) as b from {mt} interval(1d)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 200)

        tdLog.info(f"=============== step10")
        tdSql.query(f"select count(tbcol) as b from {mt} group by tgcol")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, rowNum)

        tdSql.checkRows(tbNum)

        tdLog.info(f"=============== step11")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(
            f"select count(tbcol) as b from {mt}  where ts <= {ms} partition by tgcol interval(1m)"
        )
        tdSql.checkData(0, 0, 1)
        tdSql.checkRows(50)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        print("\n")
        print("do_sim_count .......................... [passed]")

    #
    # ------------------ test_count.py ------------------
    #

    def query_stb(self,k,stbname,tbnum,rownum):
        tdSql.query(f'select count({k}) from {stbname}')
        tdSql.checkEqual(tdSql.queryResult[0][0],tbnum*rownum)
        tdSql.query(f'select count({k}) from {stbname} where ts <={self.ts+self.rowNum-1}')
        tdSql.checkEqual(tdSql.queryResult[0][0],tbnum*rownum)
        tdSql.query(f'select count({k}) from {stbname} where ts <={self.ts+self.rowNum-2}')
        tdSql.checkEqual(tdSql.queryResult[0][0],tbnum*(rownum-1))
    def query_ctb(self,k,i,stbname,rownum):
        tdSql.query(f'select count({k}) from {stbname}_{i}')
        tdSql.checkEqual(tdSql.queryResult[0][0],rownum)
        tdSql.query(f'select count({k}) from {stbname}_{i} where ts <={self.ts+self.rowNum-1}')
        tdSql.checkEqual(tdSql.queryResult[0][0],rownum)
        tdSql.query(f'select count({k}) from {stbname}_{i} where ts <={self.ts+self.rowNum-2}')
        tdSql.checkEqual(tdSql.queryResult[0][0],rownum-1)
    def query_ntb(self,k,ntbname,rownum):
        tdSql.query(f'select count({k}) from {ntbname}')
        tdSql.checkEqual(tdSql.queryResult[0][0],rownum)
        tdSql.query(f'select count({k}) from {ntbname} where ts <={self.ts+self.rowNum-1}')
        tdSql.checkEqual(tdSql.queryResult[0][0],rownum)
        tdSql.query(f'select count({k}) from {ntbname} where ts <={self.ts+self.rowNum-2}')
        tdSql.checkEqual(tdSql.queryResult[0][0],rownum-1)
    def query_empty_stb(self):
        tdSql.query(f'select count(*) from (select distinct tbname from {self.stbname})')
        tdSql.checkEqual(tdSql.queryResult[0][0],self.tbnum)
        tdSql.query(f'select count(*) from {self.stbname}')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        rows = [2, 0]
        function_names = ['count', 'hyperloglog']
        for i in range(2):
            function_name = function_names[i]
            row = rows[i]
            tdSql.query(f'select {function_name}(tbname) from {self.stbname}')
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 0)
            tdSql.query(f'select {function_name}(c1) from {self.stbname}')
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 0)
            tdSql.query(f'select {function_name}(ts) from {self.stbname}')
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 0)
            tdSql.query(f'select {function_name}(1) from {self.stbname}')
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 0)
            tdSql.query(f'select {function_name}(c1),sum(c2),max(1) from {self.stbname}')
            tdSql.checkRows(1)
            tdSql.checkCols(3)
            tdSql.checkData(0, 0, 0)
            tdSql.checkData(0, 1, None)
            tdSql.checkData(0, 2, None)
            tdSql.query(f'select sum(1),{function_name}(1),max(c2) from {self.stbname}')
            tdSql.checkRows(1)
            tdSql.checkCols(3)
            tdSql.checkData(0, 0, None)
            tdSql.checkData(0, 1, 0)
            tdSql.checkData(0, 2, None)
            tdSql.query(f'select {function_name}(1),sum(1),max(c2),min(1),min(2),min(3),min(4),min(5),min(6),min(7),min(8) from {self.stbname}')
            tdSql.checkRows(1)
            tdSql.checkCols(11)
            tdSql.checkData(0, 0, 0)
            tdSql.checkData(0, 1, None)
            tdSql.checkData(0, 2, None)
            tdSql.checkData(0, 10, None)
            tdSql.query(f'select sum(1),max(c2),min(1),leastsquares(c1,1,1) from {self.stbname}')
            tdSql.checkRows(0)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.stbname} group by tbname')
            tdSql.checkRows(row)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.stbname} group by c1')
            tdSql.checkRows(0)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.stbname} group by t0')
            tdSql.checkRows(row)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.stbname} partition by tbname')
            tdSql.checkRows(row)
            tdSql.query(f'select t0, {function_name}(c1),sum(c1) from {self.stbname} partition by tbname')
            tdSql.checkRows(row)
            tdSql.query(f'select cast(t0 as binary(12)), {function_name}(c1),sum(c1) from {self.stbname} partition by tbname')
            tdSql.checkRows(row)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.stbname} partition by c1')
            tdSql.checkRows(0)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.stbname} partition by t0')
            tdSql.checkRows(row)
            tdSql.query(f'select {function_name}(1) from (select {function_name}(c1),sum(c1) from {self.stbname} group by c1)')
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 0)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.stbname} interval(1s)')
            tdSql.checkRows(0)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.stbname} partition by tbname interval(1s)')
            tdSql.checkRows(0)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.stbname} partition by c1 interval(1s)')
            tdSql.checkRows(0)
            tdSql.query(f'select {function_name}(1),sum(1) from (select {function_name}(1) from {self.stbname} group by tbname order by tbname)')
            tdSql.checkRows(1)
            if 'count' == function_name:
              tdSql.checkData(0, 0, 2)
              tdSql.checkData(0, 1, 2)
            elif 'hyperloglog' == function_name:
              tdSql.checkData(0, 0, 0)
              tdSql.checkData(0, 1, None)

    def query_empty_ntb(self):
        tdSql.query(f'select count(*) from {self.ntbname}')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        rows = [1, 0]
        function_names = ['count', 'hyperloglog']
        for i in range(2):
            function_name = function_names[i]
            row = rows[i]
            tdSql.query(f'select {function_name}(tbname) from {self.ntbname}')
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 0)
            tdSql.query(f'select {function_name}(c1) from {self.ntbname}')
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 0)
            tdSql.query(f'select {function_name}(ts) from {self.ntbname}')
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 0)
            tdSql.query(f'select {function_name}(1) from {self.ntbname}')
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 0)
            tdSql.query(f'select {function_name}(c1),sum(c2),max(1) from {self.ntbname}')
            tdSql.checkRows(1)
            tdSql.checkCols(3)
            tdSql.checkData(0, 0, 0)
            tdSql.checkData(0, 1, None)
            tdSql.checkData(0, 2, None)
            tdSql.query(f'select sum(1),{function_name}(1),max(c2) from {self.ntbname}')
            tdSql.checkRows(1)
            tdSql.checkCols(3)
            tdSql.checkData(0, 0, None)
            tdSql.checkData(0, 1, 0)
            tdSql.checkData(0, 2, None)
            tdSql.query(f'select {function_name}(1),sum(1),max(c2),min(1),min(2),min(3),min(4),min(5),min(6),min(7),min(8) from {self.ntbname}')
            tdSql.checkRows(1)
            tdSql.checkCols(11)
            tdSql.checkData(0, 0, 0)
            tdSql.checkData(0, 1, None)
            tdSql.checkData(0, 2, None)
            tdSql.checkData(0, 10, None)
            tdSql.query(f'select sum(1),max(c2),min(1),leastsquares(c1,1,1) from {self.ntbname}')
            tdSql.checkRows(0)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.ntbname} group by tbname')
            tdSql.checkRows(row)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.ntbname} group by c1')
            tdSql.checkRows(0)
            tdSql.query(f'select {function_name}(1) from (select {function_name}(c1),sum(c1) from {self.ntbname} group by c1)')
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 0)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.ntbname} interval(1s)')
            tdSql.checkRows(0)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.ntbname} partition by tbname interval(1s)')
            tdSql.checkRows(0)
            tdSql.query(f'select {function_name}(c1),sum(c1) from {self.ntbname} partition by c1 interval(1s)')
            tdSql.checkRows(0)
            tdSql.query(f'select count(1),sum(1) from (select count(1) from {self.ntbname} group by tbname order by tbname)')
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, 1)

    def count_query_stb(self,column_dict,tag_dict,stbname,tbnum,rownum):
        tdSql.query(f'select count(tbname) from {stbname}')
        tdSql.checkEqual(tdSql.queryResult[0][0],tbnum*rownum)
        tdSql.query(f'SELECT count(*) from (select distinct tbname from {stbname})')
        tdSql.checkEqual(tdSql.queryResult[0][0],tbnum)
        for k in column_dict.keys():
            self.query_stb(k,stbname,tbnum,rownum)
        for k in tag_dict.keys():
            self.query_stb(k,stbname,tbnum,rownum)
    def count_query_ctb(self,column_dict,tag_dict,stbname,tbnum,rownum):
        for i in range(tbnum):
            tdSql.query(f'select count(tbname) from {stbname}_{i}')
            tdSql.checkEqual(tdSql.queryResult[0][0],rownum)
            for k in column_dict.keys():
                self.query_ctb(k,i,stbname,rownum)
            for k in tag_dict.keys():
                self.query_ctb(k,i,stbname,rownum)
    def count_query_ntb(self,column_dict,ntbname,rownum):
        tdSql.query(f'select count(tbname) from {ntbname}')
        tdSql.checkEqual(tdSql.queryResult[0][0],rownum)
        for k in column_dict.keys():
            self.query_ntb(k,ntbname,rownum)
    def insert_data(self,column_dict,tbname,row_num):
        insert_sql = self.setsql.set_insertsql(column_dict,tbname)
        for i in range(row_num):
            insert_list = []
            self.setsql.insert_values(column_dict,i,insert_sql,insert_list,self.ts)
    def check_ntb(self):
        tdSql.prepare()
        tdSql.execute(self.setsql.set_create_normaltable_sql(self.ntbname,self.column_dict))
        self.query_empty_ntb()
        tdSql.execute('flush database db')
        tdSql.query(f'select count(tbname) from {self.ntbname}')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        self.insert_data(self.column_dict,self.ntbname,self.rowNum)
        self.count_query_ntb(self.column_dict,self.ntbname,self.rowNum)
        tdSql.execute('flush database db')
        self.count_query_ntb(self.column_dict,self.ntbname,self.rowNum)
        tdSql.execute('drop database db')
    def check_stb(self):
        tdSql.prepare()
        tdSql.execute(self.setsql.set_create_stable_sql(self.stbname,self.column_dict,self.tag_dict))
        for i in range(self.tbnum):
            tdSql.execute(f'create table {self.stbname}_{i} using {self.stbname} tags({self.tag_values[i]})')
        self.query_empty_stb()
        tdSql.execute('flush database db')
        tdSql.query(f'select count(tbname) from {self.stbname}')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        tdSql.query(f'SELECT count(*) from (select distinct tbname from {self.stbname})')
        tdSql.checkEqual(tdSql.queryResult[0][0],self.tbnum)
        for i in range(self.tbnum):
            self.insert_data(self.column_dict,f'{self.stbname}_{i}',self.rowNum)
        self.count_query_stb(self.column_dict,self.tag_dict,self.stbname,self.tbnum,self.rowNum)
        self.count_query_ctb(self.column_dict,self.tag_dict,self.stbname,self.tbnum,self.rowNum)
        tdSql.execute('flush database db')
        self.count_query_stb(self.column_dict,self.tag_dict,self.stbname,self.tbnum,self.rowNum)
        self.count_query_ctb(self.column_dict,self.tag_dict,self.stbname,self.tbnum,self.rowNum)
        tdSql.execute('drop database db')

    def check_count_with_sma_data(self):
        sql_file = os.path.join(os.path.dirname(__file__), "count_test.sql")
        
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        current_dir = os.path.dirname(__file__)
        csv_file_path = os.path.join(current_dir, "count_test.csv")
        if platform.system() == 'Windows':
            csv_file_path = csv_file_path.replace('\\', '\\\\')
        modified_content = sql_content.replace('CSV_PATH', csv_file_path)
        temp_sql_file = os.path.join(current_dir, "temp_count_test.sql")
        with open(temp_sql_file, 'w', encoding='utf-8') as f:
            f.write(modified_content)

        os.system(f'taos -f {temp_sql_file}')
        tdSql.query('select count(c_1) from d2.t2 where c_1 < 10', queryTimes=1)
        tdSql.checkData(0, 0, 0)
        tdSql.query('select count(c_1), min(c_1),tbname from d2.can partition by tbname order by 3', queryTimes=1)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, 't1')

        tdSql.checkData(1, 0, 15)
        tdSql.checkData(1, 1, 1471617148940980000)
        tdSql.checkData(1, 2, 't2')

        tdSql.checkData(2, 0, 0)
        tdSql.checkData(2, 1, None)
        tdSql.checkData(2, 2, 't3')

    def do_count(self):
        self.check_count_with_sma_data()
        self.check_stb()
        self.check_ntb()
        print("do_count .............................. [passed]")

    #
    # ------------------ test_count_interval.py ------------------
    #
    def restartTaosd(self, index=1, dbname="db"):
        tdDnodes.stop(index)
        tdDnodes.startWithoutSleep(index)
        tdSql.execute(f"use d")
        
    def do_count_interval(self):
        tdSql.execute("drop database if exists d")
        tdSql.execute("create database d")
        tdSql.execute("use d")
        tdSql.execute("create table st(ts timestamp, f int) tags (t int)")
        
        for i in range(-2048, 2047):
            ts = 1626624000000 + i
            tdSql.execute(f"insert into ct1 using st tags(1) values({ts}, {i})")
            
        tdSql.execute("flush database d")
        for i in range(1638):
            ts = 1648758398208 + i
            tdSql.execute(f"insert into ct1 using st tags(1) values({ts}, {i})")
        tdSql.execute("insert into ct1 using st tags(1) values(1648970742528, 1638)")
        tdSql.execute("flush database d")
        
        tdSql.query("select count(ts) from ct1 interval(17n, 5n)")           
        self.restartTaosd()
        tdSql.query("select count(ts) from ct1 interval(17n, 5n)")

        print("do_count_interval ..................... [passed]")


    #
    # ------------------ test_count_null.py ------------------
    #
    def check_results(self):
        tdSql.query(f"select count(*) from tb1")
        tdSql.checkData(0, 0, 20000)
        tdSql.query(f"select count(c1) from tb1")
        tdSql.checkData(0, 0, 0)
        tdSql.query(f"select count(c2) from tb1")
        tdSql.checkData(0, 0, 0)
        tdSql.query(f"select count(c3) from tb1")
        tdSql.checkData(0, 0, 0)
        tdSql.query(f"select count(c4) from tb1")
        tdSql.checkData(0, 0, 0)
        tdSql.query(f"select count(c5) from tb1")
        tdSql.checkData(0, 0, 0)
        tdSql.query(f"select count(c6) from tb1")
        tdSql.checkData(0, 0, 0)
        tdSql.query(f"select count(c7) from tb1")
        tdSql.checkData(0, 0, 0)
        tdSql.query(f"select count(c8) from tb1")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select count(*) from tb2")
        tdSql.checkData(0, 0, 20000)
        tdSql.query(f"select count(c1) from tb2")
        tdSql.checkData(0, 0, 20000)
        tdSql.query(f"select count(c2) from tb2")
        tdSql.checkData(0, 0, 20000)
        tdSql.query(f"select count(c3) from tb2")
        tdSql.checkData(0, 0, 20000)
        tdSql.query(f"select count(c4) from tb2")
        tdSql.checkData(0, 0, 20000)
        tdSql.query(f"select count(c5) from tb2")
        tdSql.checkData(0, 0, 20000)
        tdSql.query(f"select count(c6) from tb2")
        tdSql.checkData(0, 0, 20000)
        tdSql.query(f"select count(c7) from tb2")
        tdSql.checkData(0, 0, 0)
        tdSql.query(f"select count(c8) from tb2")
        tdSql.checkData(0, 0, 0)

        for i in range (3, 6):
            tdSql.query(f"select count(*) from tb{i}")
            tdSql.checkData(0, 0, 20000)
            tdSql.query(f"select count(c1) from tb{i}")
            tdSql.checkData(0, 0, 10000)
            tdSql.query(f"select count(c2) from tb{i}")
            tdSql.checkData(0, 0, 10000)
            tdSql.query(f"select count(c3) from tb{i}")
            tdSql.checkData(0, 0, 10000)
            tdSql.query(f"select count(c4) from tb{i}")
            tdSql.checkData(0, 0, 10000)
            tdSql.query(f"select count(c5) from tb{i}")
            tdSql.checkData(0, 0, 10000)
            tdSql.query(f"select count(c6) from tb{i}")
            tdSql.checkData(0, 0, 10000)
            tdSql.query(f"select count(c7) from tb{i}")
            tdSql.checkData(0, 0, 10000)
            tdSql.query(f"select count(c8) from tb{i}")
            tdSql.checkData(0, 0, 10000)

    def do_count_null(self):
        dbname = 'db'
        tbnames = ['tb1', 'tb2', 'tb3', 'tb4', 'tb5', 'tb6']
        num_rows = 20000
        num_tables = 6
        ts_base = 1685548800000

        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")

        for i in range (num_tables):
            tdSql.execute(
                f'''create table if not exists {dbname}.{tbnames[i]}
                (ts timestamp, c0 tinyint, c1 smallint, c2 int, c3 bigint, c4 double, c5 float, c6 bool, c7 varchar(10), c8 nchar(10))

                '''
            )


        tdLog.printNoPrefix("==========step2:insert data")

        for i in range(num_rows):
            tdSql.execute(f"insert into {dbname}.{tbnames[0]} values ({ts_base + i}, null, null, null, null, null, null, null, null, null)")

        for i in range(num_rows):
            tdSql.execute(f"insert into {dbname}.{tbnames[1]} values ({ts_base + i}, 1, 1, 1, 1, 1, 1, 1, null, null)")

        for i in range(num_rows):
            if i % 2 == 0:
                tdSql.execute(f"insert into {dbname}.{tbnames[2]} values ({ts_base + i}, null, null, null, null, null, null, null, null, null)")
            else:
                tdSql.execute(f"insert into {dbname}.{tbnames[2]} values ({ts_base + i}, 1, 1, 1, 1, 1, 1, 1, 'binary', 'nchar')")

        for i in range(num_rows):
            if i % 2 == 0:
                tdSql.execute(f"insert into {dbname}.{tbnames[3]} values ({ts_base + i}, null, null, null, null, null, null, null, 'binary', 'nchar')")
            else:
                tdSql.execute(f"insert into {dbname}.{tbnames[3]} values ({ts_base + i}, 1, 1, 1, 1, 1, 1, 1, null, null)")

        for i in range(num_rows):
            if i < num_rows / 2:
                tdSql.execute(f"insert into {dbname}.{tbnames[4]} values ({ts_base + i}, null, null, null, null, null, null, null, null, null)")
            else:
                tdSql.execute(f"insert into {dbname}.{tbnames[4]} values ({ts_base + i}, 1, 1, 1, 1, 1, 1, 1, 'binary', 'nchar')")

        for i in range(num_rows):
            if i >= num_rows / 2:
                tdSql.execute(f"insert into {dbname}.{tbnames[5]} values ({ts_base + i}, null, null, null, null, null, null, null, null, null)")
            else:
                tdSql.execute(f"insert into {dbname}.{tbnames[5]} values ({ts_base + i}, 1, 1, 1, 1, 1, 1, 1, 'binary', 'nchar')")


        tdLog.printNoPrefix("==========step3:check result in memory")
        self.check_results()

        tdLog.printNoPrefix("==========step3:check result from disk")
        tdSql.execute(f"flush database db")
        self.check_results()

        print("do_count_null ......................... [passed]")


    #
    # ------------------ test_count_partition.py ------------------
    #

    def init_count_partition(self):
        self.row_nums = 10
        self.tb_nums = 10
        self.ts = 1537146000000

    def prepare_datas(self, stb_name , tb_nums , row_nums, dbname="db" ):
        tdSql.execute(f" use {dbname} ")
        tdSql.execute(f" create stable {dbname}.{stb_name} (ts timestamp , c1 int , c2 bigint , c3 float , c4 double , c5 smallint , c6 tinyint , c7 bool , c8 binary(36) , c9 nchar(36) , uc1 int unsigned,\
            uc2 bigint unsigned ,uc3 smallint unsigned , uc4 tinyint unsigned ) tags(t1 timestamp , t2 int , t3 bigint , t4 float , t5 double , t6 smallint , t7 tinyint , t8 bool , t9 binary(36)\
                , t10 nchar(36) , t11 int unsigned , t12 bigint unsigned ,t13 smallint unsigned , t14 tinyint unsigned ) ")

        for i in range(tb_nums):
            tbname = f"{dbname}.sub_{stb_name}_{i}"
            ts = self.ts + i*10000
            tdSql.execute(f"create table {tbname} using {dbname}.{stb_name} tags ({ts} , {i} , %d , %f , %f , 1 , 2, 'true', 'binary_{i}' ,'nchar_{i}',{i},{i},10,20 )"%(i*10,i*1.0,i*1.0))

            for row in range(row_nums):
                ts = self.ts + row*1000
                tdSql.execute(f"insert into {tbname} values({ts} , {row} , {row} , {row} , {row} , 1 , 2 , 'true' , 'binary_{row}' , 'nchar_{row}' , {row} , {row} , 1 ,2 )")

            for null in range(5):
                ts =  self.ts + row_nums*1000 + null*1000
                tdSql.execute(f"insert into {tbname} values({ts} , NULL , NULL , NULL , NULL , NULL , NULL , NULL , NULL , NULL , NULL , NULL , NULL , NULL )")

    def basic_query(self, dbname="db"):
        tdSql.query(f"select count(*) from {dbname}.stb")
        tdSql.checkData(0,0,(self.row_nums + 5 )*self.tb_nums)
        tdSql.query(f"select count(c1) from {dbname}.stb")
        tdSql.checkData(0,0,(self.row_nums )*self.tb_nums)
        tdSql.query(f"select tbname , count(*) from {dbname}.stb partition by tbname ")
        tdSql.checkRows(self.tb_nums)
        tdSql.query(f"select count(c1) from {dbname}.stb group by t1 order by t1 ")
        tdSql.checkRows(self.tb_nums)
        tdSql.error(f"select count(c1) from {dbname}.stb group by c1 order by t1 ")
        tdSql.error(f"select count(t1) from {dbname}.stb group by c1 order by t1 ")
        tdSql.query(f"select count(c1) from {dbname}.stb group by tbname order by tbname ")
        tdSql.checkRows(self.tb_nums)
        # bug need fix
        # tdSql.query(f"select count(t1) from {dbname}.stb group by t2 order by t2 ")
        # tdSql.checkRows(self.tb_nums)
        tdSql.query(f"select count(c1) from {dbname}.stb group by c1 order by c1 ")
        tdSql.checkRows(self.row_nums+1)

        tdSql.query(f"select c1 , count(c1) from {dbname}.stb group by c1 order by c1 ")
        tdSql.checkRows(self.row_nums+1)

        tdSql.query(f"select count(c1) from {dbname}.stb group by abs(c1) order by abs(c1)")
        tdSql.checkRows(self.row_nums+1)
        tdSql.query(f"select abs(c1+c3), count(c1+c3) from {dbname}.stb group by abs(c1+c3) order by abs(c1+c3)")
        tdSql.checkRows(self.row_nums+1)
        tdSql.query(f"select count(c1+c3)+max(c2) ,abs(c1) from {dbname}.stb group by abs(c1) order by abs(c1)")
        tdSql.checkRows(self.row_nums+1)
        tdSql.error(f"select count(c1+c3)+max(c2) ,abs(c1) ,abs(t1) from {dbname}.stb group by abs(c1) order by abs(t1)+c2")
        tdSql.error(f"select count(c1+c3)+max(c2) ,abs(c1) from {dbname}.stb group by abs(c1) order by abs(c1)+c2")
        tdSql.query(f"select abs(c1+c3)+abs(c2) , count(c1+c3)+count(c2) from {dbname}.stb group by abs(c1+c3)+abs(c2) order by abs(c1+c3)+abs(c2)")
        tdSql.checkRows(self.row_nums+1)

        tdSql.query(f"select count(c1) , count(t2) from {dbname}.stb where abs(c1+t2)=1 partition by tbname")
        tdSql.checkRows(10)
        tdSql.query(f"select count(c1) from {dbname}.stb where abs(c1+t2)=1 partition by tbname")
        tdSql.checkRows(10)

        tdSql.query(f"select tbname , count(c1) from {dbname}.stb partition by tbname order by tbname")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkData(0,1,self.row_nums)

        tdSql.error(f"select tbname , count(c1) from {dbname}.stb partition by t1 order by t1")
        tdSql.error(f"select tbname , count(t1) from {dbname}.stb partition by t1 order by t1")
        tdSql.error(f"select tbname , count(t1) from {dbname}.stb partition by t2 order by t2")

        # # bug need fix
        # tdSql.query(f"select t2 , count(t1) from {dbname}.stb partition by t2 order by t2")
        # tdSql.checkRows(self.tb_nums)

        tdSql.query(f"select tbname , count(c1) from {dbname}.stb partition by tbname order by tbname")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkData(0,1,self.row_nums)


        tdSql.error(f"select tbname , count(c1) from {dbname}.stb partition by t2 order by t2")

        tdSql.query(f"select c2, count(c1) from {dbname}.stb partition by c2 order by c2 desc")
        tdSql.checkRows(self.tb_nums+1)
        tdSql.checkData(0,1,self.tb_nums)

        tdSql.error(f"select tbname , count(c1) from {dbname}.stb partition by c1 order by c2")


        tdSql.query(f"select tbname , abs(t2) from {dbname}.stb partition by c2 order by t2")
        tdSql.checkRows(self.tb_nums*(self.row_nums+5))

        tdSql.query(f"select count(c1) , count(t2) from {dbname}.stb partition by c2 ")
        tdSql.checkRows(self.row_nums+1)
        tdSql.checkData(0,1,self.row_nums)

        tdSql.query(f"select count(c1) , count(t2) ,c2 from {dbname}.stb partition by c2 order by c2")
        tdSql.checkRows(self.row_nums+1)

        tdSql.query(f"select count(c1) , count(t1) ,max(c2) ,tbname  from {dbname}.stb partition by tbname order by tbname")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkCols(4)

        tdSql.query(f"select count(c1) , count(t2) ,t1  from {dbname}.stb partition by t1 order by t1")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkData(0,0,self.row_nums)

        # bug need fix
        # tdSql.query(f"select count(c1) , count(t1) ,abs(c1) from {dbname}.stb partition by abs(c1) order by abs(c1)")
        # tdSql.checkRows(self.row_nums+1)


        tdSql.query(f"select count(ceil(c2)) , count(floor(t2)) ,count(floor(c2)) from {dbname}.stb partition by abs(c2) order by abs(c2)")
        tdSql.checkRows(self.row_nums+1)


        tdSql.query(f"select count(ceil(c1-2)) , count(floor(t2+1)) ,max(c2-c1) from {dbname}.stb partition by abs(floor(c1)) order by abs(floor(c1))")
        tdSql.checkRows(self.row_nums+1)


        # interval
        tdSql.query(f"select count(c1) from {dbname}.stb interval(2s) sliding(1s)")

        # bug need fix

        tdSql.query(f'select max(c1) from {dbname}.stb where ts>="2022-07-06 16:00:00.000 " and ts < "2022-07-06 17:00:00.000 " interval(50s) sliding(30s) fill(NULL)')

        tdSql.query(f"select tbname , count(c1) from {dbname}.stb partition by tbname interval(10s) slimit 5 soffset 1 ")

        tdSql.query(f"select tbname , count(c1) from {dbname}.stb partition by tbname interval(10s)")

        tdSql.query(f"select tbname , count(c1) from {dbname}.sub_stb_1 partition by tbname interval(10s)")
        tdSql.checkData(0,0,'sub_stb_1')
        tdSql.checkData(0,1,self.row_nums)

        # tdSql.query(f"select tbname , count(c1) from {dbname}.stb partition by tbname order by tbname slimit 5 soffset 0 ")
        # tdSql.checkRows(5)

        # tdSql.query(f"select tbname , count(c1) from {dbname}.stb partition by tbname order by tbname slimit 5 soffset 1 ")
        # tdSql.checkRows(5)

        tdSql.query(f"select tbname , count(c1) from {dbname}.sub_stb_1 partition by tbname interval(10s) sliding(5s) ")

        tdSql.query(f'select max(c1) from {dbname}.stb where ts>={self.ts} and ts < {self.ts}+10000 partition by tbname interval(50s) sliding(30s)')
        tdSql.query(f'select max(c1) from {dbname}.stb where ts>={self.ts} and ts < {self.ts}+10000 interval(50s) sliding(30s)')
        tdSql.query(f'select tbname , count(c1) from {dbname}.stb where ts>={self.ts} and ts < {self.ts}+10000 partition by tbname interval(50s) sliding(30s)')

    def do_count_partition(self):
        # init
        self.init_count_partition()

        # do
        tdSql.prepare()
        self.prepare_datas("stb",self.tb_nums,self.row_nums)
        self.basic_query()
        dbname="db"

        # # coverage case for taosd crash about bug fix
        tdSql.query(f"select sum(c1) from {dbname}.stb where t2+10 >1 ")
        tdSql.query(f"select count(c1),count(t1) from {dbname}.stb where -t2<1 ")
        tdSql.query(f"select tbname ,max(ceil(c1)) from {dbname}.stb group by tbname ")
        tdSql.query(f"select avg(abs(c1)) , tbname from {dbname}.stb group by tbname ")
        tdSql.query(f"select t1,c1 from {dbname}.stb where abs(t2+c1)=1 ")

        print("do_count_partition .................... [passed]")

    #
    # ------------------ test_distribute_agg_count.py ------------------
    #

    def check_count_functions(self, tbname , col_name):

        max_sql = f"select count({col_name}) from {tbname};"

        same_sql = f"select sum(c) from (select {col_name} ,1 as c  from {tbname} where {col_name} is not null) "

        tdSql.query(max_sql)
        max_result = tdSql.queryResult

        tdSql.query(same_sql)
        same_result = tdSql.queryResult

        if max_result !=same_result:
            tdLog.exit(" count function work not as expected, sql : %s "% max_sql)
        else:
            tdLog.info(" count function work as expected, sql : %s "% max_sql)

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

    def check_count_distribute_diff_vnode(self,col_name, dbname="testdb"):

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

        max_sql = f"select count({col_name}) from {dbname}.stb1 where tbname in ({tbname_filters});"

        same_sql = f"select sum(c) from (select {col_name} ,1 as c  from {dbname}.stb1 where tbname in ({tbname_filters}) and {col_name} is not null) "

        tdSql.query(max_sql)
        max_result = tdSql.queryResult

        tdSql.query(same_sql)
        same_result = tdSql.queryResult

        if max_result !=same_result:
            tdLog.exit(" count function work not as expected, sql : %s "% max_sql)
        else:
            tdLog.info(" count function work as expected, sql : %s "% max_sql)

    def check_count_status(self, dbname="testdb"):
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
                self.check_count_functions(tablename,colname)

        # check max function for different vnode

        for colname in colnames:
            if colname.startswith("c"):
                self.check_count_distribute_diff_vnode(colname, dbname)
            else:
                # self.check_count_distribute_diff_vnode(colname, dbname) # bug for tag
                pass

    def distribute_agg_query(self, dbname="testdb"):
        # basic filter
        tdSql.query(f"select count(c1) from {dbname}.stb1 ")
        tdSql.checkData(0,0,184)

        tdSql.query(f"select count(c1) from {dbname}.stb1 where t1=1")
        tdSql.checkData(0,0,9)

        tdSql.query(f"select count(c1+c2) from {dbname}.stb1 where c1 =1 ")
        tdSql.checkData(0,0,2)

        tdSql.query(f"select count(c1) from {dbname}.stb1 where tbname=\"ct2\"")
        tdSql.checkData(0,0,9)

        tdSql.query(f"select count(c1) from {dbname}.stb1 partition by tbname")
        tdSql.checkRows(20)

        tdSql.query(f"select count(c1) from {dbname}.stb1 where t1> 4  partition by tbname")
        tdSql.checkRows(15)

        # union all
        tdSql.query(f"select count(c1) from {dbname}.stb1 union all select count(c1) from {dbname}.stb1 ")
        tdSql.checkRows(2)
        tdSql.checkData(0,0,184)

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

        tdSql.query(f"select count(tb1.c1), count(tb2.c2) from db.tb1 tb1, db.tb2 tb2 where tb1.ts=tb2.ts")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,10)
        tdSql.checkData(0,1,10)

        # group by
        tdSql.execute(f" use {dbname} ")

        tdSql.query(f"select count(*)  from {dbname}.stb1 ")
        tdSql.checkData(0,0,187)
        tdSql.query(f"select count(*)  from {dbname}.stb1 group by t1 ")
        tdSql.checkRows(20)
        tdSql.query(f"select count(*)  from {dbname}.stb1 group by c1 ")
        tdSql.checkRows(30)
        tdSql.query(f"select count(*)  from {dbname}.stb1 group by c2 ")
        tdSql.checkRows(31)

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
        tdSql.query(f"select abs(c2+2)+1 from (select count(c1) c2  from {dbname}.stb1)")
        tdSql.checkData(0,0,187.000000000)
        tdSql.query(f"select count(c1+2)  as c2 from (select ts ,c1 ,c2  from {dbname}.stb1)")
        tdSql.checkData(0,0,184)
        tdSql.query(f"select count(a+2)  as c2 from (select ts ,abs(c1) a ,c2  from {dbname}.stb1)")
        tdSql.checkData(0,0,184)

        # mixup with other functions
        tdSql.query(f"select max(c1),count(c1),last(c2,c3) from {dbname}.stb1")
        tdSql.checkData(0,0,28)
        tdSql.checkData(0,1,184)
        tdSql.checkData(0,2,-99999)
        tdSql.checkData(0,3,-999)

    def do_count_distribute(self):
        # init
        self.vnode_disbutes = None
        self.ts = 1537146000000

        self.prepare_datas_of_distribute()
        self.check_distribute_datas()
        self.check_count_status()
        self.distribute_agg_query()

    #
    # ------------------ main ------------------
    #
    def test_func_agg_count(self):
        """ Fun: count()

        1. Sim case including time windows, filtering on ordinary data columns, filtering on tag columns, GROUP BY, and PARTITION BY.
        2. Basic query
        3. Error check
        4. Query on stable/normal table
        5. Query with interval clause
        6. Query after restart taosd
        7. Query on null data
        8. Query on partition by clause
        9. Query on distributed 

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/compute/count.sim
            - 2025-9-24 Alex  Duan Migrated from uncatalog/system-test/2-query/test_count.py
            - 2025-9-24 Alex  Duan Migrated from uncatalog/system-test/2-query/test_count_interval.py
            - 2025-9-24 Alex  Duan Migrated from uncatalog/system-test/2-query/test_count_null.py
            - 2025-9-24 Alex  Duan Migrated from uncatalog/system-test/2-query/test_count_partition.py
            - 2025-9-24 Alex  Duan Migrated from uncatalog/system-test/2-query/test_distribute_agg_count.py

        """
        self.do_sim_count()
        self.do_count()
        self.do_count_null()
        self.do_count_partition()
        self.do_count_distribute()
        self.do_count_interval()        