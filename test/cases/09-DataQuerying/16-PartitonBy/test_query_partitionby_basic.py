import time
import socket
import os
import threading
from typing import List
from datetime import datetime
from new_test_framework.utils import tdLog, tdSql, tdCom


COMPARE_DATA = 0
COMPARE_LEN = 1

class TestPartitionByBasic:
    #
    # ------------------- col  ----------------
    # 
    def setup_class(cls):
        cls.vgroups    = 4
        cls.ctbNum     = 10
        cls.rowsPerTbl = 10000
        cls.duraion = '1d'

    def create_database(self,tsql, dbName,dropFlag=1,vgroups=2,replica=1, duration:str='1d'):
        if dropFlag == 1:
            tsql.execute("drop database if exists %s"%(dbName))

        tsql.execute("create database if not exists %s vgroups %d replica %d duration %s stt_trigger 1"%(dbName, vgroups, replica, duration))
        tdLog.debug("complete to create database %s"%(dbName))
        return

    def create_stable(self,tsql, paraDict):
        colString = tdCom.gen_column_type_str(colname_prefix=paraDict["colPrefix"], column_elm_list=paraDict["colSchema"])
        tagString = tdCom.gen_tag_type_str(tagname_prefix=paraDict["tagPrefix"], tag_elm_list=paraDict["tagSchema"])
        sqlString = f"create table if not exists %s.%s (%s) tags (%s)"%(paraDict["dbName"], paraDict["stbName"], colString, tagString)
        tdLog.debug("%s"%(sqlString))
        tsql.execute(sqlString)
        return

    def create_ctable(self,tsql=None, dbName='dbx',stbName='stb',ctbPrefix='ctb',ctbNum=1,ctbStartIdx=0):
        for i in range(ctbNum):
            sqlString = "create table %s.%s%d using %s.%s tags(%d, 'tb%d', 'tb%d', %d, %d, %d)" % \
                    (dbName,ctbPrefix,i+ctbStartIdx,dbName,stbName,(i+ctbStartIdx) % 5,i+ctbStartIdx,i+ctbStartIdx,i+ctbStartIdx,i+ctbStartIdx,i+ctbStartIdx)
            tsql.execute(sqlString)

        tdLog.debug("complete to create %d child tables by %s.%s" %(ctbNum, dbName, stbName))
        return

    def insert_data(self,tsql,dbName,ctbPrefix,ctbNum,rowsPerTbl,batchNum,startTs,tsStep):
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        for i in range(ctbNum):
            rowsBatched = 0
            sql += " %s%d values "%(ctbPrefix,i)
            for j in range(rowsPerTbl):
                if (i < ctbNum/2):
                    sql += "(%d, %d, %d, %d,%d,%d,%d,true,'binary%d', 'nchar%d') "%(startTs + j*tsStep, j%10, j%10, j%10, j%10, j%10, j%10, j%10, j%10)
                else:
                    sql += "(%d, %d, NULL, %d,NULL,%d,%d,true,'binary%d', 'nchar%d') "%(startTs + j*tsStep, j%10, j%10, j%10, j%10, j%10, j%10)
                rowsBatched += 1
                if ((rowsBatched == batchNum) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
                    rowsBatched = 0
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s%d values " %(ctbPrefix,i)
                    else:
                        sql = "insert into "
        if sql != pre_insert:
            tsql.execute(sql)

    def prepareTestEnv(self):
        tdLog.printNoPrefix("======== prepare test env include database, stable, ctables, and insert data: ")
        paraDict = {'dbName':     'test',
                    'dropFlag':   1,
                    'vgroups':    2,
                    'stbName':    'meters',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},
                                    {'type': 'BIGINT', 'count':1},
                                    {'type': 'FLOAT', 'count':1},
                                    {'type': 'DOUBLE', 'count':1},
                                    {'type': 'smallint', 'count':1},
                                    {'type': 'tinyint', 'count':1},
                                    {'type': 'bool', 'count':1},
                                    {'type': 'binary', 'len':10, 'count':1},
                                    {'type': 'nchar', 'len':10, 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'nchar', 'len':20, 'count':1},{'type': 'binary', 'len':20, 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'smallint', 'count':1},{'type': 'DOUBLE', 'count':1}],
                    'ctbPrefix':  't',
                    'ctbStartIdx': 0,
                    'ctbNum':     100,
                    'rowsPerTbl': 10000,
                    'batchNum':   3000,
                    'startTs':    1537146000000,
                    'tsStep':     600000}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        tdLog.info("create database")
        self.create_database(tsql=tdSql, dbName=paraDict["dbName"], dropFlag=paraDict["dropFlag"], vgroups=paraDict["vgroups"], replica=self.replicaVar, duration=self.duraion)

        tdLog.info("create stb")
        self.create_stable(tsql=tdSql, paraDict=paraDict)

        tdLog.info("create child tables")
        self.create_ctable(tsql=tdSql, dbName=paraDict["dbName"], \
                stbName=paraDict["stbName"],ctbPrefix=paraDict["ctbPrefix"],\
                ctbNum=paraDict["ctbNum"],ctbStartIdx=paraDict["ctbStartIdx"])
        self.insert_data(tsql=tdSql, dbName=paraDict["dbName"],\
                ctbPrefix=paraDict["ctbPrefix"],ctbNum=paraDict["ctbNum"],\
                rowsPerTbl=paraDict["rowsPerTbl"],batchNum=paraDict["batchNum"],\
                startTs=paraDict["startTs"],tsStep=paraDict["tsStep"])
        return

    def check_explain_res_has_row(self, plan_str_expect: str, rows):
        plan_found = False
        for row in rows:
            if str(row).find(plan_str_expect) >= 0:
                tdLog.debug("plan: [%s] found in: [%s]" % (plan_str_expect, str(row)))
                plan_found = True
                break
        if not plan_found:
            tdLog.exit("plan: %s not found in res: [%s]" % (plan_str_expect, str(rows)))

    def check_sort_for_partition_hint(self):
        sql = 'select count(*), c1 from meters partition by c1'
        sql_hint = 'select /*+ sort_for_group() */count(*), c1 from meters partition by c1'
        #self.check_explain_res_has_row("Partition on", self.explain_sql(sql))
        self.check_explain_res_has_row("Sort", self.explain_sql(sql_hint))

        sql = 'select count(*), c1, tbname from meters partition by tbname, c1'
        sql_hint = 'select /*+ sort_for_group() */ count(*), c1, tbname from meters partition by tbname, c1'
        #self.check_explain_res_has_row("Partition on", self.explain_sql(sql))
        self.check_explain_res_has_row("Sort", self.explain_sql(sql_hint))

        sql = 'select count(*), c1, tbname from meters partition by tbname, c1 interval(1s)'
        sql_hint = 'select /*+ sort_for_group() */ count(*), c1, tbname from meters partition by tbname, c1 interval(1s)'
        self.check_explain_res_has_row("Partition on", self.explain_sql(sql))
        self.check_explain_res_has_row("Sort", self.explain_sql(sql_hint))

        sql = 'select count(*), c1, t1 from meters partition by t1, c1'
        sql_hint = 'select /*+ sort_for_group() */ count(*), c1, t1 from meters partition by t1, c1'
        #self.check_explain_res_has_row("Partition on", self.explain_sql(sql))
        self.check_explain_res_has_row("Sort", self.explain_sql(sql_hint))

        sql = 'select count(*), c1, t1 from meters partition by t1, c1 interval(1s)'
        sql_hint = 'select /*+ sort_for_group() */ count(*), c1, t1 from meters partition by t1, c1 interval(1s)'
        self.check_explain_res_has_row("Partition on", self.explain_sql(sql))
        self.check_explain_res_has_row("Sort", self.explain_sql(sql_hint))

        sql = 'select count(*), c1 from meters partition by c1 interval(1s)'
        sql_hint = 'select /*+ sort_for_group() */ count(*), c1 from meters partition by c1 interval(1s)'
        self.check_explain_res_has_row("Partition on", self.explain_sql(sql))
        self.check_explain_res_has_row("Sort", self.explain_sql(sql_hint))

        sql = 'select count(*), c1 from meters partition by c1'
        sql_hint = 'select /*+ sort_for_group() partition_first()*/ count(*), c1 from meters partition by c1'
        self.check_explain_res_has_row("Sort", self.explain_sql(sql_hint))
        sql_hint = 'select /*+ partition_first()*/ count(*), c1 from meters partition by c1'
        self.check_explain_res_has_row("Partition on", self.explain_sql(sql_hint))
        sql_hint = 'select /*+ partition_first() sort_for_group()*/ count(*), c1 from meters partition by c1'
        self.check_explain_res_has_row("Partition on", self.explain_sql(sql_hint))
        sql_hint = 'select /*+ sort_for_group() partition_first()*/ count(*), c1 from meters partition by c1'
        self.check_explain_res_has_row("Sort", self.explain_sql(sql_hint))

    def add_order_by(self, sql: str, order_by: str, select_list: str = "*") -> str:
        return "select %s from (%s)t order by %s" % (select_list, sql, order_by)

    def add_hint(self, sql: str) -> str:
        return "select /*+ sort_for_group() */ %s" % sql[6:]

    def query_with_time(self, sql):
        start = datetime.now()
        tdSql.query(sql, queryTimes=1)
        return (datetime.now().timestamp() - start.timestamp()) * 1000

    def explain_sql(self, sql: str):
        sql = "explain " + sql
        tdSql.query(sql, queryTimes=1)
        return tdSql.queryResult

    def query_and_compare_res(self, sql1, sql2, compare_what: int = 0):
        dur = self.query_with_time(sql1)
        tdLog.debug("sql1 query with time: [%f]" % dur)
        res1 = tdSql.queryResult
        dur = self.query_with_time(sql2)
        tdLog.debug("sql2 query with time: [%f]" % dur)
        res2 = tdSql.queryResult
        if res1 is None or res2 is None:
            tdLog.exit("res1 or res2 is None")
        if compare_what <= COMPARE_LEN:
            if len(res1) != len(res2):
                tdLog.exit("query and copare failed cause different rows, sql1: [%s], rows: [%d], sql2: [%s], rows: [%d]" % (sql1, len(res1), sql2, len(res2)))
        if compare_what == COMPARE_DATA:
            for i in range(0, len(res1)):
                if res1[i] != res2[i]:
                    tdLog.exit("compare failed for row: [%d], sqls: [%s] res1: [%s], sql2 : [%s] res2: [%s]" % (i, sql1, res1[i], sql2, res2[i]))
        tdLog.debug("sql: [%s] and sql: [%s] have same results, rows: [%d]" % (sql1, sql2, len(res1)))

    def prepare_and_query_and_compare(self, sqls: List[str], order_by: str, select_list: str = "*", compare_what: int = 0):
        for sql in sqls:
            sql_hint = self.add_order_by(self.add_hint(sql), order_by, select_list)
            sql = self.add_order_by(sql, order_by, select_list)
            self.check_explain_res_has_row("Sort", self.explain_sql(sql_hint))
            #self.check_explain_res_has_row("Partition", self.explain_sql(sql))
            self.query_and_compare_res(sql, sql_hint, compare_what=compare_what)

    def check_sort_for_partition_res(self):
        sqls_par_c1_agg = [
                "select count(*), c1 from meters partition by c1",
                "select count(*), min(c2), max(c3), c1 from meters partition by c1",
                "select c1 from meters partition by c1",
                ]
        sqls_par_c1 = [
                "select * from meters partition by c1"
                ]
        sqls_par_c1_c2_agg = [
                "select count(*), c1, c2 from meters partition by c1, c2",
                "select c1, c2 from meters partition by c1, c2",
                "select count(*), c1, c2, min(c4), max(c5), sum(c6) from meters partition by c1, c2",
                ]
        sqls_par_c1_c2 = [
                "select * from meters partition by c1, c2"
                ]

        sqls_par_tbname_c1 = [
                "select count(*), c1 , tbname as a from meters partition by tbname, c1"
                ]
        sqls_par_tag_c1 = [
                "select count(*), c1, t1 from meters partition by t1, c1"
                ]
        self.prepare_and_query_and_compare(sqls_par_c1_agg, "c1")
        self.prepare_and_query_and_compare(sqls_par_c1, "c1, ts, c2", "c1, ts, c2")
        self.prepare_and_query_and_compare(sqls_par_c1_c2_agg, "c1, c2")
        self.prepare_and_query_and_compare(sqls_par_c1_c2, "c1, c2, ts, c3", "c1, c2, ts, c3")
        self.prepare_and_query_and_compare(sqls_par_tbname_c1, "a, c1")
        self.prepare_and_query_and_compare(sqls_par_tag_c1, "t1, c1")

    def get_interval_template_sqls(self, col_name):
        sqls = [
                'select _wstart as ts, count(*), tbname as a, %s from meters partition by tbname, %s interval(1s)' %  (col_name, col_name),
                #'select _wstart as ts, count(*), tbname as a, %s from meters partition by tbname, %s interval(30s)' % (col_name, col_name),
                #'select _wstart as ts, count(*), tbname as a, %s from meters partition by tbname, %s interval(1m)' %  (col_name, col_name),
                #'select _wstart as ts, count(*), tbname as a, %s from meters partition by tbname, %s interval(30m)' % (col_name, col_name),
                #'select _wstart as ts, count(*), tbname as a, %s from meters partition by tbname, %s interval(1h)' %  (col_name, col_name),

                'select _wstart as ts, count(*), t1 as a, %s from meters partition by t1, %s interval(1s)' %  (col_name, col_name),
                #'select _wstart as ts, count(*), t1 as a, %s from meters partition by t1, %s interval(30s)' % (col_name, col_name),
                #'select _wstart as ts, count(*), t1 as a, %s from meters partition by t1, %s interval(1m)' %  (col_name, col_name),
                #'select _wstart as ts, count(*), t1 as a, %s from meters partition by t1, %s interval(30m)' % (col_name, col_name),
                #'select _wstart as ts, count(*), t1 as a, %s from meters partition by t1, %s interval(1h)' %  (col_name, col_name),

                'select _wstart as ts, count(*), %s as a, %s from meters partition by %s interval(30d)' %  (col_name, col_name, col_name),
                #'select _wstart as ts, count(*), %s as a, %s from meters partition by %s interval(30s)' % (col_name, col_name, col_name),
                #'select _wstart as ts, count(*), %s as a, %s from meters partition by %s interval(1m)' %  (col_name, col_name, col_name),
                #'select _wstart as ts, count(*), %s as a, %s from meters partition by %s interval(30m)' % (col_name, col_name, col_name),
                'select _wstart as ts, count(*), %s as a, %s from meters partition by %s interval(1h)' %  (col_name, col_name, col_name),

                'select _wstart as ts, count(*), tbname as a, %s from meters partition by %s, tbname interval(1s)' %  (col_name, col_name),
                'select _wstart as ts, count(*), t1 as a, %s from meters partition by %s, t1 interval(1s)' %  (col_name, col_name),
                ]
        order_list = 'a, %s, ts' % (col_name)
        return (sqls, order_list)

    def check_sort_for_partition_interval(self):
        sqls, order_list = self.get_interval_template_sqls('c1')
        self.prepare_and_query_and_compare(sqls, order_list)
        #sqls, order_list = self.get_interval_template_sqls('c2')
        #self.prepare_and_query(sqls, order_list)
        sqls, order_list = self.get_interval_template_sqls('c3')
        self.prepare_and_query_and_compare(sqls, order_list)
        #sqls, order_list = self.get_interval_template_sqls('c4')
        #self.prepare_and_query(sqls, order_list)
        #sqls, order_list = self.get_interval_template_sqls('c5')
        #self.prepare_and_query(sqls, order_list)
        sqls, order_list = self.get_interval_template_sqls('c6')
        self.prepare_and_query_and_compare(sqls, order_list)
        #sqls, order_list = self.get_interval_template_sqls('c7')
        #self.prepare_and_query(sqls, order_list)
        sqls, order_list = self.get_interval_template_sqls('c8')
        self.prepare_and_query_and_compare(sqls, order_list)
        sqls, order_list = self.get_interval_template_sqls('c9')
        self.prepare_and_query_and_compare(sqls, order_list)

    def check_sort_for_partition_no_agg_limit(self):
        sqls_template = [
                'select * from meters partition by c1 slimit %d limit %d',
                'select * from meters partition by c2 slimit %d limit %d',
                'select * from meters partition by c8 slimit %d limit %d',
                ]
        sqls = []
        for sql in sqls_template:
            sqls.append(sql % (1,1))
            sqls.append(sql % (1,10))
            sqls.append(sql % (10,10))
            sqls.append(sql % (100, 100))
        order_by_list = 'ts,c1,c2,c3,c4,c5,c6,c7,c8,c9,t1,t2,t3,t4,t5,t6'

        self.prepare_and_query_and_compare(sqls, order_by_list, compare_what=COMPARE_LEN)
    
    def check_tsdb_read(self):
        tdSql.execute('delete from t0')
        tdSql.execute('flush database test')
        for i in range(0, 4096):
            tdSql.execute(f"insert into test.t0 values({1537146000000 + i}, 1,1,1,1,1,1,1,'a','1')")
        tdSql.execute("flush database test")

        tdSql.execute(f"insert into t0 values({1537146000000 + 4095}, 1,1,1,1,1,1,1,'a','1')")
        for i in range(4095, 4096*2 + 100):
            tdSql.execute(f"insert into test.t0 values({1537146000000 + i}, 1,1,1,1,1,1,1,'a','1')")
        tdSql.execute("flush database test")
        time.sleep(5)
        tdSql.query('select first(ts), last(ts) from t0', queryTimes=1)
        tdSql.checkRows(1)

    def do_partition_by_col(self):
        self.prepareTestEnv()
        tdSql.execute('flush database test')
        #time.sleep(99999999)
        self.check_sort_for_partition_hint()
        self.check_sort_for_partition_res()
        self.check_sort_for_partition_interval()
        self.check_sort_for_partition_no_agg_limit()
        self.check_tsdb_read()

        print("\ndo do_partition_by_col ................ [passed]")

    #
    # ------------------- second ----------------
    # 
    def init_data(self):        
        self.vgroups = 4
        self.ctbNum = 10
        self.rowsPerTbl = 10000
        self.duraion = '1h'

    def check_explain_res_no_row(self, plan_str_not_expect: str, res):
        for row in res:
            if str(row).find(plan_str_not_expect) >= 0:
                tdLog.exit('plan: [%s] found in: [%s]' % (plan_str_not_expect, str(row)))
    
    def add_remove_partition_hint(self, sql: str) -> str:
        return "select /*+ remove_partition() */ %s" % sql[6:]

    def explain_sql_verbose(self, sql: str):
        sql = "explain verbose true " + sql
        tdSql.query(sql, queryTimes=1)
        return tdSql.queryResult

    def query_and_compare_first_rows(self, sql1, sql2):
        dur = self.query_with_time(sql1)
        tdLog.debug("sql1 query with time: [%f]" % dur)
        res1 = tdSql.queryResult
        dur = self.query_with_time(sql2)
        tdLog.debug("sql2 query with time: [%f]" % dur)
        res2 = tdSql.queryResult
        if res1 is None or res2 is None:
            tdLog.exit("res1 or res2 is None")
        for i in range(0, min(len(res1), len(res2))):
            if res1[i] != res2[i]:
                tdLog.exit("compare failed for row: [%d], sqls: [%s] res1: [%s], sql2 : [%s] res2: [%s]" % (i, sql1, res1[i], sql2, res2[i]))
        tdLog.debug("sql: [%s] and sql: [%s] have same results, rows: [%d]" % (sql1, sql2, min(len(res1), len(res2))))

    def check_explain(self, sql):
        sql_hint = self.add_hint(sql)
        explain_res = self.explain_sql_verbose(sql)
        #self.check_explain_res_has_row('SortMerge', explain_res)
        #self.check_explain_res_has_row("blocking=0", explain_res)
        explain_res = self.explain_sql_verbose(sql_hint)
        self.check_explain_res_has_row('Merge', explain_res)
        self.check_explain_res_has_row('blocking=0', explain_res)

    def check_pipelined_agg_plan_with_slimit(self):
        sql = 'select count(*), %s from meters partition by %s slimit 1'
        self.check_explain(sql % ('c1','c1'))
        self.check_explain(sql % ('c1,c2', 'c1,c2'))

        # should fail
        # self.check_explain(sql % ('t1', 'c1,t1'))
        # self.check_explain(sql % ('t1', 'c1,tbname'))

    def check_pipelined_agg_data_with_slimit(self):
        sql_template = 'select %s from meters partition by %s %s'

        sql_elems = [
                ['count(*), min(c1), c2', 'c2', 'slimit 10', 'c2'],
                ['count(*), c1, c2', 'c1, c2', 'slimit 100', 'c1,c2'],
                ['count(*), c2, c1', 'c1, c2', 'slimit 1000', 'c1,c2'],
                ['count(*), c4,c3', 'c3, c4', 'slimit 2000', 'c3,c4'],
                ['count(*), c8,c6', 'c8, c6', 'slimit 3000', 'c8,c6'],
                ['count(*), c1 +1 as a,c6', 'c1, c6', 'slimit 3000', 'a,c6'],
                ['count(*), c1 +1 as a,c6', 'c1+1, c6', 'slimit 3000', 'a, c6'],
                ]

        for ele in sql_elems:
            sql = sql_template % (ele[0], ele[1], ele[2])
            sql_hint = self.add_hint(sql)
            sql = self.add_order_by(sql, ele[3])
            sql_no_slimit = sql_template % (ele[0], ele[1], '')

            sql_no_slimit = self.add_order_by(sql_no_slimit, ele[3])
            self.query_and_compare_first_rows(sql_hint, sql_no_slimit)

    def check_remove_partition(self):
        sql = 'select c1, count(*) from meters partition by c1 slimit 10'
        explain_res = self.explain_sql_verbose(sql)
        self.check_explain_res_no_row("Partition", explain_res)
        self.check_explain_res_has_row("blocking=1", explain_res)

        sql = 'select c1, count(*) from meters partition by c1,c2 slimit 10'
        explain_res = self.explain_sql_verbose(sql)
        self.check_explain_res_no_row("Partition", explain_res)
        self.check_explain_res_has_row("blocking=1", explain_res)

    def do_partition_by_col_agg(self):
        self.init_data()
        self.prepareTestEnv()
        #time.sleep(99999999)
        self.check_pipelined_agg_plan_with_slimit()
        self.check_pipelined_agg_data_with_slimit()
        self.check_remove_partition()
        print("do do_partition_by_col agg ............ [passed]")

    #
    # ------------------- expr ----------------
    # 
    def do_partition_expr(self):
        tdSql.prepare()
        tdSql.execute(f'''create table sta(ts timestamp, f int, col2 bigint) tags(tg1 int, tg2 binary(20))''')
        tdSql.execute(f"create table sta1 using sta tags(1, 'a')")
        tdSql.execute(f"insert into sta1 values(1537146000001, 11, 110)")
        tdSql.execute(f"insert into sta1 values(1537146000002, 12, 120)")
        tdSql.execute(f"insert into sta1 values(1537146000003, 13, 130)")

        tdSql.query("select _wstart, f+100, count(*) from db.sta partition by f+100 session(ts, 1a) order by _wstart");
        tdSql.checkData(0, 1, 111.0)    
        print("do partiotn by expr ................... [passed]")

    #
    # ------------------- main ----------------
    # 
    def prepare_datas(self, stb_name , tb_nums , row_nums, dbname="db" ):
        tdSql.execute(f'''create database {self.dbname} MAXROWS 4096 MINROWS 100''')
        tdSql.execute(f'''use {self.dbname}''')
        tdSql.execute(f'''CREATE STABLE {self.dbname}.{self.stable} (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT) TAGS (`groupid` TINYINT, `location` VARCHAR(16))''')
        
        for i in range(self.tb_nums):
            tbname = f"{self.dbname}.sub_{self.stable}_{i}"
            ts = self.ts + i*10000
            tdSql.execute(f"create table {tbname} using {self.dbname}.{self.stable} tags({i} ,'nchar_{i}')")
            tdLog.info(f"create table {tbname} using {self.dbname}.{self.stable} tags({i} ,'nchar_{i}')")
            if i < (self.tb_nums - 2):
                for row in range(row_nums):
                    ts = self.ts + row*1000
                    tdSql.execute(f"insert into {tbname} values({ts} , {row/10}, {215 + (row % 100)})")

                for null in range(5):
                    ts =  self.ts + row_nums*1000 + null*1000
                    tdSql.execute(f"insert into {tbname} values({ts} , NULL , NULL)")

    def basic_query(self):
        tdSql.query(f"select groupid, count(*) from {self.dbname}.{self.stable} partition by groupid interval(1d) limit 100")
        tdSql.checkRows(8)
        tdSql.checkData(0, 1, 1005)
        
        tdSql.query(f"select groupid, count(*) from {self.dbname}.{self.stable} partition by tbname interval(1d) order by groupid limit 100;")
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, 0)   
        tdSql.checkData(0, 1, 1005)   
        tdSql.checkData(7, 0, 7)  
        tdSql.checkData(7, 1, 1005)  
        
        tdSql.query(f"select groupid, count(*) from {self.dbname}.{self.stable} partition by tbname, groupid interval(5d) order by groupid limit 10")
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, 0)   
        tdSql.checkData(0, 1, 1005)   
        tdSql.checkData(7, 0, 7)  
        tdSql.checkData(7, 1, 1005)
        
        tdSql.query(f"select groupid, count(*), min(current) from {self.dbname}.{self.stable} partition by groupid interval(5d) order  by groupid limit 10;")
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, 0)   
        tdSql.checkData(0, 1, 1005)
        tdSql.checkData(0, 2, 0)  
        tdSql.checkData(7, 0, 7)  
        tdSql.checkData(7, 1, 1005)
        tdSql.checkData(7, 2, 0)
        
        tdSql.query(f"select groupid, min(current) from {self.dbname}.{self.stable} partition by groupid interval(5d) limit 100;")
        tdSql.checkRows(8)
        tdSql.checkData(0, 1, 0)
        
        tdSql.query(f"select groupid, avg(current) from {self.dbname}.{self.stable} partition by groupid interval(5d) limit 10000;")
        tdSql.checkRows(8)
        tdSql.checkData(0, 1, tdSql.getData(7, 1))

        tdSql.query(f"select current, avg(current) from {self.dbname}.{self.stable} partition by current interval(5d) limit 100;")
        tdSql.checkData(0, 0, tdSql.getData(0, 1)) 
        
        tdSql.query(f"select groupid, last(voltage), min(current) from {self.dbname}.{self.stable} partition by groupid interval(5d) limit 10")
        tdSql.checkRows(8)
        tdSql.checkData(0, 1, tdSql.getData(7, 1))
        tdSql.checkData(0, 2, tdSql.getData(7, 2))
        
        tdSql.query(f"select groupid, min(current), min(voltage) from {self.dbname}.{self.stable} partition by tbname, groupid interval(5d) limit 100;")
        tdSql.checkRows(8)
        tdSql.checkData(0, 1, 0)   
        tdSql.checkData(0, 2, 215)   
        tdSql.checkData(7, 1, 0)  
        tdSql.checkData(7, 2, 215)
        
        tdSql.query(f"select groupid, min(voltage), min(current) from {self.dbname}.{self.stable} partition by tbname, groupid interval(5d) limit 100;")
        tdSql.checkRows(8)
        tdSql.checkData(0, 2, 0)   
        tdSql.checkData(0, 1, 215)   
        tdSql.checkData(7, 2, 0)  
        tdSql.checkData(7, 1, 215)           
        
    def do_partition_limit_interval(self):
        # init
        self.row_nums = 1000
        self.tb_nums = 10
        self.ts = 1537146000000
        self.dbname = "db1"
        self.stable = "meters"
        
        # prepare data
        tdSql.prepare()
        self.prepare_datas("stb",self.tb_nums,self.row_nums)
        self.basic_query()
    
        print("do partition by interval .............. [passed]")

    #
    # ------------------- main ----------------
    # 
    def test_query_partitionby_basic(self):
        """Partiton by basic

        1. Partiton by normal columns
        2. Partiton by tag columns
        3. Partiton by with express
        4. Partiton by with interval
        5. Partiton by with aggregation
        6. Partiton by with scalar
        7. Partiton by with tbname
        8. Partiton by with limit/slimit

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-09 Alex Duan Migrated from uncatalog/system-test/2-query/test_partition_by_col.py
            - 2025-12-09 Alex Duan Migrated from uncatalog/system-test/2-query/test_partition_by_col_agg.py
            - 2025-12-09 Alex Duan Migrated from uncatalog/system-test/2-query/test_partition_expr.py
            - 2025-12-09 Alex Duan Migrated from uncatalog/system-test/2-query/test_partition_limit_interval.py

        """
        self.do_partition_by_col()
        self.do_partition_by_col_agg()
        self.do_partition_expr()
        self.do_partition_limit_interval()