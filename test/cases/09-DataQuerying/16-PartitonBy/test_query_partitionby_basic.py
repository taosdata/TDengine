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
        sql = "insert into test.t0 values "
        for i in range(0, 4096):
            sql += f" ({1537146000000 + i}, 1,1,1,1,1,1,1,'a','1')"
        tdSql.execute(sql)
        tdSql.execute("flush database test")

        sql = "insert into test.t0 values "
        sql += f"({1537146000000 + 4095}, 1,1,1,1,1,1,1,'a','1')"
        for i in range(4095, 4096*2 + 100):
            sql += f" ({1537146000000 + i}, 1,1,1,1,1,1,1,'a','1')"
        tdSql.execute(sql)

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
        sql = "insert into sta1 values "
        sql += f" (1537146000001, 11, 110)"
        sql += f" (1537146000002, 12, 120)"
        sql += f" (1537146000003, 13, 130)"
        tdSql.execute(sql)

        tdSql.query("select _wstart, f+100, count(*) from db.sta partition by f+100 session(ts, 1a) order by _wstart");
        tdSql.checkData(0, 1, 111.0)    
        print("do partiotn by expr ................... [passed]")

    #
    # ------------------- basic ----------------
    # 
    def prepare_datas(self, stb_name , tb_nums , row_nums, dbname="db" ):
        tdSql.execute(f'''create database {self.dbname} MAXROWS 4096 MINROWS 100''')
        tdSql.execute(f'''use {self.dbname}''')
        tdSql.execute(f'''CREATE STABLE {self.dbname}.{self.stable} (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT) TAGS (`groupid` TINYINT, `location` VARCHAR(16))''')
        
        sql1 = "create table "
        sql2 = "insert into "
        for i in range(self.tb_nums):
            tbname = f"{self.dbname}.sub_{self.stable}_{i}"
            ts = self.ts + i*10000
            sql1 += f" {tbname} using {self.dbname}.{self.stable} tags({i} ,'nchar_{i}')"
            if i < (self.tb_nums - 2):
                for row in range(row_nums):
                    ts = self.ts + row*1000
                    sql2 += f" {tbname} values ({ts}, {row/10}, {215 + (row % 100)})"

                for null in range(5):
                    ts =  self.ts + row_nums*1000 + null*1000
                    sql2 += f" {tbname} values ({ts}, NULL, NULL)"
        
        tdSql.execute(sql1)
        tdSql.execute(sql2)

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
    # ------------------- partition/group ----------------
    # 
    def init_class(self):
        self.row_nums = 10
        self.tb_nums = 10
        self.ts = 1537146000000
        self.dbname = "db"
        self.stable = "stb"

    def prepare_db(self):
        tdSql.execute(f" use {self.dbname} ")
        tdSql.execute(f" create stable {self.dbname}.{self.stable} (ts timestamp , c1 int , c2 bigint , c3 float , c4 double , c5 smallint , c6 tinyint , c7 bool , c8 binary(36) , c9 nchar(36) , uc1 int unsigned,\
            uc2 bigint unsigned ,uc3 smallint unsigned , uc4 tinyint unsigned ) tags(t1 timestamp , t2 int , t3 bigint , t4 float , t5 double , t6 smallint , t7 tinyint , t8 bool , t9 binary(36)\
                , t10 nchar(36) , t11 int unsigned , t12 bigint unsigned ,t13 smallint unsigned , t14 tinyint unsigned ) ")

        for i in range(self.tb_nums):
            tbname = f"{self.dbname}.sub_{self.stable}_{i}"
            ts = self.ts + i*10000
            tdSql.execute(f"create table {tbname} using {self.dbname}.{self.stable} tags ({ts} , {i} , %d ,%d , %f , 1 , 2, 'true', 'binary_{i}' ,'nchar_{i}',{i},{i},10,20 )"%(i*10,i*1.0,i*1.0))

    def insert_db(self, tb_nums, row_nums):           
        for i in range(tb_nums):
            tbname = f"{self.dbname}.sub_{self.stable}_{i}"
            ts_base = self.ts + i*10000
            for row in range(row_nums):
                ts = ts_base + row*1000
                tdSql.execute(f"insert into {tbname} values({ts} , {row} , {row} , {row} , {row} , 1 , 2 , 'true' , 'binary_{row}' , 'nchar_{row}' , {row} , {row} , 1 ,2 )")


    def check_groupby(self, keyword, check_num, nonempty_tb_num):
        ####### by tbname
        tdSql.query(f"select count(*), count(1), count(c1) from {self.dbname}.{self.stable} {keyword} by tbname ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} {keyword} by tbname ")
        tdSql.checkRows(check_num)

        if(keyword.strip() == "group"):
            tdSql.query(f"select tbname from {self.dbname}.{self.stable} {keyword} by tbname order by count(*)")
            tdSql.checkRows(check_num)
        else:
            tdSql.error(f"select tbname from {self.dbname}.{self.stable} {keyword} by tbname order by count(*)")

        # last
        tdSql.query(f"select last(ts), count(*) from {self.dbname}.{self.stable} {keyword} by tbname order by last(ts)")
        tdSql.checkRows(check_num)

        tdSql.query(f"select tbname from {self.dbname}.{self.stable} {keyword} by tbname having count(*)>=0")
        tdSql.checkRows(check_num)

        # having filter out empty
        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} {keyword} by tbname having count(*) <= 0")
        tdSql.checkRows(check_num - nonempty_tb_num)

        ####### by tag
        tdSql.query(f"select t2, count(*), count(1), count(c1) from {self.dbname}.{self.stable} {keyword} by t2 ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select t2, count(*) from {self.dbname}.{self.stable} {keyword} by t2 having count(*) <= 0")
        tdSql.checkRows(check_num - nonempty_tb_num)

        # where
        tdSql.query(f"select t2, count(*) from {self.dbname}.{self.stable} where ts < now {keyword} by t2 ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select t2, count(*) from {self.dbname}.{self.stable} where ts > 1737146000000 {keyword} by t2 ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select t2, count(*) from {self.dbname}.{self.stable} where c1 = 1 {keyword} by t2 ")
        tdSql.checkRows(check_num)

        ####### by col
        tdSql.query(f"select c1, count(*), count(1), count(c1) from {self.dbname}.{self.stable} {keyword} by c1 ")
        num = 0
        if nonempty_tb_num > 0:
            num = self.row_nums
        tdSql.checkRows(num)

        tdSql.query(f"select c1, count(*), count(1), count(c1) from {self.dbname}.{self.stable} {keyword} by c1 having c1 >= 0")
        num = 0
        if nonempty_tb_num > 0:
            num = self.row_nums
        tdSql.checkRows(num)
    
        tdSql.query(f"select ts, count(*) from {self.dbname}.{self.stable} {keyword} by ts ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)

        # col + tag
        tdSql.query(f"select t2, c1, count(*) from {self.dbname}.{self.stable} {keyword} by t2, c1 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)

        tdSql.query(f"select t2, t3, c1, count(*) from {self.dbname}.{self.stable} {keyword} by t2, t3, c1 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)

    def check_groupby_position(self, keyword, check_num, nonempty_tb_num):
        ####### by tbname
        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} {keyword} by 1 ")
        tdSql.checkRows(check_num)

        if(keyword.strip() == "group"):
            tdSql.query(f"select tbname from {self.dbname}.{self.stable} {keyword} by 1 order by count(*)")
            tdSql.checkRows(check_num)
        else:
            tdSql.error(f"select tbname from {self.dbname}.{self.stable} {keyword} by 1 order by count(*)")

        # last
        tdSql.query(f"select tbname from {self.dbname}.{self.stable} {keyword} by 1 having count(*)>=0")
        tdSql.checkRows(check_num)

        # having filter out empty
        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} {keyword} by 1 having count(*) <= 0")
        tdSql.checkRows(check_num - nonempty_tb_num)

        ####### by tag
        tdSql.query(f"select t2, count(*), count(1), count(c1) from {self.dbname}.{self.stable} {keyword} by 1 ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select t2, count(*) from {self.dbname}.{self.stable} {keyword} by 1 having count(*) <= 0")
        tdSql.checkRows(check_num - nonempty_tb_num)

        # where
        tdSql.query(f"select t2, count(*) from {self.dbname}.{self.stable} where ts < now {keyword} by 1 ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select t2, count(*) from {self.dbname}.{self.stable} where ts > 1737146000000 {keyword} by 1 ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select t2, count(*) from {self.dbname}.{self.stable} where c1 = 1 {keyword} by 1 ")
        tdSql.checkRows(check_num)

        ####### by col
        tdSql.query(f"select c1, count(*), count(1), count(c1) from {self.dbname}.{self.stable} {keyword} by 1 ")
        num = 0
        if nonempty_tb_num > 0:
            num = self.row_nums
        tdSql.checkRows(num)

        tdSql.query(f"select ts, count(*) from {self.dbname}.{self.stable} {keyword} by 1 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)

        # col + tag
        tdSql.query(f"select t2, c1, count(*) from {self.dbname}.{self.stable} {keyword} by 1, 2 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)
        tdSql.query(f"select t2, c1, count(*) from {self.dbname}.{self.stable} {keyword} by 1, c1 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)
        tdSql.query(f"select t2, c1, count(*) from {self.dbname}.{self.stable} {keyword} by t2, 2 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)

        tdSql.query(f"select t2, t3, c1, count(*) from {self.dbname}.{self.stable} {keyword} by 1, 2, 3 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)
        tdSql.query(f"select t2, t3, c1, count(*) from {self.dbname}.{self.stable} {keyword} by t2, 2, 3 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)
        tdSql.query(f"select t2, t3, c1, count(*) from {self.dbname}.{self.stable} {keyword} by 1, t3, 3 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)

        tdSql.query(f"select sum(t0.sumc2) from (select c1, sum(c2) as sumc2 from {self.dbname}.{self.stable} {keyword} by 1) t0")
        num = 0
        if nonempty_tb_num > 0:
            num = 1
        tdSql.checkRows(num)

        tdSql.query(f"select abs(c1), count(*) from {self.dbname}.{self.stable} {keyword} by 1")
        num = 0
        if nonempty_tb_num > 0:
            num = self.row_nums
        tdSql.checkRows(num)

        ####### error case
        tdSql.error(f"select c1, count(*) from {self.dbname}.{self.stable} {keyword} by 10")
        tdSql.error(f"select c1, count(*) from {self.dbname}.{self.stable} {keyword} by 0")
        tdSql.error(f"select c1, c2, count(*) from {self.dbname}.{self.stable} {keyword} by 0, 1")
        tdSql.error(f"select c1, count(*) from {self.dbname}.{self.stable} {keyword} by 1.2")
        tdSql.error(f"select c1, c2, c3, count(*) from {self.dbname}.{self.stable} {keyword} by 1, 2.2, 3")
        tdSql.error(f"select c1, c2, count(*) from {self.dbname}.{self.stable} {keyword} by 1")
        tdSql.error(f"select c1, avg(c2), count(*) from {self.dbname}.{self.stable} {keyword} by 1, 2")

    def check_groupby_alias(self, keyword, check_num, nonempty_tb_num):
        tdSql.query(f"select t1 as t1_alias, count(*) from {self.dbname}.{self.stable} {keyword} by t1_alias ")
        tdSql.checkRows(check_num)

        if(keyword.strip() == "group"):
            tdSql.query(f"select t1 as t1_alias from {self.dbname}.{self.stable} {keyword} by t1_alias order by count(*)")
            tdSql.checkRows(check_num)
        else:
            tdSql.error(f"select t1 as t1_alias from {self.dbname}.{self.stable} {keyword} by t1_alias order by count(*)")

        # last
        tdSql.query(f"select t1 as t1_alias from {self.dbname}.{self.stable} {keyword} by t1_alias having count(*)>=0")
        tdSql.checkRows(check_num)

        # having filter out empty
        tdSql.query(f"select t1 as t1_alias, count(*) from {self.dbname}.{self.stable} {keyword} by t1_alias having count(*) <= 0")
        tdSql.checkRows(check_num - nonempty_tb_num)

        ####### by tag
        tdSql.query(f"select t2 as t2_alias, count(*), count(1), count(c1) from {self.dbname}.{self.stable} {keyword} by t2_alias ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select t2 as t2_alias, count(*) from {self.dbname}.{self.stable} {keyword} by t2_alias having count(*) <= 0")
        tdSql.checkRows(check_num - nonempty_tb_num)

        # where
        tdSql.query(f"select t2 as t2_alias, count(*) from {self.dbname}.{self.stable} where ts < now {keyword} by t2_alias ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select t2 as t2_alias, count(*) from {self.dbname}.{self.stable} where ts > 1737146000000 {keyword} by t2_alias ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select t2 as t2_alias, count(*) from {self.dbname}.{self.stable} where c1 = 1 {keyword} by t2_alias ")
        tdSql.checkRows(check_num)

        ####### by col
        tdSql.query(f"select c1 as c1_alias, count(*), count(1), count(c1) from {self.dbname}.{self.stable} {keyword} by c1_alias ")
        num = 0
        if nonempty_tb_num > 0:
            num = self.row_nums
        tdSql.checkRows(num)

        tdSql.query(f"select ts as ts_alias, count(*) from {self.dbname}.{self.stable} {keyword} by ts_alias ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)

        # col + tag
        tdSql.query(f"select t2 as t2_alias, c1 as c1_alias, count(*) from {self.dbname}.{self.stable} {keyword} by 1, 2 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)
        tdSql.query(f"select t2 as t2_alias, c1 as c1_alias, count(*) from {self.dbname}.{self.stable} {keyword} by 1, c1 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)
        tdSql.query(f"select t2 as t2_alias, c1 as c1_alias, count(*) from {self.dbname}.{self.stable} {keyword} by t2, 2 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)

        tdSql.query(f"select t2 as t2_alias, t3 as t3_alias, c1 as c1_alias, count(*) from {self.dbname}.{self.stable} {keyword} by t2_alias, t3_alias, 3 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)
        tdSql.query(f"select t2 as t2_alias, t3 as t3_alias, c1 as c1_alias, count(*) from {self.dbname}.{self.stable} {keyword} by t2, t3_alias, c1_alias ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)
        tdSql.query(f"select t2 as t2_alias, t3 as t3_alias, c1 as c1_alias, count(*) from {self.dbname}.{self.stable} {keyword} by t2_alias, t3, c1_alias ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)

        tdSql.query(f"select sum(t0.sumc2) from (select c1 as c1_alias, sum(c2) as sumc2 from {self.dbname}.{self.stable} {keyword} by c1_alias) t0")
        num = 0
        if nonempty_tb_num > 0:
            num = 1
        tdSql.checkRows(num)

        tdSql.query(f"select abs(c1) as abs_alias, count(*) from {self.dbname}.{self.stable} {keyword} by abs_alias")
        num = 0
        if nonempty_tb_num > 0:
            num = self.row_nums
        tdSql.checkRows(num)

        ####### error case
        tdSql.error(f"select c1, avg(c2) as avg_alias, count(*) from {self.dbname}.{self.stable} {keyword} by 1, avg_alias")

    def check_groupby_sub_table(self):
        for i in range(self.tb_nums):
            tbname = f"{self.dbname}.sub_{self.stable}_{i}"
            ts = self.ts + i*10000
            tdSql.query(f"select t1, t2, t3,count(*) from {tbname}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, i)
            tdSql.checkData(0, 2, i*10)

            tdSql.query(f"select cast(t2 as binary(12)),count(*) from {tbname}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, i)

            tdSql.query(f"select t2 + 1, count(*) from {tbname}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, i + 1)

            tdSql.query(f"select t1, t2, t3, count(*) from {tbname} group by tbname")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, i)
            tdSql.checkData(0, 2, i*10)

            tdSql.query(f"select t1, t2, t3, count(*) from {tbname} group by tbname, c1, t4")
            tdSql.checkData(0, 1, i)
            tdSql.checkData(0, 2, i*10)

            tdSql.query(f"select t1, t2, t3, count(*) from {tbname} partition by tbname")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, i)
            tdSql.checkData(0, 2, i*10)

            tdSql.query(f"select t1, t2, t3, count(*) from {tbname} partition by c1, tbname")
            tdSql.checkData(0, 1, i)
            tdSql.checkData(0, 2, i*10)

        tdSql.query(f"select t1, t2, t3, count(*) from {self.dbname}.{self.stable} partition by c1, tbname order by tbname desc")
        tdSql.checkRows(50)
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(0, 2, 40)


    def check_multi_group_key(self, check_num, nonempty_tb_num):
        # multi tag/tbname
        tdSql.query(f"select t2, t3, tbname, count(*) from {self.dbname}.{self.stable} group by t2, t3, tbname")
        tdSql.checkRows(check_num)

        tdSql.query(f"select cast(t2 as binary(12)), count(*) from {self.dbname}.{self.stable} group by t2, t3, tbname")
        tdSql.checkRows(check_num)

        tdSql.query(f"select t2, t3, tbname, count(*) from {self.dbname}.{self.stable} partition by t2, t3, tbname")
        tdSql.checkRows(check_num)

        tdSql.query(f"select t2, t3, tbname, count(*) from {self.dbname}.{self.stable} group by tbname order by tbname asc")
        tdSql.checkRows(check_num)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 1, 20)
        tdSql.checkData(3, 1, 30)

        tdSql.query(f"select t2, t3, tbname, count(*) from {self.dbname}.{self.stable} partition by tbname order by tbname asc")
        tdSql.checkRows(check_num)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(2, 1, 20)
        tdSql.checkData(3, 1, 30)

        # multi tag + col
        tdSql.query(f"select t1, t2, c1, count(*) from {self.dbname}.{self.stable} partition by t1, t2, c1 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)

        # tag + multi col
        tdSql.query(f"select t2, c1, c2, count(*) from {self.dbname}.{self.stable} partition by t2, c1, c2 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)


    def check_multi_agg(self, all_tb_num, nonempty_tb_num):
        tdSql.query(f"select count(*), sum(c1) from {self.dbname}.{self.stable} group by tbname ")
        tdSql.checkRows(all_tb_num)

        tdSql.query(f"select count(1), sum(1), avg(c1), apercentile(c1, 50), spread(ts) from {self.dbname}.{self.stable} group by tbname ")
        tdSql.checkRows(all_tb_num)

        tdSql.query(f"select count(c1), sum(c1), min(c1), mode(c1), stddev(c1), spread(c1) from {self.dbname}.{self.stable} partition by tbname ")
        tdSql.checkRows(all_tb_num)

        # elapsed: continuous duration in a statistical period, table merge scan
        tdSql.query(f" select count(c1), max(c5), last_row(c5), elapsed(ts), spread(c1) from {self.dbname}.{self.stable} group by tbname")
        tdSql.checkRows(all_tb_num)

        tdSql.query(f" select count(c1), min(c1), avg(c1), elapsed(ts), mode(c1) from {self.dbname}.{self.stable} partition by tbname")
        tdSql.checkRows(all_tb_num)

        tdSql.query(f" select count(c1), elapsed(ts), twa(c1), irate(c1), leastsquares(c1,1,1) from {self.dbname}.{self.stable} partition by tbname")
        tdSql.checkRows(all_tb_num)

        tdSql.query(f" select avg(c1), elapsed(ts), twa(c1), irate(c1) from {self.dbname}.{self.stable} partition by tbname")
        tdSql.checkRows(nonempty_tb_num)

        # if nonempty_tb_num > 0:
        #   tdSql.query(f" select avg(c1), percentile(c1, 50) from {self.dbname}.sub_{self.stable}_1")
        #   tdSql.checkRows(1)

    def check_innerSelect(self, check_num):
        tdSql.query(f"select * from (select count(c1) from {self.dbname}.{self.stable} group by tbname) ")
        tdSql.checkRows(check_num)   

        tdSql.query(f"select * from (select count(c1) from {self.dbname}.{self.stable} partition by tbname) ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select t1, c from (select t1, sum(c1) as s, count(*) as c from {self.dbname}.{self.stable} partition by t1)")
        tdSql.checkRows(check_num)


    def check_window(self, nonempty_tb_num):
        # empty group optimization condition is not met
        # time window
        tdSql.query(f"select count(c1) from {self.dbname}.{self.stable} partition by tbname interval(1d)")
        tdSql.checkRows(nonempty_tb_num)

        tdSql.query(f"select _wstart, _wend, count(c1), max(c1), apercentile(c1, 50) from {self.dbname}.{self.stable} partition by tbname interval(1d)")
        tdSql.checkRows(nonempty_tb_num)

        # state window
        tdSql.query(f"select tbname, count(*), c1 from {self.dbname}.{self.stable} partition by tbname state_window(c1)")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)

        # session window
        tdSql.query(f"select count(c1) from {self.dbname}.{self.stable} partition by tbname session(ts, 5s)")
        tdSql.checkRows(nonempty_tb_num)

        # event window
        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9;")
        tdSql.checkRows(nonempty_tb_num)

    def check_event_window(self, nonempty_tb_num):
        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and 1=1;")
        tdSql.checkRows(nonempty_tb_num)

        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and 1=0;")
        tdSql.checkRows(0)

        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and tbname='sub_{self.stable}_0';")
        tdSql.checkRows(1)

        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and t2=0;")
        tdSql.checkRows(1)

        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and t2=0 having count(*) > 10;")
        tdSql.checkRows(0)

        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and _rowts>0;")
        tdSql.checkRows(nonempty_tb_num)

        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and _qstart>0;")
        tdSql.checkRows(0)
        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and _qstart<0;")
        tdSql.checkRows(0)
        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and _qstart<_qend;")
        tdSql.checkRows(0)

        tdSql.error(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and _wstart<q_start;")
        tdSql.error(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and _wstart - q_start > 0;")
        tdSql.error(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and _irowts>0;")
        tdSql.error(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 and _wduration > 5s end with c2 = 9;")
        tdSql.error(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and _wstart > 1299845454;")
        tdSql.error(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and _wduration + 1s > 5s;")
        tdSql.error(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and count(*) > 10;")

    def check_error(self):
        tdSql.error(f"select * from {self.dbname}.{self.stable} group by t2")
        tdSql.error(f"select t2, count(*) from {self.dbname}.{self.stable} group by t2 where t2 = 1")
        tdSql.error(f"select t2, count(*) from {self.dbname}.{self.stable} group by t2 interval(1d)")

    def check_TS5567(self):
        tdSql.query(f"select const_col from (select 1 as const_col from {self.dbname}.{self.stable}) t group by const_col")
        tdSql.checkRows(1)
        tdSql.query(f"select const_col from (select 1 as const_col from {self.dbname}.{self.stable}) t partition by const_col")
        tdSql.checkRows(50)
        tdSql.query(f"select const_col from (select 1 as const_col, count(c1) from {self.dbname}.{self.stable} t group by c1) group by const_col")
        tdSql.checkRows(1)
        tdSql.query(f"select const_col from (select 1 as const_col, count(c1) from {self.dbname}.{self.stable} t group by c1) partition by const_col")
        tdSql.checkRows(10)
        tdSql.query(f"select const_col as c_c from (select 1 as const_col from {self.dbname}.{self.stable}) t group by c_c")
        tdSql.checkRows(1)
        tdSql.query(f"select const_col as c_c from (select 1 as const_col from {self.dbname}.{self.stable}) t partition by c_c")
        tdSql.checkRows(50)
        tdSql.query(f"select const_col from (select 1 as const_col, count(c1) from {self.dbname}.{self.stable} t group by c1) group by 1")
        tdSql.checkRows(1)
        tdSql.query(f"select const_col from (select 1 as const_col, count(c1) from {self.dbname}.{self.stable} t group by c1) partition by 1")
        tdSql.checkRows(10)
    
    def check_TD_32883(self):
        sql = "select avg(c1), t9 from db.stb group by t9,t9, tbname"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(5)
        sql = "select avg(c1), t10 from db.stb group by t10,t10, tbname"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(5)
        sql = "select avg(c1), t10 from db.stb partition by t10,t10, tbname"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(5)
        sql = "select avg(c1), concat(t9,t10) from db.stb group by concat(t9,t10), concat(t9,t10),tbname"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(5)
        
    def check_TS_5727(self):
        tdSql.execute(f" use {self.dbname} ")
        stableName = "test5727"
        sql = f"CREATE STABLE {self.dbname}.{stableName} (`ts` TIMESTAMP, `WaterConsumption` FLOAT,  \
        `ElecConsumption` INT, `Status` BOOL, `status2` BOOL, `online` BOOL)                         \
        TAGS (`ActivationTime` TIMESTAMP, `ProductId` INT,                                           \
        `ProductMac` VARCHAR(24), `location` INT)"
        tdSql.execute(sql)
        
        sql = f'CREATE TABLE {self.dbname}.`d00` USING {self.dbname}.{stableName} \
            (`ActivationTime`, `ProductId`, `ProductMac`, `location`)   \
                TAGS (1733124710578, 1001, "00:11:22:33:44:55", 100000)'
        tdSql.execute(sql)
        sql = f'CREATE TABLE {self.dbname}.`d01` USING {self.dbname}.{stableName}  \
            (`ActivationTime`, `ProductId`, `ProductMac`, `location`)  \
                TAGS (1733124723572, 1002, "00:12:22:33:44:55", 200000)'
        tdSql.execute(sql)  
        sql = f'CREATE TABLE {self.dbname}.`d02` USING {self.dbname}.{stableName} \
            (`ActivationTime`, `ProductId`, `ProductMac`, `location`) \
                TAGS (1733124730908, 1003, "00:11:23:33:44:55", 100000)'
        tdSql.execute(sql)
        
        sql = f'insert into {self.dbname}.d00 values(now - 2s, 5, 5, true, true, false);'
        tdSql.execute(sql)
        sql = f'insert into {self.dbname}.d01 values(now - 1s, 6, 5, true, true, true);'
        tdSql.execute(sql)
        sql = f'insert into {self.dbname}.d02 values(now, 6, 7, true, true, true);'
        tdSql.execute(sql)
        
        sql = f'select `location`, tbname from {self.dbname}.{stableName} where ts < now group by tbname order by tbname;'
        tdSql.query(sql)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 100000)
        tdSql.checkData(1, 0, 200000)
        tdSql.checkData(2, 0, 100000)
        tdSql.checkData(0, 1, "d00")
        tdSql.checkData(1, 1, "d01")
        tdSql.checkData(2, 1, "d02")
        
        sql = f'select tbname,last(online) as online,location from {self.dbname}.{stableName} where ts < now group by tbname order by tbname;'
        tdSql.query(sql)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "d00")
        tdSql.checkData(1, 0, "d01")
        tdSql.checkData(2, 0, "d02")
        tdSql.checkData(0, 1, False)
        tdSql.checkData(1, 1, True)
        tdSql.checkData(2, 1, True)
        tdSql.checkData(0, 2, 100000)
        tdSql.checkData(1, 2, 200000)
        tdSql.checkData(2, 2, 100000)

              
        sql = f'select location,tbname,last_row(online) as online from {self.dbname}.{stableName} where ts < now group by tbname order by tbname;'
        tdSql.query(sql)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 100000)
        tdSql.checkData(1, 0, 200000)
        tdSql.checkData(2, 0, 100000)
        tdSql.checkData(0, 1, "d00")
        tdSql.checkData(1, 1, "d01")
        tdSql.checkData(2, 1, "d02")
        tdSql.checkData(0, 2, False)
        tdSql.checkData(1, 2, True)
        tdSql.checkData(2, 2, True)

    def do_group_partition(self):
        self.init_class()
        tdSql.prepare()
        self.prepare_db()
        # empty table only
        self.check_groupby('group', self.tb_nums, 0)
        self.check_groupby('partition', self.tb_nums, 0)
        self.check_groupby_position('group', self.tb_nums, 0)
        self.check_groupby_position('partition', self.tb_nums, 0)
        self.check_groupby_alias('group', self.tb_nums, 0)
        self.check_groupby_alias('partition', self.tb_nums, 0)
        self.check_innerSelect(self.tb_nums)
        self.check_multi_group_key(self.tb_nums, 0)
        self.check_multi_agg(self.tb_nums, 0)
        self.check_window(0)

        # insert data to 5 tables
        nonempty_tb_num = 5
        self.insert_db(nonempty_tb_num, self.row_nums)

        self.check_groupby('group', self.tb_nums, nonempty_tb_num)
        self.check_groupby('partition', self.tb_nums, nonempty_tb_num)
        self.check_groupby_position('group', self.tb_nums, nonempty_tb_num)
        self.check_groupby_position('partition', self.tb_nums, nonempty_tb_num)
        self.check_groupby_alias('group', self.tb_nums, nonempty_tb_num)
        self.check_groupby_alias('partition', self.tb_nums, nonempty_tb_num)
        self.check_groupby_sub_table()
        self.check_innerSelect(self.tb_nums)
        self.check_multi_group_key(self.tb_nums, nonempty_tb_num)
        self.check_multi_agg(self.tb_nums, nonempty_tb_num)
        self.check_window(nonempty_tb_num)
        self.check_event_window(nonempty_tb_num)

        self.check_TS5567()
        self.check_TD_32883()

        ## test old version before changed
        # self.check_groupby('group', 0, 0)
        # self.insert_db(5, self.row_nums)
        # self.check_groupby('group', 5, 5)

        self.check_error()
        
        self.check_TS_5727()
    
        print("do partition/group by ................. [passed]")
        
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
        9. Partiton by with order by
        10. Partiton by with where/having
        11. Partiton by with multi-column key
        12. Partiton by with multi-aggregation
        13. Partiton by with window function
        14. Partiton by with inner select
        15. Partiton by with alias
        16. Partiton by with position number
        17. Partition by with error case

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-09 Alex Duan Migrated from uncatalog/system-test/2-query/test_partition_by_col.py
            - 2025-12-09 Alex Duan Migrated from uncatalog/system-test/2-query/test_partition_by_col_agg.py
            - 2025-12-09 Alex Duan Migrated from uncatalog/system-test/2-query/test_partition_expr.py
            - 2025-12-09 Alex Duan Migrated from uncatalog/system-test/2-query/test_partition_limit_interval.py
            - 2025-12-09 Alex Duan Migrated from uncatalog/system-test/2-query/test_group_partition.py

        """
        self.do_partition_by_col()
        self.do_partition_by_col_agg()
        self.do_partition_expr()
        self.do_partition_limit_interval()
        self.do_group_partition()