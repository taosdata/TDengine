import queue
import random
import os
import threading
import taos
import sys
import time
import socket
import math

from datetime import timedelta
from pandas._libs import interval
from new_test_framework.utils import tdLog, tdSql
from new_test_framework.utils.common import TDCom
from new_test_framework.utils.common import tdCom
from new_test_framework.utils.sql import TDSql

class TestFill:
    updatecfgDict = {'asynclog': 0, 'ttlUnit': 1, 'ttlPushInterval': 5, 'ratioOfVnodeStreamThrea': 4, 'numOfVnodeQueryThreads': 80}

    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)

    def generate_fill_range(self, data_start: int, data_end: int, interval: int, step: int):
        ret = []
        begin = data_start - 10 * interval
        end = data_end + 10 * interval
        for i in range(begin, end, step):
            for j in range(begin, end, step):
                ret.append((i,j))
        return ret

    def check_fill_range(self, where_start, where_end, res_asc, res_desc, sql: str, interval):
        if len(res_asc) != len(res_desc):
            tdLog.exit(f"err, asc desc with different rows, asc: {len(res_asc)}, desc: {len(res_desc)} sql: {sql}")
        if len(res_asc) == 0:
            tdLog.info(f'from {where_start} to {where_end} no rows returned')
            return
        asc_first = res_asc[0]
        asc_last = res_asc[-1]
        desc_first = res_desc[0]
        desc_last = res_desc[-1]
        if asc_first[0] != desc_last[0] or asc_last[0] != desc_first[0]:
            tdLog.exit(f'fill sql different row data {sql}: asc<{asc_first[0].timestamp()}, {asc_last[0].timestamp()}>, desc<{desc_last[0].timestamp()}, {desc_first[0].timestamp()}>')
        else:
            tdLog.info(f'from {where_start} to {where_end} same time returned asc<{asc_first[0].timestamp()}, {asc_last[0].timestamp()}>, desc<{desc_last[0].timestamp()}, {desc_first[0].timestamp()}> interval: {interval}')

    def generate_partition_by(self):
        val = random.random()
        if val < 0.6:
            return ""
        elif val < 0.8:
            return "partition by location"
        else:
            return "partition by tbname"

    def generate_fill_interval(self):
        ret = []
        #intervals = [60, 90, 120, 300, 3600]
        intervals = [120, 300, 3600]
        for i in range(0, len(intervals)):
            for j in range(0, i+1):
                ret.append((intervals[i], intervals[j]))
        return ret

    def generate_fill_sql(self, where_start, where_end, fill_interval: tuple):
        partition_by = self.generate_partition_by()
        where = f'where ts >= {where_start} and ts < {where_end}'
        sql = f'select first(_wstart), last(_wstart) from (select _wstart, _wend, count(*) from test.meters {where} {partition_by} interval({fill_interval[0]}s) sliding({fill_interval[1]}s) fill(NULL)'
        sql_asc = sql + " order by _wstart asc) t"
        sql_desc = sql + " order by _wstart desc) t"
        return sql_asc, sql_desc

    def fill_test_thread_routine(self, cli: TDSql, interval, data_start, data_end, step):
        ranges = self.generate_fill_range(data_start, data_end, interval[0], step)
        for range in ranges:
            sql_asc, sql_desc = self.generate_fill_sql(range[0], range[1], interval)
            cli.query(sql_asc, queryTimes=1)
            asc_res = cli.queryResult
            cli.query(sql_desc, queryTimes=1)
            desc_res = cli.queryResult
            self.check_fill_range(range[0], range[1], asc_res,desc_res , sql_asc, interval)

    def fill_test_task_routine(self, tdCom: TDCom, queue: queue.Queue):
        cli = tdCom.newTdSql()
        while True:
            m: list = queue.get()
            if len(m) == 0:
                break
            interval = m[0]
            range = m[1]
            sql_asc, sql_desc = self.generate_fill_sql(range[0], range[1], interval)
            cli.query(sql_asc, queryTimes=1)
            asc_res = cli.queryResult
            cli.query(sql_desc, queryTimes=1)
            desc_res = cli.queryResult
            self.check_fill_range(range[0], range[1], asc_res,desc_res , sql_asc, interval)
        cli.close()

    def schedule_fill_test_tasks(self):
        num: int = 20
        threads = []
        tdCom = TDCom()
        q: queue.Queue = queue.Queue()
        for _ in range(num):
            t = threading.Thread(target=self.fill_test_task_routine, args=(tdCom, q))
            t.start()
            threads.append(t)

        data_start = 1500000000000
        data_end = 1500319968000
        step = 30000000

        fill_intervals: list[tuple] = self.generate_fill_interval()
        for interval in fill_intervals:
            ranges = self.generate_fill_range(data_start, data_end, interval[0], step)
            for r in ranges:
                q.put([interval, r])

        for _ in range(num):
            q.put([])

        for t in threads:
            t.join()

    def check_fill_range(self):
        os.system('taosBenchmark -t 10 -n 10000 -v 8 -S 32000 -y')
        self.schedule_fill_test_tasks()
        sql = "select first(_wstart), last(_wstart) from (select _wstart, count(*) from test.meters where ts >= '2019-09-19 23:54:00.000' and ts < '2019-09-20 01:00:00.000' interval(10s) sliding(10s) fill(VALUE_F, 122) order by _wstart) t"
        tdSql.query(sql, queryTimes=1)
        rows = tdSql.getRows()
        first_ts = tdSql.queryResult[0][0]
        last_ts = tdSql.queryResult[0][1]

        sql = "select first(_wstart), last(_wstart) from (select _wstart, count(*) from test.meters where ts >= '2019-09-19 23:54:00.000' and ts < '2019-09-20 01:00:00.000' interval(10s) sliding(10s) fill(VALUE_F, 122) order by _wstart desc) t"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(rows)
        tdSql.checkData(0, 0, first_ts)
        tdSql.checkData(0, 1, last_ts)
        tdSql.execute('drop database test')
    
    def check_ns_db_fill(self):
        tdSql.execute("create database nsdb precision 'ns'")
        tdSql.execute("create table nsdb.nsdb_test(ts timestamp, c0 int, c1 bool, c2 varchar(100), c3 nchar(100), c4 varbinary(100))")
        tdSql.query("select _wstart, count(*) from nsdb.nsdb_test where ts >='2025-01-01' and ts <= '2025-12-01 23:59:59' interval(1n) fill(NULL)")
        tdSql.checkRows(0)
        tdSql.execute("drop database nsdb")

    def do_fill_datatype_method(self):
        self.check_ns_db_fill()
        self.check_fill_range()
        dbname = "db"
        tbname = "tb"

        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")

        tdSql.execute(
            f'''create table if not exists {dbname}.{tbname}
            (ts timestamp, c0 int, c1 bool, c2 varchar(100), c3 nchar(100), c4 varbinary(100))
            '''
        )

        tdLog.printNoPrefix("==========step2:insert data")

        tdSql.execute(f"use db")

        tdSql.execute(f"insert into {dbname}.{tbname} values ('2020-02-01 00:00:05', 5, true, 'varchar', 'nchar', 'varbinary')")
        tdSql.execute(f"insert into {dbname}.{tbname} values ('2020-02-01 00:00:10', 10, true, 'varchar', 'nchar', 'varbinary')")
        tdSql.execute(f"insert into {dbname}.{tbname} values ('2020-02-01 00:00:15', 15, NULL, NULL, NULL, NULL)")

        tdLog.printNoPrefix("==========step3:fill data")
        tdSql.query(f"select avg(c0), last(c1) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(value, 9, 'xx');")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 9)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 10)
        tdSql.checkData(3, 0, 15)
        tdSql.checkData(4, 0, 9)
        tdSql.checkData(0, 1, False)
        tdSql.checkData(1, 1, True)
        tdSql.checkData(2, 1, True)
        tdSql.checkData(3, 1, False)
        tdSql.checkData(4, 1, False)

        tdSql.query(f"select avg(c0), last(c1) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(value, 9, True);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, True)
        tdSql.checkData(1, 1, True)
        tdSql.checkData(2, 1, True)
        tdSql.checkData(3, 1, True)
        tdSql.checkData(4, 1, True)

        tdSql.query(f"select avg(c0), last(c1) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(value, 9, False);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, False)
        tdSql.checkData(1, 1, True)
        tdSql.checkData(2, 1, True)
        tdSql.checkData(3, 1, False)
        tdSql.checkData(4, 1, False)

        tdSql.query(f"select avg(c0), last(c2) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(value, 9, 'abc');")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, "abc")
        tdSql.checkData(1, 1, 'varchar')
        tdSql.checkData(2, 1, 'varchar')
        tdSql.checkData(3, 1, "abc")
        tdSql.checkData(4, 1, "abc")

        tdSql.query(f"select avg(c0), last(c2) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(value, 9, '我是#$^中文');")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, '我是#$^中文')
        tdSql.checkData(1, 1, 'varchar')
        tdSql.checkData(2, 1, 'varchar')
        tdSql.checkData(3, 1, "我是#$^中文")
        tdSql.checkData(4, 1, "我是#$^中文")

        tdSql.query(f"select avg(c0), last(c3) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(value, 9, '我是#$^中文');")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, '我是#$^中文')
        tdSql.checkData(1, 1, 'nchar')
        tdSql.checkData(2, 1, 'nchar')
        tdSql.checkData(3, 1, "我是#$^中文")
        tdSql.checkData(4, 1, "我是#$^中文")

        tdSql.query(f"select avg(c0), last(c4) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(value, 9, '我是中文');")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, b'\xe6\x88\x91\xe6\x98\xaf\xe4\xb8\xad\xe6\x96\x87')
        tdSql.checkData(1, 1, b'varbinary')
        tdSql.checkData(2, 1, b'varbinary')
        tdSql.checkData(3, 1, b'\xe6\x88\x91\xe6\x98\xaf\xe4\xb8\xad\xe6\x96\x87')
        tdSql.checkData(4, 1, b'\xe6\x88\x91\xe6\x98\xaf\xe4\xb8\xad\xe6\x96\x87')
        
        tdLog.printNoPrefix("==========step4:fill null")
        tdSql.query(f"select avg(c0), last(c1) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(value, NULL, NULL);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 10)
        tdSql.checkData(3, 0, 15)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 1, True)
        tdSql.checkData(2, 1, True)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, None)

        tdSql.query(f"select avg(c0), last(c1) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(value, 9, NULL);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 1, True)
        tdSql.checkData(2, 1, True)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, None)

        tdSql.query(f"select avg(c0), last(c2) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(value, 9, NULL);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 1, 'varchar')
        tdSql.checkData(2, 1, 'varchar')
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, None)

        tdSql.query(f"select avg(c0), last(c3) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(value, 9, NULL);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 1, 'nchar')
        tdSql.checkData(2, 1, 'nchar')
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, None)

        tdSql.query(f"select avg(c0), last(c4) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(value, 9, NULL);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 1, b'varbinary')
        tdSql.checkData(2, 1, b'varbinary')
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, None)

        tdLog.printNoPrefix("==========step5:fill prev")
        tdSql.query(f"select avg(c0), last(c1) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(PREV);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 10)
        tdSql.checkData(3, 0, 15)
        tdSql.checkData(4, 0, 15)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 1, True)
        tdSql.checkData(2, 1, True)
        tdSql.checkData(3, 1, True)
        tdSql.checkData(4, 1, True)

        tdSql.query(f"select avg(c0), last(c2) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(PREV);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 1, 'varchar')
        tdSql.checkData(2, 1, 'varchar')
        tdSql.checkData(3, 1, 'varchar')
        tdSql.checkData(4, 1, 'varchar')

        tdSql.query(f"select avg(c0), last(c3) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(PREV);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 1, 'nchar')
        tdSql.checkData(2, 1, 'nchar')
        tdSql.checkData(3, 1, 'nchar')
        tdSql.checkData(4, 1, 'nchar')

        tdSql.query(f"select avg(c0), last(c4) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(PREV);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 1, b'varbinary')
        tdSql.checkData(2, 1, b'varbinary')
        tdSql.checkData(3, 1, b'varbinary')
        tdSql.checkData(4, 1, b'varbinary')

        tdLog.printNoPrefix("==========step6:fill next")
        tdSql.query(f"select avg(c0), last(c1) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(NEXT);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 10)
        tdSql.checkData(3, 0, 15)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(0, 1, True)
        tdSql.checkData(1, 1, True)
        tdSql.checkData(2, 1, True)
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, None)

        tdSql.query(f"select avg(c0), last(c2) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(NEXT);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 'varchar')
        tdSql.checkData(1, 1, 'varchar')
        tdSql.checkData(2, 1, 'varchar')
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, None)

        tdSql.query(f"select avg(c0), last(c3) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(NEXT);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 'nchar')
        tdSql.checkData(1, 1, 'nchar')
        tdSql.checkData(2, 1, 'nchar')
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, None)

        tdSql.query(f"select avg(c0), last(c4) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(NEXT);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, b'varbinary')
        tdSql.checkData(1, 1, b'varbinary')
        tdSql.checkData(2, 1, b'varbinary')
        tdSql.checkData(3, 1, None)
        tdSql.checkData(4, 1, None)

        tdSql.execute(f"insert into {dbname}.{tbname} values ('2020-02-01 00:00:20', 15, False, '中文', '中文', '中文');")
        tdLog.printNoPrefix("==========step6:fill next")
        tdSql.query(f"select avg(c0), last(c1) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(NEXT);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, True)
        tdSql.checkData(1, 1, True)
        tdSql.checkData(2, 1, True)
        tdSql.checkData(3, 1, False)
        tdSql.checkData(4, 1, False)

        tdSql.query(f"select avg(c0), last(c2) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(NEXT);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 'varchar')
        tdSql.checkData(1, 1, 'varchar')
        tdSql.checkData(2, 1, 'varchar')
        tdSql.checkData(3, 1, '中文')
        tdSql.checkData(4, 1, '中文')

        tdSql.query(f"select avg(c0), last(c3) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(NEXT);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 'nchar')
        tdSql.checkData(1, 1, 'nchar')
        tdSql.checkData(2, 1, 'nchar')
        tdSql.checkData(3, 1, '中文')
        tdSql.checkData(4, 1, '中文')

        tdSql.query(f"select avg(c0), last(c4) from {dbname}.{tbname} where ts>='2020-02-01 00:00:00' and ts<='2020-02-01 00:00:20' interval(5s) fill(NEXT);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, b'varbinary')
        tdSql.checkData(1, 1, b'varbinary')
        tdSql.checkData(2, 1, b'varbinary')
        tdSql.checkData(3, 1, b'\xe4\xb8\xad\xe6\x96\x87')
        tdSql.checkData(4, 1, b'\xe4\xb8\xad\xe6\x96\x87')

    #
    # ------------------- test_fill_with_group.py ----------------
    #
    def init_class(self):        
        self.vgroups    = 4
        self.ctbNum     = 10
        self.rowsPerTbl = 10000
        self.duraion = '1h'

    def create_database(self,tsql, dbName,dropFlag=1,vgroups=2,replica=1, duration:str='1d'):
        if dropFlag == 1:
            tsql.execute("drop database if exists %s"%(dbName))

        tsql.execute("create database if not exists %s vgroups %d replica %d duration %s"%(dbName, vgroups, replica, duration))
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
        tdLog.debug("start to insert data ............")
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
        tdLog.debug("insert data ............ [OK]")
        return

    def prepareTestEnv(self):
        tdLog.printNoPrefix("======== prepare test env include database, stable, ctables, and insert data: ")
        paraDict = {'dbName':     'test',
                    'dropFlag':   1,
                    'vgroups':    2,
                    'stbName':    'meters',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'FLOAT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'smallint', 'count':1},{'type': 'tinyint', 'count':1},{'type': 'bool', 'count':1},{'type': 'binary', 'len':10, 'count':1},{'type': 'nchar', 'len':10, 'count':1}],
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

    def check_partition_by_with_interval_fill_prev_new_group_fill_error(self):
        ## every table has 1500 rows after fill, 10 tables, total 15000 rows.
        ## there is no data from 9-17 08:00:00 ~ 9-17 09:00:00, so first 60 rows of every group will be NULL, cause no prev value.
        sql = "select _wstart, count(*),tbname from meters where ts > '2018-09-17 08:00:00.000' and ts < '2018-09-18 09:00:00.000' partition by tbname interval(1m) fill(PREV) order by tbname, _wstart"
        tdSql.query(sql)
        for i in range(0,10):
            for j in range(0,60):
                tdSql.checkData(i*1500+j, 1, None)

        sql = "select _wstart, count(*),tbname from meters where ts > '2018-09-17 08:00:00.000' and ts < '2018-09-18 09:00:00.000' partition by tbname interval(1m) fill(LINEAR) order by tbname, _wstart"
        tdSql.query(sql)
        for i in range(0,10):
            for j in range(0,60):
                tdSql.checkData(i*1500+j, 1, None)

    def check_fill_with_order_by(self):
        sql = "select _wstart, _wend, count(ts), sum(c1) from meters where ts > '2018-11-25 00:00:00.000' and ts < '2018-11-26 00:00:00.00' interval(1d) fill(NULL) order by _wstart"
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = "select _wstart, _wend, count(ts), sum(c1) from meters where ts > '2018-11-25 00:00:00.000' and ts < '2018-11-26 00:00:00.00' interval(1d) fill(NULL) order by _wstart desc"
        tdSql.query(sql)
        tdSql.checkRows(1)
        sql = "select _wstart, count(*) from meters where ts > '2018-08-20 00:00:00.000' and ts < '2018-09-30 00:00:00.000' interval(9d) fill(NULL) order by _wstart desc;"
        tdSql.query(sql)
        tdSql.checkRows(6)
        sql = "select _wstart, count(*) from meters where ts > '2018-08-20 00:00:00.000' and ts < '2018-09-30 00:00:00.000' interval(9d) fill(NULL) order by _wstart;"
        tdSql.query(sql)
        tdSql.checkRows(6)

    def check_fill_with_order_by2(self):
        ## window size: 5 minutes, with 6 rows in meters every 10 minutes
        sql = "select _wstart, count(*) from meters where ts >= '2018-09-20 00:00:00.000' and ts < '2018-09-20 01:00:00.000' interval(5m) fill(prev) order by _wstart asc;"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(12)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(1, 1, 10)
        tdSql.checkData(2, 1, 10)
        tdSql.checkData(3, 1, 10)
        tdSql.checkData(4, 1, 10)
        tdSql.checkData(5, 1, 10)
        tdSql.checkData(6, 1, 10)
        tdSql.checkData(7, 1, 10)
        tdSql.checkData(8, 1, 10)
        tdSql.checkData(9, 1, 10)
        tdSql.checkData(10, 1, 10)
        tdSql.checkData(11, 1, 10)

        sql = "select _wstart, count(*) from meters where ts >= '2018-09-20 00:00:00.000' and ts < '2018-09-20 01:00:00.000' interval(5m) fill(prev) order by _wstart desc;"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(12)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(1, 1, 10)
        tdSql.checkData(2, 1, 10)
        tdSql.checkData(3, 1, 10)
        tdSql.checkData(4, 1, 10)
        tdSql.checkData(5, 1, 10)
        tdSql.checkData(6, 1, 10)
        tdSql.checkData(7, 1, 10)
        tdSql.checkData(8, 1, 10)
        tdSql.checkData(9, 1, 10)
        tdSql.checkData(10, 1, 10)
        tdSql.checkData(11, 1, 10)

        sql = "select _wstart, count(*) from meters where ts >= '2018-09-20 00:00:00.000' and ts < '2018-09-20 01:00:00.000' interval(5m) fill(linear) order by _wstart desc;"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(12)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 1, 10)
        tdSql.checkData(2, 1, 10)
        tdSql.checkData(3, 1, 10)
        tdSql.checkData(4, 1, 10)
        tdSql.checkData(5, 1, 10)
        tdSql.checkData(6, 1, 10)
        tdSql.checkData(7, 1, 10)
        tdSql.checkData(8, 1, 10)
        tdSql.checkData(9, 1, 10)
        tdSql.checkData(10, 1, 10)
        tdSql.checkData(11, 1, 10)

        sql = "select _wstart, first(ts), last(ts) from meters where ts >= '2018-09-20 00:00:00.000' and ts < '2018-09-20 01:00:00.000' partition by t1 interval(5m) fill(NULL)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(60)

        sql = "select _wstart, count(*) from meters where ts >= '2018-09-19 23:54:00.000' and ts < '2018-09-20 01:00:00.000' interval(5m) fill(next) order by _wstart asc;"
        tdSql.query(sql, queryTimes=1)
        for i in range(0, 13):
            tdSql.checkData(i, 1, 10)
        tdSql.checkData(13, 1, None)
        sql = "select _wstart, count(*) from meters where ts >= '2018-09-19 23:54:00.000' and ts < '2018-09-20 01:00:00.000' interval(5m) fill(next) order by _wstart desc;"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkData(0, 1, None)
        for i in range(1, 14):
            tdSql.checkData(i, 1, 10)

        sql = "select _wstart, count(*) from meters where ts >= '2018-09-19 23:54:00.000' and ts < '2018-09-20 01:00:00.000' interval(5m) fill(prev) order by _wstart asc;"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 1, None)
        for i in range(2, 14):
            tdSql.checkData(i, 1, 10)
        sql = "select _wstart, count(*) from meters where ts >= '2018-09-19 23:54:00.000' and ts < '2018-09-20 01:00:00.000' interval(5m) fill(prev) order by _wstart desc;"
        tdSql.query(sql, queryTimes=1)
        for i in range(0, 12):
            tdSql.checkData(i, 1, 10)
        tdSql.checkData(12, 1, None)
        tdSql.checkData(13, 1, None)

        sql = "select _wstart, count(*) from meters where ts >= '2018-09-19 23:54:00.000' and ts < '2018-09-20 01:00:00.000' interval(5m) fill(linear) order by _wstart asc;"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 1, None)
        for i in range(2, 13):
            tdSql.checkData(i, 1, 10)
        tdSql.checkData(13, 1, None)
        sql = "select _wstart, count(*) from meters where ts >= '2018-09-19 23:54:00.000' and ts < '2018-09-20 01:00:00.000' interval(5m) fill(linear) order by _wstart desc;"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkData(0, 1, None)
        for i in range(1, 12):
            tdSql.checkData(i, 1, 10)
        tdSql.checkData(12, 1, None)
        tdSql.checkData(13, 1, None)

    def check_fill_with_complex_expr(self):
        sql = "SELECT _wstart, _wstart + 1d, count(*), now, 1+1 FROM meters WHERE ts >= '2018-09-20 00:00:00.000' AND ts < '2018-09-20 01:00:00.000' INTERVAL(5m) FILL(NULL)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(12)
        for i in range(0, 12, 2):
            tdSql.checkData(i, 2, 10)
        for i in range(1, 12, 2):
            tdSql.checkData(i, 2, None)
        for i in range(0, 12):
            firstCol = tdSql.getData(i, 0)
            secondCol = tdSql.getData(i, 1)
            tdLog.debug(f"firstCol: {firstCol}, secondCol: {secondCol}, secondCol - firstCol: {secondCol - firstCol}")
            if secondCol - firstCol != timedelta(days=1):
                tdLog.exit(f"query error: secondCol - firstCol: {secondCol - firstCol}")
            nowCol = tdSql.getData(i, 3)
            if nowCol is None:
                tdLog.exit(f"query error: nowCol: {nowCol}")
            constCol = tdSql.getData(i, 4)
            if constCol != 2:
                tdLog.exit(f"query error: constCol: {constCol}")

        sql = "SELECT _wstart + 1d, count(*), last(ts) + 1a, timediff(_wend, last(ts)) FROM meters WHERE ts >= '2018-09-20 00:00:00.000' AND ts < '2018-09-20 01:00:00.000' INTERVAL(5m) FILL(NULL)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(12)
        for i in range(0, 12, 2):
            tdSql.checkData(i, 1, 10)
            tdSql.checkData(i, 3, 300000)
        for i in range(1, 12, 2):
            tdSql.checkData(i, 1, None)
            tdSql.checkData(i, 2, None)
            tdSql.checkData(i, 3, None)

        sql = "SELECT count(*), tbname FROM meters WHERE ts >= '2018-09-20 00:00:00.000' AND ts < '2018-09-20 01:00:00.000' PARTITION BY tbname INTERVAL(5m) FILL(NULL)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(120)

        sql = "SELECT * from (SELECT count(*), timediff(_wend, last(ts)) + t1, tbname FROM meters WHERE ts >= '2018-09-20 00:00:00.000' AND ts < '2018-09-20 01:00:00.000' PARTITION BY tbname, t1 INTERVAL(5m) FILL(NULL) LIMIT 1) order by tbname"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(10)
        j = 0
        for i in range(0, 10):
            tdSql.checkData(i, 1, 300000 + j)
            j = j + 1
            if j == 5:
                j = 0

        sql = "SELECT count(*), timediff(_wend, last(ts)) + t1, tbname,t1 FROM meters WHERE ts >= '2018-09-20 00:00:00.000' AND ts < '2018-09-20 01:00:00.000' PARTITION BY tbname, t1 INTERVAL(5m) FILL(NULL) ORDER BY timediff(last(ts), _wstart)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(120)

        sql = "SELECT 1+1, count(*), timediff(_wend, last(ts)) + t1 FROM meters WHERE ts >= '2018-09-20 00:00:00.000' AND ts < '2018-09-20 01:00:00.000' PARTITION BY tbname, t1 INTERVAL(5m) FILL(NULL) HAVING(timediff(last(ts), _wstart)+ t1 >= 1)  ORDER BY timediff(last(ts), _wstart)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(48)

        sql = "SELECT count(*), timediff(_wend, last(ts)) + t1, timediff('2018-09-20 01:00:00', _wstart) + t1, concat(to_char(_wstart, 'HH:MI:SS__'), tbname) FROM meters WHERE ts >= '2018-09-20 00:00:00.000' AND ts < '2018-09-20 01:00:00.000' PARTITION BY tbname, t1 INTERVAL(5m) FILL(NULL) HAVING(timediff(last(ts), _wstart) + t1 >= 1)  ORDER BY timediff(last(ts), _wstart), tbname"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(48)

        sql = "SELECT count(*) FROM meters WHERE ts >= '2018-09-20 00:00:00.000' AND ts < '2018-09-20 01:00:00.000' PARTITION BY tbname, t1 INTERVAL(5m) FILL(NULL) HAVING(timediff(last(ts), _wstart) >= 0)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(60)

        sql = "SELECT count(*) + 1 FROM meters WHERE ts >= '2018-09-20 00:00:00.000' AND ts < '2018-09-20 01:00:00.000' PARTITION BY tbname, t1 INTERVAL(5m) FILL(NULL) HAVING(count(*) > 1)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(0)

        sql = "SELECT count(*), timediff(_wend, last(ts)) + t1, timediff('2018-09-20 01:00:00', _wstart) + t1, concat(to_char(_wstart, 'HH:MI:SS__'), tbname) FROM meters WHERE ts >= '2018-09-20 00:00:00.000' AND ts < '2018-09-20 01:00:00.000' PARTITION BY tbname, t1 INTERVAL(5m) FILL(value, 0, 0) HAVING(timediff(last(ts), _wstart) + t1 >= 1) ORDER BY timediff(last(ts), _wstart), tbname"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(48)
        sql = "SELECT count(*), timediff(_wend, last(ts)) + t1, timediff('2018-09-20 01:00:00', _wstart) + t1, concat(to_char(_wstart, 'HH:MI:SS__'), tbname) FROM meters WHERE ts >= '2018-09-20 00:00:00.000' AND ts < '2018-09-20 01:00:00.000' PARTITION BY tbname, t1 INTERVAL(5m) FILL(value, 0, 0) HAVING(count(*) >= 0) ORDER BY timediff(last(ts), _wstart), tbname"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(120)
        sql = "SELECT count(*), timediff(_wend, last(ts)) + t1, timediff('2018-09-20 01:00:00', _wstart) + t1, concat(to_char(_wstart, 'HH:MI:SS__'), tbname) FROM meters WHERE ts >= '2018-09-20 00:00:00.000' AND ts < '2018-09-20 01:00:00.000' PARTITION BY tbname, t1 INTERVAL(5m) FILL(value, 0, 0) HAVING(count(*) > 0) ORDER BY timediff(last(ts), _wstart), tbname"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(60)
        sql = "SELECT count(*), timediff(_wend, last(ts)) + t1, timediff('2018-09-20 01:00:00', _wstart) + t1, concat(to_char(_wstart, 'HH:MI:SS__'), tbname) FROM meters WHERE ts >= '2018-09-20 00:00:00.000' AND ts < '2018-09-20 01:00:00.000' PARTITION BY tbname INTERVAL(5m) FILL(linear) HAVING(count(*) >= 0 and t1 <= 1) ORDER BY timediff(last(ts), _wstart), tbname, t1"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(44)
        sql = "SELECT count(*), timediff(_wend, last(ts)) + t1, timediff('2018-09-20 01:00:00', _wstart) + t1, concat(to_char(_wstart, 'HH:MI:SS__'), tbname) FROM meters WHERE ts >= '2018-09-20 00:00:00.000' AND ts < '2018-09-20 01:00:00.000' PARTITION BY tbname INTERVAL(5m) FILL(prev) HAVING(count(*) >= 0 and t1 > 1) ORDER BY timediff(last(ts), _wstart), tbname, t1"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(72)

        sql = "SELECT 1+1, count(*), timediff(_wend, last(ts)) + t1, timediff('2018-09-20 01:00:00', _wstart) + t1, concat(to_char(_wstart, 'HH:MI:SS__'), tbname) FROM meters WHERE ts >= '2018-09-20 00:00:00.000' AND ts < '2018-09-20 01:00:00.000' PARTITION BY tbname INTERVAL(5m) FILL(linear) ORDER BY tbname, _wstart;"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(120)
        for i in range(11, 120, 12):
            tdSql.checkData(i, 1, None)
        for i in range(0, 120):
            tdSql.checkData(i, 0, 2)

        sql = "SELECT count(*), timediff(_wend, last(ts)) + t1, timediff('2018-09-20 01:00:00', _wstart) + t1, concat(to_char(_wstart, 'HH:MI:SS__'), tbname) FROM meters WHERE ts >= '2018-09-20 00:00:00.000' AND ts < '2018-09-20 01:00:00.000' PARTITION BY tbname INTERVAL(5m) FILL(linear) HAVING(count(*) >= 0) ORDER BY tbname;"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(110)
        for i in range(0, 110, 11):
            lastCol = tdSql.getData(i, 3)
            tdLog.debug(f"lastCol: {lastCol}")
            if lastCol[-1:] != str(i//11):
                tdLog.exit(f"query error: lastCol: {lastCol}")

        sql = "SELECT 1+1, count(*), timediff(_wend, last(ts)) + t1, timediff('2018-09-20 01:00:00', _wstart) + t1,t1 FROM meters WHERE ts >= '2018-09-20 00:00:00.000' AND ts < '2018-09-20 01:00:00.000' PARTITION BY t1 INTERVAL(5m) FILL(linear) ORDER BY t1, _wstart;"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(60)

        sql = "SELECT 1+1, count(*), timediff(_wend, last(ts)) + t1, timediff('2018-09-20 01:00:00', _wstart) + t1,t1 FROM meters WHERE ts >= '2018-09-20 00:00:00.000' AND ts < '2018-09-20 01:00:00.000' PARTITION BY t1 INTERVAL(5m) FILL(linear) HAVING(count(*) > 0) ORDER BY t1, _wstart;"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(55)

        sql = "SELECT count(*), timediff(_wend, last(ts)), timediff('2018-09-20 01:00:00', _wstart) FROM meters WHERE ts >= '2018-09-20 00:00:00.000' AND ts < '2018-09-20 01:00:00.000' PARTITION BY concat(tbname, 'asd') INTERVAL(5m) having(concat(tbname, 'asd') like '%asd');"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(60)

        sql = "SELECT count(*), timediff(_wend, last(ts)), timediff('2018-09-20 01:00:00', _wstart) FROM meters WHERE ts >= '2018-09-20 00:00:00.000' AND ts < '2018-09-20 01:00:00.000' PARTITION BY concat(tbname, 'asd') INTERVAL(5m) having(concat(tbname, 'asd') like 'asd%');"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(0)
        
        sql = "SELECT c1 FROM meters PARTITION BY c1 HAVING c1 > 0 slimit 2 limit 10"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(20)

        sql = "SELECT t1 FROM meters PARTITION BY t1 HAVING(t1 = 1)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(20000)

        sql = "SELECT concat(t2, 'asd') FROM meters PARTITION BY t2 HAVING(t2 like '%5')"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(10000)
        tdSql.checkData(0, 0, 'tb5asd')

        sql = "SELECT concat(t2, 'asd') FROM meters PARTITION BY concat(t2, 'asd') HAVING(concat(t2, 'asd')like '%5%')"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(10000)
        tdSql.checkData(0, 0, 'tb5asd')

        sql = "SELECT avg(c1) FROM meters PARTITION BY tbname, t1 HAVING(t1 = 1)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(2)

        sql = "SELECT count(*) FROM meters PARTITION BY concat(tbname, 'asd') HAVING(concat(tbname, 'asd') like '%asd')"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(10)

        sql = "SELECT count(*), concat(tbname, 'asd') FROM meters PARTITION BY concat(tbname, 'asd') HAVING(concat(tbname, 'asd') like '%asd')"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(10)

        sql = "SELECT count(*) FROM meters PARTITION BY t1 HAVING(t1 < 4) order by t1 +1"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(4)

        sql = "SELECT count(*), t1 + 100 FROM meters PARTITION BY t1 HAVING(t1 < 4) order by t1 +1"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(4)

        sql = "SELECT count(*), t1 + 100 FROM meters PARTITION BY t1 INTERVAL(1d) HAVING(t1 < 4) order by t1 +1 desc"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(280)

        sql = "SELECT count(*), concat(t3, 'asd') FROM meters PARTITION BY concat(t3, 'asd') INTERVAL(1d) HAVING(concat(t3, 'asd') like '%5asd' and count(*) = 118)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(1)

        sql = "SELECT count(*), concat(t3, 'asd') FROM meters PARTITION BY concat(t3, 'asd') INTERVAL(1d) HAVING(concat(t3, 'asd') like '%5asd' and count(*) != 118)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(69)

        sql = "SELECT count(*), concat(t3, 'asd') FROM meters PARTITION BY concat(t3, 'asd') INTERVAL(1d) HAVING(concat(t3, 'asd') like '%5asd') order by count(*) asc limit 10"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(10)

        sql = "SELECT count(*), concat(t3, 'asd') FROM meters PARTITION BY concat(t3, 'asd') INTERVAL(1d) HAVING(concat(t3, 'asd') like '%5asd' or concat(t3, 'asd') like '%3asd') order by count(*) asc limit 10000"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(140)

    def do_fill_with_group(self):
        self.init_class()
        self.prepareTestEnv()
        self.check_partition_by_with_interval_fill_prev_new_group_fill_error()
        self.check_fill_with_order_by()
        self.check_fill_with_order_by2()
        self.check_fill_with_complex_expr()
        
    #
    # ------------------- main ----------------
    #
    def test_ts_fill_method(self):
        """Fill method

        1. Create database with ns precision
        2. Create table with different data types
        3. Insert data with nulls
        4. Perform fill queries with different data types and methods (value, null, prev, next)
        5. Query with multi-threaded fill range
        6. Validate the filled results for each data type
        7. Fill with group by queries
        8. Fill with complex expressions

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-19 Alex Duan Migrated from uncatalog/system-test/2-query/test_fill.py
            - 2025-12-22 Alex Duan Migrated from uncatalog/system-test/2-query/test_fill_with_group.py

        """
        self.do_fill_datatype_method()
        self.do_fill_with_group()