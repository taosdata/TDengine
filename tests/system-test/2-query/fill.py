import queue
import random
from fabric2.runners import threading
from pandas._libs import interval
import taos
import sys

from util.common import TDCom
from util.log import *
from util.sql import *
from util.cases import *



class TDTestCase:
    updatecfgDict = {'asynclog': 0, 'ttlUnit': 1, 'ttlPushInterval': 5, 'ratioOfVnodeStreamThrea': 4, 'numOfVnodeQueryThreads': 80}

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor())
        tdSql.init(conn.cursor(), logSql)  # output sql.txt file

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

    def test_fill_range(self):
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

    def run(self):
        self.test_fill_range()
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
    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
