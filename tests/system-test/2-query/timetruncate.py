import taos
from util.log import *
from util.cases import *
from util.sql import *
import numpy as np
import time
from datetime import datetime

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.rowNum = 10
        self.ts = 1537146000000  # 2018-9-17 09:00:00.000

        self.ts_str = [
            '2020-1-1',
            '2020-2-1 00:00:01',
            '2020-3-1 00:00:00.001',
            '2020-4-1 00:00:00.001002',
            '2020-5-1 00:00:00.001002001'

        ]
        self.db_param_precision = ['ms','us','ns']
        self.time_unit = ['1w','1d','1h','1m','1s','1a','1u']
    def get_ms_timestamp(self,ts_str):
        _ts_str = ts_str
        if " " in ts_str:
            p = ts_str.split(" ")[1]
            if len(p) > 15 :
                _ts_str = ts_str[:-3]
        if ':' in _ts_str and '.' in _ts_str:
            timestamp = datetime.strptime(_ts_str, "%Y-%m-%d %H:%M:%S.%f")
            date_time = int(int(time.mktime(timestamp.timetuple()))*1000 + timestamp.microsecond/1000)
        elif ':' in _ts_str and '.' not in _ts_str:
            timestamp = datetime.strptime(_ts_str, "%Y-%m-%d %H:%M:%S")
            date_time = int(int(time.mktime(timestamp.timetuple()))*1000 + timestamp.microsecond/1000)
        else:
            timestamp = datetime.strptime(_ts_str, "%Y-%m-%d")
            date_time = int(int(time.mktime(timestamp.timetuple()))*1000 + timestamp.microsecond/1000)
        return date_time
    def get_us_timestamp(self,ts_str):
        _ts = self.get_ms_timestamp(ts_str) * 1000
        if " " in ts_str:
            p = ts_str.split(" ")[1]
            if len(p) > 12:
                us_ts = p[12:15]
                _ts += int(us_ts)
        return _ts
    def get_ns_timestamp(self,ts_str):
        _ts = self.get_us_timestamp(ts_str) *1000
        if " " in ts_str:
            p = ts_str.split(" ")[1]
            if len(p) > 15:
                us_ts = p[15:]
                _ts += int(us_ts)
        return _ts
    def time_transform(self,ts_str,precision):
        date_time = []
        if precision == 'ms':
            for i in ts_str:
                date_time.append(self.get_ms_timestamp(i))
        elif precision == 'us':
            for i in ts_str:
                date_time.append(self.get_us_timestamp(i))
        elif precision == 'ns':
            for i in ts_str:
                date_time.append(self.get_us_timestamp(i))
        return date_time
    def function_check_ntb(self):
        
        for precision in self.db_param_precision:
            tdSql.execute('drop database if exists db')
            tdSql.execute(f'create database db precision "{precision}"')
            tdSql.execute('use db')
            tdSql.execute('create table ntb (ts timestamp,c0 int)')
            for ts in self.ts_str:
                tdSql.execute(f'insert into ntb values("{ts}",1)')
            date_time = self.time_transform(self.ts_str,precision)
            
            for unit in self.time_unit:
                if (unit.lower() == '1u' and precision.lower() == 'ms') or () :
                    tdSql.error(f'select timetruncate(ts,{unit}) from ntb')
                elif precision.lower() == 'ms':
                    tdSql.query(f'select timetruncate(ts,{unit}) from ntb')
                    if unit.lower() == '1a':
                        for i in range(len(self.ts_str)):
                            ts_result = self.get_ms_timestamp(str(tdSql.queryResult[i][0]))
                            tdSql.checkEqual(ts_result,int(date_time[i]))
                    elif unit.lower() == '1s':
                        for i in range(len(self.ts_str)):
                            ts_result = self.get_ms_timestamp(str(tdSql.queryResult[i][0]))
                            tdSql.checkEqual(ts_result,int(date_time[i]/1000)*1000)
                    elif unit.lower() == '1m':
                        for i in range(len(self.ts_str)):
                            ts_result = self.get_ms_timestamp(str(tdSql.queryResult[i][0]))
                            tdSql.checkEqual(ts_result,int(date_time[i]/1000/60)*60*1000)
                    elif unit.lower() == '1h':
                        for i in range(len(self.ts_str)):
                            ts_result = self.get_ms_timestamp(str(tdSql.queryResult[i][0]))
                            tdSql.checkEqual(ts_result,int(date_time[i]/1000/60/60)*60*60*1000  )
                    elif unit.lower() == '1d':
                        for i in range(len(self.ts_str)):
                            ts_result = self.get_ms_timestamp(str(tdSql.queryResult[i][0]))
                            tdSql.checkEqual(ts_result,int(date_time[i]/1000/60/60/24)*24*60*60*1000)  
                    elif unit.lower() == '1w':
                        for i in range(len(self.ts_str)):
                            ts_result = self.get_ms_timestamp(str(tdSql.queryResult[i][0]))
                            tdSql.checkEqual(ts_result,int(date_time[i]/1000/60/60/24/7)*7*24*60*60*1000)         
                elif precision.lower() == 'us':
                    tdSql.query(f'select timetruncate(ts,{unit}) from ntb')
                    if unit.lower() == '1u':
                        for i in range(len(self.ts_str)):
                            ts_result = self.get_us_timestamp(str(tdSql.queryResult[i][0]))
                            tdSql.checkEqual(ts_result,int(date_time[i]))
                    elif unit.lower() == '1a':
                        for i in range(len(self.ts_str)):
                            ts_result = self.get_us_timestamp(str(tdSql.queryResult[i][0]))
                            tdSql.checkEqual(ts_result,int(date_time[i]/1000)*1000)
                    elif unit.lower() == '1s':
                        for i in range(len(self.ts_str)):
                            ts_result = self.get_us_timestamp(str(tdSql.queryResult[i][0]))
                            tdSql.checkEqual(ts_result,int(date_time[i]/1000/1000)*1000*1000)
                    elif unit.lower() == '1m':
                        for i in range(len(self.ts_str)):
                            ts_result = self.get_us_timestamp(str(tdSql.queryResult[i][0]))
                            tdSql.checkEqual(ts_result,int(date_time[i]/1000/1000/60)*60*1000*1000)
                    elif unit.lower() == '1h':
                        for i in range(len(self.ts_str)):
                            ts_result = self.get_us_timestamp(str(tdSql.queryResult[i][0]))
                            tdSql.checkEqual(ts_result,int(date_time[i]/1000/1000/60/60)*60*60*1000*1000  )
                    elif unit.lower() == '1d':
                        for i in range(len(self.ts_str)):
                            ts_result = self.get_us_timestamp(str(tdSql.queryResult[i][0]))
                            tdSql.checkEqual(ts_result,int(date_time[i]/1000/1000/60/60/24)*24*60*60*1000*1000 ) 
                    elif unit.lower() == '1w':
                        for i in range(len(self.ts_str)):
                            print(tdSql.queryResult[i][0])
                            ts_result = self.get_us_timestamp(str(tdSql.queryResult[i][0]))
                            tdSql.checkEqual(ts_result,int(date_time[i]/1000/1000/60/60/24/7)*7*24*60*60*1000*1000)



    def run(self):
        self.function_check_ntb()
        # tdSql.prepare()

        # intData = []
        # floatData = []

        # tdSql.execute('''create table stb(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double, 
        #             col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned) tags(loc nchar(20))''')
        # tdSql.execute("create table stb_1 using stb tags('beijing')")
        # tdSql.execute('''create table ntb(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double, 
        #             col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned)''')
        # for i in range(self.rowNum):
        #     tdSql.execute("insert into ntb values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)" 
        #                 % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))
        #     intData.append(i + 1)
        #     floatData.append(i + 0.1)
        # for i in range(self.rowNum):
        #     tdSql.execute("insert into stb_1 values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)" 
        #                 % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))
        #     intData.append(i + 1)
        #     floatData.append(i + 0.1)

        # tdSql.query("select timetruncate(1,1d) from ntb")
        # tdSql.checkRows(10)
        # tdSql.error("select timetruncate(1,1u) from ntb")
        # #tdSql.checkRows(10)
        # tdSql.query("select timetruncate(1,1a) from ntb")
        # tdSql.checkRows(10)
        # tdSql.query("select timetruncate(1,1m) from ntb")
        # tdSql.checkRows(10)
        # tdSql.query("select timetruncate(1,1h) from ntb")
        # tdSql.checkRows(10)
        # tdSql.query("select timetruncate(ts,1d) from ntb")
        # tdSql.checkRows(10)
        # tdSql.checkData(0,0,"2018-09-17 08:00:00.000")
        # tdSql.query("select timetruncate(ts,1h) from ntb")
        # tdSql.checkRows(10)
        # tdSql.checkData(0,0,"2018-09-17 09:00:00.000")
        # tdSql.query("select timetruncate(ts,1m) from ntb")
        # tdSql.checkRows(10)
        # tdSql.checkData(0,0,"2018-09-17 09:00:00.000")
        # tdSql.query("select timetruncate(ts,1s) from ntb")
        # tdSql.checkRows(10)
        # tdSql.checkData(0,0,"2018-09-17 09:00:00.000")
        # tdSql.query("select timetruncate(ts,1a) from ntb")
        # tdSql.checkRows(10)
        # tdSql.checkData(0,0,"2018-09-17 09:00:00.000")
        # tdSql.checkData(1,0,"2018-09-17 09:00:00.001")
        # tdSql.checkData(2,0,"2018-09-17 09:00:00.002")
        # tdSql.checkData(3,0,"2018-09-17 09:00:00.003")
        # tdSql.checkData(4,0,"2018-09-17 09:00:00.004")
        # tdSql.checkData(5,0,"2018-09-17 09:00:00.005")
        # tdSql.checkData(6,0,"2018-09-17 09:00:00.006")
        # tdSql.checkData(7,0,"2018-09-17 09:00:00.007")
        # tdSql.checkData(8,0,"2018-09-17 09:00:00.008")
        # tdSql.checkData(9,0,"2018-09-17 09:00:00.009")
        # # tdSql.query("select timetruncate(ts,1u) from ntb")
        # # tdSql.checkRows(10)
        # # tdSql.checkData(0,0,"2018-09-17 09:00:00.000000")
        # # tdSql.checkData(1,0,"2018-09-17 09:00:00.001000")
        # # tdSql.checkData(2,0,"2018-09-17 09:00:00.002000")
        # # tdSql.checkData(3,0,"2018-09-17 09:00:00.003000")
        # # tdSql.checkData(4,0,"2018-09-17 09:00:00.004000")
        # # tdSql.checkData(5,0,"2018-09-17 09:00:00.005000")
        # # tdSql.checkData(6,0,"2018-09-17 09:00:00.006000")
        # # tdSql.checkData(7,0,"2018-09-17 09:00:00.007000")
        # # tdSql.checkData(8,0,"2018-09-17 09:00:00.008000")
        # # tdSql.checkData(9,0,"2018-09-17 09:00:00.009000")
        # # tdSql.query("select timetruncate(ts,1b) from ntb")
        # # tdSql.checkRows(10)
        # # tdSql.checkData(0,0,"2018-09-17 09:00:00.000000000")
        # # tdSql.checkData(1,0,"2018-09-17 09:00:00.001000000")
        # # tdSql.checkData(2,0,"2018-09-17 09:00:00.002000000")
        # # tdSql.checkData(3,0,"2018-09-17 09:00:00.003000000")
        # # tdSql.checkData(4,0,"2018-09-17 09:00:00.004000000")
        # # tdSql.checkData(5,0,"2018-09-17 09:00:00.005000000")
        # # tdSql.checkData(6,0,"2018-09-17 09:00:00.006000000")
        # # tdSql.checkData(7,0,"2018-09-17 09:00:00.007000000")
        # # tdSql.checkData(8,0,"2018-09-17 09:00:00.008000000")
        # # tdSql.checkData(9,0,"2018-09-17 09:00:00.009000000")


        # tdSql.query("select timetruncate(1,1d) from stb")
        # tdSql.checkRows(10)
        # tdSql.error("select timetruncate(1,1u) from stb")
        # #tdSql.checkRows(10)
        # tdSql.query("select timetruncate(1,1a) from stb")
        # tdSql.checkRows(10)
        # tdSql.query("select timetruncate(1,1m) from stb")
        # tdSql.checkRows(10)
        # tdSql.query("select timetruncate(1,1h) from stb")
        # tdSql.checkRows(10)
        # tdSql.query("select timetruncate(ts,1d) from stb")
        # tdSql.checkRows(10)
        # tdSql.checkData(0,0,"2018-09-17 08:00:00.000")
        # tdSql.query("select timetruncate(ts,1h) from stb")
        # tdSql.checkRows(10)
        # tdSql.checkData(0,0,"2018-09-17 09:00:00.000")
        # tdSql.query("select timetruncate(ts,1m) from stb")
        # tdSql.checkRows(10)
        # tdSql.checkData(0,0,"2018-09-17 09:00:00.000")
        # tdSql.query("select timetruncate(ts,1s) from stb")
        # tdSql.checkRows(10)
        # tdSql.checkData(0,0,"2018-09-17 09:00:00.000")
        # tdSql.query("select timetruncate(ts,1a) from stb")
        # tdSql.checkRows(10)
        # tdSql.checkData(0,0,"2018-09-17 09:00:00.000")
        # tdSql.checkData(1,0,"2018-09-17 09:00:00.001")
        # tdSql.checkData(2,0,"2018-09-17 09:00:00.002")
        # tdSql.checkData(3,0,"2018-09-17 09:00:00.003")
        # tdSql.checkData(4,0,"2018-09-17 09:00:00.004")
        # tdSql.checkData(5,0,"2018-09-17 09:00:00.005")
        # tdSql.checkData(6,0,"2018-09-17 09:00:00.006")
        # tdSql.checkData(7,0,"2018-09-17 09:00:00.007")
        # tdSql.checkData(8,0,"2018-09-17 09:00:00.008")
        # tdSql.checkData(9,0,"2018-09-17 09:00:00.009")
        # # tdSql.query("select timetruncate(ts,1u) from stb")
        # # tdSql.checkRows(10)
        # # tdSql.checkData(0,0,"2018-09-17 09:00:00.000000")
        # # tdSql.checkData(1,0,"2018-09-17 09:00:00.001000")
        # # tdSql.checkData(2,0,"2018-09-17 09:00:00.002000")
        # # tdSql.checkData(3,0,"2018-09-17 09:00:00.003000")
        # # tdSql.checkData(4,0,"2018-09-17 09:00:00.004000")
        # # tdSql.checkData(5,0,"2018-09-17 09:00:00.005000")
        # # tdSql.checkData(6,0,"2018-09-17 09:00:00.006000")
        # # tdSql.checkData(7,0,"2018-09-17 09:00:00.007000")
        # # tdSql.checkData(8,0,"2018-09-17 09:00:00.008000")
        # # tdSql.checkData(9,0,"2018-09-17 09:00:00.009000")
        # # tdSql.query("select timetruncate(ts,1b) from stb")
        # # tdSql.checkRows(10)
        # # tdSql.checkData(0,0,"2018-09-17 09:00:00.000000000")
        # # tdSql.checkData(1,0,"2018-09-17 09:00:00.001000000")
        # # tdSql.checkData(2,0,"2018-09-17 09:00:00.002000000")
        # # tdSql.checkData(3,0,"2018-09-17 09:00:00.003000000")
        # # tdSql.checkData(4,0,"2018-09-17 09:00:00.004000000")
        # # tdSql.checkData(5,0,"2018-09-17 09:00:00.005000000")
        # # tdSql.checkData(6,0,"2018-09-17 09:00:00.006000000")
        # # tdSql.checkData(7,0,"2018-09-17 09:00:00.007000000")
        # # tdSql.checkData(8,0,"2018-09-17 09:00:00.008000000")
        # # tdSql.checkData(9,0,"2018-09-17 09:00:00.009000000")

        # tdSql.query("select timetruncate(1,1d) from stb_1")
        # tdSql.checkRows(10)
        # tdSql.error("select timetruncate(1,1u) from stb_1")
        # #tdSql.checkRows(10)
        # tdSql.query("select timetruncate(1,1a) from stb_1")
        # tdSql.checkRows(10)
        # tdSql.query("select timetruncate(1,1m) from stb_1")
        # tdSql.checkRows(10)
        # tdSql.query("select timetruncate(1,1h) from stb_1")
        # tdSql.checkRows(10)
        # tdSql.query("select timetruncate(ts,1d) from stb_1")
        # tdSql.checkRows(10)
        # tdSql.checkData(0,0,"2018-09-17 08:00:00.000")
        # tdSql.query("select timetruncate(ts,1h) from stb_1")
        # tdSql.checkRows(10)
        # tdSql.checkData(0,0,"2018-09-17 09:00:00.000")
        # tdSql.query("select timetruncate(ts,1m) from stb_1")
        # tdSql.checkRows(10)
        # tdSql.checkData(0,0,"2018-09-17 09:00:00.000")
        # tdSql.query("select timetruncate(ts,1s) from stb_1")
        # tdSql.checkRows(10)
        # tdSql.checkData(0,0,"2018-09-17 09:00:00.000")
        # tdSql.query("select timetruncate(ts,1a) from stb_1")
        # tdSql.checkRows(10)
        # tdSql.checkData(0,0,"2018-09-17 09:00:00.000")
        # tdSql.checkData(1,0,"2018-09-17 09:00:00.001")
        # tdSql.checkData(2,0,"2018-09-17 09:00:00.002")
        # tdSql.checkData(3,0,"2018-09-17 09:00:00.003")
        # tdSql.checkData(4,0,"2018-09-17 09:00:00.004")
        # tdSql.checkData(5,0,"2018-09-17 09:00:00.005")
        # tdSql.checkData(6,0,"2018-09-17 09:00:00.006")
        # tdSql.checkData(7,0,"2018-09-17 09:00:00.007")
        # tdSql.checkData(8,0,"2018-09-17 09:00:00.008")
        # tdSql.checkData(9,0,"2018-09-17 09:00:00.009")
        # # tdSql.query("select timetruncate(ts,1u) from stb_1")
        # # tdSql.checkRows(10)
        # # tdSql.checkData(0,0,"2018-09-17 09:00:00.000000")
        # # tdSql.checkData(1,0,"2018-09-17 09:00:00.001000")
        # # tdSql.checkData(2,0,"2018-09-17 09:00:00.002000")
        # # tdSql.checkData(3,0,"2018-09-17 09:00:00.003000")
        # # tdSql.checkData(4,0,"2018-09-17 09:00:00.004000")
        # # tdSql.checkData(5,0,"2018-09-17 09:00:00.005000")
        # # tdSql.checkData(6,0,"2018-09-17 09:00:00.006000")
        # # tdSql.checkData(7,0,"2018-09-17 09:00:00.007000")
        # # tdSql.checkData(8,0,"2018-09-17 09:00:00.008000")
        # # tdSql.checkData(9,0,"2018-09-17 09:00:00.009000")
        # # tdSql.query("select timetruncate(ts,1b) from stb_1")
        # # tdSql.checkRows(10)
        # # tdSql.checkData(0,0,"2018-09-17 09:00:00.000000000")
        # # tdSql.checkData(1,0,"2018-09-17 09:00:00.001000000")
        # # tdSql.checkData(2,0,"2018-09-17 09:00:00.002000000")
        # # tdSql.checkData(3,0,"2018-09-17 09:00:00.003000000")
        # # tdSql.checkData(4,0,"2018-09-17 09:00:00.004000000")
        # # tdSql.checkData(5,0,"2018-09-17 09:00:00.005000000")
        # # tdSql.checkData(6,0,"2018-09-17 09:00:00.006000000")
        # # tdSql.checkData(7,0,"2018-09-17 09:00:00.007000000")
        # # tdSql.checkData(8,0,"2018-09-17 09:00:00.008000000")
        # # tdSql.checkData(9,0,"2018-09-17 09:00:00.009000000")
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
