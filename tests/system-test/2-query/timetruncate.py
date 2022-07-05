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
        #self.error_unit = ['1b','2w','2d','2h','2m','2s','2a','2u','1c','#1']
        self.error_unit = ['2w','2d','2h','2m','2s','2a','2u','1c','#1']
        self.ntbname = 'ntb'
        self.stbname = 'stb'
        self.ctbname = 'ctb'
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
    def check_ms_timestamp(self,unit,date_time):
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
    def check_us_timestamp(self,unit,date_time):
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
                ts_result = self.get_us_timestamp(str(tdSql.queryResult[i][0]))
                tdSql.checkEqual(ts_result,int(date_time[i]/1000/1000/60/60/24/7)*7*24*60*60*1000*1000)
    def check_ns_timestamp(self,unit,date_time):
        if unit.lower() == '1u':
            for i in range(len(self.ts_str)):
                tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]*1000/1000)*1000)
        elif unit.lower() == '1a':
            for i in range(len(self.ts_str)):
                tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]*1000/1000/1000)*1000*1000)
        elif unit.lower() == '1s':
            for i in range(len(self.ts_str)):
                tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]*1000/1000/1000/1000)*1000*1000*1000)
        elif unit.lower() == '1m':
            for i in range(len(self.ts_str)):
                tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]*1000/1000/1000/1000/60)*60*1000*1000*1000)
        elif unit.lower() == '1h':
            for i in range(len(self.ts_str)):
                tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]*1000/1000/1000/1000/60/60)*60*60*1000*1000*1000  )
        elif unit.lower() == '1d':
            for i in range(len(self.ts_str)):
                tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]*1000/1000/1000/1000/60/60/24)*24*60*60*1000*1000*1000 )
        elif unit.lower() == '1w':
            for i in range(len(self.ts_str)):
                tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]*1000/1000/1000/1000/60/60/24/7)*7*24*60*60*1000*1000*1000)
    def data_check(self,date_time,precision,tb_type):
        for unit in self.time_unit:
            if (unit.lower() == '1u' and precision.lower() == 'ms') or () :
                if tb_type.lower() == 'ntb':
                    tdSql.error(f'select timetruncate(ts,{unit}) from {self.ntbname}')
                elif tb_type.lower() == 'ctb':
                    tdSql.error(f'select timetruncate(ts,{unit}) from {self.ctbname}')
                elif tb_type.lower() == 'stb':
                    tdSql.error(f'select timetruncate(ts,{unit}) from {self.stbname}')
            elif precision.lower() == 'ms':
                if tb_type.lower() == 'ntb':
                    tdSql.query(f'select timetruncate(ts,{unit}) from {self.ntbname}')
                elif tb_type.lower() == 'ctb':
                    tdSql.query(f'select timetruncate(ts,{unit}) from {self.ctbname}')
                elif tb_type.lower() == 'stb':
                    tdSql.query(f'select timetruncate(ts,{unit}) from {self.stbname}')
                tdSql.checkRows(len(self.ts_str))
                self.check_ms_timestamp(unit,date_time)
            elif precision.lower() == 'us':
                if tb_type.lower() == 'ntb':
                    tdSql.query(f'select timetruncate(ts,{unit}) from {self.ntbname}')
                elif tb_type.lower() == 'ctb':
                    tdSql.query(f'select timetruncate(ts,{unit}) from {self.ctbname}')
                elif tb_type.lower() == 'stb':
                    tdSql.query(f'select timetruncate(ts,{unit}) from {self.stbname}')
                tdSql.checkRows(len(self.ts_str))
                self.check_us_timestamp(unit,date_time)
            elif precision.lower() == 'ns':
                if tb_type.lower() == 'ntb':
                    tdSql.query(f'select timetruncate(ts,{unit}) from {self.ntbname}')
                elif tb_type.lower() == 'ctb':
                    tdSql.query(f'select timetruncate(ts,{unit}) from {self.ctbname}')
                elif tb_type.lower() == 'stb':
                    tdSql.query(f'select timetruncate(ts,{unit}) from {self.stbname}')
                tdSql.checkRows(len(self.ts_str))
                self.check_ns_timestamp(unit,date_time)
        for unit in self.error_unit:
            if tb_type.lower() == 'ntb':
                tdSql.error(f'select timetruncate(ts,{unit}) from {self.ntbname}')
            elif tb_type.lower() == 'ctb':
                tdSql.error(f'select timetruncate(ts,{unit}) from {self.ctbname}')
            elif tb_type.lower() == 'stb':
                tdSql.error(f'select timetruncate(ts,{unit}) from {self.stbname}')
    def function_check_ntb(self):
        for precision in self.db_param_precision:
            tdSql.execute('drop database if exists db')
            tdSql.execute(f'create database db precision "{precision}"')
            tdSql.execute('use db')
            tdSql.execute(f'create table {self.ntbname} (ts timestamp,c0 int)')
            for ts in self.ts_str:
                tdSql.execute(f'insert into {self.ntbname} values("{ts}",1)')
            date_time = self.time_transform(self.ts_str,precision)
            self.data_check(date_time,precision,'ntb')

    def function_check_stb(self):
        for precision in self.db_param_precision:
            tdSql.execute('drop database if exists db')
            tdSql.execute(f'create database db precision "{precision}"')
            tdSql.execute('use db')
            tdSql.execute(f'create table {self.stbname} (ts timestamp,c0 int) tags(t0 int)')
            tdSql.execute(f'create table {self.ctbname} using {self.stbname} tags(1)')
            for ts in self.ts_str:
                tdSql.execute(f'insert into {self.ctbname} values("{ts}",1)')
            date_time = self.time_transform(self.ts_str,precision)
            self.data_check(date_time,precision,'ctb')
            self.data_check(date_time,precision,'stb')
    def run(self):
        self.function_check_ntb()
        self.function_check_stb()
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
