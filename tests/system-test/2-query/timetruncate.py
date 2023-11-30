import taos
from util.log import *
from util.cases import *
from util.sql import *
import numpy as np
import time
from datetime import datetime
from util.gettime import *
class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        print(conn)
        self.rest_tag = str(conn).lower().split('.')[0].replace("<taos","")
        print(self.rest_tag)
        self.get_time = GetTime()
        self.ts_str = [
            '2020-1-1',
            '2020-2-1 00:00:01',
            '2020-3-1 00:00:00.001',
            '2020-4-1 00:00:00.001002',
            '2020-5-1 00:00:00.001002001'
        ]
        self.db_param_precision = ['ms','us','ns']
        self.time_unit = ['1w','1d','1h','1m','1s','1a','1u','1b']
        self.error_unit = ['2w','2d','2h','2m','2s','2a','2u','1c','#1']
        self.dbname = 'db'
        self.ntbname = f'{self.dbname}.ntb'
        self.stbname = f'{self.dbname}.stb'
        self.ctbname = f'{self.dbname}.ctb'

    def check_ms_timestamp(self,unit,date_time, ignore_tz):
        if unit.lower() == '1a':
            for i in range(len(self.ts_str)):
                ts_result = self.get_time.get_ms_timestamp(str(tdSql.queryResult[i][0]))
                tdSql.checkEqual(ts_result,int(date_time[i]))
        elif unit.lower() == '1s':
            for i in range(len(self.ts_str)):
                ts_result = self.get_time.get_ms_timestamp(str(tdSql.queryResult[i][0]))
                tdSql.checkEqual(ts_result,int(date_time[i]/1000)*1000)
        elif unit.lower() == '1m':
            for i in range(len(self.ts_str)):
                ts_result = self.get_time.get_ms_timestamp(str(tdSql.queryResult[i][0]))
                tdSql.checkEqual(ts_result,int(date_time[i]/1000/60)*60*1000)
        elif unit.lower() == '1h':
            for i in range(len(self.ts_str)):
                ts_result = self.get_time.get_ms_timestamp(str(tdSql.queryResult[i][0]))
                tdSql.checkEqual(ts_result,int(date_time[i]/1000/60/60)*60*60*1000  )
        elif unit.lower() == '1d':
            for i in range(len(self.ts_str)):
                ts_result = self.get_time.get_ms_timestamp(str(tdSql.queryResult[i][0]))
                if ignore_tz == 0:
                    tdSql.checkEqual(ts_result,int(date_time[i]/1000/60/60/24)*24*60*60*1000)
                else:
                    # assuming the client timezone is UTC+0800
                    tdSql.checkEqual(ts_result,int(date_time[i] - (date_time[i] + 8 * 3600 * 1000) % (86400 * 1000)))
        elif unit.lower() == '1w':
            for i in range(len(self.ts_str)):
                ts_result = self.get_time.get_ms_timestamp(str(tdSql.queryResult[i][0]))
                if ignore_tz == 0:
                    tdSql.checkEqual(ts_result,int(date_time[i]/1000/60/60/24/7)*7*24*60*60*1000)
                else:
                    # assuming the client timezone is UTC+0800
                    tdSql.checkEqual(ts_result,int(date_time[i] - (date_time[i] + 8 * 3600 * 1000) % (86400 * 7 * 1000)))

    def check_us_timestamp(self,unit,date_time, ignore_tz):
        if unit.lower() == '1u':
            for i in range(len(self.ts_str)):
                ts_result = self.get_time.get_us_timestamp(str(tdSql.queryResult[i][0]))
                tdSql.checkEqual(ts_result,int(date_time[i]))
        elif unit.lower() == '1a':
            for i in range(len(self.ts_str)):
                ts_result = self.get_time.get_us_timestamp(str(tdSql.queryResult[i][0]))
                tdSql.checkEqual(ts_result,int(date_time[i]/1000)*1000)
        elif unit.lower() == '1s':
            for i in range(len(self.ts_str)):
                ts_result = self.get_time.get_us_timestamp(str(tdSql.queryResult[i][0]))
                tdSql.checkEqual(ts_result,int(date_time[i]/1000/1000)*1000*1000)
        elif unit.lower() == '1m':
            for i in range(len(self.ts_str)):
                ts_result = self.get_time.get_us_timestamp(str(tdSql.queryResult[i][0]))
                tdSql.checkEqual(ts_result,int(date_time[i]/1000/1000/60)*60*1000*1000)
        elif unit.lower() == '1h':
            for i in range(len(self.ts_str)):
                ts_result = self.get_time.get_us_timestamp(str(tdSql.queryResult[i][0]))
                tdSql.checkEqual(ts_result,int(date_time[i]/1000/1000/60/60)*60*60*1000*1000  )
        elif unit.lower() == '1d':
            for i in range(len(self.ts_str)):
                ts_result = self.get_time.get_us_timestamp(str(tdSql.queryResult[i][0]))
                if ignore_tz == 0:
                    tdSql.checkEqual(ts_result,int(date_time[i]/1000/1000/60/60/24)*24*60*60*1000*1000 )
                else:
                    # assuming the client timezone is UTC+0800
                    tdSql.checkEqual(ts_result,int(date_time[i] - (date_time[i] + 8 * 3600 * 1000000) % (86400 * 1000000)))
        elif unit.lower() == '1w':
            for i in range(len(self.ts_str)):
                ts_result = self.get_time.get_us_timestamp(str(tdSql.queryResult[i][0]))
                if ignore_tz == 0:
                    tdSql.checkEqual(ts_result,int(date_time[i]/1000/1000/60/60/24/7)*7*24*60*60*1000*1000)
                else:
                    # assuming the client timezone is UTC+0800
                    tdSql.checkEqual(ts_result,int(date_time[i] - (date_time[i] + 8 * 3600 * 1000000) % (86400 * 7 * 1000000)))

    def check_ns_timestamp(self,unit,date_time, ignore_tz):
        if unit.lower() == '1b':
            for i in range(len(self.ts_str)):
                if self.rest_tag != 'rest':
                    tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]))
        elif unit.lower() == '1u':
            for i in range(len(self.ts_str)):
                if self.rest_tag != 'rest':
                    tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]*1000/1000/1000)*1000)
        elif unit.lower() == '1a':
            for i in range(len(self.ts_str)):
                if self.rest_tag != 'rest':
                    tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]*1000/1000/1000/1000)*1000*1000)
        elif unit.lower() == '1s':
            for i in range(len(self.ts_str)):
                if self.rest_tag != 'rest':
                    tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]*1000/1000/1000/1000/1000)*1000*1000*1000)
        elif unit.lower() == '1m':
            for i in range(len(self.ts_str)):
                if self.rest_tag != 'rest':
                    tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]*1000/1000/1000/1000/1000/60)*60*1000*1000*1000)
        elif unit.lower() == '1h':
            for i in range(len(self.ts_str)):
                if self.rest_tag != 'rest':
                    tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]*1000/1000/1000/1000/1000/60/60)*60*60*1000*1000*1000  )
        elif unit.lower() == '1d':
            for i in range(len(self.ts_str)):
                if self.rest_tag != 'rest':
                    if ignore_tz == 0:
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]*1000/1000/1000/1000/1000/60/60/24)*24*60*60*1000*1000*1000 )
                    else:
                        # assuming the client timezone is UTC+0800
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i] - (date_time[i] + 8 * 3600 * 1000000) % (86400 * 1000000)))
        elif unit.lower() == '1w':
            for i in range(len(self.ts_str)):
                if self.rest_tag != 'rest':
                    if ignore_tz == 0:
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]*1000/1000/1000/1000/1000/60/60/24/7)*7*24*60*60*1000*1000*1000)
                    else:
                        # assuming the client timezone is UTC+0800
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i] - (date_time[i] + 8 * 3600  * 1000000000) % (86400 * 7 * 1000000000)))

    def check_tb_type(self,unit,tb_type,ignore_tz):
        if tb_type.lower() == 'ntb':
            tdSql.query(f'select timetruncate(ts,{unit},{ignore_tz}) from {self.ntbname}')
        elif tb_type.lower() == 'ctb':
            tdSql.query(f'select timetruncate(ts,{unit},{ignore_tz}) from {self.ctbname}')
        elif tb_type.lower() == 'stb':
            tdSql.query(f'select timetruncate(ts,{unit},{ignore_tz}) from {self.stbname}')

    def data_check(self,date_time,precision,tb_type):
        tz_options = [0, 1]
        for unit in self.time_unit:
            if (unit.lower() == '1u' and precision.lower() == 'ms') or (unit.lower() == '1b' and precision.lower() == 'us') or (unit.lower() == '1b' and precision.lower() == 'ms'):
                if tb_type.lower() == 'ntb':
                    tdSql.error(f'select timetruncate(ts,{unit}) from {self.ntbname}')
                elif tb_type.lower() == 'ctb':
                    tdSql.error(f'select timetruncate(ts,{unit}) from {self.ctbname}')
                elif tb_type.lower() == 'stb':
                    tdSql.error(f'select timetruncate(ts,{unit}) from {self.stbname}')
            elif precision.lower() == 'ms':
                for ignore_tz in tz_options:
                    self.check_tb_type(unit,tb_type,ignore_tz)
                    tdSql.checkRows(len(self.ts_str))
                    self.check_ms_timestamp(unit,date_time,ignore_tz)
            elif precision.lower() == 'us':
                for ignore_tz in tz_options:
                    self.check_tb_type(unit,tb_type,ignore_tz)
                    tdSql.checkRows(len(self.ts_str))
                    self.check_us_timestamp(unit,date_time,ignore_tz)
            elif precision.lower() == 'ns':
                for ignore_tz in tz_options:
                    self.check_tb_type(unit,tb_type, ignore_tz)
                    tdSql.checkRows(len(self.ts_str))
                    self.check_ns_timestamp(unit,date_time,ignore_tz)
        for unit in self.error_unit:
            if tb_type.lower() == 'ntb':
                tdSql.error(f'select timetruncate(ts,{unit}) from {self.ntbname}')
            elif tb_type.lower() == 'ctb':
                tdSql.error(f'select timetruncate(ts,{unit}) from {self.ctbname}')
            elif tb_type.lower() == 'stb':
                tdSql.error(f'select timetruncate(ts,{unit}) from {self.stbname}')

    def function_check_ntb(self):
        for precision in self.db_param_precision:
            tdSql.execute(f'drop database if exists {self.dbname}')
            tdSql.execute(f'create database {self.dbname} precision "{precision}"')
            tdLog.info(f"=====now is in a {precision} database=====")
            tdSql.execute(f'use {self.dbname}')
            tdSql.execute(f'create table {self.ntbname} (ts timestamp,c0 int)')
            for ts in self.ts_str:
                tdSql.execute(f'insert into {self.ntbname} values("{ts}",1)')
            date_time = self.get_time.time_transform(self.ts_str,precision)
            self.data_check(date_time,precision,'ntb')

    def function_check_stb(self):
        for precision in self.db_param_precision:
            tdSql.execute(f'drop database if exists {self.dbname}')
            tdSql.execute(f'create database {self.dbname} precision "{precision}"')
            tdSql.execute(f'use {self.dbname}')
            tdSql.execute(f'create table {self.stbname} (ts timestamp,c0 int) tags(t0 int)')
            tdSql.execute(f'create table {self.ctbname} using {self.stbname} tags(1)')
            for ts in self.ts_str:
                tdSql.execute(f'insert into {self.ctbname} values("{ts}",1)')
            date_time = self.get_time.time_transform(self.ts_str,precision)
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
