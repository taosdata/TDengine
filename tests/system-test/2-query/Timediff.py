from util.log import *
from util.sql import *
from util.cases import *
from util.gettime import *
class TDTestCase:

    updatecfgDict = {'keepColumnName': 1}

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        self.get_time = GetTime()
        self.ts_str = [
            '2020-1-1',
            '2020-2-1 00:00:01',
            '2020-3-1 00:00:00.001',
            '2020-4-1 00:00:00.001002',
            '2020-5-1 00:00:00.001002001'

        ]
        self.rest_tag = str(conn).lower().split('.')[0].replace("<taos","")
        self.db_param_precision = ['ms','us','ns']
        self.time_unit = ['1w','1d','1h','1m','1s','1a','1u','1b']
        self.error_unit = ['2w','2d','2h','2m','2s','2a','2u','1c','#1']
        self.dbname = 'db'
        self.ntbname = f'{self.dbname}.ntb'
        self.stbname = f'{self.dbname}.stb'
        self.ctbname = f'{self.dbname}.ctb'
        self.subtractor = 1  # unit:s
    def check_tbtype(self,tb_type):
        if tb_type.lower() == 'ntb':
            tdSql.query(f'select timediff(ts,{self.subtractor}) from {self.ntbname}')
        elif tb_type.lower() == 'ctb':
            tdSql.query(f'select timediff(ts,{self.subtractor}) from {self.ctbname}')
        elif tb_type.lower() == 'stb':
            tdSql.query(f'select timediff(ts,{self.subtractor}) from {self.stbname}')
    def check_tb_type(self,unit,tb_type):
        if tb_type.lower() == 'ntb':
            tdSql.query(f'select timediff(ts,{self.subtractor},{unit}) from {self.ntbname}')
        elif tb_type.lower() == 'ctb':
            tdSql.query(f'select timediff(ts,{self.subtractor},{unit}) from {self.ctbname}')
        elif tb_type.lower() == 'stb':
            tdSql.query(f'select timediff(ts,{self.subtractor},{unit}) from {self.stbname}')
    def data_check(self,date_time,precision,tb_type):
        for unit in self.time_unit:
            if (unit.lower() == '1u' and precision.lower() == 'ms') or (unit.lower() == '1b' and precision.lower() == 'us') or (unit.lower() == '1b' and precision.lower() == 'ms'):
                if tb_type.lower() == 'ntb':
                    tdSql.error(f'select timediff(ts,{self.subtractor},{unit}) from {self.ntbname}')
                elif tb_type.lower() == 'ctb':
                    tdSql.error(f'select timediff(ts,{self.subtractor},{unit}) from {self.ctbname}')
                elif tb_type.lower() == 'stb':
                    tdSql.error(f'select timediff(ts,{self.subtractor},{unit}) from {self.stbname}')
            elif precision.lower() == 'ms':
                self.check_tb_type(unit,tb_type)
                tdSql.checkRows(len(self.ts_str))
                if unit.lower() == '1a':
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i])-self.subtractor*1000)
                elif unit.lower() == '1s':
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]/1000)-self.subtractor)
                elif unit.lower() == '1m':
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(((date_time[i]/1000)-self.subtractor)/60))
                elif unit.lower() == '1h':
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(((date_time[i]/1000)-self.subtractor)/60/60))
                elif unit.lower() == '1d':
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(((date_time[i]/1000)-self.subtractor)/60/60/24))
                elif unit.lower() == '1w':
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(((date_time[i]/1000)-self.subtractor)/60/60/24/7))
                self.check_tbtype(tb_type)
                tdSql.checkRows(len(self.ts_str))
                for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i])-self.subtractor*1000)
            elif precision.lower() == 'us':
                self.check_tb_type(unit,tb_type)
                tdSql.checkRows(len(self.ts_str))
                if unit.lower() == '1w':
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(((date_time[i]/1000000)-self.subtractor)/60/60/24/7))
                elif unit.lower() == '1d':
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(((date_time[i]/1000000)-self.subtractor)/60/60/24))
                elif unit.lower() == '1h':
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(((date_time[i]/1000000)-self.subtractor)/60/60))
                elif unit.lower() == '1m':
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(((date_time[i]/1000000)-self.subtractor)/60))
                elif unit.lower() == '1s':
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(((date_time[i]/1000000)-self.subtractor)))
                elif unit.lower() == '1a':
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(((date_time[i]/1000)-self.subtractor*1000)))
                elif unit.lower() == '1u':
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(((date_time[i])-self.subtractor*1000000)))
                self.check_tbtype(tb_type)
                tdSql.checkRows(len(self.ts_str))
                for i in range(len(self.ts_str)):
                    tdSql.checkEqual(tdSql.queryResult[i][0],int(((date_time[i])-self.subtractor*1000000)))
            elif precision.lower() == 'ns':
                if self.rest_tag != 'rest':
                    self.check_tb_type(unit,tb_type)
                    tdSql.checkRows(len(self.ts_str))
                    if unit.lower() == '1w':
                        for i in range(len(self.ts_str)):
                            tdSql.checkEqual(tdSql.queryResult[i][0],int(((date_time[i]/1000000000)-self.subtractor)/60/60/24/7))
                    elif unit.lower() == '1d':
                        for i in range(len(self.ts_str)):
                            tdSql.checkEqual(tdSql.queryResult[i][0],int(((date_time[i]/1000000000)-self.subtractor)/60/60/24))
                    elif unit.lower() == '1h':
                        for i in range(len(self.ts_str)):
                            tdSql.checkEqual(tdSql.queryResult[i][0],int(((date_time[i]/1000000000)-self.subtractor)/60/60))
                    elif unit.lower() == '1m':
                        for i in range(len(self.ts_str)):
                            tdSql.checkEqual(tdSql.queryResult[i][0],int(((date_time[i]/1000000000)-self.subtractor)/60))
                    elif unit.lower() == '1s':
                        for i in range(len(self.ts_str)):
                            tdSql.checkEqual(tdSql.queryResult[i][0],int(((date_time[i]/1000000000)-self.subtractor)))
                    elif unit.lower() == '1a':
                        for i in range(len(self.ts_str)):
                            tdSql.checkEqual(tdSql.queryResult[i][0],int(((date_time[i]/1000000)-self.subtractor*1000)))
                    elif unit.lower() == '1u':
                        for i in range(len(self.ts_str)):
                            tdSql.checkEqual(tdSql.queryResult[i][0],int(((date_time[i]/1000)-self.subtractor*1000000)))
                    self.check_tbtype(tb_type)
                    tdSql.checkRows(len(self.ts_str))
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(((date_time[i])-self.subtractor*1000000000)))
            for unit in self.error_unit:
                if tb_type.lower() == 'ntb':
                    tdSql.error(f'select timediff(ts,{self.subtractor},{unit}) from {self.ntbname}')
                    tdSql.error(f'select timediff(c0,{self.subtractor},{unit}) from {self.ntbname}')
                elif tb_type.lower() == 'ctb':
                    tdSql.error(f'select timediff(ts,{self.subtractor},{unit}) from {self.ctbname}')
                    tdSql.error(f'select timediff(c0,{self.subtractor},{unit}) from {self.ntbname}')
                elif tb_type.lower() == 'stb':
                    tdSql.error(f'select timediff(ts,{self.subtractor},{unit}) from {self.stbname}')
                    tdSql.error(f'select timediff(c0,{self.subtractor},{unit}) from {self.ntbname}')
    def function_check_ntb(self):
        for precision in self.db_param_precision:
            tdSql.execute(f'drop database if exists {self.dbname}')
            tdSql.execute(f'create database {self.dbname} precision "{precision}"')
            tdSql.execute(f'use {self.dbname}')
            tdSql.execute(f'create table {self.ntbname} (ts timestamp,c0 int)')
            for ts in self.ts_str:
                tdSql.execute(f'insert into {self.ntbname} values("{ts}",1)')
            for unit in self.error_unit:
                tdSql.error(f'select timediff(ts,{self.subtractor},{unit}) from {self.ntbname}')
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
    def function_without_param(self):
        for precision in self.db_param_precision:
            tdSql.execute(f'drop database if exists {self.dbname}')
            tdSql.execute(f'create database {self.dbname} precision "{precision}"')
            tdSql.execute(f'use {self.dbname}')
            tdSql.execute(f'create table {self.stbname} (ts timestamp,c0 int) tags(t0 int)')
            tdSql.execute(f'create table {self.ctbname} using {self.stbname} tags(1)')
            for ts in self.ts_str:
                tdSql.execute(f'insert into {self.ctbname} values("{ts}",1)')
            date_time = self.get_time.time_transform(self.ts_str,precision)
            tdSql.query(f'select timediff(ts,{self.subtractor}) from {self.ctbname}')
            if precision.lower() == 'ms':
                for i in range(len(self.ts_str)):
                    tdSql.checkEqual(tdSql.queryResult[i][0],int(((date_time[i])-self.subtractor*1000)))
            elif precision.lower() == 'us':
                for i in range(len(self.ts_str)):
                    tdSql.checkEqual(tdSql.queryResult[i][0],int(((date_time[i])-self.subtractor*1000000)))
            elif precision.lower() == 'ns':
                for i in range(len(self.ts_str)):
                    tdSql.checkEqual(tdSql.queryResult[i][0],int(((date_time[i])-self.subtractor*1000000000)))
    def function_multi_res_param(self):
        tdSql.execute(f'drop database if exists {self.dbname}')
        tdSql.execute(f'create database {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp,c0 int)')
        tdSql.execute(f'insert into {self.ntbname} values("2023-01-01 00:00:00",1)')
        tdSql.execute(f'insert into {self.ntbname} values("2023-01-01 00:01:00",2)')

        tdSql.query(f'select timediff(last(ts), first(ts)) from {self.ntbname}')
        tdSql.checkData(0, 0, 60000)



    def run(self):  # sourcery skip: extract-duplicate-method
        self.function_check_ntb()
        self.function_check_stb()
        self.function_without_param()
        self.function_multi_res_param()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
