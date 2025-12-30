from new_test_framework.utils import tdLog, tdSql, gettime

def c_style_div(a, b):
    quotient = a // b
    remainder = a % b
    # 当余数不为0且a、b异号时，向零截断需调整商
    if remainder != 0 and (a < 0) ^ (b < 0):
        quotient += 1
    return quotient

class TestFunTimediff:

    updatecfgDict = {'keepColumnName': 1}

    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        cls.get_time = gettime.GetTime()
        cls.ts_str = [
            '2020-1-1',
            '2020-2-1 00:00:01',
            '2020-3-1 00:00:00.001',
            '2020-4-1 00:00:00.001002',
            '2020-5-1 00:00:00.001002001'
        ]
        cls.rest_tag = str(cls.conn).lower().split('.')[0].replace("<taos","")
        cls.db_param_precision = ['ms','us','ns']
        cls.time_unit = ['1w','1d','1h','1m','1s','1a','1u','1b']
        cls.error_unit = ['2w','2d','2h','2m','2s','2a','2u','1c','#1']
        cls.dbname = 'db'
        cls.ntbname = f'{cls.dbname}.ntb'  # normal table
        cls.stbname = f'{cls.dbname}.stb'  # super table
        cls.ctbname = f'{cls.dbname}.ctb'  # child table
        cls.subtractor = 1

    def check_tb_type(self,tb_name):
        tdSql.query(f'select timediff(ts,{self.subtractor}) from {tb_name}')

    def check_tb_unit_type(self,unit,tb_name):
        tdSql.query(f'select timediff(ts,{self.subtractor},{unit}) from {tb_name}')

    def data_check(self,date_time,precision,tb_name):
        for unit in self.time_unit:
            if (unit.lower() == '1u' and precision.lower() == 'ms') or (unit.lower() == '1b' and precision.lower() == 'ms') or (unit.lower() == '1b' and precision.lower() == 'us'):
                # unit too small
                tdSql.error(f'select timediff(ts,{self.subtractor},{unit}) from {tb_name}')
            elif precision.lower() == 'ms':
                self.check_tb_unit_type(unit,tb_name)
                tdSql.checkRows(len(self.ts_str))
                if unit.lower() == '1a':
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]-self.subtractor))
                elif unit.lower() == '1s':
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(c_style_div(date_time[i]-self.subtractor, 1000)))
                elif unit.lower() == '1m':
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(c_style_div(date_time[i]-self.subtractor, 1000*60)))
                elif unit.lower() == '1h':
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(c_style_div(date_time[i]-self.subtractor, 1000*60*60)))
                elif unit.lower() == '1d':
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(c_style_div(date_time[i]-self.subtractor, 1000*60*60*24)))
                elif unit.lower() == '1w':
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(c_style_div(date_time[i]-self.subtractor, 1000*60*60*24*7)))
                self.check_tb_type(tb_name)
                tdSql.checkRows(len(self.ts_str))
                for i in range(len(self.ts_str)):
                    tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]-self.subtractor))
            elif precision.lower() == 'us':
                self.check_tb_unit_type(unit,tb_name)
                tdSql.checkRows(len(self.ts_str))
                if unit.lower() == '1u':
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]-self.subtractor))
                elif unit.lower() == '1a':
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(c_style_div(date_time[i]-self.subtractor, 1000)))
                elif unit.lower() == '1s':
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(c_style_div(date_time[i]-self.subtractor, 1000*1000)))
                elif unit.lower() == '1m':
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(c_style_div(date_time[i]-self.subtractor, 1000*1000*60)))
                elif unit.lower() == '1h':
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(c_style_div(date_time[i]-self.subtractor, 1000*1000*60*60)))
                elif unit.lower() == '1d':
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(c_style_div(date_time[i]-self.subtractor, 1000*1000*60*60*24)))
                elif unit.lower() == '1w':
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(c_style_div(date_time[i]-self.subtractor, 1000*1000*60*60*24*7)))
                self.check_tb_type(tb_name)
                tdSql.checkRows(len(self.ts_str))
                for i in range(len(self.ts_str)):
                    tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]-self.subtractor))
            elif precision.lower() == 'ns':
                if self.rest_tag != 'rest':
                    self.check_tb_unit_type(unit,tb_name)
                    tdSql.checkRows(len(self.ts_str))
                    if unit.lower() == '1b':
                        for i in range(len(self.ts_str)):
                            tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]-self.subtractor))
                    elif unit.lower() == '1u':
                        for i in range(len(self.ts_str)):
                            tdSql.checkEqual(tdSql.queryResult[i][0],int(c_style_div(date_time[i]-self.subtractor, 1000)))
                    elif unit.lower() == '1a':
                        for i in range(len(self.ts_str)):
                            tdSql.checkEqual(tdSql.queryResult[i][0],int(c_style_div(date_time[i]-self.subtractor, 1000*1000)))
                    elif unit.lower() == '1s':
                        for i in range(len(self.ts_str)):
                            tdSql.checkEqual(tdSql.queryResult[i][0],int(c_style_div(date_time[i]-self.subtractor, 1000*1000*1000)))
                    elif unit.lower() == '1m':
                        for i in range(len(self.ts_str)):
                            tdSql.checkEqual(tdSql.queryResult[i][0],int(c_style_div(date_time[i]-self.subtractor, 1000*1000*1000*60)))
                    elif unit.lower() == '1h':
                        for i in range(len(self.ts_str)):
                            tdSql.checkEqual(tdSql.queryResult[i][0],int(c_style_div(date_time[i]-self.subtractor, 1000*1000*1000*60*60)))
                    elif unit.lower() == '1d':
                        for i in range(len(self.ts_str)):
                            tdSql.checkEqual(tdSql.queryResult[i][0],int(c_style_div(date_time[i]-self.subtractor, 1000*1000*1000*60*60*24)))
                    elif unit.lower() == '1w':
                        for i in range(len(self.ts_str)):
                            tdSql.checkEqual(tdSql.queryResult[i][0],int(c_style_div(date_time[i]-self.subtractor, 1000*1000*1000*60*60*24*7)))
                    self.check_tb_type(tb_name)
                    tdSql.checkRows(len(self.ts_str))
                    for i in range(len(self.ts_str)):
                        tdSql.checkEqual(tdSql.queryResult[i][0],int(date_time[i]-self.subtractor))

            for unit in self.error_unit:
                tdSql.error(f'select timediff(ts,{self.subtractor},{unit}) from {tb_name}')
                tdSql.error(f'select timediff(c0,{self.subtractor},{unit}) from {tb_name}')

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
            self.data_check(date_time,precision,self.ntbname)

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
            self.data_check(date_time,precision,self.ctbname)
            self.data_check(date_time,precision,self.stbname)

    def function_without_unit(self):
        for precision in self.db_param_precision:
            tdSql.execute(f'drop database if exists {self.dbname}')
            tdSql.execute(f'create database {self.dbname} precision "{precision}"')
            tdSql.execute(f'use {self.dbname}')
            tdSql.execute(f'create table {self.stbname} (ts timestamp,c0 int) tags(t0 int)')
            tdSql.execute(f'create table {self.ctbname} using {self.stbname} tags(1)')
            for ts in self.ts_str:
                tdSql.execute(f'insert into {self.ctbname} values("{ts}",1)')
            tdSql.query(f'select timediff(ts,{self.subtractor}) from {self.ctbname}')

            date_time = self.get_time.time_transform(self.ts_str,precision)
            for i in range(len(self.ts_str)):
                tdSql.checkEqual(tdSql.queryResult[i][0],int(((date_time[i])-self.subtractor)))

    def function_multi_res_param(self):
        tdSql.execute(f'drop database if exists {self.dbname}')
        tdSql.execute(f'create database {self.dbname}')  # precision: ms
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp,c0 int)')
        tdSql.execute(f'insert into {self.ntbname} values("2023-01-01 00:00:00",1)')
        tdSql.execute(f'insert into {self.ntbname} values("2023-01-01 00:01:00",2)')

        tdSql.query(f'select timediff(last(ts), first(ts)) from {self.ntbname}')
        tdSql.checkData(0, 0, 60000)

    def function_constant_timestamp(self):
        tdSql.execute(f'drop database if exists {self.dbname}')
        tdSql.execute(f'create database {self.dbname}')  # precision: ms
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'create table {self.ntbname} (ts timestamp,c0 int)')
        tdSql.execute(f'insert into {self.ntbname} values("2025-07-01 00:00:00.000",1)')
        tdSql.execute(f'insert into {self.ntbname} values("2025-07-01 00:00:00.001",2)')
        tdSql.execute(f'insert into {self.ntbname} values("2025-07-01 00:00:01.000",3)')
        tdSql.execute(f'insert into {self.ntbname} values("2025-07-01 00:01:00.000",4)')
        tdSql.execute(f'insert into {self.ntbname} values("2025-07-01 01:00:00.000",5)')

        tdSql.query(f'select cast(timediff(ts, 1a) as timestamp) from {self.ntbname}')
        tdSql.checkData(0, 0, "2025-06-30 23:59:59.999")
        tdSql.checkData(1, 0, "2025-07-01 00:00:00.000")
        tdSql.checkData(2, 0, "2025-07-01 00:00:00.999")
        tdSql.checkData(3, 0, "2025-07-01 00:00:59.999")
        tdSql.checkData(4, 0, "2025-07-01 00:59:59.999")

        tdSql.query(f'select cast(timediff(ts, 999u) as timestamp) from {self.ntbname}')
        tdSql.checkData(0, 0, "2025-07-01 00:00:00.000")
        tdSql.checkData(1, 0, "2025-07-01 00:00:00.001")

        tdSql.query(f'select cast(timediff(ts, 1h) as timestamp) from {self.ntbname}')
        tdSql.checkData(3, 0, "2025-06-30 23:01:00.000")
        tdSql.checkData(4, 0, "2025-07-01 00:00:00.000")

        tdSql.query(f'select timediff("2025-07-01 00:00:00.000", "2025-06-01", 1d)')
        tdSql.checkData(0, 0, 30)
        tdSql.query(f'select timediff("2025-07-01 00:00:00.000", "2025-06-01", 1w)')
        tdSql.checkData(0, 0, 4)
        tdSql.query(f'select timediff(now, now+30h, 1d)')
        tdSql.checkData(0, 0, -1)
        tdSql.query(f'select timediff(now, now+23h+59m, 1d)')
        tdSql.checkData(0, 0, 0)
        tdSql.query(f'select timediff(now, now-23h-59m, 1d)')
        tdSql.checkData(0, 0, 0)    
    
    def test_fun_sca_timediff(self):
        """ Fun: timediff()

        1. Constant timestamp test
        2. Normal table test
        3. Super table test
        4. Without unit test
        5. Multi-res parameters test
   
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-9-24 Alex Duan Migrated from uncatalog/system-test/2-query/test_Timediff.py
        """
        # sourcery skip: extract-duplicate-method
        self.function_constant_timestamp()
        
        self.function_check_ntb()
        self.function_check_stb()
        self.function_without_unit()
        self.function_multi_res_param()

        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
