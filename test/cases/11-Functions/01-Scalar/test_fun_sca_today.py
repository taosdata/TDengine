from new_test_framework.utils import tdLog, tdSql
import datetime
import pandas as pd

class TestFunToday:

    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        cls.today_date = datetime.datetime.strptime(datetime.datetime.now().strftime("%Y-%m-%d"), "%Y-%m-%d")
        cls.today_ts = datetime.datetime.strptime(datetime.datetime.now().strftime("%Y-%m-%d"), "%Y-%m-%d").timestamp()
        cls.today_ts_ns = 0
        cls.time_unit = ['b','u','a','s','m','h','d','w']
        cls.error_param = ['abc','!@#','"abc"','today()']
        cls.arithmetic_operators = ['+','-','*','/']
        cls.relational_operator = ['<','<=','=','>=','>']
        # prepare data
        cls.dbname = 'db'
        cls.ntbname = f'{cls.dbname}.ntb'
        cls.stbname = f'{cls.dbname}.stb'
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
        cls.values_list = [f'now,1,1.55,100.555555,today()',
                        f'now+1d,10,11.11,99.999999,now()',
                        f'today(),3,3.333,333.333333,now()',
                        f'today()-1d,10,11.11,99.999999,now()',
                        f'today()+1d,1,1.55,100.555555,today()']
        
        cls.rest_tag = str(cls.conn).lower().split('.')[0].replace("<taos","")
        if cls.rest_tag != 'rest':
            cls.db_percision = ['ms','us','ns']
        else:
            cls.db_percision = ['ms','us']

    def set_create_normaltable_sql(self, ntbname, column_dict):
        column_sql = ''
        for k, v in column_dict.items():
            column_sql += f"{k} {v},"
        create_ntb_sql = f'create table {ntbname} ({column_sql[:-1]})'
        return create_ntb_sql
    def set_create_stable_sql(self,stbname,column_dict,tag_dict):
        column_sql = ''
        tag_sql = ''
        for k,v in column_dict.items():
            column_sql += f"{k} {v},"
        for k,v in tag_dict.items():
            tag_sql += f"{k} {v},"
        create_stb_sql = f'create table {stbname} ({column_sql[:-1]}) tags({tag_sql[:-1]})'
        return create_stb_sql

    def data_check(self,column_dict={},tbname = '',values_list = [],tb_num = 1,tb = 'tb',precision = 'ms'):
        for k,v in column_dict.items():
            num_up = 0
            num_down = 0
            num_same = 0
            if v.lower() == 'timestamp':
                tdSql.query(f'select {k} from {tbname}')
                
                for i in tdSql.queryResult:
                    if precision == 'ms':
                        self.today_ts_trans = int(self.today_ts)*1000
                        if int(i[0].timestamp())*1000 > int(self.today_ts)*1000:
                            num_up += 1
                        elif int(i[0].timestamp())*1000 == int(self.today_ts)*1000:
                            num_same += 1
                        elif int(i[0].timestamp())*1000 < int(self.today_ts)*1000:
                            num_down += 1
                    elif precision == 'us':
                        self.today_ts_trans = int(self.today_ts)*1000000
                        if int(i[0].timestamp())*1000000 > int(self.today_ts)*1000000:
                            num_up += 1
                        elif int(i[0].timestamp())*1000000 == int(self.today_ts)*1000000:
                            num_same += 1
                        elif int(i[0].timestamp())*1000000 < int(self.today_ts)*1000000:
                            num_down += 1
                    elif precision == 'ns':
                        self.today_ts_trans = int(self.today_ts)*1000000000
                        if i[0] > int(self.today_ts)*1000000000:
                            num_up += 1
                        elif i[0] == int(self.today_ts)*1000000000:
                            num_same += 1
                        elif i[0] < int(self.today_ts)*1000000000:
                            num_down += 1
                tdSql.query(f"select today() from {tbname}")
                tdSql.checkRows(len(values_list)*tb_num)    
                print(self.today_ts_trans,self.today_ts,precision,num_up,num_down,i[0])
                tdSql.checkData(0, 0, self.today_ts_trans)
                tdSql.query(f"select * from {tbname} where {k}=today()")
                if tb == 'tb':
                    tdSql.checkRows(num_same*tb_num)
                elif tb == 'stb':
                    tdSql.checkRows(num_same)
                for i in [f'{tbname}']:
                    for unit in self.time_unit:
                        for symbol in ['+','-']:
                            tdSql.query(f"select today() {symbol}1{unit} from {i}")
                            tdSql.checkRows(len(values_list)*tb_num)
                    for unit in self.error_param:
                        for symbol in self.arithmetic_operators:
                            tdSql.error(f'select today() {symbol}{unit} from {i}')
                    for symbol in self.arithmetic_operators:
                        tdSql.query(f'select now(){symbol}null from {i}')
                        tdSql.checkData(0,0,None)
                    for symbol in self.relational_operator:
                        tdSql.query(f'select * from {i} where {k} {symbol} today()')
                        if symbol == '<' :
                            if tb == 'tb':
                                tdSql.checkRows(num_down*tb_num)
                            elif tb == 'stb':
                                tdSql.checkRows(num_down)
                        elif symbol == '<=':
                            if tb == 'tb':
                                tdSql.checkRows((num_same+num_down)*tb_num)
                            elif tb == 'stb':
                                tdSql.checkRows(num_same+num_down)
                        elif symbol == '=':
                            if tb == 'tb':
                                tdSql.checkRows(num_same*tb_num)
                            elif tb == 'stb':
                                tdSql.checkRows(num_same)
                        elif symbol == '>=':
                            if tb == 'tb':
                                tdSql.checkRows((num_up + num_same)*tb_num)
                            elif tb == 'stb':
                                tdSql.checkRows(num_up + num_same)
                        elif symbol == '>':
                            if tb == 'tb':
                                tdSql.checkRows(num_up*tb_num)
                            elif tb == 'stb':
                                tdSql.checkRows(num_up)
                tdSql.query(f"select today()/0 from {tbname}")
                tdSql.checkRows(len(values_list)*tb_num)
                tdSql.checkData(0,0,None)
                tdSql.query(f"select {k} from {tbname} where {k}=today()")
                if tb == 'tb':
                    tdSql.checkRows(num_same*tb_num)
                    for i in range(num_same*tb_num):
                        print(self.today_ts_trans,precision,num_up,num_down)
                        tdSql.checkData(i, 0, self.today_ts_trans)
                elif tb == 'stb':
                    tdSql.checkRows(num_same)
                    for i in range(num_same):
                        tdSql.checkData(i, 0, self.today_ts_trans)
    def today_check_ntb(self):
        for time_unit in self.db_percision:
            
            tdSql.execute(f'create database {self.dbname} precision "{time_unit}"')
            tdSql.execute(f'use {self.dbname}')
            tdSql.execute(self.set_create_normaltable_sql(self.ntbname,self.column_dict))
            for i in self.values_list:
                tdSql.execute(
                    f'insert into {self.ntbname} values({i})')
            self.data_check(self.column_dict,self.ntbname,self.values_list,1,'tb',time_unit)
            tdSql.execute(f'drop database {self.dbname}')
    def today_check_stb_tb(self):
        for time_unit in self.db_percision:
            
            tdSql.execute(f'create database {self.dbname} precision "{time_unit}"')
            tdSql.execute(f'use {self.dbname}')
            tdSql.execute(self.set_create_stable_sql(self.stbname,self.column_dict,self.tag_dict))
            for i in range(self.tbnum):
                tdSql.execute(f'create table if not exists {self.stbname}_{i} using {self.stbname} tags({self.tag_values[i]})')
                for j in self.values_list:
                    tdSql.execute(f'insert into {self.stbname}_{i} values ({j})')
            # check child table
            for i in range(self.tbnum):
                self.data_check(self.column_dict,f'{self.stbname}_{i}',self.values_list,1,'tb',time_unit)
            # check stable
            self.data_check(self.column_dict,self.stbname,self.values_list,self.tbnum,'stb',time_unit)
            tdSql.execute(f'drop database {self.dbname}')

    def test_fun_sca_today(self):
        """ Fun: today()

        1. Query from normal/super table
        2. Query from ms/us/ns precision database
        3. Query from where condition
        4. Query Error parameter
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-9-24 Alex Duan Migrated from uncatalog/system-test/2-query/test_Today.py
        """

        self.today_check_ntb()
        self.today_check_stb_tb()

        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
