
from util.dnodes import *
from util.log import *
from util.sql import *
from util.cases import *
from util.sqlset import *

class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        self.setsql = TDSetSql()
        self.dbname = 'db'
        # name of normal table
        self.ntbname = f'{self.dbname}.ntb'
        # name of stable
        self.stbname = f'{self.dbname}.stb'
        # structure of column
        self.column_dict = {
            'ts':'timestamp',
            'c1':'int',
            'c2':'float',
            'c3':'double'
        }
        # structure of tag
        self.tag_dict = {
            't0':'int'
        }
        # number of child tables
        self.tbnum = 2
        # values of tag,the number of values should equal to tbnum
        self.tag_values = [
            f'10',
            f'100'
        ]
        self.values_list = [
            f'now,10,99.99,11.111111',
            f'today(),100,11.111,22.222222'
        ]
        self.time_unit = ['b','u','a','s','m','h','d','w']
        self.symbol = ['+','-','*','/']
        self.error_values = [1.5,'abc','"abc"','!@','today()']
        self.db_percision = ['ms','us','ns']
    def tbtype_check(self,tb_type):
        if tb_type == 'normal table' or tb_type == 'child table':
            tdSql.checkRows(len(self.values_list))
        elif tb_type == 'stable':
            tdSql.checkRows(len(self.values_list) * self.tbnum)
    def data_check(self,tbname,tb_type):
        tdSql.query(f'select now() from {tbname}')
        self.tbtype_check(tb_type)
        for unit in self.time_unit:
            for symbol in self.symbol:
                if symbol in ['+','-']:
                    tdSql.query(f'select now() {symbol}1{unit} from {tbname}')
                    self.tbtype_check(tb_type)
        for k,v in self.column_dict.items():
            if v.lower() != 'timestamp':
                continue
            else:
                tdSql.query(f'select * from {tbname} where {k}>=now()')
                tdSql.checkRows(0)
                tdSql.query(f'select * from {tbname} where {k}<now()')
                self.tbtype_check(tb_type)
        for symbol in self.symbol:
            for param in self.error_values:
                tdSql.error(f'select now() {symbol}{param} from {tbname}')
            tdSql.query(f'select now(){symbol}null from {tbname}')
            self.tbtype_check(tb_type)
            for i in range(len(self.values_list)):
                tdSql.checkData(i,0,None)

    def now_check_ntb(self):
        for time_unit in self.db_percision:
            tdSql.execute(f'create database {self.dbname} precision "{time_unit}"')
            tdSql.execute(f'use {self.dbname}')
            tdSql.execute(self.setsql.set_create_normaltable_sql(self.ntbname,self.column_dict))
            for value in self.values_list:
                tdSql.execute(
                    f'insert into {self.ntbname} values({value})')
            self.data_check(self.ntbname,'normal table')
            tdSql.execute(f'drop database {self.dbname}')

    def now_check_stb(self):
        for time_unit in self.db_percision:
            tdSql.execute(f'create database {self.dbname} precision "{time_unit}"')
            tdSql.execute(f'use {self.dbname}')
            tdSql.execute(self.setsql.set_create_stable_sql(self.stbname,self.column_dict,self.tag_dict))
            for i in range(self.tbnum):
                tdSql.execute(f"create table {self.stbname}_{i} using {self.stbname} tags({self.tag_values[0]})")
                for value in self.values_list:
                    tdSql.execute(f'insert into {self.stbname}_{i} values({value})')
            for i in range(self.tbnum):
                self.data_check(f'{self.stbname}_{i}','child table')
            self.data_check(self.stbname,'stable')
            tdSql.execute(f'drop database {self.dbname}')
    def run(self):  # sourcery skip: extract-duplicate-method

        self.now_check_ntb()
        self.now_check_stb()

        ## TD-25540
        tdSql.execute(f'create database db1')
        tdSql.execute(f'use db1')
        tdSql.execute(f"create table db1.tb (ts timestamp, c0 int)")
        tdSql.execute(f'insert into db1.tb values(now + 1h, 1)')

        for func in {"NOW", "NOW()", "TODAY()", "1", "'1970-01-01 00:00:00'"}:
            tdSql.query(f"SELECT _wstart, count(*) FROM (SELECT ts, LAST(c0) FROM db1.tb WHERE ts > {func}) interval(1d);")
            tdSql.checkRows(1)



    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
