from new_test_framework.utils import tdLog, tdSql
from new_test_framework.utils.sqlset import TDSetSql

class TestNow:

    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        # cls.setsql = # TDSetSql()
        cls.dbname = 'db'
        # name of normal table
        cls.ntbname = f'{cls.dbname}.ntb'
        # name of stable
        cls.stbname = f'{cls.dbname}.stb'
        # structure of column
        cls.column_dict = {
            'ts':'timestamp',
            'c1':'int',
            'c2':'float',
            'c3':'double'
        }
        # structure of tag
        cls.tag_dict = {
            't0':'int'
        }
        # number of child tables
        cls.tbnum = 2
        # values of tag,the number of values should equal to tbnum
        cls.tag_values = [
            f'10',
            f'100'
        ]
        cls.values_list = [
            f'now,10,99.99,11.111111',
            f'today(),100,11.111,22.222222'
        ]
        cls.time_unit = ['b','u','a','s','m','h','d','w']
        cls.symbol = ['+','-','*','/']
        cls.error_values = ['abc','"abc"','!@','today()']
        cls.db_percision = ['ms','us','ns']
        cls.test_values = [1.5, 10]

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
            for param in self.test_values:
                tdSql.query(f'select now() {symbol}{param} from {tbname}')
                tdSql.query(f'select 1 {symbol}{param} from {tbname}')

    def now_check_ntb(self):
        for time_unit in self.db_percision:
            tdSql.execute(f'create database {self.dbname} precision "{time_unit}"')
            tdSql.execute(f'use {self.dbname}')
            tdSql.execute(TDSetSql.set_create_normaltable_sql(self.ntbname,self.column_dict))
            for value in self.values_list:
                tdSql.execute(
                    f'insert into {self.ntbname} values({value})')
            self.data_check(self.ntbname,'normal table')
            tdSql.execute(f'drop database {self.dbname}')

    def now_check_stb(self):
        for time_unit in self.db_percision:
            tdSql.execute(f'create database {self.dbname} precision "{time_unit}"')
            tdSql.execute(f'use {self.dbname}')
            tdSql.execute(TDSetSql.set_create_stable_sql(self.stbname,self.column_dict,self.tag_dict))
            for i in range(self.tbnum):
                tdSql.execute(f"create table {self.stbname}_{i} using {self.stbname} tags({self.tag_values[0]})")
                for value in self.values_list:
                    tdSql.execute(f'insert into {self.stbname}_{i} values({value})')
            for i in range(self.tbnum):
                self.data_check(f'{self.stbname}_{i}','child table')
            self.data_check(self.stbname,'stable')
            tdSql.execute(f'drop database {self.dbname}')
    def test_Now(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """
  # sourcery skip: extract-duplicate-method

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

        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
