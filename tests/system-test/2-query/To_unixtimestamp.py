from time import sleep

from util.log import *
from util.sql import *
from util.cases import *
from util.sqlset import TDSetSql




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
            'c3':'binary(20)'

        }
        # structure of tag
        self.tag_dict = {
            't0':'int'
        }
        # number of child tables
        self.tbnum = 2
        # values of tag,the number of values should equal to tbnum
        self.tag_values = [
            '10',
            '100'
        ]
        # values of rows, structure should be same as column
        self.values_list = [
            f'now,10,99.99,"abc"',
            f'today(),100,11.111,"abc"'

        ]
        self.error_param = [1,1.5,'now()']
    def data_check(self,tbname,values_list,tb_type,tb_num=1):
        for time in ['1970-01-01T08:00:00+0800','1970-01-01T08:00:00+08:00']:
            tdSql.query(f"select to_unixtimestamp('{time}') from {tbname}")
            if tb_type == 'ntb' or tb_type == 'ctb':
                tdSql.checkRows(len(values_list))
                for i in range(len(values_list)):
                    tdSql.checkEqual(tdSql.queryResult[i][0],0)
            elif tb_type == 'stb':
                tdSql.checkRows(len(self.values_list)*tb_num)
        for time in ['1900-01-01T08:00:00+08:00']:
            tdSql.query(f"select to_unixtimestamp('{time}') from {tbname}")
            if tb_type == 'ntb' or tb_type == 'ctb':
                tdSql.checkRows(len(values_list))
            elif tb_type == 'stb':
                tdSql.checkRows(len(self.values_list)*tb_num)
        for time in ['2020-01-32T08:00:00','2020-13-32T08:00:00','acd']:
            tdSql.query(f"select to_unixtimestamp('{time}') from {tbname}")
            if tb_type == 'ntb' or tb_type == 'ctb':
                tdSql.checkRows(len(values_list))
                for i in range(len(values_list)):
                    tdSql.checkEqual(tdSql.queryResult[i][0],None)
            elif tb_type == 'stb':
                tdSql.checkRows(len(values_list)*tb_num)
        for i in self.column_dict.keys():
            tdSql.query(f"select {i} from {tbname} where to_unixtimestamp('1970-01-01T08:00:00+08:00')=0")
            if tb_type == 'ntb' or tb_type == 'ctb':
                tdSql.checkRows(len(values_list))
            elif tb_type == 'stb':
                tdSql.checkRows(len(values_list)*tb_num)
        for time in self.error_param:
            tdSql.error(f"select to_unixtimestamp({time}) from {tbname}")
    def timestamp_change_check_ntb(self):
        tdSql.execute(f'create database {self.dbname}')
        tdSql.execute(self.setsql.set_create_normaltable_sql(self.ntbname,self.column_dict))
        for i in range(len(self.values_list)):
            tdSql.execute(f'insert into {self.ntbname} values({self.values_list[i]})')
        self.data_check(self.ntbname,self.values_list,'ntb')
        tdSql.execute(f'drop database {self.dbname}')
    def timestamp_change_check_stb(self):
        tdSql.execute(f'create database {self.dbname}')
        tdSql.execute(self.setsql.set_create_stable_sql(self.stbname,self.column_dict,self.tag_dict))
        for i in range(self.tbnum):
            tdSql.execute(f'create table {self.stbname}_{i} using {self.stbname} tags({self.tag_values[i]})')
            for j in range(len(self.values_list)):
                tdSql.execute(f'insert into {self.stbname}_{i} values({self.values_list[j]})')
        for i in range(self.tbnum):
            self.data_check(f'{self.stbname}_{i}',self.values_list,'ctb')
        self.data_check(self.stbname,self.values_list,'stb',self.tbnum)
        tdSql.execute(f'drop database {self.dbname}')
    def timestamp_change_return_type(self):
        tdSql.query(f"select to_unixtimestamp('1970-01-01 08:00:00+08:00', 0);")
        tdSql.checkEqual(tdSql.queryResult[0][0], 0)
        tdSql.query(f"select to_unixtimestamp('1970-01-01 00:00:00', 1);")
        tdSql.checkData(0, 0, datetime.datetime(1970, 1, 1, 0, 0, 0))
        tdSql.error(f"select to_unixtimestamp('1970-01-01 08:00:00+08:00', 2);")
        tdSql.error(f"select to_unixtimestamp('1970-01-01 08:00:00+08:00', 1.5);")
        tdSql.error(f"select to_unixtimestamp('1970-01-01 08:00:00+08:00', 'abc');")
        tdSql.error(f"select to_unixtimestamp('1970-01-01 08:00:00+08:00', true);")
        tdSql.error(f"select to_unixtimestamp('1970-01-01 08:00:00+08:00', 1, 3);")
    def run(self):  # sourcery skip: extract-duplicate-method
        self.timestamp_change_check_ntb()
        self.timestamp_change_check_stb()
        self.timestamp_change_return_type()
    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
